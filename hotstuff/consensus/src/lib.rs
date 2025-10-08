pub mod aux_schema;
pub mod import;
pub mod message;

pub mod aggregator;
pub mod authorities;
pub mod client;
pub mod config;
pub mod consensus;
pub mod network;
pub mod primitives;
pub mod store;
pub mod synchronizer;
pub mod import_queue;

pub use client::{block_import, LinkHalf};
pub use import::HotstuffBlockImport;
use std::fmt::Debug;
use codec::Codec;
use log::trace;
use hotstuff_primitives::{AuthorityList, HotstuffApi};
use hotstuff_primitives::{SlotDuration, digests::CompatibleDigestItem, AuthorityId};
use sc_client_api::{AuxStore, UsageProvider};
use sp_api::{BlockT, Core, ProvideRuntimeApi};
use sp_consensus::Error as ConsensusError;
use sp_consensus_slots::Slot;
use sp_core::Pair;
use sp_runtime::DigestItem;
use sp_runtime::traits::{Header, NumberFor, Zero};
use crate::message::QC;

pub const LOG_TARGET: &str = "hots";
/// The log target to be used by client code.
pub const CLIENT_LOG_TARGET: &str = "hotstuff";

/// Get the slot duration for Hotstuff by reading from a runtime API at the best block's state.
pub fn slot_duration<A, B, C>(client: &C) -> sp_blockchain::Result<SlotDuration>
where
    A: Codec,
    B: BlockT,
    C: AuxStore + ProvideRuntimeApi<B> + UsageProvider<B>,
    C::Api: HotstuffApi<B, A>,
{
    slot_duration_at(client, client.usage_info().chain.best_hash)
}

/// Get the slot duration for Hotstuff by reading from a runtime API at a given block's state.
pub fn slot_duration_at<A, B, C>(client: &C, parent_hash: B::Hash) -> sp_blockchain::Result<SlotDuration>
where
    A: Codec,
    B: BlockT,
    C: AuxStore + ProvideRuntimeApi<B>,
    C::Api: HotstuffApi<B, A>,
{
    client
        .runtime_api()
        .slot_duration(parent_hash).map_err(|err| err.into())
}

/// Hotstuff Errors
#[derive(Debug, thiserror::Error)]
pub enum Error<B: BlockT> {
    /// Multiple Hotstuff pre-runtime headers
    #[error("Multiple Hotstuff pre-runtime headers")]
    MultipleHeaders,
    /// No Hotstuff pre-runtime digest found
    #[error("No Hotstuff pre-runtime digest found")]
    NoDigestFound,
    /// Header is unsealed
    #[error("Header {0:?} is unsealed")]
    HeaderUnsealed(B::Hash),
    /// Header has a bad seal
    #[error("Header {0:?} has a bad seal")]
    HeaderBadSeal(B::Hash),
    /// Slot Author not found
    #[error("Slot Author not found")]
    SlotAuthorNotFound,
    /// The author is incorrect.
    #[error("No expected author")]
    InvalidAuthor,
    /// Bad signature
    #[error("Bad signature on {0:?}")]
    BadSignature(B::Hash),
    /// Client Error
    #[error(transparent)]
    Client(sp_blockchain::Error),
    /// Unknown inherent error for identifier
    #[error("Unknown inherent error for identifier: {}", String::from_utf8_lossy(.0))]
    UnknownInherentError(sp_inherents::InherentIdentifier),
    /// Inherents Error
    #[error("Inherent error: {0}")]
    Inherent(sp_inherents::Error),
}

impl<B: BlockT> From<Error<B>> for String {
    fn from(error: Error<B>) -> String {
        error.to_string()
    }
}

impl<B: BlockT> From<PreDigestLookupError> for Error<B> {
    fn from(e: PreDigestLookupError) -> Self {
        match e {
            PreDigestLookupError::MultipleHeaders => Error::MultipleHeaders,
            PreDigestLookupError::NoDigestFound => Error::NoDigestFound,
        }
    }
}

/// Errors in slot and seal verification.
#[derive(Debug, thiserror::Error)]
pub enum SealVerificationError<Header> {
    /// Header is deferred to the future.
    #[error("Header slot is in the future")]
    Deferred(Header, Slot),

    /// The header has no seal digest.
    #[error("Header is unsealed.")]
    Unsealed,

    /// The header has a malformed seal.
    #[error("Header has a malformed seal")]
    BadSeal,

    /// The header has a bad signature.
    #[error("Header has a bad signature")]
    BadSignature,

    /// No QC found.
    #[error("No full QC for provided slot")]
    FullQCNotFound,

    /// The author is incorrect.
    #[error("No expected author")]
    InvalidAuthor,

    /// Header has no valid slot pre-digest.
    #[error("Header has no valid slot pre-digest")]
    InvalidPreDigest(PreDigestLookupError),
}

/// Errors in pre-digest lookup.
#[derive(Debug, thiserror::Error)]
pub enum PreDigestLookupError {
    /// Multiple Hotstuff pre-runtime headers
    #[error("Multiple Hotstuff pre-runtime headers")]
    MultipleHeaders,
    /// No Hotstuff pre-runtime digest found
    #[error("No Hotstuff pre-runtime digest found")]
    NoDigestFound,
}

/// Run `Hotstuff` in a compatibility mode.
///
/// This is required for when the chain was launched and later there
/// was a consensus breaking change.
#[derive(Debug, Clone)]
pub enum CompatibilityMode<N> {
    /// Don't use any compatibility mode.
    None,
    /// Call `initialize_block` before doing any runtime calls.
    ///
    /// Previously the node would execute `initialize_block` before fetchting the authorities
    /// from the runtime. This behaviour changed in: <https://github.com/paritytech/substrate/pull/9132>
    ///
    /// By calling `initialize_block` before fetching the authorities, on a block that
    /// would enact a new validator set, the block would already be build/sealed by an
    /// authority of the new set. With this mode disabled (the default) a block that enacts a new
    /// set isn't sealed/built by an authority of the new set, however to make new nodes be able to
    /// sync old chains this compatibility mode exists.
    UseInitializeBlock {
        /// The block number until this compatibility mode should be executed. The first runtime
        /// call in the context of the `until` block (importing it/building it) will disable the
        /// compatibility mode (i.e. at `until` the default rules will apply). When enabling this
        /// compatibility mode the `until` block should be a future block on which all nodes will
        /// have upgraded to a release that includes the updated compatibility mode configuration.
        /// At `until` block there will be a hard fork when the authority set changes, between the
        /// old nodes (running with `initialize_block`, i.e. without the compatibility mode
        /// configuration) and the new nodes.
        until: N,
    },
}

impl<N> Default for CompatibilityMode<N> {
    fn default() -> Self {
        Self::None
    }
}

fn authorities<A, B, C>(
    client: &C,
    parent_hash: B::Hash,
    context_block_number: NumberFor<B>,
    compatibility_mode: &CompatibilityMode<NumberFor<B>>,
) -> Result<Vec<A>, ConsensusError>
where
    A: Codec + Debug,
    B: BlockT,
    C: ProvideRuntimeApi<B>,
    C::Api: HotstuffApi<B, A>,
{
    let runtime_api = client.runtime_api();

    match compatibility_mode {
        CompatibilityMode::None => {},
        // Use `initialize_block` until we hit the block that should disable the mode.
        CompatibilityMode::UseInitializeBlock { until } =>
            if *until > context_block_number {
                runtime_api
                    .initialize_block(
                        parent_hash,
                        &B::Header::new(
                            context_block_number,
                            Default::default(),
                            Default::default(),
                            parent_hash,
                            Default::default(),
                        ),
                    )
                    .map_err(|_| ConsensusError::InvalidAuthoritiesSet)?;
            },
    }

    runtime_api
        .authorities(parent_hash)
        .ok()
        .ok_or(ConsensusError::InvalidAuthoritiesSet)
}

/// Extract a pre-digest from a block header.
///
/// This fails if there is no pre-digest or there are multiple.
///
/// Returns the `slot` stored in the pre-digest or an error if no pre-digest was found.
pub fn find_pre_digest<B: BlockT, Signature: Codec>(
    header: &B::Header,
) -> Result<Slot, PreDigestLookupError> {
    if header.number().is_zero() {
        return Ok(0.into())
    }

    let mut pre_digest: Option<Slot> = None;
    for log in header.digest().logs() {
        trace!(target: LOG_TARGET, "Checking log {:?}", log);
        match (CompatibleDigestItem::<Signature>::as_hotstuff_pre_digest(log), pre_digest.is_some()) {
            (Some(_), true) => return Err(PreDigestLookupError::MultipleHeaders),
            (None, _) => trace!(target: LOG_TARGET, "Ignoring digest not meant for us"),
            (s, false) => pre_digest = s,
        }
    }
    pre_digest.ok_or_else(|| PreDigestLookupError::NoDigestFound)
}

/// Get the slot author for given block along with authorities.
pub fn slot_author<P: Pair>(slot: Slot, authorities: &[AuthorityId]) -> Option<&AuthorityId> {
    if authorities.is_empty() {
        return None
    }

    let idx = *slot % (authorities.len() as u64);
    assert!(
        idx <= usize::MAX as u64,
        "It is impossible to have a vector with length beyond the address space; qed",
    );

    let current_author = authorities.get(idx as usize).expect(
        "authorities not empty; index constrained to list length;this is a valid index; qed",
    );

    Some(current_author)
}

/// Check a header has been signed by the right key. If the slot is too far in the future, an error
/// will be returned. If it's successful, returns the pre-header (i.e. without the seal),
/// the slot, and the digest item containing the seal.
///
/// Note that this does not check for equivocations, and [`check_equivocation`] is recommended
/// for that purpose.
///
/// This digest item will always return `Some` when used with `as_hotstuff_seal`.
pub fn check_header_slot_and_seal<B: BlockT, P: Pair>(
    slot_now: Slot,
    mut header: B::Header,
    authorities: &[AuthorityId],
) -> Result<(B::Header, Slot, DigestItem), SealVerificationError<B::Header>>
where
    P::Signature: Codec,
    P::Public: Codec + PartialEq + Clone,
{
    // TODO 1. We should check Full QC or check signatures for block? we have no sign for block header.
    let seal = header.digest_mut().pop().ok_or(SealVerificationError::Unsealed)?;
    let slot = find_pre_digest::<B, P::Signature>(&header)
        .map_err(SealVerificationError::InvalidPreDigest)?;

    if slot > slot_now {
        header.digest_mut().push(seal);
        Err(SealVerificationError::Deferred(header, slot))
    } else {
        // check the signature is valid under the expected authority and
        // chain state.
        let full_qc: Vec<QC<B>> = seal.as_hotstuff_seal().ok_or(SealVerificationError::FullQCNotFound)?;
        let authorities = authorities
            .iter()
            .map(|id| (id.clone(), 0))
            .collect::<AuthorityList>();
        for qc in full_qc {
            if let Err(e) = qc.verify(&authorities) {
                log::warn!(target: "Hotstuff", "check_header qc.verify err: {e:?} for header: {header:?}");
                return Err(SealVerificationError::BadSignature);
            }
        }
        Ok((header, slot, seal))
    }
}
