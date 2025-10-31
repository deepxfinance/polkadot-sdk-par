#![cfg_attr(not(feature = "std"), no_std)]

use codec::{Encode, Decode, Codec, MaxEncodedLen};
use scale_info::TypeInfo;
use sp_runtime::ConsensusEngineId;
use sp_std::vec::Vec;

pub mod digests;
pub mod inherents;

/// Key type for HOTSTUFF module.
pub const HOTSTUFF_KEY_TYPE: sp_core::crypto::KeyTypeId = sp_core::crypto::KeyTypeId(*b"hots");

/// Authority set id starts with zero at HOTSTUFF pallet genesis.
pub const GENESIS_AUTHORITY_SET_ID: u64 = 0;

pub use sp_consensus_slots::{Slot, SlotDuration};
use serde::{Deserialize, Serialize};
use sp_application_crypto::RuntimeAppPublic;
use sp_core::crypto::KeyTypeId;
use sp_runtime::transaction_validity::{TransactionSource, TransactionValidity};

mod app {
	use crate::HOTSTUFF_KEY_TYPE;

	use sp_application_crypto::{app_crypto, sr25519};
	app_crypto!(sr25519, HOTSTUFF_KEY_TYPE);
}

sp_application_crypto::with_pair! {
	/// The hotstuff crypto scheme defined via the keypair type.
	pub type AuthorityPair = app::Pair;
}

#[cfg(feature = "bls-experimental")]
pub mod bls_crypto {
	use crate::HOTSTUFF_KEY_TYPE;
	use crate::RuntimeAuthorityId;

	use sp_application_crypto::{app_crypto, bls381};
	use sp_core::ByteArray;

	app_crypto!(bls381, HOTSTUFF_KEY_TYPE);

	pub type AuthorityId = Public;

	pub type AuthoritySignature = Signature;

	sp_application_crypto::with_pair! {
		/// The hotstuff BLS381 crypto scheme defined via the keypair type.
		pub type AuthorityPair = Pair;
	}

	impl From<RuntimeAuthorityId> for Public {
		fn from(value: RuntimeAuthorityId) -> Self {
			Self::try_from(value.0.as_slice()).unwrap()
		}
	}
}

/// Identity for Hotstuff BLS381 authority.
#[derive(Debug, Eq, PartialEq, Clone, Encode, Decode, TypeInfo, Serialize, Deserialize)]
pub struct RuntimeAuthorityId(pub Vec<u8>);

impl AsRef<[u8]> for RuntimeAuthorityId {
	fn as_ref(&self) -> &[u8] {
		self.0.as_slice()
	}
}

impl RuntimeAppPublic for RuntimeAuthorityId {
	const ID: KeyTypeId = HOTSTUFF_KEY_TYPE;
	type Signature = Vec<u8>;

	fn all() -> Vec<Self> {
		Vec::new()
	}

	fn generate_pair(_seed: Option<Vec<u8>>) -> Self {
		Self(Vec::new())
	}

	fn sign<M: AsRef<[u8]>>(&self, _msg: &M) -> Option<Self::Signature> {
		None
	}

	fn verify<M: AsRef<[u8]>>(&self, _msg: &M, _signature: &Self::Signature) -> bool {
		false
	}

	fn to_raw_vec(&self) -> Vec<u8> {
		self.0.clone()
	}
}

impl MaxEncodedLen for RuntimeAuthorityId {
	fn max_encoded_len() -> usize {
		150
	}
}

#[cfg(feature = "bls-experimental")]
impl From<bls_crypto::AuthorityId> for RuntimeAuthorityId {
	fn from(value: bls_crypto::AuthorityId) -> Self {
		Self(<bls_crypto::AuthorityId as AsRef<[u8]>>::as_ref(&value).to_vec())
	}
}

/// Identity of a Hotstuff authority.
pub type AuthorityId = app::Public;

/// Signature for a Hotstuff authority.
pub type AuthoritySignature = app::Signature;

/// The `ConsensusEngineId` of HOTSTUFF.
pub const HOTSTUFF_ENGINE_ID: ConsensusEngineId = *b"HOTS";

/// The storage key for the current set of weighted hotstuff authorities.
/// The value stored is an encoded VersionedAuthorityList.
pub const HOTSTUFF_AUTHORITIES_KEY: &[u8] = b":hotstuff_authorities";

/// The weight of an authority.
pub type AuthorityWeight = u64;

/// The index of an authority.
pub type AuthorityIndex = u64;

/// The monotonic identifier of a Hotstuff set of authorities.
pub type SetId = u64;

/// The round indicator.
pub type RoundNumber = u64;

/// A list of Hotstuff authorities with associated weights.
pub type AuthorityList<AuthorityId> = Vec<(AuthorityId, AuthorityWeight)>;

/// An consensus log item for Hotstuff.
#[derive(Decode, Encode)]
pub enum ConsensusLog<AuthorityId: Codec> {
	/// The authorities have changed.
	#[codec(index = 1)]
	AuthoritiesChange(Vec<AuthorityId>),
	/// Disable the authority with given index.
	#[codec(index = 2)]
	OnDisabled(AuthorityIndex),
}

sp_api::decl_runtime_apis! {
	/// API necessary for block authorship with hotstuff.
	pub trait HotstuffApi<AuthorityId: Codec> {
		/// Returns the slot duration for hotstuff.
		///
		/// Currently, only the value provided by this type at genesis will be used.
		fn slot_duration() -> SlotDuration;

		/// Return current slot.
		fn current_slot() -> Slot;

		/// Return the current set of authorities.
		fn authorities() -> Vec<AuthorityId>;

		/// Validate multi transactions.
		fn validate_transactions(
			txs: sp_std::vec::Vec<(TransactionSource, Block::Extrinsic)>,
			block_hash: Block::Hash,
		) -> Vec<TransactionValidity>;
	}
}
