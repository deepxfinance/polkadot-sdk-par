#![allow(clippy::type_complexity)]
use std::{marker::PhantomData, sync::Arc};

use codec::Decode;
use hotstuff_primitives::RuntimeAuthorityId;
use sc_client_api::{AuxStore, Backend, BlockchainEvents, CallExecutor, ExecutionStrategy, ExecutorProvider, Finalizer, LockImportRun, StorageProvider};
use sc_consensus::BlockImport;
use sp_api::ProvideRuntimeApi;
use sp_blockchain::{Error as ClientError, HeaderBackend, HeaderMetadata};
use sp_core::traits::CallContext;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, NumberFor},
};

use sc_basic_authorship::BlockPropose;
use crate::{authorities::SharedAuthoritySet, aux_schema, AuthorityId};

/// A trait that includes all the client functionalities hotstuff requires.
/// Ideally this would be a trait alias, we're not there yet.
/// tracking issue <https://github.com/rust-lang/rust/issues/41517>
pub trait ClientForHotstuff<Block, BE>:
    LockImportRun<Block, BE>
    + Finalizer<Block, BE>
    + AuxStore
    + HeaderMetadata<Block, Error = sp_blockchain::Error>
    + HeaderBackend<Block>
    + BlockchainEvents<Block>
    + ProvideRuntimeApi<Block>
    + ExecutorProvider<Block>
    + BlockImport<Block, Error = sp_consensus::Error>
    + StorageProvider<Block, BE>
where
    BE: Backend<Block>,
    Block: BlockT,
{
}

impl<Block, BE, T> ClientForHotstuff<Block, BE> for T
where
    BE: Backend<Block>,
    Block: BlockT,
    T: LockImportRun<Block, BE>
        + Finalizer<Block, BE>
        + AuxStore
        + HeaderMetadata<Block, Error = sp_blockchain::Error>
        + HeaderBackend<Block>
        + BlockchainEvents<Block>
        + ProvideRuntimeApi<Block>
        + ExecutorProvider<Block>
        + BlockImport<Block, Error = sp_consensus::Error>
        + StorageProvider<Block, BE>,
{
}

/// Something that one can ask to do a block sync request.
pub(crate) trait BlockSyncRequester<Block: BlockT> {
    /// Notifies the sync service to try and sync the given block from the given
    /// peers.
    ///
    /// If the given vector of peers is empty then the underlying implementation
    /// should make a best effort to fetch the block from any peers it is
    /// connected to (NOTE: this assumption will change in the future #3629).
    fn set_sync_fork_request(
        &self,
        peers: Vec<sc_network::PeerId>,
        hash: Block::Hash,
        number: NumberFor<Block>,
    );
}

/// Link between the block importer and the background voter.
pub struct LinkHalf<Block: BlockT, C, SC> {
    pub client: Arc<C>,
    pub select_chain: Option<PhantomData<SC>>,
    pub persistent_data: aux_schema::PersistentData<Block>,
}

impl<Block: BlockT, C, SC> LinkHalf<Block, C, SC> {
    /// Get the shared authority set.
    pub fn shared_authority_set(&self) -> &SharedAuthoritySet<NumberFor<Block>> {
        &self.persistent_data.authority_set
    }
}

/// Provider for the Hotstuff authority set configured on the genesis block.
pub trait AuthoritySetProvider<Block: BlockT> {
    /// Get the authority set at the genesis block.
    fn get(&self, block_id: BlockId<Block>) -> Result<Vec<AuthorityId>, ClientError>;
}

impl<Block: BlockT, E, Client> AuthoritySetProvider<Block> for Arc<Client>
where
    E: CallExecutor<Block>,
    Client: ExecutorProvider<Block, Executor = E> + HeaderBackend<Block>,
{
    fn get(&self, block_id: BlockId<Block>) -> Result<Vec<AuthorityId>, ClientError> {
        let runtime_authorities: Vec<RuntimeAuthorityId> = self.executor()
            .call(
                self.expect_block_hash_from_id(&block_id)?,
                "HotstuffApi_authorities",
                &[],
                ExecutionStrategy::NativeElseWasm,
                CallContext::Offchain,
            )
            .and_then(|call_result| {
                Decode::decode(&mut &call_result[..]).map_err(|err| {
                    ClientError::CallResultDecode(
                        "failed to decode hotstuff authorities set proof",
                        err,
                    )
                })
            })?;
        Ok(runtime_authorities.into_iter().map(|runtime_authority| runtime_authority.into()).collect())
    }
}
