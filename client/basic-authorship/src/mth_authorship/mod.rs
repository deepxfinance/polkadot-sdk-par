pub mod mth_authorship;
pub mod merge_system;
pub mod groups;
pub mod executor;
pub mod execute_info;
pub mod oracle;

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use codec::Decode;
use sp_api::{ApiExt, HeaderT};
use sp_consensus::Proposal;
use sp_spot_api::SpotRuntimeApi;
use sp_perp_api::PerpRuntimeApi;
use sp_runtime::traits::Block as BlockT;
use sp_state_machine::{MergeChange, OverlayedEntry, StorageKey, StorageValue};
pub use merge_system::*;
pub use mth_authorship::*;
use sp_core::ExecutionContext;
use sp_inherents::InherentData;
use sp_runtime::Digest;
use crate::GroupInfo;
use crate::mth_authorship::execute_info::BlockExecuteInfo;

/// Extended merge help trat for better handle state merge.
pub trait MultiThreadBlockBuilder<B, Block: BlockT, Api>: MergeChange<StorageKey, Option<StorageValue>> + Default {
    /// Pre handle the state for future [MergeChange::merge_changes]
    fn prepare(&mut self, _backend: &Arc<B>, _parent: &Block::Hash, _api: &Api) {}

    /// Copy a new Self for another spawn merge work.
    fn copy_state(&self) -> Self;
}

#[async_trait::async_trait]
pub trait GroupTransaction<Block: BlockT> {
    async fn extrinsic(
        &self,
        parent: <Block::Header as HeaderT>::Number,
        // time limit for waiting transaction_pool response before start get transaction.
        wait_pool: Duration,
        // time limit to get transactions from pool(this includes `wait_pool`).
        max_time: Duration,
        init_block_size: Option<usize>,
        init_proof_size: Option<usize>,
        filter: HashSet<Block::Hash>,
    ) -> Result<(Vec<Vec<Block::Extrinsic>>, Vec<Block::Extrinsic>, GroupInfo), String>;
}

/// Get extrinsic by grouped.
#[async_trait::async_trait]
pub trait BlockPropose<Block: BlockT> {
    type Transaction;
    type Proof;
    type Error;

    async fn propose_block(
        self,
        source: &str,
        parent_hash: Block::Hash,
        parent_number: <<Block as BlockT>::Header as HeaderT>::Number,
        max_duration: Duration,
        linear_execute_time: Duration,
        estimated_merge_time: Duration,
        inherent_data: InherentData,
        inherent_digests: Digest,
        extrinsic: (Vec<Vec<Block::Extrinsic>>, Vec<Block::Extrinsic>),
        round_tx: usize,
        allow_extend: bool,
        merge_in_thread_order: bool,
        limit_execution_time: bool,
    ) -> Result<
        (Proposal<Block, Self::Transaction, Self::Proof>, BlockExecuteInfo<Block>),
        Self::Error
    >;

    async fn execute_block_for_import(
        &self,
        source: &str,
        context: ExecutionContext,
        parent_hash: Block::Hash,
        parent_number: <<Block as BlockT>::Header as HeaderT>::Number,
        inherent_digests: Digest,
        extrinsic: Vec<Block::Extrinsic>,
        groups: Vec<u32>,
        round_tx: usize,
        linear_execute_time: Duration,
        estimated_merge_time: Duration,
        allow_extend: bool,
        merge_in_thread_order: bool,
        limit_execution_time: bool,
    ) -> Result<
        (Proposal<Block, Self::Transaction, Self::Proof>, BlockExecuteInfo<Block>),
        Self::Error
    >;
}

/// Special trait for mth authorship used to generate extend extrinsic before finalize.
pub trait ExtendExtrinsic {
    /// Input runtime api with latest state.
    /// Return extrinsic with group_info
    fn extend_extrinsic<Block: BlockT, Api: ApiExt<Block> + SpotRuntimeApi<Block> + PerpRuntimeApi<Block>>(api: &Api, hash: <Block as BlockT>::Hash) -> Vec<Block::Extrinsic>;
}

pub struct EmptyExtendTx;

impl ExtendExtrinsic for EmptyExtendTx {
    fn extend_extrinsic<Block: BlockT, Api: ApiExt<Block> + SpotRuntimeApi<Block> + PerpRuntimeApi<Block>>(_api: &Api, _hash: <Block as BlockT>::Hash) -> Vec<Block::Extrinsic> {
        Vec::new()
    }
}

pub fn parse_entry_value<T: codec::Decode>(entry: &OverlayedEntry<Option<StorageValue>>) -> Option<T> {
    entry.value_ref().as_ref().map(|v| Decode::decode(&mut v.as_slice()).unwrap())
}

pub fn get_map_value<T: codec::Decode>(map: &BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>, key: &Vec<u8>) -> Option<T> {
    map.get(key)
        .map(|e| e.value_ref().as_ref().map(|v| codec::Decode::decode(&mut v.as_slice()).unwrap()))
        .unwrap_or_default()
}

pub fn get_top_value<Block: BlockT, Api: ApiExt<Block>, T: Decode>(api: &Api, key: &Vec<u8>) -> Option<T> {
    api
        .get_top_change(key)
        .map(|data| data.map(|d| Decode::decode(&mut d.as_slice()).unwrap()))
        .unwrap_or_default()
}
