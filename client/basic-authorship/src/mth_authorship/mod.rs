pub mod mth_authorship;
pub mod merge_system;

use std::collections::BTreeMap;
use codec::Decode;
use sp_api::ApiExt;
use sp_spot_api::SpotRuntimeApi;
use sp_runtime::traits::Block as BlockT;
use sp_state_machine::{MergeChange, OverlayedEntry, StorageKey, StorageValue};
pub use merge_system::*;
pub use mth_authorship::*;

/// Extended merge help trat for better handle state merge.
pub trait MultiThreadBlockBuilder<B, Block: BlockT, Api>: MergeChange<StorageKey, Option<StorageValue>> + Default {
    /// Pre handle the state for future [MergeChange::merge_changes]
    fn prepare(&mut self, _backend: &B, _parent: &Block::Hash, _api: &Api) {}

    /// Copy a new Self for another spawn merge work.
    fn copy_state(&self) -> Self;
}

/// Special trait for mth authorship used to generate extend extrinsic before finalize.
pub trait ExtendExtrinsic<Extrinsic: codec::Encode> {
    /// Input runtime api with latest state.
    /// Return extrinsic with group_info
    fn extend_extrinsic<Block: BlockT, Api: ApiExt<Block> + SpotRuntimeApi<Block>>(api: &Api, hash: <Block as BlockT>::Hash) -> Vec<(Extrinsic, Vec<Vec<u8>>)>;
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
