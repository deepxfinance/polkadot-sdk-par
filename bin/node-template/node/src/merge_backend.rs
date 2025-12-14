use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;
use codec::Decode;
use sc_basic_authorship::{MultiThreadBlockBuilder};
use sc_client_api::Backend;
use sp_api::{ApiExt, MergeChange, StateBackend, OverlayedEntry, StorageKey, StorageValue};
use sp_runtime::traits::Block as BlockT;

const BALANCE_TOTAL_ISSUANCE: [u8; 32] = [194, 38, 18, 118, 204, 157, 31, 133, 152, 234, 75, 106, 116, 177, 92, 47, 87, 200, 117, 228, 207, 247, 65, 72, 228, 98, 143, 38, 75, 151, 76, 128];

#[derive(Clone)]
pub struct MergeBackend<B, Block: BlockT> {
    pub backend: Option<Arc<B>>,
    pub parent: Option<Block::Hash>,
    phantom: PhantomData<Block>,
}

impl<B, Block: BlockT> Default for MergeBackend<B, Block> {
    fn default() -> Self {
        Self {
            backend: None,
            parent: None,
            phantom: PhantomData,
        }
    }
}

impl<B, Block, Api: ApiExt<Block>> MultiThreadBlockBuilder<B, Block, Api> for MergeBackend<B, Block>
where
    B: Backend<Block>,
    Block: BlockT,
{
    fn prepare(&mut self, backend: &Arc<B>, parent: &Block::Hash, _api: &Api) {
        self.backend = Some(backend.clone());
        self.parent = Some(*parent);
    }

    fn copy_state(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            parent: self.parent.clone(),
            phantom: PhantomData,
        }
    }
}

impl<B: Backend<Block>, Block: BlockT> MergeChange<StorageKey, Option<StorageValue>> for MergeBackend<B, Block> {
    fn merge_changes(
        &self,
        _local: &mut BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>,
        _other: &mut BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>,
        _in_order: bool,
    ) -> Vec<StorageKey> {
        let _backend_init_total_issuance: u128 = self.backend.as_ref().map(|b| {
            self.parent.map(|h|
                b.state_at(h).unwrap().storage(&BALANCE_TOTAL_ISSUANCE.to_vec())
                    .map(|v| v.map(|data| Decode::decode(&mut data.as_slice()).unwrap()))
                    .unwrap_or_default()
                    .unwrap_or_default()
            )
        })
            .unwrap_or_default()
            .unwrap_or_default();
        Vec::new()
    }

    fn finalize_merge(&self, _map: &mut BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>) {}

    fn merge_weight(_changes: &BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>) -> u32 {
        1
    }
}
