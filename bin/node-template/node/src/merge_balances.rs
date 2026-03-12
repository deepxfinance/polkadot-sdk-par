use std::collections::BTreeMap;
use std::sync::Arc;
use codec::Encode;
use frame_benchmarking::log;
use sc_basic_authorship::{get_map_value, parse_entry_value, MultiThreadBlockBuilder};
use sc_client_api::Backend;
use sp_api::{ApiExt, MergeChange, StateBackend, OverlayedEntry, StorageKey, StorageValue, Changes};
use sp_runtime::traits::Block as BlockT;

const EXTRINSIC_INDEX: [u8; 16] = [58, 101, 120, 116, 114, 105, 110, 115, 105, 99, 95, 105, 110, 100, 101, 120];
const BALANCE_TOTAL_ISSUANCE: [u8; 32] = [194, 38, 18, 118, 204, 157, 31, 133, 152, 234, 75, 106, 116, 177, 92, 47, 87, 200, 117, 228, 207, 247, 65, 72, 228, 98, 143, 38, 75, 151, 76, 128];

#[derive(Default, Clone)]
pub struct MergeBalances {
    pub init_total_issuance: u128,
}

impl<B, Block, Api: ApiExt<Block>> MultiThreadBlockBuilder<B, Block, Api> for MergeBalances
where
    B: Backend<Block>,
    Block: BlockT,
{
    fn prepare(&mut self, backend: &Arc<B>, parent: &Block::Hash, _api: &Api) {
        let parent_state = backend.state_at(*parent).unwrap();
        self.init_total_issuance =  parent_state
            .storage(&BALANCE_TOTAL_ISSUANCE.to_vec())
            .map(|v| v.map(|data| data.get_t()))
            .unwrap_or_default()
            .unwrap_or_default()
            .unwrap_or_default();
    }

    fn copy_state(&self) -> Self {
        self.clone()
    }
}

impl MergeChange<StorageKey, Option<StorageValue>> for MergeBalances {
    fn merge_changes(&self, local: &mut Changes, other: &mut Changes, _in_order: bool) -> Result<Changes, Vec<StorageKey>> {
        let mut changes = Changes::default();
        let offset: u32 = get_map_value(local, &EXTRINSIC_INDEX.to_vec()).unwrap_or_default();
        // Custom merge for "Balances TotalIssuance"
        let init_total_issuance = self.init_total_issuance;
        if let Some(entry_other) = other.remove(&BALANCE_TOTAL_ISSUANCE.to_vec()) {
            let other_total_issuance: u128 = parse_entry_value(&entry_other).unwrap_or_default();
            let extrinsics = entry_other.extrinsics();
            let final_extrinsic = extrinsics.last().cloned().map(|e| e + offset);
            if let Some(entry_local) = local.get_mut(&BALANCE_TOTAL_ISSUANCE.to_vec()) {
                let local_total_issuance: u128 = parse_entry_value(&entry_local).unwrap_or_default();
                let last_sum = local_total_issuance + other_total_issuance;
                let double_init = init_total_issuance * 2;
                let final_total_issuance = if double_init >= last_sum {
                    init_total_issuance - (double_init - last_sum)
                } else {
                    init_total_issuance + (last_sum - double_init)
                };
                log::trace!(target: "merge_balances", "merge Balances TotalIssuance init: {init_total_issuance} local: {local_total_issuance}, other: {other_total_issuance}, final: {final_total_issuance}, extrinsic: {final_extrinsic:?}");
                entry_local.set(Some(final_total_issuance.encode().into()), false, final_extrinsic);
                // update conflict changes
                let mut new_entry = OverlayedEntry::default();
                new_entry.set(Some(final_total_issuance.encode().into()), true, final_extrinsic);
                changes.insert(BALANCE_TOTAL_ISSUANCE.to_vec(), new_entry);
            } else {
                log::trace!(target: "merge_balances", "merge Balances TotalIssuance init: {init_total_issuance} local: None, other: {other_total_issuance}, final: {other_total_issuance}, extrinsic: {final_extrinsic:?}");
                let mut new_entry = OverlayedEntry::default();
                new_entry.set(Some(other_total_issuance.encode().into()), true, final_extrinsic);
                local.insert(BALANCE_TOTAL_ISSUANCE.to_vec(), new_entry);
            }
        }

        Ok(changes)
    }

    fn finalize_merge(&self, _map: &mut BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>) {}

    fn merge_weight(_changes: &BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>) -> u32 {
        1
    }
}
