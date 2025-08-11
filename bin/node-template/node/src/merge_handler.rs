use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use codec::{Encode, Decode};
use frame_benchmarking::log;
use node_template_runtime::RuntimeEvent;
use sc_basic_authorship::{DefaultMergeHandler, MultiThreadBlockBuilder};
use sc_client_api::Backend;
use sp_api::{MergeChange, OverlayedEntry, StateBackend, StorageKey, StorageValue};
use sp_runtime::traits::Block as BlockT;

pub fn storage_prefix(pallet_name: &[u8], storage_name: &[u8]) -> [u8; 32] {
    let pallet_hash = sp_io::hashing::twox_128(pallet_name);
    let storage_hash = sp_io::hashing::twox_128(storage_name);

    let mut final_key = [0u8; 32];
    final_key[..16].copy_from_slice(&pallet_hash);
    final_key[16..].copy_from_slice(&storage_hash);

    final_key
}

#[derive(Default)]
pub struct MergeHandler {
    pub cache_state: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    pub default: DefaultMergeHandler<RuntimeEvent>,
}

impl<B, Block> MultiThreadBlockBuilder<B, Block> for MergeHandler
where
    B: Backend<Block>,
    Block: BlockT,
{
    fn pre_handle(&self, backend: &B, parent: &Block::Hash) {
        let parent_state = backend.state_at(*parent).unwrap();

        // query `balances_total_issuance`
        let balances_total_issuance_key = storage_prefix(b"Balances", b"TotalIssuance");
        let balances_total_issuance = parent_state
            .storage(balances_total_issuance_key.as_ref())
            .unwrap_or_default();

        let mut cache_state = self.cache_state.lock().unwrap();
        // store initial `balances_total_issuance`
        if let Some(balances_total_issuance) = balances_total_issuance {
            cache_state.insert(balances_total_issuance_key.to_vec(), balances_total_issuance);
        }
    }
}

impl MergeChange<StorageKey, Option<StorageValue>> for MergeHandler {
    fn merge_changes(
        &self,
        local: &mut BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>,
        other: &mut BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>,
    ) -> Vec<StorageKey> {
        const EXTRINSIC_INDEX: [u8; 16] = [58, 101, 120, 116, 114, 105, 110, 115, 105, 99, 95, 105, 110, 100, 101, 120];

        let mut duplicate_keys = Vec::new();
        let cache_state = self.cache_state.lock().unwrap();
        let offset: u32 = local.get(&EXTRINSIC_INDEX.to_vec())
            .map(|e| e.value_ref().clone().map(|v| Decode::decode(&mut v.as_slice()).unwrap()))
            .unwrap_or_default()
            .unwrap_or_default();
        // Custom merge for "Balances TotalIssuance"
        let balances_total_issuance_key = storage_prefix(b"Balances", b"TotalIssuance");
        let init_balances_total_issuance: Option<u128> = cache_state
            .get(balances_total_issuance_key.as_slice())
            .cloned()
            .map(|data| Decode::decode(&mut data.as_slice()).unwrap());
        if let Some(init_balances_total_issuance) = init_balances_total_issuance {
            if let Some(entry_other) = other.remove(balances_total_issuance_key.as_slice()) {
                let other_total_issuance: u128 = entry_other
                    .value_ref()
                    .as_ref()
                    .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                    .unwrap_or_default();
                let extrinsics = entry_other.extrinsics();
                let final_extrinsic = extrinsics.last().cloned().map(|e| e + offset);
                if let Some(entry_local) = local.get_mut(balances_total_issuance_key.as_slice()) {
                    let local_total_issuance: u128 = entry_local
                        .value_ref()
                        .as_ref()
                        .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                        .unwrap_or_default();
                    let last_sum = local_total_issuance + other_total_issuance;
                    let double_init = init_balances_total_issuance * 2;
                    let final_total_issuance = if double_init >= last_sum {
                        init_balances_total_issuance - (double_init - last_sum)
                    } else {
                        init_balances_total_issuance + (last_sum - double_init)
                    };
                    log::debug!(target: "develop-template", "merge Balances TotalIssuance init: {init_balances_total_issuance} local: {local_total_issuance}, other: {other_total_issuance}, final: {final_total_issuance}, extrinsic: {final_extrinsic:?}");
                    entry_local.set(Some(final_total_issuance.encode()), false, final_extrinsic);
                } else {
                    log::debug!(target: "develop-template", "merge Balances TotalIssuance init: {init_balances_total_issuance} local: None, other: {other_total_issuance}, final: {other_total_issuance}, extrinsic: {final_extrinsic:?}");
                    let mut new_entry = OverlayedEntry::default();
                    new_entry.set(Some(other_total_issuance.encode()), true, final_extrinsic);
                    local.insert(balances_total_issuance_key.to_vec(), new_entry);
                }
            }
        }

        duplicate_keys.extend(self.default.merge_changes(local, other));
        duplicate_keys
    }
}

#[test]
fn test_storage_key() {
    let key = storage_prefix(b"System", b"Account");
    println!("System Account: {key:?}");
    let key = storage_prefix(b"System", b"ExtrinsicCount");
    println!("System ExtrinsicCount: {key:?}");
    let key = storage_prefix(b"System", b"BlockWeight");
    println!("System BlockWeight: {key:?}");
    let key = storage_prefix(b"System", b"AllExtrinsicsLen");
    println!("System AllExtrinsicsLen: {key:?}");
    let key = storage_prefix(b"System", b"BlockHash");
    println!("System BlockHash: {key:?}");
    let key = storage_prefix(b"System", b"ExtrinsicData");
    println!("System ExtrinsicData: {key:?}");
    let key = storage_prefix(b"System", b"Number");
    println!("System Number: {key:?}");
    let key = storage_prefix(b"System", b"ParentHash");
    println!("System ParentHash: {key:?}");
    let key = storage_prefix(b"System", b"Digest");
    println!("System Digest: {key:?}");
    let key = storage_prefix(b"System", b"Events");
    println!("System Events: {key:?}");
    let key = storage_prefix(b"System", b"EventCount");
    println!("System EventCount: {key:?}");
    let key = storage_prefix(b"System", b"EventTopics");
    println!("System EventTopics: {key:?}");
    let key = storage_prefix(b"System", b"ExecutionPhase");
    println!("System ExecutionPhase: {key:?}");
    let key = storage_prefix(b"Babe", b"GenesisSlot");
    println!("Babe GenesisSlot: {key:?}");
    let key = storage_prefix(b"Babe", b"Lateness");
    println!("Babe Lateness: {key:?}");
    let key = storage_prefix(b"Babe", b"CurrentSlot");
    println!("Babe CurrentSlot: {key:?}");
    let key = storage_prefix(b"Babe", b"Initialized");
    println!("Babe Initialized: {key:?}");
    println!("well_known_keys EXTRINSIC_INDEX: {:?}", b":extrinsic_index");
    println!("well_known_keys INTRABLOCK_ENTROPY: {:?}", b":intrablock_entropy");

    let key = storage_prefix(b"Timestamp", b"Now");
    println!("Timestamp Now: {key:?}");
    let key = storage_prefix(b"Timestamp", b"DidUpdate");
    println!("Timestamp DidUpdate: {key:?}");
    let key = storage_prefix(b"Aura", b"CurrentSlot");
    println!("Aura CurrentSlot: {key:?}");

    let key = storage_prefix(b"Balances", b"TotalIssuance");
    println!("Balances TotalIssuance: {key:?}");
    let key = storage_prefix(b"Balances", b"InactiveIssuance");
    println!("Balances InactiveIssuance: {key:?}");
    let key = storage_prefix(b"Balances", b"Account");
    println!("Balances Account: {key:?}");
    let key = storage_prefix(b"Balances", b"Locks");
    println!("Balances Locks: {key:?}");
    let key = storage_prefix(b"Balances", b"Reserves");
    println!("Balances Reserves: {key:?}");
    let key = storage_prefix(b"Balances", b"Holds");
    println!("Balances Holds: {key:?}");
    let key = storage_prefix(b"Balances", b"Freezes");
    println!("Balances Freezes: {key:?}");

    let key = storage_prefix(b"Grandpa", b"CurrentSlot");
    println!("Grandpa CurrentSlot: {key:?}");
    let key = storage_prefix(b"TransactionPayment", b"CurrentSlot");
    println!("TransactionPayment CurrentSlot: {key:?}");
    let key = storage_prefix(b"Sudo", b"CurrentSlot");
    println!("Sudo CurrentSlot: {key:?}");
    let key = storage_prefix(b"TemplateModule", b"CurrentSlot");
    println!("TemplateModule CurrentSlot: {key:?}");
}
