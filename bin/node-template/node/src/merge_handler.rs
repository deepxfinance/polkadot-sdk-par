use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;
use node_template_runtime::RuntimeEvent;
use sc_basic_authorship::{MergeSystem, MultiThreadBlockBuilder};
use sc_client_api::Backend;
use sp_api::{ApiExt, MergeChange, OverlayCache, OverlayedChanges, OverlayedEntry, StorageKey, StorageValue};
use sp_runtime::traits::Block as BlockT;
use crate::merge_backend::MergeBackend;
use crate::merge_balances::MergeBalances;

pub struct MergeHandler<B, Block: BlockT> {
    pub balances: MergeBalances,
    pub system: MergeSystem<RuntimeEvent>,
    pub backend: MergeBackend<B, Block>,
    phantom: PhantomData<Block>,
}

impl<B, Block: BlockT> Default for MergeHandler<B, Block> {
    fn default() -> Self {
        Self {
            balances: MergeBalances::default(),
            system: MergeSystem::default(),
            backend: MergeBackend::default(),
            phantom: PhantomData,
        }
    }
}

impl<B, Block, Api: ApiExt<Block>> MultiThreadBlockBuilder<B, Block, Api> for MergeHandler<B, Block>
where
    B: Backend<Block>,
    Block: BlockT,
{
    fn prepare(&mut self, backend: &Arc<B>, parent: &Block::Hash, api: &Api) {
        <MergeBalances as MultiThreadBlockBuilder<B, Block, Api>>::prepare(&mut self.balances, backend, parent, api);
        <MergeSystem<RuntimeEvent> as MultiThreadBlockBuilder<B, Block, Api>>::prepare(&mut self.system, backend, parent, api);
        <MergeBackend<B, Block> as MultiThreadBlockBuilder<B, Block, Api>>::prepare(&mut self.backend, backend, parent, api);
    }

    fn prepare_thread_in_order(&mut self, thread: usize, txs: usize, cache: &mut OverlayCache, changes: &mut OverlayedChanges) {
        <MergeBalances as MultiThreadBlockBuilder<B, Block, Api>>::prepare_thread_in_order(&mut self.balances, thread, txs, cache, changes);
        <MergeSystem<RuntimeEvent> as MultiThreadBlockBuilder<B, Block, Api>>::prepare_thread_in_order(&mut self.system, thread, txs, cache, changes);
        <MergeBackend<B, Block> as MultiThreadBlockBuilder<B, Block, Api>>::prepare_thread_in_order(&mut self.backend, thread, txs, cache, changes);
    }

    fn copy_state(&self) -> Self {
        Self {
            balances: <MergeBalances as MultiThreadBlockBuilder<B, Block, Api>>::copy_state(&self.balances),
            system: <MergeSystem<RuntimeEvent> as MultiThreadBlockBuilder<B, Block, Api>>::copy_state(&self.system),
            backend: <MergeBackend<B, Block> as MultiThreadBlockBuilder<B, Block, Api>>::copy_state(&self.backend),
            phantom: PhantomData,
        }
    }
}

impl<B: Backend<Block>, Block: BlockT> MergeChange<StorageKey, Option<StorageValue>> for MergeHandler<B, Block> {
    fn merge_changes(
        &self,
        local: &mut BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>,
        other: &mut BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>,
        in_order: bool,
    ) -> Vec<StorageKey> {
        let mut duplicate_keys = Vec::new();
        duplicate_keys.extend(self.balances.merge_changes(local, other, in_order));
        duplicate_keys.extend(self.system.merge_changes(local, other, in_order));
        duplicate_keys
    }

    fn finalize_merge(&self, map: &mut BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>) {
        self.balances.finalize_merge(map);
        self.system.finalize_merge(map);
    }

    fn merge_weight(changes: &BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>) -> u32 {
        let balances_weight = <MergeBalances as MergeChange<StorageKey, Option<StorageValue>>>::merge_weight(changes);
        let system_weight = <MergeSystem<RuntimeEvent> as MergeChange<StorageKey, Option<StorageValue>>>::merge_weight(changes);
        // let backend_weight = <MergeBackend<B, Block> as MergeChange<StorageKey, Option<StorageValue>>>::merge_weight(changes);
        balances_weight + system_weight
    }
}

#[test]
fn test_storage_key() {
    pub fn storage_prefix(pallet_name: &[u8], storage_name: &[u8]) -> [u8; 32] {
        let pallet_hash = sp_io::hashing::twox_128(pallet_name);
        let storage_hash = sp_io::hashing::twox_128(storage_name);

        let mut final_key = [0u8; 32];
        final_key[..16].copy_from_slice(&pallet_hash);
        final_key[16..].copy_from_slice(&storage_hash);

        final_key
    }

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
    let key = storage_prefix(b"Ethereum", b"Pending");
    println!("Ethereum Pending: {key:?}");
}
