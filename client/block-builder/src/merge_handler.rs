//! Basic requests and implements for multi thread block builder.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Mutex;
use codec::{Decode, Encode};
use sp_runtime::Digest;
use sp_runtime::traits::Block as BlockT;
use sp_state_machine::{MergeChange, OverlayedEntry, StorageKey, StorageValue};

/// Extended merge help trat for better handle state merge.
pub trait MultiThreadBlockBuilder<B, Block: BlockT>: MergeChange<StorageKey, Option<StorageValue>> + Default {
    /// Pre handle the state for future [MergeChange::merge_changes]
    fn pre_handle(&self, _backend: &B, _parent: &Block::Hash, _init_change: Vec<(Vec<u8>, Option<Vec<u8>>)>) {}
}

/// Default type implement MultiThreadBlockBuilder trait.
pub struct DefaultMergeHandler<RE> {
    cache_state: Mutex<HashMap<Vec<u8>, Option<Vec<u8>>>>,
    phantom: PhantomData<RE>,
}

impl<RE: Encode + Decode + Debug + Clone> Default for DefaultMergeHandler<RE> {
    fn default() -> Self {
        DefaultMergeHandler {
            cache_state: Default::default(),
            phantom: PhantomData,
        }
    }
}

impl<RE: Encode + Decode + Debug + Clone, B, Block: BlockT> MultiThreadBlockBuilder<B, Block> for DefaultMergeHandler<RE> {
    fn pre_handle(&self, _backend: &B, _parent: &Block::Hash, init_changes: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        let mut cache_state = self.cache_state.lock().unwrap();
        // store useful initial changes.
        for (key, value) in init_changes {
            cache_state.insert(key, value);
        }
    }
}

impl<RE: Encode + Decode + Debug + Clone> MergeChange<StorageKey, Option<StorageValue>> for DefaultMergeHandler<RE> {
    fn merge_changes(
        &self,
        local: &mut BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>,
        other: &mut BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>,
    ) -> Vec<StorageKey> {
        use sp_io::hashing::twox_64;

        const EXTRINSIC_INDEX: [u8; 16] = [58, 101, 120, 116, 114, 105, 110, 115, 105, 99, 95, 105, 110, 100, 101, 120];
        const SYSTEM_EXTRINSIC_COUNT: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 189, 192, 189, 48, 62, 152, 85, 129, 58, 168, 163, 13, 78, 252, 81, 18];
        const SYSTEM_BLOCK_WEIGHT: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 52, 171, 245, 203, 52, 214, 36, 67, 120, 205, 219, 241, 142, 132, 157, 150];
        const SYSTEM_ALL_EXTRINSICS_LEN: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 168, 109, 165, 169, 50, 104, 79, 25, 149, 57, 131, 111, 203, 140, 136, 111];
        const SYSTEM_DIGEST: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 153, 231, 249, 63, 198, 169, 143, 8, 116, 253, 5, 127, 17, 28, 77, 45];
        const SYSTEM_EVENT_COUNT: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 10, 152, 253, 190, 156, 230, 197, 88, 55, 87, 108, 96, 199, 175, 56, 80];
        const SYSTEM_EVENTS: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 128, 212, 30, 94, 22, 5, 103, 101, 188, 132, 97, 133, 16, 114, 201, 215];
        const SYSTEM_EXTRINSIC_DATA_PREFIX: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 223, 29, 174, 184, 152, 104, 55, 242, 28, 197, 209, 117, 150, 187, 120, 209];
        const SYSTEM_EXECUTION_PHASE: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 255, 85, 59, 90, 152, 98, 165, 22, 147, 157, 130, 179, 211, 216, 102, 26];

        let mut duplicate_keys = vec![];
        let offset: u32 = local.get(&EXTRINSIC_INDEX.to_vec())
            .map(|e| e.value_ref().clone().map(|v| Decode::decode(&mut v.as_slice()).unwrap()))
            .unwrap_or_default()
            .unwrap_or_default();
        // update well_known_keys::EXTRINSIC_INDEX u32
        if let Some(entry_other) = other.remove(&EXTRINSIC_INDEX.to_vec()) {
            let index_other: u32 = entry_other.value_ref().clone().map(|v| Decode::decode(&mut v.as_slice()).unwrap()).unwrap_or(0);
            let final_index = offset.saturating_add(index_other);
            log::debug!(target: "develop", "merge EXTRINSIC_INDEX local: {offset}, other: {index_other}, final: {final_index}, extrinsic: {final_index}");
            if let Some(entry_local) = local.get_mut(&EXTRINSIC_INDEX.to_vec()) {
                entry_local.set(Some(final_index.encode()), false, Some(final_index));
            }
        }

        // update "System ExtrinsicCount" u32
        // This is actually updated at pallet_system::finalize_block, we do not need to handle this when merge(which is before finalize block).
        if let Some(entry_other) = other.remove(&SYSTEM_EXTRINSIC_COUNT.to_vec()) {
            let other_count: u32 = entry_other
                .value_ref()
                .as_ref()
                .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                .unwrap_or_default();
            if let Some(entry_local) = local.get_mut(&SYSTEM_EXTRINSIC_COUNT.to_vec()) {
                let local_count: u32 = entry_local
                    .value_ref()
                    .as_ref()
                    .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                    .unwrap_or_default();
                let final_count = other_count.saturating_add(local_count);
                log::debug!(target: "develop", "merge ExtrinsicCount local: {local_count}, other: {other_count}, final: {final_count}, extrinsic: {final_count}");
                entry_local.set(Some(final_count.encode()), false, Some(final_count));
            } else {
                log::debug!(target: "develop", "merge ExtrinsicCount local: None, other: {other_count}, final: {other_count}, extrinsic: {other_count}");
                let mut new_entry = OverlayedEntry::default();
                new_entry.set(Some(other_count.encode()), true, Some(other_count));
                local.insert(SYSTEM_EXTRINSIC_COUNT.to_vec(), new_entry);
            }
        }

        // update "System AllExtrinsicsLen" u32
        if let Some(entry_other) = other.remove(&SYSTEM_ALL_EXTRINSICS_LEN.to_vec()) {
            let other_len: u32 = entry_other
                .value_ref()
                .as_ref()
                .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                .unwrap_or_default();
            let extrinsics = entry_other.extrinsics();
            let final_extrinsic = extrinsics.last().cloned().map(|e| e.saturating_add(offset));
            if let Some(entry_local) = local.get_mut(&SYSTEM_ALL_EXTRINSICS_LEN.to_vec()) {
                let local_len: u32 = entry_local
                    .value_ref()
                    .as_ref()
                    .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                    .unwrap_or_default();
                let final_len = local_len.saturating_add(other_len);
                log::debug!(target: "develop", "merge AllExtrinsicsLen local: {local_len}, other: {other_len}, final: {final_len}, extrinsic: {final_extrinsic:?}");
                entry_local.set(Some(final_len.encode()), false, final_extrinsic);
            } else {
                log::debug!(target: "develop", "merge AllExtrinsicsLen local: None, other: {other_len}, final: {other_len}, extrinsic: {final_extrinsic:?}");
                let mut new_entry = OverlayedEntry::default();
                new_entry.set(Some(other_len.encode()), true, final_extrinsic);
                local.insert(SYSTEM_ALL_EXTRINSICS_LEN.to_vec(), new_entry);
            }
        }

        // update "System Digest"
        if let Some(entry_other) = other.remove(&SYSTEM_DIGEST.to_vec()) {
            let other_digest: Digest = entry_other
                .value_ref()
                .as_ref()
                .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                .unwrap_or_default();
            let extrinsics = entry_other.extrinsics();
            let final_extrinsic = extrinsics.last().cloned().map(|e| e.saturating_add(offset));
            if let Some(entry_local) = local.get_mut(&SYSTEM_DIGEST.to_vec()) {
                let local_digest: Digest = entry_local
                    .value_ref()
                    .as_ref()
                    .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                    .unwrap_or_default();
                let mut final_digest = local_digest.clone();
                final_digest.logs.extend_from_slice(other_digest.logs.as_slice());
                log::debug!(target: "develop", "merge AllExtrinsicsLen local: {local_digest:?}, other: {other_digest:?}, final: {final_digest:?}, extrinsic: {final_extrinsic:?}");
                entry_local.set(Some(final_digest.encode()), false, final_extrinsic);
            } else {
                log::debug!(target: "develop", "merge AllExtrinsicsLen local: None, other: {other_digest:?}, final: {other_digest:?}, extrinsic: {final_extrinsic:?}");
                let mut new_entry = OverlayedEntry::default();
                new_entry.set(Some(other_digest.encode()), true, final_extrinsic);
                local.insert(SYSTEM_DIGEST.to_vec(), new_entry);
            }
        }
        
        
        // update "System BlockWeight"
        if let Some(entry_other) = other.remove(&SYSTEM_BLOCK_WEIGHT.to_vec()) {
            #[derive(Encode, Decode, Debug, Clone, Default)]
            pub struct Weight {
                #[codec(compact)]
                ref_time: u64,
                #[codec(compact)]
                proof_size: u64,
            }

            #[derive(Encode, Decode, Debug, Clone, Default)]
            pub struct PerDispatchClass<T> {
                normal: T,
                operational: T,
                mandatory: T,
            }

            let other_weight: PerDispatchClass<Weight> = entry_other
                .value_ref()
                .as_ref()
                .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                .unwrap_or_default();
            let extrinsics = entry_other.extrinsics();
            let final_extrinsic = extrinsics.last().cloned().map(|e| e.saturating_add(offset));
            if let Some(entry_local) = local.get_mut(&SYSTEM_BLOCK_WEIGHT.to_vec()) {
                if entry_local.value_ref() == entry_other.value_ref() {
                    log::debug!(target: "develop", "merge BlockWeight local same with other, not merge");
                } else {
                    let local_weight: PerDispatchClass<Weight> = entry_local
                        .value_ref()
                        .as_ref()
                        .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                        .unwrap_or_default();
                    let mut final_weight = local_weight.clone();
                    final_weight.normal.ref_time = final_weight.normal.ref_time.saturating_add(other_weight.normal.ref_time);
                    final_weight.normal.proof_size = final_weight.normal.proof_size.saturating_add(other_weight.normal.proof_size);
                    final_weight.operational.ref_time = final_weight.operational.ref_time.saturating_add(other_weight.operational.ref_time);
                    final_weight.operational.proof_size = final_weight.operational.proof_size.saturating_add(other_weight.operational.proof_size);
                    final_weight.mandatory.ref_time = final_weight.mandatory.ref_time.saturating_add(other_weight.mandatory.ref_time);
                    final_weight.mandatory.proof_size = final_weight.mandatory.proof_size.saturating_add(other_weight.mandatory.proof_size);
                    let init_weight: Option<Option<PerDispatchClass<Weight>>> = self.cache_state.lock().unwrap()
                        .get(&SYSTEM_BLOCK_WEIGHT.to_vec())
                        .cloned()
                        .map(|data| data.map(|d| Decode::decode(&mut d.as_slice()).unwrap()));
                    // remove initial BlockWeight for every storage change
                    if let Some(Some(init_weight)) = init_weight.clone() {
                        final_weight.normal.ref_time = final_weight.normal.ref_time.saturating_sub(init_weight.normal.ref_time);
                        final_weight.normal.proof_size = final_weight.normal.proof_size.saturating_sub(init_weight.normal.proof_size);
                        final_weight.operational.ref_time = final_weight.operational.ref_time.saturating_sub(init_weight.operational.ref_time);
                        final_weight.operational.proof_size = final_weight.operational.proof_size.saturating_sub(init_weight.operational.proof_size);
                        final_weight.mandatory.ref_time = final_weight.mandatory.ref_time.saturating_sub(init_weight.mandatory.ref_time);
                        final_weight.mandatory.proof_size = final_weight.mandatory.proof_size.saturating_sub(init_weight.mandatory.proof_size);
                    }
                    log::debug!(target: "develop", "merge BlockWeight init: {init_weight:?}, local: {local_weight:?}, other: {other_weight:?}, final: {final_weight:?}, extrinsic: {final_extrinsic:?}");
                    entry_local.set(Some(final_weight.encode()), false, final_extrinsic);
                }
            } else {
                log::debug!(target: "develop", "merge BlockWeight local: None, other: {other_weight:?}, final: {other_weight:?}, extrinsic: {final_extrinsic:?}");
                let mut new_entry = OverlayedEntry::default();
                new_entry.set(Some(other_weight.encode()), true, final_extrinsic);
                local.insert(SYSTEM_BLOCK_WEIGHT.to_vec(), new_entry);
            }
        }

        // update "System EventCount" u32
        if let Some(entry_other) = other.remove(&SYSTEM_EVENT_COUNT.to_vec()) {
            let other_count: u32 = entry_other
                .value_ref()
                .as_ref()
                .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                .unwrap_or_default();
            let extrinsics = entry_other.extrinsics();
            let final_extrinsic = extrinsics.last().cloned().map(|e| e.saturating_add(offset));
            if let Some(entry_local) = local.get_mut(&SYSTEM_EVENT_COUNT.to_vec()) {
                let local_count: u32 = entry_local
                    .value_ref()
                    .as_ref()
                    .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                    .unwrap_or_default();
                let final_count = local_count.saturating_add(other_count);
                log::debug!(target: "develop", "merge EventCount local: {local_count}, other: {other_count}, final: {final_count}, extrinsic: {final_extrinsic:?}");
                entry_local.set(Some(final_count.encode()), false, final_extrinsic);
            } else {
                log::debug!(target: "develop", "merge EventCount local: None, other: {other_count}, final: {other_count}, extrinsic: {final_extrinsic:?}");
                let mut new_entry = OverlayedEntry::default();
                new_entry.set(Some(other_count.encode()), true, final_extrinsic);
                local.insert(SYSTEM_EVENT_COUNT.to_vec(), new_entry);
            }
        }

        // update "System Events"
        if let Some(entry_other) = other.remove(&SYSTEM_EVENTS.to_vec()) {
            pub use sp_core::hash::H256;

            #[derive(Encode, Decode, Clone, Debug)]
            pub struct EventRecord<RuntimeEvent> {
                pub phase: Phase,
                pub event: RuntimeEvent,
                pub topics: Vec<H256>,
            }

            let other_events: Vec<EventRecord<RE>> = entry_other
                .value_ref()
                .as_ref()
                .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                .unwrap_or_default();
            let extrinsics = entry_other.extrinsics();
            let final_extrinsic = extrinsics.last().cloned().map(|e| e.saturating_add(offset));
            if let Some(entry_local) = local.get_mut(&SYSTEM_EVENTS.to_vec()) {
                let local_events: Vec<EventRecord<RE>> = entry_local
                    .value_ref()
                    .as_ref()
                    .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                    .unwrap_or_default();
                let mut final_events = local_events.clone();
                for mut event in other_events.clone() {
                    if let Phase::ApplyExtrinsic(e) = &mut event.phase {
                        *e = e.saturating_add(offset);
                    }
                    final_events.push(event);
                }
                log::debug!(target: "develop", "merge Events local: {local_events:?}, other: {other_events:?}, final: {final_events:?}, extrinsic: {final_extrinsic:?}");
                entry_local.set(Some(final_events.encode()), false, final_extrinsic);
            } else {
                log::debug!(target: "develop", "merge Events local: None, other: {other_events:?}, final: {other_events:?}, extrinsic: {final_extrinsic:?}");
                let mut new_entry = OverlayedEntry::default();
                new_entry.set(Some(other_events.encode()), true, final_extrinsic);
                local.insert(SYSTEM_EVENTS.to_vec(), new_entry);
            }
        }

        #[derive(Encode, Decode, Debug, Clone)]
        pub enum Phase {
            ApplyExtrinsic(u32),
            Finalization,
            Initialization,
        }
        // update "System ExecutionPhase" u32
        if let Some(entry_other) = other.remove(&SYSTEM_EXECUTION_PHASE.to_vec()) {
            let other_phase: Phase = entry_other
                .value_ref()
                .as_ref()
                .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                .unwrap_or(Phase::ApplyExtrinsic(0));
            let extrinsics = entry_other.extrinsics();
            let final_extrinsic = extrinsics.last().cloned().map(|e| e.saturating_add(offset));
            if let Some(entry_local) = local.get_mut(&SYSTEM_EXECUTION_PHASE.to_vec()) {
                let local_phase: Phase = entry_local
                    .value_ref()
                    .as_ref()
                    .map(|data| Decode::decode(&mut data.as_slice()).unwrap())
                    .unwrap_or(Phase::ApplyExtrinsic(0));
                let final_phase = match (&local_phase, &other_phase) {
                    (Phase::ApplyExtrinsic(e1), Phase::ApplyExtrinsic(e2)) => {
                        if e1 > e2 {
                            Phase::ApplyExtrinsic(*e1)
                        } else {
                            Phase::ApplyExtrinsic(e2.saturating_add(offset))
                        }
                    }
                    (Phase::ApplyExtrinsic(_), Phase::Initialization) => local_phase.clone(),
                    (Phase::Finalization, _) => Phase::Finalization,
                    _ => match other_phase {
                        Phase::ApplyExtrinsic(e) => Phase::ApplyExtrinsic(e.saturating_add(offset)),
                        _ => other_phase.clone(),
                    },
                };
                log::debug!(target: "develop", "merge ExecutionPhase local: {local_phase:?}, other: {other_phase:?}, final: {final_phase:?}, extrinsic: {final_extrinsic:?}");
                entry_local.set(Some(final_phase.encode()), false, final_extrinsic);
            } else {
                let final_phase = match other_phase {
                    Phase::ApplyExtrinsic(e) => Phase::ApplyExtrinsic(e.saturating_add(offset)),
                    _ => other_phase.clone(),
                };
                log::debug!(target: "develop", "merge ExecutionPhase local: None, other: {other_phase:?}, final: {final_phase:?}, extrinsic: {final_extrinsic:?}");
                let mut new_entry = OverlayedEntry::default();
                new_entry.set(Some(final_phase.encode()), true, final_extrinsic);
                local.insert(SYSTEM_EXECUTION_PHASE.to_vec(), new_entry);
            }
        }

        let other_keys = other.keys().cloned().collect::<Vec<_>>();
        for key in other_keys {
            let entry_other = other.remove(&key).unwrap();
            if key.starts_with(SYSTEM_EXTRINSIC_DATA_PREFIX.as_slice()) {
                // update "System ExtrinsicData"
                let other_index_data = key[40..].to_vec();
                let other_index: u32 = Decode::decode(&mut other_index_data.as_slice()).unwrap();
                let new_index = other_index.saturating_add(offset);
                let new_index_data = new_index.encode();
                log::debug!(target: "develop", "merge System ExtrinsicData other: {other_index} -> {new_index}");
                let new_key = [SYSTEM_EXTRINSIC_DATA_PREFIX.to_vec(), twox_64(&new_index_data).to_vec(), new_index_data].concat();
                let mut new_entry = OverlayedEntry::default();
                new_entry.set(entry_other.value_ref().clone(), true, Some(new_index));
                local.insert(new_key, new_entry);
            } else if let Some(entry_local) = local.get_mut(&key) {
                let other_val = entry_other.value_ref();
                let other_exts: Vec<_> = entry_other.extrinsics().into_iter().collect();
                if entry_local.value_ref() == other_val
                    && entry_local.extrinsics().into_iter().collect::<Vec<u32>>() == other_exts {
                    continue;
                } else {
                    duplicate_keys.push(key);
                }
            } else {
                local.insert(key, entry_other);
            }
        }
        duplicate_keys
    }
}

#[cfg(test)]
mod tests {
    pub use sp_std::collections::btree_map::BTreeMap;
    use super::*;
    use sp_blockchain::HeaderBackend;
    use sp_state_machine::ExecutionStrategy;
    use substrate_test_runtime_client::{
        runtime::ExtrinsicBuilder, DefaultTestClientBuilderExt, TestClientBuilderExt,
    };

    #[test]
    fn two_threads_api_merge_check() {
        use std::ops::DerefMut;
        use substrate_test_runtime_client::{
            runtime::{
                currency::DOLLARS, Transfer, RuntimeEvent,
            },
            AccountKeyring,
        };
        use sp_io::hashing::{blake2_128, blake2_256, twox_128, twox_256, twox_64};

        env_logger::init();
        let thread1_extrinsics = vec![
            ExtrinsicBuilder::new_transfer(Transfer {
                from: AccountKeyring::Alice.into(),
                to: AccountKeyring::Ferdie.into(),
                amount: 2 * DOLLARS,
                nonce: 0,
            })
                .build(),
            ExtrinsicBuilder::new_transfer(Transfer {
                from: AccountKeyring::Ferdie.into(),
                to: AccountKeyring::Alice.into(),
                amount: 1 * DOLLARS,
                nonce: 0,
            })
                .build()
        ];
        let thread2_extrinsics = vec![
            ExtrinsicBuilder::new_transfer(Transfer {
                from: AccountKeyring::Bob.into(),
                to: AccountKeyring::Charlie.into(),
                amount: 3 * DOLLARS,
                nonce: 0,
            })
                .build(),
            ExtrinsicBuilder::new_transfer(Transfer {
                from: AccountKeyring::Charlie.into(),
                to: AccountKeyring::Bob.into(),
                amount: 1 * DOLLARS,
                nonce: 0,
            })
                .build()
        ];
        let builder = substrate_test_runtime_client::TestClientBuilder::new();
        let backend = builder.backend();
        let client = builder.build();
        let mut block_builder = BlockBuilder::new(
            &client,
            client.info().best_hash,
            client.info().best_number,
            RecordProof::Yes,
            Default::default(),
            &*backend,
        )
            .unwrap();
        let parent_hash = block_builder.parent_hash;
        // println!("ini: {}", block_builder.api.debug_info());
        println!("start raw_builder****************");
        let raw_builder_result = {
            println!("builder record_proof_size: {}", block_builder.api.proof_recorder().map(|pr| pr.estimate_encoded_size()).unwrap_or(0));
            for tx in [thread1_extrinsics.clone(), thread2_extrinsics.clone()].concat() {
                block_builder.push(tx).unwrap();
                println!("builder record_proof_size: {}", block_builder.api.proof_recorder().map(|pr| pr.estimate_encoded_size()).unwrap_or(0));
            }
            block_builder.build().unwrap()
        };

        println!("start two_threads****************");
        let two_threads_result = {
            let builder = substrate_test_runtime_client::TestClientBuilder::new()
                .set_execution_strategy(ExecutionStrategy::NativeElseWasm);
            let backend = builder.backend();
            let client = builder.build();
            let mut block_builder = BlockBuilder::new(
                &client,
                client.info().best_hash,
                client.info().best_number,
                RecordProof::Yes,
                Default::default(),
                &*backend,
            )
                .unwrap();

            let client1 = client.clone_for_execution();
            let backend1 = backend.clone();
            let extrinsics1 = thread1_extrinsics.clone();
            let task1 = std::thread::spawn(move || {
                let mut block_builder = BlockBuilder::new(
                    &client1,
                    client1.info().best_hash,
                    client1.info().best_number,
                    RecordProof::Yes,
                    Default::default(),
                    &*backend1,
                )
                    .unwrap();
                println!("builder1 record_proof_size: {}", block_builder.api.proof_recorder().map(|pr| pr.estimate_encoded_size()).unwrap_or(0));
                for extrinsic in extrinsics1 {
                    block_builder.push(extrinsic).unwrap();
                    println!("builder1 record_proof_size: {}", block_builder.api.proof_recorder().map(|pr| pr.estimate_encoded_size()).unwrap_or(0));
                }
                block_builder.api.take_all_changes()
            });

            let client2 = client.clone_for_execution();
            let backend2 = backend.clone();
            let extrinsics2 = thread2_extrinsics.clone();
            let task2 = std::thread::spawn(move || {
                let mut block_builder = BlockBuilder::new(
                    &client2,
                    client2.info().best_hash,
                    client2.info().best_number,
                    RecordProof::Yes,
                    Default::default(),
                    &*backend2,
                )
                    .unwrap();
                println!("builder2 record_proof_size: {}", block_builder.api.proof_recorder().map(|pr| pr.estimate_encoded_size()).unwrap_or(0));
                for extrinsic in extrinsics2 {
                    block_builder.push(extrinsic).unwrap();
                    println!("builder2 record_proof_size: {}", block_builder.api.proof_recorder().map(|pr| pr.estimate_encoded_size()).unwrap_or(0));
                }
                block_builder.api.take_all_changes()
            });

            let (changes1, _, recorder1) = task1.join().unwrap();
            let (changes2, _, recorder2) = task2.join().unwrap();
            println!("api1: {:?}", changes1.top_keys());
            println!("api2: {:?}", changes2.top_keys());
            let merge_handler = DefaultMergeHandler::<RuntimeEvent>::default();
            block_builder.api.deref_mut().merge_all_changes(changes1, recorder1, &merge_handler).unwrap();
            println!("merge 1 finished");
            block_builder.api.deref_mut().merge_all_changes(changes2, recorder2, &merge_handler).unwrap();
            block_builder.extrinsics.extend_from_slice(&thread1_extrinsics);
            block_builder.extrinsics.extend_from_slice(&thread2_extrinsics);
            println!("merg: {}", block_builder.api.debug_info());
            block_builder.build().unwrap()
        };

        println!("parent_hash: {parent_hash}");
        println!(
            "raw_builder_result: {}\nmain_storage_changes: {:?}\nchild_storage_changes: {:?}\noffchain_storage_changes: {:?}\ntx_root: {}\ntx_changes: {:?}\n",
            raw_builder_result.block.header().hash(),
            raw_builder_result.storage_changes.main_storage_changes,
            raw_builder_result.storage_changes.child_storage_changes,
            raw_builder_result.storage_changes.offchain_storage_changes,
            raw_builder_result.storage_changes.transaction_storage_root,
            raw_builder_result.storage_changes.transaction_index_changes,
        );
        println!(
            "two_threads_result: {}\nmain_storage_changes: {:?}\nchild_storage_changes: {:?}\noffchain_storage_changes: {:?}\ntx_root: {}\ntx_changes: {:?}\n",
            two_threads_result.block.header().hash(),
            two_threads_result.storage_changes.main_storage_changes,
            raw_builder_result.storage_changes.child_storage_changes,
            raw_builder_result.storage_changes.offchain_storage_changes,
            two_threads_result.storage_changes.transaction_storage_root,
            two_threads_result.storage_changes.transaction_index_changes,
        );
    }
}
