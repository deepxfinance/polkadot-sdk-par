//! Basic requests and implements for multi thread block builder.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use codec::{Compact, Decode, Encode, EncodeAppend};
use sp_api::ApiExt;
use sp_runtime::Digest;
use sp_runtime::traits::Block as BlockT;
use sp_state_machine::{MergeChange, OverlayedEntry, StorageKey, StorageValue};
use super::{get_map_value, get_top_value, parse_entry_value};

pub const EXTRINSIC_INDEX: [u8; 16] = [58, 101, 120, 116, 114, 105, 110, 115, 105, 99, 95, 105, 110, 100, 101, 120];
pub const SYSTEM_EXTRINSIC_COUNT: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 189, 192, 189, 48, 62, 152, 85, 129, 58, 168, 163, 13, 78, 252, 81, 18];
pub const SYSTEM_BLOCK_WEIGHT: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 52, 171, 245, 203, 52, 214, 36, 67, 120, 205, 219, 241, 142, 132, 157, 150];
pub const SYSTEM_ALL_EXTRINSICS_LEN: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 168, 109, 165, 169, 50, 104, 79, 25, 149, 57, 131, 111, 203, 140, 136, 111];
pub const SYSTEM_DIGEST: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 153, 231, 249, 63, 198, 169, 143, 8, 116, 253, 5, 127, 17, 28, 77, 45];
pub const SYSTEM_EVENT_COUNT: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 10, 152, 253, 190, 156, 230, 197, 88, 55, 87, 108, 96, 199, 175, 56, 80];
pub const SYSTEM_EVENTS: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 128, 212, 30, 94, 22, 5, 103, 101, 188, 132, 97, 133, 16, 114, 201, 215];
pub const SYSTEM_EXTRINSIC_DATA_PREFIX: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 223, 29, 174, 184, 152, 104, 55, 242, 28, 197, 209, 117, 150, 187, 120, 209];
pub const SYSTEM_EXECUTION_PHASE: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 255, 85, 59, 90, 152, 98, 165, 22, 147, 157, 130, 179, 211, 216, 102, 26];

/// Default type implement MultiThreadBlockBuilder trait.
#[derive(Clone)]
pub struct MergeSystem<RE> {
    init_index: u32,
    init_extrinsic_count: u32,
    init_all_extrinsics_len: u32,
    init_event_count: u32,
    init_block_weight: Option<PerDispatchClass<Weight>>,
    phantom: PhantomData<RE>,
}

impl<RE: Encode + Decode + Debug + Clone> Default for MergeSystem<RE> {
    fn default() -> Self {
        MergeSystem {
            init_index: 0,
            init_extrinsic_count: 0,
            init_all_extrinsics_len: 0,
            init_event_count: 0,
            init_block_weight: None,
            phantom: PhantomData,
        }
    }
}

impl<RE: Encode + Decode + Debug + Clone, B, Block: BlockT, Api: ApiExt<Block>> super::MultiThreadBlockBuilder<B, Block, Api> for MergeSystem<RE> {
    fn prepare(&mut self, _backend: &Arc<B>, _parent: &Block::Hash, api: &Api) {
        self.init_index = get_top_value(api, &EXTRINSIC_INDEX.to_vec()).unwrap_or_default();
        self.init_event_count = get_top_value(api, &SYSTEM_EVENT_COUNT.to_vec()).unwrap_or_default();
        self.init_block_weight = get_top_value(api, &SYSTEM_BLOCK_WEIGHT.to_vec());
        self.init_extrinsic_count = get_top_value(api, &SYSTEM_EXTRINSIC_COUNT.to_vec()).unwrap_or_default();
        self.init_all_extrinsics_len = get_top_value(api, &SYSTEM_ALL_EXTRINSICS_LEN.to_vec()).unwrap_or_default();
    }

    fn copy_state(&self) -> Self {
        self.clone()
    }
}

impl<RE: Encode + Decode + Debug + Clone> MergeChange<StorageKey, Option<StorageValue>> for MergeSystem<RE> {
    fn merge_changes(
        &self,
        local: &mut BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>,
        other: &mut BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>,
    ) -> Vec<StorageKey> {
        use sp_io::hashing::twox_64;

        let init_index: u32 = self.init_index;
        let offset: u32 = get_map_value(local, &EXTRINSIC_INDEX.to_vec()).unwrap_or_default();
        // update well_known_keys::EXTRINSIC_INDEX u32
        if let Some(entry_other) = other.remove(&EXTRINSIC_INDEX.to_vec()) {
            let index_other: u32 = parse_entry_value(&entry_other).unwrap_or(0);
            let final_index = offset.saturating_add(index_other).saturating_sub(init_index);
            log::trace!(target: "merge_system", "merge EXTRINSIC_INDEX init: {init_index}, local: {offset}, other: {index_other}, final: {final_index}");
            if let Some(entry_local) = local.get_mut(&EXTRINSIC_INDEX.to_vec()) {
                entry_local.set(Some(final_index.encode()), false, Some(final_index));
            }
        }

        // update "System ExtrinsicCount" u32
        // This is actually updated at pallet_system::finalize_block, we do not need to handle this when merge(which is before finalize block).
        if let Some(entry_other) = other.remove(&SYSTEM_EXTRINSIC_COUNT.to_vec()) {
            let other_count: u32 = parse_entry_value(&entry_other).unwrap_or_default();
            if let Some(entry_local) = local.get_mut(&SYSTEM_EXTRINSIC_COUNT.to_vec()) {
                let local_count: u32 = parse_entry_value(&entry_local).unwrap_or_default();
                let final_count = other_count.saturating_add(local_count).saturating_sub(self.init_extrinsic_count);
                log::trace!(target: "merge_system", "merge ExtrinsicCount init: {}, local: {local_count}, other: {other_count}, final: {final_count}", self.init_extrinsic_count);
                entry_local.set(Some(final_count.encode()), false, Some(final_count));
            } else {
                log::trace!(target: "merge_system", "merge ExtrinsicCount local: None, other: {other_count}, final: {other_count}");
                local.insert(SYSTEM_EXTRINSIC_COUNT.to_vec(), entry_other);
            }
        }

        // update "System AllExtrinsicsLen" u32
        if let Some(entry_other) = other.remove(&SYSTEM_ALL_EXTRINSICS_LEN.to_vec()) {
            let other_len: u32 = parse_entry_value(&entry_other).unwrap_or_default();
            let extrinsics = entry_other.extrinsics();
            let final_extrinsic = extrinsics.last().cloned().map(|e| e.saturating_add(offset).saturating_sub(init_index));
            if let Some(entry_local) = local.get_mut(&SYSTEM_ALL_EXTRINSICS_LEN.to_vec()) {
                let local_len: u32 = parse_entry_value(&entry_local).unwrap_or_default();
                let final_len = local_len.saturating_add(other_len).saturating_sub(self.init_all_extrinsics_len);
                log::trace!(target: "merge_system", "merge AllExtrinsicsLen init: {}, local: {local_len}, other: {other_len}, final: {final_len}", self.init_all_extrinsics_len);
                entry_local.set(Some(final_len.encode()), false, final_extrinsic);
            } else {
                log::trace!(target: "merge_system", "merge AllExtrinsicsLen local: None, other: {other_len}, final: {other_len}");
                local.insert(SYSTEM_ALL_EXTRINSICS_LEN.to_vec(), entry_other);
            }
        }

        // update "System Digest"
        if let Some(entry_other) = other.remove(&SYSTEM_DIGEST.to_vec()) {
            let other_digest: Digest = parse_entry_value(&entry_other).unwrap_or_default();
            let extrinsics = entry_other.extrinsics();
            let final_extrinsic = extrinsics.last().cloned().map(|e| e.saturating_add(offset).saturating_sub(init_index));
            if let Some(entry_local) = local.get_mut(&SYSTEM_DIGEST.to_vec()) {
                let local_digest: Digest = parse_entry_value(&entry_local).unwrap_or_default();
                let mut final_digest = local_digest.clone();
                for item in &other_digest.logs {
                    if !final_digest.logs.contains(item) {
                        final_digest.logs.push(item.clone());
                    }
                }
                log::trace!(target: "merge_system", "merge Digest local: {local_digest:?}, other: {other_digest:?}, final: {final_digest:?}");
                entry_local.set(Some(final_digest.encode()), false, final_extrinsic);
            } else {
                log::trace!(target: "merge_system", "merge Digest local: None, other: {other_digest:?}, final: {other_digest:?}");
                local.insert(SYSTEM_DIGEST.to_vec(), entry_other);
            }
        }

        // update "System BlockWeight"
        if let Some(entry_other) = other.remove(&SYSTEM_BLOCK_WEIGHT.to_vec()) {
            let other_weight: PerDispatchClass<Weight> = parse_entry_value(&entry_other).unwrap_or_default();
            let extrinsics = entry_other.extrinsics();
            let final_extrinsic = extrinsics.last().cloned().map(|e| e.saturating_add(offset).saturating_sub(init_index));
            if let Some(entry_local) = local.get_mut(&SYSTEM_BLOCK_WEIGHT.to_vec()) {
                let local_weight: PerDispatchClass<Weight> = parse_entry_value(&entry_local).unwrap_or_default();
                let mut final_weight = local_weight.clone();
                final_weight.normal.ref_time = final_weight.normal.ref_time.saturating_add(other_weight.normal.ref_time);
                final_weight.normal.proof_size = final_weight.normal.proof_size.saturating_add(other_weight.normal.proof_size);
                final_weight.operational.ref_time = final_weight.operational.ref_time.saturating_add(other_weight.operational.ref_time);
                final_weight.operational.proof_size = final_weight.operational.proof_size.saturating_add(other_weight.operational.proof_size);
                final_weight.mandatory.ref_time = final_weight.mandatory.ref_time.saturating_add(other_weight.mandatory.ref_time);
                final_weight.mandatory.proof_size = final_weight.mandatory.proof_size.saturating_add(other_weight.mandatory.proof_size);
                if let Some(init_weight) = &self.init_block_weight {
                    // remove initial BlockWeight for every storage change
                    final_weight.normal.ref_time = final_weight.normal.ref_time.saturating_sub(init_weight.normal.ref_time);
                    final_weight.normal.proof_size = final_weight.normal.proof_size.saturating_sub(init_weight.normal.proof_size);
                    final_weight.operational.ref_time = final_weight.operational.ref_time.saturating_sub(init_weight.operational.ref_time);
                    final_weight.operational.proof_size = final_weight.operational.proof_size.saturating_sub(init_weight.operational.proof_size);
                    final_weight.mandatory.ref_time = final_weight.mandatory.ref_time.saturating_sub(init_weight.mandatory.ref_time);
                    final_weight.mandatory.proof_size = final_weight.mandatory.proof_size.saturating_sub(init_weight.mandatory.proof_size);
                }
                log::trace!(target: "merge_system", "merge BlockWeight init: {:?}, local: {local_weight:?}, other: {other_weight:?}, final: {final_weight:?}", self.init_block_weight);
                entry_local.set(Some(final_weight.encode()), false, final_extrinsic);
            } else {
                log::trace!(target: "merge_system", "merge BlockWeight local: None, other: {other_weight:?}, final: {other_weight:?}");
                local.insert(SYSTEM_BLOCK_WEIGHT.to_vec(), entry_other);
            }
        }

        // update "System EventCount" u32
        if let Some(entry_other) = other.remove(&SYSTEM_EVENT_COUNT.to_vec()) {
            let other_count: u32 = parse_entry_value(&entry_other).unwrap_or_default();
            let extrinsics = entry_other.extrinsics();
            let final_extrinsic = extrinsics.last().cloned().map(|e| e.saturating_add(offset).saturating_sub(init_index));
            if let Some(entry_local) = local.get_mut(&SYSTEM_EVENT_COUNT.to_vec()) {
                let local_count: u32 = parse_entry_value(&entry_local).unwrap_or_default();
                let final_count = local_count.saturating_add(other_count).saturating_sub(self.init_event_count);
                log::trace!(target: "merge_system", "merge EventCount, init: {}, local: {local_count}, other: {other_count}, final: {final_count}", self.init_event_count);
                entry_local.set(Some(final_count.encode()), false, final_extrinsic);
            } else {
                log::trace!(target: "merge_system", "merge EventCount local: None, other: {other_count}, final: {other_count}");
                local.insert(SYSTEM_EVENT_COUNT.to_vec(), entry_other);
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

            let other_events: Vec<EventRecord<RE>> = parse_entry_value(&entry_other).unwrap_or_default();
            let extrinsics = entry_other.extrinsics();
            let final_extrinsic = extrinsics.last().cloned().map(|e| e.saturating_add(offset).saturating_sub(init_index));
            if let Some(entry_local) = local.get_mut(&SYSTEM_EVENTS.to_vec()) {
                log::trace!(target: "merge_system", "merge Events other: {other_events:?}");
                let local_event_data = entry_local
                    .value_ref()
                    .clone()
                    .unwrap_or_default();
                let append_events: Vec<_> = other_events
                    .into_iter()
                    .filter_map(|mut event| match &mut event.phase {
                        // drop duplicate initialize events.
                        Phase::Initialization => None,
                        Phase::ApplyExtrinsic(e) => {
                            if *e < init_index {
                                // duplicate initial events will not be pushed.
                                None
                            } else {
                                *e = e.saturating_add(offset).saturating_sub(init_index);
                                Some(event)
                            }
                        }
                        Phase::Finalization => Some(event)
                    })
                    .collect();
                let final_events_encoded = <Vec<EventRecord<RE>> as EncodeAppend>::append_or_new(local_event_data, append_events)
                    .expect("append new events");
                entry_local.set(Some(final_events_encoded), false, final_extrinsic);
            } else {
                log::trace!(target: "merge_system", "merge Events local: None, other: {other_events:?}");
                local.insert(SYSTEM_EVENTS.to_vec(), entry_other);
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
            let other_phase: Phase = parse_entry_value(&entry_other).unwrap_or(Phase::ApplyExtrinsic(0));
            let extrinsics = entry_other.extrinsics();
            let final_extrinsic = extrinsics.last().cloned().map(|e| e.saturating_add(offset).saturating_sub(init_index));
            if let Some(entry_local) = local.get_mut(&SYSTEM_EXECUTION_PHASE.to_vec()) {
                let local_phase: Phase = parse_entry_value(&entry_local).unwrap_or(Phase::ApplyExtrinsic(0));
                let final_phase = match (&local_phase, &other_phase) {
                    (Phase::ApplyExtrinsic(e1), Phase::ApplyExtrinsic(e2)) => {
                        Phase::ApplyExtrinsic(e2.saturating_add(*e1).saturating_sub(init_index))
                    }
                    (Phase::ApplyExtrinsic(_), Phase::Initialization) => local_phase.clone(),
                    (Phase::Finalization, _) => Phase::Finalization,
                    _ => match other_phase {
                        Phase::ApplyExtrinsic(e) => Phase::ApplyExtrinsic(e.saturating_add(offset).saturating_sub(init_index)),
                        _ => other_phase.clone(),
                    },
                };
                log::trace!(target: "merge_system", "merge ExecutionPhase init: {init_index}, local: {local_phase:?}, other: {other_phase:?}, final: {final_phase:?}");
                entry_local.set(Some(final_phase.encode()), false, final_extrinsic);
            } else {
                log::trace!(target: "merge_system", "merge ExecutionPhase local: None, other: {other_phase:?}, final: {other_phase:?}");
                local.insert(SYSTEM_EXECUTION_PHASE.to_vec(), entry_other);
            }
        }

        // update "System ExtrinsicData"
        let other_keys = other.keys().filter(|k| k.starts_with(SYSTEM_EXTRINSIC_DATA_PREFIX.as_slice())).cloned().collect::<Vec<_>>();
        for key in other_keys {
            let entry_other = other.remove(&key).unwrap();
            // update "System ExtrinsicData"
            let other_index_data = key[40..].to_vec();
            let other_index: u32 = Decode::decode(&mut other_index_data.as_slice()).unwrap();
            if other_index < init_index {
                // skip duplicate initialize extrinsic.
                if local.contains_key(&key) {
                    continue;
                }
            }
            let new_index = other_index.saturating_add(offset).saturating_sub(init_index);
            let new_index_data = new_index.encode();
            log::trace!(target: "merge_system", "merge System ExtrinsicData other: {other_index} -> {new_index}");
            let new_key = [SYSTEM_EXTRINSIC_DATA_PREFIX.to_vec(), twox_64(&new_index_data).to_vec(), new_index_data].concat();
            local.insert(new_key, entry_other);
        }
        Vec::new()
    }

    fn finalize_merge(&self, _map: &mut BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>) {}

    fn merge_weight(value: &BTreeMap<StorageKey, OverlayedEntry<Option<StorageValue>>>) -> u32 {
        let mut weight = value.len() as u32;
        let events_weight = value
            .get(&SYSTEM_EVENTS.to_vec())
            .map(|v| v.value_ref().as_ref().map(|d| <Compact<u32>>::decode(&mut d.as_slice()).unwrap().0))
            .unwrap_or_default()
            .unwrap_or_default();
        weight += events_weight;
        weight
    }
}

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
