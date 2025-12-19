use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};
use codec::Encode;
use futures::{select, FutureExt};
use log::{trace, debug};
use sc_transaction_pool_api::{InPoolTransaction, TransactionPool};
use sp_runtime::traits::{Block as BlockT, Header as HeaderT, One};
use crate::{GroupInfo, GroupTxInput, GroupTxOutput};

/// If the block is full we will attempt to push at most
/// this number of transactions before quitting for real.
/// It allows us to increase block utilization.
const MAX_SKIPPED_TRANSACTIONS: usize = 8;
const LOG_TARGET: &str = "tx_group";

pub struct TransactionGrouper<A, B> {
    /// The transaction pool.
    transaction_pool: Arc<A>,
    phantom: PhantomData<B>,
}

impl<A, B: BlockT> TransactionGrouper<A, B>
where
    A: TransactionPool<Block=B, Hash=B::Hash>,
    <A as TransactionPool>::InPoolTransaction: Send + Sync + 'static,
{
    pub fn new(transaction_pool: Arc<A>) -> Self {
        Self { transaction_pool, phantom: PhantomData }
    }

    pub async fn group_transactions_from_pool(
        &self,
        parent_number: <B::Header as HeaderT>::Number,
        input: GroupTxInput,
        mut block_size: usize,
        proof_size: usize,
        mut filter: HashSet<B::Hash>,
    ) -> Result<GroupTxOutput<A>, String> {
        let GroupTxInput {
            wait_pool,
            max_time,
            thread_limit,
            linear_tx_limit,
            single_tx_min,
            total_tx_limit,
            block_size_limit,
        } = input.clone();
        let start = Instant::now();
        let deadline = start + max_time;
        let block = parent_number + One::one();
        let mut t1 = self.transaction_pool.ready_at(parent_number).fuse();
        let mut t2 = futures_timer::Delay::new(wait_pool).fuse();
        let mut pending_iterator = select! {
			res = t1 => res,
			_ = t2 => {
				trace!(target: LOG_TARGET, "[GroupTx B {block}] Timeout fired waiting for transaction pool. Proceeding with production.");
				self.transaction_pool.ready()
			},
		};
        let wait = start.elapsed();

        let mut skipped = 0;
        let mut filtered = 0;
        let mut raw_tx_count = 0usize;
        // collect transactions' group info and dispatch to different groups.
        let mut grouper = ConflictGroup::new();
        // loop for
        // 1. get enough transaction from pool.
        // 2. spawn mission for every transaction:
        //      parse transaction runtime call group info and return by channel.
        loop {
            if raw_tx_count >= total_tx_limit {
                debug!(target: LOG_TARGET, "[GroupTx B {block}] Reach tx_limit {}/{} ms (total {raw_tx_count}, block size: {}/{block_size_limit})", start.elapsed().as_millis(), max_time.as_millis(), block_size + proof_size);
                break;
            }
            if Instant::now() > deadline {
                debug!(target: LOG_TARGET, "[GroupTx B {block}] Reach deadline {}/{} ms (total {raw_tx_count}, block size: {}/{block_size_limit})", start.elapsed().as_millis(), max_time.as_millis(), block_size + proof_size);
                break;
            }
            let pending_tx = if let Some(pending_tx) = pending_iterator.next() {
                pending_tx
            } else {
                debug!(target: LOG_TARGET, "[GroupTx B {block}] Out of transactions({} ms, total {raw_tx_count}, block size: {}/{block_size_limit})", start.elapsed().as_millis(), block_size + proof_size);
                break;
            };
            if filter.remove(pending_tx.hash()) {
                filtered += 1;
                continue;
            }

            if block_size + pending_tx.data().encoded_size() + proof_size > block_size_limit {
                if skipped < MAX_SKIPPED_TRANSACTIONS {
                    skipped += 1;
                    trace!(
                        target: LOG_TARGET,
                        "[GroupTx B {block}] Transaction would overflow the block size limit, but will try {} more transactions before quitting.",
                        MAX_SKIPPED_TRANSACTIONS - skipped,
                    );
                    continue
                } else {
                    debug!(
                        target: LOG_TARGET,
                        "[GroupTx B {block}] Reached block size limit({}/{block_size_limit}) with extrinsic: {raw_tx_count} in {} ms. start execute transactions.",
                        block_size + proof_size,
                        start.elapsed().as_millis(),
                    );
                    break;
                }
            }
            block_size += pending_tx.data().encoded_size();
            grouper.insert(pending_tx.group_info().to_vec(), (raw_tx_count, pending_tx));
            // TODO update proof_size with new extrinsic. This is hard since we do not actually execute the extrinsic.
            raw_tx_count += 1;
        }
        let (mut groups, raw_groups, max_group_length, sort) = grouper.group(thread_limit, linear_tx_limit, single_tx_min);
        for group in & mut groups {
            group.sort_by(|a, b| a.0.cmp(&b.0));
        }
        let mut single: Vec<_> = grouper.single_group(linear_tx_limit.saturating_sub(max_group_length));
        // if on one parallel group, merge it to single_group.
        if groups.len() == 1 {
            single = [groups.pop().unwrap(), single].concat();
        }
        single.sort_by(|a, b| a.0.cmp(&b.0));
        let tx_count = groups.iter().map(|g| g.len()).sum::<usize>() + single.len();
        Ok(GroupTxOutput {
            info: GroupInfo {
                input,
                tx_count,
                filtered,
                raw_tx_count,
                raw_groups,
                groups: groups.len(),
                time: start.elapsed(),
                wait,
                sort,
            },
            groups,
            single,
        })
    }
}

pub struct ConflictGroup<K, V> {
    group_id: usize,
    group: HashMap<usize, Vec<V>>,
    single_group: Vec<V>,
    key_group: HashMap<K, usize>,
    group_keys: HashMap<usize, Vec<K>>,
}

impl<K: PartialEq + Eq + Hash + Clone, V> ConflictGroup<K, V> {
    pub fn new() -> Self {
        Self {
            group_id: 0,
            group: HashMap::new(),
            single_group: Vec::new(),
            key_group: HashMap::new(),
            group_keys: HashMap::new(),
        }
    }

    /// Group value with some specific keys.
    /// If two values have some conflict key, they should be merged into one group.
    /// If keys is empty, it is considered as ungroupable value, we just insert to a single group.
    /// As a result, values with any conflict key should be finally in same group.
    pub fn insert(&mut self, keys: Vec<K>, value: V) {
        // if keys is empty, the value will be push to single group.
        if keys.is_empty() {
            self.single_group.push(value);
            return;
        }
        // by default, we will insert new value to new group id.
        let mut gid = self.group_id;
        for key in &keys {
            // if some dependent key is already dispatched to group id, this value should be in this group.
            if let Some(id) = self.key_group.get(key) {
                gid = *id;
                break;
            }
        };
        let mut tmp_move_groups = vec![];
        // merge conflict groups into one group
        for key in &keys {
            if let Some(used_group) = self.key_group.remove(key) {
                if used_group != gid {
                    let move_keys = self.group_keys.remove(&used_group).unwrap_or_default();
                    // update move_key -> gid map.
                    for move_key in &move_keys {
                        self.key_group.insert(move_key.clone(), gid);
                    }
                    // insert move_keys to gid
                    if let Some(keys) = self.group_keys.get_mut(&gid) {
                        keys.extend_from_slice(&move_keys);
                    } else {
                        self.group_keys.insert(gid, move_keys);
                    }
                    // move transactions from used_group to gid.
                    let move_values = self.group.remove(&used_group).unwrap_or_default();
                    tmp_move_groups.push((used_group, move_values.len()));
                    if let Some(values) = self.group.get_mut(&gid) {
                        values.extend(move_values);
                    } else {
                        self.group.insert(gid, move_values);
                    }
                }
            } else {
                if let Some(keys) = self.group_keys.get_mut(&gid) {
                    keys.push(key.clone());
                } else {
                    self.group_keys.insert(gid, vec![key.clone()]);
                }
            }
            // record key group_id
            self.key_group.insert(key.clone(), gid);
        }
        if !tmp_move_groups.is_empty() {
            trace!(target: LOG_TARGET, "ConflictGroup move to group {gid} with groups: {tmp_move_groups:?}");
        }
        // insert tx to gid
        if let Some(values) = self.group.get_mut(&gid) {
            values.push(value);
        } else {
            self.group.insert(gid, vec![value]);
        }
        // if new group_id used, group_id += 1
        if gid == self.group_id {
            self.group_id += 1;
        }
    }

    /// Get groups no more than `thread_limit`(get longest group first).
    /// Final single_group length should l.e than single_min(final_single_group_length).
    /// Each parallel group's length should l.e. than `linear_value_limit - final_single_group_length`(max_group_length).
    /// We will merge some other short groups to fulfill our final groups to max_group_length.
    pub fn group(&mut self, thread_limit: usize, linear_value_limit: usize, single_min: usize) -> (Vec<Vec<V>>, usize, usize, Duration) {
        let single_min = self.single_group.len().min(single_min);
        let group_max_length = linear_value_limit.saturating_sub(single_min);

        let mut max_length = 0usize;
        let mut groups = core::mem::take(&mut self.group).into_values().collect::<Vec<_>>();
        let groups_len = groups.len();
        if groups_len <= thread_limit {
            groups.iter_mut().for_each(|g| {
                g.truncate(group_max_length);
                max_length = max_length.max(g.len());
            });
            return (groups, groups_len, max_length, Default::default());
        }
        let sort_start = Instant::now();
        groups.sort_by(|a, b| a.len().cmp(&b.len()));
        let mut result_groups = groups.split_off(groups_len - thread_limit);
        loop {
            if !result_groups.is_empty() && !groups.is_empty() {
                // get other short group for first group.
                let group = groups.remove(0);
                result_groups[0].extend(group);
                if result_groups[0].len() >= group_max_length {
                    // shortest group is enough
                    break;
                } else {
                    // sort by ascending group's length.
                    result_groups.sort_by(|a, b| a.len().cmp(&b.len()));
                }
            } else {
                break;
            }
        }
        result_groups.iter_mut().for_each(|g| {
            g.truncate(group_max_length);
            max_length = max_length.max(g.len());
        });
        // final result_groups order is not ensured.
        (result_groups, groups_len, max_length, sort_start.elapsed())
    }

    pub fn single_group(&mut self, length_limit: usize) -> Vec<V> {
        let mut single_group = std::mem::take(&mut self.single_group);
        single_group.truncate(length_limit);
        single_group
    }
}

#[test]
fn test_conflict_group() {
    let mut conflict_group: ConflictGroup<u32, &str> = ConflictGroup::new();
    // gid 0
    conflict_group.insert(vec![0], "0");
    // gid 1
    conflict_group.insert(vec![1], "1");
    // gid 2
    conflict_group.insert(vec![2], "2");
    // gid 3
    conflict_group.insert(vec![3], "3");
    // this insert will merge "1", "4" to gid 1.
    conflict_group.insert(vec![1, 4], "4");
    // this insert will merge "2", "5" to gid 2.
    conflict_group.insert(vec![2, 5], "5");
    // this insert will group "2", "3", "5", "6" to gid 3.
    conflict_group.insert(vec![3, 5], "6");

    assert_eq!(conflict_group.key_group.remove(&0), Some(0));
    assert_eq!(conflict_group.key_group.remove(&1), Some(1));
    assert_eq!(conflict_group.key_group.remove(&2), Some(3));
    assert_eq!(conflict_group.key_group.remove(&3), Some(3));
    assert_eq!(conflict_group.key_group.remove(&4), Some(1));
    assert_eq!(conflict_group.key_group.remove(&5), Some(3));

    assert_eq!(conflict_group.group_keys.remove(&0), Some(vec![0]));
    assert_eq!(conflict_group.group_keys.remove(&1), Some(vec![1, 4]));
    assert_eq!(conflict_group.group_keys.remove(&2), None);
    assert_eq!(conflict_group.group_keys.remove(&3), Some(vec![3, 2, 5]));

    assert_eq!(conflict_group.group.remove(&0), Some(vec!["0"]));
    assert_eq!(conflict_group.group.remove(&1), Some(vec!["1", "4"]));
    assert_eq!(conflict_group.group.remove(&2), None);
    assert_eq!(conflict_group.group.remove(&3), Some(vec!["3", "2", "5", "6"]));
}

#[test]
fn test_conflict_group_short_groups() {
    let mut conflict_group: ConflictGroup<u32, (u32, &str)> = ConflictGroup::new();
    conflict_group.insert(vec![0], (0, "0"));
    conflict_group.insert(vec![0], (5, "00"));
    conflict_group.insert(vec![1], (1, "1"));
    conflict_group.insert(vec![1], (11, "11"));
    conflict_group.insert(vec![1], (111, "111"));
    conflict_group.insert(vec![2], (2, "2"));
    conflict_group.insert(vec![3], (3, "3"));
    conflict_group.insert(vec![4], (4, "4"));
    conflict_group.insert(vec![], (8, "e"));

    let (mut groups, _, _, _) = conflict_group.group(3, 4, 1);
    groups.iter_mut().for_each(|g| g.sort_by(|a, b| a.0.cmp(&b.0)));
    assert_eq!(
        groups,
        vec![
            vec![(2, "2"), (3, "3"), (4, "4")],
            vec![(0, "0"), (5, "00")],
            vec![(1, "1"), (11, "11"), (111, "111")],
        ]
    );
    let mut single = conflict_group.single_group(1);
    single.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(single, vec![(8, "e")]);
}
