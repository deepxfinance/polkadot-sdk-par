// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License

//! Houses the code that implements the transactional overlay storage.

use smallvec::SmallVec;
use sp_std::{
	collections::btree_map::BTreeMap,
	hash::Hash,
};
#[cfg(not(feature = "std"))]
pub use sp_std::collections::{btree_set::BTreeSet as Set, btree_map::BTreeMap as Map};
#[cfg(feature = "std")]
pub use std::collections::{HashSet as Set, HashMap as Map};
use std::collections::BTreeSet;
use crate::{Changes, StorageKey, StorageValue};

const PROOF_OVERLAY_NON_EMPTY: &str = "\
	An OverlayValue is always created with at least one transaction and dropped as soon
	as the last transaction is removed; qed";

type Transactions<V> = SmallVec<[V; 5]>;

/// Error returned when trying to commit or rollback while no transaction is open or
/// when the runtime is trying to close a transaction started by the client.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct NoOpenTransaction;

/// Error when calling `enter_runtime` when already being in runtime execution mode.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct AlreadyInRuntime;

/// Error when calling `exit_runtime` when not being in runtime execution mode.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct NotInRuntime;

/// Describes in which mode the node is currently executing.
#[derive(Debug, Clone, Copy)]
pub enum ExecutionMode {
	/// Executing in client mode: Removal of all transactions possible.
	Client,
	/// Executing in runtime mode: Transactions started by the client are protected.
	Runtime,
}

/// An overlay that contains all versions of a value for a specific key.
#[derive(Clone, Debug)]
pub struct OverlayedEntry<V> {
	/// The individual versions of that value.
	/// One entry per transactions during that the value was actually written.
	pub(crate) transactions: Transactions<V>,
}

impl<V> Default for OverlayedEntry<V> {
	fn default() -> Self {
		Self { transactions: SmallVec::new() }
	}
}

impl<V> OverlayedEntry<V> {
	pub fn empty(&self) -> bool {
		self.transactions.is_empty()
	}

	pub fn extrinsics(&self) -> BTreeSet<u32> {
		BTreeSet::new()
	}
}

pub type DirtyKeysSets<K> = SmallVec<[Set<K>; 5]>;

/// Inserts a key into the dirty set.
///
/// Returns true iff we are currently have at least one open transaction and if this
/// is the first write to the given key that transaction.
fn insert_dirty<K: Ord + Hash>(set: &mut DirtyKeysSets<K>, key: K) -> bool {
	set.last_mut().map(|dk| dk.insert(key)).unwrap_or_default()
}

/// Holds a set of changes with the ability modify them using nested transactions.
#[derive(Clone, Debug)]
pub struct OverlayedMap<K, V> {
	/// Stores the changes that this overlay constitutes.
	pub changes: BTreeMap<K, OverlayedEntry<V>>,
	/// Stores which keys are dirty per transaction. Needed in order to determine which
	/// values to merge into the parent transaction on commit. The length of this vector
	/// therefore determines how many nested transactions are currently open (depth).
	dirty_keys: DirtyKeysSets<K>,
	/// transactions layer when in ExecutionMode::Client.
	num_client_transactions: usize,
	/// Determines whether the node is using the overlay from the client or the runtime.
	execution_mode: ExecutionMode,
}

unsafe impl<K: Ord + Hash + Clone, V: Clone> Send for OverlayedMap<K, V> {}
unsafe impl<K: Ord + Hash + Clone, V: Clone> Sync for OverlayedMap<K, V> {}

impl<K, V> Default for OverlayedMap<K, V> {
	fn default() -> Self {
		Self {
			changes: BTreeMap::new(),
			dirty_keys: SmallVec::new(),
			num_client_transactions: Default::default(),
			execution_mode: Default::default(),
		}
	}
}

impl Default for ExecutionMode {
	fn default() -> Self {
		Self::Client
	}
}

impl<V> OverlayedEntry<V> {
	/// The value as seen by the current transaction.
	pub fn value_ref(&self) -> &V {
		&self.transactions.last().expect(PROOF_OVERLAY_NON_EMPTY)
	}

	/// The value as seen by the current transaction.
	pub fn into_value(mut self) -> V {
		self.transactions.pop().expect(PROOF_OVERLAY_NON_EMPTY)
	}

	/// Mutable reference to the most recent version.
	pub fn value_mut(&mut self) -> &mut V {
		self.transactions.last_mut().expect(PROOF_OVERLAY_NON_EMPTY)
	}

	pub fn pop_value(&mut self) -> V {
		self.pop_transaction()
	}

	/// Remove the last version and return it.
	fn pop_transaction(&mut self) -> V {
		self.transactions.pop().expect(PROOF_OVERLAY_NON_EMPTY)
	}

	/// Writes a new version of a cache value.
	pub fn init_cache(&mut self, value: V) -> bool {
		if self.transactions.is_empty() {
			self.transactions.push(value);
			true
		} else {
			false
		}
	}

	/// Writes a new version of a value.
	///
	/// This makes sure that the old version is not overwritten and can be properly
	/// rolled back when required.
	pub fn set(&mut self, value: V, first_write_in_tx: bool, _at_extrinsic: Option<u32>) {
		if first_write_in_tx || self.transactions.is_empty() {
			self.transactions.push(value);
		} else {
			*self.value_mut() = value;
		}
	}

	/// Insert a new transaction layer value.
	pub fn new_transaction(&mut self, value: V) {
		self.transactions.push(value);
	}
}

impl OverlayedEntry<Option<StorageValue>> {
	/// The value as seen by the current transaction.
	pub fn value(&self) -> Option<&StorageValue> {
		self.value_ref().as_ref()
	}
}

impl<K: Ord + Hash + Clone, V> OverlayedMap<K, V> {
	pub fn new() -> Self {
		Self {
			changes: Default::default(),
			dirty_keys: DirtyKeysSets::new(),
			num_client_transactions: 0,
			execution_mode: Default::default(),
		}
	}

	/// Move current changes to read_only data.
	/// This should be called only once.
	pub fn read_only(&mut self) {
		self.dirty_keys.clear();
		self.num_client_transactions = 0;
		self.execution_mode = ExecutionMode::Client;
		// TODO how to handle read_only
	}

	pub fn extend(&mut self, other: Self) {
		self.extend_changes(other.changes)
	}

	/// Set changes data.
	pub fn extend_changes(&mut self, mut changes: BTreeMap<K, OverlayedEntry<V>>) {
		if self.changes.is_empty() {
			core::mem::swap(&mut self.changes, &mut changes);
		} else {
			self.changes.extend(changes);
		}
	}

	/// Merge read_only and changes into changes.
	pub fn merge_read_only(&mut self) {
		// TODO how to handle merge_read_only
	}

	/// Create a new changeset at the same transaction state but without any contents.
	///
	/// This changeset might be created when there are already open transactions.
	/// We need to catch up here so that the child is at the same transaction depth.
	pub fn spawn_child(&self) -> Self {
		use sp_std::iter::repeat;
		Self {
			changes: Default::default(),
			dirty_keys: repeat(Set::new()).take(self.transaction_depth()).collect(),
			num_client_transactions: self.num_client_transactions,
			execution_mode: self.execution_mode,
		}
	}

	pub fn clear(&mut self) {
		self.changes.clear();
	}

	pub fn transaction_depth(&self) -> usize {
		self.dirty_keys.len()
	}

	pub fn has_open_runtime_transactions(&self) -> bool {
		self.transaction_depth() > self.num_client_transactions
	}

	pub fn first_write_in_tx(&mut self, key: &K) -> bool {
		self.has_open_runtime_transactions() && insert_dirty(&mut self.dirty_keys, key.clone())
	}

	/// True if no changes at all are contained in the change set.
	pub fn is_empty(&self) -> bool {
		self.changes.is_empty()
	}

	/// Cache a new value for the specified key.
	pub fn init_cache(&mut self, key: K, value: V) -> bool {
		 self.changes.entry(key.clone()).or_default().init_cache(value)
	}

	pub fn get<Q>(&self, key: &Q) -> Option<&OverlayedEntry<V>>
	where
		K: sp_std::borrow::Borrow<Q>,
		Q: Ord + ?Sized,
	{
		self.changes.get(key)
	}

	/// Set a new value for the specified key.
	///
	/// Can be rolled back or committed when called inside a transaction.
	pub fn set(&mut self, key: K, value: V, at_extrinsic: Option<u32>) {
		let first_write_in_tx = self.first_write_in_tx(&key);
		let overlayed = self.changes.entry(key).or_default();
		overlayed.set(value, first_write_in_tx, at_extrinsic);
	}
	
	pub fn drain(&mut self) -> BTreeMap<K, OverlayedEntry<V>> {
		core::mem::take(&mut self.changes)
	}

	/// Get a list of all changes as seen by current transaction.
	pub fn changes(&self) -> impl Iterator<Item = (&K, &OverlayedEntry<V>)> {
		self.changes.iter()
	}

	/// Get a list of all changes as seen by current transaction, consumes
	/// the overlay.
	pub fn into_changes(self) -> impl Iterator<Item = (K, OverlayedEntry<V>)> {
		self.changes.into_iter()
	}

	pub fn drain_changes(&mut self) -> BTreeMap<K, OverlayedEntry<V>> {
		sp_std::mem::take(&mut self.changes)
	}

	pub fn enter_runtime(&mut self) -> Result<(), AlreadyInRuntime> {
		if let ExecutionMode::Runtime = self.execution_mode {
			return Err(AlreadyInRuntime);
		}
		self.execution_mode = ExecutionMode::Runtime;
		self.num_client_transactions = self.transaction_depth();
		Ok(())
	}

	pub fn exit_runtime(&mut self) -> Result<(), NotInRuntime> {
		if let ExecutionMode::Client = self.execution_mode {
			return Err(NotInRuntime)
		}
		self.execution_mode = ExecutionMode::Client;
		if self.has_open_runtime_transactions() {
			log::warn!(
				"{} storage transactions are left open by the runtime. Those will be rolled back.",
				self.transaction_depth() - self.num_client_transactions,
			);
		}
		while self.has_open_runtime_transactions() {
			self.rollback_transaction()
				.expect("The loop condition checks that the transaction depth is > 0; qed");
		}
		Ok(())
	}

	pub fn start_transaction(&mut self) {
		self.dirty_keys.push(Default::default());
	}

	/// Rollback the last transaction started by `start_transaction`.
	///
	/// Any changes made during that transaction are discarded. Returns an error if
	/// there is no open transaction that can be rolled back.
	pub fn rollback_transaction(&mut self) -> Result<(), NoOpenTransaction> {
		self.close_transaction(true)
	}

	/// Commit the last transaction started by `start_transaction`.
	///
	/// Any changes made during that transaction are committed. Returns an error if
	/// there is no open transaction that can be committed.
	pub fn commit_transaction(&mut self) -> Result<(), NoOpenTransaction> {
		self.close_transaction(false)
	}

	fn close_transaction(&mut self, rollback: bool) -> Result<(), NoOpenTransaction> {
		// runtime is not allowed to close transactions started by the client
		if let ExecutionMode::Runtime = self.execution_mode {
			if !self.has_open_runtime_transactions() {
				return Err(NoOpenTransaction)
			}
		}
		for key in self.dirty_keys.pop().ok_or(NoOpenTransaction)? {
			let overlayed = self.changes.get_mut(&key).expect(
				"\
				A write to an OverlayedValue is recorded in the dirty key set. Before an
				OverlayedValue is removed, its containing dirty set is removed. This
				function is only called for keys that are in the dirty set. qed\
			",
			);

			if rollback {
				overlayed.pop_transaction();

				// We need to remove the key as an `OverlayValue` with no transactions
				// violates its invariant of always having at least one transaction.
				if overlayed.transactions.is_empty() {
					self.changes.remove(&key);
				}
			} else {
				// We only need to merge if there is an pre-existing value. It may be a value from
				// the previous transaction or a value committed without any open transaction.
				if overlayed.transactions.len() > 1 {
					let dropped_tx = overlayed.pop_transaction();
					*overlayed.value_mut() = dropped_tx;
				}
			}
		}

		Ok(())
	}

	/// Get a mutable reference for a value.
	///
	/// Can be rolled back or committed when called inside a transaction.
	#[must_use = "A change was registered, so this value MUST be modified."]
	pub fn modify(
		&mut self,
		key: K,
		init: impl FnOnce() -> V,
		extrinsic_index: Option<u32>,
	) -> &mut V
	where V: Clone
	{
		let first_write_in_tx = self.first_write_in_tx(&key);
		let overlayed = self.changes.entry(key).or_default();
		let clone_into_new_tx = if let Some(tx) = overlayed.transactions.last() {
			if first_write_in_tx {
				Some(tx.clone())
			} else {
				None
			}
		} else {
			Some(init())
		};

		if let Some(cloned) = clone_into_new_tx {
			overlayed.set(cloned, first_write_in_tx, extrinsic_index);
		}
		overlayed.value_mut()
	}
}

#[cfg(feature = "std")]
impl From<BTreeMap<Vec<u8>, Vec<u8>>> for OverlayedMap<StorageKey, Option<StorageValue>> {
	fn from(storage: BTreeMap<Vec<u8>, Vec<u8>>) -> Self {
		Self {
			changes: storage
				.into_iter()
				.map(|(k, v)| {
					(
						k,
						OverlayedEntry {
							transactions: SmallVec::from_iter([Some(StorageValue::new_raw(Some(v), false))]),
						},
					)
				})
				.collect(),
			dirty_keys: Default::default(),
			num_client_transactions: 0,
			execution_mode: ExecutionMode::Client,
		}
	}
}

/// Basic requirement for custom merge two changes into one.
pub trait MergeChange<K, V> {
	/// Merge other changes into local changes, return duplicate keys.
	fn merge_changes(
		&self,
		_local: &mut BTreeMap<K, OverlayedEntry<V>>,
		_other: &mut BTreeMap<K, OverlayedEntry<V>>,
		_in_order: bool,
	) -> Result<Changes, Vec<K>>;

	/// Finalize state if after all merge finished.(e.g. delete tmp values when merge_changes).
	fn finalize_merge(&self, _map: &mut BTreeMap<K, OverlayedEntry<V>>);

	/// Estimate merge weight(time) to merge this `changes` to others. 
	/// This is used to decide faster merge(merge `a to b` or `b to a`)
	fn merge_weight(_changes: &BTreeMap<K, OverlayedEntry<V>>) -> u32;
}

#[derive(Default)]
pub struct DefaultMerge;

impl<K: Ord + Hash + Clone, V: PartialEq> MergeChange<K, V> for DefaultMerge {
	fn merge_changes(
		&self,
		local: &mut BTreeMap<K, OverlayedEntry<V>>,
		other: &mut BTreeMap<K, OverlayedEntry<V>>,
		_in_order: bool,
	) -> Result<Changes, Vec<K>>  {
		let mut duplicate_keys = Vec::new();
		for (k, v) in core::mem::replace(other, Default::default()) {
			if let Some(entry) = local.get(&k) {
				if entry.value_ref() == v.value_ref() {
					continue;
				}
				duplicate_keys.push(k.clone());
			} else {
				local.insert(k, v);
			}
		}
		if !duplicate_keys.is_empty() {
			return Err(duplicate_keys);
		}
		Ok(Default::default())
	}

	fn finalize_merge(&self, _map: &mut BTreeMap<K, OverlayedEntry<V>>) {}

	fn merge_weight(_changes: &BTreeMap<K, OverlayedEntry<V>>) -> u32 {
		0
	}
}

impl<K: Ord + Hash + Clone, V: PartialEq> OverlayedMap<K, V> {
	pub fn merge_weight<M: MergeChange<K, V>>(&self) -> u32 {
		M::merge_weight(&self.changes)
	}

	pub fn merge(&mut self, other: &mut Self, in_order: bool) -> Result<Changes, Vec<K>> {
		self.merge_custom::<DefaultMerge>(other, in_order, None)
	}

	pub fn merge_custom<M: MergeChange<K, V>>(&mut self, other: &mut Self, in_order: bool, custom: Option<&M>) -> Result<Changes, Vec<K>> {
		// 1. If local or other change set have dirty key, that means some transaction not closed or rollback.
		// Unfinished change should not merge.
		if self.transaction_depth() != 0 || other.transaction_depth() != 0 {
			return Err(Vec::new());
		}
		// 2. num_client_transactions and execution_mode will not be used, so we do not merge here.
		// 3. merge changes.
		if let Some(m) = custom {
			match (
				m.merge_changes(&mut self.changes, &mut other.changes, in_order),
				DefaultMerge::default().merge_changes(&mut self.changes, &mut other.changes, in_order),
			) {
				(Ok(mut changes1), Ok(changes2)) => {
					changes1.extend(changes2);
					Ok(changes1)
				}
				(Err(duplicate_keys1), Err(duplicate_keys2)) => Err([duplicate_keys1, duplicate_keys2].concat()),
				(Ok(_), Err(duplicate_keys)) => Err(duplicate_keys),
				(Err(duplicate_keys), Ok(_)) => Err(duplicate_keys),
			}
		} else {
			DefaultMerge::default().merge_changes(&mut self.changes, &mut other.changes, in_order)
		}
	}

	pub fn finalize_merge(&mut self) {
		self.finalize_merge_custom::<DefaultMerge>(None)
	}

	pub fn finalize_merge_custom<M: MergeChange<K, V>>(&mut self, custom: Option<&M>) {
		if let Some(m) = custom {
			m.finalize_merge(&mut self.changes);
		} else {
			DefaultMerge::default().finalize_merge(&mut self.changes)
		}
	}
}
