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

use super::{Extrinsics, StorageKey};

#[cfg(not(feature = "std"))]
use sp_std::collections::btree_set::BTreeSet as Set;
#[cfg(feature = "std")]
use std::collections::HashSet as Set;
use sp_std::sync::Arc;
use once_cell::sync::OnceCell;
use crate::warn;
use smallvec::SmallVec;
use sp_std::{
	collections::{btree_map::BTreeMap, btree_set::BTreeSet},
	hash::Hash, vec::Vec,
};

const PROOF_OVERLAY_NON_EMPTY: &str = "\
	An OverlayValue is always created with at least one transaction and dropped as soon
	as the last transaction is removed; qed";

type DirtyKeysSets<K> = SmallVec<[Set<K>; 5]>;
type Transactions<V> = SmallVec<[InnerValue<V>; 5]>;

/// Error returned when trying to commit or rollback while no transaction is open or
/// when the runtime is trying to close a transaction started by the client.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct NoOpenTransaction;

/// Error when calling `enter_runtime` when already being in runtime execution mode.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct AlreadyInRuntime;

/// Error when calling `exit_runtime` when not being in runtime exection mdde.
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

#[derive(Debug, Default, Clone)]
#[cfg_attr(test, derive(PartialEq))]
struct InnerValue<V> {
	/// Current value. None if value has been deleted.
	value: V,
	/// The set of extrinsic indices where the values has been changed.
	extrinsics: Extrinsics,
}

/// An overlay that contains all versions of a value for a specific key.
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub struct OverlayedEntry<V> {
	/// The individual versions of that value.
	/// One entry per transactions during that the value was actually written.
	transactions: Transactions<V>,
}

impl<V> Default for OverlayedEntry<V> {
	fn default() -> Self {
		Self { transactions: SmallVec::new() }
	}
}

/// History of value, with removal support.
pub type OverlayedValue<V> = OverlayedEntry<Option<V>>;

/// Change set for basic key value with extrinsics index recording and removal support.
pub type OverlayedChangeSet<V> = StorageOverlay<StorageKey, Option<V>>;

pub type Cache<T> = Arc<OnceCell<T>>;

/// Holds a set of changes with the ability modify them using nested transactions.
#[derive(Clone)]
pub struct StorageOverlay<K: Ord + Hash + Clone, V: Clone> {
	pub space: Vec<u8>,
	/// Cached best value.
	/// For cache, any value will only insert once(data should not change).
	pub cache: BTreeMap<K, Cache<V>>,
	/// Stores the changes that this overlay constitutes.
	pub changes: BTreeMap<K, OverlayedEntry<V>>,
	/// Stores which keys are dirty per transaction. Needed in order to determine which
	/// values to merge into the parent transaction on commit. The length of this vector
	/// therefore determines how many nested transactions are currently open (depth).
	dirty_keys: DirtyKeysSets<K>,
	/// The number of how many transactions beginning from the first transactions are started
	/// by the client. Those transactions are protected against close (commit, rollback)
	/// when in runtime mode.
	num_client_transactions: usize,
}

impl Default for ExecutionMode {
	fn default() -> Self {
		Self::Client
	}
}

impl<V> OverlayedEntry<V> {
	/// The value as seen by the current transaction.
	pub fn value_ref(&self) -> &V {
		&self.transactions.last().expect(PROOF_OVERLAY_NON_EMPTY).value
	}

	/// The value as seen by the current transaction.
	pub fn into_value(mut self) -> V {
		self.transactions.pop().expect(PROOF_OVERLAY_NON_EMPTY).value
	}

	/// Unique list of extrinsic indices which modified the value.
	pub fn extrinsics(&self) -> BTreeSet<u32> {
		let mut set = BTreeSet::new();
		self.transactions
			.iter()
			.for_each(|t| t.extrinsics.copy_extrinsics_into(&mut set));
		set
	}

	/// Mutable reference to the most recent version.
	pub fn value_mut(&mut self) -> &mut V {
		&mut self.transactions.last_mut().expect(PROOF_OVERLAY_NON_EMPTY).value
	}

	pub fn pop_value(&mut self) -> V {
		self.pop_transaction().value
	}

	/// Remove the last version and return it.
	fn pop_transaction(&mut self) -> InnerValue<V> {
		self.transactions.pop().expect(PROOF_OVERLAY_NON_EMPTY)
	}

	/// Mutable reference to the set which holds the indices for the **current transaction only**.
	fn transaction_extrinsics_mut(&mut self) -> &mut Extrinsics {
		&mut self.transactions.last_mut().expect(PROOF_OVERLAY_NON_EMPTY).extrinsics
	}

	/// Writes a new version of a value.
	///
	/// This makes sure that the old version is not overwritten and can be properly
	/// rolled back when required.
	pub fn set(&mut self, value: V, first_write_in_tx: bool, at_extrinsic: Option<u32>) {
		if first_write_in_tx || self.transactions.is_empty() {
			self.transactions.push(InnerValue { value, extrinsics: Default::default() });
		} else {
			*self.value_mut() = value;
		}

		if let Some(extrinsic) = at_extrinsic {
			self.transaction_extrinsics_mut().insert(extrinsic);
		}
	}
}

impl<V> OverlayedEntry<Option<V>> {
	/// The value as seen by the current transaction.
	pub fn value(&self) -> Option<&V> {
		self.value_ref().as_ref()
	}
}

/// Inserts a key into the dirty set.
///
/// Returns true iff we are currently have at least one open transaction and if this
/// is the first write to the given key that transaction.
fn insert_dirty<K: Ord + Hash>(set: &mut DirtyKeysSets<K>, key: K) -> bool {
	set.last_mut().map(|dk| dk.insert(key)).unwrap_or_default()
}

impl<K: Ord + Hash + Clone, V: Clone> StorageOverlay<K, V> {
	pub fn new(space: &[u8], client_transactions: usize, runtime_transactions: usize) -> Self {
		Self {
			space: space.to_vec(),
			cache: Default::default(),
			changes: Default::default(),
			dirty_keys: (0..client_transactions + runtime_transactions).map(|_| Default::default()).collect(),
			num_client_transactions: client_transactions,
		}
	}

	pub fn clone_with_changes(&self) -> Self {
		Self {
			space: self.space.clone(),
			cache: self.cache.clone(),
			// changes are different(if changed) between copies
			changes: self.changes.clone(),
			dirty_keys: self.dirty_keys.clone(),
			num_client_transactions: self.num_client_transactions,
		}
	}

	// /// Create a new changeset at the same transaction state but without any contents.
	// ///
	// /// This changeset might be created when there are already open transactions.
	// /// We need to catch up here so that the child is at the same transaction depth.
	// pub fn spawn_child(&self) -> Self {
	// 	use sp_std::iter::repeat;
	// 	Self {
	// 		space: self.space.clone(),
	// 		cache: Default::default(),
	// 		changes: Default::default(),
	// 		dirty_keys: repeat(Set::new()).take(self.transaction_depth()).collect(),
	// 		num_client_transactions: self.num_client_transactions,
	// 	}
	// }

	/// True if no changes at all are contained in the change set.
	pub fn is_empty(&self) -> bool {
		self.changes.is_empty()
	}

	/// Get an optional reference to the value stored for the specified key.
	pub fn get_ref<Q>(&self, key: &Q) -> Option<&OverlayedEntry<V>>
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
		let overlayed = self.changes.entry(key.clone()).or_default();
		overlayed.set(value, insert_dirty(&mut self.dirty_keys, key), at_extrinsic);
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

	/// Consume this changeset and return all committed changes.
	///
	/// Panics:
	/// Panics if there are open transactions: `transaction_depth() > 0`
	pub fn drain_commited(self) -> impl Iterator<Item = (K, V)> {
		assert!(self.transaction_depth() == 0, "Drain is not allowed with open transactions.");
		self.changes.into_iter().map(|(k, mut v)| (k, v.pop_transaction().value))
	}

	/// Returns the current nesting depth of the transaction stack.
	///
	/// A value of zero means that no transaction is open and changes are committed on write.
	pub fn transaction_depth(&self) -> usize {
		self.dirty_keys.len()
	}

	/// Call this before transfering control to the runtime.
	///
	/// This protects all existing transactions from being removed by the runtime.
	/// Calling this while already inside the runtime will return an error.
	pub fn enter_runtime(&mut self) {
		self.num_client_transactions = self.transaction_depth();
	}

	/// Call this when control returns from the runtime.
	///
	/// This commits all dangling transaction left open by the runtime.
	/// Calling this while already outside the runtime will return an error.
	pub fn exit_runtime(&mut self) -> usize {
		if self.has_open_runtime_transactions() {
			warn!(
				"{} storage transactions are left open by the runtime. Those will be rolled back.",
				self.transaction_depth() - self.num_client_transactions,
			);
		}
		while self.has_open_runtime_transactions() {
			self.rollback_transaction(&ExecutionMode::Client)
				.expect("The loop condition checks that the transaction depth is > 0; qed");
		}
		// depth should be equal than `num_client_transactions`
		self.transaction_depth()
	}

	/// Start a new nested transaction.
	///
	/// This allows to either commit or roll back all changes that were made while this
	/// transaction was open. Any transaction must be closed by either `commit_transaction`
	/// or `rollback_transaction` before this overlay can be converted into storage changes.
	///
	/// Changes made without any open transaction are committed immediately.
	pub fn start_transaction(&mut self) {
		self.dirty_keys.push(Default::default());
	}

	/// Rollback the last transaction started by `start_transaction`.
	///
	/// Any changes made during that transaction are discarded. Returns an error if
	/// there is no open transaction that can be rolled back.
	pub fn rollback_transaction(&mut self, mode: &ExecutionMode) -> Result<(), NoOpenTransaction> {
		self.close_transaction(true, mode)
	}

	/// Commit the last transaction started by `start_transaction`.
	///
	/// Any changes made during that transaction are committed. Returns an error if
	/// there is no open transaction that can be committed.
	pub fn commit_transaction(&mut self, mode: &ExecutionMode) -> Result<(), NoOpenTransaction> {
		self.close_transaction(false, mode)
	}

	fn close_transaction(&mut self, rollback: bool, mode: &ExecutionMode) -> Result<(), NoOpenTransaction> {
		// runtime is not allowed to close transactions started by the client
		if let ExecutionMode::Runtime = mode {
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
				let has_predecessor = if let Some(dirty_keys) = self.dirty_keys.last_mut() {
					// Not the last tx: Did the previous tx write to this key?
					!dirty_keys.insert(key)
				} else {
					// Last tx: Is there already a value in the committed set?
					// Check against one rather than empty because the current tx is still
					// in the list as it is popped later in this function.
					overlayed.transactions.len() > 1
				};

				// We only need to merge if there is an pre-existing value. It may be a value from
				// the previous transaction or a value committed without any open transaction.
				if has_predecessor {
					let dropped_tx = overlayed.pop_transaction();
					*overlayed.value_mut() = dropped_tx.value;
					overlayed.transaction_extrinsics_mut().extend(dropped_tx.extrinsics);
				}
			}
		}

		Ok(())
	}

	fn has_open_runtime_transactions(&self) -> bool {
		self.transaction_depth() > self.num_client_transactions
	}
}

impl<V: Clone> OverlayedChangeSet<V> {
	/// Get a mutable reference for a value.
	///
	/// Can be rolled back or committed when called inside a transaction.
	#[must_use = "A change was registered, so this value MUST be modified."]
	pub fn modify(
		&mut self,
		key: StorageKey,
		init: Option<impl FnOnce() -> Option<V>>,
		at_extrinsic: Option<u32>,
	) -> Option<&mut Option<V>> {
		let overlayed = self.changes.entry(key.clone()).or_default();
		let first_write_in_tx = insert_dirty(&mut self.dirty_keys, key.clone());
		let clone_into_new_tx = if let Some(tx) = overlayed.transactions.last() {
			if first_write_in_tx {
				Some(tx.value.clone())
			} else {
				None
			}
		} else if let Some(value) = self.cache.get(&key).map(|c| c.get()).unwrap_or(None) {
			Some(value.clone())
		} else {
			if let Some(init) = init {
				Some(init())
			} else {
				return None;
			}
		};

		if let Some(cloned) = clone_into_new_tx {
			overlayed.set(cloned, first_write_in_tx, at_extrinsic);
		}
		Some(overlayed.value_mut())
	}

	pub fn modify_append(
		&mut self,
		key: StorageKey,
		init: impl FnOnce() -> V,
		at_extrinsic: Option<u32>,
	) -> Option<&mut V> {
		let overlayed = self.changes.entry(key.clone()).or_default();
		let first_write_in_tx = insert_dirty(&mut self.dirty_keys, key.clone());
		let clone_into_new_tx = if let Some(tx) = overlayed.transactions.last() {
			if first_write_in_tx {
				Some(Some(tx.value.clone().unwrap_or(init())))
			} else if tx.value.is_none() {
				Some(Some(init()))
			} else {
				None
			}
		} else if let Some(value) = self.cache.get(&key).map(|c| c.get()).unwrap_or(None) {
			Some(Some(value.clone().unwrap_or(init())))
		} else {
			Some(Some(init()))
		};

		if let Some(cloned) = clone_into_new_tx {
			overlayed.set(cloned, first_write_in_tx, at_extrinsic);
		}
		overlayed.value_mut().as_mut()
	}

	/// Set all values to deleted which are matched by the predicate.
	///
	/// Can be rolled back or committed when called inside a transaction.
	pub fn clear_where(
		&mut self,
		predicate: impl Fn(&[u8], &OverlayedValue<V>) -> bool,
		at_extrinsic: Option<u32>,
	) -> u32 {
		let mut count = 0;
		for (key, val) in self.changes.iter_mut().filter(|(k, v)| predicate(k, v)) {
			if val.value_ref().is_some() {
				count += 1;
			}
			val.set(None, insert_dirty(&mut self.dirty_keys, key.clone()), at_extrinsic);
		}
		count
	}

	/// Get the iterator over all changes that follow the supplied `key`.
	pub fn changes_after(&self, key: &[u8]) -> impl Iterator<Item = (&[u8], &OverlayedValue<V>)> {
		use sp_std::ops::Bound;
		let range = (Bound::Excluded(key), Bound::Unbounded);
		self.changes.range::<[u8], _>(range).map(|(k, v)| (k.as_slice(), v))
	}
}

