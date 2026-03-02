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

use crate::{RcT, Set};
use smallvec::SmallVec;
use sp_std::{
	collections::{btree_map::BTreeMap, btree_set::BTreeSet},
	hash::Hash,
};

const PROOF_OVERLAY_NON_EMPTY: &str = "\
	An OverlayValue is always created with at least one transaction and dropped as soon
	as the last transaction is removed; qed";

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
#[derive(Clone)]
pub struct OverlayedEntry<V> {
	/// The individual versions of that value.
	/// One entry per transactions during that the value was actually written.
	transactions: Transactions<RcT<V>>,
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
}

// pub type Cache<T> = Arc<OnceCell<T>>;

/// Holds a set of changes with the ability modify them using nested transactions.
#[derive(Clone)]
pub struct StorageOverlay<K: Ord + Hash + Clone, V: Clone> {
	/// Stores the changes that this overlay constitutes.
	pub changes: BTreeMap<K, OverlayedEntry<V>>,
}

impl Default for ExecutionMode {
	fn default() -> Self {
		Self::Client
	}
}

impl<V> OverlayedEntry<V> {
	pub fn take_ref(&mut self) -> RcT<V> {
		self.transactions.pop().expect(PROOF_OVERLAY_NON_EMPTY).value
	}

	/// The value as seen by the current transaction.
	pub fn value_ref(&self) -> &RcT<V> {
		&self.transactions.last().expect(PROOF_OVERLAY_NON_EMPTY).value
	}

	/// The value as seen by the current transaction.
	pub fn into_value(mut self) -> RcT<V> {
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
	pub fn value_mut(&mut self) -> &mut RcT<V> {
		&mut self.transactions.last_mut().expect(PROOF_OVERLAY_NON_EMPTY).value
	}

	pub fn pop_value(&mut self) -> RcT<V> {
		self.pop_transaction().value
	}

	/// Remove the last version and return it.
	fn pop_transaction(&mut self) -> InnerValue<RcT<V>> {
		self.transactions.pop().expect(PROOF_OVERLAY_NON_EMPTY)
	}

	/// Mutable reference to the set which holds the indices for the **current transaction only**.
	fn transaction_extrinsics_mut(&mut self) -> &mut Extrinsics {
		&mut self.transactions.last_mut().expect(PROOF_OVERLAY_NON_EMPTY).extrinsics
	}

	/// Writes a new version of a cache value.
	pub fn init_cache(&mut self, value: Option<V>) -> bool {
		if self.transactions.is_empty() {
			self.transactions.push(InnerValue { value: RcT::new(value, false), extrinsics: Default::default() });
			true
		} else {
			false
		}
	}

	/// Writes a new version of a value.
	///
	/// This makes sure that the old version is not overwritten and can be properly
	/// rolled back when required.
	pub fn set(&mut self, value: Option<V>, first_write_in_tx: bool, muted: bool, at_extrinsic: Option<u32>) {
		if first_write_in_tx || self.transactions.is_empty() {
			self.transactions.push(InnerValue { value: RcT::new(value, muted), extrinsics: Default::default() });
		} else {
			self.value_mut().mutate(|t| *t = value);
		}

		if let Some(extrinsic) = at_extrinsic {
			self.transaction_extrinsics_mut().insert(extrinsic);
		}
	}

	/// Insert a new transaction layer value.
	pub fn new_transaction(&mut self, value: RcT<V>) {
		self.transactions.push(InnerValue { value, extrinsics: Default::default() });
	}
}

impl<V: Clone> StorageOverlay<StorageKey, V> {
	pub fn new() -> Self {
		Self {
			changes: Default::default(),
		}
	}

	pub fn clone_with_changes(&self) -> Self {
		Self {
			// changes are different(if changed) between copies
			changes: self.changes.clone(),
		}
	}

	/// True if no changes at all are contained in the change set.
	pub fn is_empty(&self) -> bool {
		self.changes.is_empty()
	}

	/// Get an optional reference to the value stored for the specified key.
	pub fn get_ref(&self, key: &StorageKey) -> Option<&OverlayedEntry<V>> {
		self.changes.get(key)
	}

	/// Cache a new value for the specified key.
	pub fn init_cache(&mut self, key: StorageKey, value: Option<V>) -> bool {
		 self.changes.entry(key.clone()).or_default().init_cache(value)
	}

	/// Set a new value for the specified key.
	///
	/// Can be rolled back or committed when called inside a transaction.
	pub fn set(&mut self, first_write_in_tx: bool, key: StorageKey, value: Option<V>, at_extrinsic: Option<u32>) {
		let overlayed = self.changes.entry(key).or_default();
		overlayed.set(value, first_write_in_tx, true, at_extrinsic);
	}

	/// Get a list of all changes as seen by current transaction.
	pub fn changes(&self) -> impl Iterator<Item = (&StorageKey, &OverlayedEntry<V>)> {
		self.changes.iter()
	}

	/// Get a list of all changes as seen by current transaction, consumes
	/// the overlay.
	pub fn into_changes(self) -> impl Iterator<Item = (StorageKey, OverlayedEntry<V>)> {
		self.changes.into_iter()
	}

	pub fn drain_changes(&mut self) -> BTreeMap<StorageKey, OverlayedEntry<V>> {
		sp_std::mem::take(&mut self.changes)
	}

	/// Consume this changeset and return all committed changes.
	///
	/// Panics:
	/// Panics if there are open transactions: `transaction_depth() > 0`
	pub fn drain_commited(self) -> impl Iterator<Item = (StorageKey, RcT<V>)> {
		self.changes.into_iter().map(|(k, mut v)| (k, v.pop_transaction().value))
	}

	/// Rollback the last transaction started by `start_transaction`.
	///
	/// Any changes made during that transaction are discarded. Returns an error if
	/// there is no open transaction that can be rolled back.
	pub fn rollback_transaction(&mut self, dirty_keys: Set<StorageKey>) -> Result<(), NoOpenTransaction> {
		self.close_transaction(true, dirty_keys)
	}

	/// Commit the last transaction started by `start_transaction`.
	///
	/// Any changes made during that transaction are committed. Returns an error if
	/// there is no open transaction that can be committed.
	pub fn commit_transaction(&mut self, dirty_keys: Set<StorageKey>) -> Result<(), NoOpenTransaction> {
		self.close_transaction(false, dirty_keys)
	}

	fn close_transaction(&mut self, rollback: bool, dirty_keys: Set<StorageKey>) -> Result<(), NoOpenTransaction> {
		for key in dirty_keys {
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
					*overlayed.value_mut() = dropped_tx.value;
					overlayed.transaction_extrinsics_mut().extend(dropped_tx.extrinsics);
				}
			}
		}

		Ok(())
	}
}

impl<V: Clone> StorageOverlay<StorageKey, V> {
	/// Get a mutable reference for a value.
	///
	/// Can be rolled back or committed when called inside a transaction.
	#[must_use = "A change was registered, so this value MUST be modified."]
	pub fn modify(
		&mut self,
		key: StorageKey,
		init: Option<impl FnOnce() -> Option<V>>,
		first_write_in_tx: bool,
		at_extrinsic: Option<u32>,
	) -> Option<&mut RcT<V>> {
		let overlayed = self.changes.entry(key).or_default();
		let mut muted = false;
		let clone_into_new_tx = if let Some(tx) = overlayed.transactions.last() {
			if first_write_in_tx {
				muted = tx.value.muted();
				Some(tx.value.clone_inner())
			} else {
				None
			}
		} else {
			if let Some(init) = init {
				Some(init())
			} else {
				return None;
			}
		};

		if let Some(cloned) = clone_into_new_tx {
			overlayed.set(cloned, first_write_in_tx, muted, at_extrinsic);
		}
		Some(overlayed.value_mut())
	}

	pub fn modify_append(
		&mut self,
		key: StorageKey,
		init: impl FnOnce() -> V,
		first_write_in_tx: bool,
		at_extrinsic: Option<u32>,
	) -> Option<&mut RcT<V>> {
		let overlayed = self.changes.entry(key).or_default();
		let mut muted = false;
		let clone_into_new_tx = if let Some(tx) = overlayed.transactions.last() {
			if tx.value.borrow().is_some() {
				if first_write_in_tx {
					muted = tx.value.muted();
					Some(tx.value.clone_inner())
				} else {
					None
				}
			} else {
				Some(Some(init()))
			}
		} else {
			Some(Some(init()))
		};

		if let Some(cloned) = clone_into_new_tx {
			overlayed.set(cloned, first_write_in_tx, muted, at_extrinsic);
		}
		Some(overlayed.value_mut())
	}

	/// Get the iterator over all changes that follow the supplied `key`.
	pub fn changes_after(&self, key: &[u8]) -> impl Iterator<Item = (&[u8], &OverlayedEntry<V>)> {
		use sp_std::ops::Bound;
		let range = (Bound::Excluded(key), Bound::Unbounded);
		self.changes.range::<[u8], _>(range).map(|(k, v)| (k.as_slice(), v))
	}
}

