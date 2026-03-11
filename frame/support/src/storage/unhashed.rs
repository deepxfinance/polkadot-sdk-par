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
// limitations under the License.

//! Operation on unhashed runtime storage.

use codec::Encode;
use crate::storage::{TStorageOverlay, TypedAppend};
#[cfg(not(feature = "std"))]
pub use unhashed_no_std::*;
#[cfg(feature = "std")]
pub use unhashed_std::*;

#[cfg(all(feature = "std", feature = "dev-time"))]
lazy_static::lazy_static! {
	pub static ref GLOBAL_ENCODE: std::sync::Mutex<std::collections::HashMap<Vec<u8>, Vec<(std::time::Duration, std::time::Duration, usize)>>> = std::sync::Mutex::new(std::collections::HashMap::new());
	pub static ref GLOBAL_DECODE: std::sync::Mutex<std::collections::HashMap<Vec<u8>, Vec<(std::time::Duration, std::time::Duration, usize)>>> = std::sync::Mutex::new(std::collections::HashMap::new());
}

#[cfg(not(feature = "std"))]
mod unhashed_no_std {
	use codec::{Decode, Encode};
	use crate::storage::unhashed::kill;

	fn key_prefix(key: &[u8]) -> &[u8] {
		&key[..key.len().min(32)]
	}

	fn parse_type<T: Decode>(key: &[u8], val: Vec<u8>) -> Option<T> {
		#[cfg(all(feature = "std", feature = "dev-time"))]
		let decode_start = std::time::Instant::now();
		let res = Decode::decode(&mut val.as_slice()).map(Some).unwrap_or_else(|e| {
			// TODO #3700: error should be handleable.
			log::error!(
			target: "runtime::storage",
			"Corrupted state at `{:?}: {:?}`",
			key,
			e,
		);
			None
		});
		#[cfg(all(feature = "std", feature = "dev-time"))]
		let decode_time = decode_start.elapsed();
		#[cfg(all(feature = "std", feature = "dev-time"))]
		if res.is_some() {
			let time = start.elapsed();
			let mut lock = GLOBAL_DECODE.lock().unwrap();
			let mut key = key.to_vec();
			if key.len() > 32 {
				key.resize(32, 0);
			}
			if let Some(v) = lock.get_mut(&key) {
				v.push((decode_time, time, val.len()));
			} else {
				lock.insert(key, vec![(decode_time, time, val.len())]);
			}
		}
		res
	}

	/// Return the value of the item in storage under `key`, or `None` if there is no explicit entry.
	pub fn get<T: Decode + Sized>(key: &[u8]) -> Option<T> {
		sp_io::storage::get(key).and_then(|val| parse_type(key, val.to_vec()))
	}

	/// Return the value of the item in storage under `key`, or the type's default if there is no
	/// explicit entry.
	pub fn get_or_default<T: Decode + Sized + Default>(key: &[u8]) -> T {
		get(key).unwrap_or_default()
	}

	/// Return the value of the item in storage under `key`, or `default_value` if there is no
	/// explicit entry.
	pub fn get_or<T: Decode + Sized>(key: &[u8], default_value: T) -> T {
		get(key).unwrap_or(default_value)
	}

	/// Return the value of the item in storage under `key`, or `default_value()` if there is no
	/// explicit entry.
	pub fn get_or_else<T: Decode + Sized, F: FnOnce() -> T>(key: &[u8], default_value: F) -> T {
		get(key).unwrap_or_else(default_value)
	}

	pub fn get_cache_ref<T: Decode + Sized>(key: &[u8]) -> RcT<T> {
		RcT::new(key, get(key))
	}

	/// Direct encode `item` to `value`.
	pub fn append<Item: Encode>(key: &[u8], item: Item) {
		#[cfg(all(feature = "std", feature = "dev-time"))]
		let start = std::time::Instant::now();
		let encoded = item.encode();
		#[cfg(all(feature = "std", feature = "dev-time"))]
		let encode_time = start.elapsed();
		#[cfg(all(feature = "std", feature = "dev-time"))]
		let len = encoded.len();
		sp_io::storage::append(&key, encoded);
		#[cfg(all(feature = "std", feature = "dev-time"))]
		{
			let time = start.elapsed();
			let mut lock = crate::storage::unhashed::GLOBAL_ENCODE.lock().unwrap();
			if let Some(v) = lock.get_mut(key_prefix(key)) {
				v.push((encode_time, time, len));
			} else {
				lock.insert(key_prefix(key).to_vec(), vec![(encode_time, time, len)]);
			}
		}
	}

	/// Put `value` in storage under `key`.
	pub fn put<T: Encode + ?Sized>(key: &[u8], value: &T) {
		#[cfg(all(feature = "std", feature = "dev-time"))]
		let start = std::time::Instant::now();
		let slice = value.encode();
		#[cfg(all(feature = "std", feature = "dev-time"))]
		let encode_time = start.elapsed();
		sp_io::storage::set(key, slice.as_slice());
		#[cfg(all(feature = "std", feature = "dev-time"))]
		{
			let time = start.elapsed();
			let mut lock = GLOBAL_ENCODE.lock().unwrap();
			let mut key = key.to_vec();
			if key.len() > 32 {
				key.resize(32, 0);
			}
			if let Some(v) = lock.get_mut(&key) {
				v.push((encode_time, time, slice.len()));
			} else {
				lock.insert(key, vec![(encode_time, time, slice.len())]);
			}
		}
		// value.using_encoded(|slice| sp_io::storage::set(key, slice));
	}

	/// Remove `key` from storage, returning its value if it had an explicit entry or `None` otherwise.
	pub fn take<T: Decode + Sized>(key: &[u8]) -> Option<T> {
		let r = crate::storage::unhashed::get(key);
		if r.is_some() {
			kill(key);
		}
		r
	}

	/// Remove `key` from storage, returning its value, or, if there was no explicit entry in storage,
	/// the default for its type.
	pub fn take_or_default<T: Decode + Sized + Default>(key: &[u8]) -> T {
		take(key).unwrap_or_default()
	}

	/// Return the value of the item in storage under `key`, or `default_value` if there is no
	/// explicit entry. Ensure there is no explicit entry on return.
	pub fn take_or<T: Decode + Sized>(key: &[u8], default_value: T) -> T {
		take(key).unwrap_or(default_value)
	}

	/// Return the value of the item in storage under `key`, or `default_value()` if there is no
	/// explicit entry. Ensure there is no explicit entry on return.
	pub fn take_or_else<T: Decode + Sized, F: FnOnce() -> T>(key: &[u8], default_value: F) -> T {
		take(key).unwrap_or_else(default_value)
	}

	/// Check to see if `key` has an explicit entry in storage.
	pub fn exists(key: &[u8]) -> bool {
		sp_io::storage::exists(key)
	}

	/// Ensure `key` has no explicit entry in storage.
	pub fn kill(key: &[u8]) {
		sp_io::storage::clear(key);
	}

	/// Ensure keys with the given `prefix` have no entries in storage.
	#[deprecated = "Use `clear_prefix` instead"]
	pub fn kill_prefix(prefix: &[u8], limit: Option<u32>) -> sp_io::KillStorageResult {
		// TODO: Once the network has upgraded to include the new host functions, this code can be
		// enabled.
		// clear_prefix(prefix, limit).into()
		sp_io::storage::clear_prefix(prefix, limit)
	}

	/// Partially clear the storage of all keys under a common `prefix`.
	///
	/// # Limit
	///
	/// A *limit* should always be provided through `maybe_limit`. This is one fewer than the
	/// maximum number of backend iterations which may be done by this operation and as such
	/// represents the maximum number of backend deletions which may happen. A *limit* of zero
	/// implies that no keys will be deleted, though there may be a single iteration done.
	///
	/// The limit can be used to partially delete storage items in case it is too large or costly
	/// to delete all in a single operation.
	///
	/// # Cursor
	///
	/// A *cursor* may be passed in to this operation with `maybe_cursor`. `None` should only be
	/// passed once (in the initial call) for any attempt to clear storage. In general, subsequent calls
	/// operating on the same prefix should pass `Some` and this value should be equal to the
	/// previous call result's `maybe_cursor` field. The only exception to this is when you can
	/// guarantee that the subsequent call is in a new block; in this case the previous call's result
	/// cursor need not be passed in an a `None` may be passed instead. This exception may be useful
	/// then making this call solely from a block-hook such as `on_initialize`.
	///
	/// Returns [`MultiRemovalResults`](sp_io::MultiRemovalResults) to inform about the result. Once the
	/// resultant `maybe_cursor` field is `None`, then no further items remain to be deleted.
	///
	/// NOTE: After the initial call for any given child storage, it is important that no keys further
	/// keys are inserted. If so, then they may or may not be deleted by subsequent calls.
	///
	/// # Note
	///
	/// Please note that keys which are residing in the overlay for the child are deleted without
	/// counting towards the `limit`.
	pub fn clear_prefix(
		prefix: &[u8],
		maybe_limit: Option<u32>,
		_maybe_cursor: Option<&[u8]>,
	) -> sp_io::MultiRemovalResults {
		// TODO: Once the network has upgraded to include the new host functions, this code can be
		// enabled.
		// sp_io::storage::clear_prefix(prefix, maybe_limit, maybe_cursor)
		use sp_io::{KillStorageResult::*, MultiRemovalResults};
		#[allow(deprecated)]
		let (maybe_cursor, i) = match kill_prefix(prefix, maybe_limit) {
			AllRemoved(i) => (None, i),
			SomeRemaining(i) => (Some(prefix.to_vec()), i),
		};
		MultiRemovalResults { maybe_cursor, backend: i, unique: i, loops: i }
	}

	/// Returns `true` if the storage contains any key, which starts with a certain prefix,
	/// and is longer than said prefix.
	/// This means that a key which equals the prefix will not be counted.
	pub fn contains_prefixed_key(prefix: &[u8]) -> bool {
		match sp_io::storage::next_key(prefix) {
			Some(key) => key.starts_with(prefix),
			None => false,
		}
	}

	/// Get a Vec of bytes from storage.
	pub fn get_raw(key: &[u8]) -> Option<Vec<u8>> {
		#[cfg(all(feature = "std", feature = "dev-time"))]
		let start = std::time::Instant::now();
		let res = sp_io::storage::get(key).map(|value| value.to_vec());
		#[cfg(all(feature = "std", feature = "dev-time"))]
		{
			let time = start.elapsed();
			let mut lock = GLOBAL_DECODE.lock().unwrap();
			let mut key = key.to_vec();
			if key.len() > 32 {
				key.resize(32, 0);
			}
			if let Some(v) = lock.get_mut(&key) {
				v.push((Default::default(), time, res.as_ref().map(|r| r.len()).unwrap_or(0)));
			} else {
				lock.insert(key, vec![(Default::default(), time, res.as_ref().map(|r| r.len()).unwrap_or(0))]);
			}
		}
		res
	}

	/// Put a raw byte slice into storage.
	///
	/// **WARNING**: If you set the storage of the Substrate Wasm (`well_known_keys::CODE`),
	/// you should also call `frame_system::RuntimeUpgraded::put(true)` to trigger the
	/// `on_runtime_upgrade` logic.
	pub fn put_raw(key: &[u8], value: &[u8]) {
		#[cfg(all(feature = "std", feature = "dev-time"))]
		let start = std::time::Instant::now();
		sp_io::storage::set(key, value);
		#[cfg(all(feature = "std", feature = "dev-time"))]
		{
			let time = start.elapsed();
			let mut lock = GLOBAL_ENCODE.lock().unwrap();
			let mut key = key.to_vec();
			if key.len() > 32 {
				key.resize(32, 0);
			}
			if let Some(v) = lock.get_mut(&key) {
				v.push((Default::default(), time, value.len()));
			} else {
				lock.insert(key, vec![(Default::default(), time, value.len())]);
			}
		}
	}

	/// Take raw byte slice from storage
	pub fn take_raw(key: &[u8])  -> Option<Vec<u8>> {
		let res = get_raw(key);
		if res.is_some() {
			kill(key);
		}
		res
	}
}

#[cfg(feature = "std")]
mod unhashed_std {
	use codec::{Decode, Encode};
	use typed_cache::{QueryTransfer, RcT, StorageValue, TStorage, TStorageOverlay};
	use crate::storage::TypedAppend;

	/// Return the value of the item in storage under `key`, or `None` if there is no explicit entry.
	pub fn get<T: TStorageOverlay>(key: &[u8]) -> Option<T> {
		get_cache_ref(key).clone_inner()
	}

	/// Return the value of the item in storage under `key`, or the type's default if there is no
	/// explicit entry.
	pub fn get_or_default<T: TStorageOverlay + Default>(key: &[u8]) -> T {
		get(key).unwrap_or_default()
	}

	/// explicit entry.
	pub fn get_or<T: TStorageOverlay>(key: &[u8], default_value: T) -> T {
		get(key).unwrap_or(default_value)
	}
	/// Return the value of the item in storage under `key`, or `default_value()` if there is no
	/// explicit entry.
	pub fn get_or_else<T: TStorageOverlay, F: FnOnce() -> T>(key: &[u8], default_value: F) -> T {
		get(key).unwrap_or_else(default_value)
	}

	pub fn put<T: TStorageOverlay>(key: &[u8], val: T) {
		sp_io::mut_externalities(|ext|
			ext.place_storage(key.to_vec(), Some(StorageValue::new_rct(Some(val), true)))
		);
	}

	pub fn non_f<T>(_k: &[u8]) -> Option<T> { None }

	pub fn non_t<T>() -> Option<T> { None }

	pub fn get_cache_ref<T: TStorageOverlay>(key: &[u8]) -> RcT<T> {
		let mut value = sp_io::mut_externalities(|ext| {
			ext.storage(key).expect("Not expected to fail")
		});
		if value.is_raw() {
			sp_io::mut_typed_cache(|o| {
				o.get_ref(key)
			})
				.expect("Not expected to fail")
				.expect("Not expected to fail")
		} else {
			value.downcast_mut::<T>().clone_ref()
		}
	}

	pub fn mutate<QT: QueryTransfer<T>, T: TStorageOverlay, Ini, F, R, E>(key: &[u8], _ini: Ini, f: F) -> Result<R, E>
	where
		Ini: FnOnce() -> Option<T>,
		F: FnOnce(&mut QT::Query) -> Result<R, E>
	{
		let cached = sp_io::mut_typed_cache(|o| o.cached(key))
			.expect("mut_typed_cache SHOULD return Some result");
		if !cached { let _ = get_cache_ref::<T>(key); }
		sp_io::mut_typed_cache(|o| o.mutate::<QT, T, Ini, _, _, F>(key, None, f))
			.expect("typed_cache should exists")
			.expect("mutate should finish")
	}

	pub fn take<T: TStorageOverlay>(key: &[u8]) -> Option<T> {
		match sp_io::mut_typed_cache(
			|o| o.take::<T>(key),
		)
			.expect("mut_typed_cache SHOULD return Some result") {
			Some(value) => value,
			None => {
				let _ = get_cache_ref::<T>(key);
				take(key)
			}
		}
	}

	pub fn take_or_default<T: TStorageOverlay + Default>(key: &[u8]) -> T {
		take(key).unwrap_or_default()
	}

	pub fn take_or<T: TStorageOverlay>(key: &[u8], default_value: T) -> T {
		take(key).unwrap_or(default_value)
	}

	pub fn take_or_else<T: TStorageOverlay, F: FnOnce() -> T>(key: &[u8], default_value: F) -> T {
		take(key).unwrap_or_else(default_value)
	}

	pub fn kill(key: &[u8]) {
		sp_io::mut_typed_cache(|o| o.kill(key))
			.expect("mut_typed_cache SHOULD return Some result");
	}

	pub fn exists(key: &[u8]) -> bool {
		sp_io::storage::exists(key)
	}

	pub fn append<T: TStorageOverlay, Item: Encode + Clone>(key: &[u8], item: Item)
	where
		T: TypedAppend<Item> + TStorage
	{
		get_cache_ref::<T>(key).mutate_value_query(|v| v.append(item))
	}

	pub fn clear_prefix(
		_prefix: &[u8],
		_maybe_limit: Option<u32>,
		_maybe_cursor: Option<&[u8]>,
	) -> sp_io::MultiRemovalResults {
		panic!("`unhashed_std::clear_prefix` not supported")
	}

	pub fn contains_prefixed_key(_prefix: &[u8]) -> bool {
		panic!("`unhashed_std::contains_prefixed_key` not supported")
	}

	pub fn get_raw(key: &[u8]) -> Option<Vec<u8>> {
		sp_io::mut_typed_cache(|o| o.get_raw(key, true))
			.expect("mut_typed_cache SHOULD return Some result")
	}

	pub fn put_raw(key: &[u8], value: &[u8]) {
		sp_io::mut_typed_cache(|o| o.put_raw(key, value.to_vec()))
			.expect("mut_typed_cache SHOULD return Some result")
	}

	pub fn take_raw(key: &[u8])  -> Option<Vec<u8>> {
		sp_io::mut_typed_cache(|o| o.take_raw(key))
			.expect("mut_typed_cache SHOULD return Some result")?
	}
}
