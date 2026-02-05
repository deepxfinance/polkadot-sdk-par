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

use codec::{Decode, Encode, FullCodec};
use sp_std::prelude::*;
use crate::storage::{QueryTransfer, TypedAppend, TStorage,  RcT};

#[cfg(all(feature = "std", feature = "dev-time"))]
lazy_static::lazy_static! {
	pub static ref GLOBAL_ENCODE: std::sync::Mutex<std::collections::HashMap<Vec<u8>, Vec<(std::time::Duration, std::time::Duration, usize)>>> = std::sync::Mutex::new(std::collections::HashMap::new());
	pub static ref GLOBAL_DECODE: std::sync::Mutex<std::collections::HashMap<Vec<u8>, Vec<(std::time::Duration, std::time::Duration, usize)>>> = std::sync::Mutex::new(std::collections::HashMap::new());
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

#[cfg(feature = "std")]
pub fn contains_key_cache<QT: QueryTransfer<T>, T: FullCodec + TStorage>(key: &[u8]) -> bool {
	sp_io::mut_typed_cache(|o| o.contains_key::<QT, T>(key_prefix(key), &key))
		.map(|v| v.unwrap_or(exists(key)))
		.unwrap_or(exists(key))
}

#[cfg(feature = "std")]
pub fn put_cache<QT: QueryTransfer<T>, T: FullCodec + TStorage>(key: &[u8], val: T) {
	if sp_io::mut_typed_cache(|_| ()).is_none() {
		put(key, &val);
	} else {
		sp_io::mut_typed_cache(|o| o.put::<QT, _>(key_prefix(key), &key, val));
	}
}

#[cfg(feature = "std")]
pub fn get_cache_ref<QT: QueryTransfer<T>, T: FullCodec + TStorage, F>(key: &[u8], _f: F) -> RcT<QT::Qry>
where
	F: Fn(&[u8]) -> Option<T>
{
	match sp_io::mut_typed_cache(
		|o| o.get_ref::<QT, T, F>(key_prefix(key), key, None),
	) {
		Some(Some(value)) => value,
		Some(None) => {
			let res = get::<T>(key);
			sp_io::mut_typed_cache(|o| o.init::<QT, _>(key_prefix(key), key, res));
			sp_io::mut_typed_cache(|o| o.get_ref::<QT, T, F>(key_prefix(key), key, None))
				.expect("get_cache_ref should enable typed_cache")
				.expect("get_cache_ref should have initialized")
		}
		None => panic!("get_cache_ref should enable typed_cache"),
	}
}

#[cfg(not(feature = "std"))]
pub fn get_cache_ref<QT: QueryTransfer<T>, T: Decode + Sized, F>(key: &[u8], f: F) -> RcT<QT::Qry>
where
	F: Fn(&[u8]) -> Option<T>
{
	RcT::new(key, QT::from_optional_value_to_query(f(key)))
}

#[cfg(feature = "std")]
pub fn get_cache<QT: QueryTransfer<T>, T: FullCodec + TStorage, F>(key: &[u8], _f: F) -> Option<T> where F: Fn(&[u8]) -> Option<T> {
	match sp_io::mut_typed_cache(
		|o| o.get::<QT, T, F>(key_prefix(key), key, None),
	) {
		Some(Some(value)) => value,
		Some(None) => {
			let res = get(key);
			sp_io::mut_typed_cache(|o| o.init::<QT, _>(key_prefix(key), key, res.clone()));
			res
		}
		None => get(key),
	}
}

/// If `f` used, return `true`.
/// If `f` not used, return `false`. Then we should try raw cache.
#[cfg(feature = "std")]
pub fn mutate_cache<QT: QueryTransfer<T>, T: FullCodec + TStorage, Ini, F, R, E>(key: &[u8], _ini: Ini, f: F) -> Result<R, E>
where
	Ini: FnOnce() -> Option<T>,
	F: FnOnce(&mut QT::Qry) -> Result<R, E>
{
	let key_prefix = key_prefix(key);
	match sp_io::mut_typed_cache(
		|o| o.cached::<QT, T>(key_prefix, key),
	) {
		Some(cached) => if cached {
			sp_io::mut_typed_cache(|o| o.mutate::<QT, T, Ini, F, _, _>(key_prefix, key, None, f))
				.expect("typed_cache should exists")
				.expect("mutate should finish")
		} else {
			let init = get::<T>(key);
			sp_io::mut_typed_cache(|o| o.mutate::<QT, T, _, F, _, _>(key_prefix, key, Some(|| { init }), f))
				.expect("typed_cache should exists")
				.expect("mutate should finish")
		},
		None => {
			let mut val = QT::from_optional_value_to_query(get(key));
			let ret = f(&mut val);
			if ret.is_ok() {
				match QT::from_query_to_optional_value(val) {
					Some(ref val) => put(key, val),
					None => kill(key),
				}
			}
			ret
		}
	}
}

#[cfg(feature = "std")]
pub fn take_cache<QT: QueryTransfer<T>, T: FullCodec + TStorage>(key: &[u8]) -> Option<T> {
	match sp_io::mut_typed_cache(
		|o| o.take::<QT, T>(key_prefix(key), key),
	) {
		Some(Some(value)) => QT::from_query_to_optional_value(value),
		Some(None) => {
			let res = get(key);
			if res.is_some() {
				sp_io::mut_typed_cache(|o| o.kill::<QT, T>(key_prefix(key), key));
			} else {
				sp_io::mut_typed_cache(|o| o.init::<QT, _>(key_prefix(key), key, res.clone()));
			}
			res
		}
		None => take(key),
	}
}

#[cfg(feature = "std")]
pub fn kill_cache<QT: QueryTransfer<T>, T: FullCodec + TStorage>(key: &[u8]) {
	if sp_io::mut_typed_cache(|_| ()).is_none() {
		kill(key);
	} else {
		sp_io::mut_typed_cache(|o| o.kill::<QT, T>(key_prefix(key), key));
	}
}

#[cfg(feature = "std")]
pub fn append_cache<QT: QueryTransfer<T>, T, Item: Encode>(key: &[u8], item: Item)
where
	T: TypedAppend<Item> + FullCodec + TStorage
{
	sp_io::mut_externalities(|ext|  {
		let mut raw_value = None;
		if let Some(overlay) = ext.overlay_cache() {
			if overlay.contains_key::<QT, T>(key_prefix(key), key).is_none() {
				raw_value = ext.storage(&key);
			};
		};
		if let Some(overlay) = ext.overlay_cache() {
			if !overlay.append::<QT, T, _, _>(
				key_prefix(key),
				&key,
				|| { raw_value.clone().and_then(|val| parse_type(key, val)).unwrap_or_default() },
				item
			) {
				panic!("append_cache should success");
			}
		} else {
			append(key, item);
		}
	})
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
	let r = get(key);
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
