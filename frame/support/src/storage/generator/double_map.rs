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

use crate::{
	hash::{ReversibleStorageHasher, StorageHasher},
	storage::{self, storage_prefix, unhashed, KeyPrefixIterator, PrefixIterator, StorageAppend, TStorage},
	Never,
};
use codec::{Decode, Encode, EncodeLike, FullCodec, FullEncode};
use sp_std::prelude::*;
use scale_info::prelude::string::String;
use crate::storage::TypedAppend;

/// Generator for `StorageDoubleMap` used by `decl_storage`.
///
/// # Mapping of keys to a storage path
///
/// The storage key (i.e. the key under which the `Value` will be stored) is created from two parts.
/// The first part is a hash of a concatenation of the `key1_prefix` and `Key1`. And the second part
/// is a hash of a `Key2`.
///
/// Thus value for (key1, key2) is stored at:
/// ```nocompile
/// Twox128(module_prefix) ++ Twox128(storage_prefix) ++ Hasher1(encode(key1)) ++ Hasher2(encode(key2))
/// ```
///
/// # Warning
///
/// If the key1s are not trusted (e.g. can be set by a user), a cryptographic `hasher` such as
/// `blake2_256` must be used for Hasher1. Otherwise, other values in storage can be compromised.
/// If the key2s are not trusted (e.g. can be set by a user), a cryptographic `hasher` such as
/// `blake2_256` must be used for Hasher2. Otherwise, other items in storage with the same first
/// key can be compromised.
pub trait StorageDoubleMap<K1: FullEncode, K2: FullEncode, V: FullCodec + TStorage> {
	/// The type that get/take returns.
	type Query;

	/// Hasher for the first key.
	type Hasher1: StorageHasher;

	/// Hasher for the second key.
	type Hasher2: StorageHasher;

	/// Module prefix. Used for generating final key.
	fn module_prefix() -> &'static [u8];

	/// Storage prefix. Used for generating final key.
	fn storage_prefix() -> &'static [u8];

	/// The full prefix; just the hash of `module_prefix` concatenated to the hash of
	/// `storage_prefix`.
	fn prefix_hash() -> Vec<u8> {
		let result = storage_prefix(Self::module_prefix(), Self::storage_prefix());
		result.to_vec()
	}

	/// Convert an optional value retrieved from storage to the type queried.
	fn from_optional_value_to_query(v: Option<V>) -> Self::Query;

	/// Convert a query to an optional value into storage.
	fn from_query_to_optional_value(v: Self::Query) -> Option<V>;

	/// Generate the first part of the key used in top storage.
	fn storage_double_map_final_key1<KArg1>(k1: KArg1) -> Vec<u8>
	where
		KArg1: EncodeLike<K1>,
	{
		let storage_prefix = storage_prefix(Self::module_prefix(), Self::storage_prefix());
		let key_hashed = k1.using_encoded(Self::Hasher1::hash);

		let mut final_key = Vec::with_capacity(storage_prefix.len() + key_hashed.as_ref().len());

		final_key.extend_from_slice(&storage_prefix);
		final_key.extend_from_slice(key_hashed.as_ref());

		final_key
	}

	/// Generate the full key used in top storage.
	fn storage_double_map_final_key<KArg1, KArg2>(k1: KArg1, k2: KArg2) -> Vec<u8>
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>,
	{
		let storage_prefix = storage_prefix(Self::module_prefix(), Self::storage_prefix());
		let key1_hashed = k1.using_encoded(Self::Hasher1::hash);
		let key2_hashed = k2.using_encoded(Self::Hasher2::hash);

		let mut final_key = Vec::with_capacity(
			storage_prefix.len() + key1_hashed.as_ref().len() + key2_hashed.as_ref().len(),
		);

		final_key.extend_from_slice(&storage_prefix);
		final_key.extend_from_slice(key1_hashed.as_ref());
		final_key.extend_from_slice(key2_hashed.as_ref());

		final_key
	}
}

impl<K1, K2, V, G> storage::StorageDoubleMap<K1, K2, V> for G
where
	K1: FullEncode,
	K2: FullEncode,
	V: FullCodec + TStorage,
	G: StorageDoubleMap<K1, K2, V>,
{
	type Query = G::Query;

	fn hashed_key_for<KArg1, KArg2>(k1: KArg1, k2: KArg2) -> Vec<u8>
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>,
	{
		Self::storage_double_map_final_key(k1, k2)
	}

	fn contains_key<KArg1, KArg2>(k1: KArg1, k2: KArg2) -> bool
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>,
	{
		let key = Self::storage_double_map_final_key(k1, k2);
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_none() {
			unhashed::exists(&key)
		} else {
			Self::get_cache(&key, |_| { Option::<V>::None }).is_some()
		}
		#[cfg(not(feature = "std"))]
		unhashed::exists(&key)
	}

	#[cfg(feature = "std")]
	fn get_cache<F>(key: &[u8], _f: F) -> Option<V> where F: Fn(&[u8]) -> Option<V> {
		match sp_io::mut_typed_cache(
			|o| o.get::<V, F>(&key[..32], key, None),
		) {
			Some(Some(value)) => value,
			Some(None) => {
				let res = unhashed::get(key);
				sp_io::mut_typed_cache(|o| o.cache(&key[..32], key, res.clone()));
				res
			}
			None => unhashed::get(key),
		}
	}

	#[cfg(feature = "std")]
	fn put_cache(key: &[u8], val: V) {
		if sp_io::mut_typed_cache(|_| ()).is_none() {
			unhashed::put(key, &val);
		} else {
			sp_io::mut_typed_cache(|o| o.put(&key[..32], &key, val));
		}
	}

	#[cfg(feature = "std")]
	fn kill_cache(key: &[u8]) {
		if sp_io::mut_typed_cache(|_| ()).is_none() {
			unhashed::kill(key);
		} else {
			sp_io::mut_typed_cache(|o| o.kill::<V>(&key[..32], key));
		}
	}

	#[cfg(feature = "std")]
	fn take_cache<F>(key: &[u8], _f: F) -> Option<V> where F: Fn(&[u8]) -> Option<V> {
		match sp_io::mut_typed_cache(
			|o| o.take::<V, F>(&key[..32], key, None),
		) {
			Some(Some(value)) => value,
			Some(None) => {
				let res = unhashed::take(key);
				if res.is_some() {
					sp_io::mut_typed_cache(|o| o.kill::<V>(&key[..32], key));
				} else {
					sp_io::mut_typed_cache(|o| o.cache(&key[..32], key, res.clone()));
				}
				res
			}
			None => unhashed::take(key),
		}
	}

	fn get<KArg1, KArg2>(k1: KArg1, k2: KArg2) -> Self::Query
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>,
	{
		#[cfg(feature = "std")]
		{
			G::from_optional_value_to_query(Self::get_cache(
				&Self::storage_double_map_final_key(k1, k2),
				|_| { Option::<V>::None }
			))
		}
		#[cfg(not(feature = "std"))]
		G::from_optional_value_to_query(unhashed::get(&Self::storage_double_map_final_key(k1, k2)))
	}

	fn try_get<KArg1, KArg2>(k1: KArg1, k2: KArg2) -> Result<V, ()>
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>,
	{
		#[cfg(feature = "std")]
		{
			Self::get_cache(
				&Self::storage_double_map_final_key(k1, k2),
				|_| { Option::<V>::None }
			)
				.ok_or(())
		}
		#[cfg(not(feature = "std"))]
		unhashed::get(&Self::storage_double_map_final_key(k1, k2)).ok_or(())
	}

	fn set<KArg1: EncodeLike<K1>, KArg2: EncodeLike<K2>>(k1: KArg1, k2: KArg2, q: Self::Query) {
		match G::from_query_to_optional_value(q) {
			Some(v) => Self::insert(k1, k2, v),
			None => Self::remove(k1, k2),
		}
	}

	fn take<KArg1, KArg2>(k1: KArg1, k2: KArg2) -> Self::Query
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>,
	{
		let final_key = Self::storage_double_map_final_key(k1, k2);
		#[cfg(feature = "std")]
		let value = Self::take_cache(&final_key, |_| { Option::<V>::None });
		#[cfg(not(feature = "std"))]
		let value = unhashed::take(&final_key);
		G::from_optional_value_to_query(value)
	}

	fn swap<XKArg1, XKArg2, YKArg1, YKArg2>(x_k1: XKArg1, x_k2: XKArg2, y_k1: YKArg1, y_k2: YKArg2)
	where
		XKArg1: EncodeLike<K1>,
		XKArg2: EncodeLike<K2>,
		YKArg1: EncodeLike<K1>,
		YKArg2: EncodeLike<K2>,
	{
		let final_x_key = Self::storage_double_map_final_key(x_k1, x_k2);
		let final_y_key = Self::storage_double_map_final_key(y_k1, y_k2);

		#[cfg(feature = "std")]
		{
			let v1 = Self::get_cache(&final_x_key, |_| { Option::<V>::None });
			if let Some(val) = Self::get_cache(&final_y_key, |_| { Option::<V>::None }) {
				Self::put_cache(&final_x_key, val);
			} else {
				Self::kill_cache(&final_x_key);
			}
			if let Some(val) = v1 {
				Self::put_cache(&final_y_key, val);
			} else {
				Self::kill_cache(&final_y_key);
			}
		}
		#[cfg(not(feature = "std"))]
		{
			let v1 = unhashed::get_raw(&final_x_key);
			if let Some(val) = unhashed::get_raw(&final_y_key) {
				unhashed::put_raw(&final_x_key, &val);
			} else {
				unhashed::kill(&final_x_key)
			}
			if let Some(val) = v1 {
				unhashed::put_raw(&final_y_key, &val);
			} else {
				unhashed::kill(&final_y_key)
			}
		}
	}

	#[cfg(feature = "std")]
	fn insert<KArg1, KArg2>(k1: KArg1, k2: KArg2, val: V)
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>
	{
		log::trace!(target: "storage_dev", "double map insert {} {}",
			String::from_utf8(Self::module_prefix().to_vec()).unwrap(),
			String::from_utf8(Self::storage_prefix().to_vec()).unwrap(),
		);
		#[cfg(feature = "std")]
		Self::put_cache(&Self::storage_double_map_final_key(k1, k2), val);
		#[cfg(not(feature = "std"))]
		unhashed::put(&Self::storage_double_map_final_key(k1, k2), &val)
	}

	#[cfg(not(feature = "std"))]
	fn insert<KArg1, KArg2, VArg>(k1: KArg1, k2: KArg2, val: VArg)
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>,
		VArg: EncodeLike<V>,
	{
		log::trace!(target: "storage_dev", "double map insert {} {}",
			String::from_utf8(Self::module_prefix().to_vec()).unwrap(), 
			String::from_utf8(Self::storage_prefix().to_vec()).unwrap(),
		);
		unhashed::put(&Self::storage_double_map_final_key(k1, k2), &val)
	}

	fn remove<KArg1, KArg2>(k1: KArg1, k2: KArg2)
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>,
	{
		#[cfg(feature = "std")]
		Self::kill_cache(&Self::storage_double_map_final_key(k1, k2));
		#[cfg(not(feature = "std"))]
		unhashed::kill(&Self::storage_double_map_final_key(k1, k2))
	}

	// TODO `typed_cache` not support `remove_prefix`
	fn remove_prefix<KArg1>(k1: KArg1, maybe_limit: Option<u32>) -> sp_io::KillStorageResult
	where
		KArg1: EncodeLike<K1>,
	{
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::remove_prefix` not supported") };
		unhashed::clear_prefix(Self::storage_double_map_final_key1(k1).as_ref(), maybe_limit, None)
			.into()
	}

	// TODO `typed_cache` not support `clear_prefix`
	fn clear_prefix<KArg1>(
		k1: KArg1,
		limit: u32,
		maybe_cursor: Option<&[u8]>,
	) -> sp_io::MultiRemovalResults
	where
		KArg1: EncodeLike<K1>,
	{
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::clear_prefix` not supported") };
		unhashed::clear_prefix(
			Self::storage_double_map_final_key1(k1).as_ref(),
			Some(limit),
			maybe_cursor,
		)
		.into()
	}

	// TODO `typed_cache` not support `contains_prefix`
	fn contains_prefix<KArg1>(k1: KArg1) -> bool
	where
		KArg1: EncodeLike<K1>,
	{
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::contains_prefix` not supported") };
		unhashed::contains_prefixed_key(Self::storage_double_map_final_key1(k1).as_ref())
	}

	// TODO `typed_cache` not support `iter_prefix_values`
	fn iter_prefix_values<KArg1>(k1: KArg1) -> storage::PrefixIterator<V>
	where
		KArg1: ?Sized + EncodeLike<K1>,
	{
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::iter_prefix_values` not supported") };
		let prefix = Self::storage_double_map_final_key1(k1);
		storage::PrefixIterator {
			prefix: prefix.clone(),
			previous_key: prefix,
			drain: false,
			closure: |_raw_key, mut raw_value| V::decode(&mut raw_value),
			phantom: Default::default(),
		}
	}

	fn mutate<KArg1, KArg2, R, F>(k1: KArg1, k2: KArg2, f: F) -> R
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>,
		F: FnOnce(&mut Self::Query) -> R,
	{
		Self::try_mutate(k1, k2, |v| Ok::<R, Never>(f(v)))
			.expect("`Never` can not be constructed; qed")
	}

	fn mutate_exists<KArg1, KArg2, R, F>(k1: KArg1, k2: KArg2, f: F) -> R
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>,
		F: FnOnce(&mut Option<V>) -> R,
	{
		Self::try_mutate_exists(k1, k2, |v| Ok::<R, Never>(f(v)))
			.expect("`Never` can not be constructed; qed")
	}

	fn try_mutate<KArg1, KArg2, R, E, F>(k1: KArg1, k2: KArg2, f: F) -> Result<R, E>
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>,
		F: FnOnce(&mut Self::Query) -> Result<R, E>,
	{
		let final_key = Self::storage_double_map_final_key(k1, k2);
		#[cfg(feature = "std")]
		let mut val = G::from_optional_value_to_query(Self::get_cache(
			final_key.as_ref(),
			|_| { Option::<V>::None }
		));
		#[cfg(not(feature = "std"))]
		let mut val = G::from_optional_value_to_query(unhashed::get(final_key.as_ref()));

		let ret = f(&mut val);
		if ret.is_ok() {
			log::trace!(target: "storage_dev", "double map mutate {} {}",
				String::from_utf8(Self::module_prefix().to_vec()).unwrap(), 
				String::from_utf8(Self::storage_prefix().to_vec()).unwrap(),
			);
			match G::from_query_to_optional_value(val) {
				Some(ref val) => {
					#[cfg(feature = "std")]
					Self::put_cache(final_key.as_ref(), val.clone());
					#[cfg(not(feature = "std"))]
					unhashed::put(final_key.as_ref(), val)
				},
				None => {
					#[cfg(feature = "std")]
					Self::kill_cache(final_key.as_ref());
					#[cfg(not(feature = "std"))]
					unhashed::kill(final_key.as_ref())
				},
			}
		}
		ret
	}

	fn try_mutate_exists<KArg1, KArg2, R, E, F>(k1: KArg1, k2: KArg2, f: F) -> Result<R, E>
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>,
		F: FnOnce(&mut Option<V>) -> Result<R, E>,
	{
		let final_key = Self::storage_double_map_final_key(k1, k2);
		#[cfg(feature = "std")]
		let mut val = Self::get_cache(final_key.as_ref(), |_| { Option::<V>::None });
		#[cfg(not(feature = "std"))]
		let mut val = unhashed::get(final_key.as_ref());

		let ret = f(&mut val);
		if ret.is_ok() {
			log::trace!(target: "storage_dev", "double map mutate_exists {} {}",
				String::from_utf8(Self::module_prefix().to_vec()).unwrap(), 
				String::from_utf8(Self::storage_prefix().to_vec()).unwrap(),
			);
			match val {
				Some(ref val) => {
					#[cfg(feature = "std")]
					Self::put_cache(final_key.as_ref(), val.clone());
					#[cfg(not(feature = "std"))]
					unhashed::put(final_key.as_ref(), val)
				},
				None => {
					#[cfg(feature = "std")]
					Self::kill_cache(final_key.as_ref());
					#[cfg(not(feature = "std"))]
					unhashed::kill(final_key.as_ref())
				},
			}
		}
		ret
	}

	#[cfg(feature = "std")]
	fn append<Item: Encode + Clone, KArg1, KArg2>(k1: KArg1, k2: KArg2, item: Item)
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>,
		V: TypedAppend<Item> + TStorage
	{
		log::trace!(target: "storage_dev", "double map append {} {}",
			String::from_utf8(Self::module_prefix().to_vec()).unwrap(),
			String::from_utf8(Self::storage_prefix().to_vec()).unwrap(),
		);
		let final_key = Self::storage_double_map_final_key(k1, k2);
		if sp_io::mut_typed_cache(|_| ()).is_none() {
			let start = std::time::Instant::now();
			let encoded = item.encode();
			let encode_time = start.elapsed();
			let len = encoded.len();
			sp_io::storage::append(&final_key, encoded);
			let time = start.elapsed();
			let mut lock = crate::storage::unhashed::GLOBAL_ENCODE.lock().unwrap();
			if let Some(v) = lock.get_mut(&final_key[..32]) {
				v.push((encode_time, time, len));
			} else {
				lock.insert(final_key[..32].to_vec(), vec![(encode_time, time, len)]);
			}
		} else {
			let mut none_f = Some(|_k: &[u8]| { None });
			none_f.take();
			let updated = sp_io::mut_typed_cache(|o| o.mutate::<V, _, _>(
				&final_key[..32],
				&final_key,
				none_f,
				|t| {
					t.map(|t| t.append(item.clone()));
				}
			)).unwrap();
			if !updated {
				let mut new_value = V::default();
				new_value.append(item);
				sp_io::mut_typed_cache(|o| o.put(&final_key[..32], &final_key, new_value));
			}
		}
	}

	#[cfg(not(feature = "std"))]
	fn append<Item, EncodeLikeItem, KArg1, KArg2>(k1: KArg1, k2: KArg2, item: EncodeLikeItem)
	where
		KArg1: EncodeLike<K1>,
		KArg2: EncodeLike<K2>,
		Item: Encode,
		EncodeLikeItem: EncodeLike<Item>,
		V: StorageAppend<Item>,
	{
		log::trace!(target: "storage_dev", "double map append {} {}",
			String::from_utf8(Self::module_prefix().to_vec()).unwrap(), 
			String::from_utf8(Self::storage_prefix().to_vec()).unwrap(),
		);
		let final_key = Self::storage_double_map_final_key(k1, k2);
		#[cfg(feature = "std")]
		let start = std::time::Instant::now();
		let encoded = item.encode();
		#[cfg(feature = "std")]
		let encode_time = start.elapsed();
		let len = encoded.len();
		sp_io::storage::append(&final_key, encoded);
		#[cfg(feature = "std")]
		{
			let time = start.elapsed();
			let mut key = final_key.clone();
			if key.len() > 32 {
				key.resize(32, 0);
			}
			let mut lock = crate::storage::unhashed::GLOBAL_ENCODE.lock().unwrap();
			if let Some(v) = lock.get_mut(&key[..32]) {
				v.push((encode_time, time, len));
			} else {
				lock.insert(key[..32].to_vec(), vec![(encode_time, time, len)]);
			}
		}
	}

	fn migrate_keys<
		OldHasher1: StorageHasher,
		OldHasher2: StorageHasher,
		KeyArg1: EncodeLike<K1>,
		KeyArg2: EncodeLike<K2>,
	>(
		key1: KeyArg1,
		key2: KeyArg2,
	) -> Option<V> {
		let old_key = {
			let storage_prefix = storage_prefix(Self::module_prefix(), Self::storage_prefix());

			let key1_hashed = key1.using_encoded(OldHasher1::hash);
			let key2_hashed = key2.using_encoded(OldHasher2::hash);

			let mut final_key = Vec::with_capacity(
				storage_prefix.len() + key1_hashed.as_ref().len() + key2_hashed.as_ref().len(),
			);

			final_key.extend_from_slice(&storage_prefix);
			final_key.extend_from_slice(key1_hashed.as_ref());
			final_key.extend_from_slice(key2_hashed.as_ref());

			final_key
		};
		#[cfg(feature = "std")]
		{
			Self::take_cache(&old_key, |_| { Option::<V>::None }).map(|value| {
				Self::put_cache(Self::storage_double_map_final_key(key1, key2).as_ref(), value.clone());
				value
			})
		}
		#[cfg(not(feature = "std"))]
		unhashed::take(old_key.as_ref()).map(|value| {
			unhashed::put(Self::storage_double_map_final_key(key1, key2).as_ref(), &value);
			value
		})
	}
}

impl<K1: FullCodec, K2: FullCodec, V: FullCodec + TStorage, G: StorageDoubleMap<K1, K2, V>>
	storage::IterableStorageDoubleMap<K1, K2, V> for G
where
	G::Hasher1: ReversibleStorageHasher,
	G::Hasher2: ReversibleStorageHasher,
{
	type PartialKeyIterator = KeyPrefixIterator<K2>;
	type PrefixIterator = PrefixIterator<(K2, V)>;
	type FullKeyIterator = KeyPrefixIterator<(K1, K2)>;
	type Iterator = PrefixIterator<(K1, K2, V)>;

	fn iter_prefix(k1: impl EncodeLike<K1>) -> Self::PrefixIterator {
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::iter_prefix` not supported") };
		let prefix = G::storage_double_map_final_key1(k1);
		Self::PrefixIterator {
			prefix: prefix.clone(),
			previous_key: prefix,
			drain: false,
			closure: |raw_key_without_prefix, mut raw_value| {
				let mut key_material = G::Hasher2::reverse(raw_key_without_prefix);
				Ok((K2::decode(&mut key_material)?, V::decode(&mut raw_value)?))
			},
			phantom: Default::default(),
		}
	}

	fn iter_prefix_from(
		k1: impl EncodeLike<K1>,
		starting_raw_key: Vec<u8>,
	) -> Self::PrefixIterator {
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::iter_prefix_from` not supported") };
		let mut iter = Self::iter_prefix(k1);
		iter.set_last_raw_key(starting_raw_key);
		iter
	}

	fn iter_key_prefix(k1: impl EncodeLike<K1>) -> Self::PartialKeyIterator {
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::iter_key_prefix` not supported") };
		let prefix = G::storage_double_map_final_key1(k1);
		Self::PartialKeyIterator {
			prefix: prefix.clone(),
			previous_key: prefix,
			drain: false,
			closure: |raw_key_without_prefix| {
				let mut key_material = G::Hasher2::reverse(raw_key_without_prefix);
				K2::decode(&mut key_material)
			},
		}
	}

	fn iter_key_prefix_from(
		k1: impl EncodeLike<K1>,
		starting_raw_key: Vec<u8>,
	) -> Self::PartialKeyIterator {
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::iter_key_prefix_from` not supported") };
		let mut iter = Self::iter_key_prefix(k1);
		iter.set_last_raw_key(starting_raw_key);
		iter
	}

	fn drain_prefix(k1: impl EncodeLike<K1>) -> Self::PrefixIterator {
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::drain_prefix` not supported") };
		let mut iterator = Self::iter_prefix(k1);
		iterator.drain = true;
		iterator
	}

	fn iter() -> Self::Iterator {
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::iter` not supported") };
		let prefix = G::prefix_hash();
		Self::Iterator {
			prefix: prefix.clone(),
			previous_key: prefix,
			drain: false,
			closure: |raw_key_without_prefix, mut raw_value| {
				let mut k1_k2_material = G::Hasher1::reverse(raw_key_without_prefix);
				let k1 = K1::decode(&mut k1_k2_material)?;
				let mut k2_material = G::Hasher2::reverse(k1_k2_material);
				let k2 = K2::decode(&mut k2_material)?;
				Ok((k1, k2, V::decode(&mut raw_value)?))
			},
			phantom: Default::default(),
		}
	}

	fn iter_from(starting_raw_key: Vec<u8>) -> Self::Iterator {
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::iter_from` not supported") };
		let mut iter = Self::iter();
		iter.set_last_raw_key(starting_raw_key);
		iter
	}

	fn iter_keys() -> Self::FullKeyIterator {
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::iter_keys` not supported") };
		let prefix = G::prefix_hash();
		Self::FullKeyIterator {
			prefix: prefix.clone(),
			previous_key: prefix,
			drain: false,
			closure: |raw_key_without_prefix| {
				let mut k1_k2_material = G::Hasher1::reverse(raw_key_without_prefix);
				let k1 = K1::decode(&mut k1_k2_material)?;
				let mut k2_material = G::Hasher2::reverse(k1_k2_material);
				let k2 = K2::decode(&mut k2_material)?;
				Ok((k1, k2))
			},
		}
	}

	fn iter_keys_from(starting_raw_key: Vec<u8>) -> Self::FullKeyIterator {
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::iter_keys_from` not supported") };
		let mut iter = Self::iter_keys();
		iter.set_last_raw_key(starting_raw_key);
		iter
	}

	fn drain() -> Self::Iterator {
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::drain` not supported") };
		let mut iterator = Self::iter();
		iterator.drain = true;
		iterator
	}

	fn translate<O: Decode, F: FnMut(K1, K2, O) -> Option<V>>(mut f: F) {
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_some() { panic!("`StorageDoubleMap::translate` not supported") };
		let prefix = G::prefix_hash();
		let mut previous_key = prefix.clone();
		while let Some(next) =
			sp_io::storage::next_key(&previous_key).filter(|n| n.starts_with(&prefix))
		{
			previous_key = next;
			let value = match unhashed::get::<O>(&previous_key) {
				Some(value) => value,
				None => {
					log::error!("Invalid translate: fail to decode old value");
					continue
				},
			};
			let mut key_material = G::Hasher1::reverse(&previous_key[prefix.len()..]);
			let key1 = match K1::decode(&mut key_material) {
				Ok(key1) => key1,
				Err(_) => {
					log::error!("Invalid translate: fail to decode key1");
					continue
				},
			};

			let mut key2_material = G::Hasher2::reverse(key_material);
			let key2 = match K2::decode(&mut key2_material) {
				Ok(key2) => key2,
				Err(_) => {
					log::error!("Invalid translate: fail to decode key2");
					continue
				},
			};

			match f(key1, key2, value) {
				Some(new) => unhashed::put::<V>(&previous_key, &new),
				None => unhashed::kill(&previous_key),
			}
		}
	}
}

/// Test iterators for StorageDoubleMap
#[cfg(test)]
mod test_iterators {
	use crate::{
		hash::StorageHasher,
		storage::{
			generator::{tests::*, StorageDoubleMap},
			unhashed,
		},
	};
	use codec::Encode;

	#[test]
	fn double_map_iter_from() {
		sp_io::TestExternalities::default().execute_with(|| {
			use crate::hash::Identity;
			#[crate::storage_alias]
			type MyDoubleMap = StorageDoubleMap<MyModule, Identity, u64, Identity, u64, u64>;

			MyDoubleMap::insert(1, 10, 100);
			MyDoubleMap::insert(1, 21, 201);
			MyDoubleMap::insert(1, 31, 301);
			MyDoubleMap::insert(1, 41, 401);
			MyDoubleMap::insert(2, 20, 200);
			MyDoubleMap::insert(3, 30, 300);
			MyDoubleMap::insert(4, 40, 400);
			MyDoubleMap::insert(5, 50, 500);

			let starting_raw_key = MyDoubleMap::storage_double_map_final_key(1, 21);
			let iter = MyDoubleMap::iter_key_prefix_from(1, starting_raw_key);
			assert_eq!(iter.collect::<Vec<_>>(), vec![31, 41]);

			let starting_raw_key = MyDoubleMap::storage_double_map_final_key(1, 31);
			let iter = MyDoubleMap::iter_prefix_from(1, starting_raw_key);
			assert_eq!(iter.collect::<Vec<_>>(), vec![(41, 401)]);

			let starting_raw_key = MyDoubleMap::storage_double_map_final_key(2, 20);
			let iter = MyDoubleMap::iter_keys_from(starting_raw_key);
			assert_eq!(iter.collect::<Vec<_>>(), vec![(3, 30), (4, 40), (5, 50)]);

			let starting_raw_key = MyDoubleMap::storage_double_map_final_key(3, 30);
			let iter = MyDoubleMap::iter_from(starting_raw_key);
			assert_eq!(iter.collect::<Vec<_>>(), vec![(4, 40, 400), (5, 50, 500)]);
		});
	}

	#[test]
	fn double_map_reversible_reversible_iteration() {
		sp_io::TestExternalities::default().execute_with(|| {
			type DoubleMap = self::frame_system::DoubleMap<Runtime>;

			// All map iterator
			let prefix = DoubleMap::prefix_hash();

			unhashed::put(&key_before_prefix(prefix.clone()), &1u64);
			unhashed::put(&key_after_prefix(prefix.clone()), &1u64);

			for i in 0..4 {
				DoubleMap::insert(i as u16, i as u32, i as u64);
			}

			assert_eq!(
				DoubleMap::iter().collect::<Vec<_>>(),
				vec![(3, 3, 3), (0, 0, 0), (2, 2, 2), (1, 1, 1)],
			);

			assert_eq!(
				DoubleMap::iter_keys().collect::<Vec<_>>(),
				vec![(3, 3), (0, 0), (2, 2), (1, 1)],
			);

			assert_eq!(DoubleMap::iter_values().collect::<Vec<_>>(), vec![3, 0, 2, 1]);

			assert_eq!(
				DoubleMap::drain().collect::<Vec<_>>(),
				vec![(3, 3, 3), (0, 0, 0), (2, 2, 2), (1, 1, 1)],
			);

			assert_eq!(DoubleMap::iter().collect::<Vec<_>>(), vec![]);
			assert_eq!(unhashed::get(&key_before_prefix(prefix.clone())), Some(1u64));
			assert_eq!(unhashed::get(&key_after_prefix(prefix.clone())), Some(1u64));

			// Prefix iterator
			let k1 = 3 << 8;
			let prefix = DoubleMap::storage_double_map_final_key1(k1);

			unhashed::put(&key_before_prefix(prefix.clone()), &1u64);
			unhashed::put(&key_after_prefix(prefix.clone()), &1u64);

			for i in 0..4 {
				DoubleMap::insert(k1, i as u32, i as u64);
			}

			assert_eq!(
				DoubleMap::iter_prefix(k1).collect::<Vec<_>>(),
				vec![(1, 1), (2, 2), (0, 0), (3, 3)],
			);

			assert_eq!(DoubleMap::iter_key_prefix(k1).collect::<Vec<_>>(), vec![1, 2, 0, 3]);

			assert_eq!(DoubleMap::iter_prefix_values(k1).collect::<Vec<_>>(), vec![1, 2, 0, 3]);

			assert_eq!(
				DoubleMap::drain_prefix(k1).collect::<Vec<_>>(),
				vec![(1, 1), (2, 2), (0, 0), (3, 3)],
			);

			assert_eq!(DoubleMap::iter_prefix(k1).collect::<Vec<_>>(), vec![]);
			assert_eq!(unhashed::get(&key_before_prefix(prefix.clone())), Some(1u64));
			assert_eq!(unhashed::get(&key_after_prefix(prefix.clone())), Some(1u64));

			// Translate
			let prefix = DoubleMap::prefix_hash();

			unhashed::put(&key_before_prefix(prefix.clone()), &1u64);
			unhashed::put(&key_after_prefix(prefix.clone()), &1u64);
			for i in 0..4 {
				DoubleMap::insert(i as u16, i as u32, i as u64);
			}

			// Wrong key1
			unhashed::put(&[prefix.clone(), vec![1, 2, 3]].concat(), &3u64.encode());

			// Wrong key2
			unhashed::put(
				&[prefix.clone(), crate::Blake2_128Concat::hash(&1u16.encode())].concat(),
				&3u64.encode(),
			);

			// Wrong value
			unhashed::put(
				&[
					prefix.clone(),
					crate::Blake2_128Concat::hash(&1u16.encode()),
					crate::Twox64Concat::hash(&2u32.encode()),
				]
				.concat(),
				&vec![1],
			);

			DoubleMap::translate(|_k1, _k2, v: u64| Some(v * 2));
			assert_eq!(
				DoubleMap::iter().collect::<Vec<_>>(),
				vec![(3, 3, 6), (0, 0, 0), (2, 2, 4), (1, 1, 2)],
			);
		})
	}
}
