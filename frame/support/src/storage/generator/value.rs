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
	storage::{self, unhashed, StorageAppend},
	Never,
};
use codec::{Decode, Encode, EncodeLike, FullCodec};
use scale_info::prelude::string::String;

/// Generator for `StorageValue` used by `decl_storage`.
///
/// By default value is stored at:
/// ```nocompile
/// Twox128(module_prefix) ++ Twox128(storage_prefix)
/// ```
pub trait StorageValue<T: FullCodec> {
	/// The type that get/take returns.
	type Query;

	/// Module prefix. Used for generating final key.
	fn module_prefix() -> &'static [u8];

	/// Storage prefix. Used for generating final key.
	fn storage_prefix() -> &'static [u8];

	/// Convert an optional value retrieved from storage to the type queried.
	fn from_optional_value_to_query(v: Option<T>) -> Self::Query;

	/// Convert a query to an optional value into storage.
	fn from_query_to_optional_value(v: Self::Query) -> Option<T>;

	/// Generate the full key used in top storage.
	fn storage_value_final_key() -> [u8; 32] {
		crate::storage::storage_prefix(Self::module_prefix(), Self::storage_prefix())
	}
}

impl<T: FullCodec, G: StorageValue<T>> storage::StorageValue<T> for G {
	type Query = G::Query;

	fn hashed_key() -> [u8; 32] {
		Self::storage_value_final_key()
	}

	fn exists() -> bool {
		unhashed::exists(&Self::storage_value_final_key())
	}

	fn get() -> Self::Query {
		let value = unhashed::get(&Self::storage_value_final_key());
		G::from_optional_value_to_query(value)
	}

	fn try_get() -> Result<T, ()> {
		unhashed::get(&Self::storage_value_final_key()).ok_or(())
	}

	fn translate<O: Decode, F: FnOnce(Option<O>) -> Option<T>>(f: F) -> Result<Option<T>, ()> {
		let key = Self::storage_value_final_key();

		// attempt to get the length directly.
		let maybe_old = unhashed::get_raw(&key)
			.map(|old_data| O::decode(&mut &old_data[..]).map_err(|_| ()))
			.transpose()?;
		let maybe_new = f(maybe_old);
		if let Some(new) = maybe_new.as_ref() {
			new.using_encoded(|d| unhashed::put_raw(&key, d));
		} else {
			unhashed::kill(&key);
		}
		Ok(maybe_new)
	}

	fn put<Arg: EncodeLike<T>>(val: Arg) {
		log::trace!(target: "storage_dev", "value put {} {}",
			String::from_utf8(Self::module_prefix().to_vec()).unwrap(), 
			String::from_utf8(Self::storage_prefix().to_vec()).unwrap(),
		);
		unhashed::put(&Self::storage_value_final_key(), &val)
	}

	fn set(maybe_val: Self::Query) {
		log::trace!(target: "storage_dev", "value set {} {}",
			String::from_utf8(Self::module_prefix().to_vec()).unwrap(), 
			String::from_utf8(Self::storage_prefix().to_vec()).unwrap(),
		);
		if let Some(val) = G::from_query_to_optional_value(maybe_val) {
			unhashed::put(&Self::storage_value_final_key(), &val)
		} else {
			unhashed::kill(&Self::storage_value_final_key())
		}
	}

	fn kill() {
		unhashed::kill(&Self::storage_value_final_key())
	}

	fn mutate<R, F: FnOnce(&mut G::Query) -> R>(f: F) -> R {
		Self::try_mutate(|v| Ok::<R, Never>(f(v))).expect("`Never` can not be constructed; qed")
	}

	fn try_mutate<R, E, F: FnOnce(&mut G::Query) -> Result<R, E>>(f: F) -> Result<R, E> {
		let mut val = G::get();

		let ret = f(&mut val);
		if ret.is_ok() {
			log::trace!(target: "storage_dev", "value mutate {} {}",
				String::from_utf8(Self::module_prefix().to_vec()).unwrap(), 
				String::from_utf8(Self::storage_prefix().to_vec()).unwrap(),
			);
			match G::from_query_to_optional_value(val) {
				Some(ref val) => G::put(val),
				None => G::kill(),
			}
		}
		ret
	}

	fn mutate_exists<R, F>(f: F) -> R
	where
		F: FnOnce(&mut Option<T>) -> R,
	{
		Self::try_mutate_exists(|v| Ok::<R, Never>(f(v)))
			.expect("`Never` can not be constructed; qed")
	}

	fn try_mutate_exists<R, E, F>(f: F) -> Result<R, E>
	where
		F: FnOnce(&mut Option<T>) -> Result<R, E>,
	{
		let mut val = G::from_query_to_optional_value(Self::get());

		let ret = f(&mut val);
		if ret.is_ok() {
			log::trace!(target: "storage_dev", "value mutate_exists {} {}",
				String::from_utf8(Self::module_prefix().to_vec()).unwrap(), 
				String::from_utf8(Self::storage_prefix().to_vec()).unwrap(),
			);
			match val {
				Some(ref val) => Self::put(val),
				None => Self::kill(),
			}
		}
		ret
	}

	fn take() -> G::Query {
		let key = Self::storage_value_final_key();
		let value = unhashed::get(&key);
		if value.is_some() {
			unhashed::kill(&key)
		}
		G::from_optional_value_to_query(value)
	}

	fn append<Item, EncodeLikeItem>(item: EncodeLikeItem)
	where
		Item: Encode,
		EncodeLikeItem: EncodeLike<Item>,
		T: StorageAppend<Item>,
	{
		log::trace!(target: "storage_dev", "value append {} {}",
			String::from_utf8(Self::module_prefix().to_vec()).unwrap(), 
			String::from_utf8(Self::storage_prefix().to_vec()).unwrap(),
		);
		let key = Self::storage_value_final_key();
		#[cfg(feature = "std")]
		let start = std::time::Instant::now();
		let encoded = item.encode();
		#[cfg(feature = "std")]
		{
			let time = start.elapsed();
			let mut lock = crate::storage::unhashed::GLOBAL_ENCODE.lock().unwrap();
			if let Some(v) = lock.get_mut(key.as_ref()) {
				v.push((time, encoded.len()));
			} else {
				lock.insert(key.to_vec(), vec![(time, encoded.len())]);
			}
		}
		sp_io::storage::append(&key, encoded);
	}
}
