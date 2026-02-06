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
	storage::{self, unhashed, StorageAppend, TStorage},
	Never,
};
use codec::{Decode, Encode, EncodeLike, FullCodec};
use scale_info::prelude::string::String;
use typed_cache::{OptionQT, QueryTransfer};
use crate::storage::{TypedAppend, RcT};

/// Generator for `StorageValue` used by `decl_storage`.
///
/// By default value is stored at:
/// ```nocompile
/// Twox128(module_prefix) ++ Twox128(storage_prefix)
/// ```
pub trait StorageValue<T: FullCodec + TStorage>: QueryTransfer<T> {
	/// Module prefix. Used for generating final key.
	fn module_prefix() -> &'static [u8];

	/// Storage prefix. Used for generating final key.
	fn storage_prefix() -> &'static [u8];

	/// Generate the full key used in top storage.
	fn storage_value_final_key() -> [u8; 32] {
		crate::storage::storage_prefix(Self::module_prefix(), Self::storage_prefix())
	}
}

impl<T: FullCodec + TStorage, G: StorageValue<T>> storage::StorageValue<T> for G {
	type Query = G::Query;

	fn hashed_key() -> [u8; 32] {
		Self::storage_value_final_key()
	}

	fn exists() -> bool {
		#[cfg(feature = "std")]
		if sp_io::mut_typed_cache(|_| ()).is_none() {
			unhashed::exists(&Self::storage_value_final_key())
		} else {
			unhashed::get_cache(&Self::storage_value_final_key(), |_| { Option::<T>::None }).is_some()
		}
		#[cfg(not(feature = "std"))]
		unhashed::exists(&Self::storage_value_final_key())
	}

	fn get() -> Self::Query {
		#[cfg(feature = "std")]
		let value = unhashed::get_cache(&Self::storage_value_final_key(), |_| { Option::<T>::None });
		#[cfg(not(feature = "std"))]
		let value = unhashed::get(&Self::storage_value_final_key());
		G::from_optional_value_to_query(value)
	}

	fn get_ref() -> RcT<T> {
		unhashed::get_cache_ref(&Self::storage_value_final_key(), #[cfg(feature = "std")] |_| { Option::<T>::None })
	}

	fn try_get() -> Result<T, ()> {
		#[cfg(feature = "std")]
		{ unhashed::get_cache(&Self::storage_value_final_key(), |_| { Option::<T>::None }).ok_or(()) }
		#[cfg(not(feature = "std"))]
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
			#[cfg(feature = "std")]
			Self::put(new.clone());
			#[cfg(not(feature = "std"))]
			new.using_encoded(|d| unhashed::put_raw(&key, d));
		} else {
			#[cfg(feature = "std")]
			Self::kill();
			#[cfg(not(feature = "std"))]
			unhashed::kill(&key);
		}
		Ok(maybe_new)
	}

	#[cfg(feature = "std")]
	fn put(val: T) {
		// detect if typed_cache exists.
		let key = Self::storage_value_final_key();
		unhashed::put_cache(&key, val);
	}

	#[cfg(not(feature = "std"))]
	fn put<Arg: EncodeLike<T>>(val: Arg) {
		unhashed::put(&Self::storage_value_final_key(), &val)
	}

	fn set(maybe_val: Self::Query) {
		let key = Self::storage_value_final_key();
		if let Some(val) = G::from_query_to_optional_value(maybe_val) {
			#[cfg(feature = "std")]
			unhashed::put_cache(&key, val);
			#[cfg(not(feature = "std"))]
			unhashed::put(&key, &val)
		} else {
			#[cfg(feature = "std")]
			unhashed::kill_cache::<T>(&key);
			#[cfg(not(feature = "std"))]
			unhashed::kill(&key)
		}
	}

	fn kill() {
		let key = Self::storage_value_final_key();
		#[cfg(feature = "std")]
		unhashed::kill_cache::<T>(&key);
		#[cfg(not(feature = "std"))]
		unhashed::kill(&key)
	}

	fn mutate<R, F: FnOnce(&mut G::Query) -> R>(f: F) -> R {
		Self::try_mutate(|v| Ok::<R, Never>(f(v))).expect("`Never` can not be constructed; qed")
	}

	fn mutate_ref<R, F: FnOnce(&mut Self::Query) -> R>(f: F) -> R {
		Self::try_mutate_ref(|v| Ok::<R, Never>(f(v))).expect("`Never` can not be constructed; qed")
	}

	fn try_mutate<R, E, F: FnOnce(&mut G::Query) -> Result<R, E>>(f: F) -> Result<R, E> {
		let mut val = G::get();

		let ret = f(&mut val);
		if ret.is_ok() {
			match G::from_query_to_optional_value(val) {
				Some(ref val) => {
					#[cfg(feature = "std")]
					G::put(val.clone());
					#[cfg(not(feature = "std"))]
					G::put(val)
				},
				None => G::kill(),
			}
		}
		ret
	}

	fn try_mutate_ref<R, E, F: FnOnce(&mut G::Query) -> Result<R, E>>(f: F) -> Result<R, E> {
		#[cfg(feature = "std")]
		{
			unhashed::mutate_cache::<G, T, _, _, _, _>(
				&Self::storage_value_final_key(),
				|| { None::<T> },
				|v| f(v),
			)
		}
		#[cfg(not(feature = "std"))]
		Self::try_mutate(|v| f(v))
	}

	fn mutate_exists<R, F>(f: F) -> R
	where
		F: FnOnce(&mut Option<T>) -> R,
	{
		Self::try_mutate_exists(|v| Ok::<R, Never>(f(v)))
			.expect("`Never` can not be constructed; qed")
	}

	fn mutate_exists_ref<R, F: FnOnce(&mut Option<T>) -> R>(f: F) -> R {
		#[cfg(feature = "std")]
		{
			unhashed::mutate_cache::<OptionQT, T, _, _, _, _>(
				&Self::storage_value_final_key(),
				|| { None::<T> },
				|v| Ok::<R, Never>(f(v)),
			)
			.expect("`Never` can not be constructed; qed")
		}
		#[cfg(not(feature = "std"))]
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
			match val {
				Some(ref val) => Self::put(val.clone()),
				None => Self::kill(),
			}
		}
		ret
	}

	fn take() -> G::Query {
		#[cfg(feature = "std")]
		let value = unhashed::get_cache(&Self::storage_value_final_key(), |_| { Option::<T>::None });

		#[cfg(not(feature = "std"))]
		let value = unhashed::get(&Self::storage_value_final_key());
		if value.is_some() {
			Self::kill()
		}
		G::from_optional_value_to_query(value)
	}

	#[cfg(feature = "std")]
	fn append<Item: Encode + Clone>(item: Item)
	where
		T: TypedAppend<Item> + TStorage
	{
		let key = Self::storage_value_final_key();
		unhashed::append_cache::<T, Item>(&key, item);
	}

	#[cfg(not(feature = "std"))]
	fn append<Item, EncodeLikeItem>(item: EncodeLikeItem)
	where
		Item: Encode,
		EncodeLikeItem: EncodeLike<Item>,
		T: StorageAppend<Item>,
	{
		sp_io::storage::append(&Self::storage_value_final_key(), item.encode());
	}
}
