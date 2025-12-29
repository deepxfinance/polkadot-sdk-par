use hash_db::Hasher;
use kv_db::{DBValue, KVCache};
use parking_lot::MutexGuard;
use schnellru::LruMap;
use std::marker::PhantomData;

pub type ValueCacheKey = Vec<u8>;

#[cfg(feature = "std")]
pub type KValueCacheMap =
	LruMap<ValueCacheKey, DBValue, LocalValueCacheLimiter, schnellru::RandomState>;

#[cfg(feature = "std")]
/// A limiter for the local value cache. This makes sure the local cache doesn't grow too big.
pub struct LocalValueCacheLimiter {
	/// The maximum size (in bytes) the cache can hold inline.
	///
	/// This space is always consumed whether there are any items in the map or not.
	pub(crate) max_inline_size: usize,

	/// The maximum size (in bytes) the cache can hold on the heap.
	pub(crate) max_heap_size: usize,

	/// The current size (in bytes) of data allocated by this cache on the heap.
	///
	/// This doesn't include the size of the map itself.
	pub(crate) heap_size: usize,

	/// A counter with the number of elements that got evicted from the cache.
	///
	/// Reset to zero before every update.
	pub(crate) items_evicted: usize,

	/// The maximum number of elements that we allow to be evicted.
	///
	/// Reset on every update.
	pub(crate) max_items_evicted: usize,
}

impl Default for LocalValueCacheLimiter {
	fn default() -> Self {
		Self {
			max_inline_size: 1 * 1024 * 1024 * 1024,
			max_heap_size: 1 * 1024 * 1024 * 1024,
			heap_size: 0,
			items_evicted: 0,
			max_items_evicted: 0,
		}
	}
}

#[cfg(feature = "std")]
impl schnellru::Limiter<ValueCacheKey, DBValue> for LocalValueCacheLimiter {
	type KeyToInsert<'a> = ValueCacheKey;
	type LinkType = u32;

	#[inline]
	fn is_over_the_limit(&self, _length: usize) -> bool {
		self.items_evicted <= self.max_items_evicted && self.heap_size > self.max_heap_size
	}

	#[inline]
	fn on_insert(
		&mut self,
		_length: usize,
		key: Self::KeyToInsert<'_>,
		value: kv_db::DBValue,
	) -> Option<(ValueCacheKey, kv_db::DBValue)> {
		let new_item_heap_size = key.len();
		if new_item_heap_size > self.max_heap_size {
			// Item's too big to add even if the cache's empty; bail.
			return None
		}

		self.heap_size += new_item_heap_size;
		Some((key, value))
	}

	#[inline]
	fn on_replace(
		&mut self,
		_length: usize,
		_old_key: &mut ValueCacheKey,
		_new_key: Self::KeyToInsert<'_>,
		_old_value: &mut kv_db::DBValue,
		_new_value: &mut kv_db::DBValue,
	) -> bool {
		debug_assert_eq!(_old_key, &_new_key);
		true
	}

	#[inline]
	fn on_removed(&mut self, key: &mut ValueCacheKey, _: &mut kv_db::DBValue) {
		self.heap_size -= key.len();
		self.items_evicted += 1;
	}

	#[inline]
	fn on_cleared(&mut self) {
		self.heap_size = 0;
	}

	#[inline]
	fn on_grow(&mut self, new_memory_usage: usize) -> bool {
		new_memory_usage <= self.max_inline_size
	}
}

pub struct LocalKVCache<'a, H> {
	#[cfg(feature = "std")]
	cache: MutexGuard<'a, KValueCacheMap>,
	#[cfg(not(feature = "std"))]
	cache: MutexGuard<'a, Vec<u8>>,
	phantom: PhantomData<H>,
}

#[cfg(feature = "std")]
impl<'a, H: Hasher> KVCache<H> for LocalKVCache<'a, H> {
	fn lookup_value_for_key(&mut self, hash: H::Out, key: &[u8], pad: Option<u8>) -> Option<DBValue> {
		let key = [hash.as_ref(), key, [pad.unwrap_or_default()].as_slice()].concat();
		match self.cache.peek(&key).cloned() {
			Some(val) => if val.is_empty() {
				None
			} else {
				Some(val)
			},
			None => None,
		}
	}

	fn cache_value_for_key(&mut self, hash: H::Out, key: &[u8], pad: Option<u8>, value: DBValue) {
		let full_key = [hash.as_ref(), key, [pad.unwrap_or_default()].as_slice()].concat();
		self.cache.insert(full_key, value);
	}

	fn remove_value_for_key(&mut self, hash: H::Out, key: &[u8], pad: Option<u8>) {
		let full_key = [hash.as_ref(), key, [pad.unwrap_or_default()].as_slice()].concat();
		self.cache.insert(full_key, vec![]);
	}
}

#[cfg(not(feature = "std"))]
impl<H: Hasher> KVCache<H> for LocalKVCache {
	fn lookup_value_for_key(&mut self, _hash: H::Out, _key: &[u8], _pad: Option<u8>) -> Option<DBValue> {
		None
	}

	fn cache_value_for_key(&mut self, _hash: H::Out, _key: &[u8], _pad: Option<u8>, _value: DBValue) {}

	fn remove_value_for_key(&mut self, _hash: H::Out, _key: &[u8], _pad: Option<u8>) {}
}
