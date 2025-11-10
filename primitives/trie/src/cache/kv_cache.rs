use std::marker::PhantomData;
use hash_db::Hasher;
use parking_lot::MutexGuard;
use schnellru::LruMap;
use kv_db::{DBValue, KVCache};


const LOCAL_VALUE_CACHE_MAX_INLINE_SIZE: usize = 512 * 1024;
/// The maximum size of the memory allocated on the heap by the local cache, in bytes.
const LOCAL_VALUE_CACHE_MAX_HEAP_SIZE: usize = 2 * 1024 * 1024 * 1024;

pub type ValueCacheKey = Vec<u8>;

#[cfg(feature = "std")]
pub type KValueCacheMap = LruMap<
    ValueCacheKey,
    DBValue,
    LocalValueCacheLimiter,
    schnellru::RandomState,
>;

#[cfg(feature = "std")]
/// A limiter for the local value cache. This makes sure the local cache doesn't grow too big.
#[derive(Default)]
pub struct LocalValueCacheLimiter {
    // TODO better cache size check
    /// The current size (in bytes) of data allocated by this cache on the heap.
    pub current_heap_size: usize,
}

#[cfg(feature = "std")]
impl schnellru::Limiter<ValueCacheKey, DBValue> for LocalValueCacheLimiter {
    type KeyToInsert<'a> = ValueCacheKey;
    type LinkType = u32;

    #[inline]
    fn is_over_the_limit(&self, length: usize) -> bool {
        // Only enforce the limit if there's more than one element to make sure
        // we can always add a new element to the cache.
        if length <= 1 {
            return false
        }

        self.current_heap_size > LOCAL_VALUE_CACHE_MAX_HEAP_SIZE
    }

    #[inline]
    fn on_insert(
        &mut self,
        _length: usize,
        key: Self::KeyToInsert<'_>,
        value: DBValue,
    ) -> Option<(ValueCacheKey, DBValue)> {
        self.current_heap_size += key.len();
        Some((key.clone(), value))
    }

    #[inline]
    fn on_replace(
        &mut self,
        _length: usize,
        _old_key: &mut ValueCacheKey,
        _new_key: Self::KeyToInsert<'_>,
        _old_value: &mut DBValue,
        _new_value: &mut DBValue,
    ) -> bool {
        debug_assert_eq!(_old_key.len(), _new_key.len());
        true
    }

    #[inline]
    fn on_removed(&mut self, key: &mut ValueCacheKey, _: &mut DBValue) {
        self.current_heap_size -= key.len();
    }

    #[inline]
    fn on_cleared(&mut self) {
        self.current_heap_size = 0;
    }

    #[inline]
    fn on_grow(&mut self, new_memory_usage: usize) -> bool {
        new_memory_usage <= LOCAL_VALUE_CACHE_MAX_INLINE_SIZE
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
    fn lookup_value_for_key(&mut self, hash: H::Out, key: &[u8]) -> Option<DBValue> {
        let key = [hash.as_ref(), key].concat();
        self.cache.peek(&key).cloned()
    }

    fn cache_value_for_key(&mut self, hash: H::Out, key: &[u8], value: DBValue) {
        let full_key = [hash.as_ref(), key].concat();
        self.cache.insert(full_key, value);
    }

    fn remove_value_for_key(&mut self, hash: H::Out, key: &[u8]) {
        let full_key = [hash.as_ref(), key].concat();
        self.cache.insert(full_key, vec![0u8]);
    }
}

#[cfg(not(feature = "std"))]
impl<H: Hasher> KVCache<H> for LocalKVCache {
    fn lookup_value_for_key(&mut self, _hash: H::Out, _key: &[u8]) -> Option<DBValue> {
        None
    }

    fn cache_value_for_key(&mut self, _hash: H::Out, _key: &[u8], _value: DBValue) {}

    fn remove_value_for_key(&mut self, _hash: H::Out, _key: &[u8]) {}
}
