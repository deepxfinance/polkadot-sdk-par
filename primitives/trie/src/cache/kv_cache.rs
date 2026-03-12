pub use overlay_cache::*;

pub mod overlay_cache {
	use std::marker::PhantomData;
	use hash_db::Hasher;
	use parking_lot::MutexGuard;
	use kv_db::{DBValue, KVCache, OverlayedChangeSet};

	#[cfg(feature = "std")]
	pub type KValueCacheMap = OverlayedChangeSet;
	
	pub struct LocalKVTrieCache<'a, H> {
		cache: MutexGuard<'a, KValueCacheMap>,
		phantom: PhantomData<H>,
	}

	#[cfg(feature = "std")]
	impl<'a, H: Hasher> KVCache<H> for LocalKVTrieCache<'a, H> {
		fn lookup_value_for_key(&mut self, key: &[u8]) -> Option<DBValue> {
			self.cache.get(key)?.value_ref().clone().map(|mut v| {
				v.set_muted(false);
				v
			})
		}

		fn cache_value_for_key(&mut self, key: &[u8], value: DBValue) {
			self.cache.set(key.to_vec(), Some(value), None);
		}

		fn remove_value_for_key(&mut self, key: &[u8]) {
			self.cache.set(key.to_vec(), None, None);
		}
	}

	#[cfg(not(feature = "std"))]
	impl<H: Hasher> KVCache<H> for LocalKVTrieCache {
		fn lookup_value_for_key(&mut self, _key: &[u8]) -> Option<DBValue> {
			None
		}

		fn cache_value_for_key(&mut self, _key: &[u8], _value: DBValue) {}

		fn remove_value_for_key(&mut self, _key: &[u8]) {}
	}
}
