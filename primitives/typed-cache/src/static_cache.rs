use codec::Encode;
use sp_std::boxed::Box;
use sp_std::collections::btree_map::{BTreeMap, Entry::Vacant};
use sp_std::sync::Arc;
#[cfg(feature = "std")]
use sp_std::sync::RwLock;
use sp_std::any::Any;
use crate::{StorageIO, StorageKey};
#[cfg(feature = "std")]
use crate::StorageApi;
#[cfg(feature = "std")]
use crate::{Cache, StorageOverlay};

#[cfg(feature = "std")]
pub type AnyStorage = Arc<Box<dyn StorageApi>>;
#[cfg(feature = "std")]
pub type Overlay = Arc<RwLock<BTreeMap<Vec<u8>, AnyStorage>>>;
pub type AnyCache = Arc<Box<dyn Any + Send + Sync>>;
#[cfg(feature = "std")]
pub type SharedCache = Arc<RwLock<BTreeMap<Vec<u8>, AnyCache>>>;

#[cfg(not(feature = "std"))]
#[derive(Clone, Default)]
pub struct OverlayCache;

#[cfg(feature = "std")]
#[derive(Clone, Default)]
pub struct OverlayCache {
    pub cache: SharedCache,
    pub inner: Overlay,
}

#[cfg(feature = "std")]
impl OverlayCache {
    pub fn get_or_register<V: 'static + Send + Sync + Clone + Encode>(&self, space: &[u8]) -> AnyStorage {
        let try_get = self.inner.read().unwrap().get(space).cloned();
        match try_get {
            Some(overlay) => overlay,
            None => {
                let shared_cache = self.cache.read().unwrap().get(space).cloned();
                let cache = match shared_cache {
                    Some(cache) => cache,
                    None => {
                        // try insert new entry, not just insert(multi threads insert may conflict).
                        if let Vacant(entry) = self.cache.write().unwrap().entry(space.to_vec()) {
                            entry.insert(Arc::new(Box::new(Cache::<BTreeMap<Vec<u8>, V>>::default())));
                        }
                        self.cache.read().unwrap().get(space).cloned().unwrap()
                    }
                };
                self.inner.write().unwrap().insert(space.to_vec(), Arc::new(Box::new(StorageOverlay::<Vec<u8>, V>::new(
                    space,
                    Some(cache.downcast_ref::<Cache<BTreeMap<Vec<u8>, V>>>().unwrap().clone())
                ))));
                self.inner.read().unwrap().get(space).cloned().unwrap()
            }
        }
    }

    pub fn get_all(&self) -> Vec<(Vec<u8>, AnyStorage)> {
        self.inner.read().unwrap().iter().map(|(id, overlay)| (id.clone(), overlay.clone())).collect()
    }
}

#[cfg(feature = "std")]
impl<V: 'static + Send + Sync + Clone + Encode> StorageIO<V> for OverlayCache {
    fn put(&self, space: &[u8], key: &[u8], value: V) {
        self.get_or_register::<V>(space)
            .downcast_ref::<StorageOverlay<Vec<u8>, V>>()
            .map(|overlay| overlay.put(space, key, value));
    }

    fn get<F>(&self, space: &[u8], key: &[u8], extra: F) -> Option<V>
    where
        F: Fn(&[u8]) -> Option<V>
    {
        self.get_or_register::<V>(space)
            .downcast_ref::<StorageOverlay<Vec<u8>, V>>()
            .map(|overlay| overlay.get(space, key, extra))
            .unwrap_or(None)
    }

    fn take<F>(&self, space: &[u8], key: &[u8], extra: F) -> Option<V>
    where
        F: Fn(&[u8]) -> Option<V>
    {
        self.get_or_register::<V>(space)
            .downcast_ref::<StorageOverlay<Vec<u8>, V>>()
            .map(|overlay| overlay.take(space, key, extra))
            .unwrap_or(None)
    }

    fn kill(&self, space: &[u8], key: &[u8]) {
        self.get_or_register::<V>(space)
            .downcast_ref::<StorageOverlay<Vec<u8>, V>>()
            .map(|overlay| overlay.kill(space, key));
    }

    fn mutate<F, M>(&self, space: &[u8], key: &[u8], get: F, mutate: M) -> bool
    where
        F: Fn(&[u8]) -> Option<V>,
        M: FnOnce(Option<&mut V>)
    {
        self.get_or_register::<V>(space)
            .downcast_ref::<StorageOverlay<Vec<u8>, V>>()
            .map(|overlay| overlay.mutate(space, key, get, mutate))
            .unwrap_or(false)
    }
}

#[cfg(feature = "std")]
impl OverlayCache {
    pub fn copy_data(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            inner: Arc::new(RwLock::new(
                self.inner.read().unwrap().iter()
                .map(|(space, overlay)| (space.clone(), overlay.copy_data()))
                .collect()
            ))
        }
    }

    pub fn enter_runtime(&self) {
        self.get_all().iter().for_each(|(_, overlay)| overlay.enter_runtime());
    }

    pub fn exit_runtime(&self) {
        self.get_all().iter().for_each(|(_, overlay)| overlay.exit_runtime());
    }

    pub fn start_transaction(&self) {
        self.get_all().iter().for_each(|(_, overlay)| overlay.start_transaction());
    }

    pub fn commit_transaction(&self) {
        self.get_all().iter().for_each(|(_, overlay)| overlay.commit_transaction());
    }

    pub fn rollback_transaction(&self) {
        self.get_all().iter().for_each(|(_, overlay)| overlay.rollback_transaction());
    }

    pub fn drain_commited(&self) -> BTreeMap<StorageKey, Option<Vec<u8>>> {
        self.get_all().into_iter().map(|(_, overlay)| overlay.drain_commited()).flatten().collect()
    }
}

#[cfg(test)]
pub mod test {
    use crate::{OverlayCache, StorageIO};

    pub mod a {
        use codec::Encode;
        use crate::{OverlayCache, StorageIO};

        #[derive(Clone, Default, Eq, PartialEq, Encode, Debug)]
        pub struct Account {
            pub address: Vec<u8>,
            pub nonce: u32,
            pub balance: u128,
        }

        impl Account {
            pub fn new(add: &[u8], nonce: u32, balance: u128) -> Self {
                Self { address: add.to_vec(), nonce, balance }
            }
        }

        pub fn write_values(cache: &OverlayCache) {
            cache.put(b"Account", b"alice", Account::new(b"alice", 0, 1000));
            cache.put(b"Account", b"bob", Account::new(b"bob", 0, 500));
        }
    }

    pub mod b {
        use crate::{OverlayCache, StorageIO};

        pub fn write_values(cache: &OverlayCache) {
            cache.put(b"u32", b"alice_extend", 100u32);
            cache.put(b"u32", b"bob_extend", 50u32);
        }
    }

    #[test]
    fn test_mods_write() {
        let cache = OverlayCache::default();
        a::write_values(&cache);
        b::write_values(&cache);
        assert_eq!(cache.get(b"Account", b"alice", |_| { None }), Some(a::Account::new(b"alice", 0, 1000)));
        assert_eq!(cache.get(b"Account", b"bob", |_| { None }), Some(a::Account::new(b"bob", 0, 500)));
        assert_eq!(cache.get(b"u32", b"alice_extend", |_| { None }), Some(100u32));
        assert_eq!(cache.get(b"u32", b"bob_extend", |_| { None }), Some(50u32));
    }

    #[test]
    fn test_multi_threads_write() {
        let cache1 = OverlayCache::default();
        let cache2 = cache1.copy_data();

        let cache1 = std::thread::spawn(move || {
            // this extra will insert `1u32` with key `thread1` at space `u32`. This is shared between threads.
            assert_eq!(cache1.get(b"u32", b"thread1", |_| { Some(1u32) }), Some(1u32));
            cache1.put(b"u32", b"thread1", 132u32);
            cache1.put(b"u64", b"thread1", 100u64);
            cache1
        }).join().unwrap();

        let cache2 = std::thread::spawn(move || {
            assert_eq!(cache2.get(b"u32", b"thread1", |_| { None }), Some(1u32));
            cache2.put(b"u64", b"thread2", 200u64);
            cache2
        }).join().unwrap();

        // get values
        assert_eq!(cache1.get(b"u32", b"thread1", |_| { Some(1u32) }), Some(132u32));
        assert_eq!(cache1.get(b"u64", b"thread1", |_| { None }), Some(100u64));
        assert_eq!(cache2.get(b"u32", b"thread1", |_| { None }), Some(1u32));
        assert_eq!(cache2.get(b"u64", b"thread2", |_| { None }), Some(200u64));
        let _ = cache1.drain_commited();
        let _ = cache2.drain_commited();
        // get values from cached values
        assert_eq!(cache1.get(b"u32", b"thread1", |_| { Some(1u32) }), Some(1u32));
        assert_eq!(cache1.get(b"u64", b"thread1", |_| { None }), Option::<u64>::None);
        assert_eq!(cache2.get(b"u32", b"thread2", |_| { None }), Option::<u32>::None);
        assert_eq!(cache2.get(b"u64", b"thread2", |_| { None }), Option::<u64>::None);
    }
}
