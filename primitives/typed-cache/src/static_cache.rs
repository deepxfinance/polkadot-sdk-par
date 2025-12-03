#![allow(unused)]
use codec::Encode;
use sp_std::boxed::Box;
use sp_std::collections::btree_map::{BTreeMap, Entry::{Vacant, Occupied}};
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
pub type AnyStorage = Box<dyn StorageApi>;
#[cfg(feature = "std")]
pub type Overlay = BTreeMap<Vec<u8>, AnyStorage>;

#[cfg(not(feature = "std"))]
#[derive(Clone, Default)]
pub struct OverlayCache;

#[cfg(feature = "std")]
#[derive(Default)]
pub struct OverlayCache {
    // pub cache: SharedCache,
    pub inner: Overlay,
}

#[cfg(feature = "std")]
impl OverlayCache {
    pub fn handle_mut<V: Clone + Encode + 'static, F, O>(&mut self, space: &[u8], f: F) -> O
    where
        F: FnOnce(&mut AnyStorage) -> O
    {
        match self.inner.entry(space.to_vec()) {
            Occupied(entry) => { return f(&mut entry.into_mut()) },
            Vacant(e) => {
                e.insert(Box::new(StorageOverlay::<Vec<u8>, V>::new(space)));
            }
        }
        f(&mut self.inner.get_mut(space).unwrap())
    }

    pub fn handle_ref<V: Clone + Encode, F, O>(&self, space: &[u8], f: F) -> Option<O>
    where
        F: FnOnce(&AnyStorage) -> O
    {
        self.inner.get(space).map(|v| f(v))
    }
}

#[cfg(feature = "std")]
impl OverlayCache {
    pub fn put<V: Clone + Encode + 'static>(&mut self, space: &[u8], key: &[u8], value: V) {
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.put(space, key, value));
        };
        self.handle_mut::<V, _, _>(space, f);
    }

    pub fn get<V: Clone + Encode + 'static, F>(&mut self, space: &[u8], key: &[u8], init: Option<F>) -> Option<Option<V>>
    where
        F: Fn(&[u8]) -> Option<V>
    {
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.get(space, key, init))
        };
        self.handle_mut::<V, _, _>(space, f).unwrap_or(None)
    }

    pub fn get_change<V: Clone + Encode + 'static>(&self, space: &[u8], key: &[u8]) -> Option<Option<V>> {
        let f = |storage: &AnyStorage| {
            storage.downcast_ref::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.get_change(space, key))
        };
        self.handle_ref::<V, _, _>(space, f)?.unwrap_or(None)
    }

    pub fn take<V: Clone + Encode + 'static, F>(&mut self, space: &[u8], key: &[u8], init: Option<F>) -> Option<Option<V>>
    where
        F: Fn(&[u8]) -> Option<V>
    {
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.take(space, key, init))
        };
        self.handle_mut::<V, _, _>(space, f).unwrap_or(None)
    }

    pub fn kill<V: Clone + Encode + 'static>(&mut self, space: &[u8], key: &[u8]) {
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.kill(space, key));
        };
        self.handle_mut::<V, _, _>(space, f);
    }

    pub fn mutate<V: Clone + Encode + 'static, F, M>(&mut self, space: &[u8], key: &[u8], init: Option<F>, mutate: M) -> bool
    where
        F: Fn(&[u8]) -> Option<V>,
        M: FnOnce(Option<&mut V>)
    {
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.mutate(space, key, init, mutate))
        };
        self.handle_mut::<V, _, _>(space, f).unwrap_or(false)
    }

    pub fn cache<V: Clone + Encode + 'static>(&mut self, space: &[u8], key: &[u8], value: Option<V>) {
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.cache(space, key, value));
        };
        self.handle_mut::<V, _, _>(space, f);
    }
}

#[cfg(feature = "std")]
impl OverlayCache {
    pub fn copy_data(&self) -> Self {
        Self {
            // cache: self.cache.clone(),
            inner: self.inner.iter()
                .map(|(space, overlay)| (space.clone(), overlay.copy_data()))
                .collect()
        }
    }

    pub fn enter_runtime(&mut self) {
        self.inner.iter_mut().for_each(|(_, overlay)| overlay.enter_runtime());
    }

    pub fn exit_runtime(&mut self) {
        self.inner.iter_mut().for_each(|(_, overlay)| overlay.exit_runtime());
    }

    pub fn start_transaction(&mut self) {
        self.inner.iter_mut().for_each(|(_, overlay)| overlay.start_transaction());
    }

    pub fn commit_transaction(&mut self) {
        self.inner.iter_mut().for_each(|(_, overlay)| overlay.commit_transaction());
    }

    pub fn rollback_transaction(&mut self) {
        self.inner.iter_mut().for_each(|(_, overlay)| overlay.rollback_transaction());
    }
    
    pub fn get_commited(&self) -> BTreeMap<StorageKey, Option<Vec<u8>>> {
        self.inner.iter().map(|(_, overlay)| overlay.get_commited()).flatten().collect()
    }

    pub fn drain_commited(&mut self) -> BTreeMap<StorageKey, Option<Vec<u8>>> {
        sp_std::mem::take(&mut self.inner)
            .into_iter()
            .map(|(_, mut overlay)| overlay.drain_commited()).flatten().collect()
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

        pub fn write_values(cache: &mut OverlayCache) {
            cache.put(b"Account", b"alice", Account::new(b"alice", 0, 1000));
            cache.put(b"Account", b"bob", Account::new(b"bob", 0, 500));
        }
    }

    pub mod b {
        use crate::{OverlayCache, StorageIO};

        pub fn write_values(cache: &mut OverlayCache) {
            cache.put(b"u32", b"alice_extend", 100u32);
            cache.put(b"u32", b"bob_extend", 50u32);
        }
    }

    #[test]
    fn test_mods_write() {
        let mut cache = OverlayCache::default();
        a::write_values(&mut cache);
        b::write_values(&mut cache);
        assert_eq!(cache.get(b"Account", b"alice", None), Some(Some(a::Account::new(b"alice", 0, 1000))));
        assert_eq!(cache.get(b"Account", b"bob", None), Some(Some(a::Account::new(b"bob", 0, 500))));
        assert_eq!(cache.get(b"u32", b"alice_extend", None), Some(Some(100u32)));
        assert_eq!(cache.get(b"u32", b"bob_extend", None), Some(Some(50u32)));
    }

    #[test]
    fn test_multi_threads_write() {
        let mut cache1 = OverlayCache::default();
        let mut cache2 = cache1.copy_data();

        let mut cache1 = std::thread::spawn(move || {
            // this extra will insert `1u32` with key `thread1` at space `u32`. This is shared between threads.
            assert_eq!(cache1.get(b"u32", b"thread1", Some(|_| { Some(1u32) })), Some(Some(1u32)));
            cache1.put(b"u32", b"thread1", 132u32);
            cache1.put(b"u64", b"thread1", 100u64);
            cache1
        }).join().unwrap();

        let mut cache2 = std::thread::spawn(move || {
            assert_eq!(cache2.get(b"u32", b"thread1", None), Option::<Option<u32>>::None);
            cache2.put(b"u64", b"thread2", 200u64);
            cache2
        }).join().unwrap();

        // get values
        assert_eq!(cache1.get(b"u32", b"thread1", Some(|_| { Some(1u32) })), Some(Some(132u32)));
        assert_eq!(cache1.get(b"u64", b"thread1", None), Some(Some(100u64)));
        assert_eq!(cache2.get(b"u32", b"thread1", Some(|_| { None })), Some(Option::<u32>::None));
        assert_eq!(cache2.get(b"u64", b"thread2", None), Some(Some(200u64)));
        let _ = cache1.drain_commited();
        let _ = cache2.drain_commited();
        // get values from cached values
        assert_eq!(cache1.get(b"u32", b"thread1", Some(|_| { Some(1u32) })), Some(Some(1u32)));
        assert_eq!(cache1.get(b"u64", b"thread1", None), Option::<Option<u64>>::None);
        assert_eq!(cache2.get(b"u32", b"thread2", None), Option::<Option<u32>>::None);
        assert_eq!(cache2.get(b"u64", b"thread2", None), Option::<Option<u64>>::None);
    }

    #[test]
    fn test_storage_api() {
        let mut cache = OverlayCache::default();
        cache.enter_runtime();
        cache.put(b"u32", b"11", 11u32);
        cache.put(b"u32", b"12", 12u32);
        cache.put(b"u64", b"22", 22u64);

    }
}
