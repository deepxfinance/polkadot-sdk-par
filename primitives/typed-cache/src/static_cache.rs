#![allow(unused)]
use codec::{FullCodec, Output};
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

#[cfg(all(feature = "std", feature = "dev-time"))]
lazy_static::lazy_static!{
    pub static ref GET: std::sync::Mutex<std::collections::HashMap<Vec<u8>, Vec<(std::time::Duration, std::time::Duration)>>> = std::sync::Mutex::new(std::collections::HashMap::new());
    pub static ref PUT: std::sync::Mutex<std::collections::HashMap<Vec<u8>, Vec<(std::time::Duration, std::time::Duration)>>> = std::sync::Mutex::new(std::collections::HashMap::new());
    pub static ref ENCODE: std::sync::Mutex<std::collections::HashMap<Vec<u8>, Vec<std::time::Duration>>> = std::sync::Mutex::new(std::collections::HashMap::new());
}

#[cfg(feature = "std")]
pub type AnyStorage = Box<dyn StorageApi>;
#[cfg(feature = "std")]
pub type Overlay = std::collections::HashMap<Vec<u8>, AnyStorage>;

#[cfg(not(feature = "std"))]
#[derive(Clone, Default)]
pub struct OverlayCache;

#[cfg(feature = "std")]
#[derive(Default)]
pub struct OverlayCache {
    // pub cache: SharedCache,
    pub inner: Overlay,
    pub closed: bool,
}

#[cfg(feature = "std")]
impl OverlayCache {
    /// Handle with `f` for mutable reference with create default space if not exist.
    pub fn handle_mut_or_default<V: Clone + FullCodec + 'static, F, O>(&mut self, space: &[u8], f: F) -> O
    where
        F: FnOnce(&mut AnyStorage) -> O
    {
        match self.inner.entry(space.to_vec()) {
            std::collections::hash_map::Entry::Occupied(entry) => { return f(&mut entry.into_mut()) },
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(Box::new(StorageOverlay::<Vec<u8>, V>::new(space)));
            }
        }
        f(&mut self.inner.get_mut(space).unwrap())
    }

    /// Handle with `f` for mutable reference without create default space.
    pub fn handle_mut<F, O>(&mut self, space: &[u8], f: F) -> Option<O>
    where
        F: FnOnce(&mut AnyStorage) -> O
    {
        self.inner.get_mut(space).map(|v| f(v))
    }

    /// Handle with `f` for immutable reference without create default space.
    pub fn handle_ref<F, O>(&self, space: &[u8], f: F) -> Option<O>
    where
        F: FnOnce(&AnyStorage) -> O
    {
        self.inner.get(space).map(|v| f(v))
    }
}

#[cfg(feature = "std")]
impl OverlayCache {
    pub fn put<V: Clone + FullCodec + 'static>(&mut self, space: &[u8], key: &[u8], value: V) {
        #[cfg(all(feature = "std", feature = "dev-time"))]
        let start = std::time::Instant::now();
        let f = |storage: &mut AnyStorage| {
            let step1 = storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>();
            #[cfg(all(feature = "std", feature = "dev-time"))]
            let time1 = start.elapsed();
            step1.map(|overlay| overlay.put(space, key, value));
            #[cfg(all(feature = "std", feature = "dev-time"))]
            {
                let time = start.elapsed();
                PUT.lock().unwrap().entry(space.to_vec()).or_default().push((time1, time));
            }
        };
        self.handle_mut_or_default::<V, _, _>(space, f);
    }

    pub fn try_update_raw(&mut self, space: &[u8], key: &[u8], data: Vec<u8>) {
        #[cfg(all(feature = "std", feature = "dev-time"))]
        let start = std::time::Instant::now();
        let f = |storage: &mut AnyStorage| {
            storage.try_update_raw(space, key, data);
            #[cfg(all(feature = "std", feature = "dev-time"))]
            {
                let time = start.elapsed();
                PUT.lock().unwrap().entry(space.to_vec()).or_default().push((Default::default(), time));
            }
        };
        self.handle_mut(space, f);
    }

    pub fn try_kill(&mut self, space: &[u8], key: &[u8]) {
        #[cfg(all(feature = "std", feature = "dev-time"))]
        let start = std::time::Instant::now();
        let f = |storage: &mut AnyStorage| {
            storage.try_kill(space, key);
            #[cfg(all(feature = "std", feature = "dev-time"))]
            {
                let time = start.elapsed();
                PUT.lock().unwrap().entry(space.to_vec()).or_default().push((Default::default(), time));
            }
        };
        self.handle_mut(space, f);
    }

    pub fn get<V: Clone + FullCodec + 'static, F>(&mut self, space: &[u8], key: &[u8], init: Option<F>) -> Option<Option<V>>
    where
        F: Fn(&[u8]) -> Option<V>
    {
        #[cfg(all(feature = "std", feature = "dev-time"))]
        let start = std::time::Instant::now();
        let f = |storage: &mut AnyStorage| {
            let step1 = storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>();
            #[cfg(all(feature = "std", feature = "dev-time"))]
            let time1 = start.elapsed();
            let res = step1.map(|overlay| overlay.get(space, key, init));
            #[cfg(all(feature = "std", feature = "dev-time"))]
            {
                let time = start.elapsed();
                GET.lock().unwrap().entry(space.to_vec()).or_default().push((time1, time));
            }
            res
        };
        self.handle_mut_or_default::<V, _, _>(space, f).unwrap_or(None)
    }

    pub fn get_change_encode(&self, space: &[u8], key: &[u8]) -> Option<Option<Vec<u8>>> {
        #[cfg(all(feature = "std", feature = "dev-time"))]
        let start = std::time::Instant::now();
        let f = |storage: &AnyStorage| {
            let res = storage.get_change_encode(key);
            #[cfg(all(feature = "std", feature = "dev-time"))]
            {
                let time = start.elapsed();
                GET.lock().unwrap().entry(space.to_vec()).or_default().push((Default::default(), time));
            }
            res
        };
        self.handle_ref::<_, _>(space, f)?
    }

    pub fn get_change<V: Clone + FullCodec + 'static>(&self, space: &[u8], key: &[u8]) -> Option<Option<V>> {
        #[cfg(all(feature = "std", feature = "dev-time"))]
        let start = std::time::Instant::now();
        let f = |storage: &AnyStorage| {
            let step1 = storage.downcast_ref::<StorageOverlay<Vec<u8>, V>>();
            #[cfg(all(feature = "std", feature = "dev-time"))]
            let time1 = start.elapsed();
            let res = step1.map(|overlay| overlay.get_change(space, key));
            #[cfg(all(feature = "std", feature = "dev-time"))]
            {
                let time = start.elapsed();
                GET.lock().unwrap().entry(space.to_vec()).or_default().push((time1, time));
            }
            res
        };
        self.handle_ref::<_, _>(space, f)?.unwrap_or(None)
    }

    pub fn take<V: Clone + FullCodec + 'static, F>(&mut self, space: &[u8], key: &[u8], init: Option<F>) -> Option<Option<V>>
    where
        F: Fn(&[u8]) -> Option<V>
    {
        #[cfg(all(feature = "std", feature = "dev-time"))]
        let start = std::time::Instant::now();
        let f = |storage: &mut AnyStorage| {
            let step1 = storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>();
            #[cfg(all(feature = "std", feature = "dev-time"))]
            let time1 = start.elapsed();
            let res = step1.map(|overlay| overlay.take(space, key, init));
            #[cfg(all(feature = "std", feature = "dev-time"))]
            {
                let time = start.elapsed();
                GET.lock().unwrap().entry(space.to_vec()).or_default().push((time1, time));
            }
            res
        };
        self.handle_mut_or_default::<V, _, _>(space, f).unwrap_or(None)
    }

    pub fn kill<V: Clone + FullCodec + 'static>(&mut self, space: &[u8], key: &[u8]) {
        #[cfg(all(feature = "std", feature = "dev-time"))]
        let start = std::time::Instant::now();
        let f = |storage: &mut AnyStorage| {
            let step1 = storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>();
            #[cfg(all(feature = "std", feature = "dev-time"))]
            let time1 = start.elapsed();
            step1.map(|overlay| overlay.kill(space, key));
            #[cfg(all(feature = "std", feature = "dev-time"))]
            {
                let time = start.elapsed();
                PUT.lock().unwrap().entry(space.to_vec()).or_default().push((time1, time));
            }
        };
        self.handle_mut_or_default::<V, _, _>(space, f);
    }

    pub fn mutate<V: Clone + FullCodec + 'static, F, M>(&mut self, space: &[u8], key: &[u8], init: Option<F>, mutate: M) -> bool
    where
        F: Fn(&[u8]) -> Option<V>,
        M: FnOnce(Option<&mut V>)
    {
        #[cfg(all(feature = "std", feature = "dev-time"))]
        let start = std::time::Instant::now();
        let f = |storage: &mut AnyStorage| {
            let step1 = storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>();
            #[cfg(all(feature = "std", feature = "dev-time"))]
            let time1 = start.elapsed();
            let res = step1.map(|overlay| overlay.mutate(space, key, init, mutate));
            #[cfg(all(feature = "std", feature = "dev-time"))]
            {
                let time = start.elapsed();
                PUT.lock().unwrap().entry(space.to_vec()).or_default().push((time1, time));
            }
            res
        };
        self.handle_mut_or_default::<V, _, _>(space, f).unwrap_or(false)
    }

    pub fn cache<V: Clone + FullCodec + 'static>(&mut self, space: &[u8], key: &[u8], value: Option<V>) {
        #[cfg(all(feature = "std", feature = "dev-time"))]
        let start = std::time::Instant::now();
        let f = |storage: &mut AnyStorage| {
            let step1 = storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>();
            #[cfg(all(feature = "std", feature = "dev-time"))]
            let time1 = start.elapsed();
            step1.map(|overlay| overlay.cache(space, key, value));
            #[cfg(all(feature = "std", feature = "dev-time"))]
            {
                let time = start.elapsed();
                PUT.lock().unwrap().entry(space.to_vec()).or_default().push((time1, time));
            }
        };
        self.handle_mut_or_default::<V, _, _>(space, f);
    }
}

#[cfg(feature = "std")]
impl OverlayCache {
    pub fn refresh(&mut self) {
        self.inner.clear();
        self.closed = false;
    }

    pub fn copy_data(&self) -> Self {
        Self {
            // cache: self.cache.clone(),
            inner: self.inner.iter()
                .map(|(space, overlay)| (space.clone(), overlay.copy_data()))
                .collect(),
            closed: self.closed,
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

    pub fn get_changed_keys_by_prefix(&self, space: &[u8]) -> Option<Vec<StorageKey>> {
        self.inner.get(space).map(|overlay| overlay.get_changed_keys())
    }
    
    pub fn get_commited(&self) -> BTreeMap<StorageKey, Option<Vec<u8>>> {
        if self.closed { panic!("OverlayCache::get_commited should only before closed"); }
        self.inner.iter().map(|(_, overlay)| overlay.get_commited()).flatten().collect()
    }

    pub fn drain_commited(&mut self) -> Vec<(StorageKey, Option<Vec<u8>>)> {
        if self.closed { panic!("OverlayCache::drain_commited should only be called once"); }
        self.closed = true;
        sp_std::mem::take(&mut self.inner)
            .into_iter()
            .map(|(_, mut overlay)| overlay.drain_commited()).flatten().collect()
    }
}

#[cfg(test)]
pub mod test {
    use codec::{Decode, Encode};
    use crate::{OverlayCache, StorageIO};

    pub mod a {
        use codec::{Decode, Encode};
        use crate::{OverlayCache, StorageIO};

        #[derive(Clone, Default, Eq, PartialEq, Encode, Decode, Debug)]
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
        assert_eq!(cache.get(b"Account", b"alice", Some(|_: &_| { None })), Some(Some(a::Account::new(b"alice", 0, 1000))));
        assert_eq!(cache.get(b"Account", b"bob", Some(|_: &_| { None })), Some(Some(a::Account::new(b"bob", 0, 500))));
        assert_eq!(cache.get(b"u32", b"alice_extend", Some(|_: &_| { None })), Some(Some(100u32)));
        assert_eq!(cache.get(b"u32", b"bob_extend", Some(|_: &_| { None })), Some(Some(50u32)));
    }

    #[test]
    fn test_multi_threads_write() {
        let mut cache1 = OverlayCache::default();
        let mut cache2 = cache1.copy_data();

        let mut cache1 = std::thread::spawn(move || {
            // this extra will insert `1u32` with key `thread1` at space `u32`. This is shared between threads.
            assert_eq!(cache1.get(b"u32", b"thread1", Some(|_: &_| { Some(1u32) })), Some(Some(1u32)));
            cache1.put(b"u32", b"thread1", 132u32);
            cache1.put(b"u64", b"thread1", 100u64);
            cache1
        }).join().unwrap();

        let mut none_f_u32 = Some(|_: &_| { None });
        none_f_u32.take();
        let mut cache2 = std::thread::spawn(move || {
            assert_eq!(cache2.get(b"u32", b"thread1", none_f_u32), Option::<Option<u32>>::None);
            cache2.put(b"u64", b"thread2", 200u64);
            cache2
        }).join().unwrap();

        let mut none_f_u64 = Some(|_: &_| { None });
        none_f_u64.take();
        // get values
        assert_eq!(cache1.get(b"u32", b"thread1", Some(|_: &_| { Some(1u32) })), Some(Some(132u32)));
        assert_eq!(cache1.get(b"u64", b"thread1", none_f_u64), Some(Some(100u64)));
        assert_eq!(cache2.get(b"u32", b"thread1", Some(|_: &_| { None })), Some(Option::<u32>::None));
        assert_eq!(cache2.get(b"u64", b"thread2", none_f_u64), Some(Some(200u64)));
        let _ = cache1.drain_commited();
        let _ = cache2.drain_commited();
        // get values from cached values
        assert_eq!(cache1.get(b"u32", b"thread1", Some(|_: &_| { Some(1u32) })), Some(Some(1u32)));
        assert_eq!(cache1.get(b"u64", b"thread1", none_f_u64), Option::<Option<u64>>::None);
        assert_eq!(cache2.get(b"u32", b"thread2", none_f_u32), Option::<Option<u32>>::None);
        assert_eq!(cache2.get(b"u64", b"thread2", none_f_u64), Option::<Option<u64>>::None);
    }

    #[test]
    fn test_storage_api() {
        let mut cache = OverlayCache::default();
        cache.enter_runtime();
        cache.put(b"u32", b"11", 11u32);
        cache.put(b"u32", b"12", 12u32);
        cache.put(b"u64", b"22", 22u64);
    }

    #[test]
    fn test_copy_data() {
        let mut cache = OverlayCache::default();
        cache.cache(b"u32", b"11", Some(0u32));
        cache.start_transaction();
        cache.put(b"u32", b"11", 11u32);

        let mut none_f_u32 = Some(|_: &_| { None });
        none_f_u32.take();
        let mut cache1 = cache.copy_data();
        assert_eq!(cache1.get(b"u32", b"11", none_f_u32), Some(Some(11u32)));
        cache1.rollback_transaction();
        assert_eq!(cache1.get(b"u32", b"11", none_f_u32), Some(Some(0u32)));
    }

    #[test]
    fn get_cache_change() {
        #[derive(Clone, Encode, PartialEq, Eq, Decode, Debug)]
        struct A {
            v: u32,
        }
        let mut cache = OverlayCache::default();
        cache.put(b"u32", b"11", A { v: 1 });

        {
            #[derive(Clone, Encode, PartialEq, Eq, Decode, Debug)]
            struct A {
                v: u32,
            }
            assert_eq!(cache.get_change(b"u32", b"11"), Some(Some(A { v: 1 })));
        }
        assert_eq!(cache.get_change(b"u32", b"11"), Some(Some(A { v: 1 })));
    }
}
