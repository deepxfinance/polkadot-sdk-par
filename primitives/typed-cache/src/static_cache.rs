#![allow(unused)]
use codec::{FullCodec, Output};
use sp_std::boxed::Box;
use sp_std::collections::btree_map::{BTreeMap, Entry::{Vacant, Occupied}};
use sp_std::sync::Arc;
#[cfg(feature = "std")]
use sp_std::sync::RwLock;
use sp_std::any::Any;
use crate::{QueryTransfer, RcT, StorageIO, StorageKey};
#[cfg(feature = "std")]
use crate::StorageApi;
#[cfg(feature = "std")]
use crate::changeset::StorageOverlay;
use crate::changeset::ExecutionMode;

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
    /// transactions layer when in ExecutionMode::Runtime.
    pub runtime_transactions: usize,
    /// transactions layer when in ExecutionMode::Client.
    pub client_transactions: usize,
    /// Determines whether the node is using the overlay from the client or the runtime.
    execution_mode: ExecutionMode,
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
                e.insert(Box::new(StorageOverlay::<Vec<u8>, V>::new(space, self.client_transactions, self.runtime_transactions)));
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
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.put(space, key, value));
        };
        self.handle_mut_or_default::<V, _, _>(space, f);
    }

    pub fn try_update_raw(&mut self, space: &[u8], key: &[u8], data: Vec<u8>) {
        let f = |storage: &mut AnyStorage| { storage.try_update_raw(space, key, data); };
        self.handle_mut(space, f);
    }

    pub fn try_kill(&mut self, space: &[u8], key: &[u8]) {
        let f = |storage: &mut AnyStorage| { storage.try_kill(space, key); };
        self.handle_mut(space, f);
    }

    pub fn contains_key<V: Clone + FullCodec + 'static>(&self, space: &[u8], key: &[u8]) -> Option<bool> {
        let f = |storage: &AnyStorage| {
            storage.downcast_ref::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.contains(space, key))
                .unwrap_or(None)
        };
        self.handle_ref::<_, _>(space, f).unwrap_or(None)
    }

    pub fn get<V: Clone + FullCodec + 'static, F>(&mut self, space: &[u8], key: &[u8], init: Option<F>) -> Option<Option<V>>
    where
        F: Fn(&[u8]) -> Option<V>
    {
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.get(space, key, init))
        };
        self.handle_mut_or_default::<V, _, _>(space, f).unwrap_or(None)
    }

    pub fn get_ref<V: Clone + FullCodec + 'static, F>(&mut self, space: &[u8], key: &[u8], init: Option<F>) -> Option<RcT<Option<V>>>
    where
        F: Fn(&[u8]) -> Option<V>
    {
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.get_ref(space, key, init))
        };
        self.handle_mut_or_default::<V, _, _>(space, f).unwrap_or(None)
    }

    pub fn get_change_encode(&self, space: &[u8], key: &[u8]) -> Option<Option<Vec<u8>>> {
        let f = |storage: &AnyStorage| { storage.get_change_encode(key) };
        self.handle_ref::<_, _>(space, f)?
    }

    pub fn get_change<V: Clone + FullCodec + 'static>(&self, space: &[u8], key: &[u8]) -> Option<Option<V>> {
        let f = |storage: &AnyStorage| {
            storage.downcast_ref::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.get_change(space, key))
        };
        self.handle_ref::<_, _>(space, f)?.unwrap_or(None)
    }

    pub fn get_change_ref<V: Clone + FullCodec + 'static>(&self, space: &[u8], key: &[u8]) -> Option<RcT<Option<V>>> {
        let f = |storage: &AnyStorage| {
            storage.downcast_ref::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.get_change_ref(space, key))
        };
        self.handle_ref::<_, _>(space, f)?.unwrap_or(None)
    }

    pub fn take<V: Clone + FullCodec + 'static>(&mut self, space: &[u8], key: &[u8]) -> Option<Option<V>> {
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.take(space, key))
        };
        self.handle_mut_or_default::<V, _, _>(space, f).unwrap_or(None)
    }

    pub fn pop_ref<V: Clone + FullCodec + 'static>(&mut self, space: &[u8], key: &[u8]) -> Option<RcT<Option<V>>> {
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.pop_ref(space, key))
                .unwrap_or(None)
        };
        self.handle_mut::<_, _>(space, f).unwrap_or(None)
    }

    pub fn kill<V: Clone + FullCodec + 'static>(&mut self, space: &[u8], key: &[u8]) {
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.kill(space, key));
        };
        self.handle_mut_or_default::<V, _, _>(space, f);
    }

    pub fn mutate<QT: QueryTransfer<V>, V: Clone + FullCodec + 'static, F, M, R, E>(&mut self, space: &[u8], key: &[u8], init: Option<F>, mutate: M) -> Option<Result<R, E>>
    where
        F: FnOnce() -> Option<V>,
        M: FnOnce(&mut QT::Query) -> Result<R, E>,
    {
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.mutate::<QT, _, _, _, _>(space, key, init, mutate))
        };
        self.handle_mut_or_default::<V, _, _>(space, f).unwrap_or(None)
    }

    pub fn append<V: Clone + FullCodec + 'static, F, M>(&mut self, space: &[u8], key: &[u8], init: F, mutate: M) -> bool
    where
        F: FnOnce() -> V,
        M: FnOnce(&mut Option<V>)
    {
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.append(space, key, init, mutate))
        };
        self.handle_mut_or_default::<V, _, _>(space, f).unwrap_or(false)
    }

    pub fn init<V: Clone + FullCodec + 'static>(&mut self, space: &[u8], key: &[u8], value: Option<V>) {
        let f = |storage: &mut AnyStorage| {
            storage.downcast_mut::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.init(space, key, value));
        };
        self.handle_mut_or_default::<V, _, _>(space, f);
    }

    pub fn cached<V: Clone + FullCodec + 'static>(&self, space: &[u8], key: &[u8]) -> bool {
        let f = |storage: &AnyStorage| {
            storage.downcast_ref::<StorageOverlay<Vec<u8>, V>>()
                .map(|overlay| overlay.cached(space, key))
                .unwrap_or(false)
        };
        self.handle_ref::<_, _>(space, f).unwrap_or(false)
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
            runtime_transactions: self.runtime_transactions,
            client_transactions: self.client_transactions,
            execution_mode: self.execution_mode.clone(),
            closed: self.closed,
        }
    }

    pub fn enter_runtime(&mut self) {
        self.execution_mode = ExecutionMode::Runtime;
        self.inner.iter_mut().for_each(|(_, overlay)| overlay.enter_runtime());
    }

    pub fn exit_runtime(&mut self) {
        self.execution_mode = ExecutionMode::Client;
        let max_depth = self.inner.iter_mut().map(|(_, overlay)| overlay.exit_runtime()).max().unwrap_or(self.client_transactions);
        self.client_transactions = self.client_transactions.max(max_depth);
        self.runtime_transactions = 0;
    }

    pub fn start_transaction(&mut self) {
        self.inner.iter_mut().for_each(|(_, overlay)| overlay.start_transaction());
        match self.execution_mode {
            ExecutionMode::Runtime => self.runtime_transactions = self.runtime_transactions.saturating_add(1),
            ExecutionMode::Client => self.client_transactions = self.client_transactions.saturating_add(1),
        }
    }

    pub fn commit_transaction(&mut self) {
        self.inner.iter_mut().for_each(|(_, overlay)| overlay.commit_transaction(&self.execution_mode));
        match self.execution_mode {
            ExecutionMode::Runtime => self.runtime_transactions = self.runtime_transactions.saturating_sub(1),
            ExecutionMode::Client => self.client_transactions = self.client_transactions.saturating_sub(1),
        }
    }

    pub fn rollback_transaction(&mut self) {
        self.inner.iter_mut().for_each(|(_, overlay)| overlay.rollback_transaction(&self.execution_mode));
        match self.execution_mode {
            ExecutionMode::Runtime => self.runtime_transactions = self.runtime_transactions.saturating_sub(1),
            ExecutionMode::Client => self.client_transactions = self.client_transactions.saturating_sub(1),
        }
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
        cache1.init(b"u128", b"thread1", Some(128u128));
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
        let mut none_f_u128 = Some(|_: &_| { Some(1u128) });
        none_f_u128.take();
        let mut cache2 = std::thread::spawn(move || {
            assert_eq!(cache2.get(b"u32", b"thread1", none_f_u32), Option::<Option<u32>>::None);
            cache2.put(b"u64", b"thread2", 200u64);
            assert_eq!(cache2.get(b"u128", b"thread1", none_f_u128), Some(Some(128u128)));
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
        cache.init(b"u32", b"11", Some(0u32));
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
            assert_eq!(cache.get_change::<A>(b"u32", b"11"), None);
        }
        assert_eq!(cache.get_change(b"u32", b"11"), Some(Some(A { v: 1 })));
    }

    #[test]
    fn get_cache_change_ref() {
        #[derive(Clone, Encode, PartialEq, Eq, Decode, Debug)]
        struct A {
            v: u32,
        }
        let mut cache = OverlayCache::default();
        cache.init::<A>(b"not_change", b"11", None);
        cache.put(b"u32", b"11", A { v: 1 });

        {
            #[derive(Clone, Encode, PartialEq, Eq, Decode, Debug)]
            struct A {
                v: u32,
            }
            assert_eq!(cache.get_change_ref::<A>(b"u32", b"11").map(|r| r.clone_inner()), None);
        }
        assert_eq!(cache.get_change_ref(b"u32", b"11").unwrap().clone_inner(), Some(A { v: 1 }));

        let mut value_ref = cache.get_change_ref::<A>(b"u32", b"11").unwrap();
        value_ref.mutate(|a| a.as_mut().map(|v| v.v += 1));
        assert_eq!(cache.get_change_ref(b"u32", b"11").unwrap().clone_inner(), Some(A { v: 2 }));

        cache.init(b"another", b"11", Some(A { v: 10 }));
        let mut value_ref = cache.get_ref::<A, _>(b"another", b"11", Some(|_: &_| { Some(A { v: 10 }) })).unwrap();
        assert_eq!(value_ref.muted(), false);
        value_ref.mutate(|a| a.as_mut().map(|v| v.v += 1));
        assert_eq!(value_ref.muted(), true);
        assert_eq!(cache.get_change_ref(b"another", b"11").unwrap().clone_inner(), Some(A { v: 11 }));
        assert_eq!(cache.get_change_ref::<A>(b"another", b"11").unwrap().muted(), true);
        assert_eq!(cache.get_change_ref::<A>(b"another", b"11").unwrap().into_inner(), Err(3));
        let a = value_ref.muted();
        let popped_ref = cache.pop_ref::<A>(b"another", b"11");
        assert_eq!(popped_ref.as_ref().map(|c| c.clone_ref()).unwrap().into_inner(), Err(3));
        drop(value_ref);
        assert_eq!(popped_ref.unwrap().into_inner(), Ok(Some(Some(A { v: 11 }))));

        assert_eq!(cache.drain_commited().len(), 1);
    }

    #[test]
    fn test_rollback_transaction() {
        env_logger::init();
        let mut cache = OverlayCache::default();
        cache.enter_runtime();
        cache.put(b"u32", b"3211", 1u32);
        cache.start_transaction();
        cache.put(b"u32", b"3211", 2u32);
        cache.put(b"u64", b"6411", 2u64);
        cache.rollback_transaction();
        cache.exit_runtime();
        assert_eq!(cache.get_change(b"u32", b"3211"), Some(Some(1u32)));
        assert_eq!(cache.get_change(b"u64", b"6411"), Option::<Option<u64>>::None);
    }
}
