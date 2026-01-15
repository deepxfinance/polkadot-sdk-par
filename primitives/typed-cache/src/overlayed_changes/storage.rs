use sp_std::boxed::Box;
use sp_std::collections::btree_map::BTreeMap;
use sp_std::sync::Arc;
use sp_std::hash::Hash;
use sp_std::vec::Vec;
use codec::FullCodec;
use once_cell::sync::OnceCell;
use crate::changeset::OverlayedMap;
use crate::{StorageIO, StorageApi, StorageKey};

pub type Cache<T> = Arc<OnceCell<T>>;

// TODO support just simple `value` which doesn't need `Map` to cache many values.
#[derive(Clone)]
pub struct StorageOverlay<K: Ord + Hash + Clone, V: Clone> {
    pub space: Vec<u8>,
    /// Cached best value.
    /// For cache, any value will only insert once(data should not change).
    cache: BTreeMap<K, Cache<Option<V>>>,
    changes: OverlayedMap<K, Option<V>>,
}

impl<K: Ord + Hash + Clone, V: Clone> StorageOverlay<K, V> {
    pub fn new(space: &[u8]) -> Self {
        Self {
            space: space.to_vec(),
            ..Default::default()
        }
    }

    pub fn clone_with_changes(&self) -> Self {
        Self {
            space: self.space.clone(),
            cache: self.cache.clone(),
            // changes are different(if changed) between copies
            changes: self.changes.clone(),
        }
    }
}

impl<K: Ord + Hash + Clone, V: Clone> Default for StorageOverlay<K, V> {
    fn default() -> Self {
        Self {
            space: Vec::new(),
            cache: Default::default(),
            changes: Default::default(),
        }
    }
}

unsafe impl<K: Ord + Hash + Clone, V: Clone> Send for StorageOverlay<K, V> {}
unsafe impl<K: Ord + Hash + Clone, V: Clone> Sync for StorageOverlay<K, V> {}

impl<V: Clone + FullCodec + 'static> StorageApi for StorageOverlay<StorageKey, V> {
    fn enter_runtime(&mut self) {
        self.changes.enter_runtime();
    }

    fn exit_runtime(&mut self) {
        self.changes.exit_runtime();
    }

    fn start_transaction(&mut self) {
        self.changes.start_transaction()
    }

    fn commit_transaction(&mut self) {
        // ignore here since we mey call any unchanged storages.
        let _ = self.changes.commit_transaction();
    }

    fn rollback_transaction(&mut self) {
        // ignore here since we mey call any unchanged storages.
        let _ = self.changes.rollback_transaction();
    }

    fn get_change_encode(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.changes.changes.get(key).map(|v| v.value().map(|v| v.encode()))
    }

    fn get_changed_keys(&self) -> Vec<StorageKey> {
        self.changes
            .changes
            .iter()
            .map(|(k , _)| k.clone())
            .collect()
    }

    fn get_commited(&self) -> BTreeMap<StorageKey, Option<Vec<u8>>> {
        self.changes
            .changes
            .iter()
            .map(|(k ,v)| {
                #[cfg(all(feature = "std", feature = "dev-time"))]
                let start = std::time::Instant::now();
                let res = (k.clone(), v.value().map(|v| v.encode()));
                #[cfg(all(feature = "std", feature = "dev-time"))]
                crate::ENCODE.lock().unwrap().entry(self.space.clone()).or_default().push(start.elapsed());
                res
            })
            .collect()
    }

    fn drain_commited(&mut self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.changes.drain_changes()
            .into_iter()
            .map(|(k, mut v)| {
                #[cfg(all(feature = "std", feature = "dev-time"))]
                let start = std::time::Instant::now();
                let res = (k, v.pop_value().map(|v| v.encode()));
                #[cfg(all(feature = "std", feature = "dev-time"))]
                crate::ENCODE.lock().unwrap().entry(self.space.clone()).or_default().push(start.elapsed());
                res
            })
            .collect()
    }

    fn copy_data(&self) -> Box<dyn StorageApi> {
        Box::new(self.clone_with_changes())
    }

    fn try_update_raw(&mut self, space: &[u8], key: &[u8], data: Vec<u8>) {
        if space != &self.space { return; }
        if let Ok(value) = codec::Decode::decode(&mut data.as_slice()) {
            self.changes.set(key.to_vec(), Some(value), None);
        }
    }

    fn try_kill(&mut self, space: &[u8], key: &[u8]) {
        if space != &self.space { return; }
        self.changes.set(key.to_vec(), None, None);
    }
}

impl<V: Clone> StorageIO<V> for StorageOverlay<StorageKey, V> {
    fn contains(&self, space: &[u8], key: &[u8]) -> bool {
        if space != self.space { return false; }
        self.changes.changes.contains_key(key)
    }
    
    fn put(&mut self, space: &[u8], key: &[u8], value: V) {
        if space != &self.space { return; }
        self.changes.set(key.to_vec(), Some(value), None);
    }

    fn get<F>(&mut self, space: &[u8], key: &[u8], init: Option<F>) -> Option<Option<V>>
    where
        F: Fn(&[u8]) -> Option<V>
    {
        if space != &self.space { return None; }
        self.changes
            .get(key)
            .map(|x| x.value().cloned())
            .or(
                self.cache
                    .get(key)
                    .map(|c| c.get().cloned().unwrap_or(None))
                    .or({
                        init.map(|f| {
                            let cache_value = f(key);
                            let new_cell = Arc::new(OnceCell::new());
                            let _ = new_cell.set(cache_value.clone());
                            self.cache.insert(key.to_vec(), new_cell);
                            cache_value
                        })
                    })
            )
    }

    fn get_change(&self, space: &[u8], key: &[u8]) -> Option<Option<V>> {
        if space != &self.space { return None; }
        self.changes.get(key).map(|x| x.value().cloned())
    }

    fn take<F>(&mut self, space: &[u8], key: &[u8], init: Option<F>) -> Option<Option<V>>
    where
        F: Fn(&[u8]) -> Option<V>
    {
        if space != &self.space { return None; }
        let v = self.get(space, key, init);
        if let Some(Some(_)) = &v { self.kill(space, key); }
        v
    }

    fn kill(&mut self, space: &[u8], key: &[u8]) {
        if space != &self.space { return; }
        self.changes.set(key.to_vec(), None, None);
    }

    fn mutate<F, M>(&mut self, space: &[u8], key: &[u8], init: F, mutate: M) -> bool
    where
        F: Fn() -> V,
        M: FnOnce(Option<&mut V>)
    {
        if space != &self.space { return false; }
        // modify must have mutable reference of value returned
        mutate(self.changes.modify(key.to_vec(), init, None).as_mut());
        true
    }

    fn cache(&mut self, space: &[u8], key: &[u8], value: Option<V>) {
        if space != &self.space { return; }
        let new_cache = Arc::new(OnceCell::<Option<V>>::new());
        let _ = new_cache.try_insert(value);
        self.cache.insert(key.to_vec(), new_cache);
    }
}

#[test]
fn test_basic_types() {
    let mut overlay1 = StorageOverlay::new(b"str");
    let mut overlay2 = StorageOverlay::new(b"str");

    let mut none_f = Some(|_: &_| { None });
    none_f.take();

    overlay1.put(b"str", b"key1", "value1");
    overlay1.put(b"str", b"key2", "value2");
    overlay2.put(b"str", b"key3", "value3");
    assert_eq!(overlay1.get(b"str", b"key1", none_f), Some(Some("value1")));
    assert_eq!(overlay1.get(b"str", b"key2", none_f), Some(Some("value2")));
    assert_eq!(overlay2.get(b"str", b"key3", none_f), Some(Some("value3")));
}

#[test]
fn test_commit() {
    let mut none_f = Some(|_: &_| { None });
    none_f.take();
    let mut overlay = StorageOverlay::new(b"str");
    overlay.start_transaction();
    overlay.put(b"str", b"key1", b"value1".to_vec());
    overlay.rollback_transaction();
    overlay.put(b"str", b"key2", b"value2".to_vec());
    assert_eq!(overlay.get(b"str", b"key1", none_f), Option::<Option<Vec<u8>>>::None);
    assert_eq!(overlay.get(b"str", b"key2", none_f), Some(Some(b"value2".to_vec())));
}

#[test]
fn test_clone_for_multi_threads() {
    let mut none_f = Some(|_: &_| { None });
    none_f.take();
    let mut overlay1 = StorageOverlay::new(b"str");
    overlay1.put(b"str", b"key1", "value1");
    let mut overlay2 = overlay1.clone_with_changes();
    let mut overlay2 = std::thread::spawn(move || {
        overlay2.put(b"str", b"key2", "value2");
        overlay2
    }).join().unwrap();

    assert_eq!(overlay1.get(b"str", b"key1", none_f), Some(Some("value1")));
    assert_eq!(overlay1.get(b"str", b"key2", none_f), Option::<Option<&str>>::None);
    assert_eq!(overlay2.get(b"str", b"key1", none_f), Some(Some("value1")));
    assert_eq!(overlay2.get(b"str", b"key2", none_f), Some(Some("value2")));
}

#[test]
fn test_cache() {
    let mut none_f = Some(|_: &_| { None });
    none_f.take();
    let mut overlay = StorageOverlay::new(b"str");
    overlay.cache(b"str", b"key1", Some("value1"));
    assert_eq!(overlay.get(b"str", b"key1", none_f), Some(Some("value1")));
}

#[test]
fn test_storage_copy_data() {
    let mut none_f = Some(|_: &_| { None });
    none_f.take();
    let mut overlay = StorageOverlay::new(b"str");
    overlay.cache(b"str", b"key1", Some(b"value1".to_vec()));
    assert_eq!(overlay.get(b"str", b"key1", none_f), Some(Some(b"value1".to_vec())));
    overlay.start_transaction();
    overlay.put(b"str", b"key1", b"value2".to_vec());
    assert_eq!(overlay.get(b"str", b"key1", none_f), Some(Some(b"value2".to_vec())));

    let mut overlay1 = overlay.copy_data();

    overlay.rollback_transaction();
    assert_eq!(overlay.get(b"str", b"key1", none_f), Some(Some(b"value1".to_vec())));

    assert_eq!(
        overlay1.downcast_mut::<StorageOverlay<Vec<u8>, Vec<u8>>>()
            .unwrap()
            .get(b"str", b"key1", none_f),
        Some(Some(b"value2".to_vec()))
    );
    overlay1.rollback_transaction();
    assert_eq!(
        overlay1
            .downcast_mut::<StorageOverlay<Vec<u8>, Vec<u8>>>()
            .unwrap()
            .get(b"str", b"key1", none_f),
        Some(Some(b"value1".to_vec()))
    );
}
