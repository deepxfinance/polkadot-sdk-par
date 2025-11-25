use sp_std::collections::btree_map::BTreeMap;
use sp_std::sync::{Arc, RwLock};
use sp_std::hash::Hash;
use sp_std::vec::Vec;
use codec::Encode;
use crate::changeset::OverlayedMap;
use crate::{StorageIO, StorageApi, StorageKey};

#[derive(Clone, Default)]
pub struct Cache<T> {
    pub inner: Arc<RwLock<T>>,
}

// TODO support just simple `value` which doesn't need `Map` to cache many values.
#[derive(Clone)]
pub struct StorageOverlay<K: Ord + Hash + Clone, V: Clone> {
    pub space: Vec<u8>,
    /// Cached best value.
    /// For cache, any value will only insert once(data should not change).
    cache: Cache<BTreeMap<K, V>>,
    changes: Arc<RwLock<OverlayedMap<K, Option<V>>>>,
}

impl<K: Ord + Hash + Clone, V: Clone> StorageOverlay<K, V> {
    pub fn new(space: &[u8], cache: Option<Cache<BTreeMap<K, V>>>) -> Self {
        Self {
            space: space.to_vec(),
            cache: cache.unwrap_or(Cache::default()),
            ..Default::default()
        }
    }

    pub fn clone_with_changes(&self) -> Self {
        Self {
            space: self.space.clone(),
            // cache are shared between copies
            cache: self.cache.clone(),
            // changes are different(if changed) between copies
            changes: Arc::new(RwLock::new(self.changes.read().unwrap().clone())),
        }
    }

    pub fn drain_changes(&self) -> Vec<(K, Option<V>)> {
        self.changes
            .write()
            .unwrap()
            .drain_changes()
            .into_iter()
            .map(|(k, v)| (k.clone(), v.into_value()))
            .collect()
    }
}

impl<K: Ord + Hash + Clone, V: Clone> Default for StorageOverlay<K, V> {
    fn default() -> Self {
        Self {
            space: Vec::new(),
            cache: Cache::default(),
            changes: Default::default(),
        }
    }
}

unsafe impl<K: Ord + Hash + Clone, V: Clone> Send for StorageOverlay<K, V> {}
unsafe impl<K: Ord + Hash + Clone, V: Clone> Sync for StorageOverlay<K, V> {}

impl<V: Clone + Encode + 'static> StorageApi for StorageOverlay<StorageKey, V> {
    fn enter_runtime(&self) {
        self.changes.write().unwrap().enter_runtime();
    }

    fn exit_runtime(&self) {
        self.changes.write().unwrap().exit_runtime();
    }

    fn start_transaction(&self) {
        self.changes.write().unwrap().start_transaction()
    }

    fn commit_transaction(&self) {
        self.changes.write().unwrap().commit_transaction().unwrap();
    }

    fn rollback_transaction(&self) {
        self.changes.write().unwrap().rollback_transaction().unwrap();
    }

    fn drain_commited(&self) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        self.changes.write().unwrap().drain_changes()
            .into_iter()
            .map(|(k, mut v)| (k, v.pop_value()))
            .map(|(k ,v)| (k, v.map(|v| v.encode())))
            .collect()
    }

    fn copy_data(&self) -> Arc<Box<dyn StorageApi>> {
        Arc::new(Box::new(self.clone_with_changes()))
    }
}

impl<V: Clone + 'static> StorageIO<V> for StorageOverlay<StorageKey, V> {
    fn put(&self, space: &[u8], key: &[u8], value: V) {
        if space != &self.space { return; }
        self.changes.write().unwrap().set(key.to_vec(), Some(value), None);
    }

    fn get<F>(&self, space: &[u8], key: &[u8], extra: F) -> Option<V>
    where
        F: Fn(&[u8]) -> Option<V>
    {
        if space != &self.space { return None; }
        self.changes
            .write()
            .unwrap()
            .get(key)
            .map(|x| x.value().cloned())
            .or({
                let cached = self.cache.inner.read().unwrap().get(key).cloned();
                if let Some(v) = cached {
                    Some(Some(v))
                } else {
                    match extra(key) {
                        Some(v) => {
                            // data from extra will be stored in shared cache between copies.
                            self.cache.inner.write().unwrap().insert(key.to_vec(), v.clone());
                            Some(Some(v))
                        },
                        None => None,
                    }
                }
            })?
    }

    fn take<F>(&self, space: &[u8], key: &[u8], extra: F) -> Option<V>
    where
        F: Fn(&[u8]) -> Option<V>
    {
        if space != &self.space { return None; }
        let v = self.get(space, key, extra);
        if v.is_some() { self.kill(space, key); }
        v
    }

    fn kill(&self, space: &[u8], key: &[u8]) {
        if space != &self.space { return; }
        self.changes.write().unwrap().set(key.to_vec(), None, None);
    }
}

#[test]
fn test_basic_types() {
    let overlay1 = StorageOverlay::new(b"str", None);
    let overlay2 = StorageOverlay::new(b"str", None);

    overlay1.put(b"str", b"key1", "value1");
    overlay1.put(b"str", b"key2", "value2");
    overlay2.put(b"str", b"key3", "value3");
    assert_eq!(overlay1.get(b"str", b"key1", |_| { None }), Some("value1"));
    assert_eq!(overlay1.get(b"str", b"key2", |_| { None }), Some("value2"));
    assert_eq!(overlay2.get(b"str", b"key3", |_| { None }), Some("value3"));
}

#[test]
fn test_commit() {
    let overlay = StorageOverlay::new(b"str", None);
    overlay.start_transaction();
    overlay.put(b"str", b"key1", "value1");
    overlay.rollback_transaction();
    overlay.put(b"str", b"key2", "value2");
    assert_eq!(overlay.get(b"str", b"key1", |_| { None }), None);
    assert_eq!(overlay.get(b"str", b"key2", |_| { None }), Some("value2"));
}

#[test]
fn test_clone_for_multi_threads() {
    let overlay1 = StorageOverlay::new(b"str", None);
    overlay1.put(b"str", b"key1", "value1");
    let overlay2 = overlay1.clone_with_changes();
    let overlay2 = std::thread::spawn(move || {
        overlay2.put(b"str", b"key2", "value2");
        overlay2
    }).join().unwrap();

    assert_eq!(overlay1.get(b"str", b"key1", |_| { None }), Some("value1"));
    assert_eq!(overlay1.get(b"str", b"key2", |_| { None }), None);
    assert_eq!(overlay2.get(b"str", b"key1", |_| { None }), Some("value1"));
    assert_eq!(overlay2.get(b"str", b"key2", |_| { None }), Some("value2"));
}
