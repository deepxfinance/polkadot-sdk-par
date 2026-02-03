use sp_std::boxed::Box;
use sp_std::collections::btree_map::BTreeMap;
use sp_std::sync::Arc;
use sp_std::hash::Hash;
use sp_std::vec::Vec;
use codec::FullCodec;
use once_cell::sync::OnceCell;
use crate::{StorageIO, StorageApi, StorageKey, QueryTransfer};
use crate::changeset::{ExecutionMode, StorageOverlay};

unsafe impl<K: Ord + Hash + Clone, V: Clone> Send for StorageOverlay<K, V> {}
unsafe impl<K: Ord + Hash + Clone, V: Clone> Sync for StorageOverlay<K, V> {}

impl<V: Clone + FullCodec + 'static> StorageApi for StorageOverlay<StorageKey, Option<V>> {
    fn enter_runtime(&mut self) {
        self.enter_runtime();
    }

    fn exit_runtime(&mut self) -> usize {
        self.exit_runtime()
    }

    fn start_transaction(&mut self) {
        self.start_transaction()
    }

    fn commit_transaction(&mut self, mode: &ExecutionMode) {
        self.commit_transaction(mode).expect("commit_transaction NoOpenTransaction");
    }

    fn rollback_transaction(&mut self, mode: &ExecutionMode) {
        // ignore here since we mey call any unchanged storages.
        self.rollback_transaction(mode).expect("rollback_transaction NoOpenTransaction");
    }

    fn get_change_encode(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.changes.get(key).map(|v| v.value().map(|v| v.encode()))
    }

    fn get_changed_keys(&self) -> Vec<StorageKey> {
        self.changes
            .iter()
            .map(|(k , _)| k.clone())
            .collect()
    }

    fn get_commited(&self) -> BTreeMap<StorageKey, Option<Vec<u8>>> {
        self.changes
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
        self.drain_changes()
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
            self.set(key.to_vec(), Some(value), None);
        }
    }

    fn try_kill(&mut self, space: &[u8], key: &[u8]) {
        if space != &self.space { return; }
        self.set(key.to_vec(), None, None);
    }
}

impl<V: Clone> StorageIO<V> for StorageOverlay<StorageKey, Option<V>> {
    fn contains(&self, space: &[u8], key: &[u8]) -> Option<bool> {
        if space != self.space { return None; }
        self.changes
            .get(key)
            .map(|x| x.value().is_some())
            .or(
                self.cache
                    .get(key)
                    .map(|c| c
                        .get()
                        .is_some()
                    )
            )
    }
    
    fn put(&mut self, space: &[u8], key: &[u8], value: V) {
        if space != &self.space { return; }
        self.set(key.to_vec(), Some(value), None);
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
        self.set(key.to_vec(), None, None);
    }

    fn mutate<QT: QueryTransfer<V>, F, R, E, M>(&mut self, space: &[u8], key: &[u8], init: Option<F>, mutate: M) -> Option<Result<R, E>>
    where
        F: FnOnce() -> Option<V>,
        M: FnOnce(&mut QT::Query) -> Result<R, E>,
    {
        if space != &self.space { return None; }
        // modify must have mutable reference of value returned
        match self.modify(key.to_vec(), init, None) {
            Some(v) => {
                let (res, new_value) = QT::mut_from_optional_value_to_query(v, mutate);
                if let Some(new_value) = new_value {
                    self.set(key.to_vec(), Some(new_value), None);
                }
                Some(res)
            }
            None => None,
        }
    }

    fn append<F, M>(&mut self, space: &[u8], key: &[u8], init: F, mutate: M) -> bool
    where
        F: FnOnce() -> V,
        M: FnOnce(Option<&mut V>)
    {
        if space != &self.space { return false; }
        // modify must have mutable reference of value returned
        mutate(self.modify_append(key.to_vec(), init, None));
        true
    }

    fn cache(&mut self, space: &[u8], key: &[u8], value: Option<V>) {
        if space != &self.space { return; }
        let new_cache = Arc::new(OnceCell::<Option<V>>::new());
        let _ = new_cache.try_insert(value);
        self.cache.insert(key.to_vec(), new_cache);
    }
    
    fn cached(&self, space: &[u8], key: &[u8]) -> bool {
        if space != &self.space { return false; }
        self.changes
            .get(key)
            .map(|_| true)
            .or(
                self.cache
                    .get(key)
                    .map(|c| c
                        .get()
                        .is_some()
                    )
            )
            .unwrap_or(false)
    }
}

#[test]
fn test_basic_types() {
    let mut overlay1 = StorageOverlay::new(b"str", 0, 0);
    let mut overlay2 = StorageOverlay::new(b"str", 0, 0);

    let mut none_f = Some(|_: &_| { None });
    none_f.take();

    overlay1.put(b"str", b"key1", b"value1".to_vec());
    overlay1.put(b"str", b"key2", b"value2".to_vec());
    overlay2.put(b"str", b"key3", b"value3".to_vec());
    assert_eq!(overlay1.get(b"str", b"key1", none_f), Some(Some(b"value1".to_vec())));
    assert_eq!(overlay1.get(b"str", b"key2", none_f), Some(Some(b"value2".to_vec())));
    assert_eq!(overlay2.get(b"str", b"key3", none_f), Some(Some(b"value3".to_vec())));
}

#[test]
fn test_commit() {
    let mut none_f = Some(|_: &_| { None });
    none_f.take();
    let mut overlay = StorageOverlay::new(b"str", 0, 0);
    overlay.start_transaction();
    overlay.put(b"str", b"key1", b"value1".to_vec());
    let _ = overlay.rollback_transaction(&ExecutionMode::Client);
    overlay.put(b"str", b"key2", b"value2".to_vec());
    assert_eq!(overlay.get(b"str", b"key1", none_f), Option::<Option<Vec<u8>>>::None);
    assert_eq!(overlay.get(b"str", b"key2", none_f), Some(Some(b"value2".to_vec())));
}

#[test]
fn test_clone_for_multi_threads() {
    let mut none_f = Some(|_: &_| { None });
    none_f.take();
    let mut overlay1 = StorageOverlay::new(b"str", 0, 0);
    overlay1.put(b"str", b"key1", b"value1".to_vec());
    let mut overlay2 = overlay1.clone_with_changes();
    let mut overlay2 = std::thread::spawn(move || {
        overlay2.put(b"str", b"key2", b"value2".to_vec());
        overlay2
    }).join().unwrap();

    assert_eq!(overlay1.get(b"str", b"key1", none_f), Some(Some(b"value1".to_vec())));
    assert_eq!(overlay1.get(b"str", b"key2", none_f), Option::<Option<Vec<u8>>>::None);
    assert_eq!(overlay2.get(b"str", b"key1", none_f), Some(Some(b"value1".to_vec())));
    assert_eq!(overlay2.get(b"str", b"key2", none_f), Some(Some(b"value2".to_vec())));
}

#[test]
fn test_cache() {
    let mut none_f = Some(|_: &_| { None });
    none_f.take();
    let mut overlay = StorageOverlay::new(b"str", 0, 0);
    overlay.cache(b"str", b"key1", Some(b"value1".to_vec()));
    assert_eq!(overlay.get(b"str", b"key1", none_f), Some(Some(b"value1".to_vec())));
}

#[test]
fn test_storage_copy_data() {
    let mut none_f = Some(|_: &_| { None });
    none_f.take();
    let mut overlay = StorageOverlay::new(b"str", 0, 0);
    overlay.cache(b"str", b"key1", Some(b"value1".to_vec()));
    assert_eq!(overlay.get(b"str", b"key1", none_f), Some(Some(b"value1".to_vec())));
    overlay.start_transaction();
    overlay.put(b"str", b"key1", b"value2".to_vec());
    assert_eq!(overlay.get(b"str", b"key1", none_f), Some(Some(b"value2".to_vec())));

    let mut overlay1 = overlay.copy_data();

    let _ = overlay.rollback_transaction(&ExecutionMode::Client);
    assert_eq!(overlay.get(b"str", b"key1", none_f), Some(Some(b"value1".to_vec())));

    assert_eq!(
        overlay1.downcast_mut::<StorageOverlay<Vec<u8>, Option<Vec<u8>>>>()
            .unwrap()
            .get(b"str", b"key1", none_f),
        Some(Some(b"value2".to_vec()))
    );
    overlay1.rollback_transaction(&ExecutionMode::Client);
    assert_eq!(
        overlay1
            .downcast_mut::<StorageOverlay<Vec<u8>, Option<Vec<u8>>>>()
            .unwrap()
            .get(b"str", b"key1", none_f),
        Some(Some(b"value1".to_vec()))
    );
}
