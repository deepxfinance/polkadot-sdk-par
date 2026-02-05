use std::ops::Deref;
use sp_std::boxed::Box;
use sp_std::collections::btree_map::BTreeMap;
use sp_std::hash::Hash;
use sp_std::vec::Vec;
use codec::FullCodec;
use crate::{StorageIO, StorageApi, StorageKey, QueryTransfer, RcT, TypedAppend};
use crate::changeset::StorageOverlay;
use crate::overlayed_changes::ExecutionMode;

unsafe impl<QT: QueryTransfer<V>, K: Ord + Hash + Clone, V: Clone> Send for StorageOverlay<QT, K, V> {}
unsafe impl<QT: QueryTransfer<V>, K: Ord + Hash + Clone, V: Clone> Sync for StorageOverlay<QT, K, V> {}

impl<QT: QueryTransfer<V>, V: Clone + FullCodec + 'static> StorageApi for StorageOverlay<QT, StorageKey, V> {
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
        match self.changes.get(key) {
            Some(entry) => {
                let value = entry.value_ref();
                if value.muted() {
                    Some(QT::from_query_to_optional_value(value.clone_inner()).map(|v| v.encode()))
                } else {
                    None
                }
            },
            None => None,
        }
    }

    fn get_changed_keys(&self) -> Vec<StorageKey> {
        self.changes
            .iter()
            .filter_map(|(k , v)|
                if v.value_ref().muted() {
                    Some(k.clone())
                } else {
                    None
                }
            )
            .collect()
    }

    fn get_commited(&self) -> BTreeMap<StorageKey, Option<Vec<u8>>> {
        self.changes
            .iter()
            .filter_map(|(k ,v)|
                if v.value_ref().muted() {
                    Some((k.clone(), QT::from_query_to_optional_value(v.value_ref().clone_inner()).as_ref().map(|v| v.encode())))
                } else {
                    None
                }
            )
            .collect()
    }

    fn drain_commited(&mut self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.drain_changes()
            .into_iter()
            .filter_map(|(k, mut v)|
                if v.value_ref().muted() {
                    Some((
                        k,
                        QT::from_query_to_optional_value(
                        v.pop_value().into_inner().expect("drain_commited should have no ref")
                        ).as_ref()
                        .map(|v| v.encode()))
                    )
                } else {
                    None
                }
            )
            .collect()
    }

    fn copy_data(&self) -> Box<dyn StorageApi> {
        Box::new(self.clone_with_changes())
    }

    fn try_update_raw(&mut self, space: &[u8], key: &[u8], data: Vec<u8>) {
        if space != &self.space { return; }
        if let Ok(value) = codec::Decode::decode(&mut data.as_slice()) {
            self.set(key.to_vec(), QT::from_optional_value_to_query(Some(value)), None);
        }
    }

    fn try_kill(&mut self, space: &[u8], key: &[u8]) {
        if space != &self.space { return; }
        self.set(key.to_vec(), QT::from_optional_value_to_query(None), None);
    }
}

impl<QT: QueryTransfer<V>, V: Clone> StorageIO<QT, V> for StorageOverlay<QT, StorageKey, V> {
    fn contains(&self, space: &[u8], key: &[u8]) -> Option<bool> {
        if space != self.space { return None; }
        self.changes
            .get(key)
            .map(|x| <QT as QueryTransfer<V>>::exists(x.value_ref().borrow().deref()))
    }
    
    fn put(&mut self, space: &[u8], key: &[u8], value: V) {
        if space != &self.space { return; }
        self.set(key.to_vec(), QT::from_optional_value_to_query(Some(value)), None);
    }

    fn get<F>(&mut self, space: &[u8], key: &[u8], init: Option<F>) -> Option<Option<V>>
    where
        F: Fn(&[u8]) -> Option<V>
    {
        if space != &self.space { return None; }
        if let Some(x) = self.changes.get(key) {
            Some(QT::from_query_to_optional_value(x.value_ref().clone_inner()))
        } else {
            init.map(|f| {
                let value = f(key);
                self.init_cache(key.to_vec(), QT::from_optional_value_to_query(value.clone()));
                Some(value)
            })
                .unwrap_or(None)
        }
    }

    fn get_change(&self, space: &[u8], key: &[u8]) -> Option<QT::Qry> {
        if space != &self.space { return None; }
        match self.changes.get(key) {
            Some(entry) => if entry.value_ref().muted() {
                Some(entry.value_ref().clone_inner())
            } else {
                None
            },
            None => None,
        }
    }

    fn get_ref<F>(&mut self, space: &[u8], key: &[u8], init: Option<F>) -> Option<RcT<QT::Qry>>
    where
        F: Fn(&[u8]) -> Option<V>
    {
        if space != &self.space { return None; }
        if let Some(x) = self.changes.get(key) {
            Some(x.value_ref().clone_ref())
        } else {
            init.map(|f| {
                let value = f(key);
                self.init_cache(key.to_vec(), QT::from_optional_value_to_query(value.clone()));
                self.changes.get(key).map(|e| e.value_ref().clone_ref())
            })
                .unwrap_or(None)
        }
    }

    fn get_change_ref(&self, space: &[u8], key: &[u8]) -> Option<RcT<QT::Qry>> {
        if space != &self.space { return None; }
        match self.changes.get(key) {
            Some(entry) => if entry.value_ref().muted() {
                Some(entry.value_ref().clone_ref())
            } else {
                None
            },
            None => None,
        }
    }

    fn take(&mut self, space: &[u8], key: &[u8]) -> Option<QT::Qry> {
        if space != &self.space { return None; }
        self.changes
            .get_mut(key)
            .map(|entry| {
                let mut take_value = QT::from_optional_value_to_query(None);
                entry
                    .value_mut()
                    .mutate(|v| std::mem::swap(v, &mut take_value));
                take_value
            })
    }

    fn pop_ref(&mut self, space: &[u8], key: &[u8]) -> Option<RcT<QT::Qry>> {
        if space != &self.space { return None; }
        if let Some(mut entry) = self.changes.remove(key) {
            let rct = entry.pop_value();
            if !entry.empty() {
                self.changes.insert(key.to_vec(), entry);
            }
            Some(rct)
        } else {
            None
        }
    }

    fn kill(&mut self, space: &[u8], key: &[u8]) {
        if space != &self.space { return; }
        self.set(key.to_vec(), QT::from_optional_value_to_query(None), None);
    }

    fn mutate<F, R, E, M>(&mut self, space: &[u8], key: &[u8], init: Option<F>, mutate: M) -> Option<Result<R, E>>
    where
        F: FnOnce() -> Option<V>,
        M: FnOnce(&mut QT::Qry) -> Result<R, E>,
    {
        if space != &self.space { return None; }
        // modify must have mutable reference of value returned
        self.modify(key.to_vec(), init.map(|f| || QT::from_optional_value_to_query(f())), None)
            .map(|v| v.mutate(|t| mutate(t)))
    }

    fn append<F, Item>(&mut self, space: &[u8], key: &[u8], init: F, item: Item) -> bool
    where
        F: FnOnce() -> V,
        V: TypedAppend<Item>,
    {
        if space != &self.space { return false; }
        // modify must have mutable reference of value returned
        self.modify_append(key.to_vec(), || QT::from_optional_value_to_query(Some(init())), None)
            .map(|v| v.mutate(|q| QT::append_query::<QT, Item>(q, item)))
            .expect("append value should have initialized");
        true
    }

    fn init(&mut self, space: &[u8], key: &[u8], value: Option<V>) -> bool {
        if space != &self.space { false; }
        self.init_cache(key.to_vec(), QT::from_optional_value_to_query(value))
    }
    
    fn cached(&self, space: &[u8], key: &[u8]) -> bool {
        if space != &self.space { return false; }
        self.changes
            .get(key)
            .map(|_| true)
            .unwrap_or(false)
    }
}

#[test]
fn test_basic_types() {
    use crate::OptionQT;

    let mut overlay1 = StorageOverlay::<OptionQT, _, _>::new(b"str", 0, 0);
    let mut overlay2 = StorageOverlay::<OptionQT, _, _>::new(b"str", 0, 0);

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
    use crate::OptionQT;

    let mut none_f = Some(|_: &_| { None });
    none_f.take();
    let mut overlay = StorageOverlay::<OptionQT, _, _>::new(b"str", 0, 0);
    overlay.start_transaction();
    overlay.put(b"str", b"key1", b"value1".to_vec());
    let _ = overlay.rollback_transaction(&ExecutionMode::Client);
    overlay.put(b"str", b"key2", b"value2".to_vec());
    assert_eq!(overlay.get(b"str", b"key1", none_f), Option::<Option<Vec<u8>>>::None);
    assert_eq!(overlay.get(b"str", b"key2", none_f), Some(Some(b"value2".to_vec())));
}

#[test]
fn test_clone_for_multi_threads() {
    use crate::OptionQT;

    let mut none_f = Some(|_: &_| { None });
    none_f.take();
    let mut overlay1 = StorageOverlay::<OptionQT, _, _>::new(b"str", 0, 0);
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
    use crate::OptionQT;

    let mut none_f = Some(|_: &_| { None });
    none_f.take();
    let mut overlay = StorageOverlay::<OptionQT, _, _>::new(b"str", 0, 0);
    overlay.init(b"str", b"key1", Some(b"value1".to_vec()));
    assert_eq!(overlay.get(b"str", b"key1", none_f), Some(Some(b"value1".to_vec())));
}

#[test]
fn test_storage_copy_data() {
    use crate::OptionQT;

    let mut none_f = Some(|_: &_| { None });
    none_f.take();
    let mut overlay = StorageOverlay::<OptionQT, _, _>::new(b"str", 0, 0);
    overlay.init(b"str", b"key1", Some(b"value1".to_vec()));
    assert_eq!(overlay.get(b"str", b"key1", none_f), Some(Some(b"value1".to_vec())));
    overlay.start_transaction();
    overlay.put(b"str", b"key1", b"value2".to_vec());
    assert_eq!(overlay.get(b"str", b"key1", none_f), Some(Some(b"value2".to_vec())));

    let mut overlay1 = overlay.copy_data();

    let _ = overlay.rollback_transaction(&ExecutionMode::Client);
    assert_eq!(overlay.get(b"str", b"key1", none_f), Some(Some(b"value1".to_vec())));

    assert_eq!(
        overlay1.downcast_mut::<StorageOverlay<OptionQT, Vec<u8>, Vec<u8>>>()
            .unwrap()
            .get(b"str", b"key1", none_f),
        Some(Some(b"value2".to_vec()))
    );
    overlay1.rollback_transaction(&ExecutionMode::Client);
    assert_eq!(
        overlay1
            .downcast_mut::<StorageOverlay<OptionQT, Vec<u8>, Vec<u8>>>()
            .unwrap()
            .get(b"str", b"key1", none_f),
        Some(Some(b"value1".to_vec()))
    );
}
