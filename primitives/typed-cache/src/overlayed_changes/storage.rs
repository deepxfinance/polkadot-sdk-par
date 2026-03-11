use sp_std::collections::btree_map::BTreeMap;
use sp_std::vec::Vec;
use crate::{OverlayedEntry, QueryTransfer, RcT, TStorageOverlay};
use crate::changeset::OverlayedMap;
pub use crate::StorageValue;

pub type StorageKey = Vec<u8>;

/// Change set for basic key value with extrinsics index recording and removal support.
pub type OverlayedChangeSet = OverlayedMap<StorageKey, Option<StorageValue>>;

pub type OverlayCache = OverlayedChangeSet;

/// History of value, with removal support.
pub type OverlayedValue = OverlayedEntry<Option<StorageValue>>;

impl OverlayedChangeSet {
    pub fn get_raw_changes(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        match self.changes.get(key) {
            Some(entry) => {
                let value = entry.value_ref().as_ref().expect("MustSome");
                if value.muted() {
                    Some(value.get_raw(false))
                } else {
                    None
                }
            },
            None => None,
        }
    }

    pub fn get_changes_keys(&self) -> Vec<StorageKey> {
        self.changes
            .iter()
            .filter_map(|(k , v)|
                if v.value_ref().as_ref().expect("MustSome").muted() {
                    Some(k.clone())
                } else {
                    None
                }
            )
            .collect()
    }

    pub fn get_commited(&self) -> BTreeMap<StorageKey, Option<Vec<u8>>> {
        self.changes
            .iter()
            .filter_map(|(k ,v)| {
                let value = v.value_ref().as_ref().expect("MustSome");
                if value.muted() {
                    Some((k.clone(), value.get_raw(false)))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn drain_commited(mut self) -> impl Iterator<Item = (StorageKey,  Option<StorageValue>)> {
        self.drain_changes()
            .into_iter()
            .filter_map(|(k, mut v)|
                if v.value_ref().as_ref().expect("MustSome").muted() {
                    Some((k, v.pop_value()))
                } else {
                    None
                }
            )
            .map(|(k, v)| (k, v))
    }

    pub fn get_raw(&self, key: &[u8], cache: bool) -> Option<Vec<u8>> {
        self.changes.get(key).map(|v| v.value_ref()
            .as_ref().expect("MustSome").get_raw(cache)
        )?
    }

    pub fn put_raw(&mut self, key: &[u8], raw: Vec<u8>) {
        let first_write_in_tx = self.first_write_in_tx(&key.to_vec());
        if let Some(overlayed) = self.changes.get_mut(key) {
            if first_write_in_tx || overlayed.transactions.is_empty() {
                overlayed.transactions.push(Some(StorageValue::new_raw(Some(raw), true)))
            } else {
                overlayed.value_mut().as_mut().expect("MustSome").put_raw(raw, true)
                    .expect("Invalid RawData")
            }
        } else {
            let mut overlayed: OverlayedEntry<Option<StorageValue>> = Default::default();
            overlayed.transactions.push(Some(StorageValue::new_raw(Some(raw), true)));
            self.changes.insert(key.to_vec(), overlayed);
        }
    }

    pub fn take_raw(&mut self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.changes
            .get_mut(key)
            .map(|entry| entry.value_mut()
                .as_mut()
                .expect("MustSome")
                .take_raw()
            )
    }

    /// Set all values to deleted which are matched by the predicate.
    ///
    /// Can be rolled back or committed when called inside a transaction.
    pub fn clear_where(
        &mut self,
        predicate: impl Fn(&[u8], &OverlayedValue) -> bool,
        _at_extrinsic: Option<u32>,
    ) -> u32 {
        let mut count = 0;
        for (_, val) in self.changes.iter_mut()
            .filter(|(k, v)| predicate(k, v))
        {
            if val.value_mut().as_mut().expect("MustSome").kill() {
                count += 1;
            }
        }
        count
    }

    /// Get the iterator over all changes that follow the supplied `key`.
    pub fn changes_after(&self, key: &[u8]) -> impl Iterator<Item = (&[u8], &OverlayedEntry<Option<StorageValue>>)> {
        use sp_std::ops::Bound;
        let range = (Bound::Excluded(key), Bound::Unbounded);
        self.changes.range::<[u8], _>(range).map(|(k, v)| (k.as_slice(), v))
    }
}

/// Inner type level operation.
impl OverlayedChangeSet {
    pub fn cached(&self, key: &[u8]) -> bool {
        self.changes
            .get(key)
            .map(|_| true)
            .unwrap_or(false)
    }

    pub fn contains(&self, key: &[u8]) -> Option<bool> {
        self.changes
            .get(key)
            .map(|x| x.value_ref().as_ref().expect("MustSome").exists())
    }

    pub fn put_t<T: TStorageOverlay>(&mut self, key: &[u8], value: T) {
        let first_write_in_tx = self.first_write_in_tx(&key.to_vec());
        if let Some(overlayed) = self.changes.get_mut(key) {
            if first_write_in_tx || overlayed.transactions.is_empty() {
                overlayed.transactions.push(Some(StorageValue::new_rct(Some(value), true)))
            } else {
                overlayed.value_mut().as_mut().expect("MustSome").downcast_mut::<T>()
                    .mutate(|v| *v = Some(value));
            }
        } else {
            let mut overlayed: OverlayedEntry<Option<StorageValue>> = Default::default();
            overlayed.transactions.push(Some(StorageValue::new_rct(Some(value), true)));
            self.changes.insert(key.to_vec(), overlayed);
        }
    }

    pub fn get_t<T: TStorageOverlay>(&self, key: &[u8]) -> Option<Option<T>> {
        self.changes.get(key).map(|x|
            x.value_ref().as_ref().expect("MustSome").get_t()
        )
    }

    pub fn get_muted_t<T: TStorageOverlay>(&self, key: &[u8]) -> Option<Option<T>> {
        self.changes
            .get(key)
            .map(|entry| entry.value_ref().as_ref()
                .expect("MustSome").get_muted_t()
            )
    }

    pub fn get_ref<T: TStorageOverlay>(&mut self, key: &[u8]) -> Option<RcT<T>> {
        self.changes.get_mut(key)
            .map(|entry| entry.value_mut().as_mut()
                .expect("MustSome")
                .get_ref()
            )
    }

    pub fn get_change_ref<T: TStorageOverlay>(&mut self, key: &[u8]) -> Option<RcT<T>> {
        self.changes.get_mut(key)
            .map(|entry| entry.value_mut().as_mut()
                .expect("MustSome")
                .get_muted_ref()
            )?
    }

    pub fn take<T: TStorageOverlay>(&mut self, key: &[u8]) -> Option<Option<T>> {
        self.changes
            .get_mut(key)
            .map(|entry| entry.value_mut()
                .as_mut()
                .expect("MustSome")
                .take_t()
            )
    }

    pub fn pop_ref<T: TStorageOverlay>(&mut self, key: &[u8]) -> Option<RcT<T>> {
        if let Some(mut entry) = self.changes.remove(key) {
            let any_rc = entry.pop_value();
            if !entry.empty() {
                self.changes.insert(key.to_vec(), entry);
            }
            any_rc.map(|value| value.downcast())
        } else {
            None
        }
    }

    pub fn kill(&mut self, key: &[u8]) {
        let first_write_in_tx = self.first_write_in_tx(&key.to_vec());
        if let Some(overlayed) = self.changes.get_mut(key) {
            if first_write_in_tx || overlayed.transactions.is_empty() {
                overlayed.transactions.push(Some(StorageValue::new_raw(None, true)))
            } else {
                overlayed.value_mut().as_mut().map(|value| value.kill());
            }
        } else {
            let mut overlayed: OverlayedEntry<Option<StorageValue>> = Default::default();
            overlayed.transactions.push(Some(StorageValue::new_raw(None, true)));
            self.changes.insert(key.to_vec(), overlayed);
        }
    }

    pub fn mutate<QT: QueryTransfer<T>, T: TStorageOverlay, F, R, E, M>(&mut self, key: &[u8], init: Option<F>, mutate: M) -> Option<Result<R, E>>
    where
        F: FnOnce() -> Option<T>,
        M: FnOnce(&mut QT::Query) -> Result<R, E>,
    {
        // modify must have mutable reference of value returned
        let modify_init = init.map(|init| || { Some(StorageValue::new_rct(init(), false)) })?;
        let v =  self.modify(key.to_vec(), modify_init, None);
        let rct = v.as_mut().unwrap().downcast_mut::<T>();
        let (res, new_value) = rct.mutate(|t| QT::mut_from_optional_value_to_query(t, mutate));
        if let Some(new_value) = new_value {
            rct.mutate(|t| *t = Some(new_value));
        }
        Some(res)
    }

    fn init<T: TStorageOverlay>(&mut self, key: &[u8], value: Option<T>) -> bool {
        self.init_cache(key.to_vec(), Some(StorageValue::new_rct(value, false)))
    }
}

#[test]
fn test_basic_types() {
    let mut overlay1 = OverlayedMap::new();
    let mut overlay2 = OverlayedMap::new();

    overlay1.put_t(b"key1", b"value1".to_vec());
    overlay1.put_t(b"key2", b"value2".to_vec());
    overlay2.put_t(b"key3", b"value3".to_vec());
    assert_eq!(overlay1.get_t(b"key1"), Some(Some(b"value1".to_vec())));
    assert_eq!(overlay1.get_t(b"key2"), Some(Some(b"value2".to_vec())));
    assert_eq!(overlay2.get_t(b"key3"), Some(Some(b"value3".to_vec())));
}

#[test]
fn test_commit() {
    let mut overlay = OverlayedMap::new();
    overlay.start_transaction();
    overlay.put_t(b"key1", b"value1".to_vec());
    overlay.rollback_transaction().unwrap();
    overlay.put_t(b"key2", b"value2".to_vec());
    assert_eq!(overlay.get_t(b"key1"), Option::<Option<Vec<u8>>>::None);
    assert_eq!(overlay.get_t(b"key2"), Some(Some(b"value2".to_vec())));
}

#[test]
fn test_cache() {
    let mut overlay = OverlayedMap::new();
    overlay.init(b"key1", Some(b"value1".to_vec()));
    assert_eq!(overlay.get_t(b"key1"), Some(Some(b"value1".to_vec())));
}

#[test]
fn test_storage_copy_data() {
    let mut overlay = OverlayedMap::new();
    overlay.init(b"key1", Some(b"value1".to_vec()));
    assert_eq!(overlay.get_t(b"key1"), Some(Some(b"value1".to_vec())));
    overlay.start_transaction();
    overlay.put_t(b"key1", b"value2".to_vec());
    assert_eq!(overlay.get_t(b"key1"), Some(Some(b"value2".to_vec())));

    let mut overlay1 = overlay.clone();

    overlay.rollback_transaction().unwrap();
    assert_eq!(overlay.get_t(b"key1"), Some(Some(b"value1".to_vec())));

    assert_eq!(overlay1.get_t(b"key1"), Some(Some(b"value2".to_vec())));
    overlay1.rollback_transaction().unwrap();
    assert_eq!(overlay1.get_t(b"key1"), Some(Some(b"value1".to_vec())));
}

#[test]
fn test_speed() {
    let mut overlay = OverlayedMap::new();
    let loop_times = 100000u32;
    let key_v: Vec<_> = (1..loop_times).into_iter().map(|i| (format!("key{i}"), format!("value{i}").as_bytes().to_vec())).collect();
    let start = std::time::SystemTime::now();
    for (key, value) in &key_v {
        overlay.put_t::<Vec<u8>>(key.as_bytes(), value.clone());
    }
    let put_time = start.elapsed().unwrap();
    for (key, _) in &key_v {
        overlay.get_t::<Vec<u8>>(key.as_bytes());
    }
    let get_time = start.elapsed().unwrap().saturating_sub(put_time);
    println!("loop times {loop_times}, put {put_time:?} get {get_time:?}");
}
