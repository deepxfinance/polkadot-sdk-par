#[cfg(not(feature = "std"))]
use alloc::collections::BTreeMap;
#[cfg(feature = "std")]
use std::collections::BTreeMap;
use hash_db::{HashDB, HashDBRef, Hasher};
use log::trace;
use crate::{DBValue, KVCache, KVMut, rstd::vec::Vec, STORAGE_HASH};

const NULL_DATA: [u8; 0] = [];

pub struct KVDBMut<'a, 'cache, H: Hasher> {
    db: &'a mut dyn HashDB<H, DBValue>,
    hash: &'a H::Out,
    /// Decide if all date work direct to db.
    direct: bool,
    storage: BTreeMap<(Vec<u8>, Option<u8>), DBValue>,
    cache: Option<core::cell::RefCell<&'cache mut dyn KVCache<H>>>,
    direct_changes: Vec<u8>,
    #[cfg(all(feature = "std", feature = "dev-time"))]
    time: std::time::Duration,
}

impl <'db, 'cache, H: Hasher> KVDBMut<'db, 'cache, H> {
    pub fn new(
        db: &'db mut dyn HashDB<H, DBValue>,
        hash: &'db H::Out,
        cache: Option<&'cache mut dyn KVCache<H>>,
        direct: bool,
    ) -> Self {
        Self {
            hash,
            db,
            direct,
            storage: Default::default(),
            cache: cache.map(core::cell::RefCell::new),
            direct_changes: Vec::with_capacity(1024 * 1024 * 5),
            #[cfg(all(feature = "std", feature = "dev-time"))]
            time: Default::default(),
        }
    }

    /// Get the backing database.
    pub fn db(&'db self) -> &'db dyn HashDB<H, DBValue> {
        self.db
    }

    /// Get the backing database mutably.
    pub fn db_mut(&mut self) -> &mut dyn HashDB<H, DBValue> {
        self.db
    }

    pub fn get_data(&self, key: &[u8], update_cache: bool) -> Option<DBValue> {
        if self.direct {
            return self.db_get(key, None, update_cache);
        }
        match self.storage.get(&(key.to_vec(), None)).cloned() {
            Some(value) => Some(value),
            None => self.db_get(key, None, update_cache)
        }
    }

    /// Insert a key-value pair.
    fn insert(
        &mut self,
        key: &[u8],
        mut value: DBValue,
    ) {
        #[cfg(all(feature = "std", feature = "dev-time"))]
        let start = std::time::Instant::now();
        if !value.muted() { return; }
        if self.direct {
            self.db_insert(key, None, value);
        } else {
            self.storage.insert((key.to_vec(), None), value);
        }
        if extend_storage_hash(key) {
            if self.direct {
                self.db_insert(key, Some(STORAGE_HASH), self.hash.as_ref().into());
            } else {
                self.storage.insert((key.to_vec(), Some(STORAGE_HASH)), self.hash.as_ref().into());
            }
        }
        #[cfg(all(feature = "std", feature = "dev-time"))]
        { self.time += start.elapsed(); }
    }

    fn remove(&mut self, key: &[u8]) -> Option<DBValue> {
        #[cfg(all(feature = "std", feature = "dev-time"))]
        let start = std::time::Instant::now();
        let removed = if self.direct {
            self.db_remove(key, None)
        } else {
            self.storage.insert((key.to_vec(), None), NULL_DATA.as_slice().into())
        };
        if extend_storage_hash(key) {
            if self.direct {
                self.db_remove(key, Some(STORAGE_HASH));
            } else {
                self.storage.insert((key.to_vec(), Some(STORAGE_HASH)), NULL_DATA.as_slice().into());
            }
        }
        #[cfg(all(feature = "std", feature = "dev-time"))]
        { self.time += start.elapsed(); }
        removed
    }

    fn db_insert(&mut self, key: &[u8], pad: Option<u8>, value: DBValue) {
        let key_hash = H::hash(key);
        self.cache.as_mut().map(|c| (*c.borrow_mut()).cache_value_for_key(
            &memory_db::prefixed_key::<H>(&key_hash, (key, pad)), value.clone())
        );
        self.db.emplace(key_hash, (key, pad), value.clone());
        #[cfg(not(feature = "async-root"))]
        self.direct_changes.extend([key, &pad_encode(&pad), &value.get_raw(true).expect("SHOULD BE SOME")].concat());
    }

    /// Remove some value from db and cache.
    /// Notice!!! Do not support return removed value.
    fn db_remove(&mut self, key: &[u8], pad: Option<u8>) -> Option<DBValue> {
        let key_hash = H::hash(key);
        self.cache.as_mut().map(|c| (*c.borrow_mut()).remove_value_for_key(
            &memory_db::prefixed_key::<H>(&key_hash, (key, pad)))
        );
        self.db.remove(&key_hash, (key, pad));
        #[cfg(not(feature = "async-root"))]
        self.direct_changes.extend([key, &pad_encode(&pad), &NULL_DATA].concat());
        None
    }

    fn db_get(&self, key: &[u8], pad: Option<u8>, update_cache: bool) -> Option<DBValue> {
        if let Some(mut cache) = self.cache.as_ref().map(|c| c.borrow_mut()) {
            let cache_key = memory_db::prefixed_key::<H>(&H::hash(key), (key, pad));
            let cache_result = (*cache).lookup_value_for_key(&cache_key);
            match cache_result {
                Some(value) => Some(value.clone()),
                None => match self.db.get(&H::hash(key), (key, pad)) {
                    Some(value) => {
                        if update_cache {
                            (*cache).cache_value_for_key(&cache_key, value.clone());
                        }
                        Some(value)
                    }
                    None => None
                }
            }
        } else {
            self.db.get(&H::hash(key), (key, pad))
        }
    }

    /// Commit the in-memory changes to disk, freeing their storage.
    pub fn commit(&mut self) -> H::Out {
        #[cfg(feature = "std")]
        let start = std::time::Instant::now();
        #[cfg(feature = "std")]
        trace!(target: "kvdb", "Committing kv changes to db.");
        let changes = if self.direct {
            core::mem::take(&mut self.direct_changes)
        } else {
            let mut changes = Vec::new();
            for (prefix, value) in core::mem::take(&mut self.storage).into_iter() {
                #[cfg(not(feature = "async-root"))]
                changes.extend([prefix.0.as_slice(), &pad_encode(&prefix.1), &value.get_raw(false).unwrap_or_default()].concat());
                let key_hash = H::hash(prefix.0.as_slice());
                if !value.exists() {
                    self.db.remove(&key_hash, (prefix.0.as_slice(), prefix.1));
                    self.cache.as_mut().map(|c| (*c.borrow_mut()).remove_value_for_key(
                        &memory_db::prefixed_key::<H>(&key_hash, (prefix.0.as_slice(), prefix.1))
                    ));
                } else {
                    self.db.emplace(key_hash, (prefix.0.as_slice(), prefix.1), value.clone());
                    self.cache.as_mut().map(|c| (*c.borrow_mut()).cache_value_for_key(
                        &memory_db::prefixed_key::<H>(&key_hash, (prefix.0.as_slice(), prefix.1)),
                        value,
                    ));
                }
            }
            changes
        };
        #[cfg(feature = "std")]
        let db_handle_time = start.elapsed();
        #[cfg(feature = "std")]
        let changes_len = changes.len();
        #[cfg(feature = "async-root")]
        let changes_root = H::Out::default();
        #[cfg(not(feature = "async-root"))]
        let changes_root = H::hash(&changes);
        // extra storage for state_root, which will be used with this special prefix.
        #[cfg(not(feature = "async-root"))]
        self.db.emplace(changes_root, (&[], None), changes_root.as_ref().to_vec().into());
        #[cfg(feature = "std")]
        let hash_time = start.elapsed();
        #[cfg(all(feature = "std", not(feature = "dev-time")))]
        log::debug!(target: "kvdb", "KVDB mut Commit direct {} {changes_len} {db_handle_time:?}/{hash_time:?}", self.direct);
        #[cfg(all(feature = "std", feature = "dev-time"))]
        log::debug!(target: "kvdb", "KVDB mut Commit direct {} {changes_len} IO {:?} {db_handle_time:?}/{hash_time:?} total {:?}", self.direct, self.time, self.time + hash_time);
        changes_root
    }
}

impl<'db, 'cache, H: Hasher> KVMut<'db, H> for KVDBMut<'db, 'cache, H> {
    fn get<'a, 'key>(&'a self, key: &'key [u8]) -> Option<DBValue>
    where
        'a: 'key
    {
        self.get_data(key, true).map(|v| v.clone())
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.get(key).map(|v| v.exists()).unwrap_or(false)
    }

    fn insert(&mut self, key: &[u8], value: DBValue) {
        self.insert(key, value)
    }

    fn remove(&mut self, key: &[u8]) -> Option<DBValue> {
        self.remove(key)
    }
}

/// Return special storage hash for some well_known_key(e.g. CODE)
/// This extended storage_hash should be stored at prefix `(key, Some(STORAGE_HASH))`.
pub fn extend_storage_hash(key: &[u8]) -> bool {
    if key == b":code" {
        return true
    }
    false
}

pub fn key_pad(key: &[u8]) -> Option<u8> {
    if key == b":code" {
        Some(STORAGE_HASH)
    } else {
        None
    }
}

pub fn cache_key<'a>(key: &'a [u8], pad: &'a Option<u8>) -> Vec<u8> {
    if let Some(pad) = pad.as_ref() {
        [key.to_vec(), vec![*pad]].concat()
    } else {
        key.to_vec()
    }
}

fn pad_encode(pad: &Option<u8>) -> Vec<u8> {
    match pad {
        Some(pad) => [*pad].to_vec(),
        None => [].to_vec(),
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use hash_db::Hasher;
    use memory_db::prefixed_key;
    use sp_core::Blake2Hasher;
    use typed_cache::StorageValue;
    use crate::{DBValue, KVDBMut};

    type PrefixedMemoryDB = crate::MemoryDB<Blake2Hasher, memory_db::PrefixedKey<Blake2Hasher>, DBValue>;
    type MemoryDB = crate::MemoryDB<Blake2Hasher, crate::HashKey<Blake2Hasher>, DBValue>;

    fn collect_changes(insert: usize, remove: usize, size: usize) -> BTreeMap<Vec<u8>, Option<DBValue>> {
        let mut changes = BTreeMap::new();
        for i in 0..insert {
            changes.insert(i.to_le_bytes().to_vec(), Some(vec![(i / 256) as u8; size].into()));
        }
        for r in insert..(remove + insert) {
            changes.insert(r.to_le_bytes().to_vec(), None);
        }
        changes
    }

    fn process<H: Hasher>(db: &mut KVDBMut<H>, changes: &BTreeMap<Vec<u8>, Option<DBValue>>) {
        for (k, v) in changes {
            if let Some(v) = v {
                db.insert(k, v.clone());
            } else {
                db.remove(k);
            }
        }
    }

    fn check_prefix_db_drain<H: Hasher>(db: &mut PrefixedMemoryDB, changes: &BTreeMap<Vec<u8>, Option<DBValue>>) {
        let mut drain = db.drain();
        for (k, v) in changes {
            let db_key = prefixed_key::<H>(&H::hash(k), (k, None));
            if let Some(v) = v {
                assert_eq!(drain.remove(&db_key), Some((v.clone(), 1i32)));
            } else {
                assert_eq!(drain.remove(&db_key), Some((Default::default(), -1i32)));
            }
        }
        assert!(drain.len() <= 1);
    }

    fn check_equal<H: Hasher>(db1: &KVDBMut<H>, db2: &KVDBMut<H>, insert: usize, remove: usize) {
        use crate::KVMut;

        for k in (0..remove + insert).map(|r| r.to_le_bytes().to_vec()) {
            assert_eq!(db1.get(&k), db2.get(&k));
        }
    }

    #[test]
    fn test_direct_or_not() {
        let _ = env_logger::builder().filter_level(log::LevelFilter::Info).is_test(true).try_init();

        let hash = Blake2Hasher::hash(&[1u8]);
        for total in [10usize, 100, 1000, 10000, 20000, 40000, 60000, 80000, 100000] {
            let insert = total  * 8 / 10;
            let remove  = total - insert;
            let mut memory_db1 = PrefixedMemoryDB::default();
            let mut memory_db2 = PrefixedMemoryDB::default();
            let mut direct_db = KVDBMut::new(&mut memory_db1, &hash, None, true);
            let mut no_dir_db = KVDBMut::new(&mut memory_db2, &hash, None, false);
            let changes = collect_changes(insert, remove, 100);
            #[cfg(all(feature = "std", not(feature = "dev-time")))]
            let start = std::time::Instant::now();
            process(&mut direct_db, &changes);
            #[cfg(all(feature = "std", not(feature = "dev-time")))]
            let direct_time = start.elapsed();
            #[cfg(all(feature = "std", not(feature = "dev-time")))]
            let start = std::time::Instant::now();
            process(&mut no_dir_db, &changes);
            #[cfg(all(feature = "std", not(feature = "dev-time")))]
            let no_dir_time = start.elapsed();
            #[cfg(all(feature = "std", not(feature = "dev-time")))]
            println!("direct/no_dir time {direct_time:?}/{no_dir_time:?}");
            check_equal(&direct_db, &no_dir_db, insert, remove);
            assert_eq!(direct_db.commit(), no_dir_db.commit());
            println!();
        }
    }

    #[test]
    fn test_prefixed_key_speed() {
        let _ = env_logger::builder().filter_level(log::LevelFilter::Info).is_test(true).try_init();

        let hash = Blake2Hasher::hash(&[1u8]);
        for total in [10usize, 100, 1000, 10000, 20000, 40000, 60000, 80000, 100000] {
            let insert = total  * 8 / 10;
            let remove  = total - insert;
            let mut memory_db1 = PrefixedMemoryDB::default();
            let mut memory_db2 = MemoryDB::default();
            let mut prefix_db = KVDBMut::new(&mut memory_db1, &hash, None, true);
            let mut no_pre_db = KVDBMut::new(&mut memory_db2, &hash, None, true);
            let changes = collect_changes(insert, remove, 100);
            #[cfg(all(feature = "std", not(feature = "dev-time")))]
            let start = std::time::Instant::now();
            process(&mut prefix_db, &changes);
            #[cfg(all(feature = "std", not(feature = "dev-time")))]
            let prefix_time = start.elapsed();
            #[cfg(all(feature = "std", not(feature = "dev-time")))]
            let start = std::time::Instant::now();
            process(&mut no_pre_db, &changes);
            #[cfg(all(feature = "std", not(feature = "dev-time")))]
            let no_pre_time = start.elapsed();
            #[cfg(all(feature = "std", not(feature = "dev-time")))]
            println!("prefix/no_pre time {prefix_time:?}/{no_pre_time:?}");
            check_equal(&prefix_db, &no_pre_db, insert, remove);
            assert_eq!(prefix_db.commit(), no_pre_db.commit());
            println!();
        }
    }

    #[test]
    fn drain_correctness() {
        let _ = env_logger::builder().filter_level(log::LevelFilter::Info).is_test(true).try_init();

        let hash = Blake2Hasher::hash(&[1u8]);
        for total in [10usize, 100, 1000, 10000, 20000, 40000, 60000, 80000, 100000] {
            let insert = total  * 8 / 10;
            let remove  = total - insert;
            let mut memory_db = PrefixedMemoryDB::default();
            let mut db = KVDBMut::new(&mut memory_db, &hash, None, true);
            let changes = collect_changes(insert, remove, 100);
            process(&mut db, &changes);
            check_prefix_db_drain::<Blake2Hasher>(&mut memory_db, &changes);
        }
    }

    #[test]
    fn test_btree_collect() {
        for total in [10usize, 100, 1000, 10000, 20000, 40000, 60000, 80000, 100000] {
            let insert = total  * 8 / 10;
            let remove  = total - insert;
            let changes = collect_changes(insert, remove, 100);

            let mut insert_btree = BTreeMap::new();
            let start = std::time::Instant::now();
            for (k, v) in changes.iter() {
                let db_key = prefixed_key::<Blake2Hasher>(&Blake2Hasher::hash(k), (k, None));
                if let Some(v) = v {
                    insert_btree.insert(db_key, (v.clone(), 1));
                } else {
                    insert_btree.insert(db_key, (Default::default(), -1));
                }
            }
            let insert_time = start.elapsed();

            let start = std::time::Instant::now();
            let ordered_tree = changes.iter().map(|(k, v)| (
                prefixed_key::<Blake2Hasher>(&Blake2Hasher::hash(k), (k, None)),
                match v {
                    Some(v) => (v.clone(), 1),
                    None => (Default::default(), -1),
                }
            ))
                .collect::<BTreeMap<_, _>>();
            let ordered_time = start.elapsed();

            assert_eq!(insert_btree, ordered_tree);
            println!("total {total} insert_time/ordered_time: {insert_time:?} {ordered_time:?}");
        }
    }
}
