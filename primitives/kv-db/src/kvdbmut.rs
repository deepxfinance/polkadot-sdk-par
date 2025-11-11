#[cfg(not(feature = "std"))]
use alloc::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "std")]
use std::collections::{BTreeMap, BTreeSet};
use hash_db::{HashDB, HashDBRef, Hasher};
use log::trace;
use crate::{DBValue, KVCache, KVMut, rstd::vec::Vec};

pub struct KVDBMut<'a, 'cache, H: Hasher> {
    db: &'a mut dyn HashDB<H, DBValue>,
    hash: &'a H::Out,
    storage: BTreeMap<(Vec<u8>, Option<u8>), DBValue>,
    death_row: BTreeSet<(Vec<u8>, Option<u8>)>,
    cache: Option<core::cell::RefCell<&'cache mut dyn KVCache<H>>>,
}

impl <'db, 'cache, H: Hasher> KVDBMut<'db, 'cache, H> {
    pub fn new(
        db: &'db mut dyn HashDB<H, DBValue>,
        hash: &'db H::Out,
        cache: Option<&'cache mut dyn KVCache<H>>,
    ) -> Self {
        Self {
            hash,
            db,
            storage: Default::default(),
            death_row: Default::default(),
            cache: cache.map(core::cell::RefCell::new),
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

    pub fn get_data(&self, key: &[u8]) -> Option<DBValue> {
        match self.storage.get(&(key.to_vec(), None)) {
            Some(value) => Some(value.to_vec()),
            None => if let Some(mut cache) = self.cache.as_ref().map(|c| c.borrow_mut()) {
                let cache_result = (*cache).lookup_value_for_key(H::hash(key), key);
                match cache_result {
                    Some(value) => Some(value.clone()),
                    None => match self.db.get(&H::hash(key), (key, None)) {
                        Some(value) => {
                            (*cache).cache_value_for_key(H::hash(key), key, value);
                            (*cache).lookup_value_for_key(H::hash(key), key).map(|v| v.to_vec())
                        }
                        None => None
                    }
                }
            } else {
                self.db.get(&H::hash(key), (key, None))
            }
        }
    }

    /// Insert a key-value pair.
    fn insert(
        &mut self,
        key: &[u8],
        mut value: DBValue,
    ) {
        self.death_row.remove(&(key.to_vec(), None));
        if value == [0u8].to_vec() {
            // to avoid null data `vec![0u8]`.
            // for encoded value, this should not exist. we refactor it to empty vec encode.
            value = [1u8].to_vec();
        }
        self.storage.insert((key.to_vec(), None), value);
    }

    fn remove(&mut self, key: &[u8]) -> Option<DBValue> {
        let removed = self.storage.remove(&(key.to_vec(), None));
        self.death_row.insert((key.to_vec(), None));
        removed
    }

    /// Commit the in-memory changes to disk, freeing their storage.
    pub fn commit(&mut self) -> H::Out {
        #[cfg(feature = "std")]
        trace!(target: "kvdb", "Committing kv changes to db.");

        // always kill all the nodes on death row.
        #[cfg(feature = "std")]
        trace!(target: "kvdb", "{:?} to remove from db", self.death_row.len());
        let mut changes = Vec::new();
        for prefix in self.death_row.iter() {
            let key_hash = H::hash(prefix.0.as_slice());
            self.db.remove(&key_hash, (prefix.0.as_slice(), prefix.1));
            self.cache.as_mut().map(|c| (*c.borrow_mut()).remove_value_for_key(key_hash, &prefix.0));
            changes.push([prefix.0.as_slice(), &[0u8]].concat());
        }
        // always kill all the nodes on death row.
        #[cfg(feature = "std")]
        trace!(target: "kvdb", "{:?} to insert db", self.storage.len());
        for (prefix, db_value) in self.storage.iter() {
            let key_hash = H::hash(prefix.0.as_slice());
            self.cache.as_mut().map(|c| (*c.borrow_mut()).cache_value_for_key(key_hash, &prefix.0, db_value.to_vec()));
            // TODO remove for decrease rc.
            self.db.remove(&key_hash, (prefix.0.as_slice(), prefix.1));
            self.db.emplace(key_hash, (prefix.0.as_slice(), prefix.1), Vec::new());
            self.db.emplace(key_hash, (prefix.0.as_slice(), prefix.1), db_value.clone());
            changes.push([prefix.0.as_slice(), db_value.as_slice()].concat());
        }
        let changes_root = H::hash(&changes.into_iter().flatten().collect::<Vec<u8>>());
        self.db.emplace(changes_root, (&[], None), changes_root.as_ref().to_vec());
        changes_root
    }
}

impl<'db, 'cache, H: Hasher> KVMut<'db, H> for KVDBMut<'db, 'cache, H> {
    fn get<'a, 'key>(&'a self, key: &'key [u8]) -> Option<DBValue>
    where
        'a: 'key
    {
        match self.get_data(key).map(|v| v.clone()) {
            Some(v) => if v == [0u8].to_vec() {
                None
            } else if v == [1u8].to_vec() {
                Some([0u8].to_vec())
            } else {
                Some(v)
            },
            None => None
        }
    }

    fn insert(&mut self, key: &[u8], value: DBValue) {
        self.insert(key, value)
    }

    fn remove(&mut self, key: &[u8]) -> Option<DBValue> {
        self.remove(key)
    }
}
