#[cfg(not(feature = "std"))]
extern crate alloc;

use hash_db::{
    AsHashDB, AsPlainDB, HashDB, HashDBRef, Hasher as KeyHasher, MaybeDebug, PlainDB, PlainDBRef,
    Prefix,
};
#[cfg(feature = "std")]
use std::{
    borrow::Borrow, cmp::Eq, collections::btree_map::Entry, collections::BTreeMap as Map,
    marker::PhantomData, mem,
};

pub use memory_db::{KeyFunction, PrefixedKey};

#[cfg(not(feature = "std"))]
use alloc::collections::btree_map::{BTreeMap as Map, Entry};

#[cfg(not(feature = "std"))]
use core::{borrow::Borrow, cmp::Eq, hash, marker::PhantomData, mem};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use typed_cache::StorageValue;
use crate::DBValue;

pub trait FastBuild<H: KeyHasher, KF: KeyFunction<H>, T> {
    fn set_ordered_data(&mut self, data: Map<KF::Key, (T, i32)>);
}

/// Key function that only uses the hash
pub struct HashKey<H>(PhantomData<H>);

impl<H> Clone for HashKey<H> {
    fn clone(&self) -> Self {
        Self(Default::default())
    }
}

impl<H> core::fmt::Debug for HashKey<H> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::write!(f, "HashKey")
    }
}

impl<H: KeyHasher> KeyFunction<H> for HashKey<H> {
    type Key = Vec<u8>;

    fn key(hash: &H::Out, prefix: Prefix) -> Vec<u8> {
        hash_key::<H>(hash, prefix)
    }
}

/// Make database key from hash only.
pub fn hash_key<H: KeyHasher>(key: &H::Out, _prefix: Prefix) -> Vec<u8> {
    key.as_ref().to_vec()
}

/// Special MemoryDb for kvdb.
///
/// All reference count will be `-1`, `0` or `1`, all `insert`/`remove`/`emplace`/`clear` will change
/// reference count directly.
pub struct MemoryDB<H, KF, T>
where
    H: KeyHasher,
    KF: KeyFunction<H>,
{
    data: Map<KF::Key, (T, i32)>,
    hashed_null_node: H::Out,
    _kf: PhantomData<KF>,
}

impl<H, KF, T> Clone for MemoryDB<H, KF, T>
where
    H: KeyHasher,
    KF: KeyFunction<H>,
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            hashed_null_node: self.hashed_null_node,
            _kf: Default::default(),
        }
    }
}

impl<H, KF, T> PartialEq<MemoryDB<H, KF, T>> for MemoryDB<H, KF, T>
where
    H: KeyHasher,
    KF: KeyFunction<H>,
    T: Eq + MaybeDebug,
{
    fn eq(&self, other: &MemoryDB<H, KF, T>) -> bool {
        for a in self.data.iter() {
            match other.data.get(a.0) {
                Some(v) if v != a.1 => return false,
                None => return false,
                _ => (),
            }
        }
        true
    }
}

impl<H, KF, T> Eq for MemoryDB<H, KF, T>
where
    H: KeyHasher,
    KF: KeyFunction<H>,
    T: Eq + MaybeDebug,
{
}

impl<H, KF, T> Default for MemoryDB<H, KF, T>
where
    H: KeyHasher,
    T: for<'a> From<&'a [u8]>,
    KF: KeyFunction<H>,
{
    fn default() -> Self {
        Self::from_null_node(&[0u8][..])
    }
}

impl<H, KF, T> MemoryDB<H, KF, T>
where
    H: KeyHasher,
    T: for<'a> From<&'a [u8]>,
    KF: KeyFunction<H>,
{
    /// Create a new `MemoryDB` from a given null key/data
    pub fn from_null_node(null_key: &[u8]) -> Self {
        MemoryDB {
            data: Map::default(),
            hashed_null_node: H::hash(null_key),
            _kf: Default::default(),
        }
    }

    /// Create a new instance of `Self`.
    pub fn new(data: &[u8]) -> Self {
        Self::from_null_node(data)
    }

    /// Create a new default instance of `Self` and returns `Self` and the root hash.
    pub fn default_with_root() -> (Self, H::Out) {
        let db = Self::default();
        let root = db.hashed_null_node;

        (db, root)
    }

    /// Clear all data from the database.
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Purge all zero-referenced data from the database.
    pub fn purge(&mut self) {}

    pub fn data(&self) -> &Map<KF::Key, (T, i32)> {
        &self.data
    }
}

impl<H, KF, T> MemoryDB<H, KF, T>
where
    H: KeyHasher,
    KF: KeyFunction<H>,
{
    /// Return the internal key-value Map, clearing the current state.
    pub fn drain(&mut self) -> Map<KF::Key, (T, i32)> {
        mem::take(&mut self.data)
    }

    /// Consolidate all the entries of `other` into `self`.
    pub fn consolidate(&mut self, mut other: Self) {
        self.data.extend(other.drain());
    }

    /// Get the keys in the database together with number of underlying references.
    pub fn keys(&self) -> Map<KF::Key, i32> {
        self.data
            .iter()
            .filter_map(|(k, v)| if v.1 != 0 { Some((k.clone(), v.1)) } else { None })
            .collect()
    }
}

impl<H, KF> PlainDB<H::Out, DBValue> for MemoryDB<H, KF, DBValue>
where
    H: KeyHasher,
    KF: Send + Sync + KeyFunction<H>,
    KF::Key: Borrow<[u8]> + for<'a> From<&'a [u8]>,
{
    fn get(&self, key: &H::Out) -> Option<DBValue> {
        match self.data.get(key.as_ref()) {
            Some(&(ref d, rc)) if rc > 0 => Some(d.clone()),
            _ => None,
        }
    }

    fn contains(&self, key: &H::Out) -> bool { 
        match self.data.get(key.as_ref()) {
            Some(&(_, x)) if x > 0 => true,
            _ => false,
        }
    }

    fn emplace(&mut self, key: H::Out, value: DBValue) {
        match self.data.entry(key.as_ref().into()) {
            Entry::Occupied(mut entry) => {
                let &mut (ref mut old_value, ref mut rc) = entry.get_mut();
                *old_value = value;
                *rc = 1;
            },
            Entry::Vacant(entry) => {
                entry.insert((value, 1));
            },
        }
    }

    fn remove(&mut self, key: &H::Out) {
        match self.data.entry(key.as_ref().into()) {
            Entry::Occupied(mut entry) => {
                let &mut (_, ref mut rc) = entry.get_mut();
                *rc = -1;
            },
            Entry::Vacant(entry) => {
                let value = DBValue::default();
                entry.insert((value, -1));
            },
        }
    }
}

impl<H, KF> PlainDBRef<H::Out, DBValue> for MemoryDB<H, KF, DBValue>
where
    H: KeyHasher,
    KF: Send + Sync + KeyFunction<H>,
    KF::Key: Borrow<[u8]> + for<'a> From<&'a [u8]>,
{
    fn get(&self, key: &H::Out) -> Option<DBValue> {
        PlainDB::get(self, key)
    }
    fn contains(&self, key: &H::Out) -> bool {
        PlainDB::contains(self, key)
    }
}

impl<H, KF> HashDB<H, DBValue> for MemoryDB<H, KF, DBValue>
where
    H: KeyHasher,
    KF: KeyFunction<H> + Send + Sync,
{
    fn get(&self, key: &H::Out, prefix: Prefix) -> Option<DBValue> {
        if key == &self.hashed_null_node {
            return None
        }

        let key = KF::key(key, prefix);
        match self.data.get(&key) {
            Some(&(ref d, rc)) if rc > 0 => Some(d.clone()),
            _ => None,
        }
    }

    fn contains(&self, key: &H::Out, prefix: Prefix) -> bool {
        if key == &self.hashed_null_node {
            return true
        }

        let key = KF::key(key, prefix);
        match self.data.get(&key) {
            Some(&(_, x)) if x > 0 => true,
            _ => false,
        }
    }

    fn emplace(&mut self, key: H::Out, prefix: Prefix, value: DBValue) {
        let key = KF::key(&key, prefix);
        match self.data.entry(key) {
            Entry::Occupied(mut entry) => {
                let &mut (ref mut old_value, ref mut rc) = entry.get_mut();
                *old_value = value;
                *rc = 1;
            },
            Entry::Vacant(entry) => {
                entry.insert((value, 1));
            },
        }
    }

    fn insert(&mut self, prefix: Prefix, value: &[u8]) -> H::Out {
        let key = H::hash(value);
        let mut value: DBValue = value.into();
        value.set_muted(true);
        <Self as HashDB<H, DBValue>>::emplace(self, key, prefix, value);
        key
    }

    fn remove(&mut self, key: &H::Out, prefix: Prefix) {
        if key == &self.hashed_null_node {
            return
        }

        let key = KF::key(key, prefix);
        match self.data.entry(key) {
            Entry::Occupied(mut entry) => {
                let &mut (_, ref mut rc) = entry.get_mut();
                *rc = -1;
            },
            Entry::Vacant(entry) => {
                let value = DBValue::default();
                entry.insert((value, -1));
            },
        }
    }
}

impl<H, KF> HashDBRef<H, DBValue> for MemoryDB<H, KF, DBValue>
where
    H: KeyHasher,
    KF: KeyFunction<H> + Send + Sync,
{
    fn get(&self, key: &H::Out, prefix: Prefix) -> Option<DBValue> {
        <Self as HashDB<H, DBValue>>::get(self, key, prefix)
    }
    fn contains(&self, key: &H::Out, prefix: Prefix) -> bool {
        <Self as HashDB<H, DBValue>>::contains(self, key, prefix)
    }
}

impl<H, KF> HashDBRef<H, Vec<u8>> for MemoryDB<H, KF, DBValue>
where
    H: KeyHasher,
    KF: KeyFunction<H> + Send + Sync,
{
    fn get(&self, key: &H::Out, prefix: Prefix) -> Option<Vec<u8>> {
        <Self as HashDB<H, Vec<u8>>>::get(self, key, prefix)
    }
    fn contains(&self, key: &H::Out, prefix: Prefix) -> bool {
        <Self as HashDB<H, Vec<u8>>>::contains(self, key, prefix)
    }
}

impl<H, KF> AsPlainDB<H::Out, DBValue> for MemoryDB<H, KF, DBValue>
where
    H: KeyHasher,
    KF: KeyFunction<H> + Send + Sync,
    KF::Key: Borrow<[u8]> + for<'a> From<&'a [u8]>,
{
    fn as_plain_db(&self) -> &dyn PlainDB<H::Out, DBValue> {
        self
    }
    fn as_plain_db_mut(&mut self) -> &mut dyn PlainDB<H::Out, DBValue> {
        self
    }
}

impl<H, KF> AsHashDB<H, DBValue> for MemoryDB<H, KF, DBValue>
where
    H: KeyHasher,
    KF: KeyFunction<H> + Send + Sync,
{
    fn as_hash_db(&self) -> &dyn HashDB<H, DBValue> {
        self
    }
    fn as_hash_db_mut(&mut self) -> &mut dyn HashDB<H, DBValue> {
        self
    }
}

impl<H, KF> PlainDB<H::Out, Vec<u8>> for MemoryDB<H, KF, Vec<u8>>
where
    H: KeyHasher,
    KF: Send + Sync + KeyFunction<H>,
    KF::Key: Borrow<[u8]> + for<'a> From<&'a [u8]>,
{
    fn get(&self, key: &H::Out) -> Option<Vec<u8>> {
        match self.data.get(key.as_ref()) {
            Some(&(ref d, rc)) if rc > 0 => Some(d.clone()),
            _ => None,
        }
    }

    fn contains(&self, key: &H::Out) -> bool {
        match self.data.get(key.as_ref()) {
            Some(&(_, x)) if x > 0 => true,
            _ => false,
        }
    }

    fn emplace(&mut self, key: H::Out, value: Vec<u8>) {
        match self.data.entry(key.as_ref().into()) {
            Entry::Occupied(mut entry) => {
                let &mut (ref mut old_value, ref mut rc) = entry.get_mut();
                *old_value = value;
                *rc = 1;
            },
            Entry::Vacant(entry) => {
                entry.insert((value, 1));
            },
        }
    }

    fn remove(&mut self, key: &H::Out) {
        match self.data.entry(key.as_ref().into()) {
            Entry::Occupied(mut entry) => {
                let &mut (_, ref mut rc) = entry.get_mut();
                *rc = -1;
            },
            Entry::Vacant(entry) => {
                let value = Default::default();
                entry.insert((value, -1));
            },
        }
    }
}

impl<H, KF> PlainDBRef<H::Out, Vec<u8>> for MemoryDB<H, KF, Vec<u8>>
where
    H: KeyHasher,
    KF: Send + Sync + KeyFunction<H>,
    KF::Key: Borrow<[u8]> + for<'a> From<&'a [u8]>,
{
    fn get(&self, key: &H::Out) -> Option<Vec<u8>> {
        PlainDB::get(self, key)
    }
    fn contains(&self, key: &H::Out) -> bool {
        PlainDB::contains(self, key)
    }
}

impl<H, KF> HashDB<H, Vec<u8>> for MemoryDB<H, KF, Vec<u8>>
where
    H: KeyHasher,
    KF: KeyFunction<H> + Send + Sync,
{
    fn get(&self, key: &H::Out, prefix: Prefix) -> Option<Vec<u8>> {
        if key == &self.hashed_null_node {
            return None
        }

        let key = KF::key(key, prefix);
        match self.data.get(&key) {
            Some(&(ref d, rc)) if rc > 0 => Some(d.clone()),
            _ => None,
        }
    }

    fn contains(&self, key: &H::Out, prefix: Prefix) -> bool {
        if key == &self.hashed_null_node {
            return true
        }

        let key = KF::key(key, prefix);
        match self.data.get(&key) {
            Some(&(_, x)) if x > 0 => true,
            _ => false,
        }
    }

    fn emplace(&mut self, key: H::Out, prefix: Prefix, value: Vec<u8>) {
        let key = KF::key(&key, prefix);
        match self.data.entry(key) {
            Entry::Occupied(mut entry) => {
                let &mut (ref mut old_value, ref mut rc) = entry.get_mut();
                *old_value = value;
                *rc = 1;
            },
            Entry::Vacant(entry) => {
                entry.insert((value, 1));
            },
        }
    }

    fn insert(&mut self, prefix: Prefix, value: &[u8]) -> H::Out {
        let key = H::hash(value);
        <Self as HashDB<H, Vec<u8>>>::emplace(self, key, prefix, value.into());
        key
    }

    fn remove(&mut self, key: &H::Out, prefix: Prefix) {
        if key == &self.hashed_null_node {
            return
        }

        let key = KF::key(key, prefix);
        match self.data.entry(key) {
            Entry::Occupied(mut entry) => {
                let &mut (_, ref mut rc) = entry.get_mut();
                *rc = -1;
            },
            Entry::Vacant(entry) => {
                let value = Default::default();
                entry.insert((value, -1));
            },
        }
    }
}

impl<H, KF> HashDBRef<H, Vec<u8>> for MemoryDB<H, KF, Vec<u8>>
where
    H: KeyHasher,
    KF: KeyFunction<H> + Send + Sync,
{
    fn get(&self, key: &H::Out, prefix: Prefix) -> Option<Vec<u8>> {
        <Self as HashDB<H, Vec<u8>>>::get(self, key, prefix)
    }
    fn contains(&self, key: &H::Out, prefix: Prefix) -> bool {
        <Self as HashDB<H, Vec<u8>>>::contains(self, key, prefix)
    }
}

impl<H, KF> AsPlainDB<H::Out, Vec<u8>> for MemoryDB<H, KF, Vec<u8>>
where
    H: KeyHasher,
    KF: KeyFunction<H> + Send + Sync,
    KF::Key: Borrow<[u8]> + for<'a> From<&'a [u8]>,
{
    fn as_plain_db(&self) -> &dyn PlainDB<H::Out, Vec<u8>> {
        self
    }
    fn as_plain_db_mut(&mut self) -> &mut dyn PlainDB<H::Out, Vec<u8>> {
        self
    }
}

impl<H, KF> AsHashDB<H, Vec<u8>> for MemoryDB<H, KF, Vec<u8>>
where
    H: KeyHasher,
    KF: KeyFunction<H> + Send + Sync,
{
    fn as_hash_db(&self) -> &dyn HashDB<H, Vec<u8>> {
        self
    }
    fn as_hash_db_mut(&mut self) -> &mut dyn HashDB<H, Vec<u8>> {
        self
    }
}

impl<H, KF> HashDB<H, Vec<u8>> for MemoryDB<H, KF, DBValue>
where
    H: KeyHasher,
    KF: KeyFunction<H> + Send + Sync,
{
    fn get(&self, key: &H::Out, prefix: Prefix) -> Option<Vec<u8>> {
        if key == &self.hashed_null_node {
            return None
        }

        let key = KF::key(key, prefix);
        match self.data.get(&key) {
            Some(&(ref d, rc)) if rc > 0 => Some(d.get_raw(true).expect("ShouldNotErr")),
            _ => None,
        }
    }

    fn contains(&self, key: &H::Out, prefix: Prefix) -> bool {
        if key == &self.hashed_null_node {
            return true
        }

        let key = KF::key(key, prefix);
        match self.data.get(&key) {
            Some(&(_, x)) if x > 0 => true,
            _ => false,
        }
    }

    fn emplace(&mut self, key: H::Out, prefix: Prefix, value: Vec<u8>) {
        let key = KF::key(&key, prefix);
        match self.data.entry(key) {
            Entry::Occupied(mut entry) => {
                let &mut (ref mut old_value, ref mut rc) = entry.get_mut();
                old_value.put_raw(value, true).expect("ShouldNotFail");
                *rc = 1;
            },
            Entry::Vacant(entry) => {
                entry.insert((StorageValue::new_raw(Some(value), true), 1));
            },
        }
    }

    fn insert(&mut self, prefix: Prefix, value: &[u8]) -> H::Out {
        let key = H::hash(value);
        <Self as HashDB<H, Vec<u8>>>::emplace(self, key, prefix, value.into());
        key
    }

    fn remove(&mut self, key: &H::Out, prefix: Prefix) {
        if key == &self.hashed_null_node {
            return
        }

        let key = KF::key(key, prefix);
        match self.data.entry(key) {
            Entry::Occupied(mut entry) => {
                let &mut (_, ref mut rc) = entry.get_mut();
                *rc = -1;
            },
            Entry::Vacant(entry) => {
                let value = Default::default();
                entry.insert((value, -1));
            },
        }
    }
}

impl<H, KF> AsHashDB<H, Vec<u8>> for MemoryDB<H, KF, DBValue>
where
    H: KeyHasher,
    KF: KeyFunction<H> + Send + Sync,
{
    fn as_hash_db(&self) -> &dyn HashDB<H, Vec<u8>> {
        self
    }
    fn as_hash_db_mut(&mut self) -> &mut dyn HashDB<H, Vec<u8>> {
        self
    }
}

