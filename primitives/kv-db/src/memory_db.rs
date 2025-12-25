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
    null_node_data: T,
    _kf: PhantomData<KF>,
}

impl<H, KF, T> FastBuild<H, KF, T> for MemoryDB<H, KF, T>
where
    H: KeyHasher,
    KF: KeyFunction<H>,
    T: Clone,
{
    fn set_ordered_data(&mut self, data: Map<KF::Key, (T, i32)>) {
        self.data = data;
    }
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
            null_node_data: self.null_node_data.clone(),
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
        Self::from_null_node(&[0u8][..], [][..].into())
    }
}

/// Create a new `MemoryDB` from a given null key/data
impl<H, KF, T> MemoryDB<H, KF, T>
where
    H: KeyHasher,
    T: Default,
    KF: KeyFunction<H>,
{
    /// Remove an element and delete it from storage no matter how big the reference count is.
    pub fn remove_and_purge(&mut self, key: &<H as KeyHasher>::Out, prefix: Prefix) -> Option<T> {
        if key == &self.hashed_null_node {
            return None
        }
        let key = KF::key(key, prefix);
        match self.data.entry(key) {
            Entry::Occupied(mut entry) => {
                let (value, rc) = entry.remove();
                if rc > 0 {
                    Some(value)
                } else {
                    None
                }
            },
            Entry::Vacant(entry) => {
                let value = T::default();
                entry.insert((value, -1));
                None
            },
        }
    }
}

impl<H, KF, T> MemoryDB<H, KF, T>
where
    H: KeyHasher,
    T: for<'a> From<&'a [u8]>,
    KF: KeyFunction<H>,
{
    /// Create a new `MemoryDB` from a given null key/data
    pub fn from_null_node(null_key: &[u8], null_node_data: T) -> Self {
        MemoryDB {
            data: Map::default(),
            hashed_null_node: H::hash(null_key),
            null_node_data,
            _kf: Default::default(),
        }
    }

    /// Create a new instance of `Self`.
    pub fn new(data: &[u8]) -> Self {
        Self::from_null_node(data, data.into())
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
    pub fn purge(&mut self) {
        self.data.retain(|_, (_, rc)| {
            let keep = *rc != 0;
            keep
        });
    }

    /// Return the internal key-value Map, clearing the current state.
    pub fn drain(&mut self) -> Map<KF::Key, (T, i32)> {
        mem::take(&mut self.data)
    }

    /// Grab the raw information associated with a key. Returns None if the key
    /// doesn't exist.
    ///
    /// Even when Some is returned, the data is only guaranteed to be useful
    /// when the refs > 0.
    pub fn raw(&self, key: &<H as KeyHasher>::Out, prefix: Prefix) -> Option<(&T, i32)> {
        if key == &self.hashed_null_node {
            return Some((&self.null_node_data, 1))
        }
        self.data.get(&KF::key(key, prefix)).map(|(value, count)| (value, *count))
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

impl<H, KF, T> PlainDB<H::Out, T> for MemoryDB<H, KF, T>
where
    H: KeyHasher,
    T: Default + PartialEq<T> + for<'a> From<&'a [u8]> + Clone + Send + Sync,
    KF: Send + Sync + KeyFunction<H>,
    KF::Key: Borrow<[u8]> + for<'a> From<&'a [u8]>,
{
    fn get(&self, key: &H::Out) -> Option<T> {
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

    fn emplace(&mut self, key: H::Out, value: T) {
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
                let value = T::default();
                entry.insert((value, -1));
            },
        }
    }
}

impl<H, KF, T> PlainDBRef<H::Out, T> for MemoryDB<H, KF, T>
where
    H: KeyHasher,
    T: Default + PartialEq<T> + for<'a> From<&'a [u8]> + Clone + Send + Sync,
    KF: Send + Sync + KeyFunction<H>,
    KF::Key: Borrow<[u8]> + for<'a> From<&'a [u8]>,
{
    fn get(&self, key: &H::Out) -> Option<T> {
        PlainDB::get(self, key)
    }
    fn contains(&self, key: &H::Out) -> bool {
        PlainDB::contains(self, key)
    }
}

impl<H, KF, T> HashDB<H, T> for MemoryDB<H, KF, T>
where
    H: KeyHasher,
    T: Default + PartialEq<T> + AsRef<[u8]> + for<'a> From<&'a [u8]> + Clone + Send + Sync,
    KF: KeyFunction<H> + Send + Sync,
{
    fn get(&self, key: &H::Out, prefix: Prefix) -> Option<T> {
        if key == &self.hashed_null_node {
            return Some(self.null_node_data.clone())
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

    fn emplace(&mut self, key: H::Out, prefix: Prefix, value: T) {
        if value == self.null_node_data {
            return
        }

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
        if T::from(value) == self.null_node_data {
            return self.hashed_null_node
        }

        let key = H::hash(value);
        HashDB::emplace(self, key, prefix, value.into());
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
                let value = T::default();
                entry.insert((value, -1));
            },
        }
    }
}

impl<H, KF, T> HashDBRef<H, T> for MemoryDB<H, KF, T>
where
    H: KeyHasher,
    T: Default + PartialEq<T> + AsRef<[u8]> + for<'a> From<&'a [u8]> + Clone + Send + Sync,
    KF: KeyFunction<H> + Send + Sync,
{
    fn get(&self, key: &H::Out, prefix: Prefix) -> Option<T> {
        HashDB::get(self, key, prefix)
    }
    fn contains(&self, key: &H::Out, prefix: Prefix) -> bool {
        HashDB::contains(self, key, prefix)
    }
}

impl<H, KF, T> AsPlainDB<H::Out, T> for MemoryDB<H, KF, T>
where
    H: KeyHasher,
    T: Default + PartialEq<T> + for<'a> From<&'a [u8]> + Clone + Send + Sync,
    KF: KeyFunction<H> + Send + Sync,
    KF::Key: Borrow<[u8]> + for<'a> From<&'a [u8]>,
{
    fn as_plain_db(&self) -> &dyn PlainDB<H::Out, T> {
        self
    }
    fn as_plain_db_mut(&mut self) -> &mut dyn PlainDB<H::Out, T> {
        self
    }
}

impl<H, KF, T> AsHashDB<H, T> for MemoryDB<H, KF, T>
where
    H: KeyHasher,
    T: Default + PartialEq<T> + AsRef<[u8]> + for<'a> From<&'a [u8]> + Clone + Send + Sync,
    KF: KeyFunction<H> + Send + Sync,
{
    fn as_hash_db(&self) -> &dyn HashDB<H, T> {
        self
    }
    fn as_hash_db_mut(&mut self) -> &mut dyn HashDB<H, T> {
        self
    }
}
