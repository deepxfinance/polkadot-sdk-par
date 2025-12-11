#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;
extern crate core;

#[cfg(feature = "std")]
mod rstd {
    pub use std::{
        borrow, boxed, cmp, collections::VecDeque, convert, error::Error, fmt, hash, iter, marker,
        mem, ops, rc, result, sync, vec,
    };
}

#[cfg(not(feature = "std"))]
mod rstd {
    pub use alloc::{borrow, boxed, collections::VecDeque, rc, sync, vec};
    pub use core::{cmp, convert, fmt, hash, iter, marker, mem, ops, result};
    pub trait Error {}
    impl<T> Error for T {}
}

use hash_db::Hasher;
use crate::rstd::vec::Vec;
pub use kvdb::KVDB;
pub use kvdbmut::KVDBMut;
pub use memory_db::*;
pub mod kvdb;
pub mod kvdbmut;
pub mod memory_db;

/// Database value
pub type DBValue = Vec<u8>;

/// storage Prefix following pad for special key storage_hash(e.g. CODE).
pub const STORAGE_HASH: u8 = 1;

/// A key-value datastore implemented as a database-backed.
pub trait KV<H: Hasher> {
    /// Does the db contain a given key?
    fn contains(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// What is the value of the given key?
    fn get(&self, key: &[u8]) -> Option<DBValue>;
}

/// A key-value datastore implemented as a database-backed modified.
pub trait KVMut<'db, H: Hasher> {
    /// Does the trie contain a given key?
    fn contains(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// What is the value of the given key?
    fn get<'a, 'key>(&'a self, key: &'key [u8]) -> Option<DBValue>
    where
        'a: 'key;

    /// Insert a `key`/`value` pair into the db. An empty value is equivalent to removing
    /// `key` from the db. Returns the old value associated with this key, if it existed.
    fn insert(
        &mut self,
        key: &[u8],
        value: DBValue,
    );

    /// Remove a `key` from the db. Equivalent to making it equal to the empty
    /// value. Returns the old value associated with this key, if it existed.
    fn remove(&mut self, key: &[u8]) -> Option<DBValue>;
}

pub trait KVCache<H: Hasher> {
    /// Lookup value for the given `key`.
    // TODO return reference for less copy.
    fn lookup_value_for_key(&mut self, hash: H::Out, key: &[u8], pad: Option<u8>) -> Option<DBValue>;

    /// Cache the given `value` for the given `key`.
    fn cache_value_for_key(&mut self, hash: H::Out, key: &[u8], pad: Option<u8>, value: DBValue);

    /// remove the given `value` for the given `key`.
    fn remove_value_for_key(&mut self, hash: H::Out, key: &[u8], pad: Option<u8>);
}


/// A container for storing bytes.
///
/// This uses a reference counted pointer internally, so it is cheap to clone this object.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bytes(rstd::sync::Arc<[u8]>);

impl rstd::ops::Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes.into())
    }
}

impl From<&[u8]> for Bytes {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes.into())
    }
}

impl<T: AsRef<[u8]>> PartialEq<T> for Bytes {
    fn eq(&self, other: &T) -> bool {
        self.as_ref() == other.as_ref()
    }
}

/// A weak reference of [`Bytes`].
///
/// A weak reference means that it doesn't prevent [`Bytes`] from being dropped because
/// it holds a non-owning reference to the associated [`Bytes`] object. With [`Self::upgrade`] it
/// is possible to upgrade it again to [`Bytes`] if the reference is still valid.
#[derive(Clone, Debug)]
pub struct BytesWeak(rstd::sync::Weak<[u8]>);

impl BytesWeak {
    /// Upgrade to [`Bytes`].
    ///
    /// Returns `None` when the inner value was already dropped.
    pub fn upgrade(&self) -> Option<Bytes> {
        self.0.upgrade().map(Bytes)
    }
}

impl From<Bytes> for BytesWeak {
    fn from(bytes: Bytes) -> Self {
        Self(rstd::sync::Arc::downgrade(&bytes.0))
    }
}
