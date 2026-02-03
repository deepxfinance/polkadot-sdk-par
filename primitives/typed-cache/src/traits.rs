use std::collections::BTreeSet;
use codec::Encode;
#[cfg(feature = "std")]
use downcast_rs::{impl_downcast, DowncastSync};
use sp_std::collections::btree_map::BTreeMap;
use sp_std::vec::Vec;
use crate::changeset::ExecutionMode;
use crate::StorageKey;

#[cfg(not(feature = "std"))]
/// Default no requirements for `no_std`
pub trait TStorage {}

#[cfg(not(feature = "std"))]
impl<S> TStorage for S {}

#[cfg(feature = "std")]
pub trait TStorage: Clone + 'static {}

#[cfg(feature = "std")]
impl<S: Clone + 'static> TStorage for S {}

#[cfg(feature = "std")]
/// Manager for all storage types with overlay management.
pub trait StorageApi: DowncastSync {
    /// Enter runtime.
    fn enter_runtime(&mut self);
    /// Exit runtime
    fn exit_runtime(&mut self) -> usize;
    /// Start transaction changes.
    fn start_transaction(&mut self);
    /// Commit tmp changes when some transaction changes.
    fn commit_transaction(&mut self, mode: &ExecutionMode);
    /// Drop tmp changes.
    fn rollback_transaction(&mut self, mode: &ExecutionMode);
    /// Get expected change if exists.
    fn get_change_encode(&self, key: &[u8]) -> Option<Option<Vec<u8>>>;
    /// Get all changed keys if exists.
    fn get_changed_keys(&self) -> Vec<StorageKey>;
    /// Get all changes.
    fn get_commited(&self) -> BTreeMap<StorageKey, Option<Vec<u8>>>;
    /// Convert storages to expected key/value pairs.
    fn drain_commited(&mut self) -> Vec<(StorageKey, Option<Vec<u8>>)>;
    /// Get another copy with actual data, not just pointer(e.g. Arc).
    fn copy_data(&self) -> sp_std::boxed::Box<dyn StorageApi>;
    /// Try to update by raw data if exists.
    fn try_update_raw(&mut self, space: &[u8], key: &[u8], data: Vec<u8>);
    /// Try to update by raw data if exists.
    fn try_kill(&mut self, space: &[u8], key: &[u8]);
}

#[cfg(feature = "std")]
impl_downcast!(sync StorageApi);

/// For all storage types including Value/Map/DoubleMap/NMap
/// `key` should be calculated full key
/// `space` define specific workspace for same `V`.
/// `init` is another data source
pub trait StorageIO {
    fn contains_key<V>(&mut self, _space: &[u8], _key: &[u8]) -> bool {
        panic!("StorageO::contains_key should be override");
    }
    fn put<V>(&mut self, _space: &[u8], _key: &[u8], _value: V) {
        panic!("StorageO::put should be override");
    }
    fn get<V>(&mut self, _space: &[u8], _key: &[u8]) -> Option<V> {
        panic!("StorageO::get should be override");
    }
    fn get_change<V>(&self, _space: &[u8], _key: &[u8]) -> Option<Option<V>> {
        panic!("StorageO::get_change should be override");
    }
    fn take<V>(&mut self, _space: &[u8], _key: &[u8]) -> Option<V> {
        panic!("StorageO::take should be override");
    }
    fn kill<V>(&mut self, _space: &[u8], _key: &[u8]) {
        panic!("StorageO::kill should be override");
    }
    fn mutate<V, QT: QueryTransfer<V>, F, R, E, M>(&mut self, _space: &[u8], _key: &[u8], _mutate: M) -> Result<R, E>
    where
        M: FnOnce(&mut QT::Query) -> Result<R, E>
    {
        panic!("StorageO::mutate should be override");
    }
    fn typed_append<T, Item: Encode>(&mut self, space: &[u8], key: &[u8], item: Item)
    where
        T: TypedAppend<Item>;
}

pub trait TypedAppend<Item>: Default {
    fn append(&mut self, item: Item);
}

impl<T> TypedAppend<T> for Vec<T> {
    fn append(&mut self, item: T) {
        self.push(item);
    }
}

impl<T: core::cmp::Ord> TypedAppend<T> for BTreeSet<T> {
    fn append(&mut self, item: T) {
        self.insert(item);
    }
}

/// Trait for value transfer.
pub trait QueryTransfer<V> {
    type Query;

    /// Convert an optional value retrieved from storage to the type queried.
    fn from_optional_value_to_query(v: Option<V>) -> Self::Query;

    /// Convert an optional value retrieved from storage to the type queried.
    fn mut_from_optional_value_to_query<M, R, E>(v: &mut Option<V>, m: M) -> (Result<R, E>, Option<V>)
    where
        M: FnOnce(&mut Self::Query) -> Result<R, E>;

    /// Convert a query to an optional value into storage.
    fn from_query_to_optional_value(v: Self::Query) -> Option<V>;
}

pub struct OptionQT;

impl<V> QueryTransfer<V> for OptionQT {
    type Query = Option<V>;
    fn from_optional_value_to_query(v: Option<V>) -> Self::Query {
        v
    }

    fn mut_from_optional_value_to_query<M, R, E>(v: &mut Option<V>, m: M) -> (Result<R, E>, Option<V>)
    where
        M: FnOnce(&mut Self::Query) -> Result<R, E>
    {
        (m(v), None)
    }

    fn from_query_to_optional_value(v: Self::Query) -> Option<V> {
        v
    }
}

impl<V> QueryTransfer<V> for () {
    type Query = V;

    fn from_optional_value_to_query(v: Option<V>) -> Self::Query {
        v.expect("Default QueryTransfer should not be None")
    }

    fn mut_from_optional_value_to_query<M, R, E>(v: &mut Option<V>, m: M) -> (Result<R, E>, Option<V>)
    where
        M: FnOnce(&mut Self::Query) -> Result<R, E>
    {
        match v {
            Some(v) => (m(v), None),
            None => panic!("Default QueryTransfer should not be None"),
        }
    }

    fn from_query_to_optional_value(v: Self::Query) -> Option<V> {
        Some(v)
    }
}
