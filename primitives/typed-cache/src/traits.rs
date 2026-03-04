use codec::FullCodec;
use downcast_rs::{impl_downcast, DowncastSync};
use sp_std::collections::btree_map::BTreeMap;
use sp_std::vec::Vec;
use crate::{RcT, Set, StorageKey};

pub trait TStorage: Clone + 'static {}

impl<S: Clone + 'static> TStorage for S {}

pub trait TStorageOverlay: TStorage + FullCodec {}

impl<S: TStorage + FullCodec> TStorageOverlay for S {}

/// Manager for all storage types with overlay management.
pub trait StorageApi: DowncastSync {
    /// Enter runtime.
    fn enter_runtime(&mut self);
    /// Exit runtime
    fn exit_runtime(&mut self) -> usize;
    /// Start transaction changes.
    fn start_transaction(&mut self);
    /// Commit tmp changes when some transaction changes.
    fn commit_transaction(&mut self, dirty_keys: Set<StorageKey>);
    /// Drop tmp changes.
    fn rollback_transaction(&mut self, dirty_keys: Set<StorageKey>);
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
    /// put by raw data.
    fn put_raw(&mut self, first_write_in_tx: bool, key: &[u8], data: Vec<u8>);
    /// Try to get raw value by key
    fn get_raw(&self, key: &[u8], cache_raw: bool) -> Option<Vec<u8>>;
    /// Try to update by raw data if exists.
    fn try_kill(&mut self, first_write_in_tx: bool, key: &[u8]);
}

impl_downcast!(sync StorageApi);

/// For all storage types including Value/Map/DoubleMap/NMap
/// `key` should be calculated full key
/// `space` define specific workspace for same `V`.
/// `init` is another data source
pub trait StorageIO<V> {
    fn contains(&self, key: &[u8]) -> Option<bool>;
    fn put(&mut self, first_write_in_tx: bool, key: &[u8], value: V);
    fn get<F>(&mut self, key: &[u8], init: Option<F>) -> Option<Option<V>> where F: Fn(&[u8]) -> Option<V>;
    fn get_change(&self, key: &[u8]) -> Option<Option<V>>;
    fn take(&mut self, key: &[u8]) -> Option<Option<V>>;
    fn kill(&mut self, first_write_in_tx: bool, key: &[u8]);
    fn get_ref<F>(&mut self, first_write_in_tx: bool, key: &[u8], init: Option<F>) -> Option<RcT<V>> where F: FnOnce() -> Option<V>;
    fn get_change_ref(&self, key: &[u8]) -> Option<RcT<V>>;
    fn pop_ref(&mut self, key: &[u8]) -> Option<RcT<V>>;
    fn mutate<QT: QueryTransfer<V>, F, R, E, M>(&mut self, first_write_in_tx: bool, key: &[u8], init: Option<F>, mutate: M) -> Option<Result<R, E>>
    where
        F: FnOnce() -> Option<V>,
        M: FnOnce(&mut QT::Query) -> Result<R, E>;
    fn append<F, M>(&mut self, first_write_in_tx: bool, key: &[u8], init: F, mutate: M) -> bool
    where
        F: FnOnce() -> V,
        M: FnOnce(&mut Option<V>);
    fn init(&mut self, key: &[u8], value: Option<V>) -> bool;
    fn cached(&self, key: &[u8]) -> bool;
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
