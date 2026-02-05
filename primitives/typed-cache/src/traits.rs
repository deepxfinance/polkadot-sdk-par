#[cfg(feature = "std")]
use downcast_rs::{impl_downcast, DowncastSync};
use sp_std::collections::btree_map::BTreeMap;
use sp_std::collections::btree_set::BTreeSet;
use sp_std::vec::Vec;
use crate::overlayed_changes::ExecutionMode;
use crate::{RcT, StorageKey};

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
pub trait StorageIO<QT: QueryTransfer<V>, V> {
    fn contains(&self, space: &[u8], key: &[u8]) -> Option<bool>;
    fn put(&mut self, space: &[u8], key: &[u8], value: V);
    fn get<F>(&mut self, space: &[u8], key: &[u8], init: Option<F>) -> Option<Option<V>> where F: Fn(&[u8]) -> Option<V>;
    fn get_change(&self, space: &[u8], key: &[u8]) -> Option<QT::Qry>;
    fn take(&mut self, space: &[u8], key: &[u8]) -> Option<QT::Qry>;
    fn kill(&mut self, space: &[u8], key: &[u8]);
    fn get_ref<F>(&mut self, space: &[u8], key: &[u8], init: Option<F>) -> Option<RcT<QT::Qry>> where F: Fn(&[u8]) -> Option<V>;
    fn get_change_ref(&self, space: &[u8], key: &[u8]) -> Option<RcT<QT::Qry>>;
    fn pop_ref(&mut self, space: &[u8], key: &[u8]) -> Option<RcT<QT::Qry>>;
    fn mutate<F, R, E, M>(&mut self, space: &[u8], key: &[u8], init: Option<F>, mutate: M) -> Option<Result<R, E>>
    where
        F: FnOnce() -> Option<V>,
        M: FnOnce(&mut QT::Qry) -> Result<R, E>;
    fn append<F, Item>(&mut self, space: &[u8], key: &[u8], init: F, item: Item) -> bool
    where
        F: FnOnce() -> V,
        V: TypedAppend<Item>;
    fn init(&mut self, space: &[u8], key: &[u8], value: Option<V>) -> bool;
    fn cached(&self, space: &[u8], key: &[u8]) -> bool;
}

/// Trait for value transfer.
pub trait QueryTransfer<V>: 'static {
    #[cfg(feature = "std")]
    type Qry: Clone;
    #[cfg(not(feature = "std"))]
    type Qry;

    /// Convert an optional value retrieved from storage to the type queried.
    fn from_optional_value_to_query(v: Option<V>) -> Self::Qry;

    /// Convert a query to an optional value into storage.
    fn from_query_to_optional_value(v: Self::Qry) -> Option<V>;

    /// Convert an optional value retrieved from storage to the type queried.
    fn mut_query<M, R, E>(v: &mut Self::Qry, m: M) -> Result<R, E>
    where
        M: FnOnce(&mut Self::Qry) -> Result<R, E>;

    fn append_query<QT: QueryTransfer<V>, Item>(v: &mut Self::Qry, item: Item)
    where
        V: TypedAppend<Item>;

    fn exists(v: &Self::Qry) -> bool;
}

#[cfg(not(feature = "std"))]
/// Default no requirements for `no_std`
pub trait StdClone {}

#[cfg(not(feature = "std"))]
impl<S> StdClone for S {}

#[cfg(feature = "std")]
pub trait StdClone: Clone {}

#[cfg(feature = "std")]
impl<S: Clone> StdClone for S {}

pub struct ValueQT;

impl<V: Default + StdClone> QueryTransfer<V> for ValueQT {
    type Qry = V;
    fn from_optional_value_to_query(v: Option<V>) -> Self::Qry {
        v.unwrap_or_default()
    }

    fn from_query_to_optional_value(v: Self::Qry) -> Option<V> {
        Some(v)
    }

    fn mut_query<M, R, E>(v: &mut Self::Qry, m: M) -> Result<R, E>
    where
        M: FnOnce(&mut Self::Qry) -> Result<R, E>
    {
        m(v)
    }

    fn append_query<QT: QueryTransfer<V>, Item>(v: &mut Self::Qry, item: Item)
    where
        V: TypedAppend<Item>,
    {
        v.append(item)
    }

    fn exists(_v: &Self::Qry) -> bool {
        true
    }
}

pub struct OptionQT;

impl<V: StdClone> QueryTransfer<V> for OptionQT {
    type Qry = Option<V>;
    fn from_optional_value_to_query(v: Option<V>) -> Self::Qry {
        v
    }

    fn from_query_to_optional_value(v: Self::Qry) -> Option<V> {
        v
    }

    fn mut_query<M, R, E>(v: &mut Self::Qry, m: M) -> Result<R, E>
    where
        M: FnOnce(&mut Self::Qry) -> Result<R, E>
    {
        m(v)
    }

    fn append_query<QT: QueryTransfer<V>, Item>(v: &mut Self::Qry, item: Item)
    where
        V: TypedAppend<Item>,
    {
        if let Some(v) = v.as_mut() {
            v.append(item);
        } else {
            let mut default = V::default();
            default.append(item);
            *v = Some(default);
        }
    }

    fn exists(v: &Self::Qry) -> bool {
        v.is_some()
    }
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

