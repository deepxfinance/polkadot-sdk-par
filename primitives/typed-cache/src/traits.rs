#[cfg(feature = "std")]
use downcast_rs::{impl_downcast, DowncastSync};
use sp_std::collections::btree_map::BTreeMap;
use sp_std::sync::Arc;
use sp_std::vec::Vec;
use crate::StorageKey;

#[cfg(feature = "std")]
/// Manager for all storage types with overlay management.
pub trait StorageApi: DowncastSync {
    /// Enter runtime.
    fn enter_runtime(&self);
    /// Exit runtime
    fn exit_runtime(&self);
    /// Start transaction changes.
    fn start_transaction(&self);
    /// Commit tmp changes when some transaction changes.
    fn commit_transaction(&self);
    /// Drop tmp changes.
    fn rollback_transaction(&self);
    /// Convert storages to expected key/value pairs.
    fn drain_commited(&self) -> BTreeMap<StorageKey, Option<Vec<u8>>>;
    /// Get another copy with actual data, not just pointer(e.g. Arc).
    fn copy_data(&self) -> Arc<sp_std::boxed::Box<dyn StorageApi>>;
}

#[cfg(feature = "std")]
impl_downcast!(sync StorageApi);

/// For all storage types including Value/Map/DoubleMap/NMap
/// `key` should be calculated full key
/// `space` define specific workspace for same `V`.
/// `extra` is another data source if `get`/`take` get None
pub trait StorageIO<V> {
    fn put(&self, space: &[u8], key: &[u8], value: V);
    fn get<F>(&self, space: &[u8], key: &[u8], extra: F) -> Option<V> where F: Fn(&[u8]) -> Option<V>;
    fn take<F>(&self, space: &[u8], key: &[u8], extra: F) -> Option<V> where F: Fn(&[u8]) -> Option<V>;
    fn kill(&self, space: &[u8], key: &[u8]);
    fn mutate<F, M>(&self, space: &[u8], key: &[u8], get: F, mutate: M) -> bool
    where
        F: Fn(&[u8]) -> Option<V>,
        M: FnOnce(Option<&mut V>);
}
