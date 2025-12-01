#[cfg(feature = "std")]
use downcast_rs::{impl_downcast, DowncastSync};
use sp_std::collections::btree_map::BTreeMap;
use sp_std::vec::Vec;
use crate::StorageKey;

#[cfg(feature = "std")]
/// Manager for all storage types with overlay management.
pub trait StorageApi: DowncastSync {
    /// Enter runtime.
    fn enter_runtime(&mut self);
    /// Exit runtime
    fn exit_runtime(&mut self);
    /// Start transaction changes.
    fn start_transaction(&mut self);
    /// Commit tmp changes when some transaction changes.
    fn commit_transaction(&mut self);
    /// Drop tmp changes.
    fn rollback_transaction(&mut self);
    /// Convert storages to expected key/value pairs.
    fn drain_commited(&mut self) -> BTreeMap<StorageKey, Option<Vec<u8>>>;
    /// Get another copy with actual data, not just pointer(e.g. Arc).
    fn copy_data(&self) -> sp_std::boxed::Box<dyn StorageApi>;
}

#[cfg(feature = "std")]
impl_downcast!(sync StorageApi);

/// For all storage types including Value/Map/DoubleMap/NMap
/// `key` should be calculated full key
/// `space` define specific workspace for same `V`.
/// `extra` is another data source if `get`/`take` get None
pub trait StorageIO<V> {
    fn put(&mut self, space: &[u8], key: &[u8], value: V);
    fn get<F>(&mut self, space: &[u8], key: &[u8], extra: F) -> Option<V> where F: Fn(&[u8]) -> Option<V>;
    fn get_change(&self, space: &[u8], key: &[u8]) -> Option<Option<V>>;
    fn take<F>(&mut self, space: &[u8], key: &[u8], extra: F) -> Option<V> where F: Fn(&[u8]) -> Option<V>;
    fn kill(&mut self, space: &[u8], key: &[u8]);
    fn mutate<F, M>(&mut self, space: &[u8], key: &[u8], get: F, mutate: M) -> bool
    where
        F: Fn(&[u8]) -> Option<V>,
        M: FnOnce(Option<&mut V>);
}
