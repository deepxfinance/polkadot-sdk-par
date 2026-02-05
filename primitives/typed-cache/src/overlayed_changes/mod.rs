#[cfg(feature = "std")]
pub mod changeset;
#[cfg(feature = "std")]
pub mod storage;
#[cfg(feature = "std")]
pub use storage::*;
use sp_std::vec::Vec;
use sp_std::collections::btree_set::BTreeSet;

pub type StorageKey = Vec<u8>;

/// Keep trace of extrinsics index for a modified value.
#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct Extrinsics(Vec<u32>);

impl Extrinsics {
    /// Extracts extrinsics into a `BTreeSets`.
    fn copy_extrinsics_into(&self, dest: &mut BTreeSet<u32>) {
        dest.extend(self.0.iter())
    }

    /// Add an extrinsics.
    fn insert(&mut self, ext: u32) {
        if Some(&ext) != self.0.last() {
            self.0.push(ext);
        }
    }

    /// Extend `self` with `other`.
    fn extend(&mut self, other: Self) {
        self.0.extend(other.0.into_iter());
    }
}

/// Describes in which mode the node is currently executing.
#[derive(Debug, Clone, Copy)]
pub enum ExecutionMode {
    /// Executing in client mode: Removal of all transactions possible.
    Client,
    /// Executing in runtime mode: Transactions started by the client are protected.
    Runtime,
}
