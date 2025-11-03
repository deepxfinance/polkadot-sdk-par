use std::{fmt::Debug, ops::Add};
use codec::{Decode, Encode};
use parking_lot::MappedMutexGuard;
use sc_consensus::shared_data::{SharedData, SharedDataLocked};
use crate::AuthorityList;

/// A shared authority set.
#[derive(Clone)]
pub struct SharedAuthoritySet<N> {
    inner: SharedData<AuthoritySet<N>>,
}

impl<N> SharedAuthoritySet<N>
where
    N: Add<Output = N> + Ord + Clone + Debug,
{
    /// Clone the inner `AuthoritySetChanges`.
    pub fn authority_set_changes(&self) -> AuthoritySetChanges<N> {
        self.inner().authority_set_changes.clone()
    }
}

impl<N> SharedAuthoritySet<N> {
    pub fn inner(&self) -> MappedMutexGuard<AuthoritySet<N>> {
        self.inner.shared_data()
    }

    /// Returns access to the [`AuthoritySet`] and locks it.
    ///
    /// For more information see [`SharedDataLocked`].
    #[allow(unused)]
    pub(crate) fn inner_locked(&self) -> SharedDataLocked<AuthoritySet<N>> {
        self.inner.shared_data_locked()
    }
}

/// Tracks historical authority set changes. We store the view numbers for the last block
/// of each authority set.
#[derive(Debug, Encode, Decode, Clone, PartialEq)]
pub struct AuthoritySetChanges<N>(Vec<(u64, N)>);

impl<N: Ord + Clone> AuthoritySetChanges<N> {
    pub(crate) fn empty() -> Self {
        Self(Default::default())
    }

    pub fn as_ref(&self) -> &Vec<(u64, N)> {
        &self.0
    }

    pub fn as_ref_mut(&mut self) -> &mut Vec<(u64, N)> {
        &mut self.0
    }
}

/// A set of authorities.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct AuthoritySet<N> {
    pub current_authorities: AuthorityList,
    pub authority_set_changes: AuthoritySetChanges<N>,
}

impl<N> From<AuthoritySet<N>> for SharedAuthoritySet<N> {
    fn from(set: AuthoritySet<N>) -> Self {
        SharedAuthoritySet {
            inner: SharedData::new(set),
        }
    }
}

impl<N> AuthoritySet<N>
where
    N: Ord + Clone,
{
    // authority sets must be non-empty and all weights must be greater than 0
    fn invalid_authority_list(authorities: &AuthorityList) -> bool {
        authorities.is_empty()
            // || authorities.iter().any(|(_, w)| *w == 0)
    }

    /// Get a genesis set with given authorities.
    pub(crate) fn genesis(initial: AuthorityList) -> Option<Self> {
        if Self::invalid_authority_list(&initial) {
            return None;
        }

        Some(AuthoritySet {
            current_authorities: initial,
            authority_set_changes: AuthoritySetChanges::empty(),
        })
    }

    /// Create a new authority set.
    #[allow(unused)]
    pub(crate) fn new(
        authorities: AuthorityList,
        authority_set_changes: AuthoritySetChanges<N>,
    ) -> Option<Self> {
        if Self::invalid_authority_list(&authorities) {
            return None;
        }

        Some(AuthoritySet {
            current_authorities: authorities,
            authority_set_changes,
        })
    }

    pub fn update_authorities_change(&mut self, view: u64, block: N, authorities: AuthorityList) {
        if Self::invalid_authority_list(&authorities) { return; }
        if let Some((last_change_view, last_block)) = self.authority_set_changes.0.last().as_ref() {
            if view <= *last_change_view || block <= *last_block {
                return;
            }
        }
        self.authority_set_changes.0.push((view, block));
        self.current_authorities = authorities;
    }

    pub fn authorities_for_view(&self, view: u64) -> Option<&AuthorityList> {
        if let Some((last_change_view, _)) = self.authority_set_changes.0.last().as_ref() {
            if self.authority_set_changes.0.is_empty() || view > *last_change_view {
                Some(&self.current_authorities)
            } else {
                None
            }
        } else {
            Some(&self.current_authorities)
        }
    }
}
