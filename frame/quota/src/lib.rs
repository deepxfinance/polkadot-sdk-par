//! # Balances Pallet
//!
//! This pallets manages account's quota

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "std")]
use frame_support::traits::GenesisBuild;
use frame_support::{
	pallet_prelude::{DispatchResult, CallType},
	traits::Get,
};
use sp_std::prelude::*;

pub use pallet::*;

#[frame_support::pallet]
pub mod pallet {
	use super::*;
	use frame_support::pallet_prelude::*;
	use frame_system::AccountInfo;
	use frame_system::pallet_prelude::*;

	#[pallet::config]
	pub trait Config: frame_system::Config {
		type RuntimeEvent: From<Event<Self>> + IsType<<Self as frame_system::Config>::RuntimeEvent>;

		/// Initial quota for account first activated.
		/// This value should not be `0` or `u32::MAX`
		type ActivateQuota: Get<u32>;
	}

	#[pallet::pallet]
	pub struct Pallet<T>(PhantomData<T>);

	#[pallet::event]
	#[pallet::generate_deposit(pub(super) fn deposit_event)]
	pub enum Event<T: Config> {
		/// An account was set of management.
		Manager { address: T::AccountId, enable: bool },
		/// Initial activate account with quota.
		Activate { address: T::AccountId, quota: u32 },
		/// Account's quota added for some reason.
		AddQuota { address: T::AccountId, quota: u32 },
		/// Freeze account.
		Freeze { address: T::AccountId },
		/// Unfreeze account with quota.
		Unfreeze { address: T::AccountId, quota: u32 },
		/// Account is removed.
		RemoveAccount { address: T::AccountId, quota: u32 },
	}

	#[pallet::error]
	pub enum Error<T> {
		/// Not manager.
		NotManager,
		/// Account is frozen, should be unfrozen first.
		AccountFrozen,
		/// Account is not frozen, can not unfreeze it.
		AccountNotFrozen,
		/// Account is already activated.
		AccountAlreadyActivated,
		/// Account is not activated.
		AccountNotActivated,
		/// If quota reach MAX value, it is frozen.
		MaxQuotaFrozen,
		/// Account quota insufficient.
		InsufficientQuota,
	}

	/// Managers that can activate other accounts.
	#[pallet::storage]
	#[pallet::getter(fn managers)]
	pub type Managers<T: Config> = StorageMap<_, Blake2_128Concat, T::AccountId, bool, ValueQuery>;

	#[pallet::genesis_config]
	pub struct GenesisConfig<T: Config> {
		pub quotas:  sp_std::vec::Vec<(T::AccountId, u32)>,
	}

	impl<T: Config> Default for GenesisConfig<T> {
		fn default() -> Self {
			Self { quotas: Default::default() }
		}
	}

	#[pallet::genesis_build]
	impl<T: Config> GenesisBuild<T> for GenesisConfig<T> {
		fn build(&self) {
			for (_, quota) in &self.quotas {
				assert!(
					*quota != u32::MAX && *quota > 0,
					"the quota of any account should always be at least 1 and not equal with u32::MAX.",
				)
			}

			// ensure no duplicates exist.
			let endowed_accounts = self
				.quotas
				.iter()
				.map(|(x, _)| x)
				.cloned()
				.collect::<sp_std::collections::btree_set::BTreeSet<_>>();

			assert_eq!(endowed_accounts.len(), self.quotas.len(), "duplicate quotas in genesis.");

			for &(ref who, quota) in self.quotas.iter() {
				frame_system::Pallet::<T>::genesis_inc_providers(who);
				let mut account = frame_system::Account::<T>::get(who.clone());
				account.quota = quota;
				account.time_nonce.init();
				frame_system::Account::<T>::insert(who.clone(), account);
				<Managers<T>>::insert(who.clone(), true);
			}
		}
	}

	#[pallet::hooks]
	impl<T: Config> Hooks<BlockNumberFor<T>> for Pallet<T> {
		// TODO maybe some clear quota logic
	}

	#[pallet::call]
	impl<T: Config> Pallet<T> {
		/// Set managers if enabled
		#[pallet::call_index(0)]
		#[pallet::call_type(CallType::NonceQuotaFree)]
		#[pallet::weight(0)]
		pub fn set_manager(origin: OriginFor<T>, address_with_enable: Vec<(T::AccountId, bool)>) -> DispatchResult {
			ensure_root(origin)?;
			for (address, enable) in address_with_enable {
				<Managers<T>>::insert(address.clone(), enable);
				Self::deposit_event(Event::Manager { address, enable });
			}
			Ok(())
		}

		/// Freeze account
		#[pallet::call_index(1)]
		#[pallet::call_type(CallType::NonceQuotaFree)]
		#[pallet::weight(0)]
		pub fn freeze(origin: OriginFor<T>, address: T::AccountId) -> DispatchResult {
			ensure_root(origin)?;
			let mut account = frame_system::Account::<T>::get(address.clone());
			if account.quota == u32::MAX {
				return Err(Error::<T>::AccountFrozen.into());
			}
			account.quota = u32::MAX;
			Self::deposit_event(Event::Freeze { address: address.clone() });
			frame_system::Account::<T>::insert(address, account);
			Ok(())
		}

		/// Freeze account
		#[pallet::call_index(2)]
		#[pallet::call_type(CallType::NonceQuotaFree)]
		#[pallet::weight(0)]
		pub fn unfreeze(origin: OriginFor<T>, address: T::AccountId, quota: u32) -> DispatchResult {
			ensure_root(origin)?;
			let mut account = frame_system::Account::<T>::get(address.clone());
			if account.quota != u32::MAX {
				return Err(Error::<T>::AccountNotFrozen.into());
			}
			account.quota = quota;
			Self::deposit_event(Event::Unfreeze { address: address.clone(), quota});
			frame_system::Account::<T>::insert(address, account);
			Ok(())
		}

		/// Add quota for account by sudo
		/// If account is Frozen, fail
		/// If account is not activated, activate it.
		#[pallet::call_index(3)]
		#[pallet::call_type(CallType::NonceQuotaFree)]
		#[pallet::weight(0)]
		pub fn sudo_add_quota(origin: OriginFor<T>, address: T::AccountId, mut quota: u32) -> DispatchResult {
			ensure_root(origin)?;
			let mut account = frame_system::Account::<T>::get(address.clone());
			if account.quota == u32::MAX {
				return Err(Error::<T>::AccountFrozen.into());
            }
			if account.quota == 0 {
				account.quota = quota + 1;
				Self::deposit_event(Event::Activate { address: address.clone(), quota: quota + 1 });
			} else {
				account.quota += quota;
				if account.quota == u32::MAX {
					account.quota -= 1;
					quota -= 1;
				}
				Self::deposit_event(Event::AddQuota { address: address.clone(), quota });
			}
			frame_system::Account::<T>::insert(address, account);
			Ok(())
		}

		#[pallet::call_index(4)]
		#[pallet::call_type(CallType::NonceQuotaFree)]
		#[pallet::weight(0)]
		pub fn activate_account(origin: OriginFor<T>, address: T::AccountId) -> DispatchResult {
			let manager = ensure_signed(origin)?;
			if !<Managers<T>>::get(manager) {
                return Err(Error::<T>::NotManager.into());
            }
			let mut account = frame_system::Account::<T>::get(address.clone());
			if account.quota == u32::MAX {
				return Err(Error::<T>::AccountFrozen.into());
			}
			if account.quota == 0 {
				let init_quota = T::ActivateQuota::get();
				account.quota = init_quota;
				account.time_nonce.init();
				Self::deposit_event(Event::Activate { address: address.clone(), quota: init_quota });
			} else {
				return Err(Error::<T>::AccountAlreadyActivated.into());
			}
			frame_system::Account::<T>::insert(address, account);
			Ok(())
		}

		/// This is for test, managers should not `add_quota` for other accounts in `production` environment.
		#[pallet::call_index(5)]
		#[pallet::call_type(CallType::NonceQuotaFree)]
		#[pallet::weight(0)]
		pub fn manager_add_quota(origin: OriginFor<T>, address: T::AccountId, quota: u32) -> DispatchResult {
			let manager = ensure_signed(origin)?;
			if !<Managers<T>>::get(manager) {
				return Err(Error::<T>::NotManager.into());
			}
			Self::add_quota(address, quota)?;
			Ok(())
		}
	}

	impl<T: Config> Pallet<T> {
		/// Get account's quota
		///
		/// `0` means account is not activated.
		/// `u32::MAX` means account is frozen.
		/// Other means account is active.
		pub fn get_account_quota(address: T::AccountId) -> u32 {
			frame_system::Account::<T>::get(address).quota
		}

		/// Add quota for account by manager
		/// If account is Frozen, fail
		/// If account is not activated, activate it.
		pub fn add_quota(address: T::AccountId, quota: u32) -> DispatchResult {
			let mut account = frame_system::Account::<T>::get(address.clone());
			if account.quota == u32::MAX {
				return Err(Error::<T>::AccountFrozen.into());
			}
			if account.quota == 0 {
				return Err(Error::<T>::AccountNotActivated.into());
			} else {
				account.quota += quota;
				if account.quota == u32::MAX {
					return Err(Error::<T>::MaxQuotaFrozen.into());
				}
				Self::deposit_event(Event::AddQuota { address: address.clone(), quota });
			}
			frame_system::Account::<T>::insert(address, account);
			Ok(())
		}

		/// Notice!!! This call will activate account if needed. Please ensure rights before call this!!!.
		///
		/// After transfer:
		/// 	If from_account's quota less than `quota_limit_to_remove`, this account will be removed.
		/// 	If you don't want to remove from_account, please set quota_limit_to_remove as `None`.
		pub fn transfer_quota(from: T::AccountId, to: T::AccountId, quota: u32, quota_limit_to_remove: Option<u32>) -> DispatchResult {
			let mut from_account = frame_system::Account::<T>::get(from.clone());
			if from_account.quota == u32::MAX {
				return Err(Error::<T>::AccountFrozen.into());
			}
			if from_account.quota <= quota {
				return Err(Error::<T>::InsufficientQuota.into());
			}
			let mut to_account = frame_system::Account::<T>::get(to.clone());
			if to_account.quota == u32::MAX {
				return Err(Error::<T>::AccountFrozen.into());
			}

			from_account.quota -= quota;
			if to_account.quota == 0 {
				to_account.quota += quota + 1;
				Self::deposit_event(Event::Activate { address: to.clone(), quota: to_account.quota });
			} else {
				to_account.quota += quota;
			}
			let quota_limit_to_remove = quota_limit_to_remove.unwrap_or_default();
			if !Self::try_remove_account(Some(&from_account), from.clone(), quota_limit_to_remove) {
				frame_system::Account::<T>::insert(from, from_account);
			}
			frame_system::Account::<T>::insert(to, to_account);
			Ok(())
		}

		/// Try remove account if account's quota less than `quota_limit`
		/// Return `true` if removed, else `false`.
		pub fn try_remove_account(account: Option<&AccountInfo<T::Index, T::AccountData>>, address: T::AccountId, quota_limit: u32) -> bool {
			let quota = match account {
				Some(account) => account.quota,
				None => frame_system::Account::<T>::get(address.clone()).quota,
			};
			if quota < quota_limit {
				frame_system::Account::<T>::remove(address.clone());
				Self::deposit_event(Event::RemoveAccount { address, quota });
				true
			} else {
				false
			}
		}
	}
}
