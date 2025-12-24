#![cfg_attr(not(feature = "std"), no_std)]

use codec::{Decode, Encode, MaxEncodedLen};
use frame_support::RuntimeDebug;
use frame_support::sp_runtime::{traits::One, Saturating, transaction_validity::InvalidTransaction};
use scale_info::TypeInfo;
use sp_core::Get;
use sp_std::vec::Vec;

/// Special TimeNonce with latest used list.
/// Its length is limited by upper layer.
#[derive(Clone, Eq, PartialEq, Default, Encode, Decode, RuntimeDebug, TypeInfo)]
pub struct TimeNonce<Index>(Vec<Index>);

impl<Index: Encode> MaxEncodedLen for TimeNonce<Index> {
	fn max_encoded_len() -> usize {
		// limited max 100 values
		core::mem::size_of::<Index>() * 100 + 2
	}
}

impl<Index> TimeNonce<Index> {
	pub fn find_index(&self, nonce: &Index) -> Result<usize, InvalidTransaction>
	where
		Index: Ord,
	{
		if let Some(first_nonce) = self.0.first() {
			if nonce <= first_nonce {
				return Err(InvalidTransaction::Stale);
			}
		}
		let mut index = self.0.len();
		// TODO faster method to get correct ordered index.
		// For most use case, incoming timestamp are newer, we choose to check by descending order.
		for i in (0..self.0.len()).rev() {
			if nonce < &self.0[i] {
				index = i;
			} else if nonce > &self.0[i] {
				break;
			} else {
				return Err(InvalidTransaction::Stale);
			}
		}
		Ok(index)
	}

	pub fn try_update<S: Get<u32>>(&mut self, nonce: Index) -> Result<(), InvalidTransaction>
	where
		Index: Ord,
	{
		self.update::<S>(self.find_index(&nonce)?, nonce);
		Ok(())
	}

	pub fn update<S: Get<u32>>(&mut self, index: usize, nonce: Index) {
		self.0.insert(index, nonce);
		if self.0.len() > S::get() as usize {
			self.0.remove(0);
		}
	}
}

/// Information of an account.
#[derive(Clone, Eq, PartialEq, Default, RuntimeDebug, Encode, Decode, TypeInfo, MaxEncodedLen)]
pub struct AccountInfo<Index, AccountData> {
	/// The number of transactions this account has sent.
	pub nonce: Index,
	/// The number of other modules that currently depend on this account's existence. The account
	/// cannot be reaped until this is zero.
	pub consumers: u32,
	/// The number of other modules that allow this account to exist. The account may not be reaped
	/// until this and `sufficients` are both zero.
	pub providers: u32,
	/// The number of modules that allow this account to exist for their own purposes only. The
	/// account may not be reaped until this and `providers` are both zero.
	pub sufficients: u32,
	/// The additional data that belongs to this account. Used to store the balance(s) in a lot of
	/// chains.
	pub data: AccountData,
	/// Last updated block timestamp(milliseconds).
	pub update: u64,
	///
	pub time_nonce: TimeNonce<Index>,
	/// Valid quota count for any transaction.
	///
	/// If quota == 0, this account is not activated, can't do any transaction.
	/// If quota == 1, this account is out of quota, can only do transactions by limited rate.
	/// If quota == u32::MAX, this account is frozen, can't do any transaction.
	pub quota: u32,
}

impl<Index, AccountData> AccountInfo<Index, AccountData> {
	/// Unfreeze account to enable account to do transactions.
	/// Not work if account already unfrozen.
	pub fn unfreeze(&mut self, quota: u32) {
		if self.quota != u32::MAX {
			return;
		}
		self.quota = quota;
	}

	/// Freeze account. So that account can't do any transactions.
	pub fn freeze(&mut self) {
		self.quota = u32::MAX;
	}

	/// Add account's quota, can't reach u32::MAX.
	pub fn add_quota(&mut self, quota: u32) {
		self.quota = self.quota.saturating_add(quota);
		if self.quota == u32::MAX {
			self.quota -= 1;
		}
	}

	/// Check if account can do transaction with quota check and nonce check.
	/// `time` should be the estimated execute timestamp or actual block timestamp
	pub fn check_extrinsic_nonce<Free: Get<u64>>(&self, time: u64, nonce: Index) -> Result<(), InvalidTransaction>
	where
		Index: Ord
	{
		if nonce < self.nonce {
			return Err(InvalidTransaction::Stale);
		}
		self.check_quota::<Free>(time)
	}

	/// Check if account can do transaction with quota check and time_nonce check.
	/// `time` should be the estimated execute timestamp or actual block timestamp
	pub fn check_extrinsic_time_nonce<Free: Get<u64>>(&self, time: u64, nonce: Index) -> Result<(), InvalidTransaction>
	where
		Index: Ord,
	{
		self.check_quota::<Free>(time)?;
		self.check_time_nonce(&nonce)
	}

	/// Apply account transaction with quota update and nonce update.
	/// `time` should be the actual executing block timestamp.
	/// `nonce` should be normal nonce.
	pub fn apply_extrinsic_nonce<Free: Get<u64>>(&mut self, time: u64, nonce: Index) -> Result<(), InvalidTransaction>
	where
		Index: Ord + One + Saturating
	{
		if self.quota == 0 {
			Err(InvalidTransaction::BadSigner)
		} else if self.update + Free::get() <= time {
			if self.nonce == nonce {
				self.update = time;
				self.nonce = nonce.saturating_add(One::one());
				Ok(())
			} else if self.nonce > nonce {
				Err(InvalidTransaction::Stale)
			} else {
				Err(InvalidTransaction::Future)
			}
		} else if self.quota > 1 {
			if self.nonce == nonce {
				self.update = time;
				self.quota -= 1;
				self.nonce = nonce.saturating_add(One::one());
				Ok(())
			} else if self.nonce > nonce {
				Err(InvalidTransaction::Stale)
			} else {
				Err(InvalidTransaction::Future)
			}
		} else {
			Err(InvalidTransaction::Payment)
		}
	}

	/// Apply account transaction with quota update and time_nonce update.
	/// `time` should be the actual executing block timestamp.
	/// `nonce` should be the singed timestamp.
	pub fn apply_extrinsic_time_nonce<Free: Get<u64>, S: Get<u32>>(&mut self, time: u64, nonce: Index) -> Result<(), InvalidTransaction>
	where
		Index: Ord,
	{
		if self.quota == 0 {
			Err(InvalidTransaction::BadSigner)
		} else if self.update + Free::get() <= time {
			let index = self.time_nonce.find_index(&nonce)?;
			self.update = time;
			self.time_nonce.update::<S>(index, nonce);
			Ok(())
		} else if self.quota > 1 {
			let index = self.time_nonce.find_index(&nonce)?;
			self.update = time;
			self.quota -= 1;
			self.time_nonce.update::<S>(index, nonce);
			Ok(())
		} else {
			Err(InvalidTransaction::Payment)
		}
	}

	/// Check if quota is enough.
	pub fn check_quota<Free: Get<u64>>(&self, time: u64) -> Result<(), InvalidTransaction> {
		if self.update + Free::get() <= time {
			Ok(())
		} else {
			if self.quota > 1 {
				Ok(())
			} else {
				Err(InvalidTransaction::Payment)
			}
		}
	}

	/// Update `quota` and `update`.
	pub fn try_apply_quota<Free: Get<u64>>(&mut self, time: u64) -> Result<(), InvalidTransaction> {
		if self.quota == 0 {
			Err(InvalidTransaction::BadSigner)
		} else if self.update + Free::get() <= time {
			Ok(())
		} else if self.quota > 1 {
			self.update = time;
			self.quota -= 1;
			Ok(())
		} else {
			Err(InvalidTransaction::Payment)
		}
	}

	/// Check if transaction time nonce is valid.
	pub fn check_time_nonce(&self, nonce: &Index) -> Result<(), InvalidTransaction>
	where
		Index: Ord,
	{
		self.time_nonce.find_index(nonce).map(|_| ())
	}

	/// Try to apply new transaction time_nonce.
	pub fn try_apply_time_nonce<S: Get<u32>>(&mut self, nonce: Index) -> Result<(), InvalidTransaction>
	where
		Index: Ord,
	{
		self.time_nonce.try_update::<S>(nonce)
	}
}

#[test]
fn test_time_nonce() {
	use sp_timestamp::Timestamp;

	let mut time_nonce: TimeNonce<u64> = TimeNonce::default();
	let current_time = Timestamp::current().as_millis();
	time_nonce.try_update::<sp_core::ConstU32<5>>(current_time + 5000).unwrap();
	time_nonce.try_update::<sp_core::ConstU32<5>>(current_time + 10000).unwrap();
	time_nonce.try_update::<sp_core::ConstU32<5>>(current_time + 15000).unwrap();
	time_nonce.try_update::<sp_core::ConstU32<5>>(current_time + 15000).unwrap();
	time_nonce.try_update::<sp_core::ConstU32<5>>(current_time + 20000).unwrap();
	time_nonce.try_update::<sp_core::ConstU32<5>>(current_time + 25000).unwrap();
	time_nonce.try_update::<sp_core::ConstU32<5>>(current_time + 30000).unwrap();
	time_nonce.try_update::<sp_core::ConstU32<5>>(current_time + 30000).unwrap();
	time_nonce.try_update::<sp_core::ConstU32<5>>(current_time + 35000).unwrap();
	assert_eq!(
		time_nonce.0,
		vec![
			current_time + 15000,
			current_time + 20000,
			current_time + 25000,
			current_time + 30000,
			current_time + 35000,
		]
	);
}

#[test]
fn test_account() {
	use sp_timestamp::Timestamp;
	use sp_core::{ConstU64, ConstU32};

	#[derive(Encode, Decode, Clone, PartialEq, Eq, Default)]
	pub struct AccountData<Balance> {
		pub free: Balance,
		pub reserved: Balance,
		pub frozen: Balance,
		pub flags: ExtraFlags,
	}

	#[derive(Encode, Decode, Clone, PartialEq, Eq, Default)]
	pub struct ExtraFlags(u128);

	fn test_time_nonce_set(mut base: u64, len: usize, back_rate: usize) -> Vec<u64> {
		let mut set = Vec::new();
		set.push(base);
		for c in (0..len).collect::<Vec<_>>().chunks(back_rate) {
			for i in c {
				set.push(base + *i as u64 * 9500);
			}
			base = *set.last().unwrap();
			set.push(set.last().unwrap() - 10000);
		}

		set
	}

	let base_time = Timestamp::current().as_millis();
	let set = test_time_nonce_set(base_time, 200, 4);
	let mut account: AccountInfo<u64, AccountData<u128>> = AccountInfo::default();
	account.add_quota(100);
	let start = std::time::Instant::now();
	let mut applied = 0usize;
	let mut failed = 0usize;
	let mut current = base_time + 2000;
	for time in set {
		if account.apply_extrinsic_time_nonce::<ConstU64<10000>, ConstU32<100>>(current, time).is_ok() {
			applied += 1;
		} else {
			failed += 1;
		}
		current = account.update + 2000;
	}
	let apply_time = start.elapsed();

	let start = std::time::Instant::now();
	let enc_time_nonce = account.time_nonce.encode();
	let enc_time_nonce_time = start.elapsed();
	let start = std::time::Instant::now();
	let enc_acc = account.encode();
	let enc_time = start.elapsed();
	println!("applied {applied} failed {failed} account {}({enc_time:?}) time_nonce {}({enc_time_nonce_time:?}) apply_time {apply_time:?}", enc_acc.len(), enc_time_nonce.len());
}
