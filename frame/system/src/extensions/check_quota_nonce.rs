// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::Config;
#[cfg(not(feature = "std"))]
use crate::CallLimits;
use codec::{Decode, Encode};
use frame_support::dispatch::{CallType, DispatchInfo};
use scale_info::TypeInfo;
use frame_support::ValueQT;
use sp_runtime::{traits::{DispatchInfoOf, Dispatchable, One, SignedExtension}, transaction_validity::{
	InvalidTransaction, TransactionLongevity, TransactionValidity, TransactionValidityError,
	ValidTransaction,
}, SaturatedConversion};
use sp_std::{vec::Vec, vec};

/// Nonce check and increment to give replay protection for transactions.
///
/// # Transaction Validity
///
/// This extension affects `requires` and `provides` tags of validity, but DOES NOT
/// set the `priority` field. Make sure that AT LEAST one of the signed extension sets
/// some kind of priority upon validating transactions.
#[derive(Encode, Decode, Clone, Eq, PartialEq, TypeInfo)]
#[scale_info(skip_type_params(T))]
pub struct CheckQuotaNonce<T: Config>(#[codec(compact)] pub T::Index);

impl<T: Config> CheckQuotaNonce<T> {
	/// utility constructor. Used only in client/factory code.
	pub fn from(nonce: T::Index) -> Self {
		Self(nonce)
	}
}

pub fn get_timestamp() -> Option<u64> {
	/// Storage Key for [Timestamp::Now]
	const TIMESTAMP: [u8; 32] = [240, 195, 101, 195, 207, 89, 214, 113, 235, 114, 218, 14, 122, 65, 19, 196, 159, 31, 5, 21, 244, 98, 205, 207, 132, 224, 241, 214, 4, 93, 252, 187];

	#[cfg(feature = "std")]
	{ frame_support::storage::unhashed::get_cache::<ValueQT, _, _>(&TIMESTAMP, |_| { Option::<u64>::None }) }
	#[cfg(not(feature = "std"))]
	frame_support::storage::unhashed::get::<u64>(&TIMESTAMP)
}

impl<T: Config> sp_std::fmt::Debug for CheckQuotaNonce<T> {
	#[cfg(feature = "std")]
	fn fmt(&self, f: &mut sp_std::fmt::Formatter) -> sp_std::fmt::Result {
		write!(f, "CheckQuotaNonce({})", self.0)
	}

	#[cfg(not(feature = "std"))]
	fn fmt(&self, _: &mut sp_std::fmt::Formatter) -> sp_std::fmt::Result {
		Ok(())
	}
}

impl<T: Config> CheckQuotaNonce<T>
where
	T::RuntimeCall: Dispatchable<Info = DispatchInfo>,
{
	pub fn do_pre_dispatch(
		self,
		who: &T::AccountId,
		info: &DispatchInfoOf<T::RuntimeCall>,
		_len: usize,
	) -> Result<(), TransactionValidityError> {
		let mut account = crate::Account::<T>::get(who);
		let timestamp_now = get_timestamp()
			.ok_or(TransactionValidityError::Invalid(InvalidTransaction::AncientBirthBlock))?;
		match info.call_type {
			CallType::Nonce => account.apply_extrinsic_nonce::<T::CallLimits>(timestamp_now, self.0),
			CallType::Timestamp => account.apply_extrinsic_time_nonce::<T::CallLimits>(timestamp_now, self.0),
			CallType::NonceQuotaFree => account.apply_extrinsic_nonce_free(timestamp_now, self.0),
			CallType::TimestampQuotaFree => account.apply_extrinsic_time_nonce_free::<T::CallLimits>(timestamp_now, self.0),
		}
			.map_err(|e| TransactionValidityError::Invalid(e))?;
		crate::Account::<T>::insert(who, account);
		Ok(())
	}

	pub fn do_validate(
		&self,
		who: &T::AccountId,
		info: &DispatchInfoOf<T::RuntimeCall>,
		_len: usize,
	) -> TransactionValidity {
		// check index and quote
		let account = crate::Account::<T>::get(who);

		let mut priority2: Option<u64> = None;
		#[cfg(feature = "std")]
		let timestamp_now = sp_timestamp::Timestamp::current().as_millis();
		#[cfg(not(feature = "std"))]
		let timestamp_now = get_timestamp()
			.ok_or(TransactionValidityError::Invalid(InvalidTransaction::AncientBirthBlock))?;
		let mut requires = Vec::new();
		match info.call_type {
			CallType::Nonce => {
				account.check_extrinsic_nonce::<T::CallLimits>(timestamp_now, self.0)
					.map_err(|e| TransactionValidityError::Invalid(e))?;
				if account.nonce < self.0 {
					requires = vec![Encode::encode(&(who, self.0 - One::one()))]
				}
			},
			CallType::Timestamp => {
				account.check_extrinsic_time_nonce::<T::CallLimits>(timestamp_now, self.0)
					.map_err(|e| TransactionValidityError::Invalid(e))?;
				priority2 = Some(self.0.saturated_into());
			},
			CallType::NonceQuotaFree => {
				if self.0 < account.nonce {
					return Err(TransactionValidityError::Invalid(InvalidTransaction::Stale));
				}
				if account.nonce < self.0 {
					requires = vec![Encode::encode(&(who, self.0 - One::one()))]
				}
			}
			CallType::TimestampQuotaFree => {
				account.check_time_nonce::<T::CallLimits>(timestamp_now, &self.0)
					.map_err(|e| TransactionValidityError::Invalid(e))?;
				priority2 = Some(self.0.saturated_into());
			}
		}

		Ok(ValidTransaction {
			groups: None,
			priority: 0,
			priority2: priority2.into(),
			requires,
			provides: vec![Encode::encode(&(who, self.0))],
			longevity: TransactionLongevity::MAX,
			propagate: true,
		})
	}
}

impl<T: Config> SignedExtension for CheckQuotaNonce<T>
where
	T::RuntimeCall: Dispatchable<Info = DispatchInfo>,
{
	type AccountId = T::AccountId;
	type Call = T::RuntimeCall;
	type AdditionalSigned = ();
	type Pre = ();
	const IDENTIFIER: &'static str = "CheckNonce";

	fn additional_signed(&self) -> sp_std::result::Result<(), TransactionValidityError> {
		Ok(())
	}

	fn pre_dispatch(
		self,
		who: &Self::AccountId,
		_call: &Self::Call,
		info: &DispatchInfoOf<Self::Call>,
		len: usize,
	) -> Result<(), TransactionValidityError> {
		self.do_pre_dispatch(who, info, len)
	}

	fn validate(
		&self,
		who: &Self::AccountId,
		_call: &Self::Call,
		info: &DispatchInfoOf<Self::Call>,
		len: usize,
	) -> TransactionValidity {
		self.do_validate(who, info, len)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::mock::{new_test_ext, Test, CALL};
	use frame_support::{assert_noop, assert_ok};

	#[test]
	fn signed_ext_check_nonce_works() {
		new_test_ext().execute_with(|| {
			crate::Account::<Test>::insert(
				1,
				crate::AccountInfo {
					nonce: 1,
					consumers: 0,
					providers: 0,
					sufficients: 0,
					data: 0,
					update: 0,
					time_nonce: TimeNonce::default(),
					quota: 100,
				},
			);
			let info = DispatchInfo::default();
			let len = 0_usize;
			// stale
			assert_noop!(
				CheckQuotaNonce::<Test>(0).validate(&1, CALL, &info, len),
				InvalidTransaction::Stale
			);
			assert_noop!(
				CheckQuotaNonce::<Test>(0).pre_dispatch(&1, CALL, &info, len),
				InvalidTransaction::Stale
			);
			// correct
			assert_ok!(CheckQuotaNonce::<Test>(1).validate(&1, CALL, &info, len));
			assert_ok!(CheckQuotaNonce::<Test>(1).pre_dispatch(&1, CALL, &info, len));
			// future
			assert_ok!(CheckQuotaNonce::<Test>(5).validate(&1, CALL, &info, len));
			assert_noop!(
				CheckQuotaNonce::<Test>(5).pre_dispatch(&1, CALL, &info, len),
				InvalidTransaction::Future
			);
		})
	}
}
