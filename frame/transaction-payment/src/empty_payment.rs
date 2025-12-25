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

//! # Empty Payment to replace Transaction Payment Pallet

#![cfg_attr(not(feature = "std"), no_std)]

use crate::{Config, OnChargeTransaction, BalanceOf};
use codec::{Decode, Encode};
use scale_info::TypeInfo;
use frame_support::dispatch::{DispatchInfo, PostDispatchInfo};
use sp_runtime::traits::{DispatchInfoOf, Dispatchable, SignedExtension};
use sp_runtime::transaction_validity::TransactionValidityError;

/// Empty Payment check
#[derive(Encode, Decode, Clone, Eq, PartialEq, TypeInfo)]
#[scale_info(skip_type_params(T))]
pub struct EmptyTransactionPayment<T: Config>(#[codec(compact)] BalanceOf<T>);

impl<T: Config> EmptyTransactionPayment<T> {
    pub fn from(fee: BalanceOf<T>) -> Self {
        Self(fee)
    }
}

impl<T: Config> sp_std::fmt::Debug for EmptyTransactionPayment<T> {
    #[cfg(feature = "std")]
    fn fmt(&self, f: &mut sp_std::fmt::Formatter) -> sp_std::fmt::Result {
        write!(f, "EmptyTransactionPayment<{:?}>", self.0)
    }
    #[cfg(not(feature = "std"))]
    fn fmt(&self, _: &mut sp_std::fmt::Formatter) -> sp_std::fmt::Result {
        Ok(())
    }
}

impl<T: Config> SignedExtension for EmptyTransactionPayment<T>
where
    BalanceOf<T>: Send + Sync + From<u64>,
    T::RuntimeCall: Dispatchable<Info = DispatchInfo, PostInfo = PostDispatchInfo>,
{
    const IDENTIFIER: &'static str = "ChargeTransactionPayment";
    type AccountId = T::AccountId;
    type Call = T::RuntimeCall;
    type AdditionalSigned = ();
    type Pre = (
        // tip
        BalanceOf<T>,
        // who paid the fee - this is an option to allow for a Default impl.
        Self::AccountId,
        // imbalance resulting from withdrawing the fee
        <<T as Config>::OnChargeTransaction as OnChargeTransaction<T>>::LiquidityInfo,
    );
    fn additional_signed(&self) -> sp_std::result::Result<(), TransactionValidityError> {
        Ok(())
    }

    fn pre_dispatch(
        self,
        who: &Self::AccountId,
        _call: &Self::Call,
        _info: &DispatchInfoOf<Self::Call>,
        _len: usize,
    ) -> Result<Self::Pre, TransactionValidityError> {
        Ok((self.0, who.clone(), Default::default()))
    }
}
