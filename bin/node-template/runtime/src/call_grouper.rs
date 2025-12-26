use codec::Encode;
use sp_runtime::transaction_validity::{RCGroup, TransactionSource, TransactionValidityError, ValidTransaction};
use crate::{AccountId, BalancesCall, RuntimeCall};

pub struct DefaultRCGroup;

impl RCGroup<AccountId, RuntimeCall> for DefaultRCGroup {
    fn call_groups(signer: Option<&AccountId>, call: &RuntimeCall, _source: TransactionSource) -> Result<ValidTransaction, TransactionValidityError> {
        let mut groups = sp_std::vec![];
        match call {
            RuntimeCall::Balances(balance_call) => {
                if let Some(signer) = signer {
                    groups.push(signer.encode());
                }
                match balance_call {
                    BalancesCall::transfer_keep_alive { dest, .. } => groups.push(dest.encode()),
                    BalancesCall::transfer { dest, .. } => groups.push(dest.encode()),
                    BalancesCall::force_transfer { dest, .. } => groups.push(dest.encode()),
                    BalancesCall::transfer_allow_death { dest, .. } => groups.push(dest.encode()),
                    BalancesCall::transfer_all { dest, .. } => groups.push(dest.encode()),
                    _ => (),
                }
            },
            _ => (),
        }

        Ok(ValidTransaction {
            groups: Some(groups),
            ..Default::default()
        })
    }
}
