use codec::{Decode, Encode};
use node_template_runtime::{UncheckedExtrinsic, RuntimeCall, BalancesCall};
use sc_transaction_pool::RCGroup;
use sp_runtime::OpaqueExtrinsic as Extrinsic;
use sc_transaction_pool::error::Error;
use sc_transaction_pool_api::error::Error as PoolError;
use sp_runtime::transaction_validity::TransactionSource;

pub struct DefaultRCGroup;

impl RCGroup<Extrinsic> for DefaultRCGroup {
    type Error = Error;
    fn call_dependent_data(extrinsic: &mut Extrinsic, _source: TransactionSource) -> Result<Vec<Vec<u8>>, Self::Error> {
        let mut group_data = vec![];
        // if parse extrinsic failed, it means native code don't know the call might be runtime updated.
        let utx: UncheckedExtrinsic = match Decode::decode(&mut extrinsic.encode().as_slice()) {
            Ok(utx) => utx,
            Err(e) => {
                return Err(Error::Pool(PoolError::GroupInfo(e.to_string())));
            },
        };
        match utx.function {
            RuntimeCall::Balances(balance_call) => {
                if let Some((signer, _, _)) = &utx.signature {
                    group_data.push(signer.encode());
                }
                match balance_call {
                    BalancesCall::transfer_keep_alive { dest, .. } => group_data.push(dest.encode()),
                    BalancesCall::transfer { dest, .. } => group_data.push(dest.encode()),
                    BalancesCall::force_transfer { dest, .. } => group_data.push(dest.encode()),
                    BalancesCall::transfer_allow_death { dest, .. } => group_data.push(dest.encode()),
                    BalancesCall::transfer_all { dest, .. } => group_data.push(dest.encode()),
                    _ => (),
                }
            },
            _ => (),
        }

        Ok(group_data)
    }
}
