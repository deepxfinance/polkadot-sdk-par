use codec::{Decode, Encode};
use node_template_runtime::{UncheckedExtrinsic, RuntimeCall, BalancesCall};
use sc_basic_authorship::RCGroup;

pub struct DefaultRCGroup;

impl RCGroup for DefaultRCGroup {
    fn call_dependent_data(tx_data: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
        let mut group_data = vec![];
        // if parse extrinsic failed, it means native code don't know the call might be runtime updated.
        let utx: UncheckedExtrinsic = match Decode::decode(&mut tx_data.as_slice()) {
            Ok(utx) => utx,
            Err(_) => return Ok(Vec::new()),
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
