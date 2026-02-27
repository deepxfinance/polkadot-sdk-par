use std::sync::Arc;
use codec::Decode;
use sc_client_api::{Backend, CallExecutor, ExecutionStrategy};
use sp_core::traits::CallContext;
use sp_runtime::{generic::BlockId, traits::Block as BlockT};
use hotstuff_primitives::RuntimeAuthorityId;
use crate::{client::ClientForHotstuff, AuthorityList};
pub use worker::*;

pub mod state;
pub mod worker;
pub mod network;
pub mod oracle;
pub mod message;
pub mod aggregator;
pub mod error;

pub fn get_authorities_from_client<
    B: BlockT,
    BE: Backend<B>,
    C: ClientForHotstuff<B, BE>,
>(
    client: Arc<C>,
) -> AuthorityList {
    let block_id = BlockId::hash(client.info().best_hash);
    let block_hash = client
        .expect_block_hash_from_id(&block_id)
        .expect("get genesis block hash from client failed");

    let authorities_data = client
        .executor()
        .call(
            block_hash,
            "HotstuffApi_authorities",
            &[],
            ExecutionStrategy::NativeElseWasm,
            CallContext::Offchain,
        )
        .expect("call runtime failed");

    let authorities: Vec<RuntimeAuthorityId> = Decode::decode(&mut &authorities_data[..]).expect("");

    authorities
        .iter()
        .map(|id| (id.clone().into(), 0))
        .collect::<AuthorityList>()
}
