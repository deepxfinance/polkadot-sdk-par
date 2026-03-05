pub mod import;
pub mod import_queue;

pub use import::*;
pub use import_queue::*;
use std::sync::Arc;
use sc_basic_authorship::{BlockOracle, BlockPropose};
use sc_client_api::Backend;
use sc_network_common::role::Role;
use sp_api::{BlockT, TransactionFor};
use sp_runtime::generic::BlockId;
use crate::client::{AuthoritySetProvider, ClientForHotstuff};
use crate::{aux_schema, AuthorityId, LinkHalf};

/// Make block importer and link half necessary to tie the background voter
/// to it.
pub fn block_import<BE, Block: BlockT, Client, SC, E, O, Error>(
    role: Role,
    client: Arc<Client>,
    executor: E,
    oracle: Arc<O>,
    authorities_provider: &dyn AuthoritySetProvider<Block>,
) -> Result<
    (
        HotstuffBlockImport<BE, Block, Client, E, O>,
        LinkHalf<Block, Client, SC>,
    ),
    sp_blockchain::Error,
>
where
    BE: Backend<Block> + 'static,
    Client: ClientForHotstuff<Block, BE> + 'static,
    E: BlockPropose<Block, Transaction = TransactionFor<Client, Block>, Error = Error> + Send + Sync + 'static,
    O: BlockOracle<Block> + Sync + Send + 'static,
    Error: std::error::Error + Send + From<sp_consensus::Error> + 'static,
{
    let chain_info = client.info();
    let persistent_data = aux_schema::load_persistent(
        &*client,
        move || {
            let authorities = authorities_provider.get(BlockId::Hash(chain_info.best_hash))?;
            let authorities = authorities
                .iter()
                .map(|p| (p.clone(), 1))
                .collect::<Vec<(AuthorityId, u64)>>();

            Ok(authorities)
        },
    )?;

    Ok((
        HotstuffBlockImport::new(client.clone(), role, Arc::new(executor), oracle, persistent_data.clone()),
        LinkHalf {
            client,
            select_chain: None,
            persistent_data,
        },
    ))
}
