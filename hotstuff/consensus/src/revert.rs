use std::sync::Arc;
use codec::{Decode, Encode};
use sc_client_api::AuxStore;
use sp_api::{BlockT, HeaderT};
use sp_blockchain::HeaderBackend;
use crate::find_block_commit;
use crate::message::BlockCommit;
use crate::primitives::{HotstuffError, ViewNumber};
use crate::synchronizer::Synchronizer;

#[derive(Debug, Clone,Copy, Encode, Decode, Default)]
pub enum RevertTo {
    #[default]
    Latest,
    Finalized,
}

pub fn revert<B, C>(client: &Arc<C>, to: Option<RevertTo>) -> Result<(), HotstuffError>
where
    B: BlockT,
    C: AuxStore + HeaderBackend<B>,
{
    let mut synchronizer = Synchronizer::<B, C>::new(client.clone());
    let revert_to = to.unwrap_or_default();
    let (to_block_number, to_block_hash, high_view, high_digest) = revert_with(client, &mut synchronizer, revert_to)?;
    println!("HotstuffRevert to {revert_to:?} block #{to_block_number} ({to_block_hash:?}), high_view {high_view}, high_proposal_digest {high_digest:?}");
    Ok(())
}

pub(crate) fn revert_with<B, C>(
    client: &Arc<C>,
    synchronizer: &mut Synchronizer<B, C>,
    revert_to: RevertTo,
) -> Result<(<B::Header as HeaderT>::Number, B::Hash, ViewNumber, B::Hash), HotstuffError>
where
    B: BlockT,
    C: AuxStore + HeaderBackend<B>,
{
    let best_block_number = client.info().best_number;
    let best_block_hash = client.info().best_hash;
    let best_block_commit = get_block_commit(client, best_block_hash)?;
    let (to_block_number, to_block_hash, to_view, to_digest) = match revert_to {
        RevertTo::Latest => {
            (best_block_number, best_block_hash, best_block_commit.view[2], best_block_commit.commit_hash())
        }
        RevertTo::Finalized => {
            let finalized_number = best_block_commit.block_claim.best_block.number;
            let finalized_hash = best_block_commit.block_claim.best_block.hash;
            let finalized_block_commit = get_block_commit(client, finalized_hash)?;
            (finalized_number, finalized_hash, finalized_block_commit.view[2], finalized_block_commit.commit_hash())
        }
    };
    let (high_view, high_digest) = synchronizer.revert(to_view, to_digest)?;
    Ok((to_block_number, to_block_hash, high_view, high_digest))
}

pub fn get_block_commit<B: BlockT, C: HeaderBackend<B>>(client: &Arc<C>, block_hash: B::Hash) -> Result<BlockCommit<B>, HotstuffError> {
    let block_header = client
        .header(block_hash)
        .map_err(|e| HotstuffError::ClientError(format!("{e:?}")))?
        .ok_or(HotstuffError::ClientError(format!("BlockHeader {block_hash} not stored in local!!!")))?;
    find_block_commit::<B>(&block_header)
        .ok_or(HotstuffError::ClientError(format!("Block ({})({block_hash}) have no commit!!!", block_header.number())))
}
