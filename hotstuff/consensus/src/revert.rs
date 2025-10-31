use std::sync::Arc;
use sc_client_api::AuxStore;
use sp_api::{BlockT, HeaderT};
use sp_blockchain::HeaderBackend;
use crate::find_block_commit;
use crate::message::BlockCommit;
use crate::primitives::HotstuffError;
use crate::synchronizer::Synchronizer;

pub fn revert<B, C>(client: &Arc<C>) -> Result<(), HotstuffError>
where
    B: BlockT,
    C: AuxStore + HeaderBackend<B>,
{
    let mut synchronizer = Synchronizer::<B, C>::new(client.clone());
    let best_block_number = client.info().best_number;
    let best_block_hash = client.info().best_hash;
    let best_block_commit = get_block_commit(client, best_block_hash)?;
    let (high_view, high_digest) = synchronizer.revert(best_block_commit.view(), best_block_commit.commit_hash())?;
    println!("HotstuffRevert to block #{best_block_number} ({best_block_hash:?}), high_view {high_view}, high_proposal_digest {high_digest:?}");
    Ok(())
}

pub fn get_block_commit<B: BlockT, C: HeaderBackend<B>>(client: &Arc<C>, block_hash: B::Hash) -> Result<BlockCommit<B>, HotstuffError> {
    let block_header = client
        .header(block_hash)
        .map_err(|e| HotstuffError::ClientError(format!("{e:?}")))?
        .ok_or(HotstuffError::ClientError(format!("BlockHeader {block_hash} not stored in local!!!")))?;
    find_block_commit::<B>(&block_header)
        .ok_or(HotstuffError::ClientError(format!("Block ({})({block_hash}) have no commit!!!", block_header.number())))
}
