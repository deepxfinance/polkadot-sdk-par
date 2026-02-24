use std::{
	collections::VecDeque,
	marker::PhantomData,
	ops::Add,
	pin::Pin,
	sync::{Arc, Mutex},
	task::{Context, Poll},
};
use std::collections::HashMap;
use codec::{Encode, Decode};
use futures::{Future, StreamExt};
use log::warn;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::RwLock;
use hotstuff_primitives::{ConsensusLog, RuntimeAuthorityId};
use hotstuff_primitives::digests::CompatibleDigestItem;
use sc_basic_authorship::{BlockExecuteInfo, BlockOracle, BlockPropose};
use sc_client_api::{
	client::{FinalityNotifications, ImportNotifications},
	Backend, BlockImportNotification,
};
use sc_consensus::{BlockCheckParams, BlockImport, BlockImportParams, ForkChoiceStrategy, ImportResult, JustificationImport, StateAction, StorageChanges};
use sc_network::{PeerId, ReputationChange};
use sc_network_common::role::Role;
use sp_api::TransactionFor;
use sp_blockchain::BlockStatus;
use sp_consensus::{BlockOrigin, Error as ConsensusError};
use sp_runtime::{generic::BlockId, traits::{Block as BlockT, Header as HeaderT, NumberFor, One}, Digest, Justification, Saturating};
use crate::{client::ClientForHotstuff, find_block_commit, find_consensus_logs, error::{HotstuffError, HotstuffError::*}};
use crate::aux_schema::PersistentData;
use crate::message::BlockCommit;

const LOG_TARGET: &str = "hots_import";

pub struct BlockNotification<B: BlockT> {
	pub origin: BlockOrigin,
    pub notifier: Sender<()>,
    pub receiver: Option<Receiver<()>>,
    // TODO use B for better notification.
    phantom: PhantomData<B>,
}

#[async_trait::async_trait]
pub trait ImportLock<B: BlockT> {
    async fn lock(&self, origin: BlockOrigin, number: <B::Header as HeaderT>::Number);

    async fn unlock(&self, origin: BlockOrigin, number: <B::Header as HeaderT>::Number);
}

pub struct HotstuffBlockImport<Backend, Block: BlockT, Client, E, O> {
	inner: Arc<Client>,
	role: Role,
	executor: Arc<E>,
	oracle: Arc<O>,
	persistent_data: PersistentData<Block>,
    import_lock: Arc<RwLock<HashMap<<Block::Header as HeaderT>::Number, BlockNotification<Block>>>>,
	phantom: PhantomData<(Backend, Block)>,
}

impl<BE, Block: BlockT, Client, E, O, Error> HotstuffBlockImport<BE, Block, Client, E, O>
where
	BE: Backend<Block>,
	Client: ClientForHotstuff<Block, BE> + 'static,
	for<'a> &'a Client:
	BlockImport<Block, Error = ConsensusError, Transaction = TransactionFor<Client, Block>>,
	TransactionFor<Client, Block>: 'static,
    E: BlockPropose<Block, Transaction = TransactionFor<Client, Block>, Error = Error> + Send + Sync + 'static,
	O: BlockOracle<Block> + Sync + Send + 'static,
    Error: std::error::Error + Send + From<ConsensusError> + 'static,
{
	/// Return
	/// 	1. if we should import this new block
	/// 	2. if we should cover pre-imported same block number with different state by this new block.
	pub fn should_import(&self, new_header: &Block::Header, new_commit: &Option<BlockCommit<Block>>)
		-> Result<(bool, Option<(Block::Header, BlockCommit<Block>)>), sp_blockchain::Error>
	{
		let mut import = true;
		let mut reorg = None;
		if let Some(hash) = self.inner.block_hash_from_id(&BlockId::Number(*new_header.number()))? {
			if let Some(pre_header) = self.inner.header(hash)? {
				let pre_commit = find_block_commit::<Block>(&pre_header);
				match (pre_commit, new_commit) {
					(Some(pre_commit), Some(new_commit)) => {
						if pre_commit.view() > new_commit.view() {
							// new header is earlier, not import.
							import = false;
						} else if pre_commit.view() < new_commit.view() {
							// new header is later, re-import this new block.
							reorg = Some((pre_header, pre_commit));
						} else {
							// same commit view. not import again.
							import = false
						}
					}
					(Some(_), None) => import = false,
					(None, Some(_)) => if *pre_header.number() > 0u32.into() {
						log::error!(
							target: LOG_TARGET,
							"Block {}:{} have no commit!!!",
							pre_header.number(),
							pre_header.hash(),
						);
					}
					(None, None) => {
						// pre_header not valid, remove
						log::error!(
							target: LOG_TARGET,
							"Block {}:{} have no commit!!!",
							pre_header.number(),
							pre_header.hash(),
						);
						import = false;
					},
				}
			}
		}
		Ok((import, reorg))
	}

	pub async fn import_one(
		&self,
		mut block: BlockImportParams<Block, <Self as BlockImport<Block>>::Transaction>,
	) -> Result<(ImportResult, Option<BlockExecuteInfo<Block>>), <Self as BlockImport<Block>>::Error> {
		let calculate_block = |block: &BlockImportParams<Block, <Self as BlockImport<Block>>::Transaction>| {
			let post_header = block.post_header();
			let post_digests = post_header.digest();
			let post_digests_len = post_digests.logs().len();
			if post_digests_len < 3 {
				return Err(ConsensusError::ClientImport(format!("Insufficient post_digests: {post_digests_len}/3").into()));
			}
			let inherent_digest = Digest { logs: vec![post_digests.logs()[0].clone()] };
			let groups: Vec<u32> = post_digests.logs()[post_digests_len - 2].as_hotstuff_seal().ok_or(ConsensusError::ClientImport("Invalid post_digests for groups".into()))?;
			futures::executor::block_on(async {
				self
					.executor
					.execute_block_for_import(
						"NetImport",
						block.origin.into(),
						*block.header.parent_hash(),
						block.header.number().saturating_sub(1u32.into()),
						inherent_digest,
						block.body.clone().unwrap_or_default(),
						groups,
						// For check block, we should execute all transactions success by BlockBuilder::put_batch.
						usize::MAX,
						// doesn't work since `limit_execution_time` is false
						std::time::Duration::from_secs(10),
						// doesn't work since `limit_execution_time` is false
						std::time::Duration::from_secs(5),
						true,
						false,
					)
					.await
					.map_err(|e| <Self as BlockImport<Block>>::Error::ClientImport(format!("Execute block error {e:?}")))
			})
		};
		let start = std::time::Instant::now();
		let hash = block.post_hash();
		let _ = block.justifications.take();
		let block_commit = find_block_commit::<Block>(&block.post_header());
		let mut block_execute_info = None;
		let import_result = match self.inner.status(hash) {
			Ok(BlockStatus::InChain) => Ok(ImportResult::AlreadyInChain),
			Ok(BlockStatus::Unknown) => {
				let (import, reorg) = self.should_import(&block.header, &block_commit)
					.map_err(|e| ConsensusError::ClientImport(e.to_string()))?;
				if !import {
					Ok(ImportResult::AlreadyInChain)
				} else if let Ok(BlockStatus::InChain) = self.inner.status(*block.header.parent_hash()) {
					if matches!(block.origin, BlockOrigin::ConsensusBroadcast | BlockOrigin::Own)
						&& !matches!(block.state_action, StateAction::Execute | StateAction::ExecuteIfPossible)
					{
						// Block from local/local_consensus with calculated changes do not need check.
					} else if matches!(block.state_action, StateAction::Execute | StateAction::ExecuteIfPossible)
						|| self.role.is_authority()
					{
						let result = match calculate_block(&block) {
							Ok((proposal, info)) => {
								block_execute_info = Some(info);
								if let Err(e) = check_header::<Block>(&block.header, &proposal.block.header()) {
									Err(ConsensusError::ClientImport(format!("Check header with calculated: {e}")))
								} else {
									block.state_action = StateAction::ApplyChanges(StorageChanges::Changes(proposal.storage_changes));
									Ok(())
								}
							},
							Err(e) => Err(e),
						};
						if let Err(e) = result {
							warn!(target: LOG_TARGET, "{:?}: {} failed for {e}", block.origin, block.header.number());
							return Err(e);
						}
					}
					let header = block.post_header();
					if let Some((pre, pre_commit)) = reorg {
						log::warn!(
							target: LOG_TARGET,
							"Try replace best block #{}:{}({}) -> #{}:{}({})",
							pre.number(),
							pre.hash(),
							pre_commit.view(),
							header.number(),
							header.hash(),
							block_commit.as_ref().map(|bc| bc.view()).unwrap_or(0),
						);
						// This ForkChoiceStrategy::Custom(true) will set `is_new_best` to true.
						block.fork_choice = Some(ForkChoiceStrategy::Custom(true));
					}
					if let Some(commit) = block_commit {
						let mut authorities_updated = false;
						for consensus_log in find_consensus_logs::<Block, RuntimeAuthorityId>(&header) {
							match consensus_log {
								ConsensusLog::AuthoritiesPending(authorities, target_block) => {
									let authority_list = authorities.into_iter().map(|a| (a.into(), 0)).collect();
									self.persistent_data.authority_set.inner().update_pending_authorities(target_block, authority_list);
								}
								ConsensusLog::AuthoritiesChange(authorities) => {
									let authority_list = authorities.into_iter().map(|a| (a.into(), 0)).collect();
									self.persistent_data.authority_set.inner().update_authorities_change(commit.view(), commit.block_number(), authority_list);
									authorities_updated = true;
								}
								_ => {}
							}
						}
						if authorities_updated {
							crate::aux_schema::update_authority_set::<Block, _, _>(
								&self.persistent_data.authority_set.inner(),
								|insert| self.inner.insert_aux(insert, []),
							)
								.map_err(|e| ConsensusError::ClientImport(e.to_string()))?;
						}
					}
					let result = (&*self.inner).import_block(block).await;
					// TODO update persistent data if authority changes.
					if result.is_ok() && self.inner.info().finalized_number < *header.number() {
						if let Err(e) = self.inner.finalize_block(header.hash(), None, true) {
							warn!(target: LOG_TARGET, "FinalizeBlock #{} ({}) failed for {e:?}", header.number(), header.hash());
						}
					}
					let import_time = start.elapsed();
					log::debug!(target: LOG_TARGET, "ImportBlock #{} in {import_time:?}", header.number());
					block_execute_info.as_mut()
						.map(|info| info.set_import_time(import_time));
					result
				} else {
					Ok(ImportResult::UnknownParent)
				}
			},
			Err(e) => Err(ConsensusError::ClientImport(e.to_string())),
		};
		import_result.map(|r| (r, block_execute_info))
	}
}

impl<Backend, Block: BlockT, Client, E, O> Clone for HotstuffBlockImport<Backend, Block, Client, E, O> {
	fn clone(&self) -> Self {
		HotstuffBlockImport {
			inner: self.inner.clone(),
			role: self.role.clone(),
			executor: self.executor.clone(),
			oracle: self.oracle.clone(),
			persistent_data: self.persistent_data.clone(),
            import_lock: self.import_lock.clone(),
			phantom: PhantomData,
		}
	}
}

#[async_trait::async_trait]
impl<Backend, Block: BlockT, Client, E, O> ImportLock<Block> for HotstuffBlockImport<Backend, Block, Client, E, O>
where
    Backend: Send + Sync,
    Client: Send + Sync + 'static,
    E: Send + Sync,
	O: Send + Sync,
{
    async fn lock(&self, origin: BlockOrigin, number: <Block::Header as HeaderT>::Number) {
		// if locked by same origin, return for continue import.
        // if locked by different origin, wait for unlock notification.
        let mut receiver = None;
        let mut lock = self.import_lock.write().await;
        if let Some(notifier) = lock.get_mut(&number) {
			if origin == notifier.origin {
				return;
			}
            receiver = notifier.receiver.take();
        } else {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let notifier = BlockNotification {
				origin,
                notifier: sender,
                receiver: Some(receiver),
                phantom: PhantomData,
            };
            lock.insert(number, notifier);
        }
        drop(lock);
        if let Some(receiver) = receiver {
            let _ = receiver.await;
        } else {
			log::trace!(target: LOG_TARGET, "{origin:?} lock block #{number}");
		}
    }

    async fn unlock(&self, origin: BlockOrigin, number: <Block::Header as HeaderT>::Number) {
        // try to send unlock notification
        let mut lock = self.import_lock.write().await;
        if let Some(notification) = lock.remove(&number) {
			if origin != notification.origin {
				lock.insert(number, notification);
				return;
			}
            if let Err(_) = notification.notifier.send(()) {
                log::warn!(target: LOG_TARGET, "notification for block #{number} execution result failed");
            }
			log::trace!(target: LOG_TARGET, "{origin:?} unlock block #{number}");
        }
    }
}

impl<Backend, Block: BlockT, Client, E, O> HotstuffBlockImport<Backend, Block, Client, E, O> {
	pub fn new(inner: Arc<Client>, role: Role, executor: Arc<E>, oracle: Arc<O>, persistent_data: PersistentData<Block>) -> HotstuffBlockImport<Backend, Block, Client, E, O> {
		HotstuffBlockImport {
            inner,
            role,
            executor,
			oracle,
			persistent_data,
            import_lock: Arc::new(RwLock::new(HashMap::new())),
            phantom: PhantomData,
        }
	}
}

#[async_trait::async_trait]
impl<BE, Block: BlockT, Client, E, O, Error> BlockImport<Block> for HotstuffBlockImport<BE, Block, Client, E, O>
where
	BE: Backend<Block>,
	Client: ClientForHotstuff<Block, BE> + 'static,
	for<'a> &'a Client:
		BlockImport<Block, Error = ConsensusError, Transaction = TransactionFor<Client, Block>>,
	TransactionFor<Client, Block>: 'static,
    E: BlockPropose<Block, Transaction = TransactionFor<Client, Block>, Error = Error> + Send + Sync + 'static,
	O: BlockOracle<Block> + Sync + Send + 'static,
    Error: std::error::Error + Send + From<ConsensusError> + 'static,
{
	type Error = ConsensusError;
	type Transaction = TransactionFor<Client, Block>;

	async fn check_block(
		&mut self,
		block: BlockCheckParams<Block>,
	) -> Result<ImportResult, Self::Error> {
		self.inner.check_block(block).await
	}

	async fn import_block(
		&mut self,
		block: BlockImportParams<Block, Self::Transaction>,
	) -> Result<ImportResult, Self::Error> {
		log::debug!(
			target: LOG_TARGET,
			"ImportBlock({:?}): #{}(parent: {:?}, body: {:?}, digests: {}, post_digests: {}, justifications: {:?}, finalized: {}, action: {})",
			block.origin,
			block.header.number(),
			block.header.parent_hash(),
			block.body.as_ref().map(|e| e.len()),
			block.header.digest().logs().len(),
			block.post_digests.len(),
			block.justifications.as_ref().map(|justifications| justifications.encode().len()),
			block.finalized,
			match block.state_action {
				StateAction::ApplyChanges(StorageChanges::Changes(_)) => "ApplyChanges(Changes)",
				StateAction::ApplyChanges(StorageChanges::Import(_)) => "ApplyChanges(Import)",
				StateAction::Skip => "Skip",
				StateAction::ExecuteIfPossible => "ExecuteIfPossible",
				StateAction::Execute => "Execute",
			},
		);
		let orign = block.origin;
		let number = *block.header.number();
		self.lock(orign, number).await;
		let import_result = self.import_one(block).await;
		self.unlock(orign, number).await;
		import_result.map(|(r, mut block_execute_info)| {
			if let Some(info) = block_execute_info.take() {
				self.oracle.update_execute_info(info);
			}
			r
		})
	}
}

impl<BE, Block: BlockT, Client, E, O> HotstuffBlockImport<BE, Block, Client, E, O>
where
	BE: Backend<Block>,
	Client: ClientForHotstuff<Block, BE>,
    E: Send + Sync + 'static,
	O: Send + Sync + 'static,
{
	/// Import a block justification and finalize the block.
	///
	/// If `enacts_change` is set to true, then finalizing this block *must*
	/// enact an authority set change, the function will panic otherwise.
	fn import_justification(
		&mut self,
		hash: Block::Hash,
		_number: NumberFor<Block>,
		_justification: Justification,
		_enacts_change: bool,
		_initial_sync: bool,
	) -> Result<(), ConsensusError> {
		let client = self.inner.clone();
		let _status = client.info();

		let _res: Result<(), sp_blockchain::Error> = self.inner.finalize_block(hash, None, true);
		match _res {
			Ok(()) => {
				println!("🔥💃🏻 success finalize_block #{_number:?}, block_hash: {hash:?}");
			},
			Err(err) => {
				println!("🔥💃🏻 finalize_block #{_number:?}, block_hash: {hash:?} error: {:?}", err);
			},
		}

		Ok(())
	}
}

#[async_trait::async_trait]
impl<BE, Block: BlockT, Client, E, O> JustificationImport<Block>
	for HotstuffBlockImport<BE, Block, Client, E, O>
where
	BE: Backend<Block>,
	Client: ClientForHotstuff<Block, BE>,
    E: Send + Sync + 'static,
	O: Send + Sync + 'static,
{
	type Error = ConsensusError;

	async fn on_start(&mut self) -> Vec<(Block::Hash, NumberFor<Block>)> {
		let mut _out = Vec::new();
		let _chain_info = self.inner.info();
		// TODO
		_out
	}

	async fn import_justification(
		&mut self,
		hash: Block::Hash,
		number: NumberFor<Block>,
		justification: Justification,
	) -> Result<(), Self::Error> {
		// this justification was requested by the sync service, therefore we
		// are not sure if it should enact a change or not. it could have been a
		// request made as part of initial sync but that means the justification
		// wasn't part of the block and was requested asynchronously, probably
		// makes sense to log in that case.
		HotstuffBlockImport::import_justification(self, hash, number, justification, false, false)
	}
}

// /// Report specifying a reputation change for a given peer.
#[derive(Debug, PartialEq)]
pub(crate) struct PeerReport {
	pub who: PeerId,
	pub cost_benefit: ReputationChange,
}

#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub struct BlockInfo<B: BlockT> {
	pub hash: B::Hash,
	pub number: <B::Header as HeaderT>::Number,
}

impl<B: BlockT> Default for BlockInfo<B> {
	fn default() -> Self {
		BlockInfo {
			hash: Default::default(),
			number: 0u32.into(),
		}
	}
}

// A queue cache the best block from the client.
pub(crate) struct PendingFinalizeBlockQueue<B: BlockT> {
	import_notification: ImportNotifications<B>,

	// The finalize notification not guaranteed to be fired for every finalize block.
	finalize_notification: FinalityNotifications<B>,

	// A queue cache the block hash and block number which wait for finalize.
	inner: Arc<Mutex<VecDeque<BlockInfo<B>>>>,
}

impl<B: BlockT> PendingFinalizeBlockQueue<B> {
	pub fn new<BE: Backend<B>, C: ClientForHotstuff<B, BE>>(
		client: Arc<C>,
	) -> Result<Self, HotstuffError> {
		let chain_info = client.info();
		let mut block_number = chain_info.finalized_number.add(One::one());
		let mut queue = VecDeque::new();

		while block_number <= chain_info.best_number {
			let block_hash = client
				.block_hash_from_id(&BlockId::Number(block_number))
				.map_err(|e| ClientError(e.to_string()))?;

			queue.push_back((block_hash, block_number));
			block_number += One::one();
		}

		Ok(Self {
			import_notification: client.every_import_notification_stream(),
			finalize_notification: client.finality_notification_stream(),
			inner: Arc::new(Mutex::new(VecDeque::new())),
		})
	}
}

impl<B: BlockT> Unpin for PendingFinalizeBlockQueue<B> {}

impl<B: BlockT> Future for PendingFinalizeBlockQueue<B> {
	type Output = BlockImportNotification<B>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let mut result = None;
		loop {
			match StreamExt::poll_next_unpin(&mut self.import_notification, cx) {
				Poll::Ready(None) => break,
				Poll::Ready(Some(notification)) => {
					if !notification.is_new_best {
						continue;
					}

					if let Ok(mut pending) = self.inner.lock() {
						pending.push_back(BlockInfo {
							hash: notification.hash,
							number: notification.header.number().clone(),
						});
						result = Some(notification.clone());
					}
				},
				Poll::Pending => break,
			}
		}

		loop {
			match StreamExt::poll_next_unpin(&mut self.finalize_notification, cx) {
				Poll::Ready(None) => break,
				Poll::Ready(Some(notification)) => {
					if let Ok(mut pending) = self.inner.lock() {
						let finalized_number = notification.header.number();

						while let Some(elem) = pending.front() {
							if elem.number > *finalized_number {
								break;
							}
							let _ = pending.pop_front();
						}
					}
				},
				Poll::Pending => break,
			}
		}
		match result {
			Some(notification) => Poll::Ready(notification),
			None => Poll::Pending,
		}
	}
}

fn check_header<B: BlockT>(header: &B::Header, new_header: &B::Header) -> Result<(), String> {
	// check digest
	if header.digest().logs().len() != new_header.digest().logs().len() {
		return Err(format!("Number of digest items must match that calculated({}/{}).", new_header.digest().logs().len(), header.digest().logs().len()));
	}
	let items_zip = header.digest().logs().iter().zip(new_header.digest().logs().iter());
	for (item, new_item) in items_zip {
		if item != new_item {
			return Err("Digest item must match that calculated.".to_string());
		}
	}
	// check extrinsic root
	if header.extrinsics_root() != new_header.extrinsics_root() {
		return Err(format!("Transaction trie root must be valid({}/{}).", new_header.extrinsics_root(), header.extrinsics_root()));
	}
	// check storage root.
	if header.state_root() != new_header.state_root() {
		return Err(format!("Storage root must match that calculated({}/{}).", new_header.state_root(), header.state_root()));
	}
	Ok(())
}
