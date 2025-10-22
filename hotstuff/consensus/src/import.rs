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
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::RwLock;
use hotstuff_primitives::digests::CompatibleDigestItem;
use hotstuff_primitives::HOTSTUFF_ENGINE_ID;
use sc_basic_authorship::BlockPropose;
use sc_client_api::{
	client::{FinalityNotifications, ImportNotifications},
	Backend, BlockImportNotification,
};
use sc_consensus::{BlockCheckParams, BlockImport, BlockImportParams, ImportResult, JustificationImport, StateAction, StorageChanges};
use sc_network::{PeerId, ReputationChange};
use sc_network_common::role::Role;
use sp_api::TransactionFor;
use sp_blockchain::BlockStatus;
use sp_consensus::{BlockOrigin, Environment, Error as ConsensusError, Proposer};
use sp_runtime::{generic::BlockId, traits::{Block as BlockT, Header as HeaderT, NumberFor, One}, Digest, Justification};
use crate::{client::ClientForHotstuff, find_block_commit, primitives::{HotstuffError, HotstuffError::*}, CLIENT_LOG_TARGET};
use crate::message::BlockCommit;

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

pub struct HotstuffBlockImport<Backend, Block: BlockT, Client, PF> {
	inner: Arc<Client>,
	role: Role,
	proposer_factory: Arc<RwLock<PF>>,
    import_lock: Arc<RwLock<HashMap<<Block::Header as HeaderT>::Number, BlockNotification<Block>>>>,
	backend: PhantomData<Backend>,
	_phantom: PhantomData<Block>,
}

impl<Backend, Block: BlockT, Client, PF> Clone for HotstuffBlockImport<Backend, Block, Client, PF> {
	fn clone(&self) -> Self {
		HotstuffBlockImport {
			inner: self.inner.clone(),
			role: self.role.clone(),
			proposer_factory: self.proposer_factory.clone(),
            import_lock: self.import_lock.clone(),
			backend: PhantomData,
			_phantom: PhantomData,
		}
	}
}

#[async_trait::async_trait]
impl<Backend, Block: BlockT, Client, PF> ImportLock<Block> for HotstuffBlockImport<Backend, Block, Client, PF>
where
    Backend: Send + Sync,
    Client: Send + Sync + 'static,
    PF: Send + Sync,
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
			log::trace!(target: CLIENT_LOG_TARGET, "{origin:?} lock block {number}");
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
            if let Err(e) = notification.notifier.send(()) {
                log::warn!(target: CLIENT_LOG_TARGET, "notification for block {number} execution result failed {e:?}");
            }
			log::trace!(target: CLIENT_LOG_TARGET, "{origin:?} unlock block {number}");
        }
    }
}

impl<Backend, Block: BlockT, Client, PF> HotstuffBlockImport<Backend, Block, Client, PF> {
	pub fn new(inner: Arc<Client>, role: Role, proposer_factory: Arc<RwLock<PF>>) -> HotstuffBlockImport<Backend, Block, Client, PF> {
		HotstuffBlockImport {
            inner,
            role,
            proposer_factory,
            backend: PhantomData,
            import_lock: Arc::new(RwLock::new(HashMap::new())),
            _phantom: PhantomData,
        }
	}
}

#[async_trait::async_trait]
impl<BE, Block: BlockT, Client, PF, Error> BlockImport<Block> for HotstuffBlockImport<BE, Block, Client, PF>
where
	BE: Backend<Block>,
	Client: ClientForHotstuff<Block, BE> + 'static,
	for<'a> &'a Client:
		BlockImport<Block, Error = ConsensusError, Transaction = TransactionFor<Client, Block>>,
	TransactionFor<Client, Block>: 'static,
    PF: Environment<Block, Error = Error> + Send + Sync + 'static,
    PF::Proposer: Proposer<Block, Error = Error, Transaction = TransactionFor<Client, Block>> + BlockPropose<Block>,
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
		mut block: BlockImportParams<Block, Self::Transaction>,
	) -> Result<ImportResult, Self::Error> {
		let calculate_block = |block: &BlockImportParams<Block, Self::Transaction>| {
			let post_header = block.post_header();
			let post_digests = post_header.digest();
			let post_digests_len = post_digests.logs().len();
			if post_digests_len < 3 {
				return Err(ConsensusError::ClientImport(format!("Insufficient post_digests: {post_digests_len}/3").into()));
			}
			let inherent_digest = Digest { logs: vec![post_digests.logs()[0].clone()] };
			let groups: Vec<u32> = post_digests.logs()[post_digests_len - 2].as_hotstuff_seal().ok_or(ConsensusError::ClientImport("Invalid post_digests for groups".into()))?;
			let parent_header = self.inner
				.header(*block.header.parent_hash())
				.map_err(|e| ConsensusError::ClientImport(format!("Get header for {:?} failed: {e:?}", block.header.parent_hash())))?
				.ok_or(ConsensusError::ClientImport(format!("No header for {:?}", block.header.parent_hash())))?;
			// TODO !!!MUST should we check actual extrinsic_root in header with BlockCommit in header.digest?
			futures::executor::block_on(async {
				self
					.proposer_factory
					.write()
					.await
					.init(&parent_header)
					.await
					.map_err(|e| ConsensusError::ClientImport(format!("init proposer: {e:?}")))?
					.execute_block_for_changes(
						"ImportBlock",
						block.origin.into(),
						*block.header.parent_hash(),
						inherent_digest,
						block.body.clone().unwrap_or_default(),
						groups,
						true,
						false,
					)
					.await
					.map_err(|e| Self::Error::ClientImport(format!("Execute block error {e:?}")))
			})
		};
		let hash = block.post_hash();
		log::trace!(
			target: CLIENT_LOG_TARGET,
			"ImportBlock({:?}): {}(parent: {:?}, body: {:?}, digests: {}, post_digests: {}, justifications: {:?}, finalized: {}, action: {})",
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
        self.lock(block.origin, *block.header.number()).await;
		let justifications = block.justifications.take();
		let block_commit = find_block_commit::<Block>(&block.post_header());
		let import_result = match self.inner.status(hash) {
			Ok(BlockStatus::InChain) => {
                self.unlock(block.origin, *block.header.number()).await;
				Ok(ImportResult::AlreadyInChain)
			},
			Ok(BlockStatus::Unknown) => {
				if let Ok(BlockStatus::InChain) = self.inner.status(*block.header.parent_hash()) {
					if matches!(block.origin, BlockOrigin::ConsensusBroadcast | BlockOrigin::Own)
						&& !matches!(block.state_action, StateAction::Execute | StateAction::ExecuteIfPossible)
					{
						// Block from local/local_consensus with calculated changes do not need check.
					} else if matches!(block.state_action, StateAction::Execute | StateAction::ExecuteIfPossible)
						|| self.role.is_authority()
					{
						let proposal = match calculate_block(&block) {
							Ok((proposal, _groups, _avg_execute_time)) => proposal,
							Err(e) => {
								log::warn!(target: CLIENT_LOG_TARGET, "ImportBlock({:?}): {} failed for {e}", block.origin, block.header.number());
                                self.unlock(block.origin, *block.header.number()).await;
								return Err(e);
							}
						};
						if let Err(e) = check_header::<Block>(&block.header, &proposal.block.header()) {
							log::warn!(target: CLIENT_LOG_TARGET, "ImportBlock({:?}): {} failed for {e}", block.origin, block.header.number());
							self.unlock(block.origin, *block.header.number()).await;
							return Err(ConsensusError::ClientImport(format!("Check header with calculated: {e}")));
						}
						block.state_action = StateAction::ApplyChanges(StorageChanges::Changes(proposal.storage_changes));
					}
                    let header = block.header.clone();
					let origin = block.origin;
					let result = (&*self.inner).import_block(block).await;
                    self.unlock(origin, *header.number()).await;
                    result
				} else {
                    self.unlock(block.origin, *block.header.number()).await;
					Ok(ImportResult::UnknownParent)
				}
			},
			Err(e) => Err(ConsensusError::ClientImport(e.to_string())),
		};
		// try to finalize base block
		if let Some(Some(encoded_commit)) = justifications.as_ref().map(|j| j.get(HOTSTUFF_ENGINE_ID)) {
			let commit: BlockCommit<Block> = match Decode::decode(&mut &encoded_commit[..]) {
				Ok(commit) => commit,
				Err(e) => return Err(ConsensusError::ClientImport(e.to_string())),
			};
			let finalize_block = commit.base_block();
			if self.inner.info().finalized_number < finalize_block.number {
				if let Err(e) = self.inner.finalize_block(finalize_block.hash, Some((HOTSTUFF_ENGINE_ID, encoded_commit.clone())), false) {
					log::warn!(target: CLIENT_LOG_TARGET, "[ImportBlock] FinalizeBlock #{} ({}) failed for {e:?}", finalize_block.number, finalize_block.hash);
				}
			}
		} else if let Some(commit) = block_commit {
			let finalize_block = commit.base_block();
			if self.inner.info().finalized_number < finalize_block.number {
				if let Err(e) = self.inner.finalize_block(finalize_block.hash, None, false) {
					log::warn!(target: CLIENT_LOG_TARGET, "[ImportBlock] FinalizeBlock #{} ({}) failed for {e:?}", finalize_block.number, finalize_block.hash);
				}
			}
		}
		import_result
	}
}

impl<BE, Block: BlockT, Client, PF> HotstuffBlockImport<BE, Block, Client, PF>
where
	BE: Backend<Block>,
	Client: ClientForHotstuff<Block, BE>,
    PF: Send + Sync + 'static,
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
				println!("🔥💃🏻 success finalize_block {_number:?}, block_hash: {hash:?}");
			},
			Err(err) => {
				println!("🔥💃🏻 finalize_block {_number:?}, block_hash: {hash:?} error: {:?}", err);
			},
		}

		Ok(())
	}
}

#[async_trait::async_trait]
impl<BE, Block: BlockT, Client, PF> JustificationImport<Block>
	for HotstuffBlockImport<BE, Block, Client, PF>
where
	BE: Backend<Block>,
	Client: ClientForHotstuff<Block, BE>,
    PF: Send + Sync + 'static,
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
							if let Some(b) = pending.pop_front() {
								log::info!(target: CLIENT_LOG_TARGET, "✨ Finalized #{} ({})", b.number, b.hash);
							}
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
