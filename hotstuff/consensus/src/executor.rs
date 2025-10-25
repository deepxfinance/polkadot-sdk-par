use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use codec::Encode;
use log::{debug, info, warn};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::RwLock;
use hotstuff_primitives::digests::CompatibleDigestItem;
use hotstuff_primitives::HOTSTUFF_ENGINE_ID;
use sc_basic_authorship::{BlockOracle, BlockPropose};
use sc_client_api::Backend;
use sc_consensus::{BlockImport, BlockImportParams, ForkChoiceStrategy, StateAction};
use sc_consensus_slots::{InherentDataProviderExt, StorageChanges};
use sp_api::{BlockT, HeaderT, TransactionFor};
use sp_consensus::{BlockOrigin, Environment, Error as ConsensusError, Proposer, SelectChain};
use sp_consensus_slots::{Slot, SlotDuration};
use sp_inherents::{CreateInherentDataProviders, InherentDataProvider};
use sp_runtime::{Digest, DigestItem, Saturating};
use sp_runtime::generic::BlockId;
use sp_timestamp::Timestamp;
use crate::client::ClientForHotstuff;
use crate::{find_block_commit, find_consensus_logs};
use crate::import::ImportLock;
use crate::message::{BlockCommit, Payload};
use crate::oracle::HotsOracle;

const LOG_TARGET: &str = "hots_executor";

pub enum ExecutorMission<B: BlockT> {
    Consensus(BlockCommit<B>, Payload<B>),
    Cancel(<B::Header as HeaderT>::Number),
}

pub struct BlockExecutor<C: ClientForHotstuff<B, BE>, B: BlockT, I, PF, L: sc_consensus::JustificationSyncLink<B>, BE: Backend<B>, CIDP, SC, O> {
    pub client: Arc<C>,
    pub proposer_factory: Arc<RwLock<PF>>,
    pub import: I,
    pub justification_sync_link: L,
    pub create_inherent_data_providers: CIDP,
    pub select_chain: SC,
    pub last_slot: Slot,
    pub slot_duration: SlotDuration,
    pub oracle: Arc<O>,
    pub executor_rx: UnboundedReceiver<ExecutorMission<B>>,
    pub missions: HashMap<<B::Header as HeaderT>::Number, (BlockCommit<B>, Payload<B>)>,
    pub block_size_limit: Option<usize>,
    phantom_data: PhantomData<BE>,
}

impl<C, B: BlockT, I, PF, L, BE: Backend<B>, Error, CIDP, SC, O> BlockExecutor<C, B, I, PF, L, BE, CIDP, SC, O>
where
    C: ClientForHotstuff<B, BE> + Send + Sync + 'static,
    I: BlockImport<B, Transaction = TransactionFor<C, B>> + ImportLock<B> + Send + Sync + 'static,
    PF: Environment<B, Error = Error> + Send + Sync + 'static,
    PF::Proposer: Proposer<B, Error = Error, Transaction = TransactionFor<C, B>>
        + BlockPropose<B, Transaction = TransactionFor<C, B>, Error = Error>
        + Send + Sync + 'static,
    L: sc_consensus::JustificationSyncLink<B>,
    Error: std::error::Error + Send + From<ConsensusError> + 'static,
    CIDP: CreateInherentDataProviders<B, Timestamp> + Send + 'static,
    CIDP::InherentDataProviders: InherentDataProviderExt + Send,
    SC: SelectChain<B>,
    O: BlockOracle<B> + HotsOracle + Sync + Send + 'static,
{
    pub fn new(
        client: Arc<C>,
        import: I,
        slot_duration: SlotDuration,
        oracle: Arc<O>,
        proposer_factory: Arc<RwLock<PF>>,
        justification_sync_link: L,
        create_inherent_data_providers: CIDP,
        select_chain: SC,
        executor_rx: UnboundedReceiver<ExecutorMission<B>>,
    ) -> Self {
        Self {
            client,
            oracle,
            proposer_factory,
            import,
            justification_sync_link,
            create_inherent_data_providers,
            select_chain,
            last_slot: 0.into(),
            slot_duration,
            executor_rx,
            missions: HashMap::new(),
            block_size_limit: None,
            phantom_data: PhantomData,
        }
    }

    pub async fn block_import_params(
        &self,
        header: B::Header,
        body: Vec<B::Extrinsic>,
        storage_changes: StorageChanges<TransactionFor<C, B>, B>,
        groups: Vec<u32>,
        commit: BlockCommit<B>,
        fork_choice: Option<ForkChoiceStrategy>,
    ) -> Result<BlockImportParams<B, TransactionFor<C, B>>, ConsensusError> {
        let mut import_block = BlockImportParams::new(BlockOrigin::ConsensusBroadcast, header);
        let groups_digest_item = <DigestItem as CompatibleDigestItem<Vec<u32>>>::hotstuff_seal(groups);
        import_block.post_digests.push(groups_digest_item);
        // claim for post_hash including `groups_digest_item`.
        let commit_digest_item = <DigestItem as CompatibleDigestItem<BlockCommit<B>>>::hotstuff_seal(commit);
        import_block.post_digests.push(commit_digest_item);
        import_block.body = Some(body);
        import_block.state_action =
            StateAction::ApplyChanges(sc_consensus::StorageChanges::Changes(storage_changes));
        import_block.fork_choice = match fork_choice {
            Some(choice) => Some(choice),
            None => Some(ForkChoiceStrategy::LongestChain)
        };

        Ok(import_block)
    }

    fn try_finalize_block(&mut self, commit: &BlockCommit<B>) {
        // try to finalize block
        let finalize_block = commit.base_block();
        if self.client.info().finalized_number >= finalize_block.number {
            return;
        }
        if let Ok(Some(header)) = self.client.header(finalize_block.hash) {
            // insert justification if authorities change.
            let mut justification = None;
            if !find_consensus_logs::<B>(&header).is_empty() {
                justification = Some((HOTSTUFF_ENGINE_ID, commit.encode()));
            }
            if let Err(e) = self.client.finalize_block(finalize_block.hash, justification, true) {
                warn!(target: LOG_TARGET, "FinalizeBlock #{} ({}) failed for {e:?}", finalize_block.number, finalize_block.hash);
            }
        }
    }

    async fn try_execute_all(&mut self) {
        let mut blocks = self.missions.keys().cloned().collect::<Vec<_>>();
        blocks.sort();
        let mut applied = vec![];
        for block in blocks {
            if let Some((commit, payload)) = self.missions.get(&block).cloned() {
                let best_block = self.client.info().best_number;
                let best_hash = self.client.info().best_hash;
                if payload.extrinsic.is_none() {
                    warn!(target: LOG_TARGET, "Skip next block number: {} for None extrinsic", commit.block_number);
                    self.import.unlock(BlockOrigin::ConsensusBroadcast, commit.block_number).await;
                    continue;
                }
                if self.client.info().finalized_number >= commit.block_number {
                    debug!(target: LOG_TARGET, "Skip next block number: {} for finalized {}", commit.block_number, self.client.info().finalized_number);
                    self.import.unlock(BlockOrigin::ConsensusBroadcast, commit.block_number).await;
                    applied.push(commit.block_number);
                    continue;
                }
                let mut reorg = false;
                if commit.block_number <= best_block {
                    if let Ok(Some(header)) = self.client.header(best_hash) {
                        if let Some(pre_commit) = find_block_commit::<B>(&header) {
                            if commit.digest() == pre_commit.digest() {
                                debug!(target: LOG_TARGET, "Skip same block {} with same commit already imported.", commit.block_number);
                                applied.push(commit.block_number);
                                continue;
                            }
                            if commit.commit.qc.view > pre_commit.commit.qc.view {
                                info!(target: LOG_TARGET, "Reorg re-execute block: {} view {} -> {}", commit.block_number, commit.commit.qc.view, pre_commit.commit.qc.view);
                                reorg = true;
                            } else {
                                debug!(target: LOG_TARGET, "Skip same block {} with earlier commit view {}/{}.", commit.block_number, commit.commit.qc.view, pre_commit.commit.qc.view);
                                continue;
                            }
                        }
                    }
                }
                self.try_finalize_block(&commit);
                let parent_hash = match self.client.block_hash_from_id(&BlockId::Number(commit.block_number.saturating_sub(1u32.into()))) {
                    Ok(Some(parent_hash)) => parent_hash,
                    Ok(None) => {
                        debug!(target: LOG_TARGET, "Skip next block number: {} for no parent header", commit.block_number);
                        continue;
                    },
                    Err(e) => {
                        debug!(target: LOG_TARGET, "Skip next block number: {} for get parent header error: {e:?}", commit.block_number);
                        continue;
                    }
                };
                let extrinsic = payload.extrinsic.clone().unwrap();
                // execute block
                // creat inherent data
                let inherent_data_providers = match self
                    .create_inherent_data_providers
                    .create_inherent_data_providers(parent_hash, *commit.commit_time())
                    .await
                {
                    Ok(x) => x,
                    Err(e) => {
                        warn!(target: LOG_TARGET, "Unable to author block in slot. Failure creating inherent data provider: {e}");
                        self.import.unlock(BlockOrigin::ConsensusBroadcast, payload.block_number).await;
                        continue
                    },
                };
                debug!(target: LOG_TARGET, "Best block number: {best_block}, next block number: {} start execute", payload.block_number);
                let slot = inherent_data_providers.slot();// Never yield the same slot twice.
                if slot <= self.last_slot {
                    warn!(target: LOG_TARGET, "Best block number: {best_block}, next block number: {} Skipped for last_slot {}, current_slot {slot}", payload.block_number, self.last_slot);
                    self.import.unlock(BlockOrigin::ConsensusBroadcast, payload.block_number).await;
                    applied.push(commit.block_number);
                    continue;
                }
                let inherent_data = inherent_data_providers.create_inherent_data().await.expect("create_inherent_data");

                let logs = vec![<DigestItem as CompatibleDigestItem<Slot>>::hotstuff_pre_digest(slot)];
                let parent_header = self
                    .client
                    .header(parent_hash)
                    .expect("get best header")
                    .expect("no expected best header");
                let proposer = self.proposer_factory.write().await.init(&parent_header).await.expect("proposer init");
                // let (proposal, groups, avg_execute_time) = match BlockPropose::<B>::propose_block(
                let (proposal, info) = match BlockPropose::<B>::propose_block(
                    proposer,
                    "Consensus",
                    best_hash,
                    best_block,
                    // actually we must execute all transactions, but we still limit time.
                    Duration::from_millis(self.slot_duration.as_millis() * 6),
                    self.oracle.linear_execute_time(),
                    self.oracle.merge_time(),
                    inherent_data,
                    Digest { logs },
                    extrinsic,
                    self.oracle.round_tx(),
                    true,
                    true,
                    false,
                ).await {
                    Ok(propose) => propose,
                    Err(e) => {
                        warn!(target: LOG_TARGET, "Propose block {} failed for {e:?}", payload.block_number);
                        self.import.unlock(BlockOrigin::ConsensusBroadcast, payload.block_number).await;
                        continue;
                    }
                };
                self.oracle.update_execute_info(&info);
                // generate import params
                let (block, _storage_proof) = (proposal.block, proposal.proof);
                let (header, body) = block.deconstruct();
                let block_import_params = match self.block_import_params(
                    header,
                    body.clone(),
                    proposal.storage_changes,
                    info.groups(),
                    commit.clone(),
                    if reorg {
                        Some(ForkChoiceStrategy::Custom(true))
                    } else {
                        None
                    },
                ).await {
                    Ok(import_params) => import_params,
                    Err(e) => {
                        warn!(target: LOG_TARGET, "Propose block {} get block_import_params failed for {e:?}", payload.block_number);
                        self.import.unlock(BlockOrigin::ConsensusBroadcast, payload.block_number).await;
                        continue;
                    }
                };
                // import block
                let header = block_import_params.post_header();
                // self.import.import_block will call `unlock` by self.
                let current = Timestamp::current().as_millis();
                if current < commit.commit_time().as_millis() {
                    tokio::time::sleep(std::time::Duration::from_millis(commit.commit_time().as_millis() - current)).await;
                }
                match self.import.import_block(block_import_params).await {
                    Ok(res) => {
                        res.handle_justification(
                            &header.hash(),
                            *header.number(),
                            &self.justification_sync_link,
                        );
                        // Since we allow ahead consensus, we do not skip following block event authorities changes.
                        self.last_slot = slot;
                        applied.push(commit.block_number);
                    },
                    Err(err) => {
                        warn!(target: LOG_TARGET, "Import block {}:{} built on {parent_hash:?}: {err}", header.number(), header.hash());
                    },
                }
            } else {
                break;
            }
        }
        for block in applied {
            self.missions.remove(&block);
        }
    }

    pub async fn run(&mut self) {
        loop {
            if let Some(mission) = self.executor_rx.recv().await {
                let mut missions = vec![mission];
                loop {
                    // try collect all mission to cancel mission before execute block.
                    match self.executor_rx.try_recv() {
                        Ok(mission) => missions.push(mission),
                        Err(_) => break,
                    }
                }
                for mission in missions {
                    match mission {
                        ExecutorMission::Consensus(commit, payload) => {
                            self.import.lock(BlockOrigin::ConsensusBroadcast, payload.block_number).await;
                            let block_number = payload.block_number;
                            if block_number > self.client.info().finalized_number {
                                // push new_block to missions.
                                if let Some(mission) = self.missions.get_mut(&block_number) {
                                    if commit.commit_time() >= mission.0.commit_time() {
                                        *mission = (commit, payload);
                                    }
                                } else {
                                    self.missions.insert(block_number, (commit, payload));
                                }
                            }
                        }
                        ExecutorMission::Cancel(block) => {
                            let remove_missions: Vec<_> = self.missions
                                .iter()
                                .filter_map(|(b, _)| if *b >= block { Some(*b) } else { None })
                                .collect();
                            for block in remove_missions {
                                self.missions.remove(&block);
                                self.import.unlock(BlockOrigin::ConsensusBroadcast, block).await;
                            }
                        }
                    }
                }
                self.try_execute_all().await;
            }
        }
    }
}
