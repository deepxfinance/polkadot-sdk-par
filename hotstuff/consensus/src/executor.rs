use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use log::{debug, warn};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::RwLock;
use hotstuff_primitives::digests::CompatibleDigestItem;
use sc_basic_authorship::BlockPropose;
use sc_client_api::Backend;
use sc_consensus::{BlockImport, BlockImportParams, ForkChoiceStrategy, StateAction};
use sc_consensus_slots::{InherentDataProviderExt, StorageChanges};
use sp_api::{BlockT, HeaderT, TransactionFor};
use sp_consensus::{BlockOrigin, Environment, Error as ConsensusError, Proposer, SelectChain};
use sp_consensus_slots::Slot;
use sp_inherents::{CreateInherentDataProviders, InherentDataProvider};
use sp_runtime::{Digest, DigestItem, Saturating};
use sp_timestamp::Timestamp;
use crate::client::ClientForHotstuff;
use crate::{find_consensus_logs, CLIENT_LOG_TARGET};
use crate::import::ImportLock;
use crate::message::{BlockCommit, Payload};

pub enum ExecutorMission<B: BlockT> {
    Consensus(BlockCommit<B>, Payload<B>),
    Cancel(<B::Header as HeaderT>::Number),
}

pub struct BlockExecutor<C: ClientForHotstuff<B, BE>, B: BlockT, I, PF, L: sc_consensus::JustificationSyncLink<B>, BE: Backend<B>, CIDP, SC> {
    pub client: Arc<C>,
    pub proposer_factory: Arc<RwLock<PF>>,
    pub import: I,
    pub justification_sync_link: L,
    pub create_inherent_data_providers: CIDP,
    pub select_chain: SC,
    pub last_slot: Slot,
    pub executor_rx: UnboundedReceiver<ExecutorMission<B>>,
    pub missions: HashMap<<B::Header as HeaderT>::Number, (BlockCommit<B>, Payload<B>)>,
    pub block_size_limit: Option<usize>,
    phantom_data: PhantomData<BE>,
}

impl<C, B: BlockT, I, PF, L, BE: Backend<B>, Error, CIDP, SC> BlockExecutor<C, B, I, PF, L, BE, CIDP, SC>
where
    C: ClientForHotstuff<B, BE> + Send + Sync + 'static,
    I: BlockImport<B, Transaction = TransactionFor<C, B>> + ImportLock<B> + Send + Sync + 'static,
    PF: Environment<B, Error = Error> + Send + Sync + 'static,
    PF::Proposer: Proposer<B, Error = Error, Transaction = TransactionFor<C, B>> + BlockPropose<B>,
    L: sc_consensus::JustificationSyncLink<B>,
    Error: std::error::Error + Send + From<ConsensusError> + 'static,
    CIDP: CreateInherentDataProviders<B, Timestamp> + Send + 'static,
    CIDP::InherentDataProviders: InherentDataProviderExt + Send,
    SC: SelectChain<B>,
{
    pub fn new(
        client: Arc<C>,
        import: I,
        proposer_factory: Arc<RwLock<PF>>,
        justification_sync_link: L,
        create_inherent_data_providers: CIDP,
        select_chain: SC,
        executor_rx: UnboundedReceiver<ExecutorMission<B>>,
    ) -> Self {
        Self {
            client,
            proposer_factory,
            import,
            justification_sync_link,
            create_inherent_data_providers,
            select_chain,
            last_slot: 0.into(),
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
        import_block.fork_choice = Some(ForkChoiceStrategy::LongestChain);

        Ok(import_block)
    }

    async fn try_execute_all(&mut self) {
        loop {
            let chain_head = match self.select_chain.best_chain().await {
                Ok(x) => x,
                Err(e) => {
                    warn!(target: CLIENT_LOG_TARGET, "[Execute] Unable to author block in slot. No best block header: {e}");
                    continue
                },
            };
            let best_block = chain_head.number().clone();
            let best_hash = chain_head.hash();
            let next_block_number = best_block.saturating_add(1u32.into());
            self.missions.remove(&best_block);
            if let Some((commit, mut payload)) = self.missions.remove(&next_block_number) {
                if payload.extrinsic.is_none() {
                    warn!(target: CLIENT_LOG_TARGET, "[Execute] Next block number: {} skipped for None extrinsic", payload.block_number);
                    self.import.unlock(BlockOrigin::ConsensusBroadcast, payload.block_number).await;
                    continue;
                }
                let extrinsic = payload.extrinsic.take().unwrap();
                // execute block
                // creat inherent data
                let inherent_data_providers = match self
                    .create_inherent_data_providers
                    .create_inherent_data_providers(chain_head.hash(), *commit.commit_time())
                    .await
                {
                    Ok(x) => x,
                    Err(e) => {
                        warn!(target: CLIENT_LOG_TARGET, "[Execute] Unable to author block in slot. Failure creating inherent data provider: {e}");
                        self.import.unlock(BlockOrigin::ConsensusBroadcast, payload.block_number).await;
                        continue
                    },
                };
                debug!(target: CLIENT_LOG_TARGET, "[Execute] Best block number: {best_block}, next block number: {} start execute", payload.block_number);
                let slot = inherent_data_providers.slot();// Never yield the same slot twice.
                if slot > self.last_slot {
                    self.last_slot = slot;
                } else {
                    warn!(target: CLIENT_LOG_TARGET, "[Execute] Best block number: {best_block}, next block number: {} skipped for last_slot {}, current_slot {slot}", payload.block_number, self.last_slot);
                    self.import.unlock(BlockOrigin::ConsensusBroadcast, payload.block_number).await;
                    continue;
                }
                let inherent_data = inherent_data_providers.create_inherent_data().await.expect("create_inherent_data");

                let logs = vec![<DigestItem as CompatibleDigestItem<Slot>>::hotstuff_pre_digest(slot)];
                let parent_header = self
                    .client
                    .header(best_hash)
                    .expect("get best header")
                    .expect("no expected best header");
                let proposer = self.proposer_factory.write().await.init(&parent_header).await.expect("proposer init");
                let (proposal, groups) = match BlockPropose::<B>::propose_block(
                    proposer,
                    self.block_size_limit,
                    inherent_data,
                    Digest { logs },
                    extrinsic,
                    true,
                ).await {
                    Ok(propose) => propose,
                    Err(e) => {
                        warn!(target: CLIENT_LOG_TARGET, "[Execute] Propose block {} failed for {e:?}", payload.block_number);
                        self.import.unlock(BlockOrigin::ConsensusBroadcast, payload.block_number).await;
                        continue;
                    }
                };
                // generate import params
                let (block, _storage_proof) = (proposal.block, proposal.proof);
                let (header, body) = block.deconstruct();
                let block_import_params = match self.block_import_params(
                    header,
                    body.clone(),
                    proposal.storage_changes,
                    groups,
                    commit,
                ).await {
                    Ok(import_params) => import_params,
                    Err(e) => {
                        warn!(target: CLIENT_LOG_TARGET, "[Execute] Propose block {} get block_import_params failed for {e:?}", payload.block_number);
                        self.import.unlock(BlockOrigin::ConsensusBroadcast, payload.block_number).await;
                        continue;
                    }
                };
                // import block
                let header = block_import_params.post_header();
                // self.import.import_block will call `unlock` by self.
                match self.import.import_block(block_import_params).await {
                    Ok(res) => {
                        res.handle_justification(
                            &header.hash(),
                            *header.number(),
                            &self.justification_sync_link,
                        );
                        if !find_consensus_logs::<B>(&header).is_empty() {
                            // if imported block have any ConsensusLog(authorities change)
                            // we should not execute following blocks(may be re-processed).
                            break;
                        }
                    },
                    Err(err) => {
                        warn!(target: CLIENT_LOG_TARGET, "[Execute] Error with block {} built on {best_hash:?}: {err}", payload.block_number);
                    },
                }
            } else {
                break;
            }
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
