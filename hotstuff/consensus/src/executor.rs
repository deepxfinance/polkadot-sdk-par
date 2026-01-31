use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use codec::{Decode, Encode};
use log::{debug, error, trace, warn};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::RwLock;
use hotstuff_primitives::digests::CompatibleDigestItem;
use sc_basic_authorship::{BlockExecuteInfo, BlockOracle, BlockPropose};
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
use crate::find_block_commit;
use crate::import::ImportLock;
use crate::message::{BlockCommit, ExtrinsicBlock};
use crate::oracle::HotsOracle;

const LOG_TARGET: &str = "hots_executor";

#[derive(Clone, Debug, Encode, Decode)]
pub struct NewBlockMission<B: BlockT> {
    pub commit: BlockCommit<B>,
    pub block: ExtrinsicBlock<B>,
    pub extrinsics: Vec<Vec<Vec<B::Extrinsic>>>,
}

impl<B: BlockT> NewBlockMission<B> {
    pub fn empty() -> Self {
        Self {
            commit: BlockCommit::empty(),
            block: ExtrinsicBlock::empty(),
            extrinsics: Vec::new(),
        }
    }

    pub fn block_number(&self) -> <B::Header as HeaderT>::Number {
        self.commit.block_number()
    }
}

pub enum ExecutorMission<B: BlockT> {
    Consensus(NewBlockMission<B>),
    Imported(B::Header),
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
    pub pending: NewBlockMission<B>,
    pub missions: HashMap<<B::Header as HeaderT>::Number, NewBlockMission<B>>,
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
    O: BlockOracle<B> + HotsOracle<B> + Sync + Send + 'static,
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
            pending: NewBlockMission::empty(),
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

    // execute one block, return if remove this mission and execute info.
    async fn execute_mission(&mut self, mission: NewBlockMission<B>) -> Result<BlockExecuteInfo<B>, String> {
        let best_block = self.client.info().best_number;
        let best_hash = self.client.info().best_hash;
        let mission_block = mission.block_number();
        if mission_block > best_block.saturating_add(1u32.into()) {
            return Err(format!("skip next block number: {mission_block} for too far than best {best_block}"));
        }
        if self.client.info().best_number >= mission_block {
            return Err(format!("skip next block number: {mission_block} for best_block {}", self.client.info().best_number));
        }
        let parent_hash = match self.client.block_hash_from_id(&BlockId::Number(mission_block.saturating_sub(1u32.into()))) {
            Ok(Some(parent_hash)) => parent_hash,
            Ok(None) => {
                return Err(format!("skip next block number: {mission_block} for no parent hash"));
            },
            Err(e) => {
                return Err(format!("skip next block number: {mission_block} for get parent hash error: {e:?}"));
            }
        };
        // check parent block's commit qc hash. ensure correct chain fork.
        if mission_block > 1u32.into() {
            match self.client.header(parent_hash) {
                Ok(Some(parent_header)) => match find_block_commit::<B>(&parent_header) {
                    Some(parent_commit) => if parent_commit.commit_hash() != mission.commit.parent_commit_hash()
                        || mission_block != parent_commit.block_number().saturating_add(1u32.into()) {
                        return Err(format!(
                            "skip next block number: {mission_block} for no expected parent {} commit_qc_hash {} expect: {}",
                            parent_commit.block_number(),
                            parent_commit.commit_hash(),
                            mission.commit.parent_commit_hash(),
                        ));
                    },
                    None => {
                        return Err(format!("skip next block number: {mission_block} for no parent header have no commit!!!"));
                    }
                },
                Ok(None) => {
                    return Err(format!("skip next block number: {mission_block} for no parent header"));
                },
                Err(e) => {
                    return Err(format!("skip next block number: {mission_block} for get parent header error: {e:?}"));
                }
            }
        }
        // execute block
        // creat inherent datano state to check
        let inherent_data_providers = match self
            .create_inherent_data_providers
            .create_inherent_data_providers(parent_hash, *mission.commit.commit_time())
            .await
        {
            Ok(x) => x,
            Err(e) => {
                return Err(format!("Unable to author block in slot. Failure creating inherent data provider: {e}"));
            },
        };
        debug!(target: LOG_TARGET, "Start #{mission_block} with view {} (best {best_block}:{best_hash})", mission.commit.view());
        let slot = inherent_data_providers.slot();// Never yield the same slot twice.
        if slot <= self.last_slot {
            return Err(format!("Best block number: {best_block}, next block number: {mission_block} Skipped for last_slot {}, current_slot {slot}", self.last_slot));
        }
        let inherent_data = inherent_data_providers.create_inherent_data().await.expect("create_inherent_data");

        let logs = vec![<DigestItem as CompatibleDigestItem<Slot>>::hotstuff_pre_digest(slot)];
        let parent_header = self
            .client
            .header(parent_hash)
            .expect("get best header")
            .expect("no expected best header");
        let proposer = self.proposer_factory.write().await.init(&parent_header).await.expect("proposer init");
        let mut multi = vec![];
        let mut single = vec![];
        let extrinsic = mission.extrinsics.clone();
        if extrinsic.len() >= 1 {
            multi = extrinsic[0].clone();
        }
        if extrinsic.len() >= 2 && extrinsic[1].len() >= 1 {
            single = extrinsic[1][0].clone();
        }
        let (proposal, mut info) = match BlockPropose::<B>::propose_block(
            proposer,
            "Consensus",
            parent_hash,
            *parent_header.number(),
            // actually we must execute all transactions, but we still limit time.
            Duration::from_millis(self.slot_duration.as_millis() * 6),
            self.oracle.linear_execute_time(),
            self.oracle.merge_time(),
            inherent_data,
            Digest { logs },
            (multi, single),
            Some(mission.block.extrinsics_root),
            self.oracle.round_tx(),
            true,
            false,
        ).await {
            Ok(propose) => propose,
            Err(e) => {
                return Err(format!("Propose block {mission_block} failed for {e:?}"));
            }
        };
        let import_start = std::time::Instant::now();
        // generate import params
        let (block, _storage_proof) = (proposal.block, proposal.proof);
        let (header, body) = block.deconstruct();
        if *header.extrinsics_root() != mission.commit.extrinsics_root() {
            return Err(format!("Propose block {mission_block} check extrinsics_root incorrect calculated {} expected {}", header.extrinsics_root(), mission.commit.extrinsics_root()));
        }
        let block_import_params = match self.block_import_params(
            header,
            body.clone(),
            proposal.storage_changes,
            info.groups(),
            mission.commit.clone(),
            Some(ForkChoiceStrategy::Custom(true)),
        ).await {
            Ok(import_params) => import_params,
            Err(e) => {
                return Err(format!("Propose block {mission_block} get block_import_params failed for {e:?}"));
            }
        };
        // import block
        let header = block_import_params.post_header();
        let current = Timestamp::current().as_millis();
        if current < mission.commit.commit_time().as_millis() {
            tokio::time::sleep(std::time::Duration::from_millis(mission.commit.commit_time().as_millis() - current)).await;
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
                if self.client.info().finalized_number < *header.number() {
                    if let Err(e) = self.client.finalize_block(header.hash(), None, true) {
                        warn!(target: LOG_TARGET, "FinalizeBlock #{} ({}) failed for {e:?}", header.number(), header.hash());
                    }
                }
                info.import = import_start.elapsed();
                Ok(info)
            },
            Err(err) => {
                Err(format!("Import block {}:{} built on {parent_hash:?}: {err}", header.number(), header.hash()))
            },
        }
    }

    async fn try_execute_all(&mut self) {
        let mut blocks = self.missions.keys().cloned().collect::<Vec<_>>();
        blocks = blocks.into_iter().filter(|b| *b > self.client.info().best_number).collect();
        if blocks.len() <= 1 { return; }
        blocks.sort();
        let mut removes = vec![];
        for pending_block in &blocks[1..]  {
            let mission_block = pending_block.saturating_sub(1u32.into());
            let mission = match self.missions.get(pending_block) {
                Some(pending_mission) => match self.missions.get(&mission_block) {
                    Some(mission) => if pending_mission.commit.parent_commit_hash() == mission.commit.commit_hash() {
                        mission.clone()
                    } else {
                        debug!(target: LOG_TARGET, "break for invalid parent hash {} {}", pending_mission.commit.parent_commit_hash(), mission.commit.commit_hash());
                        break;
                    },
                    None => {
                        debug!(target: LOG_TARGET, "break for no parent mission for {}", pending_block);
                        break
                    },
                },
                None => {
                    error!(target: LOG_TARGET, "break for no pending block mission {pending_block}");
                    break
                },
            };
            self.import.lock(BlockOrigin::ConsensusBroadcast, mission_block).await;
            let result = self.execute_mission(mission).await;
            self.import.unlock(BlockOrigin::ConsensusBroadcast, mission_block).await;
            match result {
                Ok(info) => {
                    self.oracle.update_execute_info(&info);
                    removes.push(mission_block);
                },
                Err(e) => {
                    debug!(target: LOG_TARGET, "ExecuteFailed: {e}");
                }
            }
        }
        if !removes.is_empty() {
            trace!(target: LOG_TARGET, "execute removing blocks: {removes:?}");
        }
        for block in removes {
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
                        ExecutorMission::Consensus(new_mission) => {
                            let block_number = new_mission.block_number();
                            if block_number > self.client.info().finalized_number {
                                // push new_block to missions.
                                if let Some(mission) = self.missions.get_mut(&block_number) {
                                    if new_mission.commit.view() >= mission.commit.view() {
                                        trace!(target: LOG_TARGET, "Receive #{} with view {}, replace same block with view {:?}", new_mission.block_number(), new_mission.commit.view(), mission.commit.view());
                                        *mission = new_mission;
                                    }
                                } else {
                                    trace!(target: LOG_TARGET, "Receive #{} with view {}", new_mission.block_number(), new_mission.commit.view());
                                    self.missions.insert(block_number, new_mission);
                                }
                            }
                        }
                        ExecutorMission::Imported(header) => {
                            // remove all block missions l.e. than imported.
                            let removes = self.missions.keys().cloned().filter(|b| b <= header.number()).collect::<Vec<_>>();
                            for block in removes {
                                self.missions.remove(&block);
                            }
                        }
                        ExecutorMission::Cancel(block) => {
                            let remove_missions: Vec<_> = self.missions
                                .iter()
                                .filter_map(|(b, _)| if *b >= block { Some(*b) } else { None })
                                .collect();
                            for block in remove_missions {
                                self.missions.remove(&block);
                            }
                        }
                    }
                }
                self.try_execute_all().await;
            }
        }
    }
}
