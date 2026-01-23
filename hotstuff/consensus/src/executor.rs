pub mod types;

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
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
use sp_runtime::traits::NumberFor;
use sp_timestamp::Timestamp;
use types::{BlockMission, ExecutorMission, ImportMission, SafeImportMission};
use crate::client::ClientForHotstuff;
use crate::executor::types::SafeWrap;
use crate::find_block_commit;
use crate::import::ImportLock;
use crate::message::BlockCommit;
use crate::oracle::HotsOracle;

const LOG_TARGET: &str = "hots_executor";

pub struct BlockExecutor<C: ClientForHotstuff<B, BE>, B: BlockT, I, PF, L: sc_consensus::JustificationSyncLink<B>, BE: Backend<B>, CIDP, SC, O> {
    pub client: Arc<C>,
    pub proposer_factory: Arc<RwLock<PF>>,
    pub block_lock: I,
    pub justification_sync_link: L,
    pub create_inherent_data_providers: CIDP,
    pub select_chain: SC,
    pub last_slot: Slot,
    pub slot_duration: SlotDuration,
    pub oracle: Arc<O>,
    pub executor_rx: UnboundedReceiver<ExecutorMission<B>>,
    /// Block execution missions.
    pub missions: HashMap<NumberFor<B>, BlockMission<B>>,
    /// Block execution results to be imported.
    pub imports: HashMap<NumberFor<B>, SafeImportMission<B, C>>,
    /// Block confirms that decide the block can be imported.
    pub confirms: HashMap<NumberFor<B>, BlockCommit<B>>,
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
            block_lock: import,
            justification_sync_link,
            create_inherent_data_providers,
            select_chain,
            last_slot: 0.into(),
            slot_duration,
            executor_rx,
            missions: HashMap::new(),
            imports: HashMap::new(),
            confirms: HashMap::new(),
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
        commit: Option<BlockCommit<B>>,
        fork_choice: Option<ForkChoiceStrategy>,
    ) -> Result<BlockImportParams<B, TransactionFor<C, B>>, ConsensusError> {
        let mut import_block = BlockImportParams::new(BlockOrigin::ConsensusBroadcast, header);
        let groups_digest_item = <DigestItem as CompatibleDigestItem<Vec<u32>>>::hotstuff_seal(groups);
        import_block.post_digests.push(groups_digest_item);
        // claim for post_hash including `groups_digest_item`.
        if let Some(commit) = commit {
            let commit_digest_item = <DigestItem as CompatibleDigestItem<BlockCommit<B>>>::hotstuff_seal(commit);
            import_block.post_digests.push(commit_digest_item);
        }
        import_block.body = Some(body);
        import_block.state_action =
            StateAction::ApplyChanges(sc_consensus::StorageChanges::Changes(storage_changes));
        import_block.fork_choice = match fork_choice {
            Some(choice) => Some(choice),
            None => Some(ForkChoiceStrategy::LongestChain)
        };

        Ok(import_block)
    }

    /// Try to import block execute result
    /// If not confirmed
    /// Return:
    ///     `Ok(true)` if import success.
    ///     `Ok(false)` if not confirmed.
    ///     `Err(_)` if any other error.
    async fn try_import_block(&mut self, mut import: ImportMission<B, C>) -> Result<bool, String> {
        let best_block = self.client.info().best_number;
        if import.mission.block_number() <= best_block {
            // this may happen if block imported by Synchronization before we lock this.
            return Ok(true);
        }
        let confirm = match self.confirms.get(&import.mission.block_number()).cloned() {
            Some(confirm) => confirm,
            None => {
                // not confirmed, insert back to import list.
                self.imports.insert(import.mission.block_number(), SafeWrap::new(import));
                return Ok(false);
            }
        };
        import.confirm(&confirm)?;
        let header = import.import.post_header();

        // We estimate import time to import block ahead of `commit_time` if possible
        let commit_time = confirm.commit_time().as_millis();
        let import_time = self.oracle.import_time().as_millis() as u64;
        let import_start = commit_time.saturating_sub(import_time);
        let current = Timestamp::current().as_millis();
        if current < import_start {
            tokio::time::sleep(Duration::from_millis(import_start - current)).await;
        }
        match self.block_lock.import_block(import.import).await {
            Ok(res) => {
                res.handle_justification(
                    &header.hash(),
                    *header.number(),
                    &self.justification_sync_link,
                );
                // Since we allow ahead consensus, we do not skip following block event authorities changes.
                self.last_slot = import.slot;
                if self.client.info().finalized_number < *header.number() {
                    if let Err(e) = self.client.finalize_block(header.hash(), None, true) {
                        warn!(target: LOG_TARGET, "FinalizeBlock #{} ({}) failed for {e:?}", header.number(), header.hash());
                    }
                }
                // remove confirm.
                self.confirms.remove(header.number());
                Ok(true)
            },
            Err(err) => {
                Err(format!("Import block {}:{} {err}", header.number(), header.hash()))
            },
        }
    }

    async fn try_import_all(&mut self) {
        let mut mission_block = self.client.info().best_number.saturating_add(1u32.into());
        loop {
            let import = match self.imports.remove(&mission_block) {
                Some(mission) => mission,
                None => break,
            };
            self.block_lock.lock(BlockOrigin::ConsensusBroadcast, mission_block).await;
            match self.try_import_block(import.into_inner()).await {
                Ok(imported) => if !imported { break; }
                Err(e) => {
                    error!(target: LOG_TARGET, "Block {mission_block} import failed for {e}");
                    break;
                }
            }
            self.block_lock.unlock(BlockOrigin::ConsensusBroadcast, mission_block).await;
            mission_block = mission_block.saturating_add(1u32.into());
        }
    }

    // Execute one block and try import, return execute info.
    async fn execute_mission(&mut self, mission: BlockMission<B>) -> Result<BlockExecuteInfo<B>, String> {
        let best_block = self.client.info().best_number;
        let best_hash = self.client.info().best_hash;
        let mission_block = mission.block_number();
        if best_block >= mission_block {
            return Err(format!("skip old block #{mission_block} for best_block #{best_block}"));
        } else if mission_block != best_block.saturating_add(1u32.into()) {
            return Err(format!("skip next block #{mission_block} for too far than best #{best_block}"));
        }
        // check parent block's commit qc hash. ensure correct chain fork.
        if mission_block > 1u32.into() {
            match self.client.header(best_hash) {
                Ok(Some(parent_header)) => match find_block_commit::<B>(&parent_header) {
                    Some(parent_commit) => if parent_commit.commit_hash() != mission.parent_commit_hash()
                        || mission_block != parent_commit.block_number().saturating_add(1u32.into()) {
                        return Err(format!(
                            "skip next block: #{mission_block} for no expected parent #{} commit_qc_hash {}:{} expect: {}",
                            parent_commit.block_number(),
                            parent_commit.round(),
                            parent_commit.commit_hash(),
                            mission.parent_commit_hash(),
                        ));
                    },
                    None => {
                        return Err(format!("skip next block #{mission_block} for parent #{best_block} header have no commit!!!"));
                    }
                },
                Ok(None) => {
                    return Err(format!("skip next block #{mission_block} for no parent header"));
                },
                Err(e) => {
                    return Err(format!("skip next block #{mission_block} for get parent header error: {e:?}"));
                }
            }
        }
        // execute block
        // creat inherent data to check
        let inherent_data_providers = match self
            .create_inherent_data_providers
            .create_inherent_data_providers(best_hash, *mission.commit_time())
            .await
        {
            Ok(x) => x,
            Err(e) => {
                return Err(format!("Unable to author block in slot. Failure creating inherent data provider: {e}"));
            },
        };
        debug!(target: LOG_TARGET, "Start #{mission_block} with view {} (best {best_block}:{best_hash})", mission.view());
        let slot = inherent_data_providers.slot();// Never yield the same slot twice.
        if slot <= self.last_slot {
            return Err(format!("Best block number: {best_block}, next block number: {mission_block} Skipped for last_slot {}, current_slot {slot}", self.last_slot));
        }
        let inherent_data = inherent_data_providers.create_inherent_data().await.expect("create_inherent_data");

        let logs = vec![<DigestItem as CompatibleDigestItem<Slot>>::hotstuff_pre_digest(slot)];
        let parent_header = self
            .client
            .header(best_hash)
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
        let (proposal, info) = match BlockPropose::<B>::propose_block(
            proposer,
            "Consensus",
            best_hash,
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
        // generate import params
        let (block, _storage_proof) = (proposal.block, proposal.proof);
        let (header, body) = block.deconstruct();
        if *header.extrinsics_root() != mission.extrinsics_root() {
            return Err(format!("Propose block {mission_block} check extrinsics_root incorrect calculated {} expected {}", header.extrinsics_root(), mission.extrinsics_root()));
        }
        let block_import_params = match self.block_import_params(
            header,
            body.clone(),
            proposal.storage_changes,
            info.groups(),
            mission.block_commit(),
            Some(ForkChoiceStrategy::Custom(true)),
        ).await {
            Ok(import_params) => import_params,
            Err(e) => {
                return Err(format!("Propose block {mission_block} get block_import_params failed for {e:?}"));
            }
        };
        // try import block
        self.try_import_block(ImportMission::new(mission, slot, block_import_params)).await?;
        Ok(info)
    }

    async fn try_execute_all(&mut self) {
        loop  {
            let mission_block = self.client.info().best_number.saturating_add(1u32.into());
            let mission = match self.missions.get(&mission_block) {
                Some(mission) => mission.clone(),
                None => break,
            };

            // execute block and try import
            self.block_lock.lock(BlockOrigin::ConsensusBroadcast, mission_block).await;
            let result = self.execute_mission(mission).await;
            self.block_lock.unlock(BlockOrigin::ConsensusBroadcast, mission_block).await;
            match result {
                Ok(info) => {
                    self.oracle.update_execute_info(&info);
                    self.missions.remove(&mission_block);
                },
                Err(e) => {
                    warn!(target: LOG_TARGET, "ExecuteFailed: {e}");
                    break;
                }
            }
        }
    }

    fn retain(&mut self) {
        let best_number = self.client.info().best_number;
        self.missions.retain(|b, _| *b > best_number);
        self.imports.retain(|b, _| *b > best_number);
        self.confirms.retain(|b, _| *b > best_number);
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
                        ExecutorMission::Execute(mission) => {
                            let block = mission.block_number();
                            if block <= self.client.info().finalized_number { continue; }
                            if let Some(import) = self.imports.get(&block) {
                                if import.inner().mission.view() >= mission.view() {
                                    continue;
                                }
                            }
                            if let Some(pre_mission) = self.missions.get_mut(&block) {
                                if mission.view() >= pre_mission.view() {
                                    trace!(target: LOG_TARGET, "Receive {:?} #{block} with view {} -> {}", mission.mode(), pre_mission.view(), mission.view());
                                    *pre_mission = mission;
                                }
                            } else {
                                trace!(target: LOG_TARGET, "Receive {:?} #{block} with view {}", mission.mode(), mission.view());
                                self.missions.insert(block, mission);
                            }
                        }
                        ExecutorMission::Confirm(view, confirm) => {
                            let block = confirm.block_number();
                            if block <= self.client.info().finalized_number { continue; }
                            if let Some(pre_confirm) = self.confirms.get_mut(&block) {
                                if confirm.view() > pre_confirm.view() {
                                    trace!(target: LOG_TARGET, "Receive confirm for #{block} with view {view}");
                                    *pre_confirm = confirm;
                                }
                            } else {
                                trace!(target: LOG_TARGET, "Receive confirm for #{block} with view {view}");
                                self.confirms.insert(block, confirm);
                            }
                        },
                        ExecutorMission::Imported(header) => {
                            self.missions.retain(|b, _| b > header.number());
                            self.imports.retain(|b, _| b > header.number());
                            self.confirms.retain(|b, _| b > header.number());
                        }
                    }
                }
                self.try_import_all().await;
                self.try_execute_all().await;
                self.retain();
            }
        }
    }
}
