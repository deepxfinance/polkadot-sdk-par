use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use codec::Encode;
use log::{trace, debug, info, warn};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::RwLock;
use hotstuff_primitives::digests::CompatibleDigestItem;
use hotstuff_primitives::HOTSTUFF_ENGINE_ID;
use sc_basic_authorship::BlockPropose;
use sc_client_api::Backend;
use sc_consensus::{BlockImport, BlockImportParams, ForkChoiceStrategy, StateAction};
use sc_consensus_slots::{InherentDataProviderExt, StorageChanges};
use sp_api::{BlockT, HeaderT, TransactionFor};
use sp_consensus::{BlockOrigin, Environment, Error as ConsensusError, Proposer, SelectChain};
use sp_consensus_slots::{Slot, SlotDuration};
use sp_inherents::{CreateInherentDataProviders, InherentDataProvider};
use sp_runtime::{Digest, DigestItem, Percent, Saturating};
use sp_runtime::generic::BlockId;
use sp_timestamp::Timestamp;
use crate::client::ClientForHotstuff;
use crate::{find_block_commit, find_consensus_logs};
use crate::import::ImportLock;
use crate::message::{BlockCommit, Payload};

const LOG_TARGET: &str = "hots_executor";

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
    pub slot_duration: SlotDuration,
    pub execution_oracle: Arc<ExecutionOracle>,
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
        slot_duration: SlotDuration,
        execution_oracle: Arc<ExecutionOracle>,
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
            slot_duration,
            execution_oracle,
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
                let execution_start = std::time::Instant::now();
                let (proposal, groups, avg_execute_time) = match BlockPropose::<B>::propose_block(
                    proposer,
                    "Consensus",
                    // actually we must execute all transactions, but we still limit time.
                    std::time::Duration::from_millis(self.slot_duration.as_millis() * 6),
                    self.block_size_limit,
                    inherent_data,
                    Digest { logs },
                    extrinsic,
                    self.execution_oracle.execute_tx_per_millis(),
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
                self.execution_oracle.update_execute_micros_per_tx(avg_execute_time, execution_start.elapsed().as_millis());
                // generate import params
                let (block, _storage_proof) = (proposal.block, proposal.proof);
                let (header, body) = block.deconstruct();
                let block_import_params = match self.block_import_params(
                    header,
                    body.clone(),
                    proposal.storage_changes,
                    groups,
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

pub struct ExecutionOracle {
    /// max block execute time(default 200 millis, greater and queal than slot_duration).
    pub max_block_duration: u64,
    /// max hotstuff time decide extrinsic verify time tolerance(default 200 millis).
    pub hotstuff_duration: u64,
    /// hotstuff_duration * verify_percent decide actual extrinsic verify time.
    pub verify_percent: Percent,
    /// max_block_duration * execution_percent decide actual block extrinsic execution time.
    pub execution_percent: Percent,
    /// state recording last verify extrinsic speed.
    pub verify_micros_per_tx: Arc<Mutex<u128>>,
    /// state recording last block extrinsic execution speed.
    pub execute_micros_per_tx: Arc<Mutex<u128>>,
}

impl ExecutionOracle {
    pub fn new(
        max_block_duration: u64,
        hotstuff_duration: u64,
        verify_percent: Percent,
        execution_percent: Percent,
        verify_micros_per_tx: u128,
        execute_micros_per_tx: u128,
    ) -> Self {
        Self {
            max_block_duration,
            hotstuff_duration,
            verify_percent,
            execution_percent,
            verify_micros_per_tx: Arc::new(Mutex::new(verify_micros_per_tx)),
            execute_micros_per_tx: Arc::new(Mutex::new(execute_micros_per_tx)),
        }
    }

    // verify time should < slot_duration to finish consensus
    // limited extrinsic number for thread verify time.
    pub fn thread_verify_limit(&self) -> Option<usize> {
        let verify_micros_per_tx = *self.verify_micros_per_tx.lock().unwrap();
        if verify_micros_per_tx == 0 {
            return None;
        }
        // During full consensus process, verify extrinsic takes most time.
        // Default we use 75%  `HotstuffDuration` for verify extrinsic
        let verify_micros =  self.verify_percent.mul_floor(self.hotstuff_duration) * 1000;
        Some(verify_micros as usize / verify_micros_per_tx as usize)
    }

    // one thread execution limit (multi_max + single) to limit get extrinsic from pool.
    pub fn thread_execution_limit(&self) -> Option<usize> {
        let execute_micros_per_tx = *self.execute_micros_per_tx.lock().unwrap();
        if execute_micros_per_tx == 0 {
            return None;
        }
        // we allow full block execution time is slot_duration * 2.
        // and default 70 percent block time to execute extrinsic.
        let execution_micros =  self.execution_percent.mul_floor(self.max_block_duration) * 1000;
        Some(execution_micros as usize / execute_micros_per_tx as usize)
    }

    pub fn thread_tx_limit(&self) -> Option<usize> {
        let thread_tx_limit = match (self.thread_verify_limit(), self.thread_execution_limit()) {
            (None, execution_limit) => execution_limit,
            (verify_limit, None) => verify_limit,
            (Some(verify_limit), Some(execution_limit)) => {
                Some(verify_limit.min(execution_limit))
            }
        };
        trace!(target: "execution_oracle", "[Get] thread_tx_limit: {thread_tx_limit:?}");
        thread_tx_limit
    }

    // This value is used to decide each round execute extrinsic number.(execute_tx_per_millis * execution_millis)
    // But it will be covered  by env `MTH_DEFAULT_ROUND_TX` if it > 0.
    pub fn execute_tx_per_millis(&self) -> Option<usize> {
        let execute_micros_per_tx = *self.execute_micros_per_tx.lock().unwrap();
        let res = if execute_micros_per_tx == 0 {
            return None;
        } else {
            // if execute_micros_per_tx > 1000, we consider it as 1000.
            Some((1000 / execute_micros_per_tx as usize).max(1))
        };
        trace!(target: "execution_oracle", "[Get] execute_tx_per_millis: {res:?}");
        res
    }

    // each value at input should be (verify_number, verify_micros)
    pub fn update_verify_times(&self, verify_times: &Vec<(usize, u128)>) {
        let mut max_micros_per_verify = 0;
        for (number, micros) in verify_times {
            if *number == 0 {
                continue;
            }
            max_micros_per_verify = max_micros_per_verify.max(*micros / *number as u128);
        }
        if max_micros_per_verify > 0 {
            let pre_verify_micros_per_tx = *self.verify_micros_per_tx.lock().unwrap();
            if pre_verify_micros_per_tx != max_micros_per_verify {
                *self.verify_micros_per_tx.lock().unwrap() = max_micros_per_verify;
                debug!(target: "execution_oracle", "[Update] verify_micros_per_tx: {max_micros_per_verify}");
            }
        }
    }

    // `ave_execute_time`: average extrinsic execution time(micros).
    // `full_execution_time`: full block execution time(millis).
    pub fn update_execute_micros_per_tx(&self, ave_execute_time: u128, full_execution_time: u128) {
        let mut exceed_percent = None;
        let mut remain_percent = None;
        let mut execute_micros_per_tx = None;
        if ave_execute_time > 0 {
            let pre_execute_micros_per_tx = *self.execute_micros_per_tx.lock().unwrap();
            if ave_execute_time != pre_execute_micros_per_tx {
                *self.execute_micros_per_tx.lock().unwrap() = ave_execute_time;
                execute_micros_per_tx = Some(ave_execute_time);
            }
        }
        // since block execution timeout, we should slow down execution speed.
        if (full_execution_time as u64) > self.max_block_duration {
            let exceed_time_percent = Percent::from_rational(full_execution_time as u64 - self.max_block_duration, self.hotstuff_duration);
            let pre_execute_micros_per_tx = *self.execute_micros_per_tx.lock().unwrap();
            let new_execute_micros_per_tx = pre_execute_micros_per_tx + exceed_time_percent.mul_floor(pre_execute_micros_per_tx);
            if new_execute_micros_per_tx != pre_execute_micros_per_tx {
                *self.execute_micros_per_tx.lock().unwrap() = new_execute_micros_per_tx;
                execute_micros_per_tx = Some(new_execute_micros_per_tx);
            }
            if !exceed_time_percent.is_zero() {
                exceed_percent = Some(exceed_time_percent);
            }
        }
        if (full_execution_time as u64) < self.max_block_duration {
            let remain_time_percent = Percent::from_rational(self.max_block_duration - full_execution_time as u64, self.hotstuff_duration);
            let pre_execute_micros_per_tx = *self.execute_micros_per_tx.lock().unwrap();
            let new_execute_micros_per_tx = pre_execute_micros_per_tx - remain_time_percent.mul_floor(pre_execute_micros_per_tx);
            if new_execute_micros_per_tx != pre_execute_micros_per_tx {
                *self.execute_micros_per_tx.lock().unwrap() = new_execute_micros_per_tx;
                execute_micros_per_tx = Some(new_execute_micros_per_tx);
            }
            if !remain_time_percent.is_zero() {
                remain_percent = Some(remain_time_percent);
            }
        }
        if let Some(new_execute_micros_per_tx) = execute_micros_per_tx {
            let mut info = format!("[Update] new_execute_micros_per_tx: {new_execute_micros_per_tx}");
            if let Some(exceed_percent) = exceed_percent {
                info += &format!(", execution: {full_execution_time}/{} millis(timeout: {exceed_percent:?})", self.max_block_duration);
            } else if let Some(remain_percent) = remain_percent {
                info += &format!(", execution: {full_execution_time}/{} millis(remain: {remain_percent:?})", self.max_block_duration);
            }
            debug!(target: "execution_oracle", "{}", info);
        }
    }
}
