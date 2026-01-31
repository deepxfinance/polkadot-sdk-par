use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time;
use std::time::Duration;
use codec::Encode;
use futures::{SinkExt, StreamExt};
use futures::channel::mpsc::{Receiver, Sender, channel};
use log::{debug, error, info, trace, warn};
use sc_block_builder::{BlockBuilder, BlockBuilderApi, BlockBuilderProvider};
use sc_client_api::{backend, ChildInfo, StorageProof};
use sc_client_api::execution_extensions::ExecutionStrategies;
use sc_proposer_metrics::{EndProposingReason, MetricsLink as PrometheusMetrics};
use sc_telemetry::{telemetry, TelemetryHandle, CONSENSUS_INFO};
use sc_transaction_pool_api::TransactionPool;
use sp_api::{ApiExt, BlockT, CallApiAt, HeaderT, ProofRecorder, ProvideRuntimeApi, StorageTransactionCache};
use sp_blockchain::ApplyExtrinsicFailed::Validity;
use sp_blockchain::Error::ApplyExtrinsicFailed;
use sp_blockchain::HeaderBackend;
use sp_consensus::{ProofRecording, Proposal};
use sp_core::ExecutionContext;
use sp_core::traits::SpawnNamed;
use sp_inherents::InherentData;
use sp_perp_api::PerpRuntimeApi;
use sp_runtime::{traits, Digest, Percent, SaturatedConversion, Saturating};
use sp_runtime::traits::{Hash, Header, NumberFor, One};
use sp_spot_api::SpotRuntimeApi;
use sp_state_machine::{Changes, ExecutionStrategy, MergeErr, OverlayedChanges, OverlayedEntry, StorageKey, StorageValue};
use sp_state_machine::backend::Consolidate;
use crate::{BlockPropose, ExtraExecute, MultiThreadBlockBuilder};
use crate::mth_authorship::execute_info::{BlockExecuteInfo, InfoRecorder, MergeInfo, ThreadExecutionInfo};

const LOG_TARGET: &str = "authorship";

type MergeType<A, B, C> = (
    Vec<usize>,
    OverlayedChanges,
    StorageTransactionCache<B, <<C as ProvideRuntimeApi<B>>::Api as ApiExt<B>>::StateBackend>,
    Option<ProofRecorder<B>>,
    Vec<<B as BlockT>::Extrinsic>,
    Vec<<A as TransactionPool>::Hash>,
    EndProposingReason,
);

pub struct BlockExecutor<B, C, A, PR, MBH, E> {
    spawn_handle: Box<dyn SpawnNamed>,
    client: Arc<C>,
    transaction_pool: Arc<A>,
    native_version: sp_version::NativeVersion,
    execution_strategies: ExecutionStrategies,
    metrics: PrometheusMetrics,
    telemetry: Option<TelemetryHandle>,
    phantom: PhantomData<(B, PR, MBH, E)>,
}

impl<A, B, Block, C, PR, MBH, E> BlockExecutor<B, C, A, PR, MBH, E>
where
    A: TransactionPool<Block = Block, Hash = Block::Hash>,
    <A as TransactionPool>::InPoolTransaction: Send + Sync + 'static,
    B: backend::Backend<Block> + Send + Sync + 'static,
    Block: BlockT,
    C: BlockBuilderProvider<B, Block, C>
    + HeaderBackend<Block>
    + ProvideRuntimeApi<Block>
    + CallApiAt<Block>
    + Send
    + Sync
    + 'static,
    C::Api:
    ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>> + BlockBuilderApi<Block> + SpotRuntimeApi<Block> + PerpRuntimeApi<Block>,
    PR: ProofRecording,
    MBH: MultiThreadBlockBuilder<B, Block, C::Api> + Send + Sync + 'static,
    E: ExtraExecute<Block, C::Api> + Send + Sync + 'static,
{
    pub fn new(
        spawn_handle: Box<dyn SpawnNamed>,
        client: Arc<C>,
        transaction_pool: Arc<A>,
        native_version: sp_version::NativeVersion,
        execution_strategies: ExecutionStrategies,
        metrics: PrometheusMetrics,
        telemetry: Option<TelemetryHandle>,
    ) -> Self {
        Self {
            spawn_handle,
            client,
            transaction_pool,
            native_version,
            execution_strategies,
            metrics,
            telemetry,
            phantom: PhantomData,
        }
    }

    pub fn strategy(&self, context: &ExecutionContext) -> ExecutionStrategy {
        match context {
            ExecutionContext::Importing => self.execution_strategies.importing,
            ExecutionContext::Syncing => self.execution_strategies.syncing,
            ExecutionContext::BlockConstruction => self.execution_strategies.block_construction,
            ExecutionContext::OffchainCall(_) => self.execution_strategies.offchain_worker,
        }
    }

    pub async fn execute_block<'a>(
        &self,
        context: ExecutionContext,
        parent_hash: Block::Hash,
        parent_number: <<Block as BlockT>::Header as HeaderT>::Number,
        propose_with_start: time::Instant,
        deadline: time::Instant,
        inherents: Vec<Block::Extrinsic>,
        inherent_digests: Digest,
        mut block_builder: BlockBuilder<'a, Block, C, B>,
        linear_execute_time: Duration,
        estimated_merge_time: Duration,
        round_tx: usize,
        mut extrinsic_group: Vec<Vec<(usize, <A as TransactionPool>::Hash, Block::Extrinsic)>>,
        mut single_group: Vec<(usize, <A as TransactionPool>::Hash, Block::Extrinsic)>,
        set_extrinsics_root: Option<Block::Hash>,
        merge_in_thread_order: bool,
        limit_execution_time: bool,
    ) -> Result<(Proposal<Block, backend::TransactionFor<B, Block>, PR::Proof>, BlockExecuteInfo<Block>), sp_blockchain::Error> {
        let thread_number = extrinsic_group.len();
        let max_duration = deadline.saturating_duration_since(propose_with_start);
        let mut recorder = InfoRecorder::<Block>::new()
            .parent_hash(parent_hash)
            .number(parent_number + One::one())
            .max_time(max_duration)
            .strategy(self.strategy(&context))
            .start();
        // if native version not latest, we can only execute in single thread.
        let onchain_version = CallApiAt::runtime_version_at(&*self.client, parent_hash)
            .map_err(|e| sp_blockchain::Error::RuntimeApiError(e))?;
        recorder.version_match(onchain_version.can_call_with(&self.native_version.runtime_version));
        if extrinsic_group.len() == 1 {
            single_group = [extrinsic_group.pop().unwrap(), single_group].concat();
        }
        // 1. initialize main thread block_builder by inherent transactions.
        let inherent_start = time::Instant::now();
        let mut inherent_info = ThreadExecutionInfo::default();
        let inherent_length = inherents.len();
        for inherent in inherents.clone() {
            match block_builder.push(inherent) {
                Err(ApplyExtrinsicFailed(Validity(e))) if e.exhausted_resources() => {
                    warn!("⚠️  Dropping non-mandatory inherent from overweight block.")
                },
                Err(ApplyExtrinsicFailed(Validity(e))) if e.was_mandatory() => {
                    error!("❌️ Mandatory inherent extrinsic returned error. Block cannot be produced.");
                    return Err(ApplyExtrinsicFailed(Validity(e)));
                },
                Err(e) => {
                    warn!("❗️ Inherent extrinsic returned unexpected error: {}. Dropping.", e);
                },
                Ok(_) => {},
            }
        }
        inherent_info.total = inherent_length;
        inherent_info.time = inherent_start.elapsed();
        recorder.inherent_finish(inherent_info);

        #[cfg(feature = "dev-time")]
        {
            use frame_support::storage::unhashed;

            unhashed::GLOBAL_ENCODE.lock().unwrap().clear();
            unhashed::GLOBAL_DECODE.lock().unwrap().clear();
            sp_state_machine::GET.lock().unwrap().clear();
            sp_state_machine::PUT.lock().unwrap().clear();
            sp_state_machine::ENCODE.lock().unwrap().clear();
        }
        // 2. execute main process for multi thread and single thread.
        let (mth_invalid, single_invalid, final_end_reason) = self.multi_single_process(
            parent_number,
            &mut block_builder,
            round_tx,
            linear_execute_time.as_micros(),
            estimated_merge_time,
            extrinsic_group,
            single_group,
            inherents.clone(),
            merge_in_thread_order,
            limit_execution_time,
            &mut recorder,
        ).await?;
        #[cfg(feature = "dev-time")]
        if block_builder.extrinsics.len() > 1 {
            Self::collect_encode_decode(format!("Block {}", recorder.number).as_str());
        }
        self.transaction_pool.remove_invalid(&mth_invalid);
        self.transaction_pool.remove_invalid(&single_invalid);

        // 5. build block by finalize block.
        recorder.finalize_start();
        let (mut block, storage_changes, proof) = block_builder.build()?.into_inner();
        if let Some(extrinsics_root) = set_extrinsics_root {
            block.header_mut().set_extrinsics_root(extrinsics_root);
        } else if block.header().extrinsics_root() == &Default::default() {
            let extrinsics_root = extrinsics_root::<<Block::Header as HeaderT>::Hashing, <Block as BlockT>::Extrinsic>(&block.extrinsics()[1..]);
            block.header_mut().set_extrinsics_root(extrinsics_root);
        }
        recorder.set_state_hash(*block.header().state_root());
        recorder.set_hash(block.header().hash());
        let info = recorder.finalize();

        // 6. spawn a single execution check if env `MTH_CHECK` is true, this is just an extra check.
        let single_thread_check = std::env::var("MTH_CHECK").unwrap_or("false".into()).parse().unwrap_or(false);
        if single_thread_check && thread_number > 0 {
            #[cfg(feature = "kvdb")]
            Self::one_thread_build_check(
                self.client.clone(),
                inherent_digests,
                block.clone(),
                storage_changes.main_storage_changes.clone(),
                storage_changes.child_storage_changes.clone(),
                proof.clone(),
            )
                .await;
            #[cfg(not(feature = "kvdb"))]
            {
                let client = self.client.clone();
                let inherent_digests = inherent_digests.clone();
                let block = block.clone();
                let main_storage_changes = storage_changes.main_storage_changes.clone();
                let child_storage_changes = storage_changes.child_storage_changes.clone();
                let proof = proof.clone();
                self.spawn_handle.spawn(
                    "mth-authorship-proposer",
                    None,
                    Box::pin(async move {
                        Self::one_thread_build_check(
                            client,
                            inherent_digests,
                            block,
                            main_storage_changes,
                            child_storage_changes,
                            proof,
                        )
                            .await
                    })
                );
            }
        }

        self.metrics.report(|metrics| {
            metrics.number_of_transactions.set(block.extrinsics().len() as u64);
            metrics.block_constructed.observe(propose_with_start.elapsed().as_secs_f64());
            metrics.report_end_proposing_reason(final_end_reason);
        });

        let proof =
            PR::into_proof(proof).map_err(|e| sp_blockchain::Error::Application(Box::new(e)))?;
        let propose_with_end = time::Instant::now();
        self.metrics.report(|metrics| {
            metrics.create_block_proposal_time.observe(
                propose_with_end.saturating_duration_since(propose_with_start).as_secs_f64(),
            );
        });

        Ok((Proposal { block, proof, storage_changes }, info))
    }

    async fn multi_single_process<'a>(
        &self,
        parent_number: <<Block as BlockT>::Header as HeaderT>::Number,
        block_builder: &mut BlockBuilder<'a, Block, C, B>,
        round_tx: usize,
        multi_single_micros: u128,
        estimated_merge_time: time::Duration,
        multi_groups: Vec<Vec<(usize, <A as TransactionPool>::Hash, Block::Extrinsic)>>,
        single_group: Vec<(usize, <A as TransactionPool>::Hash, Block::Extrinsic)>,
        inherents: Vec<Block::Extrinsic>,
        merge_in_thread_order: bool,
        limit_execution_time: bool,
        recorder: &mut InfoRecorder<Block>,
    ) -> Result<
        (Vec<<A as TransactionPool>::Hash>, Vec<<A as TransactionPool>::Hash>, EndProposingReason),
        sp_blockchain::Error
    > {
        if multi_groups.is_empty() && single_group.is_empty() {
            return Ok((Vec::new(), Vec::new(), EndProposingReason::NoMoreTransactions))
        }
        let calculate_start = time::Instant::now();
        let mut max_mth_length = 0u32;
        multi_groups.iter().for_each(|g| max_mth_length = max_mth_length.max(g.len() as u32));

        let merge_deadline_time = estimated_merge_time.as_micros();
        let execute_time: u64 = multi_single_micros.saturating_sub(merge_deadline_time).saturated_into();

        let mth_deadline_percent = Percent::from_rational(max_mth_length, max_mth_length + single_group.len() as u32);
        let single_deadline_percent = Percent::one().saturating_sub(mth_deadline_percent);
        let mth_execute_time = time::Duration::from_micros(mth_deadline_percent.mul_floor(execute_time));
        let mth_merge_time = time::Duration::from_micros(merge_deadline_time.saturated_into());
        let single_time = time::Duration::from_micros(single_deadline_percent.mul_floor(execute_time));

        let mth_execute_deadline = calculate_start + mth_execute_time;
        let mth_finish_deadline = mth_execute_deadline + mth_merge_time;
        let single_deadline = mth_finish_deadline + single_time;
        let thread_number = multi_groups.len();
        trace!(
            target: LOG_TARGET,
            "[Execute Block {}] ExecuteLimits: threads {thread_number}, mth {}({}) ms, single {} ms, total execute_deadline {} ms",
            parent_number + One::one(),
            mth_execute_time.as_millis(),
            mth_merge_time.as_millis(),
            single_time.as_millis(),
            execute_time / 1000,
        );

        let (mth_invalid_hash, mut final_end_reason) = if !multi_groups.is_empty() {
            let initial_applied = block_builder.extrinsics.len();
            let (merge_tx, merge_rx) = channel(thread_number);
            let mut mbh = MBH::default();
            mbh.prepare(&block_builder.backend, &block_builder.parent_hash, &block_builder.api);
            // 4. execute group transaction by threads.
            self.spawn_execute_groups(
                parent_number,
                block_builder,
                &mut mbh,
                mth_execute_deadline.clone(),
                inherents.clone(),
                multi_groups,
                merge_tx.clone(),
                round_tx,
                merge_in_thread_order,
                limit_execution_time,
            );

            // 5. merge all threads' changes to main block builder.
            let (mth_unqueue_invalid, end_reason) = self.merge_threads_result(
                parent_number + One::one(),
                mth_execute_time.as_millis() + mth_merge_time.as_millis(),
                block_builder,
                &mbh,
                inherents.len(),
                mth_finish_deadline,
                thread_number,
                merge_tx,
                merge_rx,
                merge_in_thread_order,
                limit_execution_time,
                recorder,
            ).await?;
            recorder.merge.as_mut().map(|info| info.mth_time = calculate_start.elapsed());
            let mth_applied = block_builder.extrinsics.len() - initial_applied;
            assert_eq!(
                mth_applied,
                recorder.threads
                    .values()
                    .filter(|info| info.thread > 0)
                    .map(|info| info.total)
                    .sum::<usize>(),
            );
            (mth_unqueue_invalid, end_reason)
        } else {
            (Vec::new(), EndProposingReason::NoMoreTransactions)
        };

        // 6. execute all single thread transactions.
        let (_, single_invalid, end_reason, single_thread_info) = Self::execute_one_thread_txs(
            parent_number + One::one(),
            single_deadline,
            block_builder,
            inherents.clone(),
            single_group,
            0,
            round_tx,
            limit_execution_time,
            false,
        );
        recorder.set_thread(single_thread_info);
        if !block_builder.extrinsics.is_empty() || !single_invalid.is_empty() {
            final_end_reason = end_reason;
        }
        Ok((mth_invalid_hash, single_invalid, final_end_reason))
    }

    fn spawn_execute_groups(
        &self,
        parent_number: <<Block as BlockT>::Header as HeaderT>::Number,
        block_builder: &mut BlockBuilder<Block, C, B>,
        mbh: &mut MBH,
        mth_execute_deadline: time::Instant,
        inherents: Vec<<Block as BlockT>::Extrinsic>,
        extrinsic_group: Vec<Vec<(usize, <A as TransactionPool>::Hash, Block::Extrinsic)>>,
        merge_tx: Sender<(MergeType<A, Block, C>, Option<ThreadExecutionInfo<Block>>)>,
        round_tx: usize,
        merge_in_thread_order: bool,
        limit_execution_time: bool,
    ) {
        let (init_cache, init_changes, _, init_recorder) = block_builder.api.take_all_changes();
        for (i, pending_txs) in extrinsic_group.into_iter().enumerate() {
            let thread_inherents = inherents.clone();
            let parent_hash = block_builder.parent_hash.clone();
            let estimated_header_size = block_builder.estimated_header_size.clone();
            let extrinsics = block_builder.extrinsics.clone();
            let context = Some(block_builder.context.clone());
            let mut init_cache = init_cache.copy_data();
            let mut init_changes = init_changes.clone();
            let init_recorder = init_recorder.clone();
            let thread_client = self.client.clone();
            let block = parent_number + One::one();
            let mut res_tx = merge_tx.clone();
            if merge_in_thread_order {
                mbh.prepare_thread_in_order(i + 1, pending_txs.len(), &mut init_cache, &mut init_changes);
            }
            self.spawn_handle.spawn_blocking(
                "mth-authorship-proposer",
                None,
                Box::pin(async move {
                    let mut thread_builder = match thread_client.new_with_other(parent_hash, estimated_header_size, context) {
                        Ok(mut builder) => {
                            builder.extrinsics = extrinsics;
                            builder.api.set_typed_cache(init_cache);
                            builder.api.set_changes(init_changes);
                            builder.api.set_recorder(init_recorder);
                            builder
                        },
                        Err(e) => {
                            error!("Could not create block builder for thread {} for error: {e:?}", i + 1);
                            return;
                        }
                    };
                    let (thread_root, unqueue_invalid, end_reason, thread_info) = Self::execute_one_thread_txs(
                        block.clone(),
                        mth_execute_deadline,
                        &mut thread_builder,
                        thread_inherents.clone(),
                        pending_txs,
                        i + 1,
                        round_tx,
                        limit_execution_time,
                        true,
                    );
                    let (_, mut overlay, stc, recorder) = thread_builder.api.take_all_changes();
                    #[cfg(feature = "kvdb")]
                    overlay.set_storage(b":thread_root".to_vec(), Some(thread_root.encode()));
                    let changes = (vec![i + 1], overlay, stc, recorder, thread_builder.extrinsics.clone(), unqueue_invalid, end_reason);
                    if res_tx.start_send((changes, Some(thread_info))).is_err() {
                        trace!("Could not send block production result to proposer!");
                    }
                }),
            );
        }
    }

    fn execute_one_thread_txs(
        block: <<Block as BlockT>::Header as HeaderT>::Number,
        thread_deadline: time::Instant,
        block_builder: &mut BlockBuilder<Block, C, B>,
        inherents: Vec<<Block as BlockT>::Extrinsic>,
        mut pending_txs: Vec<(usize, <A as TransactionPool>::Hash, Block::Extrinsic)>,
        thread: usize,
        round_tx: usize,
        limit_execution_time: bool,
        finish: bool,
    ) -> (Block::Hash, Vec<<A as TransactionPool>::Hash>, EndProposingReason, ThreadExecutionInfo<Block>) {
        let mut thread_info = ThreadExecutionInfo::new(thread);
        let mut thread_root = Default::default();
        let mut filter_transactions: HashSet<Block::Hash> = pending_txs.iter().map(|(_, tx, _)| tx.clone()).collect();
        if pending_txs.is_empty() {
            return (thread_root, Vec::new(), EndProposingReason::NoMoreTransactions, thread_info);
        }
        let mut thread_name = "single".to_string();
        if thread > 0 {
            thread_name = format!("{thread}");
        }
        let thread_start = time::Instant::now();
        let thread_time = thread_deadline.saturating_duration_since(thread_start);
        thread_info.time_limit = thread_time;
        if block_builder.extrinsics.is_empty() {
            for inherent in inherents.clone() {
                block_builder.push(inherent).unwrap();
            }
        }
        let mut unqueue_invalid = Vec::new();
        let initial_applied = block_builder.extrinsics.len();
        // thread txs should be same order with pool.
        pending_txs.sort_by(|a, b| a.0.cmp(&b.0));
        let mut round_execute_collect = Vec::new();
        let mut should_break = None;
        let mut tx_number = round_tx;
        let (end_reason, reason) = loop {
            tx_number = tx_number.min(pending_txs.len());
            if pending_txs.is_empty() {
                break (EndProposingReason::NoMoreTransactions, "NoMoreTransactions")
            }
            if time::Instant::now() > thread_deadline && limit_execution_time {
                break (EndProposingReason::HitDeadline, "HitThreadDeadline")
            }
            let execute_txs = pending_txs[..tx_number].to_vec();
            pending_txs = pending_txs[tx_number..].to_vec();
            let batch_txs = execute_txs.iter().map(|(_, _, e)| e.clone()).collect();
            let timeout = if limit_execution_time {
                let batch_start = time::Instant::now();
                thread_deadline.duration_since(batch_start)
            } else {
                Duration::from_secs(u64::MAX)
            };
            let (results, mut invalid_tx, future_or_exhausted) = block_builder.push_batch(batch_txs, timeout, false);
            thread_info.invalid += invalid_tx.len();
            thread_info.future_or_exhausted += future_or_exhausted.len();
            let executed = results.len();
            round_execute_collect.push(executed);
            for (_, result) in results.iter().enumerate() {
                match result {
                    Ok(()) => (),
                    Err(ApplyExtrinsicFailed(Validity(e))) if e.exhausted_resources() => {
                        if limit_execution_time {
                            should_break = Some(EndProposingReason::HitBlockWeightLimit);
                        }
                    }
                    _ => (),
                }
            }
            // clear invalid extrinsic.
            invalid_tx.sort_by(|a, b| a.0.cmp(&b.0));
            for (index, e) in invalid_tx {
                let invalid_hash = &execute_txs[index].1;
                warn!(target: "authorship", "[{invalid_hash:?}] Invalid transaction: {e}");
                unqueue_invalid.push(invalid_hash.clone());
            }
            for (index, e) in future_or_exhausted {
                let future_or_exhausted_hash = &execute_txs[index].1;
                warn!(target: "authorship", "[{future_or_exhausted_hash:?}] Invalid transaction: {e}");
                filter_transactions.remove(future_or_exhausted_hash);
            }
            if let Some(break_reason) = should_break {
                break (break_reason, "HitBlockWeightLimit");
            }
        };
        thread_info.total = block_builder.extrinsics.len() - initial_applied;
        thread_info.transactions = filter_transactions.into_iter().collect();
        let extend_start = time::Instant::now();
        let _ = E::extra_execute(&*block_builder.api, block_builder.parent_hash);
        thread_info.extend_time = extend_start.elapsed();
        thread_info.time = thread_start.elapsed();
        #[cfg(feature = "kvdb")]
        if finish {
            // calculate current thread changes Root And TypedCache also be drained to OverlayedChanges.
            let finish_thread_start = time::Instant::now();
            match block_builder.api.finish_thread(
                block_builder.parent_hash,
                thread.saturating_sub(1) as u8,
            ) {
                Ok(root) => thread_root = root,
                Err(e) => panic!("Could not get root for thread {thread} for error: {e:?}"),
            };
            thread_info.finish_time = finish_thread_start.elapsed();
        }
        thread_info.rounds = round_execute_collect;
        thread_info.end_reason = reason.to_string();
        Self::thread_finish_collect(block, &thread_name, &block_builder, &mut thread_info);
        (thread_root, unqueue_invalid, end_reason, thread_info)
    }

    fn thread_finish_collect(
        _block: <<Block as BlockT>::Header as HeaderT>::Number,
        thread_name: &String,
        block_builder: &BlockBuilder<Block, C, B>,
        thread_info: &mut ThreadExecutionInfo<Block>,
    ) {
        let times = |block_builder: &BlockBuilder<Block, C, B>, _thread: &String| -> (Vec<[u128; 4]>, Vec<(String, [u128; 4], usize)>, u128, u128, u128) {
            let mut execute_map: HashMap<String, ([u128; 4], usize)> = HashMap::new();
            let mut execute_time_count = 0u128;
            let mut round_times = Vec::new();
            let (execute_times, (commit_time, rollback_time)) = block_builder.api.execute_times();
            for (method, times) in execute_times {
                let method = String::from_utf8(method).unwrap_or("unknown".to_string());
                // trace!(target: LOG_TARGET, "Thread {_thread}, method: {method:?}, exe/read/read_backend/write: {times:?} nanos");
                if method.as_str() == "BlockBuilder_apply_extrinsics" {
                    execute_time_count += times[0];
                    round_times.push(times.clone());
                }
                if let Some((total_t, count)) = execute_map.get_mut(&method) {
                    for i in 0..4 {
                        total_t[i] += times[i];
                    }
                    *count += 1;
                } else {
                    execute_map.insert(method, (times, 1usize));
                }
            }
            let mut times: Vec<_> = execute_map.into_iter().map(|(k, (v, count))| (k, v, count)).collect();
            times.sort_by(|a, b| b.1[0].cmp(&a.1[0]));
            (round_times, times, execute_time_count, commit_time, rollback_time)
        };
        #[cfg(feature = "dev-time")]
        {
            let (round_times, times, execute_time, commit_time, rollback_time) = times(&block_builder, &thread_name);
            thread_info.round_times = round_times;
            let extra = thread_info.time.as_nanos().saturating_sub(execute_time);
            trace!(target: LOG_TARGET, "[Execute Block {_block}] Thread {thread_name} extra: {extra}({commit_time}/{rollback_time})ns, times(min/avg/max/count): {times:?}");
        }
        #[cfg(not(feature = "dev-time"))]
        { thread_info.round_times = times(&block_builder, &thread_name).0; }
    }

    async fn merge_threads_result<'a>(
        &self,
        block: <<Block as BlockT>::Header as HeaderT>::Number,
        mth_time: u128,
        block_builder: &mut BlockBuilder<'a, Block, C, B>,
        mbh: &MBH,
        inherents_len: usize,
        mth_finish_deadline: time::Instant,
        thread_number: usize,
        merge_tx: Sender<(MergeType<A, Block, C>, Option<ThreadExecutionInfo<Block>>)>,
        mut merge_rx: Receiver<(MergeType<A, Block, C>, Option<ThreadExecutionInfo<Block>>)>,
        merge_in_thread_order: bool,
        limit_execution_time: bool,
        recorder: &mut InfoRecorder<Block>,
    ) -> Result<(Vec<<A as TransactionPool>::Hash>, EndProposingReason), sp_blockchain::Error> {
        let mut merge_info = MergeInfo::default();
        let allow_rollback = std::env::var("MTH_MERGE_ROLLBACK").unwrap_or("false".into()).parse().unwrap_or(false);
        let mut merge_count = 0usize;
        let mut extra_merge_count = (time::Instant::now(), thread_number);
        let mut merge_box: Vec<(u32, MergeType<A, Block, C>)> = vec![];
        let mut conflict_top_changes = Changes::new();
        let mut conflict_children_changes = HashMap::new();
        loop {
            if let Some(res) = merge_rx.next().await {
                // try collect all changes in channel.
                let mut all_changes = vec![res];
                loop {
                    match merge_rx.try_next() {
                        Ok(Some(res, )) => {
                            all_changes.push(res)
                        },
                        _ => break,
                    }
                }
                for (changes, thread_info) in all_changes {
                    if let Some(thread_info) = thread_info {
                        recorder.set_thread(thread_info);
                        extra_merge_count.1 -= 1;
                        if extra_merge_count.1 == 0 {
                            // all threads execute finished, initialize extra merge start time.
                            extra_merge_count.0 = time::Instant::now();
                        }
                    } else {
                        // empty thread_info means merge result.
                        merge_count += 1;
                    }
                    merge_box.push((changes.1.merge_weight::<MBH>(), changes));
                }
                if merge_in_thread_order {
                    // sort by thread number descending order.
                    merge_box.sort_by(|a, b| b.1.0[0].cmp(&a.1.0[0]));
                } else {
                    // sort by merge weight ascending order.
                    merge_box.sort_by(|a, b| a.0.cmp(&b.0));
                }
                if merge_count + 1 == thread_number || (limit_execution_time && time::Instant::now() > mth_finish_deadline) {
                    let (_, mut changes) = merge_box.pop().unwrap();
                    if merge_count + 1 < thread_number {
                        warn!(
                            target: LOG_TARGET,
                            "Timeout fired waiting for threads result execution for block #{block} mth_time {mth_time} ms. Build block with current threads {:?}.",
                            changes.0,
                        );
                    }
                    let _ = block_builder.api.take_all_changes();
                    // finalize merge
                    changes.1.finalize_merge(mbh);
                    #[cfg(feature = "kvdb")]
                    {
                        // move all merged changes as read_only.
                        changes.1.read_only();
                        // set block threads info to changes.
                        set_threads_to_storage::<Block>(block, thread_number as u8, &mut conflict_top_changes);
                    }
                    // set conflict changes as new changes for following root calculation.
                    changes.1.extend_changes(conflict_top_changes, conflict_children_changes);
                    // final main block builder state.
                    block_builder.api.set_changes(changes.1);
                    block_builder.api.set_storage_transaction_cache(changes.2);
                    block_builder.api.set_recorder(changes.3);
                    block_builder.extrinsics = changes.4;
                    merge_info.extra_merge_time = extra_merge_count.0.elapsed();
                    recorder.merge = Some(merge_info);
                    return Ok((changes.5, changes.6));
                }
                loop {
                    if merge_box.len() >= 2 {
                        let (changes_weight_weight, pre_changes) = merge_box.pop().unwrap();
                        let (changes_weight, changes) = merge_box.pop().unwrap();
                        if merge_in_thread_order && pre_changes.0.last().unwrap() + 1 != *changes.0.first().unwrap() {
                            merge_box.push((changes_weight, changes));
                            merge_box.push((changes_weight_weight, pre_changes));
                            break;
                        }
                        Self::merge_process(
                            merge_tx.clone(),
                            block,
                            &mbh,
                            pre_changes,
                            changes,
                            &mut conflict_top_changes,
                            &mut conflict_children_changes,
                            inherents_len,
                            allow_rollback,
                            merge_in_thread_order,
                            &mut merge_info,
                        );
                        break;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    fn merge_process(
        mut merge_tx: Sender<(MergeType<A, Block, C>, Option<ThreadExecutionInfo<Block>>)>,
        block: <<Block as BlockT>::Header as HeaderT>::Number,
        mbh: &MBH,
        changes1: MergeType<A, Block, C>,
        changes2: MergeType<A, Block, C>,
        collect_top: &mut Changes,
        collect_children: &mut HashMap<StorageKey, (Changes, ChildInfo)>,
        inherents_len: usize,
        allow_rollback: bool,
        static_order: bool,
        merge_info: &mut MergeInfo,
    ) {
        let exe_merge_start = time::Instant::now();
        let merge_result = Self::merge_changes(
            &mbh,
            changes1,
            changes2,
            collect_top,
            collect_children,
            inherents_len,
            allow_rollback,
            static_order,
        );
        let merge_time = exe_merge_start.elapsed();
        let changes = match merge_result {
            Ok((changes, to_threads, from_threads, merge_transaction_time)) => {
                merge_info.records.push((to_threads, from_threads, merge_time, merge_transaction_time));
                changes
            },
            Err((keep, drop, e)) => {
                error!(
                    target: LOG_TARGET,
                    "[MergeCh Block {block}] Merge threads keep {:?}, drop [threads {:?}, {} extrinsics] in {merge_time:?} for error: {e:?}",
                    keep.0,
                    drop.0,
                    drop.4.len() + drop.5.len() - inherents_len,
                );
                keep
            }
        };
        if merge_tx.start_send((changes, None)).is_err() {
            panic!("Could not send merge result to proposer!");
        }
    }

    fn merge_changes(
        mbh: &MBH,
        mut to: MergeType<A, Block, C>,
        mut from: MergeType<A, Block, C>,
        collect_top: &mut Changes,
        collect_children: &mut HashMap<StorageKey, (Changes, ChildInfo)>,
        inherents_len: usize,
        allow_rollback: bool,
        static_order: bool,
    ) -> Result<
        (MergeType<A, Block, C>, Vec<usize>, Vec<usize>, Duration),
        (MergeType<A, Block, C>, MergeType<A, Block, C>, MergeErr)
    > {
        if !static_order && to.1.merge_weight::<MBH>() < from.1.merge_weight::<MBH>() {
            std::mem::swap(&mut to, &mut from);
        }
        // do as try merge for state
        // if success, do other merge.
        // if failed, return unchanged state.
        if allow_rollback {
            let mut tmp_state = to.1.clone();
            match tmp_state.merge(&mut from.1, static_order, mbh) {
                Ok((top_changes, children_changes)) => {
                    collect_top.extend(top_changes);
                    for (child_key, (children, info)) in children_changes {
                        if let Some((c, ci)) = collect_children.get_mut(&child_key) {
                            if *ci != info { continue; }
                            c.extend(children);
                        }
                    }
                },
                Err(e) => return Err((to, from, e)),
                // since both state are not changed. we choose to keep `to` and drop `from`
            }
            to.1 = tmp_state;
        } else {
            match to.1.merge(&mut from.1, static_order, mbh) {
                Ok((top_changes, children_changes)) => {
                    collect_top.extend(top_changes);
                    for (child_key, (children, info)) in children_changes {
                        if let Some((c, ci)) = collect_children.get_mut(&child_key) {
                            if *ci != info { continue; }
                            c.extend(children);
                        }
                    }
                },
                Err(e) => panic!("Merge threads {:?} and {:?} failed for {e:?}(rollback not allowed)", to.0, from.0),
            }
        }
        let to_threads = to.0.clone();
        to.0.extend(from.0.clone());
        let merge_transaction_start = time::Instant::now();
        to.2.consolidate(from.2);
        let merge_transaction_time = merge_transaction_start.elapsed();
        to.2.clear_root();
        to.3 = match (to.3, from.3) {
            (Some(recorder1), Some(recorder2)) => {
                recorder1.merge(&recorder2);
                Some(recorder1)
            },
            (Some(recorder1), None) => Some(recorder1),
            (None, Some(recorder2)) => Some(recorder2),
            (None, None) => None,
        };
        to.4.extend(from.4[inherents_len..].to_vec());
        to.5.extend(from.5.clone());
        to.6 = from.6;
        Ok((to, to_threads, from.0, merge_transaction_time))
    }

    async fn one_thread_build_check(
        client: Arc<C>,
        inherent_digests: Digest,
        block: Block,
        main_storage_changes: Vec<(StorageKey, Option<StorageValue>)>,
        child_storage_changes: Vec<(StorageKey, Vec<(StorageKey, Option<StorageValue>)>)>,
        _proof: Option<StorageProof>,
    ) {
        let block_timer = time::Instant::now();
        let header = block.header();
        let number = header.number().clone();
        let parent_hash = header.parent_hash().clone();
        let extrinsic = block.extrinsics().to_vec();
        let mut block_builder = match client.new_block_at(parent_hash, inherent_digests, PR::ENABLED, None) {
            Ok(builder) => builder,
            Err(e) => {
                warn!(target: "OTC", "[OTC Block {number}] Create BlockBuilder error: {e:?}");
                return;
            },
        };
        let init_time = block_timer.elapsed().as_millis();
        let execute_timer = time::Instant::now();
        for (index, pending_tx_data) in extrinsic.into_iter().enumerate() {
            if let Err(e) = sc_block_builder::BlockBuilder::push(&mut block_builder, pending_tx_data.clone()) {
                error!(
                    target: "OTC",
                    "[OTC Block {number}] push extrinsic {index} error: {e:?}"
                );
                return;
            }
        };
        let execute_time = execute_timer.elapsed().as_millis();
        let extend_start = time::Instant::now();
        E::extra_execute(&*block_builder.api, block_builder.parent_hash);
        let extend_time = extend_start.elapsed().as_millis();
        let build_timer = time::Instant::now();
        match block_builder.build() {
            Ok(res) => {
                let block_time = block_timer.elapsed().as_millis();
                let build_time = build_timer.elapsed().as_millis();
                let (block_res, storage_changes_res, _proof_res) = res.into_inner();
                // total block hash check
                if block_res.header().state_root() == block.header().state_root() {
                    info!(target: "OTC", "[OTC Block {number}({block_time}({init_time} {execute_time} {extend_time} {build_time}) ms)] Check Block Hash success");
                    return;
                }
                let change_diff = |mth: &Vec<(StorageKey, Option<StorageValue>)>, oth: &Vec<(StorageKey, Option<StorageValue>)>| {
                    #[cfg(feature = "kvdb")]
                    let filter = vec![
                        // System Threads
                        [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 156, 181, 201, 244, 0, 171, 65, 57, 156, 107, 139, 81, 248, 209, 136, 25].to_vec(),
                        // System EventsMap
                        [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 49, 208, 128, 228, 214, 125, 178, 100, 12, 86, 17, 66, 220, 220, 32, 101].to_vec(),
                        // b":thread_root",
                        [58, 116, 104, 114, 101, 97, 100, 95, 114, 111, 111, 116].to_vec(),
                    ];
                    #[cfg(not(feature = "kvdb"))]
                    let filter = vec![[58, 116, 104, 114, 101, 97, 100, 95, 114, 111, 111, 116].to_vec()];
                    // main_storage_changes check if block different.
                    let mut mth_changes = HashMap::new();
                    for (k, v) in mth.clone() {
                        mth_changes.insert(k, v);
                    }
                    let mut oth_diff = vec![];
                    let mut oth_extra = vec![];
                    for (k, v_res) in oth.iter() {
                        if let Some(v) = mth_changes.remove(k) {
                            if &v != v_res {
                                if filter.iter().any(|f| k.starts_with(f)) { continue; }
                                trace!(target: "OTC", "[OTC Block {number}] otc diff value: {v_res:?} for key: {k:?}");
                                oth_diff.push(k.clone());
                            }
                        } else {
                            if filter.iter().any(|f| k.starts_with(f)) { continue; }
                            oth_extra.push(k.clone());
                        }
                    }
                    let oth_less: Vec<_> = mth_changes
                        .keys()
                        .filter(|k| !filter.iter().any(|f| k.starts_with(f)))
                        .cloned()
                        .collect();
                    (oth_less, oth_diff, oth_extra)
                };
                // main_storage_changes check if block different.
                let (oth_main_less, oth_main_diff, oth_main_extra) = change_diff(&main_storage_changes, &storage_changes_res.main_storage_changes);
                if oth_main_less.is_empty() && oth_main_diff.is_empty() && oth_main_extra.is_empty() {
                    info!(target: "OTC", "[OTC Block {number}({block_time}({init_time} {execute_time} {build_time}) ms)] Check Block state success");
                    return;
                }
                error!(
                    target: "OTC",
                    "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] [one thread/multi threads] main change differences, less: {oth_main_less:?}, diff: {oth_main_diff:?}, extra: {oth_main_extra:?}",
                );
                // child_storage_changes check if block different.
                let mut child_changes = HashMap::new();
                for (k, v) in child_storage_changes.clone() {
                    child_changes.insert(k, v);
                }
                let mut oth_extra_childs = vec![];
                for (parent_key, child_storage_changes_res) in &storage_changes_res.child_storage_changes {
                    if let Some(child_storage_changes) = child_changes.remove(parent_key) {
                        let (oth_child_less, oth_child_diff, oth_child_extra) = change_diff(&child_storage_changes, child_storage_changes_res);
                        error!(
                            target: "OTC",
                            "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] [one thread/multi threads] child change differences parent key: {parent_key:?}, less: {oth_child_less:?}, diff: {oth_child_diff:?}, extra: {oth_child_extra:?}",
                        );
                    } else {
                        let child_keys: Vec<_> = child_storage_changes.iter().map(|(key, _)| key.clone()).collect();
                        oth_extra_childs.push((parent_key.clone(), child_keys));
                    }
                }
                if !oth_extra_childs.is_empty() {
                    error!(target: "OTC", "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] oth extra child changes {oth_extra_childs:?}");
                }
                let mth_extra_childs: Vec<(StorageKey, Vec<StorageKey>)> = child_changes
                    .into_iter()
                    .map(|(parent, childs)| (parent, childs.into_iter().map(|(key, _)| key).collect()))
                    .collect();
                if !mth_extra_childs.is_empty() {
                    error!(target: "OTC", "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] mth extra child changes {mth_extra_childs:?}");
                }
            },
            Err(e) => {
                let block_time = block_timer.elapsed().as_millis();
                let build_time = build_timer.elapsed().as_millis();
                error!(target: "OTC", "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] Build block error: {e:?}");
            }
        }
    }

    #[cfg(feature = "dev-time")]
    fn collect_encode_decode(info: &str) {
        use frame_support::storage::unhashed;

        let mut encode = unhashed::GLOBAL_ENCODE.lock().unwrap();
        let mut decode = unhashed::GLOBAL_DECODE.lock().unwrap();
        let mut get = sp_state_machine::GET.lock().unwrap();
        let mut put = sp_state_machine::PUT.lock().unwrap();
        let mut cache_encode = sp_state_machine::ENCODE.lock().unwrap();
        let mut encode = encode
            .drain()
            .into_iter()
            .map(|(k, v)| (
                hex::encode(&k),
                v.len(),
                v.into_iter().fold((Duration::default(), Duration::default()), |acc, x| (acc.0 + x.0, acc.1 + x.1))
            ))
            .collect::<Vec<_>>();
        let mut decode = decode
            .drain()
            .into_iter()
            .map(|(k, v)| (
                hex::encode(&k),
                v.len(),
                v.into_iter().fold((Duration::default(), Duration::default()), |acc, x| (acc.0 + x.0, acc.1 + x.1))
            ))
            .collect::<Vec<_>>();
        encode.sort_by(|a, b| b.2.0.cmp(&a.2.0));
        decode.sort_by(|a, b| b.2.0.cmp(&a.2.0));
        let mut total_read_times = 0usize;
        let mut total_read_time1 = Duration::default();
        let mut total_read_time = Duration::default();
        encode.iter().for_each(|(_, n, (t1, t))| {
            total_read_times += *n;
            total_read_time1 += *t1;
            total_read_time += *t;
        });
        let mut total_write_times = 0usize;
        let mut total_write_time1 = Duration::default();
        let mut total_write_time = Duration::default();
        decode.iter().for_each(|(_, n, (t1, t))| {
            total_write_times += *n;
            total_write_time1 += *t1;
            total_write_time += *t;
        });
        log::info!(target: "codec", "{info} encode: {total_read_time1:?}/{total_read_time:?}({total_read_times}) {encode:?}");
        log::info!(target: "codec", "{info} decode: {total_write_time1:?}/{total_write_time:?}({total_write_times}) {decode:?}");

        let mut put = put
            .drain()
            .into_iter()
            .map(|(k, v)| (
                hex::encode(&k),
                v.len(),
                v.into_iter().fold((Duration::default(), Duration::default()), |acc, x| (acc.0 + x.0, acc.1 + x.1))
            ))
            .collect::<Vec<_>>();
        let mut get = get
            .drain()
            .into_iter()
            .map(|(k, v)| (
                hex::encode(&k),
                v.len(),
                v.into_iter().fold((Duration::default(), Duration::default()), |acc, x| (acc.0 + x.0, acc.1 + x.1))
            ))
            .collect::<Vec<_>>();
        let mut cache_encode = cache_encode
            .drain()
            .into_iter()
            .map(|(k, v)| (hex::encode(&k), v.len(), v.into_iter().sum::<Duration>()))
            .collect::<Vec<_>>();
        put.sort_by(|a, b| b.2.1.cmp(&a.2.1));
        get.sort_by(|a, b| b.2.1.cmp(&a.2.1));
        cache_encode.sort_by(|a, b| b.2.cmp(&a.2));

        let mut total_put_times = 0usize;
        let mut total_put_time1 = Duration::default();
        let mut total_put_time = Duration::default();
        put.iter().for_each(|(_, n, (t1, t))| {
            total_put_times += *n;
            total_put_time1 += *t1;
            total_put_time += *t;
        });
        let mut total_get_times = 0usize;
        let mut total_get_time1 = Duration::default();
        let mut total_get_time = Duration::default();
        get.iter().for_each(|(_, n, (t1, t))| {
            total_get_times += *n;
            total_get_time1 += *t1;
            total_get_time += *t;
        });

        let mut total_cache_encode_times = 0usize;
        let mut total_cache_encode_time = Duration::default();
        cache_encode.iter().for_each(|(_, n, t)| {
            total_cache_encode_times += *n;
            total_cache_encode_time += *t;
        });

        log::info!(target: "codec", "{info} put: {total_put_time1:?}/{total_put_time:?}({total_put_times}) {put:?}");
        log::info!(target: "codec", "{info} get: {total_get_time1:?}/{total_get_time:?}({total_get_times}) {get:?}");
        log::info!(target: "codec", "{info} cache_encode: {total_cache_encode_time:?}({total_cache_encode_times}) {cache_encode:?}");
    }
}

#[async_trait::async_trait]
impl<A, B, Block, C, PR, MBH, E> BlockPropose<Block> for BlockExecutor<B, C, A, PR, MBH, E>
where
    A: TransactionPool<Block = Block, Hash = Block::Hash> + 'static,
    <A as TransactionPool>::InPoolTransaction: Send + Sync + 'static,
    B: backend::Backend<Block> + Send + Sync + 'static,
    Block: BlockT,
    C: BlockBuilderProvider<B, Block, C>
    + HeaderBackend<Block>
    + ProvideRuntimeApi<Block>
    + CallApiAt<Block>
    + Send
    + Sync
    + 'static,
    C::Api:
    ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>> + BlockBuilderApi<Block> + SpotRuntimeApi<Block> + PerpRuntimeApi<Block>,
    PR: ProofRecording,
    MBH: MultiThreadBlockBuilder<B, Block, C::Api> + Send + Sync + 'static,
    E: ExtraExecute<Block, C::Api> + Send + Sync + 'static,
{
    type Transaction = backend::TransactionFor<B, Block>;
    type Proof = PR::Proof;
    type Error = sp_blockchain::Error;

    async fn propose_block(
        self,
        source: &str,
        parent_hash: Block::Hash,
        parent_number: <<Block as BlockT>::Header as HeaderT>::Number,
        max_duration: Duration,
        linear_execute_time: Duration,
        estimated_merge_time: Duration,
        inherent_data: InherentData,
        inherent_digests: Digest,
        extrinsic: (Vec<Vec<Block::Extrinsic>>, Vec<Block::Extrinsic>),
        set_extrinsics_root: Option<Block::Hash>,
        round_tx: usize,
        merge_in_thread_order: bool,
        limit_execution_time: bool,
    ) -> Result<
        (Proposal<Block, Self::Transaction, Self::Proof>, BlockExecuteInfo<Block>),
        Self::Error
    >
    {
        info!(target: LOG_TARGET, "🙌 Starting consensus session on top of parent {parent_number}({parent_hash:?})");
        let mut block_builder =
            self.client.new_block_at(parent_hash.clone(), inherent_digests.clone(), PR::ENABLED, None)?;

        let create_inherents_start = time::Instant::now();
        let inherents = block_builder.create_inherents(inherent_data)?;
        let create_inherents_end = time::Instant::now();
        self.metrics.report(|metrics| {
            metrics.create_inherents_time.observe(
                create_inherents_end
                    .saturating_duration_since(create_inherents_start)
                    .as_secs_f64(),
            );
        });
        let (proposal, info) = self.execute_block(
            ExecutionContext::BlockConstruction,
            parent_hash,
            parent_number,
            time::Instant::now(),
            time::Instant::now() + max_duration,
            inherents,
            inherent_digests,
            block_builder,
            linear_execute_time,
            estimated_merge_time,
            round_tx,
            extrinsic.0
                .into_iter()
                .map(|g| g.into_iter().enumerate().map(|(i, tx)| (i, <<<Block as BlockT>::Header as Header>::Hashing as traits::Hash>::hash(&tx.encode()), tx)).collect())
                .collect(),
            extrinsic.1.into_iter().enumerate().map(|(i, tx)| (i, <<<Block as BlockT>::Header as Header>::Hashing as traits::Hash>::hash(&tx.encode()), tx)).collect(),
            set_extrinsics_root,
            merge_in_thread_order,
            limit_execution_time,
        )
            .await?;
        let block_execute_info = if log::log_enabled!(target: LOG_TARGET, log::Level::Debug) {
            for thread_info in info.threads_debug(limit_execution_time) {
                debug!(target: LOG_TARGET, "[Execute {}] {thread_info}", info.number);
            }
            for merge_info in info.merge.print_records() {
                debug!(target: LOG_TARGET, "[MergeCh {}] {merge_info}", info.number);
            }
            info.debug(limit_execution_time)
        } else if log::log_enabled!(target: LOG_TARGET, log::Level::Info) {
            info.info(limit_execution_time)
        } else {
            unreachable!()
        };
        info!(target: LOG_TARGET, "🎁 [{source}] {block_execute_info}");
        telemetry!(
			self.telemetry;
			CONSENSUS_INFO;
			"prepared_block_for_proposing";
			"number" => ?proposal.block.header().number(),
			"hash" => ?<Block as BlockT>::Hash::from(proposal.block.header().hash()),
		);
        Ok((proposal, info))
    }

    async fn execute_block_for_import(
        &self,
        source: &str,
        context: ExecutionContext,
        parent_hash: Block::Hash,
        parent_number: <<Block as BlockT>::Header as HeaderT>::Number,
        inherent_digests: Digest,
        extrinsic: Vec<Block::Extrinsic>,
        groups: Vec<u32>,
        round_tx: usize,
        linear_execute_time: Duration,
        estimated_merge_time: Duration,
        merge_in_thread_order: bool,
        limit_execution_time: bool,
    ) -> Result<
        (Proposal<Block, Self::Transaction, Self::Proof>, BlockExecuteInfo<Block>),
        Self::Error
    >
    {
        debug!(target: LOG_TARGET, "[{source}] Start to execute block {} on parent_hash {parent_hash} with groups {groups:?}", parent_number + One::one());
        let inherents = extrinsic[..groups.first().cloned().unwrap_or_default() as usize].to_vec();
        let mut multi_groups: Vec<Vec<Block::Extrinsic>> = vec![];
        let mut offset = inherents.len();
        if groups.len() > 2 {
            for index in 1..groups.len() - 1 {
                let thread_extrinsic_num = groups[index] as usize;
                multi_groups.push(extrinsic[offset..offset + thread_extrinsic_num].to_vec());
                offset += thread_extrinsic_num;
            }
        }
        let single_thread_extrinsic_num = groups.last().cloned().unwrap_or_default() as usize;
        let single: Vec<Block::Extrinsic> = extrinsic[offset..offset + single_thread_extrinsic_num].to_vec();
        let max_duration = time::Duration::from_secs(10);
        let block_builder =
            self.client.new_block_at(parent_hash, inherent_digests.clone(), PR::ENABLED, Some(context))?;
        // Spawn mission for large extrinsics root calculation.
        let (mut extrinsics_root_sender, mut extrinsics_root_receiver) = channel(1);
        let spawn_extrinsics = extrinsic[inherents.len()..].to_vec();
        self.spawn_handle.spawn(
            "mth-authorship-proposer",
            None,
            Box::pin(async move {
                let root = extrinsics_root::<<Block::Header as HeaderT>::Hashing, <Block as BlockT>::Extrinsic>(&spawn_extrinsics);
                if let Err(e) = extrinsics_root_sender.send(root).await {
                    warn!(target: LOG_TARGET,"Send extrinsics_root failed for {e:?}");
                }
            })
        );
        let (mut proposal, info) = self.execute_block(
            ExecutionContext::Importing,
            parent_hash,
            parent_number,
            time::Instant::now(),
            time::Instant::now() + max_duration,
            inherents,
            inherent_digests,
            block_builder,
            linear_execute_time,
            estimated_merge_time,
            round_tx,
            multi_groups
                .into_iter()
                .map(|g| g.into_iter().enumerate().map(|(i, tx)| (i, <<<Block as BlockT>::Header as Header>::Hashing as traits::Hash>::hash(&tx.encode()), tx)).collect())
                .collect(),
            single.into_iter().enumerate().map(|(i, tx)| (i, <<<Block as BlockT>::Header as Header>::Hashing as traits::Hash>::hash(&tx.encode()), tx)).collect(),
            Some(Default::default()),
            merge_in_thread_order,
            limit_execution_time,
        )
            .await?;
        match extrinsics_root_receiver.next().await {
            Some(extrinsics_root) => proposal.block.header_mut().set_extrinsics_root(extrinsics_root),
            None => return Err(Self::Error::UnknownBlock("Failed to get extrinsiscs_root".to_string())),
        }
        let block_execute_info = if log::log_enabled!(target: LOG_TARGET, log::Level::Debug) {
            for merge_info in info.merge.print_records() {
                debug!(target: LOG_TARGET, "[MergeCh Block {}] {merge_info}", info.number);
            }
            info.debug(limit_execution_time)
        } else if log::log_enabled!(target: LOG_TARGET, log::Level::Info) {
            info.info(limit_execution_time)
        } else {
            unreachable!()
        };
        info!(target: LOG_TARGET, "🎁 [{source}] {block_execute_info}");
        telemetry!(
			self.telemetry;
			CONSENSUS_INFO;
			"prepared_block_for_proposing";
			"number" => ?proposal.block.header().number(),
			"hash" => ?<Block as BlockT>::Hash::from(proposal.block.header().hash()),
		);
        Ok((proposal, info))
    }
}

/// Same with `frame_system::extrinsics_root`.
pub fn extrinsics_root<H: Hash, E: codec::Encode>(extrinsics: &[E]) -> H::Output {
    extrinsics_data_root::<H>(extrinsics.iter().map(codec::Encode::encode).collect())
}

/// Same with `frame_system::extrinsics_data_root`.
pub fn extrinsics_data_root<H: Hash>(xts: Vec<Vec<u8>>) -> H::Output {
    H::ordered_trie_root(xts, sp_core::storage::StateVersion::V0)
}

pub fn set_threads_to_storage<Block: BlockT>(block: NumberFor<Block>, threads: u8, changes: &mut Changes) {
    use sp_core::blake2_128;

    const SYSTEM_THREADS_PREFIX: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 156, 181, 201, 244, 0, 171, 65, 57, 156, 107, 139, 81, 248, 209, 136, 25];

    let threads_key = [SYSTEM_THREADS_PREFIX.to_vec(), blake2_128(&block.encode()).to_vec()].concat();
    if let Some(entry) = changes.get_mut(&threads_key) {
        entry.set(Some(threads.encode()), false, None);
    } else {
        let mut new_entry = OverlayedEntry::default();
        new_entry.set(Some(threads.encode()), true, None);
        changes.insert(threads_key, new_entry);
    }
}
