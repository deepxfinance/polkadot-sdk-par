use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;
use std::{cmp, time};
use std::time::Duration;
use codec::Encode;
use futures::StreamExt;
use futures::channel::mpsc::{Receiver, Sender, channel};
use futures::task::SpawnExt;
use log::{debug, error, info, trace, warn};
use sc_block_builder::{BlockBuilder, BlockBuilderApi, BlockBuilderProvider};
use sc_client_api::{backend, StorageProof};
use sc_proposer_metrics::{EndProposingReason, MetricsLink as PrometheusMetrics};
use sc_telemetry::{telemetry, TelemetryHandle, CONSENSUS_INFO};
use sc_transaction_pool_api::TransactionPool;
use sp_api::{ApiExt, BlockT, CallApiAt, HeaderT, ProofRecorder, ProvideRuntimeApi};
use sp_blockchain::ApplyExtrinsicFailed::Validity;
use sp_blockchain::Error::ApplyExtrinsicFailed;
use sp_blockchain::HeaderBackend;
use sp_consensus::{ProofRecording, Proposal};
use sp_core::ExecutionContext;
use sp_core::traits::SpawnNamed;
use sp_inherents::InherentData;
use sp_perp_api::PerpRuntimeApi;
use sp_runtime::{traits, Digest, Percent, SaturatedConversion, Saturating};
use sp_runtime::traits::{Header, One};
use sp_spot_api::SpotRuntimeApi;
use sp_state_machine::{MergeErr, OverlayedChanges, StorageKey, StorageValue};
use crate::{BlockPropose, ExtendExtrinsic, MultiThreadBlockBuilder};
use crate::mth_authorship::execute_info::{BlockExecuteInfo, InfoRecorder, MergeInfo, ThreadExecutionInfo};
use frame_support::storage::unhashed;

const LOG_TARGET: &str = "authorship";

type MergeType<A, B> = (
    Vec<usize>,
    OverlayedChanges,
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
    E: ExtendExtrinsic + Send + Sync + 'static,
{
    pub fn new(
        spawn_handle: Box<dyn SpawnNamed>,
        client: Arc<C>,
        transaction_pool: Arc<A>,
        native_version: sp_version::NativeVersion,
        metrics: PrometheusMetrics,
        telemetry: Option<TelemetryHandle>,
    ) -> Self {
        Self {
            spawn_handle,
            client,
            transaction_pool,
            native_version,
            metrics,
            telemetry,
            phantom: PhantomData,
        }
    }

    pub async fn execute_block<'a>(
        &self,
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
        merge_in_thread_order: bool,
        limit_execution_time: bool,
    ) -> Result<(Proposal<Block, backend::TransactionFor<B, Block>, PR::Proof>, BlockExecuteInfo<Block>), sp_blockchain::Error> {
        let thread_number = extrinsic_group.len();
        let max_duration = deadline.saturating_duration_since(propose_with_start);
        let mut recorder = InfoRecorder::<Block>::new()
            .number(parent_number + One::one())
            .max_time(max_duration)
            .start();
        // if native version not latest, we can only execute in single thread.
        let onchain_version = CallApiAt::runtime_version_at(&*self.client, parent_hash)
            .map_err(|e| sp_blockchain::Error::RuntimeApiError(e))?;
        if !onchain_version.can_call_with(&self.native_version.runtime_version) {
            recorder.native = false;
        }
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

        unhashed::GLOBAL_ENCODE.lock().unwrap().clear();
        unhashed::GLOBAL_DECODE.lock().unwrap().clear();
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
        if block_builder.extrinsics.len() > 1 {
            Self::collect_encode_decode(format!("Block {}", recorder.number).as_str());
        }
        self.transaction_pool.remove_invalid(&mth_invalid);
        self.transaction_pool.remove_invalid(&single_invalid);

        // 5. build block by finalize block.
        recorder.finalize_start();
        let (block, storage_changes, proof) = block_builder.build()?.into_inner();
        let info = recorder.finalize();

        // 6. spawn a single execution check if env `MTH_CHECK` is true, this is just an extra check, weill not block main block build.
        let single_thread_check = std::env::var("MTH_CHECK").unwrap_or("false".into()).parse().unwrap_or(false);
        if single_thread_check && thread_number > 0 {
            let client = self.client.clone();
            let inherent_digests = inherent_digests.clone();
            let block = block.clone();
            let main_storage_changes = storage_changes.main_storage_changes.clone();
            let child_storage_changes = storage_changes.child_storage_changes.clone();
            let proof = proof.clone();
            let kv_mode = std::env::var("DB_KV_MODE").map(|s| s.parse().unwrap_or(false)).unwrap_or(false);
            if kv_mode {
                // kv mode should check before block imported(since we have no history data).
                Self::one_thread_build_check(
                    client,
                    inherent_digests,
                    block,
                    main_storage_changes,
                    child_storage_changes,
                    proof,
                )
                    .await
            } else {
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
            "[Execute Block {}] ExecuteLimits: threads {thread_number}, mth_exe(mth_merge) {}({}) ms, single_exe {} ms, total execute_deadline {} ms",
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
                mth_execute_deadline.clone(),
                inherents.clone(),
                multi_groups,
                merge_tx.clone(),
                round_tx,
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
        let (single_invalid, end_reason, single_thread_info) = Self::execute_one_thread_txs(
            parent_number + One::one(),
            single_deadline,
            block_builder,
            inherents.clone(),
            single_group,
            0,
            round_tx,
            limit_execution_time,
        );
        recorder.threads.insert(0, single_thread_info);
        if !block_builder.extrinsics.is_empty() || !single_invalid.is_empty() {
            final_end_reason = end_reason;
        }
        Ok((mth_invalid_hash, single_invalid, final_end_reason))
    }

    fn spawn_execute_groups(
        &self,
        parent_number: <<Block as BlockT>::Header as HeaderT>::Number,
        block_builder: &mut BlockBuilder<Block, C, B>,
        mth_execute_deadline: time::Instant,
        inherents: Vec<<Block as BlockT>::Extrinsic>,
        extrinsic_group: Vec<Vec<(usize, <A as TransactionPool>::Hash, Block::Extrinsic)>>,
        merge_tx: Sender<(MergeType<A, Block>, Option<ThreadExecutionInfo<Block>>)>,
        round_tx: usize,
        limit_execution_time: bool,
    ) {
        let (init_cache, init_changes, _, init_recorder) = block_builder.api.take_all_changes();
        for (i, pending_txs) in extrinsic_group.into_iter().enumerate() {
            let thread_inherents = inherents.clone();
            let parent_hash = block_builder.parent_hash.clone();
            let estimated_header_size = block_builder.estimated_header_size.clone();
            let extrinsics = block_builder.extrinsics.clone();
            let context = Some(block_builder.context.clone());
            let init_cache = init_cache.copy_data();
            let init_changes = init_changes.clone();
            let init_recorder = init_recorder.clone();
            let thread_client = self.client.clone();
            let block = parent_number + One::one();
            let mut res_tx = merge_tx.clone();
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
                    let (unqueue_invalid, end_reason, thread_info) = Self::execute_one_thread_txs(
                        block.clone(),
                        mth_execute_deadline,
                        &mut thread_builder,
                        thread_inherents.clone(),
                        pending_txs,
                        i + 1,
                        round_tx,
                        limit_execution_time,
                    );

                    let (mut cache, mut overlay, _, recorder) = thread_builder.api.take_all_changes();
                    // TODO more check when merge cache to overlay.top?
                    for (k, v) in cache.drain_commited() {
                        overlay.top.changes.entry(k).or_default().set(v, true, None);
                    }

                    let changes = (vec![i + 1], overlay, recorder, thread_builder.extrinsics.clone(), unqueue_invalid, end_reason);
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
    ) -> (Vec<<A as TransactionPool>::Hash>, EndProposingReason, ThreadExecutionInfo<Block>) {
        let mut thread_info = ThreadExecutionInfo::new(thread);
        let mut filter_transactions: HashSet<Block::Hash> = pending_txs.iter().map(|(_, tx, _)| tx.clone()).collect();
        if pending_txs.is_empty() {
            return (Vec::new(), EndProposingReason::NoMoreTransactions, thread_info);
        }
        let mut thread_name = "single".to_string();
        if thread > 0 {
            thread_name = format!("{thread}");
        }
        let thread_start = time::Instant::now();
        let thread_time = thread_deadline.saturating_duration_since(thread_start);
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
            let (results, mut invalid_tx, future_or_exhausted) = block_builder.push_batch(batch_txs, timeout);
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
                trace!("[{invalid_hash:?}] Invalid transaction: {e}");
                unqueue_invalid.push(invalid_hash.clone());
            }
            for (index, _) in future_or_exhausted {
                let future_or_exhausted_hash = &execute_txs[index].1;
                filter_transactions.remove(future_or_exhausted_hash);
            }
            if let Some(break_reason) = should_break {
                break (break_reason, "HitBlockWeightLimit");
            }
        };
        thread_info.total = block_builder.extrinsics.len() - initial_applied;
        thread_info.transactions = filter_transactions.into_iter().collect();
        // TODO should we loop call this `E::extend_extrinsic`? does it will generate new transaction by extended result?
        let _extend_extrinsics = E::extend_extrinsic(&*block_builder.api, block_builder.parent_hash);
        Self::thread_finish_log(
            block,
            &thread_name,
            thread_start,
            thread_time,
            reason,
            &block_builder,
            &thread_info,
            round_execute_collect,
            limit_execution_time,
        );
        thread_info.time = thread_start.elapsed();

        (unqueue_invalid, end_reason, thread_info)
    }

    fn thread_finish_log(
        block: <<Block as BlockT>::Header as HeaderT>::Number,
        thread_name: &String,
        thread_start: time::Instant,
        thread_time: Duration,
        reason: &str,
        block_builder: &BlockBuilder<Block, C, B>,
        thread_info: &ThreadExecutionInfo<Block>,
        round_execute_collect: Vec<usize>,
        limit_execution_time: bool,
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
        let (round_times, times, execute_time, commit_time, rollback_time) = times(&block_builder, &thread_name);
        let extra = thread_start.elapsed().as_nanos().saturating_sub(execute_time);
        let round_with_times = round_execute_collect.iter().zip(round_times.into_iter()).map(|(txs, time)| {
            let tx_num = (*txs).max(1);
            let io_time = time[1] + time[3];
            let avg = time[0] as usize / tx_num;
            let avg_io = io_time as usize / tx_num;
            let avg_rb = time[2] as usize / tx_num;
            let avg_w = time[3] as usize / tx_num;
            (*txs, avg, avg.saturating_sub(avg_io), avg_io, avg_rb, avg_w)
        }).collect::<Vec<_>>();
        trace!(target: LOG_TARGET, "[Execute Block {block}] Thread {thread_name} extra: {extra}({commit_time}/{rollback_time}) nanos, times(min/avg/max/count): {times:?}");
        let limit_millis_info = if limit_execution_time {
            format!("/{}", thread_time.as_millis())
        } else {
            "".to_string()
        };
        let invalid_info = if thread_info.invalid > 0 || thread_info.future_or_exhausted > 0 {
            format!("({}/{})", thread_info.invalid, thread_info.future_or_exhausted)
        } else {
            "".to_string()
        };
        debug!(
            target: LOG_TARGET,
            "[Execute Block {block}] Thread {thread_name} {reason}({}{} ms) {}/{}{invalid_info} executed in {} rounds([(num, avg, avg_exe, avg_io, avg_rb, avg_w)]: {round_with_times:?}).",
            thread_start.elapsed().as_millis(),
            limit_millis_info,
            thread_info.applied(),
            thread_info.total,
            round_execute_collect.len(),
        );
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
        merge_tx: Sender<(MergeType<A, Block>, Option<ThreadExecutionInfo<Block>>)>,
        mut merge_rx: Receiver<(MergeType<A, Block>, Option<ThreadExecutionInfo<Block>>)>,
        merge_in_thread_order: bool,
        limit_execution_time: bool,
        recorder: &mut InfoRecorder<Block>,
    ) -> Result<(Vec<<A as TransactionPool>::Hash>, EndProposingReason), sp_blockchain::Error> {
        let mut merge_info = MergeInfo::default();
        let mut mth_merge = std::env::var("MTH_MERGE").unwrap_or("false".into()).parse().unwrap_or(false);
        if merge_in_thread_order { mth_merge = false; }
        let allow_rollback = std::env::var("MTH_MERGE_ROLLBACK").unwrap_or("true".into()).parse().unwrap_or(true);
        let mut merge_count = 0usize;
        let mut extra_merge_count = (time::Instant::now(), thread_number);
        let mut merge_box: Vec<(u32, MergeType<A, Block>)> = vec![];
        let pool = futures::executor::ThreadPoolBuilder::new()
            .pool_size(cmp::min(thread_number / 2, num_cpus::get()))
            .create()
            .expect("Failed to build groups merge pool");
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
                        recorder.threads.insert(thread_info.thread, thread_info);
                        extra_merge_count.1 -= 1;
                        if extra_merge_count.1 == 0 {
                            // all threads execute finished, initialize extra merge start time.
                            extra_merge_count.0 = time::Instant::now();
                            // all threads execute finished, single thread merge is faster.
                            // we do not need to merge same keys multi times(`mth_merge` actually merge much more changes).
                            mth_merge = false;
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
                    // final main block builder state.
                    block_builder.api.set_changes(changes.1);
                    block_builder.api.set_recorder(changes.2);
                    block_builder.extrinsics = changes.3;
                    merge_info.extra_merge_time = extra_merge_count.0.elapsed();
                    recorder.merge = Some(merge_info);
                    return Ok((changes.4, changes.5));
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
                        if !mth_merge {
                            Self::merge_process(merge_tx.clone(), block, &mbh, pre_changes, changes, inherents_len, allow_rollback, merge_in_thread_order);
                            break;
                        } else {
                            let merge_tx = merge_tx.clone();
                            let mbh = mbh.copy_state();
                            pool.spawn(async move {
                                Self::merge_process(merge_tx, block, &mbh, pre_changes, changes, inherents_len, allow_rollback, merge_in_thread_order);
                            })
                                .expect("Failed to spawn merge thread");
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }

    fn merge_process(
        mut merge_tx: Sender<(MergeType<A, Block>, Option<ThreadExecutionInfo<Block>>)>,
        block: <<Block as BlockT>::Header as HeaderT>::Number,
        mbh: &MBH,
        changes1: MergeType<A, Block>,
        changes2: MergeType<A, Block>,
        inherents_len: usize,
        allow_rollback: bool,
        static_order: bool,
    ) {
        let exe_merge_start = time::Instant::now();
        let merge_result = Self::merge_changes(&mbh, changes1, changes2, inherents_len, allow_rollback, static_order);
        let merge_time = exe_merge_start.elapsed().as_micros();
        let changes = match merge_result {
            Ok((changes, to_threads, from_threads)) => {
                debug!(target: LOG_TARGET, "[MergeCh Block {block}] Merge threads {to_threads:?} and {from_threads:?} in {merge_time} micros");
                changes
            },
            Err((keep, drop, e)) => {
                error!(
                    target: LOG_TARGET,
                    "[MergeCh Block {block}] Merge threads keep {:?}, drop [threads {:?}, {} extrinsics] in {merge_time} micros for error: {e:?}",
                    keep.0,
                    drop.0,
                    drop.3.len() + drop.4.len() - inherents_len,
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
        mut to: MergeType<A, Block>,
        mut from: MergeType<A, Block>,
        inherents_len: usize,
        allow_rollback: bool,
        static_order: bool,
    ) -> Result<(MergeType<A, Block>, Vec<usize>, Vec<usize>), (MergeType<A, Block>, MergeType<A, Block>, MergeErr)> {
        if !static_order && to.1.merge_weight::<MBH>() < from.1.merge_weight::<MBH>() {
            std::mem::swap(&mut to, &mut from);
        }
        // do as try merge for state
        // if success, do other merge.
        // if failed, return unchanged state.
        if allow_rollback {
            let mut tmp_state = to.1.clone();
            if let Err(e) = tmp_state.merge(&from.1, mbh, allow_rollback) {
                // since both state are not changed. we choose to keep `to` and drop `from`
                return Err((to, from, e));
            }
            to.1 = tmp_state;
        } else {
            if let Err(e) = to.1.merge(&from.1, mbh, allow_rollback) {
                // since `to` state is dirty, we should keep `from` and drop `to`
                return Err((from, to, e));
            }
        }
        let to_threads = to.0.clone();
        to.0.extend(from.0.clone());
        to.2 = match (to.2, from.2) {
            (Some(recorder1), Some(recorder2)) => {
                recorder1.merge(&recorder2);
                Some(recorder1)
            },
            (Some(recorder1), None) => Some(recorder1),
            (None, Some(recorder2)) => Some(recorder2),
            (None, None) => None,
        };
        to.3.extend(from.3[inherents_len..].to_vec());
        to.4.extend(from.4.clone());
        to.5 = from.5;
        Ok((to, to_threads, from.0))
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
                warn!(target: LOG_TARGET, "[OTC Block {number}] Create BlockBuilder error: {e:?}");
                return;
            },
        };
        let init_time = block_timer.elapsed().as_millis();
        let execute_timer = time::Instant::now();
        for (index, pending_tx_data) in extrinsic.into_iter().enumerate() {
            if let Err(e) = sc_block_builder::BlockBuilder::push(&mut block_builder, pending_tx_data.clone()) {
                error!(
                    target: LOG_TARGET,
                    "[OTC Block {number}] push extrinsic {index} error: {e:?}"
                );
                return;
            }
        };
        let execute_time = execute_timer.elapsed().as_millis();

        let build_timer = time::Instant::now();
        match block_builder.build() {
            Ok(res) => {
                let block_time = block_timer.elapsed().as_millis();
                let build_time = build_timer.elapsed().as_millis();
                let (block_res, storage_changes_res, _proof_res) = res.into_inner();
                // total block hash check
                if block_res.hash() == block.hash() {
                    info!(target: LOG_TARGET, "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] Check Block Hash success");
                    return;
                }
                let change_diff = |mth: &Vec<(StorageKey, Option<StorageValue>)>, oth: &Vec<(StorageKey, Option<StorageValue>)>| {
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
                                oth_diff.push(k.clone());
                            }
                        } else {
                            oth_extra.push(k.clone());
                        }
                    }
                    let oth_less: Vec<_> = mth_changes.keys().cloned().collect();
                    (oth_less, oth_diff, oth_extra)
                };
                // main_storage_changes check if block different.
                let (oth_main_less, oth_main_diff, oth_main_extra) = change_diff(&main_storage_changes, &storage_changes_res.main_storage_changes);
                error!(
                    target: LOG_TARGET,
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
                            target: LOG_TARGET,
                            "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] [one thread/multi threads] child change differences parent key: {parent_key:?}, less: {oth_child_less:?}, diff: {oth_child_diff:?}, extra: {oth_child_extra:?}",
                        );
                    } else {
                        let child_keys: Vec<_> = child_storage_changes.iter().map(|(key, _)| key.clone()).collect();
                        oth_extra_childs.push((parent_key.clone(), child_keys));
                    }
                }
                if !oth_extra_childs.is_empty() {
                    error!(target: LOG_TARGET, "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] oth extra child changes {oth_extra_childs:?}");
                }
                let mth_extra_childs: Vec<(StorageKey, Vec<StorageKey>)> = child_changes
                    .into_iter()
                    .map(|(parent, childs)| (parent, childs.into_iter().map(|(key, _)| key).collect()))
                    .collect();
                if !mth_extra_childs.is_empty() {
                    error!(target: LOG_TARGET, "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] mth extra child changes {mth_extra_childs:?}");
                }
            },
            Err(e) => {
                let block_time = block_timer.elapsed().as_millis();
                let build_time = build_timer.elapsed().as_millis();
                error!(target: LOG_TARGET, "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] Build block error: {e:?}");
            }
        }
    }

    fn collect_encode_decode(info: &str) {
        let mut encode = unhashed::GLOBAL_ENCODE.lock().unwrap();
        let mut decode = unhashed::GLOBAL_DECODE.lock().unwrap();
        let mut encode = encode
            .drain()
            .into_iter()
            .map(|(k, v)| (hex::encode(&k), v.len(), v.into_iter().map(|(t, _)| t).sum::<Duration>()))
            .collect::<Vec<_>>();
        let mut decode = decode
            .drain()
            .into_iter()
            .map(|(k, v)| (hex::encode(&k), v.len(), v.into_iter().map(|(t, _)| t).sum::<Duration>()))
            .collect::<Vec<_>>();
        encode.sort_by(|a, b| b.2.cmp(&a.2));
        decode.sort_by(|a, b| b.2.cmp(&a.2));
        let mut total_read_times = 0usize;
        let mut total_read_time = Duration::default();
        encode.iter().for_each(|(_, n, t)| {
            total_read_times += *n;
            total_read_time += *t;
        });
        let mut total_write_times = 0usize;
        let mut total_write_time = Duration::default();
        decode.iter().for_each(|(_, n, t)| {
            total_write_times += *n;
            total_write_time += *t;
        });
        log::info!(target: "codec", "{info} put: {total_read_time:?}({total_read_times}) {:?}", encode);
        log::info!(target: "codec", "{info} get: {total_write_time:?}({total_write_times}) {:?}", decode);
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
    E: ExtendExtrinsic + Send + Sync + 'static,
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
        let thread_number = extrinsic.0.len();

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
            merge_in_thread_order,
            limit_execution_time,
        )
            .await?;
        info!(
            target: LOG_TARGET,
			"🎁 [{source}] Prepared block {} [{}] \
			[hash: {:?}; parent_hash: {}; {}, threads {thread_number}]",
			info.number,
			info.time_info(limit_execution_time),
			<Block as BlockT>::Hash::from(proposal.block.header().hash()),
			proposal.block.header().parent_hash(),
            info.tx_info(),
		);
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
        let thread_number = multi_groups.len();
        let single_thread_extrinsic_num = groups.last().cloned().unwrap_or_default() as usize;
        let single: Vec<Block::Extrinsic> = extrinsic[offset..offset + single_thread_extrinsic_num].to_vec();
        let max_duration = time::Duration::from_secs(10);
        let block_builder =
            self.client.new_block_at(parent_hash, inherent_digests.clone(), PR::ENABLED, Some(context))?;
        let (proposal, info) = self.execute_block(
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
            merge_in_thread_order,
            limit_execution_time,
        )
            .await?;
        info!(
            target: LOG_TARGET,
			"🎁 [{source}] Prepared block {} [{}] \
			[hash: {:?}; parent_hash: {}; {}, threads {thread_number}]",
			info.number,
			info.time_info(limit_execution_time),
			<Block as BlockT>::Hash::from(proposal.block.header().hash()),
			proposal.block.header().parent_hash(),
            info.tx_info(),
		);
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
