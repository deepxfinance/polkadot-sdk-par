// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: GPL-3.0-or-later WITH Classpath-exception-2.0

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

//! A special consensus proposer which propose block by multi threads for "basic" chains which use the primitive inherent-data.

// FIXME #1021 move this into sp-consensus

use codec::Encode;
use futures::{channel::{oneshot, mpsc}, future, future::{Future, FutureExt}, select, StreamExt};
use log::{debug, error, info, trace, warn};
use sc_block_builder::{BlockBuilder, BlockBuilderApi, BlockBuilderProvider, MultiThreadBlockBuilder};
use sc_client_api::{backend, CloneForExecution};
use sc_telemetry::{telemetry, TelemetryHandle, CONSENSUS_INFO};
use sc_transaction_pool_api::{InPoolTransaction, TransactionPool};
use sp_api::{ApiExt, MergeErr, OverlayedChanges, ProofRecorder, ProvideRuntimeApi, StorageKey, StorageProof, StorageTransactionCache, StorageValue};
use sp_blockchain::{ApplyExtrinsicFailed::Validity, Error::ApplyExtrinsicFailed, HeaderBackend};
use sp_consensus::{DisableProofRecording, EnableProofRecording, ProofRecording, Proposal};
use sp_core::traits::SpawnNamed;
use sp_inherents::InherentData;
use sp_runtime::{
    traits::{Block as BlockT, Header as HeaderT},
    Digest, Percent, SaturatedConversion,
};
use std::{marker::PhantomData, pin::Pin, sync::Arc, time};
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::DerefMut;
use futures::channel::mpsc::{Sender, Receiver};
use prometheus_endpoint::Registry as PrometheusRegistry;
use sc_proposer_metrics::{EndProposingReason, MetricsLink as PrometheusMetrics};
use sp_runtime::traits::One;
use crate::DEFAULT_BLOCK_SIZE_LIMIT;

const DEFAULT_SOFT_DEADLINE_PERCENT: Percent = Percent::from_percent(80);

/// [`Proposer`] factory.
pub struct ProposerFactory<A, B, C, PR, MBH, RCG> {
    spawn_handle: Box<dyn SpawnNamed>,
    /// The client instance.
    client: Arc<C>,
    /// The transaction pool.
    transaction_pool: Arc<A>,
    /// Prometheus Link,
    metrics: PrometheusMetrics,
    /// The default block size limit.
    ///
    /// If no `block_size_limit` is passed to [`sp_consensus::Proposer::propose`], this block size
    /// limit will be used.
    default_block_size_limit: usize,
    /// Soft deadline percentage of hard deadline.
    ///
    /// The value is used to compute soft deadline during block production.
    /// The soft deadline indicates where we should stop attempting to add transactions
    /// to the block, which exhaust resources. After soft deadline is reached,
    /// we switch to a fixed-amount mode, in which after we see `MAX_SKIPPED_TRANSACTIONS`
    /// transactions which exhaust resrouces, we will conclude that the block is full.
    soft_deadline_percent: Percent,
    telemetry: Option<TelemetryHandle>,
    /// When estimating the block size, should the proof be included?
    include_proof_in_block_size_estimation: bool,
    /// phantom member to pin the `Backend`/`ProofRecording`/`RuntimeCallGroup` type.
    _phantom: PhantomData<(B, PR, MBH, RCG)>,
}

impl<A, B, C, MBH, RCG> ProposerFactory<A, B, C, DisableProofRecording, MBH, RCG> {
    /// Create a new multi thread proposer factory.
    ///
    /// Proof recording will be disabled when using proposers built by this instance to build
    /// blocks.
    pub fn new(
        spawn_handle: impl SpawnNamed + 'static,
        client: Arc<C>,
        transaction_pool: Arc<A>,
        prometheus: Option<&PrometheusRegistry>,
        telemetry: Option<TelemetryHandle>,
    ) -> Self {
        ProposerFactory {
            spawn_handle: Box::new(spawn_handle),
            transaction_pool,
            metrics: PrometheusMetrics::new(prometheus),
            default_block_size_limit: DEFAULT_BLOCK_SIZE_LIMIT,
            soft_deadline_percent: DEFAULT_SOFT_DEADLINE_PERCENT,
            telemetry,
            client,
            include_proof_in_block_size_estimation: false,
            _phantom: PhantomData,
        }
    }
}

impl<A, B, C, MBH, RCG> ProposerFactory<A, B, C, EnableProofRecording, MBH, RCG> {
    /// Create a new multi thread proposer factory with proof recording enabled.
    ///
    /// Each proposer created by this instance will record a proof while building a block.
    ///
    /// This will also include the proof into the estimation of the block size. This can be disabled
    /// by calling [`ProposerFactory::disable_proof_in_block_size_estimation`].
    pub fn with_proof_recording(
        _spawn_handle: impl SpawnNamed + 'static,
        _client: Arc<C>,
        _transaction_pool: Arc<A>,
        _prometheus: Option<&PrometheusRegistry>,
        _telemetry: Option<TelemetryHandle>,
    ) -> Self {
        // TODO currently not able to estimate record proof size for pre load transactions.
        unimplemented!()
        // ProposerFactory {
        //     client,
        //     spawn_handle: Box::new(spawn_handle),
        //     transaction_pool,
        //     metrics: PrometheusMetrics::new(prometheus),
        //     default_block_size_limit: DEFAULT_BLOCK_SIZE_LIMIT,
        //     soft_deadline_percent: DEFAULT_SOFT_DEADLINE_PERCENT,
        //     telemetry,
        //     include_proof_in_block_size_estimation: true,
        //     _phantom: PhantomData,
        // }
    }

    /// Disable the proof inclusion when estimating the block size.
    pub fn disable_proof_in_block_size_estimation(&mut self) {
        self.include_proof_in_block_size_estimation = false;
    }
}

impl<A, B, C, PR, MBH, RCG> ProposerFactory<A, B, C, PR, MBH, RCG> {
    /// Set the default block size limit in bytes.
    ///
    /// The default value for the block size limit is:
    /// [`DEFAULT_BLOCK_SIZE_LIMIT`].
    ///
    /// If there is no block size limit passed to [`sp_consensus::Proposer::propose`], this value
    /// will be used.
    pub fn set_default_block_size_limit(&mut self, limit: usize) {
        self.default_block_size_limit = limit;
    }

    /// Set soft deadline percentage.
    ///
    /// The value is used to compute soft deadline during block production.
    /// The soft deadline indicates where we should stop attempting to add transactions
    /// to the block, which exhaust resources. After soft deadline is reached,
    /// we switch to a fixed-amount mode, in which after we see `MAX_SKIPPED_TRANSACTIONS`
    /// transactions which exhaust resrouces, we will conclude that the block is full.
    ///
    /// Setting the value too low will significantly limit the amount of transactions
    /// we try in case they exhaust resources. Setting the value too high can
    /// potentially open a DoS vector, where many "exhaust resources" transactions
    /// are being tried with no success, hence block producer ends up creating an empty block.
    pub fn set_soft_deadline(&mut self, percent: Percent) {
        self.soft_deadline_percent = percent;
    }
}

impl<B, Block, C, A, PR, MBH, RCG> ProposerFactory<A, B, C, PR, MBH, RCG>
where
    A: TransactionPool<Block = Block> + 'static,
    B: backend::Backend<Block> + Send + Sync + 'static,
    Block: BlockT,
    C: BlockBuilderProvider<B, Block, C>
    + HeaderBackend<Block>
    + ProvideRuntimeApi<Block>
    + Send
    + Sync
    + 'static,
    C::Api:
    ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>> + BlockBuilderApi<Block>,
    MBH: MultiThreadBlockBuilder<B, Block>,
    RCG: RCGroup + Send + Sync + 'static,
{
    fn init_with_now(
        &mut self,
        parent_header: &<Block as BlockT>::Header,
        now: Box<dyn Fn() -> time::Instant + Send + Sync>,
    ) -> Proposer<B, Block, C, A, PR, MBH, RCG> {
        let parent_hash = parent_header.hash();

        info!("🙌 Starting consensus session on top of parent {:?}", parent_hash);

        let proposer = Proposer::<_, _, _, _, PR, MBH, RCG> {
            spawn_handle: self.spawn_handle.clone(),
            client: self.client.clone(),
            parent_hash,
            parent_number: *parent_header.number(),
            transaction_pool: self.transaction_pool.clone(),
            now,
            metrics: self.metrics.clone(),
            default_block_size_limit: self.default_block_size_limit,
            soft_deadline_percent: self.soft_deadline_percent,
            telemetry: self.telemetry.clone(),
            _phantom: PhantomData,
            include_proof_in_block_size_estimation: self.include_proof_in_block_size_estimation,
        };

        proposer
    }
}

impl<A, B, Block, C, PR, MBH, RCG> sp_consensus::Environment<Block> for ProposerFactory<A, B, C, PR, MBH, RCG>
where
    A: TransactionPool<Block = Block> + 'static,
    <A as TransactionPool>::InPoolTransaction: Send + Sync + 'static,
    B: backend::Backend<Block> + Send + Sync + 'static,
    Block: BlockT,
    C: BlockBuilderProvider<B, Block, C>
    + HeaderBackend<Block>
    + ProvideRuntimeApi<Block>
    + CloneForExecution
    + Send
    + Sync
    + 'static,
    C::Api:
    ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>> + BlockBuilderApi<Block>,
    PR: ProofRecording,
    MBH: MultiThreadBlockBuilder<B, Block> + Send + Sync + 'static,
    RCG: RCGroup + Send + Sync + 'static,
{
    type Proposer = Proposer<B, Block, C, A, PR, MBH, RCG>;
    type CreateProposer = future::Ready<Result<Self::Proposer, Self::Error>>;
    type Error = sp_blockchain::Error;

    fn init(&mut self, parent_header: &<Block as BlockT>::Header) -> Self::CreateProposer {
        future::ready(Ok(self.init_with_now(parent_header, Box::new(time::Instant::now))))
    }
}

/// The proposer logic.
pub struct Proposer<B, Block: BlockT, C, A: TransactionPool, PR, MBH: MultiThreadBlockBuilder<B, Block>, RCG> {
    spawn_handle: Box<dyn SpawnNamed>,
    client: Arc<C>,
    parent_hash: Block::Hash,
    parent_number: <<Block as BlockT>::Header as HeaderT>::Number,
    transaction_pool: Arc<A>,
    now: Box<dyn Fn() -> time::Instant + Send + Sync>,
    metrics: PrometheusMetrics,
    default_block_size_limit: usize,
    include_proof_in_block_size_estimation: bool,
    soft_deadline_percent: Percent,
    telemetry: Option<TelemetryHandle>,
    _phantom: PhantomData<(B, PR, MBH, RCG)>,
}

impl<A, B, Block, C, PR, MBH, RCG> sp_consensus::Proposer<Block> for Proposer<B, Block, C, A, PR, MBH, RCG>
where
    A: TransactionPool<Block = Block> + 'static,
    <A as TransactionPool>::InPoolTransaction: Send + Sync + 'static,
    B: backend::Backend<Block> + Send + Sync + 'static,
    Block: BlockT,
    C: BlockBuilderProvider<B, Block, C>
    + HeaderBackend<Block>
    + ProvideRuntimeApi<Block>
    + CloneForExecution
    + Send
    + Sync
    + 'static,
    C::Api:
    ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>> + BlockBuilderApi<Block>,
    PR: ProofRecording,
    MBH: MultiThreadBlockBuilder<B, Block> + Send + Sync + 'static,
    RCG: RCGroup + Send + Sync + 'static,
{
    type Error = sp_blockchain::Error;
    type Transaction = backend::TransactionFor<B, Block>;
    type Proposal = Pin<
        Box<
            dyn Future<Output = Result<Proposal<Block, Self::Transaction, PR::Proof>, Self::Error>>
            + Send,
        >,
    >;
    type ProofRecording = PR;
    type Proof = PR::Proof;

    fn propose(
        self,
        inherent_data: InherentData,
        inherent_digests: Digest,
        max_duration: time::Duration,
        block_size_limit: Option<usize>,
    ) -> Self::Proposal {
        let (tx, rx) = oneshot::channel();
        let spawn_handle = self.spawn_handle.clone();

        spawn_handle.spawn_blocking(
            "mth-authorship-proposer",
            None,
            Box::pin(async move {
                // leave some time for evaluation and block finalization (10%)
                let deadline = (self.now)() + max_duration - max_duration / 10;
                let res = self
                    .propose_with(inherent_data, inherent_digests, deadline, block_size_limit)
                    .await;
                if tx.send(res).is_err() {
                    trace!("Could not send block production result to proposer!");
                }
            }),
        );

        async move { rx.await? }.boxed()
    }
}

/// If the block is full we will attempt to push at most
/// this number of transactions before quitting for real.
/// It allows us to increase block utilization.
const MAX_SKIPPED_TRANSACTIONS: usize = 8;
type ExecuteRes<A, B, Backend> = (usize, (OverlayedChanges, StorageTransactionCache<B, Backend>, Option<ProofRecorder<B>>), Vec<<B as BlockT>::Extrinsic>, Vec<<A as TransactionPool>::Hash>, EndProposingReason);
type MergeType<A, B> = (Vec<usize>, OverlayedChanges, Option<ProofRecorder<B>>, Vec<<B as BlockT>::Extrinsic>, Vec<<A as TransactionPool>::Hash>, EndProposingReason);

impl<A, B, Block, C, PR, MBH, RCG> Proposer<B, Block, C, A, PR, MBH, RCG>
where
    A: TransactionPool<Block = Block>,
    <A as TransactionPool>::InPoolTransaction: Send + Sync + 'static,
    B: backend::Backend<Block> + Send + Sync + 'static,
    Block: BlockT,
    C: BlockBuilderProvider<B, Block, C>
    + HeaderBackend<Block>
    + ProvideRuntimeApi<Block>
    + Send
    + Sync
    + 'static,
    C::Api:
    ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>> + BlockBuilderApi<Block>,
    PR: ProofRecording,
    MBH: MultiThreadBlockBuilder<B, Block> + Send + Sync + 'static,
    RCG: RCGroup + Send + Sync + 'static,
{
    async fn propose_with(
        self,
        inherent_data: InherentData,
        inherent_digests: Digest,
        deadline: time::Instant,
        block_size_limit: Option<usize>,
    ) -> Result<Proposal<Block, backend::TransactionFor<B, Block>, PR::Proof>, sp_blockchain::Error>
    where
        C: CloneForExecution,
        <A as TransactionPool>::InPoolTransaction: Send + Sync + 'static,
    {
        const SYSTEM_BLOCK_WEIGHT: [u8; 32] = [38, 170, 57, 78, 234, 86, 48, 224, 124, 72, 174, 12, 149, 88, 206, 247, 52, 171, 245, 203, 52, 214, 36, 67, 120, 205, 219, 241, 142, 132, 157, 150];

        let propose_with_start = time::Instant::now();
        let propose_time = deadline.saturating_duration_since(propose_with_start).as_millis();
        // 1. initialize main thread
        let mut block_builder =
            self.client.new_block_at(self.parent_hash.clone(), inherent_digests.clone(), PR::ENABLED)?;
        // get changes of initialize state.
        let mut init_change = vec![];
        if let Some(block_weight) = block_builder.api.get_top_change(&SYSTEM_BLOCK_WEIGHT.to_vec()) {
            init_change.push((SYSTEM_BLOCK_WEIGHT.to_vec(), block_weight));
        }
        // 2. initialize main thread block_builder by inherent transactions.
        let create_inherents_start = time::Instant::now();
        let inherents = block_builder.create_inherents(inherent_data)?;
        let create_inherents_end = time::Instant::now();
        let mut extrinsic_count = inherents.len();

        self.metrics.report(|metrics| {
            metrics.create_inherents_time.observe(
                create_inherents_end
                    .saturating_duration_since(create_inherents_start)
                    .as_secs_f64(),
            );
        });
        let inherent_length = inherents.len();
        for inherent in inherents {
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
        let block_timer = time::Instant::now();
        // 3. prepare transactions by group.
        let block_size_limit = block_size_limit.unwrap_or(self.default_block_size_limit);
        let (extrinsic_group, single_group) = self.group_transactions_from_pool(
            deadline.clone(),
            block_size_limit,
            block_builder.estimated_header_size + block_builder.extrinsics.encoded_size(),
            if self.include_proof_in_block_size_estimation {
                block_builder.api.proof_recorder().map(|pr| pr.estimate_encoded_size()).unwrap_or(0)
            } else {
                0
            },
        )
            .await
            .unwrap();
        let prepare_tx_time = block_timer.elapsed().as_millis();
        extrinsic_count += extrinsic_group.iter().map(|g| g.len()).sum::<usize>();
        extrinsic_count += single_group.len();

        let thread_number = extrinsic_group.len();
        let (mth_time, mth_applied, mth_invalid, extra_merge_time, mut final_end_reason) = if !extrinsic_group.is_empty() {
            let mth_start = time::Instant::now();
            let (thread_tx, thread_rx) = mpsc::channel(thread_number);
            // 4. execute group transaction by threads.
            self.spawn_execute_groups(
                deadline,
                inherent_digests.clone(),
                extrinsic_group,
                thread_tx,
            );

            // 5. merge all threads' changes to main block builder.
            let (mth_unqueue_invalid, end_reason, extra_merge_time) = self.merge_threads_result(
                propose_time,
                &mut block_builder,
                init_change,
                deadline,
                thread_number,
                thread_rx,
            )
                .await?;
            self.transaction_pool.remove_invalid(&mth_unqueue_invalid);
            let mth_applied = block_builder.extrinsics.len() - inherent_length;
            (mth_start.elapsed().as_millis(), mth_applied, mth_unqueue_invalid.len(), extra_merge_time, end_reason)
        } else {
            (0, 0, 0, 0, EndProposingReason::NoMoreTransactions)
        };

        // 6. execute all single thread transactions.
        let single_exe_start = time::Instant::now();
        let block = self.parent_number + One::one();
        let (single_applied, single_unqueue_invalid, end_reason) = Self::execute_one_thread_txs(
            block,
            deadline,
            self.soft_deadline_percent,
            &mut block_builder,
            single_group,
            thread_number,
        );
        let single_exe_time = single_exe_start.elapsed().as_millis();
        self.transaction_pool.remove_invalid(&single_unqueue_invalid);
        if !single_applied.is_empty() || !single_unqueue_invalid.is_empty() {
            final_end_reason = end_reason;
        }

        let block_size =
            block_builder.estimate_block_size(self.include_proof_in_block_size_estimation);
        if block_size > block_size_limit && block_builder.extrinsics.is_empty() {
            warn!("Hit block size limit of `{block_size_limit}` without including any transaction!");
        }

        // 7. build block by finalize block.
        let (block, storage_changes, proof) = block_builder.build()?.into_inner();

        // 8. spawn a single execution check if env `MTH_CHECK` is true, this is just an extra check, weill not block main block build.
        let single_thread_check = std::env::var("MTH_CHECK").unwrap_or("false".into()).parse().unwrap_or(false);
        if single_thread_check && thread_number > 0 {
            let client = self.client.clone_for_execution();
            let inherent_digests = inherent_digests.clone();
            let block = block.clone();
            let main_storage_changes = storage_changes.main_storage_changes.clone();
            let child_storage_changes = storage_changes.child_storage_changes.clone();
            let proof = proof.clone();
            self.spawn_handle.spawn_blocking(
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

        self.metrics.report(|metrics| {
            metrics.number_of_transactions.set(block.extrinsics().len() as u64);
            metrics.block_constructed.observe(block_timer.elapsed().as_secs_f64());

            metrics.report_end_proposing_reason(final_end_reason);
        });

        info!(
			"🎁 Prepared block for proposing at {} [{}/{propose_time}ms ({prepare_tx_time}ms {mth_time}({extra_merge_time})ms {single_exe_time}ms)] \
			[hash: {:?}; parent_hash: {}; extrinsics [({mth_applied}/{mth_invalid})/({}/{})/{extrinsic_count}], threads {thread_number}]",
			block.header().number(),
			block_timer.elapsed().as_millis(),
			<Block as BlockT>::Hash::from(block.header().hash()),
			block.header().parent_hash(),
            single_applied.len(),
            single_unqueue_invalid.len(),
		);
        telemetry!(
			self.telemetry;
			CONSENSUS_INFO;
			"prepared_block_for_proposing";
			"number" => ?block.header().number(),
			"hash" => ?<Block as BlockT>::Hash::from(block.header().hash()),
		);

        let proof =
            PR::into_proof(proof).map_err(|e| sp_blockchain::Error::Application(Box::new(e)))?;

        let propose_with_end = time::Instant::now();
        self.metrics.report(|metrics| {
            metrics.create_block_proposal_time.observe(
                propose_with_end.saturating_duration_since(propose_with_start).as_secs_f64(),
            );
        });

        Ok(Proposal { block, proof, storage_changes })
    }

    async fn group_transactions_from_pool(
        &self,
        deadline: time::Instant,
        block_size_limit: usize,
        mut block_size: usize,
        mut proof_size: usize,
    ) -> Result<(Vec<Vec<(usize, Arc<<A as TransactionPool>::InPoolTransaction>)>>, Vec<(usize, Arc<<A as TransactionPool>::InPoolTransaction>)>), String> {
        let block = self.parent_number + One::one();
        let mut t1 = self.transaction_pool.ready_at(self.parent_number).fuse();
        let mut t2 =
            futures_timer::Delay::new(deadline.saturating_duration_since((self.now)()) / 8).fuse();
        let mut pending_iterator = select! {
			res = t1 => res,
			_ = t2 => {
				warn!("[GroupTx Block {block}] Timeout fired waiting for transaction pool. Proceeding with production.");
				self.transaction_pool.ready()
			},
		};

        // TODO Better limit for pool(e.g. weight). We should set 30% deadline prepare time.
        let now = time::Instant::now();
        let left = deadline.saturating_duration_since(now);
        let left_micros: u64 = left.as_micros().saturated_into();
        let pool_deadline_percent = Percent::from_percent(30);
        let pool_time = time::Duration::from_micros(pool_deadline_percent.mul_floor(left_micros));
        let pool_deadline = time::Instant::now() + pool_time;
        let mut skipped = 0;
        // channel support max 2^16 rcg parse results.
        let (rcg_tx, mut rcg_rx) = mpsc::channel(65536);
        self.spawn_handle.spawn_blocking(
            "mth-authorship-proposer",
            None,
            Box::pin(async move {
                use futures::task::SpawnExt;
                let pool = futures::executor::ThreadPool::new().expect("Failed to build rcg parse pool");
                let mut pool_extrinsic_count = 0usize;
                // loop for
                // 1. get enough transaction from pool.
                // 2. spawn mission for every transaction:
                //      parse transaction runtime call group info and return by channel.
                loop {
                    if time::Instant::now() > pool_deadline {
                        debug!(target: "mth_authorship", "[GroupTx Block {block}] Reach deadline {}ms (total {pool_extrinsic_count})", pool_time.as_millis());
                        break;
                    }
                    let pending_tx = if let Some(pending_tx) = pending_iterator.next() {
                        pending_tx
                    } else {
                        debug!(target: "mth_authorship", "[GroupTx Block {block}] Out of transactions(total {pool_extrinsic_count})");
                        break;
                    };

                    let pending_tx_data = pending_tx.data().clone();
                    if block_size + pending_tx_data.encoded_size() + proof_size > block_size_limit {
                        if skipped < MAX_SKIPPED_TRANSACTIONS {
                            skipped += 1;
                            debug!(
                                "[GroupTx Block {block}] Transaction would overflow the block size limit, but will try {} more transactions before quitting.",
                                MAX_SKIPPED_TRANSACTIONS - skipped,
                            );
                            continue
                        } else {
                            debug!(target: "mth_authorship", "[GroupTx Block {block}] Reached block size limit with extrinsic: {pool_extrinsic_count}, start execute transactions.");
                            break;
                        }
                    }
                    block_size += pending_tx.data().encoded_size();
                    let mut rcg_tx_i = rcg_tx.clone();
                    pool.spawn(async move {
                        let rcg_info = RCG::call_dependent_data(pending_tx.data().encode());
                        if rcg_tx_i.start_send((pool_extrinsic_count, pending_tx, rcg_info)).is_err() {
                            trace!("Could not send call_group_data to transaction group, maybe channel is closed.");
                        }
                    })
                        .unwrap();
                    // TODO update proof_size with new extrinsic. This is hard since we do not actually execute the extrinsic.
                    proof_size += 0;
                    pool_extrinsic_count += 1;
                }
            })
        );
        // collect transactions' group info and dispatch to different groups.
        let mut grouper = ConflictGroup::new();
        loop {
            match rcg_rx.next().await {
                Some((tx_index, pending_tx, result)) => {
                    let call_group_data = match result {
                        Ok(data) => data,
                        Err(e) => {
                            warn!(target: "mth_authorship", "[GroupTx Block {block}] Parse RuntimeCallGroup for transaction {:?} failed for {e:?}", pending_tx.hash());
                            continue;
                        }
                    };
                    grouper.insert(call_group_data, (tx_index, pending_tx));
                },
                None => {
                    // finished
                    break;
                },
            };
        }
        let mut group_txs: Vec<_> = grouper.group().into_values().collect();
        let mut single_group: Vec<_> = grouper.single_group();
        if group_txs.len() == 1 {
            single_group = [group_txs.pop().unwrap(), single_group].concat();
        }
        Ok((group_txs, single_group))
    }

    fn spawn_execute_groups(
        &self,
        deadline: time::Instant,
        inherent_digests: Digest,
        extrinsic_group: Vec<Vec<(usize, Arc<<A as TransactionPool>::InPoolTransaction>)>>,
        thread_tx: Sender<Result<ExecuteRes<A, Block, <<C as ProvideRuntimeApi<Block>>::Api as ApiExt<Block>>::StateBackend>, sp_blockchain::Error>>,
    ) where
        C: CloneForExecution,
    {
        for (i, pending_txs) in extrinsic_group.into_iter().enumerate() {
            let parent_hash = self.parent_hash.clone();
            let inherent_digests_clone = inherent_digests.clone();
            let soft_deadline_percent = self.soft_deadline_percent.clone();
            let client_clone = self.client.clone_for_execution();
            let mut res_tx = thread_tx.clone();
            let block = self.parent_number + One::one();
            self.spawn_handle.spawn_blocking(
                "mth-authorship-proposer",
                None,
                Box::pin(async move {
                    let mut block_builder = match client_clone.new_block_at(parent_hash, inherent_digests_clone, PR::ENABLED) {
                        Ok(builder) => builder,
                        Err(e) => {
                            if res_tx.start_send(Err(e)).is_err() {
                                error!("Could not send block production err to proposer!");
                            }
                            return;
                        }
                    };
                    let (applied_extrinsics, unqueue_invalid, end_reason) = Self::execute_one_thread_txs(
                        block,
                        deadline,
                        soft_deadline_percent,
                        &mut block_builder,
                        pending_txs,
                        i
                    );

                    let results = block_builder.api.take_all_changes();
                    if res_tx.start_send(Ok((i, results, applied_extrinsics, unqueue_invalid, end_reason))).is_err() {
                        trace!("Could not send block production result to proposer!");
                    }
                }),
            );
        }
    }

    fn execute_one_thread_txs(
        block: <<Block as BlockT>::Header as HeaderT>::Number,
        deadline: time::Instant,
        soft_deadline_percent: Percent,
        block_builder: &mut BlockBuilder<Block, C, B>,
        mut pending_txs: Vec<(usize, Arc<<A as TransactionPool>::InPoolTransaction>)>,
        thread: usize,
    ) -> (Vec<<Block as BlockT>::Extrinsic>, Vec<<A as TransactionPool>::Hash>, EndProposingReason) {
        if pending_txs.is_empty() {
            return (Vec::new(), Vec::new(), EndProposingReason::NoMoreTransactions);
        }
        let now = time::Instant::now();
        let left = deadline.saturating_duration_since(now);
        let left_micros: u64 = left.as_micros().saturated_into();
        let thread_time = time::Duration::from_micros(soft_deadline_percent.mul_floor(left_micros));
        let thread_deadline = now + thread_time;
        let mut skipped = 0usize;
        let mut unqueue_invalid = Vec::new();
        let mut applied_extrinsics = Vec::new();
        let total_tx = pending_txs.len();
        // thread txs should be same order with pool.
        pending_txs.sort_by(|a, b| a.0.cmp(&b.0));
        let mut pending_iterator = pending_txs.into_iter();
        let end_reason = loop {
            let pending_tx = if let Some((_, pending_tx)) = pending_iterator.next() {
                pending_tx
            } else {
                debug!(
                    target: "mth_authorship",
                    "[Execute Block {block}] Thread {thread} finished({}/{}ms) {}/{}/{total_tx} executed.",
                    now.elapsed().as_millis(),
                    thread_time.as_millis(),
                    applied_extrinsics.len(),
                    unqueue_invalid.len(),
                );
                break EndProposingReason::NoMoreTransactions
            };

            let now = time::Instant::now();
            if now > thread_deadline {
                debug!(
                    target: "mth_authorship",
                    "[Execute Block {block}] Thread {thread} reached ThreadDeadline({}/{}ms) {}/{}/{total_tx} executed.",
                    left.as_millis(),
                    thread_time.as_millis(),
                    applied_extrinsics.len(),
                    unqueue_invalid.len(),
                );
                break EndProposingReason::HitDeadline
            }

            let pending_tx_data = pending_tx.data().clone();
            let pending_tx_hash = pending_tx.hash().clone();

            trace!("[{:?}] Pushing to the Block {block}.", pending_tx_hash);
            match BlockBuilder::push(block_builder, pending_tx_data.clone()) {
                Ok(()) => applied_extrinsics.push(pending_tx_data),
                Err(ApplyExtrinsicFailed(Validity(e))) if e.exhausted_resources() => {
                    if skipped < MAX_SKIPPED_TRANSACTIONS {
                        skipped += 1;
                        trace!(
                            "[Execute Block {block}] Thread {thread} block seems full, but will try {} more transactions before quitting.",
                            MAX_SKIPPED_TRANSACTIONS - skipped,
                        );
                    } else if time::Instant::now() < thread_deadline {
                        trace!(
                            target: "mth_authorship",
                            "[Execute Block {block}] Thread {thread} block seems full, but we still have time before the thread deadline, \
                                so we will try a bit more before quitting."
                        );
                    } else {
                        debug!(target: "mth_authorship", "[Execute Block {block}] Thread {thread} reached block weight limit, proceeding with proposing.");
                        break EndProposingReason::HitBlockWeightLimit
                    }
                },
                Err(e) => {
                    trace!(target: "mth_authorship", "[{:?}] Invalid transaction: {}", pending_tx_hash, e);
                    unqueue_invalid.push(pending_tx_hash);
                },
            }
        };
        (applied_extrinsics, unqueue_invalid, end_reason)
    }

    async fn merge_threads_result<'a>(
        &self,
        propose_time: u128,
        block_builder: &mut BlockBuilder<'a, Block, C, B>,
        init_change: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        deadline: time::Instant,
        thread_number: usize,
        mut thread_rx: Receiver<Result<ExecuteRes<A, Block, <<C as ProvideRuntimeApi<Block>>::Api as ApiExt<Block>>::StateBackend>, sp_blockchain::Error>>,
    ) -> Result<(Vec<<A as TransactionPool>::Hash>, EndProposingReason, u128), sp_blockchain::Error> {
        let mth_merge = std::env::var("MTH_MERGE").unwrap_or("true".into()).parse().unwrap_or(true);
        let allow_rollback = std::env::var("MTH_MERGE_ROLLBACK").unwrap_or("true".into()).parse().unwrap_or(true);
        let mut final_end_reason = EndProposingReason::NoMoreTransactions;
        let mut finish =
            futures_timer::Delay::new(deadline.saturating_duration_since((self.now)())).fuse();
        let mut total_unqueue_invalid = vec![];
        let mut merge_start = 0usize;
        let mut merge_count = 0usize;
        let mut extra_merge_count = (time::Instant::now(), thread_number);
        let mut merge_box: Vec<MergeType<A, Block>> = vec![];
        let (mut merge_tx, mut merge_rx) = mpsc::channel(thread_number);
        let mbh = MBH::default();
        mbh.pre_handle(&block_builder.backend, &block_builder.parent_hash, init_change.clone());
        let block = self.parent_number + One::one();
        loop {
            select! {
                res = thread_rx.next() => {
                    if let Some(result) = res {
                        let (i, (overlay, _, recorder), applied_extrinsics, unqueue_invalid, end_reason) = match result {
                            Ok(thread_result) => thread_result,
                            Err(e) => return Err(e),
                        };
                        let changes = (vec![i], overlay, recorder, applied_extrinsics, unqueue_invalid, end_reason);
                        merge_tx.start_send((changes, false)).unwrap();
                        extra_merge_count.1 -= 1;
                        if extra_merge_count.1 == 0 {
                            // all threads execute finished, initialize extra merge start time.
                            extra_merge_count.0 = time::Instant::now();
                        }
                    }
                }
                res = merge_rx.next() => {
                    if let Some((changes, merged)) = res {
                        if merged { merge_count += 1; }
                        if !mth_merge || (mth_merge && merge_start >= thread_number - 2) {
                            // final merge to main block builder.
                            let (threads, overlayed_changes, recorder, applied_extrinsics, unqueue_invalid, end_reason) = changes;
                            final_end_reason = end_reason;
                            total_unqueue_invalid.extend_from_slice(&unqueue_invalid);
                            merge_start += 1;
                            let exe_merge_start = time::Instant::now();
                            let extrinsic_length = applied_extrinsics.len() + unqueue_invalid.len();
                            if let Err(e) = block_builder.api.deref_mut().merge_all_changes(overlayed_changes, recorder, &mbh, allow_rollback) {
                                let merge_time = exe_merge_start.elapsed().as_nanos();
                                error!(target: "mth_authorship", "[MergeCh Block {block}] Merge threads {threads:?} to main builder drop {extrinsic_length} extrinsics in {merge_time}nanos for error: {e:?}");
                                if !allow_rollback {
                                    return Err(sp_blockchain::Error::Backend(format!("final merge threads {threads:?} {e:?}")));
                                }
                            } else {
                                block_builder.extrinsics.extend_from_slice(&applied_extrinsics);
                                let merge_time = exe_merge_start.elapsed().as_nanos();
                                debug!(target: "mth_authorship", "[MergeCh Block {block}] Merge threads {threads:?} to main builder in {merge_time}nanos");
                            }
                            merge_count += 1;
                            if merge_count == thread_number {
                                break;
                            }
                        } else if let Some(pre_changes) = merge_box.pop() {
                            let mut merge_res_tx = merge_tx.clone();
                            let mbh = mbh.copy_state();
                            let block = self.parent_number + One::one();
                            merge_start += 1;
                            self.spawn_handle.spawn_blocking(
                                "mth-authorship-proposer",
                                None,
                                Box::pin(async move {
                                    let exe_merge_start = time::Instant::now();
                                    let merge_result = Self::merge_thread_changes(&mbh, pre_changes, changes, allow_rollback);
                                    let merge_time = exe_merge_start.elapsed().as_nanos();
                                    let changes = match merge_result {
                                        Ok((changes, to_threads, from_threads)) => {
                                            debug!(target: "mth_authorship", "[MergeCh Block {block}] Merge threads {to_threads:?} and {from_threads:?} in {merge_time}nanos");
                                            changes
                                        },
                                        Err((keep, drop, e)) => {
                                            error!(
                                                target: "mth_authorship",
                                                "[MergeCh Block {block}] Merge threads keep {:?}, drop [threads {:?}, {} extrinsics] in {merge_time}nanos for error: {e:?}",
                                                keep.0,
                                                drop.0,
                                                drop.3.len() + drop.4.len(),
                                            );
                                            keep
                                        }
                                    };
                                    if merge_res_tx.start_send((changes, true)).is_err() {
                                        panic!("Could not send merge result to proposer!");
                                    }
                                }),
                            );
                        } else {
                            merge_box.push(changes);
                            merge_box.sort_by(|a, b| b.3.len().cmp(&a.3.len()));
                        }
                    }
                }
                _ = finish => {
                    warn!(
                        target: "mth_authorship",
                        "Timeout fired waiting for threads result execution for block #{} propose_time {propose_time}ms. Build block with current state.",
                        self.parent_number + One::one(),
                    );
                    break;
                }
            }
        }
        let extra_merge_time = extra_merge_count.0.elapsed().as_millis();
        Ok((total_unqueue_invalid, final_end_reason, extra_merge_time))
    }

    fn merge_thread_changes(
        mbh: &MBH,
        res1: MergeType<A, Block>,
        res2: MergeType<A, Block>,
        allow_rollback: bool,
    ) -> Result<(MergeType<A, Block>, Vec<usize>, Vec<usize>), (MergeType<A, Block>, MergeType<A, Block>, MergeErr)> {
        let (to_threads, from_threads, mut target, source) = if res1.3.len() >= res2.3.len() {
            (res1.0.clone(), res2.0.clone(), res1, res2)
        } else {
            (res2.0.clone(), res1.0.clone(), res2, res1)
        };
        // do as try merge for state
        // if success, do other merge.
        // if failed, return unchanged state.
        let mut tmp_state = OverlayedChanges::default();
        let merge_state = if allow_rollback {
            tmp_state = target.1.clone();
            &mut tmp_state
        } else {
            &mut target.1
        };
        if let Err(e) = merge_state.merge(&source.1, mbh, allow_rollback) {
            return Err((target, source, e));
        } else if allow_rollback {
            target.1 = tmp_state;
        }
        target.0.extend(source.0.clone());
        target.2 = match (target.2, source.2) {
            (Some(recorder1), Some(recorder2)) => {
                recorder1.merge(&recorder2);
                Some(recorder1)
            },
            (Some(recorder1), None) => Some(recorder1),
            (None, Some(recorder2)) => Some(recorder2),
            (None, None) => None,
        };
        target.3.extend(source.3.clone());
        target.4.extend(source.4.clone());
        target.5 = source.5;
        Ok((target, to_threads, from_threads))
    }

    async fn one_thread_build_check(
        client: C,
        inherent_digests: Digest,
        block: Block,
        main_storage_changes: Vec<(StorageKey, Option<StorageValue>)>,
        child_storage_changes: Vec<(StorageKey, Vec<(StorageKey, Option<StorageValue>)>)>,
        _proof: Option<StorageProof>,
    ) {
        let header = block.header();
        let number = header.number().clone();
        let parent_hash = header.parent_hash().clone();
        let extrinsic = block.extrinsics().to_vec();
        let mut block_builder = match client.new_block_at(parent_hash, inherent_digests, PR::ENABLED) {
            Ok(builder) => builder,
            Err(e) => {
                warn!(target: "mth_authorship", "[OTC Block {number}] Create BlockBuilder error: {e:?}");
                return;
            },
        };
        for (index, pending_tx_data) in extrinsic.into_iter().enumerate() {
            if let Err(e) = sc_block_builder::BlockBuilder::push(&mut block_builder, pending_tx_data.clone()) {
                error!(
                    target: "mth_authorship",
                    "[OTC Block {number}] push extrinsic {index} error: {e:?}"
                );
                return;
            }
        };

        match block_builder.build() {
            Ok(res) => {
               let (block_res, storage_changes_res, _proof_res) = res.into_inner();
                // total block hash check
                if block_res.hash() == block.hash() {
                    info!(target: "mth_authorship", "[OTC Block {number}] Check Block Hash success");
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
                    target: "mth_authorship",
                    "[OTC Block {number}] [one thread/multi threads] main change differences, less: {oth_main_less:?}, diff: {oth_main_diff:?}, extra: {oth_main_extra:?}",
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
                            target: "mth_authorship",
                            "[OTC Block {number}] [one thread/multi threads] child change differences parent key: {parent_key:?}, less: {oth_child_less:?}, diff: {oth_child_diff:?}, extra: {oth_child_extra:?}",
                        );
                    } else {
                        let child_keys: Vec<_> = child_storage_changes.iter().map(|(key, _)| key.clone()).collect();
                        oth_extra_childs.push((parent_key.clone(), child_keys));

                    }
                }
                if !oth_extra_childs.is_empty() {
                    error!(target: "mth_authorship", "[OTC Block {number}] oth extra child changes {oth_extra_childs:?}");
                }
                let mth_extra_childs: Vec<(StorageKey, Vec<StorageKey>)> = child_changes
                    .into_iter()
                    .map(|(parent, childs)| (parent, childs.into_iter().map(|(key, _)| key).collect()))
                    .collect();
                if !mth_extra_childs.is_empty() {
                    error!(target: "mth_authorship", "[OTC Block {number}] mth extra child changes {mth_extra_childs:?}");
                }
            },
            Err(e) => {
                error!(target: "mth_authorship", "[OTC Block {number}] Build block error: {e:?}");
            }
        }
    }
}

pub trait RCGroup {
    /// parse runtime call, return dependent data for dispatch call to groups
    /// If return empty return, we will execute the transaction in a single thread for unknow transaction.
    fn call_dependent_data(tx_data: Vec<u8>) -> Result<Vec<Vec<u8>>, String>;
}

pub struct ConflictGroup<K, V> {
    group_id: usize,
    group: HashMap<usize, Vec<V>>,
    single_group: Vec<V>,
    key_group: HashMap<K, usize>,
    group_keys: HashMap<usize, Vec<K>>,
}

impl<K: PartialEq + Eq + Hash + Clone, V> ConflictGroup<K, V>  {
    pub fn new() -> Self {
        Self {
            group_id: 0,
            group: HashMap::new(),
            single_group: Vec::new(),
            key_group: HashMap::new(),
            group_keys: HashMap::new(),
        }
    }

    /// Group value with some specific keys.
    /// If two values have some conflict key, they should be merged into one group.
    /// If keys is empty, it is considered as ungroupable value, we just insert to a single group.
    /// As a result, values with any conflict key should be finally in same group.
    pub fn insert(&mut self, keys: Vec<K>, value: V) {
        // if keys is empty, the value will be push to single group.
        if keys.is_empty() {
            self.single_group.push(value);
            return;
        }
        // by default, we will insert new value to new group id.
        let mut gid = self.group_id;
        for key in &keys {
            // if some dependent key is already dispatched to group id, this value should be in this group.
            if let Some(id) = self.key_group.get(key) {
                gid = *id;
                break;
            }
        };
        let mut tmp_move_groups = vec![];
        // merge conflict groups into one group
        for key in &keys {
            if let Some(used_group) = self.key_group.remove(key) {
                if used_group != gid {
                    let move_keys = self.group_keys.remove(&used_group).unwrap_or_default();
                    // update move_key -> gid map.
                    for move_key in &move_keys {
                        self.key_group.insert(move_key.clone(), gid);
                    }
                    // insert move_keys to gid
                    if let Some(keys) = self.group_keys.get_mut(&gid) {
                        keys.extend_from_slice(&move_keys);
                    } else {
                        self.group_keys.insert(gid, move_keys);
                    }
                    // move transactions from used_group to gid.
                    let move_values = self.group.remove(&used_group).unwrap_or_default();
                    tmp_move_groups.push((used_group, move_values.len()));
                    if let Some(values) = self.group.get_mut(&gid) {
                        values.extend(move_values);
                    } else {
                        self.group.insert(gid, move_values);
                    }
                }
            } else {
                if let Some(keys) = self.group_keys.get_mut(&gid) {
                    keys.push(key.clone());
                } else {
                    self.group_keys.insert(gid, vec![key.clone()]);
                }
            }
            // record key group_id
            self.key_group.insert(key.clone(), gid);
        }
        if !tmp_move_groups.is_empty() {
            debug!(target: "mth_authorship", "ConflictGroup move to group {gid} with groups: {tmp_move_groups:?}");
        }
        // insert tx to gid
        if let Some(values) = self.group.get_mut(&gid) {
            values.push(value);
        } else {
            self.group.insert(gid, vec![value]);
        }
        // if new group_id used, group_id += 1
        if gid == self.group_id {
            self.group_id += 1;
        }
    }

    pub fn group(&mut self) -> HashMap<usize, Vec<V>> {
        std::mem::take(&mut self.group)
    }

    pub fn single_group(&mut self) -> Vec<V> {
        std::mem::take(&mut self.single_group)
    }
}

#[test]
fn test_conflict_group() {
    let mut conflict_group: ConflictGroup<u32, String> = ConflictGroup::new();
    // gid 0
    conflict_group.insert(vec![0], "0".to_string());
    // gid 1
    conflict_group.insert(vec![1], "1".to_string());
    // gid 2
    conflict_group.insert(vec![2], "2".to_string());
    // gid 3
    conflict_group.insert(vec![3], "3".to_string());
    // this insert will merge "1", "4" to gid 1.
    conflict_group.insert(vec![1, 4], "4".to_string());
    // this insert will merge "2", "5" to gid 2.
    conflict_group.insert(vec![2, 5], "5".to_string());
    // this insert will group "2", "3", "5", "6" to gid 3.
    conflict_group.insert(vec![3, 5], "6".to_string());

    assert_eq!(conflict_group.key_group.remove(&0), Some(0));
    assert_eq!(conflict_group.key_group.remove(&1), Some(1));
    assert_eq!(conflict_group.key_group.remove(&2), Some(3));
    assert_eq!(conflict_group.key_group.remove(&3), Some(3));
    assert_eq!(conflict_group.key_group.remove(&4), Some(1));
    assert_eq!(conflict_group.key_group.remove(&5), Some(3));

    assert_eq!(conflict_group.group_keys.remove(&0), Some(vec![0]));
    assert_eq!(conflict_group.group_keys.remove(&1), Some(vec![1, 4]));
    assert_eq!(conflict_group.group_keys.remove(&2), None);
    assert_eq!(conflict_group.group_keys.remove(&3), Some(vec![3, 2, 5]));

    assert_eq!(conflict_group.group.remove(&0), Some(vec!["0".to_string()]));
    assert_eq!(conflict_group.group.remove(&1), Some(vec!["1".to_string(), "4".to_string()]));
    assert_eq!(conflict_group.group.remove(&2), None);
    assert_eq!(conflict_group.group.remove(&3), Some(vec!["3".to_string(), "2".to_string(), "5".to_string(), "6".to_string()]));
}

