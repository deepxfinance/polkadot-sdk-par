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
use sc_block_builder::{BlockBuilder, BlockBuilderApi, BlockBuilderProvider};
use sc_client_api::{backend, CloneForExecution};
use sc_telemetry::{telemetry, TelemetryHandle, CONSENSUS_INFO};
use sc_transaction_pool_api::{InPoolTransaction, TransactionPool};
use sp_api::{ApiExt, MergeErr, OverlayedChanges, ProofRecorder, ProvideRuntimeApi, StorageKey, StorageProof, StorageValue};
use sp_blockchain::{ApplyExtrinsicFailed::Validity, Error::ApplyExtrinsicFailed, HeaderBackend};
use sp_consensus::{DisableProofRecording, EnableProofRecording, ProofRecording, Proposal};
use sp_core::traits::SpawnNamed;
use sp_inherents::InherentData;
use sp_runtime::{traits::{Block as BlockT, Header as HeaderT}, Digest, Percent, SaturatedConversion, Saturating};
use std::{cmp, marker::PhantomData, pin::Pin, sync::Arc, time};
use std::collections::HashMap;
use std::hash::Hash;
use futures::channel::mpsc::{Sender, Receiver};
use futures::task::SpawnExt;
use prometheus_endpoint::Registry as PrometheusRegistry;
use sc_proposer_metrics::{EndProposingReason, MetricsLink as PrometheusMetrics};
use sp_runtime::traits::One;
use crate::DEFAULT_BLOCK_SIZE_LIMIT;
use super::{RCGroup, MultiThreadBlockBuilder};

const DEFAULT_SOFT_DEADLINE_PERCENT: Percent = Percent::from_percent(50);

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
    /// Pool get transactions percentage of hard deadline.
    pool_deadline_percent: Percent,
    /// Merge execute result percentage of hard deadline.
    merge_deadline_percent: Percent,
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
        let soft_deadline_percent = match std::env::var("MTH_SOFT_DEADLINE_PERCENT") {
            Ok(percent) => match percent.parse() {
                Ok(percent) => Percent::from_percent(percent),
                Err(_) => DEFAULT_SOFT_DEADLINE_PERCENT,
            },
            Err(_) => DEFAULT_SOFT_DEADLINE_PERCENT,
        };
        if soft_deadline_percent.deconstruct() > 100 {
            panic!("`soft_deadline_percent` should not be greater than 100%");
        }
        let pool_deadline_percent: u8 = std::env::var("MTH_POOL_DEADLINE_PERCENT").unwrap_or("20".into()).parse().unwrap_or(20);
        let pool_deadline_percent = Percent::from_percent(pool_deadline_percent);
        let merge_deadline_percent: u8 = std::env::var("MTH_MERGE_DEADLINE_PERCENT").unwrap_or("5".into()).parse().unwrap_or(5);
        let merge_deadline_percent = Percent::from_percent(merge_deadline_percent);
        if merge_deadline_percent.deconstruct() + pool_deadline_percent.deconstruct() >= soft_deadline_percent.deconstruct() {
            panic!("`pool_deadline_percent` + `merge_deadline_percent` should not be greater equal than `soft_deadline_percent`");
        }
        info!(
            target: "mth_authorship",
            "ProposerFactory init soft_deadline_percent: {soft_deadline_percent:?}, pool_deadline_percent: {pool_deadline_percent:?}, merge_deadline_percent: {merge_deadline_percent:?}",
        );
        ProposerFactory {
            spawn_handle: Box::new(spawn_handle),
            transaction_pool,
            metrics: PrometheusMetrics::new(prometheus),
            default_block_size_limit: DEFAULT_BLOCK_SIZE_LIMIT,
            soft_deadline_percent,
            pool_deadline_percent,
            merge_deadline_percent,
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
    MBH: MultiThreadBlockBuilder<B, Block, C::Api>,
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
            pool_deadline_percent: self.pool_deadline_percent,
            merge_deadline_percent: self.merge_deadline_percent,
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
    MBH: MultiThreadBlockBuilder<B, Block, C::Api> + Send + Sync + 'static,
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
pub struct Proposer<B, Block: BlockT, C: ProvideRuntimeApi<Block>, A: TransactionPool, PR, MBH: MultiThreadBlockBuilder<B, Block, C::Api>, RCG> {
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
    pool_deadline_percent: Percent,
    merge_deadline_percent: Percent,
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
    MBH: MultiThreadBlockBuilder<B, Block, C::Api> + Send + Sync + 'static,
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
                let res = self
                    .propose_with(inherent_data, inherent_digests, max_duration, block_size_limit)
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
    MBH: MultiThreadBlockBuilder<B, Block, C::Api> + Send + Sync + 'static,
    RCG: RCGroup + Send + Sync + 'static,
{
    async fn propose_with(
        self,
        inherent_data: InherentData,
        inherent_digests: Digest,
        max_duration: time::Duration,
        block_size_limit: Option<usize>,
    ) -> Result<Proposal<Block, backend::TransactionFor<B, Block>, PR::Proof>, sp_blockchain::Error>
    where
        C: CloneForExecution,
        <A as TransactionPool>::InPoolTransaction: Send + Sync + 'static,
    {
        let propose_with_start = time::Instant::now();
        let deadline = (self.now)() + max_duration;
        // 1. initialize main thread
        let mut block_builder =
            self.client.new_block_at(self.parent_hash.clone(), inherent_digests.clone(), PR::ENABLED)?;
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
        let block_timer = time::Instant::now();
        // 3. prepare transactions by group.
        let block_size_limit = block_size_limit.unwrap_or(self.default_block_size_limit);

        let (extrinsic_group, single_group, pool_time): (Vec<Vec<(usize, Arc<<A as TransactionPool>::InPoolTransaction>)>>, Vec<(usize, Arc<<A as TransactionPool>::InPoolTransaction>)>, (u128, u128)) = Default::default();

        let mut max_mth_length = 0u32;
        extrinsic_count += extrinsic_group.iter().map(|g| {
            max_mth_length = max_mth_length.max(g.len() as u32);
            g.len()
        }).sum::<usize>();
        extrinsic_count += single_group.len();
        let calculate_start = time::Instant::now();
        let elapsed = calculate_start.saturating_duration_since(propose_with_start).as_micros();
        let hard_deadline_time = max_duration.as_micros();
        let soft_deadline_time = self.soft_deadline_percent.mul_floor(hard_deadline_time);
        let merge_deadline_time = self.merge_deadline_percent.mul_floor(hard_deadline_time);
        let execute_time: u64 = soft_deadline_time.saturating_sub(elapsed).saturating_sub(merge_deadline_time).saturated_into();
        let mth_deadline_percent = Percent::from_rational(max_mth_length, max_mth_length + single_group.len() as u32);
        let single_deadline_percent = Percent::one().saturating_sub(mth_deadline_percent);
        let mth_execute_time = time::Duration::from_micros(mth_deadline_percent.mul_floor(execute_time));
        let mth_merge_time = time::Duration::from_micros(merge_deadline_time.saturated_into());
        let single_time = time::Duration::from_micros(single_deadline_percent.mul_floor(execute_time));
        let mth_execute_deadline = calculate_start + mth_execute_time;
        let mth_finish_deadline = mth_execute_deadline + mth_merge_time;
        let single_deadline = calculate_start + single_time;
        let thread_number = extrinsic_group.len();
        debug!(
            target: "mth_authorship",
            "[Execute Block {}] GroupTx use {} ms({} threads, {} txs), execute limit mth_exe_time {} ms, mth_merge_time {} ms, oth_time {} ms, soft_deadline {} ms({:?})",
            self.parent_number + One::one(),
            pool_time.1,
            thread_number,
            extrinsic_count - inherents.len(),
            mth_execute_time.as_millis(),
            mth_merge_time.as_millis(),
            single_time.as_millis(),
            soft_deadline_time / 1000,
            self.soft_deadline_percent,
        );

        let (mth_time, mth_applied, mth_invalid, extra_merge_time, mut final_end_reason) = if !extrinsic_group.is_empty() {
            let (merge_tx, merge_rx) = mpsc::channel::<(MergeType<A, Block>, bool)>(thread_number);
            // 4. execute group transaction by threads.

            // 5. merge all threads' changes to main block builder.
            (0, 0, 0, 0, EndProposingReason::NoMoreTransactions)
        } else {
            (0, 0, 0, 0, EndProposingReason::NoMoreTransactions)
        };

        // 6. execute all single thread transactions.
        let single_exe_start = time::Instant::now();
        let (single_applied, single_unqueue_invalid, end_reason) = Self::execute_one_thread_txs(
            self.parent_number + One::one(),
            single_deadline,
            &mut block_builder,
            inherents,
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
        let finalize_exe_start = time::Instant::now();
        let (block, storage_changes, proof) = block_builder.build()?.into_inner();
        let finalize_exe_time = finalize_exe_start.elapsed().as_millis();

        // 8. spawn a single execution check if env `MTH_CHECK` is true, this is just an extra check, weill not block main block build.

        self.metrics.report(|metrics| {
            metrics.number_of_transactions.set(block.extrinsics().len() as u64);
            metrics.block_constructed.observe(block_timer.elapsed().as_secs_f64());

            metrics.report_end_proposing_reason(final_end_reason);
        });

        info!(
			"🎁 Prepared block for proposing at {} [{}/{} ms ({}({}) {mth_time}({extra_merge_time}) {single_exe_time} {finalize_exe_time})ms] \
			[hash: {:?}; parent_hash: {}; extrinsics [({mth_applied}/{mth_invalid})/({}/{})/{extrinsic_count}], threads {thread_number}]",
			block.header().number(),
			block_timer.elapsed().as_millis(),
            max_duration.as_millis(),
            pool_time.1,
            pool_time.0,
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

    fn execute_one_thread_txs(
        block: <<Block as BlockT>::Header as HeaderT>::Number,
        thread_deadline: time::Instant,
        block_builder: &mut BlockBuilder<Block, C, B>,
        inherents: Vec<<Block as BlockT>::Extrinsic>,
        mut pending_txs: Vec<(usize, Arc<<A as TransactionPool>::InPoolTransaction>)>,
        thread: usize,
    ) -> (Vec<<Block as BlockT>::Extrinsic>, Vec<<A as TransactionPool>::Hash>, EndProposingReason) {
        if pending_txs.is_empty() {
            return (Vec::new(), Vec::new(), EndProposingReason::NoMoreTransactions);
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
        let mut skipped = 0usize;
        let mut unqueue_invalid = Vec::new();
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
                    "[Execute Block {block}] Thread {thread_name} finished({}/{} ms) {}/{}/{total_tx} executed.",
                    thread_start.elapsed().as_millis(),
                    thread_time.as_millis(),
                    block_builder.extrinsics.len() - inherents.len(),
                    unqueue_invalid.len(),
                );
                break EndProposingReason::NoMoreTransactions
            };

            let now = time::Instant::now();
            if now > thread_deadline {
                debug!(
                    target: "mth_authorship",
                    "[Execute Block {block}] Thread {thread_name} reached ThreadDeadline({}/{} ms) {}/{}/{total_tx} executed.",
                    thread_start.elapsed().as_millis(),
                    thread_time.as_millis(),
                    block_builder.extrinsics.len() - inherents.len(),
                    unqueue_invalid.len(),
                );
                break EndProposingReason::HitDeadline
            }

            let pending_tx_data = pending_tx.data().clone();
            let pending_tx_hash = pending_tx.hash().clone();

            trace!("[{:?}] Pushing to the Block {block}.", pending_tx_hash);
            match BlockBuilder::push(block_builder, pending_tx_data.clone()) {
                Ok(()) => (),
                Err(ApplyExtrinsicFailed(Validity(e))) if e.exhausted_resources() => {
                    if skipped < MAX_SKIPPED_TRANSACTIONS {
                        skipped += 1;
                        trace!(
                            "[Execute Block {block}] Thread {thread_name} block seems full, but will try {} more transactions before quitting.",
                            MAX_SKIPPED_TRANSACTIONS - skipped,
                        );
                    } else if time::Instant::now() < thread_deadline {
                        trace!(
                            target: "mth_authorship",
                            "[Execute Block {block}] Thread {thread_name} block seems full, but we still have time before the thread deadline, \
                                so we will try a bit more before quitting."
                        );
                    } else {
                        debug!(target: "mth_authorship", "[Execute Block {block}] Thread {thread_name} reached block weight limit, proceeding with proposing.");
                        break EndProposingReason::HitBlockWeightLimit
                    }
                },
                Err(e) => {
                    trace!(target: "mth_authorship", "[{:?}] Invalid transaction: {}", pending_tx_hash, e);
                    unqueue_invalid.push(pending_tx_hash);
                },
            }
        };
        (block_builder.extrinsics.clone(), unqueue_invalid, end_reason)
    }
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
