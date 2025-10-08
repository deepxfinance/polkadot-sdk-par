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
use sp_api::{ApiExt, CallApiAt, MergeErr, OverlayedChanges, ProofRecorder, ProvideRuntimeApi, StorageKey, StorageProof, StorageValue};
use sp_spot_api::SpotRuntimeApi;
use sp_perp_api::PerpRuntimeApi;
use sp_blockchain::{ApplyExtrinsicFailed::Validity, Error::ApplyExtrinsicFailed, HeaderBackend};
use sp_consensus::{DisableProofRecording, EnableProofRecording, ProofRecording, Proposal};
use sp_core::traits::SpawnNamed;
use sp_inherents::InherentData;
use sp_runtime::{traits, traits::{Block as BlockT, Header as HeaderT}, Digest, Percent, SaturatedConversion, Saturating};
use std::{cmp, marker::PhantomData, pin::Pin, sync::Arc, time};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::Mul;
use futures::channel::mpsc::{Sender, Receiver};
use futures::task::SpawnExt;
use prometheus_endpoint::Registry as PrometheusRegistry;
use sc_proposer_metrics::{EndProposingReason, MetricsLink as PrometheusMetrics};
use sp_runtime::traits::{Header, One};
use crate::DEFAULT_BLOCK_SIZE_LIMIT;
use super::{ExtendExtrinsic, MultiThreadBlockBuilder, StepBlockPropose};

const DEFAULT_SOFT_DEADLINE_PERCENT: Percent = Percent::from_percent(70);

/// [`Proposer`] factory.
pub struct ProposerFactory<A, B, C, PR, MBH, E> {
    spawn_handle: Box<dyn SpawnNamed>,
    /// The client instance.
    client: Arc<C>,
    /// The transaction pool.
    transaction_pool: Arc<A>,
    /// Native version,
    native_version: sp_version::NativeVersion,
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
    /// Merge execute result percentage of soft_deadline_percent.
    merge_deadline_percent: Percent,
    /// Parallel threads limit for execution(not greater thran physical cpu threads).
    thread_limit: usize,
    /// Thread transaction number per millis.
    millis_tx_rate: usize,
    /// Thread default initialize round execute tx_number.
    default_round_tx: usize,
    telemetry: Option<TelemetryHandle>,
    /// When estimating the block size, should the proof be included?
    include_proof_in_block_size_estimation: bool,
    /// phantom member to pin the `Backend`/`ProofRecording`/`MultiThreadBlockBuilder`/`ExtendExtrinsic` type.
    _phantom: PhantomData<(B, PR, MBH, E)>,
}

impl<A, B, C, MBH, E> ProposerFactory<A, B, C, DisableProofRecording, MBH, E> {
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
        native_version: sp_version::NativeVersion,
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
        let pool_deadline_percent: u8 = std::env::var("MTH_POOL_DEADLINE_PERCENT").unwrap_or("10".into()).parse().unwrap_or(10);
        let pool_deadline_percent = Percent::from_percent(pool_deadline_percent);
        let merge_deadline_percent: u8 = std::env::var("MTH_MERGE_DEADLINE_PERCENT").unwrap_or("10".into()).parse().unwrap_or(10);
        let merge_deadline_percent = Percent::from_percent(merge_deadline_percent);
        if merge_deadline_percent.mul(soft_deadline_percent).deconstruct() + pool_deadline_percent.deconstruct() >= soft_deadline_percent.deconstruct() {
            panic!("`pool_deadline_percent` + `merge_deadline_percent` * `soft_deadline_percent` should not be greater equal than `soft_deadline_percent`");
        }
        let mut thread_limit: usize = match std::env::var("MTH_THREAD_LIMIT") {
            Ok(limit) => limit.parse().expect("`MTH_THREAD_LIMIT` should be a usize"),
            Err(_) => num_cpus::get(),
        };
        thread_limit = thread_limit.min(num_cpus::get());
        let millis_tx_rate: usize = match std::env::var("MTH_MILLIS_TX_RATE") {
            Ok(rate) => rate.parse().expect("`MTH_MILLIS_TX_RATE` should be a usize"),
            Err(_) => 5,
        };
        let default_round_tx: usize = match std::env::var("MTH_DEFAULT_ROUND_TX") {
            Ok(tx) => tx.parse().expect("`MTH_DEFAULT_ROUND_TX` should be a usize"),
            Err(_) => 0,
        };
        info!(
            target: "mth_authorship",
            "ProposerFactory init soft_deadline_percent: {soft_deadline_percent:?}, pool_deadline_percent: {pool_deadline_percent:?}, merge_deadline_percent: {merge_deadline_percent:?}, thread_limit: {thread_limit}, millis_tx_rate: {millis_tx_rate:?}, default_round_tx: {default_round_tx}",
        );
        ProposerFactory {
            spawn_handle: Box::new(spawn_handle),
            transaction_pool,
            native_version,
            metrics: PrometheusMetrics::new(prometheus),
            default_block_size_limit: DEFAULT_BLOCK_SIZE_LIMIT,
            soft_deadline_percent,
            pool_deadline_percent,
            merge_deadline_percent,
            thread_limit,
            millis_tx_rate,
            default_round_tx,
            telemetry,
            client,
            include_proof_in_block_size_estimation: false,
            _phantom: PhantomData,
        }
    }
}

impl<A, B, C, MBH, E> ProposerFactory<A, B, C, EnableProofRecording, MBH, E> {
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

impl<A, B, C, PR, MBH, E> ProposerFactory<A, B, C, PR, MBH, E> {
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

impl<B, Block, C, A, PR, MBH, E> ProposerFactory<A, B, C, PR, MBH, E>
where
    A: TransactionPool<Block = Block, Hash = Block::Hash> + 'static,
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
    MBH: MultiThreadBlockBuilder<B, Block, C::Api>,
    E: ExtendExtrinsic + Send + Sync + 'static,
{
    fn init_with_now(
        &mut self,
        parent_header: &<Block as BlockT>::Header,
        now: Box<dyn Fn() -> time::Instant + Send + Sync>,
    ) -> Proposer<B, Block, C, A, PR, MBH, E> {
        let parent_hash = parent_header.hash();

        info!("🙌 Starting consensus session on top of parent {:?}", parent_hash);

        let proposer = Proposer::<_, _, _, _, PR, MBH, E> {
            spawn_handle: self.spawn_handle.clone(),
            client: self.client.clone(),
            parent_hash,
            parent_number: *parent_header.number(),
            transaction_pool: self.transaction_pool.clone(),
            native_version: sp_version::NativeVersion {
                runtime_version: self.native_version.runtime_version.clone(),
                can_author_with: self.native_version.can_author_with.clone(),
            },
            now,
            metrics: self.metrics.clone(),
            default_block_size_limit: self.default_block_size_limit,
            soft_deadline_percent: self.soft_deadline_percent,
            pool_deadline_percent: self.pool_deadline_percent,
            merge_deadline_percent: self.merge_deadline_percent,
            thread_limit: self.thread_limit,
            millis_tx_rate: self.millis_tx_rate,
            default_round_tx: self.default_round_tx,
            telemetry: self.telemetry.clone(),
            _phantom: PhantomData,
            include_proof_in_block_size_estimation: self.include_proof_in_block_size_estimation,
        };

        proposer
    }
}

impl<A, B, Block, C, PR, MBH, E> sp_consensus::Environment<Block> for ProposerFactory<A, B, C, PR, MBH, E>
where
    A: TransactionPool<Block = Block, Hash = Block::Hash> + 'static,
    <A as TransactionPool>::InPoolTransaction: Send + Sync + 'static,
    B: backend::Backend<Block> + Send + Sync + 'static,
    Block: BlockT,
    C: BlockBuilderProvider<B, Block, C>
    + HeaderBackend<Block>
    + ProvideRuntimeApi<Block>
    + CallApiAt<Block>
    + CloneForExecution
    + Send
    + Sync
    + 'static,
    C::Api:
    ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>> + BlockBuilderApi<Block> + SpotRuntimeApi<Block> + PerpRuntimeApi<Block>,
    PR: ProofRecording,
    MBH: MultiThreadBlockBuilder<B, Block, C::Api> + Send + Sync + 'static,
    E: ExtendExtrinsic + Send + Sync + 'static,
{
    type Proposer = Proposer<B, Block, C, A, PR, MBH, E>;
    type CreateProposer = future::Ready<Result<Self::Proposer, Self::Error>>;
    type Error = sp_blockchain::Error;

    fn init(&mut self, parent_header: &<Block as BlockT>::Header) -> Self::CreateProposer {
        future::ready(Ok(self.init_with_now(parent_header, Box::new(time::Instant::now))))
    }
}

/// The proposer logic.
pub struct Proposer<B, Block: BlockT, C: ProvideRuntimeApi<Block>, A: TransactionPool, PR, MBH: MultiThreadBlockBuilder<B, Block, C::Api>, E: ExtendExtrinsic> {
    spawn_handle: Box<dyn SpawnNamed>,
    client: Arc<C>,
    parent_hash: Block::Hash,
    parent_number: <<Block as BlockT>::Header as HeaderT>::Number,
    transaction_pool: Arc<A>,
    native_version: sp_version::NativeVersion,
    now: Box<dyn Fn() -> time::Instant + Send + Sync>,
    metrics: PrometheusMetrics,
    default_block_size_limit: usize,
    include_proof_in_block_size_estimation: bool,
    soft_deadline_percent: Percent,
    pool_deadline_percent: Percent,
    merge_deadline_percent: Percent,
    thread_limit: usize,
    millis_tx_rate: usize,
    default_round_tx: usize,
    telemetry: Option<TelemetryHandle>,
    _phantom: PhantomData<(B, PR, MBH, E)>,
}

impl<A, B, Block, C, PR, MBH, E> sp_consensus::Proposer<Block> for Proposer<B, Block, C, A, PR, MBH, E>
where
    A: TransactionPool<Block = Block, Hash = Block::Hash> + 'static,
    <A as TransactionPool>::InPoolTransaction: Send + Sync + 'static,
    B: backend::Backend<Block> + Send + Sync + 'static,
    Block: BlockT,
    C: BlockBuilderProvider<B, Block, C>
    + HeaderBackend<Block>
    + ProvideRuntimeApi<Block>
    + CallApiAt<Block>
    + CloneForExecution
    + Send
    + Sync
    + 'static,
    C::Api:
    ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>> + BlockBuilderApi<Block> + SpotRuntimeApi<Block> + PerpRuntimeApi<Block>,
    PR: ProofRecording,
    MBH: MultiThreadBlockBuilder<B, Block, C::Api> + Send + Sync + 'static,
    E: ExtendExtrinsic + Send + Sync + 'static,
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

impl<A, B, Block, C, PR, MBH, E> Proposer<B, Block, C, A, PR, MBH, E>
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
    async fn propose_with(
        self,
        inherent_data: InherentData,
        inherent_digests: Digest,
        max_duration: time::Duration,
        block_size_limit: Option<usize>,
    ) -> Result<Proposal<Block, backend::TransactionFor<B, Block>, PR::Proof>, sp_blockchain::Error>
    where
        C: CloneForExecution,
    {
        let propose_with_start = time::Instant::now();
        let deadline = time::Instant::now() + max_duration;
        // 1. initialize main thread
        let block_builder =
            self.client.new_block_at(self.parent_hash.clone(), inherent_digests.clone(), PR::ENABLED)?;
        // 2. prepare transactions by group.
        let block_size_limit = block_size_limit.unwrap_or(self.default_block_size_limit);
        let (extrinsic_group, single_group, raw_groups, extrinsic_count, pool_time) = self.group_transactions_from_pool(
            deadline.saturating_duration_since((self.now)()) / 10,
            deadline.clone(),
            block_size_limit,
            block_builder.estimated_header_size + block_builder.extrinsics.encoded_size(),
            if self.include_proof_in_block_size_estimation {
                block_builder.api.proof_recorder().map(|pr| pr.estimate_encoded_size()).unwrap_or(0)
            } else {
                0
            },
            self.thread_limit,
            &vec![],
        )
            .await
            .unwrap();
        debug!(
            target: "mth_authorship",
            "[Execute Block {}] GroupTx use {} ms({} micros)({raw_groups} -> {} groups, {extrinsic_count} txs)",
            self.parent_number + One::one(),
            pool_time.2,
            pool_time.1,
            extrinsic_group.len(),
        );
        // 3. propose block
        self.execute_block(
            propose_with_start,
            deadline,
            inherent_data,
            inherent_digests,
            block_builder,
            max_duration,
            block_size_limit,
            extrinsic_count,
            pool_time,
            extrinsic_group
                .into_iter()
                .map(|g| g.iter().map(|(i, tx)| (*i, Some(tx.hash().clone()), tx.data().clone())).collect())
                .collect(),
            single_group.iter().map(|(i, tx)| (*i, Some(tx.hash().clone()), tx.data().clone())).collect(),
            false,
        ).await
    }

    async fn execute_block<'a>(
        &self,
        propose_with_start: time::Instant,
        deadline: time::Instant,
        inherent_data: InherentData,
        inherent_digests: Digest,
        mut block_builder: BlockBuilder<'a, Block, C, B>,
        max_duration: time::Duration,
        block_size_limit: usize,
        extrinsic_count: usize,
        pool_time: (u128, u128, u128),
        mut extrinsic_group: Vec<Vec<(usize, Option<<A as TransactionPool>::Hash>, Block::Extrinsic)>>,
        mut single_group: Vec<(usize, Option<<A as TransactionPool>::Hash>, Block::Extrinsic)>,
        merge_in_thread_order: bool,
    ) -> Result<Proposal<Block, backend::TransactionFor<B, Block>, PR::Proof>, sp_blockchain::Error>
    where
        C: CloneForExecution,
    {
        let mut native = true;
        // if native version not latest, we can only execute in single thread.
        let onchain_version = CallApiAt::runtime_version_at(&*self.client, self.parent_hash)
            .map_err(|e| sp_blockchain::Error::RuntimeApiError(e))?;
        if !onchain_version.can_call_with(&self.native_version.runtime_version) {
            native = false;
            let groups = std::mem::replace(&mut extrinsic_group, vec![]);
            single_group = [groups.into_iter().flatten().collect(), single_group].concat();
        }
        // 1. initialize main thread block_builder by inherent transactions.
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

        // 2. execute main process for multi thread and single thread.
        let (thread_number, mth_time, mth_applied, mth_invalid, extra_merge_time, single_exe_time, single_applied, single_invalid, final_end_reason) = self.multi_single_process(
            self.parent_number + One::one(),
            &mut block_builder,
            deadline,
            self.soft_deadline_percent.mul_floor(max_duration.as_micros()),
            &self.merge_deadline_percent,
            extrinsic_group,
            single_group,
            inherents.clone(),
            merge_in_thread_order,
        ).await?;
        self.transaction_pool.remove_invalid(&mth_invalid);
        self.transaction_pool.remove_invalid(&single_invalid);

        // 5. build block by finalize block.
        let block_size =
            block_builder.estimate_block_size(self.include_proof_in_block_size_estimation);
        if block_size > block_size_limit && block_builder.extrinsics.is_empty() {
            warn!("Hit block size limit of `{block_size_limit}` without including any transaction!");
        }
        let finalize_exe_start = time::Instant::now();
        let (block, storage_changes, proof) = block_builder.build()?.into_inner();
        let finalize_exe_time = finalize_exe_start.elapsed().as_millis();

        // 6. spawn a single execution check if env `MTH_CHECK` is true, this is just an extra check, weill not block main block build.
        let single_thread_check = std::env::var("MTH_CHECK").unwrap_or("false".into()).parse().unwrap_or(false);
        if single_thread_check && thread_number > 0 {
            let client = self.client.clone_for_execution();
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

        self.metrics.report(|metrics| {
            metrics.number_of_transactions.set(block.extrinsics().len() as u64);
            metrics.block_constructed.observe(propose_with_start.elapsed().as_secs_f64());

            metrics.report_end_proposing_reason(final_end_reason);
        });

        info!(
			"🎁 Prepared block for proposing at {} [{}/{} ms ({}({}) {mth_time}({extra_merge_time}) {single_exe_time} {finalize_exe_time})ms] \
			[hash: {:?}; parent_hash: {}; native: {native}; extrinsics [({mth_applied}/{})/({single_applied}/{})/{}], threads {thread_number}]",
			block.header().number(),
			propose_with_start.elapsed().as_millis(),
            max_duration.as_millis(),
            pool_time.2,
            pool_time.0,
			<Block as BlockT>::Hash::from(block.header().hash()),
			block.header().parent_hash(),
            mth_invalid.len(),
            single_invalid.len(),
            extrinsic_count + inherent_length,
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
        wait_pool: time::Duration,
        deadline: time::Instant,
        block_size_limit: usize,
        mut block_size: usize,
        mut proof_size: usize,
        thread_limit: usize,
        excepts: &[Block::Extrinsic],
    ) -> Result<(Vec<Vec<(usize, Arc<<A as TransactionPool>::InPoolTransaction>)>>, Vec<(usize, Arc<<A as TransactionPool>::InPoolTransaction>)>, usize, usize, (u128, u128, u128)), String> {
        let pool_timer = time::Instant::now();
        let mut except_set = HashSet::new();
        for tx in excepts {
            let tx_hash: <Block as BlockT>::Hash = <<Block::Header as Header>::Hashing as traits::Hash>::hash(&tx.encode());
            except_set.insert(tx_hash);
        }
        let block = self.parent_number + One::one();
        let mut t1 = self.transaction_pool.ready_at(self.parent_number).fuse();
        let mut t2 = futures_timer::Delay::new(wait_pool).fuse();
        let mut pending_iterator = select! {
			res = t1 => res,
			_ = t2 => {
				warn!("[GroupTx Block {block}] Timeout fired waiting for transaction pool. Proceeding with production.");
				self.transaction_pool.ready()
			},
		};
        let get_pool_time = pool_timer.elapsed().as_millis();

        // TODO Better limit for pool(e.g. weight).
        let left_micros: u64 = deadline.saturating_duration_since(pool_timer).as_micros().saturated_into();
        let execute_percent = self.soft_deadline_percent.saturating_sub(self.pool_deadline_percent).saturating_sub(self.merge_deadline_percent);
        let execute_time = execute_percent.mul_floor(left_micros) / 1000;
        let thread_tx_limit = execute_time as usize * self.millis_tx_rate;
        let tx_limit = thread_tx_limit * thread_limit;
        let pool_time = time::Duration::from_micros(self.pool_deadline_percent.mul_floor(left_micros));
        let pool_deadline = time::Instant::now() + pool_time;

        let mut skipped = 0;
        let mut pool_extrinsic_count = 0usize;
        let start = time::Instant::now();
        // collect transactions' group info and dispatch to different groups.
        let mut grouper = ConflictGroup::new();
        // loop for
        // 1. get enough transaction from pool.
        // 2. spawn mission for every transaction:
        //      parse transaction runtime call group info and return by channel.
        loop {
            if pool_extrinsic_count >= tx_limit {
                debug!(target: "mth_authorship", "[GroupTx Block {block}] Reach tx_limit {}/{} ms (total {pool_extrinsic_count}, block size: {}/{block_size_limit})", start.elapsed().as_millis(), pool_time.as_millis(), block_size + proof_size);
                break;
            }
            if time::Instant::now() > pool_deadline {
                debug!(target: "mth_authorship", "[GroupTx Block {block}] Reach deadline {}/{} ms (total {pool_extrinsic_count}, block size: {}/{block_size_limit})", start.elapsed().as_millis(), pool_time.as_millis(), block_size + proof_size);
                break;
            }
            let pending_tx = if let Some(pending_tx) = pending_iterator.next() {
                pending_tx
            } else {
                debug!(target: "mth_authorship", "[GroupTx Block {block}] Out of transactions({} ms, total {pool_extrinsic_count}, block size: {}/{block_size_limit})", start.elapsed().as_millis(), block_size + proof_size);
                break;
            };
            if except_set.remove(pending_tx.hash()) {
                continue;
            }

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
                    debug!(target: "mth_authorship", "[GroupTx Block {block}] Reached block size limit({}/{block_size_limit}) with extrinsic: {pool_extrinsic_count} in {} ms. start execute transactions.", block_size + proof_size, start.elapsed().as_millis());
                    break;
                }
            }
            block_size += pending_tx.data().encoded_size();
            grouper.insert(pending_tx.group_info().to_vec(), (pool_extrinsic_count, pending_tx));
            // TODO update proof_size with new extrinsic. This is hard since we do not actually execute the extrinsic.
            proof_size += 0;
            pool_extrinsic_count += 1;
        }
        let (mut group_txs, raw_groups, sort_time) = grouper.group(thread_limit);
        let mut single_group: Vec<_> = grouper.single_group();
        if group_txs.len() == 1 {
            single_group = [group_txs.pop().unwrap(), single_group].concat();
        }
        let pool_total_time = pool_timer.elapsed().as_millis();
        Ok((group_txs, single_group, raw_groups, pool_extrinsic_count, (get_pool_time, sort_time, pool_total_time)))
    }

    async fn multi_single_process<'a>(
        &self,
        block: <<Block as BlockT>::Header as HeaderT>::Number,
        block_builder: &mut BlockBuilder<'a, Block, C, B>,
        block_deadline: time::Instant,
        multi_single_micros: u128,
        merge_deadline_percent: &Percent,
        multi_groups: Vec<Vec<(usize, Option<<A as TransactionPool>::Hash>, Block::Extrinsic)>>,
        single_group: Vec<(usize, Option<<A as TransactionPool>::Hash>, Block::Extrinsic)>,
        inherents: Vec<Block::Extrinsic>,
        merge_in_thread_order: bool,
    ) -> Result<(usize, u128, usize, Vec<<A as TransactionPool>::Hash>, u128, u128, usize, Vec<<A as TransactionPool>::Hash>, EndProposingReason), sp_blockchain::Error>
    where
        C: CloneForExecution,
    {
        if multi_groups.is_empty() && single_group.is_empty() {
            return Ok((0, 0, 0, Vec::new(), 0, 0, 0, Vec::new(), EndProposingReason::NoMoreTransactions))
        }
        let calculate_start = time::Instant::now();
        let mut max_mth_length = 0u32;
        multi_groups.iter().for_each(|g| max_mth_length = max_mth_length.max(g.len() as u32));

        let merge_deadline_time = merge_deadline_percent.mul_floor(multi_single_micros);
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
        debug!(
            target: "mth_authorship",
            "[Execute Block {block}] ExecuteLimits: threads {thread_number}, mth_exe(mth_merge) {}({}) ms, single_exe {} ms, total execute_deadline {} ms",
            mth_execute_time.as_millis(),
            mth_merge_time.as_millis(),
            single_time.as_millis(),
            execute_time / 1000,
        );

        let (mth_time, mth_applied, mth_invalid_hash, extra_merge_time, mut final_end_reason) = if !multi_groups.is_empty() {
            let initial_applied = block_builder.extrinsics.len();
            let (merge_tx, merge_rx) = mpsc::channel(thread_number);
            let mut mbh = MBH::default();
            mbh.prepare(&block_builder.backend, &block_builder.parent_hash, &block_builder.api);
            // 4. execute group transaction by threads.
            self.spawn_execute_groups(
                block_builder,
                block_deadline.clone(),
                mth_execute_deadline.clone(),
                inherents.clone(),
                multi_groups,
                merge_tx.clone(),
            );

            // 5. merge all threads' changes to main block builder.
            let (mth_unqueue_invalid, end_reason, extra_merge_time) = self.merge_threads_result(
                mth_execute_time.as_millis() + mth_merge_time.as_millis(),
                block_builder,
                &mbh,
                inherents.len(),
                mth_finish_deadline,
                thread_number,
                merge_tx,
                merge_rx,
                merge_in_thread_order,
            ).await?;
            self.transaction_pool.remove_invalid(&mth_unqueue_invalid);
            let mth_applied = block_builder.extrinsics.len() - initial_applied;
            (calculate_start.elapsed().as_millis(), mth_applied, mth_unqueue_invalid, extra_merge_time, end_reason)
        } else {
            (0, 0, Vec::new(), 0, EndProposingReason::NoMoreTransactions)
        };

        // 6. execute all single thread transactions.
        let single_exe_start = time::Instant::now();
        let (single_invalid, end_reason) = Self::execute_one_thread_txs(
            self.parent_number + One::one(),
            block_deadline,
            single_deadline,
            block_builder,
            inherents.clone(),
            single_group,
            thread_number,
            self.default_round_tx,
            self.millis_tx_rate,
            false,
        );
        let single_exe_time = single_exe_start.elapsed().as_millis();
        if !block_builder.extrinsics.is_empty() || !single_invalid.is_empty() {
            final_end_reason = end_reason;
        }
        let single_applied = block_builder.extrinsics.len().saturating_sub(mth_applied).saturating_sub(inherents.len());

        Ok((thread_number, mth_time, mth_applied, mth_invalid_hash, extra_merge_time, single_exe_time, single_applied, single_invalid, final_end_reason))
    }

    fn spawn_execute_groups(
        &self,
        block_builder: &mut BlockBuilder<Block, C, B>,
        block_deadline: time::Instant,
        mth_execute_deadline: time::Instant,
        inherents: Vec<<Block as BlockT>::Extrinsic>,
        extrinsic_group: Vec<Vec<(usize, Option<<A as TransactionPool>::Hash>, Block::Extrinsic)>>,
        merge_tx: Sender<(MergeType<A, Block>, bool)>,
    ) where
        C: CloneForExecution,
    {
        let (init_changes, _, init_recorder) = block_builder.api.take_all_changes();
        for (i, pending_txs) in extrinsic_group.into_iter().enumerate() {
            let thread_inherents = inherents.clone();
            let parent_hash = self.parent_hash.clone();
            let estimated_header_size = block_builder.estimated_header_size.clone();
            let extrinsics = block_builder.extrinsics.clone();
            let init_changes = init_changes.clone();
            let init_recorder = init_recorder.clone();
            let thread_client = self.client.clone_for_execution();
            let default_round_tx = self.default_round_tx;
            let millis_tx_rate = self.millis_tx_rate;
            let block = self.parent_number + One::one();
            let mut res_tx = merge_tx.clone();
            self.spawn_handle.spawn_blocking(
                "mth-authorship-proposer",
                None,
                Box::pin(async move {
                    let mut thread_builder = match thread_client.new_with_other(parent_hash, estimated_header_size) {
                        Ok(mut builder) => {
                            builder.extrinsics = extrinsics;
                            builder.api.set_changes(init_changes);
                            builder.api.set_recorder(init_recorder);
                            builder
                        },
                        Err(e) => {
                            error!("Could not create block builder for thread {} for error: {e:?}", i + 1);
                            return;
                        }
                    };
                    let (unqueue_invalid, end_reason) = Self::execute_one_thread_txs(
                        block.clone(),
                        block_deadline,
                        mth_execute_deadline,
                        &mut thread_builder,
                        thread_inherents.clone(),
                        pending_txs,
                        i + 1,
                        default_round_tx,
                        millis_tx_rate,
                        false,
                    );

                    let (overlay, _, recorder) = thread_builder.api.take_all_changes();
                    let changes = (vec![i + 1], overlay, recorder, thread_builder.extrinsics.clone(), unqueue_invalid, end_reason);
                    if res_tx.start_send((changes, false)).is_err() {
                        trace!("Could not send block production result to proposer!");
                    }
                }),
            );
        }
    }

    fn execute_one_thread_txs(
        block: <<Block as BlockT>::Header as HeaderT>::Number,
        block_deadline: time::Instant,
        thread_deadline: time::Instant,
        block_builder: &mut BlockBuilder<Block, C, B>,
        inherents: Vec<<Block as BlockT>::Extrinsic>,
        mut pending_txs: Vec<(usize, Option<<A as TransactionPool>::Hash>, Block::Extrinsic)>,
        thread: usize,
        default_round_tx: usize,
        millis_tx_rate: usize,
        extend: bool,
    ) -> (Vec<<A as TransactionPool>::Hash>, EndProposingReason) {
        if pending_txs.is_empty() {
            return (Vec::new(), EndProposingReason::NoMoreTransactions);
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
        let total_tx = pending_txs.len();
        let mut total_nonce_err_tx = 0usize;
        // thread txs should be same order with pool.
        pending_txs.sort_by(|a, b| a.0.cmp(&b.0));
        let mut round_execute_collect = Vec::new();
        let mut should_break = None;
        let mut tx_number = if default_round_tx > 0 {
            default_round_tx
        } else {
            thread_time.as_millis() as usize * millis_tx_rate
        };
        let (end_reason, reason) = loop {
            tx_number = tx_number.min(pending_txs.len());
            if pending_txs.is_empty() {
                break (EndProposingReason::NoMoreTransactions, "NoMoreTransactions")
            }
            if time::Instant::now() > thread_deadline {
                break (EndProposingReason::HitDeadline, "HitThreadDeadline")
            }
            let mut execute_txs = pending_txs[..tx_number].to_vec();
            pending_txs = pending_txs[tx_number..].to_vec();
            let batch_txs = execute_txs.iter().map(|(_, _, e)| e.clone()).collect();
            let batch_start = time::Instant::now();
            let timeout = thread_deadline.duration_since(batch_start);
            let (results, rollback, stale_or_futures) = block_builder.push_batch(batch_txs, timeout);
            let invalid_nonce_length = stale_or_futures.len();
            total_nonce_err_tx += invalid_nonce_length;
            let mut executed = results.len();
            round_execute_collect.push(executed - invalid_nonce_length);
            if rollback.is_empty() {
                for (_, result) in results.into_iter().enumerate() {
                    match result {
                        Ok(()) => (),
                        Err(ApplyExtrinsicFailed(Validity(e))) if e.exhausted_resources() => {
                            if !extend {
                                should_break = Some(EndProposingReason::HitBlockWeightLimit);
                            }
                        }
                        Err(ApplyExtrinsicFailed(Validity(e))) if e.stale_or_future() => (),
                        Err(e) => panic!("Err({e:?}) should Rollback"),
                    }
                }
            } else {
                executed = 0;
            }
            // clear invalid extrinsic.
            let mut invalid: Vec<_> = [rollback, stale_or_futures].into_iter().flatten().collect();
            invalid.sort_by(|a, b| a.0.cmp(&b.0));
            let mut remove_offset = 0usize;
            for (index, e) in invalid {
                if let Some(invalid_hash) = &execute_txs[index - remove_offset].1 {
                    trace!("[{invalid_hash:?}] Invalid transaction: {e}");
                    unqueue_invalid.push(invalid_hash.clone());
                }
                // drop this rollback extrinsic
                execute_txs.remove(index - remove_offset);
                remove_offset += 1;
            }
            // push unexecuted extrinsic back to `pending_txs`
            if execute_txs.len() > executed {
                pending_txs = [execute_txs[executed..].to_vec(), pending_txs].concat();
            }
            if let Some(break_reason) = should_break {
                break (break_reason, "HitBlockWeightLimit");
            }
        };
        let applied = block_builder.extrinsics.len() - initial_applied;
        Self::thread_finish_log(extend, block, &thread_name, thread_start, thread_time, reason, &block_builder, applied, unqueue_invalid.len(), total_nonce_err_tx, total_tx, round_execute_collect);

        // execute extend extrinsics which must execute finish.
        if !extend {
            let extend_extrinsics = E::extend_extrinsic(&*block_builder.api, block_builder.parent_hash);
            let round_tx_number = extend_extrinsics.len();
            let (_extend_unqueue_invalid, _extend_end_reason) = Self::execute_one_thread_txs(
                block,
                block_deadline,
                block_deadline,
                block_builder,
                inherents,
                extend_extrinsics.into_iter().enumerate().map(|(i, tx)| (i, None, tx)).collect(),
                thread,
                round_tx_number,
                millis_tx_rate,
                true,
            );
        }

        (unqueue_invalid, end_reason)
    }

    fn thread_finish_log(
        extend: bool,
        block: <<Block as BlockT>::Header as HeaderT>::Number,
        thread_name: &String,
        thread_start: time::Instant,
        thread_time: time::Duration,
        reason: &str,
        block_builder: &BlockBuilder<Block, C, B>,
        applied: usize,
        invalid: usize,
        total_nonce_err_tx: usize,
        total_tx: usize,
        round_execute_collect: Vec<usize>,
    ) {
        let times = |block_builder: &BlockBuilder<Block, C, B>, _thread: &String| -> (Vec<[u128; 4]>, Vec<(String, [u128; 4], usize)>, u128, u128, u128) {
            let mut execute_map: HashMap<String, ([u128; 4], usize)> = HashMap::new();
            let mut execute_time_count = 0u128;
            let mut round_times = Vec::new();
            let (execute_times, (commit_time, rollback_time)) = block_builder.api.execute_times();
            for (method, times) in execute_times {
                let method = String::from_utf8(method).unwrap_or("unknown".to_string());
                // trace!(target: "mth_authorship", "Thread {_thread}, method: {method:?}, exe/read/read_backend/write: {times:?} nanos");
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
        let thread_name = if extend { format!("{thread_name}-extend") } else { thread_name.clone() };
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
        trace!(target: "mth_authorship", "[Execute Block {block}] Thread {thread_name} extra: {extra}({commit_time}/{rollback_time}) nanos, times(min/avg/max/count): {times:?}");
        debug!(
            target: "mth_authorship",
            "[Execute Block {block}] Thread {thread_name} {reason}({}/{} ms) {applied}/{}/{total_nonce_err_tx}/{total_tx} executed in {} rounds([(num, avg, avg_exe, avg_io, avg_rb, avg_w)]: {round_with_times:?}).",
            thread_start.elapsed().as_millis(),
            thread_time.as_millis(),
            invalid.saturating_sub(total_nonce_err_tx),
            round_execute_collect.len(),
        );
    }

    async fn merge_threads_result<'a>(
        &self,
        mth_time: u128,
        block_builder: &mut BlockBuilder<'a, Block, C, B>,
        mbh: &MBH,
        inherents_len: usize,
        mth_finish_deadline: time::Instant,
        thread_number: usize,
        merge_tx: Sender<(MergeType<A, Block>, bool)>,
        mut merge_rx: Receiver<(MergeType<A, Block>, bool)>,
        merge_in_thread_order: bool,
    ) -> Result<(Vec<<A as TransactionPool>::Hash>, EndProposingReason, u128), sp_blockchain::Error> {
        let mut mth_merge = std::env::var("MTH_MERGE").unwrap_or("false".into()).parse().unwrap_or(false);
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
                        Ok(Some(res)) => all_changes.push(res),
                        _ => break,
                    }
                }
                for (changes, merged) in all_changes {
                    if merged {
                        merge_count += 1;
                    } else {
                        extra_merge_count.1 -= 1;
                        if extra_merge_count.1 == 0 {
                            // all threads execute finished, initialize extra merge start time.
                            extra_merge_count.0 = time::Instant::now();
                            // all threads execute finished, single thread merge is faster.
                            // we do not need to merge same keys multi times(`mth_merge` actually merge much more changes).
                            mth_merge = false;
                        }
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
                if merge_count + 1 == thread_number || time::Instant::now() > mth_finish_deadline {
                    let (_, mut changes) = merge_box.pop().unwrap();
                    if merge_count + 1 < thread_number {
                        warn!(
                            target: "mth_authorship",
                            "Timeout fired waiting for threads result execution for block #{} mth_time {mth_time} ms. Build block with current threads {:?}.",
                            self.parent_number + One::one(),
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
                    let extra_merge_time = extra_merge_count.0.elapsed().as_millis();
                    return Ok((changes.4, changes.5, extra_merge_time));
                }
                loop {
                    if merge_box.len() >= 2 {
                        let block = self.parent_number + One::one();
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
        mut merge_tx: Sender<(MergeType<A, Block>, bool)>,
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
                debug!(target: "mth_authorship", "[MergeCh Block {block}] Merge threads {to_threads:?} and {from_threads:?} in {merge_time} micros");
                changes
            },
            Err((keep, drop, e)) => {
                error!(
                    target: "mth_authorship",
                    "[MergeCh Block {block}] Merge threads keep {:?}, drop [threads {:?}, {} extrinsics] in {merge_time} micros for error: {e:?}",
                    keep.0,
                    drop.0,
                    drop.3.len() + drop.4.len() - inherents_len,
                );
                keep
            }
        };
        if merge_tx.start_send((changes, true)).is_err() {
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
        client: C,
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
        let mut block_builder = match client.new_block_at(parent_hash, inherent_digests, PR::ENABLED) {
            Ok(builder) => builder,
            Err(e) => {
                warn!(target: "mth_authorship", "[OTC Block {number}] Create BlockBuilder error: {e:?}");
                return;
            },
        };
        let init_time = block_timer.elapsed().as_millis();
        let execute_timer = time::Instant::now();
        for (index, pending_tx_data) in extrinsic.into_iter().enumerate() {
            if let Err(e) = sc_block_builder::BlockBuilder::push(&mut block_builder, pending_tx_data.clone()) {
                error!(
                    target: "mth_authorship",
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
                    info!(target: "mth_authorship", "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] Check Block Hash success");
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
                            target: "mth_authorship",
                            "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] [one thread/multi threads] child change differences parent key: {parent_key:?}, less: {oth_child_less:?}, diff: {oth_child_diff:?}, extra: {oth_child_extra:?}",
                        );
                    } else {
                        let child_keys: Vec<_> = child_storage_changes.iter().map(|(key, _)| key.clone()).collect();
                        oth_extra_childs.push((parent_key.clone(), child_keys));

                    }
                }
                if !oth_extra_childs.is_empty() {
                    error!(target: "mth_authorship", "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] oth extra child changes {oth_extra_childs:?}");
                }
                let mth_extra_childs: Vec<(StorageKey, Vec<StorageKey>)> = child_changes
                    .into_iter()
                    .map(|(parent, childs)| (parent, childs.into_iter().map(|(key, _)| key).collect()))
                    .collect();
                if !mth_extra_childs.is_empty() {
                    error!(target: "mth_authorship", "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] mth extra child changes {mth_extra_childs:?}");
                }
            },
            Err(e) => {
                let block_time = block_timer.elapsed().as_millis();
                let build_time = build_timer.elapsed().as_millis();
                error!(target: "mth_authorship", "[OTC Block {number}({block_time} ({init_time} {execute_time} {build_time}) ms)] Build block error: {e:?}");
            }
        }
    }
}

#[async_trait::async_trait]
impl<A, B, Block, C, PR, MBH, E> StepBlockPropose<Block> for Proposer<B, Block, C, A, PR, MBH, E>
where
    A: TransactionPool<Block = Block, Hash = Block::Hash> + 'static,
    <A as TransactionPool>::InPoolTransaction: Send + Sync + 'static,
    B: backend::Backend<Block> + Send + Sync + 'static,
    Block: BlockT,
    C: BlockBuilderProvider<B, Block, C>
    + HeaderBackend<Block>
    + ProvideRuntimeApi<Block>
    + CloneForExecution
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
    async fn extrinsic(
        &self,
        wait_pool: time::Duration,
        deadline: time::Instant,
        block_size_limit: Option<usize>,
        excepts: Vec<Block::Extrinsic>,
    ) -> (Vec<Vec<Block::Extrinsic>>, Vec<Block::Extrinsic>) {
        let block_size_limit = block_size_limit.unwrap_or(self.default_block_size_limit);
        let (groups, mut single, _, _, _) = self.group_transactions_from_pool(wait_pool, deadline, block_size_limit, 0, 0, self.thread_limit, &excepts).await.unwrap();
        let groups = groups
            .into_iter()
            .map(|mut g| {
                g.sort_by(|a, b| a.0.cmp(&b.0));
                g.into_iter().map(|(_, tx)| tx.data().clone()).collect()
            })
            .collect();
        single.sort_by(|a, b| a.0.cmp(&b.0));
        let single = single.into_iter().map(|(_, tx)| tx.data().clone()).collect();
        (groups, single)
    }

    async fn step_propose(
        self,
        block_size_limit: Option<usize>,
        inherent_data: InherentData,
        inherent_digests: Digest,
        extrinsic: (Vec<Vec<Block::Extrinsic>>, Vec<Block::Extrinsic>),
        merge_in_thread_order: bool,
    ) -> Result<
        Proposal<
            Block,
            <Self as sp_consensus::Proposer<Block>>::Transaction, <Self as sp_consensus::Proposer<Block>>::Proof
        >,
        <Self as sp_consensus::Proposer<Block>>::Error
    > {
        let max_duration = time::Duration::from_secs(3);
        let block_builder =
            self.client.new_block_at(self.parent_hash.clone(), inherent_digests.clone(), PR::ENABLED)?;
        let extrinsic_count = extrinsic.0.iter().map(|g| g.len()).sum::<usize>() + extrinsic.1.len();
        self.execute_block(
            time::Instant::now(),
            time::Instant::now() + max_duration,
            inherent_data,
            inherent_digests,
            block_builder,
            max_duration,
            block_size_limit.unwrap_or(self.default_block_size_limit),
            extrinsic_count,
            (0, 0, 0),
            extrinsic.0
                .into_iter()
                .map(|g| g.into_iter().enumerate().map(|(i, tx)| (i, Some(<<<Block as BlockT>::Header as Header>::Hashing as traits::Hash>::hash(&tx.encode())), tx)).collect())
                .collect(),
            extrinsic.1.into_iter().enumerate().map(|(i, tx)| (i, Some(<<<Block as BlockT>::Header as Header>::Hashing as traits::Hash>::hash(&tx.encode())), tx)).collect(),
            merge_in_thread_order,
        )
            .await
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

    pub fn group(&mut self, limit: usize) -> (Vec<Vec<V>>, usize, u128) {
        let mut groups = core::mem::take(&mut self.group).into_values().collect::<Vec<_>>();
        let groups_len = groups.len();
        if groups_len <= limit {
            return (groups, groups_len, 0);
        }
        let sort_start = time::Instant::now();
        groups.sort_by(|a, b| a.len().cmp(&b.len()));
        let mut results = groups.split_off(groups_len - limit);
        groups.truncate(groups_len - limit);
        for group in groups {
            results[0].extend(group);
            results.sort_by(|a, b| a.len().cmp(&b.len()));
        }
        (results, groups_len, sort_start.elapsed().as_micros())
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
