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
use futures::{channel::oneshot, future, future::{Future, FutureExt}};
use log::{debug, info, trace};
use sc_block_builder::{BlockBuilderApi, BlockBuilderProvider};
use sc_client_api::backend;
use sc_telemetry::{telemetry, TelemetryHandle, CONSENSUS_INFO};
use sc_transaction_pool_api::{InPoolTransaction, TransactionPool};
use sp_api::{ApiExt, CallApiAt, ProvideRuntimeApi};
use sp_spot_api::SpotRuntimeApi;
use sp_perp_api::PerpRuntimeApi;
use sp_blockchain::HeaderBackend;
use sp_consensus::{DisableProofRecording, EnableProofRecording, ProofRecording, Proposal};
use sp_core::traits::SpawnNamed;
use sp_inherents::InherentData;
use sp_runtime::{traits::{Block as BlockT, Header as HeaderT}, Digest};
use std::{marker::PhantomData, pin::Pin, sync::Arc, time};
use std::time::Duration;
use prometheus_endpoint::Registry as PrometheusRegistry;
use sc_proposer_metrics::MetricsLink as PrometheusMetrics;
use sp_core::ExecutionContext;
use sp_runtime::traits::One;
use crate::{BlockExecuteInfo, BlockPropose, GroupTransaction};
use crate::mth_authorship::executor::BlockExecutor;
use crate::mth_authorship::groups::TransactionGrouper;
use crate::mth_authorship::oracle::BlockOracle;
use super::{ExtendExtrinsic, MultiThreadBlockBuilder};

const LOG_TARGET: &str = "authorship";

/// [`Proposer`] factory.
pub struct ProposerFactory<A, B, C, PR, O, MBH, E> {
    spawn_handle: Box<dyn SpawnNamed>,
    /// The client instance.
    client: Arc<C>,
    /// The transaction pool.
    transaction_pool: Arc<A>,
    /// Execution oracle.
    oracle: Arc<O>,
    /// Native version,
    native_version: sp_version::NativeVersion,
    /// Prometheus Link,
    metrics: PrometheusMetrics,
    telemetry: Option<TelemetryHandle>,
    /// When estimating the block size, should the proof be included?
    include_proof_in_block_size_estimation: bool,
    /// phantom member to pin the `Backend`/`ProofRecording`/`MultiThreadBlockBuilder`/`ExtendExtrinsic` type.
    _phantom: PhantomData<(B, PR, MBH, E)>,
}

impl<A, B, O, C, MBH, E> ProposerFactory<A, B, C, DisableProofRecording, O, MBH, E> {
    /// Create a new multi thread proposer factory.
    ///
    /// Proof recording will be disabled when using proposers built by this instance to build
    /// blocks.
    pub fn new(
        spawn_handle: impl SpawnNamed + 'static,
        client: Arc<C>,
        transaction_pool: Arc<A>,
        oracle: Arc<O>,
        prometheus: Option<&PrometheusRegistry>,
        telemetry: Option<TelemetryHandle>,
        native_version: sp_version::NativeVersion,
    ) -> Self {
        let spawn_handle = Box::new(spawn_handle);
        ProposerFactory {
            spawn_handle,
            transaction_pool,
            oracle,
            native_version,
            metrics: PrometheusMetrics::new(prometheus),
            telemetry,
            client,
            include_proof_in_block_size_estimation: false,
            _phantom: PhantomData,
        }
    }
}

impl<A, B, O, C, MBH, E> ProposerFactory<A, B, C, EnableProofRecording, O, MBH, E> {
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

impl<B, Block, C, A, PR, O, MBH, E> ProposerFactory<A, B, C, PR, O, MBH, E>
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
    O: BlockOracle<Block> + Send + Sync + 'static,
    MBH: MultiThreadBlockBuilder<B, Block, C::Api> + Send + Sync + 'static,
    E: ExtendExtrinsic + Send + Sync + 'static,
    PR: ProofRecording + Send + Sync + 'static,
{
    fn init_with_now(
        &mut self,
        parent_header: &<Block as BlockT>::Header,
        _now: Box<dyn Fn() -> time::Instant + Send + Sync>,
    ) -> Proposer<B, Block, C, A, PR, O, MBH, E> {
        let parent_hash = parent_header.hash();
        let block_executor = BlockExecutor::new(
            self.spawn_handle.clone(),
            self.client.clone(),
            self.transaction_pool.clone(),
            sp_version::NativeVersion{
                runtime_version: self.native_version.runtime_version.clone(),
                can_author_with: self.native_version.can_author_with.clone(),
            },
            self.metrics.clone(),
            self.telemetry.clone(),
        );
        let proposer = Proposer::<_, _, _, _, PR, _, MBH, E> {
            spawn_handle: self.spawn_handle.clone(),
            client: self.client.clone(),
            parent_hash,
            parent_number: *parent_header.number(),
            oracle: self.oracle.clone(),
            transaction_grouper: TransactionGrouper::new(self.transaction_pool.clone()),
            block_executor,
            telemetry: self.telemetry.clone(),
            _phantom: PhantomData,
            include_proof_in_block_size_estimation: self.include_proof_in_block_size_estimation,
        };

        proposer
    }
}

impl<A, B, Block, C, PR, O, MBH, E> sp_consensus::Environment<Block> for ProposerFactory<A, B, C, PR, O, MBH, E>
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
    PR: ProofRecording + Send + Sync + 'static,
    O: BlockOracle<Block> + Send + Sync + 'static,
    MBH: MultiThreadBlockBuilder<B, Block, C::Api> + Send + Sync + 'static,
    E: ExtendExtrinsic + Send + Sync + 'static,
{
    type Proposer = Proposer<B, Block, C, A, PR, O, MBH, E>;
    type CreateProposer = future::Ready<Result<Self::Proposer, Self::Error>>;
    type Error = sp_blockchain::Error;

    fn init(&mut self, parent_header: &<Block as BlockT>::Header) -> Self::CreateProposer {
        future::ready(Ok(self.init_with_now(parent_header, Box::new(time::Instant::now))))
    }
}

/// The proposer logic.
pub struct Proposer<B, Block: BlockT, C: ProvideRuntimeApi<Block>, A: TransactionPool, PR, O: BlockOracle<Block>, MBH: MultiThreadBlockBuilder<B, Block, C::Api>, E: ExtendExtrinsic> {
    spawn_handle: Box<dyn SpawnNamed>,
    client: Arc<C>,
    parent_hash: Block::Hash,
    parent_number: <<Block as BlockT>::Header as HeaderT>::Number,
    oracle: Arc<O>,
    transaction_grouper: TransactionGrouper<A, Block>,
    block_executor: BlockExecutor<B, C, A, PR, MBH, E>,
    include_proof_in_block_size_estimation: bool,
    telemetry: Option<TelemetryHandle>,
    _phantom: PhantomData<(B, PR, MBH, E)>,
}

impl<A, B, Block, C, PR, O, MBH, E> sp_consensus::Proposer<Block> for Proposer<B, Block, C, A, PR, O, MBH, E>
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
    PR: ProofRecording + Send + Sync + 'static,
    O: BlockOracle<Block> + Send + Sync + 'static,
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

impl<A, B, Block, C, PR, O, MBH, E> Proposer<B, Block, C, A, PR, O, MBH, E>
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
    PR: ProofRecording + Send + Sync + 'static,
    O: BlockOracle<Block> + Send + Sync + 'static,
    MBH: MultiThreadBlockBuilder<B, Block, C::Api> + Send + Sync + 'static,
    E: ExtendExtrinsic + Send + Sync + 'static,
{
    async fn propose_with(
        self,
        inherent_data: InherentData,
        inherent_digests: Digest,
        max_duration: Duration,
        block_size_limit: Option<usize>,
    ) -> Result<Proposal<Block, backend::TransactionFor<B, Block>, PR::Proof>, sp_blockchain::Error> {
        info!(target: LOG_TARGET, "🙌 Starting consensus session on top of parent {}({:?})", self.parent_number, self.parent_hash);
        let propose_with_start = time::Instant::now();
        let deadline = time::Instant::now() + max_duration;
        // 1. set block_duration.
        self.oracle.update_block_duration(max_duration);
        // 2. initialize main thread
        let mut block_builder =
            self.client.new_block_at(self.parent_hash.clone(), inherent_digests.clone(), PR::ENABLED, None)?;

        // 3. get groups transactions
        let pool_time = self.oracle.pool_time();
        let group_output = self.transaction_grouper.group_transactions_from_pool(
            self.parent_number,
            pool_time / 3,
            pool_time,
            self.oracle.thread_limit(),
            self.oracle.linear_tx_limit(),
            self.oracle.min_single_tx(),
            self.oracle.total_tx_limit(),
            block_size_limit.unwrap_or(self.oracle.block_size_limit()),
            block_builder.estimated_header_size + block_builder.extrinsics.encoded_size(),
            if self.include_proof_in_block_size_estimation {
                block_builder.api.proof_recorder().map(|pr| pr.estimate_encoded_size()).unwrap_or(0)
            } else {
                0
            },
            &vec![],
        ).await.map_err(|e| sp_blockchain::Error::Backend(e))?;
        debug!(target: LOG_TARGET, "[Execute Block {}] {}", self.parent_number + One::one(), group_output.info.info());
        let thread_number = group_output.groups.len();

        // 4. create inehrent
        let inherents = block_builder.create_inherents(inherent_data)?;
        // 5. execute block
        // TODO dynamic block time for oracle.linear_execute_time() and oracle.block_size_limit().
        let (proposal, mut info) = self.block_executor.execute_block(
            self.parent_hash,
            self.parent_number,
            propose_with_start,
            deadline,
            inherents,
            inherent_digests,
            block_builder,
            self.oracle.linear_execute_time(),
            self.oracle.merge_time(),
            self.oracle.round_tx(),
            group_output.groups
                .into_iter()
                .map(|g| g.iter().map(|(i, tx)| (*i, Some(tx.hash().clone()), tx.data().clone())).collect())
                .collect(),
            group_output.single.iter().map(|(i, tx)| (*i, Some(tx.hash().clone()), tx.data().clone())).collect(),
            true,
            false,
            true,
        )
            .await?;
        info.set_group_info(group_output.info);
        self.oracle.update_execute_info(&info);
        info!(
            target: LOG_TARGET,
			"🎁 [Propose] Prepared block {} [{}] \
			[hash: {:?}; parent_hash: {}; {}, threads {thread_number}]",
			info.number,
			info.time_info(true),
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
        Ok(proposal)
    }
}

#[async_trait::async_trait]
impl<A, B, Block, C, PR, O, MBH, E> GroupTransaction<Block> for Proposer<B, Block, C, A, PR, O, MBH, E>
where
    A: TransactionPool<Block=Block, Hash=Block::Hash>,
    <A as TransactionPool>::InPoolTransaction: Send + Sync + 'static,
    Block: BlockT,
    B: backend::Backend<Block> + Send + Sync + 'static,
    C: ProvideRuntimeApi<Block> + Send + Sync + 'static,
    MBH: MultiThreadBlockBuilder<B, Block, C::Api> + Send + Sync + 'static,
    E: ExtendExtrinsic + Send + Sync + 'static,
    PR: ProofRecording + Send + Sync + 'static,
    O: BlockOracle<Block> + Send + Sync + 'static,
{
    async fn extrinsic(
        &self,
        parent: <Block::Header as HeaderT>::Number,
        wait_time: Duration,
        pool_time: Duration,
        init_block_size: Option<usize>,
        init_proof_size: Option<usize>,
        excepts: Vec<&Block::Extrinsic>,
    ) -> Result<(Vec<Vec<Block::Extrinsic>>, Vec<Block::Extrinsic>), String> {
        self.transaction_grouper.group_transactions_from_pool(
            parent,
            wait_time,
            pool_time,
            self.oracle.thread_limit(),
            self.oracle.linear_tx_limit(),
            self.oracle.min_single_tx(),
            self.oracle.total_tx_limit(),
            self.oracle.block_size_limit(),
            init_block_size.unwrap_or_default(),
            init_proof_size.unwrap_or_default(),
            &excepts,
        )
            .await
            .map(|r| (
                r.groups.into_iter().map(|g| g.into_iter().map(|(_, e)| e.data().clone()).collect()).collect(),
                r.single.into_iter().map(|(_, e)| e.data().clone()).collect(),
            ))
    }
}

#[async_trait::async_trait]
impl<A, B, Block, C, PR, O, MBH, E> BlockPropose<Block> for Proposer<B, Block, C, A, PR, O, MBH, E>
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
    PR: ProofRecording + Send + Sync + 'static,
    O: BlockOracle<Block> + Send + Sync + 'static,
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
        allow_extend: bool,
        merge_in_thread_order: bool,
        limit_execution_time: bool,
    ) -> Result<(Proposal<Block, Self::Transaction, Self::Proof>, BlockExecuteInfo<Block>), Self::Error> {
        self.block_executor.propose_block(
            source,
            parent_hash,
            parent_number,
            max_duration,
            linear_execute_time,
            estimated_merge_time,
            inherent_data,
            inherent_digests,
            extrinsic,
            round_tx,
            allow_extend,
            merge_in_thread_order,
            limit_execution_time,
        ).await
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
        allow_extend: bool,
        merge_in_thread_order: bool,
        limit_execution_time: bool
    ) -> Result<(Proposal<Block, Self::Transaction, Self::Proof>, BlockExecuteInfo<Block>), Self::Error> {
        self.block_executor.execute_block_for_import(
            source,
            context,
            parent_hash,
            parent_number,
            inherent_digests,
            extrinsic,
            groups,
            round_tx,
            linear_execute_time,
            estimated_merge_time,
            allow_extend,
            merge_in_thread_order,
            limit_execution_time,
        ).await
    }
}
