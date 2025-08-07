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
use sc_block_builder::{BlockBuilderApi, BlockBuilderProvider, MultiThreadBlockBuilder};
use sc_client_api::{backend, CloneForExecution};
use sc_telemetry::{telemetry, TelemetryHandle, CONSENSUS_INFO};
use sc_transaction_pool_api::{InPoolTransaction, TransactionPool};
use sp_api::{ApiExt, ProvideRuntimeApi};
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
use std::ops::DerefMut;
use prometheus_endpoint::Registry as PrometheusRegistry;
use sc_proposer_metrics::{EndProposingReason, MetricsLink as PrometheusMetrics};
use sp_runtime::traits::One;
use crate::DEFAULT_BLOCK_SIZE_LIMIT;

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
    RCG: RCGroup + Send + 'static,
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
    RCG: RCGroup + Send + 'static,
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
    RCG: RCGroup + Send + 'static,
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
                // leave some time for evaluation and block finalization (33%)
                let deadline = (self.now)() + max_duration - max_duration / 3;
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
    RCG: RCGroup + Send + 'static,
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
        let propose_with_start = time::Instant::now();
        let mut block_builder =
            self.client.new_block_at(self.parent_hash.clone(), inherent_digests.clone(), PR::ENABLED)?;
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
        let mut group_id = 0usize;
        let mut group: HashMap<usize, Vec<Arc<<A as TransactionPool>::InPoolTransaction>>> = HashMap::new();
        let mut tx_data_group : HashMap<Vec<u8>, usize> = HashMap::new();
        let mut t1 = self.transaction_pool.ready_at(self.parent_number).fuse();
        let mut t2 =
            futures_timer::Delay::new(deadline.saturating_duration_since((self.now)()) / 8).fuse();
        let mut pending_iterator = select! {
			res = t1 => res,
			_ = t2 => {
				warn!(
					"Timeout fired waiting for transaction pool at block #{}. Proceeding with production.",
					self.parent_number,
				);
				self.transaction_pool.ready()
			},
		};
        let mut skipped = 0;
        let mut extrinsic_count = 0usize;
        let block_size_limit = block_size_limit.unwrap_or(self.default_block_size_limit);
        let mut block_size = block_builder.estimated_header_size + block_builder.extrinsics.encoded_size();
        let mut proof_size = if self.include_proof_in_block_size_estimation {
            block_builder.api.proof_recorder().map(|pr| pr.estimate_encoded_size()).unwrap_or(0)
        } else {
            0
        };
        loop {
            let pending_tx = if let Some(pending_tx) = pending_iterator.next() {
                pending_tx
            } else {
                debug!("out of transactions from pool(total {extrinsic_count})");
                break;
            };

            let pending_tx_data = pending_tx.data().clone();
            if block_size + pending_tx_data.encoded_size() + proof_size > block_size_limit - extrinsic_count * 200 {
                if skipped < MAX_SKIPPED_TRANSACTIONS {
                    skipped += 1;
                    debug!(
						"Transaction would overflow the block size limit, \
						but will try {} more transactions before quitting.",
						MAX_SKIPPED_TRANSACTIONS - skipped,
					);
                    continue
                } else {
                    debug!("Reached block size limit, start execute transactions.");
                    break;
                }
            }
            let call_group_data = RCG::call_dependent_data(pending_tx_data.encode()).unwrap();
            let mut gid = group_id;
            for data in &call_group_data {
                // if dependent data is already dispatched to group id, this call should be in this group.
                if let Some(id) = tx_data_group.get(data) {
                    gid = *id;
                    break;
                }
            };
            // insert tx to gid.
            if let Some(gp) = group.get_mut(&gid) {
                gp.push(pending_tx);
            } else {
                group.insert(gid, vec![pending_tx]);
            }
            // record call_group_data to gid
            for data in call_group_data {
                tx_data_group.insert(data, gid);
            };
            // if new group_id used, group_id += 1
            if gid == group_id {
                group_id += 1;
            }

            block_size += pending_tx_data.encoded_size();
            extrinsic_count += 1;
            // TODO update proof_size with new extrinsic. This is hard since we do not actually execute the extrinsic.
            proof_size += 0;
        }

        let mbh = MBH::default();
        mbh.pre_handle(&block_builder.backend, &block_builder.parent_hash);
        let mut extrinsic_group: Vec<Vec<Arc<<A as TransactionPool>::InPoolTransaction>>> = group.into_values().collect();
        extrinsic_group.sort_by(|a, b| a.len().cmp(&b.len()));
        let (tx, mut rx) = mpsc::channel(extrinsic_group.len());
        for i in 0..extrinsic_group.len() {
            let parent_hash = self.parent_hash.clone();
            let spawn_handle = self.spawn_handle.clone();
            let pending_txs = extrinsic_group[i].clone();
            let now = time::Instant::now();
            let left = deadline.saturating_duration_since(now);
            let left_micros: u64 = left.as_micros().saturated_into();
            let soft_deadline =
                now + time::Duration::from_micros(self.soft_deadline_percent.mul_floor(left_micros));
            let inherent_digests_clone = inherent_digests.clone();
            let client_clone = self.client.clone_for_execution();
            let mut res_tx = tx.clone();
            spawn_handle.spawn_blocking(
                "basic-authorship-proposer-thread",
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
                    let mut unqueue_invalid = Vec::new();
                    let mut applied_extrinsics = Vec::new();
                    let mut pending_iterator = pending_txs.into_iter();
                    let end_reason = loop {
                        let pending_tx = if let Some(pending_tx) = pending_iterator.next() {
                            pending_tx
                        } else {
                            break EndProposingReason::NoMoreTransactions
                        };

                        let now = time::Instant::now();
                        if now > deadline {
                            debug!(
								"[Thread {i}] Consensus deadline reached when pushing block transactions, \
								proceeding with proposing."
							);
                            break EndProposingReason::HitDeadline
                        }

                        let pending_tx_data = pending_tx.data().clone();
                        let pending_tx_hash = pending_tx.hash().clone();

                        trace!("[{:?}] Pushing to the block.", pending_tx_hash);
                        match sc_block_builder::BlockBuilder::push(&mut block_builder, pending_tx_data.clone()) {
                            Ok(()) => {
                                applied_extrinsics.push(pending_tx_data);
                                debug!("[{:?}] Pushed to the block.", pending_tx_hash);
                            },
                            Err(ApplyExtrinsicFailed(Validity(e))) if e.exhausted_resources() => {
                                if skipped < MAX_SKIPPED_TRANSACTIONS {
                                    skipped += 1;
                                    debug!(
									"[Thread {i}] Block seems full, but will try {} more transactions before quitting.",
									MAX_SKIPPED_TRANSACTIONS - skipped,
								);
                                } else if time::Instant::now() < soft_deadline {
                                    debug!(
										"[Thread {i}] Block seems full, but we still have time before the soft deadline, \
										 so we will try a bit more before quitting."
									);
                                } else {
                                    debug!("[Thread {i}] Reached block weight limit, proceeding with proposing.");
                                    break EndProposingReason::HitBlockWeightLimit
                                }
                            },
                            Err(e) => {
                                debug!("[{:?}] Invalid transaction: {}", pending_tx_hash, e);
                                unqueue_invalid.push(pending_tx_hash);
                            },
                        }
                    };

                    let results = block_builder.api.take_all_changes();
                    if res_tx.start_send(Ok((i, results, applied_extrinsics, unqueue_invalid, end_reason))).is_err() {
                        trace!("Could not send block production result to proposer!");
                    }
                }),
            );
        }
        let prepare_tx_time = block_timer.elapsed().as_millis();

        let mut final_end_reason = EndProposingReason::NoMoreTransactions;
        let mut exe_merge_time = 0;

        let mut finish =
            futures_timer::Delay::new(deadline.saturating_duration_since((self.now)())).fuse();
        let mut threads = 0;
        for _ in 0..extrinsic_group.len() {
            select! {
                res = rx.next() => {
                    if let Some(result) = res {
                        let (i, all_changes, applied_extrinsics, unqueue_invalid, end_reason) = match result {
                            Ok(result) => result,
                            Err(e) => return Err(e),
                        };
                        final_end_reason = end_reason;
                        self.transaction_pool.remove_invalid(&unqueue_invalid);
                        let exe_merge_start = time::Instant::now();
                        let (changes, _, recorder) = all_changes;
                        // TODO add ability to rollback failed merge. So that we can drop conflict threads if not group tx correctly. We can dorp this merge failed group txs.
                        if let Err(e) = block_builder.api.deref_mut().merge_all_changes(changes, recorder, &mbh) {
                            return Err(sp_blockchain::Error::Backend(format!("merge thread {i} {e:?}")));
                        }
                        block_builder.extrinsics.extend_from_slice(&applied_extrinsics);
                        exe_merge_time += exe_merge_start.elapsed().as_nanos();
                        threads += 1usize;
                    } else {
                        warn!("BlockBuilder Result Receiver Error!");
                    }
                }
                _ = finish => {
                    warn!("Timeout fired waiting for threads result execution for block #{}. Build block with current state.", self.parent_number + One::one());
                    break;
                }
            }
        }
        exe_merge_time /= 1_000_000;

        let block_size =
            block_builder.estimate_block_size(self.include_proof_in_block_size_estimation);
        if block_size > block_size_limit && block_builder.extrinsics.is_empty() {
            warn!("Hit block size limit of `{block_size_limit}` without including any transaction!");
        }

        let (block, storage_changes, proof) = block_builder.build()?.into_inner();

        self.metrics.report(|metrics| {
            metrics.number_of_transactions.set(block.extrinsics().len() as u64);
            metrics.block_constructed.observe(block_timer.elapsed().as_secs_f64());

            metrics.report_end_proposing_reason(final_end_reason);
        });

        info!(
			"🎁 Prepared block for proposing at {} ({} ms {prepare_tx_time}ms {exe_merge_time}ms) [hash: {:?}; parent_hash: {}; extrinsics ({}), threads {threads}",
			block.header().number(),
			block_timer.elapsed().as_millis(),
			<Block as BlockT>::Hash::from(block.header().hash()),
			block.header().parent_hash(),
			block.extrinsics().len(),
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
}

pub trait RCGroup {
    /// parse runtime call, return dependent data for dispatch call to groups
    fn call_dependent_data(tx_data: Vec<u8>) -> Result<Vec<Vec<u8>>, String>;
}
