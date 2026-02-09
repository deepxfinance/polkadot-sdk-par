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

//! Substrate transaction pool implementation.

#![recursion_limit = "256"]
#![warn(missing_docs)]
#![warn(unused_extern_crates)]

mod api;
mod enactment_state;
pub mod error;
mod graph;
mod metrics;
mod revalidation;
#[cfg(test)]
mod tests;

pub use crate::api::FullChainApi;
use async_trait::async_trait;
use enactment_state::{EnactmentAction, EnactmentState};
use futures::{
	channel::oneshot,
	future::{self, ready},
	prelude::*,
};
pub use graph::{
	base_pool::Limit as PoolLimit, ChainApi, Options, Pool, Transaction, ValidatedTransaction,
};
use parking_lot::Mutex;
use std::{
	collections::{HashMap, HashSet},
	pin::Pin,
	sync::Arc,
};

use graph::{ExtrinsicHash, IsValidator};
use sc_transaction_pool_api::{
	error::Error as TxPoolError, ChainEvent, ImportNotificationStream, MaintainedTransactionPool,
	PoolFuture, PoolStatus, ReadyTransactions, TransactionFor, TransactionPool, TransactionSource,
	TransactionStatusStreamFor, TxHash,
};
use sp_core::traits::SpawnEssentialNamed;
use sp_runtime::{generic::BlockId, traits::{AtLeast32Bit, Block as BlockT, Extrinsic, Header as HeaderT, NumberFor, Zero}, Saturating};
use std::time::Instant;

use crate::metrics::MetricsLink as PrometheusMetrics;
use prometheus_endpoint::Registry as PrometheusRegistry;

use sp_blockchain::{HashAndNumber, TreeRoute};

pub(crate) const LOG_TARGET: &str = "txpool";

type BoxedReadyIterator<Hash, Data> =
	Box<dyn ReadyTransactions<Item = Arc<graph::base_pool::Transaction<Hash, Data>>> + Send>;

type ReadyIteratorFor<PoolApi> =
	BoxedReadyIterator<graph::ExtrinsicHash<PoolApi>, graph::ExtrinsicFor<PoolApi>>;

type PolledIterator<PoolApi> = Pin<Box<dyn Future<Output = ReadyIteratorFor<PoolApi>> + Send>>;

/// A transaction pool for a full node.
pub type FullPool<Block, Client> = BasicPool<FullChainApi<Client, Block>, Block>;

/// Basic implementation of transaction pool that can be customized by providing PoolApi.
pub struct BasicPool<PoolApi, Block>
where
	Block: BlockT,
	PoolApi: graph::ChainApi<Block = Block>,
{
	pool: Arc<graph::Pool<PoolApi>>,
	api: Arc<PoolApi>,
	revalidation_strategy: Arc<Mutex<RevalidationStrategy<NumberFor<Block>>>>,
	revalidation_queue: Arc<revalidation::RevalidationQueue<PoolApi>>,
	ready_poll: Arc<Mutex<ReadyPoll<ReadyIteratorFor<PoolApi>, Block>>>,
	metrics: PrometheusMetrics,
	enactment_state: Arc<Mutex<EnactmentState<Block>>>,
}

struct ReadyPoll<T, Block: BlockT> {
	updated_at: NumberFor<Block>,
	pollers: Vec<(NumberFor<Block>, oneshot::Sender<T>, Option<usize>)>,
}

impl<T, Block: BlockT> Default for ReadyPoll<T, Block> {
	fn default() -> Self {
		Self { updated_at: NumberFor::<Block>::zero(), pollers: Default::default() }
	}
}

impl<T, Block: BlockT> ReadyPoll<T, Block> {
	fn new(best_block_number: NumberFor<Block>) -> Self {
		Self { updated_at: best_block_number, pollers: Default::default() }
	}

	fn trigger(&mut self, number: NumberFor<Block>, iterator_factory: impl Fn(Option<(NumberFor<Block>, NumberFor<Block>)>, Option<usize>) -> T) {
		self.updated_at = self.updated_at.max(number);

		let mut idx = 0;
		while idx < self.pollers.len() {
			if self.pollers[idx].0 <= self.updated_at {
				let poller_sender = self.pollers.swap_remove(idx);
				log::debug!(target: LOG_TARGET, "Sending ready signal at block {}", self.updated_at);
				let _ = poller_sender.1.send(iterator_factory(Some((self.updated_at, poller_sender.0)), poller_sender.2));
			} else {
				idx += 1;
			}
		}
	}

	fn add(&mut self, number: NumberFor<Block>, limit: Option<usize>) -> oneshot::Receiver<T> {
		let (sender, receiver) = oneshot::channel();
		self.pollers.push((number, sender, limit));
		receiver
	}

	fn updated_at(&self) -> NumberFor<Block> {
		self.updated_at
	}
}

/// Type of revalidation.
pub enum RevalidationType {
	/// Light revalidation type.
	///
	/// During maintenance, transaction pool makes periodic revalidation
	/// of all transactions depending on number of blocks or time passed.
	/// Also this kind of revalidation does not resubmit transactions from
	/// retracted blocks, since it is too expensive.
	Light,

	/// Full revalidation type.
	///
	/// During maintenance, transaction pool revalidates some fixed amount of
	/// transactions from the pool of valid transactions.
	Full,

	/// Never revalidate type.
	///
	/// This is used which ensure all transaction are validate.
	Never,
}

impl<PoolApi, Block> BasicPool<PoolApi, Block>
where
	Block: BlockT,
	PoolApi: graph::ChainApi<Block = Block> + 'static,
{
	/// Create new basic transaction pool with provided api, for tests.
	pub fn new_test(
		pool_api: Arc<PoolApi>,
		best_block_hash: Block::Hash,
		finalized_hash: Block::Hash,
	) -> (Self, Pin<Box<dyn Future<Output = ()> + Send>>) {
		let pool = Arc::new(graph::Pool::new(Default::default(), true.into(), pool_api.clone()));
		let (revalidation_queue, background_task) =
			revalidation::RevalidationQueue::new_background(pool_api.clone(), pool.clone());
		(
			Self {
				api: pool_api,
				pool,
				revalidation_queue: Arc::new(revalidation_queue),
				revalidation_strategy: Arc::new(Mutex::new(RevalidationStrategy::Always)),
				ready_poll: Default::default(),
				metrics: Default::default(),
				enactment_state: Arc::new(Mutex::new(EnactmentState::new(
					best_block_hash,
					finalized_hash,
				))),
			},
			background_task,
		)
	}

	/// Create new basic transaction pool with provided api and custom
	/// revalidation type.
	pub fn with_revalidation_type(
		options: graph::Options,
		is_validator: IsValidator,
		pool_api: Arc<PoolApi>,
		prometheus: Option<&PrometheusRegistry>,
		revalidation_type: RevalidationType,
		spawner: impl SpawnEssentialNamed,
		best_block_number: NumberFor<Block>,
		best_block_hash: Block::Hash,
		finalized_hash: Block::Hash,
	) -> Self {
		let pool = Arc::new(graph::Pool::new(options, is_validator, pool_api.clone()));
		let (revalidation_queue, background_task) = match revalidation_type {
			RevalidationType::Light =>
				(revalidation::RevalidationQueue::new(pool_api.clone(), pool.clone()), None),
			RevalidationType::Full => {
				let (queue, background) =
					revalidation::RevalidationQueue::new_background(pool_api.clone(), pool.clone());
				(queue, Some(background))
			},
			RevalidationType::Never =>
				(revalidation::RevalidationQueue::new(pool_api.clone(), pool.clone()), None),
		};

		if let Some(background_task) = background_task {
			spawner.spawn_essential("txpool-background", Some("transaction-pool"), background_task);
		}

		Self {
			api: pool_api,
			pool,
			revalidation_queue: Arc::new(revalidation_queue),
			revalidation_strategy: Arc::new(Mutex::new(match revalidation_type {
				RevalidationType::Light =>
					RevalidationStrategy::Light(RevalidationStatus::NotScheduled),
				RevalidationType::Full => RevalidationStrategy::Always,
				RevalidationType::Never => RevalidationStrategy::Never,
			})),
			ready_poll: Arc::new(Mutex::new(ReadyPoll::new(best_block_number))),
			metrics: PrometheusMetrics::new(prometheus),
			enactment_state: Arc::new(Mutex::new(EnactmentState::new(
				best_block_hash,
				finalized_hash,
			))),
		}
	}

	/// Gets shared reference to the underlying pool.
	pub fn pool(&self) -> &Arc<graph::Pool<PoolApi>> {
		&self.pool
	}

	/// Get access to the underlying api
	pub fn api(&self) -> &PoolApi {
		&self.api
	}
}

impl<PoolApi, Block> TransactionPool for BasicPool<PoolApi, Block>
where
	Block: BlockT,
	PoolApi: 'static + graph::ChainApi<Block = Block>,
{
	type Block = PoolApi::Block;
	type Hash = graph::ExtrinsicHash<PoolApi>;
	type InPoolTransaction = graph::base_pool::Transaction<TxHash<Self>, TransactionFor<Self>>;
	type Error = PoolApi::Error;

	fn submit_at(
		&self,
		at: &BlockId<Self::Block>,
		source: TransactionSource,
		xts: Vec<TransactionFor<Self>>,
		multi: bool,
	) -> PoolFuture<Vec<Result<TxHash<Self>, Self::Error>>, Self::Error> {
		let pool = self.pool.clone();
		let at = *at;

		self.metrics
			.report(|metrics| metrics.submitted_transactions.inc_by(xts.len() as u64));

		async move { pool.submit_at(&at, source, xts, multi).await }.boxed()
	}

	fn submit_one(
		&self,
		at: &BlockId<Self::Block>,
		source: TransactionSource,
		xt: TransactionFor<Self>,
	) -> PoolFuture<TxHash<Self>, Self::Error> {
		let pool = self.pool.clone();
		let at = *at;

		self.metrics.report(|metrics| metrics.submitted_transactions.inc());

		async move { pool.submit_one(&at, source, xt).await }.boxed()
	}

	fn submit_multi(
		&self,
		at: &BlockId<Self::Block>,
		source: TransactionSource,
		xts: Vec<TransactionFor<Self>>,
	) -> PoolFuture<Vec<Result<TxHash<Self>, Self::Error>>, Self::Error> {
		let pool = self.pool.clone();
		let at = *at;

		self.metrics.report(|metrics| metrics.submitted_transactions.inc());

		async move { pool.submit_multi(&at, source, xts).await }.boxed()
	}

	fn submit_and_watch(
		&self,
		at: &BlockId<Self::Block>,
		source: TransactionSource,
		xt: TransactionFor<Self>,
	) -> PoolFuture<Pin<Box<TransactionStatusStreamFor<Self>>>, Self::Error> {
		let at = *at;
		let pool = self.pool.clone();

		self.metrics.report(|metrics| metrics.submitted_transactions.inc());

		async move {
			let watcher = pool.submit_and_watch(&at, source, xt).await?;

			Ok(watcher.into_stream().boxed())
		}
		.boxed()
	}

	fn remove_invalid(&self, hashes: &[TxHash<Self>]) -> Vec<Arc<Self::InPoolTransaction>> {
		let removed = self.pool.validated_pool().remove_invalid(hashes);
		self.metrics
			.report(|metrics| metrics.validations_invalid.inc_by(removed.len() as u64));
		removed
	}

	fn status(&self) -> PoolStatus {
		self.pool.validated_pool().status()
	}

	fn import_notification_stream(&self) -> ImportNotificationStream<Vec<TxHash<Self>>> {
		self.pool.validated_pool().import_notification_stream()
	}

	fn hash_of(&self, xt: &TransactionFor<Self>) -> TxHash<Self> {
		self.pool.hash_of(xt)
	}

	fn on_broadcasted(&self, propagations: HashMap<TxHash<Self>, Vec<String>>) {
		self.pool.validated_pool().on_broadcasted(propagations)
	}

	fn ready_transaction(&self, hash: &TxHash<Self>) -> Option<Arc<Self::InPoolTransaction>> {
		self.pool.validated_pool().ready_by_hash(hash)
	}

	fn ready_at(&self, at: NumberFor<Self::Block>, limit: Option<usize>) -> PolledIterator<PoolApi> {
		let status = self.status();
		// If there are no transactions in the pool, it is fine to return early.
		//
		// There could be transaction being added because of some re-org happening at the relevant
		// block, but this is relative unlikely.
		if status.ready == 0 && status.future == 0 {
			return async { Box::new(std::iter::empty()) as Box<_> }.boxed()
		}

		let updated_at = self.ready_poll.lock().updated_at();
		let iterator: ReadyIteratorFor<PoolApi> = Box::new(self.pool.validated_pool().ready(Some((updated_at, at)), limit));
		async move { iterator }.boxed()
		// if updated_at >= at {
		// 	log::trace!(target: LOG_TARGET, "Transaction pool already processed block  #{}", at);
		// 	let iterator: ReadyIteratorFor<PoolApi> = Box::new(self.pool.validated_pool().ready(Some(at), limit));
		// 	return async move { iterator }.boxed()
		// }
		// 
		// let pending = self.ready_poll.lock().add(at, limit);
		// pending.map(|received| {
		// 	received.unwrap_or_else(|e| {
		// 		log::warn!("Error receiving pending set: {:?}", e);
		// 		Box::new(std::iter::empty())
		// 	})
		// })
		// 	.boxed()
	}

	fn ready(&self, limit: Option<usize>) -> ReadyIteratorFor<PoolApi> {
		Box::new(self.pool.validated_pool().ready(None, limit))
	}
}

impl<Block, Client> FullPool<Block, Client>
where
	Block: BlockT,
	Client: sp_api::ProvideRuntimeApi<Block>
		+ sc_client_api::BlockBackend<Block>
		+ sc_client_api::blockchain::HeaderBackend<Block>
		+ sp_runtime::traits::BlockIdTo<Block>
		+ sc_client_api::ExecutorProvider<Block>
		+ sc_client_api::UsageProvider<Block>
		+ sp_blockchain::HeaderMetadata<Block, Error = sp_blockchain::Error>
		+ Send
		+ Sync
		+ 'static,
	Client::Api: sp_transaction_pool::runtime_api::TaggedTransactionQueue<Block>,
{
	/// Create new basic transaction pool for a full node with the provided api.
	pub fn new_full(
		options: graph::Options,
		is_validator: IsValidator,
		prometheus: Option<&PrometheusRegistry>,
		spawner: impl SpawnEssentialNamed,
		client: Arc<Client>,
	) -> Arc<Self> {
		let revalidation_type = match std::env::var("REVALIDATION_TYPE").unwrap_or("Full".to_string()).as_str() {
			"Full" => RevalidationType::Full,
			"Light" => RevalidationType::Light,
			"Never" => RevalidationType::Never,
			_ => unimplemented!("Unsupported revalidation type(`Full`, `Light`, `Never`)"),
		};
		let pool_api = Arc::new(FullChainApi::new(client.clone(), prometheus, &spawner));
		let pool = Arc::new(Self::with_revalidation_type(
			options,
			is_validator,
			pool_api,
			prometheus,
			revalidation_type,
			spawner,
			client.usage_info().chain.best_number,
			client.usage_info().chain.best_hash,
			client.usage_info().chain.finalized_hash,
		));

		// make transaction pool available for off-chain runtime calls.
		client.execution_extensions().register_transaction_pool(&pool);

		pool
	}
}

impl<Block, Client> sc_transaction_pool_api::LocalTransactionPool
	for BasicPool<FullChainApi<Client, Block>, Block>
where
	Block: BlockT,
	Client: sp_api::ProvideRuntimeApi<Block>
		+ sc_client_api::BlockBackend<Block>
		+ sc_client_api::blockchain::HeaderBackend<Block>
		+ sp_runtime::traits::BlockIdTo<Block>
		+ sp_blockchain::HeaderMetadata<Block, Error = sp_blockchain::Error>,
	Client: Send + Sync + 'static,
	Client::Api: sp_transaction_pool::runtime_api::TaggedTransactionQueue<Block>,
{
	type Block = Block;
	type Hash = graph::ExtrinsicHash<FullChainApi<Client, Block>>;
	type Error = <FullChainApi<Client, Block> as graph::ChainApi>::Error;

	fn submit_local(
		&self,
		at: &BlockId<Self::Block>,
		xt: sc_transaction_pool_api::LocalTransactionFor<Self>,
	) -> Result<Self::Hash, Self::Error> {
		use sp_runtime::{
			traits::SaturatedConversion, transaction_validity::TransactionValidityError,
		};

		let validity = self
			.api
			.validate_transaction_blocking(at, TransactionSource::Local, xt.clone())?
			.map_err(|e| {
				Self::Error::Pool(match e {
					TransactionValidityError::Invalid(i) => TxPoolError::InvalidTransaction(i),
					TransactionValidityError::Unknown(u) => TxPoolError::UnknownTransaction(u),
				})
			})?;

		let (hash, bytes) = self.pool.validated_pool().api().hash_and_length(&xt);
		let block_number = self
			.api
			.block_id_to_number(at)?
			.ok_or_else(|| error::Error::BlockIdConversion(format!("{:?}", at)))?;

		let validated = ValidatedTransaction::valid_at(
			block_number.saturated_into::<u64>(),
			hash,
			TransactionSource::Local,
			xt,
			bytes,
			validity,
		)?;

		self.pool.validated_pool().submit(vec![validated]).remove(0)
	}
}

#[cfg_attr(test, derive(Debug))]
enum RevalidationStatus<N> {
	/// The revalidation has never been completed.
	NotScheduled,
	/// The revalidation is scheduled.
	Scheduled(Option<Instant>, Option<N>),
	/// The revalidation is in progress.
	InProgress,
}

enum RevalidationStrategy<N> {
	Always,
	Light(RevalidationStatus<N>),
	Never,
}

struct RevalidationAction {
	revalidate: bool,
	resubmit: bool,
}

impl<N: Clone + Copy + AtLeast32Bit> RevalidationStrategy<N> {
	pub fn clear(&mut self) {
		if let Self::Light(status) = self {
			status.clear()
		}
	}

	pub fn next(
		&mut self,
		block: N,
		revalidate_time_period: Option<std::time::Duration>,
		revalidate_block_period: Option<N>,
	) -> RevalidationAction {
		match self {
			Self::Light(status) => RevalidationAction {
				revalidate: status.next_required(
					block,
					revalidate_time_period,
					revalidate_block_period,
				),
				resubmit: false,
			},
			Self::Always => RevalidationAction { revalidate: true, resubmit: true },
			Self::Never => RevalidationAction { revalidate: false, resubmit: false },
		}
	}
}

impl<N: Clone + Copy + AtLeast32Bit> RevalidationStatus<N> {
	/// Called when revalidation is completed.
	pub fn clear(&mut self) {
		*self = Self::NotScheduled;
	}

	/// Returns true if revalidation is required.
	pub fn next_required(
		&mut self,
		block: N,
		revalidate_time_period: Option<std::time::Duration>,
		revalidate_block_period: Option<N>,
	) -> bool {
		match *self {
			Self::NotScheduled => {
				*self = Self::Scheduled(
					revalidate_time_period.map(|period| Instant::now() + period),
					revalidate_block_period.map(|period| block + period),
				);
				false
			},
			Self::Scheduled(revalidate_at_time, revalidate_at_block) => {
				let is_required =
					revalidate_at_time.map(|at| Instant::now() >= at).unwrap_or(false) ||
						revalidate_at_block.map(|at| block >= at).unwrap_or(false);
				if is_required {
					*self = Self::InProgress;
				}
				is_required
			},
			Self::InProgress => false,
		}
	}
}

/// Prune the known txs for the given block.
async fn prune_known_txs_for_block<Block: BlockT, Api: graph::ChainApi<Block = Block>>(
	block_hash: Block::Hash,
	api: &Api,
	pool: &graph::Pool<Api>,
) -> Vec<ExtrinsicHash<Api>> {
	let start = std::time::Instant::now();
	let extrinsics = api
		.block_body(block_hash)
		.await
		.unwrap_or_else(|e| {
			log::warn!("Prune known transactions: error request: {}", e);
			None
		})
		.unwrap_or_default();
	let body_time = start.elapsed();

	let header_start = std::time::Instant::now();
	let header = match api.block_header(block_hash) {
		Ok(Some(h)) => h,
		Ok(None) => {
			log::debug!(target: LOG_TARGET, "Could not find header for {:?}.", block_hash);
			return extrinsics.iter().map(|tx| pool.hash_of(tx)).collect::<Vec<_>>();
		},
		Err(e) => {
			log::debug!(target: LOG_TARGET, "Error retrieving header for {:?}: {}", block_hash, e);
			return extrinsics.iter().map(|tx| pool.hash_of(tx)).collect::<Vec<_>>();
		},
	};
	let header_time = header_start.elapsed();
	let (tx_root, hash_set) = pool.validated_pool().clear_consensus_hashes(&BlockId::Number(*header.number())).unwrap_or_default();
	let hash_start = std::time::Instant::now();
	let (hashes, hash_time) = if header.extrinsics_root() == &tx_root {
		(
			if extrinsics.len() > 0 {
				[vec![pool.hash_of(&extrinsics[0])], hash_set.into_iter().collect()].concat()
			} else {
				hash_set.into_iter().collect()
			},
			hash_start.elapsed()
		)
	} else {
		(extrinsics.iter().map(|tx| pool.hash_of(tx)).collect::<Vec<_>>(), hash_start.elapsed())
	};

	log::trace!(target: LOG_TARGET, "Pruning transactions: {:?}", hashes);
	let prune_start = std::time::Instant::now();
	let prune_times = pool.prune(&BlockId::Hash(block_hash), &BlockId::hash(*header.parent_hash()), &extrinsics, hashes.clone())
		.await.unwrap_or_else(|e| {
		log::error!("Cannot prune known in the pool: {}", e);
		Default::default()
	});
	let prune_time = prune_start.elapsed();
	if hashes.len() > 1 {
		log::info!(
			target: LOG_TARGET,
			"prune_known_txs_for_block {}: {} txs in {:?}(body {body_time:?} header {header_time:?} hash {hash_time:?} prune {prune_time:?}({prune_times}))",
			header.number(),
			hashes.len(),
			start.elapsed(),
		);
	}
	hashes
}

/// Prune the known txs for the given block.
async fn prune_known_consensus_txs_for_block<Block: BlockT, Api: graph::ChainApi<Block = Block>>(
	block: NumberFor<Block>,
	pool: &graph::Pool<Api>,
) -> Vec<ExtrinsicHash<Api>> {
	let start = std::time::Instant::now();
	let (_tx_root, hash_set) = pool.validated_pool()
		.clear_consensus_hashes(&BlockId::Number(block)).unwrap_or_default();
	if hash_set.is_empty() { return Vec::new(); }
	let hashes = hash_set.into_iter().collect::<Vec<_>>();
	log::trace!(target: LOG_TARGET, "Pruning transactions: {:?}", hashes);
	let prune_start = std::time::Instant::now();
	let prune_times = pool.prune_by_hashes(&BlockId::Number(block), hashes.clone())
		.await.unwrap_or_else(|e| {
		log::error!("Cannot prune known in the pool: {}", e);
		Default::default()
	});
	let prune_time = prune_start.elapsed();
	if hashes.len() > 0 {
		log::info!(
			target: LOG_TARGET,
			"prune_known_consensus_txs_for_block {block}: {} txs in {:?}(prune {prune_time:?}({prune_times}))",
			hashes.len(),
			start.elapsed(),
		);
	}
	hashes
}

impl<PoolApi, Block> BasicPool<PoolApi, Block>
where
	Block: BlockT,
	PoolApi: 'static + graph::ChainApi<Block = Block>,
{
	/// Handles enactment and retraction of blocks, prunes stale transactions
	/// (that have already been enacted) and resubmits transactions that were
	/// retracted.
	async fn handle_enactment(&self, tree_route: TreeRoute<Block>) {
		log::trace!(target: LOG_TARGET, "handle_enactment tree_route: {tree_route:?}");
		let pool = self.pool.clone();
		let api = self.api.clone();

		let (hash, block_number) = match tree_route.last() {
			Some(HashAndNumber { hash, number }) => (hash, number),
			None => {
				log::warn!(
					target: LOG_TARGET,
					"Skipping ChainEvent - no last block in tree route {:?}",
					tree_route,
				);
				return
			},
		};

		let mut next_action = self.revalidation_strategy.lock().next(
			*block_number,
			Some(std::time::Duration::from_secs(60)),
			Some(20u32.into()),
		);

		// We keep track of everything we prune so that later we won't add
		// transactions with those hashes from the retracted blocks.
		let mut pruned_log = HashSet::<ExtrinsicHash<PoolApi>>::new();

		// If there is a tree route, we use this to prune known tx based on the enacted
		// blocks. Before pruning enacted transactions, we inform the listeners about
		// retracted blocks and their transactions. This order is important, because
		// if we enact and retract the same transaction at the same time, we want to
		// send first the retract and than the prune event.
		for retracted in tree_route.retracted() {
			// notify txs awaiting finality that it has been retracted
			pool.validated_pool().on_block_retracted(retracted.hash);
		}

		// future::join_all(
		// 	tree_route
		// 		.enacted()
		// 		.iter()
		// 		.map(|h| prune_known_txs_for_block(h.hash, &*api, &*pool)),
		// )
		// .await
		// .into_iter()
		// .for_each(|enacted_log| {
		// 	pruned_log.extend(enacted_log);
		// });
		//
		// self.metrics
		// 	.report(|metrics| metrics.block_transactions_pruned.inc_by(pruned_log.len() as u64));

		next_action.resubmit = false;
		if next_action.resubmit {
			let mut resubmit_transactions = Vec::new();

			for retracted in tree_route.retracted() {
				let hash = retracted.hash;

				let block_transactions = api
					.block_body(hash)
					.await
					.unwrap_or_else(|e| {
						log::warn!("Failed to fetch block body: {}", e);
						None
					})
					.unwrap_or_default()
					.into_iter()
					.filter(|tx| tx.is_signed().unwrap_or(true));

				let mut resubmitted_to_report = 0;

				resubmit_transactions.extend(block_transactions.into_iter().filter(|tx| {
					let tx_hash = pool.hash_of(tx);
					let contains = pruned_log.contains(&tx_hash);

					// need to count all transactions, not just filtered, here
					resubmitted_to_report += 1;

					if !contains {
						log::debug!(
							target: LOG_TARGET,
							"[{:?}]: Resubmitting from retracted block {:?}",
							tx_hash,
							hash,
						);
					}
					!contains
				}));

				self.metrics.report(|metrics| {
					metrics.block_transactions_resubmitted.inc_by(resubmitted_to_report)
				});
			}

			if let Err(e) = pool
				.resubmit_at(
					&BlockId::Hash(*hash),
					// These transactions are coming from retracted blocks, we should
					// simply consider them external.
					TransactionSource::External,
					resubmit_transactions,
				)
				.await
			{
				log::debug!(
					target: LOG_TARGET,
					"[{:?}] Error re-submitting transactions: {}",
					hash,
					e,
				)
			}
		}

		let extra_pool = pool.clone();
		// After #5200 lands, this arguably might be moved to the
		// handler of "all blocks notification".
		self.ready_poll
			.lock()
			.trigger(*block_number, move |at: Option<(NumberFor<Block>, NumberFor<Block>)>, limit| Box::new(extra_pool.validated_pool().ready(at, limit)));

		if next_action.revalidate {
			let hashes = pool.validated_pool().ready(None, Some(10000)).map(|tx| tx.hash).collect();
			self.revalidation_queue.revalidate_later(*block_number, hashes).await;

			self.revalidation_strategy.lock().clear();
		}
	}

	async fn handle_consensus(&self, block: NumberFor<Block>, root: Block::Hash, hashes: Vec<Block::Hash>) {
		log::trace!(target: LOG_TARGET, "handle_consensus block: {block:?}");
		self.pool.validated_pool().on_consensus(block, root, hashes);

		// We keep track of everything we prune so that later we won't add
		// transactions with those hashes from the retracted blocks.
		let pool = self.pool.clone();
		let pruned_hashes = prune_known_consensus_txs_for_block(block.saturating_sub(1u32.into()), &*pool).await;
		self.metrics
			.report(|metrics| metrics.block_transactions_pruned.inc_by(pruned_hashes.len() as u64));
		let extra_pool = self.pool.clone();
		self.ready_poll
			.lock()
			.trigger(
				block.saturating_sub(1u32.into()),
				move |at: Option<(NumberFor<Block>, NumberFor<Block>)>, limit| Box::new(extra_pool.validated_pool().ready(at, limit))
			);
	}
}

#[async_trait]
impl<PoolApi, Block> MaintainedTransactionPool for BasicPool<PoolApi, Block>
where
	Block: BlockT,
	PoolApi: 'static + graph::ChainApi<Block = Block>,
{
	async fn maintain(&self, event: ChainEvent<Self::Block>) {
		let prev_finalized_block = self.enactment_state.lock().recent_finalized_block();
		let compute_tree_route = |from, to| -> Result<TreeRoute<Block>, String> {
			match self.api.tree_route(from, to) {
				Ok(tree_route) => Ok(tree_route),
				Err(e) =>
					return Err(format!(
						"Error occurred while computing tree_route from {from:?} to {to:?}: {e}"
					)),
			}
		};
		let block_id_to_number =
			|hash| self.api.block_id_to_number(&BlockId::Hash(hash)).map_err(|e| format!("{}", e));

		let result =
			self.enactment_state
				.lock()
				.update(&event, &compute_tree_route, &block_id_to_number);

		match result {
			Err(msg) => {
				log::debug!(target: LOG_TARGET, "{msg}");
				self.enactment_state.lock().force_update(&event);
			},
			Ok(EnactmentAction::Skip) => return,
			Ok(EnactmentAction::HandleFinalization) => {},
			Ok(EnactmentAction::HandleEnactment(tree_route)) => {
				self.handle_enactment(tree_route).await;
			},
			Ok(EnactmentAction::HandleConsensus(block, root, hashes)) => {
				self.handle_consensus(block, root, hashes).await;
			}
		};

		if let ChainEvent::Finalized { hash, tree_route } = event {
			log::trace!(
				target: LOG_TARGET,
				"on-finalized enacted: {tree_route:?}, previously finalized: \
				{prev_finalized_block:?}",
			);

			for hash in tree_route.iter().chain(std::iter::once(&hash)) {
				if let Err(e) = self.pool.validated_pool().on_block_finalized(*hash).await {
					log::warn!(
						target: LOG_TARGET,
						"Error occurred while attempting to notify watchers about finalization {}: {}",
						hash, e
					)
				}
			}
		}
	}
}

/// Inform the transaction pool about imported and finalized blocks.
pub async fn notification_future<Client, Pool, Block>(client: Arc<Client>, txpool: Arc<Pool>)
where
	Block: BlockT,
	Client: sc_client_api::BlockchainEvents<Block>,
	Pool: MaintainedTransactionPool<Block = Block>,
{
	let import_stream = client
		.import_notification_stream()
		.filter_map(|n| ready(n.try_into().ok()))
		.fuse();
	let finality_stream = client.finality_notification_stream().map(Into::into).fuse();
	let consensus_stream = client.consensus_notification_stream().map(Into::into).fuse();

	futures::stream::select(
		consensus_stream,
		futures::stream::select(import_stream, finality_stream),
	)
		.for_each(|evt| txpool.maintain(evt))
		.await
}
