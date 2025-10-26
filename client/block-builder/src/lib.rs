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

//! Substrate block builder
//!
//! This crate provides the [`BlockBuilder`] utility and the corresponding runtime api
//! [`BlockBuilder`](sp_block_builder::BlockBuilder).
//!
//! The block builder utility is used in the node as an abstraction over the runtime api to
//! initialize a block, to push extrinsics and to finalize a block.

#![warn(missing_docs)]

use std::sync::Arc;
use codec::Encode;

use sp_api::{
	ApiExt, ApiRef, Core, ProvideRuntimeApi, StorageChanges, StorageProof, TransactionOutcome,
};
use sp_blockchain::{ApplyExtrinsicFailed, Error};
use sp_core::ExecutionContext;
use sp_runtime::{
	legacy,
	traits::{Block as BlockT, Hash, HashFor, Header as HeaderT, NumberFor, One},
	Digest,
};

use sc_client_api::backend;
pub use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_blockchain::ApplyExtrinsicFailed::Validity;
use sp_runtime::transaction_validity::{InvalidTransaction, TransactionValidityError};

/// Used as parameter to [`BlockBuilderProvider`] to express if proof recording should be enabled.
///
/// When `RecordProof::Yes` is given, all accessed trie nodes should be saved. These recorded
/// trie nodes can be used by a third party to proof this proposal without having access to the
/// full storage.
#[derive(Copy, Clone, PartialEq)]
pub enum RecordProof {
	/// `Yes`, record a proof.
	Yes,
	/// `No`, don't record any proof.
	No,
}

impl RecordProof {
	/// Returns if `Self` == `Yes`.
	pub fn yes(&self) -> bool {
		matches!(self, Self::Yes)
	}
}

/// Will return [`RecordProof::No`] as default value.
impl Default for RecordProof {
	fn default() -> Self {
		Self::No
	}
}

impl From<bool> for RecordProof {
	fn from(val: bool) -> Self {
		if val {
			Self::Yes
		} else {
			Self::No
		}
	}
}

/// A block that was build by [`BlockBuilder`] plus some additional data.
///
/// This additional data includes the `storage_changes`, these changes can be applied to the
/// backend to get the state of the block. Furthermore an optional `proof` is included which
/// can be used to proof that the build block contains the expected data. The `proof` will
/// only be set when proof recording was activated.
pub struct BuiltBlock<Block: BlockT, StateBackend: backend::StateBackend<HashFor<Block>>> {
	/// The actual block that was build.
	pub block: Block,
	/// The changes that need to be applied to the backend to get the state of the build block.
	pub storage_changes: StorageChanges<StateBackend, Block>,
	/// An optional proof that was recorded while building the block.
	pub proof: Option<StorageProof>,
}

impl<Block: BlockT, StateBackend: backend::StateBackend<HashFor<Block>>>
	BuiltBlock<Block, StateBackend>
{
	/// Convert into the inner values.
	pub fn into_inner(self) -> (Block, StorageChanges<StateBackend, Block>, Option<StorageProof>) {
		(self.block, self.storage_changes, self.proof)
	}
}

/// Block builder provider
pub trait BlockBuilderProvider<B, Block, RA>
where
	Block: BlockT,
	B: backend::Backend<Block>,
	Self: Sized,
	RA: ProvideRuntimeApi<Block>,
{
	/// Create a new block, built on top of `parent`.
	///
	/// When proof recording is enabled, all accessed trie nodes are saved.
	/// These recorded trie nodes can be used by a third party to proof the
	/// output of this block builder without having access to the full storage.
	fn new_block_at<R: Into<RecordProof>>(
		&self,
		parent: Block::Hash,
		inherent_digests: Digest,
		record_proof: R,
		context: Option<ExecutionContext>,
	) -> sp_blockchain::Result<BlockBuilder<Block, RA, B>>;

	/// Create a new block with some state.
	///
	/// This is used to copy other builder.
	fn new_with_other(
		&self,
		parent: Block::Hash,
		estimated_header_size: usize,
		context: Option<ExecutionContext>,
	) -> sp_blockchain::Result<BlockBuilder<Block, RA, B>>;

	/// Create a new block, built on the head of the chain.
	fn new_block(
		&self,
		inherent_digests: Digest,
	) -> sp_blockchain::Result<BlockBuilder<Block, RA, B>>;
}

/// Utility for building new (valid) blocks from a stream of extrinsics.
pub struct BlockBuilder<'a, Block: BlockT, A: ProvideRuntimeApi<Block>, B> {
	/// execution context,
	pub context: ExecutionContext,
	/// current applied extrinsic list.
	pub extrinsics: Vec<Block::Extrinsic>,
	/// Runtime api env.
	pub api: ApiRef<'a, A::Api>,
	/// api_version.
	pub version: u32,
	/// parent hash
	pub parent_hash: Block::Hash,
	/// backend
	pub backend: &'a Arc<B>,
	/// The estimated size of the block header.
	pub estimated_header_size: usize,
}

impl<'a, Block, A, B> BlockBuilder<'a, Block, A, B>
where
	Block: BlockT,
	A: ProvideRuntimeApi<Block> + 'a,
	A::Api:
		BlockBuilderApi<Block> + ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>>,
	B: backend::Backend<Block>,
{
	/// Create a new instance of builder based on the given `parent_hash` and `parent_number`.
	///
	/// While proof recording is enabled, all accessed trie nodes are saved.
	/// These recorded trie nodes can be used by a third party to prove the
	/// output of this block builder without having access to the full storage.
	pub fn new(
		api: &'a A,
		parent_hash: Block::Hash,
		parent_number: NumberFor<Block>,
		record_proof: RecordProof,
		inherent_digests: Digest,
		backend: &'a Arc<B>,
		context: Option<ExecutionContext>,
	) -> Result<Self, Error> {
		let header = <<Block as BlockT>::Header as HeaderT>::new(
			parent_number + One::one(),
			Default::default(),
			Default::default(),
			parent_hash,
			inherent_digests,
		);

		let estimated_header_size = header.encoded_size();

		let mut api = api.runtime_api();

		if record_proof.yes() {
			api.record_proof();
		}
		let context = context.unwrap_or(ExecutionContext::BlockConstruction);
		api.initialize_block_with_context(
			parent_hash,
			context.clone(),
			&header,
		)?;

		let version = api
			.api_version::<dyn BlockBuilderApi<Block>>(parent_hash)?
			.ok_or_else(|| Error::VersionInvalid("BlockBuilderApi".to_string()))?;

		Ok(Self {
			context,
			parent_hash,
			extrinsics: Vec::new(),
			api,
			version,
			backend,
			estimated_header_size,
		})
	}

	/// Create new BlockBuilder with other state.
	pub fn new_with_other(
		api: &'a A,
		parent_hash: Block::Hash,
		estimated_header_size: usize,
		backend: &'a Arc<B>,
		context: Option<ExecutionContext>,
	) -> Result<Self, Error> {
		let api = api.runtime_api();
		let version = api
			.api_version::<dyn BlockBuilderApi<Block>>(parent_hash)?
			.ok_or_else(|| Error::VersionInvalid("BlockBuilderApi".to_string()))?;

		let context = context.unwrap_or(ExecutionContext::BlockConstruction);
		Ok(Self {
			context,
			parent_hash,
			extrinsics: Vec::new(),
			api,
			version,
			backend,
			estimated_header_size,
		})
	}

	/// Push onto the block's list of extrinsics.
	///
	/// This will ensure the extrinsic can be validly executed (by executing it).
	pub fn push(&mut self, xt: <Block as BlockT>::Extrinsic) -> Result<(), Error> {
		let parent_hash = self.parent_hash;
		let extrinsics = &mut self.extrinsics;
		let version = self.version;

		self.api.execute_in_transaction(|api| {
			let res = if version < 6 {
				#[allow(deprecated)]
				api.apply_extrinsic_before_version_6_with_context(
					parent_hash,
					self.context.clone(),
					xt.clone(),
				)
				.map(legacy::byte_sized_error::convert_to_latest)
			} else {
				api.apply_extrinsic_with_context(
					parent_hash,
					self.context.clone(),
					xt.clone(),
				)
			};

			match res {
				Ok(Ok(_)) => {
					extrinsics.push(xt);
					TransactionOutcome::Commit(Ok(()))
				},
				Ok(Err(tx_validity)) => TransactionOutcome::Rollback(Err(
					ApplyExtrinsicFailed::Validity(tx_validity).into(),
				)),
				Err(e) => TransactionOutcome::Rollback(Err(Error::from(e))),
			}
		})
	}

	/// Push onto the block's list of extrinsics.
	pub fn push_batch(&mut self, xts: Vec<<Block as BlockT>::Extrinsic>, timeout: std::time::Duration)
		-> (Vec<Result<(), Error>>, Vec<(usize, Error)>, Vec<(usize, Error)>, Vec<(usize, Error)>)
	{
		let parent_hash = self.parent_hash;
		let extrinsics = &mut self.extrinsics;
		let version = self.version;
        let mut rollback = Vec::new();
		let mut stale = Vec::new();
		let mut future = Vec::new();
        let mut batch_results = Vec::with_capacity(xts.len());

		self.api.execute_in_transaction(|api| {
			let (res, roll_back) = if version < 6 {
				let start = std::time::Instant::now();
				for (i, xt) in xts.clone().into_iter().enumerate() {
					#[allow(deprecated)]
					match api.apply_extrinsic_before_version_6_with_context(
						parent_hash,
						self.context.clone(),
						xt.clone(),
					)
						.map(legacy::byte_sized_error::convert_to_latest) {
						Ok(result) => {
                            match result {
                                Ok(r) => batch_results.push(Ok(r)),
                                Err(e) => {
									if e.stale() {
										stale.push((i, ApplyExtrinsicFailed::Validity(e).into()));
									}
									if e.future() {
										future.push((i, ApplyExtrinsicFailed::Validity(e).into()));
									}
                                    if !e.exhausted_resources() && !e.stale_or_future() {
                                        rollback.push((i, ApplyExtrinsicFailed::Validity(e).into()));
                                        break;
                                    } else {
                                        batch_results.push(Err(ApplyExtrinsicFailed::Validity(e).into()));
                                    }
                                }
                            }
                        },
						Err(e) => {
                            rollback.push((i, Error::from(e)));
							break;
						},
					}
					if start.elapsed() >= timeout {
						break;
					}
				}
				(batch_results, rollback)
			} else {
				match api.apply_extrinsics_with_context(
					parent_hash,
					ExecutionContext::BlockConstruction,
					xts.clone(),
					timeout.as_nanos(),
				) {
					Ok(results) => {
                        for (i, result) in results.into_iter().enumerate() {
                            match result {
                                Ok(r) => batch_results.push(Ok(r)),
                                Err(e) => {
                                    // For multi threads execution, exhausted_resources does not matter.
									// Nonce Stale or Future also doesn't matter since we did no storage changes.
									if e.stale() {
										stale.push((i, ApplyExtrinsicFailed::Validity(e).into()));
									}
									if e.future() {
										future.push((i, ApplyExtrinsicFailed::Validity(e).into()));
									}
                                    if !e.exhausted_resources() && !e.stale_or_future() {
                                        // if meat other error, should roll back.
                                        // record roll back index.
										rollback.push((i, ApplyExtrinsicFailed::Validity(e).into()));
                                    }
									batch_results.push(Err(ApplyExtrinsicFailed::Validity(e).into()))
                                }
                            }
                        }
                        (batch_results, rollback)
					},
					Err(e) => (vec![], vec![(0, Error::from(e))]),
				}
			};
            if roll_back.is_empty() {
                let mut results = Vec::new();
                for (res, xt) in res.into_iter().zip(xts.into_iter()) {
                    results.push(match res {
                        Ok(_) => {
                            extrinsics.push(xt.clone());
                            Ok(())
                        },
                        Err(e) => Err(e),
                    });
                }
                TransactionOutcome::Commit((results, roll_back, stale, future))
            } else {
                TransactionOutcome::Rollback((Vec::new(), roll_back, stale, future))
            }
		})
	}

	/// Consume the builder to build a valid `Block` containing all pushed extrinsics.
	///
	/// Returns the build `Block`, the changes to the storage and an optional `StorageProof`
	/// supplied by `self.api`, combined as [`BuiltBlock`].
	/// The storage proof will be `Some(_)` when proof recording was enabled.
	pub fn build(mut self) -> Result<BuiltBlock<Block, backend::StateBackendFor<B, Block>>, Error> {
		let header = self
			.api
			.finalize_block_with_context(self.parent_hash, ExecutionContext::BlockConstruction)?;

		debug_assert_eq!(
			header.extrinsics_root().clone(),
			HashFor::<Block>::ordered_trie_root(
				self.extrinsics.iter().map(Encode::encode).collect(),
				sp_runtime::StateVersion::V0,
			),
		);

		let proof = self.api.extract_proof();

		let state = self.backend.state_at(self.parent_hash)?;

		let storage_changes = self
			.api
			.into_storage_changes(&state, self.parent_hash)
			.map_err(sp_blockchain::Error::StorageChanges)?;

		Ok(BuiltBlock {
			block: <Block as BlockT>::new(header, self.extrinsics),
			storage_changes,
			proof,
		})
	}

	/// Create the inherents for the block.
	///
	/// Returns the inherents created by the runtime or an error if something failed.
	pub fn create_inherents(
		&mut self,
		inherent_data: sp_inherents::InherentData,
	) -> Result<Vec<Block::Extrinsic>, Error> {
		let parent_hash = self.parent_hash;
		self.api
			.execute_in_transaction(move |api| {
				// `create_inherents` should not change any state, to ensure this we always rollback
				// the transaction.
				TransactionOutcome::Rollback(api.inherent_extrinsics_with_context(
					parent_hash,
					ExecutionContext::BlockConstruction,
					inherent_data,
				))
			})
			.map_err(|e| Error::Application(Box::new(e)))
	}

	/// Estimate the size of the block in the current state.
	///
	/// If `include_proof` is `true`, the estimated size of the storage proof will be added
	/// to the estimation.
	pub fn estimate_block_size(&self, include_proof: bool) -> usize {
		let size = self.estimated_header_size + self.extrinsics.encoded_size();

		if include_proof {
			size + self.api.proof_recorder().map(|pr| pr.estimate_encoded_size()).unwrap_or(0)
		} else {
			size
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use sp_blockchain::HeaderBackend;
	use sp_core::Blake2Hasher;
	use sp_state_machine::Backend;
	use substrate_test_runtime_client::{
		runtime::ExtrinsicBuilder, DefaultTestClientBuilderExt, TestClientBuilderExt,
	};

	#[test]
	fn block_building_storage_proof_does_not_include_runtime_by_default() {
		let builder = substrate_test_runtime_client::TestClientBuilder::new();
		let backend = builder.backend();
		let client = builder.build();

		let genesis_hash = client.info().best_hash;

		let block = BlockBuilder::new(
			&client,
			genesis_hash,
			client.info().best_number,
			RecordProof::Yes,
			Default::default(),
			&*backend,
			None,
		)
		.unwrap()
		.build()
		.unwrap();

		let proof = block.proof.expect("Proof is build on request");
		let genesis_state_root = client.header(genesis_hash).unwrap().unwrap().state_root;

		let backend =
			sp_state_machine::create_proof_check_backend::<Blake2Hasher>(genesis_state_root, proof)
				.unwrap();

		assert!(backend
			.storage(&sp_core::storage::well_known_keys::CODE)
			.unwrap_err()
			.contains("Database missing expected key"),);
	}

	#[test]
	fn failing_extrinsic_rolls_back_changes_in_storage_proof() {
		let builder = substrate_test_runtime_client::TestClientBuilder::new();
		let backend = builder.backend();
		let client = builder.build();

		let mut block_builder = BlockBuilder::new(
			&client,
			client.info().best_hash,
			client.info().best_number,
			RecordProof::Yes,
			Default::default(),
			&*backend,
			None,
		)
		.unwrap();

		block_builder.push(ExtrinsicBuilder::new_read_and_panic(8).build()).unwrap_err();

		let block = block_builder.build().unwrap();

		let proof_with_panic = block.proof.expect("Proof is build on request").encoded_size();

		let mut block_builder = BlockBuilder::new(
			&client,
			client.info().best_hash,
			client.info().best_number,
			RecordProof::Yes,
			Default::default(),
			&*backend,
			None,
		)
		.unwrap();

		block_builder.push(ExtrinsicBuilder::new_read(8).build()).unwrap();

		let block = block_builder.build().unwrap();

		let proof_without_panic = block.proof.expect("Proof is build on request").encoded_size();

		let block = BlockBuilder::new(
			&client,
			client.info().best_hash,
			client.info().best_number,
			RecordProof::Yes,
			Default::default(),
			&*backend,
		)
		.unwrap()
		.build()
		.unwrap();

		let proof_empty_block = block.proof.expect("Proof is build on request").encoded_size();

		// Ensure that we rolled back the changes of the panicked transaction.
		assert!(proof_without_panic > proof_with_panic);
		assert!(proof_without_panic > proof_empty_block);
		assert_eq!(proof_empty_block, proof_with_panic);
	}
}
