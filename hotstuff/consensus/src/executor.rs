use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use log::{debug, warn};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::RwLock;
use hotstuff_primitives::digests::CompatibleDigestItem;
use sc_basic_authorship::BlockPropose;
use sc_client_api::Backend;
use sc_consensus::{BlockImport, BlockImportParams, ForkChoiceStrategy, StateAction};
use sc_consensus_slots::{InherentDataProviderExt, StorageChanges};
use sp_api::{BlockT, HeaderT, TransactionFor};
use sp_consensus::{BlockOrigin, Environment, Error as ConsensusError, Proposer, SelectChain};
use sp_consensus_slots::Slot;
use sp_inherents::{CreateInherentDataProviders, InherentDataProvider};
use sp_runtime::{Digest, DigestItem, Saturating};
use sp_timestamp::Timestamp;
use crate::client::ClientForHotstuff;
use crate::import::ImportLock;
use crate::message::{NextBlock, QC};

pub struct BlockExecutor<C: ClientForHotstuff<B, BE>, B: BlockT, I, PF, L: sc_consensus::JustificationSyncLink<B>, BE: Backend<B>, CIDP, SC> {
    pub client: Arc<C>,
    pub proposer_factory: Arc<RwLock<PF>>,
    pub import: I,
    pub justification_sync_link: L,
    pub create_inherent_data_providers: CIDP,
    pub select_chain: SC,
    pub last_slot: Slot,
    pub executor_rx: UnboundedReceiver<([QC<B>; 3], NextBlock<B>)>,
    pub missions: HashMap<<B::Header as HeaderT>::Number, ([QC<B>; 3], NextBlock<B>)>,
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
        proposer_factory: Arc<RwLock<PF>>,
        justification_sync_link: L,
        create_inherent_data_providers: CIDP,
        select_chain: SC,
        executor_rx: UnboundedReceiver<([QC<B>; 3], NextBlock<B>)>,
    ) -> Self {
        Self {
            client,
            proposer_factory,
            import,
            justification_sync_link,
            create_inherent_data_providers,
            select_chain,
            last_slot: 0.into(),
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
        full_qc: [QC<B>; 3],
    ) -> Result<BlockImportParams<B, TransactionFor<C, B>>, ConsensusError> {
        let mut import_block = BlockImportParams::new(BlockOrigin::ConsensusBroadcast, header);
        let digest_item = <DigestItem as CompatibleDigestItem<Vec<u32>>>::hotstuff_seal(groups);
        import_block.post_digests.push(digest_item);
        let digest_item = <DigestItem as CompatibleDigestItem<Vec<QC<B>>>>::hotstuff_seal(full_qc.to_vec());
        import_block.post_digests.push(digest_item);
        import_block.body = Some(body);
        import_block.state_action =
            StateAction::ApplyChanges(sc_consensus::StorageChanges::Changes(storage_changes));
        import_block.fork_choice = Some(ForkChoiceStrategy::LongestChain);

        Ok(import_block)
    }

    pub async fn run(&mut self) {
        loop {
            if let Some((full_qc, next_block)) = self.executor_rx.recv().await {
                self.import.lock(BlockOrigin::ConsensusBroadcast, next_block.block_number).await;
                let block_number = next_block.block_number;
                if block_number > self.client.info().finalized_number {
                    // push next_block to missions.
                    if let Some(mission) = self.missions.get_mut(&block_number) {
                        if full_qc[0].timestamp >= mission.0[0].timestamp {
                            *mission = (full_qc, next_block);
                        }
                    } else {
                        self.missions.insert(block_number, (full_qc, next_block));
                    }
                }
                // try handle all next_blocks for missions.
                loop {
                    let chain_head = match self.select_chain.best_chain().await {
                        Ok(x) => x,
                        Err(e) => {
                            warn!(target: "Hotstuff", "[Execute] Unable to author block in slot. No best block header: {e}");
                            continue
                        },
                    };
                    let best_block = chain_head.number().clone();
                    let best_hash = chain_head.hash();
                    let next_block_number = best_block.saturating_add(1u32.into());
                    self.missions.remove(&best_block);
                    if let Some((full_qc, next_block)) = self.missions.get(&next_block_number) {
                        if next_block.extrinsic.is_none() {
                            warn!(target: "Hotstuff", "[Execute] Next block number: {} skipped for None extrinsic", next_block.block_number);
                            self.import.unlock(BlockOrigin::ConsensusBroadcast, next_block.block_number).await;
                            continue;
                        }
                        let extrinsic = next_block.extrinsic.clone().unwrap();
                        // execute blcock
                        // creat inherent data
                        let inherent_data_providers = match self
                            .create_inherent_data_providers
                            .create_inherent_data_providers(chain_head.hash(), full_qc[0].timestamp)
                            .await
                        {
                            Ok(x) => x,
                            Err(e) => {
                                warn!(target: "Hotstuff", "[Execute] Unable to author block in slot. Failure creating inherent data provider: {e}");
                                self.import.unlock(BlockOrigin::ConsensusBroadcast, next_block.block_number).await;
                                continue
                            },
                        };
                        debug!(target: "Hotstuff", "[Execute] Best block number: {best_block}, next block number: {} start execute", next_block.block_number);
                        let slot = inherent_data_providers.slot();// Never yield the same slot twice.
                        if slot > self.last_slot {
                            self.last_slot = slot;
                        } else {
                            warn!(target: "Hotstuff", "[Execute] Best block number: {best_block}, next block number: {} skipped for last_slot {}, current_slot {slot}", next_block.block_number, self.last_slot);
                            self.import.unlock(BlockOrigin::ConsensusBroadcast, next_block.block_number).await;
                            continue;
                        }
                        let inherent_data = inherent_data_providers.create_inherent_data().await.expect("create_inherent_data");

                        let logs = vec![<DigestItem as CompatibleDigestItem<Slot>>::hotstuff_pre_digest(slot)];
                        let parent_header = self
                            .client
                            .header(best_hash)
                            .expect("get best header")
                            .expect("no expected best header");
                        let proposer = self.proposer_factory.write().await.init(&parent_header).await.expect("proposer init");
                        let (proposal, groups) = match BlockPropose::<B>::propose_block(
                            proposer,
                            self.block_size_limit,
                            inherent_data,
                            Digest { logs },
                            extrinsic,
                            true,
                        ).await {
                            Ok(propose) => propose,
                            Err(e) => {
                                warn!(target: "Hotstuff", "[Execute] Propose block {} failed for {e:?}", next_block.block_number);
                                self.import.unlock(BlockOrigin::ConsensusBroadcast, next_block.block_number).await;
                                continue;
                            }
                        };
                        // generate import params
                        let (block, _storage_proof) = (proposal.block, proposal.proof);
                        let (header, body) = block.deconstruct();
                        let block_import_params = match self.block_import_params(
                            header,
                            body.clone(),
                            proposal.storage_changes,
                            groups,
                            full_qc.clone(),
                        ).await {
                            Ok(import_params) => import_params,
                            Err(e) => {
                                warn!(target: "Hotstuff", "[Execute] Propose block {} get block_import_params failed for {e:?}", next_block.block_number);
                                self.import.unlock(BlockOrigin::ConsensusBroadcast, next_block.block_number).await;
                                continue;
                            }
                        };
                        // import block
                        let header = block_import_params.post_header();
                        // self.import.import_block will call `unlock` by self.
                        match self.import.import_block(block_import_params).await {
                            Ok(res) => {
                                res.handle_justification(
                                    &header.hash(),
                                    *header.number(),
                                    &self.justification_sync_link,
                                );
                            },
                            Err(err) => {
                                warn!(target: "Hotstuff", "[Execute] Error with block {} built on {best_hash:?}: {err}", next_block.block_number);
                            },
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }
}
