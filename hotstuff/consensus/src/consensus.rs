use std::{
    cmp::max,
    collections::VecDeque,
    env,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use std::marker::PhantomData;
use std::ops::{Add, Deref};
use async_recursion::async_recursion;
use futures::{channel::mpsc::Receiver as Recv, Future, StreamExt};

use codec::{Decode, Encode};
use log::{debug, error, info, trace, warn};
use tokio::sync::mpsc::{channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock;
use sc_basic_authorship::StepBlockPropose;
use sc_client_api::{Backend, CallExecutor, ExecutionStrategy};
use sc_network::types::ProtocolName;
use sc_network_gossip::TopicNotification;
use sp_application_crypto::AppCrypto;
use sp_blockchain::BlockStatus;
use sp_core::{crypto::ByteArray, traits::CallContext};
use sp_keystore::KeystorePtr;
use sp_runtime::{generic::BlockId, traits::{Block as BlockT, Hash as HashT, Header as HeaderT, Zero}, Digest, Saturating};
use hotstuff_primitives::digests::CompatibleDigestItem;
use sc_consensus::{BlockImport, BlockImportParams, ForkChoiceStrategy, StateAction};
use sc_consensus_slots::{InherentDataProviderExt, StorageChanges};
use sp_consensus::{BlockOrigin, SelectChain};
use sp_runtime::DigestItem;

use crate::{
    aggregator::Aggregator, client::{ClientForHotstuff, LinkHalf},
    import::{BlockInfo, PendingFinalizeBlockQueue},
    message::{ConsensusMessage, ConsensusMessage::*, Payload, Proposal, Timeout, Vote, QC, TC},
    network::{HotstuffNetworkBridge, Network as NetworkT, Syncing as SyncingT},
    primitives::{HotstuffError, HotstuffError::*, PayloadError, ViewNumber}, synchronizer::{Synchronizer, Timer},
};
use hotstuff_primitives::{AuthorityId, AuthorityList, AuthoritySignature, HotstuffApi, Slot, HOTSTUFF_KEY_TYPE};
use hotstuff_primitives::inherents::InherentType;
use sp_api::TransactionFor;
use sp_consensus::{Environment, Error as ConsensusError, Proposer};
use sp_consensus_slots::SlotDuration;
use sp_inherents::{CreateInherentDataProviders, InherentDataProvider};
use sp_runtime::transaction_validity::TransactionSource;
use sp_timestamp::Timestamp;
use crate::message::{ConsensusStage, NewBlock, NextBlock};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

pub(crate) const EMPTY_PAYLOAD: &[u8] = b"hotstuff/empty_payload";

// the core of hotstuff
pub struct ConsensusState<B: BlockT> {
    keystore: KeystorePtr,
    authorities: AuthorityList,
    local_authority_id: Option<AuthorityId>,
    // local view number
    view: ViewNumber,
    last_voted_view: ViewNumber,
    // last_committed_round: ViewNumber,
    high_qc: QC<B>,
    aggregator: Aggregator<B>,
}

impl<B: BlockT> ConsensusState<B> {
    pub fn new(keystore: KeystorePtr, authorities: AuthorityList) -> Self {
        Self {
            local_authority_id: authorities
                .iter()
                .find(|(p, _)| {
                    keystore
                        .has_keys(&[(p.to_raw_vec(), HOTSTUFF_KEY_TYPE)])
                })
                .map(|(p, _)| p.clone()),
            keystore,
            authorities,
            view: 0,
            last_voted_view: 0,
            high_qc: Default::default(),
            aggregator: Aggregator::<B>::new(),
        }
    }

    pub fn increase_last_voted_view(&mut self) {
        self.last_voted_view = max(self.last_voted_view, self.view)
    }

    pub fn make_timeout(&self) -> Result<Timeout<B>, HotstuffError> {
        let authority_id = self.local_authority_id.as_ref().ok_or(NotAuthority)?;

        let mut tc: Timeout<B> = Timeout {
            high_qc: self.high_qc.clone(),
            view: self.view,
            voter: authority_id.clone(),
            signature: None,
        };

        tc.signature = self
            .keystore
            .sign_with(
                AuthorityId::ID,
                AuthorityId::CRYPTO_ID,
                authority_id.as_ref(),
                tc.digest().as_ref(),
            )
            .map_err(|e| Other(e.to_string()))?
            .and_then(|data| AuthoritySignature::try_from(data).ok());

        Ok(tc)
    }

    pub fn make_proposal(
        &self,
        timestamp: Timestamp,
        payload: Payload<B>,
        tc: Option<TC<B>>,
    ) -> Result<Proposal<B>, HotstuffError> {
        let author_id = self.local_authority_id.as_ref().ok_or(NotAuthority)?;
        let mut block = Proposal::<B>::new(
            self.high_qc.clone(),
            tc,
            payload,
            self.view,
            timestamp,
            author_id.clone(),
            None,
        );

        block.signature = self
            .keystore
            .sign_with(
                AuthorityId::ID,
                AuthorityId::CRYPTO_ID,
                author_id.as_slice(),
                block.digest().as_ref(),
            )
            .map_err(|e| Other(e.to_string()))?
            .and_then(|data| AuthoritySignature::try_from(data).ok());

        Ok(block)
    }

    pub fn make_vote(&mut self, proposal: &Proposal<B>) -> Option<Vote<B>> {
        let author_id = self.local_authority_id.as_ref()?;

        if proposal.view <= self.last_voted_view {
            return None;
        }

        self.last_voted_view = max(self.last_voted_view, proposal.view);

        let mut vote = Vote::<B>::new(proposal.digest(), proposal.view, author_id.clone());

        vote.signature = self
            .keystore
            .sign_with(
                AuthorityId::ID,
                AuthorityId::CRYPTO_ID,
                author_id.as_slice(),
                vote.digest().as_ref(),
            )
            .ok()?
            .and_then(|data| AuthoritySignature::try_from(data).ok());

        Some(vote)
    }

    pub fn view(&self) -> ViewNumber {
        self.view
    }

    pub fn verify_timeout(&self, timeout: &Timeout<B>) -> Result<(), HotstuffError> {
        timeout.verify(&self.authorities)
    }

    pub fn verify_proposal(&self, proposal: &Proposal<B>) -> Result<(), HotstuffError> {
        // TODO SHOULD how process authority changed.
        match self.view_leader(proposal.view) {
            Some(leader) => if !proposal.author.eq(leader) {
                return Err(WrongProposer);
            }
            None => return Err(HotstuffError::Other("Local empty authorities to verify proposal".into())),
        }
        proposal.verify(&self.authorities)?;
        Ok(())
    }

    pub fn is_proposer(&self, proposal: &Proposal<B>) -> bool {
        if let Some(local_authority_id) = &self.local_authority_id {
            if &proposal.author == local_authority_id {
                return true;
            }
        }
        false
    }

    pub fn verify_vote(&self, vote: &Vote<B>) -> Result<(), HotstuffError> {
        if vote.view < self.view {
            return Err(ExpiredVote);
        }

        vote.verify(&self.authorities)
    }

    pub fn verify_tc(&self, tc: &TC<B>) -> Result<(), HotstuffError> {
        if tc.view < self.view {
            return Err(InvalidTC);
        }

        tc.verify(&self.authorities)
    }

    // add a verified timeout then try return a TC.
    pub fn add_timeout(&mut self, timeout: &Timeout<B>) -> Result<Option<TC<B>>, HotstuffError> {
        self.aggregator.add_timeout(timeout, &self.authorities)
    }

    // add a verified vote and try return a QC.
    pub fn add_vote(&mut self, vote: &Vote<B>) -> Result<Option<QC<B>>, HotstuffError> {
        self.aggregator.add_vote(vote.clone(), &self.authorities)
    }

    pub fn update_high_qc(&mut self, qc: &QC<B>) {
        if qc.view > self.high_qc.view {
            self.high_qc = qc.clone()
        }
    }

    pub fn advance_view_from_target(&mut self, view: ViewNumber) {
        if self.view <= view {
            self.view = view + 1;
        }
    }

    pub fn view_leader(&self, view: ViewNumber) -> Option<&AuthorityId> {
        if self.authorities.is_empty() {
            return None;
        }
        let leader_index = view % self.authorities.len() as ViewNumber;
        Some(&self.authorities[leader_index as usize].0)
    }

    pub fn is_leader(&self) -> bool {
        match self.local_authority_id.as_ref() {
            Some(id) => {
                match self.view_leader(self.view) {
                    Some(leader) => id == leader,
                    None => false,
                }
            }
            None => false,
        }
    }

    pub fn is_next_leader(&self) -> bool {
        match self.local_authority_id.as_ref() {
            Some(id) => {
                match self.view_leader(self.view + 1) {
                    Some(leader) => id == leader,
                    None => false,
                }
            }
            None => false,
        }
    }
}

pub struct ConsensusWorker<
    B: BlockT,
    BE: Backend<B>,
    C: ClientForHotstuff<B, BE>,
    N: NetworkT<B> + Sync + 'static,
    S: SyncingT<B> + Sync + 'static,
    PF: Environment<B, Error = Error> + Send + Sync + 'static,
    Error: std::error::Error + Send + From<ConsensusError> + 'static,
> {
    state: ConsensusState<B>,

    network: HotstuffNetworkBridge<B, N, S>,
    client: Arc<C>,
    sync: S,
    local_timer: Timer,
    slot_duration: SlotDuration,
    synchronizer: Synchronizer<B, BE, C>,
    consensus_msg_tx: Sender<(bool, ConsensusMessage<B>)>,
    consensus_msg_rx: Receiver<(bool, ConsensusMessage<B>)>,
    executor_tx: UnboundedSender<(Timestamp, [QC<B>; 3], NextBlock<B>)>,

    proposer_factory: Arc<RwLock<PF>>,
    processing_block: Option<NextBlock<B>>,
    /// consensus success block waiting to execute.
    processed: NextBlock<B>,
    processed_timestamp: u64,

    processing_finalize_block: Option<NewBlock<B>>,
    pending_finalize_queue: Arc<Mutex<VecDeque<BlockInfo<B>>>>,
}

impl<B, BE, C, N, S, PF, Error> ConsensusWorker<B, BE, C, N, S, PF, Error>
where
    B: BlockT,
    BE: Backend<B>,
    C: ClientForHotstuff<B, BE>,
    C::Api: HotstuffApi<B, AuthorityId>,
    N: NetworkT<B> + Sync + 'static,
    S: SyncingT<B> + Sync + 'static,
    PF: Environment<B, Error = Error> + Send + Sync + 'static,
    PF::Proposer: Proposer<B, Error = Error, Transaction = sp_api::TransactionFor<C, B>> + StepBlockPropose<B>,
    Error: std::error::Error + Send + From<ConsensusError> + 'static,
{
    #![allow(clippy::too_many_arguments)]
    pub fn new(
        consensus_state: ConsensusState<B>,
        client: Arc<C>,
        sync: S,
        network: HotstuffNetworkBridge<B, N, S>,
        synchronizer: Synchronizer<B, BE, C>,
        proposer_factory: Arc<RwLock<PF>>,
        local_timer_duration: u64,
        slot_duration: SlotDuration,
        consensus_msg_tx: Sender<(bool, ConsensusMessage<B>)>,
        consensus_msg_rx: Receiver<(bool, ConsensusMessage<B>)>,
        executor_tx: UnboundedSender<(Timestamp, [QC<B>; 3], NextBlock<B>)>,
        pending_finalize_queue: Arc<Mutex<VecDeque<BlockInfo<B>>>>,
    ) -> Self {
        if local_timer_duration * 3 <= slot_duration.as_millis() {
            let slot_duration_millis = slot_duration.as_millis();
            panic!("HOTSTUFF_DURATION({local_timer_duration}) * 3 should greater than slot_duration({slot_duration_millis})!!! Please set env `HOTSTUFF_DURATION` greater than {slot_duration_millis}!!!");
        }
        // TODO MAY initialize with actually latest processed block's QC timestamp.
        let processed_timestamp = client.runtime_api().current_slot(client.info().best_hash).unwrap_or_default().deref() * slot_duration.as_millis() + slot_duration.as_millis();
        Self {
            state: consensus_state,
            network,
            local_timer: Timer::new(local_timer_duration),
            slot_duration,
            consensus_msg_tx,
            consensus_msg_rx,
            executor_tx,
            client,
            sync,
            synchronizer,
            proposer_factory,
            // TODO SHOULD should load from some where?
            processing_block: None,
            processed_timestamp,
            processed: NextBlock::empty(),
            processing_finalize_block: None,
            pending_finalize_queue,
        }
    }

    pub async fn run(mut self) {
        if let Some(ref id) = self.state.local_authority_id {
            info!(target: "Hotstuff", "Local authority id is: {id}");
        } else {
            info!(target: "Hotstuff", "Local is not authority");
        }
        self.local_timer.reset();
        loop {
            let _ = tokio::select! {
                _ = &mut self.local_timer => self.handle_local_timer().await,
                Some((local, message)) = self.consensus_msg_rx.recv()=> {
                    let from = if local { "local" } else { "network" };
                    match message {
                        BlockImport(block_info) => {
                            match self.handle_block_import(&block_info).await{
                                Ok(_) => {},
                                Err(e) =>  debug!(target: "Hotstuff","{:#?} handle_block_import from {from} has error {e:#?}", self.state.local_authority_id),
                            };
                            Ok(())
                        },
                        Propose(proposal) => {
                            match self.handle_proposal(&proposal, local).await{
                                Ok(_) => {},
                                Err(e) =>  debug!(target: "Hotstuff","{:#?} handle_proposal from {from} has error {e:#?}", self.state.local_authority_id),
                            };
                            Ok(())
                        },
                        Vote(vote) => {
                            match self.handle_vote(&vote, local).await{
                                Ok(_) => {},
                                Err(e) => debug!(target: "Hotstuff","{:#?} handle_vote from {from} has error {e:#?}", self.state.local_authority_id),
                            };
                            Ok(())
                        },
                        Timeout(timeout) => {
                            match self.handle_timeout(&timeout, local).await{
                                Ok(_) => {},
                                Err(e) => debug!(target: "Hotstuff","{:#?} handle_timeout from {from} has error {e:#?}",self.state.local_authority_id),
                            };
                            Ok(())
                        },
                        TC(tc) => {
                            match self.handle_tc(tc, local).await{
                                Ok(_) => {},
                                Err(e) =>  debug!(target: "Hotstuff","{:#?} handle_tc from {from} has error {e:#?}", self.state.local_authority_id),
                            }
                            Ok(())
                        },
                        _ => Ok(()),
                    }
                }
            };
        }
    }

    pub async fn handle_local_timer(&mut self) -> Result<(), HotstuffError> {
        debug!(target: "Hotstuff","$L$ handle_local_timer. self.view {}", self.state.view());

        self.local_timer.reset();
        self.state.increase_last_voted_view();

        // current self.view will be timeout view.
        let timeout = self.state.make_timeout()?;
        let message = ConsensusMessage::Timeout(timeout.clone());

        self.network.gossip_engine.lock().gossip_message(
            ConsensusMessage::<B>::gossip_topic(),
            message.encode(),
            true,
        );

        if let Err(e) = self.consensus_msg_tx.send((true, ConsensusMessage::Timeout(timeout))).await {
            debug!(target: "Hotstuff","$L$ handle_local_timer. Can't inform self `Timeout` for {e:?}.");
        }
        Ok(())
    }

    pub async fn handle_timeout(&mut self, timeout: &Timeout<B>, local: bool) -> Result<(), HotstuffError> {
        if self.state.view() > timeout.view {
            return Ok(());
        }
        let from = if local { "local" } else { "network" };
        debug!(
            target: "Hotstuff",
            "~~ handle_timeout from {from}. view {}, author {}, high_qc.view {}, self.view {}",
			timeout.view,
            timeout.voter,
            timeout.high_qc.view,
            self.state.view(),
        );

        if !local {
            self.state.verify_timeout(timeout)?;
            timeout.high_qc.verify(&self.state.authorities)?;
            if timeout.high_qc.view >= self.state.view {
                self.handle_qc(&timeout.high_qc);
            }
        }

        if let Some(tc) = self.state.add_timeout(timeout)? {
            if let Err(e) = self.consensus_msg_tx.send((true, ConsensusMessage::TC(tc))).await {
                debug!(target: "Hotstuff","~~ handle_timeout from {from}. Can't inform self `TC` for {e:?}.");
            }
        }

        Ok(())
    }

    pub async fn handle_block_import(&mut self, _block_info: &BlockInfo<B>) -> Result<(), HotstuffError> {
        // Should update local time limit if generate new proposal.
        // check processing_finalize_block,
        // if self.processing_finalize_block.is_none() {
        //     self.local_timer.reset();
        //     if self.state.is_leader() {
        //         debug!(target: "Hotstuff","¥T¥ handle_block_import. leader make proposal, block: {}, hash: {:?}, self.view {}", block_info.number, block_info.hash, self.state.view());
        //         self.generate_proposal(None).await?;
        //     }
        // }
        Ok(())
    }

    #[async_recursion]
    pub async fn handle_proposal(&mut self, proposal: &Proposal<B>, local: bool) -> Result<(), HotstuffError> {
        let from = if local { "local" } else { "network" };
        debug!(target: "Hotstuff","~~ handle_proposal from {from}. self.view {}, proposal[ view:{}, author {}, digest {},  payload:{:?}]",
            self.state.view(),
            proposal.view,
            proposal.author,
            proposal.digest(),
            proposal.payload,
        );

        if !local {
            self.state.verify_proposal(proposal)?;
            proposal.qc.verify(&self.state.authorities)?;
            // verify payload
            if let Err(e) = self.check_payload(&proposal.payload) {
                debug!(target:"Hotstuff", "#^# Check proposal {} digest {} parent {} payload {:?} error: {e:?}", proposal.view, proposal.digest(), proposal.parent_hash(), proposal.payload);
                return Ok(())
            }
        }
        if let Some(tc) = proposal.tc.as_ref() {
            self.handle_qc(&tc.high_qc);
            if tc.view > self.state.view() {
                self.advance_view(tc.view);
                self.processing_block = None;
                self.processing_finalize_block = None;
            }
        }
        if !local {
            self.handle_qc(&proposal.qc);
        }
        if proposal.view != self.state.view() {
            return Ok(());
        }
        self.synchronizer.save_proposal(proposal)?;
        if !proposal.payload.next_block.is_empty() {
            self.processing_block = Some(proposal.payload.next_block.clone());
        }
        if !proposal.payload.new_block.is_empty() {
            self.processing_finalize_block = Some(proposal.payload.new_block.clone());
        }
        if !local {
            // if from local, the qc mission is triggered before this proposal generate.
            self.trigger_qc_mission(&proposal.qc, local)?;
        }

        if let Some(vote) = self.state.make_vote(proposal) {
            debug!(target: "Hotstuff","~~ handle proposal from {from}. make vote. view {}", vote.view);
            // If the current authority is the leader of the next view, it directly processes the
            // vote. Otherwise, it sends the vote to the next leader.
            if self.state.is_next_leader() {
                if let Err(e) = self.consensus_msg_tx.send((true, ConsensusMessage::Vote(vote))).await {
                    debug!(target: "Hotstuff","~~ handle proposal from {from}. Can't inform self of `Vote` for {e:?}.");
                }
            } else {
                let vote_message = ConsensusMessage::Vote(vote);

                self.network.gossip_engine.lock().gossip_message(
                    ConsensusMessage::<B>::gossip_topic(),
                    vote_message.encode(),
                    false,
                );
            }
        }
        Ok(())
    }

    pub async fn handle_vote(&mut self, vote: &Vote<B>, local: bool) -> Result<(), HotstuffError> {
        let from = if local { "local" } else { "network" };
        debug!(target: "Hotstuff","~~ handle_vote from {from}. self.view {}, vote.view {}, vote.author {}, vote.hash {}",
            self.state.view(),
            vote.view,
            vote.voter,
            vote.proposal_hash,
        );

        if !local {
            self.state.verify_vote(vote)?;
        }

        if let Some(mut qc) = self.state.add_vote(vote)? {
            self.handle_qc(&qc);
            if self.state.is_leader() {
                debug!(target: "Hotstuff","~~ handle_vote from {from}. QC.view {}, proposal_hash {}, self.view {}", qc.view, qc.proposal_hash, self.state.view());
                // keep self consensus duration greater than 1 / 3 slot_duration;
                let parent_start_time = match self.synchronizer.get_proposal(qc.proposal_hash)? {
                    Some(parent) => parent.timestamp,
                    // TODO MUST this is not real correct
                    None => self.state.high_qc.timestamp,
                };
                let min_qc_time = parent_start_time.as_millis() + self.slot_duration.as_millis() / 3;
                let current = Timestamp::current().as_millis();
                if qc.timestamp.as_millis() < min_qc_time {
                    tokio::time::sleep(std::time::Duration::from_millis(min_qc_time - current)).await;
                    qc.timestamp = Timestamp::from(min_qc_time);
                    self.state.high_qc.timestamp = qc.timestamp;
                    debug!(target: "Hotstuff","~~ handle_vote from {from}. QC.view {} delay to timestamp {min_qc_time} now {}", qc.view, Timestamp::current().as_millis());
                }
                self.trigger_qc_mission(&qc, true)?;
                self.generate_proposal(None).await?
            }
        }

        Ok(())
    }

    fn handle_qc(&mut self, qc: &QC<B>) {
        if qc.view >= self.state.view() {
            self.advance_view(qc.view);
            self.state.update_high_qc(qc);
            self.local_timer.reset();
        }
    }

    fn trigger_qc_mission(&mut self, qc: &QC<B>, local: bool) -> Result<(), HotstuffError> {
        let from = if local { "local" } else { "network" };
        // Try get proposal ancestors to handle consensus finish.
        if qc.proposal_hash != B::Hash::default() {
            let qc_proposal = match self.synchronizer.get_proposal(qc.proposal_hash)? {
                Some(p) => p,
                None => {
                    // TODO If we can't get them from local store, then get them by network. So should we block here.
                    unimplemented!("For devlop, should not missing proposal from synchronizer");
                }
            };
            // grandpa -> parent(grandpa_qc) -> qc_proposal(parent_qc) -> qc
            if qc_proposal.payload.next_block.stage.finish() {
                match self.synchronizer.get_proposal_ancestors(&qc_proposal, &|p| !p.payload.next_block.is_empty()) {
                    Ok(Some((parent, grandpa))) => {
                        if qc_proposal.view == parent.view + 1 && parent.view == grandpa.view + 1 {
                            if NextBlock::<B>::full_consensus(&qc_proposal.payload.next_block, &parent.payload.next_block, &grandpa.payload.next_block) {
                                if grandpa.payload.next_block.block_number > self.client.info().best_number {
                                    if grandpa.payload.next_block.extrinsic.is_some() {
                                        let slot = InherentType::from_timestamp(qc.timestamp, self.slot_duration);
                                        let processed_slot = InherentType::from_timestamp(Timestamp::from(self.processed_timestamp), self.slot_duration);
                                        if slot > processed_slot {
                                            self.processed_timestamp = qc.timestamp.as_millis();
                                            // send execute block mission to a block execute queue and execute/import it.
                                            let full_qc = [qc.clone(), qc_proposal.qc.clone(), parent.qc.clone()];
                                            debug!(
                                                target: "Hotstuff",
                                                "~~ trigger_qc_mission from {from}. block {} slot {slot} timestamp {} can be execute with full proposal: [{}, {}, {}]",
                                                qc_proposal.payload.next_block.block_number,
                                                qc.timestamp,
                                                qc.proposal_hash,
                                                qc_proposal.parent_hash(),
                                                parent.parent_hash(),
                                            );
                                            if let Err(e) = self.executor_tx.send((qc.timestamp, full_qc, grandpa.payload.next_block.clone())) {
                                                error!(target: "Hotstuff","~~ trigger_qc_mission from {from}. block {} execute mission send failed for {e:?}", grandpa.payload.next_block.block_number);
                                            }
                                            self.processed = grandpa.payload.next_block.clone();
                                            self.processing_block = None;
                                        } else {
                                            // skip update `self.processing_block` here should make next following `generate_proposal` to re-process this block.
                                            error!(target: "Hotstuff","~~ trigger_qc_mission from {from}. block {} execute mission skipped for slot rollback {processed_slot} -> {slot} ", grandpa.payload.next_block.block_number);
                                        }
                                    }
                                } else {
                                    debug!(target: "Hotstuff","~~ trigger_qc_mission from {from}. block {} already be executed or imported", grandpa.payload.next_block.block_number);
                                }
                            }
                        }
                    },
                    Err(e) => {
                        debug!(target: "Hotstuff", "~~ trigger_qc_mission from {from}. has error when consensus for block {} extrinsic {e:#?}", qc_proposal.payload.next_block.block_number);
                    }
                    _ => (),
                }
            }
            if !qc_proposal.payload.new_block.is_empty() {
                match self.synchronizer.get_proposal_ancestors(&qc_proposal, &|p| !p.payload.new_block.is_empty()) {
                    Ok(Some((parent, grandpa))) => {
                        if qc_proposal.view == parent.view + 1 && parent.view == grandpa.view + 1 {
                            if NewBlock::<B>::full_consensus(&qc_proposal.payload.new_block, &parent.payload.new_block, &grandpa.payload.new_block) {
                                let client_info = self.client.info();
                                if qc_proposal.payload.new_block.block_number > client_info.finalized_number
                                    && qc_proposal.payload.new_block.block_hash != client_info.finalized_hash
                                    && qc_proposal.payload.new_block.block_number <= client_info.best_number {
                                    info!(
                                        target: "Hotstuff",
                                        "^^_^^. block {:?}:{:?} can finalize with full proposal: [{}, {}, {}]",
                                        qc_proposal.payload.new_block.block_number,
                                        qc_proposal.payload.new_block.block_hash,
                                        qc.proposal_hash,
                                        qc_proposal.parent_hash(),
                                        parent.parent_hash(),
                                    );
                                    self.client
                                        .finalize_block(qc_proposal.payload.new_block.block_hash, None, true)
                                        .map_err(|e| FinalizeBlock(e.to_string()))?;
                                    self.processing_finalize_block = None;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!(target: "Hotstuff", "~~ trigger_qc_mission. has error when finalize block {} {e:#?}", qc_proposal.payload.new_block.block_number);
                    }
                    _ => (),
                }
            }
        }
        Ok(())
    }

    pub async fn handle_tc(&mut self, tc: TC<B>, local: bool) -> Result<(), HotstuffError> {
        let from = if local { "local" } else { "network" };
        debug!(target: "Hotstuff","~~ handle_tc from {from}. self.view {}, tc.view {}",self.state.view(), tc.view);
        if !local {
            self.state.verify_tc(&tc)?;
            tc.high_qc.verify(&self.state.authorities)?;
        }
        self.handle_qc(&tc.high_qc);
        if tc.view >= self.state.view() {
            self.advance_view(tc.view);
            self.local_timer.reset();
        }
        self.processing_block = None;
        self.processing_finalize_block = None;

        if self.state.is_leader() {
            debug!(
                target: "Hotstuff",
                "~~ handle_tc from {from}. leader propose. self.view {}, TC.view {}",
                self.state.view(),
                tc.view,
            );
            self.generate_proposal(Some(tc)).await?;
        }
        Ok(())
    }

    pub async fn generate_proposal(&mut self, tc: Option<TC<B>>) -> Result<(), HotstuffError> {
        let proposal_start = Timestamp::current();
        match self.get_proposal_payload().await {
            Some(payload) => {
                if payload.new_block.is_empty() && payload.next_block.is_empty() {
                    debug!(target:"Hotstuff", "^^ invalid proposal, this not gossip");
                    return Ok(());
                }
                let proposal = self.state.make_proposal(proposal_start, payload, tc)?;
                debug!(target: "Hotstuff","~~ generate_proposal. view: {}, tc: {}, payload: {:?}",
                    proposal.view,
                    proposal.tc.is_some(),
                    proposal.payload,
                );
                let proposal_message = ConsensusMessage::Propose(proposal.clone());
                self.network.gossip_engine.lock().gossip_message(
                    ConsensusMessage::<B>::gossip_topic(),
                    proposal_message.encode(),
                    false,
                );

                if let Err(e) = self.consensus_msg_tx.send((true, ConsensusMessage::Propose(proposal))).await {
                    debug!(target: "Hotstuff","~~ generate_proposal. Can't inform self of `Propose` for {e:?}.");
                }
            }
            None => {
                debug!(target: "Hotstuff","~~ generate_proposal. can't get next proposal.")
            }
        }

        Ok(())
    }

    async fn get_proposal_payload(&mut self) -> Option<Payload<B>> {
        // propose for new block to execute
        let mut next_block = None;
        let best = self.client.info().best_number;
        let best_next = best.saturating_add(1u32.into());
        if let Some(processing) = self.processing_block.as_ref() {
            if processing.block_number < best_next {
                let (get_next_block, time) = self.get_next_block(best_next, None).await;
                trace!(target: "Hotstuff", "~~ get_proposal_payload(next). processing: {}, best: {best}, new: {best_next} in {time} micros", processing.block_number);
                next_block = Some(get_next_block);
            } else {
                match processing.stage.next() {
                    Some(next_stage) => {
                        trace!(target: "Hotstuff", "~~ get_proposal_payload(next). processing: {}, best: {best}", processing.block_number);
                        next_block = Some(NextBlock {
                            block_number: processing.block_number,
                            hash: processing.hash,
                            extrinsic: None,
                            stage: next_stage,
                        });
                    },
                    None => {
                        // Restart to Prepare -> PreCommit -> Commit process. We may generate new block extrinsic here.
                        let (get_next_block, time) = self.get_next_block(processing.block_number, None).await;
                        trace!(target: "Hotstuff", "~~ get_proposal_payload(next). re-processing: {}, best: {best} in {time} micros", processing.block_number);
                        next_block = Some(get_next_block);
                    }
                }
            }
        } else {
            // We consensus for no more than best_block + 2
            if self.processed.block_number < best_next.saturating_add(1u32.into()) {
                let block_number = best_next.max(self.processed.block_number.saturating_add(1u32.into()));
                let mut except_extrinsic = None;
                if self.processed.block_number > best {
                    // this processed block extrinsic are not executed, should exclude it for
                    except_extrinsic = self.processed.extrinsic.clone();
                }
                let (get_next_block, time) = self.get_next_block(block_number, except_extrinsic).await;
                trace!(target: "Hotstuff", "~~ get_proposal_payload(next). best: {best}, processed: {}, new: {block_number} in {time} micros", self.processed.block_number);
                next_block = Some(get_next_block);
            }
        }

        // propose for new block to finalize
        let mut new_block = None;
        if let Ok(queue) = self.pending_finalize_queue.lock() {
            if let Some(processing) = self.processing_finalize_block.as_ref() {
                let finalized_number = self.client.info().finalized_number;
                if processing.block_number > finalized_number {
                    match processing.stage.next() {
                        Some(next_stage) => {
                            trace!(target: "Hotstuff", "~~ get_proposal_payload(finalize). processing: {}, finalized: {finalized_number}", processing.block_number);
                            new_block = Some(NewBlock {
                                block_hash: processing.block_hash,
                                block_number: processing.block_number,
                                stage: next_stage,
                            });
                        },
                        None => {
                            // Restart to Prepare -> PreCommit -> Commit process for finalize.
                            trace!(target: "Hotstuff", "~~ get_proposal_payload(finalize). re-processing: {}, finalized: {finalized_number}", processing.block_number);
                            new_block = Some(NewBlock {
                                block_hash: processing.block_hash,
                                block_number: processing.block_number,
                                stage: ConsensusStage::Prepare,
                            });
                        }
                    }
                } else if let Some(find) = queue.iter().find(|elem| elem.number > finalized_number) {
                    trace!(target: "Hotstuff", "~~ get_proposal_payload(finalize). get from queue. processing: {}, finalized: {finalized_number}, find {}",
                        processing.block_number,
                        find.number,
                    );
                    new_block = Some(NewBlock {
                        block_hash: find.hash.unwrap_or(Self::empty_payload_hash()),
                        block_number: find.number,
                        stage: ConsensusStage::Prepare,
                    });
                }
            } else if let Some(info) = queue.front().cloned() {
                debug!(target: "Hotstuff", "Q-Q get_proposal_payload(finalize). processing is None,get from queue {}", info.number);
                new_block = Some(NewBlock {
                    block_hash: info.hash.unwrap_or(Self::empty_payload_hash()),
                    block_number: info.number,
                    stage: ConsensusStage::Prepare,
                });
            }
        }

        if next_block.is_none() && new_block.is_none() {
            return None;
        }
        Some(Payload {
            new_block: new_block.unwrap_or(NewBlock::empty()),
            next_block: next_block.unwrap_or(NextBlock::empty()),
        })
    }

    fn advance_view(&mut self, view: ViewNumber) {
        self.state.advance_view_from_target(view);
        self.network.set_view(self.state.view());
    }

    pub(crate) fn empty_payload_hash() -> B::Hash {
        <<B::Header as HeaderT>::Hashing as HashT>::hash(EMPTY_PAYLOAD)
    }

    async fn get_next_block(
        &self,
        block_number: <B::Header as HeaderT>::Number,
        exclude: Option<(Vec<Vec<B::Extrinsic>>, Vec<B::Extrinsic>)>,
    ) -> (NextBlock<B>, u128) {
        let start = std::time::Instant::now();
        self.client.info().best_hash;
        let parent_header = self.client.header( self.client.info().best_hash).unwrap().unwrap();
        let proposer = self.proposer_factory.write().await.init(&parent_header).await.unwrap();
        let except = exclude
            .map(|(groups, single)| [groups.into_iter().flatten().collect(), single].concat())
            .unwrap_or_default();
        let local_timer_period = self.local_timer.period();
        let mut extrinsic = StepBlockPropose::<B>::extrinsic(
            &proposer,
            // time wait for pool response.
            local_timer_period.checked_div(10).unwrap(),
            // execute time limit to estimate extrinsic number. We currently set `2 * slot_duration`. `slot_duration` is min_block_time.
            std::time::Instant::now().add(std::time::Duration::from_millis(self.slot_duration.as_millis().saturating_mul(2))),
            // TODO SHOULD add block size limit
            None,
            except,
        ).await;
        // sort by extrinsic length ascending order for merge.
        extrinsic.0.sort_by(|a, b| a.len().cmp(&b.len()));
        (
            NextBlock {
                block_number,
                hash: <<B::Header as HeaderT>::Hashing as HashT>::hash_of(&extrinsic.encode()),
                extrinsic: Some(extrinsic),
                stage: ConsensusStage::Prepare,
            },
            start.elapsed().as_micros(),
        )
    }

    fn check_payload(&self, payload: &Payload<B>) -> Result<(), HotstuffError> {
        let info = self.client.info();
        if !payload.next_block.is_empty() {
            // if we meet processed next_block. we can re-process it.
            if info.finalized_number >= payload.next_block.block_number {
                return Err(PayloadError::BlockRollBack.into());
            }
            if let Some((groups, single)) = payload.next_block.extrinsic.clone() {
                let extrinsic = [groups.into_iter().flatten().collect(), single].concat();
                let extrinsic = extrinsic.into_iter().map(|e| (TransactionSource::External, e)).collect();
                if self.client
                    .runtime_api()
                    .validate_transactions(info.best_hash, extrinsic, info.best_hash)
                    .map_err(|e| HotstuffError::ClientError(e.to_string()))?
                    .into_iter()
                    .any(|r| if let Err(e) = r {
                        !(e.exhausted_resources() || e.future())
                    } else {
                        true
                    })
                {
                    // some extrinsic check filed(error but not exhausted_resources or future).
                    return Err(PayloadError::ExtrinsicErr(format!("validate_transactions meet err: {e:?}")).into());
                }
            }
        }
        if !payload.new_block.is_empty() {
            if info.finalized_number >= payload.new_block.block_number {
                return Err(PayloadError::BlockRollBack.into());
            }
            match self.client.status(payload.new_block.block_hash) {
                Ok(block_status) => {
                    if BlockStatus::Unknown == block_status {
                        // skip for no new_block in proposal is not in local.
                        return Err(PayloadError::UnknownBlock.into());
                    }
                }
                Err(e) => return Err(ClientError(e.to_string())),
            }
        }
        Ok(())
    }
}

pub struct ConsensusNetwork<
    B: BlockT,
    N: NetworkT<B> + Sync + 'static,
    S: SyncingT<B> + Sync + 'static,
> {
    network: HotstuffNetworkBridge<B, N, S>,
    message_recv: Recv<TopicNotification>,
    consensus_msg_tx: Sender<(bool, ConsensusMessage<B>)>,
    pending_queue: PendingFinalizeBlockQueue<B>,
}

impl<B, N, S> Future for ConsensusNetwork<B, N, S>
where
    B: BlockT,
    N: NetworkT<B> + Sync + 'static,
    S: SyncingT<B> + Sync + 'static,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match StreamExt::poll_next_unpin(&mut self.message_recv, cx) {
                Poll::Ready(None) => break,
                Poll::Ready(Some(notification)) => {
                    if let Err(e) = self.incoming_message_handler(notification) {
                        error!("process incoming message error: {:#?}", e)
                    }
                }
                Poll::Pending => break,
            };
        }

        match Future::poll(Pin::new(&mut self.pending_queue), cx) {
            Poll::Ready(notification) => {
                let block_info = BlockInfo {
                    hash: Some(notification.hash),
                    number: notification.header.number().clone(),
                };
                if let Err(e) = self
                    .consensus_msg_tx
                    .try_send((false, ConsensusMessage::BlockImport(block_info)))
                {
                    error!("process incoming block error: {:#?}", e)
                }
            }
            Poll::Pending => {}
        };

        match Future::poll(Pin::new(&mut self.network), cx) {
            Poll::Ready(_) => {}
            Poll::Pending => {}
        };

        Poll::Pending
    }
}

impl<B, N, S> ConsensusNetwork<B, N, S>
where
    B: BlockT,
    N: NetworkT<B> + Sync + 'static,
    S: SyncingT<B> + Sync + 'static,
{
    fn new(
        network: HotstuffNetworkBridge<B, N, S>,
        consensus_msg_tx: Sender<(bool, ConsensusMessage<B>)>,
        pending_queue: PendingFinalizeBlockQueue<B>,
    ) -> Self {
        let message_recv = network
            .gossip_engine
            .clone()
            .lock()
            .messages_for(ConsensusMessage::<B>::gossip_topic());

        Self {
            network,
            consensus_msg_tx,
            message_recv,
            pending_queue,
        }
    }

    pub fn incoming_message_handler(
        &mut self,
        notification: TopicNotification,
    ) -> Result<(), HotstuffError> {
        let message: ConsensusMessage<B> =
            Decode::decode(&mut &notification.message[..]).map_err(|e| Other(e.to_string()))?;

        self.consensus_msg_tx
            .try_send((false, message))
            .map_err(|e| Other(e.to_string()))
    }
}

impl<B, BE, C, N, S, PF, Error> Unpin for ConsensusWorker<B, BE, C, N, S, PF, Error>
where
    B: BlockT,
    BE: Backend<B>,
    C: ClientForHotstuff<B, BE>,
    N: NetworkT<B> + Sync + 'static,
    S: SyncingT<B> + Sync + 'static,
    PF: Environment<B, Error = Error> + Send + Sync + 'static,
    PF::Proposer: Proposer<B, Error = Error, Transaction = sp_api::TransactionFor<C, B>> + StepBlockPropose<B>,
    Error: std::error::Error + Send + From<ConsensusError> + 'static,
{
}

pub struct BlockAuthorship<C: ClientForHotstuff<B, BE>, B: BlockT, I, PF, L: sc_consensus::JustificationSyncLink<B>, BE: Backend<B>, CIDP, SC> {
    pub client: Arc<C>,
    pub proposer_factory: Arc<RwLock<PF>>,
    pub import: I,
    pub justification_sync_link: L,
    pub create_inherent_data_providers: CIDP,
    pub select_chain: SC,
    pub last_slot: Slot,
    pub executor_rx: UnboundedReceiver<(Timestamp, [QC<B>; 3], NextBlock<B>)>,
    pub mission_queue: Arc<Mutex<VecDeque<(Timestamp, [QC<B>; 3], NextBlock<B>)>>>,
    pub working: Arc<Mutex<Option<<B::Header as HeaderT>::Number>>>,
    pub block_size_limit: Option<usize>,
    phantom_data: PhantomData<BE>,
}

impl<C, B: BlockT, I, PF, L, BE: Backend<B>, Error, CIDP, SC> BlockAuthorship<C, B, I, PF, L, BE, CIDP, SC>
where
    C: ClientForHotstuff<B, BE> + Send + Sync + 'static,
    I: BlockImport<B, Transaction = TransactionFor<C, B>> + Send + Sync + 'static,
    PF: Environment<B, Error = Error> + Send + Sync + 'static,
    PF::Proposer: Proposer<B, Error = Error, Transaction = sp_api::TransactionFor<C, B>> + StepBlockPropose<B>,
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
        executor_rx: UnboundedReceiver<(Timestamp, [QC<B>; 3], NextBlock<B>)>,
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
            mission_queue: Arc::new(Mutex::new(VecDeque::new())),
            working: Arc::new(Mutex::new(None)),
            block_size_limit: None,
            phantom_data: PhantomData,
        }
    }

    pub async fn block_import_params(
        &self,
        header: B::Header,
        body: Vec<B::Extrinsic>,
        storage_changes: StorageChanges<TransactionFor<C, B>, B>,
        full_qc: [QC<B>; 3],
    ) -> Result<BlockImportParams<B, TransactionFor<C, B>>, ConsensusError> {
        let mut import_block = BlockImportParams::new(BlockOrigin::ConsensusBroadcast, header);
        let digest_item = <DigestItem as CompatibleDigestItem<Vec<QC<B>>>>::hotstuff_seal(full_qc.to_vec());
        import_block.post_digests.push(digest_item);
        import_block.body = Some(body);
        import_block.state_action =
            StateAction::ApplyChanges(sc_consensus::StorageChanges::Changes(storage_changes));
        import_block.fork_choice = Some(ForkChoiceStrategy::LongestChain);

        Ok(import_block)
    }
}

pub async fn start_hotstuff_block_authorship<C, B: BlockT, I, PF, L, BE: Backend<B>, Error, CIDP, SC>(
    mut block_authorship: BlockAuthorship<C, B, I, PF, L, BE, CIDP, SC>,
)
where
    C: ClientForHotstuff<B, BE>+ Send + Sync + 'static,
    I: BlockImport<B, Transaction = TransactionFor<C, B>> + Send + Sync + 'static,
    PF: Environment<B, Error = Error> + Send + Sync + 'static,
    PF::Proposer: Proposer<B, Error = Error, Transaction = TransactionFor<C, B>> + StepBlockPropose<B>,
    L: sc_consensus::JustificationSyncLink<B>,
    Error: std::error::Error + Send + From<ConsensusError> + 'static,
    CIDP: CreateInherentDataProviders<B, Timestamp> + Send + 'static,
    CIDP::InherentDataProviders: InherentDataProviderExt + Send,
    SC: SelectChain<B>,
{
    // TODO SHOULD add restart recover logic to execute unexecuted block.
    loop {
        if let Some((timestamp, full_qc, next_block)) = block_authorship.executor_rx.recv().await {
            // push next_block to mission_queue.
            block_authorship.mission_queue.lock().unwrap().push_back((timestamp, full_qc, next_block));
            // try handle all next_blocks for mission_queue.
            loop {
                let next_mission = block_authorship.mission_queue.lock().unwrap().pop_front();
                if let Some((timestamp, full_qc, mut next_block)) = next_mission {
                    let extrinsic = next_block.extrinsic.take();
                    if extrinsic.is_none() {
                        warn!(target: "Hotstuff", "[Execute] Next block number: {} skipped for None extrinsic", next_block.block_number);
                        continue;
                    }
                    let chain_head = match block_authorship.select_chain.best_chain().await {
                        Ok(x) => x,
                        Err(e) => {
                            warn!(target: "Hotstuff", "[Execute] Unable to author block in slot. No best block header: {e}");
                            continue
                        },
                    };
                    let best_block = chain_head.number().clone();
                    let best_hash = chain_head.hash();
                    if best_block.saturating_add(1u32.into()) != next_block.block_number {
                        warn!(target: "Hotstuff", "[Execute] Best block number: {best_block}, next block number: {} skipped", next_block.block_number);
                        continue;
                    }
                    block_authorship.working.lock().unwrap().replace(next_block.block_number.clone());
                    // execute block
                    // creat inherent data
                    let inherent_data_providers = match block_authorship
                        .create_inherent_data_providers
                        .create_inherent_data_providers(chain_head.hash(), timestamp)
                        .await
                    {
                        Ok(x) => x,
                        Err(e) => {
                            warn!(target: "Hotstuff", "[Execute] Unable to author block in slot. Failure creating inherent data provider: {e}");
                            continue
                        },
                    };
                    debug!(target: "Hotstuff", "[Execute] Best block number: {best_block}, next block number: {} start execute", next_block.block_number);
                    let slot = inherent_data_providers.slot();// Never yield the same slot twice.
                    if slot > block_authorship.last_slot {
                        block_authorship.last_slot = slot;
                    } else {
                        warn!(target: "Hotstuff", "[Execute] Best block number: {best_block}, next block number: {} skipped for last_slot {}, current_slot {slot}", next_block.block_number, block_authorship.last_slot);
                        continue;
                    }
                    let inherent_data = inherent_data_providers.create_inherent_data().await.unwrap();

                    let logs = vec![<DigestItem as CompatibleDigestItem<Slot>>::hotstuff_pre_digest(slot)];
                    let parent_header = block_authorship
                        .client
                        .header(best_hash)
                        .unwrap()
                        .unwrap();
                    let proposer = block_authorship.proposer_factory.write().await.init(&parent_header).await.unwrap();
                    let proposal = match StepBlockPropose::<B>::step_propose(
                        proposer,
                        block_authorship.block_size_limit,
                        inherent_data,
                        Digest { logs },
                        extrinsic.unwrap(),
                        true,
                    ).await {
                        Ok(propose) => propose,
                        Err(e) => {
                            warn!(target: "Hotstuff", "[Execute] Propose block {} failed for {e:?}", next_block.block_number);
                            block_authorship.working.lock().unwrap().take();
                            continue;
                        }
                    };
                    // generate import params
                    let (block, _storage_proof) = (proposal.block, proposal.proof);
                    let (header, body) = block.deconstruct();
                    let block_import_params = match block_authorship.block_import_params(
                        header,
                        body.clone(),
                        proposal.storage_changes,
                        full_qc,
                    ).await {
                        Ok(import_params) => import_params,
                        Err(e) => {
                            warn!(target: "Hotstuff", "[Execute] Propose block {} get block_import_params failed for {e:?}", next_block.block_number);
                            continue;
                        }
                    };
                    // import block
                    let header = block_import_params.post_header();
                    match block_authorship.import.import_block(block_import_params).await {
                        Ok(res) => {
                            res.handle_justification(
                                &header.hash(),
                                *header.number(),
                                &block_authorship.justification_sync_link,
                            );
                        },
                        Err(err) => {
                            warn!(target: "Hotstuff", "[Execute] Error with block {} built on {best_hash:?}: {err}", next_block.block_number);
                        },
                    }

                    block_authorship.working.lock().unwrap().take();
                } else {
                    break;
                }
            }
        }
    }
}

pub fn start_hotstuff<B, BE, C, N, S, SC, PF, Error, L, I, CIDP>(
    network: N,
    link: LinkHalf<B, C, SC>,
    sync: S,
    import: I,
    justification_sync_link: L,
    proposer_factory: PF,
    hotstuff_protocol_name: ProtocolName,
    keystore: KeystorePtr,
    create_inherent_data_providers: CIDP,
    select_chain: SC,
    slot_duration: SlotDuration,
) -> sp_blockchain::Result<(
    impl Future<Output = ()> + Send,
    impl Future<Output = ()> + Send,
    impl Future<Output = ()> + Send,
)>
where
    B: BlockT,
    BE: Backend<B> + 'static,
    N: NetworkT<B> + Sync + 'static,
    S: SyncingT<B> + Sync + 'static,
    C: ClientForHotstuff<B, BE> + Send + Sync + 'static,
    C::Api: hotstuff_primitives::HotstuffApi<B, AuthorityId>,
    PF: Environment<B, Error = Error> + Send + Sync + 'static,
    PF::Proposer: Proposer<B, Error = Error, Transaction = TransactionFor<C, B>> + StepBlockPropose<B>,
    L: sc_consensus::JustificationSyncLink<B>,
    I: BlockImport<B, Transaction = TransactionFor<C, B>> + Send + Sync + 'static,
    Error: std::error::Error + Send + From<ConsensusError> + 'static,
    CIDP: CreateInherentDataProviders<B, Timestamp> + Send + 'static,
    CIDP::InherentDataProviders: InherentDataProviderExt + Send,
    SC: SelectChain<B>,
{
    let LinkHalf { client, .. } = link;
    let authorities = get_genesis_authorities_from_client::<B, BE, C>(client.clone());

    let network = HotstuffNetworkBridge::new(network.clone(), sync.clone(), hotstuff_protocol_name);
    let synchronizer = Synchronizer::<B, BE, C>::new(client.clone());
    let consensus_state = ConsensusState::<B>::new(keystore, authorities);

    let (consensus_msg_tx, consensus_msg_rx) = channel(1000);

    let queue = PendingFinalizeBlockQueue::<B>::new(client.clone()).expect("error");

    let mut local_timer_duration = 100;
    if let Ok(value) = env::var("HOTSTUFF_DURATION") {
        if let Ok(duration) = value.parse::<u64>() {
            local_timer_duration = duration;
        }
    }
    let proposer_factory = Arc::new(RwLock::new(proposer_factory));
    let (executor_tx, executor_rx) = unbounded_channel();
    let consensus_worker = ConsensusWorker::<B, BE, C, N, S, PF, Error>::new(
        consensus_state,
        client.clone(),
        sync,
        network.clone(),
        synchronizer,
        proposer_factory.clone(),
        local_timer_duration,
        slot_duration,
        consensus_msg_tx.clone(),
        consensus_msg_rx,
        executor_tx,
        queue.queue(),
    );

    let consensus_network = ConsensusNetwork::<B, N, S>::new(network, consensus_msg_tx, queue);
    let block_authorship = BlockAuthorship::new(client, import, proposer_factory, justification_sync_link, create_inherent_data_providers, select_chain, executor_rx);
    let block_authorship_task = start_hotstuff_block_authorship(block_authorship);
    Ok((async { consensus_worker.run().await }, consensus_network, block_authorship_task))
}

pub fn get_genesis_authorities_from_client<
    B: BlockT,
    BE: Backend<B>,
    C: ClientForHotstuff<B, BE>,
>(
    client: Arc<C>,
) -> AuthorityList {
    let genesis_block_hash = client
        .expect_block_hash_from_id(&BlockId::Number(Zero::zero()))
        .expect("get genesis block hash from client failed");

    let authorities_data = client
        .executor()
        .call(
            genesis_block_hash,
            "HotstuffApi_authorities",
            &[],
            ExecutionStrategy::NativeElseWasm,
            CallContext::Offchain,
        )
        .expect("call runtime failed");

    let authorities: Vec<AuthorityId> = Decode::decode(&mut &authorities_data[..]).expect("");

    authorities
        .iter()
        .map(|id| (id.clone(), 0))
        .collect::<AuthorityList>()
}
