use std::{
    cmp::max,
    env,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use std::marker::PhantomData;
use std::str::FromStr;
use std::thread::JoinHandle;
use std::time::Duration;
use async_recursion::async_recursion;
use futures::{channel::mpsc::Receiver as Recv, Future, StreamExt};

use codec::{Decode, Encode};
use log::{debug, error, info, trace, warn};
use tokio::sync::mpsc::{channel, unbounded_channel, Receiver, Sender, UnboundedSender};
use tokio::sync::RwLock;
use frame_system::extrinsics_data_root;
use sc_basic_authorship::{BlockPropose, GroupTransaction, BlockOracle};
use sc_client_api::{Backend, CallExecutor, ExecutionStrategy};
use sc_network::types::ProtocolName;
use sc_network_gossip::TopicNotification;
use sp_application_crypto::AppCrypto;
use sp_core::{crypto::ByteArray, traits::CallContext};
use sp_keystore::KeystorePtr;
use sp_runtime::{generic::BlockId, traits::{Block as BlockT, Hash as HashT, Header as HeaderT}, Saturating};
use sc_consensus::BlockImport;
use sc_consensus_slots::InherentDataProviderExt;
use sp_consensus::SelectChain;

use crate::{
    aggregator::Aggregator,
    client::{ClientForHotstuff, LinkHalf}, find_consensus_logs, find_block_commit,
    executor::{BlockExecutor, ExecutorMission},
    import::{BlockInfo, PendingFinalizeBlockQueue, ImportLock},
    message::{
        ConsensusMessage, Proposal, Timeout, Vote, QC, TC, BlockCommit, ConsensusStage, Payload,
        PeerAuthority, ProposalKey, ProposalReq, ProposalRequest,
    },
    network::{HotstuffNetworkBridge, Network as NetworkT, Syncing as SyncingT},
    primitives::{HotstuffError, PayloadError, ViewNumber},
    synchronizer::{Synchronizer, Timer}, CLIENT_LOG_TARGET,
};
use hotstuff_primitives::{AuthorityId, AuthorityIndex, AuthorityList, AuthoritySignature, ConsensusLog, HotstuffApi, HOTSTUFF_KEY_TYPE};
use hotstuff_primitives::inherents::InherentType;
use sc_network::PeerId;
use sp_api::TransactionFor;
use sp_consensus::{Environment, Error as ConsensusError, Proposer};
use sp_consensus_slots::SlotDuration;
use sp_inherents::CreateInherentDataProviders;
use sp_runtime::traits::Zero;
use sp_runtime::transaction_validity::TransactionSource;
use sp_timestamp::Timestamp;
use crate::oracle::HotsOracle;

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

pub(crate) const EMPTY_PAYLOAD: &[u8] = b"hotstuff/empty_payload";

// the core of hotstuff
pub struct ConsensusState<B: BlockT> {
    keystore: KeystorePtr,
    authorities: AuthorityList,
    disabled_authorities: Vec<AuthorityIndex>,
    local_authority_id: Option<AuthorityId>,
    // local view number
    view: ViewNumber,
    last_voted_view: ViewNumber,
    last_proposed_view: ViewNumber,
    // last_committed_round: ViewNumber,
    high_qc: QC<B>,
    aggregator: Aggregator<B>,
}

impl<B: BlockT> ConsensusState<B> {
    pub fn new(
        keystore: KeystorePtr,
        high_proposal: Option<Proposal<B>>,
        authorities: AuthorityList,
        disabled_authorities: Vec<AuthorityIndex>,
    ) -> Self {
        let mut state = Self {
            local_authority_id: authorities
                .iter()
                .find(|(p, _)| {
                    keystore
                        .has_keys(&[(p.to_raw_vec(), HOTSTUFF_KEY_TYPE)])
                })
                .map(|(p, _)| p.clone()),
            keystore,
            authorities,
            disabled_authorities,
            view: 0,
            last_voted_view: 0,
            last_proposed_view: 0,
            high_qc: Default::default(),
            aggregator: Aggregator::<B>::new(),
        };
        if let Some(high_proposal) = high_proposal {
            state.view = high_proposal.view.saturating_add(1);
            state.last_voted_view = high_proposal.view;
            state.high_qc = high_proposal.qc;
        }
        state
    }

    pub fn change_authorities(&mut self, _block: <B::Header as HeaderT>::Number, authorities: AuthorityList) {
        self.local_authority_id = authorities
            .iter()
            .find(|(p, _)| {
                self.keystore
                    .has_keys(&[(p.to_raw_vec(), HOTSTUFF_KEY_TYPE)])
            })
            .map(|(p, _)| p.clone());
        self.authorities = authorities;
        self.disabled_authorities.clear();
    }

    pub fn disable_authority(&mut self, _block: <B::Header as HeaderT>::Number, _index: AuthorityIndex) {
        // TODO not in use. If need: 1. storage on pallet_hotstuff. 2. Initialize on start. 3. Update
    }

    pub fn increase_last_voted_view(&mut self) {
        self.last_voted_view = max(self.last_voted_view, self.view)
    }

    pub fn increase_last_proposed_view(&mut self) {
        self.last_proposed_view = max(self.last_proposed_view, self.view)
    }

    pub fn is_authority(&self) -> bool {
        if let Some(authority) = self.local_authority_id.as_ref() {
            return self.authorities.iter().any(|(p, _)| p == authority);
        }
        false
    }

    pub fn make_timeout(&self) -> Result<Option<Timeout<B>>, HotstuffError> {
        if !self.is_authority() { return Ok(None); }
        let authority_id = self.local_authority_id.as_ref().ok_or(HotstuffError::NotAuthority)?;

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
            .map_err(|e| HotstuffError::Other(e.to_string()))?
            .and_then(|data| AuthoritySignature::try_from(data).ok());

        Ok(Some(tc))
    }

    pub fn make_proposal(
        &self,
        payload: Payload<B>,
        tc: Option<TC<B>>,
    ) -> Result<Option<Proposal<B>>, HotstuffError> {
        if !self.is_authority() { return Ok(None); }
        let author_id = self.local_authority_id.as_ref().ok_or(HotstuffError::NotAuthority)?;
        let mut block = Proposal::<B>::new(
            self.high_qc.clone(),
            tc,
            payload,
            self.view,
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
            .map_err(|e| HotstuffError::Other(e.to_string()))?
            .and_then(|data| AuthoritySignature::try_from(data).ok());

        Ok(Some(block))
    }

    pub fn make_vote(&mut self, proposal: &Proposal<B>) -> Option<Vote<B>> {
        if !self.is_authority() {
            trace!(target: CLIENT_LOG_TARGET, "make_vote proposal view {}, skip vote for not authority!!!", proposal.view);
            return None;
        }
        let author_id = self.local_authority_id.as_ref()?;

        if proposal.view <= self.last_voted_view {
            trace!(target: CLIENT_LOG_TARGET, "make_vote proposal view {}, last_voted_view {}, skip vote for proposal!!!", proposal.view, self.last_voted_view);
            return None;
        }

        self.last_voted_view = max(self.last_voted_view, proposal.view);

        let mut vote = Vote::<B>::new(proposal.digest(), proposal.view, proposal.payload.stage, author_id.clone());

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

    pub fn make_peer_authority(&self, peer_id: String) -> Option<PeerAuthority<B>> {
        if self.local_authority_id.is_none() { return None; }
        let author_id = self.local_authority_id.as_ref()?;
        let mut peer_authority = PeerAuthority::<B> {
            peer_id,
            authority: author_id.clone(),
            signature: None,
            phantom: PhantomData,
        };

        peer_authority.signature = self
            .keystore
            .sign_with(
                AuthorityId::ID,
                AuthorityId::CRYPTO_ID,
                author_id.as_slice(),
                peer_authority.digest().as_ref(),
            )
            .ok()?
            .and_then(|data| AuthoritySignature::try_from(data).ok());

        Some(peer_authority)
    }

    pub fn make_proposal_request(&self, requests: ProposalReq<B>) -> Option<ProposalRequest<B>> {
        // if not authority, can't request proposal.
        if !self.is_authority() { return None; }
        let author_id = self.local_authority_id.as_ref()?;
        let mut request = ProposalRequest {
            authority: author_id.clone(),
            requests,
            signature: None,
        };

        request.signature = self
            .keystore
            .sign_with(
                AuthorityId::ID,
                AuthorityId::CRYPTO_ID,
                author_id.as_slice(),
                request.digest().as_ref(),
            )
            .ok()?
            .and_then(|data| AuthoritySignature::try_from(data).ok());

        Some(request)
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
                return Err(HotstuffError::WrongProposer);
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
            return Err(HotstuffError::ExpiredVote);
        }

        vote.verify(&self.authorities)
    }

    pub fn verify_tc(&self, tc: &TC<B>) -> Result<(), HotstuffError> {
        if tc.view < self.view {
            return Err(HotstuffError::InvalidTC);
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

    pub fn is_leader_for(&self, view: ViewNumber) -> bool {
        match self.local_authority_id.as_ref() {
            Some(id) => {
                match self.view_leader(view) {
                    Some(leader) => id == leader,
                    None => false,
                }
            }
            None => false,
        }
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader_for(self.view)
    }

    pub fn is_next_leader(&self) -> bool {
        self.is_leader_for(self.view + 1)
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
    O: BlockOracle<B> + HotsOracle + Sync + Send + 'static,
> {
    state: ConsensusState<B>,

    network: HotstuffNetworkBridge<B, N, S>,
    client: Arc<C>,
    _sync: S,
    local_timer: Timer,
    slot_duration: SlotDuration,
    blocks_ahead_best: <B::Header as HeaderT>::Number,
    synchronizer: Synchronizer<B, C>,
    consensus_msg_tx: Sender<(bool, ConsensusMessage<B>)>,
    consensus_msg_rx: Receiver<(bool, ConsensusMessage<B>)>,
    executor_tx: UnboundedSender<ExecutorMission<B>>,
    proposer_factory: Arc<RwLock<PF>>,
    oracle: Arc<O>,
    /// Tmp cache for not voted proposal(should be verified and qc mission is handled).
    pending_proposal: Option<Proposal<B>>,
    processing: Option<Payload<B>>,
    processed: Payload<B>,
    /// consensus success block waiting to execute.
    commit: BlockCommit<B>,
    processed_extrinsic: Option<(Vec<Vec<B::Extrinsic>>, Vec<B::Extrinsic>)>,
    phantom: PhantomData<BE>,
}

impl<B, BE, C, N, S, PF, Error, O> ConsensusWorker<B, BE, C, N, S, PF, Error, O>
where
    B: BlockT,
    BE: Backend<B>,
    C: ClientForHotstuff<B, BE> + 'static,
    C::Api: HotstuffApi<B, AuthorityId>,
    N: NetworkT<B> + Sync + 'static,
    S: SyncingT<B> + Sync + 'static,
    PF: Environment<B, Error = Error> + Send + Sync + 'static,
    PF::Proposer: Proposer<B, Error = Error, Transaction = sp_api::TransactionFor<C, B>> + BlockPropose<B> + GroupTransaction<B>,
    Error: std::error::Error + Send + From<ConsensusError> + 'static,
    O: BlockOracle<B> + HotsOracle + Sync + Send + 'static,
{
    #![allow(clippy::too_many_arguments)]
    pub fn new(
        consensus_state: ConsensusState<B>,
        client: Arc<C>,
        sync: S,
        network: HotstuffNetworkBridge<B, N, S>,
        synchronizer: Synchronizer<B, C>,
        proposer_factory: Arc<RwLock<PF>>,
        local_timer_duration: u64,
        slot_duration: SlotDuration,
        oracle: Arc<O>,
        blocks_ahead_best: <B::Header as HeaderT>::Number,
        consensus_msg_tx: Sender<(bool, ConsensusMessage<B>)>,
        consensus_msg_rx: Receiver<(bool, ConsensusMessage<B>)>,
        executor_tx: UnboundedSender<ExecutorMission<B>>,
    ) -> Self {
        if local_timer_duration < slot_duration.as_millis() {
            let slot_duration_millis = slot_duration.as_millis();
            warn!(
                target: CLIENT_LOG_TARGET,
                "HOTSTUFF_DURATION({local_timer_duration}) smaller than slot_duration({slot_duration_millis})!!!"
            );
        }
        if blocks_ahead_best > 2u32.into() {
            panic!("HOTSTUFF_BLOCKS_AHEAD({blocks_ahead_best}) should only be 1 or 2!!! Please restart with correct environment!!!");
        }
        let best_header = client.header(client.info().best_hash).unwrap().expect("Best header should be present");
        let commit = if !best_header.number().is_zero() {
            find_block_commit::<B>(&best_header).expect("Best block should have BlockCommit!!!")
        } else {
            BlockCommit::empty()
        };
        info!(target: CLIENT_LOG_TARGET, "Start consensus worker with local_timer_duration: {local_timer_duration} millis, slot_duration: {} millis", slot_duration.as_millis());
        let mut processed = Payload::empty();
        processed.stage = ConsensusStage::Commit;
        Self {
            state: consensus_state,
            network,
            local_timer: Timer::new(local_timer_duration),
            slot_duration,
            blocks_ahead_best,
            consensus_msg_tx,
            consensus_msg_rx,
            executor_tx,
            client,
            _sync: sync,
            synchronizer,
            proposer_factory,
            oracle,
            pending_proposal: None,
            processing: None,
            processed,
            commit,
            processed_extrinsic: None,
            phantom: PhantomData,
        }
    }

    // Try recover all unapplied commit.
    pub async fn recover(&mut self) {
        let latest = self.client.info().best_number;
        let mut view = self.synchronizer.high_view();
        let mut proposal_key = ProposalKey::digest(self.synchronizer.high_digest());
        if view == 0 {
            debug!(target: CLIENT_LOG_TARGET, "[Recover] Skip for high_view 0");
            return;
        }
        loop {
            match self.synchronizer.get_proposal(proposal_key.clone()) {
                Ok(Some(proposal)) => {
                    trace!(target: CLIENT_LOG_TARGET, "[Recover] ~~ handle proposal {}:{} qc {}:{}", proposal.view, proposal.digest(), proposal.qc.view, proposal.qc.proposal_hash);
                    if let Err(e) = self.trigger_qc_mission(&proposal.qc, true).await {
                        error!(target: CLIENT_LOG_TARGET, "[Recover] ~~ trigger_qc_mission {}:{} failed for {e:?}", proposal.qc.view, proposal.qc.proposal_hash);
                    }
                    if proposal.payload.block_number <= latest.saturating_add(1u32.into()) {
                        debug!(target: CLIENT_LOG_TARGET, "[Recover] Finish for proposal {}:{} payload block {} <= latest {latest} + 1", proposal.view, proposal.digest(), proposal.payload.block_number);
                        break;
                    }
                    view = proposal.qc.view;
                    proposal_key = ProposalKey::digest(proposal.qc.proposal_hash);
                },
                Ok(None) => {
                    debug!(target: CLIENT_LOG_TARGET, "[Recover] No proposal for view {}", self.synchronizer.high_view());
                    view -= 1;
                    proposal_key = ProposalKey::view(view);
                }
                Err(e) => {
                    error!(target: CLIENT_LOG_TARGET, "[Recover] Get high proposal {}:{} failed for {e:?}", self.synchronizer.high_view(), self.synchronizer.high_digest());
                    return;
                }
            };
        }
    }

    pub async fn run(mut self) {
        if let Some(ref id) = self.state.local_authority_id {
            info!(target: CLIENT_LOG_TARGET, "Local authority id is: {id}, start with view {}, high_qc {}", self.state.view, self.state.high_qc.view);
        } else {
            info!(target: CLIENT_LOG_TARGET, "Local is not authority, start with view {}, high_qc {}", self.state.view, self.state.high_qc.view);
        }
        self.recover().await;
        self.local_timer.reset();
        loop {
            let _ = tokio::select! {
                _ = &mut self.local_timer => if let Err(e) = self.handle_local_timer().await {
                    debug!(target: CLIENT_LOG_TARGET, "handle_local_timer has error {e:?}");
                },
                Some((local, message)) = self.consensus_msg_rx.recv()=> {
                    let from = if local { "local" } else { "network" };
                    match message {
                        ConsensusMessage::Greeting(authority_id) => {
                            if let Err(e) = self.handle_greeting(&authority_id) {
                                trace!(target: CLIENT_LOG_TARGET, "[handle_greeting({from})] has error {e:?}");
                            }
                        },
                        ConsensusMessage::GetProposal(request) => {
                            if let Err(e) = self.handle_proposal_request(&request).await {
                                trace!(target: CLIENT_LOG_TARGET, "[handle_proposal_request({from})] has error {e:?}");
                            }
                        },
                        ConsensusMessage::BlockImport(header) => {
                            if let Err(e) = self.handle_block_import(&header).await {
                                debug!(target: CLIENT_LOG_TARGET, "[handle_block_import({from})] has error {e:?}");
                            }
                        },
                        ConsensusMessage::Propose(proposal) => {
                            if let Err(e) = self.handle_proposal(&proposal, local).await{
                                debug!(target: CLIENT_LOG_TARGET, "[handle_proposal({from})] has error {e:?}");
                            }
                        },
                        ConsensusMessage::Vote(vote) => {
                            if let Err(e) = self.handle_vote(&vote, local).await{
                                trace!(target: CLIENT_LOG_TARGET, "[handle_vote({from})] has error {e:?}");
                            }
                        },
                        ConsensusMessage::Timeout(timeout) => {
                            if let Err(e) = self.handle_timeout(&timeout, local).await{
                                trace!(target: CLIENT_LOG_TARGET, "[handle_timeout({from})] has error {e:?}");
                            }
                        },
                        ConsensusMessage::TC(tc) => {
                            if let Err(e) = self.handle_tc(tc, local).await{
                                debug!(target: CLIENT_LOG_TARGET, "[handle_tc({from})] has error {e:?}");
                            }
                        },
                        _ => (),
                    }
                }
            };
        }
    }

    pub async fn handle_local_timer(&mut self) -> Result<(), HotstuffError> {
        debug!(target: CLIENT_LOG_TARGET, "$L$ handle_local_timer. self.view {}", self.state.view());

        self.local_timer.reset();
        self.pending_proposal = None;

        // current self.view will be timeout view.
        let timeout = match self.state.make_timeout()? {
            Some(timeout) => timeout,
            None => return Ok(()),
        };
        self.state.increase_last_voted_view();
        let message = ConsensusMessage::Timeout(timeout.clone());

        self.network.gossip_engine.lock().gossip_message(
            ConsensusMessage::<B>::gossip_topic(),
            message.encode(),
            true,
        );

        if let Err(e) = self.consensus_msg_tx.send((true, ConsensusMessage::Timeout(timeout))).await {
            warn!(target: CLIENT_LOG_TARGET, "$L$ handle_local_timer. Can't inform self `Timeout` for {e:?}.");
        }
        Ok(())
    }

    pub async fn handle_timeout(&mut self, timeout: &Timeout<B>, local: bool) -> Result<(), HotstuffError> {
        if self.state.view() > timeout.view {
            return Ok(());
        }
        let from = if local { "local" } else { "network" };
        trace!(
            target: CLIENT_LOG_TARGET,
            "~~ handle_timeout({from} self.view {}). view {}, high_qc.view {}, author {}",
            self.state.view(),
			timeout.view,
            timeout.high_qc.view,
            timeout.voter,
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
                warn!(target: CLIENT_LOG_TARGET, "~~ handle_timeout({from}). Can't inform self `TC` for {e:?}.");
            }
        }

        Ok(())
    }

    pub fn handle_greeting(&mut self, peer_authority: &PeerAuthority<B>) -> Result<(), HotstuffError> {
        peer_authority.verify()?;
        let peer_id = PeerId::from_str(&peer_authority.peer_id).map_err(|e| HotstuffError::Other(e.to_string()))?;
        debug!(target: CLIENT_LOG_TARGET, "~~ handle_greeting. Authority {} with PeerId {}.", peer_authority.authority, peer_authority.peer_id);
        self.network.authorities.insert(peer_authority.authority.clone(), Some(peer_id));
        Ok(())
    }

    pub async fn handle_proposal_request(&self, request: &ProposalRequest<B>) -> Result<(), HotstuffError> {
        // only reply to authorities
        request.verify(&self.state.authorities)?;
        let proposals = match &request.requests {
            ProposalReq::Keys(keys) => {
                let mut proposals = vec![];
                for key in keys.clone() {
                    if let Some(proposal) = self.synchronizer.get_proposal(key)? {
                        proposals.push(proposal);
                    }
                }
                proposals
            },
            ProposalReq::Range(from, to) => {
                self.synchronizer.get_proposals(from.clone(), to.clone())?
            }
        };
        trace!(
            target: CLIENT_LOG_TARGET,
            "~~ handle_proposal_request. Response {} proposals to {}({:?}).",
            proposals.len(),
            request.authority,
            proposals.iter().map(|p| format!("{}:{}", p.view, p.digest())).collect::<Vec<_>>(),
        );
        for proposal in proposals {
            self.network.send_to_authorities(Some(vec![request.authority.clone()]), ConsensusMessage::Propose(proposal).encode())?;
        }
        Ok(())
    }

    pub async fn handle_block_import(&mut self, header: &B::Header) -> Result<(), HotstuffError> {
        if let Some(commit) = find_block_commit::<B>(header) {
            let (deleted, deleted_invalid) = self.synchronizer.clear_all_before(ProposalKey::View(commit.prepare.qc.view))?;
            if !deleted.is_empty() || !deleted_invalid.is_empty() {
                trace!(target: CLIENT_LOG_TARGET, "~~ handle_block_import. Block {} imported, delete proposals {deleted:?}, invalid proposals: {deleted_invalid:?}", header.number());
            }
        }
        if self.client.info().best_number > *header.number() {
            return Ok(());
        }
        for consensus_log in find_consensus_logs::<B>(header) {
            match consensus_log {
                ConsensusLog::AuthoritiesChange(authorities) => {
                    // changes authorities list.
                    self.state
                        .change_authorities(*header.number(), authorities.into_iter().map(|a| (a, 0)).collect());
                    // since we allow pre consensus block, we accept even authorities change.
                },
                ConsensusLog::OnDisabled(index) => self.state.disable_authority(*header.number(), index),
            }
        }
        // try handle pending proposal.
        if let Some(pending_proposal) = self.pending_proposal.take() {
            self.vote_for_proposal(&pending_proposal, true, "pending").await?;
        }
        // if local is leader, try to generate propose.
        if self.state.is_leader() {
            self.generate_proposal(None).await?;
        }
        Ok(())
    }

    #[async_recursion]
    pub async fn handle_proposal(&mut self, proposal: &Proposal<B>, local: bool) -> Result<(), HotstuffError> {
        let from = if local { "local" } else { "network" };
        debug!(target: CLIENT_LOG_TARGET, "~~ handle_proposal({from} self.view {}). proposal[ view {}({}), tc {:?}, digest {},  payload {}, author {}]",
            self.state.view(),
            proposal.view,
            proposal.qc.view,
            proposal.tc.as_ref().map(|tc| format!("{}({})", tc.view, tc.high_qc.view)),
            proposal.digest(),
            proposal.payload,
            proposal.author,
        );
        if self.state.high_qc.higher_than(&proposal.qc) {
            trace!(
                target: CLIENT_LOG_TARGET,
                "skip proposal view {} low qc {}:{}, local high_qc {}:{}",
                proposal.view,
                proposal.qc.view,
                proposal.qc.stage,
                self.state.high_qc.view,
                self.state.high_qc.stage,
            );
            return Ok(())
        }
        if !local {
            self.state.verify_proposal(&proposal)?;
            proposal.qc.verify(&self.state.authorities)?;
        }
        self.synchronizer.save_proposal(&proposal)?;
        if !local {
            if let Some(tc) = proposal.tc.as_ref() {
                self.handle_qc(&tc.high_qc);
                if tc.view >= self.state.view() {
                    self.advance_view(tc.view);
                    self.processing = None;
                }
            }
            self.handle_qc(&proposal.qc);
            // if from local, the qc mission is triggered before this proposal generate.
            self.trigger_qc_mission(&proposal.qc, local).await?;
        }
        self.vote_for_proposal(proposal, !local, from).await?;
        Ok(())
    }

    async fn vote_for_proposal(&mut self, proposal: &Proposal<B>, check_payload: bool, from: &str) -> Result<(), HotstuffError> {
        if proposal.view != self.state.view() {
            trace!(target: CLIENT_LOG_TARGET, "skip proposal view {}, local view {}", proposal.view, self.state.view());
            return Ok(());
        }
        if !self.ensure_parents(proposal)? {
            trace!(target: CLIENT_LOG_TARGET, "check proposal parents not enough, skip vote!!!");
            return Ok(());
        }
        if check_payload {
            match self.check_payload(&proposal.payload) {
                Ok(true) => {
                    self.pending_proposal = Some(proposal.clone());
                    trace!(target: CLIENT_LOG_TARGET, "check_payload no state to check, skip vote for proposal to pending!!!");
                    return Ok(())
                },
                Ok(false) => (),
                Err(e) => {
                    debug!(target: CLIENT_LOG_TARGET, "#^# Check proposal {} digest {} parent {} payload {} error: {e:?}", proposal.view, proposal.digest(), proposal.parent_hash(), proposal.payload);
                    return Ok(())
                }
            }
        }
        self.processing = Some(proposal.payload.clone());

        if let Some(vote) = self.state.make_vote(&proposal) {
            trace!(target: CLIENT_LOG_TARGET, "~~ handle proposal({from}). make vote. view {}", vote.view);
            // If the current authority is the leader of the next view, it directly processes the
            // vote. Otherwise, it sends the vote to the next leader.
            if self.state.is_next_leader() {
                if let Err(e) = self.consensus_msg_tx.send((true, ConsensusMessage::Vote(vote))).await {
                    warn!(target: CLIENT_LOG_TARGET, "~~ handle proposal({from}). Can't inform self of `Vote` for {e:?}.");
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
        trace!(target: CLIENT_LOG_TARGET, "~~ handle_vote({from} self.view {}). vote.view {}, vote.author {}, vote.hash {}",
            self.state.view(),
            vote.view,
            vote.voter,
            vote.proposal_hash,
        );

        if !local {
            self.state.verify_vote(vote)?;
        }

        if let Some(mut qc) = self.state.add_vote(vote)? {
            trace!(target: CLIENT_LOG_TARGET, "~~ handle_vote({from} self.view {}). QC.view {}, proposal_hash {}", self.state.view(), qc.view, qc.proposal_hash);
            if self.state.is_leader_for(self.state.view().max(qc.view + 1)) {
                // TODO update qc timestamp for correct min_block_duration.
                self.handle_qc_timestamp(&mut qc).await?;
                self.trigger_qc_mission(&qc, true).await?;
                self.handle_qc(&qc);
                self.generate_proposal(None).await?;
            }
        }

        Ok(())
    }

    fn handle_qc(&mut self, qc: &QC<B>) {
        if qc.view >= self.state.view() {
            self.advance_view(qc.view);
        }
        // we should make sure high_qc updated.
        self.state.update_high_qc(qc);
        if qc.view >= self.pending_proposal.as_ref().map(|p| p.view).unwrap_or_default() {
            self.pending_proposal = None;
        }
    }

    // TODO better qc timestamp decide?
    async fn handle_qc_timestamp(&mut self, qc: &mut QC<B>) -> Result<(), HotstuffError> {
        if qc.stage != ConsensusStage::Commit { return Ok(()); }
        let qc_proposal = match self.synchronizer.get_proposal(ProposalKey::digest(qc.proposal_hash))? {
            Some(p) => p,
            None => {
                if let Some(req) = self.state.make_proposal_request(ProposalReq::Keys(vec![ProposalKey::digest(qc.proposal_hash)])) {
                    self.network.send_to_authorities(None, req.encode())?;
                }
                return Err(HotstuffError::GetProposal(format!("No proposal for commit qc of proposal {}, skip for can't check timestamp", qc.proposal_hash)));
            }
        };
        let new_block_number = qc_proposal.payload.block_number;
        // if from local, we should make time delay for min slot_duration.
        let last_commit_time = if self.commit.block_number.saturating_add(1u32.into()) == new_block_number {
            *self.commit.commit_time()
        } else if self.client.info().best_number.saturating_add(1u32.into()) == new_block_number {
            match self.client.header(self.client.info().best_hash).map_err(|e| HotstuffError::ClientError(e.to_string()))? {
                Some(best_header) => if *best_header.number() > 0u32.into() {
                    *find_block_commit::<B>(&best_header)
                        .expect("Best Header should have block commit")
                        .commit_time()
                } else {
                    Default::default()
                },
                None => {
                    return Err(HotstuffError::ClientError(
                        format!("No block header for {} {}", self.client.info().best_number, self.client.info().best_hash))
                    );
                },
            }
        } else {
            return Err(HotstuffError::ClientError(format!("Can't get last commit time for new block {} slot. skip ", new_block_number)));
        };
        let min_qc_time = last_commit_time.as_millis() + self.slot_duration.as_millis();
        if qc.timestamp.as_millis() < min_qc_time {
            qc.timestamp = Timestamp::from(min_qc_time);
            trace!(target: CLIENT_LOG_TARGET, "~~ handle_qc from local. QC.view {} set timestamp {min_qc_time} for next slot", qc.view);
        }
        Ok(())
    }

    // If consensus success(reach Commit stage), generate BlockCommit and send execute mission.
    async fn trigger_qc_mission(&mut self, qc: &QC<B>, local: bool) -> Result<(), HotstuffError> {
        let from = if local { "local" } else { "network" };
        if qc.proposal_hash == B::Hash::default() { return Ok(()) }
        // Try to get proposal ancestors to handle consensus finish.
        let qc_proposal = match self.synchronizer.get_proposal(ProposalKey::digest(qc.proposal_hash))? {
            Some(p) => p,
            None => {
                if let Some(req) = self.state.make_proposal_request(ProposalReq::Keys(vec![ProposalKey::digest(qc.proposal_hash)])) {
                    self.network.send_to_authorities(None, req.encode())?;
                }
                debug!(target: CLIENT_LOG_TARGET, "Can't trigger qc mission for no proposal of qc: {}:{}", qc.view, qc.proposal_hash);
                return Ok(());
            }
        };
        // update processed payload here. Any Prepare/Precommit/Commit success stage will be recorded.
        self.processed = qc_proposal.payload.clone();
        if !qc_proposal.payload.stage.finish()
            || qc_proposal.payload.block_number <= self.client.info().best_number
            || qc_proposal.payload.block_number <= self.commit.block_number
        {
            return Ok(());
        }
        // grandpa -> parent(grandpa_qc) -> qc_proposal(parent_qc) -> qc
        let new_block_number = qc_proposal.payload.block_number;
        match self.synchronizer.get_proposal_ancestors(&qc_proposal) {
            Ok(Some((parent, grandpa))) => {
                if grandpa.payload.extrinsic.is_none() || !Payload::<B>::full_consensus(&qc_proposal.payload, &parent.payload, &grandpa.payload) {
                    return Ok(());
                }
                let slot = InherentType::from_timestamp(qc.timestamp, self.slot_duration);
                let processed_slot = InherentType::from_timestamp(*self.commit.commit_time(), self.slot_duration);
                if slot > processed_slot {
                    let commit = match BlockCommit::generate(
                        (&grandpa, parent.qc.clone()),
                        (&parent, qc_proposal.qc.clone()),
                        (&qc_proposal, qc.clone()),
                    ) {
                        Some(commit) => commit,
                        None => return Ok(()),
                    };
                    // send execute block mission to a block execute queue and execute/import it.
                    info!(
                            target: CLIENT_LOG_TARGET,
                            "^^_^^. block {} slot {slot} timestamp {} can be execute with full proposal: [{}:{}, {}:{}, {}:{}]",
                            qc_proposal.payload.block_number,
                            qc.timestamp,
                            grandpa.view,
                            parent.parent_hash(),
                            parent.view,
                            qc_proposal.parent_hash(),
                            qc.view,
                            qc.proposal_hash,
                        );
                    if self.executor_tx.send(ExecutorMission::Consensus(commit.clone(), grandpa.payload.clone())).is_err() {
                        warn!(target: CLIENT_LOG_TARGET, "~~ trigger_qc_mission({from}). block {new_block_number} execute mission send failed");
                    }
                    self.commit = commit;
                    self.processed_extrinsic = grandpa.payload.extrinsic.clone();
                    if self.processing.is_some()
                        && new_block_number >= self.processing.as_ref().unwrap().block_number
                    {
                        // clear here for local to get correct next proposal payload
                        self.processing = None;
                    }
                    // if full consensus for a new block success, reset timer.
                    self.local_timer.reset();
                } else {
                    // skip update `self.processing_block` here should make next following `generate_proposal` to re-process this block.
                    debug!(target: CLIENT_LOG_TARGET, "~~ trigger_qc_mission({from}). block {new_block_number} execute mission skipped for slot rollback {processed_slot} -> {slot} ");
                }
            },
            Err(e) => {
                debug!(target: CLIENT_LOG_TARGET, "~~ trigger_qc_mission({from}). synchronizer: get {new_block_number} proposal_ancestors failed: {e:#?}");
            }
            _ => (),
        }
        Ok(())
    }

    pub async fn handle_tc(&mut self, tc: TC<B>, local: bool) -> Result<(), HotstuffError> {
        let from = if local { "local" } else { "network" };
        if !local {
            self.state.verify_tc(&tc)?;
            tc.high_qc.verify(&self.state.authorities)?;
        }
        self.handle_qc(&tc.high_qc);
        if tc.view >= self.state.view() {
            self.advance_view(tc.view);
            self.local_timer.reset();
        }
        self.processing = None;
        let is_leader = self.state.is_leader();
        debug!(
            target: CLIENT_LOG_TARGET,
            "~~ handle_tc({from}). self.view {}, tc {:?}.{}",
            self.state.view(),
            format!("{}({})", tc.view, tc.high_qc.view),
            if is_leader {
                " leader propose."
            } else {
                ""
            }
        );

        if is_leader {
            self.generate_proposal(Some(tc)).await?;
        }
        Ok(())
    }

    pub async fn generate_proposal(&mut self, tc: Option<TC<B>>) -> Result<(), HotstuffError> {
        if self.state.last_proposed_view == self.state.view {
            // already proposed for this view.
            return Ok(())
        }
        match self.get_proposal_payload().await {
            Some(payload) => {
                if payload.is_empty() {
                    return Ok(());
                }
                let proposal = match self.state.make_proposal(payload, tc)? {
                    Some(proposal) => proposal,
                    None => return Err(HotstuffError::NotAuthority),
                };
                trace!(target: CLIENT_LOG_TARGET, "~~ generate_proposal. view: {}({}), tc: {:?}, payload: {}",
                    proposal.view,
                    proposal.qc.view,
                    proposal.tc.as_ref().map(|tc| format!("{}({})", tc.view, tc.high_qc.view)),
                    proposal.payload,
                );
                let proposal_message = ConsensusMessage::Propose(proposal.clone());
                self.network.gossip_engine.lock().gossip_message(
                    ConsensusMessage::<B>::gossip_topic(),
                    proposal_message.encode(),
                    false,
                );

                if let Err(e) = self.consensus_msg_tx.send((true, ConsensusMessage::Propose(proposal))).await {
                    warn!(target: CLIENT_LOG_TARGET, "~~ generate_proposal. Can't inform self of `Propose` for {e:?}.");
                }
                // update proposed view to prevent duplicate propose for same view.
                self.state.increase_last_proposed_view();
            }
            None => {
                trace!(target: CLIENT_LOG_TARGET, "~~ generate_proposal. can't get next proposal.")
            }
        }

        Ok(())
    }

    async fn get_proposal_payload(&mut self) -> Option<Payload<B>> {
        let mut payload = None;
        let info = self.client.info();
        let best = info.best_number;
        let best_next = best.saturating_add(1u32.into());
        // if processed is in Precommit stage but not updated, continue commit.
        if let Some(next_stage) = self.processed.stage.next() {
            trace!(target: CLIENT_LOG_TARGET, "~~ get_proposal_payload(next). best: {best}, processed: {}:{}", self.processed.stage, self.processed.block_number);
            payload = Some(Payload {
                best_block: self.processed.best_block.clone(),
                block_number: self.processed.block_number,
                extrinsics_root: self.processed.extrinsics_root,
                extrinsic: None,
                stage: next_stage,
            });
        } else if self.commit.block_number < best.saturating_add(self.blocks_ahead_best) {
            // consensus for no more than best_block + blocks_ahead_best
            let block_number = best_next
                // self.processed.stage.next() is none, this means the processed block is finished, continue for next block.
                .max(self.processed.block_number.saturating_add(1u32.into()))
                .max(self.commit.block_number.saturating_add(1u32.into()));
            let mut except_extrinsic = None;
            if self.commit.block_number > best {
                // this processed block extrinsic are not executed, should exclude it for
                except_extrinsic = self.processed_extrinsic.as_ref();
            }
            let (get_payload, time) = self.get_payload(block_number, except_extrinsic).await;
            trace!(target: CLIENT_LOG_TARGET, "~~ get_proposal_payload(next). best: {best}, processed: Commit:{}, new: {block_number} in {time} micros", self.commit.block_number);
            payload = Some(get_payload);
        }
        payload
    }

    fn advance_view(&mut self, view: ViewNumber) {
        self.state.advance_view_from_target(view);
        self.network.set_view(self.state.view());
    }

    #[allow(unused)]
    pub(crate) fn empty_payload_hash() -> B::Hash {
        <<B::Header as HeaderT>::Hashing as HashT>::hash(EMPTY_PAYLOAD)
    }

    async fn get_payload(
        &self,
        block_number: <B::Header as HeaderT>::Number,
        exclude: Option<&(Vec<Vec<B::Extrinsic>>, Vec<B::Extrinsic>)>,
    ) -> (Payload<B>, u128) {
        let start = std::time::Instant::now();
        let info = self.client.info();
        let parent_header = self.client.header(info.best_hash).expect("failed to get best_hash").expect("no expected header");
        let proposer = self.proposer_factory.write().await.init(&parent_header).await.expect("proposer init");
        let except: Vec<&B::Extrinsic> = exclude
            .map(|(groups, single)| [groups.iter().flatten().collect::<Vec<&B::Extrinsic>>(), single.iter().collect()].concat())
            .unwrap_or_default();
        let mut extrinsic = match GroupTransaction::<B>::extrinsic(
            &proposer,
            *parent_header.number(),
            // time wait for pool response. (slot_duration / 10)
            std::time::Duration::from_millis(self.slot_duration.as_millis()).checked_div(10).unwrap_or_default(),
            // time for pool get transactions. (HotstuffDuration / 10)
            self.local_timer.period() / 10,
            None,
            None,
            except,
        ).await {
            Ok(extrinsic) => extrinsic,
            Err(e) => {
                warn!(target: CLIENT_LOG_TARGET, "GroupTransaction base on {} failed for {e:?}", parent_header.number());
                return (Payload::empty(), start.elapsed().as_micros());
            }
        };
        // sort by extrinsic length ascending order for merge.
        extrinsic.0.sort_by(|a, b| a.len().cmp(&b.len()));
        let mut extrinsic_data : Vec<Vec<u8>>= extrinsic.0
            .iter()
            .map(|g| g.iter().map(|e| e.encode()).collect::<Vec<Vec<u8>>>())
            .flatten()
            .collect::<Vec<Vec<u8>>>();
        extrinsic_data.extend(extrinsic.1.iter().map(|e| e.encode()).collect::<Vec<Vec<u8>>>());
        (
            Payload {
                best_block: BlockInfo { number: info.best_number, hash: info.best_hash },
                block_number,
                extrinsics_root: extrinsics_data_root::<<B::Header as HeaderT>::Hashing>(extrinsic_data),
                extrinsic: Some(extrinsic),
                stage: ConsensusStage::Prepare,
            },
            start.elapsed().as_micros(),
        )
    }

    // check proposal.payload have consist parents in local for final Commit
    fn ensure_parents(&self, proposal: &Proposal<B>) -> Result<bool, HotstuffError> {
        match proposal.payload.stage {
            ConsensusStage::Prepare => Ok(true),
            ConsensusStage::PreCommit => self.synchronizer.get_proposal_parent(proposal).map(|r| r.is_some()),
            ConsensusStage::Commit => self.synchronizer.get_proposal_ancestors(proposal).map(|r| r.is_some()),
        }
    }

    // return if we should keep pending_proposal.
    fn check_payload(&self, payload: &Payload<B>) -> Result<bool, HotstuffError> {
        if payload.is_empty() { return Ok(false); }
        if !payload.is_valid_next(&self.processed) {
            return Err(PayloadError::BlockRollBack(format!("processed: {}, new_payload: {}", self.processed, payload)).into());
        }
        let info = self.client.info();
        if payload.block_number > info.best_number.saturating_add(self.blocks_ahead_best) && self.state.is_authority() {
            // if propose new block is much more than local best block, do not vote for it.
            debug!(target: CLIENT_LOG_TARGET, "Meet proposal new_block {} > local_best_block {}(+{}). skip vote and pending it!", payload.block_number, info.best_number, self.blocks_ahead_best);
            return Ok(true);
        }
        match self.client.header(payload.best_block.hash) {
            Ok(Some(_header)) => (),
            // if local state have no best_block in payload, do not vote for it.
            Ok(None) => {
                debug!(target: CLIENT_LOG_TARGET, "Meet proposal new_block {} but local not imported. skip vote and pending it!", payload.best_block.number);
                return Ok(true);
            },
            Err(e) => return Err(HotstuffError::ClientError(e.to_string())),
        }
        // processed new_block. should not re-process it.
        if payload.block_number <= self.commit.block_number {
            return Err(PayloadError::BlockRollBack(format!("Next/Processed: {}/{}", payload.block_number, self.commit.block_number)).into());
        }
        if let Some((mut groups, single)) = payload.extrinsic.clone() {
            let start = std::time::Instant::now();
            if !single.is_empty() { groups.push(single); }
            let mut extrinsic_data = vec![];
            let extrinsic: Vec<_> = groups
                .into_iter()
                .map(|e| e.into_iter().map(|e| {
                    extrinsic_data.push(e.encode());
                    (TransactionSource::External, e)
                }).collect::<Vec<_>>())
                .collect();
            let extrinsics_root = extrinsics_data_root::<<B::Header as HeaderT>::Hashing>(extrinsic_data);
            if extrinsics_root != payload.extrinsics_root {
                return Err(PayloadError::ExtrinsicErr(format!("Incorrect extrinsics_root/payload: {extrinsics_root}/{})", payload.extrinsics_root)).into());
            }
            let mut check_tasks: Vec<JoinHandle<Result<(usize, Duration), HotstuffError>>> = vec![];
            for thread_extrinsic in extrinsic {
                let client = self.client.clone();
                let task = std::thread::spawn(move || {
                    let thread_start = std::time::Instant::now();
                    let length = thread_extrinsic.len();
                    let validate_results = client
                        .clone()
                        .runtime_api()
                        .validate_transactions(info.best_hash, thread_extrinsic, info.best_hash)
                        .map_err(|e| HotstuffError::ClientError(e.to_string()))?;
                    for res in validate_results {
                        if let Err(e) = res {
                            if !(e.exhausted_resources() || e.future()) {
                                // some extrinsic check filed(error but not exhausted_resources or future).
                                return Err(PayloadError::ExtrinsicErr(format!("validate_transactions meet err: {e:?}")).into());
                            }
                        }
                    }
                    Ok((length, thread_start.elapsed()))
                });
                check_tasks.push(task);
            }
            let mut threads_verify = vec![];
            for task in check_tasks {
                let res = task
                    .join()
                    .map_err(|e| HotstuffError::Payload(PayloadError::ExtrinsicErr(format!("validate_transactions meet err: {e:?}"))))??;
                threads_verify.push(res);
            }
            self.oracle.update_verify_times(&threads_verify);
            let check_extrinsic_time = start.elapsed().as_micros();
            if check_extrinsic_time / 1000 >= self.slot_duration.as_millis() as u128 {
                let mut threads_verify = threads_verify
                    .into_iter()
                    .filter(|(n, _)| *n > 0)
                    .map(|(n, time)| (n, time.as_millis())).collect::<Vec<_>>();
                threads_verify.sort_by(|a, b| b.1.cmp(&a.1));
                warn!(target: CLIENT_LOG_TARGET, "check_payload extrinsic exceed slot_duration({} millis): {check_extrinsic_time} micros(each length & millis: {threads_verify:?})", self.slot_duration.as_millis());
            }
        }
        Ok(false)
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
                if let Err(e) = self
                    .consensus_msg_tx
                    .try_send((false, ConsensusMessage::BlockImport(notification.header.clone())))
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
            Decode::decode(&mut &notification.message[..]).map_err(|e| HotstuffError::Other(e.to_string()))?;

        self.consensus_msg_tx
            .try_send((false, message))
            .map_err(|e| HotstuffError::Other(e.to_string()))
    }
}

impl<B, BE, C, N, S, PF, Error, O> Unpin for ConsensusWorker<B, BE, C, N, S, PF, Error, O>
where
    B: BlockT,
    BE: Backend<B>,
    C: ClientForHotstuff<B, BE>,
    N: NetworkT<B> + Sync + 'static,
    S: SyncingT<B> + Sync + 'static,
    PF: Environment<B, Error = Error> + Send + Sync + 'static,
    PF::Proposer: Proposer<B, Error = Error, Transaction = sp_api::TransactionFor<C, B>> + BlockPropose<B>,
    Error: std::error::Error + Send + From<ConsensusError> + 'static,
    O: BlockOracle<B> + HotsOracle + Sync + Send + 'static,
{
}

pub fn start_hotstuff<B, BE, C, N, S, SC, PF, Error, O, L, I, CIDP>(
    network: N,
    link: LinkHalf<B, C, SC>,
    sync: S,
    import: I,
    justification_sync_link: L,
    oracle: Arc<O>,
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
    PF::Proposer: Proposer<B, Error = Error, Transaction = TransactionFor<C, B>>
        + BlockPropose<B, Transaction = TransactionFor<C, B>, Proof = <PF::Proposer as Proposer<B>>::Proof, Error = Error>
        + GroupTransaction<B>
        + Send + Sync + 'static,
    L: sc_consensus::JustificationSyncLink<B>,
    I: BlockImport<B, Transaction = TransactionFor<C, B>> + ImportLock<B> + Send + Sync + 'static,
    Error: std::error::Error + Send + From<ConsensusError> + 'static,
    CIDP: CreateInherentDataProviders<B, Timestamp> + Send + 'static,
    CIDP::InherentDataProviders: InherentDataProviderExt + Send,
    SC: SelectChain<B>,
    O: BlockOracle<B> + HotsOracle + Sync + Send + 'static,
{
    let LinkHalf { client, .. } = link;
    let authorities = get_authorities_from_client::<B, BE, C>(client.clone());

    let synchronizer = Synchronizer::<B, C>::new(client.clone());
    let high_proposal = synchronizer.get_high_proposal().unwrap();
    let consensus_state = ConsensusState::<B>::new(keystore, high_proposal, authorities.clone(), Vec::new());
    let peer_authority = consensus_state.make_peer_authority(network.local_peer_id().to_base58());
    let network = HotstuffNetworkBridge::new(network.clone(), sync.clone(), hotstuff_protocol_name, authorities, peer_authority);

    let (consensus_msg_tx, consensus_msg_rx) = channel(1000);

    let queue = PendingFinalizeBlockQueue::<B>::new(client.clone()).expect("error");

    let mut local_timer_duration = slot_duration.as_millis();
    if let Ok(value) = env::var("HOTSTUFF_DURATION") {
        if let Ok(duration) = value.parse::<u64>() {
            local_timer_duration = duration;
        }
    }
    let mut blocks_ahead_best = 2u32;
    if let Ok(value) = env::var("HOTSTUFF_BLOCKS_AHEAD") {
        if let Ok(ahead) = value.parse::<u32>() {
            blocks_ahead_best = ahead;
        }
    }
    let (executor_tx, executor_rx) = unbounded_channel();
    let proposer_factory = Arc::new(RwLock::new(proposer_factory));
    let consensus_worker = ConsensusWorker::<B, BE, C, N, S, PF, Error, O>::new(
        consensus_state,
        client.clone(),
        sync,
        network.clone(),
        synchronizer,
        proposer_factory.clone(),
        local_timer_duration,
        slot_duration,
        oracle.clone(),
        blocks_ahead_best.into(),
        consensus_msg_tx.clone(),
        consensus_msg_rx,
        executor_tx,
    );

    let consensus_network = ConsensusNetwork::<B, N, S>::new(network, consensus_msg_tx, queue);
    let mut block_executor = BlockExecutor::new(
        client,
        import,
        slot_duration,
        oracle,
        proposer_factory,
        justification_sync_link,
        create_inherent_data_providers,
        select_chain,
        executor_rx,
    );
    Ok((async { consensus_worker.run().await }, consensus_network, async move { block_executor.run().await }))
}

pub fn get_authorities_from_client<
    B: BlockT,
    BE: Backend<B>,
    C: ClientForHotstuff<B, BE>,
>(
    client: Arc<C>,
) -> AuthorityList {
    let block_id = BlockId::hash(client.info().best_hash);
    let block_hash = client
        .expect_block_hash_from_id(&block_id)
        .expect("get genesis block hash from client failed");

    let authorities_data = client
        .executor()
        .call(
            block_hash,
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
