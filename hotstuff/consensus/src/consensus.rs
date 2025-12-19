use std::{
    env,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use std::collections::{HashMap, HashSet};
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
use sp_core::traits::CallContext;
use sp_keystore::KeystorePtr;
use sp_runtime::{generic::BlockId, traits::{Block as BlockT, Hash as HashT, Header as HeaderT}, Saturating};
use sc_consensus::BlockImport;
use sc_consensus_slots::InherentDataProviderExt;
use sp_consensus::SelectChain;

use crate::{AuthorityList, client::{ClientForHotstuff, LinkHalf}, find_consensus_logs, find_block_commit, executor::{BlockExecutor, ExecutorMission}, import::{PendingFinalizeBlockQueue, ImportLock}, message::{
    ConsensusMessage, Proposal, Timeout, Vote, QC, TC, BlockCommit, ConsensusStage, Payload,
    PeerAuthority, ProposalKey, ProposalReq, ProposalRequest,
}, network::{HotstuffNetworkBridge, Network as NetworkT, Syncing as SyncingT}, error::{HotstuffError, PayloadError, ViewNumber}, aux_data::{AuxDataStore, Timer}, CLIENT_LOG_TARGET, AuthorityId};
use hotstuff_primitives::{ConsensusLog, HotstuffApi, RuntimeAuthorityId};
use sc_network::PeerId;
use sc_network_common::sync::SyncState;
use sp_api::TransactionFor;
use sp_consensus::{Environment, Error as ConsensusError, Proposer};
use sp_consensus_slots::SlotDuration;
use sp_inherents::CreateInherentDataProviders;
use sp_runtime::traits::Zero;
use sp_runtime::transaction_validity::TransactionSource;
use sp_timestamp::Timestamp;
use crate::executor::NewBlockMission;
use crate::message::{CommitQC, Round};
use crate::oracle::HotsOracle;
use crate::revert::get_block_commit;
use crate::state::ConsensusState;

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

pub struct ConsensusWorker<
    B: BlockT,
    BE: Backend<B>,
    C: ClientForHotstuff<B, BE>,
    N: NetworkT<B> + Sync + 'static,
    S: SyncingT<B> + Sync + 'static,
    PF: Environment<B, Error = Error> + Send + Sync + 'static,
    Error: std::error::Error + Send + From<ConsensusError> + 'static,
    O: BlockOracle<B> + HotsOracle<B> + Sync + Send + 'static,
> {
    state: ConsensusState<B, C>,

    network: HotstuffNetworkBridge<B, N, S>,
    client: Arc<C>,
    sync: S,
    local_timer: Timer,
    slot_duration: SlotDuration,
    aux_data: AuxDataStore<B, C>,
    consensus_msg_tx: Sender<(bool, ConsensusMessage<B>)>,
    consensus_msg_rx: Receiver<(bool, ConsensusMessage<B>)>,
    executor_tx: UnboundedSender<ExecutorMission<B>>,
    proposer_factory: Arc<RwLock<PF>>,
    oracle: Arc<O>,
    /// Tmp cache for not voted proposal(should be verified and qc mission is handled).
    pending_proposal: Option<Proposal<B>>,
    processed: Proposal<B>,
    /// consensus success block waiting to execute.
    commit: BlockCommit<B>,
    commit_extrinsic: HashMap<<B::Header as HeaderT>::Number, (ViewNumber, Vec<Vec<Vec<B::Extrinsic>>>)>,

    network_notification_limit: usize,
    phantom: PhantomData<BE>,
}

impl<B, BE, C, N, S, PF, Error, O> ConsensusWorker<B, BE, C, N, S, PF, Error, O>
where
    B: BlockT,
    BE: Backend<B>,
    C: ClientForHotstuff<B, BE> + 'static,
    C::Api: HotstuffApi<B, RuntimeAuthorityId>,
    N: NetworkT<B> + Sync + 'static,
    S: SyncingT<B> + Sync + 'static,
    PF: Environment<B, Error = Error> + Send + Sync + 'static,
    PF::Proposer: Proposer<B, Error = Error, Transaction = sp_api::TransactionFor<C, B>> + BlockPropose<B> + GroupTransaction<B>,
    Error: std::error::Error + Send + From<ConsensusError> + 'static,
    O: BlockOracle<B> + HotsOracle<B> + Sync + Send + 'static,
{
    #![allow(clippy::too_many_arguments)]
    pub fn new(
        consensus_state: ConsensusState<B, C>,
        client: Arc<C>,
        sync: S,
        network: HotstuffNetworkBridge<B, N, S>,
        aux_data: AuxDataStore<B, C>,
        proposer_factory: Arc<RwLock<PF>>,
        local_timer_duration: u64,
        slot_duration: SlotDuration,
        oracle: Arc<O>,
        consensus_msg_tx: Sender<(bool, ConsensusMessage<B>)>,
        consensus_msg_rx: Receiver<(bool, ConsensusMessage<B>)>,
        executor_tx: UnboundedSender<ExecutorMission<B>>,
        network_notification_limit: usize,
    ) -> Self {
        if local_timer_duration < slot_duration.as_millis() {
            let slot_duration_millis = slot_duration.as_millis();
            warn!(
                target: CLIENT_LOG_TARGET,
                "HOTSTUFF_DURATION({local_timer_duration}) smaller than slot_duration({slot_duration_millis})!!!"
            );
        }
        let best_header = client.header(client.info().best_hash).unwrap().expect("Best header should be present");
        let commit = if !best_header.number().is_zero() {
            find_block_commit::<B>(&best_header).expect("Best block should have BlockCommit!!!")
        } else {
            BlockCommit::empty()
        };
        let processed = aux_data.get_high_proposal().unwrap().unwrap_or(Proposal::empty());
        info!(target: CLIENT_LOG_TARGET, "Start consensus worker with local_timer_duration: {local_timer_duration}ms, slot_duration: {}ms", slot_duration.as_millis());
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
            aux_data,
            proposer_factory,
            oracle,
            pending_proposal: None,
            processed,
            commit,
            commit_extrinsic: HashMap::new(),
            network_notification_limit,
            phantom: PhantomData,
        }
    }

    // Try recover all unapplied commit.
    pub fn recover(&mut self) {
        let latest = self.client.info().best_number;
        let latest_round = if latest > 0u32.into() {
            get_block_commit(&self.client, self.client.info().best_hash)
                .expect("Get best block commit failed")
                .round()
        } else {
            Round::zero()
        };
        let mut round = self.aux_data.high_round();
        let mut proposal_key = ProposalKey::digest(self.aux_data.high_digest());
        if round == Round::default() {
            debug!(target: CLIENT_LOG_TARGET, "[Recover] Skip for default high_round {round}");
            return;
        }
        // ready to recover higher state since recover will change it.
        let init_processed = self.processed.clone();
        let mut init_commit = self.commit.clone();
        let mut commit_qc_list = vec![];
        // load commit_qc
        loop {
            if round <= latest_round {
                debug!(target: CLIENT_LOG_TARGET, "[Recover] load finish for round {round} <= latest {latest_round}");
                break;
            }
            match self.aux_data.get_proposal(proposal_key.clone()).unwrap() {
                Some(proposal) => {
                    trace!(target: CLIENT_LOG_TARGET, "[Recover] ~~ load proposal {} qc {}", proposal.round(), proposal.qc.round());
                    if proposal.qc.stage.finish() {
                        commit_qc_list.push(proposal.qc.clone());
                    }
                    if proposal.payload.block_number() <= latest {
                        debug!(target: CLIENT_LOG_TARGET, "[Recover] load finish for proposal {} payload block #{} <= latest #{latest}", proposal.round(), proposal.payload.block_number());
                        break;
                    }
                    round = proposal.qc.round();
                    proposal_key = ProposalKey::digest(proposal.qc.proposal_hash);
                },
                None => {
                    debug!(target: CLIENT_LOG_TARGET, "[Recover] No proposal for round {round}");
                    round = round.sub_one();
                    if round == Round::zero() {
                        debug!(target: CLIENT_LOG_TARGET, "[Recover] load finish for reach round zero");
                        break;
                    }
                    proposal_key = ProposalKey::round(round);
                }
            };
        }
        commit_qc_list.sort_by(|a, b| a.round().cmp(&b.round()));
        if let Some(commit_qc) = self.aux_data.get_commit_qc().unwrap() {
            self.update_by_qc(&commit_qc.qc);
            if commit_qc.qc.round() > commit_qc_list.last().map(|qc| qc.round()).unwrap_or_default() {
                commit_qc_list.push(commit_qc.qc);
            }
        }
        for qc in commit_qc_list {
            if let Err(e) = self.trigger_qc_mission(&qc, true) {
                error!(target: CLIENT_LOG_TARGET, "[Recover] ~~ trigger_qc_mission {} failed for {e:?}", qc.view);
            }
            // since init commit is from best block, recovered commit might be higher.
            if self.commit.view() > init_commit.view() {
                init_commit = self.commit.clone();
            }
        }
        self.processed = init_processed;
        self.commit = init_commit;
    }

    pub async fn try_wait_sync(&mut self) {
        let mut sync_state = self.sync.status().await.unwrap();
        if sync_state.state != SyncState::Idle {
            if let Some(best_seen_block) = sync_state.best_seen_block {
                if self.client.info().best_number.saturating_add(30u32.into()) >= best_seen_block {
                    return;
                }
                info!(target: CLIENT_LOG_TARGET, "Local best_block: {:?}, best_seen block: {best_seen_block}, waiting for sync", self.client.info().best_number);
            }
            let mut counter = 0usize;
            loop {
                if sync_state.state == SyncState::Idle {
                    info!(target: CLIENT_LOG_TARGET, "Sync state: {:?}, best_seen block: {:?}, sync finished!!!", sync_state.state, sync_state.best_seen_block);
                    self.local_timer.reset();
                    break;
                }
                if counter / 30 == 0 {
                    info!(target: CLIENT_LOG_TARGET, "Current sync state: {:?}, best_seen block: {:?}", sync_state.state, sync_state.best_seen_block);
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
                sync_state = self.sync.status().await.unwrap();
                counter += 1;
            }
        }
    }

    pub async fn run(mut self) {
        if self.state.local_authority_ids.is_empty() {
            info!(target: CLIENT_LOG_TARGET, "Local is not authority");
        } else {
            info!(target: CLIENT_LOG_TARGET, "Local authority id is: {:?}", self.state.local_authority_ids);
        }
        self.recover();
        self.try_wait_sync().await;
        self.local_timer.reset();
        info!(target: CLIENT_LOG_TARGET, "Start with round {} processed: {}, commit: {}, high_qc {}", self.state.round, self.processed.round(), self.commit.round(), self.state.high_qc.round());
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
                        ConsensusMessage::ResponseProposal(proposals) => {
                            if let Err(e) = self.handle_proposal_response(&proposals) {
                                trace!(target: CLIENT_LOG_TARGET, "[handle_proposal_response({from})] has error {e:?}");
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
                        ConsensusMessage::CommitQC(qc) => {
                            if let Err(e) = self.handle_commit_qc(&qc, local).await{
                                trace!(target: CLIENT_LOG_TARGET, "[handle_commit_qc({from})] has error {e:?}");
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
        debug!(target: CLIENT_LOG_TARGET, "$L$ handle_local_timer. self.round {}", self.state.round);

        self.local_timer.reset();
        self.pending_proposal = None;

        // current self.view will be timeout view.
        let timeout = match self.state.make_timeout()? {
            Some(timeout) => timeout,
            None => return Ok(()),
        };
        self.state.increase_last_voted();
        let message = ConsensusMessage::Timeout(timeout.clone());

        self.network.send_to_authorities(self.state.authorities(self.state.view(), true)?, message.encode())?;

        if let Err(e) = self.consensus_msg_tx.send((true, ConsensusMessage::Timeout(timeout))).await {
            warn!(target: CLIENT_LOG_TARGET, "$L$ handle_local_timer. Can't inform self `Timeout` for {e:?}.");
        }
        Ok(())
    }

    pub async fn handle_timeout(&mut self, timeout: &Timeout<B>, local: bool) -> Result<(), HotstuffError> {
        if self.state.view() > timeout.view { return Ok(()); }
        let from = if local { "local" } else { "network" };
        trace!(
            target: CLIENT_LOG_TARGET,
            "~~ handle_timeout({from} self.round {}). view {}, high_qc.round {}, author {}",
            self.state.round,
			timeout.view,
            timeout.high_qc.round(),
            timeout.voter,
        );

        if !local {
            self.state.verify_timeout(timeout)?;
            if timeout.high_qc.round() >= self.state.round {
                self.update_by_qc(&timeout.high_qc);
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
        request.verify(self.state.authority_list(self.state.view())?)?;
        let proposals = match &request.requests {
            ProposalReq::Keys(keys) => {
                let mut proposals = vec![];
                for key in keys.clone() {
                    if let Some(proposal) = self.aux_data.get_proposal(key)? {
                        proposals.push(proposal);
                    }
                }
                proposals
            },
            ProposalReq::Range(from, to) => {
                self.aux_data.get_proposals(from.clone(), to.clone())?
            }
        };
        if proposals.is_empty() { return Ok(()); }
        trace!(
            target: CLIENT_LOG_TARGET,
            "~~ handle_proposal_request. Response {} proposals to {}({:?}).",
            proposals.len(),
            request.authority,
            proposals.iter().map(|p| p.round()).collect::<Vec<_>>(),
        );
        self.network.send_to_authorities(vec![&request.authority], ConsensusMessage::ResponseProposal(proposals).encode())?;
        Ok(())
    }

    pub fn handle_proposal_response(&mut self, proposals: &Vec<Proposal<B>>) -> Result<(), HotstuffError> {
        for proposal in proposals {
            if self.aux_data.get_proposal(ProposalKey::digest(proposal.digest()))?.is_some() {
                continue;
            }
            self.state.verify_proposal(&proposal)?;
            self.aux_data.save_proposal(&proposal)?;
            self.update_by_qc(&proposal.qc);
            self.trigger_qc_mission(&proposal.qc, false)?;
        }
        Ok(())
    }

    pub async fn handle_block_import(&mut self, header: &B::Header) -> Result<(), HotstuffError> {
        if let Some(commit) = find_block_commit::<B>(header) {
            for consensus_log in find_consensus_logs::<B, RuntimeAuthorityId>(header) {
                match consensus_log {
                    ConsensusLog::AuthoritiesPending(authorities, target_block) => {
                        debug!(target: CLIENT_LOG_TARGET, "~~ handle_block_import. change_pending_authorities block {target_block} view {} authorities: {}", commit.view(), authorities.len());
                        let authority_list = authorities.into_iter().map(|a| (a.into(), 0)).collect();
                        if let Err(e) = self.state.change_pending_authorities(target_block, authority_list) {
                            warn!(target: CLIENT_LOG_TARGET, "~~ handle_block_import. change_pending_authorities block {} failed: {e:?}", header.number());
                        }
                    }
                    ConsensusLog::AuthoritiesChange(authorities) => {
                        // changes authorities list.
                        debug!(target: CLIENT_LOG_TARGET, "~~ handle_block_import. change_authorities block #{} view {} authorities: {}", commit.block_number(), commit.view(), authorities.len());
                        let authority_list = authorities.into_iter().map(|a| (a.into(), 0)).collect();
                        if let Err(e) = self.state.change_authorities(commit.view(), *header.number(), authority_list) {
                            warn!(target: CLIENT_LOG_TARGET, "~~ handle_block_import. change_authorities block {} failed: {e:?}", header.number());
                        }
                        // since we allow pre consensus block, we accept even authorities change.
                    },
                    ConsensusLog::OnDisabled(index) => self.state.disable_authority(*header.number(), index),
                }
            }
            let (deleted, deleted_invalid) = self.aux_data.clear_all_before(ProposalKey::digest(commit.parent_commit_hash()))?;
            if !deleted.is_empty() || !deleted_invalid.is_empty() {
                trace!(target: CLIENT_LOG_TARGET, "~~ handle_block_import. Block {} imported, delete proposals {deleted:?}, invalid proposals: {deleted_invalid:?}", header.number());
            }
            // update if imported block have higher commit.
            if commit.view() > self.commit.view() {
                self.commit = commit;
            }
            self.commit_extrinsic.remove(header.number());
        }
        if self.client.info().best_number > *header.number() {
            return Ok(());
        }
        if self.executor_tx.send(ExecutorMission::Imported(header.clone())).is_err() {
            warn!(target: CLIENT_LOG_TARGET, "~~ handle_block_import. Notify executor block {} imported failed", header.number());
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
        if self.aux_data.get_proposal(ProposalKey::round(proposal.round()))?.is_some() { return Ok(()); }
        let from = if local { "local" } else { "network" };
        debug!(target: CLIENT_LOG_TARGET, "~~ handle_proposal({from} self.round {}). proposal[ round {}({}), tc {:?}, digest {},  payload {}, author {}]",
            self.state.round,
            proposal.round(),
            proposal.qc.round(),
            proposal.tc.as_ref().map(|tc| format!("{}({})", tc.view, tc.high_qc.view)),
            proposal.digest(),
            proposal.payload,
            proposal.author,
        );
        if proposal.qc.round().view < self.state.high_qc.view
            && proposal.payload.stage == ConsensusStage::Prepare
        {
            if proposal.qc.round() < self.state.commit_qc.round() {
                trace!(
                    target: CLIENT_LOG_TARGET,
                    "skip proposal round {} low qc {}, local commit_qc {}",
                    proposal.round(),
                    proposal.qc.round(),
                    self.state.commit_qc.round(),
                );
                return Ok(());
            }
        } else if self.state.high_qc.round() > proposal.qc.round() {
            trace!(
                target: CLIENT_LOG_TARGET,
                "skip proposal round {} low qc {}, local high_qc {}",
                proposal.round(),
                proposal.qc.round(),
                self.state.high_qc.round(),
            );
            return Ok(());
        }
        if !local {
            self.state.verify_proposal(&proposal)?;
        }
        self.aux_data.save_proposal(&proposal)?;
        if !local {
            if proposal.round() > self.processed.round() {
                self.processed = proposal.clone();
            }
            if let Some(tc) = proposal.tc.as_ref() {
                self.update_by_qc(&tc.high_qc);
                if tc.view >= self.state.view() {
                    self.advance_view(tc.view);
                }
            }
            self.update_by_qc(&proposal.qc);
        }
        // the qc mission is triggered before this proposal generate.
        // self.trigger_qc_mission(&proposal.qc, local).await?;
        self.vote_for_proposal(proposal, !local, from).await?;
        Ok(())
    }

    async fn vote_for_proposal(&mut self, proposal: &Proposal<B>, check_payload: bool, from: &str) -> Result<(), HotstuffError> {
        if proposal.view != self.state.view() {
            trace!(target: CLIENT_LOG_TARGET, "skip proposal round {}, local round {}", proposal.round(), self.state.round);
            return Ok(());
        }
        if !self.ensure_parents(proposal)? {
            trace!(target: CLIENT_LOG_TARGET, "check proposal parents not enough, skip vote!!!");
            return Ok(());
        }
        if check_payload {
            match self.check_payload(proposal) {
                Ok(true) => {
                    self.pending_proposal = Some(proposal.clone());
                    self.try_wait_sync().await;
                    trace!(target: CLIENT_LOG_TARGET, "check_payload no state to check, skip vote for proposal to pending!!!");
                    return Ok(())
                },
                Ok(false) => (),
                Err(e) => {
                    debug!(target: CLIENT_LOG_TARGET, "#^# Check proposal {} digest {} parent {} payload {} error: {e:?}", proposal.round(), proposal.digest(), proposal.parent_hash(), proposal.payload);
                    return Ok(())
                }
            }
        }

        if let Some(vote) = self.state.make_vote(&proposal) {
            trace!(target: CLIENT_LOG_TARGET, "~~ handle proposal({from}). make vote. round {}", vote.round());
            if self.state.is_leader() {
                if let Err(e) = self.consensus_msg_tx.send((true, ConsensusMessage::Vote(vote.clone()))).await {
                    warn!(target: CLIENT_LOG_TARGET, "~~ vote_for_proposal({from}). Can't inform self `Vote` for {e:?}.");
                }
            } else {
                let leader = self.state.view_leader(self.state.round.view)
                    .ok_or(HotstuffError::Other(format!("No leader for round {}", self.state.round)))?;
                self.network.send_to_authorities(vec![&leader], ConsensusMessage::Vote(vote).encode())?;
            }
        }
        Ok(())
    }

    pub async fn handle_vote(&mut self, vote: &Vote<B>, local: bool) -> Result<(), HotstuffError> {
        let from = if local { "local" } else { "network" };
        trace!(target: CLIENT_LOG_TARGET, "~~ handle_vote({from} self.round {}). vote.round {}, vote.hash {}, vote.author {}",
            self.state.round,
            vote.round(),
            vote.proposal_hash,
            vote.voter,
        );

        if !local {
            if vote.view < self.state.view() { return Err(HotstuffError::ExpiredVote); }
            self.state.verify_vote(vote)?;
        }

        if let Some(mut qc) = self.state.add_vote(vote)? {
            trace!(target: CLIENT_LOG_TARGET, "~~ handle_vote({from} self.round {}). QC.round {}, proposal_hash {}", self.state.round, qc.round(), qc.proposal_hash);
            if self.state.is_leader() {
                if qc.stage.finish() {
                    self.handle_qc_timestamp(&mut qc).await?;
                    // if this stage finish, should not be next proposer
                    if let Some(commit_qc) = self.state.make_commit_qc(qc.clone()) {
                        let message = ConsensusMessage::CommitQC(commit_qc);
                        self.network.gossip_engine.lock().gossip_message(ConsensusMessage::<B>::gossip_topic(), message.encode(), false);
                    }
                }
                self.trigger_qc_mission(&qc, true)?;
                self.update_by_qc(&qc);
                if !qc.stage.finish() {
                    self.generate_proposal(None).await?;
                }
            }
        }

        Ok(())
    }

    pub async fn handle_commit_qc(&mut self, commit_qc: &CommitQC<B>, local: bool) -> Result<(), HotstuffError> {
        let from = if local { "local" } else { "network" };
        if !commit_qc.qc.stage.finish() { return Ok(()); }
        trace!(target: CLIENT_LOG_TARGET, "~~ handle_commit_qc({from} self.round {}). round {}, hash {}, author {}",
            self.state.round,
            commit_qc.qc.round(),
            commit_qc.qc.proposal_hash,
            commit_qc.author,
        );
        commit_qc.verify(self.state.authority_list(commit_qc.qc.view)?)?;
        if let Err(e) = self.aux_data.save_commit_qc(commit_qc) {
            warn!(target: CLIENT_LOG_TARGET, "Save CommitQC failed for {e:?}");
        }
        self.trigger_qc_mission(&commit_qc.qc, local)?;
        self.update_by_qc(&commit_qc.qc);
        if self.state.is_leader() {
            self.generate_proposal(None).await?;
        }
        Ok(())
    }

    fn update_by_qc(&mut self, qc: &QC<B>) {
        // we should make sure high_qc updated.
        self.state.update_high_qc(qc);
        if qc.round() >= self.state.round {
            self.advance_round(qc.round());
        }
        if qc.round() >= self.pending_proposal.as_ref().map(|p| p.round()).unwrap_or_default() {
            self.pending_proposal = None;
        }
    }

    async fn handle_qc_timestamp(&mut self, qc: &mut QC<B>) -> Result<(), HotstuffError> {
        if !qc.stage.finish() { return Ok(()); }
        let qc_proposal = match self.aux_data.get_proposal(ProposalKey::digest(qc.proposal_hash))? {
            Some(p) => p,
            None => {
                self.request_proposals(
                    vec![ProposalKey::digest(qc.proposal_hash)],
                    self.state.find_authority(qc.view).as_ref().map(|(_, a)| vec![a]).unwrap_or(
                        self.state.authorities(self.state.view(), false)?,
                    ),
                )?;
                return Err(HotstuffError::GetProposal(format!("No proposal for commit qc of proposal {}, skip for can't check timestamp", qc.proposal_hash)));
            }
        };
        let parent_commit_hash = match self.aux_data.get_proposal_ancestors(&qc_proposal)? {
            Some((_, grandpa)) => grandpa.qc.proposal_hash,
            None => {
                return Err(HotstuffError::GetProposal(format!("No ancestor proposals for commit qc of proposal {}, skip for can't check timestamp", qc.proposal_hash)));
            }
        };
        let new_block_number = qc_proposal.payload.block_number();
        // if from local, we should make time delay for min slot_duration.
        let last_commit_time = if self.commit.commit_hash() == parent_commit_hash {
            *self.commit.commit_time()
        } else if self.client.info().best_number >= new_block_number.saturating_sub(1u32.into()) {
            let parent_block = new_block_number.saturating_sub(1u32.into());
            if new_block_number > 1u32.into() {
                match self.client.block_hash_from_id(&BlockId::Number(parent_block)) {
                    Ok(Some(parent_hash)) => match self.client.header(parent_hash).map_err(|e| HotstuffError::ClientError(e.to_string()))? {
                        Some(parent_header) => {
                            let parent_commit = find_block_commit::<B>(&parent_header).expect("Best Header should have block commit");
                            if parent_commit.commit_hash() == parent_commit_hash {
                                *parent_commit.commit_time()
                            } else {
                                return Err(HotstuffError::ProposalNoParent)
                            }
                        }
                        None => {
                            return Err(HotstuffError::ClientError(format!("No block header for {parent_block}:{parent_hash}")));
                        },
                    },
                    Ok(None) => {
                        return Err(HotstuffError::ClientError(format!("No block hash for {parent_block}")));
                    },
                    Err(e) => {
                        return Err(HotstuffError::ClientError(format!("Get block {parent_block} hash failed for {e:?}")));
                    }
                }
            } else {
                Default::default()
            }
        } else {
            return Err(HotstuffError::ClientError(format!("Can't get last commit time for new block {} slot. skip ", new_block_number)));
        };
        let min_qc_time = last_commit_time.as_millis() + self.slot_duration.as_millis();
        if qc.timestamp.as_millis() < min_qc_time {
            qc.timestamp = Timestamp::from(min_qc_time);
            trace!(target: CLIENT_LOG_TARGET, "~~ handle_qc from local. QC.round {} set timestamp {min_qc_time} for next slot", qc.round());
        }
        Ok(())
    }

    // If consensus success(reach Commit stage), generate BlockCommit and send execute mission.
    fn trigger_qc_mission(&mut self, qc: &QC<B>, local: bool) -> Result<(), HotstuffError> {
        let from = if local { "local" } else { "network" };
        if qc.proposal_hash == B::Hash::default() { return Ok(()) }
        // Try to get proposal ancestors to handle consensus finish.
        let qc_proposal = match self.aux_data.get_proposal(ProposalKey::digest(qc.proposal_hash))? {
            Some(p) => p,
            None => {
                self.request_proposals(
                    vec![ProposalKey::digest(qc.proposal_hash)],
                    self.state.find_authority(qc.view).as_ref().map(|(_, a)| vec![a]).unwrap_or(
                        self.state.authorities(self.state.view(), false)?,
                    )
                )?;
                debug!(target: CLIENT_LOG_TARGET, "Can't trigger qc mission for no proposal of qc: {}", qc.view);
                return Ok(());
            }
        };
        if !qc_proposal.payload.stage.finish() { return Ok(()); }
        // grandpa -> parent(grandpa_qc) -> qc_proposal(parent_qc) -> qc
        match self.aux_data.get_proposal_ancestors(&qc_proposal) {
            Ok(Some((parent, grandpa))) => {
                if grandpa.payload.extrinsics.is_none() {
                    return Ok(())
                };
                if grandpa.payload.block_number() <= self.client.info().best_number {
                    return Ok(());
                }
                let new_block_number = grandpa.payload.block_number();
                if qc_proposal.view > self.commit.view() {
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
                        "^^_^^. block {new_block_number} can be execute with QC {} parent {}",
                        commit.round(),
                        grandpa.qc.round(),
                    );
                    self.state.authority.on_block_commit(commit.view(), commit.block_number());
                    let extrinsics = grandpa.payload.extrinsics.clone().unwrap();
                    let mission = ExecutorMission::Consensus(NewBlockMission {
                        commit: commit.clone(),
                        block: grandpa.payload.block.clone(),
                        extrinsics: extrinsics.clone(),
                    });
                    if self.executor_tx.send(mission).is_err() {
                        warn!(target: CLIENT_LOG_TARGET, "~~ trigger_qc_mission({from}). block {new_block_number} execute mission send failed");
                    }
                    if let Some((pre_view, extrinsic)) = self.commit_extrinsic.get_mut(&commit.block_number()) {
                        if commit.view() > *pre_view {
                            *extrinsic = extrinsics;
                        }
                    } else {
                        self.commit_extrinsic.insert(commit.block_number(), (commit.view(), extrinsics));
                    }
                    self.state.update_commit_qc(&qc);
                    self.commit = commit;
                    // if full consensus for a new block success, reset timer.
                    self.local_timer.reset();
                } else {
                    // skip update `self.processing_block` here should make next following `generate_proposal` to re-process this block.
                    trace!(target: CLIENT_LOG_TARGET, "~~ trigger_qc_mission({from}). block {new_block_number} execute mission skipped for not higher view than commit: {} {}", qc_proposal.view, self.commit.view());
                }
            },
            Err(e) => {
                debug!(target: CLIENT_LOG_TARGET, "~~ trigger_qc_mission({from}). aux_data: get proposal {} ancestors failed: {e:#?}", qc.view);
            }
            _ => (),
        }
        Ok(())
    }

    pub async fn handle_tc(&mut self, tc: TC<B>, local: bool) -> Result<(), HotstuffError> {
        let from = if local { "local" } else { "network" };
        if !local {
            self.state.verify_tc(&tc)?;
            tc.high_qc.verify(self.state.authority_list(tc.high_qc.view)?)?;
        }
        self.update_by_qc(&tc.high_qc);
        if tc.view >= self.state.view() {
            self.advance_view(tc.view);
            self.local_timer.reset();
        }
        let is_leader = self.state.is_leader();
        debug!(
            target: CLIENT_LOG_TARGET,
            "~~ handle_tc({from}). self.round {}, tc {}.{}",
            self.state.round,
            format!("{}({})", tc.view, tc.high_qc.round()),
            if is_leader {
                " leader propose."
            } else {
                ""
            }
        );
        // try process tc.high_qc mission.
        if self.processed.qc.round() < self.state.high_qc.round() {
            self.trigger_qc_mission(&tc.high_qc, local)?;
        }

        if is_leader {
            self.generate_proposal(Some(tc)).await?;
        }
        Ok(())
    }

    pub async fn generate_proposal(&mut self, tc: Option<TC<B>>) -> Result<(), HotstuffError> {
        if self.state.last_proposed == self.state.round {
            // already proposed for this round.
            return Ok(())
        }
        match self.get_proposal_payload().await {
            Some(payload) => {
                if payload.stage != self.state.round.stage {
                    warn!(target: CLIENT_LOG_TARGET, "~~ generate_proposal. get payload stage not match state stage {} {}", payload.stage, self.state.round.stage);
                    return Ok(())
                }
                let parent_qc = if payload.stage == ConsensusStage::Prepare {
                    self.state.commit_qc.clone()
                } else {
                    self.state.high_qc.clone()
                };

                let proposal = match self.state.make_proposal(parent_qc, payload, tc)? {
                    Some(proposal) => proposal,
                    None => return Err(HotstuffError::NotAuthority),
                };
                trace!(target: CLIENT_LOG_TARGET, "~~ generate_proposal. round: {}({}), tc: {:?}, payload: {}",
                    proposal.round(),
                    proposal.qc.round(),
                    proposal.tc.as_ref().map(|tc| format!("{}({})", tc.view, tc.high_qc.view)),
                    proposal.payload,
                );
                let proposal_message = ConsensusMessage::Propose(proposal.clone());
                let encoded_message = proposal_message.encode();
                if encoded_message.len() > self.network_notification_limit {
                    warn!(target: CLIENT_LOG_TARGET, "~~ generate_proposal. Proposal {} message exceed max notification limit {}/{}", proposal.round(), encoded_message.len(), self.network_notification_limit);
                };
                self.network.send_to_authorities(self.state.authorities(self.state.view(), true)?, encoded_message)?;

                if let Err(e) = self.consensus_msg_tx.send((true, ConsensusMessage::Propose(proposal))).await {
                    warn!(target: CLIENT_LOG_TARGET, "~~ generate_proposal. Can't inform self of `Propose` for {e:?}.");
                }
                // update proposed view to prevent duplicate propose for same view.
                self.state.increase_last_proposed();
            }
            None => {
                trace!(target: CLIENT_LOG_TARGET, "~~ generate_proposal. can't get next proposal.")
            }
        }

        Ok(())
    }

    async fn get_proposal_payload(&mut self) -> Option<Payload<B>> {
        let best = self.client.info().best_number;
        let best_next = best.saturating_add(1u32.into());
        let info = format!("best: {best} high_qc: {}:{}", self.state.high_qc.round(), self.state.high_qc.proposal_hash);
        // if processed lower than high_qc. local have no parent proposal, we can't get next payload.
        let high_proposal = if self.state.high_qc.round() == Round::default() {
            Proposal::empty()
        } else {
            match self.aux_data.get_proposal(ProposalKey::digest(self.state.high_qc.proposal_hash)) {
                Ok(Some(proposal)) => proposal,
                Ok(None) => {
                    trace!(target: CLIENT_LOG_TARGET, "~~ get_proposal_payload. {info} get no high_proposal");
                    return None;
                },
                Err(e) => {
                    error!(target: CLIENT_LOG_TARGET, "~~ get_proposal_payload. {info} get high_proposal failed: {e:?}");
                    return None;
                }
            }
        };
        let high_state = format!("{}:#{}", high_proposal.round(), high_proposal.payload.block_number());
        let (payload, time) = if self.state.round.view > high_proposal.round().view {
            if self.commit.block_number() >= best.saturating_add(2u32.into()) {
                (None, Default::default())
            } else {
                // restart consensus for a new block
                let block_number = best_next
                    .max(self.commit.block_number().saturating_add(1u32.into()));
                let (next_payload, time) = self.get_new_block_payload(block_number).await;
                (Some(next_payload), time)
            }
        } else if self.state.round.view == high_proposal.view {
            if let Some(next_payload) = high_proposal.payload.next() {
                // if processed is latest, get next payload by processed.payload.
                (Some(next_payload), Default::default())
            } else if high_proposal.payload.block_number() < best.saturating_add(2u32.into()) {
                // if processed is Commit, try to propose for next block.
                // consensus for no more than best_block + 2
                let block_number = best_next
                    // high_proposal.payload.next() is none, this means the high_proposal block is finished, continue for next block.
                    .max(high_proposal.payload.block_number().saturating_add(1u32.into()))
                    .max(self.commit.block_number().saturating_add(1u32.into()));
                let (next_payload, time) = self.get_new_block_payload(block_number).await;
                (Some(next_payload), time)
            } else {
                (None, Default::default())
            }
        } else {
            (None, Default::default())
        };
        if let Some(payload) = &payload {
            trace!(target: CLIENT_LOG_TARGET, "~~ get_proposal_payload. {info}({high_state}) next {}:#{} in {}μs", payload.stage, payload.block_number(), time.as_micros());
        }
        payload
    }

    fn request_proposals(&self, keys: Vec<ProposalKey<B>>, from: Vec<&AuthorityId>) -> Result<(), HotstuffError> {
        if let Some(req) = self.state.make_proposal_request(ProposalReq::Keys(keys)) {
            let message = ConsensusMessage::GetProposal(req);
            self.network.send_to_authorities(from, message.encode())?;
        }
        Ok(())
    }

    fn advance_round(&mut self, round: Round) {
        self.state.advance_round_from_target(round);
        self.network.set_view(self.state.view());
    }

    fn advance_view(&mut self, view: ViewNumber) {
        self.state.advance_view_from_target(view);
        self.network.set_view(self.state.view());
    }

    async fn get_new_block_payload(&self, block_number: <B::Header as HeaderT>::Number) -> (Payload<B>, Duration) {
        let start = std::time::Instant::now();
        let info = self.client.info();
        let parent_header = self.client.header(info.best_hash).expect("failed to get best_hash").expect("no expected header");
        let proposer = self.proposer_factory.write().await.init(&parent_header).await.expect("proposer init");
        let (mut multi, single, group_info) = match GroupTransaction::<B>::extrinsic(
            &proposer,
            *parent_header.number(),
            // time wait for pool response. (slot_duration / 10)
            std::time::Duration::from_millis(self.slot_duration.as_millis()).checked_div(10).unwrap_or_default(),
            // time for pool get transactions. (HotstuffDuration / 10)
            self.local_timer.period() / 10,
            None,
            None,
            self.filter_transactions(block_number),
        ).await {
            Ok((multi, single, info)) => {
                let multi_length: Vec<_> = multi.iter().map(|g| g.len()).collect();
                let groups = vec![multi_length, vec![single.len()]];
                debug!(target: CLIENT_LOG_TARGET, "GroupTransaction for #{block_number} base on #{}({}) {groups:?}", parent_header.number(), parent_header.hash());
                (multi, single, info)
            },
            Err(e) => {
                warn!(target: CLIENT_LOG_TARGET, "GroupTransaction base on {} failed for {e:?}", parent_header.number());
                return (Payload::empty(), start.elapsed());
            }
        };
        self.oracle.update_group_info(&group_info);
        // sort by extrinsic length ascending order for merge.
        multi.sort_by(|a, b| a.len().cmp(&b.len()));
        let mut extrinsic_data : Vec<Vec<u8>>= multi
            .iter()
            .map(|g| g.iter().map(|e| e.encode()).collect::<Vec<Vec<u8>>>())
            .flatten()
            .collect::<Vec<Vec<u8>>>();
        extrinsic_data.extend(single.iter().map(|e| e.encode()).collect::<Vec<Vec<u8>>>());
        (
            Payload {
                stage: ConsensusStage::Prepare,
                block: (block_number, extrinsics_data_root::<<B::Header as HeaderT>::Hashing>(extrinsic_data)).into(),
                extrinsics: Some(vec![multi, vec![single]]),
            },
            start.elapsed(),
        )
    }

    fn filter_transactions(&self, block_number: <B::Header as HeaderT>::Number) -> HashSet<B::Hash> {
        let mut filter = self.oracle.filter_transactions();
        let mut block = self.client.info().best_number.saturating_add(1u32.into());
        loop {
            if block > block_number { break; }
            if let Some((_, extrinsic)) = self.commit_extrinsic.get(&block) {
                // this processed block extrinsic are not executed, should exclude it for
                for round in extrinsic {
                    for hash in round.iter().flatten().map(|tx| <<B::Header as HeaderT>::Hashing as HashT>::hash(&tx.encode())) {
                        filter.insert(hash);
                    }
                }
            }
            block = block.saturating_add(1u32.into());
        }
        filter
    }

    // check proposal.payload have consist parents in local for final Commit
    fn ensure_parents(&self, proposal: &Proposal<B>) -> Result<bool, HotstuffError> {
        let enough = match proposal.payload.stage {
            ConsensusStage::Prepare => true,
            ConsensusStage::PreCommit => self.aux_data.get_proposal_parent(proposal).map(|r| r.is_some())?,
            ConsensusStage::Commit => self.aux_data.get_proposal_parent(proposal).map(|r| r.is_some())?,
        };
        if !enough {
            let mut need = vec![];
            match proposal.payload.stage {
                ConsensusStage::Prepare => (),
                ConsensusStage::PreCommit => {
                    need.push(ProposalKey::Round((proposal.view, ConsensusStage::Prepare).into()));
                },
                ConsensusStage::Commit => {
                    need.push(ProposalKey::Round((proposal.view, ConsensusStage::PreCommit).into()));
                }
            }
            self.request_proposals(need, vec![&proposal.author])?;
        }
        Ok(enough)
    }

    // return if we should keep pending_proposal.
    fn check_payload(&self, proposal: &Proposal<B>) -> Result<bool, HotstuffError> {
        let info = self.client.info();
        let payload = &proposal.payload;
        if payload.block_number() > info.best_number.saturating_add(2u32.into()) && self.state.find_authority(proposal.view).is_some() {
            // if propose new block is much more than local best block, do not vote for it.
            debug!(target: CLIENT_LOG_TARGET, "Meet proposal new_block #{} > local_best_block #{}(+2). skip vote and pending it!", payload.block_number(), info.best_number);
            return Ok(true);
        }
        if payload.block_number() < self.commit.block_number() {
            return Err(PayloadError::BlockRollBack(format!("Next/Processed: #{}/#{}", payload.block_number(), self.commit.block_number())).into());
        }
        if payload.stage != ConsensusStage::Prepare { return Ok(false); }
        let parent_commit_hash = self.commit.commit_hash();
        if self.commit.block_number().saturating_add(1u32.into()) == payload.block_number() {
            if parent_commit_hash != proposal.parent_hash() {
                return Err(PayloadError::BaseBlock(format!("proposal {} incorrect parent #{} commit {parent_commit_hash} {}", proposal.round(), self.commit.block_number(), proposal.parent_hash())).into());
            }
        } else if self.commit.block_number().saturating_add(1u32.into()) < payload.block_number() {
            // request parent block full proposals from proposal author.
            self.request_proposals(
                vec![
                    ProposalKey::Round((proposal.qc.view, ConsensusStage::Prepare).into()),
                    ProposalKey::Round((proposal.qc.view, ConsensusStage::PreCommit).into()),
                    ProposalKey::digest(proposal.qc.proposal_hash),
                ],
                vec![&proposal.author],
            )?;
            return Err(PayloadError::BaseBlock(format!("proposal {} no local parent commit to check, commit: {}, commit_qc: {}", proposal.round(), self.commit.round(), self.state.commit_qc.round())).into());
        }
        if payload.extrinsics.is_none() {
            return Ok(false);
        }
        // verify extrinsic
        let start = std::time::Instant::now();
        // spawn extrinsics_root check thread.
        let extrinsics = payload.extrinsics.clone().unwrap();
        let extrinsics_root = std::thread::spawn(|| {
            let extrinsic_data: Vec<_> = extrinsics
                .into_iter()
                .flatten()
                .flatten()
                .map(|e| e.encode())
                .collect();
            extrinsics_data_root::<<B::Header as HeaderT>::Hashing>(extrinsic_data)
        });
        // spawn threads for transactions check
        let thread_verify_limit = self.oracle.thread_verify_limit().unwrap_or(1000);
        let groups = payload.extrinsics.as_ref().unwrap().iter().flatten();
        let extrinsic: Vec<_> = groups
            .map(|e|
                // limit each verify_thread's extrinsic number.
                e.chunks(thread_verify_limit)
                    .map(|chunk|
                        chunk.iter().map(|e| { (TransactionSource::External, e.clone()) }).collect::<Vec<_>>()
                    )
            )
            .flatten()
            .collect();
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
        match extrinsics_root.join() {
            Ok(root) => if root != payload.block.extrinsics_root {
                return Err(PayloadError::ExtrinsicErr(format!("Incorrect extrinsics_root/payload: {root}/{})", payload.block.extrinsics_root)).into());
            },
            Err(e) => return Err(HotstuffError::Other(format!("Failed to calculate extrinsics_root for {e:?}"))),
        }
        self.oracle.update_verify_times(&threads_verify);
        let check_extrinsic_time = start.elapsed().as_micros();
        if check_extrinsic_time / 1000 >= self.slot_duration.as_millis() as u128 {
            let mut threads_verify = threads_verify
                .into_iter()
                .filter(|(n, _)| *n > 0)
                .map(|(n, time)| (n, time.as_millis())).collect::<Vec<_>>();
            threads_verify.sort_by(|a, b| b.1.cmp(&a.1));
            warn!(target: CLIENT_LOG_TARGET, "check_payload extrinsic exceed slot_duration({}ms): {check_extrinsic_time}μs(each length & ms: {threads_verify:?})", self.slot_duration.as_millis());
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
    O: BlockOracle<B> + HotsOracle<B> + Sync + Send + 'static,
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
    max_notification_size: Option<usize>,
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
    C::Api: hotstuff_primitives::HotstuffApi<B, RuntimeAuthorityId>,
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
    O: BlockOracle<B> + HotsOracle<B> + Sync + Send + 'static,
{
    let LinkHalf { client, select_chain: _, persistent_data } = link;
    let authorities = get_authorities_from_client::<B, BE, C>(client.clone());

    let aux_data = AuxDataStore::<B, C>::new(client.clone());
    let commit_qc = aux_data.get_commit_qc().unwrap();
    let high_proposal = aux_data.get_high_proposal().unwrap();
    let consensus_state = ConsensusState::<B, C>::new(client.clone(), keystore, commit_qc, high_proposal, persistent_data);
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
    let (executor_tx, executor_rx) = unbounded_channel();
    let proposer_factory = Arc::new(RwLock::new(proposer_factory));
    let consensus_worker = ConsensusWorker::<B, BE, C, N, S, PF, Error, O>::new(
        consensus_state,
        client.clone(),
        sync,
        network.clone(),
        aux_data,
        proposer_factory.clone(),
        local_timer_duration,
        slot_duration,
        oracle.clone(),
        consensus_msg_tx.clone(),
        consensus_msg_rx,
        executor_tx,
        max_notification_size.unwrap_or(1024 * 1024 * 5),
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

    let authorities: Vec<RuntimeAuthorityId> = Decode::decode(&mut &authorities_data[..]).expect("");

    authorities
        .iter()
        .map(|id| (id.clone().into(), 0))
        .collect::<AuthorityList>()
}
