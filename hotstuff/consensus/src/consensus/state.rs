use std::cmp::max;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;
use log::trace;
use w3f_bls::TinyBLS381;
use sp_application_crypto::AppCrypto;
use sp_core::crypto::ByteArray;
use sp_keystore::KeystorePtr;
use sp_runtime::traits::{Block as BlockT, Header as HeaderT};
use crate::{
    find_consensus_logs, AuthorityId,
    AuthorityList, AuthoritySignature, CLIENT_LOG_TARGET
};
use hotstuff_primitives::{AuthorityIndex, ConsensusLog, RuntimeAuthorityId, HOTSTUFF_KEY_TYPE};
use sc_client_api::AuxStore;
use sp_blockchain::HeaderBackend;
use crate::aux_schema::PersistentData;
use crate::consensus::aggregator::Aggregator;
use crate::consensus::error::{HotstuffError, ViewNumber};
use crate::consensus::message::{
    Payload, PeerAuthority, Proposal, ProposalReq, ProposalRequest, Timeout,
    Vote, QC, TC,
};
use crate::consensus::message::{extend_message, CommitQC, ConsensusStage, Round};

pub struct Authority<B: BlockT, C: AuxStore + HeaderBackend<B>> {
    pub client: Arc<C>,
    pub cache_size: usize,
    pub pending: Option<(Option<ViewNumber>, <B::Header as HeaderT>::Number, AuthorityList)>,
    pub cache: Vec<(ViewNumber, <B::Header as HeaderT>::Number, AuthorityList)>,
    pub persistent_data: PersistentData<B>,
}

impl<B: BlockT, C: AuxStore + HeaderBackend<B>> Authority<B, C> {
    pub fn load_from_persistent_data(&mut self) -> Result<(), HotstuffError> {
        let authority_set = self.persistent_data.authority_set.inner();
        if let Some((pending_block, pending_authorities)) = authority_set.pending_authority_set.as_ref() {
            self.pending = Some((None, *pending_block, pending_authorities.clone()))
        }
        if authority_set.authority_set_changes.as_ref().is_empty() {
            self.cache.push((0, 0u32.into(), authority_set.current_authorities.clone()));
        }
        for (view, block) in authority_set.authority_set_changes.as_ref().iter().rev().take(self.cache_size) {
            if let Some(authority_list) = authority_set.authorities_for_view(*view) {
                self.cache.push((*view, *block, authority_list.clone()));
            } else {
                let authority_list = self.get_authorities_from_backend(*view, *block)?;
                self.cache.push((*view, *block, authority_list));
            }
        }
        Ok(())
    }

    pub fn on_block_commit(&mut self, commit_view: ViewNumber, commit_block: <B::Header as HeaderT>::Number) {
        if let Some((view, b, _)) = &mut self.pending {
            if commit_block == *b {
                if let Some(view) = view {
                    // update if higher view for this block commit.
                    *view = (*view).max(commit_view);
                } else {
                    *view = Some(commit_view);
                }
            }
        }
    }

    pub fn authorities_from_cache(&self, view: ViewNumber) -> Option<&AuthorityList> {
        if let Some((Some(v), _, authorities)) = self.pending.as_ref() {
            if view > *v {
                return Some(authorities);
            }
        }
        for (v, _, authorities) in self.cache.iter() {
            if *v == 0 || view > *v { return Some(authorities); }
        }
        None
    }

    pub fn get_authorities_from_backend(&self, view: ViewNumber, block: <B::Header as HeaderT>::Number) -> Result<AuthorityList, HotstuffError> {
        let block_hash = self.client.hash(block)
            .map_err(|e| HotstuffError::ClientError(e.to_string()))?
            .ok_or(HotstuffError::ClientError(format!("Block #{block} should have hash")))?;
        let header = self.client.header(block_hash)
            .map_err(|e| HotstuffError::ClientError(e.to_string()))?
            .ok_or(HotstuffError::ClientError(format!("Block #{block}:{block_hash} should have header")))?;
        for consensus_log in find_consensus_logs::<B, RuntimeAuthorityId>(&header) {
            if let ConsensusLog::AuthoritiesChange(authorities) = consensus_log {
                return Ok(authorities.into_iter().map(|a| (a.into(), 0)).collect());
            }
        }
        Err(HotstuffError::InvalidAuthority(format!("can't get authorities for view {view} block #{block}")))
    }

    pub fn update_pending_authorities(&mut self, block: <B::Header as HeaderT>::Number, authority_list: AuthorityList) -> bool {
        if let Some((_, b, al)) = self.pending.clone() {
            if block > b {
                if block == b && al != authority_list {
                    log::warn!(target: CLIENT_LOG_TARGET, "Pending authorities for block #{block} changed which should not happen!!!");
                }
                self.pending = Some((None, block, authority_list));
                return true;
            }
        } else {
            self.pending = Some((None, block, authority_list));
            return true;
        }
        false
    }

    pub fn update_authorities(&mut self, view: ViewNumber, block: <B::Header as HeaderT>::Number, authority_list: AuthorityList) -> bool {
        if let Some((_, b, al)) = self.pending.clone() {
            if b >= block {
                if b == block && authority_list != al {
                    log::error!(target: CLIENT_LOG_TARGET, "Pending authorities not match actual authorities applied at block #{block}");
                }
                self.pending.take();
            }
        }
        if view > self.cache.first().map(|(v, _, _)| *v).unwrap_or_default() {
            self.cache.insert(0, (view, block, authority_list));
            self.cache.resize(self.cache_size, (0, 0u32.into(), Vec::new()));
            true
        } else {
            false
        }
    }
}

// the core of hotstuff
pub struct ConsensusState<B: BlockT, C: AuxStore + HeaderBackend<B>> {
    pub keystore: KeystorePtr,
    pub authority: Authority<B, C>,
    pub local_authority_ids: Vec<AuthorityId>,
    // local Round
    pub round: Round,
    pub last_voted: Round,
    pub last_proposed: Round,
    pub commit_qc: QC<B>,
    pub high_qc: QC<B>,
    pub aggregator: Aggregator<B, TinyBLS381>,
}

impl<B: BlockT, C: AuxStore + HeaderBackend<B>> ConsensusState<B, C> {
    pub fn new(
        client: Arc<C>,
        keystore: KeystorePtr,
        commit_qc: Option<CommitQC<B>>,
        high_proposal: Option<Proposal<B>>,
        persistent_data: PersistentData<B>,
    ) -> Self {
        let mut authority = Authority {
            client,
            cache_size: 10,
            pending: None,
            cache: Vec::new(),
            persistent_data,
        };
        authority.load_from_persistent_data().unwrap();
        let mut local_authority_ids = vec![];
        for key_data in keystore.keys(HOTSTUFF_KEY_TYPE).unwrap() {
            if let Ok(authority_id) = AuthorityId::try_from(key_data.as_slice()) {
                local_authority_ids.push(authority_id);
                // TODO We currently use only first authority key.
                break;
            }
        }
        let mut state = Self {
            local_authority_ids,
            keystore,
            authority,
            round: Round::default(),
            last_voted: Round::default(),
            last_proposed: Round::default(),
            commit_qc: Default::default(),
            high_qc: Default::default(),
            aggregator: Aggregator::<B, TinyBLS381>::new(),
        };
        if let Some(commit_qc) = commit_qc {
            state.commit_qc = commit_qc.qc;
        }
        if let Some(high_proposal) = high_proposal {
            state.round = high_proposal.round().add_one();
            state.last_voted = high_proposal.round();
            state.high_qc = high_proposal.qc;
        }
        state
    }

    pub fn change_pending_authorities(&mut self, block: <B::Header as HeaderT>::Number, authority_list: AuthorityList) -> Result<(), HotstuffError> {
        self.authority.update_pending_authorities(block, authority_list);
        Ok(())
    }

    pub fn change_authorities(&mut self, view: ViewNumber, block: <B::Header as HeaderT>::Number, authority_list: AuthorityList) -> Result<(), HotstuffError> {
        self.authority.update_authorities(view, block, authority_list);
        Ok(())
    }

    pub fn disable_authority(&mut self, _block: <B::Header as HeaderT>::Number, _index: AuthorityIndex) {
        // TODO not in use. If need: 1. storage on pallet_hotstuff. 2. Initialize on start. 3. Update
    }

    pub fn increase_last_voted(&mut self) {
        self.last_voted = max(self.last_voted, self.round)
    }

    pub fn increase_last_proposed(&mut self) {
        self.last_proposed = max(self.last_proposed, self.round)
    }

    pub fn have_authority(&self, authority: &AuthorityId) -> Option<usize> {
        if self.local_authority_ids.is_empty() {
            return None;
        }
        self.local_authority_ids.iter().enumerate().find(|(_, a)| *a == authority).map(|(i, _)| i)
    }

    pub fn find_authority(&self, view: ViewNumber) -> Option<(u16, AuthorityId)> {
        if self.local_authority_ids.is_empty() {
            return None;
        }
        let authority_list = self.authority_list(view).ok()?;
        for (index, authority) in authority_list.iter().enumerate() {
            if let Some(_) = self.have_authority(&authority.0) {
                return Some((index as u16, authority.0.clone()))
            }
        }
        None
    }

    pub fn authority_list(&self, view: ViewNumber) -> Result<&AuthorityList, HotstuffError> {
        self.authority.authorities_from_cache(view).ok_or(HotstuffError::InvalidAuthority(format!("can't get authorities for view {view}")))
    }

    pub fn authorities(&self, view: ViewNumber, with_pending: bool) -> Result<Vec<&AuthorityId>, HotstuffError> {
        if with_pending {
            let mut authorities: HashSet<&AuthorityId> = self.authority_list(view)
                .map(|a| a.iter().map(|a| &a.0).collect())?;
            if let Some((_, _, pending_authorities)) = self.authority.pending.as_ref() {
                for (a, _) in pending_authorities.iter() {
                    authorities.insert(a);
                }
            }
            Ok(authorities.into_iter().collect())
        } else {
            self.authority_list(view)
                .map(|a| a.iter().map(|a| &a.0).collect())
        }
    }

    pub fn make_timeout(&self) -> Result<Option<Timeout<B>>, HotstuffError> {
        let (index, authority_id) = match self.find_authority(self.round.view) {
            Some(r) => r,
            None => return Ok(None),
        };

        let mut tc: Timeout<B> = Timeout {
            high_qc: self.high_qc.clone(),
            view: self.round.view,
            voter: authority_id.clone(),
            signature: None,
        };

        tc.signature = self
            .keystore
            .sign_with(
                AuthorityId::ID,
                AuthorityId::CRYPTO_ID,
                authority_id.as_ref(),
                &extend_message(index, tc.digest().as_ref()),
            )
            .map_err(|e| HotstuffError::Other(e.to_string()))?
            .and_then(|data| AuthoritySignature::try_from(data).ok());

        Ok(Some(tc))
    }

    pub fn make_proposal(
        &self,
        parent_qc: QC<B>,
        payload: Payload<B>,
        tc: Option<TC<B>>,
    ) -> Result<Option<Proposal<B>>, HotstuffError> {
        let (index, authority_id) = match self.find_authority(self.round.view) {
            Some(r) => r,
            None => return Ok(None),
        };
        let mut block = Proposal::<B>::new(
            parent_qc,
            tc,
            payload,
            self.round.view,
            authority_id.clone(),
            None,
        );

        block.signature = self
            .keystore
            .sign_with(
                AuthorityId::ID,
                AuthorityId::CRYPTO_ID,
                authority_id.as_slice(),
                &extend_message(index, block.digest().as_ref()),
            )
            .map_err(|e| HotstuffError::Other(e.to_string()))?
            .and_then(|data| AuthoritySignature::try_from(data).ok());

        Ok(Some(block))
    }

    pub fn make_vote(&mut self, proposal: &Proposal<B>) -> Option<Vote<B>> {
        if proposal.round() <= self.last_voted {
            trace!(target: CLIENT_LOG_TARGET, "make_vote proposal round {}, last_voted {}, skip vote for proposal!!!", proposal.round(), self.last_voted);
            return None;
        }
        let (index, authority_id) = match self.find_authority(proposal.view) {
            Some(r) => r,
            None => return None,
        };

        self.last_voted = max(self.last_voted, proposal.round());

        let mut vote = Vote::<B>::new(
            proposal.digest(),
            proposal.view,
            proposal.payload.stage,
            authority_id.clone(),
        );

        vote.signature = self
            .keystore
            .sign_with(
                AuthorityId::ID,
                AuthorityId::CRYPTO_ID,
                authority_id.as_slice(),
                &extend_message(index, vote.digest().as_ref()),
            )
            .ok()?
            .and_then(|data| AuthoritySignature::try_from(data).ok());

        Some(vote)
    }

    pub fn make_commit_qc(&self, qc: QC<B>) -> Option<CommitQC<B>> {
        let (index, authority_id) = match self.find_authority(qc.view) {
            Some(r) => r,
            None => return None,
        };
        let mut commit_qc = CommitQC::<B>::new(qc, authority_id.clone());

        commit_qc.signature = self
            .keystore
            .sign_with(
                AuthorityId::ID,
                AuthorityId::CRYPTO_ID,
                authority_id.as_slice(),
                &extend_message(index, commit_qc.digest().as_ref()),
            )
            .ok()?
            .and_then(|data| AuthoritySignature::try_from(data).ok());

        Some(commit_qc)
    }

    pub fn make_peer_authority(&self, peer_id: String) -> Option<PeerAuthority<B>> {
        if self.local_authority_ids.is_empty() { return None; }
        let author_id = self.local_authority_ids[0].clone();
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
        // if not latest authority, can't request proposal.
        let (_, authority_id) = match self.find_authority(self.round.view) {
            Some(r) => r,
            None => return None,
        };
        let mut request = ProposalRequest {
            authority: authority_id.clone(),
            requests,
            signature: None,
        };

        request.signature = self
            .keystore
            .sign_with(
                AuthorityId::ID,
                AuthorityId::CRYPTO_ID,
                authority_id.as_slice(),
                request.digest().as_ref(),
            )
            .ok()?
            .and_then(|data| AuthoritySignature::try_from(data).ok());

        Some(request)
    }

    pub fn view(&self) -> ViewNumber {
        self.round.view
    }

    pub fn verify_timeout(&self, timeout: &Timeout<B>) -> Result<(), HotstuffError> {
        timeout.verify(self.authority_list(timeout.view)?, self.authority_list(timeout.high_qc.view)?)
    }

    pub fn verify_proposal(&self, proposal: &Proposal<B>) -> Result<(), HotstuffError> {
        match self.view_leader(proposal.view) {
            Some(leader) => if !proposal.author.eq(leader) {
                return Err(HotstuffError::WrongProposer);
            }
            None => return Err(HotstuffError::Other(format!("can't find authorities to verify proposal {}", proposal.view))),
        }
        proposal.verify(self.authority_list(proposal.view)?, self.authority_list(proposal.qc.view)?)?;
        Ok(())
    }

    pub fn is_proposer(&self, proposal: &Proposal<B>) -> bool {
        self.local_authority_ids.contains(&proposal.author)
    }

    pub fn verify_vote(&self, vote: &Vote<B>) -> Result<(), HotstuffError> {
        if vote.round() < self.round {
            return Err(HotstuffError::ExpiredVote);
        }

        vote.verify(self.authority_list(vote.view)?)
    }

    pub fn verify_tc(&self, tc: &TC<B>) -> Result<(), HotstuffError> {
        if tc.view < self.round.view {
            return Err(HotstuffError::InvalidTC);
        }

        tc.verify(self.authority_list(tc.view)?)
    }

    // add a verified timeout then try return a TC.
    pub fn add_timeout(&mut self, timeout: &Timeout<B>) -> Result<Option<TC<B>>, HotstuffError> {
        let authority_list = self.authority_list(timeout.view)?.clone();
        self.aggregator.add_timeout(timeout, &authority_list)
    }

    // add a verified vote and try return a QC.
    pub fn add_vote(&mut self, vote: &Vote<B>) -> Result<Option<QC<B>>, HotstuffError> {
        let authority_list = self.authority_list(vote.view)?.clone();
        self.aggregator.add_vote(vote.clone(), &authority_list)
    }

    pub fn update_commit_qc(&mut self, qc: &QC<B>) {
        if qc.stage.finish() && qc.round() > self.commit_qc.round() {
            self.commit_qc = qc.clone()
        }
    }

    pub fn update_high_qc(&mut self, qc: &QC<B>) {
        if qc.round() > self.high_qc.round() {
            self.high_qc = qc.clone()
        }
    }

    pub fn advance_view_from_target(&mut self, view: ViewNumber) {
        if self.round.view <= view {
            self.round = (view + 1, ConsensusStage::Prepare).into();
        }
    }

    pub fn advance_round_from_target(&mut self, round: Round) {
        if self.round <= round {
            self.round = round.add_one();
        }
    }

    pub fn view_leader(&self, view: ViewNumber) -> Option<&AuthorityId> {
        let authorities = self.authority_list(view).ok()?;
        if authorities.is_empty() {
            return None;
        }
        let leader_index = view % authorities.len() as ViewNumber;
        Some(&authorities[leader_index as usize].0)
    }

    pub fn is_leader_for(&self, view: ViewNumber) -> bool {
        match self.view_leader(view) {
            Some(leader) => self.local_authority_ids.contains(&leader),
            None => false,
        }
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader_for(self.round.view)
    }
}
