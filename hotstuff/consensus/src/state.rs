use std::cmp::max;
use std::marker::PhantomData;
use log::trace;
use w3f_bls::TinyBLS381;
use sp_application_crypto::AppCrypto;
use sp_core::crypto::ByteArray;
use sp_keystore::KeystorePtr;
use sp_runtime::traits::{Block as BlockT, Header as HeaderT};
use crate::{
    AuthorityId, AuthorityList, AuthoritySignature,
    aggregator::Aggregator,
    message::{
        Proposal, Timeout, Vote, QC, TC, Payload,
        PeerAuthority, ProposalReq, ProposalRequest,
    },
    primitives::{HotstuffError, ViewNumber},
    CLIENT_LOG_TARGET,
};
use hotstuff_primitives::{AuthorityIndex, HOTSTUFF_KEY_TYPE};
use crate::message::extend_message;

// the core of hotstuff
pub struct ConsensusState<B: BlockT> {
    pub keystore: KeystorePtr,
    pub authorities: AuthorityList,
    pub disabled_authorities: Vec<AuthorityIndex>,
    pub local_authority_id: Option<AuthorityId>,
    pub local_authority_index: Option<u16>,
    // local view number
    pub view: ViewNumber,
    pub last_voted_view: ViewNumber,
    pub last_proposed_view: ViewNumber,
    // last_committed_round: ViewNumber,
    pub high_qc: QC<B>,
    pub aggregator: Aggregator<B, TinyBLS381>,
}

impl<B: BlockT> ConsensusState<B> {
    pub fn new(
        keystore: KeystorePtr,
        high_proposal: Option<Proposal<B>>,
        authorities: AuthorityList,
        disabled_authorities: Vec<AuthorityIndex>,
    ) -> Self {
        let local_authority_id = authorities
            .iter()
            .enumerate()
            .find(|(_, (p, _))| {
                keystore
                    .has_keys(&[(p.to_raw_vec(), HOTSTUFF_KEY_TYPE)])
            })
            .map(|(index, (p, _))| (index, p.clone()));
        let mut state = Self {
            local_authority_id: local_authority_id.clone().map(|(_, p)| p.clone()),
            local_authority_index: local_authority_id.map(|(id, _)| id as u16),
            keystore,
            authorities,
            disabled_authorities,
            view: 0,
            last_voted_view: 0,
            last_proposed_view: 0,
            high_qc: Default::default(),
            aggregator: Aggregator::<B, TinyBLS381>::new(),
        };
        if let Some(high_proposal) = high_proposal {
            state.view = high_proposal.view.saturating_add(1);
            state.last_voted_view = high_proposal.view;
            state.high_qc = high_proposal.qc;
        }
        state
    }

    pub fn change_authorities(&mut self, _block: <B::Header as HeaderT>::Number, authorities: AuthorityList) {
        let local_authority_id = authorities
            .iter()
            .enumerate()
            .find(|(_, (p, _))| {
                self.keystore
                    .has_keys(&[(p.to_raw_vec(), HOTSTUFF_KEY_TYPE)])
            })
            .map(|(index, (p, _))| (index, p.clone()));
        self.local_authority_id = local_authority_id.clone().map(|(_, p)| p.clone());
        self.local_authority_index = local_authority_id.map(|(id, _)| id as u16);
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
        self.local_authority_id.is_some()
    }

    pub fn authorities(&self) -> Vec<&AuthorityId> {
        self.authorities.iter().map(|a| &a.0).collect()
    }

    pub fn message_with_id(&self, msg: &[u8]) -> Result<Vec<u8>, HotstuffError> {
        match self.local_authority_index {
            Some(index) => Ok(extend_message(index, msg)),
            None => Err(HotstuffError::NotAuthority)
        }
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
                &self.message_with_id(tc.digest().as_ref())?,
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
                &self.message_with_id(block.digest().as_ref())?,
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

        let mut vote = Vote::<B>::new(proposal.digest(), proposal.view, proposal.payload.stage(), author_id.clone());

        vote.signature = self
            .keystore
            .sign_with(
                AuthorityId::ID,
                AuthorityId::CRYPTO_ID,
                author_id.as_slice(),
                &self.message_with_id(vote.digest().as_ref()).ok()?,
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
