use std::cmp::max;
use std::marker::PhantomData;
use std::sync::Arc;
use log::trace;
use w3f_bls::TinyBLS381;
use sp_application_crypto::AppCrypto;
use sp_core::crypto::ByteArray;
use sp_keystore::KeystorePtr;
use sp_runtime::traits::{Block as BlockT, Header as HeaderT};
use crate::{AuthorityId, AuthorityList, AuthoritySignature, aggregator::Aggregator, message::{
    Proposal, Timeout, Vote, QC, TC, Payload,
    PeerAuthority, ProposalReq, ProposalRequest,
}, primitives::{HotstuffError, ViewNumber}, CLIENT_LOG_TARGET, find_consensus_logs};
use hotstuff_primitives::{AuthorityIndex, ConsensusLog, HOTSTUFF_KEY_TYPE};
use sc_client_api::AuxStore;
use sp_blockchain::HeaderBackend;
use crate::aux_schema::PersistentData;
use crate::message::extend_message;

pub struct Authority<B: BlockT, C: AuxStore + HeaderBackend<B>> {
    pub client: Arc<C>,
    pub cache_size: usize,
    pub cache: Vec<(ViewNumber, <B::Header as HeaderT>::Number, AuthorityList)>,
    pub persistent_data: PersistentData<B>,
}

impl<B: BlockT, C: AuxStore + HeaderBackend<B>> Authority<B, C> {
    pub fn load_from_persistent_data(&mut self) -> Result<(), HotstuffError> {
        let authority_set = self.persistent_data.authority_set.inner();
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

    pub fn authorities_from_cache(&self, view: ViewNumber) -> Option<&AuthorityList> {
        for (v, _, authorities) in self.cache.iter() {
            if *v == 0 || view > *v { return Some(authorities); }
        }
        None
    }

    pub fn get_authorities_from_backend(&self, view: ViewNumber, block: <B::Header as HeaderT>::Number) -> Result<AuthorityList, HotstuffError> {
        let block_hash = self.client.hash(block)
            .map_err(|e| HotstuffError::ClientError(e.to_string()))?
            .ok_or(HotstuffError::ClientError(format!("Block {block} should have hash")))?;
        let header = self.client.header(block_hash)
            .map_err(|e| HotstuffError::ClientError(e.to_string()))?
            .ok_or(HotstuffError::ClientError(format!("Block {block}:{block_hash} should have header")))?;
        for consensus_log in find_consensus_logs::<B>(&header) {
            if let ConsensusLog::AuthoritiesChange(authorities) = consensus_log {
                return Ok(authorities.into_iter().map(|a| (a, 0)).collect());
            }
        }
        Err(HotstuffError::InvalidAuthority(format!("can't get authorities for view {view} block {block}")))
    }

    pub fn update_authorities(&mut self, view: ViewNumber, block: <B::Header as HeaderT>::Number, authority_list: AuthorityList) -> bool {
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

impl<B: BlockT, C: AuxStore + HeaderBackend<B>> ConsensusState<B, C> {
    pub fn new(
        client: Arc<C>,
        keystore: KeystorePtr,
        high_proposal: Option<Proposal<B>>,
        persistent_data: PersistentData<B>,
        disabled_authorities: Vec<AuthorityIndex>,
    ) -> Self {
        let mut authority = Authority {
            client,
            cache_size: 10,
            cache: Vec::new(),
            persistent_data,
        };
        authority.load_from_persistent_data().unwrap();
        let authorities = authority
            .authorities_from_cache(high_proposal.as_ref().map(|p| p.view).unwrap_or_default())
            .unwrap();
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
            authority,
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

    pub fn change_authorities(&mut self, view: ViewNumber, block: <B::Header as HeaderT>::Number, authority_list: AuthorityList) -> Result<(), HotstuffError> {
        let updated = self.authority.update_authorities(view, block, authority_list.clone());
        if updated {
            let local_authority_id = authority_list
                .iter()
                .enumerate()
                .find(|(_, (p, _))| {
                    self.keystore
                        .has_keys(&[(p.to_raw_vec(), HOTSTUFF_KEY_TYPE)])
                })
                .map(|(index, (p, _))| (index as u16, p.clone()));
            self.local_authority_id = local_authority_id.clone().map(|(_, p)| p.clone());
            self.local_authority_index = local_authority_id.map(|(id, _)| id);
            self.disabled_authorities.clear();
        }
        Ok(())
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

    pub fn authority_list(&self, view: ViewNumber) -> Result<&AuthorityList, HotstuffError> {
        self.authority.authorities_from_cache(view).ok_or(HotstuffError::InvalidAuthority(format!("can't get authorities for view {view}")))
    }

    pub fn authorities(&self, view: ViewNumber) -> Result<Vec<&AuthorityId>, HotstuffError> {
        self.authority_list(view)
            .map(|a| a.iter().map(|a| &a.0).collect())
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
        timeout.verify(self.authority_list(timeout.view)?)
    }

    pub fn verify_proposal(&self, proposal: &Proposal<B>) -> Result<(), HotstuffError> {
        // TODO SHOULD how process authority changed.
        match self.view_leader(proposal.view) {
            Some(leader) => if !proposal.author.eq(leader) {
                return Err(HotstuffError::WrongProposer);
            }
            None => return Err(HotstuffError::Other("Local empty authorities to verify proposal".into())),
        }
        proposal.verify(self.authority_list(proposal.view)?)?;
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

        vote.verify(self.authority_list(vote.view)?)
    }

    pub fn verify_tc(&self, tc: &TC<B>) -> Result<(), HotstuffError> {
        if tc.view < self.view {
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
        let authorities = self.authority_list(view).ok()?;
        if authorities.is_empty() {
            return None;
        }
        let leader_index = view % authorities.len() as ViewNumber;
        Some(&authorities[leader_index as usize].0)
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
