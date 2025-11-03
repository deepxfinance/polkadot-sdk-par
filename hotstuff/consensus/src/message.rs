use std::{collections::HashSet, marker::PhantomData};
use std::fmt::{Debug, Display, Formatter};
use codec::{Decode, Encode};
use w3f_bls::TinyBLS381;
use hotstuff_primitives::AuthorityWeight;
use sp_core::{ByteArray, Pair};
use sp_runtime::traits::{Block as BlockT, Hash as HashT, Header as HeaderT};
use sp_timestamp::Timestamp;

use crate::{AuthorityId, AuthorityList, AuthorityPair, AuthoritySignature};
use crate::{primitives::{HotstuffError, ViewNumber}};
use crate::aggregator::AggregateSignature;

#[cfg(test)]
#[path = "tests/message_tests.rs"]
pub mod message_tests;

pub fn extend_message(index: u16, msg: &[u8]) -> Vec<u8> {
    [index.to_le_bytes().to_vec(), msg.to_vec()].concat()
}

pub fn find_authority_index<'a>(authorities: &'a AuthorityList, author: &AuthorityId) -> Option<(u16, &'a (AuthorityId, AuthorityWeight))> {
    authorities
        .iter()
        .enumerate()
        .find(|(_, authority)| &authority.0 == author)
        .map(|(i, a)| (i as u16, a))
}

#[derive(Clone, Encode, Decode, Debug, Copy, Default, PartialEq)]
pub enum ConsensusStage {
    #[default]
    Prepare,
    PreCommit,
    Commit,
}

impl ConsensusStage {
    pub fn next(&self) -> Option<Self> {
        match self {
            Self::Prepare => Some(Self::PreCommit),
            Self::PreCommit => Some(Self::Commit),
            Self::Commit => None,
        }
    }

    pub fn finish(&self) -> bool {
        self == &Self::Commit
    }
}

impl Display for ConsensusStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Prepare => write!(f, "Prepare"),
            Self::PreCommit => write!(f, "PreCommit"),
            Self::Commit => write!(f, "Commit"),
        }
    }
}

#[derive(Clone, Copy, Debug, Encode, Decode)]
pub enum ProposalKey<B: BlockT> {
    View(ViewNumber),
    Digest(B::Hash),
}

impl<B: BlockT> ProposalKey<B> {
    pub fn view(view: ViewNumber) -> ProposalKey<B> {
        Self::View(view)
    }

    pub fn digest(digest: B::Hash) -> ProposalKey<B> {
        Self::Digest(digest)
    }
}

#[derive(Clone, Encode, Decode)]
pub struct NewBlock<Block: BlockT> {
    pub best_block: <Block::Header as HeaderT>::Number,
    pub extrinsic_block: ExtrinsicBlock<Block>,
    pub extrinsic: Vec<Vec<Vec<Block::Extrinsic>>>,
}

impl<Block: BlockT> NewBlock<Block> {
    pub fn digest(&self) -> Block::Hash {
        self.claim().digest()
    }

    pub fn empty() -> Self {
        Self {
            best_block: 0u32.into(),
            extrinsic_block: ExtrinsicBlock{
                number: 0u32.into(),
                extrinsics_root: Block::Hash::default(),
            },
            extrinsic: Vec::new(),
        }
    }

    pub fn block_number(&self) -> <Block::Header as HeaderT>::Number {
        self.extrinsic_block.number
    }

    pub fn extrinsics_root(&self) -> Block::Hash {
        self.extrinsic_block.extrinsics_root
    }

    pub fn claim(&self) -> BlockClaim<Block> {
        BlockClaim {
            best_block: self.best_block,
            extrinsic_block: self.extrinsic_block.clone(),
        }
    }

    pub fn groups(&self) -> Vec<Vec<u32>> {
        let mut groups = Vec::new();
        for rounds in &self.extrinsic {
            groups.push(rounds.iter().map(|extrinsic| extrinsic.len() as u32).collect::<Vec<u32>>());
        }
        groups
    }
}

impl<Block: BlockT> Display for NewBlock<Block> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:", self.extrinsic_block)?;
        let groups = self.groups();
        if !groups.is_empty() {
            write!(f, "{groups:?}")?;
        } else {
            write!(f, "None")?;
        }
        write!(f, "({})", self.best_block)
    }
}

impl<Block: BlockT> Debug for NewBlock<Block> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}:", self.extrinsic_block)?;
        let groups = self.groups();
        if !groups.is_empty() {
            write!(f, "{groups:?}")?;
        } else {
            write!(f, "None")?;
        }
        write!(f, "({})", self.best_block)
    }
}

// Vote for a Proposal
#[derive(Debug, Clone, Encode, Decode)]
pub struct Vote<Block: BlockT> {
    pub view: ViewNumber,
    pub proposal_hash: Block::Hash,
    pub stage: ConsensusStage,
    pub voter: AuthorityId,
    pub signature: Option<AuthoritySignature>,
}

impl<Block: BlockT> Vote<Block> {
    pub fn new(proposal_hash: Block::Hash, view: ViewNumber, stage: ConsensusStage, voter: AuthorityId) -> Self {
        Self { proposal_hash, view, stage, voter, signature: None }
    }

    pub fn digest(&self) -> Block::Hash {
        self.proposal_hash
    }

    pub fn verify(&self, authorities: &AuthorityList) -> Result<(), HotstuffError> {
        let (index, _) = find_authority_index(authorities, &self.voter)
            .ok_or(HotstuffError::UnknownAuthority(self.voter.to_owned()))?;
        let message =  extend_message(index, self.digest().as_ref());
        self.signature.as_ref().ok_or(HotstuffError::NullSignature).and_then(|signature| {
            if !AuthorityPair::verify(signature, message, &self.voter) {
                return Err(HotstuffError::InvalidSignature(self.voter.to_owned()));
            }
            Ok(())
        })
    }
}

// Timeout notification
#[derive(Debug, Clone, Encode, Decode)]
pub struct Timeout<Block: BlockT> {
    // The hightest QC of local node.
    pub high_qc: QC<Block>,
    // The view of local node at timeout.
    pub view: ViewNumber,
    pub voter: AuthorityId,
    pub signature: Option<AuthoritySignature>,
}

impl<Block: BlockT> Timeout<Block> {
    pub fn digest(&self) -> Block::Hash {
        let mut data = self.view.encode();
        data.append(&mut self.high_qc.view.encode());
        <<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
    }

    pub fn verify(&self, authorities: &AuthorityList, high_qc_authorities: &AuthorityList) -> Result<(), HotstuffError> {
        let (index, _) = find_authority_index(authorities, &self.voter)
            .ok_or(HotstuffError::UnknownAuthority(self.voter.to_owned()))?;
        let message = extend_message(index, self.digest().as_ref());
        self.signature.as_ref().ok_or(HotstuffError::NullSignature).and_then(|signature| {
            if !AuthorityPair::verify(signature, message, &self.voter) {
                return Err(HotstuffError::InvalidSignature(self.voter.to_owned()));
            }
            Ok(())
        })?;

        if let Err(e) = self.high_qc.verify(high_qc_authorities) {
            println!("verify timeout high_qc view {} authorities: {high_qc_authorities:?}", self.high_qc.view);
            return Err(e)
        }
        Ok(())
    }
}

#[derive(Clone, Encode, Decode, PartialEq)]
pub struct ExtrinsicBlock<B: BlockT> {
    pub number: <B::Header as HeaderT>::Number,
    pub extrinsics_root: B::Hash,
}

impl<B: BlockT> ExtrinsicBlock<B> {
    pub fn empty() -> Self {
        Self {
            number: 0u32.into(),
            extrinsics_root: Default::default(),
        }
    }
}

impl<Block: BlockT> Display for ExtrinsicBlock<Block> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.number, self.extrinsics_root)
    }
}

impl<Block: BlockT> Debug for ExtrinsicBlock<Block> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{:?}", self.number, self.extrinsics_root)
    }
}

impl<B: BlockT> From<(<B::Header as HeaderT>::Number, B::Hash)> for ExtrinsicBlock<B> {
    fn from((number, extrinsics_root): (<B::Header as HeaderT>::Number, B::Hash)) -> Self {
        Self { number, extrinsics_root }
    }
}

#[derive(Clone, Encode, Decode)]
pub struct Payload<Block: BlockT> {
    pub stage: ConsensusStage,
    pub block: ExtrinsicBlock<Block>,
    pub extrinsics: Option<Vec<Vec<Vec<Block::Extrinsic>>>>,
}

impl<Block: BlockT> Payload<Block> {
    pub fn empty() -> Self {
        Self {
            stage: ConsensusStage::Commit,
            block: ExtrinsicBlock::empty(),
            extrinsics: None,
        }
    }

    pub fn block_number(&self) -> <Block::Header as HeaderT>::Number {
        self.block.number
    }

    pub fn extrinsic(&self) -> Option<&Vec<Vec<Vec<Block::Extrinsic>>>> {
        self.extrinsics.as_ref()
    }

    pub fn encode_data(&self) -> Vec<u8> {
        (self.stage, self.block.clone()).encode()
    }

    pub fn next(&self) -> Option<Self> {
        match &self.stage {
            ConsensusStage::Prepare => Some(Self {
                stage: ConsensusStage::PreCommit,
                block: self.block.clone(),
                extrinsics: None,
            }),
            ConsensusStage::PreCommit => Some(Self {
                stage: ConsensusStage::Commit,
                block: self.block.clone(),
                extrinsics: None,
            }),
            _ => None,
        }
    }

    pub fn full_consensus(prepare: &Self, precommit: &Self, commit: &Self) -> bool {
        if prepare.stage != ConsensusStage::Prepare || precommit.stage != ConsensusStage::PreCommit
            || commit.stage != ConsensusStage::Commit
        {
            return false;
        }
        if prepare.block != precommit.block || precommit.block != precommit.block {
            return false;
        }
        true
    }

    pub fn groups(&self) -> Option<Vec<Vec<u32>>> {
        self.extrinsics.as_ref().map(|groups| {
            groups
                .iter()
                .map(|g|
                    g.iter().map(|extrinsic| extrinsic.len() as u32).collect::<Vec<u32>>()
                )
                .collect::<Vec<Vec<u32>>>()

        })
    }
}

impl<Block: BlockT> Display for Payload<Block> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.stage, self.block)
    }
}

impl<Block: BlockT> Debug for Payload<Block> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(groups) = self.groups() {
            write!(f, "{}:{}:{groups:?}", self.stage, self.block)
        } else {
            write!(f, "{}:{}", self.stage, self.block)
        }
    }
}

// Hotstuff Proposal
#[derive(Debug, Clone, Encode, Decode)]
pub struct Proposal<Block: BlockT> {
    // QC of parent proposal.
    pub qc: QC<Block>,
    pub tc: Option<TC<Block>>,
    pub payload: Payload<Block>,
    pub view: ViewNumber,
    // The authority id of current hotstuff block author,
    // which is not the origin block producer.
    pub author: AuthorityId,
    // Signature of proposal digest.
    pub signature: Option<AuthoritySignature>,
}

impl<Block: BlockT> Proposal<Block> {
    pub fn new(
        qc: QC<Block>,
        tc: Option<TC<Block>>,
        payload: Payload<Block>,
        view: ViewNumber,
        author: AuthorityId,
        signature: Option<AuthoritySignature>,
    ) -> Self {
        Proposal { qc, tc, payload, view, author, signature }
    }

    pub fn empty() -> Self {
        let length = <AuthorityId as ByteArray>::LEN;
        let empty_authority = AuthorityId::from_slice(&vec![1u8; length]).unwrap();
        Self::new(QC::default(), None, Payload::empty(), 0, empty_authority, None)
    }

    pub fn digest(&self) -> Block::Hash {
        let mut data = self.view.encode();
        data.append(&mut self.qc.digest().encode());
        data.append(&mut self.payload.encode_data());
        <<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
    }

    pub fn verify(&self, authorities: &AuthorityList, qc_authorities: &AuthorityList) -> Result<(), HotstuffError> {
        let (index, _) = find_authority_index(authorities, &self.author)
            .ok_or(HotstuffError::UnknownAuthority(self.author.to_owned()))?;
        let message =  extend_message(index, self.digest().as_ref());
        self.signature.as_ref().ok_or(HotstuffError::NullSignature).and_then(|signature| {
            if !AuthorityPair::verify(signature, message, &self.author) {
                return Err(HotstuffError::InvalidSignature(self.author.to_owned()));
            }
            Ok(())
        })?;

        self.qc.verify(qc_authorities)
    }

    pub fn parent_hash(&self) -> Block::Hash {
        self.qc.proposal_hash
    }
}

/// Quorum certificate for a block.
#[derive(Debug, Clone, Encode, Decode)]
pub struct QC<Block: BlockT> {
    pub view: ViewNumber,
    /// proposal hash.
    pub proposal_hash: Block::Hash,
    /// Aggregated BLS signature the digest of QC.
    pub signature: AggregateSignature,
    pub stage: ConsensusStage,
    pub timestamp: Timestamp,
}

impl<Block: BlockT> QC<Block> {
    pub fn digest(&self) -> Block::Hash {
        // QC digest should be same with proposal_hash
        self.proposal_hash
    }

    pub fn higher_than(&self, other: &Self) -> bool {
        if self.view > other.view {
            true
        } else if self.view == other.view {
            match (other.stage, self.stage) {
                (ConsensusStage::Prepare, ConsensusStage::PreCommit)
                | (ConsensusStage::PreCommit, ConsensusStage::Commit) => true,
                _ => false,
            }
        } else {
            false
        }
    }

    // Verify if the number of votes in the QC has exceeded (2/3 + 1) of the total authorities.
    // We are currently not considering the weight of authorities.
    pub fn verify(&self, authorities: &AuthorityList) -> Result<(), HotstuffError> {
        if self.proposal_hash == Block::Hash::default() && self.view == 0 {
            return Ok(());
        }
        if self.signature.mask.count_ones() as usize <= (authorities.len() * 2 / 3) {
            return Err(HotstuffError::InsufficientQuorum);
        }
        self.signature.verify::<TinyBLS381>(self.digest().as_ref(), &authorities)
    }
}

impl<Block: BlockT> Default for QC<Block> {
    fn default() -> Self {
        Self {
            view: 0,
            proposal_hash: BlockCommit::<Block>::empty().commit_hash(),
            signature: AggregateSignature::default(),
            stage: ConsensusStage::Prepare,
            timestamp: Default::default(),
        }
    }
}

impl<Block: BlockT> PartialEq for QC<Block> {
    fn eq(&self, other: &Self) -> bool {
        self.proposal_hash == other.proposal_hash
            && self.view == other.view
            && self.stage == other.stage
    }
}

/// Commit Quorum certificate for a block.
#[derive(Debug, Clone, Encode, Decode)]
pub struct CommitQC<Block: BlockT> {
    pub qc: QC<Block>,
    pub author: AuthorityId,
    pub signature: Option<AuthoritySignature>,
}

impl<Block: BlockT> CommitQC<Block> {
    pub fn new(qc: QC<Block>, author: AuthorityId) -> Self {
        Self { author, qc, signature: None }
    }

    pub fn digest(&self) -> Block::Hash {
        self.qc.proposal_hash
    }

    pub fn verify(&self, authorities: &AuthorityList) -> Result<(), HotstuffError> {
        let (index, _) = find_authority_index(authorities, &self.author)
            .ok_or(HotstuffError::UnknownAuthority(self.author.to_owned()))?;
        let message = extend_message(index, self.digest().as_ref());
        self.signature.as_ref().ok_or(HotstuffError::NullSignature).and_then(|signature| {
            if !AuthorityPair::verify(signature, message, &self.author) {
                return Err(HotstuffError::InvalidSignature(self.author.to_owned()));
            }
            Ok(())
        })?;
        self.qc.verify(authorities)
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct TC<Block: BlockT> {
    // timeout view number which have consensus.
    pub view: ViewNumber,
    // votes for time out view numbers
    pub votes: Vec<(u16, AuthoritySignature, ViewNumber)>,
    // most high qc in votes.
    pub high_qc: QC<Block>,
}

impl<Block: BlockT> TC<Block> {
    pub fn verify(&self, authorities: &AuthorityList) -> Result<(), HotstuffError> {
        let mut used = HashSet::<AuthorityId>::new();
        let mut grant_votes = 0;

        for (index, _, _) in self.votes.iter() {
            if *index as usize >= authorities.len() {
                return Err(HotstuffError::NotAuthority);
            }
            let authority_id = &authorities[*index as usize].0;
            if used.contains(authority_id) {
                return Err(HotstuffError::AuthorityReuse(authority_id.clone()));
            }
            used.insert(authority_id.clone());
            grant_votes += 1;
        }

        if grant_votes <= (authorities.len() * 2 / 3) {
            return Err(HotstuffError::InsufficientQuorum);
        }
        let mut final_high_qc_view = 0;

        for (index, signature, high_qc_view) in self.votes.iter() {
            let voter = &authorities[*index as usize].0;
            let mut data = self.view.encode();
            data.append(&mut high_qc_view.encode());
            let digest = <<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data);
            let message =  extend_message(*index, digest.as_ref());
            if !AuthorityPair::verify(signature, message, voter) {
                return Err(HotstuffError::InvalidSignature(voter.clone()));
            }
            final_high_qc_view = final_high_qc_view.max(*high_qc_view);
        }
        if final_high_qc_view != self.high_qc.view {
            return Err(HotstuffError::InvalidTC);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct PeerAuthority<Block> {
    pub peer_id: String,
    pub authority: AuthorityId,
    pub signature: Option<AuthoritySignature>,
    pub phantom: PhantomData<Block>,
}

impl<Block: BlockT> PeerAuthority<Block> {
    pub fn digest(&self) -> Block::Hash {
        let mut data = self.peer_id.encode();
        data.append(&mut self.authority.encode());
        <<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
    }

    pub fn verify(&self) -> Result<(), HotstuffError> {
        let digest = self.digest();
        match &self.signature {
            Some(signature) => if !AuthorityPair::verify(signature, digest, &self.authority) {
                Err(HotstuffError::InvalidSignature(self.authority.clone()))
            } else {
                Ok(())
            },
            None => Err(HotstuffError::NullSignature),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum ProposalReq<Block: BlockT> {
    Range(ProposalKey<Block>, ProposalKey<Block>),
    Keys(Vec<ProposalKey<Block>>),
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct ProposalRequest<Block: BlockT> {
    pub authority: AuthorityId,
    pub requests: ProposalReq<Block>,
    pub signature: Option<AuthoritySignature>,
}

impl<Block: BlockT> ProposalRequest<Block> {
    pub fn digest(&self) -> Block::Hash {
        let data = self.requests.encode();
        <<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
    }

    pub fn verify(&self, authorities: &AuthorityList) -> Result<(), HotstuffError> {
        if authorities.iter().find(|(a, _)| a == &self.authority).is_none() {
            return Err(HotstuffError::NotAuthority);
        }
        let digest = self.digest();
        match &self.signature {
            Some(signature) => if !AuthorityPair::verify(signature, digest, &self.authority) {
                Err(HotstuffError::InvalidSignature(self.authority.clone()))
            } else {
                Ok(())
            },
            None => Err(HotstuffError::NullSignature),
        }
    }
}

#[derive(Debug, Encode, Decode)]
pub enum ConsensusMessage<B: BlockT> {
    Greeting(PeerAuthority<B>),
    GetProposal(ProposalRequest<B>),
    ResponseProposal(Vec<Proposal<B>>),
    Propose(Proposal<B>),
    Vote(Vote<B>),
    CommitQC(CommitQC<B>),
    Timeout(Timeout<B>),
    TC(TC<B>),
    SyncRequest(B::Hash, AuthorityId),
    BlockImport(B::Header),
}

impl<Block: BlockT> ConsensusMessage<Block> {
    pub fn gossip_topic() -> Block::Hash {
        // TODO maybe use Lazy then just call hash once.
        <<Block::Header as HeaderT>::Hashing as HashT>::hash(b"hotstuff/consensus")
    }
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub struct BlockClaim<Block: BlockT> {
    pub best_block: <Block::Header as HeaderT>::Number,
    pub extrinsic_block: ExtrinsicBlock<Block>,
}

impl<Block: BlockT> BlockClaim<Block> {
    pub fn empty() -> Self {
        Self {
            best_block: 0u32.into(),
            extrinsic_block: ExtrinsicBlock::empty(),
        }
    }

    /// Should be same with [NewBlock::digest]
    pub fn digest(&self) -> Block::Hash {
        let mut data = self.best_block.encode();
        data.append(&mut self.extrinsic_block.encode());
        <<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct BlockCommit<B: BlockT> {
    /// parent block commit hash
    parent_commit: B::Hash,
    view: ViewNumber,
    extrinsic_block: ExtrinsicBlock<B>,
    precommit: B::Hash,
    signature: AggregateSignature,
    timestamp: Timestamp,
}

impl<B: BlockT> BlockCommit<B> {
    pub fn empty() -> Self {
        Self {
            parent_commit: Default::default(),
            view: Default::default(),
            extrinsic_block: ExtrinsicBlock::empty(),
            precommit: Default::default(),
            signature: Default::default(),
            timestamp: Default::default(),
        }
    }

    pub fn parent_commit_hash(&self) -> B::Hash {
        self.parent_commit
    }

    pub fn view(&self) -> ViewNumber {
        self.view
    }

    pub fn commit_hash(&self) -> B::Hash {
        if self.extrinsic_block.number == 0u32.into() {
            return B::Hash::default();
        }
        // We should calculate final commit hash to ensure the extrinsics_root is correct.
        let mut data = self.view.encode();
        data.append(&mut self.precommit.encode());
        data.append(&mut (ConsensusStage::Commit, self.extrinsic_block.clone()).encode());
        <<B::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
    }

    pub fn generate(prepare: (&Proposal<B>, QC<B>), precommit: (&Proposal<B>, QC<B>), commit: (&Proposal<B>, QC<B>)) -> Option<Self> {
        if prepare.1.proposal_hash != prepare.0.digest()
            || precommit.1.proposal_hash != precommit.0.digest()
            || commit.1.proposal_hash != commit.0.digest() {
            return None;
        }
        if commit.0.qc.proposal_hash != precommit.1.proposal_hash
            || precommit.0.qc.proposal_hash != prepare.1.proposal_hash {
            return None;
        }
        let block_commit = BlockCommit {
            parent_commit: prepare.0.qc.proposal_hash,
            view: commit.1.view,
            extrinsic_block: commit.0.payload.block.clone(),
            precommit: precommit.1.proposal_hash,
            signature: commit.1.signature,
            timestamp: commit.1.timestamp,
        };
        Some(block_commit)
    }

    pub fn verify(
        &self,
        authorities: &AuthorityList,
        block_number: <B::Header as HeaderT>::Number,
        extrinsics_root: B::Hash,
    ) -> Result<(), HotstuffError> {
        if block_number != self.block_number() {
            return Err(HotstuffError::VerifyClaim(format!("Incorrect block number {block_number}/{}", self.block_number()).into()));
        }
        if extrinsics_root != self.extrinsics_root() {
            return Err(HotstuffError::VerifyClaim(format!("Incorrect block {block_number} extrinsics_root: {extrinsics_root}/{}", self.extrinsics_root()).into()));
        }
        let commit_qc: QC<B> = QC {
            view: self.view,
            proposal_hash: self.commit_hash(),
            signature: self.signature.clone(),
            stage: ConsensusStage::Commit,
            timestamp: Timestamp::default(),
        };
        commit_qc.verify(authorities)
    }

    pub fn block_number(&self) -> <B::Header as HeaderT>::Number {
        self.extrinsic_block.number
    }

    pub fn extrinsics_root(&self) -> B::Hash {
        self.extrinsic_block.extrinsics_root
    }

    pub fn commit_time(&self) -> &Timestamp {
        &self.timestamp
    }
}
