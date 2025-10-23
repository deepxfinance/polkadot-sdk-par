use std::{collections::HashSet, marker::PhantomData};
use std::fmt::{Debug, Display, Formatter};
use codec::{Decode, Encode};
use sp_core::Pair;
use sp_runtime::traits::{Block as BlockT, Hash as HashT, Header as HeaderT};
use sp_timestamp::Timestamp;

use hotstuff_primitives::{AuthorityId, AuthorityList, AuthorityPair, AuthoritySignature};
use crate::{primitives::{HotstuffError, ViewNumber}};
use crate::import::BlockInfo;

#[cfg(test)]
#[path = "tests/message_tests.rs"]
pub mod message_tests;

/// Quorum certificate for a block.
#[derive(Debug, Eq, Clone, Encode, Decode)]
pub struct QC<Block: BlockT> {
	/// Hotstuff proposal hash.
	pub proposal_hash: Block::Hash,
	pub view: ViewNumber,
	/// Public key signature pairs for the digest of QC.
	pub votes: Vec<(AuthorityId, AuthoritySignature)>,
	/// Timestamp of QC generate time.
	pub timestamp: Timestamp,
}

impl<B: BlockT> Default for QC<B> {
	fn default() -> Self {
		Self {
			proposal_hash: Default::default(),
			view: Default::default(),
			votes: Default::default(),
			timestamp: Default::default(),
		}
	}
}

impl<Block: BlockT> QC<Block> {
	pub fn digest(&self) -> Block::Hash {
		let mut data = self.proposal_hash.encode();
		data.append(&mut self.view.encode());

		<<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
	}

	// Add votes to QC.
	// TODO: Need check signature ?
	pub fn add_votes(&mut self, authority_id: AuthorityId, signature: AuthoritySignature) {
		self.votes.push((authority_id, signature));
	}

	// Verify if the number of votes in the QC has exceeded (2/3 + 1) of the total authorities.
	// We are currently not considering the weight of authorities.
	pub fn verify(&self, authorities: &AuthorityList) -> Result<(), HotstuffError> {
		if self.proposal_hash == Block::Hash::default() {
			return Ok(());
		}
		let mut used = HashSet::<AuthorityId>::new();
		let mut grant_votes = 0;

		for (authority_id, _) in self.votes.iter() {
			if used.contains(authority_id) {
				return Err(HotstuffError::AuthorityReuse(authority_id.clone()));
			}
			used.insert(authority_id.clone());
			grant_votes += 1;
		}

		if grant_votes <= (authorities.len() * 2 / 3) {
			return Err(HotstuffError::InsufficientQuorum);
		}

		for (voter, signature) in self.votes.iter() {
			if !AuthorityPair::verify(signature, self.digest(), voter) {
				return Err(HotstuffError::InvalidSignature(voter.clone()));
			}
		}
		Ok(())
	}
}

impl<Block: BlockT> PartialEq for QC<Block> {
	fn eq(&self, other: &Self) -> bool {
		self.proposal_hash == other.proposal_hash && self.view == other.view
	}
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

#[derive(Clone, Encode, Decode)]
pub struct Payload<Block: BlockT> {
	pub best_block: BlockInfo<Block>,
	pub block_number: <Block::Header as HeaderT>::Number,
	pub extrinsics_root: Block::Hash,
	pub extrinsic: Option<(Vec<Vec<Block::Extrinsic>>, Vec<Block::Extrinsic>)>,
	pub stage: ConsensusStage,
}

impl<Block: BlockT> Payload<Block> {
	pub fn digest(&self) -> Block::Hash {
		let mut data = self.best_block.encode();
		data.append(&mut self.block_number.encode());
		data.append(&mut self.extrinsics_root.encode());
		data.append(&mut self.stage.encode());
		<<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
	}

	pub fn empty() -> Self {
		Self {
			best_block: Default::default(),
			block_number: 0u32.into(),
			extrinsics_root: Block::Hash::default(),
			extrinsic: None,
			stage: ConsensusStage::default(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.block_number == 0u32.into() && self.extrinsics_root == Block::Hash::default() && self.extrinsic.is_none()
	}
	
	pub fn claim(&self) -> PayloadClaim<Block> {
		PayloadClaim {
			best_block: self.best_block.clone(),
			extrinsics_root: self.extrinsics_root.clone(),
		}
	}

	pub fn groups(&self) -> Vec<u32> {
		let mut groups = Vec::new();
		if let Some((multi, single)) = &self.extrinsic {
			groups.extend(multi.iter().map(|extrinsic| extrinsic.len() as u32).collect::<Vec<u32>>());
			groups.push(single.len() as u32);
		}
		groups
	}

	pub fn full_consensus(son: &Self, parent: &Self, grandpa: &Self) -> bool {
		if (son.stage, parent.stage, grandpa.stage) != (ConsensusStage::Commit, ConsensusStage::PreCommit, ConsensusStage::Prepare) {
			return false;
		}
		if son.block_number != parent.block_number || son.extrinsics_root != parent.extrinsics_root {
			return false;
		}
		if parent.block_number != grandpa.block_number || parent.extrinsics_root != grandpa.extrinsics_root {
			return false;
		}
		true
	}

	pub fn is_next(&self, parent: &Self) -> bool {
		if self.block_number < parent.block_number {
			return false;
		}
		if self.extrinsic.is_some() && self.stage == ConsensusStage::Prepare && self.block_number > parent.block_number {
			return true;
		}
		match (parent.stage, self.stage) {
			(ConsensusStage::Prepare, ConsensusStage::PreCommit)
			| (ConsensusStage::PreCommit, ConsensusStage::Commit) => {
				parent.block_number == self.block_number
					&& parent.extrinsics_root == self.extrinsics_root
					&& self.best_block == self.best_block
			},
			_ => false,
		}
	}
}

impl<Block: BlockT> Display for Payload<Block> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		if self.is_empty() {
			write!(f, "Empty")
		} else {
			write!(f, "{}:{}:{}:", self.stage, self.block_number, self.extrinsics_root)?;
			let groups = self.groups();
			if !groups.is_empty() {
				write!(f, "{groups:?}")?;
			} else {
				write!(f, "None")?;
			}
			write!(f, "({}:{})", self.best_block.number, self.best_block.hash)
		}
	}
}

impl<Block: BlockT> Debug for Payload<Block> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		if self.is_empty() {
			write!(f, "Empty")
		} else {
			write!(f, "{}:{}:{:?}:", self.stage, self.block_number, self.extrinsics_root)?;
			let groups = self.groups();
			if !groups.is_empty() {
				write!(f, "{groups:?}")?;
			} else {
				write!(f, "None")?;
			}
			write!(f, "({}:{:?})", self.best_block.number, self.best_block.hash)
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
	// Timestamp for proposal start generate time(also means this stage start time).
	pub timestamp: Timestamp,
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
		timestamp: Timestamp,
		author: AuthorityId,
		signature: Option<AuthoritySignature>,
	) -> Self {
		Proposal { qc, tc, payload, view, timestamp, author, signature }
	}

	pub fn parent_hash(&self) -> Block::Hash {
		self.qc.proposal_hash
	}
	
	pub fn part_digest(&self) -> Block::Hash {
		let mut data = self.author.encode();
		data.append(&mut self.view.encode());
		data.append(&mut self.timestamp.encode());
		data.append(&mut self.qc.proposal_hash.encode());
		data.append(&mut self.qc.timestamp.encode());
		<<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
	}

	pub fn digest(&self) -> Block::Hash {
		let mut data = self.part_digest().encode();
		data.append(&mut self.payload.digest().encode());
		<<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
	}

	pub fn verify(&self, authorities: &AuthorityList) -> Result<(), HotstuffError> {
		authorities
			.iter()
			.find(|authority| authority.0 == self.author)
			.ok_or(HotstuffError::UnknownAuthority(self.author.to_owned()))?;

		self.signature.as_ref().ok_or(HotstuffError::NullSignature).and_then(|signature| {
			if !AuthorityPair::verify(signature, self.digest(), &self.author) {
				return Err(HotstuffError::InvalidSignature(self.author.to_owned()));
			}
			Ok(())
		})?;

		if self.qc != QC::<Block>::default() {
			self.qc.verify(authorities)?;
		}

		Ok(())
	}
}

// Vote for a Proposal
#[derive(Debug, Clone, Encode, Decode)]
pub struct Vote<Block: BlockT> {
	pub proposal_hash: Block::Hash,
	pub view: ViewNumber,
	pub voter: AuthorityId,
	pub signature: Option<AuthoritySignature>,
}

impl<Block: BlockT> Vote<Block> {
	pub fn new(proposal_hash: Block::Hash, proposal_view: ViewNumber, voter: AuthorityId) -> Self {
		Self { proposal_hash, view: proposal_view, voter, signature: None }
	}

	pub fn digest(&self) -> Block::Hash {
		let mut data = self.proposal_hash.encode();
		data.append(&mut self.view.encode());

		<<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
	}

	pub fn verify(&self, authorities: &AuthorityList) -> Result<(), HotstuffError> {
		authorities
			.iter()
			.find(|authority| authority.0 == self.voter)
			.ok_or(HotstuffError::UnknownAuthority(self.voter.to_owned()))?;

		self.signature.as_ref().ok_or(HotstuffError::NullSignature).and_then(|signature| {
			if !AuthorityPair::verify(signature, self.digest(), &self.voter) {
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

	pub fn verify(&self, authorities: &AuthorityList) -> Result<(), HotstuffError> {
		authorities
			.iter()
			.find(|authority| authority.0 == self.voter)
			.ok_or(HotstuffError::UnknownAuthority(self.voter.to_owned()))?;

		self.signature.as_ref().ok_or(HotstuffError::NullSignature).and_then(|signature| {
			if !AuthorityPair::verify(signature, self.digest(), &self.voter) {
				return Err(HotstuffError::InvalidSignature(self.voter.to_owned()));
			}
			Ok(())
		})?;

		if self.high_qc != QC::<Block>::default() {
			self.high_qc.verify(authorities)?;
		}
		Ok(())
	}
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct TC<Block: BlockT> {
	// timeout view number which have consensus.
	pub view: ViewNumber,
	// votes for time out view numbers
	pub votes: Vec<(AuthorityId, AuthoritySignature, ViewNumber)>,
	// most high qc in votes.
	pub high_qc: QC<Block>,
}

impl<Block: BlockT> TC<Block> {
	pub fn digest(&self) -> Block::Hash {
		let data = self.view.encode();
		<<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
	}

	pub fn verify(&self, authorities: &AuthorityList) -> Result<(), HotstuffError> {
		let mut used = HashSet::<AuthorityId>::new();
		let mut grant_votes = 0;

		for (authority_id, _, _) in self.votes.iter() {
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

		for (voter, signature, high_qc_view) in self.votes.iter() {
			let mut data = self.view.encode();
			data.append(&mut high_qc_view.encode());
			let digest = <<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data);
			if !AuthorityPair::verify(signature, digest, voter) {
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
	Propose(Proposal<B>),
	Vote(Vote<B>),
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
pub struct PayloadClaim<Block: BlockT> {
	pub best_block: BlockInfo<Block>,
	pub extrinsics_root: Block::Hash,
}

impl<Block: BlockT> PayloadClaim<Block> {
	pub fn empty() -> Self {
		Self {
			best_block: Default::default(),
			extrinsics_root: Default::default(),
		}
	}

	pub fn digest(
		&self,
		block_number: <Block::Header as HeaderT>::Number,
		stage: ConsensusStage,
	) -> Block::Hash {
		let mut data = self.best_block.encode();
		data.append(&mut block_number.encode());
		data.append(&mut self.extrinsics_root.encode());
		data.append(&mut stage.encode());
		<<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
	}
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct ProposalClaim<B: BlockT> {
	pub part_digest: B::Hash,
	pub qc: QC<B>,
}

impl<B: BlockT> ProposalClaim<B> {
	pub fn empty() -> Self {
		Self {
			part_digest: Default::default(),
			qc: Default::default(),
		}
	}

	pub fn verify(
		&self, 
		authorities: &AuthorityList,
		payload_claim_digest: B::Hash,
	) -> Result<(), HotstuffError> {
		if self.qc.proposal_hash == B::Hash::default() {
			return Err(HotstuffError::VerifyClaim("Claim qc should not be default".into()));
		}
		let proposal_digest = {
			let mut data = self.part_digest.encode();
			data.append(&mut payload_claim_digest.encode());
			<<B::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
		};
		if proposal_digest != self.qc.proposal_hash {
			return Err(HotstuffError::VerifyClaim("Incorrect proposal hash".into()));
		}
		self.qc.verify(authorities)
	}
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct BlockCommit<B: BlockT> {
	pub block_number: <B::Header as HeaderT>::Number,
	pub payload_claim: PayloadClaim<B>,
	pub prepare: ProposalClaim<B>,
	pub precommit: ProposalClaim<B>,
	pub commit: ProposalClaim<B>,
}

impl<B: BlockT> BlockCommit<B> {
	pub fn empty() -> Self {
		Self {
			block_number: 0u32.into(),
			payload_claim: PayloadClaim::empty(),
			prepare: ProposalClaim::empty(),
			precommit: ProposalClaim::empty(),
			commit: ProposalClaim::empty(),
		}
	}

	pub fn generate(prepare: (&Proposal<B>, QC<B>), precommit: (&Proposal<B>, QC<B>), commit: (&Proposal<B>, QC<B>)) -> Option<Self> {
		if prepare.0.payload.stage != ConsensusStage::Prepare || prepare.0.payload.extrinsic.is_none() {
			return None;
		}
		if precommit.0.payload.stage != ConsensusStage::PreCommit {
			return None;
		}
		if commit.0.payload.stage != ConsensusStage::Commit {
			return None;
		}
		let prepare_payload_claim = prepare.0.payload.claim();
		let precommit_payload_claim = precommit.0.payload.claim();
		let payload_claim = commit.0.payload.claim();
		if prepare_payload_claim != precommit_payload_claim  || precommit_payload_claim != payload_claim {
			return None;
		}
		let block_number = commit.0.payload.block_number;
		let prepare = ProposalClaim {
			part_digest: prepare.0.part_digest(),
			qc: prepare.1
		};
		let precommit = ProposalClaim {
			part_digest: precommit.0.part_digest(),
			qc: precommit.1
		};
		let commit = ProposalClaim {
			part_digest: commit.0.part_digest(),
			qc: commit.1
		};
		
		Some(BlockCommit { block_number, payload_claim, prepare, precommit, commit })
	}

	pub fn verify_qc(&self, authorities: &AuthorityList) -> Result<(), HotstuffError> {
		self.prepare.qc.verify(authorities)?;
		self.precommit.qc.verify(authorities)?;
		self.commit.qc.verify(authorities)?;
		Ok(())
	}
	
	pub fn verify(
		&self,
		authorities: &AuthorityList,
		block_number: <B::Header as HeaderT>::Number,
	) -> Result<(), HotstuffError> {
		if block_number != self.block_number {
			return Err(HotstuffError::VerifyClaim("Incorrect block number".into()));
		}
		self.prepare.verify(authorities, self.payload_claim.digest(self.block_number, ConsensusStage::Prepare))?;
		self.precommit.verify(authorities, self.payload_claim.digest(self.block_number, ConsensusStage::PreCommit))?;
		self.commit.verify(authorities, self.payload_claim.digest(self.block_number, ConsensusStage::Commit))?;
		Ok(())
	}

	pub fn prepare_time(&self) -> &Timestamp {
		&self.prepare.qc.timestamp
	}

	pub fn commit_time(&self) -> &Timestamp {
		&self.commit.qc.timestamp
	}

	pub fn base_block(&self) -> &BlockInfo<B> {
		&self.payload_claim.best_block
	}
}
