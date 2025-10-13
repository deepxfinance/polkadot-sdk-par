use std::{collections::HashSet, marker::PhantomData};
use std::fmt::{Debug, Display, Formatter};
use codec::{Decode, Encode};
use sp_core::Pair;
use sp_runtime::traits::{Block as BlockT, Hash as HashT, Header as HeaderT};
use sp_timestamp::Timestamp;

use hotstuff_primitives::{AuthorityId, AuthorityList, AuthorityPair, AuthoritySignature};
use crate::{import::BlockInfo, primitives::{HotstuffError::{self, *}, ViewNumber}};

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
				return Err(AuthorityReuse(authority_id.clone()));
			}
			used.insert(authority_id.clone());
			grant_votes += 1;
		}

		if grant_votes <= (authorities.len() * 2 / 3) {
			log::warn!(target: "Hotstuff", "qc.verify votes: {} grant_votes: {grant_votes} authorities: {} proposal_hash: {}", self.votes.len(), authorities.len(), self.proposal_hash);
			return Err(InsufficientQuorum);
		}

		for (voter, signature) in self.votes.iter() {
			if !AuthorityPair::verify(signature, self.digest(), voter) {
				return Err(InvalidSignature(voter.clone()));
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
pub struct NewBlock<Block: BlockT> {
	pub block_number: <Block::Header as HeaderT>::Number,
	pub block_hash: Block::Hash,
	pub stage: ConsensusStage,
}

impl<Block: BlockT> NewBlock<Block> {
	pub fn empty() -> Self {
		NewBlock {
			block_number: 0u32.into(),
			block_hash: Block::Hash::default(),
			stage: ConsensusStage::default(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.block_number == 0u32.into() && self.block_hash == Block::Hash::default()
	}

	pub fn full_consensus(son: &Self, parent: &Self, grandpa: &Self) -> bool {
		if (son.stage, parent.stage, grandpa.stage) != (ConsensusStage::Commit, ConsensusStage::PreCommit, ConsensusStage::Prepare) {
			return false;
		}
		if son.block_number != parent.block_number || son.block_hash != parent.block_hash {
			return false;
		}
		if parent.block_number != grandpa.block_number || parent.block_hash != grandpa.block_hash {
			return false;
		}
		true
	}
}

impl<Block: BlockT> Display for NewBlock<Block> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		if self.is_empty() {
			write!(f, "Empty")
		} else {
			write!(f, "{}:{}:{}", self.stage, self.block_number, self.block_hash)
		}
	}
}

impl<Block: BlockT> Debug for NewBlock<Block> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self)
	}
}

#[derive(Clone, Encode, Decode)]
pub struct NextBlock<Block: BlockT> {
	pub block_number: <Block::Header as HeaderT>::Number,
	pub hash: Block::Hash,
	pub extrinsic: Option<(Vec<Vec<Block::Extrinsic>>, Vec<Block::Extrinsic>)>,
	pub stage: ConsensusStage,
}

impl<Block: BlockT> NextBlock<Block> {
	pub fn empty() -> Self {
		Self {
			block_number: 0u32.into(),
			hash: Block::Hash::default(),
			extrinsic: None,
			stage: ConsensusStage::default(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.block_number == 0u32.into() && self.hash == Block::Hash::default() && self.extrinsic.is_none()
	}

	pub fn full_consensus(son: &Self, parent: &Self, grandpa: &Self) -> bool {
		if (son.stage, parent.stage, grandpa.stage) != (ConsensusStage::Commit, ConsensusStage::PreCommit, ConsensusStage::Prepare) {
			return false;
		}
		if son.block_number != parent.block_number || son.hash != parent.hash {
			return false;
		}
		if parent.block_number != grandpa.block_number || parent.hash != grandpa.hash {
			return false;
		}
		true
	}
}

impl<Block: BlockT> Display for NextBlock<Block> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		if self.is_empty() {
			write!(f, "Empty")
		} else {
			write!(f, "{}:{}:{}:", self.stage, self.block_number, self.hash)?;
			if let Some((multi, single)) = &self.extrinsic {
				let mut length: Vec<_> = multi.iter().map(|extrinsic| extrinsic.len()).collect();
				length.push(single.len());
				write!(f, "{length:?}")
			} else {
				write!(f, "None")
			}
		}
	}
}

impl<Block: BlockT> Debug for NextBlock<Block> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self)
	}
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Payload<Block: BlockT> {
	pub new_block: NewBlock<Block>,
	pub next_block: NextBlock<Block>,
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

	pub fn digest(&self) -> Block::Hash {
		let mut data = self.author.encode();
		data.append(&mut self.payload.encode());
		data.append(&mut self.view.encode());
		data.append(&mut self.timestamp.encode());
		data.append(&mut self.qc.proposal_hash.encode());
		data.append(&mut self.qc.timestamp.encode());

		<<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data)
	}

	pub fn verify(&self, authorities: &AuthorityList) -> Result<(), HotstuffError> {
		authorities
			.iter()
			.find(|authority| authority.0 == self.author)
			.ok_or(HotstuffError::UnknownAuthority(self.author.to_owned()))?;

		self.signature.as_ref().ok_or(NullSignature).and_then(|signature| {
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

		self.signature.as_ref().ok_or(NullSignature).and_then(|signature| {
			if !AuthorityPair::verify(signature, self.digest(), &self.voter) {
				return Err(InvalidSignature(self.voter.to_owned()));
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

		self.signature.as_ref().ok_or(NullSignature).and_then(|signature| {
			if !AuthorityPair::verify(signature, self.digest(), &self.voter) {
				return Err(InvalidSignature(self.voter.to_owned()));
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
				return Err(AuthorityReuse(authority_id.clone()));
			}
			used.insert(authority_id.clone());
			grant_votes += 1;
		}

		if grant_votes <= (authorities.len() * 2 / 3) {
			return Err(InsufficientQuorum);
		}
		let mut final_high_qc_view = 0;

		for (voter, signature, high_qc_view) in self.votes.iter() {

			let mut data = self.view.encode();
			data.append(&mut high_qc_view.encode());
			let digest = <<Block::Header as HeaderT>::Hashing as HashT>::hash_of(&data);
			if !AuthorityPair::verify(signature, digest, voter) {
				return Err(InvalidSignature(voter.clone()));
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
				Err(InvalidSignature(self.authority.clone()))
			} else {
				Ok(())
			},
			None => Err(NullSignature),
		}
	}
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum ProposalReq<Block: BlockT> {
	Hashes(Vec<Block::Hash>),
	View(ViewNumber, Option<ViewNumber>),
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
			return Err(NotAuthority);
		}
		let digest = self.digest();
		match &self.signature {
			Some(signature) => if !AuthorityPair::verify(signature, digest, &self.authority) {
				Err(InvalidSignature(self.authority.clone()))
			} else {
				Ok(())
			},
			None => Err(NullSignature),
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
	BlockImport(BlockInfo<B>),
}

impl<Block: BlockT> ConsensusMessage<Block> {
	pub fn gossip_topic() -> Block::Hash {
		// TODO maybe use Lazy then just call hash once.
		<<Block::Header as HeaderT>::Hashing as HashT>::hash(b"hotstuff/consensus")
	}
}
