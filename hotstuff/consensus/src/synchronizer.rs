use std::{
	future::Future,
	pin::Pin,
	sync::Arc,
	task::{Context, Poll},
	time::Duration,
};
use log::trace;
use tokio::time::{interval, Instant, Interval};

use sc_client_api::AuxStore;
use sp_blockchain::HeaderBackend;
use sp_core::{Decode, Encode};
use sp_runtime::traits::Block as BlockT;

use crate::{message::Proposal, primitives::HotstuffError, store::Store, CLIENT_LOG_TARGET};
use crate::message::{CommitQC, ProposalKey, Round};

pub const HIGH_ROUND_KEY: &str = "hots_high_round";
pub const ROUND_PREFIX: &str = "hots_round";
pub const COMMIT_QC: &str = "hots_commit_qc";

pub struct Timer {
	delay: Interval,
}

impl Timer {
	pub fn new(duration: u64) -> Self {
		Self { delay: interval(Duration::from_millis(duration)) }
	}

	pub fn reset(&mut self) {
		self.delay.reset();
	}

	pub fn period(&self) -> Duration {
		self.delay.period()
	}
}

impl Future for Timer {
	type Output = Instant;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		self.delay.poll_tick(cx)
	}
}

pub fn round_key(round: Round) -> Vec<u8> {
	format!("{ROUND_PREFIX}_{round}").as_bytes().to_vec()
}

pub struct Synchronizer<B: BlockT, C: AuxStore + HeaderBackend<B>> {
	store: Store<C>,
	high_round: Round,
	high_digest: B::Hash,
}

impl<B, C> Synchronizer<B, C>
where
	B: BlockT,
	C: AuxStore + HeaderBackend<B>,
{
	pub fn new(client: Arc<C>) -> Self {
		let store = Store::new(client);
		let (high_round, high_digest): (Round, B::Hash) = store
			.get(HIGH_ROUND_KEY.as_ref())
			.unwrap()
			.map(|v| Decode::decode(&mut v.as_slice()).unwrap())
			.unwrap_or_default();
		Self {
			store,
			high_round,
			high_digest,
		}
	}

	pub fn high_round(&self) -> Round {
		self.high_round
	}

	pub fn high_digest(&self) -> B::Hash {
		self.high_digest
	}

	fn save_high_proposal(&mut self, round: Round, digest: B::Hash) -> Result<(), HotstuffError> {
		if round >= self.high_round {
			if round == self.high_round {
				// TODO should we allow same proposal cover same view?
				log::warn!(target: CLIENT_LOG_TARGET, "Trying to cover high_proposal(round {round}) {} -> {digest}", self.high_round);
			}
			self.store
				.set(HIGH_ROUND_KEY.as_ref(), (round, digest).encode().as_slice())
				.map_err(|e| HotstuffError::SaveProposal(e.to_string()))?;
			self.high_round = round;
			self.high_digest = digest;
		}
		Ok(())
	}

	pub fn save_commit_qc(&mut self, qc: &CommitQC<B>) -> Result<(), HotstuffError> {
		self.store
			.set(COMMIT_QC.as_ref(), Encode::encode(qc).as_slice())
			.map_err(|e| HotstuffError::SaveQC(e.to_string()))
	}

	pub fn save_proposal(&mut self, proposal: &Proposal<B>) -> Result<(), HotstuffError> {
		let value = proposal.encode();
		let key = proposal.digest();

		trace!(target: CLIENT_LOG_TARGET, "~~ save_proposal {} {key} {} parent {}:{}", proposal.round(), proposal.payload, proposal.qc.round(), proposal.qc.proposal_hash);
		// try save high_view and high_digest
		self.save_high_proposal(proposal.round(), key)?;
		// save view -> digest
		self.store
			.set(round_key(proposal.round()).as_slice(), &key.encode().as_ref())
			.map_err(|e| HotstuffError::SaveProposal(e.to_string()))?;
		// save digest -> proposal
		self.store
			.set(key.as_ref(), &value)
			.map_err(|e| HotstuffError::SaveProposal(e.to_string()))?;
		Ok(())
	}

	pub fn clear_all_before(&mut self, key: ProposalKey<B>) -> Result<(Vec<String>, Vec<String>), HotstuffError> {
		let start_proposal = self.get_proposal(key.clone())?;
		let mut delete = vec![];
		let mut delete_info = vec![];
		let mut delete_invalid = vec![];
		let mut delete_invalid_info = vec![];
		if let Some(last_proposal) = start_proposal {
			let mut round = last_proposal.round();
			loop {
				if let Some(proposal) = self.get_proposal(ProposalKey::Round(round))? {
					if round >= self.high_round {
						break;
					}
					let digest = proposal.digest();
					delete.push(round_key(round));
					delete.push(digest.as_ref().to_vec());
					delete_info.push(format!("{}:{}", round, digest));
					// invalid view proposals between (view.parent_round, round)
					let mut inter_round = proposal.qc.round().add_one();
					loop {
						if inter_round >= round {
							break;
						}
						if let Some(proposal) = self.get_proposal(ProposalKey::Round(inter_round))? {
							let digest = proposal.digest();
							delete_invalid.push(round_key(proposal.round()));
							delete_invalid.push(digest.as_ref().to_vec());
							delete_invalid_info.push(format!("{}:{digest}", proposal.round()));
						}
						inter_round = inter_round.add_one();
					}
					round = proposal.qc.round();
				} else {
					break;
				}
			}
		}
		if !delete.is_empty() || !delete_invalid.is_empty() {
			delete.extend(delete_invalid);
			self.store.revert(&vec![], &delete).map_err(|e| HotstuffError::ClientError(e.to_string()))?;
		}
		Ok((delete_info, delete_invalid_info))
	}

	pub fn get_commit_qc(&self) -> Result<Option<CommitQC<B>>, HotstuffError> {
		self
			.store
			.get(COMMIT_QC.as_ref())
			.map(|value| value.map(|v| Decode::decode(&mut v.as_slice()).unwrap()))
			.map_err(|e| HotstuffError::GetProposal(e.to_string()))
	}

	pub fn get_high_proposal(&self) -> Result<Option<Proposal<B>>, HotstuffError> {
		self.get_proposal(ProposalKey::digest(self.high_digest))
	}

	pub fn get_digest(&self, round: Round) -> Result<Option<B::Hash>, HotstuffError> {
		self
			.store
			.get(round_key(round).as_slice())
			.map(|value| value.map(|v| Decode::decode(&mut v.as_slice()).unwrap()))
			.map_err(|e| HotstuffError::GetProposal(e.to_string()))
	}

	pub fn get_proposal(&self, key: ProposalKey<B>) -> Result<Option<Proposal<B>>, HotstuffError> {
		match key {
			ProposalKey::Round(round) => match self.get_digest(round)? {
				Some(digest) => self.get_proposal(ProposalKey::digest(digest)),
				None => Ok(None)
			}
			ProposalKey::Digest(digest) => {
				self
					.store
					.get(digest.as_ref())
					.map(|value| value.map(|v| Decode::decode(&mut v.as_slice()).unwrap()))
					.map_err(|e| HotstuffError::GetProposal(e.to_string()))
			}
		}
	}

	pub fn get_proposals(&self, from: ProposalKey<B>, to: ProposalKey<B>) -> Result<Vec<Proposal<B>>, HotstuffError> {
		let mut proposals = vec![];
		let mut proposal_hash = match to {
			ProposalKey::Round(round) => match self.get_digest(round)? {
				Some(digest) => digest,
				None => return Ok(proposals),
			},
			ProposalKey::Digest(digest) => digest,
		};
		loop {
			match self.get_proposal(ProposalKey::digest(proposal_hash))? {
				Some(proposal) => {
					match &from {
						ProposalKey::Round(round) => if proposal.round() <= *round {
							break;
						}
						ProposalKey::Digest(digest) => if digest == &proposal_hash {
							break;
						}
					}
					proposal_hash = proposal.parent_hash();
					proposals.push(proposal);
				},
				None => continue,
			}
		}
		proposals.reverse();
		Ok(proposals)
	}

	pub fn get_proposal_ancestors(
		&self,
		proposal: &Proposal<B>,
	) -> Result<Option<(Proposal<B>, Proposal<B>)>, HotstuffError> {
		let parent = self.get_proposal_parent(proposal)?;
		if parent.is_none() {
			return Ok(None);
		}
		let grandpa = self.get_proposal_parent(parent.as_ref().unwrap())?;
		if grandpa.is_none() {
			return Ok(None);
		}

		Ok(Some((parent.unwrap(), grandpa.unwrap())))
	}

	pub fn get_proposal_parent(
		&self,
		proposal: &Proposal<B>,
	) -> Result<Option<Proposal<B>>, HotstuffError> {
		self.get_proposal(ProposalKey::digest(proposal.parent_hash()))
	}

	pub fn revert(&mut self, to_round: Round, to_digest: B::Hash) -> Result<(Round, B::Hash), HotstuffError> {
		if let Some(to_proposal) = self.get_proposal(ProposalKey::digest(to_digest))? {
			if to_proposal.round() != to_round {
				return Err(HotstuffError::Other(format!("revert incorrect to_round {to_round} with to_digest {to_digest}")));
			}
		}
		let mut round = self.high_round;
		let mut delete = vec![];
		let mut insert = vec![];
		let mut final_high_digest = B::Hash::default();
		loop {
			if round <= to_round {
				// update hig_view and high_digest.
				if let Some(digest) = self.get_digest(round)? {
					final_high_digest = digest;
					insert.push((HIGH_ROUND_KEY.as_bytes().to_vec(), (round, digest).encode()));
					break;
				} else if round == Round::default() {
					// clear high_view and high_digest
					delete.push(HIGH_ROUND_KEY.as_bytes().to_vec());
					break;
				}
			} else {
				// delete:
				// 1. view -> digest
				// 2. digest -> proposal
				if let Some(digest) = self.get_digest(round)? {
					delete.push(round_key(round));
					delete.push(digest.as_ref().to_vec());
				}
			}
			round = round.sub_one();
		}
		self.store.revert(&insert, &delete)
			.map_err(|e| HotstuffError::Other(format!("revert delete failed for {e:?}")))?;
		Ok((round, final_high_digest))
	}
}
