use std::collections::HashMap;

use hotstuff_primitives::{AuthorityId, AuthorityList, AuthoritySignature};
use sp_runtime::traits::Block;
use sp_timestamp::Timestamp;
use crate::{
	message::{Timeout, Vote, QC, TC},
	primitives::{HotstuffError, HotstuffError::*, ViewNumber},
};

#[derive(Default)]
pub struct Aggregator<B: Block> {
	votes_aggregator: HashMap<ViewNumber, HashMap<B::Hash, QCMaker>>,
	timeouts_aggregators: HashMap<ViewNumber, TCMaker<B>>,
}

impl<B: Block> Aggregator<B> {
	pub fn new() -> Self {
		Self { votes_aggregator: HashMap::new(), timeouts_aggregators: HashMap::new() }
	}

	pub fn add_vote(
		&mut self,
		vote: Vote<B>,
		authorities: &AuthorityList,
	) -> Result<Option<QC<B>>, HotstuffError> {
		self.votes_aggregator
			.entry(vote.view)
			.or_default()
			.entry(vote.digest())
			.or_default()
			.append(vote, authorities)
	}

	pub fn add_timeout(
		&mut self,
		timeout: &Timeout<B>,
		authorities: &AuthorityList,
	) -> Result<Option<TC<B>>, HotstuffError> {
		// Add the new timeout to our aggregator and see if we have a TC.
		self.timeouts_aggregators
			.entry(timeout.view)
			.or_default()
			.append(timeout.clone(), authorities)
	}
}

pub struct QCMaker {
	weight: u64,
	votes: Vec<(AuthorityId, AuthoritySignature)>,
}

impl QCMaker {
	pub fn new() -> Self {
		QCMaker { weight: 0, votes: Vec::new() }
	}

	pub fn append<B: Block>(
		&mut self,
		vote: Vote<B>,
		authorities: &AuthorityList,
	) -> Result<Option<QC<B>>, HotstuffError> {
		if self.votes.iter().any(|(id, _)| id.eq(&vote.voter)) {
			return Ok(None);
		}

		self.votes
			.push((vote.voter, vote.signature.ok_or(HotstuffError::NullSignature)?));
		self.weight += 1;

		if self.weight < (authorities.len() * 2 / 3 + 1) as u64 {
			return Ok(None);
		}

		Ok(Some(QC::<B> {
			proposal_hash: vote.proposal_hash,
			view: vote.view,
			votes: self.votes.clone(),
			timestamp: Timestamp::current(),
		}))
	}
}

impl Default for QCMaker {
	fn default() -> Self {
		Self::new()
	}
}

pub struct TCMaker<B: Block> {
	weight: u64,
	// (authority, signature, high_qc.view).
	votes: Vec<(AuthorityId, AuthoritySignature, ViewNumber)>,
	// most high QC in votes.
	high_qc: QC<B>,
}

impl<B: Block> TCMaker<B> {
	pub fn new() -> Self {
		Self { weight: 0, votes: Vec::new(), high_qc: QC::<B>::default() }
	}

	pub fn append(
		&mut self,
		timeout: Timeout<B>,
		authorities: &AuthorityList,
	) -> Result<Option<TC<B>>, HotstuffError> {
		let voter = timeout.voter;
		if self.votes.iter().any(|(id, _, _)| id.eq(&voter)) {
			return Ok(None);
		}

		self.votes.push((voter, timeout.signature.ok_or(NullSignature)?, timeout.high_qc.view));
		if timeout.high_qc.view > self.high_qc.view {
			self.high_qc = timeout.high_qc.clone();
		}
		self.weight += 1;

		if self.weight < (authorities.len() * 2 / 3 + 1) as u64 {
			return Ok(None);
		}

		Ok(Some(TC::<B> {
			view: timeout.view,
			votes: self.votes.clone(),
			high_qc: self.high_qc.clone(),
		}))
	}
}

impl<B: Block> Default for TCMaker<B> {
	fn default() -> Self {
		Self::new()
	}
}
