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
		let view = vote.view;
		let view_room = self.votes_aggregator.entry(view).or_default();
		let digest = vote.digest();
		let digest_room = view_room.entry(digest).or_default();
		if let Some(qc) = digest_room.append(vote, authorities)? {
			view_room.remove(&digest);
			if view_room.is_empty() {
				self.votes_aggregator.remove(&view);
			}
			// remove all old view votes
			let mut views = self.votes_aggregator.keys().cloned().collect::<Vec<_>>();
			views.sort();
			for old_view in views {
				if old_view <= view {
					self.votes_aggregator.remove(&old_view);
				} else {
					break;
				}
			}
			Ok(Some(qc))
		} else {
			Ok(None)
		}
	}

	pub fn add_timeout(
		&mut self,
		timeout: &Timeout<B>,
		authorities: &AuthorityList,
	) -> Result<Option<TC<B>>, HotstuffError> {
		// Add the new timeout to our aggregator and see if we have a TC.
		let view = timeout.view;
		let view_room = self.timeouts_aggregators.entry(timeout.view).or_default();
		if let Some(tc) = view_room.append(timeout.clone(), authorities)? {
			self.timeouts_aggregators.remove(&view);
			// remove all old timeout votes
			let mut views = self.timeouts_aggregators.keys().cloned().collect::<Vec<_>>();
			views.sort();
			for old_view in views {
				if old_view <= view {
					self.timeouts_aggregators.remove(&old_view);
				} else {
					break;
				}
			}
			Ok(Some(tc))
		} else {
			Ok(None)
		}
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
			stage: vote.stage,
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
