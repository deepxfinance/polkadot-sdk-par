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

use crate::{message::Proposal, primitives::{HotstuffError, ViewNumber}, store::Store, CLIENT_LOG_TARGET};
use crate::message::ProposalKey;

pub const HIGH_VIEW_KEY: &str = "hots_high_view";
pub const VIEW_PREFIX: &str = "hots_view";

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

pub fn view_key(view: ViewNumber) -> Vec<u8> {
	format!("{VIEW_PREFIX}_{view}").as_bytes().to_vec()
}

// Synchronizer synchronizes replicas to the same view.
pub struct Synchronizer<B: BlockT, C: AuxStore + HeaderBackend<B>> {
	store: Store<C>,
	high_view: ViewNumber,
	high_digest: B::Hash,
}

impl<B, C> Synchronizer<B, C>
where
	B: BlockT,
	C: AuxStore + HeaderBackend<B>,
{
	pub fn new(client: Arc<C>) -> Self {
		let store = Store::new(client);
		let (high_view, high_digest): (ViewNumber, B::Hash) = store
			.get(HIGH_VIEW_KEY.as_ref())
			.unwrap()
			.map(|v| Decode::decode(&mut v.as_slice()).unwrap())
			.unwrap_or_default();
		Self {
			store,
			high_view,
			high_digest,
		}
	}

	pub fn high_view(&self) -> ViewNumber {
		self.high_view
	}

	pub fn high_digest(&self) -> B::Hash {
		self.high_digest
	}

	fn save_high_proposal(&mut self, view: ViewNumber, digest: B::Hash) -> Result<(), HotstuffError> {
		if view >= self.high_view {
			if view == self.high_view {
				// TODO should we allow same proposal cover same view?
				log::warn!(target: CLIENT_LOG_TARGET, "Trying to cover high_proposal(view {view}) {} -> {digest}", self.high_view);
			}
			self.store
				.set(HIGH_VIEW_KEY.as_ref(), (view, digest).encode().as_slice())
				.map_err(|e| HotstuffError::SaveProposal(e.to_string()))?;
			self.high_view = view;
			self.high_digest = digest;
		}
		Ok(())
	}

	pub fn save_proposal(&mut self, proposal: &Proposal<B>) -> Result<(), HotstuffError> {
		let value = proposal.encode();
		let key = proposal.digest();

		trace!(target: CLIENT_LOG_TARGET, "~~ save_proposal {} {key} {}, parent: {} {}", proposal.view, proposal.payload, proposal.qc.view, proposal.qc.proposal_hash);
		// try save high_view and high_digest
		self.save_high_proposal(proposal.view, key)?;
		// save view -> digest
		self.store
			.set(view_key(proposal.view).as_slice(), &key.encode().as_ref())
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
			let mut view = last_proposal.view;
			loop {
				if let Some(proposal) = self.get_proposal(ProposalKey::View(view))? {
					if view >= self.high_view {
						break;
					}
					let digest = proposal.digest();
					delete.push(view_key(view));
					delete.push(digest.as_ref().to_vec());
					delete_info.push(format!("{}:{}", view, digest));
					// invalid view proposals between (view.parent_view, view)
					for v in (proposal.qc.view + 1..view).rev() {
						if let Some(proposal) = self.get_proposal(ProposalKey::View(v))? {
							let digest = proposal.digest();
							delete_invalid.push(view_key(proposal.view));
							delete_invalid.push(digest.as_ref().to_vec());
							delete_invalid_info.push(format!("{}:{digest}", proposal.view));
						}
					}
					view = proposal.qc.view;
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

	pub fn get_high_proposal(&self) -> Result<Option<Proposal<B>>, HotstuffError> {
		self.get_proposal(ProposalKey::digest(self.high_digest))
	}

	pub fn get_digest(&self, view: ViewNumber) -> Result<Option<B::Hash>, HotstuffError> {
		self
			.store
			.get(view_key(view).as_slice())
			.map(|value| value.map(|v| Decode::decode(&mut v.as_slice()).unwrap()))
			.map_err(|e| HotstuffError::GetProposal(e.to_string()))
	}

	pub fn get_proposal(&self, key: ProposalKey<B>) -> Result<Option<Proposal<B>>, HotstuffError> {
		match key {
			ProposalKey::View(view) => match self.get_digest(view)? {
				Some(digest) => self.get_proposal(ProposalKey::digest(digest)),
				None => Ok(None)
			}
			ProposalKey::Digest(digest) => {
				self
					.store
					.get(digest.as_ref())
					.map(|value| value.map(|v| Proposal::decode(&mut v.as_slice()).unwrap()))
					.map_err(|e| HotstuffError::GetProposal(e.to_string()))
			}
		}
	}

	pub fn get_proposals(&self, from: ProposalKey<B>, to: ProposalKey<B>) -> Result<Vec<Proposal<B>>, HotstuffError> {
		let mut proposals = vec![];
		let mut proposal_hash = match to {
			ProposalKey::View(view) => match self.get_digest(view)? {
				Some(digest) => digest,
				None => return Ok(proposals),
			},
			ProposalKey::Digest(digest) => digest,
		};
		loop {
			match self.get_proposal(ProposalKey::digest(proposal_hash))? {
				Some(proposal) => {
					match &from {
						ProposalKey::View(view) => if proposal.view <= *view {
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

	pub fn revert(&mut self, to_view: ViewNumber, to_digest: B::Hash) -> Result<(ViewNumber, B::Hash), HotstuffError> {
		if let Some(to_proposal) = self.get_proposal(ProposalKey::digest(to_digest))? {
			if to_proposal.view != to_view {
				return Err(HotstuffError::Other(format!("revert incorrect to_view {to_view} with to_digest {to_digest}")));
			}
		}
		let mut view = self.high_view;
		let mut delete = vec![];
		let mut insert = vec![];
		let mut final_high_digest = B::Hash::default();
		loop {
			if view <= to_view {
				// update hig_view and high_digest.
				if let Some(digest) = self.get_digest(view)? {
					final_high_digest = digest;
					insert.push((HIGH_VIEW_KEY.as_bytes().to_vec(), (view, digest).encode()));
					break;
				} else if view == 0 {
					// clear high_view and high_digest
					delete.push(HIGH_VIEW_KEY.as_bytes().to_vec());
					break;
				}
			} else {
				// delete:
				// 1. view -> digest
				// 2. digest -> proposal
				if let Some(digest) = self.get_digest(view)? {
					delete.push(view_key(view));
					delete.push(digest.as_ref().to_vec());
				}
			}
			view = view.saturating_sub(1);
		}
		self.store.revert(&insert, &delete)
			.map_err(|e| HotstuffError::Other(format!("revert delete failed for {e:?}")))?;
		Ok((view, final_high_digest))
	}
}
