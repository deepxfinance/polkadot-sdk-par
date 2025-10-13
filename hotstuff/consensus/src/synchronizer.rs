use std::{
	future::Future,
	pin::Pin,
	sync::Arc,
	task::{Context, Poll},
	time::Duration,
};
use std::collections::HashMap;
use log::debug;
use tokio::time::{interval, Instant, Interval};

use sc_client_api::Backend;
use sp_core::{Decode, Encode};
use sp_runtime::traits::Block as BlockT;

use crate::{
	client::ClientForHotstuff, message::Proposal, primitives::{HotstuffError, ViewNumber}, store::Store,
};

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

// Synchronizer synchronizes replicas to the same view.
pub struct Synchronizer<B: BlockT, BE: Backend<B>, C: ClientForHotstuff<B, BE>> {
	store: Store<B, BE, C>,
	trace_size: usize,
	trace_from: ViewNumber,
	trace_to: ViewNumber,
	trace_view: HashMap<ViewNumber, B::Hash>,
}

impl<B, BE, C> Synchronizer<B, BE, C>
where
	B: BlockT,
	BE: Backend<B>,
	C: ClientForHotstuff<B, BE>,
{
	pub fn new(client: Arc<C>, trace_size: usize) -> Self {
		Self { store: Store::new(client), trace_size, trace_view: HashMap::with_capacity(trace_size), trace_from: 0, trace_to: 0 }
	}

	pub fn save_proposal(&mut self, proposal: &Proposal<B>) -> Result<(), HotstuffError> {
		let value = proposal.encode();
		let key = proposal.digest();

		debug!("~~ save proposal, digest {}", key);
		log::debug!(target: "Hotstuff", "save proposal {} {key} {:?}, parent: {}", proposal.view, proposal.payload, proposal.qc.proposal_hash);
		self.store
			.set(key.as_ref(), &value)
			.map_err(|e| HotstuffError::SaveProposal(e.to_string()))?;
		loop {
			if self.trace_view.len() >= self.trace_size {
				self.trace_view.remove(&self.trace_from);
				self.trace_from += 1;
			} else {
				break;
			}
		}
		self.trace_view.insert(proposal.view, key);
		self.trace_from = self.trace_from.min(proposal.view);
		self.trace_to = self.trace_to.max(proposal.view);
		Ok(())
	}

	pub fn get_proposal_by_trace_view(&self, view: ViewNumber) -> Result<Option<Proposal<B>>, HotstuffError> {
		match self.trace_view.get(&view) {
			Some(hash) => self.get_proposal(*hash),
			None => Ok(None),
		}
	}

	pub fn get_proposal(&self, hash: B::Hash) -> Result<Option<Proposal<B>>, HotstuffError> {
		self
			.store
			.get(hash.as_ref())
			.map(|value| value.map(|v| Proposal::decode(&mut v.as_slice()).unwrap()))
			.map_err(|e| HotstuffError::SaveProposal(e.to_string()))
	}

	pub fn get_proposal_ancestors<F>(
		&self,
		proposal: &Proposal<B>,
		filter: &F,
	) -> Result<Option<(Proposal<B>, Proposal<B>)>, HotstuffError>
	where
		F: Fn(&Proposal<B>) -> bool
	{
		let parent = self.get_proposal_parent(proposal, filter)?;
		if parent.is_none() {
			return Ok(None);
		}
		let grandpa = self.get_proposal_parent(parent.as_ref().unwrap(), filter)?;
		if grandpa.is_none() {
			return Ok(None);
		}

		Ok(Some((parent.unwrap(), grandpa.unwrap())))
	}

	pub fn get_proposal_parent<F>(
		&self,
		proposal: &Proposal<B>,
		filter: &F,
	) -> Result<Option<Proposal<B>>, HotstuffError>
	where
		F: Fn(&Proposal<B>) -> bool
	{
		let mut parent_hash = proposal.parent_hash().as_ref().to_vec();
		loop {
			let res = self
				.store
				.get(&parent_hash)
				.map_err(|e| HotstuffError::Other(e.to_string()))?;

			if let Some(data) = res {
				let proposal: Proposal<B> =
					Decode::decode(&mut &data[..]).map_err(|e| HotstuffError::Other(e.to_string()))?;
				if filter(&proposal) {
					return Ok(Some(proposal));
				}
				parent_hash = proposal.parent_hash().as_ref().to_vec();
			} else {
				// TODO request from network, wait result here?
				return Ok(None)
			}
		}
	}
}
