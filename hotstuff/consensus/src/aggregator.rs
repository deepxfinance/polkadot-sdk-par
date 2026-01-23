use std::collections::HashMap;
use std::marker::PhantomData;
use codec::{Encode, Decode};
use w3f_bls::{
	EngineBLS, Message, Signature as SingleSignature, Signed, PublicKey,
	distinct::DistinctMessages, SignedMessage
};
use w3f_bls::SerializableToBytes;
use sp_runtime::traits::Block;
use crate::{
	AuthorityList, AuthoritySignature,
	message::{Timeout, Vote, QC, TC},
	error::{HotstuffError, HotstuffError::*, ViewNumber},
};
use crate::message::extend_message;

const BUCKET_SIZE: usize = 8;

#[derive(Default)]
pub struct Aggregator<B: Block, Engine: EngineBLS> {
	votes_aggregator: HashMap<ViewNumber, HashMap<B::Hash, QCMaker<Engine>>>,
	timeouts_aggregators: HashMap<ViewNumber, TCMaker<B>>,
}

impl<B: Block, Engine: EngineBLS> Aggregator<B, Engine> {
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

pub struct QCMaker<Engine: EngineBLS> {
	weight: u64,
	mask: BitVec,
	votes: Vec<(u16, AuthoritySignature)>,
	phantom: PhantomData<Engine>,
}

impl<Engine: EngineBLS> QCMaker<Engine> {
	pub fn new() -> Self {
		QCMaker { weight: 0, mask: BitVec::with_num_bits(8), votes: Vec::new(), phantom: PhantomData }
	}

	pub fn append<B: Block>(
		&mut self,
		vote: Vote<B>,
		authorities: &AuthorityList,
	) -> Result<Option<QC<B>>, HotstuffError> {
		let author_i = match authorities.iter().enumerate().find(|(_, (a, _))| *a == vote.voter).map(|(i, _)| i) {
			Some(index) => index as u16,
			None => return Ok(None),
		};
		if self.mask.is_set(author_i) {
			return Ok(None);
		}
		self.mask.set(author_i);
		let digest = vote.digest();
		self.votes
			.push((author_i, vote.signature.ok_or(HotstuffError::NullSignature)?));
		self.weight += 1;

		if self.weight < (authorities.len() * 2 / 3 + 1) as u64 {
			return Ok(None);
		}
		let mut aggregator = DistinctMessages::<Engine>::new();
		let mut votes = self.votes.clone();
		votes.sort_by(|a, b| a.0.cmp(&b.0));
		for (i, sig) in votes {
			let authority = authorities[i as usize].0.clone();
			let message = Message::from(extend_message(i, digest.as_ref()).as_slice());
			let signed_message = SignedMessage {
				message: message.clone(),
				publickey: PublicKey::from_bytes(authority.clone().into_inner().as_ref())
					.map_err(|e| HotstuffError::InvalidAggregatedSignature(format!("parse authority {authority} failed for {e:?}")))?,
				signature: SingleSignature::<Engine>::from_bytes(sig.into_inner().as_ref())
					.map_err(|_| HotstuffError::InvalidSignature(authority.clone()))?,
			};
			aggregator = aggregator.add(&signed_message)
				.map_err(|e| HotstuffError::InvalidAggregatedSignature(
					format!("aggregate {} signature failed: {e:?}", authorities[i as usize].0)
				))?;
		}
		let signature = <&DistinctMessages<Engine> as Signed>::signature(&&aggregator);
		Ok(Some(QC::<B> {
			proposal_hash: vote.proposal_hash,
			view: vote.view,
			stage: vote.stage,
			signature: AggregateSignature {
				mask: self.mask.clone(),
				signature: signature.to_bytes(),
			},
		}))
	}
}

impl<Engine: EngineBLS> Default for QCMaker<Engine> {
	fn default() -> Self {
		Self::new()
	}
}

pub struct TCMaker<B: Block> {
	weight: u64,
	// (authority, signature, high_qc.view).
	votes: Vec<(u16, AuthoritySignature, ViewNumber)>,
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
		let author_i = match authorities.iter().enumerate().find(|(_, (a, _))| *a == timeout.voter).map(|(i, _)| i) {
			Some(index) => index as u16,
			None => return Ok(None),
		};
		self.votes.push((author_i, timeout.signature.ok_or(NullSignature)?, timeout.high_qc.view));
		if timeout.high_qc.view > self.high_qc.view {
			self.high_qc = timeout.high_qc.clone();
		}
		self.weight += 1;

		if self.weight < (authorities.len() * 2 / 3 + 1) as u64 {
			return Ok(None);
		}

		let mut votes = self.votes.clone();
		votes.sort_by(|a, b| a.0.cmp(&b.0));
		Ok(Some(TC::<B> {
			view: timeout.view,
			votes,
			high_qc: self.high_qc.clone(),
		}))
	}
}

impl<B: Block> Default for TCMaker<B> {
	fn default() -> Self {
		Self::new()
	}
}

#[derive(Encode, Decode, Default, Debug, Clone)]
pub struct AggregateSignature {
	pub mask: BitVec,
	pub signature: Vec<u8>,
}

impl AggregateSignature {
	pub fn verify<Engine: EngineBLS>(&self, message: &[u8], authorities: &AuthorityList) -> Result<(), HotstuffError> {
		let signature = SingleSignature::<Engine>::from_bytes(&self.signature)
			.map_err(|e| HotstuffError::InvalidAggregatedSignature(e.to_string()))?;
		let mut verifier = DistinctMessages::<Engine>::new();
		for i in self.mask.iter_ones() {
			if i >= authorities.len() {
				return Err(HotstuffError::InvalidAggregatedSignature(format!("No expected authority index {i}")));
			}
			let authority_public_key = PublicKey::<Engine>::from_bytes(authorities[i].0.clone().into_inner().as_ref())
				.map_err(|e| HotstuffError::InvalidAuthority(e.to_string()))?;
			let message = Message::from(extend_message(i as u16, message).as_slice());
			verifier = verifier.add_message_n_publickey(message.clone(), authority_public_key)
				.map_err(|e| HotstuffError::InvalidAuthority(e.to_string()))?;
		}
		verifier.add_signature(&signature);
		if verifier.verify() {
			Ok(())
		} else {
			Err(HotstuffError::InvalidAggregatedSignature("Invalid AggregateSignature".to_string()))
		}
	}
}

#[derive(Clone, Default, Debug, Eq, Hash, PartialEq, Encode, Decode)]
pub struct BitVec {
	inner: Vec<u8>,
}

impl BitVec {
	#[allow(unused)]
	fn with_capacity(num_buckets: usize) -> Self {
		Self {
			inner: Vec::with_capacity(num_buckets),
		}
	}

	pub fn with_num_bits(num_bits: u16) -> Self {
		Self {
			inner: vec![0; Self::required_buckets(num_bits)],
		}
	}

	pub fn set(&mut self, pos: u16) {
		let bucket: usize = pos as usize / BUCKET_SIZE;
		if self.inner.len() <= bucket {
			self.inner.resize(bucket + 1, 0);
		}
		let bucket_pos = pos as usize - (bucket * BUCKET_SIZE);
		self.inner[bucket] |= 0b1000_0000 >> bucket_pos as u8;
	}

	pub fn is_set(&self, pos: u16) -> bool {
		let bucket: usize = pos as usize / BUCKET_SIZE;
		if self.inner.len() <= bucket {
			return false;
		}
		// This is optimized to: let bucket_pos = pos | 0x07;
		let bucket_pos = pos as usize - (bucket * BUCKET_SIZE);
		(self.inner[bucket] & (0b1000_0000 >> bucket_pos as u8)) != 0
	}

	/// Returns the number of set bits.
	pub fn count_ones(&self) -> u32 {
		self.inner.iter().map(|a| a.count_ones()).sum()
	}

	/// Returns the index of the last set bit.
	pub fn last_set_bit(&self) -> Option<u16> {
		self.inner
			.iter()
			.rev()
			.enumerate()
			.find(|(_, byte)| byte != &&0u8)
			.map(|(i, byte)| {
				(8 * (self.inner.len() - i) - byte.trailing_zeros() as usize - 1) as u16
			})
	}

	pub fn iter_ones(&self) -> impl Iterator<Item = usize> + '_ {
		(0..self.inner.len() * BUCKET_SIZE).filter(move |idx| self.is_set(*idx as u16))
	}

	pub fn required_buckets(num_bits: u16) -> usize {
		num_bits
			.checked_sub(1)
			.map_or(0, |pos| pos as usize / BUCKET_SIZE + 1)
	}
}
