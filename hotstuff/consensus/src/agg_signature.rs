use sp_api::{Decode, Encode};
use w3f_bls::{EngineBLS, Message, PublicKey, SerializableToBytes, Signature as SingleSignature, Signed, SignedMessage};
use w3f_bls::distinct::DistinctMessages;
use crate::{AuthorityList, AuthoritySignature};
use crate::consensus::error::HotstuffError;
use crate::consensus::message::extend_message;

const BUCKET_SIZE: usize = 8;

#[derive(Encode, Decode, Default, Debug, Clone)]
pub struct AggregateSignature {
	pub mask: BitVec,
	pub signature: Vec<u8>,
}

impl AggregateSignature {
	pub fn aggregate<Engine: EngineBLS>(
		mask: BitVec,
		authorities: &AuthorityList,
		message: &[u8],
		mut votes: Vec<(u16, AuthoritySignature)>,
	) -> Result<Self, HotstuffError> {
		let mut aggregator = DistinctMessages::<Engine>::new();
		votes.sort_by(|a, b| a.0.cmp(&b.0));
		for (i, sig) in votes {
			let authority = authorities[i as usize].0.clone();
			let message = Message::from(extend_message(i, message).as_slice());
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
		let signature = <&DistinctMessages<Engine> as Signed>::signature(&&aggregator).to_bytes();
		Ok(AggregateSignature { mask, signature })
	}

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