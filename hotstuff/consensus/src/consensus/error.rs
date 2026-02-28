use crate::AuthorityId;

// define some primitives used in hotstuff
pub type ViewNumber = u64;

// TODO The `AuthorityId` in this context should be reference instead of value?
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum HotstuffError {
	// Receive more than one vote from the same authority.
	#[error("AuthorityReuse({0})")]
	AuthorityReuse(AuthorityId),
	// The QC without a quorum
	#[error("InsufficientQuorum")]
	InsufficientQuorum,
	// Get invalid signature from an authority.
	#[error("InvalidSignature({0})")]
	InvalidSignature(AuthorityId),
	#[error("InvalidAggregatedSignature({0})")]
	InvalidAggregatedSignature(String),
	#[error("NullSignature")]
	NullSignature,
	#[error("InvalidAuthority({0})")]
	InvalidAuthority(String),
	#[error("UnknownAuthority({0})")]
	UnknownAuthority(AuthorityId),
	// The voter is not in authorities.
	#[error("NotAuthority")]
	NotAuthority,
	#[error("WrongProposer")]
	WrongProposer,
	// Vote from old view.
	#[error("ExpiredVote")]
	ExpiredVote,
	#[error("InvalidTC")]
	InvalidTC,
	#[error("SaveProposal({0})")]
	SaveProposal(String),
	#[error("SaveQC({0})")]
	SaveQC(String),
	#[error("GetProposal({0})")]
	GetProposal(String),
	#[error("GetQC({0})")]
	GetQC(String),
	#[error("Payload({0:?})")]
	Payload(PayloadError),
	#[error("VerifyClaim({0})")]
	VerifyClaim(String),
	// Error generate by node client.
	#[error("ClientError({0})")]
	ClientError(String),
	#[error("Other({0})")]
	Other(String),
}

impl From<PayloadError> for HotstuffError {
	fn from(e: PayloadError) -> HotstuffError {
		HotstuffError::Payload(e)
	}
}

#[derive(Debug, PartialEq, Eq)]
pub enum PayloadError {
	UnknownBlock(String),
	BlockRollBack(String),
	BaseBlock(String),
	ExtrinsicErr(String),
}
