use scale_info::TypeInfo;
use sp_api::{Decode, Encode, HeaderT};

#[derive(Clone, Encode, Decode, PartialEq, Eq, TypeInfo)]
#[cfg_attr(feature = "std", derive(Debug))]
pub struct HotsJustification<Header: HeaderT> {
    pub round: u64,
    // pub commit: Commit<Header>,
    pub votes_ancestries: Vec<Header>,
}
