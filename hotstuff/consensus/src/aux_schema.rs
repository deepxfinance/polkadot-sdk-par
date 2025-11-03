use codec::{Decode, Encode};
use sc_client_api::backend::AuxStore;
use sp_blockchain::{HeaderBackend, Result as ClientResult};
use sp_runtime::traits::{Block as BlockT, NumberFor};
use crate::AuthorityList;
use crate::authorities::{AuthoritySet, SharedAuthoritySet};

pub const HOTS_AUTHORITY: &[u8] = b"hots_authority";

/// Persistent data kept between runs.
#[derive(Clone)]
pub struct PersistentData<Block: BlockT> {
	pub(crate) authority_set: SharedAuthoritySet<NumberFor<Block>>,
}

/// Load or initialize persistent data from backend.
#[allow(unused)]
pub(crate) fn load_persistent<Block: BlockT, B, G>(
	backend: &B,
	genesis_hash: Block::Hash,
	genesis_number: NumberFor<Block>,
	genesis_authorities: G,
) -> ClientResult<PersistentData<Block>>
where
	B: AuxStore + HeaderBackend<Block>,
	G: FnOnce() -> ClientResult<AuthorityList>,
{
	let authority_set: AuthoritySet<NumberFor<Block>> = match backend.get_aux(HOTS_AUTHORITY)? {
		Some(data) => match Decode::decode(&mut data.as_slice()) {
			Ok(authority_set) => authority_set,
			Err(e) => return Err(sp_blockchain::Error::Backend("invalid aux_data for hots_authorities".into())),
		}
		None => AuthoritySet::genesis(genesis_authorities()?)
			.ok_or(sp_blockchain::Error::Backend("genesis hots_authorities failed".into()))?,
	};
	Ok(PersistentData { authority_set: authority_set.into() })
}

pub(crate) fn update_authority_set<Block: BlockT, F, R>(
	set: &AuthoritySet<NumberFor<Block>>,
	write_aux: F,
) -> R
where
	F: FnOnce(&[(&'static [u8], &[u8])]) -> R,
{
	// write new authority set state to disk.
	let encoded_set = set.encode();
	write_aux(&[(HOTS_AUTHORITY, &encoded_set[..])])
}
