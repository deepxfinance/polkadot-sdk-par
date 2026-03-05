use std::sync::Arc;
use codec::{Decode, Encode};
use sc_client_api::backend::AuxStore;
use sp_blockchain::{Error, HeaderBackend, Result as ClientResult};
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
	authorities: G,
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
		None => AuthoritySet::genesis(authorities()?)
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

#[derive(Clone)]
pub struct Store<C: AuxStore> {
	backend: Arc<C>,
}

impl<C: AuxStore> Store<C> {
	pub fn new(backend: Arc<C>) -> Self {
		Self { backend }
	}

	pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
		self.backend.get_aux(key)
	}

	pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
		self.backend.insert_aux(&[(key, value)], &[])
	}

	pub fn revert(&mut self, insert: &[(Vec<u8>, Vec<u8>)], delete: &[Vec<u8>]) -> Result<(), Error> {
		let insert: Vec<_> = insert.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
		let delete: Vec<_> = delete.iter().map(|k| k.as_slice()).collect();
		self.backend.insert_aux(insert.as_slice(), delete.as_slice())
	}
}

#[cfg(test)]
mod tests {
	use crate::aux_schema::Store;
	#[test]
	fn test_create_store_with_substrate_client_should_work() {
		let client = substrate_test_runtime_client::new();
		let mut store = Store::new(std::sync::Arc::new(client));

		store.set(b"key0", b"value0").unwrap();

		assert_eq!(store.get(b"key0").unwrap().unwrap(), b"value0");
	}
}
