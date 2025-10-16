// Hotstuff block store, just a easy key-value store.
use std::sync::Arc;
use sc_client_api::AuxStore;
use sp_blockchain::Error;

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
	use super::*;

	#[test]
	fn test_create_store_with_substrate_client_should_work() {
		let client = substrate_test_runtime_client::new();
		let mut store = Store::new(Arc::new(client));

		store.set(b"key0", b"value0").unwrap();

		assert_eq!(store.get(b"key0").unwrap().unwrap(), b"value0");
	}
}
