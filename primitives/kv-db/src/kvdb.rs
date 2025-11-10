use hash_db::{HashDBRef, Hasher, Prefix};
use crate::{DBValue, KVCache, KV};

pub struct KVDB<'db, 'cache, H: Hasher> {
    hash: &'db H::Out,
    db: &'db dyn HashDBRef<H, DBValue>,
    cache: Option<core::cell::RefCell<&'cache mut dyn KVCache<H>>>,
}

impl <'db, 'cache, H: Hasher> KVDB<'db, 'cache, H> {
    pub fn new(
        db: &'db dyn HashDBRef<H, DBValue>,
        hash: &'db H::Out,
        cache: Option<&'cache mut dyn KVCache<H>>,
    ) -> Self {
        Self {
            hash,
            db,
            cache: cache.map(core::cell::RefCell::new),
        }
    }

    /// Get the backing database.
    pub fn db(&'db self) -> &'db dyn HashDBRef<H, DBValue> {
        self.db
    }

    /// Fetch a value under the given `hash`.
    pub(crate) fn fetch_value(
        &self,
        hash: H::Out,
        prefix: Prefix,
    ) -> Result<DBValue, String> {
        let cache = self.cache.as_ref().map(|c| c.borrow_mut());
        if let Some(mut c) = cache {
            if let Some(value) = (*c).lookup_value_for_key(hash, &prefix.0) {
                return Ok(value.to_vec());
            }
        };
        match self.db.get(&hash, prefix) {
            Some(value) => {
                let cache = self.cache.as_ref().map(|c| c.borrow_mut());
                cache.map(|mut c| {
                    (*c).cache_value_for_key(hash, &prefix.0, value.clone())
                });
                Ok(value.to_vec())
            },
            None => Err("not found".into())
        }
    }

    pub fn get_hash(&self, key: &[u8]) -> Result<Option<H::Out>, String> {
        match self.fetch_value(H::hash(key), (key, None)) {
            Ok(_) => Ok(Some(H::hash(key))),
            Err(_) => Ok(None),
        }
    }
}

impl<'db, 'cache, H: Hasher> KV<H> for KVDB<'db, 'cache, H> {
    fn get(&self, key: &[u8]) -> Option<DBValue> {
        match self.fetch_value(H::hash(key), (key, None)).ok() {
            Some(v) => if v == vec![0u8] {
                None
            } else {
                Some(v)
            },
            None => None
        }
    }
}

pub fn prefixed_key<H: Hasher>(key: &H::Out, prefix: Prefix) -> Vec<u8> {
    let mut prefixed_key = Vec::with_capacity(key.as_ref().len() + prefix.0.len() + 1);
    prefixed_key.extend_from_slice(prefix.0);
    if let Some(last) = prefix.1 {
        prefixed_key.push(last);
    }
    prefixed_key.extend_from_slice(key.as_ref());
    prefixed_key
}