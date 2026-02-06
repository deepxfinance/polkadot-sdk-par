use schnellru::{Limiter, LruMap};
use sp_runtime::traits::Block as BlockT;

const LOG_TARGET: &str = "db::block_cache";
const BLOCK_CACHE_SIZE: usize = 256;

/// Entry for block cache.
struct BlockCacheEntry<Block: BlockT> {
	/// Cached body for this block
	pub body: Option<Option<Vec<Block::Extrinsic>>>,
}

impl<Block: BlockT> Default for BlockCacheEntry<Block> {
	fn default() -> Self {
		Self { body: None }
	}
}

/// A limiter for a map which is limited by the number of elements.
#[derive(Copy, Clone, Debug)]
struct LoggingByLengthLimiter {
	max_length: usize,
}

impl LoggingByLengthLimiter {
	/// Creates a new length limiter with a given `max_length`.
	pub const fn new(max_length: usize) -> LoggingByLengthLimiter {
		LoggingByLengthLimiter { max_length }
	}
}

impl<Block: BlockT> Limiter<Block::Hash, BlockCacheEntry<Block>> for LoggingByLengthLimiter {
	type KeyToInsert<'a> = Block::Hash;
	type LinkType = usize;

	fn is_over_the_limit(&self, length: usize) -> bool {
		length > self.max_length
	}

	fn on_insert(
		&mut self,
		_length: usize,
		key: Self::KeyToInsert<'_>,
		value: BlockCacheEntry<Block>,
	) -> Option<(Block::Hash, BlockCacheEntry<Block>)> {
		if self.max_length > 0 {
			Some((key, value))
		} else {
			None
		}
	}

	fn on_replace(
		&mut self,
		_length: usize,
		_old_key: &mut Block::Hash,
		_new_key: Block::Hash,
		_old_value: &mut BlockCacheEntry<Block>,
		_new_value: &mut BlockCacheEntry<Block>,
	) -> bool {
		true
	}

	fn on_removed(&mut self, key: &mut Block::Hash, _value: &mut BlockCacheEntry<Block>) {
		log::trace!(
			target: LOG_TARGET,
			"Evicting value from block cache. hash = {}",
			key
		)
	}

	fn on_cleared(&mut self) {}

	fn on_grow(&mut self, _new_memory_usage: usize) -> bool {
		true
	}
}

/// Cache for block bodies.
pub struct BlockCache<Block: BlockT> {
	cache: LruMap<Block::Hash, BlockCacheEntry<Block>, LoggingByLengthLimiter>,
}

impl<Block: BlockT> BlockCache<Block> {
	pub fn new() -> Self {
		Self { cache: LruMap::new(LoggingByLengthLimiter::new(BLOCK_CACHE_SIZE)) }
	}

	/// Clear the cache
	pub fn clear(&mut self) {
		self.cache.clear();
	}

	/// Check if item is contained in the cache
	pub fn contains(&self, hash: Block::Hash) -> bool {
		self.cache.peek(&hash).is_some()
	}

	/// Attach body to an existing cache item
	pub fn insert_body(&mut self, hash: Block::Hash, extrinsics: Option<Vec<Block::Extrinsic>>) {
		let result = self.cache.insert(hash, BlockCacheEntry { body: Some(extrinsics) }); 
		log::info!(target:LOG_TARGET, "BlockCache insert block with hash {hash:?} result is {result}");
	}

	/// Get body for cached block
	pub fn body(&self, hash: &Block::Hash) -> Option<&Option<Vec<Block::Extrinsic>>> {
		self.cache.peek(hash).and_then(|entry| entry.body.as_ref())
	}
}
