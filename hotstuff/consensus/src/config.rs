#![allow(clippy::borrowed_box)]

use sc_chain_spec::ChainSpec;
use sc_network::types::ProtocolName;

pub(crate) const HOTSTUFF_PROTOCOL_NAME: &str = "/hotstuff/1";

pub fn standard_name<Hash: AsRef<[u8]>>(
	genesis_hash: &Hash,
	chain_spec: &Box<dyn ChainSpec>,
) -> ProtocolName {
	let genesis_hash = genesis_hash.as_ref();
	let chain_prefix = match chain_spec.fork_id() {
		Some(fork_id) => format!("/{}/{}", array_bytes::bytes2hex("", genesis_hash), fork_id),
		None => format!("/{}", array_bytes::bytes2hex("", genesis_hash)),
	};
	format!("{}{}", chain_prefix, HOTSTUFF_PROTOCOL_NAME).into()
}

pub fn hotstuff_peers_set_config(
	protocol_name: ProtocolName,
	max_notification_size: Option<u64>,
) -> sc_network::config::NonDefaultSetConfig {
	sc_network::config::NonDefaultSetConfig::new(protocol_name, max_notification_size.unwrap_or(1024 * 1024 * 5))
}
