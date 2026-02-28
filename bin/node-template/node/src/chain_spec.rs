use node_template_runtime::{
	AccountId, BalancesConfig, GenesisConfig, Signature, SudoConfig,
	SystemConfig, QuotaConfig, WASM_BINARY,
};
use sc_service::ChainType;
use hotstuff_consensus::AuthorityId as HotstuffId;
use hotstuff_primitives::RuntimeAuthorityId;
use sp_core::{sr25519, Pair, Public};
use sp_runtime::traits::{IdentifyAccount, Verify};

// The URL for the telemetry server.
// const STAGING_TELEMETRY_URL: &str = "wss://telemetry.polkadot.io/submit/";

/// Specialized `ChainSpec`. This is a specialization of the general Substrate ChainSpec type.
pub type ChainSpec = sc_service::GenericChainSpec<GenesisConfig>;

/// Generate a crypto pair from seed.
pub fn get_from_seed<TPublic: Public>(seed: &str) -> <TPublic::Pair as Pair>::Public {
	TPublic::Pair::from_string(&format!("//{}", seed), None)
		.expect("static values are valid; qed")
		.public()
}

type AccountPublic = <Signature as Verify>::Signer;

/// Generate an account ID from seed.
pub fn get_account_id_from_seed<TPublic: Public>(seed: &str) -> AccountId
where
	AccountPublic: From<<TPublic::Pair as Pair>::Public>,
{
	AccountPublic::from(get_from_seed::<TPublic>(seed)).into_account()
}

/// Generate an authority key.
/// Hotstuff BLS not supported.
pub fn authority_keys_from_seed(s: &str) -> HotstuffId {
	get_from_seed::<HotstuffId>(s)
}

pub fn authority_keys_from_str(id: &str) -> HotstuffId {
	let id = id.strip_prefix("0x").expect("should have id");
	RuntimeAuthorityId(hex::decode(id).unwrap()).into()
}

pub fn development_config() -> Result<ChainSpec, String> {
	let wasm_binary = WASM_BINARY.ok_or_else(|| "Development wasm not available".to_string())?;

	Ok(ChainSpec::from_genesis(
		// Name
		"Development",
		// ID
		"dev",
		ChainType::Development,
		move || {
			testnet_genesis(
				wasm_binary,
				// Initial PoA authorities
				vec![
					// seed: 0xf9feb7df9c22a42b13ddb756333b72e2bed7107ad47e27d0ee374fcde7b845e3
					authority_keys_from_str("0xb924a64b63d509c53c4ddcaf2250d5c470214772dde76611d5e608cfaddf7f2a068218a333c3a13caf54a5ba77db739e03af59c6963496af6860489a6e1f79fabc793802975a2e56e0e4b10f732cfb619c85b349f0ae65b7850fec1f4e19eeb1"),
				],
				// Sudo account
				get_account_id_from_seed::<sr25519::Public>("Alice"),
				// Pre-funded accounts
				vec![
					get_account_id_from_seed::<sr25519::Public>("Alice"),
					get_account_id_from_seed::<sr25519::Public>("Bob"),
					get_account_id_from_seed::<sr25519::Public>("Alice//stash"),
					get_account_id_from_seed::<sr25519::Public>("Bob//stash"),
				],
				true,
			)
		},
		// Bootnodes
		vec![],
		// Telemetry
		None,
		// Protocol ID
		None,
		None,
		// Properties
		None,
		// Extensions
		None,
	))
}

pub fn local_testnet_config() -> Result<ChainSpec, String> {
	let wasm_binary = WASM_BINARY.ok_or_else(|| "Development wasm not available".to_string())?;

	Ok(ChainSpec::from_genesis(
		// Name
		"Local Testnet",
		// ID
		"local_testnet",
		ChainType::Local,
		move || {
			testnet_genesis(
				wasm_binary,
				// Initial PoA authorities
				vec![
					// seed: 0xf9feb7df9c22a42b13ddb756333b72e2bed7107ad47e27d0ee374fcde7b845e3
					authority_keys_from_str("0xb924a64b63d509c53c4ddcaf2250d5c470214772dde76611d5e608cfaddf7f2a068218a333c3a13caf54a5ba77db739e03af59c6963496af6860489a6e1f79fabc793802975a2e56e0e4b10f732cfb619c85b349f0ae65b7850fec1f4e19eeb1"),
					// seed: 0x0427d3a91a2338878ecd27abcc5fce1a8cbbc4422d6f50ae6a6af53cb54bbda1
					authority_keys_from_str("0xb72ebf82ca150fedba25627692bd2047e3c3ad672d02ee07437f09787ad441730c99a1a40aa2d9867f1d32fd86c20a690605ba4754fa25e824af7486b261beeb0feeef9beb229d0c04f325ebd94aa32a85f30cfd1e46a8678709b265e6e735f2"),
					// seed: 0x34ac80e2d28566637259c726a7febfe3a3fd0746174f54307449427a7fe147aa
					authority_keys_from_str("0xa7ca99b5b874a2c83d0ad646e78757ca47b93ce7df59bd29ec4946606cbdd3ca6b88c442d61af31a8745f0507bcaf75f037d1bd552a64971bbfc0497eef9c2dd7e4826731b4af24635bb5433b5ec0e87443c4e67c91a500d525b27964627bb98"),
					// seed: 0xa450812cd1ff9a08ee7abe0cea32ad1a2dc48d1dae06e15752a8fef3de7df9f4
					authority_keys_from_str("0x936b4b21974369ec2f78cc972cb75cc86b71887d2596bd3410a49a7283da1737c7ce19b369b549fc7c53017cffe1c3c60c3b41e05efc75d7ce2cec9d8779488ecccdcd5a2bfb861e09f42cd44065db596d653fd2d3333e78a08021044cfff6a9"),
					// seed: 0xa2a04e87fa6f698a7247d7df8475b37df661ebeaae6db21d8d0d929df484c1f6
					authority_keys_from_str("0x8fd9271b34c7983240cb136dbacbd9df26e27a58621484c4bd54b3c1cfbf8144305eafb61b6190081062f674a730241e0ceb538d4c7b557f1ad41cfa969619676abd43660b36b19bca61deff3463f65d900fa82591031707f2d57313840f6432"),
				],
				// Sudo account
				get_account_id_from_seed::<sr25519::Public>("Alice"),
				// Pre-funded accounts
				vec![
					get_account_id_from_seed::<sr25519::Public>("Alice"),
					get_account_id_from_seed::<sr25519::Public>("Bob"),
					get_account_id_from_seed::<sr25519::Public>("Charlie"),
					get_account_id_from_seed::<sr25519::Public>("Dave"),
					get_account_id_from_seed::<sr25519::Public>("Eve"),
					get_account_id_from_seed::<sr25519::Public>("Ferdie"),
					get_account_id_from_seed::<sr25519::Public>("Alice//stash"),
					get_account_id_from_seed::<sr25519::Public>("Bob//stash"),
					get_account_id_from_seed::<sr25519::Public>("Charlie//stash"),
					get_account_id_from_seed::<sr25519::Public>("Dave//stash"),
					get_account_id_from_seed::<sr25519::Public>("Eve//stash"),
					get_account_id_from_seed::<sr25519::Public>("Ferdie//stash"),
				],
				true,
			)
		},
		// Bootnodes
		vec![],
		// Telemetry
		None,
		// Protocol ID
		None,
		// Properties
		None,
		None,
		// Extensions
		None,
	))
}

/// Configure initial storage state for FRAME modules.
fn testnet_genesis(
	wasm_binary: &[u8],
	initial_authorities: Vec<HotstuffId>,
	root_key: AccountId,
	endowed_accounts: Vec<AccountId>,
	_enable_println: bool,
) -> GenesisConfig {
	GenesisConfig {
		system: SystemConfig {
			// Add Wasm runtime to storage.
			code: wasm_binary.to_vec(),
		},
		quota: QuotaConfig {
			quotas: endowed_accounts.iter().cloned().map(|k| (k, u32::MAX - 1)).collect(),
		},
		balances: BalancesConfig {
			// Configure endowed accounts with initial balance of 1 << 60.
			balances: endowed_accounts.iter().cloned().map(|k| (k, 1 << 60)).collect(),
		},
		hotstuff: pallet_hotstuff::GenesisConfig {
			authorities: initial_authorities.iter().map(|x| x.clone().into()).collect(),
		},
		sudo: SudoConfig {
			// Assign network admin rights.
			key: Some(root_key),
		},
		transaction_payment: Default::default(),
	}
}
