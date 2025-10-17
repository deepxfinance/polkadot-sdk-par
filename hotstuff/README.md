# Introduction
A HotStuff-based block finality tool implemented for substrate.
Now we can use it to replace both `Aura/Babe` and `Grandpa`.

# Usage
Hotstuff consensus is divided into three crates:

1. hotstuff-primitives: Defines some common types and traits.
2. hotstuff-consensus: The core of the Hotstuff consensus.
3. pallet-hotstuff: On-chain state management pallet for Hotstuff consensus.

## Runtime
1. Ensure you are not using `pallet_babe` or `pallet_aura` or `pallet_grandpa`.
2. Import types and impl `pallet_hotstuff` for `Runtime`:
```
use hotstuff_primitives::{AuthorityId as HotstuffId, Slot};
...
impl pallet_hotstuff::Config for Runtime {
	type AuthorityId = HotstuffId;

	type DisabledValidators = ();
	type MaxAuthorities = ConstU32<32>;
	type AllowMultipleBlocksPerSlot = ConstBool<false>;

	type WeightInfo = ();
}
...
```
3. Add to macro `construct_runtime!`:
```
construct_runtime!(
	pub struct Runtime
	where
		Block = Block,
		NodeBlock = opaque::Block,
		UncheckedExtrinsic = UncheckedExtrinsic,
	{
		...
		Hotstuff: pallet_hotstuff,
		...
	}
);
```
4. Add rpc to macro `impl_runtime_apis!`:
```
impl_runtime_apis! {
...
	impl hotstuff_primitives::HotstuffApi<Block, HotstuffId> for Runtime {
		fn slot_duration() -> hotstuff_primitives::SlotDuration {
			hotstuff_primitives::SlotDuration::from_millis(Hotstuff::slot_duration())
		}

		fn current_slot() -> Slot {
			Hotstuff::current_slot()
		}

		fn authorities() -> Vec<HotstuffId> {
			Hotstuff::authorities().into_inner()
		}

		fn validate_transactions(
			txs: sp_std::vec::Vec<(TransactionSource, <Block as BlockT>::Extrinsic)>,
			block_hash: <Block as BlockT>::Hash,
		) -> Vec<TransactionValidity> {
			Executive::validate_transactions(txs, block_hash)
		}
	}
...
}
```

## Client service
1. Change `new_partial` return values:
```
pub fn new_partial(
	config: &Configuration,
) -> Result<
	sc_service::PartialComponents<
		...
		(
			hotstuff_consensus::HotstuffBlockImport<FullBackend, Block, FullClient, ProposerFactory>,
			hotstuff_consensus::LinkHalf<Block, FullClient, FullSelectChain>,
			...
		),
	>,
	ServiceError,
> {
    ...
    let proposer_factory: ProposerFactory = ProposerFactory::new(
		task_manager.spawn_handle(),
		client.clone(),
		transaction_pool.clone(),
		None,
		None,
		<ExecutorDispatch as sc_executor::NativeExecutionDispatch>::native_version(),
	);

	let (hotstuff_block_import, hotstuff_link) = hotstuff_consensus::block_import(config.role.clone(), client.clone(), proposer_factory, &client)?;

	let slot_duration = hotstuff_consensus::slot_duration(&*client)?;
	let import_queue =
		hotstuff_consensus::import_queue::import_queue::<HotstuffPair, _, _, _, _, _>(ImportQueueParams {
			block_import: hotstuff_block_import.clone(),
			justification_import: Some(Box::new(hotstuff_block_import.clone())),
			client: client.clone(),
			create_inherent_data_providers: move |_, ()| async move {
				let timestamp = sp_timestamp::InherentDataProvider::from_system_time();

				let slot =
					sp_consensus_aura::inherents::InherentDataProvider::from_timestamp_and_slot_duration(
						*timestamp,
						slot_duration,
					);

				Ok((slot, timestamp))
			},
			spawner: &task_manager.spawn_essential_handle(),
			registry: config.prometheus_registry(),
			check_for_equivocation: Default::default(),
			telemetry: telemetry.as_ref().map(|x| x.handle()),
			compatibility_mode: Default::default(),
		})?;

	Ok(sc_service::PartialComponents {
		...
		other: (hotstuff_block_import, hotstuff_link, ...),
	})
    ...
}
```

2. Run at `new_full`:
```
pub fn new_full(config: Configuration) -> Result<TaskManager, ServiceError> {
    let sc_service::PartialComponents {
		...
		other: (hotstuff_block_import, hotstuff_link, ...),
	} = new_partial(&config)?;
	...
	let hotstuff_protocol_name = hotstuff_consensus::config::standard_name(
		&client.block_hash(0).ok().flatten().expect("Genesis block exists; qed"),
		&config.chain_spec,
	);
	let hotstuff_protocol_config = hotstuff_consensus::config::hotstuff_peers_set_config(hotstuff_protocol_name.clone());
	net_config.add_notification_protocol(hotstuff_protocol_config);
	...
	// Create hotstuff consensus voter & network.
	let proposer_factory: ProposerFactory = ProposerFactory::new(
		task_manager.spawn_handle(),
		client.clone(),
		transaction_pool,
		prometheus_registry.as_ref(),
		telemetry.as_ref().map(|x| x.handle()),
		<ExecutorDispatch as sc_executor::NativeExecutionDispatch>::native_version(),
	);
	let slot_duration = hotstuff_consensus::slot_duration(&*client)?;
	let (voter, hotstuff_network, block_authorship_task) = hotstuff_consensus::consensus::start_hotstuff(
		network,
		hotstuff_link,
		sync_service.clone(),
		hotstuff_block_import,
		sync_service.clone(),
		proposer_factory,
		hotstuff_protocol_name,
		keystore_container.keystore(),
		move |_, extra_args: Timestamp| async move {
			let timestamp = sp_timestamp::InherentDataProvider::new(extra_args);
			let slot =
				hotstuff_primitives::inherents::InherentDataProvider::from_timestamp_and_slot_duration(
					*timestamp,
					slot_duration,
				);

			Ok((slot, timestamp))
		},
		select_chain,
		slot_duration,
	)?;

	// Start hotstuff consensus voter
	task_manager
		.spawn_essential_handle()
		.spawn_blocking("hotstuff block voter", None, voter);

	// Start hotstuff consensus network
	task_manager.spawn_essential_handle().spawn_blocking(
		"hotstuff network",
		None,
		hotstuff_network,
	);

	// Start hotstuff block executor
	task_manager
		.spawn_essential_handle()
		.spawn_blocking(
			"hotstuff block executor",
			None,
			block_authorship_task,
		);
	...
}
```

## Chain spec
Add your HotstuffId to your genesis config `initial_authorities`:
```
pub fn authority_keys_from_seed(s: &str) -> HotstuffId {
	get_from_seed::<HotstuffId>(s)
}
pub fn development_config() -> Result<ChainSpec, String> {
    ...
    testnet_genesis(
        ...
        vec![authority_keys_from_seed("Alice"), authority_keys_from_seed("Bob")],
        ...
    )
    ...
}
```

## Command
Add hotstuff revert to your node command:
```
    ...
    Some(Subcommand::Revert(cmd)) => {
			let runner = cli.create_runner(cmd)?;
			runner.async_run(|config| {
				let PartialComponents { client, task_manager, backend, .. } =
					service::new_partial(&config)?;
				let aux_revert = Box::new(|client: std::sync::Arc<service::FullClient>, _, blocks| {
					// default we request revert_to should be Latest;
					let revert_to = None;
					if revert_to.is_none() && blocks > 0u32.into() {
						return Err(sc_cli::Error::Input("!!!HotstuffRevert to latest, block number should not > 0, please add `0` number to last command".to_string()));
					}
					if let Err(e) = hotstuff_consensus::revert::revert(&client, revert_to) {
						println!("!!!HotstuffRevert failed for {e:?}");
					}
					Ok(())
				});
				Ok((cmd.run(client, backend, Some(aux_revert)), task_manager))
			})
		},
    ...
```

# Environment Variants
## Hotstuff
`HOTSTUFF_DURATION`: Each consensus timeout limit for full `Prepare->Precommit->Commit` process(default 200 millis).

`HOTSTUFF_MAX_BLOCK_DURATION`: Expected max block execution time also means `MaxBlockDuration`, it is not guaranteed(default 200 millis).

`HOTSTUFF_BLOCKS_AHEAD`: Consensus blocks ahead local latest block(1 or 2). For example 2 means when latest is N, we can consensus for N+1 and N+2(default 2 blocks).
Do not set this value greater than 2. Too big `HOTSTUFF_BLOCKS_AHEAD` will influence block consistence and transaction latency.

`HOTSTUFF_TX_VERIFY_PERCENT`: Time percent of `HOTSTUFF_DURATION` for `ExecutionOracle` to decide how many transactions we should get for block(default 75%).

`HOTSTUFF_BLOCK_EXECUTION_PERCENT`: Time percent of `HOTSTUFF_MAX_BLOCK_DURATION` ty to limit our transaction execution time. Also influence transaction queue(default 70%).

## Authorship(Block Execution)
`MTH_THREAD_LIMIT`: Max block execution parallel threads(default cpu threads).

`MTH_DEFAULT_ROUND_TX`: Max execute transactions per round for a single thread, it influences how many runtime we will call.
If this is 0 or not set(default 0), it will be decided by `ExecutionOracle`.

## Log Target
Hotstuff:
`hotstuff`: All hotstuff process info, suggest `info` for clean message, `debug` for better and `trace` for all.
`execution_oracle`: ExecutionOracle update information, `debug` for state update and `trace` for get values log.

ExecuteBlock:
`authorship`: Block execution information. `info` for clean message, `debug` for threads execution process and `trace`(not suggested) for all.