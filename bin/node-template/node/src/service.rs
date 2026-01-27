//! Service and ServiceFactory implementation. Specialized wrapper over substrate service.

use node_template_runtime::{self, opaque::Block, RuntimeApi};
use sc_client_api::BlockBackend;
use hotstuff_consensus::import_queue::ImportQueueParams;
use hotstuff_consensus::AuthorityPair as HotstuffPair;
use hotstuff_consensus::oracle::HotstuffOracle;
pub use sc_executor::NativeElseWasmExecutor;
use sc_service::{error::Error as ServiceError, Configuration, TaskManager};
use sc_telemetry::{Telemetry, TelemetryWorker};
use std::sync::Arc;
use sc_basic_authorship::ExecutionOracle;
use sp_consensus::DisableProofRecording;
use sp_timestamp::Timestamp;
use crate::extra_execute::ExtraLogics;
use crate::merge_handler::MergeHandler;

// Our native executor instance.
pub struct ExecutorDispatch;

impl sc_executor::NativeExecutionDispatch for ExecutorDispatch {
	/// Only enable the benchmarking host functions when we actually want to benchmark.
	#[cfg(feature = "runtime-benchmarks")]
	type ExtendHostFunctions = frame_benchmarking::benchmarking::HostFunctions;
	/// Otherwise we only use the default Substrate host functions.
	#[cfg(not(feature = "runtime-benchmarks"))]
	type ExtendHostFunctions = ();

	fn dispatch(method: &str, data: &[u8]) -> Option<Vec<u8>> {
		node_template_runtime::api::dispatch(method, data)
	}

	fn native_version() -> sc_executor::NativeVersion {
		node_template_runtime::native_version()
	}
}

pub(crate) type FullClient =
	sc_service::TFullClient<Block, RuntimeApi, NativeElseWasmExecutor<ExecutorDispatch>>;
type FullBackend = sc_service::TFullBackend<Block>;
type FullSelectChain = sc_consensus::LongestChain<FullBackend, Block>;
type FullPool = sc_transaction_pool::FullPool<Block, FullClient>;
type Oracle = HotstuffOracle<Block, ExecutionOracle<Block>>;
type ProposerFactory = sc_basic_authorship::MTHProposerFactory<
	FullPool,
	FullBackend,
	FullClient,
	DisableProofRecording,
	Oracle,
	MergeHandler<FullBackend, Block>,
	ExtraLogics,
>;

type BlockExecutor = sc_basic_authorship::BlockExecutor<
	FullBackend,
	FullClient,
	FullPool,
	DisableProofRecording,
	MergeHandler<FullBackend, Block>,
	ExtraLogics,
>;

pub fn new_partial(
	config: &Configuration,
) -> Result<
	sc_service::PartialComponents<
		FullClient,
		FullBackend,
		FullSelectChain,
		sc_consensus::DefaultImportQueue<Block, FullClient>,
		FullPool,
		(
			hotstuff_consensus::HotstuffBlockImport<FullBackend, Block, FullClient, BlockExecutor, Oracle>,
			//LinkHalf is something like `client`, `backend`, `network`, and other components needed by the Hotstuff consensus.
			hotstuff_consensus::LinkHalf<Block, FullClient, FullSelectChain>,
			Arc<Oracle>,
			Option<Telemetry>,
		),
	>,
	ServiceError,
> {
	let telemetry = config
		.telemetry_endpoints
		.clone()
		.filter(|x| !x.is_empty())
		.map(|endpoints| -> Result<_, sc_telemetry::Error> {
			let worker = TelemetryWorker::new(16)?;
			let telemetry = worker.handle().new_telemetry(endpoints);
			Ok((worker, telemetry))
		})
		.transpose()?;

	let executor = sc_service::new_native_or_wasm_executor(&config);
	let (client, backend, keystore_container, task_manager) =
		sc_service::new_full_parts::<Block, RuntimeApi, _>(
			config,
			telemetry.as_ref().map(|(_, telemetry)| telemetry.handle()),
			executor,
		)?;
	let client = Arc::new(client);

	let telemetry = telemetry.map(|(worker, telemetry)| {
		task_manager.spawn_handle().spawn("telemetry", None, worker.run());
		telemetry
	});

	let select_chain = sc_consensus::LongestChain::new(backend.clone());

	let transaction_pool: Arc<FullPool> = sc_transaction_pool::BasicPool::new_full(
		config.transaction_pool.clone(),
		config.role.is_authority().into(),
		config.prometheus_registry(),
		task_manager.spawn_essential_handle(),
		client.clone(),
	);
	let executor = BlockExecutor::new(
		Box::new(task_manager.spawn_handle()),
		client.clone(),
		transaction_pool.clone(),
		<ExecutorDispatch as sc_executor::NativeExecutionDispatch>::native_version(),
		config.execution_strategies.clone(),
		Default::default(),
		None,
	);
	let hotstuff_oracle = Arc::new(HotstuffOracle::new(Arc::new(ExecutionOracle::new(None)), None));
	let (hotstuff_block_import, hotstuff_link) = hotstuff_consensus::block_import(config.role.clone(), client.clone(), executor, hotstuff_oracle.clone(), &client)?;

	let slot_duration = hotstuff_consensus::slot_duration(&*client)?;
	let import_queue =
		hotstuff_consensus::import_queue::import_queue::<HotstuffPair, _, _, _, _, _>(ImportQueueParams {
			block_import: hotstuff_block_import.clone(),
			justification_import: Some(Box::new(hotstuff_block_import.clone())),
			client: client.clone(),
			create_inherent_data_providers: move |_, extra_args: Timestamp| async move {
				let timestamp = sp_timestamp::InherentDataProvider::new(extra_args);
				let slot =
					hotstuff_primitives::inherents::InherentDataProvider::from_timestamp_and_slot_duration(
						*timestamp,
						slot_duration,
					);

				Ok((slot, timestamp))
			},
			persistent_data: hotstuff_link.persistent_data.clone(),
			spawner: &task_manager.spawn_essential_handle(),
			registry: config.prometheus_registry(),
			check_for_equivocation: Default::default(),
			telemetry: telemetry.as_ref().map(|x| x.handle()),
			compatibility_mode: Default::default(),
		})?;

	Ok(sc_service::PartialComponents {
		client,
		backend,
		task_manager,
		import_queue,
		keystore_container,
		select_chain,
		transaction_pool,
		other: (hotstuff_block_import, hotstuff_link, hotstuff_oracle, telemetry),
	})
}

/// Builds a new service for a full client.
pub fn new_full(config: Configuration) -> Result<TaskManager, ServiceError> {
	let sc_service::PartialComponents {
		client,
		backend,
		mut task_manager,
		import_queue,
		keystore_container,
		select_chain,
		transaction_pool,
		other: (hotstuff_block_import, hotstuff_link, hotstuff_oracle, mut telemetry),
	} = new_partial(&config)?;

	let mut net_config = sc_network::config::FullNetworkConfiguration::new(&config.network);

	let hotstuff_protocol_name = hotstuff_consensus::config::standard_name(
		&client.block_hash(0).ok().flatten().expect("Genesis block exists; qed"),
		&config.chain_spec,
	);
	let hotstuff_protocol_config = hotstuff_consensus::config::hotstuff_peers_set_config(hotstuff_protocol_name.clone(), None);
	net_config.add_notification_protocol(hotstuff_protocol_config);

	let (network, system_rpc_tx, tx_handler_controller, network_starter, sync_service) =
		sc_service::build_network(sc_service::BuildNetworkParams {
			config: &config,
			net_config,
			client: client.clone(),
			transaction_pool: transaction_pool.clone(),
			spawn_handle: task_manager.spawn_handle(),
			import_queue,
			block_announce_validator_builder: None,
			warp_sync_params: None,
		})?;

	if config.offchain_worker.enabled {
		sc_service::build_offchain_workers(
			&config,
			task_manager.spawn_handle(),
			client.clone(),
			network.clone(),
		);
	}

	// let force_authoring = config.force_authoring;
	// let backoff_authoring_blocks: Option<()> = None;
	let prometheus_registry = config.prometheus_registry().cloned();

	let rpc_extensions_builder = {
		let client = client.clone();
		let pool = transaction_pool.clone();

		Box::new(move |deny_unsafe, _| {
			let deps =
				crate::rpc::FullDeps { client: client.clone(), pool: pool.clone(), deny_unsafe };
			crate::rpc::create_full(deps).map_err(Into::into)
		})
	};

	let execution_strategies = config.execution_strategies.clone();
	let _rpc_handlers = sc_service::spawn_tasks(sc_service::SpawnTasksParams {
		network: network.clone(),
		client: client.clone(),
		keystore: keystore_container.keystore(),
		task_manager: &mut task_manager,
		transaction_pool: transaction_pool.clone(),
		rpc_builder: rpc_extensions_builder,
		backend,
		system_rpc_tx,
		tx_handler_controller,
		sync_service: sync_service.clone(),
		config,
		telemetry: telemetry.as_mut(),
	})?;

	// Create hotstuff consensus voter & network.
	let proposer_factory: ProposerFactory = ProposerFactory::new(
		task_manager.spawn_handle(),
		client.clone(),
		transaction_pool.clone(),
		hotstuff_oracle.clone(),
		prometheus_registry.as_ref(),
		telemetry.as_ref().map(|x| x.handle()),
		<ExecutorDispatch as sc_executor::NativeExecutionDispatch>::native_version(),
		execution_strategies,
	);
	let slot_duration = hotstuff_consensus::slot_duration(&*client)?;
	let max_empty = hotstuff_consensus::max_empty(&*client)?;
	let (voter, hotstuff_network, block_authorship_task) = hotstuff_consensus::consensus::start_hotstuff(
		network,
		hotstuff_link,
		sync_service.clone(),
		hotstuff_block_import,
		sync_service.clone(),
		hotstuff_oracle,
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
		max_empty,
		None,
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

	network_starter.start_network();
	Ok(task_manager)
}
