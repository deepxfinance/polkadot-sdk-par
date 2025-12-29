# Summary
List of main changed directories:
```
client
├── basic-authorship
├── block-builder
├── db
├── network
├── rpc
├── rpc-api
├── rpc-spec-v2
├── service
└── transaction-pool
frame
├── executive
├── support
└── system
primitives
├── api
├── application-crypto
├── block-builder
├── core
├── externalities
├── io
├── kv-db
├── runtime
├── runtime-interface
├── state-machine
├── transaction-pool
└── trie
```
Added libraries
```
primitives
├── perp-api
└── spot-api
hotstuff
├── consensus
├── pallets
└── primitives
```
## Changed libraries for `client`
### `basic-authorship`
This library do block authorship works to prepare block. We add a `mth_authorship` to support multi threads transaction execution.
Each block executed by following process: 
1. Initialize: BlockBuilder should call initialize for block which will trigger every pallet's `on_initialize` function.
2. Execute transactions: 

    a. Execute all inherent transactions(e.g. set timestamp).
    
    b. If multi threads transactions exists, we copy current state to each thread and execute transactions by spawn threads. Then we merge threads' results(storage changes) by defined logs(trait `MergeChange`).
    
    c. Execute single thread transactions(can't be executed by parallel) with merged state.
    
    Notice: parallel threads and single thread will trigger another `finalize` work by calling trait `ExtendExtrinsic::extend_extrinsic` to do some extra works(e.g. match market orders).
3. Finalize all pallets and build block(calculate block header by `pallet_system::finalize`).

And it also supports `TransactionGrouper` to group transactions by transaction's `group_info` for multi threads execution.

### `block-builder`
Main block executor. We support extra method `push_batch` to execute batch transactions by single call for faster execution(less runtime env init).

### `db`
Full node database creator, responsible for read state from db or store changes to db(e.g. rocksdb/paritydb). We add feature `kvdb` to decide if we store all `STATE` by key-value mode(no MPT for state proof and no history state).

### `sync`
Main block sync entry. We do small change to check `extrinsics_root` without inherent transaction(currently only one transaction for timestamp) for `hotstuff consensus` have not consensus for inherent transaction.

### `rpc-api` and `rpc`
RPC definition and RPC implementation. We add `submit_extrinsics` to submit transactions by batch for faster submit.

### `service`
Client to get `BlockBuilder` for `basic-authorsip`, we support extra method for multi threads state copy.

### `transaction-pool`
Main transaction pool in cache. It can receive and verify transactions by batch and each transaction should be checked and parse its `group_info` by call trait `RCGroup::call_dependent_data`.

## Changed libraries for `frame`
### `execution`
Functions wrapped for runtime api call(e.g. validate_transaction, apply_extrinsic). User can choose if runtime use this or write other logics.

We support extra functions `apply_extrinsics` for batch transaction runtime execution and `validate_transactions` for batch transaction runtime validation.
For `apply_extrinsic` and `apply_extrinsics`, we add all transaction to block body even transaction fail(e.g. nonce error) for correct `extrinsics_root` with hotstuff-consensus.

### `support/procedural`
Proc macro for runtime function wrap, we no more wrap every runtime api by `frame_support::storage::with_storage_layer` for less storage copy.
This function will copy all state(if inside function used) for state rollback if execution failed.
If some call need rollback ability, we can define it in pallet with feature `#[transactional]`.

### `system`
pallet system for main block state(e.g. block_number, events...). We add storage `EventsMap` to store each block events for kv_mode database since it have no history state.

## Changed libraries for `primitives`
### `api`
Create/Manage RuntimeApi state for native execution to manage storage changes. We add some extra functions to debug/read/copy/remove current storage changes for multi threads execution.

### `application-crypto`
Crypto for runtime execution. We enable extra `bls381` for hotstuff.

### `block-builder`
Runtime api definition for trait `BlockBuilder`. We add `fn apply_extrinsics` for runtime ability of batch transactions execution.

### `core`
Core abilities for runtime. We fix some code to enable `bls` crypto.

### `io`
Low level functions for runtime state management, hash and cryptos. We enable `bls381` here.

### `kv-db`
Extra added low level database management to support store all state by key-value mode to replace MPT.

### `runtime`
Some main types for runtime. We add some transaction errors and enable transaction check without signature verify when executed.

### `runtime-interface/proc-macro`
Proc macro for runtime code generate. We fix it to support feature `bls-experimental` to enable `bls381`.

### `state-machine`
Main state management for runtime when execute block. It keeps all tmp storage changes during block execution.

We add extra abilities to `copy` and `merge` storages changes for multi threads execution and add some execution debugs.

### `transaction-pool`
This is a runtime api definition for transaction pool. We add `fn validate_transactions` for batch runtime transactions validate ability.

### `trie`
MPT trie storage entry for runtime execution. We add kv_cache to support kv_mode database. 

## Added libraries for `primitives`
### `perp-api`
Extra runtime api definition for native call to match perp orders.

### `spot-api`
Extra runtime api definition for native call to match spot orders.

## Hotstuff
### `consensus`
Main hotstuff consensus executor. It can be used to replace `Aura`/`Babe` to generate block and replace `Grandpa` to finalize block(since there is no chain forks).

It consensus for blocks, execute and import block. Special block check/import logics are also defined here.

### `pallet/hotstuff`
Runtime pallet to record/change hotstuff authorities on chain.

### `primitives`
Primitive types definition for hotstuff consensus.