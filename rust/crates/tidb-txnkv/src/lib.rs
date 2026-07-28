// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-backed transactional KV primitives for the TiDB Rust rewrite.
//!
//! This foundation translates complete dependency-closed contracts from
//! `pkg/kv/{key,version,keyflags,assertion,error,checker}.go` plus the bounded
//! transaction-source bitfield in `pkg/kv/option.go`. The bounded RPC and
//! region modules provide one real BatchCommands transport and one PD-backed
//! region route. One concrete process authority now shares its PD worker,
//! maintained RegionCache, lock resolver, and TiKV transport across reads and
//! writes, for both optimistic and pessimistic transactions: a pessimistic
//! transaction acquires statement-scoped locks at its own `for_update_ts` and
//! then finishes through the same two-phase commit.
//!
//! Async commit, 1PC, fair (aggressive) locking's `WakeUpModeForceLock`,
//! pessimistic value caching (`return_values`/`check_existence`), and TLS
//! remain outside this boundary. So does most of the SQL seam. `BEGIN
//! PESSIMISTIC` / `BEGIN OPTIMISTIC` / `@@tidb_txn_mode` now resolve to a mode
//! the session records (`tidb-planner`'s `txn_mode`), and a lock failure maps
//! to the SQL error TiDB reports for it (`tidb-exec`'s
//! `pessimistic_lock_error`), but no SQL statement drives
//! [`transaction::RealPessimisticTransaction`] yet: the real-TiKV node still
//! refuses writes inside an explicit transaction, so there is no statement to
//! take a lock for. That binding waits on multi-statement transactions there.

mod assertion;
mod batch_getter;
mod cache_db;
mod checker;
mod client;
mod counter;
pub mod driver;
mod driver_error;
mod error;
mod fault_injection;
mod go_is_print;
mod handle;
mod inner_txn;
mod iteration;
mod key;
mod key_flags;
mod key_ranges;
mod keyspace;
mod kv_api;
mod kv_contract;
pub mod lock;
mod mem_storage;
mod mpp;
mod mvcc_metadata;
mod new_txn;
mod option;
mod pd_loader;
mod prefix_ops;
mod read_runtime;
pub mod region;
mod resource_group;
mod retry;
pub mod rpc;
mod tiflash;
pub mod transaction;
mod trxevents;
mod txn_scope;
mod txn_source;
mod union_iter;
mod unistore;
mod variables;
mod version;

pub use assertion::AssertionOp;
pub use batch_getter::{
    batch_get_to_get_options, with_return_commit_ts, BatchBufferGetter, BatchGetError,
    BatchGetOption, BatchGetOptions, BatchGetter, BufferBatchGetter, GetOption, GetOptions,
    GetOrBatchGetOption, Getter, ValueEntry,
};
pub use cache_db::{new_cache_db, CacheDb, CacheDbError, MemManager, TABLE_CACHE_CAPACITY_BYTES};
pub use checker::{
    RequestTypeSupportedChecker, REQ_SUB_TYPE_ANALYZE_COL, REQ_SUB_TYPE_ANALYZE_IDX,
    REQ_SUB_TYPE_BASIC, REQ_SUB_TYPE_DESC, REQ_SUB_TYPE_GROUP_BY, REQ_SUB_TYPE_SIGNATURE,
    REQ_SUB_TYPE_TOP_N, REQ_TYPE_ANALYZE, REQ_TYPE_CHECKSUM, REQ_TYPE_DAG, REQ_TYPE_INDEX,
    REQ_TYPE_SELECT,
};
pub use client::{
    endpoint_type, inject_source_stmt, map_replica_read_type, BackoffMetadata,
    ClientReplicaReadType, DirectUnaryClient, DirectUnaryRequest, DirectUnaryResponse,
    DriverDefaults, DriverOptions, EndpointType, PdClientConfig, PdOptions, SecurityConfig,
    TikvClientConfig, TikvDriverConfig, TraceInfo, TxnLocalLatchesConfig,
};
pub use counter::{get_int64, inc_int64, CounterError, CounterStorage};
pub use driver::mem_buffer::{
    EmptyIterator as DriverEmptyIterator, MemBufferBackend, MemBufferDriver,
    MemBufferSnapshotGetter, MemBufferSnapshotIterator, StagingHandle,
};
pub use driver_error::{to_tidb_driver_error, ConvertedDriverError, StorageDriverError};
pub use error::{
    gen_entry_too_large_err, gen_key_exists_err, gen_key_too_large_err, gen_txn_too_large_err,
    gen_write_conflict_in_tidb_err, is_err_not_found, is_txn_retryable_error, ErrorClass, KvError,
    MysqlErrorCode, ERR_ASSERTION_FAILED, ERR_CANNOT_SET_NIL_VALUE, ERR_ENTRY_TOO_LARGE,
    ERR_INVALID_TXN, ERR_KEY_EXISTS, ERR_KEY_TOO_LARGE, ERR_LOCK_EXPIRE, ERR_NOT_EXIST,
    ERR_NOT_IMPLEMENTED, ERR_TXN_RETRYABLE, ERR_TXN_TOO_LARGE, ERR_WRITE_CONFLICT,
    ERR_WRITE_CONFLICT_IN_TIDB, TXN_RETRYABLE_MARK,
};
pub use fault_injection::{
    new_injected_storage, new_injected_store, InjectedSnapshot, InjectedStore, InjectedTransaction,
    InjectionConfig, KvSnapshot, KvStorage, KvTransaction,
};
pub use handle::{
    CommonHandle, Handle, HandleCompareError, HandleMap, IntHandle, MemAwareHandleMap,
    PartitionHandle,
};
pub use inner_txn::{
    get_min_inner_txn_start_ts, long_running_inner_txn, long_running_inner_txns,
    print_long_time_internal_txn, InnerTxnStartTsBox, LongRunningInnerTxn,
    GLOBAL_INNER_TXN_START_TS, TIME_TO_PRINT_LONG_INTERNAL_TXN,
};
pub use iteration::{next_until, row_key_prefix_filter, walk_mem_buffer, KvIterator, KvRetriever};
pub use key::{key_range_slice_mem_usage, Entry, Key, KeyRange};
pub use key_flags::{AssertionState, FlagsOp, KeyFlags};
pub use key_ranges::KeyRanges;
pub use keyspace::{is_system_keyspace, is_user_keyspace, KernelType, SYSTEM_KEYSPACE};
pub use kv_api::{
    batch_get_value, get_value, Client, ClientSendOption, Driver, EmptyIterator, EmptyRetriever,
    EmptyRetrieverError, EtcdBackend, FairLockingController, MemBuffer, Mutator, Response,
    ResultSubset, Retriever, RetrieverMutator, Snapshot, SnapshotInterceptor, SplittableStore,
    Storage, StorageWithPd, Transaction,
};
pub use kv_contract::{
    find_keys_in_stage, set_txn_entry_size_limit, set_txn_total_size_limit, txn_entry_size_limit,
    txn_total_size_limit, CoprocessorRequestAdjuster, IsolationLevel, Paging, PartitionIdAndRanges,
    PartitionedKeyRanges, Priority, Request, RequestType, RunawayAction, RunawayChecker,
    StoreLabel, StoreType, DEFAULT_TXN_ENTRY_SIZE_LIMIT, DEFAULT_TXN_TOTAL_SIZE_LIMIT,
    GLOBAL_REPLICA_SCOPE, UNCOMMITTED_INDEX_KV_FLAG,
};
pub use mem_storage::{MemIterator, MemStorage, MemStorageError};
pub use mpp::{
    CancelMppTasksParam, DispatchMppTaskParam, EstablishMppConnsParam, MppBuildTasksRequest,
    MppClient, MppCoordinator, MppDispatchRequest, MppQueryId, MppTask, MppTaskLocation,
    MppTaskMeta, MppTaskState, MppVersion,
};
pub use mvcc_metadata::{
    decode_extra_txn_status_key, decode_key_ts, encode_extra_txn_status_key, encode_write_cf_value,
    parse_write_cf_value, DbUserMeta, LockType, MvccLockMetadata, MvccMetadataError, WriteCfValue,
    WriteType, FOR_UPDATE_PREFIX, LOCK_USER_META_DELETE, LOCK_USER_META_NONE, MIN_COMMIT_TS_PREFIX,
    SHORT_VALUE_MAX_LEN, SHORT_VALUE_PREFIX,
};
pub use new_txn::{
    retry_backoff_delay, run_in_new_txn, run_in_new_txn_with, set_txn_resource_group, NewTxnError,
    NewTxnStorage, NewTxnTransaction, RunInNewTxnContext, TxnOptionValue, MAX_RETRY_COUNT,
};
pub use option::{
    get_internal_source_type, OptionKey, ReplicaReadType, RequestSource, TxnSizeLimits,
    INTERNAL_DDL_NOTIFIER, INTERNAL_DIST_TASK, INTERNAL_IMPORT_INTO, INTERNAL_LOAD_DATA,
    INTERNAL_TIMER, INTERNAL_TXN_ADMIN, INTERNAL_TXN_BACKFILL_DDL_PREFIX, INTERNAL_TXN_BIND_INFO,
    INTERNAL_TXN_BOOTSTRAP, INTERNAL_TXN_BR, INTERNAL_TXN_CACHE_TABLE, INTERNAL_TXN_DDL,
    INTERNAL_TXN_GC, INTERNAL_TXN_LIGHTNING, INTERNAL_TXN_META, INTERNAL_TXN_OTHERS,
    INTERNAL_TXN_PRIVILEGE, INTERNAL_TXN_STATS, INTERNAL_TXN_STATS_FOREGROUND_PRIORITY,
    INTERNAL_TXN_SYS_VAR, INTERNAL_TXN_TELEMETRY, INTERNAL_TXN_TOOLS, INTERNAL_TXN_TRACE,
    INTERNAL_TXN_TTL, INTERNAL_TXN_WORKLOAD_LEARNING,
};
pub use pd_loader::PdRegionLoader;
pub use prefix_ops::{del_key_with_prefix, scan_meta_with_prefix};
pub use read_runtime::{SharedReadAuthority, SharedReadOpener, SharedReadRuntime};
pub use resource_group::{
    set_decode_table_id, ResourceGroupTagBuilder, ResourceGroupTaggedRequest,
};
pub use retry::{
    retry_backoff_upper_bound_ms, should_retry_after_failure, RETRY_BACKOFF_BASE_MS,
    RETRY_BACKOFF_CAP_MS,
};
pub use rpc::{
    batch::{
        BatchCommandCompletion, BatchCommandEntry, BatchCommandTag, BatchCoprocessorPending,
        BatchInflightError, BatchPublicationReceipt, BatchRoute, BatchWireError,
        OpaqueBatchCommand,
    },
    DirectUnaryClientError, DirectUnaryConnectionError, DirectUnaryGrpcCode,
    DirectUnaryTransportClass, UnaryCallContext, UnaryCancellation, DEFAULT_STORE_LIVENESS_TIMEOUT,
};
pub use tiflash::{
    get_tiflash_replica_read, get_tiflash_replica_read_by_str, ReplicaRead, TiFlashReplicaRead,
    ALL_REPLICAS, CLOSEST_ADAPTIVE, CLOSEST_REPLICAS,
    MAX_REMOTE_READ_COUNT_PER_NODE_FOR_CLOSEST_REPLICAS,
};
pub use trxevents::{
    wrap_cop_meet_lock, CopMeetLock, EventCallback, EventType, TransactionEvent,
    EVENT_TYPE_COP_MEET_LOCK,
};
pub use txn_scope::{TxnScopeVar, GLOBAL_TXN_SCOPE, LOCAL_TXN_SCOPE};
pub use txn_source::{
    get_cdc_write_source, get_lossy_ddl_reorg_source, is_cdc_write_source_set,
    is_lossy_ddl_reorg_source_set, set_cdc_write_source, set_lossy_ddl_reorg_source,
    TxnSourceError, LIGHTNING_PHYSICAL_IMPORT_TXN_SOURCE, LOSSY_DDL_COLUMN_REORG_SOURCE,
};
pub use union_iter::{UnionIter, UnionIterInitError};
pub use unistore::{set_standalone_tidb, standalone_tidb, STANDALONE_TIDB};
pub use variables::{KvVariables, DEFAULT_BACKOFF_LOCK_FAST, DEFAULT_BACKOFF_WEIGHT};
pub use version::{Version, VersionProvider, MAX_VERSION, MIN_VERSION};
