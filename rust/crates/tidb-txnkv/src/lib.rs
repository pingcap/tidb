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
//! normal optimistic 2PC writes. Pessimistic transactions, async commit, 1PC,
//! large-transaction TTL management, and TLS remain outside this boundary.

mod assertion;
mod batch_getter;
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
pub mod lock;
mod mvcc_metadata;
mod pd_loader;
mod prefix_ops;
mod read_runtime;
pub mod region;
mod resource_group;
mod retry;
pub mod rpc;
pub mod transaction;
mod txn_scope;
mod txn_source;
mod union_iter;
mod version;

pub use assertion::AssertionOp;
pub use batch_getter::{
    BatchBufferGetter, BatchGetError, BatchGetOptions, BatchGetter, BufferBatchGetter, Getter,
    ValueEntry,
};
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
    EmptyIterator, MemBufferBackend, MemBufferDriver, MemBufferSnapshotGetter,
    MemBufferSnapshotIterator, StagingHandle,
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
    new_injected_store, InjectedSnapshot, InjectedStore, InjectedTransaction, InjectionConfig,
    KvSnapshot, KvStorage, KvTransaction,
};
pub use handle::{CommonHandle, Handle, HandleCompareError, HandleMap, IntHandle, PartitionHandle};
pub use inner_txn::{get_min_inner_txn_start_ts, InnerTxnStartTsBox};
pub use iteration::{next_until, row_key_prefix_filter, walk_mem_buffer, KvIterator, KvRetriever};
pub use key::{Key, KeyRange};
pub use key_flags::{AssertionState, FlagsOp, KeyFlags};
pub use key_ranges::KeyRanges;
pub use keyspace::{is_system_keyspace, is_user_keyspace, KernelType, SYSTEM_KEYSPACE};
pub use mvcc_metadata::{
    decode_extra_txn_status_key, decode_key_ts, encode_extra_txn_status_key, encode_write_cf_value,
    parse_write_cf_value, DbUserMeta, LockType, MvccLockMetadata, MvccMetadataError, WriteCfValue,
    WriteType, FOR_UPDATE_PREFIX, LOCK_USER_META_DELETE, LOCK_USER_META_NONE, MIN_COMMIT_TS_PREFIX,
    SHORT_VALUE_MAX_LEN, SHORT_VALUE_PREFIX,
};
pub use pd_loader::PdRegionLoader;
pub use prefix_ops::{del_key_with_prefix, scan_meta_with_prefix};
pub use read_runtime::{SharedReadAuthority, SharedReadOpener, SharedReadRuntime};
pub use resource_group::ResourceGroupTagBuilder;
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
pub use txn_scope::{TxnScopeVar, GLOBAL_TXN_SCOPE, LOCAL_TXN_SCOPE};
pub use txn_source::{
    get_cdc_write_source, get_lossy_ddl_reorg_source, is_cdc_write_source_set,
    is_lossy_ddl_reorg_source_set, set_cdc_write_source, set_lossy_ddl_reorg_source,
    TxnSourceError, LIGHTNING_PHYSICAL_IMPORT_TXN_SOURCE, LOSSY_DDL_COLUMN_REORG_SOURCE,
};
pub use union_iter::{UnionIter, UnionIterInitError};
pub use version::{Version, MAX_VERSION, MIN_VERSION};
