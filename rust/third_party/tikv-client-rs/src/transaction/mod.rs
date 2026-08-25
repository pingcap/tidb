// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! Transactional related functionality.
//!
//! Using the [`TransactionClient`](client::Client) you can utilize TiKV's transactional interface.
//!
//! This interface offers SQL-like transactions on top of the raw interface.
//!
//! **Warning:** It is not advisable to use both raw and transactional functionality in the same keyspace.

pub use client::Client;
pub use client::ProtoLockInfo;
pub(crate) use lock::HasLocks;
pub use priority::Priority;
pub use snapshot::Snapshot;
pub use snapshot::SnapshotIterator;
pub use snapshot::SnapshotVisibilityValidator;
pub use snapshot::{DEFAULT_SCAN_BATCH_SIZE, GET_MAX_BACKOFF_MS};
pub use snapshot_stats::SnapshotPoolTaskDetails;
pub use snapshot_stats::SnapshotRpcCommand;
pub use snapshot_stats::SnapshotRuntimeStats;
pub use snapshot_stats::SnapshotScanDetail;
pub use snapshot_stats::SnapshotTimeDetail;
pub use sync_client::SyncTransactionClient;
pub use sync_snapshot::SyncSnapshot;
pub use sync_snapshot::SyncSnapshotIterator;
pub use sync_transaction::SyncTransaction;
pub use transaction::BinlogExecutor;
pub use transaction::BinlogWriteResult;
pub use transaction::CheckLevel;
#[doc(hidden)]
pub use transaction::HeartbeatOption;
pub use transaction::KvFilter;
pub use transaction::LifecycleHooks;
pub use transaction::Mutation;
pub use transaction::MutationAssertion;
pub use transaction::MutationFlags;
pub use transaction::MutationOptions;
pub use transaction::PipelinedTxnOptions;
pub use transaction::PrewriteEncounterLockPolicy;
pub use transaction::RelatedSchemaChange;
pub use transaction::RequestSource;
pub use transaction::SchemaLeaseChecker;
pub use transaction::SchemaVersion;
pub use transaction::SnapshotRequestType;
pub use transaction::SnapshotResourceGroupTagger;
pub use transaction::Transaction;
pub use transaction::TransactionOptions;
pub use transaction::TransactionResourceGroupTagger;
pub use transaction::MAX_TXN_TIME_USE;

#[allow(dead_code)]
mod art;
mod buffer;
mod client;
pub(crate) mod latch;
mod lock;
pub mod lowering;
mod priority;
pub mod range_task;
#[allow(dead_code)]
mod rbt;
mod requests;
pub use lock::extract_lock_from_key_error;
pub use lock::extract_locks_from_key_error;
pub(crate) use lock::reject_shared_locks;
pub(crate) use lock::resolve_locks_for_read_with_context_result;
pub(crate) use lock::resolve_locks_with_context;
pub(crate) use lock::resolve_locks_with_context_result;
pub use lock::Lock;
pub use lock::LockResolver;
pub(crate) use lock::ReadLockContext;
pub use lock::ResolveLocksContext;
pub use lock::ResolveLocksOptions;
pub use lock::ResolvingLock;
pub(crate) use lock::ResolvingLocksGuard;
pub use requests::{TransactionStatus, TransactionStatusKind};
#[doc(hidden)]
pub mod arena;
mod snapshot;
mod snapshot_stats;
mod sync_client;
mod sync_snapshot;
mod sync_transaction;
mod txn_file;
pub use txn_file::{
    build_txn_file_max_backoff_ms, close_txn_file_idle_connections, is_request_source_use_txn_file,
    set_build_txn_file_max_backoff_ms,
};
#[allow(clippy::module_inception)]
mod transaction;
#[allow(dead_code)]
pub mod unionstore;
