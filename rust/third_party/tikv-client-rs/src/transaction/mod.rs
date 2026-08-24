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
pub(crate) use lock::resolve_locks;
pub(crate) use lock::HasLocks;
pub use priority::Priority;
pub use snapshot::Snapshot;
pub use sync_client::SyncTransactionClient;
pub use sync_snapshot::SyncSnapshot;
pub use sync_transaction::SyncTransaction;
pub use transaction::CheckLevel;
#[doc(hidden)]
pub use transaction::HeartbeatOption;
pub use transaction::Mutation;
pub use transaction::Transaction;
pub use transaction::TransactionOptions;

#[allow(dead_code)]
mod art;
mod buffer;
mod client;
pub(crate) mod latch;
mod lock;
pub mod lowering;
mod priority;
pub(crate) mod range_task;
#[allow(dead_code)]
mod rbt;
mod requests;
pub(crate) use lock::reject_shared_locks;
pub(crate) use lock::resolve_locks_with_ru_details;
pub use lock::LockResolver;
pub use lock::ResolveLocksContext;
pub use lock::ResolveLocksOptions;
#[doc(hidden)]
pub mod arena;
mod snapshot;
mod sync_client;
mod sync_snapshot;
mod sync_transaction;
#[allow(clippy::module_inception)]
mod transaction;
#[allow(dead_code)]
mod unionstore;
