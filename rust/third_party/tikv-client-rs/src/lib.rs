// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate provides an easy-to-use client for [TiKV](https://github.com/tikv/tikv), a
//! distributed, transactional key-value database written in Rust.
//!
//! This crate lets you connect to a TiKV cluster and use either a transactional or raw (simple
//! get/put style without transactional consistency guarantees) API to access and update your data.
//!
//! The TiKV Rust client supports several levels of abstraction. The most convenient way to use the
//! client is via [`RawClient`] and [`TransactionClient`]. This gives a very high-level API which
//! mostly abstracts over the distributed nature of the store and has sensible defaults for all
//! protocols. This interface can be configured, primarily when creating the client or transaction
//! objects via the [`Config`] and [`TransactionOptions`] structs. Using some options, you can take
//! over parts of the protocols (such as retrying failed messages) yourself.
//!
//! The lowest level of abstraction is to create and send gRPC messages directly to TiKV (and PD)
//! nodes. The `tikv-client-store` and `tikv-client-pd` crates make this easier than using the
//! protobuf definitions and a gRPC library directly, but give you the same level of control.
//!
//! In between these levels of abstraction, you can send and receive individual messages to the TiKV
//! cluster, but take advantage of library code for common operations such as resolving data to
//! regions and thus nodes in the cluster, or retrying failed messages. This can be useful for
//! testing a TiKV cluster or for some advanced use cases. See the [`request`] module for
//! this API, and [`raw::lowering`] and [`transaction::lowering`] for
//! convenience methods for creating request objects.
//!
//! ## Choosing an API
//!
//! This crate offers both [raw](RawClient) and
//! [transactional](Transaction) APIs. You should choose just one for your system.
//!
//! The consequence of supporting transactions is increased overhead of coordination with the
//! placement driver and TiKV, and additional code complexity.
//!
//! *While it is possible to use both APIs at the same time, doing so is unsafe and unsupported.*
//!
//! ### Transactional
//!
//! The [transactional](Transaction) API supports **transactions** via multi-version
//! concurrency control (MVCC).
//!
//! Best when you mostly do complex sets of actions, actions which may require a rollback,
//! operations affecting multiple keys or values, or operations that depend on strong consistency.
//!
//!
//! ### Raw
//!
//! The [raw](RawClient) API has reduced coordination overhead, but lacks any
//! transactional abilities.
//!
//! Best when you mostly do single value changes, and have very limited cross-value
//! requirements. You will not be able to use transactions with this API.
//!
//! ## Usage
//!
//! The general flow of using the client crate is to create either a raw or transaction client
//! object (which can be configured) then send commands using the client object, or use it to create
//! transactions objects. In the latter case, the transaction is built up using various commands and
//! then committed (or rolled back).
//!
//! ### Examples
//!
//! Raw mode:
//!
//! ```rust,no_run
//! # use tikv_client::{RawClient, Result};
//! # use futures::prelude::*;
//! # fn main() -> Result<()> {
//! # futures::executor::block_on(async {
//! let client = RawClient::new(vec!["127.0.0.1:2379"]).await?;
//! client.put("key".to_owned(), "value".to_owned()).await?;
//! let value = client.get("key".to_owned()).await?;
//! # Ok(())
//! # })}
//! ```
//!
//! Transactional mode:
//!
//! ```rust,no_run
//! # use tikv_client::{TransactionClient, Result};
//! # use futures::prelude::*;
//! # fn main() -> Result<()> {
//! # futures::executor::block_on(async {
//! let txn_client = TransactionClient::new(vec!["127.0.0.1:2379"]).await?;
//! let mut txn = txn_client.begin_optimistic().await?;
//! txn.put("key".to_owned(), "value".to_owned()).await?;
//! let value = txn.get("key".to_owned()).await?;
//! txn.commit().await?;
//! # Ok(())
//! # })}
//! ```

#![allow(clippy::field_reassign_with_default)]
#![allow(clippy::result_large_err)]

#[doc(hidden)]
pub mod async_util;
pub mod backoff;
pub mod error;
pub mod interceptor;
#[doc(hidden)]
pub mod intest;
#[doc(hidden)]
pub mod israce;
#[doc(hidden)]
pub mod raw;
pub mod redact;
pub mod request;
pub mod retry;
pub mod trace;
#[doc(hidden)]
pub mod transaction;

mod common;
mod compat;
pub mod config;
pub mod kv;
pub mod locate;
pub mod oracle;
pub mod pd;
mod proto;
pub mod region;
pub mod region_cache;
mod resource_control;
mod stats;
pub mod store;
mod timestamp;
pub mod util;

#[cfg(any(test, feature = "mock"))]
pub mod mock;
#[cfg(test)]
mod proptests;

#[doc(inline)]
pub use common::security::SecurityManager;
#[doc(inline)]
pub use common::Error;
#[doc(inline)]
pub use common::ProtoKeyError;
#[doc(inline)]
pub use common::ProtoRegionError;
#[doc(inline)]
pub use common::Result;
#[doc(inline)]
pub use config::Config;
#[doc(inline)]
pub use config::RawApiVersion;
#[doc(inline)]
pub use config::TxnLocalLatches;
#[doc(inline)]
pub use util::RuDetails;

#[doc(inline)]
pub use crate::backoff::Backoff;
#[doc(inline)]
pub use crate::interceptor::{
    chain_rpc_interceptors, new_rpc_interceptor, RpcDispatchResult, RpcInterceptor,
    RpcInterceptorChain, RpcInterceptorHandle, RpcNext,
};
#[doc(inline)]
pub use crate::kv::codec;
#[doc(inline)]
pub use crate::kv::AccessLocationType;
#[doc(inline)]
pub use crate::kv::BatchGetOptions;
#[doc(inline)]
pub use crate::kv::BoundRange;
#[doc(inline)]
pub use crate::kv::DeadlockError;
#[doc(inline)]
pub use crate::kv::FlagsOp;
#[doc(inline)]
pub use crate::kv::GetOption;
#[doc(inline)]
pub use crate::kv::GetOptions;
#[doc(inline)]
pub use crate::kv::IntoOwnedRange;
#[doc(inline)]
pub use crate::kv::Key;
#[doc(inline)]
pub use crate::kv::KeyFlags;
#[doc(inline)]
pub use crate::kv::KeyRange;
#[doc(inline)]
pub use crate::kv::KvPair;
#[doc(inline)]
pub use crate::kv::LockContext;
#[doc(inline)]
pub use crate::kv::ReturnedValue;
#[doc(inline)]
pub use crate::kv::Value;
#[doc(inline)]
pub use crate::kv::ValueEntry;
#[doc(inline)]
pub use crate::kv::Variables;
#[doc(inline)]
pub use crate::kv::{
    ReplicaReadAdjuster, ReplicaReadAdjustment, ReplicaReadConfig, ReplicaReadSelectorOption,
    ReplicaReadType,
};
#[doc(inline)]
pub use crate::pd::PdClient;
#[doc(inline)]
pub use crate::raw::lowering as raw_lowering;
#[doc(inline)]
pub use crate::raw::Client as RawClient;
#[doc(inline)]
pub use crate::raw::ColumnFamily;
#[doc(inline)]
pub use crate::raw::RawChecksum;
#[doc(inline)]
pub use crate::request::RetryOptions;
#[doc(inline)]
pub use crate::resource_control::{
    disable_resource_control, enable_resource_control, set_resource_control_interceptor,
    unset_resource_control_interceptor, RequestInfo as ResourceControlRequestInfo,
    RequestWaitResult, ResourceGroupController, ResourceGroupControllerHandle,
    ResponseInfo as ResourceControlResponseInfo, ResponseWaitResult,
};
#[doc(inline)]
pub use crate::timestamp::Timestamp;
#[doc(inline)]
pub use crate::timestamp::TimestampExt;
#[doc(inline)]
pub use crate::transaction::lowering as transaction_lowering;
#[doc(inline)]
pub use crate::transaction::CheckLevel;
#[doc(inline)]
pub use crate::transaction::Client as TransactionClient;
#[doc(inline)]
pub use crate::transaction::Priority;
#[doc(inline)]
pub use crate::transaction::ProtoLockInfo;
#[doc(inline)]
pub use crate::transaction::Snapshot;
#[doc(inline)]
pub use crate::transaction::SyncSnapshot;
#[doc(inline)]
pub use crate::transaction::SyncTransaction;
#[doc(inline)]
pub use crate::transaction::SyncTransactionClient;
#[doc(inline)]
pub use crate::transaction::Transaction;
#[doc(inline)]
pub use crate::transaction::TransactionOptions;
pub use crate::transaction::{extract_lock_from_key_error, extract_locks_from_key_error};
