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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Rust-native complete interface surface from `pkg/kv/kv.go`.
//!
//! Associated types replace Go's `any` and interface-returning methods, while
//! preserving every operation and lifecycle boundary.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU32};
use std::sync::Arc;
use std::time::Duration;

use crate::{
    AssertionOp, BatchGetOptions, BatchGetter, CacheDb, EventCallback, FlagsOp, GetOptions, Getter,
    Key, KeyFlags, KvIterator, MppClient, OptionKey, Request, StagingHandle, TiFlashReplicaRead,
    ValueEntry, Version,
};

/// Returns only the bytes from one point read.
pub fn get_value<G: Getter>(getter: &mut G, key: &Key) -> Result<Vec<u8>, G::Error> {
    getter
        .get(key, GetOptions::default())
        .map(|entry| entry.value)
}

/// Returns only value bytes from one batch read.
pub fn batch_get_value<G: BatchGetter>(
    getter: &mut G,
    keys: &[Key],
) -> Result<HashMap<Key, Vec<u8>>, G::Error> {
    getter
        .batch_get(keys, BatchGetOptions::default())
        .map(|entries| {
            entries
                .into_iter()
                .map(|(key, entry)| (key, entry.value))
                .collect()
        })
}

/// Error identities exposed by [`EmptyRetriever`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EmptyRetrieverError {
    /// Point key is absent.
    NotFound,
    /// Advancing an invalid iterator.
    InvalidIterator,
}

impl fmt::Display for EmptyRetrieverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => formatter.write_str("key not exist"),
            Self::InvalidIterator => formatter.write_str("iterator is invalid"),
        }
    }
}

impl std::error::Error for EmptyRetrieverError {}

impl crate::BatchGetError for EmptyRetrieverError {
    fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound)
    }
}

/// Always-invalid root `pkg/kv.EmptyIterator`.
#[derive(Default)]
pub struct EmptyIterator {
    key: Key,
}

impl KvIterator for EmptyIterator {
    type Error = EmptyRetrieverError;

    fn valid(&self) -> bool {
        false
    }

    fn key(&self) -> &Key {
        &self.key
    }

    fn value(&self) -> &[u8] {
        &[]
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        Err(EmptyRetrieverError::InvalidIterator)
    }

    fn close(&mut self) {}
}

/// Retriever with no entries.
#[derive(Default)]
pub struct EmptyRetriever;

impl Getter for EmptyRetriever {
    type Error = EmptyRetrieverError;

    fn get(&mut self, _: &Key, _: GetOptions) -> Result<ValueEntry, Self::Error> {
        Err(EmptyRetrieverError::NotFound)
    }
}

impl Retriever for EmptyRetriever {
    type Iterator = EmptyIterator;

    fn iter(
        &mut self,
        _: Option<&Key>,
        _: Option<&Key>,
    ) -> Result<Self::Iterator, <Self as Getter>::Error> {
        Ok(EmptyIterator::default())
    }

    fn iter_reverse(
        &mut self,
        _: Option<&Key>,
        _: Option<&Key>,
    ) -> Result<Self::Iterator, <Self as Getter>::Error> {
        Ok(EmptyIterator::default())
    }
}

/// Per-send client controls.
pub struct ClientSendOption<W> {
    /// Session memory tracker.
    pub session_memory_tracker: Option<Arc<AtomicI64>>,
    /// Whether a rate-limit action is active.
    pub rate_limit_action_enabled: bool,
    /// Transaction event callback.
    pub event_callback: Option<EventCallback>,
    /// Whether execution information is collected.
    pub collect_execution_info: bool,
    /// TiFlash replica-read policy.
    pub tiflash_replica_read: TiFlashReplicaRead,
    /// Warning publisher.
    pub append_warning: Option<Box<dyn FnMut(W) + Send>>,
    /// Shared lite-worker selection state.
    pub try_coprocessor_lite_worker: Option<Arc<AtomicU32>>,
}

impl<W> Default for ClientSendOption<W> {
    fn default() -> Self {
        Self {
            session_memory_tracker: None,
            rate_limit_action_enabled: false,
            event_callback: None,
            collect_execution_info: false,
            tiflash_replica_read: TiFlashReplicaRead::default(),
            append_warning: None,
            try_coprocessor_lite_worker: None,
        }
    }
}

/// Coprocessor request client.
pub trait Client {
    /// Request context.
    type Context;
    /// KV variables.
    type Variables;
    /// Response stream.
    type Response: Response<Context = Self::Context>;
    /// Warning type.
    type Warning;

    /// Sends one request to the KV layer.
    fn send(
        &mut self,
        context: &Self::Context,
        request: &Request,
        variables: &mut Self::Variables,
        options: &mut ClientSendOption<Self::Warning>,
    ) -> Self::Response;

    /// Returns whether a request/subtype pair is supported.
    fn is_request_type_supported(&self, request_type: i64, sub_type: i64) -> bool;
}

/// Basic key mutation operations.
pub trait Mutator {
    /// Mutation error.
    type Error;

    /// Sets one non-empty value.
    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), Self::Error>;
    /// Deletes one key.
    fn delete(&mut self, key: Key) -> Result<(), Self::Error>;
}

/// Point and directional range retrieval.
pub trait Retriever: Getter {
    /// Iterator type.
    type Iterator: KvIterator<Error = <Self as Getter>::Error>;

    /// Creates a forward iterator over `[key, upper_bound)`.
    fn iter(
        &mut self,
        key: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Self::Iterator, <Self as Getter>::Error>;

    /// Creates a reverse iterator over `[lower_bound, key)`.
    fn iter_reverse(
        &mut self,
        key: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iterator, <Self as Getter>::Error>;
}

/// Combined retrieval and mutation contract.
pub trait RetrieverMutator: Retriever + Mutator<Error = <Self as Getter>::Error> {}

impl<T> RetrieverMutator for T where T: Retriever + Mutator<Error = <T as Getter>::Error> {}

/// Complete staged in-memory transaction buffer.
pub trait MemBuffer: RetrieverMutator + BatchGetter<Error = <Self as Getter>::Error> {
    /// Snapshot getter.
    type SnapshotGetter: Getter<Error = <Self as Getter>::Error>;
    /// Snapshot iterator.
    type SnapshotIterator: KvIterator<Error = <Self as Getter>::Error>;

    /// Acquires the source shared-read lock.
    fn read_lock(&self);
    /// Releases the source shared-read lock.
    fn read_unlock(&self);
    /// Returns current flags for a key.
    fn flags(&self, key: &Key) -> Result<KeyFlags, <Self as Getter>::Error>;
    /// Sets a value with flag operations.
    fn set_with_flags(
        &mut self,
        key: Key,
        value: Vec<u8>,
        flags: &[FlagsOp],
    ) -> Result<(), <Self as Getter>::Error>;
    /// Updates key flags.
    fn update_flags(&mut self, key: &Key, flags: &[FlagsOp]);
    /// Updates assertion flags.
    fn update_assertion_flags(&mut self, key: &Key, assertion: AssertionOp);
    /// Deletes with flag operations.
    fn delete_with_flags(
        &mut self,
        key: Key,
        flags: &[FlagsOp],
    ) -> Result<(), <Self as Getter>::Error>;
    /// Opens a staging buffer.
    fn staging(&mut self) -> StagingHandle;
    /// Publishes a staging buffer.
    fn release(&mut self, handle: StagingHandle);
    /// Discards a staging buffer.
    fn cleanup(&mut self, handle: StagingHandle);
    /// Visits every update in one stage.
    fn inspect_stage(&self, handle: StagingHandle, visitor: &mut dyn FnMut(&Key, KeyFlags, &[u8]));
    /// Returns a point-read snapshot.
    fn snapshot_getter(&self) -> Self::SnapshotGetter;
    /// Returns a forward snapshot iterator.
    fn snapshot_iter(&self, key: Option<&Key>, upper_bound: Option<&Key>)
        -> Self::SnapshotIterator;
    /// Returns a reverse snapshot iterator.
    fn snapshot_iter_reverse(
        &self,
        key: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Self::SnapshotIterator;
    /// Returns entry count.
    fn len(&self) -> usize;
    /// Returns whether no entries exist.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Returns total key/value bytes.
    fn size(&self) -> usize;
    /// Removes an entry for test control.
    fn remove_from_buffer(&mut self, key: &Key);
    /// Reads only local memory.
    fn get_local(&self, key: &[u8]) -> Result<Vec<u8>, <Self as Getter>::Error>;
}

/// Fair-locking transaction state machine.
pub trait FairLockingController {
    /// Context/cancellation type.
    type Context;
    /// Error type.
    type Error;

    /// Enters fair-locking mode.
    fn start_fair_locking(&mut self) -> Result<(), Self::Error>;
    /// Retries the current fair-locking attempt.
    fn retry_fair_locking(&mut self, context: &Self::Context) -> Result<(), Self::Error>;
    /// Cancels fair-locking mode.
    fn cancel_fair_locking(&mut self, context: &Self::Context) -> Result<(), Self::Error>;
    /// Completes fair-locking mode.
    fn done_fair_locking(&mut self, context: &Self::Context) -> Result<(), Self::Error>;
    /// Returns whether fair-locking mode is active.
    fn is_in_fair_locking_mode(&self) -> bool;
}

/// Complete transaction interface.
pub trait Transaction:
    RetrieverMutator
    + BatchGetter<Error = <Self as Getter>::Error>
    + FairLockingController<Error = <Self as Getter>::Error>
{
    /// Lock context.
    type LockContext;
    /// Dynamically typed option value, made closed by the owner.
    type OptionValue;
    /// KV variable bundle.
    type Variables;
    /// Cached table metadata.
    type TableInfo;
    /// Disk-full policy.
    type DiskFullOption;
    /// MemDB checkpoint.
    type Checkpoint;
    /// Bound memory buffer.
    type Buffer: MemBuffer;
    /// Bound snapshot.
    type Snapshot: Snapshot
        + Getter<Error = <Self as Getter>::Error>
        + BatchGetter<Error = <Self as Getter>::Error>;

    /// Returns buffered key/value bytes.
    fn size(&self) -> usize;
    /// Returns transaction memory consumption.
    fn memory_usage(&self) -> u64;
    /// Installs a memory-footprint hook.
    fn set_memory_footprint_hook(&mut self, hook: Box<dyn FnMut(u64) + Send>);
    /// Returns whether the memory hook is installed.
    fn memory_hook_is_set(&self) -> bool;
    /// Returns buffered entry count.
    fn len(&self) -> usize;
    /// Returns whether no entries are buffered.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Commits the transaction.
    fn commit(
        &mut self,
        context: &<Self as FairLockingController>::Context,
    ) -> Result<(), <Self as Getter>::Error>;
    /// Rolls back the transaction.
    fn rollback(&mut self) -> Result<(), <Self as Getter>::Error>;
    /// Returns the source diagnostic string.
    fn diagnostic_string(&self) -> String;
    /// Locks keys.
    fn lock_keys(
        &mut self,
        context: &<Self as FairLockingController>::Context,
        lock_context: &mut Self::LockContext,
        keys: &[Key],
    ) -> Result<(), <Self as Getter>::Error>;
    /// Locks keys and runs `before_unlock` while locks remain held.
    fn lock_keys_with(
        &mut self,
        context: &<Self as FairLockingController>::Context,
        lock_context: &mut Self::LockContext,
        before_unlock: &mut dyn FnMut(),
        keys: &[Key],
    ) -> Result<(), <Self as Getter>::Error>;
    /// Sets one option.
    fn set_option(&mut self, option: OptionKey, value: Option<Self::OptionValue>);
    /// Gets one option.
    fn option(&self, option: OptionKey) -> Option<&Self::OptionValue>;
    /// Returns whether no writes have occurred.
    fn is_read_only(&self) -> bool;
    /// Returns start timestamp.
    fn start_ts(&self) -> u64;
    /// Returns commit timestamp, or zero before commit.
    fn commit_ts(&self) -> u64;
    /// Returns whether the transaction remains usable.
    fn valid(&self) -> bool;
    /// Returns its memory buffer.
    fn mem_buffer(&mut self) -> &mut Self::Buffer;
    /// Returns its snapshot.
    fn snapshot(&mut self) -> &mut Self::Snapshot;
    /// Sets KV variables.
    fn set_variables(&mut self, variables: Self::Variables);
    /// Gets KV variables.
    fn variables(&self) -> &Self::Variables;
    /// Returns whether pessimistic mode is active.
    fn is_pessimistic(&self) -> bool;
    /// Caches table metadata.
    fn cache_table_info(&mut self, id: i64, info: Self::TableInfo);
    /// Gets cached table metadata.
    fn table_info(&self, id: i64) -> Option<&Self::TableInfo>;
    /// Sets the disk-full policy.
    fn set_disk_full_option(&mut self, option: Self::DiskFullOption);
    /// Clears the disk-full policy.
    fn clear_disk_full_option(&mut self);
    /// Returns a MemDB checkpoint.
    fn mem_db_checkpoint(&self) -> Self::Checkpoint;
    /// Rolls MemDB back to a checkpoint.
    fn rollback_mem_db_to_checkpoint(&mut self, checkpoint: &Self::Checkpoint);
    /// Returns whether pipelined DML is active.
    fn is_pipelined(&self) -> bool;
    /// Flushes a pipelined buffer when thresholds require it.
    fn may_flush(&mut self) -> Result<(), <Self as Getter>::Error>;
}

/// Result from one storage unit.
pub trait ResultSubset {
    /// Raw response bytes.
    fn data(&self) -> &[u8];
    /// Range start key.
    fn start_key(&self) -> &Key;
    /// Accounted memory bytes.
    fn memory_size(&self) -> i64;
    /// Request response time.
    fn response_time(&self) -> Duration;
}

/// Streaming KV response.
pub trait Response {
    /// Request context type.
    type Context;
    /// Result subset.
    type ResultSubset: ResultSubset;
    /// Error type.
    type Error;

    /// Returns the next subset, or `None` at end of stream.
    fn next(&mut self, context: &Self::Context) -> Result<Option<Self::ResultSubset>, Self::Error>;
    /// Closes the response.
    fn close(&mut self) -> Result<(), Self::Error>;
}

/// Snapshot read surface.
pub trait Snapshot: Retriever + BatchGetter<Error = <Self as Getter>::Error> {
    /// Closed option-value type.
    type OptionValue;

    /// Sets a supported snapshot option.
    fn set_option(&mut self, option: OptionKey, value: Option<Self::OptionValue>);
}

/// Snapshot read interceptor.
pub trait SnapshotInterceptor<S>
where
    S: Snapshot,
{
    /// Intercepts point reads.
    fn on_get(
        &mut self,
        snapshot: &mut S,
        key: &Key,
        options: GetOptions,
    ) -> Result<ValueEntry, <S as Getter>::Error>;
    /// Intercepts batch reads.
    fn on_batch_get(
        &mut self,
        snapshot: &mut S,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, <S as Getter>::Error>;
    /// Intercepts forward iteration.
    fn on_iter(
        &mut self,
        snapshot: &mut S,
        key: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<S::Iterator, <S as Getter>::Error>;
    /// Intercepts reverse iteration.
    fn on_iter_reverse(
        &mut self,
        snapshot: &mut S,
        key: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<S::Iterator, <S as Getter>::Error>;
}

/// Storage driver.
pub trait Driver {
    /// Opened storage.
    type Storage: Storage;
    /// Open error.
    type Error;

    /// Opens a storage-specific path.
    fn open(&self, path: &str) -> Result<Self::Storage, Self::Error>;
}

/// Complete storage interface.
pub trait Storage {
    /// Error type.
    type Error;
    /// Transaction option.
    type TransactionOption;
    /// Transaction.
    type Transaction: Transaction;
    /// Snapshot.
    type Snapshot: Snapshot;
    /// Coprocessor client.
    type Client: Client;
    /// MPP client.
    type MppClient: MppClient;
    /// Timestamp oracle.
    type Oracle;
    /// Status value.
    type Status;
    /// Lock-wait entry.
    type LockWait;
    /// Keyspace codec.
    type Codec;
    /// Option key.
    type OptionKey;
    /// Option value.
    type OptionValue;
    /// Request context.
    type Context;

    /// Begins a global transaction.
    fn begin(
        &mut self,
        options: &[Self::TransactionOption],
    ) -> Result<Self::Transaction, Self::Error>;
    /// Creates a snapshot at a version.
    fn snapshot(&self, version: Version) -> Self::Snapshot;
    /// Returns the coprocessor client.
    fn client(&self) -> &Self::Client;
    /// Returns the MPP client.
    fn mpp_client(&self) -> &Self::MppClient;
    /// Closes storage.
    fn close(&mut self) -> Result<(), Self::Error>;
    /// Returns stable storage UUID.
    fn uuid(&self) -> &str;
    /// Returns current committed version in a transaction scope.
    fn current_version(&self, txn_scope: &str) -> Result<Version, Self::Error>;
    /// Returns the timestamp oracle.
    fn oracle(&self) -> &Self::Oracle;
    /// Returns whether delete-range is supported.
    fn supports_delete_range(&self) -> bool;
    /// Returns engine name.
    fn name(&self) -> &str;
    /// Returns engine description.
    fn describe(&self) -> &str;
    /// Returns one status value.
    fn show_status(&self, context: &Self::Context, key: &str) -> Result<Self::Status, Self::Error>;
    /// Returns table snapshot cache.
    fn memory_cache(&self) -> &CacheDb;
    /// Returns minimum SafeTS in a transaction scope.
    fn min_safe_ts(&self, txn_scope: &str) -> u64;
    /// Returns current lock waits.
    fn lock_waits(&self) -> Result<Vec<Self::LockWait>, Self::Error>;
    /// Returns keyspace codec.
    fn codec(&self) -> &Self::Codec;
    /// Sets a storage option.
    fn set_storage_option(&mut self, key: Self::OptionKey, value: Self::OptionValue);
    /// Gets a storage option.
    fn storage_option(&self, key: &Self::OptionKey) -> Option<&Self::OptionValue>;
    /// Returns physical cluster ID.
    fn cluster_id(&self) -> u64;
    /// Returns keyspace name.
    fn keyspace(&self) -> &str;
}

/// Real-TiKV backend extensions.
pub trait EtcdBackend {
    /// Error type.
    type Error;
    /// TLS configuration.
    type TlsConfig;

    /// Etcd endpoints.
    fn etcd_addresses(&self) -> Result<Vec<String>, Self::Error>;
    /// PD endpoints.
    fn pd_addresses(&self) -> Result<Vec<String>, Self::Error>;
    /// TLS configuration.
    fn tls_config(&self) -> Option<&Self::TlsConfig>;
    /// Starts the GC worker.
    fn start_gc_worker(&mut self) -> Result<(), Self::Error>;
}

/// Storage extension exposing PD clients.
pub trait StorageWithPd {
    /// gRPC PD client.
    type PdClient;
    /// HTTP PD client.
    type PdHttpClient;

    /// Returns the gRPC client.
    fn pd_client(&self) -> &Self::PdClient;
    /// Returns the HTTP client.
    fn pd_http_client(&self) -> &Self::PdHttpClient;
}

/// Region-splitting storage extension.
pub trait SplittableStore {
    /// Context type.
    type Context;
    /// Error type.
    type Error;

    /// Splits regions at keys.
    fn split_regions(
        &mut self,
        context: &Self::Context,
        split_keys: &[Vec<u8>],
        scatter: bool,
        table_id: Option<i64>,
    ) -> Result<Vec<u64>, Self::Error>;
    /// Waits for scattering to finish.
    fn wait_scatter_region_finish(
        &self,
        context: &Self::Context,
        region_id: u64,
        backoff: i32,
    ) -> Result<(), Self::Error>;
    /// Returns whether a region is scattering.
    fn region_is_scattering(&self, region_id: u64) -> Result<bool, Self::Error>;
}
