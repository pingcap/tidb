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

//! The cluster-backed [`TableStorage`]: a session-owned staging buffer over a
//! statement snapshot, which is Go's `unionStore` shape (`kv.MemBuffer` in
//! front of `kv.Snapshot`) expressed at this tier's seam.
//!
//! # The lifecycle this encodes
//!
//! Go's session holds one `kv.Transaction`. Reads inside a statement are
//! served by that transaction: first from the transaction's `MemBuffer`
//! (the statement's own uncommitted writes), and only on a miss from the
//! snapshot at `start_ts`. Writes never reach TiKV until COMMIT, when the
//! whole buffer is published as one 2PC mutation set.
//!
//! [`ClusterTableStorage`] is exactly those two halves:
//!
//! * [`MutationBuffer`] -- a shared SQL handle to the client transaction's
//!   MemDB. Its ordered iterator feeds the table overlay and the client's
//!   committer consumes that same buffer. A staged delete is a tombstone
//!   (`None`), not an erased entry, because a delete must *hide* a value the
//!   snapshot still has.
//! * [`ClusterSnapshot`] -- the read side at one timestamp. One implementation
//!   forwards to a real transaction's `snapshot_get`/`snapshot_scan`; the unit
//!   tests use a mock. Nothing here allocates a timestamp: the snapshot's
//!   owner does, which is what keeps "one statement, one `start_ts`" a
//!   property of the caller rather than an accident of the storage.
//!
//! Because both halves are `Arc` handles, [`TableStorage::clone_box`] clones
//! *handles*: two `KvTable` copies of the same session see each other's staged
//! writes, as two `table.Table` handles of one Go transaction do. That is the
//! divergence [`crate::storage`] reserved for the real backend.
//!
//! # What is deliberately refused rather than approximated
//!
//! * An unbounded [`iter`](TableStorage::iter) range. In-process, `None` means
//!   "to the end of the map"; against a cluster it would mean scanning every
//!   region of the keyspace. Both of `KvTable`'s scan sites pass a bounded
//!   range, so refusing costs nothing and keeps a mistake loud.
//! * [`clear`](TableStorage::clear) (TRUNCATE). TiKV performs it as a new
//!   table id plus an unsafe-destroy-range, not as "empty the container".
//!   Emptying the *buffer* would silently leave every committed row in place,
//!   so `clear` poisons the handle instead: every later operation reports a
//!   backend error naming the reason. It poisons THAT table's handle only --
//!   the buffer and the snapshot stay shared, so the session's other tables
//!   keep working, as they do in Go.
//! * [`key_count`](TableStorage::key_count) reports the staged key count only.
//!   TiKV has no exact count, and the seam's own doc already says so.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};

use tidb_txnkv::Key;

use crate::remote_scan::{
    PushdownScan, PushdownScanRequest, PushdownScanner, PushdownScannerError,
};
use crate::storage::{StorageError, StorageIterator, TableStorage};

/// Key/value pairs one snapshot scan returned, in key order. The same shape
/// `tidb-txnkv` names `SnapshotScanPairs`, spelled here so the seam does not
/// depend on the transport crate's alias.
pub type SnapshotPairs = Vec<(Vec<u8>, Vec<u8>)>;

/// The read half of a cluster transaction: one consistent timestamp.
///
/// Both methods speak raw TiKV-format keys, like [`TableStorage`] itself. An
/// implementation maps region errors, stale epochs and unresolvable locks onto
/// [`StorageError::Retryable`]; anything else is [`StorageError::Backend`].
pub trait ClusterSnapshot: fmt::Debug + Send {
    /// Go `SnapshotRuntimeStats.GetCmdRPCCount` for point and batch-point
    /// commands issued by this snapshot so far.
    fn point_rpc_counts(&mut self) -> (u64, u64) {
        (0, 0)
    }

    /// Starts any asynchronous work needed by an ordinary statement snapshot.
    /// The first read still owns error delivery and timestamp publication.
    fn prepare(&mut self) -> Result<(), StorageError> {
        Ok(())
    }

    /// Reads one key at the snapshot's timestamp. `None` is TiKV's
    /// `not_found`, which the caller turns into [`StorageError::NotFound`].
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError>;

    /// Reads several keys at one snapshot timestamp. The default preserves
    /// correctness for lightweight test snapshots; the production transaction
    /// snapshot overrides it with one region-grouped BatchCommands request.
    fn batch_get(&mut self, keys: &[Key]) -> Result<SnapshotPairs, StorageError> {
        let mut pairs = Vec::new();
        for key in keys {
            if let Some(value) = self.get(key)? {
                pairs.push((key.as_bytes().to_vec(), value));
            }
        }
        Ok(pairs)
    }

    /// Reads the pairs of `[start, end)` at the snapshot's timestamp, in key
    /// order, at most `limit` of them.
    ///
    /// `limit` is the whole basis of an incremental scan: a cursor asks for
    /// one batch, consumes it, and asks again from the key after the last one
    /// it got, so a range whose consumer stops early is never read past the
    /// batch it stopped in. An implementation MUST honour it -- returning
    /// fewer pairs than `limit` means the range is drained, and returning more
    /// means the caller reads rows it asked not to be sent. `None` asks for
    /// the whole range.
    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError>;

    /// The timestamp every read of this snapshot is served at.
    ///
    /// A remote scan has to name it: a coprocessor request that read at any
    /// other timestamp would not be the statement's snapshot, which is the
    /// whole basis of repeatable read and of the staged-buffer merge. `0` is
    /// the answer of a backend that has no MVCC timestamp, and refuses a
    /// remote scan for exactly that reason.
    fn start_ts(&self) -> u64 {
        0
    }

    /// Declares, before the statement's first read, that this statement's
    /// WHOLE read is one autocommit point get on the clustered handle, and
    /// reports whether the declaration was taken.
    ///
    /// This is Go's `AdviseOptimizeWithPlan`
    /// (`pkg/sessiontxn/isolation/optimistic.go`): the plan is shown to the
    /// transaction provider once per statement, and a provider that accepts it
    /// reads at `math.MaxUint64` instead of spending a timestamp.
    ///
    /// The declaration is a SHAPE, not a request. "A `get` arrived" is not
    /// this fact and must never be read as it: an `UPDATE`'s read-before-write
    /// issues the same `get`, and so does every row lookup of an index double
    /// read. Both of those would read a different latest-committed version per
    /// read -- no error, wrong rows. Only a caller that knows the statement's
    /// root plan may declare.
    ///
    /// The default REFUSES, so an implementation that has not thought about
    /// the question keeps paying for its timestamp. In particular the snapshot
    /// an explicit `BEGIN` hands its statements refuses by inheriting this
    /// default, which is [`IsAutoCommitTxn`'s `!InTxn`
    /// half](https://github.com/pingcap/tidb/blob/master/pkg/planner/core/common_plans.go)
    /// made structural: inside a transaction there is nothing to declare to.
    fn declare_autocommit_point_get(&mut self) -> bool {
        false
    }
}

/// A whole [`MutationBuffer`] as of one moment: what
/// [`MutationBuffer::checkpoint`] produces and [`MutationBuffer::restore`]
/// puts back. Go's counterpart is a `tikv.MemDBCheckpoint` -- a position in
/// the membuffer rather than a copy of it -- which a statement rollback or a
/// savepoint returns to.
///
/// MemDB owns rollback. The SQL change position records only statement deltas
/// and duplicate-key hints; it does not replay values or lock state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BufferCheckpoint {
    change_len: usize,
    native: usize,
}

/// SQL statement-delta metadata, separate from MemDB rollback.
#[derive(Clone, Debug)]
enum StatementChange {
    /// A presumption mark newly added for this key.
    Presume { key: Key },
}

/// Raw snapshot keys actually consumed by the current statement.
///
/// A pessimistic locking read must lock rows after the executor has applied
/// its predicates and limits. Tracking at the storage seam records precisely
/// those point gets and iterator rows, while leaving ordinary statements on
/// the zero-work disabled path.
#[derive(Clone, Debug, Default)]
pub struct StatementReadKeys {
    state: Arc<Mutex<StatementReadKeyState>>,
}

#[derive(Debug, Default)]
struct StatementReadKeyState {
    enabled: bool,
    keys: BTreeSet<Vec<u8>>,
}

impl StatementReadKeys {
    /// Starts a new statement and discards any prior statement's keys.
    pub fn begin(&self) {
        let mut state = self.lock();
        state.keys.clear();
        state.enabled = true;
    }

    /// Ends the statement and returns its keys in encoded-key order.
    #[must_use]
    pub fn finish(&self) -> Vec<Vec<u8>> {
        let mut state = self.lock();
        state.enabled = false;
        std::mem::take(&mut state.keys).into_iter().collect()
    }

    /// Ends a failed statement without returning its partial read set.
    pub fn cancel(&self) {
        let mut state = self.lock();
        state.enabled = false;
        state.keys.clear();
    }

    fn record(&self, key: &Key) {
        let mut state = self.lock();
        if state.enabled {
            state.keys.insert(key.as_bytes().to_vec());
        }
    }

    fn is_enabled(&self) -> bool {
        self.lock().enabled
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, StatementReadKeyState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

/// Borrows the store transaction's authoritative MemDB under its own lock.
/// Returns false after the transaction has ended.
pub type NativeMemBufferAccess = Arc<
    dyn Fn(&mut dyn FnMut(&mut tikv_client::transaction::unionstore::MemDb)) -> bool + Send + Sync,
>;

struct NativeMemBuffer {
    local: tikv_client::transaction::unionstore::MemDb,
    bound: Option<(u64, NativeMemBufferAccess)>,
}
impl Default for NativeMemBuffer {
    fn default() -> Self {
        let mut local = tikv_client::transaction::unionstore::MemDb::new();
        local.set_entry_size_limit(
            tidb_txnkv::txn_entry_size_limit(),
            tidb_txnkv::txn_total_size_limit(),
        );
        Self { local, bound: None }
    }
}

fn buffer_error(error: Box<dyn std::error::Error + Send + Sync>) -> StorageError {
    match tidb_txnkv::TikvMemBufferError::from_native_write(error) {
        tidb_txnkv::TikvMemBufferError::Kv(error) => StorageError::Sql(crate::MysqlError::new(
            error.mysql_code().as_u16(),
            error.to_string(),
        )),
        error => StorageError::Backend(error.to_string()),
    }
}

impl NativeMemBuffer {
    fn read<T>(&self, f: impl FnOnce(&tikv_client::transaction::unionstore::MemDb) -> T) -> T {
        let mut f = Some(f);
        if let Some((_, access)) = &self.bound {
            let mut answer = None;
            if access(&mut |buffer| {
                answer = Some(f.take().unwrap()(buffer));
            }) {
                return answer.unwrap();
            }
        }
        f.take().unwrap()(&self.local)
    }
    fn write<T>(
        &mut self,
        f: impl FnOnce(&mut tikv_client::transaction::unionstore::MemDb) -> T,
    ) -> T {
        let mut f = Some(f);
        if let Some((_, access)) = &self.bound {
            let mut answer = None;
            if access(&mut |buffer| {
                answer = Some(f.take().unwrap()(buffer));
            }) {
                return answer.unwrap();
            }
        }
        f.take().unwrap()(&mut self.local)
    }
    fn get_readonly(&self, key: &[u8]) -> Result<Vec<u8>, tikv_client::error::StaticError> {
        self.read(|b| b.get_readonly(key))
    }
    fn iter(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Box<dyn tikv_client::transaction::unionstore::KvIterator> {
        self.read(|b| b.iter(start, end))
    }
    fn iter_with_flags(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Box<dyn tikv_client::transaction::unionstore::KvIterator> {
        self.read(|b| b.iter_with_flags(start, end))
    }
    fn stages(&self) -> Vec<usize> {
        self.read(|b| b.stages())
    }
    fn len(&self) -> usize {
        self.read(|b| b.len())
    }
    fn size(&self) -> usize {
        self.read(|b| b.size())
    }
    fn is_empty(&self) -> bool {
        self.read(|b| b.is_empty())
    }
    fn memory_footprint(&self) -> u64 {
        self.read(|b| b.memory_footprint())
    }
    fn update_flags(&mut self, key: &[u8], flags: &[tikv_client::kv::FlagsOp]) {
        self.write(|b| b.update_flags(key, flags))
    }
    fn staging(&mut self) -> usize {
        self.write(|b| b.staging())
    }
    fn cleanup(&mut self, handle: usize) {
        self.write(|b| b.cleanup(handle))
    }
    fn release(&mut self, handle: usize) {
        self.write(|b| b.release(handle))
    }
    fn reset(&mut self) {
        self.write(|b| b.reset())
    }
}

/// SQL access to the client's MemDB. SQL retains only duplicate-key text and
/// statement deltas; values, flags, checkpoints and rollback belong to MemDB.
#[derive(Clone, Default)]
pub struct MutationBuffer {
    state: Arc<Mutex<BufferState>>,
}

#[derive(Default)]
struct BufferState {
    memdb: NativeMemBuffer,
    hints: BTreeMap<Key, DuplicateKeyHint>,
    changes: Vec<StatementChange>,
}
impl std::fmt::Debug for MutationBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MutationBuffer")
            .field("len", &self.len())
            .finish()
    }
}
impl BufferState {
    fn get(&self, key: &Key) -> Option<Option<Vec<u8>>> {
        self.memdb
            .get_readonly(key.as_bytes())
            .ok()
            .map(|value| (!value.is_empty()).then_some(value))
    }
    fn stage(&mut self, key: Key, value: Option<Vec<u8>>) -> Result<(), StorageError> {
        self.stage_batch(std::iter::once((
            key,
            value,
            false,
            tidb_txnkv::AssertionOp::AssertNone,
        )))
    }
    fn stage_batch(
        &mut self,
        writes: impl IntoIterator<Item = (Key, Option<Vec<u8>>, bool, tidb_txnkv::AssertionOp)>,
    ) -> Result<(), StorageError> {
        let Self { memdb, changes, .. } = self;
        // One native transaction borrow covers the entire statement batch.
        memdb.write(|buffer| {
            for (key, value, presume_not_exists, assertion) in writes {
                let prior = buffer
                    .get_readonly(key.as_bytes())
                    .ok()
                    .map(|value| (!value.is_empty()).then_some(value));
                match value {
                    Some(value) => buffer.set(key.as_bytes(), &value),
                    None => buffer.delete(key.as_bytes()),
                }
                .map_err(buffer_error)?;
                let was_absent = prior.is_none();
                let native_flags = buffer
                    .get_flags_readonly(key.as_bytes())
                    .unwrap_or_default();
                if !native_flags.has_assertion_flags() {
                    use tidb_txnkv::AssertionOp;
                    use tikv_client::kv::FlagsOp;
                    let flag = match assertion {
                        AssertionOp::AssertExist => Some(FlagsOp::SetAssertExist),
                        AssertionOp::AssertNotExist => Some(FlagsOp::SetAssertNotExist),
                        AssertionOp::AssertUnknown => Some(FlagsOp::SetAssertUnknown),
                        AssertionOp::AssertNone => None,
                    };
                    if let Some(flag) = flag {
                        buffer.update_flags(key.as_bytes(), &[flag]);
                    }
                }
                if presume_not_exists && was_absent {
                    Self::mark_presume_in(buffer, changes, &key);
                }
            }
            Ok(())
        })
    }
    fn mark_presume_in(
        buffer: &mut tikv_client::transaction::unionstore::MemDb,
        changes: &mut Vec<StatementChange>,
        key: &Key,
    ) {
        use tikv_client::kv::FlagsOp;
        if !buffer
            .get_flags_readonly(key.as_bytes())
            .is_ok_and(|flags| flags.has_presume_key_not_exists())
        {
            buffer.update_flags(key.as_bytes(), &[FlagsOp::SetPresumeKeyNotExists]);
            changes.push(StatementChange::Presume { key: key.clone() });
        }
    }
    fn mark_presume(&mut self, key: &Key) {
        let Self { memdb, changes, .. } = self;
        memdb.write(|buffer| Self::mark_presume_in(buffer, changes, key));
    }
    fn entries(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(Key, Option<Vec<u8>>, bool)> {
        let mut iter = self.memdb.iter_with_flags(start, end);
        let mut result = Vec::new();
        while iter.valid() {
            if iter.has_value() {
                let key = Key::from_bytes(iter.key().to_vec());
                let presumed = iter.flags().has_presume_key_not_exists();
                let value = iter.value().to_vec();
                result.push((key, (!value.is_empty()).then_some(value), presumed));
            }
            iter.next().expect("MemDB iteration is local");
        }
        result
    }
}

/// Go session.KeyNeedToLock, shared by executor writes and restricted SQL.
pub fn key_needs_pessimistic_lock(
    key: &[u8],
    value: &[u8],
    flags: tikv_client::kv::KeyFlags,
) -> bool {
    use tidb_tablecodec::{
        index_kv_is_unique, is_index_key, is_record_key, is_temp_index_key, is_untouched_index_kv,
    };
    if !key.starts_with(b"t") {
        return true;
    }
    if flags.has_need_constraint_check_in_prewrite() {
        return false;
    }
    if flags.has_presume_key_not_exists() {
        return true;
    }
    if value.is_empty() {
        return flags.has_need_locked() || is_record_key(key);
    }
    if is_untouched_index_kv(key, value) {
        return false;
    }
    if !is_index_key(key) {
        return true;
    }
    if is_temp_index_key(key) {
        if tidb_config::kerneltype::is_next_gen() {
            return true;
        }
        return match tidb_tablecodec::decode_temp_index_value(value) {
            Ok(history) => history.last().is_some_and(|current| {
                current.handle.is_some() || index_kv_is_unique(&current.value)
            }),
            Err(error) => {
                tracing::warn!(?error, "decode temp index value failed");
                false
            }
        };
    }
    index_kv_is_unique(value) || flags.has_need_locked()
}

/// SQL identity retained by the table layer for a deferred duplicate error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DuplicateKeyHint {
    /// SQL rendering of the duplicated value.
    pub value: String,
    /// SQL index or constraint name.
    pub key: String,
}
impl MutationBuffer {
    /// Attach SQL staging to the native transaction. Pending statement writes
    /// must be attached before acquiring locks; an empty handle can also attach
    /// to a transaction that already owns locks without replacing its MemDB.
    pub fn bind_native(&self, owner: u64, access: NativeMemBufferAccess) {
        let mut state = self.state();
        if state
            .memdb
            .bound
            .as_ref()
            .is_some_and(|(id, _)| *id == owner)
        {
            return;
        }
        let mut local = Some(std::mem::replace(
            &mut state.memdb.local,
            NativeMemBuffer::default().local,
        ));
        assert!(
            access(&mut |buffer| {
                if buffer.is_empty() {
                    *buffer = local.take().unwrap();
                } else {
                    let local = local.as_ref().unwrap();
                    assert!(
                        local.is_empty() && local.stages().is_empty(),
                        "bind pending SQL writes before acquiring locks"
                    );
                }
            }),
            "cannot bind a finished transaction"
        );
        state.memdb.bound = Some((owner, access));
    }
    /// Transfer an unbound statement buffer to its newly opened transaction.
    pub fn take_native_buffer(&self) -> tikv_client::transaction::unionstore::MemDb {
        let mut state = self.state();
        assert!(
            state.memdb.bound.is_none(),
            "the store transaction already owns this buffer"
        );
        state.changes.clear();
        std::mem::replace(&mut state.memdb.local, NativeMemBuffer::default().local)
    }
    /// The start timestamp of the bound native transaction, if any.
    pub fn native_owner(&self) -> Option<u64> {
        self.state().memdb.bound.as_ref().map(|(id, _)| *id)
    }
    #[must_use]
    /// Creates an unbound SQL handle with a native MemDB.
    pub fn new() -> Self {
        Self::default()
    }
    /// Stages a nonempty SQL value.
    pub fn set(&self, key: Key, value: Vec<u8>) -> Result<(), StorageError> {
        self.state().stage(key, Some(value))
    }

    /// Stages a batch of owned entries while holding the buffer lock once.
    /// Statement execution owns this batch already; keeping one critical
    /// section matches Go's single-threaded MemBuffer staging path.
    pub fn stage_owned_batch<I>(&self, writes: I) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = (Key, Option<Vec<u8>>, bool, tidb_txnkv::AssertionOp)>,
    {
        self.state().stage_batch(writes)
    }

    /// Stages a delete as a tombstone, so the read path stops seeing the
    /// snapshot's value for the key.
    pub fn delete(&self, key: Key) -> Result<(), StorageError> {
        self.state().stage(key, None)
    }
    /// Returns the staged value or tombstone; a missing entry requires a snapshot read.
    pub fn get(&self, key: &Key) -> Option<Option<Vec<u8>>> {
        self.state().get(key)
    }
    /// Marks a lazy absence check in the authoritative buffer.
    pub fn mark_presume_key_not_exists(&self, key: &Key) {
        self.state().mark_presume(key);
    }
    /// Retains SQL duplicate-key text alongside a native absence check.
    pub fn mark_presume_key_not_exists_with_hint(
        &self,
        key: &Key,
        value: impl Into<String>,
        index: impl Into<String>,
    ) {
        let mut state = self.state();
        state.mark_presume(key);
        state.hints.insert(
            key.clone(),
            DuplicateKeyHint {
                value: value.into(),
                key: index.into(),
            },
        );
    }
    /// Looks up SQL error text retained for a deferred duplicate.
    pub fn duplicate_key_hint_for(&self, key: &[u8]) -> Option<DuplicateKeyHint> {
        self.state()
            .hints
            .get(&Key::from_bytes(key.to_vec()))
            .cloned()
    }
    /// Lists current native absence checks.
    pub fn presume_not_exists_keys(&self) -> BTreeSet<Vec<u8>> {
        let state = self.state();
        let mut iter = state.memdb.iter_with_flags(None, None);
        let mut keys = BTreeSet::new();
        while iter.valid() {
            if iter.flags().has_presume_key_not_exists() {
                keys.insert(iter.key().to_vec());
            }
            iter.next().expect("MemDB iteration is local");
        }
        keys
    }
    /// Lists absence checks introduced after the SQL checkpoint.
    pub fn presume_not_exists_since(&self, checkpoint: BufferCheckpoint) -> BTreeSet<Vec<u8>> {
        self.state()
            .changes
            .iter()
            .skip(checkpoint.change_len)
            .map(|StatementChange::Presume { key }| key.as_bytes().to_vec())
            .collect()
    }
    /// Consumes current absence checks while preserving staged values.
    pub fn take_presume_not_exists(&self) -> BTreeSet<Key> {
        let mut state = self.state();
        let keys: BTreeSet<Key> = {
            let mut iter = state.memdb.iter_with_flags(None, None);
            let mut keys = BTreeSet::new();
            while iter.valid() {
                if iter.flags().has_presume_key_not_exists() {
                    keys.insert(Key::from_bytes(iter.key().to_vec()));
                }
                iter.next().expect("MemDB iteration is local");
            }
            keys
        };
        for key in &keys {
            state.memdb.update_flags(
                key.as_bytes(),
                &[tikv_client::kv::FlagsOp::DelPresumeKeyNotExists],
            );
        }
        keys
    }
    /// Copies staged values and tombstones in the encoded-key range.
    pub fn range(&self, start: &Key, end: &Key) -> Vec<(Key, Option<Vec<u8>>)> {
        self.state()
            .entries(Some(start.as_bytes()), Some(end.as_bytes()))
            .into_iter()
            .map(|(k, v, _)| (k, v))
            .collect()
    }
    /// Whether the native buffer has values in the encoded-key range.
    pub fn has_keys_in_range(&self, start: &Key, end: &Key) -> bool {
        self.state()
            .memdb
            .iter(Some(start.as_bytes()), Some(end.as_bytes()))
            .valid()
    }
    /// Starts a native staging scope and records the SQL delta position.
    pub fn checkpoint(&self) -> BufferCheckpoint {
        let mut state = self.state();
        BufferCheckpoint {
            change_len: state.changes.len(),
            native: state.memdb.staging(),
        }
    }
    /// Ends a statement scope after its delta has been consumed. Outer SQL
    /// savepoints remain active; completed transaction scopes are already gone.
    pub fn release(&self, checkpoint: BufferCheckpoint) {
        let mut state = self.state();
        while checkpoint.native != 0 && state.memdb.stages().len() >= checkpoint.native {
            let handle = state.memdb.stages().len();
            state.memdb.release(handle);
        }
        if state.memdb.stages().is_empty() {
            state.changes.clear();
        }
    }
    /// Copies the staged value view without consuming native flags.
    pub fn snapshot(&self) -> Vec<(Key, Option<Vec<u8>>)> {
        self.state()
            .entries(None, None)
            .into_iter()
            .map(|(k, v, _)| (k, v))
            .collect()
    }
    /// Lists staged keys without copying their values. This is used by the
    /// native transaction commit path for schema-lease table identification.
    pub fn staged_keys(&self) -> Vec<Key> {
        let state = self.state();
        let mut iter = state.memdb.iter_with_flags(None, None);
        let mut keys = Vec::with_capacity(state.memdb.len());
        while iter.valid() {
            if iter.has_value() {
                keys.push(Key::from_bytes(iter.key().to_vec()));
            }
            iter.next().expect("MemDB iteration is local");
        }
        keys
    }

    /// Returns Go's transaction size and key count from native MemDB metadata.
    /// Reading metrics must not clone or scan staged values.
    #[must_use]
    pub fn write_details(&self) -> (usize, usize) {
        self.state()
            .memdb
            .read(|buffer| (buffer.size(), buffer.len()))
    }

    /// Go LazyTxn.KeysNeedToLock reads the native statement stage, including flags.
    pub fn pessimistic_keys_since(&self, checkpoint: BufferCheckpoint) -> Vec<Vec<u8>> {
        if checkpoint.native == 0 {
            return Vec::new();
        }
        self.state().memdb.read(|buffer| {
            let mut keys = Vec::new();
            buffer.inspect_stage(checkpoint.native, |key, flags, value| {
                if key_needs_pessimistic_lock(key, value, flags) {
                    keys.push(key.to_vec());
                }
            });
            keys
        })
    }
    /// Number of native entries, including flags-only keys.
    pub fn len(&self) -> usize {
        self.state().memdb.len()
    }
    /// Whether the authoritative native buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.state().memdb.is_empty()
    }
    /// Current native key/value byte count.
    pub fn staged_bytes(&self) -> usize {
        self.state().memdb.size()
    }
    /// Native MemDB memory retained by values, flags and staging history.
    pub fn memory_footprint(&self) -> u64 {
        self.state().memdb.memory_footprint()
    }
    /// Clears the completed transaction buffer and SQL metadata.
    pub fn reset(&self) {
        let mut state = self.state();
        state.memdb.reset();
        state.memdb.bound = None;
        state.changes.clear();
        state.hints.clear();
    }
    /// Rolls back through native staging while retaining the checkpoint for another retry.
    pub fn restore(&self, checkpoint: BufferCheckpoint) {
        let mut state = self.state();
        if checkpoint.native == 0 {
            state.memdb.reset();
        } else {
            while state.memdb.stages().len() >= checkpoint.native {
                let handle = state.memdb.stages().len();
                state.memdb.cleanup(handle);
            }
            state.memdb.staging();
        }
        let changed_marks: Vec<_> = state
            .changes
            .iter()
            .skip(checkpoint.change_len)
            .map(|StatementChange::Presume { key }| key.clone())
            .collect();
        for key in changed_marks {
            state.memdb.update_flags(
                key.as_bytes(),
                &[tikv_client::kv::FlagsOp::DelPresumeKeyNotExists],
            );
            state.hints.remove(&key);
        }
        state.changes.truncate(checkpoint.change_len);
    }
    fn state(&self) -> std::sync::MutexGuard<'_, BufferState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

/// The snapshot half of a *session's* storage: one slot the session rebinds at
/// every statement boundary.
///
/// A [`ClusterTableStorage`] fixes its snapshot handle at construction, and a
/// catalog of `KvTable`s is built once per connection -- but "one statement,
/// one `start_ts`" requires a different transaction for every statement. Both
/// hold at once when the handle every table shares is this slot: the session
/// binds a fresh snapshot before a statement and takes it back afterwards, and
/// no table is rebuilt.
///
/// An unbound slot is not an empty table: every read reports a backend error
/// naming the missing snapshot, so a statement that somehow escapes the
/// session's bind/unbind pairing fails loudly instead of reading nothing.
#[derive(Debug, Default)]
pub struct SwappableSnapshot {
    bound: Option<Box<dyn ClusterSnapshot>>,
}

impl SwappableSnapshot {
    /// An unbound slot, as a session opens with.
    #[must_use]
    pub fn new() -> Self {
        SwappableSnapshot::default()
    }

    /// Binds this statement's snapshot, returning whatever the slot held.
    ///
    /// A returned `Some` means the previous statement never unbound; the
    /// caller owns finishing it.
    pub fn bind(&mut self, snapshot: Box<dyn ClusterSnapshot>) -> Option<Box<dyn ClusterSnapshot>> {
        self.bound.replace(snapshot)
    }

    /// Takes the bound snapshot back, leaving the slot unbound.
    pub fn unbind(&mut self) -> Option<Box<dyn ClusterSnapshot>> {
        self.bound.take()
    }

    /// Whether a statement's snapshot is currently bound.
    #[must_use]
    pub const fn is_bound(&self) -> bool {
        self.bound.is_some()
    }

    fn snapshot(&mut self) -> Result<&mut Box<dyn ClusterSnapshot>, StorageError> {
        self.bound.as_mut().ok_or_else(|| {
            StorageError::Backend(
                "no statement snapshot is bound to this session's cluster storage".to_owned(),
            )
        })
    }
}

impl ClusterSnapshot for SwappableSnapshot {
    fn point_rpc_counts(&mut self) -> (u64, u64) {
        self.snapshot()
            .map_or((0, 0), |snapshot| snapshot.point_rpc_counts())
    }

    fn prepare(&mut self) -> Result<(), StorageError> {
        self.snapshot()?.prepare()
    }

    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.snapshot()?.get(key)
    }

    fn batch_get(&mut self, keys: &[Key]) -> Result<SnapshotPairs, StorageError> {
        self.snapshot()?.batch_get(keys)
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        self.snapshot()?.scan(start, end, limit)
    }

    fn start_ts(&self) -> u64 {
        self.bound
            .as_ref()
            .map_or(0, |snapshot| snapshot.start_ts())
    }

    /// An unbound slot has no statement to declare for, so it refuses -- the
    /// same fail-closed answer its reads give.
    fn declare_autocommit_point_get(&mut self) -> bool {
        self.bound
            .as_mut()
            .is_some_and(|snapshot| snapshot.declare_autocommit_point_get())
    }
}

/// A table's view of cluster storage: staged writes in front of a snapshot.
///
/// Cloning shares both halves, so every table of one session stages into the
/// same buffer and reads at the same timestamp.
#[derive(Clone, Debug)]
pub struct ClusterTableStorage {
    buffer: MutationBuffer,
    snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
    read_keys: StatementReadKeys,
    /// Whether THIS table handle was truncated (see [`Self::check_usable`]).
    ///
    /// Deliberately a plain `bool` and not shared: the buffer and the snapshot
    /// belong to the SESSION and every table of it stages into them, but a
    /// TRUNCATE names ONE table. Sharing the flag made one `TRUNCATE TABLE t`
    /// refuse every subsequent statement on every other table of the
    /// connection, which Go never does -- it swaps the truncated table for a
    /// fresh one with a new id and the session carries on.
    truncated: bool,
    /// The coprocessor capability, when the node was given one. `None` keeps
    /// every scan on the byte-level merge below.
    scanner: Option<Arc<dyn PushdownScanner>>,
}

impl ClusterTableStorage {
    /// Binds one session buffer to one statement snapshot.
    #[must_use]
    pub fn new(buffer: MutationBuffer, snapshot: Arc<Mutex<dyn ClusterSnapshot>>) -> Self {
        ClusterTableStorage {
            buffer,
            snapshot,
            read_keys: StatementReadKeys::default(),
            truncated: false,
            scanner: None,
        }
    }

    /// Gives this session's tables a coprocessor to serve base-table scans
    /// with, so a predicate is evaluated at the region instead of after the
    /// range's bytes have crossed the network.
    ///
    /// The staged buffer is untouched by it: see
    /// [`TableStorage::open_remote_scan`] below for how the two are merged.
    #[must_use]
    pub fn with_remote_scanner(mut self, scanner: Arc<dyn PushdownScanner>) -> Self {
        self.scanner = Some(scanner);
        self
    }

    /// The session buffer these tables stage into, for the COMMIT path.
    #[must_use]
    pub fn buffer(&self) -> MutationBuffer {
        self.buffer.clone()
    }

    /// The per-statement raw-key collector shared by every table clone.
    #[must_use]
    pub fn read_keys(&self) -> StatementReadKeys {
        self.read_keys.clone()
    }

    fn check_usable(&self) -> Result<(), StorageError> {
        if self.truncated {
            return Err(StorageError::Backend(
                "TRUNCATE is not a cluster storage operation; this table handle is no longer usable"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    fn snapshot_get(&self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.snapshot
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(key)
    }
}

impl TableStorage for ClusterTableStorage {
    fn has_external_statement_rollback(&self) -> bool {
        true
    }

    fn point_rpc_counts(&mut self) -> (u64, u64) {
        self.snapshot
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .point_rpc_counts()
    }

    fn get(&mut self, key: &Key) -> Result<Vec<u8>, StorageError> {
        self.check_usable()?;
        // Counted at the SEAM, as the in-process backend counts it, so the
        // two backends report the same request shape for the same plan and a
        // test can pin an access path against either. A key the session's own
        // buffer answers is still one `get` here, because the shape the count
        // describes is the plan's, not the transport's.
        crate::storage::note_storage_op(|ops| ops.gets += 1);
        match self.buffer.get(key) {
            Some(Some(value)) => Ok(value),
            Some(None) => Err(StorageError::NotFound),
            None => match self.snapshot_get(key)? {
                Some(value) => {
                    self.read_keys.record(key);
                    Ok(value)
                }
                None => Err(StorageError::NotFound),
            },
        }
    }

    fn get_local(&mut self, key: &Key) -> Result<Vec<u8>, StorageError> {
        self.check_usable()?;
        // Strictly the staged writes -- never the snapshot. An empty answer
        // is a tombstone, the same shape Go's `GetLocal` hands back.
        match self.buffer.get(key) {
            Some(Some(value)) => Ok(value),
            Some(None) => Ok(Vec::new()),
            None => Err(StorageError::NotFound),
        }
    }

    fn mark_presume_key_not_exists(&mut self, key: &Key) {
        self.buffer.mark_presume_key_not_exists(key);
    }

    fn mark_presume_key_not_exists_with_hint(&mut self, key: &Key, value: &str, index: &str) {
        self.buffer
            .mark_presume_key_not_exists_with_hint(key, value, index);
    }

    fn batch_get(&mut self, keys: &[Key]) -> Result<HashMap<Key, Vec<u8>>, StorageError> {
        self.check_usable()?;
        if keys.is_empty() {
            return Ok(HashMap::new());
        }
        crate::storage::note_storage_op(|ops| ops.gets += 1);
        let mut values = HashMap::with_capacity(keys.len());
        let mut missing = Vec::with_capacity(keys.len());
        for key in keys {
            match self.buffer.get(key) {
                Some(Some(value)) => {
                    values.insert(key.clone(), value);
                }
                Some(None) => {}
                None => missing.push(key.clone()),
            }
        }
        if !missing.is_empty() {
            let snapshot_values = self
                .snapshot
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .batch_get(&missing)?;
            for (key, value) in snapshot_values {
                let key = Key::from_bytes(key);
                self.read_keys.record(&key);
                values.insert(key, value);
            }
        }
        Ok(values)
    }

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), StorageError> {
        self.check_usable()?;
        self.buffer.set(key, value)
    }

    fn delete(&mut self, key: Key) -> Result<(), StorageError> {
        self.check_usable()?;
        self.buffer.delete(key)
    }

    fn iter(
        &mut self,
        start: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Box<dyn StorageIterator>, StorageError> {
        self.check_usable()?;
        let (Some(start), Some(end)) = (start, upper_bound) else {
            return Err(StorageError::Backend(
                "cluster storage requires a bounded scan range".to_owned(),
            ));
        };
        crate::storage::note_storage_op(|ops| ops.scans += 1);
        let staged = self.buffer.range(start, end);
        Ok(Box::new(MergedIterator::open(
            Arc::clone(&self.snapshot),
            self.read_keys.clone(),
            start.clone(),
            end.clone(),
            staged,
        )?))
    }

    fn first(
        &mut self,
        start: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Option<(Key, Vec<u8>)>, StorageError> {
        self.check_usable()?;
        let (Some(start), Some(end)) = (start, upper_bound) else {
            return Err(StorageError::Backend(
                "cluster storage requires a bounded scan range".to_owned(),
            ));
        };
        let staged = self.buffer.range(start, end);
        // A staged row can displace or shadow the snapshot prefix. Keep the
        // ordinary merge in that case; the native one-row request is only a
        // safe replacement for a clean table.
        if !staged.is_empty() {
            let mut iterator = self.iter(Some(start), Some(end))?;
            let first = if iterator.valid() {
                Some((iterator.key().clone(), iterator.value().to_vec()))
            } else {
                None
            };
            iterator.close();
            return Ok(first);
        }
        crate::storage::note_storage_op(|ops| ops.scans += 1);
        self.snapshot
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .scan(start, end, Some(1))
            .map(|pairs| {
                pairs
                    .into_iter()
                    .next()
                    .map(|(key, value)| (Key::from_bytes(key), value))
            })
    }

    /// Opens the snapshot stream and returns the range-bounded staged overlay.
    /// Readers that can suppress shadowed keys admit unordered responses;
    /// order-sensitive readers retain the key-ordered merge contract.
    fn open_remote_scan(
        &mut self,
        request: &PushdownScanRequest,
    ) -> Option<Result<PushdownScan, StorageError>> {
        let scanner = self.scanner.as_ref()?;
        if self.read_keys.is_enabled() {
            return None;
        }
        if let Err(error) = self.check_usable() {
            return Some(Err(error));
        }
        let snapshot_ts = self
            .snapshot
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .start_ts();
        // One staged slice per requested range, concatenated. The ranges are
        // ascending; equal point ranges are allowed by Go's table-handle
        // lookup and deliberately repeat the same staged row. Thus the
        // concatenation remains nondecreasing, which is the order the merge
        // below relies on. A staged row outside every range is dropped for the
        // same reason a snapshot row there is:
        // the ranges bound the `WHERE`'s access conditions, so no row outside
        // them can satisfy the statement.
        let mut staged: Vec<_> = request
            .ranges
            .iter()
            .flat_map(|(start, end)| self.buffer.range(start, end))
            .collect();
        // A `desc` request's remote stream arrives in DESCENDING key order;
        // the staged slice reverses to match, so the caller's one-pass merge
        // walks both sides the same way.
        if request.desc {
            staged.reverse();
        }
        let mut request = request.clone();
        request.snapshot_ts = snapshot_ts;
        request.keep_order |= (!staged.is_empty() && !request.allow_unordered_response)
            || request.aggregate.as_ref().is_some_and(|aggregate| {
                matches!(
                    aggregate,
                    crate::remote_scan::PushdownPartialAggregate::Grouped { streamed: true, .. }
                )
            });
        if !staged.is_empty() {
            request.limit = None;
        }
        match scanner.open(&request) {
            Ok(stream) => Some(Ok(PushdownScan { stream, staged })),
            // A refusal is not a failure: the caller falls back to `iter`,
            // which answers the same question from the same snapshot.
            Err(PushdownScannerError::Unsupported(_reason)) => None,
            Err(PushdownScannerError::Backend(error)) => Some(Err(error)),
        }
    }

    fn key_count(&self) -> usize {
        self.buffer.len()
    }

    fn clear(&mut self) {
        self.truncated = true;
    }

    fn clone_box(&self) -> Box<dyn TableStorage> {
        Box::new(self.clone())
    }
}

/// How many snapshot pairs one refill asks the cluster for.
///
/// It is the transport's own page size (`SCAN_PAGE_LIMIT` in the coordinator),
/// so a batch is one round trip rather than a fraction or a multiple of one.
const SNAPSHOT_BATCH: usize = 256;

/// A forward cursor over one range, merging the snapshot with the session's
/// staged writes as it goes -- Go's `unionIter` over `kv.Iterator`.
///
/// Both halves are pulled, not materialized:
///
/// * the snapshot is read one [`SNAPSHOT_BATCH`] at a time, each refill
///   starting at the key just past the last one served, so a consumer that
///   stops after one row (a `LIMIT 1`) leaves every later batch unread. This
///   is what makes an early-stopping cursor cost what it reads instead of what
///   its range holds.
/// * the staged half is the transaction's own uncommitted writes for this
///   range, taken from the session's `BTreeMap` once at open. That copy is
///   deliberate and is not the eager read this shape exists to avoid: it is
///   process-local memory bounded by what this transaction itself wrote, and
///   borrowing it lazily instead would mean holding the session's buffer lock
///   for the whole lifetime of the cursor -- including across the cluster
///   round trips above, and against the same statement's own writes.
///
/// The merge is the linear walk it always was: the staged entry wins a tie (it
/// is the transaction's newer write) and a tombstone drops the key entirely,
/// so a staged row still shadows, inserts and deletes at exactly its position
/// in key order.
#[derive(Debug)]
struct MergedIterator {
    snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
    read_keys: StatementReadKeys,
    /// Where the next refill starts, or `None` once the snapshot half of the
    /// range is drained.
    cursor: Option<Key>,
    end: Key,
    batch: SnapshotPairs,
    batch_position: usize,
    staged: Vec<(Key, Option<Vec<u8>>)>,
    staged_position: usize,
    /// The pair `key`/`value` report, which the seam hands out as borrows.
    current: Option<(Key, Vec<u8>)>,
    empty_key: Key,
}

impl MergedIterator {
    /// Opens the cursor on the first merged pair of `[start, end)`, reading
    /// one snapshot batch to find it.
    fn open(
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        read_keys: StatementReadKeys,
        start: Key,
        end: Key,
        staged: Vec<(Key, Option<Vec<u8>>)>,
    ) -> Result<Self, StorageError> {
        let empty = end <= start;
        let mut iterator = MergedIterator {
            snapshot,
            read_keys,
            cursor: (!empty).then_some(start),
            end,
            batch: Vec::new(),
            batch_position: 0,
            staged: if empty { Vec::new() } else { staged },
            staged_position: 0,
            current: None,
            empty_key: Key::default(),
        };
        iterator.advance()?;
        Ok(iterator)
    }

    /// Reads the next snapshot batch when the current one is spent.
    fn refill(&mut self) -> Result<(), StorageError> {
        if self.batch_position < self.batch.len() {
            return Ok(());
        }
        let Some(start) = self.cursor.take() else {
            return Ok(());
        };
        if start >= self.end {
            return Ok(());
        }
        let pairs = self
            .snapshot
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .scan(&start, &self.end, Some(SNAPSHOT_BATCH))?;
        // A short batch is the end of the range; a full one may not be, so the
        // next refill resumes at the smallest key this batch cannot have
        // covered.
        if pairs.len() >= SNAPSHOT_BATCH {
            if let Some((last, _)) = pairs.last() {
                let mut next = last.clone();
                next.push(0);
                self.cursor = Some(Key::from_bytes(next));
            }
        }
        self.batch = pairs;
        self.batch_position = 0;
        Ok(())
    }

    /// Produces the next merged pair, or `None` at the end of the range.
    fn advance(&mut self) -> Result<(), StorageError> {
        loop {
            self.refill()?;
            let snapshot_head = self.batch.get(self.batch_position).map(|(key, _)| key);
            let staged_head = self.staged.get(self.staged_position).map(|(key, _)| key);
            let order = match (snapshot_head, staged_head) {
                (None, None) => {
                    self.current = None;
                    return Ok(());
                }
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some(snapshot_key), Some(staged_key)) => {
                    snapshot_key.as_slice().cmp(staged_key.as_bytes())
                }
            };
            if order != std::cmp::Ordering::Greater {
                let (key, value) = self.batch[self.batch_position].clone();
                self.batch_position += 1;
                self.read_keys.record(&Key::from_bytes(key.clone()));
                if order == std::cmp::Ordering::Less {
                    self.current = Some((Key::from_bytes(key), value));
                    return Ok(());
                }
                // Equal: the transaction's own write replaces this key.
            }
            let (key, value) = self.staged[self.staged_position].clone();
            self.staged_position += 1;
            if let Some(value) = value {
                self.current = Some((key, value));
                return Ok(());
            }
            // A tombstone yields nothing; keep walking.
        }
    }
}

impl StorageIterator for MergedIterator {
    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn key(&self) -> &Key {
        let key = self
            .current
            .as_ref()
            .map_or(&self.empty_key, |(key, _)| key);
        key
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .map_or(&[][..], |(_, value)| value.as_slice())
    }

    fn next(&mut self) -> Result<(), StorageError> {
        if !self.valid() {
            return Err(StorageError::InvalidIterator);
        }
        self.advance()
    }

    fn close(&mut self) {
        self.current = None;
        self.cursor = None;
        self.batch = Vec::new();
        self.batch_position = 0;
        self.staged_position = self.staged.len();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_locks_inspect_native_writes_even_when_values_are_equal() {
        let buffer = MutationBuffer::new();
        let key = Key::from_bytes(b"meta-key".to_vec());
        buffer.set(key.clone(), b"value".to_vec()).unwrap();
        let statement = buffer.checkpoint();
        buffer.set(key.clone(), b"value".to_vec()).unwrap();
        assert_eq!(
            buffer.pessimistic_keys_since(statement),
            vec![key.into_bytes()]
        );
    }

    #[test]
    fn statement_lock_selection_preserves_native_flags() {
        use tikv_client::kv::{apply_flags_ops, FlagsOp, KeyFlags};
        let key = tidb_codec::table_key::encode_index_seek_key(1, 1, b"index-value");
        let plain = KeyFlags::default();
        let forced = apply_flags_ops(plain, &[FlagsOp::SetNeedLocked]);
        let presumed = apply_flags_ops(plain, &[FlagsOp::SetPresumeKeyNotExists]);
        let deferred = apply_flags_ops(presumed, &[FlagsOp::SetNeedConstraintCheckInPrewrite]);
        assert!(!key_needs_pessimistic_lock(&key, b"0", plain));
        assert!(key_needs_pessimistic_lock(&key, b"0", forced));
        assert!(key_needs_pessimistic_lock(&key, b"", forced));
        assert!(key_needs_pessimistic_lock(&key, b"0", presumed));
        assert!(!key_needs_pessimistic_lock(&key, b"0", deferred));
        assert!(key_needs_pessimistic_lock(b"metadata", b"", deferred));
        assert!(key_needs_pessimistic_lock(
            &key,
            &1_i64.to_be_bytes(),
            plain
        ));
    }

    /// A snapshot that answers from a fixed map and counts what the storage
    /// asked it for, so a test can prove a read never reached the cluster.
    #[derive(Debug, Default)]
    struct MockSnapshot {
        data: BTreeMap<Vec<u8>, Vec<u8>>,
        gets: Vec<Vec<u8>>,
        scans: Vec<(Vec<u8>, Vec<u8>)>,
        /// Every pair this snapshot handed back, summed over all scans.
        rows_read: usize,
        fail_with: Option<StorageError>,
    }

    impl ClusterSnapshot for MockSnapshot {
        fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
            if let Some(error) = self.fail_with.clone() {
                return Err(error);
            }
            self.gets.push(key.as_bytes().to_vec());
            Ok(self.data.get(key.as_bytes()).cloned())
        }

        fn scan(
            &mut self,
            start: &Key,
            end: &Key,
            limit: Option<usize>,
        ) -> Result<SnapshotPairs, StorageError> {
            if let Some(error) = self.fail_with.clone() {
                return Err(error);
            }
            self.scans
                .push((start.as_bytes().to_vec(), end.as_bytes().to_vec()));
            let pairs: SnapshotPairs = self
                .data
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
                .take(limit.unwrap_or(usize::MAX))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            // What the cluster actually served, which is the cost a scan pays
            // whether or not the caller goes on to consume it.
            self.rows_read += pairs.len();
            Ok(pairs)
        }
    }

    fn key(bytes: &[u8]) -> Key {
        Key::from_bytes(bytes.to_vec())
    }

    fn storage(
        pairs: &[(&[u8], &[u8])],
    ) -> (
        ClusterTableStorage,
        Arc<Mutex<MockSnapshot>>,
        MutationBuffer,
    ) {
        let snapshot = Arc::new(Mutex::new(MockSnapshot {
            data: pairs
                .iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect(),
            ..MockSnapshot::default()
        }));
        let buffer = MutationBuffer::new();
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
        (
            ClusterTableStorage::new(buffer.clone(), handle),
            snapshot,
            buffer,
        )
    }

    #[test]
    fn native_buffer_limits_return_sql_errors_and_statement_rollback() {
        let buffer = MutationBuffer::new();
        buffer
            .state()
            .memdb
            .write(|native| native.set_entry_size_limit(10, 16));
        let oversized = buffer.set(key(b"123456"), b"12345".to_vec()).unwrap_err();
        assert!(matches!(oversized, StorageError::Sql(ref error)
            if error.code == tidb_txnkv::MysqlErrorCode::EntryTooLarge.as_u16()));
        assert!(buffer.is_empty());
        buffer.set(key(b"a"), b"1234567".to_vec()).unwrap();
        let checkpoint = buffer.checkpoint();
        buffer.set(key(b"b"), b"1234567".to_vec()).unwrap();
        let oversized = buffer.set(key(b"c"), b"1".to_vec()).unwrap_err();
        assert!(matches!(oversized, StorageError::Sql(ref error)
            if error.code == tidb_txnkv::MysqlErrorCode::TxnTooLarge.as_u16()));
        buffer.restore(checkpoint);
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.get(&key(b"a")), Some(Some(b"1234567".to_vec())));
        assert_eq!(buffer.get(&key(b"b")), None);
        assert_eq!(buffer.get(&key(b"c")), None);
    }

    #[test]
    fn owned_batch_borrows_the_native_buffer_once_and_keeps_statement_rollback() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let buffer = MutationBuffer::new();
        let native = Arc::new(Mutex::new(
            tikv_client::transaction::unionstore::MemDb::default(),
        ));
        let accesses = Arc::new(AtomicUsize::new(0));
        buffer.bind_native(42, {
            let native = Arc::clone(&native);
            let accesses = Arc::clone(&accesses);
            Arc::new(move |visit| {
                accesses.fetch_add(1, Ordering::Relaxed);
                visit(&mut native.lock().unwrap());
                true
            })
        });
        let checkpoint = buffer.checkpoint();
        accesses.store(0, Ordering::Relaxed);
        buffer
            .stage_owned_batch([
                (
                    key(b"a"),
                    Some(b"value".to_vec()),
                    true,
                    tidb_txnkv::AssertionOp::AssertNotExist,
                ),
                (key(b"b"), None, false, tidb_txnkv::AssertionOp::AssertNone),
            ])
            .unwrap();
        assert_eq!(accesses.load(Ordering::Relaxed), 1);
        assert_eq!(buffer.write_details(), (7, 2));
        assert_eq!(buffer.get(&key(b"a")), Some(Some(b"value".to_vec())));
        assert_eq!(buffer.get(&key(b"b")), Some(None));
        assert!(native
            .lock()
            .unwrap()
            .get_flags_readonly(b"a")
            .unwrap()
            .has_presume_key_not_exists());
        buffer.restore(checkpoint);
        assert!(buffer.get(&key(b"a")).is_none());
        assert!(buffer.get(&key(b"b")).is_none());
    }

    #[test]
    fn binding_an_empty_sql_handle_preserves_native_lock_metadata() {
        use tikv_client::kv::FlagsOp;
        let buffer = MutationBuffer::new();
        let native = Arc::new(Mutex::new(
            tikv_client::transaction::unionstore::MemDb::default(),
        ));
        native.lock().unwrap().update_flags(
            b"locked",
            &[FlagsOp::SetKeyLocked, FlagsOp::SetAssertUnknown],
        );
        buffer.bind_native(42, {
            let native = native.clone();
            Arc::new(move |visit| {
                visit(&mut native.lock().unwrap());
                true
            })
        });
        buffer.set(key(b"locked"), b"value".to_vec()).unwrap();
        let native = native.lock().unwrap();
        let flags = native.get_flags_readonly(b"locked").unwrap();
        assert!(flags.has_locked());
        assert!(flags.has_assert_unknown());
        assert_eq!(native.get_readonly(b"locked").unwrap(), b"value");
    }

    #[test]
    fn completed_statement_scopes_release_without_losing_outer_savepoints() {
        let buffer = MutationBuffer::new();
        let key = Key::from_bytes(b"key".to_vec());
        buffer.set(key.clone(), b"old".to_vec()).unwrap();
        let savepoint = buffer.checkpoint();
        for _ in 0..64 {
            let statement = buffer.checkpoint();
            buffer.set(key.clone(), b"new".to_vec()).unwrap();
            buffer.release(statement);
            assert_eq!(buffer.state().memdb.stages().len(), 1);
        }
        buffer.restore(savepoint);
        assert_eq!(buffer.get(&key), Some(Some(b"old".to_vec())));
        buffer.release(savepoint);
        assert!(buffer.state().memdb.stages().is_empty());
        assert!(buffer.state().changes.is_empty());
    }

    #[test]
    fn insert_dup_check_is_in_place_eagerly_and_local_only_lazily() {
        use crate::kv_table::{KvColumn, KvIndex, KvTable};
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};

        let record_key = Key::from_bytes(encode_row_key_with_handle(42, &RecordHandle::Int(1)));
        let mut snapshot = MockSnapshot {
            ..MockSnapshot::default()
        };
        snapshot
            .data
            .insert(record_key.as_bytes().to_vec(), b"committed row".to_vec());
        let snapshot = std::sync::Arc::new(std::sync::Mutex::new(snapshot));
        let buffer = MutationBuffer::new();
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
        let mut table = KvTable::with_storage(
            42,
            vec![KvColumn {
                name: "a".to_owned(),
                id: 1,
                field_type: FieldType::new(FieldTypeCode::LongLong),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                origin_default: None,
                comment: String::new(),
                generated: None,
            }],
            Box::new(ClusterTableStorage::new(buffer.clone(), handle)),
        );
        let ctx = crate::StmtContext::default();
        let row = [Datum::Int(7)];
        table.add_index(
            KvIndex {
                id: 1,
                name: "unique_a".to_owned(),
                comment: String::new(),
                unique: true,
                column_offsets: vec![0],
                prefix_lengths: vec![-1],
                visible: true,
                global: false,
                global_index_version: 0,
                clustered_primary: false,
            },
            false,
        );

        // In place: the committed duplicate is reported at statement time,
        // exactly Go's eager `txn.Get` arm finding the key.
        let error = table
            .insert_row_with_row_id_checked(&row, Some(1), 0, &ctx, false)
            .unwrap_err();
        assert!(matches!(
            error,
            crate::kv_table::KvTableError::DuplicateEntry { .. }
        ));
        assert!(buffer.take_presume_not_exists().is_empty());

        // Lazy: the same statement reads nothing from the cluster, succeeds,
        // and stages the row presumed absent for the commit to verify.
        let error_count = snapshot.lock().unwrap().gets.len();
        table
            .insert_row_with_row_id_checked(&row, Some(1), 0, &ctx, true)
            .unwrap();
        assert_eq!(snapshot.lock().unwrap().gets.len(), error_count);
        let marks = buffer.take_presume_not_exists();
        assert!(marks.contains(&record_key));
        assert_eq!(
            marks.len(),
            2,
            "record and unique index share the lazy policy"
        );
        let index_key = marks
            .iter()
            .find(|key| tidb_tablecodec::is_index_key(key.as_bytes()))
            .unwrap()
            .clone();

        let checkpoint = buffer.checkpoint();
        assert!(
            matches!(
                table.insert_row_with_row_id_checked(&row, Some(2), 0, &ctx, true),
                Err(crate::kv_table::KvTableError::DuplicateEntry { value, .. }) if value == "7"
            ),
            "a duplicate staged by this transaction is checked locally"
        );
        buffer.restore(checkpoint);
        buffer.delete(index_key.clone()).unwrap();
        table
            .insert_row_with_row_id_checked(&row, Some(2), 0, &ctx, true)
            .unwrap();
        assert!(
            !buffer.take_presume_not_exists().contains(&index_key),
            "replacing a local tombstone does not add an absence assertion"
        );
        assert_eq!(snapshot.lock().unwrap().gets.len(), error_count);

        buffer.reset();
        snapshot.lock().unwrap().fail_with =
            Some(StorageError::Backend("index read failed".to_owned()));
        let error = table
            .insert_row_with_row_id_checked_without_primary_duplicate_check(
                &[Datum::Int(8)],
                Some(3),
                0,
                &ctx,
                false,
            )
            .expect_err("a failed eager index read is not an absent key");
        assert!(
            matches!(error, crate::kv_table::KvTableError::Storage(detail) if detail.contains("index read failed"))
        );
        assert!(
            buffer.is_empty(),
            "read failure must not stage the index or record"
        );
    }

    #[test]
    fn get_local_reads_only_the_staged_writes() {
        let (mut store, snapshot, _buffer) = storage(&[(b"a", b"snap")]);
        // A key only the SNAPSHOT holds is not local: Go `GetLocal` answers
        // `ErrNotExist` without touching the cluster.
        assert_eq!(store.get_local(&key(b"a")), Err(StorageError::NotFound));
        assert!(snapshot.lock().unwrap().gets.is_empty());
        // A staged value is the local answer, whatever the snapshot holds.
        store.set(key(b"a"), b"mine".to_vec()).unwrap();
        assert_eq!(store.get_local(&key(b"a")).unwrap(), b"mine".to_vec());
        assert!(snapshot.lock().unwrap().gets.is_empty());
        // A staged tombstone reads back EMPTY, not missing -- Go's
        // `GetLocal` returning a zero-length value for a delete.
        store.delete(key(b"a")).unwrap();
        assert_eq!(store.get_local(&key(b"a")).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn presumption_marks_follow_the_buffer_lifecycle() {
        let buffer = MutationBuffer::new();
        let first = key(b"k1");
        let second = key(b"k2");
        buffer.mark_presume_key_not_exists_with_hint(&first, "1", "t.PRIMARY");
        buffer.set(first.clone(), b"v1".to_vec()).unwrap();
        // The statement's savepoint: the mark and its write are both in.
        let savepoint = buffer.checkpoint();
        assert!(buffer
            .presume_not_exists_keys()
            .contains(&first.as_bytes().to_vec()));
        assert_eq!(
            buffer.duplicate_key_hint_for(first.as_bytes()),
            Some(DuplicateKeyHint {
                value: "1".to_owned(),
                key: "t.PRIMARY".to_owned(),
            })
        );
        // A second statement inserts another presumed-absent row ...
        buffer.mark_presume_key_not_exists(&second);
        buffer.set(second.clone(), b"v2".to_vec()).unwrap();
        assert_eq!(
            buffer.presume_not_exists_since(savepoint),
            BTreeSet::from([second.as_bytes().to_vec()]),
            "earlier INSERT commit flags are not new statement assertions"
        );
        // ... which then FAILS and rolls back to the savepoint: the withdrawn
        // write takes its presumption with it, while the earlier statement's
        // mark -- on a key the restored image still stages -- survives.
        buffer.restore(savepoint);
        assert!(buffer.presume_not_exists_since(savepoint).is_empty());
        assert_eq!(buffer.take_presume_not_exists(), {
            let mut set = std::collections::BTreeSet::new();
            set.insert(first.clone());
            set
        });
        // A drained mark does not survive publication: COMMIT consumes the
        // set once, whatever the outcome it reports.
        assert!(buffer.take_presume_not_exists().is_empty());
        // And ending the transaction empties the buffer and every remaining
        // presumption with it.
        buffer.mark_presume_key_not_exists(&second);
        buffer.reset();
        assert!(buffer.take_presume_not_exists().is_empty());
    }

    #[test]
    fn get_reads_the_buffer_before_the_snapshot() {
        let (mut store, snapshot, buffer) = storage(&[(b"a", b"snap")]);
        // A key the transaction never touched falls through to the snapshot.
        assert_eq!(store.get(&key(b"a")).unwrap(), b"snap".to_vec());
        assert_eq!(snapshot.lock().unwrap().gets, vec![b"a".to_vec()]);
        // Its own write shadows the snapshot, without asking the cluster.
        store.set(key(b"a"), b"mine".to_vec()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), b"mine".to_vec());
        assert_eq!(snapshot.lock().unwrap().gets.len(), 1);
        // A staged delete hides the snapshot's value, also without a read.
        store.delete(key(b"a")).unwrap();
        assert_eq!(store.get(&key(b"a")), Err(StorageError::NotFound));
        assert_eq!(snapshot.lock().unwrap().gets.len(), 1);
        assert_eq!(buffer.get(&key(b"a")), Some(None));
        // A key in neither is missing, and nothing was committed.
        assert_eq!(store.get(&key(b"zz")), Err(StorageError::NotFound));
        assert!(snapshot.lock().unwrap().data.contains_key(b"a".as_slice()));
    }

    #[test]
    fn iter_merges_staged_writes_into_the_snapshot_order() {
        let (mut store, snapshot, _) = storage(&[(b"a", b"1"), (b"c", b"3"), (b"e", b"5")]);
        store.set(key(b"b"), b"2".to_vec()).unwrap();
        store.set(key(b"c"), b"3-new".to_vec()).unwrap();
        store.delete(key(b"e")).unwrap();
        store.set(key(b"z"), b"26".to_vec()).unwrap();
        let mut iterator = store.iter(Some(&key(b"a")), Some(&key(b"f"))).unwrap();
        let mut seen = Vec::new();
        while iterator.valid() {
            seen.push((
                iterator.key().as_bytes().to_vec(),
                iterator.value().to_vec(),
            ));
            iterator.next().unwrap();
        }
        iterator.close();
        assert_eq!(
            seen,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3-new".to_vec()),
            ]
        );
        // The scan asked the cluster for exactly the caller's range; the key
        // staged outside it never entered the merge.
        assert_eq!(
            snapshot.lock().unwrap().scans,
            vec![(b"a".to_vec(), b"f".to_vec())]
        );
        // An exhausted cursor reports the source's iterator error.
        assert_eq!(iterator.next(), Err(StorageError::InvalidIterator));
    }

    #[test]
    fn clones_share_the_session_buffer_and_snapshot() {
        let (mut store, _, buffer) = storage(&[(b"a", b"1")]);
        let mut other = store.clone_box();
        other.set(key(b"b"), b"2".to_vec()).unwrap();
        // One session, one buffer: a write through one table handle is visible
        // through another, exactly as two `table.Table` handles of one Go
        // transaction see one `MemBuffer`.
        assert_eq!(store.get(&key(b"b")).unwrap(), b"2".to_vec());
        assert_eq!(buffer.snapshot().len(), 1);
        assert_eq!(buffer.len(), 1);
        assert_eq!(store.key_count(), 1);
        buffer.reset();
        assert!(buffer.is_empty());
        assert_eq!(store.get(&key(b"b")), Err(StorageError::NotFound));
    }

    #[test]
    fn transferring_the_native_buffer_keeps_tombstones_and_assertions() {
        let buffer = MutationBuffer::new();
        buffer.set(key(b"b"), b"value".to_vec()).unwrap();
        buffer
            .stage_owned_batch([
                (
                    key(b"a"),
                    Some(b"insert".to_vec()),
                    true,
                    tidb_txnkv::AssertionOp::AssertUnknown,
                ),
                (key(b"a"), None, false, tidb_txnkv::AssertionOp::AssertNone),
            ])
            .unwrap();
        let native = buffer.take_native_buffer();
        assert_eq!(native.get_readonly(b"b").unwrap(), b"value");
        assert!(native.get_readonly(b"a").unwrap().is_empty());
        let flags = native.get_flags_readonly(b"a").unwrap();
        assert!(flags.has_presume_key_not_exists());
        assert!(flags.has_assert_unknown());
        assert!(buffer.is_empty());
    }

    #[test]
    fn retryable_snapshot_failures_reach_the_caller() {
        let (mut store, snapshot, _) = storage(&[(b"a", b"1")]);
        snapshot.lock().unwrap().fail_with =
            Some(StorageError::Retryable("region epoch is stale".to_owned()));
        assert_eq!(
            store.get(&key(b"a")),
            Err(StorageError::Retryable("region epoch is stale".to_owned()))
        );
        assert!(matches!(
            store.iter(Some(&key(b"a")), Some(&key(b"b"))),
            Err(StorageError::Retryable(_))
        ));
    }

    #[test]
    fn restore_puts_the_buffer_back_where_a_statement_found_it() {
        let buffer = MutationBuffer::new();
        buffer.set(key(b"a"), b"1".to_vec()).unwrap();
        let savepoint = buffer.checkpoint();
        // A statement writes, deletes, and overwrites; restoring undoes all
        // three and leaves the earlier write exactly as it was.
        buffer.set(key(b"b"), b"2".to_vec()).unwrap();
        buffer.delete(key(b"a")).unwrap();
        buffer.restore(savepoint);
        assert_eq!(buffer.get(&key(b"a")), Some(Some(b"1".to_vec())));
        assert_eq!(buffer.get(&key(b"b")), None);
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn a_rebound_slot_serves_the_new_statements_snapshot() {
        let slot = Arc::new(Mutex::new(SwappableSnapshot::new()));
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&slot) as _;
        let mut store = ClusterTableStorage::new(MutationBuffer::new(), handle);
        // An unbound slot is a loud error, never an empty table.
        assert!(matches!(
            store.get(&key(b"a")),
            Err(StorageError::Backend(_))
        ));

        let first = MockSnapshot {
            data: [(b"a".to_vec(), b"first".to_vec())].into_iter().collect(),
            ..MockSnapshot::default()
        };
        assert!(slot.lock().unwrap().bind(Box::new(first)).is_none());
        assert_eq!(store.get(&key(b"a")).unwrap(), b"first".to_vec());

        // The next statement's snapshot replaces it without touching the
        // table, which is the whole point of the slot.
        let previous = slot
            .lock()
            .unwrap()
            .bind(Box::new(MockSnapshot {
                data: [(b"a".to_vec(), b"second".to_vec())].into_iter().collect(),
                ..MockSnapshot::default()
            }))
            .expect("the first snapshot is still bound");
        drop(previous);
        assert_eq!(store.get(&key(b"a")).unwrap(), b"second".to_vec());

        assert!(slot.lock().unwrap().unbind().is_some());
        assert!(!slot.lock().unwrap().is_bound());
        assert!(matches!(
            store.get(&key(b"a")),
            Err(StorageError::Backend(_))
        ));
    }

    #[test]
    fn unbounded_and_truncating_operations_are_refused() {
        let (mut store, _, _) = storage(&[(b"a", b"1")]);
        assert!(matches!(
            store.iter(Some(&key(b"a")), None),
            Err(StorageError::Backend(_))
        ));
        // An empty or inverted range yields nothing rather than a cluster scan.
        let iterator = store.iter(Some(&key(b"b")), Some(&key(b"b"))).unwrap();
        assert!(!iterator.valid());
        // TRUNCATE has no cluster meaning here, so the handle refuses to keep
        // serving rather than silently reporting an empty table.
        store.clear();
        assert!(matches!(
            store.get(&key(b"a")),
            Err(StorageError::Backend(_))
        ));
        assert!(matches!(
            store.set(key(b"a"), Vec::new()),
            Err(StorageError::Backend(_))
        ));
    }

    /// A TRUNCATE names ONE table, so its refusal must not reach the OTHER
    /// tables of the same session -- which share this storage's buffer and
    /// snapshot by design. Go's TRUNCATE swaps the truncated table for a
    /// fresh one and the connection carries on; captured from TiDB, a query
    /// on a different table right after `TRUNCATE TABLE ai` answers normally.
    #[test]
    fn truncating_one_table_leaves_the_sessions_other_tables_usable() {
        let (mut truncated, _snapshot, buffer) = storage(&[(b"a", b"1")]);
        // The second table of the same session: same buffer, same snapshot.
        let mut other = truncated.clone();
        truncated.clear();
        assert!(
            matches!(truncated.get(&key(b"a")), Err(StorageError::Backend(_))),
            "the truncated handle stays refused"
        );
        assert_eq!(
            other.get(&key(b"a")).unwrap(),
            b"1".to_vec(),
            "a sibling table of the same session still reads"
        );
        other.set(key(b"b"), b"2".to_vec()).unwrap();
        assert_eq!(buffer.len(), 1, "and still stages into the session buffer");
    }

    /// A key wide enough that byte order and numeric order agree.
    fn row_key(index: usize) -> Vec<u8> {
        format!("row{index:06}").into_bytes()
    }

    /// A storage over `rows` snapshot rows, with every tenth row also staged
    /// (a newer value) and every hundredth staged as a tombstone.
    fn large_storage(
        rows: usize,
    ) -> (
        ClusterTableStorage,
        Arc<Mutex<MockSnapshot>>,
        MutationBuffer,
    ) {
        let snapshot = Arc::new(Mutex::new(MockSnapshot {
            data: (0..rows)
                .map(|index| (row_key(index), format!("snap{index}").into_bytes()))
                .collect(),
            ..MockSnapshot::default()
        }));
        let buffer = MutationBuffer::new();
        for index in (0..rows).step_by(10) {
            if index % 100 == 0 {
                buffer.delete(Key::from_bytes(row_key(index))).unwrap();
            } else {
                buffer
                    .set(
                        Key::from_bytes(row_key(index)),
                        format!("mine{index}").into_bytes(),
                    )
                    .unwrap();
            }
        }
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
        (
            ClusterTableStorage::new(buffer.clone(), handle),
            snapshot,
            buffer,
        )
    }

    /// A cursor the caller stops after one row must cost one batch, not the
    /// range. This is the `LIMIT 1` over a big table: while `iter` merged the
    /// whole range at open, all 10_000 rows crossed the seam before the first
    /// was returned, and dropping the cursor saved nothing -- the reading was
    /// already done.
    #[test]
    fn a_cursor_dropped_after_one_row_reads_one_batch_not_the_range() {
        let rows = 10_000;
        let (mut store, snapshot, _buffer) = large_storage(rows);
        let start = key(b"row");
        let end = key(b"rox");
        {
            let iterator = store.iter(Some(&start), Some(&end)).unwrap();
            assert!(iterator.valid());
            // row000000 is staged as a tombstone, so the first merged row is
            // the snapshot's row000001: the merge is live, not skipped.
            assert_eq!(iterator.key().as_bytes(), row_key(1).as_slice());
            assert_eq!(iterator.value(), b"snap1");
            // The caller has its one row and abandons the cursor.
        }
        let read = snapshot.lock().unwrap().rows_read;
        assert!(
            read <= SNAPSHOT_BATCH,
            "a LIMIT 1 read {read} rows of {rows}; one batch of {SNAPSHOT_BATCH} is the budget"
        );
    }

    /// The bounded one-row primitive must retain the staged/snapshot merge.
    /// In particular, a staged tombstone at the range start exposes the next
    /// snapshot row instead of recursing through the trait override.
    #[test]
    fn first_row_with_staged_prefix_uses_the_merged_iterator() {
        let (mut store, _snapshot, _buffer) = large_storage(100);
        let start = key(b"row");
        let end = key(b"rox");

        let first = store.first(Some(&start), Some(&end)).unwrap().unwrap();
        assert_eq!(first.0.as_bytes(), row_key(1).as_slice());
        assert_eq!(first.1, b"snap1");
    }

    /// The batched merge must answer exactly what a one-shot merge did,
    /// including at a batch boundary: a staged insert that lands at the seam
    /// between two batches has no snapshot row to be compared against until
    /// the next batch has been pulled.
    #[test]
    fn batched_reading_yields_the_same_rows_as_one_shot_reading() {
        let rows = 1_000;
        let (mut store, snapshot, _buffer) = large_storage(rows);
        store
            .set(key(b"row000255a"), b"inserted-at-the-seam".to_vec())
            .unwrap();
        store
            .set(key(b"row000512a"), b"inserted-past-a-seam".to_vec())
            .unwrap();
        let mut expected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for index in 0..rows {
            if index % 100 == 0 {
                // A staged tombstone hides the snapshot's row entirely.
            } else if index % 10 == 0 {
                expected.push((row_key(index), format!("mine{index}").into_bytes()));
            } else {
                expected.push((row_key(index), format!("snap{index}").into_bytes()));
            }
            if index == 255 {
                expected.push((b"row000255a".to_vec(), b"inserted-at-the-seam".to_vec()));
            }
            if index == 512 {
                expected.push((b"row000512a".to_vec(), b"inserted-past-a-seam".to_vec()));
            }
        }
        let mut seen: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut iterator = store.iter(Some(&key(b"row")), Some(&key(b"rox"))).unwrap();
        while iterator.valid() {
            seen.push((
                iterator.key().as_bytes().to_vec(),
                iterator.value().to_vec(),
            ));
            iterator.next().unwrap();
        }
        assert_eq!(seen, expected);
        assert!(
            snapshot.lock().unwrap().scans.len() > 1,
            "a 1000-row range is more than one batch, so it took more than one scan"
        );
    }
}
