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
//! * [`MutationBuffer`] -- the session-owned staged writes, shared by every
//!   table of the session (`Arc`), ordered by key (`BTreeMap`) so the merge
//!   below is a linear walk and so the COMMIT mutation set is already sorted
//!   the way `validate_and_sort` wants it. A staged delete is a tombstone
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
/// The position indexes an undo log the buffer keeps alongside its staged
/// map: taking a checkpoint copies NOTHING (Go's `Staging()` is equally
/// O(1)), and rolling back replays only the log entries written after it,
/// so a failed statement pays for its own writes and never for the bytes
/// earlier statements of the transaction already staged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BufferCheckpoint {
    undo_len: usize,
}

/// One recorded prior state in the buffer's undo log.
#[derive(Clone, Debug)]
enum UndoEntry {
    /// A `set`/`delete` overwrote this key; `prior` is the entry the map held
    /// before (`None` = the key was not staged).
    Write {
        key: Key,
        prior: Option<Option<Vec<u8>>>,
    },
    /// A presumption mark newly added for this key.
    Presume { key: Key },
}

/// Raw keys actually consumed by the current statement.
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
    /// Starts a new locking statement and discards any prior statement's keys.
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

/// The session's staged writes: Go's `kv.MemBuffer`.
///
/// `None` is a tombstone -- a staged delete of a key the snapshot may still
/// hold. Ordering is by key so the COMMIT mutation set and the scan merge both
/// get a sorted walk for free.
#[derive(Clone, Debug, Default)]
pub struct MutationBuffer {
    staged: Arc<Mutex<BTreeMap<Key, Option<Vec<u8>>>>>,
    /// Keys an INSERT staged presumed absent -- Go's per-key
    /// `SetPresumeKeyNotExists` flag on the `MemBuffer`. The committer turns
    /// a marked key into `Op_Insert`, so prewrite rejects it when a committed
    /// version turns out to exist; the pessimistic lock step reads the same
    /// set as Go's `KeysNeedToLock` reads its flags.
    presume_not_exists: Arc<Mutex<BTreeSet<Key>>>,
    /// Prior states of every write since the last `reset`, oldest first:
    /// what [`Self::checkpoint`] positions and [`Self::restore`] unwinds.
    undo: Arc<Mutex<Vec<UndoEntry>>>,
}

impl MutationBuffer {
    /// An empty buffer, as a session opens with.
    #[must_use]
    pub fn new() -> Self {
        MutationBuffer::default()
    }

    /// Stages a write, replacing any earlier staged value or tombstone.
    pub fn set(&self, key: Key, value: Vec<u8>) {
        let prior = self.lock().insert(key.clone(), Some(value));
        self.undo()
            .push(UndoEntry::Write { key, prior });
    }

    /// Stages a delete as a tombstone, so the read path stops seeing the
    /// snapshot's value for the key.
    pub fn delete(&self, key: Key) {
        let prior = self.lock().insert(key.clone(), None);
        self.undo()
            .push(UndoEntry::Write { key, prior });
    }

    fn undo(&self) -> std::sync::MutexGuard<'_, Vec<UndoEntry>> {
        self.undo.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    /// The staged entry for `key`: `None` if the key was never touched,
    /// `Some(None)` if it is a tombstone, `Some(Some(value))` if it was set.
    #[must_use]
    pub fn get(&self, key: &Key) -> Option<Option<Vec<u8>>> {
        self.lock().get(key).cloned()
    }

    /// Marks one staged key presumed absent (`kv.SetPresumeKeyNotExists`).
    /// Only a key this buffer just STAGED is marked: Go sets the flag when
    /// `AddRecord`'s lazy check finds no local entry, and never on a tombstone
    /// overwrite, whose plain `Set` must stay one.
    pub fn mark_presume_key_not_exists(&self, key: &Key) {
        if self.presume().insert(key.clone()) {
            // Newly marked: a rollback past this point withdraws the mark
            // with the insert that carried it.
            self.undo().push(UndoEntry::Presume { key: key.clone() });
        }
    }

    /// Drains every presumption mark, in no particular order. COMMIT consumes
    /// this set to type its mutations; like Go's flags, which die with the
    /// membuffer, a drained mark does not survive the publication attempt.
    pub fn take_presume_not_exists(&self) -> BTreeSet<Key> {
        let mut marks = self.presume();
        std::mem::take(&mut *marks)
    }

    /// Every staged entry in `[start, end)`, in key order.
    #[must_use]
    pub fn range(&self, start: &Key, end: &Key) -> Vec<(Key, Option<Vec<u8>>)> {
        self.lock()
            .range(start.clone()..end.clone())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    /// A checkpoint of the current moment: O(1), no copying. Go's
    /// `MemBuffer.Staging()`.
    #[must_use]
    pub fn checkpoint(&self) -> BufferCheckpoint {
        BufferCheckpoint {
            undo_len: self.undo().len(),
        }
    }

    /// Every staged entry, in key order: the COMMIT mutation set.
    #[must_use]
    pub fn snapshot(&self) -> Vec<(Key, Option<Vec<u8>>)> {
        self.lock()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    /// What changed since `checkpoint`, as the `(before, after)` pair the
    /// pessimistic lock diff consumes: each side holds one entry per touched
    /// key -- the value the checkpoint would restore, and the value staged
    /// now. Cost is O(writes since the checkpoint), never O(staged bytes).
    #[must_use]
    pub fn delta_since(
        &self,
        checkpoint: BufferCheckpoint,
    ) -> (Vec<(Key, Option<Vec<u8>>)>, Vec<(Key, Option<Vec<u8>>)>) {
        let undo = self.undo();
        let mut keys: BTreeMap<Key, (Option<Option<Vec<u8>>>, ())> = BTreeMap::new();
        for entry in undo.iter().skip(checkpoint.undo_len) {
            match entry {
                UndoEntry::Write { key, prior } => {
                    keys.entry(key.clone())
                        .or_insert_with(|| (prior.clone(), ()));
                }
                UndoEntry::Presume { .. } => {}
            }
        }
        drop(undo);
        let staged = self.lock();
        let before: Vec<(Key, Option<Vec<u8>>)> = keys
            .iter()
            .map(|(key, (prior, _))| {
                (
                    key.clone(),
                    prior
                        .clone()
                        .unwrap_or(None),
                )
            })
            .collect();
        let after: Vec<(Key, Option<Vec<u8>>)> = keys
            .keys()
            .map(|key| (key.clone(), staged.get(key).cloned().unwrap_or(None)))
            .collect();
        (before, after)
    }

    /// How many keys the buffer stages, tombstones included.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the transaction has staged nothing, so COMMIT has no work.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Drops every staged entry: what COMMIT and ROLLBACK both do once the
    /// transaction ends.
    pub fn reset(&self) {
        self.lock().clear();
        self.presume().clear();
        self.undo().clear();
    }

    /// Rewinds the buffer to `checkpoint`: entries the log recorded after it
    /// are undone newest-first, so a key written several times lands back on
    /// the value it had at the checkpoint and a key the checkpoint never held
    /// leaves the map entirely.
    ///
    /// This is statement-level rollback: Go undoes a failed statement's writes
    /// back to the `MemBuffer` staging handle it took at statement start, so a
    /// failure inside an explicit transaction discards that statement's writes
    /// and keeps every earlier one. The rewind pays for the failed
    /// statement's OWN writes -- Go's checkpoint revert is the same shape --
    /// and never re-copies bytes earlier statements already staged.
    pub fn restore(&self, checkpoint: BufferCheckpoint) {
        let mut undo = self.undo();
        while undo.len() > checkpoint.undo_len {
            match undo.pop() {
                Some(UndoEntry::Write { key, prior }) => match prior {
                    None => {
                        self.lock().remove(&key);
                    }
                    Some(value) => {
                        self.lock().insert(key, value);
                    }
                },
                Some(UndoEntry::Presume { key }) => {
                    self.presume().remove(&key);
                }
                None => break,
            }
        }
    }

    fn presume(&self) -> std::sync::MutexGuard<'_, BTreeSet<Key>> {
        self.presume_not_exists
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<Key, Option<Vec<u8>>>> {
        // A poisoned buffer means another statement panicked mid-write; the
        // staged bytes are still exactly what was written before that, and the
        // transaction is going to be rolled back by its owner either way.
        self.staged
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
    fn get(&mut self, key: &Key) -> Result<Vec<u8>, StorageError> {
        self.check_usable()?;
        // Counted at the SEAM, as the in-process backend counts it, so the
        // two backends report the same request shape for the same plan and a
        // test can pin an access path against either. A key the session's own
        // buffer answers is still one `get` here, because the shape the count
        // describes is the plan's, not the transport's.
        crate::storage::note_storage_op(|ops| ops.gets += 1);
        self.read_keys.record(key);
        match self.buffer.get(key) {
            Some(Some(value)) => Ok(value),
            Some(None) => Err(StorageError::NotFound),
            None => self.snapshot_get(key)?.ok_or(StorageError::NotFound),
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
                values.insert(Key::from_bytes(key), value);
            }
        }
        for key in keys {
            self.read_keys.record(key);
        }
        Ok(values)
    }

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), StorageError> {
        self.check_usable()?;
        self.buffer.set(key, value);
        Ok(())
    }

    fn delete(&mut self, key: Key) -> Result<(), StorageError> {
        self.check_usable()?;
        self.buffer.delete(key);
        Ok(())
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

    /// Serves a clean scan through the node's unordered coprocessor path.
    /// Session-local staged writes require key-ordered merging, so that shape
    /// falls back to the snapshot cursor below instead.
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
        // ascending and disjoint, so the concatenation is still in key order,
        // which is the order the merge below relies on. A staged row outside
        // every range is dropped for the same reason a snapshot row there is:
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
        request.keep_order |= !staged.is_empty()
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
        if self.current.is_some() {
            self.read_keys.record(key);
        }
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

    /// Go `AddRecord`'s two duplicate-check modes, against one committed row:
    /// `pkg/table/tables/tables.go` reads the whole transaction IN PLACE (the
    // snapshot included) and reports 1062, while the lazy pessimistic arm
    /// reads ONLY `GetLocal`, reports nothing, and stages the row with
    /// `kv.SetPresumeKeyNotExists` -- deferring the verdict to prewrite, whose
    /// `Op_Insert` rejects a key that exists. This pins both arms and the no
    /// cluster read property of the second.
    #[test]
    fn insert_dup_check_is_in_place_eagerly_and_local_only_lazily() {
        use crate::kv_table::{KvColumn, KvTable};
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};

        let record_key = Key::from_bytes(encode_row_key_with_handle(
            42,
            &RecordHandle::Int(1),
        ));
        let mut snapshot = MockSnapshot {
            ..MockSnapshot::default()
        };
        snapshot.data.insert(
            record_key.as_bytes().to_vec(),
            b"committed row".to_vec(),
        );
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

        // In place: the committed duplicate is reported at statement time,
        // exactly Go's eager `txn.Get` arm finding the key.
        let error = table
            .insert_row_with_row_id_checked(&row, Some(1), 0, &ctx, false)
            .unwrap_err();
        assert!(matches!(error, crate::kv_table::KvTableError::DuplicateEntry { .. }));
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
        buffer.mark_presume_key_not_exists(&first);
        buffer.set(first.clone(), b"v1".to_vec());
        // The statement's savepoint: the mark and its write are both in.
        let savepoint = buffer.checkpoint();
        // A second statement inserts another presumed-absent row ...
        buffer.mark_presume_key_not_exists(&second);
        buffer.set(second.clone(), b"v2".to_vec());
        // ... which then FAILS and rolls back to the savepoint: the withdrawn
        // write takes its presumption with it, while the earlier statement's
        // mark -- on a key the restored image still stages -- survives.
        buffer.restore(savepoint);
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
        buffer.set(key(b"a"), b"1".to_vec());
        let savepoint = buffer.checkpoint();
        // A statement writes, deletes, and overwrites; restoring undoes all
        // three and leaves the earlier write exactly as it was.
        buffer.set(key(b"b"), b"2".to_vec());
        buffer.delete(key(b"a"));
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
                buffer.delete(Key::from_bytes(row_key(index)));
            } else {
                buffer.set(
                    Key::from_bytes(row_key(index)),
                    format!("mine{index}").into_bytes(),
                );
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
