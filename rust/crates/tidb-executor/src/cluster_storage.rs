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
//!   backend error naming the reason.
//! * [`key_count`](TableStorage::key_count) reports the staged key count only.
//!   TiKV has no exact count, and the seam's own doc already says so.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use tidb_txnkv::Key;

use crate::pushdown_scan::{
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
    /// Reads one key at the snapshot's timestamp. `None` is TiKV's
    /// `not_found`, which the caller turns into [`StorageError::NotFound`].
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError>;

    /// Reads every pair in `[start, end)` at the snapshot's timestamp, in key
    /// order.
    fn scan(&mut self, start: &Key, end: &Key) -> Result<SnapshotPairs, StorageError>;

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
}

/// The session's staged writes: Go's `kv.MemBuffer`.
///
/// `None` is a tombstone -- a staged delete of a key the snapshot may still
/// hold. Ordering is by key so the COMMIT mutation set and the scan merge both
/// get a sorted walk for free.
#[derive(Clone, Debug, Default)]
pub struct MutationBuffer {
    staged: Arc<Mutex<BTreeMap<Key, Option<Vec<u8>>>>>,
}

impl MutationBuffer {
    /// An empty buffer, as a session opens with.
    #[must_use]
    pub fn new() -> Self {
        MutationBuffer::default()
    }

    /// Stages a write, replacing any earlier staged value or tombstone.
    pub fn set(&self, key: Key, value: Vec<u8>) {
        self.lock().insert(key, Some(value));
    }

    /// Stages a delete as a tombstone, so the read path stops seeing the
    /// snapshot's value for the key.
    pub fn delete(&self, key: Key) {
        self.lock().insert(key, None);
    }

    /// The staged entry for `key`: `None` if the key was never touched,
    /// `Some(None)` if it is a tombstone, `Some(Some(value))` if it was set.
    #[must_use]
    pub fn get(&self, key: &Key) -> Option<Option<Vec<u8>>> {
        self.lock().get(key).cloned()
    }

    /// Every staged entry in `[start, end)`, in key order.
    #[must_use]
    pub fn range(&self, start: &Key, end: &Key) -> Vec<(Key, Option<Vec<u8>>)> {
        self.lock()
            .range(start.clone()..end.clone())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    /// Every staged entry, in key order: the COMMIT mutation set.
    #[must_use]
    pub fn staged(&self) -> Vec<(Key, Option<Vec<u8>>)> {
        self.lock()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
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
    }

    /// Replaces the whole buffer with `entries`, which [`Self::staged`]
    /// produced before a statement ran.
    ///
    /// This is statement-level rollback: Go undoes a failed statement's writes
    /// back to the `MemBuffer` staging handle it took at statement start, so a
    /// failure inside an explicit transaction discards that statement's writes
    /// and keeps every earlier one. Restoring a whole snapshot has the same
    /// effect at this seam, which records no per-key undo log.
    pub fn restore(&self, entries: Vec<(Key, Option<Vec<u8>>)>) {
        let mut staged = self.lock();
        staged.clear();
        staged.extend(entries);
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
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.snapshot()?.get(key)
    }

    fn scan(&mut self, start: &Key, end: &Key) -> Result<SnapshotPairs, StorageError> {
        self.snapshot()?.scan(start, end)
    }

    fn start_ts(&self) -> u64 {
        self.bound
            .as_ref()
            .map_or(0, |snapshot| snapshot.start_ts())
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
    truncated: Arc<Mutex<bool>>,
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
            truncated: Arc::new(Mutex::new(false)),
            scanner: None,
        }
    }

    /// Gives this session's tables a coprocessor to serve base-table scans
    /// with, so a predicate is evaluated at the region instead of after the
    /// range's bytes have crossed the network.
    ///
    /// The staged buffer is untouched by it: see
    /// [`TableStorage::open_pushdown_scan`] below for how the two are merged.
    #[must_use]
    pub fn with_pushdown_scanner(mut self, scanner: Arc<dyn PushdownScanner>) -> Self {
        self.scanner = Some(scanner);
        self
    }

    /// The session buffer these tables stage into, for the COMMIT path.
    #[must_use]
    pub fn buffer(&self) -> MutationBuffer {
        self.buffer.clone()
    }

    fn check_usable(&self) -> Result<(), StorageError> {
        if *self
            .truncated
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
        {
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

    fn snapshot_scan(&self, start: &Key, end: &Key) -> Result<SnapshotPairs, StorageError> {
        self.snapshot
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .scan(start, end)
    }
}

impl TableStorage for ClusterTableStorage {
    fn get(&mut self, key: &Key) -> Result<Vec<u8>, StorageError> {
        self.check_usable()?;
        match self.buffer.get(key) {
            Some(Some(value)) => Ok(value),
            Some(None) => Err(StorageError::NotFound),
            None => self.snapshot_get(key)?.ok_or(StorageError::NotFound),
        }
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
        if end <= start {
            return Ok(Box::new(MergedIterator::new(Vec::new())));
        }
        let snapshot = self.snapshot_scan(start, end)?;
        let staged = self.buffer.range(start, end);
        Ok(Box::new(MergedIterator::new(merge(snapshot, staged))))
    }

    /// Serves the scan through the node's coprocessor when it has one, and
    /// hands the session's staged writes for the same range back with it.
    ///
    /// The row cap is the one place the two halves interact. TiKV stops after
    /// `limit` *snapshot* rows, which is the right prefix only when nothing is
    /// staged in the range: a staged insert with a smaller key displaces the
    /// last remote row, and a staged delete uncovers a row past it. So the cap
    /// travels only when the staged range is empty, and the caller enforces it
    /// again either way.
    fn open_pushdown_scan(
        &mut self,
        request: &PushdownScanRequest,
    ) -> Option<Result<PushdownScan, StorageError>> {
        let scanner = self.scanner.as_ref()?;
        if let Err(error) = self.check_usable() {
            return Some(Err(error));
        }
        let snapshot_ts = self
            .snapshot
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .start_ts();
        let staged = self.buffer.range(&request.start, &request.end);
        let mut request = request.clone();
        request.snapshot_ts = snapshot_ts;
        if !staged.is_empty() {
            request.limit = None;
        }
        match scanner.open(&request) {
            Ok(stream) => Some(Ok(PushdownScan { stream, staged })),
            // A refusal is not a failure: the caller falls back to `iter`,
            // which answers the same question from the same snapshot.
            Err(PushdownScannerError::Unsupported(_)) => None,
            Err(PushdownScannerError::Backend(error)) => Some(Err(error)),
        }
    }

    fn key_count(&self) -> usize {
        self.buffer.len()
    }

    fn clear(&mut self) {
        *self
            .truncated
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = true;
    }

    fn clone_box(&self) -> Box<dyn TableStorage> {
        Box::new(self.clone())
    }
}

/// Merges the snapshot pairs and the staged entries of one range.
///
/// Both inputs are already sorted, so this is one linear pass: the staged
/// entry wins on a tie (it is the transaction's own newer write), and a
/// tombstone drops the key entirely.
fn merge(snapshot: SnapshotPairs, staged: Vec<(Key, Option<Vec<u8>>)>) -> Vec<(Key, Vec<u8>)> {
    let mut merged = Vec::with_capacity(snapshot.len() + staged.len());
    let mut snapshot = snapshot.into_iter().peekable();
    let mut staged = staged.into_iter().peekable();
    loop {
        let order = match (snapshot.peek(), staged.peek()) {
            (None, None) => break,
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (Some((snapshot_key, _)), Some((staged_key, _))) => {
                snapshot_key.as_slice().cmp(staged_key.as_bytes())
            }
        };
        match order {
            std::cmp::Ordering::Less => {
                let (key, value) = snapshot.next().expect("peeked");
                merged.push((Key::from_bytes(key), value));
            }
            std::cmp::Ordering::Greater => {
                let (key, value) = staged.next().expect("peeked");
                if let Some(value) = value {
                    merged.push((key, value));
                }
            }
            std::cmp::Ordering::Equal => {
                snapshot.next();
                let (key, value) = staged.next().expect("peeked");
                if let Some(value) = value {
                    merged.push((key, value));
                }
            }
        }
    }
    merged
}

/// A forward cursor over one already-merged range.
///
/// The range is materialized because a cluster scan is answered page by page
/// anyway; the source's streaming `Iterator` shape is preserved at the seam,
/// not in the transport.
#[derive(Debug)]
struct MergedIterator {
    pairs: Vec<(Key, Vec<u8>)>,
    position: usize,
    empty_key: Key,
}

impl MergedIterator {
    fn new(pairs: Vec<(Key, Vec<u8>)>) -> Self {
        MergedIterator {
            pairs,
            position: 0,
            empty_key: Key::default(),
        }
    }
}

impl StorageIterator for MergedIterator {
    fn valid(&self) -> bool {
        self.position < self.pairs.len()
    }

    fn key(&self) -> &Key {
        self.pairs
            .get(self.position)
            .map_or(&self.empty_key, |(key, _)| key)
    }

    fn value(&self) -> &[u8] {
        self.pairs
            .get(self.position)
            .map_or(&[][..], |(_, value)| value.as_slice())
    }

    fn next(&mut self) -> Result<(), StorageError> {
        if !self.valid() {
            return Err(StorageError::InvalidIterator);
        }
        self.position += 1;
        Ok(())
    }

    fn close(&mut self) {
        self.position = self.pairs.len();
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

        fn scan(&mut self, start: &Key, end: &Key) -> Result<SnapshotPairs, StorageError> {
            if let Some(error) = self.fail_with.clone() {
                return Err(error);
            }
            self.scans
                .push((start.as_bytes().to_vec(), end.as_bytes().to_vec()));
            Ok(self
                .data
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
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
        assert_eq!(buffer.staged().len(), 1);
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
        let savepoint = buffer.staged();
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
}
