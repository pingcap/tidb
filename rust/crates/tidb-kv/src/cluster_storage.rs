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

//! The read half of a cluster transaction (Go `kv.Snapshot`) as the table
//! and executor layers reach it, and the duplicate-key identity the table
//! layer leaves for the driver's error conversion. The union store over it
//! -- the transaction's staged writes in front of this snapshot -- is the
//! transaction driver's (`tidb_store_driver_txn::cluster_storage`, Go
//! `pkg/store/driver/txn`).

use std::fmt;

use tidb_txnkv::Key;

use crate::storage::StorageError;

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
    /// Installs immutable statement policy on the native snapshot, where available.
    fn set_snapshot_read_options(&mut self, _options: tidb_txnkv::SnapshotReadOptions) {}

    /// Binds the session's statement ctx values (Go `RUDetails` and
    /// resource-group taggers) to every read this snapshot serves.
    fn set_statement_kv_context(&mut self, _context: &tidb_txnkv::StatementKvContextCell) {}

    /// Go `SnapshotRuntimeStats.GetCmdRPCCount` for point and batch-point
    /// commands issued by this snapshot so far.
    fn point_rpc_counts(&mut self) -> (u64, u64) {
        (0, 0)
    }

    /// See [`TableStorage::lock_keys_for_read`]. Only a locking statement
    /// handle of a pessimistic transaction locks; every other snapshot
    /// answers `Ok(())`.
    fn lock_keys_for_read(
        &mut self,
        _keys: &[Key],
        _wait: tidb_txnkv::transaction::LockWaitTime,
        _only_if_exists: bool,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    /// See [`TableStorage::do_lock_keys`]. Only a locking statement handle of
    /// a pessimistic transaction locks; every other snapshot answers `Ok(())`.
    fn do_lock_keys(
        &mut self,
        _keys: &[Key],
        _wait: tidb_txnkv::transaction::LockWaitTime,
    ) -> Result<(), StorageError> {
        Ok(())
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

    /// [`scan`](Self::scan) over the committed snapshot alone: Go's
    /// `GetSnapshot(startTS).Iter`, which has no membuffer. A snapshot that
    /// overlays its transaction's staged writes (a read-your-own-writes view)
    /// overrides this to skip the overlay; `TABLESAMPLE` reads through it.
    fn scan_committed(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        self.scan(start, end, limit)
    }

    /// [`get`](Self::get) over the committed snapshot alone: Go
    /// `GetSnapshot(startTS).Get`, which has no membuffer. A snapshot that
    /// overlays its transaction's staged writes overrides this to skip the
    /// overlay.
    fn get_committed(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.get(key)
    }

    /// [`batch_get`](Self::batch_get) over the committed snapshot alone; see
    /// [`get_committed`](Self::get_committed).
    fn batch_get_committed(&mut self, keys: &[Key]) -> Result<SnapshotPairs, StorageError> {
        self.batch_get(keys)
    }

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

/// SQL identity retained by the table layer for a deferred duplicate error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DuplicateKeyHint {
    /// SQL rendering of the duplicated value.
    pub value: String,
    /// SQL index or constraint name.
    pub key: String,
}
