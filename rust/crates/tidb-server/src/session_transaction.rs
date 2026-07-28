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

//! One connection's explicit-transaction state.
//!
//! Models the `BEGIN`/`START TRANSACTION` ... `COMMIT`/`ROLLBACK` lifecycle a
//! session carries across statements (Go `pkg/session` `LazyTxn`/`TxnState`).
//! The bounded read-only slice this owns pins ONE read snapshot for the whole
//! transaction — acquired lazily on the first read, exactly as TiDB defers the
//! transaction's `start_ts` to first use — so every read in the transaction
//! observes the same consistent snapshot instead of a fresh timestamp per
//! statement. Buffered writes and their commit-time two-phase commit are a
//! later slice; a write inside a transaction fails closed until then.
//!
//! The transaction's mode (`BEGIN PESSIMISTIC` / `BEGIN OPTIMISTIC` /
//! `@@tidb_txn_mode`) is resolved and recorded here, because it is what the
//! client asked for. It changes nothing yet: the only statements this slice
//! admits inside a transaction are reads at the pinned snapshot, which take no
//! pessimistic locks in either mode, and every locking or writing statement
//! fails closed rather than silently dropping a lock.

use tidb_planner::txn_mode::{txn_mode_for_begin, SessionTxnMode, TransactionMode};

/// The explicit-transaction state of one session.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SessionTransaction {
    /// Whether an explicit transaction is currently open.
    active: bool,
    /// The transaction's pinned read snapshot, acquired lazily on its first
    /// read. `None` while no transaction is open, or before the open
    /// transaction's first read.
    snapshot_ts: Option<u64>,
    /// The mode the open transaction was opened in. `None` while no
    /// transaction is open.
    mode: Option<SessionTxnMode>,
}

impl SessionTransaction {
    /// A session that starts outside any explicit transaction (autocommit).
    #[must_use]
    pub const fn new() -> Self {
        Self {
            active: false,
            snapshot_ts: None,
            mode: None,
        }
    }

    /// Opens an explicit transaction for `BEGIN` / `START TRANSACTION`.
    ///
    /// Re-issuing `BEGIN` while a transaction is already open implicitly ends
    /// the current one and starts a fresh transaction, matching MySQL — so any
    /// pinned snapshot is dropped and re-acquired on the next read.
    /// `mode` is the `BEGIN` statement's own keyword. This node has no
    /// `SET`-able session-variable store, so a bare `BEGIN` resolves against
    /// the registry default of `@@tidb_txn_mode`, which is `pessimistic`.
    pub fn begin(&mut self, mode: TransactionMode) {
        self.active = true;
        self.snapshot_ts = None;
        self.mode = Some(txn_mode_for_begin(
            mode,
            tidb_planner::txn_mode::PESSIMISTIC_TXN_MODE,
        ));
    }

    /// Ends the open transaction for `COMMIT` or `ROLLBACK`.
    ///
    /// A read-only transaction has nothing to publish, so commit and rollback
    /// are identical here: both release the pinned snapshot and return the
    /// session to autocommit. `COMMIT`/`ROLLBACK` with no open transaction is a
    /// no-op, which this expresses by simply clearing already-clear state.
    pub const fn end(&mut self) {
        self.active = false;
        self.snapshot_ts = None;
        self.mode = None;
    }

    /// The mode the open transaction runs in, if one is open.
    #[must_use]
    pub const fn mode(&self) -> Option<SessionTxnMode> {
        self.mode
    }

    /// Whether an explicit transaction is open, i.e. the connection should
    /// advertise `SERVER_STATUS_IN_TRANS`.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.active
    }

    /// Returns the snapshot a read must execute at.
    ///
    /// Inside an explicit transaction this is the pinned transaction snapshot,
    /// acquired via `acquire` on the first read and reused verbatim for every
    /// later read so the transaction is snapshot-consistent. Outside a
    /// transaction it is `None`: each autocommit read takes its own fresh
    /// snapshot, exactly as before.
    pub fn read_snapshot<E>(
        &mut self,
        acquire: impl FnOnce() -> Result<u64, E>,
    ) -> Result<Option<u64>, E> {
        if !self.active {
            return Ok(None);
        }
        if let Some(pinned) = self.snapshot_ts {
            return Ok(Some(pinned));
        }
        let acquired = acquire()?;
        self.snapshot_ts = Some(acquired);
        Ok(Some(acquired))
    }
}

#[cfg(test)]
mod tests {
    use super::{SessionTransaction, SessionTxnMode, TransactionMode};
    use std::cell::Cell;

    /// A test timestamp source that hands out increasing values and counts how
    /// many timestamps it issued.
    struct FakeClock {
        next: Cell<u64>,
        issued: Cell<usize>,
    }

    impl FakeClock {
        fn new() -> Self {
            Self {
                next: Cell::new(1000),
                issued: Cell::new(0),
            }
        }

        fn acquire(&self) -> Result<u64, ()> {
            self.issued.set(self.issued.get() + 1);
            let ts = self.next.get();
            self.next.set(ts + 1);
            Ok(ts)
        }
    }

    #[test]
    fn a_fresh_session_is_not_in_a_transaction_and_reads_take_fresh_snapshots() {
        let clock = FakeClock::new();
        let mut txn = SessionTransaction::new();
        assert!(!txn.is_active());
        // Outside a transaction, every read reports "no pinned snapshot" (take a
        // fresh one) and none is issued from the pin.
        assert_eq!(txn.read_snapshot(|| clock.acquire()), Ok(None));
        assert_eq!(txn.read_snapshot(|| clock.acquire()), Ok(None));
        assert_eq!(clock.issued.get(), 0);
    }

    #[test]
    fn every_read_in_a_transaction_shares_one_lazily_pinned_snapshot() {
        let clock = FakeClock::new();
        let mut txn = SessionTransaction::new();
        txn.begin(TransactionMode::Default);
        assert!(txn.is_active());
        // The first read pins the snapshot; later reads reuse it verbatim.
        let first = txn.read_snapshot(|| clock.acquire()).unwrap();
        let second = txn.read_snapshot(|| clock.acquire()).unwrap();
        assert_eq!(first, Some(1000));
        assert_eq!(second, Some(1000), "reads share one transaction snapshot");
        assert_eq!(
            clock.issued.get(),
            1,
            "the snapshot is acquired exactly once"
        );
    }

    #[test]
    fn ending_a_transaction_returns_to_fresh_per_read_snapshots() {
        let clock = FakeClock::new();
        let mut txn = SessionTransaction::new();
        txn.begin(TransactionMode::Default);
        txn.read_snapshot(|| clock.acquire()).unwrap();
        txn.end();
        assert!(!txn.is_active());
        assert_eq!(txn.read_snapshot(|| clock.acquire()), Ok(None));
    }

    #[test]
    fn re_beginning_a_transaction_repins_a_new_snapshot() {
        let clock = FakeClock::new();
        let mut txn = SessionTransaction::new();
        txn.begin(TransactionMode::Default);
        assert_eq!(txn.read_snapshot(|| clock.acquire()).unwrap(), Some(1000));
        // A second BEGIN implicitly ends the first transaction and starts a new
        // one, so the next read pins a fresh snapshot.
        txn.begin(TransactionMode::Default);
        assert_eq!(txn.read_snapshot(|| clock.acquire()).unwrap(), Some(1001));
        assert_eq!(clock.issued.get(), 2);
    }

    #[test]
    fn the_begin_keyword_decides_the_mode_and_ending_clears_it() {
        let mut txn = SessionTransaction::new();
        assert_eq!(txn.mode(), None);
        // No SET-able variable store here, so a bare BEGIN takes the registry
        // default of @@tidb_txn_mode.
        txn.begin(TransactionMode::Default);
        assert_eq!(txn.mode(), Some(SessionTxnMode::Pessimistic));
        txn.begin(TransactionMode::Optimistic);
        assert_eq!(txn.mode(), Some(SessionTxnMode::Optimistic));
        txn.begin(TransactionMode::Pessimistic);
        assert_eq!(txn.mode(), Some(SessionTxnMode::Pessimistic));
        txn.end();
        assert_eq!(txn.mode(), None);
    }

    #[test]
    fn a_snapshot_acquisition_failure_leaves_the_transaction_unpinned() {
        let mut txn = SessionTransaction::new();
        txn.begin(TransactionMode::Default);
        // A failed acquisition surfaces and pins nothing, so a later read retries.
        assert_eq!(
            txn.read_snapshot(|| Err::<u64, _>("pd unavailable")),
            Err("pd unavailable")
        );
        let clock = FakeClock::new();
        assert_eq!(txn.read_snapshot(|| clock.acquire()).unwrap(), Some(1000));
    }
}
