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

//! Connected source tests for the store-level begin family, the Go
//! `pkg/store/driver.tikvStore` shape: one opener per store handing out
//! transactions over the vendored engine.

use std::sync::Arc;

use tikv_client::mock::mocktikv::MockPdClient;
use tikv_client::testutils::{bootstrap_with_single_store, new_mock_tikv};
use tikv_client::{Timestamp, TimestampExt, TransactionOptions};

use tidb_txnkv::{
    Key, TikvCommitProtocol, TikvTransactionError, TikvTransactionOpener, TikvTransactionSource,
};

/// The in-process source, written against the same trait the production
/// `TikvClusterSource` implements. The crate's own `TikvInProcessSource` is
/// behind the `tikv-inprocess` feature; this local copy keeps the test honest
/// about the seam rather than about the feature wiring.
struct MockSource {
    pd: Arc<MockPdClient>,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl TikvTransactionSource for MockSource {
    type PdC = MockPdClient;

    fn current_timestamp(&self) -> Result<Timestamp, TikvTransactionError> {
        let pd = self.pd.clone();
        Ok(self
            .runtime
            .block_on(tikv_client::pd::PdClient::get_timestamp(pd))?)
    }

    fn begin(
        &self,
        timestamp: Timestamp,
        options: TransactionOptions,
    ) -> Result<tikv_client::transaction::Transaction<Self::PdC>, TikvTransactionError> {
        Ok(tikv_client::transaction::Transaction::new(
            timestamp,
            self.pd.clone(),
            options,
            tikv_client::request::Keyspace::Disable,
        ))
    }
}

fn opener() -> TikvTransactionOpener<MockSource> {
    let runtime = Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime builds"),
    );
    let (_client, cluster, pd) = new_mock_tikv("", None).expect("in-memory mock TiKV opens");
    bootstrap_with_single_store(&cluster);
    TikvTransactionOpener::new(
        MockSource {
            pd: Arc::new(pd),
            runtime: runtime.clone(),
        },
        runtime,
    )
}

fn k(value: &str) -> Key {
    Key::from(value.as_bytes().to_vec())
}

#[test]
fn the_begin_family_opens_transactions_that_commit_and_read() {
    let opener = opener();

    // A normal optimistic transaction commits.
    let mut txn = opener.begin().expect("begin spends a PD timestamp");
    assert!(txn.start_ts() > 0, "the oracle issued a real timestamp");
    txn.set(k("a"), b"1".to_vec()).unwrap();
    txn.commit().expect("2PC commits");

    // A read-only transaction sees it and never prewrites.
    let mut reader = opener.begin_read_only().expect("read-only begins");
    assert_eq!(reader.get(&k("a")).unwrap(), Some(b"1".to_vec()));
    reader.finish_without_writes().unwrap();

    // A pessimistic transaction locks at statement time and commits.
    let mut pessimistic = opener.begin_pessimistic().expect("pessimistic begins");
    pessimistic.lock_keys(&[k("a")]).unwrap();
    pessimistic.set(k("a"), b"2".to_vec()).unwrap();
    pessimistic.commit().expect("pessimistic commit succeeds");

    let mut reader = opener.begin_read_only().unwrap();
    assert_eq!(reader.get(&k("a")).unwrap(), Some(b"2".to_vec()));
    reader.finish_without_writes().unwrap();
}

#[test]
fn begin_at_reuses_an_already_spent_timestamp() {
    let opener = opener();

    // The statement's own read already spent this timestamp; opening a
    // writable transaction at it must not spend a second one, which is what
    // makes an implicit single-statement transaction one PD round trip.
    let spent = opener.current_timestamp().expect("a timestamp is spent");
    let mut txn = opener.begin_at(spent).expect("begin_at opens at that ts");
    assert_eq!(txn.start_ts(), spent);
    txn.set(k("implicit"), b"v".to_vec()).unwrap();
    txn.commit().expect("the implicit transaction commits");

    let mut reader = opener.begin_read_only().unwrap();
    assert_eq!(reader.get(&k("implicit")).unwrap(), Some(b"v".to_vec()));
    reader.finish_without_writes().unwrap();

    // A read-only transaction can pin the same spent timestamp.
    let mut pinned = opener.begin_read_only_at(spent).expect("read-only at ts");
    assert_eq!(pinned.start_ts(), spent);
    pinned.finish_without_writes().unwrap();
}

#[test]
fn the_commit_protocol_is_fixed_once_per_store() {
    // Async commit and 1PC are session variables resolved once per store, as
    // the coordinator facade resolved them; a transaction from a
    // so-configured opener commits through that protocol.
    let opener = opener().with_commit_protocol(TikvCommitProtocol {
        async_commit: true,
        one_pc: true,
    });

    let mut txn = opener.begin().unwrap();
    txn.set(k("fast"), b"path".to_vec()).unwrap();
    let commit_ts = txn.commit().expect("the 1PC/async-commit path commits");
    assert!(commit_ts.is_some());

    let mut reader = opener.begin_read_only().unwrap();
    assert_eq!(reader.get(&k("fast")).unwrap(), Some(b"path".to_vec()));
    reader.finish_without_writes().unwrap();

    // Two-phase-only is the default; that transaction is still writable, so
    // it has to be finished rather than dropped.
    let mut plain = opener.begin().unwrap();
    assert!(plain.start_ts() > 0);
    assert!(!plain.is_read_only_mode());
    plain.finish_without_writes().unwrap();
}

#[test]
fn timestamps_advance_across_transactions() {
    let opener = opener();
    let mut first_txn = opener.begin().unwrap();
    let mut second_txn = opener.begin().unwrap();
    let first = first_txn.start_ts();
    let second = second_txn.start_ts();
    first_txn.finish_without_writes().unwrap();
    second_txn.finish_without_writes().unwrap();
    assert!(
        second > first,
        "each begin spends a fresh oracle timestamp: {first} then {second}"
    );
    assert!(Timestamp::from_version(second).version() == second);
}

#[test]
fn the_pessimistic_surface_locks_reads_and_reports_locked_keys() {
    let opener = opener();

    let mut seed = opener.begin().unwrap();
    seed.set(k("row-a"), b"1".to_vec()).unwrap();
    seed.set(k("row-b"), b"2".to_vec()).unwrap();
    seed.commit().unwrap();

    let mut txn = opener.begin_pessimistic().unwrap();

    // A locking read returns the committed value and records the lock.
    assert_eq!(txn.get_for_update(&k("row-a")).unwrap(), Some(b"1".to_vec()));
    let locked = txn.locked_keys();
    assert!(
        locked.contains(&k("row-a")),
        "the locking read records its key: {locked:?}"
    );

    // A batch locking read returns the committed values for its keys.
    let pairs = txn.batch_get_for_update(&[k("row-a"), k("row-b")]).unwrap();
    assert_eq!(pairs.len(), 2);
    assert!(pairs.contains(&(k("row-b"), b"2".to_vec())));

    // The transaction then finishes through the same two-phase commit.
    txn.set(k("row-a"), b"locked-write".to_vec()).unwrap();
    txn.commit().expect("the pessimistic transaction commits");

    let mut reader = opener.begin_read_only().unwrap();
    assert_eq!(
        reader.get(&k("row-a")).unwrap(),
        Some(b"locked-write".to_vec())
    );
    reader.finish_without_writes().unwrap();
}

#[test]
fn statement_lock_scopes_retry_and_cancel_without_ending_the_transaction() {
    let opener = opener();

    let mut seed = opener.begin().unwrap();
    seed.set(k("stmt-row"), b"v1".to_vec()).unwrap();
    seed.set(k("other-row"), b"v1".to_vec()).unwrap();
    seed.commit().unwrap();

    let mut txn = opener.begin_pessimistic().unwrap();
    assert!(!txn.is_statement_locking());

    // A statement takes a lock inside its own scope, then is retried: the
    // transaction survives and can keep locking.
    txn.start_statement_locking();
    assert!(txn.is_statement_locking());
    txn.lock_keys(&[k("stmt-row")]).unwrap();
    txn.retry_statement_locking()
        .expect("the statement retries at a fresh for_update_ts");
    assert!(
        txn.is_statement_locking(),
        "a retry reopens the scope rather than closing it"
    );
    txn.lock_keys(&[k("stmt-row")]).unwrap();
    txn.done_statement_locking()
        .expect("the scope closes, keeping its locks for the transaction");
    assert!(!txn.is_statement_locking());

    // A second statement is cancelled: it releases its own locks only, and
    // the transaction still commits the first statement's work.
    txn.start_statement_locking();
    txn.lock_keys(&[k("other-row")]).unwrap();
    txn.cancel_statement_locking()
        .expect("the statement rolls back its own locks");
    assert!(!txn.is_statement_locking());

    txn.set(k("stmt-row"), b"v2".to_vec()).unwrap();
    txn.commit().expect("the transaction commits after all that");

    let mut reader = opener.begin_read_only().unwrap();
    assert_eq!(reader.get(&k("stmt-row")).unwrap(), Some(b"v2".to_vec()));
    assert_eq!(reader.get(&k("other-row")).unwrap(), Some(b"v1".to_vec()));
    reader.finish_without_writes().unwrap();
}
