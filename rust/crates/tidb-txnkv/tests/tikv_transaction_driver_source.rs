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

//! Connected source tests for the Go `pkg/store/driver/txn.tikvTxn` shape:
//! TiDB's typed staged-buffer contract paired with client-go two-phase
//! commit, run end-to-end over the vendored crate's in-process mocktikv
//! cluster.
//!
//! The driver stages into the transaction's own authoritative buffer, so
//! these tests also pin that there is exactly one buffer: what a statement
//! stages is what commit writes, and a rolled-back statement leaves nothing
//! behind for commit to find.

use std::sync::Arc;

use tikv_client::mock::mocktikv::MockPdClient;
use tikv_client::pd::PdClient;
use tikv_client::request::Keyspace;
use tikv_client::testutils::{bootstrap_with_single_store, new_mock_tikv};
use tikv_client::transaction::Transaction;
use tikv_client::TransactionOptions;

use tidb_txnkv::{FlagsOp, Key, TikvTransactionDriver};

fn runtime() -> Arc<tokio::runtime::Runtime> {
    Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime builds"),
    )
}

fn mock_store() -> Arc<MockPdClient> {
    let (_client, cluster, pd) = new_mock_tikv("", None).expect("in-memory mock TiKV opens");
    bootstrap_with_single_store(&cluster);
    Arc::new(pd)
}

/// Begins one optimistic transaction. The engine's `begin` is asynchronous,
/// so it runs on the runtime; every later driver call is blocking, exactly
/// how TiDB's synchronous transaction consumers use it.
fn begin(pd: &Arc<MockPdClient>, runtime: &Arc<tokio::runtime::Runtime>) -> Transaction<MockPdClient> {
    runtime.block_on(async {
        let timestamp = pd
            .clone()
            .get_timestamp()
            .await
            .expect("mock PD allocates a timestamp");
        Transaction::new(
            timestamp,
            pd.clone(),
            TransactionOptions::new_optimistic(),
            Keyspace::Disable,
        )
    })
}

fn driver(
    pd: &Arc<MockPdClient>,
    runtime: &Arc<tokio::runtime::Runtime>,
) -> TikvTransactionDriver<MockPdClient> {
    TikvTransactionDriver::new(begin(pd, runtime), runtime.clone())
}

fn k(value: &str) -> Key {
    Key::from(value.as_bytes().to_vec())
}

#[test]
fn staged_statements_commit_through_client_go_two_phase_commit() {
    let runtime = runtime();
    let pd = mock_store();

    // Seed committed data.
    let mut seed = driver(&pd, &runtime);
    seed.set(k("seeded"), b"old".to_vec()).unwrap();
    seed.set(k("victim"), b"doomed".to_vec()).unwrap();
    seed.commit().unwrap();

    let mut txn = driver(&pd, &runtime);

    // Statement one commits: an insert and an overwrite survive.
    let statement = txn.staging();
    txn.set(k("fresh"), b"v1".to_vec()).unwrap();
    txn.set(k("seeded"), b"new".to_vec()).unwrap();
    txn.release(statement);

    // Statement two rolls back: its write never reaches the store, and is
    // gone from the very buffer commit reads.
    let statement = txn.staging();
    txn.set(k("aborted"), b"never".to_vec()).unwrap();
    txn.cleanup(statement);
    assert_eq!(txn.get(&k("aborted")).unwrap(), None);

    // A tombstone deletes committed data at commit.
    txn.delete(k("victim")).unwrap();

    let commit_ts = txn.commit().expect("2PC commits the staged buffer");
    assert!(commit_ts.is_some());

    // A fresh transaction observes exactly the surviving statements.
    let mut reader = driver(&pd, &runtime);
    assert_eq!(reader.get(&k("fresh")).unwrap(), Some(b"v1".to_vec()));
    assert_eq!(reader.get(&k("seeded")).unwrap(), Some(b"new".to_vec()));
    assert_eq!(reader.get(&k("aborted")).unwrap(), None);
    assert_eq!(reader.get(&k("victim")).unwrap(), None);
    reader.rollback().unwrap();
}


#[test]
fn union_reads_overlay_the_buffer_on_the_transaction_snapshot() {
    let runtime = runtime();
    let pd = mock_store();

    let mut seed = driver(&pd, &runtime);
    seed.set(k("base"), b"committed".to_vec()).unwrap();
    seed.commit().unwrap();

    let mut txn = driver(&pd, &runtime);

    // A miss in the buffer falls through to the snapshot.
    assert_eq!(txn.get(&k("base")).unwrap(), Some(b"committed".to_vec()));
    assert_eq!(txn.get(&k("absent")).unwrap(), None);

    // A buffered write overlays the committed value.
    txn.set(k("base"), b"local".to_vec()).unwrap();
    assert_eq!(txn.get(&k("base")).unwrap(), Some(b"local".to_vec()));

    // A buffered tombstone hides the committed value.
    txn.delete(k("base")).unwrap();
    assert_eq!(txn.get(&k("base")).unwrap(), None);

    txn.rollback().unwrap();
}

#[test]
fn presume_key_not_exists_maps_to_insert_semantics() {
    let runtime = runtime();
    let pd = mock_store();

    let mut seed = driver(&pd, &runtime);
    seed.set(k("occupied"), b"existing".to_vec()).unwrap();
    seed.commit().unwrap();

    // Inserting over an existing key must fail, exactly like Go's lazy
    // existence check turning PresumeKeyNotExists into Op_Insert.
    let mut conflicting = driver(&pd, &runtime);
    conflicting
        .set_with_flags(
            k("occupied"),
            b"conflict".to_vec(),
            &[FlagsOp::SetPresumeKeyNotExists],
        )
        .unwrap();
    let conflict = conflicting.commit();
    assert!(
        conflict.is_err(),
        "insert over an existing key must fail, got {conflict:?}"
    );

    // The same insert on a free key commits.
    let mut inserting = driver(&pd, &runtime);
    inserting
        .set_with_flags(
            k("free"),
            b"inserted".to_vec(),
            &[FlagsOp::SetPresumeKeyNotExists],
        )
        .unwrap();
    inserting.commit().expect("insert on a free key commits");

    let mut reader = driver(&pd, &runtime);
    assert_eq!(
        reader.get(&k("occupied")).unwrap(),
        Some(b"existing".to_vec())
    );
    assert_eq!(reader.get(&k("free")).unwrap(), Some(b"inserted".to_vec()));
    reader.rollback().unwrap();
}

#[test]
fn the_staged_buffer_is_the_buffer_commit_reads() {
    let runtime = runtime();
    let pd = mock_store();
    let mut txn = driver(&pd, &runtime);

    // Staging through TiDB's typed buffer view is visible to the engine's own
    // size/length accounting: there is one buffer, not two.
    assert!(txn.is_empty());
    txn.set(k("a"), b"1".to_vec()).unwrap();
    txn.set(k("b"), b"2".to_vec()).unwrap();
    assert_eq!(txn.len(), 2);
    assert!(txn.size() > 0);

    // Flags applied through the typed view survive on the same buffer.
    txn
        .set_with_flags(k("c"), b"3".to_vec(), &[FlagsOp::SetNeedLocked])
        .unwrap();
    assert!(txn.get_flags(&k("c")).unwrap().has_need_locked());
    assert_eq!(txn.len(), 3);

    txn.commit().expect("the staged keys commit");

    let mut reader = driver(&pd, &runtime);
    assert_eq!(reader.get(&k("a")).unwrap(), Some(b"1".to_vec()));
    assert_eq!(reader.get(&k("c")).unwrap(), Some(b"3".to_vec()));
    reader.rollback().unwrap();
}

#[test]
fn pessimistic_locks_are_acquired_at_statement_time() {
    let runtime = runtime();
    let pd = mock_store();

    let mut seed = driver(&pd, &runtime);
    seed.set(k("row"), b"v1".to_vec()).unwrap();
    seed.commit().unwrap();

    let timestamp = runtime.block_on(pd.clone().get_timestamp()).unwrap();
    let pessimistic = Transaction::new(
        timestamp,
        pd.clone(),
        TransactionOptions::new_pessimistic(),
        Keyspace::Disable,
    );
    let mut txn = TikvTransactionDriver::new(pessimistic, runtime.clone());

    // Statement-time lock acquisition, then a write under that lock.
    txn.lock_keys(&[k("row")]).unwrap();
    txn.set(k("row"), b"v2".to_vec()).unwrap();
    txn.commit().expect("pessimistic commit succeeds");

    let mut reader = driver(&pd, &runtime);
    assert_eq!(reader.get(&k("row")).unwrap(), Some(b"v2".to_vec()));
    reader.rollback().unwrap();
}
