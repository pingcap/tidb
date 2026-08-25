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

//! Differential gate for the vendored client-rust transaction stack.
//!
//! Go TiDB trusts client-go's `twoPhaseCommitter` because
//! `tikv.NewTestTiKVStore` runs the complete stack over the in-process
//! `mockstore/mocktikv` cluster. This is the same gate for this workspace:
//! the vendored crate's full optimistic-transaction path — begin at a PD
//! timestamp, buffer writes, two-phase prewrite/commit through region
//! routing, snapshot reads, rollback, and first-committer-wins conflicts —
//! executed end-to-end over its transcreated `mocktikv` MVCC store, entirely
//! inside this workspace. This is the foundation the coordinator swap
//! (ExecPlan Phase 2) builds on: it proves the engine TiDB would delegate
//! 2PC to actually works here, not just in upstream's own repository.

use std::sync::Arc;

use tikv_client::mock::mocktikv::MockPdClient;
use tikv_client::pd::PdClient;
use tikv_client::request::Keyspace;
use tikv_client::testutils::{bootstrap_with_single_store, new_mock_tikv};
use tikv_client::transaction::Transaction;
use tikv_client::TransactionOptions;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime builds")
}

fn mock_store() -> Arc<MockPdClient> {
    let (_client, cluster, pd) = new_mock_tikv("", None).expect("in-memory mock TiKV opens");
    bootstrap_with_single_store(&cluster);
    Arc::new(pd)
}

async fn begin_optimistic(pd: &Arc<MockPdClient>) -> Transaction<MockPdClient> {
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
}

#[test]
fn optimistic_two_phase_commit_round_trips_over_mocktikv() {
    runtime().block_on(async {
        let pd = mock_store();

        let mut txn = begin_optimistic(&pd).await;
        txn.put("k1".to_owned(), "v1".to_owned()).await.unwrap();
        txn.put("k2".to_owned(), "v2".to_owned()).await.unwrap();
        // Buffered reads observe the transaction's own writes before commit.
        assert_eq!(
            txn.get("k1".to_owned()).await.unwrap(),
            Some(b"v1".to_vec())
        );
        let commit_timestamp = txn.commit().await.expect("2PC commits over mocktikv");
        assert!(commit_timestamp.is_some(), "a committed write reports its commit timestamp");

        // A later transaction snapshot-reads both committed keys.
        let mut reader = begin_optimistic(&pd).await;
        assert_eq!(
            reader.get("k1".to_owned()).await.unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            reader.get("k2".to_owned()).await.unwrap(),
            Some(b"v2".to_vec())
        );
        assert_eq!(reader.get("absent".to_owned()).await.unwrap(), None);
        reader.rollback().await.unwrap();
    });
}

#[test]
fn rolled_back_writes_never_become_visible() {
    runtime().block_on(async {
        let pd = mock_store();

        let mut txn = begin_optimistic(&pd).await;
        txn.put("doomed".to_owned(), "value".to_owned())
            .await
            .unwrap();
        txn.rollback().await.unwrap();

        let mut reader = begin_optimistic(&pd).await;
        assert_eq!(reader.get("doomed".to_owned()).await.unwrap(), None);
        reader.rollback().await.unwrap();
    });
}

#[test]
fn first_committer_wins_on_a_write_conflict() {
    runtime().block_on(async {
        let pd = mock_store();

        // Seed a committed baseline value.
        let mut seed = begin_optimistic(&pd).await;
        seed.put("contended".to_owned(), "base".to_owned())
            .await
            .unwrap();
        seed.commit().await.unwrap();

        // Two overlapping optimistic transactions write the same key.
        let mut winner = begin_optimistic(&pd).await;
        let mut loser = begin_optimistic(&pd).await;
        winner
            .put("contended".to_owned(), "winner".to_owned())
            .await
            .unwrap();
        loser
            .put("contended".to_owned(), "loser".to_owned())
            .await
            .unwrap();

        winner.commit().await.expect("first committer succeeds");
        let conflict = loser.commit().await;
        assert!(
            conflict.is_err(),
            "the second optimistic committer must observe the conflict, got {conflict:?}"
        );
        // Failed optimistic commits leave the winner's value in place.
        let mut reader = begin_optimistic(&pd).await;
        assert_eq!(
            reader.get("contended".to_owned()).await.unwrap(),
            Some(b"winner".to_vec())
        );
        reader.rollback().await.unwrap();
    });
}

#[test]
fn pessimistic_lock_write_commit_and_unlock_after_rollback() {
    runtime().block_on(async {
        let pd = mock_store();

        // A pessimistic transaction locks at statement time, writes, commits.
        let ts = pd.clone().get_timestamp().await.unwrap();
        let mut txn = Transaction::new(
            ts,
            pd.clone(),
            TransactionOptions::new_pessimistic(),
            Keyspace::Disable,
        );
        assert_eq!(txn.get_for_update("locked".to_owned()).await.unwrap(), None);
        txn.put("locked".to_owned(), "v1".to_owned()).await.unwrap();
        txn.commit().await.expect("pessimistic commit succeeds");

        let mut reader = begin_optimistic(&pd).await;
        assert_eq!(
            reader.get("locked".to_owned()).await.unwrap(),
            Some(b"v1".to_vec())
        );
        reader.rollback().await.unwrap();

        // A rolled-back pessimistic transaction releases its locks: a later
        // pessimistic transaction locks and commits the same key.
        let ts = pd.clone().get_timestamp().await.unwrap();
        let mut aborted = Transaction::new(
            ts,
            pd.clone(),
            TransactionOptions::new_pessimistic(),
            Keyspace::Disable,
        );
        aborted
            .get_for_update("locked".to_owned())
            .await
            .unwrap();
        aborted.rollback().await.expect("pessimistic rollback releases locks");

        let ts = pd.clone().get_timestamp().await.unwrap();
        let mut successor = Transaction::new(
            ts,
            pd.clone(),
            TransactionOptions::new_pessimistic(),
            Keyspace::Disable,
        );
        assert_eq!(
            successor.get_for_update("locked".to_owned()).await.unwrap(),
            Some(b"v1".to_vec())
        );
        successor
            .put("locked".to_owned(), "v2".to_owned())
            .await
            .unwrap();
        successor.commit().await.expect("successor lock acquisition and commit succeed");
    });
}

#[test]
fn snapshot_scans_return_ordered_committed_pairs() {
    runtime().block_on(async {
        let pd = mock_store();

        let mut seed = begin_optimistic(&pd).await;
        for (key, value) in [("sa", "1"), ("sb", "2"), ("sc", "3"), ("sd", "4")] {
            seed.put(key.to_owned(), value.to_owned()).await.unwrap();
        }
        seed.commit().await.unwrap();

        let mut reader = begin_optimistic(&pd).await;
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = reader
            .scan("sa".to_owned().."sd".to_owned(), 100)
            .await
            .unwrap()
            .map(|pair| (pair.0.into(), pair.1))
            .collect();
        assert_eq!(
            pairs,
            vec![
                (b"sa".to_vec(), b"1".to_vec()),
                (b"sb".to_vec(), b"2".to_vec()),
                (b"sc".to_vec(), b"3".to_vec()),
            ]
        );
        reader.rollback().await.unwrap();
    });
}
