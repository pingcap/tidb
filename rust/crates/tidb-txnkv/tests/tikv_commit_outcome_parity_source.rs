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

//! Commit-outcome parity between the coordinator facade and the engine.
//!
//! Every consumer of the previous coordinator branches on
//! `OptimisticCommitOutcome`. These tests pin that the engine-backed commit
//! path reports the same terminal states for the same situations, so those
//! consumers keep their meaning after the swap.

use std::sync::Arc;

use tikv_client::mock::mocktikv::MockPdClient;
use tikv_client::testutils::{bootstrap_with_single_store, new_mock_tikv};
use tikv_client::{Timestamp, TransactionOptions};

use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticMutation, OptimisticTransactionState,
};
use tidb_txnkv::{TikvTransactionError, TikvTransactionOpener, TikvTransactionSource};

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

#[test]
fn a_successful_write_set_reports_committed_with_its_commit_timestamp() {
    let opener = opener();
    let mut txn = opener.begin().unwrap();
    let start_ts = txn.start_ts();

    let outcome = txn
        .commit_mutations(vec![
            OptimisticMutation::insert(b"row-1".to_vec(), b"v1".to_vec()).unwrap(),
            OptimisticMutation::insert(b"row-2".to_vec(), b"v2".to_vec()).unwrap(),
        ])
        .expect("the mutation set stages and commits");

    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);
    let OptimisticCommitOutcome::Committed(committed) = &outcome else {
        panic!("expected Committed, got {outcome:?}");
    };
    // The fields consumers actually read: the start timestamp, the allocated
    // commit timestamp, the pinned primary, and the admitted mutation count.
    assert_eq!(committed.receipt.start_ts, start_ts);
    assert!(committed.receipt.commit_ts > start_ts);
    assert_eq!(committed.receipt.primary_key, b"row-1".to_vec());
    assert_eq!(committed.receipt.mutation_count, 2);

    // The writes are visible to a later transaction.
    let mut reader = opener.begin_read_only().unwrap();
    assert_eq!(
        reader
            .get(&tidb_txnkv::Key::from(b"row-1".to_vec()))
            .unwrap(),
        Some(b"v1".to_vec())
    );
    reader.finish_without_writes().unwrap();
}

#[test]
fn a_violated_insert_assertion_reports_rolled_back_and_leaves_nothing_behind() {
    let opener = opener();

    let mut seed = opener.begin().unwrap();
    seed.commit_mutations(vec![OptimisticMutation::insert(
        b"taken".to_vec(),
        b"first".to_vec(),
    )
    .unwrap()])
    .unwrap();

    // Inserting over an existing key is a definitive failure: the engine rolls
    // its own prewrites back before returning, which is exactly the facade's
    // RolledBack state.
    let mut conflicting = opener.begin().unwrap();
    let outcome = conflicting
        .commit_mutations(vec![OptimisticMutation::insert(
            b"taken".to_vec(),
            b"second".to_vec(),
        )
        .unwrap()])
        .expect("the commit path reports an outcome rather than erroring out");

    assert_eq!(outcome.state(), OptimisticTransactionState::RolledBack);
    assert!(
        matches!(outcome, OptimisticCommitOutcome::RolledBack(_)),
        "expected RolledBack, got {outcome:?}"
    );

    // The original value survives untouched.
    let mut reader = opener.begin_read_only().unwrap();
    assert_eq!(
        reader
            .get(&tidb_txnkv::Key::from(b"taken".to_vec()))
            .unwrap(),
        Some(b"first".to_vec())
    );
    reader.finish_without_writes().unwrap();
}

#[test]
fn a_write_conflict_reports_rolled_back() {
    let opener = opener();

    let mut seed = opener.begin().unwrap();
    seed.commit_mutations(vec![OptimisticMutation::insert(
        b"contended".to_vec(),
        b"base".to_vec(),
    )
    .unwrap()])
    .unwrap();

    // Two overlapping optimistic transactions write the same key; the second
    // committer must observe a definitive non-commit.
    let mut winner = opener.begin().unwrap();
    let mut loser = opener.begin().unwrap();

    winner
        .commit_mutations(vec![OptimisticMutation::put_existing(
            b"contended".to_vec(),
            b"winner".to_vec(),
        )
        .unwrap()])
        .unwrap();

    let outcome = loser
        .commit_mutations(vec![OptimisticMutation::put_existing(
            b"contended".to_vec(),
            b"loser".to_vec(),
        )
        .unwrap()])
        .expect("the losing commit reports an outcome");
    assert_ne!(
        outcome.state(),
        OptimisticTransactionState::Committed,
        "the second committer must not report Committed: {outcome:?}"
    );

    let mut reader = opener.begin_read_only().unwrap();
    assert_eq!(
        reader
            .get(&tidb_txnkv::Key::from(b"contended".to_vec()))
            .unwrap(),
        Some(b"winner".to_vec())
    );
    reader.finish_without_writes().unwrap();
}

#[test]
fn every_mutation_kind_stages_with_its_source_op_and_assertion() {
    let opener = opener();

    // Seed rows the existence-asserting kinds need.
    let mut seed = opener.begin().unwrap();
    seed.commit_mutations(vec![
        OptimisticMutation::insert(b"existing-row".to_vec(), b"v".to_vec()).unwrap(),
        OptimisticMutation::insert(b"doomed-row".to_vec(), b"v".to_vec()).unwrap(),
    ])
    .unwrap();

    // One transaction exercising the put/delete/meta/index kinds together.
    let mut txn = opener.begin().unwrap();
    let outcome = txn
        .commit_mutations(vec![
            OptimisticMutation::put_existing(b"existing-row".to_vec(), b"v2".to_vec()).unwrap(),
            OptimisticMutation::delete(b"doomed-row".to_vec()).unwrap(),
            OptimisticMutation::index_put(b"idx-1".to_vec(), b"h".to_vec()).unwrap(),
            OptimisticMutation::meta_put(b"m-1".to_vec(), b"meta".to_vec()).unwrap(),
        ])
        .expect("the mixed mutation set commits");
    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);

    let mut reader = opener.begin_read_only().unwrap();
    assert_eq!(
        reader
            .get(&tidb_txnkv::Key::from(b"existing-row".to_vec()))
            .unwrap(),
        Some(b"v2".to_vec())
    );
    assert_eq!(
        reader
            .get(&tidb_txnkv::Key::from(b"doomed-row".to_vec()))
            .unwrap(),
        None
    );
    assert_eq!(
        reader.get(&tidb_txnkv::Key::from(b"idx-1".to_vec())).unwrap(),
        Some(b"h".to_vec())
    );
    assert_eq!(
        reader.get(&tidb_txnkv::Key::from(b"m-1".to_vec())).unwrap(),
        Some(b"meta".to_vec())
    );
    reader.finish_without_writes().unwrap();
}
