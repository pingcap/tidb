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
//! TiDB's staged transaction buffer paired with client-go two-phase commit,
//! run end-to-end over the vendored crate's in-process mocktikv cluster.

use std::sync::Arc;

use tikv_client::mock::mocktikv::MockPdClient;
use tikv_client::pd::PdClient;
use tikv_client::request::Keyspace;
use tikv_client::testutils::{bootstrap_with_single_store, new_mock_tikv};
use tikv_client::transaction::Transaction;
use tikv_client::TransactionOptions;

use tidb_txnkv::{FlagsOp, Key, TikvTransactionDriver};

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

async fn begin(pd: &Arc<MockPdClient>) -> Transaction<MockPdClient> {
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

fn k(value: &str) -> Key {
    Key::from(value.as_bytes().to_vec())
}

#[test]
fn staged_statements_commit_through_client_go_two_phase_commit() {
    runtime().block_on(async {
        let pd = mock_store();

        // Seed committed data through the raw engine path.
        let mut seed = begin(&pd).await;
        seed.put("seeded".to_owned(), "old".to_owned()).await.unwrap();
        seed.put("victim".to_owned(), "doomed".to_owned())
            .await
            .unwrap();
        seed.commit().await.unwrap();

        let mut driver = TikvTransactionDriver::new(begin(&pd).await);

        // Statement one commits: an insert and an overwrite survive.
        let statement = driver.staging();
        driver.set(k("fresh"), b"v1".to_vec()).unwrap();
        driver.set(k("seeded"), b"new".to_vec()).unwrap();
        driver.release(statement);

        // Statement two rolls back: its write never reaches the store.
        let statement = driver.staging();
        driver.set(k("aborted"), b"never".to_vec()).unwrap();
        driver.cleanup(statement);

        // A tombstone deletes committed data at commit.
        driver.delete(k("victim")).unwrap();

        let commit_timestamp = driver.commit().await.expect("2PC commits the staged buffer");
        assert!(commit_timestamp.is_some());

        // A fresh transaction observes exactly the surviving statements.
        let mut reader = begin(&pd).await;
        assert_eq!(
            reader.get("fresh".to_owned()).await.unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            reader.get("seeded".to_owned()).await.unwrap(),
            Some(b"new".to_vec())
        );
        assert_eq!(reader.get("aborted".to_owned()).await.unwrap(), None);
        assert_eq!(reader.get("victim".to_owned()).await.unwrap(), None);
        reader.rollback().await.unwrap();
    });
}

#[test]
fn union_reads_overlay_the_buffer_on_the_transaction_snapshot() {
    runtime().block_on(async {
        let pd = mock_store();

        let mut seed = begin(&pd).await;
        seed.put("base".to_owned(), "committed".to_owned())
            .await
            .unwrap();
        seed.commit().await.unwrap();

        let mut driver = TikvTransactionDriver::new(begin(&pd).await);

        // A miss in the buffer falls through to the snapshot.
        assert_eq!(
            driver.get(&k("base")).await.unwrap(),
            Some(b"committed".to_vec())
        );
        assert_eq!(driver.get(&k("absent")).await.unwrap(), None);

        // A buffered write overlays the committed value.
        driver.set(k("base"), b"local".to_vec()).unwrap();
        assert_eq!(driver.get(&k("base")).await.unwrap(), Some(b"local".to_vec()));

        // A buffered tombstone hides the committed value.
        driver.delete(k("base")).unwrap();
        assert_eq!(driver.get(&k("base")).await.unwrap(), None);

        driver.rollback().await.unwrap();
    });
}

#[test]
fn presume_key_not_exists_maps_to_insert_semantics() {
    runtime().block_on(async {
        let pd = mock_store();

        let mut seed = begin(&pd).await;
        seed.put("occupied".to_owned(), "existing".to_owned())
            .await
            .unwrap();
        seed.commit().await.unwrap();

        // Inserting over an existing key must fail, exactly like Go's lazy
        // existence check turning PresumeKeyNotExists into Op_Insert.
        let mut driver = TikvTransactionDriver::new(begin(&pd).await);
        driver
            .set_with_flags(
                k("occupied"),
                b"conflict".to_vec(),
                &[FlagsOp::SetPresumeKeyNotExists],
            )
            .unwrap();
        let conflict = driver.commit().await;
        assert!(
            conflict.is_err(),
            "insert over an existing key must fail, got {conflict:?}"
        );

        // The same insert on a free key commits.
        let mut driver = TikvTransactionDriver::new(begin(&pd).await);
        driver
            .set_with_flags(
                k("free"),
                b"inserted".to_vec(),
                &[FlagsOp::SetPresumeKeyNotExists],
            )
            .unwrap();
        driver.commit().await.expect("insert on a free key commits");

        let mut reader = begin(&pd).await;
        assert_eq!(
            reader.get("occupied".to_owned()).await.unwrap(),
            Some(b"existing".to_vec())
        );
        assert_eq!(
            reader.get("free".to_owned()).await.unwrap(),
            Some(b"inserted".to_vec())
        );
        reader.rollback().await.unwrap();
    });
}
