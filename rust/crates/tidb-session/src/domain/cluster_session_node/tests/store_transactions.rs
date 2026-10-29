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

//! Tests of the store-transaction seam (`tidb_domain_core::cluster_transactions`) that
//! need the embedded unistore the server crate owns.

use std::sync::Arc;
use std::time::Duration;

use std::sync::atomic::{AtomicUsize, Ordering};
use tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener;
use tidb_store_driver_txn::session_commit_protocol::SessionTransactionOptions;
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_sessionctx::advisory_lock_state::{AdvisoryLockError, AdvisoryLockLease};
use tidb_kv::cluster_storage::{ClusterSnapshot, MutationBuffer, SnapshotPairs};
use tidb_kv::storage::StorageError;
use tidb_txnkv::Key;

use tidb_domain_core::cluster_transactions::*;

#[derive(Debug)]
struct BatchCountingSnapshot {
    get_calls: Arc<AtomicUsize>,
    batch_get_calls: Arc<AtomicUsize>,
}

impl ClusterSnapshot for BatchCountingSnapshot {
    fn get(&mut self, _key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.get_calls.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }

    fn batch_get(&mut self, keys: &[Key]) -> Result<SnapshotPairs, StorageError> {
        self.batch_get_calls.fetch_add(1, Ordering::Relaxed);
        Ok(keys
            .iter()
            .map(|key| (key.as_bytes().to_vec(), b"value".to_vec()))
            .collect())
    }

    fn scan(
        &mut self,
        _start: &Key,
        _end: &Key,
        _limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        Ok(Vec::new())
    }
}

struct BatchCountingTransactions {
    get_calls: Arc<AtomicUsize>,
    batch_get_calls: Arc<AtomicUsize>,
}

impl ClusterTransactions for BatchCountingTransactions {
    fn prepare_snapshot(
        &self,
        _resource_group: &str,
    ) -> Result<Box<dyn PendingClusterSnapshot>, String> {
        panic!("the test opens its snapshot directly")
    }

    fn open_snapshot(&self, _resource_group: &str) -> Result<Box<dyn ClusterSnapshot>, String> {
        Ok(Box::new(BatchCountingSnapshot {
            get_calls: Arc::clone(&self.get_calls),
            batch_get_calls: Arc::clone(&self.batch_get_calls),
        }))
    }

    fn open_max_ts_snapshot(
        &self,
        resource_group: &str,
    ) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.open_snapshot(resource_group)
    }

    fn commit(
        &self,
        _buffer: &MutationBuffer,
        _read_ts: Option<u64>,
        _resource_group: &str,
        _options: SessionTransactionOptions,
    ) -> Result<Option<CommittedTxnInfo>, SqlQueryError> {
        panic!("unused in batch-get forwarding test")
    }

    fn begin(
        &self,
        _pessimistic: bool,
        _fair_locking: bool,
        _resource_group: &str,
        _options: SessionTransactionOptions,
    ) -> Result<Box<dyn OpenClusterTransaction>, String> {
        panic!("unused in batch-get forwarding test")
    }

    fn prepare_autocommit_write(
        &self,
        _resource_group: &str,
        _pessimistic: bool,
        _fair_locking: bool,
        _options: SessionTransactionOptions,
    ) -> Result<Box<dyn PendingClusterTransaction>, String> {
        panic!("unused in batch-get forwarding test")
    }

    fn acquire_advisory_lock(
        &self,
        _name: &str,
        _timeout: Duration,
    ) -> Result<Box<dyn AdvisoryLockLease>, AdvisoryLockError> {
        panic!("unused in batch-get forwarding test")
    }

    fn is_advisory_lock_used(&self, _name: &str) -> bool {
        panic!("unused in batch-get forwarding test")
    }
}

#[test]
fn session_activation_retains_options_after_timestamp_future_failure() {
    use std::sync::atomic::AtomicBool;
    use tikv_client::oracle::{
        Oracle, OracleOption, OracleResult, ReadTimestampValidator, TimestampFuture,
    };

    /// The store's oracle, failing the next dispatched timestamp once:
    /// client-go's `SetOracle` with a test oracle.
    struct FailingOnce {
        inner: Arc<dyn Oracle>,
        fail: Arc<AtomicBool>,
    }
    struct Failed;
    #[async_trait::async_trait]
    impl TimestampFuture for Failed {
        async fn wait(&self) -> OracleResult<u64> {
            Err("injected timestamp wait failure".into())
        }
    }
    #[async_trait::async_trait]
    impl ReadTimestampValidator for FailingOnce {
        async fn validate_read_timestamp(
            &self,
            read_timestamp: u64,
            is_stale_read: bool,
            option: &OracleOption,
        ) -> OracleResult<()> {
            self.inner
                .validate_read_timestamp(read_timestamp, is_stale_read, option)
                .await
        }
    }
    #[async_trait::async_trait]
    impl Oracle for FailingOnce {
        async fn get_timestamp(&self, option: &OracleOption) -> OracleResult<u64> {
            self.inner.get_timestamp(option).await
        }
        fn get_timestamp_async(&self, option: &OracleOption) -> Box<dyn TimestampFuture> {
            if self.fail.swap(false, Ordering::AcqRel) {
                Box::new(Failed)
            } else {
                self.inner.get_timestamp_async(option)
            }
        }
        async fn get_low_resolution_timestamp(&self, option: &OracleOption) -> OracleResult<u64> {
            self.inner.get_low_resolution_timestamp(option).await
        }
        fn get_low_resolution_timestamp_async(
            &self,
            option: &OracleOption,
        ) -> Box<dyn TimestampFuture> {
            self.inner.get_low_resolution_timestamp_async(option)
        }
        fn set_low_resolution_timestamp_update_interval(
            &self,
            update_interval: Duration,
        ) -> OracleResult<()> {
            self.inner
                .set_low_resolution_timestamp_update_interval(update_interval)
        }
        async fn get_stale_timestamp(
            &self,
            transaction_scope: &str,
            previous_seconds: u64,
        ) -> OracleResult<u64> {
            self.inner
                .get_stale_timestamp(transaction_scope, previous_seconds)
                .await
        }
        fn is_expired(&self, lock_timestamp: u64, ttl: u64, option: &OracleOption) -> bool {
            self.inner.is_expired(lock_timestamp, ttl, option)
        }
        fn until_expired(&self, lock_timestamp: u64, ttl: u64, option: &OracleOption) -> i64 {
            self.inner.until_expired(lock_timestamp, ttl, option)
        }
        fn close(&self) {}
        async fn get_external_timestamp(&self) -> OracleResult<u64> {
            self.inner.get_external_timestamp().await
        }
        async fn set_external_timestamp(&self, timestamp: u64) -> OracleResult<()> {
            self.inner.set_external_timestamp(timestamp).await
        }
        async fn get_all_tso_keyspace_group_min_timestamp(&self) -> OracleResult<u64> {
            self.inner.get_all_tso_keyspace_group_min_timestamp().await
        }
    }
    let stack =
        tidb_ddl::embedded_store::in_process_stack(&tidb_pd_client::ClusterSecurity::plaintext())
            .unwrap();
    let fail = Arc::new(AtomicBool::new(false));
    let store = stack.tikv_store.store();
    store.set_oracle(Arc::new(FailingOnce {
        inner: store.get_oracle(),
        fail: fail.clone(),
    }));
    let opener = RealOptimisticTransactionOpener::from_capabilities(
        (*stack.tikv_store).clone(),
        Duration::from_secs(3),
    )
    .unwrap();
    let transactions = RealClusterTransactions::new(opener, Duration::from_secs(3));
    for pessimistic in [false, true] {
        fail.store(true, Ordering::Release);
        let options = SessionTransactionOptions {
            commit_protocol: tidb_store_driver_txn::transaction::CommitProtocol::two_phase_only(),
            assertion_level: tidb_proto::KvrpcAssertionLevel::Strict,
            kv_context: None,
        };
        let pending = transactions
            .prepare_autocommit_write("default", pessimistic, false, options)
            .unwrap();
        let transaction = pending.wait().unwrap();
        assert!(!fail.load(Ordering::Acquire));
        let buffer = MutationBuffer::new();
        transaction.bind_mutation_buffer(&buffer);
        buffer
            .stage_owned_batch([(
                Key::from_bytes(b"absent"),
                Some(b"value".to_vec()),
                false,
                tidb_txnkv::AssertionOp::AssertExist,
            )])
            .unwrap();
        let error = transaction
            .commit(&buffer)
            .expect_err("fallback must retain Strict");
        assert_eq!(error.code, 8141, "{error:?}");
    }
}

#[test]
fn deferred_snapshot_forwards_batch_get_without_point_read_fallback() {
    let get_calls = Arc::new(AtomicUsize::new(0));
    let batch_get_calls = Arc::new(AtomicUsize::new(0));
    let transactions = Arc::new(BatchCountingTransactions {
        get_calls: Arc::clone(&get_calls),
        batch_get_calls: Arc::clone(&batch_get_calls),
    });
    let mut snapshot = deferred_snapshot(
        transactions,
        StatementReadTs::default(),
        Arc::<str>::from("default"),
    );
    let keys = vec![Key::from_bytes(b"k1"), Key::from_bytes(b"k2")];

    let pairs = snapshot.batch_get(&keys).expect("batch get succeeds");

    assert_eq!(pairs.len(), 2);
    assert_eq!(batch_get_calls.load(Ordering::Relaxed), 1);
    assert_eq!(get_calls.load(Ordering::Relaxed), 0);
}
