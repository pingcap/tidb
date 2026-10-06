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

// Global size settings must be isolated from parallel transaction fixtures.
use std::sync::Arc;
use tidb_txnkv::{Key, TikvMemBufferError, TikvTransactionDriver, TikvTransactionError};
use tikv_client::{PdClient, Transaction, TransactionOptions};

#[test]
fn driver_captures_configured_byte_limits_at_begin() {
    let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let (_, cluster, pd) = tikv_client::testutils::new_mock_tikv("", None).unwrap();
    tikv_client::testutils::bootstrap_with_single_store(&cluster);
    let pd = Arc::new(pd);
    let ts = runtime.block_on(pd.clone().get_timestamp()).unwrap();
    let entry = tidb_txnkv::txn_entry_size_limit();
    let total = tidb_txnkv::txn_total_size_limit();
    tidb_txnkv::set_txn_entry_size_limit(12);
    tidb_txnkv::set_txn_total_size_limit(20);
    let mut txn = TikvTransactionDriver::new(
        Transaction::new(
            ts,
            pd,
            TransactionOptions::new_optimistic(),
            tikv_client::request::Keyspace::Disable,
        ),
        runtime,
    );
    tidb_txnkv::set_txn_entry_size_limit(entry);
    tidb_txnkv::set_txn_total_size_limit(total);
    let error = txn
        .set(Key::from(b"12345678".to_vec()), b"12345".to_vec())
        .unwrap_err();
    assert!(
        matches!(error, TikvTransactionError::Buffer(TikvMemBufferError::Kv(ref e))
        if e.mysql_code() == tidb_txnkv::MysqlErrorCode::EntryTooLarge)
    );
    txn.set(Key::from(b"a".to_vec()), vec![1; 9]).unwrap();
    txn.set(Key::from(b"b".to_vec()), vec![2; 9]).unwrap();
    let error = txn.set(Key::from(b"c".to_vec()), vec![3]).unwrap_err();
    assert!(
        matches!(error, TikvTransactionError::Buffer(TikvMemBufferError::Kv(ref e))
        if e.mysql_code() == tidb_txnkv::MysqlErrorCode::TxnTooLarge)
    );
    txn.rollback().unwrap();
}
