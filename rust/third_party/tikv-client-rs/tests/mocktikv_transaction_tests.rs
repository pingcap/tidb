// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Downstream-crate behavioral checks for the public mock TiKV test facade.

#![cfg(feature = "internal-tests")]

use std::sync::Arc;

use tikv_client::testutils::{bootstrap_with_single_store, new_mock_tikv, Keyspace};
use tikv_client::{FlagsOp, Timestamp, TimestampExt, Transaction, TransactionOptions};

#[tokio::test]
async fn transactional_get_distinguishes_missing_from_empty_value() {
    let (_client, cluster, pd) = new_mock_tikv("", None).unwrap();
    bootstrap_with_single_store(&cluster);
    let mut transaction = Transaction::new(
        Timestamp::from_version(2),
        Arc::new(pd),
        TransactionOptions::new_optimistic().read_only(),
        Keyspace::Disable,
    );

    assert_eq!(transaction.get("missing".to_owned()).await.unwrap(), None);
}

#[tokio::test]
async fn transaction_commits_direct_staged_memdb_writes_without_a_drain() {
    let (_client, cluster, pd) = new_mock_tikv("", None).unwrap();
    bootstrap_with_single_store(&cluster);
    let pd = Arc::new(pd);
    let mut transaction = Transaction::new(
        Timestamp::from_version(2),
        pd.clone(),
        TransactionOptions::new_optimistic(),
        Keyspace::Disable,
    );

    let discarded = transaction.get_mem_buffer().staging();
    transaction
        .get_mem_buffer()
        .set(b"discarded", b"value")
        .unwrap();
    assert_eq!(
        transaction.get("discarded".to_owned()).await.unwrap(),
        Some(b"value".to_vec())
    );
    transaction.get_mem_buffer().cleanup(discarded);

    let released = transaction.get_mem_buffer().staging();
    transaction
        .get_mem_buffer()
        .set_with_flags(
            b"committed",
            b"value",
            &[
                FlagsOp::SetAssertNotExist,
                FlagsOp::SetNeedConstraintCheckInPrewrite,
            ],
        )
        .unwrap();
    transaction.get_mem_buffer().release(released);
    assert_eq!(transaction.len(), 1);
    assert_eq!(transaction.size(), b"committed".len() + b"value".len());
    assert_eq!(
        transaction.get("committed".to_owned()).await.unwrap(),
        Some(b"value".to_vec())
    );
    transaction.commit().await.unwrap();

    let mut reader = Transaction::new(
        Timestamp::from_version(u64::MAX),
        pd,
        TransactionOptions::new_optimistic().read_only(),
        Keyspace::Disable,
    );
    assert_eq!(reader.get("discarded".to_owned()).await.unwrap(), None);
    assert_eq!(
        reader.get("committed".to_owned()).await.unwrap(),
        Some(b"value".to_vec())
    );
}
