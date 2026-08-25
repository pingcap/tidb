// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Downstream-crate behavioral checks for the public mock TiKV test facade.

#![cfg(feature = "internal-tests")]

use std::sync::Arc;

use tikv_client::testutils::{bootstrap_with_single_store, new_mock_tikv, Keyspace};
use tikv_client::{Timestamp, TimestampExt, Transaction, TransactionOptions};

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
