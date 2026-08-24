#![cfg(feature = "integration-tests")]
#![allow(clippy::result_large_err)]

//! Tests for SyncTransactionClient
//!
//! These tests mirror the async TransactionClient tests but use the synchronous API.

mod common;
use common::*;
use serial_test::serial;
use std::collections::HashMap;
use tikv_client::transaction::Mutation;
use tikv_client::Config;
use tikv_client::Key;
use tikv_client::Result;
use tikv_client::SyncTransactionClient;
use tikv_client::TransactionOptions;
use tikv_client::Value;

/// Helper to initialize and return a sync client
fn sync_client() -> Result<SyncTransactionClient> {
    SyncTransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
}

#[test]
#[serial]
fn test_sync_client_outside_async_context() -> Result<()> {
    // This test verifies that SyncTransactionClient works correctly when called
    // from a synchronous context (no Tokio runtime active)
    let client = sync_client()?;

    // Should be able to call methods without error
    let _timestamp = client.current_timestamp()?;
    let mut txn = client.begin_optimistic()?;

    // Must rollback or commit the transaction to avoid panic on drop
    txn.rollback()?;

    Ok(())
}

#[test]
#[serial]
fn test_sync_client_new_inside_async_context() {
    // This test verifies that creating a SyncTransactionClient inside an async
    // context returns an error instead of panicking
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        let result = SyncTransactionClient::new_with_config(
            pd_addrs(),
            Config::default().with_default_keyspace(),
        );

        // Verify the error type is correct
        match result {
            Err(tikv_client::Error::NestedRuntimeError(_)) => {
                // Expected case - test passes
            }
            Err(other) => panic!("Expected NestedRuntimeError, got: {:?}", other),
            Ok(_) => panic!("Expected error but got Ok"),
        }
    });
}

#[test]
#[serial]
fn test_sync_client_methods_inside_async_context() -> Result<()> {
    // This test verifies that calling SyncTransactionClient methods inside an
    // async context returns an error instead of panicking.
    // The client is created outside the async context, but methods are called inside.

    let client = sync_client()?;
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        // All of these should return errors when called inside an async context
        let timestamp_result = client.current_timestamp();
        assert!(
            timestamp_result.is_err(),
            "current_timestamp should fail in async context"
        );

        let begin_result = client.begin_optimistic();
        assert!(
            begin_result.is_err(),
            "begin_optimistic should fail in async context"
        );

        let pessimistic_result = client.begin_pessimistic();
        assert!(
            pessimistic_result.is_err(),
            "begin_pessimistic should fail in async context"
        );

        // Verify the error type is correct
        match timestamp_result {
            Err(tikv_client::Error::NestedRuntimeError(_)) => {
                // Expected case - test passes
            }
            other => panic!("Expected NestedRuntimeError, got: {:?}", other),
        }
    });

    Ok(())
}

#[test]
#[serial]
fn txn_sync_get_timestamp() -> Result<()> {
    const COUNT: usize = 1 << 8;
    let client = sync_client()?;

    let mut versions = (0..COUNT)
        .map(|_| client.current_timestamp())
        .map(|res| res.map(|ts| (ts.physical << 18) + ts.logical))
        .collect::<Result<Vec<_>>>()?;

    // Each version should be unique
    versions.sort_unstable();
    versions.dedup();
    assert_eq!(versions.len(), COUNT);
    Ok(())
}

#[test]
#[serial]
fn txn_sync_crud() -> Result<()> {
    init_sync()?;

    let client = sync_client()?;
    let mut txn = client.begin_optimistic()?;

    // Get non-existent keys
    assert!(txn.get("foo".to_owned())?.is_none());

    // batch_get do not return non-existent entries
    assert_eq!(
        txn.batch_get(vec!["foo".to_owned(), "bar".to_owned()])?
            .count(),
        0
    );

    txn.put("foo".to_owned(), "bar".to_owned())?;
    txn.put("bar".to_owned(), "foo".to_owned())?;

    // Read buffered values
    assert_eq!(txn.get("foo".to_owned())?, Some("bar".to_owned().into()));

    let batch_get_res: HashMap<Key, Value> = txn
        .batch_get(vec!["foo".to_owned(), "bar".to_owned()])?
        .map(|pair| (pair.0, pair.1))
        .collect();

    assert_eq!(
        batch_get_res.get(&Key::from("foo".to_owned())),
        Some(Value::from("bar".to_owned())).as_ref()
    );
    assert_eq!(
        batch_get_res.get(&Key::from("bar".to_owned())),
        Some(Value::from("foo".to_owned())).as_ref()
    );

    txn.commit()?;

    // Verify the values were committed
    let mut txn = client.begin_optimistic()?;
    assert_eq!(txn.get("foo".to_owned())?, Some("bar".to_owned().into()));

    // Test delete
    txn.delete("foo".to_owned())?;
    assert!(txn.get("foo".to_owned())?.is_none());
    txn.commit()?;

    // Verify deletion
    let mut txn = client.begin_optimistic()?;
    assert!(txn.get("foo".to_owned())?.is_none());
    txn.rollback()?;

    Ok(())
}

#[test]
#[serial]
fn txn_sync_begin_pessimistic() -> Result<()> {
    init_sync()?;

    let client = sync_client()?;
    let mut txn = client.begin_pessimistic()?;

    txn.put("pessimistic_key".to_owned(), "value".to_owned())?;
    assert_eq!(
        txn.get("pessimistic_key".to_owned())?,
        Some("value".to_owned().into())
    );

    txn.commit()?;

    // Verify committed
    let mut txn = client.begin_optimistic()?;
    assert_eq!(
        txn.get("pessimistic_key".to_owned())?,
        Some("value".to_owned().into())
    );
    txn.rollback()?;

    Ok(())
}

#[test]
#[serial]
fn txn_sync_snapshot() -> Result<()> {
    init_sync()?;

    let client = sync_client()?;

    // Write some data
    let mut txn = client.begin_optimistic()?;
    txn.put("snapshot_key".to_owned(), "initial".to_owned())?;
    txn.commit()?;

    // Get snapshot at current timestamp
    let ts = client.current_timestamp()?;
    let mut snapshot = client.snapshot(ts, TransactionOptions::new_optimistic());

    // Verify snapshot reads initial value
    assert_eq!(
        snapshot.get("snapshot_key".to_owned())?,
        Some("initial".to_owned().into())
    );

    // Modify the value
    let mut txn = client.begin_optimistic()?;
    txn.put("snapshot_key".to_owned(), "updated".to_owned())?;
    txn.commit()?;

    // Snapshot still reads old value
    assert_eq!(
        snapshot.get("snapshot_key".to_owned())?,
        Some("initial".to_owned().into())
    );

    // New transaction reads updated value
    let mut txn = client.begin_optimistic()?;
    assert_eq!(
        txn.get("snapshot_key".to_owned())?,
        Some("updated".to_owned().into())
    );
    txn.rollback()?;

    Ok(())
}

#[test]
#[serial]
fn txn_sync_snapshot_batch_get() -> Result<()> {
    init_sync()?;

    let client = sync_client()?;

    // Write test data
    let mut txn = client.begin_optimistic()?;
    txn.put("snap_batch_k1".to_owned(), "v1".to_owned())?;
    txn.put("snap_batch_k2".to_owned(), "v2".to_owned())?;
    txn.put("snap_batch_k3".to_owned(), "v3".to_owned())?;
    txn.commit()?;

    // Get snapshot
    let ts = client.current_timestamp()?;
    let mut snapshot = client.snapshot(ts, TransactionOptions::new_optimistic());

    // Test batch_get
    let keys = vec![
        "snap_batch_k1".to_owned(),
        "snap_batch_k2".to_owned(),
        "snap_batch_k3".to_owned(),
        "snap_batch_k_nonexistent".to_owned(),
    ];
    let results: Vec<_> = snapshot.batch_get(keys)?.collect();

    assert_eq!(results.len(), 3);

    // Convert results to HashMap for order-agnostic verification
    let result_map: HashMap<Key, Value> = results
        .iter()
        .map(|kv| (kv.0.clone(), kv.1.clone()))
        .collect();

    // Verify all keys and values (order-agnostic)
    assert_eq!(
        result_map.get(&Key::from("snap_batch_k1".to_owned())),
        Some(&Value::from("v1".to_owned())),
        "snap_batch_k1 should have value v1"
    );
    assert_eq!(
        result_map.get(&Key::from("snap_batch_k2".to_owned())),
        Some(&Value::from("v2".to_owned())),
        "snap_batch_k2 should have value v2"
    );
    assert_eq!(
        result_map.get(&Key::from("snap_batch_k3".to_owned())),
        Some(&Value::from("v3".to_owned())),
        "snap_batch_k3 should have value v3"
    );

    // Verify non-existent key is not in results
    assert!(
        !result_map.contains_key(&Key::from("snap_batch_k_nonexistent".to_owned())),
        "Non-existent key should not be in results"
    );

    // Test edge case: empty key list
    let empty_results: Vec<_> = snapshot.batch_get(Vec::<String>::new())?.collect();
    assert_eq!(
        empty_results.len(),
        0,
        "Empty key list should return no results"
    );

    // Test edge case: all non-existent keys
    let nonexistent_keys = vec![
        "snap_batch_k_fake1".to_owned(),
        "snap_batch_k_fake2".to_owned(),
        "snap_batch_k_fake3".to_owned(),
    ];
    let nonexistent_results: Vec<_> = snapshot.batch_get(nonexistent_keys)?.collect();
    assert_eq!(
        nonexistent_results.len(),
        0,
        "All non-existent keys should return no results"
    );

    // Test edge case: duplicate keys
    // Ensure that providing duplicate keys yields one result per unique key.
    let duplicate_keys = vec![
        "snap_batch_k1".to_owned(),
        "snap_batch_k2".to_owned(),
        "snap_batch_k1".to_owned(), // Duplicate
        "snap_batch_k3".to_owned(),
        "snap_batch_k2".to_owned(), // Duplicate
    ];
    let duplicate_results: Vec<_> = snapshot.batch_get(duplicate_keys)?.collect();

    // Verify API behavior: duplicate keys in the input result in unique keys in the output
    let dup_result_map: HashMap<Key, Value> = duplicate_results
        .iter()
        .map(|kv| (kv.0.clone(), kv.1.clone()))
        .collect();
    assert_eq!(
        dup_result_map.len(),
        3,
        "API returns unique results: duplicate keys in input are deduplicated in output"
    );
    assert!(
        dup_result_map.contains_key(&Key::from("snap_batch_k1".to_owned())),
        "snap_batch_k1 should be present"
    );
    assert!(
        dup_result_map.contains_key(&Key::from("snap_batch_k2".to_owned())),
        "snap_batch_k2 should be present"
    );
    assert!(
        dup_result_map.contains_key(&Key::from("snap_batch_k3".to_owned())),
        "snap_batch_k3 should be present"
    );

    Ok(())
}

#[test]
#[serial]
fn txn_sync_snapshot_scan() -> Result<()> {
    init_sync()?;

    let client = sync_client()?;

    // Write test data
    let mut txn = client.begin_optimistic()?;
    txn.put("snap_scan_1".to_owned(), "value1".to_owned())?;
    txn.put("snap_scan_2".to_owned(), "value2".to_owned())?;
    txn.put("snap_scan_3".to_owned(), "value3".to_owned())?;
    txn.put("snap_scan_4".to_owned(), "value4".to_owned())?;
    txn.commit()?;

    // Get snapshot
    let ts = client.current_timestamp()?;
    let mut snapshot = client.snapshot(ts, TransactionOptions::new_optimistic());

    // Test scan
    let results: Vec<_> = snapshot
        .scan("snap_scan_1".to_owned().."snap_scan_4".to_owned(), 10)?
        .collect();

    // snap_scan_1, snap_scan_2, snap_scan_3 (snap_scan_4 is exclusive)
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, Key::from("snap_scan_1".to_owned()));
    assert_eq!(results[0].1, Value::from("value1".to_owned()));
    assert_eq!(results[1].0, Key::from("snap_scan_2".to_owned()));
    assert_eq!(results[1].1, Value::from("value2".to_owned()));
    assert_eq!(results[2].0, Key::from("snap_scan_3".to_owned()));
    assert_eq!(results[2].1, Value::from("value3".to_owned()));

    // Test scan with limit
    let results: Vec<_> = snapshot
        .scan("snap_scan_1".to_owned()..="snap_scan_4".to_owned(), 2)?
        .collect();

    assert_eq!(results.len(), 2); // Limited to 2

    Ok(())
}

#[test]
#[serial]
fn txn_sync_snapshot_scan_keys() -> Result<()> {
    init_sync()?;

    let client = sync_client()?;

    // Write test data
    let mut txn = client.begin_optimistic()?;
    txn.put("snap_keys_1".to_owned(), "v1".to_owned())?;
    txn.put("snap_keys_2".to_owned(), "v2".to_owned())?;
    txn.put("snap_keys_3".to_owned(), "v3".to_owned())?;
    txn.commit()?;

    // Get snapshot
    let ts = client.current_timestamp()?;
    let mut snapshot = client.snapshot(ts, TransactionOptions::new_optimistic());

    // Test scan_keys (only keys, no values)
    let keys: Vec<_> = snapshot
        .scan_keys("snap_keys_1".to_owned()..="snap_keys_3".to_owned(), 10)?
        .collect();

    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0], Key::from("snap_keys_1".to_owned()));
    assert_eq!(keys[1], Key::from("snap_keys_2".to_owned()));
    assert_eq!(keys[2], Key::from("snap_keys_3".to_owned()));

    Ok(())
}

#[test]
#[serial]
fn txn_sync_snapshot_scan_reverse() -> Result<()> {
    init_sync()?;

    let client = sync_client()?;

    // Write test data
    let mut txn = client.begin_optimistic()?;
    txn.put("snap_rev_1".to_owned(), "value1".to_owned())?;
    txn.put("snap_rev_2".to_owned(), "value2".to_owned())?;
    txn.put("snap_rev_3".to_owned(), "value3".to_owned())?;
    txn.put("snap_rev_4".to_owned(), "value4".to_owned())?;
    txn.commit()?;

    // Get snapshot
    let ts = client.current_timestamp()?;
    let mut snapshot = client.snapshot(ts, TransactionOptions::new_optimistic());

    // Test scan_reverse (reverse order)
    let results: Vec<_> = snapshot
        .scan_reverse("snap_rev_1".to_owned()..="snap_rev_4".to_owned(), 10)?
        .collect();

    assert_eq!(results.len(), 4);
    // Should be in reverse order
    assert_eq!(results[0].0, Key::from("snap_rev_4".to_owned()));
    assert_eq!(results[1].0, Key::from("snap_rev_3".to_owned()));
    assert_eq!(results[2].0, Key::from("snap_rev_2".to_owned()));
    assert_eq!(results[3].0, Key::from("snap_rev_1".to_owned()));

    Ok(())
}

#[test]
#[serial]
fn txn_sync_snapshot_scan_keys_reverse() -> Result<()> {
    init_sync()?;

    let client = sync_client()?;

    // Write test data
    let mut txn = client.begin_optimistic()?;
    txn.put("snap_krev_a".to_owned(), "v1".to_owned())?;
    txn.put("snap_krev_b".to_owned(), "v2".to_owned())?;
    txn.put("snap_krev_c".to_owned(), "v3".to_owned())?;
    txn.commit()?;

    // Get snapshot
    let ts = client.current_timestamp()?;
    let mut snapshot = client.snapshot(ts, TransactionOptions::new_optimistic());

    // Test scan_keys_reverse (keys only, in reverse order)
    let keys: Vec<_> = snapshot
        .scan_keys_reverse("snap_krev_a".to_owned()..="snap_krev_c".to_owned(), 10)?
        .collect();

    assert_eq!(keys.len(), 3);
    // Should be in reverse order
    assert_eq!(keys[0], Key::from("snap_krev_c".to_owned()));
    assert_eq!(keys[1], Key::from("snap_krev_b".to_owned()));
    assert_eq!(keys[2], Key::from("snap_krev_a".to_owned()));

    Ok(())
}

#[test]
#[serial]
fn txn_sync_snapshot_key_exists() -> Result<()> {
    init_sync()?;

    let client = sync_client()?;

    // Write test data
    let mut txn = client.begin_optimistic()?;
    txn.put("snap_exists_key".to_owned(), "value".to_owned())?;
    txn.commit()?;

    // Get snapshot
    let ts = client.current_timestamp()?;
    let mut snapshot = client.snapshot(ts, TransactionOptions::new_optimistic());

    // Test key_exists
    assert!(snapshot.key_exists("snap_exists_key".to_owned())?);
    assert!(!snapshot.key_exists("snap_nonexistent_key".to_owned())?);

    Ok(())
}

#[test]
#[serial]
fn txn_sync_begin_with_options() -> Result<()> {
    init_sync()?;

    let client = sync_client()?;
    let options = TransactionOptions::new_optimistic();
    let mut txn = client.begin_with_options(options)?;

    txn.put("options_key".to_owned(), "value".to_owned())?;
    txn.commit()?;

    // Verify
    let mut txn = client.begin_optimistic()?;
    assert_eq!(
        txn.get("options_key".to_owned())?,
        Some("value".to_owned().into())
    );
    txn.rollback()?;

    Ok(())
}

#[test]
#[serial]
fn txn_sync_rollback() -> Result<()> {
    init_sync()?;

    let client = sync_client()?;

    // Write initial value
    let mut txn = client.begin_optimistic()?;
    txn.put("rollback_key".to_owned(), "initial".to_owned())?;
    txn.commit()?;

    // Start new transaction and modify
    let mut txn = client.begin_optimistic()?;
    txn.put("rollback_key".to_owned(), "modified".to_owned())?;

    // Rollback instead of commit
    txn.rollback()?;

    // Verify value is still "initial"
    let mut txn = client.begin_optimistic()?;
    assert_eq!(
        txn.get("rollback_key".to_owned())?,
        Some("initial".to_owned().into())
    );
    txn.rollback()?;

    Ok(())
}

#[test]
#[serial]
fn txn_sync_clone_client() -> Result<()> {
    init_sync()?;

    let client1 = sync_client()?;
    let client2 = client1.clone();

    // Both clients should work independently
    let mut txn1 = client1.begin_optimistic()?;
    let mut txn2 = client2.begin_optimistic()?;

    txn1.put("clone_key1".to_owned(), "value1".to_owned())?;
    txn2.put("clone_key2".to_owned(), "value2".to_owned())?;

    txn1.commit()?;
    txn2.commit()?;

    // Verify both writes succeeded
    let mut txn = client1.begin_optimistic()?;
    assert_eq!(
        txn.get("clone_key1".to_owned())?,
        Some("value1".to_owned().into())
    );
    assert_eq!(
        txn.get("clone_key2".to_owned())?,
        Some("value2".to_owned().into())
    );
    txn.rollback()?;

    Ok(())
}

#[test]
#[serial]
fn txn_sync_scan() -> Result<()> {
    init_sync()?;
    let client = sync_client()?;

    // Setup: Write test data
    let mut txn = client.begin_optimistic()?;
    txn.put("key1".to_owned(), "value1".to_owned())?;
    txn.put("key2".to_owned(), "value2".to_owned())?;
    txn.put("key3".to_owned(), "value3".to_owned())?;
    txn.put("key4".to_owned(), "value4".to_owned())?;
    txn.commit()?;

    // Test scan in forward order
    let mut txn = client.begin_optimistic()?;
    let results: Vec<_> = txn
        .scan("key1".to_owned().."key4".to_owned(), 10)?
        .collect();

    assert_eq!(results.len(), 3); // key1, key2, key3 (key4 is exclusive)
    assert_eq!(results[0].0, Key::from("key1".to_owned()));
    assert_eq!(results[0].1, Value::from("value1".to_owned()));

    txn.rollback()?;
    Ok(())
}

#[test]
#[serial]
fn txn_sync_scan_keys() -> Result<()> {
    init_sync()?;
    let client = sync_client()?;

    // Setup
    let mut txn = client.begin_optimistic()?;
    txn.put("scan_k1".to_owned(), "v1".to_owned())?;
    txn.put("scan_k2".to_owned(), "v2".to_owned())?;
    txn.put("scan_k3".to_owned(), "v3".to_owned())?;
    txn.commit()?;

    // Test scan_keys (only keys, no values)
    let mut txn = client.begin_optimistic()?;
    let keys: Vec<_> = txn
        .scan_keys("scan_k1".to_owned()..="scan_k3".to_owned(), 10)?
        .collect();

    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0], Key::from("scan_k1".to_owned()));
    assert_eq!(keys[1], Key::from("scan_k2".to_owned()));
    assert_eq!(keys[2], Key::from("scan_k3".to_owned()));

    txn.rollback()?;
    Ok(())
}

#[test]
#[serial]
fn txn_sync_scan_reverse() -> Result<()> {
    init_sync()?;
    let client = sync_client()?;

    // Setup
    let mut txn = client.begin_optimistic()?;
    txn.put("rev1".to_owned(), "value1".to_owned())?;
    txn.put("rev2".to_owned(), "value2".to_owned())?;
    txn.put("rev3".to_owned(), "value3".to_owned())?;
    txn.commit()?;

    // Test scan_reverse - should return in reverse order
    let mut txn = client.begin_optimistic()?;
    let results: Vec<_> = txn
        .scan_reverse("rev1".to_owned()..="rev3".to_owned(), 10)?
        .collect();

    assert_eq!(results.len(), 3);
    // Reverse order: rev3, rev2, rev1
    assert_eq!(results[0].0, Key::from("rev3".to_owned()));
    assert_eq!(results[1].0, Key::from("rev2".to_owned()));
    assert_eq!(results[2].0, Key::from("rev1".to_owned()));

    txn.rollback()?;
    Ok(())
}

#[test]
#[serial]
fn txn_sync_scan_keys_reverse() -> Result<()> {
    init_sync()?;
    let client = sync_client()?;

    // Setup
    let mut txn = client.begin_optimistic()?;
    txn.put("revkey1".to_owned(), "v1".to_owned())?;
    txn.put("revkey2".to_owned(), "v2".to_owned())?;
    txn.put("revkey3".to_owned(), "v3".to_owned())?;
    txn.commit()?;

    // Test scan_keys_reverse
    let mut txn = client.begin_optimistic()?;
    let keys: Vec<_> = txn
        .scan_keys_reverse("revkey1".to_owned()..="revkey3".to_owned(), 10)?
        .collect();

    assert_eq!(keys.len(), 3);
    // Reverse order
    assert_eq!(keys[0], Key::from("revkey3".to_owned()));
    assert_eq!(keys[1], Key::from("revkey2".to_owned()));
    assert_eq!(keys[2], Key::from("revkey1".to_owned()));

    txn.rollback()?;
    Ok(())
}

#[test]
#[serial]
fn txn_sync_scan_with_limit() -> Result<()> {
    init_sync()?;
    let client = sync_client()?;

    // Setup: Write more data than we'll scan
    let mut txn = client.begin_optimistic()?;
    for i in 1..=10 {
        txn.put(format!("limit_key{:02}", i), format!("value{}", i))?;
    }
    txn.commit()?;

    // Test with limit
    let mut txn = client.begin_optimistic()?;
    let results: Vec<_> = txn
        .scan("limit_key00".to_owned().., 5)? // Limit to 5 results
        .collect();

    assert_eq!(results.len(), 5); // Should only get 5 results

    txn.rollback()?;
    Ok(())
}

#[test]
#[serial]
fn txn_sync_insert() -> Result<()> {
    init_sync()?;
    let client = sync_client()?;

    // Test insert on non-existent key
    let mut txn = client.begin_optimistic()?;
    txn.insert("insert_key".to_owned(), "value".to_owned())?;
    txn.commit()?;

    // Verify insert succeeded
    let mut txn = client.begin_optimistic()?;
    assert_eq!(
        txn.get("insert_key".to_owned())?,
        Some("value".to_owned().into())
    );
    txn.rollback()?;

    // Test insert on existing key should fail at commit
    let mut txn = client.begin_optimistic()?;
    txn.insert("insert_key".to_owned(), "new_value".to_owned())?; // This succeeds
    let result = txn.commit(); // But commit should fail
    assert!(result.is_err());

    Ok(())
}

#[test]
#[serial]
fn txn_sync_batch_mutate() -> Result<()> {
    init_sync()?;
    let client = sync_client()?;

    // Setup: Write initial data
    let mut txn = client.begin_optimistic()?;
    txn.put("mutate_key1".to_owned(), "initial1".to_owned())?;
    txn.put("mutate_key2".to_owned(), "initial2".to_owned())?;
    txn.put("mutate_key3".to_owned(), "initial3".to_owned())?;
    txn.commit()?;

    // Test batch_mutate with mix of puts and deletes
    let mut txn = client.begin_optimistic()?;
    let mutations = vec![
        Mutation::Put(
            Key::from("mutate_key1".to_owned()),
            Value::from("updated1".to_owned()),
        ),
        Mutation::Delete(Key::from("mutate_key2".to_owned())),
        Mutation::Put(
            Key::from("mutate_key4".to_owned()),
            Value::from("new4".to_owned()),
        ),
    ];
    txn.batch_mutate(mutations)?;
    txn.commit()?;

    // Verify mutations applied correctly
    let mut txn = client.begin_optimistic()?;
    assert_eq!(
        txn.get("mutate_key1".to_owned())?,
        Some("updated1".to_owned().into())
    );
    assert!(txn.get("mutate_key2".to_owned())?.is_none()); // Deleted
    assert_eq!(
        txn.get("mutate_key3".to_owned())?,
        Some("initial3".to_owned().into())
    ); // Unchanged
    assert_eq!(
        txn.get("mutate_key4".to_owned())?,
        Some("new4".to_owned().into())
    ); // New key
    txn.rollback()?;

    Ok(())
}

#[test]
#[serial]
fn txn_sync_lock_keys() -> Result<()> {
    init_sync()?;
    let client = sync_client()?;

    // Setup: Write initial data
    let mut txn = client.begin_optimistic()?;
    txn.put("lock_key1".to_owned(), "value1".to_owned())?;
    txn.put("lock_key2".to_owned(), "value2".to_owned())?;
    txn.commit()?;

    // Test lock_keys - should lock keys without modifying them
    let mut txn = client.begin_pessimistic()?;
    txn.lock_keys(vec!["lock_key1".to_owned(), "lock_key2".to_owned()])?;

    // Should still be able to read the values
    assert_eq!(
        txn.get("lock_key1".to_owned())?,
        Some("value1".to_owned().into())
    );
    assert_eq!(
        txn.get("lock_key2".to_owned())?,
        Some("value2".to_owned().into())
    );

    txn.commit()?;

    // Verify values unchanged
    let mut txn = client.begin_optimistic()?;
    assert_eq!(
        txn.get("lock_key1".to_owned())?,
        Some("value1".to_owned().into())
    );
    txn.rollback()?;

    Ok(())
}

#[test]
#[serial]
fn txn_sync_get_for_update() -> Result<()> {
    init_sync()?;
    let client = sync_client()?;

    // Setup: Write initial data
    let mut txn = client.begin_optimistic()?;
    txn.put("gfu_key".to_owned(), "initial".to_owned())?;
    txn.commit()?;

    // Test get_for_update - gets value and locks the key
    let mut txn = client.begin_pessimistic()?;
    let value = txn.get_for_update("gfu_key".to_owned())?;
    assert_eq!(value, Some("initial".to_owned().into()));

    // Update the value after locking
    txn.put("gfu_key".to_owned(), "updated".to_owned())?;
    txn.commit()?;

    // Verify update succeeded
    let mut txn = client.begin_optimistic()?;
    assert_eq!(
        txn.get("gfu_key".to_owned())?,
        Some("updated".to_owned().into())
    );
    txn.rollback()?;

    // Test get_for_update on non-existent key
    let mut txn = client.begin_pessimistic()?;
    let value = txn.get_for_update("nonexistent_gfu".to_owned())?;
    assert!(value.is_none());
    txn.rollback()?;

    Ok(())
}

#[test]
#[serial]
fn txn_sync_batch_get_for_update() -> Result<()> {
    init_sync()?;
    let client = sync_client()?;

    // Setup: Write initial data
    let mut txn = client.begin_optimistic()?;
    txn.put("bgfu_key1".to_owned(), "value1".to_owned())?;
    txn.put("bgfu_key2".to_owned(), "value2".to_owned())?;
    txn.put("bgfu_key3".to_owned(), "value3".to_owned())?;
    txn.commit()?;

    // Test batch_get_for_update
    let mut txn = client.begin_pessimistic()?;
    let results = txn.batch_get_for_update(vec![
        "bgfu_key1".to_owned(),
        "bgfu_key2".to_owned(),
        "bgfu_key3".to_owned(),
        "nonexistent".to_owned(), // Non-existent key
    ])?;

    // Should only return existing keys
    assert_eq!(results.len(), 3);
    let result_map: HashMap<Key, Value> = results.into_iter().map(|kv| (kv.0, kv.1)).collect();

    assert_eq!(
        result_map.get(&Key::from("bgfu_key1".to_owned())),
        Some(&Value::from("value1".to_owned()))
    );
    assert_eq!(
        result_map.get(&Key::from("bgfu_key2".to_owned())),
        Some(&Value::from("value2".to_owned()))
    );
    assert_eq!(
        result_map.get(&Key::from("bgfu_key3".to_owned())),
        Some(&Value::from("value3".to_owned()))
    );

    // Modify values after locking
    txn.put("bgfu_key1".to_owned(), "updated1".to_owned())?;
    txn.commit()?;

    // Verify update succeeded
    let mut txn = client.begin_optimistic()?;
    assert_eq!(
        txn.get("bgfu_key1".to_owned())?,
        Some("updated1".to_owned().into())
    );
    txn.rollback()?;

    Ok(())
}

#[test]
#[serial]
fn txn_sync_key_exists() -> Result<()> {
    init_sync()?;
    let client = sync_client()?;

    // Setup: Write some data
    let mut txn = client.begin_optimistic()?;
    txn.put("exists_key".to_owned(), "value".to_owned())?;
    txn.commit()?;

    // Test key_exists on existing key
    let mut txn = client.begin_optimistic()?;
    assert!(txn.key_exists("exists_key".to_owned())?);

    // Test key_exists on non-existent key
    assert!(!txn.key_exists("nonexistent_key".to_owned())?);

    txn.rollback()?;

    // Test key_exists with buffered operations
    let mut txn = client.begin_optimistic()?;
    txn.put("buffered_key".to_owned(), "value".to_owned())?;

    // Should see the buffered key as existing
    assert!(txn.key_exists("buffered_key".to_owned())?);

    // Test with buffered delete
    txn.delete("exists_key".to_owned())?;
    assert!(!txn.key_exists("exists_key".to_owned())?);

    txn.rollback()?;

    Ok(())
}

#[test]
#[serial]
fn txn_sync_send_heart_beat() -> Result<()> {
    init_sync()?;
    let client = sync_client()?;

    // Start a pessimistic transaction (heart beat is typically used with long-running transactions)
    let mut txn = client.begin_pessimistic()?;

    // Put some data
    txn.put("heartbeat_key".to_owned(), "value".to_owned())?;

    // Send heart beat to keep transaction alive
    let ttl = txn.send_heart_beat()?;

    // TTL should be a positive value (in milliseconds)
    assert!(ttl > 0);

    // Can send multiple heart beats
    let ttl2 = txn.send_heart_beat()?;
    assert!(ttl2 > 0);

    // Commit the transaction
    txn.commit()?;

    // Verify data was written
    let mut txn = client.begin_optimistic()?;
    assert_eq!(
        txn.get("heartbeat_key".to_owned())?,
        Some("value".to_owned().into())
    );
    txn.rollback()?;

    Ok(())
}
