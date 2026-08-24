// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod common;

use tikv_client::Config;
use tikv_client::Key;
use tikv_client::TransactionClient as Client;
use tikv_client::TransactionOptions;
use tikv_client::Value;

use crate::common::parse_args;

#[tokio::main]
async fn main() {
    env_logger::init();

    // You can try running this example by passing your pd endpoints
    // (and SSL options if necessary) through command line arguments.
    let args = parse_args("txn");

    // Create a configuration to use for the example.
    // Optionally encrypt the traffic.
    let config = if let (Some(ca), Some(cert), Some(key)) = (args.ca, args.cert, args.key) {
        Config::default().with_security(ca, cert, key)
    } else {
        Config::default()
    }
    // This example uses the default keyspace, so api-v2 must be enabled on the server.
    .with_default_keyspace();

    // init
    let client = Client::new_with_config(args.pd, config)
        .await
        .expect("Could not connect to tikv");

    let key1: Key = b"key01".to_vec().into();
    let value1: Value = b"value1".to_vec();
    let key2: Key = b"key02".to_vec().into();
    let value2: Value = b"value2".to_vec();
    let mut txn0 = client
        .begin_optimistic()
        .await
        .expect("Could not begin a transaction");
    for (key, value) in [(key1.clone(), value1), (key2, value2)] {
        txn0.put(key, value).await.expect("Could not set key value");
    }
    txn0.commit().await.expect("Could not commit");
    drop(txn0);
    let mut txn1 = client
        .begin_pessimistic()
        .await
        .expect("Could not begin a transaction");
    // lock the key
    let value = txn1
        .get_for_update(key1.clone())
        .await
        .expect("Could not get_and_lock the key");
    println!("{:?}", (&key1, value));
    {
        // another txn cannot write to the locked key
        let mut txn2 = client
            .begin_with_options(TransactionOptions::new_optimistic().no_resolve_locks())
            .await
            .expect("Could not begin a transaction");
        let value2: Value = b"value2".to_vec();
        txn2.put(key1.clone(), value2).await.unwrap();
        let result = txn2.commit().await;
        assert!(result.is_err());
    }
    // while this txn can still write it
    let value3: Value = b"value3".to_vec();
    txn1.put(key1.clone(), value3).await.unwrap();
    txn1.commit().await.unwrap();
    let mut txn3 = client
        .begin_optimistic()
        .await
        .expect("Could not begin a transaction");
    let result = txn3.get(key1.clone()).await.unwrap().unwrap();
    txn3.commit()
        .await
        .expect("Committing read-only transaction should not fail");
    println!("{:?}", (key1, result));
}
