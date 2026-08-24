// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod common;

use tikv_client::BoundRange;
use tikv_client::Config;
use tikv_client::Key;
use tikv_client::KvPair;
use tikv_client::TransactionClient as Client;
use tikv_client::Value;

use crate::common::parse_args;

async fn puts(client: &Client, pairs: impl IntoIterator<Item = impl Into<KvPair>>) {
    let mut txn = client
        .begin_optimistic()
        .await
        .expect("Could not begin a transaction");
    for pair in pairs {
        let (key, value) = pair.into().into();
        txn.put(key, value).await.expect("Could not set key value");
    }
    txn.commit().await.expect("Could not commit transaction");
}

async fn get(client: &Client, key: Key) -> Option<Value> {
    let mut txn = client
        .begin_optimistic()
        .await
        .expect("Could not begin a transaction");
    let res = txn.get(key).await.expect("Could not get value");
    txn.commit()
        .await
        .expect("Committing read-only transaction should not fail");
    res
}

async fn key_exists(client: &Client, key: Key) -> bool {
    let mut txn = client
        .begin_optimistic()
        .await
        .expect("Could not begin a transaction");
    let res = txn
        .key_exists(key)
        .await
        .expect("Could not check key exists");
    txn.commit()
        .await
        .expect("Committing read-only transaction should not fail");
    res
}

async fn scan(client: &Client, range: impl Into<BoundRange>, limit: u32) {
    let mut txn = client
        .begin_optimistic()
        .await
        .expect("Could not begin a transaction");
    txn.scan(range, limit)
        .await
        .expect("Could not scan key-value pairs in range")
        .for_each(|pair| println!("{pair:?}"));
    txn.commit().await.expect("Could not commit transaction");
}

async fn dels(client: &Client, keys: impl IntoIterator<Item = Key>) {
    let mut txn = client
        .begin_optimistic()
        .await
        .expect("Could not begin a transaction");
    for key in keys {
        txn.delete(key).await.expect("Could not delete the key");
    }
    txn.commit().await.expect("Could not commit transaction");
}

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

    let txn = Client::new_with_config(args.pd, config)
        .await
        .expect("Could not connect to tikv");

    // set
    let key1: Key = b"key1".to_vec().into();
    let value1: Value = b"value1".to_vec();
    let key2: Key = b"key2".to_vec().into();
    let value2: Value = b"value2".to_vec();
    puts(&txn, vec![(key1, value1), (key2, value2)]).await;

    // get
    let key1: Key = b"key1".to_vec().into();
    let value1 = get(&txn, key1.clone()).await;
    println!("{:?}", (key1, value1));

    // check key exists
    let key1: Key = b"key1".to_vec().into();
    let key1_exists = key_exists(&txn, key1.clone()).await;
    let key2: Key = b"key_not_exist".to_vec().into();
    let key2_exists = key_exists(&txn, key2.clone()).await;
    println!(
        "check exists {:?}",
        vec![(key1, key1_exists), (key2, key2_exists)]
    );

    // scan
    let key1: Key = b"key1".to_vec().into();
    scan(&txn, key1.., 10).await;

    // delete
    let key1: Key = b"key1".to_vec().into();
    let key2: Key = b"key2".to_vec().into();
    dels(&txn, vec![key1, key2]).await;
}
