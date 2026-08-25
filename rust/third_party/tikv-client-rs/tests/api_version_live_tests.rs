// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg(feature = "integration-tests")]
#![allow(clippy::result_large_err)]

use std::env;
use tikv_client::{Config, Key, RawClient, Result, TransactionClient};

const ENV_PD_ADDRS: &str = "PD_ADDRS";
const ENV_API_VERSION: &str = "TIKV_API_VERSION";

fn pd_addrs() -> Vec<String> {
    env::var(ENV_PD_ADDRS)
        .unwrap_or_else(|_| "127.0.0.1:2379".to_owned())
        .split(',')
        .map(str::to_owned)
        .collect()
}

fn live_config() -> (u8, Config) {
    match env::var(ENV_API_VERSION).as_deref() {
        Ok("1") => (1, Config::default()),
        Ok("2") => (2, Config::default().with_default_keyspace()),
        Ok(version) => panic!("{ENV_API_VERSION} must be 1 or 2, got {version:?}"),
        Err(_) => panic!("set {ENV_API_VERSION}=1 or 2 for this live test"),
    }
}

fn tagged(prefix: &[u8], suffix: u8) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.push(suffix);
    key
}

fn suffixes(pairs: &[tikv_client::KvPair]) -> Vec<u8> {
    pairs
        .iter()
        .map(|pair| {
            let key: &[u8] = pair.key().into();
            *key.last().expect("test key is nonempty")
        })
        .collect()
}

/// Runs one codec-neutral contract against a server configured for the selected
/// API version. It is ignored so ordinary test runs never require a live
/// cluster; the repository parity receipt records explicit API-v1 and API-v2
/// invocations.
#[ignore = "requires a live TiKV/PD cluster and TIKV_API_VERSION"]
#[tokio::test]
async fn raw_and_transaction_contract_matches_client_go() -> Result<()> {
    let (api_version, config) = live_config();
    let prefix = format!("client-parity/api{api_version}/").into_bytes();
    let end = tagged(&prefix, 0xff);
    let raw_a = tagged(&prefix, b'a');
    let raw_c = tagged(&prefix, b'c');
    let missing = tagged(&prefix, b'm');

    let raw = RawClient::new_with_config(pd_addrs(), config.clone()).await?;
    raw.delete_range(prefix.clone()..end.clone()).await?;
    raw.batch_put(vec![
        (raw_a.clone(), b"raw-a".to_vec()),
        (raw_c.clone(), b"raw-c".to_vec()),
    ])
    .await?;
    assert_eq!(
        raw.batch_get(vec![raw_c.clone(), missing.clone(), raw_a.clone()])
            .await?,
        vec![Some(b"raw-c".to_vec()), None, Some(b"raw-a".to_vec())]
    );
    assert_eq!(
        suffixes(&raw.scan(prefix.clone()..end.clone(), 16).await?),
        vec![b'a', b'c']
    );
    assert_eq!(
        suffixes(&raw.scan_reverse(prefix.clone()..end.clone(), 16).await?),
        vec![b'c', b'a']
    );
    raw.delete_range(prefix.clone()..end.clone()).await?;
    assert_eq!(raw.get(raw_a).await?, None);

    let txn_client = TransactionClient::new_with_config(pd_addrs(), config).await?;
    let txn_b = tagged(&prefix, b'b');
    let txn_d = tagged(&prefix, b'd');
    let mut writer = txn_client.begin_optimistic().await?;
    writer.put(txn_b.clone(), b"txn-b".to_vec()).await?;
    writer.put(txn_d.clone(), b"txn-d".to_vec()).await?;
    writer.commit().await?;

    let mut reader = txn_client.begin_optimistic().await?;
    assert_eq!(reader.get(txn_b.clone()).await?, Some(b"txn-b".to_vec()));
    assert_eq!(reader.get(missing).await?, None);
    let scanned = reader
        .scan(prefix.clone()..end.clone(), 16)
        .await?
        .collect::<Vec<_>>();
    assert_eq!(suffixes(&scanned), vec![b'b', b'd']);
    reader.commit().await?;

    txn_client.delete_range(prefix.clone()..end).await?;
    println!(
        "client-parity api={api_version} raw=batch_get:c,-,a scan:a,c reverse:c,a txn=get:b,- scan:b,d"
    );
    Ok(())
}

#[test]
fn tagged_keys_keep_the_contract_range_disjoint() {
    let prefix = b"client-parity/api1/";
    assert!(Key::from(prefix.to_vec()) < Key::from(tagged(prefix, b'a')));
    assert!(Key::from(tagged(prefix, b'z')) < Key::from(tagged(prefix, 0xff)));
}
