#![cfg(feature = "integration-tests")]
#![allow(clippy::result_large_err)]

//! # Naming convention
//!
//! Test names should begin with one of the following:
//! 1. txn_
//! 2. raw_
//!
//! We make use of the convention to control the order of tests in CI, to allow
//! transactional and raw tests to coexist, since transactional requests have
//! requirements on the region boundaries.

mod common;
use common::*;
use futures::prelude::*;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use rand::Rng;
use serial_test::serial;
use std::collections::HashMap;
use std::iter;
use tikv_client::backoff::DEFAULT_REGION_BACKOFF;
use tikv_client::transaction::HeartbeatOption;
use tikv_client::transaction::Mutation;
use tikv_client::Config;
use tikv_client::Error;
use tikv_client::Key;
use tikv_client::KvPair;
use tikv_client::RawClient;
use tikv_client::Result;
use tikv_client::TransactionClient;
use tikv_client::TransactionOptions;
use tikv_client::Value;
use tikv_client::{Backoff, BoundRange, RetryOptions, Transaction};

// Parameters used in test
const NUM_PEOPLE: u32 = 100;
const NUM_TRNASFER: u32 = 100;

#[tokio::test]
#[serial]
async fn txn_get_timestamp() -> Result<()> {
    const COUNT: usize = 1 << 8; // use a small number to make test fast
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    let mut versions = future::join_all((0..COUNT).map(|_| client.current_timestamp()))
        .await
        .into_iter()
        .map(|res| res.map(|ts| (ts.physical << 18) + ts.logical))
        .collect::<Result<Vec<_>>>()?;

    // Each version should be unique
    versions.sort_unstable();
    versions.dedup();
    assert_eq!(versions.len(), COUNT);
    Ok(())
}

// Tests transactional get, put, delete, batch_get
#[tokio::test]
#[serial]
async fn txn_crud() -> Result<()> {
    init().await?;

    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    let mut txn = client.begin_optimistic().await?;

    // Get non-existent keys
    assert!(txn.get("foo".to_owned()).await?.is_none());

    // batch_get do not return non-existent entries
    assert_eq!(
        txn.batch_get(vec!["foo".to_owned(), "bar".to_owned()])
            .await?
            .count(),
        0
    );

    txn.put("foo".to_owned(), "bar".to_owned()).await?;
    txn.put("bar".to_owned(), "foo".to_owned()).await?;
    // Read buffered values
    assert_eq!(
        txn.get("foo".to_owned()).await?,
        Some("bar".to_owned().into())
    );
    let batch_get_res: HashMap<Key, Value> = txn
        .batch_get(vec!["foo".to_owned(), "bar".to_owned()])
        .await?
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
    txn.commit().await?;

    // Read from TiKV then update and delete
    let mut txn = client.begin_optimistic().await?;
    assert_eq!(
        txn.get("foo".to_owned()).await?,
        Some("bar".to_owned().into())
    );
    let batch_get_res: HashMap<Key, Value> = txn
        .batch_get(vec!["foo".to_owned(), "bar".to_owned()])
        .await?
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
    txn.put("foo".to_owned(), "foo".to_owned()).await?;
    txn.delete("bar".to_owned()).await?;
    txn.commit().await?;

    // Read again from TiKV
    let mut snapshot = client.snapshot(
        client.current_timestamp().await?,
        // TODO needed because pessimistic does not check locks (#235)
        TransactionOptions::new_optimistic(),
    );
    let batch_get_res: HashMap<Key, Value> = snapshot
        .batch_get(vec!["foo".to_owned(), "bar".to_owned()])
        .await?
        .map(|pair| (pair.0, pair.1))
        .collect();
    assert_eq!(
        batch_get_res.get(&Key::from("foo".to_owned())),
        Some(Value::from("foo".to_owned())).as_ref()
    );
    assert_eq!(batch_get_res.get(&Key::from("bar".to_owned())), None);
    Ok(())
}

// Tests transactional insert and delete-your-writes cases
#[tokio::test]
#[serial]
async fn txn_insert_duplicate_keys() -> Result<()> {
    init().await?;

    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    // Initialize TiKV store with {foo => bar}
    let mut txn = client.begin_optimistic().await?;
    txn.put("foo".to_owned(), "bar".to_owned()).await?;
    txn.commit().await?;
    // Try insert foo again
    let mut txn = client.begin_optimistic().await?;
    txn.insert("foo".to_owned(), "foo".to_owned()).await?;
    assert!(txn.commit().await.is_err());

    // Delete-your-writes
    let mut txn = client.begin_optimistic().await?;
    txn.insert("foo".to_owned(), "foo".to_owned()).await?;
    txn.delete("foo".to_owned()).await?;
    assert!(txn.commit().await.is_err());

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_pessimistic() -> Result<()> {
    init().await?;

    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    let mut txn = client.begin_pessimistic().await?;
    txn.put("foo".to_owned(), "foo".to_owned()).await.unwrap();

    let ttl = txn.send_heart_beat().await.unwrap();
    assert!(ttl > 0);

    txn.commit().await.unwrap();

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_split_batch() -> Result<()> {
    init().await?;

    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    let mut txn = client.begin_optimistic().await?;
    let mut rng = thread_rng();

    // testing with raft-entry-max-size = "1MB"
    let keys_count: usize = 1000;
    let val_len = 15000;

    let values: Vec<_> = (0..keys_count)
        .map(|_| (0..val_len).map(|_| rng.gen::<u8>()).collect::<Vec<_>>())
        .collect();

    for (i, value) in values.iter().enumerate() {
        let key = Key::from(i.to_be_bytes().to_vec());
        txn.put(key, value.clone()).await?;
    }

    txn.commit().await?;

    let mut snapshot = client.snapshot(
        client.current_timestamp().await?,
        TransactionOptions::new_optimistic(),
    );

    for (i, value) in values.iter().enumerate() {
        let key = Key::from(i.to_be_bytes().to_vec());
        let from_snapshot = snapshot.get(key).await?.unwrap();
        assert_eq!(from_snapshot, value.clone());
    }

    Ok(())
}

/// bank transfer mainly tests raw put and get
#[tokio::test]
#[serial]
async fn raw_bank_transfer() -> Result<()> {
    init().await?;
    let client =
        RawClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace()).await?;
    let mut rng = thread_rng();

    let people = gen_u32_keys(NUM_PEOPLE, &mut rng);
    let mut sum: u32 = 0;
    for person in &people {
        let init = rng.gen::<u8>() as u32;
        sum += init;
        client
            .put(person.clone(), init.to_be_bytes().to_vec())
            .await?;
    }

    // transfer
    for _ in 0..NUM_TRNASFER {
        let chosen_people = people.iter().choose_multiple(&mut rng, 2);
        let alice = chosen_people[0];
        let mut alice_balance = get_u32(&client, alice.clone()).await?;
        let bob = chosen_people[1];
        let mut bob_balance = get_u32(&client, bob.clone()).await?;
        if alice_balance == 0 {
            continue;
        }
        let transfer = rng.gen_range(0..alice_balance);
        alice_balance -= transfer;
        bob_balance += transfer;
        client
            .put(alice.clone(), alice_balance.to_be_bytes().to_vec())
            .await?;
        client
            .put(bob.clone(), bob_balance.to_be_bytes().to_vec())
            .await?;
    }

    // check
    let mut new_sum = 0;
    for person in &people {
        new_sum += get_u32(&client, person.clone()).await?;
    }
    assert_eq!(sum, new_sum);
    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_read() -> Result<()> {
    const NUM_BITS_TXN: u32 = 4;
    const NUM_BITS_KEY_PER_TXN: u32 = 4;
    let interval = 2u32.pow(32 - NUM_BITS_TXN - NUM_BITS_KEY_PER_TXN);
    let value = "large_value".repeat(10);

    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    for i in 0..2u32.pow(NUM_BITS_TXN) {
        let mut cur = i * 2u32.pow(32 - NUM_BITS_TXN);
        let keys = iter::repeat_with(|| {
            let v = cur;
            cur = cur.overflowing_add(interval).0;
            v
        })
        .map(|u| u.to_be_bytes().to_vec())
        .take(2usize.pow(NUM_BITS_KEY_PER_TXN))
        .collect::<Vec<_>>();
        let mut txn = client.begin_optimistic().await?;
        for (k, v) in keys.iter().zip(iter::repeat(value.clone())) {
            txn.put(k.clone(), v).await?;
        }
        txn.commit().await?;

        let mut txn = client.begin_optimistic().await?;
        let res = txn.batch_get(keys).await?;
        assert_eq!(res.count(), 2usize.pow(NUM_BITS_KEY_PER_TXN));
        txn.commit().await?;
    }
    // test scan
    let limit = 2u32.pow(NUM_BITS_KEY_PER_TXN + NUM_BITS_TXN + 2); // large enough
    let mut snapshot = client.snapshot(
        client.current_timestamp().await?,
        TransactionOptions::default(),
    );
    let res = snapshot.scan(vec![].., limit).await?;
    assert_eq!(res.count(), 2usize.pow(NUM_BITS_KEY_PER_TXN + NUM_BITS_TXN));

    // scan by small range and combine them
    let mut rng = thread_rng();
    let mut keys = gen_u32_keys(200, &mut rng)
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    keys.sort();

    let mut sum = 0;

    // empty key to key[0]
    let mut snapshot = client.snapshot(
        client.current_timestamp().await?,
        TransactionOptions::default(),
    );
    let res = snapshot.scan(vec![]..keys[0].clone(), limit).await?;
    sum += res.count();

    // key[i] .. key[i+1]
    for i in 0..keys.len() - 1 {
        let res = snapshot
            .scan(keys[i].clone()..keys[i + 1].clone(), limit)
            .await?;
        sum += res.count();
    }

    // keys[last] to unbounded
    let res = snapshot.scan(keys[keys.len() - 1].clone().., limit).await?;
    sum += res.count();

    assert_eq!(sum, 2usize.pow(NUM_BITS_KEY_PER_TXN + NUM_BITS_TXN));

    // test batch_get and batch_get_for_update
    const SKIP_BITS: u32 = 0; // do not retrieve all because there's a limit of message size
    let mut cur = 0u32;
    let keys = iter::repeat_with(|| {
        let v = cur;
        cur = cur.overflowing_add(interval * 2u32.pow(SKIP_BITS)).0;
        v
    })
    .map(|u| u.to_be_bytes().to_vec())
    .take(2usize.pow(NUM_BITS_KEY_PER_TXN + NUM_BITS_TXN - SKIP_BITS))
    .collect::<Vec<_>>();

    let mut txn = client.begin_pessimistic().await?;
    let res = txn.batch_get(keys.clone()).await?;
    assert_eq!(res.count(), keys.len());

    let res = txn.batch_get_for_update(keys.clone()).await?;
    assert_eq!(res.len(), keys.len());

    txn.commit().await?;
    Ok(())
}

// FIXME: the test is temporarily ingnored since it's easy to fail when scheduling is frequent.
#[tokio::test]
#[serial]
async fn txn_bank_transfer() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    let mut rng = thread_rng();
    let options = TransactionOptions::new_optimistic()
        .use_async_commit()
        .drop_check(tikv_client::CheckLevel::Warn);

    let people = gen_u32_keys(NUM_PEOPLE, &mut rng);
    let mut txn = client.begin_with_options(options.clone()).await?;
    let mut sum: u32 = 0;
    for person in &people {
        let init = rng.gen::<u8>() as u32;
        sum += init;
        txn.put(person.clone(), init.to_be_bytes().to_vec()).await?;
    }
    txn.commit().await?;

    // transfer
    for _ in 0..NUM_TRNASFER {
        let mut txn = client.begin_with_options(options.clone()).await?;
        let chosen_people = people.iter().choose_multiple(&mut rng, 2);
        let alice = chosen_people[0];
        let mut alice_balance = get_txn_u32(&mut txn, alice.clone()).await?;
        let bob = chosen_people[1];
        let mut bob_balance = get_txn_u32(&mut txn, bob.clone()).await?;
        if alice_balance == 0 {
            txn.rollback().await?;
            continue;
        }
        let transfer = rng.gen_range(0..alice_balance);
        alice_balance -= transfer;
        bob_balance += transfer;
        txn.put(alice.clone(), alice_balance.to_be_bytes().to_vec())
            .await?;
        txn.put(bob.clone(), bob_balance.to_be_bytes().to_vec())
            .await?;
        txn.commit().await?;
    }

    // check
    let mut new_sum = 0;
    let mut txn = client.begin_optimistic().await?;
    for person in people.iter() {
        new_sum += get_txn_u32(&mut txn, person.clone()).await?;
    }
    assert_eq!(sum, new_sum);
    txn.commit().await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn raw_req() -> Result<()> {
    init().await?;
    let client =
        RawClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace()).await?;

    // empty; get non-existent key
    let res = client.get("k1".to_owned()).await;
    assert_eq!(res?, None);

    // empty; put then batch_get
    client.put("k1".to_owned(), "v1".to_owned()).await?;
    client.put("k2".to_owned(), "v2".to_owned()).await?;

    let res = client
        .batch_get(vec!["k1".to_owned(), "k2".to_owned(), "k3".to_owned()])
        .await?;
    assert_eq!(res, vec![Some(b"v1".to_vec()), Some(b"v2".to_vec()), None]);

    // k1,k2; batch_put then batch_get
    client
        .batch_put(vec![
            ("k3".to_owned(), "v3".to_owned()),
            ("k4".to_owned(), "v4".to_owned()),
        ])
        .await?;

    let res = client
        .batch_get(vec!["k4".to_owned(), "k3".to_owned()])
        .await?;
    assert_eq!(res, vec![Some(b"v4".to_vec()), Some(b"v3".to_vec())]);

    // k1,k2,k3,k4; delete then get
    let res = client.delete("k3".to_owned()).await;
    assert!(res.is_ok());

    let res = client.get("k3".to_owned()).await?;
    assert_eq!(res, None);

    // k1,k2,k4; batch_delete then batch_get
    let res = client
        .batch_delete(vec![
            "k1".to_owned(),
            "k2".to_owned(),
            "k3".to_owned(),
            "k4".to_owned(),
        ])
        .await;
    assert!(res.is_ok());

    let res = client
        .batch_get(vec![
            "k1".to_owned(),
            "k2".to_owned(),
            "k3".to_owned(),
            "k4".to_owned(),
        ])
        .await?;
    assert_eq!(res.len(), 0);

    // empty; batch_put then scan
    client
        .batch_put(vec![
            ("k3".to_owned(), "v3".to_owned()),
            ("k5".to_owned(), "v5".to_owned()),
            ("k1".to_owned(), "v1".to_owned()),
            ("k2".to_owned(), "v2".to_owned()),
            ("k4".to_owned(), "v4".to_owned()),
        ])
        .await?;

    let res = client.scan("k2".to_owned()..="k5".to_owned(), 5).await?;
    assert_eq!(res.len(), 4);
    assert_eq!(res[0].1, "v2".as_bytes());
    assert_eq!(res[1].1, "v3".as_bytes());
    assert_eq!(res[2].1, "v4".as_bytes());
    assert_eq!(res[3].1, "v5".as_bytes());

    let res = client.scan("k2".to_owned().."k5".to_owned(), 2).await?;
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].1, "v2".as_bytes());
    assert_eq!(res[1].1, "v3".as_bytes());

    let res = client.scan("k1".to_owned().., 20).await?;
    assert_eq!(res.len(), 5);
    assert_eq!(res[0].1, "v1".as_bytes());
    assert_eq!(res[1].1, "v2".as_bytes());
    assert_eq!(res[2].1, "v3".as_bytes());
    assert_eq!(res[3].1, "v4".as_bytes());
    assert_eq!(res[4].1, "v5".as_bytes());

    let res = client
        .batch_scan(
            vec![
                "".to_owned().."k1".to_owned(),
                "k1".to_owned().."k2".to_owned(),
                "k2".to_owned().."k3".to_owned(),
                "k3".to_owned().."k4".to_owned(),
                "k4".to_owned().."k5".to_owned(),
            ],
            2,
        )
        .await?;
    assert_eq!(res.len(), 4);

    let res = client
        .batch_scan(
            vec![
                "".to_owned()..="k3".to_owned(),
                "k2".to_owned()..="k5".to_owned(),
            ],
            4,
        )
        .await?;
    assert_eq!(res.len(), 7);
    assert_eq!(res[0].1, "v1".as_bytes());
    assert_eq!(res[1].1, "v2".as_bytes());
    assert_eq!(res[2].1, "v3".as_bytes());
    assert_eq!(res[3].1, "v2".as_bytes());
    assert_eq!(res[4].1, "v3".as_bytes());
    assert_eq!(res[5].1, "v4".as_bytes());
    assert_eq!(res[6].1, "v5".as_bytes());

    // reverse scan

    // By default end key is exclusive, so k5 is not included and start key in included
    let res = client
        .scan_reverse("k2".to_owned().."k5".to_owned(), 5)
        .await?;
    assert_eq!(res.len(), 3);
    assert_eq!(res[0].1, "v4".as_bytes());
    assert_eq!(res[1].1, "v3".as_bytes());
    assert_eq!(res[2].1, "v2".as_bytes());

    // by default end key in exclusive and start key is inclusive but now exclude start key
    let res = client
        .scan_reverse("k2\0".to_owned().."k5".to_owned(), 5)
        .await?;
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].1, "v4".as_bytes());
    assert_eq!(res[1].1, "v3".as_bytes());

    // reverse scan
    // by default end key is exclusive and start key is inclusive but now include end key
    let res = client
        .scan_reverse("k2".to_owned()..="k5".to_owned(), 5)
        .await?;
    assert_eq!(res.len(), 4);
    assert_eq!(res[0].1, "v5".as_bytes());
    assert_eq!(res[1].1, "v4".as_bytes());
    assert_eq!(res[2].1, "v3".as_bytes());
    assert_eq!(res[3].1, "v2".as_bytes());

    // by default end key is exclusive and start key is inclusive but now include end key and exclude start key
    let res = client
        .scan_reverse("k2\0".to_owned()..="k5".to_owned(), 5)
        .await?;
    assert_eq!(res.len(), 3);
    assert_eq!(res[0].1, "v5".as_bytes());
    assert_eq!(res[1].1, "v4".as_bytes());
    assert_eq!(res[2].1, "v3".as_bytes());

    // limit results to first 2
    let res = client
        .scan_reverse("k2".to_owned().."k5".to_owned(), 2)
        .await?;
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].1, "v4".as_bytes());
    assert_eq!(res[1].1, "v3".as_bytes());

    // client-go cannot reverse-scan without an upper endpoint.
    let range = BoundRange::range_from(Key::from("k2".to_owned()));
    let res = client.scan_reverse(range, 20).await?;
    assert!(res.is_empty());

    Ok(())
}

/// Only checks if we successfully update safepoint to PD.
#[tokio::test]
#[serial]
async fn txn_update_safepoint() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    let res = client.gc(client.current_timestamp().await?).await?;
    assert!(res);
    Ok(())
}

/// Tests raw API when there are multiple regions.
#[tokio::test]
#[serial]
async fn raw_write_million() -> Result<()> {
    const NUM_BITS_TXN: u32 = 4;
    const NUM_BITS_KEY_PER_TXN: u32 = 4;
    let interval = 2u32.pow(32 - NUM_BITS_TXN - NUM_BITS_KEY_PER_TXN);

    init().await?;
    let client =
        RawClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace()).await?;

    for i in 0..2u32.pow(NUM_BITS_TXN) {
        let mut cur = i * 2u32.pow(32 - NUM_BITS_TXN);
        let keys = iter::repeat_with(|| {
            let v = cur;
            cur = cur.overflowing_add(interval).0;
            v
        })
        .map(|u| u.to_be_bytes().to_vec())
        .take(2usize.pow(NUM_BITS_KEY_PER_TXN))
        .collect::<Vec<_>>(); // each txn puts 2 ^ 12 keys. 12 = 25 - 13
        client
            .batch_put(
                keys.iter()
                    .cloned()
                    .zip(iter::repeat(1u32.to_be_bytes().to_vec())),
            )
            .await?;

        let res = client.batch_get_pairs(keys).await?;
        assert_eq!(res.len(), 2usize.pow(NUM_BITS_KEY_PER_TXN));
    }

    // test scan, key range from [0,0,0,0] to [255.0.0.0]
    let mut limit = 2000;
    let mut r = client.scan(.., limit).await?;
    assert_eq!(r.len(), 256);
    for (i, val) in r.iter().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8);
    }
    r = client.scan(vec![100, 0, 0, 0].., limit).await?;
    assert_eq!(r.len(), 156);
    for (i, val) in r.iter().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8 + 100);
    }
    r = client
        .scan(vec![5, 0, 0, 0]..vec![200, 0, 0, 0], limit)
        .await?;
    assert_eq!(r.len(), 195);
    for (i, val) in r.iter().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8 + 5);
    }
    r = client
        .scan(vec![5, 0, 0, 0]..=vec![200, 0, 0, 0], limit)
        .await?;
    assert_eq!(r.len(), 196);
    for (i, val) in r.iter().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8 + 5);
    }
    r = client
        .scan(vec![5, 0, 0, 0]..=vec![255, 10, 0, 0], limit)
        .await?;
    assert_eq!(r.len(), 251);
    for (i, val) in r.iter().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8 + 5);
    }
    r = client
        .scan(vec![255, 1, 0, 0]..=vec![255, 10, 0, 0], limit)
        .await?;
    assert_eq!(r.len(), 0);
    r = client.scan(..vec![0, 0, 0, 0], limit).await?;
    assert_eq!(r.len(), 0);

    limit = 3;
    let mut r = client.scan(.., limit).await?;
    assert_eq!(r.len(), limit as usize);
    for (i, val) in r.iter().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8);
    }
    r = client.scan(vec![100, 0, 0, 0].., limit).await?;
    assert_eq!(r.len(), limit as usize);
    for (i, val) in r.iter().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8 + 100);
    }
    r = client
        .scan(vec![5, 0, 0, 0]..vec![200, 0, 0, 0], limit)
        .await?;
    assert_eq!(r.len(), limit as usize);
    for (i, val) in r.iter().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8 + 5);
    }
    r = client
        .scan(vec![5, 0, 0, 0]..=vec![200, 0, 0, 0], limit)
        .await?;
    assert_eq!(r.len(), limit as usize);
    for (i, val) in r.iter().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8 + 5);
    }
    r = client
        .scan(vec![5, 0, 0, 0]..=vec![255, 10, 0, 0], limit)
        .await?;
    assert_eq!(r.len(), limit as usize);
    for (i, val) in r.iter().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8 + 5);
    }
    r = client
        .scan(vec![255, 1, 0, 0]..=vec![255, 10, 0, 0], limit)
        .await?;
    assert_eq!(r.len(), 0);
    r = client.scan(..vec![0, 0, 0, 0], limit).await?;
    assert_eq!(r.len(), 0);

    limit = 0;
    r = client.scan(.., limit).await?;
    assert_eq!(r.len(), limit as usize);

    // test scan_reverse
    // client-go cannot locate the last region, so an unbounded upper endpoint
    // is an empty successful reverse scan.
    let mut limit = 2000;
    let mut r = client.scan_reverse(.., limit).await?;
    assert!(r.is_empty());
    r = client.scan_reverse(vec![100, 0, 0, 0].., limit).await?;
    assert!(r.is_empty());
    r = client
        .scan_reverse(vec![5, 0, 0, 0]..vec![200, 0, 0, 0], limit)
        .await?;
    assert_eq!(r.len(), 195);
    for (i, val) in r.iter().rev().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8 + 5);
    }
    r = client
        .scan_reverse(vec![5, 0, 0, 0]..=vec![200, 0, 0, 0], limit)
        .await?;
    assert_eq!(r.len(), 196);
    for (i, val) in r.iter().rev().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8 + 5);
    }
    r = client
        .scan_reverse(vec![5, 0, 0, 0]..=vec![255, 10, 0, 0], limit)
        .await?;
    assert_eq!(r.len(), 251);
    for (i, val) in r.iter().rev().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8 + 5);
    }
    r = client
        .scan_reverse(vec![255, 1, 0, 0]..=vec![255, 10, 0, 0], limit)
        .await?;
    assert_eq!(r.len(), 0);
    r = client.scan_reverse(..vec![0, 0, 0, 0], limit).await?;
    assert_eq!(r.len(), 0);

    limit = 3;
    let mut r = client.scan_reverse(.., limit).await?;
    assert!(r.is_empty());
    r = client.scan_reverse(vec![100, 0, 0, 0].., limit).await?;
    assert!(r.is_empty());
    r = client
        .scan_reverse(vec![5, 0, 0, 0]..vec![200, 0, 0, 0], limit)
        .await?;
    let mut expected_start = 200 - limit as u8;
    assert_eq!(r.len(), limit as usize);
    for (i, val) in r.iter().rev().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8 + expected_start);
    }
    r = client
        .scan_reverse(vec![5, 0, 0, 0]..=vec![200, 0, 0, 0], limit)
        .await?;
    expected_start = 200 - limit as u8 + 1; // including endKey
    assert_eq!(r.len(), limit as usize);
    for (i, val) in r.iter().rev().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8 + expected_start);
    }
    r = client
        .scan_reverse(vec![5, 0, 0, 0]..=vec![255, 10, 0, 0], limit)
        .await?;
    expected_start = 255 - limit as u8 + 1; // including endKey
    assert_eq!(r.len(), limit as usize);
    for (i, val) in r.iter().rev().enumerate() {
        let k: Vec<u8> = val.0.clone().into();
        assert_eq!(k[0], i as u8 + expected_start);
    }
    r = client
        .scan_reverse(vec![255, 1, 0, 0]..=vec![255, 10, 0, 0], limit)
        .await?;
    assert_eq!(r.len(), 0);
    r = client.scan_reverse(..vec![0, 0, 0, 0], limit).await?;
    assert_eq!(r.len(), 0);

    limit = 0;
    r = client.scan_reverse(.., limit).await?;
    assert_eq!(r.len(), limit as usize);

    // test batch_scan
    for batch_num in 1..4 {
        let _ = client
            .batch_scan(iter::repeat_n(vec![].., batch_num), limit)
            .await?;
        // FIXME: `each_limit` parameter does no work as expected. It limits the
        // entries on each region of each rangqe, instead of each range.
        // assert_eq!(res.len(), limit as usize * batch_num);
    }

    Ok(())
}

/// Tests raw batch put has a large payload.
#[tokio::test]
#[serial]
async fn raw_large_batch_put() -> Result<()> {
    const TARGET_SIZE_MB: usize = 100;
    const KEY_SIZE: usize = 32;
    const VALUE_SIZE: usize = 1024;

    let pair_size = KEY_SIZE + VALUE_SIZE;
    let target_size_bytes = TARGET_SIZE_MB * 1024 * 1024;
    let num_pairs = target_size_bytes / pair_size;
    let mut pairs = Vec::with_capacity(num_pairs);
    for i in 0..num_pairs {
        // Generate key: "bench_key_" + zero-padded number
        let key = format!("bench_key_{:010}", i);

        // Generate value: repeat pattern to reach VALUE_SIZE
        let pattern = format!("value_{}", i % 1000);
        let repeat_count = VALUE_SIZE.div_ceil(pattern.len());
        let value = pattern.repeat(repeat_count);

        pairs.push(KvPair::from((key, value)));
    }

    init().await?;
    let client =
        RawClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace()).await?;

    client.batch_put(pairs.clone()).await?;

    let keys = pairs.iter().map(|pair| pair.0.clone()).collect::<Vec<_>>();
    // split into multiple batch_get to avoid response too large error
    const BATCH_SIZE: usize = 1000;
    let mut got = Vec::with_capacity(num_pairs);
    for chunk in keys.chunks(BATCH_SIZE) {
        let mut partial = client.batch_get_pairs(chunk.to_vec()).await?;
        got.append(&mut partial);
    }
    assert_eq!(got, pairs);

    client.batch_delete(keys.clone()).await?;
    let res = client.batch_get_pairs(keys).await?;
    assert!(res.is_empty());

    Ok(())
}

/// Tests raw ttl API.
#[tokio::test]
#[serial]
async fn raw_ttl() -> Result<()> {
    init().await?;
    let client =
        RawClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace()).await?;
    let key1 = vec![1];
    let key2 = vec![2];
    let val = vec![42];

    assert_eq!(client.get_key_ttl_secs(key1.clone()).await?, None);
    client.put_with_ttl(key1.clone(), val.clone(), 10).await?;
    assert_eq!(client.get(key1.clone()).await?, Some(val.clone()));
    let ttl = client.get_key_ttl_secs(key1.clone()).await?;
    assert!(
        matches!(ttl, Some(v) if v > 5 && v <= 10),
        "unexpected ttl for key1 after put_with_ttl: {ttl:?}"
    );
    client
        .batch_put_with_ttl(
            vec![(key1.clone(), val.clone()), (key2.clone(), val.clone())],
            vec![20, 20],
        )
        .await?;
    assert_eq!(client.get(key1.clone()).await?, Some(val.clone()));
    assert_eq!(client.get(key2.clone()).await?, Some(val.clone()));
    let ttl1 = client.get_key_ttl_secs(key1.clone()).await?;
    assert!(
        matches!(ttl1, Some(v) if v > 15 && v <= 20),
        "unexpected ttl for key1 after batch_put_with_ttl: {ttl1:?}"
    );
    let ttl2 = client.get_key_ttl_secs(key2.clone()).await?;
    assert!(
        matches!(ttl2, Some(v) if v > 15 && v <= 20),
        "unexpected ttl for key2 after batch_put_with_ttl: {ttl2:?}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_pessimistic_rollback() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    let mut preload_txn = client.begin_optimistic().await?;
    let key1 = vec![1];
    let key2 = vec![2];
    let value = key1.clone();

    preload_txn.put(key1.clone(), value).await?;
    preload_txn.commit().await?;

    for _ in 0..100 {
        let mut txn = client.begin_pessimistic().await?;
        let result = txn.get_for_update(key1.clone()).await;
        txn.rollback().await?;
        result?;
    }

    for _ in 0..100 {
        let mut txn = client.begin_pessimistic().await?;
        let result = txn
            .batch_get_for_update(vec![key1.clone(), key2.clone()])
            .await;
        txn.rollback().await?;
        let _ = result?;
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_pessimistic_delete() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    // The transaction will lock the keys and must release the locks on commit,
    // even when values are not written to the DB.
    let mut txn = client.begin_pessimistic().await?;
    txn.put(vec![1], vec![42]).await?;
    txn.delete(vec![1]).await?;
    // FIXME
    //
    // A behavior change in TiKV 7.1 introduced in tikv/tikv#14293.
    //
    // An insert can return AlreadyExist error when the key exists.
    // We comment this line to allow the test to pass so that we can release v0.2
    // Should be addressed alter.
    // txn.insert(vec![2], vec![42]).await?;
    txn.delete(vec![2]).await?;
    txn.put(vec![3], vec![42]).await?;
    txn.commit().await?;

    // Check that the keys are not locked.
    let mut txn2 = client.begin_optimistic().await?;
    txn2.put(vec![1], vec![42]).await?;
    txn2.put(vec![2], vec![42]).await?;
    txn2.put(vec![3], vec![42]).await?;
    txn2.commit().await?;

    // As before, but rollback instead of commit.
    let mut txn = client.begin_pessimistic().await?;
    txn.put(vec![1], vec![42]).await?;
    txn.delete(vec![1]).await?;
    txn.delete(vec![2]).await?;
    // Same with upper comment.
    //
    // txn.insert(vec![2], vec![42]).await?;
    txn.delete(vec![2]).await?;
    txn.put(vec![3], vec![42]).await?;
    txn.rollback().await?;

    let mut txn2 = client.begin_optimistic().await?;
    txn2.put(vec![1], vec![42]).await?;
    txn2.put(vec![2], vec![42]).await?;
    txn2.put(vec![3], vec![42]).await?;
    txn2.commit().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_lock_keys() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    let k1 = b"key1".to_vec();
    let k2 = b"key2".to_vec();
    let v = b"some value".to_vec();

    // optimistic
    let mut t1 = client.begin_optimistic().await?;
    let mut t2 = client.begin_optimistic().await?;
    t1.lock_keys(vec![k1.clone(), k2.clone()]).await?;
    t2.put(k1.clone(), v.clone()).await?;
    t2.commit().await?;
    // must have commit conflict
    assert!(t1.commit().await.is_err());

    // pessimistic
    let k3 = b"key3".to_vec();
    let k4 = b"key4".to_vec();
    let mut t3 = client.begin_pessimistic().await?;
    let mut t4 = client.begin_pessimistic().await?;
    t3.lock_keys(vec![k3.clone(), k4.clone()]).await?;
    assert!(t4.lock_keys(vec![k3.clone(), k4.clone()]).await.is_err());

    t3.rollback().await?;
    t4.lock_keys(vec![k3.clone(), k4.clone()]).await?;
    t4.commit().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_lock_keys_error_handle() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    // Keys in `k` should locate in different regions. See `init()` for boundary of regions.
    let k: Vec<Key> = vec![
        0x00000000_u32,
        0x40000000_u32,
        0x80000000_u32,
        0xC0000000_u32,
    ]
    .into_iter()
    .map(|x| x.to_be_bytes().to_vec().into())
    .collect();

    let mut t1 = client.begin_pessimistic().await?;
    let mut t2 = client.begin_pessimistic().await?;
    let mut t3 = client.begin_pessimistic().await?;

    t1.lock_keys(vec![k[0].clone(), k[1].clone()]).await?;
    assert!(t2
        .lock_keys(vec![k[0].clone(), k[2].clone()])
        .await
        .is_err());
    t3.lock_keys(vec![k[2].clone(), k[3].clone()]).await?;

    t1.rollback().await?;
    t3.rollback().await?;

    t2.lock_keys(vec![k[0].clone(), k[2].clone()]).await?;
    t2.commit().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_get_for_update() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    let key1 = "key".to_owned();
    let key2 = "another key".to_owned();
    let value1 = b"some value".to_owned();
    let value2 = b"another value".to_owned();
    let keys = vec![key1.clone(), key2.clone()];

    let mut t1 = client.begin_pessimistic().await?;
    let mut t2 = client.begin_pessimistic().await?;
    let mut t3 = client.begin_optimistic().await?;
    let mut t4 = client.begin_optimistic().await?;
    let mut t0 = client.begin_pessimistic().await?;
    t0.put(key1.clone(), value1).await?;
    t0.put(key2.clone(), value2).await?;
    t0.commit().await?;

    assert!(t1.get(key1.clone()).await?.is_none());
    assert!(t1.get_for_update(key1.clone()).await?.unwrap() == value1);
    t1.commit().await?;

    assert!(t2.batch_get(keys.clone()).await?.count() == 0);
    let res: HashMap<_, _> = t2
        .batch_get_for_update(keys.clone())
        .await?
        .into_iter()
        .map(From::from)
        .collect();
    t2.commit().await?;
    assert!(res.get(key1.as_bytes()).unwrap() == &value1);
    assert!(res.get(key2.as_bytes()).unwrap() == &value2);

    assert!(t3.get_for_update(key1).await?.is_none());
    assert!(t3.commit().await.is_err());

    assert!(t4.batch_get_for_update(keys).await?.is_empty());
    assert!(t4.commit().await.is_err());

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_pessimistic_heartbeat() -> Result<()> {
    init().await?;

    let key1 = "key1".to_owned();
    let key2 = "key2".to_owned();
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    let mut heartbeat_txn = client
        .begin_with_options(TransactionOptions::new_pessimistic())
        .await?;
    heartbeat_txn.put(key1.clone(), "foo").await.unwrap();

    let mut txn_without_heartbeat = client
        .begin_with_options(
            TransactionOptions::new_pessimistic().heartbeat_option(HeartbeatOption::NoHeartbeat),
        )
        .await?;
    txn_without_heartbeat
        .put(key2.clone(), "fooo")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(23)).await;

    // use other txns to check these locks
    let mut t3 = client
        .begin_with_options(TransactionOptions::new_optimistic().no_resolve_locks())
        .await?;
    t3.put(key1.clone(), "gee").await?;
    assert!(t3.commit().await.is_err());
    let mut t4 = client.begin_optimistic().await?;
    t4.put(key2.clone(), "geee").await?;
    t4.commit().await?;

    assert!(heartbeat_txn.commit().await.is_ok());
    assert!(txn_without_heartbeat.commit().await.is_err());

    Ok(())
}

// It tests very basic functionality of atomic operations (put, cas, delete).
#[tokio::test]
#[serial]
async fn raw_cas() -> Result<()> {
    init().await?;
    let client = RawClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
        .await?
        .with_atomic_for_cas();
    let key = "key".to_owned();
    let value = "value".to_owned();
    let new_value = "new value".to_owned();

    client.put(key.clone(), value.clone()).await?;
    assert_eq!(
        client.get(key.clone()).await?.unwrap(),
        value.clone().as_bytes()
    );

    client
        .compare_and_swap(
            key.clone(),
            Some("another_value".to_owned()).map(|v| v.into()),
            new_value.clone(),
        )
        .await?;
    assert_ne!(
        client.get(key.clone()).await?.unwrap(),
        new_value.clone().as_bytes()
    );

    client
        .compare_and_swap(
            key.clone(),
            Some(value.to_owned()).map(|v| v.into()),
            new_value.clone(),
        )
        .await?;
    assert_eq!(
        client.get(key.clone()).await?.unwrap(),
        new_value.clone().as_bytes()
    );

    client.delete(key.clone()).await?;
    assert!(client.get(key.clone()).await?.is_none());

    // client-go permits BatchDelete while atomic CAS mode is enabled.
    client.batch_delete(vec![key.clone()]).await?;
    let client =
        RawClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace()).await?;
    assert!(matches!(
        client
            .compare_and_swap(key.clone(), None, vec![])
            .await
            .err()
            .unwrap(),
        Error::UnsupportedMode
    ));

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_scan() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    let k1 = b"a".to_vec();
    let v = b"b".to_vec();

    // pessimistic
    let option = TransactionOptions::new_pessimistic().drop_check(tikv_client::CheckLevel::Warn);
    let mut t = client.begin_with_options(option.clone()).await?;
    t.put(k1.clone(), v).await?;
    t.commit().await?;

    let mut t2 = client.begin_with_options(option).await?;
    t2.get(k1.clone()).await?;
    t2.key_exists(k1).await?;
    t2.commit().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_scan_reverse() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    let k1 = b"k1".to_vec();
    let k2 = b"k2".to_vec();
    let k3 = b"k3".to_vec();

    let v1 = b"v1".to_vec();
    let v2 = b"v2".to_vec();
    let v3 = b"v3".to_vec();

    // Pessimistic option is not stable in this case. Use optimistic options instead.
    let option = TransactionOptions::new_optimistic().drop_check(tikv_client::CheckLevel::Warn);
    let mut t = client.begin_with_options(option.clone()).await?;
    t.put(k1.clone(), v1.clone()).await?;
    t.put(k2.clone(), v2.clone()).await?;
    t.put(k3.clone(), v3.clone()).await?;
    t.commit().await?;

    let mut t2 = client.begin_with_options(option).await?;
    {
        // For [k1, k3]:
        let bound_range: BoundRange = (k1.clone()..=k3.clone()).into();
        let resp = t2
            .scan_reverse(bound_range, 3)
            .await?
            .map(|kv| (kv.0, kv.1))
            .collect::<Vec<(Key, Vec<u8>)>>();
        assert_eq!(
            resp,
            vec![
                (Key::from(k3.clone()), v3.clone()),
                (Key::from(k2.clone()), v2.clone()),
                (Key::from(k1.clone()), v1.clone()),
            ]
        );
    }
    {
        // For [k1, k3):
        let bound_range: BoundRange = (k1.clone()..k3.clone()).into();
        let resp = t2
            .scan_reverse(bound_range, 3)
            .await?
            .map(|kv| (kv.0, kv.1))
            .collect::<Vec<(Key, Vec<u8>)>>();
        assert_eq!(
            resp,
            vec![
                (Key::from(k2.clone()), v2.clone()),
                (Key::from(k1.clone()), v1),
            ]
        );
    }
    {
        // For (k1, k3):
        let mut start_key = k1.clone();
        start_key.push(0);
        let bound_range: BoundRange = (start_key..k3).into();
        let resp = t2
            .scan_reverse(bound_range, 3)
            .await?
            .map(|kv| (kv.0, kv.1))
            .collect::<Vec<(Key, Vec<u8>)>>();
        assert_eq!(resp, vec![(Key::from(k2), v2),]);
    }
    t2.commit().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_scan_reverse_multi_regions() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    // Keys in `keys` should locate in different regions. See `init()` for boundary of regions.
    let keys: Vec<Key> = vec![
        0x00000000_u32.to_be_bytes().to_vec(),
        0x40000000_u32.to_be_bytes().to_vec(),
        0x80000000_u32.to_be_bytes().to_vec(),
        0xC0000000_u32.to_be_bytes().to_vec(),
    ]
    .into_iter()
    .map(Into::into)
    .collect();
    let values: Vec<Vec<u8>> = (0..keys.len())
        .map(|i| format!("v{}", i).into_bytes())
        .collect();
    let bound_range: BoundRange =
        (keys.first().unwrap().clone()..=keys.last().unwrap().clone()).into();

    // Pessimistic option is not stable in this case. Use optimistic options instead.
    let option = TransactionOptions::new_optimistic().drop_check(tikv_client::CheckLevel::Warn);
    let mut t = client.begin_with_options(option.clone()).await?;
    let mut reverse_resp = Vec::with_capacity(keys.len());
    for (k, v) in keys.into_iter().zip(values.into_iter()).rev() {
        t.put(k.clone(), v.clone()).await?;
        reverse_resp.push((k, v));
    }
    t.commit().await?;

    let mut t2 = client.begin_with_options(option).await?;
    let resp = t2
        .scan_reverse(bound_range, 100)
        .await?
        .map(|kv| (kv.0, kv.1))
        .collect::<Vec<(Key, Vec<u8>)>>();
    assert_eq!(resp, reverse_resp);
    t2.commit().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_key_exists() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    let key = "key".to_owned();
    let value = "value".to_owned();
    let mut t1 = client.begin_optimistic().await?;
    t1.put(key.clone(), value.clone()).await?;
    assert!(t1.key_exists(key.clone()).await?);
    t1.commit().await?;

    let mut t2 = client.begin_optimistic().await?;
    assert!(t2.key_exists(key).await?);
    t2.commit().await?;

    let not_exists_key = "not_exists_key".to_owned();
    let mut t3 = client.begin_optimistic().await?;
    assert!(!t3.key_exists(not_exists_key).await?);
    t3.commit().await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_batch_mutate_optimistic() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    // Put k0
    {
        let mut txn = client.begin_optimistic().await?;
        txn.put(b"k0".to_vec(), b"v0".to_vec()).await?;
        txn.commit().await?;
    }
    // Delete k0 and put k1, k2
    do_mutate(false).await.unwrap();
    // Read and verify
    verify_mutate(false).await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_batch_mutate_pessimistic() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    // Put k0
    {
        let mut txn = client.begin_pessimistic().await?;
        txn.put(b"k0".to_vec(), b"v0".to_vec()).await?;
        txn.commit().await?;
    }
    // txn1 lock k0, to verify pessimistic locking.
    let mut txn1 = client.begin_pessimistic().await?;
    txn1.put(b"k0".to_vec(), b"vv".to_vec()).await?;

    // txn2 is blocked by txn1, then timeout.
    let txn2_handle = tokio::spawn(do_mutate(true));
    assert!(matches!(
        txn2_handle.await?.unwrap_err(),
        Error::PessimisticLockError { .. }
    ));

    let txn3_handle = tokio::spawn(do_mutate(true));
    // txn1 rollback to release lock.
    txn1.rollback().await?;
    txn3_handle.await?.unwrap();

    // Read and verify
    verify_mutate(true).await?;
    Ok(())
}

async fn begin_mutate(client: &TransactionClient, is_pessimistic: bool) -> Result<Transaction> {
    if is_pessimistic {
        let options = TransactionOptions::new_pessimistic().retry_options(RetryOptions {
            region_backoff: DEFAULT_REGION_BACKOFF,
            lock_backoff: Backoff::no_jitter_backoff(500, 500, 2),
        });
        client.begin_with_options(options).await
    } else {
        client.begin_optimistic().await
    }
}

async fn do_mutate(is_pessimistic: bool) -> Result<()> {
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    let mut txn = begin_mutate(&client, is_pessimistic).await.unwrap();

    let mutations = vec![
        Mutation::Delete(Key::from("k0".to_owned())),
        Mutation::Put(Key::from("k1".to_owned()), Value::from("v1".to_owned())),
        Mutation::Put(Key::from("k2".to_owned()), Value::from("v2".to_owned())),
    ];

    match txn.batch_mutate(mutations).await {
        Ok(()) => {
            txn.commit().await?;
            Ok(())
        }
        Err(err) => {
            let _ = txn.rollback().await;
            Err(err)
        }
    }
}

async fn verify_mutate(is_pessimistic: bool) -> Result<()> {
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    let mut snapshot = snapshot(&client, is_pessimistic).await.unwrap();
    let res: HashMap<Key, Value> = snapshot
        .batch_get(vec!["k0".to_owned(), "k1".to_owned(), "k2".to_owned()])
        .await
        .unwrap()
        .map(|pair| (pair.0, pair.1))
        .collect();
    assert_eq!(res.len(), 2);
    assert_eq!(
        res.get(&Key::from("k1".to_owned())),
        Some(Value::from("v1".to_owned())).as_ref()
    );
    assert_eq!(
        res.get(&Key::from("k2".to_owned())),
        Some(Value::from("v2".to_owned())).as_ref()
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_unsafe_destroy_range() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    const DATA_COUNT: usize = 10;

    {
        let mut txn = client.begin_pessimistic().await.unwrap();
        for i in 0..DATA_COUNT {
            let prefix = i % 2;
            let idx = i / 2;
            txn.put(
                format!("prefix{}_key{}", prefix, idx).into_bytes(),
                format!("value{}{}", prefix, idx).into_bytes(),
            )
            .await
            .unwrap();
        }
        txn.commit().await.unwrap();

        let mut snapshot = client.snapshot(
            client.current_timestamp().await.unwrap(),
            TransactionOptions::new_pessimistic(),
        );
        let kvs = snapshot
            .scan(b"prefix0".to_vec()..b"prefix2".to_vec(), 100)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(kvs.len(), DATA_COUNT);
    }

    {
        // destroy "prefix0"
        client
            .unsafe_destroy_range(b"prefix0".to_vec()..b"prefix1".to_vec())
            .await
            .unwrap();

        let mut snapshot = client.snapshot(
            client.current_timestamp().await.unwrap(),
            TransactionOptions::new_pessimistic(),
        );
        let kvs = snapshot
            .scan(b"prefix0".to_vec()..b"prefix2".to_vec(), 100)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(kvs.len(), DATA_COUNT / 2);
        for (i, kv) in kvs.into_iter().enumerate() {
            assert_eq!(kv.key(), &Key::from(format!("prefix1_key{}", i)));
            assert_eq!(kv.value(), &format!("value1{}", i).into_bytes());
        }
    }

    Ok(())
}
