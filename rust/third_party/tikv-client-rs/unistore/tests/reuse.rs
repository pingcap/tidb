// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use unistore::{
    AssertionLevel, IsolationLevel, MockEngine, Mutation, MvccStore, PrewriteRequest, TxnMutation,
};

#[test]
fn committed_version_facade_is_reusable_outside_the_crate() {
    let store = MvccStore::new();
    store
        .commit(
            1,
            2,
            [Mutation::Put {
                key: b"shared".to_vec(),
                value: b"first".to_vec(),
            }],
        )
        .unwrap();
    let consumer = store.clone();
    assert_eq!(consumer.get(b"shared", 1), None);
    assert_eq!(consumer.get(b"shared", 2), Some(b"first".to_vec()));
    assert_eq!(
        consumer.scan(Some(b"shared"), Some(b"sharer"), 2),
        vec![(b"shared".to_vec(), b"first".to_vec())]
    );
    assert_eq!(consumer.versions(b"shared")[0].commit_ts, 2);
}

#[test]
fn source_mapped_mock_engine_is_reusable_without_tikv_client() {
    let engine = MockEngine::new();
    assert_eq!(
        engine.prewrite(&PrewriteRequest {
            mutations: vec![TxnMutation::put(b"key", b"value")],
            primary: b"key".to_vec(),
            start_ts: 5,
            ttl: 100,
            txn_size: 1,
            assertion_level: AssertionLevel::Strict,
            ..Default::default()
        }),
        vec![None]
    );
    engine.commit(&[b"key".to_vec()], 5, 10).unwrap();
    assert_eq!(
        engine
            .get(b"key", 10, IsolationLevel::SnapshotIsolation, &[])
            .unwrap(),
        Some((b"value".to_vec(), 10))
    );

    engine.raw_put("default", b"raw".to_vec(), b"bytes".to_vec());
    assert_eq!(engine.raw_get("default", b"raw"), Some(b"bytes".to_vec()));
    let (_, keys, bytes) = engine.raw_checksum("default", b"", b"");
    assert_eq!((keys, bytes), (1, 8));
}
