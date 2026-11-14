// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go capture: `zzprobe/o15_store.TestStoreKeyspaceIdentity`.
use super::tikv_store::*;
use std::sync::Arc;
use tidb_keyspace::KeyspaceCodec;
use tikv_client::proto::keyspacepb;

/// Go `zzprobe/o15_store.TestStoreKeyspaceIdentity`.
#[test]
fn test_store_keyspace_identity() {
    let runtime = store_runtime().unwrap();
    for (name, id, namespace) in [
        ("", u32::MAX, ""),
        ("SYSTEM", 1, "/keyspaces/tidb/1"),
        ("tenant", 42, "/keyspaces/tidb/42"),
        ("DEFAULT", 0, "/keyspaces/tidb/0"),
    ] {
        let pd = Arc::new(tikv_client::mock::MockPdClient::default());
        pd.set_keyspace_meta(keyspacepb::KeyspaceMeta {
            name: name.into(),
            keyspace: Some(keyspacepb::keyspace_meta::Keyspace::Id(id)),
            state: keyspacepb::KeyspaceState::Enabled as i32,
            ..Default::default()
        });
        let config = if name.is_empty() {
            tikv_client::Config::default()
        } else {
            tikv_client::Config::default().with_keyspace(name)
        };
        let inner = runtime
            .block_on(tikv_client::tikv::KvStore::new_with_pd_client(
                pd,
                Arc::new(tikv_client::tikv::MockSafePointKv::new()),
                config,
            ))
            .unwrap();
        let store = TikvStore::new(Arc::clone(&runtime), inner);
        assert_eq!(store.get_keyspace(), name);
        assert_eq!(store.keyspace_id(), id);
        assert_eq!(store.is_api_v1(), name.is_empty());
        assert_eq!(
            tidb_keyspace::make_keyspace_etcd_namespace(&store),
            namespace
        );
        // Go storage identity belongs to the opened store, including its clones.
        assert_eq!(store.clone().get_keyspace(), name);
        runtime.block_on(store.store().clone().close()).unwrap();
    }
}

/// Go `zzprobe/o15_store.TestKeyspaceGCNamespace`.
#[test]
fn test_keyspace_gc_namespace() {
    let runtime = store_runtime().unwrap();
    for (config, expected) in [
        (vec![], false),
        (vec![("gc_management_type", "unified")], false),
        (vec![("gc_management_type", "keyspace_level")], true),
        (vec![("safe_point_version", "v2")], true),
        (
            vec![
                ("gc_management_type", "unified"),
                ("safe_point_version", "v2"),
            ],
            false,
        ),
    ] {
        let pd = Arc::new(tikv_client::mock::MockPdClient::default());
        pd.set_keyspace_meta(keyspacepb::KeyspaceMeta {
            name: "tenant".into(),
            keyspace: Some(keyspacepb::keyspace_meta::Keyspace::Id(42)),
            state: keyspacepb::KeyspaceState::Enabled as i32,
            config: config
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            ..Default::default()
        });
        let store = runtime
            .block_on(tikv_client::tikv::KvStore::new_with_pd_client(
                pd,
                Arc::new(tikv_client::tikv::MockSafePointKv::new()),
                tikv_client::Config::default().with_keyspace("tenant"),
            ))
            .unwrap();
        assert_eq!(store.is_keyspace_using_keyspace_level_gc(), expected);
        runtime.block_on(store.close()).unwrap();
    }
}
