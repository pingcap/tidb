//! Go `pkg/util/etcd/etcd_test.go` and `zzprobe/o15_etcd/TestNamespaceOperations`.
//! The worker boundary records physical keys; callers observe logical keys.

use super::*;

fn client_with_worker(
    work: impl FnOnce(mpsc::Receiver<EtcdCommand>) + Send + 'static,
) -> EtcdClient {
    let (commands, receiver) = mpsc::channel();
    let (shutdown, _) = watch::channel(false);
    EtcdClient {
        namespace: Vec::new(),
        shared: Arc::new(EtcdClientShared {
            endpoints: Vec::new(),
            timeout: Duration::from_secs(2),
            security: Arc::new(ClusterSecurity::plaintext()),
            commands,
            shutdown,
            worker: Mutex::new(Some(std::thread::spawn(move || work(receiver)))),
        }),
    }
}

/// Go `TestSetEtcdCliByNamespace`: the unprefixed client sees prefix + key.
#[test]
fn test_set_etcd_cli_by_namespace() {
    let raw = client_with_worker(|commands| {
        let mut values = HashMap::new();
        while let Ok(command) = commands.recv() {
            match command {
                EtcdCommand::Put {
                    key, value, reply, ..
                } => {
                    values.insert(key, value);
                    reply.send(Ok(())).unwrap();
                }
                EtcdCommand::Get { key, reply, .. } => {
                    reply.send(Ok(values.get(&key).cloned())).unwrap();
                }
                EtcdCommand::Close { reply } => {
                    let _ = reply.send(());
                    return;
                }
                _ => panic!("unexpected operation"),
            }
        }
    });
    let mut scoped = raw.clone();
    scoped.set_etcd_cli_by_namespace("testNamespace/");
    scoped.put(b"testkey", b"test").unwrap();
    assert_eq!(
        raw.get(b"testNamespace/testkey").unwrap(),
        Some(b"test".to_vec())
    );
    assert_eq!(raw.get(b"testkey").unwrap(), None);
    assert_eq!(scoped.get(b"testkey").unwrap(), Some(b"test".to_vec()));

    // Go wrappers compose, do not add '/', and do not mutate older handles.
    let original_scoped = scoped.clone();
    scoped.set_etcd_cli_by_namespace("nested");
    scoped.put(b"/b", b"v3").unwrap();
    assert_eq!(
        raw.get(b"testNamespace/nested/b").unwrap(),
        Some(b"v3".to_vec())
    );
    assert_eq!(original_scoped.get(b"/b").unwrap(), None);
}

/// Go `TestNamespaceOperations` capture: the namespace wraps comparisons,
/// writes, range/delete operations and returned keys without changing leases.
#[test]
fn test_namespace_operations() {
    let raw = client_with_worker(|commands| {
        let physical = b"/keyspaces/tidb/42/a";
        let entry = || EtcdKeyValue {
            key: physical.to_vec(),
            value: b"v2".to_vec(),
            create_revision: 2,
            mod_revision: 3,
            lease: 7,
        };
        while let Ok(command) = commands.recv() {
            match command {
                EtcdCommand::Put { key, reply, .. } => {
                    assert_eq!(key, physical);
                    reply.send(Ok(())).unwrap();
                }
                EtcdCommand::PutWithLease {
                    key, lease, reply, ..
                } => {
                    assert_eq!(key, physical);
                    assert_eq!(lease, 7);
                    reply.send(Ok(())).unwrap();
                }
                EtcdCommand::Get { key, reply, .. } => {
                    assert_eq!(key, physical);
                    reply.send(Ok(Some(b"v2".to_vec()))).unwrap();
                }
                EtcdCommand::GetPrefix { prefix, reply } => {
                    assert_eq!(prefix, b"/keyspaces/tidb/42/");
                    reply
                        .send(Ok(vec![(physical.to_vec(), b"v2".to_vec())]))
                        .unwrap();
                }
                EtcdCommand::GetPrefixMetadata { prefix, reply } => {
                    assert_eq!(prefix, b"/keyspaces/tidb/42/");
                    reply.send(Ok((vec![entry()], 3))).unwrap();
                }
                EtcdCommand::CreateWithLease {
                    key, lease, reply, ..
                } => {
                    assert_eq!(key, physical);
                    assert_eq!(lease, 7);
                    reply.send(Ok(true)).unwrap();
                }
                EtcdCommand::CreateOrGetWithLease {
                    key, lease, reply, ..
                } => {
                    assert_eq!(key, physical);
                    assert_eq!(lease, 7);
                    reply
                        .send(Ok(EtcdCreateOrGet {
                            created: false,
                            existing: Some(entry()),
                        }))
                        .unwrap();
                }
                EtcdCommand::Create { key, reply, .. }
                | EtcdCommand::CompareValueAndPut { key, reply, .. }
                | EtcdCommand::CompareAndPut { key, reply, .. }
                | EtcdCommand::DeleteIfModRevision { key, reply, .. } => {
                    assert_eq!(key, physical);
                    reply.send(Ok(true)).unwrap();
                }
                EtcdCommand::CompareAndPutWithLease {
                    key, lease, reply, ..
                } => {
                    assert_eq!(key, physical);
                    assert_eq!(lease, 7);
                    reply.send(Ok(true)).unwrap();
                }
                EtcdCommand::DeleteKeysAndPutWithLease {
                    key,
                    delete_keys,
                    lease,
                    reply,
                    ..
                } => {
                    assert_eq!(key, physical);
                    assert_eq!(lease, 7);
                    assert_eq!(delete_keys, [b"/keyspaces/tidb/42/old".to_vec()]);
                    reply.send(Ok(())).unwrap();
                }
                EtcdCommand::Delete { key, reply, .. } => {
                    assert_eq!(key, physical);
                    reply.send(Ok(1)).unwrap();
                }
                EtcdCommand::DeletePrefix { prefix, reply } => {
                    assert_eq!(prefix, b"/keyspaces/tidb/42/");
                    reply.send(Ok(())).unwrap();
                }
                EtcdCommand::LeaseGrant {
                    ttl_seconds, reply, ..
                } => {
                    assert_eq!(ttl_seconds, 30);
                    reply.send(Ok((7, 30))).unwrap();
                }
                EtcdCommand::LeaseKeepAliveOnce { id, reply } => {
                    assert_eq!(id, 7);
                    reply.send(Ok(30)).unwrap();
                }
                EtcdCommand::LeaseRevoke { id, reply } => {
                    assert_eq!(id, 7);
                    reply.send(Ok(())).unwrap();
                }
                EtcdCommand::Close { reply } => {
                    let _ = reply.send(());
                    return;
                }
            }
        }
    });
    let mut scoped = raw.clone();
    scoped.set_etcd_cli_by_namespace("/keyspaces/tidb/42");
    let key = b"/a";
    scoped.put(key, b"v2").unwrap();
    scoped.put_with_lease(key, b"v2", 7).unwrap();
    assert_eq!(scoped.get(key).unwrap(), Some(b"v2".to_vec()));
    assert_eq!(
        scoped.get_prefix(b"/").unwrap(),
        [(key.to_vec(), b"v2".to_vec())]
    );
    let (entries, revision) = scoped.get_prefix_metadata_with_revision(b"/").unwrap();
    assert_eq!(revision, 3);
    assert_eq!(entries[0].key, key);
    assert_eq!(entries[0].lease, 7);
    assert_eq!(entries[0].create_revision, 2);
    assert_eq!(entries[0].mod_revision, 3);
    assert!(scoped.create(key, b"v2").unwrap());
    assert!(scoped.create_with_lease(key, b"v2", 7).unwrap());
    let existing = scoped.create_or_get_with_lease(key, b"v2", 7).unwrap();
    assert!(!existing.created);
    assert_eq!(existing.existing.unwrap(), entries[0]);
    assert!(scoped.compare_value_and_put(key, b"v1", b"v2").unwrap());
    assert!(scoped.compare_and_put(key, 2, b"v2").unwrap());
    assert!(scoped.compare_and_put_with_lease(key, 2, b"v2", 7).unwrap());
    assert!(scoped.delete_if_mod_revision(key, 3).unwrap());
    scoped
        .delete_keys_and_put_with_lease(vec![b"/old".to_vec()], key, b"v2", 7)
        .unwrap();
    assert_eq!(scoped.delete_count(key).unwrap(), 1);
    scoped.delete_prefix(b"/").unwrap();
    assert_eq!(scoped.lease_grant(30).unwrap(), (7, 30));
    assert_eq!(scoped.lease_keep_alive_once(7).unwrap(), 30);
    scoped.lease_revoke(7).unwrap();
}

/// Go `TestNamespaceOperations`, using a real etcd endpoint supplied by the
/// throwaway Go probe. Exercise transport, response unprefixing and watches.
#[test]
#[ignore = "requires TIDB_ETCD_TEST_ENDPOINT (an isolated etcd server)"]
fn test_namespace_operations_real_etcd() {
    let endpoint = std::env::var("TIDB_ETCD_TEST_ENDPOINT").unwrap();
    let raw = EtcdClient::connect([endpoint], Duration::from_secs(5)).unwrap();
    let prefix = format!("/o15-{}/", std::process::id());
    let mut a = raw.clone();
    a.set_etcd_cli_by_namespace(&prefix);
    let mut b = a.clone();
    b.set_etcd_cli_by_namespace("other/");
    let (lease, _) = a.lease_grant(30).unwrap();
    a.put_with_lease(b"key", b"v1", lease).unwrap();
    assert_eq!(
        raw.get(format!("{prefix}key").as_bytes()).unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(b.get(b"key").unwrap(), None);
    let (entries, revision) = a.get_prefix_metadata_with_revision(b"key").unwrap();
    assert_eq!(entries[0].key, b"key");
    assert_eq!(entries[0].lease, lease);

    let (events_tx, events_rx) = mpsc::channel();
    let mut event_watch = a
        .watch_key(b"key".to_vec(), revision + 1, move |event| {
            events_tx.send(event.clone()).unwrap();
        })
        .unwrap();
    let (single_tx, single_rx) = mpsc::channel();
    let mut single_watch = a
        .watch_key_responses(
            b"key".to_vec(),
            revision + 1,
            || false,
            move |response| {
                single_tx.send(response.clone()).unwrap();
            },
        )
        .unwrap();
    let (prefix_tx, prefix_rx) = mpsc::channel();
    let mut prefix_watch = a
        .watch_prefix_responses(
            b"ke".to_vec(),
            revision + 1,
            || false,
            move |response| {
                prefix_tx.send(response.clone()).unwrap();
            },
        )
        .unwrap();
    assert!(a
        .compare_and_put(b"key", entries[0].mod_revision, b"v2")
        .unwrap());
    let event = events_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(event.key, b"key");
    assert_eq!(event.value, b"v2");
    for receiver in [single_rx, prefix_rx] {
        let response = receiver.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(response.events[0].key, b"key");
        assert_eq!(response.events[0].value, b"v2");
        assert_eq!(response.events[0].mod_revision, event.mod_revision);
    }
    event_watch.shutdown();
    single_watch.shutdown();
    prefix_watch.shutdown();

    let existing = a
        .create_or_get_with_lease(b"key", b"unused", lease)
        .unwrap();
    assert!(!existing.created);
    assert_eq!(existing.existing.unwrap().key, b"key");
    assert!(a.compare_value_and_put(b"key", b"v2", b"v3").unwrap());
    let entries = a.get_prefix_metadata(b"key").unwrap();
    assert!(a
        .compare_and_put_with_lease(b"key", entries[0].mod_revision, b"v4", lease)
        .unwrap());
    assert!(a.create_with_lease(b"old", b"old", lease).unwrap());
    a.delete_keys_and_put_with_lease(vec![b"old".to_vec()], b"key", b"v5", lease)
        .unwrap();
    assert_eq!(a.get(b"old").unwrap(), None);
    assert_eq!(
        a.get_prefix(b"key").unwrap(),
        [(b"key".to_vec(), b"v5".to_vec())]
    );
    b.put(b"key", b"other").unwrap();
    a.delete_prefix(b"key").unwrap();
    assert_eq!(a.get(b"key").unwrap(), None);
    assert_eq!(b.get(b"key").unwrap(), Some(b"other".to_vec()));
    assert!(a.create(b"new", b"new").unwrap());
    let entries = a.get_prefix_metadata(b"new").unwrap();
    assert!(a
        .delete_if_mod_revision(b"new", entries[0].mod_revision)
        .unwrap());
    assert!(a.lease_keep_alive_once(lease).unwrap() > 0);
    a.lease_revoke(lease).unwrap();
    raw.delete_prefix(prefix.as_bytes()).unwrap();
}
