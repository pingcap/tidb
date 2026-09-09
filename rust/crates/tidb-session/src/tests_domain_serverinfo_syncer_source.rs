// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Port of `pkg/domain/serverinfo/syncer_test.go` (origin/master):
//! `TestTopology`, `TestCleanupStaleServerAndOwnerInfo`, and
//! `TestAssumedServerInfoSyncer`, against
//! `tidb_domain::serverinfo` + `tidb_domain::serverinfo_syncer` — the
//! transcreations of `pkg/domain/serverinfo/info.go` and `syncer.go`.
//!
//! Go's tests run an EMBEDDED etcd cluster (`integration.NewClusterV3`),
//! which has no counterpart in this tier. The etcd surface is taken through
//! the port's [`EtcdOps`] boundary with a recording fake, exactly the tier's
//! established pattern (`rust/crates/tidb-domain/src/serverinfo_syncer.rs`
//! `mod tests`); what a real etcd does with a revoked or expired lease stays
//! etcd's own behavior and is not pinned here. The node fixture mirrors
//! `getServerInfo` (`pkg/domain/serverinfo/syncer.go:481`) with the
//! `mockServerInfo` failpoint's values (`syncer.go:501-508`: start
//! timestamp 1282967700, labels `foo=bar`).

#![cfg(test)]

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use tidb_domain::serverinfo::{DynamicInfo, ServerInfo, StaticInfo, TopologyInfo};
use tidb_domain::serverinfo_syncer::{server_info_key_path, EtcdOps, Syncer};

/// The DDL-owner key path prefix, `pkg/ddl/util/util.go:58`.
const DDL_OWNER_KEY: &str = "/tidb/ddl/fg/owner";

/// A recording etcd: key -> (value, lease). `lease == 0` is a leaseless PUT.
#[derive(Default)]
struct FakeEtcd {
    keys: Mutex<BTreeMap<String, (Vec<u8>, i64)>>,
    next_lease: AtomicI64,
}

impl FakeEtcd {
    /// `KV.Get` of ONE key, as the Go test helpers (`getTopologyFromEtcd`,
    /// `ttlKeyExists`) read: the kv list under that exact key.
    fn get(&self, key: &str) -> Vec<(String, Vec<u8>)> {
        self.keys
            .lock()
            .unwrap()
            .iter()
            .filter(|(stored, _)| stored.as_str() == key)
            .map(|(k, (v, _))| (k.clone(), v.clone()))
            .collect()
    }
}

impl EtcdOps for FakeEtcd {
    fn lease_grant(&self, _ttl_seconds: i64) -> Result<i64, String> {
        Ok(self.next_lease.fetch_add(1, Ordering::SeqCst) + 1)
    }
    fn lease_keep_alive_once(&self, _lease: i64) -> Result<(), String> {
        Ok(())
    }
    fn lease_revoke(&self, _lease: i64) -> Result<(), String> {
        Ok(())
    }
    fn put_with_lease(&self, key: &str, value: &[u8], lease: i64) -> Result<(), String> {
        self.keys
            .lock()
            .unwrap()
            .insert(key.to_owned(), (value.to_vec(), lease));
        Ok(())
    }
    fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
        Ok(self
            .keys
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(k, (v, _))| (k.clone(), v.clone()))
            .collect())
    }
    fn delete(&self, key: &str) -> Result<(), String> {
        self.keys.lock().unwrap().remove(key);
        Ok(())
    }
    fn delete_prefix(&self, prefix: &str) -> Result<(), String> {
        self.keys
            .lock()
            .unwrap()
            .retain(|key, _| !key.starts_with(prefix));
        Ok(())
    }
    fn put(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.put_with_lease(key, value, 0)
    }
}

fn put_str(fake: &FakeEtcd, key: &str, value: &str) {
    fake.put(key, value.as_bytes()).unwrap();
}

/// Go `getServerInfo` (`pkg/domain/serverinfo/syncer.go:481`) under the
/// `mockServerInfo` failpoint (`:501-508`).
fn mock_server_info(id: &str, ip: &str, port: usize) -> ServerInfo {
    ServerInfo {
        static_info: StaticInfo {
            id: id.to_owned(),
            ip: ip.to_owned(),
            port,
            status_port: 10080,
            start_timestamp: 1_282_967_700,
            server_id_getter: Some(Arc::new(|| 1)),
            ..StaticInfo::default()
        },
        dynamic_info: DynamicInfo {
            labels: HashMap::from([("foo".to_owned(), "bar".to_owned())]),
        },
    }
}

/// Go `syncer_test.go:141-155` `(s *Syncer).getTopologyFromEtcd`, on the
/// fake: read this node's `/info` key and decode it.
fn get_topology_from_etcd(syncer: &Syncer, fake: &FakeEtcd) -> TopologyInfo {
    let key = format!("{}/info", syncer.topology_prefix());
    let entries = fake.get(&key);
    assert_eq!(entries.len(), 1, "exactly one /info kv under {key}");
    serde_json::from_slice(&entries[0].1).expect("topology json decodes")
}

/// Go `syncer_test.go:157-166` `(s *Syncer).ttlKeyExists`, on the fake.
fn ttl_key_exists(syncer: &Syncer, fake: &FakeEtcd) -> bool {
    let key = format!("{}/ttl", syncer.topology_prefix());
    let entries = fake.get(&key);
    assert!(entries.len() < 2, "too many arguments in resp.Kvs");
    entries.len() == 1
}

/// Go `pkg/domain/serverinfo/syncer_test.go:53::TestTopology`: the topology
/// record is published, survives its own key being deleted via
/// `RestartTopology`, and the leased `/ttl` key comes back through
/// `updateTopologyAliveness`.
#[test]
fn topology_repairs_itself_after_key_loss() {
    let fake = Arc::new(FakeEtcd::default());
    let info = mock_server_info("test", "127.0.0.1", 4000);
    let syncer = Syncer::new(info.clone(), Some(fake.clone()));

    syncer
        .new_topology_session_and_store_server_info()
        .expect("topology session taken");

    let topology = get_topology_from_etcd(&syncer, &fake);
    assert_eq!(topology.start_timestamp, 1_282_967_700);
    assert_eq!(topology.labels["foo"], "bar");
    assert_eq!(syncer.local_server_info().to_topology_info(), topology);

    let info_key = format!("{}/info", syncer.topology_prefix());
    let ttl_key = format!("{}/ttl", syncer.topology_prefix());

    // Go deletes the non-TTL (leaseless) key and restarts the syncer.
    fake.delete(&info_key).unwrap();
    syncer.restart_topology().expect("restart");

    let topology = get_topology_from_etcd(&syncer, &fake);
    let dir = std::env::current_exe()
        .expect("executable path")
        .parent()
        .expect("executable has a parent directory")
        .to_path_buf();
    assert_eq!(
        topology.deploy_path,
        dir.to_string_lossy(),
        "deploy path is the executable's directory"
    );
    assert_eq!(topology.start_timestamp, 1_282_967_700);
    assert_eq!(syncer.local_server_info().to_topology_info(), topology);

    // Check ttl key: present, then deleted, then rewritten by aliveness.
    assert!(ttl_key_exists(&syncer, &fake));
    fake.delete(&ttl_key).unwrap();
    syncer.update_topology_aliveness().expect("ttl refresh");
    assert!(ttl_key_exists(&syncer, &fake));
}

/// Go `pkg/domain/serverinfo/syncer_test.go:160::TestCleanupStaleServerAndOwnerInfo`
/// (server-info half): a NEW syncer at a previously used address deletes the
/// dead instance's record, keeps a peer at a different address, and
/// registers itself.
#[test]
fn startup_cleans_the_stale_server_info_at_this_address() {
    let fake = Arc::new(FakeEtcd::default());

    // Go configures the global config so new Syncers get IP=1.1.1.1,
    // Port=4000; here the same address goes into the node fixtures.
    let stale_id = "stale-uuid-old";
    let mut stale_info = ServerInfo {
        static_info: StaticInfo {
            id: stale_id.to_owned(),
            ip: "1.1.1.1".to_owned(),
            port: 4000,
            server_id_getter: Some(Arc::new(|| 0)),
            ..StaticInfo::default()
        },
        ..ServerInfo::default()
    };
    let stale_info_path = server_info_key_path(stale_id);
    let stale_info_buf = stale_info.marshal().expect("stale info marshals");
    fake.put(&stale_info_path, &stale_info_buf).unwrap();

    // A peer at a different address must NOT be deleted.
    let other_id = "other-uuid";
    let mut other_info = ServerInfo {
        static_info: StaticInfo {
            id: other_id.to_owned(),
            ip: "2.2.2.2".to_owned(),
            port: 4000,
            server_id_getter: Some(Arc::new(|| 0)),
            ..StaticInfo::default()
        },
        ..ServerInfo::default()
    };
    let other_info_path = server_info_key_path(other_id);
    let other_info_buf = other_info.marshal().expect("other info marshals");
    fake.put(&other_info_path, &other_info_buf).unwrap();

    // A stale DDL owner election record left by the dead instance; its
    // deletion is the owner-election half of the Go test and is covered by
    // `stale_ddl_owner_key_is_deleted_too` below.
    put_str(&fake, &format!("{DDL_OWNER_KEY}/12345"), stale_id);

    // Act: create a new Syncer with same IP+Port and store its server info.
    let new_id = "new-uuid";
    let syncer = Syncer::new(
        mock_server_info_at(new_id, "1.1.1.1", 4000),
        Some(fake.clone()),
    );
    let new_info = syncer.local_server_info();
    assert_eq!(new_info.static_info.ip, "1.1.1.1");
    assert_eq!(new_info.static_info.port, 4000);
    syncer
        .new_session_and_store_server_info()
        .expect("session and store");

    // Stale ServerInfo should be deleted.
    assert!(
        fake.get(&stale_info_path).is_empty(),
        "stale server info should have been deleted"
    );
    // Other node's ServerInfo should still exist.
    assert_eq!(
        fake.get(&other_info_path).len(),
        1,
        "other node's server info should not be deleted"
    );
    // New ServerInfo should be registered.
    assert_eq!(
        fake.get(&server_info_key_path(new_id)).len(),
        1,
        "new server info should be registered"
    );
}

/// The same fixture shape as Go's test: id, address, server-id getter only.
fn mock_server_info_at(id: &str, ip: &str, port: usize) -> ServerInfo {
    ServerInfo {
        static_info: StaticInfo {
            id: id.to_owned(),
            ip: ip.to_owned(),
            port,
            server_id_getter: Some(Arc::new(|| 1)),
            ..StaticInfo::default()
        },
        ..ServerInfo::default()
    }
}

/// Go `pkg/domain/serverinfo/syncer_test.go:196-205`: the stale DDL owner
/// key `DDLOwnerKey + "/12345"` carrying the dead instance's UUID must be
/// deleted by the new syncer's session setup.
// go-parity-gap: the owner-key half of cleanupStaleServerAndOwnerInfo
// (owner.DeleteOwnerKeyByID) is not transcreated; the port's cleanup covers
// the server-info half only (see serverinfo_syncer.rs module doc).
#[test]
#[ignore = "go-parity-gap: owner.DeleteOwnerKeyByID (owner election) is not \
           transcreated; stale DDL owner keys survive startup cleanup"]
fn stale_ddl_owner_key_is_deleted_too() {
    let fake = Arc::new(FakeEtcd::default());

    let stale_id = "stale-uuid-old";
    let mut stale_info = ServerInfo {
        static_info: StaticInfo {
            id: stale_id.to_owned(),
            ip: "1.1.1.1".to_owned(),
            port: 4000,
            server_id_getter: Some(Arc::new(|| 0)),
            ..StaticInfo::default()
        },
        ..ServerInfo::default()
    };
    fake.put(
        &server_info_key_path(stale_id),
        &stale_info.marshal().expect("marshals"),
    )
    .unwrap();
    let stale_owner_key = format!("{DDL_OWNER_KEY}/12345");
    put_str(&fake, &stale_owner_key, stale_id);

    let syncer = Syncer::new(
        mock_server_info_at("new-uuid", "1.1.1.1", 4000),
        Some(fake.clone()),
    );
    syncer.new_session_and_store_server_info().unwrap();

    let resp = fake.get(&stale_owner_key);
    assert!(
        resp.is_empty(),
        "stale DDL owner key should have been deleted"
    );
}

/// Go `pkg/domain/serverinfo/syncer_test.go:253::TestAssumedServerInfoSyncer`,
/// current-keyspace arm: a plain `NewSyncer` is NOT assumed and carries no
/// assumed keyspace.
///
/// Go's third assertion (`info.Keyspace == keyspace.System`, the global
/// `KeyspaceName`) rides `getServerInfo`'s global-config read
/// (`pkg/domain/serverinfo/syncer.go:491`); the transcreation's
/// `server_info_from_config` narrows keyspaces away (see its doc), so only
/// the two assumptions-related assertions port here.
#[test]
fn assumed_server_info_syncer_current_keyspace_arm() {
    // current ks
    let syncer = Syncer::new(mock_server_info_at("1", "", 0), None);
    let info = syncer.local_server_info();
    assert!(!info.static_info.is_assumed());
    assert!(info.static_info.assumed_keyspace.is_empty());
}

/// Go `pkg/domain/serverinfo/syncer_test.go:270-278`, cross-keyspace arm:
/// `NewCrossKSSyncer(..., "ks1")` reports `IsAssumed()` with the assumed
/// keyspace name, while `Keyspace` stays the system keyspace.
// go-parity-gap: NewCrossKSSyncer is not transcreated (keyspaces arrive
// with their own track, per the serverinfo_syncer module doc); only the
// IsAssumed predicate over the carried field is pinnable today.
#[test]
#[ignore = "go-parity-gap: NewCrossKSSyncer (cross-keyspace syncer wiring) is \
           not transcreated; keyspace track pending"]
fn assumed_server_info_syncer_cross_keyspace_arm() {
    // The predicate NewCrossKSSyncer's result must satisfy: a non-empty
    // assumed keyspace IS assumed.
    let assumed = StaticInfo {
        keyspace: "SYSTEM".to_owned(),
        assumed_keyspace: "ks1".to_owned(),
        ..StaticInfo::default()
    };
    assert!(assumed.is_assumed());
    assert_eq!(assumed.assumed_keyspace, "ks1");
    // And the wiring this arm pins once the constructor exists:
    // NewCrossKSSyncer("1", getter, nil, nil, "ks1").GetLocalServerInfo()
    //   .Keyspace == "SYSTEM" (the global KeyspaceName, syncer.go:491).
}
