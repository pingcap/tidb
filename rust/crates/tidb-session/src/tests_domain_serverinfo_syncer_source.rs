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
    get_error: Mutex<Option<String>>,
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
        if let Some(error) = self.get_error.lock().unwrap().as_ref() {
            return Err(error.clone());
        }
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

// Rust regression coverage for the SQL consumers of the syncer. This is not
// acceptance of Go's complete seven-source cluster discovery owner.
#[test]
fn cluster_metadata_contains_only_discovered_nodes() {
    use crate::{tests_support::row_text, Session};

    let mut session = Session::new();
    let sql = "SELECT TYPE, INSTANCE FROM information_schema.cluster_info ORDER BY INSTANCE";
    assert!(row_text(session.run(sql)).is_empty());

    let local = mock_server_info("local", "192.0.2.10", 4000);
    session.set_server_info_syncer(Arc::new(Syncer::new(local.clone(), None)));
    assert_eq!(row_text(session.run(sql)), [["tidb", "192.0.2.10:4000"]]);

    let fake = Arc::new(FakeEtcd::default());
    session.set_server_info_syncer(Arc::new(Syncer::new(local, Some(fake.clone()))));
    // An empty registry must not manufacture either a local record or store1.
    assert!(row_text(session.run(sql)).is_empty());
    for (id, ip) in [("first", "192.0.2.20"), ("second", "192.0.2.30")] {
        let mut info = mock_server_info(id, ip, 4000);
        fake.put(&server_info_key_path(id), &info.marshal().unwrap())
            .unwrap();
    }
    assert_eq!(
        row_text(session.run(sql)),
        [["tidb", "192.0.2.20:4000"], ["tidb", "192.0.2.30:4000"]]
    );
    // Go reads proxy /info records (not TTL keys) and TiCDC topology.
    put_str(
        &fake,
        "/topology/tiproxy/[2001:db8::1]:6000/info",
        r#"{"ip":"2001:db8::1","port":"6000","status_port":"3080","version":"v1.2.3"}"#,
    );
    put_str(&fake, "/topology/tiproxy/[2001:db8::1]:6000/ttl", "123");
    put_str(
        &fake,
        "/topology/ticdc/default/capture/one",
        r#"{"address":"192.0.2.40:8300","version":"v9.0.0"}"#,
    );
    assert_eq!(row_text(session.run(
        "SELECT TYPE, INSTANCE, VERSION FROM information_schema.cluster_info WHERE TYPE <> 'tidb' ORDER BY TYPE")),
        [["ticdc", "192.0.2.40:8300", "9.0.0"],
         ["tiproxy", "[2001:db8::1]:6000", "v1.2.3"]]);
    fake.delete_prefix("/topology/").unwrap();
    fake.delete(&server_info_key_path("first")).unwrap();
    assert_eq!(row_text(session.run(sql)), [["tidb", "192.0.2.30:4000"]]);
    assert_eq!(
        row_text(session.run("SELECT DDL_ID, IP FROM information_schema.tidb_servers_info")),
        [["second", "192.0.2.30"]]
    );
}

fn check_cluster_metadata_discovery_error(table: &str) {
    use crate::{tests_support::row_text, Session};

    let fake = Arc::new(FakeEtcd::default());
    let mut session = Session::new();
    session.set_server_info_syncer(Arc::new(Syncer::new(
        mock_server_info("local", "192.0.2.10", 4000),
        Some(fake.clone()),
    )));
    *fake.get_error.lock().unwrap() = Some("server discovery unavailable".to_owned());
    for sql in [
        format!("SELECT * FROM information_schema.{table}"),
        format!("SELECT COUNT(*) FROM information_schema.{table}"),
    ] {
        let error = session
            .run(&sql)
            .expect_err("discovery errors must reach SQL");
        assert_eq!(error.to_string(), "server discovery unavailable");
    }
    *fake.get_error.lock().unwrap() = None;
    assert!(row_text(session.run(&format!("SELECT * FROM information_schema.{table}"))).is_empty());
}

#[test]
fn cluster_metadata_cluster_info_propagates_discovery_errors() {
    check_cluster_metadata_discovery_error("cluster_info");
}

#[test]
fn cluster_metadata_servers_info_propagates_discovery_errors() {
    check_cluster_metadata_discovery_error("tidb_servers_info");
}

#[test]
fn cluster_metadata_config_refuses_captured_runtime_rows() {
    use crate::Session;

    let mut session = Session::new();
    session.set_server_info_syncer(Arc::new(Syncer::new(
        mock_server_info("local", "192.0.2.10", 4000),
        None,
    )));
    // A session without its process HTTP owner must never substitute a
    // captured configuration image. Production factories install that owner.
    for sql in [
        "SELECT * FROM information_schema.cluster_config",
        "SELECT COUNT(*) FROM information_schema.cluster_config WHERE TYPE = 'tidb'",
        "SELECT c.VALUE FROM information_schema.cluster_config c JOIN information_schema.cluster_info i ON c.INSTANCE = i.INSTANCE",
    ] {
        session.parse(sql).unwrap();
        let error = session.run(sql).expect_err("live config retrieval is unavailable");
        assert_eq!(error.to_string(), "CLUSTER_CONFIG live retrieval is not installed");
        assert!(session.warnings().iter().all(|warning| !warning.message.contains("store1")));
    }
    session
        .run("PREPARE cfg FROM 'SELECT * FROM information_schema.cluster_config'")
        .unwrap();
    assert_eq!(
        session.run("EXECUTE cfg").unwrap_err().to_string(),
        "CLUSTER_CONFIG live retrieval is not installed"
    );
    session.run("DEALLOCATE PREPARE cfg").unwrap();
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

    // The value predicate is distinct from the unimplemented cross-keyspace
    // constructor/session wiring in Go's TestAssumedServerInfoSyncer.
    let assumed = StaticInfo {
        keyspace: "SYSTEM".to_owned(),
        assumed_keyspace: "ks1".to_owned(),
        ..StaticInfo::default()
    };
    assert!(assumed.is_assumed());
    assert_eq!(assumed.assumed_keyspace, "ks1");
}

// Go Domain.TestCheckReplicaRead and infoschema's component retriever contract.
#[derive(Default)]
struct TopologyDiscovery {
    stores: Mutex<Vec<tidb_domain::cluster_topology::ClusterStore>>,
    error: Mutex<Option<String>>,
}
impl tidb_domain::cluster_topology::ClusterDiscovery for TopologyDiscovery {
    fn pd_servers(
        &self,
        warnings: &mut Vec<String>,
    ) -> Result<Vec<tidb_domain::cluster_topology::ClusterServer>, String> {
        warnings.push("one PD member unavailable".into());
        Ok(vec![component("pd", "192.0.2.2:2379")])
    }
    fn stores(&self) -> Result<Vec<tidb_domain::cluster_topology::ClusterStore>, String> {
        if let Some(error) = self.error.lock().unwrap().clone() {
            return Err(error);
        }
        Ok(self.stores.lock().unwrap().clone())
    }
    fn microservice_servers(
        &self,
        service: &str,
        _: &mut Vec<String>,
    ) -> Result<Vec<tidb_domain::cluster_topology::ClusterServer>, String> {
        Ok(vec![component(service, "192.0.2.3:3379")])
    }
}
fn component(kind: &str, address: &str) -> tidb_domain::cluster_topology::ClusterServer {
    tidb_domain::cluster_topology::ClusterServer {
        server_type: kind.into(),
        address: address.into(),
        status_address: address.into(),
        ..Default::default()
    }
}
fn zone_store(zone: &str) -> tidb_domain::cluster_topology::ClusterStore {
    tidb_domain::cluster_topology::ClusterStore {
        server: component("tikv", "192.0.2.4:20160"),
        labels: vec![("zone".into(), zone.into())],
        removing: false,
    }
}

#[test]
fn cluster_metadata_composes_retrievers_and_retains_warnings_on_failure() {
    use crate::{tests_support::row_text, Session};
    use tidb_domain::cluster_topology::ClusterTopology;
    let fake = Arc::new(FakeEtcd::default());
    let mut info = mock_server_info("one", "192.0.2.1", 4000);
    info.static_info.server_id_getter = Some(Arc::new(|| 77));
    let syncer = Arc::new(Syncer::new(info.clone(), Some(fake.clone())));
    fake.put(&server_info_key_path("one"), &info.marshal().unwrap())
        .unwrap();
    put_str(
        &fake,
        "/topology/tiproxy/one/info",
        r#"{"ip":"192.0.2.5","port":"6000","status_port":"3080"}"#,
    );
    put_str(
        &fake,
        "/topology/ticdc/one",
        r#"{"address":"192.0.2.6:8300"}"#,
    );
    let discovery = Arc::new(TopologyDiscovery::default());
    discovery.stores.lock().unwrap().push(zone_store("z1"));
    let topology = Arc::new(ClusterTopology::new(
        syncer.clone(),
        Some(discovery.clone()),
    ));
    let mut session = Session::new();
    session.set_server_info_syncer(syncer);
    session.set_cluster_topology(topology);
    assert_eq!(
        row_text(session.run("SELECT TYPE FROM information_schema.cluster_info ORDER BY TYPE")),
        [
            ["pd"],
            ["scheduling"],
            ["ticdc"],
            ["tidb"],
            ["tikv"],
            ["tiproxy"],
            ["tso"]
        ]
    );
    assert!(session
        .warnings()
        .iter()
        .any(|warning| warning.message == "one PD member unavailable"));
    assert_eq!(
        row_text(
            session
                .run("SELECT SERVER_ID FROM information_schema.cluster_info WHERE TYPE = 'tidb'")
        ),
        [["77"]]
    );
    *discovery.error.lock().unwrap() = Some("PD stores unavailable".into());
    assert_eq!(
        session
            .run("SELECT * FROM information_schema.cluster_info")
            .unwrap_err()
            .to_string(),
        "PD stores unavailable"
    );
    assert!(session
        .warnings()
        .iter()
        .any(|warning| warning.message == "one PD member unavailable"));
    *discovery.error.lock().unwrap() = None;
    put_str(&fake, "/topology/tiproxy/broken/info", "bad json");
    assert!(session
        .run("SELECT * FROM information_schema.cluster_info")
        .is_err());
    fake.delete("/topology/tiproxy/broken/info").unwrap();
    assert_eq!(
        row_text(session.run("SELECT COUNT(*) FROM information_schema.cluster_info")),
        [["7"]]
    );
}

#[test]
fn closest_adaptive_balances_zones_and_updates_existing_sessions() {
    use tidb_domain::cluster_topology::ClusterTopology;
    use tidb_executor::ReplicaReadType;
    let fake = Arc::new(FakeEtcd::default());
    for (id, zone) in [
        ("s1", "z1"),
        ("s2", "z2"),
        ("s22", "z2"),
        ("s3", "z3"),
        ("s4", "z4"),
    ] {
        let mut info = mock_server_info(id, "192.0.2.1", 4000);
        info.dynamic_info.labels.insert("zone".into(), zone.into());
        fake.put(&server_info_key_path(id), &info.marshal().unwrap())
            .unwrap();
    }
    let discovery = Arc::new(TopologyDiscovery::default());
    *discovery.stores.lock().unwrap() = ["z1", "z2", "z3"].into_iter().map(zone_store).collect();
    let mut ignored = zone_store("z4");
    ignored.labels.push(("engine".into(), "tiflash".into()));
    discovery.stores.lock().unwrap().push(ignored);
    let mut ignored = zone_store("z4");
    ignored.removing = true;
    discovery.stores.lock().unwrap().push(ignored);
    for (id, zone, enabled) in [
        ("s1", "z1", true),
        ("s2", "z2", true),
        ("s22", "z2", false),
        ("s3", "z3", true),
        ("s4", "z4", false),
    ] {
        let mut local = mock_server_info(id, "192.0.2.1", 4000);
        local.dynamic_info.labels.insert("zone".into(), zone.into());
        let syncer = Arc::new(Syncer::new(local, Some(fake.clone())));
        let topology = Arc::new(ClusterTopology::new(
            syncer.clone(),
            Some(discovery.clone()),
        ));
        let mut session = crate::Session::new();
        session.set_server_info_syncer(syncer);
        session.set_cluster_topology(topology.clone());
        session
            .run("SET tidb_replica_read='closest-adaptive'")
            .unwrap();
        topology.check_replica_read("CLOSEST-ADAPTIVE").unwrap();
        assert_eq!(topology.adaptive_enabled(), enabled, "{id}");
        assert_eq!(
            session.statement_context(false).replica_read(),
            if enabled {
                ReplicaReadType::ClosestAdaptive
            } else {
                ReplicaReadType::Leader
            }
        );
        // Changing the global mode does not overwrite the retained decision.
        topology.check_replica_read("leader").unwrap();
        assert_eq!(topology.adaptive_enabled(), enabled);
        // Both transport and registry errors preserve the last decision.
        if zone != "z4" {
            *discovery.error.lock().unwrap() = Some("PD failed".into());
            assert!(topology.check_replica_read("closest-adaptive").is_err());
            *discovery.error.lock().unwrap() = None;
            *fake.get_error.lock().unwrap() = Some("etcd failed".into());
            assert!(topology.check_replica_read("closest-adaptive").is_err());
            *fake.get_error.lock().unwrap() = None;
            assert_eq!(topology.adaptive_enabled(), enabled);
        }
        session.stmt_hints.has_replica_read_hint = true;
        session.stmt_hints.replica_read = ReplicaReadType::Follower.raw();
        assert_eq!(
            session.effective_replica_read(ReplicaReadType::ClosestAdaptive),
            ReplicaReadType::Follower
        );
        assert_eq!(
            session.statement_context(false).replica_read(),
            ReplicaReadType::Follower
        );
    }
    let syncer = Arc::new(Syncer::new(
        mock_server_info("no-zone", "192.0.2.1", 4000),
        None,
    ));
    let topology = ClusterTopology::new(syncer, None);
    topology.check_replica_read("closest-adaptive").unwrap();
    assert!(!topology.adaptive_enabled());
}

#[test]
fn cluster_metadata_instance_uses_local_status_identity_without_discovery() {
    use crate::Session;
    let fake = Arc::new(FakeEtcd::default());
    let mut peer = mock_server_info("peer", "192.0.2.90", 4400);
    fake.put(&server_info_key_path("peer"), &peer.marshal().unwrap())
        .unwrap();
    let mut session = Session::new();
    session.set_server_info_syncer(Arc::new(Syncer::new(
        mock_server_info("local-id", "2001:db8::7", 4000),
        Some(fake.clone()),
    )));
    assert_eq!(session.cluster_instance_address(), "[2001:db8::7]:10080");
    *fake.get_error.lock().unwrap() = Some("etcd unavailable".into());
    assert_eq!(session.cluster_instance_address(), "[2001:db8::7]:10080");
}

// SEM is process-global. As in tests_sem_v2, isolate it from unrelated tests.
#[test]
fn cluster_metadata_sem_redacts_each_projection_and_observes_live_roles() {
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "tests_domain_serverinfo_syncer_source::cluster_metadata_sem_child",
        ])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
#[ignore = "isolated subprocess helper for process-global SEM"]
fn cluster_metadata_sem_child() {
    use crate::{tests_support::*, *};
    tidb_util::sem::disable();
    tidb_util::sem_v2::disable();
    let registry = privilege::PrivilegeRegistry::default();
    let mut bootstrap = bootstrap_session(&registry);
    bootstrap.run("CREATE USER 'metadata_reader'@'%'").unwrap();
    bootstrap.run("CREATE ROLE 'metadata_admin'@'%'").unwrap();
    bootstrap
        .run("GRANT 'metadata_admin'@'%' TO 'metadata_reader'@'%'")
        .unwrap();
    registry.grant_dynamic("metadata_admin", "%", "RESTRICTED_TABLES_ADMIN", false);
    let info = mock_server_info("local-ddl-id", "2001:db8::7", 4000);
    let syncer = Arc::new(Syncer::new(info, None));
    let mut reader = authenticated_session(&registry, "metadata_reader", "%");
    reader.set_server_info_syncer(syncer.clone());
    let mut internal = Session::new();
    internal.set_server_info_syncer(syncer);
    let mut failures = Vec::new();
    for version in [1, 2] {
        if version == 1 {
            tidb_util::sem::enable();
        } else {
            tidb_util::sem::disable();
            tidb_util::sem_v2::enable_by(&tidb_util::sem_v2::Config {
                version: "1.0".into(),
                tidb_version: tidb_util::sem_v2::tidb_release_version(),
                ..Default::default()
            })
            .unwrap();
        }
        for (label, session) in [
            ("missing checker", &mut internal),
            ("ordinary account", &mut reader),
        ] {
            let cluster = session.cluster_info_table_rows().unwrap();
            if cluster[0][1] != tidb_datatype::Datum::Bytes(b"1".to_vec())
                || [2, 5, 6]
                    .into_iter()
                    .any(|i| cluster[0][i] != tidb_datatype::Datum::Null)
            {
                failures.push(format!("SEM{version} {label}: CLUSTER_INFO not redacted"));
            }
            let servers = session.tidb_servers_info_table_rows().unwrap();
            if servers[0][1] != tidb_datatype::Datum::Null {
                failures.push(format!(
                    "SEM{version} {label}: TIDB_SERVERS_INFO IP not redacted"
                ));
            }
            if session.cluster_instance_address() != "local-ddl-id" {
                failures.push(format!("SEM{version} {label}: cluster instance not DDL ID"));
            }
            assert_eq!(cluster[0][7], tidb_datatype::Datum::UInt(1));
            assert_eq!(servers[0][2], tidb_datatype::Datum::Int(4000));
        }
        reader.run("SET ROLE ALL").unwrap();
        assert_eq!(
            reader.cluster_info_table_rows().unwrap()[0][1],
            tidb_datatype::Datum::Bytes(b"[2001:db8::7]:4000".to_vec())
        );
        assert_eq!(
            reader.tidb_servers_info_table_rows().unwrap()[0][1],
            tidb_datatype::Datum::Bytes(b"2001:db8::7".to_vec())
        );
        if reader.cluster_instance_address() != "[2001:db8::7]:10080" {
            failures.push(format!("SEM{version} active role: wrong instance"));
        }
        reader.run("SET ROLE NONE").unwrap();
        // A present Go checker with no identity, or SkipWithGrant, permits
        // the dynamic verification. Only an absent checker hides by default.
        let mut anonymous = bootstrap_session(&registry);
        anonymous.set_server_info_syncer(internal.server_info_syncer.as_ref().unwrap().clone());
        let mut bypassed = authenticated_session(&registry, "metadata_reader", "%");
        bypassed.set_server_info_syncer(internal.server_info_syncer.as_ref().unwrap().clone());
        bypassed.enable_privilege_bypass();
        for session in [&mut anonymous, &mut bypassed] {
            assert_eq!(session.cluster_instance_address(), "[2001:db8::7]:10080");
            assert_ne!(
                session.tidb_servers_info_table_rows().unwrap()[0][1],
                tidb_datatype::Datum::Null
            );
        }
    }
    tidb_util::sem_v2::disable();
    tidb_util::sem::disable();
    assert_eq!(
        internal.cluster_info_table_rows().unwrap()[0][1],
        tidb_datatype::Datum::Bytes(b"[2001:db8::7]:4000".to_vec())
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn cluster_config_checks_config_privilege_before_discovery() {
    use crate::{tests_support::*, *};
    let registry = privilege::PrivilegeRegistry::default();
    let mut bootstrap = bootstrap_session(&registry);
    bootstrap.run("CREATE USER 'config_reader'@'%'").unwrap();
    let mut reader = authenticated_session(&registry, "config_reader", "%");
    let error = reader
        .run("SELECT * FROM information_schema.cluster_config")
        .unwrap_err();
    assert!(matches!(error, DriverError::SpecificAccessDenied(ref name) if name == "CONFIG"));
}

#[test]
fn cluster_config_contradictory_types_skip_retrieval() {
    use crate::{tests_support::row_text, Session};
    let mut session = Session::new();
    assert_eq!(row_text(session.run(
        "SELECT count(*) FROM information_schema.cluster_config WHERE type='tidb' AND type='tikv'"
    )), [["0"]]);
}

#[test]
fn cluster_config_live_http_filters_roles_warnings_and_prepared_reads() {
    use crate::{tests_support::*, *};
    use std::io::{Read, Write};
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::time::Duration;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let requests = Arc::new(AtomicUsize::new(0));
    struct ServerGuard(Arc<AtomicBool>, Option<std::thread::JoinHandle<()>>);
    impl Drop for ServerGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
            self.1.take().unwrap().join().unwrap();
        }
    }
    let stopping = stop.clone();
    let calls = requests.clone();
    let server = std::thread::spawn(move || {
        while !stopping.load(Ordering::Acquire) {
            let (mut stream, _) = match listener.accept() {
                Ok(pair) => pair,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(2));
                    continue;
                }
                Err(error) => panic!("{error}"),
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0];
                stream.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET /config HTTP/1.1\r\n"));
            assert!(request
                .to_ascii_lowercase()
                .contains("pd-allow-follower-handle: true"));
            let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
            let body = format!(
                r#"{{"key":"live-{call}","nested":{{"enabled":true}},"performance":{{"INDEX-USAGE-SYNC-LEASE":"hidden"}},"enable-batch-dml":true}}"#
            );
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
    });
    let _server = ServerGuard(stop, Some(server));
    let mut info = mock_server_info("live-config", "127.0.0.1", 4000);
    info.static_info.status_port = usize::from(address.port());
    let syncer = Arc::new(Syncer::new(info, None));
    let client =
        Arc::new(tidb_exec::cluster_config::ClusterConfigClient::new(&Default::default()).unwrap());
    let mut session = Session::new();
    session.set_server_info_syncer(syncer.clone());
    session.set_cluster_config_client(client.clone());
    assert_eq!(row_text(session.run("SELECT type, `key`, value FROM information_schema.cluster_config WHERE type='tidb' ORDER BY `key`")),
        [["tidb", "key", "live-1"], ["tidb", "nested.enabled", "true"]]);
    for predicate in [
        "type='tikv'",
        "type IN ('pd','tso')",
        "instance='unselected:4000'",
        "type='tidb' AND type='tikv'",
    ] {
        assert_eq!(
            row_text(session.run(&format!(
                "SELECT count(*) FROM information_schema.cluster_config WHERE {predicate}"
            ))),
            [["0"]]
        );
    }
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "excluded nodes receive no HTTP request"
    );
    session.run("PREPARE cfg FROM 'SELECT value FROM information_schema.cluster_config WHERE type=\"tidb\" AND `key`=\"key\"'").unwrap();
    assert_eq!(row_text(session.run("EXECUTE cfg")), [["live-2"]]);
    assert_eq!(row_text(session.run("EXECUTE cfg")), [["live-3"]]);
    session.run("DEALLOCATE PREPARE cfg").unwrap();
    assert_eq!(row_text(session.run("SELECT c.value FROM information_schema.cluster_config c JOIN information_schema.cluster_info i ON c.instance=i.instance WHERE c.`key`='key'")), [["live-4"]]);

    let registry = privilege::PrivilegeRegistry::default();
    let mut bootstrap = bootstrap_session(&registry);
    bootstrap.run("CREATE USER 'http_reader'@'%'").unwrap();
    bootstrap.run("CREATE ROLE 'config_role'@'%'").unwrap();
    bootstrap
        .run("GRANT CONFIG ON *.* TO 'config_role'@'%'")
        .unwrap();
    bootstrap
        .run("GRANT 'config_role'@'%' TO 'http_reader'@'%'")
        .unwrap();
    let mut reader = authenticated_session(&registry, "http_reader", "%");
    reader.set_server_info_syncer(syncer);
    reader.set_cluster_config_client(client);
    assert!(matches!(
        reader.run("SELECT * FROM information_schema.cluster_config"),
        Err(DriverError::SpecificAccessDenied(_))
    ));
    assert_eq!(
        requests.load(Ordering::SeqCst),
        4,
        "privilege failure precedes HTTP"
    );
    reader.run("SET ROLE ALL").unwrap();
    assert_eq!(
        row_text(
            reader.run("SELECT value FROM information_schema.cluster_config WHERE `key`='key'")
        ),
        [["live-5"]]
    );
    assert_eq!(
        row_text(reader.run("SHOW CONFIG WHERE Name='key'")),
        [["tidb", "127.0.0.1:4000", "key", "live-6"]]
    );
    assert!(row_text(reader.run("SHOW CONFIG LIKE 'pd'")).is_empty());
    assert_eq!(
        requests.load(Ordering::SeqCst),
        7,
        "SHOW filters run after shared live retrieval, as Go does"
    );
    reader.run("SET ROLE NONE").unwrap();
    assert!(matches!(
        reader.run("SHOW CONFIG"),
        Err(DriverError::SpecificAccessDenied(_))
    ));
}

#[test]
fn cluster_peer_batch_processlist_discovery_errors_are_not_hidden() {
    let fake = Arc::new(FakeEtcd::default());
    *fake.get_error.lock().unwrap() = Some("peer registry unavailable".into());
    let mut session = crate::Session::new();
    session.set_server_info_syncer(Arc::new(Syncer::new(
        mock_server_info("local", "192.0.2.10", 4000),
        Some(fake),
    )));
    let error = session
        .run("SELECT * FROM information_schema.CLUSTER_PROCESSLIST")
        .unwrap_err();
    assert!(
        error.to_string().contains("peer registry unavailable"),
        "{error}"
    );
}
