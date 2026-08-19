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

//! Go `pkg/domain/serverinfo/syncer.go`, the SERVER-INFO half: this node's
//! own `/tidb/server/info/<uuid>` entry, published under an etcd lease so
//! it disappears when the node does, plus the reads and the stale-entry
//! cleanup its peers depend on.
//!
//! # The lease IS the liveness signal
//!
//! Go stores the info with `clientv3.WithLease(session.Lease())`: the key
//! lives exactly as long as the session keeps its lease alive, so a node
//! that dies -- OOM, `kill -9`, a partition outlasting the TTL -- has its
//! entry removed BY ETCD, with no peer having to notice. That is why
//! [`Syncer::store_server_info`] refuses to run without a session: a
//! leaseless PUT would leave an immortal entry claiming a dead node is up.
//!
//! # What rides here and what does not
//!
//! The TOPOLOGY half (`/topology/tidb/<ip:port>`, its own session and its
//! 30-second `ttl` refresh) is a separate session with the same shape and
//! follows on this track. `ServerInfoSyncLoop`'s min-start-ts reporting
//! needs `MinStartTSReporter`, which reaches into the session manager, and
//! waits on that seam.
//!
//! # Testing
//!
//! Go's `syncer_test.go` runs an EMBEDDED etcd cluster
//! (`integration.NewClusterV3`), which this tier has no counterpart for.
//! The etcd surface is therefore taken through [`EtcdOps`], and the tests
//! below drive a recording fake: the key paths, the lease lifecycle, the
//! marshal round trip, and the stale-entry rule are pinned exactly; what
//! a real etcd does with a revoked lease is etcd's own behavior.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::serverinfo::{ServerInfo, SERVER_INFORMATION_PATH};

/// Go `pkg/ddl/util.SessionTTL`: the server-info session's TTL, seconds.
pub const SESSION_TTL_SECONDS: i64 = 90;

/// The etcd calls this syncer makes, as a seam.
///
/// Every method mirrors one `clientv3` call Go's syncer issues. The
/// production implementation is `tidb_pd_client::etcd::EtcdClient`; the
/// tests below use a fake.
pub trait EtcdOps: Send + Sync {
    /// `Lease.LeaseGrant`, answering the granted lease id.
    fn lease_grant(&self, ttl_seconds: i64) -> Result<i64, String>;
    /// One `Lease.LeaseKeepAlive` round.
    fn lease_keep_alive_once(&self, lease: i64) -> Result<(), String>;
    /// `Lease.LeaseRevoke`.
    fn lease_revoke(&self, lease: i64) -> Result<(), String>;
    /// `KV.Put` with a lease attached.
    fn put_with_lease(&self, key: &str, value: &[u8], lease: i64) -> Result<(), String>;
    /// `KV.Range` over `[prefix, prefix+1)` -- `clientv3.WithPrefix()`.
    fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String>;
    /// `KV.DeleteRange` of one key.
    fn delete(&self, key: &str) -> Result<(), String>;
}

/// Go `serverInfoKeyPath`.
#[must_use]
pub fn server_info_key_path(id: &str) -> String {
    format!("{SERVER_INFORMATION_PATH}/{id}")
}

/// Go `Syncer`, the server-info half.
pub struct Syncer {
    etcd: Option<Arc<dyn EtcdOps>>,
    info: Mutex<ServerInfo>,
    server_info_path: String,
    /// The session's lease, or `None` before `new_session_and_store_server_info`.
    session: Mutex<Option<i64>>,
}

impl Syncer {
    /// Go `NewSyncer`. A `None` client is Go's `etcdCli == nil`: every
    /// etcd-touching method becomes a no-op and the reads answer from the
    /// local info alone, which is how a single-node deployment runs.
    #[must_use]
    pub fn new(info: ServerInfo, etcd: Option<Arc<dyn EtcdOps>>) -> Self {
        let server_info_path = server_info_key_path(&info.static_info.id);
        Self {
            etcd,
            info: Mutex::new(info),
            server_info_path,
            session: Mutex::new(None),
        }
    }

    /// The etcd key this node publishes itself under.
    #[must_use]
    pub fn server_info_path(&self) -> &str {
        &self.server_info_path
    }

    /// Go `GetLocalServerInfo`.
    #[must_use]
    pub fn local_server_info(&self) -> ServerInfo {
        self.info.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// The session's lease id, once one is held.
    #[must_use]
    pub fn session_lease(&self) -> Option<i64> {
        *self.session.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `NewSessionAndStoreServerInfo`: clean up whatever a previous
    /// instance at this address left behind, take a session, publish.
    pub fn new_session_and_store_server_info(&self) -> Result<(), String> {
        let Some(etcd) = self.etcd.as_ref() else {
            return Ok(());
        };
        self.cleanup_stale_server_info();
        let lease = etcd.lease_grant(SESSION_TTL_SECONDS)?;
        *self.session.lock().unwrap_or_else(|e| e.into_inner()) = Some(lease);
        self.store_server_info()
    }

    /// Go `StoreServerInfo`: the marshaled info PUT under the session's
    /// lease. `Marshal` reads the server-id getter first, so the published
    /// bytes carry this node's CURRENT server id.
    pub fn store_server_info(&self) -> Result<(), String> {
        let Some(etcd) = self.etcd.as_ref() else {
            return Ok(());
        };
        let Some(lease) = self.session_lease() else {
            return Err("[info-syncer] no session to store server info under".to_owned());
        };
        let bytes = {
            let mut info = self.info.lock().unwrap_or_else(|e| e.into_inner());
            info.marshal().map_err(|error| error.to_string())?
        };
        etcd.put_with_lease(&self.server_info_path, &bytes, lease)
    }

    /// Go `Restart`: a fresh session and a republish, which is what a
    /// session whose lease expired needs.
    pub fn restart(&self) -> Result<(), String> {
        self.new_session_and_store_server_info()
    }

    /// One keepalive round for the session's lease.
    pub fn keep_alive_once(&self) -> Result<(), String> {
        let Some(etcd) = self.etcd.as_ref() else {
            return Ok(());
        };
        let Some(lease) = self.session_lease() else {
            return Ok(());
        };
        etcd.lease_keep_alive_once(lease)
    }

    /// Go `UpdateServerLabel`: merge the labels into the dynamic info and
    /// republish, but ONLY when something actually changed -- an
    /// unchanged label set writes nothing, and the local info is updated
    /// only after the PUT succeeded.
    pub fn update_server_label(&self, labels: &HashMap<String, String>) -> Result<(), String> {
        if self.etcd.is_none() {
            return Ok(());
        }
        let mut candidate = self.local_server_info();
        let mut changed = false;
        for (key, value) in labels {
            if candidate.dynamic_info.labels.get(key) != Some(value) {
                changed = true;
                candidate
                    .dynamic_info
                    .labels
                    .insert(key.clone(), value.clone());
            }
        }
        if !changed {
            return Ok(());
        }
        let Some(lease) = self.session_lease() else {
            return Err("[info-syncer] no session to store server info under".to_owned());
        };
        let bytes = candidate.marshal().map_err(|error| error.to_string())?;
        self.etcd
            .as_ref()
            .expect("checked above")
            .put_with_lease(&self.server_info_path, &bytes, lease)?;
        self.info
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .dynamic_info = candidate.dynamic_info;
        Ok(())
    }

    /// Go `GetAllServerInfo`: every peer's entry, keyed by server id.
    /// Without a client the map is this node alone, which is Go's
    /// single-node answer.
    pub fn all_server_info(&self) -> Result<HashMap<String, ServerInfo>, String> {
        let Some(etcd) = self.etcd.as_ref() else {
            let info = self.local_server_info();
            return Ok(HashMap::from([(info.static_info.id.clone(), info)]));
        };
        let entries = etcd.get_prefix(SERVER_INFORMATION_PATH)?;
        decode_entries(&entries)
    }

    /// Go `GetServerInfoByID`: the local info for this node's own id (no
    /// etcd read at all), otherwise the peer's entry -- and a MISSING peer
    /// is an error, not a `None`, exactly as Go's `get %s failed`.
    pub fn server_info_by_id(&self, id: &str) -> Result<ServerInfo, String> {
        let local = self.local_server_info();
        let Some(etcd) = self.etcd.as_ref() else {
            return Ok(local);
        };
        if id == local.static_info.id {
            return Ok(local);
        }
        let key = server_info_key_path(id);
        let entries = etcd.get_prefix(&key)?;
        decode_entries(&entries)?
            .remove(id)
            .ok_or_else(|| format!("[info-syncer] get {key} failed"))
    }

    /// Go `RemoveServerInfo`: the graceful-shutdown delete. Go logs a
    /// failure and moves on -- the lease would have expired the key
    /// anyway -- so this answers `()` either way.
    pub fn remove_server_info(&self) {
        let Some(etcd) = self.etcd.as_ref() else {
            return;
        };
        let _ = etcd.delete(&self.server_info_path);
        if let Some(lease) = self.session_lease() {
            let _ = etcd.lease_revoke(lease);
        }
    }

    /// Go `cleanupStaleServerAndOwnerInfo`, the server-info half: an entry
    /// from a PREVIOUS instance at this same IP+Port -- one that died
    /// without cleanup -- is deleted so it stops appearing as a live peer.
    /// Best-effort throughout: any failure leaves startup running, since
    /// the stale entry's own lease expires on its own.
    ///
    /// The DDL-owner-key half (`owner.DeleteOwnerKeyByID`) waits on the
    /// owner-election port.
    fn cleanup_stale_server_info(&self) {
        let Some(etcd) = self.etcd.as_ref() else {
            return;
        };
        let info = self.local_server_info();
        let Ok(entries) = etcd.get_prefix(SERVER_INFORMATION_PATH) else {
            return;
        };
        let Ok(all) = decode_entries(&entries) else {
            return;
        };
        for (id, stale) in all {
            if id == info.static_info.id {
                continue;
            }
            if stale.static_info.ip != info.static_info.ip
                || stale.static_info.port != info.static_info.port
            {
                continue;
            }
            let _ = etcd.delete(&server_info_key_path(&id));
        }
    }
}

/// Go `getInfo`'s decode loop: each value unmarshaled into a `ServerInfo`
/// and keyed by the DECODED id, not by the key's suffix.
fn decode_entries(entries: &[(String, Vec<u8>)]) -> Result<HashMap<String, ServerInfo>, String> {
    let mut all = HashMap::with_capacity(entries.len());
    for (key, value) in entries {
        let mut info = ServerInfo::default();
        info.unmarshal(value)
            .map_err(|error| format!("[info-syncer] decode {key} failed: {error}"))?;
        all.insert(info.static_info.id.clone(), info);
    }
    Ok(all)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serverinfo::StaticInfo;

    #[derive(Default)]
    struct FakeEtcd {
        keys: Mutex<Vec<(String, Vec<u8>, i64)>>,
        leases: Mutex<Vec<i64>>,
        revoked: Mutex<Vec<i64>>,
        keepalives: Mutex<Vec<i64>>,
        next_lease: Mutex<i64>,
    }

    impl FakeEtcd {
        fn value(&self, key: &str) -> Option<(Vec<u8>, i64)> {
            self.keys
                .lock()
                .unwrap()
                .iter()
                .find(|(stored, _, _)| stored == key)
                .map(|(_, value, lease)| (value.clone(), *lease))
        }

        fn seed(&self, key: &str, info: &mut ServerInfo) {
            let bytes = info.marshal().unwrap();
            self.keys.lock().unwrap().push((key.to_owned(), bytes, 0));
        }
    }

    impl EtcdOps for FakeEtcd {
        fn lease_grant(&self, _ttl_seconds: i64) -> Result<i64, String> {
            let mut next = self.next_lease.lock().unwrap();
            *next += 1;
            self.leases.lock().unwrap().push(*next);
            Ok(*next)
        }

        fn lease_keep_alive_once(&self, lease: i64) -> Result<(), String> {
            self.keepalives.lock().unwrap().push(lease);
            Ok(())
        }

        fn lease_revoke(&self, lease: i64) -> Result<(), String> {
            self.revoked.lock().unwrap().push(lease);
            Ok(())
        }

        fn put_with_lease(&self, key: &str, value: &[u8], lease: i64) -> Result<(), String> {
            let mut keys = self.keys.lock().unwrap();
            keys.retain(|(stored, _, _)| stored != key);
            keys.push((key.to_owned(), value.to_vec(), lease));
            Ok(())
        }

        fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
            Ok(self
                .keys
                .lock()
                .unwrap()
                .iter()
                .filter(|(key, _, _)| key.starts_with(prefix))
                .map(|(key, value, _)| (key.clone(), value.clone()))
                .collect())
        }

        fn delete(&self, key: &str) -> Result<(), String> {
            self.keys
                .lock()
                .unwrap()
                .retain(|(stored, _, _)| stored != key);
            Ok(())
        }
    }

    fn info_at(id: &str, ip: &str, port: u32) -> ServerInfo {
        ServerInfo {
            static_info: StaticInfo {
                id: id.to_owned(),
                ip: ip.to_owned(),
                port,
                ..StaticInfo::default()
            },
            ..ServerInfo::default()
        }
    }

    /// The key is Go's `ServerInformationPath/<uuid>`, and the info is
    /// PUT under the session's lease -- never leaseless.
    #[test]
    fn a_session_publishes_the_info_under_its_lease() {
        let etcd = Arc::new(FakeEtcd::default());
        let syncer = Syncer::new(info_at("uuid-1", "10.0.0.1", 4000), Some(etcd.clone()));
        assert_eq!(syncer.server_info_path(), "/tidb/server/info/uuid-1");
        syncer.new_session_and_store_server_info().unwrap();
        let lease = syncer.session_lease().expect("a session was taken");
        let (value, stored_lease) = etcd.value("/tidb/server/info/uuid-1").expect("published");
        assert_eq!(stored_lease, lease, "the entry rides the session's lease");
        let mut decoded = ServerInfo::default();
        decoded.unmarshal(&value).unwrap();
        assert_eq!(decoded.static_info.id, "uuid-1");
        assert_eq!(decoded.static_info.port, 4000);
    }

    /// Without a session there is nothing to attach: a leaseless entry
    /// would outlive the node it claims is running, so the store refuses.
    #[test]
    fn storing_without_a_session_refuses() {
        let etcd = Arc::new(FakeEtcd::default());
        let syncer = Syncer::new(info_at("uuid-1", "10.0.0.1", 4000), Some(etcd.clone()));
        assert!(syncer.store_server_info().is_err());
        assert!(etcd.value("/tidb/server/info/uuid-1").is_none());
    }

    /// Go's stale rule: a previous instance at the SAME IP+Port is
    /// deleted at startup; a peer at another address is untouched, and so
    /// is an entry carrying this node's own id.
    #[test]
    fn startup_cleans_only_the_stale_entry_at_this_address() {
        let etcd = Arc::new(FakeEtcd::default());
        etcd.seed(
            "/tidb/server/info/old-uuid",
            &mut info_at("old-uuid", "10.0.0.1", 4000),
        );
        etcd.seed(
            "/tidb/server/info/peer",
            &mut info_at("peer", "10.0.0.2", 4000),
        );
        etcd.seed(
            "/tidb/server/info/other-port",
            &mut info_at("other-port", "10.0.0.1", 4001),
        );
        let syncer = Syncer::new(info_at("uuid-1", "10.0.0.1", 4000), Some(etcd.clone()));
        syncer.new_session_and_store_server_info().unwrap();
        assert!(etcd.value("/tidb/server/info/old-uuid").is_none());
        assert!(etcd.value("/tidb/server/info/peer").is_some());
        assert!(etcd.value("/tidb/server/info/other-port").is_some());
        assert!(etcd.value("/tidb/server/info/uuid-1").is_some());
    }

    /// `GetAllServerInfo` keys by the DECODED id, and `GetServerInfoByID`
    /// answers the local info without reading etcd while a missing peer
    /// is an error.
    #[test]
    fn reads_key_by_decoded_id_and_error_on_a_missing_peer() {
        let etcd = Arc::new(FakeEtcd::default());
        etcd.seed(
            "/tidb/server/info/peer",
            &mut info_at("peer", "10.0.0.2", 4000),
        );
        let syncer = Syncer::new(info_at("uuid-1", "10.0.0.1", 4000), Some(etcd.clone()));
        syncer.new_session_and_store_server_info().unwrap();

        let all = syncer.all_server_info().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all["peer"].static_info.ip, "10.0.0.2");
        assert_eq!(all["uuid-1"].static_info.ip, "10.0.0.1");

        assert_eq!(
            syncer.server_info_by_id("uuid-1").unwrap().static_info.port,
            4000
        );
        assert_eq!(
            syncer.server_info_by_id("peer").unwrap().static_info.ip,
            "10.0.0.2"
        );
        assert!(syncer.server_info_by_id("absent").is_err());
    }

    /// An unchanged label set writes NOTHING; a changed one republishes
    /// and only then updates the local copy.
    #[test]
    fn a_label_update_writes_only_when_it_changes_something() {
        let etcd = Arc::new(FakeEtcd::default());
        let syncer = Syncer::new(info_at("uuid-1", "10.0.0.1", 4000), Some(etcd.clone()));
        syncer.new_session_and_store_server_info().unwrap();
        let (published, _) = etcd.value("/tidb/server/info/uuid-1").unwrap();

        syncer.update_server_label(&HashMap::new()).unwrap();
        assert_eq!(
            etcd.value("/tidb/server/info/uuid-1").unwrap().0,
            published,
            "an empty label set changes nothing"
        );

        syncer
            .update_server_label(&HashMap::from([("zone".to_owned(), "east".to_owned())]))
            .unwrap();
        let (updated, _) = etcd.value("/tidb/server/info/uuid-1").unwrap();
        assert_ne!(updated, published);
        let mut decoded = ServerInfo::default();
        decoded.unmarshal(&updated).unwrap();
        assert_eq!(decoded.dynamic_info.labels["zone"], "east");
        assert_eq!(
            syncer.local_server_info().dynamic_info.labels["zone"],
            "east",
            "the local copy follows the successful PUT"
        );

        // Re-applying the same value is not a change.
        let before = etcd.keys.lock().unwrap().len();
        syncer
            .update_server_label(&HashMap::from([("zone".to_owned(), "east".to_owned())]))
            .unwrap();
        assert_eq!(etcd.keys.lock().unwrap().len(), before);
    }

    /// Shutdown deletes the entry and revokes the lease; a restart takes
    /// a NEW lease and republishes under it.
    #[test]
    fn shutdown_removes_the_entry_and_restart_takes_a_new_lease() {
        let etcd = Arc::new(FakeEtcd::default());
        let syncer = Syncer::new(info_at("uuid-1", "10.0.0.1", 4000), Some(etcd.clone()));
        syncer.new_session_and_store_server_info().unwrap();
        let first = syncer.session_lease().unwrap();
        syncer.keep_alive_once().unwrap();
        assert_eq!(etcd.keepalives.lock().unwrap().as_slice(), &[first]);

        syncer.remove_server_info();
        assert!(etcd.value("/tidb/server/info/uuid-1").is_none());
        assert_eq!(etcd.revoked.lock().unwrap().as_slice(), &[first]);

        syncer.restart().unwrap();
        let second = syncer.session_lease().unwrap();
        assert_ne!(second, first);
        assert_eq!(etcd.value("/tidb/server/info/uuid-1").unwrap().1, second);
    }

    /// Go's `etcdCli == nil` deployment: nothing is published, and the
    /// reads answer from the local info alone.
    #[test]
    fn without_a_client_every_write_is_a_noop() {
        let syncer = Syncer::new(info_at("uuid-1", "10.0.0.1", 4000), None);
        syncer.new_session_and_store_server_info().unwrap();
        assert!(syncer.session_lease().is_none());
        syncer.store_server_info().unwrap();
        syncer.keep_alive_once().unwrap();
        syncer.remove_server_info();
        let all = syncer.all_server_info().unwrap();
        assert_eq!(all.len(), 1);
        assert!(all.contains_key("uuid-1"));
        assert_eq!(
            syncer.server_info_by_id("anything").unwrap().static_info.id,
            "uuid-1",
            "with no client every id answers the local info"
        );
    }
}
