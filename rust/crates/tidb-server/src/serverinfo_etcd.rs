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

//! The production binding of [`tidb_domain::serverinfo_syncer::EtcdOps`]
//! onto the real etcd client.
//!
//! The syncer states its etcd needs as a trait so `tidb-domain` stays free
//! of a transport dependency (and so its own tests can drive a fake). This
//! module is the one place that trait meets [`EtcdClient`]: every method is
//! a direct forward, with the client's typed error rendered as the string
//! the syncer logs.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use tidb_domain::serverinfo::{KEY_OP_DEFAULT_RETRY_CNT, KEY_OP_DEFAULT_TIMEOUT};
use tidb_domain::serverinfo_syncer::EtcdOps;
use tidb_domain::status_endpoint_claim::{ObservedStatusEndpointClaim, StatusEndpointClaimCreate};
use tidb_pd_client::EtcdClient;
use tidb_schemaver::etcd_syncer::{EtcdWatchOps, WatchStream};
use tidb_schemaver::{SharedRecv, WatchEvent};

/// [`EtcdOps`] over a connected [`EtcdClient`].
pub struct EtcdClientOps {
    client: Arc<EtcdClient>,
}

impl EtcdClientOps {
    /// Binds the syncer's etcd surface to this client.
    #[must_use]
    pub fn new(client: Arc<EtcdClient>) -> Self {
        Self { client }
    }
}

impl EtcdOps for EtcdClientOps {
    fn create_if_absent_with_lease(
        &self,
        key: &str,
        value: &[u8],
        lease: i64,
    ) -> Result<bool, String> {
        self.client
            .create_or_get_with_lease_with_timeout(
                key.as_bytes(),
                value,
                lease,
                Duration::from_secs(10),
            )
            .map(|outcome| outcome.created)
            .map_err(|error| error.to_string())
    }

    fn lease_keep_alive_ttl(&self, lease: i64) -> Result<i64, String> {
        match self.client.lease_keep_alive_once(lease) {
            Ok(ttl) => Ok(ttl),
            Err(tidb_pd_client::EtcdError::LeaseExpired) => Ok(0),
            Err(error) => Err(error.to_string()),
        }
    }
    fn lease_grant(&self, ttl_seconds: i64) -> Result<i64, String> {
        self.client
            .lease_grant(ttl_seconds)
            .map(|(id, _ttl)| id)
            .map_err(|error| error.to_string())
    }

    fn lease_keep_alive_once(&self, lease: i64) -> Result<(), String> {
        self.client
            .lease_keep_alive_once(lease)
            .map(|_ttl| ())
            .map_err(|error| error.to_string())
    }

    fn lease_revoke(&self, lease: i64) -> Result<(), String> {
        self.client
            .lease_revoke(lease)
            .map_err(|error| error.to_string())
    }

    fn put_with_lease(&self, key: &str, value: &[u8], lease: i64) -> Result<(), String> {
        self.client
            .put_with_lease_with_timeout(key.as_bytes(), value, lease, KEY_OP_DEFAULT_TIMEOUT)
            .map_err(|error| error.to_string())
    }

    fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
        self.client
            .get_prefix(prefix.as_bytes())
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|(key, value)| (String::from_utf8_lossy(&key).into_owned(), value))
                    .collect()
            })
            .map_err(|error| error.to_string())
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        self.client
            .delete_with_retry(
                key.as_bytes(),
                KEY_OP_DEFAULT_RETRY_CNT,
                KEY_OP_DEFAULT_TIMEOUT,
            )
            .map_err(|error| error.to_string())
    }

    fn put(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.client
            .put(key.as_bytes(), value)
            .map_err(|error| error.to_string())
    }

    fn delete_prefix(&self, prefix: &str) -> Result<(), String> {
        self.client
            .delete_prefix(prefix.as_bytes())
            .map_err(|error| error.to_string())
    }

    fn status_claim_try_create(
        &self,
        key: &str,
        value: &str,
        lease: i64,
    ) -> Result<StatusEndpointClaimCreate, String> {
        let outcome = self
            .client
            .create_or_get_with_lease(key.as_bytes(), value.as_bytes(), lease)
            .map_err(|error| error.to_string())?;
        if outcome.created {
            return Ok(StatusEndpointClaimCreate::Created);
        }
        let entry = outcome.existing.ok_or_else(|| {
            "advertised status endpoint claim disappeared while reading its owner".to_owned()
        })?;
        Ok(StatusEndpointClaimCreate::Existing(observed_claim(entry)))
    }

    fn status_claim_reattach(
        &self,
        key: &str,
        value: &str,
        expected_mod_revision: i64,
        lease: i64,
    ) -> Result<bool, String> {
        self.client
            .compare_and_put_with_lease(
                key.as_bytes(),
                expected_mod_revision,
                value.as_bytes(),
                lease,
            )
            .map_err(|error| error.to_string())
    }

    fn status_claim_remove(&self, key: &str, value: &str, lease: i64) -> Result<(), String> {
        let entries = self
            .client
            .get_prefix_metadata(key.as_bytes())
            .map_err(|error| error.to_string())?;
        let Some(entry) = entries
            .into_iter()
            .find(|entry| entry.key == key.as_bytes())
        else {
            return Ok(());
        };
        if entry.value != value.as_bytes() || entry.lease != lease {
            return Ok(());
        }
        self.client
            .delete_if_mod_revision(key.as_bytes(), entry.mod_revision)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

fn observed_claim(entry: tidb_pd_client::EtcdKeyValue) -> ObservedStatusEndpointClaim {
    ObservedStatusEndpointClaim {
        id: String::from_utf8_lossy(&entry.value).into_owned(),
        lease: entry.lease,
        mod_revision: entry.mod_revision,
    }
}

impl EtcdWatchOps for EtcdClientOps {
    fn get_prefix_with_rev(&self, prefix: &str) -> Result<(Vec<(String, Vec<u8>)>, i64), String> {
        self.client
            .get_prefix_metadata_with_revision(prefix.as_bytes())
            .map(|(entries, revision)| {
                (
                    entries
                        .into_iter()
                        .map(|entry| {
                            (
                                String::from_utf8_lossy(&entry.key).into_owned(),
                                entry.value,
                            )
                        })
                        .collect(),
                    revision,
                )
            })
            .map_err(|error| error.to_string())
    }

    fn get_with_mod_revision(&self, key: &str) -> Result<(Option<Vec<u8>>, i64), String> {
        self.client
            .get_prefix_metadata(key.as_bytes())
            .map(|entries| {
                entries
                    .into_iter()
                    .find(|entry| entry.key == key.as_bytes())
                    .map_or((None, 0), |entry| (Some(entry.value), entry.mod_revision))
            })
            .map_err(|error| error.to_string())
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_mod_revision: i64,
        value: &[u8],
    ) -> Result<bool, String> {
        self.client
            .compare_and_put(key.as_bytes(), expected_mod_revision, value)
            .map_err(|error| error.to_string())
    }

    fn put_if_not_exists(&self, key: &str, value: &[u8]) -> Result<bool, String> {
        self.client
            .create(key.as_bytes(), value)
            .map_err(|error| error.to_string())
    }

    fn watch(
        &self,
        key: &str,
        start_revision: i64,
        with_prefix: bool,
        require_leader: bool,
    ) -> Result<WatchStream, String> {
        // Go attaches `WithRequireLeader` through gRPC header metadata. The
        // etcd-client crate exposes no metadata hook for watch requests, so
        // the flag is accepted here but not yet enforceable in transport;
        // the watch still fails over via its ordinary error paths.
        let _ = require_leader;
        let (sender, receiver) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let canceled = Arc::clone(&stop);
        let on_response = move |response: &tidb_pd_client::EtcdWatchResponse| {
            if response.canceled {
                let message = if response.cancel_reason.is_empty() {
                    format!(
                        "watch canceled at compact revision {}",
                        response.compact_revision
                    )
                } else {
                    response.cancel_reason.clone()
                };
                let _ = sender.send(Err(message));
                return;
            }
            for event in &response.events {
                let _ = sender.send(Ok(WatchEvent {
                    key: String::from_utf8_lossy(&event.key).into_owned(),
                    value: event.value.clone(),
                    deleted: event.deleted,
                }));
            }
        };
        let watcher = if with_prefix {
            self.client.watch_prefix_responses(
                key.as_bytes(),
                start_revision,
                move || canceled.load(Ordering::Acquire),
                on_response,
            )
        } else {
            self.client.watch_key_responses(
                key.as_bytes(),
                start_revision,
                move || canceled.load(Ordering::Acquire),
                on_response,
            )
        }
        .map_err(|error| error.to_string())?;
        let thread_stop = Arc::clone(&stop);
        std::thread::Builder::new()
            .name("schemaver-etcd-watch".to_owned())
            .spawn(move || {
                // This thread only keeps the watcher alive until `stop`;
                // 100 ms slices bound the stop latency without waking a
                // hundred times a second on an idle node.
                let _watcher = watcher;
                while !thread_stop.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(100));
                }
            })
            .map_err(|error| error.to_string())?;
        Ok(WatchStream {
            events: SharedRecv::new(receiver),
            stop,
        })
    }
}

/// Go `getServerInfo` over the node's own configuration.
///
/// Reuses the domain constructor over the canonical effective config, including
/// advertised address, lease text and labels, instead of a second projection.
pub(crate) fn node_server_info(
    config: &crate::node_config::NodeConfig,
) -> tidb_domain::serverinfo::ServerInfo {
    tidb_domain::serverinfo_syncer::server_info_from_config(
        &tidb_domain::serverinfo_syncer::new_node_id(),
        &config.global_config,
        tidb_util::versioninfo::TIDB_GIT_HASH,
        config
            .global_config
            .enable_global_kill
            .then(|| Arc::new(|| 1u64) as Arc<dyn Fn() -> u64 + Send + Sync>),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs() as i64),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tidb_domain::server_id::{ServerIdAuthority, ServerIdIntervals, ServerIdKeeper};
    use tidb_domain::serverinfo_syncer::Syncer;

    // Go domain/serverinfo tests embed etcd. The Rust integration uses the
    // official executable, explicitly selected so ordinary unit runs need no service.
    #[test]
    #[ignore = "requires etcd on PATH; run with --include-ignored"]
    fn cluster_lifecycle_batch_real_etcd_claims_recovery_and_minimum_lease() {
        struct EtcdProcess {
            child: std::process::Child,
            path: std::path::PathBuf,
        }
        impl Drop for EtcdProcess {
            fn drop(&mut self) {
                let _ = self.child.kill();
                let _ = self.child.wait();
                let _ = std::fs::remove_dir_all(&self.path);
            }
        }
        fn endpoint() -> String {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            format!("http://{}", listener.local_addr().unwrap())
        }
        let address = endpoint();
        let peer = endpoint();
        let path = std::env::temp_dir().join(format!("tidb-identity-etcd-{}", std::process::id()));
        std::fs::create_dir(&path).expect("unique integration data directory");
        let child = std::process::Command::new("etcd")
            .args([
                "--data-dir",
                path.to_str().unwrap(),
                "--listen-client-urls",
                &address,
                "--advertise-client-urls",
                &address,
                "--listen-peer-urls",
                &peer,
                "--initial-advertise-peer-urls",
                &peer,
                "--initial-cluster",
                &format!("default={peer}"),
                "--log-level",
                "error",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let _process = EtcdProcess { child, path };
        let client =
            tidb_pd_client::EtcdClient::connect([address], std::time::Duration::from_secs(1))
                .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while client.get(b"/ready").is_err() {
            assert!(std::time::Instant::now() < deadline, "etcd startup failed");
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let etcd = Arc::new(EtcdClientOps::new(Arc::new(client.clone())));
        let create = |name: &str| {
            let identity = ServerIdAuthority::new(Some(etcd.clone()), true);
            let get = identity.clone();
            let mut info = tidb_domain::serverinfo::ServerInfo::default();
            info.static_info.id = name.into();
            info.static_info.server_id_getter = Some(Arc::new(move || get.id()));
            let syncer = Arc::new(Syncer::new_with_status_endpoint_claim(
                info,
                Some(etcd.clone()),
                false,
            ));
            syncer.new_session_and_store_server_info().unwrap();
            let keeper = ServerIdKeeper::start(
                identity.clone(),
                syncer.clone(),
                ServerIdIntervals {
                    refresh: std::time::Duration::from_millis(20),
                    retry: std::time::Duration::from_millis(20),
                    ..Default::default()
                },
            )
            .unwrap();
            (identity, syncer, keeper)
        };
        let (first, info, keeper) = create("first");
        let (second, _, other_keeper) = create("second");
        assert_ne!(first.id(), second.id());
        let key = format!("/tidb/server_id/{}", first.id());
        let claim = client
            .get_prefix_metadata(key.as_bytes())
            .unwrap()
            .into_iter()
            .find(|kv| kv.key == key.as_bytes())
            .unwrap();
        assert!(!etcd
            .create_if_absent_with_lease(&key, b"0", info.session_lease().unwrap())
            .unwrap());
        let kills = Arc::new(AtomicUsize::new(0));
        let observed = kills.clone();
        first.set_connection_killer(Some(Arc::new(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        })));
        client.lease_revoke(claim.lease).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if kills.load(Ordering::SeqCst) == 1 && !first.is_lost() {
                let bytes = client
                    .get(info.server_info_path().as_bytes())
                    .unwrap()
                    .unwrap();
                let published: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(published["server_id"], first.id());
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "identity recovery failed"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        info.store_min_start_ts(12345).unwrap();
        let minimum = b"/tidb/server/minstartts/first";
        assert_eq!(client.get(minimum).unwrap().unwrap(), b"12345");
        client.lease_revoke(info.session_lease().unwrap()).unwrap();
        assert!(client.get(minimum).unwrap().is_none());
        info.restart().unwrap();
        info.store_min_start_ts(23456).unwrap();
        assert_eq!(client.get(minimum).unwrap().unwrap(), b"23456");
        let retired = format!("/tidb/server_id/{}", first.id());
        drop(keeper);
        assert!(first.is_lost());
        assert!(client.get(retired.as_bytes()).unwrap().is_none());
        assert!(!second.is_lost());
        drop(other_keeper);
        info.remove_min_start_ts();
        assert!(client.get(minimum).unwrap().is_none());
    }
}
