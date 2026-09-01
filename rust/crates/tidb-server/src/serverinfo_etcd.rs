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
            .put_with_lease(key.as_bytes(), value, lease)
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
    ) -> Result<WatchStream, String> {
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
                let _watcher = watcher;
                while !thread_stop.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(10));
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
/// `NodeConfig` is this tier's spelling of the pieces Go reads from the
/// global config, so the two are mapped here rather than in `tidb-domain`:
/// the ADVERTISE address is what a peer would dial, the DDL lease travels
/// as the text Go stores, and the labels are empty until the config file's
/// `labels` section is threaded (named, not silently dropped).
pub(crate) fn node_server_info(
    config: &crate::node_config::NodeConfig,
) -> tidb_domain::serverinfo::ServerInfo {
    let mut info = tidb_domain::serverinfo::ServerInfo::default();
    info.static_info.id = tidb_domain::serverinfo_syncer::new_node_id();
    info.static_info.ip = config.advertise_address.clone();
    info.static_info.port = usize::from(config.port);
    info.static_info.status_port = usize::from(config.status_port);
    info.static_info.lease = format!("{}ms", config.schema_lease.as_millis());
    info.static_info.start_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or_default();
    info.static_info.version_info = tidb_domain::serverinfo::VersionInfo {
        version: tidb_mysql::runtime_versions().server_version,
        git_hash: tidb_util::versioninfo::TIDB_GIT_HASH.to_owned(),
    };
    info
}
