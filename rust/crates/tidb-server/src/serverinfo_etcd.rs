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

use std::sync::Arc;

use tidb_domain::serverinfo_syncer::EtcdOps;
use tidb_pd_client::EtcdClient;

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
            .delete(key.as_bytes())
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
}

/// Go `getServerInfo` over the node's own configuration.
///
/// `NodeConfig` is this tier's spelling of the pieces Go reads from the
/// global config, so the two are mapped here rather than in `tidb-domain`:
/// the ADVERTISE address is what a peer would dial, the DDL lease travels
/// as the text Go stores, and the labels are empty until the config file's
/// `labels` section is threaded (named, not silently dropped).
pub(crate) fn node_server_info(config: &crate::node_config::NodeConfig) -> tidb_domain::serverinfo::ServerInfo {
    let mut info = tidb_domain::serverinfo::ServerInfo::default();
    info.static_info.id = tidb_domain::serverinfo_syncer::new_node_id();
    info.static_info.ip = config.advertise_address.clone();
    info.static_info.port = u32::from(config.port);
    info.static_info.status_port = u32::from(config.status_port);
    info.static_info.lease = format!("{}ms", config.schema_lease.as_millis());
    info.static_info.start_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or_default();
    info.static_info.version_info = tidb_domain::serverinfo::VersionInfo {
        version: config.version_info.server_version.clone(),
        git_hash: config.version_info.git_hash.clone(),
    };
    info
}
