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
