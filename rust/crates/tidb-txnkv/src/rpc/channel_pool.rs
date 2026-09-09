// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tidb_pd_client::{secure_endpoint, ClusterSecurity};
use tonic::transport::Channel;

use super::execution::ConnectionTasks;
use crate::client::PhysicalChannelIdentity;

use super::DirectUnaryClientError;

#[derive(Clone)]
pub(super) struct VersionedChannel {
    physical_channel: PhysicalChannelIdentity,
    pub(super) channel: Channel,
    pub(super) tasks: ConnectionTasks,
}

impl VersionedChannel {
    pub(super) const fn physical_channel(&self) -> &PhysicalChannelIdentity {
        &self.physical_channel
    }
}

pub(super) struct ChannelPool {
    channels: HashMap<String, VersionedChannel>,
    versions: Arc<Mutex<HashMap<String, u64>>>,
    security: Arc<ClusterSecurity>,
    closed: bool,
}

impl ChannelPool {
    /// Builds a pool whose every TiKV channel is secured with the given
    /// cluster TLS material. A plaintext [`ClusterSecurity`] keeps the
    /// backward-compatible `http://` behavior.
    pub(super) fn with_security(
        security: Arc<ClusterSecurity>,
        versions: Arc<Mutex<HashMap<String, u64>>>,
    ) -> Self {
        Self {
            channels: HashMap::new(),
            versions,
            security,
            closed: false,
        }
    }

    pub(super) fn get_or_create(
        &mut self,
        address: &str,
        runtime: &tokio::runtime::Handle,
    ) -> Result<VersionedChannel, DirectUnaryClientError> {
        if self.closed {
            return Err(DirectUnaryClientError::Closed);
        }
        if let Some(channel) = self.channels.get(address) {
            return Ok(channel.clone());
        }

        let endpoint = secure_endpoint(address, &self.security).map_err(|error| {
            DirectUnaryClientError::InvalidAddress {
                address: address.to_owned(),
                message: error.to_string(),
            }
        })?;
        // Every connection in the shared transport uses this allocator. A
        // failed-attempt identity must never name a healthy sibling connection.
        let version = {
            let mut versions = self.versions.lock().expect("channel version allocator");
            let version = versions.entry(address.to_owned()).or_default();
            *version = version.saturating_add(1);
            *version
        };
        let tasks = ConnectionTasks::new(runtime);
        let channel = {
            let _runtime = runtime.enter();
            endpoint.executor(tasks.clone()).connect_lazy()
        };
        let versioned = VersionedChannel {
            physical_channel: PhysicalChannelIdentity::new(address, version),
            channel,
            tasks,
        };
        self.channels.insert(address.to_owned(), versioned.clone());
        Ok(versioned)
    }

    pub(super) async fn close_address(&mut self, address: &str) -> Option<PhysicalChannelIdentity> {
        if self.closed {
            return None;
        }
        let channel = self.channels.remove(address)?;
        channel.tasks.close().await;
        Some(channel.physical_channel)
    }

    pub(super) async fn close_address_version(
        &mut self,
        address: &str,
        version: u64,
    ) -> Option<PhysicalChannelIdentity> {
        if self.closed {
            return None;
        }
        if self
            .channels
            .get(address)
            .is_some_and(|channel| channel.physical_channel.version() == version)
        {
            return self.close_address(address).await;
        }
        None
    }

    pub(super) async fn close(&mut self) {
        self.closed = true;
        for (_, channel) in self.channels.drain() {
            channel.tasks.close().await;
        }
    }

    pub(super) fn version(&self, address: &str) -> Option<u64> {
        self.channels
            .get(address)
            .map(|channel| channel.physical_channel.version())
    }

    pub(super) fn addresses(&self) -> impl Iterator<Item = &String> {
        self.channels.keys()
    }
}

impl Drop for ChannelPool {
    fn drop(&mut self) {
        for channel in self.channels.values() {
            channel.tasks.abort();
        }
    }
}
