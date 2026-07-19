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

use tonic::transport::{Channel, Endpoint};

use crate::client::PhysicalChannelIdentity;

use super::DirectUnaryClientError;

#[derive(Clone)]
pub(super) struct VersionedChannel {
    physical_channel: PhysicalChannelIdentity,
    pub(super) channel: Channel,
}

impl VersionedChannel {
    pub(super) const fn physical_channel(&self) -> &PhysicalChannelIdentity {
        &self.physical_channel
    }
}

pub(super) struct ChannelPool {
    channels: HashMap<String, VersionedChannel>,
    versions: HashMap<String, u64>,
    closed: bool,
}

impl ChannelPool {
    pub(super) fn new() -> Self {
        Self {
            channels: HashMap::new(),
            versions: HashMap::new(),
            closed: false,
        }
    }

    pub(super) fn get_or_create(
        &mut self,
        address: &str,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<VersionedChannel, DirectUnaryClientError> {
        if self.closed {
            return Err(DirectUnaryClientError::Closed);
        }
        if let Some(channel) = self.channels.get(address) {
            return Ok(channel.clone());
        }

        let uri = if address.contains("://") {
            address.to_owned()
        } else {
            format!("http://{address}")
        };
        let endpoint =
            Endpoint::from_shared(uri).map_err(|error| DirectUnaryClientError::InvalidAddress {
                address: address.to_owned(),
                message: error.to_string(),
            })?;
        let version = self
            .versions
            .get(address)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        let channel = {
            let _runtime = runtime.enter();
            endpoint.connect_lazy()
        };
        let versioned = VersionedChannel {
            physical_channel: PhysicalChannelIdentity::new(address, version),
            channel,
        };
        self.versions.insert(address.to_owned(), version);
        self.channels.insert(address.to_owned(), versioned.clone());
        Ok(versioned)
    }

    pub(super) fn close_address(&mut self, address: &str) -> Option<PhysicalChannelIdentity> {
        if self.closed {
            return None;
        }
        self.channels
            .remove(address)
            .map(|channel| channel.physical_channel)
    }

    pub(super) fn close_address_version(
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
            return self
                .channels
                .remove(address)
                .map(|channel| channel.physical_channel);
        }
        None
    }

    pub(super) fn close(&mut self) {
        self.closed = true;
        self.channels.clear();
    }

    pub(super) fn version(&self, address: &str) -> Option<u64> {
        self.channels
            .get(address)
            .map(|channel| channel.physical_channel.version())
    }

    pub(super) fn len(&self) -> usize {
        self.channels.len()
    }
}
