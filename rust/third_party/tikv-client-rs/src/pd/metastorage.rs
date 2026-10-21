// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! PD client's `clients/metastorage` package: the meta storage client
//! interface, its watch response, and service address discovery.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use super::errs;
use super::opt::{with_prefix, MetaStorageOption};
use crate::proto::meta_storagepb;
use crate::Result;

/// The response of a watch request.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WatchResponse {
    pub events: Vec<meta_storagepb::Event>,
    pub compact_revision: i64,
}

/// The meta storage client interface.
#[async_trait]
pub trait Client: Send + Sync {
    /// Watches a key or prefix. The channel closes when the watch ends.
    async fn watch(
        &self,
        key: &[u8],
        opts: &[MetaStorageOption],
    ) -> Result<mpsc::Receiver<WatchResponse>>;
    /// Gets the value for a key.
    async fn get(
        &self,
        key: &[u8],
        opts: &[MetaStorageOption],
    ) -> Result<meta_storagepb::GetResponse>;
    /// Puts a key-value pair into meta storage.
    async fn put(
        &self,
        key: &[u8],
        value: &[u8],
        opts: &[MetaStorageOption],
    ) -> Result<meta_storagepb::PutResponse>;
}

/// A cause carried by its text, for wrapping a transport error.
#[derive(Debug)]
struct Cause(String);

impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Cause {}

fn wrap_by_cause(definition: errs::Definition, cause: String) -> crate::Error {
    let cause: errs::SharedError = Arc::new(Cause(cause));
    crate::Error::Pd(
        definition
            .wrap(Some(cause))
            .expect("a present cause always wraps")
            .gen_with_stack_by_cause(),
    )
}

/// Go `DiscoveryMSAddrs`: the sorted addresses of every member registered
/// under `service_key`.
pub async fn discovery_ms_addrs(service_key: &str, client: &dyn Client) -> Result<Vec<String>> {
    let resp = client
        .get(service_key.as_bytes(), &[with_prefix()])
        .await
        .map_err(|error| {
            wrap_by_cause(errs::ERR_CLIENT_GET_META_STORAGE_CLIENT, error.to_string())
        })?;
    if let Some(error) = resp
        .header
        .as_ref()
        .and_then(|header| header.error.as_ref())
    {
        return Err(wrap_by_cause(
            errs::ERR_CLIENT_GET_PROTO_CLIENT,
            error.message.clone(),
        ));
    }
    let mut sorted_addrs = Vec::with_capacity(resp.kvs.len());
    for kv in &resp.kvs {
        let mut entry = ServiceRegistryEntry::default();
        if let Err(error) = entry.deserialize(&kv.value) {
            log::warn!(
                "service-key":% = service_key,
                key:% = String::from_utf8_lossy(&kv.key),
                error:% = error;
                "try to deserialize service registry entry failed"
            );
            continue;
        }
        sorted_addrs.push(entry.service_addr);
    }
    sorted_addrs.sort();
    Ok(sorted_addrs)
}

/// The registry entry of a service.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ServiceRegistryEntry {
    /// Assigned only by a startup parameter; otherwise the service hostname.
    pub name: String,
    pub service_addr: String,
    pub version: String,
    pub git_hash: String,
    pub deploy_path: String,
    pub start_timestamp: i64,
}

#[derive(serde_derive::Deserialize)]
struct ServiceRegistryEntryWire {
    name: Option<String>,
    #[serde(rename = "service-addr")]
    service_addr: Option<String>,
    version: Option<String>,
    #[serde(rename = "git-hash")]
    git_hash: Option<String>,
    #[serde(rename = "deploy-path")]
    deploy_path: Option<String>,
    #[serde(rename = "start-timestamp")]
    start_timestamp: Option<i64>,
}

impl ServiceRegistryEntry {
    /// Go `Deserialize`: JSON fields present in `data` overwrite this entry.
    pub fn deserialize(&mut self, data: &[u8]) -> std::result::Result<(), serde_json::Error> {
        let wire: ServiceRegistryEntryWire = serde_json::from_slice(data).map_err(|error| {
            log::warn!(error:% = error; "json unmarshal the service registry entry failed");
            error
        })?;
        if let Some(name) = wire.name {
            self.name = name;
        }
        if let Some(service_addr) = wire.service_addr {
            self.service_addr = service_addr;
        }
        if let Some(version) = wire.version {
            self.version = version;
        }
        if let Some(git_hash) = wire.git_hash {
            self.git_hash = git_hash;
        }
        if let Some(deploy_path) = wire.deploy_path {
            self.deploy_path = deploy_path;
        }
        if let Some(start_timestamp) = wire.start_timestamp {
            self.start_timestamp = start_timestamp;
        }
        Ok(())
    }
}
