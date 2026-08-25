// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Various mock versions of the various clients and other objects.
//!
//! The goal is to be able to test functionality independently of the rest of
//! the system, in particular without requiring a TiKV or PD server, or RPC layer.

pub(crate) mod cluster;
pub(crate) mod deadlock;

use std::any::Any;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use derive_new::new;

use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::pd::RetryClient;
use crate::proto::keyspacepb;
use crate::proto::metapb::RegionEpoch;
use crate::proto::metapb::{self};
use crate::region::RegionId;
use crate::region::RegionVerId;
use crate::region::RegionWithLeader;
use crate::store::KvConnect;
use crate::store::RegionStore;
use crate::store::Request;
use crate::store::{KvClient, Store};
use crate::Config;
use crate::Error;
use crate::Key;
use crate::Result;
use crate::Timestamp;

/// Create a `PdRpcClient` with it's internals replaced with mocks so that the
/// client can be tested without doing any RPC calls.
pub async fn pd_rpc_client() -> PdRpcClient<MockKvConnect, MockCluster> {
    let config = Config::default();
    PdRpcClient::new(
        config.clone(),
        |_| MockKvConnect,
        |sm| {
            futures::future::ok(RetryClient::new_with_cluster(
                sm,
                config.timeout,
                MockCluster,
            ))
        },
        false,
    )
    .await
    .unwrap()
}

#[allow(clippy::type_complexity)]
#[derive(new, Default, Clone)]
pub struct MockKvClient {
    pub addr: String,
    dispatch: Option<Arc<dyn Fn(&dyn Any) -> Result<Box<dyn Any>> + Send + Sync + 'static>>,
}

impl MockKvClient {
    pub fn with_dispatch_hook<F>(dispatch: F) -> MockKvClient
    where
        F: Fn(&dyn Any) -> Result<Box<dyn Any>> + Send + Sync + 'static,
    {
        MockKvClient {
            addr: String::new(),
            dispatch: Some(Arc::new(dispatch)),
        }
    }
}

pub struct MockKvConnect;

pub struct MockCluster;

#[derive(new)]
pub struct MockPdClient {
    client: MockKvClient,
    #[new(default)]
    timestamp: Arc<Mutex<Timestamp>>,
    #[new(default)]
    epoch_not_match_regions: Arc<Mutex<Vec<RegionWithLeader>>>,
    #[new(default)]
    invalidated_regions: Arc<Mutex<Vec<RegionVerId>>>,
    #[new(default)]
    closed_client_addresses: Arc<Mutex<Vec<String>>>,
    #[new(default)]
    bucket_updates: Arc<Mutex<Vec<(RegionVerId, u64, Vec<Vec<u8>>)>>>,
    #[new(default)]
    keyspace_meta: Arc<Mutex<Option<keyspacepb::KeyspaceMeta>>>,
    #[new(default)]
    loaded_keyspaces: Arc<Mutex<Vec<String>>>,
    #[new(default)]
    regions: Arc<Mutex<Option<Vec<RegionWithLeader>>>>,
    #[new(default)]
    split_region_keys: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
}

#[async_trait]
impl KvClient for MockKvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>> {
        match &self.dispatch {
            Some(f) => f(req.as_any()),
            None => panic!("no dispatch hook set"),
        }
    }
}

#[async_trait]
impl KvConnect for MockKvConnect {
    type KvClient = MockKvClient;

    async fn connect(&self, address: &str) -> Result<Self::KvClient> {
        Ok(MockKvClient {
            addr: address.to_owned(),
            dispatch: None,
        })
    }
}

impl MockPdClient {
    pub(crate) fn set_timestamp(&self, timestamp: Timestamp) {
        *self.timestamp.lock().unwrap() = timestamp;
    }

    pub fn default() -> MockPdClient {
        MockPdClient {
            client: MockKvClient::default(),
            timestamp: Arc::default(),
            epoch_not_match_regions: Arc::default(),
            invalidated_regions: Arc::default(),
            closed_client_addresses: Arc::default(),
            bucket_updates: Arc::default(),
            keyspace_meta: Arc::default(),
            loaded_keyspaces: Arc::default(),
            regions: Arc::default(),
            split_region_keys: Arc::default(),
        }
    }

    pub(crate) fn with_regions(regions: Vec<RegionWithLeader>) -> MockPdClient {
        let client = Self::default();
        *client.regions.lock().unwrap() = Some(regions);
        client
    }

    pub(crate) fn with_client_and_regions(
        kv_client: MockKvClient,
        regions: Vec<RegionWithLeader>,
    ) -> MockPdClient {
        let client = Self::new(kv_client);
        *client.regions.lock().unwrap() = Some(regions);
        client
    }

    pub fn region1() -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = 1;
        region.region.start_key = vec![];
        region.region.end_key = vec![10];
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });

        let leader = metapb::Peer {
            store_id: 41,
            ..Default::default()
        };
        region.leader = Some(leader);

        region
    }

    pub fn region2() -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = 2;
        region.region.start_key = vec![10];
        region.region.end_key = vec![250, 250];
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });

        let leader = metapb::Peer {
            store_id: 42,
            ..Default::default()
        };
        region.leader = Some(leader);

        region
    }

    pub fn region3() -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = 3;
        region.region.start_key = vec![250, 250];
        region.region.end_key = vec![];
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });

        let leader = metapb::Peer {
            store_id: 43,
            ..Default::default()
        };
        region.leader = Some(leader);

        region
    }

    pub(crate) fn epoch_not_match_regions(&self) -> Vec<RegionWithLeader> {
        self.epoch_not_match_regions.lock().unwrap().clone()
    }

    pub(crate) fn invalidated_regions(&self) -> Vec<RegionVerId> {
        self.invalidated_regions.lock().unwrap().clone()
    }

    pub(crate) fn closed_client_addresses(&self) -> Vec<String> {
        self.closed_client_addresses.lock().unwrap().clone()
    }

    pub(crate) fn bucket_updates(&self) -> Vec<(RegionVerId, u64, Vec<Vec<u8>>)> {
        self.bucket_updates.lock().unwrap().clone()
    }

    pub(crate) fn set_keyspace_meta(&self, meta: keyspacepb::KeyspaceMeta) {
        *self.keyspace_meta.lock().unwrap() = Some(meta);
    }

    pub(crate) fn loaded_keyspaces(&self) -> Vec<String> {
        self.loaded_keyspaces.lock().unwrap().clone()
    }

    pub(crate) fn split_region_keys(&self) -> Vec<Vec<Vec<u8>>> {
        self.split_region_keys.lock().unwrap().clone()
    }
}

#[async_trait]
impl PdClient for MockPdClient {
    type KvClient = MockKvClient;

    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        Ok(RegionStore::new(region, Arc::new(self.client.clone())))
    }

    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let bytes: &[_] = key.into();
        if let Some(regions) = self.regions.lock().unwrap().as_ref() {
            return regions
                .iter()
                .find(|region| {
                    region.region.start_key.as_slice() <= bytes
                        && (region.region.end_key.is_empty()
                            || bytes < region.region.end_key.as_slice())
                })
                .cloned()
                .ok_or_else(|| Error::StringError("mock region not found for key".to_owned()));
        }
        let region = if bytes.is_empty() || bytes < &[10][..] {
            Self::region1()
        } else if bytes >= &[10][..] && bytes < &[250, 250][..] {
            Self::region2()
        } else {
            Self::region3()
        };

        Ok(region)
    }

    async fn region_for_end_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let bytes: &[_] = key.into();
        if let Some(regions) = self.regions.lock().unwrap().as_ref() {
            return regions
                .iter()
                .find(|region| {
                    (bytes.is_empty() || region.region.start_key.as_slice() < bytes)
                        && (region.region.end_key.is_empty()
                            || bytes <= region.region.end_key.as_slice())
                })
                .cloned()
                .ok_or_else(|| Error::StringError("mock region not found for end key".to_owned()));
        }
        let region = if bytes.is_empty() || bytes <= &[10][..] {
            Self::region1()
        } else if bytes <= &[250, 250][..] {
            Self::region2()
        } else {
            Self::region3()
        };
        Ok(region)
    }

    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        if let Some(regions) = self.regions.lock().unwrap().as_ref() {
            return regions
                .iter()
                .find(|region| region.id() == id)
                .cloned()
                .ok_or(Error::RegionNotFoundInResponse { region_id: id });
        }
        match id {
            1 => Ok(Self::region1()),
            2 => Ok(Self::region2()),
            3 => Ok(Self::region3()),
            _ => Err(Error::RegionNotFoundInResponse { region_id: id }),
        }
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        Ok(vec![Store::new(Arc::new(self.client.clone()))])
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        Ok(self.timestamp.lock().unwrap().clone())
    }

    async fn split_regions(
        self: Arc<Self>,
        split_keys: Vec<Vec<u8>>,
        _retry_limit: u64,
    ) -> Result<Vec<u64>> {
        self.split_region_keys.lock().unwrap().push(split_keys);
        Ok(Vec::new())
    }

    async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
        unimplemented!()
    }

    async fn update_leader(
        &self,
        _ver_id: crate::region::RegionVerId,
        _leader: metapb::Peer,
    ) -> Result<()> {
        todo!()
    }

    async fn update_region_cache(&self, regions: Vec<RegionWithLeader>) -> Result<()> {
        self.epoch_not_match_regions.lock().unwrap().extend(regions);
        Ok(())
    }

    async fn update_buckets(&self, ver_id: RegionVerId, version: u64, keys: Vec<Vec<u8>>) {
        self.bucket_updates
            .lock()
            .unwrap()
            .push((ver_id, version, keys));
    }

    async fn invalidate_region_cache(&self, ver_id: crate::region::RegionVerId) {
        self.invalidated_regions.lock().unwrap().push(ver_id);
    }

    async fn invalidate_store_cache(&self, _store_id: crate::region::StoreId) {}

    async fn close_kv_client_addr_ver(&self, address: &str, _version: u64) {
        self.closed_client_addresses
            .lock()
            .unwrap()
            .push(address.to_owned());
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        self.loaded_keyspaces
            .lock()
            .unwrap()
            .push(keyspace.to_owned());
        self.keyspace_meta
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| Error::KeyspaceNotFound(keyspace.to_owned()))
    }
}
