// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Ordinary downstream-crate gate for client-go's injected client path.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use tikv_client::proto::{keyspacepb, metapb};
use tikv_client::tikv::{
    Client as KvClient, Keyspace, RegionId, RegionStore, RegionVerId, RegionWithLeader, Request,
    Store, StoreId,
};
use tikv_client::{
    Error, Key, PdClient, Result, Timestamp, TimestampExt, Transaction, TransactionOptions,
};

#[derive(Clone)]
struct InProcessKvClient;

#[async_trait]
impl KvClient for InProcessKvClient {
    async fn dispatch(&self, _request: &dyn Request) -> Result<Box<dyn Any>> {
        Err(Error::StringError("no request expected".to_owned()))
    }
}

struct InProcessPdClient;

fn no_region() -> Error {
    Error::StringError("no region expected".to_owned())
}

#[async_trait]
impl PdClient for InProcessPdClient {
    type KvClient = InProcessKvClient;

    async fn map_region_to_store(
        self: Arc<Self>,
        _region: RegionWithLeader,
    ) -> Result<RegionStore> {
        Err(Error::StringError("no route expected".to_owned()))
    }

    async fn region_for_key(&self, _key: &Key) -> Result<RegionWithLeader> {
        Err(no_region())
    }

    async fn region_for_id(&self, _id: RegionId) -> Result<RegionWithLeader> {
        Err(no_region())
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        Ok(Timestamp::from_version(42))
    }

    async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
        Ok(true)
    }

    async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        Err(Error::StringError("no keyspace expected".to_owned()))
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        Ok(Vec::new())
    }

    async fn update_leader(&self, _ver_id: RegionVerId, _leader: metapb::Peer) -> Result<()> {
        Ok(())
    }

    async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {}

    async fn invalidate_store_cache(&self, _store_id: StoreId) {}
}

#[test]
fn ordinary_downstream_build_can_construct_an_injected_transaction() {
    let transaction = Transaction::new(
        Timestamp::from_version(42),
        Arc::new(InProcessPdClient),
        TransactionOptions::new_optimistic().read_only(),
        Keyspace::Disable,
    );

    assert_eq!(transaction.start_timestamp().version(), 42);
}
