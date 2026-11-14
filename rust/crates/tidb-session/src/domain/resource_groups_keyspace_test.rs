// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go capture `zzprobe/o15_store.TestResourceGroupsControllerKeyspace`.
use super::*;
use async_trait::async_trait;
use tidb_unistore::embedded_resource_groups::EmbeddedResourceGroupProvider;
use tikv_client::pd_metastorage::{Client, WatchResponse};
use tikv_client::pd_options::MetaStorageOption;
use tikv_client::pd_resource_group::controller::ResourceGroupProvider;
use tikv_client::pd_resource_manager::GetResourceGroupOption;
use tikv_client::proto::resource_manager::{
    ResourceGroup, TokenBucketResponse, TokenBucketsRequest,
};
use tikv_client::proto::{keyspacepb, meta_storagepb};

/// Go probe provider: a real controller config with a tenant-specific RU policy.
#[derive(Default)]
struct PolicyProvider(EmbeddedResourceGroupProvider);
#[async_trait]
impl Client for PolicyProvider {
    async fn get(
        &self,
        _: &[u8],
        _: &[MetaStorageOption],
    ) -> tikv_client::Result<meta_storagepb::GetResponse> {
        let mut response = meta_storagepb::GetResponse::default();
        response.kvs.push(Default::default());
        response.kvs[0].value =
            br#"{"ru-version-policy":{"default":1,"overrides":{"42":2}}}"#.to_vec();
        Ok(response)
    }
    async fn put(
        &self,
        key: &[u8],
        value: &[u8],
        opts: &[MetaStorageOption],
    ) -> tikv_client::Result<meta_storagepb::PutResponse> {
        self.0.put(key, value, opts).await
    }
    async fn watch(
        &self,
        key: &[u8],
        opts: &[MetaStorageOption],
    ) -> tikv_client::Result<tokio::sync::mpsc::Receiver<WatchResponse>> {
        self.0.watch(key, opts).await
    }
}
#[async_trait]
impl ResourceGroupProvider for PolicyProvider {
    async fn get_resource_group(
        &self,
        name: &str,
        opts: &[GetResourceGroupOption],
    ) -> tikv_client::Result<Option<ResourceGroup>> {
        self.0.get_resource_group(name, opts).await
    }
    async fn list_resource_groups(
        &self,
        opts: &[GetResourceGroupOption],
    ) -> tikv_client::Result<Vec<ResourceGroup>> {
        self.0.list_resource_groups(opts).await
    }
    async fn add_resource_group(&self, group: ResourceGroup) -> tikv_client::Result<String> {
        self.0.add_resource_group(group).await
    }
    async fn modify_resource_group(&self, group: ResourceGroup) -> tikv_client::Result<String> {
        self.0.modify_resource_group(group).await
    }
    async fn delete_resource_group(&self, name: &str) -> tikv_client::Result<String> {
        self.0.delete_resource_group(name).await
    }
    async fn acquire_token_buckets(
        &self,
        request: TokenBucketsRequest,
    ) -> tikv_client::Result<Vec<TokenBucketResponse>> {
        self.0.acquire_token_buckets(request).await
    }
    async fn load_resource_groups(&self) -> tikv_client::Result<(Vec<ResourceGroup>, i64)> {
        self.0.load_resource_groups().await
    }
}

/// Go `Domain.initResourceGroupsController` and captured controller RU policy.
/// Observe the actual controller's selected RU version, not just a codec getter.
#[test]
fn test_resource_groups_controller_keyspace() {
    use tidb_txnkv::driver::tikv_store::{TikvStore, store_runtime};
    let runtime = store_runtime().unwrap();
    for (name, id, expected_version) in [("", u32::MAX, 1), ("tenant", 42, 2)] {
        let pd = Arc::new(tikv_client::mock::MockPdClient::default());
        pd.set_keyspace_meta(keyspacepb::KeyspaceMeta {
            name: name.into(),
            keyspace: Some(keyspacepb::keyspace_meta::Keyspace::Id(id)),
            state: keyspacepb::KeyspaceState::Enabled as i32,
            ..Default::default()
        });
        let config = if name.is_empty() {
            tikv_client::Config::default()
        } else {
            tikv_client::Config::default().with_keyspace(name)
        };
        let inner = runtime
            .block_on(tikv_client::tikv::KvStore::new_with_pd_client(
                pd,
                Arc::new(tikv_client::tikv::MockSafePointKv::new()),
                config,
            ))
            .unwrap();
        let store = TikvStore::new(Arc::clone(&runtime), inner);
        let controller =
            start_resource_groups_controller_over(&store, 1, Arc::new(PolicyProvider::default()))
                .unwrap();
        let version = controller.get_ru_version();
        stop_resource_groups_controller(&store, &controller);
        runtime.block_on(store.store().clone().close()).unwrap();
        assert_eq!(version, expected_version, "keyspace {name}");
    }
}
