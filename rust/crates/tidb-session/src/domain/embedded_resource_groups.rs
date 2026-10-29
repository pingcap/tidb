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

//! The resource-manager face of Go unistore's mock `pdClient`
//! (`pkg/store/mockstore/unistore/pd.go`), which embeds
//! `infosync.NewMockResourceManagerClient`: the groups are the mock's, the
//! token buckets and the initial load answer nothing, and meta storage holds
//! nothing. Go's domain builds its resource-groups controller over this
//! client (`initResourceGroupsController`), which is what prices every
//! request and response into the statement's `RUDetails`.

use std::sync::Mutex;

use async_trait::async_trait;
use prost::Message;
use tikv_client::pd_metastorage::{Client as MetaStorageClient, WatchResponse};
use tikv_client::pd_options::MetaStorageOption;
use tikv_client::pd_resource_group::controller::ResourceGroupProvider;
use tikv_client::pd_resource_manager::GetResourceGroupOption;
use tikv_client::proto::meta_storagepb;
use tikv_client::proto::resource_manager::{
    ResourceGroup, TokenBucketResponse, TokenBucketsRequest,
};
use tokio::sync::mpsc;

/// See the module docs.
#[derive(Default)]
pub(crate) struct EmbeddedResourceGroupProvider {
    /// Go's mock answers the group-settings watch with a channel it keeps;
    /// the senders stay here so the controller's watch stays open.
    watches: Mutex<Vec<mpsc::Sender<WatchResponse>>>,
}

/// The mock holds `tidb_proto` groups; the controller speaks client-rust's.
fn to_client_group(group: &tidb_proto::resource_manager::ResourceGroup) -> ResourceGroup {
    ResourceGroup::decode(group.encode_to_vec().as_slice()).unwrap_or_default()
}

fn to_mock_group(group: &ResourceGroup) -> tidb_proto::resource_manager::ResourceGroup {
    tidb_proto::resource_manager::ResourceGroup::decode(group.encode_to_vec().as_slice())
        .unwrap_or_default()
}

fn mock_error(message: String) -> tikv_client::Error {
    tikv_client::Error::StringError(message)
}

#[async_trait]
impl MetaStorageClient for EmbeddedResourceGroupProvider {
    async fn watch(
        &self,
        _key: &[u8],
        _opts: &[MetaStorageOption],
    ) -> tikv_client::Result<mpsc::Receiver<WatchResponse>> {
        let (sender, receiver) = mpsc::channel(1);
        self.watches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(sender);
        Ok(receiver)
    }

    async fn get(
        &self,
        _key: &[u8],
        _opts: &[MetaStorageOption],
    ) -> tikv_client::Result<meta_storagepb::GetResponse> {
        // Go unistore `pdClient.Get`: nothing stored.
        Ok(meta_storagepb::GetResponse::default())
    }

    async fn put(
        &self,
        _key: &[u8],
        _value: &[u8],
        _opts: &[MetaStorageOption],
    ) -> tikv_client::Result<meta_storagepb::PutResponse> {
        Ok(meta_storagepb::PutResponse::default())
    }
}

#[async_trait]
impl ResourceGroupProvider for EmbeddedResourceGroupProvider {
    async fn get_resource_group(
        &self,
        resource_group_name: &str,
        _opts: &[GetResourceGroupOption],
    ) -> tikv_client::Result<Option<ResourceGroup>> {
        tidb_domain_infosync::resource_manager_client::get_resource_group(resource_group_name)
            .map(|group| Some(to_client_group(&group)))
            .map_err(mock_error)
    }

    async fn list_resource_groups(
        &self,
        _opts: &[GetResourceGroupOption],
    ) -> tikv_client::Result<Vec<ResourceGroup>> {
        tidb_domain_infosync::resource_manager_client::list_resource_groups()
            .map(|groups| groups.iter().map(to_client_group).collect())
            .map_err(mock_error)
    }

    async fn add_resource_group(&self, meta_group: ResourceGroup) -> tikv_client::Result<String> {
        tidb_domain_infosync::resource_manager_client::add_resource_group(to_mock_group(&meta_group))
            .map(|()| "Success!".to_owned())
            .map_err(mock_error)
    }

    async fn modify_resource_group(
        &self,
        meta_group: ResourceGroup,
    ) -> tikv_client::Result<String> {
        tidb_domain_infosync::resource_manager_client::modify_resource_group(to_mock_group(&meta_group))
            .map(|()| "Success!".to_owned())
            .map_err(mock_error)
    }

    async fn delete_resource_group(
        &self,
        resource_group_name: &str,
    ) -> tikv_client::Result<String> {
        tidb_domain_infosync::resource_manager_client::delete_resource_group(resource_group_name)
            .map(|()| "Success!".to_owned())
            .map_err(mock_error)
    }

    async fn acquire_token_buckets(
        &self,
        _request: TokenBucketsRequest,
    ) -> tikv_client::Result<Vec<TokenBucketResponse>> {
        // Go unistore `pdClient.AcquireTokenBuckets`: `nil, nil`.
        Ok(Vec::new())
    }

    async fn load_resource_groups(&self) -> tikv_client::Result<(Vec<ResourceGroup>, i64)> {
        // Go unistore `pdClient.LoadResourceGroups`: `nil, 0, nil`.
        Ok((Vec::new(), 0))
    }
}
