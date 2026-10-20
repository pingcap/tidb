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

//! Go `pkg/store/mockstore/unistore/mock.go`: `New`, the embedded store
//! client-go reaches -- its RPC client, its PD client and its cluster -- and
//! the client-rust PD client a store over it uses, as Go's mock store builds
//! client-go's `KVStore` over unistore's two clients.
//!
//! # Narrowings, by name
//!
//! * `currentKeyspaceID`: Go's `pdClient` hands it only to the mock resource
//!   manager client, which is not ported. The store's keyspace is the one
//!   its client-rust PD client is configured with (`Config::keyspace`).

use std::sync::{Arc, Mutex};

use tikv_client::proto::keyspacepb::KeyspaceMeta;

use crate::cluster::Cluster;
use crate::kv_handler::KvHandler;
use crate::mock_pd::MockPd;
use crate::mvcc_store::MvccStore;
use crate::pd::PdClient;
use crate::region_manager::InProcessRegionManager;
use crate::rpc::{RpcClient, RpcConnect};
use crate::tso::Tso;

/// client-rust's PD client over the embedded store: its region cache, store
/// clients and routing over unistore's PD client and RPC client, the part
/// client-go's `KVStore` builds over Go's.
pub type StorePdClient = tikv_client::PdRpcClient<RpcConnect, PdClient>;

/// Go `New(path, pdAddrs, currentKeyspaceID, clusterKeyspaces)` over an
/// in-memory store: no store or region exists until the caller bootstraps
/// the cluster (Go's `mockstore` runs `BootstrapWithSingleStore` by
/// default). The store, its PD and the store's async-commit `minCommitTS`
/// share one oracle.
pub fn new(
    cluster_keyspaces: Vec<KeyspaceMeta>,
) -> Result<(RpcClient, Arc<PdClient>, Arc<Cluster>), String> {
    let rm = Arc::new(InProcessRegionManager::new_mock_region_manager());
    let tso = Arc::new(Tso::new());
    let handler = Arc::new(Mutex::new(KvHandler::new(
        MvccStore::with_pd(Arc::clone(&tso)),
        Arc::clone(&rm),
    )));
    over(handler, rm, tso, cluster_keyspaces)
}

/// [`new`] over a store that already exists: its handler, the region manager
/// the handler checks requests against, and the oracle its PD draws from.
pub fn over(
    handler: Arc<Mutex<KvHandler>>,
    rm: Arc<InProcessRegionManager>,
    tso: Arc<Tso>,
    cluster_keyspaces: Vec<KeyspaceMeta>,
) -> Result<(RpcClient, Arc<PdClient>, Arc<Cluster>), String> {
    let mock_pd = Arc::new(MockPd::new(Arc::clone(&rm), tso));
    let cluster = Arc::new(Cluster::new(rm));
    let client = RpcClient::new(handler, Arc::clone(&cluster));
    let pd_client = PdClient::new(mock_pd, cluster_keyspaces)?;
    Ok((client, Arc::new(pd_client), cluster))
}

/// client-go `tikv.NewTestTiKVStore`'s PD half (`NewTestKeyspaceTiKVStore`
/// with `config.keyspace`): client-rust's PD client over unistore's two.
pub async fn store_pd_client(
    rpc: &RpcClient,
    pd: Arc<PdClient>,
    config: tikv_client::Config,
) -> tikv_client::Result<StorePdClient> {
    tikv_client::PdRpcClient::new_with_pd_client(config, RpcConnect::new(rpc.clone()), pd, true)
        .await
}
