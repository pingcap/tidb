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
//! client-go reaches -- its RPC client, its PD client and its cluster.

use std::sync::{Arc, Mutex};

use tikv_client::proto::keyspacepb::KeyspaceMeta;

use crate::cluster::Cluster;
use crate::kv_handler::KvHandler;
use crate::mock_pd::MockPd;
use crate::mvcc_store::MvccStore;
use crate::pd::PdClient;
use crate::region_manager::InProcessRegionManager;
use crate::rpc::RpcClient;
use crate::tso::Tso;

/// Go `New(path, pdAddrs, currentKeyspaceID, clusterKeyspaces)` over an
/// in-memory store: no store or region exists until the caller bootstraps
/// the cluster (Go's `mockstore` runs `BootstrapWithSingleStore` by
/// default). The store, its PD and the store's async-commit `minCommitTS`
/// share one oracle.
pub fn new(
    current_keyspace_id: u32,
    cluster_keyspaces: Vec<KeyspaceMeta>,
) -> Result<(RpcClient, PdClient, Arc<Cluster>), String> {
    let rm = Arc::new(InProcessRegionManager::new_mock_region_manager());
    let tso = Arc::new(Tso::new());
    let mock_pd = Arc::new(MockPd::new(Arc::clone(&rm), Arc::clone(&tso)));
    let handler = Arc::new(Mutex::new(KvHandler::new(
        MvccStore::with_pd(tso),
        Arc::clone(&rm),
    )));
    let cluster = Arc::new(Cluster::new(rm));
    let client = RpcClient::new(handler, Arc::clone(&cluster));
    let pd_client = PdClient::new(
        mock_pd,
        client.clone(),
        current_keyspace_id,
        cluster_keyspaces,
    )?;
    Ok((client, pd_client, cluster))
}
