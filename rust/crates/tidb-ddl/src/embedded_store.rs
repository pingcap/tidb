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

//! Go `mockstore.NewMockStore` over unistore: the embedded store the `mysql`
//! bootstrap, the DDL worker's tests and `--store unistore` all run on.
//!
//! One build, [`in_process_stack`], yields the store's PD (TSO), its region
//! plane, the node's client-rust store and the transaction opener over all
//! three, so every surface derives from the same oracle and region cache.

use std::sync::Arc;
use std::time::Duration;

use tidb_txnkv::transaction::RealOptimisticTransactionOpener;
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_pd_client::ClusterSecurity;
use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_unistore::kv_handler::KvHandler;
use tidb_unistore::mvcc_store::MvccStore;
use tidb_unistore::tso::InProcessPd;

/// The per-statement RPC budget an in-process call gets. Nothing waits on a
/// network, so this bounds only local lock waits.
pub const IN_PROCESS_TIMEOUT: Duration = Duration::from_secs(20);

/// The embedded write stack: one store, its region plane, its TSO, and the
/// generic transaction opener over all three. Every unistore surface --
/// bootstrap, reads and writes alike -- derives from this one build.
pub type InProcessOpener = RealOptimisticTransactionOpener<tidb_unistore::mock::StorePdClient>;

/// The transaction half of [`InProcessStack`], for tests that open
/// transactions over the embedded store.
pub fn in_process_write_stack() -> Result<(InProcessPd, InProcessOpener), SqlQueryError> {
    in_process_stack(&ClusterSecurity::plaintext()).map(|stack| (stack.pd, stack.opener))
}

/// The embedded node's stores: the transaction stack, the region manager,
/// and the node's client-rust store (Go `tikvStore`'s `*tikv.KVStore`), which
/// the coprocessor reads through.
pub struct InProcessStack {
    /// The store's PD: the TSO oracle every start timestamp draws from.
    pub pd: InProcessPd,
    /// The transaction opener over the store, its regions and the oracle.
    pub opener: InProcessOpener,
    /// The region plane the store and its client share.
    pub regions: Arc<tidb_unistore::region_manager::InProcessRegionManager>,
    /// The node's client-rust store, which the coprocessor reads through.
    pub tikv_store: Arc<TikvStore<tidb_unistore::mock::StorePdClient>>,
    /// The store's RPC client, whose send hook is Go's
    /// `UnistoreRPCClientSendHook`: a test's look at every request.
    pub rpc: tidb_unistore::rpc::RpcClient,
    /// The store's request handler, whose committed data a bootstrapped
    /// store image is taken from.
    pub handler: Arc<std::sync::Mutex<KvHandler>>,
}

/// Builds [`InProcessStack`]; TiDB requests to peers use `security`, as Go's
/// mock store dials them with the cluster security.
pub fn in_process_stack(security: &ClusterSecurity) -> Result<InProcessStack, SqlQueryError> {
    in_process_stack_from_image(security, None)
}

/// [`in_process_stack`] over a store that starts from `image`, a
/// [`MvccStore::data_image`] of an earlier store (Go testkit's
/// bootstrapped store image), or empty without one.
pub fn in_process_stack_from_image(
    security: &ClusterSecurity,
    image: Option<&[u8]>,
) -> Result<InProcessStack, SqlQueryError> {
    let pd = InProcessPd::new();
    let regions = Arc::new(tidb_unistore::region_manager::InProcessRegionManager::new());
    // Go's unistore store holds THE cluster's mock PD client
    // (`mock_region.go`), so its async-commit/1PC `minCommitTS` draws from
    // the same oracle start timestamps do. The embedded store mirrors that:
    // one shared oracle, both faster protocols available.
    let mut store = MvccStore::with_pd(pd.oracle());
    if let Some(image) = image {
        store.load_data_image(image).map_err(SqlQueryError::unknown)?;
    }
    let handler = Arc::new(std::sync::Mutex::new(KvHandler::new(store, Arc::clone(&regions))));
    let (tikv_store, rpc) =
        embedded_tikv_store(Arc::clone(&handler), Arc::clone(&regions), &pd, security)?;
    // The bootstrapped cluster protocol -- async commit and 1PC are both ON
    // on TiKV (`session_commit_protocol`) -- and the shared-oracle store can
    // answer both, exactly as Go's unistore does under its mock PD.
    let transaction_opener =
        RealOptimisticTransactionOpener::from_capabilities(tikv_store.clone(), IN_PROCESS_TIMEOUT)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?
            .with_commit_protocol(tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol());
    Ok(InProcessStack {
        pd,
        opener: transaction_opener,
        regions,
        tikv_store: Arc::new(tikv_store),
        rpc,
        handler,
    })
}

/// Go `mockstore.NewMockStore`'s `tikv.NewTestTiKVStore` over unistore: the
/// node's client-rust store keeps its region cache and routing over
/// unistore's PD and RPC clients, which reach the same handler, regions and
/// oracle, and loads its txn safe point from the mock PD's GC states like a
/// real cluster's.
pub fn embedded_tikv_store(
    handler: Arc<std::sync::Mutex<KvHandler>>,
    regions: Arc<tidb_unistore::region_manager::InProcessRegionManager>,
    pd: &InProcessPd,
    security: &ClusterSecurity,
) -> Result<
    (
        TikvStore<tidb_unistore::mock::StorePdClient>,
        tidb_unistore::rpc::RpcClient,
    ),
    SqlQueryError,
> {
    let (rpc, pd_client, _cluster) =
        tidb_unistore::mock::over(handler, regions, pd.oracle(), Vec::new())
            .map_err(SqlQueryError::unknown)?;
    let store = tidb_unistore::mock::new_test_tikv_store(
        &rpc,
        pd_client,
        tidb_txnkv::driver::tikv_store::cluster_security_config(security),
    )
    .map_err(SqlQueryError::unknown)?;
    Ok((store, rpc))
}
