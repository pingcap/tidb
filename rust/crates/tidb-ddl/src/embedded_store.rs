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

use tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener;
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_pd_client::ClusterSecurity;
use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_unistore_tikv::kv_handler::KvHandler;
use tidb_unistore_tikv::mvcc_store::MvccStore;
use tidb_unistore_tikv::tso::InProcessPd;

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
    pub regions: Arc<tidb_unistore_tikv::region_manager::MockRegionManager>,
    /// The node's client-rust store, which the coprocessor reads through.
    pub tikv_store: Arc<TikvStore<tidb_unistore::mock::StorePdClient>>,
    /// The store's RPC client, whose send hook is Go's
    /// `UnistoreRPCClientSendHook`: a test's look at every request.
    pub rpc: tidb_unistore::rpc::RpcClient,
    /// The store's request handler, whose committed data a bootstrapped
    /// store image is taken from.
    pub handler: Arc<KvHandler>,
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
    in_process_stack_from_image_for_keyspace(security,image,None)
}

/// Go `mockstore.WithCurrentKeyspaceMeta` over the embedded store.
pub fn in_process_stack_from_image_for_keyspace(
    security:&ClusterSecurity,
    image:Option<&[u8]>,
    keyspace:Option<tikv_client::proto::keyspacepb::KeyspaceMeta>,
)->Result<InProcessStack,SqlQueryError> {
    let pd = InProcessPd::new();
    let regions = Arc::new(tidb_unistore::cluster::new_region_manager());
    // Go's unistore store holds THE cluster's mock PD client
    // (`mock_region.go`), so its async-commit/1PC `minCommitTS` draws from
    // the same oracle start timestamps do. The embedded store mirrors that:
    // one shared oracle, both faster protocols available.
    let mut store = MvccStore::with_pd(pd.oracle());
    if let Some(image) = image {
        store.load_data_image(image).map_err(SqlQueryError::unknown)?;
    }
    let handler = Arc::new(KvHandler::new(store, Arc::clone(&regions)));
    let (tikv_store, rpc) =
        embedded_tikv_store_for_keyspace(Arc::clone(&handler), Arc::clone(&regions), &pd, security,keyspace)?;
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
    handler: Arc<KvHandler>,
    regions: Arc<tidb_unistore_tikv::region_manager::MockRegionManager>,
    pd: &InProcessPd,
    security: &ClusterSecurity,
) -> Result<
    (
        TikvStore<tidb_unistore::mock::StorePdClient>,
        tidb_unistore::rpc::RpcClient,
    ),
    SqlQueryError,
> {
    embedded_tikv_store_for_keyspace(handler,regions,pd,security,None)
}

/// Go `mockstore.newUnistore` with `WithCurrentKeyspaceMeta`.
pub fn embedded_tikv_store_for_keyspace(
    handler:Arc<KvHandler>,
    regions:Arc<tidb_unistore_tikv::region_manager::MockRegionManager>,
    pd:&InProcessPd,
    security:&ClusterSecurity,
    mut keyspace:Option<tikv_client::proto::keyspacepb::KeyspaceMeta>,
)->Result<(TikvStore<tidb_unistore::mock::StorePdClient>,tidb_unistore::rpc::RpcClient),SqlQueryError> {
    if let Some(meta)=keyspace.as_mut() {
        meta.config.entry("gc_management_type".into()).or_insert_with(||"keyspace_level".into());
    }
    let mut config=tidb_txnkv::driver::tikv_store::cluster_security(security);
    if let Some(meta)=&keyspace {config=config.with_keyspace(&meta.name);}
    // Go `mockstore.NewMockStore` registers SYSTEM by default on nextgen.
    let keyspaces = if let Some(meta)=keyspace {vec![meta]} else if tidb_config_kerneltype::is_next_gen() {
        vec![tikv_client::proto::keyspacepb::KeyspaceMeta {
            keyspace: Some(tikv_client::proto::keyspacepb::keyspace_meta::Keyspace::Id(0x00ff_fffe)),
            name: "SYSTEM".to_owned(),
            ..Default::default()
        }]
    } else { Vec::new() };
    let (rpc, pd_client, _cluster) =
        tidb_unistore::mock::over(handler, regions, pd.oracle(), keyspaces)
            .map_err(SqlQueryError::unknown)?;
    let store = tidb_unistore::mock::new_test_tikv_store(
        &rpc,
        pd_client,
        config,
    )
    .map_err(SqlQueryError::unknown)?;
    Ok((store, rpc))
}

#[cfg(test)]
mod keyspace_tests {
    use super::*;
    use tidb_structure::transaction::RawTransaction as _;
    use tidb_txnkv::{NewTxnStorage as _, NewTxnTransaction as _};

    /// Go `zzprobe/o15_named_split.TestSplitRegionsInKeyspace`: split and
    /// duplicate/empty retries preserve logical transaction scan keys.
    #[test]
    fn test_split_regions_in_keyspace() {
        for name in ["SYSTEM", "tenant-split"] {
            let stack=in_process_stack_from_image_for_keyspace(&ClusterSecurity::plaintext(),None,Some(tikv_client::proto::keyspacepb::KeyspaceMeta {
                name:name.into(), keyspace:Some(tikv_client::proto::keyspacepb::keyspace_meta::Keyspace::Id(42)), ..Default::default()
            })).unwrap();
            let mut storage=tidb_store_driver_txn::meta_txn::MetaTxnStorage::new(&stack.opener,IN_PROCESS_TIMEOUT);
            let mut txn=storage.begin().unwrap();
            for key in [b"a",b"m",b"z"] {txn.set(key.to_vec(),key.to_vec()).unwrap();}
            txn.commit().unwrap();
            for keys in [vec![b"m".to_vec(),b"z".to_vec()],vec![b"m".to_vec(),b"m".to_vec()],Vec::new()] {
                let (_,result)=stack.tikv_store.block_on(stack.tikv_store.store().split_regions(keys,false,None));
                result.unwrap();
            }
            let mut txn=tidb_txnkv::kv_storage::Storage::begin_read_only(&stack.opener).unwrap();
            let pairs=txn.scan(b"a",b"zz",None,&tidb_txnkv::rpc::UnaryCallContext::with_timeout(IN_PROCESS_TIMEOUT)).unwrap();
            assert_eq!(pairs,vec![(b"a".to_vec(),b"a".to_vec()),(b"m".to_vec(),b"m".to_vec()),(b"z".to_vec(),b"z".to_vec())]);
            txn.finish_without_writes().unwrap();
        }
    }
}
