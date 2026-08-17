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

//! The `--store unistore` run path: the same SQL node over the embedded store.
//!
//! Go boundary: `cmd/tidb-server` registers the unistore driver
//! (`session.RegisterStore("unistore", mockstore.EmbedUnistoreDriver{})`) and
//! everything above `kv.Storage` runs unchanged. This module is that
//! registration's Rust half: it builds the in-process capability triple
//! (client, region plane, TSO) from `tidb-unistore`, derives the SAME
//! generic session factory the production node uses, and serves the same
//! listener. No PD is dialed, no etcd is watched, and no catalog is loaded:
//! the served table comes from the command line, which is why the
//! cluster-catalog flags are refused by name below.

use std::sync::Arc;
use std::time::Duration;

use tidb_distsql::cop_paging::DirectUnaryRuntimeConfig;
use tidb_distsql::DirectUnaryQueryTransport;
use tidb_exec::real_tikv_read::{
    ReadSessionAdmissionOwner, RealTiKvReadSessionOpener, RealTiKvSessionTransportFactory,
};
use tidb_txnkv::gc_state::TxnSafePointRefresher;
use tidb_txnkv::pd_capability::CapabilityTimestampSource;
use tidb_txnkv::region::RegionCache;
use tidb_txnkv::transaction::RealOptimisticTransactionOpener;
use tidb_txnkv::{SharedReadAuthority, SharedReadOpener};
use tidb_unistore::client::InProcessClient;
use tidb_unistore::region_loader::{InProcessRegionLoader, IN_PROCESS_CLUSTER_ID};
use tidb_unistore::tso::InProcessPd;

use crate::node_config::NodeConfig;
use crate::real_tikv_node::{
    configured_account_store, configured_table, served_table_descriptor, RealTiKvSessionFactory,
    RunConfiguredNodeError,
};
use crate::sql_node::ConcurrentSqlNode;
use crate::SqlQueryError;

/// The per-statement RPC budget an in-process call gets. Nothing waits on a
/// network, so this bounds only local lock waits.
const IN_PROCESS_TIMEOUT: Duration = Duration::from_secs(20);

/// The transport an in-process session reads through: the SAME
/// `DirectUnaryQueryTransport` machinery as production, with the embedded
/// client and the whole-keyspace region plane underneath.
pub type InProcessReadTransport = DirectUnaryQueryTransport<InProcessClient, InProcessRegionLoader>;

/// The read-session factory over the embedded store, mirror of
/// `ProductionReadSessionFactory`: cloneable handles only, no lifecycle
/// ownership, no second worker per session.
pub struct InProcessReadSessionFactory {
    read_opener: SharedReadOpener<InProcessClient, InProcessRegionLoader>,
    lock_timestamp_source: CapabilityTimestampSource<InProcessPd>,
}

impl RealTiKvSessionTransportFactory for InProcessReadSessionFactory {
    type Transport = InProcessReadTransport;

    fn open_session_transport(&self) -> Result<Self::Transport, String> {
        DirectUnaryQueryTransport::from_read_authority(
            &self.read_opener,
            DirectUnaryRuntimeConfig {
                default_timeout: IN_PROCESS_TIMEOUT,
                ..DirectUnaryRuntimeConfig::default()
            },
            self.lock_timestamp_source.clone(),
        )
        .map_err(|error| error.to_string())
    }
}

/// The concrete factory instantiation the unistore node serves through --
/// same generic shape as production, embedded parameters throughout.
pub type UnistoreSessionFactory = RealTiKvSessionFactory<
    InProcessReadSessionFactory,
    CapabilityTimestampSource<InProcessPd>,
    InProcessClient,
    InProcessRegionLoader,
    InProcessPd,
>;

/// Builds the whole in-process node: store, region plane, TSO, transaction
/// opener, session factory. Fails closed on any flag that needs a cluster
/// catalog, naming the flag.
pub(crate) fn unistore_session_factory(
    config: &NodeConfig,
) -> Result<
    (
        UnistoreSessionFactory,
        SharedReadAuthority<InProcessClient, InProcessRegionLoader>,
        ReadSessionAdmissionOwner,
    ),
    SqlQueryError,
> {
    if !config.load_tables.is_empty() {
        return Err(SqlQueryError::unknown(
            "--store unistore serves command-line tables only; --load-table needs a cluster catalog",
        ));
    }
    if config.load_privileges {
        return Err(SqlQueryError::unknown(
            "--store unistore has no bootstrapped mysql.* to load; drop --load-privileges",
        ));
    }
    let table = match config.read_tables.as_slice() {
        [one] => configured_table(one),
        [] => {
            return Err(SqlQueryError::unknown(
                "--store unistore requires exactly one --read-table",
            ))
        }
        _ => {
            return Err(SqlQueryError::unknown(
                "multiple configured tables require the multi-relation dispatcher",
            ))
        }
    };

    let client = InProcessClient::new();
    let pd = InProcessPd::new();
    let cache = RegionCache::new(InProcessRegionLoader);
    let read_authority = SharedReadAuthority::start(client, cache)
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    // The embedded store never garbage-collects, so the read floor is a
    // static zero -- Go's unistore behavior for a store with no PD to ask.
    let gc_state = TxnSafePointRefresher::start_with_source(|| Ok(0))
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    let transaction_opener = RealOptimisticTransactionOpener::from_capabilities(
        read_authority.opener(),
        pd.clone(),
        IN_PROCESS_TIMEOUT,
        gc_state,
    )
    .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    // The opener's default protocol -- classic two-phase only -- stands: the
    // embedded store names its async-commit/1PC refusal in prewrite, so the
    // node must not offer what the store will abort.

    let transport_factory = InProcessReadSessionFactory {
        read_opener: read_authority.opener(),
        lock_timestamp_source: CapabilityTimestampSource(pd.clone()),
    };
    let (opener, admission) = RealTiKvReadSessionOpener::new_with_admission_owner(
        table,
        transport_factory,
        CapabilityTimestampSource(pd),
        IN_PROCESS_CLUSTER_ID,
    );
    let factory = RealTiKvSessionFactory::from_opener_parts(
        opener,
        transaction_opener,
        read_authority.authority_id(),
    );
    Ok((factory, read_authority, admission))
}

/// Runs the SQL node over the embedded store until shutdown.
///
/// Same listener, same session code, same flags as the production node;
/// only the store underneath differs, which is the entire point.
pub(crate) fn run_unistore_node(
    config: NodeConfig,
    spill_storage: Arc<tidb_util::disk::SpillStorage>,
    memory_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
) -> Result<(), RunConfiguredNodeError> {
    let users = configured_account_store(&config)?;
    let users = Arc::new(users);
    let (factory, read_authority, admission) =
        unistore_session_factory(&config).map_err(RunConfiguredNodeError::Engine)?;
    let factory = factory.with_spill_storage(spill_storage);
    let factory = match memory_arbitrator {
        Some(arbitrator) => factory.with_mem_arbitrator(arbitrator),
        None => factory,
    };
    let factory = Arc::new(factory);
    let served_table = factory.served_table().clone();
    let node = ConcurrentSqlNode::bind(&config, factory, Arc::clone(&users))
        .map_err(RunConfiguredNodeError::Node)?;
    let address = node.local_addr().map_err(RunConfiguredNodeError::Node)?;
    let shutdown_grace_ms = node.shutdown_grace_ms();
    let shutdown = node.shutdown_handle();
    ctrlc::set_handler(move || shutdown.shutdown()).map_err(RunConfiguredNodeError::Signal)?;
    let table_descriptors = served_table_descriptor(&served_table);
    eprintln!(
        "{{\"event\":\"sql_node_ready\",\"address\":\"{address}\",\"store\":\"unistore\",\"cluster_id\":{IN_PROCESS_CLUSTER_ID},\"tables\":[{table_descriptors}],\"max_connections\":{},\"account_count\":{},\"shutdown_grace_ms\":{shutdown_grace_ms}}}",
        config.max_connections,
        users.len(),
    );
    let result = node.run().map_err(RunConfiguredNodeError::Node);
    // The admission owner and read authority outlive every session by
    // construction; drop order alone ends the store with the node.
    drop(admission);
    drop(read_authority);
    result
}
