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

//! The node's process authority over its client-rust store: Go's one
//! `kv.Storage` (`tikvStore`) with the coprocessor client and the transaction
//! opener every session shares. Statements reach it only through the shared
//! planner and executor (`tidb-executor::cluster_storage` / `remote_scan`).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tidb_distsql::{CopRuntimeConfig, CoprCache, CoprCacheConfig, QueryTransport};
use tidb_txnkv::driver::tikv_store::TikvStore;

/// The production node's coprocessor transport: Go's `pkg/store/copr` over
/// the node's client-rust store.
pub type ProductionReadTransport = tidb_distsql::cop_paging::NativeCopTransport<TikvStore>;

/// Opens worker-local coprocessor transports over process-owned capabilities.
///
/// This value contains no worker join authority: the process authority owns
/// the node's client-rust store, whose region cache, store clients and
/// background work every coprocessor read shares.
pub struct ProductionReadSessionFactory {
    tikv_store: Arc<TikvStore>,
    default_timeout: Duration,
    copr_cache: Option<CoprCache>,
}

/// Construct the process cache from the same effective TiKV settings used by
/// server startup. Zero capacity is a valid disabled store, with no workers.
pub fn configured_coprocessor_cache(
    config: &tidb_config::tikvcfg::CoprocessorCache,
) -> Result<Option<CoprCache>, ProcessAuthorityError> {
    CoprCache::from_config(&CoprCacheConfig {
        capacity_mb: config.capacity_mb,
        admission_max_ranges: config.admission_max_ranges,
        admission_max_result_mb: config.admission_max_result_mb,
        admission_min_process_ms: config.admission_min_process_ms,
    })
    .map_err(|error| ProcessAuthorityError::Transport(error.to_string()))
}

impl RealTiKvSessionTransportFactory for ProductionReadSessionFactory {
    type Transport = ProductionReadTransport;

    fn open_session_transport(&self) -> Result<Self::Transport, String> {
        Ok(tidb_distsql::cop_paging::NativeCopTransport::new(
            Arc::clone(&self.tikv_store),
            CopRuntimeConfig {
                default_timeout: self.default_timeout,
                shared_cache: self.copr_cache.clone(),
                ..CopRuntimeConfig::default()
            },
        ))
    }
}

/// Write capability re-exported so the server never depends on `tidb-txnkv`
/// directly; the dependency direction stays server -> exec -> txnkv.
pub use tidb_txnkv::transaction::RealOptimisticTransactionOpener;

/// Builds one worker-local query transport from process-owned shared handles.
///
/// An implementation may retain the unique process lifecycle owners together
/// with their cloneable request/cache handles, but it must give a session only
/// the handles. In particular, opening a session must not connect another PD
/// client, create a region cache, start a maintenance worker, or start a tonic
/// runtime. The returned transport deliberately has no `Send` bound because
/// the fixed server worker creates and consumes it on one thread.
///
/// The production implementation reads through the node's client-rust store
/// (`NativeCopTransport` over `TikvStore`).
pub trait RealTiKvSessionTransportFactory: Send + Sync {
    /// Worker-local transport type created for one admitted connection.
    type Transport: QueryTransport;

    /// Creates query-local state over the already-running process authority.
    fn open_session_transport(&self) -> Result<Self::Transport, String>;
}

/// Why the process authority could not be built.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProcessAuthorityError {
    /// The store or the coprocessor cache could not be constructed.
    Transport(String),
    /// The start-up work run over the new store (bootstrap, catalog load)
    /// failed.
    Startup(String),
}

impl fmt::Display for ProcessAuthorityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(message) => write!(formatter, "open the store: {message}"),
            Self::Startup(message) => write!(formatter, "start the node: {message}"),
        }
    }
}

impl std::error::Error for ProcessAuthorityError {}

/// The unique process authority over the node's store.
pub struct ProductionReadProcessAuthority {
    factory: Option<Arc<ProductionReadSessionFactory>>,
    transaction_opener: Option<RealOptimisticTransactionOpener>,
    copr_cache: Option<CoprCache>,
    tikv_store: TikvStore,
}

/// Why closing the process authority failed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadProcessShutdownError {
    /// Go `tikvStore.Close` failed.
    StoreClose(String),
}

impl fmt::Display for ReadProcessShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StoreClose(message) => write!(formatter, "close the store: {message}"),
        }
    }
}

impl std::error::Error for ReadProcessShutdownError {}

impl ProductionReadProcessAuthority {
    /// Connects the node's store over `pd_endpoints`, then runs `startup` over
    /// its transaction opener (Go `BootstrapSession` reading the stored
    /// catalog).
    pub fn connect<I, E, F>(
        pd_endpoints: I,
        timeout: Duration,
        startup: F,
    ) -> Result<Self, ProcessAuthorityError>
    where
        I: IntoIterator<Item = E>,
        E: Into<String>,
        F: FnOnce(&RealOptimisticTransactionOpener) -> Result<(), String>,
    {
        let pd_endpoints: Vec<String> = pd_endpoints.into_iter().map(Into::into).collect();
        let security = tidb_pd_client::ClusterSecurity::plaintext();
        let tikv_store = TikvStore::connect_with_security(pd_endpoints, &security)
            .map_err(ProcessAuthorityError::Transport)?;
        Self::connect_with_store(timeout, tikv_store, startup)
    }

    /// The authority over the node's store, `tikv_store`.
    pub fn connect_with_store<F>(
        timeout: Duration,
        tikv_store: TikvStore,
        startup: F,
    ) -> Result<Self, ProcessAuthorityError>
    where
        F: FnOnce(&RealOptimisticTransactionOpener) -> Result<(), String>,
    {
        let copr_cache = configured_coprocessor_cache(
            &tidb_config::tikvcfg::get_global_config()
                .tikv_client
                .copr_cache,
        )?;
        let factory = ProductionReadSessionFactory {
            tikv_store: Arc::new(tikv_store.clone()),
            default_timeout: timeout,
            copr_cache: copr_cache.clone(),
        };
        // Go's `tikvStore.Begin` over the node's one `KVStore`: transactions
        // share its region cache, sender, lock resolver and txn safe point.
        let transaction_opener =
            RealOptimisticTransactionOpener::from_process_capabilities(tikv_store.clone(), timeout)
                .map_err(|error| ProcessAuthorityError::Transport(error.to_string()))?
                // `@@tidb_enable_async_commit` / `@@tidb_enable_1pc`, resolved once for
                // the node exactly as `@@tidb_pessimistic_txn_fair_locking` is.
                .with_commit_protocol(crate::session_commit_protocol::bootstrap_commit_protocol());
        startup(&transaction_opener).map_err(ProcessAuthorityError::Startup)?;
        Ok(Self {
            factory: Some(Arc::new(factory)),
            transaction_opener: Some(transaction_opener),
            copr_cache,
            tikv_store,
        })
    }

    /// The node's client-rust store: region splitting and the GC worker share
    /// it, as Go's share the one `KVStore`.
    #[must_use]
    pub fn tikv_store(&self) -> &TikvStore {
        &self.tikv_store
    }

    /// The process-owned capability that opens worker-local transports.
    ///
    /// This is what a coprocessor-backed scan source needs and all it needs:
    /// it opens no PD client, no region cache and no runtime of its own (see
    /// [`RealTiKvSessionTransportFactory`]).
    #[must_use]
    pub fn transport_factory(&self) -> Arc<ProductionReadSessionFactory> {
        Arc::clone(
            self.factory
                .as_ref()
                .expect("production process authority is closed"),
        )
    }

    /// Cloneable write capability over the same store; it reconnects
    /// nothing.
    #[must_use]
    pub fn transaction_opener(&self) -> RealOptimisticTransactionOpener {
        self.transaction_opener
            .as_ref()
            .expect("production process authority is closed")
            .clone()
    }

    /// Closes the store (Go `tikvStore.Close`).
    pub fn shutdown(&mut self) -> Result<(), ReadProcessShutdownError> {
        if let Some(cache) = self.copr_cache.take() {
            cache.close();
        }
        drop(self.factory.take());
        drop(self.transaction_opener.take());
        // Go `mppStoreProber.Stop`: its probes send through the store.
        tidb_txnkv::global_mpp_failed_store_prober().stop();
        let store = self.tikv_store.store().clone();
        self.tikv_store
            .block_on(store.close())
            .map_err(|error| ReadProcessShutdownError::StoreClose(error.to_string()))
    }
}

#[cfg(test)]
mod coprocessor_cache_configuration_tests {
    use super::*;

    #[test]
    fn process_cache_uses_effective_tikv_configuration_and_allows_disabled() {
        let mut config = tidb_config::tikvcfg::CoprocessorCache {
            capacity_mb: 0.0,
            admission_max_ranges: 2,
            admission_max_result_mb: 0.5,
            admission_min_process_ms: 11,
        };
        assert!(configured_coprocessor_cache(&config).unwrap().is_none());
        config.capacity_mb = 1.0;
        let cache = configured_coprocessor_cache(&config).unwrap().unwrap();
        assert!(cache.check_request_admission(2));
        assert!(!cache.check_request_admission(3));
        assert!(!cache.check_response_admission(10, 10_000_000, 0));
        assert!(cache.check_response_admission(10, 11_000_000, 0));
        assert!(!cache.check_response_admission(524_289, 11_000_000, 0));
        cache.close();
    }
}
