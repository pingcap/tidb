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

//! Go `pkg/store/driver`'s `tikvStore`, the half that holds client-go's
//! `*tikv.KVStore`: the node's one client-rust store, with the runtime its
//! background work -- the txn safe point updater -- runs on. The GC worker,
//! region splitting and the read path's safe-point check share it, as Go's
//! share the one `KVStore`.

use std::sync::{Arc, OnceLock};

use tikv_client::tikv::KvStore;
use tikv_client::{PdClient, PdRpcClient};

use crate::gc_state::{StoreVisibility, VisibilityError};

/// Go `tikvStore`'s `*tikv.KVStore`, over any client-rust PD client: a real
/// cluster's, or the embedded store's.
pub struct TikvStore<PdC: PdClient = PdRpcClient> {
    runtime: Arc<tokio::runtime::Runtime>,
    store: KvStore<PdC>,
}

impl<PdC: PdClient> Clone for TikvStore<PdC> {
    fn clone(&self) -> Self {
        Self {
            runtime: Arc::clone(&self.runtime),
            store: self.store.clone(),
        }
    }
}

impl<PdC: PdClient> std::fmt::Debug for TikvStore<PdC> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TikvStore")
            .field("uuid", &self.store.uuid())
            .finish_non_exhaustive()
    }
}

/// The process's runtime for client-rust stores, the stand-in for Go's one
/// goroutine scheduler. It is never dropped, so a store or transaction
/// released on one of its threads never drops the runtime it runs on, and a
/// store's background work stops with the store itself (client-rust's tasks
/// hold it weakly).
pub fn store_runtime() -> Result<Arc<tokio::runtime::Runtime>, String> {
    static RUNTIME: OnceLock<Result<Arc<tokio::runtime::Runtime>, String>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_name("tikv-store")
                .enable_all()
                .build()
                .map(Arc::new)
                .map_err(|error| error.to_string())
        })
        .clone()
}

impl TikvStore {
    /// Go `tikv.NewKVStore` over the cluster whose PD answers at
    /// `pd_endpoints`: like client-go, it fails when the txn safe point
    /// cannot be loaded.
    pub fn connect(pd_endpoints: Vec<String>, config: tikv_client::Config) -> Result<Self, String> {
        let runtime = store_runtime()?;
        let store = runtime
            .block_on(KvStore::new_with_config(pd_endpoints, config))
            .map_err(|error| error.to_string())?;
        Ok(Self { runtime, store })
    }

    /// [`Self::connect`] with the TLS settings the node's other cluster
    /// clients use.
    pub fn connect_with_security(
        pd_endpoints: Vec<String>,
        security: &tidb_pd_client::ClusterSecurity,
    ) -> Result<Self, String> {
        let mut config = tikv_client::Config::default();
        if security.is_tls_enabled() {
            config = config.with_security(
                security.ca_path(),
                security.cert_path(),
                security.key_path(),
            );
        }
        Self::connect(pd_endpoints, config)
    }
}

impl<PdC: PdClient> TikvStore<PdC> {
    /// A node store over a client-rust store already built on `runtime`.
    #[must_use]
    pub fn new(runtime: Arc<tokio::runtime::Runtime>, store: KvStore<PdC>) -> Self {
        Self { runtime, store }
    }

    /// The runtime the store's calls block on.
    #[must_use]
    pub fn runtime(&self) -> &Arc<tokio::runtime::Runtime> {
        &self.runtime
    }

    /// client-go's `*tikv.KVStore`.
    #[must_use]
    pub fn store(&self) -> &KvStore<PdC> {
        &self.store
    }

    /// client-go `KVStore.CheckVisibility`.
    pub fn check_visibility(&self, start_ts: u64) -> Result<(), VisibilityError> {
        self.store
            .check_visibility(start_ts)
            .map_err(VisibilityError::from_client_error)
    }

    /// The check as the transactions opened over this store hold it; holding
    /// it keeps the store, and so its txn safe point updater, alive.
    #[must_use]
    pub fn visibility(&self) -> StoreVisibility {
        let store = self.clone();
        Arc::new(move |start_ts| store.check_visibility(start_ts))
    }
}
