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

//! client-go's `*tikv.KVStore` as TiDB holds it (Go `pkg/store/driver`'s
//! `tikvStore` embeds it; Go's `store/copr` and `store/gcworker` use it
//! directly): the node's one client-rust store, with the runtime its
//! background work -- the txn safe point updater -- runs on. The GC worker,
//! region splitting and the read path's safe-point check share it, as Go's
//! share the one `KVStore`. It also holds the node's one PD HTTP client (Go
//! `KVStore.GetPDHTTPClient`), which every PD HTTP API user reaches through
//! it.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tikv_client::pd_http;
use tikv_client::tikv::KvStore;
use tikv_client::{PdClient, PdRpcClient};

use crate::gc_state::{StoreVisibility, VisibilityError};

/// Go `tikvStore`'s `*tikv.KVStore`, over any client-rust PD client: a real
/// cluster's, or the embedded store's.
pub struct TikvStore<PdC: PdClient = PdRpcClient> {
    runtime: Arc<tokio::runtime::Runtime>,
    store: KvStore<PdC>,
    authority_id: u64,
    /// Go `KVStore.pdHttpClient`: the node's one PD HTTP client, absent for
    /// an embedded store, which has no PD server (Go's mockstore has none
    /// either).
    pd_http_client: Option<pd_http::Client>,
}

static NEXT_AUTHORITY_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn next_authority_id() -> u64 {
    NEXT_AUTHORITY_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

impl<PdC: PdClient> Clone for TikvStore<PdC> {
    fn clone(&self) -> Self {
        Self {
            runtime: Arc::clone(&self.runtime),
            store: self.store.clone(),
            authority_id: self.authority_id,
            pd_http_client: self.pd_http_client.clone(),
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
    ///
    /// The store gets the node's PD HTTP client as Go's `TiKVDriver.Open`
    /// gives it one: `tikv.WithPDHTTPClient("tikv-driver", ...,
    /// pdhttp.WithTLSConfig(tlsConfig), pdhttp.WithMetrics(...))`, which is
    /// `pdhttp.NewClientWithServiceDiscovery` over the store's PD client's
    /// service discovery, so a request follows PD's current leader.
    /// `WithMetrics` is not passed: Go's `metrics.PDAPIRequestCounter` and
    /// `PDAPIExecutionHistogram` are ported in `tidb-server` on a different
    /// `prometheus` major version than client-rust's, so the client records
    /// no PD API metrics.
    pub fn connect(pd_endpoints: Vec<String>, config: tikv_client::Config) -> Result<Self, String> {
        let runtime = store_runtime()?;
        // Go `d.security.ToTLSConfig()`: no CA means plain HTTP.
        let tls_config = config
            .security
            .to_tls_config()
            .map_err(|error| error.to_string())?;
        let store = runtime
            .block_on(KvStore::new_with_config(pd_endpoints, config))
            .map_err(|error| error.to_string())?;
        let mut opts = Vec::new();
        if let Some(tls_config) = tls_config {
            opts.push(pd_http::with_tls_config(Arc::new(tls_config)));
        }
        let pd_http_client = pd_http::new_client_with_service_discovery(
            "tikv-driver",
            store.pd_client().get_service_discovery(),
            opts,
        )
        .map_err(|error| error.to_string())?;
        let mut store = Self::new(runtime, store);
        store.pd_http_client = Some(pd_http_client);
        Ok(store)
    }

    /// [`Self::connect`] with the TLS settings the node's other cluster
    /// clients use.
    pub fn connect_with_security(
        pd_endpoints: Vec<String>,
        security: &tidb_pd_client::ClusterSecurity,
    ) -> Result<Self, String> {
        Self::connect(pd_endpoints, cluster_security_config(security))
    }
}

/// Go `config.Security.ClusterSecurity()` as client-rust takes it: the
/// default store config with the cluster TLS files `security` names.
#[must_use]
pub fn cluster_security_config(security: &tidb_pd_client::ClusterSecurity) -> tikv_client::Config {
    let config = tikv_client::Config::default();
    if security.is_tls_enabled() {
        config.with_security(
            security.ca_path(),
            security.cert_path(),
            security.key_path(),
        )
    } else {
        config
    }
}

impl<PdC: PdClient> TikvStore<PdC> {
    /// A node store over a client-rust store already built on `runtime`.
    #[must_use]
    pub fn new(runtime: Arc<tokio::runtime::Runtime>, store: KvStore<PdC>) -> Self {
        Self {
            runtime,
            store,
            authority_id: next_authority_id(),
            pd_http_client: None,
        }
    }

    /// The PD cluster the store serves, as PD names it.
    #[must_use]
    pub fn cluster_id(&self) -> u64 {
        self.store.cluster_id()
    }

    /// Go `KVStore.GetPDHTTPClient`: the node's PD HTTP client, `None` for a
    /// store [`Self::connect`] did not build (the embedded store has no PD
    /// server). Its requests are async; synchronous callers run them with
    /// [`Self::block_on`] or [`block_on_store_runtime`].
    #[must_use]
    pub fn pd_http_client(&self) -> Option<pd_http::Client> {
        self.pd_http_client.clone()
    }

    /// Go `store.GetOracle().SetExternalTimestamp` with `Some`, and
    /// `GetExternalTimestamp` with `None`.
    pub fn external_timestamp(&self, value: Option<u64>) -> Result<u64, String> {
        let oracle = self.store.get_oracle();
        self.block_on(async move {
            match value {
                Some(timestamp) => oracle
                    .set_external_timestamp(timestamp)
                    .await
                    .map(|()| timestamp),
                None => oracle.get_external_timestamp().await,
            }
            .map_err(|error| error.to_string())
        })
    }

    /// PD's GC safe point for the store's keyspace (Go reads it from the GC
    /// state the store's PD client answers).
    pub fn gc_safe_point(&self) -> Result<u64, String> {
        let pd = self.store.pd_client();
        self.block_on(async move { pd.get_gc_state().await })
            .map(|state| state.gc_safe_point)
            .map_err(|error| error.to_string())
    }

    /// The TiKV stores' client addresses, as the store's region cache reads
    /// them through PD.
    pub fn store_addresses(&self) -> Result<Vec<String>, String> {
        let pd = self.store.pd_client();
        let stores = self
            .block_on(async move { pd.all_stores().await })
            .map_err(|error| error.to_string())?;
        Ok(stores
            .into_iter()
            .filter(|store| store.endpoint_type == tikv_client::tikv::EndpointType::TiKv)
            .map(|store| store.target)
            .collect())
    }

    /// Go `infoschema.GetTiFlashStoreCount`: the stores PD lists whose type
    /// (`tikvrpc.GetStoreTypeByMeta`) is TiFlash.
    pub fn tiflash_store_count(&self) -> Result<u64, String> {
        #[cfg(feature = "failpoints")]
        fail::fail_point!("mockTiFlashStoreCount", |_| Ok(10));
        let pd = self.store.pd_client();
        let stores = self
            .block_on(async move { pd.all_stores().await })
            .map_err(|error| error.to_string())?;
        Ok(stores
            .into_iter()
            .filter(|store| store.endpoint_type == tikv_client::tikv::EndpointType::TiFlash)
            .count() as u64)
    }

    /// The store's identity in this process: transactions opened over it
    /// carry it, so a receipt names the store that committed it.
    #[must_use]
    pub const fn authority_id(&self) -> u64 {
        self.authority_id
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

    /// Runs `operation` on the store's runtime from synchronous code -- the
    /// coprocessor's worker threads, the session's statement thread. A
    /// caller already on a multi-thread runtime hands its worker back while it
    /// waits; one on a current-thread runtime waits on a scoped thread.
    ///
    /// The calling thread's span is the operation's Go `ctx` span: under a
    /// traced statement the client spans it opens report under it
    /// ([`crate::rpc::trace::with_span_ctx`]).
    pub fn block_on<F>(&self, operation: F) -> F::Output
    where
        F: std::future::Future + Send,
        F::Output: Send,
    {
        let span = tidb_tracing::current_span();
        block_on_runtime(
            &self.runtime,
            crate::rpc::trace::with_span_ctx(span, operation),
        )
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

    /// Go `tikvStore.GetLockWaits`: every TiKV store's lock-wait entries,
    /// each store asked once through the store's TiKV client with Go's 30s
    /// timeout. A store that fails to answer is skipped, as Go logs and
    /// moves on.
    pub fn get_lock_waits(&self) -> Result<Vec<tidb_proto::KvrpcWaitForEntry>, String> {
        const LOCK_WAIT_INFO_TIMEOUT: Duration = Duration::from_secs(30);
        self.block_on(async {
            let stores = self
                .store
                .pd_client()
                .all_stores()
                .await
                .map_err(|error| error.to_string())?;
            let mut result = Vec::new();
            for store in stores {
                if store.endpoint_type != tikv_client::tikv::EndpointType::TiKv {
                    continue;
                }
                let mut request = tikv_client::proto::kvrpcpb::GetLockWaitInfoRequest::default();
                let response = match self
                    .store
                    .send_request(&store.target, &mut request, LOCK_WAIT_INFO_TIMEOUT)
                    .await
                {
                    Ok(response) => response,
                    Err(error) => {
                        tidb_logutil::bg_logger().warn(
                            "query lock wait info failed",
                            &[tidb_log::Field::new(
                                "error",
                                tidb_log::Value::Str(error.to_string()),
                            )],
                        );
                        continue;
                    }
                };
                match response.downcast::<tikv_client::proto::kvrpcpb::GetLockWaitInfoResponse>() {
                    Ok(response) => result.extend(response.entries),
                    Err(_) => tidb_logutil::bg_logger()
                        .warn("lock wait info from store is nil", &[]),
                }
            }
            Ok(result)
        })
    }

    /// Go `store.GetOracle().GetTimestampAsync(ctx, &oracle.Option{TxnScope:
    /// oracle.GlobalTxnScope})`: a timestamp request dispatched now on the
    /// store's oracle, waited for by [`StoreTimestampFuture::wait`].
    pub fn get_timestamp_async(&self) -> StoreTimestampFuture {
        let option = tikv_client::oracle::OracleOption {
            txn_scope: tikv_client::oracle::GLOBAL_TXN_SCOPE.to_owned(),
        };
        // The PD oracle spawns its request on the current runtime.
        let _runtime = self.runtime.enter();
        StoreTimestampFuture {
            runtime: Arc::clone(&self.runtime),
            future: self.store.get_oracle().get_timestamp_async(&option),
        }
    }

    /// Go `KVStore.GetLockResolver().Resolving()`: the locks this store's
    /// transactions are resolving now.
    pub fn resolving_locks(&self) -> Vec<tikv_client::transaction::ResolvingLock> {
        self.block_on(self.store.get_lock_resolver().resolving_locks())
    }
}

/// Go `oracle.Future` from the store's oracle.
pub struct StoreTimestampFuture {
    runtime: Arc<tokio::runtime::Runtime>,
    future: Box<dyn tikv_client::oracle::TimestampFuture>,
}

impl StoreTimestampFuture {
    /// Go `oracle.Future.Wait`.
    pub fn wait(self) -> Result<u64, String> {
        let Self { runtime, future } = self;
        block_on_runtime(&runtime, async move {
            future.wait().await.map_err(|error| error.to_string())
        })
    }
}

/// [`TikvStore::block_on`] on the process's store runtime, for components
/// that hold only the store's PD HTTP client: its connections are pooled on
/// the runtime that opened them, so every request runs on the one runtime
/// that is never dropped.
pub fn block_on_store_runtime<F>(operation: F) -> Result<F::Output, String>
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    let runtime = store_runtime()?;
    Ok(block_on_runtime(&runtime, operation))
}

/// Go `servicediscovery.NewMockServiceDiscovery`: a fixed member list, none
/// of them the leader, whose membership checks do nothing.
struct MockServiceDiscovery {
    clients: Vec<pd_http::ServiceClient>,
}

#[async_trait::async_trait]
impl pd_http::ServiceDiscovery for MockServiceDiscovery {
    async fn get_all_service_clients(&self) -> Vec<pd_http::ServiceClient> {
        self.clients.clone()
    }

    fn schedule_check_member_changed(&self) {}

    async fn close(&self) {}
}

/// Go pdhttp's `newClientWithMockServiceDiscovery`: a PD HTTP client over
/// the fixed member URLs `pd_addrs` (with scheme), for tests that serve PD's
/// HTTP API from a fake server. Its requests run on the store runtime
/// ([`block_on_store_runtime`]) like the node's client's.
pub fn new_pd_http_client_with_mock_service_discovery(
    source: &str,
    pd_addrs: &[String],
    opts: Vec<pd_http::ClientOption>,
) -> Result<pd_http::Client, String> {
    let sd = Arc::new(MockServiceDiscovery {
        clients: pd_addrs
            .iter()
            .map(|url| pd_http::ServiceClient::new(url.clone(), false))
            .collect(),
    });
    pd_http::new_client_with_service_discovery(source, sd, opts).map_err(|error| error.to_string())
}

/// [`TikvStore::block_on`] on `runtime`.
fn block_on_runtime<F>(runtime: &tokio::runtime::Runtime, operation: F) -> F::Output
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| runtime.block_on(operation))
        }
        Ok(_) => std::thread::scope(|scope| {
            scope
                .spawn(|| runtime.block_on(operation))
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
        }),
        Err(_) => runtime.block_on(operation),
    }
}
