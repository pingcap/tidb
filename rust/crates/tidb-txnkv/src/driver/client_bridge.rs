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

//! Client-rust injection over the process-owned TiDB storage capabilities.
//!
//! This module only adapts routing metadata and typed RPCs. Transaction phases,
//! retries, lock resolution and heartbeat tasks belong to client-rust.

use crate::lock::{LockRecoveryClient, TimestampSource};
use crate::region::{LeaderRequest, RegionLocation, RegionQueryLoader, RegionRecoveryLoader};
use crate::rpc::UnaryCallContext;
use crate::transaction::{PublishedCommand, TransactionCommandClient};
use crate::SharedReadRuntime;
use async_trait::async_trait;
use prost::Message;
use std::any::Any;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tikv_client::proto::{keyspacepb, kvrpcpb, metapb};
use tikv_client::tikv::{Client as KvClient, RegionStore, Request, Store};
use tikv_client::tikv::{
    MixedReplicaSelection, ReplicaCandidate, ReplicaFlowsType, ReplicaRouting,
    ReplicaSelectorState, StoreLiveness as NativeLiveness,
};
use tikv_client::PdClient;
use tikv_client::ReplicaReadConfig;
use tikv_client::{Error, Key, Result, Timestamp, TimestampExt};
use tikv_client::{RegionVerId, RegionWithLeader};

pub(crate) fn runtime() -> Arc<tokio::runtime::Runtime> {
    static RUNTIME: OnceLock<Arc<tokio::runtime::Runtime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(48)
                    .thread_name("tidb-kv-client")
                    .enable_all()
                    .build()
                    .expect("transaction runtime initialization"),
            )
        })
        .clone()
}

#[derive(Debug)]
pub(crate) enum ResolverBridgeError {
    Timestamp(String),
    CallerCancelled,
}

impl std::fmt::Display for ResolverBridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timestamp(error) => f.write_str(error),
            Self::CallerCancelled => f.write_str("context canceled"),
        }
    }
}
impl std::error::Error for ResolverBridgeError {}

impl ResolverBridgeError {
    fn native(self) -> Error {
        match self {
            Self::CallerCancelled => Error::ContextCanceled,
            error => Error::Io(std::io::Error::other(error)),
        }
    }
}

fn failure(error: impl std::fmt::Display) -> Error {
    Error::StringError(error.to_string())
}
fn region_id(id: RegionVerId) -> crate::region::RegionVerId {
    crate::region::RegionVerId::new(id.id, id.conf_ver, id.ver)
}
fn client_region(location: RegionLocation) -> RegionWithLeader {
    let peers: Vec<_> = location
        .peers
        .iter()
        .map(|peer| metapb::Peer {
            id: peer.id,
            store_id: peer.store_id,
            role: peer.role.as_i32(),
            is_witness: peer.is_witness,
        })
        .collect();
    let leader = peers
        .iter()
        .find(|peer| Some(peer.id) == location.leader_peer_id)
        .cloned();
    RegionWithLeader::new(
        metapb::Region {
            id: location.region.id,
            start_key: location.start_key,
            end_key: location.end_key,
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: location.region.epoch.conf_ver,
                version: location.region.epoch.version,
            }),
            peers,
            ..Default::default()
        },
        leader,
    )
}

trait Backend: Send + Sync {
    fn timestamp(&self) -> Result<Timestamp>;
    fn cluster_id(&self) -> u64;
    fn locate_key(&self, key: &[u8], end: bool) -> Result<RegionWithLeader>;
    fn locate_id(&self, id: u64) -> Result<RegionLocation>;
    fn store_state(&self, id: u64) -> Result<crate::region::StoreState>;
    fn candidates(
        &self,
        region: RegionVerId,
        labels: &[metapb::StoreLabel],
        stores: &[u64],
        state: &ReplicaSelectorState,
    ) -> Result<Vec<ReplicaCandidate>>;
    fn route(
        &self,
        region: RegionVerId,
        target: u64,
        proxy: Option<u64>,
        forwarding: bool,
    ) -> Result<LeaderRequest>;
    fn proxy(
        &self,
        region: RegionVerId,
        leader: u64,
        state: &ReplicaSelectorState,
    ) -> Result<Option<u64>>;
    fn route_feedback(&self, route: &LeaderRequest, success: bool);
    fn record_server_load(&self, id: u64, estimated_wait_ms: u32);
    fn update_leader(&self, id: RegionVerId, leader: metapb::Peer) -> Result<()>;
    fn update_regions(&self, regions: Vec<RegionWithLeader>) -> Result<()>;
    fn invalidate_region(&self, id: RegionVerId);
    fn invalidate_store(&self, id: u64);
    fn dispatch(
        &self,
        address: &str,
        request: &dyn Request,
        call: &UnaryCallContext,
    ) -> Result<Box<dyn Any + Send>>;
}

type Dispatch<C, L> = fn(
    &SharedReadRuntime<C, L>,
    &Mutex<ClientTrace>,
    &str,
    &dyn Request,
    &UnaryCallContext,
) -> Result<Box<dyn Any + Send>>;

struct StorageBackend<C, L, T> {
    storage: SharedReadRuntime<C, L>,
    timestamps: Mutex<T>,
    trace: Arc<Mutex<ClientTrace>>,
    dispatch: Dispatch<C, L>,
}
impl<C, L, T> Backend for StorageBackend<C, L, T>
where
    C: LockRecoveryClient + Send + 'static,
    L: RegionRecoveryLoader + RegionQueryLoader + Send + 'static,
    T: TimestampSource + Send + 'static,
{
    fn timestamp(&self) -> Result<Timestamp> {
        self.timestamps
            .lock()
            .map_err(failure)?
            .current_ts()
            .map(Timestamp::from_version)
            .map_err(|error| ResolverBridgeError::Timestamp(error).native())
    }
    fn cluster_id(&self) -> u64 {
        self.storage.cluster_id()
    }
    fn locate_key(&self, key: &[u8], end: bool) -> Result<RegionWithLeader> {
        let location = if end {
            self.storage
                .with_region_cache(|cache| cache.locate_end_key(key).cloned())
        } else {
            self.storage.locate_key(key)
        };
        location
            .map_err(failure)?
            .map(client_region)
            .map_err(failure)
    }
    fn locate_id(&self, id: u64) -> Result<RegionLocation> {
        self.storage
            .with_region_cache(|cache| cache.locate_region_by_id(id))
            .map_err(failure)?
            .map_err(failure)
    }
    fn store_state(&self, id: u64) -> Result<crate::region::StoreState> {
        self.storage
            .with_region_cache(|cache| cache.store_state(id).cloned())
            .map_err(failure)?
            .ok_or_else(|| failure("region store is missing"))
    }
    fn candidates(
        &self,
        region: RegionVerId,
        labels: &[metapb::StoreLabel],
        stores: &[u64],
        state: &ReplicaSelectorState,
    ) -> Result<Vec<ReplicaCandidate>> {
        self.storage
            .with_region_cache(|cache| {
                cache.native_replica_candidates(region_id(region), labels, stores, state)
            })
            .map_err(failure)?
            .map_err(failure)
    }
    fn route(
        &self,
        region: RegionVerId,
        target: u64,
        proxy: Option<u64>,
        forwarding: bool,
    ) -> Result<LeaderRequest> {
        self.storage
            .with_region_cache(|cache| {
                cache.native_route(region_id(region), target, proxy, forwarding)
            })
            .map_err(failure)?
            .map_err(failure)
    }
    fn proxy(
        &self,
        region: RegionVerId,
        leader: u64,
        state: &ReplicaSelectorState,
    ) -> Result<Option<u64>> {
        self.storage
            .with_region_cache(|cache| cache.native_proxy(region_id(region), leader, state))
            .map_err(failure)?
            .map_err(failure)
    }
    fn route_feedback(&self, route: &LeaderRequest, success: bool) {
        if success {
            if route.cached_leader {
                let _ = self.storage.with_region_cache(|cache| {
                    cache.apply_route_feedback(&crate::region::RouteFeedback::from_request(
                        route,
                        crate::region::RouteOutcome::Success,
                    ))
                });
            }
            return;
        }
        // Probe outside the cache lock. Publication validates both captured
        // generations, including when a concurrent refresh completes meanwhile.
        let worker = self
            .storage
            .client()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .fork_for_async_worker();
        let liveness = if let Some(mut worker) = worker {
            // The real transport clone shares channels, not the session's
            // command mutex. Health I/O must not block unrelated publications.
            worker.store_liveness_for_route(route.dispatch_address())
        } else {
            self.storage
                .client()
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .store_liveness_for_route(route.dispatch_address())
        };
        let _ = self.storage.with_region_cache(|cache| {
            let result = cache.on_route_send_failure(route, liveness);
            if matches!(
                result,
                Ok(crate::region::StoreFailureOutcome::Invalidated { .. })
            ) {
                // Native retries keep their immutable peer vector. The next
                // cache lookup must refresh stale store epochs instead of
                // handing a new statement the same invalidated snapshot.
                cache.mark_reload_on_access(route.attempt.region);
            }
            result
        });
        let _ = self.storage.trigger_store_check();
    }
    fn record_server_load(&self, id: u64, estimated_wait_ms: u32) {
        let _ = self.storage.with_region_cache(|cache| {
            cache.record_native_server_load(id, estimated_wait_ms);
        });
    }
    fn update_leader(&self, id: RegionVerId, leader: metapb::Peer) -> Result<()> {
        self.storage
            .with_region_cache(|cache| {
                if !cache.update_leader(region_id(id.clone()), leader.id, leader.store_id) {
                    cache.invalidate(region_id(id));
                }
            })
            .map_err(failure)
    }
    fn update_regions(&self, regions: Vec<RegionWithLeader>) -> Result<()> {
        let regions = regions
            .into_iter()
            .map(|region| {
                let mut proto = region.region;
                // The client codec decoded EpochNotMatch. The shared loader
                // consumes PD/TiKV wire boundaries, so restore that domain.
                for boundary in [&mut proto.start_key, &mut proto.end_key] {
                    if !boundary.is_empty() {
                        let raw = std::mem::take(boundary);
                        tidb_codec::encode_bytes(boundary, &raw);
                    }
                }
                let metadata = crate::region::recovery::region_metadata(&proto).map_err(failure)?;
                let leader_store = region.leader.as_ref().map_or(0, |peer| peer.store_id);
                Ok((metadata, leader_store))
            })
            .collect::<Result<Vec<_>>>()?;
        self.storage
            .region_cache_handle()
            .update_client_regions(regions)
            .map_err(failure)?
            .map_err(failure)
    }
    fn invalidate_region(&self, id: RegionVerId) {
        let _ = self
            .storage
            .with_region_cache(|cache| cache.invalidate(region_id(id)));
    }
    fn invalidate_store(&self, id: u64) {
        let _ = self
            .storage
            .with_region_cache(|cache| cache.invalidate_store(id));
    }
    fn dispatch(
        &self,
        address: &str,
        request: &dyn Request,
        call: &UnaryCallContext,
    ) -> Result<Box<dyn Any + Send>> {
        (self.dispatch)(&self.storage, &self.trace, address, request, call)
    }
}

fn encode_native_get(request: &kvrpcpb::GetRequest, context: &tidb_proto::KvrpcContext) -> Vec<u8> {
    let mut request = request.clone();
    let native_context = request.context.get_or_insert_with(Default::default);
    native_context.cluster_id = context.cluster_id;
    native_context.request_origin = context.request_origin;
    request.encode_to_vec()
}

fn dispatch_transaction<C, L>(
    storage: &SharedReadRuntime<C, L>,
    trace: &Mutex<ClientTrace>,
    address: &str,
    request: &dyn Request,
    call: &UnaryCallContext,
) -> Result<Box<dyn Any + Send>>
where
    C: TransactionCommandClient + LockRecoveryClient + Clone,
    L: RegionRecoveryLoader,
{
    let mut client = storage.client().lock().map_err(failure)?.clone();
    // Point gets dominate prepared point-update workloads. Keep the
    // client-rust protobuf bytes intact across the transport boundary;
    // decoding into tidb-proto and encoding again costs an allocation on
    // every RPC.
    if let Some(request) = request.as_any().downcast_ref::<kvrpcpb::GetRequest>() {
        let mut context: tidb_proto::KvrpcContext = request.context.clone().unwrap_or_default();
        context.cluster_id = storage.cluster_id();
        context.request_origin = tidb_proto::KvrpcRequestOrigin::TiDb as i32;
        if let Some(published) = client.publish_raw_transaction(
            address,
            crate::rpc::BatchCommandTag::Get,
            encode_native_get(request, &context),
            &context,
            call,
        ) {
            trace
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .observe_native_get(request, &context, &published);
            let response = match published {
                PublishedCommand::BeforePublication(error) => return Err(failure(error)),
                PublishedCommand::AfterPublication { error, .. } => {
                    return Err(Error::GrpcAPI(tonic::Status::unavailable(error)))
                }
                PublishedCommand::Response(response) => response.response,
            };
            return Ok(Box::new(response));
        }
    }
    macro_rules! command {
        ($request:ident, $response:ident, $method:ident) => {
            if let Some(request) = request.as_any().downcast_ref::<kvrpcpb::$request>() {
                let mut request = request.clone();
                let mut context = request.context.take().unwrap_or_default();
                context.cluster_id = storage.cluster_id();
                context.request_origin = tidb_proto::KvrpcRequestOrigin::TiDb as i32;
                request.context = Some(context.clone());
                let published = client.$method(address, &request, &context, call);
                trace
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .observe(&request, &context, address, &published);
                let response = match published {
                    PublishedCommand::BeforePublication(error) => return Err(failure(error)),
                    PublishedCommand::AfterPublication { error, .. } => {
                        return Err(Error::GrpcAPI(tonic::Status::unavailable(error)))
                    }
                    PublishedCommand::Response(response) => response.response,
                };
                return Ok(Box::new(response));
            }
        };
    }
    command!(GetRequest, GetResponse, publish_transaction_get);
    command!(
        BatchGetRequest,
        BatchGetResponse,
        publish_transaction_batch_get
    );
    command!(ScanRequest, ScanResponse, publish_transaction_scan);
    command!(PrewriteRequest, PrewriteResponse, publish_prewrite);
    command!(CommitRequest, CommitResponse, publish_commit);
    command!(
        BatchRollbackRequest,
        BatchRollbackResponse,
        publish_batch_rollback
    );
    command!(
        PessimisticLockRequest,
        PessimisticLockResponse,
        publish_pessimistic_lock
    );
    command!(
        PessimisticRollbackRequest,
        PessimisticRollbackResponse,
        publish_pessimistic_rollback
    );
    command!(
        TxnHeartBeatRequest,
        TxnHeartBeatResponse,
        publish_txn_heart_beat
    );
    dispatch_lock_request(&mut client, storage.cluster_id(), address, request, call)
}

fn dispatch_resolver<C, L>(
    storage: &SharedReadRuntime<C, L>,
    _trace: &Mutex<ClientTrace>,
    address: &str,
    request: &dyn Request,
    call: &UnaryCallContext,
) -> Result<Box<dyn Any + Send>>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
{
    let worker = storage
        .client()
        .lock()
        .map_err(failure)?
        .fork_for_async_worker();
    if let Some(mut worker) = worker {
        dispatch_lock_request(
            worker.as_mut(),
            storage.cluster_id(),
            address,
            request,
            call,
        )
    } else {
        dispatch_lock_request(
            &mut *storage.client().lock().map_err(failure)?,
            storage.cluster_id(),
            address,
            request,
            call,
        )
    }
}

fn dispatch_lock_request(
    client: &mut dyn LockRecoveryClient,
    cluster_id: u64,
    address: &str,
    request: &dyn Request,
    call: &UnaryCallContext,
) -> Result<Box<dyn Any + Send>> {
    macro_rules! lock_command {
        ($request:ident, $response:ident, $method:ident) => {
            if let Some(request) = request.as_any().downcast_ref::<kvrpcpb::$request>() {
                let mut request = request.clone();
                let mut context = request.context.take().unwrap_or_default();
                context.cluster_id = cluster_id;
                context.request_origin = tidb_proto::KvrpcRequestOrigin::TiDb as i32;
                request.context = Some(context.clone());
                let response = client.$method(address, &request, &context, call).map_err(
                    |error| match error {
                        crate::DirectUnaryClientError::CallerCancelled => {
                            ResolverBridgeError::CallerCancelled.native()
                        }
                        error => Error::GrpcAPI(tonic::Status::unavailable(error.to_string())),
                    },
                )?;
                return Ok(Box::new(response));
            }
        };
    }
    lock_command!(
        CheckTxnStatusRequest,
        CheckTxnStatusResponse,
        check_txn_status_for_lock
    );
    lock_command!(
        CheckSecondaryLocksRequest,
        CheckSecondaryLocksResponse,
        check_secondary_locks_for_lock
    );
    lock_command!(
        PessimisticRollbackRequest,
        PessimisticRollbackResponse,
        pessimistic_rollback_for_lock
    );
    lock_command!(
        ResolveLockRequest,
        ResolveLockResponse,
        resolve_lock_for_read
    );
    Err(failure(format!(
        "unsupported transaction RPC {}",
        request.label()
    )))
}

/// One client engine's view of the existing store, with no second cache or transport.
#[derive(Clone)]
pub struct ClientPd {
    source_leader_read: bool,
    trace: Arc<Mutex<ClientTrace>>,
    backend: Arc<dyn Backend>,
    call: Arc<Mutex<Option<UnaryCallContext>>>,
}
impl ClientPd {
    /// Adapts process-owned transport, metadata and timestamp capabilities.
    pub fn new<C, L, T>(storage: SharedReadRuntime<C, L>, timestamps: T) -> Arc<Self>
    where
        C: TransactionCommandClient + LockRecoveryClient + Clone + Send + 'static,
        L: RegionRecoveryLoader + RegionQueryLoader + Send + 'static,
        T: TimestampSource + Send + 'static,
    {
        let trace = Arc::new(Mutex::new(ClientTrace::default()));
        Arc::new(Self {
            source_leader_read: true,
            backend: Arc::new(StorageBackend {
                storage,
                timestamps: Mutex::new(timestamps),
                trace: trace.clone(),
                dispatch: dispatch_transaction::<C, L>,
            }),
            trace,
            call: Arc::new(Mutex::new(None)),
        })
    }
    /// Adapts the existing read transport without requiring transaction commands.
    pub(crate) fn new_resolver<C, L, T>(
        storage: SharedReadRuntime<C, L>,
        timestamps: Arc<T>,
    ) -> Arc<Self>
    where
        C: LockRecoveryClient + Send + 'static,
        L: RegionRecoveryLoader + RegionQueryLoader + Send + 'static,
        T: TimestampSource + Send + Sync + 'static + ?Sized,
    {
        let trace = Arc::new(Mutex::new(ClientTrace::default()));
        Arc::new(Self {
            source_leader_read: true,
            backend: Arc::new(StorageBackend {
                storage,
                timestamps: Mutex::new(timestamps),
                trace: trace.clone(),
                dispatch: dispatch_resolver::<C, L>,
            }),
            trace,
            call: Arc::new(Mutex::new(None)),
        })
    }

    pub(crate) fn observe_detached_commits(
        &self,
    ) -> std::sync::mpsc::Receiver<crate::transaction::DetachedCommitCompletion> {
        let (sender, receiver) = std::sync::mpsc::channel();
        self.trace
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .secondary_observer = Some(sender);
        receiver
    }
    pub(crate) fn last_lock_primary(&self) -> Vec<u8> {
        self.trace
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .lock_primary
            .clone()
    }
    pub(crate) fn capture_scan_pages(&self, enabled: bool) -> Vec<ScanPage> {
        let mut trace = self.trace.lock().unwrap_or_else(|e| e.into_inner());
        let pages = trace.scan_pages.take().unwrap_or_default();
        if enabled {
            trace.scan_pages = Some(Vec::new());
        }
        pages
    }
    pub(crate) fn take_read_trace(&self) -> ReadTrace {
        std::mem::take(&mut self.trace.lock().unwrap_or_else(|e| e.into_inner()).reads)
    }
    pub(crate) fn capture_attempts_for_test(&self) {
        self.trace
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .capture_attempts = true;
    }
    pub(crate) fn fill_receipt(
        &self,
        receipt: &mut crate::transaction::OptimisticTransactionReceipt,
    ) {
        let trace = self.trace.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(write) = trace.writes.as_ref() {
            let authority_id = receipt.authority_id;
            let commit_ts = receipt.commit_ts;
            let mutation_count = receipt.mutation_count;
            *receipt = write.clone();
            receipt.authority_id = authority_id;
            receipt.commit_ts = commit_ts.max(receipt.commit_ts);
            receipt.mutation_count = mutation_count;
        }
    }
    /// Binds foreground RPCs to the current caller; cleanup owns its context.
    pub fn set_call(&self, call: &UnaryCallContext) {
        *self.call.lock().unwrap_or_else(|e| e.into_inner()) = Some(call.clone());
    }
}

/// A native KV endpoint backed by the existing TiDB transport.
pub struct ClientKv {
    route: Option<LeaderRequest>,
    backend: Arc<dyn Backend>,
    address: String,
    call: Arc<Mutex<Option<UnaryCallContext>>>,
}
struct CancelBackgroundCall(crate::rpc::UnaryCancellation);

impl Drop for CancelBackgroundCall {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[async_trait]
impl KvClient for ClientKv {
    async fn dispatch(&self, request: &dyn Request) -> Result<Box<dyn Any>> {
        self.dispatch_with_timeout(request, None).await
    }
    async fn dispatch_with_timeout(
        &self,
        request: &dyn Request,
        timeout: Option<Duration>,
    ) -> Result<Box<dyn Any>> {
        self.dispatch_with_timeout_and_forwarded_host(request, timeout, "")
            .await
    }
    async fn dispatch_with_forwarded_host(
        &self,
        request: &dyn Request,
        host: &str,
    ) -> Result<Box<dyn Any>> {
        self.dispatch_with_timeout_and_forwarded_host(request, None, host)
            .await
    }
    async fn dispatch_with_timeout_and_forwarded_host(
        &self,
        request: &dyn Request,
        timeout: Option<Duration>,
        host: &str,
    ) -> Result<Box<dyn Any>> {
        let timeout = timeout.unwrap_or(Duration::from_secs(30));
        let background = tikv_client::async_util::background_rpc_cancellation();
        if background
            .as_ref()
            .is_some_and(|scope| scope.is_cancelled())
        {
            return Err(Error::ContextCanceled);
        }
        let call = if background.is_some() {
            UnaryCallContext::with_timeout(timeout)
        } else {
            self.call
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .map_or_else(
                    || UnaryCallContext::with_timeout(timeout),
                    |parent| {
                        UnaryCallContext::new(
                            timeout.min(parent.timeout()),
                            parent.cancellation().clone(),
                        )
                    },
                )
        };
        let call = call.with_forwarded_host(host);
        if call.cancellation().is_cancelled() {
            return Err(Error::ContextCanceled);
        }
        if call.timeout().is_zero() {
            return Err(failure("context deadline exceeded"));
        }
        // A dropped resolver future must cancel its blocking transport call.
        // Foreground cancellation belongs to the statement and is never owned here.
        let _cancel_background = background
            .as_ref()
            .map(|_| CancelBackgroundCall(call.cancellation().clone()));
        let backend = self.backend.clone();
        let address = self.address.clone();
        let route = self.route.clone();
        macro_rules! send {
            ($(($request:ident, $response:ident)),+) => { $(
                if let Some(request) = request.as_any().downcast_ref::<kvrpcpb::$request>() {
                    let request = request.clone();
                    // Foreground transaction RPCs are already driven from the
                    // SQL thread's blocking client runtime. Calling the
                    // synchronous backend directly avoids an extra
                    // spawn_blocking hop for every Get/lock/commit. Resolver
                    // cleanup keeps the worker hop so cancellation can abort
                    // an in-flight request safely.
                    let dispatch = move || {
                        let response = backend.dispatch(&address, &request, &call);
                        if let Some(mut route) = route {
                            let context = request.context.as_ref();
                            route.replica_read = context.is_some_and(|c| c.replica_read);
                            route.stale_read = context.is_some_and(|c| c.stale_read);
                            if route.replica_read || route.stale_read {
                                route.read_mode = crate::region::ReplicaReadMode::Mixed;
                            }
                            match &response {
                                Ok(value) if value.downcast_ref::<kvrpcpb::$response>()
                                    .is_some_and(|r| r.region_error.is_none()) => backend.route_feedback(&route, true),
                                Err(error) if !call.cancellation().is_cancelled() && !call.timeout().is_zero()
                                    && !matches!(error, Error::ContextCanceled)
                                    && !matches!(error, Error::GrpcAPI(status) if status.code() == tonic::Code::Cancelled)
                                    && !matches!(error, Error::StringError(message) if message == "context canceled") => {
                                    backend.route_feedback(&route, false);
                                }
                                _ => {}
                            }
                        }
                        response
                    };
                    if background.is_none() {
                        return dispatch().map(|response| response as Box<dyn Any>);
                    }
                    let pending = tokio::task::spawn_blocking(dispatch);
                    let owner = background.as_ref().unwrap();
                    let response = tokio::select! {
                        result = pending => result.map_err(failure)??,
                        _ = owner.cancelled() => return Err(Error::ContextCanceled),
                    };
                    return Ok(response);
                }
            )+ }
        }
        send!(
            (GetRequest, GetResponse),
            (BatchGetRequest, BatchGetResponse),
            (ScanRequest, ScanResponse),
            (PrewriteRequest, PrewriteResponse),
            (CommitRequest, CommitResponse),
            (BatchRollbackRequest, BatchRollbackResponse),
            (PessimisticLockRequest, PessimisticLockResponse),
            (PessimisticRollbackRequest, PessimisticRollbackResponse),
            (TxnHeartBeatRequest, TxnHeartBeatResponse),
            (CheckTxnStatusRequest, CheckTxnStatusResponse),
            (CheckSecondaryLocksRequest, CheckSecondaryLocksResponse),
            (ResolveLockRequest, ResolveLockResponse)
        );
        Err(failure(format!(
            "unsupported transaction RPC {}",
            request.label()
        )))
    }
}

// This adapter supplies cache facts and transport handles. Replica choices,
// wire flags and request-scoped retry transitions belong to client-rust.
#[async_trait]
impl ReplicaRouting for ClientPd {
    fn forwarding_enabled(&self) -> bool {
        tidb_config::config_tree::config::get_global_config().enable_forwarding
    }
    async fn get_store_by_id(&self, id: u64) -> Result<()> {
        self.backend.store_state(id).map(|_| ())
    }
    fn store_liveness(&self, id: u64) -> Option<NativeLiveness> {
        self.backend
            .store_state(id)
            .ok()
            .map(|store| match store.liveness() {
                crate::region::StoreLiveness::Reachable => NativeLiveness::Reachable,
                crate::region::StoreLiveness::Unreachable => NativeLiveness::Unreachable,
                crate::region::StoreLiveness::Unknown => NativeLiveness::Unknown,
            })
    }
    async fn store_epoch_is_stale(&self, region: &RegionVerId, id: u64) -> bool {
        let Ok(location) = self.backend.locate_id(region.id) else {
            return true;
        };
        let Ok(store) = self.backend.store_state(id) else {
            return true;
        };
        location.region != region_id(region.clone())
            || !location
                .peers
                .iter()
                .any(|peer| peer.store_id == id && peer.store_epoch == store.epoch())
    }
    fn estimated_store_wait(&self, id: u64) -> Option<Duration> {
        self.backend.store_state(id).ok().map(|store| {
            store
                .routing_health()
                .load
                .estimated_wait(std::time::Instant::now())
        })
    }
    async fn select_mixed_replica(
        &self,
        region: &RegionWithLeader,
        labels: &[metapb::StoreLabel],
        stores: &[u64],
        state: &ReplicaSelectorState,
        selection: MixedReplicaSelection,
    ) -> Result<Option<metapb::Peer>> {
        let candidates = self
            .backend
            .candidates(region.ver_id(), labels, stores, state)?;
        Ok(selection
            .choose(&candidates)
            .and_then(|candidate| {
                region
                    .region
                    .peers
                    .iter()
                    .find(|peer| peer.id == candidate.peer_id)
            })
            .cloned())
    }
    async fn proxy_for_unavailable_leader(
        &self,
        region: &RegionWithLeader,
        state: &ReplicaSelectorState,
    ) -> Result<Option<metapb::Peer>> {
        let Some(leader) = &region.leader else {
            return Ok(None);
        };
        let proxy = self.backend.proxy(region.ver_id(), leader.id, state)?;
        Ok(region
            .region
            .peers
            .iter()
            .find(|p| Some(p.id) == proxy)
            .cloned())
    }
    async fn map_region_to_route(
        self: Arc<Self>,
        region: RegionWithLeader,
        target: metapb::Peer,
        proxy: Option<metapb::Peer>,
    ) -> Result<RegionStore> {
        let mut route = self.backend.route(
            region.ver_id(),
            target.id,
            proxy.as_ref().map(|p| p.id),
            self.forwarding_enabled(),
        )?;
        if !self.source_leader_read {
            route.read_mode = crate::region::ReplicaReadMode::Mixed;
        }
        let target_store = self.backend.store_state(target.store_id)?;
        let physical = route.dispatch_attempt();
        if physical.address.is_empty() || route.attempt.address.is_empty() {
            return Err(failure("region store address is empty"));
        }
        let client = ClientKv {
            backend: self.backend.clone(),
            address: physical.address.clone(),
            call: self.call.clone(),
            route: Some(route.clone()),
        };
        let mut native = RegionStore::new(region, Arc::new(client))
            .with_target(physical.address.clone())
            .with_target_peer(target)
            .with_physical_store(physical.store_id, tikv_client::tikv::EndpointType::TiKv)
            .with_health_status(target_store.routing_health().health.clone());
        if let Some(host) = route.forwarded_host() {
            native = native.with_forwarded_host(host);
        }
        Ok(native)
    }

    fn record_store_replica_flow(&self, _id: u64, _destination: ReplicaFlowsType) {
        // Periodic replica-flow metrics are not composed by this adapter yet.
    }
}

#[async_trait]
impl PdClient for ClientPd {
    type KvClient = ClientKv;
    async fn on_send_failure(self: Arc<Self>, _route: Option<&RegionStore>) -> bool {
        // ClientKv applied exact-generation feedback before returning the error.
        // Preserve native request attempts and the shared region snapshot.
        false
    }
    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        self.route_leader(region, &ReplicaSelectorState::default())
            .await
    }
    async fn map_region_to_store_with_replica(
        self: Arc<Self>,
        region: RegionWithLeader,
        config: ReplicaReadConfig,
        state: ReplicaSelectorState,
        is_read: bool,
    ) -> Result<RegionStore> {
        // Source mode belongs to this selection, including the ordinary-wire
        // leader probe in a stale read. Wire flags alone lose that distinction.
        let source_leader_read =
            config.read_type == tikv_client::ReplicaReadType::Leader && !config.stale_read;
        let routing = if self.source_leader_read == source_leader_read {
            self
        } else {
            Arc::new(Self {
                source_leader_read,
                ..(*self).clone()
            })
        };
        routing.route_replica(region, config, state, is_read).await
    }
    fn record_server_load(&self, id: u64, estimated_wait_ms: u32) {
        self.backend.record_server_load(id, estimated_wait_ms);
    }
    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        // Region-cache lookup is synchronous and normally a read-only cache hit.
        let key: Vec<u8> = key.clone().into();
        self.backend.locate_key(&key, false)
    }
    async fn region_for_end_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let key: Vec<u8> = key.clone().into();
        self.backend.locate_key(&key, true)
    }
    async fn region_for_id(&self, id: u64) -> Result<RegionWithLeader> {
        self.backend.locate_id(id).map(client_region)
    }
    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        let background = tikv_client::async_util::background_rpc_cancellation();
        let is_cancelled = || {
            background.as_ref().map_or_else(
                || {
                    self.call
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .as_ref()
                        .is_some_and(|call| call.cancellation().is_cancelled())
                },
                |owner| owner.is_cancelled(),
            )
        };
        if is_cancelled() {
            return Err(ResolverBridgeError::CallerCancelled.native());
        }
        let result = self.backend.timestamp();
        if is_cancelled() {
            return Err(ResolverBridgeError::CallerCancelled.native());
        }
        result
    }
    async fn cluster_id(&self) -> u64 {
        self.backend.cluster_id()
    }
    async fn update_leader(&self, id: RegionVerId, peer: metapb::Peer) -> Result<()> {
        self.backend.update_leader(id, peer)
    }
    async fn update_region_cache(&self, regions: Vec<RegionWithLeader>) -> Result<()> {
        let backend = self.backend.clone();
        tokio::task::spawn_blocking(move || backend.update_regions(regions))
            .await
            .map_err(failure)?
    }
    async fn invalidate_region_cache(&self, id: RegionVerId) {
        self.backend.invalidate_region(id);
    }
    async fn invalidate_store_cache(&self, id: u64) {
        self.backend.invalidate_store(id);
    }
    async fn all_stores(&self) -> Result<Vec<Store>> {
        Err(Error::Unimplemented)
    }
    async fn update_safepoint(self: Arc<Self>, _timestamp: u64) -> Result<bool> {
        Err(Error::Unimplemented)
    }
    async fn load_keyspace(&self, _name: &str) -> Result<keyspacepb::KeyspaceMeta> {
        Err(Error::Unimplemented)
    }
}

// This is transport evidence only. No transaction decisions, retry budget or
// cleanup state is owned here; client-rust consumes every response unchanged.
#[derive(Default)]
pub(crate) struct ReadTrace {
    pub rpc_count: u64,
    pub last_get: Option<(
        crate::region::RegionVerId,
        crate::rpc::TransactionBatchPublication,
    )>,
}
pub(crate) struct ScanPage {
    pub region: crate::region::RegionVerId,
    pub end_key: Vec<u8>,
    pub pairs: Vec<(Vec<u8>, Vec<u8>)>,
}
#[derive(Default)]
struct ClientTrace {
    // Explicit test observation; ordinary transactions retain no key history.
    capture_attempts: bool,
    secondary_observer:
        Option<std::sync::mpsc::Sender<crate::transaction::DetachedCommitCompletion>>,
    lock_primary: Vec<u8>,
    scan_pages: Option<Vec<ScanPage>>,
    reads: ReadTrace,
    writes: Option<crate::transaction::OptimisticTransactionReceipt>,
}
impl ClientTrace {
    fn observe_native_get(
        &mut self,
        request: &kvrpcpb::GetRequest,
        context: &tidb_proto::KvrpcContext,
        published: &PublishedCommand<kvrpcpb::GetResponse>,
    ) {
        self.reads.rpc_count += 1;
        if let PublishedCommand::Response(response) = published {
            if response.response.region_error.is_none() && response.response.error.is_none() {
                let epoch = context.region_epoch.as_ref().cloned().unwrap_or_default();
                let region = crate::region::RegionVerId::new(
                    context.region_id,
                    epoch.conf_ver,
                    epoch.version,
                );
                if !response.response.not_found {
                    if let Some(pages) = self.scan_pages.as_mut() {
                        pages.push(ScanPage {
                            region,
                            end_key: Vec::new(),
                            pairs: vec![(request.key.clone(), response.response.value.clone())],
                        });
                    }
                }
                self.reads.last_get = Some((region, response.publication.clone()));
            }
        }
    }

    fn observe<R: Any>(
        &mut self,
        request: &dyn Any,
        context: &tidb_proto::KvrpcContext,
        address: &str,
        published: &PublishedCommand<R>,
    ) {
        use crate::transaction::{
            CommittedProtocol, OptimisticTransactionReceipt, TransactionAttemptPhase as Phase,
            TransactionAttemptReceipt, TransactionAttemptResult as Attempt,
            TransactionCause as Cause,
        };
        use tidb_proto::kvrpcpb as rpc;
        let epoch = context.region_epoch.as_ref().cloned().unwrap_or_default();
        let region =
            crate::region::RegionVerId::new(context.region_id, epoch.conf_ver, epoch.version);
        let (publication, response, failure) = match published {
            PublishedCommand::BeforePublication(error) => (
                None,
                None,
                Some(Attempt::DefinitiveFailure(Cause::Transport {
                    detail: error.clone(),
                })),
            ),
            PublishedCommand::AfterPublication { publication, error } => (
                Some(publication.clone()),
                None,
                Some(Attempt::Ambiguous(Cause::Transport {
                    detail: error.clone(),
                })),
            ),
            PublishedCommand::Response(response) => (
                Some(response.publication.clone()),
                Some(&response.response as &dyn Any),
                None,
            ),
        };
        if request.is::<rpc::GetRequest>()
            || request.is::<rpc::BatchGetRequest>()
            || request.is::<rpc::ScanRequest>()
        {
            self.reads.rpc_count += 1;
            if let (Some(pages), Some(request), Some(response)) = (
                self.scan_pages.as_mut(),
                request.downcast_ref::<rpc::ScanRequest>(),
                response.and_then(|r| r.downcast_ref::<rpc::ScanResponse>()),
            ) {
                if response.region_error.is_none() && response.error.is_none() {
                    pages.push(ScanPage {
                        region,
                        end_key: request.end_key.clone(),
                        pairs: response
                            .pairs
                            .iter()
                            .filter(|p| p.error.is_none())
                            .map(|p| (p.key.clone(), p.value.clone()))
                            .collect(),
                    });
                }
            }
            // A scanner can resolve an individual locked pair with a point
            // Get; those values have a serving region too.
            if let (Some(pages), Some(request), Some(response)) = (
                self.scan_pages.as_mut(),
                request.downcast_ref::<rpc::GetRequest>(),
                response.and_then(|r| r.downcast_ref::<rpc::GetResponse>()),
            ) {
                if response.region_error.is_none()
                    && response.error.is_none()
                    && !response.not_found
                {
                    pages.push(ScanPage {
                        region,
                        end_key: Vec::new(),
                        pairs: vec![(request.key.clone(), response.value.clone())],
                    });
                }
            }
            if request.is::<rpc::GetRequest>() {
                if let Some(publication) = publication {
                    self.reads.last_get = Some((region, publication));
                }
            }
            return;
        }
        if let (Some(observer), Some(request), Some(response), Some(publication)) = (
            &self.secondary_observer,
            request.downcast_ref::<rpc::CommitRequest>(),
            response.and_then(|r| r.downcast_ref::<rpc::CommitResponse>()),
            publication.as_ref(),
        ) {
            if (request.commit_role == rpc::CommitRole::Secondary as i32
                || request.use_async_commit)
                && response.region_error.is_none()
                && response.error.is_none()
            {
                let _ = observer.send(Ok(crate::rpc::TransactionBatchResponse {
                    response: response.clone(),
                    publication: publication.clone(),
                }));
            }
        }
        if let (Some(request), Some(response)) = (
            request.downcast_ref::<rpc::PessimisticLockRequest>(),
            response.and_then(|r| r.downcast_ref::<rpc::PessimisticLockResponse>()),
        ) {
            if response.region_error.is_none() && response.errors.is_empty() {
                self.lock_primary = request.primary_lock.clone();
            }
        }
        if !self.capture_attempts {
            return;
        }
        let (phase, keys, start_ts, primary) =
            if let Some(request) = request.downcast_ref::<rpc::PrewriteRequest>() {
                (
                    Phase::Prewrite,
                    request.mutations.iter().map(|m| m.key.clone()).collect(),
                    request.start_version,
                    request.primary_lock.clone(),
                )
            } else if let Some(request) = request.downcast_ref::<rpc::CommitRequest>() {
                let primary = self
                    .writes
                    .as_ref()
                    .map_or_else(Vec::new, |r| r.primary_key.clone());
                let phase = if request.keys.contains(&primary) {
                    Phase::PrimaryCommit
                } else {
                    Phase::SecondaryCommit
                };
                (phase, request.keys.clone(), request.start_version, primary)
            } else if let Some(request) = request.downcast_ref::<rpc::BatchRollbackRequest>() {
                (
                    Phase::BatchRollback,
                    request.keys.clone(),
                    request.start_version,
                    Vec::new(),
                )
            } else {
                return;
            };
        let receipt = self.writes.get_or_insert_with(|| {
            OptimisticTransactionReceipt::new(0, start_ts, primary.clone(), 0)
        });
        if !primary.is_empty() {
            receipt.primary_key = primary;
        }
        if let Some(request) = request.downcast_ref::<rpc::PrewriteRequest>() {
            receipt.lock_ttl_ms = request.lock_ttl;
        }
        if let Some(request) = request.downcast_ref::<rpc::CommitRequest>() {
            receipt.commit_ts = request.commit_version;
        }
        let mut result = failure.unwrap_or(Attempt::Confirmed);
        if let Some(response) = response {
            macro_rules! errors {
                ($r:expr, $errors:expr) => {
                    if let Some(error) = &$r.region_error {
                        result = Attempt::Retry(Cause::Region {
                            detail: format!("{error:?}"),
                        });
                    } else if let Some(error) = $errors {
                        result = Attempt::DefinitiveFailure(Cause::InvalidResponse {
                            detail: format!("{error:?}"),
                        });
                    }
                };
            }
            if let Some(r) = response.downcast_ref::<rpc::PrewriteResponse>() {
                errors!(r, r.errors.first());
                if r.one_pc_commit_ts > 0 {
                    receipt.commit_ts = r.one_pc_commit_ts;
                    receipt.commit_protocol = CommittedProtocol::OnePc;
                }
            } else if let Some(r) = response.downcast_ref::<rpc::CommitResponse>() {
                errors!(r, r.error.as_ref());
            } else if let Some(r) = response.downcast_ref::<rpc::BatchRollbackResponse>() {
                errors!(r, r.error.as_ref());
            }
        }
        receipt.region_attempts.push(region);
        if let Some(publication) = &publication {
            let confirmed = result == Attempt::Confirmed;
            match phase {
                Phase::Prewrite => {
                    receipt
                        .prewrite_attempt_publications
                        .push(publication.clone());
                    if confirmed {
                        receipt.prewrite_publications.push(publication.clone());
                    }
                }
                Phase::PrimaryCommit => receipt.primary_publications.push(publication.clone()),
                Phase::SecondaryCommit => {
                    receipt
                        .secondary_attempt_publications
                        .push(publication.clone());
                    if confirmed {
                        receipt.secondary_publications.push(publication.clone());
                    }
                }
                Phase::BatchRollback => {
                    receipt
                        .rollback_attempt_publications
                        .push(publication.clone());
                    if confirmed {
                        receipt.rollback_publications.push(publication.clone());
                    }
                }
            }
        }
        receipt.attempt_history.push(TransactionAttemptReceipt {
            phase,
            keys,
            region,
            address: address.to_owned(),
            publication,
            result,
        });
    }
}

#[cfg(test)]
mod ownership_regressions {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn native_get_bytes_keep_routed_context_and_lock_hints() {
        let request = kvrpcpb::GetRequest {
            context: Some(kvrpcpb::Context {
                region_id: 42,
                resolved_locks: vec![7],
                committed_locks: vec![8],
                request_source: "external_test".into(),
                ..Default::default()
            }),
            key: b"key".to_vec(),
            version: 99,
            ..Default::default()
        };
        let mut context: tidb_proto::KvrpcContext = request.context.clone().unwrap();
        context.cluster_id = 77;
        context.request_origin = tidb_proto::KvrpcRequestOrigin::TiDb as i32;
        let bytes = encode_native_get(&request, &context);
        let decoded = tidb_proto::KvrpcGetRequest::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.context, Some(context));
        assert_eq!(decoded.key, b"key");
        assert_eq!(decoded.version, 99);
    }

    #[test]
    fn native_get_preserves_scan_recovery_observations() {
        let mut trace = ClientTrace {
            scan_pages: Some(Vec::new()),
            ..Default::default()
        };
        let context = tidb_proto::KvrpcContext {
            region_id: 42,
            ..Default::default()
        };
        let response = PublishedCommand::Response(crate::rpc::TransactionBatchResponse {
            response: kvrpcpb::GetResponse {
                value: b"value".to_vec(),
                ..Default::default()
            },
            publication: crate::rpc::TransactionBatchPublication::in_process(
                crate::rpc::BatchCommandTag::Get,
                "local",
                7,
            ),
        });
        trace.observe_native_get(
            &kvrpcpb::GetRequest {
                key: b"key".to_vec(),
                ..Default::default()
            },
            &context,
            &response,
        );
        let pages = trace.scan_pages.unwrap();
        assert_eq!(
            pages.len(),
            1,
            "a point Get resolving a scan lock retains its serving region"
        );
        assert_eq!(pages[0].region.id, 42);
        assert_eq!(pages[0].pairs, vec![(b"key".to_vec(), b"value".to_vec())]);
        assert_eq!(trace.reads.rpc_count, 1);
    }

    #[derive(Default)]
    struct CleanupBackend {
        calls: AtomicUsize,
        error: Mutex<Option<Error>>,
        forwarded: Mutex<Vec<Option<String>>>,
        blocking: bool,
        started: tokio::sync::Notify,
        finished: tokio::sync::Notify,
    }
    impl Backend for CleanupBackend {
        fn timestamp(&self) -> Result<Timestamp> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Timestamp::from_version(2))
        }
        fn cluster_id(&self) -> u64 {
            1
        }
        fn locate_key(&self, _: &[u8], _: bool) -> Result<RegionWithLeader> {
            unreachable!()
        }
        fn locate_id(&self, _: u64) -> Result<RegionLocation> {
            unreachable!()
        }
        fn store_state(&self, _: u64) -> Result<crate::region::StoreState> {
            unreachable!()
        }
        fn candidates(
            &self,
            _: RegionVerId,
            _: &[metapb::StoreLabel],
            _: &[u64],
            _: &ReplicaSelectorState,
        ) -> Result<Vec<ReplicaCandidate>> {
            unreachable!()
        }
        fn route(&self, _: RegionVerId, _: u64, _: Option<u64>, _: bool) -> Result<LeaderRequest> {
            unreachable!()
        }
        fn proxy(&self, _: RegionVerId, _: u64, _: &ReplicaSelectorState) -> Result<Option<u64>> {
            unreachable!()
        }
        fn route_feedback(&self, _: &LeaderRequest, _: bool) {
            unreachable!()
        }
        fn record_server_load(&self, _: u64, _: u32) {
            unreachable!()
        }
        fn update_leader(&self, _: RegionVerId, _: metapb::Peer) -> Result<()> {
            unreachable!()
        }
        fn update_regions(&self, _: Vec<RegionWithLeader>) -> Result<()> {
            unreachable!()
        }
        fn invalidate_region(&self, _: RegionVerId) {
            unreachable!()
        }
        fn invalidate_store(&self, _: u64) {
            unreachable!()
        }
        fn dispatch(
            &self,
            _: &str,
            request: &dyn Request,
            call: &UnaryCallContext,
        ) -> Result<Box<dyn Any + Send>> {
            assert!(!call.cancellation().is_cancelled());
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.forwarded
                .lock()
                .unwrap()
                .push(call.forwarded_host().map(str::to_owned));
            if let Some(error) = self.error.lock().unwrap().take() {
                return Err(error);
            }
            assert!(
                request.as_any().is::<kvrpcpb::ResolveLockRequest>()
                    || request.as_any().is::<kvrpcpb::TxnHeartBeatRequest>()
            );
            if self.blocking {
                self.started.notify_one();
                assert!(
                    call.cancellation().wait_timeout(Duration::from_secs(5)),
                    "dropped background call left the transport running"
                );
                self.finished.notify_one();
                return Err(Error::ContextCanceled);
            }
            if request.as_any().is::<kvrpcpb::TxnHeartBeatRequest>() {
                Ok(Box::<kvrpcpb::TxnHeartBeatResponse>::default())
            } else {
                Ok(Box::<kvrpcpb::ResolveLockResponse>::default())
            }
        }
    }

    #[test]
    fn ordinary_forwarding_preserves_metadata_with_timeout_and_background_scope() {
        let backend = Arc::new(CleanupBackend::default());
        let client = ClientKv {
            route: None,
            backend: backend.clone(),
            address: "proxy".into(),
            call: Arc::new(Mutex::new(None)),
        };
        let requests: Vec<Box<dyn Request>> = vec![
            Box::new(kvrpcpb::GetRequest::default()),
            Box::new(kvrpcpb::BatchGetRequest::default()),
            Box::new(kvrpcpb::ScanRequest::default()),
            Box::new(kvrpcpb::PrewriteRequest::default()),
            Box::new(kvrpcpb::CommitRequest::default()),
            Box::new(kvrpcpb::BatchRollbackRequest::default()),
            Box::new(kvrpcpb::PessimisticLockRequest::default()),
            Box::new(kvrpcpb::PessimisticRollbackRequest::default()),
            Box::new(kvrpcpb::TxnHeartBeatRequest::default()),
            Box::new(kvrpcpb::CheckTxnStatusRequest::default()),
            Box::new(kvrpcpb::CheckSecondaryLocksRequest::default()),
            Box::new(kvrpcpb::ResolveLockRequest::default()),
        ];
        for background in [false, true] {
            for request in &requests {
                for host in ["leader:20160", ""] {
                    *backend.error.lock().unwrap() = Some(Error::StringError("fixture".into()));
                    let send = client.dispatch_with_timeout_and_forwarded_host(
                        request.as_ref(),
                        Some(Duration::from_secs(2)),
                        host,
                    );
                    let result = if background {
                        runtime().block_on(tikv_client::async_util::with_background_rpc_context(
                            tikv_client::async_util::Cancellation::default(),
                            send,
                        ))
                    } else {
                        runtime().block_on(send)
                    };
                    assert!(result.is_err());
                    assert_eq!(
                        backend.forwarded.lock().unwrap().last().unwrap().as_deref(),
                        (!host.is_empty()).then_some(host)
                    );
                }
            }
        }
    }

    #[test]
    fn every_rpc_preserves_native_error_identity_in_both_operation_scopes() {
        let requests: Vec<Box<dyn Request>> = vec![
            Box::new(kvrpcpb::GetRequest::default()),
            Box::new(kvrpcpb::BatchGetRequest::default()),
            Box::new(kvrpcpb::ScanRequest::default()),
            Box::new(kvrpcpb::PrewriteRequest::default()),
            Box::new(kvrpcpb::CommitRequest::default()),
            Box::new(kvrpcpb::BatchRollbackRequest::default()),
            Box::new(kvrpcpb::PessimisticLockRequest::default()),
            Box::new(kvrpcpb::PessimisticRollbackRequest::default()),
            Box::new(kvrpcpb::TxnHeartBeatRequest::default()),
            Box::new(kvrpcpb::CheckTxnStatusRequest::default()),
            Box::new(kvrpcpb::CheckSecondaryLocksRequest::default()),
            Box::new(kvrpcpb::ResolveLockRequest::default()),
        ];
        for background in [false, true] {
            let backend = Arc::new(CleanupBackend::default());
            let client = ClientKv {
                route: None,
                backend: backend.clone(),
                address: "store1".to_owned(),
                call: Arc::new(Mutex::new(None)),
            };
            for request in &requests {
                for error in [
                    Error::ContextCanceled,
                    Error::StringError("context canceled".to_owned()),
                    Error::GrpcAPI(tonic::Status::cancelled("remote cancellation")),
                ] {
                    let expected = std::mem::discriminant(&error);
                    *backend.error.lock().unwrap() = Some(error);
                    let dispatch = client.dispatch(request.as_ref());
                    let result = if background {
                        runtime().block_on(tikv_client::async_util::with_background_rpc_context(
                            tikv_client::async_util::Cancellation::default(),
                            dispatch,
                        ))
                    } else {
                        runtime().block_on(dispatch)
                    };
                    let error = result.unwrap_err();
                    assert_eq!(
                        std::mem::discriminant(&error),
                        expected,
                        "{}: {error:?}",
                        request.label()
                    );
                    if let Error::GrpcAPI(status) = error {
                        assert_eq!(status.code(), tonic::Code::Cancelled);
                        assert_eq!(status.message(), "remote cancellation");
                    }
                }
            }
            assert_eq!(backend.calls.load(Ordering::SeqCst), requests.len() * 3);
        }
    }

    #[test]
    fn background_resolve_lock_can_dispatch_after_statement_cancellation() {
        let parent = UnaryCallContext::with_timeout(Duration::from_secs(30));
        parent.cancellation().cancel();
        let backend = Arc::new(CleanupBackend::default());
        let client = ClientKv {
            route: None,
            backend: backend.clone(),
            address: "store1".to_owned(),
            call: Arc::new(Mutex::new(Some(parent))),
        };
        // Native schedule_read_lock_cleanup sends this through the same client
        // after returning the lock classification to the foreground reader.
        let result = runtime().block_on(tikv_client::async_util::with_background_rpc_context(
            tikv_client::async_util::Cancellation::default(),
            client.dispatch(&kvrpcpb::ResolveLockRequest {
                start_version: 10,
                commit_version: 20,
                keys: vec![b"key".to_vec()],
                ..Default::default()
            }),
        ));
        assert!(
            result.is_ok(),
            "background resolver inherited the statement context: {:?}",
            result.err()
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn foreground_resolution_retains_statement_cancellation() {
        let parent = UnaryCallContext::with_timeout(Duration::from_secs(30));
        parent.cancellation().cancel();
        let backend = Arc::new(CleanupBackend::default());
        let client = ClientKv {
            route: None,
            backend: backend.clone(),
            address: "store1".to_owned(),
            call: Arc::new(Mutex::new(Some(parent))),
        };
        let result = runtime().block_on(client.dispatch(&kvrpcpb::ResolveLockRequest::default()));
        assert!(result.is_err());
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn foreground_transaction_requests_retain_statement_cancellation() {
        let parent = UnaryCallContext::with_timeout(Duration::from_secs(30));
        parent.cancellation().cancel();
        let backend = Arc::new(CleanupBackend::default());
        let client = ClientKv {
            route: None,
            backend: backend.clone(),
            address: "store1".to_owned(),
            call: Arc::new(Mutex::new(Some(parent))),
        };
        let commit = kvrpcpb::CommitRequest::default();
        let rollback = kvrpcpb::PessimisticRollbackRequest::default();
        let heartbeat = kvrpcpb::TxnHeartBeatRequest::default();
        let requests: [&dyn Request; 3] = [&commit, &rollback, &heartbeat];
        for request in requests {
            let result = runtime().block_on(client.dispatch(request));
            assert!(matches!(result, Err(Error::ContextCanceled)));
        }
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn background_heartbeat_can_dispatch_after_statement_cancellation() {
        let parent = UnaryCallContext::with_timeout(Duration::from_secs(30));
        parent.cancellation().cancel();
        let backend = Arc::new(CleanupBackend::default());
        let client = ClientKv {
            route: None,
            backend: backend.clone(),
            address: "store1".to_owned(),
            call: Arc::new(Mutex::new(Some(parent))),
        };
        let result = runtime().block_on(tikv_client::async_util::with_background_rpc_context(
            tikv_client::async_util::Cancellation::default(),
            client.dispatch(&kvrpcpb::TxnHeartBeatRequest::default()),
        ));
        assert!(result.is_ok());
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn timestamp_cancellation_follows_the_operation_scope() {
        for background in [false, true] {
            let parent = UnaryCallContext::with_timeout(Duration::from_secs(30));
            parent.cancellation().cancel();
            let backend = Arc::new(CleanupBackend::default());
            let client = Arc::new(ClientPd {
                source_leader_read: true,
                trace: Arc::new(Mutex::new(ClientTrace::default())),
                backend: backend.clone(),
                call: Arc::new(Mutex::new(Some(parent))),
            });
            if background {
                let owner = tikv_client::async_util::Cancellation::default();
                let result =
                    runtime().block_on(tikv_client::async_util::with_background_rpc_context(
                        owner.clone(),
                        client.clone().get_timestamp(),
                    ));
                assert_eq!(result.unwrap().version(), 2);
                owner.cancel();
                let result =
                    runtime().block_on(tikv_client::async_util::with_background_rpc_context(
                        owner,
                        client.get_timestamp(),
                    ));
                assert!(matches!(result, Err(Error::ContextCanceled)));
                assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
            } else {
                let result = runtime().block_on(client.get_timestamp());
                assert!(matches!(result, Err(Error::ContextCanceled)));
                assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
            }
        }
    }

    #[test]
    fn background_resolution_obeys_its_owner_cancellation() {
        let backend = Arc::new(CleanupBackend::default());
        let client = ClientKv {
            route: None,
            backend: backend.clone(),
            address: "store1".to_owned(),
            call: Arc::new(Mutex::new(None)),
        };
        let owner = tikv_client::async_util::Cancellation::default();
        owner.cancel();
        let result = runtime().block_on(tikv_client::async_util::with_background_rpc_context(
            owner,
            client.dispatch(&kvrpcpb::ResolveLockRequest::default()),
        ));
        assert!(result.is_err());
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn inflight_background_cleanup_cancels_its_blocking_transport() {
        for abort_task in [false, true] {
            runtime().block_on(async {
                let backend = Arc::new(CleanupBackend {
                    blocking: true,
                    ..Default::default()
                });
                let client = ClientKv {
                    route: None,
                    backend: backend.clone(),
                    address: "store1".to_owned(),
                    call: Arc::new(Mutex::new(None)),
                };
                let owner = tikv_client::async_util::Cancellation::default();
                let operation_owner = owner.clone();
                let task = tokio::spawn(async move {
                    tikv_client::async_util::with_background_rpc_context(operation_owner, async {
                        client
                            .dispatch(&kvrpcpb::ResolveLockRequest::default())
                            .await
                            .map(|_| ())
                    })
                    .await
                });
                tokio::time::timeout(Duration::from_secs(2), backend.started.notified())
                    .await
                    .unwrap();
                if abort_task {
                    task.abort();
                    assert!(task.await.unwrap_err().is_cancelled());
                } else {
                    owner.cancel();
                    assert!(tokio::time::timeout(Duration::from_secs(2), task)
                        .await
                        .unwrap()
                        .unwrap()
                        .is_err());
                }
                tokio::time::timeout(Duration::from_secs(2), backend.finished.notified())
                    .await
                    .unwrap();
            });
        }
    }
}
