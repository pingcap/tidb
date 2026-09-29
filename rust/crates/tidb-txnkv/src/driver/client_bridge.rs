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
use crate::region::{RegionLocation, RegionQueryLoader, RegionRecoveryLoader};
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
use tikv_client::PdClient;
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
        Error::Io(std::io::Error::other(self))
    }
}

fn failure(error: impl std::fmt::Display) -> Error {
    Error::StringError(error.to_string())
}
fn transcode<A: Message, B: Message + Default>(value: &A) -> Result<B> {
    B::decode(value.encode_to_vec().as_slice()).map_err(failure)
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
    C: Send + 'static,
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
                let mut proto = transcode::<_, tidb_proto::metapb::Region>(&region.region)?;
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
        let mut context: tidb_proto::KvrpcContext = request
            .context
            .as_ref()
            .map(transcode)
            .transpose()?
            .unwrap_or_default();
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
                let mut request: tidb_proto::kvrpcpb::$request = transcode(request)?;
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
                return Ok(Box::new(transcode::<_, kvrpcpb::$response>(&response)?));
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
                let mut request: tidb_proto::kvrpcpb::$request = transcode(request)?;
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
                return Ok(Box::new(transcode::<_, kvrpcpb::$response>(&response)?));
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
pub struct ClientPd {
    trace: Arc<Mutex<ClientTrace>>,
    backend: Arc<dyn Backend>,
    call: Arc<Mutex<Option<UnaryCallContext>>>,
    transaction_tasks: bool,
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
            backend: Arc::new(StorageBackend {
                storage,
                timestamps: Mutex::new(timestamps),
                trace: trace.clone(),
                dispatch: dispatch_transaction::<C, L>,
            }),
            trace,
            call: Arc::new(Mutex::new(None)),
            transaction_tasks: true,
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
            backend: Arc::new(StorageBackend {
                storage,
                timestamps: Mutex::new(timestamps),
                trace: trace.clone(),
                dispatch: dispatch_resolver::<C, L>,
            }),
            trace,
            call: Arc::new(Mutex::new(None)),
            transaction_tasks: false,
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
    backend: Arc<dyn Backend>,
    address: String,
    call: Arc<Mutex<Option<UnaryCallContext>>>,
    transaction_tasks: bool,
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
        let timeout = timeout.unwrap_or(Duration::from_secs(30));
        let background = tikv_client::async_util::background_rpc_cancellation();
        if background
            .as_ref()
            .is_some_and(|scope| scope.is_cancelled())
        {
            return Err(failure("context canceled"));
        }
        let detached = background.is_some()
            || (self.transaction_tasks
                && (request.as_any().is::<kvrpcpb::BatchRollbackRequest>()
                    || request.as_any().is::<kvrpcpb::PessimisticRollbackRequest>()
                    || request.as_any().is::<kvrpcpb::TxnHeartBeatRequest>()
                    || request
                        .as_any()
                        .downcast_ref::<kvrpcpb::CommitRequest>()
                        .is_some_and(|request| {
                            request.commit_role == kvrpcpb::CommitRole::Secondary as i32
                                || request.use_async_commit
                        })));
        let call = if detached {
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
        if call.cancellation().is_cancelled() {
            return Err(failure("context canceled"));
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
        macro_rules! send {
            ($($request:ident),+) => { $(
                if let Some(request) = request.as_any().downcast_ref::<kvrpcpb::$request>() {
                    let request = request.clone();
                    let pending = tokio::task::spawn_blocking(move || backend.dispatch(&address, &request, &call));
                    let response = if let Some(owner) = &background {
                        tokio::select! {
                            result = pending => result.map_err(failure)??,
                            _ = owner.cancelled() => return Err(failure("context canceled")),
                        }
                    } else {
                        pending.await.map_err(failure)??
                    };
                    return Ok(response);
                }
            )+ }
        }
        send!(
            GetRequest,
            BatchGetRequest,
            ScanRequest,
            PrewriteRequest,
            CommitRequest,
            BatchRollbackRequest,
            PessimisticLockRequest,
            PessimisticRollbackRequest,
            TxnHeartBeatRequest,
            CheckTxnStatusRequest,
            CheckSecondaryLocksRequest,
            ResolveLockRequest
        );
        Err(failure(format!(
            "unsupported transaction RPC {}",
            request.label()
        )))
    }
}

#[async_trait]
impl PdClient for ClientPd {
    type KvClient = ClientKv;
    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        let backend = self.backend.clone();
        let id = region.region.id;
        let location = tokio::task::spawn_blocking(move || backend.locate_id(id))
            .await
            .map_err(failure)??;
        let peer = region
            .leader
            .as_ref()
            .ok_or_else(|| failure("region has no leader"))?;
        let address = location
            .stores
            .iter()
            .find(|store| store.id == peer.store_id)
            .ok_or_else(|| failure("leader store is missing"))?
            .address
            .clone();
        let client = ClientKv {
            backend: self.backend.clone(),
            address: address.clone(),
            call: self.call.clone(),
            transaction_tasks: self.transaction_tasks,
        };
        Ok(RegionStore::new(region, Arc::new(client)).with_target(address))
    }
    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let backend = self.backend.clone();
        let key: Vec<u8> = key.clone().into();
        tokio::task::spawn_blocking(move || backend.locate_key(&key, false))
            .await
            .map_err(failure)?
    }
    async fn region_for_end_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let backend = self.backend.clone();
        let key: Vec<u8> = key.clone().into();
        tokio::task::spawn_blocking(move || backend.locate_key(&key, true))
            .await
            .map_err(failure)?
    }
    async fn region_for_id(&self, id: u64) -> Result<RegionWithLeader> {
        let backend = self.backend.clone();
        tokio::task::spawn_blocking(move || backend.locate_id(id).map(client_region))
            .await
            .map_err(failure)?
    }
    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        let backend = self.backend.clone();
        let result = tokio::task::spawn_blocking(move || backend.timestamp())
            .await
            .map_err(failure)?;
        if !self.transaction_tasks
            && self
                .call
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .is_some_and(|call| call.cancellation().is_cancelled())
        {
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
        let mut context: tidb_proto::KvrpcContext =
            transcode(request.context.as_ref().unwrap()).unwrap();
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
        blocking: bool,
        started: tokio::sync::Notify,
        finished: tokio::sync::Notify,
    }
    impl Backend for CleanupBackend {
        fn timestamp(&self) -> Result<Timestamp> {
            unreachable!()
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
            assert!(request.as_any().is::<kvrpcpb::ResolveLockRequest>());
            assert!(!call.cancellation().is_cancelled());
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.blocking {
                self.started.notify_one();
                assert!(
                    call.cancellation().wait_timeout(Duration::from_secs(5)),
                    "dropped background call left the transport running"
                );
                self.finished.notify_one();
                return Err(failure("context canceled"));
            }
            Ok(Box::<kvrpcpb::ResolveLockResponse>::default())
        }
    }

    #[test]
    fn background_resolve_lock_can_dispatch_after_statement_cancellation() {
        let parent = UnaryCallContext::with_timeout(Duration::from_secs(30));
        parent.cancellation().cancel();
        let backend = Arc::new(CleanupBackend::default());
        let client = ClientKv {
            backend: backend.clone(),
            address: "store1".to_owned(),
            call: Arc::new(Mutex::new(Some(parent))),
            transaction_tasks: false,
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
            backend: backend.clone(),
            address: "store1".to_owned(),
            call: Arc::new(Mutex::new(Some(parent))),
            transaction_tasks: false,
        };
        let result = runtime().block_on(client.dispatch(&kvrpcpb::ResolveLockRequest::default()));
        assert!(result.is_err());
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn background_resolution_obeys_its_owner_cancellation() {
        let backend = Arc::new(CleanupBackend::default());
        let client = ClientKv {
            backend: backend.clone(),
            address: "store1".to_owned(),
            call: Arc::new(Mutex::new(None)),
            transaction_tasks: false,
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
                    backend: backend.clone(),
                    address: "store1".to_owned(),
                    call: Arc::new(Mutex::new(None)),
                    transaction_tasks: false,
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
