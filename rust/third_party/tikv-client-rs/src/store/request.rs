// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::IntoRequest;

use crate::proto::coprocessor;
use crate::proto::debugpb;
use crate::proto::kvrpcpb;
use crate::proto::mpp;
use crate::proto::tikvpb::{self, tikv_client::TikvClient};
use crate::request::{set_context_keyspace_id, ApiV2Codec};
use crate::store::RegionWithLeader;
use crate::Error;
use crate::Result;

/// Callback that can amend a fully typed physical request before dispatch.
///
/// This is the object-safe native form of client-go's
/// `tikvrpc.ResourceGroupTagger func(*Request)`.
#[allow(dead_code)]
pub type ResourceGroupTagger = Arc<dyn Fn(&mut dyn Request) + Send + Sync>;

/// A typed response paired with the address of the logical target store.
///
/// The address is the forwarded destination when proxy forwarding is active,
/// matching client-go's `ResponseExt.Addr` contract.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub struct ResponseExt<T> {
    pub response: T,
    pub address: String,
}

static DEFAULT_REQUEST_ORIGIN: AtomicI32 = AtomicI32::new(kvrpcpb::RequestOrigin::Unknown as i32);

/// Sets the process-wide origin copied into otherwise-unset TiKV request contexts.
///
/// This is the typed Rust counterpart of client-go's
/// `tikvrpc.SetDefaultRequestOrigin`.
#[allow(dead_code)]
pub fn set_default_request_origin(origin: kvrpcpb::RequestOrigin) {
    DEFAULT_REQUEST_ORIGIN.store(origin as i32, Ordering::SeqCst);
}

/// Returns the process-wide default TiKV request origin.
pub fn get_default_request_origin() -> kvrpcpb::RequestOrigin {
    kvrpcpb::RequestOrigin::try_from(DEFAULT_REQUEST_ORIGIN.load(Ordering::SeqCst))
        .unwrap_or(kvrpcpb::RequestOrigin::Unknown)
}

fn fill_default_request_origin(context: &mut kvrpcpb::Context) {
    if context.request_origin == kvrpcpb::RequestOrigin::Unknown as i32 {
        context.request_origin = get_default_request_origin() as i32;
    }
}

pub(super) fn fill_context_default_request_origin(context: &mut Option<kvrpcpb::Context>) {
    fill_default_request_origin(context.get_or_insert_with(kvrpcpb::Context::default));
}

macro_rules! streaming_response {
    ($name:ident, $item:ty) => {
        /// A server-streaming RPC after the first response has been read.
        ///
        /// Eagerly reading `first` matches client-go's internal client: region
        /// errors surface to the request sender before it returns the stream.
        #[allow(dead_code)]
        pub struct $name {
            pub first: Option<$item>,
            stream: Option<tonic::Streaming<$item>>,
            timeout: Duration,
            ru_details: Option<Arc<crate::RuDetails>>,
            count_read_rpc: bool,
            bypass_ru_v2: bool,
        }

        #[allow(dead_code)]
        impl $name {
            pub async fn message(&mut self) -> Result<Option<$item>> {
                let Some(stream) = self.stream.as_mut() else {
                    return Ok(None);
                };
                let mut response = tokio::time::timeout(self.timeout, stream.message())
                    .await
                    .map_err(|_| {
                        Error::GrpcAPI(tonic::Status::deadline_exceeded(
                            "TiKV streaming response deadline exceeded",
                        ))
                    })?
                    .map_err(Error::from)?;
                if let Some(response) = response.as_mut() {
                    self.update_ru_v2(response);
                }
                Ok(response)
            }

            fn update_ru_v2(&mut self, response: &mut dyn Any) {
                let read_rpc_count = i64::from(std::mem::take(&mut self.count_read_rpc));
                if self.bypass_ru_v2 {
                    return;
                }
                crate::config::update_tikv_ru_v2_from_exec_details_v2(
                    exec_details_v2_mut(response),
                    read_rpc_count,
                    0,
                    self.ru_details.as_deref(),
                );
            }

            /// Cancels the remaining stream. Dropping this value has the same
            /// effect through Tonic's request-body cancellation.
            pub fn close(&mut self) {
                self.stream = None;
            }
        }
    };
}

streaming_response!(CoprocessorStreamResponse, coprocessor::Response);
streaming_response!(BatchCoprocessorStreamResponse, coprocessor::BatchResponse);
streaming_response!(MppStreamResponse, mpp::MppDataPacket);

#[cfg(test)]
impl CoprocessorStreamResponse {
    pub(crate) fn from_first_for_test(first: Option<coprocessor::Response>) -> Self {
        Self {
            first,
            stream: None,
            timeout: Duration::ZERO,
            ru_details: None,
            count_read_rpc: false,
            bypass_ru_v2: true,
        }
    }
}

/// Distinguishes client-go's `CmdCopStream` from the unary `CmdCop`, which
/// carries the same protobuf request.
#[derive(Clone)]
#[allow(dead_code)]
pub struct CoprocessorStreamRequest {
    request: coprocessor::Request,
    api_v2_codec: Option<ApiV2Codec>,
    ru_details: Option<Arc<crate::RuDetails>>,
}

#[allow(dead_code)]
impl CoprocessorStreamRequest {
    pub fn new(request: coprocessor::Request) -> Self {
        Self {
            request,
            api_v2_codec: None,
            ru_details: None,
        }
    }

    pub fn with_api_v2_codec(mut self, codec: ApiV2Codec) -> Self {
        self.api_v2_codec = Some(codec);
        self
    }

    pub fn with_ru_details(mut self, ru_details: Arc<crate::RuDetails>) -> Self {
        self.ru_details = Some(ru_details);
        self
    }

    fn wire_request(&self) -> coprocessor::Request {
        let mut request = if let Some(codec) = self.api_v2_codec.as_ref() {
            let mut request = codec.encode_coprocessor_request(&self.request);
            let context = request
                .context
                .get_or_insert_with(kvrpcpb::Context::default);
            context.api_version = kvrpcpb::ApiVersion::V2 as i32;
            set_context_keyspace_id(context, codec.keyspace_id());
            request
        } else {
            self.request.clone()
        };
        fill_context_default_request_origin(&mut request.context);
        request
    }
}

/// Client-go's TiFlash `CmdBatchCop` server-streaming request.
#[derive(Clone)]
#[allow(dead_code)]
pub struct BatchCoprocessorStreamRequest {
    request: coprocessor::BatchRequest,
    api_v2_codec: Option<ApiV2Codec>,
}

#[allow(dead_code)]
impl BatchCoprocessorStreamRequest {
    pub fn new(request: coprocessor::BatchRequest) -> Self {
        Self {
            request,
            api_v2_codec: None,
        }
    }

    pub fn with_api_v2_codec(mut self, codec: ApiV2Codec) -> Self {
        self.api_v2_codec = Some(codec);
        self
    }

    fn wire_request(&self) -> coprocessor::BatchRequest {
        let mut request = if let Some(codec) = self.api_v2_codec.as_ref() {
            let mut request = codec.encode_batch_coprocessor_request(&self.request);
            let context = request
                .context
                .get_or_insert_with(kvrpcpb::Context::default);
            context.api_version = kvrpcpb::ApiVersion::V2 as i32;
            set_context_keyspace_id(context, codec.keyspace_id());
            request
        } else {
            self.request.clone()
        };
        fill_context_default_request_origin(&mut request.context);
        request
    }
}

/// Client-go's `CmdMPPConn` server-streaming request.
#[derive(Clone)]
#[allow(dead_code)]
pub struct MppStreamRequest(pub mpp::EstablishMppConnectionRequest);

#[async_trait]
pub trait Request: Any + Sync + Send + 'static {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>>;
    fn label(&self) -> &'static str;
    fn as_any(&self) -> &dyn Any;
    fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()>;
    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion);
    /// Marks a resend of the same logical request. Context-bearing TiKV
    /// requests expose this as `Context.is_retry_request`; store-only
    /// requests intentionally retain the default no-op.
    fn set_is_retry_request(&mut self) {}
    /// Set the numeric API V2 keyspace carried alongside the request API version.
    ///
    /// Requests without a `Context` deliberately retain the no-op default.
    fn set_keyspace_id(&mut self, _keyspace_id: Option<u32>) {}
    /// Set the API V2 keyspace name carried alongside the numeric identifier.
    fn set_keyspace_name(&mut self, _keyspace_name: Option<&str>) {}
    fn set_priority(&mut self, _priority: kvrpcpb::CommandPri) {}
    /// Controls whether TiKV should bypass its data cache for this request.
    fn set_not_fill_cache(&mut self, _not_fill_cache: bool) {}
    /// Sets the isolation level carried in this request's TiKV context.
    fn set_isolation_level(&mut self, _isolation_level: kvrpcpb::IsolationLevel) {}
    /// Sets TiKV's scheduling task ID for this request.
    fn set_task_id(&mut self, _task_id: u64) {}
    /// Sets the source resource-group tag carried by this request's TiKV context.
    fn set_resource_group_tag(&mut self, _resource_group_tag: Vec<u8>) {}
    /// Marks a request sent to a selected follower or learner. Leader reads,
    /// including leader-through-proxy forwarding, retain the default false.
    fn set_replica_read(&mut self, _replica_read: bool) {}
    /// Sets TiKV's stale-read context bit. Context-free requests retain the
    /// no-op default.
    fn set_stale_read(&mut self, _stale_read: bool) {}
    /// Sets TiKV's source-configurable busy threshold for a context request.
    fn set_busy_threshold_ms(&mut self, _busy_threshold_ms: u32) {}
    /// Sets the cached source bucket version for a context request.
    fn set_buckets_version(&mut self, _buckets_version: u64) {}
    /// Source resource-control override priority used by BatchCommands
    /// admission. Requests without a TiKV `Context` are direct/normal.
    fn batch_priority(&self) -> u64 {
        0
    }
    /// Resource-group identity carried in TiKV's request context.
    fn resource_group_name(&self) -> Option<&str> {
        None
    }
    /// Sets the request's resource-group identity.
    fn set_resource_group_name(&mut self, _resource_group_name: &str) {}
    /// Sets source resource-control penalty returned by PD admission.
    fn set_resource_control_penalty(
        &mut self,
        _penalty: Option<crate::proto::resource_manager::Consumption>,
    ) {
    }
    /// Applies resource-group priority only when the caller has not supplied one.
    fn set_resource_control_priority_if_unset(&mut self, _priority: u64) {}
    /// Set TiKV's server-side maximum execution duration.
    ///
    /// Requests without a `Context` deliberately retain the no-op default.
    fn set_max_execution_duration_ms(&mut self, _duration_ms: u64) {}

    /// Returns the source `tikvrpc.Request.MaxExecutionDurationMs` carried by
    /// a context-bearing request. Store-scoped requests have no such field.
    fn max_execution_duration_ms(&self) -> u64 {
        0
    }

    /// Transactions whose locks have been determined rolled back, or whose
    /// commit timestamp is newer than this read, can be ignored by TiKV.
    /// Context-free requests deliberately retain the no-op default.
    fn set_resolved_locks(&mut self, _resolved_locks: Vec<u64>) {}

    /// Transactions committed no later than this read may be read through by
    /// TiKV without waiting for their secondary locks to be cleaned up.
    /// Context-free requests deliberately retain the no-op default.
    fn set_committed_locks(&mut self, _committed_locks: Vec<u64>) {}

    /// The TiKV context carried by a region-scoped request. Store-scoped and
    /// streaming requests deliberately retain the no-context default.
    fn tikv_context(&self) -> Option<&kvrpcpb::Context> {
        None
    }

    /// Replaces the concrete protobuf context at the transport boundary.
    ///
    /// The boolean follows client-go's `AttachContext`: source commands which
    /// do not accept a region context return false, while MPP and Empty store
    /// commands accept the operation as an intentional no-op.
    fn attach_context(&mut self, _context: kvrpcpb::Context) -> bool {
        false
    }

    /// Source `tikvrpc.Request.GetStartTS` for logging and retry diagnostics.
    fn start_timestamp(&self) -> u64 {
        request_start_timestamp(self.as_any())
    }

    /// Source `tikvrpc.Request.GetSize()` for resource-control accounting.
    /// Concrete protobuf requests provide their encoded size; non-protobuf
    /// test or stream wrappers retain zero until they model a wire request.
    fn encoded_request_size(&self) -> u64 {
        0
    }

    /// Source `tikvrpc.Request.GetSize` intentionally covers only this
    /// historical command subset; other protobuf requests report zero even
    /// though their wire encoding has a size.
    fn network_request_size(&self) -> u64 {
        if matches!(
            self.label(),
            "kv_get"
                | "kv_batch_get"
                | "kv_scan"
                | "coprocessor"
                | "kv_prewrite"
                | "kv_commit"
                | "kv_pessimistic_lock"
                | "kv_pessimistic_rollback"
                | "kv_batch_rollback"
                | "kv_check_secondary_locks_request"
                | "kv_scan_lock"
                | "kv_resolve_lock"
                | "kv_flush"
                | "kv_check_txn_status"
                | "dispatch_mpp_task"
        ) {
            self.encoded_request_size()
        } else {
            0
        }
    }

    /// Source `isReadReq` classification used only by replica-read byte
    /// observations, not the broader resource-control read/write split.
    fn is_network_read_request(&self) -> bool {
        matches!(
            self.label(),
            "kv_get"
                | "kv_batch_get"
                | "kv_scan"
                | "coprocessor"
                | "batch_coprocessor"
                | "coprocessor_stream"
        )
    }

    /// Apply the source transport-level codec after observability has consumed
    /// the physical response. Most Rust requests decode in their typed plan;
    /// stream wrappers use this hook because they cannot implement `KvRequest`
    /// with a cloneable response.
    fn decode_transport_response(&self, _response: &mut dyn Any) -> Result<()> {
        Ok(())
    }

    /// Source `IsTxnWriteRequest || IsRawWriteRequest` classification. The
    /// generic request wrapper owns this classification rather than callers
    /// guessing from an operation's high-level API.
    fn is_resource_control_write(&self) -> bool {
        matches!(
            self.label(),
            "kv_pessimistic_lock"
                | "kv_prewrite"
                | "kv_commit"
                | "kv_batch_rollback"
                | "kv_pessimistic_rollback"
                | "kv_check_txn_status"
                | "kv_check_secondary_locks_request"
                | "kv_cleanup"
                | "kv_txn_heart_beat"
                | "kv_resolve_lock"
                | "kv_flashback_to_version"
                | "kv_prepare_flashback_to_version"
                | "kv_flush"
                | "raw_put"
                | "raw_batch_put"
                | "raw_delete"
        )
    }

    /// Source `resourcecontrol.isCopRequest`: only ordinary and streaming
    /// coprocessor reads participate in PD's paging accounting. BatchCop is
    /// deliberately excluded even though it also carries an analyze type.
    fn is_resource_control_coprocessor(&self) -> bool {
        matches!(self.label(), "coprocessor" | "coprocessor_stream")
    }

    /// Returns the coprocessor request type used by NextGen's internal
    /// analyze bypass. Ordinary Cop can be recovered from its protobuf;
    /// stream wrappers override this to expose their owned wire request.
    fn resource_control_coprocessor_type(&self) -> Option<i64> {
        self.as_any()
            .downcast_ref::<coprocessor::Request>()
            .map(|request| request.tp)
    }

    /// Dispatch with source `tikvrpc.Request.ForwardedHost` transport
    /// metadata. Implementations that do not represent a TiKV RPC retain the
    /// ordinary dispatch behavior by default.
    async fn dispatch_with_forwarded_host(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
        _forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        self.dispatch(client, timeout).await
    }
}

fn request_start_timestamp(request: &dyn Any) -> u64 {
    macro_rules! timestamp_field {
        ($type:ty, $field:ident) => {
            if let Some(request) = request.downcast_ref::<$type>() {
                return request.$field;
            }
        };
    }

    timestamp_field!(kvrpcpb::GetRequest, version);
    timestamp_field!(kvrpcpb::ScanRequest, version);
    timestamp_field!(kvrpcpb::PrewriteRequest, start_version);
    timestamp_field!(kvrpcpb::CommitRequest, start_version);
    timestamp_field!(kvrpcpb::CleanupRequest, start_version);
    timestamp_field!(kvrpcpb::BatchGetRequest, version);
    timestamp_field!(kvrpcpb::BatchRollbackRequest, start_version);
    timestamp_field!(kvrpcpb::ScanLockRequest, max_version);
    timestamp_field!(kvrpcpb::ResolveLockRequest, start_version);
    timestamp_field!(kvrpcpb::PessimisticLockRequest, start_version);
    timestamp_field!(kvrpcpb::PessimisticRollbackRequest, start_version);
    timestamp_field!(kvrpcpb::TxnHeartBeatRequest, start_version);
    timestamp_field!(kvrpcpb::CheckTxnStatusRequest, lock_ts);
    timestamp_field!(kvrpcpb::CheckSecondaryLocksRequest, start_version);
    timestamp_field!(kvrpcpb::FlashbackToVersionRequest, start_ts);
    timestamp_field!(kvrpcpb::PrepareFlashbackToVersionRequest, start_ts);
    timestamp_field!(kvrpcpb::FlushRequest, start_ts);
    timestamp_field!(kvrpcpb::BufferBatchGetRequest, version);
    timestamp_field!(coprocessor::Request, start_ts);
    timestamp_field!(coprocessor::BatchRequest, start_ts);
    timestamp_field!(kvrpcpb::MvccGetByStartTsRequest, start_ts);

    if let Some(request) = request.downcast_ref::<CoprocessorStreamRequest>() {
        return request.request.start_ts;
    }
    if let Some(request) = request.downcast_ref::<BatchCoprocessorStreamRequest>() {
        return request.request.start_ts;
    }
    0
}

/// Returns the mutable execution details carried by a successful unary TiKV
/// response. This is the native counterpart of client-go's protobuf
/// `getExecDetailsV2` interface assertion.
pub(crate) fn exec_details_v2_mut(response: &mut dyn Any) -> Option<&mut kvrpcpb::ExecDetailsV2> {
    macro_rules! detail_response {
        ($($response:ty),+ $(,)?) => {
            $(
                if response.is::<$response>() {
                    return response
                        .downcast_mut::<$response>()
                        .and_then(|response| response.exec_details_v2.as_mut());
                }
            )+
        };
    }
    detail_response!(
        kvrpcpb::GetResponse,
        kvrpcpb::PrewriteResponse,
        kvrpcpb::PessimisticLockResponse,
        kvrpcpb::PessimisticRollbackResponse,
        kvrpcpb::TxnHeartBeatResponse,
        kvrpcpb::CheckTxnStatusResponse,
        kvrpcpb::CheckSecondaryLocksResponse,
        kvrpcpb::CommitResponse,
        kvrpcpb::BatchGetResponse,
        kvrpcpb::BatchRollbackResponse,
        kvrpcpb::ScanLockResponse,
        kvrpcpb::ResolveLockResponse,
        kvrpcpb::FlushResponse,
        kvrpcpb::BufferBatchGetResponse,
        coprocessor::Response,
    );
    None
}

/// Immutable companion used by source transport latency accounting.
pub(crate) fn exec_details_v2(response: &dyn Any) -> Option<&kvrpcpb::ExecDetailsV2> {
    macro_rules! detail_response {
        ($($response:ty),+ $(,)?) => {
            $(
                if let Some(response) = response.downcast_ref::<$response>() {
                    return response.exec_details_v2.as_ref();
                }
            )+
        };
    }
    detail_response!(
        kvrpcpb::GetResponse,
        kvrpcpb::PrewriteResponse,
        kvrpcpb::PessimisticLockResponse,
        kvrpcpb::PessimisticRollbackResponse,
        kvrpcpb::TxnHeartBeatResponse,
        kvrpcpb::CheckTxnStatusResponse,
        kvrpcpb::CheckSecondaryLocksResponse,
        kvrpcpb::CommitResponse,
        kvrpcpb::BatchGetResponse,
        kvrpcpb::BatchRollbackResponse,
        kvrpcpb::ScanLockResponse,
        kvrpcpb::ResolveLockResponse,
        kvrpcpb::FlushResponse,
        kvrpcpb::BufferBatchGetResponse,
        coprocessor::Response,
    );
    if let Some(response) = response.downcast_ref::<CoprocessorStreamResponse>() {
        return response
            .first
            .as_ref()
            .and_then(|response| response.exec_details_v2.as_ref());
    }
    None
}

/// Source `tikvrpc.Response.GetSize` matrix. Keep this deliberately narrower
/// than all generated response types: unlisted commands report zero.
pub(crate) fn network_response_size(response: &dyn Any) -> u64 {
    macro_rules! response_size {
        ($($response:ty),+ $(,)?) => {
            $(
                if let Some(response) = response.downcast_ref::<$response>() {
                    return response.encoded_len() as u64;
                }
            )+
        };
    }
    response_size!(
        kvrpcpb::GetResponse,
        kvrpcpb::BatchGetResponse,
        kvrpcpb::ScanResponse,
        coprocessor::Response,
        kvrpcpb::PrewriteResponse,
        kvrpcpb::CommitResponse,
        kvrpcpb::PessimisticLockResponse,
        kvrpcpb::PessimisticRollbackResponse,
        kvrpcpb::BatchRollbackResponse,
        kvrpcpb::CheckSecondaryLocksResponse,
        kvrpcpb::ScanLockResponse,
        kvrpcpb::ResolveLockResponse,
        kvrpcpb::FlushResponse,
        kvrpcpb::CheckTxnStatusResponse,
        mpp::MppDataPacket,
        mpp::DispatchTaskResponse,
    );
    0
}

fn with_forwarded_host<T>(
    mut request: tonic::Request<T>,
    forwarded_host: &str,
) -> Result<tonic::Request<T>> {
    crate::trace::inject_current_grpc_trace_metadata(request.metadata_mut());
    if !forwarded_host.is_empty() {
        request.metadata_mut().insert(
            "tikv-forwarded-host",
            MetadataValue::try_from(forwarded_host).map_err(|error| {
                Error::StringError(format!("invalid unary forwarding host metadata: {error}"))
            })?,
        );
    }
    Ok(request)
}

macro_rules! impl_request {
    ($name: ident, $fun: ident, $label: literal) => {
        impl_request!($name, $fun, $label, true);
    };
    ($name: ident, $fun: ident, $label: literal, $accepts_context: literal) => {
        #[async_trait]
        impl Request for kvrpcpb::$name {
            async fn dispatch(
                &self,
                client: &TikvClient<Channel>,
                timeout: Duration,
            ) -> Result<Box<dyn Any>> {
                let mut wire_request = self.clone();
                if $accepts_context {
                    fill_context_default_request_origin(&mut wire_request.context);
                }
                let mut req = with_forwarded_host(wire_request.into_request(), "")?;
                req.set_timeout(timeout);
                client
                    .clone()
                    .$fun(req)
                    .await
                    .map(|r| Box::new(r.into_inner()) as Box<dyn Any>)
                    .map_err(Error::from)
            }

            async fn dispatch_with_forwarded_host(
                &self,
                client: &TikvClient<Channel>,
                timeout: Duration,
                forwarded_host: &str,
            ) -> Result<Box<dyn Any>> {
                let mut wire_request = self.clone();
                if $accepts_context {
                    fill_context_default_request_origin(&mut wire_request.context);
                }
                let mut req = with_forwarded_host(wire_request.into_request(), forwarded_host)?;
                req.set_timeout(timeout);
                client
                    .clone()
                    .$fun(req)
                    .await
                    .map(|r| Box::new(r.into_inner()) as Box<dyn Any>)
                    .map_err(Error::from)
            }

            fn label(&self) -> &'static str {
                $label
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()> {
                let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                let leader_peer = leader.leader.as_ref().ok_or(Error::LeaderNotFound {
                    region: leader.ver_id(),
                })?;
                ctx.region_id = leader.region.id;
                ctx.region_epoch = leader.region.region_epoch.clone();
                ctx.peer = Some(leader_peer.clone());
                Ok(())
            }

            fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
                let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                ctx.api_version = api_version.into();
            }

            fn set_is_retry_request(&mut self) {
                self.context
                    .get_or_insert(kvrpcpb::Context::default())
                    .is_retry_request = true;
            }

            fn set_keyspace_id(&mut self, keyspace_id: Option<u32>) {
                if let Some(keyspace_id) = keyspace_id {
                    let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                    set_context_keyspace_id(ctx, keyspace_id);
                }
            }

            fn set_keyspace_name(&mut self, keyspace_name: Option<&str>) {
                if let Some(keyspace_name) = keyspace_name {
                    let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                    ctx.keyspace_name = keyspace_name.to_owned();
                }
            }

            fn set_priority(&mut self, priority: kvrpcpb::CommandPri) {
                let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                ctx.priority = priority.into();
            }

            fn set_not_fill_cache(&mut self, not_fill_cache: bool) {
                self.context
                    .get_or_insert(kvrpcpb::Context::default())
                    .not_fill_cache = not_fill_cache;
            }

            fn set_isolation_level(&mut self, isolation_level: kvrpcpb::IsolationLevel) {
                self.context
                    .get_or_insert(kvrpcpb::Context::default())
                    .isolation_level = isolation_level.into();
            }

            fn set_task_id(&mut self, task_id: u64) {
                self.context
                    .get_or_insert(kvrpcpb::Context::default())
                    .task_id = task_id;
            }

            fn set_resource_group_tag(&mut self, resource_group_tag: Vec<u8>) {
                self.context
                    .get_or_insert(kvrpcpb::Context::default())
                    .resource_group_tag = resource_group_tag;
            }

            fn set_replica_read(&mut self, replica_read: bool) {
                let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                ctx.replica_read = replica_read;
            }

            fn set_stale_read(&mut self, stale_read: bool) {
                let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                ctx.stale_read = stale_read;
            }

            fn set_busy_threshold_ms(&mut self, busy_threshold_ms: u32) {
                let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                ctx.busy_threshold_ms = busy_threshold_ms;
            }

            fn set_buckets_version(&mut self, buckets_version: u64) {
                let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                ctx.buckets_version = buckets_version;
            }

            fn batch_priority(&self) -> u64 {
                self.context
                    .as_ref()
                    .and_then(|context| context.resource_control_context.as_ref())
                    .map_or(0, |resource_control| resource_control.override_priority)
            }

            fn resource_group_name(&self) -> Option<&str> {
                self.context
                    .as_ref()
                    .and_then(|context| context.resource_control_context.as_ref())
                    .map(|resource_control| resource_control.resource_group_name.as_str())
                    .filter(|resource_group_name| !resource_group_name.is_empty())
            }

            fn set_resource_group_name(&mut self, resource_group_name: &str) {
                self.context
                    .get_or_insert_with(kvrpcpb::Context::default)
                    .resource_control_context
                    .get_or_insert_with(kvrpcpb::ResourceControlContext::default)
                    .resource_group_name = resource_group_name.to_owned();
            }

            fn set_resource_control_penalty(
                &mut self,
                penalty: Option<crate::proto::resource_manager::Consumption>,
            ) {
                self.context
                    .get_or_insert_with(kvrpcpb::Context::default)
                    .resource_control_context
                    .get_or_insert_with(kvrpcpb::ResourceControlContext::default)
                    .penalty = penalty;
            }

            fn set_resource_control_priority_if_unset(&mut self, priority: u64) {
                let resource_control = self
                    .context
                    .get_or_insert_with(kvrpcpb::Context::default)
                    .resource_control_context
                    .get_or_insert_with(kvrpcpb::ResourceControlContext::default);
                if resource_control.override_priority == 0 {
                    resource_control.override_priority = priority;
                }
            }

            fn set_max_execution_duration_ms(&mut self, duration_ms: u64) {
                let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                ctx.max_execution_duration_ms = duration_ms;
            }

            fn max_execution_duration_ms(&self) -> u64 {
                self.context
                    .as_ref()
                    .map_or(0, |context| context.max_execution_duration_ms)
            }

            fn set_resolved_locks(&mut self, resolved_locks: Vec<u64>) {
                self.context
                    .get_or_insert_with(kvrpcpb::Context::default)
                    .resolved_locks = resolved_locks;
            }

            fn set_committed_locks(&mut self, committed_locks: Vec<u64>) {
                self.context
                    .get_or_insert_with(kvrpcpb::Context::default)
                    .committed_locks = committed_locks;
            }

            fn tikv_context(&self) -> Option<&kvrpcpb::Context> {
                self.context.as_ref()
            }

            fn attach_context(&mut self, mut context: kvrpcpb::Context) -> bool {
                if !$accepts_context {
                    return false;
                }
                fill_default_request_origin(&mut context);
                self.context = Some(context);
                true
            }

            fn encoded_request_size(&self) -> u64 {
                self.encoded_len() as u64
            }
        }
    };
}

/// Implements a store-level unary RPC whose protobuf has no `Context`.
///
/// These requests must not acquire a region leader or inherit request-context
/// metadata. This matches client-go's `AttachContext` exceptions for MPP and
/// other store-scoped commands.
macro_rules! impl_store_request {
    ($ty:path, $fun:ident, $label:literal, $accepts_context:literal) => {
        #[async_trait]
        impl Request for $ty {
            async fn dispatch(
                &self,
                client: &TikvClient<Channel>,
                timeout: Duration,
            ) -> Result<Box<dyn Any>> {
                let mut request = with_forwarded_host(self.clone().into_request(), "")?;
                request.set_timeout(timeout);
                client
                    .clone()
                    .$fun(request)
                    .await
                    .map(|response| Box::new(response.into_inner()) as Box<dyn Any>)
                    .map_err(Error::from)
            }

            async fn dispatch_with_forwarded_host(
                &self,
                client: &TikvClient<Channel>,
                timeout: Duration,
                forwarded_host: &str,
            ) -> Result<Box<dyn Any>> {
                let mut request = with_forwarded_host(self.clone().into_request(), forwarded_host)?;
                request.set_timeout(timeout);
                client
                    .clone()
                    .$fun(request)
                    .await
                    .map(|response| Box::new(response.into_inner()) as Box<dyn Any>)
                    .map_err(Error::from)
            }

            fn label(&self) -> &'static str {
                $label
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
                Ok(())
            }

            fn set_api_version(&mut self, _api_version: kvrpcpb::ApiVersion) {}

            fn attach_context(&mut self, _context: kvrpcpb::Context) -> bool {
                $accepts_context
            }
        }
    };
}

impl_request!(RawGetRequest, raw_get, "raw_get");
impl_request!(RawBatchGetRequest, raw_batch_get, "raw_batch_get");
impl_request!(RawGetKeyTtlRequest, raw_get_key_ttl, "raw_get_key_ttl");
impl_request!(RawPutRequest, raw_put, "raw_put");
impl_request!(RawBatchPutRequest, raw_batch_put, "raw_batch_put");
impl_request!(RawDeleteRequest, raw_delete, "raw_delete");
impl_request!(RawBatchDeleteRequest, raw_batch_delete, "raw_batch_delete");
impl_request!(RawScanRequest, raw_scan, "raw_scan");
impl_request!(RawBatchScanRequest, raw_batch_scan, "raw_batch_scan");
impl_request!(RawDeleteRangeRequest, raw_delete_range, "raw_delete_range");
impl_request!(RawCasRequest, raw_compare_and_swap, "raw_compare_and_swap");
impl_request!(RawCoprocessorRequest, raw_coprocessor, "raw_coprocessor");
impl_request!(RawChecksumRequest, raw_checksum, "raw_checksum");

impl_request!(GetRequest, kv_get, "kv_get");
impl_request!(ScanRequest, kv_scan, "kv_scan");
impl_request!(PrewriteRequest, kv_prewrite, "kv_prewrite");
impl_request!(CommitRequest, kv_commit, "kv_commit");
impl_request!(CleanupRequest, kv_cleanup, "kv_cleanup");
impl_request!(BatchGetRequest, kv_batch_get, "kv_batch_get");
impl_request!(BatchRollbackRequest, kv_batch_rollback, "kv_batch_rollback");
impl_request!(
    PessimisticRollbackRequest,
    kv_pessimistic_rollback,
    "kv_pessimistic_rollback"
);
impl_request!(ResolveLockRequest, kv_resolve_lock, "kv_resolve_lock");
impl_request!(ScanLockRequest, kv_scan_lock, "kv_scan_lock");
impl_request!(
    PessimisticLockRequest,
    kv_pessimistic_lock,
    "kv_pessimistic_lock"
);
impl_request!(TxnHeartBeatRequest, kv_txn_heart_beat, "kv_txn_heart_beat");
impl_request!(
    CheckTxnStatusRequest,
    kv_check_txn_status,
    "kv_check_txn_status"
);
impl_request!(
    CheckSecondaryLocksRequest,
    kv_check_secondary_locks,
    "kv_check_secondary_locks_request"
);
impl_request!(GcRequest, kv_gc, "kv_gc");
impl_request!(DeleteRangeRequest, kv_delete_range, "kv_delete_range");
impl_request!(
    PrepareFlashbackToVersionRequest,
    kv_prepare_flashback_to_version,
    "kv_prepare_flashback_to_version"
);
impl_request!(
    FlashbackToVersionRequest,
    kv_flashback_to_version,
    "kv_flashback_to_version"
);
impl_request!(FlushRequest, kv_flush, "kv_flush");
impl_request!(
    BufferBatchGetRequest,
    kv_buffer_batch_get,
    "kv_buffer_batch_get"
);
impl_request!(
    UnsafeDestroyRangeRequest,
    unsafe_destroy_range,
    "unsafe_destroy_range"
);
impl_request!(
    PhysicalScanLockRequest,
    physical_scan_lock,
    "physical_scan_lock"
);
impl_request!(MvccGetByKeyRequest, mvcc_get_by_key, "mvcc_get_by_key");
impl_request!(
    MvccGetByStartTsRequest,
    mvcc_get_by_start_ts,
    "mvcc_get_by_start_ts"
);
impl_request!(
    CheckLockObserverRequest,
    check_lock_observer,
    "check_lock_observer"
);
impl_request!(
    RegisterLockObserverRequest,
    register_lock_observer,
    "register_lock_observer"
);
impl_request!(
    RemoveLockObserverRequest,
    remove_lock_observer,
    "remove_lock_observer"
);
impl_request!(
    GetLockWaitInfoRequest,
    get_lock_wait_info,
    "get_lock_wait_info"
);
impl_request!(SplitRegionRequest, split_region, "split_region");
impl_request!(
    GetHealthFeedbackRequest,
    get_health_feedback,
    "get_health_feedback",
    false
);
impl_request!(
    BroadcastTxnStatusRequest,
    broadcast_txn_status,
    "broadcast_txn_status",
    false
);

impl_store_request!(
    mpp::CancelTaskRequest,
    cancel_mpp_task,
    "cancel_mpp_task",
    true
);
impl_store_request!(mpp::IsAliveRequest, is_alive, "is_alive", true);
impl_store_request!(
    kvrpcpb::TiFlashSystemTableRequest,
    get_ti_flash_system_table,
    "get_tiflash_system_table",
    false
);

#[async_trait]
impl Request for debugpb::GetRegionPropertiesRequest {
    async fn dispatch(
        &self,
        _client: &TikvClient<Channel>,
        _timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        Err(Error::StringError(
            "GetRegionProperties requires KvRpcClient's debug service channel".to_owned(),
        ))
    }

    fn label(&self) -> &'static str {
        "debug_get_region_properties"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
        Ok(())
    }

    fn set_api_version(&mut self, _api_version: kvrpcpb::ApiVersion) {}
}

#[async_trait]
impl Request for tikvpb::BatchCommandsEmptyRequest {
    async fn dispatch(
        &self,
        _client: &TikvClient<Channel>,
        _timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        Ok(Box::new(tikvpb::BatchCommandsEmptyResponse::default()))
    }

    fn label(&self) -> &'static str {
        "empty"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
        Ok(())
    }

    fn set_api_version(&mut self, _api_version: kvrpcpb::ApiVersion) {}

    fn attach_context(&mut self, _context: kvrpcpb::Context) -> bool {
        true
    }
}

#[async_trait]
impl Request for kvrpcpb::CompactRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        let mut request = with_forwarded_host(self.clone().into_request(), "")?;
        request.set_timeout(timeout);
        client
            .clone()
            .compact(request)
            .await
            .map(|response| Box::new(response.into_inner()) as Box<dyn Any>)
            .map_err(Error::from)
    }

    async fn dispatch_with_forwarded_host(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        let mut request = with_forwarded_host(self.clone().into_request(), forwarded_host)?;
        request.set_timeout(timeout);
        client
            .clone()
            .compact(request)
            .await
            .map(|response| Box::new(response.into_inner()) as Box<dyn Any>)
            .map_err(Error::from)
    }

    fn label(&self) -> &'static str {
        "compact"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
        Ok(())
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        self.api_version = api_version.into();
    }

    fn set_keyspace_id(&mut self, keyspace_id: Option<u32>) {
        if let Some(keyspace_id) = keyspace_id {
            self.keyspace = Some(kvrpcpb::compact_request::Keyspace::KeyspaceId(keyspace_id));
        }
    }
}

#[async_trait]
impl Request for kvrpcpb::StoreSafeTsRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        let mut request = with_forwarded_host(self.clone().into_request(), "")?;
        request.set_timeout(timeout);
        client
            .clone()
            .get_store_safe_ts(request)
            .await
            .map(|response| Box::new(response.into_inner()) as Box<dyn Any>)
            .map_err(Error::from)
    }

    async fn dispatch_with_forwarded_host(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        let mut request = with_forwarded_host(self.clone().into_request(), forwarded_host)?;
        request.set_timeout(timeout);
        client
            .clone()
            .get_store_safe_ts(request)
            .await
            .map(|response| Box::new(response.into_inner()) as Box<dyn Any>)
            .map_err(Error::from)
    }

    fn label(&self) -> &'static str {
        "store_safe_ts"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
        Ok(())
    }

    fn set_api_version(&mut self, _api_version: kvrpcpb::ApiVersion) {}
}

#[async_trait]
impl Request for coprocessor::Request {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        let mut wire_request = self.clone();
        fill_context_default_request_origin(&mut wire_request.context);
        let mut request = with_forwarded_host(wire_request.into_request(), "")?;
        request.set_timeout(timeout);
        client
            .clone()
            .coprocessor(request)
            .await
            .map(|response| Box::new(response.into_inner()) as Box<dyn Any>)
            .map_err(Error::from)
    }

    async fn dispatch_with_forwarded_host(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        let mut wire_request = self.clone();
        fill_context_default_request_origin(&mut wire_request.context);
        let mut request = with_forwarded_host(wire_request.into_request(), forwarded_host)?;
        request.set_timeout(timeout);
        client
            .clone()
            .coprocessor(request)
            .await
            .map(|response| Box::new(response.into_inner()) as Box<dyn Any>)
            .map_err(Error::from)
    }

    fn label(&self) -> &'static str {
        "coprocessor"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()> {
        let context = self.context.get_or_insert(kvrpcpb::Context::default());
        let leader_peer = leader.leader.as_ref().ok_or(Error::LeaderNotFound {
            region: leader.ver_id(),
        })?;
        context.region_id = leader.region.id;
        context.region_epoch = leader.region.region_epoch.clone();
        context.peer = Some(leader_peer.clone());
        Ok(())
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        self.context
            .get_or_insert(kvrpcpb::Context::default())
            .api_version = api_version.into();
    }

    fn set_is_retry_request(&mut self) {
        self.context
            .get_or_insert(kvrpcpb::Context::default())
            .is_retry_request = true;
    }

    fn set_keyspace_id(&mut self, keyspace_id: Option<u32>) {
        if let Some(keyspace_id) = keyspace_id {
            set_context_keyspace_id(
                self.context.get_or_insert(kvrpcpb::Context::default()),
                keyspace_id,
            );
        }
    }

    fn set_keyspace_name(&mut self, keyspace_name: Option<&str>) {
        if let Some(keyspace_name) = keyspace_name {
            self.context
                .get_or_insert(kvrpcpb::Context::default())
                .keyspace_name = keyspace_name.to_owned();
        }
    }

    fn set_priority(&mut self, priority: kvrpcpb::CommandPri) {
        self.context
            .get_or_insert(kvrpcpb::Context::default())
            .priority = priority.into();
    }

    fn set_max_execution_duration_ms(&mut self, duration_ms: u64) {
        self.context
            .get_or_insert(kvrpcpb::Context::default())
            .max_execution_duration_ms = duration_ms;
    }

    fn max_execution_duration_ms(&self) -> u64 {
        self.context
            .as_ref()
            .map_or(0, |context| context.max_execution_duration_ms)
    }

    fn tikv_context(&self) -> Option<&kvrpcpb::Context> {
        self.context.as_ref()
    }

    fn attach_context(&mut self, mut context: kvrpcpb::Context) -> bool {
        fill_default_request_origin(&mut context);
        self.context = Some(context);
        true
    }

    fn encoded_request_size(&self) -> u64 {
        self.encoded_len() as u64
    }
}

#[async_trait]
impl Request for CoprocessorStreamRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        self.dispatch_with_forwarded_host(client, timeout, "").await
    }

    async fn dispatch_with_forwarded_host(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        let mut request = with_forwarded_host(self.wire_request().into_request(), forwarded_host)?;
        request.set_timeout(timeout);
        let mut stream = client
            .clone()
            .coprocessor_stream(request)
            .await
            .map_err(Error::from)?
            .into_inner();
        let first = tokio::time::timeout(timeout, stream.message())
            .await
            .map_err(|_| {
                Error::GrpcAPI(tonic::Status::deadline_exceeded(
                    "TiKV CoprocessorStream first response deadline exceeded",
                ))
            })?
            .map_err(Error::from)?;
        let mut response = CoprocessorStreamResponse {
            first,
            stream: Some(stream),
            timeout,
            ru_details: self.ru_details.clone(),
            count_read_rpc: true,
            bypass_ru_v2: crate::resource_control::RequestInfo::from_store_request(self).bypass,
        };
        if let Some(first) = response.first.as_mut() {
            let read_rpc_count = i64::from(std::mem::take(&mut response.count_read_rpc));
            crate::config::update_tikv_ru_v2_from_exec_details_v2(
                exec_details_v2_mut(first),
                read_rpc_count,
                0,
                response.ru_details.as_deref(),
            );
        }
        Ok(Box::new(response))
    }

    fn label(&self) -> &'static str {
        "coprocessor_stream"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()> {
        self.request.set_leader(leader)
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        self.request.set_api_version(api_version);
    }

    fn set_is_retry_request(&mut self) {
        self.request.set_is_retry_request();
    }

    fn set_keyspace_id(&mut self, keyspace_id: Option<u32>) {
        self.request.set_keyspace_id(keyspace_id);
    }

    fn set_keyspace_name(&mut self, keyspace_name: Option<&str>) {
        self.request.set_keyspace_name(keyspace_name);
    }

    fn set_priority(&mut self, priority: kvrpcpb::CommandPri) {
        self.request.set_priority(priority);
    }

    fn set_max_execution_duration_ms(&mut self, duration_ms: u64) {
        self.request.set_max_execution_duration_ms(duration_ms);
    }

    fn max_execution_duration_ms(&self) -> u64 {
        self.request.max_execution_duration_ms()
    }

    fn tikv_context(&self) -> Option<&kvrpcpb::Context> {
        self.request.tikv_context()
    }

    fn attach_context(&mut self, context: kvrpcpb::Context) -> bool {
        self.request.attach_context(context)
    }

    fn encoded_request_size(&self) -> u64 {
        self.wire_request().encoded_len() as u64
    }

    fn resource_control_coprocessor_type(&self) -> Option<i64> {
        Some(self.request.tp)
    }

    fn decode_transport_response(&self, _response: &mut dyn Any) -> Result<()> {
        if self.api_v2_codec.is_some() {
            return Err(Error::StringError(
                "streaming coprocessor is not supported yet".to_owned(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl Request for BatchCoprocessorStreamRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        self.dispatch_with_forwarded_host(client, timeout, "").await
    }

    async fn dispatch_with_forwarded_host(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        let mut request = with_forwarded_host(self.wire_request().into_request(), forwarded_host)?;
        request.set_timeout(timeout);
        let mut stream = client
            .clone()
            .batch_coprocessor(request)
            .await
            .map_err(Error::from)?
            .into_inner();
        let first = tokio::time::timeout(timeout, stream.message())
            .await
            .map_err(|_| {
                Error::GrpcAPI(tonic::Status::deadline_exceeded(
                    "TiKV BatchCoprocessor first response deadline exceeded",
                ))
            })?
            .map_err(Error::from)?;
        Ok(Box::new(BatchCoprocessorStreamResponse {
            first,
            stream: Some(stream),
            timeout,
            ru_details: None,
            count_read_rpc: false,
            bypass_ru_v2: true,
        }))
    }

    fn label(&self) -> &'static str {
        "batch_coprocessor"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
        Ok(())
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        self.request
            .context
            .get_or_insert(kvrpcpb::Context::default())
            .api_version = api_version.into();
    }

    fn set_keyspace_id(&mut self, keyspace_id: Option<u32>) {
        if let Some(keyspace_id) = keyspace_id {
            set_context_keyspace_id(
                self.request
                    .context
                    .get_or_insert(kvrpcpb::Context::default()),
                keyspace_id,
            );
        }
    }

    fn set_keyspace_name(&mut self, keyspace_name: Option<&str>) {
        if let Some(keyspace_name) = keyspace_name {
            self.request
                .context
                .get_or_insert(kvrpcpb::Context::default())
                .keyspace_name = keyspace_name.to_owned();
        }
    }

    fn set_priority(&mut self, priority: kvrpcpb::CommandPri) {
        self.request
            .context
            .get_or_insert(kvrpcpb::Context::default())
            .priority = priority.into();
    }

    fn tikv_context(&self) -> Option<&kvrpcpb::Context> {
        self.request.context.as_ref()
    }

    fn attach_context(&mut self, mut context: kvrpcpb::Context) -> bool {
        fill_default_request_origin(&mut context);
        self.request.context = Some(context);
        true
    }

    fn encoded_request_size(&self) -> u64 {
        self.wire_request().encoded_len() as u64
    }

    fn resource_control_coprocessor_type(&self) -> Option<i64> {
        Some(self.request.tp)
    }
}

#[async_trait]
impl Request for MppStreamRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        self.dispatch_with_forwarded_host(client, timeout, "").await
    }

    async fn dispatch_with_forwarded_host(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        let mut request = with_forwarded_host(self.0.clone().into_request(), forwarded_host)?;
        request.set_timeout(timeout);
        let mut stream = client
            .clone()
            .establish_mpp_connection(request)
            .await
            .map_err(Error::from)?
            .into_inner();
        let first = tokio::time::timeout(timeout, stream.message())
            .await
            .map_err(|_| {
                Error::GrpcAPI(tonic::Status::deadline_exceeded(
                    "TiKV MPP stream first response deadline exceeded",
                ))
            })?
            .map_err(Error::from)?;
        Ok(Box::new(MppStreamResponse {
            first,
            stream: Some(stream),
            timeout,
            ru_details: None,
            count_read_rpc: false,
            bypass_ru_v2: true,
        }))
    }

    fn label(&self) -> &'static str {
        "establish_mpp_connection"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
        Ok(())
    }

    fn set_api_version(&mut self, _api_version: kvrpcpb::ApiVersion) {}

    fn attach_context(&mut self, _context: kvrpcpb::Context) -> bool {
        true
    }

    fn encoded_request_size(&self) -> u64 {
        self.0.encoded_len() as u64
    }
}

#[async_trait]
impl Request for mpp::DispatchTaskRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        let mut request = with_forwarded_host(self.clone().into_request(), "")?;
        request.set_timeout(timeout);
        client
            .clone()
            .dispatch_mpp_task(request)
            .await
            .map(|response| Box::new(response.into_inner()) as Box<dyn Any>)
            .map_err(Error::from)
    }

    async fn dispatch_with_forwarded_host(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        let mut request = with_forwarded_host(self.clone().into_request(), forwarded_host)?;
        request.set_timeout(timeout);
        client
            .clone()
            .dispatch_mpp_task(request)
            .await
            .map(|response| Box::new(response.into_inner()) as Box<dyn Any>)
            .map_err(Error::from)
    }

    fn label(&self) -> &'static str {
        "dispatch_mpp_task"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
        Ok(())
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        if let Some(meta) = &mut self.meta {
            meta.api_version = api_version.into();
        }
    }

    fn attach_context(&mut self, _context: kvrpcpb::Context) -> bool {
        true
    }

    fn set_keyspace_id(&mut self, keyspace_id: Option<u32>) {
        if let (Some(meta), Some(keyspace_id)) = (&mut self.meta, keyspace_id) {
            meta.keyspace = Some(mpp::task_meta::Keyspace::KeyspaceId(keyspace_id));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_call_rpc_command_matrix_has_typed_request_implementations() {
        let requests: Vec<Box<dyn Request>> = vec![
            Box::new(kvrpcpb::GetRequest::default()),
            Box::new(kvrpcpb::ScanRequest::default()),
            Box::new(kvrpcpb::PrewriteRequest::default()),
            Box::new(kvrpcpb::PessimisticLockRequest::default()),
            Box::new(kvrpcpb::PessimisticRollbackRequest::default()),
            Box::new(kvrpcpb::CommitRequest::default()),
            Box::new(kvrpcpb::CleanupRequest::default()),
            Box::new(kvrpcpb::BatchGetRequest::default()),
            Box::new(kvrpcpb::BatchRollbackRequest::default()),
            Box::new(kvrpcpb::ScanLockRequest::default()),
            Box::new(kvrpcpb::ResolveLockRequest::default()),
            Box::new(kvrpcpb::GcRequest::default()),
            Box::new(kvrpcpb::DeleteRangeRequest::default()),
            Box::new(kvrpcpb::RawGetRequest::default()),
            Box::new(kvrpcpb::RawBatchGetRequest::default()),
            Box::new(kvrpcpb::RawPutRequest::default()),
            Box::new(kvrpcpb::RawBatchPutRequest::default()),
            Box::new(kvrpcpb::RawDeleteRequest::default()),
            Box::new(kvrpcpb::RawBatchDeleteRequest::default()),
            Box::new(kvrpcpb::RawDeleteRangeRequest::default()),
            Box::new(kvrpcpb::RawScanRequest::default()),
            Box::new(kvrpcpb::UnsafeDestroyRangeRequest::default()),
            Box::new(kvrpcpb::RawGetKeyTtlRequest::default()),
            Box::new(kvrpcpb::RawCasRequest::default()),
            Box::new(kvrpcpb::RawChecksumRequest::default()),
            Box::new(kvrpcpb::RegisterLockObserverRequest::default()),
            Box::new(kvrpcpb::CheckLockObserverRequest::default()),
            Box::new(kvrpcpb::RemoveLockObserverRequest::default()),
            Box::new(kvrpcpb::PhysicalScanLockRequest::default()),
            Box::new(coprocessor::Request::default()),
            Box::new(mpp::DispatchTaskRequest::default()),
            Box::new(mpp::IsAliveRequest::default()),
            Box::new(MppStreamRequest(
                mpp::EstablishMppConnectionRequest::default(),
            )),
            Box::new(mpp::CancelTaskRequest::default()),
            Box::new(CoprocessorStreamRequest::new(
                coprocessor::Request::default(),
            )),
            Box::new(BatchCoprocessorStreamRequest::new(
                coprocessor::BatchRequest::default(),
            )),
            Box::new(kvrpcpb::MvccGetByKeyRequest::default()),
            Box::new(kvrpcpb::MvccGetByStartTsRequest::default()),
            Box::new(kvrpcpb::SplitRegionRequest::default()),
            Box::new(tikvpb::BatchCommandsEmptyRequest::default()),
            Box::new(kvrpcpb::CheckTxnStatusRequest::default()),
            Box::new(kvrpcpb::CheckSecondaryLocksRequest::default()),
            Box::new(kvrpcpb::TxnHeartBeatRequest::default()),
            Box::new(kvrpcpb::StoreSafeTsRequest::default()),
            Box::new(kvrpcpb::GetLockWaitInfoRequest::default()),
            Box::new(kvrpcpb::CompactRequest::default()),
            Box::new(kvrpcpb::FlashbackToVersionRequest::default()),
            Box::new(kvrpcpb::PrepareFlashbackToVersionRequest::default()),
            Box::new(kvrpcpb::TiFlashSystemTableRequest::default()),
            Box::new(kvrpcpb::FlushRequest::default()),
            Box::new(kvrpcpb::BufferBatchGetRequest::default()),
            Box::new(kvrpcpb::GetHealthFeedbackRequest::default()),
            Box::new(kvrpcpb::BroadcastTxnStatusRequest::default()),
            Box::new(debugpb::GetRegionPropertiesRequest::default()),
        ];

        assert_eq!(requests.len(), 54);
        assert!(requests.iter().all(|request| !request.label().is_empty()));
    }

    fn assert_context_metadata(request: &mut dyn Request) {
        request.set_api_version(kvrpcpb::ApiVersion::V2);
        request.set_keyspace_id(Some(42));
        request.set_keyspace_name(Some("tenant"));
        request.set_priority(kvrpcpb::CommandPri::High);
        request.set_max_execution_duration_ms(99);
    }

    #[test]
    fn source_context_bearing_unary_requests_retain_full_context_metadata() {
        let mut register = kvrpcpb::RegisterLockObserverRequest::default();
        let mut remove = kvrpcpb::RemoveLockObserverRequest::default();

        for request in [&mut register as &mut dyn Request, &mut remove] {
            assert_context_metadata(request);
            let context = request
                .as_any()
                .downcast_ref::<kvrpcpb::RegisterLockObserverRequest>()
                .map(|request| request.context.as_ref())
                .or_else(|| {
                    request
                        .as_any()
                        .downcast_ref::<kvrpcpb::RemoveLockObserverRequest>()
                        .map(|request| request.context.as_ref())
                })
                .flatten()
                .expect("source context-bearing RPC");
            assert_eq!(context.api_version, kvrpcpb::ApiVersion::V2 as i32);
            assert_eq!(crate::request::context_keyspace_id(context), Some(42));
            assert_eq!(context.keyspace_name, "tenant");
            assert_eq!(context.priority, kvrpcpb::CommandPri::High as i32);
            assert_eq!(context.max_execution_duration_ms, 99);
        }
    }

    fn source_generated_context_requests() -> Vec<Box<dyn Request>> {
        vec![
            Box::new(kvrpcpb::GetRequest::default()),
            Box::new(kvrpcpb::ScanRequest::default()),
            Box::new(kvrpcpb::PrewriteRequest::default()),
            Box::new(kvrpcpb::PessimisticLockRequest::default()),
            Box::new(kvrpcpb::PessimisticRollbackRequest::default()),
            Box::new(kvrpcpb::CommitRequest::default()),
            Box::new(kvrpcpb::CleanupRequest::default()),
            Box::new(kvrpcpb::BatchGetRequest::default()),
            Box::new(kvrpcpb::BatchRollbackRequest::default()),
            Box::new(kvrpcpb::ScanLockRequest::default()),
            Box::new(kvrpcpb::ResolveLockRequest::default()),
            Box::new(kvrpcpb::GcRequest::default()),
            Box::new(kvrpcpb::DeleteRangeRequest::default()),
            Box::new(kvrpcpb::RawGetRequest::default()),
            Box::new(kvrpcpb::RawBatchGetRequest::default()),
            Box::new(kvrpcpb::RawPutRequest::default()),
            Box::new(kvrpcpb::RawBatchPutRequest::default()),
            Box::new(kvrpcpb::RawDeleteRequest::default()),
            Box::new(kvrpcpb::RawBatchDeleteRequest::default()),
            Box::new(kvrpcpb::RawDeleteRangeRequest::default()),
            Box::new(kvrpcpb::RawScanRequest::default()),
            Box::new(kvrpcpb::RawGetKeyTtlRequest::default()),
            Box::new(kvrpcpb::RawCasRequest::default()),
            Box::new(kvrpcpb::RawChecksumRequest::default()),
            Box::new(kvrpcpb::UnsafeDestroyRangeRequest::default()),
            Box::new(kvrpcpb::RegisterLockObserverRequest::default()),
            Box::new(kvrpcpb::CheckLockObserverRequest::default()),
            Box::new(kvrpcpb::RemoveLockObserverRequest::default()),
            Box::new(kvrpcpb::PhysicalScanLockRequest::default()),
            Box::new(kvrpcpb::GetLockWaitInfoRequest::default()),
            Box::new(coprocessor::Request::default()),
            Box::new(BatchCoprocessorStreamRequest::new(
                coprocessor::BatchRequest::default(),
            )),
            Box::new(kvrpcpb::MvccGetByKeyRequest::default()),
            Box::new(kvrpcpb::MvccGetByStartTsRequest::default()),
            Box::new(kvrpcpb::SplitRegionRequest::default()),
            Box::new(kvrpcpb::TxnHeartBeatRequest::default()),
            Box::new(kvrpcpb::CheckTxnStatusRequest::default()),
            Box::new(kvrpcpb::CheckSecondaryLocksRequest::default()),
            Box::new(kvrpcpb::FlashbackToVersionRequest::default()),
            Box::new(kvrpcpb::PrepareFlashbackToVersionRequest::default()),
            Box::new(kvrpcpb::FlushRequest::default()),
            Box::new(kvrpcpb::BufferBatchGetRequest::default()),
        ]
    }

    #[test]
    fn source_generated_attach_context_matrix_is_complete() {
        let mut requests = source_generated_context_requests();
        assert_eq!(requests.len(), 42, "gen.sh command inventory drifted");
        for (index, request) in requests.iter_mut().enumerate() {
            assert!(request.attach_context(kvrpcpb::Context {
                region_id: index as u64 + 1,
                ..Default::default()
            }));
            assert_eq!(
                request.tikv_context().map(|context| context.region_id),
                Some(index as u64 + 1),
                "{} did not retain its attached context",
                request.label()
            );
        }

        let mut cop_stream = CoprocessorStreamRequest::new(coprocessor::Request::default());
        assert!(cop_stream.attach_context(kvrpcpb::Context {
            region_id: 99,
            ..Default::default()
        }));
        assert_eq!(cop_stream.tikv_context().unwrap().region_id, 99);

        let mut accepted_no_ops: Vec<Box<dyn Request>> = vec![
            Box::new(mpp::DispatchTaskRequest::default()),
            Box::new(MppStreamRequest(
                mpp::EstablishMppConnectionRequest::default(),
            )),
            Box::new(mpp::CancelTaskRequest::default()),
            Box::new(mpp::IsAliveRequest::default()),
            Box::new(tikvpb::BatchCommandsEmptyRequest::default()),
        ];
        assert!(accepted_no_ops
            .iter_mut()
            .all(|request| request.attach_context(kvrpcpb::Context::default())));

        let mut rejected: Vec<Box<dyn Request>> = vec![
            Box::new(kvrpcpb::StoreSafeTsRequest::default()),
            Box::new(kvrpcpb::GetHealthFeedbackRequest::default()),
            Box::new(kvrpcpb::BroadcastTxnStatusRequest::default()),
            Box::new(kvrpcpb::CompactRequest::default()),
            Box::new(kvrpcpb::TiFlashSystemTableRequest::default()),
            Box::new(debugpb::GetRegionPropertiesRequest::default()),
        ];
        assert!(rejected
            .iter_mut()
            .all(|request| !request.attach_context(kvrpcpb::Context::default())));
    }

    #[test]
    fn source_default_request_origin_fills_only_unknown_contexts() {
        static ORIGIN_TEST: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = ORIGIN_TEST.lock().unwrap();
        let previous = get_default_request_origin();
        struct ResetOrigin(kvrpcpb::RequestOrigin);
        impl Drop for ResetOrigin {
            fn drop(&mut self) {
                set_default_request_origin(self.0);
            }
        }
        let _reset = ResetOrigin(previous);

        set_default_request_origin(kvrpcpb::RequestOrigin::TiDb);
        for mut request in [
            Box::new(kvrpcpb::GetRequest::default()) as Box<dyn Request>,
            Box::new(kvrpcpb::ScanLockRequest::default()),
            Box::new(kvrpcpb::CleanupRequest::default()),
            Box::new(kvrpcpb::CheckTxnStatusRequest::default()),
            Box::new(kvrpcpb::CheckSecondaryLocksRequest::default()),
        ] {
            assert!(request.attach_context(kvrpcpb::Context::default()));
            assert_eq!(
                request.tikv_context().unwrap().request_origin,
                kvrpcpb::RequestOrigin::TiDb as i32
            );
        }

        let get = kvrpcpb::GetRequest::default();
        let batch = crate::store::BatchCommandRequest::from_store_request(&get)
            .expect("Get is source-batchable")
            .into_proto();
        assert!(matches!(
            batch.cmd,
            Some(tikvpb::batch_commands_request::request::Cmd::Get(request))
                if request.context.as_ref().unwrap().request_origin
                    == kvrpcpb::RequestOrigin::TiDb as i32
        ));

        set_default_request_origin(kvrpcpb::RequestOrigin::Unknown);
        let mut explicit = kvrpcpb::GetRequest::default();
        assert!(explicit.attach_context(kvrpcpb::Context {
            request_origin: kvrpcpb::RequestOrigin::TiDb as i32,
            ..Default::default()
        }));
        assert_eq!(
            explicit.context.unwrap().request_origin,
            kvrpcpb::RequestOrigin::TiDb as i32
        );
    }

    #[test]
    fn source_attach_context_replaces_the_owned_request_snapshot() {
        for mut request in [
            Box::new(kvrpcpb::GetRequest::default()) as Box<dyn Request>,
            Box::new(kvrpcpb::GetLockWaitInfoRequest::default()),
        ] {
            assert!(request.attach_context(kvrpcpb::Context {
                region_id: 123,
                api_version: kvrpcpb::ApiVersion::V2 as i32,
                keyspace_name: "test-keyspace".to_owned(),
                ..Default::default()
            }));
            let old_context = request.tikv_context().unwrap().clone();
            assert!(request.attach_context(kvrpcpb::Context {
                region_id: 789,
                api_version: kvrpcpb::ApiVersion::V2 as i32,
                keyspace_name: "next-test-keyspace".to_owned(),
                ..Default::default()
            }));
            assert_eq!(old_context.region_id, 123);
            assert_eq!(old_context.keyspace_name, "test-keyspace");
            assert_eq!(request.tikv_context().unwrap().region_id, 789);
            assert_eq!(
                request.tikv_context().unwrap().keyspace_name,
                "next-test-keyspace"
            );
        }
    }

    #[test]
    fn source_tidb_51921_batch_snapshots_encode_after_relocation() {
        let mut handles = Vec::new();
        for (index, mut request) in source_generated_context_requests().into_iter().enumerate() {
            let store_id = index as u64 + 1;
            assert!(request.attach_context(kvrpcpb::Context {
                region_id: store_id,
                peer: Some(crate::proto::metapb::Peer {
                    store_id,
                    ..Default::default()
                }),
                ..Default::default()
            }));
            let Some(batch) = crate::store::BatchCommandRequest::from_store_request(&*request)
            else {
                continue;
            };
            assert_eq!(batch.store_id(), store_id);

            assert!(request.attach_context(kvrpcpb::Context {
                region_id: store_id + 1_000,
                peer: Some(crate::proto::metapb::Peer {
                    store_id: store_id + 1_000,
                    ..Default::default()
                }),
                ..Default::default()
            }));
            assert_eq!(batch.store_id(), store_id);
            handles.push(std::thread::spawn(move || {
                batch.into_proto().encode_to_vec()
            }));
        }

        assert_eq!(handles.len(), 29, "source batchable command matrix drifted");
        assert!(handles
            .into_iter()
            .all(|handle| !handle.join().unwrap().is_empty()));
    }

    #[test]
    fn source_get_start_ts_matrix_is_complete() {
        let requests: Vec<(Box<dyn Request>, u64)> = vec![
            (
                Box::new(kvrpcpb::GetRequest {
                    version: 1,
                    ..Default::default()
                }),
                1,
            ),
            (
                Box::new(kvrpcpb::ScanRequest {
                    version: 2,
                    ..Default::default()
                }),
                2,
            ),
            (
                Box::new(kvrpcpb::PrewriteRequest {
                    start_version: 3,
                    ..Default::default()
                }),
                3,
            ),
            (
                Box::new(kvrpcpb::CommitRequest {
                    start_version: 4,
                    ..Default::default()
                }),
                4,
            ),
            (
                Box::new(kvrpcpb::CleanupRequest {
                    start_version: 5,
                    ..Default::default()
                }),
                5,
            ),
            (
                Box::new(kvrpcpb::BatchGetRequest {
                    version: 6,
                    ..Default::default()
                }),
                6,
            ),
            (
                Box::new(kvrpcpb::BatchRollbackRequest {
                    start_version: 7,
                    ..Default::default()
                }),
                7,
            ),
            (
                Box::new(kvrpcpb::ScanLockRequest {
                    max_version: 8,
                    ..Default::default()
                }),
                8,
            ),
            (
                Box::new(kvrpcpb::ResolveLockRequest {
                    start_version: 9,
                    ..Default::default()
                }),
                9,
            ),
            (
                Box::new(kvrpcpb::PessimisticLockRequest {
                    start_version: 10,
                    ..Default::default()
                }),
                10,
            ),
            (
                Box::new(kvrpcpb::PessimisticRollbackRequest {
                    start_version: 11,
                    ..Default::default()
                }),
                11,
            ),
            (
                Box::new(kvrpcpb::TxnHeartBeatRequest {
                    start_version: 12,
                    ..Default::default()
                }),
                12,
            ),
            (
                Box::new(kvrpcpb::CheckTxnStatusRequest {
                    lock_ts: 13,
                    ..Default::default()
                }),
                13,
            ),
            (
                Box::new(kvrpcpb::CheckSecondaryLocksRequest {
                    start_version: 14,
                    ..Default::default()
                }),
                14,
            ),
            (
                Box::new(kvrpcpb::FlashbackToVersionRequest {
                    start_ts: 15,
                    ..Default::default()
                }),
                15,
            ),
            (
                Box::new(kvrpcpb::PrepareFlashbackToVersionRequest {
                    start_ts: 16,
                    ..Default::default()
                }),
                16,
            ),
            (
                Box::new(kvrpcpb::FlushRequest {
                    start_ts: 17,
                    ..Default::default()
                }),
                17,
            ),
            (
                Box::new(kvrpcpb::BufferBatchGetRequest {
                    version: 18,
                    ..Default::default()
                }),
                18,
            ),
            (
                Box::new(coprocessor::Request {
                    start_ts: 19,
                    ..Default::default()
                }),
                19,
            ),
            (
                Box::new(CoprocessorStreamRequest::new(coprocessor::Request {
                    start_ts: 20,
                    ..Default::default()
                })),
                20,
            ),
            (
                Box::new(BatchCoprocessorStreamRequest::new(
                    coprocessor::BatchRequest {
                        start_ts: 21,
                        ..Default::default()
                    },
                )),
                21,
            ),
            (
                Box::new(kvrpcpb::MvccGetByStartTsRequest {
                    start_ts: 22,
                    ..Default::default()
                }),
                22,
            ),
        ];
        assert_eq!(requests.len(), 22);
        for (request, expected) in requests {
            assert_eq!(request.start_timestamp(), expected, "{}", request.label());
        }
        assert_eq!(kvrpcpb::RawGetRequest::default().start_timestamp(), 0);
    }

    #[test]
    fn source_response_ext_and_resource_group_tagger_are_typed() {
        let mut request = kvrpcpb::GetRequest::default();
        let tagger: ResourceGroupTagger = Arc::new(|request| {
            request.set_resource_group_tag(b"resource-tag".to_vec());
        });
        tagger(&mut request);
        assert_eq!(request.context.unwrap().resource_group_tag, b"resource-tag");

        let response = ResponseExt {
            response: kvrpcpb::GetResponse::default(),
            address: "logical-target:20160".to_owned(),
        };
        assert_eq!(response.address, "logical-target:20160");
    }

    #[test]
    fn source_replica_read_flag_is_carried_only_by_context_requests() {
        let mut get = kvrpcpb::GetRequest::default();
        get.set_replica_read(true);
        get.set_stale_read(true);
        get.set_busy_threshold_ms(123);
        get.set_buckets_version(456);
        let context = get.context.unwrap();
        assert!(context.replica_read);
        assert!(context.stale_read);
        assert_eq!(context.busy_threshold_ms, 123);
        assert_eq!(context.buckets_version, 456);

        let mut store_safe_ts = kvrpcpb::StoreSafeTsRequest::default();
        store_safe_ts.set_replica_read(true);
        store_safe_ts.set_busy_threshold_ms(123);
        assert!(store_safe_ts.key_range.is_none());
    }

    #[test]
    fn source_snapshot_lock_hints_are_carried_by_context_requests() {
        let mut get = kvrpcpb::GetRequest::default();
        get.set_resolved_locks(vec![3, 1]);
        get.set_committed_locks(vec![2]);

        let context = get.context.unwrap();
        assert_eq!(context.resolved_locks, [3, 1]);
        assert_eq!(context.committed_locks, [2]);
    }

    #[test]
    fn store_level_unary_requests_do_not_synthesize_region_context() {
        let mut cancel = mpp::CancelTaskRequest::default();
        let mut alive = mpp::IsAliveRequest::default();
        let mut system_table = kvrpcpb::TiFlashSystemTableRequest::default();

        for request in [
            &mut cancel as &mut dyn Request,
            &mut alive,
            &mut system_table,
        ] {
            request.set_api_version(kvrpcpb::ApiVersion::V2);
            request.set_keyspace_id(Some(42));
            assert!(request.set_leader(&RegionWithLeader::default()).is_ok());
        }

        assert_eq!(cancel.label(), "cancel_mpp_task");
        assert_eq!(alive.label(), "is_alive");
        assert_eq!(system_table.label(), "get_tiflash_system_table");
    }

    #[test]
    fn compact_carries_source_api_metadata_without_region_context() {
        let mut compact = kvrpcpb::CompactRequest::default();
        compact.set_api_version(kvrpcpb::ApiVersion::V2);
        compact.set_keyspace_id(Some(42));
        assert!(compact.set_leader(&RegionWithLeader::default()).is_ok());

        assert_eq!(compact.api_version, kvrpcpb::ApiVersion::V2 as i32);
        assert_eq!(
            compact.keyspace,
            Some(kvrpcpb::compact_request::Keyspace::KeyspaceId(42))
        );
        assert_eq!(compact.label(), "compact");
    }

    #[test]
    fn source_stream_wrappers_apply_the_v2_encode_and_decode_matrix() {
        let codec = ApiV2Codec::new(crate::request::KeyMode::Txn, 7).unwrap();
        let range = coprocessor::KeyRange {
            start: b"a".to_vec(),
            end: b"z".to_vec(),
        };
        let cop = CoprocessorStreamRequest::new(coprocessor::Request {
            ranges: vec![range.clone()],
            ..Default::default()
        })
        .with_api_v2_codec(codec);
        let encoded = cop.wire_request();
        assert_eq!(encoded.ranges[0].start, b"x\0\0\x07a");
        assert_eq!(encoded.ranges[0].end, b"x\0\0\x07z");
        let context = encoded.context.unwrap();
        assert_eq!(context.api_version, kvrpcpb::ApiVersion::V2 as i32);
        assert_eq!(crate::request::context_keyspace_id(&context), Some(7));
        assert!(matches!(
            cop.decode_transport_response(&mut ()),
            Err(Error::StringError(message)) if message == "streaming coprocessor is not supported yet"
        ));

        let batch = BatchCoprocessorStreamRequest::new(coprocessor::BatchRequest {
            regions: vec![coprocessor::RegionInfo {
                ranges: vec![range],
                ..Default::default()
            }],
            ..Default::default()
        })
        .with_api_v2_codec(codec);
        let encoded = batch.wire_request();
        assert_eq!(encoded.regions[0].ranges[0].start, b"x\0\0\x07a");
        assert_eq!(encoded.regions[0].ranges[0].end, b"x\0\0\x07z");
        let context = encoded.context.unwrap();
        assert_eq!(context.api_version, kvrpcpb::ApiVersion::V2 as i32);
        assert_eq!(crate::request::context_keyspace_id(&context), Some(7));
        assert!(batch.decode_transport_response(&mut ()).is_ok());
    }

    #[test]
    fn source_cop_stream_ru_v2_counts_only_the_first_received_rpc() {
        let details = Arc::new(crate::RuDetails::new());
        let mut stream = CoprocessorStreamResponse {
            first: None,
            stream: None,
            timeout: Duration::from_secs(1),
            ru_details: Some(details.clone()),
            count_read_rpc: true,
            bypass_ru_v2: false,
        };
        let mut first = coprocessor::Response {
            exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                ru_v2: Some(kvrpcpb::Ruv2 {
                    storage_processed_keys_get: 2,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut second = coprocessor::Response {
            exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                ru_v2: Some(kvrpcpb::Ruv2 {
                    storage_processed_keys_get: 3,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        stream.update_ru_v2(&mut first);
        stream.update_ru_v2(&mut second);

        assert_eq!(
            first.exec_details_v2.unwrap().ru_v2.unwrap().read_rpc_count,
            1
        );
        assert_eq!(
            second
                .exec_details_v2
                .unwrap()
                .ru_v2
                .unwrap()
                .read_rpc_count,
            0
        );
        let accumulated = details.drain_ru_v2().unwrap();
        assert_eq!(accumulated.read_rpc_count, 1);
        assert_eq!(accumulated.storage_processed_keys_get, 5);

        let bypass_details = Arc::new(crate::RuDetails::new());
        let mut bypass_stream = CoprocessorStreamResponse {
            first: None,
            stream: None,
            timeout: Duration::from_secs(1),
            ru_details: Some(bypass_details.clone()),
            count_read_rpc: true,
            bypass_ru_v2: true,
        };
        let mut bypass_response = coprocessor::Response {
            exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                ru_v2: Some(kvrpcpb::Ruv2 {
                    storage_processed_keys_get: 2,
                    storage_processed_keys_batch_get: 3,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        bypass_stream.update_ru_v2(&mut bypass_response);
        assert!(!bypass_stream.count_read_rpc);
        let bypass_ru = bypass_response.exec_details_v2.unwrap().ru_v2.unwrap();
        assert_eq!(bypass_ru.read_rpc_count, 0);
        assert_eq!(bypass_ru.storage_processed_keys_get, 2);
        assert_eq!(bypass_ru.storage_processed_keys_batch_get, 3);
        assert!(bypass_details.drain_ru_v2().is_none());
    }
}
