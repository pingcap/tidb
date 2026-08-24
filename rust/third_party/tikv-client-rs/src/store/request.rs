// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::time::Duration;

use async_trait::async_trait;
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::IntoRequest;

use crate::proto::coprocessor;
use crate::proto::kvrpcpb;
use crate::proto::mpp;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::store::RegionWithLeader;
use crate::Error;
use crate::Result;

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

    /// Source `tikvrpc.Request.GetSize()` for resource-control accounting.
    /// Concrete protobuf requests provide their encoded size; non-protobuf
    /// test or stream wrappers retain zero until they model a wire request.
    fn encoded_request_size(&self) -> u64 {
        0
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

fn with_forwarded_host<T>(
    mut request: tonic::Request<T>,
    forwarded_host: &str,
) -> Result<tonic::Request<T>> {
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
        #[async_trait]
        impl Request for kvrpcpb::$name {
            async fn dispatch(
                &self,
                client: &TikvClient<Channel>,
                timeout: Duration,
            ) -> Result<Box<dyn Any>> {
                let mut req = self.clone().into_request();
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
                let mut req = with_forwarded_host(self.clone().into_request(), forwarded_host)?;
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
                    ctx.keyspace_id = keyspace_id;
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
    ($ty:path, $fun:ident, $label:literal) => {
        #[async_trait]
        impl Request for $ty {
            async fn dispatch(
                &self,
                client: &TikvClient<Channel>,
                timeout: Duration,
            ) -> Result<Box<dyn Any>> {
                let mut request = self.clone().into_request();
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
    "get_health_feedback"
);
impl_request!(
    BroadcastTxnStatusRequest,
    broadcast_txn_status,
    "broadcast_txn_status"
);

impl_store_request!(mpp::CancelTaskRequest, cancel_mpp_task, "cancel_mpp_task");
impl_store_request!(mpp::IsAliveRequest, is_alive, "is_alive");
impl_store_request!(
    kvrpcpb::TiFlashSystemTableRequest,
    get_ti_flash_system_table,
    "get_tiflash_system_table"
);

#[async_trait]
impl Request for kvrpcpb::CompactRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        let mut request = self.clone().into_request();
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
            self.keyspace_id = keyspace_id;
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
        let mut request = self.clone().into_request();
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
        let mut request = self.clone().into_request();
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
        let mut request = with_forwarded_host(self.clone().into_request(), forwarded_host)?;
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
            self.context
                .get_or_insert(kvrpcpb::Context::default())
                .keyspace_id = keyspace_id;
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

    fn encoded_request_size(&self) -> u64 {
        self.encoded_len() as u64
    }
}

#[async_trait]
impl Request for mpp::DispatchTaskRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        let mut request = self.clone().into_request();
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

    fn set_keyspace_id(&mut self, keyspace_id: Option<u32>) {
        if let (Some(meta), Some(keyspace_id)) = (&mut self.meta, keyspace_id) {
            meta.keyspace_id = keyspace_id;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let mut health = kvrpcpb::GetHealthFeedbackRequest::default();
        let mut broadcast = kvrpcpb::BroadcastTxnStatusRequest::default();

        for request in [
            &mut register as &mut dyn Request,
            &mut remove,
            &mut health,
            &mut broadcast,
        ] {
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
                .or_else(|| {
                    request
                        .as_any()
                        .downcast_ref::<kvrpcpb::GetHealthFeedbackRequest>()
                        .map(|request| request.context.as_ref())
                })
                .or_else(|| {
                    request
                        .as_any()
                        .downcast_ref::<kvrpcpb::BroadcastTxnStatusRequest>()
                        .map(|request| request.context.as_ref())
                })
                .flatten()
                .expect("source context-bearing RPC");
            assert_eq!(context.api_version, kvrpcpb::ApiVersion::V2 as i32);
            assert_eq!(context.keyspace_id, 42);
            assert_eq!(context.keyspace_name, "tenant");
            assert_eq!(context.priority, kvrpcpb::CommandPri::High as i32);
            assert_eq!(context.max_execution_duration_ms, 99);
        }
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
        assert_eq!(compact.keyspace_id, 42);
        assert_eq!(compact.label(), "compact");
    }
}
