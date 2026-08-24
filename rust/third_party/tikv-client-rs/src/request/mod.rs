// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use derive_new::new;

#[cfg(test)]
pub(crate) use self::keyspace::context_keyspace_id;
pub use self::keyspace::EncodeKeyspace;
pub use self::keyspace::KeyMode;
pub use self::keyspace::Keyspace;
pub use self::keyspace::TruncateKeyspace;
pub use self::keyspace::{
    api_v1_excluded_prefixes, api_v2_prefixes, build_keyspace_name, decode_api_key,
    is_decode_error, parse_keyspace_id, ApiV1Codec, ApiV2Codec, DEFAULT_KEYSPACE_ID,
    DEFAULT_KEYSPACE_NAME, MAX_KEYSPACE_ID, NULL_KEYSPACE_ID,
};
pub(crate) use self::keyspace::{
    keyspace_from_pd_meta, keyspace_id_from_pd_meta, set_context_keyspace_id,
};
pub use self::plan::Collect;
pub use self::plan::CollectError;
pub use self::plan::CollectSingle;
pub use self::plan::CollectWithShard;
pub use self::plan::DefaultProcessor;
pub use self::plan::Dispatch;
pub use self::plan::ExtractError;
pub use self::plan::Merge;
pub use self::plan::MergeResponse;
pub use self::plan::Plan;
pub use self::plan::Process;
pub use self::plan::ProcessResponse;
pub use self::plan::ResolveLock;
pub use self::plan::ResponseWithShard;
pub use self::plan::RetryableMultiRegion;
pub(crate) use self::plan_builder::NoTarget;
pub use self::plan_builder::PlanBuilder;
pub use self::plan_builder::SingleKey;
pub(crate) use self::shard::key_batches;
pub use self::shard::Batchable;
pub use self::shard::HasNextBatch;
pub use self::shard::NextBatch;
pub use self::shard::RangeRequest;
pub use self::shard::Shardable;
use crate::backoff::Backoff;
use crate::backoff::DEFAULT_REGION_BACKOFF;
use crate::backoff::OPTIMISTIC_BACKOFF;
use crate::backoff::PESSIMISTIC_BACKOFF;
use crate::store::Request;
use crate::store::{HasKeyErrors, Store};
use crate::transaction::HasLocks;

mod keyspace;
pub mod plan;
mod plan_builder;
mod shard;

/// Abstracts any request sent to a TiKV server.
#[async_trait]
pub trait KvRequest: Request + Sized + Clone + Sync + Send + 'static {
    /// The expected response to the request.
    type Response: HasKeyErrors + HasLocks + Clone + Send + 'static;

    /// Source `isReadReq` command classification used by load-based replica
    /// routing. Raw commands deliberately remain false: client-go only
    /// classifies transactional Get/BatchGet/Scan and coprocessor commands
    /// here.
    fn is_read_request(&self) -> bool {
        matches!(
            self.label(),
            "kv_get" | "kv_batch_get" | "kv_scan" | "coprocessor"
        )
    }

    /// Source `CmdCop` requests carrying per-store tasks must not retry to a
    /// replica after `ServerIsBusy`, because those tasks already belong to
    /// concrete regions. Ordinary coprocessor requests retain the normal
    /// read classification above.
    fn is_batched_coprocessor_read(&self) -> bool {
        false
    }

    /// Decode transport-level response fields before plans inspect retry, lock, or
    /// user-visible key data. API V2 implementations use this to mirror
    /// client-go's `Codec.DecodeResponse` placement.
    fn key_mode(&self) -> Option<KeyMode> {
        None
    }

    fn decode_response(
        &self,
        _response: &mut Self::Response,
        _codec: Option<&ApiV2Codec>,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn decode_v1_response(
        &self,
        _response: &mut Self::Response,
        _codec: Option<&ApiV1Codec>,
    ) -> crate::Result<()> {
        Ok(())
    }
}

/// For requests or plans which are handled at TiKV store (other than region) level.
pub trait StoreRequest {
    /// Apply the request to specified TiKV store.
    fn apply_store(&mut self, store: &Store);
}

#[derive(Clone, Debug, new, Eq, PartialEq)]
pub struct RetryOptions {
    /// How to retry when there is a region error and we need to resolve regions with PD.
    pub region_backoff: Backoff,
    /// How to retry when a key is locked.
    pub lock_backoff: Backoff,
}

impl RetryOptions {
    pub const fn default_optimistic() -> RetryOptions {
        RetryOptions {
            region_backoff: DEFAULT_REGION_BACKOFF,
            lock_backoff: OPTIMISTIC_BACKOFF,
        }
    }

    pub const fn default_pessimistic() -> RetryOptions {
        RetryOptions {
            region_backoff: DEFAULT_REGION_BACKOFF,
            lock_backoff: PESSIMISTIC_BACKOFF,
        }
    }

    pub const fn none() -> RetryOptions {
        RetryOptions {
            region_backoff: Backoff::no_backoff(),
            lock_backoff: Backoff::no_backoff(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::any::Any;
    use std::iter;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use tonic::transport::Channel;

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::coprocessor;
    use crate::proto::keyspacepb;
    use crate::proto::kvrpcpb;
    use crate::proto::metapb::{self, RegionEpoch};
    use crate::proto::pdpb::Timestamp;
    use crate::proto::tikvpb::tikv_client::TikvClient;
    use crate::region::{RegionId, RegionVerId, RegionWithLeader, StoreId};
    use crate::store::region_stream_for_keys;
    use crate::store::HasRegionError;
    use crate::store::{RegionStore, Store};
    use crate::transaction::lowering::new_commit_request;
    use crate::Error;
    use crate::Key;
    use crate::Result;

    #[test]
    fn source_load_based_replica_routing_uses_only_read_commands() {
        assert!(KvRequest::is_read_request(&kvrpcpb::GetRequest::default()));
        assert!(KvRequest::is_read_request(
            &kvrpcpb::BatchGetRequest::default()
        ));
        assert!(KvRequest::is_read_request(&kvrpcpb::ScanRequest::default()));
        assert!(KvRequest::is_read_request(&coprocessor::Request::default()));
        assert!(!KvRequest::is_read_request(
            &kvrpcpb::PrewriteRequest::default()
        ));
        assert!(!KvRequest::is_read_request(
            &kvrpcpb::RawGetRequest::default()
        ));

        let mut batched_cop = coprocessor::Request::default();
        batched_cop.tasks.push(Default::default());
        assert!(KvRequest::is_batched_coprocessor_read(&batched_cop));
        assert!(!KvRequest::is_batched_coprocessor_read(
            &coprocessor::Request::default()
        ));
    }

    #[tokio::test]
    async fn source_region_error_selector_retries_reuse_the_existing_shard() {
        #[derive(Debug, Clone)]
        struct MockRpcResponse {
            region_error: Option<crate::proto::errorpb::Error>,
        }

        impl HasKeyErrors for MockRpcResponse {
            fn key_errors(&mut self) -> Option<Vec<Error>> {
                None
            }
        }

        impl HasRegionError for MockRpcResponse {
            fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
                self.region_error.clone()
            }
        }

        impl HasLocks for MockRpcResponse {}

        #[derive(Clone)]
        struct MockKvRequest {
            test_invoking_count: Arc<AtomicUsize>,
            is_retry_request: bool,
        }

        #[async_trait]
        impl Request for MockKvRequest {
            async fn dispatch(&self, _: &TikvClient<Channel>, _: Duration) -> Result<Box<dyn Any>> {
                Ok(Box::new(MockRpcResponse { region_error: None }))
            }

            fn label(&self) -> &'static str {
                "kv_get"
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn set_leader(&mut self, _: &RegionWithLeader) -> Result<()> {
                Ok(())
            }

            fn set_api_version(&mut self, _: kvrpcpb::ApiVersion) {}

            fn set_is_retry_request(&mut self) {
                self.is_retry_request = true;
            }
        }

        #[async_trait]
        impl KvRequest for MockKvRequest {
            type Response = MockRpcResponse;
        }

        impl Shardable for MockKvRequest {
            type Shard = Vec<Vec<u8>>;

            fn shards(
                &self,
                pd_client: &std::sync::Arc<impl crate::pd::PdClient>,
            ) -> futures::stream::BoxStream<
                'static,
                crate::Result<(Self::Shard, crate::region::RegionWithLeader)>,
            > {
                // Increases by 1 for each call.
                self.test_invoking_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                region_stream_for_keys(
                    Some(Key::from("mock_key".to_owned())).into_iter(),
                    pd_client.clone(),
                )
            }

            fn apply_shard(&mut self, _shard: Self::Shard) {}

            fn apply_store(&mut self, _store: &crate::store::RegionStore) -> crate::Result<()> {
                Ok(())
            }
        }

        let invoking_count = Arc::new(AtomicUsize::new(0));

        let request = MockKvRequest {
            test_invoking_count: invoking_count.clone(),
            is_retry_request: false,
        };

        let rpc_invoking_count = Arc::new(AtomicUsize::new(0));
        let observed_rpc_invoking_count = rpc_invoking_count.clone();
        let retry_flags = Arc::new(Mutex::new(Vec::new()));
        let observed_retry_flags = retry_flags.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request.downcast_ref::<MockKvRequest>().unwrap();
                observed_retry_flags
                    .lock()
                    .unwrap()
                    .push(request.is_retry_request);
                Ok(Box::new(MockRpcResponse {
                    region_error: (observed_rpc_invoking_count.fetch_add(1, Ordering::SeqCst) == 0)
                        .then(crate::proto::errorpb::Error::default),
                }) as Box<dyn Any>)
            },
        )));

        let runtime_stats = Arc::new(crate::RegionRequestRuntimeStats::new());
        let region_error_before = crate::stats::region_error_count("unknown", Some(42));
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, request)
            .region_request_runtime_stats(Some(runtime_stats.clone()))
            .retry_multi_region(Backoff::no_jitter_backoff(1, 1, 3))
            .extract_error()
            .plan();
        assert!(plan.execute().await.is_ok());

        // Client-go creates a replica selector for every TiKV request. Its
        // unknown-error fallback immediately selects again: it neither
        // re-shards nor consumes a region backoff budget.
        assert_eq!(invoking_count.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(rpc_invoking_count.load(Ordering::SeqCst), 2);
        assert_eq!(*retry_flags.lock().unwrap(), vec![false, true]);
        assert_eq!(runtime_stats.command_rpc_count(crate::CommandType::Get), 2);
        assert_eq!(runtime_stats.error_stats().error_count("unknown"), 1);
        let accesses = runtime_stats.replica_access_stats().access_infos();
        assert_eq!(accesses.len(), 1);
        assert_eq!(accesses[0].store_id, 42);
        let region_error_after = crate::stats::region_error_count("unknown", Some(42));
        assert!(
            region_error_after >= region_error_before + 1,
            "before={region_error_before}, after={region_error_after}, accesses={accesses:?}"
        );

        let busy_shard_count = Arc::new(AtomicUsize::new(0));
        let busy_rpc_count = Arc::new(AtomicUsize::new(0));
        let observed_busy_rpc_count = busy_rpc_count.clone();
        let busy_pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |_: &dyn Any| {
                Ok(Box::new(MockRpcResponse {
                    region_error: (observed_busy_rpc_count.fetch_add(1, Ordering::SeqCst) == 0)
                        .then(|| crate::proto::errorpb::Error {
                            server_is_busy: Some(Default::default()),
                            ..Default::default()
                        }),
                }) as Box<dyn Any>)
            },
        )));
        let busy_plan = crate::request::PlanBuilder::new(
            busy_pd_client,
            Keyspace::Disable,
            MockKvRequest {
                test_invoking_count: busy_shard_count.clone(),
                is_retry_request: false,
            },
        )
        .retry_multi_region(Backoff::no_jitter_backoff(1, 1, 3))
        .extract_error()
        .plan();
        assert!(busy_plan.execute().await.is_ok());
        assert_eq!(busy_shard_count.load(Ordering::SeqCst), 1);
        assert_eq!(busy_rpc_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_region_store_mapping_retry() {
        #[derive(Debug, Clone)]
        struct MockOkResponse;

        impl HasKeyErrors for MockOkResponse {
            fn key_errors(&mut self) -> Option<Vec<Error>> {
                None
            }
        }

        impl HasRegionError for MockOkResponse {
            fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
                None
            }
        }

        impl HasLocks for MockOkResponse {}

        struct FlakyStoreMappingPdClient {
            client: MockKvClient,
            invalidated: AtomicBool,
            invalidation_count: AtomicUsize,
        }

        impl FlakyStoreMappingPdClient {
            fn region(store_id: StoreId) -> RegionWithLeader {
                let mut region = RegionWithLeader::default();
                region.region.id = 1;
                region.region.start_key = vec![];
                region.region.end_key = vec![];
                region.region.region_epoch = Some(RegionEpoch {
                    conf_ver: 0,
                    version: 0,
                });
                region.leader = Some(metapb::Peer {
                    store_id,
                    ..Default::default()
                });
                region
            }
        }

        #[async_trait]
        impl crate::pd::PdClient for FlakyStoreMappingPdClient {
            type KvClient = MockKvClient;

            async fn map_region_to_store(
                self: Arc<Self>,
                region: RegionWithLeader,
            ) -> Result<RegionStore> {
                match region.get_store_id()? {
                    41 => Err(Error::InternalError {
                        message: "invalid store ID 41, not found".to_owned(),
                    }),
                    _ => Ok(RegionStore::new(region, Arc::new(self.client.clone()))),
                }
            }

            async fn region_for_key(&self, _: &Key) -> Result<RegionWithLeader> {
                let store_id = if self.invalidated.load(Ordering::SeqCst) {
                    42
                } else {
                    41
                };
                Ok(Self::region(store_id))
            }

            async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader> {
                match id {
                    1 => self.region_for_key(&Key::EMPTY).await,
                    _ => Err(Error::RegionNotFoundInResponse { region_id: id }),
                }
            }

            async fn all_stores(&self) -> Result<Vec<Store>> {
                Ok(vec![Store::new(Arc::new(self.client.clone()))])
            }

            async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
                Ok(Timestamp::default())
            }

            async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
                unimplemented!()
            }

            async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
                unimplemented!()
            }

            async fn update_leader(
                &self,
                _ver_id: RegionVerId,
                _leader: metapb::Peer,
            ) -> Result<()> {
                Ok(())
            }

            async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {
                self.invalidated.store(true, Ordering::SeqCst);
                self.invalidation_count.fetch_add(1, Ordering::SeqCst);
            }

            async fn invalidate_store_cache(&self, _store_id: StoreId) {}
        }

        #[derive(Clone)]
        struct MockKvRequest {
            shard_invoking_count: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl Request for MockKvRequest {
            async fn dispatch(&self, _: &TikvClient<Channel>, _: Duration) -> Result<Box<dyn Any>> {
                Ok(Box::new(MockOkResponse))
            }

            fn label(&self) -> &'static str {
                "mock"
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn set_leader(&mut self, _: &RegionWithLeader) -> Result<()> {
                Ok(())
            }

            fn set_api_version(&mut self, _: kvrpcpb::ApiVersion) {}
        }

        #[async_trait]
        impl KvRequest for MockKvRequest {
            type Response = MockOkResponse;
        }

        impl Shardable for MockKvRequest {
            type Shard = Vec<Vec<u8>>;

            fn shards(
                &self,
                pd_client: &Arc<impl crate::pd::PdClient>,
            ) -> futures::stream::BoxStream<
                'static,
                crate::Result<(Self::Shard, crate::region::RegionWithLeader)>,
            > {
                self.shard_invoking_count.fetch_add(1, Ordering::SeqCst);
                region_stream_for_keys(
                    Some(Key::from("mock_key".to_owned())).into_iter(),
                    pd_client.clone(),
                )
            }

            fn apply_shard(&mut self, _shard: Self::Shard) {}

            fn apply_store(&mut self, _store: &crate::store::RegionStore) -> crate::Result<()> {
                Ok(())
            }
        }

        let dispatch_count = Arc::new(AtomicUsize::new(0));
        let shard_invoking_count = Arc::new(AtomicUsize::new(0));
        let dispatch_count_clone = dispatch_count.clone();

        let pd_client = Arc::new(FlakyStoreMappingPdClient {
            client: MockKvClient::with_dispatch_hook(move |_: &dyn Any| {
                dispatch_count_clone.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(MockOkResponse) as Box<dyn Any>)
            }),
            invalidated: AtomicBool::new(false),
            invalidation_count: AtomicUsize::new(0),
        });

        let request = MockKvRequest {
            shard_invoking_count: shard_invoking_count.clone(),
        };

        let plan = crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, request)
            .retry_multi_region(Backoff::no_jitter_backoff(1, 1, 3))
            .plan();

        let response = plan.execute().await;
        assert!(response.is_ok());
        assert_eq!(dispatch_count.load(Ordering::SeqCst), 1);
        assert_eq!(shard_invoking_count.load(Ordering::SeqCst), 2);
        assert_eq!(pd_client.invalidation_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_extract_error() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_: &dyn Any| {
                Ok(Box::new(kvrpcpb::CommitResponse {
                    error: Some(kvrpcpb::KeyError::default()),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));

        let key: Key = "key".to_owned().into();
        let req = new_commit_request(iter::once(key), Timestamp::default(), Timestamp::default());

        // does not extract error
        let plan =
            crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, req.clone())
                .retry_multi_region(OPTIMISTIC_BACKOFF)
                .plan();
        assert!(plan.execute().await.is_ok());

        // extract error
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, req)
            .retry_multi_region(OPTIMISTIC_BACKOFF)
            .extract_error()
            .plan();
        assert!(plan.execute().await.is_err());
    }

    #[tokio::test]
    async fn test_grpc_error_invalidates_store_cache() {
        #[derive(Debug, Clone)]
        struct MockOkResponse;

        impl HasKeyErrors for MockOkResponse {
            fn key_errors(&mut self) -> Option<Vec<Error>> {
                None
            }
        }

        impl HasRegionError for MockOkResponse {
            fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
                None
            }
        }

        impl HasLocks for MockOkResponse {}

        struct InvalidationTrackingPdClient {
            client: MockKvClient,
            invalidate_region_count: AtomicUsize,
            invalidate_store_count: AtomicUsize,
            region_lookup_count: AtomicUsize,
            preserve_route_on_failure: bool,
        }

        impl InvalidationTrackingPdClient {
            fn region() -> RegionWithLeader {
                let mut region = RegionWithLeader::default();
                region.region.id = 1;
                region.region.start_key = vec![];
                region.region.end_key = vec![];
                region.region.region_epoch = Some(RegionEpoch {
                    conf_ver: 0,
                    version: 0,
                });
                region.leader = Some(metapb::Peer {
                    id: 40,
                    store_id: 41,
                    ..Default::default()
                });
                region
            }
        }

        #[async_trait]
        impl crate::pd::PdClient for InvalidationTrackingPdClient {
            type KvClient = MockKvClient;

            async fn map_region_to_store(
                self: Arc<Self>,
                region: RegionWithLeader,
            ) -> Result<RegionStore> {
                Ok(RegionStore::new(region, Arc::new(self.client.clone())))
            }

            async fn region_for_key(&self, _: &Key) -> Result<RegionWithLeader> {
                self.region_lookup_count.fetch_add(1, Ordering::SeqCst);
                Ok(Self::region())
            }

            async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader> {
                match id {
                    1 => Ok(Self::region()),
                    _ => Err(Error::RegionNotFoundInResponse { region_id: id }),
                }
            }

            async fn all_stores(&self) -> Result<Vec<Store>> {
                Ok(vec![Store::new(Arc::new(self.client.clone()))])
            }

            async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
                Ok(Timestamp::default())
            }

            async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
                unimplemented!()
            }

            async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
                unimplemented!()
            }

            async fn update_leader(
                &self,
                _ver_id: RegionVerId,
                _leader: metapb::Peer,
            ) -> Result<()> {
                Ok(())
            }

            async fn on_send_failure(self: Arc<Self>, _route: Option<&RegionStore>) -> bool {
                !self.preserve_route_on_failure
            }

            async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {
                self.invalidate_region_count.fetch_add(1, Ordering::SeqCst);
            }

            async fn invalidate_store_cache(&self, _store_id: StoreId) {
                self.invalidate_store_count.fetch_add(1, Ordering::SeqCst);
            }
        }

        #[derive(Clone)]
        struct MockKvRequest;

        #[async_trait]
        impl Request for MockKvRequest {
            async fn dispatch(&self, _: &TikvClient<Channel>, _: Duration) -> Result<Box<dyn Any>> {
                Ok(Box::new(MockOkResponse))
            }

            fn label(&self) -> &'static str {
                "kv_get"
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn set_leader(&mut self, _: &RegionWithLeader) -> Result<()> {
                Ok(())
            }

            fn set_api_version(&mut self, _: kvrpcpb::ApiVersion) {}
        }

        #[async_trait]
        impl KvRequest for MockKvRequest {
            type Response = MockOkResponse;
        }

        impl Shardable for MockKvRequest {
            type Shard = Vec<Vec<u8>>;

            fn shards(
                &self,
                pd_client: &Arc<impl crate::pd::PdClient>,
            ) -> futures::stream::BoxStream<
                'static,
                crate::Result<(Self::Shard, crate::region::RegionWithLeader)>,
            > {
                region_stream_for_keys(
                    Some(Key::from("mock_key".to_owned())).into_iter(),
                    pd_client.clone(),
                )
            }

            fn apply_shard(&mut self, _shard: Self::Shard) {}

            fn apply_store(&mut self, _store: &crate::store::RegionStore) -> crate::Result<()> {
                Ok(())
            }
        }

        let fail_first_dispatch = Arc::new(AtomicBool::new(true));
        let pd_client = Arc::new(InvalidationTrackingPdClient {
            client: MockKvClient::with_dispatch_hook(move |_: &dyn Any| {
                if fail_first_dispatch.swap(false, Ordering::SeqCst) {
                    Err(Error::GrpcAPI(tonic::Status::unavailable(
                        "transient failure",
                    )))
                } else {
                    Ok(Box::new(MockOkResponse) as Box<dyn Any>)
                }
            }),
            invalidate_region_count: AtomicUsize::new(0),
            invalidate_store_count: AtomicUsize::new(0),
            region_lookup_count: AtomicUsize::new(0),
            preserve_route_on_failure: false,
        });

        let runtime_stats = Arc::new(crate::RegionRequestRuntimeStats::new());
        let plan =
            crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, MockKvRequest)
                .region_request_runtime_stats(Some(runtime_stats.clone()))
                .retry_multi_region(Backoff::no_jitter_backoff(1, 1, 1))
                .plan();
        let response = plan.execute().await;
        assert!(response.is_ok());
        assert_eq!(pd_client.invalidate_region_count.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidate_store_count.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.region_lookup_count.load(Ordering::SeqCst), 2);
        assert_eq!(runtime_stats.command_rpc_count(crate::CommandType::Get), 2);
        assert_eq!(runtime_stats.error_stats().distinct_error_count(), 1);
        let accesses = runtime_stats.replica_access_stats().access_infos();
        assert_eq!(accesses.len(), 1);
        assert_eq!(accesses[0].peer_id, 40);
        assert_eq!(accesses[0].store_id, 41);
        assert!(accesses[0].error.contains("transient failure"));

        let preserve_first_dispatch = Arc::new(AtomicBool::new(true));
        let preserve_pd_client = Arc::new(InvalidationTrackingPdClient {
            client: MockKvClient::with_dispatch_hook(move |_: &dyn Any| {
                if preserve_first_dispatch.swap(false, Ordering::SeqCst) {
                    Err(Error::GrpcAPI(tonic::Status::unavailable(
                        "transient failure",
                    )))
                } else {
                    Ok(Box::new(MockOkResponse) as Box<dyn Any>)
                }
            }),
            invalidate_region_count: AtomicUsize::new(0),
            invalidate_store_count: AtomicUsize::new(0),
            region_lookup_count: AtomicUsize::new(0),
            preserve_route_on_failure: true,
        });
        let preserve_plan = crate::request::PlanBuilder::new(
            preserve_pd_client.clone(),
            Keyspace::Disable,
            MockKvRequest,
        )
        .retry_multi_region(Backoff::no_jitter_backoff(1, 1, 1))
        .plan();
        assert!(preserve_plan.execute().await.is_ok());
        assert_eq!(
            preserve_pd_client
                .invalidate_region_count
                .load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            preserve_pd_client
                .invalidate_store_count
                .load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            preserve_pd_client
                .region_lookup_count
                .load(Ordering::SeqCst),
            1
        );
    }
}
