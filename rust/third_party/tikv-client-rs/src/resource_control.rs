//! Native deterministic accounting from client-go's `internal/resourcecontrol`.

use std::any::Any;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::time::Duration;

use async_trait::async_trait;
use prost::Message;

use crate::kv::AccessLocationType;
use crate::proto::{coprocessor, kvrpcpb, resource_manager};
use crate::store::{CoprocessorStreamResponse, Request};
use crate::Result;

/// Source RU pre-charge inputs translated from Rust's typed request and route
/// boundaries. A write byte count of `None` denotes a read request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestInfo {
    pub write_bytes: Option<u64>,
    pub store_id: u64,
    pub replica_number: i64,
    pub request_size: u64,
    pub access_location: AccessLocationType,
    pub predicted_read_bytes: u64,
    pub is_coprocessor: bool,
    pub bypass: bool,
}

impl RequestInfo {
    /// Creates precomputed source admission information. `None` represents a
    /// read request; `Some(0)` remains a zero-byte write.
    pub fn new(write_bytes: Option<u64>, store_id: u64, replica_number: i64, bypass: bool) -> Self {
        Self {
            write_bytes,
            store_id,
            replica_number,
            request_size: 0,
            access_location: AccessLocationType::Unknown,
            predicted_read_bytes: 0,
            is_coprocessor: false,
            bypass,
        }
    }

    /// Extracts source `resourcecontrol.MakeRequestInfo` fields from the
    /// concrete typed TiKV request. Route-owned replica/location fields and
    /// the caller's predicted read hint are installed by `select` after this
    /// request-local extraction step.
    pub(crate) fn from_store_request(request: &dyn Request) -> Self {
        let context = request.tikv_context();
        let store_id = context
            .and_then(|context| context.peer.as_ref())
            .map_or(0, |peer| peer.store_id);
        let bypass = should_bypass(request);
        let request_size = request.network_request_size();
        let is_coprocessor = request.is_resource_control_coprocessor();
        if !request.is_resource_control_write() {
            return Self {
                write_bytes: None,
                store_id,
                replica_number: 0,
                request_size,
                access_location: AccessLocationType::Unknown,
                predicted_read_bytes: 0,
                is_coprocessor,
                bypass,
            };
        }

        let write_bytes =
            if let Some(prewrite) = request.as_any().downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite
                    .mutations
                    .iter()
                    .map(|mutation| (mutation.key.len() + mutation.value.len()) as u64)
                    .sum::<u64>()
                    + prewrite.primary_lock.len() as u64
                    + prewrite
                        .secondaries
                        .iter()
                        .map(|key| key.len() as u64)
                        .sum::<u64>()
            } else if let Some(commit) = request.as_any().downcast_ref::<kvrpcpb::CommitRequest>() {
                commit.keys.iter().map(|key| key.len() as u64).sum()
            } else {
                0
            };
        Self {
            write_bytes: Some(write_bytes),
            store_id,
            replica_number: 0,
            request_size,
            access_location: AccessLocationType::Unknown,
            predicted_read_bytes: 0,
            is_coprocessor: false,
            bypass,
        }
    }

    pub fn is_write(&self) -> bool {
        self.write_bytes.is_some()
    }
    pub fn write_bytes(&self) -> u64 {
        self.write_bytes.unwrap_or_default()
    }

    pub fn replica_number(&self) -> i64 {
        self.replica_number
    }

    pub fn bypass(&self) -> bool {
        self.bypass
    }

    pub fn store_id(&self) -> u64 {
        self.store_id
    }

    pub fn request_size(&self) -> u64 {
        self.request_size
    }

    pub fn access_location_type(&self) -> AccessLocationType {
        self.access_location
    }

    pub fn predicted_read_bytes(&self) -> u64 {
        self.predicted_read_bytes
    }

    pub fn is_cop(&self) -> bool {
        self.is_coprocessor
    }
}

fn should_bypass(request: &dyn Request) -> bool {
    let request_source = request
        .tikv_context()
        .map_or("", |context| context.request_source.as_str());
    if cfg!(feature = "nextgen")
        && request_source.contains("stats")
        && request.resource_control_coprocessor_type() == Some(104)
    {
        return true;
    }
    request_source.contains("internal_others")
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResponseInfo {
    pub read_bytes: u64,
    pub kv_cpu: Duration,
    pub response_size: u64,
}

pub(crate) enum Response<'a> {
    Cop(&'a coprocessor::Response),
    CopStream(Option<&'a coprocessor::Response>),
    Get(&'a kvrpcpb::GetResponse),
    BatchGet(&'a kvrpcpb::BatchGetResponse),
    Scan(&'a kvrpcpb::ScanResponse),
}

impl ResponseInfo {
    pub(crate) fn from_response(response: Response<'_>) -> Self {
        match response {
            Response::Cop(response) => {
                let details = response.exec_details_v2.as_ref();
                let mut read_bytes = details
                    .and_then(|details| details.scan_detail_v2.as_ref())
                    .map(scan_read_bytes)
                    .unwrap_or(response.data.len() as u64);
                let mut kv_cpu_duration = kv_cpu(details, response.exec_details.as_ref());
                for task in &response.batch_responses {
                    let task_details = task.exec_details_v2.as_ref();
                    read_bytes += task_details
                        .and_then(|details| details.scan_detail_v2.as_ref())
                        .map(scan_read_bytes)
                        .unwrap_or(task.data.len() as u64);
                    kv_cpu_duration += kv_cpu(task_details, None);
                }
                Self {
                    read_bytes,
                    kv_cpu: kv_cpu_duration,
                    response_size: response.encoded_len() as u64,
                }
            }
            Response::CopStream(response) => {
                let Some(response) = response else {
                    return Self::default();
                };
                let details = response.exec_details_v2.as_ref();
                Self {
                    read_bytes: details
                        .and_then(|details| details.scan_detail_v2.as_ref())
                        .map(scan_read_bytes)
                        .unwrap_or(response.data.len() as u64),
                    kv_cpu: kv_cpu(details, response.exec_details.as_ref()),
                    // `tikvrpc.Response.GetSize` does not include the
                    // CopStream wrapper even though its embedded first
                    // response supplies bytes and execution details.
                    response_size: 0,
                }
            }
            Response::Get(response) => {
                Self::from_details(response.exec_details_v2.as_ref(), response.encoded_len())
            }
            Response::BatchGet(response) => {
                Self::from_details(response.exec_details_v2.as_ref(), response.encoded_len())
            }
            Response::Scan(response) => {
                let response_size = response.encoded_len() as u64;
                Self {
                    // client-go has no execution details for ScanResponse, so
                    // it uses the encoded response as its best read-byte
                    // estimate.
                    read_bytes: response_size,
                    response_size,
                    ..Self::default()
                }
            }
        }
    }

    /// Extracts source response accounting from an erased physical RPC response.
    /// Unsupported commands deliberately settle with zero values, matching
    /// client-go's `MakeResponseInfo` default case.
    pub(crate) fn from_dispatch_response(response: &dyn Any) -> Self {
        if let Some(response) = response.downcast_ref::<coprocessor::Response>() {
            Self::from_response(Response::Cop(response))
        } else if let Some(response) = response.downcast_ref::<CoprocessorStreamResponse>() {
            Self::from_response(Response::CopStream(response.first.as_ref()))
        } else if let Some(response) = response.downcast_ref::<kvrpcpb::GetResponse>() {
            Self::from_response(Response::Get(response))
        } else if let Some(response) = response.downcast_ref::<kvrpcpb::BatchGetResponse>() {
            Self::from_response(Response::BatchGet(response))
        } else if let Some(response) = response.downcast_ref::<kvrpcpb::ScanResponse>() {
            Self::from_response(Response::Scan(response))
        } else {
            Self::default()
        }
    }

    fn from_details(details: Option<&kvrpcpb::ExecDetailsV2>, size: usize) -> Self {
        Self {
            read_bytes: details
                .and_then(|details| details.scan_detail_v2.as_ref())
                .map(scan_read_bytes)
                .unwrap_or_default(),
            kv_cpu: kv_cpu(details, None),
            response_size: size as u64,
        }
    }

    pub fn read_bytes(&self) -> u64 {
        self.read_bytes
    }

    pub fn kv_cpu(&self) -> Duration {
        self.kv_cpu
    }

    pub const fn succeed(&self) -> bool {
        true
    }

    pub fn response_size(&self) -> u64 {
        self.response_size
    }
}

/// Result of source PD admission before a physical TiKV RPC.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RequestWaitResult {
    pub consumption: resource_manager::Consumption,
    pub penalty: Option<resource_manager::Consumption>,
    pub wait_duration: Duration,
    pub priority: u64,
}

/// Result of source PD settlement after a physical TiKV RPC response.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResponseWaitResult {
    pub consumption: resource_manager::Consumption,
    pub wait_duration: Duration,
}

/// PD-backed resource-group admission and settlement controller.
///
/// This is the native counterpart of client-go's
/// `ResourceGroupKVInterceptor`. Implementations may wait for tokens and
/// return an error before dispatch; a successful TiKV response is always
/// settled through [`ResourceGroupController::on_response_wait`].
#[async_trait]
pub trait ResourceGroupController: Send + Sync {
    async fn on_request_wait(
        &self,
        resource_group_name: &str,
        request: RequestInfo,
    ) -> Result<RequestWaitResult>;

    fn on_response_wait(
        &self,
        resource_group_name: &str,
        request: RequestInfo,
        response: ResponseInfo,
    ) -> Result<ResponseWaitResult>;

    /// Background work is charged and reported by TiKV itself, so client-go
    /// bypasses controller-side admission and settlement for it.
    fn is_background_request(&self, _resource_group_name: &str, _request_source: &str) -> bool {
        false
    }
}

pub type ResourceGroupControllerHandle = Arc<dyn ResourceGroupController>;

#[derive(Default)]
struct GlobalResourceControl {
    enabled: bool,
    controller: Option<ResourceGroupControllerHandle>,
}

fn global_resource_control() -> &'static RwLock<GlobalResourceControl> {
    static RESOURCE_CONTROL: OnceLock<RwLock<GlobalResourceControl>> = OnceLock::new();
    RESOURCE_CONTROL.get_or_init(|| RwLock::new(GlobalResourceControl::default()))
}

/// Enables the process-wide source-compatible resource-control dispatch path.
pub fn enable_resource_control() {
    global_resource_control().write().unwrap().enabled = true;
}

/// Disables the process-wide source-compatible resource-control dispatch path.
pub fn disable_resource_control() {
    global_resource_control().write().unwrap().enabled = false;
}

/// Installs the controller used by enabled process-wide resource control.
pub fn set_resource_control_interceptor(controller: ResourceGroupControllerHandle) {
    global_resource_control().write().unwrap().controller = Some(controller);
}

/// Removes the controller used by process-wide resource control.
pub fn unset_resource_control_interceptor() {
    global_resource_control().write().unwrap().controller = None;
}

pub(crate) fn global_controller() -> Option<ResourceGroupControllerHandle> {
    let resource_control = global_resource_control().read().unwrap();
    resource_control
        .enabled
        .then(|| resource_control.controller.clone())
        .flatten()
}

pub(crate) struct SelectedResourceControl {
    pub resource_group_name: String,
    pub controller: ResourceGroupControllerHandle,
    pub request: RequestInfo,
}

pub(crate) fn select(
    controller: &ResourceGroupControllerHandle,
    request: &dyn Request,
    replica_number: i64,
    access_location: AccessLocationType,
    predicted_read_bytes: u64,
) -> Option<SelectedResourceControl> {
    let resource_group_name = request.resource_group_name()?;
    let request_source = request
        .tikv_context()
        .map_or("", |context| context.request_source.as_str());
    if controller.is_background_request(resource_group_name, request_source) {
        return None;
    }
    let mut request_info = RequestInfo::from_store_request(request);
    request_info.replica_number = replica_number;
    request_info.access_location = match access_location {
        AccessLocationType::LocalZone => AccessLocationType::LocalZone,
        AccessLocationType::CrossZone => AccessLocationType::CrossZone,
        AccessLocationType::Unknown | AccessLocationType::Other(_) => AccessLocationType::Unknown,
    };
    if !request_info.is_write() {
        request_info.predicted_read_bytes = predicted_read_bytes;
    }
    (!request_info.bypass).then_some(SelectedResourceControl {
        resource_group_name: resource_group_name.to_owned(),
        controller: controller.clone(),
        request: request_info,
    })
}

fn scan_read_bytes(detail: &kvrpcpb::ScanDetailV2) -> u64 {
    if cfg!(feature = "nextgen") {
        detail
            .total_versions_size
            .max(detail.processed_versions_size)
    } else {
        detail.processed_versions_size
    }
}

fn kv_cpu(
    details_v2: Option<&kvrpcpb::ExecDetailsV2>,
    legacy_details: Option<&kvrpcpb::ExecDetails>,
) -> Duration {
    if let Some(detail) = details_v2.and_then(|details| details.time_detail_v2.as_ref()) {
        Duration::from_nanos(detail.process_wall_time_ns)
    } else if let Some(detail) = details_v2.and_then(|details| details.time_detail.as_ref()) {
        Duration::from_millis(detail.process_wall_time_ms)
    } else if let Some(detail) = legacy_details.and_then(|details| details.time_detail.as_ref()) {
        Duration::from_millis(detail.process_wall_time_ms)
    } else {
        Duration::ZERO
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::store::{BatchCoprocessorStreamRequest, CoprocessorStreamRequest};

    #[derive(Default)]
    struct NoopController {
        background: bool,
    }

    #[async_trait]
    impl ResourceGroupController for NoopController {
        async fn on_request_wait(
            &self,
            _: &str,
            _: RequestInfo,
        ) -> crate::Result<RequestWaitResult> {
            Ok(RequestWaitResult::default())
        }

        fn on_response_wait(
            &self,
            _: &str,
            _: RequestInfo,
            _: ResponseInfo,
        ) -> crate::Result<ResponseWaitResult> {
            Ok(ResponseWaitResult::default())
        }

        fn is_background_request(&self, _: &str, _: &str) -> bool {
            self.background
        }
    }

    #[test]
    fn source_response_info_accounts_for_cop_tasks() {
        let response = coprocessor::Response {
            exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
                    processed_versions_size: 80,
                    total_versions_size: 100,
                    ..Default::default()
                }),
                time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                    process_wall_time_ns: 1_000,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            batch_responses: vec![
                coprocessor::StoreBatchTaskResponse {
                    data: b"data".to_vec(),
                    exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                        scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
                            processed_versions_size: 10,
                            total_versions_size: 15,
                            ..Default::default()
                        }),
                        time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                            process_wall_time_ns: 100,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                coprocessor::StoreBatchTaskResponse {
                    exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                        scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
                            processed_versions_size: 20,
                            total_versions_size: 25,
                            ..Default::default()
                        }),
                        time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                            process_wall_time_ns: 200,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                coprocessor::StoreBatchTaskResponse {
                    data: b"12345678".to_vec(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let info = ResponseInfo::from_response(Response::Cop(&response));
        let expected = if cfg!(feature = "nextgen") {
            100 + 15 + 25 + 8
        } else {
            80 + 10 + 20 + 8
        };
        assert_eq!(info.read_bytes, expected);
        assert_eq!(info.kv_cpu, Duration::from_nanos(1_300));
        assert_eq!(info.response_size, response.encoded_len() as u64);

        let stream = ResponseInfo::from_response(Response::CopStream(Some(&response)));
        assert_eq!(
            stream.read_bytes(),
            if cfg!(feature = "nextgen") { 100 } else { 80 }
        );
        assert_eq!(stream.kv_cpu(), Duration::from_nanos(1_000));
        assert_eq!(stream.response_size(), 0);
        let dispatch_stream =
            CoprocessorStreamResponse::from_first_for_test(Some(response.clone()));
        assert_eq!(
            ResponseInfo::from_dispatch_response(&dispatch_stream),
            stream
        );
        assert_eq!(
            ResponseInfo::from_response(Response::CopStream(None)),
            ResponseInfo::default()
        );

        if cfg!(feature = "nextgen") {
            let compatibility = coprocessor::Response {
                exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                    scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
                        processed_versions_size: 100,
                        total_versions_size: 80,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            };
            assert_eq!(
                ResponseInfo::from_response(Response::Cop(&compatibility)).read_bytes(),
                100
            );
        }
    }

    #[test]
    fn source_response_info_accounts_for_transactional_reads() {
        let details = kvrpcpb::ExecDetailsV2 {
            scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
                processed_versions_size: 8,
                total_versions_size: 13,
                ..Default::default()
            }),
            time_detail: Some(kvrpcpb::TimeDetail {
                process_wall_time_ms: 2,
                ..Default::default()
            }),
            ..Default::default()
        };
        let get = kvrpcpb::GetResponse {
            value: b"value".to_vec(),
            exec_details_v2: Some(details.clone()),
            ..Default::default()
        };
        let batch_get = kvrpcpb::BatchGetResponse {
            exec_details_v2: Some(details),
            ..Default::default()
        };
        let expected_read_bytes = if cfg!(feature = "nextgen") { 13 } else { 8 };
        for info in [
            ResponseInfo::from_response(Response::Get(&get)),
            ResponseInfo::from_response(Response::BatchGet(&batch_get)),
        ] {
            assert_eq!(info.read_bytes, expected_read_bytes);
            assert_eq!(info.kv_cpu, Duration::from_millis(2));
        }

        let scan = kvrpcpb::ScanResponse {
            pairs: vec![kvrpcpb::KvPair {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let info = ResponseInfo::from_response(Response::Scan(&scan));
        assert_eq!(info.read_bytes, scan.encoded_len() as u64);
        assert_eq!(info.kv_cpu, Duration::ZERO);
        assert_eq!(info.response_size, scan.encoded_len() as u64);
    }

    #[test]
    fn source_request_info_uses_typed_command_context_and_bypass_rules() {
        let request = kvrpcpb::PrewriteRequest {
            mutations: vec![kvrpcpb::Mutation {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                ..Default::default()
            }],
            primary_lock: b"primary".to_vec(),
            secondaries: vec![b"secondary".to_vec()],
            context: Some(kvrpcpb::Context {
                peer: Some(crate::proto::metapb::Peer {
                    store_id: 9,
                    ..Default::default()
                }),
                request_source: "tidb_internal_others".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let info = RequestInfo::from_store_request(&request);
        assert!(info.is_write());
        assert_eq!(info.write_bytes(), 3 + 5 + 7 + 9);
        assert_eq!(info.store_id, 9);
        assert_eq!(info.request_size, request.encoded_len() as u64);
        assert!(info.bypass);
        assert_eq!(info.access_location, AccessLocationType::Unknown);
        assert_eq!(info.predicted_read_bytes, 0);

        let raw_delete = kvrpcpb::RawDeleteRequest::default();
        assert!(RequestInfo::from_store_request(&raw_delete).is_write());
        let raw_batch_delete = kvrpcpb::RawBatchDeleteRequest::default();
        assert!(!RequestInfo::from_store_request(&raw_batch_delete).is_write());
    }

    #[test]
    fn original_request_info_matrix() {
        let read = kvrpcpb::BatchGetRequest {
            context: Some(kvrpcpb::Context {
                peer: Some(crate::proto::metapb::Peer {
                    store_id: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let info = RequestInfo::from_store_request(&read);
        assert!(!info.is_write());
        assert_eq!(info.write_bytes(), 0);
        assert!(!info.bypass());
        assert_eq!(info.store_id(), 1);
        assert_eq!(info.request_size(), read.encoded_len() as u64);

        let prewrite = kvrpcpb::PrewriteRequest {
            mutations: vec![kvrpcpb::Mutation {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
                ..Default::default()
            }],
            primary_lock: b"baz".to_vec(),
            context: Some(kvrpcpb::Context {
                peer: Some(crate::proto::metapb::Peer {
                    store_id: 2,
                    ..Default::default()
                }),
                request_source: "xxx_internal_others".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let info = RequestInfo::from_store_request(&prewrite);
        assert!(info.is_write());
        assert_eq!(info.write_bytes(), 9);
        assert!(info.bypass());
        assert_eq!(info.store_id(), 2);

        let commit = kvrpcpb::CommitRequest {
            keys: vec![b"qux".to_vec()],
            context: Some(kvrpcpb::Context {
                peer: Some(crate::proto::metapb::Peer {
                    store_id: 3,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let info = RequestInfo::from_store_request(&commit);
        assert!(info.is_write());
        assert_eq!(info.write_bytes(), 3);
        assert!(!info.bypass());
        assert_eq!(info.store_id(), 3);

        let mut nil_peer = commit;
        nil_peer.context = Some(kvrpcpb::Context::default());
        assert_eq!(RequestInfo::from_store_request(&nil_peer).store_id(), 0);

        let raw_delete = kvrpcpb::RawDeleteRequest {
            key: b"raw-key".to_vec(),
            ..Default::default()
        };
        assert!(raw_delete.encoded_len() > 0);
        assert_eq!(
            RequestInfo::from_store_request(&raw_delete).request_size(),
            0
        );
    }

    #[test]
    fn source_transactional_and_raw_write_command_matrix() {
        let writes: Vec<Box<dyn Request>> = vec![
            Box::new(kvrpcpb::PessimisticLockRequest::default()),
            Box::new(kvrpcpb::PrewriteRequest::default()),
            Box::new(kvrpcpb::CommitRequest::default()),
            Box::new(kvrpcpb::BatchRollbackRequest::default()),
            Box::new(kvrpcpb::PessimisticRollbackRequest::default()),
            Box::new(kvrpcpb::CheckTxnStatusRequest::default()),
            Box::new(kvrpcpb::CheckSecondaryLocksRequest::default()),
            Box::new(kvrpcpb::CleanupRequest::default()),
            Box::new(kvrpcpb::TxnHeartBeatRequest::default()),
            Box::new(kvrpcpb::ResolveLockRequest::default()),
            Box::new(kvrpcpb::FlashbackToVersionRequest::default()),
            Box::new(kvrpcpb::PrepareFlashbackToVersionRequest::default()),
            Box::new(kvrpcpb::FlushRequest::default()),
            Box::new(kvrpcpb::RawPutRequest::default()),
            Box::new(kvrpcpb::RawBatchPutRequest::default()),
            Box::new(kvrpcpb::RawDeleteRequest::default()),
        ];
        for request in writes {
            assert!(
                RequestInfo::from_store_request(request.as_ref()).is_write(),
                "{} should be a resource-control write",
                request.label()
            );
        }

        let reads: Vec<Box<dyn Request>> = vec![
            Box::new(kvrpcpb::GetRequest::default()),
            Box::new(kvrpcpb::RawGetRequest::default()),
            Box::new(kvrpcpb::RawBatchDeleteRequest::default()),
            Box::new(kvrpcpb::RawDeleteRangeRequest::default()),
            Box::new(kvrpcpb::DeleteRangeRequest::default()),
        ];
        for request in reads {
            assert!(
                !RequestInfo::from_store_request(request.as_ref()).is_write(),
                "{} should retain source read accounting",
                request.label()
            );
        }
    }

    #[test]
    fn source_resource_control_selection_uses_routed_replica_and_zone() {
        let request = kvrpcpb::GetRequest {
            context: Some(kvrpcpb::Context {
                resource_control_context: Some(kvrpcpb::ResourceControlContext {
                    resource_group_name: "rg".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let controller: ResourceGroupControllerHandle = Arc::new(NoopController::default());
        let selected = select(
            &controller,
            &request,
            3,
            AccessLocationType::CrossZone,
            256 * 1024,
        )
        .unwrap();
        assert_eq!(selected.request.replica_number, 3);
        assert_eq!(
            selected.request.access_location,
            AccessLocationType::CrossZone
        );
        assert_eq!(selected.request.predicted_read_bytes, 256 * 1024);

        let selected_without_hint =
            select(&controller, &request, 3, AccessLocationType::CrossZone, 0).unwrap();
        assert_eq!(selected_without_hint.request.predicted_read_bytes(), 0);

        let selected_unknown =
            select(&controller, &request, 3, AccessLocationType::Other(9), 0).unwrap();
        assert_eq!(
            selected_unknown.request.access_location_type(),
            AccessLocationType::Unknown
        );

        let write = kvrpcpb::PrewriteRequest {
            context: Some(kvrpcpb::Context {
                resource_control_context: Some(kvrpcpb::ResourceControlContext {
                    resource_group_name: "rg".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let selected = select(
            &controller,
            &write,
            3,
            AccessLocationType::CrossZone,
            256 * 1024,
        )
        .unwrap();
        assert_eq!(selected.request.predicted_read_bytes, 0);

        let mut bypass = request.clone();
        bypass.context.as_mut().unwrap().request_source = "tidb_internal_others".to_owned();
        assert!(select(&controller, &bypass, 3, AccessLocationType::CrossZone, 0,).is_none());

        let background: ResourceGroupControllerHandle =
            Arc::new(NoopController { background: true });
        assert!(select(&background, &request, 3, AccessLocationType::CrossZone, 0,).is_none());
    }

    #[test]
    fn source_analyze_coprocessor_bypass_is_nextgen_only() {
        let request = coprocessor::Request {
            tp: 104,
            context: Some(kvrpcpb::Context {
                request_source: "tidb_internal_stats".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            RequestInfo::from_store_request(&request).bypass,
            cfg!(feature = "nextgen")
        );
        assert!(RequestInfo::from_store_request(&request).is_coprocessor);

        let stream = CoprocessorStreamRequest::new(request.clone());
        let stream_info = RequestInfo::from_store_request(&stream);
        assert_eq!(stream_info.bypass, cfg!(feature = "nextgen"));
        assert!(stream_info.is_cop());

        let batch = BatchCoprocessorStreamRequest::new(coprocessor::BatchRequest {
            tp: 104,
            context: request.context.clone(),
            ..Default::default()
        });
        let batch_info = RequestInfo::from_store_request(&batch);
        assert_eq!(batch_info.bypass, cfg!(feature = "nextgen"));
        assert!(!batch_info.is_cop());
    }

    #[test]
    fn original_is_cop_request_matrix() {
        let cop = coprocessor::Request::default();
        assert!(RequestInfo::from_store_request(&cop).is_cop());
        assert!(RequestInfo::from_store_request(&CoprocessorStreamRequest::new(cop)).is_cop());
        assert!(!RequestInfo::from_store_request(&kvrpcpb::GetRequest::default()).is_cop());
        assert!(!RequestInfo::from_store_request(&kvrpcpb::BatchGetRequest::default()).is_cop());
        assert!(!RequestInfo::from_store_request(&kvrpcpb::ScanRequest::default()).is_cop());
    }

    #[test]
    fn source_response_info_uses_legacy_cop_time_details_as_a_fallback() {
        let response = coprocessor::Response {
            exec_details: Some(kvrpcpb::ExecDetails {
                time_detail: Some(kvrpcpb::TimeDetail {
                    process_wall_time_ms: 3,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let info = ResponseInfo::from_response(Response::Cop(&response));
        assert_eq!(info.kv_cpu, Duration::from_millis(3));
    }

    #[test]
    fn source_request_info_counts_transactional_write_bytes() {
        let prewrite = kvrpcpb::PrewriteRequest {
            mutations: vec![kvrpcpb::Mutation {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
                ..Default::default()
            }],
            primary_lock: b"baz".to_vec(),
            secondaries: vec![b"secondary".to_vec()],
            ..Default::default()
        };
        let info = RequestInfo::from_store_request(&prewrite);
        assert!(info.is_write());
        assert_eq!(info.write_bytes(), 18);
        let constructed = RequestInfo::new(Some(3), 3, 2, false);
        assert!(constructed.is_write());
        assert_eq!(constructed.write_bytes(), 3);
        assert_eq!(constructed.store_id(), 3);
        assert_eq!(constructed.replica_number(), 2);
        assert!(!constructed.bypass());
        assert!(!RequestInfo::new(None, 1, 0, false).is_write());

        let response = ResponseInfo::default();
        assert_eq!(response.read_bytes(), 0);
        assert_eq!(response.kv_cpu(), Duration::ZERO);
        assert!(response.succeed());
        assert_eq!(response.response_size(), 0);
    }
}
