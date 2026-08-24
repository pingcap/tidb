//! Native deterministic accounting from client-go's `internal/resourcecontrol`.

use std::time::Duration;

use prost::Message;

use crate::kv::AccessLocationType;
use crate::proto::{coprocessor, kvrpcpb};
use crate::store::Request;

/// Source RU pre-charge inputs independent of the unfinished dynamic request
/// wrapper. A write byte count of `None` denotes a read request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RequestInfo {
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
    pub(crate) fn read(store_id: u64, request_size: u64, bypass: bool) -> Self {
        Self {
            write_bytes: None,
            store_id,
            replica_number: 0,
            request_size,
            access_location: AccessLocationType::Unknown,
            predicted_read_bytes: 0,
            is_coprocessor: false,
            bypass,
        }
    }

    pub(crate) fn prewrite(
        request: &kvrpcpb::PrewriteRequest,
        store_id: u64,
        replica_number: i64,
        request_size: u64,
        bypass: bool,
    ) -> Self {
        let write_bytes = request
            .mutations
            .iter()
            .map(|mutation| (mutation.key.len() + mutation.value.len()) as u64)
            .sum::<u64>()
            + request.primary_lock.len() as u64
            + request
                .secondaries
                .iter()
                .map(|key| key.len() as u64)
                .sum::<u64>();
        Self {
            write_bytes: Some(write_bytes),
            store_id,
            replica_number,
            request_size,
            access_location: AccessLocationType::Unknown,
            predicted_read_bytes: 0,
            is_coprocessor: false,
            bypass,
        }
    }

    pub(crate) fn commit(
        request: &kvrpcpb::CommitRequest,
        store_id: u64,
        replica_number: i64,
        request_size: u64,
        bypass: bool,
    ) -> Self {
        Self {
            write_bytes: Some(request.keys.iter().map(|key| key.len() as u64).sum()),
            store_id,
            replica_number,
            request_size,
            access_location: AccessLocationType::Unknown,
            predicted_read_bytes: 0,
            is_coprocessor: false,
            bypass,
        }
    }

    /// Extracts source `resourcecontrol.MakeRequestInfo` fields from the
    /// concrete typed TiKV request. Access location and predicted bytes live
    /// on client-go's dynamic `tikvrpc.Request` wrapper; Rust does not yet
    /// expose those caller-owned knobs, so they remain their source defaults
    /// here while command/context data is preserved exactly.
    pub(crate) fn from_store_request(request: &dyn Request) -> Self {
        let context = request.tikv_context();
        let store_id = context
            .and_then(|context| context.peer.as_ref())
            .map_or(0, |peer| peer.store_id);
        let bypass = should_bypass(request);
        let request_size = request.encoded_request_size();
        let is_coprocessor = request.label() == "coprocessor";
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

    pub(crate) fn is_write(self) -> bool {
        self.write_bytes.is_some()
    }
    pub(crate) fn write_bytes(self) -> u64 {
        self.write_bytes.unwrap_or_default()
    }
}

fn should_bypass(request: &dyn Request) -> bool {
    let request_source = request
        .tikv_context()
        .map_or("", |context| context.request_source.as_str());
    if request_source.contains("internal_others") {
        return true;
    }
    cfg!(feature = "nextgen")
        && request_source.contains("internal_stats")
        && request.label() == "coprocessor"
        && request
            .as_any()
            .downcast_ref::<coprocessor::Request>()
            .is_some_and(|request| request.tp == 104)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResponseInfo {
    pub read_bytes: u64,
    pub kv_cpu: Duration,
    pub response_size: u64,
}

pub(crate) enum Response<'a> {
    Cop(&'a coprocessor::Response),
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
                    data: b"12345678".to_vec(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let info = ResponseInfo::from_response(Response::Cop(&response));
        let expected = if cfg!(feature = "nextgen") {
            100 + 15 + 8
        } else {
            80 + 10 + 8
        };
        assert_eq!(info.read_bytes, expected);
        assert_eq!(info.kv_cpu, Duration::from_nanos(1_100));
        assert_eq!(info.response_size, response.encoded_len() as u64);
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
        let info = RequestInfo::prewrite(&prewrite, 2, 1, 99, true);
        assert!(info.is_write());
        assert_eq!(info.write_bytes(), 18);
        assert_eq!(info.store_id, 2);
        assert!(info.bypass);
        let commit = RequestInfo::commit(
            &kvrpcpb::CommitRequest {
                keys: vec![b"qux".to_vec()],
                ..Default::default()
            },
            3,
            2,
            10,
            false,
        );
        assert_eq!(commit.write_bytes(), 3);
        assert!(!RequestInfo::read(1, 4, false).is_write());
    }
}
