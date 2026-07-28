//! Generated protobuf contracts shared by the Rust SQL layer.
//!
//! The source protos are checked in next to this crate and generated with
//! `prost`; this crate intentionally does not contain handwritten wire
//! encoders.  The response leaf is a dependency-closed projection of TiDB's
//! upstream `tipb/select.proto` contract, retaining exact field numbers and
//! cardinality for the next raw DistSQL response decoder.  The coprocessor
//! request leaf follows the same rule for fields needed before region/RPC
//! ownership.

#![allow(missing_docs)]

/// The generated `tipb` package.
pub mod tipb {
    include!(concat!(env!("OUT_DIR"), "/tipb.rs"));
}

/// The generated dependency-closed coprocessor request package.
pub mod coprocessor {
    include!(concat!(env!("OUT_DIR"), "/coprocessor.rs"));
}

/// The generated dependency-closed region-error package.
pub mod errorpb {
    include!(concat!(env!("OUT_DIR"), "/errorpb.rs"));
}

/// The generated dependency-closed TiKV request-context package.
pub mod kvrpcpb {
    include!(concat!(env!("OUT_DIR"), "/kvrpcpb.rs"));
}

/// The generated dependency-closed TiKV gRPC service package.
pub mod tikvpb {
    include!(concat!(env!("OUT_DIR"), "/tikvpb.rs"));
}

/// The generated dependency-closed region metadata package.
pub mod metapb {
    include!(concat!(env!("OUT_DIR"), "/metapb.rs"));
}

/// The generated dependency-closed PD control-plane package.
pub mod pdpb {
    include!(concat!(env!("OUT_DIR"), "/pdpb.rs"));
}

/// The generated dependency-closed TiFlash MPP package.
pub mod mpp {
    include!(concat!(env!("OUT_DIR"), "/mpp.rs"));
}

/// The generated dependency-closed etcd MVCC key/value package.
pub mod mvccpb {
    include!(concat!(env!("OUT_DIR"), "/mvccpb.rs"));
}

/// The generated dependency-closed etcd v3 KV/Watch service package.
///
/// PD serves this on its own client port, which is how a node PUTs and
/// watches `/tidb/ddl/global_schema_version`.
pub mod etcdserverpb {
    include!(concat!(env!("OUT_DIR"), "/etcdserverpb.rs"));
}

pub use coprocessor::{
    ExecDetailsV2 as CoprocessorExecDetailsV2, KeyRange as CoprocessorKeyRange,
    Peer as CoprocessorPeer, RegionEpoch as CoprocessorRegionEpoch, Request as CoprocessorRequest,
    Response as CoprocessorResponse, ScanDetailV2 as CoprocessorScanDetailV2, StoreBatchTask,
    StoreBatchTaskResponse, VersionedKeyRange as CoprocessorVersionedKeyRange,
};

pub use errorpb::Error as RegionError;

pub use mpp::{
    DispatchTaskResponse as MppDispatchTaskResponse, Error as MppError,
    ReportTaskStatusRequest as MppReportTaskStatusRequest,
    ReportTaskStatusResponse as MppReportTaskStatusResponse, TaskMeta as MppTaskMeta,
};

pub use kvrpcpb::prewrite_request::PessimisticAction as KvrpcPessimisticAction;

pub use kvrpcpb::{
    Action as KvrpcTxnAction, AlreadyExist as KvrpcAlreadyExist, Assertion as KvrpcAssertion,
    AssertionFailed as KvrpcAssertionFailed, AssertionLevel as KvrpcAssertionLevel,
    BatchRollbackRequest as KvrpcBatchRollbackRequest,
    BatchRollbackResponse as KvrpcBatchRollbackResponse,
    CheckTxnStatusRequest as KvrpcCheckTxnStatusRequest,
    CheckTxnStatusResponse as KvrpcCheckTxnStatusResponse, CommandPri as KvrpcCommandPriority,
    CommitRequest as KvrpcCommitRequest, CommitResponse as KvrpcCommitResponse,
    CommitRole as KvrpcCommitRole, CommitTsExpired as KvrpcCommitTsExpired,
    CommitTsTooLarge as KvrpcCommitTsTooLarge, Context as KvrpcContext, Deadlock as KvrpcDeadlock,
    GetRequest as KvrpcGetRequest, GetResponse as KvrpcGetResponse,
    IsolationLevel as KvrpcIsolationLevel, KeyError as KvrpcKeyError, KvPair as KvrpcKvPair,
    LockInfo as KvrpcLockInfo, Mutation as KvrpcMutation, Op as KvrpcOp, Peer as KvrpcPeer,
    PessimisticLockKeyResult as KvrpcPessimisticLockKeyResult,
    PessimisticLockKeyResultType as KvrpcPessimisticLockKeyResultType,
    PessimisticLockRequest as KvrpcPessimisticLockRequest,
    PessimisticLockResponse as KvrpcPessimisticLockResponse,
    PessimisticLockWakeUpMode as KvrpcPessimisticLockWakeUpMode,
    PessimisticRollbackRequest as KvrpcPessimisticRollbackRequest,
    PessimisticRollbackResponse as KvrpcPessimisticRollbackResponse,
    PrewriteRequest as KvrpcPrewriteRequest, PrewriteResponse as KvrpcPrewriteResponse,
    RegionEpoch as KvrpcRegionEpoch, RequestOrigin as KvrpcRequestOrigin,
    ResolveLockRequest as KvrpcResolveLockRequest, ResolveLockResponse as KvrpcResolveLockResponse,
    ResourceControlContext as KvrpcResourceControlContext, ScanRequest as KvrpcScanRequest,
    ScanResponse as KvrpcScanResponse, SourceStmt as KvrpcSourceStmt,
    TxnHeartBeatRequest as KvrpcTxnHeartBeatRequest,
    TxnHeartBeatResponse as KvrpcTxnHeartBeatResponse, TxnInfo as KvrpcTxnInfo,
    WaitForEntry as KvrpcWaitForEntry, WriteConflict as KvrpcWriteConflict,
};

pub use tipb::{
    Chunk, EncodeType, Error, ExecutorExecutionSummary, IntermediateOutput, ResourceGroupTag,
    ResourceGroupTagLabel, Row, RowMeta, SelectResponse, StreamResponse, TiFlashNetWorkSummary,
    TiFlashRegionNumOfInstance, TiFlashScanContext, TiFlashWaitSummary,
};

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::{
        Chunk, EncodeType, Error, ExecutorExecutionSummary, IntermediateOutput, ResourceGroupTag,
        ResourceGroupTagLabel, Row, RowMeta, SelectResponse, StreamResponse, TiFlashNetWorkSummary,
        TiFlashRegionNumOfInstance, TiFlashScanContext, TiFlashWaitSummary,
    };

    #[test]
    fn resource_group_tag_round_trips_go_wire_vectors() {
        let sql_digest = vec![0xab; 32];
        let plan_digest = vec![0xcd; 32];
        let tag = ResourceGroupTag {
            sql_digest: Some(sql_digest.clone()),
            plan_digest: Some(plan_digest.clone()),
            label: Some(ResourceGroupTagLabel::Index as i32),
            // Go's gogoproto `(nullable) = false` encoding emits table_id even
            // when its value is zero.  Option preserves that wire presence when
            // decoding and lets callers choose the same compatibility form.
            table_id: Some(42),
            keyspace_name: Some(b"tenant-a".to_vec()),
        };

        let encoded = tag.encode_to_vec();
        let mut go_wire = vec![0x0a, 0x20];
        go_wire.extend_from_slice(&sql_digest);
        go_wire.extend_from_slice(&[0x12, 0x20]);
        go_wire.extend_from_slice(&plan_digest);
        go_wire.extend_from_slice(&[0x18, 0x02, 0x20, 0x2a, 0x2a, 0x08]);
        go_wire.extend_from_slice(b"tenant-a");
        assert_eq!(encoded, go_wire);

        let decoded = ResourceGroupTag::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded, tag);
        assert_eq!(decoded.sql_digest.as_deref(), Some(sql_digest.as_slice()));
        assert_eq!(decoded.plan_digest.as_deref(), Some(plan_digest.as_slice()));
        assert_eq!(decoded.label, Some(ResourceGroupTagLabel::Index as i32));
        assert_eq!(decoded.table_id, Some(42));
        assert_eq!(
            decoded.keyspace_name.as_deref(),
            Some(b"tenant-a".as_slice())
        );
    }

    #[test]
    fn resource_group_tag_matches_go_gogoproto_lengths() {
        let digest = vec![0x11; 32];

        // Go's TestResourceGroupTagEncodingPB expects the always-present
        // table_id field (field 4, varint zero) for nullable=false.
        let both = ResourceGroupTag {
            sql_digest: Some(digest.clone()),
            plan_digest: Some(vec![0x22; 32]),
            table_id: Some(0),
            ..Default::default()
        };
        assert_eq!(both.encode_to_vec().len(), 70);

        let sql_only = ResourceGroupTag {
            sql_digest: Some(digest),
            table_id: Some(0),
            ..Default::default()
        };
        assert_eq!(sql_only.encode_to_vec().len(), 36);

        let decoded = ResourceGroupTag::decode(sql_only.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded.table_id, Some(0));
        assert_eq!(decoded.label, None);
    }

    #[test]
    fn select_response_round_trips_go_wire_contract() {
        let response = SelectResponse {
            error: Some(Error {
                code: Some(1064),
                msg: Some("syntax error".to_owned()),
            }),
            rows: vec![Row {
                handle: Some(vec![0x01, 0x02]),
                data: Some(vec![0x03]),
            }],
            chunks: vec![Chunk {
                rows_data: Some(vec![0xaa, 0xbb]),
                rows_meta: vec![RowMeta {
                    handle: Some(7),
                    length: Some(2),
                }],
            }],
            warnings: vec![Error {
                code: Some(1265),
                msg: Some("truncated".to_owned()),
            }],
            output_counts: vec![3, 5],
            warning_count: Some(1),
            execution_summaries: vec![ExecutorExecutionSummary {
                time_processed_ns: Some(11),
                num_produced_rows: Some(3),
                num_iterations: Some(4),
                executor_id: Some("TableScan_1".to_owned()),
                concurrency: Some(2),
                detail_info: None,
                ru_consumption: Some(vec![0x0a, 0x01]),
                tiflash_wait_summary: Some(TiFlashWaitSummary {
                    min_tso_wait_ns: Some(13),
                    pipeline_queue_wait_ns: Some(17),
                    pipeline_breaker_wait_ns: Some(19),
                }),
                tiflash_network_summary: Some(TiFlashNetWorkSummary {
                    inner_zone_send_bytes: Some(23),
                    inner_zone_receive_bytes: Some(29),
                    inter_zone_send_bytes: Some(31),
                    inter_zone_receive_bytes: Some(37),
                }),
            }],
            encode_type: Some(EncodeType::TypeChunk as i32),
            ndvs: vec![41, 43],
            intermediate_outputs: vec![IntermediateOutput {
                encode_type: Some(EncodeType::TypeDefault as i32),
                chunks: vec![Chunk {
                    rows_data: Some(vec![0x55]),
                    rows_meta: vec![RowMeta {
                        handle: Some(-1),
                        length: Some(1),
                    }],
                }],
            }],
        };

        let encoded = response.encode_to_vec();
        let decoded = SelectResponse::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded, response);

        // These anchors are the upstream select.proto field numbers.  They
        // make a wire-contract regression visible even if a field is later
        // accidentally renumbered in the checked-in projection.
        assert!(encoded.contains(&0x12)); // rows, field 2
        assert!(encoded.contains(&0x1a)); // chunks, field 3
        assert!(encoded.contains(&0x42)); // execution summaries, field 8
        assert!(encoded.contains(&0x5a)); // intermediate outputs, field 11
    }

    #[test]
    fn stream_response_and_chunk_preserve_sparse_field_numbers() {
        let response = StreamResponse {
            error: None,
            data: Some(vec![0xde, 0xad]),
            warnings: Vec::new(),
            output_counts: vec![2],
            warning_count: Some(1),
            ndvs: vec![3],
        };
        let encoded = response.encode_to_vec();
        // Upstream intentionally leaves field 2 unused: data is field 3.
        assert_eq!(
            encoded,
            vec![0x1a, 0x02, 0xde, 0xad, 0x28, 0x02, 0x30, 0x01, 0x38, 0x03]
        );
        assert_eq!(
            StreamResponse::decode(encoded.as_slice()).unwrap(),
            response
        );

        let chunk = Chunk {
            rows_data: Some(vec![]),
            rows_meta: vec![],
        };
        // A present empty rows_data is still field 3 on the Go wire contract.
        assert_eq!(chunk.encode_to_vec(), vec![0x1a, 0x00]);
        assert_eq!(
            Chunk::decode(chunk.encode_to_vec().as_slice()).unwrap(),
            chunk
        );
    }

    #[test]
    fn tiflash_scan_context_keeps_high_numbered_fields() {
        let context = TiFlashScanContext {
            regions_of_instance: vec![TiFlashRegionNumOfInstance {
                instance_id: Some("i-1".to_owned()),
                region_num: Some(2),
            }],
            vector_idx_load_from_s3: Some(3),
            inverted_idx_search_selected_rows: Some(5),
            fts_n_from_inmemory_noindex: Some(6),
            fts_brute_total_search_ms: Some(7),
            ..Default::default()
        };
        let encoded = context.encode_to_vec();
        let decoded = TiFlashScanContext::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded, context);
        assert!(encoded.windows(2).any(|window| window == [0xa0, 0x06])); // field 100 key
        assert!(encoded.windows(2).any(|window| window == [0xf8, 0x07])); // field 127 key
        assert!(encoded.windows(2).any(|window| window == [0xb0, 0x09])); // field 150 key
    }

    #[test]
    fn the_etcd_put_of_a_global_schema_version_matches_the_go_wire_shape() {
        use crate::etcdserverpb::PutRequest;

        // Exactly what `OwnerUpdateGlobalVersion` sends: the key bytes, the
        // decimal ASCII text of the int64 version, and no lease.
        let request = PutRequest {
            key: b"/tidb/ddl/global_schema_version".to_vec(),
            value: b"127".to_vec(),
            ..Default::default()
        };
        let encoded = request.encode_to_vec();
        let mut expected = vec![0x0a, 31];
        expected.extend_from_slice(b"/tidb/ddl/global_schema_version");
        expected.extend_from_slice(&[0x12, 0x03]);
        expected.extend_from_slice(b"127");
        assert_eq!(encoded, expected);
        assert_eq!(PutRequest::decode(encoded.as_slice()).unwrap(), request);
    }

    #[test]
    fn a_single_key_watch_create_keeps_its_oneof_and_field_numbers() {
        use crate::etcdserverpb::{watch_request::RequestUnion, WatchCreateRequest, WatchRequest};

        let request = WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                key: b"/k".to_vec(),
                ..Default::default()
            })),
        };
        // oneof create_request is field 1; the nested key is field 1 too.
        assert_eq!(
            request.encode_to_vec(),
            vec![0x0a, 0x04, 0x0a, 0x02, b'/', b'k']
        );
        assert_eq!(
            WatchRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
            request
        );
    }

    #[test]
    fn a_watch_response_carries_its_events_on_field_eleven() {
        use crate::etcdserverpb::WatchResponse;
        use crate::mvccpb::{event::EventType, Event, KeyValue};

        let response = WatchResponse {
            events: vec![Event {
                r#type: EventType::Put as i32,
                kv: Some(KeyValue {
                    key: b"/k".to_vec(),
                    value: b"9".to_vec(),
                    ..Default::default()
                }),
                prev_kv: None,
            }],
            ..Default::default()
        };
        let encoded = response.encode_to_vec();
        // Field 11, wire type 2 -> tag byte 0x5a: the upstream sparse number
        // that a renumbered projection would silently break.
        assert_eq!(encoded[0], 0x5a);
        assert_eq!(WatchResponse::decode(encoded.as_slice()).unwrap(), response);
    }

    #[test]
    fn absent_proto2_optional_scalars_are_distinct_from_present_zero() {
        let absent = Error::default();
        assert!(absent.encode_to_vec().is_empty());

        let present = Error {
            code: Some(0),
            msg: Some(String::new()),
        };
        // Prost preserves proto2 optional presence, matching gogoproto's
        // nullable=false fields when Go explicitly emits their zero values.
        assert_eq!(present.encode_to_vec(), vec![0x08, 0x00, 0x12, 0x00]);
        assert_eq!(
            Error::decode(present.encode_to_vec().as_slice()).unwrap(),
            present
        );
    }
}
