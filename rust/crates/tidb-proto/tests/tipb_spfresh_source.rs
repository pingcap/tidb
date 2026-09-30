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

//! Translates every test in the pinned go-tipb/spfresh_test.go.

use prost::Message;
use tidb_proto::tipb::{
    sp_fresh_search_response::Result as SearchResult, Error, SpFreshAnnStats, SpFreshErrorCode,
    SpFreshEvalContext, SpFreshFilterExprColumn, SpFreshFilterExprColumnSource,
    SpFreshSearchRequest, SpFreshSearchResponse, SpFreshSearchResult, SpFreshSearchStats,
};

#[test]
fn filter_expr_column_source_wire_values() {
    assert_eq!(SpFreshFilterExprColumnSource::Unspecified as i32, 0);
    assert_eq!(SpFreshFilterExprColumnSource::Stored as i32, 1);
    assert_eq!(SpFreshFilterExprColumnSource::Handle as i32, 2);
}

#[test]
fn filter_expr_column_missing_source_is_unspecified() {
    let column = SpFreshFilterExprColumn::decode([0x08, 0x2a].as_slice()).unwrap();
    assert_eq!(column.source(), SpFreshFilterExprColumnSource::Unspecified);
}

#[test]
fn eval_context_preserves_sql_mode() {
    let context = SpFreshEvalContext {
        sql_mode: (1 << 6) | (1 << 21) | (1 << 26),
        ..Default::default()
    };
    let decoded = SpFreshEvalContext::decode(context.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.sql_mode, context.sql_mode);
}

#[test]
fn search_request_max_response_bytes_wire_field() {
    let request = SpFreshSearchRequest {
        max_response_bytes: 1 << 63,
        ..Default::default()
    };
    let expected = [
        0x78, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01,
    ];
    assert_eq!(request.encode_to_vec(), expected);
    assert_eq!(
        SpFreshSearchRequest::decode(expected.as_slice())
            .unwrap()
            .max_response_bytes,
        1 << 63
    );
}

#[test]
fn search_response_result_is_exclusive() {
    let success = SpFreshSearchResponse {
        result: Some(SearchResult::Success(SpFreshSearchResult {
            warning_count: 1 << 63,
            ..Default::default()
        })),
    };
    let decoded = SpFreshSearchResponse::decode(success.encode_to_vec().as_slice()).unwrap();
    let Some(SearchResult::Success(result)) = decoded.result else {
        panic!("expected success only")
    };
    assert_eq!(result.warning_count, 1 << 63);
    let error = SpFreshSearchResponse {
        result: Some(SearchResult::Error(Error {
            code: Some(SpFreshErrorCode::SpFreshIndexCorruption as i32),
            ..Default::default()
        })),
    };
    let decoded = SpFreshSearchResponse::decode(error.encode_to_vec().as_slice()).unwrap();
    assert!(matches!(decoded.result, Some(SearchResult::Error(_))));
    assert_eq!(SpFreshSearchResponse::default().result, None);
}

#[test]
fn ann_stats_wire_round_trip() {
    for stats in [
        SpFreshSearchStats {
            partitions_scanned: 1,
            ..Default::default()
        },
        SpFreshSearchStats {
            partitions_scanned: 1,
            ann: Some(SpFreshAnnStats::default()),
            ..Default::default()
        },
        SpFreshSearchStats {
            ann: Some(SpFreshAnnStats {
                exact_fallback: true,
                search_micros: (1 << 63) | 1,
                partitions_scanned: 18,
                centroids_scanned: 120,
                leaf_vectors_scanned: 1460,
                rough_candidates: 200,
                bound_pruned_candidates: 76,
                rerank_candidates: 124,
                exact_evaluated: 124,
                partition_cache_hits: 15,
                partition_cache_misses: 3,
                partition_cache_lookup_micros: (1 << 63) | 12,
                partition_cache_miss_load_micros: 2800,
                table_lookup_micros: 2400,
                rerank_micros: 700,
            }),
            ..Default::default()
        },
    ] {
        let decoded = SpFreshSearchStats::decode(stats.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, stats);
        assert_eq!(decoded.ann.is_some(), stats.ann.is_some());
    }
}

#[test]
fn search_stats_field_numbers() {
    // Go tests descriptors; prost tests the encoded tag of each field separately
    // so swapping equal-valued fields cannot hide a numbering change.
    let fields = [
        (
            SpFreshSearchStats {
                partitions_scanned: 1,
                ..Default::default()
            },
            vec![0x08, 1],
        ),
        (
            SpFreshSearchStats {
                vectors_scanned: 1,
                ..Default::default()
            },
            vec![0x10, 1],
        ),
        (
            SpFreshSearchStats {
                table_lookup_keys: 1,
                ..Default::default()
            },
            vec![0x18, 1],
        ),
        (
            SpFreshSearchStats {
                table_lookup_bytes: 1,
                ..Default::default()
            },
            vec![0x20, 1],
        ),
        (
            SpFreshSearchStats {
                permit_micros: 1,
                ..Default::default()
            },
            vec![0x28, 1],
        ),
        (
            SpFreshSearchStats {
                config_micros: 1,
                ..Default::default()
            },
            vec![0x30, 1],
        ),
        (
            SpFreshSearchStats {
                index_open_micros: 1,
                ..Default::default()
            },
            vec![0x38, 1],
        ),
        (
            SpFreshSearchStats {
                search_micros: 1,
                ..Default::default()
            },
            vec![0x40, 1],
        ),
        (
            SpFreshSearchStats {
                table_lookup_micros: 1,
                ..Default::default()
            },
            vec![0x48, 1],
        ),
        (
            SpFreshSearchStats {
                tikv_client_rpc_count: 1,
                ..Default::default()
            },
            vec![0x50, 1],
        ),
        (
            SpFreshSearchStats {
                tikv_client_rpc_micros: 1,
                ..Default::default()
            },
            vec![0x58, 1],
        ),
        (
            SpFreshSearchStats {
                read_only: true,
                ..Default::default()
            },
            vec![0x60, 1],
        ),
        (
            SpFreshSearchStats {
                oversample_factor: 1.0,
                ..Default::default()
            },
            vec![0x69, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f],
        ),
        (
            SpFreshSearchStats {
                partition_cache_hits: 1,
                ..Default::default()
            },
            vec![0x70, 1],
        ),
        (
            SpFreshSearchStats {
                partition_cache_misses: 1,
                ..Default::default()
            },
            vec![0x78, 1],
        ),
        (
            SpFreshSearchStats {
                ann: Some(SpFreshAnnStats::default()),
                ..Default::default()
            },
            vec![0x82, 1, 0],
        ),
    ];
    for (message, expected) in fields {
        assert_eq!(message.encode_to_vec(), expected);
    }
}

#[test]
fn index_corruption_code() {
    assert_eq!(SpFreshErrorCode::SpFreshIndexCorruption as i32, 9015);
}

#[test]
fn response_too_large_code() {
    assert_eq!(SpFreshErrorCode::SpFreshResponseTooLarge as i32, 9016);
}
