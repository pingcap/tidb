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

//! Source-backed tests for the complete `pkg/distsql/distsql.go` runtime owner.
//!
//! Exact Go anchors are retained in the evidence TSV; response bytes,
//! streaming, protobuf, TiKV client, memory tracker implementation, and the
//! full select-result runtime-stat aggregation remain external boundaries.

use tidb_distsql::{
    analyze_request_source, analyze_result_metadata, can_use_chunk_rpc, checksum_result_metadata,
    mpp_result_metadata, select_result_metadata, select_with_runtime_stats, set_encode_type,
    system_endian, tiflash_conf_metadata, with_sql_kv_exec_counter_interceptor, EncodeType,
    SelectInput, SelectResultMetadata, StoreType, SystemEndian, TiFlashSettings,
    ANALYZE_RESULT_LABEL, CHECKSUM_RESULT_LABEL, DAG_RESULT_LABEL, GENERAL_SQL_TYPE,
    INTERNAL_SQL_TYPE, INTERNAL_TXN_STATS_SOURCE, MPP_RESULT_LABEL,
};

fn select_input() -> SelectInput {
    SelectInput {
        store_type: StoreType::TiKv,
        row_len: 4,
        mem_tracker_bound: true,
        paging_enabled: false,
        paging_size_bytes: 0,
        dist_sql_concurrency: 15,
        ..SelectInput::default()
    }
}

#[test]
fn source_select_normal_preserves_dag_result_metadata() {
    let metadata = select_result_metadata(select_input());
    assert_eq!(metadata.label, DAG_RESULT_LABEL);
    assert_eq!(metadata.sql_type, Some(GENERAL_SQL_TYPE));
    assert_eq!(metadata.row_len, 4);
    assert!(metadata.mem_tracker_bound);
    assert_eq!(metadata.store_type, StoreType::TiKv);
}

#[test]
fn source_select_mem_tracker_and_paging_plumbing_is_explicit() {
    let mut input = select_input();
    input.paging_size_bytes = 4096;
    let metadata = select_result_metadata(input);
    assert!(metadata.mem_tracker_bound);
    assert!(metadata.paging);
    assert_eq!(metadata.dist_sql_concurrency, 15);
}

#[test]
fn source_select_normal_chunk_size_uses_alignment_gated_encoding() {
    let mut context = tidb_distsql::DistSqlContext::new();
    assert!(!can_use_chunk_rpc(&context, true));
    assert_eq!(set_encode_type(&context, true), EncodeType::Default);

    context.request.enable_chunk_rpc = true;
    let detached = context.detach();
    assert!(can_use_chunk_rpc(&detached, true));
    assert!(!can_use_chunk_rpc(&detached, false));
    assert_eq!(set_encode_type(&detached, false), EncodeType::Default);
    assert_eq!(set_encode_type(&detached, true), EncodeType::Chunk);
    assert!(matches!(
        system_endian(),
        SystemEndian::Big | SystemEndian::Little
    ));
}

#[test]
fn source_select_with_runtime_stats_preserves_plan_ids_and_root() {
    let beyond_i32 = (i32::MAX as isize) + 1;
    let metadata = select_with_runtime_stats(select_input(), vec![1, 2, beyond_i32], beyond_i32);
    assert_eq!(metadata.label, DAG_RESULT_LABEL);
    assert_eq!(metadata.cop_plan_ids, vec![1, 2, beyond_i32]);
    assert_eq!(metadata.root_plan_id, Some(beyond_i32));
}

#[test]
fn source_select_result_runtime_stats_metadata_is_cloneable() {
    let metadata = select_with_runtime_stats(select_input(), vec![4, 5], 6);
    let clone = metadata.clone();
    assert_eq!(clone, metadata);
    // Full selectResultRuntimeStats merge/string formatting is owned by the
    // existing select_result.go boundary; this test pins option metadata only.
}

#[test]
fn source_analyze_result_sets_internal_stats_source() {
    let metadata = analyze_result_metadata(StoreType::TiKv, true);
    assert_eq!(metadata.label, ANALYZE_RESULT_LABEL);
    assert_eq!(metadata.sql_type, Some(INTERNAL_SQL_TYPE));
    let source = analyze_request_source();
    assert!(source.internal);
    assert_eq!(source.source_type, INTERNAL_TXN_STATS_SOURCE);
}

#[test]
fn source_checksum_result_uses_general_sql_metadata() {
    let metadata = checksum_result_metadata(StoreType::TiKv);
    assert_eq!(metadata.label, CHECKSUM_RESULT_LABEL);
    assert_eq!(metadata.sql_type, Some(GENERAL_SQL_TYPE));
}

#[test]
fn source_mpp_result_preserves_store_and_runtime_plan_metadata() {
    let metadata: SelectResultMetadata = mpp_result_metadata(2, vec![7], 8);
    assert_eq!(metadata.label, MPP_RESULT_LABEL);
    assert_eq!(metadata.sql_type, None);
    assert_eq!(metadata.store_type, StoreType::TiFlash);
    assert_eq!(metadata.cop_plan_ids, vec![7]);
    assert_eq!(metadata.root_plan_id, Some(8));
}

#[test]
fn source_tiflash_conf_vars_preserve_source_order_and_unset_filtering() {
    let metadata = tiflash_conf_metadata(TiFlashSettings {
        max_threads: 8,
        max_bytes_before_external_join: -1,
        max_bytes_before_external_group_by: 16,
        max_bytes_before_external_sort: 32,
        max_query_memory_per_node: -1,
        query_spill_ratio: 0.7,
        hash_join_optimized: true,
    });
    assert_eq!(metadata[0].key, "tidb_max_tiflash_threads");
    assert_eq!(metadata[0].value, "8");
    assert_eq!(
        metadata[1].key,
        "tidb_max_bytes_before_tiflash_external_group_by"
    );
    assert_eq!(
        metadata[2].key,
        "tidb_max_bytes_before_tiflash_external_sort"
    );
    assert_eq!(metadata[3].key, "tiflash_mem_quota_query_per_node");
    assert_eq!(metadata[3].value, "0");
    assert_eq!(metadata[4].key, "tiflash_query_spill_ratio");
    assert_eq!(metadata[4].value, "0.7");
    assert_eq!(metadata[5].key, "tiflash_use_hash_join_v2");
    assert_eq!(metadata[5].value, "true");
}

#[test]
fn source_tiflash_spill_ratio_uses_go_special_float_spelling() {
    for (ratio, expected) in [
        (f64::INFINITY, "+Inf"),
        (f64::NEG_INFINITY, "-Inf"),
        (f64::NAN, "NaN"),
        (-0.0, "-0"),
    ] {
        let metadata = tiflash_conf_metadata(TiFlashSettings {
            query_spill_ratio: ratio,
            ..TiFlashSettings::default()
        });
        assert_eq!(metadata[1].key, "tiflash_query_spill_ratio");
        assert_eq!(metadata[1].value, expected);
    }
}

#[test]
fn source_kv_counter_interceptor_binds_only_when_present() {
    assert!(with_sql_kv_exec_counter_interceptor(true));
    assert!(!with_sql_kv_exec_counter_interceptor(false));
}
