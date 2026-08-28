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

//! Source-backed inventory for manifest batch `b150`, `pkg/table`.
//!
//! This is the deterministic items 1--60 of the repository-wide `pkg/table`
//! test enumeration. The module keeps one Rust carrier for every upstream
//! `Test*` or `Benchmark*` function. Runnable carriers use the executor's
//! public SQL, key-codec, and metadata seams; Go-only transaction-buffer,
//! failpoint, cache-lease, and session/information-schema behavior remains an
//! explicit ignored carrier rather than an invented approximation.

#![cfg(test)]

use crate::{
    Catalog, StmtContext, run_create_table_on, run_insert_on, run_select_on, run_update_on,
};

fn query_ctx() -> StmtContext {
    StmtContext::for_query()
}

/// `pkg/table/column_test.go:37::TestString`.
#[test]
#[ignore = "go-parity-carrier: complete behavior is covered by tidb-model column string tests"]
fn test_string() {}

/// `pkg/table/column_test.go:87::TestFind`.
#[test]
#[ignore = "go-parity-carrier: complete behavior is covered by tidb-model column lookup tests"]
fn test_find() {}

/// `pkg/table/column_test.go:116::TestCheck`.
#[test]
#[ignore = "go-parity-carrier: complete behavior is covered by executor bad-null/model column tests"]
fn test_check() {}

/// `pkg/table/column_test.go:132::TestHandleBadNull`.
#[test]
#[ignore = "go-parity-carrier: complete behavior is covered by executor bad_null::tests"]
fn test_handle_bad_null() {}

/// `pkg/table/column_test.go:153::TestDesc`.
#[test]
#[ignore = "go-parity-carrier: column description rendering is owned by tidb-session"]
fn test_desc() {}

/// `pkg/table/column_test.go:174::TestGetZeroValue`.
#[test]
#[ignore = "go-parity-carrier: complete type zero-value matrix is covered by executor bad_null::tests"]
fn test_get_zero_value() {}

/// `pkg/table/column_test.go:277::TestCastValue`.
#[test]
#[ignore = "go-parity-carrier: complete cast/error matrix is covered by executor write-cast tests"]
fn test_cast_value() {}

/// `pkg/table/column_test.go:369::TestGetDefaultValue`.
#[test]
#[ignore = "go-parity-carrier: complete default-expression matrix is covered by executor default tests"]
fn test_get_default_value() {}

/// `pkg/table/column_test.go:563::TestCastValueStrict`.
#[test]
#[ignore = "go-parity-carrier: strict cast behavior is covered by executor write-cast tests"]
fn test_cast_value_strict() {}

/// `pkg/table/main_test.go:24::TestMain`.
#[test]
#[ignore = "go-parity-gap: Go test-main/goleak harness is not a Rust behavior test surface"]
fn test_table_main() {}

/// `pkg/table/table_test.go:26::TestErrorCode`.
#[test]
#[ignore = "go-parity-carrier: table error-code mappings are covered by tidb-error"]
fn test_error_code() {}

/// `pkg/table/table_test.go:46::TestOptions`.
#[test]
#[ignore = "go-parity-carrier: option propagation is covered by kv_table::tests::test_options"]
fn test_options() {}

/// `pkg/table/tables/assertion_test.go:29::TestSetAssertion`.
#[test]
#[ignore = "go-parity-carrier: assertion transitions are covered by tidb-txnkv assertion tests"]
fn test_set_assertion() {}

/// `pkg/table/tables/bench_test.go:37::BenchmarkAddRecordInPipelinedDML`.
#[test]
#[ignore = "go-parity-gap: Go pipelined-DML benchmark harness has no Rust benchmark carrier in this crate"]
fn benchmark_add_record_in_pipelined_dml() {}

/// `pkg/table/tables/bench_test.go:91::BenchmarkRemoveRecordInPipelinedDML`.
#[test]
#[ignore = "go-parity-gap: Go pipelined-DML benchmark harness has no Rust benchmark carrier in this crate"]
fn benchmark_remove_record_in_pipelined_dml() {}

/// `pkg/table/tables/bench_test.go:152::BenchmarkUpdateRecordInPipelinedDML`.
#[test]
#[ignore = "go-parity-gap: Go pipelined-DML benchmark harness has no Rust benchmark carrier in this crate"]
fn benchmark_update_record_in_pipelined_dml() {}

/// `pkg/table/tables/bench_test.go:220::TestBenchDaily`.
#[test]
#[ignore = "go-parity-gap: Go benchmark-day harness is not a Rust behavior test surface"]
fn test_bench_daily() {}

/// `pkg/table/tables/cache_test.go:40::TestCacheTableBasicScan`.
#[test]
#[ignore = "go-parity-gap: cache-table scan needs the Go cache lease/session/storage wrapper"]
fn test_cache_table_basic_scan() {}

/// `pkg/table/tables/cache_test.go:132::TestCacheCondition`.
#[test]
#[ignore = "go-parity-gap: cache-condition admission and lease state are not executor-only behavior"]
fn test_cache_condition() {}

/// `pkg/table/tables/cache_test.go:189::TestCacheTableBasicReadAndWrite`.
#[test]
#[ignore = "go-parity-gap: cache-table read/write requires Go cache lease coordination"]
fn test_cache_table_basic_read_and_write() {}

/// `pkg/table/tables/cache_test.go:239::TestCacheTableComplexRead`.
#[test]
#[ignore = "go-parity-gap: cache-table complex reads require Go cache lease coordination"]
fn test_cache_table_complex_read() {}

/// `pkg/table/tables/cache_test.go:277::TestBeginSleepABA`.
#[test]
#[ignore = "go-parity-gap: cache lease ABA timing/failpoint behavior is not transcreated"]
fn test_begin_sleep_aba() {}

/// `pkg/table/tables/cache_test.go:332::TestRenewLease`.
#[test]
#[ignore = "go-parity-gap: cache lease renewal is a Go session/cache service contract"]
fn test_renew_lease() {}

/// `pkg/table/tables/cache_test.go:373::TestCacheTableWriteOperatorWaitLockLease`.
#[test]
#[ignore = "go-parity-gap: cache write-operator lease waiting is not transcreated"]
fn test_cache_table_write_operator_wait_lock_lease() {}

/// `pkg/table/tables/cache_test.go:403::TestTableCacheLeaseVariable`.
#[test]
#[ignore = "go-parity-gap: table-cache lease variable is owned by Go session variables"]
fn test_table_cache_lease_variable() {}

/// `pkg/table/tables/cache_test.go:448::TestMetrics`.
#[test]
#[ignore = "go-parity-gap: cache metrics and lease lifecycle are not executor-only behavior"]
fn test_metrics() {}

/// `pkg/table/tables/cache_test.go:506::TestRenewLeaseABAFailPoint`.
#[test]
#[ignore = "go-parity-gap: cache lease ABA failpoint behavior is not transcreated"]
fn test_renew_lease_aba_fail_point() {}

/// `pkg/table/tables/index_test.go:49::TestMultiColumnCommonHandle`.
#[test]
#[ignore = "go-parity-carrier: complete common-handle index codec coverage is in tidb-tablecodec"]
fn test_multi_column_common_handle() {}

/// `pkg/table/tables/index_test.go:122::TestSingleColumnCommonHandle`.
#[test]
#[ignore = "go-parity-carrier: complete common-handle index codec coverage is in tidb-tablecodec"]
fn test_single_column_common_handle() {}

/// `pkg/table/tables/index_test.go:186::TestGenIndexValueFromIndex`.
#[test]
#[ignore = "go-parity-carrier: index-value decoding coverage is in tidb-tablecodec"]
fn test_gen_index_value_from_index() {}

/// `pkg/table/tables/index_test.go:226::TestGenIndexValueWithLargePaddingSize`.
#[test]
#[ignore = "go-parity-carrier: restored-data padding coverage is in tidb-tablecodec"]
fn test_gen_index_value_with_large_padding_size() {}

/// `pkg/table/tables/index_test.go:298::TestTableOperationsInDDLDropIndexWriteOnly`.
#[test]
#[ignore = "go-parity-gap: DDL write-only index states require the Go DDL job state machine"]
fn test_table_operations_in_ddl_drop_index_write_only() {}

/// `pkg/table/tables/index_test.go:366::TestForceLockNonUniqueIndexInDDLMergingTempIndex`.
#[test]
#[ignore = "go-parity-gap: merging temporary-index lock behavior requires Go DDL/transaction state"]
fn test_force_lock_non_unique_index_in_ddl_merging_temp_index() {}

/// `pkg/table/tables/index_test.go:434::TestMeetPartialCondition`.
#[test]
#[ignore = "go-parity-gap: partial-index condition evaluation is not maintained by this executor tier"]
fn test_meet_partial_condition() {}

/// `pkg/table/tables/index_test.go:505::TestPartialIndexDML`.
#[test]
#[ignore = "go-parity-gap: partial-index DML is refused rather than silently maintained as a full index"]
fn test_partial_index_dml() {}

/// `pkg/table/tables/index_test.go:601::TestExtractColumnsFromCondition`.
#[test]
fn test_extract_columns_from_condition() {
    let column = |name: &str, offset, generated: &str, stored| tidb_model::ColumnInfo {
        name: tidb_ast::CiString::new(name),
        offset,
        state: tidb_model::SchemaState::PUBLIC,
        generated_expr_string: generated.to_owned(),
        generated_stored: stored,
        ..Default::default()
    };
    let table = tidb_model::TableInfo {
        name: tidb_ast::CiString::new("test_table"),
        columns: vec![
            column("c1", 0, "", false),
            column("c2", 1, "", false),
            column("c3", 2, "c1 + c2", false),
            column("c4", 3, "c1 + c2", true),
        ]
        .into(),
        ..Default::default()
    };
    let index = tidb_model::IndexInfo {
        condition_expr_string: "c3 > 50".to_owned(),
        ..Default::default()
    };
    let columns = crate::kv_table::extract_columns_from_index_condition(&index, &table, true)
        .expect("condition columns");
    let names = columns
        .iter()
        .map(|column| column.read().name.original().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(names, ["c1", "c2", "c3"]);
}

/// `pkg/table/tables/index_test.go:661::TestDedupIndexColumns4Test`.
#[test]
fn test_dedup_index_columns4_test() {
    let columns = (0..4)
        .map(|offset| {
            tidb_model::GoShared::new(tidb_model::IndexColumn {
                name: tidb_ast::CiString::new(format!("c{offset}")),
                offset,
                ..Default::default()
            })
        })
        .collect::<Vec<_>>();
    let input = vec![
        columns[0].clone(),
        columns[1].clone(),
        columns[0].clone(),
        columns[2].clone(),
        columns[1].clone(),
        columns[3].clone(),
    ];
    let output = crate::kv_table::dedup_index_columns(input);
    assert_eq!(output.len(), 4);
    for (actual, expected) in output.iter().zip(columns) {
        assert!(actual.ptr_eq(&expected));
    }
}

/// `pkg/table/tables/index_test.go:684::TestPartialIndexDMLDuringDDL`.
#[test]
#[ignore = "go-parity-gap: partial-index DDL transition and concurrent DML are not transcreated"]
fn test_partial_index_dml_during_ddl() {}

/// `pkg/table/tables/index_test.go:732::TestPartialIndexDMLUniqueness`.
#[test]
#[ignore = "go-parity-gap: partial-index uniqueness is not maintained by this executor tier"]
fn test_partial_index_dml_uniqueness() {}

/// `pkg/table/tables/main_test.go:24::TestMain`.
#[test]
#[ignore = "go-parity-gap: Go test-main/goleak harness is not a Rust behavior test surface"]
fn test_tables_main() {}

/// `pkg/table/tables/mutation_checker_test.go:38::TestCompareIndexData`.
#[test]
#[ignore = "go-parity-carrier: index consistency comparison is covered by executor admin-check tests"]
fn test_compare_index_data() {}

/// `pkg/table/tables/mutation_checker_test.go:91::TestCheckRowInsertionConsistency`.
#[test]
#[ignore = "go-parity-gap: mutation-checker transaction inspection is not exposed by the executor API"]
fn test_check_row_insertion_consistency() {}

/// `pkg/table/tables/mutation_checker_test.go:176::TestCheckIndexKeysAndCheckHandleConsistency`.
#[test]
#[ignore = "go-parity-gap: mutation-checker key/handle inspection needs the Go transaction buffer"]
fn test_check_index_keys_and_check_handle_consistency() {}

/// `pkg/table/tables/state_remote_test.go:36::TestStateRemote`.
#[test]
#[ignore = "go-parity-gap: StateRemote is a Go table/storage remote-state interface"]
fn test_state_remote() {}

/// `pkg/table/tables/tables_test.go:77::TestBasic`.
#[test]
fn test_basic() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int primary key auto_increment, b varchar(255) unique)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, 'abc')",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    run_update_on(
        "update t set b = 'cba' where a = 1",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    let rows = run_select_on("select a, b from t", &catalog, &query_ctx()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], tidb_datatype::Datum::Int(1));
    run_create_table_on("create table t2 (a int)", &mut catalog).unwrap();
    crate::run_delete_on("delete from t", &mut catalog, &query_ctx()).unwrap();
    assert_eq!(
        run_select_on("select count(*) from t", &catalog, &query_ctx()).unwrap(),
        vec![vec![tidb_datatype::Datum::Int(0)]]
    );
}

/// `pkg/table/tables/tables_test.go:179::TestTypes`.
#[test]
fn test_types() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 text, c6 blob, c7 varchar(64), c10 decimal(10,1))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, 2, 3, 4, '5', '6', '7', 1.4)",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    let rows = run_select_on(
        "select c1, c5, c10 from t where c1 = 1",
        &catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 3);
}

/// `pkg/table/tables/tables_test.go:231::TestUniqueIndexMultipleNullEntries`.
#[test]
fn test_unique_index_multiple_null_entries() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int primary key, b varchar(255) unique)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, null), (2, null)",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on("select count(*) from t", &catalog, &query_ctx()).unwrap(),
        vec![vec![tidb_datatype::Datum::Int(2)]]
    );
}

/// `pkg/table/tables/tables_test.go:271::TestRowKeyCodec`.
#[test]
fn test_row_key_codec() {
    use tidb_codec::table_key::{
        RecordHandle, decode_record_key, decode_row_key, encode_row_key_with_handle,
    };

    for (table_id, handle) in [(1, 1_234_567_890), (2, 1), (3, -1), (4, -1)] {
        let key = encode_row_key_with_handle(table_id, &RecordHandle::Int(handle));
        assert_eq!(
            decode_record_key(&key),
            Ok((table_id, RecordHandle::Int(handle)))
        );
        assert_eq!(decode_row_key(&key), Ok(RecordHandle::Int(handle)));
    }
    for invalid in [
        "",
        "x",
        "t1",
        "t12345678",
        "t12345678_i",
        "t12345678_r1",
        "t12345678_r1234567",
    ] {
        assert!(
            decode_row_key(invalid.as_bytes()).is_err(),
            "invalid: {invalid:?}"
        );
    }
}

/// `pkg/table/tables/tables_test.go:312::TestUnsignedPK`.
#[test]
fn test_unsigned_pk() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a bigint unsigned primary key, b varchar(255))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, 'abc')",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    let rows = run_select_on("select a, b from t", &catalog, &query_ctx()).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], tidb_datatype::Datum::UInt(1)));
}

/// `pkg/table/tables/tables_test.go:335::TestIterRecords`.
#[test]
fn test_iter_records() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int primary key, b int)", &mut catalog).unwrap();
    run_insert_on(
        "insert into t values (-1, 2), (2, null)",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on("select count(*) from t", &catalog, &query_ctx()).unwrap(),
        vec![vec![tidb_datatype::Datum::Int(2)]]
    );
}

/// `pkg/table/tables/tables_test.go:360::TestTableFromMeta`.
#[test]
#[ignore = "go-parity-gap: TableFromMeta allocator and partial-condition metadata API is not exposed here"]
fn test_table_from_meta() {}

/// `pkg/table/tables/tables_test.go:412::TestTableFromMetaWithCollateUsesFixedMode`.
#[test]
#[ignore = "go-parity-gap: table metadata collation snapshot construction is not exposed by the executor driver"]
fn test_table_from_meta_with_collate_uses_fixed_mode() {}

/// `pkg/table/tables/tables_test.go:452::TestHiddenColumn`.
#[test]
#[ignore = "go-parity-gap: complete hidden/generated-column metadata and SHOW surface needs session infoschema"]
fn test_hidden_column() {}

/// `pkg/table/tables/tables_test.go:617::TestAddRecordWithCtx`.
#[test]
#[ignore = "go-parity-carrier: option/context propagation is covered by kv_table::tests::test_options"]
fn test_add_record_with_ctx() {}

/// `pkg/table/tables/tables_test.go:657::TestConstraintCheckForUniqueIndex`.
#[test]
fn test_constraint_check_for_unique_index() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (id int primary key, k int not null, c varchar(20) not null, unique key uk(k, c))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, 1, 'tidb'), (2, 2, 'tidb')",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert!(
        run_update_on(
            "update t set k = 1 where id = 2",
            &mut catalog,
            &query_ctx()
        )
        .is_err()
    );
}

/// `pkg/table/tables/tables_test.go:714::TestViewColumns`.
#[test]
#[ignore = "go-parity-gap: view column information_schema queries are owned by tidb-session"]
fn test_view_columns() {}

/// `pkg/table/tables/tables_test.go:758::TestConstraintCheckForOptimisticUntouched`.
#[test]
#[ignore = "go-parity-gap: optimistic transaction commit conflict checking is owned by tidb-session"]
fn test_constraint_check_for_optimistic_untouched() {}

/// `pkg/table/tables/tables_test.go:782::TestTxnAssertion`.
#[test]
#[ignore = "go-parity-gap: transaction assertion failpoints and assertion-level session variables are not transcreated"]
fn test_txn_assertion() {}

/// `pkg/table/tables/tables_test.go:978::TestSkipWriteUntouchedIndices`.
#[test]
#[ignore = "go-parity-gap: exact transaction-buffer index write accounting is not exposed by the executor driver"]
fn test_skip_write_untouched_indices() {}

/// `pkg/table/tables/tables_test.go:1173::TestDupKeyCheckMode`.
#[test]
#[ignore = "go-parity-gap: duplicate-key check modes require Go transaction and pessimistic-lock state"]
fn test_dup_key_check_mode() {}
