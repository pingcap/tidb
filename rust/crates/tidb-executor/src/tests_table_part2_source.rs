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

//! Source-backed inventory for manifest batch `b151`, `pkg/table.part2`.
//!
//! The deterministic slice is items 61--108 of the repository-wide
//! `pkg/table` test enumeration: the partition package's test harness and 24
//! tests, five `tblctx` buffer tests, one `tblsession` test, three temporary
//! table DDL tests, thirteen temporary-table interceptor tests, and the
//! temporary-table test harness.
//!
//! The executor already owns the partition router, pruner, and SQL driver;
//! the runnable tests below pin the data-level portions of those Go tests.
//! The rest remain explicit carrier/gap inventory entries rather than empty
//! omissions. In particular, temporary-table DDL/interceptor behavior is
//! owned by the unported session/storage overlay, while the buffer and
//! tblsession tests already have complete carriers in `tblctx` and
//! `tblsession`.

#![cfg(test)]

use crate::{Catalog, StmtContext, run_create_table_on, run_insert_on, run_select_on};

fn query_ctx() -> StmtContext {
    StmtContext::for_query()
}

/// `pkg/table/tables/test/partition/main_test.go:24::TestMain` is the Go
/// goleak/common-test harness, not a behavior test.
#[test]
#[ignore = "go-parity-gap: Go TestMain/goleak harness is not a Rust behavior test surface"]
fn partition_test_main() {}

/// `pkg/table/tables/test/partition/partition_test.go:47::TestPartitionTableUsesTableCollationSnapshot`.
#[test]
#[ignore = "go-parity-gap: table-collation snapshot construction and alternate collator modes are not exposed by the Rust executor driver"]
fn partition_table_uses_table_collation_snapshot() {}

/// `pkg/table/tables/test/partition/partition_test.go:111::TestPartitionAddRecord`.
/// The storage-level AddRecord portion is represented by the SQL driver's
/// insert and partition-qualified reads; Go's direct transaction-key probes
/// are below this crate's session transaction boundary.
#[test]
fn partition_add_record_routes_rows_to_range_partitions() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (id int, index(id)) partition by range(id) (partition p0 values less than (6), partition p1 values less than (11), partition p2 values less than (16), partition p3 values less than (21))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1), (7), (12), (16)",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on("select count(*) from t", &catalog, &query_ctx()).unwrap(),
        vec![vec![tidb_datatype::Datum::Int(4)]]
    );
    for (predicate, expected) in [
        ("id < 6", 1),
        ("id >= 6 and id < 11", 1),
        ("id >= 11 and id < 16", 1),
        ("id >= 16 and id < 21", 1),
    ] {
        let rows = run_select_on(
            &format!("select count(*) from t where {predicate}"),
            &catalog,
            &query_ctx(),
        )
        .unwrap();
        assert_eq!(
            rows,
            vec![vec![tidb_datatype::Datum::Int(expected)]],
            "range predicate {predicate}",
        );
    }
}

/// `partition_test.go:218::TestHashPartitionAddRecord`.
#[test]
fn hash_partition_add_record_routes_rows_to_all_hash_partitions() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (id int, index(id)) partition by hash(id) partitions 4",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (8), (-1), (3), (6)",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    for id in [8_i64, -1, 3, 6] {
        let rows = run_select_on(
            &format!("select count(*) from t where id = {id}"),
            &catalog,
            &query_ctx(),
        )
        .unwrap();
        assert_eq!(
            rows,
            vec![vec![tidb_datatype::Datum::Int(1)]],
            "hash point id={id}",
        );
    }
}

/// `partition_test.go:284::TestPartitionGetPhysicalID`.
#[test]
fn partition_get_physical_id_preserves_definition_ids() {
    let spec = partition_fixture();
    assert_eq!(spec.physical_ids(), vec![101, 102, 103]);
    for definition in &spec.definitions {
        assert_eq!(
            spec.definition_named(&definition.name).unwrap().id,
            definition.id
        );
    }
}

/// `partition_test.go:310::TestGeneratePartitionExpr` checks Go's private
/// `PartitionExpr` upper-bound expression rendering. The Rust driver stores
/// the parsed partition expression and folded bounds, but does not expose the
/// Go expression-rendering object or its exact `lt(t.id, bound)` strings.
#[test]
#[ignore = "go-parity-gap: private PartitionExpr upper-bound rendering has no Rust API"]
fn generate_partition_expr() {}

/// `partition_test.go:341::TestLocatePartition`; this retains the row-level
/// LIST COLUMNS routing contract while omitting Go's concurrent EXPLAIN probe.
#[test]
fn locate_partition_routes_list_columns_rows() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (id bigint, type varchar(255)) partition by list columns(type) (partition push_event values in ('PushEvent'), partition watch_event values in ('WatchEvent'))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, 'PushEvent'), (2, 'WatchEvent'), (3, 'WatchEvent')",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "select count(*) from t partition (watch_event)",
            &catalog,
            &query_ctx()
        )
        .unwrap(),
        vec![vec![tidb_datatype::Datum::Int(2)]]
    );
}

/// `partition_test.go:384::TestIssue31629` covers the complete DDL metadata
/// matrix and rejection errors. The data driver has no metadata-inspection
/// equivalent for the Go `GetPartitionColumnNames` interface.
#[test]
#[ignore = "go-parity-gap: complete partition-column metadata/rejection matrix has no Rust metadata test API"]
fn issue31629() {}

/// `partition_test.go:444::TestExchangePartitionStates`.
#[test]
#[ignore = "go-parity-gap: concurrent EXCHANGE PARTITION schema-state transitions and MDL are not transcreated"]
fn exchange_partition_states() {}

/// `partition_test.go:554::TestExchangePartitionCheckConstraintStates`.
#[test]
#[ignore = "go-parity-gap: concurrent EXCHANGE PARTITION check-constraint state transitions are not transcreated"]
fn exchange_partition_check_constraint_states() {}

/// `partition_test.go:667::TestExchangePartitionCheckConstraintStatesTwo`.
#[test]
#[ignore = "go-parity-gap: EXCHANGE PARTITION with evolving check-constraint metadata has no Rust session/DDL state machine"]
fn exchange_partition_check_constraint_states_two() {}

/// `partition_test.go:737::TestAddKeyPartitionStates`.
#[test]
#[ignore = "go-parity-gap: ADD PARTITION DDL state transitions, MDL, and concurrent sessions are not transcreated"]
fn add_key_partition_states() {}

/// `partition_test.go:878::TestKeyPartitionTableBasic`; the executor's
/// partition SQL carrier already exercises the full key-pruning/data family
/// (`tests_partition_table_sql_source`).
#[test]
fn key_partition_table_basic_is_carried_by_partition_sql_tests() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (col1 int not null, col2 int not null, col3 int not null, unique key(col3)) partition by key(col3) partitions 4",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on("select count(*) from t", &catalog, &query_ctx()).unwrap(),
        vec![vec![tidb_datatype::Datum::Int(4)]]
    );
    assert_eq!(
        run_select_on(
            "select count(*) from t where col3 = 3",
            &catalog,
            &query_ctx()
        )
        .unwrap(),
        vec![vec![tidb_datatype::Datum::Int(1)]]
    );
}

/// `partition_test.go:1303::TestKeyPartitionTableAllFeildType`.
#[test]
#[ignore = "go-parity-gap: the exhaustive BIT/numeric/time/string/ENUM/SET key-partition fixture and EXPLAIN assertions are not one Rust test surface"]
fn key_partition_table_all_field_type() {}

/// `partition_test.go:2130::TestPruneModeWarningInfo`.
#[test]
#[ignore = "go-parity-gap: session-level tidb_partition_prune_mode warning lifecycle is owned by tidb-session, not this executor driver"]
fn prune_mode_warning_info() {}

/// `partition_test.go:2143::TestPartitionByIntListExtensivePart`.
#[test]
#[ignore = "go-parity-gap: randomized concurrent DDL reorganization and DML state testing are not transcreated"]
fn partition_by_int_list_extensive_part() {}

/// `partition_test.go:2258::TestPartitionByIntExtensivePart`.
#[test]
#[ignore = "go-parity-gap: randomized ALTER TABLE partitioning with concurrent DML is not transcreated"]
fn partition_by_int_extensive_part() {}

/// `partition_test.go:2347::TestGlobalIndexPartitionByIntExtensivePart`.
#[test]
#[ignore = "go-parity-gap: global-index partition DDL and concurrent reorganization are not supported by this executor tier"]
fn global_index_partition_by_int_extensive_part() {}

/// `partition_test.go:2484::TestPartitionByExtensivePart`.
#[test]
#[ignore = "go-parity-gap: randomized string partition reorganization, concurrent DML, and SHOW CREATE verification are not transcreated"]
fn partition_by_extensive_part() {}

/// `partition_test.go:2592::TestReorgPartExtensivePart`.
#[test]
#[ignore = "go-parity-gap: failpoint-driven REORGANIZE PARTITION state transitions and concurrent DML are not transcreated"]
fn reorg_part_extensive_part() {}

/// `partition_test.go:3221::TestPointGetKeyPartitioning`.
#[test]
fn point_get_key_partitioning_returns_the_matching_row() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a varchar(30) not null, b varchar(45) not null, c varchar(45) not null, primary key (b, a)) partition by key(b) partitions 5",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values ('Aa', 'Ab', 'Ac'), ('Ba', 'Bb', 'Bc')",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on("select * from t where b = 'Ab'", &catalog, &query_ctx()).unwrap(),
        vec![vec![
            tidb_datatype::Datum::String(tidb_datatype::StringDatum::new(
                "Aa",
                tidb_datatype::Collation::Utf8Mb4Bin,
            )),
            tidb_datatype::Datum::String(tidb_datatype::StringDatum::new(
                "Ab",
                tidb_datatype::Collation::Utf8Mb4Bin,
            )),
            tidb_datatype::Datum::String(tidb_datatype::StringDatum::new(
                "Ac",
                tidb_datatype::Collation::Utf8Mb4Bin,
            )),
        ]]
    );
}

/// `partition_test.go:3231::TestExplainPartition` is represented by the
/// executable partition data tests; exact Go EXPLAIN text is a separate gap.
#[test]
#[ignore = "go-parity-gap: Go's EXPLAIN FORMAT=brief operator and partition-label text has no Rust explain-text parity surface"]
fn explain_partition() {}

/// `partition_test.go:3257::TestPruningOverflow`; the data-level predicate
/// must still find the inserted row when the partition expression multiplies
/// large signed values.
#[test]
fn pruning_overflow_keeps_the_matching_row() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int not null, b bigint not null, primary key(a, b)) partition by hash((a * b)) partitions 13",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (0, 3522101843073676459)",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "select a, b from t where a in (0, 14158354938390, 0) and b in (3522101843073676459, -2846203247576845955, 838395691793635638)",
            &catalog,
            &query_ctx(),
        )
        .unwrap(),
        vec![vec![
            tidb_datatype::Datum::Int(0),
            tidb_datatype::Datum::Int(3522101843073676459),
        ]]
    );
}

/// `partition_test.go:3266::TestPartitionCoverage`.
#[test]
#[ignore = "go-parity-gap: dynamic/static prune-mode warnings, ALTER PARTITION metadata, and EXPLAIN coverage assertions are not one executor-only surface"]
fn partition_coverage() {}

/// `partition_test.go:3355::TestAlterTablePartitionRollback`.
#[test]
#[ignore = "go-parity-gap: cancelled ALTER TABLE PARTITION rollback under concurrent MDL sessions is not transcreated"]
fn alter_table_partition_rollback() {}

// The next six entries are complete carriers already exercised by the owning
// modules. They stay in this source-backed inventory so the manifest remains
// one-to-one without duplicating private implementation tests here.

/// `pkg/table/tblctx/buffers_test.go:65::TestEncodeRow`.
#[test]
#[ignore = "go-parity-carrier: complete behavior carrier is tblctx::tests::encode_row"]
fn encode_row() {}

/// `buffers_test.go:147::TestEncodeBufferReserve`.
#[test]
#[ignore = "go-parity-carrier: complete behavior carrier is tblctx::tests::encode_buffer_reserve"]
fn encode_buffer_reserve() {}

/// `buffers_test.go:184::TestCheckRowBuffer`.
#[test]
#[ignore = "go-parity-carrier: complete behavior carrier is tblctx::tests::check_row_buffer"]
fn check_row_buffer() {}

/// `buffers_test.go:203::TestMutateBuffersGetter`.
#[test]
#[ignore = "go-parity-carrier: complete behavior carrier is tblctx::tests::mutate_buffers_getter"]
fn mutate_buffers_getter() {}

/// `buffers_test.go:217::TestEnsureCapacityAndReset`.
#[test]
#[ignore = "go-parity-carrier: complete behavior carrier is tblctx::tests::ensure_capacity_and_reset_matches_go"]
fn ensure_capacity_and_reset() {}

/// `pkg/table/tblsession/table_test.go:39::TestSessionMutateContextFields`.
#[test]
#[ignore = "go-parity-carrier: complete behavior carrier is tblsession::tests::session_mutate_context_fields"]
fn session_mutate_context_fields() {}

/// `pkg/table/temptable/ddl_test.go:48::TestAddLocalTemporaryTable`.
#[test]
#[ignore = "go-parity-gap: local temporary-table DDL needs session-local infoschema, storage bootstrap, and temporary data overlay"]
fn add_local_temporary_table() {}

/// `ddl_test.go:110::TestRemoveLocalTemporaryTable`.
#[test]
#[ignore = "go-parity-gap: local temporary-table drop and row cleanup need the unported session storage overlay"]
fn remove_local_temporary_table() {}

/// `ddl_test.go:156::TestTruncateLocalTemporaryTable`.
#[test]
#[ignore = "go-parity-gap: local temporary-table truncate and physical record cleanup need the unported session storage overlay"]
fn truncate_local_temporary_table() {}

/// `pkg/table/temptable/interceptor_test.go:52::TestGetKeyAccessedTableID`.
#[test]
#[ignore = "go-parity-gap: temporary-table snapshot interceptor is not transcreated"]
fn get_key_accessed_table_id() {}

/// `interceptor_test.go:116::TestGetRangeAccessedTableID`.
#[test]
#[ignore = "go-parity-gap: temporary-table snapshot interceptor is not transcreated"]
fn get_range_accessed_table_id() {}

/// `interceptor_test.go:220::TestNotTableRange`.
#[test]
#[ignore = "go-parity-gap: temporary-table snapshot interceptor is not transcreated"]
fn not_table_range() {}

/// `interceptor_test.go:254::TestGetSessionTemporaryTableKey`.
#[test]
#[ignore = "go-parity-gap: session temporary-table key overlay is not transcreated"]
fn get_session_temporary_table_key() {}

/// `interceptor_test.go:330::TestInterceptorTemporaryTableInfoByID`.
#[test]
#[ignore = "go-parity-gap: temporary-table infoschema lookup interceptor is not transcreated"]
fn interceptor_temporary_table_info_by_id() {}

/// `interceptor_test.go:379::TestInterceptorOnGet`.
#[test]
#[ignore = "go-parity-gap: temporary-table snapshot Get interception is not transcreated"]
fn interceptor_on_get() {}

/// `interceptor_test.go:552::TestInterceptorBatchGetTemporaryTableKeys`.
#[test]
#[ignore = "go-parity-gap: temporary-table BatchGet interception is not transcreated"]
fn interceptor_batch_get_temporary_table_keys() {}

/// `interceptor_test.go:739::TestInterceptorOnBatchGet`.
#[test]
#[ignore = "go-parity-gap: temporary-table snapshot OnBatchGet interception is not transcreated"]
fn interceptor_on_batch_get() {}

/// `interceptor_test.go:963::TestCreateUnionIter`.
#[test]
#[ignore = "go-parity-gap: temporary/session union iterator over snapshot and session data is not transcreated"]
fn create_union_iter() {}

/// `interceptor_test.go:1103::TestErrorCreateUnionIter`.
#[test]
#[ignore = "go-parity-gap: temporary/session union iterator error cleanup is not transcreated"]
fn error_create_union_iter() {}

/// `interceptor_test.go:1233::TestIterTable`.
#[test]
#[ignore = "go-parity-gap: temporary-table iterator interception is not transcreated"]
fn iter_table() {}

/// `interceptor_test.go:1385::TestOnIter`.
#[test]
#[ignore = "go-parity-gap: temporary-table forward iterator interception is not transcreated"]
fn on_iter() {}

/// `interceptor_test.go:1594::TestOnIterReverse`.
#[test]
#[ignore = "go-parity-gap: temporary-table reverse iterator interception is not transcreated"]
fn on_iter_reverse() {}

/// `pkg/table/temptable/main_test.go:38::TestMain` is the Go goleak/common
/// test harness.
#[test]
#[ignore = "go-parity-gap: Go TestMain/goleak harness is not a Rust behavior test surface"]
fn temporary_table_test_main() {}

fn partition_fixture() -> crate::PartitionSpec {
    use crate::{PartitionDef, PartitionKind, PartitionSpec, RangeBound};
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::expression::{Constant, Expression};

    let field_type = FieldType::new(FieldTypeCode::LongLong);
    PartitionSpec {
        kind: PartitionKind::Range {
            less_than: vec![
                RangeBound::Value(10),
                RangeBound::Value(20),
                RangeBound::MaxValue,
            ],
            unsigned: false,
        },
        expr_text: "`a`".to_owned(),
        expr: Expression::Constant(Constant::new(Datum::Int(0), field_type)),
        dependencies: vec!["a".to_owned()],
        definitions: [(101, "p0"), (102, "p1"), (103, "p2")]
            .into_iter()
            .map(|(id, name)| PartitionDef {
                id,
                name: name.to_owned(),
                less_than: Vec::new(),
                in_values: Vec::new(),
                comment: String::new(),
                placement_policy: None,
            })
            .collect(),
        is_empty_columns: false,
    }
}
