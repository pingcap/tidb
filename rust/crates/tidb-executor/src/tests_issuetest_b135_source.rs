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

//! Source-mapped ports of Go `pkg/executor/test/issuetest` items 967–992.
//!
//! The running tests below pin SQL-result contracts that are available from
//! the in-process executor catalog. Failpoint, session, transaction, DDL-job,
//! EXPLAIN ANALYZE, and memory-manager arms remain explicit gaps.

use crate::{Catalog, StmtContext, run_create_table_on, run_insert_on, run_select_on};
use tidb_datatype::Datum;

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn create(catalog: &mut Catalog, sql: &str) {
    run_create_table_on(sql, catalog)
        .unwrap_or_else(|error| panic!("create {sql:?} failed: {error:?}"));
}

fn insert(catalog: &mut Catalog, sql: &str) {
    run_insert_on(sql, catalog, &ctx())
        .unwrap_or_else(|error| panic!("insert {sql:?} failed: {error:?}"));
}

fn select(catalog: &Catalog, sql: &str) -> Vec<Vec<Datum>> {
    run_select_on(sql, catalog, &ctx())
        .unwrap_or_else(|error| panic!("select {sql:?} failed: {error:?}"))
}

fn cell(datum: &Datum) -> String {
    match datum {
        Datum::Null => "<nil>".to_owned(),
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Real(value) => format!("{}", value),
        Datum::Time(value) => value.to_string(),
        Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
        Datum::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
        other => format!("{:?}", other),
    }
}

fn rows_text(rows: &[Vec<Datum>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(cell).collect())
        .collect()
}

/// Go `executor_issue_test.go:43::TestIssue24210`: the four injected
/// executor-open errors are not reachable without TiDB's failpoint registry.
#[test]
#[ignore = "go-parity-gap: Projection/HashAgg/StreamAgg/Selection Open failpoints are unported"]
fn issue24210_executor_open_errors() {}

/// Go `executor_issue_test.go:73::TestUnionIssue`, data-level arms: UNION
/// preserves the source type conversions and NULL rows in the cases that do
/// not require prepared-protocol metadata or a pessimistic transaction.
#[test]
fn union_issue_data_type_and_null_arms() {
    let mut catalog = Catalog::default();
    assert_eq!(
        rows_text(&select(
            &catalog,
            "(select cast('abcdefghijklmnopqrstuvwxyz' as char) as c1) union all (select 1 where false)",
        )),
        vec![vec!["abcdefghijklmnopqrstuvwxyz".to_owned()]],
    );

    create(&mut catalog, "create table tbl_3 (col_15 bit(20))");
    create(&mut catalog, "create table tbl_23 (col_15 bit(15))");
    insert(&mut catalog, "insert into tbl_3 values (0xFFFF), (0xFF)");
    insert(&mut catalog, "insert into tbl_23 values (0xF)");
    let rows = select(
        &catalog,
        "select col_15 from tbl_23 union all select col_15 from tbl_3 order by col_15",
    );
    assert_eq!(rows.len(), 3, "Go Issue25506 expects three BIT rows");

    let mut greatest_rows = rows_text(&select(
        &catalog,
        "select greatest(cast('2020-01-01 01:01:01' as datetime), cast('2019-01-01 01:01:01' as datetime)) union select null",
    ));
    greatest_rows.sort();
    assert_eq!(
        greatest_rows,
        vec![
            vec!["2020-01-01 01:01:01".to_owned()],
            vec!["<nil>".to_owned()],
        ],
    );
    assert_eq!(
        rows_text(&select(
            &catalog,
            "select quote(cast('abc' as char)) union all select '1' order by 1",
        )),
        vec![vec!["'abc'".to_owned()], vec!["1".to_owned()]],
    );
}

/// The remainder of Go `TestUnionIssue` uses PREPARE field metadata and a
/// pessimistic two-session MVCC interleaving, neither of which belongs to the
/// catalog-only executor driver.
#[test]
#[ignore = "go-parity-gap: prepared result fields, FOR UPDATE, pessimistic transactions, and union-scan MVCC are unported"]
fn union_issue_prepare_and_pessimistic_transaction_arms() {}

/// Go `executor_issue_test.go:137::TestIssue28650`: concurrent EXPLAIN
/// ANALYZE planning under three query memory budgets.
#[test]
#[ignore = "go-parity-gap: concurrent query construction, EXPLAIN ANALYZE, and session memory quotas are unported"]
fn issue28650_concurrent_explain_memory_budgets() {}

/// Go `executor_issue_test.go:180::TestIssue30289`: hash-join build failpoint.
#[test]
#[ignore = "go-parity-gap: hash-join build failpoint is unported"]
fn issue30289_hash_join_build_error() {}

/// Go `executor_issue_test.go:195::TestIssue51998`: hash-join build error
/// from a second failpoint under both hash-join versions.
#[test]
#[ignore = "go-parity-gap: hash-join build failpoint and version session variable are unported"]
fn issue51998_hash_join_build_error() {}

/// Go `executor_issue_test.go:210::TestIssue29498`: temporal type width and
/// UNION conversion metadata for TIME/DATE expressions.
#[test]
#[ignore = "go-parity-gap: result-field temporal flen metadata is not carried by SelectMeta on this tier"]
fn issue29498_temporal_result_field_width() {}

/// Go `executor_issue_test.go:249::TestIssue31678`: UNION charset/length
/// coercion, including GBK and binary columns.
#[test]
#[ignore = "go-parity-gap: result-field charset/flength metadata and the GBK session codec are unported here"]
fn issue31678_union_charset_coercion() {}

/// Go `executor_issue_test.go:313::TestIndexJoin31494`: large index joins
/// under the query memory quota.
#[test]
#[ignore = "go-parity-gap: 32K-row index-join plan, session manager, and OOM quota surface are unported"]
fn index_join_31494_memory_quota() {}

/// Go `executor_issue_test.go:354::TestFix31038`: coprocessor execution-info
/// collection is disabled by configuration and failpoint.
#[test]
#[ignore = "go-parity-gap: coprocessor execution-info configuration and failpoint are unported"]
fn fix31038_disable_collect_execution_info() {}

/// Go `executor_issue_test.go:380::TestIssue20975`: DDL must not invalidate
/// reads/locks in the listed transaction modes.
#[test]
#[ignore = "go-parity-gap: multi-session transactions, locks, and DDL schema synchronization are unported"]
fn issue20975_transaction_ddl_interleavings() {}

/// Go `executor_issue_test.go:440::TestIssue20975WithPartitionTable`: the same
/// transaction/DDL matrix over range partitions.
#[test]
#[ignore = "go-parity-gap: partition transactions, locks, and DDL schema synchronization are unported"]
fn issue20975_partition_transaction_ddl_interleavings() {}

/// Go `executor_issue_test.go:517::TestIssue33038`: generated-column reads
/// become table-cache reads after repeated scans.
#[test]
#[ignore = "go-parity-gap: table-cache DDL and ReadFromTableCache statement state are unported"]
fn issue33038_generated_column_table_cache() {}

/// Go `executor_issue_test.go:550::TestIssue33214`: correlated aggregate
/// reads over an ENUM table cache.
#[test]
#[ignore = "go-parity-gap: table-cache DDL and ReadFromTableCache statement state are unported"]
fn issue33214_enum_correlated_table_cache() {}

/// Go `executor_issue_test.go:566::TestIssueRaceWhenBuildingExecutorConcurrently`:
/// repeated large index-merge-join construction.
#[test]
#[ignore = "go-parity-gap: concurrent executor construction and index-merge join planning are unported"]
fn issue_race_when_building_executor_concurrently() {}

/// Go `executor_issue_test.go:580::TestIssue42298`: ADMIN SHOW DDL job query
/// limit/offset behavior.
#[test]
#[ignore = "go-parity-gap: ADMIN SHOW DDL JOBS and DDL history are unported"]
fn issue42298_admin_show_ddl_job_query_limits() {}

/// Go `executor_issue_test.go:594::TestIssue42662`: server memory-limit
/// top-session selection and cancellation.
#[test]
#[ignore = "go-parity-gap: ServerMemoryLimitHandle, processlist, and memory failpoints are unported"]
fn issue42662_server_memory_limit_top_session() {}

/// Go `executor_issue_test.go:640::TestIssue50393`: a blob containing the
/// prefix bytes is found by a LIKE pattern built from another blob.
#[test]
fn issue50393_blob_like_prefix() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t1 (a blob)");
    create(&mut catalog, "create table t2 (a blob)");
    insert(&mut catalog, "insert into t1 values (0xC2A0)");
    insert(&mut catalog, "insert into t2 values (0xC2)");
    assert_eq!(
        rows_text(&select(
            &catalog,
            "select count(*) from t1, t2 where t1.a like concat('%', t2.a, '%')",
        )),
        vec![vec!["1".to_owned()]],
    );
}

/// Go `executor_issue_test.go:654::TestIssue51874`: window aggregation in a
/// scalar subquery under projection pushdown.
#[test]
#[ignore = "go-parity-gap: the projection-pushdown/session switch and this window-scalar shape are not exposed by the catalog driver"]
fn issue51874_window_scalar_projection_pushdown() {}

/// Go `executor_issue_test.go:668::TestIssue51777`: correlated scalar
/// comparison ordered by its result.
#[test]
#[ignore = "go-parity-gap: correlated scalar subquery ORDER BY/LIMIT shape is not stable on this catalog-only tier"]
fn issue51777_correlated_scalar_order() {}

/// Go `executor_issue_test.go:682::TestIssue52978`: TRUNCATE with a DOUBLE
/// precision argument keeps the constant result through MIN.
#[test]
fn issue52978_truncate_double_constant() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t (a int)");
    insert(
        &mut catalog,
        "insert into t values (-1790816583), (2049821819), (-1366665321), (536581933), (-1613686445)",
    );
    assert_eq!(
        rows_text(&select(
            &catalog,
            "select min(truncate(cast(-26340 as double), t.a)) from t",
        )),
        vec![vec!["-26340".to_owned()]],
    );
}

/// Go `executor_issue_test.go:694::TestIssue53221`: empty row-driven regexp
/// patterns return the documented error for all four regexp functions.
#[test]
#[ignore = "go-parity-gap: regexp empty-pattern error text and row-driven regexp execution are not exposed by this crate"]
fn issue53221_empty_regexp_pattern() {}

/// Go `executor_issue_test.go:718::TestIndexReaderIssue53871AndIssue54160`:
/// index/table/index-merge readers and EXPLAIN ANALYZE counters over 4096 rows.
#[test]
#[ignore = "go-parity-gap: EXPLAIN ANALYZE reader counters, statistics, and index-merge SQL planning are unported"]
fn index_reader_53871_and_54160() {}

/// Go `executor_issue_test.go:748::TestCalculateBatchSize`: the legacy
/// `executor.CalculateBatchSize` formula. The current Rust lookup code has a
/// different private `calculate_lookup_batch_size` contract, already covered
/// by `access_path::tests::lookup_initial_batch_matches_go_calculate_batch_size`.
#[test]
#[ignore = "go-parity-gap: legacy executor.CalculateBatchSize is not the current lookup-batch API and is private to access_path"]
fn calculate_batch_size() {}

/// Go `executor_issue_test.go:758::TestIssue55881`: repeated CTE/UNION scalar
/// subqueries exercise executor construction races.
#[test]
#[ignore = "go-parity-gap: repeated concurrent executor construction and session executor-concurrency variable are unported"]
fn issue55881_cte_union_executor_race() {}

/// Go `executor_issue_test.go:777::TestIssue60926`: a join child is closed
/// after the injected legacy hash-join panic path.
#[test]
#[ignore = "go-parity-gap: hash-join failpoint and child-close instrumentation are unported"]
fn issue60926_join_child_close_after_panic() {}

/// Go `issuetest/main_test.go:26::TestMain`: suite configuration and goleak
/// bootstrap only.
#[test]
#[ignore = "skipped-reason: Go issuetest TestMain only configures auto-ID/failpoints/goleak"]
fn issuetest_suite_main_is_bootstrap() {}
