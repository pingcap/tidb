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

//! Ports of Go `pkg/executor/test/executor/executor_test.go` items 84–1072
//! (the metadata / session-variable / admin-check slice of the suite).
//!
//! SCOPE NOTE. The Go tests in this range drive a mock TiKV through full
//! sessions and assert on session variables (`@@tidb_current_ts`,
//! `@@tidb_last_txn_info`), kv-request hooks, coprocessor DAG fields,
//! failpoints, and the DDL job history — surfaces this tier's local executor
//! does not model. Each gap is recorded as an `#[ignore]` test citing the Go
//! source; the contracts the local engine CAN answer (ADMIN CHECK's
//! consistency errors, hash-partition reads, the duplicate-entry error
//! shape, the `any_value` column-flag regression) are running tests.

use crate::{admin_check, run_create_table_on, run_insert_on, run_select_on, Catalog, StmtContext};

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

fn kv_table_of(catalog: &Catalog, name: &str) -> crate::kv_table::KvTable {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in("test", name) else {
        panic!("table {name} is not stored as bytes");
    };
    table.clone()
}

/// Go `executor_test.go:84::TestTimezonePushDown`: the system time-zone NAME
/// (never the literal `System`) must be pushed into the coprocessor
/// `DAGRequest.TimeZoneName` for every pushdowned read.
///
/// go-parity-gap: the assertion lives on the marshalled `tipb.DAGRequest`
/// (`req.Data` unmarshalled in the `CheckSelectRequestHook`); this tier has
/// no coprocessor request to inspect.
#[test]
#[ignore = "go-parity-gap: DAGRequest time-zone pushdown field unported (no coprocessor request)"]
fn timezone_push_down_names_the_system_zone_in_the_dag_request() {}

/// Go `executor_test.go:115::TestNotFillCacheFlag`: `SQL_NO_CACHE` sets
/// `kv.Request.NotFillCache`, `SQL_CACHE` and the plain form clear it.
///
/// go-parity-gap: `NotFillCache` is a `kv.Request` field (`pkg/kv/key.go`),
/// set by `SelectRequestBuilder` and read by the client cache layer; no
/// request object exists here.
#[test]
#[ignore = "go-parity-gap: kv.Request.NotFillCache flag unported"]
fn not_fill_cache_flag_follows_the_sql_no_cache_hint() {}

/// Go `executor_test.go:146::TestCheckIndex`, ported at the
/// [`admin_check::check_table`] boundary the `ADMIN CHECK INDEX` statement
/// drives (Go `pkg/executor/check_table_index.go:110::CheckTableExec.Next` ->
/// `pkg/util/admin/admin.go::CheckIndicesCount`/`CheckRecordAndIndex`).
///
/// Go corrupts the table through `idx.Create`/`idx.Delete` on a raw txn; the
/// same corruption is made here through the raw-key seam
/// ([`crate::kv_table::KvTable::delete_record_for_test`] /
/// [`crate::kv_table::KvTable::delete_raw_key_for_test`]), which leaves the
/// store byte-identical to a half-applied write. The contracts pinned:
/// a consistent table passes under both spellings of the index name, an
/// unknown index is an error containing Go's "does not exist" wording at the
/// SQL boundary (the session arm renders `secondary index … does not exist`,
/// `rust/crates/tidb-session/src/admin_check_arm.rs:215`), a row missing its
/// record while the entry survives is the 8223 `Inconsistent` shape naming
/// the handle and both sides, and an entry missing while the row survives is
/// the 8003 count mismatch.
#[test]
fn admin_check_index_reports_each_corruption_shape() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table t (pk int primary key, c int default 1, c1 int default 1, unique key c(c))",
    );
    insert(&mut catalog, "insert into t (pk, c, c1) values (1, 10, 11), (2, 20, 21)");

    let context = crate::RowDecodeContext::for_test_query_utc();
    let mut consistent = kv_table_of(&catalog, "t");
    // Go: "admin check index t c" and "admin check index t C" both succeed.
    let checked = admin_check::check_table(&mut consistent, Some("c"), &context)
        .expect("consistent table passes the check");
    assert_eq!(checked, 1, "exactly the named index is checked");
    let checked = admin_check::check_table(&mut consistent, Some("C"), &context)
        .expect("index names resolve case-insensitively as in Go");
    assert_eq!(checked, 1);

    // Go: "admin check index t idx_inexistent" errors with "not exist".
    let unknown = admin_check::check_table(&mut consistent, Some("idx_inexistent"), &context)
        .expect_err("unknown index must be an error");
    assert!(
        matches!(unknown, admin_check::AdminCheckError::UnknownIndex { ref index, .. } if index == "idx_inexistent"),
        "unexpected error: {unknown:?}"
    );

    // Go arm: index (1,10),(2,20),(3,30) vs rows missing handle 3 ->
    // "data inconsistency in table: t, index: c, handle: 3,
    // index-values:\"handle: 3, values: [KindInt64 30]\" != record-values:\"\"".
    // The counts must AGREE for the check to drill into which side is wrong
    // (Go's `CheckIndicesCount` passes first), so the equivalent corruption
    // keeps two rows against two entries while pairing one entry with a
    // missing record: drop the record of handle 3 and the entry of handle 1.
    insert(&mut catalog, "insert into t (pk, c, c1) values (3, 30, 31)");
    let mut record_lost = kv_table_of(&catalog, "t");
    let entry_of = |table: &mut crate::kv_table::KvTable, handle: i64| {
        let index = table
            .index_list_for_check()
            .into_iter()
            .find(|index| index.name.eq_ignore_ascii_case("c"))
            .expect("index c");
        table
            .index_entries_for_check(index.id)
            .expect("entries")
            .into_iter()
            .find(|(_, entry_handle)| *entry_handle == crate::kv_table::TableHandle::Int(handle))
            .map(|(key, _)| key)
            .expect("entry for handle")
    };
    let drop_key = entry_of(&mut record_lost, 1);
    record_lost
        .delete_raw_key_for_test(&drop_key)
        .expect("entry removal");
    record_lost
        .delete_record_for_test(&crate::kv_table::TableHandle::Int(3))
        .expect("record removal");
    let inconsistent = admin_check::check_table(&mut record_lost, Some("c"), &context)
        .expect_err("orphaned index entry must be an error");
    let admin_check::AdminCheckError::Inconsistent {
        table,
        index,
        handle,
        index_values,
        record_values,
    } = inconsistent
    else {
        panic!("expected Inconsistent, got {inconsistent:?}");
    };
    assert_eq!(table, "t");
    assert_eq!(index, "c");
    assert_eq!(handle, "3", "the surviving entry names the dropped row");
    assert!(
        !index_values.is_empty(),
        "the entry side names the surviving entry"
    );
    assert_eq!(record_values, "", "the record side is empty, as in Go");

    // Go arm: more rows than entries -> the count check fails before the
    // scan (Go's "table count %d != index(%s) count %d", 8003).
    let mut entry_lost = kv_table_of(&catalog, "t");
    let index = entry_lost
        .index_list_for_check()
        .into_iter()
        .find(|index| index.name.eq_ignore_ascii_case("c"))
        .expect("index c");
    let entries = entry_lost
        .index_entries_for_check(index.id)
        .expect("index entries");
    assert_eq!(entries.len(), 3, "one entry per row");
    entry_lost
        .delete_raw_key_for_test(&entries[1].0)
        .expect("raw entry removal");
    let mismatch = admin_check::check_table(&mut entry_lost, Some("c"), &context)
        .expect_err("missing entry must be an error");
    let admin_check::AdminCheckError::CountMismatch {
        table_count,
        index,
        index_count,
    } = mismatch
    else {
        panic!("expected CountMismatch, got {mismatch:?}");
    };
    assert_eq!((table_count, index.as_str(), index_count), (3, "c", 2));
}

/// Go `executor_test.go:262::TestTimestampDefaultValueTimeZone`: a
/// `TIMESTAMP` column's default is stored in UTC and rendered in the SESSION
/// time zone, so `show create table` under `+08:00` / `+00:00` / `-08:00`
/// prints `14:46:14` / `06:46:14` / `22:46:14` for one stored instant, and
/// an existing column's rows re-render when the zone changes.
///
/// go-parity-gap: the pinned rendering happens in `SET time_zone` +
/// `SHOW CREATE TABLE`, neither of which this tier has (no session time-zone
/// statement surface, no show-create executor).
#[test]
#[ignore = "go-parity-gap: session time_zone statement + SHOW CREATE TABLE rendering unported"]
fn timestamp_default_value_renders_in_the_session_time_zone() {}

/// Go `executor_test.go:359::TestTiDBCurrentTS`: `@@tidb_current_ts` is 0
/// outside a transaction, equals the txn start ts inside, changes on
/// re-`begin`, returns to 0 after commit, and is read-only
/// (`ErrIncorrectScope` on assignment).
///
/// go-parity-gap: the variable needs the session transaction context
/// (`SessionVars.TxnCtx.StartTS`); this tier has no transaction surface.
#[test]
#[ignore = "go-parity-gap: @@tidb_current_ts / transaction context unported"]
fn tidb_current_ts_tracks_the_transaction() {}

/// Go `executor_test.go:384::TestTiDBLastTxnInfo`: `@@tidb_last_txn_info`
/// records the last committed/rolled-back transaction's start/commit ts,
/// survives reads, is untouched by point-get short paths (commit ts
/// `MaxUint64`), and is read-only.
///
/// go-parity-gap: needs committed transactions, `SessionVars.LastCommitTS`
/// and the session-variable read path.
#[test]
#[ignore = "go-parity-gap: @@tidb_last_txn_info / LastCommitTS session state unported"]
fn tidb_last_txn_info_records_each_transaction_outcome() {}

/// Go `executor_test.go:499::TestTiDBLastQueryInfo`: `@@tidb_last_query_info`
/// advances its start/for-update ts per statement, feeds the lock-wait view,
/// and coexists with `@@tidb_last_txn_info`.
///
/// go-parity-gap: needs the session's `TxnCtx` and pessimistic transactions.
#[test]
#[ignore = "go-parity-gap: @@tidb_last_query_info session state unported"]
fn tidb_last_query_info_advances_per_statement() {}

/// Go `executor_test.go:560::TestPartitionHashCode`: five concurrent
/// sessions each run `select * from t` five times over an empty table
/// hash-partitioned on its primary key; the test is a race regression for
/// the hash-partition routing code (`pkg/util/ranger`'s hash code reuse).
/// The rows must come back — empty — every time, with no panic.
#[test]
fn partition_hash_code_reads_are_stable_across_repeated_queries() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table t(c1 bigint, c2 bigint, c3 bigint, primary key(c1)) partition by hash (c1) partitions 4",
    );
    // Five Go sessions x five queries, run sequentially: the data contract
    // (empty result, no error) is what is pinned; the concurrent sessions
    // are Go's delivery mechanism for the race detector.
    for _ in 0..5 {
        for _ in 0..5 {
            let rows = run_select_on("select * from t", &catalog, &ctx())
                .expect("select over the hash-partitioned table");
            assert!(rows.is_empty(), "the Go table is never populated");
        }
    }
}

/// Go `executor_test.go:579::TestPrevStmtDesensitization` under
/// `tidb_redact_log=1`: the previous-statement digest redacts literal lists
/// (`insert into \`t\` values ( ... )`) and the duplicate-entry error masks
/// the offending value (`Duplicate entry '?' for key 't.a'`).
///
/// The observable core this tier owns is the duplicate-entry error itself:
/// inserting a colliding value into `t.a` fails naming the value and the
/// key. The redaction layer (`PrevStmt.String()` and the '?' masking driven
/// by `@@global.tidb_redact_log`) is the session's, and has no surface here.
#[test]
fn duplicate_entry_error_names_the_value_and_key() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t (a int, unique key (a))");
    insert(&mut catalog, "insert into t values (1),(2)");
    let error = run_insert_on("insert into t values (1)", &mut catalog, &ctx())
        .expect_err("duplicate key must fail");
    let sql_error = error.clone().to_mysql_error();
    assert_eq!(sql_error.code, 1062, "{error:?}");
    assert_eq!(sql_error.message, "Duplicate entry '1' for key 't.a'");
}

/// The redaction half of Go `executor_test.go:579::TestPrevStmtDesensitization`:
/// with `@@global.tidb_redact_log=1` the statement digest prints
/// `insert into \`t\` values ( ... )` and the 1062 message masks the value
/// as '?'.
#[test]
#[ignore = "go-parity-gap: tidb_redact_log PrevStmt digest redaction unported"]
fn prev_stmt_desensitization_masks_literals_under_redact_log() {}

/// Go `executor_test.go:593::TestIssue19148`: `where a > any_value(a)` must
/// not corrupt the referenced column's flag — Go requires
/// `tblInfo.Meta().Columns[0].GetFlag() == 0` after the query.
#[test]
fn any_value_comparison_leaves_the_column_flag_untouched() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t(a decimal(16, 2))");
    let rows = run_select_on("select * from t where a > any_value(a)", &catalog, &ctx())
        .expect("Go runs this query against the empty table");
    assert!(rows.is_empty());
    let table = kv_table_of(&catalog, "t");
    let flag = table.columns[0].field_type.flags();
    assert_eq!(flag, 0, "Go requires GetFlag() == 0 after the query");
}

/// Go `executor_test.go:608::TestUnreasonablyClose`: closing every operator
/// (`PhysicalHashJoin` … `PhysicalUnionAll`) WITHOUT calling `Open` first
/// must not panic, proven by building 21 SQL shapes through the planner and
/// calling `e.Close()` directly.
///
/// go-parity-gap: needs the planner→executor-builder seam
/// (`NewMockExecutorBuilderForTest`) exposed per plan shape; this tier only
/// runs whole statements through the driver.
#[test]
#[ignore = "go-parity-gap: per-operator executor build + close-without-open seam unported"]
fn unreasonably_close_without_open_never_panics() {}

/// Go `executor_test.go:747::TestTwiceCloseUnionExec`: a union/agg tree
/// closed twice must not leak, and a failpoint-injected Open error must
/// surface as `mock StreamAggExec.baseExecutor.Open returned error`.
///
/// go-parity-gap: needs the failpoint harness and the built executor tree.
#[test]
#[ignore = "go-parity-gap: failpoint-injected StreamAgg Open error + built tree double close unported"]
fn twice_close_union_exec_does_not_leak() {}

/// Go `executor_test.go:791::TestApplyCache`: `explain analyze` of an apply
/// over an indexed correlated subquery prints the apply-cache verdict in the
/// operator info — `cache:ON, cacheHitRatio:88.889%` when the outer side
/// repeats values (9 identical rows) and `cache:OFF` when all 9 are distinct.
///
/// go-parity-gap: needs `explain analyze` operator-info text with the apply
/// cache statistics (`ApplyCache` hit-ratio rendering).
#[test]
#[ignore = "go-parity-gap: explain-analyze apply-cache hit-ratio text unported"]
fn apply_cache_verdict_follows_outer_side_cardinality() {}

/// Go `executor_test.go:836::TestCollectDMLRuntimeStats`: DML statements
/// record root runtime stats (`time…loops…Get…num_rpc…total_time`,
/// `lock_keys…`, `check_insert {… prefetch … rpc {BatchGet …} }`) readable
/// through `ShowProcess`.
///
/// go-parity-gap: runtime stats collection is deferred in this tier (see the
/// crate doc's DEFERRED note).
#[test]
#[ignore = "go-parity-gap: DML runtime-stats collation strings unported"]
fn collect_dml_runtime_stats_strings() {}

/// Go `executor_test.go:905::TestTableSampleTemporaryTable`: `tablesample
/// regions()` over a GLOBAL temporary table returns an empty result (and
/// keeps doing so inside transactions and under `tidb_snapshot`), while over
/// a LOCAL temporary table it is rejected with `TABLESAMPLE clause can not
/// be applied to local temporary tables`.
///
/// go-parity-gap: the statement executor rejects `TABLESAMPLE` wholesale
/// (`Unsupported("TABLESAMPLE is not supported yet")`), so neither arm's
/// behavior is reachable yet.
#[test]
#[ignore = "go-parity-gap: TABLESAMPLE executor unported (global-temp empty read / local-temp 1174 error)"]
fn tablesample_on_temporary_tables() {}

/// Go `executor_test.go:958::TestGetResultRowsCount`:
/// `StmtCtx.GetResultRowsCount()` reports the result-set size of the LAST
/// statement (10 / 0 / 3 for the selects, 0 for insert/replace/update).
///
/// go-parity-gap: this tier's `StmtContext` carries warnings and memory
/// state but no result-rows counter; the row counts themselves are pinned by
/// the statement tests (e.g. `union2_matrix`).
#[test]
#[ignore = "go-parity-gap: StmtCtx.GetResultRowsCount counter unported"]
fn get_result_rows_count_of_the_last_statement() {}

/// Go `executor_test.go:992::TestAdminShowDDLJobs`: `admin show ddl jobs`
/// lists history jobs with schema names, supports `where job_type='create
/// table'`, and renders START/END_TIME in the session time zone.
///
/// go-parity-gap: needs the DDL job history table
/// (`mysql.tidb_ddl_history`) and the `admin show ddl jobs` executor.
#[test]
#[ignore = "go-parity-gap: admin show ddl jobs / history-job store unported"]
fn admin_show_ddl_jobs_lists_history() {}

/// Go `executor_test.go:1072::TestAdminShowDDLJobsInfo`: the `job_type`
/// column names each DDL flavor — `alter table placement`, `rename tables`,
/// `alter table partition placement`, `alter table cache`/`nocache`.
///
/// go-parity-gap: same missing surface as `admin_show_ddl_jobs_lists_history`;
/// the job_type labels are only observable through `admin show ddl jobs`.
#[test]
#[ignore = "go-parity-gap: admin show ddl jobs job_type labels unported"]
fn admin_show_ddl_jobs_reports_job_types() {}
