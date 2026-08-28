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

//! Port of the ported slice of the nine `pkg/ddl` part-16 tests in
//! `pkg/ddl/tests/partition/reorg_partition_test.go`
//! (`TestReorgPartitionRollback` :915 through
//! `TestPartitionByFailuresAddPlacementPolicyGlobalIndex` :1181).
//!
//! Eight of the nine Go tests drive `testReorganizePartitionFailures`
//! (:361) or the concurrent state-machine walkers, which need the
//! online-DDL job queue, failpoint injection
//! (`github.com/pingcap/tidb/pkg/ddl/reorgPart*`), the GC worker and the
//! placement-rule bundle store. None of that exists in this tier — DDL
//! applies synchronously to metadata (see the `crate::ddl` module doc) — so
//! they are `#[ignore]` gap tests with the contract re-derived from the Go
//! source.
//!
//! The one portable slice is the CREATE-TABLE half of
//! `TestPartitionByColumnChecks` (:961), whose contract lives in Go
//! `checkPartitionFuncValid` (`pkg/ddl/partition.go:1845`),
//! `checkPartitionExprArgs`/`checkResultOK` (:4981-4991),
//! `checkPartitionFuncType` (:1872) and the LIST/RANGE-COLUMNS column-type
//! gates, all transcreated in `crate::ddl::table_partition`. Every row below
//! was measured against this engine; Go's expectations are
//! `reorg_partition_test.go:981-1015`.

use tidb_executor::{run_create_table_on, Catalog, DriverError};

/// The column set Go `serial_test.go:970` builds:
/// `i int, f float, c char(20), b bit(2), b32 bit(32), b64 bit(64), d date,
/// dt datetime, dt6 datetime(6), ts timestamp, ts6 timestamp(6), j json`.
const COLS: &str = "(i int, f float, c char(20), b bit(2), b32 bit(32), b64 bit(64), d date, \
dt datetime, dt6 datetime(6), ts timestamp, ts6 timestamp(6), j json)";

fn create_error(catalog: &mut Catalog, sql: &str) -> DriverError {
    run_create_table_on(sql, catalog)
        .map(|_| panic!("{sql} was expected to fail"))
        .expect_err("expected error")
}

/// Go `reorg_partition_test.go:981-1015`, the `create table tt <cols>
/// partition by <clause>` half of `TestPartitionByColumnChecks`: each clause
/// either builds or answers `dbterror.ErrNotAllowedTypeInPartition` (1659,
/// "Field '<col>' is of a not allowed type for this type of partitioning") or
/// `dbterror.ErrWrongExprInPartitionFunc` (1486, the timezone-dependent
/// expression message). The one divergence — Go accepts `list (c)` — is the
/// `#[ignore]` test below.
#[test]
fn partition_by_column_checks_create_half_matches_go_per_clause() {
    // (clause, expected): Err(None) = accepted, Err(1659, col) / Err(1486) =
    // Go's expected dbterror.
    let cases: Vec<(&str, Result<(), (u16, &str)>)> = vec![
        ("key (c) partitions 2", Ok(())),
        ("key (j) partitions 2", Err((1659, "j"))),
        // {"list (c) ...", nil} is the one diverging row; see the gap test.
        ("list (b) (partition pDef default)", Ok(())),
        ("list (f) (partition pDef default)", Err((1659, "f"))),
        ("list (j) (partition pDef default)", Err((1659, "j"))),
        ("list columns (b) (partition pDef default)", Err((1659, "b"))),
        ("list columns (f) (partition pDef default)", Err((1659, "f"))),
        ("list columns (ts) (partition pDef default)", Err((1659, "ts"))),
        ("list columns (j) (partition pDef default)", Err((1659, "j"))),
        ("hash (year(ts)) partitions 2", Err((1486, ""))),
        ("hash (ts) partitions 2", Err((1659, "ts"))),
        ("hash (ts6) partitions 2", Err((1659, "ts6"))),
        ("hash (d) partitions 2", Err((1659, "d"))),
        ("hash (f) partitions 2", Err((1659, "f"))),
        ("range (c) (partition pMax values less than (maxvalue))", Err((1659, "c"))),
        ("range (f) (partition pMax values less than (maxvalue))", Err((1659, "f"))),
        ("range (d) (partition pMax values less than (maxvalue))", Err((1659, "d"))),
        ("range (dt) (partition pMax values less than (maxvalue))", Err((1659, "dt"))),
        ("range (dt6) (partition pMax values less than (maxvalue))", Err((1659, "dt6"))),
        ("range (ts) (partition pMax values less than (maxvalue))", Err((1659, "ts"))),
        ("range (ts6) (partition pMax values less than (maxvalue))", Err((1659, "ts6"))),
        ("range (j) (partition pMax values less than (maxvalue))", Err((1659, "j"))),
        ("range columns (b) (partition pMax values less than (maxvalue))", Err((1659, "b"))),
        ("range columns (b64) (partition pMax values less than (maxvalue))", Err((1659, "b64"))),
        ("range columns (c) (partition pMax values less than (maxvalue))", Ok(())),
        ("range columns (f) (partition pMax values less than (maxvalue))", Err((1659, "f"))),
        ("range columns (d) (partition pMax values less than (maxvalue))", Ok(())),
        ("range columns (dt) (partition pMax values less than (maxvalue))", Ok(())),
        ("range columns (dt6) (partition pMax values less than (maxvalue))", Ok(())),
        ("range columns (ts) (partition pMax values less than (maxvalue))", Err((1659, "ts"))),
        ("range columns (ts6) (partition pMax values less than (maxvalue))", Err((1659, "ts6"))),
        ("range columns (j) (partition pMax values less than (maxvalue))", Err((1659, "j"))),
    ];
    assert_eq!(cases.len(), 32, "Go's table minus the diverging list (c) row");

    for (clause, expected) in cases {
        // Go builds `tt` once and `tt`-copies per clause; the tier's
        // statement names must be unique per catalog, so each clause runs on
        // a fresh catalog with both tables.
        let mut catalog = Catalog::default();
        run_create_table_on(&format!("create table t {COLS}"), &mut catalog)
            .expect("the plain column set builds");
        let sql = format!("create table tt {COLS} partition by {clause}");
        match expected {
            Ok(()) => {
                run_create_table_on(&sql, &mut catalog)
                    .unwrap_or_else(|error| panic!("{clause} should build: {error:?}"));
            }
            Err((code, column)) => {
                let error = create_error(&mut catalog, &sql);
                let rendered = error.clone().to_mysql_error();
                assert_eq!(rendered.code, code, "{clause}");
                let expected_message = match code {
                    1659 => format!(
                        "Field '{column}' is of a not allowed type for this type of partitioning"
                    ),
                    _ => "Constant, random or timezone-dependent expressions in (sub)partitioning \
                          function are not allowed"
                        .to_owned(),
                };
                assert_eq!(rendered.message, expected_message, "{clause}");
            }
        }
    }
}

/// Go `reorg_partition_test.go:990` `{"list (c) (partition pDef default)",
/// nil}`: LIST partitioning over a CHAR column with a `pDef default`
/// partition is ACCEPTED by Go (the list-default extension relaxes the
/// integer-type gate), and the clause is then re-run as
/// `alter table t partition by list (c) ...` with the same answer.
// go-parity-gap: this tier refuses `list (c)` with
// ErrNotAllowedTypeInPartition 1659 naming `c` — its LIST partition builder
// (crate::ddl::table_partition_list) only accepts the integer-family
// columns Go's plain LIST path accepts, and has no list-DEFAULT relaxation.
#[test]
#[ignore]
fn partition_by_list_char_column_with_default_partition_is_accepted() {
}

/// Go `reorg_partition_test.go:1017-1020`: every clause of the table is
/// re-run as `alter table t partition by <clause>` with the same expected
/// error, against a non-partitioned `t`.
// go-parity-gap: this tier's ALTER TABLE dispatch has no `PARTITION BY`
// (Repartition) carrier — the action answers its generic
// "this ALTER TABLE action is not supported yet" refusal (1105) instead of
// Go's per-clause verdicts.
#[test]
#[ignore]
fn partition_by_column_checks_alter_half_matches_go_per_clause() {
}

/// Go `reorg_partition_test.go:915-959::TestReorgPartitionRollback`: on a
/// range-partitioned table with data, `alter table t reorganize partition p1
/// into (partition p1a ..., partition p1b ...)` twice — first failing at
/// `mockUpdateVersionAndTableInfoErr` (return(1)), then at
/// `reorgPartitionAfterDataCopy` (return(true)) — must roll back cleanly:
/// `admin check table t` passes, `SHOW CREATE TABLE t` still prints the
/// ORIGINAL three partitions, no table data survives above the table's
/// highest physical id (`noNewTablesAfter`, :82), and the indices keep
/// their ids.
// go-parity-gap: REORGANIZE PARTITION is refused by this tier's ALTER
// dispatch, and the rollback contract needs the online-DDL job queue,
// failpoints and GC-worker range deletion.
#[test]
#[ignore]
fn reorg_partition_rollback_restores_original_partitioning() {
}

/// Go `reorg_partition_test.go:1045-1055::TestPartitionIssue56634`: with the
/// failpoint `updateVersionAndTableInfoErrInStateDeleteReorganization`
/// returning an error 4 times, `alter table t partition by range(a)
/// (partition p1 values less than (20))` still COMPLETES (issue #56634: a
/// partition-by ALTER past its last rollback point succeeds instead of
/// erroring, because StatePublic can no longer roll back).
// go-parity-gap: ALTER TABLE ... PARTITION BY is refused by this tier's
// ALTER dispatch, and the failure-injection harness needs the online-DDL job
// queue state machine.
#[test]
#[ignore]
fn partition_by_alter_survives_delete_reorganization_version_errors() {
}

/// Go `reorg_partition_test.go:1057-1077::TestReorgPartitionFailuresPlacementPolicy`:
/// with table- and partition-level placement policies bound (pp1 on the
/// table, pp2 on p1, pp3 on p2), a failing/cancelled/rolled-forward
/// REORGANIZE PARTITION of p1,p2 must keep every pre-alter placement bundle
/// byte-identical after rollback (`oldBundles`/`newBundles` comparison at
/// :487-500) and migrate the new p1b's `placement policy 'pp1'` ref on
/// success; DML during all phases keeps `select * from t` correct.
// go-parity-gap: REORGANIZE PARTITION, the job queue/failpoints and the PD
// rule-bundle store are all outside this tier.
#[test]
#[ignore]
fn reorg_partition_failures_placement_policy_bundles_survive_rollback() {
}

/// Go `reorg_partition_test.go:1078-1109::TestRemovePartitionFailuresPlacementPolicy`:
/// `alter table t remove partitioning` under injected failures with a table
/// policy pp3 and per-partition policies pp1/pp2 must roll back to the
/// original partitioned metadata and bundles, and on success converge to a
/// non-partitioned table with the rows intact.
// go-parity-gap: REMOVE PARTITIONING is refused by this tier's ALTER
// dispatch; the failure-injection harness needs the job queue and the
// rule-bundle store.
#[test]
#[ignore]
fn remove_partitioning_failures_placement_policy_bundles_survive_rollback() {
}

/// Go `reorg_partition_test.go:1110-1139::TestPartitionByFailuresPlacementPolicy`:
/// `alter table t partition by range (b) (...) update indexes (`primary`
/// global)` with a table policy pp1 and a p0 policy pp2, under injected
/// failures — rollback restores the old partitioning/bundles; success
/// rebuilds the table on the new partitioning with the pp3 policy on the new
/// first partition and the primary index global.
// go-parity-gap: ALTER TABLE ... PARTITION BY is refused by this tier's
// ALTER dispatch; the failure-injection harness needs the job queue, global
// index rebuild and the rule-bundle store.
#[test]
#[ignore]
fn partition_by_failures_placement_policy_rolls_back_and_rebuilds() {
}

/// Go `reorg_partition_test.go:1140-1165::TestPartitionNonPartitionedFailuresPlacementPolicy`:
/// partitioning a NON-partitioned table carrying a table-level policy pp1,
/// under injected failures — same rollback/success contract, with the new
/// p2 bound to pp1 and `primary` made global.
// go-parity-gap: ALTER TABLE ... PARTITION BY is refused by this tier's
// ALTER dispatch; the failure-injection harness needs the job queue and the
// rule-bundle store.
#[test]
#[ignore]
fn partition_non_partitioned_failures_placement_policy_rolls_back() {
}

/// Go `reorg_partition_test.go:1166-1180::TestReorganizePartitionFailuresAddPlacementPolicy`:
/// `alter table t reorganize partition p2 into (partition p2 ..., partition
/// pMax ... placement policy pp1)` under injected failures — rollback keeps
/// the original two-boundary metadata and bundles; success adds the pMax
/// partition bound to pp1.
// go-parity-gap: REORGANIZE PARTITION is refused by this tier's ALTER
// dispatch; the failure-injection harness needs the job queue and the
// rule-bundle store.
#[test]
#[ignore]
fn reorganize_partition_failures_add_placement_policy_rolls_back() {
}

/// Go `reorg_partition_test.go:1181-1205::TestPartitionByFailuresAddPlacementPolicyGlobalIndex`:
/// re-partitioning a table whose primary key and unique key `c` are GLOBAL
/// indexes, with `update indexes (`primary` local, `c` global)` and a pp1
/// policy on the new pMax — rollback restores original metadata/bundles;
/// success keeps the data and the global/local index split.
// go-parity-gap: ALTER TABLE ... PARTITION BY is refused by this tier's
// ALTER dispatch, and this tier has no global-index rebuild carrier.
#[test]
#[ignore]
fn partition_by_failures_with_global_indexes_and_placement_policy_rolls_back() {
}
