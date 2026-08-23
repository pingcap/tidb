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

//! Go's join-key type-cast chain, pinned against
//! `r/planner/core/join_key_type_cast.result`.
//!
//! `rule_join_key_type_cast.go` rewrites an INT-vs-VARCHAR join equality --
//! which `updateEQCond` had materialized as DOUBLE casts in child
//! projections -- into an integer equality: the varchar side becomes
//! `cast(id AS SIGNED)` under a freshly allocated plan column, a guard
//! `Selection` drops values whose integer cast is not their numeric value,
//! and an `INL_JOIN` on the int side can then range over its clustered
//! handle. The recorded plan names the injected column by its exact position
//! in the statement's `AllocPlanColumnID` stream: `Column#12` for this
//! schema (seven source ids, two `updateEQCond` casts, two
//! `BuildKeyInfoPortal` re-allocations, then the rule's own). See
//! `tidb_executor::driver::join_key_cast` for the verified arithmetic.

#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// The plan rows of one statement as `|`-joined text, one string per row.
fn plan(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .map(|row| row.join("|"))
        .collect()
}

/// `r/planner/core/join_key_type_cast.result`'s IndexJoin fixture.
fn fixture() -> Session {
    let mut session = Session::new();
    session
        .run("create table t_idx_int (id int primary key, v int)")
        .unwrap();
    session
        .run("create table t_idx_str (id varchar(20), v int, index idx_id(id))")
        .unwrap();
    session
        .run("insert into t_idx_int values (1, 10), (2, 20), (3, 30)")
        .unwrap();
    session
        .run("insert into t_idx_str values ('1', 10), ('2', 20), ('1.5', 15)")
        .unwrap();
    session.run("analyze table t_idx_int").unwrap();
    session.run("analyze table t_idx_str").unwrap();
    session
}

/// `r/planner/core/join_key_type_cast.result` records, for the INL_JOIN on
/// the INT side:
///
/// ```text
/// └─IndexJoin ... inner:Projection, outer key:Column#12, inner key:Column#1,
///                 equal cond:eq(Column#12, Column#1)
///   ├─Projection(Build)  ... cast(...t_idx_str.id, bigint BINARY)->Column#12
///   ...
///   └─Projection(Probe)  ... t_idx_int.id->Column#1
///     └─TableReader ... data:TableRangeScan
///       └─TableRangeScan table:t_idx_int  range: decided by [Column#12], keep order:false
/// ```
///
/// The row the replay harness compares is the SCAN row: `TableRangeScan`
/// over `t_idx_int`, ranged by the injected `Column#12` -- the number being
/// the statement's own allocation history, not a literal.
#[test]
fn the_hinted_index_join_ranges_over_the_injected_cast_column() {
    let mut session = fixture();
    let rows = plan(
        &mut session,
        "explain select /*+ INL_JOIN(t_idx_int) */ * from t_idx_int \
         join t_idx_str on t_idx_int.id = t_idx_str.id",
    );
    assert!(
        rows.iter().any(|row| row.contains("IndexJoin")),
        "the hint must reach the index strategy: {rows:#?}",
    );
    let scan = rows
        .iter()
        .find(|row| row.contains("TableRangeScan") && row.contains("table:t_idx_int"))
        .unwrap_or_else(|| panic!("no ranged scan of t_idx_int in {rows:#?}"));
    assert!(
        scan.contains("range: decided by [Column#12]"),
        "the range must name the rule's injected column: {scan}",
    );
}

/// The same statement's ROWS: `'1.5'` must not match `1` -- the guard drops
/// it before the probe (`CAST('1.5' AS SIGNED)` is 2, whose DOUBLE value is
/// not 1.5) -- while `'1'` and `'2'` land on their handles.
#[test]
fn the_hinted_index_join_returns_the_recorded_rows() {
    let mut session = fixture();
    let rows = row_text(session.run(
        "select /*+ INL_JOIN(t_idx_int) */ * from t_idx_int \
         join t_idx_str on t_idx_int.id = t_idx_str.id order by t_idx_int.id",
    ));
    assert_eq!(
        rows,
        vec![vec!["1", "10", "1", "10"], vec!["2", "20", "2", "20"],]
            .into_iter()
            .map(|row| row.into_iter().map(str::to_owned).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
    );
}

/// The OTHER side's hint must not reach the cast probe: the rewritten join
/// key on the varchar side is `cast(id AS SIGNED)`, not the indexed column,
/// so `INL_JOIN(t_idx_str)` stays a hash join -- the recording shows
/// `HashJoin ... equal:[eq(Column#1, Column#12)]` with full scans on both
/// sides.
#[test]
fn hinting_the_varchar_side_keeps_the_hash_join() {
    let mut session = fixture();
    let rows = plan(
        &mut session,
        "explain select /*+ INL_JOIN(t_idx_str) */ * from t_idx_int \
         join t_idx_str on t_idx_int.id = t_idx_str.id",
    );
    assert!(
        !rows.iter().any(|row| row.contains("IndexJoin")),
        "no index over the cast key exists to join on: {rows:#?}",
    );
    assert!(
        rows.iter()
            .any(|row| row.contains("TableFullScan") && row.contains("table:t_idx_int")),
        "the int side is read whole under the hash join: {rows:#?}",
    );
}

/// The multi-way `t_mj` statement's ROW ORDER, decoded from the recorded
/// result: the mid-tree cartesian with `t3` PROBES `t3` and BUILDS the
/// joined `(t1,t2)` side, whose rows then emit newest-first per probe row.
/// That build-side choice is the ver2 cost comparison's -- reachable only
/// because the row inventory models a `straight_join` subtree
/// (`driver::join_reorder::collect_rows`) -- and the recording was made at
/// `tidb_hash_join_concurrency=1` (mysql-tester's DSN), which the session
/// must mirror.
#[test]
fn the_multiway_cartesian_builds_the_joined_side() {
    let mut session = Session::new();
    session.run("set @@tidb_hash_join_concurrency=1").unwrap();
    session
        .run("create table t_mj (a varchar(1), b integer)")
        .unwrap();
    session
        .run("insert into t_mj values ('1', 1), ('2', 2)")
        .unwrap();
    let rows = row_text(session.run(
        "select * from t_mj t1 \
         join t_mj t2 on t1.b = t2.b \
         join t_mj t3 \
         join (t_mj t4 straight_join t_mj t5 on t4.a = t5.b) on t1.b = t4.b \
         where t1.a = t5.b",
    ));
    let expected: Vec<Vec<String>> = [
        ["2", "2", "2", "2", "1", "1", "2", "2", "2", "2"],
        ["1", "1", "1", "1", "1", "1", "1", "1", "1", "1"],
        ["2", "2", "2", "2", "2", "2", "2", "2", "2", "2"],
        ["1", "1", "1", "1", "2", "2", "1", "1", "1", "1"],
    ]
    .into_iter()
    .map(|row| row.into_iter().map(str::to_owned).collect())
    .collect();
    assert_eq!(rows, expected);
}
