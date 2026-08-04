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

//! When a join prints `MergeJoin`, and what its children then say about order.
//!
//! Every expectation here is pinned against a RECORDED TiDB plan in
//! `tests/integrationtest/r/**`, not against a capture and not against this
//! tier's own previous output. The recording quoted in each test is the
//! standing oracle; this tier's rows are compared to it on the decision the
//! test is about -- which join algorithm, and whether the child scans keep
//! order -- because the tree between them diverges by construction (no
//! `cop[tikv]` task and no `TableReader` wrapper here; see
//! `tidb_executor::explain`'s module doc).

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

/// `r/planner/core/join_reorder_through_projection.result`'s schema: an
/// integer primary key, which is the clustered handle, plus a secondary index
/// the merge decision must NOT reach for.
fn pk_session() -> Session {
    let mut session = Session::new();
    for name in ["t1", "t2", "t3"] {
        session
            .run(&format!(
                "create table {name}(a int, b int, c varchar(32), primary key (a), key(b))"
            ))
            .unwrap();
        session
            .run(&format!(
                "insert into {name} values(1,10,'x'),(2,20,'y'),(3,30,'z')"
            ))
            .unwrap();
    }
    session
}

/// The batch's target shape. `r/planner/core/join_reorder_through_projection.result`
/// records, for the sub-join of the same two tables on the same key:
///
/// ```text
/// MergeJoin      root  inner join, left key:...t2.a, right key:...t3.a
/// ├─TableReader(Build)   root  data:TableFullScan
/// │ └─TableFullScan  cop[tikv]  table:t3  keep order:true, stats:pseudo
/// └─TableReader(Probe)   root  data:TableFullScan
///   └─TableFullScan  cop[tikv]  table:t2  keep order:true, stats:pseudo
/// ```
///
/// Three separate things are being pinned: the operator is `MergeJoin` and not
/// `HashJoin`, the info is Go's `left key:`/`right key:` and not
/// `equal:[...]`, and BOTH child scans say `keep order:true` -- which is the
/// point of the batch, because it is the first plan in which a parent's
/// required order reaches a leaf at all.
#[test]
fn a_join_on_both_sides_clustered_primary_keys_merges() {
    let mut session = pk_session();
    let rows = plan(
        &mut session,
        "explain select * from t2 join t3 on t2.a = t3.a",
    );
    let joined = rows.join("\n");
    assert!(
        joined.contains("MergeJoin"),
        "the join must merge, not hash:\n{joined}"
    );
    assert!(
        joined.contains("inner join, left key:test.t2.a, right key:test.t3.a"),
        "Go's merge-join info is `left key:`/`right key:`:\n{joined}"
    );
    assert_eq!(
        joined.matches("keep order:true").count(),
        2,
        "both sides were required to keep order:\n{joined}"
    );
    assert!(
        !joined.contains("equal:["),
        "a merge join prints its keys, not a hash join's equal list:\n{joined}"
    );
}

/// The rows a merge join returns are the rows the query has. Pinned against
/// the same query's recorded ROW result rather than against the plan: a plan
/// that changed while the answer did not is the failure this catches.
#[test]
fn the_merged_rows_are_the_joins_rows() {
    let mut session = pk_session();
    let rows = row_text(session.run("select t2.a, t3.b from t2 join t3 on t2.a = t3.a"));
    assert_eq!(
        rows,
        vec![
            vec!["1".to_owned(), "10".to_owned()],
            vec!["2".to_owned(), "20".to_owned()],
            vec!["3".to_owned(), "30".to_owned()],
        ]
    );
}

/// A join on a NON-handle column of the same tables keeps the hash join and
/// keeps `keep order:false`. This is the boundary the batch deliberately does
/// not cross: Go answers `HashJoin` over `IndexFullScan index:b(b)` here, and
/// reaching for that index is the follow-on increment -- so what must hold now
/// is that nothing about this plan moved.
#[test]
fn a_join_on_an_indexed_non_handle_column_still_hashes() {
    let mut session = pk_session();
    let joined = plan(
        &mut session,
        "explain select * from t2 join t3 on t2.b = t3.b",
    )
    .join("\n");
    assert!(
        joined.contains("HashJoin"),
        "the secondary-index order is not offered yet:\n{joined}"
    );
    assert!(
        !joined.contains("keep order:true"),
        "no order was required, so no scan may claim one:\n{joined}"
    );
}

/// A table with NO clustered integer primary key provides no order, so the
/// join hashes even when the key is the same column on both sides. Go agrees:
/// `r/executor/merge_join.result`'s `t(c1 int, c2 int)` reaches a merge join
/// only under an explicit `TIDB_SMJ` hint, and then with a `Sort` on each side
/// and `keep order:false` on the scans.
#[test]
fn a_heap_table_join_still_hashes() {
    let mut session = Session::new();
    session.run("create table h1(c1 int, c2 int)").unwrap();
    session.run("create table h2(c1 int, c2 int)").unwrap();
    session.run("insert into h1 values(1,1),(2,2)").unwrap();
    session.run("insert into h2 values(2,3),(4,4)").unwrap();
    let joined = plan(
        &mut session,
        "explain select * from h1 join h2 on h1.c1 = h2.c1",
    )
    .join("\n");
    assert!(
        joined.contains("HashJoin"),
        "neither side provides an order:\n{joined}"
    );
    assert!(!joined.contains("keep order:true"), "{joined}");
}

/// A LEFT join over two clustered primary keys merges too, and the preserved
/// side's unmatched rows survive: the merge strategy's outer semantics reach
/// the driver, not just its unit test.
#[test]
fn a_left_join_merges_and_keeps_its_unmatched_rows() {
    let mut session = pk_session();
    session.run("insert into t2 values(9,90,'w')").unwrap();
    let joined = plan(
        &mut session,
        "explain select * from t2 left join t3 on t2.a = t3.a",
    )
    .join("\n");
    assert!(joined.contains("MergeJoin"), "{joined}");
    let rows = row_text(
        session.run("select t2.a, t3.a from t2 left join t3 on t2.a = t3.a order by t2.a"),
    );
    assert_eq!(rows.len(), 4, "{rows:?}");
    assert_eq!(rows[3], vec!["9".to_owned(), "NULL".to_owned()]);
}

/// An ALIASED self-join merges: the two occurrences are distinguishable, and
/// each provides its own handle's order. An UNALIASED one never reaches the
/// decision at all -- `from t2, t2` is a duplicate table name and is refused
/// before any join is built, as it is in MySQL.
#[test]
fn an_aliased_self_join_merges_and_an_unaliased_one_is_refused() {
    let mut session = pk_session();
    let joined = plan(
        &mut session,
        "explain select x.a from t2 x join t2 y on x.a = y.a",
    )
    .join("\n");
    assert!(joined.contains("MergeJoin"), "{joined}");
    assert!(
        joined.contains("left key:test.x.a, right key:test.y.a"),
        "the keys name the ALIASES, which is what makes them tellable apart:\n{joined}"
    );
    assert!(session.run("select t2.a from t2, t2").is_err());
}

/// The ORDER-BY-driven keep-order scan this batch did NOT reach.
///
/// Go, on `t(a int, b int, c int, key ia(a))`:
///
/// ```text
/// Limit_13            root
/// └─IndexReader_26    root       index:Limit_25
///   └─Limit_25        cop[tikv]
///     └─IndexFullScan_24  cop[tikv]  table:t, index:ia(a)  keep order:true, desc
/// ```
///
/// -- no ordering operator at all, because the index's own order satisfies the
/// `ORDER BY`. Reaching that needs the property to flow into the ACCESS-PATH
/// choice (Go's second, order-carrying `findBestTask` invocation), which is
/// the follow-on increment: only a merge join demands an order here, and only
/// of a scan whose path was already fixed. This test records where the tier
/// actually stands so the gap is a measured fact rather than a claim.
#[test]
fn an_order_by_limit_still_sorts_rather_than_reading_the_index_in_order() {
    let mut session = Session::new();
    session
        .run("create table t (a int, b int, c int, key ia(a))")
        .unwrap();
    let joined = plan(
        &mut session,
        "explain select x.a from (select a from t order by a desc limit 2) x",
    )
    .join("\n");
    assert!(
        joined.contains("Sort") || joined.contains("TopN"),
        "an ordering operator is still present, where Go has none:\n{joined}"
    );
    assert!(
        !joined.contains("keep order:true"),
        "no scan claims the order yet:\n{joined}"
    );
}
