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

//! The JOIN-REORDER COST family: which physical join a site takes when the
//! choice is Go's ver2 COST pick rather than a structural preference.
//!
//! Every expectation is pinned against a RECORDED TiDB plan in
//! `tests/integrationtest/r/planner/core/join_reorder2.result` or
//! `.../join_reorder_through_projection.result`. Three Go mechanisms are
//! under test, one per section below:
//!
//! * `LogicalJoin.DeriveStats`' LeftOuterJoin arm reached INSIDE a derived
//!   table (Go recurses `optimizeRecursive` into the subquery), which is
//!   what gives the join ABOVE the derived table a row estimate at all;
//! * `getHashJoins` stamping the SESSION's `tidb_hash_join_concurrency` on
//!   the candidate, which `getPlanCostVer24PhysicalHashJoin` divides the
//!   probe terms by;
//! * a COMPUTED projection delivering Go's `PhysicalProjection` task, so a
//!   parent join compares PRICED candidates instead of falling back to a
//!   structural merge.

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

/// `r/planner/core/join_reorder2.result`'s schema: five tables with an
/// integer-handle primary key, and data giving the LEFT OUTER join below an
/// unmatched preserved row to prove null extension survives the plan change.
fn join_reorder2_session() -> Session {
    let mut session = Session::new();
    for name in ["t1", "t2", "t3", "t4", "t5"] {
        session
            .run(&format!(
                "create table {name}(id int not null primary key, name varchar(100))"
            ))
            .unwrap();
    }
    session
        .run("insert into t1 values(1,'test1'),(2,'x')")
        .unwrap();
    session
        .run("insert into t2 values(1,'test2'),(2,'test2')")
        .unwrap();
    // No id=2 row: t2's second row must null-extend through the LEFT JOIN.
    session.run("insert into t3 values(1,'test3')").unwrap();
    session
        .run("insert into t4 values(1,'test4'),(2,'test4')")
        .unwrap();
    session
}

/// A DERIVED TABLE HOLDING A LEFT OUTER JOIN IS MODELLED, so the join above
/// it is PRICED and takes Go's hash pick -- not the structural merge whose
/// child order then forces an index join by elimination.
///
/// `r/planner/core/join_reorder2.result` records, for this statement (the
/// `leading` hint's `@sel_2` targets resolve nothing at
/// `tidb_opt_join_reorder_through_sel = 0`, so Go clears it and plans
/// unhinted):
///
/// ```text
/// HashJoin    inner join, equal:[eq(...t4.id, ...t1.id)]
/// ├─TableReader(Build)      data:TableFullScan
/// │ └─TableFullScan  table:t4  keep order:false
/// └─Selection(Probe)  or(like(...t2.name, "test2", 92), like(...t3.name, "test3", 92))
///   └─MergeJoin  left outer join, left key:...t2.id, right key:...t3.id
///     ├─TableReader(Build)  data:TableFullScan
///     │ └─TableFullScan  table:t3  keep order:true
///     └─MergeJoin(Probe)  inner join, left key:...t1.id, right key:...t2.id
/// ```
///
/// The mechanism: `sub` writes `... left join t3 ...` in its own `FROM`, and
/// the row inventory (`tidb_executor::driver::join_reorder`) used to DECLINE
/// any derived table containing an outer join. With no estimate the top
/// `(sub, t4)` site priced NO alternatives, and `build_join_with_choice`'s
/// fallback kept the structurally-available merge; the merge's child order
/// then reached the left-outer site as a non-empty property, where Go's
/// `getHashJoins` enumerates nothing ("hash join doesn't promise any
/// orders") and the INDEX join won by elimination -- the recorded divergence
/// `TableRangeScan table:t3 range: decided by [test.t2.id]`. Modelling the
/// derived outer join (Go `LogicalJoin.DeriveStats`: `count = math.Max(count,
/// leftProfile.RowCount)` for `LeftOuterJoin`) lets ver2 compare hash
/// 3,872,144 against merge 8,331,569 at the top, Go's own answer.
#[test]
fn a_derived_left_outer_join_is_modelled_and_the_join_above_hashes() {
    let mut session = join_reorder2_session();
    let sql = "select * from \
        (select t1.id, t1.name as n1, t2.name as n2, t3.name as n3 \
         from t1 inner join t2 on t1.id=t2.id left join t3 on t2.id=t3.id \
         where t2.name like 'test2' or t3.name like 'test3') sub \
        inner join t4 on sub.id=t4.id";
    let joined = plan(&mut session, &format!("explain {sql}")).join("\n");
    assert!(
        joined.contains("HashJoin") && joined.contains("equal:[eq(test.t4.id, test.t1.id)]"),
        "the top join must be the recorded hash, t4 first:\n{joined}"
    );
    assert!(
        joined.contains("left outer join, left side:MergeJoin"),
        "the left-outer join must MERGE over the ordered t3 scan:\n{joined}"
    );
    assert!(
        !joined.contains("TableRangeScan") && !joined.contains("decided by"),
        "no site may probe t3 per outer row -- that is the closed divergence:\n{joined}"
    );
    // The rows are the recording's semantics: t1.id=1 matches everywhere and
    // t2's id=2 row null-extends through t3, surviving the OR filter only
    // when a side matches.
    let rows = row_text(session.run(&format!(
        "select sub.id, sub.n3, t4.name from \
        (select t1.id, t1.name as n1, t2.name as n2, t3.name as n3 \
         from t1 inner join t2 on t1.id=t2.id left join t3 on t2.id=t3.id \
         where t2.name like 'test2' or t3.name like 'test3') sub \
        inner join t4 on sub.id=t4.id order by sub.id"
    )));
    assert_eq!(
        rows,
        vec![
            vec!["1".to_owned(), "test3".to_owned(), "test4".to_owned()],
            vec!["2".to_owned(), "NULL".to_owned(), "test4".to_owned()],
        ]
    );
}

/// `r/planner/core/join_reorder_through_projection.result`'s schema.
fn through_projection_session() -> Session {
    let mut session = Session::new();
    for name in ["t1", "t2", "t3", "t5"] {
        session
            .run(&format!(
                "create table {name}(a int, b int, c varchar(32), primary key (a), key(b))"
            ))
            .unwrap();
    }
    session
        .run("insert into t1 values(1,10,'a1'),(2,20,'a2'),(4,200,'a4')")
        .unwrap();
    session
        .run("insert into t2 values(1,100,'b1'),(2,200,'b2'),(3,300,'b3')")
        .unwrap();
    session
        .run("insert into t3 values(1,10,'c1'),(2,20,'c2'),(3,30,'c3')")
        .unwrap();
    session
        .run("insert into t5 values(1,10,'e1'),(2,20,'e2'),(3,30,'e3')")
        .unwrap();
    session
}

/// HASH-JOIN PRICING READS THE SESSION'S CONCURRENCY. mysql-tester's DSN sets
/// `tidb_hash_join_concurrency = 1` in every connection the recordings were
/// made from, and `getPlanCostVer24PhysicalHashJoin` divides the probe filter
/// and probe hash by `p.Concurrency` (stamped by `getHashJoins` from
/// `sctx.GetSessionVars().HashJoinConcurrency()`). At 1 a hash join is
/// charged what five workers would have shared, and only then does the
/// recorded plan win:
///
/// `result:1319` (`tidb_opt_join_reorder_through_proj = on`) records
/// `MergeJoin(t5)` over `MergeJoin(t3)` over `IndexHashJoin` whose inner is
/// `IndexRangeScan  table:t1, index:b(b)  range: decided by
/// [eq(...t1.b, Column)]`. Hardcoding the plain-session 5 instead priced a
/// DIFFERENT session and flipped this statement to an all-hash tree.
#[test]
fn hash_join_pricing_reads_the_sessions_concurrency() {
    let mut session = through_projection_session();
    session
        .run("set tidb_opt_join_reorder_through_proj = on")
        .unwrap();
    session
        .run("set tidb_opt_join_reorder_threshold = 10")
        .unwrap();
    let sql = "explain select t1.a, dt.key_a from t1, t5, \
        (select t2.a as key_a, t2.b * 2 as doubled_b from t2 join t3 on t2.a = t3.a) dt \
        where t1.b = dt.doubled_b and dt.key_a = t5.a";
    session.run("set tidb_hash_join_concurrency = 1").unwrap();
    let recorded = plan(&mut session, sql).join("\n");
    assert!(
        recorded.contains("IndexRangeScan")
            && recorded.contains("range: decided by [eq(test.t1.b, Column)]"),
        "at the recorded concurrency the index join probes t1 by the injected column:\n{recorded}"
    );
    session.run("set tidb_hash_join_concurrency = 5").unwrap();
    let plain = plan(&mut session, sql).join("\n");
    assert_ne!(
        recorded, plain,
        "at 5 the probe terms are shared by five workers and the comparison moves; \
         if these became equal the chooser stopped reading the session"
    );
}

/// A COMPUTED PROJECTION DELIVERS GO'S `PhysicalProjection` TASK, so the join
/// above the derived table is PRICED. `result:1584`
/// (`tidb_opt_join_reorder_through_proj = off`, the shipped default) records:
///
/// ```text
/// HashJoin  inner join, equal:[eq(...t1.b, Column)]
/// ├─Projection(Build)  ...t2.a, mul(...t2.b, 2)->Column
/// │ └─MergeJoin ... t2/t3 keep order:true
/// └─TableReader(Probe)  data:Selection
///   └─Selection  not(isnull(...t1.b))
///     └─TableFullScan  table:t1  keep order:false
/// ```
///
/// -- t1 read WHOLE under a hash join. Before the projection receipt landed,
/// `dt`'s missing candidate made every alternative unpriceable and the
/// structural chooser took the index join wherever it was possible: this
/// statement probed t1 with `IndexRangeScan ... decided by
/// [eq(test.t1.b, Column)]`, a plan Go builds only under `through_proj = on`
/// at the recording's session.
#[test]
fn a_computed_projection_delivers_a_priced_receipt_and_t1_is_read_whole() {
    let mut session = through_projection_session();
    session.run("set tidb_hash_join_concurrency = 1").unwrap();
    let sql = "select t1.*, dt.* from t1, \
        (select t2.a as key_a, t2.b * 2 as doubled_b from t2 join t3 on t2.a = t3.a) dt \
        where t1.b = dt.doubled_b";
    let joined = plan(&mut session, &format!("explain {sql}")).join("\n");
    assert!(
        joined.contains("HashJoin"),
        "the recorded OFF plan hashes over a whole read of t1:\n{joined}"
    );
    assert!(
        !joined.contains("decided by"),
        "no index join may probe t1 -- that is through_proj=on's plan, not this session's:\n{joined}"
    );
    assert!(
        joined.contains("table:t1|keep order:false"),
        "t1 is read whole and unordered under the hash:\n{joined}"
    );
    let rows = row_text(session.run(&format!("{sql} order by t1.a")));
    assert_eq!(
        rows,
        vec![vec![
            "4".to_owned(),
            "200".to_owned(),
            "a4".to_owned(),
            "1".to_owned(),
            "200".to_owned(),
        ]]
    );
}

/// AN ORDERED CHILD KEEPS ITS ORDERED SCAN: a single-table derived SELECT a
/// merge-join parent requires an order of must not swap its ordered table
/// scan for a cheaper covering index that walks in a DIFFERENT order.
///
/// Go's `convertToIndexScan` / `convertToTableScan` both open with `if
/// !prop.IsSortItemEmpty() && !candidate.matchPropResult.Matched() { return
/// invalidTask }` -- under a required order a non-matching path is not a
/// candidate at all. The derived select here needs only `{a, b}`, which
/// `key(b)` COVERS on an integer-handle table, so without that gate the
/// single-table pipeline replaced the ordered scan with `IndexFullScan
/// index:b(b) keep order:false` and the merge join above interleaved
/// unsorted rows.
#[test]
fn an_ordered_derived_child_keeps_its_ordered_scan() {
    let mut session = through_projection_session();
    // The COMPUTED column keeps the derived table from dissolving
    // (`ProjectionEliminator` removes only all-bare-column projections), so
    // its inner SELECT reaches the single-table pipeline -- reading `{a, b}`,
    // which `key(b)` covers -- while the merge join above requires the
    // `a`-order of its output.
    let sql = "select dt.a, dt.d from \
        (select t2.a, t2.b, t2.b * 2 as d from t2) dt join t5 on dt.a = t5.a";
    let joined = plan(&mut session, &format!("explain {sql}")).join("\n");
    if joined.contains("MergeJoin") {
        assert!(
            !joined.contains("index:b(b)"),
            "a scan of index b cannot deliver the a-order the merge relies on:\n{joined}"
        );
    }
    let rows = row_text(session.run(&format!("{sql} order by dt.a")));
    assert_eq!(
        rows,
        vec![
            vec!["1".to_owned(), "200".to_owned()],
            vec!["2".to_owned(), "400".to_owned()],
            vec!["3".to_owned(), "600".to_owned()],
        ]
    );
}
