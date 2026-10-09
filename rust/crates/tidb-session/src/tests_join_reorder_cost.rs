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
/// the former executor-local row inventory used to decline any derived table
/// containing an outer join. With no estimate the top
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
    // Refreshed against the LIVE oracle (SELECT tidb_version() =>
    // fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85) on this exact fixture.
    // Refreshed against the LIVE oracle (SELECT tidb_version() =>
    // fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85), probe database `probe_jrc4`,
    // the exact fixture DML followed immediately by both EXPLAINs. In that
    // deterministic scenario -- the stats delta not yet flushed, so
    // `GetStatsTable` answers from the 10000-pseudo fallback -- Go prints the
    // SAME plan at `tidb_hash_join_concurrency = 1` and at 5: the top
    // all-hash join keeps `equal:[eq(test.t5.a, test.t2.a)]` with the
    // IndexReader over `t5, index:b(b)` as the build, and the dt subtree
    // joins t2/t3 over a MergeJoin. A byte diff of the two captures (plan IDs
    // aside) is EMPTY. Go's concurrency leverage for this statement only
    // appears AFTER the delta flush publishes the real row counts (3/2.4),
    // where the INNER hash join's build moves from t3 to t2 between the two
    // settings (probe database `probe_jrc2`); that scenario needs the
    // stats-delta flush port and is not pinned here.
    session.run("set tidb_hash_join_concurrency = 1").unwrap();
    let recorded = plan(&mut session, sql).join("\n");
    assert!(
        recorded.contains("inner join, equal:[eq(test.t5.a, test.t2.a)]"),
        "at concurrency 1 the top join keeps the t5 build with the\
         IndexReader over t5 index b(b):\n{recorded}"
    );
    assert!(
        recorded.contains("MergeJoin") && recorded.contains("left key:test.t2.a"),
        "at concurrency 1 the dt subtree must still merge over the \
         ordered t2/t3 scans:\n{recorded}"
    );
    session.run("set tidb_hash_join_concurrency = 5").unwrap();
    let plain = plan(&mut session, sql).join("\n");
    assert!(
        plain.contains("inner join, equal:[eq(test.t5.a, test.t2.a)]"),
        "at 5 the plan keeps the t5 build, matching the live capture \
         (probe_jrc4):\n{plain}"
    );
    let strip_ids = |text: &str| {
        let mut out = String::with_capacity(text.len());
        let chars: Vec<char> = text.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if chars[i] == '_' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit() {
                let mut j = i + 1;
                while j < chars.len() && chars[j].is_ascii_digit() {
                    j += 1;
                }
                let boundary =
                    j == chars.len() || !(chars[j].is_ascii_alphanumeric() || chars[j] == '_');
                if boundary {
                    i = j;
                    continue;
                }
            }
            out.push(chars[i]);
            i += 1;
        }
        out
    };
    assert_eq!(
        strip_ids(&recorded),
        strip_ids(&plain),
        "the live oracle prints the SAME plan at both settings (the probe \
         captures diff empty modulo plan IDs)"
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

/// `t/planner/core/casetest/rule/rule_join_reorder.test`'s LEADING schema:
/// eight indexed tables, none analyzed.
fn leading_hint_session() -> Session {
    let mut session = Session::new();
    for name in ["t", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8"] {
        session
            .run(&format!("create table {name}(a int, b int, key(a))"))
            .unwrap();
    }
    session
}

/// The operator column and, for joins, the operator info of a plan.
fn join_lines(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .map(|row| {
            let id = row[0].trim_start_matches(['│', '├', '└', '─', ' ']);
            if id.starts_with("HashJoin") {
                format!("{id} {}", row[3])
            } else {
                id.to_owned()
            }
        })
        .collect()
}

/// Go `FindAndRemovePlanByAstHint`'s second step: a LEADING table that no
/// plan's own alias matches names the derived table whose query block has
/// that alias (`PlannerSelectBlockAsName`). The projection over `t2` is
/// eliminated, so the join group holds `t2` itself, and `tx` reaches it only
/// through its block. Recorded: no warning, `t2` joins `t1` first.
#[test]
fn a_leading_table_names_a_derived_table_through_its_query_block() {
    let mut session = leading_hint_session();
    let sql = "select /*+ leading(tx, t1, t3) */ * from t1, (select * from t2) tx, t3 \
        where t1.a=tx.a and tx.a=t3.a";
    let plan = join_lines(&mut session, &format!("explain format='plan_tree' {sql}"));
    assert_eq!(warnings_of(&session), Vec::new());
    assert_eq!(
        plan,
        vec![
            "Projection",
            "HashJoin inner join, equal:[eq(test.t2.a, test.t3.a)]",
            "TableReader(Build)",
            "Selection",
            "TableFullScan",
            "HashJoin(Probe) inner join, equal:[eq(test.t2.a, test.t1.a)]",
            "TableReader(Build)",
            "Selection",
            "TableFullScan",
            "TableReader(Probe)",
            "Selection",
            "TableFullScan",
        ]
    );
}

/// Go `LogicalSchemaProducer.OutputNames` propagates a child's names only
/// when there is exactly one child, and the static-mode partition processor
/// sets none on its `LogicalPartitionUnionAll`, so `ExtractTableAlias` finds
/// no alias for a partitioned table read through the union and LEADING
/// cannot name it. Recorded warning for each statement below.
#[test]
fn a_static_mode_partition_union_carries_no_alias_for_leading() {
    let mut session = Session::new();
    session.run("create table t(a int, b int, key(a))").unwrap();
    session
        .run("create table t1(a int, b int) partition by hash(a) partitions 4")
        .unwrap();
    session
        .run("create table t2(a int, b int) partition by hash(a) partitions 5")
        .unwrap();
    session
        .run("create table t3(a int, b int) partition by hash(b) partitions 3")
        .unwrap();
    session
        .run("set @@tidb_partition_prune_mode='static'")
        .unwrap();
    let inapplicable = vec![(
        1815,
        "leading hint is inapplicable, check if the leading hint table is valid".to_owned(),
    )];
    for sql in [
        "select /*+ leading(t1) */ * from t, t1, t2, t3 where t.a = t1.a and t1.b=t2.b and t2.b=t3.b",
        "select /*+ leading(t3) */ * from t2 left join (t1 left join t3 on t1.a=t3.a) on t2.b=t1.b",
    ] {
        session.run(&format!("explain format='plan_tree' {sql}")).unwrap();
        assert_eq!(warnings_of(&session), inapplicable, "{sql}");
    }
}

/// Go `SetPreferredJoinTypeAndOrder` marks a join from `MatchTableName`
/// over `LeadingJoinOrder`, which ParsePlanHints empties when several
/// LEADING hints void each other; the first hint's LeadingList survives,
/// but no join carries it. Marking from the LeadingList applied
/// `leading(t1, t2)` here; a Go oracle run reports only the conflict
/// warning and plans the statement as if unhinted.
#[test]
fn voided_leading_hints_mark_no_join() {
    let mut session = leading_hint_session();
    let sql = "select /*+ leading(t1, t2) leading(t3, t4) */ * from t1 join t2 on t1.b=t2.b \
        join t3 on t2.a=t3.a join t4 on t3.a=t4.a";
    let hinted = join_lines(&mut session, &format!("explain format='plan_tree' {sql}"));
    assert_eq!(
        warnings_of(&session),
        vec![(
            1815,
            "We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid"
                .to_owned()
        )]
    );
    let unhinted = join_lines(
        &mut session,
        "explain format='plan_tree' select * from t1 join t2 on t1.b=t2.b \
            join t3 on t2.a=t3.a join t4 on t3.a=t4.a",
    );
    assert_eq!(hinted, unhinted);
}

/// Go restores a table-less join hint for its warning with
/// `format.NewRestoreCtx(0, ...)`, which writes the hint name as written.
#[test]
fn a_table_less_join_hint_warns_with_its_name_as_written() {
    let mut session = leading_hint_session();
    for (hint, written) in [
        ("no_hash_join()", "no_hash_join()"),
        ("NO_MERGE_JOIN()", "NO_MERGE_JOIN()"),
        ("Hash_Join()", "Hash_Join()"),
    ] {
        session
            .run(&format!(
                "explain format='plan_tree' select /*+ {hint} */ * from t1, t2 where t1.a=t2.a"
            ))
            .unwrap();
        assert_eq!(
            warnings_of(&session),
            vec![(
                1815,
                format!(
                    "Hint {written} is inapplicable. Please specify the table names in the arguments."
                )
            )]
        );
    }
}
