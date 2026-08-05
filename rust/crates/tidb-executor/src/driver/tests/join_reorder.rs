//! WHEN THE JOIN REORDER FIRES, AND WHAT IT LEAVES ALONE.
//!
//! Every test here names the gate it is about and asserts either the PLAN or
//! the leaf order the reorder chose, because that is the reorder's whole
//! observable effect. The oracle for the plan that IS reordered is TiDB's own
//! recording, quoted in [`the_dp_builds_the_tree_tidb_records`].
//!
//! Both solvers are covered. A DEFAULT session takes the GREEDY one
//! (`tidb_opt_join_reorder_threshold` is `0` and every group is bigger), so
//! that arm is the one with corpus-wide reach; the DP is reachable only from a
//! session that raised the threshold. The refusals are the load-bearing tests:
//! a decline leaves the statement's written tree alone, and after the greedy
//! landed "the two thresholds agree" no longer proves one, so every refusal
//! asserts [`fired`] is `None` instead.

use super::*;

/// The schema the recorded topic uses
/// (`t/planner/core/join_reorder_through_projection.test`).
fn tables() -> Catalog {
    let mut catalog = Catalog::default();
    for name in ["t1", "t2", "t3", "t5"] {
        crate::run_create_table_on(
            &format!("CREATE TABLE {name} (a INT, b INT, c VARCHAR(32), PRIMARY KEY (a), KEY(b))"),
            &mut catalog,
        )
        .unwrap();
    }
    catalog
}

/// The plan `EXPLAIN` prints, one operator name per line with its tree prefix.
fn plan(sql: &str, catalog: &Catalog, threshold: i32) -> Vec<String> {
    let ctx = crate::StmtContext::for_query().with_join_reorder_threshold(threshold);
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) = crate::explain::explain_select_stmt(
        select,
        catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Row,
    )
    .unwrap();
    rows.iter()
        .map(|row| match &row[0] {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        })
        .collect()
}

/// Whether the reorder FIRED, and where each written leaf ended up.
///
/// `None` is a decline, which is the assertion every refusal test below wants:
/// once the greedy runs at the default threshold, "the two thresholds agree"
/// no longer distinguishes a refusal from two solvers reaching the same tree.
fn fired(sql: &str, catalog: &Catalog, threshold: i32) -> Option<Vec<usize>> {
    let ctx = crate::StmtContext::for_query().with_join_reorder_threshold(threshold);
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    crate::driver::join_reorder::reorder(
        select.from.as_ref().unwrap(),
        select.where_clause.as_ref(),
        catalog,
        "test",
        &ctx,
    )
    .map(|plan| plan.written_order)
}

/// The statement the recorded topic runs at
/// `set tidb_opt_join_reorder_threshold = 10`.
const THREE_WAY: &str = "SELECT t1.a, dt.key_a FROM t1, t5, \
     (SELECT t2.a AS key_a, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a) dt \
     WHERE t1.a = dt.key_a AND dt.key_a = t5.a";

/// The written tree is `(t1, t5), dt`, whose bottom join has no equality at
/// all; the DP's answer is `(t1, dt), t5`.
///
/// `r/planner/core/join_reorder_through_projection.result:1249` records
/// exactly that for this statement:
///
/// ```text
/// MergeJoin           inner join, left key:t2.a, right key:t5.a
/// ├─TableReader(Build)      TableFullScan table:t5  keep order:true
/// └─MergeJoin(Probe)  inner join, left key:t1.a, right key:t2.a
///   ├─Projection(Build)     t2.a, mul(t2.b, 2)
///   │ └─MergeJoin     inner join, left key:t2.a, right key:t3.a
///   │   ├─TableReader       TableFullScan table:t3  keep order:true
///   │   └─TableReader       TableFullScan table:t2  keep order:true
///   └─TableReader(Probe)    TableFullScan table:t1  keep order:true
/// ```
///
/// `t5` at the top and `t1` under the second join is the reorder; every leaf
/// keeping order is the merge that only becomes possible once it happens.
#[test]
fn the_dp_builds_the_tree_tidb_records() {
    let catalog = tables();
    let reordered = plan(THREE_WAY, &catalog, 10);
    assert_eq!(
        reordered,
        vec![
            "Projection_10",
            "└─Selection_9",
            "  └─MergeJoin_8",
            "    ├─TableFullScan_1(Build)",
            "    └─MergeJoin_7(Probe)",
            "      ├─Projection_5(Build)",
            "      │ └─MergeJoin_4",
            "      │   ├─TableFullScan_2(Build)",
            "      │   └─TableFullScan_3(Probe)",
            "      └─TableFullScan_6(Probe)",
        ],
        "the reordered plan is not TiDB's",
    );
}

/// THE DEFAULT SESSION. `vardef.DefTiDBOptJoinReorderThreshold` is `0`, and
/// `useGreedy := ... || joinGroupNum > threshold` is then true for every group
/// (the smallest is 2), so Go takes the GREEDY solver -- which is why this is
/// the arm that fires on every multi-relation inner join in the corpus.
///
/// The written leaf order is `t1, t5, dt`; the greedy's is `t1, dt, t5`, so
/// `t5` moves from position `1` to position `2` and `dt` from `2` to `1`. That
/// is the tree TiDB records, quoted in [`the_dp_builds_the_tree_tidb_records`].
///
/// This exact order is what a REVERSED cost sort would break: `dt` is the most
/// expensive node, so a descending sort starts the tree there and reaches
/// `(dt, t1), t5` instead.
#[test]
fn the_default_threshold_takes_the_greedy_solver() {
    let catalog = tables();
    assert_eq!(fired(THREE_WAY, &catalog, 0), Some(vec![0, 2, 1]));
    assert_eq!(
        plan(THREE_WAY, &catalog, 0),
        plan(THREE_WAY, &catalog, 10),
        "the greedy and the DP disagree on the recorded statement",
    );
    // The written bottom join `(t1, t5)` has no equality: a cartesian product,
    // which is precisely what the reorder removes. Nothing hashes afterwards.
    assert!(
        !plan(THREE_WAY, &catalog, 0)
            .iter()
            .any(|row| row.contains("HashJoin")),
        "the greedy left the written cartesian join in place",
    );
}

/// THE CARTESIAN REFUSAL. `checkConnectionAndMakeJoin` returns no join at all
/// when the pair has no equality edge and
/// `tidb_opt_cartesian_join_order_threshold` is not positive -- and its default
/// is `0`. So `t1--t5` is joinable and `t2` never is: the greedy peels one
/// connected tree and leaves `t2` behind, which is Go's `makeBushyJoin` case
/// and this module's decline.
///
/// Dropping the refusal makes the greedy swallow `t2` and return a tree, which
/// this `None` catches.
#[test]
fn the_cartesian_refusal_leaves_a_second_component_behind() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, t2 WHERE t1.a = t5.a";
    assert_eq!(fired(sql, &catalog, 0), None);
    assert_eq!(fired(sql, &catalog, 10), None);
}

/// A NON-EQUALITY CONJUNCT SPANNING TWO LEAVES is one of Go's `otherConds`,
/// which `makeJoin` hands to whichever join first covers it and
/// `hasOtherJoinCondition` counts as a connection. The greedy arm models
/// neither, so it declines; the DP arm, which only a raised threshold reaches,
/// keeps its existing behaviour of ignoring the conjunct while costing.
///
/// Misplacing such a conjunct is exactly the failure this decline forecloses.
#[test]
fn a_spanning_non_equality_conjunct_declines_the_greedy() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, t2 \
               WHERE t1.a = t5.a AND t5.a = t2.a AND t1.b > t2.b";
    assert_eq!(fired(sql, &catalog, 0), None, "the greedy did not decline");
    assert!(fired(sql, &catalog, 10).is_some(), "the DP arm moved");
}

/// THE SOLVER BOUNDARY, to the exact integer. This group has THREE leaves and
/// only the greedy arm declines it, so which solver ran is directly readable
/// from whether the reorder fired.
///
/// Go's `joinGroupNum > threshold` puts the switch BETWEEN `2` and `3`: at a
/// threshold of `2` a three-leaf group is still greedy, and at `3` it is the
/// DP. An off-by-one in either direction moves one of these four.
#[test]
fn the_solver_switches_between_a_threshold_of_two_and_three() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, t2 \
               WHERE t1.a = t5.a AND t5.a = t2.a AND t1.b > t2.b";
    for greedy in [i32::MIN, 0, 1, 2] {
        assert_eq!(
            fired(sql, &catalog, greedy),
            None,
            "threshold {greedy} did not take the greedy arm",
        );
    }
    for dp in [3, 4, i32::MAX] {
        assert!(
            fired(sql, &catalog, dp).is_some(),
            "threshold {dp} did not take the DP arm",
        );
    }
}

/// THE COST SORT, where it is observable. `generateJoinOrderNode` sorts the
/// group by `baseNodeCumCost` ASCENDING and
/// `constructConnectedJoinTree` starts at `curJoinGroup[0]`, so the cheapest
/// node is the seed. Here the statement writes the EXPENSIVE relation first --
/// `dt` is a join of two tables, so its cumulative cost exceeds either base
/// table's -- and the seed must still be `t1`.
///
/// Written leaf order is `dt, t1, t5`; the greedy's is `t1, dt, t5`, so `dt`
/// moves from `0` to `1` and `t1` from `1` to `0`. Drop the sort and the seed
/// becomes `dt`, which yields `dt, t1, t5` -- the identity.
#[test]
fn the_seed_is_the_cheapest_node_not_the_first_written() {
    let catalog = tables();
    let sql = "SELECT t1.a, dt.key_a FROM \
        (SELECT t2.a AS key_a, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a) dt, \
        t1, t5 \
        WHERE t1.a = dt.key_a AND dt.key_a = t5.a";
    assert_eq!(fired(sql, &catalog, 0), Some(vec![1, 0, 2]));
}

/// THE SOLVER-CHOICE TABLE, as a plan-level assertion.
///
/// A group of three takes the DP at a threshold of `3` or more and the greedy
/// below that (`joinGroupNum > threshold`). Both solvers agree here, so the
/// assertion is that every threshold reorders -- the boundary is exercised,
/// not the disagreement.
#[test]
fn every_threshold_reorders_and_the_boundary_is_go_s() {
    let catalog = tables();
    for threshold in [i32::MIN, -1, 0, 1, 2, 3, 4, 10, i32::MAX] {
        assert_eq!(
            fired(THREE_WAY, &catalog, threshold),
            Some(vec![0, 2, 1]),
            "threshold {threshold} produced a different tree",
        );
    }
}

/// A TWO-NODE GROUP. Go's rule fires (`joinGroupNum > 1`) and the greedy runs,
/// but with one edge and two nodes there is only one tree to build, so the
/// cheapest node takes the left and the reorder is observable only when the
/// written order already had the expensive one first. Both leaves here are the
/// same pseudo size, so the STABLE sort keeps the written order and the tree
/// comes back unchanged -- fired, and a no-op.
#[test]
fn a_two_node_group_is_solved_and_leaves_equal_costs_where_they_were() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5 WHERE t1.a = t5.a";
    assert_eq!(fired(sql, &catalog, 0), Some(vec![0, 1]));
}

/// A ONE-NODE `FROM` is not a group at all: Go's rule needs
/// `len(curJoinGroup) > 1` before it picks a solver.
#[test]
fn a_single_relation_is_not_a_group() {
    let catalog = tables();
    assert_eq!(fired("SELECT a FROM t1 WHERE a = 1", &catalog, 0), None);
}

/// A group of three under a threshold of `2` is `joinGroupNum > threshold`,
/// the greedy arm, and the reorder happens there too -- Go's default solver is
/// not a "leave it alone" solver.
#[test]
fn a_group_larger_than_the_threshold_is_still_reordered() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, \
        (SELECT t2.a AS key_a FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.a = dt.key_a AND dt.key_a = t5.a";
    let greedy = plan(sql, &catalog, 2);
    assert_eq!(greedy, plan(sql, &catalog, 0));
    assert!(
        !greedy.iter().any(|row| row.contains("HashJoin")),
        "the written cartesian join survived the greedy: {greedy:?}",
    );
}

/// NO EQUALITY EDGE, so nothing connects the group: Go finishes such a group
/// with `makeBushyJoin` over its components, which this module declines. The
/// statement's own cartesian product stays where it was written.
#[test]
fn a_group_with_no_equality_keeps_the_written_tree() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, t2 WHERE t1.b > 1";
    assert_eq!(fired(sql, &catalog, 0), None);
    assert_eq!(fired(sql, &catalog, 10), None);
}

/// AN OUTER JOIN ANYWHERE IN THE GROUP. Go stops extracting at one unless
/// `EnableOuterJoinReorder` is on AND the join has equality conditions
/// (`rule_join_reorder.go:133-159`); this module declines the whole group,
/// which is the conservative side of that rule.
#[test]
fn an_edge_over_an_outer_join_is_refused() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1 LEFT JOIN t5 ON t1.a = t5.a JOIN t2 ON t5.a = t2.a";
    assert_eq!(fired(sql, &catalog, 0), None);
    assert_eq!(fired(sql, &catalog, 10), None);
}

/// A `STRAIGHT_JOIN` is the user asking for the written order, and is Go's
/// stop condition in the same test.
#[test]
fn a_straight_join_is_refused() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1 STRAIGHT_JOIN t5 ON t1.a = t5.a \
               JOIN t2 ON t5.a = t2.a";
    assert_eq!(fired(sql, &catalog, 0), None);
    assert_eq!(fired(sql, &catalog, 10), None);
}

/// A NON-COLUMN equi key is Go's `injectExpr` case, which materializes the
/// expression in a `Projection` the reorder then joins on. There is no way to
/// spell that in a `FROM` clause, so the group keeps its written tree rather
/// than losing the key.
///
/// The other two equalities make the group CONNECTED without the computed
/// one, so this test fails when the computed key is quietly dropped rather
/// than declined -- which is the failure the connectivity check would
/// otherwise mask.
#[test]
fn a_non_column_equi_key_is_refused() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, t2 \
               WHERE t1.a + 1 = t5.a AND t1.a = t2.a AND t2.a = t5.a";
    assert_eq!(fired(sql, &catalog, 0), None);
    assert_eq!(fired(sql, &catalog, 10), None);
}

/// The reorder changes the ROW LAYOUT, and `*` expands in row order: Go
/// restores the written order with a `Projection` (`restoreSchemaIfChanged`)
/// and this tier restores it through `FromScope::star`. Without the restore
/// the three `t5` columns would come back where `dt`'s two were.
#[test]
fn a_star_still_expands_in_the_written_order() {
    let mut catalog = tables();
    for (table, row) in [("t1", "(1, 10, 'a')"), ("t5", "(1, 50, 'e')")] {
        run_insert_on(
            &format!("INSERT INTO {table} VALUES {row}"),
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
    }
    for table in ["t2", "t3"] {
        run_insert_on(
            &format!("INSERT INTO {table} VALUES (1, 7, 'x')"),
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
    }
    let sql = "SELECT * FROM t1, t5, \
        (SELECT t2.a AS key_a, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.a = dt.key_a AND dt.key_a = t5.a";
    let ctx = crate::StmtContext::for_query();
    let written = run_select_on(sql, &catalog, &ctx).unwrap();
    let reordered = run_select_on(
        sql,
        &catalog,
        &crate::StmtContext::for_query().with_join_reorder_threshold(10),
    )
    .unwrap();
    // `t1.a, t1.b, t1.c, t5.a, t5.b, t5.c, dt.key_a, dt.doubled_b` -- the
    // WRITTEN order, whose middle three columns the reorder would otherwise
    // push to the end.
    let numbers: Vec<Vec<i64>> = written
        .iter()
        .map(|row| {
            row.iter()
                .filter_map(|value| match value {
                    Datum::Int(int) => Some(*int),
                    _ => None,
                })
                .collect()
        })
        .collect();
    assert_eq!(numbers, vec![vec![1, 10, 1, 50, 1, 14]]);
    assert_eq!(reordered, written, "the reorder moved the output columns");
}
