//! WHEN THE JOIN REORDER FIRES, AND WHAT IT LEAVES ALONE.
//!
//! Every test here names the gate it is about and asserts the PLAN, because
//! the reorder's whole observable effect is which relation ends up under which
//! join. The oracle for the one plan that IS reordered is TiDB's own
//! recording, quoted in [`the_dp_builds_the_tree_tidb_records`].
//!
//! The three refusals are the load-bearing ones: a group with no equality is a
//! cartesian product Go's `makeBushyJoin` owns and this tier declines, an
//! outer join is Go's own stop condition (`rule_join_reorder.go:133-159`), and
//! a threshold that no group fits under is the DEFAULT session -- which is why
//! a stock TiDB never reaches the DP at all.

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
/// (the smallest is 2), so Go takes the greedy solver and this tier takes the
/// tree as written. This is what makes the reorder's blast radius on every
/// topic that does not `SET` the variable exactly zero.
#[test]
fn the_default_threshold_keeps_the_written_tree() {
    let catalog = tables();
    let written = plan(THREE_WAY, &catalog, 0);
    assert_ne!(
        written,
        plan(THREE_WAY, &catalog, 10),
        "the default threshold produced the DP's tree",
    );
    // `(t1, t5)` is the written bottom join, and it has no equality: a
    // cartesian product, which is precisely what the reorder removes.
    assert!(
        written.iter().any(|row| row.contains("HashJoin")),
        "the written tree should still hash: {written:?}",
    );
}

/// A group LARGER than the threshold takes the same greedy path. Two tables
/// under a threshold of `1` is Go's `joinGroupNum > threshold`.
#[test]
fn a_group_larger_than_the_threshold_keeps_the_written_tree() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, \
        (SELECT t2.a AS key_a FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.a = dt.key_a AND dt.key_a = t5.a";
    assert_eq!(
        plan(sql, &catalog, 2),
        plan(sql, &catalog, 0),
        "a three-relation group was reordered under a threshold of 2",
    );
}

/// NO EQUALITY EDGE, so nothing connects the group: Go finishes such a group
/// with `makeBushyJoin` over its components, which this module declines. The
/// statement's own cartesian product stays where it was written.
#[test]
fn a_group_with_no_equality_keeps_the_written_tree() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, t2 WHERE t1.b > 1";
    assert_eq!(
        plan(sql, &catalog, 10),
        plan(sql, &catalog, 0),
        "an edgeless group was reordered",
    );
}

/// A group whose equality graph is DISCONNECTED is the same refusal: `t1--t5`
/// is an edge and `t2` is joined to nothing.
#[test]
fn a_disconnected_group_keeps_the_written_tree() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, t2 WHERE t1.a = t5.a";
    assert_eq!(
        plan(sql, &catalog, 10),
        plan(sql, &catalog, 0),
        "a disconnected group was reordered",
    );
}

/// AN OUTER JOIN ANYWHERE IN THE GROUP. Go stops extracting at one unless
/// `EnableOuterJoinReorder` is on AND the join has equality conditions
/// (`rule_join_reorder.go:133-159`); this module declines the whole group,
/// which is the conservative side of that rule.
#[test]
fn an_edge_over_an_outer_join_is_refused() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1 LEFT JOIN t5 ON t1.a = t5.a JOIN t2 ON t5.a = t2.a";
    assert_eq!(
        plan(sql, &catalog, 10),
        plan(sql, &catalog, 0),
        "a group containing an outer join was reordered",
    );
}

/// A `STRAIGHT_JOIN` is the user asking for the written order, and is Go's
/// stop condition in the same test.
#[test]
fn a_straight_join_is_refused() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1 STRAIGHT_JOIN t5 ON t1.a = t5.a \
               JOIN t2 ON t5.a = t2.a";
    assert_eq!(
        plan(sql, &catalog, 10),
        plan(sql, &catalog, 0),
        "a STRAIGHT_JOIN group was reordered",
    );
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
    assert_eq!(
        plan(sql, &catalog, 10),
        plan(sql, &catalog, 0),
        "a group with a computed equi key was reordered",
    );
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
