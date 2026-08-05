//! WHEN A PROJECTION DISSOLVES INTO THE JOIN GROUP, AND WHAT SURVIVES IT.
//!
//! Every test here is a mutation probe on
//! [`crate::driver::through_proj`]: each one fails if a specific piece of the
//! rewrite is removed or gets the wrong answer, and each names the piece.
//!
//! The oracle for the one plan that IS reordered is TiDB's own recording,
//! quoted in [`the_group_reaches_the_tree_tidb_records`]. The rest hold the
//! rewrite's own contract: the two gates, the output NAMES the restore has to
//! preserve, the columns a substitution has to pick, and the equality shape
//! this tier declines to dissolve.

use super::*;
use crate::StmtContext;

/// The schema and the data
/// `t/planner/core/join_reorder_through_projection.test` sets up, for the
/// tables the statements below read.
fn tables() -> Catalog {
    let mut catalog = Catalog::default();
    for name in ["t1", "t2", "t3", "t5"] {
        crate::run_create_table_on(
            &format!("CREATE TABLE {name} (a INT, b INT, c VARCHAR(32), PRIMARY KEY (a), KEY(b))"),
            &mut catalog,
        )
        .unwrap();
    }
    let rows = [
        (
            "t1",
            "(1, 10, 'a1'), (2, 20, 'a2'), (3, 30, 'a3'), (4, 200, 'a4')",
        ),
        ("t2", "(1, 100, 'b1'), (2, 200, 'b2'), (3, 300, 'b3')"),
        ("t3", "(1, 1000, 'c1'), (2, 2000, 'c2'), (3, 3000, 'c3')"),
        (
            "t5",
            "(1, 10, 'e1'), (2, 20, 'e2'), (3, 30, 'e3'), (4, 40, 'e4')",
        ),
    ];
    for (name, values) in rows {
        crate::run_insert_on(
            &format!("INSERT INTO {name} VALUES {values}"),
            &mut catalog,
            &StmtContext::for_dml(false, true, false),
        )
        .unwrap();
    }
    catalog
}

/// Both of the rewrite's gates, as a session sets them.
fn ctx(through_proj: bool, threshold: i32) -> StmtContext {
    StmtContext::for_query()
        .with_join_reorder_threshold(threshold)
        .with_join_reorder_through_proj(through_proj)
}

/// The plan `EXPLAIN` prints, one operator name per line with its tree prefix.
fn plan(sql: &str, catalog: &Catalog, ctx: &StmtContext) -> Vec<String> {
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
        ctx,
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

/// The output column NAMES and the rows, which is what the restore is
/// answerable to: a dissolve must not rename or reorder a single one.
fn result(sql: &str, catalog: &Catalog, ctx: &StmtContext) -> (Vec<String>, Vec<Vec<Datum>>) {
    let (columns, rows) = crate::run_select_meta_on(sql, catalog, ctx).unwrap();
    (columns.into_iter().map(|(name, _)| name).collect(), rows)
}

/// The recorded statement at
/// `r/planner/core/join_reorder_through_projection.result:1270`, which the
/// topic runs at `set tidb_opt_join_reorder_threshold = 10` and
/// `set tidb_opt_join_reorder_through_proj = on`.
const THREE_WAY: &str = "SELECT t1.a, dt.key_a, dt.doubled_b FROM t1, t5, \
     (SELECT t2.a AS key_a, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a) dt \
     WHERE t1.a = dt.key_a AND dt.key_a = t5.a";

/// THE TREE. With the projection dissolved the group is `{t1, t5, t2, t3}`,
/// and the recording is
///
/// ```text
/// Projection            t1.a, t2.a, mul(t2.b, 2)
/// └─MergeJoin           inner join, left key:t2.a, right key:t5.a
///   ├─TableReader(Build)      TableFullScan table:t5  keep order:true
///   └─MergeJoin(Probe)  inner join, left key:t2.a, right key:t3.a
///     ├─TableReader(Build)    TableFullScan table:t3  keep order:true
///     └─MergeJoin(Probe)      inner join, left key:t1.a, right key:t2.a
///       ├─TableReader(Build)  TableFullScan table:t2  keep order:true
///       └─TableReader(Probe)  TableFullScan table:t1  keep order:true
/// ```
///
/// The load-bearing part is `MergeJoin(t1, t2)` at the bottom: a pair whose
/// two sides are single leaves, which no tree that keeps `dt` whole can form,
/// and which is exactly the ordered-merge pair `difftest-result-tests`'
/// `join_shape` counts for this statement.
#[test]
fn the_group_reaches_the_tree_tidb_records() {
    let catalog = tables();
    let dissolved = plan(THREE_WAY, &catalog, &ctx(true, 10));
    let kept = plan(THREE_WAY, &catalog, &ctx(false, 10));
    assert_ne!(
        dissolved, kept,
        "dissolving the projection produced the same tree as keeping it",
    );
    // `t1` and `t2` under one join, each a single leaf: the recorded pair.
    let bottom = dissolved
        .iter()
        .rposition(|row| row.contains("MergeJoin"))
        .expect("a merge join");
    assert!(
        dissolved[bottom + 1..]
            .iter()
            .filter(|row| row.contains("TableFullScan"))
            .count()
            == 2,
        "the bottom merge is not over two single leaves: {dissolved:?}",
    );
}

/// GATE ONE, Go's own: `@@tidb_opt_join_reorder_through_proj` is `OFF` in a
/// shipped session (`vardef.DefTiDBOptJoinReorderThroughProj`), and a
/// projection over a join is then an atomic group leaf.
///
/// Remove the variable check in `through_proj::inline` and this fails.
#[test]
fn the_variable_off_keeps_the_derived_table_whole() {
    let catalog = tables();
    assert_eq!(
        plan(THREE_WAY, &catalog, &ctx(false, 10)),
        plan(THREE_WAY, &catalog, &ctx(false, 10)),
    );
    assert_ne!(
        plan(THREE_WAY, &catalog, &ctx(false, 10)),
        plan(THREE_WAY, &catalog, &ctx(true, 10)),
        "the plan is the same with the variable off, so the gate is not read",
    );
}

/// THE THRESHOLD IS NOT A GATE. `@@tidb_opt_join_reorder_through_proj` is Go's
/// only condition (`extractJoinGroupImpl`), and the topic that exercises this
/// runs almost every one of its statements with the variable ON and the
/// threshold left at its default `0` -- which is Go's GREEDY solver, modelled
/// in `driver::join_reorder`. So the dissolve must happen at the default
/// threshold too, and it must reach the same tree the DP does here.
///
/// Restore the old `threshold <= 0` gate and this fails.
#[test]
fn the_default_threshold_still_dissolves_the_derived_table() {
    let catalog = tables();
    assert_ne!(
        plan(THREE_WAY, &catalog, &ctx(true, 0)),
        plan(THREE_WAY, &catalog, &ctx(false, 0)),
        "the projection did not dissolve under the default threshold",
    );
    assert_eq!(
        plan(THREE_WAY, &catalog, &ctx(true, 0)),
        plan(THREE_WAY, &catalog, &ctx(true, 10)),
        "the greedy and the DP disagree once the projection dissolves",
    );
}

/// THE RESTORE. Go rebuilds the original output schema with a `Projection`
/// over `colExprMap`; here the same restore is the alias each rewritten field
/// carries. `dt.key_a` must still be called `key_a` after it becomes `t2.a`,
/// and `dt.doubled_b` must still be `doubled_b` after it becomes `t2.b * 2`.
///
/// Drop the alias in `through_proj::inline`'s field arm and the names collapse
/// to `a` and `t2.b * 2`, which is what this pins.
#[test]
fn the_output_names_and_rows_survive_the_dissolve() {
    let catalog = tables();
    let (names, rows) = result(THREE_WAY, &catalog, &ctx(true, 10));
    let (kept_names, kept_rows) = result(THREE_WAY, &catalog, &ctx(false, 10));
    assert_eq!(
        names,
        vec!["a".to_owned(), "key_a".to_owned(), "doubled_b".to_owned()],
        "the dissolve renamed an output column",
    );
    assert_eq!(names, kept_names);
    let sorted = |mut rows: Vec<Vec<Datum>>| {
        rows.sort_by_key(|row| format!("{row:?}"));
        rows
    };
    assert_eq!(sorted(rows), sorted(kept_rows), "the dissolve moved a row");
}

/// THE SUBSTITUTION picks the column the projection actually defined. Go keys
/// `colExprMap` by `UniqueID`; here it is keyed by name, so a lookup that
/// returned the wrong field would silently swap two output columns of one
/// dissolved table. `key_a` is `t2.a` (1, 2, 3) and `other_b` is `t3.b`
/// (1000, 2000, 3000), so any swap moves every row.
#[test]
fn the_substitution_picks_the_defining_column() {
    let catalog = tables();
    let sql = "SELECT dt.key_a, dt.other_b FROM t1, \
        (SELECT t2.a AS key_a, t3.b AS other_b FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.a = dt.key_a";
    let (names, rows) = result(sql, &catalog, &ctx(true, 10));
    let (kept_names, kept_rows) = result(sql, &catalog, &ctx(false, 10));
    assert_eq!(names, kept_names);
    let sorted = |mut rows: Vec<Vec<Datum>>| {
        rows.sort_by_key(|row| format!("{row:?}"));
        rows
    };
    assert_eq!(
        sorted(rows.clone()),
        vec![
            vec![Datum::Int(1), Datum::Int(1000)],
            vec![Datum::Int(2), Datum::Int(2000)],
            vec![Datum::Int(3), Datum::Int(3000)],
        ],
        "the substitution did not pick the defining column",
    );
    assert_eq!(sorted(rows), sorted(kept_rows));
}

/// `dt.*` EXPANDS to the dissolved table's own field list, in its own order
/// and under its own names -- the wildcard half of the restore.
#[test]
fn a_qualified_star_expands_to_the_dissolved_fields() {
    let catalog = tables();
    let sql = "SELECT t1.a, dt.* FROM t1, \
        (SELECT t2.a AS key_a, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.a = dt.key_a";
    let (names, rows) = result(sql, &catalog, &ctx(true, 10));
    let (kept_names, kept_rows) = result(sql, &catalog, &ctx(false, 10));
    assert_eq!(
        names,
        vec!["a".to_owned(), "key_a".to_owned(), "doubled_b".to_owned()],
    );
    assert_eq!(names, kept_names);
    let sorted = |mut rows: Vec<Vec<Datum>>| {
        rows.sort_by_key(|row| format!("{row:?}"));
        rows
    };
    assert_eq!(sorted(rows), sorted(kept_rows));
}

/// AN EQUALITY THAT WOULD STOP BEING `col = col` is not dissolved.
///
/// `t1.b = dt.doubled_b` becomes `t1.b = t2.b * 2`, and
/// `crate::hash_join::split_equi` takes a key only when both sides are
/// columns -- Go materializes it with `injectExpr`, which a rebuilt `FROM`
/// clause cannot spell. Dissolving here would turn a hash join into a nested
/// loop, so the statement keeps the derived table it was written with.
#[test]
fn an_expression_join_key_is_declined() {
    let catalog = tables();
    let sql = "SELECT t1.a, dt.key_a FROM t1, t5, \
        (SELECT t2.a AS key_a, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.b = dt.doubled_b AND dt.key_a = t5.a";
    assert_eq!(
        plan(sql, &catalog, &ctx(true, 10)),
        plan(sql, &catalog, &ctx(false, 10)),
        "an expression join key was dissolved",
    );
}

/// A NON-DETERMINISTIC projection expression is not dissolved:
/// `canInlineProjectionBasic` rejects `unFoldableFunctions`, because join
/// reorder may move or duplicate where the expression is evaluated.
#[test]
fn a_non_deterministic_projection_is_declined() {
    let catalog = tables();
    let sql = "SELECT t1.a, dt.key_a FROM t1, \
        (SELECT t2.a AS key_a, rand() AS r FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.a = dt.key_a";
    assert_eq!(
        plan(sql, &catalog, &ctx(true, 10)),
        plan(sql, &catalog, &ctx(false, 10)),
        "a projection holding rand() was dissolved",
    );
}

/// A CROSS-LEAF projection expression is not dissolved: `canInlineProjection`
/// requires every expression to depend on exactly ONE leaf, because the
/// reorder has to attribute it to one side of a join.
#[test]
fn a_cross_leaf_projection_expression_is_declined() {
    let catalog = tables();
    let sql = "SELECT t1.a, dt.key_a FROM t1, \
        (SELECT t2.a AS key_a, t2.b + t3.b AS both_b FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.a = dt.key_a";
    assert_eq!(
        plan(sql, &catalog, &ctx(true, 10)),
        plan(sql, &catalog, &ctx(false, 10)),
        "a projection spanning two leaves was dissolved",
    );
}
