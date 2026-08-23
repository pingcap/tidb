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
    assert!(
        dissolved
            .first()
            .is_some_and(|row| row.starts_with("Projection_")),
        "Go restoreSchemaIfChanged keeps one root Projection: {dissolved:?}",
    );
    assert_eq!(
        dissolved
            .iter()
            .filter(|row| row
                .trim_start_matches([' ', '├', '└', '─', '│'])
                .starts_with("Projection_"))
            .count(),
        1,
        "the schema restore must not duplicate the root Projection: {dissolved:?}",
    );
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

/// AN EQUALITY THAT STOPS BEING `col = col` gets Go's INJECTED COLUMN.
///
/// `t1.b = dt.doubled_b` becomes `t1.b = t2.b * 2`, which
/// `crate::hash_join::split_equi` cannot key on. Go's `injectExpr`
/// materializes the computed side as a column of the branch that owns it, and
/// `r/planner/core/join_reorder_through_projection.result:1319` records the
/// operator that results:
///
/// ```text
/// MergeJoin                 inner join, left key:t2.a, right key:t5.a
/// ├─TableReader(Build)            TableFullScan table:t5
/// └─MergeJoin(Probe)        inner join, left key:t2.a, right key:t3.a
///   ├─TableReader(Build)          TableFullScan table:t3
///   └─IndexHashJoin(Probe)  outer key:Column, inner key:t1.b
///     ├─Projection(Build)         t2.a, mul(t2.b, 2)->Column
///     │ └─TableReader             Selection not(isnull(mul(t2.b, 2)))
///     └─IndexReader(Probe)        table:t1
/// ```
///
/// `t5` at the top, `t3` next, and `Projection(t2)` joined against `t1` at the
/// bottom is the ORDER this asserts. The dissolve must therefore CHANGE the
/// plan -- the old refusal asserted the opposite -- and the leaf order it
/// reaches is TiDB's.
#[test]
fn an_expression_join_key_gets_an_injected_column() {
    let catalog = tables();
    let sql = "SELECT t1.a, dt.key_a FROM t1, t5, \
        (SELECT t2.a AS key_a, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.b = dt.doubled_b AND dt.key_a = t5.a";
    // The recording this pins was made by mysql-tester, whose DSN sets
    // `tidb_hash_join_concurrency = 1` -- the session at which Go builds
    // this tree at all (`mysqltest_connections` measured a HEAD-built server:
    // seven of the topic's eight recorded index-join plans are unreachable at
    // the plain-session 5, where the hash side prices below them). The
    // sibling test below already runs the same session for the same reason.
    let recorded_session = |through_proj: bool| {
        ctx(through_proj, 10)
            .with_optimizer_cost_env(tidb_planner::candidate_cost::CostEnv::default(), 1.0)
    };
    let dissolved = plan(sql, &catalog, &recorded_session(true));
    assert_ne!(
        dissolved,
        plan(sql, &catalog, &recorded_session(false)),
        "the expression join key was still declined",
    );
    // The `Projection` Go injects, over `t2` alone, as the BUILD side of the
    // bottom join -- and `t5`, `t3` above it in that order.
    //
    // CORRECTION. This pinned the ALL-HASH tree until the promise/verify
    // contract landed (see `crate::driver::merge_decision`'s CORRECTION). It
    // now pins TiDB's own SHAPE, quoted in this test's doc above: two
    // `MergeJoin`s over an index join, with the two upper leaves reading the
    // TABLE in handle order rather than walking a covering index -- an index
    // walk is not the handle's order, so a leaf asked for that order declines
    // the index path, which is why `IndexFullScan_1`/`_2` became
    // `TableFullScan`. TiDB prints `keep order:true` on exactly those two.
    //
    // SECOND CORRECTION. The bottom operator read `IndexJoin_6` here while
    // Go's recording reads `IndexHashJoin`, and that last divergence is
    // closed: `plan_trace::index_join_operator` costs the two kinds with the
    // one term that differs between them and keeps the cheaper, which at this
    // site is Go's own answer (its measurement is quoted in that function).
    // The join families and leaf order are now the recorded ones. The trace
    // also exposes every reader boundary it can prove; it deliberately keeps
    // the scan below the injected Projection bare instead of claiming a
    // pushdown boundary through that expression.
    //
    // THIRD CORRECTION. Two changes, each toward the recording. The session:
    // this test priced a PLAIN session (`tidb_hash_join_concurrency = 5`)
    // against a tree TiDB only builds at mysql-tester's 1, so it pinned a
    // comparison Go itself decides the other way at 5; it now runs the
    // recording's own session, like the sibling test below. The tree: the
    // join key's null-rejection now reaches t2's leaf THROUGH the injected
    // Projection (`derived_projection_pushdown`, Go `breakDownPredicates`),
    // so the recording's `Selection  not(isnull(mul(t2.b, 2)))` under that
    // Projection exists here too -- `Selection_6` below. The recording reads
    // it as `TableReader -> Selection cop[tikv]`.
    //
    // FOURTH CORRECTION. That reader boundary exists here now. Go closes
    // EVERY base-table read as a cop task capped by a reader
    // (`convertToTableScan` -> `addPushedDownSelection4PhysicalTableScan` ->
    // `ConvertToRootTask`), and `driver::select_rows` records the same
    // boundary for the single-table read below the injected Projection --
    // `TableReader_6` over `TableFullScan_5`, which is why every id above it
    // moved by one. The null-rejection itself stays at ROOT, which is Go's
    // own `CopTask.RootTaskConds` placement for a condition
    // `expression.PushDownExprs` refuses: `mul(int, int)` and `isnull` are
    // not in `tidb_expr::pushdown_catalog`, so no source here can promise to
    // evaluate them and this tier must not print a promise it cannot keep.
    // What is left of the residue is therefore the CATALOG's width, named in
    // `predicate_pushdown`'s module doc, not the reader boundary.
    assert_eq!(
        dissolved,
        vec![
            "MergeJoin_14".to_owned(),
            "├─TableReader_2(Build)".to_owned(),
            "│ └─TableFullScan_1".to_owned(),
            "└─MergeJoin_13(Probe)".to_owned(),
            "  ├─TableReader_4(Build)".to_owned(),
            "  │ └─TableFullScan_3".to_owned(),
            "  └─IndexHashJoin_12(Probe)".to_owned(),
            "    ├─Projection_8(Build)".to_owned(),
            "    │ └─Selection_7".to_owned(),
            "    │   └─TableReader_6".to_owned(),
            "    │     └─TableFullScan_5".to_owned(),
            "    └─IndexReader_11(Probe)".to_owned(),
            "      └─Selection_10".to_owned(),
            "        └─IndexRangeScan_9".to_owned(),
        ],
    );
}

/// THE OUTER LEAF OF THAT INDEX JOIN KEEPS ORDER, because the `MergeJoin`
/// above it relies on `t2.a` and the derived table the injected `Projection`
/// became forwards the request to the leaf that answers it.
///
/// `r/planner/core/join_reorder_through_projection.result:1319` records
/// `TableFullScan table:t2 ... keep order:true` under exactly that
/// `Projection`. Before `merge_decision::from_required_prop` this tier
/// printed `keep order:false` there -- a `MergeJoin` over a stream nothing
/// had put in order.
#[test]
fn the_index_joins_outer_leaf_is_asked_for_the_order_through_the_derived_table() {
    let catalog = tables();
    let sql = "SELECT t1.a, dt.key_a FROM t1, t5, \
        (SELECT t2.a AS key_a, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.b = dt.doubled_b AND dt.key_a = t5.a";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    // The recording this test pins was made by mysql-tester, whose DSN sets
    // `tidb_hash_join_concurrency = 1` in every connection; `getHashJoins`
    // stamps that value on the candidate and
    // `getPlanCostVer24PhysicalHashJoin` divides the probe terms by it, so
    // at the plain-session 5 the hash alternative is charged what five
    // workers share and WINS this comparison -- a different session's plan.
    // The chooser now reads the session value, so the session must say 1.
    let ctx = ctx(true, 10).with_optimizer_cost_env(
        tidb_planner::candidate_cost::CostEnv::default(),
        1.0,
    );
    let (_, rows) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Row,
    )
    .unwrap();
    let text = |datum: &Datum| match datum {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    // `TableFullScan_5`: the scan under `TableReader_6` under `Selection_7`
    // under `Projection_8(Build)` -- the tree the family's pin asserts whole.
    // The Selection is the join key's null-rejection, pushed through the
    // wrapper by `derived_projection_pushdown`, which is also why the scan's
    // number moved from `_3`.
    let t2 = rows
        .iter()
        .find(|row| text(&row[0]).contains("TableFullScan_5"))
        .expect("the injected projection's own leaf");
    assert!(
        text(&t2[4]).starts_with("keep order:true"),
        "the leaf under the injected projection: {}",
        text(&t2[4]),
    );
}

/// THE INJECTED WRAPPER IS PRUNED, AND ITS LEAF TAKES THE COVERING INDEX.
///
/// Go's column pruning runs before join reorder, so `injectExpr` republishes
/// the leaf's PRUNED schema (`Column2Exprs(p.Schema().Columns)`), and the
/// join key's `not(isnull(...))` -- derived by `LogicalJoin.PredicatePushDown`
/// -- was substituted through the derived projection into the leaf
/// (`breakDownPredicates`, `logical_projection.go:647`) before the reorder
/// dissolved it. `r/planner/core/join_reorder_through_projection.result:1604`
/// records what the two together produce for `t1.b = dt.doubled_b` at
/// `tidb_opt_join_reorder_threshold = 0`:
///
/// ```text
/// HashJoin        inner join, equal:[eq(Column, t1.b)]
/// ├─Projection(Build)   t2.a, t2.b, mul(t2.b, 2)->Column   <- no t2.c
/// │ └─IndexReader       index:Selection
/// │   └─Selection       not(isnull(mul(t2.b, 2)))
/// │     └─IndexFullScan table:t2, index:b(b)               <- COVERING
/// ```
///
/// The leaf's reader boundary exists here now; the null-rejection is the one
/// piece still ABOVE it rather than inside, for the reason the assertion
/// below states.
///
/// Publishing `t2.c` -- which nothing above the wrapper reads -- forced a
/// `TableFullScan` here for as long as the wrapper republished every column,
/// and was the last recorded divergence class of this topic. Each assertion
/// names the mechanism whose removal re-opens it: the pruned field list is
/// `through_proj::used_base_columns`, the Selection BELOW the projection is
/// `derived_projection_pushdown`'s computed-output substitution, and the
/// covering walk is the access chooser reading the narrowed demand.
#[test]
fn the_injected_wrapper_is_pruned_and_its_leaf_takes_the_covering_index() {
    let catalog = tables();
    let sql = "SELECT t1.*, dt.* FROM t1, \
        (SELECT t2.a AS key_a, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.b = dt.doubled_b";
    let ctx = ctx(true, 0).with_optimizer_cost_env(
        tidb_planner::candidate_cost::CostEnv::default(),
        1.0,
    );
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Row,
    )
    .unwrap();
    let text = |datum: &Datum| match datum {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let find = |needle: &str| {
        rows.iter()
            .find(|row| text(&row[0]).contains(needle))
            .unwrap_or_else(|| panic!("no `{needle}` operator in the plan"))
    };
    // The pruned pass-through half: t2.a and t2.b survive, t2.c does not.
    let projection = rows
        .iter()
        .find(|row| text(&row[0]).contains("Projection") && text(&row[4]).contains("mul"))
        .expect("the injected wrapper's Projection");
    assert!(
        text(&projection[4]).contains("t2.a") && text(&projection[4]).contains("t2.b"),
        "the wrapper's pass-through half: {}",
        text(&projection[4]),
    );
    assert!(
        !text(&projection[4]).contains("t2.c"),
        "an unread column survived the pruning: {}",
        text(&projection[4]),
    );
    // The join key's null-rejection, substituted BELOW the projection.
    //
    // The recording reads it INSIDE the cop task (`IndexReader
    // index:Selection` over `Selection cop[tikv]`); this tier now closes the
    // leaf's cop task -- `IndexReader_4` over `IndexFullScan_3` -- but the
    // conjunct itself stays at root, because `mul(int, int)` and `isnull`
    // are not in `tidb_expr::pushdown_catalog`, so no source here can promise
    // to evaluate it. That is Go's own `CopTask.RootTaskConds` placement for
    // a condition `expression.PushDownExprs` refuses; what differs from the
    // recording is which conditions the catalog admits, not where each half
    // runs.
    let selection = find("Selection_5");
    assert_eq!(
        text(&selection[4]),
        "not(isnull(mul(test.t2.b, 2)))",
        "the null-rejection under the wrapper",
    );
    // The covering walk the pruning makes possible.
    let scan = find("IndexFullScan_3");
    assert_eq!(text(&scan[3]), "table:t2, index:b(b)");
}

/// A `leading` HINT PINS THE ORDER, so the projection is not dissolved.
///
/// Go dissolves AND honours the hint through `s.leadingJoinGroup`; nothing
/// here models that prefix, so dissolving would answer a question the
/// statement already answered.
/// `r/planner/core/join_reorder_through_projection.result:2349` records
/// `leading(t2, t3, t1)` keeping `MergeJoin(t2, t3)` under the injected
/// `Projection` -- the tree the UNDISSOLVED statement already builds.
#[test]
fn a_leading_hint_declines_the_dissolve() {
    let catalog = tables();
    let sql = "SELECT /*+ leading(t2, t3, t1) */ t1.a, dt.a2 FROM t1 JOIN ( \
        SELECT t2.a AS a2, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a \
        ) dt ON t1.b = dt.doubled_b";
    assert_eq!(
        plan(sql, &catalog, &ctx(true, 0)),
        plan(sql, &catalog, &ctx(false, 0)),
        "a leading hint was reordered past",
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

/// AN EQUALITY WHOSE TWO SIDES LIVE IN ONE RELATION is not a join edge, so
/// there is no key to preserve and nothing to inject.
///
/// `dt.k = dt.doubled_b` becomes `t2.a = t2.b * 2`: Go's `checkConnection`
/// never builds an eq-edge from a condition both of whose sides come from one
/// node, so `injectExpr` is never called for it either. Materializing a column
/// for it anyway would wrap `t2` in a projection that buys no key, so the
/// statement keeps the derived table it was written with.
#[test]
fn an_equality_inside_one_relation_is_not_injected() {
    let catalog = tables();
    let sql = "SELECT t1.a, dt.a2 FROM t1, \
        (SELECT t2.a AS a2, t2.a AS k, t2.b * 2 AS doubled_b \
         FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.a = dt.a2 AND dt.k = dt.doubled_b";
    assert_eq!(
        plan(sql, &catalog, &ctx(true, 10)),
        plan(sql, &catalog, &ctx(false, 10)),
        "a single-relation equality was injected into",
    );
}

/// A QUALIFIED `*` OVER A RELATION THE INJECTION WRAPS would gain a column.
///
/// `t1.b * 2 = dt.doubled_b` becomes `t1.b * 2 = t2.b * 2`, so BOTH sides are
/// computed and both `t1` and `t2` get an injected column. `t1` is a relation
/// the statement itself named, so `t1.*` would expand to one row shape wider
/// than the statement asked for. Go never meets this: `restoreSchemaIfChanged`
/// rebuilds the original schema by `UniqueID` above the reordered join.
///
/// The assertion is on the output column NAMES, which is the contract the
/// restore owes; the plan may legitimately differ once it holds.
#[test]
fn a_wildcard_over_an_injected_relation_keeps_its_columns() {
    let catalog = tables();
    let sql = "SELECT t1.* FROM t1, \
        (SELECT t2.a AS a2, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.b * 2 = dt.doubled_b";
    let (names, _) = result(sql, &catalog, &ctx(true, 10));
    let (kept_names, _) = result(sql, &catalog, &ctx(false, 10));
    assert_eq!(names, vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]);
    assert_eq!(names, kept_names, "the wildcard gained an injected column");
}

/// A COMPUTED OPERAND SPANNING TWO RELATIONS has no branch to sit on.
///
/// `injectExpr` materializes the expression as a column of ONE node -- Go's
/// `*leftPlan` or `*rightPlan` -- so `t1.b + t5.b` belongs to neither. A
/// wrapper for it would be a derived table whose subquery reads a column its
/// own `FROM` does not have.
#[test]
fn a_computed_key_over_two_relations_is_declined() {
    let catalog = tables();
    let sql = "SELECT t1.a, dt.a2 FROM t1, t5, \
        (SELECT t2.a AS a2, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.a = dt.a2 AND t1.b + t5.b = dt.doubled_b";
    assert_eq!(
        plan(sql, &catalog, &ctx(true, 10)),
        plan(sql, &catalog, &ctx(false, 10)),
        "a two-relation computed key was injected",
    );
}
