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

//! Go's join reorder -- BOTH solvers -- over the `FROM` clause this tier plans
//! from.
//!
//! # Which solver runs
//!
//! `JoinReOrderSolver.optimizeRecursive` picks between two solvers
//! (`rule_join_reorder.go:374`):
//!
//! ```go
//! useGreedy := !allInnerJoin || joinGroupNum > ctx.GetSessionVars().TiDBOptJoinReorderThreshold
//! ```
//!
//! [`collect`] only ever yields an ALL-INNER group -- an outer join is one of
//! its stop conditions -- so `allInnerJoin` is true here and the choice is the
//! group size alone:
//!
//! | `joinGroupNum` | `tidb_opt_join_reorder_threshold` | solver |
//! | --- | --- | --- |
//! | `< 2` | any | neither; there is no group to reorder |
//! | `n >= 2` | `n <= threshold` | [`solve`], Go's `joinReorderDPSolver` |
//! | `n >= 2` | `n > threshold` | [`greedy_solve`], Go's `joinReorderGreedySolver` |
//!
//! `vardef.DefTiDBOptJoinReorderThreshold` is `0`, so a stock session always
//! lands in the last row: the GREEDY solver is the default one, and it fires
//! on every multi-relation inner join. The DP is reachable only from a session
//! that RAISED the threshold; the one enrolled topic that does is
//! `planner/core/join_reorder_through_projection`, which sets it to `10`, `3`
//! and `63` around the statements it exercises.
//!
//! The threshold is not only the solver switch: it also gates the `0.9`
//! per-remaining-key correlation factor in `EstimateFullJoinRowCount`
//! (`cardinality/join.go:45`), which is why the cost model each solver reads
//! is built from the SAME [`DeriveStatsContext::with_join_reorder_threshold`].
//!
//! # What is reordered, and what is declined
//!
//! [`collect`] is Go's `extractJoinGroup` narrowed to the shapes this tier can
//! COST, and it declines rather than approximates: `None` leaves the caller
//! building the tree exactly as written, which is the behaviour that predates
//! this module. The stop conditions are Go's own
//! (`rule_join_reorder.go:133-159`) -- an outer join, a `STRAIGHT_JOIN`, a
//! `NATURAL`/`USING` join, a `LATERAL` derived table -- with the difference
//! that Go keeps a stopped-at subtree as an atomic group member and still
//! reorders around it, while this module declines the whole group. Three
//! further declines are this module's own scope:
//!
//! * a NON-COLUMN equi key (`t1.a + 1 = t2.a`). Go materializes one with an
//!   injected `Projection` (`baseSingleGroupJoinOrderSolver.injectExpr`);
//!   there is no way to spell that in a `FROM` clause, so the group keeps its
//!   written tree.
//! * a group whose equality graph is DISCONNECTED. Go finishes with
//!   `makeBushyJoin` over the components; here the statement's own cartesian
//!   product stays where it was written.
//! * a leaf whose row count cannot be derived -- a derived table that
//!   aggregates, sorts, limits or unions. Guessing one would silently choose a
//!   different join order.
//!
//! The GREEDY arm declines one thing more, because it is the default solver
//! and a wrong order there is corpus-wide rather than opt-in: a conjunct that
//! spans several leaves without being an equality, or that reads a column this
//! group does not own. Such a conjunct is one of Go's `otherConds`, and it
//! changes greedy decisions in two ways this module does not model -- Go's
//! `hasOtherJoinCondition` makes a pair NON-cartesian and therefore joinable,
//! and `makeJoin` hands the conjunct to whichever join first covers it, which
//! moves that join's row count. Declining keeps the written tree instead of
//! guessing.
//!
//! # The cost model is not re-derived here
//!
//! Every row count comes from [`tidb_planner::cardinality::derive_stats`],
//! which is Go's `RecursiveDeriveStats` for the node kinds a `FROM` group
//! reaches. This module's job on that side is only to BUILD its input: which
//! `DataSource`s exist, what selectivity was pushed into each, and which
//! column is which. See [`Rel`] and [`emit`].

use std::collections::BTreeSet;

use tidb_ast::{BinaryOp, Expr, Join, JoinNode, JoinType, QueryStmt, SelectField};
use tidb_datatype::FieldType;
use tidb_planner::cardinality::derive_stats::{
    derive_stats, ColumnId, DeriveStatsContext, LogicalNode, ProjectionExpr,
};

use crate::driver::catalog::{Catalog, TableEntry};
use crate::kv_table::KvTable;

/// Go's `not(isnull(col))` over a bare NULLABLE column of a pseudo table.
///
/// The recorded oracle is `9990.00` of `10000` rows -- the `IndexReader` under
/// `t1.b = dt.doubled_b` in
/// `r/planner/core/join_reorder_through_projection.result`.
/// `GetRowCountByColumnRanges` for `(NULL, +inf]` subtracts one pseudo-equal
/// bucket, `1/pseudoEqualRate`.
const NOT_NULL_RATE: f64 = 1.0 - 1.0 / 1000.0;

/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: u32 = 1;

/// The result of a successful reorder.
pub(crate) struct Reordered {
    /// The rebuilt `FROM` tree.
    pub(crate) join: Join,
    /// Where each WRITTEN leaf now sits: `written_order[i]` is the rebuilt
    /// tree's left-to-right position of the statement's `i`-th leaf.
    ///
    /// Go restores the original output order with a `Projection`
    /// (`restoreSchemaIfChanged`); this tier restores it by telling the scope
    /// which row offsets `*` expands to, the same escape hatch a `RIGHT JOIN`
    /// already uses (`FromScope::star`).
    pub(crate) written_order: Vec<usize>,
}

/// One leaf of the join group: a relation the reorder moves but never opens.
struct Leaf<'a> {
    /// The `FROM` node, cloned verbatim into the rebuilt tree.
    node: &'a JoinNode,
    /// The name a qualified column reference uses to reach it.
    visible: String,
    /// Its output column names, in order.
    columns: Vec<String>,
    /// Its cost model, before any pushed-down predicate.
    rel: Rel<'a>,
}

/// The relation shapes [`derive_stats`] can be built for.
enum Rel<'a> {
    /// A base table: Go's `DataSource`.
    Table(TableRel<'a>),
    /// A derived table over an all-inner join of further relations: Go's
    /// `LogicalProjection` over a `LogicalJoin` tree.
    Derived(DerivedRel<'a>),
}

struct TableRel<'a> {
    table: &'a KvTable,
    /// `StatisticTable.RealtimeCount`.
    realtime: f64,
    /// The statistics `Selectivity` reads, when any are loaded.
    stats: Option<&'a crate::access_cost::TableStatistics>,
    /// One [`ColumnId`] per visible column, in schema order.
    ids: Vec<ColumnId>,
    /// The visible columns, for the single-table scope `Selectivity` needs.
    columns: Vec<(String, FieldType)>,
    /// Whether each visible column may be NULL, which decides whether a join
    /// key costs a `not(isnull(...))` at all.
    nullable: Vec<bool>,
}

struct DerivedRel<'a> {
    /// The alias a column reference qualifies this relation by.
    visible: String,
    /// One entry per output column: its defining expression.
    exprs: Vec<&'a Expr>,
    /// The output columns' names, for a conjunct written against them.
    names: Vec<String>,
    /// The output columns' ids.
    ids: Vec<ColumnId>,
    /// The subquery's own `FROM` leaves.
    inner: Vec<Leaf<'a>>,
    /// The subquery's own equi edges, as `(leaf, column)` pairs into `inner`.
    inner_edges: Vec<((usize, usize), (usize, usize))>,
    /// The subquery's own single-leaf conjuncts, per inner leaf.
    inner_filters: Vec<Vec<Expr>>,
}

/// What a parent pushed into a relation.
#[derive(Default)]
struct Demand {
    /// Output columns an equi key made `not(isnull(...))`.
    not_null: BTreeSet<usize>,
    /// Output columns a predicate constrains through an EXPRESSION rather than
    /// through the column itself. Go's `Selectivity` cannot cover such a
    /// condition with a column statistics node, so the whole leftover mask
    /// takes ONE `selectionFactor` (`selectivity.go`'s trailing
    /// `ret *= selectionFactor`), however many conditions are in it.
    expression: BTreeSet<usize>,
    /// Conjuncts over this relation's own columns.
    filters: Vec<Expr>,
}

/// Hands out the [`ColumnId`]s `derive_stats` keys its NDV maps by.
#[derive(Default)]
struct Ids(ColumnId);

impl Ids {
    fn take(&mut self, count: usize) -> Vec<ColumnId> {
        (0..count)
            .map(|_| {
                self.0 += 1;
                self.0
            })
            .collect()
    }
}

/// Reorders `join` the way Go's DP solver would, or `None` to keep it as
/// written.
pub(crate) fn reorder(
    join: &Join,
    where_clause: Option<&Expr>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<Reordered> {
    let threshold = ctx.join_reorder_threshold();
    let mut ids = Ids::default();
    let mut leaves = Vec::new();
    let mut on_conds = Vec::new();
    if !collect(
        join,
        catalog,
        current_db,
        &mut ids,
        &mut leaves,
        &mut on_conds,
    ) {
        return None;
    }
    if leaves.len() < 2 {
        return None;
    }
    // Go: `useGreedy = !allInnerJoin || joinGroupNum > threshold`. `collect`
    // only yields all-inner groups, so the group size decides alone. Compared
    // in `i64` because a session may set the threshold NEGATIVE, which is more
    // greedy still, not less.
    let use_greedy = i64::from(threshold) < leaves.len() as i64;

    // Every conjunct the group can see: the `ON`s it absorbed and the `WHERE`
    // above it, which is where the comma spelling puts its equalities.
    let mut conjuncts: Vec<&Expr> = on_conds.clone();
    if let Some(where_clause) = where_clause {
        crate::plan_trace::collect_and(where_clause, &mut conjuncts);
    }

    let mut edges: Vec<Edge<'_>> = Vec::new();
    let mut filters: Vec<Vec<Expr>> = vec![Vec::new(); leaves.len()];
    for conjunct in &conjuncts {
        match classify(conjunct, &leaves)? {
            Classified::Edge(edge) => edges.push(edge),
            Classified::Single(leaf) => filters[leaf].push((*conjunct).clone()),
            // Go's `otherConds`; see the module doc for why the default solver
            // declines rather than models them.
            Classified::Spanning if use_greedy => return None,
            Classified::Spanning => {}
        }
    }
    if edges.is_empty() {
        return None;
    }
    // Non-edge `ON` conjuncts are re-attached to the rebuilt root below, so
    // nothing the statement wrote is dropped. Attaching them at the root is
    // always sound here because every join in the group is an INNER join,
    // whose `ON` is a filter over the same pairs.
    let residual_on: Vec<&Expr> = on_conds
        .iter()
        .copied()
        .filter(|cond| !matches!(classify(cond, &leaves), Some(Classified::Edge(_))))
        .collect();

    // Go's `not(isnull(key))`, derived by `LogicalJoin.PredicatePushDown` for
    // every equi key on both sides.
    let mut demands: Vec<Demand> = (0..leaves.len()).map(|_| Demand::default()).collect();
    for edge in &edges {
        demands[edge.left.0].not_null.insert(edge.left.1);
        demands[edge.right.0].not_null.insert(edge.right.1);
    }
    for (demand, filters) in demands.iter_mut().zip(filters) {
        demand.filters = filters;
    }
    let models: Option<Vec<LogicalNode>> = leaves
        .iter()
        .zip(&demands)
        .map(|(leaf, demand)| emit(&leaf.rel, demand))
        .collect();
    let models = models?;

    let context = DeriveStatsContext::with_join_reorder_threshold(threshold);
    let plan = if use_greedy {
        greedy_solve(&leaves, &edges, &models, &context)?
    } else {
        solve(&leaves, &edges, &models, &context)?
    };
    let mut order = Vec::new();
    plan.leaves(&mut order);
    let mut written_order = vec![0; leaves.len()];
    for (position, written) in order.iter().enumerate() {
        written_order[*written] = position;
    }
    let join = rebuild(&plan, &leaves, &edges, &residual_on)?;
    Some(Reordered {
        join,
        written_order,
    })
}

/// One equality edge: which `(leaf, column)` on each side, and the conjunct
/// that spells it.
struct Edge<'a> {
    left: (usize, usize),
    right: (usize, usize),
    expr: &'a Expr,
}

enum Classified<'a> {
    Edge(Edge<'a>),
    /// A conjunct over exactly one leaf.
    Single(usize),
    /// A conjunct over several leaves, or over columns this group does not
    /// own (an outer-query correlation).
    Spanning,
}

/// Which leaves a conjunct touches, and whether it is a join connector.
///
/// `None` DECLINES the whole reorder: an equality spanning two leaves that is
/// not a bare `col = col` is Go's injected-projection case, which this module
/// does not build.
fn classify<'a>(conjunct: &'a Expr, leaves: &[Leaf<'_>]) -> Option<Classified<'a>> {
    let mut touched = BTreeSet::new();
    for path in column_paths(conjunct) {
        match resolve(&path, leaves) {
            Some((leaf, _)) => {
                touched.insert(leaf);
            }
            // A path this group does not own is no leaf's own filter.
            None => return Some(Classified::Spanning),
        }
    }
    if let Expr::Binary(BinaryOp::Eq, lhs, rhs) = conjunct {
        if touched.len() == 2 {
            let (Expr::Column(left), Expr::Column(right)) = (strip(lhs), strip(rhs)) else {
                return None;
            };
            let left = resolve(left, leaves)?;
            let right = resolve(right, leaves)?;
            if left.0 == right.0 {
                return None;
            }
            return Some(Classified::Edge(Edge {
                left,
                right,
                expr: conjunct,
            }));
        }
    }
    match touched.len() {
        1 => Some(Classified::Single(
            touched.into_iter().next().expect("one leaf"),
        )),
        _ => Some(Classified::Spanning),
    }
}

fn strip(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(inner) => strip(inner),
        other => other,
    }
}

/// Every column path an expression reads.
///
/// Collected through the generated AST visitor rather than a hand-written
/// match, so a new [`Expr`] variant cannot introduce a subtree this misses.
fn column_paths(expr: &Expr) -> Vec<Vec<String>> {
    struct Collect(Vec<Vec<String>>);
    impl tidb_ast::Visitor for Collect {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(Expr::Column(path)) = node.downcast_ref::<Expr>() {
                self.0.push(path.clone());
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut collector = Collect(Vec::new());
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut collector);
    collector.0
}

/// `expr` with every column path rewritten by `rename`.
fn rewrite_paths(expr: &Expr, rename: &dyn Fn(&[String]) -> Option<Vec<String>>) -> Option<Expr> {
    struct Rewrite<'a> {
        rename: &'a dyn Fn(&[String]) -> Option<Vec<String>>,
        ok: bool,
    }
    impl tidb_ast::Visitor for Rewrite<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(Expr::Column(path)) = node.downcast_mut::<Expr>() {
                match (self.rename)(path) {
                    Some(renamed) => *path = renamed,
                    None => self.ok = false,
                }
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut owned = expr.clone();
    let mut rewrite = Rewrite { rename, ok: true };
    tidb_ast::Visitable::accept(&mut owned, &mut rewrite);
    rewrite.ok.then_some(owned)
}

/// Which `(leaf, column)` a written path names.
fn resolve(path: &[String], leaves: &[Leaf<'_>]) -> Option<(usize, usize)> {
    let (qualifier, name) = match path {
        [name] => (None, name),
        [.., qualifier, name] => (Some(qualifier), name),
        [] => return None,
    };
    let mut hit = None;
    for (index, leaf) in leaves.iter().enumerate() {
        if let Some(qualifier) = qualifier {
            if !leaf.visible.eq_ignore_ascii_case(qualifier) {
                continue;
            }
        }
        for (column, own) in leaf.columns.iter().enumerate() {
            if own.eq_ignore_ascii_case(name) {
                // An unqualified name several leaves own is ambiguous, which
                // is the statement's own error, not a reorder decision.
                if hit.is_some() {
                    return None;
                }
                hit = Some((index, column));
            }
        }
    }
    hit
}

/// Which output column of one relation a written path names.
fn resolve_output(path: &[String], visible: &str, names: &[String]) -> Option<usize> {
    let (qualifier, name) = match path {
        [name] => (None, name),
        [.., qualifier, name] => (Some(qualifier), name),
        [] => return None,
    };
    if let Some(qualifier) = qualifier {
        if !visible.eq_ignore_ascii_case(qualifier) {
            return None;
        }
    }
    names.iter().position(|own| own.eq_ignore_ascii_case(name))
}

// ---------------------------------------------------------------------------
// Extraction
// ---------------------------------------------------------------------------

/// Go's `extractJoinGroupImpl`, narrowed: walks the inner-join spine, pushing
/// every leaf and every `ON` conjunct. `false` DECLINES.
fn collect<'a>(
    join: &'a Join,
    catalog: &'a Catalog,
    current_db: &str,
    ids: &mut Ids,
    leaves: &mut Vec<Leaf<'a>>,
    on_conds: &mut Vec<&'a Expr>,
) -> bool {
    // The parser's single-relation wrapper is not a join at all.
    if join.right.is_none() && join.on.is_none() && join.using.is_empty() && !join.natural {
        return push_node(&join.left, catalog, current_db, ids, leaves, on_conds);
    }
    if join.tp != JoinType::Cross || join.straight || join.natural || !join.using.is_empty() {
        return false;
    }
    if let Some(on) = &join.on {
        crate::plan_trace::collect_and(on, on_conds);
    }
    if !push_node(&join.left, catalog, current_db, ids, leaves, on_conds) {
        return false;
    }
    match &join.right {
        Some(right) => push_node(right, catalog, current_db, ids, leaves, on_conds),
        None => true,
    }
}

fn push_node<'a>(
    node: &'a JoinNode,
    catalog: &'a Catalog,
    current_db: &str,
    ids: &mut Ids,
    leaves: &mut Vec<Leaf<'a>>,
    on_conds: &mut Vec<&'a Expr>,
) -> bool {
    if let JoinNode::Join(inner) = node {
        return collect(inner, catalog, current_db, ids, leaves, on_conds);
    }
    match leaf_of(node, catalog, current_db, ids) {
        Some(leaf) => {
            leaves.push(leaf);
            true
        }
        None => false,
    }
}

/// Models one leaf relation, or `None` for a shape this module declines.
fn leaf_of<'a>(
    node: &'a JoinNode,
    catalog: &'a Catalog,
    current_db: &str,
    ids: &mut Ids,
) -> Option<Leaf<'a>> {
    match node {
        JoinNode::Table(table_ref) => {
            if table_ref.as_of.is_some() || !table_ref.partitions.is_empty() {
                return None;
            }
            let (database, name) =
                crate::driver::split_table_path(&table_ref.name, current_db).ok()?;
            let TableEntry::Kv(table) = catalog.get_in(database, name)? else {
                return None;
            };
            let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
            let stats = catalog.table_statistics(table.table_id);
            let realtime = crate::access_cost::realtime_row_count(stats.map(AsRef::as_ref));
            let columns: Vec<(String, FieldType)> = table
                .visible_columns()
                .iter()
                .map(|column| (column.name.clone(), column.field_type.clone()))
                .collect();
            let nullable = table
                .visible_columns()
                .iter()
                .map(|column| column.field_type.flags() & NOT_NULL_FLAG == 0)
                .collect();
            let names = columns.iter().map(|(name, _)| name.clone()).collect();
            let column_ids = ids.take(columns.len());
            Some(Leaf {
                node,
                visible,
                columns: names,
                rel: Rel::Table(TableRel {
                    table,
                    realtime,
                    stats: stats.map(AsRef::as_ref),
                    ids: column_ids,
                    columns,
                    nullable,
                }),
            })
        }
        JoinNode::Derived {
            subquery,
            alias,
            lateral,
            column_names,
        } => {
            if *lateral || !column_names.is_empty() {
                return None;
            }
            let alias = alias.as_deref().filter(|alias| !alias.is_empty())?;
            let QueryStmt::Select(select) = &**subquery else {
                return None;
            };
            // Only a plain projection over a join is a relation whose row
            // count this module can derive; anything that changes the count on
            // its own way up is a decline.
            if !select.group_by.is_empty()
                || select.having.is_some()
                || select.distinct
                || select.limit.is_some()
                || select.with.is_some()
                || !select.order_by.is_empty()
                || !select.windows.is_empty()
            {
                return None;
            }
            let names = crate::driver::from::derived_field_names(select)?;
            let mut exprs = Vec::new();
            for field in select.fields.fields() {
                match field {
                    SelectField::Expr { expr, .. } => {
                        if expr.has_aggregate_flag() {
                            return None;
                        }
                        exprs.push(expr);
                    }
                    SelectField::Wildcard { .. } => return None,
                }
            }
            let from = select.from.as_ref()?;
            let mut inner = Vec::new();
            let mut inner_on = Vec::new();
            if !collect(from, catalog, current_db, ids, &mut inner, &mut inner_on) {
                return None;
            }
            let mut conjuncts = inner_on;
            if let Some(where_clause) = &select.where_clause {
                crate::plan_trace::collect_and(where_clause, &mut conjuncts);
            }
            let mut inner_edges = Vec::new();
            let mut inner_filters = vec![Vec::new(); inner.len()];
            for conjunct in &conjuncts {
                match classify(conjunct, &inner)? {
                    Classified::Edge(edge) => inner_edges.push((edge.left, edge.right)),
                    Classified::Single(leaf) => inner_filters[leaf].push((*conjunct).clone()),
                    Classified::Spanning => {}
                }
            }
            let column_ids = ids.take(names.len());
            Some(Leaf {
                node,
                visible: alias.to_owned(),
                columns: names.clone(),
                rel: Rel::Derived(DerivedRel {
                    visible: alias.to_owned(),
                    exprs,
                    names,
                    ids: column_ids,
                    inner,
                    inner_edges,
                    inner_filters,
                }),
            })
        }
        JoinNode::Join(_) => None,
    }
}

// ---------------------------------------------------------------------------
// The cost model input
// ---------------------------------------------------------------------------

/// Builds the [`LogicalNode`] of one relation with everything a parent pushed
/// into it applied.
fn emit(rel: &Rel<'_>, demand: &Demand) -> Option<LogicalNode> {
    match rel {
        Rel::Table(table) => {
            let mut selectivity = table_selectivity(table, &demand.filters);
            for column in &demand.not_null {
                if table.nullable.get(*column).copied().unwrap_or(true) {
                    selectivity *= NOT_NULL_RATE;
                }
            }
            if !demand.expression.is_empty() {
                selectivity *= crate::plan_trace::SELECTIVITY_FACTOR;
            }
            Some(LogicalNode::DataSource {
                realtime_count: table.realtime,
                columns: table.ids.clone(),
                selectivity,
            })
        }
        Rel::Derived(derived) => {
            let mut inner: Vec<Demand> = (0..derived.inner.len())
                .map(|_| Demand::default())
                .collect();
            for (leaf, filters) in derived.inner_filters.iter().enumerate() {
                inner[leaf].filters.extend(filters.iter().cloned());
            }
            for (left, right) in &derived.inner_edges {
                inner[left.0].not_null.insert(left.1);
                inner[right.0].not_null.insert(right.1);
            }
            for output in &demand.not_null {
                push_through(derived, *output, true, &mut inner)?;
            }
            for output in &demand.expression {
                push_through(derived, *output, false, &mut inner)?;
            }
            for filter in &demand.filters {
                push_filter(derived, filter, &mut inner)?;
            }
            let child = emit_tree(derived, &inner)?;
            Some(LogicalNode::Projection {
                child: Box::new(child),
                exprs: (0..derived.exprs.len())
                    .map(|output| ProjectionExpr {
                        output: derived.ids[output],
                        inputs: column_paths(derived.exprs[output])
                            .iter()
                            .filter_map(|path| {
                                let (leaf, column) = resolve(path, &derived.inner)?;
                                column_id(&derived.inner[leaf].rel, column)
                            })
                            .collect(),
                    })
                    .collect(),
            })
        }
    }
}

/// `cardinality.Selectivity` over the conjuncts pushed into one base table.
fn table_selectivity(table: &TableRel<'_>, filters: &[Expr]) -> f64 {
    if filters.is_empty() {
        return 1.0;
    }
    // The scope holds this table alone, so a path the statement qualified has
    // to be reduced to its bare column name before it will resolve.
    let bare: Vec<Expr> = filters
        .iter()
        .filter_map(|filter| {
            rewrite_paths(filter, &|path| {
                Some(vec![path.last().cloned().unwrap_or_default()])
            })
        })
        .collect();
    let scope = crate::plan_trace::PlanTrace::single_table_scope("", None, table.columns.clone());
    let resolver = crate::driver::from::scope_resolver(&scope);
    let conjuncts: Vec<&Expr> = bare.iter().collect();
    crate::access_cost::selectivity_of_conjuncts(&conjuncts, table.table, &resolver, table.stats)
}

/// Pushes one output column's demand through a projection.
///
/// Go's `rule_predicate_push_down` substitutes the projection's own expression
/// for the column and pushes the result on: a bare column reference reaches
/// the inner column it names (and keeps being a not-null demand), anything
/// else can only be charged to the leftover mask of whichever inner relation
/// owns the expression's columns.
fn push_through(
    derived: &DerivedRel<'_>,
    output: usize,
    as_not_null: bool,
    inner: &mut [Demand],
) -> Option<()> {
    let expr = strip(derived.exprs.get(output)?);
    if let Expr::Column(path) = expr {
        let (leaf, column) = resolve(path, &derived.inner)?;
        if as_not_null {
            inner[leaf].not_null.insert(column);
        } else {
            inner[leaf].expression.insert(column);
        }
        return Some(());
    }
    for path in column_paths(expr) {
        let (leaf, column) = resolve(&path, &derived.inner)?;
        inner[leaf].expression.insert(column);
    }
    Some(())
}

/// Pushes one conjunct written against a derived table through its projection.
///
/// When every column it reads is a bare pass-through of ONE inner relation the
/// conjunct is rewritten and pushed down whole, which is what lets a range
/// predicate keep its range. Otherwise it is charged as an expression demand,
/// the leftover-mask case.
fn push_filter(derived: &DerivedRel<'_>, filter: &Expr, inner: &mut [Demand]) -> Option<()> {
    let outputs: Option<Vec<usize>> = column_paths(filter)
        .iter()
        .map(|path| resolve_output(path, &derived.visible, &derived.names))
        .collect();
    let outputs = outputs?;
    let mut owner: Option<usize> = None;
    let mut pass_through = true;
    for output in &outputs {
        match strip(derived.exprs[*output]) {
            Expr::Column(path) => {
                let (leaf, _) = resolve(path, &derived.inner)?;
                if *owner.get_or_insert(leaf) != leaf {
                    pass_through = false;
                }
            }
            _ => pass_through = false,
        }
    }
    if let (true, Some(owner)) = (pass_through, owner) {
        let names = &derived.names;
        let exprs = &derived.exprs;
        let visible = &derived.visible;
        let rewritten = rewrite_paths(filter, &|path| {
            let output = resolve_output(path, visible, names)?;
            match strip(exprs[output]) {
                Expr::Column(inner) => Some(inner.clone()),
                _ => None,
            }
        });
        if let Some(rewritten) = rewritten {
            inner[owner].filters.push(rewritten);
            return Some(());
        }
    }
    for output in outputs {
        push_through(derived, output, false, inner)?;
    }
    Some(())
}

/// The id of one output column of a relation.
fn column_id(rel: &Rel<'_>, column: usize) -> Option<ColumnId> {
    match rel {
        Rel::Table(table) => table.ids.get(column).copied(),
        Rel::Derived(derived) => derived.ids.get(column).copied(),
    }
}

/// A derived table's own `FROM`, left-deep as written.
///
/// Go reorders an inner group by its own recursive `optimizeRecursive` call
/// before the outer DP costs it; for the two-relation groups this reaches, the
/// row count is the same either way, and a group this module would itself
/// reorder is one the caller reaches on its own recursion.
fn emit_tree(derived: &DerivedRel<'_>, demands: &[Demand]) -> Option<LogicalNode> {
    let mut node = emit(&derived.inner[0].rel, &demands[0])?;
    let mut joined: Vec<usize> = vec![0];
    for (right, (leaf, demand)) in derived.inner.iter().zip(demands).enumerate().skip(1) {
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();
        for (a, b) in &derived.inner_edges {
            let pair = if joined.contains(&a.0) && b.0 == right {
                Some((*a, *b))
            } else if joined.contains(&b.0) && a.0 == right {
                Some((*b, *a))
            } else {
                None
            };
            if let Some((near, far)) = pair {
                left_keys.push(column_id(&derived.inner[near.0].rel, near.1)?);
                right_keys.push(column_id(&derived.inner[far.0].rel, far.1)?);
            }
        }
        node = LogicalNode::Join {
            left: Box::new(node),
            right: Box::new(emit(&leaf.rel, demand)?),
            left_keys,
            right_keys,
        };
        joined.push(right);
    }
    Some(node)
}

// ---------------------------------------------------------------------------
// The DP
// ---------------------------------------------------------------------------

/// The tree the DP settled on, in group-leaf indices.
#[derive(Clone)]
enum Plan {
    Leaf(usize),
    Join {
        left: Box<Plan>,
        right: Box<Plan>,
        edges: Vec<usize>,
    },
}

impl Plan {
    fn leaves(&self, out: &mut Vec<usize>) {
        match self {
            Plan::Leaf(index) => out.push(*index),
            Plan::Join { left, right, .. } => {
                left.leaves(out);
                right.leaves(out);
            }
        }
    }
}

struct Candidate {
    plan: Plan,
    model: LogicalNode,
    cum_cost: f64,
}

/// Go `joinReorderDPSolver.solve` for a single connected component, which is
/// the only shape this module accepts.
fn solve(
    leaves: &[Leaf<'_>],
    edges: &[Edge<'_>],
    models: &[LogicalNode],
    context: &DeriveStatsContext,
) -> Option<Plan> {
    // `bfsGraph`: relabel the nodes breadth-first from node 0. This is what
    // fixes the subset enumeration order below, and therefore which of two
    // EQUAL-cost candidates survives the strict `>` update test.
    let mut adjacent: Vec<Vec<usize>> = vec![Vec::new(); leaves.len()];
    for edge in edges {
        adjacent[edge.left.0].push(edge.right.0);
        adjacent[edge.right.0].push(edge.left.0);
    }
    let mut visited = vec![false; leaves.len()];
    let mut visit_to_node = Vec::new();
    let mut queue = std::collections::VecDeque::from([0usize]);
    visited[0] = true;
    while let Some(node) = queue.pop_front() {
        visit_to_node.push(node);
        for next in &adjacent[node] {
            if !visited[*next] {
                visited[*next] = true;
                queue.push_back(*next);
            }
        }
    }
    // A disconnected graph is Go's `makeBushyJoin` case, declined here.
    if visit_to_node.len() != leaves.len() {
        return None;
    }
    let mut node_to_visit = vec![0usize; leaves.len()];
    for (visit, node) in visit_to_node.iter().enumerate() {
        node_to_visit[*node] = visit;
    }

    let count = leaves.len();
    let mut best: Vec<Option<Candidate>> = (0..(1usize << count)).map(|_| None).collect();
    for (visit, node) in visit_to_node.iter().enumerate() {
        let model = models[*node].clone();
        best[1 << visit] = Some(Candidate {
            plan: Plan::Leaf(*node),
            cum_cost: derive_stats(&model, context).cum_cost(),
            model,
        });
    }
    for bitmap in 1usize..(1 << count) {
        if bitmap.count_ones() == 1 {
            continue;
        }
        let mut sub = (bitmap - 1) & bitmap;
        while sub > 0 {
            let remain = bitmap ^ sub;
            // Go's `sub > remain` guard keeps only one of a subset and its
            // complement, and the numerically SMALLER half takes the left.
            if sub <= remain && best[sub].is_some() && best[remain].is_some() {
                let used = connecting(sub, remain, edges, &node_to_visit);
                if !used.is_empty() {
                    let candidate = build(
                        best[sub].as_ref().expect("checked"),
                        best[remain].as_ref().expect("checked"),
                        &used,
                        edges,
                        leaves,
                        context,
                    );
                    // `bestPlan[nodeBitmap].cumCost > curCost` is STRICT, so
                    // the first candidate enumerated survives a tie.
                    let replace = match &best[bitmap] {
                        Some(current) => current.cum_cost > candidate.cum_cost,
                        None => true,
                    };
                    if replace {
                        best[bitmap] = Some(candidate);
                    }
                }
            }
            sub = (sub - 1) & bitmap;
        }
    }
    best[(1 << count) - 1].take().map(|best| best.plan)
}

// ---------------------------------------------------------------------------
// The greedy solver
// ---------------------------------------------------------------------------

/// Go `joinReorderGreedySolver.solve` (`rule_join_reorder_greedy.go:48`).
///
/// Go's shape is: sort the group by `baseNodeCumCost` ascending, then peel one
/// CONNECTED tree at a time with `constructConnectedJoinTree`, and finally
/// `makeBushyJoin` the peeled trees into a cartesian product. This module
/// accepts only a connected equality graph, so the first peel consumes the
/// whole group and the bushy step has nothing to do; a leftover is the
/// disconnected case and is declined, leaving the statement's own cartesian
/// product where it was written.
///
/// There is no leading hint here -- this tier has no `/*+ leading(...) */` --
/// so Go's `leadingJoinGroup` prefix and its inapplicable-hint warning have no
/// counterpart.
fn greedy_solve(
    leaves: &[Leaf<'_>],
    edges: &[Edge<'_>],
    models: &[LogicalNode],
    context: &DeriveStatsContext,
) -> Option<Plan> {
    // `generateJoinOrderNode`: each leaf's own `RecursiveDeriveStats` and its
    // `baseNodeCumCost`, which for a leaf is the sum of its subtree's row
    // counts -- exactly [`DerivedNode::cum_cost`].
    let mut group: Vec<Candidate> = models
        .iter()
        .enumerate()
        .map(|(index, model)| Candidate {
            plan: Plan::Leaf(index),
            cum_cost: derive_stats(model, context).cum_cost(),
            model: model.clone(),
        })
        .collect();
    // `slices.SortStableFunc(..., cmp.Compare(i.cumCost, j.cumCost))`: ascending,
    // and STABLE, so equal-cost leaves keep the order the statement wrote them
    // in. That order is what the strict `<` in `construct_connected` then
    // resolves ties by, so the stability is load-bearing, not cosmetic.
    group.sort_by(|left, right| left.cum_cost.total_cmp(&right.cum_cost));

    let tree = construct_connected(&mut group, leaves, edges, context);
    // Anything left over is a second connected component: Go's
    // `makeBushyJoin` case, declined here.
    group.is_empty().then_some(tree.plan)
}

/// Go `joinReorderGreedySolver.constructConnectedJoinTree`.
///
/// Takes the cheapest remaining node as the tree, then repeatedly joins in the
/// remaining node that yields the cheapest cumulative cost, stopping when no
/// remaining node connects.
fn construct_connected(
    group: &mut Vec<Candidate>,
    leaves: &[Leaf<'_>],
    edges: &[Edge<'_>],
    context: &DeriveStatsContext,
) -> Candidate {
    let mut current = group.remove(0);
    loop {
        let mut best: Option<(usize, Candidate)> = None;
        for (index, node) in group.iter().enumerate() {
            // Go's `checkConnectionAndMakeJoin`. With no `otherConds` in play
            // -- the greedy arm declines a group that has any -- a pair with no
            // equality edge is a CARTESIAN one, which
            // `tidb_opt_cartesian_join_order_threshold` refuses at its default
            // of `0`. Both of Go's two refusal sites reduce to this one skip.
            let used = connecting_plans(&current.plan, &node.plan, edges);
            if used.is_empty() {
                continue;
            }
            // `curJoinTree` is Go's LEFT argument and the candidate node its
            // right; for an inner join `checkConnection` keeps those positions.
            let candidate = build(&current, node, &used, edges, leaves, context);
            // `curCost < bestCost` is STRICT, so among equal costs the node
            // enumerated first -- the cheapest by the sort above -- wins.
            let better = best
                .as_ref()
                .is_none_or(|(_, best)| candidate.cum_cost < best.cum_cost);
            if better {
                best = Some((index, candidate));
            }
        }
        // `if bestJoin == nil { break }`: the connected subgraph is exhausted.
        let Some((index, candidate)) = best else {
            return current;
        };
        group.remove(index);
        current = candidate;
    }
}

/// Go `checkConnection`'s equality half, between two already-built subtrees.
///
/// An edge counts only when it has one endpoint on each side; an edge whose
/// endpoints both sit inside `left` was consumed by a join further down.
fn connecting_plans(left: &Plan, right: &Plan, edges: &[Edge<'_>]) -> Vec<usize> {
    let mut left_leaves = Vec::new();
    left.leaves(&mut left_leaves);
    let mut right_leaves = Vec::new();
    right.leaves(&mut right_leaves);
    edges
        .iter()
        .enumerate()
        .filter(|(_, edge)| {
            (left_leaves.contains(&edge.left.0) && right_leaves.contains(&edge.right.0))
                || (left_leaves.contains(&edge.right.0) && right_leaves.contains(&edge.left.0))
        })
        .map(|(index, _)| index)
        .collect()
}

/// Go `nodesAreConnected`, equality edges only.
fn connecting(
    left_mask: usize,
    right_mask: usize,
    edges: &[Edge<'_>],
    node_to_visit: &[usize],
) -> Vec<usize> {
    let mut used = Vec::new();
    for (index, edge) in edges.iter().enumerate() {
        let left = 1usize << node_to_visit[edge.left.0];
        let right = 1usize << node_to_visit[edge.right.0];
        if (left_mask & left > 0 && right_mask & right > 0)
            || (left_mask & right > 0 && right_mask & left > 0)
        {
            used.push(index);
        }
    }
    used
}

/// Go `newJoinWithEdge` plus `calcJoinCumCost`.
fn build(
    left: &Candidate,
    right: &Candidate,
    used: &[usize],
    edges: &[Edge<'_>],
    leaves: &[Leaf<'_>],
    context: &DeriveStatsContext,
) -> Candidate {
    let mut left_leaves = Vec::new();
    left.plan.leaves(&mut left_leaves);
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    for index in used {
        let edge = &edges[*index];
        let (near, far) = if left_leaves.contains(&edge.left.0) {
            (edge.left, edge.right)
        } else {
            (edge.right, edge.left)
        };
        if let (Some(near), Some(far)) = (
            column_id(&leaves[near.0].rel, near.1),
            column_id(&leaves[far.0].rel, far.1),
        ) {
            left_keys.push(near);
            right_keys.push(far);
        }
    }
    let model = LogicalNode::Join {
        left: Box::new(left.model.clone()),
        right: Box::new(right.model.clone()),
        left_keys,
        right_keys,
    };
    let cum_cost = derive_stats(&model, context).cum_cost();
    Candidate {
        plan: Plan::Join {
            left: Box::new(left.plan.clone()),
            right: Box::new(right.plan.clone()),
            edges: used.to_vec(),
        },
        model,
        cum_cost,
    }
}

// ---------------------------------------------------------------------------
// The rebuilt tree
// ---------------------------------------------------------------------------

fn rebuild(
    plan: &Plan,
    leaves: &[Leaf<'_>],
    edges: &[Edge<'_>],
    residual_on: &[&Expr],
) -> Option<Join> {
    let JoinNode::Join(mut join) = rebuild_node(plan, leaves, edges)? else {
        return None;
    };
    for cond in residual_on {
        join.on = Some(match join.on.take() {
            Some(existing) => Expr::Binary(
                BinaryOp::LogicAnd,
                Box::new(existing),
                Box::new((*cond).clone()),
            ),
            None => (*cond).clone(),
        });
    }
    Some(*join)
}

fn rebuild_node(plan: &Plan, leaves: &[Leaf<'_>], edges: &[Edge<'_>]) -> Option<JoinNode> {
    match plan {
        Plan::Leaf(index) => Some(leaves[*index].node.clone()),
        Plan::Join {
            left,
            right,
            edges: used,
        } => {
            let on =
                used.iter()
                    .map(|index| edges[*index].expr.clone())
                    .reduce(|left, right| {
                        Expr::Binary(BinaryOp::LogicAnd, Box::new(left), Box::new(right))
                    })?;
            Some(JoinNode::Join(Box::new(Join {
                left: rebuild_node(left, leaves, edges)?,
                right: Some(rebuild_node(right, leaves, edges)?),
                tp: JoinType::Cross,
                straight: false,
                on: Some(on),
                using: Vec::new(),
                natural: false,
                explicit_parens: false,
            })))
        }
    }
}
