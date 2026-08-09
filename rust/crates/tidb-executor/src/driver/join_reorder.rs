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
//! this module. A `STRAIGHT_JOIN`, a `NATURAL`/`USING` join and a `LATERAL`
//! derived table are Go's own stop conditions
//! (`rule_join_reorder.go:133-140`), with the difference that Go keeps a
//! stopped-at subtree as an atomic group member and still reorders around it,
//! while this module declines the whole group. Three further declines are this
//! module's own scope:
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
//! # An OUTER join is NOT one of Go's stop conditions, and is not one here
//!
//! [`collect`] used to take `join.tp != JoinType::Cross` as a decline, so a
//! single `LEFT`/`RIGHT JOIN` anywhere in the `FROM` clause declined the WHOLE
//! group. That was never Go's rule. `extractJoinGroupImpl`
//! (`rule_join_reorder.go:133-158`) reads:
//!
//! ```go
//! if !isJoin || (join.PreferJoinType > uint(0) && !p.SCtx().GetSessionVars().EnableAdvancedJoinHint) || join.StraightJoin ||
//!     (join.JoinType != base.InnerJoin && join.JoinType != base.LeftOuterJoin && join.JoinType != base.RightOuterJoin) ||
//!     ((join.JoinType == base.LeftOuterJoin || join.JoinType == base.RightOuterJoin) && join.EqualConditions == nil) ||
//!     ...NullEQ... {
//!     return &joinGroupResult{group: []base.LogicalPlan{p}, ...}
//! }
//! // If the session var is set to off, we will still reject the outer joins.
//! if !p.SCtx().GetSessionVars().EnableOuterJoinReorder && (join.JoinType == base.LeftOuterJoin || join.JoinType == base.RightOuterJoin) {
//!     return &joinGroupResult{group: []base.LogicalPlan{p}, ...}
//! }
//! ```
//!
//! A LEFT/RIGHT outer join CARRYING equal conditions is therefore a reorderable
//! member of the group, and only three narrower things stop one: a SEMI/ANTI
//! join type, an outer join with `EqualConditions == nil`, and
//! `tidb_enable_outer_join_reorder` set OFF -- whose default is ON
//! (`vardef.DefTiDBEnableOuterJoinReorder = true`).
//!
//! An outer join reaches the group here under three further bounds this
//! module states rather than approximates, each of them fail-closed:
//!
//!  * its NULL-EXTENDED side must be exactly ONE relation, so it moves as a
//!    unit ([`collect`]);
//!  * every `ON` conjunct it carries must be an equality this module can
//!    re-spell, which is Go's `joinTypeWithExtMsg.outerBindCondition` declined
//!    rather than split ([`reorder`]);
//!  * no OTHER equality may reach its extended leaf, which is what stops the
//!    greedy joining that leaf before the outer join is formed ([`reorder`]).
//!
//! With an outer join in the group Go forces the GREEDY solver
//! (`useGreedy := !allInnerJoin || ...`), so [`solve`]'s DP arm still only
//! ever sees the all-inner shapes it was written for.
//!
//! # The `Selection` barrier, and why the `through_sel = 0` copies stand still
//!
//! `extractJoinGroupImpl` walks THROUGH a `LogicalSelection` only under
//! `@@tidb_opt_join_reorder_through_sel` (`rule_join_reorder.go:67-80`), whose
//! shipped default is OFF. A `WHERE` conjunct over a NULL-EXTENDED column is
//! exactly where predicate pushdown leaves such a Selection standing: Go's
//! `case base.LeftOuterJoin` arm derives only the RIGHT child's conditions and
//! "right where condition cannot be pushed down", so the conjunct stops above
//! the outer join. With the variable OFF that Selection splits the group and
//! the `leading` hint below it names nothing the outer group holds; Go clears
//! the hint (`join.HintInfo = nil`) and warns. [`reorder`] declines the whole
//! group there instead of modelling the split, which keeps the WRITTEN tree --
//! and the written tree is what the four `tidb_opt_join_reorder_through_sel =
//! 0` recordings of `planner/core/join_reorder2` hold.
//!
//! # What holds the EXTRA merges, measured rather than assumed
//!
//! The `join_shape` CASETEST reported SEVEN ordered-merge pairs this tier
//! formed and TiDB does not; it now reports FIVE. Each was opened against its
//! recording. NONE is a merge-vs-hash cost decision: in every one, TiDB's join
//! TREE puts a different pair of leaves adjacent than this tier's does, so the
//! pair this tier merges is one TiDB never forms at all.
//!
//!  * `r/planner/core/join_reorder2.result`, FOUR statements, each writing
//!    `... left join t4 ...` (or `left join t3`) inside the group and each
//!    carrying a `leading` hint. TiDB merges `(t1,t2)`, `(t3,t4)`, `(t1,t4)`
//!    and `(t1,t5)` -- the hint's own first pair, every time. The first TWO
//!    name their tables without a query-block qualifier and are now reached:
//!    `leading(t1, t2)` pins the inner pair and `leading(t3, t4, t1, t2)` pins
//!    the LEFT OUTER one, both at `through_sel = 1`. The other two write
//!    `t1@sel_2` / `t1@sel_3`, which resolve only inside a group that SPANS
//!    query blocks; see [`leading_prefix`]'s named residue.
//!  * `r/planner/core/join_reorder_through_projection.result:756`, the
//!    `oj_t2`/`oj_t3`/`oj_t5` statement at
//!    `tidb_opt_join_reorder_through_proj = on`. TiDB records
//!    `HashJoin  inner join, equal:[eq(oj_t2.a, oj_t3.a)]` OVER
//!    `HashJoin  left outer join, equal:[eq(Column, oj_t5.b)]` -- the left
//!    join moved BELOW the inner one. This tier keeps `oj_t2 ⋈ oj_t3` inside
//!    the derived table and merges it, because
//!    [`super::through_proj::splice_join`] declines a FROM tree whose top join
//!    is not `JoinType::Cross`, so the projection never dissolves and the
//!    group is never `{oj_t2, oj_t3, oj_t5}` at all.
//!  * the same topic's TWO `dt1`/`dt2` statements. Both sides DO reorder here,
//!    and the greedy solvers pick a different FIRST pair: TiDB's leaves carry
//!    `Selection cop[tikv]  not(isnull(mul(t1.b, 2)))` under each
//!    injected-column leaf -- a null-rejection filter this tier does not
//!    derive -- which lowers those leaves' row counts and so changes the
//!    `cumCost` sort [`greedy_solve`] starts from.
//!
//! MUTATION PROBES, run against the `join_shape` 5-tuple
//! `(compared, both_agree, recorded, agreed, extra)`, which is
//! `(229, 146, 88, 82, 5)` here:
//!
//!  * REFUSE EVERY MERGE (`merge_join_decision` returns `None` unconditionally)
//!    gave `(229, 69, 88, 0, 0)` against the earlier baseline. `extra` reaches
//!    zero only by taking every AGREED merge with it, so no blunt refusal
//!    separates the ones TiDB does not record.
//!  * REMOVE THE OUTER-JOIN ACCEPTANCE from [`collect`] -- put back
//!    `join.tp != JoinType::Cross` as a decline -- gives
//!    `(229, 144, 88, 80, 7)`, the old numbers exactly.
//!  * DISABLE THE LEADING PREFIX ([`leading_prefix`] returns `None` before it
//!    reads a hint) gives `(229, 144, 88, 80, 7)` as well, WITH the outer-join
//!    acceptance still in place. That is batch69's finding reproduced from the
//!    other side: with pseudo statistics every leaf ties at `cumCost` 10000,
//!    so [`greedy_solve`]'s stable sort and its strict `curCost < bestCost`
//!    reproduce the written order and the accepted group changes nothing on
//!    its own. The two halves close the same two statements only TOGETHER.
//!
//! No cost gate over the tree this module leaves standing can reach the tree
//! TiDB records.
//!
//! # Go's `otherConds`, and what they do and do not change
//!
//! A conjunct that spans several leaves without being an equality is one of
//! Go's `otherConds`. Measured against Go rather than assumed, it reaches the
//! greedy solver through exactly ONE door and no other:
//!
//! * `checkConnectionAndMakeJoin` computes
//!   `isCartesian = len(usedEdges) == 0 && !s.hasOtherJoinCondition(l, r)`
//!   (`rule_join_reorder_greedy.go:170`). A pair with NO equality edge is
//!   therefore joinable whenever a single non-equality conjunct straddles it --
//!   no equality edge is required alongside -- and, being non-cartesian, it
//!   also escapes both the `cartesianThreshold <= 0` refusal and the
//!   cost-ratio penalty. This is [`has_other_join_condition`].
//! * `makeJoin` hands each conjunct to the FIRST join whose merged schema
//!   covers it and drops it from the set the later steps see
//!   (`s.otherConds = finalRemainOthers`), so a conjunct connects at most one
//!   pair. That shrinking set has NO counterpart here, and not by omission:
//!   `constructConnectedJoinTree` only ever GROWS `curJoinTree`, and the left
//!   argument of every later `hasOtherJoinCondition` call is exactly its leaf
//!   set. A conjunct Go would have dropped is one the growing tree already
//!   covers, so `!ExprFromSchema(cond, leftPlan.Schema())` already rejects it
//!   -- the same conjunct, at the same step, for the same reason. Threading a
//!   second copy of that test would be an untestable duplicate of the first.
//!
//! What it does NOT do is move a row count. `LogicalJoin.DeriveStats` feeds
//! `EstimateFullJoinRowCount` from `p.EqualConditions` alone
//! (`logical_join.go:572`); `OtherConditions` are not read there, and
//! `calcJoinCumCost` reads only `join.StatsInfo().RowCount`. A conjunct placed
//! on a join costs that join nothing. (Go's `leftConds`/`rightConds` DO become
//! child `Selection`s and so do move a count, but those are single-relation
//! conjuncts, which this module already pushes into a leaf's [`Demand`].)
//!
//! Two spellings stay declined, because Go's answer for them is not measurable
//! from a `FROM` clause:
//!
//! * a conjunct reading a column this group does not own (an outer-query
//!   correlation) or no column at all. Go's `ExprFromSchema` answers TRUE for
//!   both against ANY schema, which makes them `leftConds` -- a child
//!   `Selection` whose selectivity this module would have to invent.
//! * a chosen join with no equality edge at all. Go spells the conjunct into
//!   that join's `OtherConditions`; here the conjunct is still sitting in the
//!   statement's own `WHERE`, which this tier applies independently of the
//!   `FROM` tree, so copying it into an `ON` would evaluate it twice and can
//!   double its warnings. [`rebuild_node`] declines instead.
//!
//! # The cost model is not re-derived here
//!
//! Every row count comes from [`tidb_planner::cardinality::derive_stats`],
//! which is Go's `RecursiveDeriveStats` for the node kinds a `FROM` group
//! reaches. This module's job on that side is only to BUILD its input: which
//! `DataSource`s exist, what selectivity was pushed into each, and which
//! column is which. See [`Rel`] and [`emit`].

use std::{cell::RefCell, collections::BTreeSet};

use tidb_ast::{BinaryOp, Expr, Join, JoinNode, JoinType, QueryStmt, SelectField};
use tidb_datatype::FieldType;
use tidb_expr::Columns as _;
use tidb_planner::cardinality::derive_stats::{
    derive_stats, ColumnId, DeriveStatsContext, JoinKind, LogicalNode, ProjectionExpr,
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

type LeafColumn = (usize, usize);
type JoinEdge = (LeafColumn, LeafColumn);

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
    /// The aggregation's group expressions, or `None` for a plain projection.
    /// `Some([])` is a global aggregation.
    group_by: Option<Vec<&'a Expr>>,
    /// The output columns' names, for a conjunct written against them.
    names: Vec<String>,
    /// The output columns' ids.
    ids: Vec<ColumnId>,
    /// The subquery's own `FROM` leaves.
    inner: Vec<Leaf<'a>>,
    /// The subquery's own equi edges, as `(leaf, column)` pairs into `inner`.
    inner_edges: Vec<JoinEdge>,
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

/// Reorders `join` the way Go's solvers would, or `None` to keep it as
/// written.
pub(crate) fn reorder(
    join: &Join,
    select: &tidb_ast::SelectStmt,
    where_clause: Option<&Expr>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<Reordered> {
    let threshold = ctx.join_reorder_threshold();
    let scope = Scope {
        outer_join_reorder: ctx.outer_join_reorder(),
    };
    let mut ids = Ids::default();
    let mut leaves = Vec::new();
    let mut on_conds = Vec::new();
    if !collect(
        join,
        catalog,
        current_db,
        &scope,
        &mut ids,
        &mut leaves,
        &mut on_conds,
    ) {
        return None;
    }
    if leaves.len() < 2 {
        return None;
    }
    // Go's `hasOuterJoin`, and with it `allInnerJoin`.
    let extended: BTreeSet<usize> = on_conds
        .iter()
        .filter_map(|cond| cond.outer.map(|outer| outer.extended))
        .collect();
    // Go: `useGreedy = !allInnerJoin || joinGroupNum > threshold`. Compared in
    // `i64` because a session may set the threshold NEGATIVE, which is more
    // greedy still, not less.
    let use_greedy = !extended.is_empty() || i64::from(threshold) < leaves.len() as i64;

    // Every conjunct the group can see: the `ON`s it absorbed and the `WHERE`
    // above it, which is where the comma spelling puts its equalities.
    let mut conjuncts: Vec<(&Expr, Option<Outer>)> = on_conds
        .iter()
        .map(|cond| (cond.expr, cond.outer))
        .collect();
    if let Some(where_clause) = where_clause {
        let mut flat = Vec::new();
        crate::plan_trace::collect_and(where_clause, &mut flat);
        conjuncts.extend(flat.into_iter().map(|expr| (expr, None)));
    }

    let mut edges: Vec<Edge<'_>> = Vec::new();
    let mut filters: Vec<Vec<Expr>> = vec![Vec::new(); leaves.len()];
    // Go's `s.otherConds` as the greedy solver first sees it, one entry per
    // conjunct, holding the leaves that conjunct reads.
    let mut others: Vec<BTreeSet<usize>> = Vec::new();
    // Go's `nullExtendedCols` half of `hasOtherJoinCondition`: a conjunct
    // reading a null-extended column may not be used to connect a pair,
    // because doing so would move it above the outer join that produced the
    // NULLs it tests.
    let mut null_extended_others = false;
    for (conjunct, outer) in &conjuncts {
        match classify(conjunct, &leaves, *outer)? {
            Classified::Edge(edge) => edges.push(edge),
            Classified::Single(leaf) => {
                // A single-relation predicate over a null-extended relation is
                // Go's `simplifyOuterJoin` case -- the outer join becomes an
                // INNER one before reorder ever runs -- which this module does
                // not model.
                if extended.contains(&leaf) {
                    return None;
                }
                filters[leaf].push((*conjunct).clone());
            }
            Classified::Other(touched) => {
                if touched.iter().any(|leaf| extended.contains(leaf)) {
                    null_extended_others = true;
                } else {
                    others.push(touched);
                }
            }
            Classified::Foreign if use_greedy => return None,
            Classified::Foreign => {}
        }
    }
    // Go's `extractJoinGroupImpl` walks THROUGH a `Selection` only under
    // `@@tidb_opt_join_reorder_through_sel` (`rule_join_reorder.go:67-80`),
    // and a `WHERE` conjunct over a null-extended column is exactly where
    // predicate pushdown leaves one standing: it cannot become the outer
    // join's `ON`, so it stops above it. With the variable OFF that Selection
    // splits the group; this module declines the whole reorder instead of
    // modelling the split, which keeps the WRITTEN tree -- the tree the
    // `tidb_opt_join_reorder_through_sel = 0` recordings hold.
    if null_extended_others && !ctx.join_reorder_through_sel() {
        return None;
    }
    if edges.is_empty() {
        return None;
    }
    // Every `ON` conjunct of an OUTER join has to be an equality this module
    // can re-spell, because an outer join's `ON` decides which rows are
    // null-extended. Go keeps the rest as `joinTypeWithExtMsg.outerBindCondition`
    // and re-attaches it to the edge; that split is not modelled here.
    if on_conds.iter().any(|cond| {
        cond.outer.is_some()
            && !matches!(
                classify(cond.expr, &leaves, cond.outer),
                Some(Classified::Edge(_))
            )
    }) {
        return None;
    }
    // An outer join's null-extended leaf may be reached by that join's OWN
    // edges and by nothing else. Another equality into it would let the greedy
    // join it before the outer join is formed, which null-extends a different
    // set of rows.
    for edge in &edges {
        let touches = [edge.left.0, edge.right.0];
        for leaf in touches {
            if extended.contains(&leaf) && edge.outer.map(|outer| outer.extended) != Some(leaf) {
                return None;
            }
        }
    }
    // Non-edge `ON` conjuncts are re-attached to the rebuilt root below, so
    // nothing the statement wrote is dropped. Attaching them at the root is
    // sound only over an INNER root, whose `ON` is a filter over the same
    // pairs; [`rebuild`] declines otherwise.
    let residual_on: Vec<&Expr> = on_conds
        .iter()
        .filter(|cond| {
            !matches!(
                classify(cond.expr, &leaves, cond.outer),
                Some(Classified::Edge(_))
            )
        })
        .map(|cond| cond.expr)
        .collect();

    // Go's `not(isnull(key))`, derived by `LogicalJoin.PredicatePushDown` for
    // every equi key. An OUTER join derives it for the NULL-EXTENDED side
    // alone -- `DeriveOtherConditions(p, ..., false, true)` under
    // `case base.LeftOuterJoin` (`logical_join.go:208-212`) -- because every
    // preserved-side row survives whether or not its key is NULL.
    let mut demands: Vec<Demand> = (0..leaves.len()).map(|_| Demand::default()).collect();
    for edge in &edges {
        for side in [edge.left, edge.right] {
            let preserved = edge.outer.is_some_and(|outer| outer.extended != side.0);
            if !preserved {
                demands[side.0].not_null.insert(side.1);
            }
        }
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
        // Go `CheckAndGenerateLeadingHint` plus `generateLeadingJoinGroup`:
        // the tables the statement PINNED to the front of the group.
        let leading = leading_prefix(select, &leaves, &edges, &models, &context, ctx);
        greedy_solve(&leaves, &edges, &models, &context, &others, leading)?
    } else {
        // Go's DP arm reads `otherConds` through its own `totalNonEqEdges`,
        // which is not modelled here; the opt-in arm keeps declining to use
        // them, which is the same tree it built before. It is only ever
        // reached by an ALL-INNER group -- `useGreedy` above is forced by any
        // outer join -- so its inner-only shapes stay exact.
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
    /// Go's `joinTypes[idx]`, parallel to `eqEdges[idx]`: the join this
    /// equality was written on, when that join is an outer one.
    outer: Option<Outer>,
}

enum Classified<'a> {
    Edge(Edge<'a>),
    /// A conjunct over exactly one leaf.
    Single(usize),
    /// Go's `otherConds`: a conjunct over SEVERAL leaves of this group, none
    /// of them foreign. Carries which leaves, which is all the two rules that
    /// read it -- [`has_other_join_condition`] and the `remaining` threading --
    /// ever ask.
    Other(BTreeSet<usize>),
    /// A conjunct over columns this group does not own (an outer-query
    /// correlation), or over no column at all. See the module doc for why Go's
    /// answer for these is not measurable from a `FROM` clause.
    Foreign,
}

/// Which leaves a conjunct touches, and whether it is a join connector.
///
/// `None` DECLINES the whole reorder: an equality spanning two leaves that is
/// not a bare `col = col` is Go's injected-projection case, which this module
/// does not build.
fn classify<'a>(
    conjunct: &'a Expr,
    leaves: &[Leaf<'_>],
    outer: Option<Outer>,
) -> Option<Classified<'a>> {
    let mut touched = BTreeSet::new();
    for path in column_paths(conjunct) {
        match resolve(&path, leaves) {
            Some((leaf, _)) => {
                touched.insert(leaf);
            }
            // A path this group does not own is no leaf's own filter.
            None => return Some(Classified::Foreign),
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
                outer,
            }));
        }
    }
    match touched.len() {
        // A conjunct over no column at all is Go's `leftConds`, not one of its
        // `otherConds`; see the module doc.
        0 => Some(Classified::Foreign),
        1 => Some(Classified::Single(
            touched.into_iter().next().expect("one leaf"),
        )),
        _ => Some(Classified::Other(touched)),
    }
}

/// Go `baseSingleGroupJoinOrderSolver.hasOtherJoinCondition`
/// (`rule_join_reorder.go:721`).
///
/// TRUE when some remaining `otherCond` reads columns from BOTH sides -- Go's
/// three tests `ExprFromSchema(cond, merged)`, `!ExprFromSchema(cond, left)`
/// and `!ExprFromSchema(cond, right)`, over leaf sets rather than schemas
/// because a leaf here is exactly one schema. Its `nullExtendedCols` test has
/// no counterpart: [`collect`] only ever yields an ALL-INNER group, which
/// null-extends nothing.
pub(crate) fn has_other_join_condition(
    left: &[usize],
    right: &[usize],
    remaining: &[BTreeSet<usize>],
) -> bool {
    remaining.iter().any(|touched| {
        let covered = touched
            .iter()
            .all(|leaf| left.contains(leaf) || right.contains(leaf));
        covered
            && !touched.iter().all(|leaf| left.contains(leaf))
            && !touched.iter().all(|leaf| right.contains(leaf))
    })
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

/// One `ON` conjunct together with the join that carries it.
struct OnCond<'a> {
    expr: &'a Expr,
    /// `None` for an INNER join, whose `ON` is a filter over the same pairs
    /// the `WHERE` sees. `Some` for an outer one, whose `ON` decides which
    /// rows are NULL-EXTENDED and can therefore never be moved.
    outer: Option<Outer>,
}

/// An outer join, as the flattened group remembers it.
///
/// The whole join is identified by the ONE leaf it null-extends: [`collect`]
/// only accepts an outer join whose extended side is a single relation, so
/// there is exactly one such leaf per outer join in the group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Outer {
    /// `JoinType::Left` or `JoinType::Right`, which is the side the statement
    /// WROTE the preserved relation on and therefore the spelling the rebuilt
    /// node has to keep.
    tp: JoinType,
    /// The written index of the leaf this join null-extends.
    extended: usize,
}

/// Whether an outer join may join the group at all: Go's
/// `SessionVars.EnableOuterJoinReorder`, plus this module's own requirement
/// that the extended side be a single relation.
struct Scope {
    outer_join_reorder: bool,
}

/// Go's `extractJoinGroupImpl`, narrowed: walks the join spine, pushing every
/// leaf and every `ON` conjunct. `false` DECLINES.
///
/// An outer join is NOT one of Go's stops (see the module doc); it is accepted
/// here when it carries equal conditions, when
/// `@@tidb_enable_outer_join_reorder` is ON, and when its NULL-EXTENDED side
/// is a single relation. The last is this module's own bound: the extended
/// side has to move as one unit, and a single leaf is the only unit whose
/// atomicity [`join_shape`](self) can state without modelling Go's
/// `outerBindCondition` split of a nested one.
fn collect<'a>(
    join: &'a Join,
    catalog: &'a Catalog,
    current_db: &str,
    scope: &Scope,
    ids: &mut Ids,
    leaves: &mut Vec<Leaf<'a>>,
    on_conds: &mut Vec<OnCond<'a>>,
) -> bool {
    // The parser's single-relation wrapper is not a join at all.
    if join.right.is_none() && join.on.is_none() && join.using.is_empty() && !join.natural {
        return push_node(
            &join.left, catalog, current_db, scope, ids, leaves, on_conds,
        );
    }
    if join.straight || join.natural || !join.using.is_empty() {
        return false;
    }
    let Some(right) = &join.right else {
        return false;
    };
    // `(join.JoinType == LeftOuterJoin || RightOuterJoin) && join.EqualConditions == nil`
    // is Go's own stop, and an outer join with no `ON` at all cannot carry
    // one.
    let outer = match join.tp {
        JoinType::Cross => None,
        JoinType::Left | JoinType::Right => {
            if !scope.outer_join_reorder || join.on.is_none() {
                return false;
            }
            Some(join.tp)
        }
    };
    // The extended side is pushed as ONE leaf; the preserved side keeps
    // walking the spine.
    let extended = match outer {
        None => None,
        Some(JoinType::Left) => {
            if !push_node(
                &join.left, catalog, current_db, scope, ids, leaves, on_conds,
            ) {
                return false;
            }
            let at = leaves.len();
            match leaf_of(right, catalog, current_db, ids) {
                Some(leaf) => leaves.push(leaf),
                None => return false,
            }
            Some(at)
        }
        Some(_) => {
            let at = leaves.len();
            match leaf_of(&join.left, catalog, current_db, ids) {
                Some(leaf) => leaves.push(leaf),
                None => return false,
            }
            if !push_node(right, catalog, current_db, scope, ids, leaves, on_conds) {
                return false;
            }
            Some(at)
        }
    };
    if let Some(on) = &join.on {
        let mut conds = Vec::new();
        crate::plan_trace::collect_and(on, &mut conds);
        on_conds.extend(conds.into_iter().map(|expr| OnCond {
            expr,
            outer: extended.map(|extended| Outer {
                tp: join.tp,
                extended,
            }),
        }));
    }
    if outer.is_some() {
        return true;
    }
    push_node(
        &join.left, catalog, current_db, scope, ids, leaves, on_conds,
    ) && push_node(right, catalog, current_db, scope, ids, leaves, on_conds)
}

fn push_node<'a>(
    node: &'a JoinNode,
    catalog: &'a Catalog,
    current_db: &str,
    scope: &Scope,
    ids: &mut Ids,
    leaves: &mut Vec<Leaf<'a>>,
    on_conds: &mut Vec<OnCond<'a>>,
) -> bool {
    if let JoinNode::Join(inner) = node {
        return collect(inner, catalog, current_db, scope, ids, leaves, on_conds);
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
            // LIMIT, DISTINCT and the remaining clauses change the relation
            // through logical nodes this row source still does not model. A
            // grouped/global aggregation is modelled explicitly below.
            if select.having.is_some()
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
            let mut has_aggregate = false;
            for field in select.fields.fields() {
                match field {
                    SelectField::Expr { expr, .. } => {
                        has_aggregate |= expr.has_aggregate_flag();
                        exprs.push(expr);
                    }
                    SelectField::Wildcard { .. } => return None,
                }
            }
            let group_by = (!select.group_by.is_empty() || has_aggregate)
                .then(|| select.group_by.iter().map(|item| &item.expr).collect());
            let from = select.from.as_ref()?;
            let mut inner = Vec::new();
            let mut inner_on = Vec::new();
            // A derived table's own `FROM` is modelled by [`emit_tree`], which
            // builds INNER joins only, so an outer join inside one is still a
            // decline here. Go reaches it by recursing `optimizeRecursive`
            // into the subquery; this module leaves that relation atomic.
            let inner_scope = Scope {
                outer_join_reorder: false,
            };
            if !collect(
                from,
                catalog,
                current_db,
                &inner_scope,
                ids,
                &mut inner,
                &mut inner_on,
            ) {
                return None;
            }
            let mut conjuncts: Vec<&Expr> = inner_on.iter().map(|cond| cond.expr).collect();
            if let Some(where_clause) = &select.where_clause {
                crate::plan_trace::collect_and(where_clause, &mut conjuncts);
            }
            let mut inner_edges = Vec::new();
            let mut inner_filters = vec![Vec::new(); inner.len()];
            for conjunct in &conjuncts {
                match classify(conjunct, &inner, None)? {
                    Classified::Edge(edge) => inner_edges.push((edge.left, edge.right)),
                    Classified::Single(leaf) => inner_filters[leaf].push((*conjunct).clone()),
                    // A derived table's own cost model carries equi keys and
                    // per-leaf filters only; its `otherConds` reach neither.
                    Classified::Other(_) | Classified::Foreign => {}
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
                    group_by,
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
            if let Some(group_by) = &derived.group_by {
                let mut group_columns = Vec::new();
                for expression in group_by {
                    for path in column_paths(expression) {
                        let (leaf, column) = resolve(&path, &derived.inner)?;
                        group_columns.push(column_id(&derived.inner[leaf].rel, column)?);
                    }
                }
                Some(LogicalNode::Aggregation {
                    child: Box::new(child),
                    group_by: group_columns,
                    columns: derived.ids.clone(),
                })
            } else {
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
            kind: JoinKind::Inner,
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
        /// The spelling the rebuilt `FROM` node takes: `Cross` for an inner
        /// join, `Left`/`Right` for the outer join this pair re-forms.
        tp: JoinType,
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
                        Shape::INNER,
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
/// accepts only a group the FIRST peel consumes whole -- connected by equality
/// edges, by `others` straddling it, or by both -- so the bushy step has
/// nothing to do; a leftover is the disconnected case and is declined, leaving
/// the statement's own cartesian product where it was written.
///
/// `leading` is Go's `s.leadingJoinGroup` after `generateLeadingJoinGroup`:
/// the sub-tree the statement PINNED to the front. `solve` prepends it to the
/// sorted group so `constructConnectedJoinTree` takes it as `curJoinTree`
/// (`rule_join_reorder_greedy.go:70-75`), and the leaves it already holds are
/// dropped from the group.
fn greedy_solve(
    leaves: &[Leaf<'_>],
    edges: &[Edge<'_>],
    models: &[LogicalNode],
    context: &DeriveStatsContext,
    others: &[BTreeSet<usize>],
    leading: Option<Candidate>,
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

    // `leadingJoinNodes := append(leadingJoinNodes, s.curJoinGroup...)`: the
    // pinned tree goes FIRST, ahead of the sort, and the leaves it consumed
    // leave the group.
    if let Some(leading) = leading {
        let mut pinned = Vec::new();
        leading.plan.leaves(&mut pinned);
        group.retain(|node| {
            let mut own = Vec::new();
            node.plan.leaves(&mut own);
            !own.iter().any(|leaf| pinned.contains(leaf))
        });
        group.insert(0, leading);
    }

    let tree = construct_connected(&mut group, leaves, edges, context, others);
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
    others: &[BTreeSet<usize>],
) -> Candidate {
    let mut current = group.remove(0);
    loop {
        let mut current_leaves = Vec::new();
        current.plan.leaves(&mut current_leaves);
        let mut best: Option<(usize, Candidate)> = None;
        for (index, node) in group.iter().enumerate() {
            // Go's `checkConnectionAndMakeJoin`: a pair with no equality edge
            // is CARTESIAN only when no remaining `otherCond` straddles it, and
            // a cartesian one is refused by
            // `tidb_opt_cartesian_join_order_threshold` at its default of `0`.
            // Both of Go's two refusal sites reduce to this one skip.
            let used = connecting_plans(&current.plan, &node.plan, edges);
            let mut node_leaves = Vec::new();
            node.plan.leaves(&mut node_leaves);
            if used.is_empty() && !has_other_join_condition(&current_leaves, &node_leaves, others) {
                continue;
            }
            // `checkConnection` reads ONE `joinTypes[idx]` per pair and, for
            // an outer one, swaps the node positions so the side the statement
            // wrote on the left stays on the left.
            let Some(shape) = shape_of(&current_leaves, &node_leaves, &used, edges) else {
                continue;
            };
            // `curJoinTree` is Go's LEFT argument and the candidate node its
            // right; for an inner join `checkConnection` keeps those positions.
            let candidate = build(&current, node, &used, edges, leaves, context, shape);
            // `curCost < bestCost` is STRICT, so among equal costs the node
            // enumerated first -- the cheapest by the sort above -- wins.
            //
            // Go's cartesian cost-ratio penalty never applies: a pair reaching
            // here either used an equality edge or was made NON-cartesian by
            // `hasOtherJoinCondition`, and `bestIsCartesian` stays false for
            // both.
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

/// How one pair is spelled: Go's `joinTypeWithExtMsg` plus the node swap
/// `checkConnection` performs to keep an outer join's sides where the
/// statement wrote them.
#[derive(Clone, Copy)]
struct Shape {
    /// The rebuilt `FROM` node's join type.
    tp: JoinType,
    /// The cost model's own kind, AFTER the swap.
    kind: JoinKind,
    /// Whether the candidate node takes the LEFT position.
    swap: bool,
}

impl Shape {
    const INNER: Self = Self {
        tp: JoinType::Cross,
        kind: JoinKind::Inner,
        swap: false,
    };
}

/// Go `checkConnection`'s join-type half: which single join type a pair's
/// connecting edges spell, and whether the two plans have to change places.
///
/// `None` REFUSES the pair, which is this module's answer wherever Go's own
/// `joinTypes[idx]` would be ambiguous or the null-extended side is not
/// exactly one of the two plans. Refusing is fail-closed: the pair is simply
/// not built, and if that leaves the group unconsumed [`greedy_solve`]
/// declines the whole reorder.
fn shape_of(
    left_leaves: &[usize],
    right_leaves: &[usize],
    used: &[usize],
    edges: &[Edge<'_>],
) -> Option<Shape> {
    let mut outer: Option<Outer> = None;
    let mut inner_edge = false;
    for index in used {
        match edges[*index].outer {
            None => inner_edge = true,
            Some(next) => match outer {
                None => outer = Some(next),
                Some(current) if current == next => {}
                // Two different outer joins over one pair: Go reads a single
                // `joinTypes[idx]` here and this module will not guess which.
                Some(_) => return None,
            },
        }
    }
    let Some(outer) = outer else {
        return Some(Shape::INNER);
    };
    // An inner edge beside an outer one is the same ambiguity.
    if inner_edge {
        return None;
    }
    // The null-extended relation joins as a whole and as itself.
    let extended_is_right = right_leaves == [outer.extended];
    let extended_is_left = left_leaves == [outer.extended];
    if !(extended_is_left || extended_is_right) {
        return None;
    }
    match outer.tp {
        // `A LEFT JOIN b`: the preserved side keeps the left position.
        JoinType::Left => Some(Shape {
            tp: JoinType::Left,
            kind: JoinKind::LeftOuter,
            swap: extended_is_left,
        }),
        // `a RIGHT JOIN B`: the extended side keeps the left position.
        _ => Some(Shape {
            tp: JoinType::Right,
            kind: JoinKind::RightOuter,
            swap: extended_is_right,
        }),
    }
}

/// Go `newJoinWithEdge` plus `calcJoinCumCost`.
fn build(
    left: &Candidate,
    right: &Candidate,
    used: &[usize],
    edges: &[Edge<'_>],
    leaves: &[Leaf<'_>],
    context: &DeriveStatsContext,
    shape: Shape,
) -> Candidate {
    let (left, right) = if shape.swap {
        (right, left)
    } else {
        (left, right)
    };
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
        kind: shape.kind,
    };
    let cum_cost = derive_stats(&model, context).cum_cost();
    Candidate {
        plan: Plan::Join {
            left: Box::new(left.plan.clone()),
            right: Box::new(right.plan.clone()),
            edges: used.to_vec(),
            tp: shape.tp,
        },
        model,
        cum_cost,
    }
}

// ---------------------------------------------------------------------------
// The leading hint
// ---------------------------------------------------------------------------

/// Go `CheckAndGenerateLeadingHint` (`rule_join_reorder.go:376`) followed by
/// `baseSingleGroupJoinOrderSolver.generateLeadingJoinGroup` (`:556`).
///
/// The statement's `/*+ leading(a, b, ...) */` names relations of THIS group
/// in the order they must join. Each name is found in the still-available
/// nodes and removed, and the running tree is extended by
/// `connectJoinNodes` -- `checkConnection` plus `makeJoin`, which is exactly
/// [`shape_of`] plus [`build`] here. The result becomes
/// `s.leadingJoinGroup`, which [`greedy_solve`] puts at the front of the
/// group so `constructConnectedJoinTree` starts from it.
///
/// `None` leaves the group unpinned, which is Go's `ok == false` arm. Go warns
/// there; this module warns only for the ONE reason it can state exactly --
/// a named table this group does not hold -- because every other reason
/// depends on machinery below.
///
/// NAMED RESIDUE: a hint table carrying an `@sel_N` query-block qualifier is
/// declined SILENTLY. Go resolves such a name against the plan of that block,
/// and a block other than this `FROM`'s own is reachable for Go only after
/// `extractJoinGroupImpl` has looked through the `Selection`/`Projection`
/// between them. This module's group never spans two blocks, so it cannot
/// tell whether Go's hint applied, and a warning either way would be a guess.
fn leading_prefix(
    select: &tidb_ast::SelectStmt,
    leaves: &[Leaf<'_>],
    edges: &[Edge<'_>],
    models: &[LogicalNode],
    context: &DeriveStatsContext,
    ctx: &crate::StmtContext,
) -> Option<Candidate> {
    use tidb_ast::{HintKind, LeadingElement};

    let mut written = select.hints.iter().filter_map(|hint| match &hint.kind {
        HintKind::Leading { elements, .. } => Some(elements),
        _ => None,
    });
    let elements = written.next()?;
    if written.next().is_some() {
        // `if hasDiffLeadingHint { ... }` -- Go's own wording.
        ctx.append_warning(
            1815,
            "We can only use one leading hint at most, when multiple leading \
             hints are used, all leading hints will be invalid",
        );
        return None;
    }
    let node_of = |leaf: usize| Candidate {
        plan: Plan::Leaf(leaf),
        cum_cost: derive_stats(&models[leaf], context).cum_cost(),
        model: models[leaf].clone(),
    };
    let mut available: Vec<usize> = (0..leaves.len()).collect();
    let mut current: Option<Candidate> = None;
    for element in elements {
        // A parenthesized nested group is Go's recursive `LeadingList` arm,
        // which this module does not build.
        let LeadingElement::Table(table) = element else {
            return None;
        };
        if table.qb_name.is_some() || table.db_name.is_some() {
            return None;
        }
        let found = available
            .iter()
            .position(|leaf| leaves[*leaf].visible.eq_ignore_ascii_case(&table.name));
        let Some(at) = found else {
            ctx.append_warning(
                1815,
                "leading hint is inapplicable, check if the leading hint table is valid",
            );
            return None;
        };
        let next = node_of(available.remove(at));
        current = Some(match current {
            None => next,
            Some(current) => {
                let (mut left_leaves, mut right_leaves) = (Vec::new(), Vec::new());
                current.plan.leaves(&mut left_leaves);
                next.plan.leaves(&mut right_leaves);
                // `connectJoinNodes` refuses a pair with no equality edge; the
                // cartesian one Go still builds when the group holds no outer
                // join has no `ON` to spell here, so it is declined too.
                let used = connecting_plans(&current.plan, &next.plan, edges);
                if used.is_empty() {
                    return None;
                }
                let shape = shape_of(&left_leaves, &right_leaves, &used, edges)?;
                build(&current, &next, &used, edges, leaves, context, shape)
            }
        });
    }
    // A one-table leading hint pins nothing Go's sort does not already decide.
    let current = current?;
    let mut pinned = Vec::new();
    current.plan.leaves(&mut pinned);
    (pinned.len() > 1).then_some(current)
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
    // An OUTER root's `ON` decides which rows are null-extended, so a leftover
    // conjunct cannot be conjoined onto it the way an inner root's filter can.
    if !residual_on.is_empty() && join.tp != JoinType::Cross {
        return None;
    }
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
            tp,
        } => {
            // A join the greedy reached through `hasOtherJoinCondition` alone
            // has no equality edge, so there is no `ON` to spell here: Go puts
            // the conjunct in `OtherConditions`, while here it is still in the
            // statement's own `WHERE`. `reduce` over an empty `used` yields
            // `None`, which declines the whole reorder rather than copying a
            // `WHERE` conjunct into an `ON` and evaluating it twice.
            let on =
                used.iter()
                    .map(|index| edges[*index].expr.clone())
                    .reduce(|left, right| {
                        Expr::Binary(BinaryOp::LogicAnd, Box::new(left), Box::new(right))
                    })?;
            Some(JoinNode::Join(Box::new(Join {
                left: rebuild_node(left, leaves, edges)?,
                right: Some(rebuild_node(right, leaves, edges)?),
                tp: *tp,
                straight: false,
                on: Some(on),
                using: Vec::new(),
                natural: false,
                explicit_parens: false,
            })))
        }
    }
}

// ---------------------------------------------------------------------------
// The row source
// ---------------------------------------------------------------------------

/// Every relation of one `FROM`'s join group with the row count
/// [`derive_stats`] derives for it, keyed by the name a column reference
/// reaches it by.
///
/// # Why this exists beside the DP solver
///
/// The DP above builds exactly these models, costs them, and throws them away.
/// A join-strategy chooser needs the same numbers
/// ([`crate::driver::join_search`]), and the ONE other place this tier derives
/// a per-node row count is [`crate::plan_trace::PlanTrace`], which the driver
/// constructs only for `EXPLAIN`. Reading rows off the trace would make the
/// STRATEGY depend on whether the statement is being explained; this source
/// reads the statement, the catalog and the statistics and nothing else, so it
/// answers identically either way. That equality is a test, not a claim:
/// `crate::tests_join_search::the_choice_is_the_same_under_explain_and_bare_execution`.
///
/// It is built from the join group as WRITTEN, before any reorder, because the
/// leaves and their pushed-down predicates are the same set either way and
/// only the tree over them moves.
pub(crate) struct RowSource {
    leaves: Vec<RowLeaf>,
    /// `(leaf, column)` pairs, as [`Edge`] holds them.
    edges: Vec<JoinEdge>,
    context: DeriveStatsContext,
    /// Whether every written predicate belongs to one leaf or is an equality
    /// edge between two leaves. An index join that installs every leaf's
    /// filter can consume this wider class even when a leaf is not a point
    /// get.
    where_is_leaf_or_join_equality: bool,
    /// Outer-join leaf predicates are still useful to access-path planning,
    /// but this source's inner-join cardinality formula must not price an
    /// outer join. `rows_of_join` declines while the filter inventory remains
    /// available.
    join_rows_valid: bool,
    /// The written WHERE inventory, classified against this join group.
    where_parts: Vec<WherePart>,
    /// Leaves whose complete local predicate was accepted by their actual
    /// physical source. The builder records this only after it commits a
    /// point/range/index/table access path, so residual selection placement
    /// cannot drift from the executor that was built.
    consumed_filter_leaves: RefCell<BTreeSet<usize>>,
}

struct WherePart {
    expr: Expr,
    class: WhereClass,
}

enum WhereClass {
    /// Executed by an inner join equality.
    Edge,
    /// Eligible for one leaf, provided that leaf's committed path accepts it.
    Single(usize),
    /// Executed as an inner join's `other cond`.
    JoinOther,
    /// Must remain above the join.
    Residual,
}

/// One relation of the group: how it is named, what it models, and the
/// [`ColumnId`] of each of its output columns.
struct RowLeaf {
    visible: String,
    /// Output column names, for resolving a GROUP BY above this FROM.
    columns: Vec<String>,
    model: LogicalNode,
    ids: Vec<ColumnId>,
    /// The predicates that reference only this relation. The physical
    /// builder reuses these for leaf access-path narrowing; the original
    /// predicates remain above the join for semantic equivalence.
    filters: Vec<Expr>,
    /// DNF-derived predicates Go records as a cop Selection above this leaf.
    trace_filters: Vec<Expr>,
}

/// Builds the [`RowSource`] of one `FROM`, or `None` for a group whose shape
/// [`emit`] cannot model.
///
/// The inputs are exactly [`reorder`]'s, and the work up to the models is the
/// same work; only the DP is not run.
pub(crate) fn row_source(
    join: &Join,
    where_clause: Option<&Expr>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<RowSource> {
    let mut ids = Ids::default();
    let mut leaves = Vec::new();
    let mut on_conds = Vec::new();
    // The row counts here feed a JOIN-STRATEGY comparison, whose
    // [`LogicalNode::Join`] is built inner-only; an outer join's own count is
    // `max(equality estimate, preserved side)` and is not derivable from the
    // leaf set alone. So this source keeps declining an outer-join group,
    // which is the answer it gave before [`reorder`] started accepting one.
    // Collect through outer joins so their preserved-side WHERE predicates
    // and ON-derived constants can still narrow both leaf reads. This does
    // not authorize reordering: `join_rows_valid` below keeps the inner-only
    // strategy coster away from these rows.
    let scope = Scope {
        outer_join_reorder: true,
    };
    if !collect(
        join,
        catalog,
        current_db,
        &scope,
        &mut ids,
        &mut leaves,
        &mut on_conds,
    ) {
        return None;
    }
    let join_rows_valid = on_conds.iter().all(|condition| condition.outer.is_none());
    let mut where_conjuncts = Vec::new();
    if let Some(where_clause) = where_clause {
        crate::plan_trace::collect_and(where_clause, &mut where_conjuncts);
    }
    let mut edges = Vec::new();
    let mut filters: Vec<Vec<Expr>> = vec![Vec::new(); leaves.len()];
    let mut trace_filters: Vec<Vec<Expr>> = vec![Vec::new(); leaves.len()];
    let extended: BTreeSet<usize> = on_conds
        .iter()
        .filter_map(|condition| condition.outer.map(|outer| outer.extended))
        .collect();
    // An outer join's ON predicate may be pushed only into its
    // null-supplying side. Pushing a preserved-side condition would delete a
    // row that the join must instead retain and NULL-extend.
    for condition in &on_conds {
        match classify(condition.expr, &leaves, condition.outer)? {
            Classified::Edge(edge) => edges.push((edge.left, edge.right)),
            Classified::Single(leaf)
                if condition.outer.is_none_or(|outer| leaf == outer.extended) =>
            {
                filters[leaf].push(condition.expr.clone());
            }
            Classified::Single(_) => {}
            Classified::Other(_) | Classified::Foreign => {}
        }
    }
    let mut where_parts = Vec::with_capacity(where_conjuncts.len());
    for conjunct in where_conjuncts {
        let class = match classify(conjunct, &leaves, None)? {
            Classified::Edge(edge) if join_rows_valid => {
                edges.push((edge.left, edge.right));
                WhereClass::Edge
            }
            Classified::Single(leaf) if !extended.contains(&leaf) => {
                filters[leaf].push(conjunct.clone());
                WhereClass::Single(leaf)
            }
            Classified::Other(_) => {
                for leaf in 0..leaves.len() {
                    if extended.contains(&leaf) {
                        continue;
                    }
                    if let Some(derived) = project_dnf_to_leaf(conjunct, leaf, &leaves) {
                        filters[leaf].push(derived.clone());
                        trace_filters[leaf].push(derived);
                    }
                }
                if join_rows_valid {
                    WhereClass::JoinOther
                } else {
                    WhereClass::Residual
                }
            }
            Classified::Edge(_) | Classified::Single(_) | Classified::Foreign => {
                WhereClass::Residual
            }
        };
        where_parts.push(WherePart {
            expr: conjunct.clone(),
            class,
        });
    }
    propagate_leaf_constants(&leaves, &edges, &mut filters);
    let where_is_leaf_or_join_equality = !where_parts.is_empty()
        && where_parts
            .iter()
            .all(|part| matches!(part.class, WhereClass::Edge | WhereClass::Single(_)));
    let mut demands: Vec<Demand> = (0..leaves.len()).map(|_| Demand::default()).collect();
    for (left, right) in &edges {
        demands[left.0].not_null.insert(left.1);
        demands[right.0].not_null.insert(right.1);
    }
    for (demand, filters) in demands.iter_mut().zip(filters) {
        demand.filters = filters;
    }
    let rows: Option<Vec<RowLeaf>> = leaves
        .iter()
        .zip(&demands)
        .enumerate()
        .map(|(at, (leaf, demand))| {
            Some(RowLeaf {
                visible: leaf.visible.clone(),
                columns: leaf.columns.clone(),
                model: emit(&leaf.rel, demand)?,
                ids: (0..leaf.columns.len())
                    .map(|column| column_id(&leaf.rel, column))
                    .collect::<Option<Vec<_>>>()?,
                filters: demand.filters.clone(),
                trace_filters: trace_filters[at].clone(),
            })
        })
        .collect();
    Some(RowSource {
        leaves: rows?,
        edges,
        context: DeriveStatsContext::with_join_reorder_threshold(ctx.join_reorder_threshold()),
        where_is_leaf_or_join_equality,
        join_rows_valid,
        where_parts,
        consumed_filter_leaves: RefCell::new(BTreeSet::new()),
    })
}

/// Go's DNF predicate pushdown: for `(a1 AND b1) OR (a2 AND b2)`, each leaf
/// may evaluate the necessary weakening `a1 OR a2` / `b1 OR b2`. Every OR
/// branch must contribute a predicate for the target leaf; otherwise the
/// weakening is `TRUE` and no Selection is useful.
fn project_dnf_to_leaf(expr: &Expr, target: usize, leaves: &[Leaf<'_>]) -> Option<Expr> {
    fn collect_or<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
        match expr {
            Expr::Paren(inner) => collect_or(inner, out),
            Expr::Binary(BinaryOp::LogicOr, left, right) => {
                collect_or(left, out);
                collect_or(right, out);
            }
            other => out.push(other),
        }
    }

    let mut branches = Vec::new();
    collect_or(expr, &mut branches);
    if branches.len() < 2 {
        return None;
    }
    branches
        .into_iter()
        .map(|branch| {
            let mut conjuncts = Vec::new();
            crate::plan_trace::collect_and(branch, &mut conjuncts);
            conjuncts
                .into_iter()
                .filter(|conjunct| {
                    matches!(classify(conjunct, leaves, None), Some(Classified::Single(leaf)) if leaf == target)
                })
                .cloned()
                .reduce(|left, right| {
                    Expr::Binary(BinaryOp::LogicAnd, Box::new(left), Box::new(right))
                })
        })
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .reduce(|left, right| Expr::Binary(BinaryOp::LogicOr, Box::new(left), Box::new(right)))
}

/// Copies a constant equality through the inner-join equality graph. This is
/// the narrow constant-propagation rule needed for composite leading keys:
/// `warehouse.w_id = 1` plus `customer.c_w_id = warehouse.w_id` gives the
/// customer leaf a local `c_w_id = 1` range. Only base-table columns with the
/// same integer domain are propagated; refusing other types is fail-closed.
fn propagate_leaf_constants(leaves: &[Leaf<'_>], edges: &[JoinEdge], filters: &mut [Vec<Expr>]) {
    let mut constants = Vec::new();
    for predicates in filters.iter() {
        for predicate in predicates {
            let Some((column, value)) = local_constant_equality(predicate, leaves) else {
                continue;
            };
            constants.push((column, value));
        }
    }
    for (source, value) in constants {
        let mut reachable = vec![source];
        let mut seen = BTreeSet::from([source]);
        let mut cursor = 0;
        while let Some(column) = reachable.get(cursor).copied() {
            cursor += 1;
            for (left, right) in edges {
                let next = if *left == column {
                    *right
                } else if *right == column {
                    *left
                } else {
                    continue;
                };
                if seen.insert(next) {
                    reachable.push(next);
                }
            }
        }
        for target in reachable.into_iter().filter(|target| *target != source) {
            if !same_integer_domain(leaves, source, target) {
                continue;
            }
            let Some(path) = leaf_column_path(leaves, target) else {
                continue;
            };
            filters[target.0].push(tidb_ast::Expr::Binary(
                tidb_ast::BinaryOp::Eq,
                Box::new(tidb_ast::Expr::Column(path)),
                Box::new(value.clone()),
            ));
        }
    }
}

/// The `(leaf, column)` and constant expression of a local equality.
fn local_constant_equality(
    predicate: &Expr,
    leaves: &[Leaf<'_>],
) -> Option<((usize, usize), Expr)> {
    let Expr::Binary(BinaryOp::Eq, lhs, rhs) = predicate else {
        return None;
    };
    let (column, value) = match (strip(lhs), strip(rhs)) {
        (Expr::Column(path), other) if propagatable_constant(other) => (path, other),
        (other, Expr::Column(path)) if propagatable_constant(other) => (path, other),
        _ => return None,
    };
    Some((resolve(column, leaves)?, value.clone()))
}

fn propagatable_constant(expr: &Expr) -> bool {
    match expr {
        Expr::Paren(inner) => propagatable_constant(inner),
        Expr::Unary(tidb_ast::UnaryOp::Minus | tidb_ast::UnaryOp::Plus, inner) => {
            propagatable_constant(inner)
        }
        Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Float(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::String(_)
        | Expr::RawString(_)
        | Expr::CharsetString { .. }
        | Expr::Bool(_) => true,
        _ => false,
    }
}

fn leaf_column_path(leaves: &[Leaf<'_>], (leaf, column): (usize, usize)) -> Option<Vec<String>> {
    Some(vec![
        leaves.get(leaf)?.visible.clone(),
        leaves.get(leaf)?.columns.get(column)?.clone(),
    ])
}

fn same_integer_domain(leaves: &[Leaf<'_>], left: (usize, usize), right: (usize, usize)) -> bool {
    let column_type = |(leaf, column): (usize, usize)| match &leaves.get(leaf)?.rel {
        Rel::Table(table) => table.columns.get(column).map(|(_, field_type)| field_type),
        Rel::Derived(_) => None,
    };
    let (Some(left), Some(right)) = (column_type(left), column_type(right)) else {
        return false;
    };
    left.code().is_type_integer()
        && right.code().is_type_integer()
        && left.is_unsigned() == right.is_unsigned()
}

impl RowSource {
    /// Whether installing every leaf-local filter and every join equality
    /// accounts for the complete written `WHERE`.
    pub(crate) const fn all_where_is_leaf_or_join_equality(&self) -> bool {
        self.where_is_leaf_or_join_equality
    }

    /// Records that the committed access path for `visible` accepted every
    /// leaf-local predicate offered to it.
    pub(crate) fn mark_leaf_filters_consumed(&self, visible: &str) {
        if let Some((leaf, _)) = self
            .leaves
            .iter()
            .enumerate()
            .find(|(_, leaf)| leaf.visible.eq_ignore_ascii_case(visible))
        {
            self.consumed_filter_leaves.borrow_mut().insert(leaf);
        }
    }

    /// The part of the written `WHERE` that the committed leaf paths and
    /// inner-join equalities did not consume.
    pub(crate) fn residual_where(&self) -> Option<Expr> {
        let consumed = self.consumed_filter_leaves.borrow();
        self.where_parts
            .iter()
            .filter_map(|part| match part.class {
                WhereClass::Edge | WhereClass::JoinOther => None,
                WhereClass::Single(leaf) if consumed.contains(&leaf) => None,
                WhereClass::Single(_) | WhereClass::Residual => Some(part.expr.clone()),
            })
            .reduce(|left, right| Expr::Binary(BinaryOp::LogicAnd, Box::new(left), Box::new(right)))
    }

    /// The predicates that can be evaluated using only the named leaf.
    pub(crate) fn filters_for(&self, visible: &str) -> Option<&[Expr]> {
        self.leaves
            .iter()
            .find(|leaf| leaf.visible.eq_ignore_ascii_case(visible))
            .map(|leaf| leaf.filters.as_slice())
    }

    /// DNF-derived predicates that are physically evaluated at this leaf.
    pub(crate) fn trace_filters_for(&self, visible: &str) -> Option<&[Expr]> {
        self.leaves
            .iter()
            .find(|leaf| leaf.visible.eq_ignore_ascii_case(visible))
            .map(|leaf| leaf.trace_filters.as_slice())
    }

    /// The rows of a joined pair, and of each side, in one walk: the three
    /// counts a join-strategy comparison reads.
    pub(crate) fn rows_of_join(&self, left: &[String], right: &[String]) -> Option<JoinRows> {
        if !self.join_rows_valid {
            return None;
        }
        let (left_rows, left_model, left_at) = self.model_of(left)?;
        let (right_rows, right_model, right_at) = self.model_of(right)?;
        let (left_keys, right_keys) = self.keys_between(&left_at, &right_at)?;
        let model = LogicalNode::Join {
            left: Box::new(left_model),
            right: Box::new(right_model),
            left_keys,
            right_keys,
            kind: JoinKind::Inner,
        };
        Some(JoinRows {
            left: left_rows,
            right: right_rows,
            joined: derive_stats(&model, &self.context).stats.row_count,
        })
    }

    /// Go's `LogicalAggregation.DeriveStats` row count for a GROUP BY above
    /// this complete FROM tree. This is the group-key NDV after leaf filters
    /// and join-key NDV clamping, so it distinguishes TPCC condition 01's
    /// one-row join from condition 04's eight district groups without a
    /// trace-only heuristic.
    pub(crate) fn grouped_rows(&self, group_by: &[tidb_ast::GroupByItem]) -> Option<f64> {
        if !self.join_rows_valid || group_by.is_empty() {
            return None;
        }
        let names: Vec<String> = self
            .leaves
            .iter()
            .map(|leaf| leaf.visible.clone())
            .collect();
        let (_, child, _) = self.model_of(&names)?;
        let mut group_columns = Vec::new();
        for item in group_by {
            for path in column_paths(&item.expr) {
                let (leaf, column) = self.resolve_output_path(&path)?;
                group_columns.push(*self.leaves.get(leaf)?.ids.get(column)?);
            }
        }
        let model = LogicalNode::Aggregation {
            child: Box::new(child),
            group_by: group_columns,
            // Only RowCount is read. Go assigns that same NDV to every
            // output, so an empty synthetic schema loses no input here.
            columns: Vec::new(),
        };
        Some(derive_stats(&model, &self.context).stats.row_count)
    }

    /// Resolves a column path against this row-count inventory with the same
    /// ambiguity rule as the logical leaf walk.
    fn resolve_output_path(&self, path: &[String]) -> Option<(usize, usize)> {
        let (qualifier, name) = match path {
            [name] => (None, name),
            [table, name] | [_, table, name] => (Some(table), name),
            _ => return None,
        };
        let mut found = None;
        for (leaf, relation) in self.leaves.iter().enumerate() {
            if qualifier.is_some_and(|table| !table.eq_ignore_ascii_case(&relation.visible)) {
                continue;
            }
            let Some(column) = relation
                .columns
                .iter()
                .position(|column| column.eq_ignore_ascii_case(name))
            else {
                continue;
            };
            if found.is_some() {
                return None;
            }
            found = Some((leaf, column));
        }
        found
    }

    /// The model of one side, its row count, and which leaves it holds.
    fn model_of(&self, names: &[String]) -> Option<(f64, LogicalNode, Vec<usize>)> {
        let at: Option<Vec<usize>> = names
            .iter()
            .map(|name| self.leaves.iter().position(|leaf| leaf.visible == *name))
            .collect();
        let at = at?;
        let (&first, rest) = at.split_first()?;
        let mut model = self.leaves[first].model.clone();
        let mut joined = vec![first];
        for &right in rest {
            let (left_keys, right_keys) = self.keys_between(&joined, &[right])?;
            model = LogicalNode::Join {
                left: Box::new(model),
                right: Box::new(self.leaves[right].model.clone()),
                left_keys,
                right_keys,
                kind: JoinKind::Inner,
            };
            joined.push(right);
        }
        let rows = derive_stats(&model, &self.context).stats.row_count;
        Some((rows, model, at))
    }

    /// The equality keys connecting two disjoint leaf sets, as `derive_stats`
    /// keys a [`LogicalNode::Join`] -- the same walk [`build`] does.
    fn keys_between(
        &self,
        left: &[usize],
        right: &[usize],
    ) -> Option<(Vec<ColumnId>, Vec<ColumnId>)> {
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();
        for (a, b) in &self.edges {
            let pair = if left.contains(&a.0) && right.contains(&b.0) {
                Some((*a, *b))
            } else if left.contains(&b.0) && right.contains(&a.0) {
                Some((*b, *a))
            } else {
                None
            };
            if let Some((near, far)) = pair {
                left_keys.push(*self.leaves[near.0].ids.get(near.1)?);
                right_keys.push(*self.leaves[far.0].ids.get(far.1)?);
            }
        }
        Some((left_keys, right_keys))
    }
}

/// The three row counts one join site's comparison reads.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct JoinRows {
    /// The left child's `StatsInfo().RowCount`.
    pub(crate) left: f64,
    /// The right child's.
    pub(crate) right: f64,
    /// The join's own.
    pub(crate) joined: f64,
}
