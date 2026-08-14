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

//! WHICH JOIN STRATEGY THIS SITE MAY USE, asked of
//! [`tidb_planner::find_best_task`] rather than of a structural rule.
//!
//! # What this wires, and to what
//!
//! [`tidb_planner::find_best_task::exhaust_join`] is
//! `exhaustPhysicalPlans4LogicalJoin` for a root task: given a `LogicalJoin`
//! and the property its parent requires, it returns the physical candidates Go
//! ENUMERATES there. This module builds that `LogicalJoin` out of what the
//! driver already knows at `build_join` -- the equality keys
//! ([`crate::hash_join::split_equi`]), the two sides' widths, and the orders
//! each side can provide ([`crate::driver::merge_decision`]'s
//! `preparePossibleProperties`) -- and reads the answer.
//!
//! Column identity is the JOINED ROW OFFSET, which is the same identity
//! [`crate::driver::merge_decision::child_required_prop`] writes into the
//! property this site is asked for, so the two are one numbering rather than
//! two that can drift.
//!
//! # The rule this landing takes from the enumeration, and the one it refuses
//!
//! `getHashJoins` opens with "hash join doesn't promise any orders" and
//! returns NOTHING under a non-empty `prop.SortItems`; `GetMergeJoin` needs a
//! `LeftProperties` entry covering every left join key, which no index
//! provides for a projected expression. A join under a parent merge join whose
//! key is such an expression therefore has exactly ONE family of candidates
//! left, and no cost can change which family wins:
//!
//! * INDEX BY ELIMINATION -- taken here.
//! * anything else -- REFUSED here, and refused fail-closed. Choosing between
//!   two families is `findBestTask`'s costing layer, which needs a priced
//!   `Candidate` tree per side ([`tidb_planner::candidate_cost`]); this tier
//!   builds executors bottom-up and has no physical-plan IR to price. NAMED
//!   RESIDUE, and the one it hides is "an index join Go picks ON COST under an
//!   EMPTY property". The measurement in [`super::index_join_decision`] says
//!   what refusing it costs.
//!
//! # Why the rows are read at all
//!
//! A candidate the search cannot PRICE is a candidate it cannot choose, so a
//! site whose row counts this tier cannot derive is refused here rather than
//! decided by the structural rule underneath. The rows come from
//! [`crate::driver::join_reorder::RowSource`] -- `derive_stats` over the
//! statement, the catalog and the statistics -- and NOT from
//! [`crate::plan_trace::PlanTrace`], which exists only under `EXPLAIN`. That
//! is the whole reason the source was built: a chooser reading the trace would
//! make `EXPLAIN` print a strategy the bare statement does not run. See
//! `crate::tests_join_search`.

use std::collections::BTreeMap;

use tidb_ast::{Join, JoinNode, JoinType, QueryStmt, SelectField};
use tidb_planner::find_best_task::{
    exhaust_join, DecisionTree, JoinCostModel, JoinStrategy, LeafAlternative, LeafRole,
    LogicalJoin, LogicalJoinType, LogicalNode, Task,
};
use tidb_planner::physical_property::{PhysicalProperty, SortItem};

use crate::driver::join_reorder::RowSource;
use crate::driver::{Catalog, TableEntry};
use crate::hash_join::EquiKey;

/// What the enumeration leaves standing at one join site.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Chosen {
    /// Only index-join candidates were enumerated: the choice by elimination,
    /// which no cost can overturn.
    Index,
    /// Refused, and why. See [`Refusal`].
    Refused(Refusal),
}

/// Why a site was refused, one variant per MEASURED population.
///
/// Over the 106-topic replay every site this tier reaches falls in one of
/// these, and `Index` is reached by NONE of them -- see the census in the
/// module doc.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Refusal {
    /// A cross join: no equality to probe an index with.
    NoEquiKeys,
    /// This `FROM`'s shape has no estimate owner
    /// ([`crate::driver::join_reorder::row_source`] declined it).
    NoRowSource,
    /// A side exposes a relation under no name.
    NoRelationNames,
    /// The estimate owner does not hold these relations.
    RowsDeclined,
    /// A side whose provided orders
    /// [`crate::driver::merge_decision::possible_properties`] declined, which
    /// would understate the merge-join candidates.
    NoChildOrders,
    /// The property this site was asked for is EMPTY, so `getHashJoins`
    /// answers too and the choice needs the costing layer this tier refuses.
    HashAlsoEnumerated,
    /// The property is non-empty -- no hash join -- but a merge join is still
    /// a candidate, so the choice again needs the costing layer.
    MergeAlsoEnumerated,
    /// No index-join candidate: `prop` reads a column the inner side owns.
    NoIndexCandidate,
}

/// Go's `p.LeftProperties` / `p.RightProperties`: the column orders each
/// child's output already carries, in that child's OWN row offsets.
pub(crate) type ChildOrders<'a> = (&'a [Vec<usize>], &'a [Vec<usize>]);

/// Everything one join site hands the search.
pub(crate) struct SearchInput<'a> {
    /// The `FROM` node being built.
    pub(crate) join: &'a Join,
    /// `base.JoinType`.
    pub(crate) join_type: LogicalJoinType,
    /// The equality conjuncts, as [`crate::hash_join::split_equi`] split them.
    pub(crate) keys: &'a [EquiKey],
    /// Where the right child's columns start in the joined row.
    pub(crate) left_width: usize,
    /// The joined row's width.
    pub(crate) width: usize,
    /// The orders each child's output already carries, in its OWN row
    /// offsets -- Go's `p.LeftProperties` / `p.RightProperties`. `None` is a
    /// side [`crate::driver::merge_decision::possible_properties`] declined,
    /// which would understate the merge-join candidates and so is refused.
    pub(crate) orders: Option<ChildOrders<'a>>,
    /// The property this join's parent requires of it.
    pub(crate) required: &'a PhysicalProperty,
    /// The estimate owner, or `None` when this `FROM` has none.
    pub(crate) rows: Option<&'a RowSource>,
}

/// The recursively costed strategy chosen for each logical join subtree.
///
/// A key is the sorted set of visible relation names under the join. The
/// builder already refuses ambiguous unaliased self joins, so this is stable
/// across physical build/probe reversals while remaining independent of AST
/// allocation addresses.
#[derive(Clone, Debug, Default)]
pub(crate) struct RecursiveGuide {
    sites: BTreeMap<Vec<String>, JoinStrategy>,
}

impl RecursiveGuide {
    /// The strategy chosen for this exact logical subtree.
    pub(crate) fn strategy_for(&self, join: &Join) -> Option<&JoinStrategy> {
        self.sites.get(&join_key(join)?)
    }
}

struct LogicalBuild {
    node: LogicalNode,
    schema: Vec<i64>,
    names: Vec<String>,
}

/// Runs Go's recursive `findBestTask` rule over the executor driver's live
/// logical join tree.
///
/// Unsupported leaf shapes return `None` and preserve the existing planner;
/// no partial tree is allowed to steer only some joins. The supported shape
/// is deliberately dependency-closed: TiKV tables, order-preserving
/// projections over them, and their ordinary inner/outer join nodes.
pub(crate) fn recursive_guide(
    join: &Join,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    demand: crate::driver::leaf_demand::FromDemand<'_>,
    required: &PhysicalProperty,
) -> Option<RecursiveGuide> {
    let hints = demand.join_hints?;
    if !tree_has_forced_merge(join, hints) {
        // The current integration closes the recursive-hint seam. Unhinted
        // trees keep the existing chooser until every index-probe leaf shape
        // is represented, so a partial access-path model cannot perturb them.
        return None;
    }
    let rows = demand.rows?;
    let columns = demand.columns?;
    let mut next_column = 1_i64;
    let built = build_logical_node(
        &JoinNode::Join(Box::new(join.clone())),
        catalog,
        current_db,
        ctx,
        demand,
        columns,
        rows,
        &mut next_column,
    )?;
    let model = LiveJoinCostModel {
        hash_concurrency: ctx.hash_join_concurrency(),
    };
    let task = tidb_planner::find_best_task::find_best_task(
        &built.node,
        required,
        LeafRole::Plain,
        &model,
        ctx.optimizer_cost_env(),
    )?;
    let mut guide = RecursiveGuide::default();
    collect_decisions(
        &JoinNode::Join(Box::new(join.clone())),
        &task.decision,
        &mut guide,
    )?;
    Some(guide)
}

fn tree_has_forced_merge(
    join: &Join,
    hints: &crate::driver::join_method_hints::JoinMethodHints,
) -> bool {
    let sides = crate::driver::join_method_hints::side_aliases(join);
    if hints.forces_merge((sides.0.as_deref(), sides.1.as_deref())) {
        return true;
    }
    let nested = |node: &JoinNode| match node {
        JoinNode::Join(child) => tree_has_forced_merge(child, hints),
        _ => false,
    };
    nested(&join.left) || join.right.as_ref().is_some_and(nested)
}

#[allow(clippy::too_many_arguments)]
fn build_logical_node(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    demand: crate::driver::leaf_demand::FromDemand<'_>,
    columns: &crate::driver::leaf_demand::LeafDemand,
    rows: &RowSource,
    next_column: &mut i64,
) -> Option<LogicalBuild> {
    match node {
        JoinNode::Table(table_ref) => {
            let (database, name) =
                crate::driver::split_table_path(&table_ref.name, current_db).ok()?;
            let entry = catalog.get_in(database, name)?;
            let TableEntry::Kv(table) = entry else {
                return None;
            };
            let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
            let table_columns = entry.column_list();
            let schema = allocate_columns(table_columns.len(), next_column);
            let alternatives = crate::driver::leaf_access::leaf_alternatives(
                table,
                table_ref,
                &visible,
                &table_columns,
                columns,
                catalog,
                ctx,
                &schema,
            )?;
            Some(LogicalBuild {
                node: LogicalNode::Leaf(alternatives),
                schema,
                names: vec![visible],
            })
        }
        JoinNode::Derived {
            subquery,
            alias,
            lateral: false,
            column_names,
        } => build_projection(
            subquery,
            alias.as_deref()?,
            column_names,
            catalog,
            current_db,
            ctx,
            demand,
            columns,
            rows,
            next_column,
        ),
        JoinNode::Derived { .. } => None,
        JoinNode::Join(join) => {
            let Some(right_node) = &join.right else {
                return build_logical_node(
                    &join.left,
                    catalog,
                    current_db,
                    ctx,
                    demand,
                    columns,
                    rows,
                    next_column,
                );
            };
            if join.natural || !join.using.is_empty() {
                return None;
            }
            let left = build_logical_node(
                &join.left,
                catalog,
                current_db,
                ctx,
                demand,
                columns,
                rows,
                next_column,
            )?;
            let right = build_logical_node(
                right_node,
                catalog,
                current_db,
                ctx,
                demand,
                columns,
                rows,
                next_column,
            )?;
            let left_props = crate::driver::merge_decision::possible_properties(
                &join.left,
                catalog,
                current_db,
                demand.offered,
            )?;
            let right_props = crate::driver::merge_decision::possible_properties(
                right_node,
                catalog,
                current_db,
                demand.offered,
            )?;
            if left_props.width != left.schema.len() || right_props.width != right.schema.len() {
                return None;
            }
            let empty = PhysicalProperty::default();
            let keys = crate::driver::merge_decision::enforced_merge_join_decision(
                join,
                catalog,
                current_db,
                &empty,
                demand.offered,
            );
            let left_keys = keys
                .as_ref()
                .map(|decision| {
                    decision
                        .plan
                        .keys
                        .iter()
                        .filter_map(|key| left.schema.get(key.left).copied())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let right_keys = keys
                .as_ref()
                .map(|decision| {
                    decision
                        .plan
                        .keys
                        .iter()
                        .filter_map(|key| right.schema.get(key.right).copied())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if left_keys.len() != right_keys.len() {
                return None;
            }
            let counts = rows.rows_of_join(&left.names, &right.names);
            let output_rows = counts.map(|count| count.joined).or_else(|| {
                Some(rows.rows_of_relations(&left.names)? * rows.rows_of_relations(&right.names)?)
            });
            let hinted_sides = crate::driver::join_method_hints::side_aliases(join);
            let force_merge = demand.join_hints.is_some_and(|hints| {
                hints.forces_merge((hinted_sides.0.as_deref(), hinted_sides.1.as_deref()))
            });
            let mut schema = left.schema.clone();
            schema.extend_from_slice(&right.schema);
            let mut names = left.names.clone();
            names.extend(right.names.clone());
            Some(LogicalBuild {
                node: LogicalNode::Join(Box::new(LogicalJoin {
                    join_type: logical_join_type(join.tp),
                    left: Box::new(left.node),
                    right: Box::new(right.node),
                    left_keys,
                    right_keys,
                    left_schema: left.schema,
                    right_schema: right.schema,
                    left_properties: map_orders(&left_props.orders, &schema[..left_props.width]),
                    right_properties: map_orders(&right_props.orders, &schema[left_props.width..]),
                    force_merge,
                    output_rows,
                })),
                schema,
                names,
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_projection(
    subquery: &QueryStmt,
    alias: &str,
    column_names: &[String],
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    demand: crate::driver::leaf_demand::FromDemand<'_>,
    columns: &crate::driver::leaf_demand::LeafDemand,
    rows: &RowSource,
    next_column: &mut i64,
) -> Option<LogicalBuild> {
    let QueryStmt::Select(select) = subquery else {
        return None;
    };
    if select.distinct
        || select.from.is_none()
        || select.where_clause.is_some()
        || !select.group_by.is_empty()
        || select.having.is_some()
        || !select.windows.is_empty()
        || !select.order_by.is_empty()
        || select.limit.is_some()
    {
        return None;
    }
    let child_join = select.from.as_ref()?;
    let child = build_logical_node(
        &JoinNode::Join(Box::new(child_join.clone())),
        catalog,
        current_db,
        ctx,
        demand,
        columns,
        rows,
        next_column,
    )?;
    let fields: Vec<&tidb_ast::Expr> = select
        .fields
        .fields()
        .iter()
        .map(|field| match field {
            SelectField::Expr { expr, .. } => Some(expr),
            SelectField::Wildcard(_) => None,
        })
        .collect::<Option<Vec<_>>>()?;
    if !column_names.is_empty() && column_names.len() != fields.len() {
        return None;
    }
    let inner_props = crate::driver::merge_decision::possible_properties(
        &JoinNode::Join(Box::new(child_join.clone())),
        catalog,
        current_db,
        demand.offered,
    )?;
    let sources: Vec<Option<usize>> = fields
        .iter()
        .map(|expr| match expr {
            tidb_ast::Expr::Column(path) => inner_props.offset_of(path),
            _ => None,
        })
        .collect();
    let schema = allocate_columns(fields.len(), next_column);
    let LogicalNode::Leaf(child_alternatives) = child.node else {
        return None;
    };
    let env = ctx.optimizer_cost_env();
    let alternatives = child_alternatives
        .into_iter()
        .filter(|alternative| alternative.role == LeafRole::Plain)
        .map(|alternative| {
            let input_rows = tidb_planner::candidate_cost::evaluate(
                &alternative.plan,
                env,
                tidb_planner::task_type::TaskType::Root,
            )
            .rows;
            let order = project_order(&alternative.order, &child.schema, &sources, &schema);
            LeafAlternative {
                plan: tidb_planner::candidate_cost::Candidate::Projection {
                    child: Box::new(alternative.plan),
                    input_rows,
                    exprs: fields
                        .iter()
                        .map(|expr| !matches!(expr, tidb_ast::Expr::Column(_)))
                        .collect(),
                },
                order,
                role: LeafRole::Plain,
            }
        })
        .collect::<Vec<_>>();
    (!alternatives.is_empty()).then_some(LogicalBuild {
        node: LogicalNode::Leaf(alternatives),
        schema,
        names: vec![alias.to_owned()],
    })
}

fn project_order(
    order: &[SortItem],
    child_schema: &[i64],
    sources: &[Option<usize>],
    schema: &[i64],
) -> Vec<SortItem> {
    let mut projected = Vec::new();
    for item in order {
        let Some(child_at) = child_schema.iter().position(|column| *column == item.col) else {
            break;
        };
        let Some(output_at) = sources.iter().position(|source| *source == Some(child_at)) else {
            break;
        };
        projected.push(SortItem::new(schema[output_at], item.desc));
    }
    projected
}

fn allocate_columns(width: usize, next: &mut i64) -> Vec<i64> {
    (0..width)
        .map(|_| {
            let id = *next;
            *next += 1;
            id
        })
        .collect()
}

fn map_orders(orders: &[Vec<usize>], schema: &[i64]) -> Vec<Vec<i64>> {
    orders
        .iter()
        .map(|order| {
            order
                .iter()
                .filter_map(|offset| schema.get(*offset).copied())
                .collect()
        })
        .collect()
}

fn logical_join_type(join_type: JoinType) -> LogicalJoinType {
    match join_type {
        JoinType::Cross => LogicalJoinType::Inner,
        JoinType::Left => LogicalJoinType::LeftOuter,
        JoinType::Right => LogicalJoinType::RightOuter,
    }
}

struct LiveJoinCostModel {
    hash_concurrency: f64,
}

impl JoinCostModel for LiveJoinCostModel {
    fn attach(
        &self,
        join: &LogicalJoin,
        strategy: &JoinStrategy,
        children: [&Task; 2],
    ) -> Option<tidb_planner::candidate_cost::Candidate> {
        use tidb_planner::candidate_cost::Candidate;
        use tidb_planner::plan_cost_ver2::{HashJoinInput, IndexJoinInput};
        match strategy {
            JoinStrategy::Hash(shape) => {
                let build_at = if shape.use_outer_to_build {
                    1 - shape.inner_idx
                } else {
                    shape.inner_idx
                };
                let probe_at = 1 - build_at;
                Some(Candidate::HashJoin {
                    build: Box::new(children[build_at].plan.clone()),
                    probe: Box::new(children[probe_at].plan.clone()),
                    input: HashJoinInput {
                        build_rows: children[build_at].costed.rows,
                        probe_rows: children[probe_at].costed.rows,
                        build_row_size: children[build_at].costed.row_size,
                        num_build_keys: join.left_keys.len(),
                        num_probe_keys: join.right_keys.len(),
                        tidb_concurrency: self.hash_concurrency,
                    },
                    build_filters: Vec::new(),
                    probe_filters: Vec::new(),
                })
            }
            JoinStrategy::Merge {
                left_keys,
                right_keys,
                ..
            } => Some(Candidate::MergeJoin {
                left: Box::new(children[0].plan.clone()),
                right: Box::new(children[1].plan.clone()),
                child_rows: (children[0].costed.rows, children[1].costed.rows),
                left_conditions: Vec::new(),
                right_conditions: Vec::new(),
                other_conditions: Vec::new(),
                num_join_keys: (left_keys.len(), right_keys.len()),
            }),
            JoinStrategy::Index {
                outer_idx, kind, ..
            } => {
                let probe_idx = 1 - *outer_idx;
                Some(Candidate::IndexJoin {
                    build: Box::new(children[*outer_idx].plan.clone()),
                    probe: Box::new(children[probe_idx].plan.clone()),
                    input: IndexJoinInput {
                        build_rows: children[*outer_idx].costed.rows,
                        build_row_size: children[*outer_idx].costed.row_size,
                        probe_rows_one: children[probe_idx].costed.rows,
                        probe_row_size: children[probe_idx].costed.row_size,
                        num_right_join_keys: join.right_keys.len(),
                        num_left_join_keys: join.left_keys.len(),
                        num_ranges: 0.0,
                        is_semi_join: matches!(
                            join.join_type,
                            LogicalJoinType::Semi
                                | LogicalJoinType::AntiSemi
                                | LogicalJoinType::LeftOuterSemi
                                | LogicalJoinType::AntiLeftOuterSemi
                        ),
                        kind: *kind,
                    },
                    build_filters: Vec::new(),
                    probe_filters: Vec::new(),
                })
            }
        }
    }

    fn enforce(
        &self,
        prop: &PhysicalProperty,
        task: &Task,
    ) -> Option<tidb_planner::candidate_cost::Candidate> {
        Some(tidb_planner::candidate_cost::Candidate::Sort {
            child: Box::new(task.plan.clone()),
            rows: task.costed.rows,
            row_size: tidb_planner::candidate_cost::RowSize::Fixed(task.costed.row_size),
            by_items: vec![false; prop.sort_items.len()],
        })
    }
}

fn collect_decisions(
    node: &JoinNode,
    decision: &DecisionTree,
    guide: &mut RecursiveGuide,
) -> Option<()> {
    let decision = match decision {
        DecisionTree::Sort { child, .. } => child.as_ref(),
        decision => decision,
    };
    match (node, decision) {
        (JoinNode::Join(join), decision) if join.right.is_none() => {
            collect_decisions(&join.left, decision, guide)
        }
        (JoinNode::Join(join), DecisionTree::Join { strategy, children }) => {
            guide.sites.insert(join_key(join)?, strategy.clone());
            collect_decisions(&join.left, &children[0], guide)?;
            collect_decisions(join.right.as_ref()?, &children[1], guide)
        }
        (JoinNode::Table(_) | JoinNode::Derived { .. }, DecisionTree::Leaf) => Some(()),
        _ => None,
    }
}

fn join_key(join: &Join) -> Option<Vec<String>> {
    let (mut left, right) = side_names(join)?;
    left.extend(right);
    left.iter_mut().for_each(|name| name.make_ascii_lowercase());
    left.sort();
    Some(left)
}

// Every site this chooser answered for, in the order it was asked. The
// recorder is the ONLY way to observe the chooser's answer for a statement
// that is not being explained, which is exactly what
// `crate::tests_join_search` has to compare against the explained one.
#[cfg(test)]
thread_local! {
    /// See above.
    pub(crate) static ANSWERS: std::cell::RefCell<Vec<Answer>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// One recorded answer.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Answer {
    /// The relations the left side reads.
    pub(crate) left: Vec<String>,
    /// The relations the right side reads.
    pub(crate) right: Vec<String>,
    /// What the estimate owner said, when it was reached.
    pub(crate) rows: Option<crate::driver::join_reorder::JoinRows>,
    /// Whether the property this site was asked for carries an order.
    pub(crate) ordered: bool,
    /// The answer.
    pub(crate) chosen: Chosen,
}

/// Asks [`exhaust_join`] which strategies this site may use.
pub(crate) fn choose(input: &SearchInput<'_>) -> Chosen {
    #[cfg(test)]
    let mut record = Answer {
        left: Vec::new(),
        right: Vec::new(),
        rows: None,
        ordered: !input.required.is_sort_item_empty(),
        chosen: Chosen::Refused(Refusal::NoEquiKeys),
    };
    #[cfg(not(test))]
    let mut record = ();
    let chosen = decide(input, &mut record);
    #[cfg(test)]
    {
        record.chosen = chosen;
        ANSWERS.with(|answers| answers.borrow_mut().push(record));
    }
    chosen
}

/// Go costs both build-side variants of an inner Cartesian hash join with
/// `getPlanCostVer24PhysicalHashJoin`; equal costs keep the first enumerated
/// candidate, whose build side is the right child.
pub(crate) fn cartesian_build_is_left(
    input: &SearchInput<'_>,
    left_row_size: f64,
    right_row_size: f64,
    concurrency: f64,
) -> bool {
    if !input.keys.is_empty() || input.join_type != LogicalJoinType::Inner {
        return false;
    }
    let (Some(rows), Some((left_names, right_names))) = (input.rows, side_names(input.join)) else {
        return false;
    };
    let Some(counts) = rows.rows_of_join(&left_names, &right_names) else {
        return false;
    };
    let factors = tidb_planner::plan_cost_ver2::Ver2Factors::default();
    let zero = tidb_planner::cost_usage::zero_cost_ver2();
    let cost = |build_rows: f64, probe_rows: f64, build_row_size: f64| {
        tidb_planner::plan_cost_ver2::hash_join_cost(
            None,
            tidb_planner::plan_cost_ver2::HashJoinInput {
                build_rows,
                probe_rows,
                build_row_size,
                num_build_keys: 0,
                num_probe_keys: 0,
                tidb_concurrency: concurrency.max(1.0),
            },
            (&[], &[]),
            (&factors.tidb_cpu, &factors.tidb_mem, 1.0),
            tidb_planner::task_type::TaskType::Root,
            (&zero, &zero),
        )
        .value()
    };
    cost(counts.left, counts.right, left_row_size) < cost(counts.right, counts.left, right_row_size)
}

#[cfg(test)]
type Record = Answer;
#[cfg(not(test))]
type Record = ();

fn decide(input: &SearchInput<'_>, record: &mut Record) -> Chosen {
    let _ = &record;
    if input.keys.is_empty() {
        return Chosen::Refused(Refusal::NoEquiKeys);
    }
    // A site the estimator cannot answer for is a site the search cannot
    // price. Fail closed rather than decide it structurally.
    let Some(rows) = input.rows else {
        return Chosen::Refused(Refusal::NoRowSource);
    };
    let Some((left_names, right_names)) = side_names(input.join) else {
        return Chosen::Refused(Refusal::NoRelationNames);
    };
    let Some(counts) = rows.rows_of_join(&left_names, &right_names) else {
        return Chosen::Refused(Refusal::RowsDeclined);
    };
    #[cfg(test)]
    {
        record.left = left_names;
        record.right = right_names;
        record.rows = Some(counts);
    }
    #[cfg(not(test))]
    let _ = counts;
    let Some(orders) = input.orders else {
        return Chosen::Refused(Refusal::NoChildOrders);
    };
    let candidates = exhaust_join(&logical_join(input, orders), input.required);
    let mut index = false;
    let mut hash = false;
    let mut merge = false;
    for candidate in &candidates {
        match candidate.strategy {
            JoinStrategy::Index { .. } => index = true,
            JoinStrategy::Hash(_) => hash = true,
            JoinStrategy::Merge { .. } => merge = true,
        }
    }
    match (index, hash, merge) {
        (true, false, false) => Chosen::Index,
        (true, true, _) => Chosen::Refused(Refusal::HashAlsoEnumerated),
        (true, false, true) => Chosen::Refused(Refusal::MergeAlsoEnumerated),
        (false, ..) => Chosen::Refused(Refusal::NoIndexCandidate),
    }
}

/// The `LogicalJoin` the enumeration reads, in joined-row-offset identity.
///
/// The two children are `Leaf`s with no alternatives: `exhaust_join` never
/// descends, so what a child could be planned as is not its input. Only
/// [`tidb_planner::find_best_task::find_best_task`] reads the alternatives,
/// and reaching it is this module's named residue.
fn logical_join(input: &SearchInput<'_>, orders: (&[Vec<usize>], &[Vec<usize>])) -> LogicalJoin {
    let shift = input.left_width as i64;
    LogicalJoin {
        join_type: input.join_type,
        left: Box::new(LogicalNode::Leaf(Vec::new())),
        right: Box::new(LogicalNode::Leaf(Vec::new())),
        left_keys: input.keys.iter().map(|key| key.left as i64).collect(),
        right_keys: input
            .keys
            .iter()
            .map(|key| key.right as i64 + shift)
            .collect(),
        left_schema: (0..shift).collect(),
        right_schema: (shift..input.width as i64).collect(),
        left_properties: orders
            .0
            .iter()
            .map(|order| order.iter().map(|at| *at as i64).collect())
            .collect(),
        right_properties: orders
            .1
            .iter()
            .map(|order| order.iter().map(|at| *at as i64 + shift).collect())
            .collect(),
        force_merge: false,
        output_rows: None,
    }
}

/// The relations each side of `join` reads, by the name a column reference
/// reaches them under -- the key [`RowSource`] answers to.
fn side_names(join: &Join) -> Option<(Vec<String>, Vec<String>)> {
    let mut left = Vec::new();
    let mut right = Vec::new();
    leaf_names(&join.left, &mut left)?;
    leaf_names(join.right.as_ref()?, &mut right)?;
    (!left.is_empty() && !right.is_empty()).then_some((left, right))
}

/// Every relation a `FROM` subtree exposes, in row order.
fn leaf_names(node: &JoinNode, out: &mut Vec<String>) -> Option<()> {
    match node {
        JoinNode::Table(table_ref) => {
            let name = table_ref
                .alias
                .clone()
                .or_else(|| table_ref.name.last().cloned())?;
            out.push(name);
            Some(())
        }
        JoinNode::Derived { alias, .. } => {
            out.push(alias.clone().filter(|alias| !alias.is_empty())?);
            Some(())
        }
        JoinNode::Join(inner) => {
            leaf_names(&inner.left, out)?;
            match &inner.right {
                Some(right) => leaf_names(right, out),
                None => Some(()),
            }
        }
    }
}
