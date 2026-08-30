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

//! Pinned Go `pkg/planner/core/joinorder`.
//!
//! This module owns the shared behavior used by ordinary and order-aware join
//! reorder. It is intentionally not registered as an optimizer rule until the
//! complete pinned package is present.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use tidb_datatype::FieldTypeFlags;
use tidb_expr::column::Column;
use tidb_expr::expr_util::normal_form::expr_from_schema;
use tidb_expr::expr_util::predicates::is_mutable_effects_expr;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::{merge_schema, Schema};
use tidb_expr::simple_expr::extract_columns;

use crate::logical::join::LogicalJoin;
use crate::logical::projection::LogicalProjection;
use crate::logical::rule::RuleContext;
use crate::logical::selection::LogicalSelection;
use crate::logical::{BaseLogicalPlan, LogicalPlan};
use crate::plan_base::PlanError;
use tidb_util::intset::FastIntSet;

/// Go `joinorder.Node`.
#[derive(Clone, Debug, Default)]
pub struct Node {
    bit_set: FastIntSet,
    plan: Option<Rc<LogicalPlan>>,
    cumulative_cost: f64,
    used_edges: BTreeSet<u64>,
}

#[derive(Clone, Debug)]
struct ConflictRule {
    from: FastIntSet,
    to: FastIntSet,
}

#[derive(Clone, Debug)]
struct Edge {
    index: u64,
    join_type: crate::find_best_task::LogicalJoinType,
    equal_conditions: Vec<ScalarFunction>,
    non_equal_conditions: Vec<Expression>,
    total_eligibility_set: FastIntSet,
    rules: Vec<ConflictRule>,
    skip_rules: bool,
    left_vertexes: FastIntSet,
    right_vertexes: FastIntSet,
}

#[derive(Clone, Debug)]
struct JoinGroup {
    root: Rc<LogicalPlan>,
    vertexes: Vec<Rc<LogicalPlan>>,
    leading_hints: Vec<Rc<crate::plan_builder::from::JoinHints>>,
    has_user_leading_hint: bool,
    vertex_hints: BTreeMap<i32, JoinMethodHint>,
    all_inner_join: bool,
    selection_conditions: BTreeMap<i32, Vec<Expression>>,
}

#[derive(Clone, Debug)]
pub(crate) struct JoinMethodHint {
    pub(crate) prefer_join_method: u32,
    pub(crate) hint_info: Rc<crate::plan_builder::from::JoinHints>,
}

impl JoinGroup {
    fn merge(&mut self, mut other: Self) {
        self.vertexes.append(&mut other.vertexes);
        self.leading_hints.append(&mut other.leading_hints);
        self.has_user_leading_hint |= other.has_user_leading_hint;
        self.vertex_hints.append(&mut other.vertex_hints);
        self.all_inner_join &= other.all_inner_join;
        self.selection_conditions
            .append(&mut other.selection_conditions);
    }
}

fn make_single_group(plan: Rc<LogicalPlan>) -> JoinGroup {
    JoinGroup {
        root: Rc::clone(&plan),
        vertexes: vec![plan],
        leading_hints: Vec::new(),
        has_user_leading_hint: false,
        vertex_hints: BTreeMap::new(),
        all_inner_join: true,
        selection_conditions: BTreeMap::new(),
    }
}

fn extract_join_group(context: &RuleContext<'_>, plan: Rc<LogicalPlan>) -> JoinGroup {
    if let LogicalPlan::Selection(selection) = plan.as_ref() {
        if context.join_reorder_through_sel
            && !selection.conditions.iter().any(is_mutable_effects_expr)
        {
            if let [child] = selection.base.children() {
                let mut child_group = extract_join_group(context, Rc::new(child.clone()));
                if child_group.vertexes.len() > 1 {
                    child_group
                        .selection_conditions
                        .insert(plan.id(), selection.conditions.clone());
                    child_group.root = plan;
                    return child_group;
                }
            }
        }
        return make_single_group(plan);
    }

    let LogicalPlan::Join(join) = plan.as_ref() else {
        return make_single_group(plan);
    };
    let current_leading_hint = if join.prefer_join_order {
        join.hint_info.clone().map(|hint| (hint, true))
    } else if join.internal_prefer_join_order {
        join.internal_hint_info.clone().map(|hint| (hint, false))
    } else {
        None
    };

    let reorderable = !join.straight_join
        && matches!(
            join.join_type,
            crate::find_best_task::LogicalJoinType::Inner
                | crate::find_best_task::LogicalJoinType::LeftOuter
                | crate::find_best_task::LogicalJoinType::RightOuter
        )
        && (context.outer_join_reorder
            || !matches!(
                join.join_type,
                crate::find_best_task::LogicalJoinType::LeftOuter
                    | crate::find_best_task::LogicalJoinType::RightOuter
            ))
        && (context.advanced_join_hint || join.prefer_join_type == 0)
        && !join
            .equal_conditions
            .iter()
            .any(|condition| condition.func_name.lowercase() == "nulleq")
        && (join.join_type == crate::find_best_task::LogicalJoinType::Inner
            || !join.equal_conditions.is_empty());

    if !reorderable {
        let mut group = make_single_group(plan);
        if let Some((hint, from_user)) = current_leading_hint {
            group.leading_hints.push(hint);
            group.has_user_leading_hint |= from_user;
        }
        return group;
    }

    let [left, right] = join.base.children() else {
        return make_single_group(plan);
    };
    let mut group = JoinGroup {
        root: Rc::clone(&plan),
        vertexes: Vec::new(),
        leading_hints: Vec::new(),
        has_user_leading_hint: false,
        vertex_hints: BTreeMap::new(),
        all_inner_join: join.join_type == crate::find_best_task::LogicalJoinType::Inner,
        selection_conditions: BTreeMap::new(),
    };

    let mut left_has_hint = false;
    let mut right_has_hint = false;
    if context.advanced_join_hint && join.prefer_join_type != 0 {
        if let Some(hint_info) = join.hint_info.as_ref() {
            if join.left_prefer_join_type != 0 {
                group.vertex_hints.insert(
                    left.id(),
                    JoinMethodHint {
                        prefer_join_method: join.left_prefer_join_type,
                        hint_info: Rc::clone(hint_info),
                    },
                );
                left_has_hint = true;
            }
            if join.right_prefer_join_type != 0 {
                group.vertex_hints.insert(
                    right.id(),
                    JoinMethodHint {
                        prefer_join_method: join.right_prefer_join_type,
                        hint_info: Rc::clone(hint_info),
                    },
                );
                right_has_hint = true;
            }
        }
    }

    let left_should_preserve = current_leading_hint
        .as_ref()
        .is_some_and(|(hint, _)| is_derived_table_in_leading_hint(left, hint));
    group.merge(if left_has_hint || left_should_preserve {
        make_single_group(Rc::new(left.clone()))
    } else {
        extract_join_group(context, Rc::new(left.clone()))
    });
    let right_should_preserve = current_leading_hint
        .as_ref()
        .is_some_and(|(hint, _)| is_derived_table_in_leading_hint(right, hint));
    group.merge(if right_has_hint || right_should_preserve {
        make_single_group(Rc::new(right.clone()))
    } else {
        extract_join_group(context, Rc::new(right.clone()))
    });
    if let Some((hint, from_user)) = current_leading_hint {
        group.leading_hints.push(hint);
        group.has_user_leading_hint |= from_user;
    }
    group
}

fn is_derived_table_in_leading_hint(
    plan: &LogicalPlan,
    hint: &crate::plan_builder::from::JoinHints,
) -> bool {
    if plan.query_block_offset() <= 1 {
        return false;
    }
    let Some(alias) = crate::plan_builder::from::extract_table_alias(plan.output_names()) else {
        return false;
    };
    fn contains(elements: &[tidb_ast::LeadingElement], database: &str, table_name: &str) -> bool {
        elements.iter().any(|element| match element {
            tidb_ast::LeadingElement::Table(table) => {
                table.name.eq_ignore_ascii_case(table_name)
                    && table.db_name.as_deref().is_none_or(|hint_database| {
                        hint_database == "*" || hint_database.eq_ignore_ascii_case(database)
                    })
            }
            tidb_ast::LeadingElement::Group(group) => contains(group, database, table_name),
        })
    }
    hint.leading
        .as_deref()
        .is_some_and(|elements| contains(elements, &alias.db_name, &alias.table_name))
}

/// Go `joinorder.ConflictDetector`, the CD-C join-legality graph.
#[derive(Clone, Debug, Default)]
pub struct ConflictDetector {
    group_vertexes: Vec<Node>,
    inner_edges: Vec<Edge>,
    non_inner_edges: Vec<Edge>,
    all_inner_join: bool,
    derive_stats_threshold: i32,
}

/// Go `joinorder.CheckConnectionResult`.
#[derive(Clone, Debug)]
pub struct CheckConnectionResult {
    node1: Node,
    node2: Node,
    applied_inner_edges: Vec<Edge>,
    applied_non_inner_edge: Option<Edge>,
    has_equal_condition: bool,
}

impl CheckConnectionResult {
    /// Go `Connected`.
    #[must_use]
    pub fn connected(&self) -> bool {
        !self.applied_inner_edges.is_empty() || self.applied_non_inner_edge.is_some()
    }

    /// Go `NoEQEdge`.
    #[must_use]
    pub fn no_equal_edge(&self) -> bool {
        !self.has_equal_condition
    }
}

impl Edge {
    fn check_rules(&self, node1: &Node, node2: &Node) -> bool {
        let candidate = node1.bit_set.union(&node2.bit_set);
        self.rules
            .iter()
            .all(|rule| !rule.from.intersects(&candidate) || rule.to.subset_of(&candidate))
    }

    fn inner_applicable(&self, node1: &Node, node2: &Node) -> bool {
        if !self.skip_rules && !self.check_rules(node1, node2) {
            return false;
        }
        let candidate = node1.bit_set.union(&node2.bit_set);
        self.total_eligibility_set.subset_of(&candidate)
            && self.total_eligibility_set.intersects(&node1.bit_set)
            && self.total_eligibility_set.intersects(&node2.bit_set)
    }

    fn non_inner_applicable(&self, node1: &Node, node2: &Node) -> bool {
        if !self.skip_rules && !self.check_rules(node1, node2) {
            return false;
        }
        self.left_vertexes
            .intersection(&self.total_eligibility_set)
            .subset_of(&node1.bit_set)
            && self
                .right_vertexes
                .intersection(&self.total_eligibility_set)
                .subset_of(&node2.bit_set)
            && self.total_eligibility_set.intersects(&node1.bit_set)
            && self.total_eligibility_set.intersects(&node2.bit_set)
    }
}

impl ConflictDetector {
    fn build(&mut self, group: &JoinGroup) -> Result<Vec<Node>, PlanError> {
        self.all_inner_join = group.all_inner_join;
        self.group_vertexes.clear();
        self.inner_edges.clear();
        self.non_inner_edges.clear();

        let mut vertex_map = BTreeMap::new();
        for (index, vertex) in group.vertexes.iter().enumerate() {
            // Go `ConflictDetector.Build` derives every leaf vertex before
            // reading its cumulative cost. The logical-rule phase runs
            // before the later whole-plan physical-optimization derivation.
            let (vertex, result) = crate::logical::rewrite::recursive_derive_stats(
                vertex.as_ref().clone(),
                Vec::new(),
                self.derive_stats_threshold,
            );
            result?;
            let vertex = Rc::new(vertex);
            let cumulative_cost = cumulative_cost_by_children(&vertex)?;
            validate_cumulative_cost(cumulative_cost)?;
            vertex_map.insert(
                vertex.id(),
                Node {
                    bit_set: FastIntSet::new([index as i64]),
                    plan: Some(vertex),
                    cumulative_cost,
                    used_edges: BTreeSet::new(),
                },
            );
        }
        self.build_recursive(group, &group.root, &vertex_map)?;
        Ok(self.group_vertexes.clone())
    }

    fn build_recursive(
        &mut self,
        group: &JoinGroup,
        plan: &LogicalPlan,
        vertex_map: &BTreeMap<i32, Node>,
    ) -> Result<(Vec<Edge>, FastIntSet), PlanError> {
        if let Some(vertex) = vertex_map.get(&plan.id()) {
            self.group_vertexes.push(vertex.clone());
            return Ok((Vec::new(), vertex.bit_set.copy()));
        }

        if let LogicalPlan::Selection(selection) = plan {
            let [child] = selection.base.children() else {
                return Err(PlanError::internal(
                    "unexpected Selection child count in conflict detector",
                ));
            };
            let (mut child_edges, child_vertexes) =
                self.build_recursive(group, child, vertex_map)?;
            let conditions = group
                .selection_conditions
                .get(&plan.id())
                .cloned()
                .ok_or_else(|| {
                    PlanError::internal(format!(
                        "unexpected Selection node (ID: {}) found in buildRecursive",
                        plan.id()
                    ))
                })?;
            let mut edge = self.make_edge_internal(
                crate::find_best_task::LogicalJoinType::Inner,
                FastIntSet::default(),
                FastIntSet::default(),
                &[],
                &[],
                child_vertexes.copy(),
            );
            edge.non_equal_conditions = conditions;
            self.replace_stored_edge(&edge);
            child_edges.push(edge);
            return Ok((child_edges, child_vertexes));
        }

        let LogicalPlan::Join(join) = plan else {
            return Err(PlanError::internal(
                "unexpected plan type in conflict detector",
            ));
        };
        let [left, right] = join.base.children() else {
            return Err(PlanError::internal(
                "unexpected LogicalJoin child count in conflict detector",
            ));
        };
        let (left_edges, left_vertexes) = self.build_recursive(group, left, vertex_map)?;
        let (right_edges, right_vertexes) = self.build_recursive(group, right, vertex_map)?;
        if left_vertexes.intersects(&right_vertexes) {
            return Err(PlanError::internal("conflicting join edges detected"));
        }
        let current_edges = if join.join_type == crate::find_best_task::LogicalJoinType::Inner {
            self.make_inner_edges(
                join,
                left_vertexes.copy(),
                right_vertexes.copy(),
                &left_edges,
                &right_edges,
            )?
        } else {
            vec![self.make_non_inner_edge(
                join,
                left_vertexes.copy(),
                right_vertexes.copy(),
                &left_edges,
                &right_edges,
            )?]
        };
        let mut edges = left_edges;
        edges.extend(right_edges);
        edges.extend(current_edges);
        Ok((edges, left_vertexes.union(&right_vertexes)))
    }

    fn make_inner_edges(
        &mut self,
        join: &LogicalJoin,
        left_vertexes: FastIntSet,
        right_vertexes: FastIntSet,
        left_edges: &[Edge],
        right_edges: &[Edge],
    ) -> Result<Vec<Edge>, PlanError> {
        if !join.na_eq_conditions.is_empty() {
            return Err(PlanError::internal(
                "NAEQConditions not supported in conflict detector yet",
            ));
        }
        let non_equal_conditions: Vec<Expression> = join
            .other_conditions
            .iter()
            .chain(&join.left_conditions)
            .chain(&join.right_conditions)
            .cloned()
            .collect();
        let mut result = Vec::new();
        if join.equal_conditions.is_empty() && non_equal_conditions.is_empty() {
            result.push(self.make_edge(
                crate::find_best_task::LogicalJoinType::Inner,
                &[],
                left_vertexes.copy(),
                right_vertexes.copy(),
                left_edges,
                right_edges,
            ));
        }
        for condition in &join.equal_conditions {
            let expression = Expression::ScalarFunction(condition.clone());
            let mut edge = self.make_edge(
                crate::find_best_task::LogicalJoinType::Inner,
                std::slice::from_ref(&expression),
                left_vertexes.copy(),
                right_vertexes.copy(),
                left_edges,
                right_edges,
            );
            edge.equal_conditions.push(condition.clone());
            self.replace_stored_edge(&edge);
            result.push(edge);
        }
        for condition in non_equal_conditions {
            let mut edge = self.make_edge(
                crate::find_best_task::LogicalJoinType::Inner,
                std::slice::from_ref(&condition),
                left_vertexes.copy(),
                right_vertexes.copy(),
                left_edges,
                right_edges,
            );
            edge.non_equal_conditions.push(condition);
            self.replace_stored_edge(&edge);
            result.push(edge);
        }
        Ok(result)
    }

    fn make_non_inner_edge(
        &mut self,
        join: &LogicalJoin,
        left_vertexes: FastIntSet,
        right_vertexes: FastIntSet,
        left_edges: &[Edge],
        right_edges: &[Edge],
    ) -> Result<Edge, PlanError> {
        if !join.na_eq_conditions.is_empty() {
            return Err(PlanError::internal(
                "NAEQConditions not supported in conflict detector yet",
            ));
        }
        let non_equal_conditions: Vec<Expression> = join
            .left_conditions
            .iter()
            .chain(&join.right_conditions)
            .chain(&join.other_conditions)
            .cloned()
            .collect();
        let mut conditions: Vec<Expression> = join
            .equal_conditions
            .iter()
            .cloned()
            .map(Expression::ScalarFunction)
            .collect();
        conditions.extend(non_equal_conditions.iter().cloned());
        let mut edge = self.make_edge(
            join.join_type,
            &conditions,
            left_vertexes,
            right_vertexes,
            left_edges,
            right_edges,
        );
        edge.equal_conditions = join.equal_conditions.clone();
        edge.non_equal_conditions = non_equal_conditions;
        self.replace_stored_edge(&edge);
        Ok(edge)
    }

    fn make_edge(
        &mut self,
        join_type: crate::find_best_task::LogicalJoinType,
        conditions: &[Expression],
        left_vertexes: FastIntSet,
        right_vertexes: FastIntSet,
        left_edges: &[Edge],
        right_edges: &[Edge],
    ) -> Edge {
        let eligibility = self.calculate_syntactic_eligibility_set(conditions);
        self.make_edge_internal(
            join_type,
            left_vertexes,
            right_vertexes,
            left_edges,
            right_edges,
            eligibility,
        )
    }

    fn make_edge_internal(
        &mut self,
        join_type: crate::find_best_task::LogicalJoinType,
        left_vertexes: FastIntSet,
        right_vertexes: FastIntSet,
        left_edges: &[Edge],
        right_edges: &[Edge],
        mut eligibility: FastIntSet,
    ) -> Edge {
        let index = (self.inner_edges.len() + self.non_inner_edges.len()) as u64;
        if !eligibility.intersects(&left_vertexes) {
            eligibility = eligibility.union(&left_vertexes);
        }
        if !eligibility.intersects(&right_vertexes) {
            eligibility = eligibility.union(&right_vertexes);
        }
        let mut edge = Edge {
            index,
            join_type,
            equal_conditions: Vec::new(),
            non_equal_conditions: Vec::new(),
            total_eligibility_set: eligibility,
            rules: Vec::new(),
            skip_rules: self.all_inner_join,
            left_vertexes,
            right_vertexes,
        };
        if !self.all_inner_join {
            for child in left_edges {
                if !associative(child, &edge) {
                    edge.rules.push(right_to_left_rule(child));
                }
                if !left_asscom(child, &edge) {
                    edge.rules.push(left_to_right_rule(child));
                }
            }
            for child in right_edges {
                if !associative(&edge, child) {
                    edge.rules.push(left_to_right_rule(child));
                }
                if !right_asscom(&edge, child) {
                    edge.rules.push(right_to_left_rule(child));
                }
            }
        }
        if join_type == crate::find_best_task::LogicalJoinType::Inner {
            self.inner_edges.push(edge.clone());
        } else {
            self.non_inner_edges.push(edge.clone());
        }
        edge
    }

    fn replace_stored_edge(&mut self, edge: &Edge) {
        let stored = if edge.join_type == crate::find_best_task::LogicalJoinType::Inner {
            &mut self.inner_edges
        } else {
            &mut self.non_inner_edges
        };
        if let Some(slot) = stored.iter_mut().find(|one| one.index == edge.index) {
            *slot = edge.clone();
        }
    }

    fn calculate_syntactic_eligibility_set(&self, conditions: &[Expression]) -> FastIntSet {
        let mut result = FastIntSet::default();
        for condition in conditions {
            let columns = extract_columns(condition);
            for node in &self.group_vertexes {
                let Some(plan) = node.plan.as_ref() else {
                    continue;
                };
                let Some(schema) = plan.schema() else {
                    continue;
                };
                if columns.iter().any(|column| schema.contains(column)) {
                    result = result.union(&node.bit_set);
                }
            }
        }
        result
    }

    /// Go `MakeJoin`.
    fn make_join(
        &self,
        context: &RuleContext<'_>,
        mut check_result: CheckConnectionResult,
        vertex_hints: &BTreeMap<i32, JoinMethodHint>,
        join_reorder_threshold: i32,
    ) -> Result<Node, PlanError> {
        let mut existing_non_inner = if check_result.applied_non_inner_edge.is_some() {
            Some(make_non_inner_join(
                context,
                &mut check_result,
                vertex_hints,
            )?)
        } else {
            None
        };
        let plan = if !check_result.applied_inner_edges.is_empty() {
            make_inner_join(
                context,
                &mut check_result,
                existing_non_inner.take(),
                vertex_hints,
            )?
        } else {
            LogicalPlan::Join(
                existing_non_inner
                    .ok_or_else(|| PlanError::internal("failed to make join plan"))?,
            )
        };
        let (plan, stats_result) = crate::logical::rewrite::recursive_derive_stats(
            plan,
            Vec::new(),
            join_reorder_threshold,
        );
        let (stats, _) = stats_result?;
        let cumulative_cost = stats.row_count()
            + check_result.node1.cumulative_cost
            + check_result.node2.cumulative_cost;
        validate_cumulative_cost(cumulative_cost)?;

        let mut used_edges = check_result.node1.used_edges.clone();
        used_edges.extend(check_result.node2.used_edges.iter().copied());
        used_edges.extend(
            check_result
                .applied_inner_edges
                .iter()
                .map(|edge| edge.index),
        );
        if let Some(edge) = &check_result.applied_non_inner_edge {
            used_edges.insert(edge.index);
        }
        Ok(Node {
            bit_set: check_result
                .node1
                .bit_set
                .union(&check_result.node2.bit_set),
            plan: Some(Rc::new(plan)),
            cumulative_cost,
            used_edges,
        })
    }

    /// Go `CheckConnection`.
    pub fn check_connection(
        &self,
        node1: &Node,
        node2: &Node,
    ) -> Result<CheckConnectionResult, PlanError> {
        let mut result = CheckConnectionResult {
            node1: node1.clone(),
            node2: node2.clone(),
            applied_inner_edges: Vec::new(),
            applied_non_inner_edge: None,
            has_equal_condition: false,
        };
        for edge in &self.inner_edges {
            if node1.used_edges.contains(&edge.index) || node2.used_edges.contains(&edge.index) {
                continue;
            }
            if edge.inner_applicable(node1, node2) {
                result.has_equal_condition |= !edge.equal_conditions.is_empty();
                result.applied_inner_edges.push(edge.clone());
            }
        }

        let mut swap_nodes = false;
        for edge in &self.non_inner_edges {
            if node1.used_edges.contains(&edge.index) || node2.used_edges.contains(&edge.index) {
                continue;
            }
            let forward = edge.non_inner_applicable(node1, node2);
            let reverse = edge.non_inner_applicable(node2, node1);
            if forward && reverse {
                return Err(PlanError::internal(
                    "node1 and node2 cannot be connected by non-inner edges of different direction",
                ));
            }
            if forward || reverse {
                if result.applied_non_inner_edge.is_some() {
                    return Err(PlanError::internal(
                        "multiple non-inner edges applied between two nodes",
                    ));
                }
                result.has_equal_condition |= !edge.equal_conditions.is_empty();
                result.applied_non_inner_edge = Some(edge.clone());
                swap_nodes = reverse;
            }
        }
        if swap_nodes {
            std::mem::swap(&mut result.node1, &mut result.node2);
        }
        Ok(result)
    }

    /// Go `TryCreateCartesianCheckResult`.
    #[must_use]
    pub fn try_create_cartesian_result(
        &mut self,
        left: &Node,
        right: &Node,
    ) -> Option<CheckConnectionResult> {
        self.all_inner_join.then(|| {
            let cartesian_edge = self.make_edge(
                crate::find_best_task::LogicalJoinType::Inner,
                &[],
                left.bit_set.copy(),
                right.bit_set.copy(),
                &[],
                &[],
            );
            CheckConnectionResult {
                node1: left.clone(),
                node2: right.clone(),
                applied_inner_edges: vec![cartesian_edge],
                applied_non_inner_edge: None,
                has_equal_condition: false,
            }
        })
    }

    /// Go `HasRemainingEdges`.
    #[must_use]
    pub fn has_remaining_edges(&self, used_edges: &BTreeSet<u64>) -> bool {
        self.inner_edges
            .iter()
            .chain(&self.non_inner_edges)
            .any(|edge| {
                (!edge.equal_conditions.is_empty() || !edge.non_equal_conditions.is_empty())
                    && !used_edges.contains(&edge.index)
            })
    }

    /// Go `HasRemainingEdgesInSubset`.
    #[must_use]
    pub fn has_remaining_edges_in_subset(
        &self,
        subset: &FastIntSet,
        used_edges: &BTreeSet<u64>,
    ) -> bool {
        self.inner_edges
            .iter()
            .chain(&self.non_inner_edges)
            .any(|edge| {
                (!edge.equal_conditions.is_empty() || !edge.non_equal_conditions.is_empty())
                    && !used_edges.contains(&edge.index)
                    && edge.total_eligibility_set.subset_of(subset)
                    && edge
                        .left_vertexes
                        .union(&edge.right_vertexes)
                        .subset_of(subset)
            })
    }
}

fn node_plan(node: &Node) -> Result<LogicalPlan, PlanError> {
    node.plan
        .as_ref()
        .map(|plan| plan.as_ref().clone())
        .ok_or_else(|| PlanError::internal("join reorder node has no logical plan"))
}

fn align_equal_conditions(
    context: &RuleContext<'_>,
    mut left: LogicalPlan,
    mut right: LogicalPlan,
    conditions: &[ScalarFunction],
) -> Result<(LogicalPlan, LogicalPlan, Vec<ScalarFunction>), PlanError> {
    let mut aligned = Vec::with_capacity(conditions.len());
    for condition in conditions {
        let [first, second] = condition.get_args() else {
            return Err(PlanError::internal(format!(
                "unexpected eq condition args: {}",
                condition.get_args().len()
            )));
        };
        let left_schema = left
            .schema()
            .ok_or_else(|| PlanError::internal("join left child has no schema"))?;
        let right_schema = right
            .schema()
            .ok_or_else(|| PlanError::internal("join right child has no schema"))?;
        if expr_from_schema(first, left_schema) && expr_from_schema(second, right_schema) {
            aligned.push(condition.clone());
            continue;
        }
        if expr_from_schema(second, left_schema) && expr_from_schema(first, right_schema) {
            let mut left_argument = second.clone();
            let mut right_argument = first.clone();
            if !matches!(left_argument, Expression::Column(_)) {
                let (new_left, column) =
                    crate::logical::rewrite::inject_join_expression(context, left, left_argument)?;
                left = new_left;
                left_argument = Expression::Column(column);
            }
            if !matches!(right_argument, Expression::Column(_)) {
                let (new_right, column) = crate::logical::rewrite::inject_join_expression(
                    context,
                    right,
                    right_argument,
                )?;
                right = new_right;
                right_argument = Expression::Column(column);
            }
            let mut swapped = condition.clone();
            swapped.args = vec![left_argument, right_argument];
            swapped.invalidate_cached_arguments();
            aligned.push(swapped);
            continue;
        }
        return Err(PlanError::internal(
            "eq condition does not match join sides",
        ));
    }
    Ok((left, right, aligned))
}

pub(crate) fn new_cartesian_join(
    context: &RuleContext<'_>,
    join_type: crate::find_best_task::LogicalJoinType,
    left: LogicalPlan,
    right: LogicalPlan,
) -> Result<LogicalJoin, PlanError> {
    let left_schema = left
        .schema()
        .cloned()
        .ok_or_else(|| PlanError::internal("join left child has no schema"))?;
    let right_schema = right
        .schema()
        .cloned()
        .ok_or_else(|| PlanError::internal("join right child has no schema"))?;
    let left_names = left.output_names().to_vec();
    let right_names = right.output_names().to_vec();
    let offset = if left.query_block_offset() == right.query_block_offset() {
        left.query_block_offset()
    } else {
        -1
    };
    let mut schema = match join_type {
        crate::find_best_task::LogicalJoinType::Semi
        | crate::find_best_task::LogicalJoinType::AntiSemi => left_schema.clone(),
        _ => merge_schema(Some(&left_schema), Some(&right_schema)).unwrap_or_default(),
    };
    match join_type {
        crate::find_best_task::LogicalJoinType::LeftOuter => {
            let len = schema.columns.len();
            crate::plan_builder::from::reset_not_null_flag(
                &mut schema,
                left_schema.columns.len(),
                len,
            );
        }
        crate::find_best_task::LogicalJoinType::RightOuter => {
            crate::plan_builder::from::reset_not_null_flag(
                &mut schema,
                0,
                left_schema.columns.len(),
            );
        }
        _ => {}
    }
    let mut base = BaseLogicalPlan::new(context.allocator, LogicalJoin::TYPE, offset);
    base.base.set_schema(Some(schema));
    base.base.set_output_names(
        left_names
            .into_iter()
            .chain(right_names)
            .collect::<Vec<_>>(),
    );
    base.set_children(vec![left, right]);
    let mut join = LogicalJoin::new(base, join_type);
    join.reordered = true;
    Ok(join)
}

fn make_non_inner_join(
    context: &RuleContext<'_>,
    check_result: &mut CheckConnectionResult,
    vertex_hints: &BTreeMap<i32, JoinMethodHint>,
) -> Result<LogicalJoin, PlanError> {
    let edge = check_result
        .applied_non_inner_edge
        .as_ref()
        .cloned()
        .ok_or_else(|| PlanError::internal("missing non-inner edge"))?;
    let (left, right, equal_conditions) = align_equal_conditions(
        context,
        node_plan(&check_result.node1)?,
        node_plan(&check_result.node2)?,
        &edge.equal_conditions,
    )?;
    check_result.node1.plan = Some(Rc::new(left.clone()));
    check_result.node2.plan = Some(Rc::new(right.clone()));
    let mut join = new_cartesian_join(context, edge.join_type, left, right)?;
    set_new_join_with_hint(&mut join, vertex_hints);
    let join_schema = join
        .base
        .base
        .schema()
        .cloned()
        .ok_or_else(|| PlanError::internal("new join has no schema"))?;
    join.equal_conditions = equal_conditions
        .into_iter()
        .map(|condition| {
            let Expression::ScalarFunction(condition) =
                align_not_null_with_schema(Expression::ScalarFunction(condition), &join_schema)
            else {
                unreachable!()
            };
            condition
        })
        .collect();
    let left_schema = join.base.children()[0]
        .schema()
        .cloned()
        .unwrap_or_default();
    let right_schema = join.base.children()[1]
        .schema()
        .cloned()
        .unwrap_or_default();
    for condition in edge.non_equal_conditions {
        let condition = align_not_null_with_schema(condition, &join_schema);
        if is_mutable_effects_expr(&condition) {
            join.other_conditions.push(condition);
            continue;
        }
        let from_left = expr_from_schema(&condition, &left_schema);
        let from_right = expr_from_schema(&condition, &right_schema);
        match (from_left, from_right) {
            (true, false) => join.left_conditions.push(condition),
            (false, true) => join.right_conditions.push(condition),
            _ => join.other_conditions.push(condition),
        }
    }
    Ok(join)
}

fn make_inner_join(
    context: &RuleContext<'_>,
    check_result: &mut CheckConnectionResult,
    existing_join: Option<LogicalJoin>,
    vertex_hints: &BTreeMap<i32, JoinMethodHint>,
) -> Result<LogicalPlan, PlanError> {
    if let Some(existing_join) = existing_join {
        let conditions = check_result
            .applied_inner_edges
            .iter()
            .flat_map(|edge| {
                edge.equal_conditions
                    .iter()
                    .cloned()
                    .map(Expression::ScalarFunction)
                    .chain(edge.non_equal_conditions.iter().cloned())
            })
            .collect();
        let offset = existing_join.base.base.query_block_offset();
        let mut selection = LogicalSelection::new(
            BaseLogicalPlan::new(context.allocator, LogicalSelection::TYPE, offset),
            conditions,
        );
        selection
            .base
            .set_children(vec![LogicalPlan::Join(existing_join)]);
        return Ok(LogicalPlan::Selection(selection));
    }

    let mut left = node_plan(&check_result.node1)?;
    let mut right = node_plan(&check_result.node2)?;
    let mut equal_conditions = Vec::new();
    let mut other_conditions = Vec::new();
    for edge in &check_result.applied_inner_edges {
        let (new_left, new_right, aligned) =
            align_equal_conditions(context, left, right, &edge.equal_conditions)?;
        left = new_left;
        right = new_right;
        equal_conditions.extend(aligned);
        other_conditions.extend(edge.non_equal_conditions.iter().cloned());
    }
    check_result.node1.plan = Some(Rc::new(left.clone()));
    check_result.node2.plan = Some(Rc::new(right.clone()));
    let join_type = check_result
        .applied_inner_edges
        .first()
        .map(|edge| edge.join_type)
        .ok_or_else(|| PlanError::internal("missing inner edge"))?;
    let mut join = new_cartesian_join(context, join_type, left, right)?;
    set_new_join_with_hint(&mut join, vertex_hints);
    join.equal_conditions = equal_conditions;
    join.other_conditions = other_conditions;
    Ok(LogicalPlan::Join(join))
}

pub(crate) fn set_new_join_with_hint(
    join: &mut LogicalJoin,
    vertex_hints: &BTreeMap<i32, JoinMethodHint>,
) {
    let [left, right] = join.base.children() else {
        return;
    };
    if let Some(hint) = vertex_hints.get(&left.id()) {
        join.left_prefer_join_type = hint.prefer_join_method;
        join.hint_info = Some(Rc::clone(&hint.hint_info));
    }
    if let Some(hint) = vertex_hints.get(&right.id()) {
        join.right_prefer_join_type = hint.prefer_join_method;
        join.hint_info = Some(Rc::clone(&hint.hint_info));
    }
    join.prefer_join_type = preferred_join_type_from_side(join.left_prefer_join_type, true)
        | preferred_join_type_from_side(join.right_prefer_join_type, false);
    if contains_different_join_types(join.prefer_join_type) {
        join.prefer_join_type = 0;
    }
}

fn preferred_join_type_from_side(mut preference: u32, left: bool) -> u32 {
    use crate::plan_builder::from::join_hint_flags as flags;

    let mut result = 0;
    for (source, left_result, right_result) in [
        (
            flags::INLJ,
            flags::LEFT_AS_INLJ_INNER,
            flags::RIGHT_AS_INLJ_INNER,
        ),
        (
            flags::INLHJ,
            flags::LEFT_AS_INLHJ_INNER,
            flags::RIGHT_AS_INLHJ_INNER,
        ),
        (
            flags::INLMJ,
            flags::LEFT_AS_INLMJ_INNER,
            flags::RIGHT_AS_INLMJ_INNER,
        ),
        (
            flags::HJ_BUILD,
            flags::LEFT_AS_HJ_BUILD,
            flags::RIGHT_AS_HJ_BUILD,
        ),
        (
            flags::HJ_PROBE,
            flags::LEFT_AS_HJ_PROBE,
            flags::RIGHT_AS_HJ_PROBE,
        ),
    ] {
        if preference & source != 0 {
            preference &= !source;
            result |= if left { left_result } else { right_result };
        }
    }
    result | preference
}

fn contains_different_join_types(preference: u32) -> bool {
    use crate::plan_builder::from::join_hint_flags as flags;

    let preference = preference
        & !flags::NO_HASH_JOIN
        & !flags::NO_MERGE_JOIN
        & !flags::NO_INDEX_JOIN
        & !flags::NO_INDEX_HASH_JOIN
        & !flags::NO_INDEX_MERGE_JOIN;
    let inl = flags::RIGHT_AS_INLJ_INNER | flags::LEFT_AS_INLJ_INNER;
    let inlhj = flags::RIGHT_AS_INLHJ_INNER | flags::LEFT_AS_INLHJ_INNER;
    let inlmj = flags::RIGHT_AS_INLMJ_INNER | flags::LEFT_AS_INLMJ_INNER;
    let hash_right_build = flags::RIGHT_AS_HJ_BUILD | flags::LEFT_AS_HJ_PROBE;
    let hash_left_build = flags::LEFT_AS_HJ_BUILD | flags::RIGHT_AS_HJ_PROBE;
    let directional = inl | inlhj | inlmj | hash_right_build | hash_left_build;
    let mpp = flags::SHUFFLE_JOIN | flags::BC_JOIN;
    let ordinary_count = (preference & !directional & !mpp).count_ones();
    if ordinary_count > 1 || ordinary_count == 1 && preference & directional != 0 {
        return true;
    }
    [inl, inlhj, inlmj, hash_left_build, hash_right_build]
        .into_iter()
        .filter(|mask| preference & mask != 0)
        .count()
        > 1
}

fn align_not_null_with_schema(expression: Expression, schema: &Schema) -> Expression {
    fn align_column(mut column: Column, schema: &Schema) -> Column {
        let Some(source) = schema.retrieve_column(&column) else {
            return column;
        };
        let source_not_null = source
            .ret_type
            .as_ref()
            .is_some_and(|field_type| field_type.has_flag(FieldTypeFlags::NOT_NULL));
        let target_not_null = column
            .ret_type
            .as_ref()
            .is_some_and(|field_type| field_type.has_flag(FieldTypeFlags::NOT_NULL));
        if source_not_null != target_not_null {
            if let Some(field_type) = column.ret_type.as_mut() {
                if source_not_null {
                    field_type.add_flags(FieldTypeFlags::NOT_NULL);
                } else {
                    field_type.del_flags(FieldTypeFlags::NOT_NULL);
                }
            }
        }
        column
    }

    match expression {
        Expression::Column(column) => Expression::Column(align_column(column, schema)),
        Expression::CorrelatedColumn(mut column) => {
            column.column = align_column(column.column, schema);
            Expression::CorrelatedColumn(column)
        }
        Expression::ScalarFunction(mut function) => {
            function.args = function
                .args
                .into_iter()
                .map(|argument| align_not_null_with_schema(argument, schema))
                .collect();
            function.invalidate_cached_arguments();
            Expression::ScalarFunction(function)
        }
        other => other,
    }
}

fn right_to_left_rule(child: &Edge) -> ConflictRule {
    ConflictRule {
        from: child.right_vertexes.copy(),
        to: if child.left_vertexes.intersects(&child.total_eligibility_set) {
            child
                .left_vertexes
                .intersection(&child.total_eligibility_set)
        } else {
            child.left_vertexes.copy()
        },
    }
}

fn left_to_right_rule(child: &Edge) -> ConflictRule {
    ConflictRule {
        from: child.left_vertexes.copy(),
        to: if child
            .right_vertexes
            .intersects(&child.total_eligibility_set)
        {
            child
                .right_vertexes
                .intersection(&child.total_eligibility_set)
        } else {
            child.right_vertexes.copy()
        },
    }
}

fn join_type_table_index(join_type: crate::find_best_task::LogicalJoinType) -> usize {
    use crate::find_best_task::LogicalJoinType;
    match join_type {
        LogicalJoinType::Inner => 0,
        LogicalJoinType::LeftOuter => 1,
        LogicalJoinType::RightOuter => 2,
        LogicalJoinType::Semi | LogicalJoinType::LeftOuterSemi => 3,
        LogicalJoinType::AntiSemi | LogicalJoinType::AntiLeftOuterSemi => 4,
    }
}

const ASSOC_RULE_TABLE: [[bool; 5]; 5] = [
    [true, true, false, true, true],
    [false, true, false, false, false],
    [true, true, true, true, true],
    [false, false, false, false, false],
    [false, false, false, false, false],
];

const LEFT_ASSCOM_RULE_TABLE: [[bool; 5]; 5] = [
    [true, true, false, true, true],
    [true, true, true, true, true],
    [false, true, false, false, false],
    [true, true, false, true, true],
    [true, true, false, true, true],
];

const RIGHT_ASSCOM_RULE_TABLE: [[bool; 5]; 5] = [
    [true, false, true, false, false],
    [false, false, true, false, false],
    [true, true, true, false, false],
    [false, false, false, false, false],
    [false, false, false, false, false],
];

fn associative(child: &Edge, parent: &Edge) -> bool {
    ASSOC_RULE_TABLE[join_type_table_index(child.join_type)]
        [join_type_table_index(parent.join_type)]
}

fn left_asscom(child: &Edge, parent: &Edge) -> bool {
    LEFT_ASSCOM_RULE_TABLE[join_type_table_index(child.join_type)]
        [join_type_table_index(parent.join_type)]
}

fn right_asscom(parent: &Edge, child: &Edge) -> bool {
    RIGHT_ASSCOM_RULE_TABLE[join_type_table_index(parent.join_type)]
        [join_type_table_index(child.join_type)]
}

fn validate_cumulative_cost(cost: f64) -> Result<(), PlanError> {
    if cost.is_nan() {
        return Err(PlanError::internal("invalid cumulative cost: NaN"));
    }
    if cost == f64::NEG_INFINITY {
        return Err(PlanError::internal("invalid cumulative cost: -Inf"));
    }
    if cost < 0.0 {
        return Err(PlanError::internal(format!(
            "invalid cumulative cost: negative value {cost}"
        )));
    }
    Ok(())
}

pub(crate) fn cumulative_cost_by_children(plan: &LogicalPlan) -> Result<f64, PlanError> {
    let mut cost = plan
        .stats_info()
        .ok_or_else(|| {
            PlanError::internal(format!(
                "join reorder requires derived statistics for plan {}",
                plan.id()
            ))
        })?
        .row_count();
    for child in plan.children() {
        cost += cumulative_cost_by_children(child)?;
    }
    Ok(cost)
}

fn cumulative_cost_significantly_less(cost: f64, best_cost: f64) -> bool {
    cost < best_cost && best_cost - cost > 1.0_f64.max(cost.abs()).max(best_cost.abs()) * 1e-12
}

fn choose_best_greedy_start(
    start_count: usize,
    mut runner: impl FnMut(usize) -> Result<Option<Node>, PlanError>,
) -> Result<(Option<Node>, isize), PlanError> {
    let mut best: Option<Node> = None;
    let mut best_start_index = -1;
    for start_index in 0..start_count {
        let candidate = runner(start_index)?;
        if candidate.as_ref().is_some_and(|candidate| {
            best.as_ref().is_none_or(|best| {
                cumulative_cost_significantly_less(candidate.cumulative_cost, best.cumulative_cost)
            })
        }) {
            best = candidate;
            best_start_index = start_index as isize;
        }
    }
    Ok((best, best_start_index))
}

fn apply_cartesian_factor(cost: f64, cartesian_factor: f64) -> Result<f64, PlanError> {
    validate_cumulative_cost(cost)?;
    if cartesian_factor <= 0.0 {
        return Ok(f64::INFINITY);
    }
    if !cartesian_factor.is_finite() {
        return Err(PlanError::internal(format!(
            "invalid cartesian factor: {cartesian_factor}"
        )));
    }
    let adjusted = cost * cartesian_factor;
    validate_cumulative_cost(adjusted)?;
    Ok(adjusted)
}

fn check_connection_and_make_join(
    context: &RuleContext<'_>,
    detector: &mut ConflictDetector,
    left: &Node,
    right: &Node,
    vertex_hints: &BTreeMap<i32, JoinMethodHint>,
    allow_no_equal_condition: bool,
) -> Result<Option<(CheckConnectionResult, Node)>, PlanError> {
    let mut result = detector.check_connection(left, right)?;
    if !result.connected() {
        if !allow_no_equal_condition {
            return Ok(None);
        }
        let Some(cartesian) = detector.try_create_cartesian_result(left, right) else {
            return Ok(None);
        };
        result = cartesian;
    }
    let node = detector.make_join(
        context,
        result.clone(),
        vertex_hints,
        context.join_reorder_threshold,
    )?;
    Ok(Some((result, node)))
}

fn collect_used_edges(nodes: &[Node]) -> BTreeSet<u64> {
    nodes
        .iter()
        .flat_map(|node| node.used_edges.iter().copied())
        .collect()
}

fn greedy_connect_join_nodes(
    context: &RuleContext<'_>,
    detector: &mut ConflictDetector,
    mut nodes: Vec<Node>,
    vertex_hints: &BTreeMap<i32, JoinMethodHint>,
    cartesian_factor: f64,
    allow_no_equal_condition: bool,
) -> Result<Vec<Node>, PlanError> {
    while nodes.len() > 1 {
        let mut made_progress = false;
        let mut current_index = 0;
        while current_index + 1 < nodes.len() {
            let current = nodes[current_index].clone();
            let mut best: Option<(usize, Node)> = None;
            for candidate_index in current_index + 1..nodes.len() {
                let Some((connection, mut candidate)) = check_connection_and_make_join(
                    context,
                    detector,
                    &current,
                    &nodes[candidate_index],
                    vertex_hints,
                    allow_no_equal_condition,
                )?
                else {
                    continue;
                };
                if connection.no_equal_edge() {
                    if !allow_no_equal_condition {
                        continue;
                    }
                    candidate.cumulative_cost =
                        apply_cartesian_factor(candidate.cumulative_cost, cartesian_factor)?;
                }
                if best
                    .as_ref()
                    .is_none_or(|(_, best)| candidate.cumulative_cost < best.cumulative_cost)
                {
                    best = Some((candidate_index, candidate));
                }
            }
            if let Some((best_index, best_node)) = best {
                nodes[current_index] = best_node;
                nodes.remove(best_index);
                made_progress = true;
            } else {
                current_index += 1;
            }
        }
        if !made_progress {
            break;
        }
    }
    Ok(nodes)
}

fn make_join_with_detector(
    context: &RuleContext<'_>,
    detector: &mut ConflictDetector,
    left: &Node,
    right: &Node,
    vertex_hints: &BTreeMap<i32, JoinMethodHint>,
) -> Result<Node, PlanError> {
    let mut connection = detector.check_connection(left, right)?;
    if !connection.connected() {
        connection = detector
            .try_create_cartesian_result(left, right)
            .ok_or_else(|| {
                PlanError::internal("failed to construct bushy tree: no valid join edge found")
            })?;
    }
    detector.make_join(
        context,
        connection,
        vertex_hints,
        context.join_reorder_threshold,
    )
}

fn make_bushy_tree(
    context: &RuleContext<'_>,
    detector: &mut ConflictDetector,
    mut nodes: Vec<Node>,
    vertex_hints: &BTreeMap<i32, JoinMethodHint>,
) -> Result<Option<Node>, PlanError> {
    if nodes.is_empty() {
        return Ok(None);
    }
    while nodes.len() > 1 {
        let mut next = Vec::with_capacity(nodes.len().div_ceil(2));
        let mut iterator = nodes.into_iter();
        while let Some(left) = iterator.next() {
            if let Some(right) = iterator.next() {
                next.push(make_join_with_detector(
                    context,
                    detector,
                    &left,
                    &right,
                    vertex_hints,
                )?);
            } else {
                next.push(left);
            }
        }
        nodes = next;
    }
    Ok(nodes.pop())
}

fn move_greedy_start_to_front(mut nodes: Vec<Node>, start_index: usize) -> Vec<Node> {
    if start_index > 0 && start_index < nodes.len() {
        let start = nodes.remove(start_index);
        nodes.insert(0, start);
    }
    nodes
}

fn hinted_node_matches(node: &Node, table: &tidb_ast::HintTable) -> bool {
    let Some(plan) = node.plan.as_ref() else {
        return false;
    };
    let Some(alias) = crate::plan_builder::from::extract_table_alias(plan.output_names()) else {
        return false;
    };
    let database_matches = table
        .db_name
        .as_deref()
        .is_none_or(|database| database == "*" || database.eq_ignore_ascii_case(&alias.db_name));
    let table_matches = table.name.eq_ignore_ascii_case(&alias.table_name);
    let query_block_matches = table.qb_name.as_deref().is_none_or(|query_block| {
        query_block
            .strip_prefix("sel_")
            .and_then(|offset| offset.parse::<i32>().ok())
            .is_none_or(|offset| offset == plan.query_block_offset())
    });
    database_matches && table_matches && query_block_matches
}

fn take_hinted_node(nodes: &mut Vec<Node>, table: &tidb_ast::HintTable) -> Option<Node> {
    let index = nodes
        .iter()
        .position(|node| hinted_node_matches(node, table))?;
    Some(nodes.remove(index))
}

fn build_leading_tree_from_elements(
    context: &RuleContext<'_>,
    detector: &mut ConflictDetector,
    elements: &[tidb_ast::LeadingElement],
    nodes: &mut Vec<Node>,
    group: &JoinGroup,
) -> Result<Option<Node>, PlanError> {
    if elements.is_empty() {
        return Ok(None);
    }
    let original = nodes.clone();
    let mut current: Option<Node> = None;
    for element in elements {
        let next = match element {
            tidb_ast::LeadingElement::Table(table) => take_hinted_node(nodes, table),
            tidb_ast::LeadingElement::Group(nested) => {
                build_leading_tree_from_elements(context, detector, nested, nodes, group)?
            }
        };
        let Some(next) = next else {
            *nodes = original;
            return Ok(None);
        };
        current = match current {
            None => Some(next),
            Some(left) => {
                let Some((_, joined)) = check_connection_and_make_join(
                    context,
                    detector,
                    &left,
                    &next,
                    &group.vertex_hints,
                    true,
                )?
                else {
                    *nodes = original;
                    return Ok(None);
                };
                Some(joined)
            }
        };
    }
    Ok(current)
}

fn build_join_by_leading_hint(
    context: &RuleContext<'_>,
    detector: &mut ConflictDetector,
    nodes: &[Node],
    group: &JoinGroup,
) -> Result<(Option<Node>, Vec<Node>), PlanError> {
    let Some(first) = group.leading_hints.first() else {
        return Ok((None, nodes.to_vec()));
    };
    if group
        .leading_hints
        .windows(2)
        .any(|hints| !Rc::ptr_eq(&hints[0], &hints[1]))
    {
        if group.has_user_leading_hint {
            set_hint_warning(
                context,
                "We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid",
            );
        }
        return Ok((None, nodes.to_vec()));
    }
    let Some(elements) = first.leading.as_deref() else {
        return Ok((None, nodes.to_vec()));
    };
    let mut remaining = nodes.to_vec();
    let hinted =
        build_leading_tree_from_elements(context, detector, elements, &mut remaining, group)?;
    if hinted.is_none() {
        if group.has_user_leading_hint {
            set_hint_warning(
                context,
                "leading hint is inapplicable, check if the leading hint table is valid",
            );
        }
        return Ok((None, nodes.to_vec()));
    }
    Ok((hinted, remaining))
}

fn optimize_greedy_with_start(
    context: &RuleContext<'_>,
    detector: &mut ConflictDetector,
    nodes: &[Node],
    start_index: usize,
    group: &JoinGroup,
) -> Result<Option<Node>, PlanError> {
    let cartesian_factor = context.cartesian_join_order_threshold;
    let allow_no_equal_condition = cartesian_factor > 0.0 && group.all_inner_join;
    let mut nodes = greedy_connect_join_nodes(
        context,
        detector,
        move_greedy_start_to_front(nodes.to_vec(), start_index),
        &group.vertex_hints,
        cartesian_factor,
        allow_no_equal_condition,
    )?;
    let mut used_edges = collect_used_edges(&nodes);
    if !allow_no_equal_condition && detector.has_remaining_edges(&used_edges) {
        let before = nodes.len();
        nodes = greedy_connect_join_nodes(
            context,
            detector,
            nodes,
            &group.vertex_hints,
            cartesian_factor.max(1.0),
            true,
        )?;
        if nodes.len() != before {
            used_edges = collect_used_edges(&nodes);
        }
    }
    if detector.has_remaining_edges(&used_edges) {
        return Ok(None);
    }
    make_bushy_tree(context, detector, nodes, &group.vertex_hints)
}

fn optimize_greedy(
    context: &RuleContext<'_>,
    detector: &mut ConflictDetector,
    mut nodes: Vec<Node>,
    group: &JoinGroup,
) -> Result<Option<Node>, PlanError> {
    let (hinted, remaining) = build_join_by_leading_hint(context, detector, &nodes, group)?;
    if remaining.is_empty() {
        return Ok(hinted);
    }
    nodes = remaining;
    nodes.sort_by(|left, right| left.cumulative_cost.total_cmp(&right.cumulative_cost));
    if let Some(hinted) = hinted {
        nodes.insert(0, hinted);
        return optimize_greedy_with_start(context, detector, &nodes, 0, group);
    }
    if nodes.len() < 2 {
        return Ok(nodes.pop());
    }
    let (best, _) = choose_best_greedy_start(2, |start_index| {
        optimize_greedy_with_start(context, detector, &nodes, start_index, group)
    })?;
    Ok(best)
}

fn build_bushy_tree_from_dp(
    context: &RuleContext<'_>,
    detector: &mut ConflictDetector,
    leaves: &[Node],
    best_plans: &[Option<Node>],
    group: &JoinGroup,
) -> Result<Option<Node>, PlanError> {
    let mut candidates: Vec<(u64, Node)> = best_plans
        .iter()
        .enumerate()
        .filter_map(|(mask, node)| {
            let node = node.as_ref()?;
            (!node.cumulative_cost.is_infinite()
                && !detector.has_remaining_edges_in_subset(&node.bit_set, &node.used_edges))
            .then(|| (mask as u64, node.clone()))
        })
        .collect();
    candidates.sort_by(|(left_mask, left), (right_mask, right)| {
        right_mask
            .count_ones()
            .cmp(&left_mask.count_ones())
            .then_with(|| left.cumulative_cost.total_cmp(&right.cumulative_cost))
            .then_with(|| left_mask.cmp(right_mask))
    });

    let mut forest = Vec::new();
    let mut covered = FastIntSet::default();
    for (_, candidate) in candidates {
        if candidate.bit_set.intersects(&covered) {
            continue;
        }
        covered = covered.union(&candidate.bit_set);
        forest.push(candidate);
    }
    for leaf in leaves {
        if leaf.bit_set.intersects(&covered) {
            continue;
        }
        covered = covered.union(&leaf.bit_set);
        forest.push(leaf.clone());
    }
    make_bushy_tree(context, detector, forest, &group.vertex_hints)
}

fn optimize_dp(
    context: &RuleContext<'_>,
    detector: &mut ConflictDetector,
    nodes: Vec<Node>,
    group: &JoinGroup,
) -> Result<Option<Node>, PlanError> {
    if !group.leading_hints.is_empty() {
        set_hint_warning(
            context,
            "leading hint is inapplicable for the DP join reorder algorithm",
        );
    }
    if nodes.is_empty() {
        return Err(PlanError::internal("join group has no nodes"));
    }
    if nodes.len() == 1 {
        return Ok(nodes.into_iter().next());
    }
    if nodes.len() >= 63 {
        return Err(PlanError::internal(format!(
            "DP join reorder supports at most 62 nodes, got {}",
            nodes.len()
        )));
    }

    let full_mask = (1_u64 << nodes.len()) - 1;
    let mut best_plans = vec![None; (full_mask + 1) as usize];
    for node in &nodes {
        let mask = node
            .bit_set
            .get_small_uint64()
            .map_err(PlanError::internal)?;
        best_plans[mask as usize] = Some(node.clone());
    }

    for subset in 1..=full_mask {
        if subset.count_ones() == 1 {
            continue;
        }
        let mut left = (subset - 1) & subset;
        while left > 0 {
            let right = subset ^ left;
            if left <= right {
                if let (Some(left_plan), Some(right_plan)) = (
                    best_plans[left as usize].clone(),
                    best_plans[right as usize].clone(),
                ) {
                    if let Some((connection, mut candidate)) = check_connection_and_make_join(
                        context,
                        detector,
                        &left_plan,
                        &right_plan,
                        &group.vertex_hints,
                        true,
                    )? {
                        if connection.no_equal_edge() {
                            candidate.cumulative_cost = apply_cartesian_factor(
                                candidate.cumulative_cost,
                                context.cartesian_join_order_threshold,
                            )?;
                        }
                        if best_plans[subset as usize]
                            .as_ref()
                            .is_none_or(|best| candidate.cumulative_cost < best.cumulative_cost)
                        {
                            best_plans[subset as usize] = Some(candidate);
                        }
                    }
                }
            }
            left = (left - 1) & subset;
        }
    }

    let final_plan = best_plans[full_mask as usize].clone();
    if final_plan.as_ref().is_some_and(|node| {
        !node.cumulative_cost.is_infinite() && !detector.has_remaining_edges(&node.used_edges)
    }) {
        return Ok(final_plan);
    }
    let bushy = build_bushy_tree_from_dp(context, detector, &nodes, &best_plans, group)?;
    if bushy
        .as_ref()
        .is_some_and(|node| !detector.has_remaining_edges(&node.used_edges))
    {
        return Ok(bushy);
    }
    if final_plan.as_ref().is_some_and(|node| {
        node.cumulative_cost == f64::INFINITY && !detector.has_remaining_edges(&node.used_edges)
    }) {
        return Ok(final_plan);
    }
    Ok(None)
}

fn replace_join_group_vertices(
    mut plan: LogicalPlan,
    replacements: &BTreeMap<i32, LogicalPlan>,
) -> LogicalPlan {
    if let Some(replacement) = replacements.get(&plan.id()) {
        return replacement.clone();
    }
    if plan.children().is_empty() {
        return plan;
    }
    let children = plan
        .children()
        .to_vec()
        .into_iter()
        .map(|child| replace_join_group_vertices(child, replacements))
        .collect();
    plan.set_children(children);
    plan
}

fn optimize_join_group(
    context: &RuleContext<'_>,
    group: &JoinGroup,
) -> Result<LogicalPlan, PlanError> {
    let original_schema = group
        .root
        .schema()
        .cloned()
        .ok_or_else(|| PlanError::internal("join group root has no schema"))?;
    let original_names = group.root.output_names().to_vec();
    let mut detector = ConflictDetector {
        derive_stats_threshold: context.join_reorder_threshold,
        ..ConflictDetector::default()
    };
    let nodes = detector.build(group)?;
    let reordered_node = if i32::try_from(group.vertexes.len()).unwrap_or(i32::MAX)
        > context.join_reorder_threshold
    {
        optimize_greedy(context, &mut detector, nodes, group)?
    } else {
        optimize_dp(context, &mut detector, nodes, group)?
    };
    if reordered_node.is_none() {
        set_hint_warning(
            context,
            "no valid join order found, the original join order will be used",
        );
    }
    let reordered = reordered_node
        .and_then(|node| node.plan.as_ref().map(|plan| plan.as_ref().clone()))
        .unwrap_or_else(|| group.root.as_ref().clone());

    if reordered
        .schema()
        .is_some_and(|schema| schema.equal(&original_schema))
    {
        return Ok(reordered);
    }
    let expressions = original_schema
        .columns
        .iter()
        .cloned()
        .map(Expression::Column)
        .collect();
    let mut projection = LogicalProjection::new(
        BaseLogicalPlan::new(
            context.allocator,
            LogicalProjection::TYPE,
            reordered.query_block_offset(),
        ),
        expressions,
    );
    projection.base.base.set_schema(Some(original_schema));
    projection.base.base.set_output_names(original_names);
    projection.base.set_children(vec![reordered]);
    Ok(LogicalPlan::Projection(projection))
}

fn optimize_recursive(
    context: &RuleContext<'_>,
    mut plan: LogicalPlan,
) -> Result<LogicalPlan, PlanError> {
    if matches!(plan, LogicalPlan::CTE(_)) {
        return Ok(plan);
    }
    let mut group = extract_join_group(context, Rc::new(plan.clone()));
    if group.vertexes.is_empty() {
        return Err(PlanError::internal(format!(
            "join group has no vertexes, plan: {}",
            plan.id()
        )));
    }
    if group.vertexes.len() == 1 {
        let children = plan.children().to_vec();
        let mut optimized = Vec::with_capacity(children.len());
        for child in children {
            optimized.push(optimize_recursive(context, child)?);
        }
        plan.set_children(optimized);
        if group.has_user_leading_hint && !group.leading_hints.is_empty() {
            set_hint_warning(
                context,
                "leading hint is inapplicable, check the join type or the join algorithm hint",
            );
        }
        return Ok(plan);
    }

    let mut replacements = BTreeMap::new();
    for vertex in &mut group.vertexes {
        let old_id = vertex.id();
        let optimized = optimize_recursive(context, vertex.as_ref().clone())?;
        *vertex = Rc::new(optimized.clone());
        replacements.insert(old_id, optimized);
    }
    group.root = Rc::new(replace_join_group_vertices(
        group.root.as_ref().clone(),
        &replacements,
    ));
    optimize_join_group(context, &group)
}

/// Go `joinorder.Optimize`.
pub fn optimize(context: &RuleContext<'_>, plan: LogicalPlan) -> Result<LogicalPlan, PlanError> {
    optimize_recursive(context, plan)
}

fn set_hint_warning(context: &RuleContext<'_>, message: &str) {
    if let Some(sink) = context.hint_warning_sink {
        sink.set_hint_warning(message);
    }
}

/// Go `SubstituteColsInEqEdges`.
#[must_use]
pub fn substitute_cols_in_eq_edges(
    edges: &[ScalarFunction],
    column_expressions: &BTreeMap<i64, Expression>,
) -> Vec<ScalarFunction> {
    edges
        .iter()
        .map(|edge| {
            let original = edge.clone();
            match substitute_cols_in_expr(
                Expression::ScalarFunction(edge.clone()),
                column_expressions,
            ) {
                Expression::ScalarFunction(rewritten) => rewritten,
                _ => original,
            }
        })
        .collect()
}

/// Go `SubstituteColsInExprs`.
#[must_use]
pub fn substitute_cols_in_exprs(
    expressions: &[Expression],
    column_expressions: &BTreeMap<i64, Expression>,
) -> Vec<Expression> {
    expressions
        .iter()
        .cloned()
        .map(|expression| substitute_cols_in_expr(expression, column_expressions))
        .collect()
}

/// Go `SubstituteColsInExpr` and its `rewriteExprTree` helper.
#[must_use]
pub fn substitute_cols_in_expr(
    expression: Expression,
    column_expressions: &BTreeMap<i64, Expression>,
) -> Expression {
    if column_expressions.is_empty() {
        return expression;
    }
    match expression {
        Expression::Column(column) => column_expressions
            .get(&column.unique_id)
            .cloned()
            .map(|replacement| substitute_cols_in_expr(replacement, column_expressions))
            .unwrap_or(Expression::Column(column)),
        Expression::ScalarFunction(mut function) => {
            function.args = function
                .args
                .into_iter()
                .map(|argument| substitute_cols_in_expr(argument, column_expressions))
                .collect();
            function.invalidate_cached_arguments();
            Expression::ScalarFunction(function)
        }
        other => other,
    }
}

/// Go `GetEqEdgeArgsAndCols`.
#[must_use]
pub fn get_eq_edge_args_and_cols(
    edge: &ScalarFunction,
) -> Option<(&Expression, &Expression, Vec<Column>, Vec<Column>)> {
    let [left, right] = edge.get_args() else {
        return None;
    };
    Some((left, right, extract_columns(left), extract_columns(right)))
}

/// Go `AlignJoinEdgeArgs`.
#[must_use]
pub fn align_join_edge_args(
    left_argument: &Expression,
    right_argument: &Expression,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Option<(Expression, Expression, bool)> {
    if expr_from_schema(left_argument, left_schema)
        && expr_from_schema(right_argument, right_schema)
    {
        return Some((left_argument.clone(), right_argument.clone(), false));
    }
    if expr_from_schema(left_argument, right_schema)
        && expr_from_schema(right_argument, left_schema)
    {
        return Some((right_argument.clone(), left_argument.clone(), true));
    }
    None
}

/// Go `OuterJoinSideFiltersTouchMultipleLeaves`.
#[must_use]
pub fn outer_join_side_filters_touch_multiple_leaves(
    join: &LogicalJoin,
    outer_group: &[LogicalPlan],
    outer_column_expressions: &BTreeMap<i64, Expression>,
    outer_is_left: bool,
) -> bool {
    let mut other_conditions = join.other_conditions.clone();
    let mut side_conditions = if outer_is_left {
        join.left_conditions.clone()
    } else {
        join.right_conditions.clone()
    };
    let mut equal_conditions: Vec<Expression> = join
        .equal_conditions
        .iter()
        .cloned()
        .map(Expression::ScalarFunction)
        .collect();

    if !outer_column_expressions.is_empty() {
        other_conditions = substitute_cols_in_exprs(&other_conditions, outer_column_expressions);
        side_conditions = substitute_cols_in_exprs(&side_conditions, outer_column_expressions);
        equal_conditions = substitute_cols_in_exprs(&equal_conditions, outer_column_expressions);
    }

    let referenced: BTreeSet<i64> = other_conditions
        .iter()
        .chain(&side_conditions)
        .chain(&equal_conditions)
        .flat_map(extract_columns)
        .map(|column| column.unique_id)
        .collect();
    let mut affected_leaves = 0;
    for leaf in outer_group {
        let Some(schema) = leaf.schema() else {
            continue;
        };
        if schema
            .columns
            .iter()
            .any(|column| referenced.contains(&column.unique_id))
        {
            affected_leaves += 1;
            if affected_leaves > 1 {
                return true;
            }
        }
    }
    false
}

/// Go `DsSatisfiesOrdering`.
#[must_use]
pub fn data_source_satisfies_ordering(
    data_source: &crate::logical::data_source::DataSource,
    ordering_columns: &[Column],
    parent_filters: &[Expression],
) -> bool {
    let Some((ordering_column_ids, ordering_unique_ids)) =
        normalize_ordering_columns(ordering_columns)
    else {
        return false;
    };
    let Some(schema) = data_source.base.base.schema() else {
        return false;
    };
    if !schema_contains_all_ordering_columns(schema, &ordering_unique_ids) {
        return false;
    }
    table_has_index_matching_ordering(data_source, &ordering_column_ids, &[], parent_filters)
}

/// Go `OrderedLeadingChoice`.
#[derive(Clone, Debug)]
pub struct OrderedLeadingChoice {
    /// The join-group vertex carrying the required order.
    pub carrier_vertex: Rc<LogicalPlan>,
    /// The single-table identity usable by an internal LEADING hint.
    pub leading_table: Option<tidb_ast::HintTable>,
    /// Every vertex in the current join group.
    pub vertices: Vec<Rc<LogicalPlan>>,
}

/// Go `FindOrderedLeadingChoice`.
#[must_use]
pub fn find_ordered_leading_choice(
    context: &RuleContext<'_>,
    root: &LogicalPlan,
    ordering_columns: &[Column],
) -> Option<OrderedLeadingChoice> {
    let (_, unique_ids) = normalize_ordering_columns(ordering_columns)?;
    let group = extract_join_group(context, Rc::new(root.clone()));
    if group.vertexes.len() <= 1 {
        return None;
    }
    for vertex in &group.vertexes {
        let schema = vertex.schema()?;
        if !schema_contains_all_ordering_columns(schema, &unique_ids) {
            continue;
        }
        let leading_table = crate::plan_builder::from::extract_table_alias(vertex.output_names())
            .map(|alias| tidb_ast::HintTable {
                db_name: (!alias.db_name.is_empty()).then_some(alias.db_name),
                name: alias.table_name,
                qb_name: (vertex.query_block_offset() > 0)
                    .then(|| format!("sel_{}", vertex.query_block_offset())),
                partitions: Vec::new(),
            });
        return Some(OrderedLeadingChoice {
            carrier_vertex: Rc::clone(vertex),
            leading_table,
            vertices: group.vertexes,
        });
    }
    None
}

/// Go `TryAnnotateOrderedLeading`.
pub fn try_annotate_ordered_leading(
    context: &RuleContext<'_>,
    root: &mut LogicalPlan,
    choice: &OrderedLeadingChoice,
) -> bool {
    let snapshot = root.clone();
    let Some(anchor) = find_leading_hint_anchor(root) else {
        return false;
    };
    try_annotate_ordered_leading_anchor(context, &snapshot, anchor, choice)
}

/// Go `TryAnnotateOrderedLeading` for a selection anchor already mutably borrowed by a rule.
pub fn try_annotate_ordered_leading_on_selection(
    context: &RuleContext<'_>,
    selection: &mut LogicalSelection,
    choice: &OrderedLeadingChoice,
) -> bool {
    let snapshot = LogicalPlan::Selection(selection.clone());
    let Some(anchor) = selection
        .base
        .children_mut()
        .first_mut()
        .and_then(find_leading_hint_anchor)
    else {
        return false;
    };
    try_annotate_ordered_leading_anchor(context, &snapshot, anchor, choice)
}

/// Go `TryAnnotateOrderedLeading` for a join anchor already mutably borrowed by a rule.
pub fn try_annotate_ordered_leading_on_join(
    context: &RuleContext<'_>,
    join: &mut LogicalJoin,
    choice: &OrderedLeadingChoice,
) -> bool {
    let snapshot = LogicalPlan::Join(join.clone());
    try_annotate_ordered_leading_anchor(context, &snapshot, join, choice)
}

fn try_annotate_ordered_leading_anchor(
    context: &RuleContext<'_>,
    root: &LogicalPlan,
    anchor: &mut LogicalJoin,
    choice: &OrderedLeadingChoice,
) -> bool {
    let group = extract_join_group(context, Rc::new(root.clone()));
    if !group.leading_hints.is_empty() {
        return false;
    }
    let Some(table) = choice.leading_table.clone() else {
        return false;
    };
    if anchor.prefer_join_order
        || anchor.internal_prefer_join_order
        || anchor.prefer_join_type != 0
        || anchor.hint_info.is_some()
        || anchor.internal_hint_info.is_some()
    {
        return false;
    }
    anchor.internal_prefer_join_order = true;
    anchor.internal_hint_info = Some(Rc::new(crate::plan_builder::from::JoinHints {
        tables: BTreeMap::new(),
        leading: Some(vec![tidb_ast::LeadingElement::Table(table)]),
    }));
    true
}

fn find_leading_hint_anchor(root: &mut LogicalPlan) -> Option<&mut LogicalJoin> {
    match root {
        LogicalPlan::Join(join) => Some(join),
        LogicalPlan::Selection(selection) => {
            let child = selection.base.children_mut().first_mut()?;
            find_leading_hint_anchor(child)
        }
        _ => None,
    }
}

fn normalize_ordering_columns(ordering_columns: &[Column]) -> Option<(Vec<i64>, BTreeSet<i64>)> {
    let mut ids = Vec::with_capacity(ordering_columns.len());
    let mut unique_ids = BTreeSet::new();
    for column in ordering_columns {
        if column.id <= 0 || column.unique_id <= 0 {
            return None;
        }
        ids.push(column.id);
        unique_ids.insert(column.unique_id);
    }
    (!ids.is_empty() && unique_ids.len() == ordering_columns.len()).then_some((ids, unique_ids))
}

fn schema_contains_all_ordering_columns(
    schema: &Schema,
    ordering_unique_ids: &BTreeSet<i64>,
) -> bool {
    !ordering_unique_ids.is_empty()
        && schema.columns.len() >= ordering_unique_ids.len()
        && schema
            .columns
            .iter()
            .filter(|column| ordering_unique_ids.contains(&column.unique_id))
            .count()
            == ordering_unique_ids.len()
}

fn table_has_index_matching_ordering(
    data_source: &crate::logical::data_source::DataSource,
    ordering_column_ids: &[i64],
    group_selection_conditions: &[Expression],
    parent_filters: &[Expression],
) -> bool {
    let equality_column_ids = collect_equality_predicate_column_ids(
        data_source,
        group_selection_conditions,
        parent_filters,
    );
    data_source.indexes.iter().any(|index| {
        index.is_public
            && index.is_visible
            && index_matches_ordering(
                index,
                data_source,
                ordering_column_ids,
                &equality_column_ids,
            )
    })
}

fn index_matches_ordering(
    index: &crate::plan_builder::catalog::SourceIndex,
    data_source: &crate::logical::data_source::DataSource,
    ordering_column_ids: &[i64],
    equality_column_ids: &BTreeSet<i64>,
) -> bool {
    if ordering_column_ids.is_empty() {
        return false;
    }
    let mut order_position = 0;
    for index_column in &index.columns {
        let Some(column) = data_source.table_columns.get(index_column.offset) else {
            return false;
        };
        if ordering_column_ids.get(order_position) == Some(&column.id) {
            order_position += 1;
            if order_position == ordering_column_ids.len() {
                return true;
            }
            continue;
        }
        if order_position == 0 && equality_column_ids.contains(&column.id) {
            continue;
        }
        return false;
    }
    order_position == ordering_column_ids.len()
}

fn collect_equality_predicate_column_ids(
    data_source: &crate::logical::data_source::DataSource,
    group_selection_conditions: &[Expression],
    parent_filters: &[Expression],
) -> BTreeSet<i64> {
    let mut result = BTreeSet::new();
    for condition in &data_source.all_conds {
        extract_equality_columns(condition, &mut result);
    }
    let Some(schema) = data_source.base.base.schema() else {
        return result;
    };
    add_equality_columns_from_local_conditions(&mut result, schema, group_selection_conditions);
    add_equality_columns_from_local_conditions(&mut result, schema, parent_filters);
    result
}

fn add_equality_columns_from_local_conditions(
    result: &mut BTreeSet<i64>,
    schema: &Schema,
    conditions: &[Expression],
) {
    for condition in conditions {
        let columns = extract_columns(condition);
        if columns.is_empty() || columns.iter().any(|column| !schema.contains(column)) {
            continue;
        }
        extract_equality_columns(condition, result);
    }
}

fn extract_equality_columns(expression: &Expression, result: &mut BTreeSet<i64>) {
    let Expression::ScalarFunction(function) = expression else {
        return;
    };
    match function.func_name.lowercase() {
        "and" => {
            for argument in function.get_args() {
                extract_equality_columns(argument, result);
            }
        }
        "eq" => {
            let [left, right] = function.get_args() else {
                return;
            };
            if let Expression::Column(column) = left {
                if column.id > 0 && is_deterministic_const_expression(right) {
                    result.insert(column.id);
                }
            }
            if let Expression::Column(column) = right {
                if column.id > 0 && is_deterministic_const_expression(left) {
                    result.insert(column.id);
                }
            }
        }
        "in" => {
            let [Expression::Column(column), value] = function.get_args() else {
                return;
            };
            if column.id > 0 && is_deterministic_const_expression(value) {
                result.insert(column.id);
            }
        }
        _ => {}
    }
}

fn is_deterministic_const_expression(expression: &Expression) -> bool {
    extract_columns(expression).is_empty() && !is_mutable_effects_expr(expression)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::data_source::DataSource;
    use crate::logical::table_dual::LogicalTableDual;
    use crate::logical::BaseLogicalPlan;
    use crate::plan_base::PlanIdAllocator;
    use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
    use crate::stats_info::StatsInfo;
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::constant::Constant;

    fn column(id: i64) -> Column {
        let mut column = Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        column.id = id;
        column
    }

    fn call(name: &str, arguments: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new(name),
            FieldType::new(FieldTypeCode::LongLong),
            arguments,
        ))
    }

    #[test]
    fn substitute_columns_follows_chains_and_rewrites_nested_arguments() {
        let mut replacements = BTreeMap::new();
        replacements.insert(3, Expression::Column(column(2)));
        replacements.insert(
            2,
            call(
                "plus",
                vec![Expression::Column(column(1)), Expression::Column(column(1))],
            ),
        );

        let rewritten = substitute_cols_in_expr(
            call(
                "eq",
                vec![Expression::Column(column(3)), Expression::Column(column(4))],
            ),
            &replacements,
        );
        let ids: Vec<i64> = extract_columns(&rewritten)
            .into_iter()
            .map(|one| one.unique_id)
            .collect();
        assert_eq!(ids, vec![1, 4]);
    }

    #[test]
    fn align_join_edge_arguments_accepts_both_directions_only() {
        let left = Schema::new(vec![column(1)]);
        let right = Schema::new(vec![column(2)]);
        let l = Expression::Column(column(1));
        let r = Expression::Column(column(2));
        assert_eq!(
            align_join_edge_args(&l, &r, &left, &right).map(|(_, _, swapped)| swapped),
            Some(false)
        );
        assert_eq!(
            align_join_edge_args(&r, &l, &left, &right).map(|(_, _, swapped)| swapped),
            Some(true)
        );
        assert!(align_join_edge_args(&l, &l, &left, &right).is_none());
    }

    #[test]
    fn ordered_data_source_requires_exact_index_sequence_or_fixed_prefix() {
        let mut base = BaseLogicalPlan::default();
        base.base
            .set_schema(Some(Schema::new(vec![column(1), column(2), column(3)])));
        let mut data_source = DataSource::new(base, 1, "t");
        data_source.table_columns = vec![column(1), column(2), column(3)];
        data_source.indexes = vec![SourceIndex {
            is_public: true,
            is_visible: true,
            columns: vec![
                SourceIndexColumn {
                    name: "a".to_owned(),
                    offset: 0,
                    ..SourceIndexColumn::default()
                },
                SourceIndexColumn {
                    name: "b".to_owned(),
                    offset: 1,
                    ..SourceIndexColumn::default()
                },
                SourceIndexColumn {
                    name: "c".to_owned(),
                    offset: 2,
                    ..SourceIndexColumn::default()
                },
            ],
            ..SourceIndex::default()
        }];

        assert!(data_source_satisfies_ordering(
            &data_source,
            &[column(1), column(2)],
            &[]
        ));
        assert!(!data_source_satisfies_ordering(
            &data_source,
            &[column(2)],
            &[]
        ));

        let fixed_prefix = call(
            "eq",
            vec![
                Expression::Column(column(1)),
                Expression::Constant(Constant::new(
                    Datum::Int(7),
                    FieldType::new(FieldTypeCode::LongLong),
                )),
            ],
        );
        assert!(data_source_satisfies_ordering(
            &data_source,
            &[column(2), column(3)],
            &[fixed_prefix]
        ));
        assert!(!data_source_satisfies_ordering(
            &data_source,
            &[column(1), column(3)],
            &[]
        ));

        data_source.indexes[0].is_visible = false;
        assert!(!data_source_satisfies_ordering(
            &data_source,
            &[column(1)],
            &[]
        ));
    }

    #[test]
    fn choose_best_greedy_start_matches_all_original_subtests() {
        let (best, index) = choose_best_greedy_start(2, |start| {
            Ok(Some(Node {
                cumulative_cost: [100.0, 10.0][start],
                ..Node::default()
            }))
        })
        .unwrap();
        assert_eq!(index, 1);
        assert_eq!(best.unwrap().cumulative_cost, 10.0);

        let (best, index) = choose_best_greedy_start(2, |start| {
            Ok((start == 1).then_some(Node {
                cumulative_cost: 10.0,
                ..Node::default()
            }))
        })
        .unwrap();
        assert_eq!(index, 1);
        assert_eq!(best.unwrap().cumulative_cost, 10.0);

        let costs = [14166.666666666668, 14166.666666666666];
        let (best, index) = choose_best_greedy_start(2, |start| {
            Ok(Some(Node {
                cumulative_cost: costs[start],
                ..Node::default()
            }))
        })
        .unwrap();
        assert_eq!(index, 0);
        assert_eq!(best.unwrap().cumulative_cost, costs[0]);
    }

    #[test]
    fn cloned_greedy_nodes_isolate_edges_and_share_the_plan() {
        let allocator = PlanIdAllocator::new();
        let plan = Rc::new(LogicalPlan::TableDual(LogicalTableDual::new(
            BaseLogicalPlan::new(&allocator, LogicalTableDual::TYPE, 0),
            1,
        )));
        let original = Node {
            bit_set: FastIntSet::new([0]),
            plan: Some(Rc::clone(&plan)),
            cumulative_cost: 7.0,
            used_edges: BTreeSet::from([1]),
        };
        let mut cloned = original.clone();
        assert!(cloned.used_edges.remove(&1));
        cloned.used_edges.insert(2);
        assert!(original.used_edges.contains(&1));
        assert!(!original.used_edges.contains(&2));
        assert!(Rc::ptr_eq(
            original.plan.as_ref().unwrap(),
            cloned.plan.as_ref().unwrap()
        ));
        assert!(original.bit_set.equals(&cloned.bit_set));
    }

    #[test]
    fn cartesian_factor_matches_pinned_validation_and_disable_policy() {
        assert_eq!(apply_cartesian_factor(5.0, 3.0).unwrap(), 15.0);
        assert_eq!(apply_cartesian_factor(5.0, 0.0).unwrap(), f64::INFINITY);
        assert!(apply_cartesian_factor(-1.0, 3.0).is_err());
        assert!(apply_cartesian_factor(1.0, f64::NAN).is_err());
    }

    fn edge(
        index: u64,
        join_type: crate::find_best_task::LogicalJoinType,
        left: &[i64],
        right: &[i64],
        eligibility: &[i64],
    ) -> Edge {
        Edge {
            index,
            join_type,
            equal_conditions: Vec::new(),
            non_equal_conditions: Vec::new(),
            total_eligibility_set: FastIntSet::new(eligibility),
            rules: Vec::new(),
            skip_rules: false,
            left_vertexes: FastIntSet::new(left),
            right_vertexes: FastIntSet::new(right),
        }
    }

    fn node(vertexes: &[i64]) -> Node {
        Node {
            bit_set: FastIntSet::new(vertexes),
            ..Node::default()
        }
    }

    #[test]
    fn conflict_edges_enforce_eligibility_rules_and_outer_direction() {
        use crate::find_best_task::LogicalJoinType;

        let mut inner = edge(0, LogicalJoinType::Inner, &[0], &[1], &[0, 1]);
        assert!(inner.inner_applicable(&node(&[0]), &node(&[1])));
        assert!(!inner.inner_applicable(&node(&[0]), &node(&[2])));
        inner.rules.push(ConflictRule {
            from: FastIntSet::new([1]),
            to: FastIntSet::new([2]),
        });
        assert!(!inner.inner_applicable(&node(&[0]), &node(&[1])));
        assert!(inner.inner_applicable(&node(&[0, 2]), &node(&[1])));

        let outer = edge(1, LogicalJoinType::LeftOuter, &[0], &[1], &[0, 1]);
        assert!(outer.non_inner_applicable(&node(&[0]), &node(&[1])));
        assert!(!outer.non_inner_applicable(&node(&[1]), &node(&[0])));

        let detector = ConflictDetector {
            non_inner_edges: vec![outer],
            ..ConflictDetector::default()
        };
        let result = detector.check_connection(&node(&[1]), &node(&[0])).unwrap();
        assert!(result.connected());
        assert!(result.no_equal_edge());
        assert!(result.node1.bit_set.has(0));
        assert!(result.node2.bit_set.has(1));
    }

    #[test]
    fn conflict_rule_tables_and_cartesian_gate_match_pinned_go() {
        use crate::find_best_task::LogicalJoinType;

        let inner = edge(0, LogicalJoinType::Inner, &[0], &[1], &[0, 1]);
        let left = edge(1, LogicalJoinType::LeftOuter, &[0], &[1], &[0, 1]);
        let right = edge(2, LogicalJoinType::RightOuter, &[0], &[1], &[0, 1]);
        assert!(associative(&inner, &left));
        assert!(!associative(&left, &inner));
        assert!(left_asscom(&left, &right));
        assert!(right_asscom(&right, &left));

        let rule = right_to_left_rule(&left);
        assert!(rule.from.equals(&FastIntSet::new([1])));
        assert!(rule.to.equals(&FastIntSet::new([0])));
        let rule = left_to_right_rule(&left);
        assert!(rule.from.equals(&FastIntSet::new([0])));
        assert!(rule.to.equals(&FastIntSet::new([1])));

        let mut detector = ConflictDetector {
            all_inner_join: true,
            ..ConflictDetector::default()
        };
        let cartesian = detector
            .try_create_cartesian_result(&node(&[0]), &node(&[1]))
            .unwrap();
        assert!(cartesian.connected());
        assert!(cartesian.no_equal_edge());
        assert!(!detector.has_remaining_edges(&BTreeSet::new()));

        let mut detector = ConflictDetector {
            all_inner_join: false,
            ..ConflictDetector::default()
        };
        assert!(detector
            .try_create_cartesian_result(&node(&[0]), &node(&[1]))
            .is_none());
    }

    #[test]
    fn remaining_edge_subset_requires_a_real_fully_contained_edge() {
        use crate::find_best_task::LogicalJoinType;

        let mut real = edge(7, LogicalJoinType::Inner, &[0], &[1], &[0, 1]);
        real.non_equal_conditions
            .push(Expression::Column(column(1)));
        let detector = ConflictDetector {
            inner_edges: vec![real],
            all_inner_join: true,
            ..ConflictDetector::default()
        };
        assert!(detector.has_remaining_edges(&BTreeSet::new()));
        assert!(detector.has_remaining_edges_in_subset(&FastIntSet::new([0, 1]), &BTreeSet::new()));
        assert!(!detector.has_remaining_edges_in_subset(&FastIntSet::new([0]), &BTreeSet::new()));
        assert!(!detector.has_remaining_edges(&BTreeSet::from([7])));
    }

    fn data_source_plan(allocator: &PlanIdAllocator, column_id: i64, rows: f64) -> LogicalPlan {
        let mut base = BaseLogicalPlan::new(allocator, DataSource::TYPE, 0);
        base.base
            .set_schema(Some(Schema::new(vec![column(column_id)])));
        base.base
            .set_stats(Some(StatsInfo::new(rows, [(column_id, rows)])));
        let mut data_source = DataSource::new(base, column_id, format!("t{column_id}"));
        data_source.table_stats = Some(StatsInfo::new(rows, [(column_id, rows)]));
        LogicalPlan::DataSource(data_source)
    }

    #[test]
    fn conflict_detector_builds_edges_from_the_real_logical_tree() {
        use crate::find_best_task::LogicalJoinType;

        let allocator = PlanIdAllocator::new();
        let left = data_source_plan(&allocator, 1, 10.0);
        let right = data_source_plan(&allocator, 2, 20.0);
        let mut join_base = BaseLogicalPlan::new(&allocator, LogicalJoin::TYPE, 0);
        join_base
            .base
            .set_schema(Some(Schema::new(vec![column(1), column(2)])));
        join_base
            .base
            .set_stats(Some(StatsInfo::new(5.0, [(1, 5.0), (2, 5.0)])));
        join_base.set_children(vec![left.clone(), right.clone()]);
        let mut join = LogicalJoin::new(join_base, LogicalJoinType::Inner);
        let Expression::ScalarFunction(equality) = call(
            "eq",
            vec![Expression::Column(column(1)), Expression::Column(column(2))],
        ) else {
            unreachable!()
        };
        join.equal_conditions.push(equality);
        let root = Rc::new(LogicalPlan::Join(join));
        let group = JoinGroup {
            root: Rc::clone(&root),
            vertexes: vec![Rc::new(left), Rc::new(right)],
            leading_hints: Vec::new(),
            has_user_leading_hint: false,
            vertex_hints: BTreeMap::new(),
            all_inner_join: true,
            selection_conditions: BTreeMap::new(),
        };

        let mut detector = ConflictDetector::default();
        let nodes = detector.build(&group).unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(detector.inner_edges.len(), 1);
        assert_eq!(detector.non_inner_edges.len(), 0);
        assert_eq!(nodes[0].cumulative_cost, 10.0);
        assert_eq!(nodes[1].cumulative_cost, 20.0);
        let connection = detector.check_connection(&nodes[0], &nodes[1]).unwrap();
        assert!(connection.connected());
        assert!(!connection.no_equal_edge());

        let allocator = crate::plan_base::PlanIdAllocator::new();
        let context = crate::logical::rule_tests::test_context(&allocator);
        let mut greedy_detector = ConflictDetector::default();
        let greedy_nodes = greedy_detector.build(&group).unwrap();
        let greedy = optimize_greedy(&context, &mut greedy_detector, greedy_nodes, &group)
            .unwrap()
            .unwrap();
        assert!(matches!(greedy.plan.as_deref(), Some(LogicalPlan::Join(_))));
        assert_eq!(greedy.used_edges.len(), 1);

        let mut dp_detector = ConflictDetector::default();
        let dp_nodes = dp_detector.build(&group).unwrap();
        let dp = optimize_dp(&context, &mut dp_detector, dp_nodes, &group)
            .unwrap()
            .unwrap();
        assert!(matches!(dp.plan.as_deref(), Some(LogicalPlan::Join(_))));
        assert_eq!(dp.used_edges.len(), 1);

        let optimized = optimize(&context, root.as_ref().clone()).unwrap();
        assert!(matches!(optimized, LogicalPlan::Join(_)));
    }
}
