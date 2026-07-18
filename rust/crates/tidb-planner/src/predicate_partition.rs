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

//! Dependency-only routing for deferred join predicates.
//!
//! Go's `LogicalJoin.ExtractOnCondition` partitions predicates into left,
//! right, equality, and `OtherConditions` buckets.  The actual Go expression
//! evaluator and predicate-pushdown rules are much wider than this Rust
//! rewrite stage.  This module therefore records only a conservative
//! dependency route for a previously bound [`DeferredConditionPlan`]: a
//! single-child predicate is a *candidate* for that child, a cross-child
//! predicate remains a join residual, and any opaque/mutable-shaped predicate
//! stays deferred.  No value is evaluated and no join algorithm is selected.

use tidb_ast::Expr;

use crate::condition_binding::{bind_residual, ConditionBindingError, DeferredConditionPlan};
use crate::join_condition::{JoinSchema, JoinSide};
use crate::residual_condition::{ResidualLeafKind, ResidualPredicate};

/// A conservative candidate route for one predicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredicateRoute {
    /// The predicate references only the left child.
    LeftPushdown,
    /// The predicate references only the right child.
    RightPushdown,
    /// The predicate references both children and remains at the join seam.
    JoinResidual,
    /// A value-independent shape can be considered by a caller's constant
    /// simplifier; this module never evaluates it.
    ConstantCandidate,
    /// Dependencies or AST semantics are not represented by this bounded
    /// planner owner.
    Deferred,
}

/// Whether the route still needs a typed expression/effects check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredicateSafety {
    /// Shape has no function/unsupported marker and can be considered by the
    /// caller's dependency-only pushdown gate.
    ShapeOnly,
    /// A typed owner must check mutability, null semantics, and evaluator
    /// support before changing where the predicate executes.
    RequiresTypedCheck,
}

/// One source predicate and its conservative dependency route.
#[derive(Clone, Debug, PartialEq)]
pub struct PlannedPredicate {
    expression: Expr,
    plan: DeferredConditionPlan,
    route: PredicateRoute,
    safety: PredicateSafety,
    full_schema_width: usize,
}

impl PlannedPredicate {
    /// Returns the underlying deferred condition plan.
    #[must_use]
    pub const fn plan(&self) -> &DeferredConditionPlan {
        &self.plan
    }

    /// Returns the original AST expression reserved for typed evaluation.
    #[must_use]
    pub const fn expression(&self) -> &Expr {
        &self.expression
    }

    /// Returns the dependency-only candidate route.
    #[must_use]
    pub const fn route(&self) -> PredicateRoute {
        self.route
    }

    /// Returns whether a typed effects/evaluator check remains mandatory.
    #[must_use]
    pub const fn safety(&self) -> PredicateSafety {
        self.safety
    }

    /// Returns the source-shaped `FullSchema` width required by a typed row
    /// evaluator.  This includes redundant `USING` columns; the visible
    /// result projection is a separate executor contract.
    #[must_use]
    pub const fn full_schema_width(&self) -> usize {
        self.full_schema_width
    }

    /// Creates a typed evaluator handoff without evaluating the predicate.
    #[must_use]
    pub fn typed_request(
        &self,
        mode: crate::typed_condition::ConditionEvaluationMode,
    ) -> crate::typed_condition::TypedConditionRequest {
        crate::typed_condition::TypedConditionRequest::from_parts(
            self.plan.clone(),
            self.expression.clone(),
            self.route,
            self.safety,
            self.full_schema_width,
            mode,
        )
    }
}

/// A batch of predicates retaining source order and explicit deferred gaps.
#[derive(Clone, Debug, PartialEq)]
pub struct PredicatePartitionPlan {
    predicates: Vec<PlannedPredicate>,
}

impl PredicatePartitionPlan {
    /// Returns predicates in their source order.
    #[must_use]
    pub fn predicates(&self) -> &[PlannedPredicate] {
        &self.predicates
    }
}

/// Binds and routes predicates without executing or rewriting them.
pub fn partition_predicates<I>(
    expressions: I,
    schema: &JoinSchema,
) -> Result<PredicatePartitionPlan, ConditionBindingError>
where
    I: IntoIterator<Item = Expr>,
{
    let full_schema_width = schema.full_columns().count();
    let predicates = expressions
        .into_iter()
        .map(|expr| {
            let plan = bind_residual(&expr, schema)?;
            let route = route_for(&plan);
            let safety = safety_for(&plan);
            Ok(PlannedPredicate {
                expression: expr,
                plan,
                route,
                safety,
                full_schema_width,
            })
        })
        .collect::<Result<Vec<_>, ConditionBindingError>>()?;
    Ok(PredicatePartitionPlan { predicates })
}

fn route_for(plan: &DeferredConditionPlan) -> PredicateRoute {
    // Binding columns inside a dedicated AST shape is required for safe typed
    // execution, but it does not make that shape eligible for predicate
    // pushdown. Keep syntax ownership and dependency discovery independent:
    // an unsupported residual remains deferred even when every nested column
    // resolved to one child.
    if contains_unsupported(plan.predicate()) {
        return PredicateRoute::Deferred;
    }
    let mut left = false;
    let mut right = false;
    for binding in plan.bindings() {
        match binding.column().side() {
            JoinSide::Left => left = true,
            JoinSide::Right => right = true,
        }
    }
    match (left, right) {
        (true, true) => PredicateRoute::JoinResidual,
        (true, false) => PredicateRoute::LeftPushdown,
        (false, true) => PredicateRoute::RightPushdown,
        (false, false) if is_constant_shape(plan) => PredicateRoute::ConstantCandidate,
        (false, false) => PredicateRoute::Deferred,
    }
}

fn contains_unsupported(predicate: &ResidualPredicate) -> bool {
    match predicate {
        ResidualPredicate::All(children) | ResidualPredicate::Any(children) => {
            children.iter().any(contains_unsupported)
        }
        ResidualPredicate::Not(inner) => contains_unsupported(inner),
        ResidualPredicate::Leaf(_) => false,
        ResidualPredicate::Unsupported(_) => true,
    }
}

fn safety_for(plan: &DeferredConditionPlan) -> PredicateSafety {
    if plan.opaque_shapes().is_empty() && !requires_typed_check(plan.predicate()) {
        PredicateSafety::ShapeOnly
    } else {
        PredicateSafety::RequiresTypedCheck
    }
}

fn is_constant_shape(plan: &DeferredConditionPlan) -> bool {
    plan.opaque_shapes().is_empty() && !contains_function(plan.predicate())
}

fn requires_typed_check(predicate: &ResidualPredicate) -> bool {
    match predicate {
        ResidualPredicate::All(children) | ResidualPredicate::Any(children) => {
            children.iter().any(requires_typed_check)
        }
        ResidualPredicate::Not(inner) => requires_typed_check(inner),
        ResidualPredicate::Leaf(leaf) => {
            matches!(leaf.kind(), ResidualLeafKind::Function { .. })
        }
        ResidualPredicate::Unsupported(_) => true,
    }
}

fn contains_function(predicate: &ResidualPredicate) -> bool {
    match predicate {
        ResidualPredicate::All(children) | ResidualPredicate::Any(children) => {
            children.iter().any(contains_function)
        }
        ResidualPredicate::Not(inner) => contains_function(inner),
        ResidualPredicate::Leaf(leaf) => {
            matches!(leaf.kind(), ResidualLeafKind::Function { .. })
        }
        ResidualPredicate::Unsupported(_) => false,
    }
}
