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

//! Typed-expression evaluator handoff for deferred predicates.
//!
//! TiDB's joiner evaluates `OtherConditions` over a shallow joined row.  A
//! normal filter discards `NULL`/UNKNOWN, while outer/semi join paths must
//! retain that distinction for their unmatched-row status.  This module
//! carries only that execution contract plus the source `FullSchema` width;
//! it does not evaluate a Datum, materialize a row, or select a join
//! algorithm.  A future typed expression owner consumes this request.

use tidb_ast::Expr;

use crate::condition_binding::DeferredConditionPlan;
use crate::predicate_partition::{PredicateRoute, PredicateSafety};

/// Which executor-facing evaluation context will consume a condition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConditionEvaluationMode {
    /// A candidate predicate applied to one child before join assembly.
    ChildFilter,
    /// A predicate applied to the joined shallow row.
    JoinFilter,
    /// A predicate whose UNKNOWN result must be retained for outer/semi join
    /// unmatched-row handling.
    OuterMatchStatus,
}

/// The truth information a typed evaluator must return to its caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TruthPolicy {
    /// Keep only TRUE rows; FALSE and UNKNOWN are filtered out.
    KeepTrueOnly,
    /// Keep TRUE/FALSE separately from UNKNOWN so the joiner can apply its
    /// outer/semi-join status rules.
    TrackUnknown,
}

/// A dependency-closed request for the eventual typed condition evaluator.
#[derive(Clone, Debug, PartialEq)]
pub struct TypedConditionRequest {
    expression: Expr,
    plan: DeferredConditionPlan,
    route: PredicateRoute,
    safety: PredicateSafety,
    full_schema_width: usize,
    mode: ConditionEvaluationMode,
    truth_policy: TruthPolicy,
}

impl TypedConditionRequest {
    pub(crate) fn from_parts(
        plan: DeferredConditionPlan,
        expression: Expr,
        route: PredicateRoute,
        safety: PredicateSafety,
        full_schema_width: usize,
        mode: ConditionEvaluationMode,
    ) -> Self {
        let truth_policy = match mode {
            ConditionEvaluationMode::OuterMatchStatus => TruthPolicy::TrackUnknown,
            ConditionEvaluationMode::ChildFilter | ConditionEvaluationMode::JoinFilter => {
                TruthPolicy::KeepTrueOnly
            }
        };
        Self {
            expression,
            plan,
            route,
            safety,
            full_schema_width,
            mode,
            truth_policy,
        }
    }

    /// Returns the original AST expression reserved for the typed evaluator.
    #[must_use]
    pub const fn expression(&self) -> &Expr {
        &self.expression
    }

    /// Returns the shape-only deferred condition plan.
    #[must_use]
    pub const fn plan(&self) -> &DeferredConditionPlan {
        &self.plan
    }

    /// Returns the conservative planner route that produced this request.
    #[must_use]
    pub const fn route(&self) -> PredicateRoute {
        self.route
    }

    /// Returns whether a typed effects/evaluator check remains mandatory.
    #[must_use]
    pub const fn safety(&self) -> PredicateSafety {
        self.safety
    }

    /// Returns the required source `FullSchema` row width.
    #[must_use]
    pub const fn full_schema_width(&self) -> usize {
        self.full_schema_width
    }

    /// Returns the executor-facing evaluation context.
    #[must_use]
    pub const fn mode(&self) -> ConditionEvaluationMode {
        self.mode
    }

    /// Returns whether UNKNOWN must be preserved by the eventual evaluator.
    #[must_use]
    pub const fn truth_policy(&self) -> TruthPolicy {
        self.truth_policy
    }
}
