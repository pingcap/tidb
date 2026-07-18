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

//! Dependency-closed projection-elimination metadata from
//! `pkg/planner/core/rule_eliminate_projection.go`.
//!
//! The source's loose logical projection predicate is intentionally small: a
//! projection may be removed only when it is not the special Expand projection
//! and every expression is a direct column reference.  This module keeps that
//! predicate over a typed expression-shape adapter; expression evaluation,
//! schema replacement, child rewrites, and physical projection handling stay
//! with the future planner integration.

/// The expression shapes relevant to the source loose-elimination predicate.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProjectionExprShape {
    /// A direct `expression.Column` reference.
    Column,
    /// Any computed, scalar, or otherwise non-column expression.
    Computed,
}

/// Source-shaped logical projection metadata.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LogicalProjectionShape {
    proj4_expand: bool,
    expressions: Vec<ProjectionExprShape>,
}

impl LogicalProjectionShape {
    /// Creates a projection metadata value from its Expand marker and
    /// expression shapes.
    #[must_use]
    pub fn new(proj4_expand: bool, expressions: Vec<ProjectionExprShape>) -> Self {
        Self {
            proj4_expand,
            expressions,
        }
    }

    /// Returns whether this projection was generated for an Expand operator.
    #[must_use]
    pub fn proj4_expand(&self) -> bool {
        self.proj4_expand
    }

    /// Returns the ordered expression shapes.
    #[must_use]
    pub fn expressions(&self) -> &[ProjectionExprShape] {
        &self.expressions
    }
}

/// Source-shaped projection elimination rule facade.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct ProjectionEliminator;

impl ProjectionEliminator {
    /// Evaluates the source loose logical projection predicate.
    #[must_use]
    pub fn can_eliminate_loose(self, projection: &LogicalProjectionShape) -> bool {
        can_eliminate_loose(projection)
    }

    /// Returns the source rule registry name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        "projection_eliminate"
    }
}

/// Returns whether a logical projection can be removed by the source loose
/// predicate from `canProjectionBeEliminatedLoose`.
#[must_use]
pub fn can_eliminate_loose(projection: &LogicalProjectionShape) -> bool {
    !projection.proj4_expand
        && projection
            .expressions
            .iter()
            .all(|expr| matches!(expr, ProjectionExprShape::Column))
}
