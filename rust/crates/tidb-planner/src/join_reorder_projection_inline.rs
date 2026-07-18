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

//! Projection safety gates from
//! `pkg/planner/core/rule_join_reorder_projection_inline.go`.
//!
//! The source allows a projection to be inlined only when every expression is
//! built from Column/ScalarFunction/Constant nodes, references at least one
//! column, and contains no deferred, mutable, non-deterministic, or correlated
//! behavior.  This module keeps those recursive gates over a typed expression
//! shape; join-group leaf attribution, expression substitution, and optimizer
//! plan construction remain external.

/// Typed expression shapes understood by the projection-inline safety gate.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ProjectionInlineExpr {
    /// A direct column reference.
    Column,
    /// A constant, optionally wrapping a deferred expression.
    Constant {
        /// Whether the constant is deferred by the caller.
        deferred: bool,
    },
    /// A scalar function and its recursive arguments, with source effect
    /// metadata supplied by the caller.
    ScalarFunction {
        /// Recursive scalar-function arguments.
        args: Vec<Self>,
        /// Whether evaluation has mutable effects.
        mutable_effects: bool,
        /// Whether evaluation is non-deterministic.
        non_deterministic: bool,
        /// Whether evaluation references correlated outer state.
        correlated: bool,
    },
    /// An expression implementation outside the source-supported tree.
    Unsupported {
        /// Number of column references exposed by the unsupported node.
        referenced_columns: usize,
    },
}

impl ProjectionInlineExpr {
    /// Returns the number of column references visible to the source
    /// ExtractColumns gate.
    #[must_use]
    pub fn referenced_columns(&self) -> usize {
        match self {
            Self::Column => 1,
            Self::Constant { .. } => 0,
            Self::ScalarFunction { args, .. } => args.iter().map(Self::referenced_columns).sum(),
            Self::Unsupported { referenced_columns } => *referenced_columns,
        }
    }

    /// Returns whether this expression consists only of source-supported
    /// Column/ScalarFunction/Constant nodes.
    #[must_use]
    pub fn is_inlineable(&self) -> bool {
        match self {
            Self::Column => true,
            Self::Constant { deferred } => !deferred,
            Self::ScalarFunction { args, .. } => args.iter().all(Self::is_inlineable),
            Self::Unsupported { .. } => false,
        }
    }

    /// Returns whether this expression or a nested scalar function has a
    /// mutable side effect.
    #[must_use]
    pub fn has_mutable_effects(&self) -> bool {
        match self {
            Self::ScalarFunction {
                args,
                mutable_effects,
                ..
            } => *mutable_effects || args.iter().any(Self::has_mutable_effects),
            Self::Column | Self::Constant { .. } | Self::Unsupported { .. } => false,
        }
    }

    /// Returns whether this expression or a nested scalar function is
    /// non-deterministic.
    #[must_use]
    pub fn is_non_deterministic(&self) -> bool {
        match self {
            Self::ScalarFunction {
                args,
                non_deterministic,
                ..
            } => *non_deterministic || args.iter().any(Self::is_non_deterministic),
            Self::Column | Self::Constant { .. } | Self::Unsupported { .. } => false,
        }
    }

    /// Returns whether this expression or a nested scalar function is
    /// correlated.
    #[must_use]
    pub fn is_correlated(&self) -> bool {
        match self {
            Self::ScalarFunction {
                args, correlated, ..
            } => *correlated || args.iter().any(Self::is_correlated),
            Self::Column | Self::Constant { .. } | Self::Unsupported { .. } => false,
        }
    }
}

/// Logical projection metadata needed by `canInlineProjectionBasic`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProjectionInlineShape {
    proj4_expand: bool,
    expressions: Vec<ProjectionInlineExpr>,
}

impl ProjectionInlineShape {
    /// Creates projection metadata from the Expand marker and expressions.
    #[must_use]
    pub fn new(proj4_expand: bool, expressions: Vec<ProjectionInlineExpr>) -> Self {
        Self {
            proj4_expand,
            expressions,
        }
    }

    /// Returns the source Expand marker.
    #[must_use]
    pub fn proj4_expand(&self) -> bool {
        self.proj4_expand
    }

    /// Returns the projection expressions.
    #[must_use]
    pub fn expressions(&self) -> &[ProjectionInlineExpr] {
        &self.expressions
    }
}

/// Returns whether an expression is in the source-supported recursive tree.
#[must_use]
pub fn is_inlineable_projection_expr(expr: &ProjectionInlineExpr) -> bool {
    expr.is_inlineable()
}

/// Returns whether the projection passes the source basic safety gate.
#[must_use]
pub fn can_inline_projection_basic(projection: &ProjectionInlineShape) -> bool {
    if projection.proj4_expand {
        return false;
    }

    projection.expressions.iter().all(|expr| {
        expr.referenced_columns() != 0
            && is_inlineable_projection_expr(expr)
            && !expr.has_mutable_effects()
            && !expr.is_non_deterministic()
            && !expr.is_correlated()
    })
}
