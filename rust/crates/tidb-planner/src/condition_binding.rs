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

//! Deferred residual-condition bindings for the planner/executor seam.
//!
//! TiDB's expression rewriter keeps `OtherConditions` separate from direct
//! join keys.  [`crate::residual_condition`] already preserves that boolean
//! shape, while [`crate::join_condition`] owns direct equality classification.
//! This module is the narrow hand-off between those two boundaries: it binds
//! column references in the *known* scalar shapes to the planner's flattened
//! `FullSchema` indices and records that value evaluation remains deferred.
//!
//! Dedicated expression variants whose nested fields are not yet represented
//! by this bounded walker are reported as opaque metadata.  They are never
//! guessed as constants, join keys, or evaluated here.

use tidb_ast::Expr;

use crate::join_condition::{BoundColumn, JoinSchema, UnsupportedJoinCondition};
use crate::residual_condition::ResidualPredicate;

/// The only execution state this planner leaf can promise.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeferredEvaluation {
    /// A typed expression owner must evaluate the original expression.
    TypedExecutor,
}

/// A source column occurrence and its position in the join `FullSchema`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConditionBinding {
    path: Vec<String>,
    column: BoundColumn,
}

impl ConditionBinding {
    fn new(path: &[String], column: BoundColumn) -> Self {
        Self {
            path: path.to_vec(),
            column,
        }
    }

    /// Returns the source path exactly as supplied by the parser.
    #[must_use]
    pub fn path(&self) -> &[String] {
        &self.path
    }

    /// Returns the planner's child/local/full-schema binding.
    #[must_use]
    pub const fn column(&self) -> &BoundColumn {
        &self.column
    }
}

/// Shapes encountered by the bounded walker but not yet traversed for column
/// references.  The typed evaluator remains responsible for their semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpaqueConditionShape {
    /// A dedicated AST predicate/function variant (for example `IN`, `CASE`,
    /// a subquery, or a cast) needs its own typed traversal.
    AstVariant(&'static str),
}

/// A residual predicate plus source-column bindings for deferred evaluation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeferredConditionPlan {
    predicate: ResidualPredicate,
    bindings: Vec<ConditionBinding>,
    opaque_shapes: Vec<OpaqueConditionShape>,
    evaluation: DeferredEvaluation,
}

impl DeferredConditionPlan {
    /// Returns the shape-only residual predicate tree.
    #[must_use]
    pub const fn predicate(&self) -> &ResidualPredicate {
        &self.predicate
    }

    /// Returns column occurrences in deterministic source traversal order.
    #[must_use]
    pub fn bindings(&self) -> &[ConditionBinding] {
        &self.bindings
    }

    /// Returns dedicated shapes that still require a typed traversal owner.
    #[must_use]
    pub fn opaque_shapes(&self) -> &[OpaqueConditionShape] {
        &self.opaque_shapes
    }

    /// Returns the explicit deferred execution status.
    #[must_use]
    pub const fn evaluation(&self) -> DeferredEvaluation {
        self.evaluation
    }
}

/// Errors at the planner/full-schema name-resolution boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConditionBindingError {
    /// The parser produced an empty column path.
    InvalidColumnPath,
    /// No planner column matched a source path.
    UnknownColumn {
        /// The unresolved source column path.
        path: Vec<String>,
    },
    /// More than one planner column matched a source path.
    AmbiguousColumn {
        /// The source column path with multiple matches.
        path: Vec<String>,
    },
    /// A prepared marker reached a generic residual path before its typed
    /// prepared-statement owner bound an execute value.
    UnboundParameterMarker {
        /// Statement-local parameter marker position.
        position: usize,
    },
}

impl From<UnsupportedJoinCondition> for ConditionBindingError {
    fn from(value: UnsupportedJoinCondition) -> Self {
        match value {
            UnsupportedJoinCondition::InvalidColumnPath => Self::InvalidColumnPath,
            UnsupportedJoinCondition::UnknownColumn { path } => Self::UnknownColumn { path },
            UnsupportedJoinCondition::AmbiguousColumn { path } => Self::AmbiguousColumn { path },
            // The schema resolver only returns the three lookup errors above
            // for a direct path.  Keep any future expansion explicit instead
            // of manufacturing a binding from an unrelated error category.
            other => Self::UnknownColumn {
                path: vec![format!("<unsupported:{other:?}>")],
            },
        }
    }
}

/// Binds a residual expression to a planner `FullSchema` without evaluating
/// it.  Known scalar/container shapes are traversed; dedicated variants are
/// retained as opaque gaps for the eventual typed evaluator.
pub fn bind_residual(
    expr: &Expr,
    schema: &JoinSchema,
) -> Result<DeferredConditionPlan, ConditionBindingError> {
    let predicate = crate::residual_condition::classify_residual(expr);
    let mut bindings = Vec::new();
    let mut opaque_shapes = Vec::new();
    collect_known_columns(expr, schema, &mut bindings, &mut opaque_shapes)?;
    Ok(DeferredConditionPlan {
        predicate,
        bindings,
        opaque_shapes,
        evaluation: DeferredEvaluation::TypedExecutor,
    })
}

fn collect_known_columns(
    expr: &Expr,
    schema: &JoinSchema,
    bindings: &mut Vec<ConditionBinding>,
    opaque_shapes: &mut Vec<OpaqueConditionShape>,
) -> Result<(), ConditionBindingError> {
    match expr {
        Expr::ParamMarker { position } => {
            return Err(ConditionBindingError::UnboundParameterMarker {
                position: *position,
            });
        }
        Expr::Column(path) => {
            let column = schema
                .bind_column_path(path)
                .map_err(ConditionBindingError::from)?;
            bindings.push(ConditionBinding::new(path, column));
        }
        Expr::Binary(_, left, right) => {
            collect_known_columns(left, schema, bindings, opaque_shapes)?;
            collect_known_columns(right, schema, bindings, opaque_shapes)?;
        }
        Expr::Unary(_, inner) | Expr::Paren(inner) => {
            collect_known_columns(inner, schema, bindings, opaque_shapes)?;
        }
        Expr::Row(values) => {
            for value in values {
                collect_known_columns(value, schema, bindings, opaque_shapes)?;
            }
        }
        Expr::Func { args, .. }
        | Expr::GenericFuncCall { args, .. }
        | Expr::Aggregate { args, .. }
        | Expr::Window { args, .. }
        | Expr::GroupConcat { args, .. } => {
            for arg in args {
                collect_known_columns(arg, schema, bindings, opaque_shapes)?;
            }
        }
        Expr::In { expr, list, .. } => {
            collect_known_columns(expr, schema, bindings, opaque_shapes)?;
            for value in list {
                collect_known_columns(value, schema, bindings, opaque_shapes)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_known_columns(expr, schema, bindings, opaque_shapes)?;
            collect_known_columns(low, schema, bindings, opaque_shapes)?;
            collect_known_columns(high, schema, bindings, opaque_shapes)?;
        }
        Expr::Like { expr, pattern, .. } | Expr::Regexp { expr, pattern, .. } => {
            collect_known_columns(expr, schema, bindings, opaque_shapes)?;
            collect_known_columns(pattern, schema, bindings, opaque_shapes)?;
        }
        Expr::Is { expr, .. }
        | Expr::Cast(tidb_ast::CastExpr { expr, .. })
        | Expr::ConvertUsing { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::Extract { value: expr, .. }
        | Expr::Assign { value: expr, .. } => {
            collect_known_columns(expr, schema, bindings, opaque_shapes)?;
        }
        Expr::Position { substr, str } => {
            collect_known_columns(substr, schema, bindings, opaque_shapes)?;
            collect_known_columns(str, schema, bindings, opaque_shapes)?;
        }
        Expr::MemberOf { expr, array } => {
            collect_known_columns(expr, schema, bindings, opaque_shapes)?;
            collect_known_columns(array, schema, bindings, opaque_shapes)?;
            opaque_shapes.push(OpaqueConditionShape::AstVariant("member_of"));
        }
        Expr::Trim { expr, remstr, .. } => {
            collect_known_columns(expr, schema, bindings, opaque_shapes)?;
            if let Some(remstr) = remstr {
                collect_known_columns(remstr, schema, bindings, opaque_shapes)?;
            }
        }
        Expr::TimestampAdd { interval, expr, .. } => {
            collect_known_columns(interval, schema, bindings, opaque_shapes)?;
            collect_known_columns(expr, schema, bindings, opaque_shapes)?;
            opaque_shapes.push(OpaqueConditionShape::AstVariant("timestamp_add"));
        }
        Expr::TimestampDiff { expr1, expr2, .. } => {
            collect_known_columns(expr1, schema, bindings, opaque_shapes)?;
            collect_known_columns(expr2, schema, bindings, opaque_shapes)?;
            opaque_shapes.push(OpaqueConditionShape::AstVariant("timestamp_diff"));
        }
        Expr::Interval { value, .. } => {
            collect_known_columns(value, schema, bindings, opaque_shapes)?;
            opaque_shapes.push(OpaqueConditionShape::AstVariant("interval"));
        }
        Expr::WeightString { expr, .. } => {
            collect_known_columns(expr, schema, bindings, opaque_shapes)?;
            opaque_shapes.push(OpaqueConditionShape::AstVariant("weight_string"));
        }
        Expr::GetFormat { expr, .. } => {
            collect_known_columns(expr, schema, bindings, opaque_shapes)?;
            opaque_shapes.push(OpaqueConditionShape::AstVariant("get_format"));
        }
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            if let Some(value) = value {
                collect_known_columns(value, schema, bindings, opaque_shapes)?;
            }
            for (condition, result) in when_clauses {
                collect_known_columns(condition, schema, bindings, opaque_shapes)?;
                collect_known_columns(result, schema, bindings, opaque_shapes)?;
            }
            if let Some(else_clause) = else_clause {
                collect_known_columns(else_clause, schema, bindings, opaque_shapes)?;
            }
        }
        // Query-bearing nodes cannot be evaluated by the row-only join
        // executor. Keep them explicit so compilation rejects them before a
        // candidate row is touched.
        Expr::Subquery(_) => opaque_shapes.push(OpaqueConditionShape::AstVariant("subquery")),
        Expr::Exists { .. } => opaque_shapes.push(OpaqueConditionShape::AstVariant("exists")),
        Expr::InSubquery { expr, .. } => {
            collect_known_columns(expr, schema, bindings, opaque_shapes)?;
            opaque_shapes.push(OpaqueConditionShape::AstVariant("in_subquery"))
        }
        Expr::CompareSubquery { left, .. } => {
            collect_known_columns(left, schema, bindings, opaque_shapes)?;
            opaque_shapes.push(OpaqueConditionShape::AstVariant("compare_subquery"))
        }
        Expr::MatchAgainst {
            columns, against, ..
        } => {
            for path in columns {
                let column = schema
                    .bind_column_path(path)
                    .map_err(ConditionBindingError::from)?;
                bindings.push(ConditionBinding::new(path, column));
            }
            collect_known_columns(against, schema, bindings, opaque_shapes)?;
            opaque_shapes.push(OpaqueConditionShape::AstVariant("match_against"))
        }
        // Scalar literals and variables have no column references.
        Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Float(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::String(_)
        | Expr::RawString(_)
        | Expr::CharsetString { .. }
        | Expr::Null
        | Expr::Bool(_)
        | Expr::UserVar(_)
        | Expr::SysVar { .. } => {}
    }
    Ok(())
}
