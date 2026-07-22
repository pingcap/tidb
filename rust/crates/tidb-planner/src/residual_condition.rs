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

//! Planner-owned shape metadata for residual boolean predicates.
//!
//! TiDB first separates direct join equalities from `OtherConditions`, then
//! keeps the remaining boolean expression as a residual predicate for the
//! executor.  This leaf preserves that shape without trying to evaluate a
//! value or to re-classify a non-equality as a hash-join key.  `AND`/`OR`/
//! `NOT` structure is retained, while scalar leaves carry only syntax-level
//! metadata and an explicit deferred-evaluation marker.

use tidb_ast::{BinaryOp, Expr, UnaryOp};

/// A residual predicate's boolean shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResidualPredicate {
    /// All children must be true.  This mirrors a source CNF conjunction but
    /// does not flatten nested expression nodes unless they are adjacent
    /// `AND`s in the input tree.
    All(Vec<Self>),
    /// At least one child may be true.
    Any(Vec<Self>),
    /// Logical negation.  Three-valued SQL truth remains the executor's job.
    Not(Box<Self>),
    /// A scalar leaf whose value evaluation is deferred to the typed
    /// expression owner.
    Leaf(ResidualLeaf),
    /// An AST shape this boundary cannot describe without inventing a value
    /// domain or evaluator.
    Unsupported(ResidualUnsupported),
}

/// Syntax-only metadata for one residual scalar expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResidualLeaf {
    kind: ResidualLeafKind,
    evaluation: ResidualEvaluation,
}

impl ResidualLeaf {
    fn deferred(kind: ResidualLeafKind) -> Self {
        Self {
            kind,
            evaluation: ResidualEvaluation::Deferred,
        }
    }

    /// Returns the syntax class of this residual leaf.
    #[must_use]
    pub fn kind(&self) -> &ResidualLeafKind {
        &self.kind
    }

    /// Returns the explicit execution status for the leaf.
    #[must_use]
    pub const fn evaluation(&self) -> ResidualEvaluation {
        self.evaluation
    }
}

/// Syntax classes that are safe to carry across the planner/executor seam
/// without evaluating a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResidualLeafKind {
    /// A non-boolean binary operator (comparison, arithmetic, bitwise, ...).
    Binary {
        /// The source operator.
        operator: BinaryOp,
        /// Shape-only metadata for the left operand.
        left: OperandShape,
        /// Shape-only metadata for the right operand.
        right: OperandShape,
    },
    /// A scalar builtin or generic function call.
    Function {
        /// The source function name, retaining its spelling where available.
        name: String,
        /// Number of source arguments.
        arity: usize,
    },
    /// A column or literal used directly as a predicate value.
    Scalar(OperandShape),
}

/// Shape metadata for an operand; no datum, coercion, or null inference is
/// stored here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperandShape {
    /// A column path, where `parts` is the number of qualifiers/name pieces.
    Column {
        /// Number of components in the source column path.
        parts: usize,
    },
    /// A literal or parameter-like scalar with no value copied into metadata.
    Constant,
    /// A nested expression that needs the typed expression evaluator.
    Nested,
    /// An explicit row/tuple expression.
    Row {
        /// Number of tuple elements.
        arity: usize,
    },
    /// A shape not yet represented by the bounded metadata contract.
    Opaque {
        /// Stable source-facing category name.
        category: &'static str,
    },
}

/// Evaluation is intentionally outside this module.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResidualEvaluation {
    /// The planner has retained shape only; a typed executor must evaluate it.
    Deferred,
}

/// Explicit unsupported shapes, never silently downgraded to a constant or
/// join key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResidualUnsupported {
    /// A unary operator other than logical NOT.
    UnaryOperator {
        /// The source unary operator.
        operator: UnaryOp,
    },
    /// A dedicated AST variant whose semantics are not represented here.
    AstVariant {
        /// Stable category of the variant.
        category: &'static str,
    },
}

/// Classifies one expression after direct join-key extraction.
///
/// This function never executes the expression and never decides whether a
/// binary equality is a join key.  Callers that already own that decision can
/// retain this tree as the residual `OtherConditions` metadata.
#[must_use]
pub fn classify_residual(expr: &Expr) -> ResidualPredicate {
    match strip_parens(expr) {
        Expr::ParamMarker { .. } => {
            ResidualPredicate::Unsupported(ResidualUnsupported::AstVariant {
                category: "param_marker",
            })
        }
        Expr::Binary(BinaryOp::LogicAnd, left, right) => {
            ResidualPredicate::All(vec![classify_residual(left), classify_residual(right)])
        }
        Expr::Binary(BinaryOp::LogicOr, left, right) => {
            ResidualPredicate::Any(vec![classify_residual(left), classify_residual(right)])
        }
        Expr::Unary(UnaryOp::Not | UnaryOp::NotKeyword, inner) => {
            ResidualPredicate::Not(Box::new(classify_residual(inner)))
        }
        Expr::Unary(operator, _) => {
            ResidualPredicate::Unsupported(ResidualUnsupported::UnaryOperator {
                operator: *operator,
            })
        }
        Expr::Binary(operator, left, right) => {
            ResidualPredicate::Leaf(ResidualLeaf::deferred(ResidualLeafKind::Binary {
                operator: *operator,
                left: operand_shape(left),
                right: operand_shape(right),
            }))
        }
        Expr::Func { name, args } | Expr::GenericFuncCall { name, args, .. } => {
            ResidualPredicate::Leaf(ResidualLeaf::deferred(ResidualLeafKind::Function {
                name: name.clone(),
                arity: args.len(),
            }))
        }
        Expr::Aggregate { name, args, .. } | Expr::Window { name, args, .. } => {
            ResidualPredicate::Leaf(ResidualLeaf::deferred(ResidualLeafKind::Function {
                name: name.clone(),
                arity: args.len(),
            }))
        }
        Expr::GroupConcat { args, .. } => {
            ResidualPredicate::Leaf(ResidualLeaf::deferred(ResidualLeafKind::Function {
                name: "GROUP_CONCAT".to_owned(),
                arity: args.len(),
            }))
        }
        Expr::Column(path) => ResidualPredicate::Leaf(ResidualLeaf::deferred(
            ResidualLeafKind::Scalar(OperandShape::Column { parts: path.len() }),
        )),
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
        | Expr::SysVar { .. } => ResidualPredicate::Leaf(ResidualLeaf::deferred(
            ResidualLeafKind::Scalar(OperandShape::Constant),
        )),
        Expr::Row(values) => ResidualPredicate::Leaf(ResidualLeaf::deferred(
            ResidualLeafKind::Scalar(OperandShape::Row {
                arity: values.len(),
            }),
        )),
        _ => ResidualPredicate::Unsupported(ResidualUnsupported::AstVariant {
            category: ast_variant_category(expr),
        }),
    }
}

fn operand_shape(expr: &Expr) -> OperandShape {
    match strip_parens(expr) {
        Expr::ParamMarker { .. } => OperandShape::Opaque {
            category: "param_marker",
        },
        Expr::Column(path) => OperandShape::Column { parts: path.len() },
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
        | Expr::SysVar { .. } => OperandShape::Constant,
        Expr::Row(values) => OperandShape::Row {
            arity: values.len(),
        },
        Expr::Binary(..) | Expr::Unary(..) => OperandShape::Nested,
        _ => OperandShape::Opaque {
            category: ast_variant_category(expr),
        },
    }
}

fn strip_parens(mut expr: &Expr) -> &Expr {
    while let Expr::Paren(inner) = expr {
        expr = inner;
    }
    expr
}

fn ast_variant_category(expr: &Expr) -> &'static str {
    match expr {
        Expr::ParamMarker { .. } => "param_marker",
        Expr::Default(_) => "default",
        Expr::Assign { .. } => "assign",
        Expr::GroupConcat { .. } => "group_concat",
        Expr::Window { .. } => "window",
        Expr::Interval { .. } => "interval",
        Expr::Extract { .. } => "extract",
        Expr::Position { .. } => "position",
        Expr::WeightString { .. } => "weight_string",
        Expr::Trim { .. } => "trim",
        Expr::TimestampAdd { .. } => "timestamp_add",
        Expr::TimestampDiff { .. } => "timestamp_diff",
        Expr::GetFormat { .. } => "get_format",
        Expr::In { .. } => "in",
        Expr::Between { .. } => "between",
        Expr::Like { .. } => "like",
        Expr::Regexp { .. } => "regexp",
        Expr::Is { .. } => "is",
        Expr::Cast(_) => "cast",
        Expr::ConvertUsing { .. } => "convert_using",
        Expr::Collate { .. } => "collate",
        Expr::Case { .. } => "case",
        Expr::Subquery(_) => "subquery",
        Expr::Exists { .. } => "exists",
        Expr::InSubquery { .. } => "in_subquery",
        Expr::CompareSubquery { .. } => "compare_subquery",
        Expr::MatchAgainst { .. } => "match_against",
        Expr::MemberOf { .. } => "member_of",
        Expr::Row(_) | Expr::Paren(_) => "scalar",
        Expr::Column(_)
        | Expr::Int(_)
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
        | Expr::SysVar { .. }
        | Expr::Unary(..)
        | Expr::Binary(..)
        | Expr::Func { .. }
        | Expr::GenericFuncCall { .. }
        | Expr::Aggregate { .. } => "scalar",
    }
}
