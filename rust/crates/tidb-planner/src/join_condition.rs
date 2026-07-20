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

//! Source-shaped ON/USING binding for the planner/executor seam.
//!
//! Go's `LogicalJoin` keeps a join's full schema separate from its visible
//! output schema.  That distinction is what lets `USING` coalesce one key in
//! `SELECT *` while still allowing an upper expression to resolve either
//! side's redundant column.  The Rust executor already has a deliberately
//! narrower direct-equality leaf; this module owns the *planner* half of that
//! contract: name resolution, full-schema indices, and predicate shape.
//!
//! This is intentionally a classifier, not a join evaluator.  A caller can
//! dispatch [`JoinCondition::Equality`] to a typed expression implementation,
//! while every other shape remains explicit and is left to the eventual
//! generic expression owner.  In particular, a function or compound ON
//! expression must never be silently treated as an equi-join key.

use tidb_ast::{BinaryOp, Expr};

/// Which child of a join owns a bound column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JoinSide {
    /// The outer/left child.
    Left,
    /// The inner/right child.
    Right,
}

/// A planner-visible column declaration.
///
/// `qualifiers` contains table aliases and names under which the column can be
/// resolved.  The name matching contract is case-insensitive, as in TiDB's
/// resolver.  `nullable` is metadata only; this module does not evaluate a
/// row or infer nullability from a predicate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnSpec {
    name: String,
    qualifiers: Vec<String>,
    nullable: bool,
    unqualified_visible: bool,
}

impl ColumnSpec {
    /// Creates a nullable column reachable under one relation qualifier.
    #[must_use]
    pub fn new(name: impl Into<String>, qualifier: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            qualifiers: vec![qualifier.into()],
            nullable: true,
            unqualified_visible: true,
        }
    }

    /// Creates a column with explicit relation qualifiers and nullability.
    #[must_use]
    pub fn with_qualifiers(
        name: impl Into<String>,
        qualifiers: impl IntoIterator<Item = impl Into<String>>,
        nullable: bool,
    ) -> Self {
        Self {
            name: name.into(),
            qualifiers: qualifiers.into_iter().map(Into::into).collect(),
            nullable,
            unqualified_visible: true,
        }
    }

    /// Marks a physical redundant USING column as qualified-only.
    ///
    /// TiDB retains this column in `FullSchema` for `right.col`, while the
    /// coalesced visible column owns bare `col` and `SELECT *`.
    #[must_use]
    pub fn qualified_only(mut self) -> Self {
        self.unqualified_visible = false;
        self
    }

    /// Returns the visible column name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns aliases/table names under which the column is reachable.
    #[must_use]
    pub fn qualifiers(&self) -> &[String] {
        &self.qualifiers
    }

    /// Returns whether the source column is nullable.
    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

/// The source-shaped full schema of a binary join.
///
/// `full_columns()` is ordered `[left columns..., right columns...]`, matching
/// `LogicalJoin.FullSchema`.  The visible output schema may omit a redundant
/// right-side `USING` column; keeping this full ordering here means binding an
/// upper expression never loses that source column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinSchema {
    left: Vec<ColumnSpec>,
    right: Vec<ColumnSpec>,
}

impl JoinSchema {
    /// Creates a join schema with full-schema order `[left..., right...]`.
    #[must_use]
    pub fn new(
        left: impl IntoIterator<Item = ColumnSpec>,
        right: impl IntoIterator<Item = ColumnSpec>,
    ) -> Self {
        Self {
            left: left.into_iter().collect(),
            right: right.into_iter().collect(),
        }
    }

    /// Returns the left child columns in child order.
    #[must_use]
    pub fn left(&self) -> &[ColumnSpec] {
        &self.left
    }

    /// Returns the right child columns in child order.
    #[must_use]
    pub fn right(&self) -> &[ColumnSpec] {
        &self.right
    }

    /// Returns the source-shaped `FullSchema` in `[left..., right...]` order.
    pub fn full_columns(&self) -> impl Iterator<Item = &ColumnSpec> {
        self.left.iter().chain(self.right.iter())
    }

    /// Binds every name in a `USING` list to one left/right equality pair.
    ///
    /// A `USING` key is ordinary SQL equality, so its semantics are
    /// [`EqualitySemantics::ThreeValued`]: a `NULL` key does not match.  A
    /// missing or duplicate name is returned as an explicit unsupported
    /// condition rather than guessed or silently dropped.
    pub fn bind_using<I, S>(&self, names: I) -> Vec<JoinCondition>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        names
            .into_iter()
            .map(|name| {
                let name = name.into();
                let left = self.lookup_side(&name, JoinSide::Left, None);
                let right = self.lookup_side(&name, JoinSide::Right, None);
                match (left, right) {
                    (Ok(left), Ok(right)) => JoinCondition::Equality(JoinEquality {
                        left,
                        right,
                        semantics: EqualitySemantics::ThreeValued,
                    }),
                    (Err(left), _) => JoinCondition::Unsupported(left.into_using(name)),
                    (_, Err(right)) => JoinCondition::Unsupported(right.into_using(name)),
                }
            })
            .collect()
    }

    /// Classifies an `ON` expression into a typed column equality or an
    /// explicit unsupported shape.
    ///
    /// Parentheses and either operand order are accepted.  Unqualified names
    /// are resolved against both children and must be globally unique; a name
    /// present on both sides is therefore reported as ambiguous, matching the
    /// planner's resolver boundary.  Only `=` and `<=>` are classified as
    /// equalities.  Functions, literals, same-side comparisons, and compound
    /// expressions remain visible to the generic expression path.
    #[must_use]
    pub fn classify_on(&self, expr: &Expr) -> JoinCondition {
        let expr = strip_parens(expr);
        let Expr::Binary(operator, lhs, rhs) = expr else {
            return JoinCondition::Unsupported(unsupported_shape(expr));
        };

        let semantics = match operator {
            BinaryOp::Eq => EqualitySemantics::ThreeValued,
            BinaryOp::NullEq => EqualitySemantics::NullSafe,
            operator => {
                return JoinCondition::Unsupported(UnsupportedJoinCondition::Compound {
                    operator: *operator,
                });
            }
        };

        let left = match self.bind_operand(lhs) {
            Ok(column) => column,
            Err(reason) => return JoinCondition::Unsupported(reason),
        };
        let right = match self.bind_operand(rhs) {
            Ok(column) => column,
            Err(reason) => return JoinCondition::Unsupported(reason),
        };

        match (left.side, right.side) {
            (JoinSide::Left, JoinSide::Right) => JoinCondition::Equality(JoinEquality {
                left,
                right,
                semantics,
            }),
            (JoinSide::Right, JoinSide::Left) => JoinCondition::Equality(JoinEquality {
                left: right,
                right: left,
                semantics,
            }),
            (side, _) => JoinCondition::Unsupported(UnsupportedJoinCondition::SameSide { side }),
        }
    }

    /// Binds one source column path against the planner's full join schema.
    ///
    /// This is the shared name-resolution seam for deferred conditions.  It
    /// returns the same [`BoundColumn`] used by direct join-key extraction,
    /// including the child-local and flattened `FullSchema` indices; it does
    /// not evaluate a value or decide whether the reference is a join key.
    pub fn bind_column_path(
        &self,
        path: &[String],
    ) -> Result<BoundColumn, UnsupportedJoinCondition> {
        let Some(name) = path.last() else {
            return Err(UnsupportedJoinCondition::InvalidColumnPath);
        };
        let qualifier = (path.len() >= 2).then(|| path[path.len() - 2].as_str());
        self.resolve_column(name, qualifier)
    }

    fn bind_operand(&self, expr: &Expr) -> Result<BoundColumn, UnsupportedJoinCondition> {
        let Expr::Column(path) = strip_parens(expr) else {
            return Err(unsupported_shape(expr));
        };
        let Some(name) = path.last() else {
            return Err(UnsupportedJoinCondition::InvalidColumnPath);
        };
        let qualifier = if path.len() >= 2 {
            path.get(path.len() - 2).map(String::as_str)
        } else {
            None
        };
        self.resolve_column(name, qualifier)
    }

    fn resolve_column(
        &self,
        name: &str,
        qualifier: Option<&str>,
    ) -> Result<BoundColumn, UnsupportedJoinCondition> {
        let left = self.lookup_side(name, JoinSide::Left, qualifier);
        let right = self.lookup_side(name, JoinSide::Right, qualifier);
        if let (Ok(column), Err(UnsupportedJoinCondition::UnknownColumn { .. })) = (&left, &right) {
            return Ok(column.clone());
        }
        if let (Err(UnsupportedJoinCondition::UnknownColumn { .. }), Ok(column)) = (&left, &right) {
            return Ok(column.clone());
        }
        match (left, right) {
            (Ok(_), Ok(_)) => Err(UnsupportedJoinCondition::AmbiguousColumn {
                path: qualifier
                    .map(|qualifier| vec![qualifier.to_owned(), name.to_owned()])
                    .unwrap_or_else(|| vec![name.to_owned()]),
            }),
            (Err(left), Err(_)) | (Err(left), Ok(_)) => Err(left),
            (Ok(_), Err(right)) => Err(right),
        }
    }

    fn lookup_side(
        &self,
        name: &str,
        side: JoinSide,
        qualifier: Option<&str>,
    ) -> Result<BoundColumn, UnsupportedJoinCondition> {
        let columns = match side {
            JoinSide::Left => &self.left,
            JoinSide::Right => &self.right,
        };
        let mut found = None;
        for (index, column) in columns.iter().enumerate() {
            if !column.name.eq_ignore_ascii_case(name)
                || (qualifier.is_none() && !column.unqualified_visible)
                || qualifier.is_some_and(|qualifier| {
                    !column
                        .qualifiers
                        .iter()
                        .any(|candidate| candidate.eq_ignore_ascii_case(qualifier))
                })
            {
                continue;
            }
            if found.is_some() {
                return Err(UnsupportedJoinCondition::AmbiguousColumn {
                    path: qualifier
                        .map(|qualifier| vec![qualifier.to_owned(), name.to_owned()])
                        .unwrap_or_else(|| vec![name.to_owned()]),
                });
            }
            found = Some(index);
        }
        let Some(index) = found else {
            return Err(UnsupportedJoinCondition::UnknownColumn {
                path: qualifier
                    .map(|qualifier| vec![qualifier.to_owned(), name.to_owned()])
                    .unwrap_or_else(|| vec![name.to_owned()]),
            });
        };
        Ok(BoundColumn {
            side,
            side_index: index,
            full_index: match side {
                JoinSide::Left => index,
                JoinSide::Right => self.left.len() + index,
            },
            name: columns[index].name.clone(),
            nullable: columns[index].nullable,
        })
    }
}

/// A bound column, including its position in the source `FullSchema`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundColumn {
    side: JoinSide,
    side_index: usize,
    full_index: usize,
    name: String,
    nullable: bool,
}

impl BoundColumn {
    /// Returns the owning child side.
    #[must_use]
    pub const fn side(&self) -> JoinSide {
        self.side
    }

    /// Returns the zero-based index in the owning child schema.
    #[must_use]
    pub const fn side_index(&self) -> usize {
        self.side_index
    }

    /// Returns the zero-based index in `JoinSchema::full_columns()`.
    #[must_use]
    pub const fn full_index(&self) -> usize {
        self.full_index
    }

    /// Returns the source column name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns source nullability metadata.
    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

/// SQL equality's null behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EqualitySemantics {
    /// Ordinary `=`: any `NULL` operand produces `UNKNOWN` and does not match.
    ThreeValued,
    /// Null-safe `<=>`: two `NULL`s compare true, and exactly one `NULL`
    /// compares false.
    NullSafe,
}

/// A bound cross-side equality condition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinEquality {
    left: BoundColumn,
    right: BoundColumn,
    semantics: EqualitySemantics,
}

impl JoinEquality {
    /// Returns the normalized left-side column.
    #[must_use]
    pub const fn left(&self) -> &BoundColumn {
        &self.left
    }

    /// Returns the normalized right-side column.
    #[must_use]
    pub const fn right(&self) -> &BoundColumn {
        &self.right
    }

    /// Returns whether this uses ordinary or null-safe SQL equality.
    #[must_use]
    pub const fn semantics(&self) -> EqualitySemantics {
        self.semantics
    }

    /// Returns the truth result contributed by NULL operands alone.
    ///
    /// Non-NULL value comparison still belongs to the typed expression owner;
    /// this helper only records the SQL three-valued/null-safe boundary.
    #[must_use]
    pub const fn null_truth(&self, left_is_null: bool, right_is_null: bool) -> TruthValue {
        match self.semantics {
            EqualitySemantics::ThreeValued if left_is_null || right_is_null => TruthValue::Unknown,
            EqualitySemantics::NullSafe if left_is_null && right_is_null => TruthValue::True,
            EqualitySemantics::NullSafe if left_is_null || right_is_null => TruthValue::False,
            _ => TruthValue::DependsOnValues,
        }
    }
}

/// The planner-level truth outcome needed by an ON predicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TruthValue {
    /// Predicate is true.
    True,
    /// Predicate is false.
    False,
    /// Predicate is SQL `UNKNOWN` (usually a `NULL` result).
    Unknown,
    /// Both values are non-NULL; comparison belongs to the typed expression
    /// evaluator and cannot be decided from schema metadata alone.
    DependsOnValues,
}

/// Explicit shape/lookup boundaries left to the generic expression owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnsupportedJoinCondition {
    /// A non-equality binary expression (`AND`, `OR`, `<`, arithmetic, ...).
    Compound {
        /// The source binary operator.
        operator: BinaryOp,
    },
    /// A scalar function call in an ON condition.
    Function {
        /// The source function name.
        name: String,
    },
    /// An aggregate function in an ON condition.
    Aggregate {
        /// The source aggregate name.
        name: String,
    },
    /// An operand was not a simple column reference.
    NonColumnOperand,
    /// A comparison used columns from only one child.
    SameSide {
        /// The side containing both columns.
        side: JoinSide,
    },
    /// A column path was empty.
    InvalidColumnPath,
    /// No column matched the name/qualifier.
    UnknownColumn {
        /// The unresolved source path.
        path: Vec<String>,
    },
    /// More than one column matched the name/qualifier.
    AmbiguousColumn {
        /// The ambiguous source path.
        path: Vec<String>,
    },
    /// A parsed expression variant outside this classifier's scope.
    Other,
}

impl UnsupportedJoinCondition {
    fn into_using(self, name: String) -> Self {
        match self {
            Self::UnknownColumn { .. } => Self::UnknownColumn { path: vec![name] },
            Self::AmbiguousColumn { .. } => Self::AmbiguousColumn { path: vec![name] },
            other => other,
        }
    }
}

/// The result of classifying one ON or USING condition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinCondition {
    /// A directly bindable cross-side equality.
    Equality(JoinEquality),
    /// A shape that must stay on the generic expression path.
    Unsupported(UnsupportedJoinCondition),
}

fn strip_parens(mut expr: &Expr) -> &Expr {
    while let Expr::Paren(inner) = expr {
        expr = inner;
    }
    expr
}

fn unsupported_shape(expr: &Expr) -> UnsupportedJoinCondition {
    match strip_parens(expr) {
        Expr::Binary(operator, _, _) => UnsupportedJoinCondition::Compound {
            operator: *operator,
        },
        Expr::Func { name, .. } | Expr::GenericFuncCall { name, .. } => {
            UnsupportedJoinCondition::Function { name: name.clone() }
        }
        Expr::Aggregate { name, .. } => UnsupportedJoinCondition::Aggregate { name: name.clone() },
        Expr::GroupConcat { .. } | Expr::Window { .. } => UnsupportedJoinCondition::Aggregate {
            name: "WINDOW".to_owned(),
        },
        Expr::Column(_) => UnsupportedJoinCondition::NonColumnOperand,
        _ => UnsupportedJoinCondition::NonColumnOperand,
    }
}
