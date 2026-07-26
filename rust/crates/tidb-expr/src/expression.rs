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

//! The `pkg/expression` node hierarchy spine (from `expression.go`).
//!
//! DESIGN DECISION: Go's `Expression` is an interface implemented by a *closed*
//! set of node types (`Column`, `Constant`, `ScalarFunction`,
//! `CorrelatedColumn`), and callers pervasively type-switch (`expr.(*Column)`).
//! The faithful, idiomatic Rust model is therefore an **enum**, not a
//! `Box<dyn Expression>`: matching replaces the type-switches, and `Clone`/`Eq`
//! are cheap and structural.
//!
//! SEED SCOPE (grown incrementally, like `meta/model` was): the [`Column`]
//! variant plus [`Schema`] are ported here (the type every plan node exposes).
//! DEFERRED variants and behavior: `Constant`, `ScalarFunction`,
//! `CorrelatedColumn`, and the ~30 `Eval*`/`GetType(ctx)`/`ResolveIndices`/
//! `ExplainInfo` methods of the interface (they need `EvalContext`,
//! `chunk.Row`, and the `builtinFunc` dispatch). Structural, context-free
//! methods (identity, hash code, const-level) are ported now.

pub use crate::column::{Column, CorrelatedColumn};
pub use crate::constant::{Constant, ParamMarker};
pub use crate::scalar_function::ScalarFunction;
pub use crate::schema::{KeyInfo, Schema};

// Type tags written as the first byte of an expression `HashCode`
// (`pkg/expression/expression.go`).
pub(crate) const CONSTANT_FLAG: u8 = 0;
pub(crate) const COLUMN_FLAG: u8 = 1;
pub(crate) const SCALAR_FUNCTION_FLAG: u8 = 3;
pub(crate) const PARAMETER_FLAG: u8 = 4;

/// Go `ConstLevel` (a `uint`): how constant an expression is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConstLevel(pub u32);

impl ConstLevel {
    /// Not a constant; may differ per input row (Go `ConstNone`, the zero value).
    pub const NONE: ConstLevel = ConstLevel(0);
    /// Constant only within one context/execution, e.g. a plan-cache `?`
    /// placeholder (Go `ConstOnlyInContext`).
    pub const ONLY_IN_CONTEXT: ConstLevel = ConstLevel(1);
    /// Always the same regardless of context or row (Go `ConstStrict`).
    pub const STRICT: ConstLevel = ConstLevel(2);
}

/// Go `Expression`: a scalar expression node.
///
/// A closed enum over the concrete node types: [`Column`](Expression::Column),
/// [`Constant`](Expression::Constant),
/// [`CorrelatedColumn`](Expression::CorrelatedColumn), and
/// [`ScalarFunction`](Expression::ScalarFunction) -- the full Go variant set.
#[derive(Clone, Debug)]
pub enum Expression {
    /// A column reference (Go `*Column`).
    Column(Column),
    /// A literal / deferred / parameter constant (Go `*Constant`).
    Constant(Constant),
    /// A column bound to an outer query's value (Go `*CorrelatedColumn`).
    CorrelatedColumn(CorrelatedColumn),
    /// A built-in function applied to arguments (Go `*ScalarFunction`).
    ScalarFunction(ScalarFunction),
}

impl Expression {
    /// Go `Expression.HashCode`: the type-tagged canonical byte encoding used as
    /// a map/dedup key. Structural and context-free.
    pub fn hash_code(&mut self) -> &[u8] {
        match self {
            Expression::Column(c) => c.hash_code(),
            Expression::Constant(c) => c.hash_code(),
            Expression::CorrelatedColumn(c) => c.hash_code(),
            Expression::ScalarFunction(c) => c.hash_code(),
        }
    }

    /// Go `Expression.IsCorrelated`.
    #[must_use]
    pub fn is_correlated(&self) -> bool {
        match self {
            Expression::Column(c) => c.is_correlated(),
            Expression::Constant(c) => c.is_correlated(),
            Expression::CorrelatedColumn(c) => c.is_correlated(),
            Expression::ScalarFunction(c) => c.is_correlated(),
        }
    }

    /// Go `Expression.ConstLevel`.
    #[must_use]
    pub fn const_level(&self) -> ConstLevel {
        match self {
            Expression::Column(c) => c.const_level(),
            Expression::Constant(c) => c.const_level(),
            Expression::CorrelatedColumn(c) => c.const_level(),
            Expression::ScalarFunction(c) => c.const_level(),
        }
    }

    /// Context-free identity equality.
    ///
    /// This is only the part of Go's `Expression.Equal(ctx, e)` that needs no
    /// `EvalContext`: columns are equal by `UniqueID`. `Constant.Equal` compares
    /// evaluated values through a collator and `ScalarFunction.Equal` compares
    /// through the function's `equal(ctx, ...)`, so neither can be answered
    /// without a context; both conservatively report `false` here and gain a
    /// faithful context-aware form when `Eval*` is ported. No wired consumer
    /// relies on constant/function equality yet.
    #[must_use]
    pub fn equal(&self, other: &Expression) -> bool {
        match self {
            Expression::Column(c) => c.equal_column(other),
            Expression::CorrelatedColumn(c) => c.equal_column(other),
            Expression::Constant(_) | Expression::ScalarFunction(_) => false,
        }
    }

    /// Go `Expression.Eval(ctx, row)`: evaluate this expression against one row.
    ///
    /// The [`Columns`](crate::context::Columns) context stands in for Go's
    /// richer `EvalContext`; the currently ported variants (`Column`,
    /// `Constant`, `CorrelatedColumn`) do not read it. `ScalarFunction`
    /// evaluation (routing arguments through the builtin dispatch) is the next
    /// unit and is reported as unsupported until then.
    pub fn eval(
        &self,
        ctx: &impl crate::context::Columns,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<tidb_datatype::Datum, crate::context::EvalError> {
        match self {
            Expression::Column(c) => c.eval(row),
            Expression::Constant(c) => c.eval(),
            Expression::CorrelatedColumn(c) => Ok(c.eval()),
            Expression::ScalarFunction(c) => c.eval(ctx, row),
        }
    }

    /// Borrows the inner [`Column`] when this expression is a column reference.
    #[must_use]
    pub fn as_column(&self) -> Option<&Column> {
        match self {
            Expression::Column(c) => Some(c),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constant::Constant;
    use crate::context::NoColumns;
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    #[test]
    fn eval_constant_column_and_unsupported() {
        let ft = FieldType::new(FieldTypeCode::Long);
        let mut chk = Chunk::new_with_capacity(std::slice::from_ref(&ft), 1);
        chk.append_int64(0, 99);
        let row = chk.get_row(0);

        // A literal constant evaluates to its value, ignoring the row.
        let konst = Expression::Constant(Constant::new(Datum::Int(7), ft.clone()));
        assert_eq!(konst.eval(&NoColumns, row).unwrap(), Datum::Int(7));

        // A column reads its cell from the row at its Index.
        let mut col = Column::new(1, ft.clone());
        col.index = 0;
        let col_expr = Expression::Column(col);
        assert_eq!(col_expr.eval(&NoColumns, row).unwrap(), Datum::Int(99));

        // A binary-operator scalar function evaluates its arguments: 1 + 1 = 2.
        let one = || Expression::Constant(Constant::new(Datum::Int(1), ft.clone()));
        let plus = Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("plus"),
            ft.clone(),
            vec![one(), one()],
        ));
        assert_eq!(plus.eval(&NoColumns, row).unwrap(), Datum::Int(2));

        // Nested: (1 + 1) + 1 = 3.
        let nested = Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("plus"),
            ft.clone(),
            vec![plus, one()],
        ));
        assert_eq!(nested.eval(&NoColumns, row).unwrap(), Datum::Int(3));

        // A unary operator: -(1 + 1) = -2.
        let plus2 = Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("plus"),
            ft.clone(),
            vec![one(), one()],
        ));
        let neg = Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("unaryminus"),
            ft.clone(),
            vec![plus2],
        ));
        assert_eq!(neg.eval(&NoColumns, row).unwrap(), Datum::Int(-2));

        // A non-operator function is still unsupported.
        let unknown = Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("some_udf"),
            ft.clone(),
            vec![one()],
        ));
        assert!(unknown.eval(&NoColumns, row).is_err());
    }
}
