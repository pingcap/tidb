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

pub use crate::column::Column;
pub use crate::schema::{KeyInfo, Schema};

/// Type tag written as the first byte of a column's `HashCode`
/// (`pkg/expression/expression.go`, `columnFlag`). The other tags land with
/// their variants: `constantFlag = 0`, `scalarFunctionFlag = 3`.
pub(crate) const COLUMN_FLAG: u8 = 1;

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
/// A closed enum over the concrete node types. Only [`Column`](Expression::Column)
/// is populated so far; the remaining variants are added as the corresponding
/// nodes are ported (see the module docs).
#[derive(Clone, Debug)]
pub enum Expression {
    /// A column reference (Go `*Column`).
    Column(Column),
}

impl Expression {
    /// Go `Expression.HashCode`: the type-tagged canonical byte encoding used as
    /// a map/dedup key. Structural and context-free.
    pub fn hash_code(&mut self) -> &[u8] {
        match self {
            Expression::Column(c) => c.hash_code(),
        }
    }

    /// Go `Expression.IsCorrelated`.
    #[must_use]
    pub fn is_correlated(&self) -> bool {
        match self {
            Expression::Column(c) => c.is_correlated(),
        }
    }

    /// Go `Expression.ConstLevel`.
    #[must_use]
    pub fn const_level(&self) -> ConstLevel {
        match self {
            Expression::Column(c) => c.const_level(),
        }
    }

    /// Go `Expression.Equal` (without the `EvalContext`, which the ported
    /// variants do not need): structural/identity equality.
    #[must_use]
    pub fn equal(&self, other: &Expression) -> bool {
        match self {
            Expression::Column(c) => c.equal_column(other),
        }
    }

    /// Borrows the inner [`Column`] when this expression is a column reference.
    #[must_use]
    pub fn as_column(&self) -> Option<&Column> {
        match self {
            Expression::Column(c) => Some(c),
        }
    }
}
