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

//! `pkg/expression/scalar_function.go`: the `ScalarFunction` expression node.
//!
//! BRIDGE DECISION: Go keeps a `ScalarFunction`'s arguments and evaluation
//! behind a `Function builtinFunc` interface (implemented by hundreds of
//! per-signature structs). This port instead holds `args: Vec<Expression>`
//! directly on the node and identifies the function by name; the evaluation
//! dispatch (the `builtinFunc` `eval*` methods, keyed by `tipb.ScalarFuncSig`)
//! is a separate, larger unit built on `EvalContext`/`chunk.Row`.
//!
//! Ported: the struct and its argument-structural, context-free methods
//! (static type, args, correlation, the common `ReHashCode` path), and `Eval`
//! for the binary-operator functions (reusing the shared Datum operator
//! semantics). DEFERRED (documented): `Eval` for the non-operator builtins;
//! `Equal` (Go's
//! compares through the function's `equal(ctx, ...)`); `ConstLevel` (needs the
//! `unFoldableFunctions` catalog and extension-func detection); the `Grouping`
//! branch of `ReHashCode` (needs `BuiltinGroupingImplSig`); `CanonicalHashCode`;
//! per-signature collation; and `MemoryUsage`.

use crate::context::{Columns, EvalError};
use crate::expr_collation::CollationInfo;
use crate::expression::{ConstLevel, Expression, SCALAR_FUNCTION_FLAG};
use tidb_ast::{BinaryOp, CiString};
use tidb_chunk::row::Row;
use tidb_codec::encode_compact_bytes;
use tidb_datatype::{Datum, FieldType};

/// Maps a Go binary-operator scalar-function name (`pkg/parser/ast`) to a
/// [`BinaryOp`]. Returns `None` for any function that is not a binary operator.
fn binary_op_for_name(name: &str) -> Option<BinaryOp> {
    Some(match name {
        "plus" => BinaryOp::Plus,
        "minus" => BinaryOp::Minus,
        "mul" => BinaryOp::Mul,
        "div" => BinaryOp::Div,
        "intdiv" => BinaryOp::IntDiv,
        "mod" => BinaryOp::Mod,
        "bitand" => BinaryOp::BitAnd,
        "bitor" => BinaryOp::BitOr,
        "bitxor" => BinaryOp::BitXor,
        "leftshift" => BinaryOp::LeftShift,
        "rightshift" => BinaryOp::RightShift,
        "eq" => BinaryOp::Eq,
        "nulleq" => BinaryOp::NullEq,
        "ne" => BinaryOp::Ne,
        "lt" => BinaryOp::Lt,
        "le" => BinaryOp::Le,
        "gt" => BinaryOp::Gt,
        "ge" => BinaryOp::Ge,
        "and" => BinaryOp::LogicAnd,
        "or" => BinaryOp::LogicOr,
        "xor" => BinaryOp::LogicXor,
        _ => return None,
    })
}

/// Go `ScalarFunction`: the application of a built-in function to arguments.
#[derive(Clone, Debug, Default)]
pub struct ScalarFunction {
    /// Go `FuncName` (an `ast.CIStr`): the function's name.
    pub func_name: CiString,

    /// Go `RetType` (a `*types.FieldType`; `None` mirrors a nil pointer).
    pub ret_type: Option<FieldType>,

    /// The function arguments. In Go these live inside `Function.getArgs()`.
    pub args: Vec<Expression>,

    /// Lazily-filled `HashCode` cache (Go `hashcode`). Go also caches a
    /// `canonicalhashcode`; that field lands with `CanonicalHashCode`.
    hashcode: Vec<u8>,

    /// Go embedded collation state (via the `Function`'s `collationInfo`).
    pub collation: CollationInfo,
}

impl ScalarFunction {
    /// Builds a scalar-function node.
    #[must_use]
    pub fn new(func_name: CiString, ret_type: FieldType, args: Vec<Expression>) -> Self {
        ScalarFunction {
            func_name,
            ret_type: Some(ret_type),
            args,
            ..Default::default()
        }
    }

    /// Go `GetStaticType` / `GetType` (which ignores its `EvalContext`).
    #[must_use]
    pub fn get_static_type(&self) -> Option<&FieldType> {
        self.ret_type.as_ref()
    }

    /// Go `GetArgs`.
    #[must_use]
    pub fn get_args(&self) -> &[Expression] {
        &self.args
    }

    /// Go `IsCorrelated`: correlated iff any argument is correlated.
    #[must_use]
    pub fn is_correlated(&self) -> bool {
        self.args.iter().any(Expression::is_correlated)
    }

    /// Go `HashCode` (`ReHashCode`), cached on first call:
    /// `[scalarFunctionFlag, EncodeCompactBytes(FuncName.L), arg.HashCode()...]`,
    /// plus, for `cast`, a trailing byte for the target `EvalType`.
    ///
    /// DEFERRED: the `grouping` special case (needs `BuiltinGroupingImplSig`).
    /// A `grouping(...)` node therefore hashes without its grouping-mode/marks;
    /// no consumer relies on this yet, but it must be completed with that sig.
    pub fn hash_code(&mut self) -> &[u8] {
        if !self.hashcode.is_empty() {
            return &self.hashcode;
        }
        self.hashcode.push(SCALAR_FUNCTION_FLAG);
        encode_compact_bytes(&mut self.hashcode, self.func_name.lowercase().as_bytes());
        // Collect the args' hash codes first to avoid overlapping borrows.
        let arg_codes: Vec<Vec<u8>> = self
            .args
            .iter_mut()
            .map(|a| a.hash_code().to_vec())
            .collect();
        for code in arg_codes {
            self.hashcode.extend_from_slice(&code);
        }
        // Cast is special: its result type is effectively an argument.
        if self.func_name.lowercase() == "cast" {
            if let Some(rt) = &self.ret_type {
                self.hashcode.push(rt.eval_type() as u8);
            }
        }
        &self.hashcode
    }

    /// Go `ConstLevel`.
    ///
    /// DEFERRED to a conservative `ConstNone`: the faithful result needs the
    /// `unFoldableFunctions` catalog (non-deterministic builtins) and
    /// extension-function detection, then the min over argument levels. Reporting
    /// `ConstNone` is safe -- it only forgoes constant folding, never mis-folds a
    /// non-deterministic call -- until that catalog is ported. No wired consumer
    /// depends on scalar-function const-level yet.
    #[must_use]
    pub fn const_level(&self) -> ConstLevel {
        ConstLevel::NONE
    }

    /// Go `ScalarFunction.Eval`: evaluate the function against one row.
    ///
    /// The binary-operator functions (`plus`/`minus`/.../comparisons/logic) are
    /// supported by evaluating both arguments and reusing the shared Datum-level
    /// operator semantics ([`crate::apply_binary`]), which dispatch on the
    /// operand kinds exactly as Go's per-signature builtins do. Every other
    /// function is reported as unsupported until its builtin is ported.
    pub fn eval(&self, ctx: &impl Columns, row: Row<'_>) -> Result<Datum, EvalError> {
        if let Some(op) = binary_op_for_name(self.func_name.lowercase()) {
            if self.args.len() == 2 {
                let lhs = self.args[0].eval(ctx, row)?;
                let rhs = self.args[1].eval(ctx, row)?;
                return crate::apply_binary(op, lhs, rhs);
            }
        }
        Err(EvalError::Unsupported(
            "this scalar function is not yet ported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::constant::Constant;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    fn ft() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    fn plus(args: Vec<Expression>) -> ScalarFunction {
        ScalarFunction::new(CiString::new("plus"), ft(), args)
    }

    #[test]
    fn args_and_static_type() {
        let sf = plus(vec![
            Expression::Constant(Constant::new(Datum::Int(1), ft())),
            Expression::Constant(Constant::new(Datum::Int(2), ft())),
        ]);
        assert_eq!(sf.get_args().len(), 2);
        assert!(sf.get_static_type().is_some());
        assert_eq!(sf.const_level(), ConstLevel::NONE);
    }

    #[test]
    fn is_correlated_follows_args() {
        let plain = plus(vec![Expression::Column(Column::new(1, ft()))]);
        assert!(!plain.is_correlated());

        let corr = plus(vec![Expression::CorrelatedColumn(
            crate::column::CorrelatedColumn {
                column: Column::new(1, ft()),
                data: None,
            },
        )]);
        assert!(corr.is_correlated());
    }

    #[test]
    fn hash_code_is_flag_name_and_arg_codes() {
        let mut c1 = Column::new(1, ft());
        let mut c2 = Column::new(2, ft());
        let mut sf = plus(vec![
            Expression::Column(Column::new(1, ft())),
            Expression::Column(Column::new(2, ft())),
        ]);

        let mut expected = vec![SCALAR_FUNCTION_FLAG];
        encode_compact_bytes(&mut expected, b"plus");
        expected.extend_from_slice(c1.hash_code());
        expected.extend_from_slice(c2.hash_code());
        assert_eq!(sf.hash_code(), expected.as_slice());
        // Cached.
        assert_eq!(sf.hash_code(), expected.as_slice());
    }

    #[test]
    fn cast_hash_code_includes_eval_type_byte() {
        let mut sf = ScalarFunction::new(
            CiString::new("cast"),
            FieldType::new(FieldTypeCode::Long),
            vec![Expression::Column(Column::new(1, ft()))],
        );
        let hc = sf.hash_code().to_vec();
        // Ends with the target EvalType byte (Long -> Int == 0).
        assert_eq!(*hc.last().unwrap(), tidb_datatype::EvalType::Int as u8);
    }

    #[test]
    fn different_functions_hash_differently() {
        let mut a = plus(vec![Expression::Column(Column::new(1, ft()))]);
        let mut b = ScalarFunction::new(
            CiString::new("minus"),
            ft(),
            vec![Expression::Column(Column::new(1, ft()))],
        );
        assert_ne!(a.hash_code(), b.hash_code());
    }
}
