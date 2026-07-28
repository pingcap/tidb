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
//! for the binary- and unary-operator functions (reusing the shared Datum
//! operator semantics). DEFERRED (documented): `Eval` for the non-operator
//! builtins (cast, if/case, string/date functions, ...);
//! `Equal` (Go's
//! compares through the function's `equal(ctx, ...)`); `ConstLevel` (needs the
//! `unFoldableFunctions` catalog and extension-func detection); the `Grouping`
//! branch of `ReHashCode` (needs `BuiltinGroupingImplSig`); `CanonicalHashCode`;
//! per-signature collation; and `MemoryUsage`.

use crate::context::{Columns, EvalError};
use crate::expr_collation::CollationInfo;
use crate::expression::{ConstLevel, Expression, SCALAR_FUNCTION_FLAG};
use tidb_ast::{BinaryOp, CiString, UnaryOp};
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

/// Maps a Go unary-operator scalar-function name (`pkg/parser/ast`) to a
/// [`UnaryOp`]. Returns `None` for any function that is not a unary operator.
fn unary_op_for_name(name: &str) -> Option<UnaryOp> {
    Some(match name {
        "unaryplus" => UnaryOp::Plus,
        "unaryminus" => UnaryOp::Minus,
        "bitneg" => UnaryOp::BitNeg,
        "not" => UnaryOp::Not,
        _ => return None,
    })
}

/// The Go scalar-function name for a binary operator (inverse of
/// [`binary_op_for_name`]); used when building a [`ScalarFunction`] from an AST
/// operator.
#[must_use]
pub fn binary_op_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Plus => "plus",
        BinaryOp::Minus => "minus",
        BinaryOp::Mul => "mul",
        BinaryOp::Div => "div",
        BinaryOp::IntDiv => "intdiv",
        BinaryOp::Mod => "mod",
        BinaryOp::BitAnd => "bitand",
        BinaryOp::BitOr => "bitor",
        BinaryOp::BitXor => "bitxor",
        BinaryOp::LeftShift => "leftshift",
        BinaryOp::RightShift => "rightshift",
        BinaryOp::Eq => "eq",
        BinaryOp::NullEq => "nulleq",
        BinaryOp::Ne => "ne",
        BinaryOp::Lt => "lt",
        BinaryOp::Le => "le",
        BinaryOp::Gt => "gt",
        BinaryOp::Ge => "ge",
        BinaryOp::LogicAnd => "and",
        BinaryOp::LogicOr => "or",
        BinaryOp::LogicXor => "xor",
    }
}

/// The Go scalar-function name for a unary operator (inverse of
/// [`unary_op_for_name`]). `Not`/`NotKeyword` share the `not` function.
#[must_use]
pub fn unary_op_name(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Plus => "unaryplus",
        UnaryOp::Minus => "unaryminus",
        UnaryOp::BitNeg => "bitneg",
        UnaryOp::Not | UnaryOp::NotKeyword => "not",
    }
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
        let name = self.func_name.lowercase();
        if let Some(op) = binary_op_for_name(name) {
            if self.args.len() == 2 {
                let lhs = self.args[0].eval(ctx, row)?;
                let rhs = self.args[1].eval(ctx, row)?;
                // The statement context travels with the operands, so a
                // zero divisor reaches the same warning/error policy the AST
                // evaluator applies.
                return crate::apply_binary_with_div_precision(
                    op,
                    lhs,
                    rhs,
                    ctx.div_precision_increment(),
                    ctx,
                );
            }
        }
        if let Some(op) = unary_op_for_name(name) {
            if self.args.len() == 1 {
                let v = self.args[0].eval(ctx, row)?;
                return crate::apply_unary(op, v);
            }
        }
        // Go `builtinCaseWhen*Sig`: the arguments are the flattened
        // `cond, result, ..., else` list, and only the selected branch is
        // evaluated -- so an error in an unreachable branch never surfaces.
        if name == "case_when" {
            let mut pairs = self.args.chunks_exact(2);
            for pair in pairs.by_ref() {
                let condition = pair[0].eval(ctx, row)?;
                // A NULL condition is not a match, the same as false.
                if crate::truthy_of(&condition)?.unwrap_or(false) {
                    return pair[1].eval(ctx, row);
                }
            }
            // An odd argument count means a trailing ELSE.
            return match pairs.remainder().first() {
                Some(else_branch) => else_branch.eval(ctx, row),
                None => Ok(Datum::Null),
            };
        }
        // Go `builtinLikeSig`: both operands are stringified, NULL in either
        // propagates, and the third argument is the escape byte.
        if (name == "like" || name == "ilike") && self.args.len() == 3 {
            let value = self.args[0].eval(ctx, row)?;
            let pattern = self.args[1].eval(ctx, row)?;
            if value.is_null() || pattern.is_null() {
                return Ok(Datum::Null);
            }
            let escape = match self.args[2].eval(ctx, row)? {
                Datum::Int(byte) => u8::try_from(byte).ok(),
                _ => None,
            };
            let text = value
                .sql_string()
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 LIKE operand"))?;
            let pattern = pattern
                .sql_string()
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 LIKE pattern"))?;
            let matched = if name == "ilike" {
                crate::ilike_match(&text, &pattern, escape.unwrap_or(b'\\'))
            } else {
                crate::like_match_with_collation(
                    &text,
                    &pattern,
                    escape,
                    tidb_datatype::Collation::Utf8Mb4Bin,
                )
            };
            return Ok(Datum::Int(i64::from(matched)));
        }
        // Go picks one cast signature per target type; the rewriter records
        // that choice in the name, and the width/scale arguments the CHAR,
        // BINARY and DECIMAL casts need come from the result type.
        if let Some(target) = name.strip_prefix("cast_") {
            if self.args.len() == 1 {
                let value = self.args[0].eval(ctx, row)?;
                if value.is_null() {
                    return Ok(Datum::Null);
                }
                let ret_type = self
                    .get_static_type()
                    .ok_or(EvalError::Unsupported("a cast with no result type"))?;
                return crate::cast::eval_cast(&cast_type_of(target, ret_type)?, value);
            }
        }
        // Go `builtinDatabaseSig`/`builtinVersionSig` read session state
        // rather than arguments: the current database (NULL when none is
        // selected) and the same string `@@version` reports.
        if self.args.is_empty() {
            match name {
                "database" | "schema" => {
                    return Ok(match ctx.current_database() {
                        Some(name) => Datum::Bytes(name.into_bytes()),
                        None => Datum::Null,
                    })
                }
                "version" => {
                    return Ok(ctx.sysvar(None, "version").unwrap_or(Datum::Null));
                }
                _ => {}
            }
        }
        // Go picks a string-length signature from the ARGUMENT's type before
        // any value exists, which is what `build_string_length` models.
        if self.args.len() == 1 {
            let length = match name {
                "length" | "octet_length" => Some(crate::StringLengthFunction::Length),
                "char_length" | "character_length" => Some(crate::StringLengthFunction::CharLength),
                _ => None,
            };
            if let Some(function) = length {
                let argument_type = self.args[0].static_type().cloned().unwrap_or_else(|| {
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Null)
                });
                let built =
                    crate::BuildContext::default().build_string_length(function, argument_type);
                return built.eval(&self.args[0].eval(ctx, row)?);
            }
        }
        // Values-only builtins (ABS/CONCAT/COALESCE/...): evaluate every
        // argument, then reuse the single Datum-level implementation shared
        // with the AST evaluator (`crate::func::eval_func_values`). Lazy
        // control forms (IF/CASE), session-state functions, and the
        // AST-typed LENGTH family are not in that entry and stay unsupported
        // here until their chunk-row builtins are ported.
        let vals: Vec<Datum> = self
            .args
            .iter()
            .map(|a| a.eval(ctx, row))
            .collect::<Result<_, _>>()?;
        if let Some(result) = crate::func::eval_func_values(&name.to_ascii_uppercase(), &vals) {
            return result;
        }
        Err(EvalError::Unsupported(
            "this scalar function is not yet ported",
        ))
    }
}

/// The cast a `cast_*` function name describes, with the width and scale its
/// result type carries. Go stores the same fact as the chosen
/// `builtinCast*As*Sig`.
fn cast_type_of(target: &str, ret_type: &FieldType) -> Result<tidb_ast::CastType, EvalError> {
    use tidb_ast::CastType;
    let len = || u32::try_from(ret_type.flen()).ok();
    Ok(match target {
        "signed" => CastType::Signed,
        "unsigned" => CastType::Unsigned,
        "char" => CastType::Char {
            len: len(),
            charset: None,
        },
        "binary" => CastType::Binary { len: len() },
        "decimal" => CastType::Decimal {
            flen: u32::try_from(ret_type.flen()).unwrap_or(0),
            scale: u32::try_from(ret_type.decimal()).unwrap_or(0),
        },
        "date" => CastType::Date,
        "datetime" => CastType::DateTime { fsp: None },
        "year" => CastType::Year,
        "double" => CastType::Double,
        _ => return Err(EvalError::Unsupported("this cast target is not ported")),
    })
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
    fn eval_bridges_values_only_builtins() {
        use crate::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let chk = Chunk::new_with_capacity(std::slice::from_ref(&ft()), 1);
        let mut chk = chk;
        chk.append_int64(0, 0);
        let row = chk.get_row(0);
        let konst = |d: Datum| Expression::Constant(Constant::new(d, ft()));

        // ABS(-5) = 5 via math_fn's shared values-only dispatch.
        let abs = ScalarFunction::new(CiString::new("abs"), ft(), vec![konst(Datum::Int(-5))]);
        assert_eq!(abs.eval(&NoColumns, row).unwrap(), Datum::Int(5));

        // CONCAT('a', 'b') = 'ab' via the shared string arm.
        let concat = ScalarFunction::new(
            CiString::new("concat"),
            ft(),
            vec![konst(Datum::new_string("a")), konst(Datum::new_string("b"))],
        );
        assert_eq!(
            concat.eval(&NoColumns, row).unwrap(),
            Datum::new_string("ab")
        );

        // COALESCE(NULL, 7) = 7 (eager over values, matching the old
        // AST-level evaluator).
        let coalesce = ScalarFunction::new(
            CiString::new("coalesce"),
            ft(),
            vec![konst(Datum::Null), konst(Datum::Int(7))],
        );
        assert_eq!(coalesce.eval(&NoColumns, row).unwrap(), Datum::Int(7));

        // A column argument feeds the builtin from the chunk row: ABS(col).
        let mut col = Column::new(1, ft());
        col.index = 0;
        let mut chk2 = Chunk::new_with_capacity(std::slice::from_ref(&ft()), 1);
        chk2.append_int64(0, -9);
        let abs_col =
            ScalarFunction::new(CiString::new("abs"), ft(), vec![Expression::Column(col)]);
        assert_eq!(
            abs_col.eval(&NoColumns, chk2.get_row(0)).unwrap(),
            Datum::Int(9)
        );

        // A function outside the values-only entry stays unsupported.
        let iff = ScalarFunction::new(
            CiString::new("if"),
            ft(),
            vec![
                konst(Datum::Int(1)),
                konst(Datum::Int(2)),
                konst(Datum::Int(3)),
            ],
        );
        assert!(matches!(
            iff.eval(&NoColumns, row),
            Err(EvalError::Unsupported(_))
        ));
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
