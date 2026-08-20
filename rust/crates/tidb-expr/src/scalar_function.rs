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
//! Ported: the struct and its argument-structural methods, const-level rules,
//! the common `ReHashCode` path, and evaluation for operators plus the builtin
//! families owned by the shared dispatch modules. Unknown builtin names fail
//! explicitly. Remaining structural gaps are `Equal` (Go's
//! compares through the function's `equal(ctx, ...)`); the `Grouping`
//! branch of `ReHashCode` (needs `BuiltinGroupingImplSig`); `CanonicalHashCode`;
//! per-signature collation; and `MemoryUsage`.

use crate::context::{Columns, EvalError};
use crate::expr_collation::CollationInfo;
use crate::expression::{ConstLevel, Expression, SCALAR_FUNCTION_FLAG};
use tidb_ast::{BinaryOp, CiString, UnaryOp};
use tidb_chunk::row::Row;
use tidb_codec::encode_compact_bytes;
use tidb_datatype::{Datum, FieldType};

const MAX_ADVISORY_LOCK_TIMEOUT_SECS: i64 = 1_073_741_824;

fn advisory_lock_name(value: Datum) -> Result<String, EvalError> {
    let bytes = crate::arg_eval_type::eval_string(&value)?;
    let Some(bytes) = bytes else {
        return Err(EvalError::AdvisoryLock {
            code: 3057,
            message: "Incorrect user-level lock name 'NULL'.".to_owned(),
        });
    };
    let text = tidb_datatype::GoString::from_bytes(bytes).to_utf8_lossy_go();
    if text.is_empty() || text.chars().count() > 64 {
        return Err(EvalError::AdvisoryLock {
            code: 3057,
            message: format!("Incorrect user-level lock name '{text}'."),
        });
    }
    let normalized = tidb_mysql::to_lowercase(&text);
    if normalized.chars().count() > 64 {
        return Err(EvalError::IncorrectArguments(
            "Incorrect arguments to get_lock".to_owned(),
        ));
    }
    Ok(normalized)
}

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

/// The eval-type family a [`Datum`] belongs to -- the inverse of the switch
/// Go's `ScalarFunction.Eval` performs on `RetType.EvalType()`. `None` is a
/// kind no eval type names (NULL, the range sentinels, an undecoded `Raw`).
///
/// `Datum::Time` is the value domain of BOTH Go's `ETDatetime` and
/// `ETTimestamp`, so it reports `Datetime` and a timestamp result type is
/// normalized onto it by [`same_eval_family`].
fn datum_eval_type(value: &Datum) -> Option<tidb_datatype::EvalType> {
    use tidb_datatype::EvalType;
    Some(match value {
        Datum::Int(_) | Datum::UInt(_) => EvalType::Int,
        Datum::Real(_) | Datum::Float32(_) => EvalType::Real,
        Datum::Decimal(_) => EvalType::Decimal,
        Datum::String(_)
        | Datum::Bytes(_)
        | Datum::BinaryLiteral(_)
        | Datum::Bit(_)
        | Datum::Enum(..)
        | Datum::Set(..) => EvalType::String,
        Datum::Time(_) => EvalType::Datetime,
        Datum::Duration(_) => EvalType::Duration,
        Datum::Json(_) => EvalType::Json,
        Datum::VectorFloat32(_) => EvalType::VectorFloat32,
        Datum::Null | Datum::MinNotNull | Datum::MaxValue | Datum::Raw(_) => return None,
    })
}

/// Whether `value` is already the family `ret_type` declares.
///
/// A HYBRID result type (`BIT`/`ENUM`/`SET`, Go `FieldType.Hybrid`) always
/// answers yes: its eval type is the domain it COMPARES in (`BIT` compares as
/// an integer), while `getFixedLen`'s default arm gives it a variable-length
/// cell that only the hybrid datum itself fits. Converting such a value onto
/// its eval type would put an integer in a var-length column -- the very
/// disagreement this check exists to prevent.
fn same_eval_family(value: &Datum, ret_type: &tidb_datatype::FieldType) -> bool {
    use tidb_datatype::EvalType;
    if ret_type.is_hybrid() {
        return true;
    }
    // The REAL family is the one place where sharing an eval type is not
    // enough. Go's `EvalReal` is float64-valued for both `FLOAT` and `DOUBLE`,
    // so a 4-byte `KindFloat32` datum is only ever a `FLOAT` COLUMN's own
    // cell, never an expression's result -- while `getFixedLen` gives
    // `TypeFloat` a 4-byte cell and `TypeDouble` an 8-byte one. A `Float32`
    // value under a `DOUBLE` result type therefore has to widen, or
    // `append_float32` writes 4 bytes into an 8-byte cell.
    if ret_type.eval_type() == EvalType::Real {
        return match value {
            Datum::Float32(_) => ret_type.code() == tidb_datatype::FieldTypeCode::Float,
            Datum::Real(_) => ret_type.code() != tidb_datatype::FieldTypeCode::Float,
            _ => false,
        };
    }
    let normalize = |t| match t {
        EvalType::Timestamp => EvalType::Datetime,
        other => other,
    };
    datum_eval_type(value).is_some_and(|got| normalize(got) == normalize(ret_type.eval_type()))
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

/// Go `unFoldableFunctions`: calls whose result cannot be frozen during
/// constant folding even when every argument is a strict literal.
#[must_use]
pub fn is_unfoldable_function(name: &str) -> bool {
    name.starts_with("getvar_")
        || matches!(
            name,
            "sysdate"
                | "found_rows"
                | "rand"
                | "uuid"
                | "uuid_v4"
                | "uuid_v7"
                | "sleep"
                | "row"
                | "values"
                | "setvar"
                | "getvar"
                | "getparam"
                | "benchmark"
                | "dayname"
                | "nextval"
                | "lastval"
                | "setval"
                | "any_value"
        )
}

/// The Go scalar-function name for a binary operator (inverse of
/// [`binary_op_for_name`]); used when building a [`ScalarFunction`] from an AST
/// operator.
#[must_use]
pub fn binary_op_name(op: BinaryOp) -> &'static str {
    op.opcode().name()
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
    json_schema_cache: crate::builtin_ext::JsonSchemaCache,
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

    /// Go `ScalarFunction.CleanHashCode` (`scalar_function.go:604`): drops the
    /// cached hash code so the next [`Self::hash_code`] recomputes it.
    ///
    /// Required by every rewrite that mutates [`Self::args`] in place --
    /// `SetExprColumnInOperand` and `ColumnSubstituteImpl`'s grouping arm both
    /// call it in Go. Go also clears `canonicalhashcode`; this crate does not
    /// cache one yet, so there is nothing else to drop.
    pub fn clean_hash_code(&mut self) {
        self.hashcode.clear();
    }

    /// Go `ConstLevel`.
    #[must_use]
    pub fn const_level(&self) -> ConstLevel {
        if is_unfoldable_function(self.func_name.lowercase()) {
            return ConstLevel::NONE;
        }
        self.args
            .iter()
            .map(Expression::const_level)
            .min()
            .unwrap_or(ConstLevel::STRICT)
    }

    /// THE guarantee Go's `ScalarFunction.Eval` provides and this tier must
    /// reproduce: a function's evaluated value is in the EVAL-TYPE FAMILY of
    /// its own declared result type.
    ///
    /// Go gets this by construction. `ScalarFunction.Eval`
    /// (`pkg/expression/scalar_function.go`) does not ask the signature for
    /// "a value" -- it switches on `sf.GetType().EvalType()` and calls
    /// `EvalInt`/`EvalReal`/`EvalDecimal`/`EvalString`/`EvalTime`/
    /// `EvalDuration`/`EvalJSON` accordingly, so the returned datum's kind is
    /// DERIVED from the result type and cannot disagree with it. Downstream,
    /// `chunk.AppendDatum` dispatches on the datum kind while the column's
    /// cell width came from the field type, and only that construction keeps
    /// the two in step.
    ///
    /// This tier evaluates on Datums and dispatches on operand kinds, so the
    /// guarantee is restored here instead, once, for every function -- rather
    /// than at the handful of call sites whose mismatch had been noticed
    /// (`IF`/`CASE`), which left the same defect reachable from every other
    /// one. A value already in the right family is returned untouched, so the
    /// result type's LENGTH constraints are applied only where Go's own
    /// argument cast would have applied them.
    fn coerce_to_ret_type(&self, value: Datum) -> Result<Datum, EvalError> {
        if value.is_null() {
            return Ok(value);
        }
        let Some(ret_type) = self.get_static_type() else {
            return Ok(value);
        };
        if same_eval_family(&value, ret_type) {
            return Ok(value);
        }
        // COALESCE is built with `newBaseBuiltinFuncWithTp`, so its ARGUMENTS
        // are cast only to the merged eval family -- but `getFunction` then
        // assigns `bf.tp = resultFieldType`, and that merged type is what the
        // selected value is presented as. `select coalesce(1, 2.55, 3)`
        // answers `1.00`, not `1`: the integer branch is widened to the
        // merged scale, exactly as IF/IFNULL/CASE widen theirs. Handled by
        // the shared conversion below rather than a COALESCE-only rule.
        match value.convert_to(ret_type, tidb_datatype::DEFAULT_STATEMENT_FLAGS) {
            Ok(converted) => Ok(converted.value),
            Err(_) => Ok(value),
        }
    }

    /// The collation this function's own evaluation runs under: the one the
    /// expression rewriter's derivation stamped on its result type (Go's
    /// `baseBuiltinFunc.collator`, set from `ExprCollation.Collation`).
    ///
    /// A node built outside the rewriter carries no derived collation, and
    /// falls back to the connection collation the rest of the tier uses.
    #[must_use]
    pub fn derived_collation(&self) -> tidb_datatype::Collation {
        self.ret_type
            .as_ref()
            .and_then(|ft| tidb_datatype::Collation::from_name(ft.collation_name()))
            .unwrap_or(crate::ops::DERIVATION_FREE_COLLATION)
    }

    /// The user-variable name a `getvar`/`setvar` call carries in its first
    /// argument, which the rewriter built as a constant because the name is a
    /// build-time token rather than a value.
    fn uservar_name(&self) -> Option<String> {
        let Expression::Constant(name) = self.args.first()? else {
            return None;
        };
        name.value.sql_string().ok()
    }

    /// Go `ScalarFunction.Eval`: evaluate the function against one row, and
    /// return a value in the eval-type family of the function's own result
    /// type -- see [`Self::coerce_to_ret_type`] for why that is the whole
    /// point of Go's `Eval` and not an afterthought.
    pub fn eval(&self, ctx: &impl Columns, row: Row<'_>) -> Result<Datum, EvalError> {
        let value = self.eval_by_signature(ctx, row)?;
        self.coerce_to_ret_type(value)
    }

    /// The per-signature evaluation itself.
    ///
    /// The binary operators, the lazy control forms (`CASE`/`IF`), `LIKE`,
    /// the collation-aware string builtins, the casts, the session-state
    /// functions and the date/time family are each handled by name below;
    /// everything else evaluates its arguments eagerly and reuses the shared
    /// Datum-level implementations. Each returns whatever its operand kinds
    /// produced; reconciling that with the declared result type is
    /// [`Self::eval`]'s job, done once for all of them.
    fn eval_by_signature(&self, ctx: &impl Columns, row: Row<'_>) -> Result<Datum, EvalError> {
        let name = self.func_name.lowercase();
        if name == "json_schema_valid" {
            return self.json_schema_cache.eval(&self.args, ctx, row);
        }
        if let Some(op) = binary_op_for_name(name) {
            if self.args.len() == 2 {
                let lhs = self.args[0].eval(ctx, row)?;
                if matches!(op, BinaryOp::LogicAnd | BinaryOp::LogicOr) {
                    let lhs = crate::truthy_of(&lhs)?;
                    match (op, lhs) {
                        (BinaryOp::LogicAnd, Some(false)) => return Ok(Datum::Int(0)),
                        (BinaryOp::LogicOr, Some(true)) => return Ok(Datum::Int(1)),
                        _ => {}
                    }
                    let rhs = crate::truthy_of(&self.args[1].eval(ctx, row)?)?;
                    return Ok(match op {
                        BinaryOp::LogicAnd if rhs == Some(false) => Datum::Int(0),
                        BinaryOp::LogicOr if rhs == Some(true) => Datum::Int(1),
                        _ if lhs.is_none() || rhs.is_none() => Datum::Null,
                        BinaryOp::LogicAnd => Datum::Int(1),
                        BinaryOp::LogicOr => Datum::Int(0),
                        _ => unreachable!("logical operator was guarded"),
                    });
                }
                let rhs = self.args[1].eval(ctx, row)?;
                // A binary-literal operand carries its signedness in its own
                // `FieldType` and nowhere else -- `binary_literal_type(len,
                // false)` for `b'..'`, `true` for `0x..`/`x'..'`. See
                // `binary_literal::cast_signed_literal_operands`.
                let signed = [0, 1].map(|index| {
                    self.args[index]
                        .static_type()
                        .is_some_and(|ft| !ft.is_unsigned())
                });
                let (lhs, rhs) =
                    crate::binary_literal::cast_signed_literal_operands(op, lhs, rhs, signed);
                // The statement context travels with the operands, so a
                // zero divisor reaches the same warning/error policy the AST
                // evaluator applies.
                // The ARGUMENT EXPRESSIONS travel with the values. Go's
                // operator dispatch reads `args[i].GetType(ctx)` and the
                // `args[i].(*Constant)` type switch to pick a signature, and a
                // `Datum` records neither -- see `ops::operand`.
                return crate::ops::eval_binary_full(
                    op,
                    lhs,
                    rhs,
                    ctx.div_precision_increment(),
                    self.derived_collation(),
                    crate::ops::Operands::of(&self.args[0], &self.args[1]),
                    ctx,
                );
            }
        }
        if let Some(op) = unary_op_for_name(name) {
            if self.args.len() == 1 {
                let v = self.args[0].eval(ctx, row)?;
                return crate::ops::eval_unary(
                    op,
                    v,
                    crate::ops::Operand::Expr(&self.args[0]),
                    ctx,
                );
            }
        }
        if let [arg] = self.args.as_slice() {
            if let Some(value) = crate::collation_derive::info_metadata_value(name, arg) {
                return Ok(value);
            }
        }
        if name == "benchmark" {
            let [count, expression] = self.args.as_slice() else {
                return Err(EvalError::Unsupported("BENCHMARK arguments"));
            };
            let Some(loop_count) =
                crate::func::benchmark_loop_count(count.eval(ctx, row)?, count.static_type(), ctx)?
            else {
                return Ok(Datum::Null);
            };
            if loop_count < 0 {
                return Ok(Datum::Null);
            }
            crate::func::ensure_benchmark_eval_type(expression.static_type())?;
            for _ in 0..loop_count {
                expression.eval(ctx, row)?;
            }
            return Ok(Datum::Int(0));
        }
        if name == "sleep" {
            let [argument] = self.args.as_slice() else {
                return Err(EvalError::WrongParameterCount("sleep"));
            };
            let value = argument.eval(ctx, row)?;
            if value.is_null() {
                ctx.handle_sleep_incorrect_argument()?;
                return Ok(Datum::Int(0));
            }
            let seconds = crate::ops::to_f64_with_mysql_string(&value, ctx)?;
            if seconds < 0.0 {
                ctx.handle_sleep_incorrect_argument()?;
                return Ok(Datum::Int(0));
            }
            if seconds > f64::MAX / 1_000_000_000.0 {
                return Err(EvalError::IncorrectArguments(
                    "Incorrect arguments to sleep".to_owned(),
                ));
            }
            if seconds <= 0.0 {
                return Ok(Datum::Int(0));
            }
            let duration = std::time::Duration::try_from_secs_f64(seconds).map_err(|_| {
                EvalError::IncorrectArguments("Incorrect arguments to sleep".to_owned())
            })?;
            return Ok(Datum::Int(i64::from(ctx.sleep_for(duration))));
        }
        if matches!(
            name,
            "get_lock" | "release_lock" | "is_free_lock" | "is_used_lock" | "release_all_locks"
        ) {
            if name == "release_all_locks" {
                if !self.args.is_empty() {
                    return Err(EvalError::WrongParameterCount("release_all_locks"));
                }
                let count = ctx.release_all_advisory_locks()?;
                return Ok(Datum::Int(i64::try_from(count).unwrap_or(i64::MAX)));
            }

            let expected = if name == "get_lock" { 2 } else { 1 };
            if self.args.len() != expected {
                return Err(EvalError::WrongParameterCount(match name {
                    "get_lock" => "get_lock",
                    "release_lock" => "release_lock",
                    "is_free_lock" => "is_free_lock",
                    "is_used_lock" => "is_used_lock",
                    _ => unreachable!("advisory lock name was matched above"),
                }));
            }
            let raw_name = self.args[0].eval(ctx, row)?;
            let string_name =
                crate::cast::cast_arg_as_string(&raw_name, self.args[0].static_type(), ctx)?;
            let lock_name = advisory_lock_name(string_name)?;

            return match name {
                "get_lock" => {
                    let raw_timeout = self.args[1].eval(ctx, row)?;
                    let int_timeout = crate::cast::cast_arg_as_int(
                        &raw_timeout,
                        self.args[1].static_type(),
                        ctx,
                    )?;
                    let mut timeout = crate::arg_eval_type::eval_int(&int_timeout)?.unwrap_or(0);
                    if !(0..=MAX_ADVISORY_LOCK_TIMEOUT_SECS).contains(&timeout) {
                        ctx.append_warning(
                            1292,
                            &format!("Truncated incorrect get_lock value: '{timeout}'"),
                        );
                        timeout = MAX_ADVISORY_LOCK_TIMEOUT_SECS;
                    }
                    let acquired = ctx.acquire_advisory_lock(
                        &lock_name,
                        std::time::Duration::from_secs(timeout as u64),
                    )?;
                    Ok(Datum::Int(i64::from(acquired)))
                }
                "release_lock" => Ok(Datum::Int(i64::from(
                    ctx.release_advisory_lock(&lock_name)?,
                ))),
                "is_free_lock" => Ok(Datum::Int(i64::from(
                    ctx.advisory_lock_owner(&lock_name)?.is_none(),
                ))),
                "is_used_lock" => Ok(ctx
                    .advisory_lock_owner(&lock_name)?
                    .map_or(Datum::Null, |owner| Datum::Int(owner as i64))),
                _ => unreachable!("advisory lock name was matched above"),
            };
        }
        // Go `builtinCaseWhen*Sig`: the arguments are the flattened
        // `cond, result, ..., else` list, and only the selected branch is
        // evaluated -- so an error in an unreachable branch never surfaces.
        if name == "case_when" {
            let mut pairs = self.args.chunks_exact(2);
            for pair in pairs.by_ref() {
                let condition = pair[0].eval(ctx, row)?;
                // A NULL condition is not a match, the same as false.
                if crate::truthy_of(&condition)? == Some(true) {
                    return pair[1].eval(ctx, row);
                }
            }
            // An odd argument count means a trailing ELSE.
            return match pairs.remainder().first() {
                Some(else_branch) => else_branch.eval(ctx, row),
                None => Ok(Datum::Null),
            };
        }
        // Go `builtinIf*Sig` is lazy too: the condition decides which single
        // branch is evaluated, so an error in the other never surfaces.
        if name == "if" && self.args.len() == 3 {
            let condition = self.args[0].eval(ctx, row)?;
            let branch = if crate::truthy_of(&condition)? == Some(true) {
                1
            } else {
                2
            };
            return self.args[branch].eval(ctx, row);
        }
        // Go's IFNULL and COALESCE signatures stop at the first non-NULL
        // value. Evaluate only as far as that decision requires so an error
        // in a skipped argument stays unreachable.
        if name == "ifnull" && self.args.len() == 2 {
            let first = self.args[0].eval(ctx, row)?;
            return if first.is_null() {
                self.args[1].eval(ctx, row)
            } else {
                Ok(first)
            };
        }
        if name == "coalesce" {
            for arg in &self.args {
                let value = arg.eval(ctx, row)?;
                if !value.is_null() {
                    return Ok(value);
                }
            }
            return Ok(Datum::Null);
        }
        if name == "interval" && self.args.len() >= 2 {
            let arg_types = self
                .args
                .iter()
                .map(|argument| argument.static_type().cloned())
                .collect::<Vec<_>>();
            return crate::builtin_ext::interval_lazy(
                &arg_types,
                |index| self.args[index].eval(ctx, row),
                ctx,
            );
        }
        // Go's CONCAT signatures capture max_allowed_packet at build time and
        // evaluate left-to-right. Crossing the limit returns NULL immediately,
        // so a later argument (including one that would error) is unreachable.
        if name == "concat" && !self.args.is_empty() {
            let mut output = Vec::new();
            for arg in &self.args {
                let value = arg.eval(ctx, row)?;
                let Some(bytes) = crate::coerce::coerce_str_bytes(&value)? else {
                    return Ok(Datum::Null);
                };
                let next_len = output.len().saturating_add(bytes.len()) as u64;
                if next_len > ctx.max_allowed_packet() {
                    ctx.handle_allowed_packet_overflowed("concat")?;
                    return Ok(Datum::Null);
                }
                output.extend_from_slice(&bytes);
            }
            return Ok(Datum::new_string(output));
        }
        if name == "concat_ws" && self.args.len() >= 2 {
            let separator = self.args[0].eval(ctx, row)?;
            let Some(separator) = crate::coerce::coerce_str_bytes(&separator)? else {
                return Ok(Datum::Null);
            };
            let mut parts = Vec::new();
            let mut target_len = 0_u64;
            for (index, arg) in self.args[1..].iter().enumerate() {
                let value = arg.eval(ctx, row)?;
                let Some(bytes) = crate::coerce::coerce_str_bytes(&value)? else {
                    continue;
                };
                target_len = target_len.saturating_add(bytes.len() as u64);
                if index > 0 {
                    target_len = target_len.saturating_add(separator.len() as u64);
                }
                if target_len > ctx.max_allowed_packet() {
                    ctx.handle_allowed_packet_overflowed("concat_ws")?;
                    return Ok(Datum::Null);
                }
                parts.push(bytes);
            }
            let output_len = parts
                .iter()
                .fold(0_usize, |length, part| length.saturating_add(part.len()))
                .saturating_add(
                    separator
                        .len()
                        .saturating_mul(parts.len().saturating_sub(1)),
                );
            let mut output = Vec::with_capacity(output_len);
            for (index, part) in parts.iter().enumerate() {
                if index > 0 {
                    output.extend_from_slice(&separator);
                }
                output.extend_from_slice(part);
            }
            return Ok(Datum::new_string(output));
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
                .sql_bytes()
                .map_err(|_| EvalError::Unsupported("invalid LIKE operand scalar domain"))?;
            let pattern = pattern
                .sql_bytes()
                .map_err(|_| EvalError::Unsupported("invalid LIKE pattern scalar domain"))?;
            let matched = if name == "ilike" {
                crate::like::ilike_match_with_collation(
                    text,
                    pattern,
                    escape.unwrap_or(b'\\'),
                    self.derived_collation(),
                )
            } else {
                crate::like_match_with_collation(text, pattern, escape, self.derived_collation())
            };
            return Ok(Datum::Int(i64::from(matched)));
        }
        // Go's `GETVAR`/`SETVAR` families (`pkg/expression/builtin_other.go`):
        // the variable NAME is a build-time constant, so the rewriter passes
        // it as the first argument, and the session state both reach lives
        // behind `Columns::get_uservar`/`set_uservar`.
        //
        // `SETVAR` is the inline `@x := expr` assignment expression, whose
        // whole point is the SIDE EFFECT: it is evaluated once per row, and a
        // LATER select-list item of the same row (or the next row) sees what
        // it wrote, which is what makes the MySQL running-total idiom work.
        // Go's `builtinSet*VarSig` returns NULL WITHOUT touching the existing
        // variable when the value is NULL -- unlike top-level `SET @x = NULL`,
        // which clears it.
        if let Some(var) = self.uservar_name() {
            if let Some(kind) = name.strip_prefix("getvar_") {
                if self.args.len() == 1 {
                    let value = ctx.get_uservar(&var).unwrap_or(Datum::Null);
                    // The name fixed the DECLARED type from what the session
                    // held when the statement was built; an assignment earlier
                    // in the same statement may since have stored another kind
                    // (Go has the same build-time/run-time seam), so the value
                    // is converted onto the declared type rather than trusted
                    // to match it.
                    return uservar_as_kind(kind, value);
                }
            }
            if name == "setvar" && self.args.len() == 2 {
                let value = self.args[1].eval(ctx, row)?;
                if !value.is_null() {
                    ctx.set_uservar(&var, value.clone());
                }
                return Ok(value);
            }
        }
        // Go `builtinRegexpLikeSig`: both operands are stringified, NULL in
        // either propagates, and `NOT REGEXP` is a separate unary NOT wrapped
        // around this call by the rewriter -- see `Expr::Regexp`'s own doc.
        if name == "regexp" && self.args.len() == 2 {
            let value = self.args[0].eval(ctx, row)?;
            let pattern = self.args[1].eval(ctx, row)?;
            if value.is_null() || pattern.is_null() {
                return Ok(Datum::Null);
            }
            let text = value
                .sql_string()
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP operand"))?;
            let pattern = pattern
                .sql_string()
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP pattern"))?;
            let matched = crate::regexp::regexp_match_with_collation(
                &text,
                &pattern,
                self.derived_collation(),
            )?;
            return Ok(Datum::Int(i64::from(matched)));
        }
        if name == "regexp_like" && matches!(self.args.len(), 2 | 3) {
            let string_arg = |index: usize| -> Result<Option<String>, EvalError> {
                let value = self.args[index].eval(ctx, row)?;
                let value =
                    crate::cast::cast_arg_as_string(&value, self.args[index].static_type(), ctx)?;
                if value.is_null() {
                    return Ok(None);
                }
                value
                    .sql_string()
                    .map(Some)
                    .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP_LIKE argument"))
            };
            let Some(text) = string_arg(0)? else {
                return Ok(Datum::Null);
            };
            let Some(pattern) = string_arg(1)? else {
                return Ok(Datum::Null);
            };
            let match_type = if self.args.len() == 3 {
                let Some(match_type) = string_arg(2)? else {
                    return Ok(Datum::Null);
                };
                match_type
            } else {
                String::new()
            };
            let matched = crate::regexp::regexp_like_with_collation(
                &text,
                &pattern,
                &match_type,
                self.derived_collation(),
            )?;
            return Ok(Datum::Int(i64::from(matched)));
        }
        // The charset boundary: `to_binary`/`from_binary` are the implicit
        // calls the rewriter wraps a non-UTF-8 argument in (Go
        // `HandleBinaryLiteral`), and `convert_using` is `CONVERT(x USING
        // cs)`. See `crate::convert_charset` for why these are the only
        // places a string's bytes ever change.
        if (name == "to_binary" || name == "from_binary") && self.args.len() == 1 {
            let value = self.args[0].eval(ctx, row)?;
            if value.is_null() {
                return Ok(Datum::Null);
            }
            // `to_binary` encodes INTO the argument's charset; `from_binary`
            // decodes OUT of the result's.
            let typed = if name == "to_binary" {
                self.args[0].static_type()
            } else {
                self.get_static_type()
            };
            let charset = typed
                .map_or("binary", tidb_datatype::FieldType::charset_name)
                .to_owned();
            return if name == "to_binary" {
                crate::convert_charset::to_binary(&value, &charset)
            } else {
                crate::convert_charset::from_binary(&value, &charset)
            };
        }
        if name == "convert_using" && self.args.len() == 2 {
            let value = self.args[0].eval(ctx, row)?;
            if value.is_null() {
                return Ok(Datum::Null);
            }
            let target = self.args[1].eval(ctx, row)?;
            let target = crate::coerce::coerce_str_bytes(&target)?.unwrap_or_default();
            let target = String::from_utf8_lossy(&target).into_owned();
            let arg_type = self.args[0].static_type().cloned().unwrap_or_else(|| {
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString)
            });
            let value = crate::cast::cast_arg_as_string(&value, Some(&arg_type), ctx)?;
            let explicit_collation = crate::collation_derive::coercibility_of(&self.args[0])
                == crate::expr_collation::Coercibility::EXPLICIT;
            let string_type = crate::cast::cast_arg_as_string_type(
                &arg_type,
                explicit_collation,
                ctx.connection_charset_info(),
            );
            return crate::convert_charset::convert_using(&value, &string_type, &target);
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
                // `CAST(expr AS JSON)` reads the SOURCE argument's static
                // FieldType first: a genuine BINARY-charset argument renders
                // as a JSON `Opaque` value (Go's `getRealJSONValue` rule),
                // which `crate::cast::eval_cast`'s untyped `CastType::Json`
                // arm cannot see.
                if target == "json" {
                    let parse_document = self.get_static_type().is_some_and(|field_type| {
                        field_type.flags() & tidb_datatype::FieldTypeFlags::PARSE_TO_JSON != 0
                    });
                    return if parse_document {
                        crate::builtin_ext::cast_as_json_typed(&value, self.args[0].static_type())
                    } else {
                        crate::builtin_ext::cast_as_json_value_typed(
                            &value,
                            self.args[0].static_type(),
                        )
                    };
                }
                let ret_type = self
                    .get_static_type()
                    .ok_or(EvalError::Unsupported("a cast with no result type"))?;
                return crate::cast::eval_cast(
                    &cast_type_of(target, ret_type)?,
                    value,
                    self.args[0].static_type(),
                    ctx,
                );
            }
        }
        // The `DATE_ADD`/`DATE_SUB` family: the rewriter recorded the INTERVAL
        // unit in the name (it is a build-time keyword, not a value), so the
        // two remaining children are ordinary expressions and the shared
        // `time_fn::calendar::date_add` — the very implementation the AST
        // evaluator uses — runs them. `ADDDATE`/`SUBDATE` were normalized onto
        // these same names by the rewriter.
        if self.args.len() == 2 {
            let subtract = name.starts_with("date_sub_");
            if subtract || name.starts_with("date_add_") {
                let unit = &name["date_add_".len()..];
                let result_fsp = crate::time_fn::calendar::date_add_result_fsp(
                    unit,
                    self.args[0].static_type(),
                    self.args[1].static_type(),
                );
                let mut date = self.args[0].eval(ctx, row)?;
                let amount = self.args[1].eval(ctx, row)?;
                let result_type = self.get_static_type();
                if result_type.is_some_and(|field_type| {
                    field_type.code() == tidb_datatype::FieldTypeCode::Duration
                }) {
                    return crate::time_fn::add_sub::date_add_duration(
                        unit,
                        &date,
                        &amount,
                        self.args[1].static_type(),
                        if subtract { -1 } else { 1 },
                        i64::from(result_fsp.unwrap_or(0)),
                    );
                }
                if result_type.is_some_and(|field_type| {
                    field_type.code() == tidb_datatype::FieldTypeCode::Datetime
                }) && self.args[0].static_type().is_some_and(|field_type| {
                    field_type.code() == tidb_datatype::FieldTypeCode::Duration
                }) {
                    date =
                        crate::cast::cast_arg_as_datetime(&date, self.args[0].static_type(), ctx)?;
                }
                let result = crate::time_fn::calendar::date_add_with_result_fsp(
                    unit,
                    &date,
                    &amount,
                    if subtract { -1 } else { 1 },
                    result_fsp,
                )?;
                let Some(result_type) = result_type else {
                    return Ok(result);
                };
                return match result_type.code() {
                    tidb_datatype::FieldTypeCode::Date => crate::cast::parse_computed_time(
                        &result,
                        ctx,
                        tidb_datatype::TimeType::Date,
                        Some(0),
                    ),
                    tidb_datatype::FieldTypeCode::Datetime => crate::cast::parse_computed_time(
                        &result,
                        ctx,
                        tidb_datatype::TimeType::DateTime,
                        Some(result_type.decimal()),
                    ),
                    _ => Ok(result),
                };
            }
        }
        // Go `builtinDatabaseSig`/`builtinVersionSig` read session state
        // rather than arguments: the current database (NULL when none is
        // selected) and the same string `@@version` reports.
        if self.args.is_empty() {
            match name {
                "database" | "schema" => {
                    return Ok(match ctx.current_database() {
                        Some(name) => Datum::new_string(name.into_bytes()),
                        None => Datum::Null,
                    })
                }
                "version" => {
                    return Ok(ctx.sysvar(None, "version").unwrap_or(Datum::Null));
                }
                "tidb_version" => {
                    return Ok(Datum::new_string(ctx.tidb_info().into_bytes()));
                }
                // Go `builtinCurrentUserSig` reports the MATCHED grant
                // identity (`UserIdentity.String()`, which prefers
                // AuthUsername@AuthHostname). CURRENT_USER is the only name
                // bound to it: `pkg/expression/builtin.go:823` registers
                // `currentUserFunctionClass` for CURRENT_USER alone, while
                // USER, SESSION_USER and SYSTEM_USER (`:833`, `:840`, `:841`)
                // all share `userFunctionClass`, whose sig returns
                // `UserIdentity.LoginString()` -- the authenticated identity.
                "current_user" => {
                    return Ok(match ctx.current_user() {
                        Some(user) => Datum::new_string(user.into_bytes()),
                        None => Datum::Null,
                    })
                }
                // Go `builtinCurrentRoleSig` reports the session's ACTIVE
                // roles -- not the roles the account holds, which only
                // `SHOW GRANTS` prints.
                "current_role" => {
                    return Ok(match ctx.current_role() {
                        Some(roles) => Datum::new_string(roles.into_bytes()),
                        None => Datum::Null,
                    })
                }
                "user" | "session_user" | "system_user" => {
                    return Ok(match ctx.login_user() {
                        Some(user) => Datum::new_string(user.into_bytes()),
                        None => Datum::Null,
                    })
                }
                "connection_id" => {
                    return Ok(match ctx.connection_id() {
                        Some(id) => Datum::UInt(id),
                        None => Datum::Null,
                    })
                }
                "found_rows" => {
                    return Ok(match ctx.found_rows() {
                        Some(rows) => Datum::UInt(rows),
                        None => Datum::Null,
                    })
                }
                _ => {}
            }
        }
        // Go `builtinTrim*Sig`: the name carries the direction and the second
        // argument is the string to remove.
        if let Some(direction) = match name {
            "trim" if self.args.len() == 2 => Some(tidb_ast::TrimDirection::Both),
            "ltrim_with" => Some(tidb_ast::TrimDirection::Leading),
            "rtrim_with" => Some(tidb_ast::TrimDirection::Trailing),
            _ => None,
        } {
            let value = self.args[0].eval(ctx, row)?;
            let remstr = self.args[1].eval(ctx, row)?;
            let binary = matches!(value, Datum::Bytes(_));
            let text = crate::coerce::coerce_str_bytes(&value)?;
            let remove = crate::coerce::coerce_str_bytes(&remstr)?;
            return Ok(crate::string_fn::trim_value(
                text, remove, direction, binary,
            ));
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
        // Go `randFunctionClass`: a constant `RAND(N)` owns one
        // statement-scoped generator per AST occurrence, a nonconstant
        // argument starts a fresh generator every row. The AST evaluator
        // (`crate::func::eval_func`) gets that per-call identity from the
        // `Expr` node's own address (`expr as *const Expr as usize`, stable
        // for the query's lifetime because the tree is evaluated by
        // reference, never rebuilt per row); this node is evaluated the same
        // way, so its own address serves identically. A literal argument is
        // classified by matching `Expression::Constant` directly -- unlike
        // the AST classifier this does not recurse through a folded
        // arithmetic tree of literals (e.g. `RAND(1+2)`), because constant
        // folding is not yet wired for scalar functions here (`const_level`
        // above is conservatively `ConstNone`); every case this port targets
        // passes RAND a bare literal.
        if name == "rand" {
            let vals: Vec<Datum> = self
                .args
                .iter()
                .map(|a| a.eval(ctx, row))
                .collect::<Result<_, _>>()?;
            let arg_is_constant = matches!(self.args.first(), Some(Expression::Constant(_)));
            let function_key = Some(std::ptr::from_ref(self) as usize);
            return crate::math_fn::eval_rand_values(&vals, ctx, function_key, arg_is_constant);
        }
        // Go `builtinInStringSig` compares the tested value with each list
        // item through the function's own collator, which the derivation
        // aggregated over ALL of them -- so `ci_col IN ('A')` folds case just
        // like `ci_col = 'A'`. Three-valued: a match is 1, no match with a
        // NULL anywhere is NULL, otherwise 0.
        //
        // EVERY list item is compared, even once a match has settled the
        // answer. The comparison is where this tier's string-versus-number
        // coercion happens, so a skipped comparison is a skipped
        // `1292 Truncated incorrect DOUBLE value` -- and a skipped error.
        // Go's vectorized `in`, which is the one real execution runs
        // (`pkg/expression/builtin_other_vec_generated.go`,
        // `builtinInRealSig.vecEvalInt`), evaluates `args[j]` -- the
        // build-time `cast(... as double)` wrapper that IS its coercion --
        // for every `j` unconditionally, propagates its error with
        // `return err`, and skips only the comparison of an already-matched
        // row (`if r64s[i] != 0 { continue }`). The scalar
        // `builtinInRealSig.evalInt` does `return 1, false, nil` from inside
        // its loop, but that is the non-vectorized fallback; the warnings a
        // client sees are the vectorized path's.
        if name == "in" && self.args.len() >= 2 {
            let collation = self.derived_collation();
            let first_eval_type = self.args[0].static_type().map(FieldType::eval_type);
            let cast_candidate =
                |value: Datum, expression: &Expression| -> Result<Datum, EvalError> {
                    match first_eval_type {
                        Some(tidb_datatype::EvalType::Duration) => {
                            crate::cast::cast_arg_as_duration(&value, expression.static_type(), ctx)
                        }
                        _ => Ok(value),
                    }
                };
            let value = cast_candidate(self.args[0].eval(ctx, row)?, &self.args[0])?;
            let mut found_null = value.is_null();
            let mut found_match = false;
            for item_expr in &self.args[1..] {
                let item = cast_candidate(item_expr.eval(ctx, row)?, item_expr)?;
                match crate::ops::eval_binary_full(
                    tidb_ast::BinaryOp::Eq,
                    value.clone(),
                    item,
                    ctx.div_precision_increment(),
                    collation,
                    // Go's `inFunctionClass` gives EVERY argument `args[0]`'s
                    // own eval type rather than running `GetAccurateCmpType`
                    // per pair, so the item's constant-ness does not steer it
                    // there. The duration signature's unconditional cast is
                    // applied above; handing the real argument expressions
                    // over remains strictly more information than claiming
                    // both are literals for the other signatures.
                    crate::ops::Operands::of(&self.args[0], item_expr),
                    ctx,
                )? {
                    Datum::Int(0) => {}
                    Datum::Null => found_null = true,
                    _ => found_match = true,
                }
            }
            return Ok(if found_match {
                Datum::Int(1)
            } else if found_null {
                Datum::Null
            } else {
                Datum::Int(0)
            });
        }
        // The collation-aware string builtins. Go gives each of these a
        // `baseBuiltinFunc.collator` taken from the derived result collation
        // (`builtinLocate2ArgsUTF8Sig`, `builtinInstrUTF8Sig`,
        // `builtinStrcmpSig`), so `INSTR('ABC' COLLATE utf8mb4_general_ci, 'b')`
        // is 2 while the `utf8mb4_bin` form is 0. They are intercepted here,
        // ahead of the values-only dispatch, because that dispatch sees values
        // alone and cannot know which collation was derived.
        {
            let collation = self.derived_collation();
            match name {
                // `LOCATE(substr, str)` / `INSTR(str, substr)`: the same
                // 1-indexed position with the arguments swapped.
                "locate" | "instr" if self.args.len() == 2 => {
                    let (a, b) = (self.args[0].eval(ctx, row)?, self.args[1].eval(ctx, row)?);
                    let (haystack, needle) = if name == "locate" { (&b, &a) } else { (&a, &b) };
                    return crate::string_fn::locate(needle, haystack, collation);
                }
                "strcmp" if self.args.len() == 2 => {
                    let vals = [self.args[0].eval(ctx, row)?, self.args[1].eval(ctx, row)?];
                    return crate::string_fn::strcmp_with_collation(&vals, collation);
                }
                "find_in_set" if self.args.len() == 2 => {
                    let vals = [self.args[0].eval(ctx, row)?, self.args[1].eval(ctx, row)?];
                    return crate::builtin_ext::find_in_set_with_collation(&vals, collation);
                }
                // Go `greatestFunctionClass`/`leastFunctionClass`: the
                // ETString signature compares under `b.collation`, and
                // `resolveType4Extremum` may instead have selected the
                // compare-as-time signature from the argument FieldTypes.
                // Neither is visible to the values-only dispatch below.
                "greatest" | "least" if !self.args.is_empty() => {
                    let vals: Vec<Datum> = self
                        .args
                        .iter()
                        .map(|a| a.eval(ctx, row))
                        .collect::<Result<_, _>>()?;
                    let want = if name == "greatest" {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    };
                    return crate::builtin_ext::extremum_with_signature(
                        &vals,
                        want,
                        crate::rewriter::result_type::gl_signature(&self.args),
                        collation,
                        ctx,
                    );
                }
                // Go `weightStringFunctionClass`: a NUMERIC argument builds
                // `builtinWeightStringNullSig` (always NULL) from the
                // argument's FieldType, and the sort key is taken under the
                // ARGUMENT's collation -- the function's own is forced to
                // `binary`, so `derived_collation` is the wrong one here.
                "weight_string" if !self.args.is_empty() => {
                    // The `AS` clause travels as the constant second and
                    // third arguments the rewriter built.
                    let padding = match self.args.len() {
                        3 => {
                            let kind = self.args[1].eval(ctx, row)?;
                            let length = self.args[2].eval(ctx, row)?;
                            let binary = kind.sql_string().is_ok_and(|k| k == "BINARY");
                            Some((binary, crate::cast::to_i64_signed(&length)))
                        }
                        _ => None,
                    };
                    let arg_type = self.args[0].static_type().cloned().unwrap_or_else(|| {
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString)
                    });
                    // Go starts numeric inputs on the NULL signature, then
                    // lets AS BINARY replace it with the binary-padding
                    // signature. AS CHAR deliberately does not replace it.
                    if arg_type.code().is_type_numeric() && !matches!(padding, Some((true, _))) {
                        return Ok(Datum::Null);
                    }
                    let value = self.args[0].eval(ctx, row)?;
                    let value = crate::cast::cast_arg_as_string(&value, Some(&arg_type), ctx)?;
                    let explicit_collation =
                        crate::collation_derive::coercibility_of(&self.args[0])
                            == crate::expr_collation::Coercibility::EXPLICIT;
                    let string_type = crate::cast::cast_arg_as_string_type(
                        &arg_type,
                        explicit_collation,
                        ctx.connection_charset_info(),
                    );
                    let arg_collation =
                        tidb_datatype::Collation::from_name(string_type.collation_name())
                            .unwrap_or(crate::ops::DERIVATION_FREE_COLLATION);
                    return crate::string_packet::weight_string(
                        &value,
                        padding,
                        arg_collation,
                        ctx,
                    );
                }
                "field" if self.args.len() >= 2 => {
                    let vals: Vec<Datum> = self
                        .args
                        .iter()
                        .map(|a| a.eval(ctx, row))
                        .collect::<Result<_, _>>()?;
                    return crate::string_fn::field_with_collation(&vals, collation, ctx);
                }
                _ => {}
            }
        }
        if let Some(result) = crate::builtin_ext::eval_aes_lazy(
            name,
            self.args.len(),
            |index| self.args[index].eval(ctx, row),
            ctx,
        ) {
            return result;
        }
        // Values-only builtins (ABS/CONCAT/...): evaluate every
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
        let upper = name.to_ascii_uppercase();
        // `JSON_ARRAY`/`JSON_OBJECT`/`JSON_{SET,INSERT,REPLACE}`/
        // `JSON_ARRAY_{APPEND,INSERT}`: Go builds each value argument through
        // an implicit `CAST(... AS JSON)`, so a genuine BINARY-charset
        // argument must render as a JSON `Opaque` value rather than an
        // ordinary JSON string -- see `builtin_ext::json::dispatch_typed`.
        // Only this chunk path has each argument's static `FieldType`
        // (`Expression::static_type`); the row/AST evaluator
        // (`crate::func::eval_func`) still falls through to the untyped
        // `dispatch` below and stays the documented partial boundary.
        let arg_types: Vec<Option<FieldType>> =
            self.args.iter().map(|a| a.static_type().cloned()).collect();
        if upper == "HEX" {
            return crate::string_fn::hex_with_type(
                &vals,
                arg_types.first().and_then(Option::as_ref),
                ctx,
            );
        }
        // Go's `newBaseBuiltinFuncWithTp` argument-cast layer
        // (`crate::arg_eval_type`), the one point where a builtin's declared
        // argument eval types are imposed. Only this tier has the static
        // types Go's `WrapWithCastAsTime` reads, so only here can a `YEAR`
        // argument take Go's `ParseTimeFromYear`.
        let vals = crate::arg_eval_type::wrap_datetime_args(&upper, vals, &arg_types, ctx)?;
        let vals = crate::arg_eval_type::wrap_int_args(&upper, vals, &arg_types, ctx)?;
        let vals = crate::arg_eval_type::wrap_string_args(&upper, vals, &arg_types, ctx)?;
        if upper == "ORD" {
            return crate::string_fn::ord_with_type(
                &vals,
                arg_types.first().and_then(Option::as_ref),
            );
        }
        if let Some(result) = crate::builtin_ext::json_dispatch_typed(&upper, &vals, &arg_types) {
            return result;
        }
        // `ADDTIME`/`SUBTIME` are the other family Go types from the
        // ARGUMENTS' `FieldType`s rather than from their values: twelve
        // signatures over the `(tp1, tp2)` cross product, whose arms differ
        // in the result fsp and in whether the answer is NULL at all. Only
        // this tier has those types; `time_fn::dispatch` below still serves
        // the AST tier on Go's `default` branch.
        if matches!(upper.as_str(), "ADDTIME" | "SUBTIME") && vals.len() == 2 {
            use crate::time_fn::add_sub::{add_sub_time, kind_of, TemporalKind};
            let kinds: [TemporalKind; 2] = [
                kind_of(arg_types[0].as_ref(), &vals[0]),
                kind_of(arg_types[1].as_ref(), &vals[1]),
            ];
            // A call whose arguments are all CONSTANT is what Go folds, and
            // folding runs the ROW body. Anything reading a column runs the
            // vectorized one.
            let row_path = self
                .args
                .iter()
                .all(|arg| matches!(arg, Expression::Constant(_)));
            let sign = if upper == "SUBTIME" { -1 } else { 1 };
            let result = add_sub_time(&vals, kinds, sign, row_path, ctx)?;
            return match self.get_static_type().map(FieldType::code) {
                Some(tidb_datatype::FieldTypeCode::Datetime) => crate::cast::parse_computed_time(
                    &result,
                    ctx,
                    tidb_datatype::TimeType::DateTime,
                    None,
                ),
                Some(tidb_datatype::FieldTypeCode::Duration) => {
                    crate::cast::parse_computed_duration(&result, ctx)
                }
                _ => Ok(result),
            };
        }
        if upper == "TIMESTAMP" {
            let result = crate::time_fn::add_sub::timestamp(&vals, ctx)?;
            return crate::cast::parse_computed_time(
                &result,
                ctx,
                tidb_datatype::TimeType::DateTime,
                None,
            );
        }
        if upper == "SYSDATE" {
            let result = crate::time_fn::add_sub::sysdate(&vals, ctx)?;
            return crate::cast::parse_computed_time(
                &result,
                ctx,
                tidb_datatype::TimeType::DateTime,
                self.get_static_type().map(FieldType::decimal),
            );
        }
        if matches!(
            upper.as_str(),
            "NOW"
                | "CURRENT_TIMESTAMP"
                | "LOCALTIME"
                | "LOCALTIMESTAMP"
                | "UTC_TIMESTAMP"
                | "CURDATE"
                | "CURRENT_DATE"
                | "UTC_DATE"
                | "CURTIME"
                | "CURRENT_TIME"
                | "UTC_TIME"
                | "LAST_DAY"
                | "MAKEDATE"
                | "FROM_DAYS"
                | "SEC_TO_TIME"
                | "MAKETIME"
                | "STR_TO_DATE"
                | "TIMEDIFF"
                | "CONVERT_TZ"
                | "FROM_UNIXTIME"
        ) {
            let mut result = crate::time_fn::dispatch(&upper, &vals, ctx)
                .expect("the native temporal family is registered")?;
            if upper == "STR_TO_DATE"
                && self.get_static_type().map(FieldType::code)
                    == Some(tidb_datatype::FieldTypeCode::Datetime)
            {
                if let Ok(text) = result.sql_string() {
                    if text.contains(':') && !text.contains('-') {
                        if ctx.date_modes().no_zero_date {
                            return Ok(Datum::Null);
                        }
                        result = Datum::new_string(format!("0000-00-00 {text}"));
                    }
                }
            }
            return match self.get_static_type().map(FieldType::code) {
                Some(tidb_datatype::FieldTypeCode::Datetime) => crate::cast::parse_computed_time(
                    &result,
                    ctx,
                    tidb_datatype::TimeType::DateTime,
                    self.get_static_type().map(FieldType::decimal),
                ),
                Some(tidb_datatype::FieldTypeCode::Date) => crate::cast::parse_computed_time(
                    &result,
                    ctx,
                    tidb_datatype::TimeType::Date,
                    Some(0),
                ),
                Some(tidb_datatype::FieldTypeCode::Duration) => {
                    crate::cast::parse_computed_duration(&result, ctx)
                }
                _ => Ok(result),
            };
        }
        if matches!(upper.as_str(), "ROUND" | "TRUNCATE")
            && matches!(vals.first(), Some(Datum::Decimal(_)))
        {
            return crate::math_fn::round_or_truncate_with_result_decimal(
                &vals,
                upper == "ROUND",
                self.ret_type.as_ref().map(FieldType::decimal),
                ctx,
            );
        }
        if matches!(upper.as_str(), "CEIL" | "CEILING" | "FLOOR")
            && matches!(vals.first(), Some(Datum::Decimal(_)))
        {
            return crate::math_fn::ceil_floor_with_result_domain(
                &vals,
                upper != "FLOOR",
                self.ret_type
                    .as_ref()
                    .map(|field_type| field_type.eval_type() == tidb_datatype::EvalType::Decimal),
                ctx,
            );
        }
        if let Some(result) = crate::func::eval_func_values_in(&upper, &vals, ctx) {
            return result;
        }
        // The date/time family reads the statement clock and the session
        // zone, so it takes the context rather than values alone.
        if let Some(result) = crate::time_fn::dispatch(&upper, &vals, ctx) {
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
/// Converts a user variable's stored value onto the kind its `getvar_<kind>`
/// call declared. NULL stays NULL, and a value already of that kind passes
/// through untouched -- the conversion only matters when an assignment made
/// during this same statement changed the kind out from under the plan.
fn uservar_as_kind(kind: &str, value: Datum) -> Result<Datum, EvalError> {
    use tidb_ast::CastType;
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let target = match (kind, &value) {
        ("int", Datum::Int(_)) | ("uint", Datum::UInt(_)) | ("real", Datum::Real(_)) => {
            return Ok(value)
        }
        ("decimal", Datum::Decimal(_)) => return Ok(value),
        ("string", Datum::String(_) | Datum::Bytes(_)) => return Ok(value),
        ("int", _) => CastType::Signed,
        ("uint", _) => CastType::Unsigned,
        ("real", _) => CastType::Double,
        ("decimal", _) => CastType::Decimal { flen: 0, scale: 0 },
        ("string", _) => CastType::Char {
            len: None,
            charset: None,
        },
        _ => return Err(EvalError::Unsupported("unknown user-variable kind")),
    };
    // No session resolver is threaded here on purpose: every target above is
    // numeric or string, so no arm reads the date modes or raises a warning.
    // Every target above is numeric or string, so no arm reads the
    // source type.
    crate::cast::eval_cast(&target, value, None, &crate::NoColumns)
}

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
        "datetime" => CastType::DateTime {
            fsp: u32::try_from(ret_type.decimal()).ok(),
        },
        "time" => CastType::Time {
            fsp: u32::try_from(ret_type.decimal()).ok(),
        },
        "year" => CastType::Year,
        "double" => CastType::Double,
        "json" => CastType::Json,
        "vector" => CastType::Vector { dimensions: len() },
        _ => return Err(EvalError::Unsupported("this cast target is not ported")),
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::column::Column;
    use crate::constant::Constant;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    fn ft() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    /// A hand-built node must be given the result type its function actually
    /// reports, exactly as the rewriter's `builtin_return_type` would: since
    /// [`ScalarFunction::eval`] reconciles the evaluated value with the
    /// declared type (Go's own `Eval` does the same by construction), a node
    /// declared `Long` for a string- or real-valued builtin is a node Go
    /// cannot build, and asserting on one would assert nothing.
    fn text_ft() -> FieldType {
        FieldType::new(FieldTypeCode::VarString)
    }

    fn real_ft() -> FieldType {
        FieldType::new(FieldTypeCode::Double)
    }

    #[test]
    fn binary_operator_names_delegate_to_the_opcode_authority() {
        for operator in [
            BinaryOp::Plus,
            BinaryOp::Minus,
            BinaryOp::Mul,
            BinaryOp::Div,
            BinaryOp::IntDiv,
            BinaryOp::Mod,
            BinaryOp::BitAnd,
            BinaryOp::BitOr,
            BinaryOp::BitXor,
            BinaryOp::LeftShift,
            BinaryOp::RightShift,
            BinaryOp::Eq,
            BinaryOp::NullEq,
            BinaryOp::Ne,
            BinaryOp::Lt,
            BinaryOp::Le,
            BinaryOp::Gt,
            BinaryOp::Ge,
            BinaryOp::LogicAnd,
            BinaryOp::LogicOr,
            BinaryOp::LogicXor,
        ] {
            assert_eq!(binary_op_name(operator), operator.opcode().name());
        }
    }

    #[derive(Default)]
    struct InfoColumns {
        current_user: Option<String>,
        current_role: Option<String>,
        connection_id: Option<u64>,
        tidb_info: Option<String>,
    }

    impl Columns for InfoColumns {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn current_user(&self) -> Option<String> {
            self.current_user.clone()
        }

        fn current_role(&self) -> Option<String> {
            self.current_role.clone()
        }

        fn connection_id(&self) -> Option<u64> {
            self.connection_id
        }

        fn tidb_info(&self) -> String {
            self.tidb_info.clone().unwrap_or_else(|| {
                tidb_util::printer::get_tidb_info(
                    &tidb_util::versioninfo::VersionInfo::build_default(),
                )
            })
        }
    }

    fn eval_info(name: &str, result_type: FieldType, ctx: &InfoColumns) -> Datum {
        ScalarFunction::new(CiString::new(name), result_type, vec![])
            .eval(ctx, tidb_chunk::row::Row::empty())
            .expect("session information builtin must evaluate")
    }

    struct PacketColumns {
        limit: u64,
        warnings: RefCell<Vec<(u16, String)>>,
    }

    #[derive(Default)]
    struct WarningColumns {
        warnings: RefCell<Vec<(u16, String)>>,
    }

    impl Columns for WarningColumns {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }

        fn warning_count(&self) -> usize {
            self.warnings.borrow().len()
        }

        fn truncate_warnings(&self, bookmark: usize) {
            self.warnings.borrow_mut().truncate(bookmark);
        }
    }

    #[test]
    fn logical_and_interval_skip_unreachable_warning_arguments() {
        let ctx = WarningColumns::default();
        let integer = |value| Expression::Constant(Constant::new(Datum::Int(value), ft()));
        let division = || {
            let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
            decimal.set_flen(15);
            decimal.set_decimal(4);
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("div"),
                decimal,
                vec![integer(1), integer(0)],
            ))
        };

        for (name, first, expected) in [("and", 0, 0), ("or", 1, 1)] {
            let function =
                ScalarFunction::new(CiString::new(name), ft(), vec![integer(first), division()]);
            assert_eq!(
                function.eval(&ctx, tidb_chunk::row::Row::empty()).unwrap(),
                Datum::Int(expected)
            );
            assert!(ctx.warnings.borrow().is_empty());
        }

        let interval = ScalarFunction::new(
            CiString::new("interval"),
            ft(),
            vec![integer(1), integer(0), integer(1), integer(2), division()],
        );
        assert_eq!(
            interval.eval(&ctx, tidb_chunk::row::Row::empty()).unwrap(),
            Datum::Int(2)
        );
        assert!(ctx.warnings.borrow().is_empty());

        let mut not_null_int = ft();
        not_null_int.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
        let integer =
            |value| Expression::Constant(Constant::new(Datum::Int(value), not_null_int.clone()));
        let unreachable = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("not_a_function"),
            not_null_int.clone(),
            vec![],
        ));
        let interval = ScalarFunction::new(
            CiString::new("interval"),
            ft(),
            vec![integer(1), integer(0), integer(1), integer(2), unreachable],
        );
        assert_eq!(
            interval.eval(&ctx, tidb_chunk::row::Row::empty()).unwrap(),
            Datum::Int(2)
        );
    }

    impl Columns for PacketColumns {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn max_allowed_packet(&self) -> u64 {
            self.limit
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }

    #[test]
    fn concat_family_stops_at_the_packet_limit_before_later_arguments() {
        let ctx = PacketColumns {
            limit: 3,
            warnings: RefCell::new(Vec::new()),
        };
        let string =
            |value: &[u8]| Expression::Constant(Constant::new(Datum::new_string(value), text_ft()));
        let unsupported = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("not_a_function"),
            text_ft(),
            vec![],
        ));
        let concat = ScalarFunction::new(
            CiString::new("concat"),
            text_ft(),
            vec![string(b"abcd"), unsupported],
        );
        assert_eq!(
            concat.eval(&ctx, tidb_chunk::row::Row::empty()).unwrap(),
            Datum::Null
        );
        assert_eq!(ctx.warnings.borrow()[0].0, 1301);
        assert!(ctx.warnings.borrow()[0].1.contains("concat()"));

        ctx.warnings.borrow_mut().clear();
        let concat_ws = ScalarFunction::new(
            CiString::new("concat_ws"),
            text_ft(),
            vec![string(b"--"), string(b"a"), string(b"b")],
        );
        assert_eq!(
            concat_ws.eval(&ctx, tidb_chunk::row::Row::empty()).unwrap(),
            Datum::Null
        );
        assert_eq!(ctx.warnings.borrow()[0].0, 1301);
        assert!(ctx.warnings.borrow()[0].1.contains("concat_ws()"));

        ctx.warnings.borrow_mut().clear();
        assert_eq!(
            crate::func::eval_func_values_in(
                "CONCAT",
                &[Datum::new_string("abcd"), Datum::new_string("x")],
                &ctx,
            )
            .unwrap()
            .unwrap(),
            Datum::Null
        );
        assert_eq!(ctx.warnings.borrow()[0].0, 1301);
    }

    #[test]
    fn decimal_round_and_truncate_cap_dynamic_scale_by_the_result_type() {
        let mut decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
        decimal_type.set_flen(10);
        decimal_type.set_decimal(2);
        let decimal = || {
            Expression::Constant(Constant::new(
                Datum::Decimal(tidb_datatype::Decimal::from_literal("1.23")),
                decimal_type.clone(),
            ))
        };
        let scale = || Expression::Constant(Constant::new(Datum::Int(5), ft()));

        for name in ["round", "truncate"] {
            let function = ScalarFunction::new(
                CiString::new(name),
                decimal_type.clone(),
                vec![decimal(), scale()],
            );
            let value = function
                .eval(&crate::context::NoColumns, tidb_chunk::row::Row::empty())
                .expect("decimal ROUND/TRUNCATE must evaluate");
            let Datum::Decimal(value) = value else {
                panic!("{name} returned a non-decimal value")
            };
            assert_eq!(value.to_string(), "1.23", "{name}");
        }
    }

    // Go TestCurrentUser.
    #[test]
    fn test_current_user() {
        let ctx = InfoColumns {
            current_user: Some("root@localhost".to_owned()),
            ..InfoColumns::default()
        };
        assert_eq!(
            eval_info("current_user", text_ft(), &ctx),
            Datum::new_string(b"root@localhost".to_vec())
        );
        assert_eq!(
            eval_info("current_user", text_ft(), &InfoColumns::default()),
            Datum::Null
        );
    }

    // Go TestCurrentRole.
    #[test]
    fn test_current_role() {
        for (roles, expected) in [
            ("NONE", "NONE"),
            ("`r_1`@`%`,`r_2`@`localhost`", "`r_1`@`%`,`r_2`@`localhost`"),
        ] {
            let ctx = InfoColumns {
                current_role: Some(roles.to_owned()),
                ..InfoColumns::default()
            };
            assert_eq!(
                eval_info("current_role", text_ft(), &ctx),
                Datum::new_string(expected.as_bytes().to_vec())
            );
        }
    }

    // Go TestConnectionID.
    #[test]
    fn test_connection_id() {
        let ctx = InfoColumns {
            connection_id: Some(1),
            ..InfoColumns::default()
        };
        let mut result_type = FieldType::new(FieldTypeCode::LongLong);
        result_type.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
        assert_eq!(
            eval_info("connection_id", result_type, &ctx),
            Datum::UInt(1)
        );
    }

    // Go TestTiDBVersion.
    #[test]
    fn test_tidb_version() {
        let ctx = InfoColumns {
            tidb_info: Some("Release Version: test\nKernel Type: Classic".to_owned()),
            ..InfoColumns::default()
        };
        assert_eq!(
            eval_info("tidb_version", text_ft(), &ctx),
            Datum::new_string(b"Release Version: test\nKernel Type: Classic".to_vec())
        );
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
        assert_eq!(sf.const_level(), ConstLevel::STRICT);
    }

    #[test]
    fn const_level_matches_the_source_function_and_argument_rules() {
        let literal = || Expression::Constant(Constant::new(Datum::Int(1), ft()));
        let parameter = || {
            let mut constant = Constant::new(Datum::Int(1), ft());
            constant.param_marker = Some(crate::constant::ParamMarker { order: 0 });
            Expression::Constant(constant)
        };
        let call = |name: &str, args| ScalarFunction::new(CiString::new(name), ft(), args);

        assert_eq!(
            call("abs", vec![literal()]).const_level(),
            ConstLevel::STRICT
        );
        assert_eq!(
            plus(vec![literal(), parameter()]).const_level(),
            ConstLevel::ONLY_IN_CONTEXT
        );
        assert_eq!(
            plus(vec![literal(), Expression::Column(Column::new(1, ft()))]).const_level(),
            ConstLevel::NONE
        );
        for name in [
            "sysdate",
            "found_rows",
            "rand",
            "uuid",
            "uuid_v4",
            "uuid_v7",
            "sleep",
            "row",
            "values",
            "setvar",
            "getvar",
            "getvar_string",
            "getparam",
            "benchmark",
            "dayname",
            "nextval",
            "lastval",
            "setval",
            "any_value",
        ] {
            assert_eq!(
                call(name, vec![literal()]).const_level(),
                ConstLevel::NONE,
                "{name}"
            );
        }
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
            text_ft(),
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

        // `InferType4ControlFuncs` widens the result metadata to the widest
        // branch scale, and `bf.tp = resultFieldType` makes that merged type
        // the one the selected argument is presented as: recorded TiDB
        // answers `select coalesce(1, 2.55, 3)` with `1.00`.
        let mut decimal_four = FieldType::new(FieldTypeCode::NewDecimal);
        decimal_four.set_flen(15);
        decimal_four.set_decimal(4);
        let decimal_coalesce = ScalarFunction::new(
            CiString::new("coalesce"),
            decimal_four,
            vec![konst(Datum::Int(1)), konst(Datum::Null)],
        );
        assert_eq!(
            decimal_coalesce.eval(&NoColumns, row).unwrap(),
            Datum::Decimal(tidb_datatype::Decimal::from_literal("1.0000"))
        );

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

        // `IF` is a lazy control form now, so it evaluates here: the
        // condition picks the branch (this used to be the example of a
        // function outside the values-only entry).
        let iff = ScalarFunction::new(
            CiString::new("if"),
            ft(),
            vec![
                konst(Datum::Int(1)),
                konst(Datum::Int(2)),
                konst(Datum::Int(3)),
            ],
        );
        assert_eq!(iff.eval(&NoColumns, row).unwrap(), Datum::Int(2));

        // A function with no ported builtin at all still is unsupported.
        let unknown = ScalarFunction::new(
            CiString::new("no_such_builtin"),
            ft(),
            vec![konst(Datum::Int(1))],
        );
        assert!(matches!(
            unknown.eval(&NoColumns, row),
            Err(EvalError::Unsupported(_))
        ));
    }

    /// `JSON_ARRAY`/`CAST(... AS JSON)` over a column whose static `FieldType`
    /// is BINARY-charset render the JSON `Opaque` value real TiDB produces,
    /// not a plain JSON string -- the chunk path threads
    /// `Expression::static_type()` into `builtin_ext::json::dispatch_typed`/
    /// `cast_as_json_typed`. Captured: `SELECT JSON_ARRAY(vb), CAST(vb AS
    /// JSON) FROM t` where `vb varbinary(8)` holds `'ab'`
    /// (`zz_dump_frozjson_test.go`, `TestZZDumpFrozJSONBinaryOpaque`).
    #[test]
    fn eval_renders_binary_charset_column_as_json_opaque() {
        use crate::context::NoColumns;
        use tidb_chunk::chunk::Chunk;
        use tidb_datatype::{Collation, FieldTypeCode};

        let varbinary_type =
            FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary);
        let json_text_type = text_ft();
        let mut chk = Chunk::new_with_capacity(std::slice::from_ref(&varbinary_type), 1);
        chk.append_bytes(0, b"ab");
        let row = chk.get_row(0);

        let mut col = Column::new(1, varbinary_type);
        col.index = 0;

        let json_array = ScalarFunction::new(
            CiString::new("json_array"),
            json_text_type,
            vec![Expression::Column(col.clone())],
        );
        assert_eq!(
            json_array.eval(&NoColumns, row).unwrap(),
            Datum::new_string(r#"["base64:type15:YWI="]"#.to_string())
        );

        let cast_json = ScalarFunction::new(
            CiString::new("cast_json"),
            FieldType::new(FieldTypeCode::Json),
            vec![Expression::Column(col)],
        );
        let Datum::Json(got) = cast_json.eval(&NoColumns, row).unwrap() else {
            panic!("CAST AS JSON did not retain the JSON domain")
        };
        let opaque = got.opaque().expect("opaque binary JSON");
        assert_eq!(opaque.type_code, 15);
        assert_eq!(opaque.bytes, b"ab");
    }

    #[test]
    fn rand_reuses_one_generator_per_node_for_a_constant_seed() {
        use std::cell::Cell;
        use tidb_chunk::chunk::Chunk;

        // A minimal session whose `rand_seeded_next` records the key it was
        // called with and returns a fixed value, so this asserts the SAME
        // key reaches it across repeated evaluations of one `ScalarFunction`
        // node -- exactly the identity the AST evaluator gets from the
        // `Expr` node's own address.
        struct RandColumns {
            keys: Cell<Vec<usize>>,
        }
        impl Columns for RandColumns {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn rand_seeded_next(&self, key: usize, _seed: i64) -> Option<f64> {
                let mut keys = self.keys.take();
                keys.push(key);
                self.keys.set(keys);
                Some(0.5)
            }
        }

        let chk = Chunk::new_with_capacity(std::slice::from_ref(&ft()), 1);
        let row = chk.get_row(0);
        let konst = |d: Datum| Expression::Constant(Constant::new(d, ft()));
        let rand_five =
            ScalarFunction::new(CiString::new("rand"), real_ft(), vec![konst(Datum::Int(5))]);
        let columns = RandColumns {
            keys: Cell::new(Vec::new()),
        };

        assert_eq!(rand_five.eval(&columns, row).unwrap(), Datum::Real(0.5));
        assert_eq!(rand_five.eval(&columns, row).unwrap(), Datum::Real(0.5));
        let keys = columns.keys.into_inner();
        assert_eq!(keys.len(), 2);
        // The SAME node produced the same key both times.
        assert_eq!(keys[0], keys[1]);

        // A DIFFERENT node (a different RAND(5) call site) gets a different
        // key, because each owns its own generator.
        let rand_five_again =
            ScalarFunction::new(CiString::new("rand"), real_ft(), vec![konst(Datum::Int(5))]);
        let columns2 = RandColumns {
            keys: Cell::new(Vec::new()),
        };
        rand_five_again.eval(&columns2, row).unwrap();
        assert_ne!(columns2.keys.into_inner()[0], keys[0]);
    }

    #[test]
    fn rand_with_no_args_reads_the_running_generator() {
        use tidb_chunk::chunk::Chunk;

        struct SeqColumns {
            next: std::cell::Cell<f64>,
        }
        impl Columns for SeqColumns {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn rand_next(&self) -> Option<f64> {
                Some(self.next.get())
            }
        }

        let chk = Chunk::new_with_capacity(std::slice::from_ref(&ft()), 1);
        let row = chk.get_row(0);
        let rand = ScalarFunction::new(CiString::new("rand"), real_ft(), vec![]);
        let columns = SeqColumns {
            next: std::cell::Cell::new(0.75),
        };
        assert_eq!(rand.eval(&columns, row).unwrap(), Datum::Real(0.75));
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
