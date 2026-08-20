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
use tidb_datatype::Datum;

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

/// The two facts needed to prove that a predicate rejects an outer-join row
/// after its inner-side columns have been replaced by SQL `NULL`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct NullRejectProof {
    non_true: bool,
    must_null: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NullRejectTestMode {
    ReturnsFalse,
    KeepsNull,
}

// `pkg/planner/util/null_misc_builtins.go`. These functions return NULL when
// any argument is NULL. Keep the complete source table here: an omitted entry
// loses a valid outer-join simplification, while a spurious entry can make the
// optimizer return wrong rows.
const NULL_REJECT_NULL_PRESERVING_FUNCTIONS: &[&str] = &[
    "cast",
    "not",
    "unaryminus",
    "bitneg",
    "greatest",
    "least",
    "bit_count",
    "ge",
    "le",
    "eq",
    "ne",
    "lt",
    "gt",
    "xor",
    "plus",
    "minus",
    "mod",
    "div",
    "mul",
    "intdiv",
    "bitand",
    "leftshift",
    "rightshift",
    "bitor",
    "bitxor",
    "like",
    "ilike",
    "regexp",
    "regexp_like",
    "regexp_substr",
    "regexp_instr",
    "regexp_replace",
    "strcmp",
    "abs",
    "acos",
    "asin",
    "atan",
    "atan2",
    "ceil",
    "ceiling",
    "conv",
    "cos",
    "cot",
    "crc32",
    "degrees",
    "exp",
    "floor",
    "ln",
    "log",
    "log2",
    "log10",
    "pow",
    "power",
    "radians",
    "round",
    "sign",
    "sin",
    "sqrt",
    "tan",
    "ascii",
    "bin",
    "bit_length",
    "char_length",
    "character_length",
    "concat",
    "find_in_set",
    "from_base64",
    "hex",
    "insert_func",
    "instr",
    "lcase",
    "left",
    "length",
    "locate",
    "lower",
    "lpad",
    "ltrim",
    "mid",
    "oct",
    "octet_length",
    "ord",
    "position",
    "repeat",
    "replace",
    "reverse",
    "right",
    "rpad",
    "rtrim",
    "space",
    "substr",
    "substring",
    "substring_index",
    "to_base64",
    "translate",
    "trim",
    "ucase",
    "unhex",
    "upper",
    "weight_string",
    "adddate",
    "date_add",
    "subdate",
    "date_sub",
    "addtime",
    "convert_tz",
    "date",
    "date_format",
    "datediff",
    "day",
    "dayname",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "extract",
    "from_days",
    "from_unixtime",
    "hour",
    "last_day",
    "makedate",
    "maketime",
    "microsecond",
    "minute",
    "month",
    "monthname",
    "period_add",
    "period_diff",
    "quarter",
    "sec_to_time",
    "second",
    "str_to_date",
    "subtime",
    "time",
    "timediff",
    "time_format",
    "time_to_sec",
    "timestamp",
    "timestampadd",
    "timestampdiff",
    "to_days",
    "to_seconds",
    "unix_timestamp",
    "weekday",
    "weekofyear",
    "year",
    "compress",
    "md5",
    "sha1",
    "sha",
    "sha2",
    "sm3",
    "uncompress",
    "uncompressed_length",
    "json_type",
    "json_extract",
    "json_unquote",
    "json_remove",
    "json_merge",
    "json_merge_preserve",
    "json_contains",
    "json_contains_path",
    "json_overlaps",
    "json_memberof",
    "json_valid",
    "json_pretty",
    "json_quote",
    "json_storage_free",
    "json_storage_size",
    "json_depth",
    "json_keys",
    "json_length",
    "inet_aton",
    "inet_ntoa",
    "inet6_aton",
    "inet6_ntoa",
];

const NULL_REJECT_REJECT_NULL_TESTS: &[(&str, NullRejectTestMode)] = &[
    ("istrue", NullRejectTestMode::ReturnsFalse),
    ("istrue_with_null", NullRejectTestMode::KeepsNull),
    ("isfalse", NullRejectTestMode::ReturnsFalse),
];

/// Proves whether a predicate can be true after every listed inner-side
/// column is replaced by SQL `NULL`.
///
/// This is Go `pkg/planner/util.IsNullRejected`. It tracks both "cannot be
/// true" and the stronger "must be NULL" fact, because SQL three-valued
/// logic needs both for `NOT`, `AND`, and `OR`. Before symbolic reasoning it
/// also tries Go's nullify-then-fold bridge for constant subtrees.
#[must_use]
pub fn is_null_rejected(inner_column_ids: &[i64], predicate: &Expression) -> bool {
    prove_null_rejected(inner_column_ids, predicate, true).non_true
}

fn prove_null_rejected(
    inner_column_ids: &[i64],
    expression: &Expression,
    allow_nullified_fold: bool,
) -> NullRejectProof {
    if allow_nullified_fold {
        if let Some(constant) = try_fold_nullified_constant(inner_column_ids, expression) {
            return proof_from_constant(&constant);
        }
    }

    match expression {
        Expression::Column(column) if inner_column_ids.contains(&column.unique_id) => {
            NullRejectProof {
                non_true: true,
                must_null: true,
            }
        }
        Expression::Constant(constant) => {
            if constant.param_marker.is_none() {
                if let Some(deferred) = constant.deferred_expr.as_deref() {
                    return prove_null_rejected(inner_column_ids, deferred, false);
                }
            }
            proof_from_constant(constant)
        }
        Expression::ScalarFunction(function) => {
            prove_null_rejected_function(inner_column_ids, function, allow_nullified_fold)
        }
        Expression::Column(_) | Expression::CorrelatedColumn(_) => NullRejectProof::default(),
    }
}

fn prove_null_rejected_function(
    inner_column_ids: &[i64],
    function: &ScalarFunction,
    allow_nullified_fold: bool,
) -> NullRejectProof {
    let name = function.func_name.lowercase();
    let prove = |argument: &Expression| {
        prove_null_rejected(inner_column_ids, argument, allow_nullified_fold)
    };
    match (name, function.args.as_slice()) {
        ("and", [left, right]) => {
            let left = prove(left);
            let right = prove(right);
            return NullRejectProof {
                non_true: left.non_true || right.non_true,
                must_null: left.must_null && right.must_null,
            };
        }
        ("or", [left, right]) => {
            let left = prove(left);
            let right = prove(right);
            return NullRejectProof {
                non_true: left.non_true && right.non_true,
                must_null: left.must_null && right.must_null,
            };
        }
        ("not", [Expression::ScalarFunction(child)])
            if child.func_name.lowercase() == "isnull" && child.args.len() == 1 =>
        {
            return NullRejectProof {
                non_true: prove(&child.args[0]).must_null,
                must_null: false,
            };
        }
        ("not", [child]) => {
            let child = prove(child);
            return NullRejectProof {
                non_true: child.must_null,
                must_null: child.must_null,
            };
        }
        ("in", arguments) => {
            let Some((value, list)) = arguments.split_first() else {
                return NullRejectProof::default();
            };
            if prove(value).must_null || list.iter().all(|item| prove(item).must_null) {
                return NullRejectProof {
                    non_true: true,
                    must_null: true,
                };
            }
            return NullRejectProof::default();
        }
        ("isnull", _) => return NullRejectProof::default(),
        ("week" | "yearweek", [date, ..]) => {
            if prove(date).must_null {
                return NullRejectProof {
                    non_true: true,
                    must_null: true,
                };
            }
            return NullRejectProof::default();
        }
        _ => {}
    }

    if let Some((_, mode)) = NULL_REJECT_REJECT_NULL_TESTS
        .iter()
        .find(|(candidate, _)| *candidate == name)
    {
        let child = function
            .args
            .first()
            .map_or_else(NullRejectProof::default, prove);
        return NullRejectProof {
            non_true: child.must_null,
            must_null: child.must_null && *mode == NullRejectTestMode::KeepsNull,
        };
    }

    if NULL_REJECT_NULL_PRESERVING_FUNCTIONS.contains(&name)
        && function
            .args
            .iter()
            .any(|argument| prove(argument).must_null)
    {
        return NullRejectProof {
            non_true: true,
            must_null: true,
        };
    }
    NullRejectProof::default()
}

fn try_fold_nullified_constant(
    inner_column_ids: &[i64],
    expression: &Expression,
) -> Option<Constant> {
    match expression {
        Expression::Column(column) if inner_column_ids.contains(&column.unique_id) => Some(
            Constant::new(Datum::Null, column.get_static_type()?.clone()),
        ),
        Expression::Constant(constant)
            if constant.param_marker.is_none() && constant.deferred_expr.is_none() =>
        {
            Some(constant.clone())
        }
        Expression::ScalarFunction(function) => {
            try_fold_nullified_function(inner_column_ids, function)
        }
        Expression::Column(_) | Expression::Constant(_) | Expression::CorrelatedColumn(_) => None,
    }
}

fn try_fold_nullified_function(
    inner_column_ids: &[i64],
    function: &ScalarFunction,
) -> Option<Constant> {
    let name = function.func_name.lowercase();
    let result_type = function.get_static_type()?.clone();
    if matches!(name, "coalesce" | "ifnull") {
        for argument in &function.args {
            let constant = try_fold_nullified_constant(inner_column_ids, argument)?;
            if !constant.value.is_null() {
                return Some(constant);
            }
        }
        return Some(Constant::new(Datum::Null, result_type));
    }
    if name == "if" {
        let [condition, when_true, when_false] = function.args.as_slice() else {
            return None;
        };
        let condition = try_fold_nullified_constant(inner_column_ids, condition)?;
        let take_true = crate::truthy_of(&condition.value).ok()? == Some(true);
        return try_fold_nullified_constant(
            inner_column_ids,
            if take_true { when_true } else { when_false },
        );
    }
    if name == "truncate"
        && result_type.eval_type() == tidb_datatype::EvalType::Int
        && function
            .args
            .get(1)
            .and_then(Expression::static_type)
            .is_some_and(tidb_datatype::FieldType::is_unsigned)
    {
        // Both Go integer TRUNCATE signatures inspect an unsigned scale's
        // FieldType before evaluating its value. Even a nullable unsigned
        // scale therefore returns X unchanged instead of propagating NULL.
        return try_fold_nullified_constant(inner_column_ids, function.args.first()?);
    }

    let arguments = function
        .args
        .iter()
        .map(|argument| try_fold_nullified_constant(inner_column_ids, argument))
        .collect::<Option<Vec<_>>>()?;
    if NULL_REJECT_NULL_PRESERVING_FUNCTIONS.contains(&name)
        && arguments.iter().any(|argument| argument.value.is_null())
    {
        return Some(Constant::new(Datum::Null, result_type));
    }
    let folded = Expression::ScalarFunction(ScalarFunction::new(
        function.func_name.clone(),
        result_type.clone(),
        arguments.into_iter().map(Expression::Constant).collect(),
    ));
    let value = crate::eval_expression_once(&folded, &crate::NoColumns).ok()?;
    Some(Constant::new(value, result_type))
}

fn proof_from_constant(constant: &Constant) -> NullRejectProof {
    if constant.param_marker.is_some() || constant.deferred_expr.is_some() {
        return NullRejectProof::default();
    }
    if constant.value.is_null() {
        return NullRejectProof {
            non_true: true,
            must_null: true,
        };
    }
    NullRejectProof {
        non_true: crate::truthy_of(&constant.value).ok() == Some(Some(false)),
        must_null: false,
    }
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

    /// The context-free subset of Go `Expression.Equal(ctx, e)`.
    ///
    /// Columns compare by `UniqueID`. Constants compare their retained typed
    /// value, and scalar functions compare their normalized name, return type,
    /// and arguments recursively. The latter is the contract optimizer rules
    /// such as `InjectProjBelowAgg` use to share one projected expression
    /// between an aggregate argument and an identical group item.
    #[must_use]
    pub fn equal(&self, other: &Expression) -> bool {
        match self {
            Expression::Column(c) => c.equal_column(other),
            Expression::CorrelatedColumn(c) => c.equal_column(other),
            // Go `Constant.Equal` (`constant.go:508`): both constants,
            // values binary-compare equal. The deferred-expression Eval
            // legs are this port's materialized values.
            Expression::Constant(c) => {
                let Expression::Constant(y) = other else {
                    return false;
                };
                c.value
                    .compare(&y.value, tidb_datatype::Collation::Binary)
                    .map(|order| order == std::cmp::Ordering::Equal)
                    .unwrap_or(false)
            }
            // Go `ScalarFunction.Equal` (`scalar_function.go:377`): same
            // lowercased name, equal return types, pairwise-equal
            // arguments.
            Expression::ScalarFunction(sf) => {
                let Expression::ScalarFunction(fun) = other else {
                    return false;
                };
                if sf.func_name.lowercase() != fun.func_name.lowercase() {
                    return false;
                }
                match (&sf.ret_type, &fun.ret_type) {
                    (Some(left), Some(right)) if left.equal(right) => {}
                    (None, None) => {}
                    _ => return false,
                }
                sf.args.len() == fun.args.len()
                    && sf
                        .args
                        .iter()
                        .zip(&fun.args)
                        .all(|(left, right)| left.equal(right))
            }
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

    /// Go `Expression.GetType` without an `EvalContext`: the expression's static
    /// result type. `None` mirrors a nil `RetType`.
    ///
    /// For a [`ScalarFunction`] this is the placeholder result type set at
    /// construction (faithful type inference is not yet ported).
    #[must_use]
    pub fn static_type(&self) -> Option<&tidb_datatype::FieldType> {
        match self {
            Expression::Column(c) => c.get_static_type(),
            Expression::Constant(c) => c.get_static_type(),
            Expression::CorrelatedColumn(c) => c.get_static_type(),
            Expression::ScalarFunction(c) => c.get_static_type(),
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
    use sha2::{Digest, Sha256};
    use std::collections::{BTreeMap, BTreeSet};
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{BinaryJSON, Datum, FieldType, FieldTypeCode, VectorFloat32};

    fn null_reject_column(id: i64, field_type: FieldType) -> Expression {
        Expression::Column(Column::new(id, field_type))
    }

    fn null_reject_constant(value: Datum, field_type: FieldType) -> Expression {
        Expression::Constant(Constant::new(value, field_type))
    }

    fn null_reject_int(value: i64) -> Expression {
        null_reject_constant(Datum::Int(value), FieldType::new(FieldTypeCode::LongLong))
    }

    fn null_reject_uint(value: u64) -> Expression {
        null_reject_constant(
            Datum::UInt(value),
            FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
        )
    }

    fn null_reject_string(value: &str) -> Expression {
        null_reject_constant(
            Datum::new_string(value),
            FieldType::new(FieldTypeCode::VarString),
        )
    }

    fn null_reject_json(value: &str) -> Expression {
        null_reject_constant(
            Datum::Json(BinaryJSON::parse(value).unwrap()),
            FieldType::new(FieldTypeCode::Json),
        )
    }

    fn null_reject_function(
        name: &str,
        result_type: FieldType,
        arguments: Vec<Expression>,
    ) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new(name),
            result_type,
            arguments,
        ))
    }

    fn null_reject_predicate(name: &str, arguments: Vec<Expression>) -> Expression {
        null_reject_function(name, FieldType::new(FieldTypeCode::Tiny), arguments)
    }

    fn null_reject_not_null(argument: Expression) -> Expression {
        null_reject_predicate("not", vec![null_reject_predicate("isnull", vec![argument])])
    }

    fn null_reject_deferred(expression: Expression) -> Expression {
        let result_type = expression.static_type().unwrap().clone();
        let mut constant = Constant::new(Datum::Null, result_type);
        constant.deferred_expr = Some(Box::new(expression));
        Expression::Constant(constant)
    }

    #[test]
    fn test_null_reject_builtin_registry_snapshot() {
        // Parse the same two Go source authorities used by
        // TestNullRejectBuiltinRegistrySnapshot. This keeps the Rust proof
        // table pinned to the actual Go registry without copying 309 names
        // into a second, unauditable fixture.
        let functions = include_str!("../../../../pkg/parser/ast/functions.go");
        let registry = include_str!("../../../../pkg/expression/builtin.go");
        let mut literals = BTreeMap::<String, String>::new();
        let mut aliases = Vec::<(String, String)>::new();
        for line in functions.lines() {
            let Some((left, right)) = line.split_once('=') else {
                continue;
            };
            let identifier = left.trim();
            if identifier.is_empty()
                || !identifier
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                continue;
            }
            let value = right.split("//").next().unwrap_or_default().trim();
            if let Some(value) = value
                .strip_prefix('"')
                .and_then(|value| value.split('"').next())
            {
                literals.insert(identifier.to_owned(), value.to_owned());
            } else if value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                && !value.is_empty()
            {
                aliases.push((identifier.to_owned(), value.to_owned()));
            }
        }
        for _ in 0..aliases.len() {
            let mut changed = false;
            for (alias, target) in &aliases {
                if !literals.contains_key(alias) {
                    if let Some(value) = literals.get(target).cloned() {
                        literals.insert(alias.clone(), value);
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }

        let mut in_registry = false;
        let mut names = BTreeSet::new();
        for line in registry.lines() {
            if line.contains("var funcs = map[string]functionClass{") {
                in_registry = true;
                continue;
            }
            if in_registry && line.trim() == "}" {
                break;
            }
            let Some(rest) = in_registry
                .then_some(line.trim())
                .and_then(|line| line.strip_prefix("ast."))
            else {
                continue;
            };
            let Some((identifier, _)) = rest.split_once(':') else {
                continue;
            };
            names.insert(
                literals
                    .get(identifier.trim())
                    .unwrap_or_else(|| panic!("unresolved Go builtin constant {identifier}"))
                    .clone(),
            );
        }

        assert!(!names.is_empty());
        let joined = names.iter().cloned().collect::<Vec<_>>().join("\n");
        assert_eq!(
            format!("{:x}", Sha256::digest(joined.as_bytes())),
            "729f5252bcd91efe1a4bbf0c383a36c5a2e52ed2d90d7aab0a3e0b450322294c"
        );
        for name in NULL_REJECT_NULL_PRESERVING_FUNCTIONS {
            assert!(
                *name == "cast" || names.contains(*name),
                "NULL-preserving function {name} is absent from the Go registry"
            );
        }
        for (name, _) in NULL_REJECT_REJECT_NULL_TESTS {
            assert!(
                names.contains(*name),
                "NULL test {name} is absent from the Go registry"
            );
        }
    }

    #[test]
    fn test_is_null_rejected_proof_modes() {
        // This table is a direct port of
        // pkg/planner/util/null_misc_test.go::TestIsNullRejectedProofModes.
        let int_type = FieldType::new(FieldTypeCode::LongLong);
        let string_type = FieldType::new(FieldTypeCode::VarString);
        let json_type = FieldType::new(FieldTypeCode::Json);
        let inner_a = null_reject_column(1, int_type.clone());
        let inner_b = null_reject_column(2, int_type.clone());
        let outer_c = null_reject_column(3, int_type.clone());
        let inner_s = null_reject_column(4, string_type.clone());
        let inner_unsigned_d = null_reject_column(5, int_type.clone().with_unsigned(true));
        let inner_date = null_reject_column(6, FieldType::new(FieldTypeCode::Datetime));
        let inner_schema = [1, 2, 4, 5, 6];

        let gt_inner_a_zero =
            null_reject_predicate("gt", vec![inner_a.clone(), null_reject_int(0)]);
        let eq_inner_a_zero =
            null_reject_predicate("eq", vec![inner_a.clone(), null_reject_int(0)]);
        let gt_outer_c_zero =
            null_reject_predicate("gt", vec![outer_c.clone(), null_reject_int(0)]);
        let like_wrapped_inner_a = null_reject_predicate(
            "like",
            vec![
                null_reject_function(
                    "trim",
                    string_type.clone(),
                    vec![null_reject_function(
                        "cast",
                        string_type.clone(),
                        vec![inner_a.clone()],
                    )],
                ),
                null_reject_string("1%"),
                null_reject_int(92),
            ],
        );
        let coalesce_inner_a = null_reject_function(
            "coalesce",
            int_type.clone(),
            vec![inner_a.clone(), null_reject_int(1)],
        );
        let coalesce_inner_a_two = null_reject_function(
            "coalesce",
            int_type.clone(),
            vec![inner_a.clone(), null_reject_int(2)],
        );
        let null_safe_eq_inner_a =
            null_reject_predicate("nulleq", vec![inner_a.clone(), null_reject_int(1)]);
        let field_inner_a = null_reject_function(
            "field",
            int_type.clone(),
            vec![inner_a.clone(), null_reject_int(1)],
        );
        let format_null_locale_eq = null_reject_predicate(
            "eq",
            vec![
                null_reject_function(
                    "format",
                    string_type.clone(),
                    vec![null_reject_int(12345), null_reject_int(0), inner_s.clone()],
                ),
                null_reject_string("12,345"),
            ],
        );
        let quote_inner_s_like_a = null_reject_predicate(
            "like",
            vec![
                null_reject_function("quote", string_type.clone(), vec![inner_s.clone()]),
                null_reject_string("A%"),
                null_reject_int(92),
            ],
        );
        let issue_66824_like_predicate = null_reject_predicate(
            "ge",
            vec![
                null_reject_int(1),
                null_reject_predicate(
                    "and",
                    vec![
                        null_reject_predicate(
                            "or",
                            vec![
                                inner_a.clone(),
                                null_reject_constant(Datum::Null, int_type.clone()),
                            ],
                        ),
                        null_reject_predicate("ne", vec![outer_c.clone(), outer_c.clone()]),
                    ],
                ),
            ],
        );
        let if_inner_a_null_then_zero_else_outer_c = null_reject_function(
            "if",
            int_type.clone(),
            vec![
                null_reject_predicate("isnull", vec![inner_a.clone()]),
                null_reject_int(0),
                outer_c.clone(),
            ],
        );
        let truncate_unsigned_by_nullable_scale = null_reject_function(
            "truncate",
            int_type.clone().with_unsigned(true),
            vec![null_reject_uint(123), inner_unsigned_d],
        );
        let aes_encrypt_ignoring_nullable_iv = null_reject_function(
            "aes_encrypt",
            string_type.clone(),
            vec![
                null_reject_string("pingcap"),
                null_reject_string("123"),
                inner_s.clone(),
            ],
        );
        let aes_decrypt_ignoring_nullable_iv = null_reject_function(
            "aes_decrypt",
            string_type.clone(),
            vec![
                null_reject_function(
                    "unhex",
                    string_type.clone(),
                    vec![null_reject_string("996E0CA8688D7AD20819B90B273E01C6")],
                ),
                null_reject_string("123"),
                inner_s.clone(),
            ],
        );
        let json_set_nullable_value = null_reject_function(
            "json_set",
            json_type.clone(),
            vec![
                null_reject_json("{}"),
                null_reject_string("$.a"),
                inner_s.clone(),
            ],
        );
        let json_insert_nullable_value = null_reject_function(
            "json_insert",
            json_type.clone(),
            vec![
                null_reject_json("{}"),
                null_reject_string("$.a"),
                inner_s.clone(),
            ],
        );
        let json_replace_nullable_value = null_reject_function(
            "json_replace",
            json_type.clone(),
            vec![
                null_reject_json("{\"a\": 1}"),
                null_reject_string("$.a"),
                inner_s.clone(),
            ],
        );
        let json_array_append_nullable_value = null_reject_function(
            "json_array_append",
            json_type.clone(),
            vec![
                null_reject_json("[]"),
                null_reject_string("$"),
                inner_s.clone(),
            ],
        );
        let json_array_insert_nullable_value = null_reject_function(
            "json_array_insert",
            json_type.clone(),
            vec![
                null_reject_json("[]"),
                null_reject_string("$[0]"),
                inner_s.clone(),
            ],
        );
        let json_merge_patch_nullable_doc = null_reject_function(
            "json_merge_patch",
            json_type.clone(),
            vec![
                null_reject_function("cast", json_type.clone(), vec![inner_s.clone()]),
                null_reject_json("null"),
                null_reject_json("{\"a\": 1}"),
                null_reject_json("[1, 2, 3]"),
            ],
        );
        let json_search_nullable_escape = null_reject_function(
            "json_search",
            json_type,
            vec![
                null_reject_json("[\"abc\"]"),
                null_reject_string("one"),
                null_reject_string("abc"),
                inner_s.clone(),
            ],
        );
        let week_with_nullable_mode = null_reject_function(
            "week",
            int_type.clone(),
            vec![null_reject_string("2024-01-08"), inner_a.clone()],
        );
        let yearweek_with_nullable_mode = null_reject_function(
            "yearweek",
            int_type.clone(),
            vec![null_reject_string("2024-01-08"), inner_a.clone()],
        );
        let week_with_nullable_date_and_outer_mode = null_reject_function(
            "week",
            int_type.clone(),
            vec![inner_date.clone(), outer_c.clone()],
        );
        let yearweek_with_nullable_date_and_outer_mode =
            null_reject_function("yearweek", int_type, vec![inner_date, outer_c]);
        let deferred_inner_gt_zero = null_reject_deferred(gt_inner_a_zero.clone());
        let deferred_coalesce_inner_a_two_gt_two = null_reject_deferred(null_reject_predicate(
            "gt",
            vec![coalesce_inner_a_two.clone(), null_reject_int(2)],
        ));
        let deferred_one_with_null_placeholder = null_reject_deferred(null_reject_int(1));

        let cases = vec![
            (
                "or_needs_both_sides_non_true",
                null_reject_predicate(
                    "or",
                    vec![
                        gt_inner_a_zero.clone(),
                        null_reject_predicate("and", vec![eq_inner_a_zero, gt_outer_c_zero]),
                    ],
                ),
                true,
            ),
            (
                "not_uses_must_null",
                null_reject_predicate("not", vec![gt_inner_a_zero]),
                true,
            ),
            (
                "is_null_accepts_null",
                null_reject_predicate("isnull", vec![inner_a.clone()]),
                false,
            ),
            (
                "is_true_rejects_null",
                null_reject_predicate("istrue_with_null", vec![inner_a.clone()]),
                true,
            ),
            (
                "not_is_null_rejects_null",
                null_reject_predicate(
                    "not",
                    vec![null_reject_predicate("isnull", vec![inner_a.clone()])],
                ),
                true,
            ),
            (
                "null_preserving_wrapper_propagates_must_null",
                like_wrapped_inner_a,
                true,
            ),
            (
                "coalesce_constant_fallback_can_still_be_non_true",
                null_reject_predicate("gt", vec![coalesce_inner_a_two, null_reject_int(2)]),
                true,
            ),
            (
                "null_hiding_wrapper_stays_conservative",
                null_reject_predicate("gt", vec![coalesce_inner_a, null_reject_int(0)]),
                false,
            ),
            (
                "in_with_all_list_items_null_rejected",
                null_reject_predicate("in", vec![null_reject_int(1), inner_a.clone(), inner_b]),
                true,
            ),
            (
                "in_with_non_null_candidate_is_not_proven",
                null_reject_predicate(
                    "in",
                    vec![null_reject_int(1), inner_a.clone(), null_reject_int(1)],
                ),
                false,
            ),
            (
                "null_safe_eq_with_non_null_constant_rejects_null",
                null_safe_eq_inner_a,
                true,
            ),
            (
                "format_with_null_locale_is_not_null_rejected",
                format_null_locale_eq,
                false,
            ),
            (
                "field_with_null_input_can_make_not_predicate_true",
                null_reject_predicate(
                    "not",
                    vec![null_reject_predicate(
                        "gt",
                        vec![field_inner_a, null_reject_int(0)],
                    )],
                ),
                false,
            ),
            (
                "quote_with_null_input_can_make_not_predicate_true",
                null_reject_predicate("not", vec![quote_inner_s_like_a]),
                false,
            ),
            (
                "comparison_over_non_true_and_is_not_null_rejected",
                issue_66824_like_predicate,
                false,
            ),
            (
                "if_condition_folded_after_nullification_stays_provable",
                null_reject_predicate(
                    "gt",
                    vec![if_inner_a_null_then_zero_else_outer_c, null_reject_int(0)],
                ),
                true,
            ),
            (
                "truncate_with_unsigned_nullable_scale_is_not_null_preserving",
                null_reject_predicate(
                    "gt",
                    vec![truncate_unsigned_by_nullable_scale, null_reject_int(0)],
                ),
                false,
            ),
            (
                "aes_encrypt_ignores_nullable_iv_in_ecb_mode",
                null_reject_not_null(aes_encrypt_ignoring_nullable_iv),
                false,
            ),
            (
                "aes_decrypt_ignores_nullable_iv_in_ecb_mode",
                null_reject_not_null(aes_decrypt_ignoring_nullable_iv),
                false,
            ),
            (
                "json_set_nullable_value_becomes_json_null",
                null_reject_not_null(json_set_nullable_value),
                false,
            ),
            (
                "json_insert_nullable_value_becomes_json_null",
                null_reject_not_null(json_insert_nullable_value),
                false,
            ),
            (
                "json_replace_nullable_value_becomes_json_null",
                null_reject_not_null(json_replace_nullable_value),
                false,
            ),
            (
                "json_array_append_nullable_value_becomes_json_null",
                null_reject_not_null(json_array_append_nullable_value),
                false,
            ),
            (
                "json_array_insert_nullable_value_becomes_json_null",
                null_reject_not_null(json_array_insert_nullable_value),
                false,
            ),
            (
                "json_merge_patch_nullable_argument_can_still_return_document",
                null_reject_not_null(json_merge_patch_nullable_doc),
                false,
            ),
            (
                "json_search_nullable_escape_falls_back_to_default_escape",
                null_reject_not_null(json_search_nullable_escape),
                false,
            ),
            (
                "week_nullable_mode_uses_default_mode_zero",
                null_reject_predicate("ge", vec![week_with_nullable_mode, null_reject_int(0)]),
                false,
            ),
            (
                "yearweek_nullable_mode_uses_default_mode_zero",
                null_reject_predicate("ge", vec![yearweek_with_nullable_mode, null_reject_int(0)]),
                false,
            ),
            (
                "week_nullable_date_rejects_null_even_with_outer_mode",
                null_reject_predicate(
                    "ge",
                    vec![week_with_nullable_date_and_outer_mode, null_reject_int(0)],
                ),
                true,
            ),
            (
                "yearweek_nullable_date_rejects_null_even_with_outer_mode",
                null_reject_predicate(
                    "ge",
                    vec![
                        yearweek_with_nullable_date_and_outer_mode,
                        null_reject_int(0),
                    ],
                ),
                true,
            ),
            (
                "deferred_expr_uses_symbolic_null_reject_proof",
                deferred_inner_gt_zero,
                true,
            ),
            (
                "deferred_expr_skips_nullified_fold",
                deferred_coalesce_inner_a_two_gt_two,
                false,
            ),
            (
                "deferred_expr_does_not_classify_placeholder_null",
                deferred_one_with_null_placeholder,
                false,
            ),
        ];

        for (name, expression, expected) in cases {
            assert_eq!(
                is_null_rejected(&inner_schema, &expression),
                expected,
                "{name}"
            );
        }
    }

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

        // The vector arithmetic signatures are regular scalar functions too:
        // their declared return type and their value stay in the vector
        // domain all the way through ScalarFunction::eval.
        let vector_type = FieldType::new(FieldTypeCode::VectorFloat32);
        let vector = |values| {
            Expression::Constant(Constant::new(
                Datum::new_vector_float32(VectorFloat32::must_create(values)),
                vector_type.clone(),
            ))
        };
        let vector_plus = Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("plus"),
            vector_type.clone(),
            vec![vector(vec![1.0, 2.0]), vector(vec![3.0, 4.0])],
        ));
        assert_eq!(
            vector_plus.eval(&NoColumns, row).unwrap(),
            Datum::new_vector_float32(VectorFloat32::must_create(vec![4.0, 6.0]))
        );

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

    #[test]
    fn scalar_expression_equality_is_recursive() {
        let ft = FieldType::new(FieldTypeCode::LongLong);
        let scalar = |name: &str, value: i64| {
            Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new(name),
                ft.clone(),
                vec![Expression::Constant(Constant::new(
                    Datum::Int(value),
                    ft.clone(),
                ))],
            ))
        };

        assert!(scalar("YEAR", 1).equal(&scalar("year", 1)));
        assert!(!scalar("year", 1).equal(&scalar("month", 1)));
        assert!(!scalar("year", 1).equal(&scalar("year", 2)));
    }
}
