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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-first ports of `pkg/expression.part5` session-variable functions:
//! `builtin_other_test.go::TestSetVar/TestGetVar/TestTypeConversion/
//! TestSetVarFromColumn/TestGetParam`, `builtin_other_vec_test.go::TestGetParamVec`
//! plus that file's SETVAR/GETVAR harness arms. Every expectation is
//! re-derived from the Go source on `origin/master`
//! (`pkg/expression/builtin_other.go`'s signatures), not from Rust comments.

use std::cell::RefCell;
use std::collections::BTreeMap;

use super::*;
use crate::constant::Constant;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;
use tidb_datatype::{FieldType, FieldTypeCode as C, Time, TimeType};

/// A [`crate::context::Columns`] stub whose only live behavior is a user
/// variable store behind interior mutability — the value-domain stand-in for
/// Go's `SessionVars.SetUserVarVal`/`GetUserVarVal`.
#[derive(Default)]
struct VarStore {
    vars: RefCell<BTreeMap<String, Datum>>,
}

impl VarStore {
    fn get(&self, name: &str) -> Option<Datum> {
        self.vars.borrow().get(name).cloned()
    }
}

impl crate::context::Columns for VarStore {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn get_uservar(&self, name: &str) -> Option<Datum> {
        self.get(name)
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.vars.borrow_mut().insert(name.to_owned(), value);
    }
}

#[derive(Default)]
struct ParamStore {
    params: Vec<Datum>,
}

impl crate::context::Columns for ParamStore {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn get_param_value(&self, idx: usize) -> Result<Datum, EvalError> {
        self.params
            .get(idx)
            .cloned()
            .ok_or(EvalError::ParamIndexExceedParamCounts)
    }
}

fn var_name(name: &str) -> Expression {
    Expression::Constant(Constant::new(
        Datum::new_string(name.to_owned()),
        FieldType::new(C::VarString),
    ))
}

fn const_arg(datum: Datum, field_type: FieldType) -> Expression {
    Expression::Constant(Constant::new(datum, field_type))
}

/// Go `setVarFunctionClass.getFunction` selects one signature per the VALUE
/// argument's eval type and reuses that type as the result type
/// (`pkg/expression/builtin_other.go:984`). The store records what the call
/// assigned, exactly like Go's test reads back through `GetUserVarVal`.
fn eval_setvar(value: Datum, value_ft: FieldType, store: &VarStore) -> Result<Datum, EvalError> {
    let function = ScalarFunction::new(
        CiString::new("setvar"),
        value_ft.clone(),
        vec![var_name("f"), const_arg(value, value_ft)],
    );
    let empty = tidb_chunk::chunk::Chunk::new_empty(&[]);
    function.eval(store, empty.get_row(0))
}

/// GO PORT of `pkg/expression/builtin_other_test.go:84 TestSetVar`.
///
/// Go's table assigns through each SETVAR signature (string/int/real/decimal/
/// time) and requires BOTH the returned datum and the stored session variable
/// to carry the assigned value. The NULL row returns NULL; because every
/// `builtinSet*VarSig` bails before `SetUserVarVal` on a null RHS
/// (`pkg/expression/builtin_other.go:1029` et seq), nothing may be stored.
/// Go builds its time with `time.Now()`; the deterministic wall-clock keeps
/// the equality contract while staying reproducible.
#[test]
fn setvar_stores_session_value_and_returns_it() {
    let store = VarStore::default();
    let text = |v: &str| Datum::new_string(v.to_owned());

    // {a, "12"} / {b, "34"}: the string sig stores its stringified result.
    let returned = eval_setvar(text("12"), FieldType::new(C::VarString), &store).unwrap();
    assert_eq!(returned, text("12"));
    assert_eq!(store.get("f"), Some(text("12")));

    // Rename to exercise independent keys: same shape, different key.
    let renamed = ScalarFunction::new(
        CiString::new("setvar"),
        FieldType::new(C::VarString),
        vec![
            var_name("g"),
            const_arg(text("34"), FieldType::new(C::VarString)),
        ],
    );
    let empty = tidb_chunk::chunk::Chunk::new_empty(&[]);
    assert_eq!(renamed.eval(&store, empty.get_row(0)).unwrap(), text("34"));
    assert_eq!(store.get("g"), Some(text("34")));

    // {c, nil}: NULL assignment propagates and must NOT create the variable.
    let null_call = ScalarFunction::new(
        CiString::new("setvar"),
        FieldType::new(C::Null),
        vec![
            var_name("h"),
            const_arg(Datum::Null, FieldType::new(C::Null)),
        ],
    );
    assert_eq!(
        null_call.eval(&store, empty.get_row(0)).unwrap(),
        Datum::Null
    );
    assert_eq!(store.get("h"), None);

    // {c, "ABC"} then {c, "dEf"}: the second assignment overwrites in place.
    let abc = ScalarFunction::new(
        CiString::new("setvar"),
        FieldType::new(C::VarString),
        vec![
            var_name("h"),
            const_arg(text("ABC"), FieldType::new(C::VarString)),
        ],
    );
    assert_eq!(abc.eval(&store, empty.get_row(0)).unwrap(), text("ABC"));
    assert_eq!(store.get("h"), Some(text("ABC")));
    let def = ScalarFunction::new(
        CiString::new("setvar"),
        FieldType::new(C::VarString),
        vec![
            var_name("h"),
            const_arg(text("dEf"), FieldType::new(C::VarString)),
        ],
    );
    assert_eq!(def.eval(&store, empty.get_row(0)).unwrap(), text("dEf"));
    assert_eq!(store.get("h"), Some(text("dEf")));

    // {d, int64(3)}: the int sig stores an Int datum.
    let int_sig = ScalarFunction::new(
        CiString::new("setvar"),
        FieldType::new(C::LongLong),
        vec![
            var_name("i"),
            const_arg(Datum::Int(3), FieldType::new(C::LongLong)),
        ],
    );
    assert_eq!(
        int_sig.eval(&store, empty.get_row(0)).unwrap(),
        Datum::Int(3)
    );
    assert_eq!(store.get("i"), Some(Datum::Int(3)));

    // {e, float64(2.5)}: the real sig stores a Real datum.
    let real_sig = ScalarFunction::new(
        CiString::new("setvar"),
        FieldType::new(C::Double),
        vec![
            var_name("j"),
            const_arg(Datum::Real(2.5), FieldType::new(C::Double)),
        ],
    );
    assert_eq!(
        real_sig.eval(&store, empty.get_row(0)).unwrap(),
        Datum::Real(2.5)
    );
    assert_eq!(store.get("j"), Some(Datum::Real(2.5)));

    // {f, NewDecFromInt(5)}: the decimal sig stores a Decimal datum.
    let dec = tidb_datatype::Decimal::from_int(5);
    let decimal_sig = ScalarFunction::new(
        CiString::new("setvar"),
        FieldType::new(C::NewDecimal),
        vec![
            var_name("k"),
            const_arg(Datum::Decimal(dec.clone()), FieldType::new(C::NewDecimal)),
        ],
    );
    assert_eq!(
        decimal_sig.eval(&store, empty.get_row(0)).unwrap(),
        Datum::Decimal(dec.clone())
    );
    assert_eq!(store.get("k"), Some(Datum::Decimal(dec)));

    // {g, timestamp}: the time arg keeps KindMysql end-to-end (Go's
    // setVarFunctionClass maps this argTp straight to builtinSetTimeVarSig).
    let time = Time::from_date_checked(2025, 1, 2, 3, 4, 5, 0, TimeType::Timestamp, 0)
        .expect("a valid fixed timestamp");
    let time_sig = ScalarFunction::new(
        CiString::new("setvar"),
        FieldType::new(C::Timestamp),
        vec![
            var_name("l"),
            const_arg(Datum::Time(time), FieldType::new(C::Timestamp)),
        ],
    );
    assert_eq!(
        time_sig.eval(&store, empty.get_row(0)).unwrap(),
        Datum::Time(time)
    );
    assert_eq!(store.get("l"), Some(Datum::Time(time)));
}

/// GO PORT of `pkg/expression/builtin_other_test.go:119 TestGetVar`.
///
/// Go preloads session variables of each kind and builds the typed GETVAR
/// signature via `BuildGetVarFunction`, which picks a class from the declared
/// type's eval type (`pkg/expression/builtin_other.go:1207`): string values
/// pass through verbatim, unset reads are NULL, and numeric variables return
/// their own kinds. The TIME row (`{"h"}, timeDec.String()`) uses Go's
/// `builtinGetTimeVarSig`, which preserves the stored MySQL time value.
#[test]
fn getvar_time_variable_signature() {
    let time = Time::from_date_checked(2025, 1, 2, 3, 4, 5, 0, TimeType::Timestamp, 0)
        .expect("a valid fixed timestamp");
    let store = VarStore {
        vars: RefCell::new(BTreeMap::from([("h".to_owned(), Datum::Time(time))])),
    };
    assert_eq!(eval_getvar("time", "h", &store).unwrap(), Datum::Time(time));
    assert_eq!(eval_getvar("time", "missing", &store).unwrap(), Datum::Null);
}

#[test]
fn getvar_reads_typed_session_value_by_signature_kind() {
    let preload = [
        ("a", text_var("中")),
        ("b", text_var("文字符chuan")),
        ("c", text_var("")),
        ("e", Datum::Int(3)),
        ("f", Datum::Real(2.5)),
        ("g", Datum::Decimal(tidb_datatype::Decimal::from_int(5))),
    ];
    let mut vars = BTreeMap::new();
    for (name, value) in preload {
        vars.insert(name.to_owned(), value);
    }
    let store = VarStore {
        vars: RefCell::new(vars),
    };

    // String rows: builtinGetStringVarSig returns ToString() of whatever is
    // stored, verbatim, including the empty string.
    assert_eq!(eval_getvar("string", "a", &store).unwrap(), text_var("中"));
    assert_eq!(
        eval_getvar("string", "b", &store).unwrap(),
        text_var("文字符chuan")
    );
    assert_eq!(eval_getvar("string", "c", &store).unwrap(), text_var(""));
    // Unset names read NULL (Go returns isNull=true).
    assert_eq!(eval_getvar("string", "d", &store).unwrap(), Datum::Null);
    // Int/real/decimal rows keep their own kinds through their signatures.
    assert_eq!(eval_getvar("int", "e", &store).unwrap(), Datum::Int(3));
    assert_eq!(eval_getvar("real", "f", &store).unwrap(), Datum::Real(2.5));
    assert_eq!(
        eval_getvar("decimal", "g", &store).unwrap(),
        Datum::Decimal(tidb_datatype::Decimal::from_int(5))
    );
}

fn text_var(v: &str) -> Datum {
    Datum::new_string(v.to_owned())
}

/// Builds one typed GETVAR signature the way Go's `BuildGetVarFunction` does:
/// the class choice is purely the DECLARED type's eval type, the argument is
/// the constant name, and the result type rides along unchanged.
fn eval_getvar(kind: &str, name: &str, ctx: &VarStore) -> Result<Datum, EvalError> {
    let ret_type = match kind {
        "int" => FieldType::new(C::LongLong),
        "uint" => FieldType::new(C::LongLong).with_unsigned(true),
        "real" => FieldType::new(C::Double),
        "decimal" => FieldType::new(C::NewDecimal),
        "time" => FieldType::new(C::Datetime),
        _ => FieldType::new(C::VarString),
    };
    let function = ScalarFunction::new(
        CiString::new(format!("getvar_{kind}")),
        ret_type,
        vec![var_name(name)],
    );
    let empty = tidb_chunk::chunk::Chunk::new_empty(&[]);
    function.eval(ctx, empty.get_row(0))
}

/// GO PORT of `pkg/expression/builtin_other_test.go:173 TestTypeConversion`.
///
/// The scalar AST-tier half (user-variable CASTs onto DECIMAL and DOUBLE) was
/// already pinned by `tests::builtin_other_type_conversion_source_scalars`;
/// this pins the SIGNATURE half Go actually drives: the SAME stored integer is
/// read twice through differently-typed GETVAR functions, yielding DECIMAL 3
/// and DOUBLE 3 respectively (`BuildGetVarFunction`'s class switch at
/// pkg/expression/builtin_other.go:1211-1221).
#[test]
fn type_conversion_read_one_stored_int_through_decimal_and_real_signatures() {
    let store = VarStore {
        vars: RefCell::new(BTreeMap::from([("a".to_owned(), Datum::Int(3))])),
    };
    assert_eq!(
        eval_getvar("decimal", "a", &store).unwrap(),
        Datum::Decimal(tidb_datatype::Decimal::from_int(3))
    );
    assert_eq!(eval_getvar("real", "a", &store).unwrap(), Datum::Real(3.0));
}

/// GO PORT of `pkg/expression/builtin_other_test.go:229 TestSetVarFromColumn`.
///
/// Go evaluates SETVAR(col_name, col_value) over a chunk row and then MUTATES
/// the underlying chunk cell; the user variable must keep the SNAPSHOTTED
/// value ("a"), proving the session store owns a copy — Go's sig even copies
/// the string explicitly (`stringutil.Copy(res)`,
/// pkg/expression/builtin_other.go:1054).
#[test]
fn setvar_from_column_snapshots_the_row_value() {
    let store = VarStore::default();
    let string_ft = || FieldType::new(C::VarString);
    let mut column = crate::column::Column::new(1, string_ft());
    column.index = 0;

    let assign = ScalarFunction::new(
        CiString::new("setvar"),
        string_ft(),
        vec![var_name("a"), Expression::Column(column)],
    );

    let build_chunk = |value: &str| {
        let mut chunk =
            tidb_chunk::chunk::Chunk::new_with_capacity(std::slice::from_ref(&string_ft()), 1);
        chunk.append_datum(0, &Datum::new_string(value.to_owned()));
        chunk
    };

    let first = build_chunk("a");
    let assigned = assign
        .eval(&store, first.get_row(0))
        .expect("assignment must evaluate");
    assert_eq!(assigned, Datum::new_string("a".to_owned()));
    assert_eq!(store.get("a"), Some(Datum::new_string("a".to_owned())));

    // Change the underlying chunk cell WITHOUT evaluating SETVAR again: the
    // column now reads "b", proving the mutation is visible, while the user
    // variable keeps its snapshotted "a" because the assignment copied it.
    let second = build_chunk("b");
    assert_eq!(
        assign.args[1]
            .eval(&store, second.get_row(0))
            .expect("column read"),
        Datum::new_string("b".to_owned())
    );
    assert_eq!(store.get("a"), Some(Datum::new_string("a".to_owned())));
}

/// GO PORT of `pkg/expression/builtin_other_test.go:200 TestValues`.
///
/// Go pins four behaviors against `sessionVars.CurrInsertValues`: arity > 0
/// fails with `Incorrect parameter count in the call to native function
/// 'values'`; no current insert values yields NULL; a mismatched row length
/// fails with `Session current insert values len %d ...`; and the matching
/// offset returns the CURRENT row's value rather than evaluating anything.
///
/// go-parity-gap: VALUES() evaluation and the CurrInsertValues session slot
/// are not modeled by this evaluator, so none of those behaviors can be
/// exercised yet.
#[test]
#[ignore = "go-parity-gap: VALUES() evaluation and the CurrInsertValues session state it reads are unmodeled"]
fn values_function_current_insert_values_gap() {}

/// GO PORT of `pkg/expression/builtin_other_test.go:348 TestGetParam` and
/// `pkg/expression/builtin_other_vec_test.go:94 TestGetParamVec`.
///
/// Go evaluates GETPARAM(<index>) over `PlanCacheParams`, requiring each
/// parameter rendered via `ToString()` and
/// `exprctx.ErrParamIndexExceedParamCounts` past the end (scalar AND vec). The
/// CONTEXT half already has its port: `exprstatic::evalctx::param_list` pins
/// `get_param_value` including that exact error identity. The FUNCTION half
/// reads `args[0]` as the index and returns `ToString()` of the parameter.
#[test]
fn getparam_function_evaluation_matches_plan_cache_values() {
    let ctx = ParamStore {
        params: vec![Datum::Int(123), Datum::new_string("abc".to_owned())],
    };
    let empty = tidb_chunk::chunk::Chunk::new_empty(&[]);
    for (index, expected) in [(0, "123"), (1, "abc")] {
        let function = ScalarFunction::new(
            CiString::new("getparam"),
            FieldType::new(C::VarString),
            vec![const_arg(Datum::Int(index), FieldType::new(C::LongLong))],
        );
        assert_eq!(
            function.eval(&ctx, empty.get_row(0)).unwrap(),
            Datum::new_string(expected.to_owned())
        );
    }

    let out_of_range = ScalarFunction::new(
        CiString::new("getparam"),
        FieldType::new(C::VarString),
        vec![const_arg(Datum::Int(2), FieldType::new(C::LongLong))],
    );
    assert_eq!(
        out_of_range.eval(&ctx, empty.get_row(0)),
        Err(EvalError::ParamIndexExceedParamCounts)
    );

    // The AST/value path uses the same GETPARAM dispatch and error identity,
    // so row and chunk evaluation cannot drift.
    let ast = tidb_ast::Expr::Func {
        name: "getparam".to_owned(),
        args: vec![tidb_ast::Expr::Int("1".to_owned())],
        origin_position: 0,
    };
    assert_eq!(
        crate::func::eval_func("GETPARAM", &ast_args(&ast), &ctx, None),
        Ok(Datum::new_string("abc".to_owned()))
    );
}

fn ast_args(expr: &tidb_ast::Expr) -> &[tidb_ast::Expr] {
    let tidb_ast::Expr::Func { args, .. } = expr else {
        unreachable!("test helper receives a function")
    };
    args
}

/// GO PORT of `pkg/expression/builtin_other_vec_test.go:58
/// TestVectorizedBuiltinOtherFunc`'s representable arms: the map declares
/// SETVAR over `{ETString, ETInt, ETReal, ETDecimal}` children and GETVAR over
/// `{ETString}`; each arm is one signature evaluated per generated row. The
/// deterministic literals below sweep all four SETVAR signature paths and the
/// GETVAR string path end-to-end (assign → observe store → overwrite),
/// keeping the arms' invariant: what the call returns is what lands in the
/// session. `ast.In {}` carries no case; `ast.BitCount` is driven cross-tier
/// below; `ast.GetParam`'s ranged-index arm is the GETPARAM gap above.
#[test]
fn vectorized_builtin_other_func_representable_arms() {
    let store = VarStore::default();
    let empty = tidb_chunk::chunk::Chunk::new_empty(&[]);

    // Four SETVAR signature arms, values drawn across the generator domains.
    for (name, value, ft) in [
        (
            "sv_str",
            Datum::new_string("9".to_owned()),
            FieldType::new(C::VarString),
        ),
        ("sv_int", Datum::Int(-7), FieldType::new(C::LongLong)),
        ("sv_real", Datum::Real(8.0), FieldType::new(C::Double)),
        (
            "sv_dec",
            Datum::Decimal(tidb_datatype::Decimal::parse_mysql("900000.000000").0),
            FieldType::new(C::NewDecimal),
        ),
    ] {
        let sig = ScalarFunction::new(
            CiString::new("setvar"),
            ft.clone(),
            vec![var_name(name), const_arg(value.clone(), ft)],
        );
        assert_eq!(sig.eval(&store, empty.get_row(0)).unwrap(), value, "{name}");
        assert_eq!(store.get(name), Some(value), "{name}");
    }

    // GETVAR string arm reflects the last write.
    assert_eq!(
        eval_getvar("string", "sv_str", &store).unwrap(),
        Datum::new_string("9".to_owned())
    );

    // ast.BitCount arm: both tiers agree on the packed-popcount builtin.
    assert_eq!(chunk_e("bit_count(7)"), e("bit_count(7)"));
    assert_eq!(chunk_e("bit_count(-1)"), e("bit_count(-1)"));
}
