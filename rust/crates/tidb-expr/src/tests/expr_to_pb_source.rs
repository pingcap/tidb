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

//! Go `pkg/expression/expr_to_pb_test.go`'s conversion tests: the TiPB a
//! column, a function and a by-item become, compared with the JSON Go's
//! tests expect (`testdata/expr_to_pb_test_jsons.txt`, copied from the Go
//! test file line for line; the `JsonCastProbe` rows are Go's answers for
//! the JSON functions' argument casts).
//!
//! `TestConstant2Pb` is skipped by Go, and `TestPanicIfPbCodeUnspecified`
//! exists only under its failpoint; neither is here.

use prost::Message as _;
use serde_json::{json, Value};
use tidb_charset::Collation;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags, SessionTimeZone, StringDatum};
use tidb_proto::tipb::{self, ScalarFuncSig};

use crate::builtin::UnfoldedBuildContext;
use crate::column::Column;
use crate::constant::Constant;
use crate::expr_to_pb::{
    expressions_to_pb_list, group_by_item_to_pb, projection_expressions_to_pb_list, sort_by_item_to_pb,
    PbConverter,
};
use crate::expression::Expression;

const JSONS: &str = include_str!("../../testdata/expr_to_pb_test_jsons.txt");

/// Go `mock.NewContext()`: a statement context with a clock.
struct MockContext;

impl tidb_exprctx::Columns for MockContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        Some((1_700_000_000, 0, 0))
    }
    /// Go `types.DefaultStmtFlags`, a fresh `StmtCtx`'s flags: a truncation
    /// is an error.
    fn truncate_level(&self) -> tidb_exprctx::ErrorLevel {
        tidb_exprctx::ErrorLevel::Error
    }
}

/// The JSON Go's test `name` expects, in the test's order.
fn jsons(name: &str) -> Vec<Value> {
    JSONS
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .filter(|(test, _)| *test == name)
        .map(|(_, json)| serde_json::from_str(json).expect("a Go test JSON"))
        .collect()
}

/// Go `encoding/base64.StdEncoding.EncodeToString`, as `json.Marshal`
/// writes a `[]byte`.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let word = chunk
            .iter()
            .chain([0, 0].iter())
            .take(3)
            .fold(0_u32, |word, byte| word << 8 | u32::from(*byte));
        for position in 0..4 {
            if position <= chunk.len() {
                out.push(ALPHABET[(word >> (18 - 6 * position) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Go `json.Marshal(*tipb.Expr)`.
fn expr_json(expr: &tipb::Expr) -> Value {
    let mut out = serde_json::Map::new();
    out.insert("tp".into(), json!(expr.tp.unwrap_or_default()));
    if let Some(val) = expr.val.as_ref().filter(|val| !val.is_empty()) {
        out.insert("val".into(), json!(base64(val)));
    }
    if !expr.children.is_empty() {
        out.insert(
            "children".into(),
            Value::Array(expr.children.iter().map(expr_json).collect()),
        );
    }
    out.insert("sig".into(), json!(expr.sig.unwrap_or_default()));
    if let Some(field_type) = &expr.field_type {
        let mut tp = serde_json::Map::new();
        tp.insert("tp".into(), json!(field_type.tp.unwrap_or_default()));
        tp.insert("flag".into(), json!(field_type.flag.unwrap_or_default()));
        tp.insert("flen".into(), json!(field_type.flen.unwrap_or_default()));
        tp.insert("decimal".into(), json!(field_type.decimal.unwrap_or_default()));
        tp.insert("collate".into(), json!(field_type.collate.unwrap_or_default()));
        tp.insert("charset".into(), json!(field_type.charset.clone().unwrap_or_default()));
        if !field_type.elems.is_empty() {
            tp.insert("elems".into(), json!(field_type.elems));
        }
        tp.insert("array".into(), json!(field_type.array.unwrap_or_default()));
        out.insert("field_type".into(), Value::Object(tp));
    }
    out.insert("has_distinct".into(), json!(expr.has_distinct.unwrap_or_default()));
    Value::Object(out)
}

/// Go `json.Marshal(*tipb.ByItem)`.
fn by_item_json(item: &tipb::ByItem) -> Value {
    json!({
        "expr": expr_json(item.expr.as_ref().expect("a by-item has its expression")),
        "desc": item.desc.unwrap_or_default(),
    })
}

/// Go `genColumn`.
fn gen_column(tp: FieldTypeCode, id: i64) -> Column {
    let mut column = Column::default();
    column.ret_type = Some(FieldType::new(tp));
    column.id = id;
    column.index = id;
    column
}

fn col(tp: FieldTypeCode, id: i64) -> Expression {
    Expression::Column(gen_column(tp, id))
}

/// Go `columnCollation`.
fn column_collation(mut column: Column, chs: &str, coll: &str) -> Expression {
    let ret_type = column.ret_type.as_mut().expect("genColumn sets the type");
    ret_type.set_charset_name(chs);
    ret_type.set_collation_name(coll);
    Expression::Column(column)
}

/// Go `NewFunction(mock.NewContext(), name, types.NewFieldType(mysql.TypeUnspecified), args...)`.
fn func(name: &str, args: Vec<Expression>) -> Expression {
    crate::new_function::new_function(&MockContext, name, FieldType::new(FieldTypeCode::Unspecified), args)
        .unwrap_or_else(|error| panic!("NewFunction({name}): {error:?}"))
}

/// Go `ExpressionsToPBList(ctx, exprs, client)`, which the tests require to
/// succeed, marshalled.
fn pb_jsons(exprs: &[Expression]) -> Vec<Value> {
    let zone = SessionTimeZone::utc();
    let pc = PbConverter::new(&MockContext, false, &zone);
    expressions_to_pb_list(&pc, exprs)
        .unwrap_or_else(|expr| panic!("{expr:?} has no PB form"))
        .iter()
        .map(expr_json)
        .collect()
}

fn assert_jsons(test: &str, got: &[Value], want: &[Value]) {
    assert_eq!(got.len(), want.len(), "{test}");
    for (index, (got, want)) in got.iter().zip(want).enumerate() {
        assert_eq!(got, want, "{test} #{index}\n got {got}\nwant {want}");
    }
}

#[test]
fn test_column_2_pb() {
    let zone = SessionTimeZone::utc();
    let pc = PbConverter::new(&MockContext, false, &zone);
    for (tp, id) in [
        (FieldTypeCode::Set, 1),
        (FieldTypeCode::Geometry, 2),
        (FieldTypeCode::Unspecified, 3),
    ] {
        // cannot be pushed down
        assert!(expressions_to_pb_list(&pc, &[col(tp, id)]).is_err(), "{tp:?}");
    }

    use FieldTypeCode as T;
    let col_exprs: Vec<Expression> = [
        (T::Tiny, 1),
        (T::Short, 2),
        (T::Long, 3),
        (T::Float, 4),
        (T::Double, 5),
        (T::Null, 6),
        (T::Timestamp, 7),
        (T::LongLong, 8),
        (T::Int24, 9),
        (T::Date, 10),
        (T::Duration, 11),
        (T::Datetime, 12),
        (T::Year, 13),
        (T::Varchar, 15),
        (T::Json, 16),
        (T::NewDecimal, 17),
        (T::TinyBlob, 18),
        (T::MediumBlob, 19),
        (T::LongBlob, 20),
        (T::Blob, 21),
        (T::VarString, 22),
        (T::String, 23),
        (T::Enum, 24),
        (T::Bit, 25),
    ]
    .into_iter()
    .map(|(tp, id)| col(tp, id))
    .collect();
    assert_jsons("TestColumn2Pb", &pb_jsons(&col_exprs), &jsons("TestColumn2Pb"));
}

#[test]
fn test_compare_func_2_pb() {
    let compare_exprs: Vec<Expression> = ["lt", "le", "gt", "ge", "eq", "ne", "nulleq"]
        .into_iter()
        .map(|name| func(name, vec![col(FieldTypeCode::LongLong, 1), col(FieldTypeCode::LongLong, 2)]))
        .collect();
    assert_jsons("TestCompareFunc2Pb", &pb_jsons(&compare_exprs), &jsons("TestCompareFunc2Pb"));
}

#[test]
fn test_like_func_2_pb() {
    let mut ret_tp = FieldType::new(FieldTypeCode::String);
    ret_tp.add_flags(FieldTypeFlags::NOT_NULL);
    ret_tp.set_charset_name("utf8");
    ret_tp.set_collation_name("utf8_bin");
    let args: Vec<Expression> = ["string", "pattern", "%abc%", "\\"]
        .into_iter()
        .map(|value| {
            Expression::Constant(Constant::new(
                Datum::String(StringDatum::new(value, Collation::Utf8Mb4Bin)),
                ret_tp.clone(),
            ))
        })
        .collect();
    let like_funcs = [
        func("like", vec![args[0].clone(), args[1].clone(), args[3].clone()]),
        func("like", vec![args[0].clone(), args[2].clone(), args[3].clone()]),
    ];
    assert_jsons("TestLikeFunc2Pb", &pb_jsons(&like_funcs), &jsons("TestLikeFunc2Pb"));
}

#[test]
fn test_arithmetical_func_2_pb() {
    // The file holds Go's map in insertion order: Plus, Minus, Mul, Div, the
    // Mod entry no expression reads, and IntDiv's own JSON.
    let want = jsons("TestArithmeticalFunc2Pb");
    let arithmetical_funcs: Vec<Expression> = ["plus", "minus", "mul", "div"]
        .into_iter()
        .map(|name| func(name, vec![col(FieldTypeCode::Double, 1), col(FieldTypeCode::Double, 2)]))
        .collect();
    assert_jsons("TestArithmeticalFunc2Pb", &pb_jsons(&arithmetical_funcs), &want[..4]);

    // IntDiv
    let fc = func("intdiv", vec![col(FieldTypeCode::LongLong, 1), col(FieldTypeCode::LongLong, 2)]);
    assert_jsons("TestArithmeticalFunc2Pb IntDiv", &pb_jsons(&[fc]), &want[5..]);
}

#[test]
fn test_date_func_2_pb() {
    let fc = func(
        "date_format",
        vec![col(FieldTypeCode::Datetime, 1), col(FieldTypeCode::String, 2)],
    );
    assert_jsons("TestDateFunc2Pb", &pb_jsons(&[fc]), &jsons("TestDateFunc2Pb"));
}

#[test]
fn test_logical_func_2_pb() {
    let func_names = ["and", "or", "xor", "not"];
    let logical_funcs: Vec<Expression> = func_names
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            let mut args = vec![col(FieldTypeCode::Tiny, 1)];
            if i + 1 < func_names.len() {
                args.push(col(FieldTypeCode::Tiny, 2));
            }
            func(name, args)
        })
        .collect();
    assert_jsons("TestLogicalFunc2Pb", &pb_jsons(&logical_funcs), &jsons("TestLogicalFunc2Pb"));
}

#[test]
fn test_bitwise_func_2_pb() {
    let func_names = ["bitand", "bitor", "bitxor", "leftshift", "rightshift", "bitneg"];
    let bitwise_funcs: Vec<Expression> = func_names
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            let mut args = vec![col(FieldTypeCode::Long, 1)];
            if i + 1 < func_names.len() {
                args.push(col(FieldTypeCode::Long, 2));
            }
            func(name, args)
        })
        .collect();
    assert_jsons("TestBitwiseFunc2Pb", &pb_jsons(&bitwise_funcs), &jsons("TestBitwiseFunc2Pb"));
}

#[test]
fn test_control_func_2_pb() {
    let control_funcs: Vec<Expression> = ["case", "if", "ifnull"]
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            let mut args = vec![col(FieldTypeCode::Long, 1), col(FieldTypeCode::Long, 2)];
            if i < 2 {
                args.push(col(FieldTypeCode::Long, 3));
            }
            func(name, args)
        })
        .collect();
    // Go's fourth JSON, "null", has no expression.
    assert_jsons(
        "TestControlFunc2Pb",
        &pb_jsons(&control_funcs),
        &jsons("TestControlFunc2Pb")[..3],
    );
}

#[test]
fn test_other_func_2_pb() {
    let other_funcs: Vec<Expression> = ["coalesce", "isnull"]
        .into_iter()
        .map(|name| func(name, vec![col(FieldTypeCode::Long, 1)]))
        .collect();
    assert_jsons("TestOtherFunc2Pb", &pb_jsons(&other_funcs), &jsons("TestOtherFunc2Pb"));
}

/// The JSON `TestGroupByItem2Pb` and `TestSortByItem2Pb` write inline.
fn double_by_item(val: &str, desc: bool) -> Value {
    json!({
        "expr": {
            "tp": 201, "val": val, "sig": 0,
            "field_type": {
                "tp": 5, "flag": 0, "flen": -1, "decimal": -1,
                "collate": -63, "charset": "binary", "array": false,
            },
            "has_distinct": false,
        },
        "desc": desc,
    })
}

#[test]
fn test_group_by_item_2_pb() {
    let zone = SessionTimeZone::utc();
    let pc = PbConverter::new(&MockContext, false, &zone);
    for (id, val) in [(0, "gAAAAAAAAAA="), (1, "gAAAAAAAAAE=")] {
        let item = col(FieldTypeCode::Double, id);
        let pb_by_item = group_by_item_to_pb(&pc, &item).unwrap();
        assert_eq!(by_item_json(&pb_by_item), double_by_item(val, false));
    }
}

#[test]
fn test_sort_by_item_2_pb() {
    let zone = SessionTimeZone::utc();
    let pc = PbConverter::new(&MockContext, false, &zone);
    for (id, val, desc) in [(0, "gAAAAAAAAAA=", false), (1, "gAAAAAAAAAE=", false), (1, "gAAAAAAAAAE=", true)] {
        let item = col(FieldTypeCode::Double, id);
        let pb_by_item = sort_by_item_to_pb(&pc, &item, desc).unwrap();
        assert_eq!(by_item_json(&pb_by_item), double_by_item(val, desc));
    }
}

#[test]
fn test_push_collation_down() {
    let Expression::ScalarFunction(mut fc) =
        func("eq", vec![col(FieldTypeCode::Varchar, 0), col(FieldTypeCode::Varchar, 1)])
    else {
        panic!("eq builds a function");
    };
    let zone = SessionTimeZone::utc();
    let pc = PbConverter::new(&MockContext, false, &zone);
    let tps = [FieldType::new(FieldTypeCode::Varchar), FieldType::new(FieldTypeCode::Varchar)];
    for coll in ["binary", "latin1_bin", "utf8_bin", "utf8mb4_bin"] {
        fc.collation.set_charset_and_collation("binary", coll); // only collation matters
        let pb_expr = expressions_to_pb_list(&pc, &[Expression::ScalarFunction(fc.clone())]).unwrap();
        let expr = crate::distsql_builtin::pb_to_expr(&pb_expr[0], &tps).unwrap();
        let Expression::ScalarFunction(expr) = expr else {
            panic!("PBToExpr answers a function");
        };
        assert_eq!(expr.collation.charset_and_collation().1, coll);
    }
}

#[test]
fn test_new_collations_enabled() {
    use FieldTypeCode as T;
    let col_exprs = [
        col(T::Varchar, 1),
        column_collation(gen_column(T::Varchar, 2), "some_invalid_charset", "some_invalid_collation"),
        column_collation(gen_column(T::VarString, 3), "utf8mb4", "utf8mb4_general_ci"),
        column_collation(gen_column(T::String, 4), "utf8mb4", "utf8mb4_0900_ai_ci"),
        column_collation(gen_column(T::Varchar, 5), "utf8", "utf8_bin"),
        column_collation(gen_column(T::Varchar, 6), "utf8", "utf8_unicode_ci"),
        column_collation(gen_column(T::Varchar, 7), "utf8mb4", "utf8mb4_zh_pinyin_tidb_as_cs"),
    ];
    let want = jsons("TestNewCollationsEnabled");
    assert_jsons("TestNewCollationsEnabled", &pb_jsons(&col_exprs), &want[..7]);

    let zone = SessionTimeZone::utc();
    let pc = PbConverter::new(&MockContext, false, &zone);
    let item = column_collation(gen_column(T::Double, 0), "utf8mb4", "utf8mb4_0900_ai_ci");
    let pb_by_item = group_by_item_to_pb(&pc, &item).unwrap();
    assert_eq!(by_item_json(&pb_by_item), want[7]);
}

#[test]
fn test_metadata() {
    let zone = SessionTimeZone::utc();
    let pc = PbConverter::new(&MockContext, false, &zone);
    let in_union = |val: &[u8]| tipb::InUnionMetadata::decode(val).unwrap().in_union;

    // InUnion flag is false in `BuildCastFunction` when `ScalarFuncSig_CastStringAsInt`
    let cast = crate::builtin_cast::build_cast_function(
        &UnfoldedBuildContext,
        col(FieldTypeCode::String, 1),
        FieldType::new(FieldTypeCode::LongLong),
    )
    .unwrap();
    let expr = pc.expr_to_pb(&cast).unwrap();
    assert_eq!(expr.sig, Some(ScalarFuncSig::CastStringAsInt as i32));
    let val = expr.val.unwrap();
    assert!(!val.is_empty());
    assert!(!in_union(&val));

    // InUnion flag is nil in `BuildCastFunction4Union` when `ScalarFuncSig_CastIntAsString`
    let cast_in_union = crate::wrap_cast::build_cast_function_4_union(
        &UnfoldedBuildContext,
        col(FieldTypeCode::LongLong, 1),
        FieldType::new(FieldTypeCode::String),
    )
    .unwrap();
    let expr = pc.expr_to_pb(&cast_in_union).unwrap();
    assert_eq!(expr.sig, Some(ScalarFuncSig::CastIntAsString as i32));
    assert_eq!(expr.val.unwrap_or_default().len(), 0);

    // InUnion flag is true in `BuildCastFunction4Union` when `ScalarFuncSig_CastStringAsInt`
    let cast_in_union = crate::wrap_cast::build_cast_function_4_union(
        &UnfoldedBuildContext,
        col(FieldTypeCode::String, 1),
        FieldType::new(FieldTypeCode::LongLong),
    )
    .unwrap();
    let expr = pc.expr_to_pb(&cast_in_union).unwrap();
    assert_eq!(expr.sig, Some(ScalarFuncSig::CastStringAsInt as i32));
    let val = expr.val.unwrap();
    assert!(!val.is_empty());
    assert!(in_union(&val));
}

#[test]
fn test_projection_column_2_pb() {
    let zone = SessionTimeZone::utc();
    let pc = PbConverter::new(&MockContext, false, &zone);
    let col_exprs = [
        col(FieldTypeCode::Set, 1),
        col(FieldTypeCode::Short, 2),
        col(FieldTypeCode::Long, 3),
    ];

    // TypeSet column can't be converted to PB by default
    assert!(expressions_to_pb_list(&pc, &col_exprs).is_err());

    assert!(projection_expressions_to_pb_list(&pc, &col_exprs).is_ok());
}

/// The JSON functions cast their arguments by the argument's own type; the
/// rows are Go's `ExprToPB` of the same calls.
#[test]
fn json_function_argument_casts_match_go() {
    let column = |index: i64, tp: FieldTypeCode, charset: Option<(&str, &str)>, flags: u32| {
        let mut column = gen_column(tp, index);
        column.id = index + 1;
        column.unique_id = index + 1;
        let ret_type = column.ret_type.as_mut().unwrap();
        if let Some((chs, coll)) = charset {
            ret_type.set_charset_name(chs);
            ret_type.set_collation_name(coll);
        }
        ret_type.add_flags(flags);
        Expression::Column(column)
    };
    use FieldTypeCode as T;
    let jb = || column(0, T::Json, Some(("binary", "binary")), FieldTypeFlags::BINARY);
    let ju = || column(0, T::Json, Some(("utf8mb4", "utf8mb4_bin")), FieldTypeFlags::BINARY);
    let int_not_null = || column(1, T::LongLong, None, FieldTypeFlags::NOT_NULL);
    let i = || column(1, T::LongLong, None, 0);
    let s = || column(2, T::VarString, Some(("utf8mb4", "utf8mb4_general_ci")), 0);
    let r = || column(3, T::Double, None, 0);
    let d = || column(4, T::NewDecimal, None, 0);
    let exprs = [
        func("json_array", vec![jb(), int_not_null()]),
        func("json_array", vec![ju(), i()]),
        func("json_array", vec![s(), r(), d()]),
        func("json_set", vec![jb(), s(), int_not_null()]),
        func("json_insert", vec![ju(), s(), s()]),
        func("json_length", vec![s()]),
        func("json_length", vec![i()]),
    ];
    assert_jsons("JsonCastProbe", &pb_jsons(&exprs), &jsons("JsonCastProbe"));
}
