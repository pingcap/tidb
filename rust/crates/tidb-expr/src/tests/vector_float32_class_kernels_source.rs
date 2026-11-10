//! The VectorFloat32 arms of the control, comparison and arithmetic classes,
//! and the classes whose signatures set no pbCode, evaluated through their
//! function classes. Every expected value is what Go TiDB returned for the
//! SQL quoted beside it (testkit probe, 2026-11-11).

use super::*;
use tidb_datatype::{FieldType, FieldTypeCode as C, VectorFloat32};

use crate::expression::Expression;

fn v(values: &[f32]) -> Datum {
    Datum::new_vector_float32(VectorFloat32::must_create(values.to_vec()))
}

fn i(value: i64) -> Datum {
    Datum::Int(value)
}

fn s(text: &str) -> Datum {
    Datum::String(tidb_datatype::StringDatum::new(text, tidb_charset::Collation::Utf8Mb4Bin))
}

/// A NULL of the vector type, as a NULL vector column value reads.
const NULL_VECTOR: Option<Datum> = None;

fn const_arg(datum: Datum, null_type: C) -> Expression {
    let field_type = match &datum {
        Datum::Null => FieldType::new(null_type),
        Datum::Int(_) => FieldType::new(C::LongLong),
        Datum::String(_) => FieldType::new(C::VarString),
        Datum::VectorFloat32(_) => FieldType::new(C::VectorFloat32),
        other => panic!("no test mapping for {other:?}"),
    };
    Expression::Constant(crate::constant::Constant::new(datum, field_type))
}

fn eval(name: &str, args: Vec<Option<Datum>>, ret: C) -> Result<Datum, crate::EvalError> {
    let function = crate::new_function::new_function_base(
        &crate::NoColumns,
        name,
        FieldType::new(ret),
        args.into_iter()
            .map(|arg| match arg {
                Some(datum) => const_arg(datum, C::Null),
                None => const_arg(Datum::Null, C::VectorFloat32),
            })
            .collect(),
    )
    .unwrap()
    .into_scalar_function()
    .unwrap();
    let cols = tidb_exprctx::context::ZonedNoColumns(tidb_datatype::SessionTimeZone::utc());
    let empty = tidb_chunk::chunk::Chunk::new_chunk_with_capacity(&[], 1);
    function.eval(&cols, empty.get_row(0))
}

fn check(failures: &mut Vec<String>, sql: &str, actual: Result<Datum, crate::EvalError>, expected: Datum) {
    if actual != Ok(expected.clone()) {
        failures.push(format!("{sql}: {actual:?}; Go returns {expected:?}"));
    }
}

#[test]
fn the_control_classes_evaluate_a_vector_as_go_does() {
    let mut f = Vec::new();
    let vec = C::VectorFloat32;
    check(&mut f, "if(1,[1,2],[3,4])", eval("if", vec![Some(i(1)), Some(v(&[1.0, 2.0])), Some(v(&[3.0, 4.0]))], vec), v(&[1.0, 2.0]));
    check(&mut f, "if(0,[1,2],[3,4])", eval("if", vec![Some(i(0)), Some(v(&[1.0, 2.0])), Some(v(&[3.0, 4.0]))], vec), v(&[3.0, 4.0]));
    check(&mut f, "if(null,[1,2],null)", eval("if", vec![Some(Datum::Null), Some(v(&[1.0, 2.0])), NULL_VECTOR], vec), Datum::Null);
    check(&mut f, "ifnull(null,[3,4])", eval("ifnull", vec![NULL_VECTOR, Some(v(&[3.0, 4.0]))], vec), v(&[3.0, 4.0]));
    check(&mut f, "ifnull([1,2],[3,4])", eval("ifnull", vec![Some(v(&[1.0, 2.0])), Some(v(&[3.0, 4.0]))], vec), v(&[1.0, 2.0]));
    check(&mut f, "case when 0 then [1] when 1 then [2] else [3] end", eval("case", vec![Some(i(0)), Some(v(&[1.0])), Some(i(1)), Some(v(&[2.0])), Some(v(&[3.0]))], vec), v(&[2.0]));
    check(&mut f, "case when 0 then [1] else [3] end", eval("case", vec![Some(i(0)), Some(v(&[1.0])), Some(v(&[3.0]))], vec), v(&[3.0]));
    check(&mut f, "case when 0 then [1] end", eval("case", vec![Some(i(0)), Some(v(&[1.0]))], vec), Datum::Null);
    check(&mut f, "coalesce(null,[5,6],[7])", eval("coalesce", vec![NULL_VECTOR, Some(v(&[5.0, 6.0])), Some(v(&[7.0]))], vec), v(&[5.0, 6.0]));
    check(&mut f, "coalesce(null,null)", eval("coalesce", vec![NULL_VECTOR, NULL_VECTOR], vec), Datum::Null);
    check(&mut f, "any_value([1,2])", eval("any_value", vec![Some(v(&[1.0, 2.0]))], vec), v(&[1.0, 2.0]));
    assert!(f.is_empty(), "{}", f.join("\n"));
}

#[test]
fn the_comparison_classes_evaluate_a_vector_as_go_does() {
    let mut f = Vec::new();
    let vec = C::VectorFloat32;
    let int = C::LongLong;
    check(&mut f, "[1,2] in ([3,4],[1,2])", eval("in", vec![Some(v(&[1.0, 2.0])), Some(v(&[3.0, 4.0])), Some(v(&[1.0, 2.0]))], int), i(1));
    check(&mut f, "[1,2] in ([3,4],[1,3])", eval("in", vec![Some(v(&[1.0, 2.0])), Some(v(&[3.0, 4.0])), Some(v(&[1.0, 3.0]))], int), i(0));
    check(&mut f, "[1,2] in ([3,4],null)", eval("in", vec![Some(v(&[1.0, 2.0])), Some(v(&[3.0, 4.0])), NULL_VECTOR], int), Datum::Null);
    check(&mut f, "[1,2] is true", eval("istrue", vec![Some(v(&[1.0, 2.0]))], int), i(1));
    check(&mut f, "[0,0] is true", eval("istrue", vec![Some(v(&[0.0, 0.0]))], int), i(1));
    check(&mut f, "[] is true", eval("istrue", vec![Some(v(&[]))], int), i(0));
    check(&mut f, "[0,0] is false", eval("isfalse", vec![Some(v(&[0.0, 0.0]))], int), i(0));
    check(&mut f, "[1,2] is false", eval("isfalse", vec![Some(v(&[1.0, 2.0]))], int), i(0));
    check(&mut f, "null is true", eval("istrue", vec![NULL_VECTOR], int), i(0));
    check(&mut f, "null is false", eval("isfalse", vec![NULL_VECTOR], int), i(0));
    check(&mut f, "greatest([1,2],[1,3])", eval("greatest", vec![Some(v(&[1.0, 2.0])), Some(v(&[1.0, 3.0]))], vec), v(&[1.0, 3.0]));
    check(&mut f, "least([1,2],[1,3])", eval("least", vec![Some(v(&[1.0, 2.0])), Some(v(&[1.0, 3.0]))], vec), v(&[1.0, 2.0]));
    check(&mut f, "greatest([1,2],null)", eval("greatest", vec![Some(v(&[1.0, 2.0])), NULL_VECTOR], vec), Datum::Null);
    assert!(f.is_empty(), "{}", f.join("\n"));
}

#[test]
fn the_arithmetic_classes_evaluate_a_vector_as_go_does() {
    let mut f = Vec::new();
    let vec = C::VectorFloat32;
    check(&mut f, "[1,2]+[3,4]", eval("plus", vec![Some(v(&[1.0, 2.0])), Some(v(&[3.0, 4.0]))], vec), v(&[4.0, 6.0]));
    check(&mut f, "[1,2]-[3,5]", eval("minus", vec![Some(v(&[1.0, 2.0])), Some(v(&[3.0, 5.0]))], vec), v(&[-2.0, -3.0]));
    check(&mut f, "[1,2]*[3,5]", eval("mul", vec![Some(v(&[1.0, 2.0])), Some(v(&[3.0, 5.0]))], vec), v(&[3.0, 10.0]));
    check(&mut f, "[1,2]+null", eval("plus", vec![Some(v(&[1.0, 2.0])), NULL_VECTOR], vec), Datum::Null);
    let mismatch = eval("plus", vec![Some(v(&[1.0, 2.0])), Some(v(&[3.0]))], vec);
    match &mismatch {
        Err(error) if format!("{error:?}").contains("vectors have different dimensions: 2 and 1") => {}
        other => f.push(format!("[1,2]+[3]: {other:?}; Go fails with `vectors have different dimensions: 2 and 1`")),
    }
    assert!(f.is_empty(), "{}", f.join("\n"));
}

#[test]
fn the_classes_that_set_no_pb_code_evaluate_as_go_does() {
    let mut f = Vec::new();
    let int = C::LongLong;
    let string = C::VarString;
    check(&mut f, "name_const('a',5)", eval("name_const", vec![Some(s("a")), Some(i(5))], int), i(5));
    check(&mut f, "quote(null)", eval("quote", vec![Some(Datum::Null)], string), s("NULL"));
    for name in ["is_ipv4", "is_ipv6", "is_ipv4_compat", "is_ipv4_mapped"] {
        check(&mut f, name, eval(name, vec![Some(Datum::Null)], int), Datum::Null);
    }
    check(&mut f, "period_add(202401,3)", eval("period_add", vec![Some(i(202401)), Some(i(3))], int), i(202404));
    check(&mut f, "period_diff(202403,202401)", eval("period_diff", vec![Some(i(202403)), Some(i(202401))], int), i(2));
    check(&mut f, "bit_count(7)", eval("bit_count", vec![Some(i(7))], int), i(3));
    check(&mut f, "translate('abc','ab','xy')", eval("translate", vec![Some(s("abc")), Some(s("ab")), Some(s("xy"))], string), s("xyc"));
    check(&mut f, "tidb_parse_tso_logical(443852055297916932)", eval("tidb_parse_tso_logical", vec![Some(i(443852055297916932))], int), i(4));
    check(&mut f, "coercibility('a')", eval("coercibility", vec![Some(s("a"))], int), i(4));
    check(&mut f, "charset('a')", eval("charset", vec![Some(s("a"))], string), s("utf8mb4"));
    check(&mut f, "collation('a')", eval("collation", vec![Some(s("a"))], string), s("utf8mb4_bin"));
    check(&mut f, "tidb_decode_plan('x')", eval("tidb_decode_plan", vec![Some(s("x"))], string), s("x"));
    check(&mut f, "tidb_decode_binary_plan('x')", eval("tidb_decode_binary_plan", vec![Some(s("x"))], string), s(""));
    assert!(f.is_empty(), "{}", f.join("\n"));
}
