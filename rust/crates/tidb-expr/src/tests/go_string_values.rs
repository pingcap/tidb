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

//! Value-level ports of `pkg/expression/builtin_string_test.go`.
//!
//! Same door as Go's tests: constants typed by datum kind
//! (`kindToFieldType`, `evaluator_test.go:33`), one evaluation over an empty
//! row, results presented as the function's declared type.

use super::*;
use tidb_datatype::{Collation, FieldType, FieldTypeCode as C, FieldTypeFlags, StringDatum};

use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;

fn const_arg(datum: Datum) -> Expression {
    let field_type = match &datum {
        Datum::Null => FieldType::new(C::Null),
        Datum::Int(_) => FieldType::new(C::LongLong),
        Datum::UInt(_) => FieldType::new(C::LongLong).with_added_flags(FieldTypeFlags::UNSIGNED),
        Datum::Float32(_) | Datum::Real(_) => FieldType::new(C::Double),
        Datum::String(_) | Datum::Bytes(_) => FieldType::new(C::VarString),
        Datum::Decimal(_) => FieldType::new(C::NewDecimal),
        other => panic!("no test mapping for {other:?}"),
    };
    Expression::Constant(crate::constant::Constant::new(datum, field_type))
}

fn eval_string_sig(
    name: &str,
    args: Vec<Datum>,
) -> Result<Datum, crate::EvalError> {
    let mut ret = FieldType::new(C::VarString);
    // Go's string signatures declare MaxFlen-scale results; the packet
    // boundary checks read the declared length (e.g. SPACE/REPEAT's 1000).
    ret.set_flen(1_000_000);
    let function = ScalarFunction::new(
        CiString::new(name),
        ret,
        args.into_iter().map(const_arg).collect(),
    );
    let cols = crate::context::ZonedNoColumns(tidb_datatype::SessionTimeZone::utc());
    let empty = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
    function.eval(&cols, empty.get_row(0))
}

fn eval_int_sig(name: &str, args: Vec<Datum>) -> Result<Datum, crate::EvalError> {
    let function = ScalarFunction::new(
        CiString::new(name),
        FieldType::new(C::LongLong),
        args.into_iter().map(const_arg).collect(),
    );
    let cols = crate::context::ZonedNoColumns(tidb_datatype::SessionTimeZone::utc());
    let empty = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
    function.eval(&cols, empty.get_row(0))
}

fn s(v: &str) -> Datum {
    Datum::String(StringDatum::new(v, Collation::Utf8Mb4Bin))
}

fn i(v: i64) -> Datum {
    Datum::Int(v)
}

fn r(v: f64) -> Datum {
    Datum::Real(v)
}

fn str_of(datum: &Datum) -> String {
    match datum {
        Datum::String(bytes) => String::from_utf8_lossy(bytes.bytes()).into_owned(),
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => panic!("expected a string result, got {other:?}"),
    }
}

/// Go `TestLengthAndOctetLength` (`builtin_string_test.go:39`): both names
/// count BYTES; numbers render first; NULL is NULL.
#[test]
fn go_test_length_and_octet_length() {
    for name in ["length", "octet_length"] {
        let cases: &[(Datum, Option<i64>)] = &[
            (s("abc"), Some(3)),
            (s("你好"), Some(6)),
            (i(1), Some(1)),
            (Datum::Real(3.14), Some(4)),
            (Datum::Null, None),
        ];
        for (arg, expected) in cases {
            let value =
                eval_int_sig(name, vec![arg.clone()]).unwrap_or_else(|e| panic!("{name}: {e:?}"));
            match expected {
                Some(length) => assert_eq!(value, i(*length), "{name}({arg:?})"),
                None => assert!(value.is_null(), "{name}({arg:?})"),
            }
        }
    }
}

/// Go `TestLeft` (`builtin_string_test.go`): the count rounds to nearest
/// (`1.2` -> 1, `1.9` -> 2), negatives answer "", numeric strings coerce,
/// and the result reads the ARGUMENT's rendering ("1234" -> "123").
#[test]
fn go_test_left_and_right() {
    let left_cases: &[(Datum, Datum, &str)] = &[
        (s("abcde"), i(3), "abc"),
        (s("abcde"), i(0), ""),
        (s("abcde"), r(1.2), "a"),
        (s("abcde"), r(1.9), "ab"),
        (s("abcde"), i(-1), ""),
        (s("abcde"), i(100), "abcde"),
        (i(1234), i(3), "123"),
        (r_val(12.34), i(3), "12."),
    ];
    for (text, count, expected) in left_cases {
        let value = eval_string_sig("left", vec![text.clone(), count.clone()])
            .unwrap_or_else(|e| panic!("left: {e:?}"));
        assert_eq!(str_of(&value), *expected, "left({text:?}, {count:?})");
    }
    let right_cases: &[(Datum, Datum, &str)] = &[
        (s("abcde"), i(3), "cde"),
        (s("abcde"), i(0), ""),
        (s("abcde"), i(-1), ""),
        (s("abcde"), i(100), "abcde"),
    ];
    for (text, count, expected) in right_cases {
        let value = eval_string_sig("right", vec![text.clone(), count.clone()])
            .unwrap_or_else(|e| panic!("right: {e:?}"));
        assert_eq!(str_of(&value), *expected, "right({text:?}, {count:?})");
    }
}

/// Go `TestRepeatSig` (`builtin_string_test.go`): repetition within the
/// declared flen; anything past it answers NULL with the packet warning.
#[test]
fn go_test_repeat() {
    let value = eval_string_sig("repeat", vec![s("a"), i(6)]).unwrap();
    assert_eq!(str_of(&value), "aaaaaa");
    let value = eval_string_sig("repeat", vec![s("毅"), i(6)]).unwrap();
    assert_eq!(str_of(&value), "毅毅毅毅毅毅");
    // Go's table also pins {a, 10001} and {毅, 334}: both answer NULL with
    // warning 1301 because the SIGNATURE was built with a 1000-byte
    // maxAllowedPacket (`builtinRepeatSig{base, 1000}`). That limit is a
    // session value here, so the boundary itself is exercised by
    // `string_packet`'s own tests under a controlled context; through the
    // default session the same rows simply repeat.
    {
        struct SmallPacket;
        impl crate::context::Columns for SmallPacket {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
                tidb_datatype::SessionTimeZone::utc()
            }
            fn max_allowed_packet(&self) -> u64 {
                1000
            }
        }
        let function = ScalarFunction::new(
            CiString::new("repeat"),
            FieldType::new(C::VarString),
            vec![const_arg(s("a")), const_arg(i(10_001))],
        );
        let empty = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
        let value = function
            .eval(&SmallPacket, empty.get_row(0))
            .unwrap_or_else(|e| panic!("repeat over packet: {e:?}"));
        assert!(value.is_null(), "past the packet bound repeat answers NULL");
    }
}

/// Go `TestUpper` (`builtin_string_test.go`): ASCII case-folds; CJK passes
/// through; NULL stays NULL.
#[test]
fn go_test_upper() {
    let cases: &[(Datum, Option<&str>)] = &[
        (Datum::Null, None),
        (s("ab"), Some("AB")),
        (i(1), Some("1")),
        (s("one week’s time TEST"), Some("ONE WEEK’S TIME TEST")),
        (s("abc测试def"), Some("ABC测试DEF")),
        (s("abcテストdef"), Some("ABCテストDEF")),
    ];
    for (arg, expected) in cases {
        let value = eval_string_sig("upper", vec![arg.clone()])
            .unwrap_or_else(|e| panic!("upper({arg:?}): {e:?}"));
        match expected {
            Some(text) => assert_eq!(str_of(&value).as_str(), *text, "upper({arg:?})"),
            None => assert!(value.is_null()),
        }
    }
}

/// Go `TestLTrim`/`TestRTrim`: ONLY spaces trim; tabs and newlines do not.
#[test]
fn go_test_ltrim_rtrim() {
    let ltrim_cases: &[(&str, &str)] = &[
        ("   bar   ", "bar   "),
        ("\t   bar   ", "\t   bar   "),
        ("   \tbar   ", "\tbar   "),
        ("\r   bar   ", "\r   bar   "),
        ("   \nbar   ", "\nbar   "),
        ("bar", "bar"),
        ("", ""),
    ];
    for (input, expected) in ltrim_cases {
        let value = eval_string_sig("ltrim", vec![s(input)])
            .unwrap_or_else(|e| panic!("ltrim: {e:?}"));
        assert_eq!(str_of(&value), *expected, "ltrim({input:?})");
    }
    for (input, expected) in [("   bar   ", "   bar"), ("bar   ", "bar")] {
        let value = eval_string_sig("rtrim", vec![s(input)])
            .unwrap_or_else(|e| panic!("rtrim: {e:?}"));
        assert_eq!(str_of(&value), expected, "rtrim({input:?})");
    }
    assert!(
        eval_string_sig("ltrim", vec![Datum::Null])
            .unwrap()
            .is_null()
    );
}

/// Go `TestInsert` (`builtin_string_test.go:2349`): the full source table,
/// including the multibyte rows -- INSERT is CHARACTER-positioned, and a
/// negative position means "read from the start".
#[test]
fn go_test_insert() {
    let cases: &[(Vec<Datum>, Option<&str>)] = &[
        (vec![s("Quadratic"), i(3), i(4), s("What")], Some("QuWhattic")),
        (vec![s("Quadratic"), i(-1), i(4), s("What")], Some("Quadratic")),
        (vec![s("Quadratic"), i(3), i(100), s("What")], Some("QuWhat")),
        (vec![Datum::Null, i(3), i(100), s("What")], None),
        (vec![s("Quadratic"), Datum::Null, i(4), s("What")], None),
        (vec![s("Quadratic"), i(3), Datum::Null, s("What")], None),
        (vec![s("Quadratic"), i(3), i(4), Datum::Null], None),
        (vec![s("Quadratic"), i(3), i(-1), s("What")], Some("QuWhat")),
        (vec![s("Quadratic"), i(3), i(1), s("What")], Some("QuWhatdratic")),
        (vec![s("Quadratic"), i(-1), Datum::Null, s("What")], None),
        (vec![s("Quadratic"), i(-1), i(4), Datum::Null], None),
        (vec![s("我叫小雨呀"), i(3), i(2), s("王雨叶")], Some("我叫王雨叶呀")),
        (vec![s("我叫小雨呀"), i(-1), i(2), s("王雨叶")], Some("我叫小雨呀")),
        (vec![s("我叫小雨呀"), i(3), i(100), s("王雨叶")], Some("我叫王雨叶")),
        (vec![Datum::Null, i(3), i(100), s("王雨叶")], None),
        (vec![s("我叫小雨呀"), Datum::Null, i(4), s("王雨叶")], None),
        (vec![s("我叫小雨呀"), i(3), Datum::Null, s("王雨叶")], None),
        (vec![s("我叫小雨呀"), i(3), i(4), Datum::Null], None),
        (vec![s("我叫小雨呀"), i(3), i(-1), s("王雨叶")], Some("我叫王雨叶")),
        (vec![s("我叫小雨呀"), i(3), i(1), s("王雨叶")], Some("我叫王雨叶雨呀")),
        (vec![s("我叫小雨呀"), i(-1), Datum::Null, s("王雨叶")], None),
        (vec![s("我叫小雨呀"), i(-1), i(2), Datum::Null], None),
    ];
    for (args, expected) in cases {
        let value = eval_string_sig("insert_func", args.clone())
            .or_else(|_| eval_string_sig("insert", args.clone()))
            .unwrap_or_else(|e| panic!("insert {args:?}: {e:?}"));
        match expected {
            Some(text) => assert_eq!(str_of(&value), *text, "{args:?}"),
            None => assert!(value.is_null(), "{args:?}: {value:?}"),
        }
    }
}

/// A REAL argument helper kept separate so the kind table above stays
/// readable; Go's tests pass float64 literals directly.
fn r_val(v: f64) -> Datum {
    Datum::Real(v)
}
