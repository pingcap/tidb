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

//! Value-level ports of `pkg/expression/builtin_time_test.go`'s MakeDate and
//! MakeTime tables (`builtin_time_test.go:2623`, `:2677`).

use super::*;
use tidb_datatype::{FieldType, FieldTypeCode as C};

use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;

fn const_arg(datum: Datum) -> Expression {
    let field_type = match &datum {
        Datum::Null => FieldType::new(C::Null),
        Datum::Int(_) => FieldType::new(C::LongLong),
        Datum::Real(_) => FieldType::new(C::Double),
        Datum::String(_) | Datum::Bytes(_) => FieldType::new(C::VarString),
        Datum::Decimal(_) => FieldType::new(C::NewDecimal),
        other => panic!("no test mapping for {other:?}"),
    };
    Expression::Constant(crate::constant::Constant::new(datum, field_type))
}

fn eval_named(name: &str, args: Vec<Datum>) -> Result<Datum, crate::EvalError> {
    let function = ScalarFunction::new(
        CiString::new(name),
        FieldType::new(C::VarString),
        args.into_iter().map(const_arg).collect(),
    );
    let cols = crate::context::ZonedNoColumns(tidb_datatype::SessionTimeZone::utc());
    let empty = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
    function.eval(&cols, empty.get_row(0))
}

fn s(v: &str) -> Datum {
    Datum::new_string(v)
}

fn i(v: i64) -> Datum {
    Datum::Int(v)
}

fn r(v: f64) -> Datum {
    Datum::Real(v)
}

/// Go `TestMakeDate`: two-digit years pivot at 70; out-of-range years or day
/// counts answer NULL; string arguments coerce.
#[test]
fn go_test_makedate() {
    let cases: &[(Vec<Datum>, Option<&str>)] = &[
        (vec![i(71), i(1)], Some("1971-01-01")),
        (vec![r(71.1), r(1.89)], Some("1971-01-02")),
        (vec![i(99), i(1)], Some("1999-01-01")),
        (vec![i(100), i(1)], Some("0100-01-01")),
        (vec![i(69), i(1)], Some("2069-01-01")),
        (vec![i(70), i(1)], Some("1970-01-01")),
        (vec![i(1000), i(1)], Some("1000-01-01")),
        (vec![i(-1), i(3660)], None),
        (vec![i(10000), i(3660)], None),
        (vec![i(2060), i(2_900_025)], Some("9999-12-31")),
        (vec![i(2060), i(2_900_026)], None),
        (vec![s("71"), i(1)], Some("1971-01-01")),
        (vec![i(71), s("1")], Some("1971-01-01")),
        (vec![s("71"), s("1")], Some("1971-01-01")),
        (vec![Datum::Null, i(2_900_025)], None),
        (vec![i(2060), Datum::Null], None),
        (vec![Datum::Null, Datum::Null], None),
    ];
    for (args, expected) in cases {
        let value =
            eval_named("makedate", args.clone()).unwrap_or_else(|e| panic!("{args:?}: {e:?}"));
        match expected {
            Some(text) => {
                // The label prefixes the kind ("STR:"); strip it for the
                // source-shaped comparison.
                let label = value.label();
                let rendered = label.strip_prefix("STR:").unwrap_or(&label);
                assert_eq!(rendered, *text, "{args:?}")
            }
            None => assert!(value.is_null(), "{args:?}: {value:?}"),
        }
    }
}

/// Go `TestMakeTime` (`builtin_time_test.go:2677`): hours may exceed 23 and
/// go negative; minutes/seconds past their range roll over ONLY when whole,
/// fractional minutes/seconds CARRY into the next unit; garbage combinations
/// answer NULL.
#[test]
fn go_test_maketime() {
    let cases: &[(Vec<Datum>, Option<&str>)] = &[
        (vec![i(12), i(15), i(30)], Some("12:15:30")),
        (vec![i(25), i(15), i(30)], Some("25:15:30")),
        (vec![i(-25), i(15), i(30)], Some("-25:15:30")),
        (vec![i(12), i(-15), i(30)], None),
        (vec![i(12), i(15), i(-30)], None),
        (vec![i(12), i(15), s("30.10")], Some("12:15:30.100000")),
        (vec![i(12), i(15), s("30.20")], Some("12:15:30.200000")),
        (vec![i(12), i(15), r(30.300_000_1)], Some("12:15:30.300000")),
        (vec![i(12), i(15), r(30.000_000_5)], Some("12:15:30.000001")),
        (vec![s("12"), s("15"), r(30.1)], Some("12:15:30.100000")),
        (vec![i(0), r(58.4), i(0)], Some("00:58:00")),
        (vec![i(0), s("58.4"), i(0)], Some("00:58:00")),
        (vec![i(0), r(58.5), i(1)], Some("00:58:01")),
        (vec![i(0), s("58.5"), i(1)], Some("00:58:01")),
        (vec![i(0), r(59.5), i(1)], None),
        (vec![i(0), s("59.5"), i(1)], Some("00:59:01")),
        (vec![i(0), i(1), r(59.1)], Some("00:01:59.100000")),
        (vec![i(0), i(1), s("59.1")], Some("00:01:59.100000")),
        (vec![i(0), i(1), r(59.5)], Some("00:01:59.500000")),
        (vec![i(0), i(1), s("59.5")], Some("00:01:59.500000")),
        (vec![r(23.5), i(1), i(10)], Some("24:01:10")),
        (vec![s("23.5"), i(1), i(10)], Some("23:01:10")),
        (vec![i(0), i(0), i(0)], Some("00:00:00")),
        (vec![i(837), i(59), r(59.1)], Some("837:59:59.100000")),
        (vec![i(838), i(0), r(59.1)], Some("838:00:59.100000")),
        (vec![i(838), i(50), r(59.999)], Some("838:50:59.999000")),
    ];
    for (args, expected) in cases {
        let value =
            eval_named("maketime", args.clone()).unwrap_or_else(|e| panic!("{args:?}: {e:?}"));
        match expected {
            Some(text) => {
                // The label prefixes the kind ("STR:"); strip it for the
                // source-shaped comparison.
                let label = value.label();
                let rendered = label.strip_prefix("STR:").unwrap_or(&label);
                assert_eq!(rendered, *text, "{args:?}")
            }
            None => assert!(value.is_null(), "{args:?}: {value:?}"),
        }
    }
}
