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

//! Everything Go's `functionClass.getFunction` decides, for the classes
//! [`crate::builtin::funcs`] holds, against the node [`new_function`] builds.
//!
//! `testdata/function_class_matrix.txt` is Go's own answer from
//! `NewFunction(mock.NewContext(), name, types.NewFieldType(mysql.TypeUnspecified), args...)`:
//!
//! - `T`, a number and a field type as
//!   `type,flag,flen,decimal,charset,collation`: the types the other lines
//!   refer to by number.
//! - `A`, a shape letter and its argument's type. `i` bigint, `u` bigint
//!   unsigned, `r` double, `d` decimal(20,4), `s` varchar utf8mb4_bin, `b`
//!   varbinary, `t` datetime, `D` date, `T` timestamp, `h` time, `j` json and
//!   `I` bigint NOT NULL are columns; `N` NULL, `1` the integer 1, `x` the hex
//!   literal 0x12, `q` the string '1.5', `m` the decimal 1.5 and `e` the
//!   double 1.5 are constants typed by `types.DefaultTypeForValue`.
//! - a builtin's name, a shape (one letter per argument), the signature's
//!   `PbCode()`, the return type, `coercibility,repertoire` and, per argument
//!   after the signature's casts, `=` (the argument as given) or `c` (wrapped
//!   or replaced), its type and, for a constant the cast folded to,
//!   `:kind:value` with the value's text in hex.

use std::collections::BTreeMap;

use tidb_charset::Collation;
use tidb_datatype::{BinaryLiteral, Datum, Decimal, FieldType, FieldTypeCode, StringDatum};

use crate::builtin::funcs;
use crate::column::Column;
use crate::constant::Constant;
use crate::expression::Expression;
use crate::new_function::new_function;
use crate::pushdown_catalog::pb_code;
use crate::wrap_cast::type_of;

const MATRIX: &str = include_str!("../../testdata/function_class_matrix.txt");

struct MockContext;

impl tidb_exprctx::Columns for MockContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        Some((1_700_000_000, 0, 0))
    }
    /// Go `mock.NewContext`'s statement context keeps
    /// `types.DefaultStmtFlags`, under which a truncation is an error.
    fn truncate_level(&self) -> tidb_exprctx::ErrorLevel {
        tidb_exprctx::ErrorLevel::Error
    }
}

/// Go `typeText` of the probe.
fn type_text(tp: &FieldType) -> String {
    format!(
        "{},{},{},{},{},{}",
        tp.code().mysql_type(),
        tp.flags(),
        tp.flen(),
        tp.decimal(),
        tp.charset_name(),
        tp.collation_name()
    )
}

fn type_from_text(text: &str) -> FieldType {
    let cells: Vec<&str> = text.split(',').collect();
    let number = |cell: &str| cell.parse::<i64>().expect("number");
    let mut tp = FieldType::new(FieldTypeCode::from_mysql_type(number(cells[0]) as u8));
    tp.set_flag(number(cells[1]) as u32);
    tp.set_flen(number(cells[2]));
    tp.set_decimal(number(cells[3]));
    tp.set_charset_name(cells[4]);
    tp.set_collation_name(cells[5]);
    assert_eq!(type_text(&tp), text);
    tp
}

/// Go `types.NewDatum` of the constant a shape letter stands for; `None` for
/// a column's letter.
fn constant_value(label: char) -> Option<Datum> {
    Some(match label {
        'N' => Datum::Null,
        '1' => Datum::Int(1),
        'x' => Datum::BinaryLiteral(BinaryLiteral::from(vec![0x12])),
        'q' => Datum::String(StringDatum::new("1.5", Collation::Utf8Mb4Bin)),
        'm' => Datum::Decimal(Decimal::parse_mysql("1.5").0),
        'e' => Datum::Real(1.5),
        _ => return None,
    })
}

fn argument(index: usize, label: char, tp: FieldType) -> Expression {
    match constant_value(label) {
        Some(value) => Expression::Constant(Constant::new(value, tp)),
        None => {
            let mut column = Column::default();
            column.ret_type = Some(tp);
            column.id = index as i64 + 1;
            column.unique_id = index as i64 + 1;
            column.index = index as i64;
            Expression::Column(column)
        }
    }
}

/// The probe's `value`: a datum's kind and Go `Datum.ToString` in hex.
fn value_text(value: &Datum) -> String {
    let (kind, text) = match value {
        Datum::Null => return "null".to_owned(),
        Datum::Int(value) => ("int", value.to_string()),
        Datum::UInt(value) => ("uint", value.to_string()),
        Datum::Real(value) => ("real", value.to_string()),
        Datum::Decimal(value) => ("decimal", value.to_string()),
        Datum::String(value) => ("string", String::from_utf8_lossy(value.bytes()).into_owned()),
        value => panic!("constant {value:?}"),
    };
    let hex: String = text.bytes().map(|byte| format!("{byte:02x}")).collect();
    format!("{kind}:{hex}")
}

#[test]
fn get_function_matches_go_for_every_ported_class() {
    let mut types = BTreeMap::new();
    let mut letters = BTreeMap::new();
    let mut wrong = Vec::new();
    let mut checked = 0;
    for line in MATRIX.lines() {
        let cells: Vec<&str> = line.split('\t').collect();
        match cells[0] {
            "T" => {
                types.insert(cells[1], cells[2]);
                continue;
            }
            "A" => {
                letters.insert(cells[1], type_from_text(types[cells[2]]));
                continue;
            }
            _ => {}
        }
        let [name, shape, sig, ret_type, collation, slots] = cells[..] else {
            panic!("row {line}");
        };
        assert!(funcs(name).is_some(), "{name} has no class");
        let args: Vec<Expression> = shape
            .chars()
            .enumerate()
            .map(|(index, label)| argument(index, label, letters[label.to_string().as_str()].clone()))
            .collect();
        let given: Vec<String> = args.iter().map(|arg| format!("{arg:?}")).collect();
        let built = new_function(&MockContext, name, FieldType::new(FieldTypeCode::Unspecified), args);
        let got = match &built {
            Ok(Expression::ScalarFunction(f)) => {
                let slots: Vec<String> = f
                    .args
                    .iter()
                    .zip(&given)
                    .map(|(arg, given)| {
                        let kept = format!("{arg:?}") == *given;
                        let tp = type_text(&type_of(arg));
                        match arg {
                            Expression::Constant(constant) if !kept => {
                                format!("c{tp}:{}", value_text(&constant.value))
                            }
                            _ => format!("{}{tp}", if kept { '=' } else { 'c' }),
                        }
                    })
                    .collect();
                format!(
                    "{}\t{}\t{},{}\t{}",
                    pb_code(built.as_ref().expect("built")).as_str_name(),
                    type_text(f.ret_type.as_ref().expect("typed")),
                    f.collation.coercibility().0,
                    f.collation.repertoire().0,
                    slots.join(";")
                )
            }
            other => format!("{other:?}"),
        };
        let want_slots: Vec<String> = slots
            .split(';')
            .filter(|slot| !slot.is_empty())
            .map(|slot| {
                let (mark, rest) = slot.split_at(1);
                let (number, value) = rest.split_once(':').map_or((rest, None), |(n, v)| (n, Some(v)));
                let value = value.map(|value| format!(":{value}")).unwrap_or_default();
                format!("{mark}{}{value}", types[number])
            })
            .collect();
        let want = format!("{sig}\t{}\t{collation}\t{}", types[ret_type], want_slots.join(";"));
        checked += 1;
        if got != want {
            wrong.push(format!("{name}({shape})\n  go   {want}\n  rust {got}"));
        }
    }
    assert!(checked > 1000, "{checked} rows");
    assert!(wrong.is_empty(), "{} of {checked} differ:\n{}", wrong.len(), wrong.join("\n"));
}

/// Go `ScalarFunction.Function.PbCode()` reads the signature the class set,
/// whatever name the node carries.
#[test]
fn pb_code_is_the_signature_of_the_built_function() {
    let int = || {
        let mut column = Column::default();
        column.ret_type = Some(FieldType::new(FieldTypeCode::LongLong));
        Expression::Column(column)
    };
    let f = funcs("minus")
        .expect("minus has a class")
        .get_function(&crate::builtin::ColumnsBuildContext(&MockContext), vec![int(), int()])
        .expect("builds");
    let node = crate::new_function::scalar_function_of(
        "plus",
        FieldType::new(FieldTypeCode::Unspecified),
        f,
    );
    assert_eq!(
        pb_code(&Expression::ScalarFunction(node)).as_str_name(),
        "MinusInt"
    );
}

/// Go `groupingImplFunctionClass.getFunction` sets `GroupingSig`, a
/// signature `getSignatureByPB` has no arm for; `PbCode()` still reads it.
#[test]
fn a_signature_without_a_pb_decoder_keeps_its_pb_code() {
    let mut column = Column::default();
    column.ret_type = Some(FieldType::new(FieldTypeCode::LongLong));
    let f = funcs("grouping")
        .expect("grouping has a class")
        .get_function(
            &crate::builtin::ColumnsBuildContext(&MockContext),
            vec![Expression::Column(column)],
        )
        .expect("builds");
    let node = crate::new_function::scalar_function_of(
        "grouping",
        FieldType::new(FieldTypeCode::Unspecified),
        f,
    );
    assert_eq!(
        node.pb_signature().map(|signature| signature.as_str_name()),
        Some("GroupingSig")
    );
}

/// Go `SetFTSMysqlMatchAgainstModifier` asserts the node's function is
/// `builtinFtsMysqlMatchAgainstSig` and refuses any other.
#[test]
fn the_match_against_modifier_is_set_on_its_own_signature_only() {
    let string = || {
        let mut column = Column::default();
        column.ret_type = Some(FieldType::new(FieldTypeCode::Varchar));
        Expression::Column(column)
    };
    let escape = Expression::Constant(Constant::new(
        Datum::Int(92),
        FieldType::new(FieldTypeCode::LongLong),
    ));
    let built = new_function(
        &MockContext,
        "like",
        FieldType::new(FieldTypeCode::Unspecified),
        vec![string(), string(), escape],
    )
    .expect("like builds");
    let Expression::ScalarFunction(mut like) = built else {
        panic!("{built:?}");
    };
    let error = like
        .set_fts_mysql_match_against_modifier(tidb_ast::MatchModifier::default())
        .expect_err("like is not the MATCH ... AGAINST signature");
    assert!(
        format!("{error:?}").contains("unexpected builtin signature for match_against"),
        "{error:?}"
    );
}

/// Go `patternLikeOrIlikeToExpression` types the escape with
/// `types.DefaultTypeForValue(int(v.Escape), ...)`: a BIGINT as wide as the
/// escape's printed digits (`mathutil.StrLenOfInt64Fast`).
#[test]
fn a_like_escape_is_as_wide_as_its_digits() {
    struct NoneResolver;
    impl crate::rewriter::ColumnResolver for NoneResolver {
        fn resolve(&self, _: &[String]) -> Option<(usize, FieldType, i64)> {
            None
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            tidb_datatype::SessionTimeZone::utc()
        }
    }
    for (text, flen) in [("rand() like 'a'", 2), ("rand() like 'a' escape '|'", 3)] {
        let stmt = tidb_parser::parse(&format!("select {text}")).expect("parse");
        let tidb_ast::Stmt::Query(query) = stmt else {
            panic!("{text}: not a query")
        };
        let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
            panic!("{text}: not a select")
        };
        let tidb_ast::SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("{text}: no expression")
        };
        let built = crate::rewriter::rewrite_expr_resolved(expr, &NoneResolver).expect("rewrite");
        let Expression::ScalarFunction(like) = &built else {
            panic!("{text}: {built:?}");
        };
        let escape = type_of(&like.args[2]);
        assert_eq!(type_text(&escape), format!("8,129,{flen},0,binary,binary"), "{text}");
    }
}
