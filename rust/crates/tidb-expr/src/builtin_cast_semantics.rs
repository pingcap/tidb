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

//! Focused semantic boundaries for the native `CAST` implementation.

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use crate::{Datum, EvalError};
    use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags, VectorFloat32};

    #[derive(Default)]
    struct WarningContext(RefCell<Vec<(u16, String)>>);

    impl crate::Columns for WarningContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }

    /// Go `BuildCastFunction4Union` over a constant, evaluated under strict
    /// flags: every `inUnion` branch of the int, real and decimal cast
    /// signatures (`builtin_cast.go`), as a Go probe printed them.
    #[test]
    fn in_union_casts_answer_like_go() {
        struct Strict;
        impl crate::Columns for Strict {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn truncate_level(&self) -> crate::ErrorLevel {
                crate::ErrorLevel::Error
            }
        }
        enum Want {
            I(i64),
            U(u64),
            R(f64),
            D(&'static str),
            E,
        }
        use Want::{D, E, I, R, U};
        let rows = [
            ("int", "-1", "bigint_u", U(0)),
            ("int", "-1", "bigint", I(-1)),
            ("int", "-1", "double_u", R(0.0)),
            ("int", "-1", "double", R(-1.0)),
            ("int", "-1", "dec_u", D("0.00")),
            ("int", "-1", "dec", D("-1.00")),
            ("int", "7", "bigint_u", U(7)),
            ("int", "7", "bigint", I(7)),
            ("int", "7", "double_u", R(7.0)),
            ("int", "7", "double", R(7.0)),
            ("int", "7", "dec_u", D("7.00")),
            ("int", "7", "dec", D("7.00")),
            ("uint", "max", "bigint_u", U(0)),
            ("uint", "max", "bigint", I(-1)),
            ("uint", "max", "double_u", R(1.8446744073709552e+19)),
            ("uint", "max", "double", R(1.8446744073709552e+19)),
            ("uint", "max", "dec_u", E),
            ("uint", "max", "dec", E),
            ("real", "-2.5", "bigint_u", U(0)),
            ("real", "-2.5", "bigint", I(-2)),
            ("real", "-2.5", "double_u", R(0.0)),
            ("real", "-2.5", "double", R(-2.5)),
            ("real", "-2.5", "dec_u", D("0.00")),
            ("real", "-2.5", "dec", D("0.00")),
            ("real", "2.5", "bigint_u", U(2)),
            ("real", "2.5", "bigint", I(2)),
            ("real", "2.5", "double_u", R(2.5)),
            ("real", "2.5", "double", R(2.5)),
            ("real", "2.5", "dec_u", D("2.50")),
            ("real", "2.5", "dec", D("2.50")),
            ("dec", "-2.5", "bigint_u", U(0)),
            ("dec", "-2.5", "bigint", I(-3)),
            ("dec", "-2.5", "double_u", R(0.0)),
            ("dec", "-2.5", "double", R(-2.5)),
            ("dec", "-2.5", "dec_u", D("0.00")),
            ("dec", "-2.5", "dec", D("-2.50")),
            ("dec", "-0.4", "bigint_u", U(0)),
            ("dec", "-0.4", "bigint", I(0)),
            ("dec", "-0.4", "double_u", R(0.0)),
            ("dec", "-0.4", "double", R(-0.4)),
            ("dec", "-0.4", "dec_u", D("0.00")),
            ("dec", "-0.4", "dec", D("-0.40")),
            ("dec", "2.5", "bigint_u", U(3)),
            ("dec", "2.5", "bigint", I(3)),
            ("dec", "2.5", "double_u", R(2.5)),
            ("dec", "2.5", "double", R(2.5)),
            ("dec", "2.5", "dec_u", D("2.50")),
            ("dec", "2.5", "dec", D("2.50")),
            ("str", "-2.5", "bigint_u", U(0)),
            ("str", "-2.5", "bigint", E),
            ("str", "-2.5", "double_u", R(0.0)),
            ("str", "-2.5", "double", R(-2.5)),
            ("str", "-2.5", "dec_u", D("0.00")),
            ("str", "-2.5", "dec", D("-2.50")),
            ("str", " -7 ", "bigint_u", U(0)),
            ("str", " -7 ", "bigint", I(-7)),
            ("str", " -7 ", "double_u", R(0.0)),
            ("str", " -7 ", "double", R(-7.0)),
            ("str", " -7 ", "dec_u", D("0.00")),
            ("str", " -7 ", "dec", D("-7.00")),
            ("str", "-abc", "bigint_u", U(0)),
            ("str", "-abc", "bigint", E),
            ("str", "-abc", "double_u", E),
            ("str", "-abc", "double", E),
            ("str", "-abc", "dec_u", D("0.00")),
            ("str", "-abc", "dec", E),
            ("str", "3.7", "bigint_u", E),
            ("str", "3.7", "bigint", E),
            ("str", "3.7", "double_u", R(3.7)),
            ("str", "3.7", "double", R(3.7)),
            ("str", "3.7", "dec_u", D("3.70")),
            ("str", "3.7", "dec", D("3.70")),
            ("str", "-0", "bigint_u", U(0)),
            ("str", "-0", "bigint", I(0)),
            ("str", "-0", "double_u", R(-0.0)),
            ("str", "-0", "double", R(-0.0)),
            ("str", "-0", "dec_u", D("0.00")),
            ("str", "-0", "dec", D("0.00")),
            ("str", "-", "bigint_u", E),
            ("str", "-", "bigint", E),
            ("str", "-", "double_u", E),
            ("str", "-", "double", E),
            ("str", "-", "dec_u", E),
            ("str", "-", "dec", E),
            ("str", "abc", "bigint_u", E),
            ("str", "abc", "bigint", E),
            ("str", "abc", "double_u", E),
            ("str", "abc", "double", E),
            ("str", "abc", "dec_u", E),
            ("str", "abc", "dec", E),
        ];
        for (kind, value, target, want) in rows {
            let (datum, mut source_type) = match kind {
                "int" => (
                    Datum::Int(value.parse().expect("int")),
                    FieldType::new(FieldTypeCode::LongLong),
                ),
                "uint" => (
                    Datum::UInt(u64::MAX),
                    FieldType::new(FieldTypeCode::LongLong)
                        .with_added_flags(FieldTypeFlags::UNSIGNED),
                ),
                "real" => (
                    Datum::Real(value.parse().expect("real")),
                    FieldType::new(FieldTypeCode::Double),
                ),
                "dec" => {
                    let mut tp = FieldType::new(FieldTypeCode::NewDecimal);
                    tp.set_flen(10);
                    tp.set_decimal(1);
                    (Datum::Decimal(tidb_datatype::Decimal::from_literal(value)), tp)
                }
                _ => {
                    let mut tp = FieldType::new(FieldTypeCode::VarString);
                    tp.set_charset_name("utf8mb4");
                    tp.set_collation_name("utf8mb4_bin");
                    (Datum::new_string_datum(value), tp)
                }
            };
            source_type.add_flags(FieldTypeFlags::NOT_NULL);
            let source = crate::expression::Expression::Constant(
                crate::constant::Constant::new(datum, source_type),
            );
            let (code, unsigned) = match target {
                "bigint_u" => (FieldTypeCode::LongLong, true),
                "bigint" => (FieldTypeCode::LongLong, false),
                "double_u" => (FieldTypeCode::Double, true),
                "double" => (FieldTypeCode::Double, false),
                "dec_u" => (FieldTypeCode::NewDecimal, true),
                _ => (FieldTypeCode::NewDecimal, false),
            };
            let mut tp = FieldType::new(code);
            if code == FieldTypeCode::NewDecimal {
                tp.set_flen(10);
                tp.set_decimal(2);
            }
            if unsigned {
                tp.add_flags(FieldTypeFlags::UNSIGNED);
            }
            let cast = crate::wrap_cast::build_cast_function_4_union(
                &crate::builtin::UnfoldedBuildContext,
                source,
                tp,
            )
            .expect("UNION cast builds");
            let got = cast.eval(&Strict, tidb_chunk::row::Row::empty());
            let label = format!("{kind}:{value} -> {target}: {got:?}");
            match (want, got) {
                (I(want), Ok(Datum::Int(got))) => assert_eq!(got, want, "{label}"),
                (U(want), Ok(Datum::UInt(got))) => assert_eq!(got, want, "{label}"),
                (R(want), Ok(Datum::Real(got))) => {
                    assert_eq!(got.to_bits(), want.to_bits(), "{label}");
                }
                (D(want), Ok(Datum::Decimal(got))) => assert_eq!(got.to_string(), want, "{label}"),
                (E, Err(_)) => {}
                _ => panic!("{label}"),
            }
        }
    }

    #[test]
    fn union_unsigned_integer_cast_clamps_negative_values() {
        let mut source_type = FieldType::new(FieldTypeCode::LongLong);
        source_type.add_flags(FieldTypeFlags::NOT_NULL);
        let source = crate::expression::Expression::Constant(crate::constant::Constant::new(
            Datum::Int(-1),
            source_type,
        ));
        let target =
            FieldType::new(FieldTypeCode::LongLong).with_added_flags(FieldTypeFlags::UNSIGNED);

        let ordinary = crate::wrap_cast::build_cast_to(&crate::builtin::UnfoldedBuildContext, source.clone(), target.clone())
            .expect("ordinary cast builds");
        let crate::expression::Expression::ScalarFunction(ordinary) = ordinary else {
            panic!("ordinary cast should be a scalar function");
        };
        assert_eq!(ordinary.func_name.original(), "cast");
        assert_eq!(
            ordinary
                .eval(&WarningContext::default(), tidb_chunk::row::Row::empty())
                .expect("ordinary cast evaluates"),
            Datum::UInt(u64::MAX)
        );

        let in_union = crate::wrap_cast::build_cast_function_4_union(&crate::builtin::UnfoldedBuildContext, source, target)
            .expect("UNION cast builds");
        let crate::expression::Expression::ScalarFunction(in_union) = in_union else {
            panic!("UNION cast should be a scalar function");
        };
        assert_eq!(in_union.func_name.original(), "cast");
        assert_eq!(
            in_union
                .eval(&WarningContext::default(), tidb_chunk::row::Row::empty())
                .expect("UNION cast evaluates"),
            Datum::UInt(0)
        );

        // The same carrier also selects Go's string-as-int `inUnion` branch:
        // a negative string is discarded before the ordinary 8031 advisory
        // can be appended.
        let string_source =
            crate::expression::Expression::Constant(crate::constant::Constant::new(
                Datum::new_string_datum("-1"),
                FieldType::new(FieldTypeCode::VarString),
            ));
        let string_cast = crate::wrap_cast::build_cast_function_4_union(
            &crate::builtin::UnfoldedBuildContext, string_source,
            FieldType::new(FieldTypeCode::LongLong).with_added_flags(FieldTypeFlags::UNSIGNED),
        )
        .expect("string UNION cast builds");
        let warnings = WarningContext::default();
        assert_eq!(
            string_cast
                .eval(&warnings, tidb_chunk::row::Row::empty())
                .expect("string UNION cast evaluates"),
            Datum::UInt(0)
        );
        assert!(warnings.0.borrow().is_empty());
    }

    #[test]
    fn union_unsigned_string_decimal_cast_discards_negative_before_parse() {
        let mut source_type = FieldType::new(FieldTypeCode::VarString);
        source_type.add_flags(FieldTypeFlags::NOT_NULL);
        let source = |text: &str| {
            crate::expression::Expression::Constant(crate::constant::Constant::new(
                Datum::new_string_datum(text),
                source_type.clone(),
            ))
        };
        let mut target = FieldType::new(FieldTypeCode::NewDecimal);
        target.set_flen(10);
        target.set_decimal(2);
        target.add_flags(FieldTypeFlags::UNSIGNED);

        let negative =
            crate::wrap_cast::build_cast_function_4_union(&crate::builtin::UnfoldedBuildContext, source("-1.25"), target.clone())
                .expect("string UNION decimal cast builds");
        let crate::expression::Expression::ScalarFunction(negative_fn) = &negative else {
            panic!("UNION decimal cast should be a scalar function");
        };
        assert_eq!(
            negative_fn.func_name.original(),
            "cast"
        );
        let warnings = WarningContext::default();
        assert_eq!(
            negative
                .eval(&warnings, tidb_chunk::row::Row::empty())
                .expect("negative string UNION decimal cast evaluates"),
            Datum::Decimal(tidb_datatype::Decimal::parse_mysql("0").0)
        );
        assert!(warnings.0.borrow().is_empty());

        let positive =
            crate::wrap_cast::build_cast_function_4_union(&crate::builtin::UnfoldedBuildContext, source("1.256"), target)
                .expect("positive string UNION decimal cast builds");
        assert_eq!(
            positive
                .eval(&WarningContext::default(), tidb_chunk::row::Row::empty())
                .expect("positive string UNION decimal cast evaluates"),
            Datum::Decimal(tidb_datatype::Decimal::from_literal("1.26"))
        );
    }

    #[test]
    fn union_signed_integer_decimal_cast_preserves_negative_values() {
        let source_type = FieldType::new(FieldTypeCode::LongLong);
        let source = crate::expression::Expression::Constant(crate::constant::Constant::new(
            Datum::Int(-12),
            source_type,
        ));
        let mut target = FieldType::new(FieldTypeCode::NewDecimal);
        target.set_flen(10);
        target.set_decimal(2);

        let wrapped = crate::wrap_cast::build_cast_function_4_union(&crate::builtin::UnfoldedBuildContext, source, target)
            .expect("signed integer UNION decimal cast builds");
        let crate::expression::Expression::ScalarFunction(function) = &wrapped else {
            panic!("UNION decimal cast should be a scalar function");
        };
        assert_eq!(function.func_name.original(), "cast");
        assert_eq!(
            wrapped
                .eval(&WarningContext::default(), tidb_chunk::row::Row::empty())
                .expect("signed integer UNION decimal cast evaluates"),
            Datum::Decimal(tidb_datatype::Decimal::from_literal("-12.00"))
        );
    }

    #[test]
    fn cast_result_types_keep_json_native_and_temporal_fsp() {
        for (cast, code, decimal) in [
            (tidb_ast::CastType::Date, FieldTypeCode::Date, 0),
            (
                tidb_ast::CastType::DateTime { fsp: Some(3) },
                FieldTypeCode::Datetime,
                3,
            ),
            (
                tidb_ast::CastType::Time { fsp: Some(3) },
                FieldTypeCode::Duration,
                3,
            ),
            (tidb_ast::CastType::Json, FieldTypeCode::Json, -1),
            (
                tidb_ast::CastType::Vector {
                    dimensions: Some(3),
                },
                FieldTypeCode::VectorFloat32,
                0,
            ),
        ] {
            let field_type = crate::rewriter::builtin_cast_result_type(&cast).expect("target");
            assert_eq!(field_type.code(), code);
            if decimal >= 0 {
                assert_eq!(field_type.decimal(), decimal);
            }
        }

        let time =
            crate::rewriter::builtin_cast_result_type(&tidb_ast::CastType::Time { fsp: Some(3) })
                .expect("TIME target");
        assert_eq!(time.flen(), 14);
        assert!(time.has_flag(tidb_datatype::FieldTypeFlags::BINARY));

        let vector = crate::rewriter::builtin_cast_result_type(&tidb_ast::CastType::Vector {
            dimensions: Some(3),
        })
        .expect("VECTOR target");
        assert_eq!(vector.flen(), 3);
        assert_eq!(vector.charset_name(), "binary");
        assert_eq!(vector.collation_name(), "binary");
        assert!(!vector.has_flag(tidb_datatype::FieldTypeFlags::BINARY));
    }

    #[test]
    fn vector_source_only_reaches_the_go_string_signature() {
        let vector = Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, 2.0]));
        assert!(crate::cast::eval_cast(
            &tidb_ast::CastType::Signed,
            vector.clone(),
            None,
            &crate::NoColumns,
        )
        .is_err());
        assert_eq!(
            crate::cast::eval_cast(
                &tidb_ast::CastType::Char {
                    len: None,
                    charset: None,
                },
                vector,
                None,
                &crate::NoColumns,
            )
            .expect("string cast")
            .sql_string()
            .expect("string value"),
            "[1,2]"
        );
    }

    #[test]
    fn cast_type_declarations_are_validated_before_evaluation() {
        let rewrite = |sql: &str| {
            let stmt = tidb_parser::parse(sql).expect("the CAST syntax parses");
            let tidb_ast::Stmt::Query(query) = stmt else {
                panic!("expected a query")
            };
            let tidb_ast::QueryStmt::Select(select) = &*query else {
                panic!("expected a SELECT")
            };
            let tidb_ast::SelectField::Expr { expr, .. } = &select.fields.fields()[0] else {
                panic!("expected an expression")
            };
            crate::rewriter::rewrite_expr_resolved(expr, &crate::rewriter::NoResolver)
        };

        assert_eq!(
            rewrite("select cast(12.1 as decimal(3, 4))").unwrap_err(),
            EvalError::InvalidTypeDeclaration {
                code: 1427,
                message:
                    "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '12.1')."
                        .to_owned(),
            }
        );
        assert_eq!(
            rewrite("select cast(1 as datetime(7))").unwrap_err(),
            EvalError::InvalidTypeDeclaration {
                code: 1426,
                message: "Too-big precision 7 specified for 'CAST'. Maximum is 6.".to_owned(),
            }
        );
    }
}

/// Go `CHAR(n) CHARSET binary`: the ret charset is binary, so
/// `ProduceStrWithSpecifiedTp` truncates in BYTES (`chs == CharsetBin`
/// branch, `pkg/types/datum.go:1264-1270`) and `padZeroForBinaryType`
/// refuses to pad (its gate is the FIXED `TypeString` code). The default
/// utf8mb4 CHAR keeps character-oriented truncation.
#[test]
fn cast_char_binary_charset_truncates_bytes_not_chars() {
    use tidb_ast::CastType;
    

    fn eval_char(len: Option<u32>, charset: Option<&str>, value: &str) -> crate::Datum {
        crate::cast::eval_cast(
            &CastType::Char {
                len,
                charset: charset.map(str::to_owned),
            },
            crate::Datum::new_string_datum(value),
            None,
            &tidb_exprctx::context::NoColumns,
        )
        .unwrap()
    }

    // 2 bytes of 6: a full `你` byte-truncates to the first 3 bytes.
    let crate::Datum::Bytes(truncated) = eval_char(Some(3), Some("BINARY"), "你好") else {
        panic!("a binary-charset CHAR result is raw bytes")
    };
    assert_eq!(truncated, "你".as_bytes());

    // A value already shorter than the target: NO NUL padding (the
    // TypeString gate), unlike CAST AS BINARY(5).
    let crate::Datum::Bytes(unpadded) = eval_char(Some(5), Some("BINARY"), "hi") else {
        panic!("expected raw bytes")
    };
    assert_eq!(unpadded, b"hi");

    // The default (utf8mb4) CHAR keeps character-oriented truncation.
    let crate::Datum::String(text) = eval_char(Some(1), None, "你好") else {
        panic!("expected a collation string")
    };
    assert_eq!(text.as_utf8().unwrap(), "你");
}
