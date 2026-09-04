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

    #[test]
    fn union_unsigned_integer_cast_clamps_negative_values() {
        let (name, result_type) =
            crate::rewriter::builtin_cast_result_type(&tidb_ast::CastType::UnsignedInUnion)
                .expect("internal UNION cast target");
        assert_eq!(name, "cast_unsigned_in_union");
        assert!(result_type.is_unsigned());

        let mut source_type = FieldType::new(FieldTypeCode::LongLong);
        source_type.add_flags(FieldTypeFlags::NOT_NULL);
        let source = crate::expression::Expression::Constant(crate::constant::Constant::new(
            Datum::Int(-1),
            source_type,
        ));
        let target =
            FieldType::new(FieldTypeCode::LongLong).with_added_flags(FieldTypeFlags::UNSIGNED);

        let ordinary = crate::aggregation::wrap_cast::build_cast_to(source.clone(), target.clone())
            .expect("ordinary cast builds");
        let crate::expression::Expression::ScalarFunction(ordinary) = ordinary else {
            panic!("ordinary cast should be a scalar function");
        };
        assert_eq!(ordinary.func_name.original(), "cast_unsigned");
        assert_eq!(
            ordinary
                .eval(&WarningContext::default(), tidb_chunk::row::Row::empty())
                .expect("ordinary cast evaluates"),
            Datum::UInt(u64::MAX)
        );

        let in_union = crate::aggregation::wrap_cast::build_cast_to_in_union(source, target)
            .expect("UNION cast builds");
        let crate::expression::Expression::ScalarFunction(in_union) = in_union else {
            panic!("UNION cast should be a scalar function");
        };
        assert_eq!(in_union.func_name.original(), "cast_unsigned_in_union");
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
                Datum::new_string("-1"),
                FieldType::new(FieldTypeCode::VarString),
            ));
        let string_cast = crate::aggregation::wrap_cast::build_cast_to_in_union(
            string_source,
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
            let (_, field_type) = crate::rewriter::builtin_cast_result_type(&cast).expect("target");
            assert_eq!(field_type.code(), code);
            if decimal >= 0 {
                assert_eq!(field_type.decimal(), decimal);
            }
        }

        let (_, time) =
            crate::rewriter::builtin_cast_result_type(&tidb_ast::CastType::Time { fsp: Some(3) })
                .expect("TIME target");
        assert_eq!(time.flen(), 14);
        assert!(time.has_flag(tidb_datatype::FieldTypeFlags::BINARY));

        let (_, vector) = crate::rewriter::builtin_cast_result_type(&tidb_ast::CastType::Vector {
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
    use tidb_datatype::FieldType;

    fn eval_char(len: Option<u32>, charset: Option<&str>, value: &str) -> crate::Datum {
        crate::cast::eval_cast(
            &CastType::Char {
                len,
                charset: charset.map(str::to_owned),
            },
            crate::Datum::new_string(value),
            None,
            &crate::context::NoColumns,
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
