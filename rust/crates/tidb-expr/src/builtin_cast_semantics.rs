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
    use crate::{Datum, EvalError};
    use tidb_datatype::{FieldTypeCode, VectorFloat32};

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
