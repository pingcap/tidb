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

//! Compile anchors for the checked-in `builtin_cast.go` lockdown ledger.
//!
//! The ledger deliberately names a small set of native Rust seams rather than
//! pretending every Go signature object has a one-to-one Rust type. This body
//! makes those names real compiler-checked references; the lockdown checker
//! rejects any PORTED row whose symbol is absent from [`PORTED_SYMBOLS`].

use crate::{Columns, Datum, EvalError};

#[allow(dead_code)]
pub(crate) const CAST_EVAL: &str = "builtin_cast_lockdown::CAST_EVAL";
#[allow(dead_code)]
pub(crate) const CAST_REWRITE: &str = "builtin_cast_lockdown::CAST_REWRITE";
#[allow(dead_code)]
pub(crate) const CAST_JSON: &str = "builtin_cast_lockdown::CAST_JSON";
#[allow(dead_code)]
pub(crate) const PORTED_SYMBOLS: &[&str] = &[CAST_EVAL, CAST_REWRITE, CAST_JSON];

#[allow(dead_code)]
pub(crate) fn compile_anchors(
    cast_type: &tidb_ast::CastType,
    value: Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn Columns,
) -> Result<(), EvalError> {
    let _ = crate::cast::eval_cast(cast_type, value.clone(), source, ctx)?;
    let _ = crate::rewriter::builtin_cast_lockdown_result_type_anchor(cast_type);
    let _ = crate::builtin_ext::cast_as_json_typed(&value, source)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldTypeCode, VectorFloat32};

    #[test]
    fn cast_result_types_keep_json_native_and_temporal_fsp() {
        for (cast, code, decimal) in [
            (tidb_ast::CastType::Date, FieldTypeCode::VarString, -1),
            (
                tidb_ast::CastType::DateTime { fsp: Some(3) },
                FieldTypeCode::VarString,
                3,
            ),
            (tidb_ast::CastType::Json, FieldTypeCode::Json, -1),
        ] {
            let (_, field_type) =
                crate::rewriter::builtin_cast_lockdown_result_type_anchor(&cast).expect("target");
            assert_eq!(field_type.code(), code);
            if decimal >= 0 {
                assert_eq!(field_type.decimal(), decimal);
            }
        }
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
}
