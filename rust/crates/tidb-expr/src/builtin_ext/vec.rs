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

//! Vector SQL functions from `pkg/expression/builtin_vec.go`.

use tidb_datatype::{ConversionFlags, FieldType, FieldTypeCode, VectorFloat32};

use crate::{Datum, EvalError};

pub(crate) fn dispatch(name: &str, vals: &[Datum]) -> Option<Result<Datum, EvalError>> {
    match (name, vals) {
        ("VEC_DIMS", [value]) => Some(dims(value)),
        ("VEC_L1_DISTANCE", [left, right]) => {
            Some(distance(left, right, VectorFloat32::l1_distance))
        }
        ("VEC_L2_DISTANCE", [left, right]) => {
            Some(distance(left, right, VectorFloat32::l2_distance))
        }
        ("VEC_NEGATIVE_INNER_PRODUCT", [left, right]) => {
            Some(distance(left, right, VectorFloat32::negative_inner_product))
        }
        ("VEC_COSINE_DISTANCE", [left, right]) => {
            Some(distance(left, right, VectorFloat32::cosine_distance))
        }
        ("VEC_L2_NORM", [value]) => Some(l2_norm(value)),
        ("VEC_FROM_TEXT", [value]) => Some(from_text(value)),
        ("VEC_AS_TEXT", [value]) => Some(as_text(value)),
        _ => None,
    }
}

fn vector(value: &Datum) -> Result<Option<VectorFloat32>, EvalError> {
    if matches!(value, Datum::Null) {
        return Ok(None);
    }
    let target = FieldType::new(FieldTypeCode::VectorFloat32);
    match value
        .convert_to(&target, ConversionFlags::default())
        .map_err(|error| EvalError::Vector(error.to_string()))?
        .value
    {
        Datum::VectorFloat32(value) => Ok(Some(value)),
        _ => unreachable!("a VectorFloat32 conversion returns a vector datum"),
    }
}

fn dims(value: &Datum) -> Result<Datum, EvalError> {
    Ok(vector(value)?.map_or(Datum::Null, |value| Datum::Int(value.len() as i64)))
}

fn distance(
    left: &Datum,
    right: &Datum,
    operation: impl FnOnce(&VectorFloat32, &VectorFloat32) -> Result<f64, tidb_datatype::VectorError>,
) -> Result<Datum, EvalError> {
    let Some(left) = vector(left)? else {
        return Ok(Datum::Null);
    };
    let Some(right) = vector(right)? else {
        return Ok(Datum::Null);
    };
    let result = operation(&left, &right).map_err(|error| EvalError::Vector(error.to_string()))?;
    Ok(if result.is_nan() {
        Datum::Null
    } else {
        Datum::Real(result)
    })
}

fn l2_norm(value: &Datum) -> Result<Datum, EvalError> {
    let Some(value) = vector(value)? else {
        return Ok(Datum::Null);
    };
    let norm = value.l2_norm();
    Ok(if norm.is_nan() {
        Datum::Null
    } else {
        Datum::Real(norm)
    })
}

fn from_text(value: &Datum) -> Result<Datum, EvalError> {
    let Some(bytes) = crate::arg_eval_type::eval_string(value)? else {
        return Ok(Datum::Null);
    };
    let text = std::str::from_utf8(&bytes).map_err(|error| EvalError::Vector(error.to_string()))?;
    VectorFloat32::parse(text)
        .map(Datum::new_vector_float32)
        .map_err(|error| EvalError::Vector(error.to_string()))
}

fn as_text(value: &Datum) -> Result<Datum, EvalError> {
    Ok(vector(value)?.map_or(Datum::Null, |value| Datum::new_string(value.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector(values: Vec<f32>) -> Datum {
        Datum::new_vector_float32(VectorFloat32::must_create(values))
    }

    #[test]
    fn vector_functions_follow_the_scalar_go_signatures() {
        assert_eq!(
            dispatch("VEC_DIMS", &[vector(vec![1.0, 2.0])]),
            Some(Ok(Datum::Int(2)))
        );
        assert_eq!(
            dispatch(
                "VEC_L1_DISTANCE",
                &[vector(vec![1.0, 2.0]), vector(vec![3.0, 5.0])]
            ),
            Some(Ok(Datum::Real(5.0)))
        );
        assert_eq!(
            dispatch(
                "VEC_L2_DISTANCE",
                &[vector(vec![0.0, 0.0]), vector(vec![3.0, 4.0])]
            ),
            Some(Ok(Datum::Real(5.0)))
        );
        assert_eq!(
            dispatch(
                "VEC_NEGATIVE_INNER_PRODUCT",
                &[vector(vec![1.0, 2.0]), vector(vec![3.0, 4.0])]
            ),
            Some(Ok(Datum::Real(-11.0)))
        );
        assert_eq!(
            dispatch("VEC_L2_NORM", &[vector(vec![3.0, 4.0])]),
            Some(Ok(Datum::Real(5.0)))
        );
        assert_eq!(
            dispatch("VEC_AS_TEXT", &[vector(vec![1.0, 2.0])]),
            Some(Ok(Datum::new_string("[1,2]")))
        );
        assert_eq!(
            dispatch("VEC_FROM_TEXT", &[Datum::new_string("[1,2]")]),
            Some(Ok(vector(vec![1.0, 2.0])))
        );
    }

    #[test]
    fn vector_functions_propagate_null_and_source_domain_errors() {
        assert_eq!(dispatch("VEC_DIMS", &[Datum::Null]), Some(Ok(Datum::Null)));
        assert_eq!(
            dispatch(
                "VEC_COSINE_DISTANCE",
                &[vector(vec![0.0]), vector(vec![1.0])]
            ),
            Some(Ok(Datum::Null))
        );
        assert!(matches!(
            dispatch("VEC_L2_DISTANCE", &[vector(vec![1.0]), vector(vec![1.0, 2.0])]),
            Some(Err(EvalError::Vector(message))) if message == "vectors have different dimensions: 1 and 2"
        ));
    }

    #[test]
    fn vector_functions_are_reachable_from_the_sql_expression_path() {
        let expression = tidb_ast::Expr::Func {
            name: "vec_l2_distance".to_owned(),
            args: vec![
                tidb_ast::Expr::Func {
                    name: "vec_from_text".to_owned(),
                    args: vec![tidb_ast::Expr::String("[0,0]".to_owned())],
                    origin_position: 0,
                },
                tidb_ast::Expr::Func {
                    name: "vec_from_text".to_owned(),
                    args: vec![tidb_ast::Expr::String("[3,4]".to_owned())],
                    origin_position: 0,
                },
            ],
            origin_position: 0,
        };
        assert_eq!(
            crate::eval_in(&expression, &crate::NoColumns),
            Ok(Datum::Real(5.0))
        );
        let rewritten = crate::rewriter::rewrite_expr(&expression).expect("vector rewrite");
        assert_eq!(
            rewritten.static_type().expect("vector result type").code(),
            FieldTypeCode::Double
        );
    }
}
