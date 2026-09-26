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

//! Go `pkg/expression/vs_helper.go`.

use tidb_datatype::{Datum, FieldTypeCode, VectorFloat32};
use tidb_proto::tipb::ScalarFuncSig;

use crate::column::Column;
use crate::expression::Expression;

/// Go `expression.VSInfo`.
#[derive(Clone, Debug)]
pub struct VectorSearchInfo {
    /// Distance function name.
    pub distance_fn_name: String,
    /// Pushdown function signature code.
    pub fn_pb_code: i32,
    /// Query vector constant.
    pub vector: VectorFloat32,
    /// Vector column.
    pub column: Column,
}

impl PartialEq for VectorSearchInfo {
    fn eq(&self, other: &Self) -> bool {
        self.distance_fn_name == other.distance_fn_name
            && self.fn_pb_code == other.fn_pb_code
            && self.vector == other.vector
            && self.column.equals(&other.column)
    }
}

/// Go `InterpretVectorSearchExpr`: recognizes a vector-distance scalar
/// function with exactly one vector column and one vector literal.
#[must_use]
pub fn interpret_vector_search_expr(expression: &Expression) -> Option<VectorSearchInfo> {
    let Expression::ScalarFunction(function) = expression else {
        return None;
    };
    let name = function.func_name.lowercase();
    let signature = match name {
        "vec_l1_distance" => ScalarFuncSig::VecL1DistanceSig,
        "vec_l2_distance" => ScalarFuncSig::VecL2DistanceSig,
        "vec_negative_inner_product" => ScalarFuncSig::VecNegativeInnerProductSig,
        "vec_cosine_distance" => ScalarFuncSig::VecCosineDistanceSig,
        _ => return None,
    };

    let mut vector_column = None;
    let mut vector_constant = None;
    let mut column_count = 0_u8;
    let mut constant_count = 0_u8;
    for argument in function.get_args() {
        match argument {
            Expression::Column(column) => {
                if column.get_static_type()?.code() != FieldTypeCode::VectorFloat32 {
                    return None;
                }
                vector_column = Some(column.clone());
                column_count += 1;
            }
            Expression::Constant(constant) => {
                if constant.get_static_type()?.code() != FieldTypeCode::VectorFloat32 {
                    return None;
                }
                let Datum::VectorFloat32(vector) = &constant.value else {
                    return None;
                };
                vector_constant = Some(vector.clone());
                constant_count += 1;
            }
            _ => {}
        }
    }
    if column_count != 1 || constant_count != 1 {
        return None;
    }
    Some(VectorSearchInfo {
        distance_fn_name: name.to_owned(),
        fn_pb_code: signature as i32,
        vector: vector_constant?,
        column: vector_column?,
    })
}
