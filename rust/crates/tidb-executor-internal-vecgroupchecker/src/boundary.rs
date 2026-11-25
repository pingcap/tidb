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

//! Native versions of the typed boundary values that Go feeds to EncodeKey.

use std::borrow::Cow;

use tidb_charset::Collation;
use tidb_chunk::{
    chunk::Chunk,
    column::{RawCells, VAR_ELEM_LEN},
};
use tidb_codec::{
    encode_bytes, encode_float, encode_int, BYTES_FLAG, DECIMAL_FLAG, DURATION_FLAG, FLOAT_FLAG,
    INT_FLAG, JSON_FLAG, NIL_FLAG,
};
use tidb_datatype::{CollationCollator, FieldTypeCode, MyDecimal, MYDECIMAL_STRUCT_SIZE};
use tidb_expr::expression::Expression;

#[derive(Clone, Copy, Debug)]
enum KeyType {
    Int,
    Duration,
    Float,
    Double,
    Decimal,
    String,
    Json,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BoundaryColumn {
    index: usize,
    kind: KeyType,
}

impl BoundaryColumn {
    pub(crate) fn prepare(expr: &Expression) -> Option<Self> {
        let Expression::Column(column) = expr else {
            return None;
        };
        let kind = match column.get_static_type()?.code() {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Year => KeyType::Int,
            FieldTypeCode::Duration => KeyType::Duration,
            FieldTypeCode::Float => KeyType::Float,
            FieldTypeCode::Double => KeyType::Double,
            FieldTypeCode::NewDecimal => KeyType::Decimal,
            FieldTypeCode::String
            | FieldTypeCode::VarString
            | FieldTypeCode::Varchar
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob
            | FieldTypeCode::Blob => KeyType::String,
            FieldTypeCode::Json => KeyType::Json,
            _ => return None,
        };
        Some(Self {
            index: usize::try_from(column.index).ok()?,
            kind,
        })
    }

    #[inline]
    pub(crate) fn append_pair(
        self,
        chunk: &Chunk,
        first: usize,
        last: usize,
        first_key: &mut Vec<u8>,
        last_key: &mut Vec<u8>,
        collation: Collation,
        new_collation: bool,
    ) -> bool {
        if self.index >= chunk.num_cols() {
            return false;
        }
        let column = chunk.column(self.index);
        let width = match self.kind {
            KeyType::Int | KeyType::Duration | KeyType::Double => 8,
            KeyType::Float => 4,
            KeyType::Decimal => MYDECIMAL_STRUCT_SIZE as i64,
            KeyType::String | KeyType::Json => VAR_ELEM_LEN,
        };
        if column.type_size() != width {
            return false;
        }
        let view = column.raw_cells();
        self.kind
            .append(&view, first, first_key, collation, new_collation)
            && self
                .kind
                .append(&view, last, last_key, collation, new_collation)
    }
}

impl KeyType {
    #[inline]
    fn append(
        self,
        view: &RawCells<'_>,
        row: usize,
        output: &mut Vec<u8>,
        collation: Collation,
        new_collation: bool,
    ) -> bool {
        if view.is_null(row) {
            output.push(NIL_FLAG);
            return true;
        }
        match self {
            Self::Int | Self::Duration => {
                let flag = if matches!(self, Self::Int) {
                    INT_FLAG
                } else {
                    DURATION_FLAG
                };
                output.push(flag);
                encode_int(output, i64::from_ne_bytes(view.fixed_cells::<8>()[row]));
            }
            Self::Float => {
                output.push(FLOAT_FLAG);
                encode_float(
                    output,
                    f64::from(f32::from_ne_bytes(view.fixed_cells::<4>()[row])),
                );
            }
            Self::Double => {
                output.push(FLOAT_FLAG);
                encode_float(output, f64::from_ne_bytes(view.fixed_cells::<8>()[row]));
            }
            Self::String => {
                let bytes = view.get(row);
                let key = if new_collation {
                    collation.immutable_key(bytes)
                } else {
                    Cow::Borrowed(bytes)
                };
                output.push(BYTES_FLAG);
                encode_bytes(output, &key);
            }
            Self::Json => {
                let bytes = view.get(row);
                if bytes.is_empty() {
                    return false;
                }
                output.push(JSON_FLAG);
                output.extend_from_slice(bytes);
            }
            Self::Decimal => {
                return append_exact_decimal(
                    output,
                    view.fixed_cells::<MYDECIMAL_STRUCT_SIZE>()[row],
                )
            }
        }
        true
    }
}

fn append_exact_decimal(output: &mut Vec<u8>, bytes: [u8; MYDECIMAL_STRUCT_SIZE]) -> bool {
    let Ok(value) = MyDecimal::from_raw_bytes(bytes) else {
        return false;
    };
    let scale = i32::from(value.result_frac());
    // Go round-trips the boundary through display text. Keep that path when
    // rendering would round it or normalize a negative zero.
    if scale != i32::from(value.digits_frac())
        || scale > 30
        || (value.is_negative() && value.to_i128_scaled().is_some_and(|(value, _)| value == 0))
    {
        return false;
    }
    let precision = (value.significant_integer_digits() + scale).max(1);
    output.extend_from_slice(&[DECIMAL_FLAG, precision as u8, scale as u8]);
    matches!(value.write_bin(precision, scale, output), Ok(None))
}
