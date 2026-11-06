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


//! Go `pkg/executor/aggfuncs/func_json_objectagg.go`: `JSON_OBJECTAGG`, and
//! the value conversion (`getRealJSONValue`) and value bytes
//! (`getValMemDelta`) `JSON_ARRAYAGG` shares.

use std::collections::BTreeMap;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::{BinaryJSON, BinaryJSONValue, Datum, FieldType, FieldTypeCode, Opaque};
use tidb_driver_error::ExecError;
use tidb_goruntime::GoString;
use tidb_hack::MemAwareMap;
use tidb_serialization::{self as util, InterfaceValue};

use crate::agg_func::{deserialize_partial_result_common, partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, PartialResult};
use crate::spill_serialize_helper::SerializeHelper;

/// Go `DefPartialResult4JsonObjectAgg`.
pub const DEF_PARTIAL_RESULT_4_JSON_OBJECT_AGG: i64 = 8;

/// Go `DefInterfaceSize`.
const DEF_INTERFACE_SIZE: i64 = 16;

/// Go's `any` a JSON aggregate keeps (`getRealJSONValue`'s result): `None`
/// is Go's `nil`.
#[derive(Clone, Debug, PartialEq)]
pub struct RealJSONValue(pub Option<InterfaceValue>);

/// A Go `any` in a map: an interface's two words.
impl tidb_hack::MapValueLayout for RealJSONValue {
    const SOURCE_SIZE: usize = 16;
    const SOURCE_ALIGN: usize = 8;
}

/// Go `getValMemDelta`: the interface, plus what its value holds.
pub(crate) fn get_val_mem_delta(val: &RealJSONValue) -> i64 {
    DEF_INTERFACE_SIZE
        + match &val.0 {
            None => 0,
            Some(InterfaceValue::Bool(_)) => 1,
            Some(InterfaceValue::Int64(_) | InterfaceValue::Uint64(_) | InterfaceValue::Float64(_)) => 8,
            Some(InterfaceValue::String(v)) => v.len() as i64,
            // +1 for the type code.
            Some(InterfaceValue::BinaryJSON(v)) => v.value().len() as i64 + 1,
            Some(InterfaceValue::Opaque(v)) => v.bytes.len() as i64 + 1,
            // Go `DefTimeSize` and `DefDurationSize`.
            Some(InterfaceValue::Time(_)) => 8,
            Some(InterfaceValue::Duration(_)) => 16,
        }
}

/// Go `getRealJSONValue`: the Go value a datum appends to a JSON document --
/// binary strings and bit values as opaque values of the argument's type,
/// a `FLOAT` and a `DECIMAL` as `float64`.
pub(crate) fn get_real_json_value(value: &Datum, ft: &FieldType) -> Result<RealJSONValue, ExecError> {
    let opaque = |buf: Vec<u8>| InterfaceValue::Opaque(Opaque { type_code: ft.code().mysql_type(), bytes: buf });
    let real = match value {
        Datum::Null => None,
        Datum::BinaryLiteral(b) | Datum::Bit(b) => Some(opaque(b.as_bytes().to_vec())),
        Datum::Bytes(b) => Some(opaque(b.clone())),
        Datum::String(text) if ft.charset_name() == "binary" => {
            let mut result_buf = text.bytes().to_vec();
            if ft.code() == FieldTypeCode::String {
                // The trailing zeros are part of the opaque value.
                result_buf.resize(usize::try_from(ft.flen()).unwrap_or(0), 0);
            }
            Some(opaque(result_buf))
        }
        Datum::String(text) => Some(InterfaceValue::String(GoString::from(text.bytes().to_vec()))),
        Datum::Int(v) => Some(InterfaceValue::Int64(*v)),
        Datum::UInt(v) => Some(InterfaceValue::Uint64(*v)),
        Datum::Float32(v) => Some(InterfaceValue::Float64(f64::from(*v as f32))),
        Datum::Real(v) => Some(InterfaceValue::Float64(*v)),
        Datum::Decimal(v) => Some(InterfaceValue::Float64(v.to_f64())),
        Datum::Time(v) => Some(InterfaceValue::Time(*v)),
        Datum::Duration(v) => Some(InterfaceValue::Duration(*v)),
        Datum::Json(v) => Some(InterfaceValue::BinaryJSON(v.clone())),
        other => {
            // Go: `ErrUnsupportedSecondArgumentType.GenWithStackByArgs(x)`.
            let text = String::from_utf8_lossy(&other.sql_bytes().unwrap_or_default()).into_owned();
            return Err(ExecError::Mysql(tidb_terror::MysqlError::from_terror(
                &tidb_datatype::types_errors::ERR_UNSUPPORTED_SECOND_ARGUMENT_TYPE.gen_with_stack_by_args(&[text.into()]),
            )));
        }
    };
    Ok(RealJSONValue(real))
}

/// The Go value as `CreateBinaryJSON` encodes it.
pub(crate) fn binary_json_value(val: &RealJSONValue) -> Result<BinaryJSONValue, ExecError> {
    Ok(match &val.0 {
        None => BinaryJSONValue::Null,
        Some(InterfaceValue::Bool(v)) => BinaryJSONValue::Bool(*v),
        Some(InterfaceValue::Int64(v)) => BinaryJSONValue::Int64(*v),
        Some(InterfaceValue::Uint64(v)) => BinaryJSONValue::Uint64(*v),
        Some(InterfaceValue::Float64(v)) => BinaryJSONValue::Float64(*v),
        Some(InterfaceValue::String(v)) => BinaryJSONValue::String(go_string_text(v)?),
        Some(InterfaceValue::BinaryJSON(v)) => BinaryJSONValue::Binary(v.clone()),
        Some(InterfaceValue::Opaque(v)) => BinaryJSONValue::Opaque(v.clone()),
        Some(InterfaceValue::Time(v)) => BinaryJSONValue::Time(*v),
        Some(InterfaceValue::Duration(v)) => BinaryJSONValue::Duration(*v),
    })
}

fn go_string_text(v: &GoString) -> Result<String, ExecError> {
    String::from_utf8(v.as_bytes().to_vec()).map_err(|_| ExecError::unsupported("this JSON document cannot be encoded"))
}

/// Go `types.CreateBinaryJSONWithCheck`.
pub(crate) fn create_binary_json_with_check(value: &BinaryJSONValue) -> Result<BinaryJSON, ExecError> {
    BinaryJSON::from_typed_value(value).map_err(|_| ExecError::unsupported("this JSON document cannot be encoded"))
}

/// Go `partialResult4JsonObjectAgg`.
pub struct PartialResult4JsonObjectAgg {
    pub(crate) entries: MemAwareMap<GoString, RealJSONValue>,
}

/// Go `p.entries.Init(make(map[string]any))`: an empty map and its bytes.
fn new_entries() -> (MemAwareMap<GoString, RealJSONValue>, i64) {
    let entries = MemAwareMap::new(0);
    let bytes = entries.bytes() as i64;
    (entries, bytes)
}

/// Go `jsonObjectAgg`.
#[derive(Clone, Debug)]
pub struct JsonObjectAgg {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl AggFunc for JsonObjectAgg {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        let (entries, bytes) = new_entries();
        (Box::new(PartialResult4JsonObjectAgg { entries }), DEF_PARTIAL_RESULT_4_JSON_OBJECT_AGG + bytes)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        partial::<PartialResult4JsonObjectAgg>(pr).entries = new_entries().0;
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4JsonObjectAgg>(pr);
        if p.entries.is_empty() {
            chk.append_null(self.base.ordinal);
            return Ok(());
        }
        let mut object = BTreeMap::new();
        for (key, val) in p.entries.iter() {
            object.insert(go_string_text(key)?, binary_json_value(val)?);
        }
        let bj = create_binary_json_with_check(&BinaryJSONValue::Object(object))?;
        chk.append_json(self.base.ordinal, &bj);
        Ok(())
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4JsonObjectAgg>(pr);
        let mut mem_delta = 0;
        let key_type = crate::agg_func::get_type(&self.base.args[0], sctx)?;
        let value_type = crate::agg_func::get_type(&self.base.args[1], sctx)?;
        for row in rows_in_group {
            let key = match self.base.args[0].eval(sctx, *row).map_err(ExecError::Eval)? {
                Datum::Null => {
                    // Go `ErrJSONDocumentNULLKey`, with no warning beside it.
                    return Err(ExecError::Mysql(
                        tidb_terror::MysqlError::from_terror(&tidb_datatype::types_errors::ERR_JSON_DOCUMENT_NULL_KEY.gen_with_stack_by_args(&[]))
                            .from_evaluation(),
                    ));
                }
                // Go `EvalString`.
                key => GoString::from(key.sql_bytes().map_err(|error| ExecError::internal(error.to_string()))?),
            };
            if key_type.charset_name() == "binary" {
                return Err(ExecError::Mysql(tidb_terror::MysqlError::from_terror(
                    &tidb_datatype::types_errors::ERR_INVALID_JSON_CHARSET.gen_with_stack_by_args(&["binary".to_owned().into()]),
                )));
            }
            let value = self.base.args[1].eval(sctx, *row).map_err(ExecError::Eval)?;
            let real_val = get_real_json_value(&value, &value_type)?;
            let key_len = key.len() as i64;
            let val_delta = get_val_mem_delta(&real_val);
            let (delta, insert) = p.entries.set_ext(key, real_val);
            if insert {
                mem_delta += key_len + val_delta + delta;
            }
        }
        Ok(mem_delta)
    }

    fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
        // Go: a duplicate key keeps the last value.
        let source: Vec<(GoString, RealJSONValue)> = partial::<PartialResult4JsonObjectAgg>(src)
            .entries
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let p2 = partial::<PartialResult4JsonObjectAgg>(dst);
        let mut mem_delta = 0;
        for (k, v) in source {
            mem_delta += k.len() as i64 + get_val_mem_delta(&v) + p2.entries.set(k, v);
        }
        Ok(mem_delta)
    }

    fn serialize_partial_result(&self, partial_result: &mut PartialResult, chk: &mut Chunk, spill_helper: &mut SerializeHelper) {
        let pr = partial::<PartialResult4JsonObjectAgg>(partial_result);
        let res_buf = spill_helper.serialize_partial_result_4_json_object_agg(pr);
        chk.append_bytes(self.base.ordinal, res_buf);
    }

    fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
        deserialize_partial_result_common(src, self.base.ordinal, |helper| {
            let (mut pr, mem_delta) = self.alloc_partial_result();
            let deserialize_mem_delta = helper.deserialize_partial_result_4_json_object_agg(partial::<PartialResult4JsonObjectAgg>(&mut pr))?;
            Some((pr, mem_delta + deserialize_mem_delta))
        })
    }
}

/// Go `SerializeInterface`: a nil value has no spill encoding.
pub(crate) fn serialize_interface(value: &RealJSONValue, buf: &mut Vec<u8>) {
    let Some(value) = &value.0 else {
        panic!("Agg spill encounters an unexpected interface type!");
    };
    util::serialize_interface(value, buf);
}
