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


//! Go `pkg/executor/aggfuncs/func_json_arrayagg.go`: `JSON_ARRAYAGG`, which
//! keeps every row's value -- a NULL as JSON `null`.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::{BinaryJSONValue, Datum};
use tidb_driver_error::ExecError;

use crate::agg_func::{deserialize_partial_result_common, partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, PartialResult};
use crate::func_json_objectagg::{binary_json_value, create_binary_json_with_check, get_real_json_value, get_val_mem_delta, RealJSONValue};
use crate::spill_serialize_helper::SerializeHelper;

/// Go `DefPartialResult4JsonArrayagg`.
pub const DEF_PARTIAL_RESULT_4_JSON_ARRAYAGG: i64 = 24;
/// Go `DefSliceSize`.
pub const DEF_SLICE_SIZE: i64 = 24;

/// Go `partialResult4JsonArrayagg`.
#[derive(Clone, Debug, Default)]
pub struct PartialResult4JsonArrayagg {
    pub(crate) entries: Vec<RealJSONValue>,
}

/// Go `jsonArrayagg`.
#[derive(Clone, Debug)]
pub struct JsonArrayagg {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl AggFunc for JsonArrayagg {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        (Box::new(PartialResult4JsonArrayagg::default()), DEF_PARTIAL_RESULT_4_JSON_ARRAYAGG + DEF_SLICE_SIZE)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        partial::<PartialResult4JsonArrayagg>(pr).entries.clear();
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4JsonArrayagg>(pr);
        if p.entries.is_empty() {
            chk.append_null(self.base.ordinal);
            return Ok(());
        }
        let array = p.entries.iter().map(binary_json_value).collect::<Result<Vec<_>, _>>()?;
        let json = create_binary_json_with_check(&BinaryJSONValue::Array(array))?;
        chk.append_json(self.base.ordinal, &json);
        Ok(())
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4JsonArrayagg>(pr);
        let mut mem_delta = 0;
        let arg_type = crate::agg_func::get_type(&self.base.args[0], sctx)?;
        for row in rows_in_group {
            let item: Datum = self.base.args[0].eval(sctx, *row).map_err(ExecError::Eval)?;
            let real_item = get_real_json_value(&item, &arg_type)?;
            mem_delta += get_val_mem_delta(&real_item);
            p.entries.push(real_item);
        }
        Ok(mem_delta)
    }

    fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
        let p1 = partial::<PartialResult4JsonArrayagg>(src).entries.clone();
        partial::<PartialResult4JsonArrayagg>(dst).entries.extend(p1);
        Ok(0)
    }

    fn serialize_partial_result(&self, partial_result: &mut PartialResult, chk: &mut Chunk, spill_helper: &mut SerializeHelper) {
        let pr = partial::<PartialResult4JsonArrayagg>(partial_result);
        let res_buf = spill_helper.serialize_partial_result_4_json_arrayagg(pr);
        chk.append_bytes(self.base.ordinal, res_buf);
    }

    fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
        deserialize_partial_result_common(src, self.base.ordinal, |helper| {
            let (mut pr, mem_delta) = self.alloc_partial_result();
            helper.deserialize_partial_result_4_json_arrayagg(partial::<PartialResult4JsonArrayagg>(&mut pr))?;
            Some((pr, mem_delta))
        })
    }
}
