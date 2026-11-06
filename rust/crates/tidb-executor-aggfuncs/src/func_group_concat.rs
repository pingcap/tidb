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


//! Go `pkg/executor/aggfuncs/func_group_concat.go`: `GROUP_CONCAT`, plain,
//! `DISTINCT`, `ORDER BY` (a top-N heap bounded by `group_concat_max_len`)
//! and both.
//!
//! The memory deltas are Go's: a `bytes.Buffer` is charged by its capacity
//! ([`tidb_goruntime::go_bytes::Buffer`] grows as Go's does), a DISTINCT
//! key buffer by `codec.EncodeBytes`'s `reallocBytes` capacity, and an
//! ORDER BY row by its buffer and [`get_datum_mem_size`] of each key.

use std::cmp::Ordering;
use std::sync::atomic::{AtomicI32, Ordering as AtomicOrdering};
use std::sync::Arc;

use tidb_charset::Collation;
use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::Datum;
use tidb_driver_error::ExecError;
use tidb_datatype::CollationCollator;
use tidb_expr::EvalErrorSql;
use tidb_expr::expression::Expression;
use tidb_expression_aggregation::ByItems;
use tidb_exprctx::EvalError;
use tidb_goruntime::go_bytes::Buffer;
use tidb_goruntime::{go_heap, go_pdqsort, GoString};
use tidb_set::{StringSetWithMemoryUsage, StringToStringMapWithMemoryUsage};

use crate::agg_func::{deserialize_partial_result_common, partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, PartialResult};
use crate::spill_serialize_helper::SerializeHelper;

/// Go `DefPartialResult4GroupConcatSize`.
pub const DEF_PARTIAL_RESULT_4_GROUP_CONCAT_SIZE: i64 = 16;
/// Go `DefPartialResult4GroupConcatDistinctSize`.
pub const DEF_PARTIAL_RESULT_4_GROUP_CONCAT_DISTINCT_SIZE: i64 = 72;
/// Go `DefPartialResult4GroupConcatOrderSize`.
pub const DEF_PARTIAL_RESULT_4_GROUP_CONCAT_ORDER_SIZE: i64 = 8;
/// Go `DefPartialResult4GroupConcatOrderDistinctSize`.
pub const DEF_PARTIAL_RESULT_4_GROUP_CONCAT_ORDER_DISTINCT_SIZE: i64 = 72;
/// Go `DefBytesBufferSize`.
pub const DEF_BYTES_BUFFER_SIZE: i64 = 40;
/// Go `DefTopNRowsSize`.
pub const DEF_TOP_N_ROWS_SIZE: i64 = 136;

/// Go `types.EmptyDatumSize`.
const EMPTY_DATUM_SIZE: i64 = 72;
/// Go `DefMyDecimalSize`.
const DEF_MY_DECIMAL_SIZE: i64 = 40;
/// Go `DefTimeSize`.
const DEF_TIME_SIZE: i64 = 8;

/// Go `GetDatumMemSize`: the `Datum` struct, its collation name, its `b`
/// bytes, and what its `x` interface holds beyond the interface itself (a
/// `*MyDecimal`, a `Time`; every other kind keeps `x` nil).
#[must_use]
pub fn get_datum_mem_size(d: &Datum) -> i64 {
    let (collation, bytes, interface) = match d {
        Datum::String(value) => (value.collation().name().len(), value.bytes().len(), 0),
        Datum::Bytes(value) | Datum::Raw(value) => (0, value.len(), 0),
        // Go `SetBinaryLiteral` sets `charset.CollationBin`.
        Datum::BinaryLiteral(value) => (Collation::Binary.name().len(), value.as_bytes().len(), 0),
        Datum::Bit(value) => (0, value.as_bytes().len(), 0),
        Datum::Enum(value, collation) => (collation.name().len(), value.name_bytes().len(), 0),
        Datum::Set(value, collation) => (collation.name().len(), value.name_bytes().len(), 0),
        Datum::Json(value) => (0, value.value().len(), 0),
        Datum::VectorFloat32(value) => (0, value.serialize().len(), 0),
        Datum::Decimal(_) => (0, 0, DEF_MY_DECIMAL_SIZE),
        Datum::Time(_) => (0, 0, DEF_TIME_SIZE),
        Datum::Null
        | Datum::MinNotNull
        | Datum::MaxValue
        | Datum::Int(_)
        | Datum::UInt(_)
        | Datum::Real(_)
        | Datum::Float32(_)
        | Datum::Duration(_) => (0, 0, 0),
    };
    EMPTY_DATUM_SIZE + collation as i64 + bytes as i64 + interface
}

/// Go `codec.EncodeBytes` into a reused key buffer, with Go's capacity:
/// `reallocBytes` makes room for `(len/8+1)*9` more bytes exactly.
#[derive(Clone, Debug, Default)]
pub(crate) struct EncodeBytesBuffer {
    buf: Vec<u8>,
    cap: usize,
}

impl EncodeBytesBuffer {
    fn reset(&mut self) {
        self.buf.clear();
    }

    fn encode_bytes(&mut self, data: &[u8]) {
        let realloc_size = (data.len() / 8 + 1) * 9;
        let new_size = self.buf.len() + realloc_size;
        if self.cap < new_size {
            self.cap = new_size;
        }
        tidb_codec::encode_bytes(&mut self.buf, data);
    }

    fn cap(&self) -> i64 {
        self.cap as i64
    }
}

/// Go `baseGroupConcat4String`.
#[derive(Clone, Debug)]
pub struct BaseGroupConcat4String {
    /// Go's embedded `baseAggFunc`: the arguments without the separator.
    pub base: BaseAggFunc,
    /// Go `byItems`.
    pub by_items: Vec<ByItems>,
    /// Go `sep`.
    pub sep: Vec<u8>,
    /// Go `maxLen` (`group_concat_max_len`).
    pub max_len: u64,
    /// Go `truncated`: one 1260 per statement, shared through
    /// `SetTruncated` by every function the executor builds for it.
    pub truncated: Arc<AtomicI32>,
}

impl BaseGroupConcat4String {
    /// Go `EvalString` of an argument: a string's bytes, or an ENUM, SET
    /// or BIT column's `ToString` (Go `Column.EvalString`'s hybrid case).
    /// The planner casts every other argument to a string first.
    fn eval_string(arg: &Expression, sctx: &AggFuncUpdateContext, row: Row<'_>) -> Result<Option<Vec<u8>>, ExecError> {
        Ok(match arg.eval(sctx, row).map_err(ExecError::Eval)? {
            Datum::Null => None,
            Datum::String(text) => Some(text.into_bytes()),
            Datum::Bytes(bytes) => Some(bytes),
            hybrid @ (Datum::Enum(..) | Datum::Set(..) | Datum::Bit(_)) => {
                Some(hybrid.sql_bytes().map_err(|error| ExecError::internal(error.to_string()))?)
            }
            _ => return Err(ExecError::internal("GROUP_CONCAT read a non-string argument")),
        })
    }

    /// Go `handleTruncateError`: the first truncation of the statement
    /// fails it, or warns where truncation is a warning.
    fn handle_truncate_error(&self, ctx: &AggFuncUpdateContext) -> Result<(), ExecError> {
        if self.truncated.compare_exchange(0, 1, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst).is_ok() {
            let text = tidb_expr::explain::physical_expression_text_with_columns(
                ctx,
                &self.base.args[0],
                &[],
                tidb_expr::explain::ExpressionTextStyle::StringWithCtx,
            )
            .unwrap_or_default();
            let error = EvalError::GroupConcatCut(format!("Some rows were cut by GROUPCONCAT({text})"));
            if !ctx.type_flags().truncate_as_warning() {
                return Err(ExecError::Eval(error));
            }
            ctx.append_error_warning(&error.to_sql_error());
        }
        Ok(())
    }

    /// Go `truncatePartialResultIfNeed`.
    fn truncate_partial_result_if_need(&self, ctx: &AggFuncUpdateContext, buffer: &mut Buffer) -> Result<(), ExecError> {
        if self.max_len > 0 && buffer.len() as u64 > self.max_len {
            buffer.truncate(self.max_len as usize);
            return self.handle_truncate_error(ctx);
        }
        Ok(())
    }

    /// Go `SetTruncated`.
    pub fn set_truncated(&mut self, t: Arc<AtomicI32>) {
        self.truncated = t;
    }

    /// Go `GetTruncated`.
    #[must_use]
    pub fn get_truncated(&self) -> Arc<AtomicI32> {
        Arc::clone(&self.truncated)
    }
}

/// Go `partialResult4GroupConcat` (`basePartialResult4GroupConcat`).
#[derive(Clone, Debug, Default)]
pub struct PartialResult4GroupConcat {
    pub(crate) vals_buf: Buffer,
    pub(crate) buffer: Option<Buffer>,
}

/// Go `groupConcat`.
#[derive(Clone, Debug)]
pub struct GroupConcat(pub BaseGroupConcat4String);

impl AggFunc for GroupConcat {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        (Box::new(PartialResult4GroupConcat::default()), DEF_PARTIAL_RESULT_4_GROUP_CONCAT_SIZE + DEF_BYTES_BUFFER_SIZE)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        partial::<PartialResult4GroupConcat>(pr).buffer = None;
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4GroupConcat>(pr);
        let held = |p: &PartialResult4GroupConcat| p.vals_buf.cap() as i64 + p.buffer.as_ref().map_or(0, |b| b.cap() as i64);
        let mut mem_delta = -held(p);
        let result = (|| {
            for row in rows_in_group {
                p.vals_buf.reset();
                let mut is_null = false;
                for arg in &self.0.base.args {
                    let Some(v) = BaseGroupConcat4String::eval_string(arg, sctx, *row)? else {
                        is_null = true;
                        break;
                    };
                    p.vals_buf.write(&v);
                }
                if is_null {
                    continue;
                }
                match p.buffer.as_mut() {
                    None => {
                        p.buffer = Some(Buffer::new());
                        mem_delta += DEF_BYTES_BUFFER_SIZE;
                    }
                    Some(buffer) => buffer.write(&self.0.sep),
                }
                let vals = p.vals_buf.bytes().to_vec();
                p.buffer.as_mut().expect("just set").write(&vals);
            }
            match p.buffer.as_mut() {
                Some(buffer) => self.0.truncate_partial_result_if_need(sctx, buffer),
                None => Ok(()),
            }
        })();
        // Go's deferred re-charge runs on the error return too.
        mem_delta += held(p);
        result.map(|()| mem_delta)
    }

    fn merge_partial_result(&self, sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
        let Some(p1) = partial::<PartialResult4GroupConcat>(src).buffer.clone() else {
            return Ok(0);
        };
        let p2 = partial::<PartialResult4GroupConcat>(dst);
        let Some(buffer) = p2.buffer.as_mut() else {
            p2.buffer = Some(p1);
            return Ok(0);
        };
        let mut mem_delta = -(buffer.cap() as i64);
        buffer.write(&self.0.sep);
        buffer.write(p1.bytes());
        mem_delta += buffer.cap() as i64;
        self.0.truncate_partial_result_if_need(sctx, buffer).map(|()| mem_delta)
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        // Go `baseGroupConcat4String.AppendFinalResult2Chunk`.
        match &partial::<PartialResult4GroupConcat>(pr).buffer {
            None => chk.append_null(self.0.base.ordinal),
            Some(buffer) => chk.append_bytes(self.0.base.ordinal, buffer.bytes()),
        }
        Ok(())
    }

    fn serialize_partial_result(&self, partial_result: &mut PartialResult, chk: &mut Chunk, spill_helper: &mut SerializeHelper) {
        let pr = partial::<PartialResult4GroupConcat>(partial_result);
        let res_buf = spill_helper.serialize_partial_result_4_group_concat(pr.buffer.as_ref().map(Buffer::bytes));
        chk.append_bytes(self.0.base.ordinal, res_buf);
    }

    fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
        deserialize_partial_result_common(src, self.0.base.ordinal, |helper| {
            let (mut pr, mem_delta) = self.alloc_partial_result();
            let buffer = helper.deserialize_partial_result_4_group_concat()?;
            *partial::<PartialResult4GroupConcat>(&mut pr) = PartialResult4GroupConcat {
                vals_buf: Buffer::new(),
                buffer: buffer.map(Buffer::new_buffer),
            };
            Some((pr, mem_delta))
        })
    }
}

/// Go `partialResult4GroupConcatDistinct`.
pub struct PartialResult4GroupConcatDistinct {
    vals_buf: Buffer,
    val_set: StringToStringMapWithMemoryUsage,
    encode_bytes_buffer: EncodeBytesBuffer,
}

/// Go `baseGroupConcatDistinct4String`: the update and final result both
/// distinct types share.
fn alloc_distinct() -> (PartialResult, i64) {
    let (val_set, set_size) = StringToStringMapWithMemoryUsage::new();
    let p = PartialResult4GroupConcatDistinct {
        vals_buf: Buffer::new(),
        val_set,
        encode_bytes_buffer: EncodeBytesBuffer::default(),
    };
    (Box::new(p), DEF_PARTIAL_RESULT_4_GROUP_CONCAT_DISTINCT_SIZE + DEF_BYTES_BUFFER_SIZE + set_size)
}

/// The argument collations Go builds per update (`collate.GetCollator(
/// arg.GetType(sctx).GetCollate())`).
fn arg_collators(args: &[Expression]) -> Vec<Collation> {
    args.iter().map(crate::aggfuncs::expr_collation).collect()
}

fn update_distinct(e: &BaseGroupConcat4String, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
    let p = partial::<PartialResult4GroupConcatDistinct>(pr);
    let held = |p: &PartialResult4GroupConcatDistinct| p.vals_buf.cap() as i64 + p.encode_bytes_buffer.cap();
    let mut mem_delta = -held(p);
    let collators = arg_collators(&e.base.args);
    let result = (|| {
        for row in rows_in_group {
            p.vals_buf.reset();
            p.encode_bytes_buffer.reset();
            let mut is_null = false;
            for (i, arg) in e.base.args.iter().enumerate() {
                let Some(v) = BaseGroupConcat4String::eval_string(arg, sctx, *row)? else {
                    is_null = true;
                    break;
                };
                p.encode_bytes_buffer.encode_bytes(&collators[i].immutable_key(&v));
                p.vals_buf.write(&v);
            }
            if is_null {
                continue;
            }
            let joined_val = GoString::from(p.encode_bytes_buffer.buf.clone());
            if p.val_set.get(&joined_val).is_some() {
                continue;
            }
            let val_str = GoString::from(p.vals_buf.bytes().to_vec());
            let (joined_len, val_len) = (joined_val.len() as i64, val_str.len() as i64);
            mem_delta += p.val_set.insert(joined_val, val_str);
            mem_delta += joined_len;
            mem_delta += val_len;
        }
        Ok(())
    })();
    mem_delta += held(p);
    result.map(|()| mem_delta)
}

fn append_distinct(e: &BaseGroupConcat4String, sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
    let p = partial::<PartialResult4GroupConcatDistinct>(pr);
    if p.val_set.is_empty() {
        chk.append_null(e.base.ordinal);
        return Ok(());
    }
    let mut buffer = Buffer::new();
    for (_, val) in p.val_set.iter() {
        if !buffer.is_empty() {
            buffer.write(&e.sep);
        }
        buffer.write(val.as_bytes());
        if e.max_len > 0 && buffer.len() as u64 > e.max_len {
            e.truncate_partial_result_if_need(sctx, &mut buffer)?;
            break;
        }
    }
    chk.append_bytes(e.base.ordinal, buffer.bytes());
    Ok(())
}

/// Go `groupOriginalConcatDistinct` (`merges: false`) and
/// `groupPartialConcatDistinct`.
macro_rules! group_concat_distinct {
    ($name:ident, merges: $merges:literal) => {
        #[doc = concat!("Go `", stringify!($name), "`.")]
        #[derive(Clone, Debug)]
        pub struct $name(pub BaseGroupConcat4String);

        impl AggFunc for $name {
            fn alloc_partial_result(&self) -> (PartialResult, i64) {
                alloc_distinct()
            }

            fn reset_partial_result(&self, pr: &mut PartialResult) {
                partial::<PartialResult4GroupConcatDistinct>(pr).val_set = StringToStringMapWithMemoryUsage::new().0;
            }

            fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
                update_distinct(&self.0, sctx, rows_in_group, pr)
            }

            fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
                // Go's `baseAggFunc.MergePartialResult` for the original one.
                assert!($merges, "Not implemented");
                let source: Vec<(GoString, GoString)> = partial::<PartialResult4GroupConcatDistinct>(src)
                    .val_set
                    .iter()
                    .map(|(key, val)| (key.clone(), val.clone()))
                    .collect();
                let d = partial::<PartialResult4GroupConcatDistinct>(dst);
                let mut mem_delta = 0;
                for (key, val) in source {
                    if d.val_set.get(&key).is_some() {
                        continue;
                    }
                    mem_delta += d.val_set.insert(key, val);
                }
                Ok(mem_delta)
            }

            fn append_final_result_2_chunk(&self, sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
                append_distinct(&self.0, sctx, pr, chk)
            }
        }
    };
}

group_concat_distinct!(GroupOriginalConcatDistinct, merges: false);
group_concat_distinct!(GroupPartialConcatDistinct, merges: true);

/// Go `sortRow`.
#[derive(Clone, Debug)]
struct SortRow {
    buffer: Buffer,
    by_items: Vec<Datum>,
}

/// Go `topNRows`: the rows kept so far, a heap whose top is the row the
/// ORDER BY puts last, bounded by `limit_size` bytes of output.
#[derive(Clone, Debug)]
pub struct TopNRows {
    rows: Vec<SortRow>,
    desc: Vec<bool>,
    curr_size: u64,
    limit_size: u64,
    sep_size: u64,
    /// Go `isSepTruncated`: a popped row leaves part of its separator.
    is_sep_truncated: bool,
    collators: Vec<Collation>,
}

impl TopNRows {
    fn new(desc: Vec<bool>, limit_size: u64, sep_size: u64, collators: Vec<Collation>) -> Self {
        Self {
            rows: Vec::new(),
            desc,
            curr_size: 0,
            limit_size,
            sep_size,
            is_sep_truncated: false,
            collators,
        }
    }

    /// Go `topNRows.Less`: whether `a` sorts after `b`. A comparison error
    /// answers false, as Go's does.
    fn less(desc: &[bool], collators: &[Collation], a: &SortRow, b: &SortRow) -> bool {
        for k in 0..a.by_items.len() {
            let Ok(mut ret) = tidb_expr::compare_datums_with_collation(&a.by_items[k], &b.by_items[k], collators[k]) else {
                return false;
            };
            if desc[k] {
                ret = ret.reverse();
            }
            match ret {
                Ordering::Greater => return true,
                Ordering::Less => return false,
                Ordering::Equal => {}
            }
        }
        false
    }

    /// Go `heap.Push`.
    fn push(&mut self, row: SortRow) {
        self.rows.push(row);
        let (desc, collators) = (&self.desc, &self.collators);
        let last = self.rows.len() - 1;
        go_heap::up(&mut self.rows, last, &mut |a, b| Self::less(desc, collators, a, b));
    }

    /// Go `heap.Pop`.
    fn pop(&mut self) -> SortRow {
        let (desc, collators) = (&self.desc, &self.collators);
        go_heap::pop(&mut self.rows, &mut |a, b| Self::less(desc, collators, a, b)).expect("a non-empty heap")
    }

    /// Go `tryToAdd`: keeps `row`, then pops (or trims) the rows past the
    /// byte limit.
    fn try_to_add(&mut self, row: SortRow) -> (bool, i64) {
        self.curr_size += row.buffer.len() as u64;
        if !self.rows.is_empty() {
            self.curr_size += self.sep_size;
        }
        let mut mem_delta = row.buffer.cap() as i64 + row.by_items.iter().map(get_datum_mem_size).sum::<i64>();
        self.push(row);
        if self.curr_size <= self.limit_size {
            return (false, mem_delta);
        }
        while self.curr_size > self.limit_size {
            let debt = self.curr_size - self.limit_size;
            let mut heap_pop_row = self.pop();
            if heap_pop_row.buffer.len() as u64 > debt {
                self.curr_size -= debt;
                let keep = heap_pop_row.buffer.len() - debt as usize;
                heap_pop_row.buffer.truncate(keep);
                self.push(heap_pop_row);
            } else {
                self.curr_size -= heap_pop_row.buffer.len() as u64 + self.sep_size;
                mem_delta -= heap_pop_row.buffer.cap() as i64;
                mem_delta -= heap_pop_row.by_items.iter().map(get_datum_mem_size).sum::<i64>();
                self.is_sep_truncated = true;
            }
        }
        (true, mem_delta)
    }

    /// Go `reset`.
    fn reset(&mut self) {
        self.rows.clear();
        self.curr_size = 0;
    }

    /// Go `concat`: the rows in ORDER BY order, joined by `sep`.
    fn concat(&mut self, sep: &[u8]) -> Vec<u8> {
        let mut buffer = Buffer::new();
        let (desc, collators) = (&self.desc, &self.collators);
        // Go `sort.Sort(sort.Reverse(h))`.
        go_pdqsort::go_sort_func_by(&mut self.rows, |a, b| {
            if Self::less(desc, collators, b, a) { Ordering::Less } else { Ordering::Greater }
        });
        for (i, row) in self.rows.iter().enumerate() {
            if i != 0 {
                buffer.write(sep);
            }
            buffer.write(row.buffer.bytes());
        }
        if self.is_sep_truncated {
            buffer.write(sep);
            if buffer.len() as u64 > self.limit_size {
                buffer.truncate(self.limit_size as usize);
            }
        }
        buffer.bytes().to_vec()
    }
}

/// Go `groupConcatOrder` and `groupConcatDistinctOrder`: the ORDER BY
/// collators and directions.
#[derive(Clone, Debug)]
pub struct GroupConcatOrderBase {
    /// Go's embedded `baseGroupConcat4String`.
    pub base: BaseGroupConcat4String,
    /// Go `ctors`.
    pub ctors: Vec<Collation>,
    /// Go `desc`.
    pub desc: Vec<bool>,
}

impl GroupConcatOrderBase {
    fn top_n(&self) -> TopNRows {
        TopNRows::new(self.desc.clone(), self.base.max_len, self.base.sep.len() as u64, self.ctors.clone())
    }

    /// The row's ORDER BY keys (Go `byItem.Expr.Eval(...)` then `Clone`).
    fn sort_row(&self, sctx: &AggFuncUpdateContext, row: Row<'_>, buffer: Buffer) -> Result<SortRow, ExecError> {
        let by_items = self
            .base
            .by_items
            .iter()
            .map(|by_item| by_item.expr.eval(sctx, row).map_err(ExecError::Eval))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SortRow { buffer, by_items })
    }

    /// Adds `sort_row`, raising Go's truncation error past the limit.
    fn add(&self, sctx: &AggFuncUpdateContext, top_n: &mut TopNRows, sort_row: SortRow, mem_delta: &mut i64) -> Result<(), ExecError> {
        let (truncated, sort_row_mem_size) = top_n.try_to_add(sort_row);
        *mem_delta += sort_row_mem_size;
        if truncated {
            self.base.handle_truncate_error(sctx)?;
        }
        Ok(())
    }

    fn append(&self, top_n: &mut TopNRows, chk: &mut Chunk) {
        if top_n.rows.is_empty() {
            chk.append_null(self.base.base.ordinal);
            return;
        }
        chk.append_bytes(self.base.base.ordinal, &top_n.concat(&self.base.sep));
    }
}

/// Go `partialResult4GroupConcatOrder`.
pub struct PartialResult4GroupConcatOrder {
    top_n: TopNRows,
}

/// Go `groupConcatOrder`.
#[derive(Clone, Debug)]
pub struct GroupConcatOrder(pub GroupConcatOrderBase);

impl AggFunc for GroupConcatOrder {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        (Box::new(PartialResult4GroupConcatOrder { top_n: self.0.top_n() }), DEF_PARTIAL_RESULT_4_GROUP_CONCAT_ORDER_SIZE + DEF_TOP_N_ROWS_SIZE)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        partial::<PartialResult4GroupConcatOrder>(pr).top_n.reset();
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4GroupConcatOrder>(pr);
        let mut mem_delta = 0;
        for row in rows_in_group {
            let mut buffer = Buffer::new();
            let mut is_null = false;
            for arg in &self.0.base.base.args {
                let Some(v) = BaseGroupConcat4String::eval_string(arg, sctx, *row)? else {
                    is_null = true;
                    break;
                };
                buffer.write(&v);
            }
            if is_null {
                continue;
            }
            let sort_row = self.0.sort_row(sctx, *row, buffer)?;
            self.0.add(sctx, &mut p.top_n, sort_row, &mut mem_delta)?;
        }
        Ok(mem_delta)
    }

    fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, _src: &mut PartialResult, _dst: &mut PartialResult) -> Result<i64, ExecError> {
        // Go: ORDER BY forbids the parallel hash aggregation, so nothing
        // merges.
        Err(ExecError::internal("groupConcatOrder.MergePartialResult should not be called"))
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        self.0.append(&mut partial::<PartialResult4GroupConcatOrder>(pr).top_n, chk);
        Ok(())
    }
}

/// Go `partialResult4GroupConcatOrderDistinct`.
pub struct PartialResult4GroupConcatOrderDistinct {
    top_n: TopNRows,
    val_set: StringSetWithMemoryUsage,
    encode_bytes_buffer: EncodeBytesBuffer,
}

/// Go `groupConcatDistinctOrder`.
#[derive(Clone, Debug)]
pub struct GroupConcatDistinctOrder(pub GroupConcatOrderBase);

impl AggFunc for GroupConcatDistinctOrder {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        let (val_set, set_size) = StringSetWithMemoryUsage::new(std::iter::empty::<GoString>());
        let p = PartialResult4GroupConcatOrderDistinct {
            top_n: self.0.top_n(),
            val_set,
            encode_bytes_buffer: EncodeBytesBuffer::default(),
        };
        (Box::new(p), DEF_PARTIAL_RESULT_4_GROUP_CONCAT_ORDER_DISTINCT_SIZE + DEF_TOP_N_ROWS_SIZE + set_size)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        let p = partial::<PartialResult4GroupConcatOrderDistinct>(pr);
        p.top_n.reset();
        p.val_set = StringSetWithMemoryUsage::new(std::iter::empty::<GoString>()).0;
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4GroupConcatOrderDistinct>(pr);
        let mut mem_delta = -p.encode_bytes_buffer.cap();
        let collators = arg_collators(&self.0.base.base.args);
        let result = (|| {
            for row in rows_in_group {
                let mut buffer = Buffer::new();
                p.encode_bytes_buffer.reset();
                let mut is_null = false;
                for (i, arg) in self.0.base.base.args.iter().enumerate() {
                    let Some(v) = BaseGroupConcat4String::eval_string(arg, sctx, *row)? else {
                        is_null = true;
                        break;
                    };
                    p.encode_bytes_buffer.encode_bytes(&collators[i].immutable_key(&v));
                    buffer.write(&v);
                }
                if is_null {
                    continue;
                }
                let joined_val = GoString::from(p.encode_bytes_buffer.buf.clone());
                if p.val_set.contains(&joined_val) {
                    continue;
                }
                let joined_len = joined_val.len() as i64;
                mem_delta += p.val_set.insert(joined_val);
                mem_delta += joined_len;
                let sort_row = self.0.sort_row(sctx, *row, buffer)?;
                self.0.add(sctx, &mut p.top_n, sort_row, &mut mem_delta)?;
            }
            Ok(())
        })();
        mem_delta += p.encode_bytes_buffer.cap();
        result.map(|()| mem_delta)
    }

    fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, _src: &mut PartialResult, _dst: &mut PartialResult) -> Result<i64, ExecError> {
        Err(ExecError::internal("groupConcatDistinctOrder.MergePartialResult should not be called"))
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        self.0.append(&mut partial::<PartialResult4GroupConcatOrderDistinct>(pr).top_n, chk);
        Ok(())
    }
}
