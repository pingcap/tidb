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


//! Go `pkg/executor/aggregate/agg_util.go`: `GetGroupKey`, the group keys'
//! Go memory (`getGroupKeyMemUsage`), and `AfFinalResult`.
//!
//! The partial workers charge their key buffers by Go's capacities, so each
//! key's capacity follows Go's appends: a key starts at `10 *
//! len(groupByItems)` bytes, each `codec.HashGroupKey` piece grows it as
//! Go's `append` does, and a compact string reserves exactly
//! (`reallocBytes`).

use tidb_chunk::chunk::Chunk;
use tidb_codec::NIL_FLAG;
use tidb_datatype::EvalType;
use tidb_driver_error::ExecError;
use tidb_expr::expression::Expression;
use tidb_exprctx::Columns;
use tidb_goruntime::go_runtime::{go_64_next_slice_capacity_for_element, GoSliceElementLayout};

/// Go `defaultGroupKeyCap`: a pooled key buffer's capacity.
pub(crate) const DEFAULT_GROUP_KEY_CAP: usize = 8;
/// Go `aggfuncs.DefSliceSize`.
const DEF_SLICE_SIZE: i64 = 24;

/// Go `AfFinalResult`: a final worker's chunk, or its error.
pub(crate) struct AfFinalResult {
    pub(crate) chk: Option<Chunk>,
    pub(crate) err: Option<ExecError>,
    /// Go `giveBackCh`: where the main thread returns `chk`.
    pub(crate) give_back_ch: Option<crossbeam_channel::Sender<Chunk>>,
}

/// Go's deferred `recover()` and `recoveryHashAgg`: a worker's panic (a
/// tracker's kill among them) becomes an error on `output`, and the worker's
/// remaining deferred work still runs.
pub(crate) fn recover_hash_agg(output: &crossbeam_channel::Sender<AfFinalResult>, body: impl FnOnce()) {
    if let Err(error) = tidb_driver_error::panic_recovery::recover_worker_panic(|| {
        body();
        Ok(())
    }) {
        tracing::warn!(?error, "parallel hash aggregation panicked");
        let _ = output.send(AfFinalResult { chk: None, err: Some(error), give_back_ch: None });
    }
}

/// Go's `[][]byte` group keys, with the capacities Go's appends leave.
#[derive(Clone, Debug)]
pub(crate) struct GroupKeys {
    keys: Vec<Vec<u8>>,
    key_caps: Vec<usize>,
    /// Go `len(groupKey)`.
    len: usize,
    /// Go `cap(groupKey)`.
    cap: usize,
}

impl GroupKeys {
    /// Go `getBuffer`'s `make([][]byte, 0, defaultGroupKeyCap)`.
    pub(crate) fn new() -> Self {
        Self { keys: Vec::new(), key_caps: Vec::new(), len: 0, cap: DEFAULT_GROUP_KEY_CAP }
    }

    /// The `i`th key.
    pub(crate) fn key(&self, i: usize) -> &[u8] {
        &self.keys[i]
    }

    /// Go `len(groupKey)`.
    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

/// Go `getGroupKeyMemUsage`.
pub(crate) fn get_group_key_mem_usage(group_key: &GroupKeys) -> i64 {
    let keys: usize = group_key.key_caps[..group_key.len].iter().sum();
    keys as i64 + DEF_SLICE_SIZE * group_key.cap as i64
}

/// Go `append(b, bytes...)`.
fn go_append(key: &mut Vec<u8>, cap: &mut usize, bytes: &[u8]) {
    let new_len = key.len() + bytes.len();
    if new_len > *cap {
        *cap = go_64_next_slice_capacity_for_element(new_len, *cap, 1, GoSliceElementLayout::NoPointers);
    }
    key.extend_from_slice(bytes);
}

/// Go `codec.reallocBytes(b, n)`: room for `n` more bytes, exactly.
fn go_realloc(key: &[u8], cap: &mut usize, n: usize) {
    let new_size = key.len() + n;
    if *cap < new_size {
        *cap = new_size;
    }
}

/// Appends one item's `HashGroupKey` bytes in Go's append pieces.
fn append_hash_group_key_pieces(key: &mut Vec<u8>, cap: &mut usize, eval_type: EvalType, encoded: &[u8]) -> Result<(), ExecError> {
    let (flag, rest) = encoded.split_first().ok_or_else(|| ExecError::internal("an empty group key"))?;
    go_append(key, cap, std::slice::from_ref(flag));
    if *flag == NIL_FLAG {
        return Ok(());
    }
    match eval_type {
        // `EncodeDecimal`: precision and frac, then `WriteBin`'s bytes.
        EvalType::Decimal => {
            let (head, bin) = rest.split_at(2.min(rest.len()));
            go_append(key, cap, head);
            go_append(key, cap, bin);
        }
        // `EncodeCompactBytes`: `reallocBytes(b, MaxVarintLen64+len(data))`,
        // then the varint length and the data.
        EvalType::String => {
            let (data_after, len) = tidb_codec::decode_varint(rest).map_err(|error| ExecError::internal(error.to_string()))?;
            let varint_len = rest.len() - data_after.len();
            go_realloc(key, cap, 10 + len as usize);
            go_append(key, cap, &rest[..varint_len]);
            go_append(key, cap, data_after);
        }
        // `EncodeVarint` / `EncodeFloat` / `EncodeInt` / `EncodeMySQLTime`
        // append once; a JSON document's `HashValue` is appended as one
        // piece.
        _ => go_append(key, cap, rest),
    }
    Ok(())
}

/// Go `GetGroupKey`: each row's key over `group_by_items`, evaluated and
/// encoded one item (one column) at a time.
pub(crate) fn get_group_key(ctx: &dyn Columns, input: &Chunk, group_key: &mut GroupKeys, group_by_items: &[Expression]) -> Result<(), ExecError> {
    let num_rows = input.num_rows();
    let avl_group_key_len = group_key.len.min(num_rows);
    for i in 0..avl_group_key_len {
        group_key.keys[i].clear();
    }
    for i in avl_group_key_len..num_rows {
        // Go `append(groupKey, make([]byte, 0, 10*len(groupByItems)))`.
        if group_key.len + 1 > group_key.cap {
            group_key.cap = go_64_next_slice_capacity_for_element(group_key.len + 1, group_key.cap, 24, GoSliceElementLayout::PointerBearing);
        }
        let key_cap = 10 * group_by_items.len();
        if i < group_key.keys.len() {
            group_key.keys[i] = Vec::with_capacity(key_cap);
            group_key.key_caps[i] = key_cap;
        } else {
            group_key.keys.push(Vec::with_capacity(key_cap));
            group_key.key_caps.push(key_cap);
        }
        group_key.len += 1;
    }
    group_key.len = num_rows;

    let timezone = ctx.time_zone();
    let mut encoded = Vec::new();
    for item in group_by_items {
        let field_type = item.static_type().ok_or_else(|| ExecError::internal("HashAgg group expression has no field type"))?;
        let key_type = crate::group_key::group_key_type(field_type);
        let eval_type = key_type.eval_type();
        for row in 0..num_rows {
            let value = item.eval(ctx, input.get_row(row)).map_err(ExecError::Eval)?;
            encoded.clear();
            tidb_codec::append_hash_group_key_in_timezone(&timezone, &value, &key_type, &mut encoded)
                .map_err(|error| ExecError::internal(error.to_string()))?;
            let (key, cap) = (&mut group_key.keys[row], &mut group_key.key_caps[row]);
            append_hash_group_key_pieces(key, cap, eval_type, &encoded)?;
        }
    }
    Ok(())
}


/// Go's `sync.WaitGroup`.
#[derive(Default)]
pub(crate) struct WaitGroup {
    count: std::sync::Mutex<usize>,
    done: std::sync::Condvar,
}

impl WaitGroup {
    pub(crate) fn new(count: usize) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self { count: std::sync::Mutex::new(count), done: std::sync::Condvar::new() })
    }

    /// Go `Add(1)`.
    pub(crate) fn add(&self) {
        *self.count.lock().unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
    }

    /// Go `Done`.
    pub(crate) fn done(&self) {
        let mut count = self.count.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        *count -= 1;
        if *count == 0 {
            self.done.notify_all();
        }
    }

    /// Go `Wait`.
    pub(crate) fn wait(&self) {
        let mut count = self.count.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        while *count > 0 {
            count = self.done.wait(count).unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}
