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

//! `pkg/executor/batch_point_get.go`: `BatchPointGetExec` -- the reader for
//! `WHERE pk IN (...)` and `WHERE uk IN (...)`, which resolves a fixed handle
//! list and batch-gets the rows.
//!
//! # What is here
//!
//! `initialize` (:252) is the whole operator, and it is two things braided
//! together: a KV round trip, and a small amount of ordering/identity algebra
//! that decides which rows come out and in what order. The algebra is ported:
//!
//! * [`sort_handles_for_keep_order`] -- Go :377-409, the `keepOrder` sort,
//!   including the UNSIGNED comparator that a `PKIsHandle` column with
//!   `mysql.UnsignedFlag` selects. This is the part that is easy to get wrong
//!   and impossible to notice until a primary key crosses `MaxInt64`.
//! * [`resolve_physical_ids`] -- Go :391-427, which physical table each handle
//!   is read from (`singlePartID`, then `planPhysIDs`, then the table itself)
//!   and the `tID <= 0` skip that drops a handle matching NO partition.
//! * [`BatchPointGetExec`] -- Go's `Open`/`Next`/`Close` (:99/:214/:182)
//!   shape: `inited` is a one-shot latch, `index` walks the resolved rows, and
//!   an exhausted cursor returns an empty chunk.
//!
//! # Reuse rather than restatement
//!
//! [`crate::access_path::HandleSourceExec`] is already Go's "read rows for a
//! known handle list", partition ids included
//! (`new_partitioned_projected_with_context`). It also already implements the
//! `IsValueEmpty` -> `continue` rule of :446 -- a handle whose row is absent
//! produces no output row rather than a NULL row. So this operator prepares
//! the handle list and delegates the read, which is exactly the Go split
//! between `initialize` and `Next`.
//!
//! Go duplicates `buildVirtualColumnInfo` inline at :88, a copy of
//! `table_reader.go`'s. This port does NOT duplicate it: a caller computes it
//! once with [`crate::table_reader::build_virtual_column_info`] and passes the
//! result to [`BatchPointGetExec::with_virtual_column_index`].
//!
//! # boundary: the KV round trip and the lock
//!
//! None of the following is ported, and each names its exact Go symbol:
//!
//! * `kv.BatchGetter`/`snapshot.BatchGet` and its three flavors --
//!   `driver.NewBufferBatchGetter`, `cacheBatchGetter` (:557) with
//!   `newCacheBatchGetter` (:587), and `cacheTableSnapshot` (:126). Choosing
//!   between them is `Open` (:99) reading `txn.Valid()`, `tblInfo.Lock` and
//!   `EnablePointGetCache`; this tier's storage seam
//!   ([`crate::storage::TableStorage`]) already merges the staged buffer under
//!   the same read, so the buffered getter has no separate existence here.
//! * `LockKeys` (:503) and `PessimisticLockCacheGetter` (:536): the
//!   Repeatable-Read "lock every key, present or not" pass (:432) versus the
//!   Read-Committed "lock only existing keys" pass (:487). Both need
//!   `sessionctx`'s pessimistic-lock context.
//! * `consistency.Reporter.ReportLookupInconsistent` (:456), which turns an
//!   index entry with no row into an admin-check error rather than a silently
//!   missing row. It fires only on the INDEX path and only when the statement
//!   is not `WeakConsistency`.
//! * `DecodeRowValToChunk`/`rowcodec.ChunkDecoder` and `fillRowChecksum`
//!   (:238): reached through
//!   [`crate::kv_table::RowDecodeContext`] instead.
//! * `table.FillVirtualColumnValue` (:245), the same boundary
//!   [`crate::table_reader`] records.
//! * `indexUsageReporter`, `runtimeStatsWithSnapshot`, `UpdateDeltaForTableID`
//!   and `setOptionForTopSQL` have no counterpart in this tier.
//!
//! # Narrowing
//!
//! The INDEX arm of `initialize` (:252-362) -- decode `idxVals` into index
//! keys, batch-get them, and turn the index values back into handles -- is not
//! here: it is the index double read, and the handle it yields is what this
//! operator then consumes. A caller on that path supplies the already-resolved
//! handles. Go's own `TODO` at :407 ("if partitioned table, sorting the
//! handles would also need to have the physIDs rearranged in the same order")
//! is a real bug in Go; [`sort_handles_for_keep_order`] sorts the handle and
//! its physical id TOGETHER, which is the behavior Go's `intest.Assert` on the
//! next line asserts it never has to face.

use std::cmp::Ordering;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::schema::Schema;

use crate::access_path::HandleSourceExec;
use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::kv_table::{KvTable, RowDecodeContext, TableHandle};

/// Go `slices.SortFunc(e.handles, less)` (:406) with the two comparators of
/// :377-405.
///
/// `unsigned_pk_is_handle` is Go's
/// `tblInfo.PKIsHandle && mysql.HasUnsignedFlag(tblInfo.GetPkColInfo().GetFlag())`:
/// the handle bits are the same, but `18446744073709551615` must sort ABOVE
/// `1`, not below it as the signed `-1` it would otherwise read as.
///
/// `physical_ids`, when present, is permuted with the handles. Go leaves it
/// unpermuted with a `TODO` at :407 and guards the gap with
/// `intest.Assert(e.singlePartID != 0 || len(e.planPhysIDs) <= 1)`, i.e. it
/// only ever sorts when there is at most one physical id to permute. Keeping
/// them together is the same behavior for every input Go admits, and correct
/// for the ones it asserts away.
pub fn sort_handles_for_keep_order(
    handles: &mut Vec<TableHandle>,
    physical_ids: Option<&mut Vec<i64>>,
    desc: bool,
    unsigned_pk_is_handle: bool,
) {
    let compare = |a: &TableHandle, b: &TableHandle| -> Ordering {
        let ordering = if unsigned_pk_is_handle {
            match (a, b) {
                (TableHandle::Int(left), TableHandle::Int(right)) => {
                    // Go's `uintComparator` panics on a non-int handle; an
                    // unsigned `PKIsHandle` cannot produce one.
                    (*left as u64).cmp(&(*right as u64))
                }
                _ => a.cmp(b),
            }
        } else {
            a.cmp(b)
        };
        if desc {
            ordering.reverse()
        } else {
            ordering
        }
    };
    match physical_ids {
        Some(physical_ids) if physical_ids.len() == handles.len() => {
            let mut order: Vec<usize> = (0..handles.len()).collect();
            order.sort_by(|a, b| compare(&handles[*a], &handles[*b]));
            *handles = order.iter().map(|i| handles[*i].clone()).collect();
            *physical_ids = order.iter().map(|i| physical_ids[*i]).collect();
        }
        _ => handles.sort_by(compare),
    }
}

/// Go :391-427: pair every handle with the physical table its row key belongs
/// to, dropping the handles that match no partition.
///
/// The precedence is Go's, in Go's order:
///
/// 1. `singlePartID != 0` -- static pruning already settled on one partition,
///    so every handle reads from it;
/// 2. `len(planPhysIDs) > 0` -- a direct handle read on a partitioned table,
///    one id per handle;
/// 3. otherwise the table itself.
///
/// A resolved id of `<= 0` is Go's "not matching any partition" and the handle
/// is DROPPED, not read against the base table.
#[must_use]
pub fn resolve_physical_ids(
    handles: &[TableHandle],
    table_id: i64,
    single_part_id: i64,
    plan_phys_ids: &[i64],
) -> (Vec<TableHandle>, Vec<i64>) {
    let mut kept_handles = Vec::with_capacity(handles.len());
    let mut kept_ids = Vec::with_capacity(handles.len());
    for (offset, handle) in handles.iter().enumerate() {
        let id = if single_part_id != 0 {
            single_part_id
        } else if !plan_phys_ids.is_empty() {
            plan_phys_ids.get(offset).copied().unwrap_or(0)
        } else {
            table_id
        };
        if id <= 0 {
            continue;
        }
        kept_handles.push(handle.clone());
        kept_ids.push(id);
    }
    (kept_handles, kept_ids)
}

/// Go `BatchPointGetExec` (:49).
pub struct BatchPointGetExec {
    meta: ExecutorMeta,
    table: KvTable,
    decode_context: RowDecodeContext,
    /// Go `handles`.
    handles: Vec<TableHandle>,
    /// Go `planPhysIDs`.
    plan_phys_ids: Vec<i64>,
    /// Go `singlePartID`.
    single_part_id: i64,
    /// Go `keepOrder`.
    keep_order: bool,
    /// Go `desc`.
    desc: bool,
    /// Go `tblInfo.PKIsHandle && unsigned`.
    unsigned_pk_is_handle: bool,
    /// Go `virtualColumnIndex`.
    virtual_column_index: Vec<usize>,
    /// Go `inited`, which Go latches with a `CompareAndSwapUint32` because
    /// `Next` may be entered concurrently by a detached executor; a
    /// single-threaded pull needs only the flag.
    inited: bool,
    /// Go `values`/`index`, delegated to the source that owns the read.
    source: Option<HandleSourceExec>,
}

impl BatchPointGetExec {
    /// Builds the reader over an already-resolved handle list -- Go's state
    /// after the index arm of `initialize` (:252) has run, or directly from
    /// the plan for the `PKIsHandle` arm.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        table: KvTable,
        decode_context: RowDecodeContext,
        handles: Vec<TableHandle>,
    ) -> Self {
        BatchPointGetExec {
            meta,
            table,
            decode_context,
            handles,
            plan_phys_ids: Vec::new(),
            single_part_id: 0,
            keep_order: false,
            desc: false,
            unsigned_pk_is_handle: false,
            virtual_column_index: Vec::new(),
            inited: false,
            source: None,
        }
    }

    /// Go `keepOrder`/`desc` plus the unsigned-handle flag the comparator
    /// switches on.
    #[must_use]
    pub fn with_keep_order(mut self, desc: bool, unsigned_pk_is_handle: bool) -> Self {
        self.keep_order = true;
        self.desc = desc;
        self.unsigned_pk_is_handle = unsigned_pk_is_handle;
        self
    }

    /// Go `planPhysIDs`: one physical table id per handle.
    #[must_use]
    pub fn with_plan_phys_ids(mut self, plan_phys_ids: Vec<i64>) -> Self {
        self.plan_phys_ids = plan_phys_ids;
        self
    }

    /// Go `singlePartID`: a single partition settled by static pruning.
    #[must_use]
    pub fn with_single_part_id(mut self, single_part_id: i64) -> Self {
        self.single_part_id = single_part_id;
        self
    }

    /// Go `virtualColumnIndex` (from `buildVirtualColumnInfo` :88).
    #[must_use]
    pub fn with_virtual_column_index(mut self, virtual_column_index: Vec<usize>) -> Self {
        self.virtual_column_index = virtual_column_index;
        self
    }

    /// Go `initialize` (:252), minus the KV round trip: sort for `keepOrder`,
    /// resolve the physical ids, then hand the pair to the source that reads
    /// the rows.
    fn initialize(&mut self) -> Result<(), ExecError> {
        if !self.virtual_column_index.is_empty() {
            // boundary: `table.FillVirtualColumnValue` (:245).
            return Err(ExecError::unsupported(
                "batch point get cannot fill virtual columns yet (Go: table.FillVirtualColumnValue)",
            ));
        }
        let mut handles = std::mem::take(&mut self.handles);
        let mut plan_phys_ids = std::mem::take(&mut self.plan_phys_ids);
        if self.keep_order {
            let ids = if plan_phys_ids.len() == handles.len() {
                Some(&mut plan_phys_ids)
            } else {
                None
            };
            sort_handles_for_keep_order(&mut handles, ids, self.desc, self.unsigned_pk_is_handle);
        }
        let (handles, physical_ids) = resolve_physical_ids(
            &handles,
            self.table.table_id,
            self.single_part_id,
            &plan_phys_ids,
        );
        let meta = ExecutorMeta::new(
            self.meta.schema().clone(),
            self.meta.id(),
            self.meta.init_cap(),
            self.meta.max_chunk_size(),
        );
        let mut source = HandleSourceExec::new_partitioned_projected_with_context(
            meta,
            self.table.clone(),
            handles,
            physical_ids,
            None,
            self.decode_context.clone(),
        );
        source.open()?;
        self.source = Some(source);
        Ok(())
    }
}

impl Executor for BatchPointGetExec {
    /// Go `Open` (:99) picks the batch getter; the read seam already resolves
    /// that here, so opening only clears the latch.
    fn open(&mut self) -> Result<(), ExecError> {
        self.inited = false;
        self.source = None;
        Ok(())
    }

    /// Go `Next` (:214): initialize once, then walk the resolved rows.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if !self.inited {
            self.inited = true;
            self.initialize()?;
        }
        match self.source.as_mut() {
            Some(source) => source.next(req),
            None => Ok(()),
        }
    }

    /// Go `Close` (:182).
    fn close(&mut self) -> Result<(), ExecError> {
        if let Some(source) = self.source.as_mut() {
            source.close()?;
        }
        self.source = None;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handles(values: &[i64]) -> Vec<TableHandle> {
        values.iter().map(|v| TableHandle::Int(*v)).collect()
    }

    fn values(handles: &[TableHandle]) -> Vec<i64> {
        handles
            .iter()
            .map(|h| h.int_value().expect("int handle"))
            .collect()
    }

    /// WRITTEN test (Go covers this through `testkit`): the signed comparator.
    #[test]
    fn keep_order_sorts_signed_handles_ascending() {
        let mut list = handles(&[5, -3, 1]);
        sort_handles_for_keep_order(&mut list, None, false, false);
        assert_eq!(values(&list), vec![-3, 1, 5]);
    }

    #[test]
    fn keep_order_desc_reverses_the_comparator() {
        let mut list = handles(&[5, -3, 1]);
        sort_handles_for_keep_order(&mut list, None, true, false);
        assert_eq!(values(&list), vec![5, 1, -3]);
    }

    /// The whole point of Go's `uintComparator`: `-1` is `MaxUint64` and must
    /// sort LAST, not first.
    #[test]
    fn an_unsigned_handle_sorts_by_its_bits_not_its_sign() {
        let mut list = handles(&[-1, 1, i64::MIN]);
        sort_handles_for_keep_order(&mut list, None, false, true);
        // As u64: 1 < 9223372036854775808 (i64::MIN) < 18446744073709551615.
        assert_eq!(values(&list), vec![1, i64::MIN, -1]);
    }

    #[test]
    fn an_unsigned_handle_sorted_signed_would_be_wrong() {
        let mut signed = handles(&[-1, 1]);
        sort_handles_for_keep_order(&mut signed, None, false, false);
        assert_eq!(values(&signed), vec![-1, 1]);
    }

    #[test]
    fn a_common_handle_sorts_by_its_encoded_bytes() {
        let mut list = vec![
            TableHandle::Common(vec![2, 0]),
            TableHandle::Common(vec![1, 9]),
        ];
        sort_handles_for_keep_order(&mut list, None, false, false);
        assert_eq!(list[0], TableHandle::Common(vec![1, 9]));
    }

    /// Go's `TODO` at :407 leaves `planPhysIDs` unpermuted; this port keeps
    /// each handle with its own partition.
    #[test]
    fn keep_order_permutes_the_physical_ids_with_the_handles() {
        let mut list = handles(&[3, 1, 2]);
        let mut ids = vec![300i64, 100, 200];
        sort_handles_for_keep_order(&mut list, Some(&mut ids), false, false);
        assert_eq!(values(&list), vec![1, 2, 3]);
        assert_eq!(ids, vec![100, 200, 300]);
    }

    #[test]
    fn a_single_partition_id_wins_over_the_table() {
        let (kept, ids) = resolve_physical_ids(&handles(&[1, 2]), 7, 42, &[]);
        assert_eq!(values(&kept), vec![1, 2]);
        assert_eq!(ids, vec![42, 42]);
    }

    #[test]
    fn plan_phys_ids_pair_one_to_one_with_the_handles() {
        let (kept, ids) = resolve_physical_ids(&handles(&[1, 2, 3]), 7, 0, &[10, 20, 30]);
        assert_eq!(values(&kept), vec![1, 2, 3]);
        assert_eq!(ids, vec![10, 20, 30]);
    }

    #[test]
    fn a_handle_matching_no_partition_is_dropped() {
        let (kept, ids) = resolve_physical_ids(&handles(&[1, 2, 3]), 7, 0, &[10, 0, 30]);
        // Go: `if tID <= 0 { continue }` -- the handle never becomes a key.
        assert_eq!(values(&kept), vec![1, 3]);
        assert_eq!(ids, vec![10, 30]);
    }

    #[test]
    fn with_no_partitioning_every_handle_reads_from_the_table() {
        let (kept, ids) = resolve_physical_ids(&handles(&[1, 2]), 7, 0, &[]);
        assert_eq!(values(&kept), vec![1, 2]);
        assert_eq!(ids, vec![7, 7]);
    }
}
