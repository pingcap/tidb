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

//! Go `pkg/executor/point_get.go`: `PointGetExecutor`, one row read by its
//! handle, or by the handle a unique index entry names, with the lock a
//! locking read takes and the inconsistency a missing row reports; and the
//! row decoding `DecodeRowValToChunk` / `fillRowChecksum` share with
//! `BatchPointGetExec`.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_expr::schema::Schema;
use tidb_table::tables::{TableCommon, TableHandle};

use crate::access_path::{HandleOutputColumn, PointLock};

/// Go `isCommonHandleRead`: a clustered PRIMARY has metadata but no separate
/// index entries; its values encode the record handle itself.
pub(crate) fn is_common_handle_read(table: &TableCommon, index_id: i64) -> bool {
    !table.common_handle_offsets().is_empty()
        && table.plan_indexes().any(|index| index.read().id == index_id && index.clustered_primary)
}

/// Go `matchPartitionNames(pid, partitionNames, pi)` for a global index
/// entry; a local entry carries no partition and is already confined.
pub(crate) fn match_partition_names(allowed: Option<&[i64]>, partition: Option<i64>) -> bool {
    match (allowed, partition) {
        (Some(allowed), Some(id)) => allowed.contains(&id),
        _ => true,
    }
}

/// Go `DecodeRowValToChunk` + `fillRowChecksum` for one row: the physical
/// plan's output columns of `row`, the row `handle` names on `physical_id`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn append_point_row(
    table: &TableCommon,
    output_columns: &[HandleOutputColumn],
    physical_id: Option<i64>,
    decode_context: &tidb_table::tables::RowDecodeContext,
    req: &mut Chunk,
    handle: &TableHandle,
    row: &[Datum],
    stored_value: &[u8],
) -> Result<(), ExecError> {
    for (output, source) in output_columns.iter().copied().enumerate() {
        match source {
            HandleOutputColumn::Stored(source) => {
                // The physical schema may request a hidden generated column
                // for an UPDATE/index key. Stored offsets address the full
                // decoded row, not the wildcard's visible-column prefix.
                let value = row
                    .get(source)
                    .ok_or_else(|| ExecError::unsupported("point-get output column is outside the row"))?;
                req.append_datum(output, value);
            }
            HandleOutputColumn::ExtraHandle => match handle {
                TableHandle::Int(value) => req.append_datum(output, &Datum::Int(*value)),
                _ => return Err(ExecError::unsupported("an extra row handle is not an integer handle")),
            },
            HandleOutputColumn::PhysicalTableId => {
                let physical_id = physical_id
                    .or_else(|| match table.record_physical_ids().as_slice() {
                        [id] => Some(*id),
                        _ => None,
                    })
                    .ok_or_else(|| ExecError::internal("physical-table ID output lost its record partition"))?;
                req.append_int64(output, physical_id);
            }
            HandleOutputColumn::RowChecksum => {
                let checksum = tidb_table::tables::fill_row_checksum(table, handle, stored_value, decode_context)
                    .map_err(ExecError::from)?;
                req.append_datum(output, &checksum);
            }
        }
    }
    Ok(())
}

/// Go `PointGetExecutor`.
pub struct PointGetExecutor {
    meta: ExecutorMeta,
    /// Go `tblInfo` with its columns and indexes.
    table: TableCommon,
    /// The snapshot this reader reads (Go's `txn` / `snapshot`).
    store: Box<dyn tidb_kv::storage::TableStorage>,
    /// Go `handle`, set by a handle plan.
    handle: Option<TableHandle>,
    /// Go `idxInfo`'s id and `idxVals`, set by a unique-index plan.
    idx: Option<(i64, Vec<Datum>)>,
    /// Go `partitionNames` resolved to physical ids.
    partition_names: Option<Vec<i64>>,
    /// Go `lock` with its `lockWaitTime`.
    lock: Option<PointLock>,
    /// Go `!StmtCtx.WeakConsistency`: the reporter a missing row answers.
    consistency_report: Option<crate::admin_check::ReportContext>,
    /// The plan's output columns (Go `Schema()` / `columns`).
    output_columns: Vec<HandleOutputColumn>,
    /// The statement's row-decode flags and time zone (Go `rowDecoder`'s).
    decode_context: tidb_table::tables::RowDecodeContext,
    /// Go `done`.
    done: bool,
    produced: tidb_executor_internal_exec::RowCount,
}

impl PointGetExecutor {
    /// Go `buildPointGet` over a handle plan.
    #[must_use]
    pub(crate) fn new_by_handle(
        meta: ExecutorMeta,
        table: TableCommon,
        store: Box<dyn tidb_kv::storage::TableStorage>,
        handle: TableHandle,
        output_columns: Vec<HandleOutputColumn>,
        decode_context: tidb_table::tables::RowDecodeContext,
    ) -> Self {
        Self::new(meta, table, store, Some(handle), None, output_columns, decode_context)
    }

    /// Go `buildPointGet` over a unique-index plan.
    #[must_use]
    pub(crate) fn new_by_unique_index(
        meta: ExecutorMeta,
        table: TableCommon,
        store: Box<dyn tidb_kv::storage::TableStorage>,
        index_id: i64,
        index_values: Vec<Datum>,
        output_columns: Vec<HandleOutputColumn>,
        decode_context: tidb_table::tables::RowDecodeContext,
    ) -> Self {
        Self::new(meta, table, store, None, Some((index_id, index_values)), output_columns, decode_context)
    }

    fn new(
        meta: ExecutorMeta,
        table: TableCommon,
        store: Box<dyn tidb_kv::storage::TableStorage>,
        handle: Option<TableHandle>,
        idx: Option<(i64, Vec<Datum>)>,
        output_columns: Vec<HandleOutputColumn>,
        decode_context: tidb_table::tables::RowDecodeContext,
    ) -> Self {
        Self {
            meta,
            table,
            store,
            handle,
            idx,
            partition_names: None,
            lock: None,
            consistency_report: None,
            output_columns,
            decode_context,
            done: false,
            produced: tidb_executor_internal_exec::RowCount::default(),
        }
    }

    /// Go `partitionNames`: a global index entry whose row lives elsewhere
    /// answers nothing.
    #[must_use]
    pub(crate) fn with_partition_names(mut self, ids: Option<Vec<i64>>) -> Self {
        self.partition_names = ids;
        self
    }

    /// Go `lock`: the index and record keys this read names are locked
    /// inside `Next`.
    #[must_use]
    pub(crate) fn with_lock(mut self, lock: Option<PointLock>) -> Self {
        self.lock = lock;
        self
    }

    /// Go `!StmtCtx.WeakConsistency`: the reporter a missing row answers.
    #[must_use]
    pub(crate) fn with_consistency_report(mut self, report: Option<crate::admin_check::ReportContext>) -> Self {
        self.consistency_report = report;
        self
    }

    /// The live count of rows this executor produced.
    #[must_use]
    pub fn produced_rows(&self) -> tidb_executor_internal_exec::RowCount {
        self.produced.clone()
    }

    /// Go `Next`'s unique-index half: the handle the entry names, with the
    /// index key it was read under (none for a clustered PRIMARY), or no row.
    fn handle_from_index(
        &mut self,
        index_id: i64,
        index_values: &[Datum],
    ) -> Result<Option<(TableHandle, Option<Vec<u8>>)>, ExecError> {
        let zone = self.decode_context.zone().clone();
        if is_common_handle_read(&self.table, index_id) {
            // Go `EncodeUniqueIndexValuesForKey`: a NULL value names no row.
            if index_values.iter().any(Datum::is_null) {
                return Ok(None);
            }
            let handle = self
                .table
                .common_handle_from_values(index_values, &zone)
                .map_err(|error| ExecError::unsupported(format!("common primary key encoding failed: {error:?}")))?;
            return Ok(Some((handle, None)));
        }
        // Go `EncodeUniqueIndexKey`.
        let idx_keys = match self.table.encode_unique_index_key(index_id, index_values, &zone) {
            Ok(keys) => Some(keys),
            Err(error) if self.lock.is_some() => {
                return Err(ExecError::unsupported(format!("unique index key encoding failed: {error:?}")));
            }
            Err(_) => None,
        };
        // Go: repeatable read locks the index key whether or not it exists
        // (`lockKeyIfNeeded`) and reads it from the lock cache; read
        // committed locks it only if it exists (`lockKeyIfExists`).
        if let (Some(lock), Some(keys)) = (self.lock.clone(), idx_keys.as_ref()) {
            lock.lock(&mut *self.store, keys, lock.read_committed)?;
        }
        let idx_key = idx_keys.and_then(|keys| keys.into_iter().next()).map(|key| key.as_bytes().to_vec());
        let found = self
            .table
            .lookup_unique_with_partition(&mut *self.store, index_id, index_values, &zone)
            .map_err(|error| ExecError::unsupported(format!("unique index lookup failed: {error:?}")))?;
        Ok(found
            .filter(|(_, partition)| match_partition_names(self.partition_names.as_deref(), *partition))
            .map(|(handle, _)| (handle, idx_key)))
    }

    /// Go `ReportLookupInconsistent(ctx, 1, 0, []Handle{e.handle},
    /// []Handle{e.handle}, []RecordData{{}})`.
    fn report_missing_row(
        &self,
        report: &crate::admin_check::ReportContext,
        index_id: i64,
        handle: &TableHandle,
        record_key: Option<Vec<u8>>,
        idx_key: Option<Vec<u8>>,
    ) -> ExecError {
        let kv_handle = match crate::admin_check::kv_handle_of(handle) {
            Ok(handle) => handle,
            Err(error) => return ExecError::unsupported(format!("{error:?}")),
        };
        let Some(index) = self.table.plan_indexes().find(|index| index.read().id == index_id) else {
            return ExecError::internal("a unique-index point get lost its index");
        };
        let tbl = self.table.meta().read().clone();
        let idx = index.read().clone();
        let handle_encode = |_: &tidb_txnkv::Handle| record_key.clone();
        let index_encode = |_: &tidb_logutil_consistency::RecordData| idx_key.clone();
        let reporter = tidb_logutil_consistency::Reporter {
            handle_encode: &handle_encode,
            index_encode: Some(&index_encode),
            tbl: &tbl,
            idx: &idx,
            enable_redact_log: report.enable_redact_log.clone(),
            storage: report.storage.clone(),
        };
        let empty = tidb_logutil_consistency::RecordData { handle: kv_handle.clone(), values: Vec::new() };
        let handles = [kv_handle];
        ExecError::from(reporter.report_lookup_inconsistent(1, 0, &handles, &handles, &[empty]))
    }
}

impl Executor for PointGetExecutor {
    fn open(&mut self) -> Result<(), ExecError> {
        self.done = false;
        self.produced.set(0);
        Ok(())
    }

    /// Go `PointGetExecutor.Next`.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if std::mem::replace(&mut self.done, true) {
            return Ok(());
        }
        let mut idx_key = None;
        let handle = match self.idx.clone() {
            None => self.handle.clone().ok_or_else(|| ExecError::unsupported("a point get lost its retained handle"))?,
            Some((index_id, index_values)) => match self.handle_from_index(index_id, &index_values)? {
                Some((handle, key)) => {
                    idx_key = key;
                    handle
                }
                None => return Ok(()),
            },
        };
        // Go `getAndLock`: "Lock the key before get in RR isolation, then get
        // will get the value from the cache"; read committed locks the key
        // only if it exists (`lockKeyIfExists`).
        let record_keys = self.table.record_lock_keys(std::slice::from_ref(&handle), None);
        if let (Some(lock), Some(keys)) = (self.lock.clone(), record_keys.as_ref()) {
            lock.lock(&mut *self.store, keys, lock.read_committed)?;
        }
        let value = self
            .table
            .stored_record(&mut *self.store, &handle)
            .map_err(ExecError::from)?
            .map(|(_, bytes)| bytes);
        let Some(value) = value else {
            // Go: a unique index entry that is not a clustered PRIMARY named
            // a row the table does not hold.
            if let (Some((index_id, _)), Some(report)) = (&self.idx, &self.consistency_report) {
                if !is_common_handle_read(&self.table, *index_id) {
                    let record_key = record_keys
                        .or_else(|| self.table.record_lock_keys(std::slice::from_ref(&handle), None))
                        .and_then(|keys| keys.into_iter().next())
                        .map(|key| key.as_bytes().to_vec());
                    return Err(self.report_missing_row(report, *index_id, &handle, record_key, idx_key));
                }
            }
            return Ok(());
        };
        self.table.note_keys_read(1).map_err(ExecError::from)?;
        let row = tidb_table::tables::PointRowDecoder::new(&self.table)
            .decode(&self.table, &handle, &value, &self.decode_context)
            .map_err(ExecError::from)?;
        append_point_row(&self.table, &self.output_columns, None, &self.decode_context, req, &handle, &row, &value)?;
        self.produced.set(self.produced.get() + 1);
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.done = true;
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

    fn table_access(&mut self) -> Option<&mut dyn tidb_executor_internal_exec::table_access::TableAccess> {
        Some(self)
    }
}

/// A point get refuses every offer by taking the fail-closed defaults: its
/// key is fixed by the statement text and nothing is scanned.
impl tidb_executor_internal_exec::table_access::TableAccess for PointGetExecutor {}
