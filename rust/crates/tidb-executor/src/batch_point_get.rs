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

//! Go `pkg/executor/batch_point_get.go`: `BatchPointGetExec`, the rows of a
//! handle list or of the handles a unique index's entries name, read with
//! one `BatchGet` on the first `Next` (`initialize`) and decoded a chunk at a
//! time.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_expr::schema::Schema;
use tidb_table::tables::{TableCommon, TableHandle};

use crate::access_path::{HandleOutputColumn, PointLock};
use crate::point_get::{append_point_row, is_common_handle_read, match_partition_names};

/// Go `BatchPointGetExec`.
pub struct BatchPointGetExec {
    meta: ExecutorMeta,
    /// Go `tblInfo` with its columns and indexes.
    table: TableCommon,
    /// The snapshot this reader reads (Go's `txn` / `snapshot`).
    store: Box<dyn tidb_kv::storage::TableStorage>,
    /// Go `handles`: a handle plan's list, or the handles `initialize`
    /// resolved from the index.
    handles: Vec<TableHandle>,
    /// Go `planPhysIDs`: one physical partition per handle, when routed.
    partition_ids: Option<Vec<i64>>,
    /// Go `idxInfo`'s id and `idxVals`, set by a unique-index plan.
    idx: Option<(i64, Vec<Vec<Datum>>)>,
    /// Go `partitionNames` resolved to physical ids.
    partition_names: Option<Vec<i64>>,
    /// Go `lock` with its `waitTime`.
    lock: Option<PointLock>,
    /// Go `!StmtCtx.WeakConsistency`: the reporter a missing row answers.
    consistency_report: Option<crate::admin_check::ReportContext>,
    /// The plan's output columns.
    output_columns: Vec<HandleOutputColumn>,
    decode_context: tidb_table::tables::RowDecodeContext,
    /// Go `values`: the rows read by `initialize`, aligned with `handles`.
    values: Option<tidb_table::tables::PointReadValues>,
    decoder: Option<tidb_table::tables::PointRowDecoder>,
    /// Go `index`: the next handle `Next` emits.
    index: usize,
    initialized: bool,
    produced: tidb_executor_internal_exec::RowCount,
}

impl BatchPointGetExec {
    /// Go `buildBatchPointGet` over a handle plan.
    #[must_use]
    pub fn new_by_handles(
        meta: ExecutorMeta,
        table: TableCommon,
        store: Box<dyn tidb_kv::storage::TableStorage>,
        handles: Vec<TableHandle>,
        output_columns: Vec<HandleOutputColumn>,
        decode_context: tidb_table::tables::RowDecodeContext,
    ) -> Self {
        Self::new(meta, table, store, handles, None, output_columns, decode_context)
    }

    /// Go `buildBatchPointGet` over a unique-index plan.
    #[must_use]
    pub(crate) fn new_by_unique_index(
        meta: ExecutorMeta,
        table: TableCommon,
        store: Box<dyn tidb_kv::storage::TableStorage>,
        index_id: i64,
        index_values: Vec<Vec<Datum>>,
        output_columns: Vec<HandleOutputColumn>,
        decode_context: tidb_table::tables::RowDecodeContext,
    ) -> Self {
        Self::new(meta, table, store, Vec::new(), Some((index_id, index_values)), output_columns, decode_context)
    }

    fn new(
        meta: ExecutorMeta,
        table: TableCommon,
        store: Box<dyn tidb_kv::storage::TableStorage>,
        handles: Vec<TableHandle>,
        idx: Option<(i64, Vec<Vec<Datum>>)>,
        output_columns: Vec<HandleOutputColumn>,
        decode_context: tidb_table::tables::RowDecodeContext,
    ) -> Self {
        Self {
            meta,
            table,
            store,
            handles,
            partition_ids: None,
            idx,
            partition_names: None,
            lock: None,
            consistency_report: None,
            output_columns,
            decode_context,
            values: None,
            decoder: None,
            index: 0,
            initialized: false,
            produced: tidb_executor_internal_exec::RowCount::default(),
        }
    }

    /// Go `planPhysIDs`.
    #[must_use]
    pub(crate) fn with_partition_ids(mut self, ids: Option<Vec<i64>>) -> Self {
        self.partition_ids = ids;
        self
    }

    /// Go `partitionNames`.
    #[must_use]
    pub(crate) fn with_partition_names(mut self, ids: Option<Vec<i64>>) -> Self {
        self.partition_names = ids;
        self
    }

    /// Go `lock`.
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

    /// Go `initialize`'s unique-index half: the handles the entries name
    /// (`indexKeys` aligned with them), and the index keys that named no row.
    #[allow(clippy::type_complexity)]
    fn handles_from_index(
        &mut self,
        index_id: i64,
        index_values: &[Vec<Datum>],
    ) -> Result<(Vec<TableHandle>, Vec<Option<tidb_txnkv::Key>>, Vec<tidb_txnkv::Key>), ExecError> {
        let zone = self.decode_context.zone().clone();
        if is_common_handle_read(&self.table, index_id) {
            let handles = index_values
                .iter()
                .filter(|values| !values.iter().any(Datum::is_null))
                .map(|values| {
                    self.table
                        .common_handle_from_values(values, &zone)
                        .map_err(|error| ExecError::unsupported(format!("common primary key encoding failed: {error:?}")))
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok((handles, Vec::new(), Vec::new()));
        }
        let keys = self
            .table
            .unique_batch_index_keys(index_id, index_values, &zone, Some(self.decode_context.expression()))
            .map_err(|error| ExecError::unsupported(format!("unique index batch lookup failed: {error:?}")))?;
        let looked_up = self
            .table
            .lookup_unique_index_keys(&mut *self.store, &keys)
            .map_err(|error| ExecError::unsupported(format!("unique index batch lookup failed: {error:?}")))?;
        // Go: a global index's entries for partitions the running DDL hides
        // (`IDsInDDLToIgnore`) name no row.
        let ignored_partition_ids = if crate::point_get::index_is_global(&self.table, index_id) {
            crate::point_get::ids_in_ddl_to_ignore(&self.table)
        } else {
            Vec::new()
        };
        let mut found = Vec::new();
        let mut index_keys = Vec::new();
        let mut missing_index_keys = Vec::new();
        for (key, entry) in keys.into_iter().zip(looked_up) {
            let key = key.map(|(key, _)| key);
            match entry {
                Some((_, Some(partition))) if ignored_partition_ids.contains(&partition) => {}
                Some((handle, partition)) if match_partition_names(self.partition_names.as_deref(), partition) => {
                    found.push((handle, partition));
                    index_keys.push(key);
                }
                _ => missing_index_keys.extend(key),
            }
        }
        // Each row reads from the partition its key routed to (or its global
        // entry recorded), as Go's second BatchGet does.
        if self.table.partition().is_some() && found.iter().all(|(_, partition)| partition.is_some()) {
            self.partition_ids = Some(found.iter().filter_map(|(_, id)| *id).collect());
        }
        Ok((found.into_iter().map(|(handle, _)| handle).collect(), index_keys, missing_index_keys))
    }

    /// The record keys of the handles at `positions`, then their index keys.
    fn lock_keys_at(
        record_keys: &[tidb_txnkv::Key],
        index_keys: &[Option<tidb_txnkv::Key>],
        positions: &[usize],
    ) -> Vec<tidb_txnkv::Key> {
        let mut keys: Vec<tidb_txnkv::Key> =
            positions.iter().filter_map(|position| record_keys.get(*position).cloned()).collect();
        keys.extend(positions.iter().filter_map(|position| index_keys.get(*position).cloned().flatten()));
        keys
    }

    /// Go `BatchPointGetExec.initialize`.
    fn initialize(&mut self) -> Result<(), ExecError> {
        let mut index_keys = Vec::new();
        let mut missing_index_keys = Vec::new();
        let mut common_handle_read = true;
        if let Some((index_id, index_values)) = self.idx.clone() {
            common_handle_read = is_common_handle_read(&self.table, index_id);
            let (handles, keys, missing) = self.handles_from_index(index_id, &index_values)?;
            self.handles = handles;
            index_keys = keys;
            missing_index_keys = missing;
            if self.handles.is_empty() {
                // Go: with no row found, repeatable read still locks every
                // named index key.
                if let Some(lock) = self.lock.clone().filter(|lock| !lock.read_committed) {
                    lock.lock(&mut *self.store, &missing_index_keys, false)?;
                }
                return Ok(());
            }
        }
        if self.partition_ids.as_ref().is_some_and(|ids| ids.len() != self.handles.len()) {
            return Err(ExecError::unsupported("batch point partition routes lost alignment"));
        }
        // Go: "Lock all keys in repeatable read isolation. For read
        // consistency, only lock exist keys", after reading them.
        let record_keys = self.lock.as_ref().and_then(|_| self.table.record_lock_keys(&self.handles, self.partition_ids.as_deref()));
        let mut locked_before_read = false;
        if let (Some(lock), Some(record_keys)) = (self.lock.clone(), record_keys.as_ref()) {
            if !lock.read_committed {
                let positions: Vec<usize> = (0..self.handles.len()).collect();
                let mut keys = Self::lock_keys_at(record_keys, &index_keys, &positions);
                keys.extend(missing_index_keys.iter().cloned());
                lock.lock(&mut *self.store, &keys, false)?;
                locked_before_read = true;
            }
        }
        // Go's second `BatchGet`: every handle's row at once, so the storage
        // layer can schedule regions concurrently.
        let rows = self
            .table
            .stored_records_batched(&mut *self.store, &self.handles, self.partition_ids.as_deref())
            .map_err(ExecError::from)?;
        let tidb_table::tables::PointReadValues::Batch { keys: found_keys, values } = &rows else {
            return Err(ExecError::internal("a batch point read answered a single row"));
        };
        // Go: the first handle a unique index (not a clustered PRIMARY) named
        // whose row is empty is a lookup inconsistency. A handle's key is
        // kept only when its row was found.
        if let (Some((index_id, _)), Some(report), false) = (&self.idx, &self.consistency_report, common_handle_read) {
            if let Some(i) = found_keys.iter().position(Option::is_none) {
                let record_key = self
                    .table
                    .record_lock_keys(&self.handles, self.partition_ids.as_deref())
                    .and_then(|keys| keys.into_iter().nth(i))
                    .map(|key| key.as_bytes().to_vec());
                let idx_key = index_keys.get(i).cloned().flatten().map(|key| key.as_bytes().to_vec());
                return Err(self.report_missing_row(report, *index_id, i, record_key, idx_key));
            }
        }
        self.table.note_keys_read(values.len() as u64).map_err(ExecError::from)?;
        // Go `initialize` under read committed: "Lock exists keys only", the
        // found rows and their index keys. A partitioned read whose handles
        // were not routed to one partition could name no key before reading
        // and locks what it found.
        if let Some(lock) = self.lock.clone().filter(|_| !locked_before_read) {
            let mut lock_record_keys = vec![tidb_txnkv::Key::default(); found_keys.len()];
            let mut positions = Vec::new();
            for (position, key) in found_keys.iter().enumerate() {
                if let Some(key) = key {
                    lock_record_keys[position] = key.clone();
                    positions.push(position);
                }
            }
            let keys = Self::lock_keys_at(&lock_record_keys, &index_keys, &positions);
            lock.lock(&mut *self.store, &keys, false)?;
        }
        self.values = Some(rows);
        self.decoder = Some(tidb_table::tables::PointRowDecoder::new(&self.table));
        Ok(())
    }

    /// Go `ReportLookupInconsistent(ctx, 1, 0, e.handles[i:i+1], e.handles,
    /// []RecordData{{}})`.
    fn report_missing_row(
        &self,
        report: &crate::admin_check::ReportContext,
        index_id: i64,
        i: usize,
        record_key: Option<Vec<u8>>,
        idx_key: Option<Vec<u8>>,
    ) -> ExecError {
        let handles: Vec<tidb_txnkv::Handle> =
            match self.handles.iter().map(crate::admin_check::kv_handle_of).collect() {
                Ok(handles) => handles,
                Err(error) => return ExecError::unsupported(format!("{error:?}")),
            };
        let Some(index) = self.table.plan_indexes().find(|index| index.read().id == index_id) else {
            return ExecError::internal("a unique-index batch point get lost its index");
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
        let empty = tidb_logutil_consistency::RecordData { handle: handles[i].clone(), values: Vec::new() };
        ExecError::from(reporter.report_lookup_inconsistent(1, 0, &handles[i..=i], &handles, &[empty]))
    }
}

impl Executor for BatchPointGetExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.index = 0;
        self.produced.set(0);
        self.values = None;
        self.decoder = None;
        self.initialized = false;
        if self.idx.is_some() {
            self.handles.clear();
            self.partition_ids = None;
        }
        Ok(())
    }

    /// Go `BatchPointGetExec.Next`.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if !std::mem::replace(&mut self.initialized, true) {
            self.initialize()?;
        }
        while !req.is_full() && self.index < self.handles.len() {
            let index = self.index;
            // A handle with no row is simply absent from the answer.
            let value = self.values.as_ref().and_then(|values| values.get(index));
            if let Some(value) = value {
                let handle = &self.handles[index];
                let row = self
                    .decoder
                    .as_mut()
                    .expect("initialized batch point reader")
                    .decode(&self.table, handle, value, &self.decode_context)
                    .map_err(ExecError::from)?;
                let physical_id = self.partition_ids.as_ref().and_then(|ids| ids.get(index)).copied();
                append_point_row(&self.table, &self.output_columns, physical_id, &self.decode_context, req, handle, &row, value)?;
                self.produced.set(self.produced.get() + 1);
            }
            // Go advances only after successfully decoding the current value.
            self.index += 1;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.values = None;
        self.decoder = None;
        self.initialized = true;
        self.index = self.handles.len();
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

/// A batch point get refuses every offer by taking the fail-closed defaults:
/// its handle list is fixed by the statement text (Go keeps `Limit_10 | root`
/// over `Batch_Point_Get_12`), and nothing is scanned.
impl tidb_executor_internal_exec::table_access::TableAccess for BatchPointGetExec {}
