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

//! Go `pkg/executor/admin.go`'s `RecoverIndexExec` and `CleanupIndexExec`:
//! `ADMIN RECOVER INDEX` and `ADMIN CLEANUP INDEX` repair one index in
//! batches, each batch a transaction of its own (`kv.RunInNewTxn`) that reads
//! the committed table at its start ts through a coprocessor DAG and commits
//! the entries it writes or deletes.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_driver_error::ExecError;
use tidb_kv::storage::{KvStorage, TableStorage};
use tidb_model::{ColumnInfo, GoShared, TableInfo};
use tidb_proto::tipb;
use tidb_table::tables::{Index, TableCommon, TableHandle};
use tidb_txnkv::Key;

use crate::range_group_merge::SelectResult as _;

/// Go `buildIdxColsConcatHandleCols`: the index's columns (all the table's
/// when a generated column or a condition needs them), then the handle's.
fn build_idx_cols_concat_handle_cols(
    tbl_info: &TableInfo,
    index_columns: &[usize],
    has_gened_col_or_partial_index: bool,
) -> Vec<GoShared<ColumnInfo>> {
    let all: Vec<GoShared<ColumnInfo>> = tbl_info.columns.iter_deref().collect();
    let pk_cols: Vec<usize> = if tbl_info.is_common_handle {
        tbl_info
            .indices
            .iter_deref()
            .find(|index| index.read().primary)
            .map(|pk| pk.read().columns.iter_deref().map(|c| usize::try_from(c.read().offset).unwrap_or(0)).collect())
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let pk_handle_offset = tbl_info
        .pk_is_handle
        .then(|| all.iter().position(|c| c.read().field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)))
        .flatten();
    let mut columns: Vec<GoShared<ColumnInfo>> = if has_gened_col_or_partial_index {
        all.clone()
    } else {
        index_columns
            .iter()
            .filter(|offset| pk_handle_offset != Some(**offset))
            .map(|offset| all[*offset].clone())
            .collect()
    };
    if tbl_info.is_common_handle {
        for offset in pk_cols {
            let name = all[offset].read().name.lowercase().to_owned();
            if !columns.iter().any(|c| c.read().name.lowercase() == name) {
                columns.push(all[offset].clone());
            }
        }
        return columns;
    }
    if let Some(offset) = pk_handle_offset {
        columns.push(all[offset].clone());
        return columns;
    }
    let mut handle = ColumnInfo::new_extra_handle_col_info();
    handle.offset = i64::try_from(columns.len()).unwrap_or(0);
    handle.field_type = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
    columns.push(GoShared::new(handle));
    columns
}

/// Go `buildHandleColsForExec` + `HandleCols.BuildHandle`: a row's handle,
/// its last column for an int handle, the primary key's columns found by
/// name for a common one.
struct HandleCols {
    /// The positions of the primary key's columns in the row, in key order;
    /// empty for an int handle.
    common_positions: Vec<usize>,
}

impl HandleCols {
    fn new(tbl_info: &TableInfo, columns: &[GoShared<ColumnInfo>]) -> Self {
        if !tbl_info.is_common_handle {
            return Self { common_positions: Vec::new() };
        }
        let pk = tbl_info.indices.iter_deref().find(|index| index.read().primary);
        let common_positions = pk
            .map(|pk| {
                pk.read()
                    .columns
                    .iter_deref()
                    .filter_map(|c| {
                        let name = c.read().name.lowercase().to_owned();
                        columns.iter().position(|col| col.read().name.lowercase() == name)
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self { common_positions }
    }

    fn build_handle(
        &self,
        table: &TableCommon,
        row: &[Datum],
        zone: &tidb_datatype::SessionTimeZone,
    ) -> Result<TableHandle, ExecError> {
        if self.common_positions.is_empty() {
            return match row.last() {
                Some(Datum::Int(value)) => Ok(TableHandle::Int(*value)),
                Some(Datum::UInt(value)) => Ok(TableHandle::Int(*value as i64)),
                _ => Err(ExecError::Internal("a row without its int handle".into())),
            };
        }
        let values: Vec<Datum> = self.common_positions.iter().map(|i| row[*i].clone()).collect();
        table.common_handle_of_values(&values, zone).map_err(kv_error)
    }
}

fn kv_error(error: tidb_table::tables::KvTableError) -> ExecError {
    ExecError::Internal(format!("{error:?}").into())
}

fn storage_error(error: tidb_kv::storage::StorageError) -> ExecError {
    match error {
        tidb_kv::storage::StorageError::Sql(error) => ExecError::Mysql(error),
        error => ExecError::Internal(error.to_string().into()),
    }
}

/// Go `kv.Handle.Encoded()`.
fn encoded_handle(handle: &TableHandle) -> Vec<u8> {
    match handle {
        TableHandle::Int(value) => tidb_codec::encode_int_to_cmp_uint(*value).to_be_bytes().to_vec(),
        TableHandle::Common(bytes) => bytes.clone(),
    }
}

/// Go `kv.Handle.Next()` compared with the handle: whether a later handle
/// exists.
fn handle_has_next(handle: &TableHandle) -> bool {
    match handle {
        TableHandle::Int(value) => *value != i64::MAX,
        TableHandle::Common(_) => true,
    }
}

/// The DAG Go's admin executors build (`buildDAGPB`): the session zone, the
/// push-down flags, every output offset, the executors, `SetEncodeType`.
fn admin_dag(
    statement: &tidb_distsql::remote_scan::PushdownStatementContext,
    width: usize,
    executors: Vec<tipb::Executor>,
) -> tipb::DagRequest {
    let (time_zone_name, time_zone_offset) = statement.time_zone.dag_zone();
    let (encode_type, chunk_memory_layout) = crate::dag_request::encode_type_pb(tidb_distsql::EncodeType::Chunk);
    tipb::DagRequest {
        time_zone_name: Some(time_zone_name),
        time_zone_offset: Some(time_zone_offset),
        flags: Some(statement.push_down_flags),
        output_offsets: (0..width).map(|i| u32::try_from(i).expect("an output offset fits u32")).collect(),
        executors,
        encode_type: Some(encode_type as i32),
        chunk_memory_layout,
        ..tipb::DagRequest::default()
    }
}

/// Go `constructLimitPB`.
fn limit_pb(count: u64) -> tipb::Executor {
    tipb::Executor {
        limit: Some(Box::new(tipb::Limit { limit: Some(count), ..tipb::Limit::default() })),
        ..crate::dag_request::go_executor(tipb::ExecType::TypeLimit)
    }
}

/// What the admin executors share: the statement, the coprocessor client
/// and the storage new transactions begin on.
pub(crate) struct AdminIndexContext {
    pub(crate) ctx: crate::StmtContext,
    pub(crate) client: Arc<dyn tidb_txnkv::Client>,
    pub(crate) kv_storage: Arc<dyn KvStorage>,
    pub(crate) init_cap: usize,
    pub(crate) max_chunk_size: usize,
}

impl AdminIndexContext {
    /// One DAG request over `key_ranges` at `start_ts`, kept in order
    /// (`distsql.Select`).
    fn select(
        &self,
        dag: tipb::DagRequest,
        key_ranges: Vec<tidb_txnkv::KeyRange>,
        start_ts: u64,
        field_types: &[FieldType],
    ) -> Result<tidb_distsql::select_response::SelectResponseIter, ExecError> {
        let statement = crate::remote_scan::pushdown_statement_context(&self.ctx);
        crate::cop_reader::open_table_read(
            self.client.as_ref(),
            &statement,
            crate::cop_reader::TableRead {
                store_type: tidb_kv::store_type::StoreType::TiKv,
                dag,
                key_ranges,
                key_range_hints: Vec::new(),
                partition_id_and_ranges: Vec::new(),
                batch_cop: false,
                schema_version: 0,
                connection_id: tidb_exprctx::Columns::connection_id(&self.ctx).unwrap_or_default(),
                connection_alias: self.ctx.ddl_session_alias().to_owned(),
                start_ts,
                keep_order: true,
                desc: false,
                paging: statement.paging,
                field_types: field_types.to_vec(),
                cop_plan_ids: Vec::new(),
                root_plan_id: 0,
                mem_tracker: None,
                intermediate_output_types: Vec::new(),
            },
        )
        .map_err(|error| ExecError::Internal(error.into()))
    }
}

/// Go `backfillResult`.
#[derive(Default)]
struct BackfillResult {
    current_handle: Option<TableHandle>,
    added_count: i64,
    scan_row_count: i64,
}

/// Go `recoverRows`: the row's handle, the full-width row its index values
/// come from, and whether every key it files is already stored.
struct RecoverRow {
    handle: TableHandle,
    row: Vec<Datum>,
    skip: bool,
}

/// Go `RecoverIndexExec`.
pub(crate) struct RecoverIndexExec {
    admin: AdminIndexContext,
    /// Go `table`.
    table: TableCommon,
    /// Go `index`.
    index: Index,
    /// Go `physicalID`.
    physical_id: i64,
    /// Go `batchSize`.
    batch_size: usize,
    /// Go `columns`.
    columns: Vec<GoShared<ColumnInfo>>,
    /// Go `handleCols`.
    handle_cols: HandleCols,
    /// Go `containsGenedColOrPartialIndex`.
    contains_gened_col_or_partial_index: bool,
}

impl RecoverIndexExec {
    /// Go `executorBuilder.buildRecoverIndex`; `None` when the index is not
    /// a writable one of the table.
    pub(crate) fn build(admin: AdminIndexContext, table: TableCommon, index_name: &str) -> Option<Self> {
        let idx_name = index_name.to_lowercase();
        let index = table
            .index_list_for_check()
            .into_iter()
            .find(|index| index.read().name.lowercase() == idx_name && index_writable(index))?;
        let tbl_info = table.meta().read().clone();
        let mut has_gened_col_or_partial_index = index
            .column_offsets
            .iter()
            .any(|offset| tbl_info.columns.iter_deref().nth(*offset).is_some_and(|c| c.read().is_generated()));
        if index.read().has_condition() {
            has_gened_col_or_partial_index = true;
        }
        let columns = build_idx_cols_concat_handle_cols(&tbl_info, &index.column_offsets, has_gened_col_or_partial_index);
        let handle_cols = HandleCols::new(&tbl_info, &columns);
        Some(Self {
            admin,
            physical_id: tbl_info.id,
            table,
            index,
            batch_size: 2048,
            columns,
            handle_cols,
            contains_gened_col_or_partial_index: has_gened_col_or_partial_index,
        })
    }

    fn columns_types(&self) -> Vec<FieldType> {
        self.columns.iter().map(|c| c.read().field_type.clone()).collect()
    }

    /// Go `buildDAGPB`.
    fn build_dag_pb(&self, limit_cnt: u64) -> Result<tipb::DagRequest, ExecError> {
        let statement = crate::remote_scan::pushdown_statement_context(&self.admin.ctx);
        let tbl_info = self.table.meta().read().clone();
        let mut tbl_scan = tidb_table::tables::scan_from_infos::build_table_scan_from_infos(&tbl_info, &self.columns, false);
        tbl_scan.table_id = Some(self.physical_id);
        tidb_table::tables::scan_from_infos::set_pb_columns_default_value(
            &statement.time_zone,
            &mut tbl_scan.columns,
            &self.columns,
            tbl_info.name.original(),
        )
        .map_err(|error| ExecError::Internal(error.into()))?;
        let scan = tipb::Executor {
            tbl_scan: Some(tbl_scan),
            ..crate::dag_request::go_executor(tipb::ExecType::TypeTableScan)
        };
        Ok(admin_dag(&statement, self.columns.len(), vec![scan, limit_pb(limit_cnt)]))
    }

    /// Go `buildRecoverIndexKeyRanges`: `(startHandle, unlimited)`.
    fn key_ranges(&self, start_handle: Option<&TableHandle>) -> Vec<tidb_txnkv::KeyRange> {
        let prefix = tidb_tablecodec::table_key::gen_table_record_prefix(self.physical_id);
        let start_key = match start_handle {
            None => Key::from_bytes(prefix.clone()).next(),
            Some(handle) => Key::from_bytes(tidb_tablecodec::table_key::encode_row_key(self.physical_id, &encoded_handle(handle))).prefix_next(),
        };
        let end_key = Key::from_bytes(prefix).prefix_next();
        vec![tidb_txnkv::KeyRange { start_key, end_key }]
    }

    /// Go `Next`.
    pub(crate) fn next(&mut self) -> Result<(i64, i64), ExecError> {
        if self.index.read().primary && self.table.meta().read().is_common_handle {
            return Ok((0, 0));
        }
        let partitions: Vec<i64> = self
            .table
            .meta()
            .read()
            .get_partition_info()
            .map(|pi| pi.read().definitions.snapshot().iter().map(|def| def.id).collect())
            .unwrap_or_default();
        if partitions.is_empty() {
            return self.backfill_index();
        }
        let (mut total_added, mut total_scan) = (0, 0);
        for id in partitions {
            self.physical_id = id;
            let (added, scanned) = self.backfill_index()?;
            total_added += added;
            total_scan += scanned;
        }
        Ok((total_added, total_scan))
    }

    /// Go `backfillIndex`.
    fn backfill_index(&mut self) -> Result<(i64, i64), ExecError> {
        let (mut total_added_cnt, mut total_scan_cnt) = (0, 0);
        let mut current_handle: Option<TableHandle> = None;
        loop {
            let mut result = BackfillResult::default();
            let mut failure = None;
            let storage = Arc::clone(&self.admin.kv_storage);
            let outcome = storage.run_in_new_write_txn(tidb_txnkv::INTERNAL_TXN_ADMIN, &mut |start_ts, store| {
                match self.backfill_index_in_txn(start_ts, store, current_handle.as_ref()) {
                    Ok(batch) => {
                        result = batch;
                        Ok(())
                    }
                    Err(error) => {
                        failure = Some(error);
                        Err(tidb_kv::storage::StorageError::Backend("recover index".to_owned()))
                    }
                }
            });
            if let Some(error) = failure {
                return Err(error);
            }
            outcome.map_err(storage_error)?;
            total_added_cnt += result.added_count;
            total_scan_cnt += result.scan_row_count;
            // no more rows
            if result.scan_row_count == 0 {
                break;
            }
            current_handle = result.current_handle;
            if !current_handle.as_ref().is_some_and(handle_has_next) {
                break; // There is no more handles in the table.
            }
        }
        Ok((total_added_cnt, total_scan_cnt))
    }

    /// Go `fetchRecoverRows`: the batch's rows, each as the full-width row
    /// its index values and restored data come from.
    fn fetch_recover_rows(
        &mut self,
        src: &mut tidb_distsql::select_response::SelectResponseIter,
        result: &mut BackfillResult,
    ) -> Result<Vec<RecoverRow>, ExecError> {
        let types = self.columns_types();
        let zone = tidb_exprctx::Columns::time_zone(&self.admin.ctx);
        let width = self.table.meta().read().columns.len();
        let decoder = if self.contains_gened_col_or_partial_index {
            Some(crate::KvTableReader::row_decoder_recomputed(&self.table, &crate::RowDecodeContext::for_query(&self.admin.ctx)).map_err(kv_error)?)
        } else {
            None
        };
        let mut rows = Vec::new();
        let mut chunk = Chunk::new(&types, self.admin.init_cap, self.admin.max_chunk_size);
        result.scan_row_count = 0;
        loop {
            src.next(&mut chunk)?;
            if chunk.num_rows() == 0 {
                break;
            }
            for i in 0..chunk.num_rows() {
                if result.scan_row_count >= self.batch_size as i64 {
                    return Ok(rows);
                }
                let chunk_row = chunk.get_row(i);
                let values: Vec<Datum> = types.iter().enumerate().map(|(col, tp)| chunk_row.get_datum(col, tp)).collect();
                let handle = self.handle_cols.build_handle(&self.table, &values, &zone)?;
                // The full-width row: each fetched column at its offset.
                let mut row = vec![Datum::Null; width];
                for (column, value) in self.columns.iter().zip(values) {
                    let column = column.read();
                    if column.id == tidb_model::column::EXTRA_HANDLE_ID {
                        continue;
                    }
                    if let Ok(offset) = usize::try_from(column.offset) {
                        if offset < width {
                            row[offset] = value;
                        }
                    }
                }
                if let Some(decoder) = &decoder {
                    // Go `EvalVirtualColumn` over the row.
                    let mut decoded = tidb_table::tables::row_decoder::DecodedRow { values: row, by_id: BTreeMap::new() };
                    decoder.eval_remaining(&mut decoded).map_err(kv_error)?;
                    row = decoded.values;
                }
                if self.index.read().has_condition()
                    && !self.table.index_condition_holds(&self.index, &row, &zone).map_err(kv_error)?
                {
                    // Skip partial index not match row. Still add the
                    // `scanRowCount` to avoid stopping the recovery too early.
                    result.scan_row_count += 1;
                    result.current_handle = Some(handle);
                    continue;
                }
                rows.push(RecoverRow { handle: handle.clone(), row, skip: true });
                result.scan_row_count += 1;
                result.current_handle = Some(handle);
            }
        }
        Ok(rows)
    }

    /// Go `batchMarkDup`.
    fn batch_mark_dup(&self, store: &mut dyn TableStorage, rows: &mut [RecoverRow]) -> Result<(), ExecError> {
        if rows.is_empty() {
            return Ok(());
        }
        let zone = tidb_exprctx::Columns::time_zone(&self.admin.ctx);
        let mut batch_keys = Vec::new();
        let mut distinct_flags = Vec::new();
        let mut row_idx = Vec::new();
        for (i, row) in rows.iter().enumerate() {
            for entry in self.table.index_entry_keys(&self.index, &row.row, &row.handle, self.physical_id, &zone).map_err(kv_error)? {
                batch_keys.push(Key::from_bytes(entry.key));
                distinct_flags.push(entry.distinct);
                row_idx.push(i);
            }
        }
        let values = store.batch_get(&batch_keys).map_err(storage_error)?;
        // 1. unique-key is duplicate and the handle is equal, skip it.
        // 2. unique-key is duplicate and the handle is not equal, data is not consistent, log it and skip it.
        // 3. non-unique-key is duplicate, skip it.
        for (i, key) in batch_keys.iter().enumerate() {
            let found = values.get(key);
            if let Some(value) = found {
                if distinct_flags[i] {
                    let handle = tidb_tablecodec::decode_handle_in_index_value(value)
                        .map_err(|error| ExecError::Internal(format!("{error:?}").into()))?;
                    let in_table = &rows[row_idx[i]].handle;
                    if handle.as_ref().map(crate::admin_check::table_handle_of).as_ref() != Some(in_table) {
                        tidb_logutil::bg_logger().warn(
                            "recover index: the constraint of unique index is broken, handle in index is not equal to handle in table",
                            &[tidb_log::Field::new("index", tidb_log::Value::Str(self.index.read().name.original().to_owned()))],
                        );
                    }
                }
            }
            rows[row_idx[i]].skip = found.is_some() && rows[row_idx[i]].skip;
        }
        Ok(())
    }

    /// Go `backfillIndexInTxn`.
    fn backfill_index_in_txn(
        &mut self,
        start_ts: u64,
        store: &mut dyn TableStorage,
        current_handle: Option<&TableHandle>,
    ) -> Result<BackfillResult, ExecError> {
        let mut result = BackfillResult::default();
        let dag = self.build_dag_pb(self.batch_size as u64)?;
        let mut src = self.admin.select(dag, self.key_ranges(current_handle), start_ts, &self.columns_types())?;
        let rows = self.fetch_recover_rows(&mut src, &mut result);
        let _ = src.close();
        let mut rows = rows?;
        self.batch_mark_dup(store, &mut rows)?;
        let zone = tidb_exprctx::Columns::time_zone(&self.admin.ctx);
        for row in rows {
            if row.skip {
                continue;
            }
            let record_key = tidb_tablecodec::table_key::encode_row_key(self.physical_id, &encoded_handle(&row.handle));
            store.lock_keys_optimistically(&[Key::from_bytes(record_key)]).map_err(storage_error)?;
            // `index.Create(..., table.WithIgnoreAssertion,
            // table.DupKeyCheckSkip)`: every entry of the row, unchecked.
            for entry in self.table.index_entry_keys(&self.index, &row.row, &row.handle, self.physical_id, &zone).map_err(kv_error)? {
                let value = self
                    .table
                    .gen_index_value(&self.index, &entry.values, &row.row, &row.handle, entry.distinct, self.physical_id, &zone)
                    .map_err(kv_error)?;
                store.set(Key::from_bytes(entry.key), value).map_err(storage_error)?;
            }
            result.added_count += 1;
        }
        Ok(result)
    }
}

/// Go `IsIndexWritable`: not on its way in or out.
fn index_writable(index: &Index) -> bool {
    let state = index.read().state;
    state != tidb_model::SchemaState::DELETE_ONLY && state != tidb_model::SchemaState::DELETE_REORGANIZATION
}

/// Go `CleanupIndexExec`.
pub(crate) struct CleanupIndexExec {
    admin: AdminIndexContext,
    remove_cnt: u64,
    table: TableCommon,
    index: Index,
    physical_id: i64,
    columns: Vec<GoShared<ColumnInfo>>,
    handle_cols: HandleCols,
    /// Go `idxValues`: each handle's index values, in scan order.
    idx_values: Vec<(TableHandle, Vec<Vec<Datum>>)>,
    batch_size: u64,
    last_idx_key: Vec<u8>,
    scan_row_cnt: u64,
}

impl CleanupIndexExec {
    /// Go `executorBuilder.buildCleanupIndex`; `None` when no public index
    /// has the name.
    pub(crate) fn build(admin: AdminIndexContext, table: TableCommon, index_name: &str) -> Option<Self> {
        let idx_name = index_name.to_lowercase();
        let index = table.index_list_for_check().into_iter().find(|index| {
            index.read().state == tidb_model::SchemaState::PUBLIC && index.read().name.lowercase() == idx_name
        })?;
        let tbl_info = table.meta().read().clone();
        let mut columns = build_idx_cols_concat_handle_cols(&tbl_info, &index.column_offsets, false);
        let handle_cols = HandleCols::new(&tbl_info, &columns);
        if index.read().global {
            columns.push(GoShared::new(ColumnInfo::new_extra_phys_tbl_id_col_info()));
        }
        Some(Self {
            admin,
            remove_cnt: 0,
            physical_id: tbl_info.id,
            table,
            index,
            columns,
            handle_cols,
            idx_values: Vec::new(),
            batch_size: 20000,
            last_idx_key: Vec::new(),
            scan_row_cnt: 0,
        })
    }

    /// Go `getIdxColTypes`.
    fn get_idx_col_types(&self) -> Vec<FieldType> {
        self.columns.iter().map(|c| c.read().field_type.array_type()).collect()
    }

    /// Go `init`: the scan starts after the index's least key.
    fn init(&mut self) -> Result<(), ExecError> {
        self.idx_values.clear();
        self.last_idx_key = self
            .table
            .split_index_key(&self.index, vec![Datum::Null], self.physical_id, &tidb_exprctx::Columns::time_zone(&self.admin.ctx))
            .map_err(kv_error)?;
        Ok(())
    }

    /// Go `Next`.
    pub(crate) fn next(&mut self) -> Result<u64, ExecError> {
        if self.table.meta().read().is_common_handle && self.index.read().primary {
            return Ok(0);
        }
        let partitions: Vec<i64> = self
            .table
            .meta()
            .read()
            .get_partition_info()
            .map(|pi| pi.read().definitions.snapshot().iter().map(|def| def.id).collect())
            .unwrap_or_default();
        if !partitions.is_empty() && !self.index.read().global {
            for id in partitions {
                self.physical_id = id;
                self.init()?;
                self.clean_table_index()?;
            }
        } else {
            self.init()?;
            self.clean_table_index()?;
        }
        Ok(self.remove_cnt)
    }

    /// Go `cleanTableIndex`.
    fn clean_table_index(&mut self) -> Result<(), ExecError> {
        loop {
            let mut failure = None;
            let storage = Arc::clone(&self.admin.kv_storage);
            let outcome = storage.run_in_new_write_txn(tidb_txnkv::INTERNAL_TXN_ADMIN, &mut |start_ts, store| {
                let attempt = self
                    .fetch_index(start_ts)
                    .and_then(|()| self.batch_get_record(store))
                    .and_then(|(keys, values)| self.delete_dangling_idx(store, &keys, &values));
                attempt.map_err(|error| {
                    failure = Some(error);
                    tidb_kv::storage::StorageError::Backend("cleanup index".to_owned())
                })
            });
            if let Some(error) = failure {
                return Err(error);
            }
            outcome.map_err(storage_error)?;
            if self.scan_row_cnt == 0 {
                break;
            }
            self.scan_row_cnt = 0;
            self.idx_values.clear();
        }
        Ok(())
    }

    /// Go `buildIdxDAGPB`.
    fn build_idx_dag_pb(&self) -> Result<tipb::DagRequest, ExecError> {
        let statement = crate::remote_scan::pushdown_statement_context(&self.admin.ctx);
        let tbl_info = self.table.meta().read().clone();
        let mut idx_scan = tipb::IndexScan {
            table_id: Some(self.physical_id),
            index_id: Some(self.index.read().id),
            columns: tidb_util::misc::columns_to_proto(&self.columns, tbl_info.pk_is_handle, true, false),
            primary_column_ids: tidb_table::tables::scan_from_infos::try_get_common_pk_column_ids(&tbl_info),
            ..tipb::IndexScan::default()
        };
        tidb_table::tables::scan_from_infos::set_pb_columns_default_value(
            &statement.time_zone,
            &mut idx_scan.columns,
            &self.columns,
            tbl_info.name.original(),
        )
        .map_err(|error| ExecError::Internal(error.into()))?;
        let scan = tipb::Executor {
            idx_scan: Some(idx_scan),
            ..crate::dag_request::go_executor(tipb::ExecType::TypeIndexScan)
        };
        Ok(admin_dag(&statement, self.columns.len(), vec![scan, limit_pb(self.batch_size)]))
    }

    /// Go `fetchIndex`.
    fn fetch_index(&mut self, start_ts: u64) -> Result<(), ExecError> {
        let dag = self.build_idx_dag_pb()?;
        // Go `buildIndexScan`: the full index range, from after the last key.
        let index_id = if self.index.read().global { self.table.meta().read().id } else { self.physical_id };
        let mut key_ranges: Vec<tidb_txnkv::KeyRange> = tidb_distsql::index_ranges_to_kv_ranges(
            Some(&tidb_exprctx::Columns::time_zone(&self.admin.ctx)),
            &[index_id],
            self.index.read().id,
            &tidb_ranger::points::full_range(),
        )
        .map_err(|error| ExecError::Internal(format!("{error:?}").into()))?
        .into_iter()
        .flatten()
        .collect();
        if let Some(first) = key_ranges.first_mut() {
            first.start_key = Key::from_bytes(self.last_idx_key.clone()).prefix_next();
        }
        let types = self.get_idx_col_types();
        let mut result = self.admin.select(dag, key_ranges, start_ts, &types)?;
        let fetched = self.fetch_index_rows(&mut result, &types);
        let _ = result.close();
        fetched
    }

    fn fetch_index_rows(
        &mut self,
        result: &mut tidb_distsql::select_response::SelectResponseIter,
        types: &[FieldType],
    ) -> Result<(), ExecError> {
        let zone = tidb_exprctx::Columns::time_zone(&self.admin.ctx);
        let idx_col_len = self.index.column_offsets.len();
        let mut chunk = Chunk::new(types, self.admin.init_cap, self.admin.max_chunk_size);
        loop {
            result.next(&mut chunk)?;
            if chunk.num_rows() == 0 {
                return Ok(());
            }
            for i in 0..chunk.num_rows() {
                let row = chunk.get_row(i);
                let values: Vec<Datum> = types.iter().enumerate().map(|(col, tp)| row.get_datum(col, tp)).collect();
                let handle_row = if self.index.read().global { &values[..values.len() - 1] } else { &values[..] };
                let handle = self.handle_cols.build_handle(&self.table, handle_row, &zone)?;
                let idx_vals: Vec<Datum> = values[..idx_col_len].to_vec();
                match self.idx_values.iter_mut().find(|(h, _)| *h == handle) {
                    Some((_, group)) => group.push(idx_vals.clone()),
                    None => self.idx_values.push((handle.clone(), vec![idx_vals.clone()])),
                }
                let (idx_key, _) = self
                    .table
                    .gen_index_key(self.index.read().id, idx_vals, Some(&handle), self.physical_id, &zone)
                    .map_err(kv_error)?;
                self.scan_row_cnt += 1;
                self.last_idx_key = idx_key;
                if self.scan_row_cnt >= self.batch_size {
                    return Ok(());
                }
            }
        }
    }

    /// Go `batchGetRecord`.
    fn batch_get_record(&self, store: &mut dyn TableStorage) -> Result<(Vec<Key>, HashMap<Key, Vec<u8>>), ExecError> {
        let keys: Vec<Key> = self
            .idx_values
            .iter()
            .map(|(handle, _)| Key::from_bytes(tidb_tablecodec::table_key::encode_row_key(self.physical_id, &encoded_handle(handle))))
            .collect();
        let values = store.batch_get(&keys).map_err(storage_error)?;
        Ok((keys, values))
    }

    /// Go `deleteDanglingIdx`.
    fn delete_dangling_idx(
        &mut self,
        store: &mut dyn TableStorage,
        keys: &[Key],
        values: &HashMap<Key, Vec<u8>>,
    ) -> Result<(), ExecError> {
        let zone = tidb_exprctx::Columns::time_zone(&self.admin.ctx);
        for (key, (handle, group)) in keys.iter().zip(&self.idx_values) {
            if values.contains_key(key) {
                continue;
            }
            for handle_idx_vals in group {
                let (idx_key, _) = self
                    .table
                    .gen_index_key(self.index.read().id, handle_idx_vals.clone(), Some(handle), self.physical_id, &zone)
                    .map_err(kv_error)?;
                store.delete(Key::from_bytes(idx_key)).map_err(storage_error)?;
                self.remove_cnt += 1;
            }
        }
        Ok(())
    }
}
