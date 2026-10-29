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

//! Go's delete ranges (`pkg/ddl/delete_range.go`, `delete_range_util.go`,
//! `pkg/ddl/util`): a finished job that leaves keys behind records them in
//! `mysql.gc_delete_range` (`insertJobIntoDeleteRangeTable`), and whoever
//! owns the store's garbage collection deletes them and moves the rows to
//! `mysql.gc_delete_range_done` (`CompleteDeleteRange`).

use std::collections::HashMap;

use tidb_datatype::Datum;
use tidb_model::{ActionType, Job, JobState, TableInfo};
use tidb_txnkv::transaction::BufferMutation;

use tidb_infoschema::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::cluster_ddl::DdlPlanError;
use tidb_syssession::mysql_system_tables::{scan_system_table, SystemRow, SystemTableView};
use tidb_syssession::system_row_write::{delete_row, insert_row, update_row, RowValues};

/// Go `batchInsertDeleteRangeSize`: drop schema records its tables in
/// batches of this many.
const BATCH_INSERT_DELETE_RANGE_SIZE: usize = 256;

/// Go `tablecodec.TempIndexPrefix`.
const TEMP_INDEX_PREFIX: i64 = 0x7fff_0000_0000_0000;

/// Go `util.DelRangeTask`: one range of keys a job left behind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelRangeTask {
    /// `job_id`.
    pub job_id: i64,
    /// `element_id`.
    pub element_id: i64,
    /// `start_key`, decoded from hex.
    pub start_key: Vec<u8>,
    /// `end_key`, decoded from hex.
    pub end_key: Vec<u8>,
}

/// Go `elementIDAlloc`: element ids numbered 1, 2, ... per job, one per
/// physical table and one per (physical table, index).
#[derive(Default)]
struct ElementIdAlloc {
    physical_ids: HashMap<i64, i64>,
    index_ids: HashMap<(i64, i64), i64>,
}

impl ElementIdAlloc {
    fn next(&self) -> i64 {
        (self.physical_ids.len() + self.index_ids.len() + 1) as i64
    }

    fn alloc_for_index_id(&mut self, physical_id: i64, index_id: i64) -> i64 {
        if let Some(id) = self.index_ids.get(&(physical_id, index_id)) {
            return *id;
        }
        let next = self.next();
        self.index_ids.insert((physical_id, index_id), next);
        next
    }

    fn alloc_for_physical_id(&mut self, table_id: i64) -> i64 {
        if let Some(id) = self.physical_ids.get(&table_id) {
            return *id;
        }
        let next = self.next();
        self.physical_ids.insert(table_id, next);
        next
    }
}

/// Go `JobNeedGC`: whether a finished job left keys to delete.
pub fn job_need_gc(job: &Job) -> bool {
    if job.is_cancelled() {
        return false;
    }
    if let Some(warning) = &job.warning {
        // A field or key that did not exist left nothing behind.
        if warning.read().code().value()
            == tidb_mysql::errors::errcode::ErrCantDropFieldOrKey as isize
        {
            return false;
        }
    }
    match job.type_ {
        ActionType::ACTION_DROP_SCHEMA
        | ActionType::ACTION_DROP_TABLE
        | ActionType::ACTION_TRUNCATE_TABLE
        | ActionType::ACTION_DROP_PRIMARY_KEY
        | ActionType::ACTION_DROP_TABLE_PARTITION
        | ActionType::ACTION_TRUNCATE_TABLE_PARTITION
        | ActionType::ACTION_DROP_COLUMN
        | ActionType::ACTION_MODIFY_COLUMN
        | ActionType::ACTION_ADD_INDEX
        | ActionType::ACTION_ADD_PRIMARY_KEY
        | ActionType::ACTION_REORGANIZE_PARTITION
        | ActionType::ACTION_REMOVE_PARTITIONING
        | ActionType::ACTION_ALTER_TABLE_PARTITIONING => true,
        ActionType::ACTION_DROP_INDEX => {
            // A columnar index keeps no key range.
            let mut job = job.clone();
            match tidb_model::get_finished_modify_index_args(&mut job) {
                Ok(Some(args)) => !args
                    .read()
                    .index_args
                    .iter_deref()
                    .next()
                    .is_some_and(|arg| arg.read().is_columnar),
                _ => false,
            }
        }
        ActionType::ACTION_MULTI_SCHEMA_CHANGE => {
            job.multi_schema_info.as_ref().is_some_and(|info| {
                info.read()
                    .sub_jobs
                    .iter_deref()
                    .enumerate()
                    .any(|(sequence, sub)| {
                        job_need_gc(&sub.read().to_proxy_job(job, sequence as i64))
                    })
            })
        }
        _ => false,
    }
}

/// Go `AddDelRangeJobInternal`: the ranges a finished job left behind, each
/// with its element id.
pub fn delete_ranges_for_job(job: &Job) -> Result<Vec<DelRangeTask>, DdlPlanError> {
    let mut alloc = ElementIdAlloc::default();
    let mut ranges = Vec::new();
    if let Some(info) = &job.multi_schema_info {
        for (sequence, sub) in info.read().sub_jobs.iter_deref().enumerate() {
            let proxy = sub.read().to_proxy_job(job, sequence as i64);
            if job_need_gc(&proxy) {
                insert_job_into_delete_range_table(&proxy, &mut alloc, &mut ranges)?;
            }
        }
    } else {
        insert_job_into_delete_range_table(job, &mut alloc, &mut ranges)?;
    }
    Ok(ranges)
}

fn args_error(error: serde_json::Error) -> DdlPlanError {
    DdlPlanError::Encode(error.to_string())
}

fn missing_args(job: &Job) -> DdlPlanError {
    DdlPlanError::Encode(format!("DDL job {} has no finished arguments", job.id))
}

/// Go `insertJobIntoDeleteRangeTable`.
fn insert_job_into_delete_range_table(
    job: &Job,
    alloc: &mut ElementIdAlloc,
    ranges: &mut Vec<DelRangeTask>,
) -> Result<(), DdlPlanError> {
    let mut job = job.clone();
    let job_id = job.id;
    match job.type_ {
        ActionType::ACTION_DROP_SCHEMA => {
            let args = tidb_model::get_finished_drop_schema_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let table_ids = args.read().all_dropped_table_ids.get().snapshot();
            for batch in table_ids.chunks(BATCH_INSERT_DELETE_RANGE_SIZE) {
                batch_delete_tables_range(job_id, batch, alloc, ranges);
            }
        }
        ActionType::ACTION_DROP_TABLE => {
            let args = tidb_model::get_finished_drop_table_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let partition_ids = args.read().old_partition_ids.clone();
            if !partition_ids.is_empty() {
                batch_delete_tables_range(job_id, &partition_ids, alloc, ranges);
            }
            // The logical table may hold global index regions.
            batch_delete_tables_range(job_id, &[job.table_id], alloc, ranges);
        }
        ActionType::ACTION_TRUNCATE_TABLE => {
            let args = tidb_model::get_finished_truncate_table_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let partition_ids = args.read().old_partition_ids.get().snapshot();
            if !partition_ids.is_empty() {
                batch_delete_tables_range(job_id, &partition_ids, alloc, ranges);
            }
            batch_delete_tables_range(job_id, &[job.table_id], alloc, ranges);
        }
        ActionType::ACTION_DROP_TABLE_PARTITION => {
            let args = tidb_model::get_finished_table_partition_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let ids = args.read().old_physical_table_ids.get().snapshot();
            batch_delete_tables_range(job_id, &ids, alloc, ranges);
        }
        ActionType::ACTION_REORGANIZE_PARTITION
        | ActionType::ACTION_REMOVE_PARTITIONING
        | ActionType::ACTION_ALTER_TABLE_PARTITIONING => {
            let args = tidb_model::get_finished_table_partition_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let args = args.read();
            for index in args.old_global_indexes.get().snapshot() {
                batch_delete_indices_range(
                    job_id,
                    index.table_id,
                    &[index.index_id],
                    alloc,
                    ranges,
                );
            }
            let ids = args.old_physical_table_ids.get().snapshot();
            batch_delete_tables_range(job_id, &ids, alloc, ranges);
        }
        ActionType::ACTION_TRUNCATE_TABLE_PARTITION => {
            let args = tidb_model::get_truncate_table_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let ids = args.read().old_partition_ids.get().snapshot();
            batch_delete_tables_range(job_id, &ids, alloc, ranges);
        }
        // Add index records its ranges because a cancelled one rolls back.
        ActionType::ACTION_ADD_INDEX | ActionType::ACTION_ADD_PRIMARY_KEY => {
            let args = tidb_model::get_finished_modify_index_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let args = args.read();
            let partition_ids = args.partition_ids.snapshot();
            let physical_ids = if partition_ids.is_empty() {
                vec![job.table_id]
            } else {
                partition_ids
            };
            for index in args.index_args.iter_deref() {
                let index = index.read();
                let temp_index_id = TEMP_INDEX_PREFIX | index.index_id;
                let index_ids = if job.state == JobState::ROLLBACK_DONE {
                    vec![index.index_id, temp_index_id]
                } else {
                    vec![temp_index_id]
                };
                if index.is_global {
                    batch_delete_indices_range(job_id, job.table_id, &index_ids, alloc, ranges);
                } else {
                    for physical_id in &physical_ids {
                        batch_delete_indices_range(job_id, *physical_id, &index_ids, alloc, ranges);
                    }
                }
            }
        }
        ActionType::ACTION_DROP_INDEX | ActionType::ACTION_DROP_PRIMARY_KEY => {
            let args = tidb_model::get_finished_modify_index_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let args = args.read();
            let index_id = args
                .index_args
                .iter_deref()
                .next()
                .map(|arg| arg.read().index_id)
                .ok_or_else(|| missing_args(&job))?;
            let partition_ids = args.partition_ids.snapshot();
            // No partition ids: a global index, even on a partitioned table.
            if partition_ids.is_empty() {
                batch_delete_indices_range(job_id, job.table_id, &[index_id], alloc, ranges);
            } else {
                for physical_id in partition_ids {
                    batch_delete_indices_range(job_id, physical_id, &[index_id], alloc, ranges);
                }
            }
        }
        ActionType::ACTION_DROP_COLUMN => {
            let args = tidb_model::get_table_column_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let args = args.read();
            if !args.index_ids.is_empty() {
                if args.partition_ids.is_empty() {
                    batch_delete_indices_range(
                        job_id,
                        job.table_id,
                        &args.index_ids,
                        alloc,
                        ranges,
                    );
                } else {
                    for physical_id in &args.partition_ids {
                        batch_delete_indices_range(
                            job_id,
                            *physical_id,
                            &args.index_ids,
                            alloc,
                            ranges,
                        );
                    }
                }
            }
        }
        ActionType::ACTION_MODIFY_COLUMN => {
            let args = tidb_model::get_finished_modify_column_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let args = args.read();
            if args.index_ids.is_empty() {
                return Ok(());
            }
            if args.partition_ids.is_empty() {
                batch_delete_indices_range(job_id, job.table_id, &args.index_ids, alloc, ranges);
            } else {
                for physical_id in &args.partition_ids {
                    batch_delete_indices_range(
                        job_id,
                        *physical_id,
                        &args.index_ids,
                        alloc,
                        ranges,
                    );
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Go `doBatchDeleteIndiceRange`.
fn batch_delete_indices_range(
    job_id: i64,
    table_id: i64,
    index_ids: &[i64],
    alloc: &mut ElementIdAlloc,
    ranges: &mut Vec<DelRangeTask>,
) {
    for index_id in index_ids {
        let element_id = alloc.alloc_for_index_id(table_id, *index_id);
        ranges.push(DelRangeTask {
            job_id,
            element_id,
            start_key: tidb_tablecodec::table_key::encode_table_index_prefix(table_id, *index_id),
            end_key: tidb_tablecodec::table_key::encode_table_index_prefix(table_id, index_id + 1),
        });
    }
}

/// Go `doBatchDeleteTablesRange`.
fn batch_delete_tables_range(
    job_id: i64,
    table_ids: &[i64],
    alloc: &mut ElementIdAlloc,
    ranges: &mut Vec<DelRangeTask>,
) {
    for table_id in table_ids {
        let element_id = alloc.alloc_for_physical_id(*table_id);
        ranges.push(DelRangeTask {
            job_id,
            element_id,
            start_key: tidb_tablecodec::table_key::encode_table_prefix(*table_id),
            end_key: tidb_tablecodec::table_key::encode_table_prefix(table_id + 1),
        });
    }
}

/// Builds one job's ranges in Go's element order, for a change that did not
/// run as a persisted job and so carries no finished arguments.
#[derive(Default)]
pub struct DelRangeBuilder {
    job_id: i64,
    alloc: ElementIdAlloc,
    ranges: Vec<DelRangeTask>,
}

impl DelRangeBuilder {
    /// A builder for job `job_id`.
    #[must_use]
    pub fn new(job_id: i64) -> Self {
        Self {
            job_id,
            ..Self::default()
        }
    }

    /// Go `doBatchDeleteTablesRange`.
    pub fn tables(&mut self, table_ids: &[i64]) {
        for batch in table_ids.chunks(BATCH_INSERT_DELETE_RANGE_SIZE) {
            batch_delete_tables_range(self.job_id, batch, &mut self.alloc, &mut self.ranges);
        }
    }

    /// Go `doBatchDeleteIndiceRange`.
    pub fn indices(&mut self, table_id: i64, index_ids: &[i64]) {
        batch_delete_indices_range(
            self.job_id,
            table_id,
            index_ids,
            &mut self.alloc,
            &mut self.ranges,
        );
    }

    /// The ranges, in order.
    #[must_use]
    pub fn finish(self) -> Vec<DelRangeTask> {
        self.ranges
    }
}

/// One stored `gc_delete_range` / `gc_delete_range_done` row.
pub struct StoredDelRange {
    key: Vec<u8>,
    values: RowValues,
    /// The range.
    pub task: DelRangeTask,
    /// `ts`.
    pub ts: u64,
}

/// `mysql.gc_delete_range` or `mysql.gc_delete_range_done`, located.
pub struct DelRangeTable {
    table: Box<TableInfo>,
    view: SystemTableView,
    db_id: i64,
}

/// Go `hex.EncodeToString`: lowercase.
fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    out
}

/// Go `hex.DecodeString`: either case, an even number of digits.
fn hex_decode(text: &[u8]) -> Option<Vec<u8>> {
    fn digit(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    if text.len() % 2 != 0 {
        return None;
    }
    text.chunks(2)
        .map(|pair| Some(digit(pair[0])? << 4 | digit(pair[1])?))
        .collect()
}

fn text(message: impl std::fmt::Display) -> String {
    message.to_string()
}

impl DelRangeTable {
    /// Finds `mysql.<name>` in the catalog.
    pub fn locate(catalog: &ClusterCatalog, name: &str) -> Result<Self, String> {
        let (database, table) = catalog
            .find_table("mysql", name)
            .ok_or_else(|| format!("mysql.{name} does not exist"))?;
        let db_id = database.id;
        let table = table.clone_like_go();
        let view = SystemTableView::project(
            &format!("mysql.{name}"),
            &table,
            &["job_id", "element_id", "start_key", "end_key", "ts"],
        );
        Ok(Self {
            table: Box::new(table),
            view,
            db_id,
        })
    }

    /// The table's metadata, for allocating a row id.
    #[must_use]
    pub fn table(&self) -> &TableInfo {
        &self.table
    }

    /// The id of the database holding the table.
    #[must_use]
    pub fn db_id(&self) -> i64 {
        self.db_id
    }

    fn column_id(&self, column: &str) -> Result<i64, String> {
        self.table
            .columns
            .iter_deref()
            .find(|candidate| candidate.read().name.lowercase() == column)
            .map(|candidate| candidate.read().id)
            .ok_or_else(|| format!("{} has no column `{column}`", self.table.name))
    }

    /// Go `loadDeleteRangesFromTable`: every row whose `ts` is below
    /// `safe_point`, in the table's order.
    pub fn load<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        safe_point: u64,
    ) -> Result<Vec<StoredDelRange>, String> {
        let mut rows = Vec::new();
        for (key, value) in scan_system_table(snapshot, &self.view).map_err(text)? {
            let row = SystemRow::parse(&self.view, &key, &value).map_err(text)?;
            let ts = row.i64("ts").map_err(text)?.unwrap_or(0) as u64;
            if ts >= safe_point {
                continue;
            }
            let hex_column = |column: &str| -> Result<Vec<u8>, String> {
                let raw = row.bytes(column).map_err(text)?.unwrap_or_default();
                hex_decode(&raw).ok_or_else(|| format!("{column} is not hex"))
            };
            let task = DelRangeTask {
                job_id: row.i64("job_id").map_err(text)?.unwrap_or(0),
                element_id: row.i64("element_id").map_err(text)?.unwrap_or(0),
                start_key: hex_column("start_key")?,
                end_key: hex_column("end_key")?,
            };
            rows.push(StoredDelRange {
                key: key.to_vec(),
                values: row.into_values(),
                task,
                ts,
            });
        }
        Ok(rows)
    }

    fn values(&self, task: &DelRangeTask, ts: u64) -> Result<RowValues, String> {
        let mut values = RowValues::new();
        values.insert(self.column_id("job_id")?, Datum::Int(task.job_id));
        values.insert(self.column_id("element_id")?, Datum::Int(task.element_id));
        values.insert(
            self.column_id("start_key")?,
            Datum::Bytes(hex_encode(&task.start_key).into_bytes()),
        );
        values.insert(
            self.column_id("end_key")?,
            Datum::Bytes(hex_encode(&task.end_key).into_bytes()),
        );
        values.insert(self.column_id("ts")?, Datum::Int(ts as i64));
        Ok(values)
    }

    /// `INSERT IGNORE`: one row, unless `(job_id, element_id)` is taken.
    pub fn insert_ignore_mutations(
        &self,
        existing: &[StoredDelRange],
        task: &DelRangeTask,
        ts: u64,
        row_id: i64,
    ) -> Result<Vec<BufferMutation>, String> {
        if existing
            .iter()
            .any(|row| row.task.job_id == task.job_id && row.task.element_id == task.element_id)
        {
            return Ok(Vec::new());
        }
        insert_row(&self.table, row_id, &self.values(task, ts)?).map_err(text)
    }

    /// Go `updateDeleteRangeSQL`: the row's start key moves on.
    pub fn update_start_key_mutations(
        &self,
        row: &StoredDelRange,
        start_key: &[u8],
    ) -> Result<Vec<BufferMutation>, String> {
        let mut values = row.values.clone();
        values.insert(
            self.column_id("start_key")?,
            Datum::Bytes(hex_encode(start_key).into_bytes()),
        );
        update_row(&self.table, &row.key, &row.values, &values).map_err(text)
    }

    /// Go `completeDeleteRangeSQL`.
    pub fn delete_mutations(&self, row: &StoredDelRange) -> Result<Vec<BufferMutation>, String> {
        delete_row(&self.table, &row.key, &row.values).map_err(text)
    }
}

/// Go `finishDDLJob` -> `addDelRangeJob`: the `gc_delete_range` rows a
/// finished job owes, allocated from the node's row-ID allocators.
pub fn add_del_range_job_mutations<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    job: &Job,
    ts: u64,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<Vec<BufferMutation>, DdlPlanError> {
    if !job_need_gc(job) {
        return Ok(Vec::new());
    }
    let ranges = delete_ranges_for_job(job)?;
    del_range_rows_mutations(snapshot, catalog, &ranges, ts, row_ids)
}

/// The `gc_delete_range` rows (`INSERT IGNORE`) for `ranges`.
pub fn del_range_rows_mutations<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    ranges: &[DelRangeTask],
    ts: u64,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<Vec<BufferMutation>, DdlPlanError> {
    if ranges.is_empty() {
        return Ok(Vec::new());
    }
    let table = DelRangeTable::locate(catalog, "gc_delete_range").map_err(DdlPlanError::Encode)?;
    let existing = table
        .load(snapshot, u64::MAX)
        .map_err(DdlPlanError::Encode)?;
    let ids = row_ids.ok_or_else(|| {
        DdlPlanError::Encode("mysql.gc_delete_range has no row-ID allocator".into())
    })?;
    let allocator = ids.row_allocator_for(table.db_id(), table.table());
    let mut mutations = Vec::new();
    for range in ranges {
        let id = allocator.alloc_next().map_err(|error| {
            DdlPlanError::Encode(format!("mysql.gc_delete_range's row-ID allocator: {error}"))
        })?;
        let row_id = i64::try_from(id).map_err(|_| {
            DdlPlanError::Encode("mysql.gc_delete_range's row-ID allocator overflowed".into())
        })?;
        mutations.extend(
            table
                .insert_ignore_mutations(&existing, range, ts, row_id)
                .map_err(DdlPlanError::Encode)?,
        );
    }
    Ok(mutations)
}
