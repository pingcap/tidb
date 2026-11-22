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
use tidb_model::{ActionType, Job, JobState};

use tidb_ddl_build::plan_error::DdlPlanError;

/// Go `batchInsertDeleteRangeSize`: drop schema records its tables in
/// batches of this many.
const BATCH_INSERT_DELETE_RANGE_SIZE: usize = 256;

/// Go `insertDeleteRangeSQLPrefix`.
const INSERT_DELETE_RANGE_SQL_PREFIX: &str = "INSERT IGNORE INTO mysql.gc_delete_range VALUES ";
/// Go `insertDeleteRangeSQLValue`.
const INSERT_DELETE_RANGE_SQL_VALUE: &str = "(%?, %?, %?, %?, %?)";

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
        // Go master: "Placement and TiFlash PD rules can survive rollback of
        // the replacement log's metadata. Return true to queue a
        // delete-range record for NewMLogID: processing that record also
        // removes those rules, even if the log contains no data."
        if job.type_ == ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER {
            let mut job = job.clone();
            return tidb_model::get_refresh_materialized_view_complete_out_of_place_cutover_args(&mut job)
                .ok()
                .flatten()
                .is_some_and(|args| args.read().new_mlog_id != 0);
        }
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
        | ActionType::ACTION_ALTER_TABLE_PARTITIONING
        // Go master: the materialized view drops and the out-of-place
        // cutover leave a table's keys behind.
        | ActionType::ACTION_DROP_MATERIALIZED_VIEW
        | ActionType::ACTION_DROP_MATERIALIZED_VIEW_LOG
        | ActionType::ACTION_DROP_MATERIALIZED_VIEW_SHADOW
        | ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER => true,
        // Go master: CREATE MATERIALIZED VIEW may create a physical table
        // before the initial build fails; only rollback cleanup needs it.
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW => job.is_rollback_done() && job.table_id != 0,
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
/// with its element id, handed to `wrapper`.
pub fn add_del_range_job_internal(
    wrapper: &mut dyn DelRangeExecWrapper,
    job: &Job,
) -> Result<(), DdlPlanError> {
    let mut alloc = ElementIdAlloc::default();
    if let Some(info) = &job.multi_schema_info {
        for (sequence, sub) in info.read().sub_jobs.iter_deref().enumerate() {
            let proxy = sub.read().to_proxy_job(job, sequence as i64);
            if job_need_gc(&proxy) {
                insert_job_into_delete_range_table(wrapper, &proxy, &mut alloc)?;
            }
        }
    } else {
        insert_job_into_delete_range_table(wrapper, job, &mut alloc)?;
    }
    Ok(())
}

fn args_error(error: serde_json::Error) -> DdlPlanError {
    DdlPlanError::Encode(error.to_string())
}

fn missing_args(job: &Job) -> DdlPlanError {
    DdlPlanError::Encode(format!("DDL job {} has no finished arguments", job.id))
}

/// Go `insertJobIntoDeleteRangeTable`.
fn insert_job_into_delete_range_table(
    wrapper: &mut dyn DelRangeExecWrapper,
    job: &Job,
    alloc: &mut ElementIdAlloc,
) -> Result<(), DdlPlanError> {
    wrapper.update_tso_for_job()?;
    let mut job = job.clone();
    let job_id = job.id;
    match job.type_ {
        ActionType::ACTION_DROP_SCHEMA => {
            let args = tidb_model::get_finished_drop_schema_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let table_ids = args.read().all_dropped_table_ids.get().snapshot();
            for batch in table_ids.chunks(BATCH_INSERT_DELETE_RANGE_SIZE) {
                batch_delete_tables_range(wrapper, job_id, batch, alloc)?;
            }
        }
        ActionType::ACTION_DROP_TABLE
        | ActionType::ACTION_DROP_MATERIALIZED_VIEW
        | ActionType::ACTION_DROP_MATERIALIZED_VIEW_LOG
        | ActionType::ACTION_DROP_MATERIALIZED_VIEW_SHADOW => {
            let args = tidb_model::get_finished_drop_table_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let partition_ids = args.read().old_partition_ids.clone();
            if !partition_ids.is_empty() {
                batch_delete_tables_range(wrapper, job_id, &partition_ids, alloc)?;
            }
            // The logical table may hold global index regions.
            batch_delete_tables_range(wrapper, job_id, &[job.table_id], alloc)?;
        }
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW => {
            if job.is_rollback_done() && job.table_id != 0 {
                batch_delete_tables_range(wrapper, job_id, &[job.table_id], alloc)?;
            }
        }
        ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER => {
            let args = tidb_model::get_refresh_materialized_view_complete_out_of_place_cutover_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let args = args.read();
            // Go master: the retired view and its retired log; a cancelled
            // cutover retires only the unused replacement log.
            let table_ids = if job.is_cancelled() {
                vec![args.new_mlog_id]
            } else if args.old_mlog_id != 0 {
                vec![args.old_mview_id, args.old_mlog_id]
            } else {
                vec![args.old_mview_id]
            };
            batch_delete_tables_range(wrapper, job_id, &table_ids, alloc)?;
        }
        ActionType::ACTION_TRUNCATE_TABLE => {
            let args = tidb_model::get_finished_truncate_table_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let partition_ids = args.read().old_partition_ids.get().snapshot();
            if !partition_ids.is_empty() {
                batch_delete_tables_range(wrapper, job_id, &partition_ids, alloc)?;
            }
            batch_delete_tables_range(wrapper, job_id, &[job.table_id], alloc)?;
        }
        ActionType::ACTION_DROP_TABLE_PARTITION => {
            let args = tidb_model::get_finished_table_partition_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let ids = args.read().old_physical_table_ids.get().snapshot();
            batch_delete_tables_range(wrapper, job_id, &ids, alloc)?;
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
                    wrapper,
                    job_id,
                    index.table_id,
                    &[index.index_id],
                    alloc,
                )?;
            }
            let ids = args.old_physical_table_ids.get().snapshot();
            batch_delete_tables_range(wrapper, job_id, &ids, alloc)?;
        }
        ActionType::ACTION_TRUNCATE_TABLE_PARTITION => {
            let args = tidb_model::get_truncate_table_args(&mut job)
                .map_err(args_error)?
                .ok_or_else(|| missing_args(&job))?;
            let ids = args.read().old_partition_ids.get().snapshot();
            batch_delete_tables_range(wrapper, job_id, &ids, alloc)?;
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
                    batch_delete_indices_range(wrapper, job_id, job.table_id, &index_ids, alloc)?;
                } else {
                    for physical_id in &physical_ids {
                        batch_delete_indices_range(
                            wrapper,
                            job_id,
                            *physical_id,
                            &index_ids,
                            alloc,
                        )?;
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
                batch_delete_indices_range(wrapper, job_id, job.table_id, &[index_id], alloc)?;
            } else {
                for physical_id in partition_ids {
                    batch_delete_indices_range(wrapper, job_id, physical_id, &[index_id], alloc)?;
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
                        wrapper,
                        job_id,
                        job.table_id,
                        &args.index_ids,
                        alloc,
                    )?;
                } else {
                    for physical_id in &args.partition_ids {
                        batch_delete_indices_range(
                            wrapper,
                            job_id,
                            *physical_id,
                            &args.index_ids,
                            alloc,
                        )?;
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
                batch_delete_indices_range(wrapper, job_id, job.table_id, &args.index_ids, alloc)?;
            } else {
                for physical_id in &args.partition_ids {
                    batch_delete_indices_range(
                        wrapper,
                        job_id,
                        *physical_id,
                        &args.index_ids,
                        alloc,
                    )?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Go `doBatchDeleteIndiceRange`: one statement for the indexes' ranges.
fn batch_delete_indices_range(
    wrapper: &mut dyn DelRangeExecWrapper,
    job_id: i64,
    table_id: i64,
    index_ids: &[i64],
    alloc: &mut ElementIdAlloc,
) -> Result<(), DdlPlanError> {
    let mut sql = String::from(INSERT_DELETE_RANGE_SQL_PREFIX);
    wrapper.prepare_params_list(index_ids.len() * 5);
    let Some(table_id) = wrapper.rewrite_table_id(table_id) else {
        return Ok(());
    };
    for (i, index_id) in index_ids.iter().enumerate() {
        let start_key = tidb_tablecodec::table_key::encode_table_index_prefix(table_id, *index_id);
        let end_key = tidb_tablecodec::table_key::encode_table_index_prefix(table_id, index_id + 1);
        sql.push_str(INSERT_DELETE_RANGE_SQL_VALUE);
        if i != index_ids.len() - 1 {
            sql.push(',');
        }
        let elem_id = alloc.alloc_for_index_id(table_id, *index_id);
        wrapper.append_params_list(
            job_id,
            elem_id,
            hex_encode(&start_key),
            hex_encode(&end_key),
        );
    }
    wrapper.consume_delete_range(&sql)
}

/// Go `doBatchDeleteTablesRange`: one statement for the tables' ranges.
fn batch_delete_tables_range(
    wrapper: &mut dyn DelRangeExecWrapper,
    job_id: i64,
    table_ids: &[i64],
    alloc: &mut ElementIdAlloc,
) -> Result<(), DdlPlanError> {
    let mut sql = String::from(INSERT_DELETE_RANGE_SQL_PREFIX);
    wrapper.prepare_params_list(table_ids.len() * 5);
    for (i, table_id) in table_ids.iter().enumerate() {
        let Some(table_id) = wrapper.rewrite_table_id(*table_id) else {
            continue;
        };
        let start_key = tidb_tablecodec::table_key::encode_table_prefix(table_id);
        let end_key = tidb_tablecodec::table_key::encode_table_prefix(table_id + 1);
        sql.push_str(INSERT_DELETE_RANGE_SQL_VALUE);
        if i != table_ids.len() - 1 {
            sql.push(',');
        }
        let elem_id = alloc.alloc_for_physical_id(table_id);
        wrapper.append_params_list(
            job_id,
            elem_id,
            hex_encode(&start_key),
            hex_encode(&end_key),
        );
    }
    wrapper.consume_delete_range(&sql)
}

/// Go `DelRangeExecWrapper`: consumes the delete ranges with the provided
/// table ID(s) and index ID(s).
pub trait DelRangeExecWrapper {
    /// Go `UpdateTSOForJob`: a new timestamp for the next job.
    ///
    /// # Errors
    ///
    /// The store's failure to hand out a timestamp.
    fn update_tso_for_job(&mut self) -> Result<(), DdlPlanError>;

    /// Go `PrepareParamsList`.
    fn prepare_params_list(&mut self, size: usize);

    /// Go `RewriteTableID`: `None` skips the table.
    fn rewrite_table_id(&self, table_id: i64) -> Option<i64>;

    /// Go `AppendParamsList`: one row's values, its keys hex-encoded.
    fn append_params_list(&mut self, job_id: i64, elem_id: i64, start_key: String, end_key: String);

    /// Go `ConsumeDeleteRange`: the batch's statement over the listed
    /// values.
    ///
    /// # Errors
    ///
    /// The statement's error.
    fn consume_delete_range(&mut self, sql: &str) -> Result<(), DdlPlanError>;
}

/// Go `sessionDelRangeExecWrapper`: the statements run on a session of the
/// worker's pool, each in its own transaction, stamped with the store's
/// current timestamp.
pub struct SessionDelRangeExecWrapper<'a> {
    sctx: &'a dyn tidb_ddl_build::session_pool::DdlSessionPool,
    ts: u64,
    params_list: Vec<(i64, i64, String, String, u64)>,
}

impl<'a> SessionDelRangeExecWrapper<'a> {
    /// Go `newDelRangeExecWrapper`.
    #[must_use]
    pub fn new(sctx: &'a dyn tidb_ddl_build::session_pool::DdlSessionPool) -> Self {
        Self {
            sctx,
            ts: 0,
            params_list: Vec::new(),
        }
    }
}

impl DelRangeExecWrapper for SessionDelRangeExecWrapper<'_> {
    fn update_tso_for_job(&mut self) -> Result<(), DdlPlanError> {
        // Go `getNowTSO`.
        self.ts = self
            .sctx
            .current_version()
            .map_err(DdlPlanError::from_mysql_error)?;
        Ok(())
    }

    fn prepare_params_list(&mut self, size: usize) {
        self.params_list = Vec::with_capacity(size / 5);
    }

    fn rewrite_table_id(&self, table_id: i64) -> Option<i64> {
        Some(table_id)
    }

    fn append_params_list(
        &mut self,
        job_id: i64,
        elem_id: i64,
        start_key: String,
        end_key: String,
    ) {
        self.params_list
            .push((job_id, elem_id, start_key, end_key, self.ts));
    }

    fn consume_delete_range(&mut self, sql: &str) -> Result<(), DdlPlanError> {
        use tidb_sqlescape::SqlArg;
        let params_list = std::mem::take(&mut self.params_list);
        let args: Vec<SqlArg<'_>> = params_list
            .iter()
            .flat_map(|(job_id, elem_id, start_key, end_key, ts)| {
                [
                    SqlArg::Signed(*job_id),
                    SqlArg::Signed(*elem_id),
                    SqlArg::from(start_key.as_str()),
                    SqlArg::from(end_key.as_str()),
                    SqlArg::Unsigned(*ts),
                ]
            })
            .collect();
        self.sctx
            .exec_restricted_sql_with(sql, &args)
            .map_err(DdlPlanError::from_mysql_error)?;
        Ok(())
    }
}

/// The ranges staged by deterministic snapshot-plan fixtures
/// ([`del_range_rows_mutations`]); live worker steps use their SQL session.
#[derive(Default)]
struct StagedDelRanges {
    ranges: Vec<DelRangeTask>,
}

impl DelRangeExecWrapper for StagedDelRanges {
    fn update_tso_for_job(&mut self) -> Result<(), DdlPlanError> {
        Ok(())
    }

    fn prepare_params_list(&mut self, _size: usize) {}

    fn rewrite_table_id(&self, table_id: i64) -> Option<i64> {
        Some(table_id)
    }

    fn append_params_list(
        &mut self,
        job_id: i64,
        elem_id: i64,
        start_key: String,
        end_key: String,
    ) {
        let decode = |key: String| hex_decode(key.as_bytes()).expect("hex_encode's output");
        self.ranges.push(DelRangeTask {
            job_id,
            element_id: elem_id,
            start_key: decode(start_key),
            end_key: decode(end_key),
        });
    }

    fn consume_delete_range(&mut self, _sql: &str) -> Result<(), DdlPlanError> {
        Ok(())
    }
}

/// Go `pkg/ddl/util`'s `deleteRangesTable`.
const DELETE_RANGES_TABLE: &str = "gc_delete_range";
/// Go `doneDeleteRangesTable`.
const DONE_DELETE_RANGES_TABLE: &str = "gc_delete_range_done";
/// Go `loadDeleteRangeSQL`.
const LOAD_DELETE_RANGE_SQL: &str =
    "SELECT HIGH_PRIORITY job_id, element_id, start_key, end_key FROM mysql.%n WHERE ts < %?";
/// Go `recordDoneDeletedRangeSQL`.
const RECORD_DONE_DELETED_RANGE_SQL: &str = "INSERT IGNORE INTO mysql.gc_delete_range_done SELECT * FROM mysql.gc_delete_range WHERE job_id = %? AND element_id = %?";
/// Go `completeDeleteRangeSQL`.
const COMPLETE_DELETE_RANGE_SQL: &str =
    "DELETE FROM mysql.gc_delete_range WHERE job_id = %? AND element_id = %?";
/// Go `updateDeleteRangeSQL`.
const UPDATE_DELETE_RANGE_SQL: &str = "UPDATE mysql.gc_delete_range SET start_key = %? WHERE job_id = %? AND element_id = %? AND start_key = %?";
/// Go `deleteDoneRecordSQL`.
const DELETE_DONE_RECORD_SQL: &str =
    "DELETE FROM mysql.gc_delete_range_done WHERE job_id = %? AND element_id = %?";

/// Go `kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)`,
/// the context `pkg/ddl/util`'s statements run under.
fn internal_ddl_context() -> impl tidb_sqlexec::ExecutionContext {
    tikv_client::util::with_internal_source_type(
        &tikv_client::trace::TraceContext::new(),
        tidb_txnkv::INTERNAL_TXN_DDL,
    )
}

/// Go `sctx.GetSQLExecutor().ExecuteInternal(ctx, sql, args...)`, its record
/// set drained.
fn execute_internal(
    ctx: &dyn tidb_sqlexec::ExecutionContext,
    sctx: &dyn tidb_sqlexec::SqlExecutor,
    sql: &str,
    args: &[tidb_sqlescape::SqlArg<'_>],
) -> Result<Vec<Vec<Datum>>, tidb_terror::MysqlError> {
    tidb_sqlexec::execute_sql(ctx, sctx, sql, args)
        .map_err(tidb_store_driver_error::sql_query_error::mysql_error_of)
}

/// Go `util.LoadDeleteRanges` loads delete range tasks from gc_delete_range
/// table.
///
/// # Errors
///
/// The statement's error, or a key that is not hex.
pub fn load_delete_ranges(
    ctx: &dyn tidb_sqlexec::ExecutionContext,
    sctx: &dyn tidb_sqlexec::SqlExecutor,
    safe_point: u64,
) -> Result<Vec<DelRangeTask>, tidb_terror::MysqlError> {
    load_delete_ranges_from_table(ctx, sctx, DELETE_RANGES_TABLE, safe_point)
}

/// Go `util.LoadDoneDeleteRanges` loads deleted ranges from
/// gc_delete_range_done table.
///
/// # Errors
///
/// The statement's error, or a key that is not hex.
pub fn load_done_delete_ranges(
    ctx: &dyn tidb_sqlexec::ExecutionContext,
    sctx: &dyn tidb_sqlexec::SqlExecutor,
    safe_point: u64,
) -> Result<Vec<DelRangeTask>, tidb_terror::MysqlError> {
    load_delete_ranges_from_table(ctx, sctx, DONE_DELETE_RANGES_TABLE, safe_point)
}

/// Go `loadDeleteRangesFromTable`.
fn load_delete_ranges_from_table(
    ctx: &dyn tidb_sqlexec::ExecutionContext,
    sctx: &dyn tidb_sqlexec::SqlExecutor,
    table: &str,
    safe_point: u64,
) -> Result<Vec<DelRangeTask>, tidb_terror::MysqlError> {
    use tidb_sqlescape::SqlArg;
    let rows = execute_internal(
        ctx,
        sctx,
        LOAD_DELETE_RANGE_SQL,
        &[SqlArg::from(table), SqlArg::Unsigned(safe_point)],
    )?;
    let int = |datum: &Datum| match datum {
        Datum::Int(value) => *value,
        Datum::UInt(value) => *value as i64,
        _ => 0,
    };
    let key = |datum: &Datum| {
        hex_decode(datum.go_bytes()).ok_or_else(|| {
            tidb_terror::MysqlError::unknown("encoding/hex: invalid byte in a delete range key")
        })
    };
    rows.iter()
        .map(|row| {
            Ok(DelRangeTask {
                job_id: int(&row[0]),
                element_id: int(&row[1]),
                start_key: key(&row[2])?,
                end_key: key(&row[3])?,
            })
        })
        .collect()
}

/// Go `util.CompleteDeleteRange` moves a record from gc_delete_range table
/// to gc_delete_range_done table.
///
/// # Errors
///
/// A statement's error.
pub fn complete_delete_range(
    sctx: &dyn tidb_sqlexec::SqlExecutor,
    dr: &DelRangeTask,
    need_to_record_done: bool,
) -> Result<(), tidb_terror::MysqlError> {
    use tidb_sqlescape::SqlArg;
    let ctx = internal_ddl_context();
    execute_internal(&ctx, sctx, "BEGIN", &[])?;
    if need_to_record_done {
        execute_internal(
            &ctx,
            sctx,
            RECORD_DONE_DELETED_RANGE_SQL,
            &[SqlArg::Signed(dr.job_id), SqlArg::Signed(dr.element_id)],
        )?;
    }
    remove_from_gc_delete_range(sctx, dr.job_id, dr.element_id)?;
    execute_internal(&ctx, sctx, "COMMIT", &[])?;
    Ok(())
}

/// Go `util.RemoveFromGCDeleteRange` is exported for ddl pkg to use.
///
/// # Errors
///
/// The statement's error.
pub fn remove_from_gc_delete_range(
    sctx: &dyn tidb_sqlexec::SqlExecutor,
    job_id: i64,
    element_id: i64,
) -> Result<(), tidb_terror::MysqlError> {
    use tidb_sqlescape::SqlArg;
    execute_internal(
        &internal_ddl_context(),
        sctx,
        COMPLETE_DELETE_RANGE_SQL,
        &[SqlArg::Signed(job_id), SqlArg::Signed(element_id)],
    )?;
    Ok(())
}

/// Go `util.DeleteDoneRecord` removes a record from gc_delete_range_done
/// table.
///
/// # Errors
///
/// The statement's error.
pub fn delete_done_record(
    sctx: &dyn tidb_sqlexec::SqlExecutor,
    dr: &DelRangeTask,
) -> Result<(), tidb_terror::MysqlError> {
    use tidb_sqlescape::SqlArg;
    execute_internal(
        &internal_ddl_context(),
        sctx,
        DELETE_DONE_RECORD_SQL,
        &[SqlArg::Signed(dr.job_id), SqlArg::Signed(dr.element_id)],
    )?;
    Ok(())
}

/// Go `util.UpdateDeleteRange` is only for emulator.
///
/// # Errors
///
/// The statement's error.
pub fn update_delete_range(
    sctx: &dyn tidb_sqlexec::SqlExecutor,
    dr: &DelRangeTask,
    new_start_key: &[u8],
    old_start_key: &[u8],
) -> Result<(), tidb_terror::MysqlError> {
    use tidb_sqlescape::SqlArg;
    let new_start_key_hex = hex_encode(new_start_key);
    let old_start_key_hex = hex_encode(old_start_key);
    execute_internal(
        &internal_ddl_context(),
        sctx,
        UPDATE_DELETE_RANGE_SQL,
        &[
            SqlArg::from(new_start_key_hex.as_str()),
            SqlArg::Signed(dr.job_id),
            SqlArg::Signed(dr.element_id),
            SqlArg::from(old_start_key_hex.as_str()),
        ],
    )?;
    Ok(())
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

/// Go `delRange.addDelRangeJob`: the job's ranges inserted by Go's
/// statements on a session of the worker's pool, each statement in its own
/// transaction -- not the finishing step's.
///
/// # Errors
///
/// The store's failure to hand out a timestamp, or a statement's error.
pub fn add_del_range_job(
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    job: &Job,
) -> Result<(), DdlPlanError> {
    let mut wrapper = SessionDelRangeExecWrapper::new(sess_pool);
    add_del_range_job_internal(&mut wrapper, job)
}

