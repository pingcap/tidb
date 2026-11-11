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

//! Go `pkg/ddl/cluster.go`: FLASHBACK CLUSTER's job (`onFlashbackCluster`,
//! `finishFlashbackCluster`) and the checks and key ranges it runs over.

use super::recover_table::{gc_error, worker_sessions, WorkerGcContext};
use super::*;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

use tidb_ddl_build::cluster::{merge_continuous_key_ranges, KeyRangeMayExclude};
use tidb_ddl_build::session_pool::DdlSessionPool;
use tidb_gcutil::Context as _;
use tidb_model::FlashbackClusterArgs;

/// Go `pdScheduleKey`.
const PD_SCHEDULE_KEY: [&str; 1] = ["merge-schedule-limit"];

/// Go `flashbackMaxBackoff`'s companion `flashbackGetMinSafeTimeTimeout`.
const FLASHBACK_GET_MIN_SAFE_TIME_TIMEOUT: Duration = Duration::from_secs(60);

fn unknown(message: String) -> DdlPlanError {
    DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(message))
}

/// Go's `mockFlashbackTest` failpoint: the store cannot flash back, so the
/// job walks its states without the region RPCs.
fn in_flashback_test() -> bool {
    #[cfg(feature = "failpoints")]
    {
        fail::eval("mockFlashbackTest", |val| val.as_deref() == Some("true")).unwrap_or(false)
    }
    #[cfg(not(feature = "failpoints"))]
    {
        false
    }
}

/// Go's `injectSafeTS` failpoint.
fn inject_safe_ts() -> Option<u64> {
    #[cfg(feature = "failpoints")]
    {
        fail::eval("injectSafeTS", |val| val.and_then(|val| val.parse::<u64>().ok())).flatten()
    }
    #[cfg(not(feature = "failpoints"))]
    {
        None
    }
}

/// Go's `changeFlashbackGetMinSafeTimeTimeout` failpoint (nanoseconds).
fn flashback_get_min_safe_time_timeout() -> Duration {
    #[cfg(feature = "failpoints")]
    {
        fail::eval("changeFlashbackGetMinSafeTimeTimeout", |val| {
            val.and_then(|val| val.parse::<u64>().ok())
        })
        .flatten()
        .map_or(FLASHBACK_GET_MIN_SAFE_TIME_TIMEOUT, Duration::from_nanos)
    }
    #[cfg(not(feature = "failpoints"))]
    {
        FLASHBACK_GET_MIN_SAFE_TIME_TIMEOUT
    }
}

/// Go `closePDSchedule`.
fn close_pd_schedule() -> Result<(), DdlPlanError> {
    let close_map: HashMap<String, serde_json::Value> = PD_SCHEDULE_KEY
        .iter()
        .map(|key| ((*key).to_owned(), serde_json::json!(0)))
        .collect();
    tidb_domain_infosync::schedule_manager::set_pd_schedule_config(&close_map).map_err(unknown)
}

/// Go `savePDSchedule`.
fn save_pd_schedule(args: &mut FlashbackClusterArgs) -> Result<(), DdlPlanError> {
    let ret_value = tidb_domain_infosync::schedule_manager::get_pd_schedule_config().map_err(unknown)?;
    args.pd_schedule_value = Some(
        PD_SCHEDULE_KEY
            .iter()
            .map(|key| {
                (
                    (*key).to_owned(),
                    ret_value.get(*key).cloned().unwrap_or(serde_json::Value::Null),
                )
            })
            .collect(),
    );
    Ok(())
}

/// Go `recoverPDSchedule`.
fn recover_pd_schedule(pd_schedule_param: Option<&BTreeMap<String, serde_json::Value>>) -> Result<(), DdlPlanError> {
    let Some(pd_schedule_param) = pd_schedule_param else {
        return Ok(());
    };
    let config: HashMap<String, serde_json::Value> = pd_schedule_param
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    tidb_domain_infosync::schedule_manager::set_pd_schedule_config(&config).map_err(unknown)
}

/// Go `oracle.GetTimeFromTS(ts)`'s instant, in milliseconds.
fn physical(ts: u64) -> i64 {
    tikv_client::oracle::extract_physical(ts)
}

/// Go `getStoreGlobalMinSafeTS`.
fn get_store_global_min_safe_ts(pool: &dyn DdlSessionPool) -> u64 {
    inject_safe_ts().unwrap_or_else(|| pool.min_safe_ts())
}

/// Go `ValidateFlashbackTS`: the timestamp is not in the future, every store
/// has resolved up to it, and GC has not passed it.
pub(crate) fn validate_flashback_ts(pool: &dyn DdlSessionPool, flashback_ts: u64) -> Result<(), DdlPlanError> {
    let current_ts = pool
        .current_version()
        .map_err(|error| unknown(format!("fail to validate flashback timestamp: {error}")))?;
    if physical(flashback_ts) > physical(current_ts) {
        return Err(unknown("cannot set flashback timestamp to future time".to_owned()));
    }

    let timeout = flashback_get_min_safe_time_timeout();
    let start = Instant::now();
    let mut min_safe_ts = get_store_global_min_safe_ts(pool);
    while physical(flashback_ts) > physical(min_safe_ts) {
        if start.elapsed() >= timeout {
            return Err(unknown(format!(
                "cannot set flashback timestamp after min-resolved-ts({})",
                tidb_go_time_layout::go_unix_milli_string(physical(min_safe_ts))
            )));
        }
        std::thread::sleep(Duration::from_secs(1));
        min_safe_ts = get_store_global_min_safe_ts(pool);
    }

    let mut gc = WorkerGcContext::new(pool);
    let gc_safe_point = tidb_gcutil::get_gc_safe_point(&mut gc).map_err(gc_error)?;
    tidb_gcutil::validate_snapshot_with_gc_safe_point(flashback_ts, gc_safe_point).map_err(|error| {
        gc_error(match error {
            tidb_gcutil::Error::SnapshotTooOld { safe_point_time } => {
                tidb_gcutil::Error::SnapshotTooOld { safe_point_time }
            }
            other => tidb_gcutil::Error::Context(tidb_terror::MysqlError::unknown(other.to_string())),
        })
    })
}

/// Go `getGlobalSysVarAsBool`.
fn get_global_sys_var_as_bool(pool: &dyn DdlSessionPool, name: &str) -> Result<bool, DdlPlanError> {
    let val = WorkerGcContext::new(pool)
        .get_global_sys_var(name)
        .map_err(DdlPlanError::from_mysql_error)?;
    Ok(tidb_vardef::option_values::tidb_opt_on(&val))
}

/// Go `setGlobalSysVarFromBool`.
fn set_global_sys_var_from_bool(pool: &dyn DdlSessionPool, name: &str, value: bool) -> Result<(), DdlPlanError> {
    let sv = if value { tidb_vardef::tidb_vars::ON } else { tidb_vardef::tidb_vars::OFF };
    WorkerGcContext::new(pool)
        .set_global_sys_var(name, sv)
        .map_err(DdlPlanError::from_mysql_error)
}

/// Go `isFlashbackSupportedDDLAction`.
fn is_flashback_supported_ddl_action(action: ActionType) -> bool {
    !matches!(
        action,
        ActionType::ACTION_SET_TI_FLASH_REPLICA
            | ActionType::ACTION_UPDATE_TI_FLASH_REPLICA_STATUS
            | ActionType::ACTION_ALTER_PLACEMENT_POLICY
            | ActionType::ACTION_ALTER_TABLE_PLACEMENT
            | ActionType::ACTION_ALTER_TABLE_PARTITION_PLACEMENT
            | ActionType::ACTION_CREATE_PLACEMENT_POLICY
            | ActionType::ACTION_DROP_PLACEMENT_POLICY
            | ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT
            | ActionType::ACTION_ALTER_TABLE_ATTRIBUTES
            | ActionType::ACTION_ALTER_TABLE_PARTITION_ATTRIBUTES
    )
}

/// Go `checkSystemSchemaID` over the snapshot at the flashback timestamp.
fn check_system_schema_id(
    store: &dyn SnapshotMetaAt,
    flashback_ts: u64,
    schema_id: i64,
    flashback_ts_string: &str,
) -> Result<(), DdlPlanError> {
    if schema_id <= 0 {
        return Ok(());
    }
    let Some(db_info) = store.database_at(flashback_ts, schema_id)? else {
        return Ok(());
    };
    if tidb_filter::is_system_schema(db_info.name.lowercase()) {
        return Err(unknown(format!(
            "Detected modified system table during [{flashback_ts_string}, now), can't do flashback"
        )));
    }
    Ok(())
}

/// Go `oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)`.
fn flashback_ts_string(ts: u64) -> String {
    chrono::DateTime::from_timestamp_millis(physical(ts))
        .expect("a TSO's physical part is a valid instant")
        .with_timezone(&chrono::Local)
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
}

/// Go `checkAndSetFlashbackClusterInfo`.
fn check_and_set_flashback_cluster_info(
    txn: &mut JobTxn<'_>,
    sessions: &DdlStepWorker,
    job_id: i64,
    flashback_ts: u64,
) -> Result<(), DdlPlanError> {
    let pool = sessions.sess_pool.as_ref();
    validate_flashback_ts(pool, flashback_ts)?;

    tidb_gcutil::disable_gc(&mut WorkerGcContext::new(pool)).map_err(gc_error)?;
    close_pd_schedule()?;
    set_global_sys_var_from_bool(pool, tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE, false)?;
    set_global_sys_var_from_bool(pool, tidb_vardef::tidb_vars::TIDB_SUPER_READ_ONLY, true)?;
    set_global_sys_var_from_bool(pool, tidb_vardef::tidb_vars::TIDB_TTL_JOB_ENABLE, false)?;

    let meta = txn.meta();
    let meta_error = |error| DdlPlanError::Meta(job_meta_error(&meta, error));
    let now_schema_version = meta.schema_version().map_err(meta_error)?;

    let store = sessions
        .store
        .as_deref()
        .ok_or_else(|| unknown("the DDL worker has no store to read snapshots from".to_owned()))?;
    let flashback_schema_version = store.schema_version_at(flashback_ts)?;

    let flashback_ts_string = flashback_ts_string(flashback_ts);

    // Check if there is an upgrade during [flashbackTS, now)
    let rows = pool.exec_restricted_sql(&format!(
        "select VARIABLE_VALUE from mysql.tidb as of timestamp '{flashback_ts_string}' where VARIABLE_NAME='tidb_server_version'"
    ));
    let Some(version) = rows
        .ok()
        .and_then(|rows| rows.into_iter().next())
        .and_then(|row| row.into_iter().next())
    else {
        return Err(unknown(
            "Get history `tidb_server_version` failed, can't do flashback".to_owned(),
        ));
    };
    let rows = pool
        .exec_restricted_sql(&format!(
            "select 1 from mysql.tidb where VARIABLE_NAME='tidb_server_version' and VARIABLE_VALUE={}",
            String::from_utf8_lossy(version.go_bytes())
        ))
        .map_err(DdlPlanError::from_mysql_error)?;
    if rows.is_empty() {
        return Err(unknown(format!(
            "Detected TiDB upgrade during [{flashback_ts_string}, now), can't do flashback"
        )));
    }

    // Check is there a DDL task at flashbackTS.
    let rows = pool.exec_restricted_sql(&format!(
        "select count(*) from mysql.tidb_ddl_job as of timestamp '{flashback_ts_string}'"
    ));
    let Some(count) = rows
        .ok()
        .and_then(|rows| rows.into_iter().next())
        .and_then(|row| row.into_iter().next())
    else {
        return Err(unknown("Get history ddl jobs failed, can't do flashback".to_owned()));
    };
    if count != tidb_datatype::Datum::Int(0) {
        return Err(unknown(format!(
            "Detected another DDL job at {flashback_ts_string}, can't do flashback"
        )));
    }

    // If flashbackSchemaVersion not same as nowSchemaVersion, we should
    // check all schema diffs during [flashbackTs, now).
    for version in flashback_schema_version + 1..=now_schema_version {
        let Some(diff) = meta.schema_diff(version).map_err(meta_error)? else {
            continue;
        };
        if !is_flashback_supported_ddl_action(diff.action_type) {
            return Err(unknown(format!(
                "Detected unsupported DDL job type({}) during [{flashback_ts_string}, now), can't do flashback",
                diff.action_type
            )));
        }
        check_system_schema_id(store, flashback_ts, diff.schema_id, &flashback_ts_string)?;
    }

    // Go `GetAllDDLJobs`.
    let rows = pool
        .exec_restricted_sql("select job_meta from mysql.tidb_ddl_job where 1 order by job_id")
        .map_err(DdlPlanError::from_mysql_error)?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut job = Job::default();
        job.decode(row[0].go_bytes())
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
        jobs.push(job);
    }
    // Other ddl jobs in queue, return error.
    if jobs.len() != 1 {
        let other_job = jobs.iter().find(|job| job.id != job_id).map_or(0, |job| job.id);
        return Err(unknown(format!(
            "have other ddl jobs(jobID: {other_job}) in queue, can't do flashback"
        )));
    }
    Ok(())
}

/// Go `addToSlice`.
fn add_to_slice(schema: &str, table_name: &str, table_id: i64, flashback_ids: &mut Vec<i64>) {
    if tidb_filter::is_system_schema(schema) && !table_name.starts_with("stats_") && table_name != "gc_delete_range" {
        flashback_ids.push(table_id);
    }
}

/// Go `getTableDataKeyRanges`: every table data key range but those of
/// `non_flashback_table_ids`.
fn get_table_data_key_ranges(mut non_flashback_table_ids: Vec<i64>) -> Vec<tidb_txnkv::KeyRange> {
    non_flashback_table_ids.push(-1);
    non_flashback_table_ids.sort_unstable();

    let mut key_ranges: Vec<tidb_txnkv::KeyRange> = non_flashback_table_ids
        .windows(2)
        .map(|ids| {
            tidb_txnkv::KeyRange::new(
                tidb_tablecodec::table_key::encode_table_prefix(ids[0] + 1).into(),
                tidb_tablecodec::table_key::encode_table_prefix(ids[1]).into(),
            )
        })
        .collect();

    // Add all other key ranges.
    let last = non_flashback_table_ids[non_flashback_table_ids.len() - 1];
    key_ranges.push(tidb_txnkv::KeyRange::new(
        tidb_tablecodec::table_key::encode_table_prefix(last + 1).into(),
        tidb_tablecodec::table_key::encode_table_prefix(tidb_metadef::MAX_USER_GLOBAL_ID).into(),
    ));
    key_ranges
}

/// Go `getFlashbackKeyRanges`: the meta ranges of every user schema (now or
/// at the flashback timestamp), the schema list, and every table data range
/// outside the system tables that keep their rows.
fn get_flashback_key_ranges(
    catalog: &SessionExtendedInfoSchema,
    store: &dyn SnapshotMetaAt,
    flashback_ts: u64,
) -> Result<Vec<tidb_txnkv::KeyRange>, DdlPlanError> {
    let schemas: Vec<_> = catalog
        .database_names()
        .iter()
        .filter_map(|name| catalog.schema_by_name(name))
        .map(|schema| {
            let schema = schema.read();
            (schema.id, schema.name.lowercase().to_owned())
        })
        .collect();

    // get snapshot schema IDs.
    let snapshot_schemas = store.databases_at(flashback_ts)?;

    let mut schema_ids = BTreeSet::new();
    let mut exclude_schema_ids = BTreeSet::new();
    let all = schemas
        .iter()
        .map(|(id, name)| (*id, name.as_str()))
        .chain(snapshot_schemas.iter().map(|schema| (schema.id, schema.name.lowercase())));
    for (id, name) in all {
        if tidb_filter::is_system_schema(name) {
            exclude_schema_ids.insert(id);
        } else {
            schema_ids.insert(id);
        }
    }

    let schema_key_range = |schema_id: i64, exclude: bool| KeyRangeMayExclude {
        range: tidb_txnkv::KeyRange::new(
            tidb_tablecodec::table_key::encode_meta_key_prefix(&tidb_meta::key::db_key(schema_id)).into(),
            tidb_tablecodec::table_key::encode_meta_key_prefix(&tidb_meta::key::db_key(schema_id + 1)).into(),
        ),
        exclude,
    };
    let mut schema_key_ranges: Vec<KeyRangeMayExclude> = schema_ids
        .iter()
        .map(|id| schema_key_range(*id, false))
        .chain(exclude_schema_ids.iter().map(|id| schema_key_range(*id, true)))
        .collect();
    schema_key_ranges.sort_by(|a, b| a.range.start_key.cmp(&b.range.start_key));

    let mut key_ranges = merge_continuous_key_ranges(&schema_key_ranges);

    let start_key: tidb_txnkv::Key = tidb_tablecodec::table_key::encode_meta_key_prefix(b"DBs").into();
    let end_key = start_key.prefix_next();
    key_ranges.push(tidb_txnkv::KeyRange::new(start_key, end_key));

    let mut non_flashback_table_ids = Vec::new();
    for (_, schema) in &schemas {
        for table_name in catalog.table_names(schema).unwrap_or_default() {
            let Some((_, info)) = catalog.table_info_by_name(schema, &table_name) else {
                continue;
            };
            let info = info.read();
            if !info.is_base_table() || info.id > tidb_metadef::MAX_USER_GLOBAL_ID {
                continue;
            }
            let name = info.name.lowercase();
            add_to_slice(schema, name, info.id, &mut non_flashback_table_ids);
            if let Some(partition) = info.partition.as_ref() {
                partition.read().definitions.with_visible(|definitions| {
                    for definition in definitions {
                        add_to_slice(schema, name, definition.id, &mut non_flashback_table_ids);
                    }
                });
            }
        }
    }

    key_ranges.extend(get_table_data_key_ranges(non_flashback_table_ids));
    Ok(key_ranges)
}

/// Go `updateSchemaVersion` with `SetSchemaDiffForFlashbackCluster`.
fn flashback_schema_version_step(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    mutations: Vec<BufferMutation>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let meta = txn.meta();
    let diff = SchemaDiff {
        action_type: active.job.type_,
        schema_id: active.job.schema_id,
        table_id: -1,
        regenerate_schema_map: active.job.schema_state == SchemaState::PUBLIC,
        ..SchemaDiff::default()
    };
    let (schema_version, diff) = update_schema_version(&meta, active, diff)?;
    let mut write = job_only_write(active.job.id, mutations);
    write.schema_version = schema_version;
    write.diff = diff;
    Ok(args_step(write))
}

fn args_step(write: DdlWrite) -> PersistedDdlActionStep {
    PersistedDdlActionStep {
        write,
        update_raw_args: true,
        run_error: None,
        reorg: None,
    }
}

/// Go `onFlashbackCluster`.
///
/// 1. before lock flashbackClusterJobID, check the PD schedule, GC and the
///    global variables, and record them in the job's arguments.
/// 2. check the flashback timestamp, close GC and the PD schedule, and get
///    the flashback key ranges.
/// 3. lock the related key ranges.
/// 4. send the flashback RPCs.
pub(super) fn on_flashback_cluster(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
    sessions: Option<&DdlStepWorker>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let in_flashback_test = in_flashback_test();
    let sessions = match worker_sessions(sessions) {
        Ok(sessions) => sessions,
        Err(error) => return Err(cancel_ddl_job(active, error)),
    };
    // TODO: Support flashback in unistore.
    if sessions.sess_pool.store_name() != "TiKV" && !in_flashback_test {
        return Err(cancel_ddl_job(
            active,
            unknown("Not support flashback cluster in non-TiKV env".to_owned()),
        ));
    }

    let args = match tidb_model::get_flashback_cluster_args(&mut active.job) {
        Ok(Some(args)) => args,
        Ok(None) => {
            return Err(cancel_ddl_job(
                active,
                DdlPlanError::Encode("FLASHBACK CLUSTER job has nil args".to_owned()),
            ))
        }
        Err(error) => return Err(cancel_ddl_job(active, DdlPlanError::Encode(error.to_string()))),
    };
    let pool = sessions.sess_pool.as_ref();

    match active.job.schema_state {
        // Stage 1, check and set FlashbackClusterJobID, and update job args.
        SchemaState::NONE => {
            let mut saved = args.read().clone();
            let checked = save_pd_schedule(&mut saved).and_then(|()| {
                saved.enable_gc =
                    tidb_gcutil::check_gc_enable(&mut WorkerGcContext::new(pool)).map_err(gc_error)?;
                saved.enable_auto_analyze =
                    get_global_sys_var_as_bool(pool, tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE)?;
                saved.super_read_only = get_global_sys_var_as_bool(pool, tidb_vardef::tidb_vars::TIDB_SUPER_READ_ONLY)?;
                saved.enable_ttl_job = get_global_sys_var_as_bool(pool, tidb_vardef::tidb_vars::TIDB_TTL_JOB_ENABLE)?;
                Ok(())
            });
            // Go writes each value into `args` as it is read, but the job
            // keeps its raw arguments until `FillArgs`.
            checked.map_err(|error| cancel_ddl_job(active, error))?;
            *args.write() = saved;
            active.job.schema_state = SchemaState::DELETE_ONLY;
            Ok(args_step(job_only_write(active.job.id, Vec::new())))
        }
        // Stage 2, check flashbackTS, close GC and PD schedule, get flashback key ranges.
        SchemaState::DELETE_ONLY => {
            let flashback_ts = args.read().flashback_ts;
            check_and_set_flashback_cluster_info(txn, sessions, active.job.id, flashback_ts)
                .map_err(|error| cancel_ddl_job(active, error))?;
            // We should get startTS here to avoid lost startTS when TiDB
            // crashed during send prepare flashback RPC.
            let start_ts = pool
                .current_version()
                .map_err(|error| cancel_ddl_job(active, DdlPlanError::from_mysql_error(error)))?;
            let store = sessions
                .store
                .as_deref()
                .ok_or_else(|| unknown("the DDL worker has no store to read snapshots from".to_owned()))?;
            let key_ranges = get_flashback_key_ranges(catalog, store, flashback_ts)?;
            {
                let mut args = args.write();
                args.start_ts = start_ts;
                args.flashback_key_ranges = Some(
                    key_ranges
                        .into_iter()
                        .map(|key_range| tidb_model::KeyRange {
                            start_key: Some(key_range.start_key.into()),
                            end_key: Some(key_range.end_key.into()),
                        })
                        .collect(),
                );
            }
            active.job.schema_state = SchemaState::WRITE_ONLY;
            flashback_schema_version_step(txn, active, Vec::new())
        }
        // Stage 3, lock related key ranges.
        SchemaState::WRITE_ONLY => {
            // TODO: Support flashback in unistore.
            if in_flashback_test {
                active.job.schema_state = SchemaState::WRITE_REORGANIZATION;
                return flashback_schema_version_step(txn, active, Vec::new());
            }
            let (flashback_ts, start_ts, key_ranges) = flashback_rpc_args(&args.read());
            // Split region by keyRanges, make sure no unrelated key ranges be locked.
            let region_store = flashback_region_store(sessions)?;
            let concurrency = tidb_vardef::get_ddl_flashback_concurrency() as usize;
            split_regions_by_key_ranges(region_store.as_ref(), &key_ranges);
            let mut total_regions = 0_u64;
            for (start_key, end_key) in &key_ranges {
                total_regions += region_store
                    .prepare_flashback_to_version(flashback_ts, start_ts, start_key, end_key, concurrency)
                    .map_err(unknown)?;
            }
            // We should get commitTS here to avoid lost commitTS when TiDB
            // crashed during send flashback RPC.
            let commit_ts = pool.current_version().map_err(DdlPlanError::from_mysql_error)?;
            {
                let mut args = args.write();
                args.locked_region_cnt = total_regions;
                args.commit_ts = commit_ts;
            }
            active.job.schema_state = SchemaState::WRITE_REORGANIZATION;
            Ok(args_step(job_only_write(active.job.id, Vec::new())))
        }
        // Stage 4, get key ranges and send flashback RPC.
        SchemaState::WRITE_REORGANIZATION => {
            let mut mutations = Vec::new();
            // TODO: Support flashback in unistore.
            if in_flashback_test {
                append_schema_change_mutations(
                    txn,
                    catalog,
                    active.job.id,
                    &[(-1, SchemaChangeEvent::flashback_cluster())],
                    &mut mutations,
                    row_ids,
                )?;
                active.job.state = JobState::DONE;
                active.job.schema_state = SchemaState::PUBLIC;
                return Ok(args_step(job_only_write(active.job.id, mutations)));
            }
            let (flashback_ts, start_ts, key_ranges) = flashback_rpc_args(&args.read());
            let commit_ts = args.read().commit_ts;
            let region_store = flashback_region_store(sessions)?;
            let concurrency = tidb_vardef::get_ddl_flashback_concurrency() as usize;
            for (start_key, end_key) in &key_ranges {
                // Use same startTS as prepare phase to simulate 1PC txn.
                region_store
                    .flashback_to_version(flashback_ts, start_ts, commit_ts, start_key, end_key, concurrency)
                    .map_err(unknown)?;
            }
            append_schema_change_mutations(
                txn,
                catalog,
                active.job.id,
                &[(-1, SchemaChangeEvent::flashback_cluster())],
                &mut mutations,
                row_ids,
            )?;
            active.job.state = JobState::DONE;
            active.job.schema_state = SchemaState::PUBLIC;
            flashback_schema_version_step(txn, active, mutations)
        }
        _ => Ok(args_step(job_only_write(active.job.id, Vec::new()))),
    }
}

type FlashbackRanges = Vec<(Vec<u8>, Vec<u8>)>;

fn flashback_rpc_args(args: &FlashbackClusterArgs) -> (u64, u64, FlashbackRanges) {
    let key_ranges = args
        .flashback_key_ranges
        .iter()
        .flatten()
        .map(|key_range| {
            (
                key_range.start_key.clone().unwrap_or_default(),
                key_range.end_key.clone().unwrap_or_default(),
            )
        })
        .collect();
    (args.flashback_ts, args.start_ts, key_ranges)
}

fn flashback_region_store(
    sessions: &DdlStepWorker,
) -> Result<std::sync::Arc<dyn tidb_kv::region_map::RegionStore>, DdlPlanError> {
    sessions
        .region_store
        .clone()
        .ok_or_else(|| unknown("the DDL worker has no region store to flash back through".to_owned()))
}

/// Go `splitRegionsByKeyRanges`: the regions are split at both ends of every
/// range so no unrelated key range is locked; a failed split is retried.
fn split_regions_by_key_ranges(region_store: &dyn tidb_kv::region_map::RegionStore, key_ranges: &FlashbackRanges) {
    for (start_key, end_key) in key_ranges {
        // tableID is useless when scatter == false
        while region_store
            .split_regions(vec![start_key.clone(), end_key.clone()], false, None)
            .1
            .is_some()
        {}
    }
}

/// Go `finishFlashbackCluster`: whatever the job closed is restored, however
/// the job ended.
pub(super) fn finish_flashback_cluster(job: &mut Job, sessions: Option<&DdlStepWorker>) -> Result<(), DdlPlanError> {
    if job.type_ != ActionType::ACTION_FLASHBACK_CLUSTER {
        return Ok(());
    }
    // Didn't do anything during flashback, return directly
    if job.schema_state == SchemaState::NONE {
        return Ok(());
    }
    let args = tidb_model::get_flashback_cluster_args(job)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?
        .ok_or_else(|| DdlPlanError::Encode("FLASHBACK CLUSTER job has nil args".to_owned()))?;
    let args = args.read().clone();
    let pool = worker_sessions(sessions)?.sess_pool.as_ref();

    recover_pd_schedule(args.pd_schedule_value.as_ref())?;
    if args.enable_gc {
        tidb_gcutil::enable_gc(&mut WorkerGcContext::new(pool)).map_err(gc_error)?;
    }
    set_global_sys_var_from_bool(pool, tidb_vardef::tidb_vars::TIDB_SUPER_READ_ONLY, args.super_read_only)?;
    if job.is_cancelled() {
        // only restore `tidb_ttl_job_enable` when flashback failed
        set_global_sys_var_from_bool(pool, tidb_vardef::tidb_vars::TIDB_TTL_JOB_ENABLE, args.enable_ttl_job)?;
    }
    set_global_sys_var_from_bool(pool, tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE, args.enable_auto_analyze)
}

/// Go `executor.FlashbackCluster` from its job to `doDDLJob2`. `None` for
/// any other statement.
pub fn flashback_cluster<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<add_index::AddIndexSubmission>, DdlPlanError> {
    let DdlStatement::FlashbackCluster { flashback_ts } = statement else {
        return Ok(None);
    };
    let mut job = session_job(job_context, ActionType::ACTION_FLASHBACK_CLUSTER);
    // FLASHBACK CLUSTER affects all schemas and tables.
    job.involving_schema_info = GoSharedSlice::from_vec(vec![tidb_model::InvolvingSchemaInfo {
        database: tidb_model::job::INVOLVING_ALL.into(),
        table: tidb_model::job::INVOLVING_ALL.into(),
        ..tidb_model::InvolvingSchemaInfo::default()
    }]);
    // The value for global variables is meaningless, it will cover during
    // flashback cluster.
    let args = FlashbackClusterArgs {
        flashback_ts: *flashback_ts,
        pd_schedule_value: Some(BTreeMap::new()),
        enable_gc: true,
        enable_auto_analyze: true,
        enable_ttl_job: true,
        ..FlashbackClusterArgs::default()
    };
    let built = add_index::AddIndexSubmission::Job(
        Box::new(tidb_ddl_jobsubmit::JobSpec {
            job,
            args: JobArgsValue::FlashbackCluster(Some(GoShared::new(args))),
            id_allocated: true,
        }),
        Vec::new(),
    );
    submit_built_job(snapshot, Some(built), start_ts, upgrading)
}
