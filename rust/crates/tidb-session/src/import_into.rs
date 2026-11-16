//! Go `pkg/executor/import_into.go` (`ImportIntoExec`, `ImportIntoActionExec`)
//! and the IMPORT halves of `pkg/executor/show.go` (`fetchShowImportJobs`,
//! `fetchShowImportGroups`, `FillOneImportJobInfo`).
//!
//! These live in the session rather than in `tidb_executor`: they drive
//! `tidb_dxf_importinto`, which depends on `tidb_executor::importer`.

use tidb_datatype::Datum;
use tidb_dxf_importinto::runtime_info::RuntimeInfo;
use tidb_executor::importer::job::{self as importer_job, JobInfo};
use tidb_executor::importer::precheck::PrecheckSession;
use tidb_executor::importer::{ASTArgs, ImportSessionInfo, LoadDataController, Plan as ImportPlan};
use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_gocontext::Context;

use crate::*;

/// A failure of `tidb_dxf_importinto`, as the statement's error.
fn import_error(error: &tidb_dxf_importinto::Error) -> DriverError {
    if let Some(terror) = error.downcast_ref::<tidb_terror::TerrorError>() {
        return DriverError::from(terror.clone());
    }
    if let Some(mysql) = error.downcast_ref::<tidb_terror::MysqlError>() {
        return DriverError::from(mysql.clone());
    }
    DriverError::from(tidb_terror::MysqlError::unknown(error.to_string()))
}

fn other(error: impl std::fmt::Display) -> DriverError {
    DriverError::from(tidb_terror::MysqlError::unknown(error.to_string()))
}

/// Go `taskManager.WithNewSession(func(se) { ... })`, keeping the
/// closure's own error.
fn with_task_manager_session<T>(
    f: impl FnOnce(&dyn tidb_dxf_storage::SessionContext) -> Result<T, DriverError>,
) -> Result<T, DriverError> {
    let manager = tidb_dxf_storage::get_task_manager().map_err(other)?;
    let mut f = Some(f);
    let mut out: Option<Result<T, DriverError>> = None;
    let result = manager.with_new_session(&mut |se| {
        let f = f.take().expect("WithNewSession runs its closure once");
        let r = f(se);
        let failed = r.as_ref().err().map(ToString::to_string);
        out = Some(r);
        failed.map_or(Ok(()), |text| Err(tidb_dxf_storage::StorageError::Other(text)))
    });
    match out {
        Some(r) => r,
        None => Err(other(result.err().map_or_else(String::new, |e| e.to_string()))),
    }
}

/// Go `timeutil.ZoneName`.
fn zone_name(zone: &tidb_datatype::SessionTimeZone) -> String {
    let (name, offset) = zone.dag_zone();
    if !name.is_empty() {
        return name;
    }
    let (sign, offset) = if offset < 0 { ('-', -offset) } else { ('+', offset) };
    let (hours, minutes, seconds) = (offset / 3600, offset % 3600 / 60, offset % 60);
    if seconds != 0 {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}")
    }
}

/// Go `convertToMySQLTime`: a DATETIME at second precision in `zone`.
fn to_mysql_time(t: chrono::DateTime<chrono::FixedOffset>, zone: &tidb_datatype::SessionTimeZone) -> Datum {
    use chrono::Timelike;
    let t = t.with_nanosecond(0).unwrap_or(t).with_timezone(zone);
    tidb_datatype::Time::new(tidb_datatype::core_time_from_datetime(t), tidb_datatype::TimeType::DateTime, 0)
        .map_or(Datum::Null, Datum::Time)
}

fn time_or_null(t: Option<&tidb_datatype::Time>) -> Datum {
    t.filter(|t| !t.is_zero()).map_or(Datum::Null, |t| Datum::Time(*t))
}

/// Go `utils.EncloseName`.
fn enclose_name(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Go `FillOneImportJobInfo`.
pub(crate) fn fill_one_import_job_info(
    info: &JobInfo,
    run_info: Option<&RuntimeInfo>,
    zone: &tidb_datatype::SessionTimeZone,
) -> Vec<Datum> {
    let s = |v: &str| Datum::new_string_datum(v.to_owned());
    let mut row = Vec::with_capacity(21);
    row.push(Datum::Int(info.id));
    row.push(if info.group_key.is_empty() { Datum::Null } else { s(&info.group_key) });
    row.push(s(&info.parameters.file_location));
    row.push(s(&format!("{}.{}", enclose_name(&info.table_schema), enclose_name(&info.table_name))));
    row.push(Datum::Int(info.table_id));
    row.push(s(&info.step));
    row.push(s(&info.status));
    row.push(if info.is_source_file_size_unknown() {
        s("N/A")
    } else {
        s(&tidb_config_configtypes::bytes_size(info.source_file_size as f64))
    });
    let summary = info.summary.clone().unwrap_or_default();
    row.push(match run_info {
        Some(ri) => Datum::UInt(ri.import_rows as u64),
        None if info.is_success() => Datum::UInt(summary.imported_rows as u64),
        None => Datum::Null,
    });
    if info.is_success() {
        let mut items = Vec::new();
        if summary.conflict_row_cnt > 0 {
            items.push(format!("{} conflicted rows.", summary.conflict_row_cnt));
        }
        if summary.too_many_conflicts {
            items.push("Too many conflicted rows, checksum skipped.".to_owned());
        }
        row.push(s(&items.join(" ")));
    } else {
        row.push(s(&info.error_message));
    }
    row.push(info.create_time.map_or(Datum::Null, Datum::Time));
    row.push(time_or_null(info.start_time.as_ref()));
    row.push(time_or_null(info.end_time.as_ref()));
    row.push(s(&info.created_by));
    let Some(ri) = run_info else {
        row.push(time_or_null(info.end_time.as_ref()));
        row.extend(std::iter::repeat_n(Datum::Null, 6));
        return row;
    };
    row.push(match ri.update_time {
        Some(t) => to_mysql_time(t, zone),
        None => time_or_null(info.update_time.as_ref()),
    });
    row.push(s(&tidb_dxf::step_2_str(&tidb_dxf::IMPORT_INTO, ri.step)));
    row.push(s(&ri.processed_size()));
    row.push(s(&ri.total_size()));
    row.push(s(&ri.percent()));
    row.push(s(&ri.speed_str()));
    row.push(s(&ri.eta()));
    row
}

/// Go `handleImportJobInfo`: a running job's row carries its task's
/// progress.
fn handle_import_job_info(
    ctx: &Context,
    mut info: JobInfo,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Vec<Datum>, DriverError> {
    let mut run_info = None;
    if info.status == importer_job::JOB_STATUS_RUNNING {
        let ri = tidb_dxf_importinto::runtime_info::get_runtime_info_for_job(ctx, info.id)
            .map_err(|e| import_error(&e))?;
        if ri.status == tidb_dxf::TASK_STATE_AWAITING_RESOLUTION {
            info.status = ri.status.to_string();
            info.error_message = ri.error_msg.clone();
        }
        run_info = Some(ri);
    }
    Ok(fill_one_import_job_info(&info, run_info.as_ref(), zone))
}
/// Go `GetTargetNodeCPUCnt`.
fn get_target_node_cpu_cnt(ctx: &Context, from_query: bool, path: &str, enable_dist_task: bool) -> Result<usize, DriverError> {
    if from_query {
        return Ok(tidb_cpu::get_cpu_count());
    }
    let u = tidb_objstore::parse_raw_url(path).map_err(|e| {
        DriverError::from(tidb_exeerrors::ERR_LOAD_DATA_INVALID_URI.gen_with_stack_by_args(&[
            "data source".into(),
            e.to_string().into(),
        ]))
    })?;
    if tidb_objstore::is_local(&u) || !enable_dist_task {
        return Ok(tidb_cpu::get_cpu_count());
    }
    let cnt = tidb_dxf_handle::get_cpu_count_of_node(ctx).map_err(other)?;
    Ok(usize::try_from(cnt).unwrap_or(0))
}

/// The columns of a planned statement's result schema.
fn plan_columns(
    names: &[tidb_datatype::FieldName],
    schema: &tidb_expr::schema::Schema,
) -> Vec<(String, tidb_datatype::FieldType)> {
    names
        .iter()
        .zip(&schema.columns)
        .map(|(name, column)| {
            (
                name.names.column.original.clone(),
                column.ret_type.clone().unwrap_or_else(|| tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Varchar)),
            )
        })
        .collect()
}

impl Session {
    /// Go `User.String()`.
    fn import_user(&self) -> String {
        self.current_identity().map_or_else(String::new, |(user, host)| format!("{user}@{host}"))
    }

    /// Go `pm.RequestVerification(..., mysql.SuperPriv)`.
    fn import_has_super_priv(&self) -> bool {
        self.has_any_scoped_privilege("", "", tidb_mysql::privilege::SuperPriv.0)
    }

    /// What Go `NewImportPlanFromInput` reads from the user's session.
    fn import_session_info(&self) -> ImportSessionInfo {
        let system = |name: &str| self.vars.get_system(name).ok();
        ImportSessionInfo {
            sql_mode: self.vars.sql_mode().0,
            important_sys_vars: tidb_executor::importer::get_important_sys_vars(system),
            location_id: zone_name(&self.vars.session_time_zone()),
            distsql_scan_concurrency: system(tidb_vardef::tidb_vars::TIDB_DIST_SQL_SCAN_CONCURRENCY)
                .and_then(|v| v.parse().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_DIST_SQL_SCAN_CONCURRENCY as usize),
            user: self.import_user(),
            keyspace: tidb_config::config_tree::config::get_global_keyspace_name(),
            new_collation: tidb_datatype::new_collation_enabled(),
            max_dist_task_nodes: system(tidb_vardef::tidb_vars::TIDB_MAX_DIST_TASK_NODES),
            max_node_count_by_stores: tidb_dxf_importinto::import_store().map_or(0, |s| s.max_node_count_by_stores()),
        }
    }

    /// Go `vardef.EnableDistTask.Load()`.
    fn enable_dist_task(&self) -> bool {
        self.vars
            .get_global(tidb_vardef::tidb_vars::TIDB_ENABLE_DIST_TASK)
            .map_or(tidb_vardef::defaults::DEF_TIDB_ENABLE_DIST_TASK, |v| v.eq_ignore_ascii_case("ON") || v == "1")
    }

    /// Go `ImportIntoExec.Next`.
    pub(crate) fn import_into_exec(
        &mut self,
        plan: &tidb_planner::common_plans::ImportInto,
        stmt: &tidb_ast::ImportIntoStmt,
    ) -> Result<StmtOutput, DriverError> {
        let ctx = tidb_dxf_storage::with_internal_source_type(&Context::background(), tidb_txnkv::INTERNAL_IMPORT_INTO);
        let from_select = plan.select_plan.is_some();
        let table_info = plan
            .table
            .table_info
            .as_ref()
            .map(|t| t.read().clone())
            .ok_or_else(|| other("import preparation requires target table metadata"))?;
        let (db_id, db_name) = plan
            .table
            .db_info
            .as_ref()
            .map(|d| {
                let d = d.read();
                (d.id, d.name.lowercase().to_owned())
            })
            .ok_or_else(|| other("import preparation requires target database metadata"))?;
        let enable_dist_task = self.enable_dist_task();
        let cpu = get_target_node_cpu_cnt(&ctx, from_select, &plan.path, enable_dist_task)?;
        let cluster_id = tidb_dxf_importinto::import_store().ok().and_then(|s| s.cluster_id());
        let cloud_uri = tidb_dxf_importinto::job::get_cloud_storage_uri(cluster_id);
        let import_plan = ImportPlan::new_import_plan_from_input(
            stmt,
            &db_name,
            db_id,
            table_info.clone(),
            &self.import_session_info(),
            cpu,
            &cloud_uri,
        )?;
        // The conflict collection and resolution steps that `capture` needs
        // are not ported yet.
        if !from_select && import_plan.is_global_sort() && import_plan.get_on_dup_key_mode() == "capture" {
            return Err(DriverError::unsupported("IMPORT INTO with on_duplicate_key='capture' is not supported yet"));
        }
        let new_collate = import_plan.get_use_new_collate_or_default(tidb_datatype::new_collation_enabled());
        let table = tidb_table::table_from_meta::table_from_meta(
            &table_info,
            &tidb_table::table_from_meta::AutoIdSource::Unavailable,
        )
        .map_err(other)?
        .with_new_collation_mode(new_collate);
        let mut controller = LoadDataController::new(import_plan, table, ASTArgs::from_import_plan(plan))?;
        if from_select {
            // Go `runsQueryOnWorker`: a next-gen kernel runs the SELECT on a
            // DXF worker, which `prepareQuery` only allows during
            // materialized-view maintenance; this port has no materialized
            // views, so it always refuses.
            if tidb_config_kerneltype::is_next_gen() {
                return Err(DriverError::from(tidb_plannererrors::ERR_NOT_SUPPORTED_YET.gen_with_stack_by_args(&[
                    "IMPORT INTO FROM SELECT outside materialized view maintenance".into(),
                ])));
            }
            return self.import_from_select(&ctx, plan, controller);
        }
        let result = self.import_into_with_controller(&ctx, plan, &mut controller, enable_dist_task);
        controller.close();
        let row = result?;
        Ok(StmtOutput::Rows { columns: plan_columns(&plan.names, &plan.schema), rows: row.into_iter().collect() })
    }

    fn import_into_with_controller(
        &mut self,
        ctx: &Context,
        plan: &tidb_planner::common_plans::ImportInto,
        controller: &mut LoadDataController,
        enable_dist_task: bool,
    ) -> Result<Option<Vec<Datum>>, DriverError> {
        controller.init_data_files(ctx)?;
        let store = tidb_dxf_importinto::import_store().map_err(|e| import_error(&e))?;
        // Go `CreateSession(userSctx)`: the prechecks read the system tables
        // with an internal session.
        let etcd = store.etcd();
        with_task_manager_session(|se| {
            let conn = se.sql_executor();
            let pre = PrecheckSession { ctx, conn: conn.as_ref(), etcd: etcd.as_deref() };
            controller.check_requirements(ctx, &pre)?;
            // Go `InitTiKVConfigs`.
            controller.plan.is_raft_kv2 =
                tidb_dxf_importinto::job::is_raft_kv2(ctx, se).map_err(|e| import_error(&e))?;
            Ok(())
        })?;
        let (job_id, task) = self.submit_import_task(ctx, plan, controller, enable_dist_task)?;
        if !controller.plan.detached {
            self.wait_import_task(job_id, &task)?;
        }
        // Go `fillJobInfo`.
        let user = self.import_user();
        let info = with_task_manager_session(|se| {
            importer_job::get_job(ctx, se.sql_executor().as_ref(), job_id, &user, false)
        })?;
        Ok(Some(fill_one_import_job_info(&info, None, &self.vars.session_time_zone())))
    }

    /// Go `ImportIntoExec.importFromSelect`: the SELECT's rows are encoded
    /// and ingested on this node through a local backend, without DXF; the
    /// statement answers the imported rows as affected rows.
    fn import_from_select(
        &mut self,
        ctx: &Context,
        plan: &tidb_planner::common_plans::ImportInto,
        mut controller: LoadDataController,
    ) -> Result<StmtOutput, DriverError> {
        let store = tidb_dxf_importinto::import_store().map_err(|e| import_error(&e))?;
        // Go `CreateSession(userSctx)`: the prechecks and `InitTiKVConfigs`
        // run on an internal session.
        let etcd = store.etcd();
        with_task_manager_session(|se| {
            let conn = se.sql_executor();
            let pre = PrecheckSession { ctx, conn: conn.as_ref(), etcd: etcd.as_deref() };
            controller.check_requirements(ctx, &pre)?;
            controller.plan.is_raft_kv2 =
                tidb_dxf_importinto::job::is_raft_kv2(ctx, se).map_err(|e| import_error(&e))?;
            Ok(())
        })?;
        let import_id = uuid::Uuid::new_v4().to_string();
        let table_id = controller.plan.table_info.as_ref().map_or(0, |t| t.id);
        let import_plan = controller.plan.clone();
        let client = store.import_client().map_err(other)?;
        let ti = tidb_executor::importer::table_import::TableImporter::new(controller, &import_id, client, store.keyspace())
            .map_err(other)?;
        let Some(tidb_planner::common_plans::Plan::Physical(physical)) = plan.select_plan.as_deref() else {
            ti.close();
            return Err(other("IMPORT INTO ... FROM SELECT has no physical SELECT plan"));
        };
        let mut physical = physical.clone();
        let (chunk_tx, chunk_rx) = crossbeam_channel::bounded::<tidb_executor::importer::chunk_process::QueryChunk>(1);
        let (import_ctx, cancel) = Context::with_cancel(ctx);
        let stmt_ctx = self.statement_context(false);
        // Go's errgroup: one goroutine imports, one sends the SELECT's
        // chunks (`sendQueryChunks`); the first error cancels the other.
        let (imported, sent) = std::thread::scope(|scope| {
            let importer = scope.spawn(|| {
                let result = ti.import_selected_rows(&import_ctx, &chunk_rx);
                if result.is_err() {
                    cancel.cancel();
                }
                result
            });
            let sent = self.with_planning_catalog(|catalog| {
                let mut source = tidb_executor::ImportSelectSource::open(&mut physical, catalog, &stmt_ctx)?;
                let mut row_id = 0i64;
                while let Some(rows) = source.next_chunk()? {
                    let count = rows.len() as i64;
                    let mut chunk = tidb_executor::importer::chunk_process::QueryChunk { rows, row_id_offset: row_id };
                    loop {
                        if let Some(e) = import_ctx.err() {
                            return Err(other(e));
                        }
                        match chunk_tx.send_timeout(chunk, std::time::Duration::from_millis(50)) {
                            Ok(()) => break,
                            Err(crossbeam_channel::SendTimeoutError::Timeout(back)) => chunk = back,
                            Err(crossbeam_channel::SendTimeoutError::Disconnected(_)) => return Err(other("context canceled")),
                        }
                    }
                    row_id += count;
                }
                Ok(())
            });
            if sent.is_err() {
                cancel.cancel();
            }
            drop(chunk_tx);
            (importer.join().unwrap_or_else(|_| Err("import worker panicked".to_owned().into())), sent)
        });
        self.drain_eval_warnings(&stmt_ctx);
        let result = match (imported, sent) {
            (Err(e), _) => Err(import_error(&e.0)),
            (Ok(_), Err(e)) => Err(e),
            (Ok(imported), Ok(())) => tidb_dxf_importinto::task_executor::post_process_with_new_session(
                ctx,
                &import_plan,
                &imported.max_ids,
                &imported.checksum,
            )
            .map(|()| imported.data_kv_count)
            .map_err(|e| import_error(&e)),
        };
        ti.close();
        let imported_rows = result?;
        // Go `FlushTableStats`: the rows reach `mysql.stats_meta` through
        // the stats collector.
        if let Some(collector) = &self.stats_collector {
            if table_id > 0 {
                collector.update(table_id, imported_rows, imported_rows);
            }
        }
        self.statement_message = format!("Records: {imported_rows}, ID: {import_id}");
        Ok(StmtOutput::Affected(u64::try_from(imported_rows).unwrap_or_default()))
    }

    /// Go `ImportIntoExec.submitTask`.
    fn submit_import_task(
        &self,
        ctx: &Context,
        plan: &tidb_planner::common_plans::ImportInto,
        controller: &LoadDataController,
        enable_dist_task: bool,
    ) -> Result<(i64, tidb_dxf::TaskBase), DriverError> {
        let submitted = if controller.plan.data_source_type == "file"
            && tidb_objstore::is_local_path(&controller.plan.path).map_err(|e| {
                DriverError::from(tidb_exeerrors::ERR_LOAD_DATA_INVALID_URI.gen_with_stack_by_args(&[
                    "data source".into(),
                    e.into(),
                ]))
            })? {
            let chunk_map = controller.populate_chunks(ctx).map_err(other)?;
            tidb_dxf_importinto::job::submit_standalone_task(ctx, &controller.plan, &plan.stmt, Some(chunk_map))
        } else if enable_dist_task {
            tidb_dxf_importinto::job::submit_task(ctx, &controller.plan, &plan.stmt)
        } else {
            tidb_dxf_importinto::job::submit_standalone_task(ctx, &controller.plan, &plan.stmt, None)
        };
        submitted.map_err(|e| import_error(&e))
    }

    /// Go `ImportIntoExec.waitTask`: a killed statement cancels the job and
    /// waits for it.
    fn wait_import_task(&self, job_id: i64, task: &tidb_dxf::TaskBase) -> Result<(), DriverError> {
        let (wait_ctx, cancel) = Context::with_cancel(&Context::background());
        let kill = self.sql_killer().get_kill_event_chan();
        let (stop, stopped) = crossbeam_channel::bounded::<()>(0);
        let watcher = std::thread::spawn(move || {
            crossbeam_channel::select! {
                recv(kill) -> _ => cancel.cancel(),
                recv(stopped) -> _ => {}
            }
        });
        let result = tidb_dxf_handle::wait_task_done_or_paused(&wait_ctx, task.id);
        drop(stop);
        let _ = watcher.join();
        match result {
            Err(tidb_dxf_handle::HandleError::Context(tidb_gocontext::CtxError::Canceled)) => {
                tidb_dxf_importinto::job::cancel_and_wait_import_job(&Context::background(), job_id)
                    .map_err(|e| import_error(&e))
            }
            Err(tidb_dxf_handle::HandleError::Task(e)) => Err(import_error(&e)),
            Err(e) => Err(other(e)),
            Ok(()) => Ok(()),
        }
    }

    /// Go `ImportIntoActionExec.Next`: CANCEL IMPORT JOB.
    pub(crate) fn cancel_import_job(&self, job_id: i64) -> Result<StmtOutput, DriverError> {
        let ctx = tidb_dxf_storage::with_internal_source_type(&Context::background(), tidb_txnkv::INTERNAL_DIST_TASK);
        let (user, has_super) = (self.import_user(), self.import_has_super_priv());
        // Go `checkPrivilegeAndStatus`.
        let info = with_task_manager_session(|se| {
            importer_job::get_job(&ctx, se.sql_executor().as_ref(), job_id, &user, has_super)
        })?;
        if !info.can_cancel() {
            return Err(DriverError::from(
                tidb_exeerrors::ERR_LOAD_DATA_INVALID_OPERATION.gen_with_stack_by_args(&["CANCEL".into()]),
            ));
        }
        tidb_dxf_importinto::job::cancel_and_wait_import_job(&ctx, job_id).map_err(|e| import_error(&e))?;
        Ok(StmtOutput::Affected(0))
    }

    /// Go `ShowExec.fetchShowImportJobs`.
    pub(crate) fn show_import_jobs_rows(&self, job_id: Option<i64>) -> Result<Vec<Vec<Datum>>, DriverError> {
        let ctx = tidb_dxf_storage::with_internal_source_type(&Context::background(), tidb_txnkv::INTERNAL_DIST_TASK);
        let (user, has_super) = (self.import_user(), self.import_has_super_priv());
        let zone = self.vars.session_time_zone();
        let infos = with_task_manager_session(|se| {
            let conn = se.sql_executor();
            match job_id {
                Some(id) => importer_job::get_job(&ctx, conn.as_ref(), id, &user, has_super).map(|i| vec![i]),
                None => importer_job::get_all_viewable_jobs(&ctx, conn.as_ref(), &user, has_super),
            }
        })?;
        infos.into_iter().map(|info| handle_import_job_info(&ctx, info, &zone)).collect()
    }

    /// Go `ShowExec.fetchShowImportGroups`.
    pub(crate) fn show_import_groups_rows(&self, group_key: &str) -> Result<Vec<Vec<Datum>>, DriverError> {
        let ctx = tidb_dxf_storage::with_internal_source_type(&Context::background(), tidb_txnkv::INTERNAL_DIST_TASK);
        let (user, has_super) = (self.import_user(), self.import_has_super_priv());
        let zone = self.vars.session_time_zone();
        let infos = with_task_manager_session(|se| {
            importer_job::get_jobs_by_group_key(&ctx, se.sql_executor().as_ref(), &user, group_key, has_super)
        })?;
        #[derive(Default)]
        struct GroupInfo {
            create_time: Option<tidb_datatype::Time>,
            update_time: Option<tidb_datatype::Time>,
            counts: [i64; 6],
        }
        let mut groups: std::collections::BTreeMap<String, GroupInfo> = std::collections::BTreeMap::new();
        let mut order = Vec::new();
        for info in infos {
            if !groups.contains_key(&info.group_key) {
                order.push(info.group_key.clone());
            }
            let mut update_time = None;
            if info.status == importer_job::JOB_STATUS_RUNNING {
                let ri = tidb_dxf_importinto::runtime_info::get_runtime_info_for_job(&ctx, info.id)
                    .map_err(|e| import_error(&e))?;
                update_time = match ri.update_time {
                    Some(t) => match to_mysql_time(t, &zone) {
                        Datum::Time(t) => Some(t),
                        _ => None,
                    },
                    None => info.update_time.filter(|t| !t.is_zero()),
                };
            }
            let g = groups.entry(info.group_key.clone()).or_default();
            if let Some(ct) = info.create_time.filter(|t| !t.is_zero()) {
                if g.create_time.is_none_or(|c| ct.compare(c).is_lt()) {
                    g.create_time = Some(ct);
                }
            }
            if let Some(ut) = update_time {
                if g.update_time.is_none_or(|u| ut.compare(u).is_gt()) {
                    g.update_time = Some(ut);
                }
            }
            g.counts[0] += 1;
            let slot = match info.status.as_str() {
                "pending" => 1,
                "running" => 2,
                "finished" => 3,
                "failed" => 4,
                "cancelled" => 5,
                _ => continue,
            };
            g.counts[slot] += 1;
        }
        Ok(order
            .into_iter()
            .map(|key| {
                let g = &groups[&key];
                let mut row = vec![Datum::new_string_datum(key.clone())];
                row.extend(g.counts.iter().map(|c| Datum::Int(*c)));
                row.push(time_or_null(g.create_time.as_ref()));
                row.push(time_or_null(g.update_time.as_ref()));
                row
            })
            .collect())
    }
}