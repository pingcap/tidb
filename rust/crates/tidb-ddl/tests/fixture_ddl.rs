// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go's DDL executor over the fixture meta store: the statement methods are
//! the server's (`tidb_ddl::executor`, reached through `ClusterDdl::execute_ddl`
//! as `DDLExec.Next` reaches them), with `DoDDLJobWrapper` either running the
//! job's steps to completion in-process or handing the built job back.

use std::sync::Mutex;

use tidb_ddl::cluster_ddl::add_index::AddIndexSubmission;
use tidb_ddl::cluster_ddl::{DdlContext, DdlWrite};
use tidb_ddl::real_tikv_ddl::ClusterDdlReport;
use tidb_ddl::{ClusterDdl, ExecutorStore};
use tidb_ddl_build::plan_error::DdlPlanError;
use tidb_ddl_build::session_context::DdlSessionContext;
use tidb_ddl_jobsubmit::{plan_insert_attempt, JobSpec};
use tidb_infoschema::cluster_catalog::load_info_schema;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

use crate::cluster_ddl_source::{apply, run_inserted_job, MetaStore};

/// What a fixture DDL statement came to.
#[derive(Clone, Debug)]
pub(crate) enum DdlPlan {
    /// Nothing to change (`IF [NOT] EXISTS` satisfied, a no-op option): no
    /// job runs and no schema version is spent.
    AlreadySatisfied {
        /// What was already true.
        detail: String,
        /// The warnings the statement raises anyway.
        warnings: Vec<tidb_context::SqlWarn>,
    },
    /// The job's committed writes, in order.
    Write(Box<DdlWrite>),
}

/// One DDL statement as a session issues it: parsed, with the session's
/// current database and statement context.
pub(crate) struct Ddl {
    pub(crate) stmt: tidb_ast::Stmt,
    pub(crate) current_db: String,
    pub(crate) context: DdlContext,
}

impl Ddl {
    /// The parsed `stmt` in `current_db` under `context`.
    pub(crate) fn of(
        stmt: &tidb_ast::Stmt,
        current_db: &str,
        context: &tidb_sessionctx::StmtContext,
    ) -> Self {
        Self {
            stmt: stmt.clone(),
            current_db: current_db.to_owned(),
            context: std::sync::Arc::new(context.clone()),
        }
    }

    /// `sql` in `current_db` under `context`.
    pub(crate) fn new(sql: &str, current_db: &str, context: &tidb_sessionctx::StmtContext) -> Self {
        Self {
            stmt: tidb_parser::parse(sql)
                .unwrap_or_else(|error| panic!("the fixture SQL parses: {sql}: {error:?}")),
            current_db: current_db.to_owned(),
            context: std::sync::Arc::new(context.clone()),
        }
    }
}

/// What `DoDDLJobWrapper` does with a built job.
#[derive(Clone, Copy, PartialEq)]
enum Submit {
    /// Insert the job and run its steps until it finishes.
    Run,
    /// Hand the admitted job back unsubmitted.
    Capture,
}

/// The last statement's outcome beyond its report.
enum Outcome {
    /// The job's committed writes, in order.
    Ran(DdlWrite),
    /// The admitted job, not submitted.
    Captured(Box<JobSpec>),
    /// The executor's or the job's refusal, as the planner raised it.
    Refused(DdlPlanError),
}

/// The fixture's DDL executor.
pub(crate) struct FixtureDdl<'a> {
    store: Mutex<&'a mut MetaStore>,
    start_ts: u64,
    tiflash_stores: u64,
    upgrading: bool,
    submit: Submit,
    outcome: Mutex<Option<Outcome>>,
}

impl<'a> FixtureDdl<'a> {
    pub(crate) fn new(store: &'a mut MetaStore, start_ts: u64) -> Self {
        Self {
            store: Mutex::new(store),
            start_ts,
            tiflash_stores: 0,
            upgrading: false,
            submit: Submit::Run,
            outcome: Mutex::new(None),
        }
    }

    /// A store with `count` TiFlash servers (Go `infoschema.GetTiFlashStoreCount`).
    pub(crate) fn with_tiflash_stores(mut self, count: u64) -> Self {
        self.tiflash_stores = count;
        self
    }

    /// A cluster in its upgrading state (Go `ddl.IsUpgradingState`).
    pub(crate) fn upgrading(mut self, upgrading: bool) -> Self {
        self.upgrading = upgrading;
        self
    }

    /// Hand the built job back instead of running it.
    pub(crate) fn capture(mut self) -> Self {
        self.submit = Submit::Capture;
        self
    }

    /// Runs `ddl` through `ClusterDdl::execute_ddl`.
    pub(crate) fn plan(&self, ddl: &Ddl) -> Result<DdlPlan, DdlPlanError> {
        let report = self.execute_ddl(&ddl.stmt, &ddl.current_db, &ddl.context);
        self.answer(report)
    }

    /// The executor's answer, with the job's writes or refusal.
    pub(crate) fn answer(
        &self,
        report: Result<ClusterDdlReport, SqlQueryError>,
    ) -> Result<DdlPlan, DdlPlanError> {
        let outcome = self.outcome.lock().unwrap().take();
        match (report, outcome) {
            // A refusal the executor recovered from (`DROP ... IF EXISTS`
            // turning 1051 into a note) is not the statement's.
            (Err(_), Some(Outcome::Refused(error))) => Err(error),
            (Err(error), _) => Err(DdlPlanError::from_mysql_error(
                tidb_terror::MysqlError::from(error),
            )),
            (Ok(ClusterDdlReport::AlreadySatisfied { detail, warnings }), _) => {
                Ok(DdlPlan::AlreadySatisfied { detail, warnings })
            }
            (Ok(ClusterDdlReport::Applied { .. }), Some(Outcome::Ran(write))) => {
                Ok(DdlPlan::Write(Box::new(write)))
            }
            (Ok(ClusterDdlReport::Applied { .. }), _) => {
                panic!("an applied fixture DDL ran no job")
            }
        }
    }

    /// Runs `ddl` with the job handed back unsubmitted: `None` when the
    /// statement needs no job.
    pub(crate) fn submission(&self, ddl: &Ddl) -> Result<Option<JobSpec>, DdlPlanError> {
        assert!(self.submit == Submit::Capture, "the fixture runs its jobs");
        let report = self.execute_ddl(&ddl.stmt, &ddl.current_db, &ddl.context);
        let outcome = self.outcome.lock().unwrap().take();
        match (report, outcome) {
            (Err(_), Some(Outcome::Refused(error))) => Err(error),
            (_, Some(Outcome::Captured(spec))) => Ok(Some(*spec)),
            (Err(error), _) => Err(DdlPlanError::from_mysql_error(
                tidb_terror::MysqlError::from(error),
            )),
            (Ok(_), _) => Ok(None),
        }
    }
}

impl ExecutorStore for FixtureDdl<'_> {
    type Snapshot = MetaStore;

    fn with_meta(
        &self,
        _context: &dyn DdlSessionContext,
        build: impl FnOnce(
            &mut MetaStore,
            u64,
            bool,
        ) -> Result<Option<AddIndexSubmission>, DdlPlanError>,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let mut store = self.store.lock().unwrap();
        let refuse = |error: DdlPlanError| {
            let answer = SqlQueryError::from(tidb_terror::MysqlError::from_sql_error(
                error.to_sql_error(),
            ));
            *self.outcome.lock().unwrap() = Some(Outcome::Refused(error));
            Err(answer)
        };
        let submission = match build(&mut **store, self.start_ts, self.upgrading) {
            Ok(submission) => submission,
            Err(error) => return refuse(error),
        };
        let (mut spec, warnings) = match submission {
            None => return Err(SqlQueryError::unknown("DDL executor built no job")),
            Some(AddIndexSubmission::AlreadySatisfied { detail, warnings }) => {
                return Ok(ClusterDdlReport::AlreadySatisfied { detail, warnings });
            }
            Some(AddIndexSubmission::Job(spec, warnings)) => (*spec, warnings),
        };
        if self.submit == Submit::Capture {
            *self.outcome.lock().unwrap() = Some(Outcome::Captured(Box::new(spec)));
            return Ok(ClusterDdlReport::AlreadySatisfied {
                detail: "captured".into(),
                warnings,
            });
        }
        let catalog = match load_info_schema(&mut **store) {
            Ok(catalog) => catalog,
            Err(error) => return refuse(error.into()),
        };
        let mut before_insert_with_assigned_ids = |_: &[JobSpec]| Option::<fn()>::None;
        let mutations = match plan_insert_attempt(
            &mut **store,
            &catalog,
            std::slice::from_mut(&mut spec),
            &mut before_insert_with_assigned_ids,
        ) {
            Ok((mutations, _)) => mutations,
            Err(error) => return refuse(error.into()),
        };
        let mut write = tidb_ddl::cluster_ddl::job_only_write(spec.job.id, mutations);
        apply(&mut **store, &write);
        write.warnings = warnings.clone();
        match run_inserted_job(&mut **store, spec.job.id, self.start_ts, write) {
            Ok(write) => {
                let report = ClusterDdlReport::Applied {
                    schema_version: write.schema_version,
                    created_id: write.created_id,
                    warnings,
                };
                // A statement of several jobs (a DROP list) answers all their
                // writes, in order.
                let mut outcome = self.outcome.lock().unwrap();
                let write = match outcome.take() {
                    Some(Outcome::Ran(mut earlier)) => {
                        earlier.created_id = earlier.created_id.or(write.created_id);
                        earlier.schema_version = write.schema_version;
                        earlier.diff = write.diff;
                        earlier.mutations.extend(write.mutations);
                        earlier.warnings.extend(write.warnings);
                        earlier
                    }
                    _ => write,
                };
                *outcome = Some(Outcome::Ran(write));
                Ok(report)
            }
            Err(error) => refuse(error),
        }
    }

    fn latest_schema(&self) -> std::sync::Arc<tidb_infoschema::SessionExtendedInfoSchema> {
        let mut store = self.store.lock().unwrap();
        std::sync::Arc::new(load_info_schema(&mut **store).expect("the fixture catalog loads"))
    }

    fn store_count(&self) -> usize {
        1
    }

    fn tiflash_store_count(&self) -> Result<u64, String> {
        Ok(self.tiflash_stores)
    }

    // The fixture tables hold no rows.
    fn has_null_value(
        &self,
        _schema: &str,
        _table: &tidb_model::TableInfo,
        _column_id: i64,
    ) -> Result<bool, DdlPlanError> {
        Ok(false)
    }

    fn uuid(&self) -> &str {
        "fixture-ddl"
    }
}

impl ClusterDdl for FixtureDdl<'_> {
    fn update_table_replica_info(
        &self,
        table_id: i64,
        available: bool,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::update_table_replica_info(self, table_id, available, context)
    }

    fn alter_table(
        &self,
        statement: &tidb_ast::AlterTableStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::alter_table(self, statement, current_db, context)
    }

    fn recover_schema(
        &self,
        _name: &str,
        _new_name: &str,
        _current_db: &str,
        _context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the fixture DDL has no recover_schema"))
    }

    fn recover_table(
        &self,
        _table: &Option<Vec<String>>,
        _job_id: i64,
        _new_name: &str,
        _flashback: bool,
        _current_db: &str,
        _context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the fixture DDL has no recover_table"))
    }

    fn drop_index(
        &self,
        statement: &tidb_ast::DropIndexStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::drop_index(self, statement, current_db, context)
    }

    fn create_index(
        &self,
        statement: &tidb_ast::CreateIndexStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::create_index(self, statement, current_db, context)
    }

    fn alter_table_set_tiflash_replica(
        &self,
        schema: &str,
        table: &str,
        count: u64,
        labels: &[String],
        reset_available: bool,
        hypo: bool,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::alter_table_set_tiflash_replica(
            self,
            schema,
            table,
            count,
            labels,
            reset_available,
            hypo,
            context,
        )
    }

    fn alter_schema(
        &self,
        name: &str,
        options: &[tidb_ast::DatabaseOption],
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::alter_schema(self, name, options, context)
    }

    fn drop_placement_policy(
        &self,
        statement: &tidb_ast::DropPlacementPolicyStmt,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::drop_placement_policy(self, statement, context)
    }

    fn alter_placement_policy(
        &self,
        statement: &tidb_ast::AlterPlacementPolicyStmt,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::alter_placement_policy(self, statement, context)
    }

    fn create_placement_policy(
        &self,
        statement: &tidb_ast::CreatePlacementPolicyStmt,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::create_placement_policy(self, statement, context)
    }

    fn refresh_meta(
        &self,
        schema_id: i64,
        table_id: i64,
        involved_db: &str,
        involved_table: &str,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::refresh_meta(
            self,
            schema_id,
            table_id,
            involved_db,
            involved_table,
            context,
        )
    }

    fn alter_table_mode(
        &self,
        table_mode: tidb_model::TableMode,
        schema_id: i64,
        table_id: i64,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::alter_table_mode(self, table_mode, schema_id, table_id, context)
    }

    fn lock_tables(
        &self,
        locks: &[tidb_ast::TableLock],
        current_db: &str,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::lock_tables(self, locks, current_db, context)
    }

    fn unlock_tables(
        &self,
        unlock_tables: Vec<tidb_model::TableLockTpInfo>,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::unlock_tables(self, unlock_tables, context)
    }

    fn cleanup_table_lock(
        &self,
        tables: &[Vec<String>],
        current_db: &str,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::cleanup_table_lock(self, tables, current_db, context)
    }

    fn create_masking_policy(
        &self,
        stmt: &tidb_ast::CreateMaskingPolicyStmt,
        current_db: &str,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::create_masking_policy(self, stmt, current_db, context)
    }

    fn create_materialized_view(
        &self,
        _: &tidb_ast::CreateMaterializedViewStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests create no materialized view")
    }

    fn alter_materialized_view_log(
        &self,
        _: &tidb_ast::AlterMaterializedViewLogStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests alter no materialized view log")
    }

    fn drop_materialized_view_log(
        &self,
        statement: &tidb_ast::DropMaterializedViewLogStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::drop_materialized_view_log(self, statement, current_db, context)
    }

    fn create_materialized_view_log(
        &self,
        stmt: &tidb_ast::CreateMaterializedViewLogStmt,
        current_db: &str,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::submit_create_materialized_view_log(self, stmt, current_db, context).map(|(report, ..)| report)
    }

    fn create_view(
        &self,
        schema: &str,
        name: &str,
        or_replace: bool,
        info: &tidb_model::TableInfo,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::create_view(self, schema, name, or_replace, info, context)
    }

    fn do_ddl_job_wrapper(
        &self,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
        job: tidb_ddl::JobWrapper,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::do_ddl_job_wrapper(self, context, job)
    }

    fn create_schema_with_info(
        &self,
        info: tidb_model::DBInfo,
        on_exist: tidb_ddl::OnExist,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::create_schema_with_info(self, info, on_exist, context)
    }

    fn create_table_with_info(
        &self,
        _db_name: &str,
        _info: tidb_model::TableInfo,
        _involving_ref: &[tidb_model::InvolvingSchemaInfo],
        _config: tidb_ddl::CreateTableConfig,
        _context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the fixture DDL has no create_table_with_info"))
    }

    fn batch_create_table_with_info(
        &self,
        _db_name: &str,
        _infos: Vec<tidb_model::TableInfo>,
        _config: tidb_ddl::CreateTableConfig,
        _context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the fixture DDL has no batch_create_table_with_info"))
    }

    fn create_placement_policy_with_info(
        &self,
        policy: tidb_model::PolicyInfo,
        on_exist: tidb_ddl::OnExist,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::create_placement_policy_with_info(self, policy, on_exist, context)
    }

    fn drop_table(
        &self,
        statement: &tidb_ast::DropTableStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::drop_table(self, statement, current_db, context)
    }

    fn drop_sequence(
        &self,
        statement: &tidb_ast::DropSequenceStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::drop_sequence(self, statement, current_db, context)
    }

    fn rename_table(
        &self,
        statement: &tidb_ast::RenameTableStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::rename_table(self, statement, current_db, context)
    }

    fn drop_view(
        &self,
        names: &[Vec<String>],
        if_exists: bool,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::drop_view(self, names, if_exists, current_db, context)
    }

    fn drop_schema(
        &self,
        name: &str,
        if_exists: bool,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::drop_schema(self, name, if_exists, context)
    }

    fn create_table(
        &self,
        statement: &tidb_ast::CreateTableStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::create_table(self, statement, current_db, context)
    }

    fn create_sequence(
        &self,
        statement: &tidb_ast::CreateSequenceStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::create_sequence(self, statement, current_db, context)
    }

    fn alter_sequence(
        &self,
        statement: &tidb_ast::AlterSequenceStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::alter_sequence(self, statement, current_db, context)
    }

    fn repair_table(
        &self,
        _statement: &tidb_ast::RepairTableStmt,
        _current_db: &str,
        _context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the fixture DDL has no repair_table"))
    }

    fn truncate_table(
        &self,
        statement: &[String],
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::truncate_table(self, statement, current_db, context)
    }

    fn create_schema(
        &self,
        name: &str,
        if_not_exists: bool,
        options: &[tidb_ast::DatabaseOption],
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        tidb_ddl::executor::create_schema(self, name, if_not_exists, options, context)
    }

    fn tiflash_store_count(&self) -> Result<u64, SqlQueryError> {
        Ok(self.tiflash_stores)
    }
}
