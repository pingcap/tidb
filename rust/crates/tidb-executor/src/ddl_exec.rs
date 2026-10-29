//! Go's ADMIN executors and the `SimpleExec` arms that touch no user table.
//!
//! `ADMIN CHECK TABLE` / `ADMIN CHECK INDEX`: the `AdminStmt::AdminCheck` arm
//! of the session's ADMIN dispatch.
//!
//! The check itself lives in `crate::admin_check` (which mirrors Go
//! `pkg/executor/check_table_index.go`); this file is only the statement
//! seam -- name resolution, the output SHAPE, and the refusals.
//!
//! # The shapes, from TiDB's own recording
//!
//! From `tests/integrationtest/r/util/admin.result` and
//! `tests/integrationtest/r/executor/admin.result`, which are TiDB's oracle
//! and are only ever read here:
//!
//! - `admin check table t;` -- NO output at all: Go's `CheckTable` plan has
//!   an EMPTY schema, so the server sends an OK packet, not a zero-column
//!   result set (see [`admin_check_passed`]). Producing nothing is the whole
//!   success contract, which is exactly why the check underneath must be
//!   real: an `Ok` returned without reading a byte is indistinguishable from
//!   a passing check, and would be a success return that is not one.
//! - `admin check index t idx;` -- also no output; the same consistency
//!   check, narrowed to one index.
//! - `admin check index t idx (2, 4);` -- a RESULT SET: the indexed columns
//!   followed by `extra_handle`, in index order, for the rows whose handle
//!   falls in a half-open interval.
//!
//! # What this refuses, by name
//!
//! A table whose rows are not stored as index-bearing bytes has no index
//! entries to check against, so `ADMIN CHECK` over one is REFUSED rather than
//! answered OK. Same for the multi-table `ADMIN CHECK TABLE t1, t2` form,
//! which Go's own planner rejects.

//! Go `pkg/executor/ddl.go`: `DDLExec.Next` over the connection that runs
//! it. The DDL executor's checks against the catalog, the statement lowered
//! to the job(s) the cluster runs (`ddlExecutor.DoDDLJob`), the DDL a session
//! answers itself, and `ModifySchemaSetTiFlashReplica`. The session state and
//! the store-side steps it reaches are [`DdlExec`]'s primitives.

use std::sync::Arc;

use tidb_ast::Stmt;
use tidb_infoschema::cluster_catalog::ClusterCatalog;
use tidb_store_driver::sql_query_error::SqlQueryError;
use tidb_ddl::cluster_ddl::DdlStatement;
use tidb_ddl::real_tikv_ddl::prepare_cluster_ddl_parsed;

use crate::{DriverError, StmtOutput};

/// What one statement reported for its OK packet.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteOutcome {
    /// Rows to report in the MySQL OK packet.
    pub affected_rows: u64,
    /// The auto-increment id this statement generated, which the OK packet
    /// carries as `last_insert_id`. Zero when the statement generated none.
    pub last_insert_id: u64,
}

/// What [`DdlExec::lower_ddl_exec`] lowered a DDL to.
pub enum LoweredDdl {
    /// The job(s) to run in order, stopping at the first failure (Go
    /// `AlterTable`'s table-option loop is several).
    Jobs(Vec<DdlStatement>),
    /// Go `DDLExec.Next`'s DROP that also names LOCAL temporary tables: the
    /// job drops the rest, then `dropLocalTemporaryTables` drops `local`.
    DropTableJob {
        /// The job dropping the tables that are not LOCAL temporary ones.
        statement: DdlStatement,
        /// The LOCAL temporary tables dropped once the job succeeds.
        local: Vec<Vec<String>>,
    },
    /// Go's executor functions that submit several jobs in order (the
    /// table-option loop, `AlterSchema`): each part is lowered when its turn
    /// comes, so a refusal leaves the earlier parts applied, as Go's does.
    Sequence {
        /// The statements lowered and run one after another.
        parts: Vec<Stmt>,
        /// The statement context their lowering warnings join.
        context: crate::StmtContext,
    },
}

impl LoweredDdl {
    fn one(statement: DdlStatement) -> Self {
        Self::Jobs(vec![statement])
    }
}

/// The statements one Go executor function runs as several jobs in order --
/// a multi-option `ALTER TABLE ... <table options>` (Go `AlterTable`'s
/// option loop) and an `ALTER DATABASE` with several changes (Go
/// `AlterSchema`). `None` for every other statement.
fn job_sequence_parts(stmt: &Stmt) -> Option<Vec<Stmt>> {
    let Stmt::Ddl(ddl) = stmt else {
        return None;
    };
    let parts = match &**ddl {
        tidb_ast::DdlStmt::AlterTable(alter) => {
            tidb_ddl::cluster_ddl::split_table_option_alter(alter)?
                .into_iter()
                .map(|part| tidb_ast::DdlStmt::AlterTable(Box::new(part)))
                .collect()
        }
        tidb_ast::DdlStmt::AlterDatabase { name, options } => {
            tidb_ddl::cluster_ddl::split_alter_database(name, options)?
        }
        _ => return None,
    };
    Some(
        parts
            .into_iter()
            .map(|part| Stmt::Ddl(tidb_ast::NodeBox::new(part)))
            .collect(),
    )
}

/// A DDL shape this node does not plan.
fn unplanned_ddl() -> SqlQueryError {
    SqlQueryError::unknown(
        "this node changes the cluster's catalog for CREATE TABLE, DROP TABLE, \
         CREATE DATABASE, DROP DATABASE, CREATE/DROP VIEW, placement policy \
         changes, index changes, and single-action ALTER INDEX changes only; \
         run this statement on a TiDB server",
    )
}

/// A session failure as the connection answers it, keeping the typed
/// error as its cause.
pub fn map_error(error: DriverError) -> SqlQueryError {
    let mapped = error.clone().to_mysql_error();
    SqlQueryError::from(mapped).with_cause(error)
}

/// A DDL planning refusal as Go's executor raises it.
fn ddl_error(error: &tidb_ddl::cluster_ddl::DdlPlanError) -> DriverError {
    DriverError::Mysql(crate::MysqlError::from_sql_error(error.to_sql_error()))
}

/// The connection Go's `DDLExec` runs over: the session (Go `sctx`) and the
/// store-side steps its node takes for it.
pub trait DdlExec {
    /// Go `DDLExec.Next`'s statement checks before `NewTxnInStmt`.
    fn begin_ddl_statement(&mut self, parsed: &Stmt) -> Result<(), DriverError>;
    /// Go `DDLExec.executeCleanupTableLock`'s job, none when nothing is locked.
    fn cluster_cleanup_table_lock_statement(
        &self,
        tables: &[Vec<String>],
    ) -> Result<Option<DdlStatement>, DriverError>;
    /// A DROP that also names LOCAL temporary tables: the DROP without them
    /// and the LOCAL tables `dropLocalTemporaryTables` drops after the job;
    /// `None` for every other DDL that runs as a job.
    fn drop_table_job(
        &self,
        ddl: &tidb_ast::DdlStmt,
    ) -> Option<(tidb_ast::DropTableStmt, Vec<Vec<String>>)>;
    /// The statement context a DDL is lowered under.
    fn ddl_statement_context(&self) -> crate::StmtContext;
    /// The session's current database.
    fn current_database(&self) -> &str;
    /// The session's variables.
    fn session_vars(&self) -> &tidb_variable::SessionVars;
    /// The DDL executor's target checks against the session's catalog.
    fn ddl_target_checks(&mut self, stmt: &Stmt) -> Result<(), DriverError>;
    /// Go `executor.LockTables`' checks, building the lock job.
    fn cluster_lock_tables_statement(
        &mut self,
        locks: &[tidb_ast::TableLock],
    ) -> Result<DdlStatement, DriverError>;
    /// The job releasing every table lock the session holds, if any.
    fn cluster_unlock_tables_statement(&self) -> Option<DdlStatement>;
    /// Go's preprocessor resolving one named table (1146 / 1046).
    fn require_table_exists(&mut self, path: &[String]) -> Result<(), DriverError>;
    /// Starts the statement's own warnings.
    fn begin_routed_statement_warnings(&mut self);
    /// Appends a lowering context's warnings to the statement's.
    fn drain_context_warnings(&mut self, ctx: &crate::StmtContext);
    /// Go `dropLocalTemporaryTables`.
    fn drop_local_temporary_tables(&mut self, names: &[Vec<String>]) -> Result<(), DriverError>;
    /// The session's statement-kill switch.
    fn sql_killer(&self) -> Arc<tidb_sqlkiller::SqlKiller>;
    /// Appends one warning to the statement context.
    fn append_warning(
        &mut self,
        level: tidb_context::WarningLevel,
        code: u16,
        message: String,
    );
    /// The statement's OK-packet message.
    fn set_statement_message(&mut self, message: String);
    /// Go `ExecStmt`'s `tidb_snapshot` write refusal.
    fn validate_snapshot_write(&self, stmt: &Stmt) -> Result<(), DriverError>;
    /// Whether Go `DDLExec.Next` returns before `NewTxnInStmt`.
    fn ddl_preserves_transaction(&self, stmt: &Stmt) -> bool;
    /// Go `ExecStmt.Exec` for a DDL the session answers itself.
    fn exec_statement_parsed(&mut self, stmt: Stmt, sql: &str) -> Result<StmtOutput, DriverError>;
    /// The auto-increment id the statement generated.
    fn statement_insert_id(&self) -> u64;
    /// Go `sessiontxn.NewTxnInStmt`: commits the open transaction.
    fn new_txn_in_stmt(&mut self) -> Result<(), SqlQueryError>;
    /// Records the transaction `NewTxnInStmt` opened as `@@tidb_last_txn_info`.
    fn record_new_txn_in_stmt(&mut self);
    /// Go `executor.AddResourceGroup` and its siblings' checks, run over a
    /// statement snapshot (DROP reads `mysql.user`).
    fn probe_resource_group_statement(
        &mut self,
        ddl: &tidb_ast::DdlStmt,
    ) -> Result<Option<DdlStatement>, SqlQueryError>;
    /// Go `executeCreateView`'s preprocess of the body, over a statement
    /// snapshot: the database, name, `OR REPLACE` and the resolved view.
    fn probe_cluster_view(
        &mut self,
        stmt: &Stmt,
    ) -> Result<Option<(String, String, bool, crate::ViewDef)>, SqlQueryError>;
    /// The catalog the node publishes (Go's latest infoschema).
    fn published_catalog(&self) -> Arc<ClusterCatalog>;
    /// `ddlExecutor.DoDDLJob` for one lowered statement: the ids of the
    /// tables it created.
    fn run_ddl_statement(
        &mut self,
        sql: &str,
        parsed: &Stmt,
        statement: &DdlStatement,
    ) -> Result<Option<i64>, DriverError>;
    /// The cluster's TiFlash stores, for a replica count's check.
    fn tiflash_store_count(&self) -> Result<u64, String>;
    /// Go `preSplitAndScatterTable` for the tables a DDL created.
    fn pre_split_created_tables(&mut self, created: &[i64]);

    /// Go `DDLExec.Next` after its `NewTxnInStmt` (`executor/ddl.go:151`):
    /// the DDL executor's checks against this node's catalog and the
    /// statement lowered to the job(s) the cluster runs. Every refusal here
    /// comes after the implicit commit, as Go's do.
    fn lower_ddl_exec(&mut self, stmt: &Stmt) -> Result<LoweredDdl, SqlQueryError> {
        // Go `DDLExec.executeCleanupTableLock` after the planner's
        // SUPER check: the `IsCleanup` unlock job, none when no named
        // table is locked.
        if let Stmt::Ddl(ddl) = stmt {
            if let tidb_ast::DdlStmt::CleanupTableLock(cleanup) = ddl.as_ref() {
                let statement = self
                    .cluster_cleanup_table_lock_statement(&cleanup.tables)
                    .map_err(map_error)?;
                return Ok(LoweredDdl::one(statement.unwrap_or(
                    DdlStatement::AcceptedNoOp {
                        schema: String::new(),
                        table: String::new(),
                        warning: None,
                    },
                )));
            }
        }
        // Go `DDLExec.Next`: the DDL the session answers runs there;
        // a DROP that also names LOCAL temporary tables runs the job
        // for the rest and then drops them.
        if let Stmt::Ddl(ddl) = stmt {
            if let Some((persistent, local)) = self.drop_table_job(ddl) {
                let persistent = Stmt::Ddl(tidb_ast::NodeBox::new(tidb_ast::DdlStmt::DropTable(
                    Box::new(persistent),
                )));
                let context = self.ddl_statement_context();
                let ddl_context: tidb_ddl::cluster_ddl::DdlContext = Arc::new(context.clone());
                let statement =
                    prepare_cluster_ddl_parsed(&persistent, self.current_database(), &ddl_context)
                        .map_err(|error| SqlQueryError::from(error.to_mysql_error()))?
                        .ok_or_else(|| {
                            SqlQueryError::unknown("DROP TABLE has no cluster metadata operation")
                        })?;
                return Ok(LoweredDdl::DropTableJob { statement, local });
            }
        }
        // The DDL executor's target checks; the ALTER spec walk's own
        // refusals are the lowering's (`cluster_ddl`).
        self.ddl_target_checks(stmt).map_err(map_error)?;
        // Go `DDLExec.executeLockTables`/`executeUnlockTables` with
        // `enable-table-lock` on: the planner's privilege check, then
        // `executor.LockTables`' checks against this node's catalog
        // build the lock job; UNLOCK with nothing held submits none.
        if tidb_config::config_tree::config::table_lock_enabled() {
            if let tidb_ast::Stmt::Ddl(ddl) = stmt {
                match &**ddl {
                    tidb_ast::DdlStmt::LockTables(locks) => {
                        return self
                            .cluster_lock_tables_statement(locks)
                            .map(LoweredDdl::one)
                            .map_err(map_error);
                    }
                    tidb_ast::DdlStmt::UnlockTables => {
                        return Ok(LoweredDdl::one(
                            self.cluster_unlock_tables_statement().unwrap_or(
                                DdlStatement::AcceptedNoOp {
                                    schema: String::new(),
                                    table: String::new(),
                                    warning: None,
                                },
                            ),
                        ));
                    }
                    _ => {}
                }
            }
        }
        // go `SimpleExec.executeLockTables`/`executeUnlockTables`:
        // with the experimental table lock disabled
        // (`enable-table-lock=false`), both statements answer OK
        // with Warning 1235 pointing at the configuration
        // (`pkg/ddl/tests/serial/serial_test.go:1057`).
        if let tidb_ast::Stmt::Ddl(ddl) = stmt {
            let notice = match &**ddl {
                // Go's preprocessor resolves every named table, each
                // under its own schema, before the warning: the first
                // missing one is 1146 and no current database 1046.
                tidb_ast::DdlStmt::LockTables(locks) => {
                    for lock in locks.iter() {
                        self.require_table_exists(&lock.table).map_err(map_error)?;
                    }
                    Some((
                        String::new(),
                        String::new(),
                        "LOCK TABLES is not supported. To enable this experimental \
                         feature, set 'enable-table-lock' in the configuration file."
                            .to_owned(),
                    ))
                }
                tidb_ast::DdlStmt::UnlockTables => Some((
                    String::new(),
                    String::new(),
                    "UNLOCK TABLES is not supported. To enable this experimental \
                     feature, set 'enable-table-lock' in the configuration file."
                        .to_owned(),
                )),
                _ => None,
            };
            if let Some((schema, table, message)) = notice {
                return Ok(LoweredDdl::one(DdlStatement::AcceptedNoOp {
                    schema,
                    table,
                    warning: Some((1235, message)),
                }));
            }
        }
        // Go `executor.AddResourceGroup` / `AlterResourceGroup` /
        // `DropResourceGroup` check against the session's schema
        // before the job; an `IF [NOT] EXISTS` that suppressed its
        // error submits none.
        if let Stmt::Ddl(ddl) = stmt {
            if matches!(
                &**ddl,
                tidb_ast::DdlStmt::CreateResourceGroup(_)
                    | tidb_ast::DdlStmt::AlterResourceGroup(_)
                    | tidb_ast::DdlStmt::DropResourceGroup(_)
            ) {
                // DROP reads `mysql.user` for an account bound to the
                // group (Go `MatchUserResourceGroupName`), so the
                // admission reads through a statement snapshot.
                let statement = self.probe_resource_group_statement(ddl)?;
                return Ok(LoweredDdl::one(statement.unwrap_or(
                    DdlStatement::AcceptedNoOp {
                        schema: String::new(),
                        table: String::new(),
                        warning: None,
                    },
                )));
            }
        }
        // A CREATE VIEW resolves its body against this node's own
        // catalog FIRST — a bad body fails here, at CREATE time,
        // exactly where Go's `executeCreateView` preprocess fails —
        // and the finished definition becomes the published
        // `TableInfo`.
        // The body's meta run reads through cluster storage, so it
        // needs the same statement-snapshot lifecycle the PREPARE
        // probe uses — work the client did not ask to run, reading
        // at a fresh timestamp and unbinding afterwards.
        let resolved = self.probe_cluster_view(stmt)?;
        if let Some((database, name, or_replace, view)) = resolved {
            let info = crate::view::build_view_table_info(&name, &view);
            return Ok(LoweredDdl::one(DdlStatement::CreateView {
                schema: database,
                name,
                or_replace,
                info: Box::new(info),
            }));
        }
        let context = self.ddl_statement_context();
        let ddl_context: tidb_ddl::cluster_ddl::DdlContext = Arc::new(context.clone());
        match prepare_cluster_ddl_parsed(stmt, self.current_database(), &ddl_context) {
            Ok(Some(statement)) => {
                // Warnings the LOWERING produced (an ignored CHECK
                // constraint, Go ddl/create_table.go:1470) belong to
                // this statement; Go appends them to the session's
                // own context, so drain the lowering context's here.
                self.begin_routed_statement_warnings();
                self.drain_context_warnings(&context);
                Ok(LoweredDdl::one(statement))
            }
            Ok(None) => {
                if let Some(parts) = job_sequence_parts(stmt) {
                    self.begin_routed_statement_warnings();
                    self.drain_context_warnings(&context);
                    return Ok(LoweredDdl::Sequence { parts, context });
                }
                // Go's executor refuses a cached table before any
                // shape this node does not plan.
                tidb_ddl::cluster_ddl::cached_table::check_cached_table_ddl(
                    &self.published_catalog(),
                    self.current_database(),
                    stmt,
                )
                .map_err(|error| SqlQueryError::from(error))?;
                Err(unplanned_ddl())
            }
            // The refusal carries Go's own errno where it has one
            // (`Unsupported ...` is 8200), so a client can tell a
            // shape this server will not do from an internal failure.
            // Warnings the lowering raised BEFORE the refusal (a
            // 1681 display-width warning beside an 8216 auto-random
            // refusal) survive it: go's failed DDL leaves both in
            // the warning buffer.
            Err(refusal) => {
                self.begin_routed_statement_warnings();
                self.drain_context_warnings(&context);
                tidb_ddl::cluster_ddl::cached_table::check_cached_table_ddl(
                    &self.published_catalog(),
                    self.current_database(),
                    stmt,
                )
                .map_err(|error| SqlQueryError::from(error))?;
                Err(SqlQueryError::from(refusal.to_mysql_error()))
            }
        }
    }

    /// One part of a [`LoweredDdl::Sequence`], lowered by the ordinary path
    /// when its turn comes; its warnings join the statement's.
    fn lower_sequence_part(
        &mut self,
        part: &Stmt,
        context: &crate::StmtContext,
    ) -> Result<DdlStatement, SqlQueryError> {
        let ddl_context: tidb_ddl::cluster_ddl::DdlContext = Arc::new(context.clone());
        let lowered = prepare_cluster_ddl_parsed(part, self.current_database(), &ddl_context);
        self.drain_context_warnings(context);
        match lowered {
            Ok(Some(statement)) => Ok(statement),
            Ok(None) => Err(unplanned_ddl()),
            Err(refusal) => Err(SqlQueryError::from(refusal.to_mysql_error())),
        }
    }

    /// Go `DDLExec.Next` for a DDL that runs as a job: `Compile` checked the
    /// plan and its privileges and `ExecStmt` the `tidb_snapshot` write while
    /// this connection's transaction was intact; `NewTxnInStmt` then commits
    /// it, and only then does the DDL executor check the statement against
    /// the catalog, lower it and run its job(s) -- a refusal from here on
    /// leaves the earlier writes committed, as Go's does.
    fn run_ddl_exec(&mut self, sql: &str, parsed: &Stmt) -> Result<WriteOutcome, SqlQueryError> {
        self.begin_ddl_statement(parsed).map_err(map_error)?;
        // Go `DDLExec.Next`'s `NewTxnInStmt` (`executor/ddl.go:151`).
        self.new_txn_in_stmt()?;
        self.record_new_txn_in_stmt();
        match self.lower_ddl_exec(parsed)? {
            LoweredDdl::Jobs(statements) => {
                for statement in &statements {
                    self.run_ddl_job(sql, parsed, statement)?;
                }
            }
            LoweredDdl::Sequence { parts, context } => {
                for part in &parts {
                    let statement = self.lower_sequence_part(part, &context)?;
                    self.run_ddl_job(sql, parsed, &statement)?;
                }
            }
            LoweredDdl::DropTableJob { statement, local } => {
                self.run_ddl_job(sql, parsed, &statement)?;
                self.drop_local_temporary_tables(&local)
                    .map_err(map_error)?;
            }
        }
        // Go answers a DDL with an OK packet carrying no rows and no insert
        // id, whether it changed anything or was an IF [NOT] EXISTS no-op.
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
    }

    /// One job through the DDL executor (`ddlExecutor.DoDDLJob`).
    fn run_ddl_job(
        &mut self,
        sql: &str,
        parsed: &tidb_ast::Stmt,
        statement: &DdlStatement,
    ) -> Result<(), SqlQueryError> {
        if let DdlStatement::ModifySchemaSetTiFlashReplica {
            name,
            count,
            labels,
        } = statement
        {
            return self.modify_schema_set_tiflash_replica(sql, parsed, name, *count, labels);
        }
        // The session owns the rest of the DDL path; this node installed
        // the service that publishes to the cluster.
        let created = self
            .run_ddl_statement(sql, parsed, statement)
            .map_err(map_error)?;
        // Go's executor pre-splits the table a CREATE TABLE published and
        // the new identity `onTruncateTable` gave.
        if matches!(
            statement,
            DdlStatement::CreateTable { .. }
                | DdlStatement::CreateTableLike { .. }
                | DdlStatement::TruncateTable { .. }
        ) {
            self.pre_split_created_tables(created.as_slice());
        }
        Ok(())
    }

    /// Go `ModifySchemaSetTiFlashReplica`: an `ActionSetTiFlashReplica` job
    /// for each table of the database whose replica changes. A table TiFlash
    /// cannot carry is a note and is skipped; a failed job is counted, not
    /// returned; every 100 jobs (and after a timed-out wait) the loop waits
    /// for the unavailable replicas to drop below
    /// `tidb_batch_pending_tiflash_count`; a killed session stops it. The
    /// totals are the statement's message.
    fn modify_schema_set_tiflash_replica(
        &mut self,
        sql: &str,
        parsed: &tidb_ast::Stmt,
        name: &str,
        count: u64,
        labels: &[String],
    ) -> Result<(), SqlQueryError> {
        const CHECK_PENDING_TABLES_LIMIT: usize = 100;
        let catalog = self.published_catalog();
        let (schema, tables) =
            tidb_ddl::cluster_ddl::alter_schema::schema_tiflash_replica_tables(&catalog, name)
                .map_err(|error| map_error(ddl_error(&error)))?;
        tidb_ddl::cluster_ddl::alter_table::check_tiflash_replica_count(count, &|| {
            self.tiflash_store_count()
        })
        .map_err(|error| map_error(ddl_error(&error)))?;
        let total = tables.len();
        let (mut succeeded, mut failed, mut skipped, mut one_fail) =
            (0_usize, 0_usize, 0_usize, 0_i64);
        let threshold = self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_BATCH_PENDING_TIFLASH_COUNT)
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(4000);
        let killer = self.sql_killer();
        let (mut origin_version, mut pending_count, mut force_check) = (0_i64, 0_u32, false);
        for table in tables {
            if tidb_ddl_build::ddl_executor::is_session_done(&killer).0 {
                return Ok(());
            }
            let current = table
                .tiflash_replica
                .as_ref()
                .map(|replica| replica.read().clone());
            if !tidb_ddl_build::ddl_executor::should_modify_tiflash_replica(
                current.as_ref(),
                count,
                labels,
            ) {
                skipped += 1;
                continue;
            }
            if let Err(error) =
                tidb_ddl_build::ddl_executor::is_table_tiflash_supported(&schema, table)
            {
                self.append_warning(
                    tidb_context::WarningLevel::Note,
                    error.code,
                    error.message,
                );
                skipped += 1;
                continue;
            }
            if (succeeded + failed) % CHECK_PENDING_TABLES_LIMIT == 0 || force_check {
                let finished;
                (finished, origin_version, pending_count, force_check) = self
                    .wait_pending_tiflash_table_threshold(
                        &killer,
                        origin_version,
                        pending_count,
                        threshold,
                    );
                if finished {
                    return Ok(());
                }
            }
            let job = DdlStatement::SetTiFlashReplica {
                schema: schema.clone(),
                table: table.name.original().to_owned(),
                count,
                labels: labels.to_vec(),
                reset_available: false,
                hypo: false,
            };
            match self.run_ddl_job(sql, parsed, &job) {
                Ok(()) => succeeded += 1,
                Err(_) => {
                    one_fail = table.id;
                    failed += 1;
                }
            }
        }
        let fail_stmt = if failed > 0 {
            format!("(including table {one_fail})")
        } else {
            String::new()
        };
        self.set_statement_message(format!(
            "In total {total} tables: {succeeded} succeed, {failed} failed{fail_stmt}, {skipped} skipped"
        ));
        Ok(())
    }

    /// Go `waitPendingTableThreshold`: up to seven checks, three seconds
    /// apart, of the unavailable TiFlash replicas in the latest schema
    /// (`getPendingTiFlashTableCount`, recounted only on a new schema
    /// version). Answers whether the session is done, the version and count
    /// seen, and whether the wait timed out (a forced check next time).
    fn wait_pending_tiflash_table_threshold(
        &self,
        killer: &tidb_sqlkiller::SqlKiller,
        mut origin_version: i64,
        mut pending_count: u32,
        threshold: u32,
    ) -> (bool, i64, u32, bool) {
        const RETRY: usize = 7;
        const WAIT: std::time::Duration = std::time::Duration::from_millis(3000);
        for _ in 0..RETRY {
            if tidb_ddl_build::ddl_executor::is_session_done(killer).0 {
                return (true, origin_version, pending_count, false);
            }
            let latest = self.published_catalog();
            if latest.schema_version != origin_version {
                origin_version = latest.schema_version;
                pending_count =
                    tidb_ddl::cluster_ddl::alter_schema::pending_tiflash_table_count(&latest);
            }
            if pending_count < threshold {
                return (false, origin_version, pending_count, false);
            }
            std::thread::sleep(WAIT);
        }
        (false, origin_version, pending_count, true)
    }

    /// Executes DDL whose metadata belongs to this connection rather than to
    /// the cluster catalog.
    fn run_session_ddl(
        &mut self,
        sql: &str,
        parsed: &tidb_ast::Stmt,
    ) -> Result<WriteOutcome, SqlQueryError> {
        // Go returns before NewTxnInStmt for local CREATE and local-only DROP.
        // Check before committing the cluster transaction (the staleness
        // check preceded `Compile`, which checked its privileges);
        // Session::run then owns the session-local table operation and
        // observation, its compile taking the node's plan.
        self.validate_snapshot_write(parsed).map_err(map_error)?;
        if !self.ddl_preserves_transaction(parsed) {
            self.new_txn_in_stmt()?;
        }
        match self
            .exec_statement_parsed(parsed.clone(), sql)
            .map_err(map_error)?
        {
            StmtOutput::Affected(affected_rows) => Ok(WriteOutcome {
                affected_rows,
                last_insert_id: self.statement_insert_id(),
            }),
            StmtOutput::Done(_) => Ok(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            }),
            StmtOutput::Rows { .. } => Err(SqlQueryError::unknown(
                "LOCAL temporary-table DDL unexpectedly returned rows",
            )),
        }
    }
}
