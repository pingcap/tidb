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

//! Go `pkg/ddl/session`: the sessions the domain hands the DDL package, and
//! the plan values the work done through them carries.

/// Go `ReorgMeta.SQLMode` / `ReorgMeta.Location`: the session variables a
/// reorganization runs under, which its worker turns into its statement
/// context (`newReorgSessCtx`).
#[derive(Clone, Debug)]
pub struct ReorgSessionVars {
    /// The job's SQL mode.
    pub sql_mode: i64,
    /// The job's time zone (`ReorgMeta.Location.GetLocation`).
    pub time_zone: tidb_datatype::SessionTimeZone,
}

impl ReorgSessionVars {
    /// The job's reorganization variables.
    #[must_use]
    pub fn of_job(job: &tidb_model::Job) -> Self {
        let utc = tidb_datatype::SessionTimeZone::utc();
        let zone = job
            .reorg_meta
            .as_ref()
            .and_then(|meta| meta.read().location.clone())
            .map_or(utc.clone(), |location| {
                let location = location.read();
                let name = location.name.to_string();
                if location.offset == 0 {
                    match tidb_timeutil::load_location(&name) {
                        Ok(tidb_timeutil::TimeZone::Named(zone)) => {
                            tidb_datatype::SessionTimeZone::Named(zone)
                        }
                        Ok(tidb_timeutil::TimeZone::Local) => {
                            tidb_datatype::SessionTimeZone::Local
                        }
                        Ok(tidb_timeutil::TimeZone::Fixed { name, offset_secs }) => {
                            tidb_datatype::SessionTimeZone::Fixed { name, offset_secs }
                        }
                        Err(_) => utc,
                    }
                } else {
                    tidb_datatype::SessionTimeZone::Fixed {
                        name,
                        offset_secs: location.offset as i32,
                    }
                }
            });
        let sql_mode = job
            .reorg_meta
            .as_ref()
            .map_or(job.sql_mode as i64, |meta| meta.read().sql_mode as i64);
        Self {
            sql_mode,
            time_zone: zone,
        }
    }
}

/// Go `checkForeignKeyConstrain(ctx, w, job.SchemaName, tblInfo.Name.L,
/// fkInfo, fkCheck)`'s arguments, owed by an `ADD FOREIGN KEY` under
/// `foreign_key_checks = ON` before the key becomes public.
#[derive(Clone, Debug)]
pub struct ForeignKeyCheck {
    /// `job.SchemaName`.
    pub schema: String,
    /// The child table's lowercase name.
    pub table: String,
    /// The key being added.
    pub foreign_key: Box<tidb_model::table::FKInfo>,
}

/// Go `pkg/ddl/session.Pool` as the DDL package uses it: the sessions the
/// domain hands DDL, for a job the domain submits itself and for the data
/// checks the owner runs as restricted SQL -- `checkModifyColumnData` and
/// `checkForNullValue`, `checkForeignKeyConstrain`,
/// `verifyRemainRecordsForCheckConstraint` and
/// `checkExchangePartitionRecordValidation`, whose queries the DDL package
/// builds. The node's SQL layer implements it; the DDL code only holds it.
pub trait DdlSessionPool: Send + Sync {
    /// A system session's DDL context, for a job no user session submits
    /// (Go's domain takes one from its system session pool).
    fn system_session(&self) -> Box<dyn crate::session_context::DdlSessionContext>;

    /// Go `w.sessPool.Get()` and that session's
    /// `GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil, sql)`: one
    /// statement on an internal session, in that session's own transaction,
    /// so it reads committed data rather than the job's.
    ///
    /// # Errors
    ///
    /// The statement's own error, as Go's `errors.Trace(err)` returns it.
    fn exec_restricted_sql(
        &self,
        sql: &str,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_terror::MysqlError>;

    /// [`Self::exec_restricted_sql`] over Go's `%n` / `%?` template and its
    /// arguments: `ExecRestrictedSQL(ctx, nil, sql, params...)`, which escapes
    /// them as `sqlescape.EscapeSQL` does.
    ///
    /// # Errors
    ///
    /// The escaping's error, or the statement's.
    fn exec_restricted_sql_with(
        &self,
        sql: &str,
        args: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_terror::MysqlError> {
        let escaped = tidb_sqlescape::escape_sql_text(sql, args)
            .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))?;
        self.exec_restricted_sql(&escaped)
    }

    /// Go `sctx.GetStore().CurrentVersion(kv.GlobalTxnScope)` on one of the
    /// pool's sessions (`getNowTSO`): a fresh timestamp from the store.
    ///
    /// # Errors
    ///
    /// The store's failure to hand out a timestamp.
    fn current_version(&self) -> Result<u64, tidb_terror::MysqlError> {
        Err(tidb_terror::MysqlError::unknown(
            "this DDL session pool has no store",
        ))
    }

    /// Go `sessPool.Get()`: a system session of the pool whose statements
    /// run in its own transactions.
    ///
    /// # Errors
    ///
    /// The session's failure to open.
    fn open_sql_session(&self) -> Result<Box<dyn StepSqlSession>, tidb_terror::MysqlError> {
        Err(tidb_terror::MysqlError::unknown(
            "this DDL session pool has no sessions",
        ))
    }

    /// Whether [`Self::open_step_session`] can serve a step now: a node's
    /// pool can once its domain is bound (the node builds the pool before
    /// the domain exists); a step without one stages its system-table rows
    /// itself.
    fn has_step_sessions(&self) -> bool {
        false
    }

    /// Go `w.sess` for one job step (`w.sess.Begin`): an internal session in
    /// an optimistic transaction that reads at the step's `start_ts` and
    /// stages its writes, key flags included, into the step's `buffer`; the
    /// step commits them with its meta writes. Every statement of the step
    /// runs on the one session, as Go's do.
    ///
    /// # Errors
    ///
    /// The session's failure to open or begin.
    fn open_step_session(
        &self,
        _start_ts: u64,
        _buffer: &tidb_store_driver_txn::cluster_storage::MutationBuffer,
    ) -> Result<Box<dyn StepSqlSession>, tidb_terror::MysqlError> {
        Err(tidb_terror::MysqlError::unknown(
            "this DDL session pool has no step session",
        ))
    }

    /// One statement on a step session of its own
    /// ([`Self::open_step_session`]).
    ///
    /// # Errors
    ///
    /// The session's, the escaping's or the statement's error.
    fn exec_in_step_txn(
        &self,
        start_ts: u64,
        buffer: &tidb_store_driver_txn::cluster_storage::MutationBuffer,
        sql: &str,
        args: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_terror::MysqlError> {
        self.open_step_session(start_ts, buffer)?.execute(sql, args)
    }
}

/// Go `w.sess` during one step (see [`DdlSessionPool::open_step_session`]).
/// Dropping it ends the session and leaves its writes in the step's buffer.
pub trait StepSqlSession: Send {
    /// Go `w.sess.Execute(ctx, sql, label)`.
    ///
    /// # Errors
    ///
    /// The escaping's or the statement's error.
    fn execute(
        &mut self,
        sql: &str,
        args: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_terror::MysqlError>;
}
