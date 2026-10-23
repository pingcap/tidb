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

//! The session-layer bootstrap: Go `pkg/session/bootstrap.go`'s
//! `doDDLWorks`/`doDMLWorks`, reduced to the system tables this tier actually
//! serves. This includes `mysql.user` (Go `metadef.CreateUserTable`
//! plus `doDMLWorks`' root row -- account statements keep it in sync, see
//! `crate::user_table`), `mysql.bind_info`, the statistics metadata and lock
//! tables used by `LOCK STATS`, and the two blacklist tables
//! `ADMIN RELOAD` reads (`crate::blacklist`) -- a statement that can only
//! report what a table holds needs the table to exist first.
//!
//! Faithful to Go in shape: the table is created by running its OWN
//! `CREATE TABLE` text (`tidb_metadef::system_tables_def`, the transcreated
//! `metadef.CreateBindInfoTable`), and the builtin lock row is inserted the
//! way `insertBuiltinBindInfoRow` writes it -- zero timestamps included,
//! which is why the insert runs under a permissive date-mode context: Go's
//! bootstrap statements go through an internal session whose `sql_mode` does
//! not carry `NO_ZERO_DATE`.
//!
//! Runs once for the standalone fresh store built by [`Session::default`]. A
//! front end that opens a session on an existing shared catalog
//! ([`Session::with_catalog`]) does not run bootstrap at all: like Go
//! `createSessionWithOpt`, it consumes the domain's already-bootstrapped
//! infoschema.

use crate::Session;
use tidb_executor::DriverError;

/// Go master's `systemTablesOfMaterializedViewNextGenVersion`, which
/// `tidb_metadef::BOOTSTRAP_TABLES` carries for the cluster tier.
const MATERIALIZED_VIEW_TABLES: [&str; 5] = [
    "tidb_mview_refresh_info",
    "tidb_mlog_purge_info",
    "tidb_mview_refresh_hist",
    "tidb_mview_refresh_alert",
    "tidb_mlog_purge_hist",
];

/// Go `bindinfo.BuiltinPseudoSQL4BindLock`: the pseudo statement the builtin
/// row carries, used by Go to simulate `LOCK TABLE` on `mysql.bind_info`.
pub(crate) const BUILTIN_PSEUDO_SQL_FOR_BIND_LOCK: &str = "builtin_pseudo_sql_for_bind_lock";

impl Session {
    /// Creates the system tables a fresh catalog is born with, for a caller
    /// that BUILT the catalog and therefore knows it is a fresh store.
    ///
    /// [`Session::default`] calls this for the catalog it creates itself. A
    /// front end that hands in its own catalog
    /// ([`Session::with_catalog`](crate::Session::with_catalog)) is the one
    /// that knows which case it is in -- Go decides the same question from
    /// `getStoreBootstrapVersion`, not from anything a session can see -- so
    /// a harness that opens a fresh store calls this once, and the cluster
    /// loader, whose peer already ran Go's own bootstrap, does not.
    ///
    /// Idempotent: the presence test short-circuits and the `CREATE` text
    /// carries `IF NOT EXISTS`.
    pub fn bootstrap_fresh_store(&mut self) {
        self.bootstrap_system_tables();
        // Go's domain init then rebuilds the sysvar cache from
        // `mysql.global_variables`, calling each GLOBAL sysvar's SetGlobal
        // hook.
        self.load_global_variables_table();
    }

    /// Go `Domain.rebuildSysVarCache`'s read of `mysql.global_variables`:
    /// the GLOBAL tier answers the rows bootstrap seeded
    /// (`GlobalSystemVariableInitialValue`, so `tidb_row_format_version` is
    /// 2 although its registry value is 1), and this connection -- the
    /// store's first session, which Go opens only after the cache is loaded
    /// -- copies them as `loadCommonGlobalVariablesIfNeeded` does.
    fn load_global_variables_table(&mut self) {
        let loaded = self.fetch_global_variables_table();
        self.finish_bootstrap_statement();
        let Ok(rows) = loaded else {
            return;
        };
        let globals = self.vars.global_sysvars();
        globals.load_from_cluster(rows);
        globals.mark_catalog_backed();
        // Go `BootstrapSession` -> `Domain.LoadSysVarCacheLoop`, here over the
        // store's committed catalog (Go reads it on a system session).
        let catalog = std::sync::Arc::downgrade(&self.shared_catalog());
        globals.start_load_sys_var_cache_loop(Box::new(move || {
            let catalog = catalog.upgrade()?;
            let catalog = catalog.lock().unwrap_or_else(|poison| poison.into_inner());
            Some(
                tidb_executor::run_select_meta_in(
                    tidb_domain::sysvar_cache::SYSVAR_TABLE_QUERY,
                    &catalog,
                    "mysql",
                    &tidb_executor::StmtContext::default(),
                )
                .map(|(_columns, rows)| crate::variables::global_variables_rows(&rows))
                .map_err(|error| error.to_mysql_error().message),
            )
        }));
        self.vars
            .seed_from_globals(globals)
            .expect("bootstrap: the seeded global variables must load into a session");
    }

    /// Creates the system tables a fresh catalog is born with.
    ///
    /// Idempotent by construction: the presence test short-circuits, and the
    /// `CREATE` text itself carries `IF NOT EXISTS`. A failure is a build
    /// defect (the DDL/DML surface regressed under this tier's own schema
    /// text), not a user error, so it panics rather than leaving every later
    /// binding statement to fail with a misleading missing-table message.
    pub(crate) fn bootstrap_system_tables(&mut self) {
        let has_required_tables = self
            .with_catalog_mut(|catalog| {
                Ok([
                    "user",
                    "password_history",
                    "bind_info",
                    "stats_meta",
                    "stats_table_locked",
                ]
                .into_iter()
                .all(|table| catalog.contains_in("mysql", table)))
            })
            .unwrap_or(false);
        if has_required_tables {
            return;
        }
        let result = self.run_bootstrap_statements();
        self.finish_bootstrap_statement();
        result.expect("bootstrap: the mysql system tables must be creatable by this tier's own DDL");
    }

    /// Bootstrap is an internal execution boundary, completed before the
    /// connection installs its spill storage and memory arbitrator: the
    /// statement memory its internal statements opened is released here.
    fn finish_bootstrap_statement(&mut self) {
        if let Some(authority) = self.statement_result_authority.get_mut().take() {
            authority.statement_memory().finish_statement();
        }
    }

    fn run_bootstrap_statements(&mut self) -> Result<(), DriverError> {
        let settings = tidb_executor::CreateTableSettings {
            sql_mode: self.scanner_sql_mode(),
            parser_config: self.vars.build_parser_config(),
            foreign_key_checks: self.foreign_key_checks(),
            enable_check_constraint: false,
            // Go `BootstrapSession` pins the bootstrap session's
            // `EnableClusteredIndex` to `ClusteredIndexDefModeIntOnly` "to be
            // compatible with old TiDB versions": `mysql.global_grants`' composite
            // primary key is NONCLUSTERED whatever the connection's default.
            clustered_index_mode: tidb_vardef::modes::ClusteredIndexDefMode::INT_ONLY,
        };
        // Go's bootstrap session never loads the global variables
        // (`loadCommonGlobalVariablesIfNeeded` returns early while Initing),
        // so its DDL runs with the zero, non-strict SQL mode: a TEXT column's
        // '' default (`mysql.global_priv.Priv`) is a warning, not an error.
        let ddl_ctx = self
            .statement_context(false)
            .with_strict(false)
            .with_ddl_sql_mode(0);
        // Go `doDDLWorks`: the DDL tables (`InitDDLTables`, every version a
        // fresh store lacks), then this branch's `versionedBootstrapSchemas`
        // -- `systemTablesOfBaseNextGenVersion` and
        // `systemTablesOfMaskingPolicyNextGenVersion`, `mysql.user` first.
        // The DDL tables keep their reserved metadef ids under both kernels;
        // the others take them under the next-gen kernel and, under the
        // classic kernel, the ids their `CREATE TABLE` jobs draw (table
        // `4 + 2k`, each job the odd id after it, `mysql` itself being 1 and
        // `sys` 2 / job 3). Master's materialized-view version is not
        // bootstrapped by this branch.
        let classic = tidb_exec::mysql_bootstrap::BootstrapLayout::of_kernel()
            == tidb_exec::mysql_bootstrap::BootstrapLayout::Classic;
        let ddl_tables = tidb_metadef::DDL_TABLE_VERSION_TABLES
            .iter()
            .flat_map(|version| version.tables.iter())
            .map(|table| (table, table.id));
        let other_tables = tidb_metadef::BOOTSTRAP_TABLES
            .iter()
            .filter(|table| !MATERIALIZED_VIEW_TABLES.contains(&table.name))
            .enumerate()
            .map(|(position, table)| {
                let id = if classic {
                    4 + 2 * i64::try_from(position).expect("a table position fits in i64")
                } else {
                    table.id
                };
                (table, id)
            });
        let tables = ddl_tables.chain(other_tables);
        self.with_catalog_mut(|catalog| {
            for (table, id) in tables {
                tidb_executor::run_create_system_table_in(
                    table.create_sql,
                    catalog,
                    "mysql",
                    settings,
                    &ddl_ctx,
                    id,
                )?;
            }
            if classic {
                // `mysql` is the first id Go draws, `sys` the second, `test`
                // follows the 53 tables and the two views (114). The tables'
                // jobs end at 109; the view below draws 110.
                catalog.register_database_with_id("mysql", 1);
                catalog.register_database_with_id("test", 114);
                catalog.reserve_ids_through(109);
            }
            Ok(())
        })?;

        // Go `insertBuiltinBindInfoRow`, minus the `HIGH_PRIORITY` word (a
        // scheduler hint this tier has no scheduler for). The zero timestamps
        // are Go's own values, admitted by the permissive date modes below.
        // Built from the session's own DML context so the statement clock is
        // attached: `mysql.user.Password_last_changed` defaults to
        // `CURRENT_TIMESTAMP()`, which the insert path evaluates.
        let insert_ctx = self
            .statement_context(true)
            .with_date_modes(tidb_datatype::DateModes {
                no_zero_date: false,
                no_zero_in_date: false,
                allow_invalid_dates: false,
            });
        let insert = format!(
            "INSERT INTO mysql.bind_info(original_sql, bind_sql, default_db, status, \
             create_time, update_time, charset, collation, source) VALUES \
             ('{lock}', '{lock}', 'mysql', 'builtin', '0000-00-00 00:00:00', \
             '0000-00-00 00:00:00', '', '', 'builtin')",
            lock = BUILTIN_PSEUDO_SQL_FOR_BIND_LOCK
        );
        self.with_staged_catalog(|catalog| {
            tidb_executor::run_insert_in(&insert, catalog, "mysql", &insert_ctx).map(|_| ())
        })?;
        // Go `doDDLWorks`: `mysql.tidb_mdl_view` (`metadef.CreateTiDBMDLView`)
        // after the builtin binding row.
        let Ok(tidb_ast::Stmt::Ddl(ddl)) =
            tidb_parser::parse(tidb_metadef::system_tables_def::CREATE_TI_DBMDLVIEW)
        else {
            return Err(DriverError::unsupported(
                "metadef.CreateTiDBMDLView does not parse as a statement",
            ));
        };
        let tidb_ast::DdlStmt::CreateView(create) = &*ddl else {
            return Err(DriverError::unsupported(
                "metadef.CreateTiDBMDLView is not a CREATE VIEW",
            ));
        };
        self.with_catalog_mut(|catalog| {
            tidb_executor::run_create_view_in(&**create, catalog, "mysql", &ddl_ctx)
        })?;
        // Go `doDDLWorks` ends with `ALTER RESOURCE GROUP default
        // BACKGROUND=(TASK_TYPES='stats')` (`kv.InternalTxnStats`).
        self.bootstrap_default_resource_group_background()?;
        if classic {
            // Go's counter ends at 116 with the resource-group job, and each
            // of the 58 jobs is in the history.
            self.with_catalog_mut(|catalog| {
                catalog.reserve_ids_through(116);
                file_classic_bootstrap_jobs(catalog)
            })?;
        }

        // Go `doDMLWorks`' bootstrap `root`@`%` row, minus the
        // `HIGH_PRIORITY` scheduler hint: every static privilege column `Y`
        // except `Account_locked` (`N`), an EMPTY `authentication_string`
        // under `mysql_native_password`, NULL `User_attributes`, empty
        // `Token_issuer`. Columns Go omits (`Password_expired`,
        // `Password_last_changed`, `Max_user_connections`, ...) take their
        // `CreateUserTable` declared defaults exactly as they do in Go.
        //
        // Written even for a session whose `PrivilegeRegistry` was
        // bootstrapped from a different account list: Go's answer to "what
        // is in mysql.user on a fresh store" is this one row, and the
        // registry-vs-table deviation is documented in `crate::user_table`.
        let root = "INSERT INTO mysql.user (Host,User,authentication_string,plugin,Select_priv,\
             Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Process_priv,Grant_priv,\
             References_priv,Alter_priv,Show_db_priv,Super_priv,Create_tmp_table_priv,\
             Lock_tables_priv,Execute_priv,Create_view_priv,Show_view_priv,Operate_view_priv,Create_routine_priv,\
             Alter_routine_priv,Index_priv,Create_user_priv,Event_priv,Repl_slave_priv,\
             Repl_client_priv,Trigger_priv,Create_role_priv,Drop_role_priv,Account_locked,\
             Shutdown_priv,Reload_priv,FILE_priv,Config_priv,Create_Tablespace_Priv,\
             User_attributes,Token_issuer) VALUES (\"%\", \"root\", \"\", \
             \"mysql_native_password\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \
             \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \
             \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"N\", \"Y\", \"Y\", \"Y\", \"Y\", \
             \"Y\", null, \"\")";
        self.with_staged_catalog(|catalog| {
            tidb_executor::run_insert_in(root, catalog, "mysql", &insert_ctx).map(|_| ())
        })?;

        // Go `doDMLWorks`: every GLOBAL-scoped variable's initial value
        // (`GlobalSystemVariableInitialValue`, which differs from the
        // registry value for the few defaults changed for new installs only)
        // goes into `mysql.global_variables`.
        let escape = |text: &str| {
            String::from_utf8(tidb_util::sqlescape::escape_string(text))
                .expect("escaping keeps UTF-8 text UTF-8")
        };
        let values = crate::sysvar::SYS_VARS
            .iter()
            .filter(|definition| definition.has_global_scope())
            .map(|definition| {
                format!(
                    "(\"{}\", \"{}\")",
                    escape(definition.name),
                    escape(&crate::vars::global_system_variable_initial_value(
                        definition
                    ))
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let global_variables = format!("INSERT INTO mysql.global_variables VALUES {values}");
        self.with_staged_catalog(|catalog| {
            tidb_executor::run_insert_in(&global_variables, catalog, "mysql", &insert_ctx)
                .map(|_| ())
        })
    }
}

/// The 58 DDL jobs Go's classic bootstrap runs, filed in the meta history as
/// its worker leaves them: `CREATE DATABASE sys` (database 2, job 3), one
/// `CREATE TABLE` per `mysql.*` table (table `4 + 2k`, job `5 + 2k`), the two
/// views (110 / 111 and 112 / 113), `CREATE DATABASE test` (114 / 115) and
/// `ALTER RESOURCE GROUP default` (job 116, group id 1), each at its own
/// schema version. Only the meta history is filled: the
/// `mysql.tidb_ddl_history` copy Go also writes is a SQL insert this tier
/// would pay for 58 times on every fresh store.
fn file_classic_bootstrap_jobs(catalog: &mut tidb_executor::Catalog) -> Result<(), DriverError> {
    use tidb_executor::ddl::job_history::done_schema_state;
    use tidb_model::{ActionType, JobState};

    struct BootstrapJob {
        action: ActionType,
        schema_id: i64,
        table_id: i64,
        schema_name: &'static str,
        table_name: &'static str,
        query: String,
        id: i64,
    }
    let mut jobs = vec![BootstrapJob {
        action: ActionType::ACTION_CREATE_SCHEMA,
        schema_id: 2,
        table_id: 0,
        schema_name: "sys",
        table_name: "",
        query: "CREATE DATABASE IF NOT EXISTS `sys`".to_owned(),
        id: 3,
    }];
    for (position, table) in tidb_metadef::BOOTSTRAP_TABLES
        .iter()
        .filter(|table| !MATERIALIZED_VIEW_TABLES.contains(&table.name))
        .enumerate()
    {
        let position = i64::try_from(position).expect("a table position fits in i64");
        jobs.push(BootstrapJob {
            action: ActionType::ACTION_CREATE_TABLE,
            schema_id: 1,
            table_id: 4 + 2 * position,
            schema_name: "mysql",
            table_name: table.name,
            query: table.create_sql.to_owned(),
            id: 5 + 2 * position,
        });
    }
    jobs.push(BootstrapJob {
        action: ActionType::ACTION_CREATE_VIEW,
        schema_id: 1,
        table_id: 110,
        schema_name: "mysql",
        table_name: "tidb_mdl_view",
        query: tidb_metadef::system_tables_def::CREATE_TI_DBMDLVIEW.to_owned(),
        id: 111,
    });
    jobs.push(BootstrapJob {
        action: ActionType::ACTION_CREATE_VIEW,
        schema_id: 2,
        table_id: 112,
        schema_name: "sys",
        table_name: "schema_unused_indexes",
        query: tidb_metadef::system_tables_def::CREATE_SCHEMA_UNUSED_INDEXES_VIEW.to_owned(),
        id: 113,
    });
    jobs.push(BootstrapJob {
        action: ActionType::ACTION_CREATE_SCHEMA,
        schema_id: 114,
        table_id: 0,
        schema_name: "test",
        table_name: "",
        query: "CREATE DATABASE IF NOT EXISTS test".to_owned(),
        id: 115,
    });
    jobs.push(BootstrapJob {
        action: ActionType::ACTION_ALTER_RESOURCE_GROUP,
        schema_id: 1,
        table_id: 0,
        schema_name: "default",
        table_name: "",
        query: format!(
            "ALTER RESOURCE GROUP `default` BACKGROUND=(TASK_TYPES='{}')",
            tidb_txnkv::INTERNAL_TXN_STATS
        ),
        id: 116,
    });
    for (position, bootstrap) in jobs.into_iter().enumerate() {
        let version = i64::try_from(position).expect("a job position fits in i64") + 1;
        let mut job = tidb_model::Job::default();
        job.version = tidb_model::JobVersion::V2;
        job.id = bootstrap.id;
        job.type_ = bootstrap.action;
        job.schema_id = bootstrap.schema_id;
        job.table_id = bootstrap.table_id;
        job.schema_name = bootstrap.schema_name.to_owned().into();
        job.table_name = bootstrap.table_name.to_owned().into();
        job.state = JobState::SYNCED;
        job.schema_state = done_schema_state(bootstrap.action);
        job.query = bootstrap.query.into();
        job.start_ts = catalog.allocate_tso();
        job.real_start_ts = catalog.allocate_tso();
        job.sequence_number = u64::try_from(version).expect("a version is positive");
        let mut history = tidb_model::HistoryInfo {
            schema_version: version,
            finished_ts: catalog.allocate_tso(),
            ..tidb_model::HistoryInfo::default()
        };
        if bootstrap.table_id != 0 {
            let mut info = tidb_model::TableInfo::default();
            info.id = bootstrap.table_id;
            info.name = tidb_ast::CiString::new(bootstrap.table_name);
            history.table_info = Some(tidb_model::go_runtime::GoShared::new(info));
        } else if bootstrap.action == ActionType::ACTION_CREATE_SCHEMA {
            let mut info = tidb_model::DBInfo::default();
            info.id = bootstrap.schema_id;
            info.name = tidb_ast::CiString::new(bootstrap.schema_name);
            history.db_info = Some(tidb_model::go_runtime::GoShared::new(info));
        }
        job.binlog_info = Some(tidb_model::go_runtime::GoShared::new(history));
        catalog.add_history_ddl_job(&mut job)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A classic-kernel store has Go's 58 bootstrap jobs in its DDL history
    /// (jobs 3 .. 116) and its next user table is 117 with job 118.
    #[test]
    fn a_classic_store_has_the_bootstrap_jobs_in_its_history() {
        if tidb_config::kerneltype::is_next_gen() {
            return;
        }
        let mut session = Session::new();
        let text = crate::tests_support::row_text;
        let jobs = text(session.run("ADMIN SHOW DDL JOBS 100"));
        assert_eq!(jobs.len(), 58);
        assert_eq!(
            (jobs[0][0].as_str(), jobs[0][3].as_str()),
            ("116", "alter resource group")
        );
        assert_eq!((jobs[57][0].as_str(), jobs[57][1].as_str()), ("3", "sys"));
        session.run("CREATE TABLE test.layout_t (a INT)").unwrap();
        let newest = text(session.run("ADMIN SHOW DDL JOBS 1"));
        assert_eq!(newest[0][0], "118");
        assert_eq!(newest[0][6], "117");
    }

    /// Go `createSessionWithOpt` never runs `bootstrap()`; bootstrap belongs
    /// to `bootstrapSessionImpl` and precedes ordinary session creation.
    #[test]
    fn shared_catalog_session_does_not_bootstrap_a_discarded_store() {
        let catalog = crate::SharedCatalog::default();
        let session = Session::with_catalog(crate::SharedCatalog::clone(&catalog));

        assert!(!catalog
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_in("mysql", "user"));
        assert!(session.table_delta_savepoint().is_empty());
    }
}
