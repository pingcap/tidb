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
        /// Go master's `systemTablesOfMaterializedViewNextGenVersion`, which
        /// `tidb_metadef::BOOTSTRAP_TABLES` carries for the cluster tier.
        const MATERIALIZED_VIEW_TABLES: [&str; 5] = [
            "tidb_mview_refresh_info",
            "tidb_mlog_purge_info",
            "tidb_mview_refresh_hist",
            "tidb_mview_refresh_alert",
            "tidb_mlog_purge_hist",
        ];
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
        // `systemTablesOfMaskingPolicyNextGenVersion`, `mysql.user` first --
        // each under its reserved metadef ID. Master's materialized-view
        // version is not bootstrapped by this branch.
        let tables = tidb_metadef::DDL_TABLE_VERSION_TABLES
            .iter()
            .flat_map(|version| version.tables.iter())
            .chain(
                tidb_metadef::BOOTSTRAP_TABLES
                    .iter()
                    .filter(|table| !MATERIALIZED_VIEW_TABLES.contains(&table.name)),
            );
        self.with_catalog_mut(|catalog| {
            for table in tables {
                tidb_executor::run_create_system_table_in(
                    table.create_sql,
                    catalog,
                    "mysql",
                    settings,
                    &ddl_ctx,
                    table.id,
                )?;
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

#[cfg(test)]
mod tests {
    use super::*;

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
