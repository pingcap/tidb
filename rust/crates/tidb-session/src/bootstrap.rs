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
//! serves. Today that is four: `mysql.user` (Go `metadef.CreateUserTable`
//! plus `doDMLWorks`' root row -- account statements keep it in sync, see
//! `crate::user_table`), `mysql.bind_info`, whose absence was the measured
//! gap the GLOBAL-binding refusal pointed at, and the two blacklist tables
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
//! Runs once per catalog, from the ONE place a session over a fresh catalog
//! is built ([`Session::default`]). A front end that installs a shared
//! catalog afterwards ([`Session::with_catalog`]) inherits whatever bootstrap
//! that catalog's own creator ran; the cluster-loaded catalog is deliberately
//! NOT bootstrapped here, because a locally created `bind_info` over cluster
//! storage would be a table no peer node reads -- the real cluster's copy
//! comes from Go's own bootstrap.

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
    }

    /// Creates the system tables a fresh catalog is born with.
    ///
    /// Idempotent by construction: the presence test short-circuits, and the
    /// `CREATE` text itself carries `IF NOT EXISTS`. A failure is a build
    /// defect (the DDL/DML surface regressed under this tier's own schema
    /// text), not a user error, so it panics rather than leaving every later
    /// binding statement to fail with a misleading missing-table message.
    pub(crate) fn bootstrap_system_tables(&mut self) {
        let has_bind_info = self
            .with_catalog_mut(|catalog| Ok(catalog.contains_in("mysql", "bind_info")))
            .unwrap_or(false);
        if has_bind_info {
            return;
        }
        self.run_bootstrap_statements()
            .expect("bootstrap: the mysql system tables must be creatable by this tier's own DDL");
    }

    fn run_bootstrap_statements(&mut self) -> Result<(), DriverError> {
        let settings = tidb_executor::CreateTableSettings {
            sql_mode: self.scanner_sql_mode(),
            foreign_key_checks: self.foreign_key_checks(),
            enable_check_constraint: false,
            clustered_index_mode: self.clustered_index_mode(),
        };
        let ddl_ctx = self.statement_context(false);
        for create in [
            // Go creates `mysql.user` FIRST (`tableList` in `doDDLWorks`
            // starts with `CreateUserTable`); kept first here so a failure
            // names the account table rather than a bystander.
            tidb_metadef::system_tables_def::CREATE_USER_TABLE,
            tidb_metadef::system_tables_def::CREATE_BIND_INFO_TABLE,
            // Go bootstraps these two empty (`doDDLWorks`); only an upgrade
            // from a pre-v4 cluster seeds `expr_pushdown_blacklist` rows
            // (`writeDefaultExprPushDownBlacklist`), and a fresh cluster --
            // which is what every session here is -- starts with neither
            // table holding anything.
            tidb_metadef::system_tables_def::CREATE_EXPR_PUSHDOWN_BLACKLIST_TABLE,
            tidb_metadef::system_tables_def::CREATE_OPT_RULE_BLACKLIST_TABLE,
        ] {
            self.with_catalog_mut(|catalog| {
                tidb_executor::run_create_table_in(create, catalog, "mysql", settings, &ddl_ctx)
                    .map(|_| ())
            })?;
        }

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
        self.with_catalog_mut(|catalog| {
            tidb_executor::run_insert_in(&insert, catalog, "mysql", &insert_ctx).map(|_| ())
        })?;

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
             Lock_tables_priv,Execute_priv,Create_view_priv,Show_view_priv,Create_routine_priv,\
             Alter_routine_priv,Index_priv,Create_user_priv,Event_priv,Repl_slave_priv,\
             Repl_client_priv,Trigger_priv,Create_role_priv,Drop_role_priv,Account_locked,\
             Shutdown_priv,Reload_priv,FILE_priv,Config_priv,Create_Tablespace_Priv,\
             User_attributes,Token_issuer) VALUES (\"%\", \"root\", \"\", \
             \"mysql_native_password\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \
             \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \
             \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"Y\", \"N\", \"Y\", \"Y\", \"Y\", \"Y\", \
             \"Y\", null, \"\")";
        self.with_catalog_mut(|catalog| {
            tidb_executor::run_insert_in(root, catalog, "mysql", &insert_ctx).map(|_| ())
        })
    }
}
