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

//! Who the session is, and what a front end attaches to make that answerable:
//! the authenticated identity, the active roles, the process-list registration,
//! the privilege registry, the shared GLOBAL sysvar table, and sandbox mode.
//!
//! Go sets all of this on `SessionVars` (plus the `Domain`-owned managers) once
//! a connection authenticates. Everything here is either a front end's setter
//! or a read the privilege checks and the identity builtins make -- a session
//! with no front end has none of it, which is why every check falls back to
//! "unrestricted" rather than "denied".

use crate::privilege::PrivilegeTypeExt;
use std::sync::Arc;

use tidb_ast::{SessionStmt, Stmt};

use crate::{privilege, process, vars, DriverError, Session};
use tidb_util::stringutil::go_to_lower;

/// Distinct prepared texts whose privilege requests one session keeps.

impl Session {
    /// Checks one already-resolved table name without rebuilding an AST path.
    pub(crate) fn require_named_table_privilege(
        &self,
        database: &str,
        table: &str,
        privilege: privilege::PrivilegeType,
    ) -> Result<(), DriverError> {
        let granted = match crate::table_privilege::mem_db_verdict_mask(database, privilege.bit()) {
            Some(verdict) => verdict,
            None => self.has_scoped_privilege(database, table, privilege),
        };
        if granted {
            return Ok(());
        }
        let Some((_, user, host)) = self.privilege_context() else {
            return Ok(());
        };
        Err(DriverError::TableAccessDenied {
            privilege: privilege.print_name(),
            user: user.to_owned(),
            host: host.to_owned(),
            table: go_to_lower(table),
        })
    }

    /// Go `buildShow` (`planbuilder.go:3632`) for SHOW STATS_META,
    /// STATS_HISTOGRAMS, STATS_BUCKETS and STATS_LOCKED: SELECT on the
    /// `mysql` table the parser names, refused as the `SHOW` command.
    pub(crate) fn require_show_stats_privilege(&self, table: &str) -> Result<(), DriverError> {
        if self.has_scoped_privilege("mysql", table, tidb_mysql::SelectPriv) {
            return Ok(());
        }
        let Some((_, user, host)) = self.privilege_context() else {
            return Ok(());
        };
        Err(DriverError::TableAccessDenied {
            privilege: "SHOW",
            user: user.to_owned(),
            host: host.to_owned(),
            table: table.to_owned(),
        })
    }

    /// The account table plus this session's authenticated identity -- what
    /// every privilege gate needs, and `None` for a session that is
    /// UNRESTRICTED because it has no front end (no registry attached, or no
    /// authenticated identity). Go's executors treat a missing privilege
    /// manager as "miss privilege checker" and refuse; this tier's in-process
    /// sessions legitimately have none, so they pass instead -- the rule
    /// every check here already followed, now stated once.
    pub(crate) fn privilege_context(&self) -> Option<(&privilege::PrivilegeRegistry, &str, &str)> {
        if self.privilege_bypassed {
            return None;
        }
        let registry = self.privileges.as_ref()?;
        let (user, host) = self.current_identity()?;
        Some((registry, user, host))
    }

    /// Go `RequestVerification(activeRoles, database, table, "", priv)`:
    /// whether this session holds `global_priv` at that scope, through its
    /// own grants or a role. An EMPTY `database` and `table` is Go's GLOBAL
    /// (`*.*`) scope; an empty `table` alone is schema scope.
    ///
    /// The *error* a denied caller reports is the caller's, not this
    /// method's: Go words it per statement (`ANALYZE` reports
    /// `ErrTableaccessDenied` naming INSERT or SELECT), and only the caller
    /// knows which privilege it was asking about.
    #[must_use]
    pub fn has_scoped_privilege(
        &self,
        database: &str,
        table: &str,
        global_priv: privilege::PrivilegeType,
    ) -> bool {
        let Some((registry, user, host)) = self.privilege_context() else {
            return true;
        };
        scoped_privilege(
            registry,
            user,
            host,
            self.active_roles(),
            database,
            table,
            global_priv,
        )
    }

    /// An owned [`Self::has_scoped_privilege`] for checks Go makes while
    /// EVALUATING an expression (`NEXTVAL`/`LASTVAL`/`SETVAL` ask
    /// `RequestVerification` from the builtin), which run with the session
    /// out of reach. `None` is an unrestricted session.
    #[must_use]
    pub(crate) fn sequence_privilege_check(&self) -> Option<tidb_executor::SequencePrivilegeCheck> {
        let (registry, user, host) = self.privilege_context()?;
        let registry = registry.clone();
        let (user, host) = (user.to_owned(), host.to_owned());
        let roles = self.active_roles().to_vec();
        Some(Arc::new(
            move |database: &str, sequence: &str, insert: bool| {
                let global_priv = if insert {
                    tidb_mysql::InsertPriv
                } else {
                    tidb_mysql::SelectPriv
                };
                scoped_privilege(
                    &registry,
                    &user,
                    &host,
                    &roles,
                    database,
                    sequence,
                    global_priv,
                )
            },
        ))
    }

    /// Go `privilege.Manager.RequestVerification(activeRoles, ...)` for the
    /// planner: whether this session's account, with its active roles, holds
    /// a privilege. `None` is a session that enforces no privileges.
    pub(crate) fn session_privilege_check(&self) -> Option<tidb_executor::SessionPrivilegeCheck> {
        let (registry, user, host) = self.privilege_context()?;
        let registry = registry.clone();
        let (user, host) = (user.to_owned(), host.to_owned());
        let roles = self.active_roles().to_vec();
        Some(Arc::new(
            move |database: &str,
                  table: &str,
                  _column: &str,
                  privilege: tidb_mysql::PrivilegeType| {
                crate::table_privilege::global_privileges_of(privilege)
                    .into_iter()
                    .any(|global| {
                        scoped_privilege(&registry, &user, &host, &roles, database, table, global)
                    })
            },
        ))
    }

    /// [`Self::has_scoped_privilege`] at table scope, the name the executor
    /// arms that predate the general form call it by.
    #[must_use]
    pub fn has_table_privilege(
        &self,
        database: &str,
        table: &str,
        global_priv: privilege::PrivilegeType,
    ) -> bool {
        self.has_scoped_privilege(database, table, global_priv)
    }

    /// Go `RequestVerification(activeRoles, database, table, "", mask)` for a
    /// multi-bit `mask`, which Go reads as "ANY of these privileges".
    ///
    /// The virtual schemas answer before any grant is read, exactly as they
    /// do for the single-privilege form
    /// ([`crate::table_privilege::mem_db_verdict_mask`]).
    #[must_use]
    pub fn has_any_scoped_privilege(&self, database: &str, table: &str, mask: u64) -> bool {
        let Some((registry, user, host)) = self.privilege_context() else {
            return true;
        };
        if let Some(verdict) = self.sem_table_verdict(registry, user, host, database, table, mask) {
            return verdict;
        }
        if let Some(verdict) = crate::table_privilege::mem_db_verdict_mask(database, mask) {
            return verdict;
        }
        registry.has_priv_mask_with_roles(user, host, self.active_roles(), database, table, mask)
    }

    /// The owned snapshot of this session's identity that the
    /// `information_schema` retrievers filter their rows with, since they run
    /// with the catalog borrowed and cannot ask the session anything.
    #[must_use]
    pub(crate) fn schema_visibility(&self) -> crate::infoschema::SchemaVisibility {
        match self.privilege_context() {
            Some((registry, user, host)) => crate::infoschema::SchemaVisibility::for_session(
                registry.clone(),
                user,
                host,
                self.active_roles(),
            ),
            None => crate::infoschema::SchemaVisibility::unrestricted(),
        }
    }

    /// What the account tables read of this session (see
    /// [`crate::account_state::AccountState`]).
    pub(crate) fn account_state(&self) -> crate::account_state::AccountState {
        crate::account_state::AccountState {
            privileges: self.privileges.clone(),
            privilege_bypassed: self.privilege_bypassed,
            login_user: self.vars.login_user.clone(),
            current_user: self.vars.current_user.clone(),
            active_roles: self.vars.active_roles.to_vec(),
            restricted_sql: self.restricted_sql_executor(),
        }
    }

    /// What the variable tables read of this session (see
    /// [`crate::account_state::VariablesState`]): its variables, and whether
    /// SEM hides the invisible ones (`sem_hides_sysvar` without the name).
    pub(crate) fn variables_state(&self) -> crate::account_state::VariablesState {
        crate::account_state::VariablesState {
            vars: self.vars.clone(),
            hide_invisible: tidb_util::sem_compat::is_enabled()
                && self.privilege_context().is_some()
                && !self.has_dynamic_privilege("RESTRICTED_VARIABLES_ADMIN", false),
        }
    }

    /// Go `UserPrivileges.DBIsVisible` (`privileges.go` around line 935): may
    /// this session SEE the schema at all.
    ///
    /// A session with no front end is unrestricted, exactly as every other
    /// check here is, and Go's own callers guard on
    /// `checker != nil && SessionVars.User != nil` for the same reason.
    #[must_use]
    pub fn database_is_visible(&self, database: &str) -> bool {
        let Some((registry, user, host)) = self.privilege_context() else {
            return true;
        };
        registry.db_is_visible_with_roles(user, host, self.active_roles(), database)
    }

    /// The `ErrDBaccessDenied` (1044) gate Go's `executeUse`,
    /// `fetchShowTables`, `fetchShowTableStatus` and
    /// `fetchShowCreateDatabase` all apply BEFORE they look the schema up --
    /// which is why naming a schema that does not exist reports 1044 and not
    /// `ErrBadDB` for an account that could not have seen it either way
    /// (measured: `SHOW TABLES IN nosuchdb` as an unprivileged user is
    /// 1044).
    pub(crate) fn require_visible_database(&self, database: &str) -> Result<(), DriverError> {
        if self.database_is_visible(database) {
            return Ok(());
        }
        let (user, host) = self.current_identity().unwrap_or_default();
        Err(DriverError::DbAccessDenied {
            user: user.to_owned(),
            host: host.to_owned(),
            database: database.to_owned(),
        })
    }

    /// Go `RequestDynamicVerification(activeRoles, name, grantable)`. The
    /// registry's own SUPER fallback is part of this, which is why Go's
    /// denial messages name `SUPER or <PRIVILEGE>`.
    #[must_use]
    pub fn has_dynamic_privilege(&self, name: &str, with_grant: bool) -> bool {
        let Some((registry, user, host)) = self.privilege_context() else {
            return true;
        };
        registry.has_dynamic_priv_with_roles(user, host, self.active_roles(), name, with_grant)
    }

    fn sem_table_verdict(
        &self,
        registry: &privilege::PrivilegeRegistry,
        user: &str,
        host: &str,
        database: &str,
        table: &str,
        mask: u64,
    ) -> Option<bool> {
        if !tidb_util::sem_compat::is_enabled() {
            return None;
        }
        let has_restricted_tables_admin = registry.has_dynamic_priv_with_roles(
            user,
            host,
            self.active_roles(),
            "RESTRICTED_TABLES_ADMIN",
            false,
        );
        crate::table_privilege::sem_verdict_mask(database, table, mask, has_restricted_tables_admin)
    }

    pub(crate) fn sem_hides_sysvar(&self, name: &str) -> bool {
        tidb_executor::sessionctx::sys_var_hidden_for_sem(self, name)
    }

    pub(crate) fn require_sem_visible_sysvar(&self, name: &str) -> Result<(), DriverError> {
        if self.sem_hides_sysvar(name) {
            Err(DriverError::SpecificAccessDenied(
                "RESTRICTED_VARIABLES_ADMIN".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    /// Go cluster metadata redacts even with a nil privilege checker. Active
    /// roles are evaluated at retrieval, so SET ROLE affects existing sessions.
    pub(crate) fn sem_hides_cluster_metadata(&self) -> bool {
        tidb_util::sem_compat::is_enabled()
            && (self.privileges.is_none()
                || !self.has_dynamic_privilege("RESTRICTED_TABLES_ADMIN", false))
    }

    pub(crate) fn sem_hides_status_var(&self, name: &str) -> bool {
        !self.privilege_checks_bypassed()
            && tidb_util::sem_compat::is_enabled()
            && tidb_util::sem_compat::is_invisible_status_var(name)
            && self
                .privilege_context()
                .is_none_or(|_| !self.has_dynamic_privilege("RESTRICTED_STATUS_ADMIN", false))
    }

    /// Go `RequestDynamicVerificationWithUser` (`privileges.go` around line
    /// 118): whether the NAMED account holds a dynamic privilege, evaluated
    /// over that account's own DEFAULT roles rather than the caller's active
    /// ones. `DROP USER` and `ALTER USER` ask this about their target, which
    /// is how a `SYSTEM_USER` account is protected from a merely
    /// `CREATE USER`-privileged caller.
    #[must_use]
    pub(crate) fn target_has_dynamic_privilege(&self, user: &str, host: &str, name: &str) -> bool {
        if self.privilege_bypassed {
            return true;
        }
        let Some(registry) = self.privileges.as_ref() else {
            return false;
        };
        registry.has_dynamic_priv_with_default_roles(user, host, name)
    }

    /// Go's `CheckPrivilege` (`planner/core/optimizer.go` around line 187)
    /// over the `visitInfo` one statement collects
    /// ([`crate::table_privilege::required_table_privileges`]).
    ///
    /// Entries are checked in the order the builder appended them, and the
    /// FIRST failure is reported -- with the statement's own
    /// `ErrTableaccessDenied` (1142) where Go attached one, and with the
    /// generic `ErrPrivilegeCheckFail` (8121) where it did not.
    pub fn require_statement_table_privileges(
        &self,
        stmt: &tidb_ast::Stmt,
    ) -> Result<(), DriverError> {
        self.require_table_privileges(stmt, false)
    }

    /// [`Self::require_statement_table_privileges`] for the statement inside
    /// an EXPLAIN, which Go plans under `StmtCtx.InExplainStmt`: every view
    /// `BuildDataSourceFromView` expands then needs SHOW VIEW, and a view
    /// expanded after the first SELECT, both refused with `ErrViewNoExplain`.
    pub(crate) fn require_explain_statement_privileges(
        &self,
        stmt: &tidb_ast::Stmt,
    ) -> Result<(), DriverError> {
        self.require_table_privileges(stmt, true)
    }

    fn require_table_privileges(
        &self,
        stmt: &tidb_ast::Stmt,
        in_explain: bool,
    ) -> Result<(), DriverError> {
        self.validate_ddl_preprocess(stmt)?;
        if self.privilege_context().is_none()
            && self.statement_observation.is_none()
            && !self.table_lock_check_pending()?
        {
            return Ok(());
        }
        let requests = self.collect_table_privileges_in(stmt, false, in_explain)?;
        self.check_table_privilege_requests(&requests)
    }

    pub(crate) fn collect_table_privileges(
        &self,
        stmt: &tidb_ast::Stmt,
        preparing: bool,
    ) -> Result<Vec<crate::table_privilege::TablePrivilegeRequest>, DriverError> {
        self.collect_table_privileges_in(stmt, preparing, false)
    }

    fn collect_table_privileges_in(
        &self,
        stmt: &tidb_ast::Stmt,
        preparing: bool,
        in_explain: bool,
    ) -> Result<Vec<crate::table_privilege::TablePrivilegeRequest>, DriverError> {
        self.validate_ddl_preprocess(stmt)?;
        self.deny_missing_tables_without_privileges(stmt)?;
        // Both resolvers plan against the same catalog image the executor will
        // use, under the statement's own context.
        let mut ctx = self
            .statement_context_for_stmt(stmt, false)
            .with_in_explain_stmt(in_explain);
        if preparing {
            // Go initializes each PREPARE marker to NULL before building
            // its plan. EXECUTE uses the actual bound statement values.
            ctx = ctx.with_prepared_params(
                vec![tidb_datatype::Datum::Null; tidb_executor::parsed_parameter_count(stmt)]
                    .into(),
            );
        }
        crate::table_privilege::required_table_privileges(
            stmt,
            &self.vars.current_db,
            |dml| {
                self.with_planning_catalog(|catalog| {
                    tidb_executor::driver::dml_visit_info(
                        dml,
                        catalog,
                        &self.vars.current_db,
                        &ctx,
                    )
                })
            },
            |query| {
                self.with_planning_catalog(|catalog| {
                    tidb_executor::driver::query_visit_info(query, catalog, &self.vars.current_db, &ctx)
                })
            },
            |stmt| self.statement_visit_infos(stmt, &ctx),
        )
    }

    /// Whether Go's `Compile` builds `stmt` through `PlanBuilder.Build`'s
    /// statement builders here: every statement that is not a query or DML,
    /// except SET and SHOW (which compile themselves) and PREPARE / EXECUTE
    /// / DEALLOCATE (the session's own).
    pub(crate) fn compiles_as_statement_plan(stmt: &tidb_ast::Stmt) -> bool {
        match stmt {
            tidb_ast::Stmt::Ddl(_) => true,
            tidb_ast::Stmt::Admin(admin) => !matches!(admin.as_ref(), tidb_ast::AdminStmt::Show(_)),
            tidb_ast::Stmt::Session(session) => !matches!(
                session.as_ref(),
                tidb_ast::SessionStmt::Set(_)
                    | tidb_ast::SessionStmt::Prepare { .. }
                    | tidb_ast::SessionStmt::Execute { .. }
                    | tidb_ast::SessionStmt::Deallocate(_)
            ),
            tidb_ast::Stmt::Query(_) | tidb_ast::Stmt::Dml(_) => false,
        }
    }

    /// Go `PlanBuilder.Build` for a statement [`Self::compiles_as_statement_plan`]
    /// admits: the common plan and its visit infos after
    /// `VisitInfo4PrivCheck`. `None` for any other statement.
    pub(crate) fn build_statement_plan(
        &self,
        stmt: &tidb_ast::Stmt,
    ) -> Result<
        Option<(
            tidb_planner::common_plans::Plan,
            Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
            tidb_executor::StmtContext,
        )>,
        DriverError,
    > {
        if !Self::compiles_as_statement_plan(stmt) {
            return Ok(None);
        }
        self.validate_ddl_preprocess(stmt)?;
        let ctx = self.statement_context_for_stmt(stmt, false);
        let (plan, visits) = self.with_planning_catalog(|catalog| {
            tidb_executor::driver::plan_statement(stmt, catalog, &self.vars.current_db, &ctx, self)
        })?;
        let visits = tidb_planner::plan_builder::stmt_build::visit_info_4_priv_check(
            stmt,
            visits,
            |database, table| self.is_local_temporary_table(database, table),
        );
        Ok(Some((plan, visits, ctx)))
    }

    /// Go `Compile` for a statement that is not a query or DML: the plan,
    /// after `CheckPrivilege` over its visit infos -- or over the requests a
    /// PREPARE derived (Go `checkPreparedPriv`). A statement the node already
    /// compiled ([`Self::compile_statement`]) is not built or checked again.
    pub(crate) fn compile_stmt(
        &mut self,
        stmt: &tidb_ast::Stmt,
        prepared: Option<&[crate::table_privilege::TablePrivilegeRequest]>,
    ) -> Result<Option<tidb_planner::common_plans::Plan>, DriverError> {
        if prepared.is_none()
            && self
                .precompiled
                .as_ref()
                .is_some_and(|precompiled| precompiled.stmt == *stmt)
        {
            let precompiled = self.precompiled.take().expect("matched above");
            self.observe_privilege_tables(&precompiled.requests);
            // The plan is rebuilt (it does not cross threads with the
            // session), and what this build notes is the statement's: the
            // node's compile filed none of it.
            let Some((plan, _, ctx)) = self.build_statement_plan(stmt)? else {
                return Ok(None);
            };
            self.drain_eval_warnings(&ctx);
            return Ok(Some(plan));
        }
        let Some((plan, visits, ctx)) = self.build_statement_plan(stmt)? else {
            return Ok(None);
        };
        // What the build noted (`DROP PROCEDURE IF EXISTS`) is the
        // statement's.
        self.drain_eval_warnings(&ctx);
        match prepared {
            Some(requests) => self.check_table_privilege_requests(requests)?,
            None => self.check_visit_infos(&visits)?,
        }
        Ok(Some(plan))
    }

    /// Go `Compile` for a statement whose executor this session's node runs
    /// (a DDL, an account statement): the preprocessor -- its grammar checks
    /// and, for a DDL, its table-name resolution -- then
    /// `PlanBuilder.Build`'s plan and `CheckPrivilege` over its visit infos,
    /// all before the executor commits the user's transaction. The
    /// executor's own compile ([`Self::compile_stmt`]) then takes the plan
    /// without checking it again, and the statement's observation, which
    /// begins after it, the tables it checked
    /// ([`Self::observe_precompiled_privilege_tables`]).
    pub fn compile_statement(
        &mut self,
        stmt: &tidb_ast::Stmt,
    ) -> Result<CompiledExecutor, DriverError> {
        self.precompiled = None;
        if !Self::compiles_as_statement_plan(stmt) {
            return Ok(CompiledExecutor::Other);
        }
        // What the build notes is filed by the executor's own compile, which
        // builds the plan again; a DDL job's lowering notes its own.
        let Some((plan, visits, _)) = self.preprocess_and_build(stmt)? else {
            return Ok(CompiledExecutor::Other);
        };
        let executor = CompiledExecutor::of(&plan);
        let requests: Vec<_> = visits
            .iter()
            .map(crate::table_privilege::request_of_visit)
            .collect();
        self.check_table_privilege_requests(&requests)?;
        self.precompiled = Some(PrecompiledStatement {
            stmt: stmt.clone(),
            requests,
        });
        Ok(executor)
    }

    /// Go `PREPARE` of a statement [`Self::compile_statement`] compiles
    /// (`GeneratePlanCacheStmtWithAST`): the preprocessor and
    /// `PlanBuilder.Build`, whose errors PREPARE reports, without the
    /// privilege check EXECUTE makes.
    pub fn compile_statement_for_prepare(
        &mut self,
        stmt: &tidb_ast::Stmt,
    ) -> Result<CompiledExecutor, DriverError> {
        if !Self::compiles_as_statement_plan(stmt) {
            return Ok(CompiledExecutor::Other);
        }
        Ok(self
            .preprocess_and_build(stmt)?
            .map_or(CompiledExecutor::Other, |(plan, _, _)| CompiledExecutor::of(&plan)))
    }

    /// Go `Compile`'s preprocessor and `PlanBuilder.Build`.
    fn preprocess_and_build(
        &mut self,
        stmt: &tidb_ast::Stmt,
    ) -> Result<
        Option<(
            tidb_planner::common_plans::Plan,
            Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
            tidb_executor::StmtContext,
        )>,
        DriverError,
    > {
        self.validate_ddl_preprocess(stmt)?;
        self.ddl_preprocess_table_names(stmt)?;
        self.build_statement_plan(stmt)
    }

    /// Forgets a statement [`Self::compile_statement`] compiled; the node
    /// calls it at every statement's start, so no plan outlives its
    /// statement.
    pub fn clear_precompiled_statement(&mut self) {
        self.precompiled = None;
    }

    /// Files the tables [`Self::compile_statement`] checked privileges on
    /// with the statement's observation, which the node begins after the
    /// compile (Go's statement context records them during `Compile`).
    pub fn observe_precompiled_privilege_tables(&self) {
        if let Some(precompiled) = &self.precompiled {
            self.observe_privilege_tables(&precompiled.requests);
        }
    }

    /// The visit infos [`Self::build_statement_plan`] answers, for the
    /// privilege collectors.
    pub(crate) fn statement_visit_infos(
        &self,
        stmt: &tidb_ast::Stmt,
        _ctx: &tidb_executor::StmtContext,
    ) -> Result<Vec<tidb_planner::plan_builder::visit_info::VisitInfo>, DriverError> {
        Ok(self
            .build_statement_plan(stmt)?
            .map(|(_, visits, _)| visits)
            .unwrap_or_default())
    }

    /// Go `preprocessor.tableByName`: a table that does not exist is never
    /// reported as missing to an account that would have no privilege on it
    /// even if it did -- that account is denied (`ErrTableaccessDenied`, 1142),
    /// so existence cannot be probed. Run for every statement the preprocessor
    /// resolves tables of, before planning.
    fn deny_missing_tables_without_privileges(
        &self,
        stmt: &tidb_ast::Stmt,
    ) -> Result<(), DriverError> {
        let Some((_, user, host)) = self.privilege_context() else {
            return Ok(());
        };
        let command = match stmt {
            tidb_ast::Stmt::Query(_) => "SELECT",
            tidb_ast::Stmt::Dml(dml) => match &**dml {
                tidb_ast::DmlStmt::Insert(_) => "INSERT",
                tidb_ast::DmlStmt::Update(_) => "UPDATE",
                tidb_ast::DmlStmt::Delete(_) => "DELETE",
                _ => return Ok(()),
            },
            _ => return Ok(()),
        };
        let (user, host) = (user.to_owned(), host.to_owned());
        let missing: Vec<(String, String)> = {
            let catalog = self.lock_catalog()?;
            crate::table_privilege::read_tables(stmt, &self.vars.current_db)
                .into_iter()
                .filter(|(schema, table)| {
                    catalog.table_in(schema, table).is_none()
                        && !self.is_local_temporary_table(schema, table)
                })
                .collect()
        };
        for (schema, table) in missing {
            if !self.has_any_scoped_privilege(&schema, &table, u64::MAX) {
                return Err(DriverError::TableAccessDenied {
                    privilege: command,
                    user,
                    host,
                    table,
                });
            }
        }
        Ok(())
    }

    /// Runs `plan` against the catalog the executor will use: a stale
    /// transaction's historical schema, else the session's catalog, with the session's
    /// local temporary tables overlaid (a temporary table shadows a base table
    /// of the same name for name resolution too).
    pub(crate) fn with_planning_catalog<T>(
        &self,
        plan: impl FnOnce(&tidb_executor::Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        let overlay = |catalog: &tidb_executor::Catalog| {
            if self.local_temporary_tables.is_empty() {
                return plan(catalog);
            }
            let mut catalog = catalog.clone();
            catalog.attach_local_temporary_tables(self.local_temporary_tables.clone());
            plan(&catalog)
        };
        self.with_txn_catalog(overlay)?
    }

    /// Go `privilege.Manager.RequestVerificationWithUser` for the planner's
    /// view expansion: whether an account -- matched against the stored ones by
    /// host pattern, with its default roles -- holds a privilege. `None` is a
    /// session that enforces no privileges (Go's nil manager).
    pub(crate) fn view_definer_privilege_check(
        &self,
    ) -> Option<tidb_executor::ViewDefinerPrivilegeCheck> {
        let (registry, _, _) = self.privilege_context()?;
        let registry = registry.clone();
        Some(Arc::new(
            move |user: &str,
                  host: &str,
                  database: &str,
                  table: &str,
                  _column: &str,
                  privilege: tidb_mysql::PrivilegeType| {
                let account = registry
                    .matching_account(user, host)
                    .unwrap_or_else(|| (user.to_owned(), host.to_owned()));
                let roles = registry.default_roles(&account);
                crate::table_privilege::global_privileges_of(privilege)
                    .into_iter()
                    .any(|global| {
                        scoped_privilege(
                            &registry, &account.0, &account.1, &roles, database, table, global,
                        )
                    })
            },
        ))
    }

    /// Preprocessor errors precede both grant checking and implicit commits.
    pub fn validate_ddl_preprocess(&self, stmt: &tidb_ast::Stmt) -> Result<(), DriverError> {
        // Go's walk enters an `ADMIN REPAIR TABLE` before its CREATE TABLE,
        // and `checkRepairTableGrammar` reads the process's REPAIR MODE.
        if let tidb_ast::Stmt::Ddl(ddl) = stmt {
            if let tidb_ast::DdlStmt::RepairTable(repair) = ddl.as_ref() {
                let repair_info = &*tidb_domain::domainutil::REPAIR_INFO;
                tidb_planner::preprocess::check_repair_table_grammar(
                    repair,
                    repair_info.in_repair_mode(),
                    repair_info.get_repair_table_list().is_empty(),
                )?;
            }
        }
        // Go's preprocessor grammar checks over the statement's own names
        // and declarations, before anything reads the catalog. Its
        // `handleTableName` has already given an unqualified CREATE TABLE the
        // current schema, which `checkContainDotColumn` compares against.
        let named;
        let graded = match stmt {
            tidb_ast::Stmt::Ddl(ddl) => match ddl.as_ref() {
                tidb_ast::DdlStmt::CreateTable(create)
                    if create.name.len() == 1 && !self.vars.current_db.is_empty() =>
                {
                    let mut create = create.clone();
                    create.name.insert(0, self.vars.current_db.clone());
                    named = tidb_ast::Stmt::Ddl(tidb_ast::NodeBox::new(tidb_ast::DdlStmt::CreateTable(create)));
                    &named
                }
                _ => stmt,
            },
            _ => stmt,
        };
        tidb_planner::preprocess::check_ddl_grammar(
            graded,
            self.vars.sql_mode().has_strict_mode(),
        )?;
        // Go checkCreateTableGrammar resolves LIKE through the session
        // infoschema and rejects temporary sources before privilege checking.
        // A cluster lowerer must never clone the permanent table hidden by a
        // local temporary source with the same name.
        if let tidb_ast::Stmt::Ddl(ddl) = stmt {
            if let tidb_ast::DdlStmt::CreateTable(create) = ddl.as_ref() {
                if create
                    .like_table
                    .as_ref()
                    .is_some_and(|name| self.is_local_temporary_table_path(name))
                {
                    return Err(DriverError::OptOnTemporaryTable("create table like"));
                }
            }
        }
        if let tidb_ast::Stmt::Ddl(ddl) = stmt {
            if let tidb_ast::DdlStmt::DropTable(drop) = ddl.as_ref() {
                if drop.temporary == tidb_ast::DropTemporary::Global {
                    let shared = self.lock_catalog()?;
                    let catalog = self.txn_ctx_stale_catalog().unwrap_or(&*shared);
                    for path in &drop.names {
                        let (db, name) = match path.as_slice() {
                            [name] => (self.vars.current_db.as_str(), name.as_str()),
                            [db, name] => (db.as_str(), name.as_str()),
                            _ => continue,
                        };
                        if self.is_local_temporary_table(db, name)
                            || catalog.table_in(db, name).is_some_and(|table| {
                                !matches!(table, tidb_executor::TableEntry::Kv(table)
                                    if table.temp_table_type() == tidb_model::TempTableType::GLOBAL)
                            })
                        {
                            return Err(DriverError::DropTableOnTemporaryTable);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Go `CheckPrivilege` (and `CheckTableLock`) over a plan's visit
    /// infos, through the same request check every statement takes.
    pub(crate) fn check_visit_infos(
        &self,
        visits: &[tidb_planner::plan_builder::visit_info::VisitInfo],
    ) -> Result<(), DriverError> {
        let requests: Vec<_> = visits
            .iter()
            .map(crate::table_privilege::request_of_visit)
            .collect();
        self.check_table_privilege_requests(&requests)
    }

    /// Checks one statement's `visitInfo`s as Go's optimizer does:
    /// `CheckPrivilege` against the live grants, then `CheckTableLock`.
    pub(crate) fn check_table_privilege_requests(
        &self,
        requests: &[crate::table_privilege::TablePrivilegeRequest],
    ) -> Result<(), DriverError> {
        self.check_privilege(requests)?;
        self.check_table_lock(requests)
    }

    /// Go `CheckPrivilege` over one list of table-privilege requests.
    fn check_privilege(
        &self,
        requests: &[crate::table_privilege::TablePrivilegeRequest],
    ) -> Result<(), DriverError> {
        self.observe_privilege_tables(requests);
        let Some((_, user, host)) = self.privilege_context() else {
            return Ok(());
        };
        let (user, host) = (user.to_owned(), host.to_owned());
        for request in requests {
            // Go answers a virtual schema from fixed rules before it reads a
            // single grant, so `SELECT ... FROM information_schema.*` needs
            // nothing and a write there is refused whatever is granted.
            use crate::table_privilege::TemporaryPrivilege;
            let granted = if !request.dynamic_privileges.is_empty() {
                request
                    .dynamic_privileges
                    .iter()
                    .any(|name| self.has_dynamic_privilege(name, request.dynamic_with_grant))
            } else {
                match request.temporary_privilege {
                    TemporaryPrivilege::Skip => true,
                    TemporaryPrivilege::SkipLocal
                        if self.is_local_temporary_table(&request.database, &request.table) =>
                    {
                        true
                    }
                    TemporaryPrivilege::CreateLocal => self.has_scoped_privilege(
                        &request.database,
                        "",
                        tidb_mysql::CreateTMPTablePriv,
                    ),
                    _ => {
                        self.has_scoped_privilege(
                            &request.database,
                            &request.table,
                            request.privilege,
                        ) || request.also_granted_by.iter().any(|privilege| {
                            self.has_scoped_privilege(&request.database, &request.table, *privilege)
                        })
                    }
                }
            };
            if granted {
                continue;
            }
            if let Some(denial) = request.specific_denial {
                return Err(DriverError::SpecificAccessDenied(denial.to_owned()));
            }
            if !request.dynamic_privileges.is_empty() {
                // Go `ErrPrivilegeCheckFail.GenWithStackByArgs(v.dynamicPrivs)`
                // prints the slice.
                return Err(DriverError::PrivilegeCheckFail(format!(
                    "[{}]",
                    request.dynamic_privileges.join(" ")
                )));
            }
            if request.view_no_explain {
                return Err(view_no_explain());
            }
            return Err(if request.database_named_in_error {
                DriverError::DbAccessDenied {
                    user,
                    host,
                    database: request
                        .database_in_error
                        .clone()
                        .unwrap_or_else(|| request.database.clone()),
                }
            } else if request.table_named_in_error {
                // Go returns the visit's `authErr` as the planner built it:
                // the uppercase command and the table as that call site
                // spells it (`Name.L` mostly, `Name.O` for
                // `requireInsertAndSelectPriv`).
                match &request.table_error_override {
                    Some((command, table)) => DriverError::TableAccessDenied {
                        privilege: command,
                        user,
                        host,
                        table: table.clone(),
                    },
                    None => DriverError::TableAccessDenied {
                        privilege: request.privilege.print_name(),
                        user,
                        host,
                        table: go_to_lower(&request.table),
                    },
                }
            } else {
                DriverError::PrivilegeCheckFail(request.privilege.check_fail_name().to_owned())
            });
        }
        Ok(())
    }

    /// The `user@host` this session authenticated as, as Go's
    /// `AuthUsername`/`AuthHostname` pair. `None` for a session with no front
    /// end.
    #[must_use]
    pub fn authenticated_identity(&self) -> Option<(&str, &str)> {
        self.current_identity()
    }

    /// Records the authenticated identity, which the builtins report.
    ///
    /// Go sets `SessionVars.User` once the connection authenticates; a
    /// front end that has no user leaves it unset and the builtins answer
    /// NULL, which is what Go does for a session without one.
    pub fn set_user(&mut self, current_user: String, login_user: String) {
        self.vars.current_user = Some(current_user);
        self.vars.login_user = Some(login_user);
    }

    /// Grants or revokes this session's `PROCESS` privilege.
    ///
    /// This is a direct test/front-end override. SQL `GRANT PROCESS ON *.*`
    /// is stored in the shared privilege registry instead.
    pub fn set_process_privilege(&mut self, granted: bool) {
        self.has_process_priv = granted;
    }

    /// Joins this session to the server's process list under `connection_id`.
    ///
    /// Go's server registers each connection with the `sessmgr.Manager` right
    /// after authentication; `guard` is that registration, and dropping the
    /// session removes the row.
    pub fn attach_process(&mut self, connection_id: u64, guard: process::ProcessGuard) {
        self.vars.connection_id = Some(connection_id);
        guard.set_trackers(
            std::sync::Arc::clone(self.session_memory.session_tracker()),
            std::sync::Arc::clone(self.session_memory.session_disk_tracker()),
        );
        guard.set_process_plan_info(std::sync::Arc::clone(&self.process_plan_info));
        self.process = Some(guard);
    }

    /// Retains the running statement in the process list until the server has
    /// drained its result set. Go resets `ProcessInfo` to `ComSleep` in the
    /// command-loop defer, after `writeResultSet` returns.
    #[must_use]
    pub fn retain_process_statement(&self, sql: &str) -> Option<process::ProcessStatementGuard> {
        let text = self.statement_text(sql);
        self.process.as_ref().map(|guard| {
            guard.statement_started(&text, self.vars.current_db.clone(), self.status_text())
        })
    }

    /// Marks the statements that follow as a binary-protocol EXECUTE (or
    /// ends that span). Go's `executeStmtImpl` clears `currentPlan` for
    /// `execStmt.Name == ""`, so the process list carries no plan detail
    /// for the binary path; the text `EXECUTE name` keeps it.
    pub fn set_binary_prepared_execution(&mut self, active: bool) {
        self.binary_prepared_execution = active;
    }

    /// [`Self::retain_process_statement`] for a prepared statement: Go's
    /// EXECUTE installs the digest computed at PREPARE (`InitSQLDigest`,
    /// `pkg/executor/select.go:1058`) rather than normalizing the text again.
    pub fn retain_process_statement_with_digest(
        &self,
        sql: &str,
        digest: &str,
    ) -> Option<process::ProcessStatementGuard> {
        let text = self.statement_text(sql);
        self.process.as_ref().map(|guard| {
            guard.statement_started_with_digest(
                &text,
                Some(digest),
                self.vars.current_db.clone(),
                self.status_text(),
            )
        })
    }

    /// Joins this session to the server's account/global-privilege registry.
    ///
    /// Go's session reads `privilege.Manager` off the `Domain` every
    /// connection shares; this is the equivalent handle, installed by the
    /// front end the same way [`Session::attach_process`] installs the
    /// process-list registry.
    /// Installs the instance-wide push-down blacklists
    /// ([`crate::blacklist::PushdownBlacklists`]) this session plans against.
    ///
    /// Go's are package-level, so `ADMIN RELOAD` on one connection changes
    /// what every other connection plans. A front end that opens more than
    /// one session hands them all the same handle to reproduce that; a
    /// session that never gets one keeps its own empty pair, which is what a
    /// single-session embedding wants.
    pub fn attach_pushdown_blacklists(&mut self, blacklists: crate::blacklist::PushdownBlacklists) {
        self.pushdown_blacklists = blacklists;
    }

    /// Go `privilege.GetPrivilegeManager(sctx)`: the privilege cache this
    /// session checks against -- its domain's, for a session on a store.
    #[must_use]
    pub fn privilege_manager(&self) -> Option<privilege::PrivilegeRegistry> {
        self.privileges.clone()
    }

    pub fn attach_privileges(&mut self, registry: privilege::PrivilegeRegistry) {
        // A session on the store reads its domain's privilege cache, as a Go
        // session does; a private registry would hide every account
        // statement the node persists from this session.
        let registry = match (&self.node, &self.privileges) {
            (Some(_), Some(domain)) => {
                drop(registry);
                domain.clone()
            }
            _ => registry,
        };
        // Go's `Auth` activates the account's DEFAULT roles the moment the
        // connection is authenticated (captured: a fresh session reports its
        // default roles from `CURRENT_ROLE()` with no `SET ROLE` at all).
        // The registry is what makes that answerable, so attaching it is the
        // one place that can do it -- the front end installs the identity
        // first and the registry second.
        if !self.privilege_bypassed {
            if let Some((user, host)) = self
                .current_identity()
                .map(|(user, host)| (user.to_owned(), host.to_owned()))
            {
                let (user, host) = (user.as_str(), host.as_str());
                self.vars.active_roles =
                    Arc::new(registry.default_roles(&(user.to_owned(), host.to_owned())));
                // Go `Auth`: with resource control on, the account's
                // `User_attributes->$.resource_group` (the privilege
                // cache's `ResourceGroup`) becomes the session's group.
                if tidb_vardef::ENABLE_RESOURCE_CONTROL.load(std::sync::atomic::Ordering::SeqCst) {
                    let group = registry
                        .user_attributes(user, host)
                        .and_then(|json| serde_json::from_str::<serde_json::Value>(&json).ok())
                        .and_then(|attributes| {
                            attributes
                                .get("resource_group")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_owned)
                        })
                        .filter(|group| !group.is_empty());
                    if let Some(group) = group {
                        self.vars.resource_group = group;
                    }
                }
            }
        }
        self.privileges = Some(registry);
    }

    /// Carries the login's process-wide skip-grant-table admission into this
    /// session. Account and role storage remains attached; only privilege
    /// verification and role activation are bypassed.
    pub fn enable_privilege_bypass(&mut self) {
        self.privilege_bypassed = true;
        Arc::make_mut(&mut self.vars.active_roles).clear();
    }

    /// Records that this connection completed a secure transport handshake.
    ///
    /// Go stores the full TLS state on `SessionVars`; this tier needs only
    /// its presence bit for the transport-sensitive GLOBAL sysvar validator.
    pub fn set_secure_transport(&mut self, secure: bool) {
        self.vars.secure_transport = secure;
    }

    /// Whether the front end established TLS for this connection.
    #[must_use]
    pub(crate) const fn has_secure_transport(&self) -> bool {
        self.vars.secure_transport
    }

    /// Records the negotiated TLS `(cipher, version)` names, which
    /// `Ssl_cipher` and `Ssl_version` report — Go reads them off
    /// `tls.ConnectionState` per connection (`server.go:1329`).
    pub fn set_tls_status(&mut self, status: Option<(String, String)>) {
        self.tls_status = status;
    }

    /// The negotiated TLS names, empty strings on a plaintext connection —
    /// which is the Go capture's answer for the same rows.
    #[must_use]
    pub(crate) fn tls_status(&self) -> (&str, &str) {
        self.tls_status
            .as_ref()
            .map_or(("", ""), |(cipher, version)| {
                (cipher.as_str(), version.as_str())
            })
    }

    /// Whether this connection was admitted under skip-grant-table.
    #[must_use]
    pub(crate) const fn privilege_checks_bypassed(&self) -> bool {
        self.privilege_bypassed
    }

    /// Points this session at a different account table for one statement,
    /// answering the one it was using.
    ///
    /// Unlike [`Self::attach_privileges`] this touches nothing else: the
    /// session's active roles are its own state, and a front end that runs an
    /// account statement against a scratch copy of the table -- which is how
    /// a node whose registry is a read of somebody else's `mysql.*` validates
    /// the statement before persisting it -- must be able to put the live
    /// table back without a `SET ROLE` silently reverting to the defaults.
    pub fn swap_privileges(
        &mut self,
        registry: privilege::PrivilegeRegistry,
    ) -> Option<privilege::PrivilegeRegistry> {
        self.privileges.replace(registry)
    }

    /// Joins this session to the server's shared GLOBAL-scope sysvar table
    /// and snapshots its current overrides into this session's own copy --
    /// see [`vars::SessionVars::seed_from_globals`] for why that snapshot
    /// happens exactly once, here, rather than on every read.
    pub fn attach_globals(
        &mut self,
        globals: std::sync::Arc<dyn tidb_variable::GlobalVarAccessor>,
    ) -> Result<(), crate::DriverError> {
        // A session on the store reads its domain's GLOBAL table, as a Go
        // session reads its domain's (`GlobalVarsAccessor`); a private table
        // would hide every `SET GLOBAL` the node persists from this session.
        if self.node.is_some() {
            drop(globals);
            return Ok(());
        }
        self.vars
            .seed_from_globals(globals)
            .map_err(crate::variables::var_error)
    }

    /// Points this session at a different shared GLOBAL-scope sysvar table
    /// for one statement, answering the one it was using.
    ///
    /// Unlike [`Self::attach_globals`] this does not reseed the session's own
    /// `@@x` copies: a front end running a `SET GLOBAL` against a scratch
    /// table read from the cluster (validate-then-persist, exactly like
    /// [`Self::swap_privileges`]) must be able to put the live table back
    /// unconditionally if the statement fails, without disturbing anything
    /// else this session has already seeded from it.
    pub fn swap_globals(
        &mut self,
        globals: Option<std::sync::Arc<dyn tidb_variable::GlobalVarAccessor>>,
    ) -> Option<std::sync::Arc<dyn tidb_variable::GlobalVarAccessor>> {
        self.vars.swap_globals(globals)
    }

    /// Go `SessionVars.ActiveRoles`, for the privilege checks and the
    /// `CURRENT_ROLE()` builtin.
    pub(crate) fn active_roles(&self) -> &[privilege::Account] {
        self.vars.active_roles.as_slice()
    }

    /// Splits the `CURRENT_USER()` identity (`user@host`) this session
    /// authenticated as, for privilege-registry lookups. `None` for a
    /// session with no front end.
    pub fn current_identity(&self) -> Option<(&str, &str)> {
        let identity = self.vars.current_user.as_deref()?;
        identity.split_once('@')
    }

    /// Records the connection identifier `CONNECTION_ID()` reports, which Go
    /// sets on `SessionVars.ConnectionID` when the front end opens the
    /// connection. `attach_process` sets it too; this exists for a front end
    /// that has an id but no process registry.
    pub fn set_connection_id(&mut self, connection_id: u64) {
        self.vars.connection_id = Some(connection_id);
        self.session_memory.set_connection_id(connection_id);
        self.advisory_locks.set_owner(connection_id);
    }

    /// Charges bytes retained by `COM_STMT_SEND_LONG_DATA` against the same
    /// persistent session tracker Go stores in `SessionVars.MemTracker`.
    ///
    /// The tracker is reconfigured from the live `tidb_mem_quota_query` value
    /// before every chunk, matching Go's statement-boundary refresh while
    /// keeping long data (which arrives between statements) on the session
    /// root. Equality is refused as in `AppendParam`: a chunk that would make
    /// consumption reach the limit is rejected before it is copied.
    #[must_use]
    pub fn try_consume_long_data(&self, bytes: i64) -> bool {
        if bytes <= 0 {
            return true;
        }
        let quota = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_MEM_QUOTA_QUERY)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_util::memory::DEF_MEM_QUOTA_QUERY);
        let (oom_action, tmp_storage_on_oom) = self.vars.statement_memory_policy();
        self.session_memory
            .configure(quota, oom_action, tmp_storage_on_oom);
        let tracker = self.session_memory.session_tracker();
        if quota > 0 && tracker.bytes_consumed().saturating_add(bytes) >= quota {
            return false;
        }
        tracker.consume(bytes);
        true
    }

    /// Releases bytes previously retained by `COM_STMT_SEND_LONG_DATA`.
    pub fn release_long_data(&self, bytes: i64) {
        if bytes > 0 {
            self.session_memory.session_tracker().consume(-bytes);
        }
    }

    /// Current bytes retained by this connection's session memory root.
    #[must_use]
    pub fn session_memory_bytes_consumed(&self) -> i64 {
        self.session_memory.bytes_consumed()
    }

    /// Records whether the MySQL front end negotiated `CLIENT_FOUND_ROWS`.
    /// Go stores the complete client capability word on `SessionVars`; this
    /// session currently consumes only the affected-row bit.
    pub fn set_client_found_rows(&mut self, enabled: bool) {
        self.vars.client_found_rows = enabled;
    }

    /// Installs the server/domain advisory-lock authority before this session
    /// evaluates lock functions.
    pub fn set_advisory_lock_service(
        &mut self,
        service: std::sync::Arc<dyn tidb_executor::advisory_lock_state::AdvisoryLockService>,
    ) {
        self.advisory_locks.release_all();
        self.advisory_locks = tidb_executor::advisory_lock_state::AdvisoryLockSession::new(service);
        if let Some(connection_id) = self.vars.connection_id {
            self.advisory_locks.set_owner(connection_id);
        }
    }

    /// Go `SessionVars.ConnectionID`, which `CONNECTION_ID()` reports; zero
    /// for a session no front end opened.
    #[must_use]
    pub fn connection_id(&self) -> u64 {
        self.vars.connection_id.unwrap_or(0)
    }

    /// Whether this session logged in with an expired password and is
    /// therefore restricted to fixing it -- Go's `session.InSandBoxMode`.
    #[must_use]
    pub const fn in_sandbox_mode(&self) -> bool {
        self.vars.sandbox_mode
    }

    /// Puts this session in sandbox mode. The front end calls this when the
    /// login reported an expired password the server chose to admit.
    pub const fn enable_sandbox_mode(&mut self) {
        self.vars.sandbox_mode = true;
    }

    /// Go's `TiDBContext.checkSandBoxMode` (`pkg/server/driver_tidb.go`): a
    /// sandboxed session may run `SET PASSWORD` and `ALTER USER` and nothing
    /// else; everything else reports 1820. Go gates on the PARSED statement
    /// (its front end hands `ExecuteStmt` an `ast.StmtNode`), so a syntax
    /// error still surfaces as a syntax error -- which is why this parses
    /// first and lets a parse failure fall through to the normal path that
    /// reports it. The extra parse is paid only while sandboxed, a state one
    /// statement ends.
    pub(crate) fn check_sandbox_mode(&self, sql: &str) -> Result<(), DriverError> {
        if !self.vars.sandbox_mode {
            return Ok(());
        }
        let Ok(stmt) = self.parse(sql) else {
            return Ok(());
        };
        match stmt {
            Stmt::Session(session) if matches!(*session, SessionStmt::SetPassword(_)) => Ok(()),
            Stmt::Admin(admin) if matches!(*admin, tidb_ast::AdminStmt::AlterUser(_)) => Ok(()),
            _ => Err(DriverError::MustChangePassword),
        }
    }

}

/// Go `plannererrors.ErrViewNoExplain` (1345).
fn view_no_explain() -> DriverError {
    DriverError::Mysql(tidb_executor::MysqlError::terror(
        &tidb_error::plannererrors::ERR_VIEW_NO_EXPLAIN,
        "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table",
    ))
}

/// Go `RequestVerification(activeRoles, database, table, "", priv)` for an
/// identity: the SEM table verdict, the virtual schemas, then the grants
/// (own or through `roles`).
fn scoped_privilege(
    registry: &privilege::PrivilegeRegistry,
    user: &str,
    host: &str,
    roles: &[privilege::Account],
    database: &str,
    table: &str,
    global_priv: privilege::PrivilegeType,
) -> bool {
    if tidb_util::sem_compat::is_enabled() {
        let has_restricted_tables_admin = registry.has_dynamic_priv_with_roles(
            user,
            host,
            roles,
            "RESTRICTED_TABLES_ADMIN",
            false,
        );
        if let Some(verdict) = crate::table_privilege::sem_verdict_mask(
            database,
            table,
            global_priv.mask(),
            has_restricted_tables_admin,
        ) {
            return verdict;
        }
    }
    if let Some(verdict) = crate::table_privilege::mem_db_verdict_mask(database, global_priv.mask())
    {
        return verdict;
    }
    registry.has_table_priv_with_roles(user, host, roles, database, table, global_priv)
}

/// A statement [`Session::compile_statement`] compiled, and the privilege
/// requests its visit infos made, which it passed.
pub(crate) struct PrecompiledStatement {
    stmt: tidb_ast::Stmt,
    requests: Vec<crate::table_privilege::TablePrivilegeRequest>,
}

/// The executor Go's `executorBuilder.build` picks for a compiled plan, as
/// far as this session's node runs it differently from the session's own
/// statement path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompiledExecutor {
    /// Go `DDLExec` (`core.DDL`).
    Ddl,
    /// Go `AnalyzeExec` (`core.Analyze`).
    Analyze,
    /// Go `LoadStatsExec` (`core.LoadStats`).
    LoadStats,
    /// Go `LockStatsExec` / `UnlockStatsExec`.
    StatsLock,
    /// Go `SimpleExec` (`core.Simple`), which switches on the statement node.
    Simple,
    /// Any other plan, or a query / DML the session compiles itself.
    Other,
}

impl CompiledExecutor {
    fn of(plan: &tidb_planner::common_plans::Plan) -> Self {
        use tidb_planner::common_plans::Plan;
        match plan {
            Plan::Ddl(_) => Self::Ddl,
            Plan::Analyze(_) => Self::Analyze,
            Plan::LoadStats(_) => Self::LoadStats,
            Plan::LockStats(_) | Plan::UnlockStats(_) => Self::StatsLock,
            Plan::Simple(_) => Self::Simple,
            _ => Self::Other,
        }
    }
}
