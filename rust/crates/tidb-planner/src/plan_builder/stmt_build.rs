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

//! Go `PlanBuilder.Build` (`pkg/planner/core/planbuilder.go`) for every
//! statement that does not build an operator tree: `buildSimple`,
//! `buildDDL`, `buildAdmin` and the statement builders beside them. Each
//! answers Go's common plan and appends Go's visit infos, which
//! `CheckPrivilege` reads before the executor runs.
//!
//! Queries and DML build operator trees through their own builders
//! (`build_query_stmt`, the DML builders), and PREPARE / EXECUTE /
//! DEALLOCATE and `DO` are answered by the session before it compiles, as
//! Go's `session.ExecuteStmt` answers them around `Compile`.

use tidb_ast::{AdminStmt, DdlStmt, SessionStmt, Stmt};
use tidb_exprctx::Columns;
use tidb_hack::go_to_lower;
use tidb_mysql::PrivilegeType;

use tidb_infoschema::table_source::TableSource;
use super::visit_info::{VisitAuthError, VisitInfo};
use super::PlanBuilder;
use crate::common_plans::{
    AlterDdlJob, AlterDdlJobOpt, Ddl, Plan, ShowNextRowID, Simple, SqlBindOp, SqlBindPlan,
};
use tidb_planner_core_base::PlanError;
use crate::show_schema::build_show_next_row_id;

/// Go `base.PlanContext` as the statement builders reach the session beyond
/// expressions: the privilege manager, the login user and the session
/// manager.
pub trait SessionPlanContext {
    /// Go `SessionVars.SnapshotTS`.
    fn snapshot_ts(&self) -> u64 { 0 }

    /// Go `privilege.Manager.RequestDynamicVerification(activeRoles, priv,
    /// withGrant)`.
    fn request_dynamic_verification(&self, privilege: &str, with_grant: bool) -> bool;
    /// Go `privilege.Manager.RequestDynamicVerificationWithUser`: whether
    /// `user`@`host` holds the dynamic privilege.
    fn request_dynamic_verification_with_user(
        &self,
        privilege: &str,
        with_grant: bool,
        user: &str,
        host: &str,
    ) -> bool;
    /// Go `SessionVars.User.Username`, `None` without a login user.
    fn login_user(&self) -> Option<String>;
    /// The `(user, host)` Go `SessionVars.User.String()` prints: the matched
    /// account (`AuthUsername@AuthHostname`), `None` without a login user.
    fn user_identity(&self) -> Option<(String, String)>;
    /// Go `GetSessionManager().GetProcessInfo(id)`: the connection's user
    /// and host.
    fn process_info(&self, connection_id: u64) -> Option<(String, String)>;
    /// Go `handleutil.GlobalAutoAnalyzeProcessList.Contains(id)`.
    fn is_auto_analyze_process(&self, connection_id: u64) -> bool;
}

/// Go `appendVisitInfo(vi, priv, db, tbl, "", err)`.
fn visit(privilege: PrivilegeType, database: &str, table: &str, error: VisitAuthError) -> VisitInfo {
    VisitInfo {
        privilege,
        database: database.to_owned(),
        table: table.to_owned(),
        column: String::new(),
        error,
        dynamic_privs: Vec::new(),
        dynamic_with_grant: false,
        alter_writable: false,
    }
}

/// Go `appendDynamicVisitInfo(vi, privs, withGrant, err)`.
fn dynamic_visit(privs: &[&str], with_grant: bool, error: VisitAuthError) -> VisitInfo {
    VisitInfo {
        privilege: tidb_mysql::ExtendedPriv,
        database: String::new(),
        table: String::new(),
        column: String::new(),
        error,
        dynamic_privs: privs.iter().map(|privilege| (*privilege).to_owned()).collect(),
        dynamic_with_grant: with_grant,
        alter_writable: false,
    }
}

/// Go `ErrTableaccessDenied.GenWithStackByArgs(command, user, host, table)`.
fn table_denied(command: &'static str, table: &str) -> VisitAuthError {
    VisitAuthError::TableAccessDenied {
        command,
        table: table.to_owned(),
    }
}

/// Go `ErrDBaccessDenied.GenWithStackByArgs(user, host, db)`.
fn db_denied(database: &str) -> VisitAuthError {
    VisitAuthError::DbAccessDenied {
        database: database.to_owned(),
    }
}

/// Go `ErrSpecificAccessDenied.GenWithStackByArgs(privilege)`.
const fn specific(privilege: &'static str) -> VisitAuthError {
    VisitAuthError::SpecificAccessDenied { privilege }
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// A table name as Go's preprocessor leaves it: `(schema.L, name.L,
    /// name.O)`, the schema defaulting to the current database.
    fn qualified(&self, path: &[String]) -> (String, String, String) {
        match path {
            [schema, table] => (go_to_lower(schema), go_to_lower(table), table.clone()),
            [table] => (
                go_to_lower(self.source.current_database()),
                go_to_lower(table),
                table.clone(),
            ),
            _ => (String::new(), String::new(), String::new()),
        }
    }

    /// Go `PlanBuilder.Build` for the statements that do not build an
    /// operator tree. `stmt` is the preprocessed statement.
    ///
    /// # Errors
    ///
    /// The builder's own refusals (`ErrNoDB`, the unsupported forms Go
    /// rejects while building) and a SET or SHOW value's build error.
    pub fn build_stmt(&mut self, stmt: &Stmt) -> Result<Plan, PlanError> {
        match stmt {
            Stmt::Dml(dml) if matches!(dml.as_ref(), tidb_ast::DmlStmt::ImportInto(_)) => {
                let tidb_ast::DmlStmt::ImportInto(import) = dml.as_ref() else { unreachable!() };
                self.build_import_into(import)
            }
            Stmt::Query(_) | Stmt::Dml(_) => Err(PlanError::internal(
                "queries and DML build through their own builders",
            )),
            Stmt::Session(session) => match session.as_ref() {
                SessionStmt::Set(set) => self.build_set(set),
                SessionStmt::Prepare { .. }
                | SessionStmt::Execute { .. }
                | SessionStmt::Deallocate(_) => Err(PlanError::internal(
                    "PREPARE, EXECUTE and DEALLOCATE are answered by the session",
                )),
                _ => self.build_simple(stmt),
            },
            Stmt::Ddl(ddl) => self.build_ddl(stmt, ddl),
            Stmt::Admin(admin) => self.build_admin_family(stmt, admin),
        }
    }

    /// The `AdminStmt` arm of Go's `Build`: the Rust AST groups under
    /// `AdminStmt` statements Go builds through several builders.
    fn build_admin_family(&mut self, stmt: &Stmt, admin: &AdminStmt) -> Result<Plan, PlanError> {
        Ok(match admin {
            AdminStmt::Show(show) => return self.build_show(show),
            AdminStmt::SetConfig(set_config) => {
                // Go `buildSetConfig`.
                self.visit_info.push(visit(tidb_mysql::ConfigPriv, "", "", specific("CONFIG")));
                if matches!(set_config.value, tidb_ast::Expr::Default(_)) {
                    return Err(PlanError::internal("Unknown DEFAULT for SET CONFIG"));
                }
                Plan::SetConfig(set_config.clone())
            }
            AdminStmt::Explain(explain) => Plan::Explain(explain.clone()),
            AdminStmt::ExplainFor(explain_for) => Plan::ExplainFor(explain_for.clone()),
            AdminStmt::Trace(trace) => Plan::Trace(trace.clone()),
            AdminStmt::LoadStats(load) => Plan::LoadStats(load.clone()),
            AdminStmt::LockStats(lock) => {
                self.require_insert_and_select_priv(lock.tables.iter().map(|table| &table.name));
                Plan::LockStats(lock.clone())
            }
            AdminStmt::UnlockStats(unlock) => {
                self.require_insert_and_select_priv(unlock.tables.iter().map(|table| &table.name));
                Plan::UnlockStats(unlock.clone())
            }
            AdminStmt::PlanReplayer(replayer) => Plan::PlanReplayer(replayer.clone()),
            AdminStmt::Traffic(traffic) => self.build_traffic(traffic),
            AdminStmt::RefreshStats(refresh) => return self.build_refresh_stats(stmt, refresh),
            AdminStmt::AnalyzeTable(analyze) => {
                // Go `buildAnalyze`.
                if analyze.no_write_to_binlog {
                    return Err(PlanError::from(
                        tidb_plannererrors::ERR_NOT_SUPPORTED_YET
                            .gen_with_stack_by_args(&["[NO_WRITE_TO_BINLOG | LOCAL]".into()]),
                    ));
                }
                self.require_insert_and_select_priv(analyze.tables.iter());
                Plan::Analyze(Box::new(admin.clone()))
            }
            AdminStmt::AnalyzeIncremental(_) => {
                return Err(PlanError::internal(
                    "the incremental analyze feature has already been removed in TiDB v7.5.0, so this will have no effect",
                ));
            }
            AdminStmt::CreateBinding(_) | AdminStmt::DropBinding(_) | AdminStmt::SetBinding(_) => {
                // Go `buildCreateBindPlan` / `buildDropBindPlan` /
                // `buildSetBindingStatusPlan`: binding statements are an
                // administrator's.
                let op = match admin {
                    AdminStmt::CreateBinding(_) => SqlBindOp::Create,
                    AdminStmt::DropBinding(_) => SqlBindOp::Drop,
                    _ => SqlBindOp::SetBindingStatus,
                };
                self.visit_info.push(visit(tidb_mysql::SuperPriv, "", "", VisitAuthError::None));
                Plan::SqlBind(Box::new(SqlBindPlan {
                    op,
                    statement: Some(admin.clone()),
                }))
            }
            AdminStmt::CreateProcedure(_) => {
                // Go `PlanBuilder.Build` has no builder for `ast.ProcedureInfo`
                // (a plain `stmtNode`, not a `DDLNode`): the statement falls
                // through to the closing `ErrUnsupportedType`.
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_UNSUPPORTED_TYPE
                        .gen_with_stack("Unsupported type *ast.ProcedureInfo"),
                ));
            }
            AdminStmt::SplitRegion(split) => return self.build_split_region(split),
            AdminStmt::RecommendIndex(recommend) => Plan::RecommendIndex(recommend.clone()),
            AdminStmt::CompactTable(compact) => {
                // Go `buildCompactTable`.
                let (database, table, _) = self.qualified(&compact.table);
                self.visit_info.push(visit(
                    tidb_mysql::AlterPriv,
                    &database,
                    &table,
                    table_denied("ALTER", &table),
                ));
                Plan::CompactTable(compact.clone())
            }
            AdminStmt::Grant(_)
            | AdminStmt::GrantProxy(_)
            | AdminStmt::GrantRole(_)
            | AdminStmt::Revoke(_)
            | AdminStmt::RevokeRole(_)
            | AdminStmt::Brie(_)
            | AdminStmt::Binlog(_)
            | AdminStmt::Kill(_)
            | AdminStmt::CreateStatistics(_)
            | AdminStmt::DropStatistics(_)
            | AdminStmt::ServerControl(_)
            | AdminStmt::CancelDistributionJob(_)
            | AdminStmt::CalibrateResource(_)
            | AdminStmt::AddQueryWatch(_)
            | AdminStmt::DropQueryWatch(_)
            | AdminStmt::CancelImportJob(_)
            | AdminStmt::Flush(_)
            | AdminStmt::DropStats(_)
            | AdminStmt::Do(_)
            | AdminStmt::CreateUser { .. }
            | AdminStmt::CreateRole { .. }
            | AdminStmt::AlterUser(_)
            | AdminStmt::DropUser { .. }
            | AdminStmt::RenameUser { .. }
            | AdminStmt::AlterInstance(_)
            | AdminStmt::AlterRange(_)
            | AdminStmt::DropProcedure(_)
            // Go master: `buildSimple` for `CancelMaterializedViewJobStmt`.
            | AdminStmt::CancelMaterializedViewJob(_) => return self.build_simple(stmt),
            AdminStmt::RefreshMaterializedView(refresh) => return self.build_refresh_materialized_view(refresh),
            AdminStmt::PurgeMaterializedViewLog(purge) => return self.build_purge_materialized_view_log(purge),
            AdminStmt::CompareMaterializedView(compare) => return self.build_compare_materialized_view(compare),
            AdminStmt::RefreshMaterializedViewImplement(implement) => {
                return self.build_refresh_materialized_view_implement(implement)
            }
            _ => return self.build_admin(stmt, admin),
        })
    }

    /// Go `buildSimple`: the statement as Go's `Simple`, with the visit
    /// infos Go checks in the planner (the rest are the executor's).
    ///
    /// # Errors
    ///
    /// Go `ErrNoDB` for `USE` of an empty name and for a GRANT/REVOKE on a
    /// database scope without one, and `ErrSpDoesNotExist` for `DROP
    /// PROCEDURE` without `IF EXISTS`.
    pub fn build_simple(&mut self, stmt: &Stmt) -> Result<Plan, PlanError> {
        let mut statement = stmt.clone();
        match &mut statement {
            Stmt::Admin(admin) => match admin.as_mut() {
                AdminStmt::Flush(flush) => match &mut flush.target {
                    tidb_ast::FlushStmtType::StatsDelta { objects, .. } => {
                        self.fill_default_db_for_stats_objects(objects)?;
                        dedup_stats_objects(objects);
                        self.require_select_priv_for_stats_objects(objects);
                    }
                    _ => self.visit_info.push(visit(
                        tidb_mysql::ReloadPriv,
                        "",
                        "",
                        specific("RELOAD"),
                    )),
                },
                AdminStmt::Grant(grant) => {
                    self.collect_visit_info_from_grant(
                        &grant.privileges,
                        &grant.level,
                        true,
                        &[],
                    )?;
                    for user in &grant.users {
                        self.append_visit_info_is_restricted_user(
                            &user.user.user,
                            &user.user.host,
                            "RESTRICTED_USER_ADMIN",
                        );
                    }
                }
                AdminStmt::Brie(brie) => {
                    if brie.kind == tidb_ast::BrieKind::Restore {
                        self.visit_info.push(dynamic_visit(
                            &["RESTORE_ADMIN"],
                            false,
                            specific("SUPER or RESTORE_ADMIN"),
                        ));
                    } else {
                        self.visit_info.push(dynamic_visit(
                            &["BACKUP_ADMIN"],
                            false,
                            specific("SUPER or BACKUP_ADMIN"),
                        ));
                    }
                }
                AdminStmt::CalibrateResource(_)
                | AdminStmt::AddQueryWatch(_)
                | AdminStmt::DropQueryWatch(_) => self.visit_info.push(dynamic_visit(
                    &["RESOURCE_GROUP_ADMIN"],
                    false,
                    specific("SUPER or RESOURCE_GROUP_ADMIN"),
                )),
                AdminStmt::GrantRole(grant) => {
                    self.visit_info.push(dynamic_visit(
                        &["ROLE_ADMIN"],
                        false,
                        specific("SUPER or ROLE_ADMIN"),
                    ));
                    // If any of the roles are RESTRICTED, require
                    // RESTRICTED_ROLE_ADMIN.
                    for role in &grant.roles {
                        self.append_visit_info_is_restricted_user(
                            &role.role,
                            &role.host,
                            "RESTRICTED_USER_ADMIN",
                        );
                    }
                }
                AdminStmt::RevokeRole(revoke) => {
                    self.visit_info.push(dynamic_visit(
                        &["ROLE_ADMIN"],
                        false,
                        specific("SUPER or ROLE_ADMIN"),
                    ));
                    for user in &revoke.users {
                        self.append_visit_info_is_restricted_user(
                            &user.user,
                            &user.host,
                            "RESTRICTED_USER_ADMIN",
                        );
                    }
                    for role in &revoke.roles {
                        self.append_visit_info_is_restricted_user(
                            &role.role,
                            &role.host,
                            "RESTRICTED_USER_ADMIN",
                        );
                    }
                }
                AdminStmt::Revoke(revoke) => {
                    let users: Vec<(&str, &str)> = revoke
                        .users
                        .iter()
                        .map(|user| (user.user.user.as_str(), user.user.host.as_str()))
                        .collect();
                    self.collect_visit_info_from_grant(
                        &revoke.privileges,
                        &revoke.level,
                        false,
                        &users,
                    )?;
                }
                AdminStmt::Kill(kill) => self.build_kill_visits(kill),
                AdminStmt::AlterInstance(_) => {
                    self.visit_info
                        .push(visit(tidb_mysql::SuperPriv, "", "", specific("SUPER")));
                }
                AdminStmt::RenameUser { .. } => {
                    self.visit_info.push(visit(
                        tidb_mysql::CreateUserPriv,
                        "",
                        "",
                        specific("CREATE USER"),
                    ));
                }
                AdminStmt::DropUser { users, .. } => {
                    // The main privilege checks for DROP USER are the
                    // executor's: they use OR conditions a visit cannot.
                    for user in users.iter() {
                        self.append_visit_info_is_restricted_user(
                            &user.user,
                            &user.host,
                            "RESTRICTED_USER_ADMIN",
                        );
                    }
                }
                AdminStmt::DropProcedure(drop) => {
                    let procedure = drop.name.last().cloned().unwrap_or_default();
                    let message = format!(
                        "PROCEDURE {}.{procedure} does not exist",
                        self.source.current_database()
                    );
                    if !drop.if_exists {
                        return Err(PlanError::eval(tidb_exprctx::EvalError::Conversion(
                            tidb_plannererrors::ERR_SP_DOES_NOT_EXIST.generate(message),
                        )));
                    }
                    self.ctx.append_note(
                        tidb_plannererrors::ERR_SP_DOES_NOT_EXIST.code().value() as u16,
                        &message,
                    );
                }
                AdminStmt::ServerControl(control) => {
                    if matches!(control.as_ref(), tidb_ast::ServerControlStmt::Shutdown) {
                        self.visit_info.push(visit(
                            tidb_mysql::ShutdownPriv,
                            "",
                            "",
                            VisitAuthError::None,
                        ));
                    }
                }
                _ => {}
            },
            Stmt::Session(session) => match session.as_ref() {
                SessionStmt::Use(name) if name.is_empty() => {
                    return Err(PlanError::from(
                        tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
                    ))
                }
                SessionStmt::SetPassword(set) => {
                    if let Some(user) = &set.user {
                        if !user.current_user {
                            self.append_visit_info_is_restricted_user(
                                &user.user,
                                &user.host,
                                "RESTRICTED_USER_ADMIN",
                            );
                        }
                    }
                }
                SessionStmt::SetResourceGroup(_) => {
                    if tidb_vardef::ENABLE_RESOURCE_CONTROL_STRICT_MODE
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        self.visit_info.push(dynamic_visit(
                            &["RESOURCE_GROUP_ADMIN", "RESOURCE_GROUP_USER"],
                            false,
                            specific("SUPER or RESOURCE_GROUP_ADMIN or RESOURCE_GROUP_USER"),
                        ));
                    }
                }
                _ => {}
            },
            Stmt::Ddl(_) | Stmt::Query(_) | Stmt::Dml(_) => {}
        }
        Ok(Plan::Simple(Box::new(Simple {
            statement,
            stale_txn_start_ts: 0,
        })))
    }

    /// The KILL arm of Go `buildSimple`: everyone may kill their own
    /// connections; another user's needs CONNECTION_ADMIN (or SUPER), and
    /// under SEM a restricted user's needs RESTRICTED_CONNECTION_ADMIN; an
    /// auto-analyze job needs CONNECTION_ADMIN.
    fn build_kill_visits(&mut self, kill: &tidb_ast::KillStmt) {
        let tidb_ast::KillTarget::ConnectionId(connection_id) = kill.target else {
            return;
        };
        let Some(session) = self.session_ctx else {
            return;
        };
        if let Some((user, host)) = session.process_info(connection_id) {
            if session.login_user().as_deref() != Some(user.as_str()) {
                self.visit_info.push(dynamic_visit(
                    &["CONNECTION_ADMIN"],
                    false,
                    specific("SUPER or CONNECTION_ADMIN"),
                ));
                self.append_visit_info_is_restricted_user(
                    &user,
                    &host,
                    "RESTRICTED_CONNECTION_ADMIN",
                );
            }
        } else if session.is_auto_analyze_process(connection_id) {
            // Only the users with SUPER or CONNECTION_ADMIN privilege can
            // kill auto analyze.
            self.visit_info.push(dynamic_visit(
                &["CONNECTION_ADMIN"],
                false,
                specific("SUPER or CONNECTION_ADMIN"),
            ));
        }
    }

    /// Go `appendVisitInfoIsRestrictedUser`: under SEM, touching an account
    /// that holds RESTRICTED_USER_ADMIN needs `privilege`.
    fn append_visit_info_is_restricted_user(&mut self, user: &str, host: &str, privilege: &'static str) {
        if !tidb_sem_compat::is_enabled() {
            return;
        }
        let restricted = self.session_ctx.is_some_and(|session| {
            session.request_dynamic_verification_with_user("RESTRICTED_USER_ADMIN", false, user, host)
        });
        if restricted {
            self.visit_info
                .push(dynamic_visit(&[privilege], false, specific(privilege)));
        }
    }

    /// Go `collectVisitInfoFromGrantStmt` (`is_grant`) and
    /// `collectVisitInfoFromRevokeStmt`: every static privilege named at the
    /// statement's scope, `ALL` expanded to the scope's list, then the
    /// scope's `GRANT OPTION` when any static privilege was named; a dynamic
    /// privilege needs itself WITH GRANT OPTION. REVOKE checks its
    /// `revoke_users` against `RESTRICTED_USER_ADMIN` before that trailing
    /// `GRANT OPTION`, as Go's collector does.
    fn collect_visit_info_from_grant(
        &mut self,
        privileges: &[tidb_ast::GrantPrivilege],
        level: &tidb_ast::GrantLevel,
        is_grant: bool,
        revoke_users: &[(&str, &str)],
    ) -> Result<(), PlanError> {
        let (mut db_name, table_name) = match level {
            tidb_ast::GrantLevel::Global => (String::new(), String::new()),
            tidb_ast::GrantLevel::Database(database) => (database.clone().unwrap_or_default(), String::new()),
            tidb_ast::GrantLevel::Table { database, table } => {
                (database.clone().unwrap_or_default(), table.clone())
            }
        };
        let is_global = matches!(level, tidb_ast::GrantLevel::Global);
        // This supports a local revoke SELECT on tablename, but does not add
        // dbName to the visitInfo of a *.* grant.
        if db_name.is_empty() && !is_global {
            let current = self.source.current_database();
            if current.is_empty() {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
                ));
            }
            db_name = if is_grant {
                current.to_owned()
            } else {
                go_to_lower(current)
            };
        }
        // Go `genAuthErrForGrantStmt`: a statement scoped to
        // `performance_schema` carries the database-access denial.
        let auth_err = if is_grant && db_name.eq_ignore_ascii_case("performance_schema") {
            db_denied(&db_name)
        } else {
            VisitAuthError::None
        };
        let mut non_dynamic_privilege = false;
        let mut all_privs: &[PrivilegeType] = &[];
        for item in privileges {
            if tidb_sem_v2::is_enabled()
                && tidb_sem_v2::is_restricted_privilege(&tidb_hack::go_to_upper(&item.name))
            {
                self.visit_info.push(dynamic_visit(
                    &["RESTRICTED_PRIV_ADMIN"],
                    false,
                    specific("RESTRICTED_PRIV_ADMIN"),
                ));
            }
            if item.dynamic {
                let name = tidb_hack::go_to_upper(&item.name);
                let error = if is_grant {
                    specific("GRANT OPTION")
                } else {
                    VisitAuthError::None
                };
                self.visit_info.push(VisitInfo {
                    dynamic_privs: vec![if is_grant { item.name.clone() } else { name }],
                    ..dynamic_visit(&[], true, error)
                });
                continue;
            }
            non_dynamic_privilege = true;
            let Some(privilege) = tidb_mysql::privilege_from_grant_name(&item.name) else {
                continue;
            };
            if privilege == tidb_mysql::AllPriv {
                all_privs = match level {
                    tidb_ast::GrantLevel::Global => tidb_mysql::ALL_GLOBAL_PRIVS,
                    tidb_ast::GrantLevel::Database(_) => tidb_mysql::ALL_DB_PRIVS,
                    tidb_ast::GrantLevel::Table { .. } => tidb_mysql::ALL_TABLE_PRIVS,
                };
                break;
            }
            self.visit_info
                .push(visit(privilege, &db_name, &table_name, auth_err.clone()));
        }
        for privilege in all_privs {
            self.visit_info
                .push(visit(*privilege, &db_name, &table_name, auth_err.clone()));
        }
        for (user, host) in revoke_users {
            // For SEM, make sure the users are not restricted.
            self.append_visit_info_is_restricted_user(user, host, "RESTRICTED_USER_ADMIN");
        }
        if non_dynamic_privilege {
            // Dynamic privileges use their own GRANT OPTION; any static one
            // needs the scope's GRANT OPTION.
            self.visit_info
                .push(visit(tidb_mysql::GrantPriv, &db_name, &table_name, auth_err));
        }
        Ok(())
    }

    /// Go `requireInsertAndSelectPriv`.
    fn require_insert_and_select_priv<'p>(&mut self, tables: impl Iterator<Item = &'p Vec<String>>) {
        for table in tables {
            let (database, table_l, table_o) = self.qualified(table);
            self.visit_info.push(visit(
                tidb_mysql::InsertPriv,
                &database,
                &table_l,
                table_denied("INSERT", &table_o),
            ));
            self.visit_info.push(visit(
                tidb_mysql::SelectPriv,
                &database,
                &table_l,
                table_denied("SELECT", &table_o),
            ));
        }
    }

    /// Go `fillDefaultDBForStatsObjects`: an unqualified table takes the
    /// current database.
    fn fill_default_db_for_stats_objects(
        &self,
        objects: &mut [tidb_ast::StatsObject],
    ) -> Result<(), PlanError> {
        for object in objects {
            if let tidb_ast::StatsObject::Table { database, .. } = object {
                if database.as_deref().is_none_or(str::is_empty) {
                    let current = self.source.current_database();
                    if current.is_empty() {
                        return Err(PlanError::from(
                            tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
                        ));
                    }
                    *database = Some(current.to_owned());
                }
            }
        }
        Ok(())
    }

    /// Go `requireSelectPrivForStatsObjects`.
    fn require_select_priv_for_stats_objects(&mut self, objects: &[tidb_ast::StatsObject]) {
        for object in objects {
            match object {
                tidb_ast::StatsObject::Global => {
                    self.visit_info.push(visit(
                        tidb_mysql::SelectPriv,
                        "",
                        "",
                        VisitAuthError::PrivilegeCheckFail,
                    ));
                    return;
                }
                tidb_ast::StatsObject::Database(database) => self.visit_info.push(visit(
                    tidb_mysql::SelectPriv,
                    &go_to_lower(database),
                    "",
                    db_denied(database),
                )),
                tidb_ast::StatsObject::Table { database, table } => self.visit_info.push(visit(
                    tidb_mysql::SelectPriv,
                    &go_to_lower(database.as_deref().unwrap_or_default()),
                    &go_to_lower(table),
                    table_denied("SELECT", table),
                )),
            }
        }
    }

    /// Go `buildRefreshStats`: the objects default to the current database,
    /// deduplicated, and need SELECT unless the session holds RESTORE_ADMIN.
    fn build_refresh_stats(
        &mut self,
        stmt: &Stmt,
        _refresh: &tidb_ast::RefreshStatsStmt,
    ) -> Result<Plan, PlanError> {
        let mut statement = stmt.clone();
        if let Stmt::Admin(admin) = &mut statement {
            if let AdminStmt::RefreshStats(refresh) = admin.as_mut() {
                self.fill_default_db_for_stats_objects(&mut refresh.objects)?;
                dedup_stats_objects(&mut refresh.objects);
                let restore_admin = self.session_ctx.is_some_and(|session| {
                    session.request_dynamic_verification("RESTORE_ADMIN", false)
                });
                if !restore_admin {
                    self.require_select_priv_for_stats_objects(&refresh.objects);
                }
            }
        }
        Ok(Plan::Simple(Box::new(Simple {
            statement,
            stale_txn_start_ts: 0,
        })))
    }

    /// Go `buildTraffic`.
    fn build_traffic(&mut self, traffic: &tidb_ast::TrafficStmt) -> Plan {
        let (privs, message): (&[&str], &'static str) = match traffic {
            tidb_ast::TrafficStmt::Capture { .. } => {
                (&["TRAFFIC_CAPTURE_ADMIN"], "SUPER or TRAFFIC_CAPTURE_ADMIN")
            }
            tidb_ast::TrafficStmt::Replay { .. } => {
                (&["TRAFFIC_REPLAY_ADMIN"], "SUPER or TRAFFIC_REPLAY_ADMIN")
            }
            tidb_ast::TrafficStmt::ShowJobs | tidb_ast::TrafficStmt::CancelJobs => (
                &["TRAFFIC_CAPTURE_ADMIN", "TRAFFIC_REPLAY_ADMIN"],
                "SUPER, TRAFFIC_CAPTURE_ADMIN or TRAFFIC_REPLAY_ADMIN",
            ),
        };
        self.visit_info
            .push(dynamic_visit(privs, false, specific(message)));
        Plan::Traffic(Box::new(traffic.clone()))
    }

    /// Go `buildSplitRegion`'s refusals; the regions themselves are the
    /// executor's.
    fn build_split_region(&mut self, split: &tidb_ast::SplitRegionStmt) -> Result<Plan, PlanError> {
        let (database, table, _) = self.qualified(&split.table);
        if let Some(info) = self
            .source
            .try_find_table(&database, &table)?
            .and_then(|source| source.table_info.clone())
        {
            let info = info.read();
            if info.temp_table_type != tidb_model::TempTableType::NONE {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                        .gen_with_stack_by_args(&["split table".into()]),
                ));
            }
            if split.partition_syntax && info.partition.is_none() {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_PARTITION_CLAUSE_ON_NONPARTITIONED
                        .gen_with_stack_by_args(&[]),
                ));
            }
        }
        Ok(Plan::SplitRegion(Box::new(split.clone())))
    }

    /// Go `buildAdmin`: the ADMIN statements Go plans, each needing SUPER
    /// except those Go answers before appending the visit.
    fn build_admin(&mut self, stmt: &Stmt, admin: &AdminStmt) -> Result<Plan, PlanError> {
        let plan = match admin {
            AdminStmt::AdminCheck(check) => {
                if let tidb_ast::AdminCheckStmt::Table { tables } = check.as_ref() {
                    if tables.len() > 1 {
                        return Err(PlanError::internal(
                            "admin check only supports one table at a time",
                        ));
                    }
                }
                Plan::CheckTable(check.clone())
            }
            AdminStmt::AdminRecoverIndex(recover) => Plan::RecoverIndex(recover.clone()),
            AdminStmt::AdminCleanupIndex(cleanup) => Plan::CleanupIndex(cleanup.clone()),
            AdminStmt::AdminChecksum(checksum) => Plan::ChecksumTable(checksum.clone()),
            AdminStmt::ShowNextRowId(show) => {
                let (schema, names) = build_show_next_row_id();
                Plan::ShowNextRowID(ShowNextRowID {
                    table_name: show.table.clone(),
                    schema,
                    names,
                })
            }
            AdminStmt::ShowDdl => Plan::ShowDdl,
            AdminStmt::ShowDdlJobs(show) => Plan::Logical(
                self.build_admin_show_ddl_jobs(show.job_number, show.where_clause.as_ref())?,
            ),
            AdminStmt::DdlJobControl(control) => Plan::DdlJobControl(control.clone()),
            AdminStmt::ShowDdlJobQueries(show) => Plan::ShowDdlJobQueries(show.clone()),
            AdminStmt::ShowSlow(show) => Plan::ShowSlow(show.clone()),
            AdminStmt::Reload(kind) => {
                return Ok(match kind {
                    tidb_ast::AdminReloadKind::ExprPushdownBlacklist => {
                        Plan::ReloadExprPushdownBlacklist
                    }
                    tidb_ast::AdminReloadKind::OptRuleBlacklist => Plan::ReloadOptRuleBlacklist,
                    tidb_ast::AdminReloadKind::Bindings => sql_bind(SqlBindOp::ReloadBindings),
                    tidb_ast::AdminReloadKind::ClusterBindings => {
                        sql_bind(SqlBindOp::ReloadClusterBindings)
                    }
                    tidb_ast::AdminReloadKind::Statistics => simple(stmt),
                });
            }
            AdminStmt::Plugins { enable, plugins } => {
                return Ok(Plan::AdminPlugins {
                    enable: *enable,
                    plugins: plugins.clone(),
                });
            }
            AdminStmt::BindingControl(kind) => {
                return match kind {
                    tidb_ast::AdminBindingControlKind::Flush => {
                        Ok(sql_bind(SqlBindOp::FlushBindings))
                    }
                    tidb_ast::AdminBindingControlKind::Capture => {
                        Err(PlanError::internal("Auto Capture is not supported"))
                    }
                    tidb_ast::AdminBindingControlKind::Evolve => Err(PlanError::internal(
                        "Cannot enable baseline evolution feature, it is not generally available now",
                    )),
                };
            }
            AdminStmt::FlushPlanCache(_) => return Ok(simple(stmt)),
            AdminStmt::SetBdrRole(_) | AdminStmt::UnsetBdrRole => simple(stmt),
            AdminStmt::ShowBdrRole => Plan::AdminShowBdrRole,
            AdminStmt::AlterDdlJobs(alter) => self.build_admin_alter_ddl_job(alter)?,
            AdminStmt::CreateWorkloadSnapshot => Plan::WorkloadRepoCreate,
            _ => {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_UNSUPPORTED_TYPE.gen_with_stack(format!(
                        "Unsupported ast.AdminStmt({}) for buildAdmin",
                        stmt.label()
                    )),
                ));
            }
        };
        // Admin command can only be executed by administrator.
        self.visit_info
            .push(visit(tidb_mysql::SuperPriv, "", "", VisitAuthError::None));
        Ok(plan)
    }

    /// Go `buildAdminAlterDDLJob`: each option's name is checked against
    /// Go's allowed parameters, its literal rewritten over a column-less dual
    /// (Go's `mockTablePlan`) and its value range-checked.
    ///
    /// # Errors
    ///
    /// Go's unsupported-config and value errors (plain errors, 1105).
    fn build_admin_alter_ddl_job(
        &mut self,
        stmt: &tidb_ast::AdminAlterDdlJobsStmt,
    ) -> Result<Plan, PlanError> {
        let mut options = Vec::with_capacity(stmt.options.len());
        for opt in &stmt.options {
            if !crate::common_plans::ALLOWED_ALTER_DDL_JOB_PARAMS.contains(&opt.name.as_str()) {
                return Err(plain_error(format!(
                    "unsupported admin alter ddl jobs config: {}",
                    opt.name
                )));
            }
            let value = self.rewrite_scalar(
                &opt.value,
                &tidb_expr::schema::Schema::default(),
                &[],
                &std::collections::BTreeMap::new(),
            )?;
            let alter_ddl_job_opt = AlterDdlJobOpt { name: opt.name.clone(), value: Some(value) };
            check_alter_ddl_job_opt_value(&alter_ddl_job_opt)?;
            options.push(alter_ddl_job_opt);
        }
        Ok(Plan::AlterDdlJob(AlterDdlJob { job_id: stmt.job_number, options }))
    }

    /// Go `buildDDL`: the statement as Go's `DDL`, with its visit infos.
    ///
    /// # Errors
    ///
    /// Go `ErrNoDB` for `ALTER DATABASE` without a database, and the
    /// statements Go refuses while building (`OPTIMIZE TABLE`, a foreign key
    /// on a temporary table, a view's column-list mismatch).
    pub fn build_ddl(&mut self, stmt: &Stmt, ddl: &DdlStmt) -> Result<Plan, PlanError> {
        match ddl {
            DdlStmt::AlterDatabase { name, .. } => {
                let database = name
                    .clone()
                    .unwrap_or_else(|| self.source.current_database().to_owned());
                if database.is_empty() {
                    return Err(PlanError::from(
                        tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
                    ));
                }
                self.visit_info.push(visit(
                    tidb_mysql::AlterPriv,
                    &go_to_lower(&database),
                    "",
                    db_denied(&database),
                ));
            }
            DdlStmt::AlterTable(alter) => {
                let (database, table, _) = self.qualified(&alter.name);
                self.visit_info.push(visit(
                    tidb_mysql::AlterPriv,
                    &database,
                    &table,
                    table_denied("ALTER", &table),
                ));
                for action in &alter.actions {
                    match action {
                        tidb_ast::AlterTableAction::ReadOnly(_) => {
                            // Go `PlanBuilder.buildDDL`: permit the writeable specification
                            // through CheckTableLock; LockTables checks conflicting modes.
                            self.visit_info[0].alter_writable = true;
                        }
                        tidb_ast::AlterTableAction::RenameTable { new_name: other }
                        | tidb_ast::AlterTableAction::Partition(
                            tidb_ast::AlterPartitionAction::Exchange { table: other, .. },
                        ) => {
                            let (_, new_table, _) = self.qualified(other);
                            self.visit_info.push(visit(
                                tidb_mysql::DropPriv,
                                &database,
                                &table,
                                table_denied("DROP", &table),
                            ));
                            self.visit_info.push(visit(
                                tidb_mysql::CreatePriv,
                                &database,
                                &new_table,
                                table_denied("CREATE", &new_table),
                            ));
                            self.visit_info.push(visit(
                                tidb_mysql::InsertPriv,
                                &database,
                                &new_table,
                                table_denied("INSERT", &new_table),
                            ));
                        }
                        tidb_ast::AlterTableAction::Partition(
                            tidb_ast::AlterPartitionAction::Drop { .. }
                            | tidb_ast::AlterPartitionAction::Truncate { .. },
                        ) => {
                            // Go reads `v.Table.Schema.L`, which the
                            // preprocessor filled.
                            self.visit_info.push(visit(
                                tidb_mysql::DropPriv,
                                &database,
                                &table,
                                table_denied("DROP", &table),
                            ));
                        }
                        tidb_ast::AlterTableAction::AddStatistics { .. } => {
                            self.visit_info.push(visit(
                                tidb_mysql::SelectPriv,
                                &database,
                                &table,
                                table_denied("ADD STATS_EXTENDED", &table),
                            ));
                            self.visit_info.push(visit(
                                tidb_mysql::InsertPriv,
                                "mysql",
                                "stats_extended",
                                table_denied("ADD STATS_EXTENDED", "stats_extended"),
                            ));
                        }
                        tidb_ast::AlterTableAction::DropStatistics { .. } => {
                            self.visit_info.push(visit(
                                tidb_mysql::UpdatePriv,
                                "mysql",
                                "stats_extended",
                                table_denied("DROP STATS_EXTENDED", "stats_extended"),
                            ));
                        }
                        tidb_ast::AlterTableAction::AddForeignKey(fk) => {
                            if let Some(refer) = &fk.reference.table {
                                self.append_references_visit(refer, &database);
                            }
                        }
                        _ => {}
                    }
                }
            }
            DdlStmt::AlterSequence(alter) => {
                let (database, table, _) = self.qualified(&alter.name);
                self.visit_info.push(visit(
                    tidb_mysql::AlterPriv,
                    &database,
                    &table,
                    table_denied("ALTER", &table),
                ));
            }
            DdlStmt::CreateDatabase { name, .. } => {
                self.visit_info.push(visit(
                    tidb_mysql::CreatePriv,
                    &go_to_lower(name),
                    "",
                    db_denied(name),
                ));
            }
            DdlStmt::CreateIndex(create) => {
                let (database, table, _) = self.qualified(&create.table);
                self.visit_info.push(visit(
                    tidb_mysql::IndexPriv,
                    &database,
                    &table,
                    table_denied("INDEX", &table),
                ));
            }
            DdlStmt::CreateTable(create) => {
                let temporary = create.temporary != tidb_ast::CreateTableTemporary::None;
                let has_fk = create
                    .table_constraints
                    .iter()
                    .any(|constraint| matches!(constraint, tidb_ast::TableConstraint::ForeignKey(_)));
                if temporary && has_fk {
                    return Err(PlanError::from(
                        tidb_infoschema::infoschema_errors::ERR_CANNOT_ADD_FOREIGN
                            .gen_with_stack_by_args(&[]),
                    ));
                }
                let (database, table, _) = self.qualified(&create.name);
                // The privilege check of a LOCAL temporary table is towards
                // the database (`VisitInfo4PrivCheck`).
                let mut auth_err = if create.temporary == tidb_ast::CreateTableTemporary::Local {
                    db_denied(&database)
                } else {
                    table_denied("CREATE", &table)
                };
                for constraint in &create.table_constraints {
                    if let tidb_ast::TableConstraint::ForeignKey(fk) = constraint {
                        if let Some(refer) = &fk.reference.table {
                            let (refer_db, refer_table) = self.qualified_in(refer, &database);
                            auth_err = table_denied("REFERENCES", &refer_table);
                            self.visit_info.push(visit(
                                tidb_mysql::ReferencesPriv,
                                &refer_db,
                                &refer_table,
                                auth_err.clone(),
                            ));
                        }
                    }
                }
                self.visit_info
                    .push(visit(tidb_mysql::CreatePriv, &database, &table, auth_err));
                if let Some(refer) = &create.like_table {
                    let (refer_db, refer_table, _) = self.qualified(refer);
                    self.visit_info.push(visit(
                        tidb_mysql::SelectPriv,
                        &refer_db,
                        &refer_table,
                        table_denied("CREATE", &refer_table),
                    ));
                }
            }
            DdlStmt::CreateView(create) => {
                // Go builds the view's SELECT first (its visits come first),
                // under `isCreateView` / `canExpandAST` / `renameView`, then
                // appends CREATE VIEW, DROP for OR REPLACE, and SUPER for
                // another definer.
                let (database, table, _) = self.qualified(&create.name);
                let mut body = (**create).clone();
                let _ = self.build_create_view(&database, &mut body)?;
                self.visit_info.push(visit(
                    tidb_mysql::CreateViewPriv,
                    &database,
                    &table,
                    table_denied("CREATE VIEW", &table),
                ));
                if create.or_replace {
                    self.visit_info.push(visit(
                        tidb_mysql::DropPriv,
                        &database,
                        &table,
                        table_denied("DROP", &table),
                    ));
                }
                // Go: `CURRENT_USER` becomes the session user, then
                // `v.Definer.String() != User.String()` needs SUPER.
                if let Some(session) = self.session_ctx {
                    if let Some((user, host)) = session.user_identity() {
                        let definer = &create.definer;
                        if !definer.current_user && (definer.user != user || definer.host != host) {
                            self.visit_info
                                .push(visit(tidb_mysql::SuperPriv, "", "", specific("SUPER")));
                        }
                    }
                }
            }
            // Go master `buildDDL`: the SELECT is built (its errors first,
            // then the column-list count), and CREATE VIEW is checked on the
            // view's name.
            DdlStmt::CreateMaterializedView(create) => {
                if super::create_view::check_for_user_variables(&create.select) {
                    return Err(PlanError::from(tidb_dbterror::ERR_VIEW_SELECT_VARIABLE.gen_with_stack_by_args(&[])));
                }
                tidb_mviewutil::check_materialized_view_select(&create.select).map_err(PlanError::from)?;
                let node = tidb_resolve::NodeW::new((*create.select).clone());
                let plan = self.build_query_node(&node, false)?;
                let schema_len = plan.schema().map_or(0, |schema| schema.columns.len());
                if create.cols.len() != schema_len {
                    return Err(PlanError::from(tidb_dbterror::ERR_VIEW_WRONG_LIST.gen_with_stack_by_args(&[])));
                }
                let (database, table, _) = self.qualified(&create.view_name);
                if database.is_empty() {
                    return Err(PlanError::from(tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
                }
                self.visit_info.push(visit(
                    tidb_mysql::CreateViewPriv,
                    &database,
                    &table,
                    table_denied("CREATE VIEW", &table),
                ));
            }
            DdlStmt::CreateSequence(create) => {
                let (database, table, _) = self.qualified(&create.name);
                self.visit_info.push(visit(
                    tidb_mysql::CreatePriv,
                    &database,
                    &table,
                    table_denied("CREATE", &table),
                ));
            }
            DdlStmt::DropDatabase { name, .. } => {
                self.visit_info.push(visit(
                    tidb_mysql::DropPriv,
                    &go_to_lower(name),
                    "",
                    db_denied(name),
                ));
            }
            DdlStmt::DropIndex(drop) => {
                let (database, table, _) = self.qualified(&drop.table);
                self.visit_info.push(visit(
                    tidb_mysql::IndexPriv,
                    &database,
                    &table,
                    table_denied("INDEX", &table),
                ));
            }
            DdlStmt::DropTable(drop) => {
                for name in &drop.names {
                    let (database, table, _) = self.qualified(name);
                    self.visit_info.push(visit(
                        tidb_mysql::DropPriv,
                        &database,
                        &table,
                        table_denied("DROP", &table),
                    ));
                }
            }
            DdlStmt::DropView { names, .. } => {
                for name in names {
                    let (database, table, _) = self.qualified(name);
                    self.visit_info.push(visit(
                        tidb_mysql::DropPriv,
                        &database,
                        &table,
                        table_denied("DROP", &table),
                    ));
                }
            }
            DdlStmt::DropSequence(drop) => {
                for name in &drop.names {
                    let (database, table, _) = self.qualified(name);
                    self.visit_info.push(visit(
                        tidb_mysql::DropPriv,
                        &database,
                        &table,
                        table_denied("DROP", &table),
                    ));
                }
            }
            DdlStmt::TruncateTable(name) => {
                let (database, table, _) = self.qualified(name);
                self.visit_info.push(visit(
                    tidb_mysql::DropPriv,
                    &database,
                    &table,
                    table_denied("DROP", &table),
                ));
            }
            DdlStmt::RenameTable(rename) => {
                if let Some((old, new)) = rename.pairs.first() {
                    let (old_db, old_table, _) = self.qualified(old);
                    let (new_db, new_table, _) = self.qualified(new);
                    self.visit_info.push(visit(
                        tidb_mysql::AlterPriv,
                        &old_db,
                        &old_table,
                        table_denied("ALTER", &old_table),
                    ));
                    self.visit_info.push(visit(
                        tidb_mysql::DropPriv,
                        &old_db,
                        &old_table,
                        table_denied("DROP", &old_table),
                    ));
                    self.visit_info.push(visit(
                        tidb_mysql::CreatePriv,
                        &new_db,
                        &new_table,
                        table_denied("CREATE", &new_table),
                    ));
                    self.visit_info.push(visit(
                        tidb_mysql::InsertPriv,
                        &new_db,
                        &new_table,
                        table_denied("INSERT", &new_table),
                    ));
                }
            }
            DdlStmt::RecoverTable(recover) => match &recover.table {
                None => self
                    .visit_info
                    .push(visit(tidb_mysql::SuperPriv, "", "", VisitAuthError::None)),
                Some(name) => {
                    let (database, table, _) = self.qualified(name);
                    self.visit_info.push(visit(
                        tidb_mysql::CreatePriv,
                        &database,
                        &table,
                        table_denied("CREATE", &table),
                    ));
                    self.visit_info.push(visit(
                        tidb_mysql::DropPriv,
                        &database,
                        &table,
                        table_denied("DROP", &table),
                    ));
                }
            },
            DdlStmt::FlashbackTable(flashback) => {
                let (database, table, _) = flashback
                    .table
                    .as_deref()
                    .map(|name| self.qualified(name))
                    .unwrap_or_default();
                self.visit_info.push(visit(
                    tidb_mysql::CreatePriv,
                    &database,
                    &table,
                    table_denied("CREATE", &table),
                ));
                self.visit_info.push(visit(
                    tidb_mysql::DropPriv,
                    &database,
                    &table,
                    table_denied("DROP", &table),
                ));
            }
            DdlStmt::FlashbackDatabase(flashback) => {
                let database = go_to_lower(&flashback.name);
                self.visit_info.push(visit(
                    tidb_mysql::CreatePriv,
                    &database,
                    "",
                    db_denied(&database),
                ));
                self.visit_info.push(visit(
                    tidb_mysql::DropPriv,
                    &database,
                    "",
                    db_denied(&database),
                ));
            }
            // Flashback cluster, CLEANUP TABLE LOCK and REPAIR TABLE can only
            // be executed by an administrator.
            DdlStmt::FlashbackToTimestamp(_)
            | DdlStmt::RepairTable(_)
            | DdlStmt::CleanupTableLock(_) => self
                .visit_info
                .push(visit(tidb_mysql::SuperPriv, "", "", VisitAuthError::None)),
            DdlStmt::LockTables(locks) => {
                for lock in locks.iter() {
                    let (database, table, _) = self.qualified(&lock.table);
                    self.visit_info.push(visit(
                        tidb_mysql::LockTablesPriv,
                        &database,
                        &table,
                        db_denied(&database),
                    ));
                    self.visit_info.push(visit(
                        tidb_mysql::SelectPriv,
                        &database,
                        &table,
                        table_denied("SELECT", &table),
                    ));
                }
            }
            DdlStmt::CreatePlacementPolicy(_)
            | DdlStmt::AlterPlacementPolicy(_)
            | DdlStmt::DropPlacementPolicy(_) => self.visit_info.push(dynamic_visit(
                &["PLACEMENT_ADMIN"],
                false,
                specific("SUPER or PLACEMENT_ADMIN"),
            )),
            DdlStmt::CreateResourceGroup(_)
            | DdlStmt::AlterResourceGroup(_)
            | DdlStmt::DropResourceGroup(_) => self.visit_info.push(dynamic_visit(
                &["RESOURCE_GROUP_ADMIN"],
                false,
                specific("SUPER or RESOURCE_GROUP_ADMIN"),
            )),
            DdlStmt::OptimizeTable(_) => {
                return Err(PlanError::from(
                    tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL
                        .gen_with_stack("OPTIMIZE TABLE is not supported"),
                ));
            }
            // Go master: the preprocessor's `handleTableName` (the base table
            // must exist), then `buildDDL`'s visit infos on the log table
            // (`materializedViewLogNameForBaseTable`) and the base table.
            DdlStmt::CreateMaterializedViewLog(create) => {
                let (database, table, mlog_name) = self.materialized_view_log_target(&create.table)?;
                self.visit_info.push(visit(
                    tidb_mysql::CreateViewPriv,
                    &database,
                    mlog_name.lowercase(),
                    table_denied("CREATE MATERIALIZED VIEW LOG", &table),
                ));
                self.visit_info.push(visit(tidb_mysql::SelectPriv, &database, &table, table_denied("SELECT", &table)));
            }
            // Go master `buildDDL`: DROP on the log table
            // (`materializedViewLogNameForBaseTable`).
            DdlStmt::DropMaterializedViewLog(drop) => {
                let (database, table, mlog_name) = self.materialized_view_log_target(&drop.table)?;
                self.visit_info.push(visit(
                    tidb_mysql::DropPriv,
                    &database,
                    mlog_name.lowercase(),
                    table_denied("DROP MATERIALIZED VIEW LOG", &table),
                ));
            }
            // Go master `buildDDL`: ALTER on the view's name.
            DdlStmt::AlterMaterializedView(alter) => {
                let (database, table, _) = self.qualified(&alter.view_name);
                if database.is_empty() {
                    return Err(PlanError::from(tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
                }
                self.visit_info.push(visit(tidb_mysql::AlterPriv, &database, &table, table_denied("ALTER", &table)));
            }
            // Go master `buildDDL`: DROP on the view's name.
            DdlStmt::DropMaterializedView(drop) => {
                let (database, table, _) = self.qualified(&drop.view_name);
                if database.is_empty() {
                    return Err(PlanError::from(tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
                }
                self.visit_info.push(visit(tidb_mysql::DropPriv, &database, &table, table_denied("DROP", &table)));
            }
            // Go master `buildDDL`: ALTER on the log table, and SELECT on the
            // base table when an action adds columns.
            DdlStmt::AlterMaterializedViewLog(alter) => {
                let (database, table, mlog_name) = self.materialized_view_log_target(&alter.table)?;
                self.visit_info.push(visit(
                    tidb_mysql::AlterPriv,
                    &database,
                    mlog_name.lowercase(),
                    table_denied("ALTER", mlog_name.lowercase()),
                ));
                if alter
                    .actions
                    .iter()
                    .any(|action| matches!(action, tidb_ast::AlterMaterializedViewLogAction::AddColumn(_)))
                {
                    self.visit_info.push(visit(tidb_mysql::SelectPriv, &database, &table, table_denied("SELECT", &table)));
                }
            }
            _ => {}
        }
        Ok(Plan::Ddl(Box::new(Ddl {
            statement: stmt.clone(),
        })))
    }

    /// Go master: the preprocessor's `handleTableName` on a materialized
    /// view log statement's base table (which must exist; a view or a
    /// sequence counts),
    /// then `materializedViewLogNameForBaseTable`: the schema, the base
    /// table's lowercase name and the log table's name.
    fn materialized_view_log_target(&self, path: &[String]) -> Result<(String, String, tidb_ast::CiString), PlanError> {
        let (database, table, original) = self.qualified(path);
        if database.is_empty() {
            return Err(PlanError::from(tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
        }
        let written_db = match path {
            [schema, _] => schema.clone(),
            _ => self.source.current_database().to_owned(),
        };
        let base_name = match self.source.find_table(&database, &table) {
            Some(base) => base
                .table_info
                .as_ref()
                .map_or_else(|| tidb_ast::CiString::new(original.clone()), |info| info.read().name.clone()),
            None if matches!(self.source.try_find_view(&database, &table), Ok(Some(_))) => {
                tidb_ast::CiString::new(original.clone())
            }
            // Go's `TableByName` finds a sequence too; the executor refuses it.
            None => match self.source.find_sequence(&database, &table) {
                Some(sequence) => tidb_ast::CiString::new(sequence.to_owned()),
                None => {
                    return Err(PlanError::from(
                        tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                            .gen_with_stack_by_args(&[written_db.into(), original.into()]),
                    ));
                }
            },
        };
        let mlog_name = tidb_model::table::materialized_view_log_table_name(&base_name);
        Ok((database, table, mlog_name))
    }

    /// Go master `buildPurgeMaterializedViewLog`: a database to purge in and
    /// (Go's preprocessor) an existing target; the executor checks OPERATE
    /// VIEW on the log.
    fn build_purge_materialized_view_log(
        &mut self,
        purge: &tidb_ast::PurgeMaterializedViewLogStmt,
    ) -> Result<Plan, PlanError> {
        let (database, table, original) = self.qualified(&purge.table);
        if database.is_empty() {
            return Err(PlanError::from(tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
        }
        // Go master's preprocessor `handleTableName` on the target.
        if self.source.find_table(&database, &table).is_none()
            && !matches!(self.source.try_find_view(&database, &table), Ok(Some(_)))
            && self.source.find_sequence(&database, &table).is_none()
        {
            let written_db = match purge.table.as_slice() {
                [schema, _] => schema.clone(),
                _ => self.source.current_database().to_owned(),
            };
            return Err(PlanError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .gen_with_stack_by_args(&[written_db.into(), original.into()]),
            ));
        }
        Ok(Plan::PurgeMaterializedViewLog(Box::new(purge.clone())))
    }

    /// Go master `buildCompareMaterializedView`: a database for the view and
    /// for its OUTPUT table, OPERATE VIEW on the view, and CREATE and INSERT
    /// on the OUTPUT table. The preprocessor resolves neither
    /// (`inCreateOrDropTable`); the executor does.
    fn build_compare_materialized_view(
        &mut self,
        compare: &tidb_ast::CompareMaterializedViewStmt,
    ) -> Result<Plan, PlanError> {
        let (database, table, _) = self.qualified(&compare.view_name);
        if database.is_empty() {
            return Err(PlanError::from(tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
        }
        self.visit_info.push(visit(tidb_mysql::OperateViewPriv, &database, &table, table_denied("OPERATE VIEW", &table)));
        if let Some(output) = &compare.output_table {
            let (output_database, output_table, _) = self.qualified(output);
            if output_database.is_empty() {
                return Err(PlanError::from(tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
            }
            self.visit_info.push(visit(
                tidb_mysql::CreatePriv,
                &output_database,
                &output_table,
                table_denied("CREATE", &output_table),
            ));
            self.visit_info.push(visit(
                tidb_mysql::InsertPriv,
                &output_database,
                &output_table,
                table_denied("INSERT", &output_table),
            ));
        }
        Ok(Plan::CompareMaterializedView(Box::new(compare.clone())))
    }

    /// Go master `buildRefreshMaterializedViewImplement` for COMPLETE DELTA
    /// APPLY: the view and its diff source (`mview.BuildCompleteDiffSource`).
    /// FAST's merge plan (`mview.Build`) is not ported yet.
    fn build_refresh_materialized_view_implement(
        &mut self,
        implement: &tidb_ast::RefreshMaterializedViewImplementStmt,
    ) -> Result<Plan, PlanError> {
        let refresh = &implement.refresh_stmt;
        let mode = refresh.mode;
        if !matches!(
            mode,
            tidb_ast::RefreshMaterializedViewMode::Fast | tidb_ast::RefreshMaterializedViewMode::CompleteDeltaApply
        ) {
            return Err(PlanError::internal(format!(
                "RefreshMaterializedViewImplementStmt: unsupported mode {mode:?}"
            )));
        }
        let (database, table, original) = self.qualified(&refresh.view_name);
        if database.is_empty() {
            return Err(PlanError::from(tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
        }
        let info = self
            .source
            .find_table(&database, &table)
            .and_then(|table| table.table_info.as_ref().map(|info| info.read().clone_like_go()));
        let Some(info) = info else {
            return Err(PlanError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .gen_with_stack_by_args(&[database.clone().into(), original.into()]),
            ));
        };
        if info.materialized_view.is_none() {
            return Err(PlanError::internal(format!(
                "table {database}.{} is not a materialized view",
                info.name.original()
            )));
        }
        // Go master's FAST half: `mview.Build` over the window, which this
        // tier's executor runs with the catalog's base table and log, then
        // optimizes as it does every derived SELECT.
        if mode == tidb_ast::RefreshMaterializedViewMode::Fast {
            return Ok(Plan::MViewDeltaMerge(Box::new(crate::common_plans::MViewDeltaMerge {
                database,
                name: info.name.original().to_owned(),
                options: crate::mview::BuildOptions {
                    from_ts: implement.last_successful_refresh_read_tso,
                    to_ts: implement.target_refresh_read_tso,
                },
                mlog_retained_lower_tso: implement.mlog_retained_lower_tso,
                registered_mlog_ids: implement.registered_mlog_ids.clone(),
            })));
        }
        let (charset, collation) = self.ctx.connection_charset_info();
        let (charset, collation) = (charset.to_owned(), collation.to_owned());
        let config = self.parser_config;
        let parse = |sql: &str| {
            tidb_parser::parse_with_config_and_connection(sql, config, &charset, &collation)
                .map_err(|error| error.compatibility_message(sql))
        };
        let diff = crate::mview::build_complete_diff_source(&info, &database, &parse).map_err(PlanError::internal)?;
        Ok(Plan::MViewCompleteDeltaApply(Box::new(crate::common_plans::MViewCompleteDeltaApply {
            database,
            name: info.name.original().to_owned(),
            diff,
        })))
    }

    /// Go master `buildRefreshMaterializedView`: OPERATE VIEW on the view,
    /// and SHOW VIEW for DRY RUN / WITH PROFILE.
    fn build_refresh_materialized_view(
        &mut self,
        refresh: &tidb_ast::RefreshMaterializedViewStmt,
    ) -> Result<Plan, PlanError> {
        let (database, table, original) = self.qualified(&refresh.view_name);
        if database.is_empty() {
            return Err(PlanError::from(tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
        }
        // Go master's preprocessor `handleTableName` on the target, except for
        // COMPLETE OUT OF PLACE, which validates its target in the executor.
        if refresh.mode != tidb_ast::RefreshMaterializedViewMode::CompleteOutOfPlace
            && self.source.find_table(&database, &table).is_none()
            && !matches!(self.source.try_find_view(&database, &table), Ok(Some(_)))
            && self.source.find_sequence(&database, &table).is_none()
        {
            let written_db = match refresh.view_name.as_slice() {
                [schema, _] => schema.clone(),
                _ => self.source.current_database().to_owned(),
            };
            return Err(PlanError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .gen_with_stack_by_args(&[written_db.into(), original.into()]),
            ));
        }
        self.visit_info.push(visit(tidb_mysql::OperateViewPriv, &database, &table, table_denied("OPERATE VIEW", &table)));
        if refresh.observe_type != tidb_ast::RefreshMaterializedViewObserveType::None {
            self.visit_info.push(visit(tidb_mysql::ShowViewPriv, &database, &table, table_denied("SHOW VIEW", &table)));
        }
        Ok(Plan::RefreshMaterializedView(Box::new(refresh.clone())))
    }

    /// A referenced table named relative to `owner_db`, as Go's
    /// preprocessor qualifies a foreign key's reference with the child
    /// table's schema: `(schema.L, name.L)`.
    fn qualified_in(&self, path: &[String], owner_db: &str) -> (String, String) {
        match path {
            [schema, table] => (go_to_lower(schema), go_to_lower(table)),
            [table] => (go_to_lower(owner_db), go_to_lower(table)),
            _ => (String::new(), String::new()),
        }
    }

    /// Go `buildDDL`'s REFERENCES visit for a foreign key `ALTER TABLE` adds.
    fn append_references_visit(&mut self, refer: &[String], owner_db: &str) {
        let (database, table) = self.qualified_in(refer, owner_db);
        self.visit_info.push(visit(
            tidb_mysql::ReferencesPriv,
            &database,
            &table,
            table_denied("REFERENCES", &table),
        ));
    }
}

/// Go `Simple{Statement: node}` for a statement with no visit of its own.
fn simple(stmt: &Stmt) -> Plan {
    Plan::Simple(Box::new(Simple {
        statement: stmt.clone(),
        stale_txn_start_ts: 0,
    }))
}

/// Go `SQLBindPlan{SQLBindOp: op}` for the ADMIN binding maintenance ops.
fn sql_bind(op: SqlBindOp) -> Plan {
    Plan::SqlBind(Box::new(SqlBindPlan {
        op,
        statement: None,
    }))
}

/// Go `FlushStmt.DedupFlushObjects` / `RefreshStatsStmt.Dedup`: a global
/// object covers everything, a database covers its tables, and repeats go.
fn dedup_stats_objects(objects: &mut Vec<tidb_ast::StatsObject>) {
    if objects
        .iter()
        .any(|object| matches!(object, tidb_ast::StatsObject::Global))
    {
        *objects = vec![tidb_ast::StatsObject::Global];
        return;
    }
    let databases: Vec<String> = objects
        .iter()
        .filter_map(|object| match object {
            tidb_ast::StatsObject::Database(database) => Some(go_to_lower(database)),
            _ => None,
        })
        .collect();
    let mut seen = std::collections::HashSet::new();
    objects.retain(|object| {
        let key = match object {
            tidb_ast::StatsObject::Global => "*.*".to_owned(),
            tidb_ast::StatsObject::Database(database) => format!("{}.*", go_to_lower(database)),
            tidb_ast::StatsObject::Table { database, table } => {
                let database = go_to_lower(database.as_deref().unwrap_or_default());
                if databases.contains(&database) {
                    return false;
                }
                format!("{database}.{}", go_to_lower(table))
            }
        };
        seen.insert(key)
    });
}

/// Go `VisitInfo4PrivCheck` (`optimizer.go`): the visit infos the privilege
/// check reads. A LOCAL temporary table is checked through the database's
/// CREATE TEMPORARY TABLES privilege when created and not at all otherwise
/// (`needCheckTmpTablePriv`), except for the statements that ignore local
/// temporary tables.
pub fn visit_info_4_priv_check(
    stmt: &Stmt,
    visits: Vec<VisitInfo>,
    is_local_temporary: impl Fn(&str, &str) -> bool,
) -> Vec<VisitInfo> {
    let need_check = |visit: &VisitInfo| {
        visit.database.is_empty()
            || visit.table.is_empty()
            || !is_local_temporary(&visit.database, &visit.table)
    };
    match stmt {
        Stmt::Ddl(ddl) => match ddl.as_ref() {
            DdlStmt::CreateTable(create) => visits
                .into_iter()
                .filter_map(|visit| {
                    if visit.privilege == tidb_mysql::CreatePriv {
                        if create.temporary == tidb_ast::CreateTableTemporary::Local {
                            // `CREATE TEMPORARY TABLE` privilege is required
                            // from the database, not the table.
                            Some(VisitInfo {
                                privilege: tidb_mysql::CreateTMPTablePriv,
                                table: String::new(),
                                ..visit
                            })
                        } else {
                            Some(visit)
                        }
                    } else {
                        need_check(&visit).then_some(visit)
                    }
                })
                .collect(),
            DdlStmt::DropTable(drop) => {
                if drop.temporary == tidb_ast::DropTemporary::Local {
                    // Dropping a local temporary table doesn't need any
                    // privileges.
                    Vec::new()
                } else {
                    visits.into_iter().filter(need_check).collect()
                }
            }
            DdlStmt::DropView { .. } => visits,
            DdlStmt::DropSequence(_) | DdlStmt::DropPlacementPolicy(_) => visits,
            _ => visits.into_iter().filter(need_check).collect(),
        },
        Stmt::Admin(admin) if matches!(admin.as_ref(), AdminStmt::Grant(_)) => visits,
        _ => visits.into_iter().filter(need_check).collect(),
    }
}

/// A Go `fmt.Errorf` / `errors.New` value: no terror code, so the client
/// sees 1105.
fn plain_error(message: impl Into<String>) -> PlanError {
    PlanError::from(tidb_terror::MysqlError::unknown(message))
}

/// Go `opt.Value.(*expression.Constant)`: the rewritten literal.
fn alter_ddl_job_constant(opt: &AlterDdlJobOpt) -> &tidb_expr::constant::Constant {
    match opt.value.as_ref() {
        Some(tidb_expr::expression::Expression::Constant(constant)) => constant,
        // Go's type assertion panics; the parser admits only literals, which
        // the rewriter folds to constants.
        other => panic!("admin alter ddl jobs option {} is not a constant: {other:?}", opt.name),
    }
}

/// Go `Constant.RetType.EvalType()`.
fn constant_eval_type(constant: &tidb_expr::constant::Constant) -> tidb_parser_types::EvalType {
    constant.ret_type.as_ref().map_or(tidb_parser_types::EvalType::String, |tp| tp.eval_type())
}

/// Go `checkAlterDDLJobOptValue`.
fn check_alter_ddl_job_opt_value(opt: &AlterDdlJobOpt) -> Result<(), PlanError> {
    match opt.name.as_str() {
        crate::common_plans::ALTER_DDL_JOB_THREAD => {
            let thread = get_thread_or_batch_size_from_expression(opt)?;
            let max = tidb_vardef::tidb_vars::MAX_CONFIGURABLE_CONCURRENCY as i64;
            if thread < 1 || thread > max {
                return Err(plain_error(format!(
                    "the value {thread} for {} is out of range [1, {max}]",
                    opt.name
                )));
            }
        }
        crate::common_plans::ALTER_DDL_JOB_BATCH_SIZE => {
            let batch_size = get_thread_or_batch_size_from_expression(opt)?;
            let bs = batch_size as i32;
            let (min, max) = (
                tidb_vardef::tidb_vars::MIN_DDL_REORG_BATCH_SIZE,
                tidb_vardef::tidb_vars::MAX_DDL_REORG_BATCH_SIZE,
            );
            if bs < min || bs > max {
                return Err(plain_error(format!(
                    "the value {bs} for {} is out of range [{min}, {max}]",
                    opt.name
                )));
            }
        }
        crate::common_plans::ALTER_DDL_JOB_MAX_WRITE_SPEED => {
            let speed = get_max_write_speed_from_expression(opt).map_err(plain_error)?;
            if !(0..=PIB).contains(&speed) {
                return Err(plain_error(format!(
                    "the value {speed} for {} is out of range [0, {PIB}]",
                    opt.name
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

/// Go `units.PiB`.
const PIB: i64 = 1 << 50;

/// Go `GetThreadOrBatchSizeFromExpression`.
///
/// # Errors
///
/// Go's "only integer is allowed" error for a non-integer literal.
fn get_thread_or_batch_size_from_expression(opt: &AlterDdlJobOpt) -> Result<i64, PlanError> {
    let v = alter_ddl_job_constant(opt);
    match constant_eval_type(v) {
        tidb_parser_types::EvalType::Int => Ok(v.value.get_int64()),
        _ => Err(plain_error(format!(
            "the value for {} is invalid, only integer is allowed",
            opt.name
        ))),
    }
}

/// Go `GetMaxWriteSpeedFromExpression`: a string literal is a human size
/// (`units.RAMInBytes`), an integer literal is bytes.
///
/// # Errors
///
/// Go's parse or invalid-value message.
pub fn get_max_write_speed_from_expression(opt: &AlterDdlJobOpt) -> Result<i64, String> {
    let v = alter_ddl_job_constant(opt);
    match constant_eval_type(v) {
        tidb_parser_types::EvalType::String => {
            let speed_str = String::from_utf8_lossy(v.value.go_bytes());
            tidb_config_configtypes::ram_in_bytes(&speed_str)
                .map_err(|error| format!("parse max_write_speed value error: {error}"))
        }
        tidb_parser_types::EvalType::Int => Ok(v.value.get_int64()),
        _ => Err(format!(
            "the value {} for {} is invalid",
            String::from_utf8_lossy(&v.value.truncated_stringify().unwrap_or_default()),
            opt.name
        )),
    }
}
