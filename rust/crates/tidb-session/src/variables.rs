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

//! The variable surfaces: `SET` in all its forms, and the substitution that
//! turns a statement's `@@x` and `@x` references into something the planner --
//! which has no session -- can type.
//!
//! Go splits this between `executor.SetExecutor` (the write side) and the
//! expression rewriter's `rewriteSystemVariable` / `BuildGetVarFunction` (the
//! read side); both read the one [`crate::SessionVars`] this session owns, so
//! both live here.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_executor::compiler::NonPreparedPlanCacheCompile;
use std::borrow::Cow;

use tidb_ast::{DmlStmt, Hint, SessionStmt, Stmt, Visitable, Visitor};
use tidb_datatype::Datum;
use tidb_executor::DriverError;
use tidb_hack::{GoCaseCompare, GoToLower};

use crate::{sysvar, Session, VarError};

/// Go `kv.ReplicaReadFollower`, passed as a byte to avoid coupling the hint
/// package to the storage package.
const REPLICA_READ_FOLLOWER: u8 = 1;

#[derive(Clone, Copy)]
enum ResourceGroupHintRejection {
    Disabled,
    AccessDenied,
}

/// Go `SysVar.GetNativeValType` (`pkg/sessionctx/variable/variable.go:455`),
/// which `rewriteSystemVariable` applies to every `@@var` it folds into a
/// constant: the registry's `Type` -- not the variable's name -- decides the
/// value's domain. `TypeBool` becomes the signed `1`/`0` of `TiDBOptOn`, so
/// `SELECT @@autocommit` reports `1` and never the stored `ON`; `TypeUnsigned`
/// becomes a number; every other type stays the stored string.
///
/// Go builds a `Uint` datum for `TypeUnsigned`, which this AST has no literal
/// for: [`Expr::Int`] carries the digits, and `eval_in` settles the datum —
/// `Int` below `i64::MAX`, `UInt` above — so the full unsigned domain renders
/// as a number exactly as go's does.
fn sysvar_native_expr(name: &str, value: String) -> tidb_ast::Expr {
    use tidb_ast::Expr;
    match sysvar::get_sys_var(name).map(|def| def.var_type) {
        Some(sysvar::VarType::Bool) => {
            let on = value.eq_ignore_ascii_case("ON") || value == "1";
            Expr::Int(i32::from(on).to_string())
        }
        Some(sysvar::VarType::Unsigned)
            if value.parse::<i64>().is_ok() || value.parse::<u64>().is_ok() =>
        {
            Expr::Int(value)
        }
        _ => Expr::String(value),
    }
}

fn is_set_literal(expr: &tidb_ast::Expr) -> bool {
    matches!(
        expr,
        tidb_ast::Expr::Int(_)
            | tidb_ast::Expr::Decimal(_)
            | tidb_ast::Expr::Float(_)
            | tidb_ast::Expr::Hex(_)
            | tidb_ast::Expr::Bit(_)
            | tidb_ast::Expr::String(_)
            | tidb_ast::Expr::RawString(_)
            | tidb_ast::Expr::Bool(_)
            | tidb_ast::Expr::Null
    )
}

/// One complete mutable AST pass for variable substitution.
///
/// The AST owns the child graph, including functions, CASE, casts, windows,
/// subqueries and set-operation terms. Keeping that traversal generated in
/// `tidb-ast` prevents a new expression variant from silently bypassing the
/// session's scope and visibility checks.
struct VariableBinder<'a> {
    session: &'a Session,
    error: Option<DriverError>,
    /// Go `ErrGettingNoopVariable` (8145) per noop `@@var` read while
    /// `tidb_enable_noop_variables` is OFF; appended once the walk is over.
    noop_reads: Vec<String>,
}

impl Visitor for VariableBinder<'_> {
    fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
        if self.error.is_some() {
            return true;
        }
        let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
            return false;
        };
        if !matches!(expr, tidb_ast::Expr::SysVar { .. }) {
            return false;
        }
        if let tidb_ast::Expr::SysVar { name, .. } = &*expr {
            if let Some(def) = sysvar::get_sys_var(name) {
                if def.is_noop() && !self.session.vars.noop_variables_enabled() {
                    self.noop_reads.push(def.name.to_owned());
                }
            }
        }
        match self.session.bind_variable_atom(expr) {
            Ok(bound) => *expr = bound,
            Err(error) => self.error = Some(error),
        }
        true
    }

    fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
        self.error.is_none()
    }
}

/// Maps a variable error onto the driver error the wire layer renders.
pub(crate) fn var_error(error: VarError) -> DriverError {
    DriverError::from(error)
}

pub(crate) use tidb_executor::simple::account_tables::sysvar_datum_text as datum_text;

impl Session {
    /// Go session.SetGlobalSysVar notifies after validation/hooks and before
    /// persistence. Cache rebuilds only call the variable hook, never this path.
    fn notify_global_config_change(&self, name: &str, value: &str) {
        let Some(syncer) = &self.global_config_syncer else {
            return;
        };
        let Some(definition) = sysvar::get_sys_var(name) else {
            return;
        };
        if definition.global_config_name.is_empty() {
            return;
        }
        let value = match value {
            "ON" => "true",
            "OFF" => "false",
            _ => value,
        };
        let _ = syncer.notify(tidb_domain::globalconfigsync::GlobalConfigItem {
            name: definition.global_config_name.to_owned(),
            value: value.to_owned(),
            // Go Domain.NotifyGlobalConfigChange uses EventType_PUT.
            kind: tidb_domain::globalconfigsync::EventType::Put as i32,
            ..Default::default()
        });
    }

    /// Applies a `SET` statement.
    ///
    /// Returns `Some(())` when the SQL is a `SET` this handles and `None`
    /// otherwise, so a caller can answer with an OK packet without
    /// re-parsing. Go's `SetExecutor` walks the assignments in source order
    /// and stops at the first error, which this reproduces.
    ///
    /// `SET GLOBAL` writes straight into the shared [`tidb_domain::global_sysvars::GlobalSysvars`]
    /// this call was given (see [`Self::attach_globals`],
    /// [`Self::swap_globals`]), which is this process's only copy unless a
    /// front end also persists it: the convergence node's
    /// `crate::cluster_sysvar_seam` (in `tidb-server`) is what makes that
    /// table itself a scratch read of `mysql.global_variables`, validates
    /// this call against it, and persists the result. An in-process store
    /// whose bootstrap loaded the table writes the row into its own
    /// `mysql.global_variables` and rebuilds the sysvar cache from it; a
    /// storeless table keeps its rows in memory.
    ///
    /// Non-variable `SET` forms outside the resource-group selector remain
    /// unsupported.
    pub fn apply_set(&mut self, sql: &str) -> Result<Option<()>, DriverError> {
        let stmt = self.parse(sql)?;
        self.apply_set_stmt(&stmt)
    }

    /// [`Self::apply_set`] over a statement this session already parsed. The
    /// text form parses and delegates here; the `SET` family is recognized in
    /// exactly one place either way.
    pub fn apply_set_stmt(&mut self, stmt: &Stmt) -> Result<Option<()>, DriverError> {
        let Stmt::Session(session_stmt) = stmt else {
            return Ok(None);
        };
        match &**session_stmt {
            SessionStmt::Set(set) => {
                self.run_set_stmt(set)?;
                Ok(Some(()))
            }
            _ => Ok(None),
        }
    }

    /// The connection's persistent resource group. A statement hint does not
    /// modify this value.
    #[must_use]
    pub fn current_resource_group(&self) -> &str {
        &self.vars.resource_group
    }

    /// Go `StmtCtx.ResourceGroupName` for the statement currently executing.
    #[must_use]
    pub fn active_resource_group(&self) -> &str {
        &self.active_resource_group
    }

    /// Resolves Go `StmtCtx.ResourceGroupName`: the last top-level
    /// `RESOURCE_GROUP(name)` hint wins for this statement, otherwise the
    /// connection's `SET RESOURCE GROUP` selection is used.
    #[must_use]
    pub fn statement_resource_group<'a>(&'a self, stmt: &Stmt) -> Cow<'a, str> {
        let stmt_hints = parse_statement_hints_without_catalog(stmt, &self.vars.current_db);
        if stmt_hints.has_resource_group && self.resource_group_hint_rejection().is_none() {
            Cow::Owned(stmt_hints.resource_group)
        } else {
            Cow::Borrowed(&self.vars.resource_group)
        }
    }

    fn resource_group_hint_rejection(&self) -> Option<ResourceGroupHintRejection> {
        let resource_control_enabled = self.vars.resource_control_enabled();
        if !resource_control_enabled {
            return Some(ResourceGroupHintRejection::Disabled);
        }

        let strict_mode = self.vars.resource_control_strict_mode();
        (strict_mode
            && !self.has_dynamic_privilege("RESOURCE_GROUP_ADMIN", false)
            && !self.has_dynamic_privilege("RESOURCE_GROUP_USER", false))
        .then_some(ResourceGroupHintRejection::AccessDenied)
    }

    /// Resets Go `StmtCtx.ResourceGroupName` for one statement before its
    /// snapshot, transaction, planner, or executor is opened, and publishes
    /// it as the process list's `RESOURCE_GROUP` (Go `SetProcessInfo` reads
    /// the statement's group, hint included).
    pub(crate) fn activate_statement_resource_group(&mut self, stmt: &Stmt) {
        self.resolve_statement_resource_group(stmt);
        self.publish_statement_resource_group();
    }

    /// Publishes the statement's resource group to this connection's
    /// process-list entry.
    pub(crate) fn publish_statement_resource_group(&self) {
        if let Some(guard) = &self.process {
            guard
                .registry()
                .statement_resource_group(guard.id(), self.active_resource_group.clone());
        }
    }

    fn resolve_statement_resource_group(&mut self, stmt: &Stmt) {
        self.active_resource_group.clone_from(&self.vars.resource_group);
        let Some(hints) = statement_hints(stmt) else {
            return;
        };
        if hints.is_empty() {
            return;
        }
        let stmt_hints = parse_statement_hints_without_catalog(stmt, &self.vars.current_db);
        if !stmt_hints.has_resource_group {
            return;
        }

        match self.resource_group_hint_rejection() {
            Some(ResourceGroupHintRejection::Disabled) => {
                self.append_warning(
                    crate::warnings::WarningLevel::Warning,
                    8250,
                    "Resource control feature is disabled. Run `SET GLOBAL tidb_enable_resource_control='on'` to enable the feature".to_owned(),
                );
                return;
            }
            Some(ResourceGroupHintRejection::AccessDenied) => {
                self.append_warning(
                    crate::warnings::WarningLevel::Warning,
                    1227,
                    "Access denied; you need (at least one of) the SUPER or RESOURCE_GROUP_ADMIN or RESOURCE_GROUP_USER privilege(s) for this operation".to_owned(),
                );
                return;
            }
            None => {}
        }

        let hinted = stmt_hints.resource_group.go_to_lower();
        // Go `ExecuteStmt`: a hinted group that does not exist falls back
        // to the session's own group (Go only logs it).
        let exists = hinted == self.vars.resource_group
            || self
                .lock_catalog()
                .is_ok_and(|catalog| catalog.resource_group(&hinted).is_some());
        if exists {
            self.active_resource_group = hinted;
        }
    }

    /// Parses one text-protocol statement and resolves its statement-scoped
    /// resource group before any snapshot or transaction is opened.
    pub fn statement_resource_group_sql(&self, sql: &str) -> Result<String, DriverError> {
        let stmt = self.parse(sql)?;
        Ok(self.statement_resource_group(&stmt).into_owned())
    }

    /// Go `Compile` and `runStmt` for one `SET` statement: `buildSet` (its
    /// values rewritten, subqueries evaluated), `CheckPrivilege` over the
    /// visit infos -- SYSTEM_VARIABLES_ADMIN for GLOBAL/INSTANCE,
    /// RESTRICTED_VARIABLES_ADMIN under SEM -- so a denied assignment leaves
    /// the earlier ones unapplied, then `SetExecutor`.
    fn run_set_stmt(&mut self, set: &tidb_ast::SetStmt) -> Result<(), DriverError> {
        // Go's `rewriteSystemVariable` reads a system variable while the
        // value is built: `SET @@x = 7, @y = @@x` stores the OLD `@@x`.
        let mut set = set.clone();
        for variable in &mut set.variables {
            if variable.name != tidb_ast::SET_NAMES
                && variable.name != tidb_ast::SET_CHARSET
                && !matches!(variable.value, tidb_ast::Expr::Default(None))
            {
                variable.value = self.bind_variables_in(&variable.value)?;
            }
        }
        let ctx = self.statement_context(false);
        let database = self.current_database().to_owned();
        let (plan, visits) = self.with_catalog_retrieving(&ctx, |catalog| {
            tidb_executor::driver::plan_set_stmt(&set, catalog, &database, &ctx)
        })?;
        self.check_visit_infos(&visits)?;
        let result = tidb_executor::set::execute_set(self, &ctx, &plan);
        // Go's SET evaluates under the statement's own context, so what the
        // evaluation warned (`SET @a = (SELECT 1/0)`'s 1365) is the
        // statement's warning.
        self.drain_eval_warnings(&ctx);
        result
    }

    /// Go `SessionVars.SetSystemVar` for a SESSION assignment: the
    /// variable's hooks run in `tidb-variable` (`SessionVars::set_system_var`);
    /// the `SetSession` hooks that reach the session itself -- the random
    /// generator's seed, autocommit ending the transaction -- run here.
    pub(crate) fn set_session_sys_var(
        &mut self,
        name: &str,
        value: String,
    ) -> Result<(), DriverError> {
        let was_autocommit = self.is_autocommit();
        let result = self.vars.set_system_var(name, value);
        self.take_var_warnings();
        result.map_err(var_error)?;
        self.seed_rand_from_sysvar(name)?;
        // Go `sysvar.go`'s `AutoCommit.SetSession`: turning autocommit back
        // ON ends the ongoing transaction ("Implicitly commit the possible
        // ongoing transaction if mode is changed from off to on"). Only the
        // TRANSITION does it -- `SET autocommit = 1` while it is already on
        // leaves an explicit `BEGIN` running, which is why
        // `BEGIN; INSERT; SET autocommit = 1; ROLLBACK` still rolls back
        // (captured).
        if name.eq_ignore_ascii_case("autocommit") && !was_autocommit && self.is_autocommit() {
            // Go `SetInTxn(false)`; the statement's finish commits the store
            // transaction (`autoCommitAfterStmt`).
            self.leave_txn_ctx_for_commit();
        }
        Ok(())
    }

    /// Go `GlobalVarsAccessor.SetGlobalSysVar` (`is_global`) or
    /// `SetInstanceSysVar`: the variable's hooks and the node-wide table in
    /// `tidb-variable`, then what Go's domain does after the store -- the
    /// workload repository's hooks and the global config notification.
    pub(crate) fn set_node_wide_sys_var(
        &mut self,
        name: &str,
        value: String,
        is_global: bool,
    ) -> Result<(), DriverError> {
        if is_global {
            if let Some(services) = &self.domain_services {
                let sysvars = std::sync::Arc::clone(&services.sysvars);
                return self.set_global_sys_var_through(sysvars.as_ref(), name, value);
            }
        }
        let result = if is_global {
            self.vars.set_global_sys_var(name, value)
        } else {
            self.vars.set_instance_sys_var(name, value)
        };
        self.take_var_warnings();
        result.map_err(var_error)?;
        if is_global {
            // `SetGlobal` stores Go's type-normalized value. Workloadrepo
            // hooks consume that stored value too; passing the original
            // `-1`/`7201` text would make the worker reject an assignment
            // that the sysvar layer had already accepted and clamped.
            let stored = self.vars.get_global(name).map_err(var_error)?;
            self.apply_workload_repository_global(name, &stored)?;
            self.notify_global_config_change(name, &stored);
        }
        Ok(())
    }

    /// Go `GlobalVarsAccessor.SetGlobalSysVar` over the domain's
    /// `mysql.global_variables` (`set.go:166`): each GLOBAL assignment is
    /// validated against a scratch copy read inside its own change and
    /// persisted at once, so an assignment that fails leaves the earlier
    /// ones of its statement stored, and the user's transaction is not
    /// touched. `SetExecutor.setSysVariable` then logs it.
    fn set_global_sys_var_through(
        &mut self,
        sysvars: &dyn crate::stored_state_service::SysvarService,
        name: &str,
        value: String,
    ) -> Result<(), DriverError> {
        let pending = sysvars.begin()?;
        let live = self.swap_globals(Some(std::sync::Arc::new(pending.table())));
        let shown = if name == tidb_vardef::tidb_vars::TIDB_CLOUD_STORAGE_URI {
            tidb_ast::redact_url(&value)
        } else {
            value.clone()
        };
        let result = self
            .vars
            .set_global_sys_var(name, value)
            .and_then(|()| self.vars.get_global(name));
        self.swap_globals(live);
        self.take_var_warnings();
        // `SetGlobal` stores Go's type-normalized value, which the hooks
        // below consume (see `set_node_wide_sys_var`).
        let stored = result.map_err(var_error)?;
        pending.commit()?;
        tidb_util::logutil::bg_logger().info(
            "set global var",
            &[
                tidb_log::Field::new("conn", tidb_log::Value::U64(self.connection_id())),
                tidb_log::Field::new("name", tidb_log::Value::Str(name.to_owned())),
                tidb_log::Field::new("val", tidb_log::Value::Str(shown)),
            ],
        );
        self.apply_workload_repository_global(name, &stored)?;
        self.notify_global_config_change(name, &stored);
        Ok(())
    }

    /// Moves what the sysvar hooks appended onto the statement's warnings.
    pub(crate) fn take_var_warnings(&mut self) {
        for warning in self.vars.take_stmt_warnings() {
            self.append_sql_warning(warning);
        }
    }

    pub(crate) fn load_snapshot_schema_after_set(
        &mut self,
        old_ts: u64,
        one_shot: bool,
    ) -> Result<(), DriverError> {
        let ts = if one_shot {
            self.vars.txn_read_ts()
        } else {
            self.vars.snapshot_ts()
        };
        if ts == 0 {
            self.snapshot_schema = None;
            return Ok(());
        }
        let Some(provider) = &self.snapshot_schema_provider else {
            return Ok(());
        };
        match provider(ts, self.current_resource_group(), !one_shot && ts != old_ts) {
            Ok(schema) => {
                self.snapshot_schema = Some((ts, schema));
                Ok(())
            }
            Err(error) => {
                if one_shot {
                    self.vars.set_txn_read_ts(old_ts);
                } else {
                    self.vars.restore_snapshot_ts(old_ts);
                }
                Err(error)
            }
        }
    }

    fn apply_workload_repository_global(&self, name: &str, value: &str) -> Result<(), DriverError> {
        let Some(worker) = self.workload_repository.as_ref() else {
            return Ok(());
        };
        let result = match name.to_ascii_lowercase().as_str() {
            tidb_workloadrepo::REPOSITORY_DEST => worker.set_repository_dest(value),
            tidb_workloadrepo::REPOSITORY_RETENTION_DAYS => worker.set_retention_days(value),
            tidb_workloadrepo::REPOSITORY_SAMPLING_INTERVAL => worker.set_sampling_interval(value),
            tidb_workloadrepo::REPOSITORY_SNAPSHOT_INTERVAL => worker.set_snapshot_interval(value),
            _ => return Ok(()),
        };
        result.map_err(DriverError::unsupported)
    }

    /// Go's `rand_seed1`/`rand_seed2` `SetSession` hooks: the value SET is a
    /// raw seed for this session's `RAND()` generator, and is NOT retained as
    /// the variable's value.
    ///
    /// Both sysvars answer `GetSession` with the constant `"0"` in Go, so
    /// `@@rand_seed1`, `@@session.rand_seed1` and `SHOW VARIABLES LIKE
    /// 'rand_seed1'` all report 0 no matter what was set or what the generator
    /// has advanced to (captured on all three surfaces). Clearing the session
    /// override here reproduces that everywhere at once -- the variable table
    /// answers its own default -- instead of teaching each read path to special
    /// case these two names. Only `GetStateValue`, which serializes session
    /// state, ever exposes the live seeds, and this tier has no such surface.
    ///
    /// The value read back is the one `set_system` already NORMALIZED, so Go's
    /// clamping travels with it: `2147483648` arrives as `MaxInt32` and a
    /// negative arrives as 0, which is also what `tidbOptPositiveInt32` would
    /// have produced.
    pub(crate) fn seed_rand_from_sysvar(&mut self, name: &str) -> Result<(), DriverError> {
        let first = name.eq_ignore_ascii_case("rand_seed1");
        if !first && !name.eq_ignore_ascii_case("rand_seed2") {
            return Ok(());
        }
        let seed = self
            .vars
            .get_system(name)
            .map_err(var_error)?
            .parse::<u32>()
            .unwrap_or(0);
        if first {
            self.rand.set_seed1(seed);
        } else {
            self.rand.set_seed2(seed);
        }
        self.vars.reset_system(name).map_err(var_error)
    }

    /// Evaluates a `SET` right-hand side to its TYPED value, which is what a
    /// user variable stores (Go's `SetUserVarVal` takes a `types.Datum`).
    ///
    /// System variables are substituted before planning; user variables are
    /// resolved by the shared expression rewriter against the session owner.
    pub(crate) fn eval_value(&mut self, expr: &tidb_ast::Expr) -> Result<Datum, DriverError> {
        let bound = self.bind_variables_in(expr)?;
        self.eval_bound_value(&bound)
    }

    /// Evaluates an expression whose system variables have been substituted.
    fn eval_bound_value(&mut self, bound: &tidb_ast::Expr) -> Result<Datum, DriverError> {
        // An unquoted identifier is a bare word value such as `SET sql_mode =
        // ANSI_QUOTES` or `SET autocommit = ON`, which MySQL takes literally
        // (`SET @x = ANSI_QUOTES` stores the string too, confirmed via
        // `gorun`).
        if let tidb_ast::Expr::Column(path) = bound {
            if let [word] = path.as_slice() {
                return Ok(Datum::new_string(word.clone()));
            }
        }
        // Go SetExecutor evaluates constants directly, without allocating a
        // SELECT result under the previous query quota. In particular, SET
        // must be able to raise a quota after a cancelled query.
        if is_set_literal(bound) {
            return tidb_expr::eval(bound)
                .map_err(|error| DriverError::Exec(tidb_executor::ExecError::Eval(error)));
        }
        // A scalar subquery value runs as its own SELECT: Go's executor
        // evaluates the subplan and enforces the one-row scalar contract
        // (1242 on more than one row, NULL on none). The surrounding
        // `SELECT (subquery)` shape is not plannable here, so the inner
        // query runs directly over the same catalog.
        if let tidb_ast::Expr::Subquery(sub) = bound {
            let sql = sub.restore();
            let ctx = self.statement_context(false);
            let rows = self.set_value_rows(&sql, &ctx)?;
            return match rows.len() {
                0 => Ok(Datum::Null),
                1 => Ok(rows[0].first().cloned().unwrap_or(Datum::Null)),
                _ => Err(DriverError::from(
                    tidb_util::dbterror::exeerrors::ERR_SUBQUERY_MORE_THAN1_ROW
                        .generate_with_stack_by_args(&[]),
                )),
            };
        }
        let sql = format!("SELECT {}", bound.restore());
        let ctx = self.statement_context(false);
        let rows = self.set_value_rows(&sql, &ctx)?;
        Ok(rows
            .first()
            .and_then(|row| row.first())
            .cloned()
            .unwrap_or(Datum::Null))
    }

    /// Runs the SELECT a `SET` right-hand side is evaluated through. A read
    /// of information_schema plans its virtual tables as memory tables, as
    /// the statement itself would (Go plans the assignment's expression,
    /// subqueries included, like any other).
    pub(crate) fn set_value_rows(
        &mut self,
        sql: &str,
        ctx: &tidb_executor::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        // The SELECT is restored or written by this tier, so it is UTF-8.
        self.with_catalog_retrieving(ctx, |catalog| {
            tidb_executor::run_select_on(sql, catalog, ctx)
        })
    }

    /// Resolves system-variable scope and visibility before shared planning.
    /// User-variable AST nodes remain intact so the rewriter can observe inline
    /// assignments' declared types in source order.
    pub(crate) fn bind_variables(&mut self, stmt: &mut Stmt) -> Result<(), DriverError> {
        let mut binder = VariableBinder {
            session: self,
            error: None,
            noop_reads: Vec::new(),
        };
        // Go resolves `@@x` and `@x` inside DML too (a WHERE predicate, an
        // UPDATE SET expression, an INSERT VALUES row), so the walk covers
        // query AND Dml statements. DDL and session statements keep their
        // own handling and stay untouched.
        let accepted = match stmt {
            Stmt::Query(query) => query.accept(&mut binder),
            Stmt::Dml(dml) => dml.accept(&mut binder),
            _ => true,
        };
        if !accepted {
            return Err(binder
                .error
                .expect("variable traversal stops only after recording an error"));
        }
        let noop_reads = binder.noop_reads;
        self.append_noop_read_warnings(noop_reads);
        Ok(())
    }

    fn append_noop_read_warnings(&mut self, names: Vec<String>) {
        for name in names {
            self.append_warning(
                crate::warnings::WarningLevel::Warning,
                8145,
                format!("variable {name} has no effect in TiDB"),
            );
        }
    }

    /// Substitutes one variable atom. The complete child walk belongs to
    /// [`VariableBinder`], so scope validation cannot depend on expression
    /// shape.
    fn bind_variable_atom(&self, expr: &tidb_ast::Expr) -> Result<tidb_ast::Expr, DriverError> {
        use tidb_ast::Expr;
        Ok(match expr {
            Expr::SysVar { scope, name } => {
                let def = match sysvar::get_sys_var(name) {
                    Some(def) => def,
                    None => {
                        if let Some(reason) = sysvar::removed_sys_var_reason(name) {
                            return Err(var_error(tidb_error::mysql::MysqlError::from_terror(
                                &tidb_error::catalog::variable::ERR_VARIABLE_NO_LONGER_SUPPORTED
                                    .generate_with_stack_by_args(&[
                                        (name.to_ascii_lowercase()).into(),
                                        (reason.to_owned()).into(),
                                    ]),
                            )));
                        }
                        return Err(var_error(tidb_error::mysql::MysqlError::from_terror(
                            &tidb_error::catalog::variable::ERR_UNKNOWN_SYSTEM_VAR
                                .generate_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
                        )));
                    }
                };
                if def.scope != sysvar::SCOPE_NONE {
                    let incorrect_scope = match scope {
                        Some(tidb_ast::SysVarScope::Global)
                            if !(def.has_global_scope() || def.has_instance_scope()) =>
                        {
                            Some("SESSION")
                        }
                        Some(tidb_ast::SysVarScope::Instance) if !def.has_instance_scope() => {
                            Some("SESSION or GLOBAL")
                        }
                        Some(tidb_ast::SysVarScope::Session) if !def.has_session_scope() => {
                            Some("GLOBAL")
                        }
                        _ => None,
                    };
                    if let Some(allowed) = incorrect_scope {
                        return Err(var_error(tidb_error::mysql::MysqlError::from_terror(
                            &tidb_error::catalog::variable::ERR_INCORRECT_SCOPE
                                .generate_with_stack_by_args(&[
                                    (name.to_ascii_lowercase()).into(),
                                    (allowed).into(),
                                ]),
                        )));
                    }
                    if *scope == Some(tidb_ast::SysVarScope::Session)
                        && def.is_internal_session_variable()
                    {
                        return Err(var_error(tidb_error::mysql::MysqlError::from_terror(
                            &tidb_error::catalog::variable::ERR_UNKNOWN_SYSTEM_VAR
                                .generate_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
                        )));
                    }
                }
                self.require_sem_visible_sysvar(def.name)?;
                // `@@last_insert_id` and its `@@identity` alias are the SAME
                // value `LAST_INSERT_ID()` reports -- Go's
                // `StmtCtx.PrevLastInsertID` -- not an entry in the variable
                // table, which is why they are answered from the session's
                // publication rather than from `get_system`. `@@global.` on
                // either is still the variable table's error (captured).
                if *scope != Some(tidb_ast::SysVarScope::Global)
                    && (name.eq_ignore_ascii_case("last_insert_id")
                        || name.eq_ignore_ascii_case("identity"))
                {
                    return Ok(Expr::Int(self.vars.last_insert_id.to_string()));
                }
                // `@@warning_count` and `@@error_count` are Go's `GetSession`
                // hooks on `s.SysWarningCount` / `s.SysErrorCount`: the
                // PREVIOUS statement's counts, snapshotted by
                // `ResetContextOfStmt` at every statement start. The variable
                // table's entries carry only the type and the read-only flag,
                // so answering from them -- which this port did -- reported
                // the registration's `0` for every statement, including right
                // after one that warned.
                if *scope != Some(tidb_ast::SysVarScope::Global)
                    && name.eq_ignore_ascii_case("warning_count")
                {
                    // Both counts are Go's TypeStr session variables (MySQL
                    // renders them as strings), so the oracle answer reads
                    // ('1', '1') -- not integers.
                    return Ok(Expr::String(self.sys_warning_count().to_string()));
                }
                if *scope != Some(tidb_ast::SysVarScope::Global)
                    && name.eq_ignore_ascii_case("error_count")
                {
                    return Ok(Expr::String(self.sys_error_count().to_string()));
                }
                // `@@last_plan_from_cache` is Go's `PrevFoundInPlanCache`
                // read (`sysvar.go`'s GetSession hook), not a stored value --
                // the variable table's entry is only the type and the
                // read-only flag.
                if *scope != Some(tidb_ast::SysVarScope::Global)
                    && name.eq_ignore_ascii_case("last_plan_from_cache")
                {
                    return Ok(Expr::Int(
                        i32::from(self.last_plan_from_cache()).to_string(),
                    ));
                }
                // `@@last_sql_use_alloc` is Go's `preUseChunkAlloc` read: the
                // previous statement's `StmtCtx.useChunkAlloc`.
                if *scope != Some(tidb_ast::SysVarScope::Global)
                    && name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::TIDB_USE_ALLOC)
                {
                    return Ok(Expr::Int(i32::from(self.vars.pre_use_chunk_alloc).to_string()));
                }
                // `@@last_plan_from_binding` is Go's `PrevFoundInBinding`
                // read, for the same reason and at the same boundary as
                // `@@last_plan_from_cache` above.
                if *scope != Some(tidb_ast::SysVarScope::Global)
                    && name.eq_ignore_ascii_case("last_plan_from_binding")
                {
                    return Ok(Expr::Int(
                        i32::from(self.last_plan_from_binding()).to_string(),
                    ));
                }
                // `@@tidb_current_ts` is Go's `s.TxnCtx.StartTS` read
                // (`sysvar.go:297`'s GetSession hook): the timestamp the
                // OPEN transaction is reading at, and `0` when none is
                // open. The variable table's entry carries only the type
                // and the read-only flag, so answering from it -- which
                // this port did -- reported 0 for every transaction.
                if *scope != Some(tidb_ast::SysVarScope::Global)
                    && name.eq_ignore_ascii_case("tidb_current_ts")
                {
                    // Go's `GetSession` hook returns `FormatUint`, so the value is a
                    // string like every system variable's.
                    return Ok(Expr::String(self.current_tso().value().to_string()));
                }
                // `@@tidb_last_txn_info` is Go's `SessionVars.LastTxnInfo`
                // read: client-go's `TxnInfo` JSON for the last ACTIVATED
                // transaction, written at the session's commit points and
                // empty before the first one.
                if *scope != Some(tidb_ast::SysVarScope::Global)
                    && name.eq_ignore_ascii_case("tidb_last_txn_info")
                {
                    return Ok(Expr::String(self.last_txn_info_value()));
                }
                // `@@tidb_last_ddl_info`: Go's JSON-marshalled
                // `SessionVars.LastDDLInfo`.
                if *scope != Some(tidb_ast::SysVarScope::Global)
                    && name.eq_ignore_ascii_case("tidb_last_ddl_info")
                {
                    return Ok(Expr::String(self.last_ddl_info_value()));
                }
                // `@@tidb_last_query_info` is Go's JSON-marshalled
                // `SessionVars.LastQueryInfo` getter. Keep it out of the
                // registry's empty default so the zero-value JSON shape is
                // visible even before the first diagnostic query.
                if *scope != Some(tidb_ast::SysVarScope::Global)
                    && name.eq_ignore_ascii_case("tidb_last_query_info")
                {
                    return Ok(Expr::String(self.last_query_info_value()));
                }
                // A no-scope server property uses the same read authority as
                // an unqualified session read. Most answer their registry
                // default; `version_comment` is derived from the immutable
                // server identity installed when this connection opened.
                // GLOBAL and INSTANCE instead read the node-wide table.
                let result = match scope {
                    _ if def.scope == sysvar::SCOPE_NONE => self.vars.get_system(name),
                    Some(tidb_ast::SysVarScope::Global | tidb_ast::SysVarScope::Instance) => {
                        self.vars.get_global(name)
                    }
                    _ => self.vars.get_system(name),
                };
                match result {
                    Ok(value) => sysvar_native_expr(name, value),
                    Err(error) => return Err(var_error(error)),
                }
            }
            _ => unreachable!("VariableBinder only sends variable atoms to this helper"),
        })
    }

    /// Substitutes variables in a standalone expression, such as a `SET`
    /// value, through the same complete visitor a query uses.
    fn bind_variables_in(&mut self, expr: &tidb_ast::Expr) -> Result<tidb_ast::Expr, DriverError> {
        let mut bound = expr.clone();
        let mut binder = VariableBinder {
            session: self,
            error: None,
            noop_reads: Vec::new(),
        };
        if !bound.accept(&mut binder) {
            return Err(binder
                .error
                .expect("variable traversal stops only after recording an error"));
        }
        let noop_reads = binder.noop_reads;
        self.append_noop_read_warnings(noop_reads);
        Ok(bound)
    }

    /// Applies Go `optimize.go`'s `StmtHints.SetVars` after the canonical
    /// `hint.ParseStmtHints` pass: each `SET_VAR(name = value)` writes the
    /// session variable for the duration of THIS statement only, and where
    /// the same name appears twice the FIRST occurrence wins.
    ///
    /// The snapshot goes on [`Session::set_var_hint_restore`]. The next
    /// statement boundary restores it, as Go's `ResetContextOfStmt` does,
    /// including after an execution error.
    ///
    pub(crate) fn apply_set_var_hints(&mut self, stmt: &Stmt) -> Result<(), DriverError> {
        let Some(hints) = statement_hints(stmt) else {
            return Ok(());
        };
        if hints.is_empty() {
            return Ok(());
        }
        let current_database = self.vars.current_db.clone();
        let (mut stmt_hints, _, warnings) = {
            let catalog = self.lock_catalog()?;
            let mut set_var_checker = set_var_hint_checker;
            let mut hypo_index_checker = |database: &str, table: &str, column: &str| {
                let Some(table_entry) = catalog.table_in(database, table) else {
                    return Err(format!("table '{database}.{table}' doesn't exist"));
                };
                table_entry
                    .column_names()
                    .iter()
                    .position(|table_column| table_column.go_lower_eq(column))
                    .map(|offset| offset as i64)
                    .ok_or_else(|| {
                        format!("can't find column {column} in table {database}.{table}")
                    })
            };
            tidb_hint::parse_stmt_hints(
                hints,
                &mut set_var_checker,
                &mut hypo_index_checker,
                &current_database,
                REPLICA_READ_FOLLOWER,
            )
        };
        for warning in warnings {
            self.append_sql_warning(crate::SqlWarning::from_error(
                crate::warnings::WarningLevel::Warning,
                warning.code,
                warning.message,
                warning.identity,
            ));
        }
        let set_vars = std::mem::take(&mut stmt_hints.set_vars);
        self.stmt_hints = stmt_hints;
        for (name, value) in set_vars {
            let name = name.to_ascii_lowercase();
            let is_fix_control = name == tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL;
            if is_fix_control {
                match tidb_planner::fix_control::OptimizerFixControl::parse(&value) {
                    Ok((_parsed, warnings)) => {
                        for warning in warnings {
                            self.append_warning(
                                crate::warnings::WarningLevel::Warning,
                                1105,
                                warning,
                            );
                        }
                    }
                    Err(error) => {
                        self.append_warning(
                            crate::warnings::WarningLevel::Warning,
                            1105,
                            error.to_string(),
                        );
                        continue;
                    }
                }
            }
            // Go's `SetSystemVarWithRelaxedValidation`: the hook still
            // runs and still decides the value, but its error and its
            // warnings are dropped.
            let value = self.relaxed_noop_gated_value(&name, value);
            let snapshot = self.vars.snapshot_system(&name);
            let original_value = value.clone();
            match self.vars.set_system(&name, value) {
                Ok(_) => {
                    // Go `StmtCtx.AddSetVarHintRestore` stores the value that
                    // preceded the FIRST overlay for a name. This matters when
                    // the query and its matched binding both carry SET_VAR for
                    // the same variable: restoring the binding-time snapshot
                    // would leave the query-time overlay installed.
                    for (key, previous) in snapshot {
                        if !self
                            .set_var_hint_restore
                            .iter()
                            .any(|(saved, _)| saved == &key)
                        {
                            self.set_var_hint_restore.push((key, previous));
                        }
                    }
                }
                Err(error)
                    if !is_fix_control
                        && error.equal(&tidb_error::catalog::variable::ERR_WRONG_VALUE_FOR_VAR) =>
                {
                    // go's SET_VAR application validates the hint's variables
                    // TWICE (the plan build and the execute pass), and each
                    // pass's refusal lands in the warning buffer through the
                    // statement error door (oracle m21/g-hint:
                    // SET_VAR(sql_mode=STRICT) carries the 1231 row twice).
                    // Go `mysql.SQLError.Error()`: `ERROR %d (%s): %s`.
                    let message = format!(
                        "ERROR {} ({}): {}",
                        error.code,
                        String::from_utf8_lossy(&error.state),
                        error.message
                    );
                    for _ in 0..2 {
                        self.append_warning(
                            crate::warnings::WarningLevel::Warning,
                            1105,
                            message.clone(),
                        );
                    }
                }
                Err(_) if !is_fix_control && name == "sql_mode" => {
                    // go's SET_VAR application validates the hint's variables
                    // TWICE (the plan build and the execute pass), and each
                    // pass's refusal lands in the warning buffer through the
                    // statement error door (oracle m21/g-hint:
                    // SET_VAR(sql_mode=STRICT) carries the 1231 row twice;
                    // the sql_mode parse refuses through the validation-refused
                    // arm, so key this arm on the variable name).
                    let message = format!(
                        "ERROR 1231 (42000): Variable 'sql_mode' can't be set to the value of '{original_value}'"
                    );
                    for _ in 0..2 {
                        self.append_warning(
                            crate::warnings::WarningLevel::Warning,
                            1105,
                            message.clone(),
                        );
                    }
                }
                Err(_) if is_fix_control => {
                    unreachable!("the source-shaped fix-control pre-validation just succeeded");
                }
                Err(_) => {}
            }
        }
        Ok(())
    }

    /// Go `preprocess.go:TryAddExtraLimit`: while `sql_select_limit` is not
    /// at its `MaxUint64` default, a SELECT or set operation that writes no
    /// LIMIT of its own is given one, so the variable caps the result the same
    /// way an explicit `LIMIT n` would. A statement that DOES write a LIMIT is
    /// left alone, even one asking for more rows than the cap.
    ///
    /// DEFERRED (documented): Go's `ShowStmt` arm, gated on `NeedLimitRSRow()`
    /// -- the subset of SHOW forms whose rows a LIMIT may cut -- and its
    /// `ExplainStmt` arm, which caps the wrapped statement rather than the
    /// EXPLAIN. `SELECT ... INTO OUTFILE` is excluded exactly as Go excludes
    /// it, even though this tier refuses that clause anyway.
    pub(crate) fn try_add_extra_limit(&self, stmt: &mut Stmt) {
        let cap = self.vars.select_limit();
        if cap == u64::MAX {
            return;
        }
        let limit = tidb_ast::Limit {
            offset: None,
            count: tidb_ast::Expr::Int(cap.to_string()),
        };
        if let Stmt::Query(query) = stmt {
            match &mut **query {
                tidb_ast::QueryStmt::Select(select) => {
                    if select.limit.is_none() && select.into_outfile.is_none() {
                        select.limit = Some(limit);
                    }
                }
                tidb_ast::QueryStmt::SetOpr(set_opr) => {
                    if set_opr.limit.is_none() {
                        set_opr.limit = Some(limit);
                    }
                }
            }
        }
    }
}

/// The AST-owned statement hints Go applies before planning.
///
/// SQL bindings may replace a statement's hints later in optimization; that
/// producer belongs to `pkg/bindinfo` and is deliberately not folded into
/// this direct-AST helper.
pub(crate) fn statement_hints(stmt: &Stmt) -> Option<&[Hint]> {
    match stmt {
        Stmt::Query(query) => match &**query {
            tidb_ast::QueryStmt::Select(select) => Some(&select.hints),
            tidb_ast::QueryStmt::SetOpr(set_opr) => set_opr.terms.first().and_then(|term| {
                if let tidb_ast::SetOprTermBody::Select(select) = &term.body {
                    Some(select.hints.as_slice())
                } else {
                    None
                }
            }),
        },
        Stmt::Dml(dml) => dml_hints(dml),
        Stmt::Admin(admin) => {
            if let tidb_ast::AdminStmt::Explain(explain) = &**admin {
                explain.statement().and_then(statement_hints)
            } else {
                None
            }
        }
        Stmt::Ddl(_) | Stmt::Session(_) => None,
    }
}

fn set_var_hint_checker(
    variable_name: &str,
    hint_name: &str,
) -> (bool, Option<tidb_hint::HintWarning>) {
    let folded = variable_name.to_ascii_lowercase();
    if sysvar::get_sys_var(&folded).is_none() {
        return (
            false,
            Some(tidb_hint::HintWarning::of(
                &tidb_error::plannererrors::ERR_UNRESOLVED_HINT_NAME,
                format!("Unresolved name '{variable_name}' for {hint_name} hint"),
            )),
        );
    }
    let warning =
        (!tidb_exec::hint_updatable_vars::is_hint_updatable_verified(&folded)).then(|| {
            tidb_hint::HintWarning::of(
                &tidb_error::plannererrors::ERR_NOT_HINT_UPDATABLE,
                format!("Variable '{variable_name}' might not be affected by SET_VAR hint."),
            )
        });
    (true, warning)
}

/// Runs Go `ParseStmtHints` for consumers that do not read hypothetical-index
/// metadata. Warnings are intentionally left to the execution-time parse,
/// where Go appends them to StmtCtx exactly once.
pub(crate) fn parse_statement_hints_without_catalog(
    stmt: &Stmt,
    current_database: &str,
) -> tidb_hint::StmtHints {
    let hints = statement_hints(stmt).unwrap_or_default();
    let mut set_var_checker = set_var_hint_checker;
    let mut hypo_index_checker = |_database: &str, _table: &str, _column: &str| {
        Err("hypothetical-index metadata is unavailable in this hint consumer".to_owned())
    };
    tidb_hint::parse_stmt_hints(
        hints,
        &mut set_var_checker,
        &mut hypo_index_checker,
        current_database,
        REPLICA_READ_FOLLOWER,
    )
    .0
}

/// Fix 52592 as it will be seen by this statement's planner, computed before
/// the statement-local overlay is installed.
///
/// The first direct-AST SET_VAR for this name owns the slot even when its
/// value is invalid; in that case execution keeps the persistent value, so
/// classification must do the same. Binding-injected hints are deliberately
/// outside this helper and belong to `pkg/bindinfo`.
pub(crate) fn effective_fix_52592(
    stmt: &Stmt,
    persistent: &tidb_planner::fix_control::OptimizerFixControl,
) -> bool {
    let persistent = persistent.get_bool_with_default(tidb_planner::fix_control::FIX_52592, false);
    parse_statement_hints_without_catalog(stmt, "")
        .set_vars
        .get(tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL)
        .and_then(|value| tidb_planner::fix_control::OptimizerFixControl::parse(value).ok())
        .map_or(persistent, |(overlay, _warnings)| {
            overlay.get_bool_with_default(tidb_planner::fix_control::FIX_52592, false)
        })
}

fn dml_hints(dml: &DmlStmt) -> Option<&[Hint]> {
    match dml {
        DmlStmt::Insert(insert) => Some(&insert.hints),
        DmlStmt::Update(update) => Some(&update.hints),
        DmlStmt::Delete(delete) => Some(&delete.hints),
        DmlStmt::Batch(batch) => match &batch.dml {
            tidb_ast::BatchDml::Insert(insert) => Some(&insert.hints),
            tidb_ast::BatchDml::Update(update) => Some(&update.hints),
            tidb_ast::BatchDml::Delete(delete) => Some(&delete.hints),
        },
        DmlStmt::ImportInto(_)
        | DmlStmt::LoadData(_)
        | DmlStmt::DistributeTable(_)
        | DmlStmt::Call(_) => None,
    }
}
