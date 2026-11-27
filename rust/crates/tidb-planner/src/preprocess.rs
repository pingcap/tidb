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

//! Go `pkg/planner/core/preprocess.go`: the `preprocessor` walk
//! ([`preprocess`]), its name qualification (`handleTableName` and the CTE
//! scope, `preprocessWith`, it consults) and its grammar checks over DDL
//! statements (`checkDDLGrammar` below).

use std::any::Any;

use tidb_ast::{Visitable, Visitor};
use tidb_hack::go_to_lower;

use tidb_planner_core_base::PlanError;

/// Query-local CTE visibility shared by physical-table admission and PREPARE
/// database pinning. Go's preprocessWith publishes ordinary names after their
/// definition, recursive names before it, and restores the enclosing scope.
#[derive(Default)]
pub struct CteScope {
    visible: Vec<String>,
    offsets: Vec<usize>,
    recursive: Vec<bool>,
}

impl CteScope {
    /// Tracks the scope a visitor enters; call from `Visitor::enter`.
    pub fn enter(&mut self, node: &mut dyn Any) {
        if node.is::<tidb_ast::SelectStmt>() || node.is::<tidb_ast::SetOprStmt>() {
            self.offsets.push(self.visible.len());
        } else if let Some(with) = node.downcast_mut::<tidb_ast::WithClause>() {
            self.recursive.push(with.recursive);
        } else if let Some(cte) = node.downcast_mut::<tidb_ast::Cte>() {
            self.offsets.push(self.visible.len());
            if self.recursive.last().copied().unwrap_or(false) {
                self.visible.push(go_to_lower(cte.name.clone()));
            }
        }
    }

    /// Tracks the scope a visitor leaves; call from `Visitor::leave`.
    pub fn leave(&mut self, node: &mut dyn Any) {
        if node.is::<tidb_ast::SelectStmt>() || node.is::<tidb_ast::SetOprStmt>() {
            self.visible
                .truncate(self.offsets.pop().expect("query scope entered"));
        } else if node.is::<tidb_ast::WithClause>() {
            self.recursive.pop();
        } else if let Some(cte) = node.downcast_mut::<tidb_ast::Cte>() {
            self.visible
                .truncate(self.offsets.pop().expect("CTE scope entered"));
            self.visible.push(go_to_lower(cte.name.clone()));
        }
    }

    /// Whether `path` is an unqualified name of a CTE visible here.
    #[must_use]
    pub fn contains(&self, path: &[String]) -> bool {
        matches!(path, [name] if self.visible.contains(&go_to_lower(name.clone())))
    }
}

/// Go `preprocessor.stmtTp`: the kind of the statement node entered last
/// (a nested statement sets it and nothing restores it).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum StmtType {
    #[default]
    Invalid,
    Select,
    Delete,
    Update,
    Insert,
    Show,
}

/// The session facts Go's `preprocessor` reads through its `sctx`.
pub trait PreprocessContext {
    /// Go `SessionVars.CurrentDB`.
    fn current_db(&self) -> &str;
    /// Go `SessionVars.SQLMode.HasStrictMode()`, which decides whether an
    /// over-long VARCHAR is an error (`hasAutoConvertWarning`).
    fn strict_sql_mode(&self) -> bool;
    /// Whether `path` names one of the session's local temporary tables
    /// (Go `checkCreateTableGrammar`'s LIKE source).
    fn is_local_temporary_table(&self, path: &[String]) -> bool;
    /// Go `checkDropTemporaryTableGrammar` for `DROP GLOBAL TEMPORARY TABLE`:
    /// whether `path` names a table that is not a global temporary table.
    ///
    /// # Errors
    ///
    /// When the catalog cannot be read.
    fn is_not_global_temporary_table(&self, path: &[String]) -> Result<bool, PlanError>;
    /// Go `SessionVars.NoopFuncsMode` (`tidb_enable_noop_functions`).
    fn noop_funcs_mode(&self) -> tidb_variable::set_hooks::NoopFuncsMode;
    /// Go `tidb_enable_shared_lock_promotion`: a shared lock is promoted to
    /// `FOR UPDATE` rather than gated.
    fn shared_lock_promotion(&self) -> bool;
    /// Go `SessionVars.User.AuthUsername` / `AuthHostname`, which a SHOW's
    /// `CURRENT_USER` becomes.
    fn auth_user(&self) -> Option<(String, String)>;
    /// Go `preprocessor`'s `inPrepare` flag: a PREPARE (or a binary-protocol
    /// execution) is being preprocessed.
    fn in_prepare(&self) -> bool;
    /// Go `preprocessor.tableByName` for a DDL target: 1046 without a
    /// schema; a missing table is 1146, unless the account holds no
    /// privilege at all on it, which answers 1142 naming `verb`.
    ///
    /// # Errors
    ///
    /// The lookup's refusal.
    fn table_by_name(&self, path: &[String], verb: &'static str) -> Result<(), PlanError>;
    /// Go `checkCreateTableGrammar`'s `TableByName` of a LIKE source: its
    /// `TableInfo` (`None` for a view or sequence, which
    /// `CreateTableWithLike` refuses later).
    ///
    /// # Errors
    ///
    /// 1046 without a schema, 1146 when the source is missing.
    fn like_source_info(
        &self,
        path: &[String],
    ) -> Result<Option<tidb_model::GoShared<tidb_model::TableInfo>>, PlanError>;
}

/// Go `PreprocessorReturn` and what the walk learns for its callers.
#[derive(Debug, Default)]
pub struct PreprocessorReturn {
    /// `(schema, table)` of every table reference and an INSERT's target,
    /// lowercased, in walk order (an unqualified name has an empty schema).
    pub table_names: Vec<(String, String)>,
    /// Whether a table reference reads `AS OF TIMESTAMP`.
    pub has_as_of: bool,
    /// Go `preprocessor.varsReadonly`: the user variables a SELECT reads and
    /// the statement never assigns, lowercased as the session stores them.
    pub readonly_user_vars: std::collections::HashSet<String>,
    /// Go `checkSelectNoopFuncs`' warnings under `tidb_enable_noop_functions
    /// = WARN`, in walk order.
    pub noop_warnings: Vec<PlanError>,
}

/// Go `Preprocess`: one walk of the statement that checks what the planner
/// must not see and collects what the session needs before planning. The
/// first failing node's error stops the walk, as Go's `p.err` does.
///
/// # Errors
///
/// The preprocessor's error for the first node it refuses.
pub fn preprocess(
    stmt: &mut tidb_ast::Stmt,
    ctx: &dyn PreprocessContext,
) -> Result<PreprocessorReturn, PlanError> {
    preprocess_with(stmt, ctx, false)
}

/// Go `Preprocess(..., InPrepare)`: the walk at PREPARE, then
/// `handleTableName`'s pinning of every unqualified table to the database
/// current now, so the retained statement and each EXECUTE after a `USE`
/// agree.
///
/// # Errors
///
/// The walk's error, or `ErrNoDB` for an unqualified table with no current
/// database.
pub fn preprocess_in_prepare(
    stmt: &mut tidb_ast::Stmt,
    ctx: &dyn PreprocessContext,
) -> Result<PreprocessorReturn, PlanError> {
    let result = preprocess_with(stmt, ctx, true)?;
    handle_table_name(stmt, ctx.current_db())
        .map_err(|_| PlanError::from(tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])))?;
    Ok(result)
}

fn preprocess_with(
    stmt: &mut tidb_ast::Stmt,
    ctx: &dyn PreprocessContext,
    in_prepare: bool,
) -> Result<PreprocessorReturn, PlanError> {
    let mut preprocessor = Preprocessor {
        ctx,
        in_prepare,
        stmt_tp: StmtType::Invalid,
        vars_mutable: std::collections::HashSet::new(),
        result: PreprocessorReturn::default(),
        err: None,
    };
    stmt.accept(&mut preprocessor);
    match preprocessor.err {
        Some(err) => Err(err),
        None => Ok(preprocessor.result),
    }
}

/// Go `preprocessor`.
struct Preprocessor<'a> {
    ctx: &'a dyn PreprocessContext,
    /// Go `preprocessor.flag & inPrepare`.
    in_prepare: bool,
    stmt_tp: StmtType,
    /// Go `varsMutable`.
    vars_mutable: std::collections::HashSet<String>,
    result: PreprocessorReturn,
    err: Option<PlanError>,
}

impl Preprocessor<'_> {
    /// Go `handleTableName` / `tableByName` over a DDL's target table (and
    /// `checkCreateTableGrammar`'s LIKE source, resolved without privilege
    /// masking). At PREPARE, ALTER TABLE and CREATE INDEX set
    /// `inCreateOrDropTable`, which skips the resolution; ALTER TABLE does too
    /// for a RENAME or FOREIGN KEY spec (`resolveAlterTableStmt`).
    fn ddl_table_names(&self, ddl: &tidb_ast::DdlStmt) -> Result<(), PlanError> {
        use tidb_ast::{AlterTableAction, DdlStmt};
        let prepare_skips = self.in_prepare || self.ctx.in_prepare();
        match ddl {
            DdlStmt::AlterTable(alter)
                if !prepare_skips
                    && !alter.actions.iter().any(|action| {
                        matches!(
                            action,
                            AlterTableAction::RenameTable { .. }
                                | AlterTableAction::AddForeignKey(_)
                        )
                    }) =>
            {
                self.ctx.table_by_name(&alter.name, "ALTER")
            }
            // `stmtType()`: CREATE INDEX is TypeCreate; DROP INDEX and
            // TRUNCATE fall to its default, "SELECT".
            DdlStmt::CreateIndex(create) if !prepare_skips => {
                self.ctx.table_by_name(&create.table, "CREATE")
            }
            DdlStmt::DropIndex(drop) => self.ctx.table_by_name(&drop.table, "SELECT"),
            DdlStmt::TruncateTable(path) => self.ctx.table_by_name(path, "SELECT"),
            DdlStmt::CreateTable(create) => match &create.like_table {
                Some(like) => match self.ctx.like_source_info(like)? {
                    Some(source) => {
                        check_create_table_like_source(&source.read(), create.temporary)
                            .map_err(PlanError::from)
                    }
                    None => Ok(()),
                },
                None => Ok(()),
            },
            _ => Ok(()),
        }
    }

    /// Go `checkSelectNoopFuncs` (and `checkGroupBy`'s ASC/DESC) over one
    /// SELECT: a clause TiDB parses but only implements as a no-op is an
    /// error under `tidb_enable_noop_functions = OFF` and a warning under
    /// `WARN`.
    fn check_select_noop_funcs(&mut self, select: &tidb_ast::SelectStmt) {
        use tidb_variable::set_hooks::NoopFuncsMode;
        let mode = self.ctx.noop_funcs_mode();
        if mode == NoopFuncsMode::On {
            return;
        }
        let mut clauses = Vec::new();
        if select.calc_found_rows {
            clauses.push("SQL_CALC_FOUND_ROWS");
        }
        // Go rewrites a shared lock to the FOR UPDATE path before the check
        // when shared-lock promotion is on.
        if share_lock(&select.lock) && !self.ctx.shared_lock_promotion() {
            clauses.push("LOCK IN SHARE MODE");
        }
        if select.group_by.iter().any(|item| item.desc.is_some()) {
            clauses.push("GROUP BY expr ASC|DESC");
        }
        for clause in clauses {
            let error = PlanError::from(
                tidb_expr::expression_errors::ERR_FUNCTIONS_NOOP_IMPL
                    .gen_with_stack_by_args(&[clause.into()]),
            );
            if mode == NoopFuncsMode::Off {
                self.err = Some(error);
                return;
            }
            self.result.noop_warnings.push(error);
        }
    }

    fn collect_table_name(&mut self, name: &[String]) {
        let (schema, table) = match name {
            [table] => (String::new(), table.clone()),
            [schema, table] => (schema.clone(), table.clone()),
            _ => return,
        };
        self.result
            .table_names
            .push((go_to_lower(schema), go_to_lower(table)));
    }

    /// Go `handleTableName`'s schema fill for a repair lookup: the lowered
    /// schema (the current one for a bare name, `ErrNoDB` without one) and
    /// table names.
    fn repair_table_name(&self, path: &[String]) -> Result<(String, String), PlanError> {
        let (schema, name) = match path {
            [schema, name] => (schema.as_str(), name.as_str()),
            [name] if !self.ctx.current_db().is_empty() => (self.ctx.current_db(), name.as_str()),
            _ => {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
                ));
            }
        };
        Ok((
            tidb_ast::CiString::new(schema).lowercase().to_owned(),
            tidb_ast::CiString::new(name).lowercase().to_owned(),
        ))
    }

    /// Go `Enter`'s DDL arms: `checkRepairTableGrammar` and `handleRepairName`
    /// for an `ADMIN REPAIR TABLE`; the grammar checks over the statement's
    /// own names and declarations, before anything reads the catalog (its
    /// `handleTableName` has already given an unqualified CREATE TABLE the
    /// current schema, which `checkContainDotColumn` compares against); in
    /// REPAIR MODE, no created, dropped or renamed table is under repair;
    /// `checkCreateTableGrammar`'s refusal of a local temporary LIKE source;
    /// `checkDropTemporaryTableGrammar`.
    fn check_ddl(&self, ddl: &tidb_ast::DdlStmt) -> Result<(), PlanError> {
        let repair_info = &*tidb_domainutil::REPAIR_INFO;
        if let DdlStmt::RepairTable(repair) = ddl {
            check_repair_table_grammar(
                repair,
                repair_info.in_repair_mode(),
                repair_info.get_repair_table_list().is_empty(),
            )?;
            let (schema, name) = self.repair_table_name(&repair.table)?;
            handle_repair_name(
                &schema,
                &name,
                repair_info.get_repaired_table_info_by_table_name(&schema, &name),
            )?;
        }
        let qualified;
        let graded = match ddl {
            DdlStmt::CreateTable(create)
                if create.name.len() == 1 && !self.ctx.current_db().is_empty() =>
            {
                let mut create = create.clone();
                create.name.insert(0, self.ctx.current_db().to_owned());
                qualified = Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::CreateTable(create)));
                &qualified
            }
            _ => {
                qualified = Stmt::Ddl(tidb_ast::NodeBox::new(ddl.clone()));
                &qualified
            }
        };
        check_ddl_grammar(graded, self.ctx.strict_sql_mode())?;
        if repair_info.in_repair_mode() {
            for path in create_or_drop_table_names(ddl) {
                let (schema, name) = self.repair_table_name(path)?;
                let (table, _) = repair_info.get_repaired_table_info_by_table_name(&schema, &name);
                check_not_in_repair(&name, table.is_some())?;
            }
        }
        if let DdlStmt::CreateTable(create) = ddl {
            if create
                .like_table
                .as_ref()
                .is_some_and(|name| self.ctx.is_local_temporary_table(name))
            {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                        .gen_with_stack_by_args(&["create table like".into()]),
                ));
            }
        }
        if let DdlStmt::DropTable(drop) = ddl {
            if drop.temporary == tidb_ast::DropTemporary::Global {
                for path in &drop.names {
                    if self.ctx.is_not_global_temporary_table(path)? {
                        return Err(PlanError::from(
                            tidb_plannererrors::ERR_DROP_TABLE_ON_TEMPORARY_TABLE
                                .gen_with_stack_by_args(&[]),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Go `checkCommitTSWrite`: the explicit write targets of INSERT and UPDATE
/// may not name `_tidb_commit_ts`.
fn check_commit_ts_write(columns: &[&str], assignments: &[tidb_ast::Assignment]) -> Result<(), PlanError> {
    let is_commit_ts = |name: &str| go_to_lower(name.to_owned()) == crate::plan_builder::EXTRA_COMMIT_TS_NAME;
    if columns.iter().any(|column| is_commit_ts(column))
        || assignments.iter().any(|assign| assign.col.last().is_some_and(|name| is_commit_ts(name)))
    {
        return Err(PlanError::from(tidb_plannererrors::ERR_INTERNAL.gen_with_stack(format!(
            "Usage of column name '{}' is not supported for now",
            crate::plan_builder::EXTRA_COMMIT_TS_NAME
        ))));
    }
    Ok(())
}

impl Visitor for Preprocessor<'_> {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        if let Some(select) = node.downcast_ref::<tidb_ast::SelectStmt>() {
            self.stmt_tp = StmtType::Select;
            self.check_select_noop_funcs(select);
        } else if node.is::<tidb_ast::DeleteStmt>() {
            self.stmt_tp = StmtType::Delete;
        } else if let Some(update) = node.downcast_ref::<tidb_ast::UpdateStmt>() {
            self.stmt_tp = StmtType::Update;
            if let Err(error) = check_commit_ts_write(&[], &update.assignments) {
                self.err = Some(error);
            }
        } else if let Some(insert) = node.downcast_ref::<tidb_ast::InsertStmt>() {
            self.stmt_tp = StmtType::Insert;
            // Go `node.Columns`: the column list, or a SET form's targets.
            let columns: Vec<&str> = insert
                .columns
                .iter()
                .map(String::as_str)
                .chain(insert.set_columns.iter().filter_map(|path| path.last().map(String::as_str)))
                .collect();
            if let Err(error) = check_commit_ts_write(&columns, &insert.on_duplicate) {
                self.err = Some(error);
            }
        } else if let Some(show) = node.downcast_mut::<tidb_ast::ShowStmt>() {
            self.stmt_tp = StmtType::Show;
            // Go `resolveShowStmt`.
            let auth_user = self.ctx.auth_user();
            let auth_user = auth_user
                .as_ref()
                .map(|(user, host)| (user.as_str(), host.as_str()));
            if let Err(error) = resolve_show_stmt(show, self.ctx.current_db(), auth_user) {
                self.err = Some(error);
            }
        } else if let Some(ddl) = node.downcast_mut::<tidb_ast::DdlStmt>() {
            if let Err(error) = self.check_ddl(ddl) {
                self.err = Some(error);
            } else if let tidb_ast::DdlStmt::CreateView(create) = ddl {
                // Go `checkCreateViewWithSelectGrammar`, before the walk
                // reaches the body's SELECTs.
                if let Err(error) = check_create_view_with_select(&mut create.query) {
                    self.err = Some(error);
                }
            }
            if self.err.is_none() {
                if let Err(error) = self.ddl_table_names(ddl) {
                    self.err = Some(error);
                }
            }
        } else if let Some(table_ref) = node.downcast_mut::<tidb_ast::TableRef>() {
            if table_ref.as_of.is_some() {
                self.result.has_as_of = true;
            }
            let name = table_ref.name.clone();
            self.collect_table_name(&name);
        } else if let Some(expr) = node.downcast_ref::<tidb_ast::Expr>() {
            match expr {
                // Go `*ast.VariableExpr`.
                tidb_ast::Expr::Assign { name, .. } => {
                    let name = go_to_lower(name.clone());
                    self.result.readonly_user_vars.remove(&name);
                    self.vars_mutable.insert(name);
                }
                tidb_ast::Expr::UserVar(name) if self.stmt_tp == StmtType::Select => {
                    let name = go_to_lower(name.clone());
                    if !self.vars_mutable.contains(&name) {
                        self.result.readonly_user_vars.insert(name);
                    }
                }
                // Go `*ast.FuncCastExpr` (`JSON_SUM_CRC32` is an
                // `*ast.JSONSumCrc32Expr` there).
                tidb_ast::Expr::Cast(cast) if cast.style != tidb_ast::CastStyle::JsonSumCrc32 => {
                    if let Err(error) = check_func_cast_expr(cast) {
                        self.err = Some(PlanError::eval(error));
                    }
                }
                // Go `*ast.FuncCallExpr`: stored procedures are not supported.
                tidb_ast::Expr::GenericFuncCall { schema, name, .. } => {
                    self.err = Some(PlanError::from(
                        tidb_expr::expression_errors::ERR_FUNCTION_NOT_EXISTS.gen_with_stack(
                            format!(
                                "FUNCTION {}.{} does not exist",
                                tidb_mysql::to_lowercase(schema),
                                tidb_mysql::to_lowercase(name)
                            ),
                        ),
                    ));
                }
                _ => {}
            }
        }
        self.err.is_some()
    }

    fn leave(&mut self, node: &mut dyn Any) -> bool {
        if let Some(insert) = node.downcast_mut::<tidb_ast::InsertStmt>() {
            // The INSERT target, which Go's `Enter` resolves before the rest.
            let table = insert.table.clone();
            self.collect_table_name(&table);
        } else if let Some(tidb_ast::Expr::Func { name, args, .. }) =
            node.downcast_ref::<tidb_ast::Expr>()
        {
            // Go `Leave`'s `*ast.FuncCallExpr` arm: NAME_CONST's arguments are
            // constants.
            if go_to_lower(name.clone()) == "name_const" {
                if let Err(error) = check_name_const_args(args) {
                    self.err = Some(PlanError::eval(error));
                }
            }
        }
        self.err.is_none()
    }
}

/// The table names Go's preprocessor visits under `inCreateOrDropTable`: a
/// created, dropped or renamed table, view or sequence, and a CREATE TABLE
/// LIKE source.
fn create_or_drop_table_names(ddl: &DdlStmt) -> Vec<&[String]> {
    match ddl {
        DdlStmt::CreateTable(create) => std::iter::once(create.name.as_slice())
            .chain(create.like_table.as_deref())
            .collect(),
        DdlStmt::CreateView(create) => vec![create.name.as_slice()],
        DdlStmt::CreateSequence(create) => vec![create.name.as_slice()],
        DdlStmt::DropTable(drop) => drop.names.iter().map(Vec::as_slice).collect(),
        DdlStmt::DropSequence(drop) => drop.names.iter().map(Vec::as_slice).collect(),
        DdlStmt::RenameTable(rename) => rename
            .pairs
            .iter()
            .flat_map(|(old, new)| [old.as_slice(), new.as_slice()])
            .collect(),
        _ => Vec::new(),
    }
}

/// Go `preprocessor.checkFuncCastExpr`: a DECIMAL target's M and D and a
/// DATETIME target's fsp are in range. (A TIME target's fsp is the
/// rewriter's `checkTimePrecision`.)
fn check_func_cast_expr(cast: &tidb_ast::CastExpr) -> Result<(), tidb_exprctx::EvalError> {
    use tidb_ast::CastType;

    let invalid = |code, message| tidb_exprctx::EvalError::InvalidTypeDeclaration { code, message };
    match cast.cast_type {
        CastType::Decimal { flen, scale } if flen < scale => Err(invalid(
            1427,
            format!(
                "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '{}').",
                cast.expr.restore()
            ),
        )),
        CastType::Decimal { flen, .. } if flen > 65 => Err(invalid(
            1426,
            format!(
                "Too-big precision {flen} specified for '{}'. Maximum is 65.",
                cast.expr.restore()
            ),
        )),
        CastType::Decimal { scale, .. } if scale > 30 => Err(invalid(
            1425,
            format!(
                "Too big scale {scale} specified for column '{}'. Maximum is 30.",
                cast.expr.restore()
            ),
        )),
        CastType::DateTime { fsp: Some(fsp) } if fsp > 6 => Err(invalid(
            1426,
            format!("Too-big precision {fsp} specified for 'CAST'. Maximum is 6."),
        )),
        _ => Ok(()),
    }
}

/// Go `preprocessor.Leave`'s NAME_CONST check: the name is a literal and the
/// value a literal or a unary expression; a wrong argument count is 1582.
fn check_name_const_args(args: &[tidb_ast::Expr]) -> Result<(), tidb_exprctx::EvalError> {
    use tidb_ast::Expr;

    if args.len() != 2 {
        return Err(tidb_exprctx::EvalError::WrongParameterCount("name_const"));
    }
    let literal = |expr: &Expr| {
        matches!(
            expr,
            Expr::Null
                | Expr::Int(_)
                | Expr::Decimal(_)
                | Expr::Float(_)
                | Expr::Hex(_)
                | Expr::Bit(_)
                | Expr::String(_)
                | Expr::RawString(_)
                | Expr::CharsetString { .. }
                | Expr::CharsetBinary { .. }
                | Expr::Bool(_)
        )
    };
    if literal(&args[0]) && (literal(&args[1]) || matches!(args[1], Expr::Unary(_, _))) {
        Ok(())
    } else {
        Err(tidb_exprctx::EvalError::IncorrectArguments(
            "Incorrect arguments to NAME_CONST".to_owned(),
        ))
    }
}

/// Go `preprocessor.Leave`'s `*driver.ParamMarkerExpr` case: a parameter
/// marker in a statement that is not being prepared is a syntax error.
pub fn check_param_marker<N: Visitable + Clone>(node: &N) -> Result<(), tidb_terror::TerrorError> {
    struct Finder {
        found: bool,
    }
    impl Visitor for Finder {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            if matches!(
                node.downcast_ref::<tidb_ast::Expr>(),
                Some(tidb_ast::Expr::ParamMarker { .. })
            ) {
                self.found = true;
            }
            self.found
        }
        fn leave(&mut self, _node: &mut dyn Any) -> bool {
            !self.found
        }
    }
    let mut node = node.clone();
    let mut finder = Finder { found: false };
    node.accept(&mut finder);
    if finder.found {
        return Err(
            tidb_parser::parser_errors::ERR_SYNTAX.gen_with_stack("syntax error, unexpected '?'")
        );
    }
    Ok(())
}

/// Go `preprocessor.handleTableName` over a statement PREPARE retains
/// ([`handle_table_name`]): an unqualified name is pinned to `current_db`.
///
/// # Errors
///
/// 1046 when a name needs a database and none is selected.
pub fn pin_current_database(
    stmt: &mut tidb_ast::Stmt,
    current_db: &str,
) -> Result<(), tidb_driver_error::DriverError> {
    handle_table_name(stmt, current_db).map_err(|_| {
        tidb_driver_error::DriverError::from(
            tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
        )
    })
}

/// Physical read sources in traversal order. Qualified names always name a
/// table, even when a visible CTE has the same name. Binding matching keeps its
/// separate, deliberately unfiltered syntactic table-name collection.
#[must_use]
pub fn collect_physical_table_refs(stmt: &tidb_ast::Stmt) -> Vec<Vec<String>> {
    #[derive(Default)]
    struct Collector {
        refs: Vec<Vec<String>>,
        scope: CteScope,
    }
    impl Visitor for Collector {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            self.scope.enter(node);
            if let Some(table_ref) = node.downcast_mut::<tidb_ast::TableRef>() {
                if !self.scope.contains(&table_ref.name) {
                    self.refs.push(table_ref.name.clone());
                }
            }
            false
        }

        fn leave(&mut self, node: &mut dyn Any) -> bool {
            self.scope.leave(node);
            true
        }
    }
    let mut stmt = stmt.clone();
    let mut collector = Collector::default();
    stmt.accept(&mut collector);
    collector.refs
}

/// Go `preprocessor.handleTableName` (`pkg/planner/core/preprocess.go:1758`):
/// every unqualified table name that does not name a CTE takes the current
/// database. PREPARE runs it on the statement it retains, so the stored
/// `VisitInfos` and every later EXECUTE -- after any `USE` -- resolve against
/// the database current at PREPARE (`PlanCacheStmt.StmtDB`); an unqualified
/// name with no current database is Go's `ErrNoDB`.
///
/// # Errors
///
/// `ErrNoDB` when an unqualified name has no current database to take.
pub fn handle_table_name<N: Visitable>(node: &mut N, current_db: &str) -> Result<(), PlanError> {
    struct Pinner<'a> {
        current_db: &'a str,
        scope: CteScope,
        unresolved: bool,
    }
    impl Pinner<'_> {
        fn pin(&mut self, path: &mut Vec<String>) {
            if path.len() != 1 {
                return;
            }
            if self.current_db.is_empty() {
                self.unresolved = true;
                return;
            }
            path.insert(0, self.current_db.to_owned());
        }
    }
    impl Visitor for Pinner<'_> {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            self.scope.enter(node);
            if let Some(table_ref) = node.downcast_mut::<tidb_ast::TableRef>() {
                if !self.scope.contains(&table_ref.name) {
                    self.pin(&mut table_ref.name);
                }
            } else if let Some(insert) = node.downcast_mut::<tidb_ast::InsertStmt>() {
                self.pin(&mut insert.table);
            } else if let Some(import) = node.downcast_mut::<tidb_ast::ImportIntoStmt>() {
                self.pin(&mut import.table);
            } else if let Some(distribute) = node.downcast_mut::<tidb_ast::DistributeTableStmt>() {
                self.pin(&mut distribute.table);
            } else if let Some(create) = node.downcast_mut::<tidb_ast::CreateTableStmt>() {
                self.pin(&mut create.name);
            } else if let Some(create) = node.downcast_mut::<tidb_ast::CreateViewStmt>() {
                self.pin(&mut create.name);
            } else if let Some(create) = node.downcast_mut::<tidb_ast::CreateSequenceStmt>() {
                self.pin(&mut create.name);
            } else if let Some(drop) = node.downcast_mut::<tidb_ast::DropTableStmt>() {
                for name in &mut drop.names {
                    self.pin(name);
                }
            } else if let Some(alter) = node.downcast_mut::<tidb_ast::AlterTableStmt>() {
                self.pin(&mut alter.name);
            } else if let Some(create) = node.downcast_mut::<tidb_ast::CreateIndexStmt>() {
                self.pin(&mut create.table);
            } else if let Some(drop) = node.downcast_mut::<tidb_ast::DropIndexStmt>() {
                self.pin(&mut drop.table);
            } else if let Some(split) = node.downcast_mut::<tidb_ast::SplitRegionStmt>() {
                self.pin(&mut split.table);
            }
            false
        }

        fn leave(&mut self, node: &mut dyn Any) -> bool {
            self.scope.leave(node);
            true
        }
    }
    let mut pinner = Pinner {
        current_db,
        scope: CteScope::default(),
        unresolved: false,
    };
    node.accept(&mut pinner);
    if pinner.unresolved {
        return Err(PlanError::from(
            tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
        ));
    }
    Ok(())
}

/// The table names Go's preprocessor leaves in a statement's
/// `resolve.Context` (`GetTableNames`), as written: `handleTableName`
/// (`pkg/planner/core/preprocess.go:1757`) records every `*ast.TableName` it
/// leaves except a CTE's, and records none while `inCreateOrDropTable` or
/// `inRepairTable` is set -- CREATE TABLE / VIEW / SEQUENCE, DROP TABLE /
/// VIEW / SEQUENCE, RENAME TABLE, an ALTER TABLE that renames the table or
/// adds a foreign key (`resolveAlterTableStmt`), a BRIE statement and ADMIN
/// REPAIR TABLE. FLASHBACK TABLE, RECOVER TABLE and OPTIMIZE TABLE hold
/// names Go's `Accept` never visits.
#[must_use]
pub fn resolve_ctx_table_names(stmt: &Stmt) -> Vec<Vec<String>> {
    use tidb_ast::{AdminCheckStmt, AdminStmt, AlterTableAction, DdlStmt, DmlStmt};
    struct Names {
        scope: CteScope,
        names: Vec<Vec<String>>,
    }
    impl Visitor for Names {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            self.scope.enter(node);
            if let Some(table_ref) = node.downcast_mut::<tidb_ast::TableRef>() {
                if !self.scope.contains(&table_ref.name) {
                    self.names.push(table_ref.name.clone());
                }
            } else if let Some(insert) = node.downcast_mut::<tidb_ast::InsertStmt>() {
                self.names.push(insert.table.clone());
            }
            false
        }

        fn leave(&mut self, node: &mut dyn Any) -> bool {
            self.scope.leave(node);
            true
        }
    }
    let mut names = Vec::new();
    match stmt {
        Stmt::Ddl(ddl) => match &**ddl {
            DdlStmt::CreateTable(_)
            | DdlStmt::CreateView(_)
            | DdlStmt::AlterView(_)
            | DdlStmt::DropTable(_)
            | DdlStmt::DropView { .. }
            | DdlStmt::RenameTable(_)
            | DdlStmt::CreateSequence(_)
            | DdlStmt::DropSequence(_)
            | DdlStmt::RepairTable(_)
            // Go master: `inCreateOrDropTable` for these materialized view
            // statements.
            | DdlStmt::CreateMaterializedView(_)
            | DdlStmt::DropMaterializedView(_)
            | DdlStmt::AlterMaterializedView(_) => return names,
            DdlStmt::AlterTable(alter) => {
                if alter.actions.iter().any(|action| {
                    matches!(
                        action,
                        AlterTableAction::RenameTable { .. } | AlterTableAction::AddForeignKey(_)
                    )
                }) {
                    return names;
                }
                names.push(alter.name.clone());
            }
            DdlStmt::CreateIndex(create) => names.push(create.table.clone()),
            DdlStmt::DropIndex(drop) => names.push(drop.table.clone()),
            DdlStmt::TruncateTable(table) => names.push((**table).clone()),
            DdlStmt::LockTables(locks) => names.extend(locks.iter().map(|lock| lock.table.clone())),
            DdlStmt::AlterSequence(alter) => names.push(alter.name.clone()),
            DdlStmt::CleanupTableLock(cleanup) => names.extend(cleanup.tables.iter().cloned()),
            DdlStmt::CreateMaterializedViewLog(create) => names.push(create.table.clone()),
            DdlStmt::DropMaterializedViewLog(drop) => names.push(drop.table.clone()),
            DdlStmt::AlterMaterializedViewLog(alter) => names.push(alter.table.clone()),
            _ => {}
        },
        Stmt::Admin(admin) => match &**admin {
            AdminStmt::Brie(_) | AdminStmt::CompareMaterializedView(_) => return names,
            AdminStmt::RefreshMaterializedView(refresh)
                if refresh.mode == tidb_ast::RefreshMaterializedViewMode::CompleteOutOfPlace =>
            {
                return names;
            }
            AdminStmt::RefreshMaterializedView(refresh) => names.push(refresh.view_name.clone()),
            AdminStmt::PurgeMaterializedViewLog(purge) => names.push(purge.table.clone()),
            AdminStmt::AnalyzeTable(analyze) => names.extend(analyze.tables.iter().cloned()),
            AdminStmt::AdminCheck(check) => match &**check {
                AdminCheckStmt::Table { tables } => names.extend(tables.iter().cloned()),
                AdminCheckStmt::Index { table, .. } => names.push(table.clone()),
            },
            AdminStmt::AdminChecksum(checksum) => names.extend(checksum.tables.iter().cloned()),
            AdminStmt::AdminRecoverIndex(index) | AdminStmt::AdminCleanupIndex(index) => {
                names.push(index.table.clone());
            }
            AdminStmt::ShowNextRowId(show) => names.push(show.table.clone()),
            AdminStmt::DropStats(drop) => names.extend(drop.tables.iter().cloned()),
            AdminStmt::LockStats(lock) | AdminStmt::UnlockStats(lock) => {
                names.extend(lock.tables.iter().map(|table| table.name.clone()));
            }
            AdminStmt::CompactTable(compact) => names.push(compact.table.clone()),
            AdminStmt::SplitRegion(split) => names.push(split.table.clone()),
            AdminStmt::Show(show) => names.extend(show.table.clone()),
            _ => {}
        },
        Stmt::Dml(dml) => match &**dml {
            DmlStmt::ImportInto(import) => names.push(import.table.clone()),
            DmlStmt::LoadData(load) => names.push(load.table.clone()),
            DmlStmt::DistributeTable(distribute) => names.push(distribute.table.clone()),
            _ => {}
        },
        Stmt::Query(_) | Stmt::Session(_) => {}
    }
    let mut collector = Names {
        scope: CteScope::default(),
        names,
    };
    stmt.clone().accept(&mut collector);
    collector.names
}


/// Go `preprocessor.resolveShowStmt` (`pkg/planner/core/preprocess.go:2028`)
/// and the `handleTableName` the preprocessor then runs on `Table`: a SHOW
/// without a database takes its table's schema, else the current database;
/// one with a database qualifies an unqualified table with it;
/// `CURRENT_USER` becomes the session's authenticated account
/// (`auth_user`), when the session has one; and a table still unqualified
/// takes the current database.
///
/// # Errors
///
/// `ErrNoDB` for an unqualified table with no current database.
pub fn resolve_show_stmt(
    node: &mut tidb_ast::ShowStmt,
    current_db: &str,
    auth_user: Option<(&str, &str)>,
) -> Result<(), PlanError> {
    if node.db_name.is_empty() {
        node.db_name = match node.table.as_deref() {
            Some([schema, _]) if !schema.is_empty() => schema.clone(),
            _ => current_db.to_owned(),
        };
    } else if let Some(table) = node.table.as_mut() {
        if let [name] = table.as_slice() {
            *table = vec![node.db_name.clone(), name.clone()];
        }
    }
    if let (Some(user), Some((auth_username, auth_hostname))) = (node.user.as_mut(), auth_user) {
        if user.current_user {
            user.user = auth_username.to_owned();
            user.host = auth_hostname.to_owned();
        }
    }
    if let Some(table) = node.table.as_mut() {
        if let [name] = table.as_slice() {
            if current_db.is_empty() {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
                ));
            }
            *table = vec![current_db.to_owned(), name.clone()];
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// DDL grammar checks.
//
// Go `planner/core/preprocess.go`'s grammar checks over DDL statements.
//
// Go's preprocessor walks every statement before it is planned, privilege
// checked or run, and refuses the DDL whose names or declarations are wrong
// on their face: an empty or space-terminated identifier, a second primary
// key, an index named `PRIMARY`, a zero key length, a column type out of
// range. Its order is the walk's: the statement's own checks on entry, the
// constraint nodes beneath it, then the checks Go runs on leaving a CREATE
// TABLE (`checkAutoIncrement`, `checkContainDotColumn`). The first error
// stops the walk. The checks that need the infoschema (a LIKE source, a
// DROP TEMPORARY target) stay with the session, which owns the catalog.

use tidb_ast::{
    AlterPartitionAction, AlterTableAction, ColumnDef, ColumnOption, ColumnTypeArg,
    CreateTableStmt, CreateTableTemporary, DdlStmt, IndexConstraintKind, IndexPart, Stmt,
    TableConstraint, TableOption,
};
use tidb_datatype::FieldTypeCode;

use tidb_hack::GoCaseCompare;
use tidb_hack::GoToLower;
use tidb_terror::MysqlError;

/// `mysql.MaxKeyParts`.
const MAX_KEY_PARTS: usize = 16;
/// `mysql.MaxFieldCharLength`.
const MAX_FIELD_CHAR_LENGTH: i64 = 255;
/// `mysql.MaxFieldVarCharLength`.
const MAX_FIELD_VARCHAR_LENGTH: i64 = 65535;
/// `mysql.MaxDoublePrecisionLength`.
const MAX_DOUBLE_PRECISION_LENGTH: i64 = 53;
/// `mysql.MaxFloatingTypeScale`.
const MAX_FLOATING_TYPE_SCALE: i64 = 30;
/// `mysql.MaxFloatingTypeWidth`.
const MAX_FLOATING_TYPE_WIDTH: i64 = 255;
/// `mysql.MaxDecimalScale`.
const MAX_DECIMAL_SCALE: i64 = 30;
/// `mysql.MaxDecimalWidth`.
const MAX_DECIMAL_WIDTH: i64 = 65;
/// `mysql.MaxBitDisplayWidth`.
const MAX_BIT_DISPLAY_WIDTH: i64 = 64;
/// `mysql.MaxTypeSetMembers`.
const MAX_TYPE_SET_MEMBERS: usize = 64;
/// `auth.UserNameMaxLength`.
const USER_NAME_MAX_LENGTH: usize = 32;
/// `auth.HostNameMaxLength`.
const HOST_NAME_MAX_LENGTH: usize = 255;

/// Go's preprocessor grammar checks over one DDL statement. `strict` is the
/// session's strict SQL mode, which decides whether an over-long VARCHAR is
/// an error or is left to be converted to TEXT/BLOB (`hasAutoConvertWarning`).
pub fn check_ddl_grammar(stmt: &Stmt, strict: bool) -> Result<(), MysqlError> {
    let Stmt::Ddl(ddl) = stmt else {
        return Ok(());
    };
    match ddl.as_ref() {
        DdlStmt::CreateDatabase { name, .. } | DdlStmt::DropDatabase { name, .. } => {
            check_database_name(name)
        }
        // `ALTER DATABASE` with no name alters the current database.
        DdlStmt::AlterDatabase { name, .. } => name.as_deref().map_or(Ok(()), check_database_name),
        DdlStmt::CreateTable(create) => check_create_table(create, strict),
        DdlStmt::CreateView(create) => {
            check_table_name(path_name(&create.name))?;
            for column in &create.columns {
                if is_incorrect_identifier_name(column) {
                    return Err(MysqlError::from_terror(
                        &tidb_dbterror::ERR_WRONG_COLUMN_NAME
                            .gen_with_stack_by_args(&[(column.clone()).to_string().into()]),
                    ));
                }
            }
            if !create.definer.current_user {
                check_definer_length(&create.definer.user, "user name", USER_NAME_MAX_LENGTH)?;
                check_definer_length(&create.definer.host, "host name", HOST_NAME_MAX_LENGTH)?;
            }
            Ok(())
        }
        DdlStmt::DropTable(drop) => {
            for path in &drop.names {
                check_table_name(path_name(path))?;
            }
            Ok(())
        }
        DdlStmt::DropSequence(drop) => {
            for path in &drop.names {
                check_table_name(path_name(path))?;
            }
            Ok(())
        }
        // Go checks the first pair only (`checkRenameTableGrammar`).
        DdlStmt::RenameTable(rename) => match rename.pairs.first() {
            Some((from, to)) => {
                check_table_name(path_name(from))?;
                check_table_name(path_name(to))
            }
            None => Ok(()),
        },
        DdlStmt::CreateIndex(create) => {
            check_table_name(path_name(&create.table))?;
            if create.name.is_empty() {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_WRONG_NAME_FOR_INDEX
                        .gen_with_stack_by_args(&[(&create.name).into()]),
                ));
            }
            check_index_info(&create.name, &create.parts)
        }
        DdlStmt::AlterTable(alter) => check_alter_table(alter),
        _ => Ok(()),
    }
}

/// Go `preprocessor.checkRepairTableGrammar` (`preprocess.go:1407`), which
/// the walk runs on entering an `ADMIN REPAIR TABLE`, before its CREATE
/// TABLE and before the planner's SUPER check: refused outside REPAIR MODE
/// and with an empty repair list (`domainutil.RepairInfo`, read by the
/// caller), then both names checked as a RENAME checks them.
pub fn check_repair_table_grammar(
    stmt: &tidb_ast::RepairTableStmt,
    in_repair_mode: bool,
    repair_table_list_empty: bool,
) -> Result<(), MysqlError> {
    let repair_table_fail = |reason: &str| {
        MysqlError::from_terror(
            &tidb_dbterror::ERR_REPAIR_TABLE_FAIL.gen_with_stack_by_args(&[reason.into()]),
        )
    };
    if !in_repair_mode {
        return Err(repair_table_fail("TiDB is not in REPAIR MODE"));
    }
    if repair_table_list_empty {
        return Err(repair_table_fail("repair list is empty"));
    }
    check_table_name(path_name(&stmt.table))?;
    check_table_name(path_name(&stmt.create.name))
}

/// Go `preprocessor.handleRepairName`: the table an `ADMIN REPAIR TABLE`
/// names must be one `RepairInfo` holds (`repaired` is its
/// `GetRepairedTableInfoByTableName` answer), outside the memory and system
/// databases. Answers Go's `RepairedTable` and `RepairedDatabase` values.
pub fn handle_repair_name<T, D>(
    schema_lower_name: &str,
    table_lower_name: &str,
    repaired: (Option<T>, Option<D>),
) -> Result<(T, D), MysqlError> {
    let repair_table_fail = |reason: String| {
        MysqlError::from_terror(
            &tidb_dbterror::ERR_REPAIR_TABLE_FAIL.gen_with_stack_by_args(&[reason.into()]),
        )
    };
    if tidb_metadef::is_mem_or_sys_db(schema_lower_name) {
        return Err(repair_table_fail(
            "memory or system database is not for repair".to_owned(),
        ));
    }
    let (table_info, db_info) = repaired;
    let Some(db_info) = db_info else {
        return Err(repair_table_fail(format!(
            "database {schema_lower_name} is not in repair"
        )));
    };
    let Some(table_info) = table_info else {
        return Err(repair_table_fail(format!(
            "table {table_lower_name} is not in repair"
        )));
    };
    Ok((table_info, db_info))
}

/// Go `preprocessor.checkNotInRepair`: a create, drop or rename may not name
/// a table `RepairInfo` holds (`in_repair` is whether
/// `GetRepairedTableInfoByTableName` answered a table).
pub fn check_not_in_repair(table_lower_name: &str, in_repair: bool) -> Result<(), MysqlError> {
    if in_repair {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_WRONG_TABLE_NAME.gen_with_stack_by_args(&[
                table_lower_name.into(),
                "this table is in repair".into(),
            ]),
        ));
    }
    Ok(())
}

/// Go `util.IsInCorrectIdentifierName`: an empty name, or one ending in a
/// space.
fn is_incorrect_identifier_name(name: &str) -> bool {
    name.is_empty() || name.ends_with(' ')
}

/// The last segment of a written object path: the object's own name.
fn path_name(path: &[String]) -> &str {
    path.last().map_or("", String::as_str)
}

fn check_database_name(name: &str) -> Result<(), MysqlError> {
    if is_incorrect_identifier_name(name) {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_WRONG_DB_NAME.gen_with_stack_by_args(&[name.into()]),
        ));
    }
    Ok(())
}

fn check_table_name(name: &str) -> Result<(), MysqlError> {
    if is_incorrect_identifier_name(name) {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_WRONG_TABLE_NAME.gen_with_stack_by_args(&[name.to_owned().into()]),
        ));
    }
    Ok(())
}

/// `dbterror.ErrWrongStringLength` (1470): `String '%-.70s' is too long for
/// %s (should be no longer than %d)`. The `%-.70s` keeps the first 70 runes of
/// the offending value, so a 255-character host prints as its first 70.
#[must_use]
pub fn wrong_string_length_error(value: &str, kind: &str, maximum: usize) -> MysqlError {
    let shown: String = value.chars().take(70).collect();
    MysqlError::from_terror(
        &tidb_dbterror::ERR_WRONG_STRING_LENGTH.gen_with_stack_by_args(&[
            shown.into(),
            kind.into(),
            maximum.into(),
        ]),
    )
}

/// `dbterror.ErrWrongStringLength` (1470) over a view's DEFINER.
fn check_definer_length(value: &str, kind: &str, maximum: usize) -> Result<(), MysqlError> {
    if value.len() > maximum {
        return Err(wrong_string_length_error(value, kind, maximum));
    }
    Ok(())
}

/// Go `checkCreateTableGrammar`, then the CREATE TABLE's constraint nodes,
/// then `checkAutoIncrement` and `checkContainDotColumn` on leaving it.
fn check_create_table(create: &CreateTableStmt, strict: bool) -> Result<(), MysqlError> {
    let temporary = create.temporary != CreateTableTemporary::None;
    if temporary {
        for option in &create.table_options {
            match option {
                TableOption::ShardRowIdBits(_) => {
                    return Err(MysqlError::from_terror(
                        &tidb_plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                            .gen_with_stack_by_args(&["shard_row_id_bits".into()]),
                    ));
                }
                TableOption::PlacementPolicy(_) => {
                    return Err(MysqlError::from_terror(
                        &tidb_plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                            .gen_with_stack_by_args(&["PLACEMENT".into()]),
                    ));
                }
                _ => {}
            }
        }
    }
    check_table_name(path_name(&create.name))?;
    let mut primary_keys = 0;
    for column in &create.columns {
        if let Err(error) = check_column(column) {
            // Go issue #30328: outside strict mode an over-long VARCHAR is
            // converted to TEXT/BLOB with a warning instead
            // (`hasAutoConvertWarning`).
            let converts = !strict
                && matches!(error, ColumnCheckError::TooBigFieldLength)
                && is_varchar(column);
            if !converts {
                return Err(error.into_driver_error(column));
            }
        }
        primary_keys += check_column_options(temporary, &column.options)?;
        if primary_keys > 1 {
            return Err(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_MULTIPLE_PRI_KEY
                    .gen_with_stack_by_args(&[]),
            ));
        }
    }
    for constraint in &create.table_constraints {
        match constraint {
            TableConstraint::Index(index) => match index.kind {
                IndexConstraintKind::PrimaryKey => {
                    if primary_keys > 0 {
                        return Err(MysqlError::from_terror(
                            &tidb_infoschema::infoschema_errors::ERR_MULTIPLE_PRI_KEY
                                .gen_with_stack_by_args(&[]),
                        ));
                    }
                    primary_keys += 1;
                    check_index_info(index.name.as_deref().unwrap_or(""), &index.parts)?;
                }
                IndexConstraintKind::Key
                | IndexConstraintKind::Index
                | IndexConstraintKind::Unique
                | IndexConstraintKind::UniqueKey
                | IndexConstraintKind::UniqueIndex => {
                    let name = index.name.as_deref().unwrap_or("");
                    check_index_info(name, &index.parts)?;
                    if index.is_empty_index {
                        return Err(MysqlError::from_terror(
                            &tidb_dbterror::ERR_WRONG_NAME_FOR_INDEX
                                .gen_with_stack_by_args(&[name.into()]),
                        ));
                    }
                }
                _ => {}
            },
            TableConstraint::ForeignKey(foreign_key) => {
                let name = foreign_key.name.as_deref().unwrap_or("");
                check_index_info(name, &foreign_key.parts)?;
            }
            _ => {}
        }
    }
    check_unsupported_table_options(&create.table_options)?;
    if create.ctas.is_some() {
        return Err(MysqlError::unknown(
            "'CREATE TABLE ... SELECT' is not implemented yet",
        ));
    }
    if create.columns.is_empty() && create.like_table.is_none() {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_TABLE_MUST_HAVE_COLUMNS.gen_with_stack_by_args(&[]),
        ));
    }
    if let Some(partitioning) = &create.partitioning {
        if partitioning
            .definitions
            .iter()
            .any(|definition| is_incorrect_identifier_name(&definition.name))
        {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_WRONG_PARTITION_NAME.gen_with_stack_by_args(&[]),
            ));
        }
    }
    check_auto_increment(create)?;
    check_contain_dot_column(create)
}

/// Go `checkUnsupportedTableOptions`: reject the two unsupported compatibility
/// clauses (`UNION`, `INSERT_METHOD`) and storage engines outside TiDB's
/// MySQL/MariaDB allowlist (`checkTableEngine`).
pub fn check_unsupported_table_options(options: &[TableOption]) -> Result<(), MysqlError> {
    /// Go `mysqlValidTableEngineNames`.
    const VALID_ENGINES: &[&str] = &[
        "archive",
        "blackhole",
        "csv",
        "example",
        "federated",
        "innodb",
        "memory",
        "merge",
        "mgr_myisam",
        "myisam",
        "ndb",
        "heap",
        "aria",
        "myrocks",
        "tokudb",
    ];

    for option in options {
        match option {
            TableOption::Union(_) => {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_TABLE_OPTION_UNION_UNSUPPORTED,
                ));
            }
            TableOption::InsertMethod(_) => {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_TABLE_OPTION_INSERT_METHOD_UNSUPPORTED,
                ));
            }
            TableOption::Engine(engine)
                if !VALID_ENGINES
                    .iter()
                    .any(|valid| engine.eq_ignore_ascii_case(valid)) =>
            {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_UNKNOWN_ENGINE
                        .gen_with_stack_by_args(&[(engine.clone()).to_string().into()]),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Go `checkAlterTableGrammar`.
fn check_alter_table(alter: &tidb_ast::AlterTableStmt) -> Result<(), MysqlError> {
    check_table_name(path_name(&alter.name))?;
    for action in &alter.actions {
        if let AlterTableAction::RenameTable { new_name } = action {
            check_table_name(path_name(new_name))?;
        }
        let new_columns: &[ColumnDef] = match action {
            AlterTableAction::AddColumn { column, .. }
            | AlterTableAction::ModifyColumn { column, .. }
            | AlterTableAction::ChangeColumn { column, .. } => std::slice::from_ref(column),
            AlterTableAction::AddColumns { columns, .. } => columns,
            _ => &[],
        };
        for column in new_columns {
            check_column(column).map_err(|error| error.into_driver_error(column))?;
        }
        if let AlterTableAction::SetTableOptions { options } = action {
            check_unsupported_table_options(options)?;
        }
        match action {
            AlterTableAction::AddIndexConstraint(index)
                if matches!(
                    index.kind,
                    IndexConstraintKind::Key
                        | IndexConstraintKind::Index
                        | IndexConstraintKind::Unique
                        | IndexConstraintKind::UniqueIndex
                        | IndexConstraintKind::UniqueKey
                        | IndexConstraintKind::PrimaryKey
                ) =>
            {
                check_index_info(index.name.as_deref().unwrap_or(""), &index.parts)?;
            }
            AlterTableAction::AddStatistics { name, .. }
            | AlterTableAction::DropStatistics { name, .. }
                if is_incorrect_identifier_name(name) =>
            {
                return Err(MysqlError::unknown(format!(
                    "Incorrect statistics name: {name}"
                )));
            }
            AlterTableAction::Partition(AlterPartitionAction::Add {
                spec: tidb_ast::AddPartitionSpec::Definitions(definitions),
                ..
            }) if definitions
                .iter()
                .any(|definition| is_incorrect_identifier_name(&definition.name)) =>
            {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_WRONG_PARTITION_NAME.gen_with_stack_by_args(&[]),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Go `checkIndexInfo`: the index name, the number of key parts, zero
/// prefix lengths and repeated columns.
fn check_index_info(name: &str, parts: &[IndexPart]) -> Result<(), MysqlError> {
    if name.go_equal_fold("PRIMARY") {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_WRONG_NAME_FOR_INDEX.gen_with_stack_by_args(&[name.into()]),
        ));
    }
    if parts.len() > MAX_KEY_PARTS {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TOO_MANY_KEY_PARTS
                .gen_with_stack_by_args(&[MAX_KEY_PARTS.into()]),
        ));
    }
    for part in parts {
        if let IndexPart::Column {
            name,
            prefix_len: Some(0),
            ..
        } = part
        {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_KEY_PART0
                    .gen_with_stack_by_args(&[(name.clone()).to_string().into()]),
            ));
        }
    }
    // Go `checkDuplicateColumnName`.
    let mut seen: Vec<String> = Vec::with_capacity(parts.len());
    for part in parts {
        if let IndexPart::Column { name, .. } = part {
            let folded = name.go_to_lower();
            if seen.contains(&folded) {
                return Err(MysqlError::from_terror(
                    &tidb_infoschema::infoschema_errors::ERR_COLUMN_EXISTS
                        .gen_with_stack_by_args(&[(name.clone()).to_string().into()]),
                ));
            }
            seen.push(folded);
        }
    }
    Ok(())
}

/// Go `checkColumnOptions`: whether the column declares an inline primary
/// key, refusing a virtual generated one and AUTO_RANDOM on a temporary
/// table.
fn check_column_options(temporary: bool, options: &[ColumnOption]) -> Result<usize, MysqlError> {
    let mut primary = 0;
    let mut generated = false;
    let mut stored = false;
    for option in options {
        match option {
            ColumnOption::InlineKey(key)
                if matches!(key.kind, tidb_ast::InlineKeyKind::Primary { .. }) =>
            {
                primary = 1;
            }
            ColumnOption::Generated {
                stored: is_stored, ..
            } => {
                generated = true;
                stored = *is_stored;
            }
            ColumnOption::AutoRandom(_) if temporary => {
                return Err(MysqlError::from_terror(
                    &tidb_plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                        .gen_with_stack_by_args(&["auto_random".into()]),
                ));
            }
            _ => {}
        }
    }
    if primary > 0 && generated && !stored {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_UNSUPPORTED_ON_GENERATED_COLUMN.gen_with_stack_by_args(&[
                ("Defining a virtual generated column as primary key".to_owned())
                    .to_string()
                    .into(),
            ]),
        ));
    }
    Ok(primary)
}

/// Why Go's `checkColumn` refuses a column definition.
enum ColumnCheckError {
    WrongColumnName,
    InvalidDefault,
    DisplayWidthOutOfRange,
    TooBigFieldLength,
    WrongFieldSpec,
    TooBigScale { scale: i64, maximum: i64 },
    TooBigDisplayWidth { maximum: i64 },
    MBiggerThanD,
    TooBigSet,
    IllegalSetValue(String),
    TooBigPrecision { precision: i64 },
    InvalidFieldSize,
}

impl ColumnCheckError {
    fn into_driver_error(self, column: &ColumnDef) -> MysqlError {
        let name = &column.name;
        let coded = |proto: &'static tidb_terror::TerrorError, message: String| {
            MysqlError::from_terror(&proto.gen_with_stack(message))
        };
        match self {
            Self::WrongColumnName => MysqlError::from_terror(
                &tidb_dbterror::ERR_WRONG_COLUMN_NAME
                    .gen_with_stack_by_args(&[(name.clone()).to_string().into()]),
            ),
            Self::InvalidDefault => coded(
                &tidb_dbterror::ERR_INVALID_DEFAULT_VALUE,
                format!("Invalid default value for '{name}'"),
            ),
            Self::DisplayWidthOutOfRange => coded(
                &tidb_datatype::types_errors::ERR_TOO_BIG_DISPLAY_WIDTH,
                format!(
                    "Display width out of range for column '{name}' (max = {})",
                    u32::MAX
                ),
            ),
            Self::TooBigFieldLength => {
                let maximum = if is_varchar(column) {
                    MAX_FIELD_VARCHAR_LENGTH
                } else {
                    MAX_FIELD_CHAR_LENGTH
                };
                coded(
                    &tidb_datatype::types_errors::ERR_TOO_BIG_FIELD_LENGTH,
                    format!(
                        "Column length too big for column '{name}' (max = {maximum}); use BLOB or TEXT instead"
                    ),
                )
            }
            Self::WrongFieldSpec => coded(
                &tidb_datatype::types_errors::ERR_WRONG_FIELD_SPEC,
                format!("Incorrect column specifier for column '{name}'"),
            ),
            Self::TooBigScale { scale, maximum } => coded(
                &tidb_datatype::types_errors::ERR_TOO_BIG_SCALE,
                format!(
                    "Too big scale {scale} specified for column '{name}'. Maximum is {maximum}."
                ),
            ),
            Self::TooBigDisplayWidth { maximum } => coded(
                &tidb_datatype::types_errors::ERR_TOO_BIG_DISPLAY_WIDTH,
                format!("Display width out of range for column '{name}' (max = {maximum})"),
            ),
            Self::MBiggerThanD => MysqlError::from_terror(
                &tidb_datatype::types_errors::ERR_M_BIGGER_THAN_D
                    .gen_with_stack_by_args(&[(name.clone()).to_string().into()]),
            ),
            Self::TooBigSet => coded(
                &tidb_datatype::types_errors::ERR_TOO_BIG_SET,
                format!("Too many strings for column {name} and SET"),
            ),
            Self::IllegalSetValue(value) => coded(
                &tidb_datatype::types_errors::ERR_ILLEGAL_VALUE_FOR_TYPE,
                format!("Illegal set '{value}' value found during parsing"),
            ),
            Self::TooBigPrecision { precision } => coded(
                &tidb_datatype::types_errors::ERR_TOO_BIG_PRECISION,
                format!(
                    "Too-big precision {precision} specified for '{name}'. Maximum is {MAX_DECIMAL_WIDTH}."
                ),
            ),
            Self::InvalidFieldSize => coded(
                &tidb_datatype::types_errors::ERR_INVALID_FIELD_SIZE,
                format!("Invalid size for column '{name}'."),
            ),
        }
    }
}

/// Whether the declared type is VARCHAR or VARBINARY (Go `TypeVarchar`).
fn is_varchar(column: &ColumnDef) -> bool {
    column
        .ty
        .type_code()
        .is_some_and(|code| code == FieldTypeCode::Varchar)
}

/// One numeric type argument as Go's parser stores it.
fn type_arg(argument: &ColumnTypeArg) -> Option<i64> {
    match argument {
        ColumnTypeArg::Text(text) => text.parse().ok(),
        ColumnTypeArg::Bytes(_) => None,
    }
}

/// Go `checkColumn`: the column's name, a NOW() default on a type that is
/// not a timestamp, and the declared type's range limits. A type this
/// preprocessor cannot read is left to the DDL builder, which reports it.
fn check_column(column: &ColumnDef) -> Result<(), ColumnCheckError> {
    if is_incorrect_identifier_name(&column.name) {
        return Err(ColumnCheckError::WrongColumnName);
    }
    let Some(code) = column.ty.type_code() else {
        return Ok(());
    };
    if is_invalid_default_value(column, code) {
        return Err(ColumnCheckError::InvalidDefault);
    }
    let args = &column.ty.args;
    let numeric = |index: usize| args.get(index).and_then(type_arg);
    let flen = if matches!(code, FieldTypeCode::Enum | FieldTypeCode::Set) {
        None
    } else {
        numeric(0)
    };
    if flen.is_some_and(|flen| flen > i64::from(u32::MAX)) {
        return Err(ColumnCheckError::DisplayWidthOutOfRange);
    }
    match code {
        FieldTypeCode::String => {
            if flen.is_some_and(|flen| flen > MAX_FIELD_CHAR_LENGTH) {
                return Err(ColumnCheckError::TooBigFieldLength);
            }
        }
        // Go checks a VARCHAR here only when its charset is already known
        // (`tp.GetCharset()` is set by the parser for VARBINARY and an
        // explicit CHARACTER SET); a binary charset admits 65535 bytes. A
        // multi-byte charset's limit is left to the DDL builder.
        FieldTypeCode::Varchar => {
            let binary = tidb_ast::is_intrinsically_binary(&column.ty.name)
                || column
                    .ty
                    .charset
                    .as_deref()
                    .is_some_and(|charset| charset.eq_ignore_ascii_case("binary"));
            if binary && flen.is_some_and(|flen| flen > MAX_FIELD_VARCHAR_LENGTH) {
                return Err(ColumnCheckError::TooBigFieldLength);
            }
        }
        FieldTypeCode::Float | FieldTypeCode::Double => match (flen, numeric(1)) {
            (Some(flen), None) => {
                if code == FieldTypeCode::Float && flen > MAX_DOUBLE_PRECISION_LENGTH {
                    return Err(ColumnCheckError::WrongFieldSpec);
                }
            }
            (Some(flen), Some(decimal)) => {
                if decimal > MAX_FLOATING_TYPE_SCALE {
                    return Err(ColumnCheckError::TooBigScale {
                        scale: decimal,
                        maximum: MAX_FLOATING_TYPE_SCALE,
                    });
                }
                if flen > MAX_FLOATING_TYPE_WIDTH || flen == 0 {
                    return Err(ColumnCheckError::TooBigDisplayWidth {
                        maximum: MAX_FLOATING_TYPE_WIDTH,
                    });
                }
                if flen < decimal {
                    return Err(ColumnCheckError::MBiggerThanD);
                }
            }
            _ => {}
        },
        FieldTypeCode::Set => {
            if args.len() > MAX_TYPE_SET_MEMBERS {
                return Err(ColumnCheckError::TooBigSet);
            }
            for argument in args {
                if let ColumnTypeArg::Text(member) = argument {
                    if member.contains(',') {
                        return Err(ColumnCheckError::IllegalSetValue(member.clone()));
                    }
                }
            }
        }
        FieldTypeCode::NewDecimal => {
            let decimal = numeric(1);
            if let Some(decimal) = decimal.filter(|decimal| *decimal > MAX_DECIMAL_SCALE) {
                return Err(ColumnCheckError::TooBigScale {
                    scale: decimal,
                    maximum: MAX_DECIMAL_SCALE,
                });
            }
            if let Some(flen) = flen.filter(|flen| *flen > MAX_DECIMAL_WIDTH) {
                return Err(ColumnCheckError::TooBigPrecision { precision: flen });
            }
            if let (Some(flen), Some(decimal)) = (flen, decimal) {
                if flen < decimal {
                    return Err(ColumnCheckError::MBiggerThanD);
                }
            }
        }
        FieldTypeCode::Bit => {
            if let Some(flen) = flen {
                if flen <= 0 {
                    return Err(ColumnCheckError::InvalidFieldSize);
                }
                if flen > MAX_BIT_DISPLAY_WIDTH {
                    return Err(ColumnCheckError::TooBigDisplayWidth {
                        maximum: MAX_BIT_DISPLAY_WIDTH,
                    });
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Go `isInvalidDefaultValue`: the LAST default is `NOW()` (which the parser
/// spells `CURRENT_TIMESTAMP`) on a type that is neither TIMESTAMP nor
/// DATETIME.
fn is_invalid_default_value(column: &ColumnDef, code: FieldTypeCode) -> bool {
    let Some(default) = column.options.iter().rev().find_map(|option| match option {
        ColumnOption::Default(expr) => Some(expr),
        _ => None,
    }) else {
        return false;
    };
    !matches!(code, FieldTypeCode::Timestamp | FieldTypeCode::Datetime)
        && matches!(default, tidb_ast::Expr::Func { name, .. }
            if name.eq_ignore_ascii_case("current_timestamp"))
}

/// Whether `expr` is what Go's parser builds as a `*driver.ValueExpr`
/// (`Some(true)` for the NULL literal). A signed number is a unary
/// expression there, not a value.
fn value_expr_is_null(expr: &tidb_ast::Expr) -> Option<bool> {
    use tidb_ast::Expr;
    match expr {
        Expr::Null => Some(true),
        Expr::Value(value) => {
            tidb_expr::ValueExprDatum::of(value).map(|value| value.datum.is_null())
        }
        Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Float(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::String(_)
        | Expr::RawString(_)
        | Expr::CharsetString { .. }
        | Expr::CharsetBinary { .. }
        | Expr::Bool(_) => Some(false),
        _ => None,
    }
}

/// Go `checkAutoIncrementOp` over the option at `index`: whether it is
/// AUTO_INCREMENT, refusing a default the column cannot take.
fn check_auto_increment_op(column: &ColumnDef, index: usize) -> Result<bool, MysqlError> {
    let options = &column.options;
    let invalid_default = || {
        MysqlError::from_terror(
            &tidb_dbterror::ERR_INVALID_DEFAULT_VALUE
                .gen_with_stack_by_args(&[column.name.clone().into()]),
        )
    };
    let mut has_auto_increment = false;
    if matches!(options[index], ColumnOption::AutoIncrement) {
        has_auto_increment = true;
        for later in &options[index + 1..] {
            let ColumnOption::Default(expr) = later else {
                continue;
            };
            if value_expr_is_null(expr) == Some(false) {
                return Err(invalid_default());
            }
            if matches!(expr, tidb_ast::Expr::Func { name, .. } if name.eq_ignore_ascii_case("current_date"))
            {
                return Err(invalid_default());
            }
        }
    }
    if let ColumnOption::Default(expr) = &options[index] {
        if index + 1 != options.len() {
            if value_expr_is_null(expr) == Some(true) {
                return Ok(has_auto_increment);
            }
            if options[index + 1..]
                .iter()
                .any(|later| matches!(later, ColumnOption::AutoIncrement))
            {
                // A plain `errors.Errorf`, so 1105 with the same text.
                return Err(MysqlError::unknown(format!(
                    "Invalid default value for '{}'",
                    column.name
                )));
            }
        }
    }
    Ok(has_auto_increment)
}

/// Go `checkAutoIncrement`: one AUTO_INCREMENT column at most, of an
/// integer or floating type.
fn check_auto_increment(create: &CreateTableStmt) -> Result<(), MysqlError> {
    let mut auto_columns = Vec::new();
    for column in &create.columns {
        let mut has_auto_increment = false;
        for index in 0..column.options.len() {
            if check_auto_increment_op(column, index)? {
                has_auto_increment = true;
            }
        }
        if has_auto_increment {
            auto_columns.push(column);
        }
    }
    if auto_columns.len() > 1 {
        return Err(MysqlError::from_terror(
            &tidb_autoid::autoid_errors::ERR_WRONG_AUTO_KEY.gen_with_stack_by_args(&[]),
        ));
    }
    for column in auto_columns {
        let integer_or_float = column.ty.type_code().is_some_and(|code| {
            matches!(
                code,
                FieldTypeCode::Tiny
                    | FieldTypeCode::Short
                    | FieldTypeCode::Long
                    | FieldTypeCode::Float
                    | FieldTypeCode::Double
                    | FieldTypeCode::LongLong
                    | FieldTypeCode::Int24
            )
        });
        if !integer_or_float {
            return Err(MysqlError::unknown(format!(
                "Incorrect column specifier for column '{}'",
                column.name
            )));
        }
    }
    Ok(())
}

/// Go `checkContainDotColumn`: a column written `db.t.c` must name the
/// table being created.
fn check_contain_dot_column(create: &CreateTableStmt) -> Result<(), MysqlError> {
    let table = path_name(&create.name);
    let schema = match create.name.as_slice() {
        [schema, _] => schema.as_str(),
        _ => "",
    };
    for column in &create.columns {
        let (column_schema, column_table) = match column.qualifier.as_slice() {
            [table] => ("", table.as_str()),
            [schema, table] => (schema.as_str(), table.as_str()),
            _ => ("", ""),
        };
        if !column_schema.is_empty() && column_schema != schema {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_WRONG_DB_NAME.gen_with_stack_by_args(&[column_schema.into()]),
            ));
        }
        if !column_table.is_empty() && column_table != table {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_WRONG_TABLE_NAME
                    .gen_with_stack_by_args(&[column_table.to_owned().into()]),
            ));
        }
    }
    Ok(())
}

// Go's error constructors this file raises, rendered as the driver rendered
// them (`tidb-dbterror`'s `to_mysql_error` arms, verbatim).

/// Whether a locking clause is the shared kind, which is the gated one --
/// `FOR UPDATE` is a real lock in TiDB and is never gated.
fn share_lock(lock: &Option<tidb_ast::SelectLock>) -> bool {
    matches!(
        lock,
        Some(tidb_ast::SelectLock {
            kind: tidb_ast::LockKind::Share,
            ..
        })
    )
}

#[cfg(test)]
mod ddl_grammar_tests {
    use super::*;

    #[test]
    fn table_engine_validation_matches_the_go_allowlist_and_error() {
        for engine in [
            "ARCHIVE",
            "blackhole",
            "csv",
            "example",
            "federated",
            "InnoDB",
            "memory",
            "merge",
            "mgr_myisam",
            "myisam",
            "ndb",
            "heap",
            "aria",
            "myrocks",
            "tokudb",
        ] {
            check_unsupported_table_options(&[TableOption::Engine(engine.to_owned())])
                .unwrap_or_else(|error| panic!("Go admits ENGINE={engine}: {error}"));
        }

        let stmt = tidb_parser::parse("CREATE TABLE invalid_engine (id BIGINT) ENGINE=imaginary")
            .expect("the statement parses");
        let error = check_ddl_grammar(&stmt, true)
            .expect_err("Go rejects an engine outside its compatibility allowlist");
        assert_eq!(error.code, 1286);
        assert_eq!(error.state, *b"42000");
        assert_eq!(error.message, "Unknown storage engine 'imaginary'");
    }
}

#[cfg(test)]
mod preprocessor_tests {
    use super::*;

    /// A session in `test`, strict, `tidb_mview_enable` off, no temporary
    /// tables.
    struct TestContext;

    impl PreprocessContext for TestContext {
        fn current_db(&self) -> &str {
            "test"
        }
        fn strict_sql_mode(&self) -> bool {
            true
        }
        fn is_local_temporary_table(&self, _path: &[String]) -> bool {
            false
        }
        fn is_not_global_temporary_table(&self, _path: &[String]) -> Result<bool, PlanError> {
            Ok(false)
        }
        fn noop_funcs_mode(&self) -> tidb_variable::set_hooks::NoopFuncsMode {
            tidb_variable::set_hooks::NoopFuncsMode::Off
        }
        fn shared_lock_promotion(&self) -> bool {
            false
        }
        fn auth_user(&self) -> Option<(String, String)> {
            None
        }
        fn in_prepare(&self) -> bool {
            false
        }
        fn table_by_name(&self, _path: &[String], _verb: &'static str) -> Result<(), PlanError> {
            Ok(())
        }
        fn like_source_info(
            &self,
            _path: &[String],
        ) -> Result<Option<tidb_model::GoShared<tidb_model::TableInfo>>, PlanError> {
            Ok(None)
        }
    }

    #[test]
    fn name_const_requires_the_source_literal_shapes() {
        use tidb_ast::{BinaryOp, Expr, UnaryOp};

        let string = || Expr::String("hello".to_owned());
        let int = || Expr::Int("1".to_owned());
        assert!(check_name_const_args(&[string(), int()]).is_ok());
        assert!(
            check_name_const_args(&[string(), Expr::Unary(UnaryOp::Minus, Box::new(int()))])
                .is_ok()
        );

        for args in [
            vec![Expr::Column(vec!["a".to_owned()]), int()],
            vec![string(), Expr::Column(vec!["b".to_owned()])],
            vec![
                string(),
                Expr::Binary(BinaryOp::Plus, Box::new(int()), Box::new(int())),
            ],
            vec![
                Expr::Func {
                    name: "concat".to_owned(),
                    args: vec![string(), string()],
                    origin_position: 0,
                },
                int(),
            ],
        ] {
            assert_eq!(
                check_name_const_args(&args),
                Err(tidb_exprctx::EvalError::IncorrectArguments(
                    "Incorrect arguments to NAME_CONST".to_owned()
                ))
            );
        }
    }

    /// Go `preprocessor.checkFuncCastExpr` and its NAME_CONST check refuse
    /// the statement in walk order; `JSON_SUM_CRC32` is no `FuncCastExpr`.
    #[test]
    fn preprocess_refuses_cast_declarations_and_name_const_shapes() {
        let run = |sql: &str| {
            let mut stmt = tidb_parser::parse(sql).expect("parses");
            preprocess(&mut stmt, &TestContext)
        };
        let eval = |result: Result<PreprocessorReturn, PlanError>| match result {
            Err(error) => match error.kind() {
                tidb_planner_core_base::PlanErrorKind::Eval(eval) => eval.clone(),
                other => panic!("not an expression error: {other:?}"),
            },
            Ok(_) => panic!("the statement passed the preprocessor"),
        };
        assert_eq!(
            eval(run("select cast(12.1 as decimal(3, 4))")),
            tidb_exprctx::EvalError::InvalidTypeDeclaration {
                code: 1427,
                message:
                    "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '12.1')."
                        .to_owned(),
            }
        );
        assert_eq!(
            eval(run("select cast(1 as datetime(7))")),
            tidb_exprctx::EvalError::InvalidTypeDeclaration {
                code: 1426,
                message: "Too-big precision 7 specified for 'CAST'. Maximum is 6.".to_owned(),
            }
        );
        // TIME's fsp is the rewriter's check.
        assert!(run("select cast(1 as time(7))").is_ok());
        assert_eq!(
            eval(run("select name_const('x', cast(1 as double))")),
            tidb_exprctx::EvalError::IncorrectArguments(
                "Incorrect arguments to NAME_CONST".to_owned()
            )
        );
        // The first refusal in walk order wins.
        assert_eq!(
            eval(run(
                "select cast(1 as decimal(70)), name_const('x', a) from t"
            )),
            tidb_exprctx::EvalError::InvalidTypeDeclaration {
                code: 1426,
                message: "Too-big precision 70 specified for '1'. Maximum is 65.".to_owned(),
            }
        );
        let names = run("select @a, @b := 1, @b from t1 join db.t2 as of timestamp now()")
            .expect("preprocesses");
        assert_eq!(
            names.table_names,
            vec![
                (String::new(), "t1".to_owned()),
                ("db".to_owned(), "t2".to_owned())
            ]
        );
        assert!(names.has_as_of);
        assert_eq!(
            names.readonly_user_vars,
            ["a".to_owned()].into_iter().collect()
        );
    }
}

/// Runs Go's set-operation and derived-alias preprocessing over a complete
/// query, including the ORACLE-mode exception for unnamed derived tables.
///
/// Go's preprocessor is a full `ast.Visitor`, so EVERY child query is
/// visited: a CTE definition, a derived table in `FROM`, a scalar subquery, an
/// `IN`/`EXISTS` subquery, and a nested set operation all reach
/// `checkSetOprSelectList`. Validating only the outer `SetOprStmt` (plus its
/// own `WITH`) accepted
/// `select 1 from (select a from t0 limit 1 union all select a from t0 limit 1) tmp`,
/// which Go rejects with 1221.
pub fn validate_query_usage(
    query: &tidb_ast::QueryStmt,
    ctx: &tidb_sessionctx::StmtContext,
) -> Result<(), tidb_driver_error::DriverError> {
    struct SetOprUsage {
        error: Option<tidb_driver_error::DriverError>,
        oracle_mode: bool,
    }

    impl tidb_ast::Visitor for SetOprUsage {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if self.error.is_some() {
                return true;
            }
            if let Some(tidb_ast::JoinNode::Derived { alias, .. }) =
                node.downcast_ref::<tidb_ast::JoinNode>()
            {
                if !self.oracle_mode && alias.as_ref().is_none_or(String::is_empty) {
                    self.error = Some(tidb_driver_error::DriverError::from(
                        tidb_dbterror::ERR_DERIVED_MUST_HAVE_ALIAS.gen_with_stack_by_args(&[]),
                    ));
                    return true;
                }
            }
            if let Some(set_opr) = node.downcast_ref::<tidb_ast::SetOprStmt>() {
                if let Err(error) = check_set_opr_select_list(set_opr) {
                    self.error = Some(error);
                    return true;
                }
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            self.error.is_none()
        }
    }

    // `Visitable` walks a MUTABLE child graph, which is what Go's visitor
    // takes; the check itself only reads, so a clone is enough.
    let mut owned = query.clone();
    let mut visitor = SetOprUsage {
        error: None,
        oracle_mode: ctx.ddl_sql_mode() & tidb_mysql::ModeOracle.0 != 0,
    };
    tidb_ast::Visitable::accept(&mut owned, &mut visitor);
    match visitor.error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Go `preprocessor.checkSetOprSelectList`: every nested set-operation list is
/// checked, and every plain term except that list's last one must put its own
/// `ORDER BY`/`LIMIT` inside parentheses. `INTO` is incompatible with UNION
/// even when that term is parenthesized.
pub fn check_set_opr_select_list(
    stmt: &tidb_ast::SetOprStmt,
) -> Result<(), tidb_driver_error::DriverError> {
    for (index, term) in stmt.terms.iter().enumerate() {
        match &term.body {
            tidb_ast::SetOprTermBody::Nested(nested) => check_set_opr_select_list(nested)?,
            tidb_ast::SetOprTermBody::Select(select) if index + 1 < stmt.terms.len() => {
                if select.into_outfile.is_some() {
                    return Err(tidb_driver_error::DriverError::from(
                        tidb_plannererrors::ERR_WRONG_USAGE
                            .gen_with_stack_by_args(&["UNION".into(), ("INTO").into()]),
                    ));
                }
                if term.in_braces {
                    continue;
                }
                if select.limit.is_some() {
                    return Err(tidb_driver_error::DriverError::from(
                        tidb_plannererrors::ERR_WRONG_USAGE
                            .gen_with_stack_by_args(&["UNION".into(), ("LIMIT").into()]),
                    ));
                }
                if !select.order_by.is_empty() {
                    return Err(tidb_driver_error::DriverError::from(
                        tidb_plannererrors::ERR_WRONG_USAGE
                            .gen_with_stack_by_args(&["UNION".into(), ("ORDER BY").into()]),
                    ));
                }
            }
            tidb_ast::SetOprTermBody::Select(_) => {}
        }
    }
    Ok(())
}

/// Go `preprocessor.checkCreateViewWithSelectGrammar` over
/// `checkCreateViewWithSelect`: each SELECT of the body -- the body itself, or
/// every term of a set operation, nested ones included -- may not carry an
/// `INTO` clause (1350), and its lock becomes `SelectLockNone`. Returns which
/// of them had a lock, in body order, for the trailing space
/// `ddl.BuildViewInfo` restores in its place.
///
/// # Errors
///
/// `ErrViewSelectClause` for a SELECT with `INTO OUTFILE`.
pub fn check_create_view_with_select(
    query: &mut tidb_ast::QueryStmt,
) -> Result<Vec<bool>, PlanError> {
    fn check(select: &mut tidb_ast::SelectStmt, locked: &mut Vec<bool>) -> Result<(), PlanError> {
        if select.into_outfile.is_some() {
            return Err(PlanError::from(
                (&tidb_dbterror::ERR_VIEW_SELECT_CLAUSE)
                    .gen_with_stack("View's SELECT contains a 'INFO' clause".to_owned()),
            ));
        }
        locked.push(select.lock.take().is_some());
        Ok(())
    }
    fn check_set_opr(
        set_opr: &mut tidb_ast::SetOprStmt,
        locked: &mut Vec<bool>,
    ) -> Result<(), PlanError> {
        for term in &mut set_opr.terms {
            match &mut term.body {
                tidb_ast::SetOprTermBody::Select(select) => check(select, locked)?,
                tidb_ast::SetOprTermBody::Nested(nested) => check_set_opr(nested, locked)?,
            }
        }
        Ok(())
    }
    let mut locked = Vec::new();
    match query {
        tidb_ast::QueryStmt::Select(select) => check(select, &mut locked)?,
        tidb_ast::QueryStmt::SetOpr(set_opr) => check_set_opr(set_opr, &mut locked)?,
    }
    Ok(locked)
}

/// Go `preprocessor.checkCreateTableGrammar`'s `CREATE TABLE ... LIKE` arm
/// (`preprocess.go:933`): a temporary source is refused, and a temporary
/// copy may not inherit what a temporary table cannot carry
/// (`checkReferInfoForTemporaryTable`).
pub fn check_create_table_like_source(
    source: &tidb_model::TableInfo,
    temporary: tidb_ast::CreateTableTemporary,
) -> Result<(), tidb_terror::MysqlError> {
    if source.temp_table_type != tidb_model::TempTableType::NONE {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                .gen_with_stack_by_args(&["create table like".into()]),
        ));
    }
    if temporary == tidb_ast::CreateTableTemporary::None {
        return Ok(());
    }
    if source.auto_random_bits != 0 {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                .gen_with_stack_by_args(&["auto_random".into()]),
        ));
    }
    if source.pre_split_regions != 0 {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                .gen_with_stack_by_args(&["pre split regions".into()]),
        ));
    }
    if source.partition.is_some() {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_plannererrors::ERR_PARTITION_NO_TEMPORARY,
        ));
    }
    if source.shard_row_id_bits != 0 {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                .gen_with_stack_by_args(&["shard_row_id_bits".into()]),
        ));
    }
    if source.placement_policy_ref.is_some() {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                .gen_with_stack_by_args(&["placement".into()]),
        ));
    }
    Ok(())
}
