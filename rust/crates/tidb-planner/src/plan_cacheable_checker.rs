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

//! Plan-cache admission: Go `plan_cacheable_checker.go`'s `IsASTCacheable`,
//! decided at `PREPARE`, and its non-prepared half
//! (`NonPreparedPlanCacheableWithCtx`, `isSelectStmtNonPrepCacheableFastCheck`,
//! `extractTableNames`, the `nonPreparedPlanCacheableChecker` visitor) with
//! `plan_cache_param.go`'s `GetParamSQLFromAST` and `paramReplacer`, which
//! the walk applies as it admits.
//!
//! The retained physical trees and execute-time recursive rebuild live in
//! the executor's `PreparedSelectPlan`; this module only decides whether
//! a statement may enter that cache.

use std::any::Any;

use tidb_ast::{
    Expr, GroupByItem, IsTarget, JoinNode, OrderItem, QueryStmt, SelectField, SelectStmt, Stmt, Visitable, Visitor,
};
use tidb_charset::Collation;
use tidb_datatype::{Datum, Decimal, StringDatum};
use tidb_filter::is_system_schema;
use tidb_hack::{GoCaseCompare, GoToLower};
use tidb_infoschema::{SessionExtendedInfoSchema, TableEntry};
use tidb_mysql::to_lowercase as go_simple_lowercase;

/// Go `getMaxParamLimit`'s default: more than this many values in one IN
/// list, or `rows * cols` in one `INSERT`, refuses caching to save memory.
pub const MAX_NUM_PARAM: usize = 200;

/// Go `expression.UnCacheableFunctions`: names whose value depends on session
/// state the cache key does not carry.
const UNCACHEABLE_FUNCTIONS: &[&str] = &[
    "database",
    "current_user",
    "current_role",
    "current_resource_group",
    "user",
    "connection_id",
    "last_insert_id",
    "row_count",
    "version",
    "like",
    "json_object",
    "json_array",
    "coalesce",
    "convert",
    "time",
    "date",
    "timestamp",
    "aes_encrypt",
    "aes_decrypt",
];

/// Go `IsASTCacheable`: whether Go's prepared plan cache would admit this
/// statement, with Go's reason when it would not.
///
/// The kind gate comes first, as Go's does: only `SELECT`/`UPDATE`/`INSERT`/
/// `DELETE` and a set operation are cacheable at all. The `EXECUTE` of any
/// other kind re-plans every time and reports 0 -- which is also what a
/// DISABLED cache reports, so a refusal here is never observable as an error.
pub fn stmt_cacheable(
    stmt: &mut tidb_ast::Stmt,
    context: CacheableContext<'_>,
) -> Result<(), String> {
    let kind_ok = match stmt {
        tidb_ast::Stmt::Query(_) => true,
        tidb_ast::Stmt::Dml(dml) => cacheable_dml_kind(dml),
        _ => false,
    };
    if !kind_ok {
        return Err("not a SELECT/UPDATE/INSERT/DELETE/SET statement".to_owned());
    }
    let mut checker = CacheableChecker {
        context,
        refusal: None,
        sum_in_list_len: 0,
        cte_can_use: Vec::new(),
        with_scope_offsets: Vec::new(),
    };
    stmt.accept(&mut checker);
    match checker.refusal {
        Some(reason) => Err(reason),
        None => Ok(()),
    }
}

fn cacheable_dml_kind(dml: &tidb_ast::DmlStmt) -> bool {
    match dml {
        tidb_ast::DmlStmt::Insert(_)
        | tidb_ast::DmlStmt::Update(_)
        | tidb_ast::DmlStmt::Delete(_) => true,
        _ => false,
    }
}

/// Session and infoschema state read by Go's `IsASTCacheable` visitor.
#[derive(Clone, Copy)]
pub struct CacheableContext<'a> {
    pub catalog: &'a SessionExtendedInfoSchema,
    pub current_database: &'a str,
    pub enable_param_limit: bool,
    pub enable_subquery: bool,
    pub dynamic_partition_prune: bool,
    pub disable_partition_cache: bool,
    pub enable_generated_columns: bool,
    pub max_num_param: usize,
}

/// Go's `cacheableChecker` visitor, over this tier's AST.
struct CacheableChecker<'a> {
    context: CacheableContext<'a>,
    refusal: Option<String>,
    /// Go `sumInListLen`: IN-list lengths accumulate across the statement.
    sum_in_list_len: usize,
    /// Unqualified CTE names visible at the current traversal point.
    cte_can_use: Vec<String>,
    /// CTE visibility length on entry to each query block owning a WITH.
    with_scope_offsets: Vec<usize>,
}

impl CacheableChecker<'_> {
    fn refuse(&mut self, reason: impl Into<String>) -> bool {
        self.refusal = Some(reason.into());
        // Skipping children is enough; `leave` keeps returning `true` so the
        // walk winds down without visiting further subtrees' contents, and
        // the FIRST reason -- Go's -- is the one reported.
        true
    }

    fn enter_with_scope(&mut self, with: &tidb_ast::WithClause) {
        self.with_scope_offsets.push(self.cte_can_use.len());
        if with.recursive {
            self.cte_can_use
                .extend(with.ctes.iter().map(|cte| cte.name.go_to_lower()));
        }
    }

    fn leave_with_scope(&mut self) {
        if let Some(offset) = self.with_scope_offsets.pop() {
            self.cte_can_use.truncate(offset);
        }
    }

    fn check_table(&mut self, path: &[String]) -> bool {
        let Some(table) = path.last() else {
            return false;
        };
        if path.len() == 1 && self.cte_can_use.iter().any(|cte| cte.go_lower_eq(table)) {
            return false;
        }
        let database = if path.len() > 1 {
            &path[path.len() - 2]
        } else {
            self.context.current_database
        };
        let Some(entry) = self.context.catalog.table_in(database, table) else {
            return self.refuse(format!("find table {database}.{table} failed"));
        };
        let TableEntry::Kv(table) = entry else {
            return false;
        };
        if table.partition().is_some() {
            if !self.context.dynamic_partition_prune {
                return self.refuse(
                    "query accesses partitioned tables is un-cacheable if tidb_partition_pruning_mode = 'static'",
                );
            }
            if self.context.disable_partition_cache {
                return self.refuse("Fix33031 fix-control set and partitioned table");
            }
        }
        if !self.context.enable_generated_columns
            && table
                .columns()
                .iter()
                .any(|column| column.generated.is_some())
        {
            return self.refuse("query accesses generated columns is un-cacheable");
        }
        if table.temp_table_type() != tidb_model::TempTableType::NONE {
            return self.refuse("query accesses temporary tables is un-cacheable");
        }
        false
    }
}

impl Visitor for CacheableChecker<'_> {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        if self.refusal.is_some() {
            return true;
        }
        if let Some(select) = node.downcast_ref::<tidb_ast::SelectStmt>() {
            if let Some(with) = &select.with {
                self.enter_with_scope(with);
            }
        }
        if let Some(set_opr) = node.downcast_ref::<tidb_ast::SetOprStmt>() {
            if let Some(with) = &set_opr.with {
                self.enter_with_scope(with);
            }
        }
        // Go opens no WITH scope for UPDATE/DELETE; a recursive CTE still
        // exposes its name before its own query is walked.
        let dml_with = node
            .downcast_ref::<tidb_ast::UpdateStmt>()
            .and_then(|update| update.with.as_ref())
            .or_else(|| {
                node.downcast_ref::<tidb_ast::DeleteStmt>()
                    .and_then(|delete| delete.with.as_ref())
            });
        if let Some(with) = dml_with.filter(|with| with.recursive) {
            self.cte_can_use
                .extend(with.ctes.iter().map(|cte| cte.name.go_to_lower()));
        }
        if let Some(insert) = node.downcast_ref::<tidb_ast::InsertStmt>() {
            if insert.source.is_none() {
                let rows = insert.rows.len();
                let columns = insert.rows.first().map_or(0, Vec::len);
                if rows.saturating_mul(columns) > self.context.max_num_param {
                    return self.refuse("too many values in the insert statement");
                }
            }
            if self.check_table(&insert.table) {
                return true;
            }
        }
        if let Some(table) = node.downcast_ref::<tidb_ast::TableRef>() {
            if self.check_table(&table.name) {
                return true;
            }
        }
        if let Some(expr) = node.downcast_ref::<tidb_ast::Expr>() {
            match expr {
                // Go `*ast.VariableExpr`, a user or a system variable: its
                // value is not in the cache key.
                tidb_ast::Expr::UserVar(_)
                | tidb_ast::Expr::SysVar { .. }
                | tidb_ast::Expr::Assign { .. } => {
                    return self.refuse("query has user-defined variables is un-cacheable");
                }
                tidb_ast::Expr::In { list, .. } => {
                    self.sum_in_list_len += list.len();
                    if self.sum_in_list_len > self.context.max_num_param {
                        return self.refuse("too many values in in-list");
                    }
                }
                tidb_ast::Expr::Subquery(_)
                | tidb_ast::Expr::Exists { .. }
                | tidb_ast::Expr::InSubquery { .. }
                | tidb_ast::Expr::CompareSubquery { .. }
                    if !self.context.enable_subquery =>
                {
                    return self.refuse("query has sub-queries is un-cacheable");
                }
                tidb_ast::Expr::Func { name, .. }
                | tidb_ast::Expr::GenericFuncCall { name, .. } => {
                    let lowered = name.to_ascii_lowercase();
                    if UNCACHEABLE_FUNCTIONS.contains(&lowered.as_str()) {
                        return self.refuse(format!("query has '{lowered}' is un-cacheable"));
                    }
                }
                // Go parses `CONVERT(expr USING charset)` as a FuncCallExpr
                // named `convert`, which is in UnCacheableFunctions. Rust
                // keeps the custom restore syntax in a dedicated AST node.
                tidb_ast::Expr::ConvertUsing { .. } => {
                    return self.refuse("query has 'convert' is un-cacheable");
                }
                _ => {}
            }
        }
        // Go `*ast.OrderByClause` / `*ast.GroupByClause`: a marker AS the
        // by-item re-sorts per execution, which no cached plan can express.
        if let Some(order) = node.downcast_ref::<tidb_ast::OrderItem>() {
            if matches!(order.expr, tidb_ast::Expr::ParamMarker { .. }) {
                return self.refuse("query has 'order by ?' is un-cacheable");
            }
        }
        if let Some(group) = node.downcast_ref::<tidb_ast::GroupByItem>() {
            if matches!(group.expr, tidb_ast::Expr::ParamMarker { .. }) {
                return self.refuse("query has 'group by ?' is un-cacheable");
            }
        }
        if let Some(limit) = node.downcast_ref::<tidb_ast::Limit>() {
            if !self.context.enable_param_limit {
                if matches!(limit.count, tidb_ast::Expr::ParamMarker { .. }) {
                    return self.refuse("query has 'limit ?' is un-cacheable");
                }
                if limit
                    .offset
                    .as_ref()
                    .is_some_and(|offset| matches!(offset, tidb_ast::Expr::ParamMarker { .. }))
                {
                    return self.refuse("query has 'limit ?, 10' is un-cacheable");
                }
            }
        }
        if let Some(bound) = node.downcast_ref::<tidb_ast::FrameBound>() {
            let expression = match bound {
                tidb_ast::FrameBound::Preceding(expression)
                | tidb_ast::FrameBound::Following(expression) => Some(expression.as_ref()),
                _ => None,
            };
            if expression.is_some_and(|expr| matches!(expr, tidb_ast::Expr::ParamMarker { .. })) {
                return self.refuse("query has ? in window function frames is un-cacheable");
            }
        }
        false
    }

    fn leave(&mut self, node: &mut dyn Any) -> bool {
        if let Some(cte) = node.downcast_ref::<tidb_ast::Cte>() {
            if !self
                .cte_can_use
                .iter()
                .any(|name| name.go_lower_eq(&cte.name))
            {
                self.cte_can_use.push(cte.name.go_to_lower());
            }
        }
        if node
            .downcast_ref::<tidb_ast::SelectStmt>()
            .is_some_and(|select| select.with.is_some())
            || node
                .downcast_ref::<tidb_ast::SetOprStmt>()
                .is_some_and(|set_opr| set_opr.with.is_some())
        {
            self.leave_with_scope();
        }
        true
    }
}

/// Why a statement may not enter the non-prepared plan cache.
///
/// The payload is Go's own reason string, verbatim, so a future
/// `tidb_last_plan_cache_reason`-style surface reports what TiDB reports and
/// so a divergence in the refusal itself is visible rather than silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refusal {
    /// Go's own reason string, verbatim.
    pub reason: &'static str,
    /// Whether Go increments `GetNonPrepPlanCacheUnsupportedCounter` for
    /// this refusal: checker-walk refusals set `cacheable=false` and count
    /// (`plan_cacheable_checker.go:470-476`), while the clause-level
    /// fast-check refusals and the statement-kind top gate return before
    /// the counter exists to them.
    pub counted: bool,
}

impl Refusal {
    /// A checker-walk refusal: counted by the unsupported counter.
    pub fn walk(reason: &'static str) -> Self {
        Self {
            reason,
            counted: true,
        }
    }

    /// A clause-level fast-check refusal: not counted.
    pub fn fast(reason: &'static str) -> Self {
        Self {
            reason,
            counted: false,
        }
    }
}

/// One parameterized statement and its execute-time literal values.
pub struct ParameterizedSelect {
    /// The cache key: the current database and the parameterized statement's
    /// restored text.
    pub key: String,
    /// The statement with its parameterized literals replaced by markers.
    pub statement: Stmt,
    /// The replaced literals, in marker order.
    pub values: Vec<Datum>,
}

/// Go `NonPreparedPlanCacheableWithCtx`'s UPDATE/INSERT/DELETE arms plus the
/// checker walk and `paramReplacer` over the DML tree: SET values, `VALUES`
/// rows, `ON DUPLICATE KEY UPDATE` values, and the WHERE predicate are
/// parameterized; ORDER BY and LIMIT literals stay verbatim (Go's replacer
/// skips those node kinds so different limit values plan differently).
pub fn parameterize_dml(
    stmt: &Stmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    enable_param_limit: bool,
    string_collation: Collation,
    max_num_param: usize,
) -> Result<ParameterizedSelect, Refusal> {
    let mut walk = Walk {
        catalog,
        current_db,
        enable_param_limit,
        tables: Vec::new(),
        params: Vec::new(),
        string_collation,
        const_count: 0,
        sum_in_list_len: 0,
        filter_depth: 0,
        max_num_param,
    };
    let statement = match stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Update(update) => {
                if !update.hints.is_empty() {
                    return Err(Refusal::fast(
                        "not support update statement with table hints",
                    ));
                }
                if matches!(update.kind, tidb_ast::UpdateKind::Multi { .. }) {
                    return Err(Refusal::fast(
                        "not support multiple tables update statements",
                    ));
                }
                let mut statement = Stmt::Dml(tidb_ast::NodeBox::new(tidb_ast::DmlStmt::Update(
                    update.clone(),
                )));
                if let Stmt::Dml(dml) = &mut statement {
                    let tidb_ast::DmlStmt::Update(update) = &mut **dml else {
                        unreachable!("cloned the matched update")
                    };
                    walk.dml_table(match &update.kind {
                        tidb_ast::UpdateKind::Single(table) => &table.name,
                        tidb_ast::UpdateKind::Multi { .. } => unreachable!("refused above"),
                    })?;
                    walk.table_names_cacheable()?;
                    walk.update(update)?;
                }
                statement
            }
            tidb_ast::DmlStmt::Insert(insert) => {
                if !insert.hints.is_empty() {
                    return Err(Refusal::fast(
                        "not support insert statement with table hints",
                    ));
                }
                let mut statement = Stmt::Dml(tidb_ast::NodeBox::new(tidb_ast::DmlStmt::Insert(
                    insert.clone(),
                )));
                if let Stmt::Dml(dml) = &mut statement {
                    let tidb_ast::DmlStmt::Insert(insert) = &mut **dml else {
                        unreachable!("cloned the matched insert")
                    };
                    walk.dml_table(&insert.table)?;
                    walk.table_names_cacheable()?;
                    if insert.rows.is_empty() {
                        // `INSERT ... SELECT`: Go admits only a plain SELECT
                        // source and fast-checks it before the walk.
                        let Some(source) = insert.source.as_deref_mut() else {
                            return Err(Refusal::fast("not a SELECT statement"));
                        };
                        let tidb_ast::QueryStmt::Select(select) = source else {
                            return Err(Refusal::fast("not a select statement"));
                        };
                        walk.select(select)?;
                    } else {
                        let n_rows = insert.rows.len();
                        let n_cols = insert.rows.first().map_or(0, Vec::len);
                        if n_rows * n_cols > walk.max_num_param {
                            return Err(Refusal::fast("too many values in the insert statement"));
                        }
                        walk.insert(insert)?;
                    }
                }
                statement
            }
            tidb_ast::DmlStmt::Delete(delete) => {
                if !delete.hints.is_empty() {
                    // Go's own message names INSERT for every statement kind.
                    return Err(Refusal::fast(
                        "not support insert statement with table hints",
                    ));
                }
                if matches!(delete.kind, tidb_ast::DeleteKind::Multi { .. }) {
                    return Err(Refusal::fast(
                        "not support multiple tables delete statements",
                    ));
                }
                let mut statement = Stmt::Dml(tidb_ast::NodeBox::new(tidb_ast::DmlStmt::Delete(
                    delete.clone(),
                )));
                if let Stmt::Dml(dml) = &mut statement {
                    let tidb_ast::DmlStmt::Delete(delete) = &mut **dml else {
                        unreachable!("cloned the matched delete")
                    };
                    walk.dml_table(match &delete.kind {
                        tidb_ast::DeleteKind::Single(table) => &table.name,
                        tidb_ast::DeleteKind::Multi { .. } => unreachable!("refused above"),
                    })?;
                    walk.table_names_cacheable()?;
                    walk.delete(delete)?;
                }
                statement
            }
            _ => return Err(Refusal::fast("not a SELECT/UPDATE/INSERT/DELETE statement")),
        },
        _ => return Err(Refusal::fast("not a SELECT/UPDATE/INSERT/DELETE statement")),
    };

    walk.table_names_cacheable()?;
    let mut key = String::with_capacity(64);
    key.push_str(current_db);
    key.push('|');
    key.push_str(&statement.restore());
    Ok(ParameterizedSelect {
        key,
        statement,
        values: walk.params,
    })
}

impl Walk<'_> {
    /// Go `extractTableNames` for a DML target: the dotted name path is
    /// lowered exactly like a SELECT's table reference.
    fn dml_table(&mut self, name: &[String]) -> Result<(), Refusal> {
        let Some(table_name) = name.last() else {
            return Err(Refusal::walk("some column is not found in table schema"));
        };
        let schema = if name.len() >= 2 {
            &name[name.len() - 2]
        } else {
            self.current_db
        };
        self.tables
            .push((go_simple_lowercase(schema), go_simple_lowercase(table_name)));
        Ok(())
    }

    /// Go's checker walk over `*ast.UpdateStmt` plus the replacer pass: SET
    /// values and ON DUPLICATE values are parameterized, ORDER BY and LIMIT
    /// literals stay verbatim.
    fn update(&mut self, update: &mut tidb_ast::UpdateStmt) -> Result<(), Refusal> {
        // Go's visitor enters `UpdateStmt.With` first, a node its checker
        // does not list.
        if update.with.is_some() {
            return Err(Refusal::walk("query has some unsupported Node"));
        }
        for assignment in &mut update.assignments {
            self.check_expr(&assignment.value)?;
        }
        if let Some(where_clause) = update.where_clause.as_ref() {
            self.check_inside_filter(|walk| walk.check_expr(where_clause))?;
        }
        self.order_by(&update.order_by)?;
        if let Some(limit) = update.limit.as_ref() {
            if !self.enable_param_limit {
                return Err(Refusal::walk("query has 'limit ?' is un-cacheable"));
            }
            if let Some(offset) = limit.offset.as_ref() {
                self.check_expr(offset)?;
            }
            self.check_expr(&limit.count)?;
        }
        for assignment in &mut update.assignments {
            self.replace_expr(&mut assignment.value)?;
        }
        if let Some(where_clause) = update.where_clause.as_mut() {
            self.replace_expr(where_clause)?;
        }
        Ok(())
    }

    /// Go's checker walk over `*ast.InsertStmt` plus the replacer pass:
    /// VALUES rows and ON DUPLICATE values are parameterized; the SELECT
    /// source (when present) goes through the full SELECT walk.
    fn insert(&mut self, insert: &mut tidb_ast::InsertStmt) -> Result<(), Refusal> {
        for row in &mut insert.rows {
            for value in row.iter_mut() {
                self.check_expr(value)?;
            }
        }
        for assignment in &mut insert.on_duplicate {
            self.check_expr(&assignment.value)?;
        }
        // Go's `extractTableNames` for the INSERT-SELECT source accumulates
        // the target + source tables and refuses when the combined count
        // exceeds two (plan_cacheable_checker.go:394).
        if self.tables.len() > 2 {
            return Err(Refusal::walk(
                "queries that have more than 2 tables are not supported",
            ));
        }
        for row in &mut insert.rows {
            for value in row.iter_mut() {
                self.replace_expr(value)?;
            }
        }
        for assignment in &mut insert.on_duplicate {
            self.replace_expr(&mut assignment.value)?;
        }
        Ok(())
    }

    /// Go's checker walk over `*ast.DeleteStmt` plus the replacer pass.
    fn delete(&mut self, delete: &mut tidb_ast::DeleteStmt) -> Result<(), Refusal> {
        // As for UPDATE: `DeleteStmt.With` is an unlisted node.
        if delete.with.is_some() {
            return Err(Refusal::walk("query has some unsupported Node"));
        }
        if let Some(where_clause) = delete.where_clause.as_ref() {
            self.check_inside_filter(|walk| walk.check_expr(where_clause))?;
        }
        self.order_by(&delete.order_by)?;
        if let Some(limit) = delete.limit.as_ref() {
            if !self.enable_param_limit {
                return Err(Refusal::walk("query has 'limit ?' is un-cacheable"));
            }
            if let Some(offset) = limit.offset.as_ref() {
                self.check_expr(offset)?;
            }
            self.check_expr(&limit.count)?;
        }
        if let Some(where_clause) = delete.where_clause.as_mut() {
            self.replace_expr(where_clause)?;
        }
        Ok(())
    }
}

/// Go `GetParamSQLFromAST`: clone the admitted statement, replace its filter
/// literals with ordered markers, and retain the literal datums for physical
/// planning and later recursive rebuilds.
pub fn get_param_sql_from_ast(
    stmt: &Stmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    enable_param_limit: bool,
    enable_for_dml: bool,
    string_collation: Collation,
    max_num_param: usize,
) -> Result<ParameterizedSelect, Refusal> {
    let Stmt::Query(query) = stmt else {
        return Err(Refusal::fast("not a SELECT/UPDATE/INSERT/DELETE statement"));
    };
    let QueryStmt::Select(select) = &**query else {
        // Go reaches a `UNION` through `ast.SetOprStmt`, which is not in the
        // admitted node list.
        return Err(Refusal::walk("query has some unsupported Node"));
    };

    let mut walk = Walk {
        catalog,
        current_db,
        enable_param_limit,
        tables: Vec::new(),
        params: Vec::new(),
        string_collation,
        const_count: 0,
        sum_in_list_len: 0,
        filter_depth: 0,
        max_num_param,
    };
    // Go `NonPreparedPlanCacheableWithCtx`: a locking read is "not a SELECT
    // statement" only while `tidb_enable_non_prepared_plan_cache_for_dml` is
    // off; with it on the read is admitted like any SELECT.
    if select.lock.is_some() && !enable_for_dml {
        return Err(Refusal::fast("not a SELECT statement"));
    }
    let mut parameterized = (**select).clone();
    walk.fast_check(&parameterized)?;
    walk.select(&mut parameterized)?;

    let mut key = String::with_capacity(64);
    key.push_str(current_db);
    key.push('|');
    let statement = Stmt::Query(tidb_ast::NodeBox::new(QueryStmt::Select(Box::new(
        parameterized,
    ))));
    key.push_str(&statement.restore());
    Ok(ParameterizedSelect {
        key,
        statement,
        values: walk.params,
    })
}

/// The admitting walk: Go's `nonPreparedPlanCacheableChecker`, whose `Enter`
/// admits a fixed node list and refuses everything else.
struct Walk<'a> {
    catalog: &'a SessionExtendedInfoSchema,
    current_db: &'a str,
    enable_param_limit: bool,
    /// Go `checker.tableNodes`: the at-most-two tables the query reads, whose
    /// schemas the filter-column rule consults.
    tables: Vec<(String, String)>,
    params: Vec<Datum>,
    string_collation: Collation,
    const_count: usize,
    /// Go accumulates the lengths of every IN-list before visiting their
    /// values, so its dedicated refusal wins over the general literal cap.
    sum_in_list_len: usize,
    /// Go `checker.filterCnt`: non-zero while inside a filter, which is what
    /// makes the column-type rule apply to `col = 1` but not to a select
    /// field.
    filter_depth: usize,
    max_num_param: usize,
}

impl Walk<'_> {
    /// Go `isSelectStmtNonPrepCacheableFastCheck`: the clause-level refusals
    /// taken before any node is visited.
    fn fast_check(&mut self, select: &SelectStmt) -> Result<(), Refusal> {
        if select.kind != tidb_ast::SelectStatementKind::Select {
            return Err(Refusal::fast("not a select statement"));
        }
        if select.having.is_some() {
            return Err(Refusal::fast(
                "queries with HAVING clauses are not supported",
            ));
        }
        if !select.windows.is_empty() {
            return Err(Refusal::fast(
                "queries using window-functions are not supported",
            ));
        }
        if select.limit.is_some() && !self.enable_param_limit {
            return Err(Refusal::fast(
                "queries with limit clauses are not supported",
            ));
        }
        if select.with.is_some() {
            // A CTE reaches Go as a sub-query node, which the visitor refuses.
            return Err(Refusal::fast(
                "queries that have sub-queries are not supported",
            ));
        }
        let Some(from) = &select.from else {
            return Err(Refusal::fast(
                "queries that have sub-queries are not supported",
            ));
        };
        self.extract_table_names(from)
    }

    /// Go `extractTableNames`: collects the plain table names of the join
    /// tree, refusing a derived table and a tree wider than two tables.
    fn extract_table_names(&mut self, join: &tidb_ast::Join) -> Result<(), Refusal> {
        self.extract_from_node(&join.left)?;
        if let Some(right) = &join.right {
            self.extract_from_node(right)?;
        }
        if self.tables.len() > 2 {
            return Err(Refusal::walk(
                "queries that have more than 2 tables are not supported",
            ));
        }
        Ok(())
    }

    fn extract_from_node(&mut self, node: &JoinNode) -> Result<(), Refusal> {
        match node {
            JoinNode::Table(table) => {
                let Some(table_name) = table.name.last() else {
                    return Err(Refusal::fast("some column is not found in table schema"));
                };
                let schema = if table.name.len() >= 2 {
                    &table.name[table.name.len() - 2]
                } else {
                    self.current_db
                };
                self.tables
                    .push((go_simple_lowercase(schema), go_simple_lowercase(table_name)));
                Ok(())
            }
            JoinNode::Join(inner) => self.extract_table_names(inner),
            // Go's `extractTableNames` default arm: a derived table is a
            // sub-query as far as this checker is concerned.
            JoinNode::Derived { .. } => Err(Refusal::fast(
                "queries that have sub-queries are not supported",
            )),
        }
    }

    /// Walks the admitted `SELECT` shape, parameterizing as it goes.
    fn select(&mut self, select: &mut SelectStmt) -> Result<(), Refusal> {
        self.table_names_cacheable()?;
        // Go first runs `nonPreparedPlanCacheableChecker` over the complete
        // tree. Its later `paramReplacer` skips SelectField, GroupByClause,
        // OrderByClause and Limit, so validate those nodes without replacing
        // their literals.
        for field in select.fields.fields() {
            if let SelectField::Expr { expr, .. } = field {
                self.check_expr(expr)?;
            }
        }
        if let Some(from) = select.from.as_mut() {
            self.check_join_conditions(from)?;
        }
        if let Some(where_clause) = select.where_clause.as_ref() {
            self.check_expr(where_clause)?;
        }
        self.group_by(&select.group_by)?;
        self.order_by(&select.order_by)?;
        if let Some(limit) = select.limit.as_ref() {
            if !self.enable_param_limit {
                return Err(Refusal::walk("query has 'limit ?' is un-cacheable"));
            }
            if let Some(offset) = limit.offset.as_ref() {
                self.check_expr(offset)?;
            }
            self.check_expr(&limit.count)?;
        }

        // This is the separate `paramReplacer` pass. Only predicates are
        // mutable here; the four skipped Go node types above stay verbatim.
        if let Some(from) = select.from.as_mut() {
            self.replace_join_conditions(from)?;
        }
        if let Some(where_clause) = select.where_clause.as_mut() {
            self.replace_expr(where_clause)?;
        }
        Ok(())
    }

    fn check_join_conditions(&mut self, join: &tidb_ast::Join) -> Result<(), Refusal> {
        if let Some(on) = join.on.as_ref() {
            self.check_expr(on)?;
        }
        if let JoinNode::Join(inner) = &join.left {
            self.check_join_conditions(inner)?;
        }
        if let Some(JoinNode::Join(inner)) = join.right.as_ref() {
            self.check_join_conditions(inner)?;
        }
        Ok(())
    }

    fn replace_join_conditions(&mut self, join: &mut tidb_ast::Join) -> Result<(), Refusal> {
        if let Some(on) = join.on.as_mut() {
            self.replace_expr(on)?;
        }
        if let JoinNode::Join(inner) = &mut join.left {
            self.replace_join_conditions(inner)?;
        }
        if let Some(JoinNode::Join(inner)) = join.right.as_mut() {
            self.replace_join_conditions(inner)?;
        }
        Ok(())
    }

    /// Go's `GroupByClause` arm: only bare columns are admitted.
    fn group_by(&mut self, items: &[GroupByItem]) -> Result<(), Refusal> {
        for item in items {
            if !matches!(item.expr, Expr::Column(_)) {
                return Err(Refusal::fast("only support group by {columns}'"));
            }
        }
        Ok(())
    }

    /// Go's `OrderByClause` arm: only bare columns are admitted.
    fn order_by(&mut self, items: &[OrderItem]) -> Result<(), Refusal> {
        for item in items {
            if !matches!(item.expr, Expr::Column(_)) {
                return Err(Refusal::fast("only support order by {columns}'"));
            }
        }
        Ok(())
    }

    /// Go raises `filterCnt` only for the four AST node types returned by
    /// `isFilterNode`, not for the WHERE/ON clause as a whole.
    fn check_inside_filter(
        &mut self,
        check: impl FnOnce(&mut Self) -> Result<(), Refusal>,
    ) -> Result<(), Refusal> {
        self.filter_depth += 1;
        let result = check(self);
        self.filter_depth -= 1;
        result
    }

    /// Go `nonPreparedPlanCacheableChecker.Enter`: admitted nodes recurse and
    /// every AST shape absent from its switch is refused.
    fn check_expr(&mut self, expr: &Expr) -> Result<(), Refusal> {
        match expr {
            Expr::Column(name) => self.column(name),
            Expr::Int(_)
            | Expr::Decimal(_)
            | Expr::Float(_)
            | Expr::String(_)
            | Expr::RawString(_)
            | Expr::Bool(_) => self.count_literal(),
            // Go refuses a NULL constant explicitly: `not-null-col = NULL`
            // folds to a table-dual plan that `not-null-col = ?` cannot
            // reproduce, so sharing the entry would change the plan shape.
            Expr::Null => Err(Refusal::walk("query has null constants")),
            // Go refuses BIT and HEX literals "for safety" -- their binary
            // kind survives parameterization badly.
            Expr::Hex(_) | Expr::Bit(_) => Err(Refusal::walk(
                "query has BIT / HEX literals are not supported",
            )),
            // Go refuses an underscore-charset value for the same reason.
            Expr::CharsetString { .. } | Expr::CharsetBinary { .. } => {
                Err(Refusal::walk("query has values with under-score charset"))
            }
            // Go's checker refuses a user-defined variable outright.
            Expr::UserVar(_) => Err(Refusal::walk(
                "query has user-defined variables is un-cacheable",
            )),
            Expr::Paren(inner) => self.check_expr(inner),
            Expr::Binary(_, left, right) => self.check_inside_filter(|walk| {
                walk.check_expr(left)?;
                walk.check_expr(right)
            }),
            Expr::Is {
                expr,
                target: IsTarget::Null,
                ..
            } => self.check_inside_filter(|walk| walk.check_expr(expr)),
            Expr::Between {
                expr, low, high, ..
            } => self.check_inside_filter(|walk| {
                walk.check_expr(expr)?;
                walk.check_expr(low)?;
                walk.check_expr(high)
            }),
            Expr::In { expr, list, .. } => {
                self.sum_in_list_len = self.sum_in_list_len.saturating_add(list.len());
                if self.sum_in_list_len > self.max_num_param {
                    return Err(Refusal::walk("too many values in in-list"));
                }
                self.check_inside_filter(|walk| {
                    walk.check_expr(expr)?;
                    for item in list {
                        walk.check_expr(item)?;
                    }
                    Ok(())
                })
            }
            Expr::Row(items) => {
                for item in items {
                    self.check_expr(item)?;
                }
                Ok(())
            }
            Expr::Func { name, args, .. } | Expr::GenericFuncCall { name, args, .. } => {
                if UNCACHEABLE_FUNCTIONS.contains(&name.to_ascii_lowercase().as_str()) {
                    return Err(Refusal::walk("query has un-cacheable functions"));
                }
                for arg in args {
                    self.check_expr(arg)?;
                }
                Ok(())
            }
            Expr::Aggregate { args, .. } => {
                for arg in args {
                    self.check_expr(arg)?;
                }
                Ok(())
            }
            Expr::GroupConcat { args, order_by, .. } => {
                for arg in args {
                    self.check_expr(arg)?;
                }
                for item in order_by {
                    self.check_expr(&item.expr)?;
                }
                Ok(())
            }
            // Rust gives these Go FuncCallExpr spellings dedicated variants
            // solely to preserve their custom SQL restore syntax. Their
            // cache admission remains FuncCallExpr admission. A TRIM
            // direction is the exception: Go exposes its TrimDirectionExpr
            // child, which is not in the admitted-node switch.
            Expr::Position { substr, str } => {
                self.check_expr(substr)?;
                self.check_expr(str)
            }
            Expr::Trim {
                expr,
                remstr,
                direction: None,
            } => {
                self.check_expr(expr)?;
                if let Some(remstr) = remstr {
                    self.check_expr(remstr)?;
                }
                Ok(())
            }
            Expr::MemberOf { expr, array } => {
                self.check_expr(expr)?;
                self.check_expr(array)
            }
            // A sub-query in any of its shapes is Go's
            // "query has sub-queries is un-cacheable".
            Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::CompareSubquery { .. } => {
                Err(Refusal::walk("query has sub-queries is un-cacheable"))
            }
            // Everything else -- unary operations, CASE, casts, windows,
            // LIKE, REGEXP, system variables, and specialized nodes absent
            // from Go's admitted-node switch -- lands on Go's final
            // "unexpected cases" arm. This catch-all is the whole reason the
            // port is safe: a node this walk has never been taught cannot
            // silently join a shared key.
            _ => Err(Refusal::walk("query has some unsupported Node")),
        }
    }

    /// Go `paramReplacer`: replaces values everywhere except SelectField,
    /// GroupByClause, OrderByClause and Limit. The checker above has already
    /// proved this tree consists only of admitted nodes.
    fn replace_expr(&mut self, expr: &mut Expr) -> Result<(), Refusal> {
        match expr {
            Expr::Int(_)
            | Expr::Decimal(_)
            | Expr::Float(_)
            | Expr::String(_)
            | Expr::RawString(_)
            | Expr::Bool(_) => self.parameterize(expr),
            Expr::Column(_) => Ok(()),
            Expr::Paren(inner) => self.replace_expr(inner),
            Expr::Binary(_, left, right) => {
                self.replace_expr(left)?;
                self.replace_expr(right)
            }
            Expr::Is {
                expr,
                target: IsTarget::Null,
                ..
            } => self.replace_expr(expr),
            Expr::Between {
                expr, low, high, ..
            } => {
                self.replace_expr(expr)?;
                self.replace_expr(low)?;
                self.replace_expr(high)
            }
            Expr::In { expr, list, .. } => {
                self.replace_expr(expr)?;
                for item in list {
                    self.replace_expr(item)?;
                }
                Ok(())
            }
            Expr::Row(items) => {
                for item in items {
                    self.replace_expr(item)?;
                }
                Ok(())
            }
            Expr::Func { name, args, .. } | Expr::GenericFuncCall { name, args, .. } => {
                let format_function = matches!(
                    name.to_ascii_uppercase().as_str(),
                    "DATE_FORMAT" | "STR_TO_DATE" | "TIME_FORMAT" | "FROM_UNIXTIME"
                );
                for (index, arg) in args.iter_mut().enumerate() {
                    if !format_function || index == 0 {
                        self.replace_expr(arg)?;
                    }
                }
                Ok(())
            }
            Expr::Aggregate { args, .. } => {
                for arg in args {
                    self.replace_expr(arg)?;
                }
                Ok(())
            }
            Expr::GroupConcat { args, order_by, .. } => {
                for arg in args {
                    self.replace_expr(arg)?;
                }
                for item in order_by {
                    self.replace_expr(&mut item.expr)?;
                }
                Ok(())
            }
            Expr::Position { substr, str } => {
                self.replace_expr(substr)?;
                self.replace_expr(str)
            }
            Expr::Trim {
                expr,
                remstr,
                direction: None,
            } => {
                self.replace_expr(expr)?;
                if let Some(remstr) = remstr {
                    self.replace_expr(remstr)?;
                }
                Ok(())
            }
            Expr::MemberOf { expr, array } => {
                self.replace_expr(expr)?;
                self.replace_expr(array)
            }
            _ => Err(Refusal::walk("query has some unsupported Node")),
        }
    }

    fn count_literal(&mut self) -> Result<(), Refusal> {
        self.const_count += 1;
        if self.const_count > self.max_num_param {
            Err(Refusal::walk("query has too many constants"))
        } else {
            Ok(())
        }
    }

    /// Go's `ColumnName` arm: inside a filter the column must resolve, and its
    /// type must not be one whose comparison Go declines to parameterize.
    fn column(&mut self, name: &[String]) -> Result<(), Refusal> {
        if self.filter_depth == 0 {
            return Ok(());
        }
        let Some(column) = name.last() else {
            return Err(Refusal::walk("some column is not found in table schema"));
        };
        let mut found = false;
        for (schema, table) in &self.tables {
            let Some(entry) = self.catalog.table_in(schema, table) else {
                continue;
            };
            for (held, field_type) in entry.column_types() {
                if !held.go_lower_eq(column) {
                    continue;
                }
                found = true;
                if is_unparameterizable_type(&field_type) {
                    return Err(Refusal::walk(
                        "query has some filters with JSON, Enum, Set or Bit columns",
                    ));
                }
            }
        }
        if found {
            Ok(())
        } else {
            Err(Refusal::walk("some column is not found in table schema"))
        }
    }

    /// Go's `TableName` arm plus `checkTableCacheable`: a system schema, a
    /// view and a sequence are all refused before any expression is examined.
    fn table_names_cacheable(&self) -> Result<(), Refusal> {
        for (schema, table) in &self.tables {
            if is_system_schema(schema) {
                return Err(Refusal::walk("access tables in system schema"));
            }
            let Some(entry) = self.catalog.table_in(schema, table) else {
                // An unknown table is not this check's error to report -- the
                // statement's own resolution will raise it -- but it must not
                // be cached either.
                return Err(Refusal::walk("some column is not found in table schema"));
            };
            if entry.is_view() {
                return Err(Refusal::walk("queries that access views are not supported"));
            }
            if entry.is_sequence() {
                return Err(Refusal::walk("queries that access in-memory tables"));
            }
        }
        Ok(())
    }

    /// Replaces one literal by a `?` marker and counts it against Go's
    /// `maxNumberParam`. Literal types deliberately do not enter the
    /// parameterized-SQL descriptor; the shared physical cache matches them
    /// through its parameter-type key, as Go's `SessionPlanCache.Get` does.
    fn parameterize(&mut self, expr: &mut Expr) -> Result<(), Refusal> {
        let value = match expr {
            Expr::Int(text) => text
                .parse::<i64>()
                .map(Datum::Int)
                .or_else(|_| text.parse::<u64>().map(Datum::UInt))
                .map_err(|_| Refusal::walk("query has some unsupported Node"))?,
            Expr::Decimal(text) => Datum::Decimal(Decimal::from_literal(text)),
            Expr::Float(value) => Datum::Real(*value),
            Expr::String(value) => {
                Datum::String(StringDatum::new(value.as_bytes(), self.string_collation))
            }
            Expr::RawString(value) => {
                Datum::String(StringDatum::new(value.as_bytes(), self.string_collation))
            }
            Expr::Bool(value) => Datum::Int(i64::from(*value)),
            _ => return Ok(()),
        };
        self.params.push(value);
        *expr = Expr::ParamMarker {
            offset: 0,
            order: self.params.len() - 1,
            in_execute: false,
            value: None,
            projection_offset: 0,
        };
        Ok(())
    }
}

/// Go's `mysql.TypeJSON / TypeEnum / TypeSet / TypeBit` filter-column refusal.
fn is_unparameterizable_type(field_type: &tidb_datatype::FieldType) -> bool {
    use tidb_datatype::FieldTypeCode;
    matches!(
        field_type.code(),
        FieldTypeCode::Json | FieldTypeCode::Enum | FieldTypeCode::Set | FieldTypeCode::Bit
    )
}
