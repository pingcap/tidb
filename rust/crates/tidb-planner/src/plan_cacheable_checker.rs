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

//! Prepared-plan-cache admission: Go `plan_cacheable_checker.go`'s
//! `IsASTCacheable`, decided at `PREPARE`.
//!
//! The retained physical trees and execute-time recursive rebuild live in
//! the executor's `PreparedSelectPlan`; this module only decides whether
//! a statement may enter that cache.

use std::any::Any;

use tidb_ast::{Visitable, Visitor};
use tidb_infoschema::{Catalog, TableEntry};
use tidb_hack::{GoCaseCompare, GoToLower};

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
    pub catalog: &'a Catalog,
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
                // Go `*ast.VariableExpr`: a user variable's value is not in
                // the cache key.
                tidb_ast::Expr::UserVar(_) | tidb_ast::Expr::Assign { .. } => {
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
