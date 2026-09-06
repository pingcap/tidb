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

//! Go-compatible non-prepared physical-plan caching.
//!
//! Mirrors the non-prepared half of Go `pkg/planner/core/plan_cacheable_checker.go`
//! (`NonPreparedPlanCacheableWithCtx`, `isSelectStmtNonPrepCacheableFastCheck`,
//! `extractTableNames`, and the `nonPreparedPlanCacheableChecker` visitor)
//! plus Go `getPlanFromNonPreparedPlanCache`: literal parameterization creates
//! one retained marker-bearing statement, and the same `PreparedSelectPlan`
//! used by SQL/binary PREPARE owns its physical entries. Parameter types,
//! schema/statistics versions, matched binding SQL, admission, recursive range
//! rebuild, and executor construction therefore have one implementation.

use tidb_ast::{
    Expr, GroupByItem, IsTarget, JoinNode, OrderItem, QueryStmt, SelectField, SelectStmt, Stmt,
};
use tidb_datatype::{Collation, Datum, Decimal, StringDatum};
use tidb_executor::{Catalog, PreparedDmlPlan, PreparedSelectPlan};
use tidb_mysql::to_lowercase as go_simple_lowercase;
use tidb_planner::metrics::{
    non_prep_plan_cache_unsupported_counter, plan_cache_hit_counter, plan_cache_miss_counter,
};
use tidb_util::filter::is_system_schema;
use tidb_util::kvcache::SimpleLruCache;

/// Go `getMaxParamLimit`'s default: `PlanCacheMaxParamNum` (200).
const MAX_PARAM_NUM: usize = 200;

/// Why a statement may not enter the non-prepared plan cache.
///
/// The payload is Go's own reason string, verbatim, so a future
/// `tidb_last_plan_cache_reason`-style surface reports what TiDB reports and
/// so a divergence in the refusal itself is visible rather than silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Refusal(pub &'static str);

/// One parameterized statement and its execute-time literal values.
pub(crate) struct ParameterizedSelect {
    pub(crate) key: String,
    pub(crate) statement: Stmt,
    pub(crate) values: Vec<Datum>,
}

/// The per-session parameterized-statement LRU. Each value owns the same
/// general physical-plan cache used by PREPARE.
#[derive(Default)]
pub(crate) struct NonPreparedPlanCache {
    plans: Option<SimpleLruCache<String, std::sync::Arc<PreparedSelectPlan>>>,
    capacity: usize,
}

impl NonPreparedPlanCache {
    pub(crate) fn resize(&mut self, capacity: usize) {
        if self.capacity == capacity {
            return;
        }
        match self.plans.as_mut() {
            Some(plans) if capacity > 0 => {
                let _ = plans.set_capacity(capacity);
            }
            _ => self.plans = (capacity > 0).then(|| SimpleLruCache::new(capacity)),
        }
        self.capacity = capacity;
    }

    pub(crate) fn get(&mut self, key: &str) -> Option<std::sync::Arc<PreparedSelectPlan>> {
        self.plans.as_mut()?.get(key).cloned()
    }

    pub(crate) fn put(&mut self, key: String, plan: std::sync::Arc<PreparedSelectPlan>) {
        if let Some(plans) = self.plans.as_mut() {
            plans.put(key, plan);
        }
    }
}

/// The per-session parameterized-DML-statement LRU over the same
/// general physical-plan cache PREPARE uses for DML roots.
#[derive(Default)]
pub(crate) struct NonPreparedDmlCache {
    plans: Option<SimpleLruCache<String, std::sync::Arc<PreparedDmlPlan>>>,
    capacity: usize,
}

impl NonPreparedDmlCache {
    pub(crate) fn resize(&mut self, capacity: usize) {
        if self.capacity == capacity {
            return;
        }
        match self.plans.as_mut() {
            Some(plans) if capacity > 0 => {
                let _ = plans.set_capacity(capacity);
            }
            _ => self.plans = (capacity > 0).then(|| SimpleLruCache::new(capacity)),
        }
        self.capacity = capacity;
    }

    pub(crate) fn get(&mut self, key: &str) -> Option<std::sync::Arc<PreparedDmlPlan>> {
        self.plans.as_mut()?.get(key).cloned()
    }

    pub(crate) fn put(&mut self, key: String, plan: std::sync::Arc<PreparedDmlPlan>) {
        if let Some(plans) = self.plans.as_mut() {
            plans.put(key, plan);
        }
    }
}

/// Go `NonPreparedPlanCacheableWithCtx`'s UPDATE/INSERT/DELETE arms plus the
/// checker walk and `paramReplacer` over the DML tree: SET values, `VALUES`
/// rows, `ON DUPLICATE KEY UPDATE` values, and the WHERE predicate are
/// parameterized; ORDER BY and LIMIT literals stay verbatim (Go's replacer
/// skips those node kinds so different limit values plan differently).
pub(crate) fn parameterize_dml(
    stmt: &Stmt,
    catalog: &Catalog,
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
                    return Err(Refusal("not support update statement with table hints"));
                }
                if matches!(update.kind, tidb_ast::UpdateKind::Multi { .. }) {
                    return Err(Refusal("not support multiple tables update statements"));
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
                    return Err(Refusal("not support insert statement with table hints"));
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
                            return Err(Refusal("not a SELECT statement"));
                        };
                        let tidb_ast::QueryStmt::Select(select) = source else {
                            return Err(Refusal("not a select statement"));
                        };
                        walk.select(select)?;
                    } else {
                        let n_rows = insert.rows.len();
                        let n_cols = insert.rows.first().map_or(0, Vec::len);
                        if n_rows * n_cols > walk.max_num_param {
                            return Err(Refusal("too many values in the insert statement"));
                        }
                        walk.insert(insert)?;
                    }
                }
                statement
            }
            tidb_ast::DmlStmt::Delete(delete) => {
                if !delete.hints.is_empty() {
                    // Go's own message names INSERT for every statement kind.
                    return Err(Refusal("not support insert statement with table hints"));
                }
                if matches!(delete.kind, tidb_ast::DeleteKind::Multi { .. }) {
                    return Err(Refusal("not support multiple tables delete statements"));
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
            _ => return Err(Refusal("not a SELECT/UPDATE/INSERT/DELETE statement")),
        },
        _ => return Err(Refusal("not a SELECT/UPDATE/INSERT/DELETE statement")),
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
            return Err(Refusal("some column is not found in table schema"));
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
        for assignment in &mut update.assignments {
            self.check_expr(&assignment.value)?;
        }
        if let Some(where_clause) = update.where_clause.as_ref() {
            self.check_inside_filter(|walk| walk.check_expr(where_clause))?;
        }
        self.order_by(&update.order_by)?;
        if let Some(limit) = update.limit.as_ref() {
            if !self.enable_param_limit {
                return Err(Refusal("query has 'limit ?' is un-cacheable"));
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
        if let Some(where_clause) = delete.where_clause.as_ref() {
            self.check_inside_filter(|walk| walk.check_expr(where_clause))?;
        }
        self.order_by(&delete.order_by)?;
        if let Some(limit) = delete.limit.as_ref() {
            if !self.enable_param_limit {
                return Err(Refusal("query has 'limit ?' is un-cacheable"));
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
pub(crate) fn parameterize_select(
    stmt: &Stmt,
    catalog: &Catalog,
    current_db: &str,
    enable_param_limit: bool,
    string_collation: Collation,
    max_num_param: usize,
) -> Result<ParameterizedSelect, Refusal> {
    let Stmt::Query(query) = stmt else {
        return Err(Refusal("not a SELECT/UPDATE/INSERT/DELETE statement"));
    };
    let QueryStmt::Select(select) = &**query else {
        // Go reaches a `UNION` through `ast.SetOprStmt`, which is not in the
        // admitted node list.
        return Err(Refusal("query has some unsupported Node"));
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
    catalog: &'a Catalog,
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
            return Err(Refusal("not a select statement"));
        }
        if select.lock.is_some() {
            // Go refuses a locking read at the statement-kind gate:
            // `selStmt.LockInfo != nil` is "not a SELECT statement".
            return Err(Refusal("not a SELECT statement"));
        }
        if select.having.is_some() {
            return Err(Refusal("queries with HAVING clauses are not supported"));
        }
        if !select.windows.is_empty() {
            return Err(Refusal("queries using window-functions are not supported"));
        }
        if select.limit.is_some() && !self.enable_param_limit {
            return Err(Refusal("queries with limit clauses are not supported"));
        }
        if select.with.is_some() {
            // A CTE reaches Go as a sub-query node, which the visitor refuses.
            return Err(Refusal("queries that have sub-queries are not supported"));
        }
        let Some(from) = &select.from else {
            return Err(Refusal("queries that have sub-queries are not supported"));
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
            return Err(Refusal(
                "queries that have more than 2 tables are not supported",
            ));
        }
        Ok(())
    }

    fn extract_from_node(&mut self, node: &JoinNode) -> Result<(), Refusal> {
        match node {
            JoinNode::Table(table) => {
                let Some(table_name) = table.name.last() else {
                    return Err(Refusal("some column is not found in table schema"));
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
            JoinNode::Derived { .. } => {
                Err(Refusal("queries that have sub-queries are not supported"))
            }
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
                return Err(Refusal("query has 'limit ?' is un-cacheable"));
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
                return Err(Refusal("only support group by {columns}'"));
            }
        }
        Ok(())
    }

    /// Go's `OrderByClause` arm: only bare columns are admitted.
    fn order_by(&mut self, items: &[OrderItem]) -> Result<(), Refusal> {
        for item in items {
            if !matches!(item.expr, Expr::Column(_)) {
                return Err(Refusal("only support order by {columns}'"));
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
            Expr::Null => Err(Refusal("query has null constants")),
            // Go refuses BIT and HEX literals "for safety" -- their binary
            // kind survives parameterization badly.
            Expr::Hex(_) | Expr::Bit(_) => {
                Err(Refusal("query has BIT / HEX literals are not supported"))
            }
            // Go refuses an underscore-charset value for the same reason.
            Expr::CharsetString { .. } | Expr::CharsetBinary { .. } => {
                Err(Refusal("query has values with under-score charset"))
            }
            // Go's checker refuses a user-defined variable outright.
            Expr::UserVar(_) => Err(Refusal("query has user-defined variables is un-cacheable")),
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
                    return Err(Refusal("too many values in in-list"));
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
                if is_uncacheable_function(name) {
                    return Err(Refusal("query has un-cacheable functions"));
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
            | Expr::CompareSubquery { .. } => Err(Refusal("query has sub-queries is un-cacheable")),
            // Everything else -- unary operations, CASE, casts, windows,
            // LIKE, REGEXP, system variables, and specialized nodes absent
            // from Go's admitted-node switch -- lands on Go's final
            // "unexpected cases" arm. This catch-all is the whole reason the
            // port is safe: a node this walk has never been taught cannot
            // silently join a shared key.
            _ => Err(Refusal("query has some unsupported Node")),
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
            _ => Err(Refusal("query has some unsupported Node")),
        }
    }

    fn count_literal(&mut self) -> Result<(), Refusal> {
        self.const_count += 1;
        if self.const_count > self.max_num_param {
            Err(Refusal("query has too many constants"))
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
            return Err(Refusal("some column is not found in table schema"));
        };
        let mut found = false;
        for (schema, table) in &self.tables {
            let Some(entry) = self.catalog.table_in(schema, table) else {
                continue;
            };
            for (held, field_type) in entry.column_types() {
                if !held.eq_ignore_ascii_case(column) {
                    continue;
                }
                found = true;
                if is_unparameterizable_type(&field_type) {
                    return Err(Refusal(
                        "query has some filters with JSON, Enum, Set or Bit columns",
                    ));
                }
            }
        }
        if found {
            Ok(())
        } else {
            Err(Refusal("some column is not found in table schema"))
        }
    }

    /// Go's `TableName` arm plus `checkTableCacheable`: a system schema, a
    /// view and a sequence are all refused before any expression is examined.
    fn table_names_cacheable(&self) -> Result<(), Refusal> {
        for (schema, table) in &self.tables {
            if is_system_schema(schema) {
                return Err(Refusal("access tables in system schema"));
            }
            let Some(entry) = self.catalog.table_in(schema, table) else {
                // An unknown table is not this check's error to report -- the
                // statement's own resolution will raise it -- but it must not
                // be cached either.
                return Err(Refusal("some column is not found in table schema"));
            };
            if entry.is_view() {
                return Err(Refusal("queries that access views are not supported"));
            }
            if entry.is_sequence() {
                return Err(Refusal("queries that access in-memory tables"));
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
                .map_err(|_| Refusal("query has some unsupported Node"))?,
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

/// Go `expression.UnCacheableFunctions` at the pinned source commit.
fn is_uncacheable_function(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "DATABASE"
            | "CURRENT_USER"
            | "CURRENT_ROLE"
            | "CURRENT_RESOURCE_GROUP"
            | "USER"
            | "CONNECTION_ID"
            | "LAST_INSERT_ID"
            | "ROW_COUNT"
            | "VERSION"
            | "LIKE"
            | "JSON_OBJECT"
            | "JSON_ARRAY"
            | "COALESCE"
            | "CONVERT"
            | "TIME"
            | "DATE"
            | "TIMESTAMP"
            | "AES_ENCRYPT"
            | "AES_DECRYPT"
    )
}

/// Go's `mysql.TypeJSON / TypeEnum / TypeSet / TypeBit` filter-column refusal.
fn is_unparameterizable_type(field_type: &tidb_datatype::FieldType) -> bool {
    use tidb_datatype::FieldTypeCode;
    matches!(
        field_type.code(),
        FieldTypeCode::Json | FieldTypeCode::Enum | FieldTypeCode::Set | FieldTypeCode::Bit
    )
}

impl crate::Session {
    /// Parameterizes one admitted non-prepared SELECT. Refused statements use
    /// the ordinary planner and publish no cache hit.
    pub(crate) fn parameterize_non_prepared_select(
        &mut self,
        stmt: &Stmt,
    ) -> Option<ParameterizedSelect> {
        if !self.non_prepared_plan_cache_enabled() {
            return None;
        }
        let capacity = self.non_prepared_plan_cache_capacity();
        self.non_prepared_plan_cache.resize(capacity);
        let enable_param_limit = self.session_bool("tidb_enable_plan_cache_for_param_limit", true);
        let string_collation = self
            .vars
            .get_system("collation_connection")
            .ok()
            .and_then(|name| Collation::from_name(&name))
            .unwrap_or(Collation::Utf8Mb4Bin);
        let configured_max = self
            .vars
            .optimizer_fix_control()
            .get_int_with_default(tidb_planner::fix_control::FIX_44823, MAX_PARAM_NUM as i64);
        let max_num_param = match configured_max {
            0 => usize::MAX,
            value if value > 0 => usize::try_from(value).unwrap_or(usize::MAX),
            _ => MAX_PARAM_NUM,
        };
        let catalog = self.catalog.lock().ok()?;
        parameterize_select(
            stmt,
            &catalog,
            &self.current_db,
            enable_param_limit,
            string_collation,
            max_num_param,
        )
        .ok()
    }

    /// Go's DML switch: `EnableNonPreparedPlanCacheForDML`, true by default.
    fn non_prepared_plan_cache_for_dml_enabled(&self) -> bool {
        self.session_bool("tidb_enable_non_prepared_plan_cache_for_dml", true)
    }

    /// Parameterizes one non-prepared UPDATE/INSERT/DELETE statement.
    pub(crate) fn parameterize_non_prepared_dml(
        &mut self,
        stmt: &Stmt,
    ) -> Option<ParameterizedSelect> {
        if !self.non_prepared_plan_cache_enabled()
            || !self.non_prepared_plan_cache_for_dml_enabled()
        {
            return None;
        }
        let capacity = self.non_prepared_plan_cache_capacity();
        self.non_prepared_dml_cache.resize(capacity);
        let enable_param_limit = self.session_bool("tidb_enable_plan_cache_for_param_limit", true);
        let string_collation = self
            .vars
            .get_system("collation_connection")
            .ok()
            .and_then(|name| Collation::from_name(&name))
            .unwrap_or(Collation::Utf8Mb4Bin);
        let configured_max = self
            .vars
            .optimizer_fix_control()
            .get_int_with_default(tidb_planner::fix_control::FIX_44823, MAX_PARAM_NUM as i64);
        let max_num_param = match configured_max {
            0 => usize::MAX,
            value if value > 0 => usize::try_from(value).unwrap_or(usize::MAX),
            _ => MAX_PARAM_NUM,
        };
        let catalog = self.catalog.lock().ok()?;
        parameterize_dml(
            stmt,
            &catalog,
            &self.current_db,
            enable_param_limit,
            string_collation,
            max_num_param,
        )
        .ok()
    }

    /// Generates or recursively rebuilds the retained DML root for one
    /// parameterized non-prepared statement, through the same plan object
    /// SQL and binary PREPARE use.
    pub(crate) fn bind_non_prepared_dml(
        &mut self,
        parameterized: &ParameterizedSelect,
        effective_statement: &Stmt,
        binding_sql: Option<&str>,
    ) -> Option<tidb_executor::PreparedDmlExecution> {
        if !self.non_prepared_plan_cache_allowed(effective_statement) {
            return None;
        }
        let environment = self.prepared_plan_cache_environment_for_binding(binding_sql)?;
        let plan = match self.non_prepared_dml_cache.get(&parameterized.key) {
            Some(plan) => plan,
            None => {
                let plan = {
                    let catalog = self.lock_catalog().ok()?;
                    tidb_executor::build_prepared_dml_plan(
                        &parameterized.statement,
                        parameterized.values.len(),
                        &catalog,
                        self.current_database(),
                    )
                    .ok()??
                };
                let plan = std::sync::Arc::new(plan);
                self.non_prepared_dml_cache
                    .put(parameterized.key.clone(), std::sync::Arc::clone(&plan));
                plan
            }
        };
        {
            let catalog = self.lock_catalog().ok()?;
            if let Some(execution) = plan.bind_cached_for_statement(
                &parameterized.values,
                &catalog,
                self.current_database(),
                &environment,
                effective_statement,
            ) {
                // Go `GetPlanFromPlanCache`'s hit arm.
                tidb_planner::metrics::plan_cache_hit_counter(true).inc();
                return Some(execution);
            }
        }
        // Go's miss arm re-plans; its miss counter records the replan.
        tidb_planner::metrics::plan_cache_miss_counter(true).inc();
        let ctx = self.statement_context(false);
        let catalog = self.lock_catalog().ok()?;
        plan.bind_for_statement(
            &parameterized.values,
            &catalog,
            self.current_database(),
            &ctx,
            &environment,
            effective_statement,
        )
    }

    /// Generates or recursively rebuilds the physical plan owned by one
    /// parameterized non-prepared statement. This is Go's
    /// `GetPlanFromPlanCache(..., isNonPrepared=true)` over the same Rust plan
    /// object SQL and binary PREPARE use.
    pub(crate) fn bind_non_prepared_select(
        &mut self,
        parameterized: &ParameterizedSelect,
        effective_statement: &Stmt,
        binding_sql: Option<&str>,
    ) -> Option<tidb_executor::PreparedSelectExecution> {
        if !self.non_prepared_plan_cache_allowed(effective_statement) {
            return None;
        }
        let environment = self.prepared_plan_cache_environment_for_binding(binding_sql)?;
        let plan = match self.non_prepared_plan_cache.get(&parameterized.key) {
            Some(plan) => plan,
            None => {
                let ctx = self.statement_context(false);
                let plan = {
                    let catalog = self.lock_catalog().ok()?;
                    tidb_executor::build_prepared_select_plan(
                        &parameterized.statement,
                        parameterized.values.len(),
                        &catalog,
                        self.current_database(),
                        &ctx,
                    )?
                };
                let plan = std::sync::Arc::new(plan);
                self.non_prepared_plan_cache
                    .put(parameterized.key.clone(), std::sync::Arc::clone(&plan));
                plan
            }
        };
        {
            let catalog = self.lock_catalog().ok()?;
            if let Some(execution) = plan.bind_cached_for_statement(
                &parameterized.values,
                &catalog,
                self.current_database(),
                &environment,
                effective_statement,
            ) {
                // Go `GetPlanFromPlanCache`'s hit arm.
                tidb_planner::metrics::plan_cache_hit_counter(true).inc();
                return Some(execution);
            }
        }
        // Go's miss arm re-plans; its miss counter records the replan.
        tidb_planner::metrics::plan_cache_miss_counter(true).inc();
        let ctx = self.statement_context(false);
        let catalog = self.lock_catalog().ok()?;
        plan.bind_for_statement(
            &parameterized.values,
            &catalog,
            self.current_database(),
            &ctx,
            &environment,
            effective_statement,
        )
    }

    /// Whether `@@last_plan_from_cache` should report a hit, which is the
    /// PRECEDING statement's outcome (Go `PrevFoundInPlanCache`).
    pub(crate) fn last_plan_from_cache(&self) -> bool {
        self.prev_found_in_plan_cache
    }

    fn non_prepared_plan_cache_enabled(&self) -> bool {
        self.session_bool("tidb_enable_non_prepared_plan_cache", false)
    }

    fn non_prepared_plan_cache_allowed(&self, statement: &Stmt) -> bool {
        let hints = crate::variables::parse_statement_hints_without_catalog(
            statement,
            self.current_database(),
        );
        if hints.ignore_plan_cache {
            return false;
        }
        let hint_only = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_PLAN_CACHE_STRATEGY)
            .is_ok_and(|strategy| {
                strategy.eq_ignore_ascii_case(
                    tidb_vardef::tidb_vars::TIDB_PLAN_CACHE_STRATEGY_HINT_ONLY,
                )
            });
        !hint_only || hints.use_plan_cache
    }

    fn non_prepared_plan_cache_capacity(&self) -> usize {
        self.vars
            .get_system("tidb_non_prepared_plan_cache_size")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(100)
    }

    /// Reads one session boolean, falling back to `default` when the variable
    /// is unreadable -- a probe must never turn a variable-table problem into
    /// a statement error.
    pub(crate) fn session_bool(&self, name: &str, default: bool) -> bool {
        match self.vars.get_system(name) {
            Ok(value) => matches!(
                value.to_ascii_uppercase().as_str(),
                "ON" | "1" | "TRUE" | "YES"
            ),
            Err(_) => default,
        }
    }
}
