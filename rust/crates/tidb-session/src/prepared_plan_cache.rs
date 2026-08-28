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
//! `tidb_executor::PreparedSelectPlan`; this module only decides whether
//! a statement may enter that cache.

use std::any::Any;

use tidb_ast::{Visitable, Visitor};

/// Go `getMaxParamLimit`'s default: more than this many values in one IN
/// list, or `rows * cols` in one `INSERT`, refuses caching to save memory.
const MAX_NUM_PARAM: usize = 200;

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
pub(crate) fn stmt_cacheable(stmt: &mut tidb_ast::Stmt) -> Result<(), String> {
    let kind_ok = match stmt {
        tidb_ast::Stmt::Query(_) => true,
        tidb_ast::Stmt::Dml(dml) => matches!(
            &**dml,
            tidb_ast::DmlStmt::Insert(_)
                | tidb_ast::DmlStmt::Update(_)
                | tidb_ast::DmlStmt::Delete(_)
        ),
        _ => false,
    };
    if !kind_ok {
        return Err("not a SELECT/UPDATE/INSERT/DELETE/SET statement".to_owned());
    }
    // Go refuses an `INSERT ... VALUES` whose literal grid alone exceeds the
    // parameter budget, before walking anything.
    if let tidb_ast::Stmt::Dml(dml) = stmt {
        if let tidb_ast::DmlStmt::Insert(insert) = &**dml {
            if insert.source.is_none() {
                let rows = insert.rows.len();
                let cols = insert.rows.first().map_or(0, Vec::len);
                if rows * cols > MAX_NUM_PARAM {
                    return Err("too many values in the insert statement".to_owned());
                }
            }
        }
    }
    let mut checker = CacheableChecker {
        refusal: None,
        sum_in_list_len: 0,
    };
    stmt.accept(&mut checker);
    match checker.refusal {
        Some(reason) => Err(reason),
        None => Ok(()),
    }
}

/// Go's `cacheableChecker` visitor, over this tier's AST.
struct CacheableChecker {
    refusal: Option<String>,
    /// Go `sumInListLen`: IN-list lengths accumulate across the statement.
    sum_in_list_len: usize,
}

impl CacheableChecker {
    fn refuse(&mut self, reason: impl Into<String>) -> bool {
        self.refusal = Some(reason.into());
        // Skipping children is enough; `leave` keeps returning `true` so the
        // walk winds down without visiting further subtrees' contents, and
        // the FIRST reason -- Go's -- is the one reported.
        true
    }
}

impl Visitor for CacheableChecker {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        if self.refusal.is_some() {
            return true;
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
                    if self.sum_in_list_len > MAX_NUM_PARAM {
                        return self.refuse("too many values in in-list");
                    }
                }
                tidb_ast::Expr::Func { name, .. } => {
                    let lowered = name.to_ascii_lowercase();
                    if UNCACHEABLE_FUNCTIONS.contains(&lowered.as_str()) {
                        return self.refuse(format!("query has '{lowered}' is un-cacheable"));
                    }
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
        false
    }

    fn leave(&mut self, _node: &mut dyn Any) -> bool {
        true
    }
}
