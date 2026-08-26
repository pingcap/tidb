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

//! The PREPARED plan cache's observable contract: Go
//! `plan_cacheable_checker.go`'s `IsASTCacheable`, decided at `PREPARE`, and
//! the hit `@@last_plan_from_cache` reports on a later `EXECUTE`.
//!
//! The same honesty note as [`crate::non_prepared_plan_cache`]: this tier has
//! no reified plan object, so nothing here STORES a plan. A statement is
//! re-planned from its text on every `EXECUTE`, which makes Go's danger --
//! returning a plan built for a different literal -- structurally impossible.
//! What is modelled is the contract an application can observe: which
//! prepared statements Go's cache would admit, and whether the `EXECUTE` that
//! just ran would have found its plan already there. The ADMISSION list is
//! ported at full fidelity because it is what will stand between a shared key
//! and a wrong answer when a reusable plan lands.
//!
//! # The key
//!
//! Go's cache key (`plan_cache_utils.go`, `NewPlanCacheKey`) hashes the
//! statement with the schema version, the current database, the `sql_mode`,
//! the time zone, and -- the one that surprised us -- the expression
//! push-down blacklist's reload timestamp (`plan_cache_utils.go:443`), so an
//! `ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST` invalidates every cached plan. The
//! corpus reads that directly: `planner/core/plan_cache` blacklists `mod`,
//! reloads, and expects the next `EXECUTE` to MISS. [`PreparedPlanKey`]
//! carries the same facts, with the reload timestamp as
//! [`crate::blacklist::PushdownBlacklists`]' generation counter.

use std::any::Any;

use tidb_ast::{Visitable, Visitor};

use crate::Session;

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

/// What one `EXECUTE` planned against. Two executes with an equal key are the
/// case Go serves from the cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PreparedPlanKey {
    /// The catalog's schema version: any DDL between the two executes is a
    /// miss, which is what makes a cached plan never read a dropped column.
    pub(crate) schema_version: u64,
    /// Go hashes the current database because an unqualified name resolves
    /// differently under another `USE`.
    pub(crate) current_db: String,
    /// `@@sql_mode`, which changes evaluation semantics.
    pub(crate) sql_mode: String,
    /// `@@time_zone`, which changes every temporal answer.
    pub(crate) time_zone: String,
    /// Go `ExprPushDownBlackListReloadTimeStamp`: bumped by `ADMIN RELOAD
    /// EXPR_PUSHDOWN_BLACKLIST`, so a reload invalidates cached plans.
    pub(crate) blacklist_generation: u64,
}

impl Session {
    /// The facts the CURRENT statement would be planned against.
    pub(crate) fn prepared_plan_key(&mut self) -> PreparedPlanKey {
        // Go keys on the INFOSCHEMA version, which DDL moves and DML never
        // does. This catalog's plain `version()` also advances on every write
        // path (`get_mut_in`, the transaction conflict-check approximation),
        // so keying on it would invalidate a cached plan on every UPDATE --
        // nothing like Go. `metadata_version()` is this catalog's DDL-only
        // counter and the faithful stand-in.
        let schema_version = self
            .with_catalog_mut(|catalog| Ok(catalog.metadata_version()))
            .unwrap_or(0);
        PreparedPlanKey {
            schema_version,
            current_db: self.current_db.clone(),
            sql_mode: self.vars.get_system("sql_mode").unwrap_or_default(),
            time_zone: self.vars.get_system("time_zone").unwrap_or_default(),
            blacklist_generation: self.pushdown_blacklists.generation(),
        }
    }

    /// Go `SessionVars.EnablePreparedPlanCache` (`tidb_enable_prepared_plan_cache`,
    /// default ON).
    pub(crate) fn prepared_plan_cache_enabled(&self) -> bool {
        !matches!(
            self.vars
                .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_PREP_PLAN_CACHE)
                .as_deref(),
            Ok("OFF" | "off" | "0")
        )
    }
}

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
