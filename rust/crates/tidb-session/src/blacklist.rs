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

//! `mysql.expr_pushdown_blacklist` and `mysql.opt_rule_blacklist`, and the
//! `ADMIN RELOAD` that publishes each: Go
//! `executor/reload_expr_pushdown_blacklist.go` and
//! `executor/opt_rule_blacklist.go`.
//!
//! Both are the same shape -- a table an operator writes with ordinary DML,
//! and a statement that reads it into the structure the optimizer consults.
//! Nothing is read from the table at plan time, in Go or here: the published
//! copy is what decides, so an `INSERT` alone changes nothing until the
//! `ADMIN RELOAD` that follows it.
//!
//! Go publishes into two process-wide atomics
//! (`expression.DefaultExprPushDownBlacklist`,
//! `plannercore.DefaultDisabledLogicalRulesList`) because its optimizer runs
//! far from any session. This tier holds them on the [`Session`], which is
//! the same reach -- one instance -- without a global that parallel tests
//! would share.

use std::collections::HashSet;
use std::rc::Rc;

use crate::Session;
use tidb_executor::DriverError;

impl Session {
    /// Go `LoadExprPushdownBlacklist`, and the `ADMIN RELOAD
    /// EXPR_PUSHDOWN_BLACKLIST` that calls it.
    pub(crate) fn reload_expr_pushdown_blacklist(&mut self) -> Result<(), DriverError> {
        let ctx = self.statement_context(false);
        let (_, rows) = self.with_catalog_mut(|catalog| {
            tidb_executor::run_select_meta_in(
                "SELECT name, store_type FROM mysql.expr_pushdown_blacklist",
                catalog,
                "mysql",
                &ctx,
            )
        })?;
        let mut loaded = tidb_executor::ExprPushDownBlacklist::new();
        for row in rows {
            let Some(name) = row.first().and_then(crate::datum_text) else {
                continue;
            };
            let name = tidb_executor::blacklist_name(&name);
            let store_types = row.get(1).and_then(crate::datum_text).unwrap_or_default();
            let value = loaded.get(&name).copied().unwrap_or(0)
                | tidb_executor::blacklist_store_mask(&store_types);
            loaded.insert(name, value);
        }
        self.expr_pushdown_blacklist = Rc::new(loaded);
        Ok(())
    }

    /// Go `LoadOptRuleBlacklist`, and the `ADMIN RELOAD OPT_RULE_BLACKLIST`
    /// that calls it. The names are the rules' own `Name()` strings and are
    /// NOT case-folded, which is Go's behaviour: `row.GetString(0)` goes into
    /// the set as written.
    pub(crate) fn reload_opt_rule_blacklist(&mut self) -> Result<(), DriverError> {
        let ctx = self.statement_context(false);
        let (_, rows) = self.with_catalog_mut(|catalog| {
            tidb_executor::run_select_meta_in(
                "SELECT name FROM mysql.opt_rule_blacklist",
                catalog,
                "mysql",
                &ctx,
            )
        })?;
        let mut loaded = HashSet::new();
        for row in rows {
            if let Some(name) = row.first().and_then(crate::datum_text) {
                loaded.insert(name);
            }
        }
        self.disabled_logical_rules = Rc::new(loaded);
        Ok(())
    }
}

#[cfg(test)]
impl Session {
    pub(crate) fn debug_blacklists(&self) -> (Vec<(String, u32)>, Vec<String>) {
        let mut map: Vec<(String, u32)> = self
            .expr_pushdown_blacklist
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        map.sort();
        let mut rules: Vec<String> = self.disabled_logical_rules.iter().cloned().collect();
        rules.sort();
        (map, rules)
    }
}
