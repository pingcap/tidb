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

//! The parsed statement retained by PREPARE and its execute-time bound clone.
//!
//! Go stores this as `PlanCacheStmt.PreparedAst`. The retained point-get and
//! general SELECT descriptors are immutable; the protocol layer owns their
//! cache state and rebuilds mutable execution state for every EXECUTE.

use std::sync::Arc;

use tidb_ast::Stmt;
use tidb_datatype::Datum;
use tidb_executor::{
    DriverError, PreparedPointGetExecution, PreparedPointGetPlan, PreparedSelectExecution,
    PreparedSelectPlan,
};

use crate::Session;

/// One statement parsed under the SQL mode in force at PREPARE time.
#[derive(Clone, Debug)]
pub struct PreparedAst {
    statement: Stmt,
    parameter_count: usize,
    point_get_plan: Option<Arc<PreparedPointGetPlan>>,
    select_plan: Option<Arc<PreparedSelectPlan>>,
}

impl PreparedAst {
    pub(crate) fn from_parsed(
        statement: Stmt,
        parameter_count: usize,
        point_get_plan: Option<PreparedPointGetPlan>,
        select_plan: Option<PreparedSelectPlan>,
    ) -> Self {
        Self {
            statement,
            parameter_count,
            point_get_plan: point_get_plan.map(Arc::new),
            select_plan: select_plan.map(Arc::new),
        }
    }

    /// The number of execute-time values this statement requires.
    #[must_use]
    pub const fn parameter_count(&self) -> usize {
        self.parameter_count
    }

    /// The statement parsed under PREPARE-time SQL semantics.
    #[must_use]
    pub const fn statement(&self) -> &Stmt {
        &self.statement
    }

    /// The immutable fast point-read plan compiled while the statement was
    /// prepared, when its shape is safe to reuse for every EXECUTE.
    #[must_use]
    pub fn point_get_plan(&self) -> Option<Arc<PreparedPointGetPlan>> {
        self.point_get_plan.clone()
    }

    /// The prepared SELECT descriptor whose full physical tree is generated
    /// on the first EXECUTE and rebuilt on later cache hits.
    #[must_use]
    pub fn select_plan(&self) -> Option<Arc<PreparedSelectPlan>> {
        self.select_plan.clone()
    }

    /// Clones the retained tree and installs this execution's values on its
    /// parameter markers, matching Go's immutable prepared definition plus
    /// per-execution marker state.
    pub fn bind(&self, values: &[Datum]) -> Result<Stmt, DriverError> {
        tidb_executor::bind_prepared_statement(&self.statement, values)
    }
}

impl Session {
    /// Parses and retains the statement under this session's current SQL mode.
    pub fn prepare_ast(&self, sql: &str) -> Result<PreparedAst, DriverError> {
        let statement = self.parse_statement(sql)?;
        let parameter_count = tidb_executor::parsed_parameter_count(&statement);
        let planner_context = self.statement_context(false);
        let (point_get_plan, select_plan) = {
            let catalog = self.lock_catalog()?;
            (
                tidb_executor::build_prepared_point_get_plan(
                    &statement,
                    parameter_count,
                    &catalog,
                    self.current_database(),
                    &self.session_time_zone(),
                ),
                tidb_executor::build_prepared_select_plan(
                    &statement,
                    parameter_count,
                    &catalog,
                    self.current_database(),
                    &planner_context,
                ),
            )
        };
        Ok(PreparedAst::from_parsed(
            statement,
            parameter_count,
            point_get_plan,
            select_plan,
        ))
    }

    /// Go `IsSafeToReusePointGetExecutor` plus the plan-cache reuse gates of
    /// `GetPlanFromPlanCache`. Every uncertain state declines to ordinary
    /// planning; no cached lookup is allowed to widen into a multi-read plan.
    ///
    /// An EXPLICIT transaction may reuse the cached read too: its statement
    /// snapshot is already bound before execution, so the lookup reads at the
    /// transaction's timestamp through the session's own storage — the same
    /// visibility the ordinary planner would build for it (Go serves these
    /// from its prepared plan cache inside transactions as well).
    pub(crate) fn can_reuse_prepared_point_get(&self, plan: &PreparedPointGetPlan) -> bool {
        if !self.session_bindings.is_empty() {
            return false;
        }
        if self
            .vars
            .optimizer_fix_control()
            .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false)
            || self.vars.get_system("sql_select_limit").as_deref() != Ok("18446744073709551615")
            || self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_SNAPSHOT)
                .is_ok_and(|value| !value.is_empty())
            || self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_READ_STALENESS)
                .is_ok_and(|value| value.trim().parse::<i64>().is_ok_and(|value| value != 0))
        {
            return false;
        }
        self.lock_catalog()
            .is_ok_and(|catalog| plan.matches_catalog(&catalog, self.current_database()))
    }

    /// Binds a retained point-get plan for a binary EXECUTE after applying
    /// the same autocommit, snapshot, session-binding, and schema gates used
    /// by the Go point-get cache.
    pub fn bind_cached_prepared_point_get(
        &self,
        plan: &Arc<PreparedPointGetPlan>,
        values: &[Datum],
    ) -> Option<PreparedPointGetExecution> {
        if !self.can_reuse_prepared_point_get(plan) {
            return None;
        }
        plan.bind(values, &self.session_time_zone())
    }

    /// Binds the current values into a retained SELECT after applying the
    /// same session, schema, and stale-read invalidation gates as Go's plan
    /// cache. A refusal returns the statement to ordinary planning.
    pub fn bind_cached_prepared_select(
        &self,
        plan: &Arc<PreparedSelectPlan>,
        values: &[Datum],
    ) -> Option<PreparedSelectExecution> {
        if !self.prepared_plan_cache_enabled()
            || !self.session_bindings.is_empty()
            || self
                .vars
                .optimizer_fix_control()
                .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false)
            || self.vars.get_system("sql_select_limit").as_deref() != Ok("18446744073709551615")
            || self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_SNAPSHOT)
                .is_ok_and(|value| !value.is_empty())
            || self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_READ_STALENESS)
                .is_ok_and(|value| value.trim().parse::<i64>().is_ok_and(|value| value != 0))
        {
            return None;
        }
        let ctx = self.statement_context(false);
        let environment = tidb_executor::PreparedPlanCacheEnvironment::new(
            self.vars.get_system("sql_mode").unwrap_or_default(),
            self.vars.get_system("time_zone").unwrap_or_default(),
            self.pushdown_blacklists.generation(),
        )
        .with_session_state(
            self.vars
                .get_system("character_set_connection")
                .unwrap_or_default(),
            self.vars
                .get_system("collation_connection")
                .unwrap_or_default(),
            self.vars
                .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
                .unwrap_or_default(),
            self.vars
                .get_system(tidb_vardef::tidb_vars::TIDB_ISOLATION_READ_ENGINES)
                .unwrap_or_default(),
            self.vars.get_system("sql_select_limit").unwrap_or_default(),
            self.in_transaction(),
            self.is_autocommit(),
            self.vars
                .get_system(tidb_vardef::tidb_vars::TIDB_PLAN_CACHE_INVALIDATION_ON_FRESH_STATS)
                .as_deref()
                != Ok("OFF"),
        );
        // `statement_context` takes its sequence snapshot from the catalog.
        // Build it before holding the catalog guard; taking the guard first
        // would recursively lock the same mutex on every general cache bind.
        let catalog = self.lock_catalog().ok()?;
        plan.bind(
            values,
            &catalog,
            self.current_database(),
            &ctx,
            &environment,
        )
    }
}
