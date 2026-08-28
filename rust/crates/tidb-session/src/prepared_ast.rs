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
    DriverError, PreparedDmlExecution, PreparedDmlPlan, PreparedPointGetExecution,
    PreparedPointGetPlan, PreparedSelectExecution, PreparedSelectPlan,
};

use crate::Session;

/// One statement parsed under the SQL mode in force at PREPARE time.
#[derive(Clone, Debug)]
pub struct PreparedAst {
    statement: Stmt,
    parameter_count: usize,
    point_get_plan: Option<Arc<PreparedPointGetPlan>>,
    dml_plan: Option<Arc<PreparedDmlPlan>>,
    select_plan: Option<Arc<PreparedSelectPlan>>,
}

pub(crate) struct PreparedPlanCacheEnvironmentCache {
    vars_generation: u64,
    blacklist_generation: u64,
    in_transaction: bool,
    autocommit: bool,
    environment: Option<Arc<tidb_executor::PreparedPlanCacheEnvironment>>,
}

impl PreparedAst {
    pub(crate) fn from_parsed(
        statement: Stmt,
        parameter_count: usize,
        point_get_plan: Option<PreparedPointGetPlan>,
        dml_plan: Option<PreparedDmlPlan>,
        select_plan: Option<PreparedSelectPlan>,
    ) -> Self {
        Self {
            statement,
            parameter_count,
            point_get_plan: point_get_plan.map(Arc::new),
            dml_plan: dml_plan.map(Arc::new),
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

    /// The immutable DML plan compiled while the statement was prepared.
    #[must_use]
    pub fn dml_plan(&self) -> Option<Arc<PreparedDmlPlan>> {
        self.dml_plan.clone()
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
    pub(crate) fn prepared_plan_cache_environment(
        &self,
    ) -> Option<Arc<tidb_executor::PreparedPlanCacheEnvironment>> {
        let vars_generation = self.vars.generation();
        let blacklist_generation = self.pushdown_blacklists.generation();
        let in_transaction = self.in_transaction();
        let autocommit = self.is_autocommit();
        if let Some(cached) = self.prepared_plan_cache_environment_cache.borrow().as_ref() {
            if cached.vars_generation == vars_generation
                && cached.blacklist_generation == blacklist_generation
                && cached.in_transaction == in_transaction
                && cached.autocommit == autocommit
            {
                return cached.environment.clone();
            }
        }
        let sql_select_limit = self.vars.get_system("sql_select_limit");
        let snapshot = self.vars.get_system(tidb_vardef::tidb_vars::TIDB_SNAPSHOT);
        let read_staleness = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_READ_STALENESS);
        let environment = (sql_select_limit.as_deref() == Ok("18446744073709551615")
            && !snapshot.is_ok_and(|value| !value.is_empty())
            && !read_staleness
                .is_ok_and(|value| value.trim().parse::<i64>().is_ok_and(|value| value != 0)))
        .then(|| {
            Arc::new(
                tidb_executor::PreparedPlanCacheEnvironment::new(
                    self.vars.get_system("sql_mode").unwrap_or_default(),
                    self.vars.get_system("time_zone").unwrap_or_default(),
                    blacklist_generation,
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
                    sql_select_limit.unwrap_or_default(),
                    in_transaction,
                    autocommit,
                    self.vars
                        .get_system(
                            tidb_vardef::tidb_vars::TIDB_PLAN_CACHE_INVALIDATION_ON_FRESH_STATS,
                        )
                        .as_deref()
                        != Ok("OFF"),
                ),
            )
        });
        *self.prepared_plan_cache_environment_cache.borrow_mut() =
            Some(PreparedPlanCacheEnvironmentCache {
                vars_generation,
                blacklist_generation,
                in_transaction,
                autocommit,
                environment: environment.clone(),
            });
        environment
    }

    /// Parses and retains the statement under this session's current SQL mode.
    pub fn prepare_ast(&self, sql: &str) -> Result<PreparedAst, DriverError> {
        let statement = self.parse_statement(sql)?;
        let parameter_count = tidb_executor::parsed_parameter_count(&statement);
        let planner_context = self.statement_context(false);
        let (point_get_plan, dml_plan, select_plan) = {
            let catalog = self.lock_catalog()?;
            (
                tidb_executor::build_prepared_point_get_plan(
                    &statement,
                    parameter_count,
                    &catalog,
                    self.current_database(),
                    &self.session_time_zone(),
                ),
                tidb_executor::build_prepared_dml_plan(
                    &statement,
                    parameter_count,
                    &catalog,
                    self.current_database(),
                )?,
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
            dml_plan,
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
            || self.prepared_plan_cache_environment().is_none()
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

    /// Binds fresh values into a retained DML plan after applying the prepared
    /// cache, binding, fix-control, database, and schema gates.
    pub fn bind_cached_prepared_dml(
        &self,
        plan: &Arc<PreparedDmlPlan>,
        values: &[Datum],
    ) -> Option<PreparedDmlExecution> {
        if !self.prepared_plan_cache_enabled()
            || !self.session_bindings.is_empty()
            || self
                .vars
                .optimizer_fix_control()
                .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false)
        {
            return None;
        }
        let catalog = self.lock_catalog().ok()?;
        plan.bind(values, &catalog, self.current_database())
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
        {
            return None;
        }
        let environment = self.prepared_plan_cache_environment()?;
        {
            let catalog = self.lock_catalog().ok()?;
            if let Some(execution) =
                plan.bind_cached(values, &catalog, self.current_database(), &environment)
            {
                return Some(execution);
            }
        }
        // Planning a cache miss needs sequence and decode-key snapshots from
        // the catalog. Build them before holding the catalog guard; taking
        // the guard first would recursively lock the same mutex.
        let ctx = self.statement_context(false);
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
