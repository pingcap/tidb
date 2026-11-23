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
//! used by SQL/binary PREPARE uses the shared session physical-entry owner. Parameter types,
//! schema/statistics versions, matched binding SQL, admission, recursive range
//! rebuild, and executor construction therefore have one implementation.

#[allow(unused_imports)]
use super::*;
use tidb_ast::Stmt;
use tidb_charset::Collation;
use crate::{PreparedDmlPlan, PreparedSelectPlan};
use tidb_planner::plan_cacheable_checker::{get_param_sql_from_ast, parameterize_dml, ParameterizedSelect, Refusal, MAX_NUM_PARAM};
use tidb_planner_core_metrics::get_non_prep_plan_cache_unsupported_counter;
use tidb_kvcache::SimpleLruCache;

/// Go SessionVars.nonPreparedPlanCacheStmts: syntax metadata only, with one
/// definition LRU shared by SELECT and DML. Physical entries live in the
/// session owner independently of metadata eviction.
///
/// Each entry keeps the privilege requests of its statement, Go's
/// `PlanCacheStmt.VisitInfos` (`GeneratePlanCacheStmtWithAST`), which every
/// use checks (`checkPreparedPriv`) before it binds a plan.
#[derive(Clone)]
pub enum NonPreparedPlan {
    Select(std::sync::Arc<PreparedSelectPlan>, NonPreparedPrivileges),
    Dml(std::sync::Arc<PreparedDmlPlan>, NonPreparedPrivileges),
}

/// Go `PlanCacheStmt.VisitInfos` of a non-prepared plan-cache entry.
pub type NonPreparedPrivileges =
    std::sync::Arc<[crate::table_privilege::TablePrivilegeRequest]>;

#[derive(Default)]
pub struct NonPreparedPlanCache {
    plans: Option<SimpleLruCache<String, NonPreparedPlan>>,
}

impl NonPreparedPlanCache {
    pub fn get(&mut self, key: &str) -> Option<NonPreparedPlan> {
        self.plans.as_mut()?.get(key).cloned()
    }

    pub fn put(&mut self, key: String, plan: NonPreparedPlan, capacity: usize) {
        // Go allocates the metadata LRU at the first admitted insertion.
        self.plans
            .get_or_insert_with(|| SimpleLruCache::new(capacity))
            .put(key, plan);
    }
}

/// Go `getPlanFromNonPreparedPlanCache` and `NonPreparedPlanCacheableWithCtx` over the session.
pub trait NonPreparedPlanCacheCompile: super::CompilerSession {
    /// Parameterizes one admitted non-prepared SELECT. Refused statements use
    /// the ordinary planner and publish no cache hit.
    fn parameterize_non_prepared_select(
        &mut self,
        stmt: &Stmt,
    ) -> Option<ParameterizedSelect> {
        if !self.non_prepared_plan_cache_enabled() {
            return None;
        }
        self.parameterize_non_prepared_select_or_refusal(stmt).ok()
    }

    /// [`Self::parameterize_non_prepared_select`] with Go
    /// `NonPreparedPlanCacheableWithCtx`'s refusal reason.
    fn parameterize_non_prepared_select_or_refusal(
        &mut self,
        stmt: &Stmt,
    ) -> Result<ParameterizedSelect, Refusal> {
        // Go's unsupported counter increments only for checker-walk refusals
        // (cacheable=false after `Accept`); clause-level fast-check refusals
        // return their reason without touching any counter. The
        // classification rides on `Refusal::counted`.
        let enable_param_limit = self.session_vars().system_bool("tidb_enable_plan_cache_for_param_limit", true);
        let string_collation = self
            .session_vars()
            .get_system("collation_connection")
            .ok()
            .and_then(|name| Collation::from_name(&name))
            .unwrap_or(Collation::Utf8Mb4Bin);
        let configured_max = self
            .session_vars()
            .optimizer_fix_control()
            .get_int_with_default(tidb_fixcontrol::FIX_44823, MAX_NUM_PARAM as i64);
        let max_num_param = match configured_max {
            0 => usize::MAX,
            value if value > 0 => usize::try_from(value).unwrap_or(usize::MAX),
            _ => MAX_NUM_PARAM,
        };
        let domain = self.domain_info_schema();
        let catalog = domain
            .lock()
            .map_err(|_| Refusal::fast("the catalog is unavailable"))?;
        get_param_sql_from_ast(
            stmt,
            &catalog,
            &self.session_vars().current_db,
            enable_param_limit,
            self.non_prepared_plan_cache_for_dml_enabled(),
            string_collation,
            max_num_param,
        )
        .inspect_err(|refusal| {
            // Go `NonPreparedPlanCacheableWithCtx`'s unsupported counter:
            // only checker-walk refusals count; clause-level fast-check
            // refusals return their reason without touching the counter.
            if refusal.counted {
                get_non_prep_plan_cache_unsupported_counter().inc();
            }
        })
    }

    /// Go's DML switch: `EnableNonPreparedPlanCacheForDML`, true by default.
    fn non_prepared_plan_cache_for_dml_enabled(&self) -> bool {
        self.session_vars().system_bool("tidb_enable_non_prepared_plan_cache_for_dml", true)
    }

    /// Parameterizes one non-prepared UPDATE/INSERT/DELETE statement.
    fn parameterize_non_prepared_dml(
        &mut self,
        stmt: &Stmt,
    ) -> Option<ParameterizedSelect> {
        if !self.non_prepared_plan_cache_enabled()
            || !self.non_prepared_plan_cache_for_dml_enabled()
        {
            return None;
        }
        let enable_param_limit = self.session_vars().system_bool("tidb_enable_plan_cache_for_param_limit", true);
        let string_collation = self
            .session_vars()
            .get_system("collation_connection")
            .ok()
            .and_then(|name| Collation::from_name(&name))
            .unwrap_or(Collation::Utf8Mb4Bin);
        let configured_max = self
            .session_vars()
            .optimizer_fix_control()
            .get_int_with_default(tidb_fixcontrol::FIX_44823, MAX_NUM_PARAM as i64);
        let max_num_param = match configured_max {
            0 => usize::MAX,
            value if value > 0 => usize::try_from(value).unwrap_or(usize::MAX),
            _ => MAX_NUM_PARAM,
        };
        let domain = self.domain_info_schema();
        let catalog = domain.lock().ok()?;
        parameterize_dml(
            stmt,
            &catalog,
            &self.session_vars().current_db,
            enable_param_limit,
            string_collation,
            max_num_param,
        )
        .ok()
    }

    /// Go `GeneratePlanCacheStmtWithAST`'s `VisitInfos` for a new
    /// non-prepared entry over its parameterized statement; `None` when they
    /// cannot be collected, which leaves the statement to the ordinary path
    /// (whose build reports why).
    fn non_prepared_privilege_requests(&self, statement: &Stmt) -> Option<NonPreparedPrivileges> {
        self.collect_table_privileges(statement, true)
            .ok()
            .map(std::sync::Arc::from)
    }

    /// Generates or recursively rebuilds the retained DML root for one
    /// parameterized non-prepared statement, through the same plan object
    /// SQL and binary PREPARE use. Answers the entry's privilege requests,
    /// already checked (Go `GetPlanFromPlanCache`'s `checkPreparedPriv`),
    /// with the execution.
    fn bind_non_prepared_dml(
        &mut self,
        parameterized: &ParameterizedSelect,
        effective_statement: &Stmt,
        binding_sql: Option<&str>,
    ) -> Result<Option<(crate::PreparedDmlExecution, NonPreparedPrivileges)>, DriverError> {
        if !self.non_prepared_plan_cache_allowed(effective_statement) {
            return Ok(None);
        }
        let Some(environment) = self.prepared_plan_cache_environment_for_binding(binding_sql)
        else {
            return Ok(None);
        };
        let lookup_start = std::time::Instant::now();
        let (plan, requests) = match self.non_prepared_plan_cache_mut().get(&parameterized.key) {
            Some(NonPreparedPlan::Dml(plan, requests)) => (plan, requests),
            Some(_) => return Ok(None),
            None => {
                let plan = {
                    let Ok(catalog) = self.lock_catalog() else {
                        return Ok(None);
                    };
                    let Ok(Some(plan)) = crate::build_prepared_dml_plan(
                        &parameterized.statement,
                        parameterized.values.len(),
                        &catalog,
                        self.session_vars().current_db.as_str(),
                    ) else {
                        return Ok(None);
                    };
                    plan
                };
                let Some(requests) =
                    self.non_prepared_privilege_requests(&parameterized.statement)
                else {
                    return Ok(None);
                };
                let plan = std::sync::Arc::new(
                    plan.with_sql(&parameterized.key)
                        .with_non_prepared_stats_tables(),
                );
                let capacity = self.session_plan_cache_capacity();
                self.non_prepared_plan_cache_mut().put(
                    parameterized.key.clone(),
                    NonPreparedPlan::Dml(std::sync::Arc::clone(&plan), std::sync::Arc::clone(&requests)),
                    capacity,
                );
                (plan, requests)
            }
        };
        self.check_table_privilege_requests(&requests)?;
        Ok(self
            .bind_non_prepared_dml_plan(&plan, parameterized, effective_statement, &environment, lookup_start)
            .map(|execution| (execution, requests)))
    }

    /// [`Self::bind_non_prepared_dml`]'s lookup or replan of the entry.
    fn bind_non_prepared_dml_plan(
        &mut self,
        plan: &std::sync::Arc<PreparedDmlPlan>,
        parameterized: &ParameterizedSelect,
        effective_statement: &Stmt,
        environment: &crate::PreparedPlanCacheEnvironment,
        lookup_start: std::time::Instant,
    ) -> Option<crate::PreparedDmlExecution> {
        self.configure_session_plan_cache();
        {
            let catalog = self.lock_catalog().ok()?;
            if let Some(execution) = plan.bind_cached_for_statement(
                self.physical_plan_cache(),
                &parameterized.values,
                &catalog,
                self.session_vars().current_db.as_str(),
                &self.plan_cache_dirty_tables(),
                &environment,
                effective_statement,
            ) {
                // Go `GetPlanFromPlanCache`'s hit arm; `lookupPlanCache`'s
                // defer observes the lookup duration on hits only.
                tidb_planner_core_metrics::inc_plan_cache_hit(true);
                tidb_planner_core_metrics::get_plan_cache_lookup_duration(
                    self.physical_plan_cache().uses_instance(),
                )
                .observe(lookup_start.elapsed().as_secs_f64());
                return Some(execution);
            }
        }
        // Go's miss arm re-plans; its miss counter records the replan.
        tidb_planner_core_metrics::plan_cache_miss_counter(true).inc();
        let ctx = self.statement_context(false).with_non_prepared_plan_cache();
        let catalog = self.lock_catalog().ok()?;
        plan.bind_for_statement(
            self.physical_plan_cache(),
            &parameterized.values,
            &catalog,
            self.session_vars().current_db.as_str(),
            &ctx,
            &self.plan_cache_dirty_tables(),
            &environment,
            effective_statement,
        )
        .ok()
        .flatten()
    }

    /// Generates or recursively rebuilds the physical plan owned by one
    /// parameterized non-prepared statement. This is Go's
    /// `GetPlanFromPlanCache(..., isNonPrepared=true)` over the same Rust plan
    /// object SQL and binary PREPARE use.
    fn bind_non_prepared_select(
        &mut self,
        parameterized: &ParameterizedSelect,
        effective_statement: &Stmt,
        binding_sql: Option<&str>,
    ) -> Result<Option<crate::PreparedSelectExecution>, DriverError> {
        self.bind_non_prepared_select_warning(parameterized, effective_statement, binding_sql, false)
    }

    /// [`Self::bind_non_prepared_select`]; `always_warn` is Go's
    /// `SetAlwaysWarnSkipCache`, which EXPLAIN FORMAT = 'plan_cache' sets.
    /// The entry's privilege requests are checked first (Go
    /// `GetPlanFromPlanCache`'s `checkPreparedPriv`).
    fn bind_non_prepared_select_warning(
        &mut self,
        parameterized: &ParameterizedSelect,
        effective_statement: &Stmt,
        binding_sql: Option<&str>,
        always_warn: bool,
    ) -> Result<Option<crate::PreparedSelectExecution>, DriverError> {
        if !self.non_prepared_plan_cache_allowed(effective_statement) {
            return Ok(None);
        }
        let lookup_start = std::time::Instant::now();
        let Some(environment) = self.prepared_plan_cache_environment_for_binding(binding_sql)
        else {
            return Ok(None);
        };
        let (plan, requests) = match self.non_prepared_plan_cache_mut().get(&parameterized.key) {
            Some(NonPreparedPlan::Select(plan, requests)) => (plan, requests),
            Some(_) => return Ok(None),
            None => {
                let ctx = self.statement_context(false);
                let plan = {
                    let Ok(catalog) = self.lock_catalog() else {
                        return Ok(None);
                    };
                    let Some(plan) = crate::build_prepared_select_plan(
                        &parameterized.statement,
                        parameterized.values.len(),
                        &catalog,
                        self.session_vars().current_db.as_str(),
                        &ctx,
                    ) else {
                        return Ok(None);
                    };
                    plan
                };
                let Some(requests) =
                    self.non_prepared_privilege_requests(&parameterized.statement)
                else {
                    return Ok(None);
                };
                let plan = std::sync::Arc::new(
                    plan.with_sql(&parameterized.key)
                        .with_non_prepared_stats_tables(),
                );
                let capacity = self.session_plan_cache_capacity();
                self.non_prepared_plan_cache_mut().put(
                    parameterized.key.clone(),
                    NonPreparedPlan::Select(std::sync::Arc::clone(&plan), std::sync::Arc::clone(&requests)),
                    capacity,
                );
                (plan, requests)
            }
        };
        self.check_table_privilege_requests(&requests)?;
        Ok(self.bind_non_prepared_select_plan(
            &plan,
            parameterized,
            effective_statement,
            &environment,
            lookup_start,
            always_warn,
        ))
    }

    /// [`Self::bind_non_prepared_select_warning`]'s lookup or replan of the
    /// entry.
    fn bind_non_prepared_select_plan(
        &mut self,
        plan: &std::sync::Arc<PreparedSelectPlan>,
        parameterized: &ParameterizedSelect,
        effective_statement: &Stmt,
        environment: &crate::PreparedPlanCacheEnvironment,
        lookup_start: std::time::Instant,
        always_warn: bool,
    ) -> Option<crate::PreparedSelectExecution> {
        self.configure_session_plan_cache();
        let observe = |start: std::time::Instant| {
            tidb_planner_core_metrics::get_plan_cache_lookup_duration(
                self.physical_plan_cache().uses_instance(),
            )
            .observe(start.elapsed().as_secs_f64());
        };
        {
            let catalog = self.lock_catalog().ok()?;
            if let Some(execution) = plan.bind_cached_for_statement(
                self.physical_plan_cache(),
                &parameterized.values,
                &catalog,
                self.session_vars().current_db.as_str(),
                &self.plan_cache_dirty_tables(),
                &environment,
                effective_statement,
            ) {
                // Go `GetPlanFromPlanCache`'s hit arm. Go's `lookupPlanCache`
                // defer observes the lookup duration ONLY when it hit; a
                // miss/re-plan records the miss counter and nothing else.
                tidb_planner_core_metrics::inc_plan_cache_hit(true);
                observe(lookup_start);
                return Some(execution);
            }
        }
        // Go's miss arm re-plans; its miss counter records the replan.
        tidb_planner_core_metrics::plan_cache_miss_counter(true).inc();
        let mut ctx = self.statement_context(false).with_non_prepared_plan_cache();
        if always_warn {
            ctx = ctx.with_always_warn_skip_plan_cache();
        }
        let catalog = self.lock_catalog().ok()?;
        let execution = plan.bind_for_statement(
            self.physical_plan_cache(),
            &parameterized.values,
            &catalog,
            self.session_vars().current_db.as_str(),
            &ctx,
            &self.plan_cache_dirty_tables(),
            &environment,
            effective_statement,
        );
        execution.ok().flatten()
    }

    /// Whether `@@last_plan_from_cache` should report a hit, which is the
    /// PRECEDING statement's outcome (Go `PrevFoundInPlanCache`).
    fn last_plan_from_cache(&self) -> bool {
        self.session_vars().prev_found_in_plan_cache
    }

    fn non_prepared_plan_cache_enabled(&self) -> bool {
        self.session_vars().system_bool("tidb_enable_non_prepared_plan_cache", false)
    }

    /// Go `getPlanFromNonPreparedPlanCache`'s strategy gate: `hint_only`
    /// admits only a statement carrying `use_plan_cache()`.
    fn non_prepared_hint_only_refuses(&self, statement: &Stmt) -> bool {
        let hint_only = self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_PLAN_CACHE_STRATEGY)
            .is_ok_and(|strategy| {
                strategy.eq_ignore_ascii_case(
                    tidb_vardef::tidb_vars::TIDB_PLAN_CACHE_STRATEGY_HINT_ONLY,
                )
            });
        hint_only
            && !self.parse_statement_hints_without_catalog(
                statement,
                self.session_vars().current_db.as_str(),
            )
            .use_plan_cache
    }

    fn non_prepared_plan_cache_allowed(&self, statement: &Stmt) -> bool {
        // Go `getPlanFromNonPreparedPlanCache`: a statement of a
        // multi-statement query never reaches the cache.
        if self.session_vars().in_multi_stmts {
            return false;
        }
        let hints = self.parse_statement_hints_without_catalog(
            statement,
            self.session_vars().current_db.as_str(),
        );
        if hints.ignore_plan_cache {
            return false;
        }
        let hint_only = self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_PLAN_CACHE_STRATEGY)
            .is_ok_and(|strategy| {
                strategy.eq_ignore_ascii_case(
                    tidb_vardef::tidb_vars::TIDB_PLAN_CACHE_STRATEGY_HINT_ONLY,
                )
            });
        !hint_only || hints.use_plan_cache
    }
}

impl<T: super::CompilerSession> NonPreparedPlanCacheCompile for T {}
