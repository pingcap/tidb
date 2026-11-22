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

//! Go `pkg/bindinfo/binding_auto.go`: `EXPLAIN EXPLORE <SQL>`'s historical
//! and generated plan candidates, scored and recommended.

use tidb_ast::Stmt;
use tidb_datatype::Datum;
use tidb_driver_error::DriverError;
use tidb_sqlescape::SqlArg;

use crate::binding::Binding;
use crate::binding_plan_evolution::{LlmBasedPlanPerfPredictor, PlanPerfPredictor, RuleBasedPlanPerfPredictor};
use crate::binding_plan_generation::OptVarValue;
use crate::binding_utils::InternalSqlRunner;

/// Go `BindingPlanInfo`: a binding with the execution info of its plan,
/// one row of `EXPLAIN EXPLORE`.
#[derive(Clone, Debug, Default)]
pub struct BindingPlanInfo {
    /// Go's embedded `*Binding`.
    pub binding: Binding,
    /// Go `Plan`, from the statement stats.
    pub plan: String,
    /// Go `AvgLatency`.
    pub avg_latency: f64,
    /// Go `ExecTimes`.
    pub exec_times: i64,
    /// Go `AvgScanRows`.
    pub avg_scan_rows: f64,
    /// Go `AvgReturnedRows`.
    pub avg_returned_rows: f64,
    /// Go `LatencyPerReturnRow`.
    pub latency_per_return_row: f64,
    /// Go `ScanRowsPerReturnRow`.
    pub scan_rows_per_return_row: f64,
    /// Go `Recommend`.
    pub recommend: String,
    /// Go `Reason`.
    pub reason: String,
}

/// Go's `sessionctx.Context` as `binding_auto.go` and
/// `binding_plan_generation.go` use it, with the planner's
/// `CalculatePlanDigest`, `RecordRelevantOptVarsAndFixes` and
/// `GenBriefPlanWithSCtx` hooks.
pub trait PlanExploreSession: InternalSqlRunner {
    /// Go `SessionVars.CurrentDB`.
    fn current_db(&self) -> String;
    /// Sets `SessionVars.CurrentDB`.
    fn set_current_db(&mut self, db: &str);
    /// Go `SessionVars.UsePlanBaselines`.
    fn use_plan_baselines(&self) -> bool;
    /// Sets `SessionVars.UsePlanBaselines`.
    fn set_use_plan_baselines(&mut self, enabled: bool);
    /// Sets `SessionVars.CostModelVersion`.
    fn set_cost_model_version(&mut self, version: i64);
    /// `genPlanUnderState`'s assignment of one optimizer variable.
    ///
    /// # Errors
    ///
    /// The value does not apply.
    fn set_opt_var(&mut self, var_name: &str, value: OptVarValue) -> Result<(), DriverError>;
    /// Go `SessionVars.OptimizerFixControl = fixcontrol.ParseToMap(fixes)`.
    ///
    /// # Errors
    ///
    /// The fix-control text does not parse.
    fn set_optimizer_fix_control(&mut self, fixes: &str) -> Result<(), DriverError>;
    /// Go `RecordRelevantOptVarsAndFixes`: the optimizer variables and
    /// fixes the statement's optimization consults, sorted.
    ///
    /// # Errors
    ///
    /// The statement does not plan.
    fn record_relevant_opt_vars_and_fixes(&mut self, stmt: &Stmt) -> Result<(Vec<String>, Vec<u64>), DriverError>;
    /// Go `GenBriefPlanWithSCtx`: the plan's digest, the hints that
    /// reproduce it, and its `EXPLAIN FORMAT='brief'` rows.
    ///
    /// # Errors
    ///
    /// The statement does not plan.
    fn gen_brief_plan(&mut self, stmt: &Stmt) -> Result<(String, String, Vec<Vec<String>>), DriverError>;
    /// Go `CalculatePlanDigest`.
    ///
    /// # Errors
    ///
    /// The statement does not plan.
    fn calculate_plan_digest(&mut self, stmt: &Stmt) -> Result<String, DriverError>;
    /// Go `checkBindingValidation`: `EXPLAIN FORMAT='hint' <bind SQL>` with
    /// plan baselines off.
    ///
    /// # Errors
    ///
    /// The bind SQL does not plan.
    fn check_binding_validation(&mut self, bind_sql: &str) -> Result<(), DriverError>;
    /// Go `GetLatestInfoSchema().TableInfoByName(schema, table)`.
    fn table_info_by_name(&mut self, schema: &str, table: &str) -> Option<tidb_model::GoShared<tidb_model::TableInfo>>;
    /// Go `SessionVars.OptimizerUseInvisibleIndexes`.
    fn optimizer_use_invisible_indexes(&self) -> bool;
    /// Go `execRows(sctx, sql)` followed by the statement's
    /// `GetTotalCostDuration` (nanoseconds), its
    /// `ExecDetails.ScanDetail.ProcessedKeys` and its result row count.
    ///
    /// # Errors
    ///
    /// The statement's error.
    fn exec_and_get_exec_info(&mut self, sql: &str) -> Result<(f64, Option<f64>, f64), DriverError>;
}

/// Go `util.DestroyableSessionPool` as `callWithSCtx(sPool, false, f)`
/// uses it: `f` on one of the pool's sessions.
pub trait PlanExploreSessionPool {
    /// Go `callWithSCtx(sPool, false, f)`.
    ///
    /// # Errors
    ///
    /// The session's failure to open, or `body`'s error.
    fn call_with_sctx(
        &self,
        body: &mut dyn FnMut(&mut dyn PlanExploreSession) -> Result<(), DriverError>,
    ) -> Result<(), DriverError>;
}

/// Go `bindingAuto.ExplorePlansForSQL`: the historical plan candidates, the
/// generated ones (run when `analyze`), all scored and the best
/// recommended.
///
/// # Errors
///
/// The SQL does not normalize, or a candidate's plan or run fails.
pub fn explore_plans_for_sql(
    pool: &dyn PlanExploreSessionPool,
    current_db: &str,
    charset: &str,
    collation: &str,
    sql_or_digest: &str,
    analyze: bool,
) -> Result<Vec<BindingPlanInfo>, DriverError> {
    let historical_plans = get_binding_plan_info(pool, current_db, sql_or_digest, charset, collation)?;
    let mut generated_plans =
        crate::binding_plan_generation::generate(pool, current_db, sql_or_digest, charset, collation)?;
    if analyze {
        run_to_get_exec_info(pool, &mut generated_plans)?;
    }
    let mut plan_candidates = historical_plans;
    plan_candidates.extend(generated_plans);
    match fill_recommendation(&mut plan_candidates, &RuleBasedPlanPerfPredictor, "rule-based") {
        Err(error) => return Err(error),
        Ok(true) => return Ok(plan_candidates), // hit a rule
        Ok(false) => {}
    }
    fill_recommendation(&mut plan_candidates, &LlmBasedPlanPerfPredictor, "LLM")?;
    Ok(plan_candidates)
}

/// Go `runToGetExecInfo`: runs the plans without execution info.
fn run_to_get_exec_info(pool: &dyn PlanExploreSessionPool, plans: &mut [BindingPlanInfo]) -> Result<(), DriverError> {
    for plan in plans {
        if plan.exec_times > 0 {
            continue; // execution info already present
        }
        pool.call_with_sctx(&mut |sctx| {
            let (db, use_plan_baselines) = (sctx.current_db(), sctx.use_plan_baselines());
            sctx.set_current_db(&plan.binding.db);
            sctx.set_use_plan_baselines(false);
            let result = sctx.exec_and_get_exec_info(&plan.binding.bind_sql);
            sctx.set_current_db(&db);
            sctx.set_use_plan_baselines(use_plan_baselines);
            let (latency, processed_keys, returned_rows) = result?;
            plan.exec_times = 1;
            plan.avg_latency = latency;
            if let Some(processed_keys) = processed_keys {
                plan.avg_scan_rows = processed_keys;
            }
            plan.avg_returned_rows = returned_rows;
            if plan.avg_returned_rows > 0.0 {
                plan.latency_per_return_row = plan.avg_latency / plan.avg_returned_rows;
                plan.scan_rows_per_return_row = plan.avg_scan_rows / plan.avg_returned_rows;
            }
            Ok(())
        })?;
    }
    Ok(())
}

/// Go `getBindingPlanInfo`: the bindings of the SQL (or its digest) with the
/// execution info of their plans.
fn get_binding_plan_info(
    pool: &dyn PlanExploreSessionPool,
    current_db: &str,
    sql_or_digest: &str,
    _charset: &str,
    _collation: &str,
) -> Result<Vec<BindingPlanInfo>, DriverError> {
    // A 64-byte string without spaces is a digest.
    let sql_or_digest = sql_or_digest.trim();
    let (where_cond, value) = if sql_or_digest.len() == 64 && !sql_or_digest.contains(' ') {
        ("where sql_digest = %?", sql_or_digest.to_owned())
    } else {
        let stmt = tidb_parser::parse(sql_or_digest).map_err(|error| {
            DriverError::Mysql(tidb_terror::MysqlError::unknown(format!("failed to normalize the SQL: {}", error.message)))
        })?;
        let db = crate::binding::get_default_db(&stmt, current_db);
        let (normalized, _) = crate::binding::normalize_stmt_for_binding(&stmt, &db);
        ("where original_sql = %?", normalized)
    };
    let mut bindings = Vec::new();
    pool.call_with_sctx(&mut |sctx| {
        bindings = crate::binding_utils::read_bindings_from_storage(sctx, where_cond, &[SqlArg::from(value.as_str())])?;
        Ok(())
    })?;

    // Plan info from information_schema.tidb_statements_stats.
    let mut binding_plans = Vec::with_capacity(bindings.len());
    for mut binding in bindings {
        if binding.status == crate::binding::STATUS_DELETED {
            continue;
        }
        // Go's `readBindingsFromStorage` ran `prepareHints`: the hints as
        // `hint.ParseHintsSet` normalizes them (query-block names, database).
        let db_name = if binding.table_names.iter().any(|(schema, _)| schema == "*") {
            "*".to_owned()
        } else {
            binding.db.clone()
        };
        match tidb_hint::parse_hints_set(&binding.bind_sql, &binding.charset, &binding.collation, &db_name) {
            Ok((hints, _, _)) => binding.hints = hints,
            Err(_) => continue, // Go logs and drops a binding whose hints do not parse
        }
        if binding.plan_digest.is_empty() {
            let mut plan_digest = String::new();
            let filled = pool.call_with_sctx(&mut |sctx| {
                plan_digest = get_binding_plan_digest(sctx, &binding.db, &binding.bind_sql);
                Ok(())
            });
            if filled.is_err() {
                // Go logs "get plan digest failed" and skips the binding.
                continue;
            }
            binding.plan_digest = plan_digest;
        }
        let Ok(p_info) = get_plan_exec_info(pool, &binding.plan_digest) else {
            // Go logs "get plan execution info failed" and skips it.
            continue;
        };
        let mut auto_binding = BindingPlanInfo {
            binding,
            ..BindingPlanInfo::default()
        };
        // The info can be missing when the statement stats are incomplete.
        if let Some(p_info) = p_info.filter(|info| info.exec_count > 0) {
            auto_binding.plan = p_info.plan;
            auto_binding.exec_times = p_info.exec_count;
            auto_binding.avg_latency = p_info.total_time as f64 / p_info.exec_count as f64;
            auto_binding.avg_scan_rows = p_info.processed_keys as f64 / p_info.exec_count as f64;
            auto_binding.avg_returned_rows = p_info.result_rows as f64 / p_info.exec_count as f64;
            if auto_binding.avg_returned_rows > 0.0 {
                auto_binding.latency_per_return_row = auto_binding.avg_latency / auto_binding.avg_returned_rows;
                auto_binding.scan_rows_per_return_row = auto_binding.avg_scan_rows / auto_binding.avg_returned_rows;
            }
        }
        binding_plans.push(auto_binding);
    }
    Ok(binding_plans)
}

/// Go `getBindingPlanDigest` (`utils.go`): the best effort at a binding's
/// plan digest, empty when its SQL does not parse or plan.
fn get_binding_plan_digest(sctx: &mut dyn PlanExploreSession, schema: &str, binding_sql: &str) -> String {
    let (original_baseline, original_db) = (sctx.use_plan_baselines(), sctx.current_db());
    sctx.set_use_plan_baselines(false);
    sctx.set_current_db(schema);
    let mut plan_digest = String::new();
    if let Ok(stmt) = tidb_parser::parse(binding_sql) {
        // A '?' (`create binding using select a from t where a=?`) could
        // make the plan digest wrong.
        if !crate::binding_plan_generation::has_param(&stmt) {
            plan_digest = sctx.calculate_plan_digest(&stmt).unwrap_or_default();
        }
    }
    sctx.set_use_plan_baselines(original_baseline);
    sctx.set_current_db(&original_db);
    plan_digest
}

/// Go `fillRecommendation`: marks the first best-scoring plan; true when one
/// was recommended.
fn fill_recommendation(
    plans: &mut [BindingPlanInfo],
    predictor: &dyn PlanPerfPredictor,
    name: &str,
) -> Result<bool, DriverError> {
    if plans.is_empty() {
        return Ok(false);
    }
    let (scores, explanations) = predictor.perf_predicate(plans)?;
    let max_score = scores.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    if max_score == 0.0 {
        return Ok(false);
    }
    let mut recommended = false;
    for (i, plan) in plans.iter_mut().enumerate() {
        if scores[i] == max_score && !recommended {
            plan.recommend = format!("YES (from {name})");
            plan.reason = explanations[i].clone();
            recommended = true;
        } else {
            plan.recommend = "NO".to_owned();
        }
    }
    Ok(recommended)
}

/// Go `planExecInfo`.
struct PlanExecInfo {
    plan: String,
    result_rows: i64,
    exec_count: i64,
    processed_keys: i64,
    total_time: i64,
}

/// Go `getPlanExecInfo`: the plan's summed execution info from the
/// statement stats.
fn get_plan_exec_info(pool: &dyn PlanExploreSessionPool, plan_digest: &str) -> Result<Option<PlanExecInfo>, DriverError> {
    if plan_digest.is_empty() {
        return Ok(None);
    }
    // Tests need not read the cluster table.
    let stmt_stats_table = if tidb_intest::IN_TEST {
        "information_schema.tidb_statements_stats"
    } else {
        "information_schema.cluster_tidb_statements_stats"
    };
    let stmt_query = format!(
        "select cast(sum(result_rows) as signed), cast(sum(exec_count) as signed),\n       \
         cast(sum(processed_keys) as signed), cast(sum(total_time) as signed), any_value(plan)\n       \
         from {stmt_stats_table} where plan_digest = '{plan_digest}'"
    );
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    pool.call_with_sctx(&mut |sctx| {
        rows = crate::binding_utils::exec_rows(sctx, &stmt_query, &[])?;
        Ok(())
    })?;
    let Some(row) = rows.first().filter(|row| !row.first().is_none_or(Datum::is_null)) else {
        return Ok(None);
    };
    let int = |index: usize| row.get(index).map_or(0, Datum::get_int64);
    Ok(Some(PlanExecInfo {
        result_rows: int(0),
        exec_count: int(1),
        processed_keys: int(2),
        total_time: int(3),
        plan: row
            .get(4)
            .and_then(tidb_datatype::datum_text::sysvar_datum_text)
            .unwrap_or_default(),
    }))
}

/// Go `IsSimplePointPlan`: whether the plan has only Point_Get,
/// Batch_Point_Get, Selection and Projection operators.
#[must_use]
pub fn is_simple_point_plan(plan: &str) -> bool {
    let mut empty = true;
    for line in plan.split('\n') {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        empty = false;
        let operator_name = line.split(' ').next().unwrap_or_default();
        if operator_name == "id" // the first line with column names
            || operator_name.contains("Point_Get")
            || operator_name.contains("Batch_Point_Get")
            || operator_name.contains("Selection")
            || operator_name.contains("Projection")
        {
            continue;
        }
        return false;
    }
    !empty
}

/// Go `binding_auto_test.go`'s `TestIsSimplePointPlan`.
#[cfg(test)]
mod tests {
    use super::is_simple_point_plan;

    #[test]
    fn test_is_simple_point_plan() {
        assert!(is_simple_point_plan("       id  task    estRows operator info  actRows execution info  memory          disk
        Projection_4    root    1       plus(test.t.a, 1)->Column#3     0       time:173µs, open:24.9µs, close:8.92µs, loops:1, Concurrency:OFF                         380 Bytes       N/A
        └─Point_Get_5   root    1       table:t, handle:2               0       time:143.2µs, open:1.71µs, close:5.92µs, loops:1, Get:{num_rpc:1, total_time:40µs}      N/A             N/A"));
        assert!(is_simple_point_plan("       id  task    estRows operator info  actRows execution info  memory          disk
        Point_Get_5   root    1       table:t, handle:2               0       time:143.2µs, open:1.71µs, close:5.92µs, loops:1, Get:{num_rpc:1, total_time:40µs}      N/A             N/A"));
        assert!(is_simple_point_plan("Point_Get_5   root    1       table:t, handle:2               0       time:143.2µs, open:1.71µs, close:5.92µs, loops:1, Get:{num_rpc:1, total_time:40µs}      N/A             N/A"));
        assert!(is_simple_point_plan("id                      task    estRows operator info                                           actRows execution info                                                                                                                    memory          disk
        Projection_4            root    3.00    plus(test.t.a, 1)->Column#3                             0       time:218.3µs, open:14.5µs, close:9.79µs, loops:1, Concurrency:OFF                                                                 145 Bytes       N/A
        └─Batch_Point_Get_5     root    3.00    table:t, handle:[1 2 3], keep order:false, desc:false   0       time:201.1µs, open:3.83µs, close:6.46µs, loops:1, BatchGet:{num_rpc:2, total_time:65.7µs}, rpc_errors:{epoch_not_match:1} N/A             N/A   "));
        assert!(is_simple_point_plan("id                      task    estRows operator info                                           actRows execution info                                                                                                                    memory          disk
        Batch_Point_Get_5     root    3.00    table:t, handle:[1 2 3], keep order:false, desc:false   0       time:201.1µs, open:3.83µs, close:6.46µs, loops:1, BatchGet:{num_rpc:2, total_time:65.7µs}, rpc_errors:{epoch_not_match:1} N/A             N/A   "));
        assert!(is_simple_point_plan("id                      task    estRows operator info                                           actRows execution info                                                                                                                    memory          disk
        Selection ....
        └─Batch_Point_Get_5     root    3.00    table:t, handle:[1 2 3], keep order:false, desc:false   0       time:201.1µs, open:3.83µs, close:6.46µs, loops:1, BatchGet:{num_rpc:2, total_time:65.7µs}, rpc_errors:{epoch_not_match:1} N/A             N/A   "));

        assert!(!is_simple_point_plan("       id                      task            estRows operator info                           actRows execution info memory          disk
        TableReader_5           root            10000   data:TableFullScan_4                    0       time:456.3µs, open:141µs, close:6.79µs, loops:1, cop_task: {num: 1, max: 241.3µs, proc_keys: 0, copr_cache_hit_ratio: 0.00, build_task_duration: 91.5µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:203.9µs}}      182 Bytes       N/A
        └─TableFullScan_4       cop[tikv]       10000   table:t, keep order:false, stats:pseudo 0       tikv_task:{time:155.2µs, loops:0}                                                                                                                                                                                                         N/A             N/A "));
        assert!(!is_simple_point_plan("id                      task    estRows operator info                                           actRows execution info                                                                                                                    memory          disk
        HashAgg            root    3.00    plus(test.t.a, 1)->Column#3                             0       time:218.3µs, open:14.5µs, close:9.79µs, loops:1, Concurrency:OFF                                                                 145 Bytes       N/A
        └─Batch_Point_Get_5     root    3.00    table:t, handle:[1 2 3], keep order:false, desc:false   0       time:201.1µs, open:3.83µs, close:6.46µs, loops:1, BatchGet:{num_rpc:2, total_time:65.7µs}, rpc_errors:{epoch_not_match:1} N/A             N/A   "));
        assert!(!is_simple_point_plan("       id  task    estRows operator info  actRows execution info  memory          disk
        HashJoin    root    1       plus(test.t.a, 1)->Column#3     0       time:173µs, open:24.9µs, close:8.92µs, loops:1, Concurrency:OFF                         380 Bytes       N/A
        └─Point_Get_5   root    1       table:t, handle:2               0       time:143.2µs, open:1.71µs, close:5.92µs, loops:1, Get:{num_rpc:1, total_time:40µs}      N/A             N/A"));
        assert!(!is_simple_point_plan(""));
        assert!(!is_simple_point_plan("  \\n   "));
    }
}
