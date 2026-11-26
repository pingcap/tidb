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

//! Go `pkg/executor/adapter.go`'s statement observation: `LogSlowQuery`,
//! `SummaryStmt`, `updatePrevStmt` and `recordLastQueryInfo`, over the
//! session they reach back into ([`ExecStmtObservation`]). The session opens
//! and closes the observation (Go `session.runStmt`) and logs the statement
//! (`logStmt`); what the statement measured lives in [`StatementObservation`].

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_hack::GoToLower;
use tidb_stmtsummary::statement_summary::{
    EncodedPlanError, StmtExecInfo, StmtExecLazyInfo, StmtSummaryStmtCtx,
};

pub struct StatementObservation {
    pub sql: String,
    /// Go `stmtNode.Text()` of `sql` ([`Session::statement_text`]), which Go
    /// keeps as `StmtCtx.OriginalSQL`: the summary's sample text, and the
    /// source of `normalized`.
    pub text: String,
    /// Go `SensitiveStmtNode.SecureText`, which every consumer of a
    /// password-bearing statement's text reads instead of the text.
    pub secure_text: Option<String>,
    pub normalized: String,
    pub digest: tidb_parser::Digest,
    pub started: std::time::Instant,
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub label: String,
    pub prepared: bool,
    /// Go `checkPlanReplayerContinuesCaptureValidStmt`: a SELECT, DELETE,
    /// UPDATE, INSERT or EXECUTE.
    pub plan_replayer_continues_capture_valid: bool,
    pub routed: bool,
    /// Go `StmtCtx.OriginalTableHints`: the table hints the statement wrote,
    /// after the query-block handler took the view hints out.
    pub original_table_hints: Vec<tidb_ast::Hint>,
    /// Go `ExecStmt.GetBindingSQLAndDigest`.
    pub binding_sql: String,
    pub binding_digest: String,
    pub phases: Arc<Mutex<StatementPhases>>,
    pub kv_exec_counter: tidb_topsql_stmtstats::KvExecCounterHandle,
    /// Go `StmtCtx.ExecDetails`: what the statement's coprocessor responses
    /// reported, read back when the summary is recorded.
    pub cop_stats: Arc<crate::statement_cop_stats::StatementCopStats>,
    /// Go `ctx.Value(util.RUDetailsCtxKey)`: the statement's resource units,
    /// created with its ctx (`execdetails.ContextWithInitializedExecDetails`)
    /// and charged by every TiKV/TiFlash RPC it issues.
    pub ru_details: Arc<tikv_client::RuDetails>,
    /// Go `ctx.Value(util.ExecDetailsKey)`: the statement's KV and PD waits,
    /// backoffs and traffic, created with the same ctx.
    pub exec_details: Arc<tikv_client::util::ExecDetails>,
    /// Go `SessionVars.RUV2Metrics`: the statement's TiDB-side RU v2
    /// counters (`RUV2MetricsFromContext`), created with its ctx.
    pub ruv2_metrics: Arc<tidb_execdetails::ruv2_metrics::RUV2Metrics>,
    /// Go `recordLastQueryInfo`'s statement test: a `SHOW` other than
    /// `SHOW SESSION_STATES`, an `EXECUTE`, or a DML node.
    pub record_last_query: bool,
    /// Go `logStmt`'s crucial statements (account, privilege and schema
    /// changes), which go to the background log instead of the general log.
    pub crucial: bool,
    /// Go `a.StmtNode.(*ast.SelectStmt)` with `Kind ==
    /// SelectStmtKindSelect`: a plain SELECT, not `TABLE` or `VALUES`.
    pub select_kind_select: bool,
}

#[derive(Default)]
pub struct StatementPhases {
    pub parse: Duration,
    pub parse_before_start: Duration,
    pub compile_started: Option<std::time::Instant>,
    pub compile: Duration,
    pub compile_finished: bool,
    pub compile_measured: bool,
    pub execution_started: bool,
    pub tables: Vec<tidb_stmtsummary::statement_summary::TableEntry>,
    /// Go `StmtExecDetails.WriteSQLRespDuration`: the time the server spent
    /// writing this statement's result to the client.
    pub write_sql_resp: Duration,
    /// Go `ExecStmt.retryCount`: the statement's pessimistic re-executions.
    pub exec_retry_count: u64,
    /// Go `ExecStmt.retryStartTime`: when the last re-execution began.
    pub retry_start: Option<std::time::Instant>,
    /// Go `StmtCtx.IsSQLRegistered`: `ExecuteStmt` registered the SQL text
    /// with Top SQL.
    pub sql_registered: bool,
}

impl StatementPhases {
    /// Go `SetSlowLogItems` / `SummaryStmt`: `ExecRetryTime = TimeTotal -
    /// DurationParse - DurationCompile - time.Since(retryStartTime)`, the
    /// time the failed attempts took; zero without a retry.
    pub fn exec_retry_time(&self, elapsed: Duration) -> Duration {
        match self.retry_start {
            Some(started) if self.exec_retry_count > 0 => elapsed
                .saturating_sub(self.parse)
                .saturating_sub(self.compile)
                .saturating_sub(started.elapsed()),
            _ => Duration::ZERO,
        }
    }
}

/// Go `ExecStmt.isSlowQueryCacheable`: whether this statement may keep its
/// original SQL in the plaintext slow-query cache. Only a directly executed
/// SELECT qualifies. Prepared executions, TABLE, VALUES, and sensitive
/// statements do not.
fn is_slow_query_cacheable(observation: &StatementObservation) -> bool {
    !observation.prepared && observation.secure_text.is_none() && observation.select_kind_select
}

/// Go `variable.PlanCacheParamList` as `StmtExecInfo.SampleParams` holds it:
/// a prepared execution's arguments, `String` rendering them as a log suffix.
struct PlanCacheParams(Vec<tidb_datatype::Datum>);

impl std::fmt::Display for PlanCacheParams {
    /// Go `PlanCacheParamList.String`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return Ok(());
        }
        write!(f, " [arguments: {}]", tidb_datatype::datums_to_str_no_err_smart(&self.0))
    }
}

struct LazyStatement {
    sql: String,
    /// The plan's encodings, rendered here once, when the summary first asks
    /// (Go `LazyInfo.GetEncodedPlan` / `GetBinaryPlan`).
    plan_source: Option<crate::PlanSource>,
    rendered_plan: std::sync::OnceLock<(String, String)>,
    /// `tidb_generate_binary_plan` as the statement finished.
    binary_plan_enabled: bool,
    plan_digest: String,
    plan_hint: String,
    binding_sql: String,
    binding_digest: String,
}
impl LazyStatement {
    fn rendered(&self) -> &(String, String) {
        self.rendered_plan
            .get_or_init(|| self.plan_source.as_ref().map(crate::PlanSource::render).unwrap_or_default())
    }
}
impl StmtExecLazyInfo for LazyStatement {
    fn original_sql(&self) -> String {
        self.sql.clone()
    }
    fn encoded_plan(&self) -> Result<(String, String), EncodedPlanError> {
        Ok((self.rendered().0.clone(), self.plan_hint.clone()))
    }
    fn binary_plan(&self) -> String {
        if !self.binary_plan_enabled {
            return String::new();
        }
        self.rendered().1.clone()
    }
    fn plan_digest(&self) -> String {
        self.plan_digest.clone()
    }
    fn binding_sql_and_digest(&self) -> (String, String) {
        (self.binding_sql.clone(), self.binding_digest.clone())
    }
}

/// The session a statement's observation reaches back into: Go `ExecStmt`'s
/// `sctx` as `FinishExecuteStmt`, `LogSlowQuery`, `SummaryStmt`,
/// `updatePrevStmt` and `recordLastQueryInfo` read it.
pub trait ExecStmtObservation: crate::sessionctx::SessionContext {
    /// Go `ExecStmt.FinishExecuteStmt`: the codec ID when keyspace settings are nonempty.
    fn observed_keyspace_id(&self) -> u32;
    /// The connection the statement runs on.
    fn observed_connection_id(&self) -> u64;
    /// Go `StmtHints.WriteSlowLog`: the `write_slow_log` hint.
    fn write_slow_log_hint(&self) -> bool;
    /// The session memory tracker's peak.
    fn statement_mem_max(&self) -> i64;
    /// The session disk tracker's peak.
    fn statement_disk_max(&self) -> i64;
    /// Go `stmtCtx.MemTracker.MemArbitration().Seconds()`: the time the
    /// statement waited for the memory arbitrator's grants.
    fn statement_mem_arbitration(&self) -> f64;
    /// Go `SessionVars.DurationOptimizer` / `RewritePhaseInfo` /
    /// `DurationWaitTS`.
    fn session_phase_durations(&self) -> &tidb_variable::phase_durations::SessionPhaseDurations;
    /// Go `StmtCtx.ResourceGroupName`.
    fn observed_resource_group(&self) -> String;
    /// The start timestamp of the transaction the statement ended, if it
    /// ended one.
    fn statement_ended_txn_start_ts(&self) -> Option<u64>;
    /// Whether the statement began a transaction.
    fn statement_began_txn(&self) -> bool;
    /// Go `StmtCtx.WaitLockLeaseTime`.
    fn wait_lock_lease_time(&self) -> std::time::Duration;
    /// Go `TxnCtx.StartTS` and `TxnCtx.GetForUpdateTS()`.
    fn txn_start_and_for_update_ts(&self) -> (u64, u64);
    /// The statement's warnings as the slow log prints them.
    fn slow_log_warnings(&self) -> Vec<tidb_variable::slow_log::JSONSQLWarnForSlowLog>;
    /// Go `StmtCtx.WarningCount()`.
    fn statement_warning_count(&self) -> usize;
    /// Go `SQLCPUUsages.GetCPUUsages().TikvCPUTime` of the point readers.
    fn sql_tikv_cpu_time(&self) -> i64;
    /// Go `StmtCtx.PlanCacheUnqualified()`.
    fn plan_cache_unqualified(&self) -> String;
    /// Go `SQLCPUUsages.GetCPUUsages().TidbCPUTime`: what the Top SQL
    /// profiler attributed to the statement so far, in nanoseconds.
    fn sql_tidb_cpu_time(&self) -> i64;
    /// Go `ConnectionInfo.Attributes`.
    fn connection_attributes(&self) -> &std::collections::BTreeMap<String, String>;
    /// Go `PlanCacheParams`: the prepared execution's arguments.
    fn prepared_params(&self) -> Option<&[tidb_datatype::Datum]>;
    /// Go `SessionVars.PrevStmt` / `PrevStmtDigest` as the summary keeps them.
    fn previous_summary_statement(&mut self) -> &mut Option<(String, String)>;
    /// The user the statement summary records.
    fn summary_user(&self) -> String;
    /// The statement's published plan (Go `StmtCtx`'s plan fields).
    fn process_plan_info_cell(&self) -> Arc<Mutex<crate::ProcessPlanInfo>>;

    /// Go `vardef.ProcessGeneralLog` / `cfg.Instance.*`: an instance-scoped
    /// switch read through the session's variable table.
    fn instance_switch_on(&self, name: &str) -> bool {
        self.session_vars()
            .get_system(name)
            .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("ON"))
    }

    /// Go `SessionVars.EnableRedactLog`, one of `OFF`, `ON` and `MARKER`.
    fn redact_log_mode(&self) -> String {
        let mode = self
            .session_vars()
            .enable_redact_log()
            .to_ascii_uppercase();
        match mode.as_str() {
            "ON" | "MARKER" => mode,
            _ => "OFF".to_owned(),
        }
    }

    fn redact_mode(&self) -> tidb_parser::RedactMode {
        match self.redact_log_mode().as_str() {
            "ON" => tidb_parser::RedactMode::Enabled,
            "MARKER" => tidb_parser::RedactMode::Marker,
            _ => tidb_parser::RedactMode::Disabled,
        }
    }

    /// Go `PlanCacheParamList.String`: the prepared execution's arguments,
    /// empty without any.
    fn plan_cache_params_string(&self) -> String {
        match self.prepared_params() {
            Some(params) if !params.is_empty() => format!(
                " [arguments: {}]",
                tidb_datatype::datums_to_str_no_err_smart(params)
            ),
            _ => String::new(),
        }
    }

    /// Go `ExecStmt.GetTextToLog(keepHint)`: the statement text a log may
    /// carry under `tidb_redact_log`, from `StmtCtx.OriginalSQL` (the
    /// statement's `Text()`, client-decoded, not the raw client bytes).
    fn text_to_log(&self, observation: &StatementObservation, keep_hint: bool) -> String {
        let mode = self.redact_log_mode();
        if mode == "ON" {
            if keep_hint {
                tidb_parser::normalize_keep_hint(&observation.text)
            } else {
                observation.normalized.clone()
            }
        } else if let Some(secure_text) = &observation.secure_text {
            secure_text.clone()
        } else {
            tidb_redact::string_cow(
                &mode,
                format!("{}{}", observation.text, self.plan_cache_params_string()),
            )
            .into_owned()
        }
    }

    /// Go `vardef.QueryLogMaxLen`, the GLOBAL `tidb_query_log_max_len`.
    fn query_log_max_len(&self) -> i32 {
        self.session_vars()
            .get_global(tidb_vardef::tidb_vars::TIDB_QUERY_LOG_MAX_LEN)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .map_or(4096, |value| i32::try_from(value).unwrap_or(i32::MAX))
    }

    /// Go `ExecStmt.LogSlowQuery`: writes the statement's entry through the
    /// slow-query logger when the slow-log rules (or, without rules, the
    /// `tidb_slow_log_threshold`) select it, and lands a selected statement in
    /// the domain's `ADMIN SHOW SLOW` memory.
    fn log_slow_query(
        &mut self,
        observation: &StatementObservation,
        elapsed: Duration,
        succeeded: bool,
        result_rows: i64,
        plan: &crate::ProcessPlanInfo,
        phases: &StatementPhases,
    ) {
        let conn_id = self.observed_connection_id();
        // Go's triggers, decided before any entry field is gathered: the
        // `write_slow_log` hint writes the entry without selecting it.
        let mut match_rules = false;
        let mut force = false;
        let mut rules = None;
        if !self.write_slow_log_hint() {
            // Go also forces the entry while runtime tracing is on; this node
            // has no runtime tracer.
            force = tidb_logutil::get_level() <= tidb_log::Level::Debug;
            if !self.instance_switch_on(tidb_vardef::tidb_vars::TIDB_ENABLE_SLOW_LOG) && !force {
                return;
            }
            let effective = tidb_variable::slow_log_rules::EffectiveSlowLogRules::new(
                &self
                    .session_vars()
                    .slow_log_rules().to_owned(),
                &tidb_variable::slow_log_rules::global_slow_log_rules(),
                conn_id,
            );
            if effective.is_empty() {
                // Go stores the threshold as `uint64(TidbOptInt64(..))`, so a
                // negative one never selects a statement.
                let threshold = self
                    .session_vars()
                    .get_system(tidb_vardef::tidb_vars::TIDB_SLOW_LOG_THRESHOLD)
                    .ok()
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(tidb_logutil::DEFAULT_SLOW_THRESHOLD as i64);
                match_rules = elapsed >= Duration::from_millis(threshold as u64);
                if !match_rules && !force {
                    return;
                }
            } else {
                rules = Some(effective);
            }
        }
        let session_alias = self
            .session_vars()
            .session_alias().to_owned();
        let exec_detail = observation.cop_stats.exec_details();
        let cop_tasks = observation.cop_stats.cop_tasks_details();
        // Go `getEncodedPlan` / `getBinaryPlan` as the slow log writes them.
        let (encoded_plan, summary_binary_plan) =
            plan.plan_source.as_ref().map(crate::PlanSource::render).unwrap_or_default();
        let plan_digest = if observation.routed {
            String::new()
        } else {
            plan.plan_digest.clone()
        };
        let mem_max = self.statement_mem_max();
        let disk_max = self.statement_disk_max();
        let durations = self.session_phase_durations().snapshot();
        let kv_exec_detail = tidb_variable::slow_log::kv_exec_details(
            &tidb_variable::slow_log::load_tikv_exec_details(&observation.exec_details),
        );
        if let Some(rules) = rules {
            match_rules = rules.should_write_slow_log(&tidb_variable::slow_log_rules::SlowLogRuleValues {
                conn_id,
                session_alias: session_alias.clone(),
                db: self.session_vars().current_db.clone(),
                exec_retry_count: phases.exec_retry_count,
                query_time: elapsed,
                parse_time: phases.parse,
                compile_time: phases.compile,
                rewrite_time: durations.rewrite_phase_info.duration_rewrite,
                optimize_time: durations.duration_optimizer.total,
                wait_ts: durations.duration_wait_ts,
                is_internal: self.session_vars().restricted_sql,
                digest: observation.digest.to_string(),
                num_cop_tasks: cop_tasks.as_ref().map(|details| details.num_cop_tasks),
                mem_max,
                disk_max,
                write_sql_response_total: phases.write_sql_resp,
                succ: succeeded,
                resource_group: self.observed_resource_group(),
                kv_exec_detail: Some(kv_exec_detail),
                exec_detail: exec_detail.clone(),
                plan_digest: plan_digest.clone(),
            });
            if !match_rules && !force {
                return;
            }
        }

        if !tidb_variable::slow_log::slow_log_rate_limiter_allow() {
            let max_per_sec = tidb_variable::slow_log::slow_log_max_per_sec();
            slow_log_sample_logger().info(
                "slow log skipped due to rate limiting",
                &[tidb_log::Field::new(
                    "tidb_slow_log_max_per_sec",
                    tidb_log::Value::I64(max_per_sec),
                )],
            );
            return;
        }

        let index_names = {
            // Go's `SetSlowLogItems`: the statement's index names,
            // deduplicated in first-use order.
            let mut seen = std::collections::HashSet::new();
            let names: Vec<&str> = if observation.routed {
                Vec::new()
            } else {
                plan.index_names
                    .iter()
                    .map(String::as_str)
                    .filter(|name| seen.insert(*name))
                    .collect()
            };
            if names.is_empty() {
                String::new()
            } else {
                format!("[{}]", names.join(","))
            }
        };
        // Go `FinishExecuteStmt(origTxnCtx.StartTS, ...)`: the transaction
        // context in force when the statement began -- the one a `COMMIT` or
        // `ROLLBACK` ended, and not the one a `BEGIN` opened.
        let txn_ts = match self.statement_ended_txn_start_ts() {
            Some(start_ts) => start_ts,
            None if self.statement_began_txn() => 0,
            None => self.txn_start_and_for_update_ts().0,
        };
        let (storage_kv, storage_mpp) = match &plan.plan {
            Some(stored) if !observation.routed => {
                let (tikv, tiflash) = stored
                    .get::<tidb_planner_core_operator_physicalop::PhysicalPlan>()
                    .map_or((false, false), |plan| plan_storage_flags(&plan));
                (tikv, tiflash || plan.is_tiflash)
            }
            _ => (false, false),
        };
        let login = self
            .session_vars().login_user
            .as_deref()
            .and_then(|identity| identity.split_once('@'))
            .map(|(user, host)| (user.to_owned(), host.to_owned()));
        let keyspace_name = tidb_keyspace::get_keyspace_name_by_settings();
        let items = tidb_variable::slow_log::SlowQueryLogItems {
            txn_ts,
            keyspace_name: if tidb_keyspace::is_keyspace_name_empty(&keyspace_name) {
                String::new()
            } else {
                keyspace_name
            },
            keyspace_id: self.observed_keyspace_id(),
            sql: tidb_variable::slow_log::format_sql(
                &self.text_to_log(observation, true),
                self.query_log_max_len(),
            ),
            digest: observation.digest.to_string(),
            time_total: elapsed,
            index_names,
            succ: succeeded,
            // Go `TxnCtx.IsExplicit`, which an ended transaction's context
            // keeps until the next statement replaces it.
            is_explicit_txn: self.in_txn() || self.statement_ended_txn_start_ts().is_some(),
            // Go `stmtCtx.WaitLockLeaseTime > 0`.
            is_write_cache_table: !self.wait_lock_lease_time().is_zero(),
            is_sync_stats_failed: !observation.routed && plan.is_sync_stats_failed,
            prepared: observation.prepared,
            plan_from_cache: !observation.routed && self.session_vars().found_in_plan_cache,
            plan_from_binding: !observation.routed && self.session_vars().found_in_binding,
            has_more_results: false,
            prev_stmt: if observation.label == "Commit" {
                self.session_vars().prev_stmt_text
                    .as_deref()
                    .map(|text| tidb_variable::slow_log::format_sql(text, self.query_log_max_len()))
                    .unwrap_or_default()
            } else {
                String::new()
            },
            plan: if !observation.routed
                && !encoded_plan.is_empty()
                && self.instance_switch_on(tidb_vardef::tidb_vars::TIDB_RECORD_PLAN_IN_SLOW_LOG)
            {
                format!(
                    "{}{}{}",
                    tidb_variable::slow_log::SLOW_LOG_PLAN_PREFIX,
                    encoded_plan,
                    tidb_variable::slow_log::SLOW_LOG_PLAN_SUFFIX
                )
            } else {
                String::new()
            },
            plan_digest,
            binary_plan: if !observation.routed
                && !summary_binary_plan.is_empty()
                && tidb_variable::sysvar::generate_binary_plan()
            {
                format!(
                    "{}{}{}",
                    tidb_variable::slow_log::SLOW_LOG_BINARY_PLAN_PREFIX,
                    summary_binary_plan,
                    tidb_variable::slow_log::SLOW_LOG_PLAN_SUFFIX
                )
            } else {
                String::new()
            },
            cop_tasks,
            // Go `SQLCPUUsages.GetCPUUsages()`: every coprocessor
            // response's process time (`selectResult.updateCopRuntimeStats`)
            // and every point reader's (`PointGetExecutor.Close`).
            cpu_usages: tidb_variable::slow_log::CpuUsages {
                tikv_cpu_time: exec_detail.cop_exec_details.time_detail.process_time
                    + Duration::from_nanos(
                        u64::try_from(self.sql_tikv_cpu_time())
                            .unwrap_or(0),
                    ),
                tidb_cpu_time: Duration::from_nanos(u64::try_from(self.sql_tidb_cpu_time()).unwrap_or(0)),
            },
            storage_kv,
            storage_mpp,
            exec_detail,
            result_rows,
            warnings: self.slow_log_warnings(),
            resource_group_name: self.observed_resource_group(),
            ru_details: Some(Arc::clone(&observation.ru_details)),
            mem_max,
            mem_arbitration: self.statement_mem_arbitration(),
            disk_max,
            session_connect_attrs: self.connection_attributes().clone(),
            used_stats: if observation.routed {
                std::collections::BTreeMap::new()
            } else {
                plan.used_stats.clone()
            },
            rewrite_info: durations.rewrite_phase_info,
            kv_exec_detail: Some(kv_exec_detail),
            write_sql_resp_total: phases.write_sql_resp,
            ruv2_metrics: Some((*observation.ruv2_metrics).clone()),
            exec_retry_count: phases.exec_retry_count,
            exec_retry_time: phases.exec_retry_time(elapsed),
            ..tidb_variable::slow_log::SlowQueryLogItems::default()
        };
        let vars = tidb_variable::slow_log::SlowLogSessionVars {
            client_ip: login.as_ref().map(|(_, host)| host.clone()),
            user: login,
            connection_id: conn_id,
            session_alias: session_alias.clone(),
            duration_parse: phases.parse,
            duration_compile: phases.compile,
            duration_optimizer: durations.duration_optimizer,
            duration_wait_ts: durations.duration_wait_ts,
            ruv2_weights: tidb_variable::slow_log::ruv2_weights(),
            current_db: self.session_vars().current_db.clone(),
            in_restricted_sql: self.session_vars().restricted_sql,
            current_db_changed: self.session_vars().current_db_changed,
            ..tidb_variable::slow_log::SlowLogSessionVars::default()
        };
        // Go `LogSlowQuery`: tidb_redact_log OFF already writes the original
        // SQL to the slow log, so the plaintext cache is skipped. ON and
        // MARKER redact that line; only a non-sensitive SELECT is queued,
        // before the redacted line is written. A full queue drops the cache
        // entry and the disk write still happens.
        if self.redact_log_mode() != "OFF" && is_slow_query_cacheable(observation) {
            crate::slow_query_cache::put_into_global_cache(
                crate::slow_query_cache::slow_query_key_of(
                    items.txn_ts,
                    crate::slow_query_cache::slow_query_time_key(items.time_total),
                    &items.digest,
                ),
                observation.text.clone(),
            );
        }
        let entry = tidb_variable::slow_log::slow_log_format(&vars, &items);
        // Go `SlowLogFormat` clears `CurrentDBChanged` once it printed `use`.
        self.session_vars_mut().current_db_changed = false;
        tidb_logutil::slow_query_logger().warn(&entry, &[]);

        if !match_rules {
            return;
        }
        let table_ids = if plan.table_ids.is_empty() {
            String::new()
        } else {
            // Go `strings.ReplaceAll(fmt.Sprintf("%v", TableIDs), " ", ",")`.
            format!(
                "[{}]",
                plan.table_ids
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            )
        };
        tidb_domain::topn_slow_query::log_slow_query(tidb_domain::topn_slow_query::SlowQueryInfo {
            sql: items.sql,
            start: Some(observation.start_time.into()),
            duration: elapsed,
            detail: items.exec_detail.to_string(),
            succ: succeeded,
            conn_id,
            sess_alias: session_alias,
            txn_ts,
            user: self.session_vars().current_user.clone().unwrap_or_default(),
            db: self.session_vars().current_db.clone(),
            table_ids,
            index_names: items.index_names,
            digest: items.digest,
            internal: self.session_vars().restricted_sql,
        });
    }

    /// Go `FinishExecuteStmt`: the statement's commit and lock-keys details
    /// attach to its plan's root operator in its `RuntimeStatsColl`
    /// (`RuntimeStatsWithCommit`), which `EXPLAIN FOR CONNECTION` renders.
    fn register_commit_runtime_stats(&self, exec_detail: &tidb_execdetails::exec_details::ExecDetails) {
        if exec_detail.commit_detail.is_none()
            && exec_detail.lock_keys_detail.is_none()
            && exec_detail.shared_lock_keys_detail.is_none()
        {
            return;
        }
        let cell = self.process_plan_info_cell();
        let cell = cell
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A statement whose own plan is nil (a binary EXECUTE) keeps the last
        // valid plan, whose collector is not this statement's.
        if cell.plan_retained {
            return;
        }
        let plan = cell
            .plan
            .as_ref()
            .and_then(|plan| plan.get::<tidb_planner_core_operator_physicalop::PhysicalPlan>());
        let coll = cell.runtime_stats_coll.as_ref().and_then(|coll| {
            coll.get::<Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>>()
        });
        if let (Some(plan), Some(coll)) = (plan, coll) {
            coll.register_stats(
                i64::from(plan.id()),
                Box::new(tidb_execdetails::runtime_stats::RuntimeStatsWithCommit {
                    commit: exec_detail.commit_detail.clone(),
                    lock_keys: exec_detail.lock_keys_detail.clone(),
                    shared_lock_keys: exec_detail.shared_lock_keys_detail.clone(),
                    txn_cnt: 0,
                }),
            );
        }
    }

    /// Go `ExecStmt.recordAffectedRows2Metrics`: the statement's affected
    /// rows under its type's `AffectedRowsCounter` label.
    fn record_affected_rows_2_metrics(&self, stmt_type: &str, affected_rows: u64) {
        if affected_rows == 0 {
            return;
        }
        // Go `shouldSkipAffectedRowsMetricForInternalMVMaintenance`: "Internal
        // MV maintenance DMLs are already rolled up and reported by the outer
        // REFRESH MATERIALIZED VIEW / PURGE MATERIALIZED VIEW LOG statement."
        let dml = matches!(
            stmt_type,
            "Insert" | "Replace" | "Delete" | "Update" | "NTDML-Delete" | "NTDML-Update" | "NTDML-Insert" | "NTDML-Replace"
        );
        if dml && self.session_vars().restricted_sql && self.session_vars().in_mview_maintenance {
            return;
        }
        let label = match stmt_type {
            "Insert" | "Replace" | "Delete" | "Update" | "NTDML-Delete" | "NTDML-Update" | "NTDML-Insert" | "NTDML-Replace" => stmt_type,
            "PurgeMaterializedViewLog" => "PurgeMLog",
            "RefreshMaterializedView" => "RefreshMV",
            _ => return,
        };
        tidb_metrics::executor::AFFECTED_ROWS.with_label_values(&[label]).inc_by(affected_rows as f64);
    }

    /// Go `ExecStmt.updatePrevStmt`: the statement text a later slow
    /// `COMMIT` prints as `Prev_stmt` (`FormatSQL(GetTextToLog(false))`).
    fn update_prev_stmt(&mut self, observation: &StatementObservation) {
        let text = self.text_to_log(observation, false);
        self.session_vars_mut().prev_stmt_text = Some(text);
    }

    /// Go `ExecStmt.recordLastQueryInfo`: after a `SHOW` (but not `SHOW
    /// SESSION_STATES`), an `EXECUTE` or a DML statement, the transaction
    /// timestamps, the statement's read and write RU and its error.
    fn record_last_query_info(
        &mut self,
        observation: &StatementObservation,
        error: Option<String>,
    ) {
        if !observation.record_last_query {
            return;
        }
        let (start_ts, for_update_ts) = self.txn_start_and_for_update_ts();
        self.session_vars_mut().last_query_info = tidb_sessionstates::QueryInfo {
            txn_scope: "global".to_owned(),
            start_ts,
            for_update_ts,
            ru_consumption: observation.ru_details.read_ru() + observation.ru_details.write_ru(),
            err_msg: error.unwrap_or_default(),
        };
    }


    /// Go `ExecStmt.SummaryStmt`: the statement's statement-summary record,
    /// unless the summary admission skips it.
    fn summary_stmt(
        &mut self,
        observation: &StatementObservation,
        phases: &StatementPhases,
        elapsed: Duration,
        succeeded: bool,
        affected_rows: u64,
        result_rows: i64,
    ) {
        let ruv2_weights = tidb_variable::slow_log::ruv2_weights();
        use crate::adapter::{
            decide_summary_stmt, SummaryAction, SummaryGate, SummaryStmtKind,
        };
        let user = self.summary_user();
        let action = decide_summary_stmt(&SummaryGate {
            // Go `sessVars.InRestrictedSQL`: an internal session's statements
            // are internal. EXPLAIN EXPLORE has no runtime consumer here yet.
            in_restricted_sql: self.session_vars().restricted_sql,
            user_name: user.clone(),
            in_explain_explore: false,
            summary_enabled: tidb_stmtsummary::v2::stmtsummary::enabled(),
            summary_internal_enabled: tidb_stmtsummary::v2::stmtsummary::enabled_internal(),
            stmt_kind: match observation.label.as_str() {
                "Prepare" => SummaryStmtKind::Prepare,
                "Commit" => SummaryStmtKind::Commit,
                _ => SummaryStmtKind::Other,
            },
            prev_stmt_digest: self
                .previous_summary_statement()
                .as_ref()
                .map_or_else(String::new, |(_, digest)| digest.clone()),
        });
        let (internal, attributed_prev_digest) = match action {
            SummaryAction::ClearPrevDigestAndSkip => {
                *self.previous_summary_statement() = None;
                return;
            }
            SummaryAction::SkipPrepare | SummaryAction::SkipCommitWithoutPrevDigest => return,
            SummaryAction::Record {
                is_internal,
                attributed_prev_digest,
            } => (is_internal, attributed_prev_digest),
        };
        let prev_sql = if attributed_prev_digest.is_some() {
            self.previous_summary_statement()
                .as_ref()
                .map_or_else(String::new, |(sql, _)| sql.clone())
        } else {
            String::new()
        };
        let prev_sql_digest = attributed_prev_digest.unwrap_or_default();
        // Go `ExecStmt.getLazyStmtText` (`GetOriginalSQL`, v1's sample) and
        // `updatePrevStmt` (`PrevStmt`, formatted by `formatSQL`): the text a
        // log may carry under `tidb_redact_log`.
        let summary_sql = self.text_to_log(observation, false);
        *self.previous_summary_statement() = Some((
            tidb_variable::slow_log::format_sql(&summary_sql, self.query_log_max_len()),
            observation.digest.to_string(),
        ));
        // Go `SummaryStmt`: `SampleParams = sessVars.PlanCacheParams`, the
        // v2 record's unredacted sample suffix.
        let sample_params = self
            .prepared_params()
            .filter(|params| !params.is_empty())
            .map(|params| {
                Arc::new(PlanCacheParams(params.to_vec())) as Arc<dyn std::fmt::Display + Send + Sync>
            });
        let plan = self
            .process_plan_info_cell()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut ctx = StmtSummaryStmtCtx::default();
        ctx.original_sql = observation.text.clone();
        ctx.stmt_type = observation.label.clone();
        ctx.tables = phases.tables.clone();
        ctx.index_names = if observation.routed {
            Vec::new()
        } else {
            plan.index_names
        };
        ctx.add_affected_rows(affected_rows);
        ctx.set_warning_count(u32::try_from(self.statement_warning_count()).unwrap_or(u32::MAX));
        tidb_stmtsummary::v2::stmtsummary::add(&StmtExecInfo {
            schema_name: self.session_vars().current_db.go_to_lower(),
            charset: self
                .session_vars()
                .get_system("character_set_connection")
                .unwrap_or_default(),
            collation: self
                .session_vars()
                .get_system("collation_connection")
                .unwrap_or_default(),
            normalized_sql: observation.normalized.clone(),
            sensitive_stmt: observation.secure_text.clone(),
            sample_params,
            digest: observation.digest.to_string(),
            prev_sql,
            prev_sql_digest,
            plan_digest: if observation.routed {
                String::new()
            } else {
                plan.plan_digest.clone()
            },
            user,
            total_latency: elapsed,
            parse_latency: phases.parse,
            compile_latency: phases.compile,
            stmt_ctx: Arc::new(ctx),
            cop_tasks: observation.cop_stats.cop_tasks_summary(),
            exec_detail: observation.cop_stats.exec_details(),
            mem_max: self.statement_mem_max(),
            mem_arbitration: self.statement_mem_arbitration(),
            disk_max: self.statement_disk_max(),
            start_time: observation.start_time,
            is_internal: internal,
            succeed: succeeded,
            plan_in_cache: !observation.routed && self.session_vars().found_in_plan_cache,
            plan_in_binding: !observation.routed && self.session_vars().found_in_binding,
            exec_retry_count: phases.exec_retry_count,
            exec_retry_time: phases.exec_retry_time(elapsed),
            write_sql_resp_duration: phases.write_sql_resp,
            result_rows,
            tikv_exec_details: Some(tidb_variable::slow_log::load_tikv_exec_details(
                &observation.exec_details,
            )),
            prepared: observation.prepared,
            keyspace_name: tidb_config::config_tree::config::get_global_config()
                .keyspace_name
                .clone(),
            keyspace_id: self.observed_keyspace_id(),
            resource_group_name: self.observed_resource_group(),
            ru_detail: Some(Arc::clone(&observation.ru_details)),
            // Go `calculateStatementTotalRUV2`.
            total_ru_v2: tidb_execdetails::ruv2_metrics::total_ru(
                Some(&observation.ruv2_metrics),
                ruv2_weights,
                observation.ru_details.tikv_ru_v2(),
                observation.ru_details.tiflash_ru(),
            ),
            // Go `SQLCPUUsages.GetCPUUsages()`, whose TiKV time this node
            // keeps as the coprocessor responses' plus the point readers'.
            cpu_usages: tidb_ppcpuusage::CpuUsages {
                tikv_cpu_time: i64::try_from(
                    observation
                        .cop_stats
                        .exec_details()
                        .cop_exec_details
                        .time_detail
                        .process_time
                        .as_nanos(),
                )
                .unwrap_or(i64::MAX)
                .wrapping_add(self.sql_tikv_cpu_time()),
                tidb_cpu_time: self.sql_tidb_cpu_time(),
            },
            plan_cache_unqualified: self.plan_cache_unqualified(),
            lazy_info: Arc::new(LazyStatement {
                plan_hint: if observation.routed || internal {
                    String::new()
                } else {
                    // Go `getEncodedPlan`: the hints reproducing the plan,
                    // then the statement's own hints of these kinds.
                    let mut hints = plan.plan_hints.clone();
                    hints.extend(
                        observation
                            .original_table_hints
                            .iter()
                            .filter(|hint| {
                                matches!(
                                    hint.lower_name().as_str(),
                                    "memory_quota"
                                        | "use_toja"
                                        | "no_index_merge"
                                        | "max_execution_time"
                                        | "ignore_index"
                                        | "read_from_storage"
                                        | "merge"
                                        | "semi_join_rewrite"
                                        | "no_decorrelate"
                                )
                            })
                            .cloned(),
                    );
                    tidb_hint::restore_optimizer_hints(&hints)
                },
                binding_sql: observation.binding_sql.clone(),
                binding_digest: observation.binding_digest.clone(),
                sql: summary_sql,
                plan_digest: if observation.routed {
                    String::new()
                } else {
                    plan.plan_digest
                },
                binary_plan_enabled: !observation.routed && tidb_variable::sysvar::generate_binary_plan(),
                plan_source: (!observation.routed).then_some(plan.plan_source).flatten(),
                rendered_plan: std::sync::OnceLock::new(),
            }),
        });
    }
}

/// Go `StmtCtx.IsTiKV` / `IsTiFlash` as the executor builder marks them: a
/// TiKV table reader, an index reader, index lookup or index merge reader, a
/// point or batch point get, or a table sample reads TiKV; a TiFlash table
/// reader reads TiFlash.
fn plan_storage_flags(plan: &tidb_planner_core_operator_physicalop::PhysicalPlan) -> (bool, bool) {
    use tidb_planner_core_operator_physicalop::PhysicalPlan;
    use tidb_kv::store_type::StoreType;
    let (mut tikv, mut tiflash) = (false, false);
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        match node {
            PhysicalPlan::TableReader(reader) => match reader.store_type {
                StoreType::TiKv => tikv = true,
                StoreType::TiFlash => tiflash = true,
                _ => {}
            },
            PhysicalPlan::IndexReader(_)
            | PhysicalPlan::IndexLookUpReader(_)
            | PhysicalPlan::IndexMergeReader(_)
            | PhysicalPlan::PointGet(_)
            | PhysicalPlan::BatchPointGet(_)
            | PhysicalPlan::TableSample(_) => tikv = true,
            // Go builds these producers' readers too, which set the flags;
            // they are owned fields here, not children.
            PhysicalPlan::CTE(cte) => {
                stack.push(&cte.seed_plan);
                stack.extend(cte.recur_plan.as_deref());
            }
            PhysicalPlan::Dml(dml) => stack.extend(dml.select_plan.as_deref()),
            _ => {}
        }
        stack.extend(node.children());
    }
    (tikv, tiflash)
}

/// Go `executor.sampleLoggerFactory`: one entry per minute, category
/// `slow log`.
fn slow_log_sample_logger() -> tidb_logutil::SampledLogger {
    static LOGGER: std::sync::LazyLock<tidb_logutil::SampledLogger> =
        std::sync::LazyLock::new(|| {
            tidb_logutil::sample_logger_factory(
                Duration::from_secs(60),
                1,
                vec![tidb_log::Field::new(
                    tidb_logutil::LOG_FIELD_CATEGORY,
                    tidb_log::Value::Str("slow log".to_owned()),
                )],
            )()
        });
    LOGGER.clone()
}

/// Go `AddPlanReplayerFinishedTaskKey`'s bound: the session keeps at most
/// this many continuous-capture keys before it starts over.
const PLAN_REPLAYER_SESSION_FINISHED_TASK_KEY_LEN: usize = 128;

/// What Go `ExecStmt.checkPlanReplayerCapture` reaches through `sctx` and
/// its domain's `PlanReplayerHandle`.
pub trait PlanReplayerCapture: crate::sessionctx::SessionContext + crate::plan_replayer::PlanReplayerSession {
    /// Go `GetPlanReplayerHandle().GetTasks()`; `None` without a handle.
    fn plan_replayer_capture_tasks(&self) -> Option<Vec<tidb_replayer::PlanReplayerTaskKey>>;
    /// Go `GetPlanReplayerHandle().SendTask(dumpTask)`.
    fn send_plan_replayer_task(&self, task: tidb_domain::plan_replayer::PlanReplayerDumpTask);
    /// Go `sessionBindHandle.GetAllSessionBindings()` as the nine fields
    /// `dumpSessionBindRecords` writes per binding.
    fn session_binding_records(&self) -> Vec<Vec<String>>;

    /// Go `ExecStmt.checkPlanReplayerCapture`, the first step of
    /// `FinishExecuteStmt`: a matching capture task, or the continuous
    /// capture, sends this statement to the dump workers.
    fn check_plan_replayer_capture(
        &mut self,
        observation: &StatementObservation,
        plan: &tidb_stmtctx::ProcessPlanInfo,
        txn_ts: u64,
    ) {
        // Go also skips the statistics request sources; this node's
        // statistics statements all run on restricted sessions.
        let vars = self.session_vars();
        let continues = vars.enable_plan_replayed_continues_capture();
        // Go `IsPlanReplayerCaptureEnabled`.
        if vars.restricted_sql || !(vars.enable_plan_replayer_capture() || continues) {
            return;
        }
        let Some(tasks) = self.plan_replayer_capture_tasks() else {
            return;
        };
        let key = tidb_replayer::PlanReplayerTaskKey {
            sql_digest: observation.digest.as_str().to_owned(),
            plan_digest: plan.plan_digest.clone(),
        };
        if continues {
            // Go `checkPlanReplayerContinuesCapture`.
            if !observation.plan_replayer_continues_capture_valid
                || self.session_vars().plan_replayer_finished_task_key.contains(&key)
            {
                return;
            }
            self.send_plan_replayer_dump_task(key.clone(), observation, plan, txn_ts, true);
            // Go `AddPlanReplayerFinishedTaskKey`.
            let finished = &mut self.session_vars_mut().plan_replayer_finished_task_key;
            if finished.len() >= PLAN_REPLAYER_SESSION_FINISHED_TASK_KEY_LEN {
                finished.clear();
            }
            finished.insert(key);
            return;
        }
        // Go `checkPlanReplayerCaptureTask`.
        if tasks.is_empty() || key.sql_digest.is_empty() || key.plan_digest.is_empty() {
            return;
        }
        if tasks
            .iter()
            .any(|task| task.sql_digest == key.sql_digest && (task.plan_digest == "*" || task.plan_digest == key.plan_digest))
        {
            self.send_plan_replayer_dump_task(key, observation, plan, txn_ts, false);
        }
    }

    /// Go `sendPlanReplayerDumpTask`.
    fn send_plan_replayer_dump_task(
        &self,
        key: tidb_replayer::PlanReplayerTaskKey,
        observation: &StatementObservation,
        plan: &tidb_stmtctx::ProcessPlanInfo,
        start_ts: u64,
        is_continues_capture: bool,
    ) {
        self.send_plan_replayer_task(tidb_domain::plan_replayer::PlanReplayerDumpTask {
            key,
            start_ts,
            session_bindings: self.session_binding_records(),
            session_vars: self.plan_replayer_session_vars(),
            // Go `[]ast.StmtNode{stmtNode}`, an EXECUTE's being the prepared
            // statement: the statement's original text either way.
            exec_stmts: vec![observation.text.clone()],
            // Go `getEncodedPlan(stmtCtx, false)`.
            encoded_plan: plan.plan_source.as_ref().map(tidb_stmtctx::PlanSource::render).unwrap_or_default().0,
            analyze: false,
            is_capture: true,
            is_continues_capture,
            ..tidb_domain::plan_replayer::PlanReplayerDumpTask::default()
        });
    }
}

/// Go `hex.DecodeString` of a plan digest's text, the bytes Top SQL keys a
/// plan by (an odd or non-hex tail is dropped).
#[must_use]
pub fn decode_hex_digest(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks_exact(2)
        .map_while(|pair| std::str::from_utf8(pair).ok().and_then(|pair| u8::from_str_radix(pair, 16).ok()))
        .collect()
}

/// Go `ExecStmt.FinishExecuteStmt` over its session.
pub trait FinishExecuteStmt: ExecStmtObservation + PlanReplayerCapture {
    /// The session's Top SQL statement stats (`sctx.GetStmtStats()`).
    fn statement_stats(&self) -> &Arc<tidb_topsql_stmtstats::StatementStats>;
    /// Go's Top RU user of the session.
    fn top_ru_user(&self) -> String;

    /// Go `ExecStmt.FinishExecuteStmt` for the observed statement:
    /// `observeStmtFinishedForTopProfiling`, the RU v2 metrics' finalization
    /// and, for a statement that compiled, `checkPlanReplayerCapture`,
    /// `LogSlowQuery`, `PrevStmt`, `recordLastQueryInfo` and
    /// `recordAffectedRows2Metrics`; then `SummaryStmt`.
    #[allow(clippy::too_many_arguments)]
    fn finish_execute_stmt(
        &mut self,
        observation: &StatementObservation,
        phases: &StatementPhases,
        elapsed: Duration,
        succeeded: bool,
        affected_rows: u64,
        result_rows: i64,
        error: Option<String>,
    ) {
        use tidb_topsql_stmtstats::StatementObserver as _;
        // Go `observeStmtFinishedForTopProfiling`: Top RU alone also records
        // the finish, with the statement's `RUDetails` and user.
        let top_ru = tidb_topsql_state::top_ru_enabled();
        if phases.execution_started && (tidb_topsql_state::top_sql_enabled() || top_ru) {
            // Go `GetPlanDigest(sc)`, as the begin observed it.
            let plan_digest = decode_hex_digest(
                &self
                    .process_plan_info_cell()
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .plan_digest,
            );
            self.statement_stats().on_execution_finished(
                observation.digest.as_bytes(),
                &plan_digest,
                Some(&tidb_topsql_stmtstats::ExecFinishInfo {
                    exec_duration_ns: i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX),
                    top_ru_enabled: top_ru,
                    ru_details: top_ru.then(|| Arc::clone(&observation.ru_details)),
                    user: if top_ru { self.top_ru_user() } else { String::new() },
                    ..tidb_topsql_stmtstats::ExecFinishInfo::default()
                }),
            );
        } else {
            self.statement_stats().clear_ru_exec_context();
        }
        // Go `finalizeStatementRUV2Metrics`, before the slow log and the
        // summary read the metrics: the raw TiKV RU v2 counters the
        // statement's RPCs left in its `RUDetails`.
        tidb_execdetails::ruv2_metrics::sync_ruv_2_metrics_from_ru_details(
            Some(&observation.ruv2_metrics),
            Some(&observation.ru_details),
        );
        // Go runs these from `FinishExecuteStmt`, which only a compiled
        // statement reaches.
        if phases.compile_finished || phases.execution_started {
            let plan_info = self.process_plan_info_cell();
            let plan = plan_info.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let (txn_ts, _) = self.txn_start_and_for_update_ts();
            self.check_plan_replayer_capture(observation, &plan, txn_ts);
            self.log_slow_query(observation, elapsed, succeeded, result_rows, &plan, phases);
            drop(plan);
            self.update_prev_stmt(observation);
            self.record_last_query_info(observation, error);
            self.record_affected_rows_2_metrics(&observation.label, affected_rows);
            // Go: restricted SQL helpers already record query durations.
            // One sample per user statement, its total cost including
            // parsing, under each of its database labels.
            if !self.session_vars().restricted_sql {
                let sql_type = if observation.label.is_empty() {
                    "general"
                } else {
                    observation.label.as_str()
                };
                let record_db_label =
                    tidb_config::config_tree::config::get_global_config().status.record_db_label;
                let resource_group = self.observed_resource_group();
                for db in tidb_metrics::metricsutil::get_db_names(
                    record_db_label,
                    phases.tables.iter().map(|table| table.db.as_str()),
                    &self.session_vars().current_db,
                ) {
                    tidb_metrics::server::QUERY_DURATION_HISTOGRAM
                        .with_label_values(&[sql_type, &db, &resource_group])
                        .observe(elapsed.as_secs_f64());
                }
            }
        }
        self.summary_stmt(observation, phases, elapsed, succeeded, affected_rows, result_rows);
    }
}

/// The statement's plan as Go `observeStmtBeginForTopProfiling` reads it off
/// the statement context: whether it is a fast plan, its normalized text and
/// its digest's hex text (empty until the plan is published).
pub struct TopProfilingPlan {
    pub is_fast_plan: bool,
    pub normalized_plan: String,
    pub plan_digest: String,
}

/// Go `ExecStmt.observeStmtBeginForTopProfiling`, run once the executor is
/// built: the statement's begin in its Top SQL stats (Top RU with its
/// `RUDetails` and user), and with Top Profiling on, its SQL and plan
/// registered and attached to this thread's labels. A non-fast execution is
/// admitted while profiling is off, so enabling it during the execution
/// still observes the begin.
#[allow(clippy::too_many_arguments)]
pub fn observe_stmt_begin_for_top_profiling(
    stats: &Arc<tidb_topsql_stmtstats::StatementStats>,
    kv_exec_counter: &tidb_topsql_stmtstats::KvExecCounterHandle,
    normalized: &str,
    digest: &tidb_parser::Digest,
    restricted_sql: bool,
    sql_registered: bool,
    plan: &TopProfilingPlan,
    ru_details: &Arc<tikv_client::RuDetails>,
    user: &str,
) {
    use tidb_topsql_stmtstats::StatementObserver as _;
    let top_profiling = tidb_topsql_state::top_profiling_enabled();
    if plan.is_fast_plan && !top_profiling {
        return;
    }
    // Go `GetPlanDigest`: empty until the plan is published.
    let plan_digest_bytes = decode_hex_digest(&plan.plan_digest);
    let sql_digest = (!normalized.is_empty()).then_some(digest);
    let plan_digest =
        (!plan.plan_digest.is_empty()).then(|| tidb_parser::Digest::new(plan_digest_bytes.clone()));
    let info = top_profiling_begin_info(ru_details, user);
    stats.on_execution_begin(digest.as_bytes(), &plan_digest_bytes, Some(&info));
    if !top_profiling {
        // Always attach the SQL and plan info uses to catch the running SQL
        // when Top Profiling is enabled in execution.
        tidb_topsql::attach_sql_and_plan_info(
            tidb_cpuprofile::labels::goroutine_labels(),
            sql_digest,
            plan_digest.as_ref(),
        );
        return;
    }
    if tidb_topsql_state::top_sql_enabled() {
        // This is a special logic prepared for TiKV's SQLExecCount.
        kv_exec_counter.initialize(stats, digest.as_bytes(), &plan_digest_bytes);
    }
    if !sql_registered {
        tidb_topsql::register_sql(normalized, sql_digest, restricted_sql);
    }
    if plan.normalized_plan.is_empty() {
        return;
    }
    tidb_topsql::register_plan(&plan.normalized_plan, plan_digest.as_ref());
    tidb_topsql::attach_sql_and_plan_info(
        tidb_cpuprofile::labels::goroutine_labels(),
        sql_digest,
        plan_digest.as_ref(),
    );
}

/// Go `observeStmtBeginForTopProfiling`'s `ExecBeginInfo`: with Top RU on,
/// the statement's `RUDetails`, user and RU version.
#[must_use]
pub fn top_profiling_begin_info(
    ru_details: &Arc<tikv_client::RuDetails>,
    user: &str,
) -> tidb_topsql_stmtstats::ExecBeginInfo {
    let top_ru = tidb_topsql_state::top_ru_enabled();
    let mut info = tidb_topsql_stmtstats::ExecBeginInfo {
        top_ru_enabled: top_ru,
        ..Default::default()
    };
    if top_ru {
        info.ru_details = Some(Arc::clone(ru_details));
        info.user = user.to_owned();
        // Go `stmtstats.NormalizeRUVersion(do.GetRUVersion())`.
        info.ru_version = tidb_topsql_stmtstats::normalize_ru_version(
            tidb_topsql_stmtstats::bound_ru_version(),
        );
    }
    info
}
