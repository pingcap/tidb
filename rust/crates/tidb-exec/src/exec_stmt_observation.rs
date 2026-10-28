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
    pub routed: bool,
    /// Go `StmtCtx.OriginalTableHints`: the table hints the statement wrote,
    /// after the query-block handler took the view hints out.
    pub original_table_hints: Vec<tidb_ast::Hint>,
    /// Go `ExecStmt.GetBindingSQLAndDigest`.
    pub binding_sql: String,
    pub binding_digest: String,
    pub phases: Arc<Mutex<StatementPhases>>,
    pub kv_exec_counter: tidb_util::topsql_stmtstats::KvExecCounterHandle,
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
    pub ruv2_metrics: Arc<tidb_util::ruv2_metrics::RuV2Metrics>,
    /// Go `recordLastQueryInfo`'s statement test: a `SHOW` other than
    /// `SHOW SESSION_STATES`, an `EXECUTE`, or a DML node.
    pub record_last_query: bool,
    /// Go `logStmt`'s crucial statements (account, privilege and schema
    /// changes), which go to the background log instead of the general log.
    pub crucial: bool,
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

struct LazyStatement {
    sql: String,
    binary_plan: String,
    encoded_plan: String,
    /// A plan published without its encodings renders them here, once, when
    /// the summary first asks (Go `LazyInfo.GetEncodedPlan`).
    plan_source: Option<tidb_executor::PlanSource>,
    rendered_plan: std::sync::OnceLock<(String, String)>,
    /// `tidb_generate_binary_plan` as the statement finished.
    binary_plan_enabled: bool,
    plan_digest: String,
    plan_hint: String,
    binding_sql: String,
    binding_digest: String,
}
impl StmtExecLazyInfo for LazyStatement {
    fn original_sql(&self) -> String {
        self.sql.clone()
    }
    fn encoded_plan(&self) -> Result<(String, String), EncodedPlanError> {
        let encoded = match &self.plan_source {
            Some(source) if self.encoded_plan.is_empty() => {
                self.rendered_plan.get_or_init(|| source.render()).0.clone()
            }
            _ => self.encoded_plan.clone(),
        };
        Ok((encoded, self.plan_hint.clone()))
    }
    fn binary_plan(&self) -> String {
        if !self.binary_plan_enabled {
            return String::new();
        }
        match &self.plan_source {
            Some(source) if self.binary_plan.is_empty() => {
                self.rendered_plan.get_or_init(|| source.render()).1.clone()
            }
            _ => self.binary_plan.clone(),
        }
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
pub trait ExecStmtObservation: tidb_executor::sessionctx::SessionContext {
    /// The connection the statement runs on.
    fn observed_connection_id(&self) -> u64;
    /// Go `StmtHints.WriteSlowLog`: the `write_slow_log` hint.
    fn write_slow_log_hint(&self) -> bool;
    /// The session memory tracker's peak.
    fn statement_mem_max(&self) -> i64;
    /// The session disk tracker's peak.
    fn statement_disk_max(&self) -> i64;
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
    /// Go `TxnCtx.StartTS` and `TxnCtx.GetForUpdateTS()`.
    fn txn_start_and_for_update_ts(&self) -> (u64, u64);
    /// The statement's warnings as the slow log prints them.
    fn slow_log_warnings(&self) -> Vec<tidb_variable::slow_log::JsonSqlWarnForSlowLog>;
    /// Go `StmtCtx.WarningCount()`.
    fn statement_warning_count(&self) -> usize;
    /// Go `SQLCPUUsages.GetCPUUsages().TikvCPUTime` of the point readers.
    fn sql_tikv_cpu_time(&self) -> i64;
    /// Go `ConnectionInfo.Attributes`.
    fn connection_attributes(&self) -> &std::collections::BTreeMap<String, String>;
    /// Go `PlanCacheParams`: the prepared execution's arguments.
    fn prepared_params(&self) -> Option<&[tidb_datatype::Datum]>;
    /// Go `SessionVars.PrevStmt` / `PrevStmtDigest` as the summary keeps them.
    fn previous_summary_statement(&mut self) -> &mut Option<(String, String)>;
    /// The user the statement summary records.
    fn summary_user(&self) -> String;
    /// The statement's published plan (Go `StmtCtx`'s plan fields).
    fn process_plan_info_cell(&self) -> Arc<Mutex<tidb_executor::ProcessPlanInfo>>;

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
            .get_system(tidb_vardef::tidb_vars::TIDB_REDACT_LOG)
            .unwrap_or_default()
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
                tidb_datatype::datums_to_string_no_error_smart(params)
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
            tidb_util::redact::string(
                &mode,
                &format!("{}{}", observation.text, self.plan_cache_params_string()),
            )
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
        plan: &tidb_executor::ProcessPlanInfo,
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
            force = tidb_util::logutil::get_level() <= tidb_log::Level::Debug;
            if !self.instance_switch_on(tidb_vardef::tidb_vars::TIDB_ENABLE_SLOW_LOG) && !force {
                return;
            }
            let effective = tidb_variable::slow_log_rules::EffectiveSlowLogRules::new(
                &self
                    .session_vars()
                    .get_system(tidb_vardef::tidb_vars::TIDB_SLOW_LOG_RULES)
                    .unwrap_or_default(),
                &self
                    .session_vars()
                    .get_global(tidb_vardef::tidb_vars::TIDB_SLOW_LOG_RULES)
                    .unwrap_or_default(),
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
                    .unwrap_or(tidb_util::logutil::DEFAULT_SLOW_THRESHOLD as i64);
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
            .get_system(tidb_vardef::tidb_vars::TIDB_SESSION_ALIAS)
            .unwrap_or_default();
        let exec_detail = observation.cop_stats.exec_details();
        let cop_tasks = observation.cop_stats.cop_tasks_details();
        // A plan published without its encodings (the cached point read)
        // renders them now that a slow statement asks (Go `getEncodedPlan`).
        let (encoded_plan, summary_binary_plan) = match &plan.plan_source {
            Some(source) if plan.encoded_plan.is_empty() => source.render(),
            _ => (plan.encoded_plan.clone(), plan.summary_binary_plan.clone()),
        };
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

        let max_per_sec = self
            .session_vars()
            .get_global(tidb_vardef::tidb_vars::TIDB_SLOW_LOG_MAX_PER_SEC)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        if !tidb_variable::slow_log::slow_log_rate_limiter_allow(max_per_sec) {
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
                let (tikv, tiflash) = plan_storage_flags(&stored.0);
                (tikv, tiflash || plan.is_tiflash)
            }
            _ => (false, false),
        };
        let login = self
            .session_vars().login_user
            .as_deref()
            .and_then(|identity| identity.split_once('@'))
            .map(|(user, host)| (user.to_owned(), host.to_owned()));
        let keyspace_name = tidb_util::keyspace::get_keyspace_name_by_settings();
        let items = tidb_variable::slow_log::SlowQueryLogItems {
            txn_ts,
            keyspace_name: if tidb_util::keyspace::is_keyspace_name_empty(&keyspace_name) {
                String::new()
            } else {
                keyspace_name
            },
            keyspace_id: 0,
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
            is_write_cache_table: false,
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
                && self
                    .session_vars()
                    .get_global(tidb_vardef::tidb_vars::TIDB_GENERATE_BINARY_PLAN)
                    .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("ON"))
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
                ..tidb_variable::slow_log::CpuUsages::default()
            },
            storage_kv,
            storage_mpp,
            exec_detail,
            result_rows,
            warnings: self.slow_log_warnings(),
            resource_group_name: self.observed_resource_group(),
            ru_details: Some(Arc::clone(&observation.ru_details)),
            mem_max,
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
        let entry = tidb_variable::slow_log::slow_log_format(&vars, &items);
        // Go `SlowLogFormat` clears `CurrentDBChanged` once it printed `use`.
        self.session_vars_mut().current_db_changed = false;
        tidb_util::logutil::slow_query_logger().warn(&entry, &[]);

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
        // Go `getLazyStmtText` / `updatePrevStmt`: a sensitive statement is
        // summarized by its secure text.
        let summary_sql = observation
            .secure_text
            .clone()
            .unwrap_or_else(|| observation.text.clone());
        *self.previous_summary_statement() =
            Some((summary_sql.clone(), observation.digest.to_string()));
        let plan = self
            .process_plan_info_cell()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut ctx = StmtSummaryStmtCtx::default();
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
            mem_arbitration: 0.0,
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
            keyspace_id: 0,
            resource_group_name: self.observed_resource_group(),
            ru_detail: Some(Arc::clone(&observation.ru_details)),
            // Go `calculateStatementTotalRUV2`.
            total_ru_v2: tidb_util::ruv2_metrics::total_ru(
                Some(&observation.ruv2_metrics),
                ruv2_weights,
                observation.ru_details.tikv_ru_v2(),
                observation.ru_details.tiflash_ru(),
            ),
            // Go `SQLCPUUsages.GetCPUUsages()`, whose TiKV time this node
            // keeps as the coprocessor responses' plus the point readers'.
            cpu_usages: tidb_util::ppcpuusage::CpuUsages {
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
                ..Default::default()
            },
            plan_cache_unqualified: String::new(),
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
                encoded_plan: if observation.routed {
                    String::new()
                } else {
                    plan.encoded_plan
                },
                plan_digest: if observation.routed {
                    String::new()
                } else {
                    plan.plan_digest
                },
                binary_plan_enabled: !observation.routed
                    && self
                        .session_vars()
                        .get_global("tidb_generate_binary_plan")
                        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("ON")),
                binary_plan: plan.summary_binary_plan,
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
fn plan_storage_flags(plan: &tidb_planner::physical::PhysicalPlan) -> (bool, bool) {
    use tidb_planner::physical::PhysicalPlan;
    use tidb_planner::physical_table_reader::StoreType;
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
                stack.extend(cte.recursive_plan.as_deref());
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
fn slow_log_sample_logger() -> tidb_util::logutil::SampledLogger {
    static LOGGER: std::sync::LazyLock<tidb_util::logutil::SampledLogger> =
        std::sync::LazyLock::new(|| {
            tidb_util::logutil::sample_logger_factory(
                Duration::from_secs(60),
                1,
                vec![tidb_log::Field::new(
                    tidb_util::logutil::LOG_FIELD_CATEGORY,
                    tidb_log::Value::Str("slow log".to_owned()),
                )],
            )()
        });
    LOGGER.clone()
}
