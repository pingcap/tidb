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

//! Statement completion publication uses the same owner for materialized and
//! streaming results; readers never fabricate execution records.
//! Plan digest and RPC/network/RU/CPU measurements are not connected yet;
//! their empty values do not certify those execution-detail contracts.
use crate::*;
use tidb_stmtsummary::statement_summary::{
    EncodedPlanError, StmtExecInfo, StmtExecLazyInfo, StmtSummaryStmtCtx,
};
use tidb_util::topsql_stmtstats::{ExecFinishInfo, StatementObserver};

pub(crate) struct StatementObservation {
    sql: String,
    normalized: String,
    digest: tidb_parser::Digest,
    started: std::time::Instant,
    start_time: chrono::DateTime<chrono::Utc>,
    label: String,
    prepared: bool,
    routed: bool,
    phases: Arc<Mutex<StatementPhases>>,
}

#[derive(Default)]
struct StatementPhases {
    parse: Duration,
    parse_before_start: Duration,
    compile_started: Option<std::time::Instant>,
    compile: Duration,
    compile_finished: bool,
    compile_measured: bool,
    execution_started: bool,
    stats_started: bool,
    tables: Vec<tidb_stmtsummary::statement_summary::TableEntry>,
}

struct LazyStatement {
    sql: String,
    binary_plan: String,
}
impl StmtExecLazyInfo for LazyStatement {
    fn original_sql(&self) -> String {
        self.sql.clone()
    }
    fn encoded_plan(&self) -> Result<(String, String), EncodedPlanError> {
        Ok((String::new(), String::new()))
    }
    fn binary_plan(&self) -> String {
        self.binary_plan.clone()
    }
    fn plan_digest(&self) -> String {
        String::new()
    }
    fn binding_sql_and_digest(&self) -> (String, String) {
        (String::new(), String::new())
    }
}

impl Session {
    pub(crate) fn record_observation_parse(&mut self, sql: &str, duration: Duration) {
        if let Some(observation) = self.statement_observation.as_ref().filter(|o| o.sql == sql) {
            // run() opened the clock before parsing; do not add it twice.
            observation
                .phases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .parse = duration;
        } else {
            // The wire frontend parsed before opening ExecuteStmt.
            self.pending_observation_parse = Some((sql.to_owned(), duration));
        }
    }

    pub(crate) fn start_observation_compile(&self) {
        if let Some(observation) = &self.statement_observation {
            let mut phases = observation
                .phases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Named EXECUTE re-enters compilation; retain the outer start.
            phases
                .compile_started
                .get_or_insert_with(std::time::Instant::now);
        }
    }

    pub(crate) fn observed_compile_duration(&self) -> Option<Duration> {
        let observation = self.statement_observation.as_ref()?;
        let phases = observation
            .phases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        phases.compile_measured.then_some(phases.compile)
    }

    pub(crate) fn observe_privilege_tables(
        &self,
        requests: &[crate::table_privilege::TablePrivilegeRequest],
    ) {
        let Some(observation) = &self.statement_observation else {
            return;
        };
        let mut tables = Vec::new();
        for request in requests {
            if request.database.is_empty() && request.table.is_empty() {
                continue;
            }
            let entry = tidb_stmtsummary::statement_summary::TableEntry {
                db: request.database.clone(),
                table: request.table.clone(),
            };
            if !tables.contains(&entry) {
                tables.push(entry);
            }
        }
        observation
            .phases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tables = tables;
    }

    pub(crate) fn statement_phase_observer(
        &self,
    ) -> Option<Arc<dyn Fn(tidb_executor::StatementPhase) + Send + Sync>> {
        let observation = self.statement_observation.as_ref()?;
        let phases = Arc::clone(&observation.phases);
        let stats = Arc::clone(&self.statement_stats);
        let digest = observation.digest.clone();
        let statement_started = observation.started;
        Some(Arc::new(move |phase| {
            let mut phases = phases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match phase {
                tidb_executor::StatementPhase::PlanReady => {
                    if phases.compile_finished {
                        return;
                    }
                    phases.compile_finished = true;
                    phases.compile_measured = true;
                    phases.compile = match phases.compile_started {
                        Some(started) => started.elapsed(),
                        // Retained prepared plans bypass the compile wrapper.
                        None => statement_started
                            .elapsed()
                            .saturating_sub(phases.parse.saturating_sub(phases.parse_before_start)),
                    };
                    return;
                }
                tidb_executor::StatementPhase::ExecutorReady => {
                    if phases.execution_started {
                        return;
                    }
                    phases.execution_started = true;
                    // External pre-lock and EXPLAIN paths may execute before
                    // a physical plan-ready notification. Do not fabricate a
                    // compile duration from work that has already executed.
                    phases.compile_finished = true;
                }
            }
            if !phases.stats_started && tidb_util::topsql_state::top_sql_enabled() {
                stats.on_execution_begin(digest.as_bytes(), &[], None);
                phases.stats_started = true;
            }
        }))
    }

    pub(crate) fn current_statement_observation_identity(
        &self,
        sql: &str,
    ) -> Option<(&str, &tidb_parser::Digest)> {
        self.statement_observation
            .as_ref()
            .filter(|observation| observation.sql == sql)
            .map(|observation| (observation.normalized.as_str(), &observation.digest))
    }

    pub(crate) fn begin_statement_observation(&mut self, sql: &str) {
        if self.routed_statement_observation_depth != 0 {
            return;
        }
        let (normalized, digest) = normalize_statement_digest(sql);
        let parse = self
            .pending_observation_parse
            .take()
            .filter(|(parsed_sql, _)| parsed_sql == sql)
            .map_or(Duration::ZERO, |(_, duration)| duration);
        self.statement_observation = Some(StatementObservation {
            sql: sql.to_owned(),
            normalized,
            digest,
            started: std::time::Instant::now(),
            start_time: chrono::Utc::now(),
            label: String::new(),
            prepared: false,
            routed: false,
            phases: Arc::new(Mutex::new(StatementPhases {
                parse,
                parse_before_start: parse,
                ..StatementPhases::default()
            })),
        });
    }

    // SQL-level EXECUTE and the retained body share a result lifecycle. Replace
    // attribution with the retained SQL while keeping the command's clock.
    pub(crate) fn retarget_prepared_statement_observation(&mut self, sql: &str) {
        if self.routed_statement_observation_depth != 0
            && self
                .statement_observation
                .as_ref()
                .is_none_or(|observation| observation.label != "Execute")
        {
            return;
        }
        if let Some(observation) = &mut self.statement_observation {
            let (normalized, digest) = normalize_statement_digest(sql);
            if self.statement_normalized_sql.is_some() {
                self.statement_normalized_sql = Some(normalized.clone());
            }
            observation.sql = sql.to_owned();
            observation.normalized = normalized;
            observation.digest = digest;
            observation.label.clear();
            observation.prepared = true;
        }
    }

    pub(crate) fn observe_statement_node(&mut self, stmt: &Stmt, prepared: bool) {
        if self.routed_statement_observation_depth != 0
            && self
                .statement_observation
                .as_ref()
                .is_none_or(|observation| !observation.label.is_empty())
        {
            return;
        }
        if let Some(observation) = &mut self.statement_observation {
            if observation.label.is_empty()
                && stmt.label() != "Execute"
                && !matches!(stmt, Stmt::Dml(_) | Stmt::Query(_))
                && tidb_util::topsql_state::top_sql_enabled()
            {
                self.statement_stats
                    .on_execution_begin(observation.digest.as_bytes(), &[], None);
                observation
                    .phases
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .stats_started = true;
            }
            observation.label = stmt.label().to_owned();
            observation.prepared |= prepared;
            observation.routed = !matches!(stmt, Stmt::Dml(_) | Stmt::Query(_));
        }
    }

    pub(crate) fn finish_statement_observation(
        &mut self,
        result: &Result<StatementCompletion, DriverError>,
    ) {
        if self.routed_statement_observation_depth != 0 {
            return;
        }
        let affected_rows = match result {
            Ok(StatementCompletion::Affected(rows)) => *rows,
            _ => 0,
        };
        let result_rows = match result {
            Ok(StatementCompletion::Rows(Some(rows))) => i64::try_from(*rows).unwrap_or(i64::MAX),
            _ => 0,
        };
        self.publish_statement_observation(result.is_ok(), affected_rows, result_rows);
    }

    /// Starts observation when durable completion belongs outside Session.
    pub fn begin_routed_statement_observation(&mut self, sql: &str, stmt: &Stmt) {
        self.begin_statement_observation(sql);
        self.observe_statement_node(stmt, self.binary_prepared_execution);
        if let Some(observation) = &mut self.statement_observation {
            observation.routed = !matches!(stmt, Stmt::Dml(_) | Stmt::Query(_));
        }
        self.routed_statement_observation_depth += 1;
    }

    /// Starts the outer durable scope for a binary prepared write.
    pub fn begin_prepared_routed_statement_observation(&mut self, sql: &str, stmt: &Stmt) {
        self.begin_routed_statement_observation(sql, stmt);
        if let Some(observation) = &mut self.statement_observation {
            observation.prepared = true;
        }
    }

    /// Publishes one routed completion through the same summary/counter owner.
    pub fn finish_routed_statement_observation(&mut self, succeeded: bool, affected_rows: u64) {
        if self.routed_statement_observation_depth == 0 {
            return;
        }
        self.routed_statement_observation_depth -= 1;
        if self.routed_statement_observation_depth == 0 {
            self.publish_statement_observation(succeeded, affected_rows, 0);
        }
    }

    fn publish_statement_observation(
        &mut self,
        succeeded: bool,
        affected_rows: u64,
        result_rows: i64,
    ) {
        let Some(observation) = self.statement_observation.take() else {
            return;
        };
        // Go observes compiled statements, not a parser failure without an AST.
        if observation.label.is_empty() {
            return;
        }
        let phases = observation
            .phases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Go GetTotalCostDuration includes parsing before ExecuteStmt.
        let elapsed = observation.started.elapsed() + phases.parse_before_start;
        if phases.stats_started && tidb_util::topsql_state::top_sql_enabled() {
            self.statement_stats.on_execution_finished(
                observation.digest.as_bytes(),
                &[],
                Some(&ExecFinishInfo {
                    exec_duration_ns: i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX),
                    ..ExecFinishInfo::default()
                }),
            );
        } else {
            self.statement_stats.clear_ru_exec_context();
        }
        use tidb_exec::adapter::{
            decide_summary_stmt, SummaryAction, SummaryGate, SummaryStmtKind,
        };
        let user = self
            .current_identity()
            .map_or("", |(user, _)| user)
            .to_owned();
        let action = decide_summary_stmt(&SummaryGate {
            // This session owner represents restricted SQL with no authenticated
            // identity. EXPLAIN EXPLORE has no runtime consumer here yet.
            in_restricted_sql: false,
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
                .previous_summary_statement
                .as_ref()
                .map_or_else(String::new, |(_, digest)| digest.clone()),
        });
        let (internal, attributed_prev_digest) = match action {
            SummaryAction::ClearPrevDigestAndSkip => {
                self.previous_summary_statement = None;
                return;
            }
            SummaryAction::SkipPrepare | SummaryAction::SkipCommitWithoutPrevDigest => return,
            SummaryAction::Record {
                is_internal,
                attributed_prev_digest,
            } => (is_internal, attributed_prev_digest),
        };
        let prev_sql = if attributed_prev_digest.is_some() {
            self.previous_summary_statement
                .as_ref()
                .map_or_else(String::new, |(sql, _)| sql.clone())
        } else {
            String::new()
        };
        let prev_sql_digest = attributed_prev_digest.unwrap_or_default();
        self.previous_summary_statement =
            Some((observation.sql.clone(), observation.digest.to_string()));
        let plan = self
            .process_plan_info
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut ctx = StmtSummaryStmtCtx::default();
        ctx.stmt_type = observation.label;
        ctx.tables = phases.tables.clone();
        ctx.index_names = if observation.routed {
            Vec::new()
        } else {
            plan.index_names
        };
        ctx.add_affected_rows(affected_rows);
        ctx.set_warning_count(u32::try_from(self.warnings.len()).unwrap_or(u32::MAX));
        tidb_stmtsummary::v2::stmtsummary::add(&StmtExecInfo {
            schema_name: self.current_db.to_lowercase(),
            charset: self
                .vars
                .get_system("character_set_connection")
                .unwrap_or_default(),
            collation: self
                .vars
                .get_system("collation_connection")
                .unwrap_or_default(),
            normalized_sql: observation.normalized,
            digest: observation.digest.to_string(),
            prev_sql,
            prev_sql_digest,
            plan_digest: String::new(),
            user,
            total_latency: elapsed,
            parse_latency: phases.parse,
            compile_latency: phases.compile,
            stmt_ctx: Arc::new(ctx),
            cop_tasks: None,
            exec_detail: Default::default(),
            mem_max: self.session_memory.session_tracker().max_consumed(),
            mem_arbitration: 0.0,
            disk_max: self.session_memory.session_disk_tracker().max_consumed(),
            start_time: observation.start_time,
            is_internal: internal,
            succeed: succeeded,
            plan_in_cache: !observation.routed && self.found_in_plan_cache,
            plan_in_binding: !observation.routed && self.found_in_binding,
            exec_retry_count: 0,
            exec_retry_time: std::time::Duration::ZERO,
            write_sql_resp_duration: std::time::Duration::ZERO,
            result_rows,
            tikv_exec_details: None,
            prepared: observation.prepared,
            keyspace_name: tidb_config::config_tree::config::get_global_config()
                .keyspace_name
                .clone(),
            keyspace_id: 0,
            resource_group_name: self.active_resource_group.clone(),
            ru_detail: None,
            total_ru_v2: 0.0,
            cpu_usages: Default::default(),
            plan_cache_unqualified: String::new(),
            lazy_info: Arc::new(LazyStatement {
                sql: observation.sql,
                binary_plan: if observation.routed {
                    String::new()
                } else {
                    plan.brief_binary_plan
                },
            }),
        });
    }
}
