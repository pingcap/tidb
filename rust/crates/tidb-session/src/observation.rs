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
//! Each statement owns Go's `RUDetails` (`ContextWithInitializedExecDetails`),
//! which its RPCs charge and its summary and Top RU read. Plan digest and
//! RPC/network/CPU measurements are not connected yet; their empty values do
//! not certify those execution-detail contracts.
use crate::*;
use tidb_stmtsummary::statement_summary::{
    EncodedPlanError, StmtExecInfo, StmtExecLazyInfo, StmtSummaryStmtCtx,
};
use tidb_util::topsql_stmtstats::{ExecFinishInfo, StatementObserver};

pub(crate) struct StatementObservation {
    sql: String,
    /// Go `SensitiveStmtNode.SecureText`, which every consumer of a
    /// password-bearing statement's text reads instead of the text.
    secure_text: Option<String>,
    normalized: String,
    digest: tidb_parser::Digest,
    started: std::time::Instant,
    start_time: chrono::DateTime<chrono::Utc>,
    label: String,
    prepared: bool,
    routed: bool,
    /// Go `StmtCtx.OriginalTableHints`: the table hints the statement wrote,
    /// after the query-block handler took the view hints out.
    original_table_hints: Vec<tidb_ast::Hint>,
    /// Go `ExecStmt.GetBindingSQLAndDigest`.
    binding_sql: String,
    binding_digest: String,
    phases: Arc<Mutex<StatementPhases>>,
    kv_exec_counter: tidb_util::topsql_stmtstats::KvExecCounterHandle,
    /// Go `StmtCtx.ExecDetails`: what the statement's coprocessor responses
    /// reported, read back when the summary is recorded.
    cop_stats: Arc<tidb_exec::statement_cop_stats::StatementCopStats>,
    /// Go `ctx.Value(util.RUDetailsCtxKey)`: the statement's resource units,
    /// created with its ctx (`execdetails.ContextWithInitializedExecDetails`)
    /// and charged by every TiKV/TiFlash RPC it issues.
    ru_details: Arc<tikv_client::RuDetails>,
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
    tables: Vec<tidb_stmtsummary::statement_summary::TableEntry>,
}

/// The exec details [`Session::initialize_statement_exec_details`] created
/// for the statement the session is about to run, with the digest it
/// computed for them.
pub(crate) struct PendingStatementExecDetails {
    sql: String,
    ru_details: Arc<tikv_client::RuDetails>,
    normalized: String,
    digest: tidb_parser::Digest,
}

struct LazyStatement {
    sql: String,
    binary_plan: String,
    encoded_plan: String,
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
        Ok((self.encoded_plan.clone(), self.plan_hint.clone()))
    }
    fn binary_plan(&self) -> String {
        self.binary_plan.clone()
    }
    fn plan_digest(&self) -> String {
        self.plan_digest.clone()
    }
    fn binding_sql_and_digest(&self) -> (String, String) {
        (self.binding_sql.clone(), self.binding_digest.clone())
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

    pub(crate) fn statement_kv_exec_counter(
        &self,
    ) -> tidb_util::topsql_stmtstats::KvExecCounterHandle {
        self.statement_observation
            .as_ref()
            .map(|o| o.kv_exec_counter.clone())
            .unwrap_or_default()
    }

    /// Go `tidb_enable_collect_execution_info`: the statement's consumer of
    /// coprocessor response details, `None` while the instance switch is OFF
    /// (Go's `copIteratorWorker.stats` is only created when it is ON).
    pub(crate) fn statement_cop_response_sink(
        &self,
    ) -> Option<Arc<dyn tidb_executor::CopResponseSink>> {
        let collect = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_COLLECT_EXECUTION_INFO)
            .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("ON"));
        if !collect {
            return None;
        }
        let stats = Arc::clone(&self.statement_observation.as_ref()?.cop_stats);
        Some(stats)
    }

    pub(crate) fn statement_phase_observer(
        &self,
    ) -> Option<Arc<dyn Fn(tidb_executor::StatementPhase) + Send + Sync>> {
        let observation = self.statement_observation.as_ref()?;
        let phases = Arc::clone(&observation.phases);
        let stats = Arc::clone(&self.statement_stats);
        let kv_exec_counter = observation.kv_exec_counter.clone();
        let plan = Arc::clone(&self.process_plan_info);
        let digest = observation.digest.clone();
        let statement_started = observation.started;
        let ru_details = Arc::clone(&observation.ru_details);
        let user = self.top_ru_user();
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
            // Go admits non-fast executions even while profiling is disabled,
            // so enabling it during execution can still observe their begin.
            let fast = plan
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_fast_plan;
            if !fast || tidb_util::topsql_state::top_profiling_enabled() {
                let info = top_profiling_begin_info(&ru_details, &user);
                stats.on_execution_begin(digest.as_bytes(), &[], Some(&info));
                if tidb_util::topsql_state::top_sql_enabled() {
                    kv_exec_counter.initialize(&stats, digest.as_bytes(), &[]);
                }
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
        // The details the server's door initialized for this statement, or
        // Go `execdetails.ContextWithInitializedExecDetails`'s fresh ones.
        let (normalized, digest, ru_details) = match self
            .pending_statement_exec_details
            .take()
            .filter(|pending| pending.sql == sql)
        {
            Some(pending) => (pending.normalized, pending.digest, pending.ru_details),
            None => {
                let (normalized, digest) = normalize_statement_digest(sql);
                (normalized, digest, Arc::new(tikv_client::RuDetails::new()))
            }
        };
        let parse = self
            .pending_observation_parse
            .take()
            .filter(|(parsed_sql, _)| parsed_sql == sql)
            .map_or(Duration::ZERO, |(_, duration)| duration);
        self.statement_observation = Some(StatementObservation {
            sql: sql.to_owned(),
            secure_text: None,
            normalized,
            digest,
            started: std::time::Instant::now(),
            start_time: chrono::Utc::now(),
            label: String::new(),
            prepared: false,
            routed: false,
            original_table_hints: Vec::new(),
            binding_sql: String::new(),
            binding_digest: String::new(),
            kv_exec_counter: Default::default(),
            cop_stats: Arc::new(
                tidb_exec::statement_cop_stats::StatementCopStats::with_ru_details(Arc::clone(
                    &ru_details,
                )),
            ),
            ru_details,
            phases: Arc::new(Mutex::new(StatementPhases {
                parse,
                parse_before_start: parse,
                ..StatementPhases::default()
            })),
        });
        self.publish_statement_kv_context();
    }

    /// Publishes the running statement's ctx values to the transactions,
    /// snapshots and requests it reaches: its `RUDetails` and Go's
    /// `StmtCtx.GetResourceGroupTagger` (`pkg/sessionctx/stmtctx`) and
    /// `newLockCtx`'s `lockCtx.ResourceGroupTagger` (`pkg/executor/select.go`).
    /// Both taggers read the statement's plan digest when they tag, as Go's
    /// read `StmtCtx` when they run.
    fn publish_statement_kv_context(&self) {
        let Some(observation) = &self.statement_observation else {
            return;
        };
        self.publish_kv_context(
            &observation.ru_details,
            &observation.normalized,
            &observation.digest,
            Some(Arc::clone(&self.process_plan_info)),
        );
    }

    /// Publishes `ru_details` and the taggers of the statement with this
    /// normalized text and digest. `plan` is where its plan digest will be
    /// read; `None` before the statement is planned, whose tags then carry
    /// no plan digest (Go's nil `StmtCtx` plan digest).
    fn publish_kv_context(
        &self,
        ru_details: &Arc<tikv_client::RuDetails>,
        normalized: &str,
        digest: &tidb_parser::Digest,
        plan: Option<Arc<Mutex<tidb_executor::ProcessPlanInfo>>>,
    ) {
        // Go `StmtCtx.SQLDigest`: a statement with no normalized text tags
        // no SQL digest.
        let sql_digest = (!normalized.is_empty()).then(|| digest.as_bytes().to_vec());
        let keyspace_name = tidb_util::keyspace::get_keyspace_name_bytes_by_settings();
        // Go `StmtCtx.GetPlanDigest`: empty until the plan is published.
        let plan_digest = move || {
            plan.as_ref().map_or_else(Vec::new, |plan| {
                let plan = plan
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                decode_hex_digest(&plan.plan_digest)
            })
        };
        let lock_plan_digest = plan_digest.clone();
        let lock_sql_digest = sql_digest.clone();
        self.statement_kv_context
            .publish(tidb_txnkv::StatementKvContext {
                ru_details: Some(Arc::clone(ru_details)),
                resource_group_tagger: Some(Arc::new(move || {
                    let mut tagger = tidb_txnkv::ResourceGroupTagBuilder::new(keyspace_name);
                    tagger.set_plan_digest(&plan_digest());
                    if let Some(sql_digest) = &sql_digest {
                        tagger.set_sql_digest(sql_digest);
                    }
                    tagger
                })),
                pessimistic_lock_resource_group_tagger: Some(Arc::new(move |key: &[u8]| {
                    let Some(sql_digest) = &lock_sql_digest else {
                        return Vec::new();
                    };
                    let mut tagger = tidb_txnkv::ResourceGroupTagBuilder::new(keyspace_name);
                    tagger
                        .set_plan_digest(&lock_plan_digest())
                        .set_sql_digest(sql_digest);
                    tagger.encode_tag_with_key(key)
                })),
            });
    }

    /// Go `execdetails.ContextWithInitializedExecDetails` at the server's
    /// statement door (`clientConn.handleStmt`, `executePlanCacheStmt`): the
    /// statement's `RUDetails` exist before any of its storage work, so a
    /// pessimistic lock the server takes before the session runs the
    /// statement (Go `PointGetExecutor.getAndLock`'s fold) is charged and
    /// tagged as the statement's own. The statement's observation adopts
    /// these details when it starts.
    pub fn initialize_statement_exec_details(&mut self, sql: &str) {
        if self.routed_statement_observation_depth != 0 {
            return;
        }
        let ru_details = Arc::new(tikv_client::RuDetails::new());
        let (normalized, digest) = normalize_statement_digest(sql);
        self.publish_kv_context(&ru_details, &normalized, &digest, None);
        self.pending_statement_exec_details = Some(PendingStatementExecDetails {
            sql: sql.to_owned(),
            ru_details,
            normalized,
            digest,
        });
    }

    /// Go `vars.User.String()` for Top RU: `user@host`, empty with no user.
    fn top_ru_user(&self) -> String {
        self.current_identity()
            .map_or_else(String::new, |(user, host)| format!("{user}@{host}"))
    }

    /// The session's statement ctx values, shared with the transactions and
    /// snapshots its statements use; see [`tidb_txnkv::StatementKvContextCell`].
    #[must_use]
    pub fn statement_kv_context_cell(&self) -> tidb_txnkv::StatementKvContextCell {
        self.statement_kv_context.clone()
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
        self.publish_statement_kv_context();
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
        let user = self.top_ru_user();
        if let Some(observation) = &mut self.statement_observation {
            if observation.label.is_empty()
                && stmt.label() != "Execute"
                && !matches!(stmt, Stmt::Dml(_) | Stmt::Query(_))
            {
                // SET is the administrative fast plan in Go's IsFastPlan.
                let fast = matches!(stmt, Stmt::Session(session_stmt) if matches!(
                    &**session_stmt,
                    tidb_ast::SessionStmt::Set(_)
                        | tidb_ast::SessionStmt::SetUserVar(_)
                        | tidb_ast::SessionStmt::SetCharset { .. }
                        | tidb_ast::SessionStmt::SetMixed(_)
                ));
                if !fast || tidb_util::topsql_state::top_profiling_enabled() {
                    let info = top_profiling_begin_info(&observation.ru_details, &user);
                    self.statement_stats.on_execution_begin(
                        observation.digest.as_bytes(),
                        &[],
                        Some(&info),
                    );
                    if tidb_util::topsql_state::top_sql_enabled() {
                        observation.kv_exec_counter.initialize(
                            &self.statement_stats,
                            observation.digest.as_bytes(),
                            &[],
                        );
                    }
                }
                observation
                    .phases
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .execution_started = true;
            }
            observation.label = stmt.label().to_owned();
            if tidb_stmtsummary::v2::stmtsummary::enabled() {
                // Go reads these off the statement node when it records the
                // summary, after the node has been consumed.
                if !tidb_hint::extract_table_hints_from_stmt_node(stmt).0.is_empty() {
                    let mut handled = stmt.clone();
                    let _ = tidb_hint::QBHintHandler::build(&mut handled);
                    observation.original_table_hints =
                        tidb_hint::extract_table_hints_from_stmt_node(&handled).0;
                }
                (observation.binding_sql, observation.binding_digest) =
                    crate::binding::normalize_with_db(stmt, &self.current_db);
            }
            observation.prepared |= prepared;
            observation.routed = !matches!(stmt, Stmt::Dml(_) | Stmt::Query(_));
            // Go `getSQLForProcessInfo`: the process list shows a sensitive
            // statement's secure text once its node is known.
            observation.secure_text = statement_secure_text(stmt, &observation.sql);
            if let (Some(text), Some(process)) = (&observation.secure_text, &self.process) {
                process.registry().redact_statement_info(process.id(), text);
            }
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
        // The outer routed execution owns one LazyTxn.onStmtStart, including
        // transaction control and SET. Its inner executor only republishes.
        if self.routed_statement_observation_depth == 0 {
            if let Some(guard) = &self.process {
                let digest = self
                    .statement_observation
                    .as_ref()
                    .map(|o| o.digest.to_string());
                guard.registry().statement_started_with_digest(
                    guard.id(),
                    sql,
                    digest.as_deref(),
                    &self.status_text(),
                );
            }
        }
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
            self.finish_routed_statement_warnings(succeeded);
            self.publish_statement_observation(succeeded, affected_rows, 0);
            self.refresh_process_status();
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
        // Go `observeStmtFinishedForTopProfiling`: Top RU alone also records
        // the finish, with the statement's `RUDetails` and user.
        let top_ru = tidb_util::topsql_state::top_ru_enabled();
        if phases.execution_started && (tidb_util::topsql_state::top_sql_enabled() || top_ru) {
            self.statement_stats.on_execution_finished(
                observation.digest.as_bytes(),
                &[],
                Some(&ExecFinishInfo {
                    exec_duration_ns: i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX),
                    top_ru_enabled: top_ru,
                    ru_details: top_ru.then(|| Arc::clone(&observation.ru_details)),
                    user: if top_ru {
                        self.top_ru_user()
                    } else {
                        String::new()
                    },
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
        // Go `getLazyStmtText` / `updatePrevStmt`: a sensitive statement is
        // summarized by its secure text.
        let summary_sql = observation
            .secure_text
            .clone()
            .unwrap_or_else(|| observation.sql.clone());
        self.previous_summary_statement =
            Some((summary_sql.clone(), observation.digest.to_string()));
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
            ru_detail: Some(Arc::clone(&observation.ru_details)),
            // Go `calculateStatementTotalRUV2` with no statement RU v2
            // metrics, which this node does not keep.
            total_ru_v2: tidb_util::ruv2_metrics::total_ru(
                None,
                tidb_util::ruv2_metrics::RuV2Weights::default(),
                observation.ru_details.tikv_ru_v2(),
                observation.ru_details.tiflash_ru(),
            ),
            cpu_usages: Default::default(),
            plan_cache_unqualified: String::new(),
            lazy_info: Arc::new(LazyStatement {
                plan_hint: if observation.routed || internal {
                    String::new()
                } else {
                    // Go `getEncodedPlan`: the hints reproducing the plan,
                    // then the statement's own hints of these kinds.
                    let mut hints = plan.plan_hints.clone();
                    hints.extend(observation.original_table_hints.iter().filter(|hint| {
                        matches!(
                            hint.name.to_ascii_lowercase().as_str(),
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
                    }).cloned());
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
                binary_plan: if !observation.routed
                    && self
                        .vars
                        .get_global("tidb_generate_binary_plan")
                        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("ON"))
                {
                    plan.summary_binary_plan
                } else {
                    String::new()
                },
            }),
        });
    }
}

/// Go `observeStmtBeginForTopProfiling`'s `ExecBeginInfo`: under Top RU it
/// carries the statement's `RUDetails` (Go's `Ctx`, whose only use is that
/// lookup), the user and Go `stmtstats.DefaultRUVersion()` -- Go's version
/// when no domain resource-group controller is reachable from the session.
fn top_profiling_begin_info(
    ru_details: &Arc<tikv_client::RuDetails>,
    user: &str,
) -> tidb_util::topsql_stmtstats::ExecBeginInfo {
    let top_ru = tidb_util::topsql_state::top_ru_enabled();
    let mut info = tidb_util::topsql_stmtstats::ExecBeginInfo {
        top_ru_enabled: top_ru,
        ..Default::default()
    };
    if top_ru {
        info.ru_details = Some(Arc::clone(ru_details));
        info.user = user.to_owned();
        info.ru_version = tidb_util::topsql_stmtstats::default_ru_version();
    }
    info
}

/// The bytes of a digest held as its lowercase hex text (Go
/// `parser.Digest.Bytes` of `Digest.String`); empty for no digest.
fn decode_hex_digest(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks_exact(2)
        .map_while(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
        })
        .collect()
}

/// Go `ast.SensitiveStmtNode.SecureText` for the statements that implement
/// it; `None` for every other statement, whose text is safe to show.
fn statement_secure_text(stmt: &Stmt, sql: &str) -> Option<String> {
    // Go `GrantStmt.SecureText` / `GrantRoleStmt.SecureText`: the statement
    // text cut before its first `identified`.
    let cut_identified = || {
        let index = sql.to_lowercase().find("identified");
        match index {
            Some(index) if index > 0 => sql[..index].to_owned(),
            _ => sql.to_owned(),
        }
    };
    match stmt {
        Stmt::Ddl(ddl) => match ddl.as_ref() {
            tidb_ast::DdlStmt::CreateUser { users, .. } => {
                Some(tidb_ast::create_user_secure_text(users))
            }
            tidb_ast::DdlStmt::AlterUser(alter) => Some(alter.secure_text()),
            _ => None,
        },
        Stmt::Session(session) => match session.as_ref() {
            tidb_ast::SessionStmt::SetPassword(set_password) => Some(set_password.secure_text()),
            tidb_ast::SessionStmt::Set(set) => Some(set.secure_text()),
            _ => None,
        },
        Stmt::Admin(admin) => match admin.as_ref() {
            tidb_ast::AdminStmt::Grant(_) | tidb_ast::AdminStmt::GrantRole(_) => {
                Some(cut_identified())
            }
            tidb_ast::AdminStmt::Brie(brie) => Some(brie.secure_text()),
            tidb_ast::AdminStmt::Traffic(traffic) => Some(traffic.secure_text()),
            _ => None,
        },
        Stmt::Dml(dml) => match dml.as_ref() {
            tidb_ast::DmlStmt::ImportInto(import) => Some(import.secure_text()),
            _ => None,
        },
        Stmt::Query(_) => None,
    }
}
