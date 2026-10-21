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
use tidb_hack::GoToLower;
use tidb_stmtsummary::statement_summary::{
    EncodedPlanError, StmtExecInfo, StmtExecLazyInfo, StmtSummaryStmtCtx,
};
use tidb_util::topsql_stmtstats::{ExecFinishInfo, StatementObserver};

pub(crate) struct StatementObservation {
    sql: String,
    /// Go `stmtNode.Text()` of `sql` ([`Session::statement_text`]), which Go
    /// keeps as `StmtCtx.OriginalSQL`: the summary's sample text, and the
    /// source of `normalized`.
    text: String,
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
    /// Go `ctx.Value(util.ExecDetailsKey)`: the statement's KV and PD waits
    /// and backoffs, created with the same ctx.
    exec_details: Arc<tikv_client::util::ExecDetails>,
    /// The traffic half of Go's `util.ExecDetails`, which client-rust
    /// collects apart from the `ExecDetails` value.
    network_traffic_details: Arc<tikv_client::traffic::NetworkTrafficDetails>,
    /// Go `SessionVars.RUV2Metrics`: the statement's TiDB-side RU v2
    /// counters (`RUV2MetricsFromContext`), created with its ctx.
    ruv2_metrics: Arc<tidb_util::ruv2_metrics::RuV2Metrics>,
    /// Go `recordLastQueryInfo`'s statement test: a `SHOW` other than
    /// `SHOW SESSION_STATES`, an `EXECUTE`, or a DML node.
    record_last_query: bool,
    /// Go `logStmt`'s crucial statements (account, privilege and schema
    /// changes), which go to the background log instead of the general log.
    crucial: bool,
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
    /// Go `StmtExecDetails.WriteSQLRespDuration`: the time the server spent
    /// writing this statement's result to the client.
    write_sql_resp: Duration,
    /// Go `ExecStmt.retryCount`: the statement's pessimistic re-executions.
    exec_retry_count: u64,
    /// Go `ExecStmt.retryStartTime`: when the last re-execution began.
    retry_start: Option<std::time::Instant>,
}

impl StatementPhases {
    /// Go `SetSlowLogItems` / `SummaryStmt`: `ExecRetryTime = TimeTotal -
    /// DurationParse - DurationCompile - time.Since(retryStartTime)`, the
    /// time the failed attempts took; zero without a retry.
    fn exec_retry_time(&self, elapsed: Duration) -> Duration {
        match self.retry_start {
            Some(started) if self.exec_retry_count > 0 => elapsed
                .saturating_sub(self.parse)
                .saturating_sub(self.compile)
                .saturating_sub(started.elapsed()),
            _ => Duration::ZERO,
        }
    }
}

/// The exec details [`Session::initialize_statement_exec_details`] created
/// for the statement the session is about to run, with the digest it
/// computed for them.
pub(crate) struct PendingStatementExecDetails {
    sql: String,
    ru_details: Arc<tikv_client::RuDetails>,
    exec_details: Arc<tikv_client::util::ExecDetails>,
    network_traffic_details: Arc<tikv_client::traffic::NetworkTrafficDetails>,
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

/// [`Session::statement_text`] of `sql` in the client encoding `client`.
fn statement_text_in(sql: &str, client: tidb_datatype::Encoding) -> std::borrow::Cow<'_, str> {
    // Valid UTF-8 without control characters decodes to itself under a utf8,
    // latin1 or binary client and holds no non-printable literal.
    let printable =
        !tidb_hack::raw_bytes::contains_escapes(sql) && !sql.chars().any(char::is_control);
    if printable
        && (sql.is_ascii()
            || matches!(
                client,
                tidb_datatype::Encoding::Utf8
                    | tidb_datatype::Encoding::Latin1
                    | tidb_datatype::Encoding::Binary
            ))
    {
        return std::borrow::Cow::Borrowed(sql);
    }
    let mut text = tidb_ast::NodeText::default();
    text.set_text(
        Some(client),
        tidb_hack::raw_bytes::str_to_bytes(sql).into_owned(),
    );
    std::borrow::Cow::Owned(tidb_hack::raw_bytes::bytes_to_str(text.text()).into_owned())
}

impl Session {
    /// Go `stmtNode.Text()` for SQL text this session runs. Go publishes it
    /// wherever a statement's text leaves the session: the process list
    /// (`SetProcessInfo`) and `StmtCtx.OriginalSQL`, which the slow log, the
    /// statement summary's sample text and its digest text read. `Text()`
    /// decodes the client's bytes with `@@character_set_client` and spells a
    /// non-printable string literal as a hex literal
    /// (`convertBinaryStringLiterals`): `SELECT '\xE9'` from a latin1 client
    /// is `SELECT 0xe9`, and from any client `'a<TAB>b'` is `0x610962`. The
    /// text carries the client's bytes as `raw_bytes` escapes, and so does
    /// the result where the decode keeps a byte that is not UTF-8.
    #[must_use]
    pub fn statement_text<'a>(&self, sql: &'a str) -> std::borrow::Cow<'a, str> {
        // ASCII without control characters is the common case: it decodes to
        // itself under every client and holds no non-printable literal.
        if sql.bytes().all(|byte| (0x20..0x7F).contains(&byte)) {
            return std::borrow::Cow::Borrowed(sql);
        }
        statement_text_in(sql, self.vars.build_parser_config().charset_client)
    }

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

    /// Go `clientConn.writeChunks` / `writeResultSet`:
    /// `StmtExecDetails.WriteSQLRespDuration += ...`, the server's time
    /// encoding and writing the running statement's result packets.
    pub fn add_write_sql_resp_duration(&mut self, duration: Duration) {
        if let Some(observation) = &self.statement_observation {
            observation
                .phases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .write_sql_resp += duration;
        }
    }

    /// Go `ExecStmt.handlePessimisticLockError`: `retryCount++` and
    /// `retryStartTime = time.Now()` before the running statement is
    /// executed again at a newer for-update timestamp.
    pub fn record_statement_retry(&mut self) {
        if let Some(observation) = &self.statement_observation {
            let mut phases = observation
                .phases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            phases.exec_retry_count += 1;
            phases.retry_start = Some(std::time::Instant::now());
        }
    }

    /// Go `Session.Parse`: a non-internal session's parse adds to
    /// `RUV2PendingSessionParserTotal`, which the statement it parsed (or,
    /// for a parse before its statement starts, the next one) takes over.
    pub(crate) fn record_ruv2_session_parse(&mut self, sql: &str) {
        if self.restricted_sql {
            return;
        }
        match self.statement_observation.as_ref().filter(|o| o.sql == sql) {
            Some(observation) => observation.ruv2_metrics.add_session_parser_total(1),
            None => self.ruv2_pending_session_parser_total += 1,
        }
    }

    /// Go `clientConn.writeChunks`' deferred `AddResultChunkCells`: the
    /// rows times columns the server wrote for the running statement.
    pub fn add_result_chunk_cells(&mut self, cells: i64) {
        if cells <= 0 {
            return;
        }
        if let Some(observation) = &self.statement_observation {
            observation.ruv2_metrics.add_result_chunk_cells(cells);
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
        let text = self.statement_text(sql).into_owned();
        // The details the server's door initialized for this statement, or
        // Go `execdetails.ContextWithInitializedExecDetails`'s fresh ones.
        let (normalized, digest, ru_details, exec_details, network_traffic_details) = match self
            .pending_statement_exec_details
            .take()
            .filter(|pending| pending.sql == sql)
        {
            Some(pending) => (
                pending.normalized,
                pending.digest,
                pending.ru_details,
                pending.exec_details,
                pending.network_traffic_details,
            ),
            None => {
                let (normalized, digest) = normalize_statement_digest(&text);
                (
                    normalized,
                    digest,
                    Arc::new(tikv_client::RuDetails::new()),
                    Arc::default(),
                    Arc::default(),
                )
            }
        };
        let parse = self
            .pending_observation_parse
            .take()
            .filter(|(parsed_sql, _)| parsed_sql == sql)
            .map_or(Duration::ZERO, |(_, duration)| duration);
        self.statement_txn_start_ts = 0;
        // Go `executeStmtImpl`: `sessVars.RUV2Metrics` is the statement ctx's,
        // its bypass decided (`shouldBypass`: no internal source type here),
        // and the parses counted since the last statement move into it.
        let ruv2_metrics = Arc::new(tidb_util::ruv2_metrics::RuV2Metrics::new());
        ruv2_metrics.set_bypass(false);
        let pending_parses = std::mem::take(&mut self.ruv2_pending_session_parser_total);
        if pending_parses > 0 {
            ruv2_metrics.add_session_parser_total(pending_parses);
        }
        // Go `ResetContextOfStmt`: `vars.DurationWaitTS = 0`.
        self.phase_durations.reset_for_statement();
        // Go `clientConn.dispatch`: `SQLCPUUsages.ResetCPUTimes()`.
        self.sql_cpu_usages.reset_cpu_times();
        // Go `ResetContextOfStmt`: `vars.MemTracker.ResetMaxConsumed()` and
        // `vars.DiskTracker.ResetMaxConsumed()`, so `Mem_max`/`Disk_max` are
        // this statement's peaks over what the session still holds.
        self.session_memory.session_tracker().reset_max_consumed();
        self.session_memory
            .session_disk_tracker()
            .reset_max_consumed();
        self.statement_ended_txn_start_ts = None;
        self.statement_began_txn = false;
        self.statement_committed_txn = false;
        self.statement_observation = Some(StatementObservation {
            sql: sql.to_owned(),
            text,
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
            exec_details,
            network_traffic_details,
            ruv2_metrics,
            record_last_query: false,
            crucial: false,
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
            &observation.exec_details,
            &observation.network_traffic_details,
            &observation.normalized,
            &observation.digest,
            Some(Arc::clone(&self.process_plan_info)),
        );
    }

    /// Publishes `ru_details`, `exec_details` (with the client's traffic
    /// collector `network_traffic_details`) and the taggers of the statement with this
    /// normalized text and digest. `plan` is where its plan digest will be
    /// read; `None` before the statement is planned, whose tags then carry
    /// no plan digest (Go's nil `StmtCtx` plan digest).
    fn publish_kv_context(
        &self,
        ru_details: &Arc<tikv_client::RuDetails>,
        exec_details: &Arc<tikv_client::util::ExecDetails>,
        network_traffic_details: &Arc<tikv_client::traffic::NetworkTrafficDetails>,
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
                exec_details: Some(Arc::clone(exec_details)),
                network_traffic_details: Some(Arc::clone(network_traffic_details)),
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
        let exec_details = Arc::default();
        let network_traffic_details = Arc::default();
        let (normalized, digest) = normalize_statement_digest(&self.statement_text(sql));
        self.publish_kv_context(
            &ru_details,
            &exec_details,
            &network_traffic_details,
            &normalized,
            &digest,
            None,
        );
        self.pending_statement_exec_details = Some(PendingStatementExecDetails {
            sql: sql.to_owned(),
            ru_details,
            exec_details,
            network_traffic_details,
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
        // The SQL `PREPARE` parsed this text with the scanner's default client
        // (Go `PrepareExec`, `needReset` false), whose `Text()` it is.
        let text = statement_text_in(sql, tidb_parser::ParserConfig::default().charset_client)
            .into_owned();
        if let Some(observation) = &mut self.statement_observation {
            let (normalized, digest) = normalize_statement_digest(&text);
            if self.statement_normalized_sql.is_some() {
                self.statement_normalized_sql = Some(normalized.clone());
            }
            observation.sql = sql.to_owned();
            observation.text = text;
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
            // Go's ExecuteStmt node records whatever it executes.
            observation.record_last_query = observation.prepared || records_last_query_info(stmt);
            observation.crucial = is_crucial_stmt(stmt);
            observation.routed = !matches!(stmt, Stmt::Dml(_) | Stmt::Query(_));
            // Go `getSQLForProcessInfo`: the process list shows a sensitive
            // statement's secure text once its node is known.
            observation.secure_text = statement_secure_text(stmt, &observation.text);
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
        let error = result
            .as_ref()
            .err()
            .map(|error| error.clone().to_mysql_error().message);
        self.publish_statement_observation(result.is_ok(), affected_rows, result_rows, error);
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
                    &self.statement_text(sql),
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
        self.finish_routed_statement_observation_with_error(succeeded, affected_rows, None);
    }

    /// [`Self::finish_routed_statement_observation`] with the failed
    /// statement's error text, which Go `recordLastQueryInfo` records.
    pub fn finish_routed_statement_observation_with_error(
        &mut self,
        succeeded: bool,
        affected_rows: u64,
        error: Option<String>,
    ) {
        if self.routed_statement_observation_depth == 0 {
            return;
        }
        self.routed_statement_observation_depth -= 1;
        if self.routed_statement_observation_depth == 0 {
            self.finish_routed_statement_warnings(succeeded);
            self.publish_statement_observation(succeeded, affected_rows, 0, error);
            self.refresh_process_status();
        }
    }

    fn publish_statement_observation(
        &mut self,
        succeeded: bool,
        affected_rows: u64,
        result_rows: i64,
        error: Option<String>,
    ) {
        let Some(observation) = self.statement_observation.take() else {
            return;
        };
        // Go observes compiled statements, not a parser failure without an AST.
        if observation.label.is_empty() {
            return;
        }
        // Go `finishStmt` -> `autoCommitAfterStmt`: a statement that
        // succeeded outside a transaction ends with `CommitTxn`, unless it
        // already committed one itself.
        if succeeded && !self.statement_committed_txn && !self.in_transaction() {
            self.set_txn_isolation_level_one_shot_state_for_next_txn();
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
        // Go `finalizeStatementRUV2Metrics`, before the slow log and the
        // summary read the metrics: the raw TiKV RU v2 counters the
        // statement's RPCs left in its `RUDetails`.
        tidb_util::ruv2_metrics::sync_ruv2_metrics_from_ru_details(
            Some(&observation.ruv2_metrics),
            Some(&observation.ru_details),
        );
        let ruv2_weights = crate::slow_log::ruv2_weights();
        // Go runs these from `FinishExecuteStmt` (and `logStmt` from
        // `runStmt`), which only a compiled statement reaches.
        if phases.compile_finished || phases.execution_started {
            let plan_info = Arc::clone(&self.process_plan_info);
            let plan = plan_info
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.log_slow_query(
                &observation,
                elapsed,
                succeeded,
                result_rows,
                &plan,
                &phases,
            );
            drop(plan);
            self.update_prev_stmt(&observation);
            self.record_last_query_info(&observation, error);
            self.log_stmt(&observation);
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
            .unwrap_or_else(|| observation.text.clone());
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
            schema_name: self.current_db.go_to_lower(),
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
            exec_retry_count: phases.exec_retry_count,
            exec_retry_time: phases.exec_retry_time(elapsed),
            write_sql_resp_duration: phases.write_sql_resp,
            result_rows,
            tikv_exec_details: Some(crate::slow_log::load_tikv_exec_details(
                &observation.exec_details,
                &observation.network_traffic_details,
            )),
            prepared: observation.prepared,
            keyspace_name: tidb_config::config_tree::config::get_global_config()
                .keyspace_name
                .clone(),
            keyspace_id: 0,
            resource_group_name: self.active_resource_group.clone(),
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
                .wrapping_add(self.sql_cpu_usages.get_cpu_usages().tikv_cpu_time),
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

/// The slow-log, general-log and last-query-info halves of Go
/// `ExecStmt.FinishExecuteStmt` (`pkg/executor/adapter.go`) and of
/// `session.logStmt` (`pkg/session/session.go`).
impl Session {
    /// Go `vardef.ProcessGeneralLog` / `cfg.Instance.*`: an instance-scoped
    /// switch read through the session's variable table.
    fn instance_switch_on(&self, name: &str) -> bool {
        self.vars
            .get_system(name)
            .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("ON"))
    }

    /// Go `SessionVars.EnableRedactLog`, one of `OFF`, `ON` and `MARKER`.
    fn redact_log_mode(&self) -> String {
        let mode = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_REDACT_LOG)
            .unwrap_or_default()
            .to_ascii_uppercase();
        match mode.as_str() {
            "ON" | "MARKER" => mode,
            _ => "OFF".to_owned(),
        }
    }

    /// Go `PlanCacheParamList.String`: the prepared execution's arguments,
    /// empty without any.
    fn plan_cache_params_string(&self) -> String {
        match &self.prepared_params {
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
        self.vars
            .get_global(tidb_vardef::tidb_vars::TIDB_QUERY_LOG_MAX_LEN)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .map_or(4096, |value| i32::try_from(value).unwrap_or(i32::MAX))
    }

    /// Go `TxnCtx.StartTS` and `TxnCtx.GetForUpdateTS()`: the for-update
    /// timestamp when it is newer than the start, else the start.
    fn txn_start_and_for_update_ts(&self) -> (u64, u64) {
        let start_ts = match u64::try_from(self.current_tso.value()).unwrap_or(0) {
            0 => self.statement_txn_start_ts,
            start_ts => start_ts,
        };
        let for_update_ts = u64::try_from(self.current_for_update_ts.value()).unwrap_or(0);
        (start_ts, for_update_ts.max(start_ts))
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
        let conn_id = self.connection_id();
        // Go's triggers, decided before any entry field is gathered: the
        // `write_slow_log` hint writes the entry without selecting it.
        let mut match_rules = false;
        let mut force = false;
        let mut rules = None;
        if !self.stmt_hints.write_slow_log {
            // Go also forces the entry while runtime tracing is on; this node
            // has no runtime tracer.
            force = tidb_util::logutil::get_level() <= tidb_log::Level::Debug;
            if !self.instance_switch_on(tidb_vardef::tidb_vars::TIDB_ENABLE_SLOW_LOG) && !force {
                return;
            }
            let effective = crate::slow_log_rules::EffectiveSlowLogRules::new(
                &self
                    .vars
                    .get_system(tidb_vardef::tidb_vars::TIDB_SLOW_LOG_RULES)
                    .unwrap_or_default(),
                &self
                    .vars
                    .get_global(tidb_vardef::tidb_vars::TIDB_SLOW_LOG_RULES)
                    .unwrap_or_default(),
                conn_id,
            );
            if effective.is_empty() {
                // Go stores the threshold as `uint64(TidbOptInt64(..))`, so a
                // negative one never selects a statement.
                let threshold = self
                    .vars
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
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_SESSION_ALIAS)
            .unwrap_or_default();
        let exec_detail = observation.cop_stats.exec_details();
        let cop_tasks = observation.cop_stats.cop_tasks_details();
        let plan_digest = if observation.routed {
            String::new()
        } else {
            plan.plan_digest.clone()
        };
        let mem_max = self.session_memory.session_tracker().max_consumed();
        let disk_max = self.session_memory.session_disk_tracker().max_consumed();
        let durations = self.phase_durations.snapshot();
        let kv_exec_detail =
            crate::slow_log::KvExecDetails::from(&crate::slow_log::load_tikv_exec_details(
                &observation.exec_details,
                &observation.network_traffic_details,
            ));
        if let Some(rules) = rules {
            match_rules = rules.should_write_slow_log(&crate::slow_log_rules::SlowLogRuleValues {
                conn_id,
                session_alias: session_alias.clone(),
                db: self.current_db.clone(),
                exec_retry_count: phases.exec_retry_count,
                query_time: elapsed,
                parse_time: phases.parse,
                compile_time: phases.compile,
                rewrite_time: durations.rewrite_phase_info.duration_rewrite,
                optimize_time: durations.duration_optimizer.total,
                wait_ts: durations.duration_wait_ts,
                is_internal: self.restricted_sql,
                digest: observation.digest.to_string(),
                num_cop_tasks: cop_tasks.as_ref().map(|details| details.num_cop_tasks),
                mem_max,
                disk_max,
                write_sql_response_total: phases.write_sql_resp,
                succ: succeeded,
                resource_group: self.active_resource_group.clone(),
                kv_exec_detail: Some(kv_exec_detail),
                exec_detail: exec_detail.clone(),
                plan_digest: plan_digest.clone(),
            });
            if !match_rules && !force {
                return;
            }
        }

        let max_per_sec = self
            .vars
            .get_global(tidb_vardef::tidb_vars::TIDB_SLOW_LOG_MAX_PER_SEC)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        if !crate::slow_log::slow_log_rate_limiter_allow(max_per_sec) {
            crate::slow_log::slow_log_sample_logger().info(
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
        let txn_ts = match self.statement_ended_txn_start_ts {
            Some(start_ts) => start_ts,
            None if self.statement_began_txn => 0,
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
            .login_user
            .as_deref()
            .and_then(|identity| identity.split_once('@'))
            .map(|(user, host)| (user.to_owned(), host.to_owned()));
        let keyspace_name = tidb_util::keyspace::get_keyspace_name_by_settings();
        let items = crate::slow_log::SlowQueryLogItems {
            txn_ts,
            keyspace_name: if tidb_util::keyspace::is_keyspace_name_empty(&keyspace_name) {
                String::new()
            } else {
                keyspace_name
            },
            keyspace_id: 0,
            sql: crate::slow_log::format_sql(
                &self.text_to_log(observation, true),
                self.query_log_max_len(),
            ),
            digest: observation.digest.to_string(),
            time_total: elapsed,
            index_names,
            succ: succeeded,
            // Go `TxnCtx.IsExplicit`, which an ended transaction's context
            // keeps until the next statement replaces it.
            is_explicit_txn: self.in_transaction() || self.statement_ended_txn_start_ts.is_some(),
            is_write_cache_table: false,
            is_sync_stats_failed: !observation.routed && plan.is_sync_stats_failed,
            prepared: observation.prepared,
            plan_from_cache: !observation.routed && self.found_in_plan_cache,
            plan_from_binding: !observation.routed && self.found_in_binding,
            has_more_results: false,
            prev_stmt: if observation.label == "Commit" {
                self.prev_stmt_text
                    .as_deref()
                    .map(|text| crate::slow_log::format_sql(text, self.query_log_max_len()))
                    .unwrap_or_default()
            } else {
                String::new()
            },
            plan: if !observation.routed
                && !plan.encoded_plan.is_empty()
                && self.instance_switch_on(tidb_vardef::tidb_vars::TIDB_RECORD_PLAN_IN_SLOW_LOG)
            {
                format!(
                    "{}{}{}",
                    crate::slow_log::SLOW_LOG_PLAN_PREFIX,
                    plan.encoded_plan,
                    crate::slow_log::SLOW_LOG_PLAN_SUFFIX
                )
            } else {
                String::new()
            },
            plan_digest,
            binary_plan: if !observation.routed
                && !plan.summary_binary_plan.is_empty()
                && self
                    .vars
                    .get_global(tidb_vardef::tidb_vars::TIDB_GENERATE_BINARY_PLAN)
                    .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("ON"))
            {
                format!(
                    "{}{}{}",
                    crate::slow_log::SLOW_LOG_BINARY_PLAN_PREFIX,
                    plan.summary_binary_plan,
                    crate::slow_log::SLOW_LOG_PLAN_SUFFIX
                )
            } else {
                String::new()
            },
            cop_tasks,
            // Go `SQLCPUUsages.GetCPUUsages()`: every coprocessor
            // response's process time (`selectResult.updateCopRuntimeStats`)
            // and every point reader's (`PointGetExecutor.Close`).
            cpu_usages: crate::slow_log::CpuUsages {
                tikv_cpu_time: exec_detail.cop_exec_details.time_detail.process_time
                    + Duration::from_nanos(
                        u64::try_from(self.sql_cpu_usages.get_cpu_usages().tikv_cpu_time)
                            .unwrap_or(0),
                    ),
                ..crate::slow_log::CpuUsages::default()
            },
            storage_kv,
            storage_mpp,
            exec_detail,
            result_rows,
            warnings: self
                .warnings
                .iter()
                .map(|warning| crate::slow_log::JsonSqlWarnForSlowLog {
                    level: match warning.level {
                        WarningLevel::Warning => "Warning",
                        WarningLevel::Error => "Error",
                        WarningLevel::Note => "Note",
                    }
                    .to_owned(),
                    message: warning.message.clone(),
                    is_extra: false,
                })
                .collect(),
            resource_group_name: self.active_resource_group.clone(),
            ru_details: Some(Arc::clone(&observation.ru_details)),
            mem_max,
            disk_max,
            session_connect_attrs: self.connection_attributes.clone(),
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
            ..crate::slow_log::SlowQueryLogItems::default()
        };
        let vars = crate::slow_log::SlowLogSessionVars {
            client_ip: login.as_ref().map(|(_, host)| host.clone()),
            user: login,
            connection_id: conn_id,
            session_alias: session_alias.clone(),
            duration_parse: phases.parse,
            duration_compile: phases.compile,
            duration_optimizer: durations.duration_optimizer,
            duration_wait_ts: durations.duration_wait_ts,
            ruv2_weights: crate::slow_log::ruv2_weights(),
            current_db: self.current_db.clone(),
            in_restricted_sql: self.restricted_sql,
            current_db_changed: self.current_db_changed,
            ..crate::slow_log::SlowLogSessionVars::default()
        };
        let entry = crate::slow_log::slow_log_format(&vars, &items);
        // Go `SlowLogFormat` clears `CurrentDBChanged` once it printed `use`.
        self.current_db_changed = false;
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
        crate::show_admin::record_slow_query(tidb_domain::topn_slow_query::SlowQueryInfo {
            sql: items.sql,
            start: Some(observation.start_time.into()),
            duration: elapsed,
            detail: items.exec_detail.to_string(),
            succ: succeeded,
            conn_id,
            sess_alias: session_alias,
            txn_ts,
            user: self.current_user.clone().unwrap_or_default(),
            db: self.current_db.clone(),
            table_ids,
            index_names: items.index_names,
            digest: items.digest,
            internal: self.restricted_sql,
        });
    }

    /// Go `ExecStmt.updatePrevStmt`: the statement text a later slow
    /// `COMMIT` prints as `Prev_stmt` (`FormatSQL(GetTextToLog(false))`).
    fn update_prev_stmt(&mut self, observation: &StatementObservation) {
        self.prev_stmt_text = Some(self.text_to_log(observation, false));
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
        let ru_consumption = observation.ru_details.read_ru() + observation.ru_details.write_ru();
        // Go `json.Marshal(sessionstates.QueryInfo)`: `error` is
        // `omitempty`, and a float64 marshals in its shortest form.
        let mut info = format!(
            "{{\"txn_scope\":\"global\",\"start_ts\":{start_ts},\"for_update_ts\":{for_update_ts},\"ru_consumption\":{}",
            go_json_float(ru_consumption)
        );
        if let Some(error) = error.filter(|error| !error.is_empty()) {
            info.push_str(",\"error\":");
            let mut quoted = String::new();
            crate::slow_log::go_json_string(&mut quoted, &error);
            info.push_str(&quoted);
        }
        info.push('}');
        *self.last_query_info.borrow_mut() = info;
    }

    /// Go `session.logStmt`: a crucial statement goes to the background log
    /// as `CRUCIAL OPERATION`; any other goes to the general log
    /// (`logGeneralQuery`) while `tidb_general_log` is on.
    fn log_stmt(&self, observation: &StatementObservation) {
        let user = self.current_user.clone().unwrap_or_default();
        let schema_version = self.cluster_schema_version_now();
        if observation.crucial {
            let mut fields = vec![
                tidb_log::Field::new("conn", tidb_log::Value::U64(self.connection_id())),
                tidb_log::Field::new("schemaVersion", tidb_log::Value::I64(schema_version)),
            ];
            if let Some(secure_text) = &observation.secure_text {
                fields.push(tidb_log::Field::new(
                    "secure text",
                    tidb_log::Value::Str(secure_text.clone()),
                ));
            } else {
                fields.push(tidb_log::Field::new(
                    "cur_db",
                    tidb_log::Value::Str(self.current_db.clone()),
                ));
                // Go `logStmt`: `zap.String("sql", stmt.Text())`.
                fields.push(tidb_log::Field::new(
                    "sql",
                    tidb_log::Value::Str(observation.text.clone()),
                ));
            }
            fields.push(tidb_log::Field::new("user", tidb_log::Value::Str(user)));
            tidb_util::logutil::bg_logger().info("CRUCIAL OPERATION", &fields);
            return;
        }
        self.log_general_query(observation, schema_version);
    }

    /// Go `logGeneralQuery`: one `GENERAL_LOG` entry per user statement.
    fn log_general_query(&self, observation: &StatementObservation, schema_version: i64) {
        if !self.instance_switch_on(tidb_vardef::tidb_vars::TIDB_GENERAL_LOG) || self.restricted_sql
        {
            return;
        }
        let mut query = crate::slow_log::query_replacer(&self.text_to_log(observation, false));
        let mode = self.redact_log_mode();
        if mode != "ON" {
            query.push_str(&tidb_util::redact::string(
                &mode,
                &self.plan_cache_params_string(),
            ));
        }
        let (start_ts, for_update_ts) = self.txn_start_and_for_update_ts();
        let txn_mode = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_TXN_MODE)
            .unwrap_or_default()
            .to_ascii_uppercase();
        let fields = [
            tidb_log::Field::new("conn", tidb_log::Value::U64(self.connection_id())),
            tidb_log::Field::new(
                "session_alias",
                tidb_log::Value::Str(
                    self.vars
                        .get_system(tidb_vardef::tidb_vars::TIDB_SESSION_ALIAS)
                        .unwrap_or_default(),
                ),
            ),
            tidb_log::Field::new(
                "user",
                tidb_log::Value::Str(self.login_user.clone().unwrap_or_default()),
            ),
            tidb_log::Field::new("schemaVersion", tidb_log::Value::I64(schema_version)),
            tidb_log::Field::new("txnStartTS", tidb_log::Value::U64(start_ts)),
            tidb_log::Field::new("forUpdateTS", tidb_log::Value::U64(for_update_ts)),
            tidb_log::Field::new(
                "isReadConsistency",
                tidb_log::Value::Bool(self.is_read_committed()),
            ),
            tidb_log::Field::new("currentDB", tidb_log::Value::Str(self.current_db.clone())),
            tidb_log::Field::new(
                "isPessimistic",
                tidb_log::Value::Bool(self.txn_mode() == Some(SessionTxnMode::Pessimistic)),
            ),
            tidb_log::Field::new(
                "sessionTxnMode",
                tidb_log::Value::Str(if txn_mode.is_empty() {
                    "OPTIMISTIC".to_owned()
                } else {
                    txn_mode
                }),
            ),
            tidb_log::Field::new("sql", tidb_log::Value::Str(query)),
        ];
        // Go adds the statement's original bytes, `strconv.Quote`d, when they
        // differ from its `Text()` (a literal spelled in hex, a decoded
        // client charset).
        let mut fields = fields.to_vec();
        let origin = tidb_hack::raw_bytes::str_to_bytes(&observation.sql);
        if *origin != *observation.text.as_bytes() {
            fields.push(tidb_log::Field::new(
                "originText",
                tidb_log::Value::Str(tidb_error::mysql::go_quote_bytes(&origin)),
            ));
        }
        tidb_util::logutil::general_logger().info("GENERAL_LOG", &fields);
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

/// Go `encoding/json`'s float64 spelling: the shortest round-trip digits,
/// in exponent form below 1e-6 or from 1e21 up.
fn go_json_float(value: f64) -> String {
    let abs = value.abs();
    if abs != 0.0 && (abs < 1e-6 || abs >= 1e21) {
        let formatted = tidb_datatype::go_strconv::format_float(value, b'e', -1, 64);
        // Go cleans `e-07` to `e-7`.
        if let Some(index) = formatted.find("e-0") {
            let mut cleaned = formatted.clone();
            cleaned.replace_range(index..index + 3, "e-");
            return cleaned;
        }
        return formatted;
    }
    tidb_datatype::go_strconv::format_float(value, b'f', -1, 64)
}

/// Go `recordLastQueryInfo`'s node test: `*ast.ShowStmt` other than `SHOW
/// SESSION_STATES`, `*ast.ExecuteStmt`, or an `ast.DMLNode` (SELECT, set
/// operations, INSERT/REPLACE, UPDATE, DELETE, LOAD DATA, IMPORT INTO, CALL,
/// non-transactional DML, DISTRIBUTE TABLE and SPLIT REGION).
fn records_last_query_info(stmt: &Stmt) -> bool {
    match stmt {
        Stmt::Query(_) | Stmt::Dml(_) => true,
        Stmt::Session(session) => matches!(&**session, tidb_ast::SessionStmt::Execute { .. }),
        Stmt::Admin(admin) => match &**admin {
            tidb_ast::AdminStmt::SplitRegion(_) => true,
            tidb_ast::AdminStmt::ShowInspection(show) => {
                show.kind != tidb_ast::ShowInspectionKind::SessionStates
            }
            // The `ADMIN SHOW ...` forms are Go `AdminStmt`s, not `ShowStmt`s.
            tidb_ast::AdminStmt::ShowGrants(_)
            | tidb_ast::AdminStmt::ShowMasterStatus
            | tidb_ast::AdminStmt::ShowPrivileges
            | tidb_ast::AdminStmt::ShowBuiltins
            | tidb_ast::AdminStmt::ShowImportJobs(_)
            | tidb_ast::AdminStmt::ShowImportGroups(_)
            | tidb_ast::AdminStmt::ShowCreate { .. }
            | tidb_ast::AdminStmt::ShowCreateUser(_)
            | tidb_ast::AdminStmt::ShowVariables(_)
            | tidb_ast::AdminStmt::ShowStatus(_)
            | tidb_ast::AdminStmt::ShowWarnings(_)
            | tidb_ast::AdminStmt::ShowErrors(_)
            | tidb_ast::AdminStmt::ShowCollation(_)
            | tidb_ast::AdminStmt::ShowEngines(_)
            | tidb_ast::AdminStmt::ShowCharset(_)
            | tidb_ast::AdminStmt::ShowStatsHistograms(_)
            | tidb_ast::AdminStmt::ShowStatsBuckets(_)
            | tidb_ast::AdminStmt::ShowStatsLocked(_)
            | tidb_ast::AdminStmt::ShowStatsTopN(_)
            | tidb_ast::AdminStmt::ShowDatabases(_)
            | tidb_ast::AdminStmt::ShowTables(_)
            | tidb_ast::AdminStmt::ShowOpenTables(_)
            | tidb_ast::AdminStmt::ShowTableStatus(_)
            | tidb_ast::AdminStmt::ShowTableNextRowId(_)
            | tidb_ast::AdminStmt::ShowColumns(_)
            | tidb_ast::AdminStmt::ShowIndex(_)
            | tidb_ast::AdminStmt::ShowDistributionJobs(_)
            | tidb_ast::AdminStmt::ShowTablePlacement(_)
            | tidb_ast::AdminStmt::ShowPlacement(_)
            | tidb_ast::AdminStmt::ShowProfile(_)
            | tidb_ast::AdminStmt::ShowMaskingPolicies(_)
            | tidb_ast::AdminStmt::ShowBindings(_) => true,
            _ => false,
        },
        Stmt::Ddl(_) => false,
    }
}

/// Go `logStmt`'s crucial statements: index changes other than hypothetical
/// ones, and the account, privilege, binding, BRIE and schema statements.
fn is_crucial_stmt(stmt: &Stmt) -> bool {
    match stmt {
        Stmt::Ddl(ddl) => match &**ddl {
            tidb_ast::DdlStmt::DropIndex(drop) => !drop.is_hypo,
            tidb_ast::DdlStmt::CreateIndex(create) => {
                create.options.index_type != Some(tidb_ast::IndexType::HYPO)
            }
            tidb_ast::DdlStmt::CreateUser { .. }
            | tidb_ast::DdlStmt::DropUser { .. }
            | tidb_ast::DdlStmt::AlterUser(_)
            | tidb_ast::DdlStmt::AlterTable(_)
            | tidb_ast::DdlStmt::CreateDatabase { .. }
            | tidb_ast::DdlStmt::CreateTable(_)
            | tidb_ast::DdlStmt::DropDatabase { .. }
            | tidb_ast::DdlStmt::DropTable(_)
            | tidb_ast::DdlStmt::DropView { .. }
            | tidb_ast::DdlStmt::RenameTable(_)
            | tidb_ast::DdlStmt::TruncateTable(_)
            | tidb_ast::DdlStmt::RenameUser { .. } => true,
            _ => false,
        },
        Stmt::Session(session) => matches!(&**session, tidb_ast::SessionStmt::SetPassword(_)),
        Stmt::Admin(admin) => matches!(
            &**admin,
            tidb_ast::AdminStmt::Grant(_)
                | tidb_ast::AdminStmt::Revoke(_)
                | tidb_ast::AdminStmt::CreateBinding(_)
                | tidb_ast::AdminStmt::DropBinding(_)
                | tidb_ast::AdminStmt::SetBinding(_)
                | tidb_ast::AdminStmt::Brie(_)
        ),
        Stmt::Query(_) | Stmt::Dml(_) => false,
    }
}

/// Go `observeStmtBeginForTopProfiling`'s `ExecBeginInfo`: under Top RU it
/// carries the statement's `RUDetails` (Go's `Ctx`, whose only use is that
/// lookup), the user and the domain's RU version (Go
/// `NormalizeRUVersion(do.GetRUVersion())`).
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
        // Go `stmtstats.NormalizeRUVersion(do.GetRUVersion())`.
        info.ru_version = tidb_util::topsql_stmtstats::normalize_ru_version(
            tidb_util::topsql_stmtstats::bound_ru_version(),
        );
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `ExecRetryTime = TimeTotal - DurationParse - DurationCompile -
    /// time.Since(retryStartTime)`: the failed attempts' share of the
    /// statement, and zero for a statement that never retried.
    #[test]
    fn exec_retry_time_is_the_failed_attempts_share() {
        let mut phases = StatementPhases {
            parse: Duration::from_millis(1),
            compile: Duration::from_millis(2),
            ..StatementPhases::default()
        };
        assert_eq!(
            phases.exec_retry_time(Duration::from_secs(1)),
            Duration::ZERO
        );
        phases.exec_retry_count = 2;
        phases.retry_start = Some(std::time::Instant::now());
        let retry = phases.exec_retry_time(Duration::from_millis(100));
        // The last attempt began just now, so nearly all of the 97ms left
        // after parsing and compiling belongs to the failed attempts.
        assert!(retry <= Duration::from_millis(97), "{retry:?}");
        assert!(retry >= Duration::from_millis(90), "{retry:?}");
        // A clock that ran past the total saturates at zero.
        assert_eq!(
            phases.exec_retry_time(Duration::from_millis(2)),
            Duration::ZERO
        );
    }
}
