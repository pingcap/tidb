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
use tidb_executor::exec_stmt_observation::{
    ExecStmtObservation as _, StatementObservation, StatementPhases,
};
use tidb_topsql_stmtstats::{ExecFinishInfo, StatementObserver};




/// The exec details [`Session::initialize_statement_exec_details`] created
/// for the statement the session is about to run, with the digest it
/// computed for them.
pub(crate) struct PendingStatementExecDetails {
    sql: String,
    ru_details: Arc<tikv_client::RuDetails>,
    exec_details: Arc<tikv_client::util::ExecDetails>,
    normalized: String,
    digest: tidb_parser::Digest,
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
        if self.vars.restricted_sql {
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
        requests: &[tidb_executor::table_privilege::TablePrivilegeRequest],
    ) {
        self.observe_statement_tables(
            requests
                .iter()
                .map(|request| (request.database.as_str(), request.table.as_str())),
        );
    }

    /// Go `StmtCtx.Tables`: the `db`.`table` pairs the statement reads,
    /// which its summary reports.
    pub(crate) fn observe_statement_tables<'a>(
        &self,
        names: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) {
        let Some(observation) = &self.statement_observation else {
            return;
        };
        let mut tables = Vec::new();
        for (db, table) in names {
            if db.is_empty() && table.is_empty() {
                continue;
            }
            let entry = tidb_stmtsummary::statement_summary::TableEntry {
                db: db.to_owned(),
                table: table.to_owned(),
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
    ) -> tidb_topsql_stmtstats::KvExecCounterHandle {
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
    ) -> Option<Arc<dyn tidb_distsql::select_response::CopResponseSink>> {
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
    ) -> Option<Arc<dyn Fn(tidb_sessionctx::StatementPhase) + Send + Sync>> {
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
                tidb_sessionctx::StatementPhase::PlanReady => {
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
                tidb_sessionctx::StatementPhase::ExecutorReady => {
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
            if !fast || tidb_topsql_state::top_profiling_enabled() {
                let info = top_profiling_begin_info(&ru_details, &user);
                stats.on_execution_begin(digest.as_bytes(), &[], Some(&info));
                if tidb_topsql_state::top_sql_enabled() {
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
        let (normalized, digest, ru_details, exec_details) = match self
            .pending_statement_exec_details
            .take()
            .filter(|pending| pending.sql == sql)
        {
            Some(pending) => (
                pending.normalized,
                pending.digest,
                pending.ru_details,
                pending.exec_details,
            ),
            None => {
                let (normalized, digest) = normalize_statement_digest(&text);
                (
                    normalized,
                    digest,
                    Arc::new(tikv_client::RuDetails::new()),
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
        let ruv2_metrics = Arc::new(tidb_execdetails::ruv2_metrics::RuV2Metrics::new());
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
                tidb_executor::statement_cop_stats::StatementCopStats::with_ru_details(Arc::clone(
                    &ru_details,
                )),
            ),
            ru_details,
            exec_details,
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
            &observation.normalized,
            &observation.digest,
            Some(Arc::clone(&self.process_plan_info)),
        );
    }

    /// Publishes `ru_details`, `exec_details` and the taggers of the
    /// statement with this normalized text and digest. `plan` is where its plan digest will be
    /// read; `None` before the statement is planned, whose tags then carry
    /// no plan digest (Go's nil `StmtCtx` plan digest).
    fn publish_kv_context(
        &self,
        ru_details: &Arc<tikv_client::RuDetails>,
        exec_details: &Arc<tikv_client::util::ExecDetails>,
        normalized: &str,
        digest: &tidb_parser::Digest,
        plan: Option<Arc<Mutex<tidb_stmtctx::ProcessPlanInfo>>>,
    ) {
        // Go `StmtCtx.SQLDigest`: a statement with no normalized text tags
        // no SQL digest.
        let sql_digest = (!normalized.is_empty()).then(|| digest.as_bytes().to_vec());
        let keyspace_name = tidb_keyspace::get_keyspace_name_bytes_by_settings();
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
                txn_start_ts: None,
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
        let (normalized, digest) = normalize_statement_digest(&self.statement_text(sql));
        self.publish_kv_context(&ru_details, &exec_details, &normalized, &digest, None);
        self.pending_statement_exec_details = Some(PendingStatementExecDetails {
            sql: sql.to_owned(),
            ru_details,
            exec_details,
            normalized,
            digest,
        });
    }

    /// The digest [`Self::initialize_statement_exec_details`] computed for
    /// the statement about to run, so its process info reuses it.
    pub(crate) fn pending_statement_digest(&self, sql: &str) -> Option<String> {
        self.pending_statement_exec_details
            .as_ref()
            .filter(|pending| pending.sql == sql)
            .map(|pending| pending.digest.to_string())
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
                ));
                if !fast || tidb_topsql_state::top_profiling_enabled() {
                    let info = top_profiling_begin_info(&observation.ru_details, &user);
                    self.statement_stats.on_execution_begin(
                        observation.digest.as_bytes(),
                        &[],
                        Some(&info),
                    );
                    if tidb_topsql_state::top_sql_enabled() {
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
                    tidb_bindinfo::binding::normalize_with_db(stmt, &self.vars.current_db);
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
            .map(|error| error.error_string());
        self.publish_statement_observation(result.is_ok(), affected_rows, result_rows, error);
    }

    /// Starts observation when durable completion belongs outside Session.
    pub fn begin_routed_statement_observation(&mut self, sql: &str, stmt: &Stmt) {
        self.begin_statement_observation(sql);
        self.observe_statement_node(stmt, self.binary_prepared_execution);
        // The outer routed execution owns one LazyTxn.onStmtStart, including
        // transaction control and SET. Its inner executor only republishes.
        if self.routed_statement_observation_depth == 0 {
            self.routed_statement_kind = Some(crate::classify::statement_kind_of(stmt));
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
            // Go `ResetContextOfStmt`: after SET, DDL or transaction control
            // the next `ROW_COUNT()` reads 0 (captured: after SET).
            if self.routed_statement_kind.take() == Some(crate::classify::StatementKind::Other)
                && !self.vars.in_set_session_states_stmt
            {
                self.vars.prev_row_count = 0;
            }
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
        let top_ru = tidb_topsql_state::top_ru_enabled();
        if phases.execution_started && (tidb_topsql_state::top_sql_enabled() || top_ru) {
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
        tidb_execdetails::ruv2_metrics::sync_ruv2_metrics_from_ru_details(
            Some(&observation.ruv2_metrics),
            Some(&observation.ru_details),
        );
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
        self.summary_stmt(
            &observation,
            &phases,
            elapsed,
            succeeded,
            affected_rows,
            result_rows,
        );
    }
}

/// The slow-log, general-log and last-query-info halves of Go
/// `ExecStmt.FinishExecuteStmt` (`pkg/executor/adapter.go`) and of
/// `session.logStmt` (`pkg/session/session.go`).
impl Session {


    /// Go `parser.Normalize(sql, EnableRedactLog)` then
    /// `executor.FormatSQL(sql).String()`: the statement text the connection's
    /// dispatch-failure log carries (`getLastStmtInConn` for `COM_QUERY` /
    /// `COM_STMT_PREPARE`).
    #[must_use]
    pub fn dispatch_failure_sql(&self, sql: &str) -> String {
        tidb_variable::slow_log::format_sql(
            &tidb_parser::normalize(sql, self.redact_mode()),
            self.query_log_max_len(),
        )
    }

    /// Go `clientConn.preparedStmt2String` under `FormatSQL`: the prepared
    /// text normalized, plus the execution's arguments unless redaction is
    /// fully on.
    #[must_use]
    pub fn dispatch_failure_prepared_sql(&self, sql: &str) -> String {
        let mode = self.redact_log_mode();
        let mut text = tidb_parser::normalize(sql, self.redact_mode());
        if mode != "ON" {
            text.push_str(&tidb_redact::string(
                &mode,
                &self.plan_cache_params_string(),
            ));
        }
        tidb_variable::slow_log::format_sql(&text, self.query_log_max_len())
    }

    /// Go `SessionVars.GetReadableTxnMode`: `@@tidb_txn_mode`, `OPTIMISTIC`
    /// when empty.
    #[must_use]
    pub fn readable_txn_mode(&self) -> String {
        match self.vars.get_system("tidb_txn_mode") {
            Ok(mode) if !mode.is_empty() => mode,
            _ => "OPTIMISTIC".to_owned(),
        }
    }

    /// The `timestamp` of the dispatch-failure log: `TxnCtx.StartTS`.
    #[must_use]
    pub fn dispatch_failure_timestamp(&self) -> u64 {
        self.txn_start_and_for_update_ts().0
    }

    /// Go `errStrForLog`: with log redaction on, a parse error is replaced by
    /// a fixed sentence because its text may carry secrets; otherwise the
    /// error's `Error()` text. (Go appends the stack for every error but
    /// `ErrKeyExists` / `ErrParse` / `ErrTableNotExists`; this tier has no
    /// error stacks to append.)
    #[must_use]
    pub fn err_str_for_log(
        &self,
        error_string: &str,
        identity: Option<tidb_terror::TerrorIdentity>,
    ) -> String {
        if self.redact_log_mode() != "OFF"
            && identity == Some(tidb_parser::parser_errors::ERR_PARSE.identity())
        {
            return "fail to parse SQL, and must redact the whole error when enable log redaction"
                .to_owned();
        }
        error_string.to_owned()
    }








    /// Go `handlePessimisticLockError` replaces the statement's error before
    /// `recordLastQueryInfo` reads it: the storage error a lock raised is the
    /// statement owner's to consume, and what is recorded is the lock's
    /// outcome. This tier recorded the storage error already, so the owner
    /// rewrites the recorded `error` once it knows the outcome.
    pub(crate) fn rerecord_last_query_error(&mut self, error: &str) {
        let info = &mut self.vars.last_query_info;
        if !info.err_msg.is_empty() {
            info.err_msg = error.to_owned();
        }
    }


    /// Go `session.logStmt`: a crucial statement goes to the background log
    /// as `CRUCIAL OPERATION`; any other goes to the general log
    /// (`logGeneralQuery`) while `tidb_general_log` is on.
    fn log_stmt(&self, observation: &StatementObservation) {
        let user = self.vars.current_user.clone().unwrap_or_default();
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
                    tidb_log::Value::Str(self.vars.current_db.clone()),
                ));
                // Go `logStmt`: `zap.String("sql", stmt.Text())`.
                fields.push(tidb_log::Field::new(
                    "sql",
                    tidb_log::Value::Str(observation.text.clone()),
                ));
            }
            fields.push(tidb_log::Field::new("user", tidb_log::Value::Str(user)));
            tidb_logutil::bg_logger().info("CRUCIAL OPERATION", &fields);
            return;
        }
        self.log_general_query(observation, schema_version);
    }

    /// Go `logGeneralQuery`: one `GENERAL_LOG` entry per user statement.
    fn log_general_query(&self, observation: &StatementObservation, schema_version: i64) {
        if !self.instance_switch_on(tidb_vardef::tidb_vars::TIDB_GENERAL_LOG) || self.vars.restricted_sql
        {
            return;
        }
        let mut query = tidb_variable::slow_log::query_replacer(&self.text_to_log(observation, false));
        let mode = self.redact_log_mode();
        if mode != "ON" {
            query.push_str(&tidb_redact::string(
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
                tidb_log::Value::Str(self.vars.login_user.clone().unwrap_or_default()),
            ),
            tidb_log::Field::new("schemaVersion", tidb_log::Value::I64(schema_version)),
            tidb_log::Field::new("txnStartTS", tidb_log::Value::U64(start_ts)),
            tidb_log::Field::new("forUpdateTS", tidb_log::Value::U64(for_update_ts)),
            tidb_log::Field::new(
                "isReadConsistency",
                tidb_log::Value::Bool(self.is_read_committed()),
            ),
            tidb_log::Field::new("currentDB", tidb_log::Value::Str(self.vars.current_db.clone())),
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
                tidb_log::Value::Str(tidb_mysql::errors::go_quote_bytes(&origin)),
            ));
        }
        tidb_logutil::general_logger().info("GENERAL_LOG", &fields);
    }
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
            tidb_ast::AdminStmt::Show(show) => show.tp != tidb_ast::ShowStmtType::SessionStates,
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
            tidb_ast::DdlStmt::AlterTable(_)
            | tidb_ast::DdlStmt::CreateDatabase { .. }
            | tidb_ast::DdlStmt::CreateTable(_)
            | tidb_ast::DdlStmt::DropDatabase { .. }
            | tidb_ast::DdlStmt::DropTable(_)
            | tidb_ast::DdlStmt::DropView { .. }
            | tidb_ast::DdlStmt::RenameTable(_)
            | tidb_ast::DdlStmt::TruncateTable(_) => true,
            _ => false,
        },
        Stmt::Session(session) => matches!(&**session, tidb_ast::SessionStmt::SetPassword(_)),
        Stmt::Admin(admin) => matches!(
            &**admin,
            tidb_ast::AdminStmt::CreateUser { .. }
                | tidb_ast::AdminStmt::DropUser { .. }
                | tidb_ast::AdminStmt::AlterUser(_)
                | tidb_ast::AdminStmt::RenameUser { .. }
                | tidb_ast::AdminStmt::Grant(_)
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
        Stmt::Ddl(_) => None,
        Stmt::Session(session) => match session.as_ref() {
            tidb_ast::SessionStmt::SetPassword(set_password) => Some(set_password.secure_text()),
            tidb_ast::SessionStmt::Set(set) => Some(set.secure_text()),
            _ => None,
        },
        Stmt::Admin(admin) => match admin.as_ref() {
            tidb_ast::AdminStmt::CreateUser { users, .. } => {
                Some(tidb_ast::create_user_secure_text(users))
            }
            tidb_ast::AdminStmt::AlterUser(alter) => Some(alter.secure_text()),
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

/// Go `ExecStmt`'s `sctx`, as the statement's observation reads it.
impl tidb_executor::exec_stmt_observation::ExecStmtObservation for Session {
    fn observed_connection_id(&self) -> u64 {
        self.connection_id()
    }

    fn write_slow_log_hint(&self) -> bool {
        self.stmt_hints.write_slow_log
    }

    fn statement_mem_max(&self) -> i64 {
        self.session_memory.session_tracker().max_consumed()
    }

    fn statement_disk_max(&self) -> i64 {
        self.session_memory.session_disk_tracker().max_consumed()
    }

    fn session_phase_durations(&self) -> &tidb_variable::phase_durations::SessionPhaseDurations {
        &self.phase_durations
    }

    fn observed_resource_group(&self) -> String {
        self.active_resource_group.clone()
    }

    fn statement_ended_txn_start_ts(&self) -> Option<u64> {
        self.statement_ended_txn_start_ts
    }

    fn statement_began_txn(&self) -> bool {
        self.statement_began_txn
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

    fn slow_log_warnings(&self) -> Vec<tidb_variable::slow_log::JsonSqlWarnForSlowLog> {
        self.warnings
            .iter()
            .map(|warning| tidb_variable::slow_log::JsonSqlWarnForSlowLog {
                level: warning.level.clone(),
                message: warning.message().to_owned(),
                is_extra: false,
            })
            .collect()
    }

    fn statement_warning_count(&self) -> usize {
        self.warnings.len()
    }

    fn sql_tikv_cpu_time(&self) -> i64 {
        self.sql_cpu_usages.get_cpu_usages().tikv_cpu_time
    }

    fn connection_attributes(&self) -> &std::collections::BTreeMap<String, String> {
        &self.connection_attributes
    }

    fn prepared_params(&self) -> Option<&[Datum]> {
        self.prepared_params.as_deref()
    }

    fn previous_summary_statement(&mut self) -> &mut Option<(String, String)> {
        &mut self.previous_summary_statement
    }

    fn summary_user(&self) -> String {
        self.current_identity()
            .map_or("", |(user, _)| user)
            .to_owned()
    }

    fn process_plan_info_cell(&self) -> Arc<std::sync::Mutex<tidb_stmtctx::ProcessPlanInfo>> {
        Arc::clone(&self.process_plan_info)
    }
}
