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

//! The statement warning buffer, which Go keeps in `StmtCtx.warnings` and
//! `SHOW WARNINGS` / `SHOW ERRORS` report.
//!
//! The buffer belongs to the statement before the one running, which is why the
//! statements that REPORT it must be handed the previous statement's entries --
//! see [`reports_warnings`].

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_executor::compiler::PreparedExec;
use std::borrow::Cow;

use tidb_ast::{AdminStmt, Stmt};
use tidb_datatype::Datum;

use crate::{DriverError, Session};

/// A statement warning, Go's `contextutil.SQLWarn`: the level and the error
/// `StmtCtx` keeps, which `SHOW WARNINGS` reports as `Level | Code | Message`.
pub use tidb_util::context::SqlWarn as SqlWarning;

/// `StmtCtx.warnings[i].Level`: Go's `contextutil.WarnLevelWarning`,
/// `WarnLevelError` and `WarnLevelNote` (`pkg/util/context/warn.go`), reached
/// through `StmtCtx.AppendWarning` / `AppendError` / `AppendNote`.
pub use tidb_util::context::WarningLevel;

/// Whether the statement reports the warning buffer, and so must be handed the
/// previous statement's entries instead of an empty buffer.
///
/// Go decides this on the parsed node: `ResetContextOfStmt` takes an
/// `ast.StmtNode`, builds a fresh `StatementContext`, and for exactly
/// `ShowWarnings`, `ShowErrors`, and `ShowSessionStates` copies the outgoing
/// context's entries into it (`pkg/executor/select.go`, `case *ast.ShowStmt`).
/// This mirrors that set. `SHOW COUNT(*) WARNINGS` parses to `ShowWarnings`
/// with `CountWarningsOrErrors`, so it needs no separate spelling.
pub(crate) fn reports_warnings(stmt: &Stmt) -> bool {
    let Stmt::Admin(admin) = stmt else {
        return false;
    };
    match &**admin {
        AdminStmt::Show(show) => matches!(
            show.tp,
            tidb_ast::ShowStmtType::Warnings
                | tidb_ast::ShowStmtType::Errors
                | tidb_ast::ShowStmtType::SessionStates
        ),
        _ => false,
    }
}

impl Session {
    /// The one door onto the statement's warning buffer.
    ///
    /// Go has a single sink: every `AppendWarning`/`AppendNote`/`AppendError`
    /// on `StmtCtx` reaches one `StaticWarnHandler`
    /// (`pkg/util/context/warn.go`). Keeping one door here is what lets a rule
    /// that belongs to the buffer -- its retention limit, its ordering --
    /// live in one place rather than at each caller.
    ///
    /// That rule is Go's retention limit: `StaticWarnHandler` stops appending
    /// once the buffer holds `math.MaxUint16` entries,
    /// since the count it publishes is a `uint16`.
    pub(crate) fn append_warning(&mut self, level: WarningLevel, code: u16, message: String) {
        self.append_sql_warning(SqlWarning::with_code(level, code, message));
    }

    /// Go's error door (`driver_tidb.go:376`): a statement's error is also
    /// appended to the warning buffer at `level`, error and all.
    pub(crate) fn append_driver_error(&mut self, level: WarningLevel, error: &DriverError) {
        let reported = error.clone().to_mysql_error();
        self.append_sql_warning(SqlWarning::from_error(
            level,
            reported.code,
            reported.message,
            reported.identity,
        ));
    }

    /// [`Self::append_warning`] of a warning kept with its error.
    pub(crate) fn append_sql_warning(&mut self, warning: SqlWarning) {
        if self.warnings.len() >= u16::MAX as usize {
            return;
        }
        self.warnings.push(warning);
    }

    /// Takes back the `Error` row the statement pipeline filed for a failure
    /// the statement owner then replaced: a lock that failed inside execution
    /// reaches the pipeline as a storage error, while Go's
    /// `handlePessimisticLockError` consumes that error (a retry) or returns
    /// the lock error itself, which `TiDBContext.ExecuteStmt` files once.
    pub fn retract_statement_failure(&mut self, code: u16, message: &str) {
        if let Some(position) = self.warnings.iter().rposition(|warning| {
            warning.warning_level() == WarningLevel::Error
                && warning.code() == code
                && warning.message() == message
        }) {
            self.warnings.remove(position);
        }
    }

    /// Files the error an `IF EXISTS` / `IF NOT EXISTS` clause swallowed as
    /// Go's `Note`.
    ///
    /// Go writes it that way too: `dropTableObject` hands
    /// `StmtCtx.AppendNote` the very `ErrBadTable` it would otherwise have
    /// returned (`pkg/ddl/executor.go`), so the note's code and text are the
    /// suppressed error's, never a second string that can drift from it.
    pub(crate) fn append_suppressed(&mut self, error: DriverError) {
        self.append_driver_error(WarningLevel::Note, &error);
    }

    /// Go `ShowExec.fetchShowWarnings(errOnly)`: one `Level | Code | Message`
    /// row per reported warning (errors only for SHOW ERRORS), or, for SHOW
    /// COUNT(*), the one count.
    pub(crate) fn warning_rows(&self, count_only: bool, errors_only: bool) -> Vec<Vec<Datum>> {
        let reported = self
            .warnings
            .iter()
            .filter(|warning| !errors_only || warning.warning_level() == WarningLevel::Error);
        if count_only {
            return vec![vec![Datum::Int(reported.count() as i64)]];
        }
        reported
            .map(|warning| {
                vec![
                    Datum::Bytes(warning.level.as_str().as_bytes().to_vec()),
                    Datum::Int(i64::from(warning.code())),
                    // A message quoting the client's SQL (a parse error's
                    // `near` text) carries a byte that is not UTF-8 as its
                    // `raw_bytes` escape; Go's message holds the byte.
                    Datum::Bytes(tidb_hack::raw_bytes::str_to_bytes(&warning.message()).into_owned()),
                ]
            })
            .collect()
    }

    /// Moves what evaluation recorded into the statement's warning buffer.
    /// Drains a lowering/probe context's warnings into this session's
    /// buffer — the cluster DDL route's half of Go's one warning handler:
    /// Go appends `errCheckConstraintIsOff`-style DDL warnings to the SAME
    /// session `StmtContext` the statement runs under, while this stack
    /// lowers cluster DDL in a context of its own that would otherwise drop
    /// them (the seam gap named at the CHECK arm of `table_info_build`).
    pub fn drain_context_warnings(&mut self, ctx: &tidb_executor::StmtContext) {
        self.drain_eval_warnings(ctx);
    }

    pub(crate) fn drain_eval_warnings(&mut self, ctx: &tidb_executor::StmtContext) {
        // The constant-fold stash travels thread-locally and a QueryRecordSet's
        // finish would otherwise be its only drain -- statements that ERROR
        // before producing one (FORMAT('x', 'y')'s 1582) would leave their
        // fold-time warnings for the NEXT statement's drain to mis-attribute.
        ctx.drain_fold_warnings();
        for warning in ctx.take_warnings() {
            // NOTE: the DDL lowering files LEGITIMATE Error-level rows here
            // (the 1846 algorithm refusal, the 8200 WITHOUT VALIDATION
            // warning -- oracle g-alter) on statements that SUCCEED, so this
            // drain must not blanket-drop Error level. The fatal-eval case
            // (the statement failing WITH the same error: g-group's 3158)
            // dedups at the error door's retain filter instead.
            self.append_sql_warning(warning);
        }
    }

    /// Moves the thread's constant-fold stash into the statement warning
    /// list: the error doors' drain (a statement that fails before producing
    /// a record set reports its fold-time warnings beside the error, in the
    /// order go's buffer carries them -- the fold rows first, the error row
    /// last).
    pub fn drain_fold_stash(&mut self) {
        for warning in tidb_executor::take_fold_warnings() {
            self.append_sql_warning(warning);
        }
    }

    /// Records a warning raised by a cluster-routed statement after it has
    /// bypassed the ordinary session executor.
    pub fn append_routed_warning(&mut self, code: u16, message: String) {
        self.append_warning(WarningLevel::Warning, code, message);
    }

    /// Records a NOTE raised by a cluster-routed statement: go raises every
    /// `IF [NOT] EXISTS` suppression with `StmtCtx.AppendNote`, so
    /// `CREATE DATABASE IF NOT EXISTS db` on an existing db leaves
    /// `Note | 1007 | Can't create database 'db'; database exists`.
    pub fn append_routed_note(&mut self, code: u16, message: String) {
        self.append_warning(WarningLevel::Note, code, message);
    }

    /// The warnings the last statement produced.
    #[must_use]
    pub fn warnings(&self) -> &[SqlWarning] {
        &self.warnings
    }

    /// Drops the warnings appended after the first `len`, for a caller that
    /// ran a step whose warnings belong to a later statement (routing done at
    /// PREPARE whose warnings Go raises at EXECUTE).
    pub fn truncate_warnings(&mut self, len: usize) {
        self.warnings.truncate(len);
    }

    /// Parses one statement at a statement boundary, giving it the fresh
    /// warning state Go's `ResetContextOfStmt` builds.
    ///
    /// Every front-end door that starts a statement comes through here, so the
    /// buffer `SHOW WARNINGS` reports and the count the OK/EOF packet carries
    /// have exactly one lifetime. Calling it twice for the same statement is
    /// what a front end that parses before dispatching does, and it lands on
    /// the same state both times: the inherit hands back precisely what the
    /// take removed.
    ///
    /// Taking the buffer before the parse keeps a statement that fails to
    /// parse clearing it, which is what Go's failed parse does by never
    /// reaching the copy.
    pub fn parse_at_statement_boundary(&mut self, sql: &str) -> Result<Stmt, DriverError> {
        // Text SET/control routes can bypass begin_statement_execution.
        // Retire their predecessor before evaluating the new command.
        if let Some(previous) = self.statement_result_authority.get_mut().take() {
            previous.statement_memory().finish_statement();
        }
        self.prepared_params = None;
        self.statement_boundary_open = true;
        self.parse_warnings = None;
        let previous = std::mem::take(&mut self.warnings);
        self.pending_observation_parse = None;
        let parse_started = std::time::Instant::now();
        let (mut stmt, parse_warnings) = match tidb_parser::parse_with_sql_mode_config_and_warnings(
            sql,
            self.scanner_sql_mode(),
            self.vars.build_parser_config(),
        ) {
            Ok(output) => (output.statement, output.warnings),
            Err(error) => {
                // The parse-failure door maps the error; the diagnostics
                // a failed parse carried are dropped (go's failed parse
                // never reaches the warning copy either).
                let mapped = match error.terror {
                    Some(proto) => DriverError::from((proto).generate_with_stack(error.message)),
                    None => DriverError::from(tidb_util::misc::syntax_error(
                        error.compatibility_message(sql),
                    )),
                };
                return Err(mapped);
            }
        };
        self.record_observation_parse(sql, parse_started.elapsed());
        self.record_ruv2_session_parse(sql);
        self.observe_statement_node(&stmt, false);
        self.install_statement_warning_state(&stmt, previous);
        // go's recoverable parser diagnostics land as warning rows AFTER the
        // context reset: the `[parser:8061]`-prefixed hint refusals decode
        // into their class code with the remainder as the message (oracle:
        // `SELECT /*+ WRONG_HINT_SYNTAX(xyz) */ ...` warns 8061).
        let parse_warnings_start = self.warnings.len();
        for diagnostic in parse_warnings {
            // A class-less diagnostic is Go's hint refusal (8061).
            let proto = diagnostic
                .terror
                .unwrap_or(&tidb_error::catalog::parser::ERR_WARN_OPTIMIZER_HINT_PARSE_ERROR);
            self.append_sql_warning(SqlWarning::of(
                crate::WarningLevel::Warning,
                proto,
                diagnostic.body(),
            ));
        }
        for message in decimal_literal_warnings(sql, &mut stmt) {
            // Go lexer `toDecimal`: the parser's `types.ErrTruncatedWrongValue`.
            self.append_sql_warning(SqlWarning::of(
                crate::WarningLevel::Warning,
                &tidb_error::catalog::parser_types::ERR_TRUNCATED_WRONG_VALUE,
                message,
            ));
        }
        // Go's connection appends the parser's warnings after the statement
        // has run (`handleStmt`); they are recorded here and moved behind the
        // execution's own warnings when the statement completes.
        let parsed = self.warnings[parse_warnings_start.min(self.warnings.len())..].to_vec();
        self.parse_warnings = (!parsed.is_empty()).then_some((parse_warnings_start, parsed));
        Ok(stmt)
    }

    /// Go `clientConn.handleStmt`: the parser's warnings follow the warnings
    /// the statement itself raised, once it has succeeded; a statement that
    /// failed returns before `AppendWarnings`, so its context never holds
    /// them (captured: an ascii client's `SELECT é`, whose identifier warns
    /// 1300 as `?`, shows only its 1054). A buffer the statement rebuilt keeps
    /// what it holds.
    pub(crate) fn move_parse_warnings_behind_execution(&mut self, succeeded: bool) {
        let Some((start, parsed)) = self.parse_warnings.take() else {
            return;
        };
        let end = start + parsed.len();
        if self.warnings.get(start..end) == Some(parsed.as_slice()) {
            self.warnings.drain(start..end);
            if succeeded {
                self.warnings.extend(parsed);
            }
        }
    }

    /// [`Self::parse_at_statement_boundary`]'s boundary for a text statement
    /// the front end already parsed: Go parses a command once
    /// (`session.ParseSQL`) and every later step reads that node.
    pub(crate) fn begin_text_statement_boundary(&mut self, stmt: &Stmt) {
        self.observe_statement_node(stmt, false);
        // The front end's own parse already opened the boundary
        // (`parse_at_statement_boundary`); a second open would take the
        // (already-empty) buffer and reset the counts the first boundary
        // snapshotted — go's `ResetContextOfStmt` runs once per statement.
        if self.statement_boundary_open {
            self.statement_boundary_open = false;
            self.prepared_params = None;
            return;
        }
        self.prepared_params = None;
        let previous = std::mem::take(&mut self.warnings);
        self.install_statement_warning_state(stmt, previous);
    }

    /// Starts a statement boundary for an AST retained by PREPARE.
    pub(crate) fn begin_prepared_statement_boundary(
        &mut self,
        stmt: &Stmt,
        parameters: Option<std::sync::Arc<[Datum]>>,
    ) {
        self.observe_statement_node(stmt, true);
        self.prepared_params = parameters;
        let previous = std::mem::take(&mut self.warnings);
        self.install_statement_warning_state(stmt, previous);
    }

    /// Starts the boundary for the cached prepared PointGet path. That path is
    /// known to be a SELECT rather than SHOW WARNINGS, so the previous buffer
    /// is discarded without revisiting the retained AST.
    /// The statement boundary for a cluster-routed statement, which bypasses
    /// the ordinary run path: the previous statement's warnings go, and the
    /// routed statement is never SHOW WARNINGS.
    pub fn begin_routed_statement_warnings(&mut self) {
        self.restore_statement_variables();
        // Go's `ResetContextOfStmt` runs once per statement. A text statement
        // the front end parsed already reset here
        // (`parse_at_statement_boundary`), and the buffer now holds the
        // parser's own warnings for it, which the statement must keep.
        if std::mem::take(&mut self.statement_boundary_open) {
            return;
        }
        self.snapshot_previous_warning_counts();
        self.warnings.clear();
        self.in_show_warning = false;
    }

    /// The end of a routed statement: Go's `clientConn.handleStmt` appends
    /// the parser's warnings after the statement's own, and the boundary the
    /// parse opened closes with the statement.
    pub(crate) fn finish_routed_statement_warnings(&mut self, succeeded: bool) {
        self.statement_boundary_open = false;
        self.move_parse_warnings_behind_execution(succeeded);
    }

    pub(crate) fn begin_cached_prepared_query_boundary(&mut self) {
        self.restore_statement_variables();
        self.prepared_params = None;
        if let Some(previous) = self.statement_result_authority.get_mut().take() {
            // Retire before installing the next root action, even when a
            // retained authority delays Drop beyond the statement boundary.
            previous.statement_memory().finish_statement();
        }
        self.snapshot_previous_warning_counts();
        self.warnings.clear();
        self.in_show_warning = false;
    }

    fn install_statement_warning_state(&mut self, stmt: &Stmt, previous: Vec<SqlWarning>) {
        self.restore_statement_variables();
        // Go `ResetContextOfStmt` resolves an EXECUTE's prepared statement
        // (`GetPreparedStmt`) before it counts the previous warnings and
        // installs the new statement context, so an unknown name fails with
        // the previous statement's warnings and counts still in place, and
        // its 8111 joins them (`SELEC 1; EXECUTE nonexist; SHOW WARNINGS`
        // lists both errors).
        if let Stmt::Session(session_stmt) = stmt {
            if let tidb_ast::SessionStmt::Execute { name, .. } = session_stmt.as_ref() {
                if self.prepared_statement_sql(name).is_none() {
                    self.warnings = previous;
                    self.in_show_warning = false;
                    return;
                }
            }
        }
        self.set_previous_warning_counts(&previous);
        // go `clientConn.handleLoadData`: a LOCAL load without the client
        // capability refuses BEFORE any statement context reset, so the
        // warning buffer keeps the PREVIOUS statement's rows (oracle:
        // `LOAD DATA LOCAL INFILE ...` answers 1148 with the prior
        // statement's 8154 still in SHOW WARNINGS).
        if !self.local_infile_accepted
            && matches!(
                stmt,
                Stmt::Dml(dml)
                    if matches!(
                        dml.as_ref(),
                        tidb_ast::DmlStmt::LoadData(load) if load.local
                    )
            )
        {
            self.warnings = previous;
            self.in_show_warning = false;
            return;
        }
        self.in_show_warning = reports_warnings(stmt);
        if self.in_show_warning {
            self.warnings = previous;
        }
    }

    /// Restores the preceding SET_VAR overlay at Go's ResetContextOfStmt
    /// boundary. Front ends that select a cached plan or read snapshot before
    /// entering execution call this after parsing, before those decisions.
    /// Finish/Close continue to use the producing statement's variables.
    pub fn restore_statement_variables(&mut self) {
        self.vars
            .restore_system(std::mem::take(&mut self.set_var_hint_restore));
    }

    /// Go `ResetContextOfStmt`'s
    /// `errCount, warnCount := vars.StmtCtx.NumErrorWarnings()` followed by
    /// the two assignments to `vars.SysErrorCount` / `vars.SysWarningCount`,
    /// for the boundaries that discard the buffer without taking it first.
    fn snapshot_previous_warning_counts(&mut self) {
        let previous = std::mem::take(&mut self.warnings);
        self.set_previous_warning_counts(&previous);
        self.warnings = previous;
    }

    /// Go's `NumErrorWarnings`: the error count is the ERROR-level entries,
    /// the warning count is every entry.
    fn set_previous_warning_counts(&mut self, previous: &[SqlWarning]) {
        self.vars.sys_warning_count = previous.len();
        self.vars.sys_error_count = previous
            .iter()
            .filter(|warning| warning.warning_level() == WarningLevel::Error)
            .count();
    }

    /// Go `sysvar.go`'s `GetSession` hook on `warning_count`, which returns
    /// `s.SysWarningCount`.
    #[must_use]
    pub(crate) fn sys_warning_count(&self) -> usize {
        self.vars.sys_warning_count
    }

    /// Go's matching hook on `error_count`, `s.SysErrorCount`.
    #[must_use]
    pub(crate) fn sys_error_count(&self) -> usize {
        self.vars.sys_error_count
    }

    /// Go `clientConn.initResultEncoder`'s read:
    /// `GetSessionOrGlobalSystemVar(CharacterSetResults)`, the charset every
    /// column definition's identifiers and every string cell of a result set
    /// go out in. The empty string is Go's unset state.
    #[must_use]
    pub fn result_charset(&self) -> Cow<'_, str> {
        // Read for every result (Go `initResultEncoder`, one
        // `GetSessionOrGlobalSystemVar` probe per command). Only the
        // name-to-index step is cached; the value read is unchanged.
        static INDEX: crate::sysvar::StaticSysVarIndex =
            crate::sysvar::StaticSysVarIndex::new("character_set_results");
        self.vars
            .system_value_at(INDEX.get())
            .unwrap_or(Cow::Borrowed(""))
    }

    /// Go `clientConn.initInputEncoder`'s read of
    /// `@@character_set_client` for binary protocol string parameters.
    #[must_use]
    pub fn input_charset(&self) -> Cow<'_, str> {
        static INDEX: crate::sysvar::StaticSysVarIndex =
            crate::sysvar::StaticSysVarIndex::new("character_set_client");
        self.vars
            .system_value_at(INDEX.get())
            .unwrap_or(Cow::Borrowed("utf8mb4"))
    }

    /// The warning count the OK/EOF packet carries, which Go reads through
    /// `TiDBContext.WarningCount` in `writeOkWith` and `writeEOF`.
    ///
    /// This is the second channel `SHOW WARNINGS` does not cover: a driver
    /// learns a statement warned only from this field. It is NOT simply the
    /// buffer's length, because Go's `StatementContext.WarningCount` returns 0
    /// while `InShowWarning` is set (`pkg/sessionctx/stmtctx/stmtctx.go`) --
    /// the same three statements that inherit the buffer to REPORT it send a
    /// zero count, so a client is never told that the rows it is reading are
    /// themselves warnings.
    #[must_use]
    pub fn wire_warning_count(&self) -> u16 {
        if self.in_show_warning {
            return 0;
        }
        // `append_warning` holds the buffer at Go's `uint16` ceiling, so this
        // cannot lose a count that was actually retained.
        u16::try_from(self.warnings.len()).unwrap_or(u16::MAX)
    }
}

/// Go lexer `toDecimal` (`lexer_helpers.go`): a decimal literal whose
/// integer part overflows `MyDecimal`'s word buffer warns 1292 with what
/// `FromString` parsed and is replaced by `mysql.DefaultDecimal`, which the
/// expression rewriter substitutes. An integer literal beyond uint64 already
/// lexes as a decimal (`toInt` -> `toDecimal`). Only a literal with more than
/// 81 integer digits can overflow, so text without such a run is not walked.
fn decimal_literal_warnings(sql: &str, stmt: &mut Stmt) -> Vec<String> {
    const MAX_INTEGER_DIGITS: usize = 81;
    let mut run = 0usize;
    let has_long_run = sql.bytes().any(|byte| {
        run = if byte.is_ascii_digit() { run + 1 } else { 0 };
        run > MAX_INTEGER_DIGITS
    });
    if !has_long_run {
        return Vec::new();
    }
    struct Collector(Vec<String>);
    impl tidb_ast::Visitor for Collector {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(tidb_ast::Expr::Decimal(text)) = node.downcast_ref::<tidb_ast::Expr>() {
                let (value, error) = tidb_datatype::Decimal::parse_mysql(text);
                if matches!(error, Some(tidb_datatype::DecimalParseError::Overflow)) {
                    self.0
                        .push(format!("Truncated incorrect DECIMAL value: '{value}'"));
                }
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut collector = Collector(Vec::new());
    tidb_ast::Visitable::accept(stmt, &mut collector);
    collector.0
}
