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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Session-statement dispatch, session-variable execution, and statement clocks.

use std::time::{SystemTime, UNIX_EPOCH};

use tidb_ast::{
    Expr, SessionStmt, SetStmt, SetUserVarStmt, SetVariableValue, SystemVariableAssignment,
    SystemVariableScope,
};
use tidb_datatype::{Datum, StringDatum};
use tidb_expr::{eval, eval_in, truthy_of};

use crate::noop_read_only::{validate_read_only, NoopFuncsMode, NoopScope, NoopValidation};
use crate::session::{RelResolver, SessionState};
use crate::session_settings::{
    DivPrecisionIncrement, ForeignKeyChecks, MultiStatementMode, NoopFunctionsMode, SqlSelectLimit,
    TimeZoneSetting, TimestampSetting,
};
use crate::{Database, ExecError, Outcome, StatementStatus};

impl Database {
    pub(super) fn run_session(
        &mut self,
        stmt: &SessionStmt,
        status: Option<&mut StatementStatus>,
    ) -> Result<Outcome, ExecError> {
        match stmt {
            // `USE` is an accepted no-op: this executor resolves every table
            // name in one flat namespace, and MySQL permits it mid-transaction.
            SessionStmt::Use(_) => Ok(Outcome::Done),
            // Prepared statements need placeholder/session machinery this
            // executor doesn't model — parse+restore only.
            SessionStmt::Prepare { .. } => Err(ExecError::Unsupported("PREPARE")),
            SessionStmt::Execute { .. } => Err(ExecError::Unsupported("EXECUTE")),
            SessionStmt::Deallocate(_) => Err(ExecError::Unsupported("DEALLOCATE PREPARE")),
            SessionStmt::Set(set) => {
                self.exec_set(set, status)?;
                Ok(Outcome::Done)
            }
            SessionStmt::SetUserVar(set) => {
                self.exec_set_uservar(set)?;
                Ok(Outcome::Done)
            }
            // Full TiDB updates coordinated client, connection, result, and
            // collation session state here. This seed has none of that state,
            // so rejecting before touching the transaction is the only honest
            // execution boundary.
            SessionStmt::SetCharset { kind, .. } => match kind {
                tidb_ast::CharsetSetKind::Names => Err(ExecError::Unsupported("SET NAMES")),
                tidb_ast::CharsetSetKind::Charset => Err(ExecError::Unsupported("SET CHARSET")),
            },
            SessionStmt::SetMixed(_) => Err(ExecError::Unsupported("mixed SET charset list")),
            // Password changes require an authenticated principal, privilege
            // checks, and user/dual-password metadata. This seed owns none of
            // that state, so reject before any transaction or catalog change.
            SessionStmt::SetPassword(_) => Err(ExecError::Unsupported("SET PASSWORD")),
            // Role activation and default-role assignment need an authenticated
            // principal, grant graph, and privilege checks. None are present in
            // this seed, so both commands stop before transaction state can be
            // changed or accidentally committed.
            SessionStmt::SetRole(_) => Err(ExecError::Unsupported("SET ROLE")),
            SessionStmt::SetDefaultRole(_) => Err(ExecError::Unsupported("SET DEFAULT ROLE")),
            // Resource-group selection depends on the account privilege graph
            // and resource-control subsystem. The seed owns neither, so this
            // must reject before any transaction or catalog mutation.
            SessionStmt::SetResourceGroup(_) => Err(ExecError::Unsupported("SET RESOURCE GROUP")),
            // Session-state import drives TiDB's coordinated session migration
            // handlers. This executor has no compatible state codec or
            // privilege checks, so do not consume a transaction snapshot.
            SessionStmt::SetSessionStates(_) => Err(ExecError::Unsupported("SET SESSION_STATES")),
            SessionStmt::Begin(begin) => {
                if begin.as_of.is_some() {
                    return Err(ExecError::Unsupported(
                        "START TRANSACTION READ ONLY AS OF TIMESTAMP",
                    ));
                }
                if begin.read_only {
                    return Err(ExecError::Unsupported("START TRANSACTION READ ONLY"));
                }
                if begin.causal_consistency_only {
                    return Err(ExecError::Unsupported(
                        "START TRANSACTION WITH CAUSAL CONSISTENCY ONLY",
                    ));
                }
                self.transaction.begin(&self.tables);
                Ok(Outcome::Done)
            }
            SessionStmt::Commit(tidb_ast::CompletionType::Default) => {
                self.transaction.commit();
                Ok(Outcome::Done)
            }
            SessionStmt::Commit(_) => Err(ExecError::Unsupported("COMMIT completion mode")),
            SessionStmt::Rollback {
                savepoint: None,
                completion: tidb_ast::CompletionType::Default,
            } => {
                self.transaction.rollback(&mut self.tables);
                Ok(Outcome::Done)
            }
            SessionStmt::Rollback {
                savepoint: None,
                completion: _,
            } => Err(ExecError::Unsupported("ROLLBACK completion mode")),
            SessionStmt::Savepoint(name) => {
                // Go's SimpleExec opens the lazy non-autocommit transaction
                // for SAVEPOINT itself (`Ctx().Txn(true)`), not only for a
                // later DML statement.  Reuse the same entry boundary as DML
                // so a pre-write savepoint can be rolled back to under
                // `autocommit = 0`; in autocommit mode this remains a no-op.
                self.transaction.savepoint(name, &self.tables);
                Ok(Outcome::Done)
            }
            SessionStmt::Rollback {
                savepoint: Some(name),
                completion: tidb_ast::CompletionType::Default,
            } => {
                self.transaction
                    .rollback_to_savepoint(name, &mut self.tables)?;
                Ok(Outcome::Done)
            }
            SessionStmt::Rollback {
                savepoint: Some(_),
                completion: _,
            } => Err(ExecError::Unsupported(
                "ROLLBACK TO SAVEPOINT completion mode",
            )),
            SessionStmt::ReleaseSavepoint(name) => {
                self.transaction.release_savepoint(name)?;
                Ok(Outcome::Done)
            }
        }
    }

    /// Applies a system-variable `SET` list (see [`tidb_ast::SetStmt`]'s
    /// scope note):
    /// `timestamp` (the session clock, an epoch value in seconds,
    /// optionally fractional), `time_zone` (fixed `+HH:MM`/`-HH:MM` offsets
    /// plus `SYSTEM`/`UTC` readback labels; named zones are not modelled),
    /// `foreign_key_checks` (the source-backed session switch over this
    /// executor's existing FK enforcement paths), `default_week_format`
    /// (the bounded session mode consumed only by one-argument `WEEK`), and
    /// `div_precision_increment` (the decimal scale added by `/` and `AVG`),
    /// `sql_safe_updates` (a
    /// source-defined TiDB no-op compatibility value), `autocommit`, `tx_isolation`/
    /// `transaction_isolation` (one shared session value) and
    /// `tx_isolation_one_shot` (either the `SET [SESSION] TRANSACTION
    /// ISOLATION LEVEL ...` desugared forms — see `tidb_parser`'s own
    /// `parse_set_transaction` — or a direct `SET tx_isolation = value`;
    /// see [`crate::transaction::TransactionState`] for the exact validation
    /// and mid-transaction rejection rules), and the Go compatibility-only `tidb_enable_noop_functions`
    /// plus its gated `tx_read_only`/`transaction_read_only` aliases are
    /// recognized; any other variable is `Unsupported`. `GLOBAL` and
    /// `INSTANCE` are rejected before any assignment is applied: this seed
    /// executor has neither cluster state nor an instance configuration
    /// surface, and a fake map would claim semantics it cannot provide.
    /// `DEFAULT` is likewise explicit unsupported state rather than an
    /// expression to evaluate. Ordinary `value` expressions must be
    /// constant-evaluable (real MySQL allows an arbitrary expression here
    /// too, but every realistic use is a literal).
    fn exec_set(
        &mut self,
        s: &SetStmt,
        mut status: Option<&mut StatementStatus>,
    ) -> Result<(), ExecError> {
        // Reject unsupported scope before touching an earlier session item in
        // the same comma list.  This keeps a mixed-scope statement from
        // silently becoming a partial session mutation.
        if s.assignments
            .iter()
            .any(|assignment| !matches!(assignment.scope, SystemVariableScope::Session))
        {
            return Err(ExecError::Unsupported("SET GLOBAL/INSTANCE variable"));
        }
        for assignment in &s.assignments {
            self.exec_session_system_variable(assignment, status.as_deref_mut())?;
        }
        Ok(())
    }

    fn exec_session_system_variable(
        &mut self,
        assignment: &SystemVariableAssignment,
        status: Option<&mut StatementStatus>,
    ) -> Result<(), ExecError> {
        let name = assignment.name.to_ascii_lowercase();
        if matches!(assignment.value, SetVariableValue::Default) {
            match name.as_str() {
                "timestamp" => {
                    self.timestamp = TimestampSetting::Dynamic;
                    return Ok(());
                }
                "foreign_key_checks" => {
                    self.foreign_key_checks = ForeignKeyChecks::Enabled;
                    return Ok(());
                }
                "sql_safe_updates" => {
                    self.sql_safe_updates = false;
                    return Ok(());
                }
                "default_week_format" => {
                    self.default_week_format = 0;
                    return Ok(());
                }
                "sql_select_limit" => {
                    self.sql_select_limit = SqlSelectLimit::UNLIMITED;
                    return Ok(());
                }
                "div_precision_increment" => {
                    self.div_precision_increment = DivPrecisionIncrement::DEFAULT;
                    return Ok(());
                }
                "rand_seed1" => {
                    self.rng.borrow_mut().set_seed1(0);
                    return Ok(());
                }
                "rand_seed2" => {
                    self.rng.borrow_mut().set_seed2(0);
                    return Ok(());
                }
                "tidb_multi_statement_mode" => {
                    self.multi_statement_mode = MultiStatementMode::Off;
                    return Ok(());
                }
                "tidb_retry_limit" => {
                    self.tidb_retry_limit = 10;
                    return Ok(());
                }
                _ => {}
            }
        }
        let SetVariableValue::Expr(value) = &assignment.value else {
            return Err(ExecError::Unsupported("SET DEFAULT variable"));
        };
        match name.as_str() {
            "timestamp" => {
                let value = eval(value)?;
                let readback = value
                    .sql_string()
                    .map_err(|_| ExecError::Unsupported("SET timestamp value"))?;
                let epoch = match value {
                    Datum::Int(i) => i as f64,
                    // `timestamp` is a TypeFloat sysvar in Go. An unsigned
                    // literal is therefore a real numeric input, not a
                    // signed reinterpretation; the existing MaxInt32 guard
                    // below rejects values this seed cannot model as an
                    // epoch before they can change the clock.
                    Datum::UInt(i) => i as f64,
                    Datum::Real(f) | Datum::Float32(f) => f,
                    Datum::Decimal(d) => d.to_f64(),
                    Datum::String(s) => string_text(&s, "SET timestamp value")?
                        .parse::<f64>()
                        .map_err(|_| ExecError::Unsupported("SET timestamp value"))?,
                    _ => {
                        return Err(ExecError::Unsupported("SET timestamp value"));
                    }
                };
                // Go validates timestamp as a TypeFloat. Negative values are
                // normalized to its default string 0 (with a warning); the
                // seed has no warning surface, but must retain the observable
                // dynamic-clock transition. Its custom validator rejects
                // values beyond MaxInt32 rather than clipping them.
                if epoch < 0.0 || readback == "0" {
                    self.timestamp = TimestampSetting::Dynamic;
                } else if epoch > i32::MAX as f64 {
                    return Err(ExecError::Unsupported("SET timestamp value"));
                } else {
                    self.timestamp = TimestampSetting::Fixed { epoch, readback };
                }
                Ok(())
            }
            "time_zone" => {
                let Datum::String(tz) = eval(value)? else {
                    return Err(ExecError::Unsupported("SET time_zone value"));
                };
                self.time_zone = parse_time_zone(string_text(&tz, "SET time_zone format")?)
                    .ok_or(ExecError::Unsupported("SET time_zone format"))?;
                Ok(())
            }
            "autocommit" => {
                let value = parse_bool_value(value)?;
                self.transaction.set_autocommit(value);
                Ok(())
            }
            "foreign_key_checks" => {
                self.foreign_key_checks =
                    if parse_strict_bool_value(value, "SET foreign_key_checks value")? {
                        ForeignKeyChecks::Enabled
                    } else {
                        ForeignKeyChecks::Disabled
                    };
                Ok(())
            }
            "default_week_format" => {
                self.default_week_format = parse_default_week_format(value)?;
                Ok(())
            }
            "sql_select_limit" => {
                self.sql_select_limit = SqlSelectLimit::new(parse_sql_select_limit(value)?);
                Ok(())
            }
            "div_precision_increment" => {
                self.div_precision_increment = parse_div_precision_increment(value)?;
                Ok(())
            }
            "rand_seed1" => {
                self.rng.borrow_mut().set_seed1(parse_rand_seed(value)?);
                Ok(())
            }
            "rand_seed2" => {
                self.rng.borrow_mut().set_seed2(parse_rand_seed(value)?);
                Ok(())
            }
            // TiDB itself registers this in `variable/noop.go`: retain its
            // typed session value/readback but do not invent MySQL-style DML
            // restrictions the source implementation does not apply.
            "sql_safe_updates" => {
                self.sql_safe_updates =
                    parse_strict_bool_value(value, "SET sql_safe_updates value")?;
                Ok(())
            }
            // TiDB registers `transaction_isolation` as a mutual alias of
            // `tx_isolation`.  Store both spellings in the one session field
            // rather than maintaining aliases that could drift apart.
            name @ ("tx_isolation" | "transaction_isolation" | "tx_isolation_one_shot") => {
                let Datum::String(v) = eval(value)? else {
                    return Err(ExecError::Unsupported("SET tx_isolation value"));
                };
                let normalized = string_text(&v, "SET tx_isolation value")?.to_ascii_uppercase();
                if normalized != "READ-COMMITTED" && normalized != "REPEATABLE-READ" {
                    return Err(ExecError::Unsupported("SET tx_isolation level"));
                }
                if matches!(name, "tx_isolation" | "transaction_isolation") {
                    self.transaction.set_session_isolation(normalized);
                } else {
                    self.transaction.set_one_shot_isolation(normalized)?;
                }
                Ok(())
            }
            "tidb_enable_noop_functions" => {
                let mode = parse_noop_functions_mode(value)?;
                // Go's sysvar validation deliberately rejects an OFF
                // transition while one of its dependent noop variables is
                // still ON. This seed models only tx_read_only from that
                // dependency set, so make that one invariant structural.
                if mode == NoopFunctionsMode::Off && self.tx_read_only {
                    return Err(ExecError::Unsupported(
                        "SET tidb_enable_noop_functions=OFF while tx_read_only is ON",
                    ));
                }
                self.noop_functions_mode = mode;
                Ok(())
            }
            "tidb_multi_statement_mode" => {
                self.multi_statement_mode = parse_multi_statement_mode(value)?;
                Ok(())
            }
            "tidb_retry_limit" => {
                self.tidb_retry_limit = parse_tidb_retry_limit(value)?;
                Ok(())
            }
            // TiDB registers these as mutual aliases and keeps them as
            // no-op compatibility state: setting either changes readback,
            // never DML permissions. `checkReadOnly` gates a true value on
            // tidb_enable_noop_functions, but false is always permitted.
            "tx_read_only" | "transaction_read_only" => {
                let value = parse_strict_bool_value(value, "SET tx_read_only value")?;
                if value {
                    let mode = match self.noop_functions_mode {
                        NoopFunctionsMode::Off => NoopFuncsMode::Off,
                        NoopFunctionsMode::On => NoopFuncsMode::On,
                        NoopFunctionsMode::Warn => NoopFuncsMode::Warn,
                    };
                    match validate_read_only("ON", "ON", NoopScope::Session, mode, None, false) {
                        NoopValidation::Accepted {
                            warning: Some(warning),
                            ..
                        } => {
                            if let Some(status) = status {
                                status.warn(warning.message());
                            }
                        }
                        NoopValidation::Accepted { warning: None, .. } => {}
                        NoopValidation::Rejected { .. } => {
                            return Err(ExecError::Unsupported(
                                "SET tx_read_only=ON requires tidb_enable_noop_functions",
                            ));
                        }
                    }
                }
                self.tx_read_only = value;
                Ok(())
            }
            _ => Err(ExecError::Unsupported("SET variable")),
        }
    }

    /// Executes the source-ordered `SET @name = value [, ...]` list. Each
    /// right-hand side is resolved, evaluated, and written before the next
    /// one begins, matching Go's `SetExecutor` loop. Therefore a later
    /// assignment sees an earlier assignment's value and a later error does
    /// not roll back earlier writes. User variables are session-scoped and
    /// nontransactional, so this never touches the active transaction phase.
    fn exec_set_uservar(&mut self, s: &SetUserVarStmt) -> Result<(), ExecError> {
        for assignment in &s.assignments {
            let resolver = RelResolver::new(&[], &[], self.session_state());
            let folded = self.resolve_subqueries(&assignment.value, &resolver)?;
            let value = eval_in(&folded, &resolver)?;
            let name = assignment.name.to_ascii_lowercase();
            let mut user_vars = self.user_vars.borrow_mut();
            if value == Datum::Null {
                user_vars.remove(&name);
            } else {
                user_vars.insert(name, value);
            }
        }
        Ok(())
    }

    /// The current statement's cached clock: `(utc_secs, nanos,
    /// tz_offset_seconds)` — the RAW Unix time (never adjusted) plus the
    /// session's `time_zone` offset, matching [`tidb_expr::Columns::now`]'s
    /// own contract exactly (the LOCAL-vs-UTC adjustment happens in
    /// `tidb_expr::func`, per function — `NOW()`/`CURDATE()`/`CURTIME()`
    /// add the offset, `UTC_TIMESTAMP()`/`UTC_DATE()`/`UTC_TIME()` don't).
    /// Dynamic/default mode uses a live wall time cached once in the source's
    /// StatementContext; a fixed timestamp uses that same cache path.
    pub(crate) fn now_value(&self) -> Option<(i64, u32, i32)> {
        let (secs, nanos) = self.statement_clock();
        Some((secs, nanos, self.time_zone.offset_seconds()))
    }

    /// Returns the raw UTC time TiDB's StatementContext exposes to all clock
    /// functions and to dynamic timestamp readback.
    fn statement_clock(&self) -> (i64, u32) {
        if let Some(clock) = *self.statement_clock.borrow() {
            return clock;
        }
        let clock = match &self.timestamp {
            TimestampSetting::Dynamic => {
                let duration = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("host clock must not precede the Unix epoch");
                (duration.as_secs() as i64, duration.subsec_nanos())
            }
            TimestampSetting::Fixed { epoch, .. } => epoch_to_clock(*epoch),
        };
        *self.statement_clock.borrow_mut() = Some(clock);
        clock
    }

    fn timestamp_readback(&self, clock: (i64, u32)) -> String {
        match &self.timestamp {
            TimestampSetting::Dynamic => (clock.0 as f64 + f64::from(clock.1) / 1e9).to_string(),
            TimestampSetting::Fixed { readback, .. } => readback.clone(),
        }
    }

    /// Bundles [`Database::now_value`] with the `@@autocommit`/
    /// `@@time_zone`/`@@tx_isolation`/`@@tx_isolation_one_shot` readback
    /// state and a LIVE, shared reference to `@`user variables (see
    /// [`Database::user_vars`]'s own doc — an `Rc::clone`, not a deep
    /// snapshot), into the one [`SessionState`] every
    /// `RelResolver` construction site threads through.
    pub(crate) fn session_state(&self) -> SessionState {
        SessionState {
            now: self.now_value(),
            timestamp: self.timestamp_readback(self.statement_clock()),
            autocommit: self.transaction.autocommit(),
            time_zone: self.time_zone.clone(),
            foreign_key_checks: self.foreign_key_checks.is_enabled(),
            sql_safe_updates: self.sql_safe_updates,
            tx_isolation: self.transaction.session_isolation().to_string(),
            tx_isolation_one_shot: self.transaction.one_shot_isolation().to_string(),
            noop_functions_mode: self.noop_functions_mode,
            multi_statement_mode: self.multi_statement_mode,
            tx_read_only: self.tx_read_only,
            previous_affected_rows: self.previous_affected_rows,
            previous_last_insert_id: self.previous_last_insert_id,
            statement_last_insert_id: Some(self.statement_last_insert_id.clone()),
            sql_select_limit: self.sql_select_limit,
            default_week_format: self.default_week_format,
            div_precision_increment: self.div_precision_increment,
            rng: Some(self.rng.clone()),
            statement_rngs: Some(self.statement_rngs.clone()),
            user_vars: self.user_vars.clone(),
            sequences: self.sequences.clone(),
            seq_lastval: self.seq_lastval.clone(),
        }
    }
}

/// Parses the source-observable subset of Go's `timeutil.ParseTimeZone`:
/// `SYSTEM` is case-insensitively canonicalized, `UTC` preserves the loaded
/// location's spelling, and fixed `+HH:MM`/`-HH:MM` offsets keep their normal
/// deterministic execution behavior. Named IANA zones are intentionally not
/// silently approximated as offsets because this seed carries no timezone
/// database.
fn parse_time_zone(s: &str) -> Option<TimeZoneSetting> {
    if s.eq_ignore_ascii_case("SYSTEM") {
        return Some(TimeZoneSetting::System);
    }
    if s.eq_ignore_ascii_case("UTC") {
        return Some(TimeZoneSetting::Utc(s.to_string()));
    }
    parse_tz_offset(s).map(TimeZoneSetting::FixedOffset)
}

/// Parses a fixed UTC offset (`+HH:MM`/`-HH:MM`, e.g. `'+00:00'`, `'-08:00'`,
/// `'+05:30'`) into whole seconds.
fn parse_tz_offset(s: &str) -> Option<i32> {
    let bytes = s.as_bytes();
    if bytes.len() != 6 || bytes[3] != b':' {
        return None;
    }
    let sign = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let hh: i32 = s.get(1..3)?.parse().ok()?;
    let mm: i32 = s.get(4..6)?.parse().ok()?;
    Some(sign * (hh * 3600 + mm * 60))
}

/// Formats a whole-seconds UTC offset back into `+HH:MM`/`-HH:MM` — the
/// exact inverse of [`parse_tz_offset`], used by `@@time_zone` readback
/// ([`SessionState::sysvar`]) to echo back whatever was last `SET`.
pub(super) fn format_tz_offset(seconds: i32) -> String {
    let sign = if seconds < 0 { '-' } else { '+' };
    let abs = seconds.unsigned_abs();
    format!("{sign}{:02}:{:02}", abs / 3600, (abs % 3600) / 60)
}

/// Converts a validated nonnegative epoch setting into the raw clock shape
/// consumed by every current-time builtin. Rounding a fractional second can
/// carry into the next whole second, so normalize that boundary here rather
/// than letting an impossible nanosecond value leak to all consumers.
fn epoch_to_clock(epoch: f64) -> (i64, u32) {
    let mut seconds = epoch.floor() as i64;
    let mut nanos = ((epoch - epoch.floor()) * 1e9).round() as u32;
    if nanos == 1_000_000_000 {
        seconds += 1;
        nanos = 0;
    }
    (seconds, nanos)
}

/// Parses a `SET autocommit = value` value into a bool, covering every
/// form confirmed via `gorun`: the bare keyword `OFF` (parsed by
/// [`Parser::parse_set_stmt`] as `Expr::Column(["OFF"])` — a genuine TiDB
/// grammar quirk, see that function's own doc — checked BEFORE
/// evaluating, which would otherwise fail with an unknown-column error
/// against `NoColumns`; the bare keyword `ON`, asymmetrically, already
/// parses as `Expr::String("ON")` and so falls straight through to the
/// ordinary string-value arm below, no special case needed here), the
/// quoted string forms `'ON'`/`'OFF'`, a numeric-looking string like
/// `'0'` (parsed the same as a bare number), and any other constant
/// MySQL already treats as truthy/falsy (`1`/`0`, `TRUE`/`FALSE`, which
/// evaluate to `Datum::Int` directly).
fn string_text<'a>(value: &'a StringDatum, context: &'static str) -> Result<&'a str, ExecError> {
    value.as_utf8().map_err(|_| ExecError::Unsupported(context))
}

fn parse_bool_value(value: &Expr) -> Result<bool, ExecError> {
    if let Expr::Column(path) = value {
        if let [name] = path.as_slice() {
            if name.eq_ignore_ascii_case("off") {
                return Ok(false);
            }
        }
    }
    let unsupported = || ExecError::Unsupported("SET autocommit value");
    match eval(value)? {
        Datum::String(s) => {
            let text = string_text(&s, "SET autocommit value")?;
            if text.eq_ignore_ascii_case("on") {
                Ok(true)
            } else if text.eq_ignore_ascii_case("off") {
                Ok(false)
            } else {
                text.trim()
                    .parse::<f64>()
                    .map(|n| n != 0.0)
                    .map_err(|_| unsupported())
            }
        }
        Datum::Bytes(_) => Err(unsupported()),
        other => truthy_of(&other)?.ok_or_else(unsupported),
    }
}

/// Parses the bounded boolean vocabulary used by TiDB's typed system
/// variables. Unlike autocommit's historical loose conversion, these
/// variables are `TypeBool`: arbitrary nonzero numbers must not silently turn
/// them on. `OFF` is parsed as an identifier by the Go-compatible parser, so
/// recognize it before constant evaluation just as [`parse_bool_value`] does.
pub(super) fn parse_strict_bool_value(
    value: &Expr,
    context: &'static str,
) -> Result<bool, ExecError> {
    if let Expr::Column(path) = value {
        if let [name] = path.as_slice() {
            if name.eq_ignore_ascii_case("off") || name.eq_ignore_ascii_case("false") {
                return Ok(false);
            }
            if name.eq_ignore_ascii_case("on") || name.eq_ignore_ascii_case("true") {
                return Ok(true);
            }
        }
    }
    let invalid = || ExecError::Unsupported(context);
    match eval(value)? {
        Datum::Int(0) => Ok(false),
        Datum::Int(1) => Ok(true),
        Datum::String(s) => match string_text(&s, context)?.trim() {
            text if text.eq_ignore_ascii_case("off") || text.eq_ignore_ascii_case("false") => {
                Ok(false)
            }
            text if text.eq_ignore_ascii_case("on") || text.eq_ignore_ascii_case("true") => {
                Ok(true)
            }
            "0" => Ok(false),
            "1" => Ok(true),
            _ => Err(invalid()),
        },
        Datum::Bytes(_) => Err(invalid()),
        _ => Err(invalid()),
    }
}

/// Ports `SysVar.checkUInt64SystemVar` for `default_week_format`'s small,
/// fully representable `0..=7` domain (`pkg/sessionctx/variable/variable.go`).
/// Integer values beyond either endpoint clamp with a source warning; this
/// seed has no warning surface, so retain the normalized value. Fractions and
/// `NULL` are wrong-type errors rather than silently truncated.
fn parse_default_week_format(value: &Expr) -> Result<u8, ExecError> {
    let invalid = || ExecError::Unsupported("SET default_week_format value");
    let normalized = match eval(value)? {
        Datum::Int(value) => value,
        // Preserve TypeUnsigned's clamp without narrowing a runtime UInt
        // first: even MaxUint64 must normalize to this variable's source
        // maximum 7, never wrap into a negative signed value.
        Datum::UInt(value) => value.min(7) as i64,
        Datum::String(value) => {
            let value = string_text(&value, "SET default_week_format value")?;
            if value.starts_with('-') {
                value.parse::<i64>().map_err(|_| invalid())?
            } else {
                i64::try_from(value.parse::<u64>().map_err(|_| invalid())?).unwrap_or(i64::MAX)
            }
        }
        _ => return Err(invalid()),
    };
    Ok(normalized.clamp(0, 7) as u8)
}

/// Ports the TypeUnsigned `sql_select_limit` validator and session setter
/// (`pkg/sessionctx/variable/sysvar.go:1916-1922`). Negative integers clamp
/// to zero with a source warning; the seed has no warning channel, but keeps
/// the normalized unsigned cap. Positive values retain all 64 bits, including
/// the `u64::MAX` no-limit sentinel. Fractions, NULL, malformed strings, and
/// values beyond UInt64 fail before replacing the prior session value.
fn parse_sql_select_limit(value: &Expr) -> Result<u64, ExecError> {
    let invalid = || ExecError::Unsupported("SET sql_select_limit value");
    match eval(value)? {
        Datum::Int(value) => Ok(value.max(0) as u64),
        Datum::UInt(value) => Ok(value),
        Datum::String(value) => {
            let value = string_text(&value, "SET sql_select_limit value")?;
            if let Some(magnitude) = value.strip_prefix('-') {
                magnitude.parse::<u64>().map_err(|_| invalid())?;
                Ok(0)
            } else {
                value.parse::<u64>().map_err(|_| invalid())
            }
        }
        _ => Err(invalid()),
    }
}

/// Ports the signed `TypeInt` validation for `tidb_retry_limit`
/// (`pkg/sessionctx/variable/sysvar.go`). The source accepts the whole
/// signed-64-bit domain, including `-1` (the no-retry sentinel); values that
/// cannot be represented as an `int64`, fractions, and `NULL` are rejected
/// before replacing this session's existing setting.
fn parse_tidb_retry_limit(value: &Expr) -> Result<i64, ExecError> {
    let invalid = || ExecError::Unsupported("SET tidb_retry_limit value");
    match eval(value)? {
        Datum::Int(value) => Ok(value),
        Datum::UInt(value) => i64::try_from(value).map_err(|_| invalid()),
        Datum::String(value) => string_text(&value, "SET tidb_retry_limit value")?
            .parse::<i64>()
            .map_err(|_| invalid()),
        _ => Err(invalid()),
    }
}

/// Ports `SysVar.checkUInt64SystemVar` plus the `tidbOptPositiveInt32`
/// setter for `div_precision_increment` (`pkg/sessionctx/variable/sysvar.go`).
/// Its source type is unsigned even though the fully representable `0..=30`
/// range fits our signed value domain: negative signed integers clamp to zero,
/// positive values beyond 30 clamp to 30, while fractions, `NULL`, and
/// non-integer strings are genuine type errors. The source reports clamp
/// warnings; this executor has no warning result surface, so it retains the
/// normalized state only.
fn parse_div_precision_increment(value: &Expr) -> Result<DivPrecisionIncrement, ExecError> {
    let invalid = || ExecError::Unsupported("SET div_precision_increment value");
    let normalized = match eval(value)? {
        Datum::Int(value) => value,
        // Same TypeUnsigned rule as default_week_format: clamp while still
        // unsigned so a large literal cannot accidentally cross domains.
        Datum::UInt(value) => value.min(30) as i64,
        Datum::String(value) => {
            let value = string_text(&value, "SET div_precision_increment value")?;
            if value.starts_with('-') {
                value.parse::<i64>().map_err(|_| invalid())?
            } else {
                i64::try_from(value.parse::<u64>().map_err(|_| invalid())?).unwrap_or(i64::MAX)
            }
        }
        _ => return Err(invalid()),
    };
    Ok(DivPrecisionIncrement::new(normalized.clamp(0, 30) as u8))
}

/// Ports the typed `TypeInt` check and `tidbOptPositiveInt32` setter used by
/// TiDB's session-only `rand_seed1` and `rand_seed2` variables. Validation
/// clamps the representable source range to `0..=MaxInt32`; the setter then
/// treats zero and negative values as the source default zero. Fractional and
/// `NULL` values are type errors rather than silently truncated.
fn parse_rand_seed(value: &Expr) -> Result<u32, ExecError> {
    let invalid = || ExecError::Unsupported("SET rand_seed value");
    let normalized = match eval(value)? {
        Datum::Int(value) => value,
        // rand_seed1/2 are TypeInt, not TypeUnsigned. A UInt is necessarily
        // outside their source signed input domain, so reject it rather than
        // narrowing/wrapping it into an invented seed.
        Datum::UInt(_) => return Err(invalid()),
        Datum::String(value) => string_text(&value, "SET rand_seed value")?
            .parse::<i64>()
            .map_err(|_| invalid())?,
        _ => return Err(invalid()),
    }
    .clamp(0, i64::from(i32::MAX));
    Ok(normalized as u32)
}

/// Parses TiDB's `tidb_enable_noop_functions` enum. The Go sysvar accepts
/// only `OFF`, `ON`, and `WARN`; numeric boolean spellings normalize to the
/// first two through the same typed-variable conversion path.
fn parse_noop_functions_mode(value: &Expr) -> Result<NoopFunctionsMode, ExecError> {
    if let Expr::Column(path) = value {
        if let [name] = path.as_slice() {
            return match name.to_ascii_lowercase().as_str() {
                "off" => Ok(NoopFunctionsMode::Off),
                "on" => Ok(NoopFunctionsMode::On),
                "warn" => Ok(NoopFunctionsMode::Warn),
                _ => Err(ExecError::Unsupported(
                    "SET tidb_enable_noop_functions value",
                )),
            };
        }
    }
    match eval(value)? {
        Datum::Int(0) => Ok(NoopFunctionsMode::Off),
        Datum::Int(1) => Ok(NoopFunctionsMode::On),
        Datum::String(value) => match string_text(&value, "SET tidb_enable_noop_functions value")?
            .to_ascii_lowercase()
            .as_str()
        {
            "off" | "false" | "0" => Ok(NoopFunctionsMode::Off),
            "on" | "true" | "1" => Ok(NoopFunctionsMode::On),
            "warn" => Ok(NoopFunctionsMode::Warn),
            _ => Err(ExecError::Unsupported(
                "SET tidb_enable_noop_functions value",
            )),
        },
        _ => Err(ExecError::Unsupported(
            "SET tidb_enable_noop_functions value",
        )),
    }
}

/// Ports the TypeEnum validation and TiDBOptOnOffWarn setter for
/// tidb_multi_statement_mode (pkg/sessionctx/variable/sysvar.go). The client
/// protocol behavior behind ON/WARN is outside this one-statement executor;
/// this parser only owns the source-observable typed session-variable contract.
fn parse_multi_statement_mode(value: &Expr) -> Result<MultiStatementMode, ExecError> {
    let invalid = || ExecError::Unsupported("SET tidb_multi_statement_mode value");
    if let Expr::Column(path) = value {
        if let [name] = path.as_slice() {
            return match name.to_ascii_lowercase().as_str() {
                "off" => Ok(MultiStatementMode::Off),
                "on" => Ok(MultiStatementMode::On),
                "warn" => Ok(MultiStatementMode::Warn),
                _ => Err(invalid()),
            };
        }
    }
    match eval(value)? {
        Datum::Int(0) => Ok(MultiStatementMode::Off),
        Datum::Int(1) => Ok(MultiStatementMode::On),
        Datum::Int(2) => Ok(MultiStatementMode::Warn),
        // TypeEnum accepts its numeric spellings. Keep the three valid
        // unsigned numerals distinct and reject every other UInt without a
        // lossy signed conversion.
        Datum::UInt(0) => Ok(MultiStatementMode::Off),
        Datum::UInt(1) => Ok(MultiStatementMode::On),
        Datum::UInt(2) => Ok(MultiStatementMode::Warn),
        Datum::String(value) => match string_text(&value, "SET tidb_multi_statement_mode value")?
            .to_ascii_lowercase()
            .as_str()
        {
            "off" | "0" => Ok(MultiStatementMode::Off),
            "on" | "1" => Ok(MultiStatementMode::On),
            "warn" | "2" => Ok(MultiStatementMode::Warn),
            _ => Err(invalid()),
        },
        _ => Err(invalid()),
    }
}
