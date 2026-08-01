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

//! What one statement is handed and what it hands back: Go's
//! `StatementContext`, built from this session's variables, and the `Prev*`
//! promotion `ResetContextOfStmt` performs at the statement boundary.
//!
//! Every session-state read a statement makes -- the clock, the time zone, the
//! sql_mode bits, the sequence names, the user variables, the last-insert-id
//! channel -- is funnelled through [`Session::statement_context`], so an
//! expression never reaches back into the session for anything.

use std::collections::HashMap;
use std::rc::Rc;

use crate::{DriverError, Session, StatementKind, StmtOutput};

impl Session {
    /// Go `timeutil.ParseTimeZone`: `SYSTEM` is the host zone, a named zone
    /// comes from the zone database, and a `+HH:MM`/`-HH:MM` string is a
    /// fixed offset bounded to `[-12:59, +14:00]`.
    ///
    /// An unparseable value falls back to the host zone rather than failing
    /// the statement, because this tier accepts the variable without
    /// validating it at SET time -- Go validates there instead, and that
    /// check is the deferred half of this port.
    fn session_time_zone(&self) -> tidb_executor::SessionTimeZone {
        use tidb_executor::SessionTimeZone;
        let written = self
            .vars
            .get_system("time_zone")
            .unwrap_or_else(|_| "SYSTEM".to_owned());
        if !written.eq_ignore_ascii_case("SYSTEM") {
            if let Ok(zone) = written.parse::<chrono_tz::Tz>() {
                return SessionTimeZone::Named(zone);
            }
            if let Some(rest) = written.strip_prefix(['+', '-']) {
                let negative = written.starts_with('-');
                let mut parts = rest.split(':');
                let hours: i32 = parts.next().unwrap_or_default().parse().unwrap_or(-1);
                let minutes: i32 = parts.next().unwrap_or("0").parse().unwrap_or(-1);
                if hours >= 0 && (0..60).contains(&minutes) {
                    let offset = hours * 3600 + minutes * 60;
                    let bounded = if negative {
                        offset <= 12 * 3600 + 59 * 60
                    } else {
                        offset <= 14 * 3600
                    };
                    if bounded {
                        return SessionTimeZone::Fixed {
                            name: written.clone(),
                            offset_secs: if negative { -offset } else { offset },
                        };
                    }
                }
            }
        }
        // SYSTEM: the host's own zone, which is what Go's SystemLocation is.
        let local = chrono::Local::now();
        SessionTimeZone::Fixed {
            name: "System".to_owned(),
            offset_secs: chrono::Offset::fix(local.offset()).local_minus_utc(),
        }
    }

    /// The instant every `NOW()` in one statement shares, which Go fixes on
    /// the statement context.
    ///
    /// Go `sessionexpr.getStmtTimestamp`: a `@@timestamp` left at its `0`
    /// default means the live clock, and any other value PINS the statement's
    /// whole time family (`NOW`, `CURDATE`, `UTC_TIMESTAMP`, ...) to that
    /// epoch instant. The split is `math.Modf` on a `float64`, kept here
    /// exactly: `SET timestamp = 1700000000.654321` really does land on
    /// 654320955ns, which is why the truncating readers report `.654320`
    /// while the rounding ones report `.654321`.
    fn statement_clock(&self, zone: &tidb_executor::SessionTimeZone) -> (i64, u32, i32) {
        use tidb_executor::SessionTimeZone;
        let pinned = self
            .vars
            .get_system("timestamp")
            .ok()
            .filter(|value| value != "0")
            .and_then(|value| value.parse::<f64>().ok());
        let utc = chrono::Utc::now();
        let (seconds, nanos) = match pinned {
            #[expect(clippy::cast_possible_truncation, reason = "Go's int64(seconds)")]
            #[expect(clippy::cast_sign_loss, reason = "@@timestamp's MinValue is 0")]
            Some(timestamp) => (
                timestamp.trunc() as i64,
                (timestamp.fract() * 1e9) as u32 % 1_000_000_000,
            ),
            None => (utc.timestamp(), utc.timestamp_subsec_nanos()),
        };
        let offset = match zone {
            SessionTimeZone::Fixed { offset_secs, .. } => *offset_secs,
            SessionTimeZone::Named(zone) => {
                use chrono::TimeZone;
                // A named zone's offset is a property of the INSTANT (DST), so
                // it has to be taken at the statement's own instant -- the
                // pinned one when `@@timestamp` fixes the clock.
                let at = chrono::DateTime::from_timestamp(seconds, nanos)
                    .unwrap_or(utc)
                    .naive_utc();
                chrono::Offset::fix(&zone.offset_from_utc_datetime(&at)).local_minus_utc()
            }
        };
        (seconds, nanos, offset)
    }

    /// The evaluation context for one statement, which is Go's
    /// `StatementContext`.
    ///
    /// The division-by-zero level is the only group modelled so far: Go warns
    /// for a query, and for a DML statement resolves it from `sql_mode` --
    /// without `ERROR_FOR_DIVISION_BY_ZERO` the condition is ignored, a
    /// non-strict mode warns, and the default strict mode fails the statement.
    /// The sequences a statement of this session may read, over the catalog it
    /// sees (the transaction's working copy inside `BEGIN`).
    ///
    /// Only the NAMES are snapshotted: the allocators are `Arc` handles, so
    /// consuming a value through one moves the counter the catalog holds. That
    /// is deliberate and matches Go, where `NEXTVAL` allocates in its own meta
    /// transaction -- see `with_statement_stage`'s note about a storage whose
    /// clone shares a handle rather than copying by value.
    fn sequence_snapshot(&self) -> Rc<tidb_executor::SequenceSnapshot> {
        let by_name = match &self.txn {
            Some(txn) => txn.working.sequence_allocators(),
            None => match self.catalog.lock() {
                Ok(catalog) => catalog.sequence_allocators(),
                // A poisoned catalog is reported by the statement itself; an
                // empty map here just makes every name unknown.
                Err(_) => HashMap::new(),
            },
        };
        Rc::new(tidb_executor::SequenceSnapshot::new(
            by_name,
            &self.current_db,
            Rc::clone(&self.sequence_last_values),
        ))
    }

    /// The scanner-facing half of `@@sql_mode`: the input Go hands
    /// `Parser.SetSQLMode`, read fresh at every parse so a `SET sql_mode`
    /// changes the statements AFTER it and no AST built before it.
    ///
    /// Go reads the mode once per statement, in `session.ParseSQL`
    /// (`pkg/session/session.go`), because Go parses once and passes the AST
    /// down. This tier re-parses the raw text in the executor tiers, so the
    /// mode has to travel with the statement; it travels on
    /// [`tidb_executor::StmtContext`], which every executor entry already
    /// takes, rather than on ~30 separate parameters.
    pub(crate) fn scanner_sql_mode(&self) -> tidb_parser::SqlMode {
        // `SET sql_mode = 'ANSI'` is stored already expanded (captured from
        // TiDB: `@@sql_mode` reads back
        // `REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI`),
        // so matching names against the stored text sees every flag a
        // combination brought in.
        scanner_sql_mode_of(
            &self
                .vars
                .get_system("sql_mode")
                .unwrap_or_default()
                .to_ascii_uppercase(),
        )
    }

    /// Parses one statement of THIS session, under the `sql_mode` in force
    /// right now. Go's `session.ParseSQL` is the same single door; every
    /// session-tier parse goes through here so no call site decides on its own
    /// that a scanner flag does not apply to it.
    /// [`Self::parse`] for a front end outside this crate, so a caller that
    /// asks this session several parse-only questions about one statement can
    /// pay for the parse once and hand the tree to each. The `sql_mode` used
    /// is this session's, which is the whole point: a front end must not lex
    /// with a mode of its own.
    pub fn parse_statement(&self, sql: &str) -> Result<tidb_ast::Stmt, DriverError> {
        self.parse(sql)
    }

    pub(crate) fn parse(&self, sql: &str) -> Result<tidb_ast::Stmt, DriverError> {
        tidb_parser::parse_with_sql_mode(sql, self.scanner_sql_mode())
            .map_err(|e| DriverError::Parse(format!("{e:?}")))
    }

    pub(crate) fn statement_context(&self, is_dml: bool) -> tidb_executor::StmtContext {
        // Go hands the same `SessionVars` to every expression, which is where
        // `DATABASE()` and `VERSION()` read from.
        let current_db = if self.current_db.is_empty() {
            None
        } else {
            Some(self.current_db.clone())
        };
        let version = self.vars.get_system("version").ok();
        let zone = self.session_time_zone();
        let clock = self.statement_clock(&zone);
        let mode = self
            .vars
            .get_system("sql_mode")
            .unwrap_or_default()
            .to_ascii_uppercase();
        let has = |flag: &str| mode.split(',').any(|part| part.trim() == flag);
        // Go `GetDefaultWeekFormatMode` treats an unset or empty value as
        // "0"; `GetDivPrecisionIncrement` falls back to the default of 4.
        let week_format = self
            .vars
            .get_system("default_week_format")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let div_scale = self
            .vars
            .get_system("div_precision_increment")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(4);
        // Go `SessionVars.CTEMaxRecursionDepth`, the `WITH RECURSIVE` round
        // bound; the registry default is 1000.
        let cte_depth = self
            .vars
            .get_system("cte_max_recursion_depth")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(1000);
        // Go `ResetContextOfStmt`: the statement's memory budget is
        // `@@tidb_mem_quota_query` under the action `@@tidb_mem_oom_action`
        // selects. An unreadable quota falls back to the shipped 1GiB rather
        // than to "unlimited", so a registry hiccup cannot silently remove
        // the protection.
        let mem_quota = self
            .vars
            .get_system("tidb_mem_quota_query")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_util::memory::DEF_MEM_QUOTA_QUERY);
        // `tidb_mem_oom_action` has GLOBAL scope only, so its live value is
        // the shared table's, not any session copy -- `get_system` would only
        // ever hand back the registry default.
        let oom_action = tidb_executor::OomAction::parse(
            &self
                .vars
                .get_global("tidb_mem_oom_action")
                .unwrap_or_default(),
        );
        // The SAME three bits on both branches: a query reads them for
        // `CAST(... AS DATE/DATETIME)`, a DML statement reads them for the
        // column write. They used to be attached only below, which left every
        // read with the all-false default -- and made `NO_ZERO_DATE` silently
        // inoperative on the read path.
        let date_modes = tidb_datatype::DateModes {
            no_zero_date: has("NO_ZERO_DATE"),
            no_zero_in_date: has("NO_ZERO_IN_DATE"),
            allow_invalid_dates: has("ALLOW_INVALID_DATES"),
        };
        if !is_dml {
            return tidb_executor::StmtContext::for_query()
                .with_date_modes(date_modes)
                .with_cte_max_recursion_depth(cte_depth)
                .with_only_full_group_by(has("ONLY_FULL_GROUP_BY"))
                .with_session_state(current_db, version)
                .with_user(self.current_user.clone(), self.login_user.clone())
                .with_current_role(self.current_user.as_ref().map(|_| self.current_role_text()))
                .with_connection_id(self.connection_id)
                .with_mem_quota(mem_quota, oom_action)
                .with_rand_session(Rc::clone(&self.rand))
                .with_last_insert_id_channel(Rc::clone(&self.published_last_insert_id))
                .with_retry_auto_ids(Rc::clone(&self.retry_auto_ids))
                .with_user_vars(Rc::clone(&self.user_vars))
                .with_previous_statement(self.last_insert_id, self.prev_row_count)
                .with_week_and_division_scale(week_format, div_scale)
                .with_sequences(self.sequence_snapshot())
                .with_sql_mode(scanner_sql_mode_of(&mode))
                .with_clock(clock, zone);
        }
        let (increment, offset) = self.auto_increment_step();
        tidb_executor::StmtContext::for_dml(
            has("ERROR_FOR_DIVISION_BY_ZERO"),
            has("STRICT_TRANS_TABLES") || has("STRICT_ALL_TABLES"),
        )
        .with_date_modes(date_modes)
        .with_only_full_group_by(has("ONLY_FULL_GROUP_BY"))
        .with_session_state(current_db, version)
        .with_user(self.current_user.clone(), self.login_user.clone())
        .with_current_role(self.current_user.as_ref().map(|_| self.current_role_text()))
        .with_connection_id(self.connection_id)
        .with_mem_quota(mem_quota, oom_action)
        .with_rand_session(Rc::clone(&self.rand))
        .with_last_insert_id_channel(Rc::clone(&self.published_last_insert_id))
        .with_retry_auto_ids(Rc::clone(&self.retry_auto_ids))
        .with_user_vars(Rc::clone(&self.user_vars))
        .with_previous_statement(self.last_insert_id, self.prev_row_count)
        .with_week_and_division_scale(week_format, div_scale)
        .with_sequences(self.sequence_snapshot())
        .with_clock(clock, zone)
        .with_sql_mode(scanner_sql_mode_of(&mode))
        .with_auto_increment_step(increment, offset)
        .with_auto_increment_zero_explicit(has("NO_AUTO_VALUE_ON_ZERO"))
        .with_foreign_key_checks(self.foreign_key_checks())
        .with_allow_remove_auto_inc(self.allow_remove_auto_inc())
        .with_cte_max_recursion_depth(cte_depth)
    }

    /// Go `SessionVars.ForeignKeyChecks`, read off `@@foreign_key_checks`.
    /// The registry stores a boolean as `ON`/`OFF`, and an unreadable value
    /// falls back to the ON default rather than silently disabling the
    /// checks.
    pub(crate) fn foreign_key_checks(&self) -> bool {
        !matches!(
            self.vars.get_system("foreign_key_checks").as_deref(),
            Ok("OFF") | Ok("off") | Ok("0")
        )
    }

    /// Go `SessionVars.AllowRemoveAutoInc`, read off
    /// `@@tidb_allow_remove_auto_inc`. The default is OFF, and unlike
    /// `foreign_key_checks` the safe fallback for an unreadable value is OFF:
    /// dropping AUTO_INCREMENT is the destructive direction.
    pub(crate) fn allow_remove_auto_inc(&self) -> bool {
        matches!(
            self.vars
                .get_system("tidb_allow_remove_auto_inc")
                .as_deref(),
            Ok("ON") | Ok("on") | Ok("1")
        )
    }

    /// Go `vardef.EnableCheckConstraint`, which is a process-wide atomic that
    /// `SetGlobal` writes: the variable is GLOBAL-scope only, so the value a
    /// statement sees is the global one, not a session copy. The registry
    /// defaults it to OFF, and unlike `foreign_key_checks` the safe fallback
    /// for an unreadable value is OFF -- that is what a stock TiDB does and
    /// the only mode this engine models.
    pub(crate) fn enable_check_constraint(&self) -> bool {
        matches!(
            self.vars
                .get_global("tidb_enable_check_constraint")
                .as_deref(),
            Ok("ON") | Ok("on") | Ok("1")
        )
    }

    /// Go `SessionVars.EnableClusteredIndex`, fed to `BuildTableInfo` through
    /// `metabuild.WithClusteredIndexDefMode` (`pkg/ddl/metabuild.go`).
    ///
    /// The variable is `SESSION | GLOBAL` and an ENUM of `OFF`/`ON`/`INT_ONLY`
    /// -- not a boolean -- so it is read with the session's own value and
    /// converted by Go's own `TiDBOptEnableClustered`, which maps anything
    /// that is neither `ON` nor `OFF` (including an unreadable value) onto
    /// `INT_ONLY`. The registered default is `ON`.
    pub(crate) fn clustered_index_mode(&self) -> tidb_vardef::modes::ClusteredIndexDefMode {
        // `check_enum` stores the canonical `OFF`/`ON`/`INT_ONLY` spelling
        // whatever the user typed, so this compares against it exactly as Go
        // does rather than re-normalizing here.
        match self.vars.get_system("tidb_enable_clustered_index") {
            Ok(value) => tidb_vardef::modes::tidb_opt_enable_clustered(&value),
            Err(_) => tidb_vardef::modes::ClusteredIndexDefMode(
                tidb_vardef::defaults::DEF_TIDB_ENABLE_CLUSTERED_INDEX,
            ),
        }
    }

    /// Go `SessionVars.AutoIncrementIncrement` / `AutoIncrementOffset`, which
    /// put an allocated id on the `offset + k * increment` progression.
    ///
    /// Both are `TypeUnsigned` sysvars validated into `[1, 65535]`, so an
    /// unreadable or out-of-range value falls back to the default of 1 --
    /// never to 0, which would divide by zero in the seek.
    pub(crate) fn auto_increment_step(&self) -> (u64, u64) {
        let read = |name: &str| {
            self.vars
                .get_system(name)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| (1..=65535).contains(value))
                .unwrap_or(1)
        };
        (
            read("auto_increment_increment"),
            read("auto_increment_offset"),
        )
    }

    /// Go `ResetContextOfStmt`'s `Prev*` promotion, run at the statement
    /// boundary: what the statement just published becomes what the next one
    /// reads.
    ///
    /// This is the ONE place either value moves. `LAST_INSERT_ID()`,
    /// `@@last_insert_id`, `@@identity` and `ROW_COUNT()` all read the fields
    /// it writes, and the OK packet reads
    /// [`Session::statement_insert_id`]'s own fallback off the same
    /// publication -- so the function and the wire can differ only where Go
    /// itself makes them differ.
    pub(crate) fn publish_statement_status(&mut self, result: &Result<StmtOutput, DriverError>) {
        // The publication outlives a failing statement, exactly as Go's
        // `StmtCtx.LastInsertID` does: `SELECT LAST_INSERT_ID(17), bad()`
        // fails and still moves the id (captured).
        if let Some(published) = self.published_last_insert_id.get() {
            self.last_insert_id = published;
        }
        self.prev_row_count = match self.statement_kind {
            StatementKind::Select => -1,
            // Go reads `StmtCtx.AffectedRows()`, which a failed statement
            // leaves at whatever it managed to apply -- 0 for a statement
            // that never reached a row.
            StatementKind::Dml => match result {
                Ok(StmtOutput::Affected(rows)) => i64::try_from(*rows).unwrap_or(i64::MAX),
                _ => 0,
            },
            StatementKind::Other => 0,
        };
    }
}

/// The scanner flags Go's `Parser.SetSQLMode` consults, read off an
/// already-uppercased, already-expanded `@@sql_mode` text.
///
/// `PIPES_AS_CONCAT` is deliberately absent: the lexer has no field for it
/// and the parser has no `precConcat` level, so setting it here would be a
/// claim this tier cannot honor. See `rust/docs/operations/sql-mode-coverage.md`.
pub(crate) fn scanner_sql_mode_of(mode: &str) -> tidb_parser::SqlMode {
    let has = |flag: &str| mode.split(',').any(|part| part.trim() == flag);
    tidb_parser::SqlMode {
        real_as_float: has("REAL_AS_FLOAT"),
        no_backslash_escapes: has("NO_BACKSLASH_ESCAPES"),
        ansi_quotes: has("ANSI_QUOTES"),
        high_not_precedence: has("HIGH_NOT_PRECEDENCE"),
        ignore_space: has("IGNORE_SPACE"),
    }
}
