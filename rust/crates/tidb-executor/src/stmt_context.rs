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

//! The per-statement evaluation context, which is Go's `StatementContext`.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use tidb_datatype::Datum;
use tidb_expr::{Columns, ErrorLevel, MysqlRng};

use crate::mem_quota::{OomAction, StatementMemory};

/// Go `stmtctx.StatementContext`, in the part evaluation actually reads: the
/// warning buffer and the error levels that decide whether a tolerable
/// condition warns or fails the statement.
///
/// Go hands one `sctx` to every expression, and the buffer is mutated through
/// a shared reference; the handle here is cheap to clone for the same reason,
/// so every executor in a plan writes into the one buffer the statement
/// reports at the end.
///
/// DEFERRED (documented): the rest of `StatementContext` -- the other error
/// groups (truncation, bad NULL, no default), the statement-scoped clock, the
/// resource tracker and the runtime stats.
#[derive(Clone, Default)]
pub struct StmtContext {
    warnings: Rc<RefCell<Vec<(u16, String)>>>,
    division_by_zero: ErrorLevel,
    strict: bool,
    /// Go `SessionVars.SQLMode`'s three temporal bits; see
    /// [`crate::zero_date`]. They ride the context rather than being derived
    /// from `strict` because each answers a different question and TiDB's
    /// default mode happens to set two of them at once.
    date_modes: crate::zero_date::DateModes,
    current_db: Option<String>,
    version: Option<String>,
    current_user: Option<String>,
    login_user: Option<String>,
    /// The already-rendered `CURRENT_ROLE()` text; see `Columns::current_role`.
    current_role: Option<String>,
    connection_id: Option<u64>,
    /// Go `StatementContext`'s fixed statement time as
    /// `(utc_seconds, nanos, tz_offset_seconds)`: every `NOW()` in one
    /// statement reads the same instant.
    now: Option<(i64, u32, i32)>,
    time_zone: Option<tidb_expr::SessionTimeZone>,
    /// Go `SessionVars.Rng`: the SESSION-scoped generator unseeded `RAND()`
    /// advances, shared across every statement of one session. `None` is a
    /// context with no session behind it (a test, a DEFAULT expression
    /// folded at DDL time), where `RAND()` is unsupported rather than wrong.
    rand_session: Option<Rc<RefCell<MysqlRng>>>,
    /// Go `SessionVars.userVars`: the session's user variables, keyed
    /// lowercased. The SESSION owns the map and lends it here, because `@x :=
    /// expr` writes it MID-STATEMENT, once per row, and a later select-list
    /// item of the same row must read what the earlier one wrote -- so it
    /// cannot be a value copied in and out at the statement boundary. `None`
    /// is a context with no session behind it, where a user variable reads as
    /// NULL (Go's own answer for an unset one) and an assignment is dropped.
    user_vars: Option<Rc<RefCell<HashMap<String, Datum>>>>,
    /// Go `builtinRandSig`'s per-call `*mathutil.MysqlRng`: one generator per
    /// constant `RAND(N)` occurrence, created fresh for each STATEMENT (Go
    /// builds a new `builtinFunc` per plan) and advanced once per row by the
    /// evaluator, keyed by the call site's stable identity.
    rand_seeded: Rc<RefCell<HashMap<usize, MysqlRng>>>,
    /// Go `StatementContext.LastInsertID`/`LastInsertIDSet`: the id this
    /// statement publishes as `LAST_INSERT_ID()`.
    ///
    /// It rides the context rather than the statement's return value because
    /// Go publishes it the moment a row is ACCEPTED for insertion -- long
    /// before a deferred unique-key check can fail the statement -- so a
    /// statement that ends in an error still publishes. Returning it would
    /// make the failing case unreachable and force a second, error-shaped
    /// channel for exactly that case.
    last_insert_id: Rc<Cell<Option<u64>>>,
    /// Go `StmtCtx.PrevLastInsertID`: what the PRECEDING statement published,
    /// which is the value `LAST_INSERT_ID()` and `@@last_insert_id` report.
    /// It is a plain copy rather than a handle because a statement cannot
    /// change its own predecessor's publication.
    prev_last_insert_id: u64,
    /// Go `StmtCtx.PrevAffectedRows`: what `ROW_COUNT()` reports -- the
    /// preceding statement's affected rows, `-1` after a SELECT, `0`
    /// otherwise. The session derives it from that statement's class exactly
    /// as `ResetContextOfStmt` does.
    prev_row_count: i64,
    /// Go `StmtCtx.InsertID`: the explicit value a row gave the
    /// `AUTO_INCREMENT` column, which the OK packet falls back to.
    given_insert_id: Rc<Cell<u64>>,
    /// Whether `@@auto_increment_increment`/`@@auto_increment_offset` are at
    /// their defaults of 1. A statement that would have to honour a different
    /// step is refused; see [`StmtContext::auto_increment_step_is_default`].
    auto_increment_step_is_default: bool,
    /// Go `SessionVars.SQLMode.HasNoAutoValueOnZeroMode()`: whether an
    /// explicit `0` in an AUTO_INCREMENT column is a value rather than a
    /// request for the next id. A statement that would have to honour it is
    /// refused; see [`StmtContext::auto_increment_zero_is_explicit`].
    auto_increment_zero_is_explicit: bool,
    /// Go `SessionVars.SQLMode.HasOnlyFullGroupBy()`: whether a grouped query
    /// must justify every non-aggregated value it reports. `ONLY_FULL_GROUP_BY`
    /// is in TiDB's DEFAULT `sql_mode`, so a session leaves this on; a context
    /// with no session behind it (a test, a DDL-time fold) is permissive.
    only_full_group_by: bool,
    /// Go `SessionVars`'s `default_week_format` and `div_precision_increment`,
    /// which `EvalContext::GetDefaultWeekFormatMode` and
    /// `GetDivPrecisionIncrement` hand to `WEEK()` and to the `/` operator's
    /// result scale. The defaults here are the registry's own (`0` and `4`),
    /// so a context with no session behind it behaves like a stock one.
    default_week_format: i64,
    div_precision_increment: u32,
    /// Go `SessionVars.ForeignKeyChecks` (`@@foreign_key_checks`, ON by
    /// default): whether referential integrity is enforced at all. A context
    /// with no session behind it enforces, as a stock session does.
    foreign_key_checks: bool,

    /// Go `SessionVars.AllowRemoveAutoInc` (`@@tidb_allow_remove_auto_inc`,
    /// OFF by default), read by `ALTER TABLE ... MODIFY COLUMN`.
    allow_remove_auto_inc: bool,
    /// Go `SessionVars.CTEMaxRecursionDepth` (`@@cte_max_recursion_depth`,
    /// default `1000`): how many rounds a `WITH RECURSIVE` fixpoint may run
    /// before `ErrCTEMaxRecursionDepth`. Go's variable is signed and a
    /// non-positive value simply means "no round may run", which is what
    /// clamping to `0` here expresses.
    cte_max_recursion_depth: u64,
    /// The sequences reachable from this statement, keyed by lowercase
    /// `db.name`, plus the session's per-sequence `LASTVAL` record.
    ///
    /// Go reaches a sequence through the info schema at evaluation time. Here
    /// the session hands over a snapshot instead, because every allocator is
    /// `Arc`-shared: cloning the map costs one reference bump per sequence and
    /// consuming a value through the snapshot moves the SAME counter the
    /// catalog holds. That is what keeps a `NEXTVAL` from being undone by a
    /// rollback or by a staged catalog swap.
    sequences: Rc<SequenceSnapshot>,
    /// Go `SessionVars.MemTracker` + `StmtCtx.MemTracker`: this statement's
    /// memory budget, which `tidb_mem_quota_query` is the limit of.
    ///
    /// Every statement has one BY CONSTRUCTION -- [`StmtContext::new`] builds
    /// it, so an operator can always account and the quota can never be
    /// missing because a call site forgot it. A context with no session behind
    /// it gets the shipped defaults (1GiB, `CANCEL`); see
    /// [`StatementMemory::default`].
    memory: StatementMemory,
    /// The scanner flags of `@@sql_mode` -- Go's `Parser.SetSQLMode` input.
    ///
    /// Go parses a statement once, in `session.ParseSQL`, and hands the AST
    /// down; this tier hands the raw text down and RE-PARSES it in the DML and
    /// DDL entry points, so the mode has to arrive with the statement. It
    /// rides here rather than as a parameter on every entry because every
    /// entry already takes this context: one channel, no call site that can
    /// forget.
    ///
    /// The all-false default is TiDB's default `sql_mode` for these flags, so
    /// a context with no session behind it lexes exactly as before.
    sql_mode: tidb_parser::SqlMode,
}

/// The sequence state one statement can see: the allocators it may read and
/// the session's `LASTVAL` map, which every statement of a session shares.
#[derive(Debug, Default)]
pub struct SequenceSnapshot {
    /// Every sequence in the catalog, keyed by lowercase `db.name`.
    by_name: HashMap<String, crate::sequence::SequenceAllocator>,
    /// The schema an unqualified name resolves in.
    current_db: String,
    /// Go `SessionVars.SequenceState`: the last value THIS SESSION took from
    /// each sequence. Shared with the session, so a `NEXTVAL` in one statement
    /// is visible to a `LASTVAL` in the next.
    last_values: Rc<RefCell<HashMap<String, i64>>>,
}

impl SequenceSnapshot {
    /// A snapshot over `by_name`, resolving unqualified names in `current_db`
    /// and recording `LASTVAL` into the session's shared `last_values`.
    #[must_use]
    pub fn new(
        by_name: HashMap<String, crate::sequence::SequenceAllocator>,
        current_db: &str,
        last_values: Rc<RefCell<HashMap<String, i64>>>,
    ) -> Self {
        SequenceSnapshot {
            by_name,
            current_db: current_db.to_ascii_lowercase(),
            last_values,
        }
    }

    /// The key a written name path resolves to: `db.name`, lowercased, with an
    /// unqualified name taking the session's current database.
    fn key(&self, path: &[String]) -> String {
        match path {
            [name] => format!("{}.{}", self.current_db, name.to_ascii_lowercase()),
            [database, name] => format!(
                "{}.{}",
                database.to_ascii_lowercase(),
                name.to_ascii_lowercase()
            ),
            // A longer path cannot name a sequence, and joining it produces a
            // key nothing matches -- which is the 1146 below.
            other => other
                .iter()
                .map(|part| part.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join("."),
        }
    }

    /// The allocator `path` names, or 1146 -- which is what Go reports for a
    /// name that is not a sequence, whether it is absent or is a table.
    fn resolve(
        &self,
        path: &[String],
    ) -> Result<(String, &crate::sequence::SequenceAllocator), tidb_expr::EvalError> {
        let key = self.key(path);
        match self.by_name.get(&key) {
            Some(allocator) => Ok((key, allocator)),
            None => Err(tidb_expr::EvalError::Sequence(
                tidb_expr::SequenceEvalError::NotASequence(key),
            )),
        }
    }
}

impl StmtContext {
    /// Builds a context whose division-by-zero handling and strict flag are
    /// already resolved: the ONE place a [`StmtContext`] is built.
    ///
    /// The query and DML constructors differ in exactly these two fields and
    /// agreed on the other twenty-three by having been written out twice. A
    /// field added to the struct now has one place it must be named, so the two
    /// statement classes cannot silently drift apart -- which they would, in the
    /// direction of whichever literal was edited.
    fn new(division_by_zero: ErrorLevel, strict: bool) -> Self {
        Self {
            warnings: Rc::default(),
            division_by_zero,
            strict,
            date_modes: crate::zero_date::DateModes::default(),
            current_db: None,
            version: None,
            current_user: None,
            current_role: None,
            login_user: None,
            connection_id: None,
            now: None,
            time_zone: None,
            rand_session: None,
            user_vars: None,
            rand_seeded: Rc::default(),
            last_insert_id: Rc::default(),
            prev_last_insert_id: 0,
            prev_row_count: 0,
            given_insert_id: Rc::default(),
            auto_increment_step_is_default: true,
            auto_increment_zero_is_explicit: false,
            only_full_group_by: false,
            default_week_format: 0,
            foreign_key_checks: true,
            allow_remove_auto_inc: false,
            div_precision_increment: 4,
            cte_max_recursion_depth: 1000,
            sequences: Rc::default(),
            memory: StatementMemory::default(),
            sql_mode: tidb_parser::SqlMode::default(),
        }
    }

    /// Attaches the statement's memory budget, which the session builds from
    /// `@@tidb_mem_quota_query` and `@@tidb_mem_oom_action`.
    ///
    /// Go's `ResetContextOfStmt` does this per statement:
    /// `vars.MemTracker.SetBytesLimit(vars.MemQuotaQuery)` plus the action
    /// `vardef.OOMAction` selects.
    #[must_use]
    pub fn with_mem_quota(mut self, quota: i64, oom_action: OomAction) -> Self {
        self.memory = StatementMemory::new(quota, oom_action, self.connection_id.unwrap_or(0));
        self
    }

    /// This statement's memory budget; see [`StatementMemory`].
    #[must_use]
    pub fn statement_memory(&self) -> StatementMemory {
        self.memory.clone()
    }

    /// Attaches the session's scanner `sql_mode` flags, which every re-parse
    /// this statement performs must lex under.
    #[must_use]
    pub fn with_sql_mode(mut self, sql_mode: tidb_parser::SqlMode) -> Self {
        self.sql_mode = sql_mode;
        self
    }

    /// The scanner `sql_mode` flags for this statement's re-parses.
    #[must_use]
    pub fn sql_mode(&self) -> tidb_parser::SqlMode {
        self.sql_mode
    }

    /// Re-parses this statement's own text under its `sql_mode`. Every entry
    /// point that takes SQL text as a string goes through here, so a scanner
    /// flag cannot be honored by one tier and dropped by the next.
    pub(crate) fn parse(&self, sql: &str) -> Result<tidb_ast::Stmt, crate::DriverError> {
        tidb_parser::parse_with_sql_mode(sql, self.sql_mode)
            .map_err(|e| crate::DriverError::Parse(format!("{e:?}")))
    }

    /// A context for a query, where Go always warns on a zero divisor.
    #[must_use]
    pub fn for_query() -> Self {
        Self::new(ErrorLevel::Warn, true)
    }

    /// Sets the session's `default_week_format` and `div_precision_increment`.
    #[must_use]
    pub fn with_week_and_division_scale(
        mut self,
        default_week_format: i64,
        div_precision_increment: u32,
    ) -> Self {
        self.default_week_format = default_week_format;
        self.div_precision_increment = div_precision_increment;
        self
    }

    /// Attaches the sequences this statement may read. Without it, a
    /// `NEXTVAL` reports that it needs a session rather than silently
    /// answering NULL.
    #[must_use]
    pub fn with_sequences(mut self, sequences: Rc<SequenceSnapshot>) -> Self {
        self.sequences = sequences;
        self
    }

    /// Sets `@@cte_max_recursion_depth`; a non-positive session value clamps
    /// to `0`, which refuses the very first recursive round.
    #[must_use]
    pub fn with_cte_max_recursion_depth(mut self, depth: i64) -> Self {
        self.cte_max_recursion_depth = u64::try_from(depth).unwrap_or(0);
        self
    }

    /// See [`StmtContext::with_cte_max_recursion_depth`].
    #[must_use]
    pub fn cte_max_recursion_depth(&self) -> u64 {
        self.cte_max_recursion_depth
    }

    /// Sets whether `ONLY_FULL_GROUP_BY` is in effect, which a session reads
    /// off its `sql_mode`.
    #[must_use]
    pub fn with_only_full_group_by(mut self, only_full_group_by: bool) -> Self {
        self.only_full_group_by = only_full_group_by;
        self
    }

    /// Whether `ONLY_FULL_GROUP_BY` is in effect for this statement.
    #[must_use]
    pub fn only_full_group_by(&self) -> bool {
        self.only_full_group_by
    }

    /// Sets `@@foreign_key_checks` for this statement.
    #[must_use]
    pub fn with_foreign_key_checks(mut self, foreign_key_checks: bool) -> Self {
        self.foreign_key_checks = foreign_key_checks;
        self
    }

    /// Whether this statement enforces referential integrity.
    #[must_use]
    pub fn foreign_key_checks(&self) -> bool {
        self.foreign_key_checks
    }

    /// Sets `@@tidb_allow_remove_auto_inc` for this statement.
    #[must_use]
    pub fn with_allow_remove_auto_inc(mut self, allow_remove_auto_inc: bool) -> Self {
        self.allow_remove_auto_inc = allow_remove_auto_inc;
        self
    }

    /// Whether `ALTER TABLE ... MODIFY COLUMN` may drop AUTO_INCREMENT.
    ///
    /// Go's default is OFF, so a `MODIFY COLUMN` that leaves the option out
    /// of the new definition is refused (8200) rather than quietly turning
    /// the column into an ordinary one.
    #[must_use]
    pub fn allow_remove_auto_inc(&self) -> bool {
        self.allow_remove_auto_inc
    }

    /// Attaches the session state the builtins read: Go reads both from
    /// `SessionVars`, where `DATABASE()` is `CurrentDB` and `VERSION()` is
    /// the same string `@@version` reports.
    #[must_use]
    pub fn with_session_state(
        mut self,
        current_db: Option<String>,
        version: Option<String>,
    ) -> Self {
        self.current_db = current_db;
        self.version = version;
        self
    }

    /// Attaches the authenticated identity, which Go keeps on
    /// `SessionVars.User` in the two spellings its builtins report.
    #[must_use]
    pub fn with_user(mut self, current_user: Option<String>, login_user: Option<String>) -> Self {
        self.current_user = current_user;
        self.login_user = login_user;
        self
    }

    /// Attaches the rendered `CURRENT_ROLE()` text, which Go derives from
    /// `SessionVars.ActiveRoles`.
    #[must_use]
    pub fn with_current_role(mut self, current_role: Option<String>) -> Self {
        self.current_role = current_role;
        self
    }

    /// Attaches the connection identifier `CONNECTION_ID()` reports, which Go
    /// keeps on `SessionVars.ConnectionID`. `None` is a session with no
    /// connection identity, where the builtin answers NULL.
    #[must_use]
    pub fn with_connection_id(mut self, connection_id: Option<u64>) -> Self {
        self.connection_id = connection_id;
        self
    }

    /// Attaches the session-scoped generator unseeded `RAND()` reads and
    /// advances, which Go keeps on `SessionVars.Rng` for the session's whole
    /// lifetime (shared across statements, unlike constant `RAND(N)`'s
    /// per-statement generators).
    #[must_use]
    pub fn with_rand_session(mut self, rand_session: Rc<RefCell<MysqlRng>>) -> Self {
        self.rand_session = Some(rand_session);
        self
    }

    /// Attaches the session's user-variable map, which `@x` reads and `@x :=
    /// expr` writes THROUGH -- see the field's own doc for why this is a
    /// shared handle rather than a copy.
    #[must_use]
    pub fn with_user_vars(mut self, user_vars: Rc<RefCell<HashMap<String, Datum>>>) -> Self {
        self.user_vars = Some(user_vars);
        self
    }

    /// Fixes the statement's clock, which Go does once per statement so
    /// every `NOW()` in it agrees.
    #[must_use]
    pub fn with_clock(
        mut self,
        now: (i64, u32, i32),
        time_zone: tidb_expr::SessionTimeZone,
    ) -> Self {
        self.now = Some(now);
        self.time_zone = Some(time_zone);
        self
    }

    /// A context for `INSERT`/`UPDATE`/`DELETE`, where Go resolves the level
    /// from the SQL mode: without `ERROR_FOR_DIVISION_BY_ZERO` the condition
    /// is ignored entirely, a non-strict mode warns, and the default strict
    /// mode fails the statement.
    #[must_use]
    pub fn for_dml(error_for_division_by_zero: bool, strict: bool) -> Self {
        let level = if !error_for_division_by_zero {
            ErrorLevel::Ignore
        } else if strict {
            ErrorLevel::Error
        } else {
            ErrorLevel::Warn
        };
        Self::new(level, strict)
    }

    /// Whether the statement runs under a strict SQL mode, which decides
    /// whether a value that does not fit its column fails the statement.
    #[must_use]
    pub fn strict(&self) -> bool {
        self.strict
    }

    /// The statement's `time_zone`: Go's `SessionVars.Location()`, which
    /// `types.Context` carries and the storage codecs convert `TIMESTAMP`
    /// values with. Inherent so the storage seam can ask for it without
    /// pulling the whole [`tidb_expr::Columns`] trait into scope.
    #[must_use]
    pub fn session_zone(&self) -> tidb_expr::SessionTimeZone {
        <Self as tidb_expr::Columns>::time_zone(self)
    }

    /// Attaches `NO_ZERO_DATE`, `NO_ZERO_IN_DATE` and `ALLOW_INVALID_DATES`.
    #[must_use]
    pub fn with_date_modes(mut self, date_modes: crate::zero_date::DateModes) -> Self {
        self.date_modes = date_modes;
        self
    }

    /// The SQL mode's temporal bits; see [`crate::zero_date`].
    #[must_use]
    pub fn date_modes(&self) -> crate::zero_date::DateModes {
        self.date_modes
    }

    /// Go `StatementContext.TypeFlags` in the part conversion reads: a
    /// non-strict statement tolerates truncation instead of failing.
    #[must_use]
    pub fn conversion_flags(&self) -> tidb_datatype::ConversionFlags {
        if self.strict {
            tidb_datatype::STRICT_FLAGS
        } else {
            tidb_datatype::DEFAULT_STATEMENT_FLAGS
        }
    }

    /// Go `util.GetTypeFlagsForInsert` -- the flags a COLUMN WRITE converts
    /// under, which are NOT the ones an expression converts under.
    ///
    /// The one bit that differs from [`Self::conversion_flags`] is
    /// `FlagAllowNegativeToUnsigned`, which Go clears unconditionally for a
    /// write. It is set in `types.DefaultStmtFlags` only as a refactoring
    /// leftover (the source says so), and leaving it set on the write path
    /// made a negative value REINTERPRET as unsigned instead of overflowing:
    /// captured, `INSERT INTO t(a INT UNSIGNED) VALUES (-5)` under
    /// `sql_mode = ''` stores `0` in TiDB and stored `4294967295` here, while
    /// the strict mode's 1264 was already right -- a silently wrong VALUE
    /// with a correct-looking error path beside it.
    ///
    /// `WithIgnoreInvalidDateErr` and `WithIgnoreZeroInDate` come from
    /// [`crate::zero_date::write_date_flags`], which needs the mode bits
    /// [`Self::date_modes`] carries. NOT MODELLED, and named rather than
    /// guessed: `WithTruncateAsWarning`, which is applied a level up instead
    /// -- `cast_value_for_column` reads [`Self::strict`] to decide whether a
    /// conversion event is an error or a warning.
    #[must_use]
    pub fn write_conversion_flags(&self) -> tidb_datatype::ConversionFlags {
        crate::zero_date::write_date_flags(
            self.conversion_flags()
                .with_allow_negative_to_unsigned(false),
            self.date_modes,
            self.strict,
        )
    }

    /// Records a warning the driver rendered itself.
    pub fn append_warning_parts(&self, code: u16, message: &str) {
        self.append_warning(code, message);
    }

    /// Go `SessionVars.SetLastInsertID`: publishes the id `LAST_INSERT_ID()`
    /// reports after this statement. The first publication of a statement
    /// wins, as Go's statement-scoped `e.lastInsertID` does.
    pub fn publish_last_insert_id(&self, id: u64) {
        if self.last_insert_id.get().is_none() {
            self.last_insert_id.set(Some(id));
        }
    }

    /// The id this statement published, if any.
    #[must_use]
    pub fn published_last_insert_id(&self) -> Option<u64> {
        self.last_insert_id.get()
    }

    /// Attaches the session's own publication cell so `LAST_INSERT_ID(expr)`
    /// and an allocating INSERT write the SAME storage the session reads
    /// after the statement. Without it each context would own a private cell
    /// and only the branches that bother to read theirs back would publish.
    #[must_use]
    pub fn with_last_insert_id_channel(mut self, channel: Rc<Cell<Option<u64>>>) -> Self {
        self.last_insert_id = channel;
        self
    }

    /// Attaches what the PRECEDING statement published: Go's
    /// `StmtCtx.PrevLastInsertID` and `StmtCtx.PrevAffectedRows`, which are
    /// exactly what `LAST_INSERT_ID()` and `ROW_COUNT()` read.
    #[must_use]
    pub fn with_previous_statement(
        mut self,
        prev_last_insert_id: u64,
        prev_row_count: i64,
    ) -> Self {
        self.prev_last_insert_id = prev_last_insert_id;
        self.prev_row_count = prev_row_count;
        self
    }

    /// Go `StmtCtx.InsertID`: the explicit non-zero value a row GAVE the
    /// `AUTO_INCREMENT` column. Go overwrites it per row, so the LAST such
    /// value of the statement is the one that survives.
    pub fn record_given_insert_id(&self, id: u64) {
        self.given_insert_id.set(id);
    }

    /// The explicit auto-increment value this statement last saw, or 0.
    ///
    /// This is the OK packet's fallback: Go's `session.LastInsertID()` answers
    /// `StmtCtx.LastInsertID` when the statement PUBLISHED one and
    /// `StmtCtx.InsertID` otherwise, which is why
    /// `INSERT INTO t (id,v) VALUES (50,2)` reports 50 on the wire while
    /// `LAST_INSERT_ID()` -- which never follows an explicit value -- does not
    /// move (captured).
    #[must_use]
    pub fn given_insert_id(&self) -> u64 {
        self.given_insert_id.get()
    }

    /// Declares whether `@@auto_increment_increment` and
    /// `@@auto_increment_offset` are both 1.
    #[must_use]
    pub fn with_auto_increment_step_default(mut self, is_default: bool) -> Self {
        self.auto_increment_step_is_default = is_default;
        self
    }

    /// Declares whether `NO_AUTO_VALUE_ON_ZERO` is in the session's
    /// `sql_mode`.
    #[must_use]
    pub fn with_auto_increment_zero_explicit(mut self, is_explicit: bool) -> Self {
        self.auto_increment_zero_is_explicit = is_explicit;
        self
    }

    /// Whether an explicit `0` written to an AUTO_INCREMENT column must be
    /// STORED as zero, which is what `NO_AUTO_VALUE_ON_ZERO` asks for.
    ///
    /// This tier always allocates over a zero, so a statement under that mode
    /// is REFUSED rather than answered with a different row than Go stores.
    #[must_use]
    pub fn auto_increment_zero_is_explicit(&self) -> bool {
        self.auto_increment_zero_is_explicit
    }

    /// Whether the session's auto-increment step and offset are both 1.
    ///
    /// The allocator here hands out consecutive ids only, so a statement that
    /// would have to honour a different step is REFUSED rather than answered
    /// with the wrong ids.
    #[must_use]
    pub fn auto_increment_step_is_default(&self) -> bool {
        self.auto_increment_step_is_default
    }

    /// The warnings evaluation recorded, in the order they were raised.
    #[must_use]
    pub fn take_warnings(&self) -> Vec<(u16, String)> {
        std::mem::take(&mut self.warnings.borrow_mut())
    }
}

impl Columns for StmtContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn now(&self) -> Option<(i64, u32, i32)> {
        self.now
    }

    fn time_zone(&self) -> tidb_expr::SessionTimeZone {
        self.time_zone
            .clone()
            .unwrap_or(tidb_expr::SessionTimeZone::Fixed {
                name: "UTC".to_owned(),
                offset_secs: 0,
            })
    }

    fn current_user(&self) -> Option<String> {
        self.current_user.clone()
    }

    fn login_user(&self) -> Option<String> {
        self.login_user.clone()
    }

    fn current_role(&self) -> Option<String> {
        self.current_role.clone()
    }

    fn connection_id(&self) -> Option<u64> {
        self.connection_id
    }

    /// Go `SessionVars.GetUserVarVal`: names are case-insensitive, and an
    /// unset one is NULL rather than an error.
    fn get_uservar(&self, name: &str) -> Option<Datum> {
        let vars = self.user_vars.as_ref()?;
        vars.borrow().get(&name.to_ascii_lowercase()).cloned()
    }

    /// Go `SessionVars.SetUserVarVal`. A NULL value never reaches here -- the
    /// evaluator keeps Go's rule that `@x := NULL` leaves the variable alone.
    fn set_uservar(&self, name: &str, value: Datum) {
        if let Some(vars) = self.user_vars.as_ref() {
            vars.borrow_mut().insert(name.to_ascii_lowercase(), value);
        }
    }

    fn rand_next(&self) -> Option<f64> {
        self.rand_session.as_ref().map(|rng| rng.borrow_mut().gen())
    }

    fn rand_seeded_next(&self, key: usize, seed: i64) -> Option<f64> {
        Some(
            self.rand_seeded
                .borrow_mut()
                .entry(key)
                .or_insert_with(|| MysqlRng::new_with_seed(seed))
                .gen(),
        )
    }

    fn current_database(&self) -> Option<String> {
        self.current_db.clone()
    }

    fn sysvar(&self, _scope: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum> {
        // Only the variables a builtin reads are answered here; the session
        // resolves every other `@@var` before the driver sees the statement.
        if name.eq_ignore_ascii_case("version") {
            return self
                .version
                .as_ref()
                .map(|value| Datum::Bytes(value.clone().into_bytes()));
        }
        None
    }

    fn default_week_format(&self) -> i64 {
        self.default_week_format
    }

    fn div_precision_increment(&self) -> u32 {
        self.div_precision_increment
    }

    fn division_by_zero_level(&self) -> ErrorLevel {
        self.division_by_zero
    }

    fn append_warning(&self, code: u16, message: &str) {
        self.warnings.borrow_mut().push((code, message.to_owned()));
    }

    /// The same three mode bits the WRITE path reads from
    /// [`Self::write_conversion_flags`], handed to expression evaluation so a
    /// `CAST` to a temporal type answers under the session's SQL mode.
    fn date_modes(&self) -> tidb_datatype::DateModes {
        self.date_modes
    }

    fn row_count(&self) -> Option<i64> {
        Some(self.prev_row_count)
    }

    fn last_insert_id(&self) -> Option<u64> {
        Some(self.prev_last_insert_id)
    }

    /// Go `SessionVars.SetLastInsertID`, which `LAST_INSERT_ID(expr)` calls:
    /// it writes the same `StmtCtx.LastInsertID` an allocating INSERT writes,
    /// unconditionally -- the last such call of a statement wins, unlike the
    /// insert path's single first-row publication.
    fn set_last_insert_id(&self, value: u64) {
        self.last_insert_id.set(Some(value));
    }

    fn sequence_nextval(&self, path: &[String]) -> Result<Datum, tidb_expr::EvalError> {
        let (key, allocator) = self.sequences.resolve(path)?;
        let value = allocator.next_val().map_err(|_| {
            tidb_expr::EvalError::Sequence(tidb_expr::SequenceEvalError::RunOut(key.clone()))
        })?;
        // Go records the value in the SESSION's sequence state, which is what
        // `LASTVAL` reads back.
        self.sequences.last_values.borrow_mut().insert(key, value);
        Ok(Datum::Int(value))
    }

    fn sequence_lastval(&self, path: &[String]) -> Result<Datum, tidb_expr::EvalError> {
        let (key, _) = self.sequences.resolve(path)?;
        Ok(self
            .sequences
            .last_values
            .borrow()
            .get(&key)
            .copied()
            .map_or(Datum::Null, Datum::Int))
    }

    fn sequence_setval(&self, path: &[String], value: i64) -> Result<Datum, tidb_expr::EvalError> {
        let (_, allocator) = self.sequences.resolve(path)?;
        Ok(allocator.set_val(value).map_or(Datum::Null, Datum::Int))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_id_absent_by_default() {
        assert_eq!(StmtContext::for_query().connection_id(), None);
    }

    #[test]
    fn connection_id_reports_the_attached_value() {
        let ctx = StmtContext::for_query().with_connection_id(Some(7));
        assert_eq!(ctx.connection_id(), Some(7));
    }

    #[test]
    fn rand_next_is_unsupported_without_a_session_generator() {
        assert_eq!(StmtContext::for_query().rand_next(), None);
    }

    #[test]
    fn rand_next_advances_the_attached_session_generator() {
        let rng = Rc::new(RefCell::new(MysqlRng::new_with_seed(1)));
        let ctx = StmtContext::for_query().with_rand_session(Rc::clone(&rng));
        // Matches `MysqlRng::new_with_seed(1)`'s own pinned sequence
        // (`rng.rs`'s `source_seed_vectors_match`), read through the
        // `Columns` seam instead of the generator directly.
        assert_eq!(ctx.rand_next(), Some(0.40540353712197724));
        assert_eq!(ctx.rand_next(), Some(0.8716141803857071));
    }

    #[test]
    fn rand_seeded_next_is_one_generator_per_key_advancing_across_calls() {
        let ctx = StmtContext::for_query();
        assert_eq!(ctx.rand_seeded_next(1, 1), Some(0.40540353712197724));
        assert_eq!(ctx.rand_seeded_next(1, 1), Some(0.8716141803857071));
        // A different key is seeded independently, even with the same seed.
        assert_eq!(ctx.rand_seeded_next(2, 1), Some(0.40540353712197724));
    }
}
