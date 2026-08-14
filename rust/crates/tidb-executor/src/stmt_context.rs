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
use tidb_distsql::{WarningCollector, WarningLevel};
use tidb_expr::{Columns, ErrorLevel, MysqlRng};
pub use tidb_util::context::MAX_WARNING_COUNT;

use crate::error_context::{ErrGroup, Level, LevelMap};
use crate::mem_quota::{OomAction, StatementMemory};
use crate::statement_pushdown::{push_down_flags, PushDownFlagsInput, StatementKind};
use crate::DriverError;
use std::sync::Arc;
use tidb_util::disk::SpillStorage;

/// Which of Go's mutually exclusive `StatementContext` statement-kind
/// booleans this statement sets (`InInsertStmt`, `InUpdateStmt`/
/// `InDeleteStmt`, `InSelectStmt`, `InLoadDataStmt`).
///
/// Go keeps four independent flags and `PushDownFlags` reads them in a fixed
/// precedence; `ResetContextOfStmt` only ever sets one, so one value says the
/// same thing without the unreachable combinations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StatementClass {
    /// A statement whose `ResetContextOfStmt` arm sets no kind flag -- `SHOW`,
    /// `SET`, DDL, and the `default` arm.
    #[default]
    Other,
    /// `*ast.InsertStmt`, including `INSERT ... SELECT` and `REPLACE`.
    Insert,
    /// `*ast.UpdateStmt` and `*ast.DeleteStmt`, which share one TiKV bit.
    UpdateOrDelete,
    /// `*ast.SelectStmt` and `*ast.SetOprStmt`.
    Select,
    /// `*ast.LoadDataStmt`.
    LoadData,
}

/// Go `variable.RetryInfo`'s `autoIncrementIDs`
/// (`pkg/sessionctx/variable/session.go:110-117`): the AUTO_INCREMENT ids a
/// statement assigned, kept so that a statement RUN AGAIN after a write
/// conflict writes the ids it already picked instead of fresh ones.
///
/// The gap a fresh allocation would leave is not the problem -- TiDB leaves
/// auto-increment gaps of its own and they are legal. `LAST_INSERT_ID()` is:
/// a client that inserts a row and then reads `LAST_INSERT_ID()` to key a
/// child row would be handed a value naming a row that was never written,
/// silently, and only under contention.
///
/// Go carries ONE list plus a cursor it rewinds per attempt
/// (`pkg/session/session.go:1197`), consuming from the front and appending
/// whatever it had to allocate beyond the end
/// (`pkg/executor/insert_common.go:968-1021`). Two lists say the same thing
/// without the flag Go needs to tell "record" from "replay" apart -- and
/// without Go's one incoherent case, where a replay that assigns FEWER ids
/// than the attempt before leaves a stale id stranded mid-list and shifts
/// every later row's id on the attempt after that.
///
/// # Which ids go in, and which come back out
///
/// Every id a row is given is RECORDED, including one the row supplied
/// itself; but only a row that needs an id CONSUMES from the cursor. The two
/// rules are not symmetric and it is tempting to make them so -- both
/// symmetric readings are wrong, and each is wrong by exactly one id per
/// explicit id in the batch. Go's `INSERT ... VALUES` arm is
/// `lazyAdjustAutoIncrementDatum` (`insertRows` sets `lazyFillAutoID`
/// unconditionally, `insert_common.go:237`); it records in the explicit arm at
/// `:902` and in the allocating arm at `:946`, but `continue`s past the
/// explicit arm at `:894-903` without ever reaching the consume loop at
/// `:909-921`.
///
/// Measured on TiDB over `mockstore`, with `mockCommitRetryForAutoIncID`
/// failing the first commit of an autocommit statement:
///
/// ```text
/// create table t (id int primary key auto_increment, v int)
///
/// -- mixed batch: the explicit id is recorded but not consumed, so the
/// -- NULL row's cursor read returns the EXPLICIT id, not the allocated one
/// insert into t (v) values (10)                  -> rows=[[1 10]] last_insert_id=1
/// insert into t (id, v) values (1,11),(NULL,20)
///   on duplicate key update v = 11               -> rows=[[1 11]] last_insert_id=2
///
/// -- control, no explicit id in the batch: exact reuse, no drift
/// insert into t2 values (NULL,11),(NULL,20)      -> rows=[[1 11] [2 20]] last_insert_id=1
/// ```
///
/// The control is what keeps the rule honest: a port that consumed the cursor
/// for explicit ids too passes it and still drifts on the mixed batch, and a
/// port that recorded only allocated ids passes it as well.
#[derive(Debug, Default)]
struct RetryIdQueue {
    previous: Vec<u64>,
    taken: usize,
    current: Vec<u64>,
}

impl RetryIdQueue {
    fn begin_attempt(&mut self) {
        self.previous = std::mem::take(&mut self.current);
        self.taken = 0;
    }

    fn clean(&mut self) {
        self.previous.clear();
        self.current.clear();
        self.taken = 0;
    }

    fn reuse(&mut self) -> Option<u64> {
        let id = self.previous.get(self.taken).copied()?;
        self.taken += 1;
        Some(id)
    }

    fn record(&mut self, id: u64) {
        self.current.push(id);
    }
}

#[derive(Debug, Default)]
/// Session retry state for AUTO_INCREMENT and AUTO_RANDOM assignments.
pub struct RetryAutoIds {
    increment: RetryIdQueue,
    random: RetryIdQueue,
}

impl RetryAutoIds {
    /// Go's `RetryInfo.ResetOffset` (`pkg/session/session.go:1197`), called
    /// once per replay pass: the attempt that is starting inherits the ids the
    /// attempt that just failed assigned.
    pub fn begin_attempt(&mut self) {
        self.increment.begin_attempt();
        self.random.begin_attempt();
    }

    /// Go's `RetryInfo.Clean`, called when the statement is over however it
    /// ended. Ids must never outlive their statement: the next statement's
    /// rows are not these rows.
    pub fn clean(&mut self) {
        self.increment.clean();
        self.random.clean();
    }

    /// Go's `GetCurrAutoIncrementID`: the id the previous attempt gave the
    /// next row, if it got that far. `None` means allocate -- which covers
    /// both the first attempt and a replay that needs MORE ids than the
    /// attempt before it did.
    pub fn reuse(&mut self) -> Option<u64> {
        self.increment.reuse()
    }

    /// Go's `AddAutoIncrementID`: records the id a row was actually given.
    pub fn record(&mut self, id: u64) {
        self.increment.record(id);
    }

    /// Go `GetCurrAutoRandomID` for the next auto-random row.
    pub fn reuse_random(&mut self) -> Option<u64> {
        self.random.reuse()
    }

    /// Go `AddAutoRandomID` for one completed row assignment.
    pub fn record_random(&mut self, id: u64) {
        self.random.record(id);
    }
}

/// Session-lived shard selection for row IDs. The random source is
/// intentionally Rust-native; the observable contract is that one shard is
/// retained for `@@tidb_shard_allocate_step` IDs and then replaced.
#[derive(Debug, Default)]
pub struct RowIdShardGenerator {
    step: u64,
    remaining: u64,
    current: u64,
}

impl RowIdShardGenerator {
    fn next(&mut self, step: u64, count: u64) -> u64 {
        let step = step.max(1);
        if self.step != step {
            self.step = step;
            self.remaining = 0;
        }
        if self.remaining == 0 {
            self.current = u64::from(tidb_util::fastrand::uint32());
            self.remaining = step;
        }
        self.remaining = self.remaining.saturating_sub(count);
        self.current
    }
}

/// Go `stmtctx.StatementContext`, in the part evaluation actually reads: the
/// warning buffer and the error levels that decide whether a tolerable
/// condition warns or fails the statement.
///
/// Go hands one `sctx` to every expression, and the buffer is mutated through
/// a shared reference; the handle here is cheap to clone for the same reason,
/// so every executor in a plan writes into the one buffer the statement
/// reports at the end.
///
/// DEFERRED (documented): the rest of `StatementContext` -- the remaining
/// error groups (bad NULL, no default), the resource tracker and runtime
/// stats.
#[derive(Clone, Default)]
pub struct StmtContext {
    /// Go's `StaticWarnHandler` entries: a LEVEL, a code and a message.
    ///
    /// The level is not decoration. Go reaches this one buffer through three
    /// doors -- `AppendWarning`, `AppendError` and `AppendNote` -- and an
    /// `IF EXISTS` that swallowed an error files it as a `Note`, which
    /// `SHOW WARNINGS` prints in its `Level` column. Without the level here
    /// every executor-tier note would arrive at the session as a `Warning`.
    warnings: Rc<RefCell<Vec<(WarningLevel, u16, String)>>>,
    division_by_zero: ErrorLevel,
    /// Go's truncation flags (`IgnoreTruncateErr` / `TruncateAsWarning`)
    /// collapsed to the level `types.Context.HandleTruncate` acts on. It is
    /// NOT derivable from `strict`: a SELECT warns in every mode, while a
    /// strict write fails, and both statement classes build this struct.
    truncate: ErrorLevel,
    strict: bool,
    /// Go `ast.InsertStmt/UpdateStmt/DeleteStmt.IgnoreErr`, the statement's
    /// own `IGNORE` modifier.
    ///
    /// Go folds it into every value-level decision as `!strictSQLMode ||
    /// ignoreErr` (`util.GetTypeFlagsForInsert`, `ResetContextOfStmt`'s
    /// `*ast.InsertStmt` arm), so [`Self::for_dml`] resolves it INTO `strict`
    /// and no reader of `strict` has to know about it. It is kept beside the
    /// result for the one rule that is not a plain `||`: `ErrGroupBadNull`
    /// promotes a SINGLE-ROW insert to an error even without a strict mode,
    /// and `IGNORE` overrides that promotion too.
    ignore_err: bool,
    /// Go `SessionVars.SQLMode`'s three temporal bits; see
    /// [`crate::zero_date`]. They ride the context rather than being derived
    /// from `strict` because each answers a different question and TiDB's
    /// default mode happens to set two of them at once.
    date_modes: crate::zero_date::DateModes,
    current_db: Option<String>,
    version: Option<String>,
    /// Immutable process identity returned by `TIDB_VERSION()`.
    tidb_info: Option<String>,
    current_user: Option<String>,
    login_user: Option<String>,
    /// The small set of GLOBAL system-variable values expression builtins
    /// read during this statement.
    global_sysvars: Rc<HashMap<String, String>>,
    /// The already-rendered `CURRENT_ROLE()` text; see `Columns::current_role`.
    current_role: Option<String>,
    connection_id: Option<u64>,
    /// Go `StatementContext`'s fixed statement time as
    /// `(utc_seconds, nanos, tz_offset_seconds)`: every `NOW()` in one
    /// statement reads the same instant.
    now: Option<(i64, u32, i32)>,
    sysdate_is_now: bool,
    time_zone: Option<tidb_expr::SessionTimeZone>,
    /// Go `SessionVars.Rng`: the SESSION-scoped generator unseeded `RAND()`
    /// advances, shared across every statement of one session. `None` is a
    /// context with no session behind it (a test, a DEFAULT expression
    /// folded at DDL time), where `RAND()` is unsupported rather than wrong.
    rand_session: Option<Rc<MysqlRng>>,
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
    /// The ids a previous attempt at this same statement already assigned; see
    /// [`RetryAutoIds`]. It is a session-lived handle rather than statement
    /// state because the retry loop that rewinds it sits ABOVE the statement:
    /// each attempt builds its own context, and the ids are the one thing that
    /// must cross between them. Timestamps must not -- a replay re-reads at a
    /// new one, which is what keeps the lost update closed.
    retry_auto_ids: Rc<RefCell<RetryAutoIds>>,
    row_id_shards: Rc<RefCell<RowIdShardGenerator>>,
    /// Go `table.getIncrementAndOffset`'s inputs: `@@auto_increment_increment`
    /// and `@@auto_increment_offset`, which put the allocated ids on an
    /// arithmetic progression. See [`StmtContext::auto_increment_step`].
    auto_increment_step: (u64, u64),
    /// Go `SessionVars.SQLMode.HasNoAutoValueOnZeroMode()`: whether an
    /// explicit `0` in an AUTO_INCREMENT column is a value rather than a
    /// request for the next id. A statement that would have to honour it is
    /// refused; see [`StmtContext::auto_increment_zero_is_explicit`].
    auto_increment_zero_is_explicit: bool,
    allow_auto_random_explicit_insert: bool,
    shard_allocate_step: u64,
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
    /// Go `SessionVars.TiDBOptJoinReorderThreshold`
    /// (`@@tidb_opt_join_reorder_threshold`, default `0`): the largest join
    /// group the DP join-reorder solver is allowed to enumerate. At the
    /// default NO group qualifies, so a stock session never reorders --
    /// see [`crate::driver::join_reorder`].
    join_reorder_threshold: i32,
    /// The statement snapshot of Go `SessionVars.OptimizerFixControl`.
    ///
    /// Keeping the parsed map on the context makes planner decisions consume
    /// the same value that the session writer validated, including a
    /// statement-local `SET_VAR(tidb_opt_fix_control=...)` overlay.
    optimizer_fix_control: tidb_planner::fix_control::OptimizerFixControl,
    /// The statement snapshot of every session value read by cost model v2.
    optimizer_cost_env: tidb_planner::candidate_cost::CostEnv,
    /// Resolved `tidb_hash_join_concurrency` for physical hash-join costing.
    hash_join_concurrency: f64,
    /// Go `SessionVars.TiDBOptJoinReorderThroughProj`
    /// (`@@tidb_opt_join_reorder_through_proj`, default `OFF`): whether
    /// `extractJoinGroup` may look THROUGH a `Projection` sitting on a join
    /// and take that join's own leaves into the group -- see
    /// [`crate::driver::join_reorder`]'s inlining section.
    join_reorder_through_proj: bool,
    /// Go `SessionVars.TiDBOptJoinReorderThroughSel`
    /// (`@@tidb_opt_join_reorder_through_sel`, default `OFF`): whether
    /// `extractJoinGroupImpl` may look THROUGH a `Selection` sitting on a
    /// join -- see [`crate::driver::join_reorder`]'s barrier section.
    join_reorder_through_sel: bool,
    /// Go `SessionVars.EnableOuterJoinReorder`
    /// (`@@tidb_enable_outer_join_reorder`, default `ON`): whether an outer
    /// join carrying equal conditions may join the reorder group at all.
    outer_join_reorder: bool,
    /// Go `SessionVars.EnableIndexMerge`: whether automatic IndexMerge paths
    /// participate in this statement's costed access-path selection.
    index_merge: bool,
    /// Go `SessionVars.PartitionPruneMode == Static`: see
    /// [`StmtContext::static_partition_prune`].
    static_partition_prune: bool,
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
    /// `NO_UNSIGNED_SUBTRACTION` changes expression typing and evaluation,
    /// not scanning, so it is kept beside the lexer's compact mode.
    no_unsigned_subtraction: bool,
    /// The implicit escape byte `LIKE` receives when the syntax omitted an
    /// `ESCAPE` clause. Go derives this from both `sql_mode` and
    /// `tidb_enable_no_backslash_escapes_in_like` while building expressions.
    like_default_escape: u8,
    /// The validated statement snapshot of `@@block_encryption_mode`.
    block_encryption_mode: tidb_expr::BlockEncryptionMode,
    /// `@@max_allowed_packet`, which the result-sizing string builtins read.
    max_allowed_packet: u64,
    /// `@@group_concat_max_len`, the BYTE budget `GROUP_CONCAT` truncates its
    /// joined buffer to (Go `baseGroupConcat4String.maxLen`).
    group_concat_max_len: u64,
    /// `@@tidb_mem_quota_apply_cache`, captured once for this statement so
    /// every Apply operator uses the same session-visible cache budget.
    apply_cache_capacity: i64,
    /// Which `ResetContextOfStmt` arm built this context; see
    /// [`StatementClass`]. It is an INPUT to [`StmtContext::push_down_flags`],
    /// which is why it rides the context rather than being re-derived at the
    /// request builder: a coprocessor request built for the read half of
    /// `INSERT ... SELECT` must say `InInsertStmt`, and only the statement
    /// knows that.
    statement_class: StatementClass,
    /// The warnings TiKV reported for THIS statement's coprocessor requests.
    ///
    /// It is an `Arc` sink rather than the `Rc` buffer beside it because a
    /// coprocessor response is decoded on a scan thread, and it is the
    /// SESSION'S sink rather than a fresh one because a warning nobody can
    /// read is not a warning: `SHOW WARNINGS` and the OK packet's count both
    /// read what [`StmtContext::take_warnings`] hands back. Go reaches the
    /// same place through `DistSQLContext.WarnHandler`, which is the session's
    /// `StatementContext` itself.
    cop_warnings: WarningCollector,
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
    /// The query and DML constructors differ in exactly these three fields and
    /// agreed on the other twenty-three by having been written out twice. A
    /// field added to the struct now has one place it must be named, so the two
    /// statement classes cannot silently drift apart -- which they would, in the
    /// direction of whichever literal was edited.
    fn new(
        division_by_zero: ErrorLevel,
        truncate: ErrorLevel,
        strict: bool,
        ignore_err: bool,
    ) -> Self {
        Self {
            warnings: Rc::default(),
            division_by_zero,
            truncate,
            strict,
            ignore_err,
            date_modes: crate::zero_date::DateModes::default(),
            current_db: None,
            version: None,
            tidb_info: None,
            current_user: None,
            current_role: None,
            login_user: None,
            global_sysvars: Rc::default(),
            connection_id: None,
            now: None,
            sysdate_is_now: false,
            time_zone: None,
            rand_session: None,
            user_vars: None,
            rand_seeded: Rc::default(),
            last_insert_id: Rc::default(),
            prev_last_insert_id: 0,
            prev_row_count: 0,
            given_insert_id: Rc::default(),
            retry_auto_ids: Rc::default(),
            row_id_shards: Rc::default(),
            auto_increment_step: (1, 1),
            auto_increment_zero_is_explicit: false,
            allow_auto_random_explicit_insert: false,
            shard_allocate_step: i64::MAX as u64,
            only_full_group_by: false,
            default_week_format: 0,
            foreign_key_checks: true,
            allow_remove_auto_inc: false,
            div_precision_increment: 4,
            cte_max_recursion_depth: 1000,
            join_reorder_threshold: tidb_vardef::defaults::DEF_TIDB_OPT_JOIN_REORDER_THRESHOLD
                as i32,
            optimizer_fix_control: tidb_planner::fix_control::OptimizerFixControl::default(),
            optimizer_cost_env: tidb_planner::candidate_cost::CostEnv::default(),
            hash_join_concurrency: tidb_vardef::defaults::DEF_EXECUTOR_CONCURRENCY as f64,
            // Go `vardef.DefTiDBOptJoinReorderThroughProj`.
            join_reorder_through_proj: false,
            // Go `vardef.DefTiDBOptJoinReorderThroughSel`.
            join_reorder_through_sel: false,
            // Go `vardef.DefTiDBEnableOuterJoinReorder = true`.
            outer_join_reorder: true,
            // Go `vardef.DefTiDBEnableIndexMerge = true`.
            index_merge: true,
            // Go's shipped `tidb_partition_prune_mode` is `dynamic`.
            static_partition_prune: false,
            sequences: Rc::default(),
            memory: StatementMemory::default(),
            sql_mode: tidb_parser::SqlMode::default(),
            no_unsigned_subtraction: false,
            like_default_escape: b'\\',
            block_encryption_mode: tidb_expr::BlockEncryptionMode::default(),
            // Go `vardef.DefMaxAllowedPacket`, the value a default server runs
            // with and the one the `Columns` trait default already used.
            max_allowed_packet: 64 << 20,
            group_concat_max_len: 1024,
            apply_cache_capacity: tidb_vardef::defaults::DEF_TIDB_MEM_QUOTA_APPLY_CACHE,
            statement_class: StatementClass::Other,
            cop_warnings: WarningCollector::new(),
        }
    }

    /// Records which `ResetContextOfStmt` arm this statement took; see
    /// [`StatementClass`].
    #[must_use]
    pub fn with_statement_class(mut self, class: StatementClass) -> Self {
        self.statement_class = class;
        self
    }

    /// Which `ResetContextOfStmt` arm built this context.
    #[must_use]
    pub const fn statement_class(&self) -> StatementClass {
        self.statement_class
    }

    /// The sink a coprocessor request must be given so the warnings TiKV
    /// reports reach `SHOW WARNINGS` and the OK packet's count.
    ///
    /// Cloning shares the handler, not the vector (see [`WarningCollector`]),
    /// so a scan thread that outlives the request still appends into this
    /// statement's buffer.
    #[must_use]
    pub fn cop_warning_sink(&self) -> WarningCollector {
        self.cop_warnings.clone()
    }

    /// Go `StatementContext.PushDownFlags()`: the `DAGRequest.flags` field
    /// this statement's coprocessor requests must carry.
    ///
    /// The literal `0` this replaced is TiKV's STRICTEST branch -- no
    /// truncation tolerated, no zero-in-date tolerated, division by zero an
    /// error -- so a `SELECT` that TiDB answers with a 1292 warning made the
    /// region fail the whole request instead.
    ///
    /// The INPUTS are threaded, not the output: the same code answers `482`
    /// for a plain `SELECT` (Go's `*ast.SelectStmt` arm writes
    /// `WithTruncateAsWarning(true)` and `WithIgnoreZeroInDate(true)` as
    /// literals, with no SQL-mode input, so no mode can make a read fail) and
    /// `8` for a strict `INSERT ... SELECT` under the default `sql_mode`.
    ///
    /// `INSERT IGNORE`'s `stmt.IgnoreErr` IS modelled: [`Self::for_dml`]
    /// resolves it into `strict`, so the tolerance it buys reaches the
    /// coprocessor through the same three bits a non-strict mode does.
    ///
    /// NOT MODELLED, and named rather than guessed:
    /// `StatementContext.InRestrictedSQL` (bit 11), which only internal SQL
    /// and auto-analyze set.
    #[must_use]
    pub fn push_down_flags(&self) -> u64 {
        // `push_down_flags` reads exactly three conversion bits, so a default
        // map with those three written is the whole input rather than an
        // approximation of one.
        let type_flags = tidb_datatype::ConversionFlags::default()
            .with_ignore_truncate_err(self.truncate == ErrorLevel::Ignore)
            .with_truncate_as_warning(self.truncate == ErrorLevel::Warn)
            .with_ignore_zero_in_date_err(self.ignore_zero_in_date());
        let mut err_levels = LevelMap::strict();
        err_levels[ErrGroup::DividedByZero] = match self.division_by_zero {
            ErrorLevel::Error => Level::Error,
            ErrorLevel::Warn => Level::Warn,
            ErrorLevel::Ignore => Level::Ignore,
        };
        push_down_flags(PushDownFlagsInput {
            type_flags,
            err_levels,
            statement_kind: match self.statement_class {
                StatementClass::Insert => StatementKind::Insert,
                StatementClass::UpdateOrDelete => StatementKind::UpdateOrDelete,
                StatementClass::Select => StatementKind::Select,
                StatementClass::Other | StatementClass::LoadData => StatementKind::None,
            },
            in_load_data_stmt: self.statement_class == StatementClass::LoadData,
            in_restricted_sql: false,
        })
    }

    /// Go's `Flags.IgnoreZeroInDate` for this statement.
    ///
    /// A `SELECT` gets the literal `true` its `ResetContextOfStmt` arm
    /// writes; every other class gets `GetTypeFlagsForInsert`'s mode rule,
    /// which [`crate::zero_date::write_date_flags`] already owns.
    fn ignore_zero_in_date(&self) -> bool {
        if self.statement_class == StatementClass::Select {
            return true;
        }
        crate::zero_date::write_date_flags(
            tidb_datatype::ConversionFlags::default(),
            self.date_modes,
            self.strict,
        )
        .ignore_zero_in_date_err()
    }

    /// Attaches the statement's memory budget, which the session builds from
    /// `@@tidb_mem_quota_query` and `@@tidb_mem_oom_action`.
    ///
    /// Go's `ResetContextOfStmt` does this per statement:
    /// `vars.MemTracker.SetBytesLimit(vars.MemQuotaQuery)` plus the action
    /// `vardef.OOMAction` selects.
    #[must_use]
    pub fn with_mem_quota(mut self, quota: i64, oom_action: OomAction) -> Self {
        let tmp_storage_on_oom = self.memory.tmp_storage_on_oom();
        let spill_storage = self.memory.configured_spill_storage();
        self.memory = StatementMemory::new(quota, oom_action, self.connection_id.unwrap_or(0))
            .with_tmp_storage_on_oom(tmp_storage_on_oom);
        if let Some(storage) = spill_storage {
            self.memory = self.memory.with_spill_storage(storage);
        }
        self
    }

    /// Attaches the persistent session roots and one fresh statement child
    /// supplied by the session lifecycle. This is the source-shaped path for
    /// production sessions; [`Self::with_mem_quota`] remains the standalone
    /// constructor used by focused executor tests.
    #[must_use]
    pub fn with_statement_memory(mut self, memory: StatementMemory) -> Self {
        self.memory = memory;
        self
    }

    /// Attaches `@@tidb_enable_tmp_storage_on_oom` (Go
    /// `vardef.EnableTmpStorageOnOOM`, default ON), which decides whether an
    /// operator that exceeds the quota spills to disk or fails with 8175.
    ///
    /// MUST be applied after [`StmtContext::with_mem_quota`], which rebuilds
    /// the budget.
    #[must_use]
    pub fn with_tmp_storage_on_oom(mut self, enabled: bool) -> Self {
        self.memory = self.memory.with_tmp_storage_on_oom(enabled);
        self
    }

    /// Installs the process-wide spill authority captured and validated at
    /// server startup. Every physical spill store built from this statement
    /// receives the same immutable path, encryption, and quota policy.
    #[must_use]
    pub fn with_spill_storage(mut self, storage: Arc<SpillStorage>) -> Self {
        self.memory = self.memory.with_spill_storage(storage);
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

    /// Attaches `NO_UNSIGNED_SUBTRACTION` for expression build and runtime.
    #[must_use]
    pub fn with_no_unsigned_subtraction(mut self, enabled: bool) -> Self {
        self.no_unsigned_subtraction = enabled;
        self
    }

    /// Attaches the statement-time implicit `LIKE` escape selected by the
    /// session's SQL mode and `tidb_enable_no_backslash_escapes_in_like`.
    #[must_use]
    pub fn with_like_default_escape(mut self, escape: u8) -> Self {
        self.like_default_escape = escape;
        self
    }

    /// Attaches the AES mode selected by this session for the statement.
    #[must_use]
    pub fn with_block_encryption_mode(mut self, mode: tidb_expr::BlockEncryptionMode) -> Self {
        self.block_encryption_mode = mode;
        self
    }

    /// The scanner `sql_mode` flags for this statement's re-parses.
    #[must_use]
    pub fn sql_mode(&self) -> tidb_parser::SqlMode {
        self.sql_mode
    }

    /// Whether subtraction must use a signed result domain.
    #[must_use]
    pub fn no_unsigned_subtraction(&self) -> bool {
        self.no_unsigned_subtraction
    }

    /// The implicit `LIKE` escape for expressions built in this statement.
    #[must_use]
    pub fn like_default_escape(&self) -> u8 {
        self.like_default_escape
    }

    /// Re-parses this statement's own text under its `sql_mode`. Every entry
    /// point that takes SQL text as a string goes through here, so a scanner
    /// flag cannot be honored by one tier and dropped by the next.
    pub(crate) fn parse(&self, sql: &str) -> Result<tidb_ast::Stmt, crate::DriverError> {
        tidb_parser::parse_with_sql_mode(sql, self.sql_mode)
            .map_err(|e| crate::DriverError::Parse(format!("{e:?}")))
    }

    /// A context for a query, where Go always warns on a zero divisor and
    /// always warns on a truncating conversion -- `ResetContextOfStmt`'s
    /// `*ast.SelectStmt` arm writes `WithTruncateAsWarning(true)` as a
    /// literal, with no SQL mode input, so no mode can make a read fail.
    #[must_use]
    pub fn for_query() -> Self {
        Self::new(ErrorLevel::Warn, ErrorLevel::Warn, true, false)
            .with_statement_class(StatementClass::Select)
    }

    /// Sets whether this statement runs under a strict SQL mode.
    ///
    /// [`Self::for_dml`] derives this from the mode already, because a DML
    /// statement's error LEVELS are derived from it in the same breath.
    /// [`Self::for_query`] cannot: its levels are the literals Go writes for a
    /// read, so it had to pass SOMETHING for `strict` and passed `true`.
    ///
    /// That placeholder is only sound while nothing reads it. DDL takes this
    /// same non-DML context, and Go's DDL checks do read the session's mode --
    /// `checkColumnDefaultValue` calls `SQLMode.HasStrictMode()` to decide
    /// whether an empty BLOB/TEXT/JSON default is 1101 or a warning. Reading
    /// the placeholder made `SET sql_mode=''` inoperative for DDL, so the
    /// session sets the real value here instead of DDL growing a second,
    /// parallel channel for the same fact.
    #[must_use]
    pub fn with_strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    /// Sets the session's `max_allowed_packet`.
    ///
    /// Go `EvalContext.GetMaxAllowedPacket` is what every result-sizing string
    /// builtin captures at build time (`builtinSpaceSig.maxAllowedPacket` and
    /// friends). Without this the trait default -- `DefMaxAllowedPacket`, 64
    /// MiB -- stood for every statement, so `SET GLOBAL max_allowed_packet`
    /// moved the wire limit and left `SPACE`/`REPEAT`/`RPAD` sizing results
    /// against the shipped default.
    #[must_use]
    pub fn with_max_allowed_packet(mut self, max_allowed_packet: u64) -> Self {
        self.max_allowed_packet = max_allowed_packet;
        self
    }

    /// Sets the session's `group_concat_max_len`.
    ///
    /// Go `SessionVars.GroupConcatMaxLen`, which the aggregate builder copies
    /// into every `GROUP_CONCAT` it builds. The default is Go's
    /// `DefGroupConcatMaxLen`, 1024.
    #[must_use]
    pub fn with_group_concat_max_len(mut self, group_concat_max_len: u64) -> Self {
        self.group_concat_max_len = group_concat_max_len;
        self
    }

    /// Sets the statement snapshot of `@@tidb_mem_quota_apply_cache`.
    #[must_use]
    pub fn with_apply_cache_capacity(mut self, capacity: i64) -> Self {
        self.apply_cache_capacity = capacity;
        self
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

    /// Sets `@@tidb_opt_join_reorder_threshold` for this statement.
    #[must_use]
    pub fn with_join_reorder_threshold(mut self, threshold: i32) -> Self {
        self.join_reorder_threshold = threshold;
        self
    }

    /// Attaches the validated statement snapshot of
    /// `@@tidb_opt_fix_control`.
    #[must_use]
    pub fn with_optimizer_fix_control(
        mut self,
        control: tidb_planner::fix_control::OptimizerFixControl,
    ) -> Self {
        self.optimizer_fix_control = control;
        self
    }

    /// The statement's parsed optimizer-fix controls.
    #[must_use]
    pub const fn optimizer_fix_control(&self) -> &tidb_planner::fix_control::OptimizerFixControl {
        &self.optimizer_fix_control
    }

    /// Attaches the resolved statement snapshot used by cost model v2.
    #[must_use]
    pub fn with_optimizer_cost_env(
        mut self,
        env: tidb_planner::candidate_cost::CostEnv,
        hash_join_concurrency: f64,
    ) -> Self {
        self.optimizer_cost_env = env;
        self.hash_join_concurrency = hash_join_concurrency;
        self
    }

    /// The statement's cost-model-v2 environment.
    #[must_use]
    pub const fn optimizer_cost_env(&self) -> &tidb_planner::candidate_cost::CostEnv {
        &self.optimizer_cost_env
    }

    /// Resolved `tidb_hash_join_concurrency`.
    #[must_use]
    pub const fn hash_join_concurrency(&self) -> f64 {
        self.hash_join_concurrency
    }

    /// Go `SessionVars.TiDBOptJoinReorderThreshold`. Non-positive -- and `0`
    /// is the shipped default -- means the DP join-reorder solver never runs.
    #[must_use]
    pub fn join_reorder_threshold(&self) -> i32 {
        self.join_reorder_threshold
    }

    /// Sets `@@tidb_opt_join_reorder_through_proj` for this statement.
    #[must_use]
    pub fn with_join_reorder_through_proj(mut self, through: bool) -> Self {
        self.join_reorder_through_proj = through;
        self
    }

    /// Go `SessionVars.TiDBOptJoinReorderThroughProj`. `OFF` is the shipped
    /// default, under which a `Projection` over a join is an atomic group
    /// leaf and the relations below it never join the group.
    #[must_use]
    pub fn join_reorder_through_proj(&self) -> bool {
        self.join_reorder_through_proj
    }

    /// Sets `@@tidb_opt_join_reorder_through_sel` for this statement.
    #[must_use]
    pub fn with_join_reorder_through_sel(mut self, through: bool) -> Self {
        self.join_reorder_through_sel = through;
        self
    }

    /// Go `SessionVars.TiDBOptJoinReorderThroughSel`. `OFF` is the shipped
    /// default, under which a `LogicalSelection` between two joins is a
    /// barrier `extractJoinGroupImpl` stops at
    /// (`rule_join_reorder.go:67-80`), so the relations below it form their
    /// own group.
    #[must_use]
    pub fn join_reorder_through_sel(&self) -> bool {
        self.join_reorder_through_sel
    }

    /// Sets `@@tidb_enable_outer_join_reorder` for this statement.
    #[must_use]
    pub fn with_outer_join_reorder(mut self, enabled: bool) -> Self {
        self.outer_join_reorder = enabled;
        self
    }

    /// Go `SessionVars.EnableOuterJoinReorder`, whose shipped default is ON
    /// (`vardef.DefTiDBEnableOuterJoinReorder = true`). OFF puts back the
    /// stop `extractJoinGroupImpl` spells right after its own list: "If the
    /// session var is set to off, we will still reject the outer joins."
    #[must_use]
    pub fn outer_join_reorder(&self) -> bool {
        self.outer_join_reorder
    }

    /// Sets `@@tidb_enable_index_merge` for this statement.
    #[must_use]
    pub fn with_index_merge(mut self, enabled: bool) -> Self {
        self.index_merge = enabled;
        self
    }

    /// Go `SessionVars.GetEnableIndexMerge`. This controls automatic paths;
    /// an applicable `USE_INDEX_MERGE` hint remains explicit unless
    /// `NO_INDEX_MERGE` overrides it.
    #[must_use]
    pub fn index_merge(&self) -> bool {
        self.index_merge
    }

    /// Sets `@@tidb_partition_prune_mode` for this statement, as the one bit
    /// the planner reads off it.
    #[must_use]
    pub fn with_static_partition_prune(mut self, static_prune: bool) -> Self {
        self.static_partition_prune = static_prune;
        self
    }

    /// Go `SessionVars.IsDynamicPartitionPruneEnabled()`, inverted:
    /// `@@tidb_partition_prune_mode = 'static'`.
    ///
    /// The mode does not change WHICH rows a partitioned read returns -- both
    /// modes read the surviving partitions and nothing else. It changes the
    /// PLAN SHAPE Go builds and prints: `static` runs
    /// `rule_partition_processor` and replaces the one `DataSource` with a
    /// `PartitionUnion` over one child per surviving partition, each naming
    /// its own partition in its access object; `dynamic` keeps one
    /// `DataSource` reading all of them and names the set once, on the
    /// reader above (`partition:all`). This tier's read is the same either
    /// way, so this is consulted only where the shape is PRINTED.
    #[must_use]
    pub fn static_partition_prune(&self) -> bool {
        self.static_partition_prune
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
        tidb_info: Option<String>,
    ) -> Self {
        self.current_db = current_db;
        self.version = version;
        self.tidb_info = tidb_info;
        self
    }

    /// Length of the server identity this statement returns from
    /// `TIDB_VERSION()`.
    #[must_use]
    pub fn tidb_info_len(&self) -> usize {
        self.tidb_info.as_ref().map_or_else(
            || {
                tidb_util::printer::get_tidb_info(
                    &tidb_util::versioninfo::VersionInfo::build_default(),
                )
                .len()
            },
            String::len,
        )
    }

    /// Attaches the authenticated identity, which Go keeps on
    /// `SessionVars.User` in the two spellings its builtins report.
    #[must_use]
    pub fn with_user(mut self, current_user: Option<String>, login_user: Option<String>) -> Self {
        self.current_user = current_user;
        self.login_user = login_user;
        self
    }

    /// Attaches a statement snapshot of the GLOBAL variables used by
    /// expression builtins.
    #[must_use]
    pub fn with_global_sysvars(mut self, values: HashMap<String, String>) -> Self {
        self.global_sysvars = Rc::new(values);
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
    pub fn with_rand_session(mut self, rand_session: Rc<MysqlRng>) -> Self {
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

    /// Selects the statement clock for `SYSDATE`, matching
    /// `@@tidb_sysdate_is_now`.
    #[must_use]
    pub fn with_sysdate_is_now(mut self, enabled: bool) -> Self {
        self.sysdate_is_now = enabled;
        self
    }

    /// Attaches the session's time zone without inventing a statement clock.
    ///
    /// Metadata-only statement paths such as cluster DDL need the zone for
    /// literal `TIMESTAMP` normalization but never evaluate `NOW()`. Keeping
    /// those two facts separate avoids manufacturing a clock reading merely
    /// to carry the session's real zone.
    #[must_use]
    pub fn with_time_zone(mut self, time_zone: tidb_expr::SessionTimeZone) -> Self {
        self.time_zone = Some(time_zone);
        self
    }

    /// A context for `INSERT`/`UPDATE`/`DELETE`, where Go resolves the level
    /// from the SQL mode: without `ERROR_FOR_DIVISION_BY_ZERO` the condition
    /// is ignored entirely, a non-strict mode warns, and the default strict
    /// mode fails the statement.
    ///
    /// Truncation follows `util.GetTypeFlagsForInsert`'s
    /// `WithTruncateAsWarning(!strictSQLMode || ignoreErr)`: a strict write
    /// fails on a value that lost information, a permissive one warns. That
    /// is the one place this differs from [`Self::for_query`], and the reason
    /// the level is carried rather than decided at the conversion site.
    ///
    /// `ignore_err` is the statement's own `IGNORE` modifier, and Go writes it
    /// into EVERY value-level rule as the second disjunct of
    /// `!strictSQLMode || ignoreErr` -- `GetTypeFlagsForInsert`'s truncation
    /// and zero-in-date bits, and `ResetContextOfStmt`'s bad-NULL, no-default
    /// and division-by-zero levels alike. Resolving it into `strict` here is
    /// therefore not an approximation but the same expression written once:
    /// captured from TiDB, `INSERT IGNORE INTO t(a BIT(1)) VALUES (-1)` under
    /// the DEFAULT strict mode stores the clamped `1` with warning 1406,
    /// exactly as the same plain `INSERT` does under `sql_mode = ''`.
    #[must_use]
    pub fn for_dml(error_for_division_by_zero: bool, strict: bool, ignore_err: bool) -> Self {
        let strict = strict && !ignore_err;
        let level = if !error_for_division_by_zero {
            ErrorLevel::Ignore
        } else if strict {
            ErrorLevel::Error
        } else {
            ErrorLevel::Warn
        };
        let truncate = if strict {
            ErrorLevel::Error
        } else {
            ErrorLevel::Warn
        };
        Self::new(level, truncate, strict, ignore_err)
    }

    /// Whether the statement runs under a strict SQL mode, which decides
    /// whether a value that does not fit its column fails the statement.
    ///
    /// An `IGNORE` statement is already resolved to `false` here; see
    /// [`Self::for_dml`].
    #[must_use]
    pub fn strict(&self) -> bool {
        self.strict
    }

    /// Go `stmt.IgnoreErr`. Read only where Go's rule is NOT the plain
    /// `!strictSQLMode || ignoreErr` that [`Self::for_dml`] already resolved:
    /// `ErrGroupBadNull`, whose `(strictSQLMode || isSingleInsert)` promotes a
    /// one-row insert to an error in every mode, and which `IGNORE` overrides.
    #[must_use]
    pub fn ignore_err(&self) -> bool {
        self.ignore_err
    }

    /// The statement's `time_zone`: Go's `SessionVars.Location()`, which
    /// `types.Context` carries and the storage codecs convert `TIMESTAMP`
    /// values with. Inherent so the storage seam can ask for it without
    /// pulling the whole [`tidb_expr::Columns`] trait into scope.
    #[must_use]
    pub fn session_zone(&self) -> tidb_expr::SessionTimeZone {
        <Self as tidb_expr::Columns>::time_zone(self)
    }

    /// Returns the statement's Apply-cache byte budget.
    #[must_use]
    pub fn apply_cache_capacity(&self) -> i64 {
        self.apply_cache_capacity
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

    /// Go `ResetContextOfStmt`'s CREATE/ALTER type flags after
    /// `CtxWithHandleTruncateErrLevel(LevelError)` has made every truncation
    /// fatal. DDL differs from an INSERT in both zero-date formulas, so this
    /// is intentionally not an alias for [`Self::write_conversion_flags`].
    #[must_use]
    pub fn ddl_default_conversion_flags(&self) -> tidb_datatype::ConversionFlags {
        tidb_datatype::DEFAULT_STATEMENT_FLAGS
            .with_ignore_truncate_err(false)
            .with_truncate_as_warning(false)
            .with_allow_negative_to_unsigned(false)
            .with_ignore_invalid_date_err(self.date_modes.allow_invalid_dates)
            .with_ignore_zero_in_date_err(
                !self.date_modes.no_zero_in_date
                    || !self.strict
                    || self.date_modes.allow_invalid_dates,
            )
            .with_ignore_zero_date_err(!self.date_modes.no_zero_date || !self.strict)
    }

    /// Go `ddl.reorgTypeFlagsWithSQLMode`: the type flags used while a DDL
    /// reorg decodes old rows and materializes their origin defaults.
    ///
    /// This is deliberately separate from [`Self::ddl_default_conversion_flags`].
    /// CREATE/ALTER default admission derives both zero-date bits from the
    /// session modes and makes truncation fatal; reorg starts from
    /// `StrictFlags`, warns on truncation only outside strict mode, ignores
    /// invalid/zero-in dates only when the reorg SQL mode says so, and uses
    /// the source time-to-YEAR concatenation rule.
    #[must_use]
    pub fn reorg_default_conversion_flags(&self) -> tidb_datatype::ConversionFlags {
        tidb_datatype::STRICT_FLAGS
            .with_truncate_as_warning(!self.strict)
            .with_ignore_invalid_date_err(self.date_modes.allow_invalid_dates)
            .with_ignore_zero_in_date_err(!self.strict || self.date_modes.allow_invalid_dates)
            .with_cast_time_to_year_through_concat(true)
    }

    /// Go `ResetContextOfStmt`'s `SELECT` flags for reading a stored default.
    #[must_use]
    pub fn query_default_conversion_flags(&self) -> tidb_datatype::ConversionFlags {
        tidb_datatype::DEFAULT_STATEMENT_FLAGS
            .with_truncate_as_warning(true)
            .with_ignore_zero_in_date_err(true)
            .with_ignore_invalid_date_err(self.date_modes.allow_invalid_dates)
    }

    /// Go `ResetContextOfStmt`'s `SHOW` flags for reading a stored default.
    #[must_use]
    pub fn show_default_conversion_flags(&self) -> tidb_datatype::ConversionFlags {
        tidb_datatype::DEFAULT_STATEMENT_FLAGS
            .with_ignore_truncate_err(true)
            .with_ignore_zero_in_date_err(true)
            .with_ignore_invalid_date_err(self.date_modes.allow_invalid_dates)
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

    /// Attaches the session's own [`RetryAutoIds`], so that a statement the
    /// node runs a second time can take back the ids the first run assigned.
    /// A context without one never reuses and never records, which is the
    /// right behaviour for every caller that cannot retry at all.
    #[must_use]
    pub fn with_retry_auto_ids(mut self, channel: Rc<RefCell<RetryAutoIds>>) -> Self {
        self.retry_auto_ids = channel;
        self
    }

    /// The id a previous attempt at this statement gave the next
    /// AUTO_INCREMENT row, or `None` if there was no previous attempt or it
    /// did not reach this row. See [`RetryAutoIds::reuse`].
    pub fn reuse_auto_increment_id(&self) -> Option<u64> {
        self.retry_auto_ids.borrow_mut().reuse()
    }

    /// Records the id a row was given, reused or freshly allocated, so a
    /// replay of this statement writes the same one.
    pub fn record_auto_increment_id(&self, id: u64) {
        self.retry_auto_ids.borrow_mut().record(id);
    }

    /// Attaches the session's row-ID shard generator. Its retained shard is
    /// shared by every statement in the connection, as in Go SessionVars.
    #[must_use]
    pub fn with_row_id_shards(mut self, shards: Rc<RefCell<RowIdShardGenerator>>) -> Self {
        self.row_id_shards = shards;
        self
    }

    /// Declares the two session policies used by AUTO_RANDOM insertion.
    #[must_use]
    pub fn with_auto_random_policy(mut self, explicit_allowed: bool, shard_step: u64) -> Self {
        self.allow_auto_random_explicit_insert = explicit_allowed;
        self.shard_allocate_step = shard_step.max(1);
        self
    }

    /// The complete AUTO_RANDOM id assigned to this row by the previous
    /// retry attempt, if that attempt reached the row.
    pub fn reuse_auto_random_id(&self) -> Option<u64> {
        self.retry_auto_ids.borrow_mut().reuse_random()
    }

    /// Records one complete AUTO_RANDOM id for a possible statement replay.
    pub fn record_auto_random_id(&self, id: u64) {
        self.retry_auto_ids.borrow_mut().record_random(id);
    }

    /// Chooses the shard for the next `count` generated row IDs.
    pub fn next_auto_random_shard(&self, count: u64) -> u64 {
        self.row_id_shards
            .borrow_mut()
            .next(self.shard_allocate_step, count)
    }

    /// Whether this session permits a caller-provided AUTO_RANDOM value.
    #[must_use]
    pub const fn allow_auto_random_explicit_insert(&self) -> bool {
        self.allow_auto_random_explicit_insert
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

    /// Declares `@@auto_increment_increment` and `@@auto_increment_offset`.
    #[must_use]
    pub fn with_auto_increment_step(mut self, increment: u64, offset: u64) -> Self {
        self.auto_increment_step = (increment, offset);
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
    #[must_use]
    pub fn auto_increment_zero_is_explicit(&self) -> bool {
        self.auto_increment_zero_is_explicit
    }

    /// `@@auto_increment_increment` and `@@auto_increment_offset` as the
    /// session set them, BEFORE Go's `getIncrementAndOffset` clamp -- the
    /// allocator applies that clamp itself so the raw pair is what a caller
    /// reading the session's state sees.
    #[must_use]
    pub fn auto_increment_step(&self) -> (u64, u64) {
        self.auto_increment_step
    }

    /// The warnings evaluation recorded, in the order they were raised,
    /// followed by the warnings TiKV reported for this statement's
    /// coprocessor requests.
    ///
    /// The two sources are merged HERE rather than at the session, because
    /// this is the one door the session already comes through
    /// (`Session::drain_eval_warnings`): a coprocessor warning cannot be lost
    /// by a drain site that forgot the second buffer, which is exactly how
    /// TiKV's warnings stayed invisible while `response_channel` appended
    /// them correctly.
    ///
    /// DIVERGENCE: Go interleaves the two by arrival, because a `SelectResult`
    /// appends into the same handler local evaluation writes to. Here the
    /// remote ones come last. Only the ORDER differs; both sets are reported,
    /// with their codes.
    /// How many warnings this statement has recorded, which is Go's
    /// `warnCnt` bookmark in `doDupRowUpdate` (`pkg/executor/insert.go:479`).
    #[must_use]
    pub fn warning_count(&self) -> usize {
        self.warnings.borrow().len()
    }

    /// Go `StmtCtx.TruncateWarnings(warnCnt)` + `AppendWarnings`: rewrites
    /// every warning raised since a [`Self::warning_count`] bookmark.
    ///
    /// `rewrite` is handed each warning's code and message and answers a
    /// replacement message, or `None` to leave that warning alone. This is
    /// how `completeInsertErr` re-titles the warnings a cast produced without
    /// disturbing anything that was already there.
    pub fn rewrite_warnings_from(
        &self,
        bookmark: usize,
        rewrite: impl Fn(u16, &str) -> Option<String>,
    ) {
        let mut warnings = self.warnings.borrow_mut();
        for (_, code, message) in warnings.iter_mut().skip(bookmark) {
            if let Some(replacement) = rewrite(*code, message) {
                *message = replacement;
            }
        }
    }

    /// Drains this statement's warnings, evaluation's first and the
    /// coprocessor's after them (see the note above).
    #[must_use]
    pub fn take_warnings(&self) -> Vec<(WarningLevel, u16, String)> {
        let mut warnings = std::mem::take(&mut *self.warnings.borrow_mut());
        for warning in self.cop_warnings.take() {
            if warnings.len() >= MAX_WARNING_COUNT {
                break;
            }
            // A TiKV warning without a code is one the region could not name;
            // 1105 (`ErrUnknown`) is what TiDB reports for exactly that.
            let code = u16::try_from(warning.code.unwrap_or(1105)).unwrap_or(1105);
            warnings.push((warning.level, code, warning.message));
        }
        warnings
    }

    /// Go `StmtCtx.AppendNote`: the level an `IF EXISTS` / `IF NOT EXISTS`
    /// files the error it swallowed under.
    ///
    /// The note carries the SUPPRESSED error's own code and text rather than
    /// a second string beside it, exactly as Go's `dropTableObject` hands
    /// `AppendNote` the very `ErrBadTable` it would otherwise have returned --
    /// so the two can never drift.
    pub(crate) fn append_suppressed(&self, error: &DriverError) {
        let reported = error.clone().to_mysql_error();
        self.append_leveled(WarningLevel::Note, reported.code, &reported.message);
    }

    /// The one push onto the buffer, so its retention limit lives in one
    /// place regardless of which level came through.
    fn append_leveled(&self, level: WarningLevel, code: u16, message: &str) {
        let mut warnings = self.warnings.borrow_mut();
        if warnings.len() >= MAX_WARNING_COUNT {
            return;
        }
        warnings.push((level, code, message.to_owned()));
    }
}

impl Columns for StmtContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn no_unsigned_subtraction(&self) -> bool {
        self.no_unsigned_subtraction
    }

    fn now(&self) -> Option<(i64, u32, i32)> {
        self.now
    }

    fn sysdate_is_now(&self) -> bool {
        self.sysdate_is_now
    }

    fn time_zone(&self) -> tidb_expr::SessionTimeZone {
        self.time_zone
            .clone()
            .unwrap_or(tidb_expr::SessionTimeZone::Fixed {
                name: "UTC".to_owned(),
                offset_secs: 0,
            })
    }

    fn like_default_escape(&self) -> u8 {
        self.like_default_escape
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

    fn tidb_info(&self) -> String {
        self.tidb_info.clone().unwrap_or_else(|| {
            tidb_util::printer::get_tidb_info(&tidb_util::versioninfo::VersionInfo::build_default())
        })
    }

    fn block_encryption_mode(&self) -> tidb_expr::BlockEncryptionMode {
        self.block_encryption_mode
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
        self.rand_session.as_ref().map(|rng| rng.gen())
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

    fn max_allowed_packet(&self) -> u64 {
        self.max_allowed_packet
    }

    fn sysvar(&self, scope: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum> {
        // Only the variables a builtin reads are answered here; the session
        // resolves every other `@@var` before the driver sees the statement.
        if name.eq_ignore_ascii_case("version") {
            return self
                .version
                .as_ref()
                .map(|value| Datum::Bytes(value.clone().into_bytes()));
        }
        // `GROUP_CONCAT`'s byte budget travels this way rather than as its own
        // `Columns` method: the aggregate reads it once per statement, and the
        // trait already has a general variable channel.
        if name.eq_ignore_ascii_case("group_concat_max_len") {
            return Some(Datum::UInt(self.group_concat_max_len));
        }
        if matches!(scope, Some(tidb_ast::SysVarScope::Global)) {
            return self
                .global_sysvars
                .get(&name.to_ascii_lowercase())
                .map(|value| Datum::Bytes(value.as_bytes().to_vec()));
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

    fn truncate_level(&self) -> ErrorLevel {
        self.truncate
    }

    fn append_warning(&self, code: u16, message: &str) {
        self.append_leveled(WarningLevel::Warning, code, message);
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

    /// Go `EvalContext.GetMaxAllowedPacket`, read by every result-sizing
    /// string builtin: `SPACE(n)` past the limit is NULL with warning 1301
    /// (`handleAllowedPacketOverflowed`).
    ///
    /// Asserted through an EVALUATED builtin rather than through the getter,
    /// because the getter agreeing with the setter proves nothing: what broke
    /// was that `StmtContext` did not override the trait default at all, so
    /// every builtin sized against 64 MiB whatever the session said.
    #[test]
    fn a_string_builtin_sizes_its_result_against_this_contexts_packet_limit() {
        fn space_2000(ctx: &StmtContext) -> Datum {
            let stmt = tidb_parser::parse("SELECT SPACE(2000)").expect("the probe SQL parses");
            let tidb_ast::Stmt::Query(query) = &stmt else {
                panic!("not a query")
            };
            let tidb_ast::QueryStmt::Select(select) = &**query else {
                panic!("not a select")
            };
            let tidb_ast::SelectField::Expr { expr, .. } = &select.fields.fields()[0] else {
                panic!("not an expression field")
            };
            let expression =
                tidb_expr::rewriter::rewrite_expr_resolved(expr, &tidb_expr::rewriter::NoResolver)
                    .expect("SPACE rewrites");
            let mut dual = tidb_chunk::chunk::Chunk::new_empty(&[]);
            dual.set_num_virtual_rows(1);
            expression
                .eval(ctx, dual.get_row(0))
                .expect("SPACE evaluates")
        }

        let default = StmtContext::for_query();
        assert!(
            matches!(space_2000(&default), Datum::String(_) | Datum::Bytes(_)),
            "the shipped 64 MiB limit fits 2000 spaces"
        );
        assert!(default.take_warnings().is_empty());

        let narrow = StmtContext::for_query().with_max_allowed_packet(1024);
        assert_eq!(space_2000(&narrow), Datum::Null);
        let warnings = narrow.take_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].1, 1301);
    }

    /// Go `handleAllowedPacketOverflowed` (`pkg/expression/errors.go:88-96`)
    /// returns the 1301 condition as an ERROR when the statement has neither
    /// `TruncateAsWarning` nor `IgnoreTruncateErr` -- a STRICT write. Only
    /// the warning spelling existed here, so a strict `INSERT` of an
    /// oversized `SPACE()` silently stored NULL.
    #[test]
    fn a_strict_write_raises_the_packet_overflow_as_an_error() {
        use tidb_expr::Columns;
        // `for_dml(error_for_division_by_zero, strict, ignore_err)`.
        let strict = StmtContext::for_dml(true, true, false).with_max_allowed_packet(1024);
        let error = strict
            .handle_allowed_packet_overflowed("space")
            .expect_err("a strict write must fail rather than warn");
        let wire =
            crate::DriverError::Exec(crate::executor::ExecError::Eval(error)).to_mysql_error();
        assert_eq!(wire.code, 1301);
        assert_eq!(
            wire.message,
            "Result of space() was larger than max_allowed_packet (1024) - truncated"
        );
        assert!(
            strict.take_warnings().is_empty(),
            "the error arm appends no warning"
        );

        // Non-strict, and every read: the warning spelling, and the caller's
        // NULL stands.
        let lenient = StmtContext::for_dml(true, false, false).with_max_allowed_packet(1024);
        lenient
            .handle_allowed_packet_overflowed("space")
            .expect("a non-strict write warns");
        let warnings = lenient.take_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].1, 1301);
    }

    #[test]
    fn connection_id_absent_by_default() {
        assert_eq!(StmtContext::for_query().connection_id(), None);
    }

    /// Go's `StaticWarnHandler` stops appending at `math.MaxUint16`, so a
    /// non-strict bulk write that converts millions of values retains the
    /// first 65,535 rather than one entry per value.
    #[test]
    fn the_warning_buffer_stops_at_the_source_retention_limit() {
        let ctx = StmtContext::for_query();
        for index in 0..MAX_WARNING_COUNT + 16 {
            ctx.append_warning_parts(1292, &format!("value {index}"));
        }
        let warnings = ctx.take_warnings();
        assert_eq!(warnings.len(), MAX_WARNING_COUNT);
        // The entries kept are the FIRST ones: Go appends until the limit and
        // then drops, rather than evicting the oldest.
        assert_eq!(warnings[0].2, "value 0");
        assert_eq!(warnings[MAX_WARNING_COUNT - 1].2, "value 65534");
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
        let rng = Rc::new(MysqlRng::new_with_seed(1));
        let ctx = StmtContext::for_query().with_rand_session(Rc::clone(&rng));
        // Matches `MysqlRng::new_with_seed(1)`'s own pinned sequence
        // (`tidb-util::mathutil`'s source seed vectors), read through the
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

    #[test]
    fn reorg_flags_are_not_ddl_default_admission_flags() {
        let strict =
            StmtContext::for_dml(true, true, false).with_date_modes(crate::zero_date::DateModes {
                no_zero_date: true,
                no_zero_in_date: true,
                allow_invalid_dates: false,
            });
        let flags = strict.reorg_default_conversion_flags();
        assert!(!flags.truncate_as_warning());
        assert!(!flags.ignore_zero_date_err());
        assert!(!flags.ignore_zero_in_date_err());
        assert!(!flags.ignore_invalid_date_err());
        assert!(flags.cast_time_to_year_through_concat());
        assert_ne!(flags, strict.ddl_default_conversion_flags());

        let permissive =
            StmtContext::for_dml(true, false, false).with_date_modes(crate::zero_date::DateModes {
                no_zero_date: true,
                no_zero_in_date: true,
                allow_invalid_dates: true,
            });
        let flags = permissive.reorg_default_conversion_flags();
        assert!(flags.truncate_as_warning());
        assert!(!flags.ignore_zero_date_err());
        assert!(flags.ignore_zero_in_date_err());
        assert!(flags.ignore_invalid_date_err());
        assert!(flags.cast_time_to_year_through_concat());
    }
}
