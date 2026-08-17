// Copyright 2025 PingCAP, Inc.
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

//! Go `pkg/domain/historical_stats.go` lands complete.
//!
//! Every production symbol of that file is here: `HistoricalStatsWorker`
//! ([`HistoricalStatsWorker`]), `SendTblToDumpHistoricalStats`
//! ([`HistoricalStatsWorker::send_tbl_to_dump_historical_stats`]),
//! `DumpHistoricalStats`
//! ([`HistoricalStatsWorker::dump_historical_stats`]), and
//! `GetOneHistoricalStatsTable`
//! ([`HistoricalStatsWorker::get_one_historical_stats_table`]).
//!
//! The file is one bounded, never-blocking mailbox plus one dump routine.
//! `ANALYZE` hands a table ID to the mailbox and walks away; a background
//! loop in `domain.go` drains it and asks the statistics handle to write a
//! historical snapshot of that table.
//!
//! ## Go quirks reproduced, not fixed
//!
//! - **A full mailbox silently drops the task.** Go's send is
//!   `select { case w.tblCH <- tableID: default: log }`
//!   (`historical_stats.go:48-53`): the send never blocks and never reports
//!   failure, so a burst of more than [`TBL_CHANNEL_CAPACITY`] pending
//!   tables loses the overflow with only a warning line. Reproduced:
//!   [`HistoricalStatsWorker::send_tbl_to_dump_historical_stats`] returns
//!   `()`, exactly like Go, and [`SendOutcome`] is offered only through the
//!   separate [`HistoricalStatsWorker::send_tbl_to_dump_historical_stats_outcome`]
//!   so a test can observe the drop that the real caller cannot.
//! - **The disabled switch is indistinguishable from a successful send.**
//!   When `enableDumpHistoricalStats` is false the function returns before
//!   touching the channel (`historical_stats.go:45-47`), again returning
//!   nothing. Same shape here.
//! - **`GetOneHistoricalStatsTable` returns `-1`, not an absence.** Go's
//!   empty-mailbox branch is `default: return -1`
//!   (`historical_stats.go:97-98`). `-1` is a real `int64` a caller can and
//!   does feed straight back into `DumpHistoricalStats` — the upstream tests
//!   in `pkg/executor/historical_stats_test.go` do exactly that. This is
//!   *not* a Go nil, so per the porting rules it stays a value and is not
//!   collapsed into `Option`: the API returns `i64`. [`Self::try_recv_table`]
//!   exposes the `Option` view for callers who want it.
//! - **After the channel is closed, that same call returns `0`.**
//!   `domain.go:1899` closes `tblCH` when the domain exits; a Go receive on
//!   a closed-and-drained channel succeeds immediately with the zero value,
//!   so `GetOneHistoricalStatsTable` flips from `-1` to `0` at shutdown.
//!   Reproduced by [`HistoricalStatsWorker::close_table_channel`] and
//!   asserted in the tests.
//! - **Sending after the close panics.** Go's send on a closed channel is a
//!   runtime panic, which is what a post-shutdown `ANALYZE` would hit.
//!   Reproduced as a `panic!` with the Go message, rather than being
//!   quietly turned into a drop.
//! - **The dump's own `is_partition` flag is decided by lookup order, not by
//!   the table.** `historical_stats.go:69-79` marks the table a partition
//!   only when `TableByID` misses and `FindTableByPartitionID` hits; a
//!   partitioned table's own ID therefore dumps with `isPartition == false`.
//!   Preserved verbatim.
//!
//! ## Narrowings, all named
//!
//! - `// boundary:` Go `pkg/sessionctx.Context` held as the worker's `sctx`
//!   field, used only through `GetDomain(sctx).InfoSchema()`
//!   (`historical_stats.go:34`, `:66`). Narrowed to [`SessionInfoSchema`],
//!   whose single method is that expression. No Rust home exists yet:
//!   `pkg/domain.GetDomain` is `domainctx.go`, still declined for want of a
//!   `Domain` type, and `pkg/sessionctx.Context` has no crate. When
//!   `domain.go` lands, `Domain` implements this trait in one line.
//! - `// boundary:` Go `pkg/infoschema.InfoSchema` — `TableByID` and
//!   `FindTableByPartitionID` (`historical_stats.go:69`, `:71`) plus the
//!   package function `infoschema.SchemaByTable` (`:80`). Narrowed to
//!   [`InfoSchemaView`]. The closest existing Rust home is
//!   `tidb_stats::auto_analyze_runtime::ports::InfoSchemaPort`, which
//!   already declares `table_by_id`; it is deliberately not depended on so
//!   this crate keeps its two-dependency footprint, and a later batch can
//!   collapse the two mechanically.
//! - `// boundary:` Go `pkg/meta/model.TableInfo`. Only `ID` and `Name.O`
//!   are read here, so [`TableMeta`] carries exactly those two fields. The
//!   name is the *original-case* `.O`, matching `dbInfo.Name.O` and
//!   `tblInfo.Name.O` at `historical_stats.go:84-86`. Existing Rust home:
//!   `tidb_stats::auto_analyze_runtime::model::TableMeta`.
//! - `// boundary:` Go `pkg/statistics/handle.Handle` —
//!   `CheckHistoricalStatsEnable` and `RecordHistoricalStatsToStorage`
//!   (`historical_stats.go:58`, `:84`). Narrowed to [`StatsHandle`]. The
//!   Rust home for the surrounding logic is the `tidb-stats` crate
//!   (`tidb_stats::historical_stats`), which today holds only the version
//!   selection, not the handle.
//! - `// boundary:` Go `pkg/domain/metrics` —
//!   `GenerateHistoricalStatsFailedCounter` /
//!   `GenerateHistoricalStatsSuccessCounter` (`:85`, `:88`). Kept, not
//!   dropped, because they are the only success signal a failed dump leaves
//!   behind: [`HistoricalStatsMetrics`]. [`NoopHistoricalStatsMetrics`] is
//!   the do-nothing implementation.
//! - `// boundary:` Go failpoint `github.com/pingcap/tidb/pkg/domain/
//!   sendHistoricalStats` (`historical_stats.go:40-44`), which forces
//!   `send = true` regardless of the global switch. Failpoints have no Rust
//!   home anywhere in the workspace. It is *not* invented here; the global
//!   switch alone decides. Note the Go quirk left unreproduced along with
//!   it: `val.(bool)` panics if the failpoint is configured with a
//!   non-boolean.
//! - `// boundary:` Go `enableDumpHistoricalStats` is declared in
//!   `domain.go:1882` and set true by that file's `init()`
//!   (`domain.go:1783`). It is read only here, so it is hosted here as
//!   [`enable_dump_historical_stats`] /
//!   [`set_enable_dump_historical_stats`] and moves when `domain.go` lands.
//!   `domain.go`'s `DisableDumpHistoricalStats4Test` is that setter with
//!   `false`.
//! - `// boundary:` Go `pkg/util/logutil` warn lines (`:52`, and
//!   `domain.go:1906`). Pure telemetry, dropped; the drop they announce is
//!   observable through [`SendOutcome`] instead.
//!
//! ## Tests
//!
//! `pkg/domain` has no test for this file. Every upstream exercise of these
//! symbols lives in `pkg/executor/historical_stats_test.go`,
//! `pkg/server/handler/optimizor/statistics_handler_test.go`, and
//! `plan_replayer_test.go`, all of which need a bootstrapped mockstore, a
//! real `ANALYZE`, the `mysql.stats_history` table, and the
//! `sendHistoricalStats` failpoint. None of that is reachable, so nothing is
//! transcreated; the tests below are written against the seams.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

/// Capacity of the worker's mailbox.
///
/// boundary: Go `domain.go:1763` — `make(chan int64, 16)` inside
/// `(*Domain).SetupHistoricalStatsWorker`. The constant belongs to
/// `domain.go`, but it is the buffer whose overflow this file's drop
/// behavior depends on, so it is named here until that batch lands.
pub const TBL_CHANNEL_CAPACITY: usize = 16;

/// Sentinel `GetOneHistoricalStatsTable` returns when the mailbox is empty.
///
/// boundary: Go `historical_stats.go:98`.
pub const NO_HISTORICAL_STATS_TABLE: i64 = -1;

/// Go `enableDumpHistoricalStats`.
///
/// boundary: Go `domain.go:1882`; `domain.go`'s `init()` stores `true`
/// (`domain.go:1783`), which is the initial value here.
static ENABLE_DUMP_HISTORICAL_STATS: AtomicBool = AtomicBool::new(true);

/// Reads the process-global historical-stats dump switch.
#[must_use]
pub fn enable_dump_historical_stats() -> bool {
    ENABLE_DUMP_HISTORICAL_STATS.load(Ordering::SeqCst)
}

/// Writes the process-global historical-stats dump switch.
///
/// boundary: Go `(*Domain)` has no setter beyond
/// `DisableDumpHistoricalStats4Test` (`domain.go:1792-1794`) and the
/// package `init()`; both are this function.
pub fn set_enable_dump_historical_stats(enabled: bool) {
    ENABLE_DUMP_HISTORICAL_STATS.store(enabled, Ordering::SeqCst);
}

/// What a send did — the information Go's `select`/`default` discards.
///
/// Go's `SendTblToDumpHistoricalStats` returns nothing, so a production
/// caller cannot tell these apart. Neither can the Rust caller of
/// [`HistoricalStatsWorker::send_tbl_to_dump_historical_stats`]; this enum
/// exists only for the `_outcome` variant used by tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOutcome {
    /// The table ID was queued.
    Queued,
    /// `enableDumpHistoricalStats` was false; the mailbox was not touched.
    Disabled,
    /// The mailbox was full; Go logs `"discard dump historical stats task"`.
    Dropped,
}

/// Go `model.TableInfo`, narrowed to the two fields this file reads.
///
/// boundary: Go `pkg/meta/model.TableInfo`. Existing Rust home:
/// `tidb_stats::auto_analyze_runtime::model::TableMeta`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMeta {
    /// Go `TableInfo.ID`.
    pub id: i64,
    /// Go `TableInfo.Name.O` — the original-case name.
    pub name: String,
}

/// The `infoschema` lookups this file performs.
///
/// boundary: Go `pkg/infoschema.InfoSchema` plus the package function
/// `infoschema.SchemaByTable`.
pub trait InfoSchemaView {
    /// Go `is.TableByID(ctx, tableID)`; `None` is Go's `existed == false`.
    ///
    /// boundary: Go drops the `context.Context` argument's cancellation on
    /// the floor here — `historical_stats.go:69` passes
    /// `context.Background()` — so no context is threaded through.
    fn table_by_id(&self, table_id: i64) -> Option<TableMeta>;

    /// Go `is.FindTableByPartitionID(partitionID)`, which returns the
    /// triple `(table, db, partition)`.
    ///
    /// Go treats the lookup as a miss unless *all three* are non-nil
    /// (`historical_stats.go:72`). Implementors MUST therefore return
    /// `None` whenever any of the three would be nil; returning `Some` with
    /// a table whose db or partition was nil is not representable, which is
    /// the point.
    fn find_table_by_partition_id(&self, partition_id: i64) -> Option<TableMeta>;

    /// Go `infoschema.SchemaByTable(is, tblInfo)`, returning the schema's
    /// original-case name. `None` is Go's `existed == false`.
    fn schema_by_table(&self, table: &TableMeta) -> Option<String>;
}

/// The session the worker holds, used only to reach the current schema.
///
/// boundary: Go `pkg/sessionctx.Context` stored as
/// `HistoricalStatsWorker.sctx`, dereferenced exactly once as
/// `GetDomain(sctx).InfoSchema()` (`historical_stats.go:66`).
pub trait SessionInfoSchema {
    /// The schema snapshot type.
    type View: InfoSchemaView;

    /// Go `GetDomain(w.sctx).InfoSchema()`.
    ///
    /// Go re-reads the latest schema on every call; implementors must do
    /// the same rather than caching a snapshot at construction.
    fn info_schema(&self) -> Self::View;
}

/// The two statistics-handle calls this file makes.
///
/// boundary: Go `pkg/statistics/handle.Handle`.
pub trait StatsHandle {
    /// Go `statsHandle.CheckHistoricalStatsEnable()`.
    ///
    /// The `Err` string is interpolated into Go's
    /// `"check tidb_enable_historical_stats failed: %v"`.
    fn check_historical_stats_enable(&self) -> Result<bool, String>;

    /// Go `statsHandle.RecordHistoricalStatsToStorage(dbName, tblInfo,
    /// physicalID, isPartition)`.
    ///
    /// Go returns `(uint64, error)` and this file discards the `uint64`
    /// with `_`; it is kept in the signature so the seam matches the Go
    /// symbol and a later collapse is mechanical.
    fn record_historical_stats_to_storage(
        &self,
        db_name: &str,
        table: &TableMeta,
        physical_id: i64,
        is_partition: bool,
    ) -> Result<u64, String>;
}

/// The two counters `DumpHistoricalStats` bumps.
///
/// boundary: Go `pkg/domain/metrics.GenerateHistoricalStatsFailedCounter`
/// and `GenerateHistoricalStatsSuccessCounter`.
pub trait HistoricalStatsMetrics {
    /// Go `GenerateHistoricalStatsFailedCounter.Inc()`.
    fn inc_generate_failed(&self);
    /// Go `GenerateHistoricalStatsSuccessCounter.Inc()`.
    fn inc_generate_success(&self);
}

/// A [`HistoricalStatsMetrics`] that counts nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopHistoricalStatsMetrics;

impl HistoricalStatsMetrics for NoopHistoricalStatsMetrics {
    fn inc_generate_failed(&self) {}
    fn inc_generate_success(&self) {}
}

/// Go `historical_stats.go` errors, which are all `errors.Errorf` strings.
///
/// boundary: Go builds these with `github.com/pingcap/errors.Errorf`, i.e.
/// message-only errors with no code and no class. The variants exist so a
/// Rust caller can branch; [`std::fmt::Display`] reproduces Go's text
/// byte-for-byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DumpHistoricalStatsError {
    /// Go `"check tidb_enable_historical_stats failed: %v"`.
    CheckEnableFailed(String),
    /// Go `"cannot get table by id %d"`.
    TableNotFound(i64),
    /// Go `"cannot get DBInfo by TableID %d"`.
    SchemaNotFound(i64),
    /// Go `"record table %s.%s's historical stats failed, err:%v"`, whose
    /// two names are the schema and the table, both original-case.
    RecordFailed {
        /// Schema name, Go `dbInfo.Name.O`.
        db_name: String,
        /// Table name, Go `tblInfo.Name.O`.
        table_name: String,
        /// The handle's error text.
        cause: String,
    },
}

impl std::fmt::Display for DumpHistoricalStatsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CheckEnableFailed(cause) => {
                write!(f, "check tidb_enable_historical_stats failed: {cause}")
            }
            Self::TableNotFound(id) => write!(f, "cannot get table by id {id}"),
            Self::SchemaNotFound(id) => write!(f, "cannot get DBInfo by TableID {id}"),
            Self::RecordFailed {
                db_name,
                table_name,
                cause,
            } => write!(
                f,
                "record table {db_name}.{table_name}'s historical stats failed, err:{cause}"
            ),
        }
    }
}

impl std::error::Error for DumpHistoricalStatsError {}

/// Go `HistoricalStatsWorker.tblCH`, a buffered `chan int64`.
///
/// Both ends of this channel are used non-blockingly in Go — every access
/// in the package is a `select` with a `default` — so no parking is
/// modelled. What *is* modelled is the close: `domain.go:1899` closes the
/// channel at domain exit, after which Go's receive yields the zero value
/// and Go's send panics.
#[derive(Debug)]
struct TableChannel {
    state: Mutex<TableChannelState>,
    capacity: usize,
}

#[derive(Debug)]
struct TableChannelState {
    queue: VecDeque<i64>,
    closed: bool,
}

impl TableChannel {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(TableChannelState {
                queue: VecDeque::new(),
                closed: false,
            }),
            capacity,
        }
    }

    /// Go `select { case ch <- v: true; default: false }`.
    ///
    /// # Panics
    ///
    /// Reproduces Go's panic on a send to a closed channel.
    fn try_send(&self, value: i64) -> bool {
        let mut state = self.lock();
        assert!(!state.closed, "send on closed channel");
        if state.queue.len() >= self.capacity {
            return false;
        }
        state.queue.push_back(value);
        true
    }

    /// Go `select { case v, ok := <-ch: ...; default: ... }`.
    ///
    /// `Some` is a value taken from the buffer. `None` is Go's `default`
    /// branch on an open channel. A closed, drained channel is *not* a
    /// `default` branch in Go — the receive succeeds with the zero value —
    /// so it reports `Some(0)`.
    fn try_recv(&self) -> Option<i64> {
        let mut state = self.lock();
        match state.queue.pop_front() {
            Some(value) => Some(value),
            None if state.closed => Some(0),
            None => None,
        }
    }

    /// Go `close(ch)`.
    ///
    /// # Panics
    ///
    /// Reproduces Go's panic on a double close.
    fn close(&self) {
        let mut state = self.lock();
        assert!(!state.closed, "close of closed channel");
        state.closed = true;
    }

    fn len(&self) -> usize {
        self.lock().queue.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TableChannelState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Go `HistoricalStatsWorker`: the mailbox that `ANALYZE` posts table IDs
/// to, plus the session used to resolve them.
///
/// boundary: Go's struct is constructed only by
/// `(*Domain).SetupHistoricalStatsWorker` (`domain.go:1761-1766`), which
/// supplies the capacity-16 channel and the session. [`Self::new`] is that
/// constructor with the capacity defaulted to [`TBL_CHANNEL_CAPACITY`].
#[derive(Debug)]
pub struct HistoricalStatsWorker<S> {
    tbl_ch: TableChannel,
    sctx: S,
}

impl<S> HistoricalStatsWorker<S> {
    /// Go `&HistoricalStatsWorker{tblCH: make(chan int64, 16), sctx: ctx}`.
    #[must_use]
    pub fn new(sctx: S) -> Self {
        Self::with_capacity(sctx, TBL_CHANNEL_CAPACITY)
    }

    /// [`Self::new`] with an explicit buffer size, so the drop-on-full
    /// behavior is testable without queueing sixteen tables.
    #[must_use]
    pub fn with_capacity(sctx: S, capacity: usize) -> Self {
        Self {
            tbl_ch: TableChannel::new(capacity),
            sctx,
        }
    }

    /// The session this worker holds.
    ///
    /// boundary: Go's `sctx` field is unexported and read only by
    /// `DumpHistoricalStats`; this accessor has no Go counterpart and
    /// exists because Rust callers cannot reach a private field.
    pub fn session(&self) -> &S {
        &self.sctx
    }

    /// Go `SendTblToDumpHistoricalStats`.
    ///
    /// Returns nothing, exactly like Go: a disabled switch and a full
    /// mailbox are both invisible to the caller. Use
    /// [`Self::send_tbl_to_dump_historical_stats_outcome`] to see which
    /// happened.
    ///
    /// # Panics
    ///
    /// Panics if the mailbox has been closed, reproducing Go's panic on a
    /// send to a closed channel.
    pub fn send_tbl_to_dump_historical_stats(&self, table_id: i64) {
        let _ = self.send_tbl_to_dump_historical_stats_outcome(table_id);
    }

    /// [`Self::send_tbl_to_dump_historical_stats`], reporting what it did.
    ///
    /// This has no Go counterpart; it is the same code path with the
    /// discarded verdict surfaced.
    ///
    /// # Panics
    ///
    /// Panics if the mailbox has been closed.
    pub fn send_tbl_to_dump_historical_stats_outcome(&self, table_id: i64) -> SendOutcome {
        // boundary: Go consults failpoint `sendHistoricalStats` here and
        // forces `send = true`. Failpoints have no Rust home; the global
        // switch alone decides.
        if !enable_dump_historical_stats() {
            return SendOutcome::Disabled;
        }
        if self.tbl_ch.try_send(table_id) {
            SendOutcome::Queued
        } else {
            SendOutcome::Dropped
        }
    }

    /// Go `GetOneHistoricalStatsTable`.
    ///
    /// Returns [`NO_HISTORICAL_STATS_TABLE`] (`-1`) when the mailbox is
    /// empty and open, and `0` once it has been closed and drained — see
    /// the module docs.
    #[must_use]
    pub fn get_one_historical_stats_table(&self) -> i64 {
        self.tbl_ch.try_recv().unwrap_or(NO_HISTORICAL_STATS_TABLE)
    }

    /// The `Option` view of [`Self::get_one_historical_stats_table`], for
    /// callers that want the absence separated from the sentinel.
    ///
    /// No Go counterpart. `Some(0)` after a close is retained rather than
    /// mapped away, because the zero is a real Go receive.
    #[must_use]
    pub fn try_recv_table(&self) -> Option<i64> {
        self.tbl_ch.try_recv()
    }

    /// Go `close(do.historicalStatsWorker.tblCH)` (`domain.go:1899`).
    ///
    /// Lives here rather than in the (unported) `StartHistoricalStatsWorker`
    /// because the channel is this struct's field and the close is what
    /// makes [`Self::get_one_historical_stats_table`] return `0`.
    ///
    /// # Panics
    ///
    /// Panics on a double close, reproducing Go.
    pub fn close_table_channel(&self) {
        self.tbl_ch.close();
    }

    /// Number of table IDs currently buffered — Go `len(w.tblCH)`.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.tbl_ch.len()
    }
}

impl<S: SessionInfoSchema> HistoricalStatsWorker<S> {
    /// Go `DumpHistoricalStats`.
    ///
    /// The lookup order is Go's, and so is the `is_partition` flag it
    /// produces: a table found by `TableByID` is never marked a partition,
    /// even when it is itself partitioned.
    ///
    /// # Errors
    ///
    /// Every branch of Go's four `errors.Errorf` calls, as
    /// [`DumpHistoricalStatsError`]. A disabled
    /// `tidb_enable_historical_stats` is `Ok(())` with nothing written,
    /// matching Go's early `return nil`.
    pub fn dump_historical_stats(
        &self,
        table_id: i64,
        stats_handle: &dyn StatsHandle,
        metrics: &dyn HistoricalStatsMetrics,
    ) -> Result<(), DumpHistoricalStatsError> {
        let historical_stats_enabled = stats_handle
            .check_historical_stats_enable()
            .map_err(DumpHistoricalStatsError::CheckEnableFailed)?;
        if !historical_stats_enabled {
            return Ok(());
        }
        let is = self.sctx.info_schema();
        let (table, is_partition) = match is.table_by_id(table_id) {
            Some(table) => (table, false),
            None => {
                let table = is
                    .find_table_by_partition_id(table_id)
                    .ok_or(DumpHistoricalStatsError::TableNotFound(table_id))?;
                (table, true)
            }
        };
        let db_name = is
            .schema_by_table(&table)
            .ok_or(DumpHistoricalStatsError::SchemaNotFound(table_id))?;
        match stats_handle.record_historical_stats_to_storage(
            &db_name,
            &table,
            table_id,
            is_partition,
        ) {
            Ok(_) => {
                metrics.inc_generate_success();
                Ok(())
            }
            Err(cause) => {
                metrics.inc_generate_failed();
                Err(DumpHistoricalStatsError::RecordFailed {
                    db_name,
                    table_name: table.name,
                    cause,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::atomic::AtomicUsize;

    use super::*;

    /// `ENABLE_DUMP_HISTORICAL_STATS` is process-global, exactly as in Go.
    /// Tests that move it serialize through this lock and restore it.
    static SWITCH_LOCK: Mutex<()> = Mutex::new(());

    fn with_dump_switch<T>(enabled: bool, body: impl FnOnce() -> T) -> T {
        let _guard = SWITCH_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = enable_dump_historical_stats();
        set_enable_dump_historical_stats(enabled);
        let out = body();
        set_enable_dump_historical_stats(previous);
        out
    }

    #[derive(Default)]
    struct FakeSchema {
        by_id: Vec<TableMeta>,
        by_partition: Vec<(i64, TableMeta)>,
        schemas: Vec<(i64, String)>,
    }

    impl InfoSchemaView for FakeSchema {
        fn table_by_id(&self, table_id: i64) -> Option<TableMeta> {
            self.by_id.iter().find(|t| t.id == table_id).cloned()
        }

        fn find_table_by_partition_id(&self, partition_id: i64) -> Option<TableMeta> {
            self.by_partition
                .iter()
                .find(|(id, _)| *id == partition_id)
                .map(|(_, t)| t.clone())
        }

        fn schema_by_table(&self, table: &TableMeta) -> Option<String> {
            self.schemas
                .iter()
                .find(|(id, _)| *id == table.id)
                .map(|(_, name)| name.clone())
        }
    }

    struct FakeSession {
        schema: FakeSchema,
        reads: AtomicUsize,
    }

    impl FakeSession {
        fn new(schema: FakeSchema) -> Self {
            Self {
                schema,
                reads: AtomicUsize::new(0),
            }
        }
    }

    impl SessionInfoSchema for &FakeSession {
        type View = FakeSchema;

        fn info_schema(&self) -> FakeSchema {
            self.reads.fetch_add(1, Ordering::SeqCst);
            FakeSchema {
                by_id: self.schema.by_id.clone(),
                by_partition: self.schema.by_partition.clone(),
                schemas: self.schema.schemas.clone(),
            }
        }
    }

    struct FakeHandle {
        enabled: Result<bool, String>,
        record: Result<u64, String>,
        calls: RefCell<Vec<(String, i64, bool)>>,
    }

    impl FakeHandle {
        fn enabled() -> Self {
            Self {
                enabled: Ok(true),
                record: Ok(7),
                calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl StatsHandle for FakeHandle {
        fn check_historical_stats_enable(&self) -> Result<bool, String> {
            self.enabled.clone()
        }

        fn record_historical_stats_to_storage(
            &self,
            db_name: &str,
            _table: &TableMeta,
            physical_id: i64,
            is_partition: bool,
        ) -> Result<u64, String> {
            self.calls
                .borrow_mut()
                .push((db_name.to_owned(), physical_id, is_partition));
            self.record.clone()
        }
    }

    #[derive(Default)]
    struct CountingMetrics {
        success: AtomicUsize,
        failed: AtomicUsize,
    }

    impl HistoricalStatsMetrics for CountingMetrics {
        fn inc_generate_failed(&self) {
            self.failed.fetch_add(1, Ordering::SeqCst);
        }

        fn inc_generate_success(&self) {
            self.success.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn schema_with_table(id: i64, table: &str, db: &str) -> FakeSchema {
        FakeSchema {
            by_id: vec![TableMeta {
                id,
                name: table.to_owned(),
            }],
            by_partition: Vec::new(),
            schemas: vec![(id, db.to_owned())],
        }
    }

    #[test]
    fn send_queues_when_enabled() {
        with_dump_switch(true, || {
            let worker = HistoricalStatsWorker::new(());
            assert_eq!(
                worker.send_tbl_to_dump_historical_stats_outcome(42),
                SendOutcome::Queued
            );
            assert_eq!(worker.pending_len(), 1);
            assert_eq!(worker.get_one_historical_stats_table(), 42);
        });
    }

    #[test]
    fn send_is_a_no_op_when_disabled() {
        with_dump_switch(false, || {
            let worker = HistoricalStatsWorker::new(());
            assert_eq!(
                worker.send_tbl_to_dump_historical_stats_outcome(42),
                SendOutcome::Disabled
            );
            assert_eq!(worker.pending_len(), 0);
            // The Go-shaped call reports nothing at all.
            worker.send_tbl_to_dump_historical_stats(42);
            assert_eq!(worker.pending_len(), 0);
        });
    }

    #[test]
    fn a_full_mailbox_silently_drops_the_overflow() {
        with_dump_switch(true, || {
            let worker = HistoricalStatsWorker::with_capacity((), 2);
            assert_eq!(
                worker.send_tbl_to_dump_historical_stats_outcome(1),
                SendOutcome::Queued
            );
            assert_eq!(
                worker.send_tbl_to_dump_historical_stats_outcome(2),
                SendOutcome::Queued
            );
            assert_eq!(
                worker.send_tbl_to_dump_historical_stats_outcome(3),
                SendOutcome::Dropped
            );
            assert_eq!(worker.pending_len(), 2);
            assert_eq!(worker.get_one_historical_stats_table(), 1);
            assert_eq!(worker.get_one_historical_stats_table(), 2);
            // The dropped one is simply gone.
            assert_eq!(worker.get_one_historical_stats_table(), -1);
        });
    }

    #[test]
    fn default_capacity_matches_setup_historical_stats_worker() {
        with_dump_switch(true, || {
            let worker = HistoricalStatsWorker::new(());
            for id in 0..i64::try_from(TBL_CHANNEL_CAPACITY).unwrap() {
                assert_eq!(
                    worker.send_tbl_to_dump_historical_stats_outcome(id),
                    SendOutcome::Queued
                );
            }
            assert_eq!(
                worker.send_tbl_to_dump_historical_stats_outcome(99),
                SendOutcome::Dropped
            );
        });
    }

    #[test]
    fn empty_mailbox_yields_minus_one_not_absence() {
        let worker = HistoricalStatsWorker::new(());
        assert_eq!(
            worker.get_one_historical_stats_table(),
            NO_HISTORICAL_STATS_TABLE
        );
        assert_eq!(worker.try_recv_table(), None);
    }

    #[test]
    fn a_closed_mailbox_drains_then_yields_zero() {
        with_dump_switch(true, || {
            let worker = HistoricalStatsWorker::new(());
            worker.send_tbl_to_dump_historical_stats(5);
            worker.close_table_channel();
            assert_eq!(worker.get_one_historical_stats_table(), 5);
            // Go: a receive on a closed, drained channel succeeds with the
            // zero value, so the -1 sentinel is never reached again.
            assert_eq!(worker.get_one_historical_stats_table(), 0);
            assert_eq!(worker.try_recv_table(), Some(0));
        });
    }

    #[test]
    #[should_panic(expected = "send on closed channel")]
    fn sending_after_close_panics_like_go() {
        with_dump_switch(true, || {
            let worker = HistoricalStatsWorker::new(());
            worker.close_table_channel();
            worker.send_tbl_to_dump_historical_stats(1);
        });
    }

    #[test]
    #[should_panic(expected = "close of closed channel")]
    fn double_close_panics_like_go() {
        let worker = HistoricalStatsWorker::new(());
        worker.close_table_channel();
        worker.close_table_channel();
    }

    #[test]
    fn dump_returns_ok_without_writing_when_stats_are_disabled() {
        let session = FakeSession::new(schema_with_table(3, "t", "test"));
        let worker = HistoricalStatsWorker::new(&session);
        let handle = FakeHandle {
            enabled: Ok(false),
            ..FakeHandle::enabled()
        };
        let metrics = CountingMetrics::default();
        assert_eq!(worker.dump_historical_stats(3, &handle, &metrics), Ok(()));
        assert!(handle.calls.borrow().is_empty());
        assert_eq!(metrics.success.load(Ordering::SeqCst), 0);
        assert_eq!(metrics.failed.load(Ordering::SeqCst), 0);
        // The schema is not even read.
        assert_eq!(session.reads.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn dump_wraps_the_enable_check_error() {
        let session = FakeSession::new(FakeSchema::default());
        let worker = HistoricalStatsWorker::new(&session);
        let handle = FakeHandle {
            enabled: Err("boom".to_owned()),
            ..FakeHandle::enabled()
        };
        let err = worker
            .dump_historical_stats(3, &handle, &NoopHistoricalStatsMetrics)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "check tidb_enable_historical_stats failed: boom"
        );
    }

    #[test]
    fn dump_of_a_table_found_by_id_is_never_marked_a_partition() {
        let session = FakeSession::new(schema_with_table(3, "t", "test"));
        let worker = HistoricalStatsWorker::new(&session);
        let handle = FakeHandle::enabled();
        let metrics = CountingMetrics::default();
        assert_eq!(worker.dump_historical_stats(3, &handle, &metrics), Ok(()));
        assert_eq!(
            handle.calls.borrow().as_slice(),
            [("test".to_owned(), 3, false)]
        );
        assert_eq!(metrics.success.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.failed.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn dump_falls_back_to_the_partition_lookup() {
        let table = TableMeta {
            id: 3,
            name: "t".to_owned(),
        };
        let session = FakeSession::new(FakeSchema {
            by_id: Vec::new(),
            by_partition: vec![(11, table)],
            schemas: vec![(3, "test".to_owned())],
        });
        let worker = HistoricalStatsWorker::new(&session);
        let handle = FakeHandle::enabled();
        assert_eq!(
            worker.dump_historical_stats(11, &handle, &NoopHistoricalStatsMetrics),
            Ok(())
        );
        // Go passes the *partition* ID as the physical ID, with the parent
        // table's info and isPartition == true.
        assert_eq!(
            handle.calls.borrow().as_slice(),
            [("test".to_owned(), 11, true)]
        );
    }

    #[test]
    fn dump_reports_a_missing_table() {
        let session = FakeSession::new(FakeSchema::default());
        let worker = HistoricalStatsWorker::new(&session);
        let handle = FakeHandle::enabled();
        let err = worker
            .dump_historical_stats(-1, &handle, &NoopHistoricalStatsMetrics)
            .unwrap_err();
        assert_eq!(err, DumpHistoricalStatsError::TableNotFound(-1));
        assert_eq!(err.to_string(), "cannot get table by id -1");
    }

    #[test]
    fn dump_reports_a_missing_schema() {
        let session = FakeSession::new(FakeSchema {
            by_id: vec![TableMeta {
                id: 3,
                name: "t".to_owned(),
            }],
            by_partition: Vec::new(),
            schemas: Vec::new(),
        });
        let worker = HistoricalStatsWorker::new(&session);
        let handle = FakeHandle::enabled();
        let err = worker
            .dump_historical_stats(3, &handle, &NoopHistoricalStatsMetrics)
            .unwrap_err();
        assert_eq!(err, DumpHistoricalStatsError::SchemaNotFound(3));
        assert_eq!(err.to_string(), "cannot get DBInfo by TableID 3");
    }

    #[test]
    fn dump_reports_a_record_failure_and_counts_it() {
        let session = FakeSession::new(schema_with_table(3, "t", "test"));
        let worker = HistoricalStatsWorker::new(&session);
        let handle = FakeHandle {
            record: Err("no space".to_owned()),
            ..FakeHandle::enabled()
        };
        let metrics = CountingMetrics::default();
        let err = worker
            .dump_historical_stats(3, &handle, &metrics)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "record table test.t's historical stats failed, err:no space"
        );
        assert_eq!(metrics.failed.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.success.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn dump_rereads_the_schema_on_every_call() {
        let session = FakeSession::new(schema_with_table(3, "t", "test"));
        let worker = HistoricalStatsWorker::new(&session);
        let handle = FakeHandle::enabled();
        for _ in 0..3 {
            assert_eq!(
                worker.dump_historical_stats(3, &handle, &NoopHistoricalStatsMetrics),
                Ok(())
            );
        }
        assert_eq!(session.reads.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn session_accessor_returns_the_stored_session() {
        let worker = HistoricalStatsWorker::new(7_u8);
        assert_eq!(*worker.session(), 7);
    }
}
