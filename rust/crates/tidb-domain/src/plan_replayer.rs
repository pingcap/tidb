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

//! Go `pkg/domain/plan_replayer.go`: the bookkeeping around plan-replayer
//! dumps — which capture tasks are outstanding, which are running, which are
//! finished, what gets written into `mysql.plan_replayer_status`, and the
//! periodic GC of dump files on external storage.
//!
//! It does *not* include the dumping itself, which is
//! `plan_replayer_dump.go` (`DumpPlanReplayerInfo`, 1,004 lines) and is not
//! ported; it is reached here through one named boundary method.
//!
//! ## Symbols ported
//!
//! `parseTime` ([`parse_time`]), `dumpFileGcChecker` ([`DumpFileGcChecker`]),
//! `GCDumpFiles` ([`DumpFileGcChecker::gc_dump_files`]), `setupSctx`
//! ([`DumpFileGcChecker::setup_status_hook`]), `gcDumpFilesByPath`
//! ([`DumpFileGcChecker::gc_dump_files_by_path`]),
//! `deletePlanReplayerStatus` ([`delete_plan_replayer_status`]),
//! `insertPlanReplayerStatus` ([`insert_plan_replayer_status`]),
//! `insertPlanReplayerErrorStatusRecord`
//! ([`insert_plan_replayer_error_status_record`]),
//! `insertPlanReplayerSuccessStatusRecord`
//! ([`insert_plan_replayer_success_status_record`]), `planReplayerHandle`
//! ([`PlanReplayerHandle`]) and its `SendTask`
//! ([`PlanReplayerHandle::send_task`]), `planReplayerTaskCollectorHandle`
//! ([`PlanReplayerTaskCollectorHandle`]) with `CollectPlanReplayerTask`,
//! `GetTasks`, `setupTasks`, `removeTask` and `collectAllPlanReplayerTask`,
//! `planReplayerDumpTaskStatus` ([`PlanReplayerDumpTaskStatus`]) with all
//! seven of its methods, `planReplayerTaskDumpWorker`
//! ([`PlanReplayerTaskDumpWorker`]) with `run`, `handleTask` and
//! `HandleTask`, `planReplayerTaskDumpHandle`
//! ([`PlanReplayerTaskDumpHandle`]) with `GetTaskStatus`, `GetWorker`,
//! `Close` and `DrainTask`, `checkUnHandledReplayerTask`
//! ([`check_unhandled_replayer_task`]), `CheckPlanReplayerTaskExists`
//! ([`check_plan_replayer_task_exists`]), `PlanReplayerStatusRecord`, and
//! `PlanReplayerDumpTask`.
//!
//! ## Narrowings, all named
//!
//! - `// boundary:` Go `pkg/util/replayer.PlanReplayerTaskKey` →
//!   [`PlanReplayerTaskKey`]. `pkg/util/replayer` has no Rust home yet; the
//!   key is two strings and is reproduced here rather than left dangling, so
//!   the collapse is a re-export when that package lands.
//! - `// boundary:` Go `PlanReplayerDumpTask`'s dump payload — `TblStats`,
//!   `StartTS`, `SessionBindings`, `EncodedPlan`, `SessionVars`,
//!   `ExecStmts`, `Analyze`, `HistoricalStatsTS`, `DebugTrace` and `Zf
//!   io.WriteCloser` (`plan_replayer.go:576-590`). Not one of them is read by
//!   this file; every one is consumed by `DumpPlanReplayerInfo` in
//!   `plan_replayer_dump.go`. They are omitted from [`PlanReplayerDumpTask`]
//!   deliberately — carrying `*variable.SessionVars` or `[]ast.StmtNode`
//!   would drag half the workspace into this crate — and the writer that Go
//!   stores in `Zf` stays with the [`PlanReplayerDumper`] implementor, which
//!   is the only thing that ever writes to it.
//! - `// boundary:` Go `pkg/planner/extstore.GetGlobalExtStorage(ctx)` plus
//!   `storeapi.Storage.WalkDir`/`DeleteFile` → [`DumpFileStorage`]. Go's
//!   `WalkDir` takes a callback and a `*storeapi.WalkOption{SubDir: path}`;
//!   the callback here never returns a non-nil error, so listing the names
//!   first is equivalent. The per-file size the callback receives is unused.
//! - `// boundary:` Go `sessionctx.Context.GetSQLExecutor().ExecuteInternal`
//!   plus `sqlexec.DrainRecordSet(ctx, rs, 8)` → [`InternalSqlExecutor`].
//!   Both call sites branch on `rs == nil` and then on `len(rows) > 0`, and
//!   `rs == nil` means the *opposite* thing in the two of them — "assume
//!   unhandled" in `checkUnHandledReplayerTask`, "assume absent" in
//!   `CheckPlanReplayerTaskExists`. So the trait returns `Option<usize>`:
//!   `None` is Go's nil record set and `Some(n)` is the row count. An
//!   implementor cannot report "no record set" as "zero rows" by accident.
//! - `// boundary:` Go
//!   `sessionctx.Context.GetRestrictedSQLExecutor().ExecRestrictedSQL` under
//!   `kv.WithInternalSourceType(ctx, kv.InternalTxnStatsForegroundPriority)`
//!   → [`RestrictedSqlExecutor`]. The source type is a tracing tag; the
//!   implementor sets it.
//! - `// boundary:` Go `pkg/domain/infosync.GetServerInfo()` →
//!   [`ServerInfoSource`], which yields the `IP`/`Port` pair that
//!   `net.JoinHostPort` turns into the `instance` column. `infosync` is
//!   unported. The failure path is not narrowed: it becomes the literal
//!   `"unknown"`, as in Go.
//! - `// boundary:` Go `replayer.GeneratePlanReplayerFile` and
//!   `DumpPlanReplayerInfo` → [`PlanReplayerDumper`]. The first returns
//!   `(io.WriteCloser, fileName, error)`; here it returns just the file name
//!   and the implementor keeps the writer, since nothing in this file touches
//!   it beyond assigning `task.Zf`.
//! - `// boundary:` Go
//!   `vardef.EnableHistoricalStatsForCapture.Load()` — the process-global
//!   atomic behind `tidb_enable_historical_stats_for_capture`
//!   (`tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS_FOR_CAPTURE`).
//!   It is a parameter of [`PlanReplayerDumper::generate_plan_replayer_file`]
//!   rather than a global read, so the worker stays testable.
//! - `// boundary:` Go `pkg/domain/metrics` — `PlanReplayerCaptureTaskSendCounter`,
//!   `PlanReplayerCaptureTaskDiscardCounter` and `PlanReplayerRegisterTaskGauge`
//!   are dropped; no result depends on them. Each is named at its site.
//! - `// boundary:` Go `pkg/util.Recover(metrics.LabelDomain, ...)` in
//!   `handleTask` — Go swallows a panic in the worker so the loop survives.
//!   Rust has no equivalent to install here; a panicking dumper propagates.
//!   Named at the site so the `domain.go` batch can decide where the
//!   catch-unwind belongs.
//! - `// boundary:` Go `logutil.BgLogger()` — dropped throughout.
//!
//! ## Go behaviors reproduced rather than tidied
//!
//! 1. **The GC comparison is inclusive.** `canGC = !createTime.After(target)`
//!    is "created at or before the cutoff", so a file whose timestamp is
//!    exactly the cutoff is deleted. `GCDumpFiles(ctx, 0, 0)` therefore
//!    deletes everything, which is what upstream's
//!    `TestPlanReplayerDifferentGC` asserts. Reproduced and tested.
//! 2. **`capture_normal_replayer_*.zip` counts as a capture file.** The
//!    branch is `strings.Contains(baseName, "replayer") &&
//!    strings.Contains(baseName, "capture")`, and
//!    `replayer.generatePlanReplayerFileName` emits
//!    `capture_normal_replayer_...` when `isCapture` is set without
//!    historical stats. Such a file gets the *capture* retention, not the
//!    default one — upstream's own GC test depends on it.
//! 3. **Any file whose name merely contains `replayer` triggers a status
//!    delete.** `isPlanReplayer` is a substring test on the base name, not on
//!    the dump's provenance, and the token handed to the delete is that base
//!    name. An `extract_*.zip` is parsed for its time and GCed but leaves the
//!    status table alone.
//! 4. **`clearFinishedTask` runs once per deleted replayer file**, wiping the
//!    whole finished-task set each time, not just the entry for that file.
//! 5. **A failed status insert is retried once without `origin_sql`, and the
//!    retry's failure is only logged.** The record is then lost. Reproduced:
//!    [`insert_plan_replayer_success_status_record`] returns `Ok(())` in
//!    that case, exactly as Go returns nothing.
//! 6. **`SendTask` removes the task key from the collector only for
//!    non-continuous captures**, and only when the channel accepted the task;
//!    a full channel discards the task outright rather than blocking the
//!    query that produced it.
//! 7. **`handleTask` releases the running key only when it took it**, and
//!    marks a task finished only for continuous captures that succeeded — so
//!    a one-shot capture can be re-dumped, while a continuous one is
//!    suppressed until the next GC clears the set (behavior 4).
//! 8. **`HandleTask` reports success for an already-handled task.** When
//!    `checkUnHandledReplayerTask` says the task was processed, the worker
//!    returns `true` without dumping anything — which, for a continuous
//!    capture, also marks it finished.
//!
//! ## Where this port is deliberately more conservative than Go
//!
//! - `parseTime` returns Go's `time.Unix(0, i)` for *any* integer between the
//!   last `_` and the last `.`, including a negative one. That is kept, but
//!   an integer that does not fit in `i64` is an error here as it is in Go
//!   (`strconv.ParseInt(..., 10, 64)`), so a truncated or extended timestamp
//!   can never be read as a valid one — the case upstream's
//!   `TestDumpGCFileParseTime` pins with its `%v1.zip` name.
//! - Go's `p.sctx != nil` guards a block that also dereferences
//!   `p.planReplayerTaskStatus`, which is a *separate* nil-able field: a
//!   checker with a session but no status would panic there.
//!   [`DumpFileGcChecker`] takes the two together as one
//!   [`GcStatusHook`], so that state is unrepresentable.

use std::collections::HashSet;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError};
use std::sync::{Mutex, RwLock};

use chrono::{DateTime, Utc};

/// Go `"select sql_digest, plan_digest from mysql.plan_replayer_task"`
/// (`plan_replayer.go:305`).
pub const COLLECT_ALL_TASKS_SQL: &str =
    "select sql_digest, plan_digest from mysql.plan_replayer_task";

/// Go `"delete from mysql.plan_replayer_status where token = %?"`
/// (`plan_replayer.go:137`).
pub const DELETE_STATUS_BY_TOKEN_SQL: &str =
    "delete from mysql.plan_replayer_status where token = %?";

/// Go's error-record insert (`plan_replayer.go:167`).
pub const INSERT_ERROR_STATUS_SQL: &str = "insert into mysql.plan_replayer_status (sql_digest, plan_digest, origin_sql, fail_reason, instance) values (%?,%?,%?,%?,%?)";

/// Go's success-record insert (`plan_replayer.go:186`).
pub const INSERT_SUCCESS_STATUS_SQL: &str = "insert into mysql.plan_replayer_status (sql_digest, plan_digest, origin_sql, token, instance) values (%?,%?,%?,%?,%?)";

/// Go's success-record insert retried without `origin_sql`
/// (`plan_replayer.go:202`).
pub const INSERT_SUCCESS_STATUS_NO_SQL: &str = "insert into mysql.plan_replayer_status (sql_digest, plan_digest, token, instance) values (%?,%?,%?,%?)";

/// Go's `instance` value when `infosync.GetServerInfo` fails
/// (`plan_replayer.go:150`).
pub const UNKNOWN_INSTANCE: &str = "unknown";

/// The substring that marks a dump file as a plan-replayer file
/// (`plan_replayer.go:107`).
pub const REPLAYER_MARKER: &str = "replayer";

/// The substring that marks a dump file as a capture file
/// (`plan_replayer.go:108`).
pub const CAPTURE_MARKER: &str = "capture";

/// Errors this module can produce.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanReplayerError {
    /// Go `errors.New("failed to parse the file :" + s)` (`plan_replayer.go:62`).
    ParseFileTime(String),
    /// Anything a collaborator reports: the SQL executor, the storage, or the
    /// dumper. Go wraps some of these with `errors.Trace`, which adds no
    /// semantics.
    Other(String),
}

impl std::fmt::Display for PlanReplayerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParseFileTime(name) => write!(f, "failed to parse the file :{name}"),
            Self::Other(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for PlanReplayerError {}

/// Go `replayer.PlanReplayerTaskKey` (`pkg/util/replayer/replayer.go:36`).
///
/// boundary: `pkg/util/replayer` has no Rust home yet.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PlanReplayerTaskKey {
    /// Go `PlanReplayerTaskKey.SQLDigest`.
    pub sql_digest: String,
    /// Go `PlanReplayerTaskKey.PlanDigest`.
    pub plan_digest: String,
}

impl PlanReplayerTaskKey {
    /// Build a key from its two digests.
    pub fn new(sql_digest: impl Into<String>, plan_digest: impl Into<String>) -> Self {
        Self {
            sql_digest: sql_digest.into(),
            plan_digest: plan_digest.into(),
        }
    }
}

/// Go `PlanReplayerStatusRecord` (`plan_replayer.go:563`): one row of
/// `mysql.plan_replayer_status`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlanReplayerStatusRecord {
    /// Go `PlanReplayerStatusRecord.SQLDigest`.
    pub sql_digest: String,
    /// Go `PlanReplayerStatusRecord.PlanDigest`.
    pub plan_digest: String,
    /// Go `PlanReplayerStatusRecord.OriginSQL`.
    pub origin_sql: String,
    /// Go `PlanReplayerStatusRecord.Token`.
    pub token: String,
    /// Go `PlanReplayerStatusRecord.FailedReason`. A non-empty value routes
    /// the record to the error insert; Go tests `len(...) > 0`, so an empty
    /// string means success, not "no reason given".
    pub failed_reason: String,
}

/// Go `PlanReplayerDumpTask` (`plan_replayer.go:572`), reduced to the fields
/// this file reads. See the module doc for the payload fields left out and
/// why.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlanReplayerDumpTask {
    /// Go's embedded `replayer.PlanReplayerTaskKey`.
    pub key: PlanReplayerTaskKey,
    /// Go `PlanReplayerDumpTask.FileName`, filled in by the worker from
    /// `replayer.GeneratePlanReplayerFile`.
    pub file_name: Option<String>,
    /// Go `PlanReplayerDumpTask.PresignedURL`. Never read here; carried
    /// because it is a plain string and the dumper needs it.
    pub presigned_url: Option<String>,
    /// Go `PlanReplayerDumpTask.IsCapture`.
    pub is_capture: bool,
    /// Go `PlanReplayerDumpTask.IsContinuesCapture`.
    pub is_continues_capture: bool,
}

/// Go `infosync.ServerInfo`'s two fields that reach the `instance` column.
///
/// boundary: Go `pkg/domain/infosync.GetServerInfo()`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerInfo {
    /// Go `ServerInfo.IP`.
    pub ip: String,
    /// Go `ServerInfo.Port` (a `uint`, formatted with
    /// `strconv.FormatUint`).
    pub port: u64,
}

/// boundary: Go `infosync.GetServerInfo()`.
pub trait ServerInfoSource {
    /// # Errors
    /// Whatever `infosync` reports; the caller turns any error into
    /// [`UNKNOWN_INSTANCE`].
    fn get_server_info(&self) -> Result<ServerInfo, PlanReplayerError>;
}

/// boundary: Go `sessionctx.Context.GetSQLExecutor().ExecuteInternal(ctx,
/// sql)` followed by `sqlexec.DrainRecordSet(ctx, rs, 8)`.
///
/// Both methods return `None` for Go's `rs == nil` — a statement that
/// produced no record set at all, which the two call sites interpret in
/// opposite directions.
pub trait InternalSqlExecutor {
    /// Row count of a `select *` probe.
    ///
    /// # Errors
    /// Whatever the executor or the drain reports.
    fn query_row_count(&self, sql: &str) -> Result<Option<usize>, PlanReplayerError>;

    /// The `(sql_digest, plan_digest)` pairs of [`COLLECT_ALL_TASKS_SQL`],
    /// read with `row.GetString(0)` / `row.GetString(1)`.
    ///
    /// # Errors
    /// Whatever the executor or the drain reports.
    fn query_digest_pairs(
        &self,
        sql: &str,
    ) -> Result<Option<Vec<(String, String)>>, PlanReplayerError>;
}

/// boundary: Go `sessionctx.Context.GetRestrictedSQLExecutor().ExecRestrictedSQL`.
pub trait RestrictedSqlExecutor {
    /// Run a statement with its `%?` parameters, discarding the rows.
    ///
    /// # Errors
    /// Whatever the executor reports. Every caller in this file logs and
    /// continues.
    fn exec_restricted_sql(&self, sql: &str, params: &[&str]) -> Result<(), PlanReplayerError>;
}

/// boundary: Go `extstore.GetGlobalExtStorage(ctx)` plus the two
/// `storeapi.Storage` methods `gcDumpFilesByPath` uses.
pub trait DumpFileStorage {
    /// Go `storage.WalkDir(ctx, &storeapi.WalkOption{SubDir: path}, fn)`,
    /// collected into the file names the callback would have seen. Names are
    /// storage-relative paths; the caller takes `filepath.Base` of each.
    ///
    /// # Errors
    /// Whatever the walk reports. Go logs it and stops walking that path.
    fn walk_dir(&self, sub_dir: &str) -> Result<Vec<String>, PlanReplayerError>;

    /// Go `storage.DeleteFile(ctx, fileName)`.
    ///
    /// # Errors
    /// Whatever the storage reports. Go logs it and moves to the next file
    /// *without* touching the status table.
    fn delete_file(&self, file_name: &str) -> Result<(), PlanReplayerError>;
}

/// boundary: Go `replayer.GeneratePlanReplayerFile` and
/// `DumpPlanReplayerInfo` (`plan_replayer_dump.go`).
pub trait PlanReplayerDumper {
    /// Go `replayer.GeneratePlanReplayerFile(ctx, storage, isCapture,
    /// isContinuesCapture, vardef.EnableHistoricalStatsForCapture.Load())`,
    /// returning only the generated file name; the `io.WriteCloser` stays
    /// with the implementor.
    ///
    /// # Errors
    /// Whatever file creation reports.
    fn generate_plan_replayer_file(
        &self,
        is_capture: bool,
        is_continues_capture: bool,
        enable_historical_stats_for_capture: bool,
    ) -> Result<String, PlanReplayerError>;

    /// Go `DumpPlanReplayerInfo(ctx, sctx, task)`.
    ///
    /// # Errors
    /// Whatever the dump reports.
    fn dump_plan_replayer_info(&self, task: &PlanReplayerDumpTask)
        -> Result<(), PlanReplayerError>;
}

/// Go `parseTime` (`plan_replayer.go:59`): read the nanosecond timestamp that
/// a dump file name carries between its last `_` and its last `.`.
///
/// The three rejections are Go's, in Go's order: no `_`, no `.`, and a `.`
/// that is not strictly more than one character past the `_` — so an empty
/// number is rejected, and a `.` before the `_` is too.
///
/// # Errors
/// [`PlanReplayerError::ParseFileTime`] carrying the offending name, exactly
/// as Go's message does.
pub fn parse_time(s: &str) -> Result<DateTime<Utc>, PlanReplayerError> {
    let err = || PlanReplayerError::ParseFileTime(s.to_owned());
    let start_idx = s.rfind('_').ok_or_else(err)?;
    let end_idx = s.rfind('.').ok_or_else(err)?;
    // Go: `endIdx == -1 || endIdx <= startIdx+1`.
    if end_idx <= start_idx + 1 {
        return Err(err());
    }
    // Go `strconv.ParseInt(s[startIdx+1:endIdx], 10, 64)`: base 10, 64 bits,
    // so an over-long digit run fails rather than wrapping.
    let digits = &s[start_idx + 1..end_idx];
    let nanos: i64 = digits.parse().map_err(|_| err())?;
    // Go `time.Unix(0, i)`.
    Ok(DateTime::from_timestamp_nanos(nanos))
}

/// Go `filepath.Base(fileName)` as this file uses it: the segment after the
/// last separator.
///
/// boundary: Go's `filepath.Base` also collapses trailing separators and maps
/// an empty path to `"."`. Storage walk results are never empty and never end
/// in a separator, so the simple form is what this call site needs; anything
/// else would be inventing behavior for inputs that do not occur.
fn base_name(file_name: &str) -> &str {
    match file_name.rfind('/') {
        Some(idx) => &file_name[idx + 1..],
        None => file_name,
    }
}

/// Go `net.JoinHostPort(serverInfo.IP, strconv.FormatUint(port, 10))`.
///
/// The bracket rule is `net.JoinHostPort`'s: a host containing a colon — an
/// IPv6 literal — is wrapped, so the result stays parseable.
#[must_use]
pub fn join_host_port(ip: &str, port: u64) -> String {
    if ip.contains(':') {
        format!("[{ip}]:{port}")
    } else {
        format!("{ip}:{port}")
    }
}

/// The session-bound half of [`DumpFileGcChecker`].
///
/// Go keeps `sctx` and `planReplayerTaskStatus` as two independent nil-able
/// fields and guards the block on the first alone; pairing them here makes
/// the "session but no status" state — a nil dereference in Go —
/// unrepresentable.
pub struct GcStatusHook<S> {
    /// Go `dumpFileGcChecker.sctx`.
    pub sctx: S,
    /// Go `dumpFileGcChecker.planReplayerTaskStatus`.
    pub task_status: std::sync::Arc<PlanReplayerDumpTaskStatus>,
}

/// Go `dumpFileGcChecker` (`plan_replayer.go:51`): "used to gc dump file in
/// circle. For now it is used by `plan replayer` and `trace plan` statement".
pub struct DumpFileGcChecker<S> {
    /// Go `dumpFileGcChecker.gcLease`. Set by `domain.go` and not read by
    /// this file; kept so the field does not silently disappear.
    pub gc_lease: chrono::Duration,
    /// Go `dumpFileGcChecker.paths`.
    pub paths: Vec<String>,
    /// Go's `sctx` + `planReplayerTaskStatus` pair; `None` is Go's nil
    /// `sctx`, the state upstream's own GC test uses.
    pub status_hook: Option<GcStatusHook<S>>,
    /// Go's embedded `sync.Mutex`: `GCDumpFiles` serializes whole rounds.
    lock: Mutex<()>,
}

impl<S: RestrictedSqlExecutor> DumpFileGcChecker<S> {
    /// A checker over the given paths, with no session attached — the shape
    /// upstream's `TestPlanReplayerDifferentGC` builds.
    #[must_use]
    pub fn new(gc_lease: chrono::Duration, paths: Vec<String>) -> Self {
        Self {
            gc_lease,
            paths,
            status_hook: None,
            lock: Mutex::new(()),
        }
    }

    /// Go `setupSctx` (`plan_replayer.go:84`), which also implies the status
    /// handle — see [`GcStatusHook`].
    pub fn setup_status_hook(&mut self, hook: GcStatusHook<S>) {
        self.status_hook = Some(hook);
    }

    /// Go `GCDumpFiles` (`plan_replayer.go:76`): one GC round over every
    /// configured path, under the checker's own mutex.
    ///
    /// `now` stands for Go's two `time.Now()` calls inside
    /// `gcDumpFilesByPath`; taking one instant for the whole round means the
    /// two cutoffs are derived from the same clock reading rather than from
    /// two readings nanoseconds apart.
    ///
    /// A path whose walk fails is logged and skipped in Go; here the failure
    /// is returned per path in the result vector so nothing is silently lost,
    /// and the remaining paths are still processed.
    pub fn gc_dump_files<St: DumpFileStorage>(
        &self,
        storage: &St,
        now: DateTime<Utc>,
        gc_duration_default: chrono::Duration,
        gc_duration_for_capture: chrono::Duration,
    ) -> Vec<Result<(), PlanReplayerError>> {
        let _guard = self
            .lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.paths
            .iter()
            .map(|path| {
                self.gc_dump_files_by_path(
                    storage,
                    path,
                    now,
                    gc_duration_default,
                    gc_duration_for_capture,
                )
            })
            .collect()
    }

    /// Go `gcDumpFilesByPath` (`plan_replayer.go:88`).
    ///
    /// Per file: a name that will not parse is skipped (and logged in Go); a
    /// file at or before its cutoff is deleted; a delete failure skips the
    /// status cleanup for that file; a deleted *replayer* file removes its
    /// status row and clears the whole finished-task set.
    ///
    /// # Errors
    /// Only a failed walk. Every per-file failure is non-fatal in Go and
    /// stays non-fatal here.
    pub fn gc_dump_files_by_path<St: DumpFileStorage>(
        &self,
        storage: &St,
        path: &str,
        now: DateTime<Utc>,
        gc_duration_default: chrono::Duration,
        gc_duration_for_capture: chrono::Duration,
    ) -> Result<(), PlanReplayerError> {
        let gc_target_time_default = now - gc_duration_default;
        let gc_target_time_for_capture = now - gc_duration_for_capture;

        for file_name in storage.walk_dir(path)? {
            let base = base_name(&file_name);
            let Ok(create_time) = parse_time(base) else {
                // boundary: Go logs "parseTime failed" and returns nil from
                // the walk callback, i.e. keeps walking.
                continue;
            };
            let is_plan_replayer = base.contains(REPLAYER_MARKER);
            let is_plan_replayer_capture = base.contains(CAPTURE_MARKER);
            let can_gc = if is_plan_replayer && is_plan_replayer_capture {
                // Go `!createTime.After(target)`: at-or-before, inclusive.
                create_time <= gc_target_time_for_capture
            } else {
                create_time <= gc_target_time_default
            };
            if !can_gc {
                continue;
            }
            if storage.delete_file(&file_name).is_err() {
                // boundary: Go logs "remove file failed" and skips the status
                // cleanup for this file.
                continue;
            }
            if is_plan_replayer {
                if let Some(hook) = self.status_hook.as_ref() {
                    // The token is the base name, not the full path.
                    delete_plan_replayer_status(&hook.sctx, base);
                    hook.task_status.clear_finished_task();
                }
            }
        }
        Ok(())
    }
}

/// Go `deletePlanReplayerStatus` (`plan_replayer.go:134`).
///
/// Go logs a failure and returns nothing; the row survives and will be
/// retried on no schedule at all. Reproduced: the result is dropped here for
/// the same reason, and the drop is visible in this function's signature
/// rather than buried.
pub fn delete_plan_replayer_status<E: RestrictedSqlExecutor + ?Sized>(exec: &E, token: &str) {
    // boundary: Go logs "delete mysql.plan_replayer_status record failed".
    let _ = exec.exec_restricted_sql(DELETE_STATUS_BY_TOKEN_SQL, &[token]);
}

/// Go `insertPlanReplayerStatus` (`plan_replayer.go:144`): route each record
/// to the error or the success insert, under one shared `instance` value.
///
/// The `instance` is resolved once for the whole batch, and a failure to
/// resolve it is not fatal — every row is written with [`UNKNOWN_INSTANCE`].
pub fn insert_plan_replayer_status<E, I>(exec: &E, info: &I, records: &[PlanReplayerStatusRecord])
where
    E: RestrictedSqlExecutor + ?Sized,
    I: ServerInfoSource + ?Sized,
{
    let instance = match info.get_server_info() {
        Ok(server_info) => join_host_port(&server_info.ip, server_info.port),
        // boundary: Go logs "failed to get server info".
        Err(_) => UNKNOWN_INSTANCE.to_owned(),
    };
    for record in records {
        if record.failed_reason.is_empty() {
            insert_plan_replayer_success_status_record(exec, &instance, record);
        } else {
            insert_plan_replayer_error_status_record(exec, &instance, record);
        }
    }
}

/// Go `insertPlanReplayerErrorStatusRecord` (`plan_replayer.go:163`).
///
/// There is no retry here — unlike the success path, a failed error-record
/// insert is logged and dropped.
pub fn insert_plan_replayer_error_status_record<E: RestrictedSqlExecutor + ?Sized>(
    exec: &E,
    instance: &str,
    record: &PlanReplayerStatusRecord,
) {
    // boundary: Go logs "insert mysql.plan_replayer_status record failed".
    let _ = exec.exec_restricted_sql(
        INSERT_ERROR_STATUS_SQL,
        &[
            &record.sql_digest,
            &record.plan_digest,
            &record.origin_sql,
            &record.failed_reason,
            instance,
        ],
    );
}

/// Go `insertPlanReplayerSuccessStatusRecord` (`plan_replayer.go:181`).
///
/// The retry is the point: when the full insert fails — in practice because
/// `origin_sql` is too large for the column — Go retries the same record
/// without that column, and drops the record if that fails too. Reproduced,
/// including the "second failure is only logged" part.
pub fn insert_plan_replayer_success_status_record<E: RestrictedSqlExecutor + ?Sized>(
    exec: &E,
    instance: &str,
    record: &PlanReplayerStatusRecord,
) {
    let first = exec.exec_restricted_sql(
        INSERT_SUCCESS_STATUS_SQL,
        &[
            &record.sql_digest,
            &record.plan_digest,
            &record.origin_sql,
            &record.token,
            instance,
        ],
    );
    if first.is_err() {
        // boundary: Go logs the first failure, then "try insert record
        // without original sql".
        let _ = exec.exec_restricted_sql(
            INSERT_SUCCESS_STATUS_NO_SQL,
            &[
                &record.sql_digest,
                &record.plan_digest,
                &record.token,
                instance,
            ],
        );
    }
}

/// Go `checkUnHandledReplayerTask`'s statement (`plan_replayer.go:522`).
///
/// Built with `fmt.Sprintf` and `%v`, not bound parameters — Go's choice, and
/// the values are digests produced by TiDB itself.
#[must_use]
pub fn check_unhandled_replayer_task_sql(task: &PlanReplayerTaskKey) -> String {
    format!(
        "select * from mysql.plan_replayer_status where sql_digest = '{}' and plan_digest = '{}' and fail_reason is null",
        task.sql_digest, task.plan_digest
    )
}

/// Go `CheckPlanReplayerTaskExists`'s statement (`plan_replayer.go:543`).
#[must_use]
pub fn check_plan_replayer_task_exists_sql(sql_digest: &str, plan_digest: &str) -> String {
    format!(
        "select * from mysql.plan_replayer_task where sql_digest = '{sql_digest}' and plan_digest = '{plan_digest}'"
    )
}

/// Go `checkUnHandledReplayerTask` (`plan_replayer.go:520`): has this task
/// *not* been dumped successfully yet?
///
/// A nil record set counts as unhandled — Go returns `true, nil` — so a
/// statement that produced nothing at all makes the task eligible for
/// another dump rather than silently dropping it.
///
/// # Errors
/// Whatever the executor reports.
pub fn check_unhandled_replayer_task<E: InternalSqlExecutor + ?Sized>(
    exec: &E,
    task: &PlanReplayerTaskKey,
) -> Result<bool, PlanReplayerError> {
    match exec.query_row_count(&check_unhandled_replayer_task_sql(task))? {
        None => Ok(true),
        Some(rows) => Ok(rows == 0),
    }
}

/// Go `CheckPlanReplayerTaskExists` (`plan_replayer.go:541`): is there
/// already a capture task for this pair of digests?
///
/// Here a nil record set means "no" — the opposite reading of the same
/// condition, and the reason [`InternalSqlExecutor::query_row_count`] keeps
/// the two cases apart.
///
/// # Errors
/// Whatever the executor reports.
pub fn check_plan_replayer_task_exists<E: InternalSqlExecutor + ?Sized>(
    exec: &E,
    sql_digest: &str,
    plan_digest: &str,
) -> Result<bool, PlanReplayerError> {
    match exec.query_row_count(&check_plan_replayer_task_exists_sql(
        sql_digest,
        plan_digest,
    ))? {
        None => Ok(false),
        Some(rows) => Ok(rows > 0),
    }
}

/// Go `planReplayerDumpTaskStatus` (`plan_replayer.go:328`): the running and
/// finished task-key sets, each behind its own lock.
#[derive(Debug, Default)]
pub struct PlanReplayerDumpTaskStatus {
    /// Go `runningTaskMu.runningTasks`: "records the task running by all
    /// workers in order to avoid multi workers running the same task key".
    running_tasks: RwLock<HashSet<PlanReplayerTaskKey>>,
    /// Go `finishedTaskMu.finishedTask`: "records the finished task in order
    /// to avoid running finished task key".
    finished_task: RwLock<HashSet<PlanReplayerTaskKey>>,
}

impl PlanReplayerDumpTaskStatus {
    /// An empty status, as Go's zero value once its maps are made.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `GetRunningTaskStatusLen` — "used for unit test".
    #[must_use]
    pub fn running_task_status_len(&self) -> usize {
        self.read(&self.running_tasks).len()
    }

    /// Go `CleanFinishedTaskStatus` — "only used for unit test". Identical in
    /// effect to [`Self::clear_finished_task`]; both exist in Go and both are
    /// kept, because the two names are reached from different places.
    pub fn clean_finished_task_status(&self) {
        self.write(&self.finished_task).clear();
    }

    /// Go `GetFinishedTaskStatusLen` — "used for unit test".
    #[must_use]
    pub fn finished_task_status_len(&self) -> usize {
        self.read(&self.finished_task).len()
    }

    /// Go `occupyRunningTaskKey`: claim the key, or report that someone else
    /// holds it. The check and the insert are one critical section.
    pub fn occupy_running_task_key(&self, task: &PlanReplayerDumpTask) -> bool {
        self.write(&self.running_tasks).insert(task.key.clone())
    }

    /// Go `releaseRunningTaskKey`.
    pub fn release_running_task_key(&self, task: &PlanReplayerDumpTask) {
        self.write(&self.running_tasks).remove(&task.key);
    }

    /// Go `checkTaskKeyFinishedBefore`.
    #[must_use]
    pub fn check_task_key_finished_before(&self, task: &PlanReplayerDumpTask) -> bool {
        self.read(&self.finished_task).contains(&task.key)
    }

    /// Go `setTaskFinished`.
    pub fn set_task_finished(&self, task: &PlanReplayerDumpTask) {
        self.write(&self.finished_task).insert(task.key.clone());
    }

    /// Go `clearFinishedTask`: drop the whole set, which the GC does once per
    /// deleted replayer file.
    pub fn clear_finished_task(&self) {
        self.write(&self.finished_task).clear();
    }

    fn read<'a>(
        &self,
        lock: &'a RwLock<HashSet<PlanReplayerTaskKey>>,
    ) -> std::sync::RwLockReadGuard<'a, HashSet<PlanReplayerTaskKey>> {
        lock.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write<'a>(
        &self,
        lock: &'a RwLock<HashSet<PlanReplayerTaskKey>>,
    ) -> std::sync::RwLockWriteGuard<'a, HashSet<PlanReplayerTaskKey>> {
        lock.write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Go `planReplayerTaskCollectorHandle` (`plan_replayer.go:242`): the set of
/// capture tasks that still need dumping.
pub struct PlanReplayerTaskCollectorHandle<E> {
    /// Go `taskMu.tasks`.
    tasks: RwLock<HashSet<PlanReplayerTaskKey>>,
    /// Go's `sctx`, used only for the two queries. Go also carries a `ctx`,
    /// which is the request context and has no Rust counterpart here.
    pub sctx: E,
}

impl<E: InternalSqlExecutor> PlanReplayerTaskCollectorHandle<E> {
    /// A collector with an empty task set.
    pub fn new(sctx: E) -> Self {
        Self {
            tasks: RwLock::new(HashSet::new()),
            sctx,
        }
    }

    /// Go `CollectPlanReplayerTask` (`plan_replayer.go:252`): read every row
    /// of `mysql.plan_replayer_task`, keep the ones with no successful status
    /// row, and *replace* the task set with them.
    ///
    /// The replacement is wholesale, so a task that disappeared from the
    /// table also disappears from the set. A per-key check failure aborts the
    /// whole collection and leaves the previous set untouched, as in Go.
    ///
    /// # Errors
    /// Whatever the executor reports, from either query.
    pub fn collect_plan_replayer_task(&self) -> Result<(), PlanReplayerError> {
        let all_keys = self.collect_all_plan_replayer_task()?;
        let mut tasks = Vec::new();
        for key in all_keys {
            // boundary: Go logs "collect plan replayer task failed" and
            // returns the error, dropping the keys gathered so far.
            if check_unhandled_replayer_task(&self.sctx, &key)? {
                tasks.push(key);
            }
        }
        // boundary: Go `domain_metrics.PlanReplayerRegisterTaskGauge.Set(len(tasks))`.
        self.setup_tasks(tasks);
        Ok(())
    }

    /// Go `GetTasks` (`plan_replayer.go:277`). Go iterates a map, so the
    /// order is unspecified there; callers only ever count or scan.
    #[must_use]
    pub fn get_tasks(&self) -> Vec<PlanReplayerTaskKey> {
        self.tasks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect()
    }

    /// Go `setupTasks` (`plan_replayer.go:287`): build the new set outside
    /// the lock, then swap it in.
    pub fn setup_tasks(&self, tasks: Vec<PlanReplayerTaskKey>) {
        let new_set: HashSet<PlanReplayerTaskKey> = tasks.into_iter().collect();
        *self
            .tasks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = new_set;
    }

    /// Go `removeTask` (`plan_replayer.go:297`).
    pub fn remove_task(&self, task_key: &PlanReplayerTaskKey) {
        self.tasks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(task_key);
    }

    /// Go `collectAllPlanReplayerTask` (`plan_replayer.go:303`).
    ///
    /// Go returns `nil, nil` for a nil record set, and the caller then ranges
    /// over nil — zero keys. Modelled as an empty vector *at this boundary
    /// only*, because the caller cannot distinguish the two: the very next
    /// statement is a `for` over the slice.
    ///
    /// # Errors
    /// Whatever the executor reports.
    pub fn collect_all_plan_replayer_task(
        &self,
    ) -> Result<Vec<PlanReplayerTaskKey>, PlanReplayerError> {
        let Some(rows) = self.sctx.query_digest_pairs(COLLECT_ALL_TASKS_SQL)? else {
            return Ok(Vec::new());
        };
        Ok(rows
            .into_iter()
            .map(|(sql_digest, plan_digest)| PlanReplayerTaskKey {
                sql_digest,
                plan_digest,
            })
            .collect())
    }
}

/// Go `planReplayerTaskDumpWorker` (`plan_replayer.go:399`).
pub struct PlanReplayerTaskDumpWorker<E, D> {
    /// Go's `sctx`, used for the unhandled check and handed to the dumper.
    pub sctx: E,
    /// Go's `DumpPlanReplayerInfo` / file generation.
    pub dumper: D,
    /// Go's `status`.
    pub status: std::sync::Arc<PlanReplayerDumpTaskStatus>,
    /// boundary: Go `vardef.EnableHistoricalStatsForCapture.Load()`, read
    /// inside `HandleTask`. Kept as a field so no process-global is needed.
    enable_historical_stats: bool,
}

impl<E: InternalSqlExecutor, D: PlanReplayerDumper> PlanReplayerTaskDumpWorker<E, D> {
    /// A worker over a shared status.
    pub fn new(sctx: E, dumper: D, status: std::sync::Arc<PlanReplayerDumpTaskStatus>) -> Self {
        Self {
            sctx,
            dumper,
            status,
            enable_historical_stats: false,
        }
    }

    /// Go `(*planReplayerTaskDumpWorker).run` (`plan_replayer.go:406`): drain
    /// the channel until it is closed, handling one task at a time.
    ///
    /// boundary: Go logs "planReplayerTaskDumpWorker started./exited.".
    pub fn run(&self, task_ch: &Receiver<PlanReplayerDumpTask>) {
        while let Ok(task) = task_ch.recv() {
            self.handle_task(&task);
        }
    }

    /// Go `(*planReplayerTaskDumpWorker).handleTask` (`plan_replayer.go:414`):
    /// the three-gate choreography around a dump.
    ///
    /// Returns the `(check, occupy, handle)` triple that Go logs, because
    /// those three booleans are the whole observable outcome of this method
    /// and Go's own debug line is the only place they surface.
    ///
    /// boundary: Go `util.Recover(metrics.LabelDomain,
    /// "PlanReplayerTaskDumpWorker", nil, false)` — a panicking dump is
    /// swallowed there and propagates here.
    pub fn handle_task(&self, task: &PlanReplayerDumpTask) -> (bool, bool, bool) {
        let mut check = true;
        let mut occupy = true;
        let mut handle_task = true;

        if task.is_continues_capture && self.status.check_task_key_finished_before(task) {
            check = false;
            return (check, occupy, handle_task);
        }
        occupy = self.status.occupy_running_task_key(task);
        if !occupy {
            return (check, occupy, handle_task);
        }
        handle_task = self.handle_task_inner(task);
        self.status.release_running_task_key(task);
        (check, occupy, handle_task)
    }

    /// Go `(*planReplayerTaskDumpWorker).HandleTask` (`plan_replayer.go:445`):
    /// dump one task, reporting success.
    ///
    /// Every failure path answers `false` after logging — the task is neither
    /// marked finished nor retried here. An *already handled* task answers
    /// `true` without dumping, which for a continuous capture is what marks
    /// it finished.
    pub fn handle_task_inner(&self, task: &PlanReplayerDumpTask) -> bool {
        let success = self.dump(task);
        if success && task.is_continues_capture {
            self.status.set_task_finished(task);
        }
        success
    }

    fn dump(&self, task: &PlanReplayerDumpTask) -> bool {
        let unhandled = match check_unhandled_replayer_task(&self.sctx, &task.key) {
            Ok(unhandled) => unhandled,
            // boundary: Go logs "check task failed".
            Err(_) => return false,
        };
        // the task is processed, thus we directly skip it.
        if !unhandled {
            return true;
        }
        // boundary: Go also fetches the global external storage here and
        // fails the task when that fails; the storage is the dumper's here.
        let enable_historical_stats = self.enable_historical_stats_for_capture();
        let file_name = match self.dumper.generate_plan_replayer_file(
            task.is_capture,
            task.is_continues_capture,
            enable_historical_stats,
        ) {
            Ok(name) => name,
            // boundary: Go logs "generate task file failed".
            Err(_) => return false,
        };
        // Go assigns `task.Zf` and `task.FileName` before dumping.
        let mut dumped = task.clone();
        dumped.file_name = Some(file_name);
        // boundary: Go logs "dump task result failed".
        self.dumper.dump_plan_replayer_info(&dumped).is_ok()
    }

    /// boundary: Go `vardef.EnableHistoricalStatsForCapture.Load()`. There is
    /// no process-global sysvar state in this crate, so the value comes from
    /// the worker's own field, which `domain.go` will wire to the atomic.
    fn enable_historical_stats_for_capture(&self) -> bool {
        self.enable_historical_stats
    }
}

impl<E, D> PlanReplayerTaskDumpWorker<E, D> {
    /// Set the value this worker reads for
    /// `vardef.EnableHistoricalStatsForCapture`.
    pub fn set_enable_historical_stats_for_capture(&mut self, enabled: bool) {
        self.enable_historical_stats = enabled;
    }
}

/// Go `planReplayerTaskDumpHandle` (`plan_replayer.go:494`): the task channel
/// plus the shared status.
///
/// Go also holds `workers []*planReplayerTaskDumpWorker` so that `GetWorker`
/// can hand back `workers[0]` for tests; the workers own the receiving end of
/// the channel here instead, which is how a Rust `mpsc` fan-out is spelled.
pub struct PlanReplayerTaskDumpHandle {
    /// Go `taskCH` (sending end).
    task_ch: Option<SyncSender<PlanReplayerDumpTask>>,
    /// Go `status`.
    pub status: std::sync::Arc<PlanReplayerDumpTaskStatus>,
    /// The receiving end, until a worker takes it.
    receiver: Mutex<Option<Receiver<PlanReplayerDumpTask>>>,
}

impl PlanReplayerTaskDumpHandle {
    /// Go `make(chan *PlanReplayerDumpTask, capacity)` plus a fresh status.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel(capacity);
        Self {
            task_ch: Some(tx),
            status: std::sync::Arc::new(PlanReplayerDumpTaskStatus::new()),
            receiver: Mutex::new(Some(rx)),
        }
    }

    /// Go `GetTaskStatus` — "used for test", and by the GC checker.
    #[must_use]
    pub fn get_task_status(&self) -> std::sync::Arc<PlanReplayerDumpTaskStatus> {
        std::sync::Arc::clone(&self.status)
    }

    /// The receiving end of the channel, for the worker loop. Go's workers
    /// each hold `taskCH` directly; here it is taken once.
    pub fn take_receiver(&self) -> Option<Receiver<PlanReplayerDumpTask>> {
        self.receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    /// Go `Close`: `close(h.taskCH)`, which ends every worker's `range`.
    pub fn close(&mut self) {
        self.task_ch = None;
    }

    /// Go `DrainTask` — "drain a task for unit test". Blocks, as Go's `<-`
    /// does; `None` when the channel is closed and empty.
    #[must_use]
    pub fn drain_task(&self) -> Option<PlanReplayerDumpTask> {
        let guard = self
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.as_ref().and_then(|rx| rx.recv().ok())
    }
}

/// Go `planReplayerHandle` (`plan_replayer.go:217`): the collector and the
/// dump handle, embedded together so `SendTask` can reach both.
pub struct PlanReplayerHandle<E> {
    /// Go's embedded `*planReplayerTaskCollectorHandle`.
    pub collector: PlanReplayerTaskCollectorHandle<E>,
    /// Go's embedded `*planReplayerTaskDumpHandle`.
    pub dump_handle: PlanReplayerTaskDumpHandle,
}

impl<E: InternalSqlExecutor> PlanReplayerHandle<E> {
    /// Both halves of the handle.
    pub fn new(collector: PlanReplayerTaskCollectorHandle<E>, capacity: usize) -> Self {
        Self {
            collector,
            dump_handle: PlanReplayerTaskDumpHandle::new(capacity),
        }
    }

    /// Go `SendTask` (`plan_replayer.go:223`): offer the task to the dump
    /// channel without ever blocking.
    ///
    /// On success a *non-continuous* task is removed from the collector
    /// immediately — Go's comment: "if the task was failed to dump, the task
    /// handle will re-add the task in next loop". On a full channel the task
    /// is discarded so the query that produced it is never blocked.
    ///
    /// # Panics
    /// When the channel has been closed, mirroring Go's panic on "send on
    /// closed channel". A discarded task and a closed pipeline are different
    /// failures and are not collapsed into the same `false`.
    pub fn send_task(&self, task: PlanReplayerDumpTask) -> bool {
        let Some(tx) = self.dump_handle.task_ch.as_ref() else {
            panic!("plan replayer: send on closed task channel");
        };
        match tx.try_send(task.clone()) {
            Ok(()) => {
                if !task.is_continues_capture {
                    self.collector.remove_task(&task.key);
                }
                // boundary: Go `domain_metrics.PlanReplayerCaptureTaskSendCounter.Inc()`.
                true
            }
            Err(TrySendError::Full(_)) => {
                // boundary: Go `PlanReplayerCaptureTaskDiscardCounter.Inc()`
                // and a "discard one plan replayer dump task" warning.
                false
            }
            Err(TrySendError::Disconnected(_)) => {
                panic!("plan replayer: send on closed task channel")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::Arc;

    use super::*;

    /// Upstream `plan_replayer_test.go` is reachable in part:
    /// `TestDumpGCFileParseTime` and `TestPlanReplayerDifferentGC` are
    /// transcreated below against a scripted [`DumpFileStorage`], and
    /// `TestSendTask` against the real channel. `plan_replayer_handle_test.go`
    /// is testkit-bound end to end (`testkit.CreateMockStoreAndDomain`,
    /// `dom.GetPlanReplayerHandle()`, real `mysql.plan_replayer_task` rows);
    /// its *assertions* about task counting are transcreated against the
    /// scripted executor instead.
    #[derive(Default)]
    struct MockExec {
        /// Rows the `plan_replayer_task` select returns; `None` is a nil
        /// record set.
        task_rows: Option<Vec<(String, String)>>,
        /// Status rows per `sql_digest`; absent means zero rows.
        status_rows: std::collections::HashMap<String, usize>,
        /// A nil record set for the status probe.
        status_nil: bool,
        fail_query: bool,
        statements: RefCell<Vec<(String, Vec<String>)>>,
        fail_first_insert: bool,
    }

    impl InternalSqlExecutor for MockExec {
        fn query_row_count(&self, sql: &str) -> Result<Option<usize>, PlanReplayerError> {
            if self.fail_query {
                return Err(PlanReplayerError::Other("boom".to_owned()));
            }
            self.statements
                .borrow_mut()
                .push((sql.to_owned(), Vec::new()));
            if self.status_nil {
                return Ok(None);
            }
            let digest = sql
                .split("sql_digest = '")
                .nth(1)
                .and_then(|rest| rest.split('\'').next())
                .unwrap_or_default()
                .to_owned();
            Ok(Some(self.status_rows.get(&digest).copied().unwrap_or(0)))
        }

        fn query_digest_pairs(
            &self,
            sql: &str,
        ) -> Result<Option<Vec<(String, String)>>, PlanReplayerError> {
            assert_eq!(sql, COLLECT_ALL_TASKS_SQL);
            if self.fail_query {
                return Err(PlanReplayerError::Other("boom".to_owned()));
            }
            Ok(self.task_rows.clone())
        }
    }

    impl RestrictedSqlExecutor for MockExec {
        fn exec_restricted_sql(&self, sql: &str, params: &[&str]) -> Result<(), PlanReplayerError> {
            self.statements.borrow_mut().push((
                sql.to_owned(),
                params.iter().map(|p| (*p).to_owned()).collect(),
            ));
            if self.fail_first_insert && sql == INSERT_SUCCESS_STATUS_SQL {
                return Err(PlanReplayerError::Other("too long".to_owned()));
            }
            Ok(())
        }
    }

    struct MockInfo(Option<ServerInfo>);

    impl ServerInfoSource for MockInfo {
        fn get_server_info(&self) -> Result<ServerInfo, PlanReplayerError> {
            self.0
                .clone()
                .ok_or_else(|| PlanReplayerError::Other("no info".to_owned()))
        }
    }

    #[derive(Default)]
    struct MockStorage {
        files: RefCell<Vec<String>>,
        walk_err: bool,
        undeletable: Vec<String>,
    }

    impl DumpFileStorage for MockStorage {
        fn walk_dir(&self, sub_dir: &str) -> Result<Vec<String>, PlanReplayerError> {
            if self.walk_err {
                return Err(PlanReplayerError::Other("walk failed".to_owned()));
            }
            Ok(self
                .files
                .borrow()
                .iter()
                .filter(|f| f.starts_with(sub_dir))
                .cloned()
                .collect())
        }

        fn delete_file(&self, file_name: &str) -> Result<(), PlanReplayerError> {
            if self.undeletable.iter().any(|f| f == file_name) {
                return Err(PlanReplayerError::Other("locked".to_owned()));
            }
            self.files.borrow_mut().retain(|f| f != file_name);
            Ok(())
        }
    }

    fn nanos(t: DateTime<Utc>) -> i64 {
        t.timestamp_nanos_opt().unwrap()
    }

    /// TRANSCREATED from `TestDumpGCFileParseTime`
    /// (`plan_replayer_test.go:98`), including its four named file shapes.
    #[test]
    fn dump_gc_file_parse_time() {
        let now = Utc::now();
        let name1 = format!("replayer_single_xxxxxx_{}.zip", nanos(now));
        assert_eq!(parse_time(&name1).unwrap(), now);

        // One extra digit: 20 digits no longer fit in an i64.
        let name2 = format!("replayer_single_xxxxxx_{}1.zip", nanos(now));
        assert!(parse_time(&name2).is_err());

        // The last '_' now sits *after* the last '.'.
        let name3 = format!("replayer_single_xxxxxx_{}._zip", nanos(now));
        assert!(parse_time(&name3).is_err());

        let name4 = "extract_-brq6zKMarD9ayaifkHc4A==_1678168728477502000.zip";
        assert!(parse_time(name4).is_ok());

        // The four generated shapes of `replayer.generatePlanReplayerFileName`.
        for prefix in [
            "replayer",
            "capture_replayer",
            "capture_normal_replayer",
            "extract",
        ] {
            let name = format!("{prefix}_-brq6zKMarD9ayaifkHc4A==_{}.zip", nanos(now));
            assert_eq!(parse_time(&name).unwrap(), now, "{name}");
        }
    }

    /// WRITTEN. The three rejections, one at a time.
    #[test]
    fn parse_time_rejects_names_without_a_number_between_underscore_and_dot() {
        assert_eq!(
            parse_time("nounderscore.zip"),
            Err(PlanReplayerError::ParseFileTime(
                "nounderscore.zip".to_owned()
            ))
        );
        assert!(parse_time("replayer_123").is_err());
        // endIdx == startIdx + 1: an empty number.
        assert!(parse_time("replayer_.zip").is_err());
        assert!(parse_time("replayer_abc.zip").is_err());
        // Negative values parse, as Go's ParseInt accepts them.
        assert!(parse_time("replayer_-1000.zip").is_ok());
    }

    fn file_named(prefix: &str, t: DateTime<Utc>) -> String {
        format!("replayer/{prefix}_key_{}.zip", nanos(t))
    }

    /// TRANSCREATED from `TestPlanReplayerDifferentGC`
    /// (`plan_replayer_test.go:30`): four files at the upstream ages, the
    /// same `(1h, 7*24h)` cutoffs, the same four expectations, and the same
    /// follow-up `(0, 0)` round that must remove everything.
    #[test]
    fn plan_replayer_different_gc() {
        let now = Utc::now();
        let hour = chrono::Duration::hours(1);
        // isCapture=true, historical stats off -> "capture_normal_replayer".
        let f1 = file_named("capture_normal_replayer", now - hour * 7 * 25);
        let f2 = file_named("capture_normal_replayer", now - hour * 7 * 23);
        let f3 = file_named("replayer", now - hour * 2);
        let f4 = file_named("replayer", now);
        let storage = MockStorage {
            files: RefCell::new(vec![f1.clone(), f2.clone(), f3.clone(), f4.clone()]),
            ..MockStorage::default()
        };
        let checker: DumpFileGcChecker<MockExec> =
            DumpFileGcChecker::new(chrono::Duration::zero(), vec!["replayer".to_owned()]);

        for res in checker.gc_dump_files(&storage, now, hour, hour * 24 * 7) {
            res.unwrap();
        }
        let left = storage.files.borrow().clone();
        assert!(!left.contains(&f1), "capture file past its cutoff survives");
        assert!(left.contains(&f2), "capture file inside its cutoff removed");
        assert!(!left.contains(&f3), "plain file past its cutoff survives");
        assert!(left.contains(&f4));

        // Zero durations: the cutoff is `now`, and the comparison is
        // inclusive, so everything goes.
        for res in checker.gc_dump_files(
            &storage,
            now,
            chrono::Duration::zero(),
            chrono::Duration::zero(),
        ) {
            res.unwrap();
        }
        assert!(storage.files.borrow().is_empty());
    }

    /// WRITTEN. Module doc behavior 1: a file created exactly at the cutoff
    /// is deleted, one nanosecond later is not.
    #[test]
    fn the_gc_cutoff_is_inclusive_to_the_nanosecond() {
        let now = Utc::now();
        let cutoff = now - chrono::Duration::hours(1);
        let exact = file_named("replayer", cutoff);
        let after = file_named("replayer", cutoff + chrono::Duration::nanoseconds(1));
        let storage = MockStorage {
            files: RefCell::new(vec![exact.clone(), after.clone()]),
            ..MockStorage::default()
        };
        let checker: DumpFileGcChecker<MockExec> =
            DumpFileGcChecker::new(chrono::Duration::zero(), vec!["replayer".to_owned()]);
        checker
            .gc_dump_files_by_path(
                &storage,
                "replayer",
                now,
                chrono::Duration::hours(1),
                chrono::Duration::hours(1),
            )
            .unwrap();
        assert_eq!(storage.files.borrow().clone(), vec![after]);
    }

    /// WRITTEN. Module doc behaviors 3 and 4: deleting a replayer file
    /// deletes its status row (keyed by the *base* name) and clears the whole
    /// finished set; deleting a non-replayer file does neither.
    #[test]
    fn deleting_a_replayer_file_clears_its_status_and_the_finished_set() {
        let now = Utc::now();
        let old = now - chrono::Duration::hours(2);
        let replayer_file = file_named("replayer", old);
        let extract_file = file_named("extract", old);
        let storage = MockStorage {
            files: RefCell::new(vec![replayer_file.clone(), extract_file]),
            ..MockStorage::default()
        };
        let status = Arc::new(PlanReplayerDumpTaskStatus::new());
        status.set_task_finished(&PlanReplayerDumpTask {
            key: PlanReplayerTaskKey::new("a", "b"),
            ..PlanReplayerDumpTask::default()
        });
        let mut checker: DumpFileGcChecker<MockExec> =
            DumpFileGcChecker::new(chrono::Duration::zero(), vec!["replayer".to_owned()]);
        checker.setup_status_hook(GcStatusHook {
            sctx: MockExec::default(),
            task_status: Arc::clone(&status),
        });

        checker
            .gc_dump_files_by_path(
                &storage,
                "replayer",
                now,
                chrono::Duration::hours(1),
                chrono::Duration::hours(1),
            )
            .unwrap();

        assert!(storage.files.borrow().is_empty());
        assert_eq!(status.finished_task_status_len(), 0);
        let hook = checker.status_hook.as_ref().unwrap();
        let stmts = hook.sctx.statements.borrow();
        assert_eq!(
            stmts.len(),
            1,
            "only the replayer file deletes a status row"
        );
        assert_eq!(stmts[0].0, DELETE_STATUS_BY_TOKEN_SQL);
        assert_eq!(stmts[0].1, vec![base_name(&replayer_file).to_owned()]);
    }

    /// WRITTEN. A file that cannot be deleted leaves the status row alone.
    #[test]
    fn a_failed_delete_skips_the_status_cleanup() {
        let now = Utc::now();
        let stuck = file_named("replayer", now - chrono::Duration::hours(2));
        let storage = MockStorage {
            files: RefCell::new(vec![stuck.clone()]),
            undeletable: vec![stuck.clone()],
            ..MockStorage::default()
        };
        let mut checker: DumpFileGcChecker<MockExec> =
            DumpFileGcChecker::new(chrono::Duration::zero(), vec!["replayer".to_owned()]);
        checker.setup_status_hook(GcStatusHook {
            sctx: MockExec::default(),
            task_status: Arc::new(PlanReplayerDumpTaskStatus::new()),
        });
        checker
            .gc_dump_files_by_path(
                &storage,
                "replayer",
                now,
                chrono::Duration::hours(1),
                chrono::Duration::hours(1),
            )
            .unwrap();
        assert_eq!(storage.files.borrow().clone(), vec![stuck]);
        assert!(checker
            .status_hook
            .as_ref()
            .unwrap()
            .sctx
            .statements
            .borrow()
            .is_empty());
    }

    /// WRITTEN. An unparseable name is skipped, not deleted — the GC never
    /// removes a file it cannot date.
    #[test]
    fn an_unparseable_name_is_never_deleted() {
        let now = Utc::now();
        let storage = MockStorage {
            files: RefCell::new(vec!["replayer/garbage".to_owned()]),
            ..MockStorage::default()
        };
        let checker: DumpFileGcChecker<MockExec> =
            DumpFileGcChecker::new(chrono::Duration::zero(), vec!["replayer".to_owned()]);
        checker
            .gc_dump_files_by_path(
                &storage,
                "replayer",
                now,
                chrono::Duration::zero(),
                chrono::Duration::zero(),
            )
            .unwrap();
        assert_eq!(storage.files.borrow().len(), 1);
    }

    /// WRITTEN. A failing walk is reported per path and the other paths still
    /// run.
    #[test]
    fn a_failing_walk_is_reported_per_path() {
        let storage = MockStorage {
            walk_err: true,
            ..MockStorage::default()
        };
        let checker: DumpFileGcChecker<MockExec> = DumpFileGcChecker::new(
            chrono::Duration::zero(),
            vec!["a".to_owned(), "b".to_owned()],
        );
        let results = checker.gc_dump_files(
            &storage,
            Utc::now(),
            chrono::Duration::zero(),
            chrono::Duration::zero(),
        );
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(Result::is_err));
    }

    /// WRITTEN. `net.JoinHostPort` brackets an IPv6 literal.
    #[test]
    fn instance_is_host_and_port_with_ipv6_bracketed() {
        assert_eq!(join_host_port("10.0.0.1", 4000), "10.0.0.1:4000");
        assert_eq!(join_host_port("::1", 4000), "[::1]:4000");
    }

    /// WRITTEN. A record with a failure reason takes the error insert; one
    /// without takes the success insert. The empty string means "success",
    /// not "unknown reason".
    #[test]
    fn records_are_routed_by_their_failed_reason() {
        let exec = MockExec::default();
        let info = MockInfo(Some(ServerInfo {
            ip: "10.0.0.1".to_owned(),
            port: 4000,
        }));
        insert_plan_replayer_status(
            &exec,
            &info,
            &[
                PlanReplayerStatusRecord {
                    sql_digest: "s1".to_owned(),
                    plan_digest: "p1".to_owned(),
                    origin_sql: "select 1".to_owned(),
                    token: "t1".to_owned(),
                    failed_reason: String::new(),
                },
                PlanReplayerStatusRecord {
                    sql_digest: "s2".to_owned(),
                    plan_digest: "p2".to_owned(),
                    origin_sql: "select 2".to_owned(),
                    token: String::new(),
                    failed_reason: "nope".to_owned(),
                },
            ],
        );
        let stmts = exec.statements.borrow();
        assert_eq!(stmts[0].0, INSERT_SUCCESS_STATUS_SQL);
        assert_eq!(
            stmts[0].1,
            vec!["s1", "p1", "select 1", "t1", "10.0.0.1:4000"]
        );
        assert_eq!(stmts[1].0, INSERT_ERROR_STATUS_SQL);
        assert_eq!(
            stmts[1].1,
            vec!["s2", "p2", "select 2", "nope", "10.0.0.1:4000"]
        );
    }

    /// WRITTEN. Module doc behavior 5: the success insert retries once
    /// without `origin_sql`, and the failure of that retry is swallowed.
    #[test]
    fn a_failed_success_insert_is_retried_without_the_origin_sql() {
        let exec = MockExec {
            fail_first_insert: true,
            ..MockExec::default()
        };
        insert_plan_replayer_success_status_record(
            &exec,
            "inst",
            &PlanReplayerStatusRecord {
                sql_digest: "s".to_owned(),
                plan_digest: "p".to_owned(),
                origin_sql: "huge".to_owned(),
                token: "t".to_owned(),
                failed_reason: String::new(),
            },
        );
        let stmts = exec.statements.borrow();
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[1].0, INSERT_SUCCESS_STATUS_NO_SQL);
        assert_eq!(stmts[1].1, vec!["s", "p", "t", "inst"]);
    }

    /// WRITTEN. The error insert has no retry.
    #[test]
    fn the_error_insert_is_not_retried() {
        let exec = MockExec {
            fail_first_insert: true,
            ..MockExec::default()
        };
        insert_plan_replayer_error_status_record(
            &exec,
            "inst",
            &PlanReplayerStatusRecord {
                failed_reason: "boom".to_owned(),
                ..PlanReplayerStatusRecord::default()
            },
        );
        assert_eq!(exec.statements.borrow().len(), 1);
    }

    /// WRITTEN. An unavailable server info yields the literal "unknown".
    #[test]
    fn a_missing_server_info_writes_the_unknown_instance() {
        let exec = MockExec::default();
        insert_plan_replayer_status(
            &exec,
            &MockInfo(None),
            &[PlanReplayerStatusRecord::default()],
        );
        let stmts = exec.statements.borrow();
        assert_eq!(stmts[0].1.last().unwrap(), UNKNOWN_INSTANCE);
    }

    /// WRITTEN. The two nil-record-set readings, which are opposites.
    #[test]
    fn a_nil_record_set_means_unhandled_but_also_not_existing() {
        let exec = MockExec {
            status_nil: true,
            ..MockExec::default()
        };
        assert_eq!(
            check_unhandled_replayer_task(&exec, &PlanReplayerTaskKey::new("a", "b")),
            Ok(true)
        );
        assert_eq!(check_plan_replayer_task_exists(&exec, "a", "b"), Ok(false));
    }

    /// WRITTEN. With rows present the two answers flip.
    #[test]
    fn a_present_status_row_marks_the_task_handled() {
        let exec = MockExec {
            status_rows: [("a".to_owned(), 1usize)].into_iter().collect(),
            ..MockExec::default()
        };
        assert_eq!(
            check_unhandled_replayer_task(&exec, &PlanReplayerTaskKey::new("a", "b")),
            Ok(false)
        );
        assert_eq!(
            check_unhandled_replayer_task(&exec, &PlanReplayerTaskKey::new("z", "b")),
            Ok(true)
        );
        assert_eq!(check_plan_replayer_task_exists(&exec, "a", "b"), Ok(true));
    }

    /// TRANSCREATED from `TestPlanReplayerHandleCollectTask`
    /// (`plan_replayer_handle_test.go:30`): two task rows, one of which
    /// already has a status row, leaves exactly one collected task, and an
    /// empty task table leaves none.
    #[test]
    fn collect_keeps_only_the_unhandled_tasks() {
        let exec = MockExec {
            task_rows: Some(vec![
                ("123".to_owned(), "123".to_owned()),
                ("345".to_owned(), "345".to_owned()),
            ]),
            status_rows: [("123".to_owned(), 1usize)].into_iter().collect(),
            ..MockExec::default()
        };
        let handle = PlanReplayerTaskCollectorHandle::new(exec);
        handle.collect_plan_replayer_task().unwrap();
        assert_eq!(
            handle.get_tasks(),
            vec![PlanReplayerTaskKey::new("345", "345")]
        );

        let empty = PlanReplayerTaskCollectorHandle::new(MockExec {
            task_rows: Some(vec![]),
            ..MockExec::default()
        });
        empty.collect_plan_replayer_task().unwrap();
        assert!(empty.get_tasks().is_empty());
    }

    /// WRITTEN. A nil record set from the task select collects nothing, and a
    /// failing check leaves the previous set in place.
    #[test]
    fn a_failing_collection_leaves_the_previous_task_set() {
        let nil = PlanReplayerTaskCollectorHandle::new(MockExec::default());
        nil.collect_plan_replayer_task().unwrap();
        assert!(nil.get_tasks().is_empty());

        let handle = PlanReplayerTaskCollectorHandle::new(MockExec {
            task_rows: Some(vec![("1".to_owned(), "1".to_owned())]),
            fail_query: true,
            ..MockExec::default()
        });
        handle.setup_tasks(vec![PlanReplayerTaskKey::new("old", "old")]);
        assert!(handle.collect_plan_replayer_task().is_err());
        assert_eq!(
            handle.get_tasks(),
            vec![PlanReplayerTaskKey::new("old", "old")]
        );
    }

    /// WRITTEN. The task set is replaced wholesale, and `remove_task` takes
    /// one key out.
    #[test]
    fn setup_replaces_the_task_set_and_remove_takes_one_key() {
        let handle = PlanReplayerTaskCollectorHandle::new(MockExec::default());
        handle.setup_tasks(vec![
            PlanReplayerTaskKey::new("a", "a"),
            PlanReplayerTaskKey::new("b", "b"),
        ]);
        assert_eq!(handle.get_tasks().len(), 2);
        handle.remove_task(&PlanReplayerTaskKey::new("a", "a"));
        assert_eq!(handle.get_tasks(), vec![PlanReplayerTaskKey::new("b", "b")]);
        handle.setup_tasks(vec![PlanReplayerTaskKey::new("c", "c")]);
        assert_eq!(handle.get_tasks(), vec![PlanReplayerTaskKey::new("c", "c")]);
    }

    /// WRITTEN. The running set is a claim: the second occupant is refused
    /// until the first releases.
    #[test]
    fn a_running_task_key_can_only_be_occupied_once() {
        let status = PlanReplayerDumpTaskStatus::new();
        let task = PlanReplayerDumpTask {
            key: PlanReplayerTaskKey::new("s", "p"),
            ..PlanReplayerDumpTask::default()
        };
        assert!(status.occupy_running_task_key(&task));
        assert!(!status.occupy_running_task_key(&task));
        assert_eq!(status.running_task_status_len(), 1);
        status.release_running_task_key(&task);
        assert_eq!(status.running_task_status_len(), 0);
        assert!(status.occupy_running_task_key(&task));
    }

    /// WRITTEN. The finished set, and both of Go's clearing methods.
    #[test]
    fn the_finished_set_records_and_clears() {
        let status = PlanReplayerDumpTaskStatus::new();
        let task = PlanReplayerDumpTask {
            key: PlanReplayerTaskKey::new("s", "p"),
            ..PlanReplayerDumpTask::default()
        };
        assert!(!status.check_task_key_finished_before(&task));
        status.set_task_finished(&task);
        assert!(status.check_task_key_finished_before(&task));
        assert_eq!(status.finished_task_status_len(), 1);
        status.clean_finished_task_status();
        assert_eq!(status.finished_task_status_len(), 0);
        status.set_task_finished(&task);
        status.clear_finished_task();
        assert_eq!(status.finished_task_status_len(), 0);
    }

    struct MockDumper {
        generate_err: bool,
        dump_err: bool,
        generated: RefCell<Vec<(bool, bool, bool)>>,
        dumped: RefCell<Vec<PlanReplayerDumpTask>>,
    }

    impl Default for MockDumper {
        fn default() -> Self {
            Self {
                generate_err: false,
                dump_err: false,
                generated: RefCell::new(Vec::new()),
                dumped: RefCell::new(Vec::new()),
            }
        }
    }

    impl PlanReplayerDumper for MockDumper {
        fn generate_plan_replayer_file(
            &self,
            is_capture: bool,
            is_continues_capture: bool,
            enable_historical_stats_for_capture: bool,
        ) -> Result<String, PlanReplayerError> {
            self.generated.borrow_mut().push((
                is_capture,
                is_continues_capture,
                enable_historical_stats_for_capture,
            ));
            if self.generate_err {
                return Err(PlanReplayerError::Other("no file".to_owned()));
            }
            Ok("replayer_key_1.zip".to_owned())
        }

        fn dump_plan_replayer_info(
            &self,
            task: &PlanReplayerDumpTask,
        ) -> Result<(), PlanReplayerError> {
            self.dumped.borrow_mut().push(task.clone());
            if self.dump_err {
                return Err(PlanReplayerError::Other("dump failed".to_owned()));
            }
            Ok(())
        }
    }

    fn worker(
        exec: MockExec,
        dumper: MockDumper,
    ) -> PlanReplayerTaskDumpWorker<MockExec, MockDumper> {
        PlanReplayerTaskDumpWorker::new(exec, dumper, Arc::new(PlanReplayerDumpTaskStatus::new()))
    }

    /// WRITTEN. Module doc behavior 7: a continuous capture that already
    /// finished never even tries to occupy the key.
    #[test]
    fn a_finished_continuous_capture_is_skipped_before_occupying() {
        let w = worker(MockExec::default(), MockDumper::default());
        let task = PlanReplayerDumpTask {
            key: PlanReplayerTaskKey::new("s", "p"),
            is_continues_capture: true,
            ..PlanReplayerDumpTask::default()
        };
        w.status.set_task_finished(&task);
        assert_eq!(w.handle_task(&task), (false, true, true));
        assert_eq!(w.status.running_task_status_len(), 0);
        assert!(w.dumper.dumped.borrow().is_empty());
    }

    /// WRITTEN. A key already claimed by another worker is dropped without
    /// releasing it — the claim belongs to the other worker.
    #[test]
    fn an_occupied_key_is_left_to_its_owner() {
        let w = worker(MockExec::default(), MockDumper::default());
        let task = PlanReplayerDumpTask {
            key: PlanReplayerTaskKey::new("s", "p"),
            ..PlanReplayerDumpTask::default()
        };
        assert!(w.status.occupy_running_task_key(&task));
        assert_eq!(w.handle_task(&task), (true, false, true));
        assert_eq!(w.status.running_task_status_len(), 1);
    }

    /// WRITTEN. A successful continuous capture is marked finished; a
    /// successful one-shot capture is not.
    #[test]
    fn only_a_continuous_capture_is_marked_finished() {
        let w = worker(MockExec::default(), MockDumper::default());
        let one_shot = PlanReplayerDumpTask {
            key: PlanReplayerTaskKey::new("s", "p"),
            is_capture: true,
            ..PlanReplayerDumpTask::default()
        };
        assert_eq!(w.handle_task(&one_shot), (true, true, true));
        assert_eq!(w.status.finished_task_status_len(), 0);
        assert_eq!(w.status.running_task_status_len(), 0);

        let continuous = PlanReplayerDumpTask {
            key: PlanReplayerTaskKey::new("s2", "p2"),
            is_continues_capture: true,
            ..PlanReplayerDumpTask::default()
        };
        assert_eq!(w.handle_task(&continuous), (true, true, true));
        assert_eq!(w.status.finished_task_status_len(), 1);
    }

    /// WRITTEN. Module doc behavior 8: an already-handled task reports
    /// success without dumping — and so marks a continuous capture finished.
    #[test]
    fn an_already_handled_task_succeeds_without_dumping() {
        let exec = MockExec {
            status_rows: [("s".to_owned(), 1usize)].into_iter().collect(),
            ..MockExec::default()
        };
        let w = worker(exec, MockDumper::default());
        let task = PlanReplayerDumpTask {
            key: PlanReplayerTaskKey::new("s", "p"),
            is_continues_capture: true,
            ..PlanReplayerDumpTask::default()
        };
        assert_eq!(w.handle_task(&task), (true, true, true));
        assert!(w.dumper.dumped.borrow().is_empty());
        assert_eq!(w.status.finished_task_status_len(), 1);
    }

    /// WRITTEN. Each failure path answers false and leaves the key
    /// unfinished, but always releases the running claim.
    #[test]
    fn every_dump_failure_releases_the_key_without_finishing_it() {
        for (exec, dumper) in [
            (
                MockExec {
                    fail_query: true,
                    ..MockExec::default()
                },
                MockDumper::default(),
            ),
            (
                MockExec::default(),
                MockDumper {
                    generate_err: true,
                    ..MockDumper::default()
                },
            ),
            (
                MockExec::default(),
                MockDumper {
                    dump_err: true,
                    ..MockDumper::default()
                },
            ),
        ] {
            let w = worker(exec, dumper);
            let task = PlanReplayerDumpTask {
                key: PlanReplayerTaskKey::new("s", "p"),
                is_continues_capture: true,
                ..PlanReplayerDumpTask::default()
            };
            assert_eq!(w.handle_task(&task), (true, true, false));
            assert_eq!(w.status.finished_task_status_len(), 0);
            assert_eq!(w.status.running_task_status_len(), 0);
        }
    }

    /// WRITTEN. The generated file name lands on the task the dumper sees,
    /// and the capture flags plus the sysvar reach the generator unchanged.
    #[test]
    fn the_generated_file_name_reaches_the_dumper() {
        let mut w = worker(MockExec::default(), MockDumper::default());
        w.set_enable_historical_stats_for_capture(true);
        let task = PlanReplayerDumpTask {
            key: PlanReplayerTaskKey::new("s", "p"),
            is_capture: true,
            ..PlanReplayerDumpTask::default()
        };
        assert!(w.handle_task_inner(&task));
        assert_eq!(*w.dumper.generated.borrow(), vec![(true, false, true)]);
        assert_eq!(
            w.dumper.dumped.borrow()[0].file_name.as_deref(),
            Some("replayer_key_1.zip")
        );
    }

    /// TRANSCREATED from `TestSendTask` (`plan_replayer_test.go:161`): a
    /// channel of capacity one accepts the first task and refuses the second.
    #[test]
    fn send_task_discards_when_the_channel_is_full() {
        let h =
            PlanReplayerHandle::new(PlanReplayerTaskCollectorHandle::new(MockExec::default()), 1);
        assert!(h.send_task(PlanReplayerDumpTask::default()));
        assert!(!h.send_task(PlanReplayerDumpTask::default()));
    }

    /// WRITTEN. Module doc behavior 6: a delivered one-shot task leaves the
    /// collector; a continuous one stays for the next round.
    #[test]
    fn only_a_non_continuous_task_is_removed_from_the_collector() {
        let h =
            PlanReplayerHandle::new(PlanReplayerTaskCollectorHandle::new(MockExec::default()), 4);
        let one_shot = PlanReplayerTaskKey::new("a", "a");
        let continuous = PlanReplayerTaskKey::new("b", "b");
        h.collector
            .setup_tasks(vec![one_shot.clone(), continuous.clone()]);

        assert!(h.send_task(PlanReplayerDumpTask {
            key: one_shot.clone(),
            ..PlanReplayerDumpTask::default()
        }));
        assert!(h.send_task(PlanReplayerDumpTask {
            key: continuous.clone(),
            is_continues_capture: true,
            ..PlanReplayerDumpTask::default()
        }));
        assert_eq!(h.collector.get_tasks(), vec![continuous]);

        // A discarded task is not removed either: the channel is the gate.
        let full =
            PlanReplayerHandle::new(PlanReplayerTaskCollectorHandle::new(MockExec::default()), 1);
        full.collector.setup_tasks(vec![one_shot.clone()]);
        assert!(full.send_task(PlanReplayerDumpTask {
            key: PlanReplayerTaskKey::new("z", "z"),
            ..PlanReplayerDumpTask::default()
        }));
        assert!(!full.send_task(PlanReplayerDumpTask {
            key: one_shot.clone(),
            ..PlanReplayerDumpTask::default()
        }));
        assert_eq!(full.collector.get_tasks(), vec![one_shot]);
    }

    /// WRITTEN. `DrainTask` hands back what was sent, in order.
    #[test]
    fn drain_task_returns_the_sent_tasks_in_order() {
        let h =
            PlanReplayerHandle::new(PlanReplayerTaskCollectorHandle::new(MockExec::default()), 2);
        h.send_task(PlanReplayerDumpTask {
            key: PlanReplayerTaskKey::new("1", "1"),
            ..PlanReplayerDumpTask::default()
        });
        h.send_task(PlanReplayerDumpTask {
            key: PlanReplayerTaskKey::new("2", "2"),
            ..PlanReplayerDumpTask::default()
        });
        assert_eq!(
            h.dump_handle.drain_task().unwrap().key,
            PlanReplayerTaskKey::new("1", "1")
        );
        assert_eq!(
            h.dump_handle.drain_task().unwrap().key,
            PlanReplayerTaskKey::new("2", "2")
        );
    }

    /// WRITTEN. A closed handle panics on send, as Go does.
    #[test]
    #[should_panic(expected = "send on closed task channel")]
    fn sending_on_a_closed_channel_panics() {
        let mut h =
            PlanReplayerHandle::new(PlanReplayerTaskCollectorHandle::new(MockExec::default()), 1);
        h.dump_handle.close();
        h.send_task(PlanReplayerDumpTask::default());
    }

    /// WRITTEN. The worker loop drains until the sender is gone.
    #[test]
    fn the_worker_loop_drains_the_channel_and_exits_on_close() {
        let (tx, rx) = std::sync::mpsc::sync_channel(2);
        tx.send(PlanReplayerDumpTask {
            key: PlanReplayerTaskKey::new("a", "a"),
            ..PlanReplayerDumpTask::default()
        })
        .unwrap();
        tx.send(PlanReplayerDumpTask {
            key: PlanReplayerTaskKey::new("b", "b"),
            ..PlanReplayerDumpTask::default()
        })
        .unwrap();
        drop(tx);
        let w = worker(MockExec::default(), MockDumper::default());
        w.run(&rx);
        assert_eq!(w.dumper.dumped.borrow().len(), 2);
    }

    /// WRITTEN. `filepath.Base` as this file needs it.
    #[test]
    fn base_name_takes_the_last_segment() {
        assert_eq!(base_name("replayer/replayer_k_1.zip"), "replayer_k_1.zip");
        assert_eq!(base_name("replayer_k_1.zip"), "replayer_k_1.zip");
    }
}
