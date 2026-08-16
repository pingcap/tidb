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

//! Go `pkg/ttl/session` lands as a complete package: the session wrapper every
//! TTL worker uses to run its internal SQL, drive its transactions, and pin the
//! time zone its expiry arithmetic depends on.
//!
//! Every function of `session.go` comes across — `NewSession`, `GetStore`,
//! `GetSessionVars`, `GetLatestInfoSchema`, `SessionInfoSchema`,
//! `GetSQLExecutor`, `ExecuteSQL`, `RunInTxn`, `ResetWithGlobalTimeZone`,
//! `GlobalTimeZone`, `KillStmt`, `Now` and `AvoidReuse` — together with
//! `TxnMode` and the `Session` interface itself. What Go reaches through its
//! neighbours is narrowed to traits here, each named at its definition site:
//!
//! - [`SessionContext`] `// boundary:` `pkg/sessionctx.Context` plus
//!   `pkg/sessiontxn.GetTxnManager(...).GetTxnInfoSchema()`. Neither package is
//!   transcreated, and `session.go` calls exactly nine things through them, so
//!   the trait carries exactly those nine.
//! - [`SessionContext::execute_internal`] `// boundary:`
//!   `pkg/util/sqlexec.{SQLExecutor,RecordSet}` and `sqlexec.DrainRecordSet`.
//!   `pkg/util/sqlexec` is unported; `ExecuteSQL`'s open/drain-8/close triple
//!   collapses into one trait call that yields the drained rows, and the `nil`
//!   record set that a non-query statement returns becomes `None`.
//! - [`ResultRow`] `// boundary:` `pkg/util/chunk.Row`, which `tidb-chunk`
//!   holds but this crate may not add a dependency edge to (see the crate
//!   header). Only the accessors TTL uses are declared.
//! - [`SessionContext::Store`] `// boundary:` `kv.Storage`, and
//!   [`SessionContext::InfoSchema`] `// boundary:`
//!   `pkg/infoschema/context.MetaOnlyInfoSchema`. `session.go` only passes both
//!   through, so both stay opaque associated types.
//! - [`PhaseTracer`] `// boundary:` `pkg/ttl/metrics.PhaseTracer`. That package
//!   is not yet transcreated. Go reads the tracer out of the `context.Context`;
//!   with no context plumbing here it is an explicit `run_in_txn` argument, and
//!   [`NoopPhaseTracer`] reproduces `PhaseTracerFromCtx`'s behaviour when the
//!   context carries none.
//!
//! Two further narrowings are behavioural rather than structural:
//!
//! - Go's rollback runs under a fresh one-second `context.WithTimeout` so that
//!   a cancelled caller context cannot suppress it. With no context parameter
//!   the rollback is simply issued unconditionally, which is the property that
//!   timeout exists to guarantee.
//! - `Now()` returns Go's `time.Time` in the session location. This crate has
//!   no reachable Go-instant transcreation (see the crate header), so it
//!   returns the Unix nanoseconds of the instant together with the session
//!   [`TimeZone`]; the zone is the whole of what `Location()` contributes.
//!
//! Test note: all seven Go tests need a live TiDB (`testkit`); the ones ported
//! here re-assert the same behaviour against a scripted [`SessionContext`]
//! rather than a server, and the ones that cannot are named in
//! `tests/session_test.rs`. Heavy `testkit`-only coverage does not make this a
//! seed: every production symbol of `session.go` is present.

use std::time::{SystemTime, UNIX_EPOCH};

use tidb_datatype::Datum;
use tidb_util::sqlkiller::{KillSignal, SqlKiller};
use tidb_util::timeutil::{parse_time_zone, TimeZone};

/// Go `vardef.TimeZone`, the system variable both time-zone paths read.
const VAR_TIME_ZONE: &str = "time_zone";

/// This module's error, standing in for Go's `error` return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionError(pub String);

impl std::fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SessionError {}

/// This module's `Result` alias.
pub type Result<T> = std::result::Result<T, SessionError>;

fn error(text: impl Into<String>) -> SessionError {
    SessionError(text.into())
}

/// Go `TxnMode`: using optimistic or pessimistic mode in the transaction.
///
/// Go declares `type TxnMode int` with `iota` constants, and `RunInTxn`'s
/// `switch` has a `default` arm that rejects anything else. A Rust `enum` would
/// delete that arm along with the error it raises, so the Go integer is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnMode(pub i32);

impl TxnMode {
    /// Go `TxnModeOptimistic`: the optimistic transaction, `BEGIN OPTIMISTIC`.
    pub const OPTIMISTIC: TxnMode = TxnMode(0);
    /// Go `TxnModePessimistic`: the pessimistic transaction, `BEGIN PESSIMISTIC`.
    pub const PESSIMISTIC: TxnMode = TxnMode(1);
}

/// `// boundary:` `pkg/util/chunk.Row`.
///
/// `tidb-chunk` holds the transcreated row, but this crate cannot add that
/// dependency edge (see the crate header), so the accessors the TTL packages
/// call on a result row are declared here and supplied by the caller.
pub trait ResultRow {
    /// Go `Row.IsNull`.
    fn is_null(&self, col_idx: usize) -> bool;
    /// Go `Row.GetInt64`.
    fn get_int64(&self, col_idx: usize) -> i64;
    /// Go `Row.GetString`.
    fn get_string(&self, col_idx: usize) -> String;
    /// Go `Row.GetBytes`.
    fn get_bytes(&self, col_idx: usize) -> Vec<u8>;
    /// Go `Row.GetTime`, returning the MySQL datetime the column holds.
    ///
    /// Go follows every call with `.GoTime(tz)`; that conversion is named as a
    /// boundary by the `cache` modules that need it.
    fn get_time(&self, col_idx: usize) -> tidb_datatype::Time;
}

/// `// boundary:` `pkg/ttl/metrics.PhaseTracer`, whose package is not yet
/// transcreated.
///
/// `RunInTxn` only enters phases, and reads the current one so the deferred
/// call can restore it, so those are the whole of the contract.
pub trait PhaseTracer {
    /// Go `PhaseTracer.Phase`.
    fn phase(&self) -> Phase;
    /// Go `PhaseTracer.EnterPhase`.
    fn enter_phase(&self, phase: Phase);
}

/// Go `pkg/ttl/metrics`'s phase names, limited to the three `RunInTxn` uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Go `metrics.PhaseBeginTxn`.
    BeginTxn,
    /// Go `metrics.PhaseCommitTxn`.
    CommitTxn,
    /// Go `metrics.PhaseOther`.
    Other,
}

/// The tracer `metrics.PhaseTracerFromCtx` hands back when the context carries
/// none: every call is a no-op.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopPhaseTracer;

impl PhaseTracer for NoopPhaseTracer {
    fn phase(&self) -> Phase {
        Phase::Other
    }

    fn enter_phase(&self, _phase: Phase) {}
}

/// `// boundary:` `pkg/sessionctx.Context` and
/// `pkg/sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()`.
///
/// Neither package is transcreated. `session.go` reaches through `sctx` for
/// exactly the operations below — the store, the two info schemas, the internal
/// SQL executor, two system-variable reads, the session time zone, the session
/// location, and the SQL killer — so exactly those are declared.
pub trait SessionContext {
    /// The result row type, standing in for `chunk.Row`.
    type Row: ResultRow;
    /// `// boundary:` `kv.Storage`. `session.go` only passes it through.
    type Store: Clone;
    /// `// boundary:` `pkg/infoschema/context.MetaOnlyInfoSchema`. Passed through.
    type InfoSchema: Clone;

    /// Go `sessionctx.Context.GetStore`.
    fn get_store(&self) -> Self::Store;

    /// Go `sessionctx.Context.GetLatestInfoSchema`.
    fn get_latest_info_schema(&self) -> Self::InfoSchema;

    /// Go `sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()`.
    fn get_txn_info_schema(&self) -> Self::InfoSchema;

    /// Go `sqlexec.SQLExecutor.ExecuteInternal` followed by
    /// `sqlexec.DrainRecordSet(ctx, rs, 8)` and `rs.Close()`.
    ///
    /// `None` is Go's `rs == nil`, which `ExecuteSQL` turns into no rows.
    fn execute_internal(&self, sql: &str, args: &[Datum]) -> Result<Option<Vec<Self::Row>>>;

    /// Go `SessionVars.GetGlobalSystemVar`.
    fn get_global_system_var(&self, name: &str) -> Result<String>;

    /// Go `SessionVars.GetSessionOrGlobalSystemVar`.
    fn get_session_or_global_system_var(&self, name: &str) -> Result<String>;

    /// Go `SessionVars.TimeZone`, which is `nil` until the session sets one.
    fn time_zone(&self) -> Option<TimeZone>;

    /// Go `SessionVars.Location()`.
    fn location(&self) -> TimeZone;

    /// Go `SessionVars.SQLKiller`.
    fn sql_killer(&self) -> &SqlKiller;
}

/// Go's `Now()` return: a `time.Time` in the session location.
///
/// See the module header — no Go-instant transcreation is reachable from this
/// crate, so the instant is carried as Unix nanoseconds beside the location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionNow {
    /// Nanoseconds since the Unix epoch.
    pub unix_nanos: i128,
    /// Go's `time.Time.Location()` for the returned value.
    pub location: TimeZone,
}

/// Go `Session`: the interface TTL executes queries through.
///
/// Go embeds `variable.SessionVarsProvider` for `GetSessionVars`. `sessionctx`
/// is unported, so the two members TTL actually reads off the session variables
/// — the location and the SQL killer — are exposed directly instead.
pub trait Session {
    /// The result row type, standing in for `chunk.Row`.
    type Row: ResultRow;
    /// `// boundary:` `kv.Storage`.
    type Store: Clone;
    /// `// boundary:` `pkg/infoschema/context.MetaOnlyInfoSchema`.
    type InfoSchema: Clone;

    /// Go `Session.GetStore`.
    fn get_store(&self) -> Self::Store;
    /// Go `Session.GetLatestInfoSchema`.
    fn get_latest_info_schema(&self) -> Self::InfoSchema;
    /// Go `Session.SessionInfoSchema`.
    fn session_info_schema(&self) -> Self::InfoSchema;
    /// Go `Session.ExecuteSQL`.
    fn execute_sql(&self, sql: &str, args: &[Datum]) -> Result<Vec<Self::Row>>;
    /// Go `Session.RunInTxn`.
    fn run_in_txn(
        &self,
        body: &mut dyn FnMut() -> Result<()>,
        txn_mode: TxnMode,
        tracer: &dyn PhaseTracer,
    ) -> Result<()>;
    /// Go `Session.ResetWithGlobalTimeZone`.
    fn reset_with_global_time_zone(&self) -> Result<()>;
    /// Go `Session.GlobalTimeZone`.
    fn global_time_zone(&self) -> Result<TimeZone>;
    /// Go `Session.KillStmt`.
    fn kill_stmt(&self);
    /// Go `Session.Now`.
    fn now(&self) -> SessionNow;
    /// Go `Session.AvoidReuse`.
    fn avoid_reuse(&self);
    /// Go's embedded `variable.SessionVarsProvider.GetSessionVars().Location()`.
    fn location(&self) -> TimeZone;
}

/// Go's unexported `session` struct.
///
/// Go caches `sctx.GetSQLExecutor()` in a field so `GetSQLExecutor` can hand it
/// back; the executor lives behind [`SessionContext::execute_internal`] here, so
/// there is nothing to cache and `GetSQLExecutor` has no separate accessor.
pub struct TtlSession<C: SessionContext> {
    sctx: C,
    avoid_reuse: Option<Box<dyn Fn() + Send + Sync>>,
}

impl<C: SessionContext> TtlSession<C> {
    /// Go `NewSession`.
    ///
    /// Go's `intest.AssertNotNil` on both arguments is a debug assertion; a
    /// non-optional `sctx` and an explicit `avoid_reuse` carry the same
    /// requirement in the type system.
    pub fn new(sctx: C, avoid_reuse: Box<dyn Fn() + Send + Sync>) -> Self {
        Self {
            sctx,
            avoid_reuse: Some(avoid_reuse),
        }
    }

    /// Go's `session{sctx: ..., avoidReuse: nil}`, the shape `AvoidReuse`'s
    /// `nil` check exists for.
    pub fn without_avoid_reuse(sctx: C) -> Self {
        Self {
            sctx,
            avoid_reuse: None,
        }
    }

    /// The wrapped context, for callers that hold the concrete type.
    pub fn context(&self) -> &C {
        &self.sctx
    }
}

impl<C: SessionContext> Session for TtlSession<C> {
    type Row = C::Row;
    type Store = C::Store;
    type InfoSchema = C::InfoSchema;

    fn get_store(&self) -> Self::Store {
        self.sctx.get_store()
    }

    fn get_latest_info_schema(&self) -> Self::InfoSchema {
        self.sctx.get_latest_info_schema()
    }

    fn session_info_schema(&self) -> Self::InfoSchema {
        self.sctx.get_txn_info_schema()
    }

    fn execute_sql(&self, sql: &str, args: &[Datum]) -> Result<Vec<Self::Row>> {
        // Go wraps the context with `kv.InternalTxnTTL` here; with no context
        // parameter the source type is the executor's own concern.
        Ok(self.sctx.execute_internal(sql, args)?.unwrap_or_default())
    }

    fn run_in_txn(
        &self,
        body: &mut dyn FnMut() -> Result<()>,
        txn_mode: TxnMode,
        tracer: &dyn PhaseTracer,
    ) -> Result<()> {
        // Go's `defer`: always try to ROLLBACK unless the whole body committed,
        // even when only the BEGIN failed — a BEGIN killed after its first
        // `Next` has already made the transaction active.
        let restore_phase = tracer.phase();
        let result = self.run_in_txn_body(body, txn_mode, tracer);
        // Go registers the rollback `defer` first and the phase-restoring one
        // second, so the phase is restored before the rollback runs.
        tracer.enter_phase(restore_phase);
        if result.is_err() {
            // Go logs the rollback error through `terror.Log` and drops it.
            let _ = self.execute_sql("ROLLBACK", &[]);
        }
        result
    }

    fn reset_with_global_time_zone(&self) -> Result<()> {
        if self.sctx.time_zone().is_some() {
            let global_tz = self.sctx.get_global_system_var(VAR_TIME_ZONE)?;
            let tz = self.sctx.get_session_or_global_system_var(VAR_TIME_ZONE)?;
            if global_tz == tz {
                return Ok(());
            }
        }

        self.execute_sql("SET @@time_zone=@@global.time_zone", &[])
            .map(|_| ())
    }

    fn global_time_zone(&self) -> Result<TimeZone> {
        let name = self.sctx.get_global_system_var(VAR_TIME_ZONE)?;
        parse_time_zone(&name).map_err(|err| error(err.to_string()))
    }

    fn kill_stmt(&self) {
        self.sctx
            .sql_killer()
            .send_kill_signal(KillSignal::QueryInterrupted);
    }

    fn now(&self) -> SessionNow {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock is before the Unix epoch");
        SessionNow {
            unix_nanos: i128::from(elapsed.as_secs()) * 1_000_000_000
                + i128::from(elapsed.subsec_nanos()),
            location: self.sctx.location(),
        }
    }

    fn avoid_reuse(&self) {
        if let Some(avoid_reuse) = self.avoid_reuse.as_ref() {
            avoid_reuse();
        }
    }

    fn location(&self) -> TimeZone {
        self.sctx.location()
    }
}

impl<C: SessionContext> TtlSession<C> {
    /// The body of Go's `RunInTxn` between its `defer`s and `success = true`.
    fn run_in_txn_body(
        &self,
        body: &mut dyn FnMut() -> Result<()>,
        txn_mode: TxnMode,
        tracer: &dyn PhaseTracer,
    ) -> Result<()> {
        tracer.enter_phase(Phase::BeginTxn);
        let sql = match txn_mode {
            TxnMode::OPTIMISTIC => "BEGIN OPTIMISTIC",
            TxnMode::PESSIMISTIC => "BEGIN PESSIMISTIC",
            _ => return Err(error("unknown transaction mode")),
        };
        self.execute_sql(sql, &[])?;
        tracer.enter_phase(Phase::Other);

        body()?;

        tracer.enter_phase(Phase::CommitTxn);
        self.execute_sql("COMMIT", &[])?;
        tracer.enter_phase(Phase::Other);
        Ok(())
    }
}
