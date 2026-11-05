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

//! Go `pkg/ttl/ttlworker` -- complete package, one module per Go file:
//! [`config`], [`worker`], [`session`], [`job`], [`scan`], [`del`],
//! [`task_manager`], [`job_manager`], [`timer`] and [`timer_sync`].
//!
//! # Concurrency
//!
//! Go's goroutines are threads, its channels `crossbeam_channel` channels
//! (an unbuffered `chan` is a zero-capacity channel, a closed one a
//! disconnected one) and its `select` is `crossbeam_channel::select!`.
//! `context.Context` is [`tidb_ttl_client::context::Context`]; Go threads
//! the phase tracer through the context, Rust passes it as
//! `Option<&PhaseTracer>`.
//!
//! # Sessions
//!
//! Go reaches every statement through `pkg/ttl/session.Session` over a
//! `sessionctx.Context` from `syssession.Pool`. Here that is
//! [`tidb_ttl::session::Session`] with the associated types fixed below
//! ([`Session`]), over a pool whose contexts implement
//! [`SysSessionContext`]: the timer table store's view of the session plus
//! the TTL session context the pool's owner builds for it.
//!
//! `// boundary:` Go's `ExecuteSQL(ctx, ...)` takes the caller's context, so
//! a `ttlInternalSQLTimeout` context bounds the statement; the Rust
//! `Session::execute_sql` takes none, so those timeouts are not created and
//! a statement runs to completion (a cancelled scan still stops through
//! `KillStmt`, exactly as Go stops it).

pub mod config;
pub mod del;
pub mod job;
pub mod job_manager;
pub mod scan;
pub mod session;
pub mod task_manager;
pub mod timer;
pub mod timer_sync;
pub mod worker;

use std::sync::Arc;

use tidb_ast::CiString;
use tidb_datatype::{CoreTime, Datum, Time, TimeType};
use tidb_model::{GoShared, TableInfo};
use tidb_timer::go_time::GoTime;
use tidb_timeutil::TimeZone;
use tidb_ttl::cache::infoschema::TtlInfoSchema;
use tidb_ttl::session::ResultRow;

pub use job_manager::JobManager;

/// Go `error` as this package passes it around: its message.
pub type Error = String;

/// This package's `Result`.
pub type Result<T> = std::result::Result<T, Error>;

/// Go `chunk.Row` as `ExecuteSQL` returns it: one row's datums.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row(pub Vec<Datum>);

impl ResultRow for Row {
    fn is_null(&self, col_idx: usize) -> bool {
        matches!(self.0.get(col_idx), None | Some(Datum::Null))
    }

    fn get_int64(&self, col_idx: usize) -> i64 {
        match self.0.get(col_idx) {
            Some(Datum::Int(value)) => *value,
            Some(Datum::UInt(value)) => *value as i64,
            _ => 0,
        }
    }

    fn get_string(&self, col_idx: usize) -> String {
        match self.0.get(col_idx) {
            Some(Datum::String(value)) => String::from_utf8_lossy(value.bytes()).into_owned(),
            Some(Datum::Bytes(value)) => String::from_utf8_lossy(value).into_owned(),
            Some(Datum::Json(value)) => value.to_string(),
            Some(Datum::Null) | None => String::new(),
            Some(other) => other.string(),
        }
    }

    fn get_bytes(&self, col_idx: usize) -> Vec<u8> {
        match self.0.get(col_idx) {
            Some(Datum::String(value)) => value.bytes().to_vec(),
            Some(Datum::Bytes(value)) => value.clone(),
            Some(Datum::Json(value)) => value.to_string().into_bytes(),
            _ => Vec::new(),
        }
    }

    fn get_time(&self, col_idx: usize) -> Time {
        match self.0.get(col_idx) {
            Some(Datum::Time(value)) => *value,
            _ => zero_time(),
        }
    }
}

impl Row {
    /// Go `row.Len()`.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// `// boundary:` Go `*variable.SessionVars`, restricted to the fields
/// `pkg/ttl/ttlworker` reads or writes.
pub trait SessionVars: Send + Sync {
    /// Go `StmtCtx.AffectedRows()` of the last statement.
    fn affected_rows(&self) -> u64;
    /// Go `Location()`.
    fn location(&self) -> TimeZone;
    /// Go `RetryLimit`.
    fn retry_limit(&self) -> i64;
    /// Go `Enable1PC`.
    fn enable_1pc(&self) -> bool;
    /// Go `EnableAsyncCommit`.
    fn enable_async_commit(&self) -> bool;
    /// Go's `IsolationReadEngines` holding all of `kv.TiDB`, `kv.TiKV` and
    /// `kv.TiFlash`.
    fn isolation_read_engines_are_all(&self) -> bool;
    /// Go `DistSQLScanConcurrency()`.
    fn distsql_scan_concurrency(&self) -> i64;
    /// Go `EnablePaging`.
    fn enable_paging(&self) -> bool;
    /// Go `InternalSQLScanUserTable = value`.
    fn set_internal_sql_scan_user_table(&self, value: bool);
}

/// `// boundary:` Go `infoschema.InfoSchema` (behind
/// `infoschemacontext.MetaOnlyInfoSchema`), restricted to what this package
/// and `pkg/ttl/cache` call.
pub trait InfoSchema: Send + Sync {
    /// Go `SchemaMetaVersion()`.
    fn schema_meta_version(&self) -> i64;
    /// Go `ListTablesWithSpecialAttribute(TTLAttribute)`: every table with a
    /// `TTLInfo`, grouped by schema name.
    fn list_ttl_tables(&self) -> Vec<(CiString, Vec<GoShared<TableInfo>>)>;
    /// Go `TableInfoByName(schema, table)`.
    fn table_info_by_name(&self, schema: &CiString, table: &CiString) -> Result<GoShared<TableInfo>>;
    /// Go `TableByID(ctx, id)`'s `Meta()`.
    fn table_by_id(&self, id: i64) -> Option<GoShared<TableInfo>>;
}

/// The info schema a [`Session`] hands out.
#[derive(Clone)]
pub struct InfoSchemaRef(pub Arc<dyn InfoSchema>);

impl TtlInfoSchema for InfoSchemaRef {
    fn schema_meta_version(&self) -> i64 {
        self.0.schema_meta_version()
    }

    fn list_ttl_tables(&self) -> Vec<(CiString, Vec<GoShared<TableInfo>>)> {
        self.0.list_ttl_tables()
    }
}

/// Go `kv.Storage` as this package uses it: the store's region access when
/// it is a `tikv.Storage`, `None` when Go's type assertion would fail.
pub type Store = Option<Arc<dyn tidb_kv::region_map::RegionStore>>;

/// Go's `sessionctx.Context` as `pkg/ttl/session` drives it, with this
/// package's row, store, info-schema and session-variable types.
pub trait TtlSessionContext:
    tidb_ttl::session::SessionContext<
        Row = Row,
        Store = Store,
        InfoSchema = InfoSchemaRef,
        SqlExecutor = (),
        SessionVars = dyn SessionVars,
    > + Send
    + Sync
{
}

impl<T> TtlSessionContext for T where
    T: tidb_ttl::session::SessionContext<
            Row = Row,
            Store = Store,
            InfoSchema = InfoSchemaRef,
            SqlExecutor = (),
            SessionVars = dyn SessionVars,
        > + Send
        + Sync
{
}

/// A shared [`TtlSessionContext`], which [`tidb_ttl::session::TtlSession`]
/// wraps as Go's `session.NewSession(sctx, ...)` does.
pub struct SctxRef(pub Arc<dyn TtlSessionContext>);

impl tidb_ttl::session::SessionContext for SctxRef {
    type Row = Row;
    type Store = Store;
    type InfoSchema = InfoSchemaRef;
    type SqlExecutor = ();
    type SessionVars = dyn SessionVars;

    fn get_store(&self) -> Store {
        self.0.get_store()
    }

    fn get_latest_info_schema(&self) -> InfoSchemaRef {
        self.0.get_latest_info_schema()
    }

    fn get_txn_info_schema(&self) -> InfoSchemaRef {
        self.0.get_txn_info_schema()
    }

    fn get_sql_executor(&self) -> &() {
        self.0.get_sql_executor()
    }

    fn get_session_vars(&self) -> &(dyn SessionVars + 'static) {
        self.0.get_session_vars()
    }

    fn execute_internal(&self, source: &str, sql: &str, args: &[Datum]) -> tidb_ttl::session::Result<Option<Vec<Row>>> {
        self.0.execute_internal(source, sql, args)
    }

    fn get_global_system_var(&self, name: &str) -> tidb_ttl::session::Result<String> {
        self.0.get_global_system_var(name)
    }

    fn get_session_or_global_system_var(&self, name: &str) -> tidb_ttl::session::Result<String> {
        self.0.get_session_or_global_system_var(name)
    }

    fn time_zone(&self) -> Option<TimeZone> {
        self.0.time_zone()
    }

    fn location(&self) -> TimeZone {
        self.0.location()
    }

    fn sql_killer(&self) -> &tidb_sqlkiller::SqlKiller {
        self.0.sql_killer()
    }
}

/// Go `pkg/ttl/session.Session` with this package's associated types.
pub type Session = dyn tidb_ttl::session::Session<
        Row = Row,
        Store = Store,
        InfoSchema = InfoSchemaRef,
        SqlExecutor = (),
        SessionVars = dyn SessionVars,
    > + Send
    + Sync;

/// The `sessionctx.Context` a `syssession.Pool` hands out, as this package
/// needs it: the timer table store's session surface, plus the session the
/// TTL statements run on.
pub trait SysSessionContext: tidb_timer_tablestore::store::SessionContext {
    /// The session's context as `pkg/ttl/session.NewSession` wraps it.
    fn ttl_session_context(&self) -> Arc<dyn TtlSessionContext>;
}

/// Go `se.Now()` as a Go `time.Time`.
pub fn session_now(se: &Session) -> GoTime {
    let now = se.now();
    let seconds = now.unix_nanos.div_euclid(1_000_000_000) as i64;
    let nanos = now.unix_nanos.rem_euclid(1_000_000_000) as i64;
    GoTime::from_unix(seconds, nanos).in_location(&now.location)
}

/// Go `row.GetTime(i).GoTime(loc)`: a stored wall clock resolved in `loc`.
pub fn mysql_time_to_go(time: Time, location: &TimeZone) -> GoTime {
    let core = time.core_time();
    GoTime::date(
        core.year(),
        i32::from(core.month()),
        i32::from(core.day()),
        i32::from(core.hour()),
        i32::from(core.minute()),
        i32::from(core.second()),
        i64::from(core.microsecond()) * 1000,
        location,
    )
}

/// The `DATETIME(6)` a Go `time.Time` argument binds as: its wall clock in
/// its own location.
pub fn go_time_to_mysql(time: &GoTime) -> Time {
    let naive = time.naive_local();
    use chrono::{Datelike, Timelike};
    Time::new(
        CoreTime::from_date(
            naive.year() as u16,
            naive.month() as u8,
            naive.day() as u8,
            naive.hour() as u8,
            naive.minute() as u8,
            naive.second() as u8,
            naive.nanosecond() / 1000,
        ),
        TimeType::DateTime,
        6,
    )
    .unwrap_or_else(|_| zero_time())
}

/// Go's zero `types.Time`.
fn zero_time() -> Time {
    Time::new(CoreTime::from_raw(0), TimeType::DateTime, 0).expect("the zero time is valid")
}

/// Go `errors.Wrapf(err, "execute sql: %s", sql)`.
pub(crate) fn wrap_execute_sql(err: impl std::fmt::Display, sql: &str) -> Error {
    format!("execute sql: {sql}: {err}")
}

/// A Go `string` argument.
pub(crate) fn str_arg(value: &str) -> Datum {
    Datum::new_string_datum(value.as_bytes().to_vec())
}

/// Shorthand for a string log field.
pub(crate) fn field(key: &str, value: impl Into<String>) -> tidb_log::Field {
    tidb_log::Field::new(key, tidb_log::Value::Str(value.into()))
}

/// Shorthand for an integer log field.
pub(crate) fn int_field(key: &str, value: i64) -> tidb_log::Field {
    tidb_log::Field::new(key, tidb_log::Value::I64(value))
}

/// Shorthand for an unsigned log field.
pub(crate) fn uint_field(key: &str, value: u64) -> tidb_log::Field {
    tidb_log::Field::new(key, tidb_log::Value::U64(value))
}
