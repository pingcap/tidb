// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Shared SQL execution contracts from `pkg/util/sqlexec`.

use std::any::Any;
use std::error::Error;
use std::sync::Arc;

use tidb_ast::Stmt;
use tidb_chunk::alloc::{AllocatedChunk, Allocator, EmptyAllocator};
use tidb_chunk::chunk::Chunk;
use tidb_chunk::iterator::{ChunkIterator, Iterator4Chunk};
use tidb_datatype::{Datum, FieldType};
use tidb_resolve::ResultFieldRef;
use tidb_util::sqlescape::SqlArg;

/// Error returned by a SQL executor or record set.
pub type SqlExecError = Box<dyn Error + Send + Sync>;

/// Result returned by a SQL executor or record set.
pub type Result<T> = std::result::Result<T, SqlExecError>;

/// A caller-owned Go `context.Context` equivalent.
///
/// SQL layers retain the concrete value and may downcast it to read the same
/// cancellation state and request-local tags that their caller supplied.
pub trait ExecutionContext: Any + Send + Sync {
    /// Returns the concrete request context.
    fn as_any(&self) -> &dyn Any;
}

impl<T: Any + Send + Sync> ExecutionContext for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Go `context.Background()` for callers with no request-local values.
#[derive(Clone, Copy, Debug, Default)]
pub struct BackgroundContext;

/// Go `sysproctrack.TrackProc`, carried opaquely through restricted execution.
pub trait TrackProcess: Any + Send + Sync {}

impl<T: Any + Send + Sync> TrackProcess for T {}

/// Go `ExecOption`.
#[derive(Default)]
pub struct ExecOption {
    /// Go `AnalyzeSnapshot`; `None` is a nil pointer.
    pub analyze_snapshot: Option<bool>,
    /// Go `TrackSysProc`.
    pub track_sys_proc: Option<TrackSysProc>,
    /// Go `UnTrackSysProc`.
    pub untrack_sys_proc: Option<UntrackSysProc>,
    /// Go `PartitionPruneMode`.
    pub partition_prune_mode: String,
    /// Go `SnapshotTS`.
    pub snapshot_ts: u64,
    /// Go `AnalyzeVer`.
    pub analyze_ver: i32,
    /// Go `TrackSysProcID`.
    pub track_sys_proc_id: u64,
    /// Go `IgnoreWarning`.
    pub ignore_warning: bool,
    /// Go `UseCurSession`.
    pub use_cur_session: bool,
    /// Go `EnableDDLAnalyze`.
    pub enable_ddl_analyze: bool,
}

/// Go `ExecOption.TrackSysProc`.
pub type TrackSysProc =
    Arc<dyn Fn(u64, Arc<dyn TrackProcess>) -> std::result::Result<(), SqlExecError> + Send + Sync>;

/// Go `ExecOption.UnTrackSysProc`.
pub type UntrackSysProc = Arc<dyn Fn(u64) + Send + Sync>;

/// Go `OptionFuncAlias`.
pub type OptionFuncAlias = Arc<dyn Fn(&mut ExecOption) + Send + Sync>;

/// Go `ExecOptionIgnoreWarning`.
pub fn exec_option_ignore_warning(option: &mut ExecOption) {
    option.ignore_warning = true;
}

/// Go `ExecOptionEnableDDLAnalyze`.
pub fn exec_option_enable_ddl_analyze(option: &mut ExecOption) {
    option.enable_ddl_analyze = true;
}

/// Go `ExecOptionAnalyzeVer2`.
pub fn exec_option_analyze_ver2(option: &mut ExecOption) {
    option.analyze_ver = 2;
}

/// Go `GetPartitionPruneModeOption`.
pub fn partition_prune_mode_option(mode: impl Into<String>) -> OptionFuncAlias {
    let mode = mode.into();
    Arc::new(move |option| option.partition_prune_mode.clone_from(&mode))
}

/// Go `GetAnalyzeSnapshotOption`.
pub fn analyze_snapshot_option(analyze_snapshot: bool) -> OptionFuncAlias {
    Arc::new(move |option| option.analyze_snapshot = Some(analyze_snapshot))
}

/// Go `ExecOptionUseCurSession`.
pub fn exec_option_use_current_session(option: &mut ExecOption) {
    option.use_cur_session = true;
}

/// Go `ExecOptionUseSessionPool`.
pub fn exec_option_use_session_pool(option: &mut ExecOption) {
    option.use_cur_session = false;
}

/// Go `ExecOptionWithSnapshot`.
pub fn snapshot_option(snapshot_ts: u64) -> OptionFuncAlias {
    Arc::new(move |option| option.snapshot_ts = snapshot_ts)
}

/// Go `ExecOptionWithSysProcTrack`.
pub fn sys_proc_track_option(
    process_id: u64,
    track: TrackSysProc,
    untrack: UntrackSysProc,
) -> OptionFuncAlias {
    Arc::new(move |option| {
        option.track_sys_proc_id = process_id;
        option.track_sys_proc = Some(Arc::clone(&track));
        option.untrack_sys_proc = Some(Arc::clone(&untrack));
    })
}

/// Go `GetExecOption`.
pub fn exec_option(options: &[OptionFuncAlias]) -> ExecOption {
    let mut result = ExecOption::default();
    for option in options {
        option(&mut result);
    }
    result
}

/// Go `RestrictedSQLExecutor`.
pub trait RestrictedSqlExecutor: Send + Sync {
    /// Go `ParseWithParams`: the parameterized version of parse, trying to
    /// prevent injection under utf8mb4. It works like printf with these
    /// specifiers:
    ///
    /// 1. `%?`: automatic conversion by the type of the argument, e.g.
    ///    a string list becomes `('s1','s2',..)`.
    /// 2. `%%`: outputs `%`.
    /// 3. `%n`: identifiers, e.g. `("use %n", db)`.
    ///
    /// Attention: this does not stop
    /// `parse("select '%?", ";SQL injection!;")` from yielding
    /// `"select '';SQL injection!;'"`. One argument must be a standalone
    /// entity and never "concat" with other placeholders or characters; the
    /// function only saves you from processing potentially unsafe parameters.
    fn parse_with_params(
        &self,
        context: &dyn ExecutionContext,
        sql: &str,
        arguments: &[SqlArg<'_>],
    ) -> Result<Stmt>;

    /// Go `ExecRestrictedStmt`.
    fn exec_restricted_stmt(
        &self,
        context: &dyn ExecutionContext,
        statement: &Stmt,
        options: &[OptionFuncAlias],
    ) -> Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>)>;

    /// Go `ExecRestrictedSQL`.
    fn exec_restricted_sql(
        &self,
        context: &dyn ExecutionContext,
        options: &[OptionFuncAlias],
        sql: &str,
        arguments: &[SqlArg<'_>],
    ) -> Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>)>;
}

/// Go `SQLExecutor`.
pub trait SqlExecutor: Send + Sync {
    /// Go `Execute`.
    fn execute(&self, context: &dyn ExecutionContext, sql: &str)
        -> Result<Vec<Box<dyn RecordSet>>>;

    /// Go `ExecuteInternal`.
    fn execute_internal(
        &self,
        context: &dyn ExecutionContext,
        sql: &str,
        arguments: &[SqlArg<'_>],
    ) -> Result<Option<Box<dyn RecordSet>>>;

    /// Go `ExecuteStmt`.
    fn execute_stmt(
        &self,
        context: &dyn ExecutionContext,
        statement: &Stmt,
    ) -> Result<Option<Box<dyn RecordSet>>>;
}

/// Go `SQLParser`.
pub trait SqlParser {
    /// Go `ParseSQL`.
    fn parse_sql(
        &self,
        context: &dyn ExecutionContext,
        sql: &str,
        parameters: &[tidb_parser::ParseParam],
    ) -> Result<(Vec<Stmt>, Vec<SqlExecError>)>;
}

/// The session-variable capability passed to Go `Statement.IsReadOnly`.
pub trait SessionVariables: Any {}

impl<T: Any> SessionVariables for T {}

/// Go `Statement`.
pub trait Statement: Send + Sync {
    /// Go `OriginText`.
    fn origin_text(&self) -> &str;
    /// Go `Text`.
    fn text(&self) -> &str;
    /// Go `GetTextToLog`.
    fn text_to_log(&self, keep_hint: bool) -> String;
    /// Go `Exec`.
    fn exec(&self, context: &dyn ExecutionContext) -> Result<Option<Box<dyn RecordSet>>>;
    /// Go `IsPrepared`.
    fn is_prepared(&self) -> bool;
    /// Go `IsReadOnly`.
    fn is_read_only(&self, variables: &dyn SessionVariables) -> bool;
    /// Go `RebuildPlan`.
    fn rebuild_plan(&self, context: &dyn ExecutionContext) -> Result<i64>;
    /// Go `GetStmtNode`.
    fn statement_node(&self) -> &Stmt;
}

/// Abstract result set produced by statement execution.
pub trait RecordSet {
    /// Resolved result fields.
    fn fields(&self) -> &[ResultFieldRef];

    /// Reads the next batch into `request`.
    fn next(&mut self, context: &dyn ExecutionContext, request: &mut Chunk) -> Result<()>;

    /// Creates a correctly typed result chunk.
    fn new_chunk(&self, allocator: Option<&dyn Allocator>) -> AllocatedChunk;

    /// Closes the iterator. Go's interface guarantees unconditionally that
    /// calling Next after Close restarts the iteration — the contract binds
    /// implementers.
    fn close(&mut self) -> Result<()>;
}

/// Go `DetachableRecordSet`.
pub trait DetachableRecordSet: RecordSet {
    /// Go `TryDetach`; the returned set is the original set when `detached`
    /// is false and the detached replacement when it is true.
    fn try_detach(self: Box<Self>) -> DetachResult;
}

/// The three Go `TryDetach` return values, including the dirty-set error case.
pub struct DetachResult {
    /// Original or detached record set; Go permits nil on an error path.
    pub record_set: Option<Box<dyn RecordSet>>,
    /// Whether detachment succeeded.
    pub detached: bool,
    /// Detachment error, after which the record set and session are dirty.
    pub error: Option<SqlExecError>,
}

/// Go `MultiQueryNoDelayResult`.
pub trait MultiQueryNoDelayResult {
    /// Go `AffectedRows`.
    fn affected_rows(&self) -> u64;
    /// Go `LastMessage`.
    fn last_message(&self) -> &str;
    /// Go `WarnCount`.
    fn warn_count(&self) -> u16;
    /// Go `Status`.
    fn status(&self) -> u16;
    /// Go `LastInsertID`.
    fn last_insert_id(&self) -> u64;
}

/// Drains all rows from a record set in chunks of `max_chunk_size`.
pub fn drain_record_set(
    context: &dyn ExecutionContext,
    record_set: &mut dyn RecordSet,
    max_chunk_size: usize,
) -> Result<Vec<Vec<Datum>>> {
    let field_types = result_field_types(record_set.fields());
    let mut rows = Vec::new();
    let mut request = record_set.new_chunk(None).into_chunk();
    loop {
        record_set.next(context, &mut request)?;
        if request.num_rows() == 0 {
            return Ok(rows);
        }
        let mut iterator = Iterator4Chunk::new(&request);
        let mut row = iterator.begin();
        while let Some(current) = row {
            rows.push(current.get_datum_row(&field_types));
            row = iterator.next_row();
        }
        request = request.renew(max_chunk_size);
    }
}

/// Drains a record set and always closes it, logging but not replacing a
/// drain error with a close error.
pub fn drain_record_set_and_close(
    context: &dyn ExecutionContext,
    record_set: &mut dyn RecordSet,
    max_chunk_size: usize,
) -> Result<Vec<Vec<Datum>>> {
    let result = drain_record_set(context, record_set, max_chunk_size);
    if let Err(error) = record_set.close() {
        tracing::error!(%error, "failed to close recordSet in DrainRecordSetAndClose");
    }
    result
}

/// Go `ExecSQL`.
pub fn execute_sql(
    context: &dyn ExecutionContext,
    executor: &dyn SqlExecutor,
    sql: &str,
    arguments: &[SqlArg<'_>],
) -> Result<Vec<Vec<Datum>>> {
    let Some(mut record_set) = executor.execute_internal(context, sql, arguments)? else {
        return Ok(Vec::new());
    };
    let result = drain_record_set(context, record_set.as_mut(), 1024);
    let _ = record_set.close();
    result
}

/// An in-memory result set whose complete contents are known at construction.
pub struct SimpleRecordSet {
    /// Resolved result fields.
    pub result_fields: Vec<ResultFieldRef>,
    /// Materialized rows.
    pub rows: Vec<Vec<Datum>>,
    /// Maximum rows per returned chunk.
    pub max_chunk_size: usize,
    index: usize,
}

impl SimpleRecordSet {
    /// Constructs Go's exported-field literal with the private cursor at zero.
    pub fn new(
        result_fields: Vec<ResultFieldRef>,
        rows: Vec<Vec<Datum>>,
        max_chunk_size: usize,
    ) -> Self {
        Self {
            result_fields,
            rows,
            max_chunk_size,
            index: 0,
        }
    }
}

impl RecordSet for SimpleRecordSet {
    fn fields(&self) -> &[ResultFieldRef] {
        &self.result_fields
    }

    fn next(&mut self, _context: &dyn ExecutionContext, request: &mut Chunk) -> Result<()> {
        request.reset();
        while self.index < self.rows.len() {
            if request.is_full() {
                return Ok(());
            }
            for column in 0..self.result_fields.len() {
                request.append_datum(column, &self.rows[self.index][column]);
            }
            self.index += 1;
        }
        Ok(())
    }

    fn new_chunk(&self, allocator: Option<&dyn Allocator>) -> AllocatedChunk {
        let fields = result_field_types(&self.result_fields);
        match allocator {
            Some(allocator) => allocator.alloc(&fields, 0, self.max_chunk_size),
            None => EmptyAllocator.alloc(&fields, self.max_chunk_size, self.max_chunk_size),
        }
    }

    fn close(&mut self) -> Result<()> {
        self.index = 0;
        Ok(())
    }
}

fn result_field_types(fields: &[ResultFieldRef]) -> Vec<FieldType> {
    fields
        .iter()
        .map(|field| {
            field
                .read()
                .column
                .as_ref()
                .expect("SimpleRecordSet result field has a column")
                .read()
                .field_type
                .clone()
        })
        .collect()
}
