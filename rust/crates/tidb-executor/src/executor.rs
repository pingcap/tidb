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

//! `pkg/executor/internal/exec`: the [`Executor`] trait and the [`ExecutorMeta`]
//! shared base state.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::schema::Schema;
use tidb_expr::EvalError;

/// An execution error. Go returns a bare `error`; this wraps the failures the
/// ported executors can surface.
#[derive(Debug, Clone)]
pub enum ExecError {
    /// An expression failed to evaluate.
    Eval(EvalError),
    /// An operator or feature is not yet ported.
    Unsupported(&'static str),
    /// Go `ErrSubqueryMoreThan1Row` (1242), raised by the max-one-row check a
    /// scalar subquery's plan carries. It is an executor error because it is
    /// only known per outer row, once the inner query has run.
    SubqueryReturnsMoreThanOneRow,
    /// Go `types.ErrJSONDocumentNULLKey` (3158): `JSON_OBJECTAGG` evaluated a
    /// NULL member name. It is an executor error because Go raises it while
    /// folding the group, after the result columns are already on the wire.
    JsonDocumentNullKey,
    /// Go `types.ErrInvalidJSONCharset` (3144): `JSON_OBJECTAGG` evaluated a
    /// BINARY-charset key. Like `JsonDocumentNullKey`, this surfaces only
    /// once the group is folded, not at plan time.
    InvalidJsonCharset {
        /// The rejected key argument's charset name (always `binary`; Go's
        /// message is parameterized on it, so it travels with the error
        /// rather than being hard-coded at the wire boundary).
        charset: String,
    },
}

impl From<EvalError> for ExecError {
    fn from(err: EvalError) -> Self {
        ExecError::Eval(err)
    }
}

/// Go `exec.Executor`: a pull-based operator in the execution tree.
///
/// Callers drive it with `open()`, repeated `next(&mut chunk)` (an empty result
/// chunk signals EOF, as in Go), then `close()`.
///
/// The observability/control surface of Go's interface (`RuntimeStats`,
/// `HandleSQLKillerSignal`, `RegisterSQLAndPlanInExecForTopProfiling`, `Detach`)
/// and the `context.Context` argument are intentionally omitted from this seed.
pub trait Executor {
    /// Go `Open`: prepare the operator (and, by convention, its children).
    fn open(&mut self) -> Result<(), ExecError>;

    /// Go `Next`: produce the next batch of rows into `req` (which is reset
    /// first). An empty `req` on return means the operator is exhausted.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError>;

    /// Go `Close`: release resources.
    fn close(&mut self) -> Result<(), ExecError>;

    /// Go `Schema`: the operator's output schema.
    fn schema(&self) -> &Schema;

    /// Go `RetFieldTypes`: the output column types.
    fn ret_field_types(&self) -> &[FieldType];

    /// Go `InitCap`: the initial per-chunk row capacity.
    fn init_cap(&self) -> usize;

    /// Go `MaxChunkSize`: the maximum per-chunk row count.
    fn max_chunk_size(&self) -> usize;

    /// Go `NewChunk`: allocate a result chunk sized for this operator's output.
    fn new_chunk(&self) -> Chunk;

    /// Offers `filter` to this source, as Go's predicate push-down offers a
    /// conjunct to the node below it.
    ///
    /// Returning `true` is a promise the driver relies on to *remove* those
    /// conjuncts from the `Selection` above: the source must apply every one
    /// of them to **every** row it emits, including rows merged in from the
    /// session's staged mutation buffer, which never passed through a
    /// coprocessor. A source that cannot promise that leaves the default
    /// `false` and the whole `WHERE` stays where it was.
    ///
    /// See [`crate::scan_pushdown`] for the split rule and the reasoning.
    fn accept_scan_filter(
        &mut self,
        filter: &crate::scan_pushdown::PushedScanFilter,
        ctx: &crate::StmtContext,
    ) -> bool {
        let _ = (filter, ctx);
        false
    }

    /// Offers this source a row cap, as Go's `LIMIT` push-down puts a `Limit`
    /// inside the cop task below the scan (captured: `Limit_12 | cop[tikv] |
    /// offset:0, count:3` under `IndexRangeScan_11`).
    ///
    /// `cap` is `offset + count`, because the offset rows are consumed above
    /// and must still be produced -- exactly what Go's cop-side `Limit`
    /// carries (`limit 2, 3` lowers to `offset:0, count:5`).
    ///
    /// Returning `true` promises the source stops after `cap` rows *that it
    /// itself emits*. The driver may therefore only offer a cap when every
    /// filter the query applies is applied at or below this source, and when
    /// the row order this source produces is the order the `LIMIT` selects
    /// from. Like [`Executor::accept_scan_filter`] this is fail-closed: the
    /// default refuses and the `LimitExec` above keeps doing all the work.
    fn accept_scan_limit(&mut self, cap: u64) -> bool {
        let _ = cap;
        false
    }

    /// The live count of rows this source read from storage, before any
    /// filter it accepted -- `TableFullScan`'s `actRows`, which a pushed
    /// predicate must not change. `None` for anything that is not such a
    /// scan.
    fn scanned_rows_counter(&self) -> Option<std::rc::Rc<std::cell::Cell<u64>>> {
        None
    }

    /// Offers this source the chance to emit only the columns at `keep`
    /// (offsets into its current output row, ascending and unique), as Go's
    /// column pruning narrows a `DataSource`'s schema.
    ///
    /// Returning `true` is a promise the driver relies on to renumber the
    /// `FROM` scope: from the next `open` on, every row this source emits
    /// must be exactly `keep.len()` wide and hold `keep`'s columns in
    /// `keep`'s order, and [`Executor::schema`] must already describe that
    /// narrow row. A source that cannot promise it leaves the default
    /// `false` and the driver keeps the full-width scope unchanged.
    ///
    /// See [`crate::column_prune`] for the eligibility gate and the reasoning.
    fn accept_column_prune(&mut self, keep: &[usize]) -> bool {
        let _ = keep;
        false
    }
}

/// Go `exec.executorMeta`: the schema/id/children/result-type base state shared
/// by every operator (Go embeds it via `BaseExecutorV2`).
///
/// The chunk-sizing fields (`init_cap`, `max_chunk_size`) come from Go's
/// `executorChunkAllocator`; the runtime-stats, killer, and RU-tracking helpers
/// that `BaseExecutorV2` also composes are deferred.
#[derive(Clone, Debug)]
pub struct ExecutorMeta {
    schema: Schema,
    ret_field_types: Vec<FieldType>,
    id: i64,
    init_cap: usize,
    max_chunk_size: usize,
}

impl ExecutorMeta {
    /// Go `newExecutorMeta` (+ the chunk-allocator sizing): derives the result
    /// field types from the schema's columns.
    ///
    /// # Panics
    /// If a schema column lacks a result type -- Go dereferences `RetType`
    /// unconditionally, so a nil there is already a bug at this point.
    #[must_use]
    pub fn new(schema: Schema, id: i64, init_cap: usize, max_chunk_size: usize) -> Self {
        let ret_field_types = schema
            .columns
            .iter()
            .map(|c| {
                c.ret_type
                    .clone()
                    .expect("executor schema column must have a result type")
            })
            .collect();
        ExecutorMeta {
            schema,
            ret_field_types,
            id,
            init_cap,
            max_chunk_size,
        }
    }

    /// Go `Schema`.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Go `RetFieldTypes`.
    #[must_use]
    pub fn ret_field_types(&self) -> &[FieldType] {
        &self.ret_field_types
    }

    /// Go `ID`.
    #[must_use]
    pub fn id(&self) -> i64 {
        self.id
    }

    /// Go `InitCap`.
    #[must_use]
    pub fn init_cap(&self) -> usize {
        self.init_cap
    }

    /// Go `MaxChunkSize`.
    #[must_use]
    pub fn max_chunk_size(&self) -> usize {
        self.max_chunk_size
    }

    /// Go `NewChunk`: a result chunk for this operator's output types.
    #[must_use]
    pub fn new_chunk(&self) -> Chunk {
        Chunk::new(&self.ret_field_types, self.init_cap, self.max_chunk_size)
    }
}
