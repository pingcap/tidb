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
#[derive(Debug)]
pub enum ExecError {
    /// An expression failed to evaluate.
    Eval(EvalError),
    /// An operator or feature is not yet ported.
    Unsupported(&'static str),
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
