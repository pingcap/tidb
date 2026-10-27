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

use std::borrow::Cow;

use tidb_expr::EvalError;

/// An execution error. Go returns a bare `error`; this wraps the failures the
/// ported executors can surface.
#[derive(Debug, Clone)]
pub enum ExecError {
    /// A typed SQL failure carried through a storage/executor boundary.
    Mysql(crate::MysqlError),
    /// An expression failed to evaluate.
    Eval(EvalError),
    /// A violated executor/chunk invariant surfaced as Go's generic 1105.
    ///
    /// This is distinct from [`ExecError::Unsupported`]: the requested SQL is
    /// supported, but an internal state made it unsafe to continue.
    Internal(Cow<'static, str>),
    /// An operator or feature is not yet ported.
    ///
    /// [`Cow`] for the same reason [`crate::DriverError::Unsupported`] is:
    /// most reasons are fixed text, but one built per call is the same
    /// refusal and reaches the client as the same 1105 message. Build one
    /// with [`ExecError::unsupported`].
    Unsupported(Cow<'static, str>),
    /// Go `ErrSubqueryMoreThan1Row` (1242), raised by the max-one-row check a
    /// scalar subquery's plan carries. It is an executor error because it is
    /// only known per outer row, once the inner query has run.
    SubqueryReturnsMoreThanOneRow,
    /// Go `exeerrors.ErrCTEMaxRecursionDepth` (3636). The payload is the
    /// first recursive iteration beyond `@@cte_max_recursion_depth`.
    CteMaxRecursionDepth(u64),
    /// Go `exeerrors.ErrMemoryExceedForQuery` (8175): the statement's memory
    /// consumption crossed `tidb_mem_quota_query` while
    /// `tidb_mem_oom_action` was `CANCEL`. See [`crate::StatementMemory`].
    MemoryExceedForQuery {
        /// The connection the message names; Go's message is parameterized on
        /// it (`...try again.[conn=%d]`).
        conn_id: u64,
    },
    /// A canonical SQL-killer failure transported through Rust's typed
    /// executor boundary instead of Go's panic/recover mechanism.
    Killed(tidb_error::mysql::SqlError),
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
    /// A memory-table retriever's failure when its scan is first read (Go
    /// `MemTableReaderExec.Next`), carried whole so the statement reports the
    /// driver error the retriever raised (a missing privilege, an
    /// unsupported feature) rather than a rendered copy of it.
    Retrieval(Box<crate::DriverError>),
    /// A spill-to-disk operation failed (Go returns the underlying
    /// `os`/`chunk` error straight out of the operator's `Next`, so it reaches
    /// the client as a generic 1105 carrying that text).
    SpillFailed(String),
}

impl ExecError {
    /// [`ExecError::Internal`] from either a fixed message or one built per
    /// call.
    #[must_use]
    pub fn internal(message: impl Into<Cow<'static, str>>) -> Self {
        ExecError::Internal(message.into())
    }

    /// [`ExecError::Unsupported`] from either a fixed reason or one built per
    /// call.
    #[must_use]
    pub fn unsupported(reason: impl Into<Cow<'static, str>>) -> Self {
        ExecError::Unsupported(reason.into())
    }
}

impl From<EvalError> for ExecError {
    fn from(err: EvalError) -> Self {
        ExecError::Eval(err)
    }
}
