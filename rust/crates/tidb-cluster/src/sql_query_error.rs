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

//! The client-visible error a statement path hands back to the connection:
//! Go's `*terror.Error` as it crosses `ExecuteStmt`'s boundary, carrying the
//! MySQL code, SQLSTATE, message and (for `err.Error()`) the `terror`
//! identity. It lives below the session so the DDL worker, which answers a
//! submitted job through it, does not need the server crate.

use tidb_error::terror::ERR_RESULT_UNDETERMINED;
use tidb_txnkv::driver::transaction_error::WriteConflictArgs;

use crate::pessimistic_lock_error::LockSqlError;

/// A source-rendered error returned before a query result escapes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlQueryError {
    /// MySQL error number.
    pub code: u16,
    /// Five-byte SQLSTATE.
    pub state: [u8; 5],
    /// Caller-rendered error text.
    pub message: String,
    /// A commit write conflict's `newWriteConflictError` arguments, which the
    /// session rewrites with table names (Go `tryReplaceWriteConflictError`).
    pub write_conflict: Option<Box<WriteConflictArgs>>,
    /// The Go `*terror.Error` identity the failure was raised as, `None` for a
    /// plain error. Go's `err.Error()` -- `@@tidb_last_query_info`, logs --
    /// prints it as `[class:code]message`; the wire carries only the message.
    pub identity: Option<tidb_error::terror::TerrorIdentity>,
    /// The typed error this was rendered from, when one was (Go keeps the
    /// typed error under its wrapping, `errors.Cause`).
    pub cause: Option<ErrorCause>,
}

/// The typed error under a [`SqlQueryError`], of whatever layer raised it.
/// It is provenance, not identity: two failures with the same client-visible
/// triple are equal whether or not either remembers its cause.
#[derive(Clone)]
pub struct ErrorCause(pub std::sync::Arc<dyn std::any::Any + Send + Sync>);

impl std::fmt::Debug for ErrorCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ErrorCause")
    }
}

impl PartialEq for ErrorCause {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for ErrorCause {}

impl SqlQueryError {
    /// Creates one explicit client-visible query failure.
    #[must_use]
    pub fn new(code: u16, state: [u8; 5], message: impl Into<String>) -> Self {
        Self {
            code,
            state,
            message: message.into(),
            write_conflict: None,
            identity: None,
            cause: None,
        }
    }

    /// Remembers the typed error this failure was rendered from.
    #[must_use]
    pub fn with_cause<T: std::any::Any + Send + Sync>(mut self, cause: T) -> Self {
        self.cause = Some(ErrorCause(std::sync::Arc::new(cause)));
        self
    }

    /// The typed error this failure was rendered from, when it is a `T`
    /// (Go `errors.Cause` asserted to its type).
    #[must_use]
    pub fn cause_of<T: std::any::Any>(&self) -> Option<&T> {
        self.cause.as_ref()?.0.downcast_ref::<T>()
    }

    /// Labels the failure with the Go identity it was raised as.
    #[must_use]
    pub fn with_identity(mut self, identity: Option<tidb_error::terror::TerrorIdentity>) -> Self {
        self.identity = identity;
        self
    }

    /// Go `err.Error()`: `[<class>:<code>]<message>` for a `*terror.Error`,
    /// the bare message for a plain error.
    #[must_use]
    pub fn error_string(&self) -> String {
        match self.identity {
            Some(identity) if identity.class().description().is_empty() => {
                format!("[{}]{}", identity.code().value(), self.message)
            }
            Some(identity) => format!("[{}]{}", identity.rfc_code(), self.message),
            None => self.message.clone(),
        }
    }

    /// Creates the fail-closed generic boundary for an injected engine error.
    #[must_use]
    pub fn unknown(message: impl Into<String>) -> Self {
        Self::new(1105, *b"HY000", message)
    }

    /// The one failure whose answer is "nobody knows", not "it failed".
    ///
    /// Go `pkg/parser/terror/terror.go:265-269` defines
    /// `ErrResultUndetermined = ClassGlobal.NewStdErr(CodeResultUndetermined,
    /// mysql.Message("execution result undetermined", nil))`. It exists
    /// because no SQL error code can express "unknown", and a client that
    /// receives an ordinary error is entitled to retry -- which double-applies
    /// if the commit did land. `pkg/server/conn.go:1288-1291` therefore closes
    /// the connection rather than reporting either outcome.
    #[must_use]
    pub fn result_undetermined() -> Self {
        let error = tidb_error::mysql::MysqlError::from_terror(&ERR_RESULT_UNDETERMINED);
        Self {
            identity: error.identity,
            ..Self::new(error.code, error.state, error.message)
        }
    }

    /// Whether this failure is the undetermined verdict, so the caller must
    /// close the connection instead of answering: Go
    /// `terror.ErrResultUndetermined.Equal(err)`. The client never sees it:
    /// Go closes the connection without writing an ERR packet at all, and so
    /// do we.
    #[must_use]
    pub fn is_result_undetermined(&self) -> bool {
        self.identity == Some(ERR_RESULT_UNDETERMINED.identity())
    }
}

/// Preserves a transaction driver's client-visible error triple at the SQL
/// boundary. Domain-specific commit errors delegate here instead of
/// reclassifying an already typed failure from its display text.
pub fn lock_sql_error(error: &LockSqlError) -> SqlQueryError {
    SqlQueryError {
        write_conflict: error.write_conflict.clone(),
        identity: error.identity,
        ..SqlQueryError::new(error.code, error.state, error.message.clone())
    }
}
