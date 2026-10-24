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

use tidb_txnkv::driver::transaction_error::WriteConflictArgs;

use crate::pessimistic_lock_error::LockSqlError;
use crate::real_tikv_ddl::ClusterDdlError;

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
    /// The driver error this was rendered from, when one was (Go keeps the
    /// typed error under its wrapping, `errors.Cause`).
    pub cause: Option<DriverErrorCause>,
}

/// The typed driver error under a [`SqlQueryError`]. It is provenance, not
/// identity: two failures with the same client-visible triple are equal
/// whether or not either remembers its cause.
#[derive(Clone, Debug)]
pub struct DriverErrorCause(pub std::sync::Arc<tidb_executor::DriverError>);

impl PartialEq for DriverErrorCause {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for DriverErrorCause {}

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

    /// Remembers the driver error this failure was rendered from.
    #[must_use]
    pub fn with_cause(mut self, cause: tidb_executor::DriverError) -> Self {
        self.cause = Some(DriverErrorCause(std::sync::Arc::new(cause)));
        self
    }

    /// The driver error this failure was rendered from, when its
    /// client-visible code and message are still that error's own (a caller
    /// that reworded the message has made the cause stale).
    #[must_use]
    pub fn driver_cause(&self) -> Option<&tidb_executor::DriverError> {
        let cause = self.cause.as_ref()?.0.as_ref();
        let rendered = cause.clone().to_mysql_error();
        (rendered.code == self.code && rendered.message == self.message).then_some(cause)
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
    /// mysql.Message("execution result undetermined", nil))`, which carries
    /// the default MySQL code `mysql.ErrUnknown` (1105). It exists because no
    /// SQL error code can express "unknown", and a client that receives an
    /// ordinary error is entitled to retry — which double-applies if the
    /// commit did land. `pkg/server/conn.go:1288-1291` therefore closes the
    /// connection rather than reporting either outcome.
    #[must_use]
    pub fn result_undetermined() -> Self {
        Self::new(1105, *b"HY000", RESULT_UNDETERMINED_MESSAGE)
    }

    /// Whether this failure is the undetermined verdict, so the caller must
    /// close the connection instead of answering.
    ///
    /// Go matches on the `terror` identity (`terror.ErrResultUndetermined
    /// .Equal(err)`); we have no error identities on the wire boundary, so the
    /// exact message is the identity. It is safe as an identity precisely
    /// because the client never sees it: Go closes the connection without
    /// writing an ERR packet at all, and so do we.
    #[must_use]
    pub fn is_result_undetermined(&self) -> bool {
        self.code == 1105 && self.message == RESULT_UNDETERMINED_MESSAGE
    }
}

/// Go `pkg/parser/terror/terror.go:268`: `mysql.Message("execution result
/// undetermined", nil)`.
pub const RESULT_UNDETERMINED_MESSAGE: &str = "execution result undetermined";

/// Preserves a transaction driver's client-visible error triple at the SQL
/// boundary. Domain-specific commit errors delegate here instead of
/// reclassifying an already typed failure from its display text.
pub fn lock_sql_error(error: &LockSqlError) -> SqlQueryError {
    SqlQueryError {
        write_conflict: error.write_conflict.clone(),
        ..SqlQueryError::new(error.code, error.state, error.message.clone())
    }
}

/// Preserves the one DDL outcome that is not a failure verdict while mapping
/// every determinate catalog error to its ordinary client diagnostic.
pub fn cluster_ddl_error(error: ClusterDdlError) -> SqlQueryError {
    match error {
        ClusterDdlError::Undetermined(_) => SqlQueryError::result_undetermined(),
        ClusterDdlError::Commit(error) => lock_sql_error(&error),
        ClusterDdlError::IndexBackfill(error) => lock_sql_error(&error),
        ClusterDdlError::Plan(error) => {
            let error = error.to_sql_error();
            SqlQueryError::new(
                error.code,
                error
                    .state
                    .as_bytes()
                    .try_into()
                    .expect("catalog SQLSTATE has five bytes"),
                error.message,
            )
        }
        other => SqlQueryError::unknown(other.to_string()),
    }
}
