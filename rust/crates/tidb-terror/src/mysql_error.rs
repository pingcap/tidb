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

//! Go `terror.Error` as one wire value: the code / SQLSTATE / message triple
//! plus the Go class identity, which every reporting surface (the protocol,
//! `SHOW WARNINGS`, the log) reads. Lives beside [`tidb_mysql::errors::SqlError`], the
//! `mysql.SQLError` view it is converted from.

use crate::{TerrorError, TerrorIdentity};

/// The MySQL error a driver failure becomes on the wire, which is also what
/// `SHOW WARNINGS` reports for a failed statement.
///
/// Go attaches the code, the SQLSTATE and the rendered message to the error
/// itself (`terror.Error`), so every surface that reports an error -- the
/// protocol, `SHOW WARNINGS`, the log -- reads the same three fields. This
/// keeps that single source of truth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MysqlError {
    /// MySQL error number.
    pub code: u16,
    /// Five-byte SQLSTATE.
    pub state: [u8; 5],
    /// The rendered message.
    pub message: String,
    /// The Go `*terror.Error` identity (class and code) this failure was
    /// raised as, `None` where Go raised a plain `errors.New` / `Errorf`
    /// error (wire code 1105, no class).
    ///
    /// Go keeps the class on the error VALUE: `Error()` prints
    /// `[<class>:<code>]<message>` and `terror.ToSQLError` drops the prefix
    /// for the wire. The wire message is only half of what an error is -- the
    /// other half reaches `@@tidb_last_query_info`, persisted DDL job errors,
    /// and every message that embeds `err.Error()`. Codes are not unique across
    /// classes, so the class travels with the error from the raise site that
    /// names the Go definition and is never recovered from the code.
    pub identity: Option<TerrorIdentity>,
    /// Whether this failure ORIGINATED in expression evaluation. Go's
    /// statement-error wire behavior is asymmetric: parse/plan/executor
    /// failures land in the statement warning buffer (`driver_tidb.go`
    /// `StmtCtx.AppendError`) while expression-evaluation failures do not,
    /// so the warning-buffer seam needs to tell the two apart.
    pub(crate) from_evaluation: bool,
}

impl MysqlError {
    /// A Go `*terror.Error` raise: the identity, the wire code and the
    /// SQLSTATE all come from the Go definition `proto` names, and `message`
    /// is what the raise site rendered (`GenWithStackByArgs` /
    /// `FastGenByArgs`).
    ///
    /// Go never writes a code and its state down together: `NewErr` looks the
    /// code up in `MySQLState` and falls back to `HY000`
    /// (`pkg/parser/mysql/error.go:40-57`). Spelling the state out at the
    /// raise site is what lets a code and its state disagree -- `1365` was
    /// reaching clients as `HY000` where TiDB sends `22012` -- so raise sites
    /// name the Go error and nothing else; [`TerrorError::to_sql_error`] is
    /// the only `state` source. Naming the Go error rather than a number is
    /// also what keeps the class: `1146` is `infoschema.ErrTableNotExists`
    /// (`schema`) and `plannererrors.ErrNoSuchTable` (`planner`).
    fn terror(proto: &TerrorError, message: impl Into<String>) -> Self {
        let sql = proto.to_sql_error();
        let mut state = [0u8; 5];
        state.copy_from_slice(sql.state.as_bytes());
        Self {
            code: sql.code,
            state,
            message: message.into(),
            identity: Some(proto.identity()),
            from_evaluation: false,
        }
    }

    /// Go `ErrX.Equal(err)` / `terror.ErrorEqual(err, ErrX)`: whether this
    /// failure was raised as Go's `proto` (same class and code).
    #[must_use]
    pub fn equal(&self, proto: &TerrorError) -> bool {
        self.identity == Some(proto.identity())
    }

    /// [`Self::terror`] with the prototype's own message, for an error Go
    /// raises with no arguments (`return ErrFoo`).
    pub fn from_terror(error: &TerrorError) -> Self {
        Self::terror(error, error.message())
    }

    /// A coded failure that has no Go `terror` definition at all -- Go either
    /// raises it through `mysql.NewErr` (a `*mysql.SQLError`, no class) or
    /// never raises it and the Rust tier invented the path. The SQLSTATE is
    /// still the code's own (`NewErr`'s `MySQLState` lookup). Everything Go
    /// raises through a `terror.Error` must name its definition with
    /// [`Self::terror`] instead.
    pub fn sql_error(code: u16, message: impl Into<String>) -> Self {
        let mut state = [0u8; 5];
        state.copy_from_slice(tidb_mysql::errors::mysql_state(code).as_bytes());
        Self::with_state(code, state, message)
    }

    /// A wire triple that remembers its Go identity: the form
    /// `ExecError::Killed`, statistics, transaction and variable failures
    /// carry between layers where Go passes the `error` value itself.
    pub fn from_sql_error(error: tidb_mysql::errors::SqlError) -> Self {
        let mut state = [0u8; 5];
        state.copy_from_slice(error.state.as_bytes());
        Self {
            code: error.code,
            state,
            message: error.message,
            identity: error.identity,
            from_evaluation: false,
        }
    }

    /// Reconstructs an error whose SQLSTATE was carried in from outside this
    /// module (a stored or wire-level error already holding its state), not
    /// one raised fresh here. It has no Go class: the stored form does not
    /// keep one.
    fn with_state(code: u16, state: [u8; 5], message: impl Into<String>) -> Self {
        Self {
            code,
            state,
            message: message.into(),
            identity: None,
            from_evaluation: false,
        }
    }

    /// Rebuilds an already-classified `(code, state, message)` triple from
    /// outside this crate — the stored-wire-error case [`Self::with_state`]
    /// serves internally, which struct literals outside the crate cannot
    /// spell (the evaluation-origin field is crate-private by design).
    pub fn from_parts(code: u16, state: [u8; 5], message: impl Into<String>) -> Self {
        Self::with_state(code, state, message)
    }

    /// Go `error.Error()` for this failure: `[<class>:<code>]<message>` for a
    /// `*terror.Error`, the bare message for a plain error. This is the text
    /// `@@tidb_last_query_info`, persisted job errors, logs and every
    /// `...err.Error()` embedding carry; the wire message is [`Self::message`]
    /// alone (`terror.ToSQLError` drops the prefix).
    #[must_use]
    pub fn error_string(&self) -> String {
        match self.identity {
            Some(identity) => {
                // `pingcap/errors` prints the bare code where the class has no
                // registered description.
                if identity.class().description().is_empty() {
                    format!("[{}]{}", identity.code().value(), self.message)
                } else {
                    format!("[{}]{}", identity.rfc_code(), self.message)
                }
            }
            None => self.message.clone(),
        }
    }

    /// Keeps the Go identity a wire-level carrier (`SqlQueryError`,
    /// `LockSqlError`) brought across a crate boundary.
    #[must_use]
    pub fn with_identity_opt(mut self, identity: Option<TerrorIdentity>) -> Self {
        self.identity = identity;
        self
    }

    /// Re-labels this failure with a different Go identity, keeping its wire
    /// code, state and message: `ClassTiKV.Synthesize(code, msg)` answers a
    /// coprocessor error under class `tikv` whatever number reaches the wire.
    #[must_use]
    pub fn with_identity(mut self, identity: TerrorIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Marks this error as raised by expression evaluation, the class go's
    /// warning buffer never records (`SELECT CAST('str' AS JSON)` errors
    /// 3140 with an EMPTY `SHOW WARNINGS`, while a 1054/1062 failure shows
    /// its own error row there).
    #[must_use]
    pub fn from_evaluation(mut self) -> Self {
        self.from_evaluation = true;
        self
    }

    /// Whether [`Self::from_evaluation`] marked this error.
    #[must_use]
    pub fn is_from_evaluation(&self) -> bool {
        self.from_evaluation
    }

    /// Go's catch-all `ER_UNKNOWN_ERROR` (1105), whose SQLSTATE is HY000.
    pub fn unknown(message: impl Into<String>) -> Self {
        Self::with_state(1105, *b"HY000", message)
    }
}

impl std::fmt::Display for MysqlError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MysqlError {}

impl From<String> for MysqlError {
    fn from(message: String) -> Self {
        Self::unknown(message)
    }
}

impl From<&str> for MysqlError {
    fn from(message: &str) -> Self {
        Self::unknown(message)
    }
}

