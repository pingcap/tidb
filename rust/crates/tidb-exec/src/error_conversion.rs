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

//! Executor-to-protocol error hand-off.
//!
//! TiDB's server boundary (`pkg/server/conn.go:1725-1768`) first turns a
//! `terror.Error` into a `mysql.SQLError` and only then writes the ERR packet.
//! The protocol crate owns that packet-facing descriptor; this leaf owns the
//! structural mapping from the executor's error vocabulary to its categories.
//!
//! The caller supplies the already rendered message bytes.  This is
//! intentional: Go's `terror.ToSQLError` preserves context produced by the
//! originating error (column/table names, clause names, row numbers, and
//! redaction), while the current Rust `ExecError` carries only part of that
//! context.  Formatting a guessed message here would silently lose source
//! behavior.  See `pkg/parser/terror/terror.go:230-273` and
//! `pkg/parser/mysql/error.go:35-61`.

use crate::ExecError;
use crate::PublishedStatementStatus;
use tidb_protocol::{ErrorDescriptor, ErrorKind};

/// A source-rendered executor error together with optional published
/// statement context.
///
/// Go's `clientConn.dispatch` returns an error to the connection loop, while
/// the statement context owns the already-rendered message and warning/status
/// state (`pkg/server/conn.go:1338-1345`,
/// `pkg/sessionctx/stmtctx/stmtctx.go:792-809,1129-1170`).  The Rust rewrite
/// keeps that ownership split explicit: this value carries only bytes and a
/// status snapshot supplied by the session owner, including the source-shaped
/// `ExecSuccess` bit. It never formats an `ExecError`, derives table/column
/// context, or turns a missing status into a synthetic default.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenderedExecError {
    descriptor: ErrorDescriptor,
    status: Option<PublishedStatementStatus>,
}

impl RenderedExecError {
    /// Creates a rendered error without statement context.
    ///
    /// This is the correct boundary for failures that happen before a
    /// statement context can publish (for example malformed command or parse
    /// setup).  The message is retained as raw bytes.
    #[must_use]
    pub fn new(error: &ExecError, message: impl Into<Vec<u8>>) -> Self {
        Self {
            descriptor: exec_error_descriptor(error, message),
            status: None,
        }
    }

    /// Creates a rendered error with an exact published statement snapshot.
    ///
    /// The caller must supply the session-owned snapshot; this constructor
    /// does not finish or mutate a [`crate::StatementStatus`] and does not
    /// infer warning counts from the error payload.
    #[must_use]
    pub fn with_status(
        error: &ExecError,
        message: impl Into<Vec<u8>>,
        status: &PublishedStatementStatus,
    ) -> Self {
        Self {
            descriptor: exec_error_descriptor(error, message),
            status: Some(status.clone()),
        }
    }

    /// Attaches an exact published statement snapshot to this rendered error.
    ///
    /// Replacing an attachment is intentional: a retry or session owner may
    /// publish a newer snapshot before handing the error to the connection
    /// writer.  No status is fabricated when the caller chooses not to call
    /// this method.
    #[must_use]
    pub fn attach_status(mut self, status: &PublishedStatementStatus) -> Self {
        self.status = Some(status.clone());
        self
    }

    /// Returns the typed protocol descriptor, including the raw message.
    #[must_use]
    pub const fn descriptor(&self) -> &ErrorDescriptor {
        &self.descriptor
    }

    /// Returns the optional session-published status snapshot.
    #[must_use]
    pub const fn status(&self) -> Option<&PublishedStatementStatus> {
        self.status.as_ref()
    }
}

/// Converts an executor error to the typed hand-off consumed by
/// [`tidb_protocol::error_packet_from_descriptor`].
///
/// `message` must be the source-rendered error text.  It is copied as bytes
/// without UTF-8 conversion or context synthesis.  Exact category mappings
/// are limited to executor variants whose Go errno is unambiguous:
/// parser errors use `mysql.ErrParse` (`pkg/parser/parser_api.go:30-31`),
/// catalog/constraint/data errors use the corresponding constants in
/// `pkg/parser/mysql/errcode.go` and `state.go`, and all remaining generic
/// execution failures retain their explicit broad category (which the
/// protocol layer maps to `ErrUnknown`).
pub fn exec_error_descriptor(error: &ExecError, message: impl Into<Vec<u8>>) -> ErrorDescriptor {
    ErrorDescriptor::new(exec_error_kind(error), message)
}

/// Returns the source-shaped protocol category for an executor error.
///
/// This function deliberately does not inspect payload strings.  For example,
/// `ExecError::Unsupported("...")` cannot prove that the originating Go path
/// used `ErrNotSupportedYet`; only a richer source error can make that choice.
/// Likewise, the generic foreign-key and evaluation variants have multiple or
/// no single MySQL errno in TiDB and therefore remain broad categories.
pub fn exec_error_kind(error: &ExecError) -> ErrorKind {
    match error {
        // pkg/parser/parser_api.go:30-31 defines parser.ErrParse as
        // mysql.ErrParse (1064, SQLSTATE 42000).
        ExecError::Parse { .. } => ErrorKind::Parse,

        // These executor categories are structural boundaries, not source
        // errno claims.  Their messages remain caller-owned and the protocol
        // converter intentionally emits ErrUnknown until a narrower Go
        // terror class is ported.
        ExecError::NotSelect => ErrorKind::NotSelect,
        ExecError::RequiresTable => ErrorKind::RequiresTable,
        ExecError::Wildcard => ErrorKind::Wildcard,
        ExecError::Unsupported(_) => ErrorKind::Unsupported,
        ExecError::Protocol(_) => ErrorKind::Protocol,

        // pkg/planner/core/planbuilder.go and pkg/executor/foreign_key.go
        // route these source failures through the corresponding parser/mysql
        // errno classes.  The protocol crate owns the exact code/state table.
        ExecError::UnknownTable(_) => ErrorKind::UnknownTable,
        ExecError::UnknownColumn(_) => ErrorKind::UnknownColumn,
        ExecError::DuplicateIndex(_) => ErrorKind::DuplicateIndex,
        ExecError::DuplicateKey => ErrorKind::DuplicateKey,
        ExecError::ColumnCountMismatch => ErrorKind::ColumnCountMismatch,
        ExecError::DataTooLong(_) => ErrorKind::DataTooLong,
        ExecError::OutOfRange(_) => ErrorKind::OutOfRange,

        // No single exact MySQL code is represented by these generic Rust
        // variants.  Keep the category explicit so a future richer adapter
        // can attach source errno/context without changing this seam.
        ExecError::UnknownSavepoint(_) => ErrorKind::UnknownSavepoint,
        ExecError::WriteConflict => ErrorKind::WriteConflict,
        ExecError::UngroupedColumn(_) => ErrorKind::UngroupedColumn,
        ExecError::ForeignKeyViolation => ErrorKind::ForeignKeyViolation,
        ExecError::Eval(_) => ErrorKind::Eval,
    }
}
