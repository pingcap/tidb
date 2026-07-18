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

//! Source-shaped execution-error attachment at the MySQL connection boundary.
//!
//! Go's `clientConn.dispatch` returns an execution error to the connection
//! loop, and that loop calls `clientConn.writeError` with the source-rendered
//! error (`pkg/server/conn.go:1338-1340,1725-1768`).  The Rust dispatcher keeps
//! the same separation: [`crate::DispatchError::Execution`] remains an error
//! until the connection owner has the rendered message and negotiated
//! capabilities.  This leaf attaches that typed error to one sequence-one
//! [`crate::FramedResponse`] without inventing context.
//!
//! Error conversion belongs to `tidb-exec::RenderedExecError` and its
//! `exec_error_descriptor` constructor, ERR field conversion and payload bytes
//! belong to `tidb-protocol`, and packet framing belongs to the server.
//! Keeping those owners separate prevents a server fallback from formatting a
//! guessed table/column/parser message.

use tidb_exec::{ExecError, RenderedExecError};
use tidb_protocol::{encode_error_packet, error_packet_from_descriptor};

use crate::{frame_payloads, DispatchError, FramedResponse};

/// Frames one source-rendered executor error as a MySQL ERR response.
///
/// `rendered_message` must be the message produced by the session/error
/// context.  It is copied byte-for-byte, including non-UTF-8 bytes; this
/// function never formats an [`ExecError`] or synthesizes table, column, row,
/// or parser context.  The logical ERR payload is framed at server sequence
/// one because it follows a sequence-zero command packet, matching
/// `clientConn.writeError` after `clientConn.dispatch`.
///
/// The `protocol_41` flag controls only the `#` + SQLSTATE fields.  Legacy
/// clients receive the same code and message without those fields, exactly as
/// the Go writer does when `CLIENT_PROTOCOL_41` was not negotiated.
pub fn frame_execution_error_response(
    error: &ExecError,
    rendered_message: impl Into<Vec<u8>>,
    protocol_41: bool,
) -> Result<FramedResponse, DispatchError> {
    let rendered = RenderedExecError::new(error, rendered_message);
    frame_rendered_error_response(&rendered, protocol_41)
}

/// Frames a session-rendered executor error while retaining its optional
/// statement-context attachment for the caller.
///
/// The wire ERR packet contains only errno, SQLSTATE (when Protocol 4.1 is
/// negotiated), and the already-rendered message.  A [`RenderedExecError`]
/// may additionally carry the exact `StatementContext` status snapshot,
/// including its separate success bit, for logging or a later response owner,
/// but this function deliberately does not
/// copy warnings or status fields into the ERR payload.  That matches Go's
/// `writeError` (`pkg/server/conn.go:1725-1768`) and avoids inventing a warning
/// or informational message when the session did not publish one.
pub fn frame_rendered_error_response(
    rendered: &RenderedExecError,
    protocol_41: bool,
) -> Result<FramedResponse, DispatchError> {
    let packet = error_packet_from_descriptor(rendered.descriptor(), protocol_41);
    let payload = encode_error_packet(&packet);
    frame_payloads(std::slice::from_ref(&payload), 1).map(FramedResponse::Packets)
}
