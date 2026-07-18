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

//! Explicit statement-status to result-packet conversion.
//!
//! The Go server's `writeOK` path reads statement status fields, while a row
//! result set carries only warning/status negotiation in its metadata and
//! terminal packets.  This leaf keeps those two consumers together without
//! making the executor inspect result [`tidb_datatype::Datum`] values.  The
//! session/cluster owner still decides when a statement is finished and when
//! this snapshot is attached to a wire response.

use tidb_protocol::{OkPacket, ResultSetOptions};

use crate::{PublishedStatementStatus, StatementStatus};

/// An immutable bridge between one published statement and protocol result
/// options.
///
/// `ok_packet` contains the source `writeOK` fields (affected rows, last
/// insert ID, warning count, and info message).  `result_set_options` carries
/// the same warning count plus the caller-owned status/capability flags used
/// by the text result-set metadata and terminal packets.  No field is derived
/// from runtime rows or [`tidb_datatype::Datum`] values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatusResultSnapshot {
    /// The exact published status used to construct both protocol views.
    pub published: PublishedStatementStatus,
    /// OK-packet fields for a command with no row result set.
    pub ok_packet: OkPacket,
    /// Metadata/terminal options for a text result set.
    pub result_set_options: ResultSetOptions,
}

impl StatusResultSnapshot {
    /// Converts already-published statement status into protocol views.
    ///
    /// Status flags and capability bits belong to the connection owner, so
    /// callers must supply them instead of this leaf inventing defaults.
    pub fn from_published(
        published: &PublishedStatementStatus,
        status_flags: u16,
        deprecate_eof: bool,
        protocol_41: bool,
    ) -> Self {
        let warnings = warning_count(published);
        let info = published.message.as_bytes().to_vec();
        Self {
            published: published.clone(),
            ok_packet: OkPacket {
                affected_rows: published.affected_rows,
                last_insert_id: published.last_insert_id,
                status_flags,
                warnings,
                info,
                protocol_41,
            },
            result_set_options: ResultSetOptions {
                status_flags,
                warnings,
                deprecate_eof,
                protocol_41,
            },
        }
    }

    /// Snapshots the latest status already published by a statement owner.
    ///
    /// This method does not finish the current statement.  Use
    /// [`finish_and_snapshot`] at a statement boundary when publication is
    /// required first.
    pub fn from_status(
        status: &StatementStatus,
        status_flags: u16,
        deprecate_eof: bool,
        protocol_41: bool,
    ) -> Self {
        Self::from_published(status.previous(), status_flags, deprecate_eof, protocol_41)
    }
}

/// Finishes a statement and converts its source-shaped status in one explicit
/// operation.  The caller remains responsible for attaching the returned
/// packet/options to the matching command or row result.
pub fn finish_and_snapshot(
    status: &mut StatementStatus,
    status_flags: u16,
    deprecate_eof: bool,
    protocol_41: bool,
) -> StatusResultSnapshot {
    let published = status.finish_statement();
    StatusResultSnapshot::from_published(&published, status_flags, deprecate_eof, protocol_41)
}

fn warning_count(published: &PublishedStatementStatus) -> u16 {
    published.warnings.len() as u16
}
