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

#![allow(missing_docs)]

// Source anchors:
// - pkg/sessionctx/stmtctx/stmtctx.go:311-318, 1033-1156
//   StatementContext owns affected rows, last-insert ID, message, and warning
//   count; these fields are copied without deriving anything from Datum rows.
// - pkg/server/conn.go:1684-1715
//   writeOK emits affected rows, last-insert ID, status flags, warning count,
//   and a length-encoded info message.
// - pkg/executor/adapter.go:1653-1665
//   FinishExecuteStmt is the statement publication boundary.

use tidb_exec::{
    finish_and_snapshot, StatementKind, StatementStatus, StatementWarning, StatusResultSnapshot,
    WarningLevel,
};

#[test]
fn dml_status_maps_to_ok_and_text_result_options() {
    let mut status = StatementStatus::default();
    status.begin_statement(StatementKind::Dml);
    status.add_affected_rows(3);
    status.set_last_insert_id(17);
    status.warn("truncated");
    status.set_message("Records: 3  Warnings: 1");

    let snapshot = finish_and_snapshot(&mut status, 2, true, true);

    assert_eq!(snapshot.ok_packet.affected_rows, 3);
    assert_eq!(snapshot.ok_packet.last_insert_id, 17);
    assert_eq!(snapshot.ok_packet.status_flags, 2);
    assert_eq!(snapshot.ok_packet.warnings, 1);
    assert_eq!(snapshot.ok_packet.info, b"Records: 3  Warnings: 1");
    assert!(snapshot.ok_packet.protocol_41);
    assert_eq!(snapshot.result_set_options.status_flags, 2);
    assert_eq!(snapshot.result_set_options.warnings, 1);
    assert!(snapshot.result_set_options.deprecate_eof);
    assert!(snapshot.result_set_options.protocol_41);
    assert_eq!(snapshot.published.row_count, 3);
}

#[test]
fn select_status_preserves_previous_insert_id_without_fabricating_affected_rows() {
    let mut status = StatementStatus::default();
    status.begin_statement(StatementKind::Dml);
    status.set_last_insert_id(42);
    status.add_affected_rows(2);
    finish_and_snapshot(&mut status, 0, false, false);

    status.begin_statement(StatementKind::Select);
    let snapshot = finish_and_snapshot(&mut status, 4, false, false);

    assert_eq!(snapshot.published.row_count, -1);
    assert_eq!(snapshot.published.last_insert_id, 42);
    assert_eq!(snapshot.ok_packet.affected_rows, 0);
    assert_eq!(snapshot.ok_packet.last_insert_id, 42);
    assert_eq!(snapshot.ok_packet.status_flags, 4);
    assert!(!snapshot.ok_packet.protocol_41);
    assert!(!snapshot.result_set_options.protocol_41);
}

#[test]
fn warning_count_is_protocol_sized_and_warning_payload_is_not_reinterpreted() {
    let warnings = vec![
        StatementWarning::new(WarningLevel::Error, "error"),
        StatementWarning::new(WarningLevel::Note, "note"),
    ];
    let published = tidb_exec::PublishedStatementStatus {
        affected_rows: 0,
        row_count: -1,
        last_insert_id: 0,
        warnings,
        message: "TEXT:message".to_owned(),
        exec_success: true,
    };

    let snapshot = StatusResultSnapshot::from_published(&published, 0, false, true);

    assert_eq!(snapshot.ok_packet.warnings, 2);
    assert_eq!(snapshot.result_set_options.warnings, 2);
    assert_eq!(snapshot.published.warnings, published.warnings);
    assert_eq!(snapshot.ok_packet.info, b"TEXT:message");
}

#[test]
fn ok_and_eof_warning_counts_wrap_at_the_go_uint16_boundary() {
    for (warning_len, expected) in [(usize::from(u16::MAX), u16::MAX), (1 << 16, 0)] {
        let mut status = StatementStatus::default();
        status.begin_statement(StatementKind::Session);
        status.set_warnings(vec![
            StatementWarning::new(WarningLevel::Error, "error");
            warning_len
        ]);
        let snapshot = finish_and_snapshot(&mut status, 0, false, true);
        assert_eq!(snapshot.ok_packet.warnings, expected);
        assert_eq!(snapshot.result_set_options.warnings, expected);
        assert_eq!(snapshot.published.warnings.len(), warning_len);
    }
}
