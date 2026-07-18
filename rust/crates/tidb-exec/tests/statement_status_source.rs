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

use tidb_exec::{StatementKind, StatementStatus, StatementWarning, WarningLevel};

#[test]
fn dml_status_publishes_rows_last_insert_id_warnings_and_message() {
    let mut status = StatementStatus::default();
    status.begin_statement(StatementKind::Dml);
    status.add_affected_rows(2);
    status.add_affected_rows(3);
    status.set_affected_rows(5);
    status.add_found_rows(9);
    status.add_record_rows(5);
    status.add_deleted_rows(1);
    status.add_updated_rows(2);
    status.add_copied_rows(4);
    status.add_touched_rows(6);
    status.set_last_insert_id(7);
    status.warn("truncated");
    status.note("plan hint ignored");
    status.error("constraint diagnostic");
    status.set_message("Records: 5  Duplicates: 0  Warnings: 3");

    assert_eq!(status.affected_rows(), 5);
    assert_eq!(status.found_rows(), 9);
    assert_eq!(status.record_rows(), 5);
    assert_eq!(status.deleted_rows(), 1);
    assert_eq!(status.updated_rows(), 2);
    assert_eq!(status.copied_rows(), 4);
    assert_eq!(status.touched_rows(), 6);
    assert_eq!(status.message(), "Records: 5  Duplicates: 0  Warnings: 3");
    assert_eq!(status.warning_count(), 3);
    assert_eq!(status.warnings()[1].level, WarningLevel::Note);

    let published = status.finish_statement();
    assert_eq!(published.affected_rows, 5);
    assert_eq!(published.row_count, 5);
    assert_eq!(published.last_insert_id, 7);
    assert_eq!(published.warnings.len(), 3);
    assert_eq!(published.message, "Records: 5  Duplicates: 0  Warnings: 3");
    assert_eq!(status.previous(), &published);
}

#[test]
fn statement_kind_controls_row_count_and_previous_last_insert_id() {
    let mut status = StatementStatus::default();
    status.begin_statement(StatementKind::Dml);
    status.set_last_insert_id(42);
    status.add_affected_rows(2);
    status.finish_statement();

    status.begin_statement(StatementKind::Select);
    let select = status.finish_statement();
    assert_eq!(select.affected_rows, 0);
    assert_eq!(select.row_count, -1);
    assert_eq!(select.last_insert_id, 42);

    status.begin_statement(StatementKind::Session);
    let session = status.finish_statement();
    assert_eq!(session.row_count, 0);
    assert_eq!(session.last_insert_id, 42);

    status.begin_statement(StatementKind::Ddl);
    status.add_affected_rows(99);
    let ddl = status.finish_statement();
    assert_eq!(ddl.affected_rows, 0);
    assert_eq!(ddl.row_count, 0);
    assert_eq!(ddl.last_insert_id, 42);
}

#[test]
fn begin_clears_current_execution_state_but_retry_keeps_publish_state() {
    let mut status = StatementStatus::default();
    status.begin_statement(StatementKind::Dml);
    status.add_affected_rows(8);
    status.set_last_insert_id(11);
    status.clear_last_insert_id();
    status.set_last_insert_id(11);
    status.warn("first attempt");
    let published = status.finish_statement();

    status.add_affected_rows(4);
    status.add_found_rows(3);
    status.add_record_rows(2);
    status.add_updated_rows(5);
    status.add_copied_rows(6);
    status.add_touched_rows(7);
    status.warn("retry attempt");
    status.reset_for_retry();
    assert_eq!(status.affected_rows(), 0);
    assert_eq!(status.found_rows(), 0);
    assert_eq!(status.record_rows(), 0);
    assert_eq!(status.deleted_rows(), 0);
    assert_eq!(status.updated_rows(), 0);
    assert_eq!(status.copied_rows(), 0);
    assert_eq!(status.touched_rows(), 0);
    assert!(status.warnings().is_empty());
    assert_eq!(status.current_last_insert_id(), Some(11));
    assert_eq!(status.previous(), &published);

    status.begin_statement(StatementKind::Dml);
    assert_eq!(status.current_last_insert_id(), None);
    assert_eq!(status.affected_rows(), 0);
    assert!(status.warnings().is_empty());
    let retry = status.finish_statement();
    assert_eq!(retry.last_insert_id, 11);

    status.reset();
    assert_eq!(status.previous().row_count, 0);
    assert_eq!(status.current_last_insert_id(), None);
    assert_eq!(status.warning_count(), 0);
}

#[test]
fn warnings_preserve_order_and_cap_at_source_u16_limit() {
    let mut status = StatementStatus::default();
    status.append_warning(StatementWarning::new(WarningLevel::Error, "e"));
    status.append_warning(StatementWarning::new(WarningLevel::Warning, "w"));
    status.append_warning(StatementWarning::new(WarningLevel::Note, "n"));
    assert_eq!(
        status
            .warnings()
            .iter()
            .map(|warning| warning.level)
            .collect::<Vec<_>>(),
        [
            WarningLevel::Error,
            WarningLevel::Warning,
            WarningLevel::Note
        ]
    );

    status.append_warnings([
        StatementWarning::new(WarningLevel::Warning, "batch-1"),
        StatementWarning::new(WarningLevel::Warning, "batch-2"),
    ]);
    assert_eq!(status.warnings().len(), 5);
    status.set_warnings(vec![StatementWarning::new(WarningLevel::Note, "replaced")]);
    assert_eq!(status.warnings()[0].message, "replaced");

    status.reset();
    for index in 0..=usize::from(u16::MAX) {
        status.append_warning(StatementWarning::new(
            WarningLevel::Warning,
            index.to_string(),
        ));
    }
    assert_eq!(status.warning_count(), u16::MAX);
    assert_eq!(status.warnings().len(), usize::from(u16::MAX));
    assert_eq!(status.warnings().last().unwrap().message, "65534");
}

#[test]
fn batch_and_set_retain_65536_entries_and_publish_wrapping_counts() {
    let warnings =
        vec![StatementWarning::new(WarningLevel::Error, "error"); usize::from(u16::MAX) + 1];

    let mut status = StatementStatus::default();
    status.append_warnings(warnings.clone());
    assert_eq!(status.warnings().len(), usize::from(u16::MAX) + 1);
    assert_eq!(status.warning_count(), 0);
    assert_eq!(status.num_error_warnings(), (0, usize::from(u16::MAX) + 1));
    let published = status.finish_statement();
    assert_eq!(published.warnings.len(), usize::from(u16::MAX) + 1);

    status.reset();
    status.set_warnings(warnings);
    assert_eq!(status.warnings().len(), usize::from(u16::MAX) + 1);
    assert_eq!(status.warning_count(), 0);
    assert_eq!(status.num_error_warnings(), (0, usize::from(u16::MAX) + 1));
}

#[test]
fn failed_publication_preserves_warning_order_and_marks_error_context() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:361-365, 1129-1170 and
    // pkg/executor/adapter.go:1961. ExecSuccess is a separate lifecycle bit;
    // warning levels/text stay in their original order and are not inferred
    // from the execution error.
    let mut status = StatementStatus::default();
    status.begin_statement(StatementKind::Dml);
    status.warn("truncated");
    status.error("constraint diagnostic");
    status.note("plan hint ignored");

    let published = status.finish_statement_with_outcome(false);
    assert!(!published.exec_success);
    assert_eq!(published.warnings[0].level, WarningLevel::Warning);
    assert_eq!(published.warnings[1].level, WarningLevel::Error);
    assert_eq!(published.warnings[2].level, WarningLevel::Note);
    assert_eq!(status.previous(), &published);

    status.begin_statement(StatementKind::Select);
    let next = status.finish_statement();
    assert!(next.exec_success);
}
