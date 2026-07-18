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

use tidb_error::mysql::FormatArg;
use tidb_error::terror::ERR_RESULT_UNDETERMINED;
use tidb_exec::{
    warnings_from_json, warnings_to_json, IgnoreWarnings, StatementWarning, StaticWarningHandler,
    WarningAppender, WarningHandler, WarningLevel, WarningPublication, MAX_WARNING_COUNT,
};

#[test]
fn sql_warn_json_round_trips_levels_and_error_messages() {
    let undetermined = ERR_RESULT_UNDETERMINED
        .fast_generate(
            ERR_RESULT_UNDETERMINED.message(),
            &[FormatArg::from("unknown")],
        )
        .to_string();
    let warnings = vec![
        StatementWarning::new(WarningLevel::Error, "any error"),
        StatementWarning::new(WarningLevel::Error, "any error"),
        StatementWarning::new(WarningLevel::Warning, undetermined.clone()),
        StatementWarning::new(WarningLevel::Warning, undetermined),
        StatementWarning::new(WarningLevel::Note, "EOF"),
    ];
    let encoded = warnings_to_json(&warnings).unwrap();
    let decoded = warnings_from_json(&encoded).unwrap();
    assert_eq!(decoded, warnings);
}

#[test]
fn ignore_warn_discards_every_operation() {
    let handler = IgnoreWarnings;
    assert_eq!(handler.warning_count(), 0);

    handler.append_warning("warn0".to_owned());
    assert_eq!(handler.warning_count(), 0);

    let mut nil_equivalent = Vec::new();
    handler.copy_warnings(&mut nil_equivalent);
    assert!(nil_equivalent.is_empty());
    let mut reserved = Vec::with_capacity(8);
    handler.copy_warnings(&mut reserved);
    assert!(reserved.is_empty());
    assert_eq!(handler.warning_count(), 0);

    handler.append_warning("warn1".to_owned());
    assert!(handler.truncate_warnings(0).is_empty());
    assert_eq!(handler.warning_count(), 0);
}

#[test]
fn static_warn_handler_copies_and_truncates_without_aliasing() {
    let handler = StaticWarningHandler::new(0);
    assert_eq!(handler.warning_count(), 0);
    for message in ["warn0", "warn1", "warn2", "warn3"] {
        handler.append_warning(message.to_owned());
    }
    assert_eq!(handler.warning_count(), 4);
    let expected = ["warn0", "warn1", "warn2", "warn3"]
        .map(|message| StatementWarning::new(WarningLevel::Warning, message))
        .to_vec();

    let mut copied = Vec::new();
    handler.copy_warnings(&mut copied);
    assert_eq!(copied, expected);
    assert_eq!(handler.warnings_snapshot(), expected);
    copied[0].message = "changed".to_owned();
    assert_eq!(handler.warnings_snapshot()[0].message, "warn0");

    let mut enough_capacity = Vec::with_capacity(8);
    let original_pointer = enough_capacity.as_ptr();
    handler.copy_warnings(&mut enough_capacity);
    assert_eq!(enough_capacity, expected);
    assert_eq!(enough_capacity.as_ptr(), original_pointer);

    let mut insufficient = Vec::with_capacity(1);
    let original_pointer = insufficient.as_ptr();
    handler.copy_warnings(&mut insufficient);
    assert_eq!(insufficient, expected);
    assert_ne!(insufficient.as_ptr(), original_pointer);

    let mut short_with_capacity = Vec::with_capacity(8);
    short_with_capacity.push(StatementWarning::new(WarningLevel::Note, "old"));
    let original_pointer = short_with_capacity.as_ptr();
    handler.copy_warnings(&mut short_with_capacity);
    assert_eq!(short_with_capacity, expected);
    assert_eq!(short_with_capacity.as_ptr(), original_pointer);

    assert!(handler.truncate_warnings(4).is_empty());
    assert!(handler.truncate_warnings(5).is_empty());
    assert_eq!(handler.truncate_warnings(2), expected[2..]);
    assert_eq!(handler.warnings_snapshot(), expected[..2]);
    assert_eq!(handler.warning_count(), 2);

    assert_eq!(handler.truncate_warnings(0), expected[..2]);
    assert!(handler.warnings_snapshot().is_empty());
    assert_eq!(handler.warning_count(), 0);

    let mut empty_copy = Vec::new();
    handler.copy_warnings(&mut empty_copy);
    assert!(empty_copy.is_empty());
}

#[test]
fn copied_warn_handler_has_independent_storage() {
    let first = StaticWarningHandler::new(0);
    for message in ["warn0", "warn1", "warn2"] {
        first.append_warning(message.to_owned());
    }

    let second = StaticWarningHandler::from_handler(Some(&first));
    assert_eq!(second.warning_count(), 3);
    assert_eq!(second.warnings_snapshot(), first.warnings_snapshot());
    second.append_warning("second-only".to_owned());
    assert_eq!(first.warning_count(), 3);
    assert_eq!(second.warning_count(), 4);

    let empty = StaticWarningHandler::from_handler(None);
    assert_eq!(empty.warning_count(), 0);
}

#[test]
fn mutable_handler_preserves_levels_batch_cap_and_error_counts() {
    let handler = StaticWarningHandler::new(2);
    handler.append_note("note".to_owned());
    handler.append_error("error");
    handler.append_warnings([StatementWarning::new(WarningLevel::Warning, "warning")]);
    assert_eq!(handler.num_error_warnings(), (1, 3));
    assert_eq!(
        handler
            .warnings_snapshot()
            .iter()
            .map(|warning| warning.level)
            .collect::<Vec<_>>(),
        [
            WarningLevel::Note,
            WarningLevel::Error,
            WarningLevel::Warning
        ]
    );
    handler.reset();
    assert_eq!(handler.warning_count(), 0);
}

#[test]
fn publication_preserves_source_warning_order_and_levels() {
    // Source: pkg/util/context/warn.go:27-39, 141-168, 177-207, and
    // pkg/sessionctx/stmtctx/stmtctx.go:1136-1186. The publication leaf reads
    // the ordered entries but does not create the source's mutable handler.
    let warnings = vec![
        StatementWarning::new(WarningLevel::Error, "first"),
        StatementWarning::new(WarningLevel::Warning, "second"),
        StatementWarning::new(WarningLevel::Note, "third"),
    ];
    let publication = WarningPublication::new(&warnings);

    assert_eq!(publication.warnings(), warnings.as_slice());
    assert_eq!(
        publication.levels().collect::<Vec<_>>(),
        [
            WarningLevel::Error,
            WarningLevel::Warning,
            WarningLevel::Note
        ]
    );
    assert_eq!(publication.warnings()[1].message, "second");
}

#[test]
fn publication_reports_total_and_error_counts_without_reordering() {
    // Source: pkg/util/context/warn.go:210-215, 266-278.
    let warnings = vec![
        StatementWarning::new(WarningLevel::Error, "error-1"),
        StatementWarning::new(WarningLevel::Note, "note"),
        StatementWarning::new(WarningLevel::Error, "error-2"),
        StatementWarning::new(WarningLevel::Warning, "warning"),
    ];
    let publication = WarningPublication::new(&warnings);

    assert_eq!(publication.warning_count(), 4);
    assert_eq!(publication.num_error_warnings(), (2, 4));
    assert_eq!(publication.summary().warning_count(), 4);
    assert_eq!(publication.summary().error_count(), 2);
}

#[test]
fn publication_counts_wrap_for_large_set_warnings_inputs() {
    // Source: pkg/util/context/warn.go:258-264 and
    // pkg/sessionctx/stmtctx/stmtctx.go:1151-1157. Static append paths cap at
    // MaxUint16; direct SetWarnings can supply a larger slice, so publication
    // wraps packet-sized counts while retaining source order and length.
    let max_warnings = vec![StatementWarning::new(WarningLevel::Error, "error"); MAX_WARNING_COUNT];
    let max_publication = WarningPublication::new(&max_warnings);
    assert_eq!(max_publication.warning_count(), u16::MAX);
    assert_eq!(max_publication.summary().error_count(), u16::MAX);

    let wrapped_warnings =
        vec![StatementWarning::new(WarningLevel::Error, "error"); MAX_WARNING_COUNT + 1];
    let publication = WarningPublication::new(&wrapped_warnings);
    assert_eq!(publication.warning_count(), 0);
    assert_eq!(publication.summary().error_count(), 0);
    assert_eq!(publication.num_error_warnings(), (0, MAX_WARNING_COUNT + 1));
    assert_eq!(publication.warnings().len(), MAX_WARNING_COUNT + 1);
}
