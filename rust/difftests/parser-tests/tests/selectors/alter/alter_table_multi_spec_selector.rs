#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome, GoldenRecord};

/// Go's canonical `AlterTableStmt.Restore` emits `, ` only between ordinary
/// specs. Payload-owned commas use different following tokens (index/name
/// parts, literals, or expressions), so a comma followed by an ALTER action
/// keyword is a stable source-backed multi-spec boundary.
fn is_alter_table_multi_spec(record: &GoldenRecord) -> bool {
    if !record
        .input
        .sql
        .trim_start()
        .to_ascii_uppercase()
        .starts_with("ALTER TABLE ")
    {
        return false;
    }
    let restored = String::from_utf8_lossy(&record.restores[0]);
    let has_multiple_specs = [
        ", ADD ",
        ", DROP ",
        ", MODIFY ",
        ", CHANGE ",
        ", RENAME ",
        ", SET ",
        ", CONVERT ",
        ", AFFINITY ",
        ", EXCHANGE ",
        ", REORGANIZE ",
        ", COALESCE ",
        ", TRUNCATE ",
        ", REBUILD ",
        ", OPTIMIZE ",
        ", REPAIR ",
        ", SPLIT ",
    ]
    .iter()
    .any(|boundary| restored.contains(boundary));

    // This structural wave composes the action envelopes already typed by
    // Rust. Keep Go spec families which still lack such an envelope outside
    // the selector instead of confusing a missing leaf grammar with a broken
    // multi-spec owner. Column IF EXISTS/IF NOT EXISTS metadata is part of
    // the now-typed action envelopes and therefore intentionally remains in
    // this selector.
    let has_unported_action = [
        " ON UPDATE ",
        " FOREIGN KEY ",
        " GENERATED ALWAYS ",
        "ADD PRIMARY KEY",
        "RENAME INDEX",
        "DROP FOREIGN KEY",
    ]
    .iter()
    .any(|needle| restored.contains(needle));

    has_multiple_specs && !has_unported_action
}

#[test]
fn alter_table_multi_spec_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_alter_table_multi_spec(record)
        })
        .collect();
    assert_eq!(selected.len(), 57, "source-backed selector drifted");

    let failures: Vec<_> = selected
        .into_iter()
        .filter_map(|record| match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                None
            }
            Ok(statement) => Some(format!(
                "{}:{}: {}\n  go: {}\n rust: {}",
                record.input.path,
                record.input.start_line,
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => Some(format!(
                "{}:{}: {}\n  parse error: {error:?}",
                record.input.path, record.input.start_line, record.input.sql
            )),
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
