#![allow(missing_docs)]

//! Checked Go-oracle rows for `ALTER TABLE ... DROP FOREIGN KEY name` only.

use difftest::parser_oracle::{shared_golden, GoOutcome};

const DROP_FOREIGN_KEY_ROWS: [(&str, usize); 16] = [
    ("tests/integrationtest/t/ddl/bdr_mode.test", 30),
    ("tests/integrationtest/t/ddl/bdr_mode.test", 32),
    ("tests/integrationtest/t/ddl/bdr_mode.test", 234),
    ("tests/integrationtest/t/ddl/bdr_mode.test", 236),
    ("tests/integrationtest/t/ddl/bdr_mode.test", 447),
    ("tests/integrationtest/t/ddl/db_table.test", 21),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        504,
    ),
    ("tests/integrationtest/t/ddl/foreign_key.test", 207),
    ("tests/integrationtest/t/ddl/multi_schema_change.test", 373),
    ("tests/integrationtest/t/ddl/multi_schema_change.test", 375),
    ("tests/integrationtest/t/ddl/partition.test", 170),
    ("tests/integrationtest/t/executor/foreign_key.test", 157),
    ("tests/integrationtest/t/executor/foreign_key.test", 162),
    ("tests/integrationtest/t/executor/foreign_key.test", 232),
    ("tests/integrationtest/t/executor/foreign_key.test", 256),
    ("tests/integrationtest/t/executor/foreign_key.test", 280),
];

#[test]
fn alter_drop_foreign_key_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && DROP_FOREIGN_KEY_ROWS
                    .contains(&(record.input.path.as_str(), record.input.start_line))
        })
        .collect();
    assert_eq!(rows.len(), DROP_FOREIGN_KEY_ROWS.len());

    let failures: Vec<_> = rows
        .into_iter()
        .filter_map(|record| match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                None
            }
            Ok(statement) => Some(format!(
                "{}:{}: {}\n  go: {}\n  rust: {}",
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
