#![allow(missing_docs)]

//! Checked Go-oracle rows for standalone physical/MERGE ALTER TABLE options.

use difftest::parser_oracle::{shared_golden, GoOutcome};

const GENERIC_OPTION_ROWS: [(&str, usize); 3] = [
    ("tests/integrationtest/t/ddl/table_modify.test", 54),
    ("tests/integrationtest/t/ddl/table_modify.test", 62),
    ("tests/integrationtest/t/ddl/table_modify.test", 72),
];

#[test]
fn alter_table_generic_options_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && GENERIC_OPTION_ROWS
                    .contains(&(record.input.path.as_str(), record.input.start_line))
        })
        .collect();
    assert_eq!(rows.len(), GENERIC_OPTION_ROWS.len(), "selector drifted");

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
