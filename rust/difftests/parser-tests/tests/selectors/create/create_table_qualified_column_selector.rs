#![allow(dead_code, missing_docs)]

use std::collections::BTreeSet;

use difftest::parser_oracle::{shared_golden, GoOutcome};

const QUALIFIED_COLUMN_ROWS: [(&str, usize); 7] = [
    ("tests/integrationtest/t/ddl/db_integration.test", 81),
    ("tests/integrationtest/t/ddl/db_integration.test", 83),
    ("tests/integrationtest/t/ddl/db_integration.test", 85),
    ("tests/integrationtest/t/ddl/db_integration.test", 183),
    ("tests/integrationtest/t/executor/executor.test", 1115),
    ("tests/integrationtest/t/executor/executor.test", 1116),
    ("tests/integrationtest/t/executor/executor.test", 1118),
];

#[test]
fn create_table_qualified_column_names_match_go() {
    let expected: BTreeSet<_> = QUALIFIED_COLUMN_ROWS.into_iter().collect();
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| expected.contains(&(record.input.path.as_str(), record.input.start_line)))
        .collect();
    assert_eq!(
        selected.len(),
        expected.len(),
        "qualified-column selector drifted"
    );

    let mut failures = Vec::new();
    for record in selected {
        assert_eq!(record.outcome, GoOutcome::Accepted, "{}", record.input.sql);
        assert_eq!(record.statement_count, 1, "{}", record.input.sql);
        match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {}
            Ok(statement) => failures.push(format!(
                "{}:{}\n  sql: {}\n   go: {}\n rust: {}",
                record.input.path,
                record.input.start_line,
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => failures.push(format!(
                "{}:{}\n  sql: {}\n  parse error: {error:?}",
                record.input.path, record.input.start_line, record.input.sql
            )),
        }
    }
    assert!(
        failures.is_empty(),
        "{} qualified-column mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
