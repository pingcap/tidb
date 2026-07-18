#![allow(dead_code, missing_docs)]

//! Exact integration-parser rows made restorable by column-level CHECK.
//!
//! Keep this deliberately source-addressed rather than selecting every SQL
//! string containing CHECK: the broader integration file includes unsupported
//! expression semantic-validation cases whose parser spelling belongs to
//! independent expression domains.

use std::collections::BTreeSet;

use difftest::parser_oracle::{shared_golden, GoOutcome};

const COLUMN_CHECK_FIXTURES: [(&str, usize); 3] = [
    // Generated column plus table CHECK: proves the shared payload does not
    // disturb the column-list / table-constraint boundary.
    ("tests/integrationtest/t/ddl/constraint.test", 403),
    // A plain column CHECK in the prepared-statement constraint case.
    ("tests/integrationtest/t/ddl/constraint.test", 446),
    // One CREATE carries table CHECK, plain column CHECK, and named column
    // CHECK simultaneously.
    ("tests/integrationtest/t/ddl/constraint.test", 702),
];

#[test]
fn creation_column_check_integration_rows_match_go_exactly() {
    let expected: BTreeSet<_> = COLUMN_CHECK_FIXTURES.into_iter().collect();
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| expected.contains(&(record.input.path.as_str(), record.input.start_line)))
        .collect();
    assert_eq!(selected.len(), 3, "source-backed selector drifted");

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
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
