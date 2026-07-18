#![allow(dead_code, missing_docs)]

//! Exact CREATE TABLE MERGE/UNION parser row from TiDB's fixture.

use std::collections::BTreeSet;

use difftest::parser_oracle::{shared_golden, GoOutcome};

const MERGE_UNION_ROWS: [(&str, usize); 1] =
    [("tests/integrationtest/t/ddl/table_modify.test", 52)];

#[test]
fn create_table_merge_union_matches_go() {
    let expected: BTreeSet<_> = MERGE_UNION_ROWS.into_iter().collect();
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| expected.contains(&(record.input.path.as_str(), record.input.start_line)))
        .collect();
    assert_eq!(
        selected.len(),
        expected.len(),
        "MERGE/UNION selector drifted"
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
        "{} MERGE/UNION mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
