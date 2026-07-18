#![allow(dead_code, missing_docs)]

//! Static Go-oracle selector for invalid CREATE TABLE charset names.

use std::collections::BTreeSet;

use difftest::parser_oracle::{shared_golden, GoOutcome};

const INVALID_CHARSET_ROWS: [(&str, usize); 16] = [
    ("tests/integrationtest/t/ddl/db_integration.test", 161),
    ("tests/integrationtest/t/ddl/db_integration.test", 163),
    ("tests/integrationtest/t/ddl/db_integration.test", 165),
    ("tests/integrationtest/t/ddl/table_modify.test", 8),
    ("tests/integrationtest/t/ddl/table_modify.test", 24),
    ("tests/integrationtest/t/ddl/table_modify.test", 26),
    ("tests/integrationtest/t/ddl/table_modify.test", 28),
    ("tests/integrationtest/t/ddl/table_modify.test", 30),
    ("tests/integrationtest/t/ddl/table_modify.test", 32),
    ("tests/integrationtest/t/ddl/table_modify.test", 36),
    ("tests/integrationtest/t/ddl/table_modify.test", 38),
    ("tests/integrationtest/t/ddl/table_modify.test", 40),
    ("tests/integrationtest/t/ddl/table_modify.test", 42),
    ("tests/integrationtest/t/ddl/table_modify.test", 44),
    ("tests/integrationtest/t/ddl/table_modify.test", 46),
    ("tests/integrationtest/t/ddl/table_modify.test", 48),
];

#[test]
fn invalid_create_table_charsets_match_go_rejection() {
    let expected: BTreeSet<_> = INVALID_CHARSET_ROWS.into_iter().collect();
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| expected.contains(&(record.input.path.as_str(), record.input.start_line)))
        .collect();
    assert_eq!(
        selected.len(),
        expected.len(),
        "invalid charset selector drifted"
    );

    for record in selected {
        assert_eq!(record.outcome, GoOutcome::Rejected, "{}", record.input.sql);
        assert!(
            tidb_parser::parse(&record.input.sql).is_err(),
            "Rust accepted Go-rejected invalid charset at {}:{}: {}",
            record.input.path,
            record.input.start_line,
            record.input.sql
        );
    }
}
