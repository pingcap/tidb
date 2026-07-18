#![allow(dead_code, missing_docs)]

//! Static Go-oracle selector for ALTER COLUMN collation validation.

use difftest::parser_oracle::{shared_golden, GoOutcome};

#[test]
fn invalid_alter_column_collation_matches_go_rejection() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            (record.input.path.as_str(), record.input.start_line)
                == ("tests/integrationtest/t/collation_misc.test", 36)
        })
        .collect();
    assert_eq!(
        selected.len(),
        1,
        "collation validation source selector drifted"
    );
    let record = selected[0];
    assert_eq!(record.outcome, GoOutcome::Rejected);
    assert_eq!(record.statement_count, 0);
    assert!(
        tidb_parser::parse(&record.input.sql).is_err(),
        "Rust accepted Go-rejected collation: {}",
        record.input.sql
    );
}
