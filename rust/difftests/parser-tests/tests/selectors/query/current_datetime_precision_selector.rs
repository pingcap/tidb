#![allow(dead_code, missing_docs)]

//! Static Go-oracle selector for datetime precision argument boundaries.

use difftest::parser_oracle::{shared_golden, GoOutcome};

#[test]
fn negative_datetime_precision_matches_go_rejection() {
    let records = shared_golden().expect("read checked Go parser oracle");
    for line in [1599, 1605] {
        let selected: Vec<_> = records
            .iter()
            .filter(|record| {
                (record.input.path.as_str(), record.input.start_line)
                    == ("tests/integrationtest/t/expression/builtin.test", line)
            })
            .collect();
        assert_eq!(
            selected.len(),
            1,
            "datetime precision selector drifted at {line}"
        );
        let record = selected[0];
        assert_eq!(record.outcome, GoOutcome::Rejected);
        assert_eq!(record.statement_count, 0);
        assert!(
            tidb_parser::parse(&record.input.sql).is_err(),
            "Rust accepted Go-rejected datetime precision: {}",
            record.input.sql
        );
    }
}
