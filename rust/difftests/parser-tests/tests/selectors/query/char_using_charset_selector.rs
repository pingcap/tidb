#![allow(dead_code, missing_docs)]

//! Static Go-oracle selector for `CHAR(... USING charset)` validation.

use difftest::parser_oracle::{shared_golden, GoOutcome};

#[test]
fn char_using_invalid_charset_matches_go_rejection() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            (record.input.path.as_str(), record.input.start_line)
                == ("tests/integrationtest/t/expression/builtin.test", 612)
        })
        .collect();
    assert_eq!(selected.len(), 1, "CHAR USING source selector drifted");
    let record = selected[0];
    assert_eq!(record.outcome, GoOutcome::Rejected);
    assert_eq!(record.statement_count, 0);
    assert!(
        tidb_parser::parse(&record.input.sql).is_err(),
        "Rust accepted Go-rejected CHAR USING charset at {}:{}: {}",
        record.input.path,
        record.input.start_line,
        record.input.sql
    );
}
