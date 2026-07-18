#![allow(dead_code, missing_docs)]

//! Static-Go-oracle selector for the Issue57675 binary INSERT row.

use difftest::parser_oracle::{shared_golden, GoOutcome};

#[test]
fn issue_57675_insert_binary_escapes_match_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && record.input.path == "tests/integrationtest/t/table/partition.test"
                && record.input.start_line == 449
                && record.input.end_line == 449
        })
        .collect();
    assert_eq!(selected.len(), 1, "Issue57675 source row drifted");

    let record = selected[0];
    let statement = tidb_parser::parse(&record.input.sql).expect("Rust parses Issue57675");
    assert_eq!(
        statement.restore().as_bytes(),
        record.restores[0].as_slice(),
        "Rust restore drifted from Go for {}:{}",
        record.input.path,
        record.input.start_line
    );
}
