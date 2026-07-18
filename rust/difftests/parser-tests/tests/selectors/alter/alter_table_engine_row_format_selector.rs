#![allow(missing_docs)]

//! Checked Go-oracle coverage for the comma-separated ENGINE/ROW_FORMAT row.

use difftest::parser_oracle::{shared_golden, GoOutcome};

const SOURCE_ROW: (&str, usize) = ("tests/integrationtest/t/util/admin.test", 16);

#[test]
fn alter_table_engine_row_format_static_go_row_matches() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let record = records
        .iter()
        .find(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && (record.input.path.as_str(), record.input.start_line) == SOURCE_ROW
        })
        .expect("source-backed ALTER option row");
    let statement = tidb_parser::parse(&record.input.sql).expect("parse source row");
    assert_eq!(
        statement.restore().as_bytes(),
        record.restores[0].as_slice()
    );
}
