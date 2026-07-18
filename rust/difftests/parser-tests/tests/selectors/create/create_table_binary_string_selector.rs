#![allow(dead_code, missing_docs)]

//! Static-Go-oracle slice for `parseStringOptions`' binary charset forms.

use difftest::parser_oracle::{shared_golden, GoOutcome};

#[derive(Debug, Default, Eq, PartialEq)]
struct Counts {
    matched: usize,
    parse_failure: usize,
    restore_mismatch: usize,
}

#[test]
fn create_table_binary_string_rows_are_explicitly_accounted() {
    let records = shared_golden().expect("checked static Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            let upper = record.input.sql.to_ascii_uppercase();
            upper.starts_with("CREATE ")
                && upper.contains(" TABLE ")
                && record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && (upper.contains(" CHARACTER SET BINARY") || upper.contains(" CHARSET BINARY"))
        })
        .collect();
    assert_eq!(
        selected.len(),
        11,
        "binary-string source selector unexpectedly changed"
    );
    let mut counts = Counts::default();
    for record in selected.iter().copied() {
        match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                counts.matched += 1
            }
            Ok(_) => counts.restore_mismatch += 1,
            Err(_) => counts.parse_failure += 1,
        }
    }
    assert_eq!(
        counts,
        Counts {
            matched: 11,
            parse_failure: 0,
            restore_mismatch: 0,
        },
        "binary-string source slice changed; inspect every outcome before updating this snapshot"
    );
}
