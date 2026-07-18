#![allow(dead_code, missing_docs)]

//! Full static-Go-oracle slice for creation-side table partitioning.

use difftest::parser_oracle::{shared_golden, GoOutcome};

#[derive(Debug, Default, Eq, PartialEq)]
struct Counts {
    matched: usize,
    parse_failure: usize,
    restore_mismatch: usize,
}

/// The complete creation-partition source slice is replayed, including rows
/// whose ordinary column/table payload is still independently unported. This
/// makes the residual boundary explicit instead of mistaking a parser tail
/// for partition grammar support.
#[test]
fn create_table_partition_rows_have_reviewed_static_go_outcomes() {
    let records = shared_golden().expect("checked static Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            let upper = record.input.sql.to_ascii_uppercase();
            upper.starts_with("CREATE ")
                && upper.contains(" TABLE ")
                && upper.contains("PARTITION BY")
                && record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
        })
        .collect();
    assert_eq!(
        selected.len(),
        853,
        "static oracle selector unexpectedly changed"
    );
    let mut counts = Counts::default();
    for record in selected {
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
            // Column-level CHECK, field-type aliases/modifiers (including
            // NATIONAL/NCHAR/NVARCHAR),
            // TableOptionAffinity, creation-side SPLIT, inline KEY, and
            // AUTO_RANDOM and binary string options now remain typed before
            // the same creation-side PARTITION owner runs. The binary-string
            // port resolves the former CHARSET BINARY restore mismatch.
            matched: 853,
            parse_failure: 0,
            restore_mismatch: 0,
        },
        "creation-partition source slice changed; inspect every outcome before updating this snapshot"
    );
}
