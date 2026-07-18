#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

/// The exact `Num` branch of Go's `parseAlterAdd`: `ADD PARTITION PARTITIONS
/// uint64`. Definitions use the other branch (`PartDefinitions`) and remain
/// deliberately outside this typed count-only parser slice.
fn is_add_partition_count(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    upper.starts_with("ALTER TABLE ") && upper.contains(" ADD PARTITION PARTITIONS ")
}

#[test]
fn alter_add_partition_count_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_add_partition_count(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 8, "source-backed selector drifted");

    let mut failures = Vec::new();
    for record in selected {
        match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {}
            Ok(statement) => failures.push(format!(
                "{}\n  go: {}\n rust: {}",
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => failures.push(format!("{}\n  parse error: {error:?}", record.input.sql)),
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
