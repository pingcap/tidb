#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

/// The Go hand parser has one structurally related family for partition
/// maintenance. This selector deliberately excludes ADD/DROP/EXCHANGE (their
/// independent AST contracts already have selectors) and keeps only actions
/// represented by this transition.
fn is_partition_maintenance(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    upper.starts_with("ALTER TABLE ")
        && [
            " REORGANIZE PARTITION",
            " COALESCE PARTITION",
            " TRUNCATE PARTITION",
            " REMOVE PARTITIONING",
            " REBUILD PARTITION",
            " OPTIMIZE PARTITION",
            " REPAIR PARTITION",
        ]
        .iter()
        .any(|needle| upper.contains(needle))
}

#[test]
fn alter_partition_maintenance_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_partition_maintenance(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 116, "source-backed selector drifted");

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
