#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

/// Direct `parseUpdateStmt`/`parseDeleteStmt` tails: query-containing DML is
/// excluded because its inner query grammar, not the outer ORDER/LIMIT tail,
/// would decide the result.
fn has_simple_dml_order_or_limit_tail(sql: &str) -> bool {
    let sql = sql.trim_start().to_ascii_uppercase();
    (sql.starts_with("UPDATE ") || sql.starts_with("DELETE "))
        && !sql.contains("SELECT ")
        && (sql.contains(" ORDER BY ") || sql.contains(" LIMIT "))
}

#[test]
fn simple_update_delete_order_limit_tails_match_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && has_simple_dml_order_or_limit_tail(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 23, "source-backed selector drifted");

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
