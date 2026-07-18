#![allow(dead_code, missing_docs)]

//! Static-Go-oracle slice for bare `LOCALTIME`/`LOCALTIMESTAMP` in
//! `CREATE TABLE` CHECK expressions.  The surrounding CHECK grammar already
//! owns the constraint; this selector isolates the expression-keyword gap so
//! it can be ported without claiming unrelated CHECK semantics.

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn is_bare_time_check(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    upper.starts_with("CREATE ")
        && upper.contains(" TABLE ")
        && upper.contains("CHECK")
        && (upper.contains("LOCALTIME >") || upper.contains("LOCALTIMESTAMP >"))
}

#[test]
fn create_table_check_time_rows_match_go_exactly() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_bare_time_check(&record.input.sql)
        })
        .collect();
    assert_eq!(
        selected.len(),
        2,
        "CREATE TABLE CHECK time-keyword source selector unexpectedly changed"
    );

    let mut failures = Vec::new();
    for record in selected {
        match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {}
            Ok(statement) => failures.push(format!(
                "{}:{}\n  sql: {}\n  go: {}\n rust: {}",
                record.input.path,
                record.input.start_line,
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => failures.push(format!(
                "{}:{}\n  sql: {}\n  parse error: {error:?}",
                record.input.path, record.input.start_line, record.input.sql
            )),
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
