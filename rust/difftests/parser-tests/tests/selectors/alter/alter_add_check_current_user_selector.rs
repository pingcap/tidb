#![allow(dead_code, missing_docs)]

//! Static-Go-oracle slice for bare `CURRENT_USER` in an `ALTER TABLE ADD
//! CHECK` expression. The shared ADD CHECK selector intentionally excludes
//! this expression-keyword seam until the source-owned function grammar is
//! proven here.

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn is_bare_current_user_add_check(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    upper.starts_with("ALTER TABLE ")
        && upper.contains(" ADD CHECK ")
        && upper.contains("CURRENT_USER !=")
}

#[test]
fn alter_add_check_bare_current_user_rows_match_go_exactly() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_bare_current_user_add_check(&record.input.sql)
        })
        .collect();
    assert_eq!(
        selected.len(),
        1,
        "source-backed CURRENT_USER selector drifted"
    );

    let mut failures = Vec::new();
    for record in selected {
        match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {}
            Ok(statement) => failures.push(format!(
                "{}:{}\n  sql: {}\n  go: {}\n  rust: {}",
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
