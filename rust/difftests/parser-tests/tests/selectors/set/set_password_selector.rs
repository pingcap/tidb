#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn is_set_password(sql: &str) -> bool {
    let sql = sql.trim_start();
    let phrase = "set password";
    sql.get(..phrase.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(phrase))
        && sql
            .get(phrase.len()..)
            .is_some_and(|rest| rest.starts_with(char::is_whitespace))
}

#[test]
fn set_password_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_set_password(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 20, "source-backed selector drifted");

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
