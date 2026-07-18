#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

/// This slice is deliberately only bare `SHOW TABLES` and the `LIKE` branch.
/// `FULL`, `FROM|IN`, and `WHERE` are separate Go grammar forms with fields
/// that are not represented by `ShowTablesStmt` yet.
fn is_selected_show_tables(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    if !matches!(
        (words.next(), words.next()),
        (Some(show), Some(tables))
            if show.eq_ignore_ascii_case("show")
                && tables.trim_end_matches(';').eq_ignore_ascii_case("tables")
    ) {
        return false;
    }
    match words.next() {
        None | Some(";") => true,
        Some(word) => word.eq_ignore_ascii_case("like"),
    }
}

#[test]
fn show_tables_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_selected_show_tables(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 18, "source-backed selector drifted");

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
