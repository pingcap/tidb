#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

#[test]
fn show_fields_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            let mut words = record.input.sql.split_whitespace();
            matches!(
                (words.next(), words.next()),
                (Some(show), Some(fields))
                    if show.eq_ignore_ascii_case("show")
                        && fields.eq_ignore_ascii_case("fields")
            ) && record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
        })
        .collect();
    assert_eq!(selected.len(), 4, "source-backed selector drifted");

    let mut failures = Vec::new();
    for record in selected {
        match tidb_parser::parse(&record.input.sql) {
            Ok(stmt) if stmt.restore().as_bytes() == record.restores[0].as_slice() => {}
            Ok(stmt) => failures.push(format!(
                "{}\n  go: {}\n rust: {}",
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                stmt.restore()
            )),
            Err(err) => failures.push(format!("{}\n  parse error: {err:?}", record.input.sql)),
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
