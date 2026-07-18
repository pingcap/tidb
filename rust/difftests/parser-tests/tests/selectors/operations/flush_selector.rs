#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

/// Selects every ported standalone Go FLUSH target. Any modifier before the
/// target and every stateful plugin/log/statistics target remain excluded.
fn is_selected_flush(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    if !matches!(words.next(), Some(flush) if flush.eq_ignore_ascii_case("flush")) {
        return false;
    }
    match words.next() {
        Some(target) => {
            let target = target.trim_end_matches(';');
            target.eq_ignore_ascii_case("status")
                || target.eq_ignore_ascii_case("privileges")
                || target.eq_ignore_ascii_case("table")
                || target.eq_ignore_ascii_case("tables")
        }
        None => false,
    }
}

#[test]
fn flush_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_selected_flush(&record.input.sql)
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
