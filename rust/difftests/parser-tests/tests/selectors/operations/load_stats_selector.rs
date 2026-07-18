#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn starts_load_stats(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    matches!(
        (words.next(), words.next()),
        (Some(load), Some(stats))
            if load.eq_ignore_ascii_case("load") && stats.eq_ignore_ascii_case("stats")
    )
}

#[test]
fn load_stats_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && starts_load_stats(&record.input.sql)
        })
        .collect();
    // The checked inventory contains 60 LOAD STATS inputs: 58 accepted
    // file-path forms plus two deliberately-invalid forms. This exact
    // selector guards every Go-accepted static fixture, without pretending
    // the rejected inputs are valid Rust SQL.
    assert_eq!(selected.len(), 58, "source-backed selector drifted");

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
