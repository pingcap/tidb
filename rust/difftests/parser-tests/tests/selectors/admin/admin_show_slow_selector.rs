#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

/// Go's `ADMIN SHOW SLOW` branch is distinct from DDL-job, BDR, and table
/// metadata commands even though all share the same three-word prefix.
fn is_admin_show_slow(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    matches!(
        (words.next(), words.next(), words.next()),
        (Some(admin), Some(show), Some(slow))
            if admin.eq_ignore_ascii_case("admin")
                && show.eq_ignore_ascii_case("show")
                && slow.eq_ignore_ascii_case("slow")
    )
}

#[test]
fn admin_show_slow_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_admin_show_slow(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 6, "source-backed selector drifted");

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
