#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn is_admin_cleanup_table_lock(sql: &str) -> bool {
    let words: Vec<_> = sql
        .split_ascii_whitespace()
        .map(|word| word.trim_end_matches(';'))
        .collect();
    words.len() >= 5
        && words[0].eq_ignore_ascii_case("ADMIN")
        && words[1].eq_ignore_ascii_case("CLEANUP")
        && words[2].eq_ignore_ascii_case("TABLE")
        && words[3].eq_ignore_ascii_case("LOCK")
}

#[test]
fn admin_cleanup_table_lock_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_admin_cleanup_table_lock(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 1, "source-backed selector drifted");

    let failures: Vec<_> = selected
        .into_iter()
        .filter_map(|record| match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                None
            }
            Ok(statement) => Some(format!(
                "{}\n  go: {}\n rust: {}",
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => Some(format!("{}\n  parse error: {error:?}", record.input.sql)),
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
