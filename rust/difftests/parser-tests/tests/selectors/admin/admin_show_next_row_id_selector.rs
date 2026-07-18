#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

/// This selects only the table/`NEXT_ROW_ID` branch, not sibling `ADMIN SHOW`
/// grammars such as DDL job inspection or slow-query summaries.
fn is_admin_show_next_row_id(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    matches!(
        (words.next(), words.next(), words.next(), words.next()),
        (Some(admin), Some(show), Some(_table), Some(next_row_id))
            if admin.eq_ignore_ascii_case("admin")
                && show.eq_ignore_ascii_case("show")
                && next_row_id.trim_end_matches(';').eq_ignore_ascii_case("next_row_id")
    ) && words.next().is_none()
}

#[test]
fn admin_show_next_row_id_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_admin_show_next_row_id(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 1, "source-backed selector drifted");

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
