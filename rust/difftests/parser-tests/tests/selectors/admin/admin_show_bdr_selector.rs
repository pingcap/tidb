#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

/// Go's `parseAdminShow` BDR branch is a distinct unit-payload form from
/// `ADMIN SET|UNSET BDR ROLE` and all other `ADMIN SHOW` variants.
fn is_admin_show_bdr_role(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    matches!(
        (words.next(), words.next(), words.next(), words.next()),
        (Some(admin), Some(show), Some(bdr), Some(role))
            if admin.eq_ignore_ascii_case("admin")
                && show.eq_ignore_ascii_case("show")
                && bdr.eq_ignore_ascii_case("bdr")
                && role.trim_end_matches(';').eq_ignore_ascii_case("role")
    )
}

#[test]
fn admin_show_bdr_role_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_admin_show_bdr_role(&record.input.sql)
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
