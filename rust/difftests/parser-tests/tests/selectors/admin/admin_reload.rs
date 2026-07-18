use difftest::parser_oracle::{shared_golden, GoOutcome};

/// Go's `parseAdminKeywordBased` routes every accepted `ADMIN RELOAD` form
/// through its value-less reload enum. The checked static corpus currently
/// exercises both blacklist variants.
fn starts_admin_reload(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    matches!(
        (words.next(), words.next()),
        (Some(admin), Some(reload))
            if admin.eq_ignore_ascii_case("admin")
                && reload.trim_end_matches(';').eq_ignore_ascii_case("reload")
    )
}

#[test]
fn admin_reload_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && starts_admin_reload(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 28, "source-backed selector drifted");

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
