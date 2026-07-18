use difftest::parser_oracle::{shared_golden, GoOutcome};

/// The direct `ADMIN SET|UNSET BDR ROLE` grammar from
/// `pkg/parser/admin_stmt_parser.go`. `ADMIN SHOW BDR ROLE` is a distinct
/// source branch and intentionally has its own future ownership slice.
fn is_admin_bdr_set_or_unset(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    let (Some(admin), Some(action), Some(bdr), Some(role)) =
        (words.next(), words.next(), words.next(), words.next())
    else {
        return false;
    };
    admin.eq_ignore_ascii_case("admin")
        && (action.eq_ignore_ascii_case("set") || action.eq_ignore_ascii_case("unset"))
        && bdr.eq_ignore_ascii_case("bdr")
        && role.trim_end_matches(';').eq_ignore_ascii_case("role")
}

#[test]
fn admin_bdr_set_or_unset_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_admin_bdr_set_or_unset(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 92, "source-backed selector drifted");

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
