use difftest::parser_oracle::{shared_golden, GoOutcome};

fn is_admin_checksum_table(sql: &str) -> bool {
    let mut words = sql.trim_start().split_ascii_whitespace();
    matches!(
        (words.next(), words.next(), words.next()),
        (Some(admin), Some(checksum), Some(table))
            if admin.eq_ignore_ascii_case("ADMIN")
                && checksum.eq_ignore_ascii_case("CHECKSUM")
                && table.eq_ignore_ascii_case("TABLE")
    )
}

#[test]
fn admin_checksum_table_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_admin_checksum_table(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 8, "source-backed selector drifted");

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
