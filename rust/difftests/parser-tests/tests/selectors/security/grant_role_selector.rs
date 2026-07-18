#![allow(missing_docs)]

//! Checked Go-oracle rows for the `GRANT role [, ...] TO user [, ...]` R1 family.

use difftest::parser_oracle::{shared_golden, GoOutcome};

const GRANT_ROLE_ROWS: [(&str, usize); 5] = [
    ("tests/integrationtest/t/executor/simple.test", 110),
    ("tests/integrationtest/t/executor/simple.test", 225),
    ("tests/integrationtest/t/executor/simple.test", 227),
    ("tests/integrationtest/t/privilege/privileges.test", 215),
    ("tests/integrationtest/t/privilege/privileges.test", 225),
];

#[test]
fn grant_role_r1_static_go_rows_match() {
    assert_rows_match(&GRANT_ROLE_ROWS);
}

fn assert_rows_match(expected: &[(&str, usize)]) {
    let records = shared_golden().expect("read checked Go parser oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && expected.contains(&(record.input.path.as_str(), record.input.start_line))
        })
        .collect();
    assert_eq!(rows.len(), expected.len());

    let failures: Vec<_> = rows
        .into_iter()
        .filter_map(|record| match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                None
            }
            Ok(statement) => Some(format!(
                "{}:{}: {}\n  go: {}\n  rust: {}",
                record.input.path,
                record.input.start_line,
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => Some(format!(
                "{}:{}: {}\n  parse error: {error:?}",
                record.input.path, record.input.start_line, record.input.sql
            )),
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn grant_role_all_no_on_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| {
            let sql = record.input.sql.to_ascii_lowercase();
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && sql.starts_with("grant ")
                && sql.contains(" to ")
                && !sql.contains(" on ")
        })
        .collect();
    assert_eq!(rows.len(), 21, "role branch source inventory drifted");
    assert_role_rows_restore(&rows);
}

fn assert_role_rows_restore(rows: &[&difftest::parser_oracle::GoldenRecord]) {
    let failures: Vec<_> = rows
        .iter()
        .filter_map(|record| match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                None
            }
            Ok(statement) => Some(format!(
                "{}:{}\n  go: {}\n  rust: {}",
                record.input.path,
                record.input.start_line,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => Some(format!(
                "{}:{} parse error: {error:?}",
                record.input.path, record.input.start_line
            )),
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
