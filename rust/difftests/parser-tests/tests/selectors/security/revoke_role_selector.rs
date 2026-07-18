#![allow(missing_docs)]

//! Checked Go-oracle rows for the `REVOKE role [, ...] FROM user [, ...]` R1 family.

use difftest::parser_oracle::{shared_golden, GoOutcome};

const REVOKE_ROLE_ROWS: [(&str, usize); 3] = [
    ("tests/integrationtest/t/executor/simple.test", 112),
    ("tests/integrationtest/t/executor/simple.test", 226),
    ("tests/integrationtest/t/executor/simple.test", 228),
];

#[test]
fn revoke_role_r1_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && REVOKE_ROLE_ROWS.contains(&(record.input.path.as_str(), record.input.start_line))
        })
        .collect();
    assert_eq!(rows.len(), REVOKE_ROLE_ROWS.len());

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
fn revoke_role_all_no_on_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| {
            let sql = record.input.sql.to_ascii_lowercase();
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && sql.starts_with("revoke ")
                && sql.contains(" from ")
                && !sql.contains(" on ")
                && !sql.starts_with("revoke all")
        })
        .collect();
    assert_eq!(rows.len(), 4, "role branch source inventory drifted");
    let failures: Vec<_> = rows
        .into_iter()
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
