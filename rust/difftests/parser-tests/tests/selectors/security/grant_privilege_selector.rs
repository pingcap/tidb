#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{read_golden, repo_root, GoOutcome};

/// This is the direct `parseGrantStmt` privilege branch: its mandatory `ON`
/// proves it is not a role grant. The selected core leaves out the separate
/// Go `UserSpec` authentication and TLS/resource-option payloads. `WITH GRANT
/// OPTION` remains because it is the typed `GrantStmt.WithGrant` boolean.
fn is_core_privilege_grant(sql: &str) -> bool {
    let words = format!(" {} ", sql.trim().to_ascii_uppercase());
    words.starts_with(" GRANT ")
        && words.contains(" ON ")
        && words.contains(" TO ")
        && !words.contains(" IDENTIFIED ")
        && !words.contains(" REQUIRE ")
        && !words.contains(" WITH MAX_")
        && !words.contains(" WITH RESOURCE ")
}

#[test]
fn grant_privilege_core_one_statement_matches_go() {
    let records = read_golden(&repo_root()).expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_core_privilege_grant(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 260, "source-backed selector drifted");

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
