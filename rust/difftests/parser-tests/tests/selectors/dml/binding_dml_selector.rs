#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

/// The bindable DML body families named by TiDB's
/// `pkg/planner/core/preprocess.go:bindableStmtType`. CTE-led DML and the
/// separate parenthesized INSERT-query production stay in their own waves.
fn is_binding_dml(sql: &str) -> bool {
    let sql = sql.trim_start().to_ascii_uppercase();
    let prefixes = [
        "CREATE GLOBAL BINDING FOR ",
        "CREATE SESSION BINDING FOR ",
        "DROP GLOBAL BINDING FOR ",
        "DROP SESSION BINDING FOR ",
    ];
    prefixes.into_iter().any(|prefix| {
        sql.strip_prefix(prefix).is_some_and(|body| {
            let parenthesized_insert =
                body.starts_with("INSERT INTO ") && body.contains(" (SELECT");
            ["INSERT ", "REPLACE ", "UPDATE ", "DELETE "]
                .into_iter()
                .any(|verb| body.starts_with(verb))
                && !parenthesized_insert
        })
    })
}

#[test]
fn binding_dml_body_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_binding_dml(&record.input.sql)
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
