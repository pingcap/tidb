#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

/// Directly selects the query-shaped `CREATE|DROP [GLOBAL|SESSION] BINDING`
/// surface owned by `pkg/parser/binding_parser.go`.  Nested DML lives in the
/// DML grammar wave, so this deliberately stops at SELECT bodies rather
/// than accidentally taking ownership of UPDATE/DELETE/INSERT parsing.
fn is_query_binding(sql: &str) -> bool {
    let sql = sql.trim_start().to_ascii_uppercase();
    let prefixes = [
        "CREATE GLOBAL BINDING FOR ",
        "CREATE SESSION BINDING FOR ",
        "CREATE GLOBAL BINDING USING ",
        "CREATE SESSION BINDING USING ",
        "DROP GLOBAL BINDING FOR ",
        "DROP SESSION BINDING FOR ",
    ];
    prefixes.into_iter().any(|prefix| {
        sql.strip_prefix(prefix)
            .is_some_and(|body| body.starts_with("SELECT "))
    })
}

#[test]
fn query_binding_wrapper_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_query_binding(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 60, "source-backed selector drifted");

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
