#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

/// `pkg/parser/dml_parser.go` calls `parseOptHints` directly after each
/// bindable DML verb. Keep this selector to that header position; table/index
/// hints and SELECT-body hints have distinct grammar owners.
fn has_dml_header_hint(sql: &str) -> bool {
    let sql = sql.trim_start().to_ascii_uppercase();
    ["INSERT /*+", "REPLACE /*+", "UPDATE /*+", "DELETE /*+"]
        .into_iter()
        .any(|prefix| sql.starts_with(prefix))
}

#[test]
fn dml_header_hints_one_statement_match_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && has_dml_header_hint(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 16, "source-backed selector drifted");

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
