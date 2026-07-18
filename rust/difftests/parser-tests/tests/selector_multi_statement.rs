//! Stable Cargo shard for the Go parser's multi-statement envelope.

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn is_multi_statement_source(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    matches!(
        (
            record.input.path.as_str(),
            record.input.start_line,
            record.input.end_line,
        ),
        ("tests/integrationtest/t/cte.test", 140, 140)
            | ("tests/integrationtest/t/cte.test", 144, 144)
            | ("tests/integrationtest/t/session/common.test", 157, 157)
            | ("tests/integrationtest/t/expression/issues.test", 918, 918)
            | (
                "tests/integrationtest/t/planner/core/enforce_mpp.test",
                8,
                8
            )
            | (
                "tests/integrationtest/t/planner/core/plan_cache.test",
                795,
                795
            )
            | (
                "tests/integrationtest/t/planner/core/plan_cache.test",
                1203,
                1203
            )
            | (
                "tests/integrationtest/t/planner/core/plan_cache.test",
                1204,
                1204
            )
            | (
                "tests/integrationtest/t/explain_generate_column_substitute.test",
                188,
                188
            )
            | ("tests/integrationtest/t/executor/insert.test", 421, 421)
    )
}

#[test]
fn multi_statement_source_rows_restore_every_go_statement() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| record.outcome == GoOutcome::Accepted && is_multi_statement_source(record))
        .collect();
    assert_eq!(selected.len(), 10, "source-backed selector drifted");

    let mut failures = Vec::new();
    for record in selected {
        match tidb_parser::parse_multi(&record.input.sql) {
            Ok(statements)
                if statements
                    .iter()
                    .map(|statement| statement.restore_bytes())
                    .eq(record.restores.iter().map(Vec::as_slice)) => {}
            Ok(statements) => failures.push(format!(
                "{}:{}\n  go statements: {}\n rust statements: {}",
                record.input.path,
                record.input.start_line,
                record.restores.len(),
                statements.len()
            )),
            Err(error) => failures.push(format!(
                "{}:{}\n  parse error: {error:?}",
                record.input.path, record.input.start_line
            )),
        }
    }
    assert!(
        failures.is_empty(),
        "{} multi-statement mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
