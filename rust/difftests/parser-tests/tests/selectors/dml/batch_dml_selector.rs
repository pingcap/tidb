#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn is_batch_dml(sql: &str) -> bool {
    let sql = sql.trim_start();
    let phrase = "batch";
    sql.get(..phrase.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(phrase))
        && sql
            .get(phrase.len()..)
            .is_some_and(|rest| rest.starts_with(char::is_whitespace))
}

/// This one Go-accepted BATCH row contains an optimizer hint in the inner
/// `DELETE`'s table-reference position. Its failure is owned by the generic
/// DML hint grammar, not the BATCH wrapper translated by this slice. Keep the
/// deferral source-addressed so a future hint port cannot silently shrink the
/// wrapper selector.
fn is_deferred_inner_dml_hint(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    record.input.path == "tests/integrationtest/t/session/nontransactional.test"
        && record.input.start_line == 377
        && record.input.end_line == 377
        && record.input.sql == "batch on a limit 10 dry run delete /*+ USE_INDEX(t) */ from t;"
}

#[test]
fn batch_dml_static_go_rows_match_or_reveal_inner_grammar_gaps() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let candidates: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_batch_dml(&record.input.sql)
        })
        .collect();
    assert_eq!(
        candidates.len(),
        132,
        "source-backed BATCH inventory drifted"
    );
    assert_eq!(
        candidates
            .iter()
            .filter(|record| is_deferred_inner_dml_hint(record))
            .count(),
        1,
        "deferred generic DML-hint row drifted"
    );
    let selected: Vec<_> = candidates
        .into_iter()
        .filter(|record| !is_deferred_inner_dml_hint(record))
        .collect();
    assert_eq!(selected.len(), 131, "clean BATCH wrapper selector drifted");

    let mut failures = Vec::new();
    for record in selected {
        match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {}
            Ok(statement) => failures.push(format!(
                "{}:{} {}\n  go: {}\n  rust: {}",
                record.input.path,
                record.input.start_line,
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => failures.push(format!(
                "{}:{} {}\n  parse error: {error:?}",
                record.input.path, record.input.start_line, record.input.sql
            )),
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
