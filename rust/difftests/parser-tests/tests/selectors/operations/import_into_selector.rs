#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn starts_import_into(sql: &str) -> bool {
    let sql = sql.trim_start();
    let prefix = "import";
    sql.get(..prefix.len())
        .is_some_and(|value| value.eq_ignore_ascii_case(prefix))
        && sql
            .get(prefix.len()..)
            .is_some_and(|rest| rest.starts_with(char::is_whitespace))
        && sql[prefix.len()..]
            .trim_start()
            .get(..4)
            .is_some_and(|value| value.eq_ignore_ascii_case("into"))
        && sql[prefix.len()..]
            .trim_start()
            .get(4..)
            .is_some_and(|rest| rest.starts_with(char::is_whitespace))
}

#[test]
fn import_into_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && starts_import_into(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 100, "source-backed selector drifted");

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
