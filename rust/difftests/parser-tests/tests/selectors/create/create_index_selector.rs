#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

#[test]
fn create_index_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|r| {
            let sql = r.input.sql.trim_start().to_ascii_lowercase();
            [
                "create index ",
                "create unique index ",
                "create fulltext index ",
                "create spatial index ",
                "create vector index ",
                "create columnar index ",
            ]
            .iter()
            .any(|prefix| sql.starts_with(prefix))
                && r.outcome == GoOutcome::Accepted
                && r.statement_count == 1
        })
        .collect();
    assert_eq!(selected.len(), 125, "source-backed selector drifted");
    let mut failures = Vec::new();
    for r in selected {
        match tidb_parser::parse(&r.input.sql) {
            Ok(stmt) if stmt.restore().as_bytes() == r.restores[0].as_slice() => {}
            Ok(stmt) => failures.push(format!(
                "{}\n  go: {}\n rust: {}",
                r.input.sql,
                String::from_utf8_lossy(&r.restores[0]),
                stmt.restore()
            )),
            Err(err) => failures.push(format!("{}\n  parse error: {err:?}", r.input.sql)),
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
