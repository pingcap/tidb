#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn starts_desc_or_describe(sql: &str) -> bool {
    let sql = sql.trim_start();
    ["desc", "describe"].iter().any(|prefix| {
        sql.get(..prefix.len())
            .is_some_and(|value| value.eq_ignore_ascii_case(prefix))
            && sql
                .get(prefix.len()..)
                .is_some_and(|rest| rest.starts_with(char::is_whitespace))
    })
}

fn starts_desc_table_query(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    matches!(
        (words.next(), words.next()),
        (Some(leader), Some(table))
            if (leader.eq_ignore_ascii_case("desc") || leader.eq_ignore_ascii_case("describe"))
                && table.eq_ignore_ascii_case("table")
    )
}

#[test]
fn desc_describe_explain_tail_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    // This is the exact Go normal-form boundary. It includes both the
    // ShowColumns fallback (`DESC table`) and the shared EXPLAIN-tail forms
    // (`DESC SELECT`, `DESCRIBE ANALYZE UPDATE`, ...), but excludes the
    // distinct `DESC TABLE <query>` result-set grammar.
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && starts_desc_or_describe(&record.input.sql)
                && !starts_desc_table_query(&record.input.sql)
                && (record.restores[0].starts_with(b"DESC ")
                    || record.restores[0].starts_with(b"EXPLAIN "))
        })
        .collect();
    assert_eq!(selected.len(), 85, "source-backed selector drifted");

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
