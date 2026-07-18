#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn ttl_alter(sql: &str) -> bool {
    let words: Vec<_> = sql.split_ascii_whitespace().collect();
    words.len() >= 4
        && words[0].eq_ignore_ascii_case("alter")
        && words[1].eq_ignore_ascii_case("table")
        && matches!(
            words[3].trim_end_matches(';').to_ascii_uppercase().as_str(),
            "TTL" | "TTL_ENABLE" | "TTL_JOB_INTERVAL" | "REMOVE"
        )
}

#[test]
fn alter_table_ttl_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && ttl_alter(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 34, "source-backed selector drifted");
    for record in selected {
        let statement = tidb_parser::parse(&record.input.sql)
            .unwrap_or_else(|error| panic!("{}: {error:?}", record.input.sql));
        assert_eq!(
            statement.restore().as_bytes(),
            record.restores[0].as_slice(),
            "{}",
            record.input.sql
        );
    }
}
