#![allow(missing_docs)]

//! Checked Go-oracle rows for ALTER TABLE auto-ID table options.

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn is_auto_id_option_action(sql: &str) -> bool {
    let normalized = sql.trim().to_ascii_lowercase();
    if !normalized.starts_with("alter table ") {
        return false;
    }
    normalized.contains(" auto_id_cache")
        || normalized.contains(" auto_random_base")
        || normalized.contains(" force auto_random_base")
}

#[test]
fn alter_auto_id_options_dynamic_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_auto_id_option_action(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 11, "source-backed selector drifted");

    let failures: Vec<_> = selected
        .into_iter()
        .filter_map(|record| match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                None
            }
            Ok(statement) => Some(format!(
                "{}:{}: {}\n  go: {}\n  rust: {}",
                record.input.path,
                record.input.start_line,
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => Some(format!(
                "{}:{}: {}\n  parse error: {error:?}",
                record.input.path, record.input.start_line, record.input.sql
            )),
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
