#![allow(missing_docs)]

//! Checked Go-oracle rows for `ALTER TABLE ... SHARD_ROW_ID_BITS [=] integer` only.

use difftest::parser_oracle::{shared_golden, GoOutcome};

const SHARD_ROW_ID_BITS_ROWS: [(&str, usize); 5] = [
    ("tests/integrationtest/t/ddl/bdr_mode.test", 46),
    ("tests/integrationtest/t/ddl/bdr_mode.test", 251),
    ("tests/integrationtest/t/ddl/bdr_mode.test", 455),
    ("tests/integrationtest/t/ddl/db.test", 315),
    ("tests/integrationtest/t/ddl/db.test", 321),
];

#[test]
fn alter_shard_row_id_bits_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && SHARD_ROW_ID_BITS_ROWS
                    .contains(&(record.input.path.as_str(), record.input.start_line))
        })
        .collect();
    assert_eq!(rows.len(), SHARD_ROW_ID_BITS_ROWS.len());

    let failures: Vec<_> = rows
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
