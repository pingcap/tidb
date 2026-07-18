#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

/// The direct Go grammar branches are `parseAlterTableSpec`'s `SET [HYPO]
/// TIFLASH REPLICA` case and `parseCompactTableStmt`, both after the ALTER
/// TABLE name boundary. Keep this selector lexical and deliberately narrow:
/// it does not claim the unrelated ALTER DATABASE TiFlash option list.
fn is_tiflash_replica_or_compact(sql: &str) -> bool {
    let sql = sql.trim_start().to_ascii_uppercase();
    if !sql.starts_with("ALTER TABLE ") {
        return false;
    }
    sql.contains(" SET TIFLASH REPLICA")
        || sql.contains(" SET HYPO TIFLASH REPLICA")
        || sql.contains(" COMPACT")
}

#[test]
fn alter_tiflash_replica_and_compact_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_tiflash_replica_or_compact(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 25, "source-backed selector drifted");

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
