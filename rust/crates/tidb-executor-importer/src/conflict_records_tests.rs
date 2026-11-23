//! Go `pkg/ingestor/ingestctrl/duplicate_test.go`'s
//! `TestConvertToErrFoundConflictRecords*`. They live here because their
//! KV pairs come from IMPORT INTO's encoder.
//!
//! `testdata/ingestctrl_conflict_records_go.jsonl` holds the `TableInfo`s
//! Go's `ddl.MockTableInfo` builds (table ID 108) for the tests' two
//! `CREATE TABLE` statements, printed by an uncommitted probe in a Go master
//! worktree. Its third line is a `TIMESTAMP` unique index's table, printed
//! by a probe with the index KV and Go's `GenIndexValueFromIndex` text
//! under `time.Local` = Asia/Shanghai.

use super::kv_encode::TableKVEncoder;
use super::{ASTArgs, LoadDataController, Plan};
use tidb_datatype::Datum;
use tidb_lightning_backend_encode::{EncodingConfig, SessionOptions};
use tidb_lightning_backend_kv::PanickingAllocators;

/// Go `buildTableForTestConvertToErrFoundConflictRecords`: the table and the
/// KV pairs of its three rows, record then index for each.
fn build_table(line: usize) -> (tidb_table::tables::TableCommon, Vec<(Vec<u8>, Vec<u8>)>) {
    let json = include_str!("../testdata/ingestctrl_conflict_records_go.jsonl").lines().nth(line).unwrap();
    let meta = tidb_meta::value::parse_table_info(json.as_bytes(), 0).unwrap();
    let table =
        tidb_table::table_from_meta::table_from_meta(&meta, &tidb_table::table_from_meta::AutoIdSource::Unavailable).unwrap();
    let tidb_ast::Stmt::Dml(dml) = tidb_parser::parse("import into a from '/x.csv'").unwrap() else { unreachable!() };
    let tidb_ast::DmlStmt::ImportInto(stmt) = dml.as_ref() else { unreachable!() };
    let ctrl = LoadDataController::new(Plan::new_import_plan(stmt, 1, "").unwrap(), table.clone(), ASTArgs::default()).unwrap();
    let cfg = EncodingConfig {
        session_options: SessionOptions {
            sql_mode: tidb_mysql::ModeStrictAllTables.0,
            timestamp: 1_234_567_890,
            ..SessionOptions::default()
        },
        path: String::new(),
        table: meta.clone(),
        use_identity_auto_row_id: false,
    };
    let mut encoder = TableKVEncoder::new(&cfg, &ctrl, PanickingAllocators::new(meta.sep_auto_inc())).unwrap();
    let mut pairs = Vec::new();
    for (id, b, c, d) in [(1, 6, "1.csv", 101), (2, 6, "2.csv", 102), (3, 7, "3.csv", 103)] {
        let row = [Datum::Int(id), Datum::Int(b), Datum::new_string_datum(c), Datum::Int(d)];
        let encoded = encoder.encode(&row, id).unwrap();
        pairs.extend(encoded.pairs.into_iter().map(|p| (p.key, p.val)));
    }
    (table, pairs)
}

fn convert(key: &[u8], value: &[u8], table: &tidb_table::tables::TableCommon) -> String {
    // Go `common.ErrFoundDuplicateKeys.FastGenByArgs(key, value)` as the
    // local engine reports it, behind the lightning backend's error.
    let original = tidb_lightning_backend::Error::from_cause(tidb_ingestor_ingestctrl::Error::Duplicate {
        key: key.to_vec(),
        value: value.to_vec(),
    });
    tidb_ingestor_ingestctrl::duplicate::convert_to_err_found_conflict_records(&original, table)
        .expect("a duplicate key")
        .to_string()
}

/// Go `TestConvertToErrFoundConflictRecordsSingleColumnsIndex`.
#[test]
fn test_convert_to_err_found_conflict_records_single_columns_index() {
    let (table, pairs) = build_table(0);
    assert_eq!(
        convert(&pairs[2].0, &pairs[2].1, &table),
        "[Lightning:Restore:ErrFoundDataConflictRecords]found data conflict records in table a, primary key is '2', row data is '(2, 6, \"2.csv\", 102)'"
    );
    assert_eq!(
        convert(&pairs[5].0, &pairs[5].1, &table),
        "[Lightning:Restore:ErrFoundIndexConflictRecords]found index conflict records in table a, index name is 'a.key_b', unique key is '[7]', primary key is '3'"
    );
}

/// Go `TestConvertToErrFoundConflictRecordsMultipleColumnsIndex`.
#[test]
fn test_convert_to_err_found_conflict_records_multiple_columns_index() {
    let (table, pairs) = build_table(1);
    assert_eq!(
        convert(&pairs[2].0, &pairs[2].1, &table),
        "[Lightning:Restore:ErrFoundDataConflictRecords]found data conflict records in table a, primary key is '2', row data is '(2, 6, \"2.csv\", 102)'"
    );
    assert_eq!(
        convert(&pairs[5].0, &pairs[5].1, &table),
        "[Lightning:Restore:ErrFoundIndexConflictRecords]found index conflict records in table a, index name is 'a.key_bd', unique key is '[7 103]', primary key is '3'"
    );
}

/// A non-duplicate error is left to the caller, as Go returns it unchanged.
#[test]
fn test_convert_to_err_found_conflict_records_other_error() {
    let (table, _) = build_table(0);
    let other = tidb_lightning_backend::Error::other("other error");
    assert!(tidb_ingestor_ingestctrl::duplicate::convert_to_err_found_conflict_records(&other, &table).is_none());
}

/// Go `GenIndexValueFromIndex` decodes a `TIMESTAMP` in `time.Local`: the
/// stored UTC midnight reads as 08:00 in Asia/Shanghai.
#[test]
fn test_convert_to_err_found_conflict_records_timestamp_in_local_zone() {
    // This test runs in its own process (nextest); chrono's `Local` reads TZ.
    std::env::set_var("TZ", "Asia/Shanghai");
    let json = include_str!("../testdata/ingestctrl_conflict_records_go.jsonl").lines().nth(2).unwrap();
    let meta = tidb_meta::value::parse_table_info(json.as_bytes(), 0).unwrap();
    let table =
        tidb_table::table_from_meta::table_from_meta(&meta, &tidb_table::table_from_meta::AutoIdSource::Unavailable).unwrap();
    let key = hex::decode("74800000000000006c5f6980000000000000010419a5420000000000").unwrap();
    let value = hex::decode("0000000000000003").unwrap();
    assert_eq!(
        convert(&key, &value, &table),
        "[Lightning:Restore:ErrFoundIndexConflictRecords]found index conflict records in table a, index name is 'a.key_t', unique key is '[2020-01-01 08:00:00]', primary key is '3'"
    );
}
