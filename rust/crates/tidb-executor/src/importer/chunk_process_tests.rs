//! Go master's `NewFileChunkProcessor(...).Process` against this port.
//!
//! `testdata/import_into_chunk_go.jsonl` was printed by an uncommitted probe
//! in a Go master worktree (`GOTOOLCHAIN=go1.27.3 go test --tags=intest
//! ./zzprobe/e05imp -run TestImporterChunkCapture`, with
//! `TiDBStrictIntegerDisplayWidth` set as tidb-server sets it). For each
//! case it created the table on a mock store, built a `LoadDataController`
//! from the IMPORT INTO statement's column list and SET clause, opened a
//! `TableKVEncoder` at timestamp 1234567890, parsed the CSV data with the
//! controller's CSV config, and recorded the KV pairs each writer received,
//! the group checksum and the chunk error. Covered: every column, a column
//! list with user variables and SET, short rows and NULL, AUTO_INCREMENT
//! with a unique index, explicit AUTO_INCREMENT values, an encoding error
//! and the non-strict truncation.

use super::chunk_process::{ChunkProcessor, ParserEncodeReader};
use super::kv_encode::TableKVEncoder;
use super::{ASTArgs, LoadDataController, Plan};
use serde_json::Value;
use tidb_gocontext::Context;
use tidb_lightning_backend::{ChunkFlushStatus, EngineWriter};
use tidb_lightning_backend_encode::{EncodingConfig, SessionOptions};
use tidb_lightning_backend_kv::PanickingAllocators;
use tidb_lightning_verification::{KvGroupChecksum, KvPair};

#[derive(Default)]
struct Recorder(Vec<[String; 3]>);

impl EngineWriter for Recorder {
    fn append_rows(&mut self, _: &Context, _: &[String], rows: Vec<KvPair>) -> tidb_lightning_backend::Result<()> {
        self.0.extend(rows.iter().map(|kv| [hex::encode(&kv.key), hex::encode(&kv.val), hex::encode(&kv.row_id)]));
        Ok(())
    }
    fn is_synced(&self) -> bool {
        true
    }
    fn close(&mut self, _: &Context) -> tidb_lightning_backend::Result<ChunkFlushStatus> {
        Ok(ChunkFlushStatus::default())
    }
}

fn import(sql: &str) -> tidb_ast::ImportIntoStmt {
    let tidb_ast::Stmt::Dml(dml) = tidb_parser::parse(sql).unwrap() else { panic!() };
    let tidb_ast::DmlStmt::ImportInto(stmt) = dml.as_ref() else { panic!() };
    *stmt.clone()
}

#[test]
fn file_chunk_process_matches_go_capture() {
    for line in include_str!("../../testdata/import_into_chunk_go.jsonl").lines() {
        let rec: Value = serde_json::from_str(line).unwrap();
        let name = rec["case"].as_str().unwrap();
        let meta = tidb_meta::value::parse_table_info(rec["table_info"].to_string().as_bytes(), 0).unwrap();
        let table = tidb_table::table_from_meta::table_from_meta(&meta, &tidb_table::table_from_meta::AutoIdSource::Unavailable).unwrap();
        let stmt = import(rec["import"].as_str().unwrap());
        let mut plan = Plan::new_import_plan(&stmt, 6, "").unwrap();
        let sql_mode = tidb_mysql::get_sql_mode(rec["sql_mode"].as_str().unwrap()).unwrap().0;
        plan.sql_mode = sql_mode;
        let args = ASTArgs { columns_and_user_vars: stmt.columns_and_user_vars.clone(), column_assignments: stmt.column_assignments.clone() };
        let ctrl = LoadDataController::new(plan, table, args).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        let cfg = EncodingConfig {
            session_options: SessionOptions { sql_mode, timestamp: 1_234_567_890, ..SessionOptions::default() },
            path: String::new(),
            table: meta.clone(),
            use_identity_auto_row_id: false,
        };
        let mut encoder = TableKVEncoder::new(&cfg, &ctrl, PanickingAllocators::new(meta.sep_auto_inc())).unwrap();
        let data = rec["data"].as_str().unwrap();
        let mut parser = tidb_lightning_mydump::CsvParser::from_bytes(&ctrl.generate_csv_config(), data.as_bytes().to_vec(), None).unwrap();
        let reader = ParserEncodeReader { parser: &mut parser, end_offset: data.len() as i64, filename: "x.csv:0".to_owned() };
        let (mut dw, mut iw) = (Recorder::default(), Recorder::default());
        let mut checksum = KvGroupChecksum::with_keyspace(&[]);
        let result = ChunkProcessor::new("x.csv:0".to_owned(), Box::new(reader), 0, &mut encoder, &[], &mut dw, &mut iw, None)
            .process(&Context::background(), &mut checksum);
        match rec["err"].as_str() {
            None => assert_eq!(result, Ok(()), "{name}"),
            Some(want) => assert_eq!(result.unwrap_err().0, want, "{name}"),
        }
        let want = |key: &str| -> Vec<[String; 3]> { serde_json::from_value(rec[key].clone()).unwrap_or_default() };
        assert_eq!(dw.0, want("data_kvs"), "{name}: data KVs");
        assert_eq!(iw.0, want("index_kvs"), "{name}: index KVs");
        let (ds, is) = checksum.data_and_index_sum_size();
        let (dk, ik) = checksum.data_and_index_sum_kvs();
        let sums: Vec<u64> = serde_json::from_value(rec["sums"].clone()).unwrap();
        assert_eq!([ds, is, dk, ik, checksum.merged_checksum().sum()], sums[..5], "{name}: checksums");
    }
}
