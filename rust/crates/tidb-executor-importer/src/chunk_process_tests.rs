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

use super::chunk_process::{ChunkProcessor, ParserEncodeReader, QueryChunk, QueryChunkEncodeReader};
use super::kv_encode::TableKVEncoder;
use super::{ASTArgs, LoadDataController, Plan};
use serde_json::Value;
use std::sync::Arc;
use tidb_gocontext::Context;
use tidb_lightning_backend::{ChunkFlushStatus, EngineWriter};
use tidb_lightning_backend_encode::{EncodingConfig, SessionOptions};
use tidb_lightning_backend_kv::PanickingAllocators;
use tidb_lightning_verification::{KvGroupChecksum, KvPair};

#[derive(Default)]
struct Recorder(Vec<[String; 3]>);

impl EngineWriter for Recorder {
    fn append_rows(&mut self, _: &Context, _: &[String], rows: tidb_lightning_backend::Rows) -> tidb_lightning_backend::Result<()> {
        self.0.extend(rows.into_kv_pairs().iter().map(|kv| [hex::encode(&kv.key), hex::encode(&kv.val), hex::encode(&kv.row_id)]));
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
    for line in include_str!("../testdata/import_into_chunk_go.jsonl").lines() {
        let rec: Value = serde_json::from_str(line).unwrap();
        let name = rec["case"].as_str().unwrap();
        let (ctrl, mut encoder) = go_capture_encoder(&rec);
        let data = rec["data"].as_str().unwrap();
        let mut parser = tidb_lightning_mydump::CsvParser::from_bytes(&ctrl.generate_csv_config(), data.as_bytes().to_vec(), None).unwrap();
        let reader = ParserEncodeReader { parser: &mut parser, end_offset: data.len() as i64, filename: "x.csv:0".to_owned() };
        let (mut dw, mut iw) = (Recorder::default(), Recorder::default());
        let mut checksum = KvGroupChecksum::with_keyspace(&[]);
        let result = ChunkProcessor::new("x.csv:0".to_owned(), Box::new(reader), 0, &mut encoder, &[], &mut dw, &mut iw, None)
            .process(&Context::background(), &mut checksum);
        match rec["err"].as_str() {
            None => assert!(result.is_ok(), "{name}: {result:?}"),
            Some(want) => {
                let err = result.unwrap_err();
                assert_eq!(err.to_string(), want, "{name}");
                // DXF `serializeErr` keeps the first `*errors.Error`'s RFC
                // code and `errors.GetErrStackMsg`, and Go does not retry it.
                let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&*err.0);
                let mut rfc = String::new();
                while let Some(e) = cause {
                    if let Some(t) = e.downcast_ref::<tidb_terror::TerrorError>() {
                        rfc = t.id().into_owned();
                        break;
                    }
                    cause = e.source();
                }
                assert_eq!(rfc, rec["err_rfc"].as_str().unwrap_or_default(), "{name}: rfc");
                assert_eq!(
                    tidb_terror::get_err_stack_msg(Some(&*err.0)),
                    rec["err_stack_msg"].as_str().unwrap_or_default(),
                    "{name}: stack msg"
                );
                assert!(!tidb_lightning_common::is_retryable_error(&*err.0), "{name}: retryable");
            }
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

fn go_capture_encoder(rec: &Value) -> (LoadDataController, TableKVEncoder) {
    let meta = tidb_meta::value::parse_table_info(rec["table_info"].to_string().as_bytes(), 0).unwrap();
    let table = tidb_table::table_from_meta::table_from_meta(&meta, &tidb_table::table_from_meta::AutoIdSource::Unavailable).unwrap();
    let stmt = import(rec["import"].as_str().unwrap());
    let mut plan = Plan::new_import_plan(&stmt, 6, "").unwrap();
    let sql_mode = tidb_mysql::get_sql_mode(rec["sql_mode"].as_str().unwrap()).unwrap().0;
    plan.sql_mode = sql_mode;
    let args = ASTArgs {
        columns_and_user_vars: stmt.columns_and_user_vars.clone(),
        column_assignments: stmt.column_assignments.clone(),
    };
    let ctrl = LoadDataController::new(plan, table, args)
        .unwrap_or_else(|error| panic!("{}: {error:?}", rec["case"]));
    let cfg = EncodingConfig {
        session_options: SessionOptions { sql_mode, timestamp: 1_234_567_890, ..SessionOptions::default() },
        path: String::new(),
        table: meta.clone(),
        use_identity_auto_row_id: false,
    };
    let encoder = TableKVEncoder::new(&cfg, &ctrl, PanickingAllocators::new(meta.sep_auto_inc())).unwrap();
    (ctrl, encoder)
}

#[test]
fn delivery_panic_cancels_open_query_source() {
    use std::time::Duration;
    use tidb_datatype::Datum;

    struct PanickingWriter;

    impl EngineWriter for PanickingWriter {
        fn append_rows(&mut self, _: &Context, _: &[String], _: tidb_lightning_backend::Rows) -> tidb_lightning_backend::Result<()> {
            panic!("injected writer panic");
        }
        fn is_synced(&self) -> bool {
            true
        }
        fn close(&mut self, _: &Context) -> tidb_lightning_backend::Result<ChunkFlushStatus> {
            Ok(ChunkFlushStatus::default())
        }
    }

    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let (input, rows) = crossbeam_channel::bounded(1);
    input.send(QueryChunk {
        rows: vec![vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)]],
        row_id_offset: 0,
    }).unwrap();
    let (finished, result) = crossbeam_channel::bounded(1);
    let processor = std::thread::spawn(move || {
        let capture = include_str!("../testdata/import_into_chunk_go.jsonl");
        let rec: Value = serde_json::from_str(capture.lines().next().unwrap()).unwrap();
        let (_, mut encoder) = go_capture_encoder(&rec);
        let mut data_writer = PanickingWriter;
        let mut index_writer = Recorder::default();
        let mut processor = ChunkProcessor::new(
            "query".to_owned(),
            Box::new(QueryChunkEncodeReader::new(rows)),
            -1,
            &mut encoder,
            &[],
            &mut data_writer,
            &mut index_writer,
            None,
        );
        processor.min_deliver_row_cnt = 1;
        let mut checksum = KvGroupChecksum::with_keyspace(&[]);
        let error = processor.process(&ctx, &mut checksum).unwrap_err();
        finished.send(error.to_string()).unwrap();
    });

    let outcome = result.recv_timeout(Duration::from_secs(2));
    // Let the original pipeline finish if it still needs the caller to close or cancel.
    cancel.cancel();
    drop(input);
    processor.join().unwrap();
    assert_eq!(outcome.as_deref(), Ok("deliver loop panicked"));
}

/// An import client that takes every write, records the written pairs and
/// the PD label rules, and accepts every ingest.
#[derive(Default)]
struct RecordingImportClient {
    pairs: std::sync::Mutex<Vec<[String; 2]>>,
    label_calls: Arc<std::sync::Mutex<Vec<&'static str>>>,
}

impl tidb_ingestor_ingestctrl::region_job::ImportClient for RecordingImportClient {
    fn get_region(&self, _: &[u8]) -> tidb_ingestor_ingestctrl::Result<tidb_ingestor_ingestctrl::region_job::RegionInfo> {
        let peer = tidb_proto::metapb::Peer { id: 1, store_id: 1, ..Default::default() };
        Ok(tidb_ingestor_ingestctrl::region_job::RegionInfo {
            leader: Some(peer),
            region: tidb_proto::metapb::Region { id: 1, peers: vec![peer], ..Default::default() },
        })
    }
    fn invalidate_region(&self, _: &tidb_ingestor_ingestctrl::region_job::RegionInfo) {}
    fn write(
        &self,
        _: u64,
        requests: Vec<tidb_proto::import_sstpb::WriteRequest>,
    ) -> tidb_ingestor_ingestctrl::Result<tidb_proto::import_sstpb::WriteResponse> {
        use tidb_proto::import_sstpb::write_request::Chunk;
        let mut metas = Vec::new();
        for request in requests {
            match request.chunk {
                Some(Chunk::Meta(meta)) => metas.push(meta),
                Some(Chunk::Batch(batch)) => self
                    .pairs
                    .lock()
                    .unwrap()
                    .extend(batch.pairs.iter().map(|p| [hex::encode(&p.key), hex::encode(&p.value)])),
                None => {}
            }
        }
        Ok(tidb_proto::import_sstpb::WriteResponse { metas, ..Default::default() })
    }
    fn multi_ingest(
        &self,
        _: u64,
        _: tidb_proto::import_sstpb::MultiIngestRequest,
    ) -> tidb_ingestor_ingestctrl::Result<tidb_proto::import_sstpb::IngestResponse> {
        Ok(tidb_proto::import_sstpb::IngestResponse::default())
    }
    fn ingest(
        &self,
        _: u64,
        _: tidb_proto::import_sstpb::IngestRequest,
    ) -> tidb_ingestor_ingestctrl::Result<tidb_proto::import_sstpb::IngestResponse> {
        Ok(tidb_proto::import_sstpb::IngestResponse::default())
    }
    fn duplicate_detect(
        &self,
        _: u64,
        _: tidb_proto::import_sstpb::DuplicateDetectRequest,
        _: &mut (dyn FnMut(tidb_proto::import_sstpb::DuplicateDetectResponse) -> tidb_ingestor_ingestctrl::Result<()> + Send),
    ) -> tidb_ingestor_ingestctrl::Result<()> {
        Ok(())
    }
    fn api_version(&self) -> i32 {
        0
    }
    fn encode_key(&self, key: &[u8]) -> Vec<u8> {
        key.to_vec()
    }
    fn get_ts(&self) -> tidb_ingestor_ingestctrl::Result<u64> {
        Ok(42)
    }
    fn pd_http_client(&self) -> Option<Arc<dyn tidb_br::pdutil::LabelRuleClient>> {
        Some(Arc::new(LabelRules(Arc::clone(&self.label_calls))))
    }
}

impl tidb_ingestor_ingestctrl::localhelper::SplitClient for RecordingImportClient {
    fn split_keys_and_scatter(&self, _: &Context, _: &[Vec<u8>]) -> tidb_ingestor_ingestctrl::Result<Vec<u64>> {
        Ok(Vec::new())
    }
    fn wait_regions_scattered(&self, _: &Context, _: &[u64]) -> tidb_ingestor_ingestctrl::Result<usize> {
        Ok(0)
    }
}

/// PD's label-rule API, recording each call.
struct LabelRules(Arc<std::sync::Mutex<Vec<&'static str>>>);

impl tidb_br::pdutil::LabelRuleClient for LabelRules {
    fn set_region_label_rule(&self, _: &tikv_client::pd_http::LabelRule) -> Result<(), String> {
        self.0.lock().unwrap().push("set");
        Ok(())
    }
    fn patch_region_label_rules(
        &self,
        _: &tikv_client::pd_http::LabelRulePatch,
        _: Option<std::time::Duration>,
    ) -> Result<(), String> {
        self.0.lock().unwrap().push("delete");
        Ok(())
    }
}

/// Go `TableImporter.ImportSelectedRows` (IMPORT INTO ... FROM SELECT): rows
/// from the query channel, spread over two workers, reach TiKV as the same
/// KV pairs Go's file chunk processor writes for those rows (the Go
/// capture's `all_columns` case), with PD's schedulers paused on the
/// table's range around each engine's ingest.
#[test]
fn import_selected_rows_writes_the_rows_kvs() {
    use tidb_datatype::Datum;
    let line = include_str!("../testdata/import_into_chunk_go.jsonl").lines().next().unwrap();
    let rec: Value = serde_json::from_str(line).unwrap();
    assert_eq!(rec["case"], "all_columns");
    let meta = tidb_meta::value::parse_table_info(rec["table_info"].to_string().as_bytes(), 0).unwrap();
    let table = tidb_table::table_from_meta::table_from_meta(&meta, &tidb_table::table_from_meta::AutoIdSource::Unavailable).unwrap();
    let stmt = import("import into t from select * from s");
    let mut plan = Plan::new_import_plan(&stmt, 2, "").unwrap();
    plan.sql_mode = tidb_mysql::get_sql_mode(rec["sql_mode"].as_str().unwrap()).unwrap().0;
    plan.table_info = Some(meta.clone());
    let args = ASTArgs { columns_and_user_vars: Vec::new(), column_assignments: Vec::new() };
    let ctrl = LoadDataController::new(plan, table, args).unwrap();
    let client = std::sync::Arc::new(RecordingImportClient::default());
    let ti = super::table_import::TableImporter::new(ctrl, "select-test", client.clone(), Vec::new()).unwrap();
    let (tx, rx) = crossbeam_channel::unbounded();
    let rows = |r: &[[i64; 3]]| r.iter().map(|row| row.iter().map(|v| Datum::Int(*v)).collect()).collect();
    tx.send(super::chunk_process::QueryChunk { rows: rows(&[[1, 2, 3], [4, 5, 6]]), row_id_offset: 0 }).unwrap();
    tx.send(super::chunk_process::QueryChunk { rows: rows(&[[7, 8, 9]]), row_id_offset: 2 }).unwrap();
    drop(tx);
    let imported = ti.import_selected_rows(&Context::background(), &rx).unwrap();
    ti.close();
    assert_eq!(imported.data_kv_count, 3);
    let want = |key: &str| -> Vec<[String; 2]> {
        let kvs: Vec<[String; 3]> = serde_json::from_value(rec[key].clone()).unwrap_or_default();
        kvs.into_iter().map(|[k, v, _]| [k, v]).collect()
    };
    let mut expected = want("data_kvs");
    expected.extend(want("index_kvs"));
    expected.sort();
    let mut got = client.pairs.lock().unwrap().clone();
    got.sort();
    assert_eq!(got, expected);
    let sums: Vec<u64> = serde_json::from_value(rec["sums"].clone()).unwrap();
    assert_eq!(imported.checksum.merged_checksum().sum(), sums[4]);
    assert_eq!(*client.label_calls.lock().unwrap(), ["set", "delete", "set", "delete"]);
}

/// Go `IndexRouteWriter.AppendRows`: only `kv.GroupedPairs` is accepted;
/// a writer is made per index on its first pair.
#[test]
fn index_route_writer_takes_grouped_pairs() {
    let made = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = Arc::clone(&made);
    let mut w = super::chunk_process::IndexRouteWriter::new(Box::new(move |index_id| {
        sink.lock().unwrap().push(index_id);
        Err(format!("no writer for index {index_id}"))
    }));
    let ctx = Context::background();
    let pair = KvPair { key: b"k".to_vec(), val: b"v".to_vec(), row_id: Vec::new() };
    let err = w.append_rows(&ctx, &[], tidb_lightning_backend::Rows::Pairs(vec![pair.clone()])).unwrap_err();
    assert_eq!(err.to_string(), "invalid kv pairs type for IndexRouteWriter: *kv.Pairs");
    assert!(made.lock().unwrap().is_empty());
    let grouped = std::collections::BTreeMap::from([(7, vec![pair])]);
    let err = w.append_rows(&ctx, &[], tidb_lightning_backend::Rows::Grouped(grouped)).unwrap_err();
    assert_eq!(err.to_string(), "no writer for index 7");
    assert_eq!(*made.lock().unwrap(), [7]);
}
