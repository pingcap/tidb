//! Go master's `SampleFileImportKVSize` against this port's sampler.
//!
//! `testdata/import_into_sampler_go.jsonl` was printed by an uncommitted
//! probe in a Go master worktree (`GOTOOLCHAIN=go1.27.3 go test
//! --tags=intest ./zzprobe/e05ng -run TestSamplerCapture`). For each case
//! it created the table on a mock store, wrote the files to a local store
//! and sampled them with the statement's column list and SET clause:
//! three CSV files (10 rows each), one file over the 30-row cap, skipped
//! header lines, gzip, a table without indexes, SQL and Parquet. With at
//! most three files every file is sampled, so Go's random order does not
//! change the sums.

use base64::Engine as _;
use serde_json::Value;
use tidb_gocontext::Context;
use tidb_lightning_mydump::{Compression, SourceFileMeta, SourceType};

use super::{ASTArgs, LoadDataController, Plan};

#[test]
fn sampled_kv_sizes_match_go() {
    for line in include_str!("../testdata/import_into_sampler_go.jsonl").lines() {
        let rec: Value = serde_json::from_str(line).unwrap();
        let name = rec["case"].as_str().unwrap();
        let meta = tidb_meta::value::parse_table_info(rec["table_info"].to_string().as_bytes(), 0).unwrap();
        let table =
            tidb_table::table_from_meta::table_from_meta(&meta, &tidb_table::table_from_meta::AutoIdSource::Unavailable).unwrap();
        let tidb_ast::Stmt::Dml(dml) = tidb_parser::parse(rec["import"].as_str().unwrap()).unwrap() else { panic!() };
        let tidb_ast::DmlStmt::ImportInto(stmt) = dml.as_ref() else { panic!() };
        let mut plan = Plan::new_import_plan(stmt, 6, "").unwrap();
        plan.format = rec["format"].as_str().unwrap().to_owned();
        plan.ignore_lines = rec["ignore_lines"].as_u64().unwrap();
        plan.sql_mode = tidb_mysql::get_sql_mode("STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION").unwrap().0;
        plan.charset = None;
        let args = ASTArgs {
            columns_and_user_vars: stmt.columns_and_user_vars.clone(),
            column_assignments: stmt.column_assignments.clone(),
        };
        let mut ctrl = LoadDataController::new(plan, table, args).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        let dir = tempfile::tempdir().unwrap();
        for f in rec["files"].as_array().unwrap() {
            let fname = f["name"].as_str().unwrap();
            let data = base64::engine::general_purpose::STANDARD.decode(f["data"].as_str().unwrap()).unwrap();
            std::fs::write(dir.path().join(fname), &data).unwrap();
            ctrl.data_files.push(SourceFileMeta {
                path: fname.to_owned(),
                r#type: match f["format"].as_str().unwrap() {
                    "csv" => SourceType::CSV,
                    "sql" => SourceType::SQL,
                    _ => SourceType::Parquet,
                },
                compression: if fname.ends_with(".gz") { Compression::GZ } else { Compression::None },
                file_size: data.len() as i64,
                real_size: data.len() as i64,
                ..SourceFileMeta::default()
            });
        }
        ctrl.data_store = Some(std::sync::Arc::new(tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap()));
        let got = ctrl.sample_kv_size(&Context::background(), &[]).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        let want = (
            rec["source_size"].as_i64().unwrap(),
            rec["data_kv_size"].as_u64().unwrap(),
            rec["index_kv_size"].as_u64().unwrap(),
        );
        assert_eq!((got.source_size, got.data_kv_size, got.index_kv_size), want, "{name}");
    }
}
