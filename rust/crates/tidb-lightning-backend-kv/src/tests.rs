//! Go master's `NewTableKVEncoder` / `Encode` against this port.
//!
//! `testdata/encode_go.jsonl` was printed by an uncommitted probe in a Go
//! master worktree (`GOTOOLCHAIN=go1.27.3 go test ./zzprobe/e05kv -run
//! TestEncodeCapture`) with `TiDBStrictIntegerDisplayWidth` set as tidb-server
//! sets it by default. For each `CREATE TABLE` it built the table with
//! `ddl.MockTableInfo` (the Go tests' `mockTableInfo`), opened the encoder
//! under the case's SQL mode, timestamp, system variables and AUTO_RANDOM
//! seed over `NewPanickingAllocators`, encoded string (or NULL) rows with
//! their column permutations, and recorded the KV pairs (or the error) and
//! the allocator bases after each row. Covered: casts and their strict /
//! non-strict errors, NOT NULL and missing defaults, int and clustered
//! primary keys, unique / prefix / expression indexes, AUTO_INCREMENT
//! (integer, TINYINT overflow, DOUBLE), AUTO_RANDOM (signed, unsigned with a
//! range, identity row ids), SHARD_ROW_ID_BITS and explicit `_tidb_rowid`,
//! stored and virtual generated columns (including a division error),
//! temporal defaults under a fixed timestamp and a session time zone,
//! DECIMAL/FLOAT/BIT/ENUM/SET/JSON/BLOB/CHAR, row format v1 and the row
//! checksum.

use super::*;
use serde_json::Value;
use tidb_lightning_backend_encode::{EncodingConfig, Encoder, SessionOptions};

fn case_config(case: &Value, table: tidb_model::TableInfo) -> EncodingConfig {
    let sql_mode = tidb_mysql::get_sql_mode(case["sql_mode"].as_str().unwrap()).unwrap().0;
    EncodingConfig {
        session_options: SessionOptions {
            sql_mode,
            timestamp: case["timestamp"].as_i64().unwrap(),
            sys_vars: case["sys_vars"]
                .as_object()
                .map(|vars| vars.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned())).collect())
                .unwrap_or_default(),
            auto_random_seed: case["seed"].as_i64().unwrap(),
            ..SessionOptions::default()
        },
        path: String::new(),
        table,
        use_identity_auto_row_id: case["identity_row_id"].as_bool().unwrap(),
    }
}

#[test]
fn encode_matches_go_capture() {
    let mut rows_checked = 0;
    for line in include_str!("../testdata/encode_go.jsonl").lines() {
        let rec: Value = serde_json::from_str(line).unwrap();
        let case = &rec["case"];
        let create = case["create"].as_str().unwrap();
        let table = tidb_meta::value::parse_table_info(rec["table_info"].to_string().as_bytes(), 0).unwrap();
        let allocators = PanickingAllocators::new(table.sep_auto_inc());
        let config = case_config(case, table);
        let mut encoder = TableKVEncoder::new(&config, allocators.clone()).unwrap_or_else(|e| panic!("{create}: {e}"));
        for (row_case, want) in case["rows"].as_array().unwrap().iter().zip(rec["results"].as_array().unwrap()) {
            let row: Vec<tidb_datatype::Datum> = row_case["values"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| match v.as_str() {
                    Some(text) => tidb_datatype::Datum::new_string_datum(text),
                    None => tidb_datatype::Datum::Null,
                })
                .collect();
            let perm: Vec<isize> = row_case["perm"].as_array().unwrap().iter().map(|v| v.as_i64().unwrap() as isize).collect();
            let context = format!("{create} / {} row {}", case["sql_mode"], row_case);
            match encoder.encode(&row, row_case["row_id"].as_i64().unwrap(), &perm, 0) {
                Ok(pairs) => {
                    assert_eq!(want.get("err"), None, "{context}: Go failed, Rust encoded {pairs:?}");
                    let got: Vec<[String; 3]> = pairs
                        .pairs
                        .iter()
                        .map(|kv| [hex::encode(&kv.key), hex::encode(&kv.val), hex::encode(&kv.row_id)])
                        .collect();
                    let want: Vec<[String; 3]> = serde_json::from_value(want["kvs"].clone()).unwrap();
                    assert_eq!(got, want, "{context}");
                }
                Err(error) => {
                    assert_eq!(Some(error.to_string().as_str()), want["err"].as_str(), "{context}");
                }
            }
            for (name, ty) in [
                ("row_id", AllocatorType::RowId),
                ("auto_inc", AllocatorType::AutoIncrement),
                ("auto_random", AllocatorType::AutoRandom),
            ] {
                assert_eq!(allocators.get(ty).base(), want["bases"][name].as_i64().unwrap(), "{context}: {name} base");
            }
            rows_checked += 1;
        }
    }
    assert_eq!(rows_checked, 59);
}
