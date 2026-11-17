//! Go master's Parquet parser against this port.
//!
//! `testdata/parquet_go.jsonl` was printed by an uncommitted probe in a Go
//! master worktree (`GOTOOLCHAIN=go1.27.3 go test ./zzprobe/e05pq -run
//! TestParquetCapture`). It wrote files with Go's own test writer
//! (`dumpformat/testutils.WriteParquetFile`): integers with and without
//! logical types, booleans, floats, strings/JSON/ENUM/bytes/UUID, decimals
//! on every physical type (including one too wide for `MyDecimal`), dates,
//! TIME and TIMESTAMP in every unit (UTC-adjusted or not), INT96, legacy
//! converted types, several row groups, an all-NULL column, and Spark 2.x /
//! `legacyDateTime` files that are rebased from the Julian calendar. It
//! also read Go's `aurora_snapshot.parquet` and `hive_dump.parquet`. Each
//! file was parsed in UTC and, for the temporal cases, Asia/Shanghai; the
//! record holds the file, the row count, the lowercased columns and every
//! datum's kind, text and type details.

use std::sync::Arc;

use serde_json::Value;
use tidb_datatype::Datum;
use tidb_gocontext::Context;
use tidb_objstore::storeapi::Storage;

use super::*;

/// The probe's `datum` text for a Rust datum.
fn datum(d: &Datum) -> String {
    let text = String::from_utf8_lossy(&d.sql_bytes().unwrap()).into_owned();
    let (kind, extra) = match d {
        Datum::Null => return "NULL".to_owned(),
        Datum::Int(_) => (1, String::new()),
        Datum::UInt(_) => (2, String::new()),
        Datum::Float32(_) => (3, String::new()),
        Datum::Real(_) => (4, String::new()),
        Datum::String(_) => (5, String::new()),
        Datum::Bytes(_) => (6, String::new()),
        Datum::Decimal(dec) => {
            let (p, f) = dec.precision_and_frac();
            (8, format!("/p={p}/f={f}"))
        }
        Datum::Duration(dur) => (9, format!("/fsp={}", dur.fsp())),
        Datum::Time(t) => {
            let tp = match t.kind() {
                tidb_datatype::TimeType::Date => 10,
                tidb_datatype::TimeType::DateTime => 12,
                tidb_datatype::TimeType::Timestamp => 7,
            };
            (13, format!("/tp={tp}/fsp={}", t.fsp()))
        }
        other => panic!("unexpected datum {other:?}"),
    };
    format!("{kind}:{text}{extra}")
}

#[test]
fn parquet_matches_go_capture() {
    let mut checked = 0;
    for line in include_str!("../testdata/parquet_go.jsonl").lines() {
        let rec: Value = serde_json::from_str(line).unwrap();
        let case = format!("{} ({})", rec["case"], rec["loc"]);
        use base64::Engine as _;
        let data = base64::engine::general_purpose::STANDARD.decode(rec["file"].as_str().unwrap()).unwrap();
        let ctx = Context::background();
        let store = Arc::new(tidb_objstore::MemStorage::new());
        store.write_file(&ctx, "f.parquet", &data).unwrap();
        let store: Arc<dyn Storage> = store;
        assert_eq!(read_row_count(&ctx, store.clone(), "f.parquet").unwrap(), rec["row_count"].as_i64().unwrap(), "{case}");
        let loc = session_zone(tidb_timeutil::parse_time_zone(rec["loc"].as_str().unwrap()).unwrap());
        let parser = Parser::new(&ctx, store, "f.parquet", data.len() as i64, FileMeta { loc: Some(loc) });
        let mut parser = match parser {
            Ok(p) => p,
            Err(e) => {
                assert_eq!(Some(e.as_str()), rec["err"].as_str(), "{case}");
                checked += 1;
                continue;
            }
        };
        let cols: Vec<String> = serde_json::from_value(rec["cols"].clone()).unwrap();
        assert_eq!(parser.columns(), cols, "{case}");
        let want: Vec<Vec<String>> = serde_json::from_value(rec["rows"].clone()).unwrap_or_default();
        let mut got = Vec::new();
        loop {
            match parser.read_row() {
                Ok(true) => got.push(parser.last_row().row.iter().map(datum).collect::<Vec<_>>()),
                Ok(false) => break,
                Err(e) => {
                    assert_eq!(Some(e.as_str()), rec["err"].as_str(), "{case}");
                    break;
                }
            }
        }
        assert_eq!(got, want, "{case}");
        assert_eq!(parser.pos().0, want.len() as i64, "{case}");
        assert_eq!(parser.scanned_pos(), data.len() as i64, "{case}");
        checked += 1;
    }
    assert_eq!(checked, 18);
}
