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

#[test]
fn streaming_page_reader_does_not_load_the_file_suffix() {
    let ctx = Context::background();
    let store = Arc::new(tidb_objstore::MemStorage::new());
    let data: Vec<u8> = (0..PAGE_HEADER_READ_BUFFER * 3).map(|i| (i % 251) as u8).collect();
    store.write_file(&ctx, "page", &data).unwrap();
    let file = StorageFile {
        ctx,
        store,
        path: Arc::from("page"),
        // Simulates an inaccessible tail beyond the readable page data.
        size: PAGE_HEADER_READ_BUFFER * 16,
        buffered: None,
    };

    let mut reader = file.get_read(17).unwrap();
    let mut output = vec![0; PAGE_HEADER_READ_BUFFER as usize + 1];
    reader.read_exact(&mut output).unwrap();
    assert_eq!(output, data[17..17 + output.len()]);
}

#[test]
fn column_stream_resumes_header_and_payload_reads_at_the_requested_offsets() {
    let ctx = Context::background();
    let store = Arc::new(tidb_objstore::MemStorage::new());
    let data: Vec<u8> = (0..20_000).map(|i| (i % 251) as u8).collect();
    store.write_file(&ctx, "pages", &data).unwrap();
    let file = StorageFile {
        ctx,
        store,
        path: Arc::from("pages"),
        size: data.len() as u64,
        buffered: None,
    };
    let column = StorageRange::open(&file, 13..10_017).unwrap();
    let mut header = column.get_read(13).unwrap();
    let mut buf = [0; 8];
    header.read_exact(&mut buf).unwrap();
    assert_eq!(buf, data[13..21]);
    assert_eq!(column.get_bytes(21, 9000).unwrap(), data[21..9021]);
    // A separate read can seek while an earlier header reader remains alive.
    header.read_exact(&mut buf).unwrap();
    assert_eq!(buf, data[21..29]);
    let mut last = column.get_read(10_014).unwrap();
    let mut tail = Vec::new();
    last.read_to_end(&mut tail).unwrap();
    assert_eq!(tail, data[10_014..10_017]);
    assert!(column.get_read(12).is_err());
    assert!(column.get_bytes(10_014, 4).is_err());
}

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
        let meta = FileMeta { loc: Some(loc) };
        let parser = Parser::new(&ctx, store.clone(), "f.parquet", data.len() as i64, meta.clone());
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
        if rec["case"] == "row_groups" {
            let mut parser = Parser::new(&ctx, store, "f.parquet", data.len() as i64, meta).unwrap();
            assert!(parser.read_row().unwrap());
            let first = parser.last_row().clone();
            let next = want.len() - 1;
            parser.set_pos(next as i64, 42).unwrap();
            assert_eq!(parser.pos(), (next as i64, 42));
            assert_eq!(parser.last_row().row, first.row);
            assert_eq!(parser.last_row().length, first.length);
            assert!(parser.read_row().unwrap());
            assert_eq!(parser.last_row().row_id, 43);
            assert_eq!(parser.last_row().row.iter().map(datum).collect::<Vec<_>>(), want[next]);
        }
        checked += 1;
    }
    assert_eq!(checked, 18);
}

/// Go `TestParquetParser`'s `dictionary_encoded_decimal` (#70365): enough
/// repeated rows that the writer keeps the dictionary page, so every row
/// decodes from one shared buffer, which the decimal conversion must not
/// consume in place.
#[test]
fn dictionary_encoded_decimal() {
    use parquet::data_type::{ByteArray, ByteArrayType, FixedLenByteArray, FixedLenByteArrayType};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    const ROWS: usize = 256;
    let schema = Arc::new(
        parquet::schema::parser::parse_message_type(
            "message schema {
                optional binary positive (DECIMAL(9,2));
                optional binary negative (DECIMAL(9,2));
                optional fixed_len_byte_array(4) fixed_len (DECIMAL(9,2));
                optional binary oversized (DECIMAL(75,2));
            }",
        )
        .unwrap(),
    );
    let mut oversized = vec![0x00, 0x01];
    oversized.extend([0u8; 31]); // 2^248
    let byte_arrays: [Vec<u8>; 2] = [vec![0x30, 0x39], vec![0xcf, 0xc7]]; // 12345, -12345
    let mut data = Vec::new();
    {
        // parquet-rs dictionary-encodes FIXED_LEN_BYTE_ARRAY only under the
        // 2.0 writer version.
        let props = Arc::new(
            WriterProperties::builder()
                .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
                .set_dictionary_enabled(true)
                .build(),
        );
        let mut writer = SerializedFileWriter::new(&mut data, schema, props).unwrap();
        let mut group = writer.next_row_group().unwrap();
        let levels = vec![1i16; ROWS];
        for (index, value) in byte_arrays.iter().chain(std::iter::once(&oversized)).enumerate() {
            if index == 2 {
                let mut column = group.next_column().unwrap().unwrap();
                let values = vec![FixedLenByteArray::from(vec![0x00, 0x00, 0x30, 0x39]); ROWS];
                column.typed::<FixedLenByteArrayType>().write_batch(&values, Some(&levels), None).unwrap();
                column.close().unwrap();
            }
            let mut column = group.next_column().unwrap().unwrap();
            let values = vec![ByteArray::from(value.clone()); ROWS];
            column.typed::<ByteArrayType>().write_batch(&values, Some(&levels), None).unwrap();
            column.close().unwrap();
        }
        group.close().unwrap();
        writer.close().unwrap();
    }
    let reader = parquet::file::reader::SerializedFileReader::new(bytes::Bytes::from(data.clone())).unwrap();
    use parquet::file::reader::FileReader as _;
    for column in reader.metadata().row_group(0).columns() {
        assert!(column.dictionary_page_offset().is_some(), "{} is not dictionary encoded", column.column_path());
    }

    let ctx = Context::background();
    let store = Arc::new(tidb_objstore::MemStorage::new());
    store.write_file(&ctx, "f.parquet", &data).unwrap();
    let store: Arc<dyn Storage> = store;
    let mut parser = Parser::new(&ctx, store, "f.parquet", data.len() as i64, FileMeta::default()).unwrap();
    let expected = [
        "123.45",
        "-123.45",
        "123.45",
        "4523128485832663883733241601901871400518358776001584532791311875309106626.56",
    ];
    for _ in 0..ROWS {
        assert!(parser.read_row().unwrap());
        let row = &parser.last_row().row;
        assert_eq!(row.len(), expected.len());
        for (datum, want) in row.iter().zip(expected) {
            assert_eq!(String::from_utf8_lossy(&datum.sql_bytes().unwrap()), want);
        }
    }
}
