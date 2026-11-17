//! Go `region_test.go` and `loader_test.go` cases for the ported parts,
//! and streaming checks for the parsers over storage readers.

use std::collections::BTreeMap;
use std::sync::Arc;

use tidb_gocontext::Context;
use tidb_objstore::storeapi::Storage;

use crate::region::{allocate_engine_ids, split_large_csv, DataDivideConfig, TableRegion};
use crate::{CsvConfig, CsvParser, FileInfo, MDTableMeta, Parser, SourceFileMeta, SourceType};

/// Go `NewDataDivideConfig` with a zero `config.App` and Mydumper defaults.
fn divide_config(store: Arc<dyn Storage>, columns: usize, csv: CsvConfig, max_region_size: i64) -> DataDivideConfig {
    DataDivideConfig {
        column_cnt: columns,
        engine_data_size: 0,
        max_chunk_size: max_region_size,
        concurrency: 0,
        engine_concurrency: 0,
        batch_import_ratio: 0.0,
        store,
        table_meta: MDTableMeta { db: "csv".into(), name: "t".into(), ..MDTableMeta::default() },
        skip_parquet_row_count: false,
        strict_format: true,
        data_character_set: String::new(),
        data_invalid_char_replace: String::new(),
        read_block_size: crate::READ_BLOCK_SIZE as i64,
        csv,
    }
}

fn header_csv() -> CsvConfig {
    CsvConfig {
        fields_terminated_by: b",".to_vec(),
        header: true,
        header_schema_match: true,
        field_null_defined_by: vec![b"NULL".to_vec()],
        fields_escaped_by: b"\\".to_vec(),
        ..CsvConfig::default()
    }
}

fn file_in(dir: &std::path::Path, name: &str, content: &[u8]) -> (Arc<dyn Storage>, FileInfo) {
    std::fs::write(dir.join(name), content).unwrap();
    let store: Arc<dyn Storage> = Arc::new(tidb_objstore::LocalStorage::new(dir.to_str().unwrap()).unwrap());
    let info = FileInfo {
        file_meta: SourceFileMeta {
            path: name.into(),
            r#type: SourceType::CSV,
            file_size: content.len() as i64,
            ..SourceFileMeta::default()
        },
        ..FileInfo::default()
    };
    (store, info)
}

fn offsets(regions: &[TableRegion]) -> Vec<[i64; 2]> {
    regions.iter().map(|r| [r.chunk.offset, r.chunk.end_offset]).collect()
}

#[test]
fn test_allocate_engine_ids() {
    let sizes = vec![1.0; 700];
    let mut regions = vec![TableRegion::default(); 700];
    let mut check = |batch: f64, ratio: f64, conc: f64, expected: &[(i32, usize)]| {
        allocate_engine_ids(&mut regions, &sizes, batch, ratio, conc);
        let mut actual = BTreeMap::new();
        for r in &regions {
            *actual.entry(r.engine_id).or_insert(0usize) += 1;
        }
        assert_eq!(actual, expected.iter().copied().collect::<BTreeMap<_, _>>(), "batch {batch} ratio {ratio}");
    };
    check(1000.0, 0.5, 1000.0, &[(0, 700)]);
    check(200.0, 0.5, 1000.0, &[(0, 170), (1, 213), (2, 317)]);
    check(200.0, 0.6, 1000.0, &[(0, 160), (1, 208), (2, 332)]);
    check(100.0, 0.5, 1000.0, &[(0, 93), (1, 105), (2, 122), (3, 153), (4, 227)]);
    check(
        50.0,
        0.5,
        4.0,
        &[(0, 50), (1, 59), (2, 73), (3, 110), (4, 50), (5, 50), (6, 50), (7, 50), (8, 50), (9, 50), (10, 50), (11, 50), (12, 8)],
    );
    check(100.0, 0.0, 1000.0, &[(0, 100), (1, 100), (2, 100), (3, 100), (4, 100), (5, 100), (6, 100)]);
}

#[test]
fn test_split_large_file() {
    let content = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/csv/split_large_file.csv")).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (store, info) = file_in(dir.path(), "split_large_file.csv", &content);
    for (max, expected) in [
        (1, vec![[6, 12], [12, 18], [18, 24], [24, 30]]),
        (6, vec![[6, 18], [18, 24], [24, 30]]),
        (8, vec![[6, 18], [18, 24], [24, 30]]),
        (12, vec![[6, 24], [24, 30]]),
        (13, vec![[6, 24], [24, 30]]),
        (18, vec![[6, 24], [24, 30]]),
        (19, vec![[6, 24], [24, 30]]),
    ] {
        let cfg = divide_config(store.clone(), 3, header_csv(), max);
        let (regions, _) = split_large_csv(&Context::background(), &cfg, &info, false).unwrap();
        assert_eq!(offsets(&regions), expected, "max {max}");
        for r in &regions {
            assert_eq!(r.chunk.columns, ["a", "b", "c"]);
        }
    }
}

#[test]
fn test_split_large_file_no_new_line_at_eof() {
    let dir = tempfile::tempdir().unwrap();
    let (store, info) = file_in(dir.path(), "test.csv", b"a,b\r\n123,456\r\n789,101");
    let cfg = divide_config(store, 2, header_csv(), 1);
    let (regions, _) = split_large_csv(&Context::background(), &cfg, &info, true).unwrap();
    assert_eq!(offsets(&regions), [[4, 13], [13, 14], [14, 21]]);
    for r in &regions {
        assert_eq!(r.chunk.columns, ["a", "b"]);
    }
}

#[test]
fn test_split_large_file_with_custom_terminator() {
    let dir = tempfile::tempdir().unwrap();
    let (store, info) = file_in(dir.path(), "test2.csv", b"5|+|abc\ndef\nghi|+|6|+|\n7|+|xyz|+|8|+|\n9|+||+|10");
    let csv = CsvConfig {
        fields_terminated_by: b"|+|".to_vec(),
        lines_terminated_by: b"|+|\n".to_vec(),
        ..CsvConfig::default()
    };
    let cfg = divide_config(store, 3, csv, 1);
    let (regions, _) = split_large_csv(&Context::background(), &cfg, &info, true).unwrap();
    assert_eq!(offsets(&regions), [[0, 23], [23, 38], [38, 47]]);
}

#[test]
fn test_split_large_file_only_one_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let (store, info) = file_in(dir.path(), "test.csv", b"field1,field2\r\n123,456\r\n");
    let cfg = divide_config(store, 2, header_csv(), 15);
    let (regions, _) = split_large_csv(&Context::background(), &cfg, &info, true).unwrap();
    assert_eq!(offsets(&regions), [[14, 24]]);
    assert_eq!(regions[0].chunk.columns, ["field1", "field2"]);
}

#[test]
fn test_split_large_file_seek_inside_crlf() {
    let dir = tempfile::tempdir().unwrap();
    let content = b"1\r\n2\r\n3\r\n4\r\n";
    let (store, info) = file_in(dir.path(), "test.csv", content);
    let mut csv = CsvConfig { fields_terminated_by: b",".to_vec(), ..CsvConfig::default() };
    let cfg = divide_config(store.clone(), 1, csv.clone(), 2);
    let (regions, _) = split_large_csv(&Context::background(), &cfg, &info, true).unwrap();
    assert_eq!(offsets(&regions), [[0, 3], [3, 5], [5, 8], [8, 9], [9, 11], [11, 12]]);
    let pos_of_rows = |csv: &CsvConfig| {
        let r = store.open(&Context::background(), "test.csv", None).unwrap();
        let mut parser = CsvParser::new(csv, Box::new(crate::ParserReader(r)), 128, false, None).unwrap();
        let mut pos = Vec::new();
        while parser.read_row().is_ok() {
            pos.push(parser.pos().0);
        }
        pos
    };
    assert_eq!(pos_of_rows(&csv), [2, 5, 8, 11]);
    csv.lines_terminated_by = b"\r\n".to_vec();
    let cfg = divide_config(store.clone(), 1, csv.clone(), 2);
    let (regions, _) = split_large_csv(&Context::background(), &cfg, &info, true).unwrap();
    assert_eq!(offsets(&regions), [[0, 6], [6, 9], [9, 12]]);
    assert_eq!(pos_of_rows(&csv), [3, 6, 9, 12]);
}

#[test]
fn test_compressed_make_source_file_region() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Storage> = Arc::new(tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap());
    let cfg = divide_config(store, 3, CsvConfig::default(), crate::region::MAX_REGION_SIZE);
    let info = FileInfo {
        file_meta: SourceFileMeta {
            path: "a.csv.gz".into(),
            r#type: SourceType::CSV,
            compression: crate::Compression::GZ,
            file_size: 100,
            real_size: 300,
            ..SourceFileMeta::default()
        },
        ..FileInfo::default()
    };
    let (regions, sizes) = crate::region::make_source_file_region(&Context::background(), &cfg, &info).unwrap();
    assert_eq!(regions[0].chunk.end_offset, crate::region::TABLE_FILE_SIZE_INF);
    assert_eq!(regions[0].chunk.row_id_max, 300 * crate::region::COMPRESS_SIZE_FACTOR / 3);
    assert_eq!(sizes, [300.0]);
}

#[test]
fn test_make_table_regions_rebases_row_ids() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Storage> = Arc::new(tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap());
    let mut cfg = divide_config(store, 2, CsvConfig::default(), crate::region::MAX_REGION_SIZE);
    cfg.strict_format = false;
    for (name, size) in [("a.csv", 20), ("b.csv", 40)] {
        cfg.table_meta.data_files.push(FileInfo {
            file_meta: SourceFileMeta { path: name.into(), r#type: SourceType::CSV, file_size: size, real_size: size, ..SourceFileMeta::default() },
            ..FileInfo::default()
        });
    }
    let regions = crate::make_table_regions(&Context::background(), &cfg).unwrap();
    assert_eq!(
        regions.iter().map(|r| (r.chunk.prev_row_id_max, r.chunk.row_id_max)).collect::<Vec<_>>(),
        [(0, 10), (10, 30)]
    );
}

/// Go `TestSampleFileCompressRatio` / `TestEstimateFileSize`.
#[test]
fn test_sample_file_compress_ratio() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let mut data = Vec::new();
    {
        let mut w = flate2::write::GzEncoder::new(&mut data, flate2::Compression::default());
        for i in 0..1000 {
            writeln!(w, "{i},abcdefghijklmnopqrstuvwxyz,{i}").unwrap();
        }
        w.finish().unwrap();
    }
    std::fs::write(dir.path().join("test.csv.gz"), &data).unwrap();
    let store = tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap();
    let meta = SourceFileMeta {
        path: "test.csv.gz".into(),
        r#type: SourceType::CSV,
        compression: crate::Compression::GZ,
        file_size: data.len() as i64,
        ..SourceFileMeta::default()
    };
    let ratio = crate::sample_file_compress_ratio(&Context::background(), &meta, &store).unwrap();
    assert!(ratio > 1.0, "{ratio}");
    let real = crate::estimate_real_size_for_file(&Context::background(), &meta, &store);
    assert!(real > meta.file_size, "{real}");
}

/// Go `TestParallelProcess`.
#[test]
fn test_parallel_process() {
    let inputs: Vec<i32> = (0..10).collect();
    let out = crate::parallel_process(&Context::background(), &inputs, 4, |_, v| Ok(v * 2)).unwrap();
    assert_eq!(out, (0..10).map(|v| v * 2).collect::<Vec<_>>());
    let err = crate::parallel_process(&Context::background(), &inputs, 4, |_, v| {
        if *v == 5 { Err("bad".to_owned()) } else { Ok(*v) }
    })
    .unwrap_err();
    assert_eq!(err, "bad");
}

/// The parsers read a storage file in blocks: tokens and rows that cross a
/// block boundary parse as they do from one buffer.
#[test]
fn parsers_stream_across_blocks() {
    let mut csv_data = Vec::new();
    for i in 0..500 {
        csv_data.extend_from_slice(format!("{i},\"quoted, value {i}\",tail\n").as_bytes());
    }
    let config = CsvConfig { fields_terminated_by: b",".to_vec(), fields_enclosed_by: b"\"".to_vec(), ..CsvConfig::default() };
    let mut parser = CsvParser::new(&config, Box::new(std::io::Cursor::new(csv_data)), 7, false, None).unwrap();
    let mut rows = 0;
    while let Ok(row) = parser.read_row() {
        assert_eq!(row.len(), 3);
        rows += 1;
    }
    assert_eq!(rows, 500);

    let mut sql = b"INSERT INTO t VALUES ".to_vec();
    for i in 0..300 {
        if i > 0 {
            sql.push(b',');
        }
        sql.extend_from_slice(format!("({i},'v{i}',NULL)").as_bytes());
    }
    sql.extend_from_slice(b";\n");
    let mut chunk = crate::ChunkParser::with_block_size(sql, false, 3);
    let mut rows = 0;
    while chunk.read_row().is_ok() {
        assert_eq!(chunk.last_row().row.len(), 3);
        rows += 1;
    }
    assert_eq!(rows, 300);
}

/// Go `TestParquetFileRegionUsesRealSizeForEngineAllocation`'s row count: a
/// Parquet region's `RowIDMax` is the file's row count, read from its footer.
#[test]
fn test_parquet_file_region_row_count() {
    let dir = tempfile::tempdir().unwrap();
    let src = concat!(env!("CARGO_MANIFEST_DIR"), "/../tidb-dumpformat-parquetfile/testdata/parquet_go.jsonl");
    let line = std::fs::read_to_string(src).unwrap().lines().find(|l| l.contains("\"row_groups\"")).unwrap().to_owned();
    let rec: serde_json::Value = serde_json::from_str(&line).unwrap();
    use base64::Engine as _;
    let data = base64::engine::general_purpose::STANDARD.decode(rec["file"].as_str().unwrap()).unwrap();
    std::fs::write(dir.path().join("a.parquet"), &data).unwrap();
    let store: Arc<dyn Storage> = Arc::new(tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap());
    let mut cfg = divide_config(store, 2, CsvConfig::default(), crate::region::MAX_REGION_SIZE);
    cfg.table_meta.data_files.push(FileInfo {
        file_meta: SourceFileMeta {
            path: "a.parquet".into(),
            r#type: SourceType::Parquet,
            file_size: data.len() as i64,
            real_size: 1000,
            ..SourceFileMeta::default()
        },
        ..FileInfo::default()
    });
    let regions = crate::make_table_regions(&Context::background(), &cfg).unwrap();
    assert_eq!(regions[0].chunk.row_id_max, 5);
    assert_eq!(regions[0].chunk.end_offset, i64::MAX);
}
