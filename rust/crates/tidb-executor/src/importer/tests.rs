use super::*;

fn import(sql: &str) -> ImportIntoStmt {
    let tidb_ast::Stmt::Dml(dml) = tidb_parser::parse(sql).unwrap() else {
        panic!()
    };
    let tidb_ast::DmlStmt::ImportInto(stmt) = dml.as_ref() else {
        panic!()
    };
    *stmt.clone()
}

/// Go `TestInitDefaultOptions`.
#[test]
fn test_init_default_options() {
    let mut plan = Plan {
        data_source_type: "query".into(),
        ..Plan::default()
    };
    plan.init_default_options(10, "");
    assert_eq!(plan.thread_cnt, 2);
    plan = Plan {
        data_source_type: "file".into(),
        ..Plan::default()
    };
    plan.init_default_options(1, "s3://bucket/dxf/");
    assert_eq!(plan.disk_quota, 0);
    assert_eq!(plan.checksum, PostOpLevel::Required);
    assert_eq!(plan.thread_cnt, 1);
    assert_eq!(plan.max_write_speed, 0);
    assert!(!plan.split_file);
    assert_eq!(plan.max_recorded_errors, 100);
    assert_eq!(plan.on_dup_key, "error");
    assert!(!plan.detached);
    assert_eq!(plan.charset.as_deref(), Some("utf8mb4"));
    assert!(!plan.disable_tikv_import_mode);
    assert_eq!(
        plan.max_engine_size,
        if tidb_config_kerneltype::is_next_gen() {
            100 << 30
        } else {
            500 << 30
        }
    );
    assert_eq!(plan.cloud_storage_uri, "s3://bucket/dxf/");
    plan.init_default_options(10, "");
    assert_eq!(plan.thread_cnt, 5);
}

/// Go `TestInitOptionsPositiveCase`.
#[test]
fn test_init_options_positive_case() {
    let sql = "import into t from '/file.csv' with character_set='utf8', fields_terminated_by='aaa', fields_enclosed_by='|', fields_escaped_by='', fields_defined_null_by='N', lines_terminated_by='END', skip_rows=1, disk_quota='100gib', checksum_table='optional', thread=100000, max_write_speed='200mib', split_file, record_errors=123, detached, disable_tikv_import_mode, __max_engine_size='100gib', disable_precheck";
    let plan = Plan::new_import_plan(&import(sql), 6, "").unwrap();
    assert_eq!(plan.charset.as_deref(), Some("utf8"));
    assert_eq!(plan.fields_terminated_by, "aaa");
    assert_eq!(plan.fields_enclosed_by, "|");
    assert_eq!(plan.fields_escaped_by, "");
    assert_eq!(plan.field_null_def, ["N"]);
    assert_eq!(plan.lines_terminated_by, "END");
    assert_eq!(plan.ignore_lines, 1);
    assert_eq!(plan.disk_quota, 100 << 30);
    assert_eq!(plan.checksum, PostOpLevel::Optional);
    assert_eq!(plan.on_dup_key, "error");
    assert_eq!(plan.thread_cnt, 6);
    assert_eq!(plan.max_write_speed, 200 << 20);
    assert!(
        plan.split_file && plan.detached && plan.disable_tikv_import_mode && plan.disable_precheck
    );
    assert_eq!(plan.max_recorded_errors, 123);
    assert_eq!(plan.max_engine_size, 100 << 30);
    assert!(plan.cloud_storage_uri.is_empty());
    let plan = Plan::new_import_plan(
        &import(&format!("{sql}, on_duplicate_key='capture'")),
        6,
        "s3://bucket/path/dxf/",
    )
    .unwrap();
    assert_eq!(plan.cloud_storage_uri, "s3://bucket/path/dxf/");
    assert_eq!(plan.on_dup_key, "capture");
    for uri in [
        "s3://bucket/path2",
        "gs://bucket/path2",
        "azure://container/path2",
        "azblob://container/path3",
        "",
    ] {
        let plan = Plan::new_import_plan(
            &import(&format!("{sql}, cloud_storage_uri='{uri}'")),
            6,
            "s3://bucket/path/dxf/",
        )
        .unwrap();
        assert_eq!(plan.cloud_storage_uri, uri);
    }
}

/// Go `TestParseFileType`'s extension matrix.
#[test]
fn test_parse_file_type() {
    for (path, format) in [
        ("test.sql", "sql"),
        ("data.parquet", "parquet"),
        ("file.csv", "csv"),
        ("noext", "csv"),
        ("test.sql.gz", "sql"),
        ("data.parquet.zst", "parquet"),
        ("file.csv.snappy", "csv"),
        ("file.gz", "csv"),
        ("document.txt.gz", "csv"),
        ("TEST.SQL.GZ", "sql"),
        ("file.PARQUET.zst", "parquet"),
        ("backup.file.sql.gz", "sql"),
        (".hidden.sql.gz", "sql"),
    ] {
        assert_eq!(parse_file_type(path), format, "{path}");
    }
}

/// Go `TestAdjustOptions`.
#[test]
fn test_adjust_options() {
    let mut plan = Plan {
        disk_quota: 1,
        thread_cnt: 100000000,
        max_write_speed: 10,
        data_source_type: "file".into(),
        ..Plan::default()
    };
    plan.adjust_options(16);
    assert_eq!(plan.thread_cnt, 16);
    assert_eq!(plan.max_write_speed, 10);
    assert!(!plan.disable_tikv_import_mode);
    plan.thread_cnt = 100000000;
    plan.data_source_type = "query".into();
    plan.adjust_options(16);
    assert_eq!(plan.thread_cnt, 32);
    assert!(!plan.disable_tikv_import_mode);
    plan.cloud_storage_uri = "s3://bucket/path".into();
    plan.adjust_options(16);
    assert!(plan.disable_tikv_import_mode);
}

/// Go `TestGetConflictHandlingMode`.
#[test]
fn test_get_conflict_handling_mode() {
    let mut plan = Plan::default();
    assert_eq!(plan.get_on_dup_key_mode(), "error");
    plan.on_dup_key = "capture".into();
    assert_eq!(plan.get_on_dup_key_mode(), "capture");
    plan.on_dup_key = "error".into();
    assert_eq!(plan.get_on_dup_key_mode(), "error");
}

/// Go testkit `zzprobe/e05.TestImportControllerMapping`, exercising
/// `NewImportPlan` -> `NewLoadDataController` (2026-11-14).
#[test]
fn test_import_controller_mapping() {
    let table = mapping_table();
    for line in include_str!("../../testdata/import_into_mapping_go.txt").lines() {
        let fields: Vec<_> = line.split('\t').collect();
        let stmt = import(fields[0]);
        let plan = Plan::new_import_plan(&stmt, 6, "").unwrap();
        let result = LoadDataController::new(
            plan,
            table.clone(),
            ASTArgs {
                columns_and_user_vars: stmt.columns_and_user_vars,
                column_assignments: stmt.column_assignments,
            },
        );
        if fields[1].is_empty() {
            let ctrl = result.unwrap();
            let mappings: Vec<_> = ctrl
                .field_mappings
                .iter()
                .map(|field| {
                    field
                        .column
                        .as_ref()
                        .map(|col| col.read().name.original().to_owned())
                        .or_else(|| field.user_var.as_ref().map(|name| format!("@{name}")))
                        .unwrap_or_else(|| "<nil>".to_owned())
                })
                .collect();
            assert_eq!(mappings.join(","), fields[2], "{}", fields[0]);
            assert_eq!(
                ctrl.insert_columns
                    .iter()
                    .map(|col| col.read().name.original().to_owned())
                    .collect::<Vec<_>>()
                    .join(","),
                fields[3],
                "{}",
                fields[0]
            );
            assert_eq!(ctrl.get_field_count(), mappings.len());
        } else {
            let message = fields[1]
                .split_once(']')
                .map_or(fields[1], |(_, message)| message);
            assert_eq!(
                result.unwrap_err().to_mysql_error().message,
                message,
                "{}",
                fields[0]
            );
        }
    }
}

/// Go `TestPlanUseNewCollate`'s legacy fallback and captured-mode assertions.
#[test]
fn test_plan_use_new_collate() {
    let mut plan = Plan::default();
    assert!(plan.get_use_new_collate_or_default(true));
    assert!(!plan.get_use_new_collate_or_default(false));
    plan.set_use_new_collate(false);
    assert!(!plan.get_use_new_collate_or_default(true));
    plan.set_use_new_collate(true);
    assert!(plan.get_use_new_collate_or_default(false));
}

fn mapping_table() -> tidb_table::tables::TableCommon {
    tidb_table::tables::TableCommon::new(
        1,
        ["a", "b"]
            .into_iter()
            .enumerate()
            .map(|(offset, name)| tidb_table::column::Column {
                info: tidb_model::GoShared::new(tidb_model::ColumnInfo {
                    id: offset as i64 + 1,
                    offset: offset as i64,
                    name: tidb_ast::CiString::new(name),
                    field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Long),
                    state: tidb_model::SchemaState::PUBLIC,
                    ..Default::default()
                }),
                generated: None,
                default_value: None,
                origin_default: None,
            })
            .collect(),
    )
}

/// Go `zzprobe/e05.TestImportCSVInterpretation`: controller config and
/// `HandleSkipNRows` followed by `CSVParser.ReadRow`, including terminal EOF.
#[test]
fn test_import_csv_interpretation() {
    use tidb_lightning_mydump::{CharsetConvertor, CsvParser};
    for line in include_str!("../../testdata/import_into_csv_go.jsonl").lines() {
        let capture: serde_json::Value = serde_json::from_str(line).unwrap();
        let stmt = import(capture["sql"].as_str().unwrap());
        let plan = Plan::new_import_plan(&stmt, 6, "").unwrap();
        let ctrl = LoadDataController::new(plan, mapping_table(), ASTArgs::default());
        let expected = capture["error"].as_str().unwrap();
        let expected = expected
            .split_once(']')
            .map_or(expected, |(_, message)| message);
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        let actual = match ctrl {
            Err(error) => error.to_mysql_error().message,
            Ok(ctrl) => {
                let convertor =
                    CharsetConvertor::new(ctrl.plan.charset.as_deref().unwrap(), "�").unwrap();
                let mut parser = CsvParser::new(
                    &ctrl.generate_csv_config(),
                    capture["source"].as_str().unwrap().as_bytes().to_vec(),
                    Some(convertor),
                )
                .unwrap();
                handle_skip_n_rows(&mut parser, ctrl.plan.ignore_lines).unwrap();
                loop {
                    match parser.read_row() {
                        Err(error) => break error.to_string(),
                        Ok(row) => rows.push(
                            row.into_iter()
                                .map(|datum| match datum {
                                    tidb_datatype::Datum::Null => None,
                                    tidb_datatype::Datum::Bytes(bytes) => {
                                        Some(String::from_utf8(bytes).unwrap())
                                    }
                                    other => panic!("CSV field {other:?}"),
                                })
                                .collect(),
                        ),
                    }
                }
            }
        };
        assert_eq!(actual, expected, "{}", capture["sql"]);
        assert_eq!(
            serde_json::to_value(rows).unwrap(),
            capture["rows"],
            "{}",
            capture["sql"]
        );
    }
}

/// Go `TestParserContract` probe: shared parser metadata, checkpoints,
/// `ReadChunks`, `ReadUntil` and `HandleSkipNRows` for CSV and SQL.
#[test]
fn test_import_parser_contract() {
    use serde_json::{json, Value};
    use tidb_lightning_mydump::{read_chunks, read_until, ChunkParser, CsvParser, Parser};
    for line in include_str!("../../testdata/import_parser_contract_go.jsonl").lines() {
        let expected: Value = serde_json::from_str(line).unwrap();
        let format = expected[0].as_str().unwrap();
        let make_parser = || -> Box<dyn Parser> {
            if format == "sql" {
                Box::new(ChunkParser::new(
                    b"INSERT INTO t (a,b) VALUES (1,'ab'),(2,NULL),(3,'z');".to_vec(),
                    false,
                ))
            } else {
                let plan = Plan::new_import_plan(
                    &import("import into t from '/file.csv' format 'csv'"),
                    6,
                    "",
                )
                .unwrap();
                Box::new(
                    CsvParser::new(
                        &plan.generate_csv_config(),
                        b"1,\"ab\"\n2,\\N\n3,z\n".to_vec(),
                        None,
                    )
                    .unwrap(),
                )
            }
        };
        let mut parser = make_parser();
        let result = if let Some(skip) = expected[1].as_u64() {
            handle_skip_n_rows(parser.as_mut(), skip).unwrap();
            let mut rows = Vec::new();
            loop {
                let r = parser.read_row();
                let (pos, id) = parser.pos();
                if let Err(e) = r {
                    rows.push(json!([pos, id, e.to_string()]));
                    break;
                }
                let r = parser.last_row();
                let values: Vec<Value> = r
                    .row
                    .iter()
                    .map(|v| match v {
                        tidb_datatype::Datum::Null => json!([0, ""]),
                        tidb_datatype::Datum::UInt(n) => json!([2, n.to_string()]),
                        tidb_datatype::Datum::String(s) => {
                            json!([5, String::from_utf8_lossy(s.bytes())])
                        }
                        other => panic!("unexpected {other:?}"),
                    })
                    .collect();
                rows.push(json!([pos, id, r.length, values]));
            }
            json!([format, skip, rows])
        } else {
            match expected[1].as_str().unwrap() {
                "resume" => {
                    parser.read_row().unwrap();
                    let pos = parser.pos().0;
                    parser = make_parser();
                    parser.set_pos(pos, 11).unwrap();
                    parser.set_columns(vec!["restored".into()]);
                    parser.read_row().unwrap();
                    let (pos, id) = parser.pos();
                    json!([
                        format,
                        "resume",
                        pos,
                        id,
                        parser.last_row().length,
                        parser.columns()
                    ])
                }
                "chunks" => {
                    let chunks = read_chunks(parser.as_mut(), 10).unwrap();
                    let chunks:Vec<_>=chunks.into_iter().map(|c|json!({"Offset":c.offset,"EndOffset":c.end_offset,"RealOffset":c.real_offset,"PrevRowIDMax":c.prev_row_id_max,"RowIDMax":c.row_id_max,"Columns":if c.columns.is_empty(){Value::Null}else{json!(c.columns)}})).collect();
                    json!([format, "chunks", chunks])
                }
                "until" => {
                    read_until(parser.as_mut(), 10).unwrap();
                    let (pos, id) = parser.pos();
                    json!([format, "until", pos, id])
                }
                other => panic!("unknown operation {other}"),
            }
        };
        assert_eq!(result, expected, "{line}");
    }
}

/// Go `TestSupportedSuffixForServerDisk`, with source discovery/error outputs
/// captured by the `TestImportLocalFiles` testkit probe.
#[test]
#[cfg(unix)]
fn test_supported_suffix_for_server_disk() {
    use serde_json::{json, Value};
    let dir = tempfile::tempdir().unwrap();
    for name in [
        "server-0.csv",
        "server-1.csv",
        "server-2.csv",
        "what?.csv",
        "whatx.csv",
        "文件.csv",
        "a[.csv",
    ] {
        std::fs::write(dir.path().join(name), b"1,abc\n").unwrap();
    }
    std::fs::create_dir(dir.path().join("nested")).unwrap();
    std::fs::write(dir.path().join("nested/hidden.csv"), b"2,xyz\n").unwrap();
    std::os::unix::fs::symlink(dir.path().join("server-0.csv"), dir.path().join("link.csv"))
        .unwrap();
    std::os::unix::fs::symlink(dir.path().join("absent"), dir.path().join("dangling.csv")).unwrap();
    for line in include_str!("../../testdata/import_local_files_go.jsonl").lines() {
        let expected: Value = serde_json::from_str(line).unwrap();
        let name = expected[0].as_str().unwrap();
        let path = if name == "relative.csv" {
            name.to_owned()
        } else {
            dir.path().join(name).to_string_lossy().into_owned()
        };
        let result = super::super::local::find_local_data_files(&path, true);
        match result {
            Ok((_, files)) => {
                assert_eq!(expected[2], "");
                let files: Vec<_> = files.into_iter().map(|f| json!([f.path, f.size])).collect();
                assert_eq!(
                    json!(files),
                    if expected[1].is_null() {
                        json!([])
                    } else {
                        expected[1].clone()
                    },
                    "{line}"
                );
            }
            Err(e) => {
                let expected_message = expected[2]
                    .as_str()
                    .unwrap()
                    .split_once(']')
                    .map_or(expected[2].as_str().unwrap(), |(_, s)| s);
                assert_eq!(
                    e.to_mysql_error().message.replace(dir.path().to_str().unwrap(), "<dir>"),
                    expected_message,
                    "{line}"
                );
            }
        }
    }
}
