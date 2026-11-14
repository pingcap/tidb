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
