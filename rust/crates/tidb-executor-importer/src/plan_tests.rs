//! Go master's `NewImportPlan`, `InitDataFiles` and `PopulateChunks`
//! against this port.
//!
//! `testdata/import_into_plan_go.jsonl` was printed by an uncommitted probe in
//! a Go master worktree (`GOTOOLCHAIN=go1.27.3 go test --tags=intest
//! ./zzprobe/e05plan -run TestPlanCapture`). On a mock store with table
//! `t(a int primary key, b varchar(10), c int, key(c))` it wrote five files
//! (two CSVs, a gzipped CSV, a SQL dump and a `.txt`) to a temporary
//! directory, compiled each IMPORT INTO statement, and recorded the plan's
//! Go JSON (without the table metadata), its parameters, the data files
//! `InitDataFiles` found with their real sizes, `PopulateChunks`' engines
//! (with the timestamp zeroed) and every error. The directory is `$DIR` in
//! the capture.

use super::*;
use serde_json::Value;

fn session(rec: &Value) -> ImportSessionInfo {
    let plan = &rec["plan"];
    ImportSessionInfo {
        sql_mode: plan["SQLMode"].as_i64().unwrap_or(1_436_549_152),
        important_sys_vars: serde_json::from_value(plan["ImportantSysVars"].clone()).unwrap_or_default(),
        location_id: "Asia/Shanghai".to_owned(),
        distsql_scan_concurrency: 15,
        user: String::new(),
        keyspace: String::new(),
        new_collation: true,
        max_dist_task_nodes: Some("-1".to_owned()),
        max_node_count_by_stores: 3,
    }
}

#[test]
fn plan_matches_go_capture() {
    let mut lines = include_str!("../testdata/import_into_plan_go.jsonl").lines();
    let header: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    let meta = tidb_meta::value::parse_table_info(header["table_info"].to_string().as_bytes(), 0).unwrap();
    let cpu = header["cpu"].as_u64().unwrap() as usize;
    let dir = tempfile::tempdir().unwrap();
    let dir_text = dir.path().to_str().unwrap().to_owned();
    let ctx = tidb_gocontext::Context::background();
    let mut checked = 0;
    for line in lines {
        let rec: Value = serde_json::from_str(line).unwrap();
        for f in rec["files"].as_array().unwrap() {
            let data = hex::decode(f["data"].as_str().unwrap()).unwrap();
            std::fs::write(dir.path().join(f["name"].as_str().unwrap()), data).unwrap();
        }
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        let sql_template = rec["sql"].as_str().unwrap();
        let sql = sql_template.replace("$DIR", &dir_text);
        let unsubst = |s: &str| s.replace(&dir_text, "$DIR");
        let tidb_ast::Stmt::Dml(dml) = tidb_parser::parse(&sql).unwrap() else { panic!() };
        let tidb_ast::DmlStmt::ImportInto(stmt) = dml.as_ref() else { panic!() };
        let want_err = rec["err"].as_str();
        let db_id = rec["plan"]["DBID"].as_i64().unwrap_or(1);
        let plan = Plan::new_import_plan_from_input(stmt, "test", db_id, meta.clone(), &session(&rec), cpu, "");
        let plan = match plan {
            Ok(plan) => plan,
            Err(e) => {
                assert_eq!(Some(unsubst(&e.to_mysql_error().error_string()).as_str()), want_err, "{sql_template}");
                continue;
            }
        };
        let table = tidb_table::table_from_meta::table_from_meta(&meta, &tidb_table::table_from_meta::AutoIdSource::Unavailable).unwrap();
        let args = ASTArgs { columns_and_user_vars: stmt.columns_and_user_vars.clone(), column_assignments: stmt.column_assignments.clone() };
        let mut ctrl = LoadDataController::new(plan, table, args).unwrap();
        assert_eq!(
            ctrl.plan.parameters.as_ref().map(|p| unsubst(&p.to_string())).as_deref(),
            rec["parameters"].as_str().filter(|s| !s.is_empty()),
            "{sql_template}: parameters"
        );
        match ctrl.init_data_files(&ctx) {
            Err(e) => assert_eq!(Some(unsubst(&e.to_mysql_error().error_string()).as_str()), want_err, "{sql_template}"),
            Ok(()) => {
                let files: Vec<Value> = ctrl
                    .data_files
                    .iter()
                    .map(|f| serde_json::json!({"path": f.path, "size": f.file_size, "real": f.real_size, "type": f.r#type as i32, "compression": f.compression as i32}))
                    .collect();
                let want_files = rec["data_files"].as_array().cloned().unwrap_or_default();
                assert_eq!(files, want_files, "{sql_template}: data files");
                assert_eq!(ctrl.plan.total_file_size, rec["total_size"].as_i64().unwrap(), "{sql_template}");
                assert_eq!(ctrl.total_real_size, rec["real_size"].as_i64().unwrap(), "{sql_template}");
                assert_eq!(ctrl.plan.format, rec["format"].as_str().unwrap(), "{sql_template}");
                let mut chunks = ctrl.populate_chunks(&ctx).unwrap();
                chunks.values_mut().flatten().for_each(|c| c.timestamp = 0);
                let chunks: serde_json::Map<String, Value> =
                    chunks.into_iter().map(|(k, v)| (k.to_string(), serde_json::to_value(v).unwrap())).collect();
                assert_eq!(Value::Object(chunks), rec["chunks"], "{sql_template}: chunks");
                assert_eq!(want_err, None, "{sql_template}");
            }
        }
        ctrl.close();
        let mut got = ctrl.plan.clone();
        got.table_info = None;
        got.desired_table_info = None;
        let got: Value = serde_json::from_str(&unsubst(&got.to_go_json().to_string())).unwrap();
        assert_eq!(got, rec["plan"], "{sql_template}: plan JSON");
        let back = Plan::from_go_json(&rec["plan"]).unwrap().to_go_json();
        assert_eq!(back, rec["plan"], "{sql_template}: plan JSON round trip");
        checked += 1;
    }
    assert!(checked >= 10, "{checked} plans checked");
}
