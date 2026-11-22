// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `pkg/executor/importer/query_source_test.go` and
//! `pkg/executor/import_into_query_test.go`: the SELECT of IMPORT INTO ...
//! FROM SELECT captured on the submitter and run on a worker session.

use super::sql_kit::{exec_all, q, session};
use super::*;
use tidb_executor::importer::chunk_process::QueryChunk;
use tidb_executor::importer::query_source::{QuerySource, SourceTable};

const IMPORT_QUERY_MEM_QUOTA: i64 = 32 << 20;

/// Go `capture` in `TestCaptureQuerySource`.
fn capture(tk: &mut Session, sel: &str) -> Result<QuerySource, String> {
    let sql = format!("import into unused_target from {sel}");
    // Go `PrepareTxnCtx` before the capture: the statement's read snapshot.
    tk.with_committed_snapshot(|tk| {
        let ctx = tk.plan_import_query_for_test(&sql).map_err(|e| format!("{e:?}"))?;
        tk.capture_query_source(&sql, &ctx).map_err(|e| format!("{e:?}"))
    })
    .map_err(|e| format!("{e:?}"))?
}

/// Go `sourceTables`: the captured tables by `db.table`.
fn source_tables(tk: &Session, q: &QuerySource) -> std::collections::BTreeMap<String, SourceTable> {
    let is = tk.lock_catalog().unwrap();
    q.tables
        .iter()
        .map(|src| {
            let db = is.schema_by_id(src.db_id).expect("source database");
            let tbl = is.table_info_by_id(src.id).expect("source table");
            let name = format!("{}.{}", db.read().name.lowercase(), tbl.read().name.lowercase());
            (name, *src)
        })
        .collect()
}

/// Go `TestCaptureQuerySource`.
#[test]
fn test_capture_query_source() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(&mut tk, &["create table query_src(i bigint)", "create table unused_target(i bigint)"]);

    // "session variables"
    exec_all(&mut tk, &["set cte_max_recursion_depth = 5", "set tidb_mem_quota_query = 1048576", "set @v = 1", "begin"]);
    let captured = capture(&mut tk, "select i from query_src").unwrap();
    let states = captured.session_states.as_ref().unwrap();
    assert_eq!(states.system_vars.get("cte_max_recursion_depth").map(String::as_str), Some("5"));
    assert!(!states.system_vars.get("timestamp").unwrap().is_empty());
    assert!(states.user_vars.contains_key("v"));
    exec_all(&mut tk, &["rollback", "set cte_max_recursion_depth = default, tidb_mem_quota_query = default"]);

    // "source tables"
    exec_all(&mut tk, &["create table query_other(i int)", "begin"]);
    let captured = capture(&mut tk, "(select i from query_src union all select i from query_other)").unwrap();
    let tables = source_tables(&tk, &captured);
    assert_eq!(tables.keys().cloned().collect::<Vec<_>>(), ["test.query_other", "test.query_src"]);
    exec_all(&mut tk, &["rollback"]);
    exec_all(&mut tk, &[
        "create temporary table query_tmp(i int)",
        "create global temporary table query_gtmp(i int) on commit delete rows",
    ]);
    for tbl in ["query_tmp", "query_gtmp"] {
        let err = capture(&mut tk, &format!("select * from {tbl}")).unwrap_err();
        assert!(err.contains("temporary source tables in IMPORT INTO FROM SELECT"), "{tbl}: {err}");
    }
    exec_all(&mut tk, &["create sequence query_seq"]);
    for (sql, msg) in [
        ("select nextval(query_seq)", "sequences in IMPORT INTO FROM SELECT"),
        ("select 1 from query_seq", "sequences in IMPORT INTO FROM SELECT"),
        ("select table_name from information_schema.tables", "virtual source tables in IMPORT INTO FROM SELECT"),
        ("select variable_name from performance_schema.session_variables", "virtual source tables in IMPORT INTO FROM SELECT"),
        (
            "select i from query_src where i in (select 1 from information_schema.processlist)",
            "virtual source tables in IMPORT INTO FROM SELECT",
        ),
    ] {
        exec_all(&mut tk, &["begin"]);
        let err = capture(&mut tk, sql).unwrap_err();
        exec_all(&mut tk, &["rollback"]);
        assert!(err.contains(msg), "{sql}: {err}");
    }
    let err = capture(&mut tk, "select ? from query_src").unwrap_err();
    assert!(err.contains("unexpected '?'"), "{err}");
    // A stale read replaces the txn context of the session, so use a new one.
    let mut stale_tk = session(&stack, 2);
    let err = capture(&mut stale_tk, "select i from query_src as of timestamp now(6)").unwrap_err();
    assert!(err.contains("stale reads in IMPORT INTO FROM SELECT"), "{err}");

    // "views and cached tables"
    exec_all(&mut tk, &[
        "create database query_view_db",
        "create table query_view_db.base(i int)",
        "create table query_cached(i int)",
        "alter table query_cached cache",
        "use query_view_db",
        "create view inner_v as select i from base",
        "use test",
        "create view outer_v as select i from query_view_db.inner_v union all select i from query_cached",
        "begin",
    ]);
    let captured = capture(&mut tk, "select * from outer_v").unwrap();
    exec_all(&mut tk, &["rollback"]);
    let tables = source_tables(&tk, &captured);
    assert_eq!(
        tables.keys().cloned().collect::<Vec<_>>(),
        ["query_view_db.base", "query_view_db.inner_v", "test.outer_v", "test.query_cached"]
    );
    // The worker rebuilds the schema the submitter resolved the query with.
    let domain = &stack.factory;
    let parent = tk.lock_catalog().unwrap().clone();
    let is = crate::domain::cluster_session_node::build_import_query_info_schema(domain, &captured, &parent).unwrap();
    for (name, src) in &tables {
        let tbl = is.table_info_by_id(src.id).unwrap_or_else(|| panic!("{name}"));
        assert_eq!(src.revision, tbl.read().revision, "{name}");
    }
    assert_eq!(captured.schema_version, is.schema_meta_version());
    // A table that changed after the query was resolved fails the rebuild.
    let mut changed = captured.clone();
    changed.tables[0].revision += 1;
    let err = crate::domain::cluster_session_node::build_import_query_info_schema(domain, &changed, &parent).unwrap_err();
    assert!(format!("{err:?}").contains("changed before the import query snapshot"), "{err:?}");

    // "views ignore local temporary tables"
    exec_all(&mut tk, &["create table query_shadowed(i int)", "create view query_shadowed_v as select i from query_shadowed"]);
    let shadowed_id = tk
        .lock_catalog()
        .unwrap()
        .table_info_by_name("test", "query_shadowed")
        .map(|(_, t)| t.read().id)
        .expect("query_shadowed");
    exec_all(&mut tk, &["create temporary table query_shadowed(i int)", "begin"]);
    let captured = capture(&mut tk, "select * from query_shadowed_v").unwrap();
    exec_all(&mut tk, &["rollback", "drop temporary table query_shadowed"]);
    let tables = source_tables(&tk, &captured);
    assert_eq!(tables.len(), 2);
    assert_eq!(tables["test.query_shadowed"].id, shadowed_id);

    // "restore session states"
    let mut submit_tk = session(&stack, 3);
    exec_all(&mut submit_tk, &["set cte_max_recursion_depth = 5, tidb_mem_quota_query = 1048576, @v = 1", "begin"]);
    let captured = capture(&mut submit_tk, "select i from query_src").unwrap();
    exec_all(&mut submit_tk, &["rollback"]);
    let mut worker = stack.factory.open_session(session_context(4)).expect("session opens");
    let mem_quota = worker.vars.mem_quota_query;
    captured.restore_states(&mut worker).unwrap();
    assert_eq!(worker.current_database(), "test");
    assert_eq!(worker.vars.get_system("cte_max_recursion_depth").unwrap(), "5");
    assert_eq!(q(&mut worker, "select @v"), [["1"]]);
    assert_eq!(
        Some(&worker.vars.get_system("timestamp").unwrap()),
        captured.session_states.as_ref().unwrap().system_vars.get("timestamp")
    );
    // The worker keeps its own memory budget and transaction, and drops the
    // warnings of the submitter.
    assert_eq!(worker.vars.mem_quota_query, mem_quota);
    assert!(!worker.in_transaction());
}

/// Go `captureImportQuery`: captures the IMPORT INTO statement `sql` that
/// `tk` runs, as a worker decodes it from the task meta.
fn capture_import_query(tk: &mut Session, sql: &str) -> QuerySource {
    let captured = tk
        .with_committed_snapshot(|tk| {
            let ctx = tk.plan_import_query_for_test(sql).unwrap();
            tk.capture_query_source(sql, &ctx).unwrap()
        })
        .unwrap();
    assert_ne!(captured.read_ts, 0);
    serde_json::from_str(&serde_json::to_string(&captured).unwrap()).unwrap()
}

/// Go `runImportQuery`: runs the captured query in `se`, and returns its
/// rows.
fn run_import_query(se: &mut Session, q: &QuerySource, sql: &str) -> Vec<String> {
    let (tx, rx) = crossbeam_channel::unbounded::<QueryChunk>();
    se.run_import_query(&tidb_gocontext::Context::background(), q, sql, IMPORT_QUERY_MEM_QUOTA, &tx).unwrap();
    drop(tx);
    rx.into_iter()
        .flat_map(|chunk| displayed(chunk.rows))
        .map(|row| format!("[{}]", row.join(" ")))
        .collect()
}

/// Go `TestImportQueryPlanExecution`.
#[test]
fn test_import_query_plan_execution() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(&mut tk, &[
        "create table query_src(g bigint, v decimal(20,2), i bigint, key idx_i(i))",
        "create table unused_target(i bigint)",
        "insert into query_src values (1,10,1),(1,20,2),(1,NULL,3),(2,7,4),(NULL,3,5),(NULL,NULL,6),(3,NULL,7)",
        "create view query_src_v as select g, i from query_src where i > 2",
        "create table query_cached(i bigint)",
        "insert into query_cached values (3),(4),(8)",
        "alter table query_cached cache",
        "set collation_connection='utf8mb4_general_ci'",
        "set tidb_isolation_read_engines='tikv'",
        "set tidb_mem_quota_query=16777216",
        "set div_precision_increment=8",
        "set group_concat_max_len=7",
        "set @runtime_v = 42",
    ]);
    let quota = Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));
    let _hook = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", {
        let quota = quota.clone();
        move |session: &mut dyn std::any::Any| {
            let session = session.downcast_mut::<Session>().expect("the worker session");
            quota.lock().unwrap().push(session.vars.mem_quota_query);
        }
    });
    for (sql_mode, sql) in [
        ("", "select i from (select i from query_src where i>0) c"),
        ("", r"select hex(_binary'\0\\a'), _latin1'a', 'a''b'"),
        ("NO_BACKSLASH_ESCAPES", r"select hex(_binary'\0\\a'), _latin1'a', 'a''b'"),
        ("", "select /*+ STREAM_AGG() SET_VAR(tidb_distsql_scan_concurrency=2) */ count(*) from query_src"),
        // The quota of the worker overrides the quota of the submitter and hints.
        ("", "select /*+ STREAM_AGG() SET_VAR(tidb_mem_quota_query=1073741824) */ count(*) from query_src"),
        ("", "select /*+ STREAM_AGG() MEMORY_QUOTA(1 GB) */ count(*) from query_src"),
        ("", "select /*+ STREAM_AGG() */ i,count(*),sum(v) from query_src use index(idx_i) where round(v,2)>5 group by i"),
        // Go master's `with c as (select /*+ MERGE() */ i from query_src)
        // select i from c` case is not here: this branch's Go parser, which
        // the port's parser follows, refuses a WITH query in IMPORT INTO's
        // parenthesized FROM (`line 1 column 24 near "with c as ..."`).
        ("", "select /*+ HASH_JOIN(a,b) */ a.i from query_src a join query_src b on a.i=b.i"),
        ("", "select a.i from query_src_v a join query_cached b on a.i=b.i"),
        ("", "select i/7, v/3 from query_src"),
        ("", "select group_concat(i order by i) from query_src"),
        ("", "select @runtime_v + i from query_src"),
    ] {
        exec_all(&mut tk, &[&format!("set sql_mode='{sql_mode}'"), "begin"]);
        let sql_text = format!("import into unused_target from ({sql}) with thread=1");
        let captured = capture_import_query(&mut tk, &sql_text);
        exec_all(&mut tk, &["rollback"]);
        let mut expected: Vec<String> = q(&mut tk, sql)
            .into_iter()
            .map(|row| format!("[{}]", row.join(" ")))
            .collect();
        let mut worker = stack.factory.open_session(session_context(10)).expect("session opens");
        let mut got = run_import_query(&mut worker, &captured, &sql_text);
        expected.sort();
        got.sort();
        assert_eq!(expected, got, "{sql_mode}/{sql}");
        assert_eq!(quota.lock().unwrap().pop(), Some(IMPORT_QUERY_MEM_QUOTA), "{sql}");
    }
    exec_all(&mut tk, &["set sql_mode=default"]);

    // "mv maintenance follows the submitter"
    let sql = "import into unused_target from select i from query_src";
    for in_maintenance in [false, true] {
        tk.vars.in_mview_maintenance = in_maintenance;
        exec_all(&mut tk, &["begin"]);
        let captured = capture_import_query(&mut tk, sql);
        exec_all(&mut tk, &["rollback"]);
        tk.vars.in_mview_maintenance = false;
        assert_eq!(captured.in_mview_maintenance, in_maintenance);
        let mut worker = stack.factory.open_session(session_context(11)).expect("session opens");
        run_import_query(&mut worker, &captured, sql);
        assert_eq!(worker.vars.in_mview_maintenance, in_maintenance);
    }

    // "statement time"
    let sql = "import into unused_target from select unix_timestamp(now())";
    exec_all(&mut tk, &["set @@timestamp = 1000000000", "begin"]);
    let captured = capture_import_query(&mut tk, sql);
    exec_all(&mut tk, &["rollback", "set @@timestamp = default"]);
    let mut worker = stack.factory.open_session(session_context(12)).expect("session opens");
    assert_eq!(run_import_query(&mut worker, &captured, sql), ["[1000000000]"]);
    // The submitter's transaction does not carry over to the worker.
    assert!(!worker.in_transaction());
    assert!(worker.is_autocommit());
    // Without @@timestamp, the statement time is captured, and the worker
    // runs at the captured time instead of its own clock.
    exec_all(&mut tk, &["begin"]);
    let mut captured = capture_import_query(&mut tk, sql);
    exec_all(&mut tk, &["rollback"]);
    let states = captured.session_states.as_mut().unwrap();
    assert!(!states.system_vars["timestamp"].is_empty());
    states.system_vars.insert("timestamp".to_owned(), "1000000000".to_owned());
    let mut worker = stack.factory.open_session(session_context(13)).expect("session opens");
    assert_eq!(run_import_query(&mut worker, &captured, sql), ["[1000000000]"]);

    // "source tables at read ts"
    exec_all(&mut tk, &["begin"]);
    let sql = "import into unused_target from select /*+ STREAM_AGG() */ count(*) from query_src";
    let captured = capture_import_query(&mut tk, sql);
    exec_all(&mut tk, &["rollback"]);
    assert_ne!(captured.schema_version, 0);
    exec_all(&mut tk, &["insert into query_src values (9,90,9)", "rename table query_src to query_renamed"]);
    let mut worker = stack.factory.open_session(session_context(14)).expect("session opens");
    assert_eq!(run_import_query(&mut worker, &captured, sql), ["[7]"]);
    // Retrying the persisted query must keep the same snapshot.
    assert_eq!(run_import_query(&mut worker, &captured, sql), ["[7]"]);
    // Uncorrelated subqueries execute during optimization.
    exec_all(&mut tk, &["begin"]);
    let sql = "import into unused_target from select (select count(*) from query_renamed)";
    let captured = capture_import_query(&mut tk, sql);
    exec_all(&mut tk, &["rollback"]);
    let mut worker = stack.factory.open_session(session_context(15)).expect("session opens");
    assert_eq!(run_import_query(&mut worker, &captured, sql), ["[8]"]);
}

const QUERY_STEP_TASK_ID: i64 = 1;

/// Go `newQueryStepExecutor`: submits the IMPORT INTO statement `sql` like
/// the executor does, and returns the executor of its query step and the
/// storage of global sort, a local directory.
fn new_query_step_executor(
    stack: &UnistoreClusterStack,
    tk: &mut Session,
    sql: &str,
) -> (Arc<dyn tidb_dxf_execute::StepExecutor>, Arc<dyn tidb_objstore::storeapi::Storage>, tempfile::TempDir) {
    let ctx = tidb_gocontext::Context::background();
    let sort_dir = tempfile::tempdir().unwrap();
    let mut plan = tk.new_import_plan_for_test(sql).unwrap();
    plan.cloud_storage_uri = sort_dir.path().to_str().unwrap().to_owned();
    plan.query_source = Some(capture_import_query(tk, sql));
    let meta = tidb_dxf_importinto::proto::TaskMeta { plan, stmt: sql.to_owned(), ..Default::default() };
    let task = tidb_dxf::Task {
        base: tidb_dxf::TaskBase {
            id: QUERY_STEP_TASK_ID,
            tp: tidb_dxf::IMPORT_INTO,
            step: tidb_dxf::IMPORT_STEP_QUERY,
            required_slots: 2,
            ..Default::default()
        },
        meta: meta.to_json(),
        ..Default::default()
    };
    let mgr = Arc::new(tidb_dxf_storage::TaskManager::new(stack.factory.dxf_session_pool()));
    let mut param = tidb_dxf_taskexecutor::Param::new_for_test(
        mgr as Arc<dyn tidb_dxf_taskexecutor::TaskTable>,
        Arc::new(tidb_dxf_taskexecutor::SlotManager::new(2)),
        *tidb_dxf::NODE_RESOURCE_FOR_TEST,
        ":4000",
    );
    // The query runs in a session of the runtime, like on a worker.
    param.task_runtime = Some(Arc::new(tidb_domain_core::domain::sqlsvr::DomainSqlServer(Arc::clone(&stack.factory))));
    let executor = tidb_dxf_importinto::task_executor::new_import_executor(&ctx, task.clone(), param);
    let step = tidb_dxf_taskexecutor::Extension::get_step_executor(&*executor, &task).unwrap();
    tidb_dxf_execute::set_framework_info(
        step.as_ref(),
        &task,
        Arc::new(tidb_dxf::StepResource { cpu: tidb_dxf::Allocatable::new(2), mem: tidb_dxf::Allocatable::new(64 << 20) }),
        None,
        None,
    );
    step.init(&ctx).unwrap();
    let sort_store = tidb_executor::importer::get_sort_store(&ctx, sort_dir.path().to_str().unwrap()).unwrap();
    (step, sort_store, sort_dir)
}

/// Go `newQuerySubtask`: a subtask of the query step, whose meta the
/// planner puts in the storage of global sort.
fn new_query_subtask(sort_store: &dyn tidb_objstore::storeapi::Storage, id: i64) -> tidb_dxf::Subtask {
    let ctx = tidb_gocontext::Context::background();
    let mut input = tidb_dxf_importinto::proto::ImportStepMeta { id: id as i32, ..Default::default() };
    input.external_path = tidb_ingestor_globalsort::plan_meta_path(
        QUERY_STEP_TASK_ID,
        &tidb_dxf::step_2_str(&tidb_dxf::IMPORT_INTO, tidb_dxf::IMPORT_STEP_QUERY),
        id as usize,
    );
    input.write_external(&ctx, sort_store).unwrap();
    tidb_dxf::Subtask {
        base: tidb_dxf::SubtaskBase { id, task_id: QUERY_STEP_TASK_ID, ..Default::default() },
        meta: input.marshal(),
        ..Default::default()
    }
}

/// The output meta of a query subtask, read back from global sort.
fn query_step_output(
    sort_store: &dyn tidb_objstore::storeapi::Storage,
    subtask: &tidb_dxf::Subtask,
) -> tidb_dxf_importinto::proto::ImportStepMeta {
    let mut output: tidb_dxf_importinto::proto::ImportStepMeta =
        tidb_dxf_importinto::proto::unmarshal(&subtask.meta).unwrap();
    assert_eq!(output.external_path, tidb_ingestor_globalsort::subtask_meta_path(QUERY_STEP_TASK_ID, subtask.base.id));
    output.read_external(&tidb_gocontext::Context::background(), sort_store).unwrap();
    output
}

/// Go `TestImportQueryStep`.
#[test]
fn test_import_query_step() {
    let (stack, _users) = cop_backed_stack();
    tidb_dxf_importinto::register();
    crate::import_query::register_import_query_runner();
    let _importer = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/dxf/importinto/createTableImporterForTest",
        "return(true)",
    );
    let ctx = tidb_gocontext::Context::background();
    let mut tk = session(&stack, 1);
    exec_all(&mut tk, &[
        "create table query_src(g bigint not null, v bigint not null, key idx_g(g))",
        "insert into query_src values (1,10),(1,20),(2,30)",
        "create table query_dst(g bigint primary key, c bigint, s decimal(42,0))",
    ]);

    // "encode the query result"
    let sql = "import into query_dst from (select /*+ STREAM_AGG() */ g, count(*), sum(v) from query_src use index(idx_g) group by g) with thread=2";
    let (step, sort_store, _dir) = new_query_step_executor(&stack, &mut tk, sql);
    let mut subtask = new_query_subtask(sort_store.as_ref(), 1);
    step.run_subtask(&ctx, &mut subtask).unwrap();
    let output = query_step_output(sort_store.as_ref(), &subtask);
    // One KV for each of the 2 groups.
    assert_eq!(output.sorted_data_meta.as_ref().unwrap().total_kv_cnt, 2);
    // The step only encodes and sorts, it does not ingest.
    assert_eq!(q(&mut tk, "select count(*) from query_dst"), [["0"]]);
    step.cleanup(&ctx).unwrap();

    // "query failure": the SELECT fails only when the worker runs it, after
    // the capture.
    exec_all(&mut tk, &[
        "create table query_bad(g bigint not null, j varchar(16))",
        "insert into query_bad values (1, 'not json')",
        "create table query_bad_dst(g bigint primary key, j json)",
    ]);
    let bad_sql = "import into query_bad_dst from (select g, cast(j as json) from query_bad) with thread=2";
    let (bad_step, bad_store, _bad_dir) = new_query_step_executor(&stack, &mut tk, bad_sql);
    let mut subtask = new_query_subtask(bad_store.as_ref(), 3);
    let input_meta = subtask.meta.clone();
    let err = bad_step.run_subtask(&ctx, &mut subtask).unwrap_err();
    assert!(err.to_string().contains("Invalid JSON text"), "{err}");
    assert_eq!(subtask.meta, input_meta);
    assert!(!bad_store.file_exists(&ctx, &tidb_ingestor_globalsort::subtask_meta_path(QUERY_STEP_TASK_ID, 3)).unwrap());
    bad_step.cleanup(&ctx).unwrap();

    // Many rows make the query send many chunks to the encoders.
    exec_all(&mut tk, &["create table query_many(v bigint not null)", "insert into query_many values (1)"]);
    for _ in 0..11 {
        exec_all(&mut tk, &["insert into query_many select v + (select count(*) from query_many) from query_many"]);
    }
    exec_all(&mut tk, &["create table query_many_dst(v bigint)", "set tidb_max_chunk_size=32"]);

    // "encode many chunks"
    let many_sql = "import into query_many_dst from (select v from query_many) with thread=2";
    let (many_step, many_store, _many_dir) = new_query_step_executor(&stack, &mut tk, many_sql);
    let mut subtask = new_query_subtask(many_store.as_ref(), 4);
    many_step.run_subtask(&ctx, &mut subtask).unwrap();
    let output = query_step_output(many_store.as_ref(), &subtask);
    let data = output.sorted_data_meta.as_ref().unwrap();
    assert_eq!(data.total_kv_cnt, 2048);
    // The rows of different chunks get different row IDs.
    let (kvs, reader) = tidb_ingestor_globalsort::read_kv_files_async(&ctx, Arc::clone(&many_store), data.get_data_files());
    let handles: std::collections::BTreeSet<String> = kvs
        .into_iter()
        .map(|kv| tidb_tablecodec::table_key::decode_row_key(&kv.key).unwrap().to_string())
        .collect();
    reader.join().unwrap().unwrap();
    assert_eq!(handles.len(), 2048);
    many_step.cleanup(&ctx).unwrap();

    // "empty query result"
    let empty_sql = "import into query_many_dst from (select v from query_many where v < 0) with thread=2";
    let (empty_step, empty_store, _empty_dir) = new_query_step_executor(&stack, &mut tk, empty_sql);
    let mut subtask = new_query_subtask(empty_store.as_ref(), 5);
    empty_step.run_subtask(&ctx, &mut subtask).unwrap();
    let output = query_step_output(empty_store.as_ref(), &subtask);
    assert_eq!(output.sorted_data_meta.as_ref().map_or(0, |m| m.total_kv_cnt), 0);
    empty_step.cleanup(&ctx).unwrap();

    // "encode failure stops the query": the encoders fail before reading any
    // chunk, so the query blocks on sending until the pipeline is canceled.
    let (fail_step, fail_store, _fail_dir) = new_query_step_executor(&stack, &mut tk, many_sql);
    let _sort = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/dxf/importinto/errorWhenSortChunk", "return(true)");
    let mut subtask = new_query_subtask(fail_store.as_ref(), 6);
    let input_meta = subtask.meta.clone();
    let (done_tx, done_rx) = crossbeam_channel::bounded(1);
    let runner = {
        let (step, ctx) = (Arc::clone(&fail_step), ctx.clone());
        std::thread::spawn(move || {
            let result = step.run_subtask(&ctx, &mut subtask).map(|()| ());
            let _ = done_tx.send(result.map_err(|e| e.to_string()));
            subtask
        })
    };
    match done_rx.recv_timeout(std::time::Duration::from_secs(10)) {
        Ok(result) => assert!(result.unwrap_err().contains("occur an error when sort chunk")),
        Err(_) => panic!("the query step does not stop after the encoders fail"),
    }
    let subtask = runner.join().unwrap();
    // A failed subtask keeps its input meta and writes no output meta.
    assert_eq!(subtask.meta, input_meta);
    assert!(!fail_store.file_exists(&ctx, &tidb_ingestor_globalsort::subtask_meta_path(QUERY_STEP_TASK_ID, 6)).unwrap());
    fail_step.cleanup(&ctx).unwrap();
    exec_all(&mut tk, &["set tidb_max_chunk_size=default"]);
}

/// Go `TestImportQueryRouting`: a classic kernel imports FROM SELECT on
/// this node (the next-gen half, which needs a next-gen build, routes it to
/// a DXF worker only during materialized-view maintenance and with a sort
/// store).
#[test]
fn test_import_query_routing() {
    let (stack, _users) = cop_backed_stack();
    // Go's testkit domain starts the DXF task manager.
    tidb_dxf_storage::set_task_manager(Some(Arc::new(tidb_dxf_storage::TaskManager::new(stack.factory.dxf_session_pool()))));
    let mut tk = session(&stack, 1);
    exec_all(&mut tk, &["create table query_route_src(id int)", "create table query_route_dst(id int)"]);
    let _setup = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockImportFromSelectSetupErr", "return(true)");
    let sql = "import into query_route_dst from select id from query_route_src with disable_precheck";
    assert!(!tidb_config_kerneltype::is_next_gen(), "the next-gen half needs a next-gen build");
    tk.vars.in_mview_maintenance = true;
    // Classic FROM SELECT runs locally.
    let err = format!("{:?}", tk.run(sql).unwrap_err());
    tk.vars.in_mview_maintenance = false;
    assert!(err.contains("mock import from select setup error"), "{err}");
}
