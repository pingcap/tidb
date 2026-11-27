// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/ingest/integration_test.go`, through Go's mock-backend boundary.
use crate::support::testkit::{TestKit, serial};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct MockBackendGuard {
    _guard: tidb_failpoint::Guard,
    previous: bool,
    writes: Arc<AtomicUsize>,
}
impl Drop for MockBackendGuard {
    fn drop(&mut self) {
        tidb_ddl_ingest::env::LIT_INITIALIZED.store(self.previous, Ordering::SeqCst);
    }
}
/// Go `ingesttestutil.InjectMockBackendCtx`.
fn inject_mock_backend_ctx() -> MockBackendGuard {
    let writes = Arc::new(AtomicUsize::new(0));
    let called = writes.clone();
    let guard = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/ingest/mockNewBackendContext",
        move |arg| {
            let arg = arg
                .downcast_mut::<tidb_ddl_ingest::backend_mgr::MockNewBackendContext>()
                .unwrap();
            let store=arg.store.downcast_ref::<tidb_txnkv::driver::tikv_store::TikvStore<tidb_unistore::mock::StorePdClient>>().unwrap().clone();
            let opener=tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener::from_capabilities(store,std::time::Duration::from_secs(30)).unwrap();
            arg.backend = Some(Arc::new(tidb_ddl_ingest::mock::MockBackendCtx::new(
                Arc::new(opener),
                std::time::Duration::from_secs(30),
                arg.checkpoint.clone(),
            )));
            called.fetch_add(1, Ordering::SeqCst);
        },
    );
    let previous = tidb_ddl_ingest::env::LIT_INITIALIZED.swap(true, Ordering::SeqCst);
    MockBackendGuard {
        _guard: guard,
        previous,
        writes,
    }
}
fn assert_last_n_ddl_use_ingest(tk: &mut TestKit, n: usize) {
    tk.must_exec("admin check table t");
    let jobs = tk.must_query(&format!("admin show ddl jobs {n}"));
    assert_eq!(jobs.rows().len(), n);
    for row in jobs.rows() {
        assert!(row[12].contains("ingest"), "{row:?}");
    }
}
/// Go `TestAddIndexIngestGeneratedColumns`, all three original table shapes.
#[test]
fn test_add_index_ingest_generated_columns() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let guard = inject_mock_backend_ctx();
    tk.must_exec(
        "create table t (a int, b int, c int as (b+10), d int as (b+c), primary key (a) clustered)",
    );
    tk.must_exec("insert into t (a, b) values (1, 1), (2, 2), (3, 3)");
    for sql in [
        "alter table t add index idx(c)",
        "alter table t add index idx1(c,a)",
        "alter table t add index idx2(a)",
        "alter table t add index idx3(d)",
        "alter table t add index idx4(d,c)",
    ] {
        tk.must_exec(sql);
    }
    tk.must_query("select * from t")
        .check(&["1 1 11 12", "2 2 12 14", "3 3 13 16"]);
    assert_last_n_ddl_use_ingest(&mut tk, 5);
    tk.must_exec("drop table t");
    tk.must_exec("create table t (a int, b char(10), c char(10) as (concat(b, 'x')), d int, e char(20) as (c))");
    tk.must_exec("insert into t (a,b,d) values (1,'1',1),(2,'2',2),(3,'3',3)");
    for sql in [
        "alter table t add index idx(c)",
        "alter table t add index idx1(a,c)",
        "alter table t add index idx2(c(7))",
        "alter table t add index idx3(e(5))",
    ] {
        tk.must_exec(sql);
    }
    tk.must_query("select * from t")
        .check(&["1 1 1x 1 1x", "2 2 2x 2 2x", "3 3 3x 3 3x"]);
    assert_last_n_ddl_use_ingest(&mut tk, 4);
    tk.must_exec("drop table t");
    tk.must_exec("create table t (a int,b char(10),c tinyint,d int as (a+c),e bigint as (d-a),primary key(b,a) clustered)");
    tk.must_exec("insert into t (a,b,c) values (1,'1',1),(2,'2',2),(3,'3',3)");
    for sql in [
        "alter table t add index idx(d)",
        "alter table t add index idx1(b(2),d)",
        "alter table t add index idx2(d,c)",
        "alter table t add index idx3(e)",
    ] {
        tk.must_exec(sql);
    }
    tk.must_query("select * from t")
        .check(&["1 1 1 2 1", "2 2 2 4 2", "3 3 3 6 3"]);
    assert_last_n_ddl_use_ingest(&mut tk, 4);
    assert!(
        guard.writes.load(Ordering::SeqCst) >= 13,
        "the ingest backend was bypassed"
    );
}
/// Go `TestAddIndexIngestTimezone`.
#[test]
fn test_add_index_ingest_timezone() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let guard = inject_mock_backend_ctx();
    for sql in [
        "set time_zone = '-06:00'",
        "create table t (src varchar(48),t timestamp,timezone varchar(100))",
        "insert into t values('2000-07-29 23:15:30','2000-07-29 23:15:30','-6:00')",
        "insert into t values('1991-07-21 00:00:00','1991-07-21 00:00:00','-6:00')",
        "alter table t add index idx(t)",
        "admin check table t",
        "alter table t drop index idx",
        "set time_zone='Asia/Shanghai'",
        "insert into t values('2000-07-29 23:15:30','2000-07-29 23:15:30','+8:00')",
        "insert into t values('1991-07-21 00:00:00','1991-07-21 00:00:00','+8:00')",
        "alter table t add index idx(t)",
        "admin check table t",
    ] {
        tk.must_exec(sql);
    }
    assert_eq!(guard.writes.load(Ordering::SeqCst), 2);
}
/// Go `TestAddIndexIngestMultiSchemaChange`.
#[test]
fn test_add_index_ingest_multi_schema_change() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let guard = inject_mock_backend_ctx();
    for sql in [
        "create table t (a int,b int)",
        "insert into t values(1,1),(2,2)",
        "alter table t add index idx(a),add index idx_2(b)",
        "admin check table t",
        "alter table t drop index idx,drop index idx_2",
        "alter table t add unique index idx(a),add unique index idx_2(b,a),add unique index idx_3(b)",
        "admin check table t",
        "drop table t",
        "create table t (a int,b int,c int as (b+10),d int as (b+c),primary key(a) clustered) partition by range(a)(partition p0 values less than(10),partition p1 values less than(20),partition p2 values less than MAXVALUE)",
    ] {
        tk.must_exec(sql);
    }
    for i in 0..30 {
        tk.must_exec(&format!("insert into t(a,b) values({i},{i})"));
    }
    tk.must_exec("alter table t add index idx_a(a),add index idx_ab(a,b),add index idx_d(d)");
    tk.must_exec("admin check table t");
    assert!(
        guard.writes.load(Ordering::SeqCst) >= 5,
        "the ingest backend was bypassed"
    );
}

/// Go `TestAddIndexIngestPanic`, both original operator failure sites.
#[test]
fn test_add_index_ingest_panic() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let backend = inject_mock_backend_ctx();
    tk.must_exec("set global tidb_enable_dist_task = 0");
    for scan in [true, false] {
        tk.must_exec("drop table if exists t");
        tk.must_exec("create table t(a int,b int,c int,d int,primary key(a) clustered)");
        tk.must_exec("insert into t(a,b,c,d) values(1,1,1,1),(2,2,2,2),(3,3,3,3)");
        let _fail = if scan {
            tidb_failpoint::enable_call_for_test(
                "github.com/pingcap/tidb/pkg/ddl/scanRecordExec",
                |_| panic!("mock panic"),
            )
        } else {
            tidb_failpoint::enable_for_test(
                "github.com/pingcap/tidb/pkg/ddl/mockLocalWriterPanic",
                "return",
            )
        };
        let code = tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_REORG_PANIC.gen_with_stack_by_args(&[]),
        )
        .code;
        tk.must_get_err_code("alter table t add index idx(b)", code);
    }
    assert!(backend.writes.load(Ordering::SeqCst) >= 2);
}

/// Go `TestIngestPartitionRowCount`.
#[test]
fn test_ingest_partition_row_count() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let backend = inject_mock_backend_ctx();
    tk.must_exec("create table t(a int,b int,c int as(b+10),d int as(b+c),primary key(a) clustered) partition by range(a)(partition p0 values less than(1),partition p1 values less than(2),partition p2 values less than MAXVALUE)");
    tk.must_exec("insert into t(a,b) values(0,0),(1,1),(2,2)");
    tk.must_exec("alter table t add index idx(d)");
    let jobs = tk.must_query("admin show ddl jobs 1");
    assert_eq!(jobs.rows().len(), 1);
    assert_eq!(jobs.rows()[0][7], "3");
    tk.must_exec("admin check table t");
    assert_eq!(backend.writes.load(Ordering::SeqCst), 3);
}

/// Go `env_test.go:TestLitBackendCtxMgr`: `ingest.CleanUpTempDir` keeps the
/// directories of jobs still in `mysql.tidb_ddl_job`, processing or not, and
/// removes the rest (Go's `filterProcessingJobIDs` on the session).
#[test]
fn test_lit_backend_ctx_mgr() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let sort_path = tempfile::tempdir().unwrap();
    let stale_job_dir = sort_path.path().join("100");
    let stale_job_dir2 = sort_path.path().join("101");
    std::fs::create_dir_all(&stale_job_dir).unwrap();
    std::fs::create_dir_all(&stale_job_dir2).unwrap();
    // Go stops the DDL first; these rows carry no job, so they sit at ids no
    // real job reaches and are deleted before the test ends.
    tk.must_exec("insert into mysql.tidb_ddl_job (job_id, processing) values (100, true)");
    tk.must_exec("insert into mysql.tidb_ddl_job (job_id, processing) values (101, false)");
    let mut clean_up = |tk: &mut TestKit, path: &std::path::Path| {
        tidb_ddl_ingest::env::clean_up_temp_dir(path, |job_ids| {
            Ok(tk
                .must_query(&tidb_ddl_ingest::env::filter_processing_job_ids_sql(
                    job_ids,
                ))
                .rows()
                .iter()
                .map(|row| row[0].parse().unwrap())
                .collect())
        })
    };
    clean_up(&mut tk, sort_path.path());
    assert!(stale_job_dir.is_dir());
    assert!(stale_job_dir2.is_dir());
    tk.must_exec("delete from mysql.tidb_ddl_job where job_id = 101");
    clean_up(&mut tk, sort_path.path());
    assert!(stale_job_dir.is_dir());
    assert!(!stale_job_dir2.exists());
    tk.must_exec("delete from mysql.tidb_ddl_job where job_id = 100");
    clean_up(&mut tk, sort_path.path());
    assert!(!stale_job_dir.exists());
    assert!(!stale_job_dir2.exists());
    clean_up(&mut tk, std::path::Path::new("unknown_path"));
}

/// Go `ingest.LitDiskRoot.Count()`: the jobs whose local sort still holds disk.
fn lit_disk_root_count() -> usize {
    tidb_ddl_ingest::env::lightning_env().map_or(0, |env| env.disk_root.count())
}

/// Go `TestIngestError`. Go's first failpoint, `mockCopSenderError`, no longer
/// has an injection site, so that half is a plain ingest ADD INDEX.
#[test]
fn test_ingest_error() {
    let _serial = serial();
    let mut tk = TestKit::new();
    tk.must_exec("set global tidb_enable_dist_task = 0");
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("set @@tidb_ddl_reorg_worker_cnt = 1;");
    for failpoint in [
        "github.com/pingcap/tidb/pkg/ddl/mockCopSenderError",
        "github.com/pingcap/tidb/pkg/ddl/mockLocalWriterError",
    ] {
        tk.must_exec("drop table if exists t;");
        tk.must_exec("create table t (a int primary key, b int);");
        for i in 0..4 {
            tk.must_exec(&format!("insert into t values ({}, {});", i * 10000, i * 10000));
        }
        tk.must_query("split table t between (0) and (50000) regions 5;").check(["4 1"]);
        let fp = crate::support::testkit::enable_failpoint(failpoint, "1*return");
        tk.must_exec("alter table t add index idx(a);");
        drop(fp);
        tk.must_exec("admin check table t;");
        let rows = tk.must_query("admin show ddl jobs 1;");
        let job_tp = &rows.rows()[0][12];
        assert!(job_tp.contains("ingest"), "{job_tp}");
    }
}

/// Go `TestAddIndexIngestCancel`: cancelled while its backfill runs.
#[test]
fn test_add_index_ingest_cancel() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("create table t (a int, b int);");
    tk.must_exec("insert into t (a, b) values (1, 1), (2, 2), (3, 3);");
    let tk2 = std::sync::Mutex::new(tk.new_tk());
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = cancelled.clone();
    let hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep",
        move |arg| {
            if seen.load(Ordering::SeqCst) {
                return;
            }
            let job = arg.downcast_mut::<tidb_model::Job>().unwrap();
            if job.type_ == tidb_model::ActionType::ACTION_ADD_INDEX
                && job.schema_state == tidb_model::SchemaState::WRITE_REORGANIZATION
            {
                let mut tk2 = tk2.lock().unwrap();
                let tidb_infoschema::TableEntry::Kv(table) = tk2.get_table_by_name("test", "t") else {
                    return;
                };
                let running = table.meta().read().indices.iter_deref().any(|index| {
                    let index = index.read();
                    index.name.lowercase() == "idx"
                        && index.backfill_state == tidb_model::BackfillState::RUNNING
                });
                if running {
                    tk2.must_exec(&format!("admin cancel ddl jobs {}", job.id));
                    seen.store(true, Ordering::SeqCst);
                }
            }
        },
    );
    tk.must_get_err_code("alter table t add index idx(b);", 8214);
    assert!(cancelled.load(Ordering::SeqCst));
    drop(hook);
    assert_eq!(lit_disk_root_count(), 0);
}

/// Go `TestAddIndexGetChunkCancel`: cancelled before the scan reads a chunk.
#[test]
fn test_add_index_get_chunk_cancel() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("create table t (a int primary key, b int);");
    for i in 0..100 {
        tk.must_exec(&format!("insert into t values ({}, {});", i * 10000, i * 10000));
    }
    tk.must_exec("split table t between (0) and (1000000) regions 10;");
    let job_id = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let recorded = job_id.clone();
    let _job = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep",
        move |arg| {
            let job = arg.downcast_mut::<tidb_model::Job>().unwrap();
            if recorded.load(Ordering::SeqCst) == 0 && job.type_ == tidb_model::ActionType::ACTION_ADD_INDEX {
                recorded.store(job.id, Ordering::SeqCst);
            }
        },
    );
    let tk2 = std::sync::Mutex::new(tk.new_tk());
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = cancelled.clone();
    let _chunk = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/beforeGetChunk",
        move |_| {
            if !seen.swap(true, Ordering::SeqCst) {
                tk2.lock()
                    .unwrap()
                    .must_exec(&format!("admin cancel ddl jobs {}", job_id.load(Ordering::SeqCst)));
            }
        },
    );
    tk.must_get_err_code("alter table t add index idx(b);", 8214);
    assert!(cancelled.load(Ordering::SeqCst));
    tk.must_exec("admin check table t;");
}


/// Go `TestAddIndexCancelOnNoneState`.
#[test]
fn test_add_index_cancel_on_none_state() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let tk_cancel = std::sync::Mutex::new(tk.new_tk());
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("create table t (c1 int, c2 int, c3 int)");
    tk.must_exec("insert into t values(1, 1, 1);");
    let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let _hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep",
        move |arg| {
            let job = arg.downcast_mut::<tidb_model::Job>().unwrap();
            if job.schema_state == tidb_model::SchemaState::NONE && first.swap(false, Ordering::SeqCst) {
                assert!(tk_cancel
                    .lock()
                    .unwrap()
                    .exec(&format!("admin cancel ddl jobs {}", job.id))
                    .is_ok());
            }
        },
    );
    tk.must_get_err_code("alter table t add index idx1(c1)", 8214);
    assert_eq!(lit_disk_root_count(), 0);
}


/// Go `TestAddIndexValidateRangesFailed`: two invalid region splits, then a
/// valid one, with `loadTableRanges` not retrying.
#[test]
fn test_add_index_validate_ranges_failed() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("create table t (a int primary key, b int);");
    tk.must_exec("insert into t values (1, 1);");
    let _no_retry = crate::support::testkit::enable_failpoint(
        "github.com/pingcap/tidb/pkg/ddl/loadTableRangesNoRetry",
        "return",
    );
    let _err = crate::support::testkit::enable_failpoint(
        "github.com/pingcap/tidb/pkg/ddl/validateAndFillRangesErr",
        "2*return",
    );
    tk.must_exec("alter table t add index idx(b);");
    tk.must_exec("admin check table t;");
}

/// The three reorg modes of Go's modify-column ingest cases.
const MODIFY_COLUMN_CASES: [(&str, &str, &str); 3] =
    [("txn", "off", "off"), ("local ingest", "off", "on"), ("dxf ingest", "on", "on")];

/// Go `TestModifyColumnWithMultipleIndex`.
#[test]
fn test_modify_column_with_multiple_index() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    let create_table_sql = "CREATE TABLE t (
		a int(11) DEFAULT NULL,
		b varchar(10) DEFAULT NULL,
		c decimal(10,2) DEFAULT NULL,
		KEY idx1 (a),
		UNIQUE KEY idx2 (a),
		KEY idx3 (a,b),
		KEY idx4 (a,b,c)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";
    for (case_name, enable_dist_task, enable_fast_reorg) in MODIFY_COLUMN_CASES {
        tk.must_exec(&format!("set global tidb_enable_dist_task = {enable_dist_task};"));
        tk.must_exec(&format!("set global tidb_ddl_enable_fast_reorg = {enable_fast_reorg};"));
        tk.must_exec("DROP TABLE IF EXISTS t");
        tk.must_exec(create_table_sql);
        tk.must_exec("insert into t values(19,1,1),(17,2,2)");
        tk.must_exec("admin check table t;");
        if let Some(error) = tk.exec_to_err("alter table t modify a bit(5) not null") {
            panic!("{case_name}: {error}");
        }
        tk.must_exec("admin check table t;");
    }
    tk.must_exec("set global tidb_ddl_enable_fast_reorg = on;");
}

/// Go `TestModifyColumnWithIndexWithDefaultValue`.
#[test]
fn test_modify_column_with_index_with_default_value() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    for (case_name, enable_dist_task, enable_fast_reorg) in MODIFY_COLUMN_CASES {
        tk.must_exec(&format!("set global tidb_enable_dist_task = {enable_dist_task};"));
        tk.must_exec(&format!("set global tidb_ddl_enable_fast_reorg = {enable_fast_reorg};"));
        for sql in [
            "drop table if exists t1",
            "create table t1 (c int(10), c1 datetime default (date_format(now(),'%Y-%m-%d')));",
            "insert into t1(c) values (1), (2);",
            "alter table t1 add index idx(c1);",
            "insert into t1 values (3, default);",
            "alter table t1 modify column c1 varchar(30) default 'xx';",
            "alter table t1 modify column c1 datetime DEFAULT (date_format(now(), '%Y-%m-%d'));",
            "insert into t1 values (5, default);",
            "alter table t1 drop index idx;",
        ] {
            if let Some(error) = tk.exec_to_err(sql) {
                panic!("{case_name}: {sql}: {error}");
            }
        }
    }
    tk.must_exec("set global tidb_ddl_enable_fast_reorg = on;");
}

/// Go `TestMultiSchemaAddIndexMerge`: DML lands during a multi-schema ADD
/// INDEX backfill and the merged indexes stay consistent.
#[test]
fn test_multi_schema_add_index_merge() {
    struct ResetHook;
    impl Drop for ResetHook {
        fn drop(&mut self) {
            *tidb_ddl_ingest::mock::MOCK_EXEC_AFTER_WRITE_ROW.lock().unwrap() = None;
        }
    }
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    let tk2 = Arc::new(std::sync::Mutex::new(tk.new_tk()));
    tk2.lock().unwrap().must_exec("use test");
    let _reset = ResetHook;
    for create_table_sql in [
        "create table t (a int, b int);",
        "create table t (a int, b int) PARTITION BY HASH (`a`) PARTITIONS 4;",
    ] {
        tk.must_exec("drop table if exists t;");
        tk.must_exec(create_table_sql);
        tk.must_exec("insert into t values (1, 1), (2, 2), (3, 3);");
        let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let tk2_err = Arc::new(std::sync::Mutex::new(None));
        let (hook_first, hook_err, hook_tk2) = (first.clone(), tk2_err.clone(), tk2.clone());
        *tidb_ddl_ingest::mock::MOCK_EXEC_AFTER_WRITE_ROW.lock().unwrap() = Some(Box::new(move || {
            if !hook_first.swap(false, Ordering::SeqCst) {
                return;
            }
            *hook_err.lock().unwrap() =
                hook_tk2.lock().unwrap().exec("insert into t values (4, 4), (5, 5);").err();
        }));
        tk.must_exec("alter table t add index idx1(a), add index idx2(b);");
        assert!(!first.load(Ordering::SeqCst));
        assert!(tk2_err.lock().unwrap().is_none());
        tk.must_exec("admin check table t;");
    }
}

/// Go `TestCheckpointInstanceAddrValidation`: the checkpoint's instance is
/// `AdvertiseAddress:Port:TempDir`.
#[test]
fn test_checkpoint_instance_addr_validation() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("set global tidb_enable_dist_task = 0;");
    tk.must_exec("create table t (a int primary key, b int);");
    for i in 0..10 {
        tk.must_exec(&format!("insert into t values ({i}, {i});"));
    }
    let cfg = tidb_config::config_tree::config::get_global_config();
    let instance_addr = tidb_ddl_ingest::env::instance_addr();
    assert!(!instance_addr.is_empty());
    assert!(!cfg.temp_dir.is_empty());
    let temp_dir_suffix = format!(":{}", cfg.temp_dir);
    let dsn = instance_addr
        .strip_suffix(&temp_dir_suffix)
        .unwrap_or_else(|| panic!("instance addr should end with temp dir, got: {instance_addr}"));
    let (host, port) = dsn.rsplit_once(':').expect("host:port");
    assert_eq!(port, cfg.port.to_string());
    if cfg.advertise_address.is_empty() {
        assert_ne!(host, "0.0.0.0", "instance addr should not use default host");
    } else {
        assert_eq!(host.trim_matches(['[', ']']), cfg.advertise_address);
    }
    let exercised = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = exercised.clone();
    let _hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/ingest/afterMockWriterWriteRow",
        move |_| seen.store(true, Ordering::SeqCst),
    );
    tk.must_exec("alter table t add index idx(b);");
    assert!(exercised.load(Ordering::SeqCst), "checkpoint mechanism should have been exercised during add index");
    tk.must_exec("admin check table t;");
}

/// Go `TestIndexChangeWithModifyColumn`: a MODIFY COLUMN started while ADD
/// INDEX is in its none state refuses the collation change on an indexed column.
#[test]
fn test_index_change_with_modify_column() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("create table t (b int, c varchar(100) collate utf8mb4_unicode_ci)");
    tk.must_exec("insert t values (1, 'aa'), (2, 'bb'), (3, 'cc');");
    let tkddl = Arc::new(std::sync::Mutex::new(tk.new_tk()));
    tkddl.lock().unwrap().must_exec("use test");
    let check_err = Arc::new(std::sync::Mutex::new(None));
    let handle = Arc::new(std::sync::Mutex::new(None));
    let run_modify_column = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (hook_err, hook_handle, hook_tk) = (check_err.clone(), handle.clone(), tkddl.clone());
    let _hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep",
        move |arg| {
            let job = arg.downcast_mut::<tidb_model::Job>().unwrap();
            if job.schema_state != tidb_model::SchemaState::NONE
                || run_modify_column.swap(true, Ordering::SeqCst)
            {
                return;
            }
            let (err, tkddl) = (hook_err.clone(), hook_tk.clone());
            *hook_handle.lock().unwrap() = Some(std::thread::spawn(move || {
                *err.lock().unwrap() = tkddl
                    .lock()
                    .unwrap()
                    .exec("alter table t modify column c varchar(120) default 'aaaaa' collate utf8mb4_general_ci first;")
                    .err();
            }));
        },
    );
    tk.must_exec("alter table t add index idx(c);");
    handle.lock().unwrap().take().expect("the modify column ran").join().unwrap();
    // Go master 307fb3d6ead: the indexed collation change reorganizes.
    if let Some(error) = check_err.lock().unwrap().take() {
        panic!("the modify column fails: {error}");
    }
    tk.must_query("select c from t force index (idx) where c = 'AA'").check(["aa"]);
    tk.must_exec("admin check table t");
    tk.must_exec("delete from t;");
}

/// Go `TestAddGlobalIndexInIngestWithUpdate`: rows inserted and moved while
/// a global unique index is added stay consistent with the table.
#[test]
fn test_add_global_index_in_ingest_with_update() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (a int, b int) partition by hash(a) partitions 5");
    tk.must_exec("insert into t (a, b) values (1, 1), (2, 2), (3, 3)");
    let i = Arc::new(std::sync::atomic::AtomicI32::new(3));
    let tk2 = std::sync::Mutex::new(tk.new_tk());
    let _hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced",
        move |arg| {
            let job = arg.downcast_mut::<tidb_model::Job>().unwrap();
            if job.state != tidb_model::JobState::SYNCED {
                let mut tk2 = tk2.lock().unwrap();
                let tmp = i.fetch_add(1, Ordering::SeqCst) + 1;
                assert!(tk2.exec(&format!("insert into test.t values ({tmp}, {tmp})")).is_ok());
                assert!(tk2
                    .exec(&format!("update test.t set b = b + 20, a = b where b = {}", tmp - 1))
                    .is_ok());
            }
        },
    );
    tk.must_exec("alter table t add unique index idx(b) global");
    let rs_global_index = tk.must_query("select *,_tidb_rowid from t use index(idx)").sort();
    let rs_table = tk.must_query("select *,_tidb_rowid from t use index()").sort();
    assert_eq!(rs_global_index.rows(), rs_table.rows());
}

/// Go `TestAddIndexIngestPartitionCheckpoint`: a re-delivered job resumes at
/// the checkpointed partition, writing each row once.
#[test]
fn test_add_index_ingest_partition_checkpoint() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("set global tidb_enable_dist_task = off;");
    tk.must_exec("create table t (a int primary key, b int) partition by hash(a) partitions 4;");
    for i in 0..20 {
        tk.must_exec(&format!("insert into t values ({i}, {i})"));
    }
    let job_id = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let recorded = job_id.clone();
    let _delivery = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/beforeDeliveryJob",
        move |arg| {
            recorded.store(arg.downcast_mut::<tidb_model::Job>().unwrap().id, Ordering::SeqCst);
        },
    );
    let row_cnt = Arc::new(AtomicUsize::new(0));
    let counted = row_cnt.clone();
    let tk2 = std::sync::Mutex::new(tk.new_tk());
    let _write = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/ingest/onMockWriterWriteRow",
        move |_| {
            if counted.fetch_add(1, Ordering::SeqCst) + 1 == 10 {
                let mut tk2 = tk2.lock().unwrap();
                tk2.must_exec("use test");
                let job_id = job_id.load(Ordering::SeqCst);
                tk2.must_exec(&format!("update mysql.tidb_ddl_job set processing = 0 where job_id = {job_id}"));
                tk2.must_exec(&format!("update mysql.tidb_ddl_job set processing = 1 where job_id = {job_id}"));
            }
        },
    );
    tk.must_exec("alter table t add index idx(b);");
    // It should resume to correct partition.
    assert_eq!(row_cnt.load(Ordering::SeqCst), 20);
    tk.must_exec("admin check table t;");
}


/// Go `TestAddIndexIngestJobWriteConflict`: a write conflict on the job row
/// after the backfill must not rerun the whole backfill.
#[test]
fn test_add_index_ingest_job_write_conflict() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("create table t (a int primary key, b int);");
    tk.must_exec("insert into t values (1, 1), (2, 2), (3, 3);");
    tk.must_exec("set global tidb_enable_dist_task = off;");
    let injected = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = injected.clone();
    let tk2 = std::sync::Mutex::new(tk.new_tk());
    let _hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/afterRunIngestReorgJob",
        move |arg| {
            let (job, done) = arg.downcast_mut::<(tidb_model::Job, bool)>().unwrap();
            if *done && !seen.load(Ordering::SeqCst) {
                let mut tk2 = tk2.lock().unwrap();
                tk2.must_exec("use test");
                // Simulate write-conflict on ddl job table.
                tk2.must_exec(&format!("update mysql.tidb_ddl_job set processing = 0 where job_id = {}", job.id));
                tk2.must_exec(&format!("update mysql.tidb_ddl_job set processing = 1 where job_id = {}", job.id));
                seen.store(true, Ordering::SeqCst);
            }
        },
    );
    let row_cnt = Arc::new(AtomicUsize::new(0));
    let counted = row_cnt.clone();
    let _rows = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/ingest/onMockWriterWriteRow",
        move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
        },
    );
    tk.must_exec("alter table t add index idx(b);");
    assert!(injected.load(Ordering::SeqCst));
    // Write conflict error should not retry the whole job.
    assert_eq!(row_cnt.load(Ordering::SeqCst), 3); // it should not be 6
    tk.must_exec("admin check table t;");
}

/// Go `TestCheckpointPhysicalIDValidation`: the ingest checkpoint persisted in
/// `mysql.tidb_ddl_reorg` names a real partition.
#[test]
fn test_checkpoint_physical_id_validation() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("set global tidb_enable_dist_task = 0;");
    tk.must_exec("create table t (
		a int primary key,
		b int
	) partition by hash(a) partitions 4;");
    for i in 0..20 {
        tk.must_exec(&format!("insert into t values ({i}, {i});"));
    }
    let partition_rows = tk.must_query(
        "select TIDB_PARTITION_ID from information_schema.partitions where table_schema='test' and table_name='t';",
    );
    assert_eq!(partition_rows.rows().len(), 4);
    let valid_part_ids: std::collections::HashSet<i64> =
        partition_rows.rows().iter().map(|row| row[0].parse().unwrap()).collect();
    let observed = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let seen = observed.clone();
    let tk2 = std::sync::Mutex::new(tk.new_tk());
    let _hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/ingest/onMockWriterWriteRow",
        move |_| {
            if seen.load(Ordering::SeqCst) != 0 {
                return;
            }
            let rows = tk2
                .lock()
                .unwrap()
                .must_query("select reorg_meta from mysql.tidb_ddl_reorg where ele_type = '_idx_' limit 1;");
            let Some(raw) = rows.rows().first().map(|row| row[0].clone()) else {
                return;
            };
            let Ok(meta) = serde_json::from_str::<tidb_ddl_ingest::checkpoint::JobReorgMeta>(&raw) else {
                return;
            };
            if let Some(checkpoint) = meta.reorg_checkpoint {
                if checkpoint.physical_id > 0 {
                    seen.store(checkpoint.physical_id, Ordering::SeqCst);
                }
            }
        },
    );
    tk.must_exec("alter table t add index idx(b);");
    tk.must_exec("admin check table t;");
    let physical_id = observed.load(Ordering::SeqCst);
    assert_ne!(physical_id, 0, "should have observed physical_id in checkpoint");
    assert!(valid_part_ids.contains(&physical_id), "physical_id {physical_id} should be a valid partition ID");
}

/// Go `TestAddGlobalIndexInIngest`: rows written while each chunk is
/// ingested land in normal and global indexes alike.
#[test]
fn test_add_global_index_in_ingest() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (a int, b int) partition by hash(a) partitions 5");
    tk.must_exec("insert into t (a, b) values (1, 1), (2, 2), (3, 3)");
    let i = Arc::new(std::sync::atomic::AtomicI32::new(3));
    let tk2 = std::sync::Mutex::new(tk.new_tk());
    let _hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/writeLocalExec",
        move |_| {
            let tmp = i.fetch_add(1, Ordering::SeqCst) + 1;
            assert!(tk2.lock().unwrap().exec(&format!("insert into test.t values ({tmp}, {tmp})")).is_ok());
        },
    );
    let mut check = |tk: &mut TestKit, alter: &str, first: &str, second: &str| -> usize {
        tk.must_exec(alter);
        let rs_first = tk.must_query(&format!("select * from t use index({first})")).sort();
        let rs_table = tk.must_query("select * from t use index()").sort();
        let rs_second = tk.must_query(&format!("select * from t use index({second})")).sort();
        assert_eq!(rs_first.rows(), rs_table.rows(), "{alter}");
        assert_eq!(rs_first.rows(), rs_second.rows(), "{alter}");
        rs_first.rows().len()
    };
    let num = check(&mut tk, "alter table t add index idx_1(b), add unique index idx_2(b) global", "idx_2", "idx_1");
    assert!(num > 3);
    // for indexes have different columns
    let global = check(&mut tk, "alter table t add index idx_3(a), add unique index idx_4(b) global", "idx_4", "idx_3");
    assert!(global > num);
    // for all global indexes
    let all_global = check(&mut tk, "alter table t add unique index idx_5(b) global, add unique index idx_6(b) global", "idx_6", "idx_5");
    assert!(all_global > global);
    // for non-unique global indexes
    let non_unique = check(&mut tk, "alter table t add index idx_7(b) global, add index idx_8(b) global", "idx_7", "idx_8");
    assert!(non_unique > global);
}

/// Go `TestAddIndexDuplicateMessage`: a duplicate written by DML while the
/// unique index backfills fails it with Go's 1062 text.
#[test]
fn test_add_index_duplicate_message() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("create table t(id int primary key, b int, k int);");
    tk.must_exec("insert into t values (1, 1, 1);");
    let tk1 = std::sync::Mutex::new(tk.new_tk());
    tk1.lock().unwrap().must_exec("use test");
    let err_dml = Arc::new(std::sync::Mutex::new(None));
    let recorded = err_dml.clone();
    let once = std::sync::Once::new();
    let _hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/ingest/afterMockWriterWriteRow",
        move |_| {
            once.call_once(|| {
                *recorded.lock().unwrap() =
                    Some(tk1.lock().unwrap().exec("insert into t values (2, 1, 2);").err());
            });
        },
    );
    tk.must_get_err_msg(
        "alter table t add unique index idx(b);",
        "[kv:1062]Duplicate entry '1' for key 't.idx'",
    );
    assert_eq!(err_dml.lock().unwrap().take(), Some(None));
    tk.must_exec("admin check table t;");
    tk.must_query("select * from t;").check(["1 1 1", "2 1 2"]);
}

/// Go `TestAddIndexIngestClientError`.
#[test]
fn test_add_index_ingest_client_error() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("CREATE TABLE t1 (f1 json);");
    tk.must_exec(r#"insert into t1(f1) values (cast("null" as json));"#);
    tk.must_get_err_code("create index i1 on t1((cast(f1 as unsigned array)));", 3903);
}

/// Go `TestAddIndexWithEmptyPartitions`: after each partition the next one,
/// empty or not, is what `mysql.tidb_ddl_reorg` records.
#[test]
fn test_add_index_with_empty_partitions() {
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("set global tidb_enable_dist_task = 0;");
    // Create partitioned table with empty partitions (p1, p3 are empty)
    tk.must_exec("create table t (
		a int primary key,
		b int
	) partition by range(a) (
		partition p0 values less than (100),
		partition p1 values less than (200),
		partition p2 values less than (300),
		partition p3 values less than (400)
	);");
    // Only insert into p0 and p2, leaving p1 and p3 empty
    for i in (0..10).chain(200..210) {
        tk.must_exec(&format!("insert into t values ({i}, {i});"));
    }
    let partition_rows = tk.must_query(
        "select TIDB_PARTITION_ID from information_schema.partitions where table_schema='test' and table_name='t';",
    );
    assert_eq!(partition_rows.rows().len(), 4);
    let all_part_ids: std::collections::HashSet<i64> =
        partition_rows.rows().iter().map(|row| row[0].parse().unwrap()).collect();
    let observed_ids = Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));
    let observed = observed_ids.clone();
    let tk2 = std::sync::Mutex::new(tk.new_tk());
    let _hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/afterUpdatePartitionReorgInfo",
        move |arg| {
            let job = arg.downcast_mut::<tidb_model::Job>().unwrap();
            if job.type_ != tidb_model::ActionType::ACTION_ADD_INDEX {
                return;
            }
            // The NEXT partition to process.
            let rows = tk2.lock().unwrap().must_query(&format!(
                "select physical_id from mysql.tidb_ddl_reorg where job_id = {} limit 1;",
                job.id
            ));
            if let Some(pid) = rows.rows().first().and_then(|row| row[0].parse::<i64>().ok()) {
                if pid != 0 {
                    observed.lock().unwrap().push(pid);
                }
            }
        },
    );
    tk.must_exec("alter table t add index idx(b);");
    tk.must_exec("admin check table t;");
    let rs1 = tk.must_query("select count(*) from t use index(idx);");
    let rs2 = tk.must_query("select count(*) from t ignore index(idx);");
    assert_eq!(rs1.rows(), rs2.rows());
    assert_eq!(rs1.rows()[0][0], "20");
    let observed_ids = observed_ids.lock().unwrap();
    assert!(observed_ids.len() >= 3, "should observe at least 3 partition switches for 4 partitions: {observed_ids:?}");
    for pid in observed_ids.iter() {
        assert!(all_part_ids.contains(pid), "physical_id {pid} should be a valid partition ID");
    }
}

/// Go `TestAddIndexSetInternalSessions`: the ingest scan's snapshot is a
/// transaction of a pooled internal session, listed by the session manager.
#[test]
fn test_add_index_set_internal_sessions() {
    use tidb_executor::show::exec::ShowExec as _;
    let _serial = serial();
    let mut tk = TestKit::new();
    let _backend = inject_mock_backend_ctx();
    tk.must_exec("set global tidb_enable_dist_task = 0;");
    tk.must_exec("set @@tidb_ddl_reorg_worker_cnt = 1;");
    tk.must_exec("create table t (a int);");
    tk.must_exec("insert into t values (1);");
    let expect_internal_ts = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let actual_internal_ts = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let expected = expect_internal_ts.clone();
    let _start = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/wrapInBeginRollbackStartTS",
        move |arg| expected.lock().unwrap().push(*arg.downcast_mut::<u64>().unwrap()),
    );
    let registry = tk.session().process_registry().expect("the session manager");
    let actual = actual_internal_ts.clone();
    let _scan = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/scanRecordExec",
        move |_| {
            *actual.lock().unwrap() = registry
                .internal_session_start_ts(|id| tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.contains(id));
        },
    );
    tk.must_exec("alter table t add index idx(a);");
    let expect_internal_ts = expect_internal_ts.lock().unwrap();
    let actual_internal_ts = actual_internal_ts.lock().unwrap();
    assert_eq!(expect_internal_ts.len(), 1);
    for ts in expect_internal_ts.iter() {
        assert!(actual_internal_ts.contains(ts), "{ts} not in {actual_internal_ts:?}");
    }
}

/// Go `initJobReorgMetaFromVariables`: DXF without fast reorganization is
/// refused (`ErrUnsupportedDistTask`), as realtikv
/// `TestAddIndexInvalidDistTaskVariableSetting` checks on TiKV.
#[test]
fn test_dist_task_without_fast_reorg_is_refused() {
    let _serial = serial();
    let mut tk = TestKit::new();
    tk.must_exec("set global tidb_ddl_enable_fast_reorg = off;");
    tk.must_exec("set global tidb_enable_dist_task = on;");
    tk.must_exec("create table t (a int);");
    tk.must_get_err_code("alter table t add index idx(a);", 8200);
    tk.must_get_err_code("alter table t add column b int, add index idx(a);", 8200);
    tk.must_exec("alter table t add column b int, add column c int;");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg = on;");
    tk.must_exec("set global tidb_enable_dist_task = off;");
}
