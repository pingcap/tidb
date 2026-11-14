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
