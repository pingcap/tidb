//! Go `pkg/ddl` delivery tests over the embedded store and real SQL sessions.

use super::*;
use std::sync::Mutex;
use tidb_model::{Job, JobState, JobW, SchemaState};

/// Go `TestCreateTables` (`pkg/ddl/table_test.go`): a three-table job
/// survives the first GetJobByID error and finishes all three tables.
#[test]
fn test_create_tables() {
    use tidb_model::{
        BatchCreateTableArgs, CreateTableArgs, GoField, GoShared, HistoryInfo, JobArgsValue,
        TableInfo,
    };
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(2900)).unwrap();
    rows(&mut tk, "create database test_table");
    let schema_id = stack
        .factory
        .catalog
        .get_latest()
        .schema_by_name("test_table")
        .unwrap()
        .read()
        .id;
    let args = GoShared::new(BatchCreateTableArgs {
        tables: GoField::new(
            (1..=3)
                .map(|i| CreateTableArgs {
                    table_info: GoField::new(Some(GoShared::new(TableInfo {
                        name: tidb_ast::CiString::new(format!("s{i}")),
                        ..Default::default()
                    }))),
                    ..Default::default()
                })
                .collect::<Vec<_>>()
                .into(),
        ),
    });
    let mut job = Job::default();
    job.schema_id = schema_id;
    job.schema_name = "test_table".into();
    job.type_ = tidb_model::ActionType::ACTION_CREATE_TABLES;
    job.version = tidb_model::JobVersion::V2;
    job.query = "skip".into();
    job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
    let mut spec = tidb_ddl_jobsubmit::JobSpec {
        job,
        args: JobArgsValue::BatchCreateTable(Some(args)),
        id_allocated: false,
    };
    let reads = Arc::new(AtomicU64::new(0));
    let seen = Arc::clone(&reads);
    let _failure = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockGetJobByIDFail",
        move |arg| {
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                *arg.downcast_mut::<Option<tidb_ddl_systable::SystemTableManagerError>>()
                    .unwrap() = Some(tidb_ddl_systable::SystemTableManagerError::Sql(
                    "mock get job by ID failed".to_owned(),
                ));
            }
        },
    );
    let pool = crate::ddl_session_pool::NodeDdlSessionPool::default();
    pool.bind(&stack.factory);
    tidb_ddl::real_tikv_ddl::gen_gid_and_insert_jobs_with_retry(
        Arc::new(stack.opener.clone()),
        &pool,
        std::slice::from_mut(&mut spec),
        Duration::from_secs(20),
        |_| Option::<fn()>::None,
    )
    .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let jobs = rows(
            &mut tk,
            &format!(
                "select job_meta from mysql.tidb_ddl_history where job_id = {}",
                spec.job.id
            ),
        );
        if let Some(row) = jobs.first() {
            let mut job = Job::default();
            job.decode(row[0].go_bytes()).unwrap();
            assert!(job.error.is_none(), "{job:?}");
            assert_eq!(job.state, JobState::SYNCED);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "batch job did not finish"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        reads.load(Ordering::SeqCst) >= 2,
        "GetJobByID was not retried"
    );
    let catalog = tidb_infoschema_issyncer::real_tikv_catalog::load_catalog_from_cluster(
        &stack.opener,
        Duration::from_secs(20),
    )
    .unwrap();
    for name in ["s1", "s2", "s3"] {
        assert!(
            catalog.table_info_by_name("test_table", name).is_some(),
            "missing {name}"
        );
    }
}

mod adminpause;
mod adminpause_cases;

/// Go `TestCancelJobBeforeRun` (`pkg/ddl/cancel_test.go`).
#[test]
fn test_cancel_job_before_run() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(2901)).unwrap();
    rows(&mut tk, "use test");
    rows(&mut tk, "create table t (c1 int, c2 int, c3 int)");
    rows(&mut tk, "insert into t values (1, 1, 1)");
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel = Mutex::new(stack.factory.open_session(session_context(2902)).unwrap());
    let seen = Arc::clone(&cancelled);
    let _hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/beforeTransitOneJobStep",
        move |arg| {
            let job_w = arg.downcast_mut::<JobW>().unwrap();
            let job = job_w.job.as_ref().unwrap().read();
            if job.table_name.to_string() == "t" && !seen.swap(true, Ordering::SeqCst) {
                rows(
                    &mut cancel.lock().unwrap(),
                    &format!("admin cancel ddl jobs {}", job.id),
                );
            }
        },
    );
    assert_eq!(query_error(&mut tk, "truncate table t").0, 8214);
    assert!(cancelled.load(Ordering::SeqCst));
    assert_eq!(
        displayed(rows(&mut tk, "select * from t")),
        [["1", "1", "1"]]
    );
}

/// Go `TestUpgradingRelatedJobState` (`pkg/ddl/job_scheduler_testkit_test.go`).
#[test]
fn test_upgrading_related_job_state() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(2903)).unwrap();
    rows(&mut tk, "use test");
    rows(&mut tk, "create table e2 (id int not null)");
    let _upgrading = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/serverstate/mockUpgradingState",
        "return(true)",
    );
    for (sql, state, error) in [
        ("alter table e2 add index idx(id)", JobState::DONE, false),
        (
            "alter table e2 add index idx1(id)",
            JobState::CANCELLING,
            true,
        ),
        (
            "alter table e2 add index idx2(id)",
            JobState::ROLLINGBACK,
            true,
        ),
        (
            "alter table e2 add index idx3(id)",
            JobState::ROLLBACK_DONE,
            true,
        ),
    ] {
        let cancel = Mutex::new(stack.factory.open_session(session_context(2904)).unwrap());
        let syncer = stack.factory.ddl.state_syncer().unwrap();
        let seen = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&seen);
        let hook_syncer = Arc::clone(&syncer);
        let _refresh = tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/ddl/beforeRefreshJob",
            move |arg| {
                let job = arg.downcast_mut::<Job>().unwrap();
                if job.query.to_string() != sql {
                    return;
                }
                observed.store(true, Ordering::SeqCst);
                if error && job.schema_state == SchemaState::WRITE_ONLY {
                    rows(
                        &mut cancel.lock().unwrap(),
                        &format!("admin cancel ddl jobs {}", job.id),
                    );
                }
                if job.state == state {
                    hook_syncer
                        .update_global_state(
                            &tidb_timeutil::SleepContext::background(),
                            &tidb_ddl_serverstate::StateInfo {
                                state: tidb_ddl_serverstate::STATE_UPGRADING.to_owned(),
                            },
                        )
                        .unwrap();
                }
            },
        );
        if error {
            assert_eq!(
                query_error(&mut tk, sql),
                (8214, "Cancelled DDL job".to_owned())
            );
        } else {
            rows(&mut tk, sql);
        }
        assert!(
            seen.load(Ordering::SeqCst),
            "refresh never observed {state:?}"
        );
        syncer
            .update_global_state(
                &tidb_timeutil::SleepContext::background(),
                &tidb_ddl_serverstate::StateInfo {
                    state: tidb_ddl_serverstate::STATE_NORMAL_RUNNING.to_owned(),
                },
            )
            .unwrap();
    }
}
