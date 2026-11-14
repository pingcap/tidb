//! Go `pkg/ddl/tests/adminpause/pause_cancel_test.go`.

use super::adminpause_cases as cases;
use super::*;
pub(super) use tidb_model::SchemaState;

pub(super) struct StmtCase {
    pub sql: &'static str,
    pub state: SchemaState,
    pub pausable: bool,
    pub pre: &'static [&'static str],
    pub cleanup: &'static [&'static str],
}

/// Go `pauseAndCancelStmt`, then `StmtCase.simpleRunStmt`.
fn pause_and_cancel_stmt(tk: &mut Session, admin: &Arc<Mutex<Session>>, case: &StmtCase) {
    for sql in case.pre {
        rows(tk, sql);
    }
    let paused = Arc::new(AtomicBool::new(false));
    let cancelled = Arc::new(AtomicBool::new(false));
    let job_id = Arc::new(AtomicU64::new(0));
    let (seen, command, id) = (Arc::clone(&paused), Arc::clone(admin), Arc::clone(&job_id));
    let (state, pausable) = (case.state, case.pausable);
    let pause = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep",
        move |arg| {
            let job = arg.downcast_mut::<Job>().unwrap();
            if job.schema_state == state && pausable && !seen.swap(true, Ordering::SeqCst) {
                id.store(job.id as u64, Ordering::SeqCst);
                assert_eq!(
                    displayed(rows(
                        &mut command.lock().unwrap(),
                        &format!("admin pause ddl jobs {}", job.id)
                    )),
                    [[job.id.to_string(), "successful".to_owned()]]
                );
            }
        },
    );
    let (seen, command, id) = (
        Arc::clone(&cancelled),
        Arc::clone(admin),
        Arc::clone(&job_id),
    );
    let was_paused = Arc::clone(&paused);
    let refresh = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/beforeRefreshJob",
        move |_| {
            if was_paused.load(Ordering::SeqCst) && !seen.swap(true, Ordering::SeqCst) {
                let id = id.load(Ordering::SeqCst);
                assert_eq!(
                    displayed(rows(
                        &mut command.lock().unwrap(),
                        &format!("admin cancel ddl jobs {id}")
                    )),
                    [[id.to_string(), "successful".to_owned()]]
                );
            }
        },
    );
    if case.pausable {
        assert_eq!(
            query_error(tk, case.sql).0,
            8214,
            "{} at {:?}",
            case.sql,
            case.state
        );
        assert!(paused.load(Ordering::SeqCst));
        assert!(cancelled.load(Ordering::SeqCst));
    } else {
        rows(tk, case.sql);
        assert!(!paused.load(Ordering::SeqCst));
        assert!(!cancelled.load(Ordering::SeqCst));
    }
    drop((pause, refresh));
    for sql in case.cleanup {
        let _ = tk.node_conn().execute(sql);
    }
    for sql in case.pre {
        rows(tk, sql);
    }
    rows(tk, case.sql);
    for sql in case.cleanup {
        let _ = tk.node_conn().execute(sql);
    }
}

/// Go `generateTblUser`: ten distinct rows (the Go helper randomizes strings).
fn generate_tbl_user(tk: &mut Session, vector: bool, count: usize) {
    rows(
        tk,
        if vector {
            cases::VECTOR_SQL
        } else {
            cases::TABLE_SQL
        },
    );
    for id in 1..=count {
        let table = if vector { "t_user_vec" } else { "t_user" };
        let vector = if vector { ", '[1,2,3]'" } else { "" };
        rows(tk, &format!("insert into {table} values ({id}, 'tenant{id}', 'name{id}', {id}, '', '', 'phone{id}', '2020-01-01', '2020-01-01'{vector})"));
    }
}

/// Go `prepareDomain` and each test's common case iteration.
fn run_cases(cases: &[&[StmtCase]], vector: bool, partition: bool) {
    let (stack, _users) =
        cop_backed_stack_with_config(|config| config.schema_lease = Duration::from_millis(600));
    let mut tk = stack.factory.open_session(session_context(2905)).unwrap();
    let admin = Arc::new(Mutex::new(
        stack.factory.open_session(session_context(2906)).unwrap(),
    ));
    rows(&mut tk, "use test");
    generate_tbl_user(&mut tk, false, if partition { 0 } else { 10 });
    if vector {
        // Go zzprobe/y01_primary_name TestPrimaryName: CreatePrimaryKey
        // checks the supplied name before replacing it with PRIMARY.
        assert_eq!(
            query_error(&mut tk, &format!(
                "alter table t_user add primary key {} (id)", "x".repeat(65),
            )),
            (1059, "Identifier name 'PRIMARY' is too long".to_owned()),
        );
        rows(&mut tk, "alter table t_user add primary key idx_id (id)");
        assert_eq!(
            displayed(rows(&mut tk, "select index_name from information_schema.statistics where table_schema='test' and table_name='t_user'")),
            [["PRIMARY"]],
        );
        rows(&mut tk, "alter table t_user drop primary key");
        generate_tbl_user(&mut tk, true, 10);
        rows(
            &mut tk,
            "alter table t_user_vec set tiflash replica 3 location labels 'a','b'",
        );
        tidb_domain_infosync::tiflash_manager::set_mock_tiflash(Arc::new(
            tidb_domain_infosync::tiflash_manager::MockTiFlash::new(),
        ));
    }
    if partition {
        rows(&mut tk, cases::PARTITION_SQL);
    }
    for case in cases.iter().flat_map(|cases| cases.iter()) {
        eprintln!("Go pauseAndCancelStmt: {} at {:?}", case.sql, case.state);
        pause_and_cancel_stmt(&mut tk, &admin, case);
    }
}

/// Go `TestPauseCancelAndRerunSchemaStmt`: all 14 schema/table/placement cases.
#[test]
fn test_pause_cancel_and_rerun_schema_stmt() {
    run_cases(
        &[cases::SCHEMA, cases::TABLE, cases::PLACEMENT],
        false,
        false,
    );
}

/// Go `TestPauseCancelAndRerunColumnStmt`: all 22 column cases.
#[test]
fn test_pause_cancel_and_rerun_column_stmt() {
    run_cases(&[cases::COLUMN], false, false);
}

/// Go `TestPauseCancelAndRerunIndexStmt`: all 39 index cases.
#[test]
fn test_pause_cancel_and_rerun_index_stmt() {
    let _stores = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount",
        "return(true)",
    );
    let _process = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess",
        "return(1)",
    );
    run_cases(&[cases::INDEX], true, false);
}

/// Go `TestPauseCancelAndRerunPartitionTableStmt`: all ten partition cases.
#[test]
fn test_pause_cancel_and_rerun_partition_table_stmt() {
    run_cases(&[cases::PARTITION], false, true);
}
