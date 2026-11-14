// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `executor.CreateTableWithInfo`, `BatchCreateTableWithInfo` and
//! `CreatePlacementPolicyWithInfo`: BR restore's entry points, called with
//! prebuilt infos rather than statements (`pkg/ddl/db_table_test.go`,
//! `pkg/ddl/placement_policy_test.go`).

use super::*;
use tidb_ast::CiString;
use tidb_ddl::{CreateTableConfig, OnExist};
use tidb_model::TableInfo;

fn table(id: i64, name: &str) -> TableInfo {
    TableInfo {
        id,
        name: CiString::new(name),
        ..TableInfo::default()
    }
}

fn config(on_exist: OnExist, id_allocated: bool) -> CreateTableConfig {
    CreateTableConfig {
        on_exist,
        id_allocated,
    }
}

/// Go `TestCreateTableWithInfo`: an allocated ID is kept; otherwise the job
/// draws one past the global ID counter.
#[test]
fn test_create_table_with_info() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2170)).unwrap();
    rows(&mut session, "use test");
    let context = tidb_sessionctx::StmtContext::for_query();
    stack
        .factory
        .ddl
        .batch_create_table_with_info(
            "test",
            vec![table(42042, "t")],
            config(OnExist::Error, true),
            &context,
        )
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "select tidb_table_id from information_schema.tables where table_name = 't'"
        )),
        vec![vec!["42042".to_owned()]]
    );
    let mut id = 0;
    stack
        .factory
        .meta_store
        .get()
        .unwrap()
        .run_in_new_txn(
            &tidb_gocontext::Context::background(),
            tidb_txnkv::INTERNAL_TXN_OTHERS,
            true,
            &mut |txn| {
                id = tidb_meta::transaction::Mutator::new(txn).gen_global_id()?;
                Ok(())
            },
        )
        .unwrap();
    stack
        .factory
        .ddl
        .batch_create_table_with_info(
            "test",
            vec![table(42, "tt")],
            config(OnExist::Error, false),
            &context,
        )
        .unwrap();
    let generated: i64 = displayed(rows(
        &mut session,
        "select tidb_table_id from information_schema.tables where table_name = 'tt'",
    ))[0][0]
        .parse()
        .unwrap();
    assert!(generated > id, "{generated} > {id}");
}

/// Go `TestBatchCreateTable`: one `create tables` job, Go's duplicate-name
/// refusal, and a prebuilt view.
#[test]
fn test_batch_create_table() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2171)).unwrap();
    rows(&mut session, "use test");
    let context = tidb_sessionctx::StmtContext::for_query();
    let mut infos = vec![
        table(0, "tables_1"),
        table(0, "tables_2"),
        table(0, "tables_3"),
    ];
    stack
        .factory
        .ddl
        .batch_create_table_with_info(
            "test",
            infos.clone(),
            config(OnExist::Error, false),
            &context,
        )
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "show tables like '%tables_%'")),
        vec![
            vec!["tables_1".to_owned()],
            vec!["tables_2".to_owned()],
            vec!["tables_3".to_owned()]
        ]
    );
    let job = displayed(rows(&mut session, "admin show ddl jobs")).remove(0);
    assert_eq!(job[1], "test");
    assert_eq!(job[2], "tables_1,tables_2,tables_3");
    assert_eq!(job[3], "create tables");
    assert_eq!(job[4], "public");

    // Duplicated name.
    infos[1].name = CiString::new("tables_1");
    let error = stack
        .factory
        .ddl
        .batch_create_table_with_info("test", infos, config(OnExist::Error, false), &context)
        .unwrap_err();
    assert_eq!(
        error.identity,
        Some(tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS.identity()),
        "{error:?}"
    );

    let names = ["c1", "c2"];
    let mut view = table(0, "tables_4");
    view.columns = names
        .iter()
        .enumerate()
        .map(|(offset, name)| tidb_model::ColumnInfo {
            name: CiString::new(*name),
            offset: offset as i64,
            state: tidb_model::SchemaState::PUBLIC,
            ..tidb_model::ColumnInfo::default()
        })
        .collect::<Vec<_>>()
        .into();
    view.view = Some(tidb_model::GoShared::new(tidb_model::ViewInfo {
        cols: names
            .iter()
            .map(|name| CiString::new(*name))
            .collect::<Vec<_>>()
            .into(),
        security: tidb_ast::ViewSecurity::DEFINER,
        algorithm: tidb_ast::ViewAlgorithm::MERGE,
        select_stmt: "SELECT c1,c2,1 FROM t".to_owned(),
        check_option: tidb_ast::ViewCheckOption::CASCADED,
        definer: Some(Box::new(tidb_parser::auth::UserIdentity {
            current_user: true,
            ..Default::default()
        })),
    }));
    stack
        .factory
        .ddl
        .batch_create_table_with_info("test", vec![view], config(OnExist::Error, false), &context)
        .unwrap();
}

fn policy(stack: &UnistoreClusterStack, name: &str) -> Option<tidb_model::PolicyInfo> {
    stack
        .factory
        .catalog_snapshot()
        .policies()
        .find(|policy| policy.name.lowercase() == name)
        .cloned()
}

/// Go `checkPolicyEquals`.
fn check_policy_equals(expected: &tidb_model::PolicyInfo, actual: &tidb_model::PolicyInfo) {
    assert_eq!(expected.id, actual.id);
    assert_eq!(expected.name, actual.name);
    assert_eq!(
        expected
            .placement_settings
            .as_ref()
            .map(|settings| settings.read().clone()),
        actual
            .placement_settings
            .as_ref()
            .map(|settings| settings.read().clone())
    );
    assert_eq!(expected.state, actual.state);
}

/// Go `TestCreatePlacementPolicyWithInfo` without its PD bundle checks: a new
/// name gets a fresh ID under every `OnExist`; an existing name errors,
/// is ignored, or is replaced keeping its ID.
#[test]
fn test_create_placement_policy_with_info() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2172)).unwrap();
    rows(&mut session, "use test");
    rows(
        &mut session,
        "create placement policy p LEARNERS=1 LEARNER_CONSTRAINTS=\"[+region=cn-west-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+disk=ssd]\"",
    );
    let context = tidb_sessionctx::StmtContext::for_query();
    let old = policy(&stack, "p").unwrap();
    let with_settings = |name: &str, followers: u64, learner_constraints: &str| {
        let mut policy = old.clone();
        policy.name = CiString::new(name);
        let mut settings = old.placement_settings.as_ref().unwrap().read().clone();
        settings.followers = followers;
        settings.learner_constraints = learner_constraints.to_owned();
        policy.placement_settings = Some(tidb_model::GoShared::new(settings));
        policy
    };

    // Create a policy that does not exist.
    for on_exist in [OnExist::Replace, OnExist::Ignore, OnExist::Error] {
        let mut created = with_settings("p2", 2, "[+zone=z2]");
        stack
            .factory
            .ddl
            .create_placement_policy_with_info(created.clone(), on_exist, &context)
            .unwrap();
        check_policy_equals(&old, &policy(&stack, "p").unwrap());
        let found = policy(&stack, "p2").unwrap();
        // The ID of the created policy is reassigned.
        assert_ne!(created.id, found.id);
        created.id = found.id;
        check_policy_equals(&created, &found);
        rows(&mut session, "drop placement policy if exists p2");
    }

    // The same name with OnExistError.
    let mut same = old.clone();
    same.id = old.id + 1;
    let error = stack
        .factory
        .ddl
        .create_placement_policy_with_info(same.clone(), OnExist::Error, &context)
        .unwrap_err();
    assert_eq!(
        error.identity,
        Some(tidb_infoschema::infoschema_errors::ERR_PLACEMENT_POLICY_EXISTS.identity()),
        "{error:?}"
    );
    check_policy_equals(&old, &policy(&stack, "p").unwrap());

    // The same name with OnExistIgnore.
    stack
        .factory
        .ddl
        .create_placement_policy_with_info(same, OnExist::Ignore, &context)
        .unwrap();
    check_policy_equals(&old, &policy(&stack, "p").unwrap());

    // The same name with OnExistReplace keeps the old policy's ID.
    let mut replaced = with_settings("p", 1, "[+zone=z1]");
    replaced.id = old.id + 1;
    stack
        .factory
        .ddl
        .create_placement_policy_with_info(replaced.clone(), OnExist::Replace, &context)
        .unwrap();
    replaced.id = old.id;
    check_policy_equals(&replaced, &policy(&stack, "p").unwrap());
}

/// Go `TestCreateTableHandleAutoIDOnce`: an ordinary CREATE TABLE rebases
/// its auto IDs once, in the job.
#[test]
fn test_create_table_handle_auto_id_once() {
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let _count =
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/handleAutoIncID", {
            let count = count.clone();
            move |_| {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        });
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2173)).unwrap();
    rows(&mut session, "use test");
    count.store(0, std::sync::atomic::Ordering::SeqCst);
    rows(&mut session, "create table t1(id int) AUTO_INCREMENT 1000");
    // For normal DDL, rebase is called only once.
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        displayed(rows(&mut session, "show table test.t1 next_row_id"))[0][3],
        "1000"
    );
}

/// Go `TestCreateTableWithBR`: under `StartMode::Br` the executor rebases
/// again after the job, which leaves the counter where the job put it.
#[test]
fn test_create_table_with_br() {
    let _br = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockBRStartMode",
        "return(true)",
    );
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let _count =
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/handleAutoIncID", {
            let count = count.clone();
            move |_| {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        });
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2174)).unwrap();
    rows(&mut session, "use test");
    let mut info = table(42043, "t1");
    info.columns = vec![tidb_model::ColumnInfo {
        id: 1,
        name: CiString::new("id"),
        offset: 0,
        state: tidb_model::SchemaState::PUBLIC,
        field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        ..tidb_model::ColumnInfo::default()
    }]
    .into();
    info.state = tidb_model::SchemaState::PUBLIC;
    info.auto_inc_id = 1000;
    let involving_ref = [tidb_model::InvolvingSchemaInfo {
        database: "test".into(),
        table: "t1".into(),
        mode: tidb_model::InvolvingSchemaInfoMode::SHARED,
        ..tidb_model::InvolvingSchemaInfo::default()
    }];
    count.store(0, std::sync::atomic::Ordering::SeqCst);
    stack
        .factory
        .ddl
        .create_table_with_info(
            "test",
            info,
            &involving_ref,
            config(OnExist::Error, false),
            &tidb_sessionctx::StmtContext::for_query(),
        )
        .unwrap();
    // For BR execution, rebase is called twice, without changing the result.
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(
        displayed(rows(&mut session, "show table test.t1 next_row_id"))[0][3],
        "1000"
    );
}

/// Go `CreateTable`'s `involvingRef`: CREATE TABLE ... LIKE involves its
/// source, shared, behind the new table; a plain CREATE TABLE involves
/// nothing extra (Go capture via `./gorun`).
#[test]
fn create_table_like_involves_its_source_shared_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2175)).unwrap();
    rows(&mut session, "use test");
    let involving = |session: &mut Session| {
        displayed(rows(
            session,
            "select json_extract(convert(job_meta using utf8mb4), \"$.involving_schema_info\") from mysql.tidb_ddl_history order by job_id desc limit 1",
        ))[0][0]
            .clone()
    };
    rows(&mut session, "create table t1(a int)");
    rows(&mut session, "create table t2 like t1");
    assert_eq!(
        involving(&mut session),
        r#"[{"database": "test", "table": "t2"}, {"database": "test", "mode": 1, "table": "t1"}]"#
    );
    rows(&mut session, "create table t3(a int)");
    assert_eq!(involving(&mut session), "NULL");
}
