// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! A session's reaction to DDL on the real in-process cluster: the two
//! catalogs that have to catch up after a DDL runs -- the domain's at once,
//! a connection's at its next statement, and never inside an explicit
//! transaction -- and the DDL statement's own transaction boundary.

use super::*;

fn schema_version(stack: &UnistoreClusterStack) -> i64 {
    stack.factory.catalog_snapshot().schema_meta_version()
}

/// The cop-backed stack with `test.t (id, v)` and `test.g (id, grp)`, and a
/// root session using `test`.
fn stack_with_tables(connection_id: u64) -> (UnistoreClusterStack, Arc<ConfiguredUserStore>, Session) {
    let (stack, users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(connection_id)).unwrap();
    rows(&mut session, "use test");
    rows(&mut session, "create table t (id bigint primary key, v bigint)");
    rows(&mut session, "create table g (id bigint primary key, grp bigint)");
    (stack, users, session)
}

fn root_session(stack: &UnistoreClusterStack, connection_id: u64) -> Session {
    let mut session = stack.factory.open_session(session_context(connection_id)).unwrap();
    rows(&mut session, "use test");
    session
}

/// A new table runs as one schema version, and the connection that created
/// it writes and reads it at once -- which it can only do if its own catalog
/// was rebuilt -- while its older tables survive the rebuild.
#[test]
fn create_table_runs_and_the_same_connection_uses_the_new_table() {
    let (stack, _users, mut session) = stack_with_tables(2300);
    let before = schema_version(&stack);
    let outcome = session
        .node_conn()
        .execute_write("CREATE TABLE fresh (id BIGINT PRIMARY KEY, v BIGINT)")
        .expect("the DDL runs")
        .expect("a DDL answers with an OK packet");
    assert_eq!(outcome.affected_rows, 0);
    assert_eq!(schema_version(&stack), before + 1);
    rows(&mut session, "INSERT INTO fresh (id, v) VALUES (1, 10), (2, 20)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT id, v FROM fresh ORDER BY id")),
        [["1", "10"], ["2", "20"]]
    );
    assert!(rows(&mut session, "SELECT id FROM t").is_empty());
}

/// `DROP TABLE` removes the table from the connection's own catalog too, so
/// the next statement naming it is an unknown table (1146).
#[test]
fn drop_table_removes_it_from_the_connections_own_catalog() {
    let (stack, _users, mut session) = stack_with_tables(2301);
    rows(&mut session, "INSERT INTO t (id, v) VALUES (1, 10)");
    let before = schema_version(&stack);
    rows(&mut session, "DROP TABLE t");
    assert!(schema_version(&stack) > before);
    let Err(error) = session.node_conn().execute("SELECT id FROM t") else {
        panic!("a dropped table must not answer");
    };
    assert_eq!(error.code, 1146, "{}", error.message);
    assert!(rows(&mut session, "SELECT id FROM g").is_empty());
}

/// `ALTER TABLE ... RENAME TO` is Go's two-step job (`onRenameTable`, then
/// `finishJobRenameTable`, each a schema version); the rows stay under the
/// stable table ID and the next statement resolves only the new name.
#[test]
fn alter_table_rename_rebuilds_the_connection_catalog_without_losing_rows() {
    let (stack, _users, mut session) = stack_with_tables(2302);
    rows(&mut session, "INSERT INTO t (id, v) VALUES (1, 10)");
    let before = schema_version(&stack);
    rows(&mut session, "ALTER TABLE t RENAME TO renamed");
    assert_eq!(schema_version(&stack), before + 2);
    assert!(session.node_conn().execute("SELECT id FROM t").is_err());
    assert_eq!(displayed(rows(&mut session, "SELECT id, v FROM renamed")), [["1", "10"]]);
}

/// Go runs every pair of one `RENAME TABLE` as one `ActionRenameTables` job:
/// the same two schema versions as a single rename (`onRenameTables`, then
/// `finishJobRenameTables`), every renamed table exposed together.
#[test]
fn rename_table_multiple_pairs_publish_one_catalog_change() {
    let (stack, _users, mut session) = stack_with_tables(2303);
    rows(&mut session, "INSERT INTO t (id, v) VALUES (1, 10)");
    rows(&mut session, "INSERT INTO g (id, grp) VALUES (2, 20)");
    let before = schema_version(&stack);
    rows(&mut session, "RENAME TABLE t TO t_renamed, g TO g_renamed");
    assert_eq!(schema_version(&stack), before + 2);
    assert!(session.node_conn().execute("SELECT id FROM t").is_err());
    assert!(session.node_conn().execute("SELECT id FROM g").is_err());
    assert_eq!(displayed(rows(&mut session, "SELECT id, v FROM t_renamed")), [["1", "10"]]);
    assert_eq!(displayed(rows(&mut session, "SELECT id, grp FROM g_renamed")), [["2", "20"]]);
}

/// A later pair's occupied target refuses the whole `RENAME TABLE` job (1050)
/// before anything is published: every earlier pair keeps its old name.
#[test]
fn rename_table_later_pair_failure_leaves_every_prior_pair_unpublished() {
    let (stack, _users, mut session) = stack_with_tables(2304);
    rows(&mut session, "INSERT INTO t (id, v) VALUES (1, 10)");
    rows(&mut session, "INSERT INTO g (id, grp) VALUES (2, 20)");
    let before = schema_version(&stack);
    let error = session
        .node_conn()
        .execute_write("RENAME TABLE t TO t_renamed, g TO t_renamed")
        .expect_err("the occupied second target refuses the job");
    assert_eq!(error.code, 1050, "{}", error.message);
    assert_eq!(schema_version(&stack), before);
    assert_eq!(displayed(rows(&mut session, "SELECT id, v FROM t")), [["1", "10"]]);
    assert_eq!(displayed(rows(&mut session, "SELECT id, grp FROM g")), [["2", "20"]]);
    assert!(session.node_conn().execute("SELECT id FROM t_renamed").is_err());
}

/// A connection opened before the DDL notices it at its next statement.
#[test]
fn a_second_connection_sees_the_new_table_after_the_ddl() {
    let (stack, _users, mut author) = stack_with_tables(2305);
    let mut peer = root_session(&stack, 2306);
    assert!(rows(&mut peer, "SELECT id FROM t").is_empty());
    rows(&mut author, "CREATE TABLE shared (id BIGINT PRIMARY KEY, v BIGINT)");
    rows(&mut author, "INSERT INTO shared (id, v) VALUES (5, 50)");
    assert_eq!(displayed(rows(&mut peer, "SELECT id, v FROM shared")), [["5", "50"]]);
}

/// `CREATE DATABASE` keeps its charset and collation, a table created in it
/// inherits them, `USE` reaches it, and `DROP DATABASE` takes its tables.
#[test]
fn create_and_drop_database_follow_the_ddl() {
    let (stack, _users, mut session) = stack_with_tables(2307);
    rows(&mut session, "CREATE DATABASE extra CHARACTER SET utf8 COLLATE utf8_general_ci");
    rows(&mut session, "USE extra");
    rows(&mut session, "CREATE TABLE here (id BIGINT PRIMARY KEY)");
    assert!(rows(&mut session, "SELECT id FROM here").is_empty());
    let catalog = stack.factory.catalog_snapshot();
    let database = catalog.schema_by_name("extra").expect("the created database is published");
    assert_eq!(database.read().charset, "utf8");
    assert_eq!(database.read().collate, "utf8_general_ci");
    let (_, here) = catalog.table_info_by_name("extra", "here").expect("extra.here");
    assert_eq!(here.read().charset, "utf8");
    assert_eq!(here.read().collate, "utf8_general_ci");
    drop(catalog);
    rows(&mut session, "DROP DATABASE extra");
    assert!(session.node_conn().execute("SELECT id FROM extra.here").is_err());
}

/// `IF NOT EXISTS` on a table that exists queues no job, publishes no schema
/// version, and answers OK with Go's 1050 note.
#[test]
fn an_already_satisfied_ddl_publishes_nothing() {
    let (stack, _users, mut session) = stack_with_tables(2308);
    let before = schema_version(&stack);
    let outcome = session
        .node_conn()
        .execute_write("CREATE TABLE IF NOT EXISTS t (id BIGINT PRIMARY KEY)")
        .expect("an IF NOT EXISTS no-op succeeds")
        .expect("it answers with an OK packet");
    assert_eq!(outcome.affected_rows, 0);
    assert_eq!(schema_version(&stack), before);
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [["Note", "1050", "Table 'test.t' already exists"]]
    );
}

/// DDL commits an open transaction first, as MySQL and Go both do: the staged
/// writes are published, and the transaction is over when the DDL runs.
#[test]
fn a_ddl_implicitly_commits_the_open_transaction() {
    let (stack, _users, mut session) = stack_with_tables(2309);
    let mut peer = root_session(&stack, 2310);
    session.node_conn().control_transaction("BEGIN").expect("begin");
    rows(&mut session, "INSERT INTO t (id, v) VALUES (1, 10)");
    assert!(rows(&mut peer, "SELECT id FROM t").is_empty());
    rows(&mut session, "CREATE TABLE after_ddl (id BIGINT PRIMARY KEY)");
    assert!(!session.in_transaction(), "the implicit commit ends the transaction");
    assert_eq!(
        displayed(rows(&mut peer, "SELECT id, v FROM t")),
        [["1", "10"]],
        "the DDL committed the open transaction"
    );
    assert!(rows(&mut session, "SELECT id FROM after_ddl").is_empty());
}

/// Inside an explicit transaction the connection keeps the schema its
/// `BEGIN` saw: a peer's DROP TABLE waits on the transaction's metadata lock
/// (Go MDL), the transaction keeps reading the table, and once it commits
/// the drop finishes and the connection follows the domain again.
#[test]
fn a_transaction_keeps_the_schema_its_begin_saw() {
    let (stack, _users, mut reader) = stack_with_tables(2311);
    let mut author = root_session(&stack, 2312);
    reader.node_conn().control_transaction("BEGIN").expect("begin");
    assert!(rows(&mut reader, "SELECT id FROM t").is_empty());
    let dropper = std::thread::spawn(move || {
        rows(&mut author, "DROP TABLE t");
        author
    });
    // Give the drop time to reach its metadata-lock wait.
    std::thread::sleep(std::time::Duration::from_millis(500));
    assert!(
        rows(&mut reader, "SELECT id FROM t").is_empty(),
        "a statement inside BEGIN must keep the schema BEGIN saw"
    );
    reader.node_conn().control_transaction("COMMIT").expect("commit");
    drop(dropper.join().expect("the drop finishes once the transaction ends"));
    let Err(error) = reader.node_conn().execute("SELECT id FROM t") else {
        panic!("after COMMIT the connection follows the domain");
    };
    assert_eq!(error.code, 1146, "{}", error.message);
}

/// DDL privilege admission comes before the implicit commit and before the
/// job is queued; a skip-grant-table identity bypasses the grant lookup.
#[test]
fn ddl_privileges_are_checked_before_the_implicit_commit_and_skip_grant_bypasses_them() {
    let (stack, users, mut root) = stack_with_tables(2313);
    rows(&mut root, "create user 'ddl_low'@'%' identified by 'abc'");
    rows(&mut root, "grant create temporary tables on test.* to 'ddl_low'@'%'");
    let identity = users
        .authenticate_native(
            "ddl_low",
            "127.0.0.1",
            &super::super::node_fixture::SALT,
            &super::super::node_fixture::scramble(b"abc", &super::super::node_fixture::SALT),
        )
        .expect("ddl_low logs in");
    let mut low = stack
        .factory
        .open_session(super::super::node_fixture::session_context_with_identity(2314, identity))
        .unwrap();
    rows(&mut low, "use test");
    low.node_conn().control_transaction("BEGIN").expect("begin");
    let before = schema_version(&stack);
    let denied = low
        .node_conn()
        .execute_write("CREATE TABLE blocked (id BIGINT PRIMARY KEY)")
        .expect_err("CREATE is checked before the DDL runs");
    assert_eq!((denied.code, denied.state), (1142, *b"42000"));
    assert_eq!(
        denied.message,
        "CREATE command denied to user 'ddl_low'@'%' for table 'blocked'"
    );
    assert!(low.in_transaction(), "a denied DDL must not implicitly commit");
    low.node_conn().control_transaction("ROLLBACK").expect("rollback");
    for (sql, database) in [
        ("CREATE DATABASE blocked_db", "blocked_db"),
        ("DROP DATABASE test", "test"),
    ] {
        let denied = low.node_conn().execute_write(sql).expect_err(sql);
        assert_eq!((denied.code, denied.state), (1044, *b"42000"), "{sql}");
        assert_eq!(
            denied.message,
            format!("Access denied for user 'ddl_low'@'%' to database '{database}'"),
            "{sql}"
        );
    }
    assert_eq!(schema_version(&stack), before);

    let bypass_identity = (*users)
        .clone()
        .with_skip_grant_table(true)
        .authenticate_native("ghost", "127.0.0.1", &super::super::node_fixture::SALT, b"ignored")
        .expect("skip-grant-table admits an identity without an account row");
    let mut bypass = stack
        .factory
        .open_session(super::super::node_fixture::session_context_with_identity(2315, bypass_identity))
        .unwrap();
    rows(&mut bypass, "CREATE TABLE test.allowed (id BIGINT PRIMARY KEY)");
    assert!(schema_version(&stack) > before);
}

/// With `enable-table-lock` on, LOCK TABLES is a cluster lock job: the lock
/// is stored on the table (`onLockTables`), another connection meets it as
/// `ErrTableLocked` (8020), UNLOCK and ADMIN CLEANUP TABLE LOCK release it,
/// and a closing connection releases what it holds (`session.Close`).
#[test]
fn lock_tables_run_as_a_cluster_lock_job() {
    let previous = tidb_config::config_tree::config::get_global_config();
    tidb_config::config_tree::config::update_global(|config| config.enable_table_lock = true);
    let (stack, _users, mut first) = stack_with_tables(2316);
    let mut second = root_session(&stack, 2317);
    let stored_lock = || {
        stack
            .factory
            .catalog_snapshot()
            .table_info_by_name("test", "t")
            .and_then(|(_, table)| table.read().lock.as_ref().map(|lock| lock.read().clone()))
    };
    rows(&mut first, "LOCK TABLES t WRITE");
    let lock = stored_lock().expect("the lock is stored on the table");
    assert_eq!(lock.tp, tidb_ast::TableLockType::WRITE);
    assert_eq!(lock.state, tidb_model::TableLockState::PUBLIC);
    assert_eq!(
        lock.sessions.iter().map(|session| session.session_id).collect::<Vec<_>>(),
        [2316]
    );
    let Err(error) = second.node_conn().execute("SELECT id FROM test.t") else {
        panic!("another connection must meet the WRITE lock");
    };
    assert_eq!(error.code, 8020, "{}", error.message);
    rows(&mut first, "INSERT INTO t (id, v) VALUES (1, 10)");
    rows(&mut first, "UNLOCK TABLES");
    assert!(stored_lock().is_none());
    rows(&mut second, "SELECT id FROM test.t");

    rows(&mut first, "LOCK TABLES t WRITE");
    rows(&mut second, "ADMIN CLEANUP TABLE LOCK test.t");
    assert!(stored_lock().is_none());
    rows(&mut first, "UNLOCK TABLES");

    rows(&mut first, "LOCK TABLES t READ");
    assert!(stored_lock().is_some());
    drop(first);
    assert!(
        stored_lock().is_none(),
        "a closing connection releases its locks through the cluster job"
    );
    tidb_config::config_tree::config::store_global_config((*previous).clone());
}

/// Go keeps a table object across schema versions whose changes did not touch
/// it (`Builder.copySchemaTables` in V1, the shared `Data` table cache in V2):
/// another schema's DDL leaves `other.kept` the same object.
#[test]
fn a_new_version_keeps_the_tables_of_untouched_schemas() {
    let (stack, _users, mut session) = stack_with_tables(2318);
    for sql in ["CREATE DATABASE other", "CREATE TABLE other.kept (id BIGINT PRIMARY KEY)"] {
        rows(&mut session, sql);
    }
    let kept = |catalog: &tidb_infoschema::SessionExtendedInfoSchema| {
        catalog.table_info_by_name("other", "kept").expect("other.kept").1
    };
    let before = stack.factory.catalog_snapshot();
    rows(&mut session, "CREATE DATABASE third");
    let after = stack.factory.catalog_snapshot();
    assert!(after.schema_meta_version() > before.schema_meta_version());
    assert!(kept(&before).ptr_eq(&kept(&after)));
}

/// Go refuses CHECK PARTITION and the STATS_PERSISTENT option inside
/// `executor.AlterTable`'s spec walk (`ddl/executor.go:1767`, `:1952`),
/// after `DDLExec.Next`'s `NewTxnInStmt` (`executor/ddl.go:151`) committed
/// the user's transaction.
#[test]
fn an_executor_time_alter_refusal_commits_the_open_transaction_first() {
    let (stack, _users, mut session) = stack_with_tables(2319);
    let mut peer = root_session(&stack, 2320);
    for (id, sql, message) in [
        (1, "ALTER TABLE t CHECK PARTITION p0", "Unsupported check partition"),
        (
            2,
            "ALTER TABLE t STATS_PERSISTENT = DEFAULT",
            "This type of ALTER TABLE is currently unsupported",
        ),
    ] {
        session.node_conn().control_transaction("BEGIN").unwrap();
        rows(&mut session, &format!("INSERT INTO t (id, v) VALUES ({id}, 10)"));
        let refused = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!((refused.code, refused.message.as_str()), (8200, message));
        assert_eq!(
            displayed(rows(&mut peer, "SELECT count(*) FROM t")),
            [[id.to_string()]],
            "{sql} commits the transaction first"
        );
        assert!(!session.in_transaction(), "{sql}");
    }
}

/// A coded DEFAULT refusal keeps its errno, SQLSTATE and source message: Go
/// refuses the DEFAULT in the DDL executor (`BuildTableInfoWithStmt` via
/// `executor.CreateTable`), after `DDLExec.Next`'s implicit commit and before
/// any job is queued.
#[test]
fn create_default_errors_reach_the_client_unchanged() {
    let (stack, _users, mut session) = stack_with_tables(2321);
    let before = schema_version(&stack);
    for (sql, code, state, message) in [
        (
            "CREATE TABLE bad_function (a INT DEFAULT (ABS(1)))",
            3770,
            *b"HY000",
            "Default value expression of column 'a' contains a disallowed function: `abs`.",
        ),
        (
            "CREATE TABLE bad_fsp (ts TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP)",
            1067,
            *b"42000",
            "Invalid default value for 'ts'",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).expect_err(sql);
        assert_eq!((error.code, error.state), (code, state), "{sql}");
        assert_eq!(error.message, message, "{sql}");
    }
    assert_eq!(schema_version(&stack), before);
}
