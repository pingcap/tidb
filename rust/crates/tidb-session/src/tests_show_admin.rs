//! The `SHOW` family's ADMIN-adjacent arms, split out of `tests_show` when
//! that file passed the repository's 2200-line ceiling.
//!
//! These cover the statements a client or dump tool issues that Go answers
//! from `SimpleExec`/`ShowExec` without touching a user table, plus the
//! column-metadata readers that must agree with each other.

use crate::tests_support::*;
use crate::*;

/// The statements a MySQL client or dump tool issues that Go answers from
/// `SimpleExec`/`ShowExec` without touching any table.
///
/// `SHOW PLUGINS` is Go's `fetchShowPlugins` over `plugin.GetAll()` and
/// `SHOW PROFILES` is its literal `// empty result` arm, so both answer
/// their column list and no rows. `SHOW MASTER STATUS` reports the pseudo
/// binlog file and the statement transaction's start timestamp, the
/// `TxnCtx.StartTS` Go's `buildShow` activates; a session with no server
/// activator installed has none to activate. The activated case is the
/// cluster node's `show_master_status_reports_the_activated_start_ts`.
#[test]
fn plugins_profiles_and_master_status() {
    let mut session = Session::new();

    let plugins = session.run_with_columns("SHOW PLUGINS").expect("plugins");
    let StmtOutput::Rows { columns, rows } = plugins else {
        panic!("SHOW PLUGINS answers a result set");
    };
    assert_eq!(
        columns
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        ["Name", "Status", "Type", "Library", "License", "Version"]
    );
    assert!(rows.is_empty(), "no plugin framework runs here");

    let profiles = session.run_with_columns("SHOW PROFILES").expect("profiles");
    let StmtOutput::Rows { columns, rows } = profiles else {
        panic!("SHOW PROFILES answers a result set");
    };
    assert_eq!(
        columns
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        ["Query_ID", "Duration", "Query"]
    );
    assert!(rows.is_empty(), "SHOW PROFILES is deprecated and empty");

    let status = session
        .run_with_columns("SHOW MASTER STATUS")
        .expect("master status");
    let StmtOutput::Rows { columns, rows } = status else {
        panic!("SHOW MASTER STATUS answers a result set");
    };
    assert_eq!(
        columns
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        [
            "File",
            "Position",
            "Binlog_Do_DB",
            "Binlog_Ignore_DB",
            "Executed_Gtid_Set"
        ]
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        row_text(Ok(StmtResult::Rows(rows)))[0][0],
        "tidb-binlog".to_owned()
    );
}

/// Go `SimpleExec.executeFlush`. Every target it accepts answers with an OK
/// packet and no rows; the two it does not keep Go's own diagnostics.
#[test]
fn flush_targets_follow_gos_switch() {
    let mut session = Session::new();

    for sql in [
        "FLUSH PRIVILEGES",
        "FLUSH STATUS",
        "FLUSH TABLES",
        "FLUSH HOSTS",
        "FLUSH CLIENT_ERRORS_SUMMARY",
    ] {
        // An OK packet, whichever output shape the tier gives it.
        assert!(
            matches!(
                session.run(sql),
                Ok(StmtResult::Done(_) | StmtResult::Affected(0))
            ),
            "`{sql}` is accepted"
        );
    }

    // Go returns this as a plain error -- the double space is its own.
    let error = session
        .run("FLUSH TABLES WITH READ LOCK")
        .expect_err("read lock is refused");
    assert!(
        error
            .to_string()
            .contains("FLUSH TABLES WITH READ LOCK is not supported.  Please use @@tidb_snapshot"),
        "{error}"
    );

    // Go `plugin.NotifyFlush` fails for a name no loaded plugin answers to.
    let error = session
        .run("FLUSH TIDB PLUGINS nosuch")
        .expect_err("an unknown plugin is refused");
    assert!(
        error.to_string().contains("plugin 'nosuch' not found"),
        "{error}"
    );
}

/// Go `ColumnInfo.Comment` reaches all four readers, and `MODIFY COLUMN`
/// replaces it with whatever the new definition says: Go's
/// `GetModifiableColumnJob` builds the new column from the spec, so a MODIFY
/// that does not repeat COMMENT clears it, as `COMMENT ''` does (asked of the
/// Go server).
#[test]
fn column_comments_round_trip_and_modify_overlays_them() {
    let mut session = Session::new();
    session.run("CREATE DATABASE d").unwrap();
    session.run("USE d").unwrap();
    session
        .run(
            "CREATE TABLE t (a int COMMENT 'kept', b int COMMENT 'replaced', \
              c int COMMENT 'cleared', d int)",
        )
        .unwrap();

    let created = row_text(session.run("SHOW CREATE TABLE t"))[0][1].clone();
    assert!(
        created.contains("`a` int DEFAULT NULL COMMENT 'kept'"),
        "{created}"
    );
    assert!(
        created.contains("`d` int DEFAULT NULL\n"),
        "an unset comment prints nothing: {created}"
    );

    // `information_schema.columns` and `SHOW FULL COLUMNS` are separate
    // readers with separate cells; both used to report empty.
    assert_eq!(
        row_text(session.run(
            "SELECT column_name, column_comment FROM information_schema.columns \
             WHERE table_name = 't' ORDER BY column_name"
        )),
        [
            ["a", "kept"],
            ["b", "replaced"],
            ["c", "cleared"],
            ["d", ""],
        ]
    );
    let full = row_text(session.run("SHOW FULL COLUMNS FROM t"));
    assert_eq!(full[0][8], "kept");
    assert_eq!(full[3][8], "");

    // A written COMMENT replaces; an absent or empty one clears.
    session.run("ALTER TABLE t MODIFY COLUMN a bigint").unwrap();
    session
        .run("ALTER TABLE t MODIFY COLUMN b bigint COMMENT 'new'")
        .unwrap();
    session
        .run("ALTER TABLE t MODIFY COLUMN c bigint COMMENT ''")
        .unwrap();
    let altered = row_text(session.run("SHOW CREATE TABLE t"))[0][1].clone();
    assert!(
        altered.contains("`a` bigint DEFAULT NULL,"),
        "an absent COMMENT clears the old one: {altered}"
    );
    assert!(
        altered.contains("`b` bigint DEFAULT NULL COMMENT 'new'"),
        "{altered}"
    );
    assert!(
        altered.contains("`c` bigint DEFAULT NULL,"),
        "COMMENT '' clears it: {altered}"
    );
}

/// Two `SHOW CREATE TABLE` rendering bugs found by round-tripping a
/// feature-rich definition through its own output.
///
/// Go's column loop does NOT stop at an auto-increment column: it falls
/// through to the comment, which such a column carries like any other. And
/// Go `Datum.ToString` prints an ENUM/SET value's NAME, so a default of `'x'`
/// reads back as `'x'` rather than the empty string. In both cases the stored
/// metadata was already right and only the reader was wrong, which is why
/// `information_schema` and an omitted INSERT disagreed with SHOW.
#[test]
fn show_create_table_renders_auto_increment_comments_and_enum_defaults() {
    let mut session = Session::new();
    session.run("CREATE DATABASE d").unwrap();
    session.run("USE d").unwrap();
    session
        .run(
            "CREATE TABLE t (\
               id bigint NOT NULL AUTO_INCREMENT COMMENT 'pk', \
               d enum('x','y') DEFAULT 'x', \
               e set('p','q') DEFAULT 'p', \
               PRIMARY KEY (id))",
        )
        .unwrap();

    let created = row_text(session.run("SHOW CREATE TABLE t"))[0][1].clone();
    assert!(
        created.contains("`id` bigint NOT NULL AUTO_INCREMENT COMMENT 'pk'"),
        "an auto-increment column still prints its comment: {created}"
    );
    assert!(
        created.contains("`d` enum('x','y') DEFAULT 'x'"),
        "an ENUM default prints its name: {created}"
    );
    assert!(
        created.contains("`e` set('p','q') DEFAULT 'p'"),
        "a SET default prints its name: {created}"
    );

    // The stored value was always right; these are the readers that used to
    // disagree with it.
    assert_eq!(
        row_text(session.run(
            "SELECT column_name, column_default FROM information_schema.columns \
             WHERE table_name = 't' AND column_name IN ('d','e') ORDER BY column_name"
        )),
        [["d", "x"], ["e", "p"]]
    );
    session.run("INSERT INTO t (id) VALUES (1)").unwrap();
    assert_eq!(
        row_text(session.run("SELECT d, e FROM t")),
        [["x", "p"]],
        "an omitted column takes the default the definition named"
    );
}

/// Go's DDL history as `information_schema.DDL_JOBS` and `ADMIN SHOW DDL
/// JOBS` read it: each submitted job finished the way Go's worker leaves it
/// (a duplicate on ADD UNIQUE INDEX is rolled back, an executor refusal
/// submits nothing), newest first, a multi-schema change expanded into its
/// sub-jobs, and INTERVAL sugar recorded as the partitions it generated.
/// Captured from Go TiDB.
#[test]
fn ddl_job_history_answers_as_go() {
    let mut session = Session::new();
    for sql in [
        "create database jdb",
        "use jdb",
        "create table t (a int, b int)",
        "insert into t values (1,1),(2,1),(3,3)",
        "alter table t add index ia(a)",
    ] {
        session.run(sql).unwrap();
    }
    assert!(session.run("alter table t add unique index ub(b)").is_err());
    assert!(session
        .run("alter table t add column c int, add index ic(c)")
        .is_err());
    for sql in [
        "alter table t modify column b bigint",
        "alter table t drop index ia",
        "truncate table t",
        "rename table t to t2",
        "create view v as select 1",
        "drop view v",
        "drop table t2",
        "create table x (a int)",
        "drop table if exists x, nosuch",
        "drop database jdb",
    ] {
        session.run(sql).unwrap();
    }
    let jobs = row_text(session.run(
        "select db_name, table_name, job_type, schema_state, row_count, state, query \
         from information_schema.ddl_jobs limit 14",
    ));
    let expected: [[&str; 7]; 14] = [
        [
            "jdb",
            "",
            "drop schema",
            "none",
            "0",
            "synced",
            "drop database jdb",
        ],
        [
            "jdb",
            "x",
            "drop table",
            "none",
            "0",
            "synced",
            "drop table if exists x, nosuch",
        ],
        [
            "jdb",
            "x",
            "create table",
            "public",
            "0",
            "synced",
            "create table x (a int)",
        ],
        [
            "jdb",
            "t2",
            "drop table",
            "none",
            "0",
            "synced",
            "drop table t2",
        ],
        [
            "jdb",
            "v",
            "drop view",
            "none",
            "0",
            "synced",
            "drop view v",
        ],
        [
            "jdb",
            "v",
            "create view",
            "public",
            "0",
            "synced",
            "create view v as select 1",
        ],
        [
            "jdb",
            "t2",
            "rename table",
            "public",
            "0",
            "synced",
            "rename table t to t2",
        ],
        [
            "jdb",
            "t",
            "truncate table",
            "public",
            "0",
            "synced",
            "truncate table t",
        ],
        [
            "jdb",
            "t",
            "drop index",
            "none",
            "0",
            "synced",
            "alter table t drop index ia",
        ],
        [
            "jdb",
            "t",
            "modify column",
            "public",
            "0",
            "synced",
            "alter table t modify column b bigint",
        ],
        [
            "jdb",
            "t",
            "add index",
            "none",
            "0",
            "rollback done",
            "alter table t add unique index ub(b)",
        ],
        [
            "jdb",
            "t",
            "add index",
            "public",
            "3",
            "synced",
            "alter table t add index ia(a)",
        ],
        [
            "jdb",
            "t",
            "create table",
            "public",
            "0",
            "synced",
            "create table t (a int, b int)",
        ],
        [
            "jdb",
            "",
            "create schema",
            "public",
            "0",
            "synced",
            "create database jdb",
        ],
    ];
    assert_eq!(jobs, expected);

    session.run("use test").unwrap();
    session.run("create table tt (a int, b int)").unwrap();
    session
        .run("alter table tt add index ta(a), add column c int, add unique index tb(b), add column d int")
        .unwrap();
    assert_eq!(
        row_text(
            session.run(
                "select db_name, table_name, job_type from information_schema.DDL_JOBS limit 4"
            )
        ),
        [
            ["test", "tt", "alter table multi-schema change"],
            ["test", "tt", "add column /* subjob */"],
            ["test", "tt", "add column /* subjob */"],
            ["test", "tt", "add index /* subjob */"],
        ]
    );
    session.run("create table r1 (a int)").unwrap();
    session.run("create table r2 (a int)").unwrap();
    session.run("rename table r1 to r3, r2 to r4").unwrap();
    assert_eq!(
        row_text(
            session.run(
                "select db_name, table_name, job_type from information_schema.DDL_JOBS limit 1"
            )
        ),
        [["test", "r3,r4", "rename tables"]]
    );

    let last_query = |session: &mut Session| {
        row_text(session.run("select QUERY from information_schema.ddl_jobs limit 1"))[0][0].clone()
    };
    session
        .run(
            "CREATE TABLE tp1 (id int) PARTITION BY RANGE (id) INTERVAL (100) \
             FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (500)",
        )
        .unwrap();
    assert_eq!(
        last_query(&mut session),
        "CREATE TABLE tp1 (id int) PARTITION BY RANGE (id) (PARTITION `P_LT_100` VALUES LESS THAN (100),\n \
         PARTITION `P_LT_200` VALUES LESS THAN (200),\n \
         PARTITION `P_LT_300` VALUES LESS THAN (300),\n \
         PARTITION `P_LT_400` VALUES LESS THAN (400),\n \
         PARTITION `P_LT_500` VALUES LESS THAN (500))"
    );
    session
        .run("alter table tp1 last partition less than (700)")
        .unwrap();
    assert_eq!(
        last_query(&mut session),
        "alter table tp1 ADD PARTITION (PARTITION `P_LT_600` VALUES LESS THAN (600),\n \
         PARTITION `P_LT_700` VALUES LESS THAN (700))"
    );
    session
        .run("alter table tp1 first partition less than (300)")
        .unwrap();
    assert_eq!(
        last_query(&mut session),
        "alter table tp1 DROP PARTITION `P_LT_100`, `P_LT_200`"
    );

    // ADMIN SHOW DDL JOBS takes Go's ten newest jobs unless told otherwise,
    // with COMMENTS in the last column.
    let shown = row_text(session.run("admin show ddl jobs"));
    assert_eq!(
        shown.len(),
        10 + 3,
        "a multi-schema change adds its sub-jobs"
    );
    assert_eq!(shown[0][3], "drop partition");
    assert_eq!(row_text(session.run("admin show ddl jobs 1")).len(), 1);
}
