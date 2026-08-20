//! The `SHOW` family's ADMIN-adjacent arms, split out of `tests_show` when
//! that file passed the repository's 2200-line ceiling.
//!
//! These cover the statements a client or dump tool issues that Go answers
//! from `SimpleExec`/`ShowExec` without touching a user table, plus the
//! column-metadata readers that must agree with each other.

#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// The statements a MySQL client or dump tool issues that Go answers from
/// `SimpleExec`/`ShowExec` without touching any table.
///
/// `SHOW PLUGINS` is Go's `fetchShowPlugins` over `plugin.GetAll()` and
/// `SHOW PROFILES` is its literal `// empty result` arm, so both answer
/// their column list and no rows. `SHOW MASTER STATUS` reports the pseudo
/// binlog file and the CURRENT transaction's start timestamp -- which is
/// zero outside a transaction, exactly as `@@tidb_current_ts` is, because
/// Go reads the same `TxnCtx.StartTS`.
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
        assert!(
            matches!(session.run(sql), Ok(StmtResult::Done(_))),
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
/// treats an absent `COMMENT` differently from an empty one.
///
/// Go's `getModifiableColumnJob` CLONES the old column and lets
/// `ProcessModifyColumnOptions` overlay only the options the spec names, so
/// a MODIFY that does not repeat COMMENT keeps the existing one while
/// `COMMENT ''` clears it. Before this, no column comment was stored at all:
/// the DDL accepted it and every reader showed nothing.
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

    // Absent COMMENT keeps, a written one replaces, and an empty one clears.
    session.run("ALTER TABLE t MODIFY COLUMN a bigint").unwrap();
    session
        .run("ALTER TABLE t MODIFY COLUMN b bigint COMMENT 'new'")
        .unwrap();
    session
        .run("ALTER TABLE t MODIFY COLUMN c bigint COMMENT ''")
        .unwrap();
    let altered = row_text(session.run("SHOW CREATE TABLE t"))[0][1].clone();
    assert!(
        altered.contains("`a` bigint DEFAULT NULL COMMENT 'kept'"),
        "an absent COMMENT keeps the old one: {altered}"
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
