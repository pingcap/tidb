#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// A whole session lifecycle from SQL strings alone: DDL, writes, reads.
#[test]
fn session_runs_a_sql_lifecycle() {
    let mut session = Session::new();
    assert_eq!(
        session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap(),
        StmtResult::Done(true)
    );
    assert_eq!(
        session
            .run("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap(),
        StmtResult::Affected(3)
    );
    assert_eq!(
        session
            .run("SELECT a + b FROM t WHERE a >= 2 ORDER BY a DESC LIMIT 1")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(33)]])
    );
    // A second table coexists in the same catalog.
    session.run("CREATE TABLE u (x BIGINT)").unwrap();
    session.run("INSERT INTO u VALUES (42)").unwrap();
    assert_eq!(
        session.run("SELECT x FROM u").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(42)]])
    );
}

/// A handful of everyday string/date builtins that were previously
/// refused by the chunk rewriter's return-type gate (`builtin_return_type`
/// had no arm for them, even though `eval_func_values`/`time_fn::dispatch`
/// already implement them). Expected values captured from upstream Go
/// via `SELECT ...` in a mock-store testkit session.
#[test]
fn everyday_string_and_date_builtins() {
    let mut session = Session::new();
    assert_eq!(
        session
            .run("SELECT SUBSTRING_INDEX('a.b.c', '.', 2)")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("a.b")]])
    );
    assert_eq!(
        session.run("SELECT CHAR(77, 121, 83, 81, 76)").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("MySQL")]])
    );
    assert_eq!(
        session
            .run("SELECT INSERT('Quadratic', 3, 4, 'What')")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("QuWhattic")]])
    );
    assert_eq!(
        session
            .run("SELECT EXPORT_SET(5, 'Y', 'N', ',', 4)")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("Y,N,Y,N")]])
    );
    assert_eq!(
        session
            .run("SELECT DATE_FORMAT('2024-01-01 10:00:00', '%Y-%m-%d %H:%i:%s')")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("2024-01-01 10:00:00")]])
    );
    assert_eq!(
        session
            .run("SELECT STR_TO_DATE('01,5,2024','%d,%m,%Y')")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("2024-05-01")]])
    );
    assert_eq!(
        session.run("SELECT QUOTE('a''b')").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("'a\\'b'")]])
    );
}

/// UPDATE and DELETE run through the session like any other write, and
/// report their affected-row counts.
#[test]
fn update_and_delete_through_the_session() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT)").unwrap();
    session.run("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    assert_eq!(
        session.run("UPDATE t SET a = a * 10 WHERE a > 1").unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        session.run("DELETE FROM t WHERE a >= 20").unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        session.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );
    // Both are classified as writes, so the wire answers with an OK packet.
    assert_eq!(
        session.statement_kind("UPDATE t SET a = 1").unwrap(),
        StmtKind::Write
    );
    assert_eq!(
        session.statement_kind("DELETE FROM t").unwrap(),
        StmtKind::Write
    );
}

/// A transaction stages its writes: the session reads its own, a peer
/// sharing the catalog sees nothing until COMMIT, and ROLLBACK discards.
#[test]
fn transaction_stages_writes_until_commit() {
    let mut writer = Session::new();
    writer.run("CREATE TABLE t (a BIGINT)").unwrap();
    writer.run("INSERT INTO t VALUES (1)").unwrap();
    let mut peer = Session::with_catalog(writer.shared_catalog());

    assert_eq!(writer.control_transaction("BEGIN").unwrap(), Some(true));
    assert!(writer.in_transaction());
    writer.run("INSERT INTO t VALUES (2)").unwrap();

    // The transaction reads its own write; the peer does not see it.
    assert_eq!(
        writer.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
    );
    assert_eq!(
        peer.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    assert_eq!(writer.control_transaction("COMMIT").unwrap(), Some(false));
    assert!(!writer.in_transaction());
    assert_eq!(
        peer.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
    );

    // ROLLBACK discards everything staged since BEGIN.
    writer.control_transaction("BEGIN").unwrap();
    writer.run("INSERT INTO t VALUES (3)").unwrap();
    writer.run("DELETE FROM t WHERE a = 1").unwrap();
    assert_eq!(writer.control_transaction("ROLLBACK").unwrap(), Some(false));
    assert_eq!(
        writer.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
    );
}

/// A commit that would discard a peer's writes is refused, rather than
/// silently overwriting them. The refused transaction is over, so its
/// staged writes are gone -- the statements must be retried, not the
/// COMMIT alone.
#[test]
fn a_conflicting_commit_is_refused() {
    let mut first = Session::new();
    first.run("CREATE TABLE t (a BIGINT)").unwrap();
    let mut second = Session::with_catalog(first.shared_catalog());

    first.control_transaction("BEGIN").unwrap();
    first.run("INSERT INTO t VALUES (1)").unwrap();
    // The peer commits first, moving the shared catalog.
    second.run("INSERT INTO t VALUES (2)").unwrap();

    assert!(matches!(
        first.control_transaction("COMMIT"),
        Err(DriverError::Txn(TxnErrorKind::WriteConflict))
    ));
    assert!(!first.in_transaction(), "a refused commit ends the txn");
    // The peer's write survived; the refused one did not.
    assert_eq!(
        second.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(2)]])
    );
}

/// BEGIN inside an open transaction implicitly commits it, as in Go, and
/// COMMIT/ROLLBACK outside one is a no-op, as in MySQL.
#[test]
fn nested_begin_commits_and_stray_commit_is_a_no_op() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT)").unwrap();
    assert_eq!(session.control_transaction("COMMIT").unwrap(), Some(false));
    assert_eq!(
        session.control_transaction("ROLLBACK").unwrap(),
        Some(false)
    );

    session.control_transaction("BEGIN").unwrap();
    session.run("INSERT INTO t VALUES (1)").unwrap();
    // The implicit commit publishes the first transaction's write.
    session.control_transaction("START TRANSACTION").unwrap();
    session.run("INSERT INTO t VALUES (2)").unwrap();
    session.control_transaction("ROLLBACK").unwrap();
    assert_eq!(
        session.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    // A non-transaction statement is not claimed by the hook.
    assert_eq!(session.control_transaction("SELECT 1").unwrap(), None);
    assert!(session
        .control_transaction("ROLLBACK TO SAVEPOINT s")
        .is_err());
}

/// SET and the variable reads a connecting client performs.
#[test]
fn session_variables() {
    let mut session = Session::new();

    // A stock client's opening statements.
    assert_eq!(session.apply_set("SET NAMES utf8mb4").unwrap(), Some(()));
    assert_eq!(
        session.vars().get_system("character_set_client").unwrap(),
        "utf8mb4"
    );
    assert_eq!(session.apply_set("SET autocommit = 0").unwrap(), Some(()));
    // Go's checkBoolSystemVar canonicalizes 0/1 to OFF/ON.
    assert_eq!(session.vars().get_system("autocommit").unwrap(), "OFF");

    // Reading variables back through a query.
    assert_eq!(
        scalar_text(&mut session, "SELECT @@autocommit"),
        Some("OFF".to_owned())
    );
    let comment = scalar_text(&mut session, "SELECT @@version_comment").unwrap();
    assert!(
        comment.starts_with("TiDB Server (Apache License 2.0)"),
        "{comment}"
    );

    // DEFAULT restores the registry default.
    session.apply_set("SET autocommit = DEFAULT").unwrap();
    assert_eq!(session.vars().get_system("autocommit").unwrap(), "ON");

    // An unknown system variable is Go's 1193, on read and on write.
    assert!(matches!(
        session.apply_set("SET nonexistent_variable = 1"),
        Err(DriverError::Var(
            tidb_executor::VarErrorKind::UnknownSystemVariable(_)
        ))
    ));
    assert!(matches!(
        session.run("SELECT @@nonexistent_variable"),
        Err(DriverError::Var(
            tidb_executor::VarErrorKind::UnknownSystemVariable(_)
        ))
    ));
    // A read-only variable cannot be set.
    assert!(matches!(
        session.apply_set("SET version = '1'"),
        Err(DriverError::Var(
            tidb_executor::VarErrorKind::ReadOnlyVariable(_)
        ))
    ));

    // User variables: unset reads as NULL, never an error.
    assert_eq!(
        session.run("SELECT @nope").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Null]])
    );
    session.apply_set("SET @x = 41 + 1").unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @x"),
        Some("42".to_owned())
    );

    // A non-SET statement is not claimed by the hook.
    assert_eq!(session.apply_set("SELECT 1").unwrap(), None);
}

/// Transcreated from Go `pkg/executor/test/ddl/ddl_test.go`
/// `TestCreateDropDatabase`, case for case, minus the parts that need
/// tiers this seed does not have yet.
///
/// NOT PORTED from that Go test (documented): every `charset`/`collate`
/// database option and its `SHOW CREATE DATABASE` output, which need the
/// charset tier; the `drop database mysql` rejection, which needs the
/// system schemas; and the privilege/role cases.
#[test]
fn create_drop_database() {
    let mut session = Session::new();

    // tk.MustExec("create database if not exists drop_test;")
    session
        .run("CREATE DATABASE IF NOT EXISTS drop_test")
        .unwrap();
    // tk.MustExec("drop database if exists drop_test;")
    session.run("DROP DATABASE IF EXISTS drop_test").unwrap();
    // tk.MustExec("create database drop_test;")
    session.run("CREATE DATABASE drop_test").unwrap();
    // tk.MustExec("use drop_test;")
    session.run("USE drop_test").unwrap();
    assert_eq!(session.current_database(), "drop_test");
    // tk.MustExec("drop database drop_test;")
    session.run("DROP DATABASE drop_test").unwrap();

    // tk.MustGetDBError("drop table t;", plannererrors.ErrNoDB)
    // tk.MustGetDBError("select * from t;", plannererrors.ErrNoDB)
    // Dropping the current database leaves none selected.
    assert_eq!(session.current_database(), "");
    assert!(matches!(
        session.run("SELECT * FROM t"),
        Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected))
    ));
    assert!(matches!(
        session.run("INSERT INTO t VALUES (1)"),
        Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected))
    ));

    // Creating a database that exists is Go's ErrDBCreateExists unless
    // IF NOT EXISTS was written.
    session.run("CREATE DATABASE drop_test").unwrap();
    assert!(matches!(
        session.run("CREATE DATABASE drop_test"),
        Err(DriverError::Schema(SchemaErrorKind::DatabaseExists(_)))
    ));
    session
        .run("CREATE DATABASE IF NOT EXISTS drop_test")
        .unwrap();
    // Dropping one that does not exist is ErrDBDropExists unless IF EXISTS.
    session.run("DROP DATABASE drop_test").unwrap();
    assert!(matches!(
        session.run("DROP DATABASE drop_test"),
        Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(_)))
    ));
    session.run("DROP DATABASE IF EXISTS drop_test").unwrap();

    // USE on an unknown schema is Go's ErrDatabaseNotExists.
    assert!(matches!(
        session.run("USE no_such_database"),
        Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(_)))
    ));
}

/// A table in another schema is reachable by qualifying it, which is what
/// makes the schema tier more than a listing.
#[test]
fn a_qualified_name_resolves_across_schemas() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT)").unwrap();
    session.run("INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(
        session.run("SELECT a FROM test.t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );
}

/// USE changes where unqualified names resolve, which is the point of the
/// schema tier: the same table name in two schemas is two tables.
#[test]
fn use_changes_unqualified_name_resolution() {
    let mut session = Session::new();
    session.run("CREATE DATABASE other").unwrap();

    session.run("CREATE TABLE t (a BIGINT)").unwrap();
    session.run("INSERT INTO t VALUES (1)").unwrap();

    session.run("USE other").unwrap();
    // `t` here is a different table, in the other schema.
    session.run("CREATE TABLE t (a BIGINT)").unwrap();
    session.run("INSERT INTO t VALUES (2)").unwrap();
    assert_eq!(
        session.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(2)]])
    );
    // The first schema's table is still reachable by qualifying it.
    assert_eq!(
        session.run("SELECT a FROM test.t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );
    assert_eq!(
        session.run_with_columns("SHOW TABLES").unwrap(),
        StmtOutput::Rows {
            columns: vec![(
                "Tables_in_other".to_owned(),
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString)
            )],
            rows: vec![vec![Datum::Bytes(b"t".to_vec())]],
        }
    );

    session.run("USE test").unwrap();
    assert_eq!(
        session.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );
    // Writes follow the current schema too.
    session.run("UPDATE t SET a = 10").unwrap();
    assert_eq!(
        session.run("SELECT a FROM other.t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(2)]])
    );
}

/// LAST_INSERT_ID, checked against a sequence captured from real TiDB:
/// 0, 1, 2 (the FIRST id of a multi-row insert), unchanged by an explicit
/// value, then 101 and 102, and unchanged by a non-allocating statement.
#[test]
fn last_insert_id() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE a (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)")
        .unwrap();
    let read = |session: &mut Session| match session.run("SELECT LAST_INSERT_ID()").unwrap() {
        StmtResult::Rows(rows) => datum_text(&rows[0][0]).unwrap(),
        other => panic!("expected rows, got {other:?}"),
    };

    assert_eq!(read(&mut session), "0", "captured: start");
    session.run("INSERT INTO a (v) VALUES (10)").unwrap();
    assert_eq!(read(&mut session), "1", "captured: after single auto");
    session
        .run("INSERT INTO a (v) VALUES (20), (30), (40)")
        .unwrap();
    assert_eq!(
        read(&mut session),
        "2",
        "captured: a multi-row insert reports its FIRST id"
    );
    session.run("INSERT INTO a VALUES (100, 50)").unwrap();
    assert_eq!(
        read(&mut session),
        "2",
        "captured: an explicit value leaves it unchanged"
    );
    session.run("INSERT INTO a (v) VALUES (60)").unwrap();
    assert_eq!(read(&mut session), "101", "captured: after auto again");
    session.run("INSERT INTO a VALUES (NULL, 70)").unwrap();
    assert_eq!(read(&mut session), "102", "captured: NULL allocates");

    // A table with no auto column, and an UPDATE, both leave it alone.
    session
        .run("CREATE TABLE b (id BIGINT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO b VALUES (5)").unwrap();
    assert_eq!(read(&mut session), "102", "captured: non-auto insert");
    session.run("UPDATE a SET v = 0 WHERE id = 1").unwrap();
    assert_eq!(read(&mut session), "102", "captured: after update");

    // The OK packet's field is per statement, so it is 0 for a statement
    // that allocated nothing, unlike the sticky function value.
    session.run("INSERT INTO a (v) VALUES (80)").unwrap();
    assert_eq!(session.statement_insert_id(), 103);
    session.run("INSERT INTO b VALUES (6)").unwrap();
    assert_eq!(session.statement_insert_id(), 0);
    assert_eq!(session.last_insert_id(), 103);
}

/// DROP TABLE, checked against captured TiDB behavior: a missing name is
/// 1051, IF EXISTS suppresses it, and a mixed list still drops the tables
/// that exist BEFORE reporting the error.
#[test]
fn drop_table() {
    let mut session = Session::new();
    for name in ["d1", "d2", "d3"] {
        session
            .run(&format!("CREATE TABLE {name} (a BIGINT)"))
            .unwrap();
    }
    let tables = |session: &mut Session| match session.run_with_columns("SHOW TABLES").unwrap() {
        StmtOutput::Rows { rows, .. } => rows
            .into_iter()
            .map(|row| datum_text(&row[0]).unwrap())
            .collect::<Vec<_>>(),
        other => panic!("expected rows, got {other:?}"),
    };

    // Captured: [schema:1051]Unknown table 'test.nosuch'
    assert!(matches!(
        session.run("DROP TABLE nosuch"),
        Err(DriverError::Schema(SchemaErrorKind::BadTable(_)))
    ));
    // Captured: IF EXISTS is a no-op.
    session.run("DROP TABLE IF EXISTS nosuch").unwrap();

    // Captured: `drop table d1, nosuch` errors AND still drops d1.
    assert!(matches!(
        session.run("DROP TABLE d1, nosuch"),
        Err(DriverError::Schema(SchemaErrorKind::BadTable(_)))
    ));
    assert_eq!(tables(&mut session), vec!["d2".to_owned(), "d3".to_owned()]);

    // A multi-table drop removes them all.
    session.run("DROP TABLE d2, d3").unwrap();
    assert!(tables(&mut session).is_empty());

    // A dropped name can be recreated with a different shape, so the drop
    // removed the metadata rather than only the rows.
    session.run("CREATE TABLE d2 (b BIGINT)").unwrap();
    assert_eq!(
        session.run("SELECT b FROM d2").unwrap(),
        StmtResult::Rows(vec![])
    );
    // The rows are gone too: a recreated table starts empty.
    session.run("INSERT INTO d2 VALUES (1)").unwrap();
    session.run("DROP TABLE d2").unwrap();
    session.run("CREATE TABLE d2 (b BIGINT)").unwrap();
    assert_eq!(
        session.run("SELECT b FROM d2").unwrap(),
        StmtResult::Rows(vec![])
    );
}

/// ALTER TABLE ADD/DROP COLUMN, checked against captured TiDB behavior.
/// The one that matters most: ADD COLUMN ... DEFAULT 7 makes rows written
/// EARLIER read back 7, without rewriting them.
#[test]
fn alter_table_columns() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE a (id BIGINT PRIMARY KEY, v BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO a VALUES (1, 10), (2, 20)")
        .unwrap();

    // Captured: [[1 10 7] [2 20 7]] -- the existing rows take the default.
    session
        .run("ALTER TABLE a ADD COLUMN w BIGINT DEFAULT 7")
        .unwrap();
    assert_eq!(
        session.run("SELECT id, v, w FROM a").unwrap(),
        StmtResult::Rows(vec![
            vec![Datum::Int(1), Datum::Int(10), Datum::Int(7)],
            vec![Datum::Int(2), Datum::Int(20), Datum::Int(7)],
        ])
    );
    // Captured: without a default the existing rows read NULL.
    session.run("ALTER TABLE a ADD COLUMN x BIGINT").unwrap();
    assert_eq!(
        session.run("SELECT id, x FROM a").unwrap(),
        StmtResult::Rows(vec![
            vec![Datum::Int(1), Datum::Null],
            vec![Datum::Int(2), Datum::Null],
        ])
    );

    let columns = |session: &mut Session| match session.run_with_columns("DESCRIBE a").unwrap() {
        StmtOutput::Rows { rows, .. } => rows
            .into_iter()
            .map(|row| datum_text(&row[0]).unwrap())
            .collect::<Vec<_>>(),
        other => panic!("expected rows, got {other:?}"),
    };
    // Captured order after FIRST then AFTER v: y, id, v, z, w, x.
    session
        .run("ALTER TABLE a ADD COLUMN y BIGINT FIRST")
        .unwrap();
    session
        .run("ALTER TABLE a ADD COLUMN z BIGINT AFTER v")
        .unwrap();
    assert_eq!(columns(&mut session), ["y", "id", "v", "z", "w", "x"]);

    // A new column is written and read like any other, and the rows that
    // predate it still report their defaults.
    session
        .run("INSERT INTO a (id, v, w, x, y, z) VALUES (3, 30, 1, 2, 3, 4)")
        .unwrap();
    assert_eq!(
        session.run("SELECT w FROM a WHERE id = 3").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    // Captured: DROP COLUMN removes it from the schema.
    session.run("ALTER TABLE a DROP COLUMN w").unwrap();
    assert_eq!(columns(&mut session), ["y", "id", "v", "z", "x"]);
    assert!(session.run("SELECT w FROM a").is_err());

    // Captured error codes.
    assert!(matches!(
        session.run("ALTER TABLE a ADD COLUMN v BIGINT"),
        Err(DriverError::DuplicateColumnName(_))
    ));
    assert!(matches!(
        session.run("ALTER TABLE a DROP COLUMN nosuch"),
        Err(DriverError::UnknownColumnInAlter(_))
    ));
    session.run("CREATE TABLE one (a BIGINT)").unwrap();
    assert!(matches!(
        session.run("ALTER TABLE one DROP COLUMN a"),
        Err(DriverError::CannotDropOnlyColumn { .. })
    ));
    assert!(matches!(
        session.run("ALTER TABLE a DROP COLUMN id"),
        Err(DriverError::UnsupportedDropIntegerPrimaryKey)
    ));

    // Captured: a SINGLE-column index is dropped along with its column,
    // while a COMPOSITE one refuses the drop with 8200.
    session
        .run("CREATE TABLE ix (a BIGINT, b BIGINT, KEY kb (b))")
        .unwrap();
    session.run("INSERT INTO ix VALUES (1, 2)").unwrap();
    session.run("ALTER TABLE ix DROP COLUMN b").unwrap();
    let create_ix = match session.run_with_columns("SHOW CREATE TABLE ix").unwrap() {
        StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
        other => panic!("expected rows, got {other:?}"),
    };
    assert!(!create_ix.contains("kb"), "the index went with the column");
    assert_eq!(
        session.run("SELECT a FROM ix").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    // A unique single-column index behaves the same way.
    session
        .run("CREATE TABLE uq (a BIGINT, b BIGINT, UNIQUE KEY ua (a))")
        .unwrap();
    session.run("ALTER TABLE uq DROP COLUMN a").unwrap();

    // A composite index refuses it, and the table is unchanged.
    session
        .run("CREATE TABLE comp (a BIGINT, b BIGINT, c BIGINT, KEY kab (a, b))")
        .unwrap();
    assert!(matches!(
        session.run("ALTER TABLE comp DROP COLUMN a"),
        Err(DriverError::CannotDropColumnWithCompositeIndex(_))
    ));
    let create_comp = match session.run_with_columns("SHOW CREATE TABLE comp").unwrap() {
        StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
        other => panic!("expected rows, got {other:?}"),
    };
    assert!(create_comp.contains("KEY `kab` (`a`,`b`)"));

    // An unknown table is an error, and an action this tier does not
    // implement is still rejected rather than ignored. (RENAME TO used to
    // be this example; it is implemented now.)
    assert!(session
        .run("ALTER TABLE nosuch ADD COLUMN a BIGINT")
        .is_err());
    assert!(session.run("ALTER TABLE a ORDER BY v").is_err());
}

/// CREATE INDEX / DROP INDEX / ALTER TABLE ADD INDEX, checked against
/// captured TiDB behavior -- including that CREATE INDEX backfills the
/// rows that already exist.
#[test]
fn index_ddl() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE i1 (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO i1 VALUES (1, 10, 1), (2, 20, 1), (3, 10, 2)")
        .unwrap();

    // The index is backfilled, so it finds rows written before it existed.
    // Captured: select id from i1 where a = 10 -> [[1] [3]].
    session.run("CREATE INDEX ia ON i1 (a)").unwrap();
    assert_eq!(
        session.run("SELECT id FROM i1 WHERE a = 10").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(3)]])
    );
    // SHOW CREATE TABLE reports it, captured as KEY `ia` (`a`).
    let create =
        |session: &mut Session| match session.run_with_columns("SHOW CREATE TABLE i1").unwrap() {
            StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
            other => panic!("expected rows, got {other:?}"),
        };
    assert!(create(&mut session).contains("KEY `ia` (`a`)"));

    // Captured: a duplicate index name is 1061.
    assert!(matches!(
        session.run("CREATE INDEX ia ON i1 (b)"),
        Err(DriverError::DuplicateKeyName(_))
    ));
    // Captured: a unique index over data that already collides is 1062
    // naming table.index, and the index is NOT created.
    match session.run("CREATE UNIQUE INDEX ua ON i1 (a)") {
        Err(DriverError::DuplicateEntry { value, key }) => {
            assert_eq!(value, "10");
            assert_eq!(key, "i1.ua");
        }
        other => panic!("expected a duplicate-entry error, got {other:?}"),
    }
    assert!(!create(&mut session).contains("ua"));

    // A unique index over data that does not collide is created.
    session.run("CREATE UNIQUE INDEX ub ON i1 (b, a)").unwrap();
    assert!(create(&mut session).contains("UNIQUE KEY `ub` (`b`,`a`)"));
    // It is enforced from then on.
    assert!(session.run("INSERT INTO i1 VALUES (4, 10, 1)").is_err());

    // DROP INDEX removes it, and its entries with it: the same insert now
    // succeeds.
    session.run("DROP INDEX ub ON i1").unwrap();
    assert!(!create(&mut session).contains("ub"));
    session.run("INSERT INTO i1 VALUES (4, 10, 1)").unwrap();

    // Captured: dropping one that does not exist is 1091.
    assert!(matches!(
        session.run("DROP INDEX nosuch ON i1"),
        Err(DriverError::UnknownIndex(_))
    ));

    // ALTER TABLE ADD INDEX takes the same path.
    session.run("ALTER TABLE i1 ADD INDEX ic (b)").unwrap();
    assert!(create(&mut session).contains("KEY `ic` (`b`)"));
    session.run("ALTER TABLE i1 DROP INDEX ic").unwrap();
    assert!(!create(&mut session).contains("ic"));

    // An index over an unknown column is rejected.
    assert!(session.run("CREATE INDEX bad ON i1 (nosuch)").is_err());
}

/// TRUNCATE TABLE, checked against captured TiDB behavior: the rows go,
/// the definition stays, and the auto-increment counter restarts.
#[test]
fn truncate_table() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT, KEY kv (v))")
        .unwrap();
    session
        .run("INSERT INTO t1 (v) VALUES (1), (2), (3)")
        .unwrap();
    assert_eq!(
        session.run("SELECT id FROM t1").unwrap(),
        StmtResult::Rows(vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(2)],
            vec![Datum::Int(3)]
        ])
    );

    session.run("TRUNCATE TABLE t1").unwrap();
    // Captured: no rows remain.
    assert_eq!(
        session.run("SELECT id FROM t1").unwrap(),
        StmtResult::Rows(vec![])
    );
    // Captured: the next auto-increment insert starts over at 1.
    session.run("INSERT INTO t1 (v) VALUES (9)").unwrap();
    assert_eq!(
        session.run("SELECT id FROM t1").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );
    // Captured: the definition, including the index, survives.
    let create = match session.run_with_columns("SHOW CREATE TABLE t1").unwrap() {
        StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
        other => panic!("expected rows, got {other:?}"),
    };
    assert!(create.contains("AUTO_INCREMENT"));
    assert!(create.contains("KEY `kv` (`v`)"));

    // The index entries went with the rows: a read through the index sees
    // only what was written after the truncate.
    assert_eq!(
        session.run("SELECT id FROM t1 WHERE v = 1").unwrap(),
        StmtResult::Rows(vec![])
    );
    assert_eq!(
        session.run("SELECT id FROM t1 WHERE v = 9").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    // Captured: truncating a table that does not exist is 1146.
    assert!(matches!(
        session.run("TRUNCATE TABLE nosuch"),
        Err(DriverError::Schema(SchemaErrorKind::UnknownTable(_)))
    ));
}

/// `INSERT ... SET col = value`, checked against captured TiDB output.
///
/// Go normalizes the `SET` list into a column list plus one VALUES row,
/// so every rule the VALUES form obeys -- defaults, NOT NULL, the column
/// cast, ON DUPLICATE KEY UPDATE and REPLACE -- applies unchanged.
#[test]
fn insert_set_syntax() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10) DEFAULT 'dd', \
                 c BIGINT NOT NULL DEFAULT 5)",
        )
        .unwrap();

    // Captured: the columns it names are assigned, the rest take their
    // defaults.
    assert_eq!(
        session.run("INSERT INTO t SET a = 1, b = 'x'").unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(row_text(session.run("SELECT * FROM t")), [["1", "x", "5"]]);
    session.run("INSERT INTO t SET a = 2").unwrap();
    // Captured: an assigned value may be an expression.
    session.run("INSERT INTO t SET a = 3, c = 1+1").unwrap();
    assert_eq!(
        row_text(session.run("SELECT * FROM t ORDER BY a")),
        [["1", "x", "5"], ["2", "dd", "5"], ["3", "dd", "2"]]
    );

    // Captured: a column with no default that the SET list omits is
    // 1364, the same as in the VALUES form.
    match session.run("INSERT INTO t SET b = 'nope'") {
        Err(error) => assert_eq!(error.to_mysql_error().code, 1364),
        Ok(other) => panic!("expected 1364, got {other:?}"),
    }
    // Captured: an unknown column names the field list.
    match session.run("INSERT INTO t SET nosuch = 1") {
        Err(error) => assert_eq!(error.to_mysql_error().code, 1054),
        Ok(other) => panic!("expected 1054, got {other:?}"),
    }

    // Captured: the conflict policies compose with it.
    assert_eq!(
        session
            .run("INSERT INTO t SET a = 1, b = 'dup' ON DUPLICATE KEY UPDATE b = 'updated'")
            .unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        row_text(session.run("SELECT b FROM t WHERE a = 1")),
        [["updated"]]
    );
    assert_eq!(
        session.run("REPLACE INTO t SET a = 2, b = 'repl'").unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
        [["1", "updated", "5"], ["2", "repl", "5"], ["3", "dd", "2"]]
    );
}

/// A DATETIME/DATE column compared with a string or a number, checked
/// against captured TiDB output.
///
/// This was a SILENT WRONG ANSWER before: the generic string-vs-numeric
/// rule compared '2024-12-31' by its numeric prefix, so the WHERE clause
/// every application writes returned the wrong rows without any error.
#[test]
fn time_compared_with_strings_and_numbers() {
    let mut session = Session::new();
    session.apply_set("SET time_zone = '+00:00'").unwrap();
    session
        .run("CREATE TABLE t (id BIGINT, created DATETIME, d DATE)")
        .unwrap();
    session
        .run(
            "INSERT INTO t VALUES (1,'2024-06-15 10:00:00','2024-06-15'),\
                 (2,'2024-12-30 23:59:59','2024-12-30'),(3,'2025-01-02 00:00:00','2025-01-02')",
        )
        .unwrap();

    // Captured: a bare date string means that date's midnight.
    assert_eq!(
        row_text(session.run("SELECT id FROM t WHERE created <= '2024-12-31'")),
        [["1"], ["2"]]
    );
    assert_eq!(
        row_text(session.run("SELECT id FROM t WHERE created > '2024-12-31'")),
        [["3"]]
    );
    assert_eq!(
        row_text(
            session.run(
                "SELECT id FROM t WHERE created BETWEEN '2024-01-01' AND '2024-12-31 23:59:59'"
            )
        ),
        [["1"], ["2"]]
    );
    // Captured: equality both ways, and against a DATE column.
    assert_eq!(
        row_text(session.run("SELECT id FROM t WHERE created = '2024-06-15 10:00:00'")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT '2024-06-15 10:00:00' = created FROM t WHERE id = 1")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT id FROM t WHERE d = '2024-06-15'")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT id FROM t WHERE d < '2024-12-31'")),
        [["1"], ["2"]]
    );
    // Captured: a bare NUMBER parses as a date too.
    assert_eq!(
        row_text(session.run("SELECT id FROM t WHERE created <= 20241231")),
        [["1"], ["2"]]
    );
    // Captured: garbage filters every row with warning 1292, not an error.
    assert_eq!(
        row_text(session.run("SELECT id FROM t WHERE created <= 'garbage'")),
        Vec::<Vec<String>>::new()
    );
    // DOCUMENTED DIVERGENCE (the standing coprocessor-merge one): TiDB
    // reported ONE 1292 here because its coprocessor merges a batch's
    // warnings; this tier warns once per row compared.
    assert_eq!(session.warnings().len(), 3, "one warning per row compared");
    assert_eq!(session.warnings()[0].code, 1292);
    assert_eq!(
        session.warnings()[0].message,
        "Incorrect datetime value: 'garbage'"
    );
}

/// `GROUP_CONCAT`, checked against captured TiDB output.
#[test]
fn group_concat() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (g BIGINT, v VARCHAR(10), n BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'b',2),(1,'a',1),(2,'c',3),(2,NULL,4),(1,'a',5)")
        .unwrap();

    // Captured: every non-NULL value joined by a comma, in row order.
    assert_eq!(
        row_text(session.run("SELECT GROUP_CONCAT(v) FROM t")),
        [["b,a,c,a"]]
    );
    // Captured: per group, with the NULL contributing nothing.
    assert_eq!(
        row_text(session.run("SELECT g, GROUP_CONCAT(v) FROM t GROUP BY g ORDER BY g")),
        [["1", "b,a,a"], ["2", "c"]]
    );
    // Captured: an explicit separator.
    assert_eq!(
        row_text(
            session.run("SELECT g, GROUP_CONCAT(v SEPARATOR '-') FROM t GROUP BY g ORDER BY g")
        ),
        [["1", "b-a-a"], ["2", "c"]]
    );
    // Captured: DISTINCT folds the repeat. TiDB's own output for this
    // group is `a,b`; MySQL documents the order of a GROUP_CONCAT
    // without ORDER BY as undefined, so only the membership is asserted.
    let distinct =
        row_text(session.run("SELECT g, GROUP_CONCAT(DISTINCT v) FROM t GROUP BY g ORDER BY g"));
    let mut first: Vec<&str> = distinct[0][1].split(',').collect();
    first.sort_unstable();
    assert_eq!(first, ["a", "b"]);
    assert_eq!(distinct[1][1], "c");
    // Captured: numbers are stringified.
    assert_eq!(
        row_text(session.run("SELECT GROUP_CONCAT(n) FROM t")),
        [["2,1,3,4,5"]]
    );
    // Captured: an empty group is NULL, not an empty string.
    assert_eq!(
        row_text(session.run("SELECT GROUP_CONCAT(v) FROM t WHERE g = 99")),
        [["NULL"]]
    );

    // Captured: the aggregate's own ORDER BY orders the rows WITHIN the
    // concatenation -- a separate scope from the query's ORDER BY.
    assert_eq!(
        row_text(session.run("SELECT g, GROUP_CONCAT(v ORDER BY v) FROM t GROUP BY g ORDER BY g")),
        [["1", "a,a,b"], ["2", "c"]]
    );
    // Captured: it may order by a column the concatenation does not
    // contain, descending, with its own separator.
    assert_eq!(
        row_text(session.run(
            "SELECT g, GROUP_CONCAT(v ORDER BY n DESC SEPARATOR '|') FROM t \
                 GROUP BY g ORDER BY g"
        )),
        [["1", "a|b|a"], ["2", "c"]]
    );

    // The multi-argument form: captured from TiDB, the arguments are
    // concatenated PER ROW (like CONCAT) before the rows are joined, and
    // a row is dropped as soon as ANY of its arguments is NULL -- not
    // only when all of them are.
    session.run("INSERT INTO t VALUES (2,'d',NULL)").unwrap();
    session.run("INSERT INTO t VALUES (1,'a',1)").unwrap();
    // (2,NULL,4) and (2,'d',NULL) each have one NULL argument: both drop.
    assert_eq!(
        row_text(session.run("SELECT g, GROUP_CONCAT(v, n) FROM t GROUP BY g ORDER BY g")),
        [["1", "b2,a1,a5,a1"], ["2", "c3"]]
    );
    // ...while the one-argument form still keeps 'd' (its v is not NULL).
    assert_eq!(
        row_text(session.run("SELECT GROUP_CONCAT(v) FROM t WHERE g = 2")),
        [["c,d"]]
    );
    // Captured: DISTINCT dedupes over the CONCATENATED per-row value, so
    // the repeated ('a',1) folds while ('a',5) survives. Row order
    // without ORDER BY is undefined; assert membership only.
    let multi =
        row_text(session.run("SELECT g, GROUP_CONCAT(DISTINCT v, n) FROM t GROUP BY g ORDER BY g"));
    let mut first: Vec<&str> = multi[0][1].split(',').collect();
    first.sort_unstable();
    assert_eq!(first, ["a1", "a5", "b2"]);
    assert_eq!(multi[1][1], "c3");
    // Captured: a literal argument concatenates like any other.
    assert_eq!(
        row_text(session.run("SELECT g, GROUP_CONCAT(v, '-', n) FROM t GROUP BY g ORDER BY g")),
        [["1", "b-2,a-1,a-5,a-1"], ["2", "c-3"]]
    );
    // Captured: multi-arg with the aggregate's own ORDER BY and separator.
    assert_eq!(
        row_text(session.run(
            "SELECT g, GROUP_CONCAT(v, n ORDER BY n DESC SEPARATOR '|') FROM t \
                 GROUP BY g ORDER BY g"
        )),
        [["1", "a5|b2|a1|a1"], ["2", "c3"]]
    );
}

/// Prepared-statement parameters: the marker count a PREPARE reports and
/// the values an EXECUTE binds.
///
/// This is the session half of the binary protocol -- what a JDBC or Go
/// driver client needs to run anything at all. The wire half wires
/// `COM_STMT_PREPARE`/`EXECUTE` to it.
#[test]
fn prepared_statement_parameters() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT)")
        .unwrap();

    // The marker count is what PREPARE reports.
    assert_eq!(
        session
            .parameter_count("SELECT a FROM t WHERE a = ?")
            .unwrap(),
        1
    );
    assert_eq!(
        session
            .parameter_count("INSERT INTO t (a,b,c) VALUES (?,?,?)")
            .unwrap(),
        3
    );
    assert_eq!(session.parameter_count("SELECT 1").unwrap(), 0);
    assert_eq!(
        session
            .parameter_count("SELECT a FROM t WHERE b LIKE ? AND c BETWEEN ? AND ?")
            .unwrap(),
        3
    );

    // An INSERT binds its values positionally.
    session
        .run_with_params(
            "INSERT INTO t (a,b,c) VALUES (?,?,?)",
            &[Datum::Int(1), Datum::Bytes(b"one".to_vec()), Datum::Int(10)],
        )
        .unwrap();
    session
        .run_with_params(
            "INSERT INTO t (a,b,c) VALUES (?,?,?)",
            &[Datum::Int(2), Datum::Bytes(b"two".to_vec()), Datum::Int(20)],
        )
        .unwrap();

    // A SELECT binds in WHERE, and the markers keep their source order.
    let output = session
        .run_with_params("SELECT b FROM t WHERE a = ?", &[Datum::Int(2)])
        .unwrap();
    match output {
        StmtOutput::Rows { rows, .. } => {
            assert_eq!(datum_text(&rows[0][0]).unwrap(), "two");
        }
        other => panic!("expected rows, got {other:?}"),
    }
    let output = session
        .run_with_params(
            "SELECT a FROM t WHERE c BETWEEN ? AND ? ORDER BY a",
            &[Datum::Int(5), Datum::Int(15)],
        )
        .unwrap();
    match output {
        StmtOutput::Rows { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("expected rows, got {other:?}"),
    }

    // A value that is not UTF-8 survives the round trip as a hex literal
    // rather than being mangled by a lossy conversion.
    session
        .run_with_params(
            "INSERT INTO t (a,b,c) VALUES (?,?,?)",
            &[
                Datum::Int(3),
                Datum::Bytes(vec![0xff, 0xfe, b'z']),
                Datum::Int(30),
            ],
        )
        .unwrap();
    match session
        .run_with_params("SELECT b FROM t WHERE a = ?", &[Datum::Int(3)])
        .unwrap()
    {
        StmtOutput::Rows { rows, .. } => {
            let stored = match &rows[0][0] {
                Datum::Bytes(bytes) => bytes.clone(),
                Datum::String(text) => text.bytes().to_vec(),
                other => panic!("expected a string datum, got {other:?}"),
            };
            assert_eq!(stored, vec![0xff, 0xfe, b'z']);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // A NULL parameter binds as NULL, not as the text "NULL".
    session
        .run_with_params(
            "INSERT INTO t (a,b,c) VALUES (?,?,?)",
            &[Datum::Int(4), Datum::Null, Datum::Int(40)],
        )
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b IS NULL")),
        [["4"]]
    );

    // Too few or too many values is Go's ErrWrongParamCount (1210).
    match session.run_with_params("SELECT a FROM t WHERE a = ?", &[]) {
        Ok(_) => panic!("an unbound marker should fail"),
        Err(error) => assert_eq!(error.to_mysql_error().code, 1210),
    }
    match session.run_with_params(
        "SELECT a FROM t WHERE a = ?",
        &[Datum::Int(1), Datum::Int(2)],
    ) {
        Ok(_) => panic!("an extra value should fail"),
        Err(error) => assert_eq!(error.to_mysql_error().code, 1210),
    }
}

/// One connection sends everything through one door: the transaction
/// controls, `SET`, and `SHOW VARIABLES` all answer from `run` now.
///
/// Checked against captured TiDB output: the columns are
/// `Variable_name` and `Value`, the LIKE pattern filters, and a SET is
/// visible to the next SHOW.
#[test]
fn run_routes_session_statements() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT)").unwrap();

    // The transaction controls answer through `run`.
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO t VALUES (1)").unwrap();
    session.run("COMMIT").unwrap();
    assert_eq!(row_text(session.run("SELECT a FROM t")), [["1"]]);
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO t VALUES (2)").unwrap();
    session.run("ROLLBACK").unwrap();
    assert_eq!(row_text(session.run("SELECT a FROM t")), [["1"]]);

    // So does SET.
    session.run("SET autocommit = 0").unwrap();

    // Captured: SHOW VARIABLES reports Variable_name/Value, filtered.
    match session
        .run_with_columns("SHOW VARIABLES LIKE 'autocommit'")
        .unwrap()
    {
        StmtOutput::Rows { columns, rows } => {
            assert_eq!(
                columns
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
                ["Variable_name", "Value"]
            );
            assert_eq!(rows.len(), 1);
            assert_eq!(datum_text(&rows[0][0]).unwrap(), "autocommit");
        }
        other => panic!("expected rows, got {other:?}"),
    }
    // Captured: sql_mode reports the session's own value.
    assert_eq!(
        row_text(session.run("SHOW VARIABLES LIKE 'sql_mode'")),
        [[
            "sql_mode".to_owned(),
            session.vars().get_system("sql_mode").unwrap()
        ]]
    );
    // Captured: a wildcard pattern matches a prefix family.
    let matched = row_text(session.run("SHOW VARIABLES LIKE 'max_allowed%'"));
    assert!(
        matched.iter().any(|row| row[0] == "max_allowed_packet"),
        "{matched:?}"
    );
    // A SET is visible to the next SHOW.
    session.run("SET autocommit = 1").unwrap();
    assert_eq!(
        row_text(session.run("SHOW VARIABLES LIKE 'autocommit'"))[0][1],
        session.vars().get_system("autocommit").unwrap()
    );

    // Captured: the scoped spellings a JDBC client sends read the same
    // session value here.
    assert_eq!(
        row_text(session.run("SELECT @@session.autocommit, @@global.autocommit")).len(),
        1
    );

    // Captured: the WHERE form filters the same virtual rows, including
    // over the Value column and with a case-insensitive column name.
    assert_eq!(
        row_text(session.run("SHOW VARIABLES WHERE variable_name = 'autocommit'"))[0][0],
        "autocommit"
    );
    let pair =
        row_text(session.run("SHOW VARIABLES WHERE Variable_name IN ('autocommit','sql_mode')"));
    assert_eq!(pair.len(), 2, "{pair:?}");
    assert_eq!(pair[0][0], "autocommit");
    assert_eq!(pair[1][0], "sql_mode");
    let both =
        row_text(session.run("SHOW VARIABLES WHERE value = 'ON' AND variable_name LIKE 'auto%'"));
    assert!(both.iter().any(|row| row[0] == "autocommit"), "{both:?}");
}

/// The three conflict policies -- `REPLACE`, `INSERT IGNORE` and
/// `ON DUPLICATE KEY UPDATE` -- checked against captured TiDB output,
/// including the affected-row counts, which is how MySQL clients tell
/// an insert from an update.
#[test]
fn insert_conflict_policies() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10), c BIGINT, UNIQUE KEY ub (b))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'p',10),(2,'q',20)")
        .unwrap();

    // Captured: an update that changes nothing affects no rows, and
    // raises no warning.
    assert_eq!(
        session
            .run("INSERT INTO t (a,b,c) VALUES (1,'p',10) ON DUPLICATE KEY UPDATE c = c")
            .unwrap(),
        StmtResult::Affected(0)
    );
    assert!(session.warnings().is_empty());

    // Captured: VALUES(c) is the value the insert would have written, and
    // a real update affects two rows.
    assert_eq!(
        session
            .run("INSERT INTO t (a,b,c) VALUES (1,'p',77) ON DUPLICATE KEY UPDATE c = VALUES(c)")
            .unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        row_text(session.run("SELECT c FROM t WHERE a = 1")),
        [["77"]]
    );

    // Captured: the conflict is found on a UNIQUE INDEX too, and the
    // assignment updates THAT row -- the candidate's own key is never
    // inserted.
    assert_eq!(
        session
            .run("INSERT INTO t (a,b,c) VALUES (9,'q',5) ON DUPLICATE KEY UPDATE c = 42")
            .unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
        [["1", "p", "77"], ["2", "q", "42"]]
    );

    // Captured: the assignments read the EXISTING row.
    assert_eq!(
        session
            .run("INSERT INTO t (a,b,c) VALUES (1,'p',1000) ON DUPLICATE KEY UPDATE c = c + 1")
            .unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        row_text(session.run("SELECT c FROM t WHERE a = 1")),
        [["78"]]
    );

    // Captured: INSERT IGNORE skips the conflicting row with a 1062
    // warning and inserts the rest.
    assert_eq!(
        session
            .run("INSERT IGNORE INTO t (a,b,c) VALUES (1,'zzz',1),(5,'five',5)")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1062);
    assert_eq!(
        session.warnings()[0].message,
        "Duplicate entry '1' for key 't.PRIMARY'"
    );

    // Captured: REPLACE deletes EVERY row it collides with -- here one on
    // the primary key and another on the unique key -- and the affected
    // count is one per deleted row plus one for the inserted row.
    assert_eq!(
        session
            .run("REPLACE INTO t (a,b,c) VALUES (2,'five',99)")
            .unwrap(),
        StmtResult::Affected(3)
    );
    assert_eq!(
        row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
        [["1", "p", "78"], ["2", "five", "99"]]
    );
    // Captured: a REPLACE with no conflict is a plain insert.
    assert_eq!(
        session
            .run("REPLACE INTO t (a,b,c) VALUES (77,'new',1)")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY a")),
        [["1"], ["2"], ["77"]]
    );
}

/// `INSERT ... SELECT` and the `ORDER BY`/`LIMIT` forms of UPDATE and
/// DELETE, checked against captured TiDB output.
///
/// STILL REFUSED, each recorded at its gate: `REPLACE INTO`,
/// `INSERT IGNORE`, `ON DUPLICATE KEY UPDATE` (all three need
/// conflict-time row replacement), the `SET` insert syntax, and
/// partitions. `RETURNING` is parsed and silently ignored, matching Go
/// (testkit probe: the write succeeds with a plain OK, no result set,
/// no warning).
#[test]
fn insert_select_and_ordered_dml() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10), c BIGINT)")
        .unwrap();
    session
        .run("CREATE TABLE u (x BIGINT, y VARCHAR(10))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'p',10),(2,'q',20),(3,'r',30)")
        .unwrap();
    session
        .run("INSERT INTO u VALUES (7,'seven'),(8,'eight')")
        .unwrap();

    // Captured: INSERT ... SELECT inserts the query's rows, and the
    // columns it does not name stay NULL.
    assert_eq!(
        session
            .run("INSERT INTO t (a,b) SELECT x, y FROM u")
            .unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
        [
            ["1", "p", "10"],
            ["2", "q", "20"],
            ["3", "r", "30"],
            ["7", "seven", "NULL"],
            ["8", "eight", "NULL"],
        ]
    );

    // Captured: UPDATE ... ORDER BY ... LIMIT updates that many rows, in
    // that order -- here the largest `a`.
    assert_eq!(
        session
            .run("UPDATE t SET c = 99 ORDER BY a DESC LIMIT 1")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        row_text(session.run("SELECT a, c FROM t ORDER BY a")),
        [
            ["1", "10"],
            ["2", "20"],
            ["3", "30"],
            ["7", "NULL"],
            ["8", "99"],
        ]
    );

    // Captured: DELETE ... ORDER BY ... LIMIT, and the WHERE + LIMIT form
    // whose cap counts rows DELETED rather than rows examined.
    assert_eq!(
        session
            .run("DELETE FROM t ORDER BY a DESC LIMIT 1")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY a")),
        [["1"], ["2"], ["3"], ["7"]]
    );
    assert_eq!(
        session.run("DELETE FROM t WHERE c > 0 LIMIT 2").unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY a")),
        [["3"], ["7"]]
    );

    // RETURNING parses but is silently ignored, exactly as in Go: the
    // planner and executor never read the AST's Returning list, so the
    // write lands and answers with a plain OK (affected rows), no result
    // set and no warning. Captured with a Go testkit probe.
    assert_eq!(
        session
            .run("INSERT INTO t (a) VALUES (42) RETURNING a")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        session
            .run("UPDATE t SET c = 0 WHERE a = 42 RETURNING a, c")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        session
            .run("DELETE FROM t WHERE a = 42 RETURNING a")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY a")),
        [["3"], ["7"]]
    );
}

/// `ORDER BY` resolved against the SELECT list, checked against captured
/// TiDB output.
///
/// A positional `ORDER BY 1` used to rewrite as a constant here, which
/// silently produced UNSORTED rows -- the worst kind of divergence, and
/// the reason this unit was picked.
#[test]
fn order_by_resolves_against_the_select_list() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
    session
        .run("INSERT INTO t VALUES (1,30),(2,20),(3,10)")
        .unwrap();

    // Captured: an alias names a projected expression.
    assert_eq!(
        row_text(session.run("SELECT a, a*2 AS twice FROM t ORDER BY twice DESC")),
        [["3", "6"], ["2", "4"], ["1", "2"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a AS z FROM t ORDER BY z DESC")),
        [["3"], ["2"], ["1"]]
    );
    // Captured: an expression BUILT on an alias resolves too.
    assert_eq!(
        row_text(session.run("SELECT a*2 AS twice FROM t ORDER BY twice+0 DESC")),
        [["6"], ["4"], ["2"]]
    );
    // Captured: a bare integer is a 1-based output position.
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY 1 DESC")),
        [["3"], ["2"], ["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a, b FROM t ORDER BY 2")),
        [["3", "10"], ["2", "20"], ["1", "30"]]
    );
    // Captured: an alias SHADOWS a real column of the same name.
    assert_eq!(
        row_text(session.run("SELECT b AS a FROM t ORDER BY a")),
        [["10"], ["20"], ["30"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a+0 AS a FROM t ORDER BY a DESC")),
        [["3"], ["2"], ["1"]]
    );
    // Captured: a source column that is not projected still sorts.
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY b DESC")),
        [["1"], ["2"], ["3"]]
    );

    // Captured: an unknown name and an out-of-range position are both
    // 1054 naming the order clause.
    for sql in [
        "SELECT a FROM t ORDER BY nosuch",
        "SELECT a FROM t ORDER BY 5",
    ] {
        match session.run(sql) {
            Err(error) => {
                let reported = error.to_mysql_error();
                assert_eq!(reported.code, 1054, "{sql}");
                assert!(
                    reported.message.ends_with("in 'order clause'"),
                    "{sql}: {}",
                    reported.message
                );
            }
            Ok(other) => panic!("expected 1054 from {sql}, got {other:?}"),
        }
    }
}

/// `GROUP BY` resolved against the SELECT list, checked against captured
/// TiDB output.
///
/// A positional `GROUP BY 1` used to rewrite as a constant here too --
/// the same silent-wrong-rows bug `ORDER BY 1` once had, but for
/// grouping: every row collapsed into one group instead of grouping by
/// the first select field.
#[test]
fn group_by_resolves_against_the_select_list() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
    session
        .run("INSERT INTO t VALUES (1,30),(1,31),(2,20),(3,10)")
        .unwrap();

    // Captured: a bare integer is a 1-based output position, grouping by
    // the first select field (`a`) -- three groups, not one.
    assert_eq!(
        row_text(session.run("SELECT a, COUNT(*) FROM t GROUP BY 1")),
        [["1", "2"], ["2", "1"], ["3", "1"]]
    );

    // Captured: a position landing on an aggregate select field is
    // ErrWrongGroupField (1056), whether or not it carries an alias.
    for sql in [
        "SELECT a, COUNT(*) FROM t GROUP BY 2",
        "SELECT a, COUNT(*) AS c FROM t GROUP BY 2",
    ] {
        match session.run(sql) {
            Err(error) => {
                let reported = error.to_mysql_error();
                assert_eq!(reported.code, 1056, "{sql}");
                assert!(
                    reported.message.starts_with("Can't group on"),
                    "{sql}: {}",
                    reported.message
                );
            }
            Ok(other) => panic!("expected 1056 from {sql}, got {other:?}"),
        }
    }

    // A positional ORDER BY on the AGGREGATE path was the same silent
    // drop: the bare integer fell through as a constant and the sort
    // never happened. `ORDER BY 2 DESC` sorts by the count.
    assert_eq!(
        row_text(session.run("SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY 2 DESC, a")),
        [["1", "2"], ["2", "1"], ["3", "1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY 1 DESC")),
        [["3", "1"], ["2", "1"], ["1", "2"]]
    );

    // Captured: an out-of-range position (including zero) is 1054 naming
    // the group statement.
    for sql in [
        "SELECT a, COUNT(*) FROM t GROUP BY 0",
        "SELECT a, COUNT(*) FROM t GROUP BY 3",
    ] {
        match session.run(sql) {
            Err(error) => {
                let reported = error.to_mysql_error();
                assert_eq!(reported.code, 1054, "{sql}");
                assert!(
                    reported.message.ends_with("in 'group statement'"),
                    "{sql}: {}",
                    reported.message
                );
            }
            Ok(other) => panic!("expected 1054 from {sql}, got {other:?}"),
        }
    }

    // An expression BUILT on a position (`1+1`) is arithmetic, not a
    // position: it groups every row into one bucket by the constant 2.
    assert_eq!(
        row_text(session.run("SELECT COUNT(*) FROM t GROUP BY 1+1")),
        [["4"]]
    );
}

/// The aggregates over each numeric domain, checked against captured
/// TiDB output.
///
/// The type is the load-bearing part: `SUM` over a BIGINT column is a
/// DECIMAL in MySQL (captured type 246), not a BIGINT, so it sums in the
/// decimal domain the way Go's `sum4Decimal` does. Only a real argument
/// makes it a DOUBLE.
#[test]
fn aggregates_over_numeric_domains() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT, d DECIMAL(10,2), r DOUBLE)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1.5,1.5),(2,2.25,2.5),(3,3.25,3.5)")
        .unwrap();

    // Captured: SUM over each domain, with the decimal column keeping
    // its own scale.
    assert_eq!(
        row_text(session.run("SELECT SUM(a), SUM(d), SUM(r) FROM t")),
        [["6", "7.00", "7.5"]]
    );
    // Captured: an empty SUM is NULL, not zero.
    assert_eq!(
        row_text(session.run("SELECT SUM(a) FROM t WHERE a > 100")),
        [["NULL"]]
    );
    // Captured: AVG and MIN/MAX over a decimal column.
    assert_eq!(
        row_text(session.run("SELECT MIN(d), MAX(d) FROM t")),
        [["1.50", "3.25"]]
    );
    assert_eq!(
        row_text(session.run("SELECT COUNT(DISTINCT a), COUNT(*) FROM t")),
        [["3", "3"]]
    );
    // Captured: grouped SUM over a decimal column.
    assert_eq!(
        row_text(session.run("SELECT a, SUM(d) FROM t GROUP BY a ORDER BY a")),
        [["1", "1.50"], ["2", "2.25"], ["3", "3.25"]]
    );
}

/// The math, conditional and TRIM builtins through the chunk executor,
/// checked against captured TiDB output -- including the result TYPES,
/// which are what size a chunk cell.
///
/// The types are the subtle part and were read off TiDB's own result
/// fields: `ABS` and `MOD` keep the argument's domain, `CEIL`/`FLOOR`
/// return an integer for an integer OR decimal argument but stay real
/// for a real one, `ROUND`/`TRUNCATE` keep the decimal domain, and the
/// transcendental functions are always real.
#[test]
fn math_and_conditional_builtins() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'x',10),(2,'y',20)")
        .unwrap();

    // Captured: ABS keeps the argument's domain.
    assert_eq!(
        row_text(session.run("SELECT ABS(-3), ABS(-3.5)")),
        [["3", "3.5"]]
    );
    // Captured: CEIL/FLOOR of a decimal are integers, and of an integer
    // are the integer itself.
    assert_eq!(
        row_text(session.run("SELECT CEIL(1.2), FLOOR(1.8), CEIL(3), FLOOR(3)")),
        [["2", "1", "3", "3"]]
    );
    // Captured: ROUND keeps the decimal domain and rounds half away from
    // zero; TRUNCATE cuts instead.
    assert_eq!(
        row_text(session.run("SELECT ROUND(1.55,1), ROUND(1.55), ROUND(2.5), TRUNCATE(1.999,2)")),
        [["1.6", "2", "3", "1.99"]]
    );
    // Captured: MOD follows its arguments.
    assert_eq!(
        row_text(session.run("SELECT MOD(7,3), MOD(7.5,3)")),
        [["1", "1.5"]]
    );
    // Captured: the always-real family.
    assert_eq!(
        row_text(session.run("SELECT POW(2,3), SQRT(9), LOG10(100)")),
        [["8", "3", "2"]]
    );
    // Captured: SIGN, CONV and CRC32.
    assert_eq!(
        row_text(session.run("SELECT SIGN(-2), CONV(255,10,16), CRC32('a')")),
        [["-1", "FF", "3904355907"]]
    );

    // Captured: GREATEST/LEAST take the merged argument type, and work
    // over strings as well as numbers.
    assert_eq!(
        row_text(session.run("SELECT GREATEST(1,2,3), LEAST(1,2,3), GREATEST('a','b')")),
        [["3", "1", "b"]]
    );
    // Captured: IF picks one branch, and NULLIF is NULL only on equality.
    assert_eq!(
        row_text(session.run("SELECT IF(1,'big','small'), NULLIF(1,1), NULLIF(1,2)")),
        [["big", "NULL", "1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a, IF(c>15,'big','small') FROM t")),
        [["1", "small"], ["2", "big"]]
    );

    // Captured: TRIM's three directions, and its implicit space.
    assert_eq!(
        row_text(session.run("SELECT TRIM(' x '), TRIM(LEADING 'x' FROM 'xxa')")),
        [["x", "a"]]
    );
    assert_eq!(
        row_text(session.run("SELECT TRIM(TRAILING 'a' FROM 'xaa'), SUBSTRING('abc',1,2)")),
        [["x", "ab"]]
    );

    // IF is lazy, so the branch not taken never runs -- a division by
    // zero there would otherwise warn.
    session.run("SELECT IF(1, 1, 1/0)").unwrap();
    assert!(session.warnings().is_empty());
}

/// `RAND(N)`/`RAND()` through the chunk executor and `ORDER BY RAND()`.
///
/// Captured from Go (`pkg/executor`, a fresh mock session, table `t(a)`
/// holding `(1),(2),(3),(4),(5)`): a constant `RAND(5)` evaluated once
/// per row of a 5-row scan produces the EXACT sequence asserted below --
/// one generator per AST occurrence, seeded once and advanced per row,
/// not reseeded. `ORDER BY RAND()` only needs to permute the rows: Go's
/// own captured order (`[4] [2] [5] [1] [3]`) is one specific shuffle
/// among the seed's many possible ones, so only the SET is checked here,
/// not the exact order.
#[test]
fn rand_constant_sequence_and_order_by_rand() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1),(2),(3),(4),(5)")
        .unwrap();

    // A constant RAND(5) evaluated on the SAME row three times in one
    // statement returns the SAME value: MySQL's docs describe RAND(N)
    // as "producing a repeatable sequence", but a single implicit row
    // draws only the sequence's first value from each of these three
    // INDEPENDENT call sites -- they agree because they share both seed
    // and position, not because they are the same generator.
    assert_eq!(
        row_text(session.run("SELECT RAND(5), RAND(5), RAND(5)")),
        [[
            "0.40613597483014313",
            "0.40613597483014313",
            "0.40613597483014313"
        ]]
    );

    // The SAME call site advances across rows, producing Go's exact
    // captured sequence.
    assert_eq!(
        row_text(session.run("SELECT RAND(5) FROM t")),
        [
            ["0.40613597483014313"],
            ["0.8745439358749836"],
            ["0.15431178561813363"],
            ["0.1479271511993624"],
            ["0.276700429876056"],
        ]
    );

    // ORDER BY RAND() must not error and must produce a permutation of
    // every row -- the unseeded sequence itself is not pinned.
    let mut rows: Vec<String> = row_text(session.run("SELECT a FROM t ORDER BY RAND()"))
        .into_iter()
        .flatten()
        .collect();
    rows.sort();
    assert_eq!(rows, ["1", "2", "3", "4", "5"]);
}

/// The date/time family through the chunk executor, checked against
/// captured TiDB output with `time_zone = '+00:00'`.
///
/// Go fixes the statement clock once, so every `NOW()` in one statement
/// agrees; the context carries that instant and the resolved session
/// zone (Go `timeutil.ParseTimeZone`).
///
/// DOCUMENTED DIVERGENCE, the same one the temporal casts carry: this
/// crate's date/time builtins produce formatted STRINGS, so the reported
/// column type is `VarString` where TiDB says `DATETIME`. The values
/// match.
/// `DATE_ADD`/`DATE_SUB`/`ADDDATE`/`SUBDATE`, `EXTRACT` and
/// `TIMESTAMPDIFF` through the CHUNK path, checked against captured TiDB
/// output with `time_zone = '+00:00'` (`pkg/executor`, a table holding
/// `('2024-01-31 10:20:30', '2024-01-31')` and
/// `('2025-03-15 23:59:59', '2025-03-15')` plus an all-NULL row).
///
/// The INTERVAL unit is a build-time keyword, not a value, so the
/// rewriter records it in the function NAME and the chunk evaluator
/// reuses the same `date_add` implementation the row path calls.
///
/// DOCUMENTED DIVERGENCE, the same one every other date/time builtin
/// here carries: the result is a formatted STRING (`VarString`) where
/// TiDB reports `DATE`/`DATETIME`. The values match.
#[test]
fn date_interval_extract_and_timestampdiff() {
    let mut session = Session::new();
    session.apply_set("SET time_zone = '+00:00'").unwrap();
    session
        .run("CREATE TABLE t (created VARCHAR(30), d VARCHAR(30))")
        .unwrap();
    session
        .run(
            "INSERT INTO t VALUES ('2024-01-31 10:20:30', '2024-01-31'), \
                 ('2025-03-15 23:59:59', '2025-03-15'), (NULL, NULL)",
        )
        .unwrap();

    // Captured: DAY arithmetic keeps the time-of-day, HOUR recomputes it
    // (and rolls the date over), and NULL propagates.
    assert_eq!(
        row_text(session.run("SELECT DATE_ADD(created, INTERVAL 1 DAY) FROM t")),
        [["2024-02-01 10:20:30"], ["2025-03-16 23:59:59"], ["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT DATE_ADD(created, INTERVAL 2 HOUR) FROM t")),
        [["2024-01-31 12:20:30"], ["2025-03-16 01:59:59"], ["NULL"]]
    );
    // Captured: the month-end CLAMP -- January 31 plus one month is
    // February 29 in a leap year, not March 3.
    assert_eq!(
        row_text(session.run("SELECT DATE_ADD(created, INTERVAL 1 MONTH) FROM t")),
        [["2024-02-29 10:20:30"], ["2025-04-15 23:59:59"], ["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT DATE_SUB(created, INTERVAL 1 DAY) FROM t")),
        [["2024-01-30 10:20:30"], ["2025-03-14 23:59:59"], ["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT DATE_SUB(created, INTERVAL 1 MONTH) FROM t")),
        [["2023-12-31 10:20:30"], ["2025-02-15 23:59:59"], ["NULL"]]
    );
    // Captured: a date-only column keeps no time component at all.
    assert_eq!(
        row_text(session.run("SELECT DATE_SUB(d, INTERVAL 1 MONTH) FROM t")),
        [["2023-12-31"], ["2025-02-15"], ["NULL"]]
    );

    // Captured: ADDDATE/SUBDATE's bare-number form is exactly the DAY
    // interval, and their explicit INTERVAL form agrees with it.
    assert_eq!(
        row_text(session.run("SELECT ADDDATE(d, 5), SUBDATE(d, 5) FROM t")),
        [
            ["2024-02-05", "2024-01-26"],
            ["2025-03-20", "2025-03-10"],
            ["NULL", "NULL"]
        ]
    );
    assert_eq!(
        row_text(session.run("SELECT ADDDATE(d, INTERVAL 5 DAY) FROM t")),
        [["2024-02-05"], ["2025-03-20"], ["NULL"]]
    );

    // Captured: EXTRACT of a simple unit is the same function that unit
    // already names.
    assert_eq!(
        row_text(session.run(
            "SELECT EXTRACT(YEAR FROM created), EXTRACT(MONTH FROM created), \
                 EXTRACT(DAY FROM d), EXTRACT(HOUR FROM created) FROM t"
        )),
        [
            ["2024", "1", "31", "10"],
            ["2025", "3", "15", "23"],
            ["NULL", "NULL", "NULL", "NULL"]
        ]
    );

    // Captured: TIMESTAMPDIFF counts WHOLE units -- January 31 to March 1
    // is 30 days but only 1 whole month, and a month whose day-of-month
    // is reached but whose clock time is not counts as 0.
    assert_eq!(
        row_text(session.run(
            "SELECT TIMESTAMPDIFF(DAY, '2024-01-31', '2024-03-01'), \
                 TIMESTAMPDIFF(MONTH, '2024-01-31', '2024-03-01')"
        )),
        [["30", "1"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT TIMESTAMPDIFF(MONTH, '2024-01-31 10:00:00', '2024-02-29 09:00:00'), \
                 TIMESTAMPDIFF(HOUR, '2024-01-31 10:00:00', '2024-02-01 09:00:00')"
        )),
        [["0", "23"]]
    );
    assert_eq!(
        row_text(session.run("SELECT TIMESTAMPDIFF(YEAR, d, created) FROM t")),
        [["0"], ["0"], ["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT TIMESTAMPDIFF(DAY, NULL, '2024-01-01')")),
        [["NULL"]]
    );

    // Captured: a filter is the same expression in predicate position.
    assert_eq!(
        row_text(
            session
                .run("SELECT d FROM t WHERE created >= DATE_SUB('2025-01-01', INTERVAL 1 MONTH)")
        ),
        [["2025-03-15"]]
    );

    // Captured: an unparseable calendar date and a NULL amount are both
    // NULL, not an error.
    assert_eq!(
        row_text(session.run("SELECT DATE_ADD('2024-02-30', INTERVAL 1 DAY)")),
        [["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT DATE_ADD(created, INTERVAL NULL DAY) FROM t LIMIT 1")),
        [["NULL"]]
    );

    // Composite units -- ported from `parseTimeValue`/
    // `ExtractDatetimeNum` (`pkg/types/time.go`); captured against
    // `pkg/executor`: `'2024-01-31 10:20:30' + INTERVAL '1:30'
    // HOUR_MINUTE` is `2024-01-31 11:50:30`, and `EXTRACT(HOUR_MINUTE
    // FROM '2024-01-31 10:20:30')` is `1020`. Both the row path
    // (`time_fn::calendar::date_add`/`extract_composite`) and the chunk
    // rewriter build these now.
    assert_eq!(
        row_text(session.run("SELECT DATE_ADD(created, INTERVAL '1:30' HOUR_MINUTE) FROM t")),
        [["2024-01-31 11:50:30"], ["2025-03-16 01:29:59"], ["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT EXTRACT(HOUR_MINUTE FROM created) FROM t")),
        [["1020"], ["2359"], ["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT EXTRACT(DAY_SECOND FROM created) FROM t")),
        [["31102030"], ["15235959"], ["NULL"]]
    );
}

#[test]
fn date_time_builtins() {
    let mut session = Session::new();
    session.apply_set("SET time_zone = '+00:00'").unwrap();
    session.run("CREATE TABLE t (d VARCHAR(30))").unwrap();
    session
        .run("INSERT INTO t VALUES ('2020-03-05 06:07:08')")
        .unwrap();

    // Captured: the field extractors.
    assert_eq!(
            row_text(session.run(
                "SELECT MONTH(d), DAY(d), YEAR(d), DAYOFWEEK(d), DAYOFYEAR(d), WEEKDAY(d), QUARTER(d) FROM t"
            )),
            [["3", "5", "2020", "5", "65", "3", "1"]]
        );
    assert_eq!(
        row_text(session.run(
            "SELECT MONTHNAME(d), DAYNAME(d), LAST_DAY(d), TO_DAYS(d), TIME_TO_SEC(d) FROM t"
        )),
        [["March", "Thursday", "2020-03-31", "737854", "22028"]]
    );
    assert_eq!(
        row_text(session.run("SELECT WEEK(d), WEEKOFYEAR(d), YEARWEEK(d) FROM t")),
        [["9", "10", "202009"]]
    );
    assert_eq!(
        row_text(session.run("SELECT SEC_TO_TIME(3661), MAKEDATE(2020,10), MAKETIME(1,2,3)")),
        [["01:01:01", "2020-01-10", "01:02:03"]]
    );
    assert_eq!(
        row_text(session.run("SELECT PERIOD_ADD(202001, 2), PERIOD_DIFF(202003, 202001)")),
        [["202003", "2"]]
    );

    // Captured: the statement clock is fixed, so NOW() agrees with
    // itself and prints a full second-resolution datetime.
    assert_eq!(
        row_text(session.run("SELECT NOW() = NOW(), LENGTH(NOW()) = 19")),
        [["1", "1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CURDATE() = CURDATE(), LENGTH(CURDATE()) = 10")),
        [["1", "1"]]
    );

    // The session zone reaches the clock: UTC and a +10 offset differ by
    // ten hours in the hour NOW() reports for the same instant.
    let hour_at = |session: &mut Session, zone: &str| -> i64 {
        session
            .apply_set(&format!("SET time_zone = '{zone}'"))
            .unwrap();
        match session.run("SELECT HOUR(NOW())").unwrap() {
            StmtResult::Rows(rows) => datum_text(&rows[0][0]).unwrap().parse().unwrap(),
            other => panic!("expected rows, got {other:?}"),
        }
    };
    let utc = hour_at(&mut session, "+00:00");
    let plus_ten = hour_at(&mut session, "+10:00");
    assert_eq!((utc + 10) % 24, plus_ten);
}

/// `CAST(expr AS type)` and its `CONVERT`/`BINARY` spellings through the
/// chunk executor, checked against captured TiDB output.
///
/// The target type IS the operation in Go (it picks a
/// `builtinCast*As*Sig` from it), so the rewriter puts the target in the
/// function's result type and evaluation reads it back from there.
///
/// STILL REFUSED, for the reason `cast::eval_cast` already records:
/// `TIME` and `JSON` targets have no value domain in this crate, and the
/// `ARRAY` modifier is a JSON multi-valued index.
#[test]
fn cast_and_convert() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'12abc',10),(2,'zz',20)")
        .unwrap();

    // Captured: a number to CHAR, and the width truncating it.
    assert_eq!(
        row_text(session.run("SELECT CAST(c AS CHAR) FROM t")),
        [["10"], ["20"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST(c AS CHAR(1)) FROM t")),
        [["1"], ["2"]]
    );

    // Captured: a string to a number takes the leading digits, or zero.
    assert_eq!(
        row_text(session.run("SELECT CAST(b AS SIGNED) FROM t")),
        [["12"], ["0"]]
    );
    // Captured: the rounding asymmetry -- a string keeps only the integer
    // prefix while a decimal or a float rounds.
    assert_eq!(
        row_text(session.run("SELECT CAST('3.7' AS SIGNED), CAST(3.7 AS SIGNED)")),
        [["3", "4"]]
    );
    // Captured: UNSIGNED wraps a negative rather than clamping it.
    assert_eq!(
        row_text(session.run("SELECT CAST(-1 AS UNSIGNED)")),
        [["18446744073709551615"]]
    );

    // Captured: DECIMAL rounds to the written scale, and pads to it.
    assert_eq!(
        row_text(session.run("SELECT CAST('12.345' AS DECIMAL(6,2))")),
        [["12.35"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST(1 AS DECIMAL(6,2))")),
        [["1.00"]]
    );

    // Captured: the temporal targets.
    assert_eq!(
        row_text(session.run("SELECT CAST('2020-01-02' AS DATE)")),
        [["2020-01-02"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST('2020-1-2' AS DATE)")),
        [["2020-01-02"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST('2020-01-02 03:04:05' AS DATETIME)")),
        [["2020-01-02 03:04:05"]]
    );

    // Captured: BINARY(n) pads with NUL rather than truncating short.
    assert_eq!(
        row_text(session.run("SELECT CAST(b AS BINARY(3)) FROM t")),
        [["12a"], ["zz\u{0}"]]
    );

    // Captured: CONVERT and the BINARY operator are the same node.
    assert_eq!(
        row_text(session.run("SELECT CONVERT(c, CHAR), CONVERT('7', SIGNED) FROM t")),
        [["10", "7"], ["20", "7"]]
    );
    assert_eq!(
        row_text(session.run("SELECT BINARY b FROM t")),
        [["12abc"], ["zz"]]
    );

    // Captured: NULL casts to NULL, and a cast result is an ordinary
    // operand afterwards.
    assert_eq!(
        row_text(session.run("SELECT CAST(NULL AS SIGNED) IS NULL")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST(c AS DOUBLE)/2 FROM t")),
        [["5"], ["10"]]
    );

    // The JSON target produces this tier's canonical JSON text -- see
    // `json_value_functions` for the whole slice and its divergence note.
    assert_eq!(
        row_text(session.run("SELECT CAST(c AS JSON) FROM t")),
        [["10"], ["20"]]
    );

    // The refusals are refusals, not wrong answers.
    assert!(session.run("SELECT CAST(c AS TIME) FROM t").is_err());
}

/// LIKE, BETWEEN, CASE and the ordinary builtins through the chunk
/// executor, checked against captured TiDB output.
///
/// These forms all existed in `tidb_expr`'s AST evaluator already; what
/// was missing was the rewriter building them for chunk evaluation, so a
/// query using any of them failed outright.
///
/// STILL REFUSED, each for its own reason recorded at
/// `tidb_expr::rewriter::builtin_return_type`: the session-state
/// functions (`DATABASE`, `VERSION`, `CURRENT_USER`, `NOW`) need a
/// resolver carrying session state into the chunk path, `CAST`/`CONVERT`
/// take a target type rather than a value, `GROUP_CONCAT` is an
/// aggregate, and the `DATE_ADD` family takes an `Expr::Interval`.
#[test]
fn like_between_case_and_builtins() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT, KEY kb (b))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'xy',10),(2,'Yz',20),(3,'z',30)")
        .unwrap();

    // Captured: LIKE's wildcards, its negation and its escape.
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b LIKE 'x%'")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b LIKE '%y%'")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b LIKE 'x_'")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b NOT LIKE 'x%'")),
        [["2"], ["3"]]
    );
    assert_eq!(row_text(session.run(r"SELECT 'a%b' LIKE 'a\%b'")), [["1"]]);
    assert_eq!(
        row_text(session.run("SELECT b FROM t WHERE b LIKE '%'")),
        [["xy"], ["Yz"], ["z"]]
    );

    // Captured: BETWEEN is inclusive, and its negation is the complement.
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE c BETWEEN 10 AND 20")),
        [["1"], ["2"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE c NOT BETWEEN 10 AND 20")),
        [["3"]]
    );

    // Captured: the searched CASE, the simple CASE, a NULL condition
    // (which is not a match), and a missing ELSE (which is NULL).
    assert_eq!(
        row_text(session.run("SELECT a, CASE WHEN c > 15 THEN 'hi' ELSE 'lo' END FROM t")),
        [["1", "lo"], ["2", "hi"], ["3", "hi"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CASE c WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' END FROM t")),
        [["ten"], ["twenty"], ["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CASE WHEN NULL THEN 'x' ELSE 'y' END")),
        [["y"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CASE WHEN c > 100 THEN 'x' END FROM t")),
        [["NULL"], ["NULL"], ["NULL"]]
    );

    // Captured: the string builtins, including LENGTH counting bytes
    // while CHAR_LENGTH counts characters.
    assert_eq!(
        row_text(
            session
                .run("SELECT CONCAT(b,'!'), UPPER(b), LOWER(b), LENGTH(b), CHAR_LENGTH(b) FROM t")
        ),
        [
            ["xy!", "XY", "xy", "2", "2"],
            ["Yz!", "YZ", "yz", "2", "2"],
            ["z!", "Z", "z", "1", "1"],
        ]
    );
    assert_eq!(
        row_text(session.run("SELECT LENGTH('héllo'), CHAR_LENGTH('héllo')")),
        [["6", "5"]]
    );

    // Captured: COALESCE and IFNULL over a column and a literal, whose
    // branch types Go merges to one string type.
    assert_eq!(
        row_text(session.run("SELECT COALESCE(NULL, b), IFNULL(b,'n'), IFNULL(NULL,'n') FROM t")),
        [["xy", "xy", "n"], ["Yz", "Yz", "n"], ["z", "z", "n"],]
    );

    // Captured: DATABASE() and its SCHEMA() synonym report the current
    // database, and VERSION() reports the same string as @@version.
    assert_eq!(
        row_text(session.run("SELECT DATABASE(), SCHEMA()")),
        [["test", "test"]]
    );
    let version = match session.run("SELECT VERSION()").unwrap() {
        StmtResult::Rows(rows) => datum_text(&rows[0][0]).unwrap(),
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(version, session.vars().get_system("version").unwrap());
    assert!(version.contains("TiDB"), "{version}");
    // Captured: with no database selected, DATABASE() is NULL.
    let mut fresh = Session::new();
    fresh.run("DROP DATABASE test").unwrap();
    assert_eq!(row_text(fresh.run("SELECT DATABASE()")), [["NULL"]]);

    // A session with no authenticated user answers NULL for the identity
    // builtins, which is what Go does for a session without one; a front
    // end that authenticates sets it (see the server's client test).
    assert_eq!(
        row_text(session.run("SELECT CURRENT_USER(), USER()")),
        [["NULL", "NULL"]]
    );
    session.set_user("bob@%".to_owned(), "bob@10.0.0.1".to_owned());
    assert_eq!(
        row_text(session.run("SELECT CURRENT_USER(), USER(), SESSION_USER()")),
        [["bob@%", "bob@10.0.0.1", "bob@10.0.0.1"]]
    );

    // CONNECTION_ID() is NULL until a front end attaches one (Go itself
    // errors here rather than reporting NULL, but that path is
    // unreachable in practice -- see `Columns::connection_id`'s doc); once
    // set, the same value keeps reporting on later statements.
    assert_eq!(row_text(session.run("SELECT CONNECTION_ID()")), [["NULL"]]);
    session.set_connection_id(42);
    assert_eq!(row_text(session.run("SELECT CONNECTION_ID()")), [["42"]]);
    assert_eq!(row_text(session.run("SELECT CONNECTION_ID()")), [["42"]]);

    // The refusals above are refusals, not wrong answers. (CAST,
    // GROUP_CONCAT, CURRENT_USER, GROUP_CONCAT's inner ORDER BY, and
    // multi-argument GROUP_CONCAT were each this example in turn; all of
    // them work now.) `COUNT(b, a)` without DISTINCT stays refused, but as
    // a parser-level SQL syntax error, not a driver limitation: captured
    // from TiDB, `COUNT(a, b)` is only valid SQL as `COUNT(DISTINCT a,
    // b)` (see `multi_argument_count` below) -- the grammar itself
    // rejects the non-DISTINCT, multi-argument form.
    assert!(session.run("SELECT COUNT(b, a) FROM t").is_err());
}

/// `COUNT(a, b, ...)` / `COUNT(DISTINCT a, b, ...)`, checked against
/// captured TiDB output. Only the `DISTINCT` form is valid SQL for more
/// than one argument (`pkg/parser` rejects a bare `COUNT(a, b)` at parse
/// time, matched by `tidb_parser`'s own `parse_aggregate`), so this test
/// only has `COUNT(DISTINCT ...)` to exercise: a row counts only when
/// EVERY argument is non-NULL, and DISTINCT dedupes over the whole
/// argument tuple rather than a single column.
#[test]
fn multi_argument_count() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (g INT, a INT, b INT)").unwrap();
    session
        .run(
            "INSERT INTO t VALUES \
                 (1, 1, 1), (1, 1, 1), (1, 1, NULL), (1, NULL, 1), (1, NULL, NULL), \
                 (2, 2, 2), (2, 2, 2), (2, 3, 3)",
        )
        .unwrap();

    // Captured: `count(distinct a, b)` over the whole table sees three
    // distinct non-NULL pairs -- (1,1), (2,2), (3,3) -- with every row
    // that has a NULL in either column excluded entirely.
    assert_eq!(
        row_text(session.run("SELECT COUNT(DISTINCT a, b) FROM t")),
        [["3"]]
    );
    // Captured: grouped, group 1 has one distinct non-NULL pair (1,1)
    // (its NULL-containing rows don't count), group 2 has two: (2,2) and
    // (3,3).
    assert_eq!(
        row_text(session.run("SELECT g, COUNT(DISTINCT a, b) FROM t GROUP BY g ORDER BY g")),
        [["1", "1"], ["2", "2"]]
    );
}

/// `[NOT] REGEXP` through the chunk (table-scan `WHERE`) path, checked
/// against captured TiDB output. Before this test, the chunk rewriter had
/// no `Expr::Regexp` arm, so `SELECT ... WHERE b REGEXP '...'` failed
/// even though the same expression worked as a bare `SELECT`.
#[test]
fn regexp_through_the_chunk_path() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'abc'),(2,'xyz'),(3,NULL)")
        .unwrap();

    // Captured: `abc` matches `^a`, `xyz` and the NULL row do not.
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b REGEXP '^a'")),
        [["1"]]
    );
    // Captured: NOT REGEXP is the complement, still excluding the NULL
    // row -- a NULL operand is never TRUE for either polarity.
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b NOT REGEXP '^a'")),
        [["2"]]
    );
    // Captured: a bare SELECT REGEXP still works (the row path this
    // reused already handled it), so both paths agree.
    assert_eq!(row_text(session.run("SELECT 'abc' REGEXP '^a'")), [["1"]]);
    assert_eq!(
        row_text(session.run("SELECT 'abc' NOT REGEXP '^a'")),
        [["0"]]
    );
    // Captured: NULL propagates from either operand.
    assert_eq!(row_text(session.run("SELECT NULL REGEXP '^a'")), [["NULL"]]);
    assert_eq!(
        row_text(session.run("SELECT 'abc' REGEXP NULL")),
        [["NULL"]]
    );
    // Captured: an invalid pattern is a query error, not a NULL/false
    // result -- `[expression:1139]Got error 'error parsing regexp:
    // missing closing ): `(`' from regexp`.
    assert!(session.run("SELECT 'abc' REGEXP '('").is_err());
}

/// `MAKE_SET` regression, checked against mock TiDB. `1|4` evaluates to
/// the UNSIGNED domain, which used to fall through the builtin's
/// `Datum::Int`-only match and answer NULL instead of `'a,c'`.
#[test]
fn make_set_accepts_a_bitwise_or_result() {
    let mut session = Session::new();
    assert_eq!(
        row_text(session.run("SELECT MAKE_SET(1|4,'a','b','c')")),
        [["a,c"]]
    );
    assert_eq!(
        row_text(session.run("SELECT MAKE_SET(0,'a','b','c')")),
        [[""]]
    );
    assert_eq!(
        row_text(session.run("SELECT MAKE_SET(NULL,'a','b','c')")),
        [["NULL"]]
    );
    // A NULL string argument is skipped, not propagated.
    assert_eq!(
        row_text(session.run("SELECT MAKE_SET(1,'a',NULL,'c')")),
        [["a"]]
    );
    // More set bits than strings simply has nothing left to match.
    assert_eq!(
        row_text(session.run("SELECT MAKE_SET(31,'a','b','c')")),
        [["a,b,c"]]
    );
}

/// Go `getDefaultValue` + `checkDefaultValue`: a written DEFAULT is
/// normalized and checked against the column's own type at DDL time,
/// checked against captured TiDB output.
///
/// NOT PORTED: the function-call defaults (`CURRENT_TIMESTAMP`), the
/// ENUM/SET forms with their own index rules, and BIT columns -- each is
/// its own arm of Go's `getDefaultValue` and none of those column types
/// reaches this tier yet.
#[test]
fn column_default_is_normalized_and_checked() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (a BIGINT, d DECIMAL(10,3) DEFAULT 1.5, \
                 i INT DEFAULT 7.6, v VARCHAR(4) DEFAULT 'ab')",
        )
        .unwrap();

    // Captured: only the integer and float/double types normalize their
    // stored default, so SHOW CREATE reports 8 for the INT column while
    // the DECIMAL column keeps the literal as written.
    let created = show_create(&mut session, "t");
    assert!(
        created.contains("`d` decimal(10,3) DEFAULT '1.5'"),
        "{created}"
    );
    assert!(created.contains("`i` int(11) DEFAULT '8'"), "{created}");
    assert!(created.contains("`v` varchar(4) DEFAULT 'ab'"), "{created}");

    // Captured: a row that takes the defaults casts them to the column,
    // so the decimal reaches the column's own scale here.
    session.run("INSERT INTO t (a) VALUES (1)").unwrap();
    assert_eq!(
        row_text(session.run("SELECT a, d, i, v FROM t")),
        [["1", "1.500", "8", "ab"]]
    );

    // Captured: a default the column cannot hold is 1067 at DDL time.
    for sql in [
        "CREATE TABLE w (v VARCHAR(4) DEFAULT 'abcdefg')",
        "CREATE TABLE x (i INT DEFAULT 'zz')",
    ] {
        match session.run(sql) {
            Err(error) => {
                let reported = error.to_mysql_error();
                assert_eq!(reported.code, 1067, "{sql}");
                assert!(
                    reported.message.starts_with("Invalid default value for "),
                    "{sql}: {}",
                    reported.message
                );
            }
            Ok(other) => panic!("expected 1067 from {sql}, got {other:?}"),
        }
    }
    // A numeric string a column CAN hold is accepted and kept.
    session.run("CREATE TABLE y (i INT DEFAULT '12')").unwrap();
    session
        .run("INSERT INTO y (i) VALUES (DEFAULT)")
        .unwrap_or_else(|_| {
            // `VALUES (DEFAULT)` is not parsed at this tier; an omitted
            // column takes the same path.
            session.run("INSERT INTO y () VALUES ()").unwrap()
        });
    assert_eq!(row_text(session.run("SELECT i FROM y")), [["12"]]);

    // Captured: ALTER TABLE ADD COLUMN runs the same normalization and
    // check, and existing rows read the cast default.
    session
        .run("ALTER TABLE t ADD COLUMN e DECIMAL(6,2) DEFAULT 3.14159")
        .unwrap();
    let created = show_create(&mut session, "t");
    assert!(
        created.contains("`e` decimal(6,2) DEFAULT '3.14159'"),
        "{created}"
    );
    assert_eq!(row_text(session.run("SELECT e FROM t")), [["3.14"]]);
    assert!(matches!(
        session.run("ALTER TABLE t ADD COLUMN f VARCHAR(2) DEFAULT 'toolong'"),
        Err(DriverError::InvalidDefault(_))
    ));
}

/// Go `table.CastValue`: a written value takes its column's type, checked
/// against captured TiDB output.
///
/// NOT PORTED from Go's own suites: the temporal columns (a DATE/DATETIME
/// column's zero-date handling is its own error path), ENUM/SET, and the
/// `INSERT IGNORE` form, which Go treats like a non-strict mode.
#[test]
fn insert_casts_to_column_type() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (d DECIMAL(10,3), i INT, v VARCHAR(4))")
        .unwrap();

    // Captured: a decimal rounds to the column's scale, a float rounds to
    // the integer column, and a numeric string parses.
    session
        .run("INSERT INTO t VALUES (1.23456, 7.6, 'ab')")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT d, i, v FROM t")),
        [["1.235", "8", "ab"]]
    );
    assert!(session.warnings().is_empty());
    session.run("INSERT INTO t (i) VALUES ('12')").unwrap();
    assert_eq!(row_text(session.run("SELECT i FROM t")), [["8"], ["12"]]);

    // Captured: under the default strict mode a value that does not fit
    // fails the statement, and the row is not written.
    assert!(matches!(
        session.run("INSERT INTO t (v) VALUES ('abcdefg')"),
        Err(DriverError::DataTooLong { row: 1, .. })
    ));
    assert!(matches!(
        session.run("INSERT INTO t (i) VALUES ('x')"),
        Err(DriverError::IncorrectValue { row: 1, .. })
    ));
    assert_eq!(row_text(session.run("SELECT i FROM t")).len(), 2);
    // The failure is reported with Go's own message.
    match session.run("INSERT INTO t (i) VALUES ('x')") {
        Err(error) => {
            let reported = error.to_mysql_error();
            assert_eq!(reported.code, 1366);
            assert_eq!(
                reported.message,
                "Incorrect int value: 'x' for column 'i' at row 1"
            );
        }
        Ok(other) => panic!("expected a failure, got {other:?}"),
    }

    // Captured: UPDATE casts an assigned value the same way.
    session.run("UPDATE t SET d = 9.87654 WHERE i = 8").unwrap();
    assert_eq!(
        row_text(session.run("SELECT d FROM t WHERE i = 8")),
        [["9.877"]]
    );
    assert!(matches!(
        session.run("UPDATE t SET v = 'abcdefg' WHERE i = 8"),
        Err(DriverError::DataTooLong { .. })
    ));

    // Captured: without a strict mode the converted value is stored and
    // the same message is a warning -- the string truncates to the
    // column's width and an unparseable number becomes zero.
    session.apply_set("SET sql_mode = ''").unwrap();
    session.run("INSERT INTO t (v) VALUES ('abcdefg')").unwrap();
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1406);
    assert_eq!(
        session.warnings()[0].message,
        "Data too long for column 'v' at row 1"
    );
    session.run("INSERT INTO t (i) VALUES ('x')").unwrap();
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1366);
    assert_eq!(
        row_text(session.run("SELECT v FROM t")),
        [["ab"], ["NULL"], ["abcd"], ["NULL"]]
    );
}

/// Decimal, hex and bit literals through the whole session path, checked
/// against captured TiDB output.
///
/// NOT PORTED: `-2.750` is one literal token in Go's parser, so its type
/// carries the sign in its flen; this AST keeps the sign as a unary minus
/// over the literal, so the sign shapes the value but not the literal's
/// own flen. The printed value is the same.
#[test]
fn numeric_literals() {
    let mut session = Session::new();

    // Captured: a decimal literal keeps its written scale.
    assert_eq!(row_text(session.run("SELECT 1.5")), [["1.5"]]);
    assert_eq!(row_text(session.run("SELECT 0.10")), [["0.10"]]);
    assert_eq!(row_text(session.run("SELECT -2.750")), [["-2.750"]]);

    // Captured: decimal arithmetic keeps the wider scale, and division by
    // zero is still NULL plus a warning.
    assert_eq!(row_text(session.run("SELECT 1.5 + 1")), [["2.5"]]);
    assert_eq!(row_text(session.run("SELECT 1.5 * 2")), [["3.0"]]);
    assert_eq!(
        session.run("SELECT 1.5 / 0").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Null]])
    );
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1365);

    // Captured: a decimal comparison against an integer.
    assert_eq!(row_text(session.run("SELECT 1.5 > 1")), [["1"]]);

    // Captured: DIV and MOD truncate toward zero.
    assert_eq!(
        row_text(session.run("SELECT 7 DIV 2, 7 MOD 2, -7 DIV 2")),
        [["3", "1", "-3"]]
    );

    // Captured: a hex or bit literal prints as its bytes.
    assert_eq!(row_text(session.run("SELECT 0x41")), [["A"]]);
    assert_eq!(row_text(session.run("SELECT x'4142'")), [["AB"]]);
    assert_eq!(row_text(session.run("SELECT b'1010'")), [["\n"]]);

    // Captured: and reads as a number in arithmetic.
    assert_eq!(row_text(session.run("SELECT 0x41 + 0")), [["65"]]);
    assert_eq!(row_text(session.run("SELECT b'1010' + 0")), [["10"]]);

    // A decimal literal reaches a stored decimal column and compares
    // against it.
    session.run("CREATE TABLE t (d DECIMAL(10,3))").unwrap();
    session.run("INSERT INTO t VALUES (1.5), (2.25)").unwrap();
    assert_eq!(
        row_text(session.run("SELECT d FROM t WHERE d > 1.4")),
        [["1.500"], ["2.250"]]
    );
}

/// Division by zero, checked against captured TiDB output.
///
/// The value is `NULL` in every case; what the SQL mode decides is whether
/// the statement also warns, fails, or stays silent.
///
/// NOT PORTED from Go's own suites: the coprocessor's own warning
/// merging. TiDB pushes a `WHERE a/0 IS NULL` filter down and reports ONE
/// warning for all the rows a region produced, while three zero divisors
/// in a projection give three warnings; this tier has no coprocessor
/// boundary, so it reports one warning per evaluation everywhere.
#[test]
fn division_by_zero() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();

    // Captured: a query returns NULL and warns 1365.
    assert_eq!(
        session.run("SELECT 1 / 0").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Null]])
    );
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1365);
    assert_eq!(session.warnings()[0].message, "Division by 0");
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        [[
            "Warning".to_owned(),
            "1365".to_owned(),
            "Division by 0".to_owned()
        ]]
    );

    // Captured: every zero divisor raises its own warning.
    assert_eq!(
        session.run("SELECT 1 / 0, 2 / 0").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Null, Datum::Null]])
    );
    assert_eq!(session.warnings().len(), 2);
    // DEFERRED (pre-existing rewriter gaps, not this channel's): `DIV`,
    // `MOD` and a decimal literal operand reach the same zero-divisor
    // check in `ops.rs`, but the rewriter does not build those expression
    // forms yet, so they cannot be asserted through the session here.

    // Captured: a zero dividend is ordinary arithmetic, not this case.
    session.run("SELECT 0 / 1").unwrap();
    assert!(session.warnings().is_empty());

    // Captured: under the default SQL mode an INSERT fails with 1365 and
    // writes nothing.
    assert!(matches!(
        session.run("INSERT INTO t VALUES (1 / 0, 1)"),
        Err(DriverError::Exec(tidb_executor::ExecError::Eval(
            tidb_executor::EvalError::DivisionByZero
        )))
    ));
    assert_eq!(
        session.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![])
    );

    // The same holds for UPDATE and DELETE, which Go gives the same level.
    session.run("INSERT INTO t VALUES (1, 1)").unwrap();
    assert!(session.run("UPDATE t SET a = a / 0").is_err());
    assert_eq!(
        session.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );
    assert!(session.run("DELETE FROM t WHERE a = 1 / 0").is_err());
    assert_eq!(
        session.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    // Captured: without ERROR_FOR_DIVISION_BY_ZERO the condition is
    // ignored entirely -- NULL is written, with no warning at all.
    session.apply_set("SET sql_mode = ''").unwrap();
    session.run("INSERT INTO t VALUES (1 / 0, 2)").unwrap();
    assert!(session.warnings().is_empty());
    assert_eq!(
        session.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Null]])
    );
    // Captured: a strict mode without that flag ignores it too.
    session
        .apply_set("SET sql_mode = 'STRICT_TRANS_TABLES'")
        .unwrap();
    session.run("INSERT INTO t VALUES (1 / 0, 3)").unwrap();
    assert!(session.warnings().is_empty());

    // Non-strict with the flag warns instead of failing.
    session
        .apply_set("SET sql_mode = 'ERROR_FOR_DIVISION_BY_ZERO'")
        .unwrap();
    session.run("INSERT INTO t VALUES (1 / 0, 4)").unwrap();
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1365);

    // A query keeps warning whatever the SQL mode says.
    session.apply_set("SET sql_mode = ''").unwrap();
    session.run("SELECT 1 / 0").unwrap();
    assert_eq!(session.warnings().len(), 1);
}

/// `GROUP BY ... WITH ROLLUP`, checked against captured TiDB output.
///
/// Go's hash aggregation over Expand emits rollup rows in a
/// NONDETERMINISTIC order (verified: the captured order changed across
/// runs of the same query), so without `ORDER BY` only the row MULTISET
/// is contractual. This tier's deterministic order is: full groups in
/// first-seen order, then each shorter prefix's subtotals, then the
/// grand total. The `ORDER BY` cases below match captured TiDB output
/// row for row.
#[test]
fn with_rollup() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT, b BIGINT, c BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1,10),(1,2,20),(2,1,30),(2,2,40),(1,1,5)")
        .unwrap();

    // Two-column rollup: every prefix (a,b), (a), () gets aggregate rows,
    // with the rolled-up columns NULL. Multiset captured from TiDB.
    assert_eq!(
        row_text(session.run("SELECT a, b, SUM(c) FROM t GROUP BY a, b WITH ROLLUP")),
        [
            ["1", "1", "15"],
            ["1", "2", "20"],
            ["2", "1", "30"],
            ["2", "2", "40"],
            ["1", "NULL", "35"],
            ["2", "NULL", "70"],
            ["NULL", "NULL", "105"],
        ]
    );
    // Single-column rollup.
    assert_eq!(
        row_text(session.run("SELECT a, SUM(c) FROM t GROUP BY a WITH ROLLUP")),
        [["1", "35"], ["2", "70"], ["NULL", "105"]]
    );
    // COUNT(*) counts the replicated rows per grouping set.
    assert_eq!(
        row_text(session.run("SELECT a, b, COUNT(*) FROM t GROUP BY a, b WITH ROLLUP")),
        [
            ["1", "1", "2"],
            ["1", "2", "1"],
            ["2", "1", "1"],
            ["2", "2", "1"],
            ["1", "NULL", "3"],
            ["2", "NULL", "2"],
            ["NULL", "NULL", "5"],
        ]
    );
    // AVG: captured scale is 4 (decimal AVG over BIGINT).
    assert_eq!(
        row_text(session.run("SELECT a, b, AVG(c) FROM t GROUP BY a, b WITH ROLLUP")),
        [
            ["1", "1", "7.5000"],
            ["1", "2", "20.0000"],
            ["2", "1", "30.0000"],
            ["2", "2", "40.0000"],
            ["1", "NULL", "11.6667"],
            ["2", "NULL", "35.0000"],
            ["NULL", "NULL", "21.0000"],
        ]
    );
    // Captured row for row: ORDER BY sorts NULL first, so the grand
    // total leads and each subtotal precedes its group's rows.
    assert_eq!(
        row_text(session.run("SELECT a, b, SUM(c) FROM t GROUP BY a, b WITH ROLLUP ORDER BY a, b")),
        [
            ["NULL", "NULL", "105"],
            ["1", "NULL", "35"],
            ["1", "1", "15"],
            ["1", "2", "20"],
            ["2", "NULL", "70"],
            ["2", "1", "30"],
            ["2", "2", "40"],
        ]
    );
    assert_eq!(
        row_text(session.run("SELECT a, SUM(c) FROM t GROUP BY a WITH ROLLUP ORDER BY a")),
        [["NULL", "105"], ["1", "35"], ["2", "70"]]
    );

    // A genuinely-NULL data value is indistinguishable from a rollup
    // NULL in the output, exactly as in TiDB: a=1 has rows (b=1,c=10)
    // and (b=NULL,c=20), so both the data group [1 NULL 20] and the
    // subtotal [1 NULL 30] appear (captured). Only GROUPING() tells them
    // apart -- see `grouping_with_rollup`.
    session
        .run("CREATE TABLE tn (a BIGINT, b BIGINT, c BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO tn VALUES (1,1,10),(1,NULL,20),(NULL,1,30),(2,2,40)")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT a, b, SUM(c) FROM tn GROUP BY a, b WITH ROLLUP")),
        [
            ["1", "1", "10"],
            ["1", "NULL", "20"],
            ["NULL", "1", "30"],
            ["2", "2", "40"],
            ["1", "NULL", "30"],
            ["NULL", "NULL", "30"],
            ["2", "NULL", "40"],
            ["NULL", "NULL", "100"],
        ]
    );

    // Deferred: a non-column grouping expression cannot be NULLed at the
    // source, so it is refused rather than answered wrongly.
    assert!(matches!(
        session.run("SELECT a+1, SUM(c) FROM t GROUP BY a+1 WITH ROLLUP"),
        Err(DriverError::Unsupported(_))
    ));

    // An empty source yields no rows at all -- not even the grand total
    // -- because Expand replicates zero rows (unlike a scalar aggregate).
    session.run("DELETE FROM t").unwrap();
    assert!(row_text(session.run("SELECT a, SUM(c) FROM t GROUP BY a WITH ROLLUP")).is_empty());
}

/// `GROUPING()` under `WITH ROLLUP`, checked against captured TiDB output.
///
/// `GROUPING(c)` is 1 when `c` is rolled up in the grouping set that
/// produced the row and 0 otherwise, which is the ONLY way to tell a
/// subtotal's NULL from a data NULL. With several arguments it returns a
/// bitmask whose LEFTMOST argument owns the HIGHEST bit (captured:
/// `GROUPING(a,b) = 1` and `GROUPING(b,a) = 2` on the `b`-rolled-up row).
///
/// Rows whose whole `ORDER BY` key ties -- a data-NULL row and the
/// subtotal that also reports `b = NULL` -- keep this tier's stable
/// emission order (data rows first, then subtotals); Go's order for such
/// ties is nondeterministic, so only the multiset is contractual there.
#[test]
fn grouping_with_rollup() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT, b BIGINT, c BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1,10),(1,NULL,20),(1,2,30),(2,1,40)")
        .unwrap();

    // Captured row for row. The two `1 NULL` rows are the point: the
    // first is a DATA NULL (grouping(b) = 0, sum 20), the second the
    // rollup subtotal over a=1 (grouping(b) = 1, sum 60).
    assert_eq!(
        row_text(session.run(
            "SELECT a, b, GROUPING(a), GROUPING(b), SUM(c) FROM t \
                 GROUP BY a, b WITH ROLLUP ORDER BY a, b"
        )),
        [
            ["NULL", "NULL", "1", "1", "100"],
            ["1", "NULL", "0", "0", "20"],
            ["1", "NULL", "0", "1", "60"],
            ["1", "1", "0", "0", "10"],
            ["1", "2", "0", "0", "30"],
            ["2", "NULL", "0", "1", "40"],
            ["2", "1", "0", "0", "40"],
        ]
    );

    // Multi-argument bitmask, captured row for row.
    assert_eq!(
        row_text(session.run(
            "SELECT a, b, GROUPING(a,b), GROUPING(b,a), SUM(c) FROM t \
                 GROUP BY a, b WITH ROLLUP ORDER BY a, b"
        )),
        [
            ["NULL", "NULL", "3", "3", "100"],
            ["1", "NULL", "0", "0", "20"],
            ["1", "NULL", "1", "2", "60"],
            ["1", "1", "0", "0", "10"],
            ["1", "2", "0", "0", "30"],
            ["2", "NULL", "1", "2", "40"],
            ["2", "1", "0", "0", "40"],
        ]
    );

    // HAVING reads a GROUPING() the select list does not project: the
    // column is computed, filtered on, and trimmed away. Captured.
    assert_eq!(
        row_text(session.run(
            "SELECT a, b, SUM(c) FROM t GROUP BY a, b WITH ROLLUP \
                 HAVING GROUPING(b) = 0 ORDER BY a, b"
        )),
        [
            ["1", "NULL", "20"],
            ["1", "1", "10"],
            ["1", "2", "30"],
            ["2", "1", "40"],
        ]
    );

    // ORDER BY reads one the same way.
    assert_eq!(
        row_text(session.run(
            "SELECT a, b, GROUPING(a), SUM(c) FROM t GROUP BY a, b WITH ROLLUP \
                 ORDER BY GROUPING(a), a, b"
        )),
        [
            ["1", "NULL", "0", "20"],
            ["1", "NULL", "0", "60"],
            ["1", "1", "0", "10"],
            ["1", "2", "0", "30"],
            ["2", "NULL", "0", "40"],
            ["2", "1", "0", "40"],
            ["NULL", "NULL", "1", "100"],
        ]
    );

    // Captured result type: BIGINT UNSIGNED, flen 20, binary flag.
    match session
        .run_with_columns("SELECT GROUPING(a) FROM t GROUP BY a WITH ROLLUP")
        .unwrap()
    {
        StmtOutput::Rows { columns, .. } => {
            let (name, ftype) = &columns[0];
            // Go names the column with the ORIGINAL text, `grouping(a)`;
            // this tier names every unaliased field by its restored form,
            // a pre-existing tier-wide naming gap rather than one this
            // function introduces.
            assert_eq!(name, "GROUPING(`a`)");
            assert_eq!(ftype.code(), tidb_datatype::FieldTypeCode::LongLong);
            assert!(ftype.is_unsigned());
            assert_eq!(ftype.flen(), 20);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // Captured: GROUPING() without WITH ROLLUP is
    // "[planner:1111]Invalid use of group function", whether the query
    // groups or not.
    assert!(matches!(
        session.run("SELECT a, GROUPING(a) FROM t GROUP BY a"),
        Err(DriverError::InvalidGroupFuncUse)
    ));
    assert!(matches!(
        session.run("SELECT a, GROUPING(a) FROM t"),
        Err(DriverError::InvalidGroupFuncUse)
    ));

    // Captured: an argument that is not grouped is
    // "[planner:3602]Argument #0 of GROUPING function is not in GROUP BY".
    assert!(matches!(
        session.run("SELECT a, GROUPING(c) FROM t GROUP BY a, b WITH ROLLUP"),
        Err(DriverError::FieldInGroupingNotGroupBy(0))
    ));

    // Deferred: Go evaluates `GROUPING(a) + 1` in the projection above
    // the aggregation, which this tier does not build for select fields.
    assert!(matches!(
        session.run("SELECT GROUPING(a) + 1 FROM t GROUP BY a, b WITH ROLLUP"),
        Err(DriverError::Unsupported(_))
    ));
}

/// The clauses TiDB parses but only implements as no-ops, checked
/// against captured TiDB output with `tidb_enable_noop_functions` at its
/// `OFF` default.
///
/// NOT PORTED from Go's own suites: `tidb_enable_shared_lock_promotion`
/// (no locking layer here to promote to) and the `READ ONLY` /
/// `OFFLINE MODE` / `sql_auto_is_null` gates, which belong to variable
/// and transaction surfaces this tier does not have.
#[test]
fn noop_function_gate() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    // Captured: FOR UPDATE runs and returns the rows.
    assert_eq!(
        session
            .run("SELECT b FROM t WHERE a = 1 FOR UPDATE")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(10)]])
    );
    // Its waiting options only shape a lock this tier does not take.
    session.run("SELECT b FROM t FOR UPDATE NOWAIT").unwrap();
    session.run("SELECT b FROM t FOR UPDATE OF t").unwrap();

    // Captured: the shared lock and SQL_CALC_FOUND_ROWS are 1235.
    for sql in [
        "SELECT b FROM t FOR SHARE",
        "SELECT b FROM t LOCK IN SHARE MODE",
        "SELECT SQL_CALC_FOUND_ROWS b FROM t LIMIT 1",
        "SELECT b FROM t GROUP BY b DESC",
    ] {
        assert!(
            matches!(session.run(sql), Err(DriverError::FunctionsNoopImpl(_))),
            "expected a noop-function error from {sql}"
        );
    }
    // An explicit ASC is written too, so it is gated the same way.
    assert!(matches!(
        session.run("SELECT b FROM t GROUP BY b ASC"),
        Err(DriverError::FunctionsNoopImpl("GROUP BY expr ASC|DESC"))
    ));
    // A GROUP BY with no direction is not.
    session.run("SELECT b FROM t GROUP BY b").unwrap();

    // The gate reaches a subquery, a derived table and a set operation.
    assert!(matches!(
        session.run("SELECT b FROM t WHERE a IN (SELECT a FROM t LOCK IN SHARE MODE)"),
        Err(DriverError::FunctionsNoopImpl(_))
    ));
    assert!(matches!(
        session.run("SELECT x.b FROM (SELECT b FROM t LOCK IN SHARE MODE) x"),
        Err(DriverError::FunctionsNoopImpl(_))
    ));
    assert!(matches!(
        session.run("SELECT b FROM t UNION SELECT a FROM t LOCK IN SHARE MODE"),
        Err(DriverError::FunctionsNoopImpl(_))
    ));

    // ON: the clause is accepted and does nothing, with no warning.
    session
        .apply_set("SET tidb_enable_noop_functions = 'ON'")
        .unwrap();
    session.run("SELECT b FROM t LOCK IN SHARE MODE").unwrap();
    assert!(session.warnings().is_empty());

    // WARN: accepted, with the same message as a warning.
    session
        .apply_set("SET tidb_enable_noop_functions = 'WARN'")
        .unwrap();
    session.run("SELECT b FROM t LOCK IN SHARE MODE").unwrap();
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1235);
    assert!(session.warnings()[0].message.contains("LOCK IN SHARE MODE"));
    // The warnings belong to the last statement only.
    session.run("SELECT b FROM t").unwrap();
    assert!(session.warnings().is_empty());

    // INTO OUTFILE writes a server-side file, which this tier cannot do,
    // so it is refused rather than answered with rows.
    session
        .apply_set("SET tidb_enable_noop_functions = 'OFF'")
        .unwrap();
    assert!(matches!(
        session.run("SELECT b FROM t INTO OUTFILE '/tmp/x'"),
        Err(DriverError::Unsupported(_))
    ));
}

/// ALTER TABLE MODIFY / CHANGE COLUMN, checked against captured TiDB
/// output (`alter table t modify column ...` on a mock store).
///
/// NOT PORTED from Go's own DDL suites: the concurrent/rollback schema
/// states (this tier applies a DDL atomically), reorg-worker batching,
/// and the type changes needing a full index rebuild across a partitioned
/// table -- none of those surfaces exist here.
#[test]
fn modify_column() {
    let mut session = Session::new();
    session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10), c BIGINT NOT NULL DEFAULT 5, KEY kb (b))")
            .unwrap();
    session
        .run("INSERT INTO t VALUES (1, 'xx', 7), (2, 'yy', 8)")
        .unwrap();

    // Captured: widening keeps the rows, and the index still reads.
    session
        .run("ALTER TABLE t MODIFY COLUMN b VARCHAR(20)")
        .unwrap();
    assert_eq!(
        session.run("SELECT a FROM t WHERE b = 'xx'").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    // Captured: CHANGE renames the column, and the rows survive.
    session
        .run("ALTER TABLE t CHANGE COLUMN c d BIGINT")
        .unwrap();
    assert_eq!(
        session.run("SELECT a, d FROM t").unwrap(),
        StmtResult::Rows(vec![
            vec![Datum::Int(1), Datum::Int(7)],
            vec![Datum::Int(2), Datum::Int(8)],
        ])
    );
    assert!(session.run("SELECT c FROM t").is_err());

    // Captured: an unknown column is 1054 naming the table, unless the
    // statement says IF EXISTS.
    assert!(matches!(
        session.run("ALTER TABLE t MODIFY COLUMN nosuch BIGINT"),
        Err(DriverError::UnknownColumnInTable { .. })
    ));
    assert!(matches!(
        session.run("ALTER TABLE t CHANGE COLUMN nosuch e BIGINT"),
        Err(DriverError::UnknownColumnInTable { .. })
    ));
    session
        .run("ALTER TABLE t MODIFY COLUMN IF EXISTS nosuch BIGINT")
        .unwrap();

    // Captured: a value the new type cannot read is 1292, and the table is
    // left untouched.
    assert!(matches!(
        session.run("ALTER TABLE t MODIFY COLUMN b BIGINT"),
        Err(DriverError::TruncatedIncorrectValue { kind: "DOUBLE", .. })
    ));
    assert_eq!(
        row_text(session.run("SELECT b FROM t WHERE a = 1")),
        [["xx"]]
    );

    // Captured: a clustered handle cannot leave the integer domain (8200),
    // but may change to another integer type.
    assert!(matches!(
        session.run("ALTER TABLE t MODIFY COLUMN a VARCHAR(10)"),
        Err(DriverError::UnsupportedModifyColumn(_))
    ));
    session.run("ALTER TABLE t MODIFY COLUMN a INT").unwrap();

    // Captured: an index cannot cover a full BLOB/TEXT column (1170).
    assert!(matches!(
        session.run("ALTER TABLE t MODIFY COLUMN b TEXT"),
        Err(DriverError::BlobKeyWithoutLength(_))
    ));

    // Captured: NOT NULL and DEFAULT come from the new definition.
    session
        .run("ALTER TABLE t MODIFY COLUMN b VARCHAR(20) NOT NULL")
        .unwrap();
    assert!(session.run("INSERT INTO t (a, d) VALUES (3, 1)").is_err());
    session
        .run("ALTER TABLE t MODIFY COLUMN d BIGINT DEFAULT 3")
        .unwrap();

    // Captured: FIRST and AFTER move the column, rows and index included.
    session
        .run("ALTER TABLE t MODIFY COLUMN b VARCHAR(20) NOT NULL FIRST")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT * FROM t")),
        [["xx", "1", "7"], ["yy", "2", "8"]]
    );
    session
        .run("ALTER TABLE t CHANGE COLUMN b bb VARCHAR(20) NOT NULL AFTER d")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT * FROM t")),
        [["1", "7", "xx"], ["2", "8", "yy"]]
    );
    // The renamed, moved column still reads through its index.
    assert_eq!(
        session.run("SELECT a FROM t WHERE bb = 'xx'").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    // Captured: renaming onto an existing column is 1060; renaming a
    // column to its own name is allowed.
    assert!(matches!(
        session.run("ALTER TABLE t CHANGE COLUMN bb a VARCHAR(20)"),
        Err(DriverError::DuplicateColumnName(_))
    ));
    session
        .run("ALTER TABLE t CHANGE COLUMN d d BIGINT")
        .unwrap();

    // Captured: a stored NULL is rejected by a new NOT NULL, with the
    // row's position; a convertible string becomes the new type.
    let mut session = Session::new();
    session
        .run("CREATE TABLE u (a BIGINT, b VARCHAR(10), c BIGINT)")
        .unwrap();
    session.run("INSERT INTO u VALUES (1, '12', NULL)").unwrap();
    assert!(matches!(
        session.run("ALTER TABLE u MODIFY COLUMN c BIGINT NOT NULL"),
        Err(DriverError::DataTruncatedAtRow { row: 1, .. })
    ));
    session.run("ALTER TABLE u MODIFY COLUMN b BIGINT").unwrap();
    assert_eq!(
        session.run("SELECT b FROM u").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(12)]])
    );

    // Captured: a value too wide for the narrowed type is 1265, and the
    // table keeps its old definition.
    session
        .run("CREATE TABLE w (a BIGINT, b VARCHAR(10))")
        .unwrap();
    session.run("INSERT INTO w VALUES (1, 'xxxxxxxx')").unwrap();
    assert!(matches!(
        session.run("ALTER TABLE w MODIFY COLUMN b VARCHAR(3)"),
        Err(DriverError::DataTruncatedValue { .. })
    ));
    assert_eq!(row_text(session.run("SELECT b FROM w")), [["xxxxxxxx"]]);
}

#[test]
fn rename_table() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t1 (id BIGINT PRIMARY KEY, v BIGINT, KEY kv (v))")
        .unwrap();
    session.run("INSERT INTO t1 VALUES (1, 9)").unwrap();

    // Captured: the table is renamed and keeps its rows.
    session.run("RENAME TABLE t1 TO t2").unwrap();
    assert_eq!(
        session.run("SELECT id, v FROM t2").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1), Datum::Int(9)]])
    );
    assert!(session.run("SELECT id FROM t1").is_err());
    // Its indexes come along, so a read through one still works.
    assert_eq!(
        session.run("SELECT id FROM t2 WHERE v = 9").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    // Captured: renaming onto an existing name is 1050.
    session.run("CREATE TABLE t3 (a BIGINT)").unwrap();
    assert!(matches!(
        session.run("RENAME TABLE t2 TO t3"),
        Err(DriverError::Schema(SchemaErrorKind::TableExists(_)))
    ));
    // Captured: renaming a table that does not exist is 1146.
    assert!(matches!(
        session.run("RENAME TABLE nosuch TO t9"),
        Err(DriverError::Schema(SchemaErrorKind::UnknownTable(_)))
    ));

    // Captured: ALTER TABLE ... RENAME TO is the same operation.
    session.run("ALTER TABLE t2 RENAME TO t4").unwrap();
    match session.run_with_columns("SHOW TABLES").unwrap() {
        StmtOutput::Rows { rows, .. } => assert_eq!(
            rows.into_iter()
                .map(|row| datum_text(&row[0]).unwrap())
                .collect::<Vec<_>>(),
            vec!["t3".to_owned(), "t4".to_owned()]
        ),
        other => panic!("expected rows, got {other:?}"),
    }

    // A rename may move the table to another schema.
    session.run("CREATE DATABASE other").unwrap();
    session.run("RENAME TABLE t4 TO other.moved").unwrap();
    assert_eq!(
        session.run("SELECT id FROM other.moved").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    // The renamed table reports its NEW name in a duplicate-key error,
    // which is the table.index form TiDB uses.
    session
        .run("CREATE TABLE dup (a BIGINT, UNIQUE KEY ua (a))")
        .unwrap();
    session.run("INSERT INTO dup VALUES (1)").unwrap();
    session.run("RENAME TABLE dup TO dup2").unwrap();
    match session.run("INSERT INTO dup2 VALUES (1)") {
        Err(DriverError::DuplicateEntry { key, .. }) => assert_eq!(key, "dup2.ua"),
        other => panic!("expected a duplicate-entry error, got {other:?}"),
    }
}

#[test]
fn unsupported_kinds_error() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT)").unwrap();
    // Shapes the write paths do not model yet. (ORDER BY and LIMIT used
    // to be the examples here; both work now -- see
    // `insert_select_and_ordered_dml`.)
    assert!(session.run("DELETE QUICK FROM t").is_err());
    // RETURNING is not one of them: Go parses it and silently ignores it,
    // so the insert lands with a plain OK.
    assert_eq!(
        session
            .run("INSERT INTO t (a) VALUES (1) RETURNING a")
            .unwrap(),
        StmtResult::Affected(1)
    );
}

/// Every ROLE statement parses and is refused by name -- not through a
/// generic "unsupported statement kind" fallback, which would leave a
/// user guessing whether the syntax or the feature is missing.
///
/// Captured from Go for the FUTURE roles unit: `CREATE ROLE r1` and
/// `GRANT r1 TO 'u1'@'%'` both succeed, after which `SHOW GRANTS FOR
/// 'u1'@'%'` gains a `GRANT 'r1'@'%' TO 'u1'@'%'` line positioned AFTER
/// the table-scope lines and BEFORE the dynamic ones; the role's own
/// dynamic privileges do NOT appear in that output, because a role's
/// privileges reach a user only through its ACTIVE roles. `SET ROLE r1`
/// from a session that was not granted `r1` is
/// `` `r1`@`%` is not granted to root@% `` (3530).
#[test]
fn role_statements_are_refused_by_name() {
    let mut session = session_with_privileges();
    for sql in [
        "CREATE ROLE r1",
        "DROP ROLE r1",
        "GRANT r1 TO 'u1'@'%'",
        "REVOKE r1 FROM 'u1'@'%'",
        "SET ROLE r1",
        "SET DEFAULT ROLE r1 TO 'u1'@'%'",
    ] {
        match session.run(sql) {
            Err(DriverError::Unsupported(message)) => {
                assert_eq!(message, ROLES_UNSUPPORTED, "{sql}");
            }
            other => panic!("expected the roles refusal for {sql}, got {other:?}"),
        }
    }
}
