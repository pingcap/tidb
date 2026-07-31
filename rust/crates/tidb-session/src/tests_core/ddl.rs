//! Schema statements through the session: databases, name resolution
//! against the current one, and `CREATE`/`ALTER`/`DROP`/`TRUNCATE`/`RENAME`
//! of tables and indexes -- Go `pkg/ddl`.

use crate::tests_support::*;
use crate::*;

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

    // Re-select a database: the block above deliberately left none.
    session.run("USE test").unwrap();

    // Creating a table that exists is Go's ErrTableExists (1050) with the
    // db-qualified name -- "Table 'test.t' already exists" -- not a generic
    // 1105 (a worker flagged this while porting sequences; the mapping
    // existed and the CREATE site just did not use it).
    session.run("CREATE TABLE exists_t (a INT)").unwrap();
    let error = session
        .run("CREATE TABLE exists_t (a INT)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1050);
    assert_eq!(error.message, "Table 'test.exists_t' already exists");
    session
        .run("CREATE TABLE IF NOT EXISTS exists_t (a INT)")
        .unwrap();
    session.run("DROP TABLE exists_t").unwrap();

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

/// A column type written WITHOUT a display width is not stored as written:
/// Go's parser leaves the flen unspecified and
/// `setCharsetCollationFlenDecimal` fills in the type's default, so a declared
/// `BIGINT` is a `bigint(20)` on every surface that reports it. This pins the
/// three surfaces together, because Go reaches them through different code and
/// a normalization that fixed only one of them would be a regression.
///
/// Captured from real TiDB for
/// `create table w (a bigint, b int, c smallint, d tinyint, e mediumint,
/// h bigint(30), i decimal, j float, k double, l char, m varchar(7), n year,
/// o bit, p bool)`:
///
/// ```text
/// SHOW CREATE TABLE w / information_schema.columns.column_type
///   a bigint(20)   b int(11)      c smallint(6)  d tinyint(4)
///   e mediumint(9) h bigint(30)   i decimal(10,0)
///   j float        k double       l char(1)      m varchar(7)
///   n year(4)      o bit(1)       p tinyint(1)
/// ```
///
/// `FLOAT`/`DOUBLE` are the counter-example that shows this is not "always
/// print a width": Go stores their default flen but the printer omits it, so
/// the declared spelling survives there.
#[test]
fn a_declared_type_without_a_width_normalizes_to_its_default() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE w (a BIGINT, b INT, c SMALLINT, d TINYINT, e MEDIUMINT, \
             h BIGINT(30), i DECIMAL, j FLOAT, k DOUBLE, l CHAR, m VARCHAR(7), \
             n YEAR, o BIT, p BOOL)",
        )
        .unwrap();
    let expected = [
        ("a", "bigint(20)"),
        ("b", "int(11)"),
        ("c", "smallint(6)"),
        ("d", "tinyint(4)"),
        ("e", "mediumint(9)"),
        ("h", "bigint(30)"),
        ("i", "decimal(10,0)"),
        ("j", "float"),
        ("k", "double"),
        ("l", "char(1)"),
        ("m", "varchar(7)"),
        ("n", "year(4)"),
        ("o", "bit(1)"),
        ("p", "tinyint(1)"),
    ];

    let created = show_create(&mut session, "w");
    for (column, printed) in expected {
        assert!(
            created.contains(&format!("`{column}` {printed} ")),
            "SHOW CREATE TABLE is missing `{column}` {printed}:\n{created}"
        );
    }

    let described = row_text(session.run("SHOW COLUMNS FROM w"));
    let reported = row_text(session.run(
        "SELECT column_name, column_type FROM information_schema.columns \
         WHERE table_name = 'w' ORDER BY ordinal_position",
    ));
    for (index, (column, printed)) in expected.iter().enumerate() {
        assert_eq!(
            (described[index][0].as_str(), described[index][1].as_str()),
            (*column, *printed),
            "SHOW COLUMNS"
        );
        assert_eq!(
            (reported[index][0].as_str(), reported[index][1].as_str()),
            (*column, *printed),
            "information_schema.columns"
        );
    }
}
