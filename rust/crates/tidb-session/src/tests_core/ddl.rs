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
/// charset tier; the `drop database mysql` rejection, which is pinned as a
/// divergence in `tests_system_schemas` now that the `mysql` schema exists;
/// and the privilege/role cases.
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

/// A rename that cannot be carried out MOVES NOTHING.
///
/// Captured with `difftests/gorun`: after `rename table d1.src to
/// nosuchdb.t`, `select * from d1.src` still returns both rows and `show
/// tables` still lists `src`. Every assertion below therefore reads the
/// SOURCE back rather than only checking that an error came out -- an
/// error-only assertion cannot tell a rename that refused from a rename that
/// destroyed the table and then reported failure.
#[test]
fn a_refused_rename_leaves_the_source_table_readable() {
    let mut session = Session::new();
    session.run("CREATE DATABASE d1").unwrap();
    session.run("USE d1").unwrap();
    session.run("CREATE TABLE src (a BIGINT)").unwrap();
    session.run("INSERT INTO src VALUES (1), (2)").unwrap();

    let rows = StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]);

    // Captured: 1025, and `d1.src` survives with both rows.
    assert!(matches!(
        session.run("RENAME TABLE d1.src TO nosuchdb.t"),
        Err(DriverError::Schema(
            SchemaErrorKind::RenameTargetDatabaseMissing { .. }
        ))
    ));
    assert_eq!(session.run("SELECT a FROM d1.src").unwrap(), rows);

    // `ALTER TABLE ... RENAME TO` is the same operation and the same refusal.
    assert!(matches!(
        session.run("ALTER TABLE d1.src RENAME TO nosuchdb.t"),
        Err(DriverError::Schema(
            SchemaErrorKind::RenameTargetDatabaseMissing { .. }
        ))
    ));
    assert_eq!(session.run("SELECT a FROM d1.src").unwrap(), rows);

    // Captured: renaming a table onto ITSELF is refused as 1050, and the
    // table is still there afterwards.
    assert!(matches!(
        session.run("RENAME TABLE d1.src TO d1.src"),
        Err(DriverError::Schema(SchemaErrorKind::TableExists(_)))
    ));
    assert_eq!(session.run("SELECT a FROM d1.src").unwrap(), rows);

    // Captured: a multi-pair rename is ALL OR NOTHING. `d1.src TO d1.moved`
    // would succeed on its own, but the second pair fails, so `src` keeps
    // its name and `moved` never appears.
    assert!(session
        .run("RENAME TABLE d1.src TO d1.moved, d1.nope TO d1.q")
        .is_err());
    assert_eq!(session.run("SELECT a FROM d1.src").unwrap(), rows);
    assert!(session.run("SELECT a FROM d1.moved").is_err());

    // The same for a second pair that names a missing SCHEMA rather than a
    // missing table -- the destructive shape this test exists for.
    assert!(matches!(
        session.run("RENAME TABLE d1.src TO d1.moved, d1.src TO nosuchdb.q"),
        Err(DriverError::Schema(SchemaErrorKind::UnknownTable(_)))
    ));
    assert_eq!(session.run("SELECT a FROM d1.src").unwrap(), rows);

    // The success path still works, including across schemas, and staging
    // does not break a chain whose later pair reuses a name freed earlier.
    session.run("CREATE DATABASE d2").unwrap();
    session.run("CREATE TABLE d1.other (a BIGINT)").unwrap();
    session.run("INSERT INTO d1.other VALUES (7)").unwrap();
    session
        .run("RENAME TABLE d1.src TO d2.src, d1.other TO d1.src")
        .unwrap();
    assert_eq!(session.run("SELECT a FROM d2.src").unwrap(), rows);
    assert_eq!(
        session.run("SELECT a FROM d1.src").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(7)]])
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
    assert!(created.contains("`i` int DEFAULT '8'"), "{created}");
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

/// What a declared column type REPORTS on the three surfaces that report it,
/// which is two rules pulling in opposite directions.
///
/// A width the declaration omits is FILLED IN: Go's parser leaves the flen
/// unspecified and `setCharsetCollationFlenDecimal` gives it the type's
/// default, which is why `DECIMAL` reads back as `decimal(10,0)`. A width on
/// an INTEGER is then DROPPED again when the type is printed, because
/// `deprecate-integer-display-length` defaults to true and
/// `parsertypes.CompactStr` omits the suffix -- so `BIGINT` and `BIGINT(30)`
/// are the same `bigint`, and the stored flen (still 20, still what
/// `NUMERIC_PRECISION` is derived from) is simply not shown.
///
/// The three surfaces are pinned TOGETHER because Go reaches them through
/// different code -- `SHOW CREATE TABLE` and `SHOW COLUMNS` through
/// `GetTypeDesc`, `information_schema.columns` through `InfoSchemaStr` -- and
/// a normalization that fixed only one of them would be a regression.
///
/// The oracle is `tests/integrationtest/r/explain.result`, which records a
/// real `tidb-server`:
///
/// ```text
/// create table t (id int, c1 timestamp);
/// show columns from t;
/// Field  Type       Null  Key  Default  Extra
/// id     int        YES        NULL
/// c1     timestamp  YES        NULL
/// ```
///
/// Note that a `gorun` capture disagrees and says `int(11)`: the switch is a
/// process-wide variable that only `cmd/tidb-server/main.go` sets from the
/// config, so an in-process harness leaves it off. The recorded `.result` is
/// the one that describes a running node.
///
/// `TINYINT(1)` and `BIT`/`YEAR`/`CHAR`/`VARCHAR`/`DECIMAL` are the
/// counter-examples that keep this from being read as either rule alone:
/// `BOOL` must stay `tinyint(1)` (MySQL 8.0 keeps it so connectors can still
/// recognise a boolean), and the non-integer widths are never dropped.
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
        ("a", "bigint"),
        ("b", "int"),
        ("c", "smallint"),
        ("d", "tinyint"),
        ("e", "mediumint"),
        ("h", "bigint"),
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

/// Creating an object in a schema that does not exist is Go's 1049, and --
/// the half an error-only assertion would miss -- it creates NOTHING.
///
/// This used to report success and silently discard the table:
/// `Catalog::register_in` returned `()` and its body was an `if let Some(..)`
/// over the schema map, so an absent schema dropped the built table on the
/// floor. `CREATE TABLE nosuchdb.t` answered "Query OK" and the table was
/// never there.
///
/// Captured from real TiDB (`rust/difftests/gorun`, with the error text
/// printed), against a mock-store session:
///
/// ```text
/// create table nosuchdb.t (a bigint)        ERR [schema:1049]Unknown database 'nosuchdb'
/// select a from nosuchdb.t                  ERR [schema:1146]Table 'nosuchdb.t' doesn't exist
/// create view nosuchdb.v as select 1        ERR [schema:1049]Unknown database 'nosuchdb'
/// create table if not exists nosuchdb.t2 .. ERR [schema:1049]Unknown database 'nosuchdb'
/// create sequence nosuchdb.s                ERR [schema:1049]Unknown database 'nosuchdb'
/// create table nosuchdb.c like test.src     ERR [schema:1049]Unknown database 'nosuchdb'
/// create table nosuchdb.c2 like nosuchsrc.q ERR [schema:1146]Table 'nosuchsrc.q' doesn't exist
/// ```
///
/// The last two are the ordering: a `LIKE` source is resolved BEFORE the
/// target's schema is looked at, so a missing source wins.
#[test]
fn creating_in_an_unknown_schema_creates_nothing() {
    let mut session = Session::new();
    session.run("CREATE TABLE src (a BIGINT)").unwrap();

    // IF NOT EXISTS does not soften 1049: it excuses an existing TABLE, not
    // an absent schema.
    for statement in [
        "CREATE TABLE nosuchdb.t (a BIGINT)",
        "CREATE TABLE IF NOT EXISTS nosuchdb.t (a BIGINT)",
        "CREATE TABLE nosuchdb.t LIKE test.src",
        "CREATE VIEW nosuchdb.v AS SELECT 1",
        "CREATE SEQUENCE nosuchdb.s",
    ] {
        let error = session.run(statement).unwrap_err().to_mysql_error();
        assert_eq!(error.code, 1049, "{statement}");
        assert_eq!(error.message, "Unknown database 'nosuchdb'", "{statement}");
    }

    // A missing LIKE source is reported before the missing target schema.
    let error = session
        .run("CREATE TABLE nosuchdb.t LIKE nosuchsrc.q")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1146);
    assert_eq!(error.message, "Table 'nosuchsrc.q' doesn't exist");

    // Nothing was created. An error-only assertion passes just as happily
    // when the table WAS built and then discarded, so query it back: the
    // schema itself must still be absent, and every name in it unreachable.
    let error = session.run("USE nosuchdb").unwrap_err().to_mysql_error();
    assert_eq!(error.code, 1049);
    for statement in [
        "SELECT a FROM nosuchdb.t",
        "SELECT * FROM nosuchdb.v",
        "SELECT NEXTVAL(nosuchdb.s)",
    ] {
        assert!(session.run(statement).is_err(), "{statement}");
    }

    // And the same statements work once the schema exists, which pins that
    // the refusal is about the schema and not about the statement.
    session.run("CREATE DATABASE nosuchdb").unwrap();
    session.run("CREATE TABLE nosuchdb.t (a BIGINT)").unwrap();
    session.run("CREATE VIEW nosuchdb.v AS SELECT 1").unwrap();
    session.run("CREATE SEQUENCE nosuchdb.s").unwrap();
    session.run("INSERT INTO nosuchdb.t VALUES (7)").unwrap();
    assert_eq!(
        session.run("SELECT a FROM nosuchdb.t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(7)]])
    );
}

/// `IF EXISTS` / `IF NOT EXISTS` do not silence the object that was not
/// there -- they DEMOTE it, and Go's third warning level is where it lands.
///
/// Every expectation below is a `gorun` capture against a real TiDB session,
/// taken because the integration replay cannot see any of this: it compares
/// warnings on 28 of 4,906 statements, and `drop table if exists` is in none
/// of the 28 while being in nearly every recorded file. Before this test the
/// engine answered all of them with an EMPTY buffer, and there was no `Note`
/// level to answer them with.
///
/// The multi-name cases are here because they constrain the shape: Go reports
/// EVERY missing name, one note each, and when there is no `IF EXISTS` it
/// joins the same list into ONE error -- `Unknown table 'test.a,test.b'`, not
/// the first name alone.
#[test]
fn if_exists_demotes_the_error_it_swallowed_to_a_note() {
    let mut session = Session::new();
    let seen = |session: &Session| -> Vec<String> {
        session
            .warnings()
            .iter()
            .map(|w| format!("{}|{}|{}", w.level.as_str(), w.code, w.message))
            .collect()
    };

    // `drop table if exists nosuch` ->
    // `Note | 1051 | Unknown table 'test.nosuch'`.
    session.run("DROP TABLE IF EXISTS nosuch").unwrap();
    assert_eq!(seen(&session), ["Note|1051|Unknown table 'test.nosuch'"]);
    // The wire count is the OTHER channel a client reads, and it is the only
    // one a driver sees without asking a second statement.
    assert_eq!(session.wire_warning_count(), 1);

    // ONE note per missing name, not one per statement.
    session
        .run("DROP TABLE IF EXISTS nosuchA, nosuchB")
        .unwrap();
    assert_eq!(
        seen(&session),
        [
            "Note|1051|Unknown table 'test.nosuchA'",
            "Note|1051|Unknown table 'test.nosuchB'"
        ]
    );
    assert_eq!(session.wire_warning_count(), 2);

    // Without IF EXISTS the same list is ONE error carrying every name.
    assert_eq!(
        session
            .run("DROP TABLE nosuchA, nosuchB")
            .unwrap_err()
            .to_mysql_error()
            .message,
        "Unknown table 'test.nosuchA,test.nosuchB'"
    );

    // A name that WAS there leaves no note; the drop still happened.
    session.run("CREATE TABLE tt(a int)").unwrap();
    session.run("DROP TABLE IF EXISTS tt, nosuch2").unwrap();
    assert_eq!(seen(&session), ["Note|1051|Unknown table 'test.nosuch2'"]);

    // `create table if not exists tt` over an existing table ->
    // `Note | 1050 | Table 'test.tt' already exists`.
    session.run("CREATE TABLE tt(a int)").unwrap();
    session.run("CREATE TABLE IF NOT EXISTS tt(a int)").unwrap();
    assert_eq!(seen(&session), ["Note|1050|Table 'test.tt' already exists"]);
    // The one that DID create the table says nothing.
    session.run("CREATE TABLE tt2(a int)").unwrap();
    assert!(seen(&session).is_empty());

    // `create database if not exists test` ->
    // `Note | 1007 | Can't create database 'test'; database exists`.
    session.run("CREATE DATABASE IF NOT EXISTS test").unwrap();
    assert_eq!(
        seen(&session),
        ["Note|1007|Can't create database 'test'; database exists"]
    );

    // A view that is not there is the same 1051 as a table that is not there.
    session.run("DROP VIEW IF EXISTS nvA, nvB").unwrap();
    assert_eq!(
        seen(&session),
        [
            "Note|1051|Unknown table 'test.nvA'",
            "Note|1051|Unknown table 'test.nvB'"
        ]
    );

    // `DROP DATABASE IF EXISTS` is the exception and it is not an oversight:
    // captured from `gorun`, TiDB leaves the warning buffer EMPTY there, so
    // adding a note for symmetry would be the divergence.
    session.run("DROP DATABASE IF EXISTS nodb").unwrap();
    assert!(seen(&session).is_empty());
    assert_eq!(session.wire_warning_count(), 0);
}

/// The three `IF EXISTS` sites that live in the EXECUTOR rather than the
/// session were silent, and could not have been fixed at the call site alone
/// (#172): `StmtContext`'s warning buffer held `(code, message)` with no
/// level, and `Session::drain_eval_warnings` stamped everything it took out
/// as `Warning`. A note raised in a DDL action therefore could not survive
/// the trip. The buffer now carries Go's level, so it does.
///
/// Each note carries the SUPPRESSED error's own code and text, which is why
/// the three differ from one another rather than sharing one string. All
/// captured from `gorun` against real TiDB:
///
/// ```text
/// alter table t modify column if exists no_col bigint
///   -> Note|1054|Unknown column 'no_col' in 't'
/// alter table t change column if exists no_col nc bigint
///   -> Note|1054|Unknown column 'no_col' in 't'
/// alter table t drop column if exists no_col
///   -> Note|1091|Can't DROP 'no_col'; check that column/key exists
/// alter table t drop index if exists no_idx
///   -> Note|1091|index no_idx doesn't exist
/// drop index if exists no_idx on t
///   -> Note|1091|index no_idx doesn't exist
/// ```
#[test]
fn an_executor_tier_if_exists_keeps_its_note_level() {
    let mut session = Session::new();
    let seen = |session: &Session| -> Vec<String> {
        session
            .warnings()
            .iter()
            .map(|w| format!("{}|{}|{}", w.level.as_str(), w.code, w.message))
            .collect()
    };
    session
        .run("CREATE TABLE t (a INT, b INT, INDEX ib(b))")
        .unwrap();

    for (sql, note) in [
        (
            "ALTER TABLE t MODIFY COLUMN IF EXISTS no_col BIGINT",
            "Note|1054|Unknown column 'no_col' in 't'",
        ),
        (
            "ALTER TABLE t CHANGE COLUMN IF EXISTS no_col nc BIGINT",
            "Note|1054|Unknown column 'no_col' in 't'",
        ),
        (
            "ALTER TABLE t DROP COLUMN IF EXISTS no_col",
            "Note|1091|Can't DROP 'no_col'; check that column/key exists",
        ),
        (
            "ALTER TABLE t DROP INDEX IF EXISTS no_idx",
            "Note|1091|index no_idx doesn't exist",
        ),
        (
            "DROP INDEX IF EXISTS no_idx ON t",
            "Note|1091|index no_idx doesn't exist",
        ),
    ] {
        session.run(sql).unwrap();
        assert_eq!(seen(&session), [note], "{sql}");
        assert_eq!(session.wire_warning_count(), 1, "{sql}");
    }

    // Controls in both directions. Without IF EXISTS the same three are
    // statement ERRORS, and with IF EXISTS over something that IS there the
    // action happens and says nothing.
    for sql in [
        "ALTER TABLE t MODIFY COLUMN no_col BIGINT",
        "ALTER TABLE t DROP COLUMN no_col",
        "ALTER TABLE t DROP INDEX no_idx",
    ] {
        assert!(session.run(sql).is_err(), "{sql}");
    }
    session
        .run("ALTER TABLE t DROP INDEX IF EXISTS ib")
        .unwrap();
    assert!(seen(&session).is_empty());
    session
        .run("ALTER TABLE t DROP COLUMN IF EXISTS b")
        .unwrap();
    assert!(seen(&session).is_empty());
    assert_eq!(session.wire_warning_count(), 0);
}

/// The `Level` column reads `Note`, through the same `SHOW WARNINGS` a client
/// runs -- the channel the wire count above cannot show.
#[test]
fn show_warnings_prints_the_note_level() {
    let mut session = Session::new();
    session.run("DROP TABLE IF EXISTS nosuch").unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        [["Note", "1051", "Unknown table 'test.nosuch'"]]
    );
}

/// A generated column's expression is keyed by the NAMES it reads, so an
/// `ALTER TABLE` that inserts a column BEFORE one of them cannot re-point it.
///
/// This is #202's first pin. With the expression indexing the table row by
/// offset, `ADD COLUMN z INT FIRST` shifted `b` from offset 1 to offset 2
/// while `c AS (b+1)` went on reading offset 1 -- so `c` silently began
/// computing `z+1`, for the rows written after the ALTER and, because a
/// VIRTUAL column is recomputed on every read, for the rows written before it
/// as well.
#[test]
fn a_generated_column_follows_its_dependency_across_a_column_move() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE g (a INT, b INT, c INT AS (b+1))")
        .unwrap();
    session.run("INSERT INTO g (a, b) VALUES (1, 10)").unwrap();
    session.run("ALTER TABLE g ADD COLUMN z INT FIRST").unwrap();
    session
        .run("INSERT INTO g (z, a, b) VALUES (100, 2, 20)")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT b, c FROM g ORDER BY b")),
        [["10", "11"], ["20", "21"]],
        "c is b+1, not z+1"
    );
}

/// Go `RenameColumn` refuses to rename a column that name-keyed metadata
/// reads, rather than rewriting that metadata, and the error names WHICH
/// metadata objected: 3108 for a visible generated column
/// (`checkModifyColumnWithGeneratedColumnsConstraint`, `pkg/ddl/
/// modify_column.go`), 3837 for the hidden generated column an expression
/// index was rewritten into, from the same function.
///
/// This is the pin that says the corruption did not merely relocate when the
/// metadata became name-keyed: a rename that got through would leave `c`'s
/// expression naming a column no longer there.
#[test]
fn renaming_a_column_a_generated_column_reads_is_refused() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE rg (a INT, b INT, c INT AS (b+1))")
        .unwrap();

    let rendered = session
        .run("ALTER TABLE rg RENAME COLUMN b TO z2")
        .expect_err("a generated column reads b")
        .to_mysql_error();
    assert_eq!(rendered.code, 3108);
    assert_eq!(
        rendered.message,
        "Column 'b' has a generated column dependency."
    );

    // CHANGE COLUMN renames too, and Go raises the same refusal from
    // `getModifiableColumnJob`, unwrapped.
    assert_eq!(
        session
            .run("ALTER TABLE rg CHANGE COLUMN b z2 INT")
            .expect_err("the rename half of CHANGE COLUMN is the same refusal")
            .to_mysql_error()
            .code,
        3108
    );
    // A MODIFY that keeps the name is refused too, with the dependency error
    // WRAPPED in 3106 -- see
    // `tests_expression_indexes::modifying_a_depended_on_column_is_refused_even_without_a_rename`
    // for the capture. This line used to assert the MODIFY SUCCEEDED, which is
    // what left the generated column reading a type that had moved.
    assert_eq!(
        session
            .run("ALTER TABLE rg MODIFY COLUMN b BIGINT")
            .expect_err("a MODIFY of a depended-on column is refused as well")
            .to_mysql_error()
            .code,
        3106
    );

    // Unaffected columns rename freely, and the expression still computes.
    session.run("ALTER TABLE rg RENAME COLUMN a TO a2").unwrap();
    session
        .run("INSERT INTO rg (a2, b) VALUES (1, 10)")
        .unwrap();
    assert_eq!(row_text(session.run("SELECT c FROM rg")), [["11"]]);

    // The hidden generated column behind an expression index is the same
    // refusal with Go's other code.
    session.run("CREATE TABLE fi (a INT, b INT)").unwrap();
    session.run("ALTER TABLE fi ADD INDEX idx((a+b))").unwrap();
    assert_eq!(
        session
            .run("ALTER TABLE fi RENAME COLUMN a TO z")
            .expect_err("an expression index reads a")
            .to_mysql_error()
            .code,
        3837
    );
    // And a DROP is the same question with the same answer.
    assert_eq!(
        session
            .run("ALTER TABLE rg DROP COLUMN b")
            .expect_err("a generated column reads b")
            .to_mysql_error()
            .code,
        3108
    );
}

/// `CREATE TABLE ... AS SELECT`. Transcreated from Go
/// `pkg/planner/core/preprocess_test.go` `TestPreprocess`'s four CTAS rows,
/// which all expect the same bare `errors.New` text, plus a live capture from
/// a real Go session (mockstore) confirming the wire shape:
///
/// ```text
/// create table t2 as select * from t1;
///   ERR code=1105 msg='CREATE TABLE ... SELECT' is not implemented yet
/// create table t3 (m int) select * from t1;
///   ERR code=1105 msg='CREATE TABLE ... SELECT' is not implemented yet
/// show tables;  RS:t1
/// ```
///
/// The last line is the load-bearing one: Go creates NOTHING. The column-list
/// form is the case that used to slip through here, because the CTAS clause
/// was parsed into `CreateTableStmt.ctas` and then never read -- an empty
/// table appeared where Go refuses.
#[test]
fn create_table_as_select_is_refused_and_creates_nothing() {
    let mut session = Session::new();
    session.run("CREATE TABLE t1 (a INT)").unwrap();

    for sql in [
        "CREATE TABLE t2 AS SELECT * FROM t1",
        "CREATE TABLE t3 SELECT * FROM t1",
        "CREATE TABLE t4 (m INT) SELECT * FROM t1",
        "CREATE TABLE t5 IGNORE SELECT * FROM t1 UNION SELECT * FROM t1",
    ] {
        let error = session.run(sql).expect_err(sql).to_mysql_error();
        assert_eq!(error.code, 1105, "{sql}");
        assert_eq!(
            error.message, "'CREATE TABLE ... SELECT' is not implemented yet",
            "{sql}"
        );
    }

    // Go's fourth `TestPreprocess` row. It is refused here too, but at parse
    // time with 1064 rather than 1105: `parse_create_table_result_source`
    // stops at the CTAS source's closing paren, while Go's
    // `ddl_table_parser.go` runs `maybeParseUnion` on the parenthesized
    // branch as well. That is a parser gap, tracked separately -- what this
    // asserts here is only that the statement never creates a table.
    assert!(session
        .run("CREATE TABLE t6 (m INT) REPLACE AS (SELECT * FROM t1) UNION (SELECT * FROM t1)")
        .is_err());

    // Go's `show tables` after the same script lists t1 alone.
    assert_eq!(row_text(session.run("SHOW TABLES")), [["t1"]]);
}

/// Go `buildIndexColumns` (`pkg/ddl/index.go:116`) checks TWO index-length
/// rules, not one: `checkIndexColumn` bounds each key part on its own, and a
/// running `sumLength` bounds their total. Only the first was ported, so a
/// schema whose parts are each legal and whose sum is not was accepted here
/// and refused by TiDB.
///
/// The same function's first arm, `ErrWrongKeyColumn` (1167), was missing
/// too: a `char(0)`/`binary(0)`/`varchar(0)` has no bytes to key on.
///
/// Every expectation below is captured from a real Go session (mockstore):
///
/// ```text
/// CREATE TABLE k1 (c01..c04 varchar(255) NOT NULL, PRIMARY KEY (c01,c02,c03,c04) clustered);
///   ERR 1071 Specified key was too long (4080 bytes); max key length is 3072 bytes
/// CREATE TABLE k2 (c01..c03 varchar(255) NOT NULL, KEY kk (c01,c02,c03));   OK
/// CREATE TABLE k3 (b char(0), index(b));    ERR 1167 ... can't index column 'b'
/// CREATE TABLE k4 (c binary(0), index(c));  ERR 1167 ... can't index column 'c'
/// CREATE TABLE k5 (d varchar(0), index(d)); ERR 1167 ... can't index column 'd'
/// CREATE TABLE k6 (a,b,c varchar(200) charset ascii, key kk(a,b,c));  OK
/// CREATE TABLE k7 (a text, unique (a(769)));            ERR 1071 (3076 bytes)
/// CREATE TABLE k8 (a text charset ascii, unique (a(3073)));  ERR 1071 (3073 bytes)
/// CREATE TABLE k9 (a varchar(600), b varchar(600), key(a,b)); ERR 1071 (4800 bytes)
/// ```
///
/// k2 and k6 are the load-bearing negatives: 3 x 1020 = 3060 and 3 x 200 = 600
/// both stay under the limit, so the sum has to be a real sum and not a
/// per-part check with a bigger number.
#[test]
fn create_table_bounds_the_whole_index_key_not_just_each_part() {
    let mut session = Session::new();
    let refused = |session: &mut Session, sql: &str, code: u16, message: &str| {
        let error = session.run(sql).expect_err(sql).to_mysql_error();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    };
    let too_long = |bytes: usize| {
        format!("Specified key was too long ({bytes} bytes); max key length is 3072 bytes")
    };

    refused(
        &mut session,
        "CREATE TABLE k1 (c01 varchar(255) NOT NULL, c02 varchar(255) NOT NULL, \
         c03 varchar(255) NOT NULL, c04 varchar(255) NOT NULL, \
         PRIMARY KEY (c01,c02,c03,c04) clustered)",
        1071,
        &too_long(4080),
    );
    // Three of the same columns are 3060 bytes and legal.
    session
        .run(
            "CREATE TABLE k2 (c01 varchar(255) NOT NULL, c02 varchar(255) NOT NULL, \
             c03 varchar(255) NOT NULL, KEY kk (c01,c02,c03))",
        )
        .unwrap();

    for (sql, column) in [
        ("CREATE TABLE k3 (b char(0), index(b))", "b"),
        ("CREATE TABLE k4 (c binary(0), index(c))", "c"),
        ("CREATE TABLE k5 (d varchar(0), index(d))", "d"),
    ] {
        refused(
            &mut session,
            sql,
            1167,
            &format!("The used storage engine can't index column '{column}'"),
        );
    }

    // One byte per character, so three 200-character parts are 600 bytes.
    session
        .run(
            "CREATE TABLE k6 (a varchar(200) charset ascii, b varchar(200) charset ascii, \
             c varchar(200) charset ascii, key kk(a,b,c))",
        )
        .unwrap();

    // The per-part rule still reports what it always did.
    refused(
        &mut session,
        "CREATE TABLE k7 (a text, unique (a(769)))",
        1071,
        &too_long(3076),
    );
    refused(
        &mut session,
        "CREATE TABLE k8 (a text charset ascii, unique (a(3073)))",
        1071,
        &too_long(3073),
    );
    // Neither part declares a prefix, so each takes the COLUMN's own width:
    // 2 x 4 x 600 = 4800. A port that summed only DECLARED prefixes reads 0.
    refused(
        &mut session,
        "CREATE TABLE k9 (a varchar(600), b varchar(600), key(a,b))",
        1071,
        &too_long(4800),
    );
    // ALTER TABLE ADD INDEX runs the same `buildIndexColumns`.
    session
        .run("CREATE TABLE ka (a varchar(600), b varchar(600))")
        .unwrap();
    refused(
        &mut session,
        "ALTER TABLE ka ADD INDEX kk (a,b)",
        1071,
        &too_long(4800),
    );
}

/// `CREATE TABLE` refuses what Go refuses, with Go's code and message.
///
/// The live builder had none of these -- it accepted duplicate column and
/// index names, a BLOB/TEXT/JSON default, `DECIMAL(M,D)` with `M < D`, a
/// duplicated ENUM/SET member, and answered a generic `unsupported` for an
/// out-of-range fsp and a second PRIMARY KEY. Every pair below is captured
/// from TiDB with `create ...; show errors;`:
///
/// ```text
/// a1(a int, a int)                        1060 Duplicate column name 'a'
/// a2(a int, b int, key i(a), key i(b))    1061 Duplicate key name 'i'
/// a3(a blob default 'x')                  1101 BLOB/TEXT/JSON column 'a'
///                                              can't have a default value
/// a3b(a text default 'x')                 1101 (same)
/// a3c(a json default '{}')                1101 (same)
/// a5(a decimal(2,5))                      1427 For float(M,D), double(M,D)
///     or decimal(M,D), M must be >= D (column 'a').
/// a6(a enum('x','x'))                     1291 Column 'a' has duplicated
///                                              value 'x' in ENUM
/// a6b(a set('y','y'))                     1291 ... value 'y' in SET
/// b2(a int primary key, b int primary key) 1068 Multiple primary key defined
/// b1(a datetime(7))                       1426 Too-big precision 7 specified
///                                              for 'a'. Maximum is 6.
/// ```
#[test]
fn create_table_refuses_what_go_refuses() {
    let mut session = Session::new();
    let refused = |session: &mut Session, sql: &str, code: u16, message: &str| {
        // Name the statement on the ACCEPTED path too: a lost refusal is the
        // failure this test exists to catch, and a bare `unwrap_err` would
        // report every group at the same line.
        let error = match session.run(sql) {
            Ok(_) => panic!("{sql} was accepted; expected {code} {message}"),
            Err(error) => error.to_mysql_error(),
        };
        assert_eq!(error.code, code, "{sql}");
        assert_eq!(error.message, message, "{sql}");
    };

    let blob_default = "BLOB/TEXT/JSON column 'a' can't have a default value";
    refused(
        &mut session,
        "CREATE TABLE a1 (a INT, a INT)",
        1060,
        "Duplicate column name 'a'",
    );
    refused(
        &mut session,
        "CREATE TABLE a2 (a INT, b INT, KEY i(a), KEY i(b))",
        1061,
        "Duplicate key name 'i'",
    );
    refused(
        &mut session,
        "CREATE TABLE a3 (a BLOB DEFAULT 'x')",
        1101,
        blob_default,
    );
    refused(
        &mut session,
        "CREATE TABLE a3b (a TEXT DEFAULT 'x')",
        1101,
        blob_default,
    );
    refused(
        &mut session,
        "CREATE TABLE a3c (a JSON DEFAULT '{}')",
        1101,
        blob_default,
    );
    refused(
        &mut session,
        "CREATE TABLE a5 (a DECIMAL(2,5))",
        1427,
        "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column 'a').",
    );
    refused(
        &mut session,
        "CREATE TABLE a6 (a ENUM('x','x'))",
        1291,
        "Column 'a' has duplicated value 'x' in ENUM",
    );
    refused(
        &mut session,
        "CREATE TABLE a6b (a SET('y','y'))",
        1291,
        "Column 'a' has duplicated value 'y' in SET",
    );
    refused(
        &mut session,
        "CREATE TABLE b2 (a INT PRIMARY KEY, b INT PRIMARY KEY)",
        1068,
        "Multiple primary key defined",
    );
    refused(
        &mut session,
        "CREATE TABLE b1 (a DATETIME(7))",
        1426,
        "Too-big precision 7 specified for 'a'. Maximum is 6.",
    );

    // The valid neighbours still pass, so none of the checks is a blanket
    // refusal: an unnamed second key, an equal M and D, and fsp 6.
    for ok in [
        "CREATE TABLE ok1 (a INT, b INT, KEY(a), KEY(b))",
        "CREATE TABLE ok2 (a DECIMAL(5,5))",
        "CREATE TABLE ok3 (a DATETIME(6), b TIME(6), c TIMESTAMP(6))",
        "CREATE TABLE ok4 (a ENUM('x','y'), b SET('x','y'))",
        "CREATE TABLE ok5 (a BLOB DEFAULT NULL, b JSON DEFAULT NULL)",
        "CREATE TABLE ok6 (a INT PRIMARY KEY, b INT UNIQUE KEY)",
    ] {
        session.run(ok).unwrap_or_else(|e| panic!("{ok}: {e:?}"));
    }
}

/// The OTHER half of Go's `checkColumnDefaultValue`: under a NON-strict
/// `sql_mode` an EMPTY-STRING default on a BLOB/TEXT/JSON column is a warning
/// rather than 1101, and Go then rewrites the default instead of keeping it.
///
/// This is TiDB's own `TestCheckColumnDefaultValue`
/// (`tests/integrationtest/t/ddl/column_modify.test:149-172`); the expectations
/// below are its recorded `.result`, verbatim:
///
/// ```text
/// set sql_mode='';
/// create table text_default_text(c1 text not null default '');
/// show create table text_default_text;   ->  `c1` text NOT NULL
/// create table text_default_blob(c1 blob not null default '');
/// show create table text_default_blob;   ->  `c1` blob NOT NULL
/// create table text_default_json(c1 json not null default '');
/// show create table text_default_json;   ->  `c1` json NOT NULL DEFAULT 'null'
/// ```
///
/// TEXT and BLOB DROP the default; JSON keeps one, rewritten to the text
/// `null`. A non-empty default stays 1101 in every mode, which is why the
/// strict half above and this one are both needed to pin the rule.
///
/// The rest of the table below was MEASURED against real TiDB rather than read
/// off the recording, because the recorded test covers only NOT NULL columns
/// and only three types. `sql_mode=''`, verbatim:
///
/// ```text
/// create table n4 (c1 tinyblob not null default '')  -> `c1` tinyblob NOT NULL DEFAULT ''
/// create table n5 (c1 mediumtext not null default '')-> `c1` mediumtext NOT NULL DEFAULT ''
/// create table t1 (c1 text default '')               -> `c1` text DEFAULT ''
/// ```
///
/// Two rules that a "TEXT and BLOB drop the default" reading would get wrong:
/// only `TypeBlob`/`TypeLongBlob` report `hasDefaultValue = false` (TINY and
/// MEDIUM do not), and the drop is only VISIBLE on a NOT NULL column, because
/// what Go sets is `NoDefaultValueFlag` and `setNoDefaultValueFlag` returns
/// early for a nullable one.
#[test]
fn a_non_strict_empty_default_on_blob_text_warns_and_drops_the_default() {
    let mut session = Session::new();
    session.run("SET sql_mode=''").unwrap();

    for (sql, column, printed) in [
        (
            "CREATE TABLE text_default_text (c1 TEXT NOT NULL DEFAULT '')",
            "text_default_text",
            "`c1` text NOT NULL",
        ),
        (
            "CREATE TABLE text_default_blob (c1 BLOB NOT NULL DEFAULT '')",
            "text_default_blob",
            "`c1` blob NOT NULL",
        ),
        // The line the blanket JSON refusal used to make unreachable.
        (
            "CREATE TABLE text_default_json (c1 JSON NOT NULL DEFAULT '')",
            "text_default_json",
            "`c1` json NOT NULL DEFAULT 'null'",
        ),
        // Only BLOB and LONGBLOB report `hasDefaultValue = false`.
        (
            "CREATE TABLE text_default_tiny (c1 TINYBLOB NOT NULL DEFAULT '')",
            "text_default_tiny",
            "`c1` tinyblob NOT NULL DEFAULT ''",
        ),
        (
            "CREATE TABLE text_default_medium (c1 MEDIUMTEXT NOT NULL DEFAULT '')",
            "text_default_medium",
            "`c1` mediumtext NOT NULL DEFAULT ''",
        ),
        (
            "CREATE TABLE text_default_long (c1 LONGTEXT NOT NULL DEFAULT '')",
            "text_default_long",
            "`c1` longtext NOT NULL",
        ),
        // A NULLABLE column keeps the default it was written, because the
        // flag Go sets is only reached for NOT NULL.
        (
            "CREATE TABLE text_default_null (c1 TEXT DEFAULT '')",
            "text_default_null",
            "`c1` text DEFAULT ''",
        ),
    ] {
        session.run(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        let created = show_create(&mut session, column);
        assert!(
            created.contains(printed),
            "SHOW CREATE TABLE {column} should print `{printed}`:\n{created}"
        );
    }

    // A NON-EMPTY default is still 1101 even here -- the non-strict relief is
    // for the empty string alone.
    let error = session
        .run("CREATE TABLE text_default_scds (c1 TEXT NOT NULL DEFAULT 'scds')")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1101);
    assert_eq!(
        error.message,
        "BLOB/TEXT/JSON column 'c1' can't have a default value"
    );
}

/// The SAME rule, at the three ALTER entry points Go's `SetDefaultValue`
/// serves besides CREATE TABLE. Measured against real TiDB; the ALTER COLUMN
/// line is the one that is NOT a copy of the others.
///
/// ```text
/// set sql_mode='';
/// create table s7 (c1 text);
/// alter table s7 alter column c1 set default '';  -> ERROR 1067 Invalid default value for 'c1'
/// alter table s7 add column c2 text default '';   -> OK, `c2` text DEFAULT ''
/// alter table s7 add column c3 json default '';   -> OK, `c3` json DEFAULT 'null'
/// alter table s7 modify column c1 json default '';-> OK, `c1` json DEFAULT 'null'
/// set sql_mode='STRICT_TRANS_TABLES';
/// alter table s6 alter column c1 set default 'x'; -> ERROR 1101
/// alter table s6 add column c2 text default '';   -> ERROR 1101
/// ```
///
/// 1067 rather than 1101 for `SET DEFAULT ''` is Go's `updateColumnDefaultValue`
/// (`pkg/ddl/column.go:1150`), which re-runs `checkColumnDefaultValue` inside
/// the DDL job and turns `!hasDefaultValue` into `ErrInvalidDefaultValue`.
#[test]
fn the_blob_default_rule_reaches_every_alter_entry_point() {
    let mut session = Session::new();
    session.run("SET sql_mode=''").unwrap();
    session.run("CREATE TABLE s7 (c1 TEXT)").unwrap();

    let error = session
        .run("ALTER TABLE s7 ALTER COLUMN c1 SET DEFAULT ''")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1067);
    assert_eq!(error.message, "Invalid default value for 'c1'");

    session
        .run("ALTER TABLE s7 ADD COLUMN c2 TEXT DEFAULT ''")
        .unwrap();
    session
        .run("ALTER TABLE s7 ADD COLUMN c3 JSON DEFAULT ''")
        .unwrap();
    session
        .run("ALTER TABLE s7 MODIFY COLUMN c1 JSON DEFAULT ''")
        .unwrap();
    let created = show_create(&mut session, "s7");
    for printed in [
        "`c1` json DEFAULT 'null'",
        "`c2` text DEFAULT ''",
        "`c3` json DEFAULT 'null'",
    ] {
        assert!(
            created.contains(printed),
            "expected `{printed}`:\n{created}"
        );
    }

    // The NOT NULL drop, at ADD COLUMN and MODIFY:
    //   alter table s8 add column c9 text not null default ''
    //     -> `c9` text NOT NULL, no DEFAULT
    session.run("CREATE TABLE s8 (c1 TEXT NOT NULL)").unwrap();
    session
        .run("ALTER TABLE s8 ADD COLUMN c9 TEXT NOT NULL DEFAULT ''")
        .unwrap();
    session
        .run("ALTER TABLE s8 MODIFY COLUMN c1 TEXT NOT NULL DEFAULT ''")
        .unwrap();
    let created = show_create(&mut session, "s8");
    assert!(
        created.contains("`c1` text NOT NULL,") && created.contains("`c9` text NOT NULL\n"),
        "neither column should print a DEFAULT:\n{created}"
    );

    // Strict mode refuses all three with 1101, empty string included.
    session.run("SET sql_mode='STRICT_TRANS_TABLES'").unwrap();
    session.run("CREATE TABLE s6 (c1 TEXT)").unwrap();
    for sql in [
        "ALTER TABLE s6 ALTER COLUMN c1 SET DEFAULT 'x'",
        "ALTER TABLE s6 ALTER COLUMN c1 SET DEFAULT ''",
        "ALTER TABLE s6 ADD COLUMN c2 TEXT DEFAULT ''",
        "ALTER TABLE s6 ADD COLUMN c3 JSON DEFAULT '{}'",
        "ALTER TABLE s6 MODIFY COLUMN c1 TEXT DEFAULT ''",
    ] {
        let error = session.run(sql).unwrap_err().to_mysql_error();
        assert_eq!(error.code, 1101, "{sql}");
    }
}
