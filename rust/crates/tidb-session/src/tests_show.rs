//! The `SHOW` family over a real session, against captured TiDB output.
//!
//! # One capture in here is not a running node, and it is called out inline
//!
//! An INTEGER's display width is dropped from every type a real server
//! prints: `deprecate-integer-display-length` defaults to true, and only
//! `cmd/tidb-server/main.go` copies it into the process-wide
//! `parsertypes.TiDBStrictIntegerDisplayWidth` that `CompactStr` reads. An
//! in-process capture harness never runs that line, so `gorun` prints
//! `int(11)` where `tests/integrationtest/r/explain.result` -- recorded
//! against a real `tidb-server` -- records `int`. The assertions here follow
//! the recording; the few comments that quote `gorun` verbatim say so.
//!
//! `TINYINT(1)` and `ZEROFILL` keep their widths either way, which is Go's
//! own exception for connectors that read `tinyint(1)` as a boolean.

#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// SHOW DATABASES and SHOW TABLES, with Go's column naming and ordering.
#[test]
fn show_databases_and_tables() {
    let mut session = Session::new();
    session.run("CREATE TABLE zeta (a BIGINT)").unwrap();
    session.run("CREATE TABLE alpha (a BIGINT)").unwrap();
    session.run("CREATE DATABASE other").unwrap();

    // Go's fetchShowDatabases sorts the names, then moves
    // information_schema to the front; the column is "Database".
    //
    // `mysql` is in the list because it is a schema the catalog seeds (see
    // `Catalog::default`), which moves this assertion TOWARD TiDB's own
    // answer rather than away: captured, `select schema_name from
    // information_schema.schemata` on a real server returns
    // `INFORMATION_SCHEMA;METRICS_SCHEMA;PERFORMANCE_SCHEMA;mysql;sys;test`.
    // The three still missing are a documented divergence on
    // `Catalog::default`.
    match session.run_with_columns("SHOW DATABASES").unwrap() {
        StmtOutput::Rows { columns, rows } => {
            assert_eq!(columns[0].0, "Database");
            assert_eq!(
                rows.iter()
                    .map(|row| datum_text(&row[0]).unwrap())
                    .collect::<Vec<_>>(),
                vec![
                    "INFORMATION_SCHEMA".to_owned(),
                    "mysql".to_owned(),
                    "other".to_owned(),
                    "test".to_owned()
                ]
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // Go names the column Tables_in_<db> and sorts the table names.
    match session.run_with_columns("SHOW TABLES").unwrap() {
        StmtOutput::Rows { columns, rows } => {
            assert_eq!(columns[0].0, "Tables_in_test");
            assert_eq!(
                rows.iter()
                    .map(|row| datum_text(&row[0]).unwrap())
                    .collect::<Vec<_>>(),
                vec!["alpha".to_owned(), "zeta".to_owned()]
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // SHOW TABLES IN <db> reports that schema, and an empty one is empty.
    match session.run_with_columns("SHOW TABLES IN other").unwrap() {
        StmtOutput::Rows { columns, rows } => {
            assert_eq!(columns[0].0, "Tables_in_other");
            assert!(rows.is_empty());
        }
        other => panic!("expected rows, got {other:?}"),
    }
    // An unknown schema is Go's ErrBadDB.
    assert!(matches!(
        session.run("SHOW TABLES IN nope"),
        Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(_)))
    ));
}

/// SHOW COLUMNS / DESCRIBE, with Go's ColDesc field names and key flags.
#[test]
fn show_columns_and_describe() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, \
                 tag VARCHAR(4), v BIGINT, KEY tag_idx (tag))",
        )
        .unwrap();

    let describe = |session: &mut Session, sql: &str| match session
        .run_with_columns(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    {
        StmtOutput::Rows { columns, rows } => (
            columns
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            rows.into_iter()
                .map(|row| {
                    row.iter()
                        .map(|value| match value {
                            Datum::Null => "NULL".to_owned(),
                            other => datum_text(other).unwrap_or_default(),
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
        ),
        other => panic!("expected rows, got {other:?}"),
    };

    let (names, rows) = describe(&mut session, "SHOW COLUMNS FROM t");
    assert_eq!(names, ["Field", "Type", "Null", "Key", "Default", "Extra"]);
    assert_eq!(
        rows,
        vec![
            // A handle primary key is NOT NULL and PRI, as Go marks it.
            vec!["id", "bigint", "NO", "PRI", "NULL", ""],
            // A column that is the whole of a unique index is UNI.
            vec!["code", "varchar(8)", "YES", "UNI", "NULL", ""],
            // A column leading a non-unique index is MUL.
            vec!["tag", "varchar(4)", "YES", "MUL", "NULL", ""],
            // An unindexed column has no key flag.
            vec!["v", "bigint", "YES", "", "NULL", ""],
        ]
    );

    // Go reports auto_increment in Extra; captured from TiDB's DESCRIBE:
    // [[id bigint(20) NO PRI <nil> auto_increment] [v bigint(20) YES  <nil> ]]
    // -- `gorun` spells the width; a real server drops it (see the module doc).
    session
        .run("CREATE TABLE ai (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)")
        .unwrap();
    assert_eq!(
        describe(&mut session, "DESCRIBE ai").1,
        vec![
            vec!["id", "bigint", "NO", "PRI", "NULL", "auto_increment"],
            vec!["v", "bigint", "YES", "", "NULL", ""],
        ]
    );

    // A column's stored DEFAULT shows in the Default column.
    session
        .run("CREATE TABLE withdef (a BIGINT DEFAULT 7, b VARCHAR(4) DEFAULT 'zz')")
        .unwrap();
    assert_eq!(
        describe(&mut session, "DESCRIBE withdef").1,
        vec![
            vec!["a", "bigint", "YES", "", "7", ""],
            vec!["b", "varchar(4)", "YES", "", "zz", ""],
        ]
    );

    // DESCRIBE parses to the same node and answers identically.
    assert_eq!(describe(&mut session, "DESCRIBE t"), (names.clone(), rows));
    assert_eq!(describe(&mut session, "DESC t").0, names);

    // Another schema's table is reachable by qualifying the FROM.
    session.run("CREATE DATABASE other").unwrap();
    session.run("USE other").unwrap();
    assert_eq!(describe(&mut session, "SHOW COLUMNS FROM test.t").0, names);

    // Go's DESCRIBE takes an optional column, which narrows the output.
    session.run("USE test").unwrap();
    let (_, one) = describe(&mut session, "DESCRIBE t code");
    assert_eq!(
        one,
        vec![vec!["code", "varchar(8)", "YES", "UNI", "NULL", ""]]
    );

    // An unknown table is an error, not empty output.
    assert!(session.run("SHOW COLUMNS FROM nope").is_err());
}

/// SHOW FULL COLUMNS, checked against a capture from real TiDB
/// (`SHOW FULL COLUMNS FROM t` over `create table t (a int, b
/// varchar(20))`):
/// `[a int(11) <nil> YES  <nil>  select,insert,update,references ]` -- the
/// width is `gorun`'s; a real server prints `int` (see the module doc).
/// `[b varchar(20) utf8mb4_bin YES  <nil>  select,insert,update,references ]`
#[test]
fn show_full_columns() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a INT, b VARCHAR(20))")
        .unwrap();

    let (names, rows) = match session
        .run_with_columns("SHOW FULL COLUMNS FROM t")
        .unwrap()
    {
        StmtOutput::Rows { columns, rows } => (
            columns
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            rows.into_iter()
                .map(|row| {
                    row.iter()
                        .map(|value| match value {
                            Datum::Null => "NULL".to_owned(),
                            other => datum_text(other).unwrap_or_default(),
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
        ),
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(
        names,
        [
            "Field",
            "Type",
            "Collation",
            "Null",
            "Key",
            "Default",
            "Extra",
            "Privileges",
            "Comment",
        ]
    );
    assert_eq!(
        rows,
        vec![
            // A numeric column's Collation is NULL.
            vec![
                "a",
                "int",
                "NULL",
                "YES",
                "",
                "NULL",
                "",
                "select,insert,update,references",
                "",
            ],
            // A string column's Collation is its own collation name.
            vec![
                "b",
                "varchar(20)",
                "utf8mb4_bin",
                "YES",
                "",
                "NULL",
                "",
                "select,insert,update,references",
                "",
            ],
        ]
    );
}

/// SHOW CREATE TABLE, checked against output captured from real TiDB by
/// running the same DDL through `pkg/executor/test/showtest` and printing
/// `show create table`. Every expectation below is that captured text.
#[test]
fn show_create_table() {
    let mut session = Session::new();
    let create = |session: &mut Session, sql: &str, name: &str| {
        session.run(sql).unwrap();
        match session
            .run_with_columns(&format!("SHOW CREATE TABLE {name}"))
            .unwrap()
        {
            StmtOutput::Rows { columns, rows } => {
                assert_eq!(
                    columns.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
                    ["Table", "Create Table"]
                );
                assert_eq!(datum_text(&rows[0][0]).unwrap(), name);
                datum_text(&rows[0][1]).unwrap()
            }
            other => panic!("expected rows, got {other:?}"),
        }
    };

    // Captured from TiDB verbatim.
    assert_eq!(
        create(
            &mut session,
            "create table t1 (id bigint primary key, code varchar(8) unique, \
                 tag varchar(4), v bigint, key tag_idx (tag))",
            "t1"
        ),
        "CREATE TABLE `t1` (\n  \
             `id` bigint NOT NULL,\n  \
             `code` varchar(8) DEFAULT NULL,\n  \
             `tag` varchar(4) DEFAULT NULL,\n  \
             `v` bigint DEFAULT NULL,\n  \
             PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n  \
             KEY `tag_idx` (`tag`),\n  \
             UNIQUE KEY `code` (`code`)\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    assert_eq!(
        create(
            &mut session,
            "create table t2 (a bigint default 7, b varchar(4) default 'zz', \
                 c bigint not null, d bigint)",
            "t2"
        ),
        "CREATE TABLE `t2` (\n  \
             `a` bigint DEFAULT '7',\n  \
             `b` varchar(4) DEFAULT 'zz',\n  \
             `c` bigint NOT NULL,\n  \
             `d` bigint DEFAULT NULL\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    assert_eq!(
        create(
            &mut session,
            "create table t4 (a bigint, b bigint, key ab (a,b))",
            "t4"
        ),
        "CREATE TABLE `t4` (\n  \
             `a` bigint DEFAULT NULL,\n  \
             `b` bigint DEFAULT NULL,\n  \
             KEY `ab` (`a`,`b`)\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    // Index order: table constraints first, then inline ones in column
    // order -- also captured from TiDB.
    assert_eq!(
        create(
            &mut session,
            "create table x1 (a bigint unique, b bigint unique, key kb (b))",
            "x1"
        ),
        "CREATE TABLE `x1` (\n  \
             `a` bigint DEFAULT NULL,\n  \
             `b` bigint DEFAULT NULL,\n  \
             KEY `kb` (`b`),\n  \
             UNIQUE KEY `a` (`a`),\n  \
             UNIQUE KEY `b` (`b`)\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    // An index written WITHOUT a name is named after its first column, and a
    // collision appends `_2` (Go `GetName4AnonymousIndex`). Captured with
    // `rust/difftests/gorun`: this engine used to call them `idx_1`/`idx_2`,
    // which is the name a duplicate-key error prints and the name
    // `DROP INDEX` needs.
    assert_eq!(
        create(
            &mut session,
            "create table n2 (a bigint, b bigint, unique key (a, b))",
            "n2"
        ),
        "CREATE TABLE `n2` (\n  \
             `a` bigint DEFAULT NULL,\n  \
             `b` bigint DEFAULT NULL,\n  \
             UNIQUE KEY `a` (`a`,`b`)\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
    assert_eq!(
        create(
            &mut session,
            "create table n3 (a bigint, b bigint, unique key (a), key (a))",
            "n3"
        ),
        "CREATE TABLE `n3` (\n  \
             `a` bigint DEFAULT NULL,\n  \
             `b` bigint DEFAULT NULL,\n  \
             UNIQUE KEY `a` (`a`),\n  \
             KEY `a_2` (`a`)\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    // A string primary key is now a clustered common handle, so this
    // matches TiDB's captured output exactly. The previous commit
    // reported NONCLUSTERED, truthfully, because no common handle
    // existed then.
    assert_eq!(
        create(
            &mut session,
            "create table t3 (k varchar(10) primary key)",
            "t3"
        ),
        "CREATE TABLE `t3` (\n  \
             `k` varchar(10) NOT NULL,\n  \
             PRIMARY KEY (`k`) /*T![clustered_index] CLUSTERED */\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    // AUTO_INCREMENT, captured from TiDB verbatim.
    assert_eq!(
        create(
            &mut session,
            "create table a1 (id bigint auto_increment primary key, v bigint)",
            "a1"
        ),
        "CREATE TABLE `a1` (\n  \
             `id` bigint NOT NULL AUTO_INCREMENT,\n  \
             `v` bigint DEFAULT NULL,\n  \
             PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    // An unknown table is an error, and another schema is reachable.
    assert!(session.run("SHOW CREATE TABLE nope").is_err());
    session.run("CREATE DATABASE other").unwrap();
    session.run("USE other").unwrap();
    assert!(session.run("SHOW CREATE TABLE test.t1").is_ok());
}

/// information_schema, checked against output captured from a running
/// TiDB: the column lists and the values for the same table definition.
#[test]
fn information_schema() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY, \
                 code VARCHAR(8) UNIQUE, v BIGINT DEFAULT 7)",
        )
        .unwrap();

    let query = |session: &mut Session, sql: &str| match session.run_with_columns(sql).unwrap() {
        StmtOutput::Rows { columns, rows } => (
            columns
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            rows.into_iter()
                .map(|row| {
                    row.iter()
                        .map(|value| match value {
                            Datum::Null => "<nil>".to_owned(),
                            Datum::Int(v) => v.to_string(),
                            other => datum_text(other).unwrap_or_default(),
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
        ),
        other => panic!("expected rows, got {other:?}"),
    };

    // SCHEMATA: captured column list, and a row per schema.
    let (names, rows) = query(&mut session, "SELECT * FROM information_schema.schemata");
    assert_eq!(
        names,
        [
            "CATALOG_NAME",
            "SCHEMA_NAME",
            "DEFAULT_CHARACTER_SET_NAME",
            "DEFAULT_COLLATION_NAME",
            "SQL_PATH",
            "TIDB_PLACEMENT_POLICY_NAME"
        ]
    );
    // Captured: [def INFORMATION_SCHEMA utf8mb4 utf8mb4_bin <nil> <nil>]
    assert_eq!(
        rows[0],
        vec![
            "def",
            "INFORMATION_SCHEMA",
            "utf8mb4",
            "utf8mb4_bin",
            "<nil>",
            "<nil>"
        ]
    );
    assert!(rows.iter().any(|row| row[1] == "test"));

    // TABLES: the captured 28-column list, and the captured values.
    let (names, rows) = query(&mut session, "SELECT * FROM information_schema.tables");
    assert_eq!(names.len(), 28, "the captured TABLES column count");
    assert_eq!(names[0], "TABLE_CATALOG");
    assert_eq!(names[27], "TIDB_STORAGE_CLASS");
    let row = rows.iter().find(|row| row[2] == "t").expect("table t");
    // Captured: def test t BASE TABLE InnoDB 10 Compact ...
    assert_eq!(
        &row[..7],
        ["def", "test", "t", "BASE TABLE", "InnoDB", "10", "Compact"]
    );
    assert_eq!(row[17], "utf8mb4_bin", "TABLE_COLLATION");
    assert_eq!(row[22], "NOT_SHARDED(PK_IS_HANDLE)");
    assert_eq!(row[23], "CLUSTERED");
    assert_eq!(row[25], "Normal");

    // COLUMNS: the captured 22-column list and per-column values.
    let (names, rows) = query(&mut session, "SELECT * FROM information_schema.columns");
    assert_eq!(names.len(), 22, "the captured COLUMNS column count");
    assert_eq!(names[4], "ORDINAL_POSITION");
    assert_eq!(names[21], "SRS_ID");
    let of = |name: &str| {
        rows.iter()
            .find(|row| row[2] == "t" && row[3] == name)
            .expect("column")
            .clone()
    };
    // Captured: def test t id 1 <nil> NO bigint <nil> <nil> 19 0 <nil>
    //           <nil> <nil> bigint(20) PRI auto_increment ...
    let id = of("id");
    assert_eq!(
        &id[..8],
        ["def", "test", "t", "id", "1", "<nil>", "NO", "bigint"]
    );
    assert_eq!(
        &id[8..15],
        ["<nil>", "<nil>", "19", "0", "<nil>", "<nil>", "<nil>"]
    );
    assert_eq!(
        &id[15..19],
        [
            "bigint",
            "PRI",
            "auto_increment",
            "select,insert,update,references"
        ]
    );
    // Captured: code ... 8 32 <nil> <nil> <nil> utf8mb4 utf8mb4_bin
    //           varchar(8) UNI
    let code = of("code");
    assert_eq!(code[7], "varchar");
    assert_eq!(
        &code[8..16],
        [
            "8",
            "32",
            "<nil>",
            "<nil>",
            "<nil>",
            "utf8mb4",
            "utf8mb4_bin",
            "varchar(8)"
        ]
    );
    assert_eq!(code[16], "UNI");
    // Captured: v ... 7 YES bigint, no key
    let v = of("v");
    assert_eq!(v[5], "7", "COLUMN_DEFAULT");
    assert_eq!(v[6], "YES");
    assert_eq!(v[16], "");

    // A projected column is named as WRITTEN, which is captured TiDB
    // behavior: `select table_name ...` reports `table_name`, while
    // `select TABLE_NAME ...` reports `TABLE_NAME`.
    assert_eq!(
        query(
            &mut session,
            "SELECT table_name FROM information_schema.tables"
        )
        .0,
        ["table_name"]
    );
    assert_eq!(
        query(
            &mut session,
            "SELECT TABLE_NAME FROM information_schema.tables"
        )
        .0,
        ["TABLE_NAME"]
    );
    // A bare name works while that schema is current.
    session.run("USE information_schema").unwrap();
    assert_eq!(
        query(&mut session, "SELECT schema_name FROM schemata").0,
        ["schema_name"]
    );

    // An unimplemented information_schema table is an error, not empty
    // output that would look like a table with no rows. (`views` is
    // implemented -- see `views_appear_in_the_metadata_statements`.)
    assert!(session
        .run("SELECT * FROM information_schema.engines")
        .is_err());
}

/// KEY_COLUMN_USAGE, STATISTICS, TABLE_CONSTRAINTS and
/// REFERENTIAL_CONSTRAINTS -- the introspection tables JDBC/ORM drivers
/// query -- checked against output captured from a running TiDB for a
/// table with a BIGINT primary key, a UNIQUE column, and a two-column
/// plain KEY.
#[test]
fn information_schema_jdbc_tables() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, u INT UNIQUE, a INT, b INT, \
                 KEY idx_ab (a, b))",
        )
        .unwrap();

    let query = |session: &mut Session, sql: &str| match session.run_with_columns(sql).unwrap() {
        StmtOutput::Rows { columns, rows } => (
            columns
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            rows.into_iter()
                .map(|row| {
                    row.iter()
                        .map(|value| match value {
                            Datum::Null => "<nil>".to_owned(),
                            Datum::Int(v) => v.to_string(),
                            other => datum_text(other).unwrap_or_default(),
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
        ),
        other => panic!("expected rows, got {other:?}"),
    };

    // KEY_COLUMN_USAGE: captured header, and one row per PRIMARY/UNIQUE
    // column -- the plain KEY idx_ab does not appear here.
    let (names, rows) = query(
        &mut session,
        "SELECT * FROM information_schema.key_column_usage WHERE table_schema = 'test'",
    );
    assert_eq!(
        names,
        [
            "CONSTRAINT_CATALOG",
            "CONSTRAINT_SCHEMA",
            "CONSTRAINT_NAME",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "ORDINAL_POSITION",
            "POSITION_IN_UNIQUE_CONSTRAINT",
            "REFERENCED_TABLE_SCHEMA",
            "REFERENCED_TABLE_NAME",
            "REFERENCED_COLUMN_NAME",
        ]
    );
    assert_eq!(rows.len(), 2, "PRIMARY and u, not idx_ab");
    // Captured: [def test PRIMARY def test t id 1 1 <nil> <nil> <nil>]
    assert_eq!(
        rows[0],
        ["def", "test", "PRIMARY", "def", "test", "t", "id", "1", "1", "<nil>", "<nil>", "<nil>"]
    );
    // Captured: [def test u def test t u 1 <nil> <nil> <nil> <nil>]
    assert_eq!(
        rows[1],
        ["def", "test", "u", "def", "test", "t", "u", "1", "<nil>", "<nil>", "<nil>", "<nil>"]
    );

    // STATISTICS: captured header, and one row per indexed column
    // (PRIMARY, then idx_ab's two columns, then u), matching SHOW INDEX's
    // population under this table's own column set.
    let (names, rows) = query(
        &mut session,
        "SELECT * FROM information_schema.statistics WHERE table_schema = 'test'",
    );
    assert_eq!(
        names,
        [
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "NON_UNIQUE",
            "INDEX_SCHEMA",
            "INDEX_NAME",
            "SEQ_IN_INDEX",
            "COLUMN_NAME",
            "COLLATION",
            "CARDINALITY",
            "SUB_PART",
            "PACKED",
            "NULLABLE",
            "INDEX_TYPE",
            "COMMENT",
            "INDEX_COMMENT",
            "IS_VISIBLE",
            "Expression",
        ]
    );
    assert_eq!(rows.len(), 4);
    // Captured: [def test t 0 test PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil>]
    assert_eq!(
        rows[0],
        [
            "def", "test", "t", "0", "test", "PRIMARY", "1", "id", "A", "0", "<nil>", "<nil>", "",
            "BTREE", "", "", "YES", "<nil>"
        ]
    );
    // Captured: [def test t 1 test idx_ab 1 a A 0 <nil> <nil> YES BTREE   YES <nil>]
    assert_eq!(
        rows[1],
        [
            "def", "test", "t", "1", "test", "idx_ab", "1", "a", "A", "0", "<nil>", "<nil>", "YES",
            "BTREE", "", "", "YES", "<nil>"
        ]
    );
    assert_eq!(rows[2][6], "2", "idx_ab's second column, SEQ_IN_INDEX");
    assert_eq!(rows[2][7], "b");
    // Captured: [def test t 0 test u 1 u A 0 <nil> <nil> YES BTREE   YES <nil>]
    assert_eq!(
        rows[3],
        [
            "def", "test", "t", "0", "test", "u", "1", "u", "A", "0", "<nil>", "<nil>", "YES",
            "BTREE", "", "", "YES", "<nil>"
        ]
    );

    // TABLE_CONSTRAINTS: captured header, one row per PRIMARY/UNIQUE
    // constraint (not per column).
    let (names, rows) = query(
        &mut session,
        "SELECT * FROM information_schema.table_constraints WHERE table_schema = 'test'",
    );
    assert_eq!(
        names,
        [
            "CONSTRAINT_CATALOG",
            "CONSTRAINT_SCHEMA",
            "CONSTRAINT_NAME",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "CONSTRAINT_TYPE",
        ]
    );
    assert_eq!(
        rows,
        vec![
            vec!["def", "test", "PRIMARY", "test", "t", "PRIMARY KEY"],
            vec!["def", "test", "u", "test", "t", "UNIQUE"],
        ]
    );

    // REFERENTIAL_CONSTRAINTS: captured header, always empty in this
    // tier (no foreign keys).
    let (names, rows) = query(
        &mut session,
        "SELECT * FROM information_schema.referential_constraints",
    );
    assert_eq!(
        names,
        [
            "CONSTRAINT_CATALOG",
            "CONSTRAINT_SCHEMA",
            "CONSTRAINT_NAME",
            "UNIQUE_CONSTRAINT_CATALOG",
            "UNIQUE_CONSTRAINT_SCHEMA",
            "UNIQUE_CONSTRAINT_NAME",
            "MATCH_OPTION",
            "UPDATE_RULE",
            "DELETE_RULE",
            "TABLE_NAME",
            "REFERENCED_TABLE_NAME",
        ]
    );
    assert!(rows.is_empty());

    // A WHERE filter runs through the ordinary plan path.
    let (_, rows) = query(
            &mut session,
            "SELECT * FROM information_schema.statistics WHERE table_name = 't' AND index_name = 'idx_ab'",
        );
    assert_eq!(rows.len(), 2);
}

/// `SHOW TABLE STATUS`, checked against captured TiDB output -- the
/// 18-column header GUI clients read to list a schema.
///
/// NOT MODELLED, and reported the way Go reports an absent value rather
/// than invented: every size and count is 0, which is also what TiDB
/// answers without a statistics tier, and the three timestamps are NULL
/// because this tier stores none.
#[test]
fn show_table_status() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10))")
        .unwrap();
    session.run("CREATE TABLE u (x BIGINT)").unwrap();
    session.run("INSERT INTO t VALUES (1,'p'),(2,'q')").unwrap();

    match session.run_with_columns("SHOW TABLE STATUS").unwrap() {
        StmtOutput::Rows { columns, .. } => assert_eq!(
            columns
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            [
                "Name",
                "Engine",
                "Version",
                "Row_format",
                "Rows",
                "Avg_row_length",
                "Data_length",
                "Max_data_length",
                "Index_length",
                "Data_free",
                "Auto_increment",
                "Create_time",
                "Update_time",
                "Check_time",
                "Collation",
                "Checksum",
                "Create_options",
                "Comment",
            ]
        ),
        other => panic!("expected rows, got {other:?}"),
    }

    // Captured: one row per table, with the engine, version, row format
    // and collation TiDB reports.
    let rows = row_text(session.run("SHOW TABLE STATUS"));
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(rows[0][0], "t");
    assert_eq!(rows[1][0], "u");
    assert_eq!(rows[0][1], "InnoDB");
    assert_eq!(rows[0][2], "10");
    assert_eq!(rows[0][3], "Compact");
    assert_eq!(rows[0][14], "utf8mb4_bin");
    // Captured: Auto_increment is NULL for a table with no auto column.
    assert_eq!(rows[0][10], "NULL");

    // Captured: the LIKE filter narrows to one table.
    let filtered = row_text(session.run("SHOW TABLE STATUS LIKE 't'"));
    assert_eq!(filtered.len(), 1, "{filtered:?}");
    assert_eq!(filtered[0][0], "t");

    // A table with an auto column reports its next value there.
    session
        .run("CREATE TABLE g (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)")
        .unwrap();
    session.run("INSERT INTO g (v) VALUES (1), (2)").unwrap();
    let auto = row_text(session.run("SHOW TABLE STATUS LIKE 'g'"));
    assert_eq!(auto[0][10], "3", "{auto:?}");

    // The WHERE form filters the same virtual rows.
    let named = row_text(session.run("SHOW TABLE STATUS WHERE Name = 'u'"));
    assert_eq!(named.len(), 1, "{named:?}");
    assert_eq!(named[0][0], "u");
}

/// `SHOW INDEX` / `SHOW KEYS`, checked against captured TiDB output --
/// the full 17-column header and one row per index column.
#[test]
fn show_index_reports_each_index_column() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10), c BIGINT, \
                 UNIQUE KEY ub (b), KEY bc (b,c))",
        )
        .unwrap();

    match session.run_with_columns("SHOW INDEX FROM t").unwrap() {
        StmtOutput::Rows { columns, .. } => assert_eq!(
            columns
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            [
                "Table",
                "Non_unique",
                "Key_name",
                "Seq_in_index",
                "Column_name",
                "Collation",
                "Cardinality",
                "Sub_part",
                "Packed",
                "Null",
                "Index_type",
                "Comment",
                "Index_comment",
                "Visible",
                "Expression",
                "Clustered",
                "Global",
            ]
        ),
        other => panic!("expected rows, got {other:?}"),
    }

    // Captured: the clustered primary key first, then each index in
    // definition order, one row per index column with its 1-based
    // position. Non_unique is 0 for a unique index.
    let rows = row_text(session.run("SHOW INDEX FROM t"));
    let summary: Vec<Vec<&str>> = rows
        .iter()
        .map(|row| {
            vec![
                row[1].as_str(),  // Non_unique
                row[2].as_str(),  // Key_name
                row[3].as_str(),  // Seq_in_index
                row[4].as_str(),  // Column_name
                row[9].as_str(),  // Null
                row[15].as_str(), // Clustered
            ]
        })
        .collect();
    assert_eq!(
        summary,
        [
            ["0", "PRIMARY", "1", "a", "", "YES"],
            ["0", "ub", "1", "b", "YES", "NO"],
            ["1", "bc", "1", "b", "YES", "NO"],
            ["1", "bc", "2", "c", "YES", "NO"],
        ]
    );
    // Captured: SHOW KEYS is the same statement.
    assert_eq!(row_text(session.run("SHOW KEYS FROM t")), rows);
}

/// `SHOW STATUS`, checked against captured TiDB output: the columns are
/// `Variable_name` and `Value`, `Ssl_cipher` is empty, `Compression` is
/// `OFF`, LIKE and WHERE filter the rows, and GLOBAL scope drops the
/// session-only `Compression*` family.
#[test]
fn show_status() {
    let mut session = Session::new();

    // Captured: COLUMNS [Variable_name Value], ROW [Ssl_cipher ].
    match session
        .run_with_columns("SHOW STATUS LIKE 'Ssl_cipher'")
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
            assert_eq!(datum_text(&rows[0][0]).unwrap(), "Ssl_cipher");
            assert_eq!(datum_text(&rows[0][1]).unwrap(), "");
        }
        other => panic!("expected rows, got {other:?}"),
    }
    // Captured: ROW [Compression OFF].
    assert_eq!(
        row_text(session.run("SHOW STATUS LIKE 'Compression'")),
        [["Compression", "OFF"]]
    );
    // Captured: SHOW GLOBAL STATUS LIKE 'Ssl%' lists the whole family.
    assert_eq!(
        row_text(session.run("SHOW GLOBAL STATUS LIKE 'Ssl%'")),
        [
            ["Ssl_cipher", ""],
            ["Ssl_cipher_list", ""],
            ["Ssl_verify_mode", "0"],
            ["Ssl_version", ""],
        ]
    );
    // Captured: the WHERE form filters the same virtual rows.
    assert_eq!(
        row_text(session.run("SHOW STATUS WHERE Variable_name = 'Compression'")),
        [["Compression", "OFF"]]
    );
    // Captured: GLOBAL scope drops the session-only Compression* rows,
    // and SESSION is the unscoped spelling.
    let session_rows = row_text(session.run("SHOW SESSION STATUS"));
    assert!(
        session_rows.iter().any(|row| row[0] == "Compression"),
        "{session_rows:?}"
    );
    let global_rows = row_text(session.run("SHOW GLOBAL STATUS"));
    assert!(
        global_rows
            .iter()
            .all(|row| !row[0].starts_with("Compression")),
        "{global_rows:?}"
    );
    assert!(
        global_rows.iter().any(|row| row[0] == "Ssl_version"),
        "{global_rows:?}"
    );
}

/// `SHOW CHARSET`, `SHOW ENGINES`, and `SHOW COLLATION`, checked against
/// a mock-TiDB capture: 7 SHOW CHARSET rows, one InnoDB SHOW ENGINES row,
/// and 15 SHOW COLLATION rows (LIKE 'utf8mb4%' narrows to 5).
#[test]
fn show_charset_engines_collation() {
    let mut session = Session::new();

    assert_eq!(
        row_text(session.run("SHOW CHARSET")),
        [
            ["ascii", "US ASCII", "ascii_bin", "1"],
            ["binary", "binary", "binary", "1"],
            [
                "gb18030",
                "China National Standard GB18030",
                "gb18030_chinese_ci",
                "4"
            ],
            [
                "gbk",
                "Chinese Internal Code Specification",
                "gbk_chinese_ci",
                "2"
            ],
            ["latin1", "Latin1", "latin1_bin", "1"],
            ["utf8", "UTF-8 Unicode", "utf8_bin", "3"],
            ["utf8mb4", "UTF-8 Unicode", "utf8mb4_bin", "4"],
        ]
    );

    assert_eq!(
        row_text(session.run("SHOW ENGINES")),
        [[
            "InnoDB",
            "DEFAULT",
            "Supports transactions, row-level locking, and foreign keys",
            "YES",
            "YES",
            "YES",
        ]]
    );

    let collation_rows = row_text(session.run("SHOW COLLATION"));
    assert_eq!(collation_rows.len(), 15);
    assert_eq!(
        collation_rows[0],
        ["ascii_bin", "ascii", "65", "Yes", "Yes", "1", "PAD SPACE"]
    );
    assert_eq!(
        collation_rows[1],
        ["binary", "binary", "63", "Yes", "Yes", "1", "NO PAD"]
    );

    assert_eq!(
        row_text(session.run("SHOW COLLATION LIKE 'utf8mb4%'")),
        [
            [
                "utf8mb4_0900_ai_ci",
                "utf8mb4",
                "255",
                "",
                "Yes",
                "0",
                "NO PAD"
            ],
            [
                "utf8mb4_0900_bin",
                "utf8mb4",
                "309",
                "",
                "Yes",
                "1",
                "NO PAD"
            ],
            [
                "utf8mb4_bin",
                "utf8mb4",
                "46",
                "Yes",
                "Yes",
                "1",
                "PAD SPACE"
            ],
            [
                "utf8mb4_general_ci",
                "utf8mb4",
                "45",
                "",
                "Yes",
                "1",
                "PAD SPACE"
            ],
            [
                "utf8mb4_unicode_ci",
                "utf8mb4",
                "224",
                "",
                "Yes",
                "8",
                "PAD SPACE"
            ],
        ]
    );

    assert!(session.run("SHOW CHARSET WHERE Charset = 'utf8'").is_err());
}

/// `tidb_enable_fast_analyze` names a feature TiDB v7.5.0 REMOVED: turning it
/// on is accepted and warned about, turning it off is silent, and `SET GLOBAL`
/// warns the same way. Captured through `gorun` (see
/// `Session::warn_removed_feature_var`).
#[test]
fn setting_a_removed_feature_switch_on_warns() {
    let mut session = Session::new();

    session
        .run("SET @@session.tidb_enable_fast_analyze = 1")
        .unwrap();
    let expected = vec![vec![
        "Warning".to_owned(),
        "1105".to_owned(),
        "the fast analyze feature has already been removed in TiDB v7.5.0, so this will have \
         no effect"
            .to_owned(),
    ]];
    assert_eq!(row_text(session.run("SHOW WARNINGS")), expected);

    session
        .run("SET @@session.tidb_enable_fast_analyze = 0")
        .unwrap();
    assert!(row_text(session.run("SHOW WARNINGS")).is_empty());

    session
        .run("SET GLOBAL tidb_enable_fast_analyze = ON")
        .unwrap();
    assert_eq!(row_text(session.run("SHOW WARNINGS")), expected);
}

#[test]
fn show_warnings() {
    let mut session = Session::new();
    session
        .apply_set("SET tidb_enable_noop_functions = 'WARN'")
        .unwrap();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
    session.run("INSERT INTO t VALUES (1, 1)").unwrap();

    // Captured: the warning the statement raised, as Level/Code/Message.
    session.run("SELECT a FROM t LOCK IN SHARE MODE").unwrap();
    let expected = vec![vec![
        "Warning".to_owned(),
        "1235".to_owned(),
        "function LOCK IN SHARE MODE has only noop implementation in tidb now, use \
             tidb_enable_noop_functions to enable these functions"
            .to_owned(),
    ]];
    assert_eq!(row_text(session.run("SHOW WARNINGS")), expected);
    // Captured: SHOW WARNINGS does not consume what it reports.
    assert_eq!(row_text(session.run("SHOW WARNINGS")), expected);
    match session.run_with_columns("SHOW WARNINGS").unwrap() {
        StmtOutput::Rows { columns, .. } => assert_eq!(
            columns
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            ["Level", "Code", "Message"]
        ),
        other => panic!("expected rows, got {other:?}"),
    }
    // Captured: a warning is not an error, so SHOW ERRORS is empty.
    assert!(row_text(session.run("SHOW ERRORS")).is_empty());

    // Captured: the buffer belongs to the last statement, so an ordinary
    // statement empties it.
    session.run("SELECT a FROM t").unwrap();
    assert!(row_text(session.run("SHOW WARNINGS")).is_empty());

    // Captured: a failed statement leaves its own error in the buffer,
    // which both SHOW WARNINGS and SHOW ERRORS report.
    session
        .apply_set("SET tidb_enable_noop_functions = 'OFF'")
        .unwrap();
    assert!(session.run("SELECT a FROM t LOCK IN SHARE MODE").is_err());
    let reported = row_text(session.run("SHOW WARNINGS"));
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0][0], "Error");
    assert_eq!(reported[0][1], "1235");
    assert_eq!(row_text(session.run("SHOW ERRORS")), reported);

    // Captured: the count form reports a single count column.
    match session.run_with_columns("SHOW COUNT(*) WARNINGS").unwrap() {
        StmtOutput::Rows { columns, rows } => {
            assert_eq!(columns[0].0, "@@session.warning_count");
            assert_eq!(rows, vec![vec![Datum::Int(1)]]);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // A filter would silently report the wrong rows, so it is refused.
    assert!(matches!(
        session.run("SHOW WARNINGS WHERE 1"),
        Err(DriverError::Unsupported(_)) | Err(DriverError::Parse(_))
    ));
}

/// Captured from TiDB (`pkg/executor` mock store): `SHOW FULL COLUMNS`,
/// `SHOW CREATE TABLE` and `information_schema.columns` over one table
/// carrying every string form, so a `VARBINARY` is distinguishable from a
/// `VARCHAR` at every metadata surface.
#[test]
fn charset_and_collation_metadata_surfaces() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t1 (\
                 c_varchar VARCHAR(10), c_char CHAR(10), \
                 c_varbinary VARBINARY(10), c_binary BINARY(3), \
                 c_blob BLOB, c_text TEXT, c_tinytext TINYTEXT, c_longblob LONGBLOB, \
                 c_vc_cs VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, \
                 c_vc_bin VARCHAR(10) BINARY, \
                 c_enum ENUM('a','B'), c_set SET('a','B'), c_int INT)",
        )
        .unwrap();

    // SHOW FULL COLUMNS: the type text and the Collation cell, which is NULL
    // for every binary-charset column and for the integer one.
    let (_, rows) = query_text(&mut session, "SHOW FULL COLUMNS FROM t1");
    let types_and_collations: Vec<(String, String)> = rows
        .iter()
        .map(|row| (row[1].clone(), row[2].clone()))
        .collect();
    assert_eq!(
        types_and_collations,
        vec![
            ("varchar(10)".to_owned(), "utf8mb4_bin".to_owned()),
            ("char(10)".to_owned(), "utf8mb4_bin".to_owned()),
            ("varbinary(10)".to_owned(), "<nil>".to_owned()),
            ("binary(3)".to_owned(), "<nil>".to_owned()),
            ("blob".to_owned(), "<nil>".to_owned()),
            ("text".to_owned(), "utf8mb4_bin".to_owned()),
            ("tinytext".to_owned(), "utf8mb4_bin".to_owned()),
            ("longblob".to_owned(), "<nil>".to_owned()),
            ("varchar(10)".to_owned(), "utf8mb4_general_ci".to_owned()),
            ("varchar(10)".to_owned(), "utf8mb4_bin".to_owned()),
            ("enum('a','B')".to_owned(), "utf8mb4_bin".to_owned()),
            ("set('a','B')".to_owned(), "utf8mb4_bin".to_owned()),
            ("int".to_owned(), "<nil>".to_owned()),
        ]
    );

    // SHOW CREATE TABLE: only the column whose collation differs from the
    // table's prints a COLLATE clause.
    let create = show_create(&mut session, "t1");
    assert!(
        create.contains("`c_varbinary` varbinary(10) DEFAULT NULL"),
        "{create}"
    );
    assert!(
        create.contains("`c_binary` binary(3) DEFAULT NULL"),
        "{create}"
    );
    assert!(create.contains("`c_blob` blob DEFAULT NULL"), "{create}");
    assert!(create.contains("`c_text` text DEFAULT NULL"), "{create}");
    assert!(
        create.contains("`c_vc_cs` varchar(10) COLLATE utf8mb4_general_ci DEFAULT NULL"),
        "{create}"
    );
    assert!(
        create.contains("`c_vc_bin` varchar(10) DEFAULT NULL"),
        "{create}"
    );
    assert!(
        create.ends_with(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"),
        "{create}"
    );

    // information_schema.columns: the character length, the octet length
    // (which scales by the charset's bytes per character), and the
    // charset/collation names -- NULL for a binary-charset column.
    let (_, rows) = query_text(
        &mut session,
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, \
             CHARACTER_SET_NAME, COLLATION_NAME FROM information_schema.columns \
             WHERE table_name = 't1'",
    );
    assert_eq!(
        rows,
        vec![
            vec!["c_varchar", "varchar", "10", "40", "utf8mb4", "utf8mb4_bin"],
            vec!["c_char", "char", "10", "40", "utf8mb4", "utf8mb4_bin"],
            vec!["c_varbinary", "varbinary", "10", "10", "<nil>", "<nil>"],
            vec!["c_binary", "binary", "3", "3", "<nil>", "<nil>"],
            vec!["c_blob", "blob", "65535", "65535", "<nil>", "<nil>"],
            vec![
                "c_text",
                "text",
                "65535",
                "262140",
                "utf8mb4",
                "utf8mb4_bin"
            ],
            vec![
                "c_tinytext",
                "tinytext",
                "255",
                "1020",
                "utf8mb4",
                "utf8mb4_bin"
            ],
            vec![
                "c_longblob",
                "longblob",
                "4294967295",
                "4294967295",
                "<nil>",
                "<nil>"
            ],
            vec![
                "c_vc_cs",
                "varchar",
                "10",
                "40",
                "utf8mb4",
                "utf8mb4_general_ci"
            ],
            vec!["c_vc_bin", "varchar", "10", "40", "utf8mb4", "utf8mb4_bin"],
            vec!["c_enum", "enum", "1", "4", "utf8mb4", "utf8mb4_bin"],
            vec!["c_set", "set", "3", "12", "utf8mb4", "utf8mb4_bin"],
            vec!["c_int", "int", "<nil>", "<nil>", "<nil>", "<nil>"],
        ]
    );
}

/// `NUMERIC_PRECISION`, `NUMERIC_SCALE` and `DATETIME_PRECISION` across every
/// type that has one, on all three metadata surfaces.
///
/// ABSENT and ZERO are different answers here. `FLOAT`/`DOUBLE` report a
/// precision and NO scale, `YEAR` and `DATE` report neither cell, and a
/// temporal type reports its fractional-second digits in a THIRD cell. Go
/// reaches all of that from one if-chain in `dataForColumnsInTable`
/// (`pkg/executor/infoschema_reader.go`) over `getNumericPrecision` plus
/// `mysql.GetDefaultFieldLengthAndDecimal`, which is why an unwritten
/// `DECIMAL` reports 10,0 while a written `DECIMAL(20,4)` reports 20,4.
///
/// The unsigned pairs are Go's too: BIGINT UNSIGNED widens 19 -> 20, and
/// MEDIUMINT UNSIGNED widens 7 -> 8 (MySQL bug 69042, reproduced on purpose).
#[test]
fn numeric_and_temporal_precision_surfaces() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t3 (\
                 c_tiny TINYINT, c_small SMALLINT, c_medium MEDIUMINT, \
                 c_medium_u MEDIUMINT UNSIGNED, c_int INT, c_big BIGINT, \
                 c_big_u BIGINT UNSIGNED, \
                 c_dec DECIMAL, c_dec_md DECIMAL(20,4), \
                 c_float FLOAT, c_float_md FLOAT(10,3), c_double DOUBLE, \
                 c_bit BIT, c_bit8 BIT(8), c_year YEAR, \
                 c_date DATE, c_datetime DATETIME, c_datetime3 DATETIME(3), \
                 c_ts TIMESTAMP NULL, c_time TIME(6))",
        )
        .unwrap();

    let (_, rows) = query_text(
        &mut session,
        "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, \
             DATETIME_PRECISION FROM information_schema.columns WHERE table_name = 't3'",
    );
    assert_eq!(
        rows,
        vec![
            vec!["c_tiny", "tinyint", "3", "0", "<nil>"],
            vec!["c_small", "smallint", "5", "0", "<nil>"],
            vec!["c_medium", "mediumint", "7", "0", "<nil>"],
            vec!["c_medium_u", "mediumint", "8", "0", "<nil>"],
            vec!["c_int", "int", "10", "0", "<nil>"],
            vec!["c_big", "bigint", "19", "0", "<nil>"],
            vec!["c_big_u", "bigint", "20", "0", "<nil>"],
            vec!["c_dec", "decimal", "10", "0", "<nil>"],
            vec!["c_dec_md", "decimal", "20", "4", "<nil>"],
            vec!["c_float", "float", "12", "<nil>", "<nil>"],
            vec!["c_float_md", "float", "10", "3", "<nil>"],
            vec!["c_double", "double", "22", "<nil>", "<nil>"],
            vec!["c_bit", "bit", "1", "0", "<nil>"],
            vec!["c_bit8", "bit", "8", "0", "<nil>"],
            vec!["c_year", "year", "<nil>", "<nil>", "<nil>"],
            vec!["c_date", "date", "<nil>", "<nil>", "<nil>"],
            vec!["c_datetime", "datetime", "<nil>", "<nil>", "0"],
            vec!["c_datetime3", "datetime", "<nil>", "<nil>", "3"],
            vec!["c_ts", "timestamp", "<nil>", "<nil>", "0"],
            vec!["c_time", "time", "<nil>", "<nil>", "6"],
        ]
    );

    // The two PRINTED-type surfaces must keep agreeing with each other while
    // the cells above change: SHOW COLUMNS and SHOW CREATE TABLE both spell
    // the width, which the DATA_TYPE cell above never does.
    let (_, rows) = query_text(&mut session, "SHOW COLUMNS FROM t3");
    assert_eq!(
        rows.iter().map(|row| row[1].clone()).collect::<Vec<_>>(),
        vec![
            "tinyint",
            "smallint",
            "mediumint",
            "mediumint unsigned",
            "int",
            "bigint",
            "bigint unsigned",
            "decimal(10,0)",
            "decimal(20,4)",
            "float",
            "float(10,3)",
            "double",
            // `year(4)` keeps its width: the display-width deprecation covers
            // the INTEGER types only (oracle: `tests/integrationtest/r`).
            "bit(1)",
            "bit(8)",
            "year(4)",
            "date",
            "datetime",
            "datetime(3)",
            "timestamp",
            "time(6)",
        ]
    );
    let create = show_create(&mut session, "t3");
    for spelling in [
        "`c_medium_u` mediumint unsigned",
        "`c_big_u` bigint unsigned",
        "`c_dec` decimal(10,0)",
        "`c_dec_md` decimal(20,4)",
        "`c_float` float",
        "`c_float_md` float(10,3)",
        "`c_bit` bit(1)",
        "`c_datetime3` datetime(3)",
        "`c_time` time(6)",
    ] {
        assert!(
            create.contains(spelling),
            "{spelling} missing from {create}"
        );
    }
}

/// Captured from TiDB over `... DEFAULT CHARSET=latin1`: the table tail
/// reports latin1/latin1_bin, a column whose charset differs prints the full
/// `CHARACTER SET ... COLLATE ...` pair, and `CHARACTER SET binary` on a
/// VARCHAR reports a `varbinary`. The octet length follows the charset:
/// latin1 is 1 byte per character, utf8mb4 is 4.
#[test]
fn table_default_charset_flows_into_every_surface() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t2 (a VARCHAR(10), b VARCHAR(10) CHARACTER SET utf8mb4, \
                 c VARCHAR(10) CHARACTER SET latin1, d VARCHAR(10) CHARACTER SET binary) \
                 DEFAULT CHARSET=latin1",
        )
        .unwrap();

    let (_, rows) = query_text(&mut session, "SHOW FULL COLUMNS FROM t2");
    assert_eq!(
        rows.iter()
            .map(|row| (row[1].clone(), row[2].clone()))
            .collect::<Vec<_>>(),
        vec![
            ("varchar(10)".to_owned(), "latin1_bin".to_owned()),
            ("varchar(10)".to_owned(), "utf8mb4_bin".to_owned()),
            ("varchar(10)".to_owned(), "latin1_bin".to_owned()),
            ("varbinary(10)".to_owned(), "<nil>".to_owned()),
        ]
    );

    assert_eq!(
        show_create(&mut session, "t2"),
        "CREATE TABLE `t2` (\n  \
             `a` varchar(10) DEFAULT NULL,\n  \
             `b` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  \
             `c` varchar(10) DEFAULT NULL,\n  \
             `d` varbinary(10) DEFAULT NULL\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin"
    );

    let (_, rows) = query_text(
        &mut session,
        "SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, \
             CHARACTER_SET_NAME, COLLATION_NAME FROM information_schema.columns \
             WHERE table_name = 't2'",
    );
    assert_eq!(
        rows,
        vec![
            vec!["a", "10", "10", "latin1", "latin1_bin"],
            vec!["b", "10", "40", "utf8mb4", "utf8mb4_bin"],
            vec!["c", "10", "10", "latin1", "latin1_bin"],
            vec!["d", "10", "10", "<nil>", "<nil>"],
        ]
    );

    // SHOW TABLE STATUS reports the table's own collation.
    let (_, rows) = query_text(&mut session, "SHOW TABLE STATUS LIKE 't2'");
    assert_eq!(rows[0][14], "latin1_bin");
}

/// A non-UTF-8 byte string is rejected by a utf8mb4 column and accepted by a
/// binary one -- the write-path consequence of the column carrying a real
/// charset. Captured: TiDB answers 1366 "Incorrect string value '\xFF' for
/// column 'b'" for the utf8mb4 column and stores `0xFFFE7A` in the binary one.
#[test]
fn a_utf8mb4_column_validates_its_bytes_and_a_binary_column_does_not() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (b VARCHAR(20), vb VARBINARY(20))")
        .unwrap();
    assert!(matches!(
        session.run("INSERT INTO t (b) VALUES (x'FFFE7A')"),
        Err(DriverError::IncorrectValue { .. })
    ));
    session
        .run("INSERT INTO t (vb) VALUES (x'FFFE7A')")
        .unwrap();
    assert_eq!(
        query_text(&mut session, "SELECT HEX(vb) FROM t").1,
        vec![vec!["FFFE7A".to_owned()]]
    );
}

/// A `CHECK` constraint with `tidb_enable_check_constraint` at its OFF
/// default. Every expectation is captured from real TiDB -- the
/// `SHOW CREATE TABLE` text through `rust/difftests/gorun`, the warning
/// through testkit's `SHOW WARNINGS`, the insert outcome through both:
///
/// ```text
/// create table ck (a int, check (a > 0))     -- OK, Warning 1105
/// show create table ck                       -- NO `CONSTRAINT ... CHECK` clause
/// insert into ck values (-1)                 -- OK; the constraint is gone
/// select constraint_name from information_schema.check_constraints  -- empty
/// ```
///
/// TiDB DISCARDS the constraint rather than storing it unenforced, so
/// discarding it here is faithful: storing it would make this very
/// `SHOW CREATE TABLE` grow a clause TiDB does not print.
#[test]
fn a_check_constraint_is_accepted_discarded_and_warned_about() {
    let mut session = Session::new();
    let create_table_text = |session: &mut Session, name: &str| match session
        .run_with_columns(&format!("SHOW CREATE TABLE {name}"))
        .unwrap()
    {
        StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
        other => panic!("expected rows, got {other:?}"),
    };
    let warnings = |session: &Session| {
        session
            .warnings()
            .iter()
            .map(|w| (w.code, w.message.clone()))
            .collect::<Vec<_>>()
    };
    let is_off = || (1105u16, "tidb_enable_check_constraint is off".to_owned());

    // A table-level CHECK, named and unnamed: each is accepted, warns once,
    // and NEITHER reaches the restored DDL.
    session
        .run("create table ck (a int, b int, check (a > 0), constraint c2 check (b > 0))")
        .unwrap();
    // One warning per discarded constraint, matching Go's per-constraint
    // `AppendWarning`.
    assert_eq!(warnings(&session), vec![is_off(), is_off()]);
    assert_eq!(
        create_table_text(&mut session, "ck"),
        "CREATE TABLE `ck` (\n  \
             `a` int DEFAULT NULL,\n  \
             `b` int DEFAULT NULL\n\
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    // The form written inline on a column takes the same path.
    session
        .run("create table ck3 (a int check (a > 5), b int)")
        .unwrap();
    assert_eq!(warnings(&session), vec![is_off()]);
    assert_eq!(
        create_table_text(&mut session, "ck3"),
        "CREATE TABLE `ck3` (\n  \
             `a` int DEFAULT NULL,\n  \
             `b` int DEFAULT NULL\n\
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    // `[NOT] ENFORCED` changes nothing while the variable is off: TiDB
    // discards the constraint before the keyword could matter.
    session
        .run("create table ck4 (a int, check (a > 0) not enforced)")
        .unwrap();
    session
        .run("create table ck5 (a int, check (a > 0) enforced)")
        .unwrap();
    assert_eq!(warnings(&session), vec![is_off()]);

    // The constraint really is gone, so a violating row inserts.
    session.run("insert into ck values (-1, -1)").unwrap();
    assert_eq!(
        query_text(&mut session, "select a, b from ck").1,
        vec![vec!["-1".to_owned(), "-1".to_owned()]]
    );
}

/// Turning `tidb_enable_check_constraint` ON changes what a `CHECK`
/// constraint MEANS -- TiDB then stores it (auto-named `<table>_chk_<N>`),
/// prints it in `SHOW CREATE TABLE`, and enforces it with error 3819
/// (captured: "Check constraint 'ck3_chk_1' is violated."). None of that is
/// modelled, so the DDL is refused outright rather than silently discarding a
/// constraint the session just asked to have honoured.
#[test]
fn a_check_constraint_is_refused_when_the_variable_is_on() {
    let mut session = Session::new();
    session
        .run("set @@global.tidb_enable_check_constraint = 1")
        .unwrap();
    assert!(matches!(
        session.run("create table ck (a int, check (a > 0))"),
        Err(DriverError::Unsupported(
            "CHECK constraints are only modelled with tidb_enable_check_constraint off"
        ))
    ));
    // A table with no CHECK constraint is unaffected by the variable.
    session.run("create table plain (a int)").unwrap();
}

/// A system variable whose assignment Go CLAMPS rather than refuses reports
/// `1292 Truncated incorrect <name> value: '<original>'`, and the value that
/// lands is the clamped one.
///
/// Every row is transcribed from the recorded `SHOW WARNINGS` blocks in
/// `tests/integrationtest/r/session/variable.result`. Note the pairing each
/// row asserts: the STORED value is the clamp, while the WARNING names the
/// value exactly as typed.
///
/// The multibyte alias is the load-bearing row. `中文测试` plus one digit is 5
/// CHARACTERS but 13 BYTES, so thirteen such groups are 65 characters and 169
/// bytes. Go cuts at 64 RUNES, dropping exactly the final `c`; a byte-wise cut
/// would land inside the fifth group -- and inside a UTF-8 sequence. The
/// stored value alone separates the two rules.
#[test]
fn clamping_a_system_variable_warns_1292_with_the_original_value() {
    let alias_65: String = (1..=13)
        .map(|i| format!("中文测试{}", "1234567890abc".chars().nth(i - 1).unwrap()))
        .collect();
    assert_eq!(alias_65.chars().count(), 65);
    assert_eq!(alias_65.len(), 169);
    let alias_64: String = alias_65.chars().take(64).collect();
    let digits_70 = "0123456789".repeat(7);
    let digits_64: String = digits_70.chars().take(64).collect();
    let spaced = format!("abc{}1", " ".repeat(68));

    let cases: Vec<(String, &str, String, String)> = vec![
        (
            "set @@global.tidb_memory_usage_alarm_ratio=1.1".to_owned(),
            "@@global.tidb_memory_usage_alarm_ratio",
            "1".to_owned(),
            "tidb_memory_usage_alarm_ratio value: '1.1'".to_owned(),
        ),
        (
            "set @@global.tidb_memory_usage_alarm_ratio=-1".to_owned(),
            "@@global.tidb_memory_usage_alarm_ratio",
            "0".to_owned(),
            "tidb_memory_usage_alarm_ratio value: '-1'".to_owned(),
        ),
        (
            "set @@global.tidb_memory_usage_alarm_keep_record_num=0".to_owned(),
            "@@global.tidb_memory_usage_alarm_keep_record_num",
            "1".to_owned(),
            "tidb_memory_usage_alarm_keep_record_num value: '0'".to_owned(),
        ),
        (
            "set @@global.tidb_memory_usage_alarm_keep_record_num=10001".to_owned(),
            "@@global.tidb_memory_usage_alarm_keep_record_num",
            "10000".to_owned(),
            "tidb_memory_usage_alarm_keep_record_num value: '10001'".to_owned(),
        ),
        (
            format!("set @@tidb_session_alias='{digits_70}'"),
            "@@tidb_session_alias",
            digits_64,
            format!("tidb_session_alias value: '{digits_70}'"),
        ),
        (
            format!("set @@tidb_session_alias='{alias_65}'"),
            "@@tidb_session_alias",
            alias_64,
            format!("tidb_session_alias value: '{alias_65}'"),
        ),
        (
            "set @@tidb_session_alias='abc  '".to_owned(),
            "@@tidb_session_alias",
            "abc".to_owned(),
            "tidb_session_alias value: 'abc  '".to_owned(),
        ),
        (
            format!("set @@tidb_session_alias='{spaced}'"),
            "@@tidb_session_alias",
            "abc".to_owned(),
            format!("tidb_session_alias value: '{spaced}'"),
        ),
        (
            "set @@group_concat_max_len=1".to_owned(),
            "@@group_concat_max_len",
            "4".to_owned(),
            "group_concat_max_len value: '1'".to_owned(),
        ),
    ];

    for (set, read, stored, warning) in cases {
        let mut session = Session::new();
        session.run(&set).unwrap();
        assert_eq!(
            row_text(session.run("SHOW WARNINGS")),
            vec![vec![
                "Warning".to_owned(),
                "1292".to_owned(),
                format!("Truncated incorrect {warning}"),
            ]],
            "warning for `{set}`"
        );
        assert_eq!(
            row_text(session.run(&format!("select {read}"))),
            vec![vec![stored]],
            "stored value for `{set}`"
        );
    }
}
