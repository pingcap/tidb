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
    match session.run_with_columns("SHOW DATABASES").unwrap() {
        StmtOutput::Rows { columns, rows } => {
            assert_eq!(columns[0].0, "Database");
            assert_eq!(
                rows.iter()
                    .map(|row| datum_text(&row[0]).unwrap())
                    .collect::<Vec<_>>(),
                vec![
                    "INFORMATION_SCHEMA".to_owned(),
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
            vec!["id", "bigint(20)", "NO", "PRI", "NULL", ""],
            // A column that is the whole of a unique index is UNI.
            vec!["code", "varchar(8)", "YES", "UNI", "NULL", ""],
            // A column leading a non-unique index is MUL.
            vec!["tag", "varchar(4)", "YES", "MUL", "NULL", ""],
            // An unindexed column has no key flag.
            vec!["v", "bigint(20)", "YES", "", "NULL", ""],
        ]
    );

    // Go reports auto_increment in Extra; captured from TiDB's DESCRIBE:
    // [[id bigint(20) NO PRI <nil> auto_increment] [v bigint(20) YES  <nil> ]]
    session
        .run("CREATE TABLE ai (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)")
        .unwrap();
    assert_eq!(
        describe(&mut session, "DESCRIBE ai").1,
        vec![
            vec!["id", "bigint(20)", "NO", "PRI", "NULL", "auto_increment"],
            vec!["v", "bigint(20)", "YES", "", "NULL", ""],
        ]
    );

    // A column's stored DEFAULT shows in the Default column.
    session
        .run("CREATE TABLE withdef (a BIGINT DEFAULT 7, b VARCHAR(4) DEFAULT 'zz')")
        .unwrap();
    assert_eq!(
        describe(&mut session, "DESCRIBE withdef").1,
        vec![
            vec!["a", "bigint(20)", "YES", "", "7", ""],
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
/// `[a int(11) <nil> YES  <nil>  select,insert,update,references ]`
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
                "int(11)",
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
             `id` bigint(20) NOT NULL,\n  \
             `code` varchar(8) DEFAULT NULL,\n  \
             `tag` varchar(4) DEFAULT NULL,\n  \
             `v` bigint(20) DEFAULT NULL,\n  \
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
             `a` bigint(20) DEFAULT '7',\n  \
             `b` varchar(4) DEFAULT 'zz',\n  \
             `c` bigint(20) NOT NULL,\n  \
             `d` bigint(20) DEFAULT NULL\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    assert_eq!(
        create(
            &mut session,
            "create table t4 (a bigint, b bigint, key ab (a,b))",
            "t4"
        ),
        "CREATE TABLE `t4` (\n  \
             `a` bigint(20) DEFAULT NULL,\n  \
             `b` bigint(20) DEFAULT NULL,\n  \
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
             `a` bigint(20) DEFAULT NULL,\n  \
             `b` bigint(20) DEFAULT NULL,\n  \
             KEY `kb` (`b`),\n  \
             UNIQUE KEY `a` (`a`),\n  \
             UNIQUE KEY `b` (`b`)\n\
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
             `id` bigint(20) NOT NULL AUTO_INCREMENT,\n  \
             `v` bigint(20) DEFAULT NULL,\n  \
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
            "bigint(20)",
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
