// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Direct source-order transcreation of `pkg/parser/parser_test.go`.

use tidb_ast::{
    AdminStmt, DmlStmt, Expr, ExtendedStatsType, QueryStmt, RestoreContext, RestoreFlags, Stmt,
};
use tidb_parser::{parse, parse_multi, parse_multi_with_sql_mode, parse_with_sql_mode, SqlMode};

#[test]
fn test_insert_statement_memory_allocation() {
    let sql = format!("insert t values (1){}", ",(1)".repeat(1000));
    let Stmt::Dml(statement) = parse(&sql).expect("the 1,001-row INSERT parses") else {
        panic!("expected DML")
    };
    let DmlStmt::Insert(insert) = statement.into_inner() else {
        panic!("expected INSERT")
    };
    // Rust's workspace forbids allocator hooks. Measure the complete retained
    // row/Expr backing storage instead; this catches the same accidental Vec
    // over-allocation that motivated Go's TotalAlloc regression threshold.
    let allocated = insert.rows.capacity() * std::mem::size_of::<Vec<Expr>>()
        + insert
            .rows
            .iter()
            .map(|row| {
                row.capacity() * std::mem::size_of::<Expr>()
                    + row
                        .iter()
                        .map(|expr| match expr {
                            Expr::Int(value) => value.capacity(),
                            _ => 0,
                        })
                        .sum::<usize>()
            })
            .sum::<usize>();
    assert!(
        allocated < 500 * 1024,
        "the retained INSERT AST uses {allocated} bytes (Expr={} bytes); Go's source limit is 512000",
        std::mem::size_of::<Expr>()
    );
}

/// Rust's public parser entry point owns no reusable yacc state, so the stale
/// semantic-value bug from Go `TestIssue45898` is eliminated structurally.
/// Keep the original failing-then-successful sequence executable.
#[test]
fn test_issue_45898_parse_error_does_not_poison_the_next_parse() {
    assert!(parse("a.").is_err());
    assert_eq!(
        parse("select count(1) from t").unwrap().restore(),
        "SELECT COUNT(1) FROM `t`"
    );
}

/// Go `TestSideEffect`: a failed executable-comment parse must not affect the
/// next parse performed through the same public parser authority.
#[test]
fn test_side_effect() {
    assert!(parse("create table t /*!50100 'abc', 'abc' */;").is_err());
    assert_eq!(parse("show tables;").unwrap().restore(), "SHOW TABLES");
}

/// The non-table assertions from Go `TestStatisticsOps`; its complete SQL and
/// restore matrix is exercised by the generated integration differential.
#[test]
fn test_statistics_ops_semantics() {
    let Stmt::Admin(statement) =
        parse("create statistics if not exists stats1 (cardinality) on t(a,b,c)")
            .expect("CREATE STATISTICS parses")
    else {
        panic!("expected administrative statement")
    };
    let AdminStmt::CreateStatistics(statistics) = statement.into_inner() else {
        panic!("expected CREATE STATISTICS")
    };
    assert!(statistics.if_not_exists);
    assert_eq!(statistics.name, "stats1");
    assert_eq!(statistics.stats_type, ExtendedStatsType::Cardinality);
    assert_eq!(statistics.table, ["t"]);
    assert_eq!(statistics.columns, [["a"], ["b"], ["c"]]);
}

/// Complete custom-context matrix from Go `TestCTEBindings`.
#[test]
fn test_cte_bindings() {
    let context = RestoreContext::new(
        RestoreFlags::STRING_SINGLE_QUOTES
            | RestoreFlags::SPACES_AROUND_BINARY_OPERATION
            | RestoreFlags::STRING_WITHOUT_CHARSET
            | RestoreFlags::NAME_BACK_QUOTES,
    )
    .with_default_db("test");
    for (source, valid, expected) in [
        ("WITH `cte` AS (SELECT * from t) SELECT `col1`,`col2` FROM `cte`", true, "WITH `cte` AS (SELECT * FROM `test`.`t`) SELECT `col1`,`col2` FROM `cte`"),
        ("WITH `cte` (col1, col2) AS (SELECT * from t UNION ALL SELECT 3,4) SELECT col1, col2 FROM cte;", true, "WITH `cte` (`col1`, `col2`) AS (SELECT * FROM `test`.`t` UNION ALL SELECT 3,4) SELECT `col1`,`col2` FROM `cte`"),
        ("WITH `cte` AS (SELECT * from t), cte2 as (select * from cte) SELECT `col1`,`col2` FROM `cte`", true, "WITH `cte` AS (SELECT * FROM `test`.`t`), `cte2` AS (SELECT * FROM `cte`) SELECT `col1`,`col2` FROM `cte`"),
        ("WITH RECURSIVE cte (n) AS (  SELECT * from t  UNION ALL  SELECT n + 1 FROM cte WHERE n < 5)SELECT * FROM cte;", true, "WITH RECURSIVE `cte` (`n`) AS (SELECT * FROM `test`.`t` UNION ALL SELECT `n` + 1 FROM `cte` WHERE `n` < 5) SELECT * FROM `cte`"),
        ("with cte(a) as (select * from t) update t, cte set t.a=1  where t.a=cte.a;", true, "WITH `cte` (`a`) AS (SELECT * FROM `test`.`t`) UPDATE (`test`.`t`) JOIN `cte` SET `t`.`a`=1 WHERE `t`.`a` = `cte`.`a`"),
        ("with cte(a) as (select * from t) delete t from t, cte where t.a=cte.a;", true, "WITH `cte` (`a`) AS (SELECT * FROM `test`.`t`) DELETE `test`.`t` FROM (`test`.`t`) JOIN `cte` WHERE `t`.`a` = `cte`.`a`"),
        ("WITH cte1 AS (SELECT * from t) SELECT * FROM (WITH cte2 AS (SELECT * from cte1) SELECT * FROM cte2 JOIN cte1) AS dt;", true, "WITH `cte1` AS (SELECT * FROM `test`.`t`) SELECT * FROM (WITH `cte2` AS (SELECT * FROM `cte1`) SELECT * FROM `cte2` JOIN `cte1`) AS `dt`"),
        ("WITH cte AS (SELECT * from t) SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM cte;", true, "WITH `cte` AS (SELECT * FROM `test`.`t`) SELECT /*+ MAX_EXECUTION_TIME(1000)*/ * FROM `cte`"),
        ("with cte as (table t) table cte;", true, "WITH `cte` AS (TABLE `test`.`t`) TABLE `cte`"),
        ("with cte as (select * from t) select 1 union with cte as (select * from t) select * from cte;", false, ""),
        ("with cte as (select * from t) (select * from t);", true, "WITH `cte` AS (SELECT * FROM `test`.`t`) (SELECT * FROM `test`.`t`)"),
        ("with cte as (select 1) (select 1 union select * from t)", true, "WITH `cte` AS (SELECT 1) (SELECT 1 UNION SELECT * FROM `test`.`t`)"),
        ("select * from (with cte as (select * from t) select 1 union select * from t) qn", true, "SELECT * FROM (WITH `cte` AS (SELECT * FROM `test`.`t`) SELECT 1 UNION SELECT * FROM `test`.`t`) AS `qn`"),
        ("select * from t where 1 > (with cte as (select * from t) select * from cte)", true, "SELECT * FROM `test`.`t` WHERE 1 > (WITH `cte` AS (SELECT * FROM `test`.`t`) SELECT * FROM `cte`)"),
        ("( with cte(n) as ( select * from t )  select n+1 from cte  union select n+2 from cte) union select 1", true, "(WITH `cte` (`n`) AS (SELECT * FROM `test`.`t`) SELECT `n` + 1 FROM `cte` UNION SELECT `n` + 2 FROM `cte`) UNION SELECT 1"),
        ("( with cte(n) as ( select * from t )  select n+1 from cte) union select * from t", true, "(WITH `cte` (`n`) AS (SELECT * FROM `test`.`t`) SELECT `n` + 1 FROM `cte`) UNION SELECT * FROM `test`.`t`"),
        ("with cte as (select * from t union select * from cte) select * from cte", true, "WITH `cte` AS (SELECT * FROM `test`.`t` UNION SELECT * FROM `cte`) SELECT * FROM `cte`"),
    ] {
        let parsed = parse(source);
        if !valid {
            assert!(parsed.is_err(), "source unexpectedly parsed: {source}");
            continue;
        }
        let statement = parsed.unwrap_or_else(|error| panic!("{source}: {error:?}"));
        assert_eq!(
            statement.restore_with_context(&context),
            expected,
            "source: {source}"
        );
    }
}

fn assert_restore(source: &str, expected: &str) {
    let statement = parse(source).unwrap_or_else(|error| panic!("{source}: {error:?}"));
    let restored = statement.restore();
    assert_eq!(restored, expected, "source SQL: {source}");
    assert_eq!(
        parse(&restored).unwrap().restore(),
        restored,
        "round trip: {source}"
    );
}

/// Complete diagnostic table from Go `pkg/parser/parser_test.go`'s
/// `TestErrorMsg`.
#[test]
fn test_error_msg() {
    for (sql, expected) in [
        ("select1 1", "line 1 column 7 near \"select1 1\" "),
        (
            "select 1 from1 dual",
            "line 1 column 19 near \"dual\" ",
        ),
        (
            "select * from t1 join t2 from t1.a = t2.a;",
            "line 1 column 29 near \"from t1.a = t2.a;\" ",
        ),
        (
            "select * from t1 join t2 one t1.a = t2.a;",
            "line 1 column 31 near \"t1.a = t2.a;\" ",
        ),
        (
            "select * from t1 join t2 on t1.a >>> t2.a;",
            "line 1 column 36 near \"> t2.a;\" ",
        ),
        (
            "create table t(f_year year(5))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;",
            "[parser:1818]Supports only YEAR or YEAR(4) column",
        ),
        (
            "create table ``.t (id int);",
            "[parser:1102]Incorrect database name ''",
        ),
        (
            "create table ` `.t (id int);",
            "[parser:1102]Incorrect database name ' '",
        ),
        (
            "select ifnull(a,0) & ifnull(a,0) like '55' ESCAPE '\\\\a' from t;",
            "[parser:1210]Incorrect arguments to ESCAPE",
        ),
        (
            "load data infile 'aaa' into table aaa FIELDS Enclosed by '\\\\b';",
            "[parser:1083]Field separator argument is not what is expected; check the manual",
        ),
        (
            "load data infile 'aaa' into table aaa FIELDS Escaped by '\\\\b';",
            "[parser:1083]Field separator argument is not what is expected; check the manual",
        ),
        (
            "load data infile 'aaa' into table aaa FIELDS Enclosed by '\\\\b' Escaped by '\\\\b';",
            "[parser:1083]Field separator argument is not what is expected; check the manual",
        ),
        (
            "ALTER DATABASE `` CHARACTER SET = ''",
            "[parser:1115]Unknown character set: ''",
        ),
        (
            "ALTER DATABASE t CHARACTER SET = ''",
            "[parser:1115]Unknown character set: ''",
        ),
        (
            "ALTER SCHEMA t CHARACTER SET = 'SOME_INVALID_CHARSET'",
            "[parser:1115]Unknown character set: 'SOME_INVALID_CHARSET'",
        ),
        (
            "ALTER DATABASE t COLLATE = ''",
            "[ddl:1273]Unknown collation: ''",
        ),
        (
            "ALTER SCHEMA t COLLATE = 'SOME_INVALID_COLLATION'",
            "[ddl:1273]Unknown collation: 'SOME_INVALID_COLLATION'",
        ),
        (
            "ALTER DATABASE CHARSET = 'utf8mb4' COLLATE = 'utf8_bin'",
            "line 1 column 24 near \"= 'utf8mb4' COLLATE = 'utf8_bin'\" ",
        ),
        (
            "ALTER DATABASE t ENCRYPTION = ''",
            "[parser:1525]Incorrect argument (should be Y or N) value: ''",
        ),
        ("ALTER DATABASE", "line 1 column 14 near \"\" "),
        (
            "ALTER SCHEMA `ANY_DB_NAME`",
            "line 1 column 26 near \"\" ",
        ),
        (
            "alter table t partition by range FIELDS(a)",
            "[ddl:1492]For RANGE partitions each partition must be defined",
        ),
        (
            "alter table t partition by list FIELDS(a)",
            "[ddl:1492]For LIST partitions each partition must be defined",
        ),
        (
            "alter table t partition by list FIELDS(a,b,c)",
            "[ddl:1492]For LIST partitions each partition must be defined",
        ),
        (
            "alter table t lock = first",
            "[parser:1801]Unknown LOCK type 'first'",
        ),
        (
            "alter table t lock = start",
            "[parser:1801]Unknown LOCK type 'start'",
        ),
        (
            "alter table t lock = commit",
            "[parser:1801]Unknown LOCK type 'commit'",
        ),
        (
            "alter table t lock = binlog",
            "[parser:1801]Unknown LOCK type 'binlog'",
        ),
        (
            "alter table t lock = randomStr123",
            "[parser:1801]Unknown LOCK type 'randomStr123'",
        ),
        (
            "create table t (a longtext unicode)",
            "[parser:1115]Unknown character set: 'ucs2'",
        ),
        (
            "create table t (a long byte, b text unicode)",
            "[parser:1115]Unknown character set: 'ucs2'",
        ),
        (
            "create table t (a long ascii, b long unicode)",
            "[parser:1115]Unknown character set: 'ucs2'",
        ),
        (
            "create table t (a text unicode, b mediumtext ascii, c int)",
            "[parser:1115]Unknown character set: 'ucs2'",
        ),
        (
            "select 1 collate some_unknown_collation",
            "[ddl:1273]Unknown collation: 'some_unknown_collation'",
        ),
    ] {
        let error = parse(sql).unwrap_err();
        assert_eq!(error.compatibility_message(sql), expected, "{sql}");
    }
}

/// Complete cases from Go `pkg/parser/parser_test.go`'s `TestParserErrMsg`.
#[test]
fn test_parser_err_msg() {
    for (sql, expected) in [
        (
            "delete from t where a = 7 or 1=1/*' and b = 'p'",
            Some("near '/*' and b = 'p'' at line 1"),
        ),
        (
            "delete from t where a = 7 or\n 1=1/*' and b = 'p'",
            Some("near '/*' and b = 'p'' at line 2"),
        ),
        ("select 1/*", Some("near '/*' at line 1")),
        ("select 1/* comment */", None),
        ("select a.b()", None),
        ("SELECT foo.bar('baz');", None),
    ] {
        match expected {
            Some(expected) => {
                let error = parse(sql).unwrap_err();
                assert_eq!(error.compatibility_message(sql), expected, "source: {sql}");
            }
            None => {
                parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
            }
        }
    }
}

#[test]
fn test_simple() {
    let reserved = [
        "add",
        "all",
        "alter",
        "analyze",
        "and",
        "as",
        "asc",
        "between",
        "bigint",
        "binary",
        "blob",
        "both",
        "by",
        "call",
        "cascade",
        "case",
        "change",
        "character",
        "check",
        "collate",
        "column",
        "constraint",
        "convert",
        "create",
        "cross",
        "current_date",
        "current_time",
        "current_timestamp",
        "current_user",
        "database",
        "databases",
        "day_hour",
        "day_microsecond",
        "day_minute",
        "day_second",
        "decimal",
        "default",
        "delete",
        "desc",
        "describe",
        "distinct",
        "distinctRow",
        "div",
        "double",
        "drop",
        "dual",
        "else",
        "enclosed",
        "escaped",
        "exists",
        "explain",
        "false",
        "float",
        "fetch",
        "for",
        "force",
        "foreign",
        "from",
        "fulltext",
        "grant",
        "group",
        "having",
        "hour_microsecond",
        "hour_minute",
        "hour_second",
        "if",
        "ignore",
        "in",
        "index",
        "infile",
        "inner",
        "insert",
        "int",
        "into",
        "integer",
        "interval",
        "is",
        "join",
        "key",
        "keys",
        "kill",
        "leading",
        "left",
        "like",
        "ilike",
        "limit",
        "lines",
        "load",
        "localtime",
        "localtimestamp",
        "lock",
        "longblob",
        "longtext",
        "mediumblob",
        "maxvalue",
        "mediumint",
        "mediumtext",
        "minute_microsecond",
        "minute_second",
        "mod",
        "not",
        "no_write_to_binlog",
        "null",
        "numeric",
        "on",
        "option",
        "optionally",
        "or",
        "order",
        "outer",
        "partition",
        "precision",
        "primary",
        "procedure",
        "range",
        "read",
        "real",
        "recursive",
        "references",
        "regexp",
        "rename",
        "repeat",
        "replace",
        "revoke",
        "restrict",
        "right",
        "rlike",
        "schema",
        "schemas",
        "second_microsecond",
        "select",
        "set",
        "show",
        "smallint",
        "starting",
        "table",
        "terminated",
        "then",
        "tinyblob",
        "tinyint",
        "tinytext",
        "to",
        "trailing",
        "true",
        "union",
        "unique",
        "unlock",
        "unsigned",
        "update",
        "use",
        "using",
        "utc_date",
        "values",
        "varbinary",
        "varchar",
        "when",
        "where",
        "write",
        "xor",
        "year_month",
        "zerofill",
        "generated",
        "virtual",
        "stored",
        "usage",
        "delayed",
        "high_priority",
        "low_priority",
        "cumeDist",
        "denseRank",
        "firstValue",
        "lag",
        "lastValue",
        "lead",
        "nthValue",
        "ntile",
        "over",
        "percentRank",
        "rank",
        "row",
        "rows",
        "rowNumber",
        "window",
        "linear",
        "match",
        "until",
        "placement",
        "tablesample",
        "failedLoginAttempts",
        "passwordLockTime",
    ];
    for keyword in reserved {
        for sql in [
            format!("SELECT * FROM db.{keyword};"),
            format!("SELECT * FROM {keyword}.desc"),
            format!("SELECT t.{keyword} FROM t"),
        ] {
            parse(&sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        }
    }

    let unreserved = [
        "add_columnar_replica_on_demand",
        "auto_increment",
        "after",
        "begin",
        "bit",
        "bool",
        "boolean",
        "charset",
        "columns",
        "commit",
        "date",
        "datediff",
        "datetime",
        "deallocate",
        "do",
        "from_days",
        "end",
        "engine",
        "engines",
        "execute",
        "extended",
        "first",
        "file",
        "full",
        "local",
        "names",
        "offset",
        "password",
        "prepare",
        "quick",
        "rollback",
        "savepoint",
        "session",
        "signed",
        "start",
        "global",
        "tables",
        "tablespace",
        "target",
        "text",
        "time",
        "timestamp",
        "tidb",
        "transaction",
        "truncate",
        "unknown",
        "value",
        "warnings",
        "year",
        "now",
        "substr",
        "subpartition",
        "subpartitions",
        "substring",
        "mode",
        "any",
        "some",
        "user",
        "identified",
        "collation",
        "comment",
        "avg_row_length",
        "checksum",
        "compression",
        "connection",
        "key_block_size",
        "max_rows",
        "min_rows",
        "national",
        "quarter",
        "escape",
        "grants",
        "status",
        "fields",
        "triggers",
        "language",
        "delay_key_write",
        "isolation",
        "partitions",
        "repeatable",
        "committed",
        "uncommitted",
        "only",
        "serializable",
        "level",
        "curtime",
        "variables",
        "dayname",
        "version",
        "btree",
        "hash",
        "row_format",
        "dynamic",
        "fixed",
        "compressed",
        "compact",
        "redundant",
        "1 sql_no_cache",
        "1 sql_cache",
        "action",
        "round",
        "enable",
        "disable",
        "reverse",
        "space",
        "privileges",
        "get_lock",
        "release_lock",
        "sleep",
        "no",
        "greatest",
        "least",
        "binlog",
        "hex",
        "unhex",
        "function",
        "indexes",
        "from_unixtime",
        "processlist",
        "events",
        "less",
        "than",
        "timediff",
        "ln",
        "log",
        "log2",
        "log10",
        "timestampdiff",
        "pi",
        "proxy",
        "quote",
        "none",
        "super",
        "shared",
        "exclusive",
        "always",
        "stats",
        "stats_meta",
        "stats_histogram",
        "stats_buckets",
        "stats_healthy",
        "tidb_version",
        "replication",
        "slave",
        "client",
        "max_connections_per_hour",
        "max_queries_per_hour",
        "max_updates_per_hour",
        "max_user_connections",
        "event",
        "reload",
        "routine",
        "temporary",
        "following",
        "preceding",
        "unbounded",
        "respect",
        "nulls",
        "current",
        "last",
        "against",
        "expansion",
        "chain",
        "error",
        "general",
        "nvarchar",
        "pack_keys",
        "p",
        "shard_row_id_bits",
        "pre_split_regions",
        "constraints",
        "role",
        "replicas",
        "policy",
        "s3",
        "strict",
        "running",
        "stop",
        "preserve",
        "placement",
        "attributes",
        "attribute",
        "resource",
        "burstable",
        "calibrate",
        "masking",
        "rollup",
    ];
    for keyword in unreserved {
        let sql = format!("SELECT {keyword} FROM tbl;");
        parse(&sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }

    parse("SELECT id+?, id+? from t;").unwrap();
    assert_eq!(
        parse_multi(
            "CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED); -- foo\nSelect --1 from foo;"
        )
        .unwrap()
        .len(),
        2
    );
    assert!(matches!(
        parse_multi("/*!40101 SET character_set_client = utf8 */;")
            .unwrap()
            .as_slice(),
        [Stmt::Session(_)]
    ));

    let Stmt::Dml(dml) = parse("insert into blobtable (a) values ('/*! truncated */');").unwrap()
    else {
        panic!("expected INSERT");
    };
    let DmlStmt::Insert(insert) = dml.into_inner() else {
        panic!("expected INSERT");
    };
    assert!(
        matches!(insert.rows.as_slice(), [row] if matches!(row.as_slice(), [Expr::String(value)] if value == "/*! truncated */"))
    );

    for sql in [
        "SELECT CONVERT('111', SIGNED);",
        "/* some comments */ SELECT CONVERT('111', SIGNED) ;",
        "/* some comments */ /*comment*/ SELECT CONVERT('111', SIGNED) ;",
        "SELECT /*comment*/ CONVERT('111', SIGNED) ;",
        "SELECT CONVERT('111', /*comment*/ SIGNED) ;",
        "SELECT CONVERT('111', SIGNED) /*comment*/;",
        "create table t (c int key);",
        "create table t1(a NVARCHAR(100));",
        "use quote;",
        "select b'';",
        "select B'';",
        "CREATE TABLE t(_sms smallint signed, _smu smallint unsigned);",
        "CREATE TABLE t(c1 NATIONAL CHARACTER(10));",
        "insert into tb(v) (select v from tb);",
        "SELECT a as c having c = a;",
        "SELECT 9223372036854775807;",
        "SELECT 9223372036854775808;",
        "select 99e+r10 from t1;",
        "select t./*123*/*,@c3:=0 from t order by t.c1;",
        "select t.1e from test.t;",
        "select t. `a` > 10 from t;",
    ] {
        parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }

    parse(
        "CREATE TABLE t(a tinyint signed,b smallint signed,c mediumint signed,d int signed,e int1 signed,f int2 signed,g int3 signed,h int4 signed,i int8 signed,j integer signed,k bigint signed,l bool signed,m boolean signed);",
    )
    .unwrap();

    let ansi = SqlMode {
        ansi_quotes: true,
        ..SqlMode::default()
    };
    let statement = parse_with_sql_mode(r#"select t."dot"=10 from t;"#, ansi).unwrap();
    let Stmt::Query(query) = statement else {
        panic!("expected SELECT");
    };
    assert!(matches!(query.into_inner(), QueryStmt::Select(_)));
}

#[test]
fn test_special_comments() {
    assert!(parse(r#"SELECT /*! '\' */;"#).is_err());

    let mode = SqlMode {
        no_backslash_escapes: true,
        ..SqlMode::default()
    };
    assert!(tidb_parser::parse_with_sql_mode(r#"SELECT /*! '\' */;"#, mode).is_ok());

    let statements = parse_multi_with_sql_mode("/*! SET x = 1; SELECT 2 */", mode).unwrap();
    assert_eq!(statements.len(), 2);
    assert_eq!(statements[0].text(), b"/*! SET x = 1;");
    assert_eq!(statements[1].text(), b" SELECT 2 */");

    let statement = parse("SELECT /*+ 😅 */ SLEEP(1);").unwrap();
    assert_eq!(statement.restore(), "SELECT SLEEP(1)");
}

#[test]
fn test_recommend_index() {
    for (source, expected) in [
        ("recommend index run", "RECOMMEND INDEX RUN"),
        (
            "recommend index run with A = 1",
            "RECOMMEND INDEX RUN WITH A = 1",
        ),
        (
            "recommend index run with A = 1, B = 2",
            "RECOMMEND INDEX RUN WITH A = 1, B = 2",
        ),
        (
            "recommend index run for 'select * from t where a=1'",
            "RECOMMEND INDEX RUN FOR 'select * from t where a=1'",
        ),
        (
            "recommend index run for 'select * from t where a=1' with A = 1",
            "RECOMMEND INDEX RUN FOR 'select * from t where a=1' WITH A = 1",
        ),
        (
            "recommend index run for 'select * from t where a=1' with A = 1, B = 2",
            "RECOMMEND INDEX RUN FOR 'select * from t where a=1' WITH A = 1, B = 2",
        ),
        ("recommend index show option", "RECOMMEND INDEX SHOW OPTION"),
        ("recommend index apply 1", "RECOMMEND INDEX APPLY 1"),
        ("recommend index ignore 1", "RECOMMEND INDEX IGNORE 1"),
        ("recommend index set A = 1", "RECOMMEND INDEX SET A = 1"),
        (
            "recommend index set A = 1, B = 2",
            "RECOMMEND INDEX SET A = 1, B = 2",
        ),
        (
            "recommend index set A = 1, B = 2, C = 3",
            "RECOMMEND INDEX SET A = 1, B = 2, C = 3",
        ),
    ] {
        assert_restore(source, expected);
    }
}

#[test]
fn test_admin_stmt() {
    let rows = [
        ("admin show ddl;", true, "ADMIN SHOW DDL"),
        ("admin show ddl jobs;", true, "ADMIN SHOW DDL JOBS"),
        (
            "admin show ddl jobs where id > 0;",
            true,
            "ADMIN SHOW DDL JOBS WHERE `id`>0",
        ),
        (
            "admin show ddl jobs 20 where id=0;",
            true,
            "ADMIN SHOW DDL JOBS 20 WHERE `id`=0",
        ),
        ("admin show ddl jobs -1;", false, ""),
        (
            "admin show ddl job queries 1",
            true,
            "ADMIN SHOW DDL JOB QUERIES 1",
        ),
        (
            "admin show ddl job queries 1, 2, 3, 4",
            true,
            "ADMIN SHOW DDL JOB QUERIES 1, 2, 3, 4",
        ),
        (
            "admin show ddl job queries limit 5",
            true,
            "ADMIN SHOW DDL JOB QUERIES LIMIT 0, 5",
        ),
        (
            "admin show ddl job queries limit 5, 10",
            true,
            "ADMIN SHOW DDL JOB QUERIES LIMIT 5, 10",
        ),
        (
            "admin show ddl job queries limit 3 offset 2",
            true,
            "ADMIN SHOW DDL JOB QUERIES LIMIT 2, 3",
        ),
        (
            "admin show ddl job queries limit 22 offset 0",
            true,
            "ADMIN SHOW DDL JOB QUERIES LIMIT 0, 22",
        ),
        (
            "admin show t1 next_row_id",
            true,
            "ADMIN SHOW `t1` NEXT_ROW_ID",
        ),
        (
            "admin create workload snapshot;",
            true,
            "ADMIN CREATE WORKLOAD SNAPSHOT",
        ),
        (
            "admin check table t1, t2;",
            true,
            "ADMIN CHECK TABLE `t1`, `t2`",
        ),
        (
            "admin check index tableName idxName;",
            true,
            "ADMIN CHECK INDEX `tableName` idxName",
        ),
        (
            "admin check index tableName idxName (1, 2), (4, 5);",
            true,
            "ADMIN CHECK INDEX `tableName` idxName (1,2), (4,5)",
        ),
        (
            "admin checksum table t1, t2;",
            true,
            "ADMIN CHECKSUM TABLE `t1`, `t2`",
        ),
        ("admin cancel ddl jobs 1", true, "ADMIN CANCEL DDL JOBS 1"),
        (
            "admin cancel ddl jobs 1, 2",
            true,
            "ADMIN CANCEL DDL JOBS 1, 2",
        ),
        (
            "admin pause ddl jobs 1, 3",
            true,
            "ADMIN PAUSE DDL JOBS 1, 3",
        ),
        ("admin pause ddl jobs 5", true, "ADMIN PAUSE DDL JOBS 5"),
        ("admin pause ddl jobs", false, ""),
        ("admin pause ddl jobs str_not_num", false, ""),
        (
            "admin resume ddl jobs 1, 2",
            true,
            "ADMIN RESUME DDL JOBS 1, 2",
        ),
        ("admin resume ddl jobs 3", true, "ADMIN RESUME DDL JOBS 3"),
        ("admin resume ddl jobs", false, ""),
        ("admin resume ddl jobs str_not_num", false, ""),
        (
            "admin recover index t1 idx_a",
            true,
            "ADMIN RECOVER INDEX `t1` idx_a",
        ),
        (
            "admin cleanup index t1 idx_a",
            true,
            "ADMIN CLEANUP INDEX `t1` idx_a",
        ),
        ("admin show slow top 3", true, "ADMIN SHOW SLOW TOP 3"),
        (
            "admin show slow top internal 7",
            true,
            "ADMIN SHOW SLOW TOP INTERNAL 7",
        ),
        (
            "admin show slow top all 9",
            true,
            "ADMIN SHOW SLOW TOP ALL 9",
        ),
        (
            "admin show slow recent 11",
            true,
            "ADMIN SHOW SLOW RECENT 11",
        ),
        (
            "admin reload expr_pushdown_blacklist",
            true,
            "ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST",
        ),
        (
            "admin plugins disable audit, whitelist",
            true,
            "ADMIN PLUGINS DISABLE audit, whitelist",
        ),
        (
            "admin plugins enable audit, whitelist",
            true,
            "ADMIN PLUGINS ENABLE audit, whitelist",
        ),
        ("admin flush bindings", true, "ADMIN FLUSH BINDINGS"),
        ("admin capture bindings", true, "ADMIN CAPTURE BINDINGS"),
        ("admin evolve bindings", true, "ADMIN EVOLVE BINDINGS"),
        ("admin reload bindings", true, "ADMIN RELOAD BINDINGS"),
        (
            "admin reload cluster bindings",
            true,
            "ADMIN RELOAD CLUSTER BINDINGS",
        ),
        (
            "admin reload statistics",
            true,
            "ADMIN RELOAD STATS_EXTENDED",
        ),
        (
            "admin reload stats_extended",
            true,
            "ADMIN RELOAD STATS_EXTENDED",
        ),
        (
            "admin flush instance plan_cache",
            true,
            "ADMIN FLUSH INSTANCE PLAN_CACHE",
        ),
        (
            "admin flush session plan_cache",
            true,
            "ADMIN FLUSH SESSION PLAN_CACHE",
        ),
        (
            "admin flush global plan_cache",
            true,
            "ADMIN FLUSH GLOBAL PLAN_CACHE",
        ),
        (
            "admin set bdr role primary",
            true,
            "ADMIN SET BDR ROLE PRIMARY",
        ),
        (
            "admin set bdr role secondary",
            true,
            "ADMIN SET BDR ROLE SECONDARY",
        ),
        ("admin unset bdr role", true, "ADMIN UNSET BDR ROLE"),
        ("admin show bdr role", true, "ADMIN SHOW BDR ROLE"),
        (
            "admin alter ddl jobs 1 thread = 2",
            true,
            "ADMIN ALTER DDL JOBS 1 thread = 2",
        ),
        ("admin alter ddl jobs 1 thread = ", false, ""),
        ("admin alter ddl jobs 1 thread", false, ""),
        (
            "admin alter ddl jobs 1 batch_size = 3",
            true,
            "ADMIN ALTER DDL JOBS 1 batch_size = 3",
        ),
        ("admin alter ddl jobs 1 batch_size = ", false, ""),
        ("admin alter ddl jobs 1 batch_size", false, ""),
        (
            "admin alter ddl jobs 1 max_write_speed = 4",
            true,
            "ADMIN ALTER DDL JOBS 1 max_write_speed = 4",
        ),
        (
            "admin alter ddl jobs 1 max_write_speed = _UTF8MB4'4MiB'",
            true,
            "ADMIN ALTER DDL JOBS 1 max_write_speed = _UTF8MB4'4MiB'",
        ),
        ("admin alter ddl jobs 1 max_write_speed = ", false, ""),
        ("admin alter ddl jobs 1 max_write_speed", false, ""),
    ];
    for (source, valid, expected) in rows {
        match parse(source) {
            Ok(statement) if valid => {
                assert_eq!(statement.restore(), expected, "source SQL: {source}");
                assert_eq!(
                    parse(expected).unwrap().restore(),
                    expected,
                    "round trip: {source}"
                );
            }
            Err(_) if !valid => {}
            result => panic!("source SQL: {source}; valid={valid}; result={result:?}"),
        }
    }
}

#[test]
fn test_dml_stmt() {
    let rows = [
        ("", true, ""),
        (";", true, ""),
        ("INSERT INTO foo VALUES (1234)", true, "INSERT INTO `foo` VALUES (1234)"),
        ("INSERT INTO foo VALUES (1234, 5678)", true, "INSERT INTO `foo` VALUES (1234,5678)"),
        ("INSERT INTO t1 (SELECT * FROM t2)", true, "INSERT INTO `t1` (SELECT * FROM `t2`)"),
        ("INSERT INTO t partition (p0) values(1234)", true, "INSERT INTO `t` PARTITION(`p0`) VALUES (1234)"),
        ("REPLACE INTO t partition (p0) values(1234)", true, "REPLACE INTO `t` PARTITION(`p0`) VALUES (1234)"),
        ("INSERT INTO t partition (p0, p1, p2) values(1234)", true, "INSERT INTO `t` PARTITION(`p0`, `p1`, `p2`) VALUES (1234)"),
        ("REPLACE INTO t partition (p0, p1, p2) values(1234)", true, "REPLACE INTO `t` PARTITION(`p0`, `p1`, `p2`) VALUES (1234)"),
        ("INSERT INTO foo VALUES (1 || 2)", true, "INSERT INTO `foo` VALUES (1 OR 2)"),
        ("INSERT INTO foo VALUES (1 | 2)", true, "INSERT INTO `foo` VALUES (1|2)"),
        ("INSERT INTO foo VALUES (false || true)", true, "INSERT INTO `foo` VALUES (FALSE OR TRUE)"),
        ("INSERT INTO foo VALUES (bar(5678))", true, "INSERT INTO `foo` VALUES (BAR(5678))"),
        ("INSERT INTO foo VALUES ()", true, "INSERT INTO `foo` VALUES ()"),
        ("SELECT * FROM t", true, "SELECT * FROM `t`"),
        ("SELECT * FROM t AS u", true, "SELECT * FROM `t` AS `u`"),
        ("SELECT * FROM t, v", true, "SELECT * FROM (`t`) JOIN `v`"),
        ("SELECT * FROM t AS u, v", true, "SELECT * FROM (`t` AS `u`) JOIN `v`"),
        ("SELECT * FROM t, v AS w", true, "SELECT * FROM (`t`) JOIN `v` AS `w`"),
        ("SELECT * FROM t AS u, v AS w", true, "SELECT * FROM (`t` AS `u`) JOIN `v` AS `w`"),
        ("SELECT * FROM foo, bar, foo", true, "SELECT * FROM ((`foo`) JOIN `bar`) JOIN `foo`"),
        ("SELECT DISTINCTS * FROM t", false, ""),
        ("SELECT DISTINCT * FROM t", true, "SELECT DISTINCT * FROM `t`"),
        ("SELECT DISTINCTROW * FROM t", true, "SELECT DISTINCT * FROM `t`"),
        ("SELECT ALL * FROM t", true, "SELECT ALL * FROM `t`"),
        ("SELECT DISTINCT ALL * FROM t", false, ""),
        ("SELECT DISTINCTROW ALL * FROM t", false, ""),
        ("INSERT INTO foo (a) VALUES (42)", true, "INSERT INTO `foo` (`a`) VALUES (42)"),
        ("INSERT INTO foo (a,) VALUES (42,)", false, ""),
        ("INSERT INTO foo (a,b) VALUES (42,314)", true, "INSERT INTO `foo` (`a`,`b`) VALUES (42,314)"),
        ("INSERT INTO foo (a,b,) VALUES (42,314)", false, ""),
        ("INSERT INTO foo (a,b,) VALUES (42,314,)", false, ""),
        ("INSERT INTO foo () VALUES ()", true, "INSERT INTO `foo` () VALUES ()"),
        ("INSERT INTO foo VALUE ()", true, "INSERT INTO `foo` VALUES ()"),
        ("INSERT INTO tt VALUES (01000001783);", true, "INSERT INTO `tt` VALUES (1000001783)"),
        ("INSERT INTO tt VALUES (default);", true, "INSERT INTO `tt` VALUES (DEFAULT)"),
        ("REPLACE INTO foo VALUES (1 || 2)", true, "REPLACE INTO `foo` VALUES (1 OR 2)"),
        ("REPLACE INTO foo VALUES (1 | 2)", true, "REPLACE INTO `foo` VALUES (1|2)"),
        ("REPLACE INTO foo VALUES (false || true)", true, "REPLACE INTO `foo` VALUES (FALSE OR TRUE)"),
        ("REPLACE INTO foo VALUES (bar(5678))", true, "REPLACE INTO `foo` VALUES (BAR(5678))"),
        ("REPLACE INTO foo VALUES ()", true, "REPLACE INTO `foo` VALUES ()"),
        ("REPLACE INTO foo (a,b) VALUES (42,314)", true, "REPLACE INTO `foo` (`a`,`b`) VALUES (42,314)"),
        ("REPLACE INTO foo (a,b,) VALUES (42,314)", false, ""),
        ("REPLACE INTO foo (a,b,) VALUES (42,314,)", false, ""),
        ("REPLACE INTO foo () VALUES ()", true, "REPLACE INTO `foo` () VALUES ()"),
        ("REPLACE INTO foo VALUE ()", true, "REPLACE INTO `foo` VALUES ()"),
        ("SELECT stuff.id\n\t\t\tFROM stuff\n\t\t\tWHERE stuff.value >= ALL (SELECT stuff.value\n\t\t\tFROM stuff)", true, "SELECT `stuff`.`id` FROM `stuff` WHERE `stuff`.`value`>=ALL (SELECT `stuff`.`value` FROM `stuff`)"),
        ("BEGIN", true, "START TRANSACTION"),
        ("START TRANSACTION", true, "START TRANSACTION"),
        ("COMMIT", true, "COMMIT"),
        ("COMMIT AND NO CHAIN", true, "COMMIT"),
        ("COMMIT NO RELEASE", true, "COMMIT"),
        ("COMMIT AND NO CHAIN NO RELEASE", true, "COMMIT"),
        ("COMMIT AND NO CHAIN RELEASE", true, "COMMIT RELEASE"),
        ("COMMIT AND CHAIN NO RELEASE", true, "COMMIT AND CHAIN"),
        ("COMMIT AND CHAIN RELEASE", false, ""),
        ("ROLLBACK", true, "ROLLBACK"),
        ("ROLLBACK AND NO CHAIN", true, "ROLLBACK"),
        ("ROLLBACK NO RELEASE", true, "ROLLBACK"),
        ("ROLLBACK AND NO CHAIN NO RELEASE", true, "ROLLBACK"),
        ("ROLLBACK AND NO CHAIN RELEASE", true, "ROLLBACK RELEASE"),
        ("ROLLBACK AND CHAIN NO RELEASE", true, "ROLLBACK AND CHAIN"),
        ("ROLLBACK AND CHAIN RELEASE", false, ""),
        ("BEGIN;\n\t\t\tINSERT INTO foo VALUES (42, 3.14);\n\t\t\tINSERT INTO foo VALUES (-1, 2.78);\n\t\tCOMMIT;", true, "START TRANSACTION; INSERT INTO `foo` VALUES (42,3.14); INSERT INTO `foo` VALUES (-1,2.78); COMMIT"),
        ("BEGIN;\n\t\t\tINSERT INTO tmp SELECT * from bar;\n\t\t\tSELECT * from tmp;\n\t\tROLLBACK;", true, "START TRANSACTION; INSERT INTO `tmp` SELECT * FROM `bar`; SELECT * FROM `tmp`; ROLLBACK"),
        ("SAVEPOINT x", true, "SAVEPOINT x"),
        ("RELEASE SAVEPOINT x", true, "RELEASE SAVEPOINT x"),
        ("ROLLBACK TO x", true, "ROLLBACK TO x"),
        ("ROLLBACK TO X", true, "ROLLBACK TO X"),
        ("ROLLBACK TO SAVEPOINT x", true, "ROLLBACK TO x"),
        ("TABLE t", true, "TABLE `t`"),
        ("(TABLE t)", true, "(TABLE `t`)"),
        ("TABLE t1, t2", false, ""),
        ("TABLE t ORDER BY b", true, "TABLE `t` ORDER BY `b`"),
        ("TABLE t LIMIT 3", true, "TABLE `t` LIMIT 3"),
        ("TABLE t ORDER BY b LIMIT 3", true, "TABLE `t` ORDER BY `b` LIMIT 3"),
        ("TABLE t ORDER BY b LIMIT 3 OFFSET 2", true, "TABLE `t` ORDER BY `b` LIMIT 2,3"),
        ("TABLE t ORDER BY b LIMIT 2,3", true, "TABLE `t` ORDER BY `b` LIMIT 2,3"),
        ("SELECT * FROM t LIMIT 18446744073709551615", true, "SELECT * FROM `t` LIMIT 18446744073709551615"),
        ("SELECT * FROM t LIMIT 18446744073709551616 OFFSET 3", false, ""),
        ("SELECT * FROM t LIMIT 10 OFFSET 18446744073709551616", false, ""),
        ("SELECT * FROM t LIMIT 18446744073709551616, 10", false, ""),
        ("SELECT * FROM t LIMIT 10, 18446744073709551616", false, ""),
        ("INSERT INTO ta TABLE tb", true, "INSERT INTO `ta` TABLE `tb`"),
        ("INSERT INTO t.a TABLE t.b", true, "INSERT INTO `t`.`a` TABLE `t`.`b`"),
        ("REPLACE INTO ta TABLE tb", true, "REPLACE INTO `ta` TABLE `tb`"),
        ("REPLACE INTO t.a TABLE t.b", true, "REPLACE INTO `t`.`a` TABLE `t`.`b`"),
        ("TABLE t1 INTO OUTFILE 'a.txt'", true, "TABLE `t1` INTO OUTFILE 'a.txt'"),
        ("TABLE t ORDER BY a INTO OUTFILE '/tmp/abc'", true, "TABLE `t` ORDER BY `a` INTO OUTFILE '/tmp/abc'"),
        ("CREATE TABLE t.a TABLE t.b", true, "CREATE TABLE `t`.`a` AS TABLE `t`.`b`"),
        ("CREATE TABLE ta TABLE tb", true, "CREATE TABLE `ta` AS TABLE `tb`"),
        ("CREATE TABLE ta (x INT) TABLE tb", true, "CREATE TABLE `ta` (`x` INT) AS TABLE `tb`"),
        ("CREATE VIEW v AS TABLE t", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS TABLE `t`"),
        ("CREATE VIEW v AS (TABLE t)", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS (TABLE `t`)"),
        ("SELECT * FROM t1 WHERE a IN (TABLE t2)", true, "SELECT * FROM `t1` WHERE `a` IN (TABLE `t2`)"),
        ("VALUES ROW(1)", true, "VALUES ROW(1)"),
        ("VALUES ROW()", true, "VALUES ROW()"),
        ("VALUES ROW(1, default)", true, "VALUES ROW(1,DEFAULT)"),
        ("VALUES ROW(1), ROW(2,3)", true, "VALUES ROW(1), ROW(2,3)"),
        ("VALUES (1,2)", false, ""),
        ("VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8)", true, "VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8)"),
        ("VALUES ROW(1,s,3.1), ROW(5,y,9.9)", true, "VALUES ROW(1,`s`,3.1), ROW(5,`y`,9.9)"),
        ("VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8) LIMIT 3", true, "VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8) LIMIT 3"),
        ("VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8) ORDER BY a", true, "VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8) ORDER BY `a`"),
        ("VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8) ORDER BY a LIMIT 2", true, "VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8) ORDER BY `a` LIMIT 2"),
        ("VALUES ROW(1,-2,3), ROW(5,7,9) INTO OUTFILE 'a.txt'", true, "VALUES ROW(1,-2,3), ROW(5,7,9) INTO OUTFILE 'a.txt'"),
        ("VALUES ROW(1,-2,3), ROW(5,7,9) ORDER BY a INTO OUTFILE '/tmp/abc'", true, "VALUES ROW(1,-2,3), ROW(5,7,9) ORDER BY `a` INTO OUTFILE '/tmp/abc'"),
        ("CREATE TABLE ta VALUES ROW(1)", true, "CREATE TABLE `ta` AS VALUES ROW(1)"),
        ("CREATE TABLE ta AS VALUES ROW(1)", true, "CREATE TABLE `ta` AS VALUES ROW(1)"),
        ("CREATE VIEW a AS VALUES ROW(1)", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `a` AS VALUES ROW(1)"),
        ("SELECT a.b.c FROM t", true, "SELECT `a`.`b`.`c` FROM `t`"),
        ("SELECT a.b.*.c FROM t", false, ""),
        ("SELECT a.b.* FROM t", true, "SELECT `a`.`b`.* FROM `t`"),
        ("SELECT a FROM t", true, "SELECT `a` FROM `t`"),
        ("SELECT a.b.c.d FROM t", false, ""),
        ("DO 1", true, "DO 1"),
        ("DO 1, sleep(1)", true, "DO 1, SLEEP(1)"),
        ("DO 1 from t", false, ""),
        ("load data local infile '/tmp/t.csv' into table t1 fields terminated by ',' optionally enclosed by '\"' ignore 1 lines", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t1` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES"),
        ("load data infile '/tmp/t.csv' into table t", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t`"),
        ("load data infile '/tmp/t.csv' into table t character set utf8", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` CHARACTER SET utf8"),
        ("load data infile '/tmp/t.csv' into table t fields terminated by 'ab'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab'"),
        ("load data infile '/tmp/t.csv' into table t columns terminated by 'ab'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab'"),
        ("load data infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b'"),
        ("load data infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*'"),
        ("load data infile '/tmp/t.csv' into table t lines starting by 'ab'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` LINES STARTING BY 'ab'"),
        ("load data infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` LINES STARTING BY 'ab' TERMINATED BY 'xy'"),
        ("load data infile '/tmp/t.csv' into table t fields terminated by 'ab' lines terminated by 'xy'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab' LINES TERMINATED BY 'xy'"),
        ("load data infile '/tmp/t.csv' into table t terminated by 'xy' fields terminated by 'ab'", false, ""),
        ("load data local infile '/tmp/t.csv' into table t", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t`"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab'"),
        ("load data local infile '/tmp/t.csv' into table t columns terminated by 'ab'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab'"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b'"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*'"),
        ("load data local infile '/tmp/t.csv' into table t character set utf8 fields terminated by 'ab' enclosed by 'b' escaped by '*'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` CHARACTER SET utf8 FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*'"),
        ("load data local infile '/tmp/t.csv' into table t lines starting by 'ab'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab'"),
        ("load data local infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab' TERMINATED BY 'xy'"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' lines terminated by 'xy'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' LINES TERMINATED BY 'xy'"),
        ("load data local infile '/tmp/t.csv' into table t terminated by 'xy' fields terminated by 'ab'", false, ""),
        ("load data infile '/tmp/t.csv' into table t (a,b)", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t columns terminated by 'ab' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t character set utf8 fields terminated by 'ab' enclosed by 'b' escaped by '*' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` CHARACTER SET utf8 FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t lines starting by 'ab' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab' TERMINATED BY 'xy' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t character set utf8 fields terminated by 'ab' lines terminated by 'xy' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` CHARACTER SET utf8 FIELDS TERMINATED BY 'ab' LINES TERMINATED BY 'xy' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' lines terminated by 'xy' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' LINES TERMINATED BY 'xy' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t (a,b) fields terminated by 'ab'", false, ""),
        ("load data local infile '/tmp/t.csv' into table t ignore 1 lines", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` IGNORE 1 LINES"),
        ("load data local infile '/tmp/t.csv' into table t ignore -1 lines", false, ""),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' (a,b) ignore 1 lines", false, ""),
        ("load data local infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy' ignore 1 lines", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab' TERMINATED BY 'xy' IGNORE 1 LINES"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*' ignore 1 lines (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*' IGNORE 1 LINES (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by ''", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY ''"),
        ("load data local infile '~/1.csv' into table `t_ascii` fields terminated by X'6B6B';", true, "LOAD DATA LOCAL INFILE '~/1.csv' IGNORE INTO TABLE `t_ascii` FIELDS TERMINATED BY 'kk'"),
        ("load data local infile '~/1.csv' into table `t_ascii` fields terminated by X'6B6B' enclosed by X'0D';", true, "LOAD DATA LOCAL INFILE '~/1.csv' IGNORE INTO TABLE `t_ascii` FIELDS TERMINATED BY 'kk' ENCLOSED BY '\r'"),
        ("load data local infile '~/1.csv' into table `t_ascii` fields terminated by X'6B6B' enclosed by X'0D0D';", false, ""),
        ("load data local infile '~/1.csv' into table `t_ascii` fields terminated by B'110101101101011';", true, "LOAD DATA LOCAL INFILE '~/1.csv' IGNORE INTO TABLE `t_ascii` FIELDS TERMINATED BY 'kk'"),
        ("load data local infile '~/1.csv' into table `t_ascii` fields terminated by B'110101101101011' enclosed by B'1101';", true, "LOAD DATA LOCAL INFILE '~/1.csv' IGNORE INTO TABLE `t_ascii` FIELDS TERMINATED BY 'kk' ENCLOSED BY '\r'"),
        ("load data local infile '~/1.csv' into table `t_ascii` fields terminated by B'110101101101011' enclosed by B'110100001101';", false, ""),
        ("load data local infile '~/1.csv' into table `t_ascii` lines starting by B'110101101101011' terminated by B'110101101101011';", true, "LOAD DATA LOCAL INFILE '~/1.csv' IGNORE INTO TABLE `t_ascii` LINES STARTING BY 'kk' TERMINATED BY 'kk'"),
        ("load data local infile '~/1.csv' into table `t_ascii` lines starting by X'6B6B' terminated by X'6B6B';", true, "LOAD DATA LOCAL INFILE '~/1.csv' IGNORE INTO TABLE `t_ascii` LINES STARTING BY 'kk' TERMINATED BY 'kk'"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' enclosed by 'b'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b'"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' escaped by '' enclosed by 'b'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY ''"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' escaped by '' enclosed by 'b' SET b = CAST(CONV(MID(@var1, 3, LENGTH(@var1)-3), 2, 10) AS UNSIGNED)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '' SET `b`=CAST(CONV(MID(@`var1`, 3, LENGTH(@`var1`)-3), 2, 10) AS UNSIGNED)"),
        ("load data infile '/tmp/t.csv' into table t fields enclosed by ''", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS ENCLOSED BY ''"),
        ("load data infile '/tmp/t.csv' into table t fields enclosed by 'a'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS ENCLOSED BY 'a'"),
        ("load data infile '/tmp/t.csv' into table t fields enclosed by 'aa'", false, ""),
        ("load data infile '/tmp/t.csv' into table t fields escaped by ''", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS ESCAPED BY ''"),
        ("load data infile '/tmp/t.csv' into table t fields escaped by 'a'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS ESCAPED BY 'a'"),
        ("load data infile '/tmp/t.csv' into table t fields escaped by 'aa'", false, ""),
        ("LOAD DATA INFILE 'file.txt' INTO TABLE t1 (column1, @dummy, column2, @dummy, column3)", true, "LOAD DATA INFILE 'file.txt' INTO TABLE `t1` (`column1`,@`dummy`,`column2`,@`dummy`,`column3`)"),
        ("LOAD DATA INFILE 'file.txt' INTO TABLE t1 (column1, @var1) SET column2 = @var1/100", true, "LOAD DATA INFILE 'file.txt' INTO TABLE `t1` (`column1`,@`var1`) SET `column2`=@`var1`/100"),
        ("LOAD DATA INFILE 'file.txt' INTO TABLE t1 (column1, @var1, @var2) SET column2 = @var1/100, column3 = DEFAULT, column4=CURRENT_TIMESTAMP, column5=@var2+1", true, "LOAD DATA INFILE 'file.txt' INTO TABLE `t1` (`column1`,@`var1`,@`var2`) SET `column2`=@`var1`/100, `column3`=DEFAULT, `column4`=CURRENT_TIMESTAMP(), `column5`=@`var2`+1"),
        ("LOAD DATA INFILE '/tmp/t.csv' INTO TABLE t1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t1` FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"),
        ("LOAD DATA LOCAL INFILE '/tmp/t.csv' INTO TABLE t1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t1` FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"),
        ("LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE t1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t1` FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"),
        ("LOAD DATA LOCAL INFILE '/tmp/t.csv' REPLACE INTO TABLE t1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' REPLACE INTO TABLE `t1` FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"),
        ("load data infile 's3://bucket-name/t.csv' into table t", true, "LOAD DATA INFILE 's3://bucket-name/t.csv' INTO TABLE `t`"),
        ("load data infile '/tmp/t.csv' into table t fields defined null by 'nil'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS DEFINED NULL BY 'nil'"),
        ("load data infile '/tmp/t.csv' into table t fields defined null by X'00'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS DEFINED NULL BY '\u{0000}'"),
        ("load data infile '/tmp/t.csv' into table t fields defined null by 'NULL' optionally enclosed ignore 1 lines", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS DEFINED NULL BY 'NULL' OPTIONALLY ENCLOSED IGNORE 1 LINES"),
        ("load data infile '/tmp/t.csv' format 'delimited data' into table t (column1, @var1) SET column2 = @var1/100", true, "LOAD DATA INFILE '/tmp/t.csv' FORMAT 'delimited data' INTO TABLE `t` (`column1`,@`var1`) SET `column2`=@`var1`/100"),
        ("load data local infile '/tmp/t.sql' format 'sql file' replace into table t (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.sql' FORMAT 'sql file' REPLACE INTO TABLE `t` (`a`,`b`)"),
        ("load data infile '/tmp/t.parquet' format 'parquet' into table t (column1, @var1) SET column2 = @var1/100", true, "LOAD DATA INFILE '/tmp/t.parquet' FORMAT 'parquet' INTO TABLE `t` (`column1`,@`var1`) SET `column2`=@`var1`/100"),
        ("load data infile '/tmp/t.csv' into table t with detached", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` WITH detached"),
        ("load data infile '/tmp/t.csv' into table `t` with threads=10", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` WITH threads=10"),
        ("load data infile '/tmp/t.csv' into table `t` with threads=10, detached", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` WITH threads=10, detached"),
        ("import into t from '/file.csv'", true, "IMPORT INTO `t` FROM '/file.csv'"),
        ("import into t (a,b) from '/file.csv'", true, "IMPORT INTO `t` (`a`,`b`) FROM '/file.csv'"),
        ("import into t (a,@1) from '/file.csv'", true, "IMPORT INTO `t` (`a`,@`1`) FROM '/file.csv'"),
        ("import into t (a,@1) set b=@1+100 from '/file.csv'", true, "IMPORT INTO `t` (`a`,@`1`) SET `b`=@`1`+100 FROM '/file.csv'"),
        ("import into t from '/file.csv' format 'sql file'", true, "IMPORT INTO `t` FROM '/file.csv' FORMAT 'sql file'"),
        ("import into t from '/file.csv' with detached", true, "IMPORT INTO `t` FROM '/file.csv' WITH detached"),
        ("import into `t` from '/file.csv' with thread=1", true, "IMPORT INTO `t` FROM '/file.csv' WITH thread=1"),
        ("import into `t` from '/file.csv' with detached, thread=1", true, "IMPORT INTO `t` FROM '/file.csv' WITH detached, thread=1"),
        ("select * from t for update", true, "SELECT * FROM `t` FOR UPDATE"),
        ("select * from t for share", true, "SELECT * FROM `t` FOR SHARE"),
        ("select * from t for update nowait", true, "SELECT * FROM `t` FOR UPDATE NOWAIT"),
        ("select * from t for update wait 5", true, "SELECT * FROM `t` FOR UPDATE WAIT 5"),
        ("select * from t limit 1 for update wait 11", true, "SELECT * FROM `t` LIMIT 1 FOR UPDATE WAIT 11"),
        ("select * from t for share nowait", true, "SELECT * FROM `t` FOR SHARE NOWAIT"),
        ("select * from t for update skip locked", true, "SELECT * FROM `t` FOR UPDATE SKIP LOCKED"),
        ("select * from t for share skip locked", true, "SELECT * FROM `t` FOR SHARE SKIP LOCKED"),
        ("select * from t lock in share mode", true, "SELECT * FROM `t` FOR SHARE"),
        ("select * from t lock in share mode nowait", false, ""),
        ("select * from t lock in share mode skip locked", false, ""),
        ("select * from t for update of t", true, "SELECT * FROM `t` FOR UPDATE OF `t`"),
        ("select * from t for share of t", true, "SELECT * FROM `t` FOR SHARE OF `t`"),
        ("select * from t for update of t nowait", true, "SELECT * FROM `t` FOR UPDATE OF `t` NOWAIT"),
        ("select * from t for update of t wait 5", true, "SELECT * FROM `t` FOR UPDATE OF `t` WAIT 5"),
        ("select * from t limit 1 for update of t wait 11", true, "SELECT * FROM `t` LIMIT 1 FOR UPDATE OF `t` WAIT 11"),
        ("select * from t for share of t nowait", true, "SELECT * FROM `t` FOR SHARE OF `t` NOWAIT"),
        ("select * from t for update of t skip locked", true, "SELECT * FROM `t` FOR UPDATE OF `t` SKIP LOCKED"),
        ("select * from t for share of t skip locked", true, "SELECT * FROM `t` FOR SHARE OF `t` SKIP LOCKED"),
        ("select a, b from t into outfile '/tmp/result.txt'", true, "SELECT `a`,`b` FROM `t` INTO OUTFILE '/tmp/result.txt'"),
        ("select a from t order by a into outfile '/tmp/abc'", true, "SELECT `a` FROM `t` ORDER BY `a` INTO OUTFILE '/tmp/abc'"),
        ("select 1 into outfile '/tmp/1.csv'", true, "SELECT 1 INTO OUTFILE '/tmp/1.csv'"),
        ("select 1 for update into outfile '/tmp/1.csv'", true, "SELECT 1 FOR UPDATE INTO OUTFILE '/tmp/1.csv'"),
        ("select a,b,a+b from t into outfile '/tmp/result.txt' fields terminated BY ','", true, "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' FIELDS TERMINATED BY ','"),
        ("select a,b,a+b from t into outfile '/tmp/result.txt' fields terminated BY ',' enclosed BY '\"'", true, "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '\"'"),
        ("select a,b,a+b from t into outfile '/tmp/result.txt' fields terminated BY ',' optionally enclosed BY '\"'", true, "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'"),
        ("select a,b,a+b from t into outfile '/tmp/result.txt' lines terminated BY '\n'", true, "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' LINES TERMINATED BY '\n'"),
        ("select a,b,a+b from t into outfile '/tmp/result.txt' fields terminated BY ',' optionally enclosed BY '\"' lines terminated BY '\r'", true, "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\r'"),
        ("select a,b,a+b from t into outfile '/tmp/result.txt' fields terminated BY ',' enclosed BY '\"' lines terminated BY '\r'", true, "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r'"),
        ("select a,b,a+b from t into outfile '/tmp/result.txt' fields terminated BY ',' optionally enclosed BY '\"' lines starting by 'xy' terminated BY '\r'", true, "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES STARTING BY 'xy' TERMINATED BY '\r'"),
        ("select a,b,a+b from t into outfile '/tmp/result.txt' fields terminated BY ',' enclosed BY '\"' lines starting by 'xy' terminated BY '\r'", true, "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES STARTING BY 'xy' TERMINATED BY '\r'"),
        ("SELECT * from t1, t2, t3", true, "SELECT * FROM ((`t1`) JOIN `t2`) JOIN `t3`"),
        ("select * from t1 join t2 left join t3 on t2.id = t3.id", true, "SELECT * FROM (`t1` JOIN `t2`) LEFT JOIN `t3` ON `t2`.`id`=`t3`.`id`"),
        ("select * from t1 right join t2 on t1.id = t2.id left join t3 on t3.id = t2.id", true, "SELECT * FROM (`t1` RIGHT JOIN `t2` ON `t1`.`id`=`t2`.`id`) LEFT JOIN `t3` ON `t3`.`id`=`t2`.`id`"),
        ("select * from t1 right join t2 on t1.id = t2.id left join t3", false, ""),
        ("select * from t1 join t2 left join t3 using (id)", true, "SELECT * FROM (`t1` JOIN `t2`) LEFT JOIN `t3` USING (`id`)"),
        ("select * from t1 right join t2 using (id) left join t3 using (id)", true, "SELECT * FROM (`t1` RIGHT JOIN `t2` USING (`id`)) LEFT JOIN `t3` USING (`id`)"),
        ("select * from t1 right join t2 using (id) left join t3", false, ""),
        ("select * from t1 natural join t2", true, "SELECT * FROM `t1` NATURAL JOIN `t2`"),
        ("select * from t1 natural right join t2", true, "SELECT * FROM `t1` NATURAL RIGHT JOIN `t2`"),
        ("select * from t1 natural left outer join t2", true, "SELECT * FROM `t1` NATURAL LEFT JOIN `t2`"),
        ("select * from t1 natural inner join t2", false, ""),
        ("select * from t1 natural cross join t2", false, ""),
        ("select * from t3 join t1 join t2 on t1.a=t2.a on t3.b=t2.b", true, "SELECT * FROM `t3` JOIN (`t1` JOIN `t2` ON `t1`.`a`=`t2`.`a`) ON `t3`.`b`=`t2`.`b`"),
        ("select * from t1 straight_join t2 on t1.id = t2.id", true, "SELECT * FROM `t1` STRAIGHT_JOIN `t2` ON `t1`.`id`=`t2`.`id`"),
        ("select straight_join * from t1 join t2 on t1.id = t2.id", true, "SELECT STRAIGHT_JOIN * FROM `t1` JOIN `t2` ON `t1`.`id`=`t2`.`id`"),
        ("select straight_join * from t1 left join t2 on t1.id = t2.id", true, "SELECT STRAIGHT_JOIN * FROM `t1` LEFT JOIN `t2` ON `t1`.`id`=`t2`.`id`"),
        ("select straight_join * from t1 right join t2 on t1.id = t2.id", true, "SELECT STRAIGHT_JOIN * FROM `t1` RIGHT JOIN `t2` ON `t1`.`id`=`t2`.`id`"),
        ("select straight_join * from t1 straight_join t2 on t1.id = t2.id", true, "SELECT STRAIGHT_JOIN * FROM `t1` STRAIGHT_JOIN `t2` ON `t1`.`id`=`t2`.`id`"),
        ("DELETE from t1", true, "DELETE FROM `t1`"),
        ("DELETE from t1.*", false, ""),
        ("DELETE LOW_priORITY from t1", true, "DELETE LOW_PRIORITY FROM `t1`"),
        ("DELETE quick from t1", true, "DELETE QUICK FROM `t1`"),
        ("DELETE ignore from t1", true, "DELETE IGNORE FROM `t1`"),
        ("DELETE low_priority quick ignore from t1", true, "DELETE LOW_PRIORITY QUICK IGNORE FROM `t1`"),
        ("DELETE FROM t1 WHERE t1.a > 0 ORDER BY t1.a", true, "DELETE FROM `t1` WHERE `t1`.`a`>0 ORDER BY `t1`.`a`"),
        ("delete from t1 where a=26", true, "DELETE FROM `t1` WHERE `a`=26"),
        ("DELETE from t1 where a=1 limit 1", true, "DELETE FROM `t1` WHERE `a`=1 LIMIT 1"),
        ("DELETE FROM t1 WHERE t1.a > 0 ORDER BY t1.a LIMIT 1", true, "DELETE FROM `t1` WHERE `t1`.`a`>0 ORDER BY `t1`.`a` LIMIT 1"),
        ("DELETE FROM x.y z WHERE z.a > 0", true, "DELETE FROM `x`.`y` AS `z` WHERE `z`.`a`>0"),
        ("DELETE FROM t1 AS w WHERE a > 0", true, "DELETE FROM `t1` AS `w` WHERE `a`>0"),
        ("DELETE from t1 partition (p0,p1)", true, "DELETE FROM `t1` PARTITION(`p0`, `p1`)"),
        ("delete low_priority t1, t2 from t1, t2", true, "DELETE LOW_PRIORITY `t1`,`t2` FROM (`t1`) JOIN `t2`"),
        ("delete quick t1, t2 from t1, t2", true, "DELETE QUICK `t1`,`t2` FROM (`t1`) JOIN `t2`"),
        ("delete ignore t1, t2 from t1, t2", true, "DELETE IGNORE `t1`,`t2` FROM (`t1`) JOIN `t2`"),
        ("delete ignore t1, t2 from t1 partition (p0,p1), t2", true, "DELETE IGNORE `t1`,`t2` FROM (`t1` PARTITION(`p0`, `p1`)) JOIN `t2`"),
        ("delete low_priority quick ignore t1, t2 from t1, t2 where t1.a > 5", true, "DELETE LOW_PRIORITY QUICK IGNORE `t1`,`t2` FROM (`t1`) JOIN `t2` WHERE `t1`.`a`>5"),
        ("delete t1, t2 from t1, t2", true, "DELETE `t1`,`t2` FROM (`t1`) JOIN `t2`"),
        ("delete t1, t2 from t1, t2 where t1.a = 1 and t2.b <> 1", true, "DELETE `t1`,`t2` FROM (`t1`) JOIN `t2` WHERE `t1`.`a`=1 AND `t2`.`b`!=1"),
        ("delete t1 from t1, t2", true, "DELETE `t1` FROM (`t1`) JOIN `t2`"),
        ("delete t2 from t1, t2", true, "DELETE `t2` FROM (`t1`) JOIN `t2`"),
        ("delete t1 from t1", true, "DELETE `t1` FROM `t1`"),
        ("delete t1,t2,t3 from t1, t2, t3", true, "DELETE `t1`,`t2`,`t3` FROM ((`t1`) JOIN `t2`) JOIN `t3`"),
        ("delete t1,t2,t3 from t1, t2, t3 where t3.c < 5 and t1.a = 3", true, "DELETE `t1`,`t2`,`t3` FROM ((`t1`) JOIN `t2`) JOIN `t3` WHERE `t3`.`c`<5 AND `t1`.`a`=3"),
        ("delete t1 from t1, t1 as t2 where t1.b = t2.b and t1.a > t2.a", true, "DELETE `t1` FROM (`t1`) JOIN `t1` AS `t2` WHERE `t1`.`b`=`t2`.`b` AND `t1`.`a`>`t2`.`a`"),
        ("delete t1.*,t2 from t1, t2", true, "DELETE `t1`,`t2` FROM (`t1`) JOIN `t2`"),
        ("delete t.t1.*,t2 from t1, t2", true, "DELETE `t`.`t1`,`t2` FROM (`t1`) JOIN `t2`"),
        ("delete t1.*, t2.* from t1, t2", true, "DELETE `t1`,`t2` FROM (`t1`) JOIN `t2`"),
        ("delete t11.*, t12.* from t11, t12 where t11.a = t12.a and t11.b <> 1", true, "DELETE `t11`,`t12` FROM (`t11`) JOIN `t12` WHERE `t11`.`a`=`t12`.`a` AND `t11`.`b`!=1"),
        ("DELETE quick FROM t1,t2 USING t1,t2", true, "DELETE QUICK FROM `t1`,`t2` USING (`t1`) JOIN `t2`"),
        ("DELETE low_priority ignore FROM t1,t2 USING t1,t2", true, "DELETE LOW_PRIORITY IGNORE FROM `t1`,`t2` USING (`t1`) JOIN `t2`"),
        ("DELETE low_priority quick ignore FROM t1,t2 USING t1,t2", true, "DELETE LOW_PRIORITY QUICK IGNORE FROM `t1`,`t2` USING (`t1`) JOIN `t2`"),
        ("DELETE FROM t1 USING t1 WHERE post='1'", true, "DELETE FROM `t1` USING `t1` WHERE `post`=_UTF8MB4'1'"),
        ("DELETE FROM t1,t2 USING t1,t2", true, "DELETE FROM `t1`,`t2` USING (`t1`) JOIN `t2`"),
        ("DELETE FROM t1,t2,t3 USING t1,t2,t3 where t3.a = 1", true, "DELETE FROM `t1`,`t2`,`t3` USING ((`t1`) JOIN `t2`) JOIN `t3` WHERE `t3`.`a`=1"),
        ("DELETE FROM t2,t3 USING t1,t2,t3 where t1.a = 1", true, "DELETE FROM `t2`,`t3` USING ((`t1`) JOIN `t2`) JOIN `t3` WHERE `t1`.`a`=1"),
        ("DELETE FROM t2.*,t3.* USING t1,t2,t3 where t1.a = 1", true, "DELETE FROM `t2`,`t3` USING ((`t1`) JOIN `t2`) JOIN `t3` WHERE `t1`.`a`=1"),
        ("DELETE FROM t1,t2.*,t3.* USING t1,t2,t3 where t1.a = 1", true, "DELETE FROM `t1`,`t2`,`t3` USING ((`t1`) JOIN `t2`) JOIN `t3` WHERE `t1`.`a`=1"),
        ("DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id;", true, "DELETE `t1`,`t2` FROM (`t1` JOIN `t2`) JOIN `t3` WHERE `t1`.`id`=`t2`.`id` AND `t2`.`id`=`t3`.`id`"),
        ("DELETE FROM t1, t2 USING t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id;", true, "DELETE FROM `t1`,`t2` USING (`t1` JOIN `t2`) JOIN `t3` WHERE `t1`.`id`=`t2`.`id` AND `t2`.`id`=`t3`.`id`"),
        ("DELETE /*+ TiDB_INLJ(t1, t2) */ t1, t2 from t1, t2 where t1.id=t2.id;", true, "DELETE /*+ TIDB_INLJ(`t1`, `t2`)*/ `t1`,`t2` FROM (`t1`) JOIN `t2` WHERE `t1`.`id`=`t2`.`id`"),
        ("DELETE /*+ TiDB_HJ(t1, t2) */ t1, t2 from t1, t2 where t1.id=t2.id", true, "DELETE /*+ TIDB_HJ(`t1`, `t2`)*/ `t1`,`t2` FROM (`t1`) JOIN `t2` WHERE `t1`.`id`=`t2`.`id`"),
        ("DELETE /*+ TiDB_SMJ(t1, t2) */ t1, t2 from t1, t2 where t1.id=t2.id", true, "DELETE /*+ TIDB_SMJ(`t1`, `t2`)*/ `t1`,`t2` FROM (`t1`) JOIN `t2` WHERE `t1`.`id`=`t2`.`id`"),
        ("DELETE FROM t1 USE INDEX(idx_a) WHERE t1.id=1;", true, "DELETE FROM `t1` USE INDEX (`idx_a`) WHERE `t1`.`id`=1"),
        ("DELETE t1, t2 FROM t1 USE INDEX(idx_a) JOIN t2 WHERE t1.id=t2.id;", true, "DELETE `t1`,`t2` FROM `t1` USE INDEX (`idx_a`) JOIN `t2` WHERE `t1`.`id`=`t2`.`id`"),
        ("DELETE t1, t2 FROM t1 USE INDEX(idx_a) JOIN t2 USE INDEX(idx_a) WHERE t1.id=t2.id;", true, "DELETE `t1`,`t2` FROM `t1` USE INDEX (`idx_a`) JOIN `t2` USE INDEX (`idx_a`) WHERE `t1`.`id`=`t2`.`id`"),
        ("DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id limit 10;", false, ""),
        ("DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id order by t1.id;", false, ""),
        ("INSERT INTO t (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE c=VALUES(a)+VALUES(b);", true, "INSERT INTO `t` (`a`,`b`,`c`) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE `c`=VALUES(`a`)+VALUES(`b`)"),
        ("INSERT INTO t (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE c:=VALUES(a)+VALUES(b);", true, "INSERT INTO `t` (`a`,`b`,`c`) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE `c`=VALUES(`a`)+VALUES(`b`)"),
        ("INSERT IGNORE INTO t (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE c=VALUES(a)+VALUES(b);", true, "INSERT IGNORE INTO `t` (`a`,`b`,`c`) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE `c`=VALUES(`a`)+VALUES(`b`)"),
        ("INSERT IGNORE INTO t (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE c:=VALUES(a)+VALUES(b);", true, "INSERT IGNORE INTO `t` (`a`,`b`,`c`) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE `c`=VALUES(`a`)+VALUES(`b`)"),
        ("INSERT INTO t (a) VALUES (1) RETURNING *", true, "INSERT INTO `t` (`a`) VALUES (1) RETURNING *"),
        ("INSERT INTO t (a) VALUES (1) RETURNING id", true, "INSERT INTO `t` (`a`) VALUES (1) RETURNING `id`"),
        ("INSERT INTO t (a) VALUES (1) RETURNING id, name", true, "INSERT INTO `t` (`a`) VALUES (1) RETURNING `id`, `name`"),
        ("INSERT INTO t2(id,animal) VALUES (1,'Dog'),(2,'Lion'),(3,'Tiger'),(4,'Leopard') RETURNING id,id+id,id&id,id||id", true, "INSERT INTO `t2` (`id`,`animal`) VALUES (1,_UTF8MB4'Dog'),(2,_UTF8MB4'Lion'),(3,_UTF8MB4'Tiger'),(4,_UTF8MB4'Leopard') RETURNING `id`, `id`+`id`, `id`&`id`, `id` OR `id`"),
        ("INSERT INTO t (a) VALUES (1) ON DUPLICATE KEY UPDATE a=2 RETURNING id", true, "INSERT INTO `t` (`a`) VALUES (1) ON DUPLICATE KEY UPDATE `a`=2 RETURNING `id`"),
        ("UPDATE t SET a=1 RETURNING *", true, "UPDATE `t` SET `a`=1 RETURNING *"),
        ("UPDATE t SET a=1 WHERE id=1 RETURNING id, a", true, "UPDATE `t` SET `a`=1 WHERE `id`=1 RETURNING `id`, `a`"),
        ("UPDATE t SET a=1 LIMIT 1 RETURNING *", true, "UPDATE `t` SET `a`=1 LIMIT 1 RETURNING *"),
        ("DELETE FROM t RETURNING *", true, "DELETE FROM `t` RETURNING *"),
        ("DELETE FROM t WHERE id=1 RETURNING id", true, "DELETE FROM `t` WHERE `id`=1 RETURNING `id`"),
        ("DELETE FROM t ORDER BY id LIMIT 1 RETURNING *", true, "DELETE FROM `t` ORDER BY `id` LIMIT 1 RETURNING *"),
        ("INSERT INTO t (a,b,c) VALUES (1,2,3) AS new ON DUPLICATE KEY UPDATE c=new.a+new.b;", true, "INSERT INTO `t` (`a`,`b`,`c`) VALUES (1,2,3) AS `new` ON DUPLICATE KEY UPDATE `c`=`new`.`a`+`new`.`b`"),
        ("INSERT INTO t (a,b,c) VALUES (1,2,3),(4,5,6) AS new(m,n,p) ON DUPLICATE KEY UPDATE c=m+n;", true, "INSERT INTO `t` (`a`,`b`,`c`) VALUES (1,2,3),(4,5,6) AS `new`(`m`, `n`, `p`) ON DUPLICATE KEY UPDATE `c`=`m`+`n`"),
        ("INSERT INTO t VALUES (1,2) AS new ON DUPLICATE KEY UPDATE b=new.b;", true, "INSERT INTO `t` VALUES (1,2) AS `new` ON DUPLICATE KEY UPDATE `b`=`new`.`b`"),
        ("INSERT INTO t SET a=1,b=2 AS new ON DUPLICATE KEY UPDATE b=new.a+new.b;", true, "INSERT INTO `t` SET `a`=1,`b`=2 AS `new` ON DUPLICATE KEY UPDATE `b`=`new`.`a`+`new`.`b`"),
        ("INSERT INTO t SET a=1,b=2 AS new(m,n) ON DUPLICATE KEY UPDATE b=m+n;", true, "INSERT INTO `t` SET `a`=1,`b`=2 AS `new`(`m`, `n`) ON DUPLICATE KEY UPDATE `b`=`m`+`n`"),
        ("INSERT INTO t VALUES (1,2) AS new;", true, "INSERT INTO `t` VALUES (1,2) AS `new`"),
        ("INSERT INTO t VALUES (1,2) AS new(a,b);", true, "INSERT INTO `t` VALUES (1,2) AS `new`(`a`, `b`)"),
        ("REPLACE INTO t VALUES (1,2) AS new;", false, ""),
        ("REPLACE INTO t SET a=1,b=2 AS new;", false, ""),
        ("INSERT INTO t SET a=1,b=2", true, "INSERT INTO `t` SET `a`=1,`b`=2"),
        ("INSERT INTO t (a) SET a=1", false, ""),
        ("UPDATE LOW_PRIORITY IGNORE t SET id = id + 1 ORDER BY id DESC;", true, "UPDATE LOW_PRIORITY IGNORE `t` SET `id`=`id`+1 ORDER BY `id` DESC"),
        ("UPDATE t SET id = id + 1 ORDER BY id DESC;", true, "UPDATE `t` SET `id`=`id`+1 ORDER BY `id` DESC"),
        ("UPDATE t SET id = id + 1 ORDER BY id DESC limit 3 ;", true, "UPDATE `t` SET `id`=`id`+1 ORDER BY `id` DESC LIMIT 3"),
        ("UPDATE t SET id = id + 1, name = 'jojo';", true, "UPDATE `t` SET `id`=`id`+1, `name`=_UTF8MB4'jojo'"),
        ("UPDATE items,month SET items.price=month.price WHERE items.id=month.id;", true, "UPDATE (`items`) JOIN `month` SET `items`.`price`=`month`.`price` WHERE `items`.`id`=`month`.`id`"),
        ("UPDATE user T0 LEFT OUTER JOIN user_profile T1 ON T1.id = T0.profile_id SET T0.profile_id = 1 WHERE T0.profile_id IN (1);", true, "UPDATE `user` AS `T0` LEFT JOIN `user_profile` AS `T1` ON `T1`.`id`=`T0`.`profile_id` SET `T0`.`profile_id`=1 WHERE `T0`.`profile_id` IN (1)"),
        ("UPDATE t1, t2 set t1.profile_id = 1, t2.profile_id = 1 where ta.a=t.ba", true, "UPDATE (`t1`) JOIN `t2` SET `t1`.`profile_id`=1, `t2`.`profile_id`=1 WHERE `ta`.`a`=`t`.`ba`"),
        ("UPDATE /*+ TiDB_INLJ(t1, t2) */ t1, t2 set t1.profile_id = 1, t2.profile_id = 1 where ta.a=t.ba", true, "UPDATE /*+ TIDB_INLJ(`t1`, `t2`)*/ (`t1`) JOIN `t2` SET `t1`.`profile_id`=1, `t2`.`profile_id`=1 WHERE `ta`.`a`=`t`.`ba`"),
        ("UPDATE /*+ TiDB_SMJ(t1, t2) */ t1, t2 set t1.profile_id = 1, t2.profile_id = 1 where ta.a=t.ba", true, "UPDATE /*+ TIDB_SMJ(`t1`, `t2`)*/ (`t1`) JOIN `t2` SET `t1`.`profile_id`=1, `t2`.`profile_id`=1 WHERE `ta`.`a`=`t`.`ba`"),
        ("UPDATE /*+ TiDB_HJ(t1, t2) */ t1, t2 set t1.profile_id = 1, t2.profile_id = 1 where ta.a=t.ba", true, "UPDATE /*+ TIDB_HJ(`t1`, `t2`)*/ (`t1`) JOIN `t2` SET `t1`.`profile_id`=1, `t2`.`profile_id`=1 WHERE `ta`.`a`=`t`.`ba`"),
        ("UPDATE items,month SET items.price=month.price WHERE items.id=month.id LIMIT 10;", false, ""),
        ("UPDATE items,month SET items.price=month.price WHERE items.id=month.id order by month.id;", false, ""),
        ("UPDATE t1 USE INDEX(idx_a) SET t1.price=3.25 WHERE t1.id=1;", true, "UPDATE `t1` USE INDEX (`idx_a`) SET `t1`.`price`=3.25 WHERE `t1`.`id`=1"),
        ("UPDATE t1 USE INDEX(idx_a) JOIN t2 SET t1.price=t2.price WHERE t1.id=t2.id;", true, "UPDATE `t1` USE INDEX (`idx_a`) JOIN `t2` SET `t1`.`price`=`t2`.`price` WHERE `t1`.`id`=`t2`.`id`"),
        ("UPDATE t1 USE INDEX(idx_a) JOIN t2 USE INDEX(idx_a) SET t1.price=t2.price WHERE t1.id=t2.id;", true, "UPDATE `t1` USE INDEX (`idx_a`) JOIN `t2` USE INDEX (`idx_a`) SET `t1`.`price`=`t2`.`price` WHERE `t1`.`id`=`t2`.`id`"),
        ("SELECT * FROM t WHERE 1 = 1", true, "SELECT * FROM `t` WHERE 1=1"),
        ("SELECT * FROM t FETCH FIRST 5 ROW ONLY", true, "SELECT * FROM `t` LIMIT 5"),
        ("SELECT * FROM t FETCH NEXT 5 ROW ONLY", true, "SELECT * FROM `t` LIMIT 5"),
        ("SELECT * FROM t FETCH FIRST 5 ROWS ONLY", true, "SELECT * FROM `t` LIMIT 5"),
        ("SELECT * FROM t FETCH NEXT 5 ROWS ONLY", true, "SELECT * FROM `t` LIMIT 5"),
        ("SELECT * FROM t FETCH FIRST ROW ONLY", true, "SELECT * FROM `t` LIMIT 1"),
        ("SELECT * FROM t FETCH NEXT ROW ONLY", true, "SELECT * FROM `t` LIMIT 1"),
        ("select 1 from dual", true, "SELECT 1"),
        ("select 1 from dual limit 1", true, "SELECT 1 LIMIT 1"),
        ("select 1 where exists (select 2)", true, "SELECT 1 FROM DUAL WHERE EXISTS (SELECT 2)"),
        ("select 1 from dual where not exists (select 2)", true, "SELECT 1 FROM DUAL WHERE NOT EXISTS (SELECT 2)"),
        ("select 1 as a from dual order by a", true, "SELECT 1 AS `a` ORDER BY `a`"),
        ("select 1 as a from dual where 1 < any (select 2) order by a", true, "SELECT 1 AS `a` FROM DUAL WHERE 1<ANY (SELECT 2) ORDER BY `a`"),
        ("select 1 order by 1", true, "SELECT 1 ORDER BY 1"),
        ("(select 1);", true, "(SELECT 1)"),
        ("select 1 where 1=1", true, "SELECT 1 FROM DUAL WHERE 1=1"),
        ("select 1 group by 1", true, "SELECT 1 GROUP BY 1"),
        ("select 1 from dual group by 1", true, "SELECT 1 GROUP BY 1"),
        ("select min(b) b from (select min(t.b) b from t where t.a = '');", true, "SELECT MIN(`b`) AS `b` FROM (SELECT MIN(`t`.`b`) AS `b` FROM `t` WHERE `t`.`a`=_UTF8MB4'')"),
        ("select min(b) b from (select min(t.b) b from t where t.a = '') as t1;", true, "SELECT MIN(`b`) AS `b` FROM (SELECT MIN(`t`.`b`) AS `b` FROM `t` WHERE `t`.`a`=_UTF8MB4'') AS `t1`"),
        ("SELECT /*!40001 SQL_NO_CACHE */ * FROM test WHERE 1 limit 0, 2000;", true, "SELECT SQL_NO_CACHE * FROM `test` WHERE 1 LIMIT 0,2000"),
        ("ANALYZE TABLE t", true, "ANALYZE TABLE `t`"),
        ("/** 20180417 **/ show databases;", true, "SHOW DATABASES"),
        ("/* 20180417 **/ show databases;", true, "SHOW DATABASES"),
        ("/** 20180417 */ show databases;", true, "SHOW DATABASES"),
        ("/** 20180417 ******/ show databases;", true, "SHOW DATABASES"),
        ("/**/show databases;", true, "SHOW DATABASES"),
        ("/*+*/show databases;", true, "SHOW DATABASES"),
        ("select/*+*/1;", true, "SELECT 1"),
        ("/*T*/show databases;", true, "SHOW DATABASES"),
        ("/*M*/show databases;", true, "SHOW DATABASES"),
        ("/*!*/show databases;", true, "SHOW DATABASES"),
        ("/*T!*/show databases;", true, "SHOW DATABASES"),
        ("/*M!*/show databases;", true, "SHOW DATABASES"),
        ("BINLOG '\nBxSFVw8JAAAA8QAAAPUAAAAAAAQANS41LjQ0LU1hcmlhREItbG9nAAAAAAAAAAAAAAAAAAAAAAAA\nAAAAAAAAAAAAAAAAAAAAAAAAEzgNAAgAEgAEBAQEEgAA2QAEGggAAAAICAgCAAAAAAAAAAAAAAAA\nAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\nAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\nAAAAAAAAAAAA5gm5Mg==\n'/*!*/;", true, "BINLOG '\nBxSFVw8JAAAA8QAAAPUAAAAAAAQANS41LjQ0LU1hcmlhREItbG9nAAAAAAAAAAAAAAAAAAAAAAAA\nAAAAAAAAAAAAAAAAAAAAAAAAEzgNAAgAEgAEBAQEEgAA2QAEGggAAAAICAgCAAAAAAAAAAAAAAAA\nAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\nAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\nAAAAAAAAAAAA5gm5Mg==\n'"),
        ("select * from t1 partition (p1)", true, "SELECT * FROM `t1` PARTITION(`p1`)"),
        ("select * from t1 partition (p1,p2)", true, "SELECT * FROM `t1` PARTITION(`p1`, `p2`)"),
        ("select * from t1 partition (`p1`, p2, p3)", true, "SELECT * FROM `t1` PARTITION(`p1`, `p2`, `p3`)"),
        ("select * from t1 partition ()", false, ""),
        ("split table t1 index idx1 by ('a'),('b'),('c')", true, "SPLIT TABLE `t1` INDEX `idx1` BY (_UTF8MB4'a'),(_UTF8MB4'b'),(_UTF8MB4'c')"),
        ("split table t1 index idx1 by (1)", true, "SPLIT TABLE `t1` INDEX `idx1` BY (1)"),
        ("split table t1 index idx1 by ('abc',123), ('xyz'), ('yz', 1000)", true, "SPLIT TABLE `t1` INDEX `idx1` BY (_UTF8MB4'abc',123),(_UTF8MB4'xyz'),(_UTF8MB4'yz',1000)"),
        ("split table t1 index idx1 by ", false, ""),
        ("split table t1 index idx1 between ('a') and ('z') regions 10", true, "SPLIT TABLE `t1` INDEX `idx1` BETWEEN (_UTF8MB4'a') AND (_UTF8MB4'z') REGIONS 10"),
        ("split table t1 index idx1 between ('a',1) and ('z',2) regions 10", true, "SPLIT TABLE `t1` INDEX `idx1` BETWEEN (_UTF8MB4'a',1) AND (_UTF8MB4'z',2) REGIONS 10"),
        ("split table t1 index idx1 between () and () regions 10", true, "SPLIT TABLE `t1` INDEX `idx1` BETWEEN () AND () REGIONS 10"),
        ("split table t1 index by (1)", false, ""),
        ("split region for table t1 index idx1 by ('a'),('b'),('c')", true, "SPLIT REGION FOR TABLE `t1` INDEX `idx1` BY (_UTF8MB4'a'),(_UTF8MB4'b'),(_UTF8MB4'c')"),
        ("split partition table t1 index idx1 by ('a'),('b'),('c')", true, "SPLIT PARTITION TABLE `t1` INDEX `idx1` BY (_UTF8MB4'a'),(_UTF8MB4'b'),(_UTF8MB4'c')"),
        ("split region for partition table t1 index idx1 by ('a'),('b'),('c')", true, "SPLIT REGION FOR PARTITION TABLE `t1` INDEX `idx1` BY (_UTF8MB4'a'),(_UTF8MB4'b'),(_UTF8MB4'c')"),
        ("split region for table t1 index idx1 between ('a') and ('z') regions 10", true, "SPLIT REGION FOR TABLE `t1` INDEX `idx1` BETWEEN (_UTF8MB4'a') AND (_UTF8MB4'z') REGIONS 10"),
        ("split partition table t1 index idx1 between ('a') and ('z') regions 10", true, "SPLIT PARTITION TABLE `t1` INDEX `idx1` BETWEEN (_UTF8MB4'a') AND (_UTF8MB4'z') REGIONS 10"),
        ("split region for partition table t1 index idx1 between ('a') and ('z') regions 10", true, "SPLIT REGION FOR PARTITION TABLE `t1` INDEX `idx1` BETWEEN (_UTF8MB4'a') AND (_UTF8MB4'z') REGIONS 10"),
        ("split region for table t1 partition (p0,p1) index idx1 by ('a'),('b'),('c')", true, "SPLIT REGION FOR TABLE `t1` PARTITION(`p0`, `p1`) INDEX `idx1` BY (_UTF8MB4'a'),(_UTF8MB4'b'),(_UTF8MB4'c')"),
        ("split partition table t1 partition (p0) index idx1 by ('a'),('b'),('c')", true, "SPLIT PARTITION TABLE `t1` PARTITION(`p0`) INDEX `idx1` BY (_UTF8MB4'a'),(_UTF8MB4'b'),(_UTF8MB4'c')"),
        ("split region for partition table t1 partition (p0) index idx1 by ('a'),('b'),('c')", true, "SPLIT REGION FOR PARTITION TABLE `t1` PARTITION(`p0`) INDEX `idx1` BY (_UTF8MB4'a'),(_UTF8MB4'b'),(_UTF8MB4'c')"),
        ("split region for table t1 partition (p0) index idx1 between ('a') and ('z') regions 10", true, "SPLIT REGION FOR TABLE `t1` PARTITION(`p0`) INDEX `idx1` BETWEEN (_UTF8MB4'a') AND (_UTF8MB4'z') REGIONS 10"),
        ("split partition table t1 partition (p0) index idx1 between ('a') and ('z') regions 10", true, "SPLIT PARTITION TABLE `t1` PARTITION(`p0`) INDEX `idx1` BETWEEN (_UTF8MB4'a') AND (_UTF8MB4'z') REGIONS 10"),
        ("split region for partition table t1 partition (p0) index idx1 between ('a') and ('z') regions 10", true, "SPLIT REGION FOR PARTITION TABLE `t1` PARTITION(`p0`) INDEX `idx1` BETWEEN (_UTF8MB4'a') AND (_UTF8MB4'z') REGIONS 10"),
        ("split table t1 by ('a'),('b'),('c')", true, "SPLIT TABLE `t1` BY (_UTF8MB4'a'),(_UTF8MB4'b'),(_UTF8MB4'c')"),
        ("split table t1 by (1)", true, "SPLIT TABLE `t1` BY (1)"),
        ("split table t1 by ('abc',123), ('xyz'), ('yz', 1000)", true, "SPLIT TABLE `t1` BY (_UTF8MB4'abc',123),(_UTF8MB4'xyz'),(_UTF8MB4'yz',1000)"),
        ("split table t1 by ", false, ""),
        ("split table t1 between ('a') and ('z') regions 10", true, "SPLIT TABLE `t1` BETWEEN (_UTF8MB4'a') AND (_UTF8MB4'z') REGIONS 10"),
        ("split table t1 between ('a',1) and ('z',2) regions 10", true, "SPLIT TABLE `t1` BETWEEN (_UTF8MB4'a',1) AND (_UTF8MB4'z',2) REGIONS 10"),
        ("split table t1 between () and () regions 10", true, "SPLIT TABLE `t1` BETWEEN () AND () REGIONS 10"),
        ("split region for table t1 by ('a'),('b'),('c')", true, "SPLIT REGION FOR TABLE `t1` BY (_UTF8MB4'a'),(_UTF8MB4'b'),(_UTF8MB4'c')"),
        ("split partition table t1 by ('a'),('b'),('c')", true, "SPLIT PARTITION TABLE `t1` BY (_UTF8MB4'a'),(_UTF8MB4'b'),(_UTF8MB4'c')"),
        ("split region for partition table t1 by ('a'),('b'),('c')", true, "SPLIT REGION FOR PARTITION TABLE `t1` BY (_UTF8MB4'a'),(_UTF8MB4'b'),(_UTF8MB4'c')"),
        ("split region for table t1 between (1) and (1000) regions 10", true, "SPLIT REGION FOR TABLE `t1` BETWEEN (1) AND (1000) REGIONS 10"),
        ("split partition table t1 between (1) and (1000) regions 10", true, "SPLIT PARTITION TABLE `t1` BETWEEN (1) AND (1000) REGIONS 10"),
        ("split region for partition table t1 between (1) and (1000) regions 10", true, "SPLIT REGION FOR PARTITION TABLE `t1` BETWEEN (1) AND (1000) REGIONS 10"),
        ("show table t1 regions", true, "SHOW TABLE `t1` REGIONS"),
        ("show table t1 regions where a=1", true, "SHOW TABLE `t1` REGIONS WHERE `a`=1"),
        ("show table t1", false, ""),
        ("show table t1 index idx1 regions", true, "SHOW TABLE `t1` INDEX `idx1` REGIONS"),
        ("show table t1 index idx1 regions where a=2", true, "SHOW TABLE `t1` INDEX `idx1` REGIONS WHERE `a`=2"),
        ("show table t1 index idx1", false, ""),
        ("show table t1 partition (p0,p1) regions", true, "SHOW TABLE `t1` PARTITION(`p0`, `p1`) REGIONS"),
        ("show table t1 partition (p0) regions where a=1", true, "SHOW TABLE `t1` PARTITION(`p0`) REGIONS WHERE `a`=1"),
        ("show table t1 partition", false, ""),
        ("show table t1 partition (p0) index idx1 regions", true, "SHOW TABLE `t1` PARTITION(`p0`) INDEX `idx1` REGIONS"),
        ("show table t1 partition (p0,p1) index idx1 regions where a=2", true, "SHOW TABLE `t1` PARTITION(`p0`, `p1`) INDEX `idx1` REGIONS WHERE `a`=2"),
        ("show table t1 partition index idx1", false, ""),
        ("show table t1 distributions", true, "SHOW TABLE `t1` DISTRIBUTIONS"),
        ("show table t1 distributions where a=1", true, "SHOW TABLE `t1` DISTRIBUTIONS WHERE `a`=1"),
        ("show table t1 partition (p0,p1) distributions", true, "SHOW TABLE `t1` PARTITION(`p0`, `p1`) DISTRIBUTIONS"),
        ("show table t1 partition (p0,p1) distributions where a=1", true, "SHOW TABLE `t1` PARTITION(`p0`, `p1`) DISTRIBUTIONS WHERE `a`=1"),
        ("distribute table t1", false, ""),
        ("distribute table t1 partition(p0)", false, ""),
        ("distribute table t1 partition(p0,p1)", false, ""),
        ("distribute table t1 partition(p0,p1) engine = tikv", false, ""),
        ("distribute table t1 rule = 'leader-scatter' engine = 'tikv'", true, "DISTRIBUTE TABLE `t1` RULE = 'leader-scatter' ENGINE = 'tikv'"),
        ("distribute table t1 rule = \"leader-scatter\" engine = \"tikv\"", true, "DISTRIBUTE TABLE `t1` RULE = 'leader-scatter' ENGINE = 'tikv'"),
        ("distribute table t1 partition(p0,p1) rule = 'learner-scatter' engine = 'tikv'", true, "DISTRIBUTE TABLE `t1` PARTITION(`p0`, `p1`) RULE = 'learner-scatter' ENGINE = 'tikv'"),
        ("distribute table t1 partition(p0) rule = 'peer-scatter' engine = 'tiflash'", true, "DISTRIBUTE TABLE `t1` PARTITION(`p0`) RULE = 'peer-scatter' ENGINE = 'tiflash'"),
        ("distribute table t1 partition(p0) rule = 'peer-scatter' engine = 'tiflash' timeout = '30m'", true, "DISTRIBUTE TABLE `t1` PARTITION(`p0`) RULE = 'peer-scatter' ENGINE = 'tiflash' TIMEOUT = '30m'"),
        ("show distribution jobs 1", false, ""),
        ("show distribution jobs", true, "SHOW DISTRIBUTION JOBS"),
        ("show distribution jobs where id > 0", true, "SHOW DISTRIBUTION JOBS WHERE `id`>0"),
        ("show distribution job 1 where id > 0", false, ""),
        ("show distribution job 1", true, "SHOW DISTRIBUTION JOB 1"),
        ("cancel distribution job", false, ""),
        ("cancel distribution job 1", true, "CANCEL DISTRIBUTION JOB 1"),
        ("show table t1.t1 next_row_id", true, "SHOW TABLE `t1`.`t1` NEXT_ROW_ID"),
        ("show table t1 next_row_id", true, "SHOW TABLE `t1` NEXT_ROW_ID"),
        ("show table next_row_id", false, ""),
        ("begin pessimistic", true, "BEGIN PESSIMISTIC"),
        ("begin optimistic", true, "BEGIN OPTIMISTIC"),
        ("ADMIN REPAIR TABLE t CREATE TABLE t (a int)", true, "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`a` INT)"),
        ("ADMIN REPAIR TABLE t CREATE TABLE t (a char(1))", true, "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`a` CHAR(1))"),
        ("ADMIN REPAIR TABLE t CREATE TABLE t (a char(1), b int)", true, "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`a` CHAR(1),`b` INT)"),
        ("ADMIN REPAIR TABLE t CREATE TABLE t (c1 TIME(2), c2 DATETIME(2), c3 TIMESTAMP(2));", true, "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`c1` TIME(2),`c2` DATETIME(2),`c3` TIMESTAMP(2))"),
        ("ADMIN REPAIR TABLE t CREATE TABLE t (a TINYINT UNSIGNED);", true, "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`a` TINYINT UNSIGNED)"),
        ("ADMIN REPAIR TABLE t CREATE TABLE t (name CHAR(50) CHARACTER SET UTF8)", true, "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`name` CHAR(50) CHARACTER SET UTF8)"),
        ("ALTER INSTANCE RELOAD TLS", true, "ALTER INSTANCE RELOAD TLS"),
        ("ALTER INSTANCE RELOAD TLS NO ROLLBACK ON ERROR", true, "ALTER INSTANCE RELOAD TLS NO ROLLBACK ON ERROR"),
        ("ALTER RANGE global PLACEMENT POLICY mypolicy", true, "ALTER RANGE `global` PLACEMENT POLICY = `mypolicy`"),
        ("ALTER RANGE global PLACEMENT POLICY default", true, "ALTER RANGE `global` PLACEMENT POLICY = `default`"),
        ("ALTER RANGE meta PLACEMENT POLICY mypolicy", true, "ALTER RANGE `meta` PLACEMENT POLICY = `mypolicy`"),
        ("CREATE SEQUENCE seq INCREMENT - 9223372036854775807", true, "CREATE SEQUENCE `seq` INCREMENT BY -9223372036854775807"),
        ("CREATE SEQUENCE seq INCREMENT - 9223372036854775808", true, "CREATE SEQUENCE `seq` INCREMENT BY -9223372036854775808"),
        ("CREATE SEQUENCE seq INCREMENT -9223372036854775808", true, "CREATE SEQUENCE `seq` INCREMENT BY -9223372036854775808"),
        ("CREATE SEQUENCE seq INCREMENT -9223372036854775809", false, ""),
        ("select `t`.`1a`.1 from t;", true, "SELECT `t`.`1a`.`1` FROM `t`"),
        ("select * from 1db.1table;", true, "SELECT * FROM `1db`.`1table`"),
        ("select * from t where t. status = 1;", true, "SELECT * FROM `t` WHERE `t`.`status`=1"),
        ("SHOW PLACEMENT", true, "SHOW PLACEMENT"),
        ("SHOW PLACEMENT LIKE 'POLICY foo%'", true, "SHOW PLACEMENT LIKE _UTF8MB4'POLICY foo%'"),
        ("SHOW PLACEMENT WHERE Target='TABLE test.t1'", true, "SHOW PLACEMENT WHERE `Target`=_UTF8MB4'TABLE test.t1'"),
        ("SHOW PLACEMENT FOR DATABASE db1", true, "SHOW PLACEMENT FOR DATABASE `db1`"),
        ("SHOW PLACEMENT FOR SCHEMA db1", true, "SHOW PLACEMENT FOR DATABASE `db1`"),
        ("SHOW PLACEMENT FOR TABLE tb1", true, "SHOW PLACEMENT FOR TABLE `tb1`"),
        ("SHOW PLACEMENT FOR TABLE db1.tb1", true, "SHOW PLACEMENT FOR TABLE `db1`.`tb1`"),
        ("SHOW PLACEMENT FOR TABLE tb1 PARTITION p1", true, "SHOW PLACEMENT FOR TABLE `tb1` PARTITION `p1`"),
        ("SHOW PLACEMENT FOR TABLE db1.tb1 PARTITION p1", true, "SHOW PLACEMENT FOR TABLE `db1`.`tb1` PARTITION `p1`"),
        ("SHOW PLACEMENT FOR", false, ""),
        ("SHOW PLACEMENT DATABASE db1", false, ""),
        ("SHOW PLACEMENT FOR DB db1", false, ""),
        ("SHOW PLACEMENT FOR DATABASE db1 TABLE tb1", false, ""),
        ("SHOW PLACEMENT FOR PARTITION p1", false, ""),
        ("SHOW PLACEMENT FOR DB LIKE '%'", false, ""),
        ("SHOW PLACEMENT FOR DB db1 LIKE '%'", false, ""),
        ("SHOW PLACEMENT LABELS", true, "SHOW PLACEMENT LABELS"),
        ("SHOW PLACEMENT LABELS LIKE '%zone%'", true, "SHOW PLACEMENT LABELS LIKE _UTF8MB4'%zone%'"),
        ("SHOW PLACEMENT LABELS WHERE label='l123'", true, "SHOW PLACEMENT LABELS WHERE `label`=_UTF8MB4'l123'"),
        ("SHOW SESSION_STATES", true, "SHOW SESSION_STATES"),
        ("SET SESSION_STATES 'x'", true, "SET SESSION_STATES 'x'"),
        ("SET SESSION_STATES", false, ""),
        ("SET SESSION_STATES 1", false, ""),
        ("SET SESSION_STATES now()", false, ""),
        ("calibrate resource", true, "CALIBRATE RESOURCE"),
        ("calibrate resource START_TIME '2021-04-15 00:00:00'", true, "CALIBRATE RESOURCE START_TIME _UTF8MB4'2021-04-15 00:00:00'"),
        ("calibrate resource START_TIME '2023-04-01 13:00:00' END_TIME '2023-04-01 16:00:00'", true, "CALIBRATE RESOURCE START_TIME _UTF8MB4'2023-04-01 13:00:00' END_TIME _UTF8MB4'2023-04-01 16:00:00'"),
        ("calibrate resource START_TIME '2023-04-01 13:00:00' DURATION '20m'", true, "CALIBRATE RESOURCE START_TIME _UTF8MB4'2023-04-01 13:00:00' DURATION '20m'"),
        ("calibrate resource START_TIME '2023-04-01 13:00:00' END_TIME '2023-04-01 16:00:00' DURATION '20m'", true, "CALIBRATE RESOURCE START_TIME _UTF8MB4'2023-04-01 13:00:00' END_TIME _UTF8MB4'2023-04-01 16:00:00' DURATION '20m'"),
        ("calibrate resource START_TIME '2023-04-01 13:00:00',END_TIME='2023-04-01 16:00:00'", true, "CALIBRATE RESOURCE START_TIME _UTF8MB4'2023-04-01 13:00:00' END_TIME _UTF8MB4'2023-04-01 16:00:00'"),
        ("calibrate resource START_TIME '2023-04-01 13:00:00',DURATION='20m'", true, "CALIBRATE RESOURCE START_TIME _UTF8MB4'2023-04-01 13:00:00' DURATION '20m'"),
        ("calibrate resource DURATION='20m' START_TIME '2023-04-01 13:00:00'", true, "CALIBRATE RESOURCE DURATION '20m' START_TIME _UTF8MB4'2023-04-01 13:00:00'"),
        ("calibrate resource   START_TIME '2023-04-01 13:00:00' END_TIME='2023-04-01 16:00:00',DURATION '20m'", true, "CALIBRATE RESOURCE START_TIME _UTF8MB4'2023-04-01 13:00:00' END_TIME _UTF8MB4'2023-04-01 16:00:00' DURATION '20m'"),
        ("calibrate resource START_TIME CURRENT_TIMESTAMP() END_TIME current_timestamp()", true, "CALIBRATE RESOURCE START_TIME CURRENT_TIMESTAMP() END_TIME CURRENT_TIMESTAMP()"),
        ("calibrate resource END_TIME now()", true, "CALIBRATE RESOURCE END_TIME NOW()"),
        ("calibrate resource START_TIME now()", true, "CALIBRATE RESOURCE START_TIME NOW()"),
        ("calibrate resource START_TIME NOW() END_TIME now()", true, "CALIBRATE RESOURCE START_TIME NOW() END_TIME NOW()"),
        ("calibrate resource START_TIME CURRENT_TIMESTAMP() - interval 10 minute END_TIME now()", true, "CALIBRATE RESOURCE START_TIME DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE) END_TIME NOW()"),
        ("calibrate resource START_TIME now() - 1000 END_TIME current_timestamp()", true, "CALIBRATE RESOURCE START_TIME NOW()-1000 END_TIME CURRENT_TIMESTAMP()"),
        ("calibrate resource START_TIME CURRENT_TIMESTAMP() - interval 20 minute DURATION interval 15 minute", true, "CALIBRATE RESOURCE START_TIME DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 MINUTE) DURATION INTERVAL 15 MINUTE"),
        ("calibrate resource START_TIME CURRENT_TIMESTAMP() - interval 20 minute DURATION '15m'", true, "CALIBRATE RESOURCE START_TIME DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 MINUTE) DURATION '15m'"),
        ("calibrate resource END_TIME now() START_TIME CURRENT_TIMESTAMP() - interval 20 minute", true, "CALIBRATE RESOURCE END_TIME NOW() START_TIME DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 MINUTE)"),
        ("calibrate resource workload", false, ""),
        ("calibrate resource workload tpcc", true, "CALIBRATE RESOURCE WORKLOAD TPCC"),
        ("calibrate resource workload oltp_read_write", true, "CALIBRATE RESOURCE WORKLOAD OLTP_READ_WRITE"),
        ("calibrate resource workload oltp_read_only", true, "CALIBRATE RESOURCE WORKLOAD OLTP_READ_ONLY"),
        ("calibrate resource workload oltp_write_only", true, "CALIBRATE RESOURCE WORKLOAD OLTP_WRITE_ONLY"),
        ("calibrate resource workload = oltp_read_write START_TIME '2023-04-01 13:00:00'", false, ""),
        ("query watch add SQL DIGEST b13858789fce00208f9a262c99621b7045f4869807cd4e6568008ae7ca19a377 ", true, "QUERY WATCH ADD SQL DIGEST `b13858789fce00208f9a262c99621b7045f4869807cd4e6568008ae7ca19a377`"),
        ("query watch add SQL DIGEST b13858789fce00208f9a262c99621b7045f4869807cd4e6568008ae7ca19a377 ", true, "QUERY WATCH ADD SQL DIGEST `b13858789fce00208f9a262c99621b7045f4869807cd4e6568008ae7ca19a377`"),
        ("query watch add SQL DIGEST 'b13858789fce00208f9a262c99621b7045f4869807cd4e6568008ae7ca19a377' ", true, "QUERY WATCH ADD SQL DIGEST _UTF8MB4'b13858789fce00208f9a262c99621b7045f4869807cd4e6568008ae7ca19a377'"),
        ("query watch add PLAN DIGEST `5e3ddd388f6012e328233dbcdda5d48f404e0536c6c54d9618233210f3d5762a` ", true, "QUERY WATCH ADD PLAN DIGEST `5e3ddd388f6012e328233dbcdda5d48f404e0536c6c54d9618233210f3d5762a`"),
        ("query watch add PLAN DIGEST @digest1 ", true, "QUERY WATCH ADD PLAN DIGEST @`digest1`"),
        ("query watch add SQL TEXT SIMILAR to 'select 1'", true, "QUERY WATCH ADD SQL TEXT SIMILAR TO _UTF8MB4'select 1'"),
        ("query watch add SQL TEXT EXACT to 'select 1'", true, "QUERY WATCH ADD SQL TEXT EXACT TO _UTF8MB4'select 1'"),
        ("query watch add SQL TEXT PLAN to 'select 1'", true, "QUERY WATCH ADD SQL TEXT PLAN TO _UTF8MB4'select 1'"),
        ("query watch add resource group `default` SQL TEXT SIMILAR to 'select 1'", true, "QUERY WATCH ADD RESOURCE GROUP `default` SQL TEXT SIMILAR TO _UTF8MB4'select 1'"),
        ("query watch add resource group @rg SQL TEXT SIMILAR to @sql1", true, "QUERY WATCH ADD RESOURCE GROUP @`rg` SQL TEXT SIMILAR TO @`sql1`"),
        ("query watch add resource group rg1 SQL TEXT SIMILAR to 'select 1'", true, "QUERY WATCH ADD RESOURCE GROUP `rg1` SQL TEXT SIMILAR TO _UTF8MB4'select 1'"),
        ("query watch add SQL TEXT SIMILAR to 'select 1' resource group rg1", true, "QUERY WATCH ADD SQL TEXT SIMILAR TO _UTF8MB4'select 1' RESOURCE GROUP `rg1`"),
        ("query watch add ACTION = KILL SQL TEXT SIMILAR to 'select 1'", true, "QUERY WATCH ADD ACTION = KILL SQL TEXT SIMILAR TO _UTF8MB4'select 1'"),
        ("query watch add ACTION COOLDOWN resource group rg1 SQL TEXT SIMILAR to 'select 1'", true, "QUERY WATCH ADD ACTION = COOLDOWN RESOURCE GROUP `rg1` SQL TEXT SIMILAR TO _UTF8MB4'select 1'"),
        ("query watch add resource group `default` resource group `rg1` SQL TEXT SIMILAR to 'select 1'", false, ""),
        ("query watch add SQL SIMILAR to 'select 1'", false, ""),
        ("query watch add SQL TEXT SIMILAR 'select 1'", false, ""),
        ("query watch remove 1", true, "QUERY WATCH REMOVE 1"),
        ("query watch remove resource group rg1", true, "QUERY WATCH REMOVE RESOURCE GROUP `rg1`"),
        ("query watch remove resource group @rg", true, "QUERY WATCH REMOVE RESOURCE GROUP @`rg`"),
        ("query watch remove", false, ""),
        ("replace /*+ SET_VAR(sql_mode='ALLOW_INVALID_DATES') */ into t values ('2004-04-31');", true, "REPLACE /*+ SET_VAR(sql_mode = 'ALLOW_INVALID_DATES')*/ INTO `t` VALUES (_UTF8MB4'2004-04-31')"),
    ];
    for (source, valid, expected) in rows {
        match parse_multi(source) {
            Ok(statements) if valid => {
                let restored = statements
                    .iter()
                    .map(Stmt::restore)
                    .collect::<Vec<_>>()
                    .join("; ");
                assert_eq!(restored, expected, "source SQL: {source}");
                for statement in statements {
                    let one = statement.restore();
                    assert_eq!(
                        parse(&one)
                            .unwrap_or_else(|error| panic!("{source}: {one}: {error:?}"))
                            .restore(),
                        one,
                        "round trip: {source}"
                    );
                }
            }
            Err(_) if !valid => {}
            result => panic!("source SQL: {source}; valid={valid}; result={result:?}"),
        }
    }
}
