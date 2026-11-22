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

//! Go `pkg/executor/select_into_test.go`.

use super::sql_kit::{affected, error, exec_all, session};
use super::*;

fn cmp_and_rm(expected: &[u8], outfile: &str) {
    let content = std::fs::read(outfile).expect("the outfile exists");
    assert_eq!(
        String::from_utf8_lossy(&content),
        String::from_utf8_lossy(expected),
        "{outfile}"
    );
    assert_eq!(content, expected, "{outfile}");
    std::fs::remove_file(outfile).unwrap();
}

/// Go `randomSelectFilePath`.
fn random_select_file_path(test_name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    std::env::temp_dir()
        .join(format!("select-into-{test_name}-{}-{nanos}.data", std::process::id()))
        .to_string_lossy()
        .into_owned()
}

#[test]
fn test_select_into_file_exists() {
    let outfile = random_select_file_path("TestSelectIntoFileExists");
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    let sql = format!("select 1 into outfile '{outfile}'");
    exec_all(&mut tk, &[&sql]);
    let err = error(&mut tk, &sql);
    assert!(
        err.message.contains("already exists") || err.message.contains("file exists"),
        "err: {}",
        err.message
    );
    assert!(err.message.contains(&outfile), "err: {}", err.message);
    std::fs::remove_file(&outfile).unwrap();
}

#[test]
fn test_select_into_outfile_point_get() {
    let outfile = random_select_file_path("TestSelectIntoOutfilePointGet");
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(
        &mut tk,
        &[
            "create table t (id int not null, primary key (id) /*T![clustered_index] CLUSTERED */ );",
            "insert into t values(1);",
            &format!("select * from t where id = 1 into outfile '{outfile}'"),
        ],
    );
    cmp_and_rm(b"1\n", &outfile);
}

#[test]
fn test_select_into_outfile_types() {
    let outfile = random_select_file_path("TestSelectIntoOutfileTypes");
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    let into = format!("INTO OUTFILE '{outfile}'");

    exec_all(
        &mut tk,
        &[
            "drop table if exists t",
            "CREATE TABLE `t` ( `a` bit(10) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;",
            "INSERT INTO `t` VALUES (_binary '\\0'), (_binary '\\1'), (_binary '\\2'), (_binary '\\3');",
            &format!("SELECT * FROM t {into}"),
        ],
    );
    cmp_and_rm(b"\x00\x00\n\x001\n\x002\n\x003\n", &outfile);

    exec_all(
        &mut tk,
        &[
            "drop table if exists t",
            "CREATE TABLE `t` (col ENUM ('value1','value2','value3'));",
            "INSERT INTO t values ('value1'), ('value2');",
            &format!("SELECT * FROM t {into}"),
        ],
    );
    cmp_and_rm(b"value1\nvalue2\n", &outfile);

    exec_all(
        &mut tk,
        &[
            "drop table if exists t",
            "create table t ( v json);",
            r#"insert into t values ('{"id": 1, "name": "aaa"}'), ('{"id": 2, "name": "xxx"}');"#,
            &format!("SELECT * FROM t {into}"),
        ],
    );
    cmp_and_rm(b"{\"id\": 1, \"name\": \"aaa\"}\n{\"id\": 2, \"name\": \"xxx\"}\n", &outfile);

    exec_all(
        &mut tk,
        &[
            "drop table if exists t",
            "create table t (v tinyint unsigned)",
            "insert into t values (0), (1)",
            &format!("SELECT * FROM t {into}"),
        ],
    );
    cmp_and_rm(b"0\n1\n", &outfile);

    exec_all(
        &mut tk,
        &[
            "drop table if exists t",
            "create table t (id float(16,2)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
            "insert into t values (3.4), (1), (10.1), (2.00)",
            &format!("SELECT * FROM t ORDER BY id {into}"),
        ],
    );
    cmp_and_rm(b"1.00\n2.00\n3.40\n10.10\n", &outfile);
}

#[test]
fn test_select_into_outfile_from_table() {
    let outfile = random_select_file_path("TestSelectIntoOutfileFromTable");
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(
        &mut tk,
        &[
            "drop table if exists t",
            "create table t (i int, r real, d decimal(10, 5), s varchar(100), dt datetime, ts timestamp, du time, j json)",
            "insert into t values (1, 1.1, 0.1, 'a', '2000-01-01', '01:01:01', '01:01:01', '[1]')",
            "insert into t values (2, 2.2, 0.2, 'b', '2000-02-02', '02:02:02', '02:02:02', '[1,2]')",
            "insert into t values (null, null, null, null, '2000-03-03', '03:03:03', '03:03:03', '[1,2,3]')",
            "insert into t values (4, 4.4, 0.4, 'd', null, null, null, null)",
        ],
    );

    assert_eq!(affected(&mut tk, &format!("select * from t into outfile '{outfile}'")), 4);
    cmp_and_rm(
        b"1\t1.1\t0.10000\ta\t2000-01-01 00:00:00\t2001-01-01 00:00:00\t01:01:01\t[1]\n\
2\t2.2\t0.20000\tb\t2000-02-02 00:00:00\t2002-02-02 00:00:00\t02:02:02\t[1, 2]\n\
\\N\t\\N\t\\N\t\\N\t2000-03-03 00:00:00\t2003-03-03 00:00:00\t03:03:03\t[1, 2, 3]\n\
4\t4.4\t0.40000\td\t\\N\t\\N\t\\N\t\\N\n",
        &outfile,
    );

    assert_eq!(
        affected(
            &mut tk,
            &format!("select * from t into outfile '{outfile}' fields terminated by ',' enclosed by '\"' escaped by '#'"),
        ),
        4
    );
    cmp_and_rm(
        br#""1","1.1","0.10000","a","2000-01-01 00:00:00","2001-01-01 00:00:00","01:01:01","[1]"
"2","2.2","0.20000","b","2000-02-02 00:00:00","2002-02-02 00:00:00","02:02:02","[1, 2]"
#N,#N,#N,#N,"2000-03-03 00:00:00","2003-03-03 00:00:00","03:03:03","[1, 2, 3]"
"4","4.4","0.40000","d",#N,#N,#N,#N
"#,
        &outfile,
    );

    assert_eq!(
        affected(
            &mut tk,
            &format!(
                "select * from t into outfile '{outfile}' fields terminated by ',' optionally enclosed by '\"' escaped by '#'"
            ),
        ),
        4
    );
    cmp_and_rm(
        br#"1,1.1,0.10000,"a","2000-01-01 00:00:00","2001-01-01 00:00:00","01:01:01","[1]"
2,2.2,0.20000,"b","2000-02-02 00:00:00","2002-02-02 00:00:00","02:02:02","[1, 2]"
#N,#N,#N,#N,"2000-03-03 00:00:00","2003-03-03 00:00:00","03:03:03","[1, 2, 3]"
4,4.4,0.40000,"d",#N,#N,#N,#N
"#,
        &outfile,
    );

    assert_eq!(
        affected(
            &mut tk,
            &format!(
                "select * from t into outfile '{outfile}' fields terminated by ',' optionally enclosed by '\"' escaped by '#' lines terminated by '<<<\n'"
            ),
        ),
        4
    );
    cmp_and_rm(
        br#"1,1.1,0.10000,"a","2000-01-01 00:00:00","2001-01-01 00:00:00","01:01:01","[1]"<<<
2,2.2,0.20000,"b","2000-02-02 00:00:00","2002-02-02 00:00:00","02:02:02","[1, 2]"<<<
#N,#N,#N,#N,"2000-03-03 00:00:00","2003-03-03 00:00:00","03:03:03","[1, 2, 3]"<<<
4,4.4,0.40000,"d",#N,#N,#N,#N<<<
"#,
        &outfile,
    );
}

#[test]
fn test_select_into_outfile_lines_starting_by() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(
        &mut tk,
        &[
            "create table t (id int primary key, v varchar(20))",
            "insert into t values (1, 'x'), (2, 'y'), (3, null)",
            "set tidb_init_chunk_size = 1",
        ],
    );
    let dir = random_select_file_path("TestSelectIntoOutfileLinesStartingBy");
    std::fs::create_dir_all(&dir).unwrap();
    for (name, query, options, expected) in [
        ("default", "select * from t order by id", "fields terminated by ','", "1,x\n2,y\n3,\\N\n"),
        (
            "empty prefix",
            "select * from t order by id",
            "fields terminated by ',' lines starting by ''",
            "1,x\n2,y\n3,\\N\n",
        ),
        (
            "multiple rows",
            "select * from t order by id",
            "fields terminated by ',' lines starting by 'PREFIX'",
            "PREFIX1,x\nPREFIX2,y\nPREFIX3,\\N\n",
        ),
        (
            "literal prefix",
            "select * from t order by id",
            r#"fields terminated by ',' enclosed by '"' escaped by '#' lines starting by '#,"' terminated by 'END'"#,
            "#,\"\"1\",\"x\"END#,\"\"2\",\"y\"END#,\"\"3\",#NEND",
        ),
        (
            "null first field",
            "select v, id from t where id = 3",
            "fields terminated by ',' lines starting by 'PREFIX'",
            "PREFIX\\N,3\n",
        ),
        ("empty result", "select * from t where id < 0", "lines starting by 'PREFIX'", ""),
    ] {
        let outfile = format!("{dir}/out.txt");
        exec_all(&mut tk, &[&format!("{query} into outfile '{outfile}' {options}")]);
        let content = std::fs::read(&outfile).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(String::from_utf8_lossy(&content), expected, "{name}");
        std::fs::remove_file(&outfile).unwrap();
    }
    std::fs::remove_dir(&dir).unwrap();
}

#[test]
fn test_select_into_outfile_constant() {
    let outfile = random_select_file_path("TestSelectIntoOutfileConstant");
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    // test constants
    exec_all(
        &mut tk,
        &[&format!("select 1, 2, 3, '4', '5', '6', 7.7, 8.8, 9.9, null into outfile '{outfile}'")],
    );
    cmp_and_rm(b"1\t2\t3\t4\t5\t6\t7.7\t8.8\t9.9\t\\N\n", &outfile);

    exec_all(
        &mut tk,
        &[&format!(
            "select 1e10, 1e20, 1.234567e8, 0.000123e3, 1.01234567890123456789, 123456789e-10 into outfile '{outfile}'"
        )],
    );
    cmp_and_rm(b"10000000000\t1e20\t123456700\t0.123\t1.01234567890123456789\t0.0123456789\n", &outfile);
}

#[test]
fn test_deliminators() {
    let outfile = random_select_file_path("TestDeliminators");
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(&mut tk, &["CREATE TABLE `tx` (`a` varbinary(20) DEFAULT NULL,`b` int DEFAULT NULL)"]);
    let err = error(&mut tk, &format!("select * from `tx` into outfile '{outfile}' fields enclosed by '\"\"'"));
    // enclosed by must be a single character
    assert!(err.message.contains("Field separator argument is not what is expected"), "err: {}", err.message);
    let err = error(&mut tk, &format!("select * from `tx` into outfile '{outfile}' fields escaped by 'gg'"));
    // so does escaped by
    assert!(err.message.contains("Field separator argument is not what is expected"), "err: {}", err.message);

    // since the above two test cases failed, it should not has outfile remained on disk
    assert!(!std::path::Path::new(&outfile).exists());

    exec_all(
        &mut tk,
        &[
            "insert into tx values (NULL, NULL);\n",
            &format!("select * from `tx` into outfile '{outfile}' fields escaped by ''"),
        ],
    );
    // if escaped by is set as empty, then NULL should not be escaped
    cmp_and_rm(b"NULL\tNULL\n", &outfile);

    exec_all(
        &mut tk,
        &[
            "delete from tx",
            "insert into tx values ('d\",\"e\",', 3), ('\\\\', 2)",
            &format!(
                "select * from `tx` into outfile '{outfile}' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n'"
            ),
        ],
    );
    // enclosed by character & escaped by characters should be escaped, no matter what
    cmp_and_rm(b"\"d\\\",\\\"e\\\",\",\"3\"\n\"\\\\\",\"2\"\n", &outfile);

    exec_all(
        &mut tk,
        &[
            "delete from tx",
            "insert into tx values ('a\tb', 1)",
            &format!(
                "select * from `tx` into outfile '{outfile}' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' escaped by '\t' LINES TERMINATED BY '\\n'"
            ),
        ],
    );
    // enclosed by character & escaped by characters should be escaped, no matter what
    cmp_and_rm(b"\"a\t\tb\",\"1\"\n", &outfile);

    exec_all(
        &mut tk,
        &[
            "delete from tx",
            r#"insert into tx values ('d","e",', 1)"#,
            r#"insert into tx values (unhex("00"), 2)"#,
            r#"insert into tx values ("\r\n\b\Z\t", 3)"#,
            "insert into tx values (null, 4)",
            &format!(
                "select * from `tx` into outfile '{outfile}' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n'"
            ),
        ],
    );
    // line terminator will be escaped
    cmp_and_rm(
        b"\"d\\\",\\\"e\\\",\",\"1\"\n\"\\0\",\"2\"\n\"\r\\\n\x08\x1a\t\",\"3\"\n\\N,\"4\"\n",
        &outfile,
    );

    exec_all(
        &mut tk,
        &[
            "create table tb (s char(10), b bit(48), bb blob(6))",
            "insert into tb values ('\\0\\b\\n\\r\\t\\Z', _binary '\\0\\b\\n\\r\\t\\Z', unhex('00080A0D091A'))",
            &format!("select * from tb into outfile '{outfile}'"),
        ],
    );
    // bit type won't be escaped (verified on MySQL)
    cmp_and_rm(
        b"\\0\x08\\\n\r\\\t\x1a\t\x00\x08\n\r\t\x1a\t\\0\x08\\\n\r\\\t\x1a\n",
        &outfile,
    );

    exec_all(
        &mut tk,
        &[
            "create table zero (a varchar(10), b varchar(10), c varchar(10))",
            "insert into zero values (unhex('00'), _binary '\\0', '\\0')",
            &format!("select * from zero into outfile '{outfile}'"),
        ],
    );
    // zero will always be escaped
    cmp_and_rm(b"\\0\t\\0\t\\0\n", &outfile);
    exec_all(&mut tk, &[&format!("select * from zero into outfile '{outfile}' fields enclosed by '\"'")]);
    // zero will always be escaped, including when being enclosed
    cmp_and_rm(b"\"\\0\"\t\"\\0\"\t\"\\0\"\n", &outfile);

    exec_all(
        &mut tk,
        &[
            "create table tt (a char(10), b char(10), c char(10))",
            "insert into tt values ('abcd', 'abcd', 'abcd')",
            &format!("select * from tt into outfile '{outfile}' fields terminated by 'a-' lines terminated by 'b--'"),
        ],
    );
    // when not escaped, the first character of both terminators will be escaped
    cmp_and_rm(b"\\a\\bcda-\\a\\bcda-\\a\\bcdb--", &outfile);
    exec_all(
        &mut tk,
        &[&format!(
            "select * from tt into outfile '{outfile}' fields terminated by 'a-' enclosed by '\"' lines terminated by 'b--'"
        )],
    );
    // when escaped, only line terminator's first character will be escaped
    cmp_and_rm(b"\"a\\bcd\"a-\"a\\bcd\"a-\"a\\bcd\"b--", &outfile);
}

#[test]
fn test_escape_type() {
    let outfile = random_select_file_path("TestEscapeType");
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(
        &mut tk,
        &[
            "drop table if exists t",
            "create table t (
	a int,
	b double,
	c varchar(10),
	d blob,
	e json,
	f set('1', '2', '3'),
	g enum('1', '2', '3'))",
            r#"insert into t values (1, 1, "1", "1", '{"key": 1}', "1", "1")"#,
            &format!("select * from t into outfile '{outfile}' fields terminated by ',' escaped by '1'"),
        ],
    );
    cmp_and_rm(b"1,1,11,11,{\"key\": 11},11,11\n", &outfile);
}

#[test]
fn test_year_type() {
    let outfile = random_select_file_path("TestYearType");
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(
        &mut tk,
        &[
            "drop table if exists t",
            "create table t(time1 year(4) default '2030');",
            "insert into t values (2010), (2011), (2012);",
            "insert into t values ();",
            &format!(
                "select * from t into outfile '{outfile}' fields terminated by ',' optionally enclosed by '\"' lines terminated by '\\n';"
            ),
        ],
    );
    cmp_and_rm(b"2010\n2011\n2012\n2030\n", &outfile);
}
