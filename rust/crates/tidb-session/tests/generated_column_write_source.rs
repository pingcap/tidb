//! Stored generated columns in writes: assigning one directly fails with
//! Go's `ErrBadGeneratedColumn` (3105, "The value specified for generated
//! column ... is not allowed."), and an UPDATE that moves the base column
//! regenerates the stored value.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn generated_columns_reject_writes_and_regenerate_on_update() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b int as (a * 2) stored, v int)")
        .unwrap();

    // Assigning the generated column directly is refused (3105).
    let error = session
        .run("insert into t (a, b, v) values (1, 99, 5)")
        .expect_err("a generated column cannot be assigned");
    assert!(
        error
            .to_string()
            .contains("The value specified for generated column 'b' in table 't' is not allowed."),
        "{error}"
    );

    // The normal path: a plain insert materializes `b`, and moving `a`
    // regenerates it.
    session.run("insert into t (a, v) values (3, 8)").unwrap();
    assert_eq!(rows(&mut session, "select a, b from t"), "3|6");
    session.run("update t set a = 10 where a = 3").unwrap();
    assert_eq!(
        rows(&mut session, "select a, b, v from t order by a"),
        "10|20|8",
        "the stored value follows its base column"
    );
}

fn warning_codes(session: &mut Session) -> Vec<i64> {
    let tidb_session::StmtResult::Rows(rows) = session.run("show warnings").unwrap() else {
        panic!("expected warning rows");
    };
    rows.into_iter()
        .map(|row| match row[1] {
            tidb_datatype::Datum::Int(code) => code,
            tidb_datatype::Datum::UInt(code) => code as i64,
            ref other => panic!("expected warning code, got {other:?}"),
        })
        .collect()
}

#[test]
fn generated_write_policy_strict_insert_and_update() {
    for kind in ["stored", "virtual"] {
        let mut session = Session::new();
        session.run("set sql_mode = 'STRICT_TRANS_TABLES'").unwrap();
        session
            .run(&format!(
                "create table t (id int primary key, a int, b tinyint as (a) {kind}, key idx(b))"
            ))
            .unwrap();
        let error = session
            .run("insert into t(id,a) values (1,1000)")
            .expect_err("generated TINYINT must use the strict write context");
        assert_eq!(error.to_mysql_error().code, 1264);
        assert_eq!(rows(&mut session, "select count(*) from t"), "0");
        session.run("insert into t(id,a) values (1,7)").unwrap();
        let error = session
            .run("update t set a=1000")
            .expect_err("regenerated UPDATE value must be cast before storage");
        assert_eq!(error.to_mysql_error().code, 1264);
        assert_eq!(rows(&mut session, "select a,b from t"), "7|7");
    }
}

#[test]
fn generated_write_policy_warns_once_before_index_storage() {
    for statement in [
        "insert into t(id,a) values (1,1000)",
        "insert ignore into t(id,a) values (1,1000)",
    ] {
        let mut session = Session::new();
        session.run("set sql_mode = ''").unwrap();
        session
            .run("create table t (id int primary key, a int, b tinyint as (a) stored, key idx(b))")
            .unwrap();
        session.run(statement).unwrap();
        assert_eq!(warning_codes(&mut session), [1264]);
        assert_eq!(
            rows(
                &mut session,
                "select a,b from t force index(idx) where b=127"
            ),
            "1000|127"
        );
        session.run("update t set a=1001").unwrap();
        assert_eq!(warning_codes(&mut session), [1264]);
        assert_eq!(rows(&mut session, "select a,b from t"), "1001|127");
    }
}

#[test]
fn generated_write_policy_null_substitution_precedes_dependents() {
    let mut session = Session::new();
    session.run("set sql_mode = ''").unwrap();
    session.run("create table t (id int primary key, a int, b int as (a) stored not null, c int as (b+1) stored)").unwrap();
    let error = session
        .run("insert into t(id,a) values (1,null)")
        .expect_err("single-row INSERT promotes bad NULL even outside strict mode");
    assert_eq!(error.to_mysql_error().code, 1048);
    session
        .run("insert ignore into t(id,a) values (1,null)")
        .unwrap();
    assert_eq!(warning_codes(&mut session), [1048]);
    assert_eq!(rows(&mut session, "select a,b,c from t"), "Null|0|1");
}

#[test]
fn generated_write_policy_update_nulls_after_expression_dependencies() {
    let mut session = Session::new();
    session.run("set sql_mode = ''").unwrap();
    session.run("create table t (id int primary key, a int, b int as (a) stored not null, c int as (b+1) stored)").unwrap();
    session.run("insert into t(id,a) values (1,7)").unwrap();
    session.run("update t set a=null").unwrap();
    assert_eq!(warning_codes(&mut session), [1048]);
    // Go updateRecord finishes expressions before the row-wide NULL pass;
    // fillRow handles each INSERT generated NULL before its next dependency.
    assert_eq!(rows(&mut session, "select a,b,c from t"), "Null|0|Null");
}

#[test]
fn generated_write_policy_insert_variants_share_statement_context() {
    for statement in [
        "insert into t(id,a) values (1,7),(2,1000)",
        "insert into t(id,a) select 1,1000",
        "replace into t(id,a) values (1,1000)",
        "execute p using @id,@a",
    ] {
        let mut session = Session::new();
        session.run("set sql_mode = 'STRICT_TRANS_TABLES'").unwrap();
        session
            .run("create table t (id int primary key, a int, b tinyint as (a) stored)")
            .unwrap();
        session
            .run("prepare p from 'insert into t(id,a) values (?,?)'")
            .unwrap();
        session.run("set @id=1,@a=1000").unwrap();
        let error = session
            .run(statement)
            .expect_err("every insertion reaches the statement cast owner");
        assert_eq!(
            error.clone().to_mysql_error().code,
            1264,
            "{statement}: {error}"
        );
        if statement.contains("(2,1000)") {
            assert!(error.to_string().contains("at row 2"), "{error}");
        }
        assert_eq!(rows(&mut session, "select count(*) from t"), "0");
    }
}

#[test]
fn generated_write_policy_odku_and_joined_update() {
    for statement in [
        "insert into t(id,a) values (1,7) on duplicate key update a=1000",
        "update t join u on t.id=u.id set t.a=u.a",
    ] {
        let mut session = Session::new();
        session.run("set sql_mode = 'STRICT_TRANS_TABLES'").unwrap();
        session
            .run("create table t (id int primary key, a int, b tinyint as (a) stored, key idx(b))")
            .unwrap();
        session
            .run("create table u (id int primary key, a int)")
            .unwrap();
        session.run("insert into t(id,a) values (1,7)").unwrap();
        session.run("insert into u values (1,1000)").unwrap();
        let error = session
            .run(statement)
            .expect_err("all update producers share generated casting");
        assert_eq!(error.to_mysql_error().code, 1264);
        assert_eq!(rows(&mut session, "select a,b from t"), "7|7");
        session.run("set sql_mode = ''").unwrap();
        session.run(statement).unwrap();
        assert_eq!(warning_codes(&mut session), [1264]);
        assert_eq!(
            rows(
                &mut session,
                "select a,b from t force index(idx) where b=127"
            ),
            "1000|127"
        );
    }
}

#[test]
fn generated_write_policy_odku_warning_retains_expression_input() {
    let mut session = Session::new();
    session.run("set sql_mode = ''").unwrap();
    session
        .run("create table t (id int primary key, a varchar(10), b int as (a) stored)")
        .unwrap();
    session.run("insert into t(id,a) values (1,'7')").unwrap();
    session
        .run("insert into t(id,a) values (1,'7') on duplicate key update a='12abc'")
        .unwrap();
    let tidb_session::StmtResult::Rows(warnings) = session.run("show warnings").unwrap() else {
        panic!("warnings")
    };
    assert_eq!(warnings.len(), 1);
    let tidb_datatype::Datum::Bytes(message) = &warnings[0][2] else {
        panic!("warning message: {:?}", warnings[0][2])
    };
    assert_eq!(
        message,
        b"Incorrect int value: '12abc' for column 'b' at row 1"
    );
    assert_eq!(rows(&mut session, "select b from t"), "12");
}

#[test]
fn generated_write_policy_cascade_materializes_before_nested_dependents() {
    let mut session = Session::new();
    session.run("set sql_mode = 'STRICT_TRANS_TABLES'").unwrap();
    session.run("create table p (id int primary key)").unwrap();
    session.run("create table c (id int primary key, pid int, b tinyint as (pid) stored, unique key idx(b), foreign key(pid) references p(id) on update cascade)").unwrap();
    session.run("create table g (id int primary key, bid tinyint, foreign key(bid) references c(b) on update cascade)").unwrap();
    session.run("insert into p values (7)").unwrap();
    session.run("insert into c(id,pid) values (1,7)").unwrap();
    session.run("insert into g values (1,7)").unwrap();
    session.run("update p set id=9").unwrap();
    assert_eq!(rows(&mut session, "select pid,b from c"), "9|9");
    assert_eq!(rows(&mut session, "select bid from g"), "9");
    let error = session
        .run("update p set id=1000")
        .expect_err("cascade uses the active strict conversion owner");
    assert_eq!(error.to_mysql_error().code, 1264);
    assert_eq!(rows(&mut session, "select id from p"), "9");
    assert_eq!(rows(&mut session, "select pid,b from c"), "9|9");
    assert_eq!(rows(&mut session, "select bid from g"), "9");
}

#[test]
fn generated_write_policy_uses_the_ordinary_column_cast_for_all_types() {
    for (source_type, target_type, value, strict_error) in [
        ("int", "tinyint unsigned", "-5", Some(1264)),
        ("varchar(32)", "varchar(3)", "'abcdef'", Some(1406)),
        ("varchar(32)", "date", "'0000-00-00'", Some(1292)),
        ("varchar(32)", "char(3)", "'a  '", None),
    ] {
        for mode in ["STRICT_TRANS_TABLES,NO_ZERO_DATE", ""] {
            let mut session = Session::new();
            session.run(&format!("set sql_mode='{mode}'")).unwrap();
            session
                .run(&format!("create table ordinary_col (b {target_type})"))
                .unwrap();
            session
                .run(&format!(
                    "create table generated_col (a {source_type}, b {target_type} as (a) stored)"
                ))
                .unwrap();
            let ordinary = session.run(&format!("insert into ordinary_col values ({value})"));
            let ordinary_warnings = warning_codes(&mut session);
            let generated = session.run(&format!("insert into generated_col(a) values ({value})"));
            let generated_warnings = warning_codes(&mut session);
            if let Some(code) = strict_error.filter(|_| !mode.is_empty()) {
                assert_eq!(ordinary.unwrap_err().to_mysql_error().code, code);
                assert_eq!(generated.unwrap_err().to_mysql_error().code, code);
            } else {
                ordinary.unwrap();
                generated.unwrap();
                assert_eq!(
                    generated_warnings, ordinary_warnings,
                    "{target_type}, {mode}"
                );
                assert_eq!(
                    rows(&mut session, "select cast(b as char) from generated_col"),
                    rows(&mut session, "select cast(b as char) from ordinary_col")
                );
            }
        }
    }
}

#[test]
fn generated_read_policy_uses_column_cast_and_raw_warning_shape() {
    let mut session = Session::new();
    session.run("set sql_mode=''").unwrap();
    session
        .run("create table t (id int primary key, a varchar(12), b int as (a) virtual)")
        .unwrap();
    session.run("insert into t(id,a) values (1,'12x')").unwrap();
    assert_eq!(rows(&mut session, "select b from t where id=1"), "12");
    let tidb_session::StmtResult::Rows(warnings) = session.run("show warnings").unwrap() else {
        panic!("warnings")
    };
    assert_eq!(warnings.len(), 1);
    assert_eq!(
        warnings[0][2],
        tidb_datatype::Datum::new_bytes(b"Truncated incorrect DOUBLE value: '12x'".to_vec())
    );
}

#[test]
fn generated_read_policy_virtual_fill_substitutes_null_and_clips_unsigned() {
    let mut session = Session::new();
    session.run("set sql_mode=''").unwrap();
    session.run("create table t (id int primary key, a bigint, b bigint unsigned as (a) virtual, c int as (a) virtual not null, d int as (c+1) virtual)").unwrap();
    session
        .run("insert ignore into t(id,a) values (1,null),(2,-5)")
        .unwrap();
    assert_eq!(rows(&mut session, "select c,d from t where id=1"), "0|1");
    assert_eq!(
        rows(&mut session, "select cast(b as signed) from t where id=2"),
        "0"
    );
}

#[test]
fn analyze_preserves_generated_execution_error() {
    let mut session = Session::new();
    session.run("create table t (a double)").unwrap();
    session.run("insert into t values (0)").unwrap();
    session
        .run("alter table t add column b double as (cot(a)) virtual")
        .unwrap();
    let original = session
        .run("select b from t")
        .expect_err("invalid virtual column")
        .to_mysql_error();
    let error = session
        .run("analyze table t")
        .expect_err("invalid virtual sample")
        .to_mysql_error();
    assert_eq!((error.code, error.state), (1690, *b"22003"));
    assert_eq!(error.message, original.message);
    assert_eq!(rows(&mut session, "select 1"), "1");
    assert_eq!(
        rows(&mut session, "show stats_histograms where table_name = 't'"),
        ""
    );
    session.run("alter table t drop column b").unwrap();
    session.run("analyze table t").unwrap();
    assert!(!rows(&mut session, "show stats_histograms where table_name = 't'").is_empty());
}
