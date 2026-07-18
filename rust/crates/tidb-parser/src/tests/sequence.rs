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

//! Exact parser and restore coverage attributable to
//! `pkg/parser/ddl_sequence_parser.go`.

use super::*;

fn assert_cases(cases: &[(&str, Option<&str>)]) {
    for (sql, expected) in cases {
        match expected {
            Some(expected) => assert_eq!(r(sql), *expected, "source SQL: {sql}"),
            None => assert!(parse(sql).is_err(), "source SQL unexpectedly parsed: {sql}"),
        }
    }
}

/// Every sequence-owned row in Go `parser_test.go::TestSimple`: two
/// ALTER INSTANCE rows, three ALTER RANGE rows, and four signed-boundary
/// rows. Keeping the count explicit makes future source additions visible.
#[test]
fn go_test_simple_sequence_rows() {
    let cases = [
        (
            "ALTER INSTANCE RELOAD TLS",
            Some("ALTER INSTANCE RELOAD TLS"),
        ),
        (
            "ALTER INSTANCE RELOAD TLS NO ROLLBACK ON ERROR",
            Some("ALTER INSTANCE RELOAD TLS NO ROLLBACK ON ERROR"),
        ),
        (
            "ALTER RANGE global PLACEMENT POLICY mypolicy",
            Some("ALTER RANGE `global` PLACEMENT POLICY = `mypolicy`"),
        ),
        (
            "ALTER RANGE global PLACEMENT POLICY default",
            Some("ALTER RANGE `global` PLACEMENT POLICY = `default`"),
        ),
        (
            "ALTER RANGE meta PLACEMENT POLICY mypolicy",
            Some("ALTER RANGE `meta` PLACEMENT POLICY = `mypolicy`"),
        ),
        (
            "CREATE SEQUENCE seq INCREMENT - 9223372036854775807",
            Some("CREATE SEQUENCE `seq` INCREMENT BY -9223372036854775807"),
        ),
        (
            "CREATE SEQUENCE seq INCREMENT - 9223372036854775808",
            Some("CREATE SEQUENCE `seq` INCREMENT BY -9223372036854775808"),
        ),
        (
            "CREATE SEQUENCE seq INCREMENT -9223372036854775808",
            Some("CREATE SEQUENCE `seq` INCREMENT BY -9223372036854775808"),
        ),
        ("CREATE SEQUENCE seq INCREMENT -9223372036854775809", None),
    ];
    assert_eq!(cases.len(), 9);
    assert_cases(&cases);
}

/// Every CREATE/DROP/ALTER SEQUENCE row in Go
/// `parser_test.go::TestDDL`, including duplicate rows retained by the Go
/// table. The two table-definition rows that merely use `sequence` as an
/// identifier belong to the table-parser source domain and stay there.
#[test]
fn go_test_ddl_sequence_rows() {
    let cases = [
        ("create sequence sequence", Some("CREATE SEQUENCE `sequence`")),
        ("create sequence seq", Some("CREATE SEQUENCE `seq`")),
        (
            "create sequence if not exists seq",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq`"),
        ),
        ("create sequence seq", Some("CREATE SEQUENCE `seq`")),
        ("create sequence seq", Some("CREATE SEQUENCE `seq`")),
        (
            "create sequence if not exists seq",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq`"),
        ),
        (
            "create sequence if not exists seq",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq`"),
        ),
        ("create sequence if not exists seq increment", None),
        (
            "create sequence if not exists seq increment 1",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1"),
        ),
        (
            "create sequence if not exists seq increment = 1",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1"),
        ),
        (
            "create sequence if not exists seq increment by 1",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1"),
        ),
        ("create sequence if not exists seq minvalue", None),
        (
            "create sequence if not exists seq minvalue 1",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` MINVALUE 1"),
        ),
        (
            "create sequence if not exists seq minvalue = 1",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` MINVALUE 1"),
        ),
        ("create sequence if not exists seq no", None),
        (
            "create sequence if not exists seq nominvalue",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` NO MINVALUE"),
        ),
        (
            "create sequence if not exists seq no minvalue",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` NO MINVALUE"),
        ),
        ("create sequence if not exists seq maxvalue", None),
        (
            "create sequence if not exists seq maxvalue 1",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` MAXVALUE 1"),
        ),
        (
            "create sequence if not exists seq maxvalue = 1",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` MAXVALUE 1"),
        ),
        ("create sequence if not exists seq no", None),
        (
            "create sequence if not exists seq nomaxvalue",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` NO MAXVALUE"),
        ),
        (
            "create sequence if not exists seq no maxvalue",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` NO MAXVALUE"),
        ),
        ("create sequence if not exists seq start", None),
        ("create sequence if not exists seq start with", None),
        ("create sequence if not exists seq start =", None),
        ("create sequence if not exists seq start with", None),
        (
            "create sequence if not exists seq start 1",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` START WITH 1"),
        ),
        (
            "create sequence if not exists seq start = 1",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` START WITH 1"),
        ),
        (
            "create sequence if not exists seq start with 1",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` START WITH 1"),
        ),
        ("create sequence if not exists seq cache", None),
        (
            "create sequence if not exists seq cache 1",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` CACHE 1"),
        ),
        (
            "create sequence if not exists seq cache = 1",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` CACHE 1"),
        ),
        (
            "create sequence if not exists seq nocache",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` NOCACHE"),
        ),
        (
            "create sequence if not exists seq no cache",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` NOCACHE"),
        ),
        (
            "create sequence if not exists seq cycle",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` CYCLE"),
        ),
        (
            "create sequence if not exists seq nocycle",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` NOCYCLE"),
        ),
        (
            "create sequence if not exists seq no cycle",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` NOCYCLE"),
        ),
        (
            "create sequence seq increment 1 start with 0 minvalue 0 maxvalue 1000",
            Some("CREATE SEQUENCE `seq` INCREMENT BY 1 START WITH 0 MINVALUE 0 MAXVALUE 1000"),
        ),
        (
            "create sequence seq increment 1 start with 0 minvalue 0 maxvalue 1000",
            Some("CREATE SEQUENCE `seq` INCREMENT BY 1 START WITH 0 MINVALUE 0 MAXVALUE 1000"),
        ),
        (
            "create sequence seq increment 10 start with 0 minvalue 0 maxvalue 1000",
            Some("CREATE SEQUENCE `seq` INCREMENT BY 10 START WITH 0 MINVALUE 0 MAXVALUE 1000"),
        ),
        (
            "create sequence if not exists seq cache 1 increment 1 start with -1 minvalue 0 maxvalue 1000",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` CACHE 1 INCREMENT BY 1 START WITH -1 MINVALUE 0 MAXVALUE 1000"),
        ),
        (
            "create sequence sEq start with 0 minvalue 0 maxvalue 1000",
            Some("CREATE SEQUENCE `sEq` START WITH 0 MINVALUE 0 MAXVALUE 1000"),
        ),
        (
            "create sequence if not exists seq increment 1 start with 0 minvalue -2 maxvalue 1000",
            Some("CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1 START WITH 0 MINVALUE -2 MAXVALUE 1000"),
        ),
        (
            "create sequence seq increment -1 start with -1 minvalue -1 maxvalue -1000 cache = 10 nocycle",
            Some("CREATE SEQUENCE `seq` INCREMENT BY -1 START WITH -1 MINVALUE -1 MAXVALUE -1000 CACHE 10 NOCYCLE"),
        ),
        ("drop sequence", None),
        ("drop sequence seq", Some("DROP SEQUENCE `seq`")),
        (
            "drop sequence if exists seq",
            Some("DROP SEQUENCE IF EXISTS `seq`"),
        ),
        ("drop sequence seq", Some("DROP SEQUENCE `seq`")),
        (
            "drop sequence if exists seq",
            Some("DROP SEQUENCE IF EXISTS `seq`"),
        ),
        (
            "drop sequence if exists seq, seq2, seq3",
            Some("DROP SEQUENCE IF EXISTS `seq`, `seq2`, `seq3`"),
        ),
        ("drop sequence seq seq2", None),
        (
            "drop sequence seq, seq2",
            Some("DROP SEQUENCE `seq`, `seq2`"),
        ),
        ("alter sequence seq", None),
        ("alter sequence seq comment=\"haha\"", None),
        (
            "alter sequence seq start = 1",
            Some("ALTER SEQUENCE `seq` START WITH 1"),
        ),
        (
            "alter sequence seq start with 1 increment by 1",
            Some("ALTER SEQUENCE `seq` START WITH 1 INCREMENT BY 1"),
        ),
        (
            "alter sequence seq start with 1 increment by 2 minvalue 0 maxvalue 100",
            Some("ALTER SEQUENCE `seq` START WITH 1 INCREMENT BY 2 MINVALUE 0 MAXVALUE 100"),
        ),
        (
            "alter sequence seq increment -1 start with -1 minvalue -1 maxvalue -1000 cache = 10 nocycle",
            Some("ALTER SEQUENCE `seq` INCREMENT BY -1 START WITH -1 MINVALUE -1 MAXVALUE -1000 CACHE 10 NOCYCLE"),
        ),
        (
            "alter sequence if exists seq2 increment = 2",
            Some("ALTER SEQUENCE IF EXISTS `seq2` INCREMENT BY 2"),
        ),
        (
            "alter sequence seq restart",
            Some("ALTER SEQUENCE `seq` RESTART"),
        ),
        (
            "alter sequence seq start with 3 restart with 5",
            Some("ALTER SEQUENCE `seq` START WITH 3 RESTART WITH 5"),
        ),
        (
            "alter sequence seq restart = 5",
            Some("ALTER SEQUENCE `seq` RESTART WITH 5"),
        ),
        ("create sequence seq restart = 5", None),
    ];
    assert_eq!(cases.len(), 64);
    assert_cases(&cases);
}

/// All 33 rows from Go `ast/ddl_test.go::TestSequenceRestore`.
#[test]
fn go_test_sequence_restore_rows() {
    let cases = [
        ("create sequence seq", "CREATE SEQUENCE `seq`"),
        (
            "create sequence if not exists seq",
            "CREATE SEQUENCE IF NOT EXISTS `seq`",
        ),
        (
            "create sequence if not exists seq",
            "CREATE SEQUENCE IF NOT EXISTS `seq`",
        ),
        (
            "create sequence if not exists seq increment 1",
            "CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1",
        ),
        (
            "create sequence if not exists seq increment = 1",
            "CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1",
        ),
        (
            "create sequence if not exists seq minvalue 1",
            "CREATE SEQUENCE IF NOT EXISTS `seq` MINVALUE 1",
        ),
        (
            "create sequence if not exists seq minvalue = 1",
            "CREATE SEQUENCE IF NOT EXISTS `seq` MINVALUE 1",
        ),
        (
            "create sequence if not exists seq nominvalue",
            "CREATE SEQUENCE IF NOT EXISTS `seq` NO MINVALUE",
        ),
        (
            "create sequence if not exists seq no minvalue",
            "CREATE SEQUENCE IF NOT EXISTS `seq` NO MINVALUE",
        ),
        (
            "create sequence if not exists seq maxvalue 1",
            "CREATE SEQUENCE IF NOT EXISTS `seq` MAXVALUE 1",
        ),
        (
            "create sequence if not exists seq maxvalue = 1",
            "CREATE SEQUENCE IF NOT EXISTS `seq` MAXVALUE 1",
        ),
        (
            "create sequence if not exists seq nomaxvalue",
            "CREATE SEQUENCE IF NOT EXISTS `seq` NO MAXVALUE",
        ),
        (
            "create sequence if not exists seq no maxvalue",
            "CREATE SEQUENCE IF NOT EXISTS `seq` NO MAXVALUE",
        ),
        (
            "create sequence if not exists seq start 1",
            "CREATE SEQUENCE IF NOT EXISTS `seq` START WITH 1",
        ),
        (
            "create sequence if not exists seq start with 1",
            "CREATE SEQUENCE IF NOT EXISTS `seq` START WITH 1",
        ),
        (
            "create sequence if not exists seq cache 1",
            "CREATE SEQUENCE IF NOT EXISTS `seq` CACHE 1",
        ),
        (
            "create sequence if not exists seq nocache",
            "CREATE SEQUENCE IF NOT EXISTS `seq` NOCACHE",
        ),
        (
            "create sequence if not exists seq no cache",
            "CREATE SEQUENCE IF NOT EXISTS `seq` NOCACHE",
        ),
        (
            "create sequence if not exists seq cycle",
            "CREATE SEQUENCE IF NOT EXISTS `seq` CYCLE",
        ),
        (
            "create sequence if not exists seq nocycle",
            "CREATE SEQUENCE IF NOT EXISTS `seq` NOCYCLE",
        ),
        (
            "create sequence if not exists seq no cycle",
            "CREATE SEQUENCE IF NOT EXISTS `seq` NOCYCLE",
        ),
        (
            "create sequence seq increment 1 minvalue 0 maxvalue 1000",
            "CREATE SEQUENCE `seq` INCREMENT BY 1 MINVALUE 0 MAXVALUE 1000",
        ),
        (
            "create sequence seq minvalue 0 maxvalue 1000 increment 1",
            "CREATE SEQUENCE `seq` MINVALUE 0 MAXVALUE 1000 INCREMENT BY 1",
        ),
        (
            "create sequence seq cache = 1 minvalue 0 maxvalue -1000",
            "CREATE SEQUENCE `seq` CACHE 1 MINVALUE 0 MAXVALUE -1000",
        ),
        (
            "create sequence seq increment -1 minvalue 0 maxvalue -1000",
            "CREATE SEQUENCE `seq` INCREMENT BY -1 MINVALUE 0 MAXVALUE -1000",
        ),
        (
            "create sequence seq nocycle nocache maxvalue 1000 cache 1",
            "CREATE SEQUENCE `seq` NOCYCLE NOCACHE MAXVALUE 1000 CACHE 1",
        ),
        (
            "create sequence seq increment -1 no minvalue no maxvalue cache = 1",
            "CREATE SEQUENCE `seq` INCREMENT BY -1 NO MINVALUE NO MAXVALUE CACHE 1",
        ),
        (
            "create sequence if not exists seq increment 1 minvalue 0 nomaxvalue cache 100 nocycle",
            "CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1 MINVALUE 0 NO MAXVALUE CACHE 100 NOCYCLE",
        ),
        ("drop sequence seq", "DROP SEQUENCE `seq`"),
        ("drop sequence seq, seq2", "DROP SEQUENCE `seq`, `seq2`"),
        (
            "drop sequence if exists seq, seq2",
            "DROP SEQUENCE IF EXISTS `seq`, `seq2`",
        ),
        (
            "drop sequence if exists seq",
            "DROP SEQUENCE IF EXISTS `seq`",
        ),
        ("drop sequence sequence", "DROP SEQUENCE `sequence`"),
    ];
    assert_eq!(cases.len(), 33);
    for (sql, expected) in cases {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn shared_table_options_and_invalid_state_boundaries() {
    assert_eq!(
        r("create sequence db.s comment='hi' cycle engine=InnoDB row_format=compact"),
        "CREATE SEQUENCE `db`.`s` CYCLE COMMENT = 'hi' ENGINE = InnoDB ROW_FORMAT = COMPACT"
    );
    assert_eq!(
        r("alter range global primary_region='us'"),
        "ALTER RANGE `global` PRIMARY_REGION = 'us'"
    );
    // Go admits bare ALTER RANGE into an AST that panics during restore.
    // Rust rejects that impossible state at its typed parser boundary.
    assert!(parse("alter range global").is_err());
    assert!(parse("alter instance reload").is_err());
    assert!(parse("alter instance reload tls no rollback").is_err());
}

#[test]
fn alter_instance_accepts_go_keyword_literals_across_token_kinds() {
    // Go's Token.IsKeyword compares the token literal after the dedicated
    // RELOAD/TLS-token check, so quoted string tokens with those literals are
    // accepted too. These exact outcomes were checked through the Go oracle.
    assert_eq!(
        r("alter instance 'RELOAD' 'TLS'"),
        "ALTER INSTANCE RELOAD TLS"
    );
    assert_eq!(
        r("alter instance 'reload' tls"),
        "ALTER INSTANCE RELOAD TLS"
    );
    assert!(parse("alter instance 'WRONG' 'TLS'").is_err());
}

#[test]
fn sequence_source_statements_have_typed_ddl_variants() {
    let cases = [
        ("create sequence s", "create"),
        ("alter sequence s restart", "alter"),
        ("drop sequence s", "drop"),
        ("alter instance reload tls", "instance"),
        ("alter range global placement policy default", "range"),
    ];
    for (sql, expected) in cases {
        let statement = parse(sql).expect("parse sequence-owned statement");
        let actual = match statement {
            tidb_ast::Stmt::Ddl(ddl) => match *ddl {
                tidb_ast::DdlStmt::CreateSequence(_) => "create",
                tidb_ast::DdlStmt::AlterSequence(_) => "alter",
                tidb_ast::DdlStmt::DropSequence(_) => "drop",
                tidb_ast::DdlStmt::AlterInstance(_) => "instance",
                tidb_ast::DdlStmt::AlterRange(_) => "range",
                _ => panic!("unexpected DDL variant for {sql}"),
            },
            _ => panic!("unexpected statement envelope for {sql}"),
        };
        assert_eq!(actual, expected);
    }
}
