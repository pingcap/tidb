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

//! Direct CREATE USER statement-global TLS, resource-option, and resource-group
//! rows from Go `HandParser.parseCreateUserStmt`, `parseTLSOptions`, and
//! `parseResourceOptions`, anchored in `pkg/parser/parser_test.go:TestPrivilege`.

use super::*;

#[test]
fn create_user_require_options_restore_in_go_order_with_canonical_and() {
    for (sql, expected) in [
        (
            "create user ttt require x509",
            "CREATE USER `ttt`@`%` REQUIRE X509",
        ),
        (
            "create user ttt require issuer 'issuer' cipher 'cipher' subject 'subject'",
            "CREATE USER `ttt`@`%` REQUIRE ISSUER 'issuer' AND CIPHER 'cipher' AND SUBJECT 'subject'",
        ),
        (
            "create user ttt require token_issuer 'issuer-abc'",
            "CREATE USER `ttt`@`%` REQUIRE TOKEN_ISSUER 'issuer-abc'",
        ),
        // Go accepts duplicate string TLS clauses at parser time; later DDL
        // validation owns any runtime rejection, so restore retains both.
        (
            "create user ttt require cipher 'first' and cipher 'second'",
            "CREATE USER `ttt`@`%` REQUIRE CIPHER 'first' AND CIPHER 'second'",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
    for sql in [
        "create user ttt require ssl ssl",
        "create user ttt require x509 and x509",
        "create user ttt require none none",
    ] {
        assert!(parse(sql).is_err(), "source SQL: {sql}");
    }
}

#[test]
fn create_user_with_resource_options_is_typed_before_password_policy_tail() {
    let sql = "create user ttt require none with max_queries_per_hour 1 max_updates_per_hour 10 max_connections_per_hour 20 max_user_connections 30 password expire default account unlock";
    assert_eq!(
        r(sql),
        "CREATE USER `ttt`@`%` REQUIRE NONE WITH MAX_QUERIES_PER_HOUR 1 MAX_UPDATES_PER_HOUR 10 MAX_CONNECTIONS_PER_HOUR 20 MAX_USER_CONNECTIONS 30 PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"
    );
    let Stmt::Ddl(ddl) = parse(sql).expect("parse Go CREATE USER source row") else {
        panic!("expected DDL");
    };
    let tidb_ast::DdlStmt::CreateUser {
        tls_options,
        resource_options,
        password_options,
        ..
    } = ddl.into_inner()
    else {
        panic!("expected CREATE USER");
    };
    assert_eq!(tls_options, vec![tidb_ast::AlterUserTlsOption::None]);
    assert_eq!(
        resource_options,
        vec![
            tidb_ast::AlterUserResourceOption {
                kind: tidb_ast::AlterUserResourceKind::MaxQueriesPerHour,
                count: 1,
            },
            tidb_ast::AlterUserResourceOption {
                kind: tidb_ast::AlterUserResourceKind::MaxUpdatesPerHour,
                count: 10,
            },
            tidb_ast::AlterUserResourceOption {
                kind: tidb_ast::AlterUserResourceKind::MaxConnectionsPerHour,
                count: 20,
            },
            tidb_ast::AlterUserResourceOption {
                kind: tidb_ast::AlterUserResourceKind::MaxUserConnections,
                count: 30,
            },
        ]
    );
    assert_eq!(password_options.len(), 2);
    assert_eq!(r("create user ttt require with"), "CREATE USER `ttt`@`%`");
    for sql in [
        "create user ttt with max_queries_per_hour",
        "create user ttt with max_queries_per_hour 9223372036854775808",
    ] {
        assert!(parse(sql).is_err(), "source SQL: {sql}");
    }
}

/// `pkg/parser/parser_test.go:TestPrivilege` includes the authenticated row;
/// `tests/integrationtest/t/privilege/privileges.test:467` contributes the
/// current parser-queue example. The third row proves Go's source order after
/// REQUIRE/WITH/password/COMMENT rather than treating RESOURCE GROUP as a
/// per-user or `WITH` resource option.
#[test]
fn create_user_statement_global_resource_group_matches_go() {
    for (sql, expected) in [
        (
            "CREATE USER usr1 RESOURCE GROUP rg1",
            "CREATE USER `usr1`@`%` RESOURCE GROUP `rg1`",
        ),
        (
            "CREATE USER 'root'@'127.0.0.1' IDENTIFIED BY 'hashstring' RESOURCE GROUP rg1",
            "CREATE USER `root`@`127.0.0.1` IDENTIFIED BY 'hashstring' RESOURCE GROUP `rg1`",
        ),
        (
            "CREATE USER usr1 REQUIRE NONE WITH MAX_QUERIES_PER_HOUR 1 PASSWORD EXPIRE DEFAULT COMMENT 'owner' RESOURCE GROUP rg1",
            "CREATE USER `usr1`@`%` REQUIRE NONE WITH MAX_QUERIES_PER_HOUR 1 PASSWORD EXPIRE DEFAULT COMMENT 'owner' RESOURCE GROUP `rg1`",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }

    let Stmt::Ddl(ddl) = parse("CREATE USER usr1 RESOURCE GROUP rg1").expect("parse") else {
        panic!("expected DDL");
    };
    let tidb_ast::DdlStmt::CreateUser { resource_group, .. } = ddl.into_inner() else {
        panic!("expected CREATE USER");
    };
    assert_eq!(resource_group.as_deref(), Some("rg1"));

    for sql in [
        "CREATE USER usr1 RESOURCE GROUP",
        "CREATE USER usr1 RESOURCE GROUP select",
        "CREATE USER usr1 RESOURCE GROUP rg1 ACCOUNT LOCK",
    ] {
        assert!(parse(sql).is_err(), "source SQL: {sql}");
    }
}
