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

//! Direct `pkg/parser/parser_test.go:TestPrivilege` GRANT `REQUIRE` rows.

use super::*;

#[test]
fn grant_require_tls_rows_restore_like_go_testddl() {
    for (sql, expected) in [
        (
            "GRANT ALL ON db1.* TO 'jeffrey'@'localhost' REQUIRE X509",
            "GRANT ALL ON `db1`.* TO `jeffrey`@`localhost` REQUIRE X509",
        ),
        (
            "GRANT ALL ON db1.* TO 'jeffrey'@'LOCALhost' REQUIRE SSL",
            "GRANT ALL ON `db1`.* TO `jeffrey`@`localhost` REQUIRE SSL",
        ),
        (
            "GRANT ALL ON db1.* TO 'jeffrey'@'localhost' REQUIRE NONE",
            "GRANT ALL ON `db1`.* TO `jeffrey`@`localhost` REQUIRE NONE",
        ),
        (
            "GRANT ALL ON db1.* TO 'jeffrey'@'localhost' REQUIRE ISSUER '/C=SE/ST=Stockholm/L=Stockholm/O=MySQL/CN=CA/emailAddress=ca@example.com' AND CIPHER 'EDH-RSA-DES-CBC3-SHA'",
            "GRANT ALL ON `db1`.* TO `jeffrey`@`localhost` REQUIRE ISSUER '/C=SE/ST=Stockholm/L=Stockholm/O=MySQL/CN=CA/emailAddress=ca@example.com' AND CIPHER 'EDH-RSA-DES-CBC3-SHA'",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn grant_require_tls_keeps_typed_options_before_grant_option() {
    let statement = parse(
        "GRANT SELECT ON db.t TO u REQUIRE CIPHER 'cipher' AND SUBJECT 'subject' WITH GRANT OPTION",
    )
    .expect("GRANT REQUIRE parses");
    let Stmt::Admin(admin) = statement else {
        panic!("expected administrative statement");
    };
    let AdminStmt::Grant(grant) = admin.as_ref() else {
        panic!("expected typed GRANT statement");
    };
    assert_eq!(grant.tls_options.len(), 2);
    assert!(matches!(
        grant.tls_options[0],
        tidb_ast::AlterUserTlsOption::Cipher(ref value) if value == "cipher"
    ));
    assert!(matches!(
        grant.tls_options[1],
        tidb_ast::AlterUserTlsOption::Subject(ref value) if value == "subject"
    ));
    assert!(grant.with_grant);
}

#[test]
fn grant_require_tls_rejects_missing_or_unknown_option() {
    for sql in [
        "GRANT SELECT ON db.t TO u REQUIRE",
        "GRANT SELECT ON db.t TO u REQUIRE CIPHER",
        "GRANT SELECT ON db.t TO u REQUIRE UNKNOWN",
        "GRANT SELECT ON db.t TO u REQUIRE SSL AND SSL",
    ] {
        assert!(parse(sql).is_err(), "source SQL: {sql}");
    }
}
