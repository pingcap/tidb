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

//! Direct Go `TestDBAStmt` rows for `SHOW MASTER STATUS` and
//! `SHOW PRIVILEGES` (`pkg/parser/parser_test.go:1310-1312`).

use super::*;

#[test]
fn show_master_status_and_privileges_restore_source_rows() {
    assert_eq!(r("show master status"), "SHOW MASTER STATUS");
    assert_eq!(r("show privileges"), "SHOW PRIVILEGES");
}

#[test]
fn show_master_status_and_privileges_have_distinct_typed_leaves() {
    let master = parse("show master status").expect("SHOW MASTER STATUS parses");
    let Stmt::Admin(master) = master else {
        panic!("SHOW MASTER STATUS must use Admin envelope");
    };
    assert!(matches!(
        master.as_ref(),
        tidb_ast::AdminStmt::ShowMasterStatus
    ));

    let privileges = parse("show privileges").expect("SHOW PRIVILEGES parses");
    let Stmt::Admin(privileges) = privileges else {
        panic!("SHOW PRIVILEGES must use Admin envelope");
    };
    assert!(matches!(
        privileges.as_ref(),
        tidb_ast::AdminStmt::ShowPrivileges
    ));
}

#[test]
fn show_master_status_and_privileges_reject_trailing_or_missing_payload() {
    for sql in [
        "show master",
        "show master status like 'x'",
        "show privileges like 'x'",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}
