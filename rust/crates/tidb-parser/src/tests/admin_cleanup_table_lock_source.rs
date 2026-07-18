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

//! Direct source coverage for Go's `CleanupTableLockStmt` parser/restore.

use super::*;

/// Exact original `TestAdminStmt` rows at `pkg/parser/parser_test.go:5826-5828`.
#[test]
fn admin_cleanup_table_lock_restores_original_go_rows() {
    for (sql, restored) in [
        ("ADMIN CLEANUP TABLE LOCK t", "ADMIN CLEANUP TABLE LOCK `t`"),
        (
            "ADMIN CLEANUP TABLE LOCK t1,t2",
            "ADMIN CLEANUP TABLE LOCK `t1`, `t2`",
        ),
    ] {
        assert_eq!(r(sql), restored, "source SQL: {sql}");
    }
    assert!(parse("ADMIN CLEANUP TABLE LOCK").is_err());
}

#[test]
fn admin_cleanup_table_lock_retains_typed_table_paths() {
    let Stmt::Admin(admin) = parse("admin cleanup table lock db.t1, t2").expect("parse") else {
        panic!("expected ADMIN statement");
    };
    let tidb_ast::AdminStmt::CleanupTableLock(cleanup) = admin.as_ref() else {
        panic!("expected typed cleanup table lock statement");
    };
    assert_eq!(
        cleanup.tables,
        vec![
            vec!["db".to_owned(), "t1".to_owned()],
            vec!["t2".to_owned()],
        ]
    );
}
