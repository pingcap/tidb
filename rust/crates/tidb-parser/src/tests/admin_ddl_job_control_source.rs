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

//! Source rows for Go's shared DDL-job control parser helper.

use super::*;

/// Exact original `TestAdminStmt` rows at `pkg/parser/parser_test.go:508-517`.
#[test]
fn admin_ddl_job_controls_restore_original_go_rows() {
    for (sql, restored) in [
        ("admin cancel ddl jobs 1", "ADMIN CANCEL DDL JOBS 1"),
        ("admin cancel ddl jobs 1, 2", "ADMIN CANCEL DDL JOBS 1, 2"),
        ("admin pause ddl jobs 1, 3", "ADMIN PAUSE DDL JOBS 1, 3"),
        ("admin pause ddl jobs 5", "ADMIN PAUSE DDL JOBS 5"),
        ("admin resume ddl jobs 1, 2", "ADMIN RESUME DDL JOBS 1, 2"),
        ("admin resume ddl jobs 3", "ADMIN RESUME DDL JOBS 3"),
    ] {
        assert_eq!(r(sql), restored, "{sql}");
    }
    for sql in [
        "admin pause ddl jobs",
        "admin pause ddl jobs str_not_num",
        "admin resume ddl jobs",
        "admin resume ddl jobs str_not_num",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}

/// Go's hand parser discards one required token after `DDL`, regardless of
/// spelling, then canonicalizes it to `JOBS`; it does not accept reordered
/// command words. Both sides are confirmed by `godump restore`.
#[test]
fn admin_ddl_job_control_preserves_go_discarded_noun_token_behavior() {
    for (sql, restored) in [
        ("admin cancel ddl jobs 1", "ADMIN CANCEL DDL JOBS 1"),
        ("admin cancel ddl job 1", "ADMIN CANCEL DDL JOBS 1"),
        ("admin cancel ddl foo 1", "ADMIN CANCEL DDL JOBS 1"),
        ("admin pause ddl jobs 1", "ADMIN PAUSE DDL JOBS 1"),
        ("admin pause ddl job 1", "ADMIN PAUSE DDL JOBS 1"),
        ("admin pause ddl foo 1", "ADMIN PAUSE DDL JOBS 1"),
        ("admin resume ddl jobs 1", "ADMIN RESUME DDL JOBS 1"),
        ("admin resume ddl job 1", "ADMIN RESUME DDL JOBS 1"),
        ("admin resume ddl foo 1", "ADMIN RESUME DDL JOBS 1"),
    ] {
        assert_eq!(r(sql), restored, "{sql}");
    }
    assert!(parse("admin pause foo ddl jobs 1").is_err());
}

#[test]
fn admin_ddl_job_control_retains_typed_kind_and_ids() {
    let tidb_ast::Stmt::Admin(admin) = parse("admin pause ddl jobs 1, 3").expect("parse") else {
        panic!("expected ADMIN statement");
    };
    let tidb_ast::AdminStmt::DdlJobControl(control) = admin.as_ref() else {
        panic!("expected typed DDL job control");
    };
    assert_eq!(control.kind, tidb_ast::AdminDdlJobControlKind::Pause);
    assert_eq!(control.job_ids, [1, 3]);
}
