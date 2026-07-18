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

//! Source rows for Go's `ADMIN ALTER DDL JOBS` parser branch.

use super::*;

/// Exact original `TestAdminStmt` rows at `pkg/parser/parser_test.go:546-553`.
#[test]
fn admin_alter_ddl_jobs_restores_go_option_rows() {
    for (sql, restored) in [
        (
            "admin alter ddl jobs 1 thread = 2",
            "ADMIN ALTER DDL JOBS 1 thread = 2",
        ),
        (
            "admin alter ddl jobs 1 batch_size = 3",
            "ADMIN ALTER DDL JOBS 1 batch_size = 3",
        ),
        (
            "admin alter ddl jobs 1 max_write_speed = 4",
            "ADMIN ALTER DDL JOBS 1 max_write_speed = 4",
        ),
        (
            "admin alter ddl jobs 1 max_write_speed = _UTF8MB4'4MiB'",
            "ADMIN ALTER DDL JOBS 1 max_write_speed = _UTF8MB4'4MiB'",
        ),
    ] {
        assert_eq!(r(sql), restored, "{sql}");
    }
    assert_eq!(
        r("admin alter ddl jobs 10 thread = 3, batch_size = 100, max_write_speed = '10MiB'"),
        "ADMIN ALTER DDL JOBS 10 thread = 3, batch_size = 100, max_write_speed = _UTF8MB4'10MiB'"
    );
}

#[test]
fn admin_alter_ddl_jobs_keeps_option_order_and_lowercases_names() {
    let tidb_ast::Stmt::Admin(admin) =
        parse("ADMIN ALTER DDL JOBS 10 THREAD := -3, BATCH_SIZE = +100").expect("parse")
    else {
        panic!("expected ADMIN statement");
    };
    let tidb_ast::AdminStmt::AlterDdlJobs(alter) = admin.as_ref() else {
        panic!("expected typed ADMIN ALTER DDL JOBS");
    };
    assert_eq!(alter.job_number, 10);
    assert_eq!(
        alter
            .options
            .iter()
            .map(|option| option.name.as_str())
            .collect::<Vec<_>>(),
        ["thread", "batch_size"]
    );
    assert_eq!(alter.options.len(), 2);
    assert_eq!(
        r("ADMIN ALTER DDL JOBS 10 THREAD := -3, BATCH_SIZE = +100"),
        "ADMIN ALTER DDL JOBS 10 thread = -3, batch_size = +100"
    );
}

#[test]
fn admin_alter_ddl_jobs_rejects_missing_literals() {
    for sql in [
        "admin alter ddl jobs 1 thread",
        "admin alter ddl jobs 1 thread =",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}
