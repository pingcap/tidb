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

//! Source rows for the complete `ADMIN FLUSH PLAN_CACHE` family from Go's
//! `TestAdminStmt`.

use super::*;

#[test]
fn admin_flush_plan_cache_source_rows_restore_like_go() {
    for (sql, expected) in [
        ("admin flush plan_cache", "ADMIN FLUSH SESSION PLAN_CACHE"),
        (
            "admin flush instance plan_cache",
            "ADMIN FLUSH INSTANCE PLAN_CACHE",
        ),
        (
            "admin flush session plan_cache",
            "ADMIN FLUSH SESSION PLAN_CACHE",
        ),
        (
            "admin flush global plan_cache",
            "ADMIN FLUSH GLOBAL PLAN_CACHE",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn admin_flush_plan_cache_retains_scope_as_typed_state() {
    for (sql, expected_scope) in [
        (
            "admin flush plan_cache",
            tidb_ast::AdminPlanCacheScope::Session,
        ),
        (
            "admin flush instance plan_cache",
            tidb_ast::AdminPlanCacheScope::Instance,
        ),
        (
            "admin flush session plan_cache",
            tidb_ast::AdminPlanCacheScope::Session,
        ),
        (
            "admin flush global plan_cache",
            tidb_ast::AdminPlanCacheScope::Global,
        ),
    ] {
        let tidb_ast::Stmt::Admin(admin) = parse(sql).expect("parse Go source form") else {
            panic!("expected administrative statement");
        };
        let tidb_ast::AdminStmt::FlushPlanCache(scope) = admin.as_ref() else {
            panic!("expected typed plan-cache flush");
        };
        assert_eq!(*scope, expected_scope, "source SQL: {sql}");
    }
}

#[test]
fn admin_flush_plan_cache_requires_its_scoped_plan_cache_shape() {
    for sql in [
        "admin flush session",
        "admin flush global",
        "admin flush instance",
        "admin flush session cache",
        "admin flush global cache",
    ] {
        assert!(parse(sql).is_err(), "source SQL: {sql}");
    }
}
