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

#![cfg(test)]

use crate::Session;

/// Source: `pkg/planner/core/tests/rewriter.TestVariableRewritter`.
#[test]
fn variable_rewriter_validates_scope_and_hides_internal_session_variables() {
    let mut session = Session::new();
    for (sql, code, message) in [
        (
            "SELECT @@session.ddl_slow_threshold",
            1238,
            "Variable 'ddl_slow_threshold' is a GLOBAL variable",
        ),
        (
            "SELECT @@global.warning_count",
            1238,
            "Variable 'warning_count' is a SESSION variable",
        ),
        (
            "SELECT @@instance.tidb_redact_log",
            1238,
            "Variable 'tidb_redact_log' is a SESSION or GLOBAL variable",
        ),
        (
            "SELECT @@session.tidb_redact_log",
            1193,
            "Unknown system variable 'tidb_redact_log'",
        ),
        (
            "SELECT COALESCE(@@session.ddl_slow_threshold, 0)",
            1238,
            "Variable 'ddl_slow_threshold' is a GLOBAL variable",
        ),
        (
            "SELECT 1 UNION SELECT @@session.ddl_slow_threshold",
            1238,
            "Variable 'ddl_slow_threshold' is a GLOBAL variable",
        ),
        (
            "SELECT @@instance.last_insert_id",
            1238,
            "Variable 'last_insert_id' is a SESSION or GLOBAL variable",
        ),
        (
            "SELECT @@instance.identity",
            1238,
            "Variable 'identity' is a SESSION or GLOBAL variable",
        ),
        (
            "SELECT @@instance.last_plan_from_cache",
            1238,
            "Variable 'last_plan_from_cache' is a SESSION or GLOBAL variable",
        ),
        (
            "SELECT @@instance.last_plan_from_binding",
            1238,
            "Variable 'last_plan_from_binding' is a SESSION or GLOBAL variable",
        ),
    ] {
        let error = session.run(sql).unwrap_err();
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, code, "{sql}: {mysql:?}");
        assert_eq!(mysql.message, message, "{sql}");
    }

    session
        .run("SELECT @@tidb_redact_log")
        .expect("the internal variable remains available without explicit scope");
    session
        .run("SELECT @@global.version")
        .expect("a no-scope property ignores an explicit read scope");
}
