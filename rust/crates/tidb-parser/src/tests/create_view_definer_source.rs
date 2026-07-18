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

//! Source-backed `CREATE DEFINER ... VIEW` set-operation forms.
//!
//! These rows are contiguous planner casetest inputs accepted by Go's view
//! parser. The important boundary is not view execution (which remains
//! unsupported), but preserving parentheses on each set-operation term while
//! restoring Go's default algorithm and SQL-security fields.

use super::*;

#[test]
fn go_create_definer_view_parenthesized_union_rows_restore() {
    for (sql, expected) in [
        (
            "create definer=`root`@`127.0.0.1` view v1 as (select a from t1) union (select a from t2)",
            "CREATE ALGORITHM = UNDEFINED DEFINER = `root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v1` AS (SELECT `a` FROM `t1`) UNION (SELECT `a` FROM `t2`)",
        ),
        (
            "create definer=`root`@`127.0.0.1` view v2 as (select a from t3) union (select a from t4)",
            "CREATE ALGORITHM = UNDEFINED DEFINER = `root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v2` AS (SELECT `a` FROM `t3`) UNION (SELECT `a` FROM `t4`)",
        ),
        (
            "create definer=`root`@`127.0.0.1` view v3 as (select a from t5) union (select a from t6)",
            "CREATE ALGORITHM = UNDEFINED DEFINER = `root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v3` AS (SELECT `a` FROM `t5`) UNION (SELECT `a` FROM `t6`)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}
