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

//! Direct source ports for Go's `parseAssignment` bare-`DEFAULT` branch in
//! joined UPDATE statements.  The same production is used for single-table,
//! join, and derived-table UPDATE forms; executor support decides separately
//! whether a parsed shape can mutate rows.

use super::*;

#[test]
fn joined_update_default_assignments_restore_like_go() {
    for (sql, expected) in [
        (
            "update planner__core__integration.t, (select 1 as b) as t set planner__core__integration.t.a=default",
            "UPDATE (`planner__core__integration`.`t`) JOIN (SELECT 1 AS `b`) AS `t` SET `planner__core__integration`.`t`.`a`=DEFAULT",
        ),
        (
            "update tt as s1, tt as s2 set s1.z = default, s2.z = 456",
            "UPDATE (`tt` AS `s1`) JOIN `tt` AS `s2` SET `s1`.`z`=DEFAULT, `s2`.`z`=456",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn joined_update_default_is_a_zero_argument_placeholder() {
    let Stmt::Dml(dml) = parse("UPDATE tt AS s1, tt AS s2 SET s1.z = DEFAULT, s2.z = 456")
        .expect("joined UPDATE with DEFAULT parses")
    else {
        panic!("expected DML envelope");
    };
    let tidb_ast::DmlStmt::Update(update) = *dml else {
        panic!("expected UPDATE statement");
    };
    assert!(matches!(
        update.assignments[0].value,
        Expr::Func { ref name, ref args } if name == "DEFAULT" && args.is_empty()
    ));
}
