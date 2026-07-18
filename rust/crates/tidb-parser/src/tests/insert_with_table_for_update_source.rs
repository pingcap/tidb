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

//! Source-backed `INSERT ... WITH ... TABLE ... FOR UPDATE OF` coverage from
//! `tests/integrationtest/t/planner/core/issuetest/planner_issue.test:74`.

use super::*;

const SQL: &str = "INSERT INTO v0 WITH ta2 AS (TABLE v0) TABLE ta2 FOR UPDATE OF ta2";
const RESTORED: &str =
    "INSERT INTO `v0` WITH `ta2` AS (TABLE `v0`) TABLE `ta2` FOR UPDATE OF `ta2`";

#[test]
fn insert_with_table_for_update_restores_exactly() {
    assert_eq!(r(SQL), RESTORED);
}

#[test]
fn insert_with_table_for_update_keeps_typed_query_shape() {
    let statement = parse(SQL).expect("INSERT ... WITH ... TABLE parses");
    let Stmt::Dml(dml) = statement else {
        panic!("expected DML statement");
    };
    let tidb_ast::DmlStmt::Insert(insert) = dml.as_ref() else {
        panic!("expected INSERT statement");
    };
    let source = insert.source.as_ref().expect("INSERT query source");
    let tidb_ast::QueryStmt::Select(select) = source.as_ref() else {
        panic!("expected one-term TABLE query source");
    };
    assert_eq!(select.kind, tidb_ast::SelectStatementKind::Table);
    assert_eq!(select.with.as_ref().expect("WITH clause").ctes.len(), 1);
    assert!(matches!(
        select.lock,
        Some(tidb_ast::SelectLock {
            kind: tidb_ast::LockKind::Update,
            ref of,
            wait: tidb_ast::LockWait::Default,
        }) if of == &vec![vec!["ta2".to_owned()]]
    ));
}
