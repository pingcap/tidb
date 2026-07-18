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

//! Source-backed `CREATE GLOBAL BINDING FOR WITH ... UPDATE` vectors from
//! `tests/integrationtest/t/bindinfo/temptable.test:51,67`.

use super::*;

const TMP1: &str = "create global binding for with cte1 as (select a from tmp1) update t1 set t1.a=1 where t1.a in (select a from cte1) using with cte1 as (select a from tmp1) update t1 set t1.a=1 where t1.a in (select a from cte1)";
const TMP2: &str = "create global binding for with cte1 as (select a from tmp2) update t1 set t1.a=1 where t1.a in (select a from cte1) using with cte1 as (select a from tmp2) update t1 set t1.a=1 where t1.a in (select a from cte1)";
const TMP1_RESTORED: &str = "CREATE GLOBAL BINDING FOR WITH `cte1` AS (SELECT `a` FROM `tmp1`) UPDATE `t1` SET `t1`.`a`=1 WHERE `t1`.`a` IN (SELECT `a` FROM `cte1`) USING WITH `cte1` AS (SELECT `a` FROM `tmp1`) UPDATE `t1` SET `t1`.`a`=1 WHERE `t1`.`a` IN (SELECT `a` FROM `cte1`)";
const TMP2_RESTORED: &str = "CREATE GLOBAL BINDING FOR WITH `cte1` AS (SELECT `a` FROM `tmp2`) UPDATE `t1` SET `t1`.`a`=1 WHERE `t1`.`a` IN (SELECT `a` FROM `cte1`) USING WITH `cte1` AS (SELECT `a` FROM `tmp2`) UPDATE `t1` SET `t1`.`a`=1 WHERE `t1`.`a` IN (SELECT `a` FROM `cte1`)";

#[test]
fn create_global_binding_with_dml_restore_matches_go_source_rows() {
    for (sql, want) in [(TMP1, TMP1_RESTORED), (TMP2, TMP2_RESTORED)] {
        assert_eq!(r(sql), want);
    }
}

#[test]
fn create_global_binding_with_dml_is_typed_and_bindable() {
    let statement = parse(TMP1).expect("CREATE GLOBAL BINDING with CTE UPDATE parses");
    let Stmt::Admin(admin) = statement else {
        panic!("expected Admin statement");
    };
    let tidb_ast::AdminStmt::CreateBinding(binding) = admin.as_ref() else {
        panic!("expected CREATE BINDING");
    };
    let tidb_ast::CreateBindingSource::Statement { target } = &binding.source else {
        panic!("expected statement binding source");
    };
    for target in [
        &target.origin,
        target.hinted.as_ref().expect("hinted target"),
    ] {
        let Stmt::Dml(dml) = target.as_ref() else {
            panic!("expected CTE-prefixed DML target");
        };
        let tidb_ast::DmlStmt::With { statement, .. } = dml.as_ref() else {
            panic!("expected DmlStmt::With target");
        };
        assert!(matches!(statement.as_ref(), tidb_ast::DmlStmt::Update(_)));
    }
}
