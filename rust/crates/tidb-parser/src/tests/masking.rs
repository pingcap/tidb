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

//! Assertions translated from the masking-policy rows in Go's `TestDDL`.

use crate::{parse, tests::r};
use tidb_ast::{
    AlterMaskingPolicyAction, AlterTableAction, DdlStmt, MaskingPolicyRestrictOps,
    MaskingPolicyState, Stmt,
};

fn assert_parser_case(sql: &str, expected: Option<&str>) {
    match expected {
        Some(expected) => assert_eq!(r(sql), expected, "source SQL: {sql}"),
        None => assert!(parse(sql).is_err(), "source SQL unexpectedly parsed: {sql}"),
    }
}

#[test]
fn go_parser_test_masking_policy_cases() {
    let cases = [
        (
            "create masking policy p on t(c) as c",
            Some("CREATE MASKING POLICY `p` ON `t` (`c`) AS `c`"),
        ),
        (
            "create masking policy p on t(c) as c enable",
            Some("CREATE MASKING POLICY `p` ON `t` (`c`) AS `c` ENABLE"),
        ),
        (
            "create masking policy if not exists p on t(c) as c disable",
            Some("CREATE MASKING POLICY IF NOT EXISTS `p` ON `t` (`c`) AS `c` DISABLE"),
        ),
        (
            "create or replace masking policy p on t(c) as c",
            Some("CREATE OR REPLACE MASKING POLICY `p` ON `t` (`c`) AS `c`"),
        ),
        (
            "create masking policy p on t(c) as c restrict on none",
            Some("CREATE MASKING POLICY `p` ON `t` (`c`) AS `c`"),
        ),
        (
            "create or replace masking policy if not exists p on t(c) as c",
            None,
        ),
        (
            "create masking policy p on t(c) as case when current_user() = 'root' then c else 'xxx' end enable",
            Some("CREATE MASKING POLICY `p` ON `t` (`c`) AS CASE WHEN CURRENT_USER()=_UTF8MB4'root' THEN `c` ELSE _UTF8MB4'xxx' END ENABLE"),
        ),
        (
            "create masking policy p on t(c) as c restrict on (insert_into_select, delete_select) enable",
            Some("CREATE MASKING POLICY `p` ON `t` (`c`) AS `c` RESTRICT ON (INSERT_INTO_SELECT, DELETE_SELECT) ENABLE"),
        ),
        (
            "create masking policy p on t(c) as case when current_user() not in ('root@%', 'u@%') then 'x' else c end",
            Some("CREATE MASKING POLICY `p` ON `t` (`c`) AS CASE WHEN CURRENT_USER() NOT IN (_UTF8MB4'root@%',_UTF8MB4'u@%') THEN _UTF8MB4'x' ELSE `c` END"),
        ),
        (
            "alter table t add masking policy p on (c) as c",
            Some("ALTER TABLE `t` ADD MASKING POLICY `p` ON (`c`) AS `c`"),
        ),
        (
            "alter table t add masking policy p on (c) as c restrict on none",
            Some("ALTER TABLE `t` ADD MASKING POLICY `p` ON (`c`) AS `c`"),
        ),
        (
            "alter table t add masking policy p on (c) as c disable",
            Some("ALTER TABLE `t` ADD MASKING POLICY `p` ON (`c`) AS `c` DISABLE"),
        ),
        (
            "alter table t add masking policy p on (c) as c restrict on (update_select, ctas) disable",
            Some("ALTER TABLE `t` ADD MASKING POLICY `p` ON (`c`) AS `c` RESTRICT ON (UPDATE_SELECT, CTAS) DISABLE"),
        ),
        (
            "alter table t modify masking policy p set expression = case when current_role() in ('r1', 'r2') then c else 'x' end",
            Some("ALTER TABLE `t` MODIFY MASKING POLICY `p` SET EXPRESSION = CASE WHEN CURRENT_ROLE() IN (_UTF8MB4'r1',_UTF8MB4'r2') THEN `c` ELSE _UTF8MB4'x' END"),
        ),
        (
            "alter table t modify masking policy p set expression case when current_role() in ('r1', 'r2') then c else 'x' end",
            None,
        ),
        (
            "alter table t modify masking policy p set restrict on (insert_into_select, update_select, delete_select, ctas)",
            Some("ALTER TABLE `t` MODIFY MASKING POLICY `p` SET RESTRICT ON (INSERT_INTO_SELECT, UPDATE_SELECT, DELETE_SELECT, CTAS)"),
        ),
        (
            "alter table t modify masking policy p set restrict on none",
            Some("ALTER TABLE `t` MODIFY MASKING POLICY `p` SET RESTRICT ON NONE"),
        ),
        (
            "alter table t enable masking policy p",
            Some("ALTER TABLE `t` ENABLE MASKING POLICY `p`"),
        ),
        (
            "alter table t disable masking policy p",
            Some("ALTER TABLE `t` DISABLE MASKING POLICY `p`"),
        ),
        (
            "alter table t drop masking policy p",
            Some("ALTER TABLE `t` DROP MASKING POLICY `p`"),
        ),
    ];

    for (sql, expected) in cases {
        assert_parser_case(sql, expected);
    }
}

#[test]
fn masking_policy_payload_is_typed_end_to_end() {
    let Stmt::Ddl(ddl) = parse(
        "create masking policy p on db.t(c) as c restrict on (delete_select, insert_into_select) disable",
    )
    .expect("CREATE MASKING POLICY parses")
    else {
        panic!("expected DDL statement");
    };
    let DdlStmt::CreateMaskingPolicy(create) = ddl.as_ref() else {
        panic!("expected typed CREATE MASKING POLICY payload");
    };
    let mut expected_ops = MaskingPolicyRestrictOps::default();
    assert!(expected_ops.insert_name("DELETE_SELECT"));
    assert!(expected_ops.insert_name("INSERT_INTO_SELECT"));
    assert_eq!(create.name, "p");
    assert_eq!(create.table, ["db", "t"]);
    assert_eq!(create.column, "c");
    assert_eq!(create.restrict_ops, expected_ops);
    assert_eq!(create.state, MaskingPolicyState::Disabled);

    let Stmt::Ddl(ddl) =
        parse("alter table t modify masking policy p set restrict on (ctas, ctas, update_select)")
            .expect("ALTER TABLE MODIFY MASKING POLICY parses")
    else {
        panic!("expected DDL statement");
    };
    let DdlStmt::AlterTable(table) = ddl.as_ref() else {
        panic!("expected ALTER TABLE envelope");
    };
    let [AlterTableAction::MaskingPolicy(action)] = table.actions.as_slice() else {
        panic!("expected one typed masking-policy action");
    };
    assert!(matches!(
        action.as_ref(),
        AlterMaskingPolicyAction::ModifyRestrict { name, .. } if name == "p"
    ));
    assert_eq!(
        r("alter table t modify masking policy p set restrict on (ctas, ctas, update_select)"),
        "ALTER TABLE `t` MODIFY MASKING POLICY `p` SET RESTRICT ON (UPDATE_SELECT, CTAS)"
    );
}

#[test]
fn masking_policy_source_rejects_unknown_restrict_operations() {
    for sql in [
        "create masking policy p on t(c) as c restrict on (select)",
        "alter table t add masking policy p on (c) as c restrict on (insert_into_select, merge)",
        "alter table t modify masking policy p set restrict on ()",
    ] {
        assert!(parse(sql).is_err(), "source SQL unexpectedly parsed: {sql}");
    }
}

#[test]
fn masking_policy_ident_like_slots_follow_go_source() {
    assert_eq!(
        r("create masking policy 'p' on 'db'.'t'('c') as c restrict on ('ctas')"),
        "CREATE MASKING POLICY `p` ON `db`.`t` (`c`) AS `c` RESTRICT ON (CTAS)"
    );
    assert_eq!(
        r("alter table t modify masking policy 'p' set 'expression' = c"),
        "ALTER TABLE `t` MODIFY MASKING POLICY `p` SET EXPRESSION = `c`"
    );
}
