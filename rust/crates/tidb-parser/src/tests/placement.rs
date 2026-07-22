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

//! Exact ports of every standalone placement-policy row in Go's `TestDDL`
//! and the default-restore half of its three placement-policy AST tests.

use super::*;

fn assert_parser_case(sql: &str, expected: Option<&str>) {
    match expected {
        Some(expected) => assert_eq!(r(sql), expected, "source SQL: {sql}"),
        None => assert!(parse(sql).is_err(), "source SQL unexpectedly parsed: {sql}"),
    }
}

fn restore_with_mode(sql: &str, mode: tidb_ast::PlacementRestoreMode) -> String {
    let Stmt::Ddl(statement) = parse(sql).expect("parse placement policy statement") else {
        panic!("expected DDL envelope for {sql}")
    };
    match statement.as_ref() {
        tidb_ast::DdlStmt::CreatePlacementPolicy(statement) => statement.restore_with_mode(mode),
        tidb_ast::DdlStmt::AlterPlacementPolicy(statement) => statement.restore_with_mode(mode),
        tidb_ast::DdlStmt::DropPlacementPolicy(statement) => statement.restore_with_mode(mode),
        other => panic!("unexpected placement payload for {sql}: {other:?}"),
    }
}

#[test]
fn go_parser_test_placement_policy_cases() {
    let cases = [
        (
            "drop placement policy x",
            Some("DROP PLACEMENT POLICY `x`"),
        ),
        ("drop placement policy x, y", None),
        (
            "drop placement policy if exists x",
            Some("DROP PLACEMENT POLICY IF EXISTS `x`"),
        ),
        ("drop placement policy if exists x, y", None),
        (
            "create placement policy x primary_region='us'",
            Some("CREATE PLACEMENT POLICY `x` PRIMARY_REGION = 'us'"),
        ),
        ("create placement policy x region='us, 3'", None),
        (
            "create placement policy x followers=3",
            Some("CREATE PLACEMENT POLICY `x` FOLLOWERS = 3"),
        ),
        ("create placement policy x followers=0", None),
        (
            "create placement policy x voters=3",
            Some("CREATE PLACEMENT POLICY `x` VOTERS = 3"),
        ),
        (
            "create placement policy x learners=3",
            Some("CREATE PLACEMENT POLICY `x` LEARNERS = 3"),
        ),
        (
            "create placement policy x schedule='even'",
            Some("CREATE PLACEMENT POLICY `x` SCHEDULE = 'even'"),
        ),
        (
            "create placement policy x constraints='ww'",
            Some("CREATE PLACEMENT POLICY `x` CONSTRAINTS = 'ww'"),
        ),
        (
            "create placement policy x leader_constraints='ww'",
            Some("CREATE PLACEMENT POLICY `x` LEADER_CONSTRAINTS = 'ww'"),
        ),
        (
            "create placement policy x follower_constraints='ww'",
            Some("CREATE PLACEMENT POLICY `x` FOLLOWER_CONSTRAINTS = 'ww'"),
        ),
        (
            "create placement policy x voter_constraints='ww'",
            Some("CREATE PLACEMENT POLICY `x` VOTER_CONSTRAINTS = 'ww'"),
        ),
        (
            "create placement policy x learner_constraints='ww'",
            Some("CREATE PLACEMENT POLICY `x` LEARNER_CONSTRAINTS = 'ww'"),
        ),
        (
            "create placement policy x primary_region='cn' regions='us' schedule='even'",
            Some(
                "CREATE PLACEMENT POLICY `x` PRIMARY_REGION = 'cn' REGIONS = 'us' SCHEDULE = 'even'",
            ),
        ),
        (
            "create placement policy x primary_region='cn', leader_constraints='ww', leader_constraints='yy'",
            Some(
                "CREATE PLACEMENT POLICY `x` PRIMARY_REGION = 'cn' LEADER_CONSTRAINTS = 'ww' LEADER_CONSTRAINTS = 'yy'",
            ),
        ),
        (
            "create placement policy if not exists x regions = 'us', follower_constraints='yy'",
            Some(
                "CREATE PLACEMENT POLICY IF NOT EXISTS `x` REGIONS = 'us' FOLLOWER_CONSTRAINTS = 'yy'",
            ),
        ),
        (
            "create or replace placement policy x regions='us'",
            Some("CREATE OR REPLACE PLACEMENT POLICY `x` REGIONS = 'us'"),
        ),
        ("create placement policy x placement policy y", None),
        (
            "alter placement policy x primary_region='us'",
            Some("ALTER PLACEMENT POLICY `x` PRIMARY_REGION = 'us'"),
        ),
        ("alter placement policy x region='us, 3'", None),
        (
            "alter placement policy x followers=3",
            Some("ALTER PLACEMENT POLICY `x` FOLLOWERS = 3"),
        ),
        (
            "alter placement policy x voters=3",
            Some("ALTER PLACEMENT POLICY `x` VOTERS = 3"),
        ),
        (
            "alter placement policy x learners=3",
            Some("ALTER PLACEMENT POLICY `x` LEARNERS = 3"),
        ),
        (
            "alter placement policy x schedule='even'",
            Some("ALTER PLACEMENT POLICY `x` SCHEDULE = 'even'"),
        ),
        (
            "alter placement policy x constraints='ww'",
            Some("ALTER PLACEMENT POLICY `x` CONSTRAINTS = 'ww'"),
        ),
        (
            "alter placement policy x leader_constraints='ww'",
            Some("ALTER PLACEMENT POLICY `x` LEADER_CONSTRAINTS = 'ww'"),
        ),
        (
            "alter placement policy x follower_constraints='ww'",
            Some("ALTER PLACEMENT POLICY `x` FOLLOWER_CONSTRAINTS = 'ww'"),
        ),
        (
            "alter placement policy x voter_constraints='ww'",
            Some("ALTER PLACEMENT POLICY `x` VOTER_CONSTRAINTS = 'ww'"),
        ),
        (
            "alter placement policy x learner_constraints='ww'",
            Some("ALTER PLACEMENT POLICY `x` LEARNER_CONSTRAINTS = 'ww'"),
        ),
        (
            "alter placement policy x primary_region='cn' regions='us' schedule='even'",
            Some(
                "ALTER PLACEMENT POLICY `x` PRIMARY_REGION = 'cn' REGIONS = 'us' SCHEDULE = 'even'",
            ),
        ),
        (
            "alter placement policy x primary_region='cn', leader_constraints='ww', leader_constraints='yy'",
            Some(
                "ALTER PLACEMENT POLICY `x` PRIMARY_REGION = 'cn' LEADER_CONSTRAINTS = 'ww' LEADER_CONSTRAINTS = 'yy'",
            ),
        ),
        (
            "alter placement policy if exists x regions = 'us', follower_constraints='yy'",
            Some(
                "ALTER PLACEMENT POLICY IF EXISTS `x` REGIONS = 'us' FOLLOWER_CONSTRAINTS = 'yy'",
            ),
        ),
        ("alter placement policy x placement policy y", None),
    ];
    assert_eq!(cases.len(), 36);
    for (sql, expected) in cases {
        assert_parser_case(sql, expected);
    }
}

#[test]
fn go_ast_placement_policy_default_restore_cases() {
    let cases = [
        (
            "create placement policy p1 primary_region=\"r1\" regions='r1,r2' followers=1",
            "CREATE PLACEMENT POLICY `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1",
        ),
        (
            "create placement policy if not exists p1 primary_region=\"r1\" regions='r1,r2' followers=1",
            "CREATE PLACEMENT POLICY IF NOT EXISTS `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1",
        ),
        (
            "create or replace placement policy p1 followers=1",
            "CREATE OR REPLACE PLACEMENT POLICY `p1` FOLLOWERS = 1",
        ),
        (
            "alter placement policy p1 primary_region=\"r1\" regions='r1,r2' followers=1",
            "ALTER PLACEMENT POLICY `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1",
        ),
        (
            "drop placement policy p1",
            "DROP PLACEMENT POLICY `p1`",
        ),
        (
            "drop placement policy if exists p1",
            "DROP PLACEMENT POLICY IF EXISTS `p1`",
        ),
    ];
    assert_eq!(cases.len(), 6);
    for (sql, expected) in cases {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn go_ast_placement_policy_special_comment_restore_cases() {
    use tidb_ast::PlacementRestoreMode::SpecialComment;

    let cases = [
        (
            "create placement policy p1 primary_region=\"r1\" regions='r1,r2' followers=1",
            "/*T![placement] CREATE PLACEMENT POLICY `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1 */",
        ),
        (
            "create placement policy if not exists p1 primary_region=\"r1\" regions='r1,r2' followers=1",
            "/*T![placement] CREATE PLACEMENT POLICY IF NOT EXISTS `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1 */",
        ),
        (
            "create or replace placement policy p1 followers=1",
            "/*T![placement] CREATE OR REPLACE PLACEMENT POLICY `p1` FOLLOWERS = 1 */",
        ),
        (
            "alter placement policy p1 primary_region=\"r1\" regions='r1,r2' followers=1",
            "/*T![placement] ALTER PLACEMENT POLICY `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1 */",
        ),
        (
            "drop placement policy p1",
            "/*T![placement] DROP PLACEMENT POLICY `p1` */",
        ),
        (
            "drop placement policy if exists p1",
            "/*T![placement] DROP PLACEMENT POLICY IF EXISTS `p1` */",
        ),
    ];
    assert_eq!(cases.len(), 6);
    for (sql, expected) in cases {
        assert_eq!(restore_with_mode(sql, SpecialComment), expected);
    }
}

#[test]
fn test_create_placement_policy_restore() {
    for (sql, normal, special) in [
        (
            "create placement policy p1 primary_region=\"r1\" regions='r1,r2' followers=1",
            "CREATE PLACEMENT POLICY `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1",
            "/*T![placement] CREATE PLACEMENT POLICY `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1 */",
        ),
        (
            "create placement policy if not exists p1 primary_region=\"r1\" regions='r1,r2' followers=1",
            "CREATE PLACEMENT POLICY IF NOT EXISTS `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1",
            "/*T![placement] CREATE PLACEMENT POLICY IF NOT EXISTS `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1 */",
        ),
        (
            "create or replace placement policy p1 followers=1",
            "CREATE OR REPLACE PLACEMENT POLICY `p1` FOLLOWERS = 1",
            "/*T![placement] CREATE OR REPLACE PLACEMENT POLICY `p1` FOLLOWERS = 1 */",
        ),
    ] {
        assert_eq!(r(sql), normal);
        assert_eq!(
            restore_with_mode(sql, tidb_ast::PlacementRestoreMode::SpecialComment),
            special
        );
    }
}

#[test]
fn test_alter_placement_policy_restore() {
    let sql = "alter placement policy p1 primary_region=\"r1\" regions='r1,r2' followers=1";
    assert_eq!(
        r(sql),
        "ALTER PLACEMENT POLICY `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1"
    );
    assert_eq!(
        restore_with_mode(sql, tidb_ast::PlacementRestoreMode::SpecialComment),
        "/*T![placement] ALTER PLACEMENT POLICY `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1 */"
    );
}

#[test]
fn test_drop_placement_policy_restore() {
    for (sql, normal, special) in [
        (
            "drop placement policy p1",
            "DROP PLACEMENT POLICY `p1`",
            "/*T![placement] DROP PLACEMENT POLICY `p1` */",
        ),
        (
            "drop placement policy if exists p1",
            "DROP PLACEMENT POLICY IF EXISTS `p1`",
            "/*T![placement] DROP PLACEMENT POLICY IF EXISTS `p1` */",
        ),
    ] {
        assert_eq!(r(sql), normal);
        assert_eq!(
            restore_with_mode(sql, tidb_ast::PlacementRestoreMode::SpecialComment),
            special
        );
    }
}

#[test]
fn test_remove_placement_restore() {
    let flags =
        tidb_ast::RestoreFlags::DEFAULT | tidb_ast::RestoreFlags::SKIP_PLACEMENT_RULE_FOR_RESTORE;
    for (sql, expected) in [
        (
            "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY auto_increment, b varchar(255)) PLACEMENT POLICY=placement1",
            "CREATE TABLE `t1` (`id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,`b` VARCHAR(255)) ",
        ),
        (
            "CREATE TABLE t4 (firstname VARCHAR(25) NOT NULL,lastname VARCHAR(25) NOT NULL,username VARCHAR(16) NOT NULL,email VARCHAR(35),joined DATE NOT NULL) PARTITION BY RANGE(YEAR(joined)) (PARTITION p0 VALUES LESS THAN (1960) PLACEMENT POLICY=p1,PARTITION p1 VALUES LESS THAN (1970),PARTITION p2 VALUES LESS THAN (1980),PARTITION p3 VALUES LESS THAN (1990),PARTITION p4 VALUES LESS THAN MAXVALUE)",
            "CREATE TABLE `t4` (`firstname` VARCHAR(25) NOT NULL,`lastname` VARCHAR(25) NOT NULL,`username` VARCHAR(16) NOT NULL,`email` VARCHAR(35),`joined` DATE NOT NULL) PARTITION BY RANGE (YEAR(`joined`)) (PARTITION `p0` VALUES LESS THAN (1960) ,PARTITION `p1` VALUES LESS THAN (1970),PARTITION `p2` VALUES LESS THAN (1980),PARTITION `p3` VALUES LESS THAN (1990),PARTITION `p4` VALUES LESS THAN (MAXVALUE))",
        ),
        ("ALTER TABLE t3 PLACEMENT POLICY=DEFAULT", "ALTER TABLE `t3`"),
        ("ALTER TABLE t1 PLACEMENT POLICY=p10", "ALTER TABLE `t1`"),
        (
            "ALTER TABLE t1 PLACEMENT POLICY=p10, add d text(50)",
            "ALTER TABLE `t1` ADD COLUMN `d` TEXT(50)",
        ),
        ("alter table tp PARTITION p1 placement policy p2", ""),
        (
            "alter table t add d text(50) PARTITION p1 placement policy p2",
            "ALTER TABLE `t` ADD COLUMN `d` TEXT(50)",
        ),
        (
            "alter table tp set tiflash replica 1 PARTITION p1 placement policy p2",
            "ALTER TABLE `tp` SET TIFLASH REPLICA 1",
        ),
        (
            "ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY SET DEFAULT",
            "",
        ),
        (
            "ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY p1 charset utf8mb4",
            "ALTER DATABASE `TestResetPlacementDB`  CHARACTER SET = utf8mb4",
        ),
        (
            "ALTER PLACEMENT POLICY p3 PRIMARY_REGION='us-east-1' REGIONS='us-east-1,us-east-2,us-west-1'",
            "",
        ),
    ] {
        let restored = parse(sql)
            .unwrap_or_else(|error| panic!("parse placement removal row {sql}: {error:?}"))
            .restore_with_flags(flags);
        assert_eq!(restored, expected, "source SQL: {sql}");
    }
}

#[test]
fn source_zero_values_name_slots_and_list_boundaries_remain_visible() {
    assert_eq!(
        r("create placement policy p"),
        "CREATE PLACEMENT POLICY `p`"
    );
    assert_eq!(
        r("create placement policy 'policy one' voters=0 learners=0,"),
        "CREATE PLACEMENT POLICY `policy one` VOTERS = 0 LEARNERS = 0"
    );
    assert_eq!(
        r("drop placement policy `placement`"),
        "DROP PLACEMENT POLICY `placement`"
    );

    for sql in [
        "create placement policy p , followers=1",
        "create placement policy p followers=1,,",
        "create placement policy p followers",
        "create placement policy p primary_region",
        "create placement policy p followers=18446744073709551616",
        "alter placement policy p followers=0",
    ] {
        assert!(parse(sql).is_err(), "unexpectedly accepted: {sql}");
    }
}

#[test]
fn placement_policy_payload_is_typed_end_to_end() {
    let Stmt::Ddl(statement) = parse(
        "create or replace placement policy if not exists p primary_region='cn' followers=3 voters=2 learners=1 survival_preferences='zone'",
    )
    .expect("parse typed placement payload")
    else {
        panic!("expected DDL envelope")
    };
    let tidb_ast::DdlStmt::CreatePlacementPolicy(statement) = statement.as_ref() else {
        panic!("expected CREATE PLACEMENT POLICY payload")
    };
    assert!(statement.or_replace);
    assert!(statement.if_not_exists);
    assert_eq!(statement.name, "p");
    assert_eq!(
        statement.options,
        [
            tidb_ast::PlacementOption::PrimaryRegion("cn".to_string()),
            tidb_ast::PlacementOption::Followers(3),
            tidb_ast::PlacementOption::Voters(2),
            tidb_ast::PlacementOption::Learners(1),
            tidb_ast::PlacementOption::SurvivalPreferences("zone".to_string()),
        ]
    );
}
