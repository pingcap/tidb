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
// See the License for the specific language governing permissions and
// limitations under the License.

//! `ALTER TABLE` / `RENAME TABLE` tests: the action families, option order,
//! partition maintenance, and the source-boundary rows. Split out of
//! `tests::ddl` for file size; every assertion is character-identical to the
//! original.

use super::*;

#[test]
fn alter_table_enable_disable_keys_match_go_owner_rows() {
    for (sql, restored, enabled) in [
        (
            "ALTER TABLE t ENABLE KEYS",
            "ALTER TABLE `t` ENABLE KEYS",
            true,
        ),
        (
            "ALTER TABLE t DISABLE KEYS",
            "ALTER TABLE `t` DISABLE KEYS",
            false,
        ),
    ] {
        let statement = parse(sql).expect("ALTER TABLE keys action parses");
        assert_eq!(statement.restore(), restored);
        let Stmt::Ddl(ddl) = statement else {
            panic!("expected DDL statement");
        };
        let tidb_ast::DdlStmt::AlterTable(alter) = ddl.into_inner() else {
            panic!("expected ALTER TABLE");
        };
        assert_eq!(
            alter.actions,
            vec![AlterTableAction::SetKeysEnabled(enabled)]
        );
    }

    assert_eq!(
        r("ALTER TABLE t ENABLE KEYS, COMMENT = 'cmt' PARTITION BY HASH(a)"),
        "ALTER TABLE `t` ENABLE KEYS, COMMENT = 'cmt' PARTITION BY HASH (`a`) PARTITIONS 1"
    );
    for sql in [
        "ALTER TABLE t ENABLE",
        "ALTER TABLE t DISABLE",
        "ALTER TABLE t ENABLE INDEX",
        "ALTER TABLE t DISABLE INDEX",
    ] {
        assert!(parse(sql).is_err(), "accepted invalid keys action: {sql}");
    }
}

#[test]
fn test_column_position_restore() {
    for (suffix, expected) in [
        ("", "ALTER TABLE `t` ADD COLUMN `a` VARCHAR(255)"),
        ("FIRST", "ALTER TABLE `t` ADD COLUMN `a` VARCHAR(255) FIRST"),
        (
            "AFTER b",
            "ALTER TABLE `t` ADD COLUMN `a` VARCHAR(255) AFTER `b`",
        ),
    ] {
        assert_eq!(
            r(&format!("ALTER TABLE t ADD COLUMN a VARCHAR(255) {suffix}")),
            expected
        );
    }
}

#[test]
fn test_alter_table_option_restore() {
    for (sql, expected) in [
        (
            "ALTER TABLE t ROW_FORMAT = COMPRESSED KEY_BLOCK_SIZE = 8",
            "ALTER TABLE `t` ROW_FORMAT = COMPRESSED KEY_BLOCK_SIZE = 8",
        ),
        (
            "ALTER TABLE t ROW_FORMAT = COMPRESSED, KEY_BLOCK_SIZE = 8",
            "ALTER TABLE `t` ROW_FORMAT = COMPRESSED, KEY_BLOCK_SIZE = 8",
        ),
    ] {
        assert_eq!(r(sql), expected);
    }
}

#[test]
fn alter_table_statement_options_transcreate_the_complete_go_action_family() {
    for (sql, expected) in [
        (
            "ALTER TABLE t ALGORITHM = DEFAULT",
            "ALTER TABLE `t` ALGORITHM = DEFAULT",
        ),
        (
            "ALTER TABLE t ALGORITHM COPY",
            "ALTER TABLE `t` ALGORITHM = COPY",
        ),
        (
            "ALTER TABLE t ALGORITHM = INPLACE",
            "ALTER TABLE `t` ALGORITHM = INPLACE",
        ),
        (
            "ALTER TABLE t ALGORITHM INSTANT",
            "ALTER TABLE `t` ALGORITHM = INSTANT",
        ),
        ("ALTER TABLE t READ ONLY", "ALTER TABLE `t` READ ONLY"),
        ("ALTER TABLE t READ WRITE", "ALTER TABLE `t` READ WRITE"),
        (
            "ALTER TABLE t FORCE",
            "ALTER TABLE `t` FORCE /* AlterTableForce is not supported */ ",
        ),
        (
            "ALTER TABLE d_n.t_n SECONDARY_LOAD",
            "ALTER TABLE `d_n`.`t_n` SECONDARY_LOAD",
        ),
        (
            "ALTER TABLE d_n.t_n SECONDARY_UNLOAD",
            "ALTER TABLE `d_n`.`t_n` SECONDARY_UNLOAD",
        ),
        (
            "ALTER TABLE t IMPORT TABLESPACE",
            "ALTER TABLE `t` IMPORT TABLESPACE",
        ),
        (
            "ALTER TABLE db.t DISCARD TABLESPACE",
            "ALTER TABLE `db`.`t` DISCARD TABLESPACE",
        ),
        (
            "ALTER TABLE t DISCARD TABLESPACE",
            "ALTER TABLE `t` DISCARD TABLESPACE",
        ),
        (
            "ALTER TABLE db.t IMPORT TABLESPACE",
            "ALTER TABLE `db`.`t` IMPORT TABLESPACE",
        ),
        (
            "ALTER TABLE t LOCK = DEFAULT, SECONDARY_LOAD",
            "ALTER TABLE `t` LOCK = DEFAULT, SECONDARY_LOAD",
        ),
        (
            "ALTER TABLE t ADD COLUMN c INT, ALGORITHM = INSTANT",
            "ALTER TABLE `t` ADD COLUMN `c` INT, ALGORITHM = INSTANT",
        ),
        (
            "ALTER TABLE d_n.t_n ALGORITHM = DEFAULT , MAX_ROWS 10, UNION ( d_n.t_n ) , ROW_FORMAT REDUNDANT, STATS_PERSISTENT = DEFAULT",
            "ALTER TABLE `d_n`.`t_n` ALGORITHM = DEFAULT, MAX_ROWS = 10, UNION = (`d_n`.`t_n`), ROW_FORMAT = REDUNDANT, STATS_PERSISTENT = DEFAULT /* TableOptionStatsPersistent is not supported */ ",
        ),
        (
            "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` mediumtext CHARACTER SET UTF8MB4 COLLATE UTF8MB4_UNICODE_CI NOT NULL , ALGORITHM = DEFAULT",
            "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL, ALGORITHM = DEFAULT",
        ),
        (
            "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` mediumtext CHARACTER SET UTF8MB4 COLLATE UTF8MB4_UNICODE_CI NOT NULL , ALGORITHM = INPLACE",
            "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL, ALGORITHM = INPLACE",
        ),
        (
            "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` mediumtext CHARACTER SET UTF8MB4 COLLATE UTF8MB4_UNICODE_CI NOT NULL , ALGORITHM = COPY",
            "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL, ALGORITHM = COPY",
        ),
        (
            "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE UTF8MB4_UNICODE_CI NOT NULL, ALGORITHM = INSTANT",
            "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL, ALGORITHM = INSTANT",
        ),
        (
            "ALTER TABLE t_n LOCK = DEFAULT , SECONDARY_LOAD",
            "ALTER TABLE `t_n` LOCK = DEFAULT, SECONDARY_LOAD",
        ),
        (
            "ALTER TABLE d_n.t_n ALGORITHM = DEFAULT , SECONDARY_LOAD",
            "ALTER TABLE `d_n`.`t_n` ALGORITHM = DEFAULT, SECONDARY_LOAD",
        ),
        (
            "ALTER TABLE d_n.t_n ALGORITHM = DEFAULT , SECONDARY_UNLOAD",
            "ALTER TABLE `d_n`.`t_n` ALGORITHM = DEFAULT, SECONDARY_UNLOAD",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }

    for sql in [
        "ALTER TABLE t ALGORITHM",
        "ALTER TABLE t ALGORITHM = ident",
        "ALTER TABLE t READ",
        "ALTER TABLE t READ ident",
        "ALTER TABLE t IMPORT",
        "ALTER TABLE t DISCARD ident",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }

    let statement = parse("ALTER TABLE t ALGORITHM=INPLACE").unwrap();
    let Stmt::Ddl(ddl) = statement else {
        panic!("expected DDL statement");
    };
    let DdlStmt::AlterTable(alter) = ddl.into_inner() else {
        panic!("expected ALTER TABLE statement");
    };
    assert_eq!(
        only_alter_action(&alter),
        AlterTableAction::Algorithm(tidb_ast::AlterTableAlgorithm::Inplace)
    );
}

#[test]
fn alter_table_extended_statistics_transcreate_all_go_ast_restore_rows() {
    for (sql, expected) in [
        (
            "ALTER TABLE t add stats_extended s1 cardinality(a,b)",
            "ALTER TABLE `t` ADD STATS_EXTENDED `s1` CARDINALITY(`a`, `b`)",
        ),
        (
            "ALTER TABLE t add stats_extended if not exists s1 cardinality(a,b)",
            "ALTER TABLE `t` ADD STATS_EXTENDED IF NOT EXISTS `s1` CARDINALITY(`a`, `b`)",
        ),
        (
            "ALTER TABLE t add stats_extended s1 correlation(a,b)",
            "ALTER TABLE `t` ADD STATS_EXTENDED `s1` CORRELATION(`a`, `b`)",
        ),
        (
            "ALTER TABLE t add stats_extended if not exists s1 correlation(a,b)",
            "ALTER TABLE `t` ADD STATS_EXTENDED IF NOT EXISTS `s1` CORRELATION(`a`, `b`)",
        ),
        (
            "ALTER TABLE t add stats_extended s1 dependency(a,b)",
            "ALTER TABLE `t` ADD STATS_EXTENDED `s1` DEPENDENCY(`a`, `b`)",
        ),
        (
            "ALTER TABLE t add stats_extended if not exists s1 dependency(a,b)",
            "ALTER TABLE `t` ADD STATS_EXTENDED IF NOT EXISTS `s1` DEPENDENCY(`a`, `b`)",
        ),
        (
            "ALTER TABLE t drop stats_extended s1",
            "ALTER TABLE `t` DROP STATS_EXTENDED `s1`",
        ),
        (
            "ALTER TABLE t drop stats_extended if exists s1",
            "ALTER TABLE `t` DROP STATS_EXTENDED IF EXISTS `s1`",
        ),
        (
            "ALTER TABLE t add stats_extended 's' cardinality ('a',@b)",
            "ALTER TABLE `t` ADD STATS_EXTENDED `s` CARDINALITY(`a`, `b`)",
        ),
        (
            "ALTER TABLE t drop stats_extended if exists 's'",
            "ALTER TABLE `t` DROP STATS_EXTENDED IF EXISTS `s`",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }

    for sql in [
        "ALTER TABLE t ADD STATS_EXTENDED s",
        "ALTER TABLE t ADD STATS_EXTENDED s CARDINALITY()",
        "ALTER TABLE t ADD STATS_EXTENDED s UNKNOWN(a,b)",
        "ALTER TABLE t DROP STATS_EXTENDED",
    ] {
        assert!(parse(sql).is_err(), "unexpectedly accepted: {sql}");
    }

    let statement = parse("ALTER TABLE t ADD STATS_EXTENDED s DEPENDENCY(a,b)").unwrap();
    let Stmt::Ddl(ddl) = statement else {
        panic!("expected DDL statement");
    };
    let DdlStmt::AlterTable(alter) = ddl.into_inner() else {
        panic!("expected ALTER TABLE statement");
    };
    assert_eq!(
        only_alter_action(&alter),
        AlterTableAction::AddStatistics {
            if_not_exists: false,
            name: "s".to_string(),
            stats_type: tidb_ast::ExtendedStatsType::Dependency,
            columns: vec!["a".to_string(), "b".to_string()],
        }
    );
}

#[test]
fn test_alter_table_with_special_comment_restore() {
    let flags = tidb_ast::RestoreFlags::DEFAULT | tidb_ast::RestoreFlags::TIDB_SPECIAL_COMMENT;
    for (sql, expected) in [
        (
            "ALTER TABLE t PLACEMENT POLICY p1",
            "ALTER TABLE `t` /*T![placement] PLACEMENT POLICY = `p1` */",
        ),
        (
            "ALTER TABLE t PLACEMENT POLICY p1 COMMENT='aaa'",
            "ALTER TABLE `t` /*T![placement] PLACEMENT POLICY = `p1` */ COMMENT = 'aaa'",
        ),
        (
            "ALTER TABLE t PARTITION p0 PLACEMENT POLICY p1",
            "ALTER TABLE `t` /*T![placement] PARTITION `p0` PLACEMENT POLICY = `p1` */",
        ),
    ] {
        assert_eq!(
            parse(sql).expect("parse").restore_with_flags(flags),
            expected
        );
    }
}

pub(super) fn only_alter_action(statement: &tidb_ast::AlterTableStmt) -> AlterTableAction {
    let [action] = statement.actions.as_slice() else {
        panic!(
            "expected one ALTER TABLE action, got {}",
            statement.actions.len()
        );
    };
    action.clone()
}

#[test]
fn alter_table() {
    // `ADD`/`DROP` alone (no `COLUMN`) restore identically to the
    // `COLUMN`-qualified form, matching the Go AST's normalization.
    assert_eq!(
        r("alter table t add column c int"),
        "ALTER TABLE `t` ADD COLUMN `c` INT"
    );
    assert_eq!(
        r("alter table t add c int"),
        "ALTER TABLE `t` ADD COLUMN `c` INT"
    );
    assert_eq!(
        r("alter table t add column c varchar(20) not null default 'x'"),
        "ALTER TABLE `t` ADD COLUMN `c` VARCHAR(20) NOT NULL DEFAULT _UTF8MB4'x'"
    );
    assert_eq!(
        r("alter table t add c int first"),
        "ALTER TABLE `t` ADD COLUMN `c` INT FIRST"
    );
    assert_eq!(
        r("alter table t add c int after b"),
        "ALTER TABLE `t` ADD COLUMN `c` INT AFTER `b`"
    );
    assert_eq!(
        r("alter table t drop column c"),
        "ALTER TABLE `t` DROP COLUMN `c`"
    );
    assert_eq!(r("alter table t drop c"), "ALTER TABLE `t` DROP COLUMN `c`");

    let stmt = parse("alter table t add c int after b").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    assert_eq!(
        only_alter_action(&alt),
        AlterTableAction::AddColumn {
            if_not_exists: false,
            column: ColumnDef {
                qualifier: vec![],
                name: "c".to_string(),
                ty: ColumnType {
                    name: "INT".to_string(),
                    args: vec![],
                    unsigned: false,
                    zerofill: false,
                    binary: false,
                    charset: None,
                },
                options: vec![],
            },
            position: ColumnPosition::After("b".to_string()),
        }
    );

    assert_eq!(
        r("alter table t add constraint fk1 foreign key (a) references u (id)"),
        "ALTER TABLE `t` ADD CONSTRAINT `fk1` FOREIGN KEY (`a`) REFERENCES `u`(`id`)"
    );
}

#[test]
fn alter_table_multi_specs_preserve_go_order_and_separators() {
    assert_eq!(
        r("alter table t add column b int, drop column a"),
        "ALTER TABLE `t` ADD COLUMN `b` INT, DROP COLUMN `a`"
    );
    let statement = ddl_payload!(
        parse("alter table t add column b int, drop column a").unwrap(),
        AlterTable
    );
    assert!(matches!(
        statement.actions.as_slice(),
        [
            AlterTableAction::AddColumn { column, .. },
            AlterTableAction::DropColumn { name, .. }
        ] if column.name == "b" && name == "a"
    ));

    // Direct Go parser source rows: specs retain source order and ordinary
    // specs restore with comma-space separators.
    assert_eq!(
        r("alter table t add column a smallint unsigned, add column b smallint"),
        "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, ADD COLUMN `b` SMALLINT"
    );
    assert_eq!(
        r("alter table t add constraint c_1 check (1+1) not enforced, add unique (a)"),
        "ALTER TABLE `t` ADD CONSTRAINT `c_1` CHECK(1+1) NOT ENFORCED, ADD UNIQUE(`a`)"
    );

    // Go's `parseIdentList` owns every comma after DROP PARTITION. It consumes
    // `ADD` as another partition name, then rejects the leftover COLUMN; this
    // is not a valid multi-spec boundary.
    assert!(parse("alter table t drop partition p0,p1, add column c int").is_err());
    // The label list owns every following comma, so Go tries to parse ADD as
    // another string label and rejects this as a multi-spec boundary.
    assert!(
        parse("alter table t set tiflash replica 2 location labels 'a','b', add column c int")
            .is_err()
    );

    // REMOVE PARTITIONING is Go's terminal AlterTablePartitionOpt: no comma
    // before it in either accepted input or canonical restore.
    assert_eq!(
        r("alter table t add column c int remove partitioning"),
        "ALTER TABLE `t` ADD COLUMN `c` INT REMOVE PARTITIONING"
    );
    assert!(parse("alter table t add column c int, remove partitioning").is_err());

    let empty = ddl_payload!(parse("alter table t").unwrap(), AlterTable);
    assert!(empty.actions.is_empty());
    assert_eq!(r("alter table t"), "ALTER TABLE `t`");
}

#[test]
fn alter_table_charset_collation_options_follow_go_option_order() {
    // These are direct source rows from
    // `tests/integrationtest/t/collation_misc.test`: Go's generic
    // `parseAlterTableOptions` consumes every adjacent option into ONE AST
    // spec and restores them in source order without commas.
    assert_eq!(
        r("alter table t1 collate uTf8mB4_uNiCoDe_Ci charset Utf8mB4 charset uTF8Mb4 collate UTF8MB4_BiN"),
        "ALTER TABLE `t1` DEFAULT COLLATE = UTF8MB4_UNICODE_CI DEFAULT CHARACTER SET = UTF8MB4 DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_BIN"
    );
    assert_eq!(
        r("alter table t char set utf8mb3 default collate utf8_bin"),
        "ALTER TABLE `t` CHARACTER SET UTF8 COLLATE UTF8_BIN"
    );
    assert_eq!(
        r("alter table d_n.t_n convert to char set default collate utf8mb4_0900_ai_ci"),
        "ALTER TABLE `d_n`.`t_n` CONVERT TO CHARACTER SET DEFAULT COLLATE UTF8MB4_0900_AI_CI"
    );
    assert!(parse("alter table t charset not_a_charset").is_err());

    let stmt = parse("alter table t collate utf8_bin charset utf8").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    assert_eq!(
        only_alter_action(&alt),
        AlterTableAction::SetTableOptions {
            options: vec![
                TableOption::Collate("UTF8_BIN".to_string()),
                TableOption::CharacterSet("UTF8".to_string()),
            ],
        }
    );
}

#[test]
fn alter_table_affinity_keeps_the_go_string_literal_boundary() {
    // Direct source cases from `pkg/parser/parser_test.go:TestTableAffinityOption`.
    // Restore preserves the decoded literal spelling; semantic validation of
    // `table`/`partition`/`none` happens later in real TiDB DDL.
    assert_eq!(
        r("alter table t affinity 'TABLE'"),
        "ALTER TABLE `t` AFFINITY = 'TABLE'"
    );
    assert_eq!(
        r("alter table t affinity='it\\'s'"),
        "ALTER TABLE `t` AFFINITY = 'it''s'"
    );
    let stmt = parse("alter table t affinity = 'partition'").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    assert_eq!(
        only_alter_action(&alt),
        AlterTableAction::SetTableOptions {
            options: vec![TableOption::Affinity("partition".to_string())],
        }
    );
    for sql in [
        "alter table t affinity 1",
        "alter table t affinity = 1",
        "alter table t affinity",
        "alter table t affinity = table",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}

#[test]
fn alter_table_add_check_constraint() {
    let stmt = parse("alter table t add constraint ck_a check (a > 0) not enforced").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    assert!(matches!(
        only_alter_action(&alt),
        AlterTableAction::AddCheck(tidb_ast::CheckConstraintDefinition {
            name: Some(ref name),
            enforced: false,
            ..
        }) if name == "ck_a"
    ));
    assert_eq!(
        r("alter table t add check (a > 0)"),
        "ALTER TABLE `t` ADD CHECK(`a`>0) ENFORCED"
    );
    assert_eq!(
        r("alter table t add constraint ck_a check (a > 0) not enforced"),
        "ALTER TABLE `t` ADD CONSTRAINT `ck_a` CHECK(`a`>0) NOT ENFORCED"
    );
    // `CONSTRAINT` without a name is normalized away, matching the Go AST.
    assert_eq!(
        r("alter table t add constraint check (true)"),
        "ALTER TABLE `t` ADD CHECK(TRUE) ENFORCED"
    );
}

#[test]
fn alter_table_add_partition_count() {
    let stmt = parse("alter table t add partition partitions 8").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    assert_eq!(
        only_alter_action(&alt),
        AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Add {
            if_not_exists: false,
            no_write_to_binlog: false,
            spec: tidb_ast::AddPartitionSpec::Count(8),
        })
    );
    assert_eq!(
        r("alter table t add partition partitions 8"),
        "ALTER TABLE `t` ADD PARTITION PARTITIONS 8"
    );
    let stmt = ddl_payload!(
        parse("alter table t add partition if not exists no_write_to_binlog (partition p0 values less than (10), partition p1 values in ((2, 'x'), default), partition pmax values less than maxvalue comment='tail')").unwrap(),
        AlterTable
    );
    assert!(matches!(
        only_alter_action(&stmt),
        AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Add {
            if_not_exists: true,
            no_write_to_binlog: true,
            spec: tidb_ast::AddPartitionSpec::Definitions(ref definitions),
        }) if definitions.len() == 3
    ));
    assert_eq!(
        r("alter table t add partition (partition p0 values less than (10), partition p1 values in ((2, 'x'), default), partition pmax values less than maxvalue comment='tail')"),
        "ALTER TABLE `t` ADD PARTITION (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES IN ((2, _UTF8MB4'x'), DEFAULT), PARTITION `pmax` VALUES LESS THAN (MAXVALUE) COMMENT = 'tail')"
    );
    assert!(
        parse("alter table t add partition (partition p0 values less than (default))").is_err()
    );
}

#[test]
fn alter_table_partition_maintenance_actions_are_typed_and_restore_like_go() {
    let stmt = ddl_payload!(
        parse("alter table t reorganize partition no_write_to_binlog p0, p1 into (partition p01 values less than (20), partition pmax values less than maxvalue)").unwrap(),
        AlterTable
    );
    assert!(matches!(
        only_alter_action(&stmt),
        AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Reorganize {
            no_write_to_binlog: true,
            ref names,
            ref definitions,
        }) if names == &["p0", "p1"] && definitions.len() == 2
    ));
    assert_eq!(
        r("alter table t reorganize partition no_write_to_binlog p0, p1 into (partition p01 values less than (20), partition pmax values less than maxvalue)"),
        "ALTER TABLE `t` REORGANIZE PARTITION NO_WRITE_TO_BINLOG `p0`,`p1` INTO (PARTITION `p01` VALUES LESS THAN (20), PARTITION `pmax` VALUES LESS THAN (MAXVALUE))"
    );
    assert_eq!(
        r("alter table t coalesce partition no_write_to_binlog 2"),
        "ALTER TABLE `t` COALESCE PARTITION NO_WRITE_TO_BINLOG 2"
    );
    assert_eq!(
        r("alter table t truncate partition p0, p1"),
        "ALTER TABLE `t` TRUNCATE PARTITION `p0`,`p1`"
    );
    assert_eq!(
        r("alter table t truncate partition all"),
        "ALTER TABLE `t` TRUNCATE PARTITION ALL"
    );
    assert_eq!(
        r("alter table t remove partitioning"),
        "ALTER TABLE `t` REMOVE PARTITIONING"
    );
    assert_eq!(
        r("alter table t optimize partition no_write_to_binlog p0,p1"),
        "ALTER TABLE `t` OPTIMIZE PARTITION NO_WRITE_TO_BINLOG `p0`,`p1`"
    );
    assert!(parse("alter table t coalesce partition p0").is_err());
    assert!(parse("alter table t reorganize partition p0").is_err());
    assert!(parse("alter table t truncate partition").is_err());
}

#[test]
fn alter_table_tiflash_replica_and_compact_are_typed_and_restore_like_go() {
    let stmt = ddl_payload!(
        parse("alter table t set hypo tiflash replica 2 location labels 'zone-a','zone-b'")
            .unwrap(),
        AlterTable
    );
    assert_eq!(
        only_alter_action(&stmt),
        AlterTableAction::SetTiFlashReplica {
            hypo: true,
            count: 2,
            labels: vec!["zone-a".to_owned(), "zone-b".to_owned()],
        }
    );
    // Go retains `Hypo` in its TiFlashReplicaSpec but omits it from restore.
    assert_eq!(
        r("alter table t set hypo tiflash replica 2 location labels 'zone-a','zone-b'"),
        "ALTER TABLE `t` SET TIFLASH REPLICA 2 LOCATION LABELS 'zone-a', 'zone-b'"
    );

    let stmt = ddl_payload!(
        parse("alter table db.t compact partition p1,p2 tiflash replica").unwrap(),
        AlterTable
    );
    assert_eq!(
        only_alter_action(&stmt),
        AlterTableAction::Compact {
            partitions: vec!["p1".to_owned(), "p2".to_owned()],
            replica_kind: CompactReplicaKind::TiFlash,
        }
    );
    assert_eq!(
        r("alter table db.t compact partition p1,p2 tiflash replica"),
        "ALTER TABLE `db`.`t` COMPACT PARTITION `p1`,`p2` TIFLASH REPLICA"
    );
    // The Go parser makes REPLICA optional after both engine spellings.
    assert_eq!(
        r("alter table t compact tikv"),
        "ALTER TABLE `t` COMPACT TIKV REPLICA"
    );
    assert_eq!(
        r("alter table t compact partition 'p'"),
        "ALTER TABLE `t` COMPACT PARTITION `p`"
    );
    assert!(parse("alter table t compact partition select").is_err());
}

#[test]
fn alter_table_exchange_partition() {
    assert_eq!(
        r("alter table db.pt exchange partition p0 with table archive"),
        "ALTER TABLE `db`.`pt` EXCHANGE PARTITION `p0` WITH TABLE `archive`"
    );
    // Go normalizes an explicit WITH VALIDATION to its default omitted form.
    assert_eq!(
        r("alter table pt exchange partition p0 with table db.archive with validation"),
        "ALTER TABLE `pt` EXCHANGE PARTITION `p0` WITH TABLE `db`.`archive`"
    );
    assert_eq!(
        r("alter table pt exchange partition p0 with table archive without validation"),
        "ALTER TABLE `pt` EXCHANGE PARTITION `p0` WITH TABLE `archive` WITHOUT VALIDATION"
    );
    assert_eq!(
        r("alter table pt exchange partition 'p' with table a.1"),
        "ALTER TABLE `pt` EXCHANGE PARTITION `p` WITH TABLE `a`.`1`"
    );

    let stmt = ddl_payload!(
        parse("alter table pt exchange partition p0 with table db.archive without validation")
            .unwrap(),
        AlterTable
    );
    assert_eq!(
        only_alter_action(&stmt),
        AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Exchange {
            partition: "p0".to_owned(),
            table: vec!["db".to_owned(), "archive".to_owned()],
            with_validation: false,
        })
    );

    // Each partition envelope remains one ordered spec; the outer statement
    // owns the comma between them.
    assert_eq!(
        r("alter table pt exchange partition p0 with table archive, drop partition p0"),
        "ALTER TABLE `pt` EXCHANGE PARTITION `p0` WITH TABLE `archive`, DROP PARTITION `p0`"
    );
}

#[test]
fn alter_table_drop_partition() {
    assert_eq!(
        r("alter table pt drop partition p0"),
        "ALTER TABLE `pt` DROP PARTITION `p0`"
    );
    assert_eq!(
        r("alter table pt drop partition p0, `p 1`"),
        "ALTER TABLE `pt` DROP PARTITION `p0`,`p 1`"
    );
    assert_eq!(
        r("alter table pt drop partition if exists p0"),
        "ALTER TABLE `pt` DROP PARTITION IF EXISTS `p0`"
    );
    assert_eq!(
        r("alter table pt drop partition 'p', @q"),
        "ALTER TABLE `pt` DROP PARTITION `p`,`q`"
    );

    let stmt = ddl_payload!(
        parse("alter table pt drop partition if exists p0, p1").unwrap(),
        AlterTable
    );
    assert_eq!(
        only_alter_action(&stmt),
        AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Drop {
            if_exists: true,
            names: vec!["p0".to_owned(), "p1".to_owned()],
        })
    );

    assert!(parse("alter table pt drop partition").is_err());
    assert!(parse("alter table pt drop partition p0, add column c int").is_err());
}

#[test]
fn alter_table_modify_change_column() {
    // `MODIFY`/`CHANGE` alone (no `COLUMN`) restore identically to the
    // `COLUMN`-qualified form.
    assert_eq!(
        r("alter table t modify column c bigint"),
        "ALTER TABLE `t` MODIFY COLUMN `c` BIGINT"
    );
    assert_eq!(
        r("alter table t modify c bigint"),
        "ALTER TABLE `t` MODIFY COLUMN `c` BIGINT"
    );
    assert_eq!(
        r("alter table t modify c int first"),
        "ALTER TABLE `t` MODIFY COLUMN `c` INT FIRST"
    );
    assert_eq!(
        r("alter table t modify c int after b"),
        "ALTER TABLE `t` MODIFY COLUMN `c` INT AFTER `b`"
    );
    // CHANGE COLUMN renames: the old name, then the new column def.
    assert_eq!(
        r("alter table t change b c int"),
        "ALTER TABLE `t` CHANGE COLUMN `b` `c` INT"
    );
    assert_eq!(
        r("alter table t change column b c int"),
        "ALTER TABLE `t` CHANGE COLUMN `b` `c` INT"
    );
    assert_eq!(
        r("alter table t change b c int after a"),
        "ALTER TABLE `t` CHANGE COLUMN `b` `c` INT AFTER `a`"
    );

    let stmt = parse("alter table t change b c int").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    assert_eq!(
        only_alter_action(&alt),
        AlterTableAction::ChangeColumn {
            if_exists: false,
            old_name: vec!["b".to_string()],
            column: ColumnDef {
                qualifier: vec![],
                name: "c".to_string(),
                ty: ColumnType {
                    name: "INT".to_string(),
                    args: vec![],
                    unsigned: false,
                    zerofill: false,
                    binary: false,
                    charset: None,
                },
                options: vec![],
            },
            position: ColumnPosition::Default,
        }
    );
}

#[test]
fn alter_table_rename() {
    // `TO`/`AS`/neither all restore identically as `RENAME AS`,
    // matching the Go AST's normalization.
    assert_eq!(
        r("alter table t rename to t2"),
        "ALTER TABLE `t` RENAME AS `t2`"
    );
    assert_eq!(
        r("alter table t rename t2"),
        "ALTER TABLE `t` RENAME AS `t2`"
    );
    assert_eq!(
        r("alter table t rename as t2"),
        "ALTER TABLE `t` RENAME AS `t2`"
    );
    assert_eq!(
        r("alter table t rename to a.1"),
        "ALTER TABLE `t` RENAME AS `a`.`1`"
    );
    assert_eq!(
        r("alter table t rename to 'x'"),
        "ALTER TABLE `t` RENAME AS `x`"
    );
    assert!(parse("alter table t rename to a.b.c").is_err());
    let stmt = parse("alter table t rename to t2").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    assert_eq!(
        only_alter_action(&alt),
        AlterTableAction::RenameTable {
            new_name: vec!["t2".to_string()],
        }
    );
}

#[test]
fn rename_table() {
    // A separate top-level statement from ALTER TABLE ... RENAME.
    assert_eq!(r("rename table t to t2"), "RENAME TABLE `t` TO `t2`");
    assert_eq!(
        r("rename table t to t2, u to u2"),
        "RENAME TABLE `t` TO `t2`, `u` TO `u2`"
    );
    let stmt = parse("rename table t to t2, u to u2").unwrap();
    let rt = ddl_payload!(stmt, RenameTable);
    assert_eq!(
        rt.pairs,
        vec![
            (vec!["t".to_string()], vec!["t2".to_string()]),
            (vec!["u".to_string()], vec!["u2".to_string()]),
        ]
    );
}

#[test]
fn alter_table_add_index() {
    // A bare `KEY` normalizes to `INDEX` on restore, matching the Go
    // AST — the opposite of CREATE TABLE's column-level UNIQUE, which
    // adds "KEY" rather than dropping it.
    assert_eq!(
        r("alter table t add index (a)"),
        "ALTER TABLE `t` ADD INDEX(`a`)"
    );
    assert_eq!(
        r("alter table t add key idx_a (a)"),
        "ALTER TABLE `t` ADD INDEX `idx_a`(`a`)"
    );
    assert_eq!(
        r("alter table t add constraint cn1 index idx1 (a)"),
        "ALTER TABLE `t` ADD INDEX `cn1`(`a`)"
    );
    assert_eq!(
        r("alter table t add index (a, b)"),
        "ALTER TABLE `t` ADD INDEX(`a`, `b`)"
    );
    assert_eq!(
        r("alter table t add key idx (`a`(0), b(16), (cast(j as signed array)) desc) comment 'note' global invisible where a > 1"),
        "ALTER TABLE `t` ADD INDEX `idx`(`a`, `b`(16), (CAST(`j` AS SIGNED ARRAY)) DESC) COMMENT 'note' GLOBAL INVISIBLE WHERE `a`>1"
    );
    // Go overwrites repeated scalar index options, including LOCAL's default
    // Global=false state.
    assert_eq!(
        r("alter table t add index idx(a) comment 'old' comment 'new' global local invisible invisible where a > 1 where b > 2"),
        "ALTER TABLE `t` ADD INDEX `idx`(`a`) COMMENT 'new' INVISIBLE WHERE `b`>2"
    );
    assert_eq!(
        r("alter table t add index ((json_type(doc)))"),
        "ALTER TABLE `t` ADD INDEX((JSON_TYPE(`doc`)))"
    );
    assert_eq!(
        r("alter table t add index i(a) using btree with parser p visible"),
        "ALTER TABLE `t` ADD INDEX `i`(`a`) USING BTREE WITH PARSER `p` VISIBLE"
    );
    assert_eq!(
        r("alter table t add vector index i(a)"),
        "ALTER TABLE `t` ADD VECTOR INDEX `i`(`a`)"
    );
    assert_eq!(
        r("alter table t add unique index (a)"),
        "ALTER TABLE `t` ADD UNIQUE(`a`)"
    );
    assert_eq!(
        r("alter table t add unique key idx_a (a)"),
        "ALTER TABLE `t` ADD UNIQUE `idx_a`(`a`)"
    );
    // A CONSTRAINT name wins over an inline index name when both are
    // given, matching CREATE TABLE's table-level constraints.
    assert_eq!(
        r("alter table t add constraint cn1 unique index idx1 (a)"),
        "ALTER TABLE `t` ADD UNIQUE `cn1`(`a`)"
    );

    let stmt = parse("alter table t add unique key idx_a (a)").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    let AlterTableAction::AddIndexConstraint(index) = only_alter_action(&alt) else {
        panic!("expected ADD UNIQUE KEY constraint");
    };
    assert_eq!(index.kind, tidb_ast::IndexConstraintKind::Unique);
    assert_eq!(index.name.as_deref(), Some("idx_a"));
    assert_eq!(index.parts, plain_key_parts(&["a"]));
}

#[test]
fn alter_table_drop_index() {
    // `KEY` is an input alias, while Go's AST canonical restore always uses
    // `DROP INDEX`; `IF EXISTS` is preserved.
    assert_eq!(
        r("alter table t drop index idx_a"),
        "ALTER TABLE `t` DROP INDEX `idx_a`"
    );
    assert_eq!(
        r("alter table t drop key idx_a"),
        "ALTER TABLE `t` DROP INDEX `idx_a`"
    );
    assert_eq!(
        r("alter table t drop index if exists idx_a"),
        "ALTER TABLE `t` DROP INDEX IF EXISTS `idx_a`"
    );
    let stmt = ddl_payload!(
        parse("alter table t drop key if exists idx_a").unwrap(),
        AlterTable
    );
    assert_eq!(
        only_alter_action(&stmt),
        AlterTableAction::DropIndex {
            if_exists: true,
            name: "idx_a".to_string(),
        }
    );
    // `DROP PRIMARY KEY` is a distinct payload-free action, covered by its
    // source-owned test module rather than this generic DROP INDEX slice.
}

/// `pkg/parser/ddl_alter_handlers.go` recoverable warnings.
#[test]
fn alter_table_source_warnings() {
    for (sql, expected) in [
        (
            "ALTER TABLE t ADD PARTITION NO_WRITE_TO_BINLOG",
            "The NO_WRITE_TO_BINLOG option is parsed but ignored for now.",
        ),
        (
            "ALTER TABLE t COALESCE PARTITION LOCAL 1",
            "The NO_WRITE_TO_BINLOG option is parsed but ignored for now.",
        ),
        (
            "ALTER TABLE t LAST PARTITION LESS THAN (10) LOCAL",
            "The NO_WRITE_TO_BINLOG option is parsed but ignored for now.",
        ),
        (
            "ALTER TABLE t CHECK PARTITION ALL",
            "The CHECK PARTITIONING clause is parsed but not implement yet.",
        ),
        (
            "ALTER TABLE t IMPORT TABLESPACE",
            "The IMPORT TABLESPACE clause is parsed but ignored by all storage engines.",
        ),
        (
            "ALTER TABLE t DISCARD PARTITION ALL TABLESPACE",
            "The DISCARD PARTITION TABLESPACE clause is parsed but ignored by all storage engines.",
        ),
        (
            "ALTER TABLE t SECONDARY_LOAD",
            "The SECONDARY_LOAD clause is parsed but not implement yet.",
        ),
        (
            "ALTER TABLE t SECONDARY_UNLOAD",
            "The SECONDARY_UNLOAD VALIDATION clause is parsed but not implement yet.",
        ),
    ] {
        let output = parse_with_warnings(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert_eq!(
            output
                .warnings
                .iter()
                .map(|warning| warning.message.as_str())
                .collect::<Vec<_>>(),
            vec![expected],
            "{sql}"
        );
    }
}

/// `pkg/parser/ddl_alter_parser.go` identifier and lenient ALTER boundaries.
#[test]
fn alter_table_parser_source_boundaries() {
    for (sql, expected) in [
        (
            "ALTER IGNORE TABLE t ADD COLUMN a INT",
            "ALTER TABLE `t` ADD COLUMN `a` INT",
        ),
        (
            "ALTER TABLE t CHANGE a.b c INT",
            "ALTER TABLE `t` CHANGE COLUMN `a`.`b` `c` INT",
        ),
        (
            "ALTER TABLE t MODIFY c INT AFTER @a",
            "ALTER TABLE `t` MODIFY COLUMN `c` INT AFTER `a`",
        ),
        (
            "ALTER TABLE t ORDER BY @a DESC",
            "ALTER TABLE `t` ORDER BY `a` DESC",
        ),
        (
            "ALTER TABLE t ADD PARTITION (PARTITION p VALUES LESS THAN (a AND b))",
            "ALTER TABLE `t` ADD PARTITION (PARTITION `p` VALUES LESS THAN (`a` AND `b`))",
        ),
        (
            "ALTER TABLE t ADD PARTITION (PARTITION p VALUES LESS THAN (1) (SUBPARTITION 's' COMMENT 'x'))",
            "ALTER TABLE `t` ADD PARTITION (PARTITION `p` VALUES LESS THAN (1) (SUBPARTITION `s` COMMENT = 'x'))",
        ),
        (
            "ALTER TABLE t SPLIT TABLE BY 1,2",
            "ALTER TABLE `t` SPLIT BY (1),(2)",
        ),
        (
            "ALTER TABLE t SPLIT INDEX @i BY (1)",
            "ALTER TABLE `t` SPLIT INDEX `i` BY (1)",
        ),
        (
            "ALTER TABLE t SPLIT REGION",
            "ALTER TABLE `t` SPLIT BETWEEN () AND () REGIONS 0",
        ),
        (
            "ALTER TABLE t ANALYZE PARTITION @p INDEX @i,@j WITH 1 TOPN,2 BUCKETS",
            "ANALYZE TABLE `t` PARTITION `p` INDEX `i`,`j` WITH 1 TOPN, 2 BUCKETS",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
    assert!(parse("ALTER TABLE a.b.c ADD COLUMN d INT").is_err());
    assert!(parse("ALTER TABLE t ADD PARTITION (PARTITION 'p' VALUES LESS THAN (1))").is_err());
    assert!(
        parse("ALTER TABLE t SET TIFLASH REPLICA 1 LOCATION LABELS 'a', ADD COLUMN b INT").is_err()
    );
}
