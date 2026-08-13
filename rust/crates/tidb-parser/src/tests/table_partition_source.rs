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

use super::*;

/// Exact source vectors from `pkg/parser/parser_test.go::TestTablePartition`.
#[test]
fn test_table_partition_source_of_truth() {
    for (sql, expected) in [
        ("ALTER TABLE t1 TRUNCATE PARTITION p0", Some("ALTER TABLE `t1` TRUNCATE PARTITION `p0`")),
        ("ALTER TABLE t1 TRUNCATE PARTITION p0, p1", Some("ALTER TABLE `t1` TRUNCATE PARTITION `p0`,`p1`")),
        ("ALTER TABLE t1 TRUNCATE PARTITION ALL", Some("ALTER TABLE `t1` TRUNCATE PARTITION ALL")),
        ("ALTER TABLE t1 TRUNCATE PARTITION ALL, p0", None),
        ("ALTER TABLE t1 TRUNCATE PARTITION p0, ALL", None),
        ("ALTER TABLE t1 OPTIMIZE PARTITION p0", Some("ALTER TABLE `t1` OPTIMIZE PARTITION `p0`")),
        ("ALTER TABLE t1 OPTIMIZE PARTITION NO_WRITE_TO_BINLOG p0", Some("ALTER TABLE `t1` OPTIMIZE PARTITION NO_WRITE_TO_BINLOG `p0`")),
        ("ALTER TABLE t1 OPTIMIZE PARTITION LOCAL p0", Some("ALTER TABLE `t1` OPTIMIZE PARTITION NO_WRITE_TO_BINLOG `p0`")),
        ("ALTER TABLE t1 OPTIMIZE PARTITION p0, p1", Some("ALTER TABLE `t1` OPTIMIZE PARTITION `p0`,`p1`")),
        ("ALTER TABLE t1 OPTIMIZE PARTITION NO_WRITE_TO_BINLOG p0, p1", Some("ALTER TABLE `t1` OPTIMIZE PARTITION NO_WRITE_TO_BINLOG `p0`,`p1`")),
        ("ALTER TABLE t1 OPTIMIZE PARTITION LOCAL p0, p1", Some("ALTER TABLE `t1` OPTIMIZE PARTITION NO_WRITE_TO_BINLOG `p0`,`p1`")),
        ("ALTER TABLE t1 OPTIMIZE PARTITION ALL", Some("ALTER TABLE `t1` OPTIMIZE PARTITION ALL")),
        ("ALTER TABLE t1 OPTIMIZE PARTITION NO_WRITE_TO_BINLOG ALL", Some("ALTER TABLE `t1` OPTIMIZE PARTITION NO_WRITE_TO_BINLOG ALL")),
        ("ALTER TABLE t1 OPTIMIZE PARTITION LOCAL ALL", Some("ALTER TABLE `t1` OPTIMIZE PARTITION NO_WRITE_TO_BINLOG ALL")),
        ("ALTER TABLE t1 OPTIMIZE PARTITION ALL, p0", None),
        ("ALTER TABLE t1 OPTIMIZE PARTITION p0, ALL", None),
        ("ALTER TABLE t_n OPTIMIZE PARTITION LOCAL", None),
        ("ALTER TABLE t_n OPTIMIZE PARTITION LOCAL local", Some("ALTER TABLE `t_n` OPTIMIZE PARTITION NO_WRITE_TO_BINLOG `local`")),
        ("ALTER TABLE t_n OPTIMIZE PARTITION LOCAL local, local", Some("ALTER TABLE `t_n` OPTIMIZE PARTITION NO_WRITE_TO_BINLOG `local`,`local`")),
        ("ALTER TABLE t1 REPAIR PARTITION p0", Some("ALTER TABLE `t1` REPAIR PARTITION `p0`")),
        ("ALTER TABLE t1 REPAIR PARTITION NO_WRITE_TO_BINLOG p0", Some("ALTER TABLE `t1` REPAIR PARTITION NO_WRITE_TO_BINLOG `p0`")),
        ("ALTER TABLE t1 REPAIR PARTITION LOCAL p0", Some("ALTER TABLE `t1` REPAIR PARTITION NO_WRITE_TO_BINLOG `p0`")),
        ("ALTER TABLE t1 REPAIR PARTITION p0, p1", Some("ALTER TABLE `t1` REPAIR PARTITION `p0`,`p1`")),
        ("ALTER TABLE t1 REPAIR PARTITION NO_WRITE_TO_BINLOG p0, p1", Some("ALTER TABLE `t1` REPAIR PARTITION NO_WRITE_TO_BINLOG `p0`,`p1`")),
        ("ALTER TABLE t1 REPAIR PARTITION LOCAL p0, p1", Some("ALTER TABLE `t1` REPAIR PARTITION NO_WRITE_TO_BINLOG `p0`,`p1`")),
        ("ALTER TABLE t1 REPAIR PARTITION ALL", Some("ALTER TABLE `t1` REPAIR PARTITION ALL")),
        ("ALTER TABLE t1 REPAIR PARTITION NO_WRITE_TO_BINLOG ALL", Some("ALTER TABLE `t1` REPAIR PARTITION NO_WRITE_TO_BINLOG ALL")),
        ("ALTER TABLE t1 REPAIR PARTITION LOCAL ALL", Some("ALTER TABLE `t1` REPAIR PARTITION NO_WRITE_TO_BINLOG ALL")),
        ("ALTER TABLE t1 REPAIR PARTITION ALL, p0", None),
        ("ALTER TABLE t1 REPAIR PARTITION p0, ALL", None),
        ("ALTER TABLE t_n REPAIR PARTITION LOCAL", None),
        ("ALTER TABLE t_n REPAIR PARTITION LOCAL local", Some("ALTER TABLE `t_n` REPAIR PARTITION NO_WRITE_TO_BINLOG `local`")),
        ("ALTER TABLE t_n REPAIR PARTITION LOCAL local, local", Some("ALTER TABLE `t_n` REPAIR PARTITION NO_WRITE_TO_BINLOG `local`,`local`")),
        ("ALTER TABLE t1 IMPORT PARTITION p0 TABLESPACE", Some("ALTER TABLE `t1` IMPORT PARTITION `p0` TABLESPACE")),
        ("ALTER TABLE t1 IMPORT PARTITION p0, p1 TABLESPACE", Some("ALTER TABLE `t1` IMPORT PARTITION `p0`,`p1` TABLESPACE")),
        ("ALTER TABLE t1 IMPORT PARTITION ALL TABLESPACE", Some("ALTER TABLE `t1` IMPORT PARTITION ALL TABLESPACE")),
        ("ALTER TABLE t1 IMPORT PARTITION ALL, p0 TABLESPACE", None),
        ("ALTER TABLE t1 IMPORT PARTITION p0, ALL TABLESPACE", None),
        ("ALTER TABLE t1 DISCARD PARTITION p0 TABLESPACE", Some("ALTER TABLE `t1` DISCARD PARTITION `p0` TABLESPACE")),
        ("ALTER TABLE t1 DISCARD PARTITION p0, p1 TABLESPACE", Some("ALTER TABLE `t1` DISCARD PARTITION `p0`,`p1` TABLESPACE")),
        ("ALTER TABLE t1 DISCARD PARTITION ALL TABLESPACE", Some("ALTER TABLE `t1` DISCARD PARTITION ALL TABLESPACE")),
        ("ALTER TABLE t1 DISCARD PARTITION ALL, p0 TABLESPACE", None),
        ("ALTER TABLE t1 DISCARD PARTITION p0, ALL TABLESPACE", None),
        ("ALTER TABLE t1 ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT 'APSTART \\' APEND')", Some("ALTER TABLE `t1` ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'APSTART '' APEND')")),
        ("ALTER TABLE t1 ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'xxx')", Some("ALTER TABLE `t1` ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'xxx')")),
        ("CREATE TABLE t1 (a int not null,b int not null,c int not null,primary key(a,b))\n\t\tpartition by range (a)\n\t\tpartitions 3\n\t\t(partition x1 values less than (5),\n\t\t partition x2 values less than (10),\n\t\t partition x3 values less than maxvalue);", Some("CREATE TABLE `t1` (`a` INT NOT NULL,`b` INT NOT NULL,`c` INT NOT NULL,PRIMARY KEY(`a`, `b`)) PARTITION BY RANGE (`a`) (PARTITION `x1` VALUES LESS THAN (5),PARTITION `x2` VALUES LESS THAN (10),PARTITION `x3` VALUES LESS THAN (MAXVALUE))")),
        ("CREATE TABLE t1 (a int not null) partition by range (a) (partition x1 values less than (5) tablespace ts1)", Some("CREATE TABLE `t1` (`a` INT NOT NULL) PARTITION BY RANGE (`a`) (PARTITION `x1` VALUES LESS THAN (5) TABLESPACE = `ts1`)")),
        ("create table t (a int) partition by range (a)\n\t\t  (PARTITION p0 VALUES LESS THAN (63340531200) ENGINE = MyISAM,\n\t\t   PARTITION p1 VALUES LESS THAN (63342604800) ENGINE MyISAM)", Some("CREATE TABLE `t` (`a` INT) PARTITION BY RANGE (`a`) (PARTITION `p0` VALUES LESS THAN (63340531200) ENGINE = MyISAM,PARTITION `p1` VALUES LESS THAN (63342604800) ENGINE = MyISAM)")),
        ("create table t (a int) partition by range (a)\n\t\t  (PARTITION p0 VALUES LESS THAN (63340531200) ENGINE = MyISAM COMMENT 'xxx',\n\t\t   PARTITION p1 VALUES LESS THAN (63342604800) ENGINE = MyISAM)", Some("CREATE TABLE `t` (`a` INT) PARTITION BY RANGE (`a`) (PARTITION `p0` VALUES LESS THAN (63340531200) ENGINE = MyISAM COMMENT = 'xxx',PARTITION `p1` VALUES LESS THAN (63342604800) ENGINE = MyISAM)")),
        ("create table t1 (a int) partition by range (a)\n\t\t  (PARTITION p0 VALUES LESS THAN (63340531200) COMMENT 'xxx' ENGINE = MyISAM ,\n\t\t   PARTITION p1 VALUES LESS THAN (63342604800) ENGINE = MyISAM)", Some("CREATE TABLE `t1` (`a` INT) PARTITION BY RANGE (`a`) (PARTITION `p0` VALUES LESS THAN (63340531200) COMMENT = 'xxx' ENGINE = MyISAM,PARTITION `p1` VALUES LESS THAN (63342604800) ENGINE = MyISAM)")),
        ("create table t (id int)\n\t\t    partition by range (id)\n\t\t    subpartition by key (id) subpartitions 2\n\t\t    (partition p0 values less than (42))", Some("CREATE TABLE `t` (`id` INT) PARTITION BY RANGE (`id`) SUBPARTITION BY KEY (`id`) SUBPARTITIONS 2 (PARTITION `p0` VALUES LESS THAN (42))")),
        ("create table t (id int)\n\t\t    partition by range (id)\n\t\t    subpartition by hash (id)\n\t\t    (partition p0 values less than (42))", Some("CREATE TABLE `t` (`id` INT) PARTITION BY RANGE (`id`) SUBPARTITION BY HASH (`id`) (PARTITION `p0` VALUES LESS THAN (42))")),
        ("create table t1 (a varchar(5), b int signed, c varchar(10), d datetime)\n\t\tpartition by range columns(b,c)\n\t\tsubpartition by hash(to_seconds(d))\n\t\t( partition p0 values less than (2, 'b'),\n\t\t  partition p1 values less than (4, 'd'),\n\t\t  partition p2 values less than (10, 'za'));", Some("CREATE TABLE `t1` (`a` VARCHAR(5),`b` INT,`c` VARCHAR(10),`d` DATETIME) PARTITION BY RANGE COLUMNS (`b`,`c`) SUBPARTITION BY HASH (TO_SECONDS(`d`)) (PARTITION `p0` VALUES LESS THAN (2, _UTF8MB4'b'),PARTITION `p1` VALUES LESS THAN (4, _UTF8MB4'd'),PARTITION `p2` VALUES LESS THAN (10, _UTF8MB4'za'))")),
        ("CREATE TABLE t1 (a INT, b TIMESTAMP DEFAULT '0000-00-00 00:00:00')\nENGINE=INNODB PARTITION BY LINEAR HASH (a) PARTITIONS 1;", Some("CREATE TABLE `t1` (`a` INT,`b` TIMESTAMP DEFAULT _UTF8MB4'0000-00-00 00:00:00') ENGINE = INNODB PARTITION BY LINEAR HASH (`a`) PARTITIONS 1")),
        ("create table t1 (a int) partition by hash (a) (partition x, partition y)", Some("CREATE TABLE `t1` (`a` INT) PARTITION BY HASH (`a`) (PARTITION `x`,PARTITION `y`)")),
        ("create table t1 (a int) partition by key (a) (partition x, partition y)", Some("CREATE TABLE `t1` (`a` INT) PARTITION BY KEY (`a`) (PARTITION `x`,PARTITION `y`)")),
        ("create table t1 (a int) partition by range (a) (partition x, partition y)", None),
        ("create table t1 (a int) partition by list (a) (partition x, partition y)", None),
        ("create table t1 (a int) partition by system_time (partition x, partition y)", None),
        ("create table t1 (a int) partition by hash (a) (partition x values less than (10))", None),
        ("create table t1 (a int) partition by key (a) (partition x values less than (10))", None),
        ("create table t1 (a int) partition by range (a) (partition x values less than (maxvalue))", Some("CREATE TABLE `t1` (`a` INT) PARTITION BY RANGE (`a`) (PARTITION `x` VALUES LESS THAN (MAXVALUE))")),
        ("create table t1 (a int) partition by range (a) (partition x values less than (default))", None),
        ("create table t (a varchar(100), b int) partition by list columns (a) (partition p1 values in ('a','b','DEFAULT'), partition pDef values in (default))", Some("CREATE TABLE `t` (`a` VARCHAR(100),`b` INT) PARTITION BY LIST COLUMNS (`a`) (PARTITION `p1` VALUES IN (_UTF8MB4'a', _UTF8MB4'b', _UTF8MB4'DEFAULT'),PARTITION `pDef` DEFAULT)")),
        ("create table t1 (a int) partition by range (a) (partition x values less than (10))", Some("CREATE TABLE `t1` (`a` INT) PARTITION BY RANGE (`a`) (PARTITION `x` VALUES LESS THAN (10))")),
        ("create table t1 (a int) partition by list (a) (partition x values less than (10))", None),
        ("create table t1 (a int) partition by system_time (partition x values less than (10))", None),
        ("create table t1 (a int) partition by hash (a) (partition x values in (10))", None),
        ("create table t1 (a int) partition by key (a) (partition x values in (10))", None),
        ("create table t1 (a int) partition by range (a) (partition x values in (10))", None),
        ("create table t1 (a int) partition by list (a) (partition x values in (10))", Some("CREATE TABLE `t1` (`a` INT) PARTITION BY LIST (`a`) (PARTITION `x` VALUES IN (10))")),
        ("create table t1 (a int) partition by list (a) (partition x values in (default))", Some("CREATE TABLE `t1` (`a` INT) PARTITION BY LIST (`a`) (PARTITION `x` DEFAULT)")),
        ("create table t1 (a int) partition by list (a) (partition x values in (maxvalue))", None),
        ("create table t1 (a int) partition by list (a) (partition x values in (default, 10))", Some("CREATE TABLE `t1` (`a` INT) PARTITION BY LIST (`a`) (PARTITION `x` VALUES IN (DEFAULT, 10))")),
        ("create table t1 (a int) partition by system_time (partition x values in (10))", None),
        ("create table t1 (a int) partition by hash (a) (partition x history, partition y current)", None),
        ("create table t1 (a int) partition by key (a) (partition x history, partition y current)", None),
        ("create table t1 (a int) partition by range (a) (partition x history, partition y current)", None),
        ("create table t1 (a int) partition by list (a) (partition x history, partition y current)", None),
        ("create table t1 (a int) partition by system_time (partition x history, partition y current)", Some("CREATE TABLE `t1` (`a` INT) PARTITION BY SYSTEM_TIME (PARTITION `x` HISTORY,PARTITION `y` CURRENT)")),
        ("create table t1 (a int) partition by hash (a)", Some("CREATE TABLE `t1` (`a` INT) PARTITION BY HASH (`a`) PARTITIONS 1")),
        ("create table t1 (a int) partition by key (a)", Some("CREATE TABLE `t1` (`a` INT) PARTITION BY KEY (`a`) PARTITIONS 1")),
        ("create table t1 (a int) partition by range (a)", None),
        ("create table t1 (a int) partition by list (a)", None),
        ("create table t1 (a int) partition by system_time", None),
        ("create table t1 (a int) partition by system_time (partition x history)", None),
        ("create table t1 (a int) partition by system_time (partition x current)", None),
        ("create table t1 (a int, b int) partition by range (a) (partition x values less than (10, 20))", None),
        ("create table t (id int) partition by range columns (id) (partition p0 values less than (1, 2))", None),
        ("create table t1 (a int, b int) partition by range columns (a, b) (partition x values less than (10, 20))", Some("CREATE TABLE `t1` (`a` INT,`b` INT) PARTITION BY RANGE COLUMNS (`a`,`b`) (PARTITION `x` VALUES LESS THAN (10, 20))")),
        ("create table t1 (a int, b int) partition by range columns (a, b) (partition x values less than (10))", None),
        ("create table t1 (a int, b int) partition by range columns (a, b) (partition x values less than maxvalue)", None),
        ("create table t1 (a int, b int) partition by list (a) (partition x values in ((10, 20)))", None),
        ("create table t1 (a int, b int) partition by list columns (a, b) (partition x values in ((10, 20)))", Some("CREATE TABLE `t1` (`a` INT,`b` INT) PARTITION BY LIST COLUMNS (`a`,`b`) (PARTITION `x` VALUES IN ((10, 20)))")),
        ("create table t1 (a int, b int) partition by list columns (a, b) (partition x values in (10, 20))", None),
        ("create table t1 (a int, b int) partition by list columns (a, b) (partition x values in (10, (20, 30)))", None),
        ("create table t1 (a int, b int) partition by list columns (a, b) (partition x values in ((10, 20), 30))", None),
        ("create table t1 (a int, b int) partition by list columns (a, b) (partition x values in ((10, 20), (30, 40, 50)))", None),
        ("create table t1 (a int) partition by hash (a) ()", None),
        ("create table t1 (a int primary key) partition by key ()", Some("CREATE TABLE `t1` (`a` INT PRIMARY KEY) PARTITION BY KEY () PARTITIONS 1")),
        ("create table t1 (a int) partition by range columns () (partition x values less than maxvalue)", None),
        ("create table t1 (a int) partition by list columns () (partition x default)", None),
        ("create table t1 (a int) partition by range (a) (partition x values less than ())", None),
        ("create table t1 (a int) partition by list (a) (partition x values in ())", None),
        ("create table t1 (a int) partition by list (a) (partition x default)", Some("CREATE TABLE `t1` (`a` INT) PARTITION BY LIST (`a`) (PARTITION `x` DEFAULT)")),
        ("create table t1 (a int, b int) partition by range (a) subpartition by range (b) (partition x values less than maxvalue)", None),
        ("create table t1 (a int) partition by hash (a) partitions 2 (partition x)", None),
        ("create table t1 (a int) partition by hash (a) partitions 2 (partition x, partition y)", Some("CREATE TABLE `t1` (`a` INT) PARTITION BY HASH (`a`) (PARTITION `x`,PARTITION `y`)")),
        ("create table t1 (a int, b int) partition by range (a) subpartition by hash (b) subpartitions 2 (partition x values less than maxvalue (subpartition y))", None),
        ("create table t1 (a int, b int) partition by range (a) subpartition by hash (b) (partition x values less than (10) (subpartition y),partition a values less than (20) (subpartition b,subpartition c))", None),
        ("create table t1 (a int, b int) partition by range (a) (partition x values less than (10) (subpartition y))", None),
        ("create table t1 (a int) partition by hash (a) partitions 0", Some("CREATE TABLE `t1` (`a` INT) PARTITION BY HASH (`a`)")),
        ("create table t1 (a int, b int) partition by range (a) subpartition by hash (b) subpartitions 0 (partition x values less than (10))", None),
        ("create table t1 (a int) partition by system_time interval 7 day limit 50000 (partition x history, partition y current)", None),
    ] {
        match expected {
            Some(expected) => assert_eq!(r(sql), expected, "{sql}"),
            None => assert!(parse(sql).is_err(), "expected parse error for: {sql}"),
        }
    }
}

/// Mirrors the partition-comment payload assertion that follows the Go table.
#[test]
fn test_table_partition_comment_payload() {
    let statement = parse(
        "create table t (id int) partition by range (id) (partition p0 values less than (10) comment 'check')",
    )
    .expect("parse partition comment source row");
    let Stmt::Ddl(ddl) = statement else {
        panic!("expected DDL statement");
    };
    let tidb_ast::DdlStmt::CreateTable(create) = ddl.as_ref() else {
        panic!("expected CREATE TABLE statement");
    };
    let partitioning = create.partitioning.as_ref().expect("partitioning payload");
    assert_eq!(partitioning.definitions.len(), 1);
    assert!(matches!(
        partitioning.definitions[0].options.as_slice(),
        [tidb_ast::TableOption::Comment(comment)] if comment == "check"
    ));
}
