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

//! Exact transcreation of every row in
//! `pkg/parser/ast/ddl_test.go::TestAlterTableSpecRestore`.

use super::*;

#[test]
fn alter_table_spec_restore_transcreates_every_original_go_row() {
    for (sql, expected) in [
        (
            r#"ALTER TABLE t ENGINE innodb"#,
            r#"ALTER TABLE `t` ENGINE = innodb"#,
        ),
        (
            r#"ALTER TABLE t ENGINE = innodb"#,
            r#"ALTER TABLE `t` ENGINE = innodb"#,
        ),
        (
            r#"ALTER TABLE t ENGINE = 'innodb'"#,
            r#"ALTER TABLE `t` ENGINE = innodb"#,
        ),
        (
            r#"ALTER TABLE t ENGINE tokudb"#,
            r#"ALTER TABLE `t` ENGINE = tokudb"#,
        ),
        (
            r#"ALTER TABLE t ENGINE = tokudb"#,
            r#"ALTER TABLE `t` ENGINE = tokudb"#,
        ),
        (
            r#"ALTER TABLE t ENGINE = 'tokudb'"#,
            r#"ALTER TABLE `t` ENGINE = tokudb"#,
        ),
        (
            r#"ALTER TABLE t DEFAULT CHARACTER SET utf8"#,
            r#"ALTER TABLE `t` DEFAULT CHARACTER SET = UTF8"#,
        ),
        (
            r#"ALTER TABLE t DEFAULT CHARACTER SET = utf8"#,
            r#"ALTER TABLE `t` DEFAULT CHARACTER SET = UTF8"#,
        ),
        (
            r#"ALTER TABLE t DEFAULT CHARSET utf8"#,
            r#"ALTER TABLE `t` DEFAULT CHARACTER SET = UTF8"#,
        ),
        (
            r#"ALTER TABLE t DEFAULT CHARSET = utf8"#,
            r#"ALTER TABLE `t` DEFAULT CHARACTER SET = UTF8"#,
        ),
        (
            r#"ALTER TABLE t DEFAULT COLLATE utf8_bin"#,
            r#"ALTER TABLE `t` DEFAULT COLLATE = UTF8_BIN"#,
        ),
        (
            r#"ALTER TABLE t DEFAULT COLLATE = utf8_bin"#,
            r#"ALTER TABLE `t` DEFAULT COLLATE = UTF8_BIN"#,
        ),
        (
            r#"ALTER TABLE t AUTO_INCREMENT 3"#,
            r#"ALTER TABLE `t` AUTO_INCREMENT = 3"#,
        ),
        (
            r#"ALTER TABLE t AUTO_INCREMENT = 6"#,
            r#"ALTER TABLE `t` AUTO_INCREMENT = 6"#,
        ),
        (
            r#"ALTER TABLE t COMMENT ''"#,
            r#"ALTER TABLE `t` COMMENT = ''"#,
        ),
        (
            r#"ALTER TABLE t COMMENT 'system role'"#,
            r#"ALTER TABLE `t` COMMENT = 'system role'"#,
        ),
        (
            r#"ALTER TABLE t COMMENT = 'system role'"#,
            r#"ALTER TABLE `t` COMMENT = 'system role'"#,
        ),
        (
            r#"ALTER TABLE t AVG_ROW_LENGTH 12"#,
            r#"ALTER TABLE `t` AVG_ROW_LENGTH = 12"#,
        ),
        (
            r#"ALTER TABLE t AVG_ROW_LENGTH = 6"#,
            r#"ALTER TABLE `t` AVG_ROW_LENGTH = 6"#,
        ),
        (
            r#"ALTER TABLE t connection 'abc'"#,
            r#"ALTER TABLE `t` CONNECTION = 'abc'"#,
        ),
        (
            r#"ALTER TABLE t CONNECTION = 'abc'"#,
            r#"ALTER TABLE `t` CONNECTION = 'abc'"#,
        ),
        (
            r#"ALTER TABLE t checksum 1"#,
            r#"ALTER TABLE `t` CHECKSUM = 1"#,
        ),
        (
            r#"ALTER TABLE t checksum = 0"#,
            r#"ALTER TABLE `t` CHECKSUM = 0"#,
        ),
        (
            r#"ALTER TABLE t PASSWORD '123456'"#,
            r#"ALTER TABLE `t` PASSWORD = '123456'"#,
        ),
        (
            r#"ALTER TABLE t PASSWORD = ''"#,
            r#"ALTER TABLE `t` PASSWORD = ''"#,
        ),
        (
            r#"ALTER TABLE t compression 'NONE'"#,
            r#"ALTER TABLE `t` COMPRESSION = 'NONE'"#,
        ),
        (
            r#"ALTER TABLE t compression = 'lz4'"#,
            r#"ALTER TABLE `t` COMPRESSION = 'lz4'"#,
        ),
        (
            r#"ALTER TABLE t key_block_size 1024"#,
            r#"ALTER TABLE `t` KEY_BLOCK_SIZE = 1024"#,
        ),
        (
            r#"ALTER TABLE t KEY_BLOCK_SIZE = 1024"#,
            r#"ALTER TABLE `t` KEY_BLOCK_SIZE = 1024"#,
        ),
        (
            r#"ALTER TABLE t max_rows 1000"#,
            r#"ALTER TABLE `t` MAX_ROWS = 1000"#,
        ),
        (
            r#"ALTER TABLE t max_rows = 1000"#,
            r#"ALTER TABLE `t` MAX_ROWS = 1000"#,
        ),
        (
            r#"ALTER TABLE t min_rows 1000"#,
            r#"ALTER TABLE `t` MIN_ROWS = 1000"#,
        ),
        (
            r#"ALTER TABLE t MIN_ROWS = 1000"#,
            r#"ALTER TABLE `t` MIN_ROWS = 1000"#,
        ),
        (
            r#"ALTER TABLE t DELAY_KEY_WRITE 1"#,
            r#"ALTER TABLE `t` DELAY_KEY_WRITE = 1"#,
        ),
        (
            r#"ALTER TABLE t DELAY_KEY_WRITE = 1000"#,
            r#"ALTER TABLE `t` DELAY_KEY_WRITE = 1000"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT default"#,
            r#"ALTER TABLE `t` ROW_FORMAT = DEFAULT"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = default"#,
            r#"ALTER TABLE `t` ROW_FORMAT = DEFAULT"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = fixed"#,
            r#"ALTER TABLE `t` ROW_FORMAT = FIXED"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = compressed"#,
            r#"ALTER TABLE `t` ROW_FORMAT = COMPRESSED"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = compact"#,
            r#"ALTER TABLE `t` ROW_FORMAT = COMPACT"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = redundant"#,
            r#"ALTER TABLE `t` ROW_FORMAT = REDUNDANT"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = dynamic"#,
            r#"ALTER TABLE `t` ROW_FORMAT = DYNAMIC"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT tokudb_default"#,
            r#"ALTER TABLE `t` ROW_FORMAT = TOKUDB_DEFAULT"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = tokudb_default"#,
            r#"ALTER TABLE `t` ROW_FORMAT = TOKUDB_DEFAULT"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = tokudb_fast"#,
            r#"ALTER TABLE `t` ROW_FORMAT = TOKUDB_FAST"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = tokudb_small"#,
            r#"ALTER TABLE `t` ROW_FORMAT = TOKUDB_SMALL"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = tokudb_zlib"#,
            r#"ALTER TABLE `t` ROW_FORMAT = TOKUDB_ZLIB"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = tokudb_zstd"#,
            r#"ALTER TABLE `t` ROW_FORMAT = TOKUDB_ZSTD"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = tokudb_quicklz"#,
            r#"ALTER TABLE `t` ROW_FORMAT = TOKUDB_QUICKLZ"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = tokudb_lzma"#,
            r#"ALTER TABLE `t` ROW_FORMAT = TOKUDB_LZMA"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = tokudb_snappy"#,
            r#"ALTER TABLE `t` ROW_FORMAT = TOKUDB_SNAPPY"#,
        ),
        (
            r#"ALTER TABLE t ROW_FORMAT = tokudb_uncompressed"#,
            r#"ALTER TABLE `t` ROW_FORMAT = TOKUDB_UNCOMPRESSED"#,
        ),
        (
            r#"ALTER TABLE t shard_row_id_bits 1"#,
            r#"ALTER TABLE `t` SHARD_ROW_ID_BITS = 1"#,
        ),
        (
            r#"ALTER TABLE t shard_row_id_bits = 1"#,
            r#"ALTER TABLE `t` SHARD_ROW_ID_BITS = 1"#,
        ),
        (
            r#"ALTER TABLE t CONVERT TO CHARACTER SET utf8"#,
            r#"ALTER TABLE `t` CONVERT TO CHARACTER SET UTF8"#,
        ),
        (
            r#"ALTER TABLE t CONVERT TO CHARSET utf8"#,
            r#"ALTER TABLE `t` CONVERT TO CHARACTER SET UTF8"#,
        ),
        (
            r#"ALTER TABLE t CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin"#,
            r#"ALTER TABLE `t` CONVERT TO CHARACTER SET UTF8 COLLATE UTF8_BIN"#,
        ),
        (
            r#"ALTER TABLE t CONVERT TO CHARSET utf8 COLLATE utf8_bin"#,
            r#"ALTER TABLE `t` CONVERT TO CHARACTER SET UTF8 COLLATE UTF8_BIN"#,
        ),
        (
            r#"ALTER TABLE t ADD COLUMN (a SMALLINT UNSIGNED)"#,
            r#"ALTER TABLE `t` ADD COLUMN (`a` SMALLINT UNSIGNED)"#,
        ),
        (
            r#"ALTER TABLE t ADD COLUMN (a SMALLINT UNSIGNED, b varchar(255))"#,
            r#"ALTER TABLE `t` ADD COLUMN (`a` SMALLINT UNSIGNED, `b` VARCHAR(255))"#,
        ),
        (
            r#"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED"#,
            r#"ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED"#,
        ),
        (
            r#"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED FIRST"#,
            r#"ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED FIRST"#,
        ),
        (
            r#"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED AFTER b"#,
            r#"ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED AFTER `b`"#,
        ),
        (
            r#"ALTER TABLE t ADD COLUMN name mediumtext CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL"#,
            r#"ALTER TABLE `t` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL"#,
        ),
        (
            r#"ALTER TABLE t ADD CONSTRAINT INDEX par_ind (parent_id)"#,
            r#"ALTER TABLE `t` ADD INDEX `par_ind`(`parent_id`)"#,
        ),
        (
            r#"ALTER TABLE t ADD CONSTRAINT INDEX par_ind (parent_id(6))"#,
            r#"ALTER TABLE `t` ADD INDEX `par_ind`(`parent_id`(6))"#,
        ),
        (
            r#"ALTER TABLE t ADD CONSTRAINT key par_ind (parent_id)"#,
            r#"ALTER TABLE `t` ADD INDEX `par_ind`(`parent_id`)"#,
        ),
        (
            r#"ALTER TABLE t ADD CONSTRAINT unique par_ind (parent_id)"#,
            r#"ALTER TABLE `t` ADD UNIQUE `par_ind`(`parent_id`)"#,
        ),
        (
            r#"ALTER TABLE t ADD CONSTRAINT unique key par_ind (parent_id)"#,
            r#"ALTER TABLE `t` ADD UNIQUE `par_ind`(`parent_id`)"#,
        ),
        (
            r#"ALTER TABLE t ADD CONSTRAINT unique index par_ind (parent_id)"#,
            r#"ALTER TABLE `t` ADD UNIQUE `par_ind`(`parent_id`)"#,
        ),
        (
            r#"ALTER TABLE t ADD CONSTRAINT fulltext key full_id (parent_id)"#,
            r#"ALTER TABLE `t` ADD FULLTEXT `full_id`(`parent_id`)"#,
        ),
        (
            r#"ALTER TABLE t ADD CONSTRAINT fulltext INDEX full_id (parent_id)"#,
            r#"ALTER TABLE `t` ADD FULLTEXT `full_id`(`parent_id`)"#,
        ),
        (
            r#"ALTER TABLE t ADD CONSTRAINT PRIMARY KEY (id)"#,
            r#"ALTER TABLE `t` ADD PRIMARY KEY(`id`)"#,
        ),
        (
            r#"ALTER TABLE t ADD CONSTRAINT PRIMARY KEY (id) key_block_size = 32 using hash comment 'hello'"#,
            r#"ALTER TABLE `t` ADD PRIMARY KEY(`id`) KEY_BLOCK_SIZE=32 USING HASH COMMENT 'hello'"#,
        ),
        (
            r#"ALTER TABLE t ADD CONSTRAINT FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent(id) ON DELETE CASCADE"#,
            r#"ALTER TABLE `t` ADD CONSTRAINT FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE"#,
        ),
        (
            r#"ALTER TABLE t ADD CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT"#,
            r#"ALTER TABLE `t` ADD CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"#,
        ),
        (
            r#"ALTER TABLE t ADD CONSTRAINT fk_123 FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT"#,
            r#"ALTER TABLE `t` ADD CONSTRAINT `fk_123` FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"#,
        ),
        (
            r#"ALTER TABLE t DROP COLUMN a"#,
            r#"ALTER TABLE `t` DROP COLUMN `a`"#,
        ),
        (
            r#"ALTER TABLE t DROP COLUMN a RESTRICT"#,
            r#"ALTER TABLE `t` DROP COLUMN `a`"#,
        ),
        (
            r#"ALTER TABLE t DROP COLUMN a CASCADE"#,
            r#"ALTER TABLE `t` DROP COLUMN `a`"#,
        ),
        (
            r#"ALTER TABLE t DROP PRIMARY KEY"#,
            r#"ALTER TABLE `t` DROP PRIMARY KEY"#,
        ),
        (
            r#"ALTER TABLE t drop index a"#,
            r#"ALTER TABLE `t` DROP INDEX `a`"#,
        ),
        (
            r#"ALTER TABLE t drop key a"#,
            r#"ALTER TABLE `t` DROP INDEX `a`"#,
        ),
        (
            r#"ALTER TABLE t drop FOREIGN key a"#,
            r#"ALTER TABLE `t` DROP FOREIGN KEY `a`"#,
        ),
        (
            r#"ALTER TABLE t MODIFY column a varchar(255)"#,
            r#"ALTER TABLE `t` MODIFY COLUMN `a` VARCHAR(255)"#,
        ),
        (
            r#"ALTER TABLE t modify COLUMN a varchar(255) FIRST"#,
            r#"ALTER TABLE `t` MODIFY COLUMN `a` VARCHAR(255) FIRST"#,
        ),
        (
            r#"ALTER TABLE t modify COLUMN a varchar(255) AFTER b"#,
            r#"ALTER TABLE `t` MODIFY COLUMN `a` VARCHAR(255) AFTER `b`"#,
        ),
        (
            r#"ALTER TABLE t change column a b VARCHAR(255)"#,
            r#"ALTER TABLE `t` CHANGE COLUMN `a` `b` VARCHAR(255)"#,
        ),
        (
            r#"ALTER TABLE t change COLUMN a b varchar(255) CHARACTER SET UTF8 BINARY"#,
            r#"ALTER TABLE `t` CHANGE COLUMN `a` `b` VARCHAR(255) BINARY CHARACTER SET UTF8"#,
        ),
        (
            r#"ALTER TABLE t CHANGE column a b varchar(255) FIRST"#,
            r#"ALTER TABLE `t` CHANGE COLUMN `a` `b` VARCHAR(255) FIRST"#,
        ),
        (
            r#"ALTER TABLE t change COLUMN a b varchar(255) AFTER c"#,
            r#"ALTER TABLE `t` CHANGE COLUMN `a` `b` VARCHAR(255) AFTER `c`"#,
        ),
        (
            r#"ALTER TABLE t RENAME db1.t1"#,
            r#"ALTER TABLE `t` RENAME AS `db1`.`t1`"#,
        ),
        (
            r#"ALTER TABLE t RENAME to db1.t1"#,
            r#"ALTER TABLE `t` RENAME AS `db1`.`t1`"#,
        ),
        (
            r#"ALTER TABLE t RENAME as t1"#,
            r#"ALTER TABLE `t` RENAME AS `t1`"#,
        ),
        (
            r#"ALTER TABLE t ALTER a SET DEFAULT 1"#,
            r#"ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT 1"#,
        ),
        (
            r#"ALTER TABLE t ALTER a DROP DEFAULT"#,
            r#"ALTER TABLE `t` ALTER COLUMN `a` DROP DEFAULT"#,
        ),
        (
            r#"ALTER TABLE t ALTER COLUMN a SET DEFAULT 1"#,
            r#"ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT 1"#,
        ),
        (
            r#"ALTER TABLE t ALTER COLUMN a DROP DEFAULT"#,
            r#"ALTER TABLE `t` ALTER COLUMN `a` DROP DEFAULT"#,
        ),
        (
            r#"ALTER TABLE t LOCK=NONE"#,
            r#"ALTER TABLE `t` LOCK = NONE"#,
        ),
        (
            r#"ALTER TABLE t LOCK=DEFAULT"#,
            r#"ALTER TABLE `t` LOCK = DEFAULT"#,
        ),
        (
            r#"ALTER TABLE t LOCK=SHARED"#,
            r#"ALTER TABLE `t` LOCK = SHARED"#,
        ),
        (
            r#"ALTER TABLE t LOCK=EXCLUSIVE"#,
            r#"ALTER TABLE `t` LOCK = EXCLUSIVE"#,
        ),
        (
            r#"ALTER TABLE t RENAME KEY a TO b"#,
            r#"ALTER TABLE `t` RENAME INDEX `a` TO `b`"#,
        ),
        (
            r#"ALTER TABLE t RENAME INDEX a TO b"#,
            r#"ALTER TABLE `t` RENAME INDEX `a` TO `b`"#,
        ),
        (
            r#"ALTER TABLE t ADD PARTITION"#,
            r#"ALTER TABLE `t` ADD PARTITION"#,
        ),
        (
            r#"ALTER TABLE t ADD PARTITION ( PARTITION P1 VALUES LESS THAN (2010))"#,
            r#"ALTER TABLE `t` ADD PARTITION (PARTITION `P1` VALUES LESS THAN (2010))"#,
        ),
        (
            r#"ALTER TABLE t ADD PARTITION ( PARTITION P2 VALUES LESS THAN MAXVALUE)"#,
            r#"ALTER TABLE `t` ADD PARTITION (PARTITION `P2` VALUES LESS THAN (MAXVALUE))"#,
        ),
        (
            r#"ALTER TABLE t ADD PARTITION (
PARTITION P1 VALUES LESS THAN (2010),
PARTITION P2 VALUES LESS THAN (2015),
PARTITION P3 VALUES LESS THAN MAXVALUE)"#,
            r#"ALTER TABLE `t` ADD PARTITION (PARTITION `P1` VALUES LESS THAN (2010), PARTITION `P2` VALUES LESS THAN (2015), PARTITION `P3` VALUES LESS THAN (MAXVALUE))"#,
        ),
        (
            r#"ALTER TABLE t ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT 'AP_START \' AP_END')"#,
            r#"ALTER TABLE `t` ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'AP_START '' AP_END')"#,
        ),
        (
            r#"ALTER TABLE t ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'xxx')"#,
            r#"ALTER TABLE `t` ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'xxx')"#,
        ),
        (
            r#"ALTER TABLE t coalesce partition 3"#,
            r#"ALTER TABLE `t` COALESCE PARTITION 3"#,
        ),
        (
            r#"ALTER TABLE t drop partition p1"#,
            r#"ALTER TABLE `t` DROP PARTITION `p1`"#,
        ),
        (
            r#"ALTER TABLE t TRUNCATE PARTITION p0"#,
            r#"ALTER TABLE `t` TRUNCATE PARTITION `p0`"#,
        ),
        (
            r#"ALTER TABLE t add stats_extended s1 cardinality(a,b)"#,
            r#"ALTER TABLE `t` ADD STATS_EXTENDED `s1` CARDINALITY(`a`, `b`)"#,
        ),
        (
            r#"ALTER TABLE t add stats_extended if not exists s1 cardinality(a,b)"#,
            r#"ALTER TABLE `t` ADD STATS_EXTENDED IF NOT EXISTS `s1` CARDINALITY(`a`, `b`)"#,
        ),
        (
            r#"ALTER TABLE t add stats_extended s1 correlation(a,b)"#,
            r#"ALTER TABLE `t` ADD STATS_EXTENDED `s1` CORRELATION(`a`, `b`)"#,
        ),
        (
            r#"ALTER TABLE t add stats_extended if not exists s1 correlation(a,b)"#,
            r#"ALTER TABLE `t` ADD STATS_EXTENDED IF NOT EXISTS `s1` CORRELATION(`a`, `b`)"#,
        ),
        (
            r#"ALTER TABLE t add stats_extended s1 dependency(a,b)"#,
            r#"ALTER TABLE `t` ADD STATS_EXTENDED `s1` DEPENDENCY(`a`, `b`)"#,
        ),
        (
            r#"ALTER TABLE t add stats_extended if not exists s1 dependency(a,b)"#,
            r#"ALTER TABLE `t` ADD STATS_EXTENDED IF NOT EXISTS `s1` DEPENDENCY(`a`, `b`)"#,
        ),
        (
            r#"ALTER TABLE t drop stats_extended s1"#,
            r#"ALTER TABLE `t` DROP STATS_EXTENDED `s1`"#,
        ),
        (
            r#"ALTER TABLE t drop stats_extended if exists s1"#,
            r#"ALTER TABLE `t` DROP STATS_EXTENDED IF EXISTS `s1`"#,
        ),
        (
            r#"ALTER TABLE t placement policy p1"#,
            r#"ALTER TABLE `t` PLACEMENT POLICY = `p1`"#,
        ),
        (
            r#"ALTER TABLE t placement policy p1 comment='aaa'"#,
            r#"ALTER TABLE `t` PLACEMENT POLICY = `p1` COMMENT = 'aaa'"#,
        ),
        (
            r#"ALTER TABLE t partition p0 placement policy p1"#,
            r#"ALTER TABLE `t` PARTITION `p0` PLACEMENT POLICY = `p1`"#,
        ),
    ] {
        let restored = parse(sql)
            .unwrap_or_else(|error| panic!("source SQL: {sql}: {error:?}"))
            .restore();
        assert_eq!(restored, expected, "source SQL: {sql}");
    }
}

#[test]
fn ddl_column_def_restore_transcreates_every_original_go_row() {
    let rows = [
        // for type
        ("id json", "`id` JSON"),
        ("id time(5)", "`id` TIME(5)"),
        ("id int(5) unsigned", "`id` INT(5) UNSIGNED"),
        (
            "id int(5) UNSIGNED ZEROFILL",
            "`id` INT(5) UNSIGNED ZEROFILL",
        ),
        ("id float(12,3)", "`id` FLOAT(12,3)"),
        ("id float", "`id` FLOAT"),
        ("id double(22,3)", "`id` DOUBLE(22,3)"),
        ("id double", "`id` DOUBLE"),
        ("id tinyint(4)", "`id` TINYINT(4)"),
        ("id smallint(6)", "`id` SMALLINT(6)"),
        ("id mediumint(9)", "`id` MEDIUMINT(9)"),
        ("id integer(11)", "`id` INT(11)"),
        ("id bigint(20)", "`id` BIGINT(20)"),
        ("id DATE", "`id` DATE"),
        ("id DATETIME", "`id` DATETIME"),
        ("id DECIMAL(4,2)", "`id` DECIMAL(4,2)"),
        ("id char(1)", "`id` CHAR(1)"),
        ("id varchar(10) BINARY", "`id` VARCHAR(10) BINARY"),
        ("id binary(1)", "`id` BINARY(1)"),
        ("id timestamp(2)", "`id` TIMESTAMP(2)"),
        ("id timestamp", "`id` TIMESTAMP"),
        ("id datetime(2)", "`id` DATETIME(2)"),
        ("id date", "`id` DATE"),
        ("id year", "`id` YEAR"),
        ("id INT", "`id` INT"),
        ("id INT NULL", "`id` INT NULL"),
        ("id enum('a','b')", "`id` ENUM('a','b')"),
        ("id enum('''a''','''b''')", "`id` ENUM('''a''','''b''')"),
        (
            "id enum('a\\nb','a\\tb','a\\rb')",
            "`id` ENUM('a\nb','a\tb','a\rb')",
        ),
        ("id enum('a','b') binary", "`id` ENUM('a','b') BINARY"),
        ("id enum(0x61, 0b01100010)", "`id` ENUM('a','b')"),
        ("id set('a','b')", "`id` SET('a','b')"),
        ("id set('''a''','''b''')", "`id` SET('''a''','''b''')"),
        (
            "id set('a\\nb','a''	\\r\\nb','a\\rb')",
            "`id` SET('a\nb','a''	\r\nb','a\rb')",
        ),
        (
            r#"id set("a'\nb","a'b\tc")"#,
            "`id` SET('a''\nb','a''b\tc')",
        ),
        ("id set('a','b') binary", "`id` SET('a','b') BINARY"),
        ("id set(0x61, 0b01100010)", "`id` SET('a','b')"),
        (
            "id TEXT CHARACTER SET UTF8 COLLATE UTF8_UNICODE_CI",
            "`id` TEXT CHARACTER SET UTF8 COLLATE utf8_unicode_ci",
        ),
        ("id text character set UTF8", "`id` TEXT CHARACTER SET UTF8"),
        ("id text charset UTF8", "`id` TEXT CHARACTER SET UTF8"),
        (
            "id varchar(50) collate UTF8MB4_CZECH_CI",
            "`id` VARCHAR(50) COLLATE utf8mb4_czech_ci",
        ),
        (
            "id varchar(50) collate utf8_bin",
            "`id` VARCHAR(50) COLLATE utf8_bin",
        ),
        (
            "c1 char(10) character set LATIN1 collate latin1_german1_ci",
            "`c1` CHAR(10) CHARACTER SET LATIN1 COLLATE latin1_german1_ci",
        ),
        ("id int(11) PRIMARY KEY", "`id` INT(11) PRIMARY KEY"),
        ("id int(11) NOT NULL", "`id` INT(11) NOT NULL"),
        ("id INT(11) NULL", "`id` INT(11) NULL"),
        ("id INT(11) auto_increment", "`id` INT(11) AUTO_INCREMENT"),
        ("id INT(11) DEFAULT 10", "`id` INT(11) DEFAULT 10"),
        (
            "id INT(11) DEFAULT '10'",
            "`id` INT(11) DEFAULT _UTF8MB4'10'",
        ),
        ("id INT(11) DEFAULT 1.1", "`id` INT(11) DEFAULT 1.1"),
        ("id INT(11) UNIQUE KEY", "`id` INT(11) UNIQUE KEY"),
        (
            "id INT(11) COLLATE ascii_bin",
            "`id` INT(11) COLLATE ascii_bin",
        ),
        (
            "id INT(11) on update CURRENT_TIMESTAMP",
            "`id` INT(11) ON UPDATE CURRENT_TIMESTAMP()",
        ),
        ("id INT(11) comment 'hello'", "`id` INT(11) COMMENT 'hello'"),
        (
            "id INT(11) generated always as(id + 1)",
            "`id` INT(11) GENERATED ALWAYS AS(`id`+1) VIRTUAL",
        ),
        (
            "id INT(11) REFERENCES parent(id)",
            "`id` INT(11) REFERENCES `parent`(`id`)",
        ),
        ("id bit", "`id` BIT(1)"),
        ("id bit(1)", "`id` BIT(1)"),
        ("id bit(64)", "`id` BIT(64)"),
        ("id tinyint", "`id` TINYINT"),
        ("id tinyint(255)", "`id` TINYINT(255)"),
        ("id bool", "`id` TINYINT(1)"),
        ("id boolean", "`id` TINYINT(1)"),
        ("id smallint", "`id` SMALLINT"),
        ("id smallint(255)", "`id` SMALLINT(255)"),
        ("id mediumint", "`id` MEDIUMINT"),
        ("id mediumint(255)", "`id` MEDIUMINT(255)"),
        ("id int", "`id` INT"),
        ("id int(255)", "`id` INT(255)"),
        ("id integer", "`id` INT"),
        ("id integer(255)", "`id` INT(255)"),
        ("id bigint", "`id` BIGINT"),
        ("id bigint(255)", "`id` BIGINT(255)"),
        ("id decimal", "`id` DECIMAL"),
        ("id decimal(10)", "`id` DECIMAL(10)"),
        ("id decimal(10,0)", "`id` DECIMAL(10,0)"),
        ("id decimal(65)", "`id` DECIMAL(65)"),
        ("id decimal(65,30)", "`id` DECIMAL(65,30)"),
        ("id dec(10,0)", "`id` DECIMAL(10,0)"),
        ("id numeric(10,0)", "`id` DECIMAL(10,0)"),
        ("id float(0)", "`id` FLOAT"),
        ("id float(24)", "`id` FLOAT"),
        ("id float(25)", "`id` DOUBLE"),
        ("id float(53)", "`id` DOUBLE"),
        ("id float(7,0)", "`id` FLOAT(7,0)"),
        ("id float(25,0)", "`id` FLOAT(25,0)"),
        ("id double(15,0)", "`id` DOUBLE(15,0)"),
        ("id double precision(15,0)", "`id` DOUBLE(15,0)"),
        ("id real(15,0)", "`id` DOUBLE(15,0)"),
        ("id year(4)", "`id` YEAR(4)"),
        ("id time", "`id` TIME"),
        ("id char", "`id` CHAR"),
        ("id char(0)", "`id` CHAR(0)"),
        ("id char(255)", "`id` CHAR(255)"),
        ("id national char(0)", "`id` CHAR(0)"),
        ("id binary", "`id` BINARY"),
        ("id varbinary(0)", "`id` VARBINARY(0)"),
        ("id varbinary(65535)", "`id` VARBINARY(65535)"),
        ("id tinyblob", "`id` TINYBLOB"),
        ("id tinytext", "`id` TINYTEXT"),
        ("id blob", "`id` BLOB"),
        ("id blob(0)", "`id` BLOB(0)"),
        ("id blob(65535)", "`id` BLOB(65535)"),
        ("id text(0)", "`id` TEXT(0)"),
        ("id text(65535)", "`id` TEXT(65535)"),
        ("id mediumblob", "`id` MEDIUMBLOB"),
        ("id mediumtext", "`id` MEDIUMTEXT"),
        ("id longblob", "`id` LONGBLOB"),
        ("id longtext", "`id` LONGTEXT"),
        ("id json", "`id` JSON"),
    ];
    assert_eq!(
        rows.len(),
        110,
        "TestDDLColumnDefRestore source-row count drifted"
    );
    for (source, expected_column) in rows {
        let sql = format!("CREATE TABLE t ({source})");
        let restored = parse(&sql)
            .unwrap_or_else(|error| panic!("source column definition: {source}: {error:?}"))
            .restore();
        assert_eq!(
            restored,
            format!("CREATE TABLE `t` ({expected_column})"),
            "source column definition: {source}"
        );
    }
}

#[test]
fn if_exists_restore_transcreates_every_original_go_row() {
    use tidb_ast::RestoreFlags;

    let rows = [
        (
            "drop index if exists idx on t",
            "DROP INDEX IF EXISTS `idx` ON `t`",
            "DROP INDEX /*T! IF EXISTS  */`idx` ON `t`",
        ),
        (
            "create unique index if not exists idx on t(c)",
            "CREATE UNIQUE INDEX IF NOT EXISTS `idx` ON `t` (`c`)",
            "CREATE UNIQUE INDEX /*T! IF NOT EXISTS  */`idx` ON `t` (`c`)",
        ),
        (
            "alter table t add column if not exists c int",
            "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS `c` INT",
            "ALTER TABLE `t` ADD COLUMN /*T! IF NOT EXISTS  */`c` INT",
        ),
        (
            "alter table t drop column if exists c",
            "ALTER TABLE `t` DROP COLUMN IF EXISTS `c`",
            "ALTER TABLE `t` DROP COLUMN /*T! IF EXISTS  */`c`",
        ),
        (
            "alter table t add key if not exists idx2(c2), add vector index if not exists idx3(c3), add columnar index if not exists idx4(c4)",
            "ALTER TABLE `t` ADD INDEX IF NOT EXISTS `idx2`(`c2`), ADD VECTOR INDEX IF NOT EXISTS `idx3`(`c3`), ADD COLUMNAR INDEX IF NOT EXISTS `idx4`(`c4`)",
            "ALTER TABLE `t` ADD INDEX/*T!  IF NOT EXISTS */ `idx2`(`c2`), ADD VECTOR INDEX/*T!  IF NOT EXISTS */ `idx3`(`c3`), ADD COLUMNAR INDEX/*T!  IF NOT EXISTS */ `idx4`(`c4`)",
        ),
        (
            "alter table t add foreign key if not exists fk(c) references t2(c)",
            "ALTER TABLE `t` ADD CONSTRAINT `fk` FOREIGN KEY IF NOT EXISTS (`c`) REFERENCES `t2`(`c`)",
            "ALTER TABLE `t` ADD CONSTRAINT `fk` FOREIGN KEY /*T! IF NOT EXISTS  */(`c`) REFERENCES `t2`(`c`)",
        ),
        (
            "alter table t drop index if exists idx",
            "ALTER TABLE `t` DROP INDEX IF EXISTS `idx`",
            "ALTER TABLE `t` DROP INDEX /*T! IF EXISTS  */`idx`",
        ),
        (
            "alter table t change column if exists c c2 int",
            "ALTER TABLE `t` CHANGE COLUMN IF EXISTS `c` `c2` INT",
            "ALTER TABLE `t` CHANGE COLUMN /*T! IF EXISTS  */`c` `c2` INT",
        ),
        (
            "alter table t modify column if exists c int",
            "ALTER TABLE `t` MODIFY COLUMN IF EXISTS `c` INT",
            "ALTER TABLE `t` MODIFY COLUMN /*T! IF EXISTS  */`c` INT",
        ),
        (
            "alter table t add partition if not exists (partition p1 values less than (10))",
            "ALTER TABLE `t` ADD PARTITION IF NOT EXISTS (PARTITION `p1` VALUES LESS THAN (10))",
            "ALTER TABLE `t` ADD PARTITION/*T!  IF NOT EXISTS */ (PARTITION `p1` VALUES LESS THAN (10))",
        ),
        (
            "alter table t drop partition if exists p1, p2",
            "ALTER TABLE `t` DROP PARTITION IF EXISTS `p1`,`p2`",
            "ALTER TABLE `t` DROP PARTITION /*T! IF EXISTS  */`p1`,`p2`",
        ),
    ];
    assert_eq!(
        rows.len(),
        11,
        "TestIfExistsRestore source-row count drifted"
    );
    for (sql, expected_normal, expected_special) in rows {
        let statement = parse(sql).unwrap_or_else(|error| panic!("source SQL: {sql}: {error:?}"));
        assert_eq!(statement.restore(), expected_normal, "source SQL: {sql}");
        assert_eq!(
            statement
                .restore_with_flags(RestoreFlags::DEFAULT | RestoreFlags::TIDB_SPECIAL_COMMENT),
            expected_special,
            "source SQL: {sql}"
        );
    }
}
