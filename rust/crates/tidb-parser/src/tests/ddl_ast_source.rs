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
