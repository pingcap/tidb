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

//! Source-order transcreation of Go `RunTest` tables in `pkg/parser/parser_test.go`.
//!
//! The two largest tables live in sibling files: `parser_run_test_ddl_source`
//! and `parser_run_test_builtin_source`.

use crate::parser_run_test_helper::run_cases;

fn test_dbastmt_cases_0() {
    run_cases(&[
        (
            "SHOW VARIABLES LIKE 'character_set_results'",
            true,
            "SHOW SESSION VARIABLES LIKE _UTF8MB4'character_set_results'",
        ),
        (
            "SHOW GLOBAL VARIABLES LIKE 'character_set_results'",
            true,
            "SHOW GLOBAL VARIABLES LIKE _UTF8MB4'character_set_results'",
        ),
        (
            "SHOW SESSION VARIABLES LIKE 'character_set_results'",
            true,
            "SHOW SESSION VARIABLES LIKE _UTF8MB4'character_set_results'",
        ),
        ("SHOW VARIABLES", true, "SHOW SESSION VARIABLES"),
        ("SHOW GLOBAL VARIABLES", true, "SHOW GLOBAL VARIABLES"),
        (
            "SHOW GLOBAL VARIABLES WHERE Variable_name = 'autocommit'",
            true,
            "SHOW GLOBAL VARIABLES WHERE `Variable_name`=_UTF8MB4'autocommit'",
        ),
        ("SHOW STATUS", true, "SHOW SESSION STATUS"),
        ("SHOW GLOBAL STATUS", true, "SHOW GLOBAL STATUS"),
        ("SHOW SESSION STATUS", true, "SHOW SESSION STATUS"),
        (
            "SHOW STATUS LIKE 'Up%'",
            true,
            "SHOW SESSION STATUS LIKE _UTF8MB4'Up%'",
        ),
        (
            "SHOW STATUS WHERE Variable_name",
            true,
            "SHOW SESSION STATUS WHERE `Variable_name`",
        ),
        (
            "SHOW STATUS WHERE Variable_name LIKE 'Up%'",
            true,
            "SHOW SESSION STATUS WHERE `Variable_name` LIKE _UTF8MB4'Up%'",
        ),
        (
            "SHOW FULL TABLES FROM icar_qa LIKE play_evolutions",
            true,
            "SHOW FULL TABLES IN `icar_qa` LIKE `play_evolutions`",
        ),
        (
            "SHOW FULL TABLES WHERE Table_Type != 'VIEW'",
            true,
            "SHOW FULL TABLES WHERE `Table_Type`!=_UTF8MB4'VIEW'",
        ),
        ("SHOW GRANTS", true, "SHOW GRANTS"),
        (
            "SHOW GRANTS FOR 'test'@'localhost'",
            true,
            "SHOW GRANTS FOR `test`@`localhost`",
        ),
        (
            "SHOW GRANTS FOR 'test'@'LOCALHOST'",
            true,
            "SHOW GRANTS FOR `test`@`localhost`",
        ),
        (
            "SHOW GRANTS FOR current_user()",
            true,
            "SHOW GRANTS FOR CURRENT_USER",
        ),
        (
            "SHOW GRANTS FOR current_user",
            true,
            "SHOW GRANTS FOR CURRENT_USER",
        ),
        (
            "SHOW GRANTS FOR 'u1'@'localhost' USING 'r1'",
            true,
            "SHOW GRANTS FOR `u1`@`localhost` USING `r1`@`%`",
        ),
        (
            "SHOW GRANTS FOR 'u1'@'localhost' USING 'r1', 'r2'",
            true,
            "SHOW GRANTS FOR `u1`@`localhost` USING `r1`@`%`, `r2`@`%`",
        ),
        ("SHOW COLUMNS FROM City;", true, "SHOW COLUMNS IN `City`"),
        (
            "SHOW COLUMNS FROM tv189.1_t_1_x;",
            true,
            "SHOW COLUMNS IN `tv189`.`1_t_1_x`",
        ),
        ("SHOW FIELDS FROM City;", true, "SHOW COLUMNS IN `City`"),
        (
            "SHOW TRIGGERS LIKE 't'",
            true,
            "SHOW TRIGGERS LIKE _UTF8MB4't'",
        ),
        (
            "SHOW DATABASES LIKE 'test2'",
            true,
            "SHOW DATABASES LIKE _UTF8MB4'test2'",
        ),
        (
            "SHOW PROCEDURE STATUS WHERE Db='test'",
            true,
            "SHOW PROCEDURE STATUS WHERE `Db`=_UTF8MB4'test'",
        ),
        (
            "SHOW FUNCTION STATUS WHERE Db='test'",
            true,
            "SHOW FUNCTION STATUS WHERE `Db`=_UTF8MB4'test'",
        ),
        ("SHOW INDEX FROM t;", true, "SHOW INDEX IN `t`"),
        ("SHOW KEYS FROM t;", true, "SHOW INDEX IN `t`"),
        ("SHOW INDEX IN t;", true, "SHOW INDEX IN `t`"),
        ("SHOW KEYS IN t;", true, "SHOW INDEX IN `t`"),
        (
            "SHOW INDEXES IN t where true;",
            true,
            "SHOW INDEX IN `t` WHERE TRUE",
        ),
        (
            "SHOW KEYS FROM t FROM test where true;",
            true,
            "SHOW INDEX IN `test`.`t` WHERE TRUE",
        ),
        (
            "SHOW EVENTS FROM test_db WHERE definer = 'current_user'",
            true,
            "SHOW EVENTS IN `test_db` WHERE `definer`=_UTF8MB4'current_user'",
        ),
        ("SHOW PLUGINS", true, "SHOW PLUGINS"),
        (
            "SHOW PLUGINS LIKE 'Validate%'",
            true,
            "SHOW PLUGINS LIKE _UTF8MB4'Validate%'",
        ),
        ("SHOW PROFILES", true, "SHOW PROFILES"),
        ("SHOW PROFILE", true, "SHOW PROFILE"),
        ("SHOW PROFILE FOR QUERY 1", true, "SHOW PROFILE FOR QUERY 1"),
    ]);
}

fn test_dbastmt_cases_1() {
    run_cases(&[
        ("SHOW PROFILE CPU FOR QUERY 2", true, "SHOW PROFILE CPU FOR QUERY 2"),
        ("SHOW PROFILE CPU FOR QUERY 2 LIMIT 1,1", true, "SHOW PROFILE CPU FOR QUERY 2 LIMIT 1,1"),
        ("SHOW PROFILE CPU, MEMORY, BLOCK IO, CONTEXT SWITCHES, PAGE FAULTS, IPC, SWAPS, SOURCE FOR QUERY 1 limit 100", true, "SHOW PROFILE CPU, MEMORY, BLOCK IO, CONTEXT SWITCHES, PAGE FAULTS, IPC, SWAPS, SOURCE FOR QUERY 1 LIMIT 100"),
        ("SHOW MASTER STATUS", true, "SHOW MASTER STATUS"),
        ("SHOW BINARY LOG STATUS", true, "SHOW BINARY LOG STATUS"),
        ("SHOW PRIVILEGES", true, "SHOW PRIVILEGES"),
        ("show character set;", true, "SHOW CHARSET"),
        ("show charset", true, "SHOW CHARSET"),
        ("show collation", true, "SHOW COLLATION"),
        ("show collation like 'utf8%'", true, "SHOW COLLATION LIKE _UTF8MB4'utf8%'"),
        ("show collation where Charset = 'utf8' and Collation = 'utf8_bin'", true, "SHOW COLLATION WHERE `Charset`=_UTF8MB4'utf8' AND `Collation`=_UTF8MB4'utf8_bin'"),
        ("show columns in t;", true, "SHOW COLUMNS IN `t`"),
        ("show full columns in t;", true, "SHOW FULL COLUMNS IN `t`"),
        ("SHOW COLUMNS FROM City;", true, "SHOW COLUMNS IN `City`"),
        ("SHOW EXTENDED COLUMNS FROM City;", true, "SHOW EXTENDED COLUMNS IN `City`"),
        ("SHOW EXTENDED FIELDS FROM City;", true, "SHOW EXTENDED COLUMNS IN `City`"),
        ("SHOW EXTENDED FULL COLUMNS FROM City;", true, "SHOW EXTENDED FULL COLUMNS IN `City`"),
        ("SHOW EXTENDED FULL FIELDS FROM City;", true, "SHOW EXTENDED FULL COLUMNS IN `City`"),
        ("show create table test.t", true, "SHOW CREATE TABLE `test`.`t`"),
        ("show create table t", true, "SHOW CREATE TABLE `t`"),
        ("show create view test.t", true, "SHOW CREATE VIEW `test`.`t`"),
        ("show create view t", true, "SHOW CREATE VIEW `t`"),
        ("show create database d1", true, "SHOW CREATE DATABASE `d1`"),
        ("show create database if not exists d1", true, "SHOW CREATE DATABASE IF NOT EXISTS `d1`"),
        ("show create sequence seq", true, "SHOW CREATE SEQUENCE `seq`"),
        ("show create sequence test.seq", true, "SHOW CREATE SEQUENCE `test`.`seq`"),
        ("show stats_extended", true, "SHOW STATS_EXTENDED"),
        ("show stats_extended where table_name = 't'", true, "SHOW STATS_EXTENDED WHERE `table_name`=_UTF8MB4't'"),
        ("show stats_meta", true, "SHOW STATS_META"),
        ("show stats_meta where table_name = 't'", true, "SHOW STATS_META WHERE `table_name`=_UTF8MB4't'"),
        ("show stats_locked", true, "SHOW STATS_LOCKED"),
        ("show stats_locked where table_name = 't'", true, "SHOW STATS_LOCKED WHERE `table_name`=_UTF8MB4't'"),
        ("show stats_histograms", true, "SHOW STATS_HISTOGRAMS"),
        ("show stats_histograms where col_name = 'a'", true, "SHOW STATS_HISTOGRAMS WHERE `col_name`=_UTF8MB4'a'"),
        ("show stats_buckets", true, "SHOW STATS_BUCKETS"),
        ("show stats_buckets where col_name = 'a'", true, "SHOW STATS_BUCKETS WHERE `col_name`=_UTF8MB4'a'"),
        ("show stats_healthy", true, "SHOW STATS_HEALTHY"),
        ("show stats_healthy where table_name = 't'", true, "SHOW STATS_HEALTHY WHERE `table_name`=_UTF8MB4't'"),
        ("show stats_topn", true, "SHOW STATS_TOPN"),
        ("show stats_topn where table_name = 't'", true, "SHOW STATS_TOPN WHERE `table_name`=_UTF8MB4't'"),
    ]);
}

fn test_dbastmt_cases_2() {
    run_cases(&[
        (
            "show histograms_in_flight",
            true,
            "SHOW HISTOGRAMS_IN_FLIGHT",
        ),
        ("show column_stats_usage", true, "SHOW COLUMN_STATS_USAGE"),
        (
            "show column_stats_usage where table_name = 't'",
            true,
            "SHOW COLUMN_STATS_USAGE WHERE `table_name`=_UTF8MB4't'",
        ),
        (
            "show binding_cache status",
            true,
            "SHOW BINDING_CACHE STATUS",
        ),
        ("show analyze status", true, "SHOW ANALYZE STATUS"),
        (
            "show analyze status where table_name = 't'",
            true,
            "SHOW ANALYZE STATUS WHERE `table_name`=_UTF8MB4't'",
        ),
        (
            "show analyze status where table_name like '%'",
            true,
            "SHOW ANALYZE STATUS WHERE `table_name` LIKE _UTF8MB4'%'",
        ),
        ("show builtins", true, "SHOW BUILTINS"),
        ("show backups", true, "SHOW BACKUPS"),
        (
            "show restores like 'r0001'",
            true,
            "SHOW RESTORES LIKE _UTF8MB4'r0001'",
        ),
        (
            "show backups where start_time > now() - interval 10 hour",
            true,
            "SHOW BACKUPS WHERE `start_time`>DATE_SUB(NOW(), INTERVAL 10 HOUR)",
        ),
        ("show backup", false, ""),
        ("show restore", false, ""),
        ("show replica status", true, "SHOW REPLICA STATUS"),
        ("show slave status", true, "SHOW REPLICA STATUS"),
        (
            "load stats '/tmp/stats.json'",
            true,
            "LOAD STATS '/tmp/stats.json'",
        ),
        ("lock stats test.t", true, "LOCK STATS `test`.`t`"),
        ("lock stats t, t2", true, "LOCK STATS `t`, `t2`"),
        (
            "lock stats t partition (p0, p1)",
            true,
            "LOCK STATS `t` PARTITION(`p0`, `p1`)",
        ),
        (
            "lock stats t partition p0",
            true,
            "LOCK STATS `t` PARTITION(`p0`)",
        ),
        (
            "lock stats t partition p0, p1",
            true,
            "LOCK STATS `t` PARTITION(`p0`, `p1`)",
        ),
        ("unlock stats test.t", true, "UNLOCK STATS `test`.`t`"),
        ("unlock stats t, t2", true, "UNLOCK STATS `t`, `t2`"),
        (
            "unlock stats t partition (p0, p1)",
            true,
            "UNLOCK STATS `t` PARTITION(`p0`, `p1`)",
        ),
        (
            "unlock stats t partition p0",
            true,
            "UNLOCK STATS `t` PARTITION(`p0`)",
        ),
        (
            "unlock stats t partition p0, p1",
            true,
            "UNLOCK STATS `t` PARTITION(`p0`, `p1`)",
        ),
        ("SET @ = 1", true, "SET @``=1"),
        ("SET @' ' = 1", true, "SET @` `=1"),
        ("SET @! = 1", false, ""),
        ("SET @1 = 1", true, "SET @`1`=1"),
        ("SET @a = 1", true, "SET @`a`=1"),
        ("SET @b := 1", true, "SET @`b`=1"),
        ("SET @.c = 1", true, "SET @`.c`=1"),
        ("SET @_d = 1", true, "SET @`_d`=1"),
        ("SET @_e._$. = 1", true, "SET @`_e._$.`=1"),
        ("SET @~f = 1", false, ""),
        ("SET @`g,` = 1", true, "SET @`g,`=1"),
        ("SET", false, ""),
        ("SET @a = 1, @b := 2", true, "SET @`a`=1, @`b`=2"),
        (
            "SET SESSION autocommit = 1",
            true,
            "SET @@SESSION.`autocommit`=1",
        ),
    ]);
}

fn test_dbastmt_cases_3() {
    run_cases(&[
        (
            "SET @@session.autocommit = 1",
            true,
            "SET @@SESSION.`autocommit`=1",
        ),
        (
            "SET @@SESSION.autocommit = 1",
            true,
            "SET @@SESSION.`autocommit`=1",
        ),
        (
            "SET @@GLOBAL.GTID_PURGED = '123'",
            true,
            "SET @@GLOBAL.`gtid_purged`=_UTF8MB4'123'",
        ),
        (
            "SET @MYSQLDUMP_TEMP_LOG_BIN = @@SESSION.SQL_LOG_BIN",
            true,
            "SET @`MYSQLDUMP_TEMP_LOG_BIN`=@@SESSION.`sql_log_bin`",
        ),
        (
            "SET LOCAL autocommit = 1",
            true,
            "SET @@SESSION.`autocommit`=1",
        ),
        (
            "SET @@local.autocommit = 1",
            true,
            "SET @@SESSION.`autocommit`=1",
        ),
        ("SET @@autocommit = 1", true, "SET @@SESSION.`autocommit`=1"),
        ("SET autocommit = 1", true, "SET @@SESSION.`autocommit`=1"),
        (
            "SET GLOBAL autocommit = 1",
            true,
            "SET @@GLOBAL.`autocommit`=1",
        ),
        (
            "SET @@global.autocommit = 1",
            true,
            "SET @@GLOBAL.`autocommit`=1",
        ),
        ("SET autocommit := 1", true, "SET @@SESSION.`autocommit`=1"),
        (
            "SET @@session.autocommit := 1",
            true,
            "SET @@SESSION.`autocommit`=1",
        ),
        (
            "SET @MYSQLDUMP_TEMP_LOG_BIN := @@SESSION.SQL_LOG_BIN",
            true,
            "SET @`MYSQLDUMP_TEMP_LOG_BIN`=@@SESSION.`sql_log_bin`",
        ),
        (
            "SET LOCAL autocommit := 1",
            true,
            "SET @@SESSION.`autocommit`=1",
        ),
        (
            "SET @@global.autocommit := default",
            true,
            "SET @@GLOBAL.`autocommit`=DEFAULT",
        ),
        (
            "SET @@global.autocommit = default",
            true,
            "SET @@GLOBAL.`autocommit`=DEFAULT",
        ),
        (
            "SET @@session.autocommit = default",
            true,
            "SET @@SESSION.`autocommit`=DEFAULT",
        ),
        (
            "SET @@character_set_results = binary",
            true,
            "SET @@SESSION.`character_set_results`=_UTF8MB4'BINARY'",
        ),
        ("SET CHARACTER SET utf8mb4;", true, "SET CHARSET 'utf8mb4'"),
        (
            "SET CHARACTER SET 'utf8mb4';",
            true,
            "SET CHARSET 'utf8mb4'",
        ),
        (
            "SET PASSWORD = 'password';",
            true,
            "SET PASSWORD='password'",
        ),
        (
            "SET PASSWORD FOR 'root'@'localhost' = 'password';",
            true,
            "SET PASSWORD FOR `root`@`localhost`='password'",
        ),
        (
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            true,
            "SET @@SESSION.`tx_isolation`=_UTF8MB4'REPEATABLE-READ'",
        ),
        (
            "SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            true,
            "SET @@GLOBAL.`tx_isolation`=_UTF8MB4'REPEATABLE-READ'",
        ),
        (
            "SET SESSION TRANSACTION READ WRITE",
            true,
            "SET @@SESSION.`tx_read_only`=_UTF8MB4'0'",
        ),
        (
            "SET SESSION TRANSACTION READ ONLY",
            true,
            "SET @@SESSION.`tx_read_only`=_UTF8MB4'1'",
        ),
        (
            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
            true,
            "SET @@SESSION.`tx_isolation`=_UTF8MB4'READ-COMMITTED'",
        ),
        (
            "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            true,
            "SET @@SESSION.`tx_isolation`=_UTF8MB4'READ-UNCOMMITTED'",
        ),
        (
            "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            true,
            "SET @@SESSION.`tx_isolation`=_UTF8MB4'SERIALIZABLE'",
        ),
        (
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            true,
            "SET @@SESSION.`tx_isolation_one_shot`=_UTF8MB4'REPEATABLE-READ'",
        ),
        (
            "SET TRANSACTION READ WRITE",
            true,
            "SET @@SESSION.`tx_read_only`=_UTF8MB4'0'",
        ),
        (
            "SET TRANSACTION READ ONLY",
            true,
            "SET @@SESSION.`tx_read_only`=_UTF8MB4'1'",
        ),
        (
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            true,
            "SET @@SESSION.`tx_isolation_one_shot`=_UTF8MB4'READ-COMMITTED'",
        ),
        (
            "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            true,
            "SET @@SESSION.`tx_isolation_one_shot`=_UTF8MB4'READ-UNCOMMITTED'",
        ),
        (
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            true,
            "SET @@SESSION.`tx_isolation_one_shot`=_UTF8MB4'SERIALIZABLE'",
        ),
        ("set names utf8", true, "SET NAMES 'utf8'"),
        (
            "set names utf8 collate utf8_unicode_ci",
            true,
            "SET NAMES 'utf8' COLLATE 'utf8_unicode_ci'",
        ),
        ("set names binary", true, "SET NAMES 'binary'"),
        ("set names default", true, "SET NAMES DEFAULT"),
        ("set character set default", true, "SET CHARSET DEFAULT"),
    ]);
}

fn test_dbastmt_cases_4() {
    run_cases(&[
        ("set charset default", true, "SET CHARSET DEFAULT"),
        ("set char set default", true, "SET CHARSET DEFAULT"),
        ("set role `role1`", true, "SET ROLE `role1`@`%`"),
        ("SET ROLE DEFAULT", true, "SET ROLE DEFAULT"),
        ("SET ROLE ALL", true, "SET ROLE ALL"),
        (
            "SET ROLE ALL EXCEPT `role1`, `role2`",
            true,
            "SET ROLE ALL EXCEPT `role1`@`%`, `role2`@`%`",
        ),
        (
            "SET DEFAULT ROLE administrator, developer TO `joe`@`10.0.0.1`",
            true,
            "SET DEFAULT ROLE `administrator`@`%`, `developer`@`%` TO `joe`@`10.0.0.1`",
        ),
        (
            "set names utf8, @@session.sql_mode=1;",
            true,
            "SET NAMES 'utf8', @@SESSION.`sql_mode`=1",
        ),
        (
            "set @@session.sql_mode=1, names utf8, charset utf8;",
            true,
            "SET @@SESSION.`sql_mode`=1, NAMES 'utf8', CHARSET 'utf8'",
        ),
        (
            "set config TIKV LOG.LEVEL='info'",
            true,
            "SET CONFIG TIKV LOG.LEVEL = _UTF8MB4'info'",
        ),
        (
            "set config PD LOG.LEVEL='info'",
            true,
            "SET CONFIG PD LOG.LEVEL = _UTF8MB4'info'",
        ),
        (
            "set config TIDB LOG.LEVEL='info'",
            true,
            "SET CONFIG TIDB LOG.LEVEL = _UTF8MB4'info'",
        ),
        (
            "set config '127.0.0.1:3306' LOG.LEVEL='info'",
            true,
            "SET CONFIG '127.0.0.1:3306' LOG.LEVEL = _UTF8MB4'info'",
        ),
        (
            "set config '127.0.0.1:3306' AUTO-COMPACTION-MODE=TRUE",
            true,
            "SET CONFIG '127.0.0.1:3306' AUTO-COMPACTION-MODE = TRUE",
        ),
        (
            "set config '127.0.0.1:3306' LABEL-PROPERTY.REJECT-LEADER.KEY='zone'",
            true,
            "SET CONFIG '127.0.0.1:3306' LABEL-PROPERTY.REJECT-LEADER.KEY = _UTF8MB4'zone'",
        ),
        ("show config", true, "SHOW CONFIG"),
        (
            "show config where type='tidb'",
            true,
            "SHOW CONFIG WHERE `type`=_UTF8MB4'tidb'",
        ),
        (
            "show config where instance='127.0.0.1:3306'",
            true,
            "SHOW CONFIG WHERE `instance`=_UTF8MB4'127.0.0.1:3306'",
        ),
        (
            "create table CONFIG (a int)",
            true,
            "CREATE TABLE `CONFIG` (`a` INT)",
        ),
        (
            "flush no_write_to_binlog tables tbl1 with read lock",
            true,
            "FLUSH NO_WRITE_TO_BINLOG TABLES `tbl1` WITH READ LOCK",
        ),
        ("flush table", true, "FLUSH TABLES"),
        ("flush tables", true, "FLUSH TABLES"),
        ("flush tables tbl1", true, "FLUSH TABLES `tbl1`"),
        (
            "flush no_write_to_binlog tables tbl1",
            true,
            "FLUSH NO_WRITE_TO_BINLOG TABLES `tbl1`",
        ),
        (
            "flush local tables tbl1",
            true,
            "FLUSH NO_WRITE_TO_BINLOG TABLES `tbl1`",
        ),
        (
            "flush table with read lock",
            true,
            "FLUSH TABLES WITH READ LOCK",
        ),
        (
            "flush tables tbl1, tbl2, tbl3",
            true,
            "FLUSH TABLES `tbl1`, `tbl2`, `tbl3`",
        ),
        (
            "flush tables tbl1, tbl2, tbl3 with read lock",
            true,
            "FLUSH TABLES `tbl1`, `tbl2`, `tbl3` WITH READ LOCK",
        ),
        ("flush privileges", true, "FLUSH PRIVILEGES"),
        ("flush status", true, "FLUSH STATUS"),
        (
            "flush tidb plugins plugin1",
            true,
            "FLUSH TIDB PLUGINS plugin1",
        ),
        (
            "flush tidb plugins plugin1, plugin2",
            true,
            "FLUSH TIDB PLUGINS plugin1, plugin2",
        ),
        ("flush hosts", true, "FLUSH HOSTS"),
        ("flush logs", true, "FLUSH LOGS"),
        ("flush binary logs", true, "FLUSH BINARY LOGS"),
        ("flush engine logs", true, "FLUSH ENGINE LOGS"),
        ("flush error logs", true, "FLUSH ERROR LOGS"),
        ("flush general logs", true, "FLUSH GENERAL LOGS"),
        ("flush slow logs", true, "FLUSH SLOW LOGS"),
        (
            "flush client_errors_summary",
            true,
            "FLUSH CLIENT_ERRORS_SUMMARY",
        ),
    ]);
}

fn test_dbastmt_cases_5() {
    run_cases(&[
        ("flush stats_delta", false, ""),
        (
            "flush stats_delta cluster",
            true,
            "FLUSH STATS_DELTA `cluster`",
        ),
        (
            "flush stats_delta cluster cluster",
            true,
            "FLUSH STATS_DELTA `cluster` CLUSTER",
        ),
        ("flush stats_delta *.*", true, "FLUSH STATS_DELTA *.*"),
        (
            "flush stats_delta *.* cluster",
            true,
            "FLUSH STATS_DELTA *.* CLUSTER",
        ),
        ("flush stats_delta db1.*", true, "FLUSH STATS_DELTA `db1`.*"),
        (
            "flush stats_delta db1.* cluster",
            true,
            "FLUSH STATS_DELTA `db1`.* CLUSTER",
        ),
        ("flush stats_delta t1", true, "FLUSH STATS_DELTA `t1`"),
        (
            "flush stats_delta db1.t1",
            true,
            "FLUSH STATS_DELTA `db1`.`t1`",
        ),
        (
            "flush stats_delta db1.t1 cluster",
            true,
            "FLUSH STATS_DELTA `db1`.`t1` CLUSTER",
        ),
        (
            "flush stats_delta db1.t1, db2.*",
            true,
            "FLUSH STATS_DELTA `db1`.`t1`, `db2`.*",
        ),
        (
            "flush stats_delta db1.t1, db2.* cluster",
            true,
            "FLUSH STATS_DELTA `db1`.`t1`, `db2`.* CLUSTER",
        ),
        ("call ", false, ""),
        ("call test", true, "CALL `test`()"),
        ("call test()", true, "CALL `test`()"),
        (
            "call test(1, 'test', true)",
            true,
            "CALL `test`(1, _UTF8MB4'test', TRUE)",
        ),
        ("call x.y;", true, "CALL `x`.`y`()"),
        ("call x.y();", true, "CALL `x`.`y`()"),
        (
            "call x.y('p', 'q', 'r');",
            true,
            "CALL `x`.`y`(_UTF8MB4'p', _UTF8MB4'q', _UTF8MB4'r')",
        ),
        ("call `x`.`y`;", true, "CALL `x`.`y`()"),
        ("call `x`.`y`();", true, "CALL `x`.`y`()"),
        (
            "call `x`.`y`('p', 'q', 'r');",
            true,
            "CALL `x`.`y`(_UTF8MB4'p', _UTF8MB4'q', _UTF8MB4'r')",
        ),
    ]);
}

fn test_expression_cases_0() {
    run_cases(&[
        ("SELECT ++1", true, "SELECT ++1"),
        ("SELECT -*1", false, "SELECT -*1"),
        ("SELECT -+1", true, "SELECT -+1"),
        ("SELECT -1", true, "SELECT -1"),
        ("SELECT --1", true, "SELECT --1"),
        (
            "select '''a''', \"\"\"a\"\"\"",
            true,
            "SELECT _UTF8MB4'''a''',_UTF8MB4'\"a\"'",
        ),
        ("select ''a''", false, ""),
        ("select \"\"a\"\"", false, ""),
        ("select '''a''';", true, "SELECT _UTF8MB4'''a'''"),
        ("select '\\'a\\'';", true, "SELECT _UTF8MB4'''a'''"),
        ("select \"\\\"a\\\"\";", true, "SELECT _UTF8MB4'\"a\"'"),
        ("select \"\"\"a\"\"\";", true, "SELECT _UTF8MB4'\"a\"'"),
        ("select _utf8\"string\";", true, "SELECT _UTF8'string'"),
        ("select _binary\"string\";", true, "SELECT _BINARY'string'"),
        ("select N'string'", true, "SELECT _UTF8'string'"),
        ("select n'string'", true, "SELECT _UTF8'string'"),
        ("select _utf8 0xD0B1;", true, "SELECT _UTF8 x'd0b1'"),
        ("select _utf8 X'D0B1';", true, "SELECT _UTF8 x'd0b1'"),
        (
            "select _utf8 0b1101000010110001;",
            true,
            "SELECT _UTF8 b'1101000010110001'",
        ),
        (
            "select _utf8 B'1101000010110001';",
            true,
            "SELECT _UTF8 b'1101000010110001'",
        ),
        (
            "select 1 <=> 0, 1 <=> null, 1 = null",
            true,
            "SELECT 1<=>0,1<=>NULL,1=NULL",
        ),
        ("select date'1989-09-10'", true, "SELECT DATE '1989-09-10'"),
        ("select date 19890910", false, ""),
        (
            "select time '00:00:00.111'",
            true,
            "SELECT TIME '00:00:00.111'",
        ),
        ("select time 19890910", false, ""),
        (
            "select timestamp '1989-09-10 11:11:11'",
            true,
            "SELECT TIMESTAMP '1989-09-10 11:11:11'",
        ),
        ("select timestamp 19890910", false, ""),
        (
            "select {ts '1989-09-10 11:11:11'}",
            true,
            "SELECT TIMESTAMP '1989-09-10 11:11:11'",
        ),
        ("select {d '1989-09-10'}", true, "SELECT DATE '1989-09-10'"),
        (
            "select {t '00:00:00.111'}",
            true,
            "SELECT TIME '00:00:00.111'",
        ),
        (
            "select * from t where a > {ts '1989-09-10 11:11:11'}",
            true,
            "SELECT * FROM `t` WHERE `a`>TIMESTAMP '1989-09-10 11:11:11'",
        ),
        (
            "select * from t where a > {ts {abc '1989-09-10 11:11:11'}}",
            true,
            "SELECT * FROM `t` WHERE `a`>TIMESTAMP '1989-09-10 11:11:11'",
        ),
        (
            "select {ts123 '1989-09-10 11:11:11'}",
            true,
            "SELECT _UTF8MB4'1989-09-10 11:11:11'",
        ),
        ("select {ts123 123}", true, "SELECT 123"),
        ("select {ts123 1 xor 1}", true, "SELECT 1 XOR 1"),
        (
            "select * from t where a > {ts123 '1989-09-10 11:11:11'}",
            true,
            "SELECT * FROM `t` WHERE `a`>_UTF8MB4'1989-09-10 11:11:11'",
        ),
        ("select .t.a from t", false, ""),
    ]);
}

fn test_identifier_cases_0() {
    run_cases(&[
        (
            "select `a`, `a.b`, `a b` from t",
            true,
            "SELECT `a`,`a.b`,`a b` FROM `t`",
        ),
        (
            "create table MergeContextTest$Simple (value integer not null, primary key (value))",
            true,
            "CREATE TABLE `MergeContextTest$Simple` (`value` INT NOT NULL,PRIMARY KEY(`value`))",
        ),
        (
            "select 1 as a, 1 as `a`, 1 as \"a\", 1 as 'a'",
            true,
            "SELECT 1 AS `a`,1 AS `a`,1 AS `a`,1 AS `a`",
        ),
        (
            "select 1 as a, 1 as \"a\", 1 as 'a'",
            true,
            "SELECT 1 AS `a`,1 AS `a`,1 AS `a`",
        ),
        (
            "select 1 a, 1 \"a\", 1 'a'",
            true,
            "SELECT 1 AS `a`,1 AS `a`,1 AS `a`",
        ),
        ("select * from t as \"a\"", false, ""),
        ("select * from t a", true, "SELECT * FROM `t` AS `a`"),
        ("select * from ROW", false, ""),
        ("select COUNT from DESC", false, ""),
        (
            "select COUNT from SELECT.DESC",
            true,
            "SELECT `COUNT` FROM `SELECT`.`DESC`",
        ),
        ("use `select`", true, "USE `select`"),
        ("use `sel``ect`", true, "USE `sel``ect`"),
        ("use select", false, "USE `select`"),
        ("select * from t as a", true, "SELECT * FROM `t` AS `a`"),
        ("select 1 full, 1 row, 1 abs", false, ""),
        (
            "select 1 full, 1 `row`, 1 abs",
            true,
            "SELECT 1 AS `full`,1 AS `row`,1 AS `abs`",
        ),
        ("select * from t full, t1 row, t2 abs", false, ""),
        (
            "select * from t full, t1 `row`, t2 abs",
            true,
            "SELECT * FROM ((`t` AS `full`) JOIN `t1` AS `row`) JOIN `t2` AS `abs`",
        ),
        ("create database 123test", true, "CREATE DATABASE `123test`"),
        ("create database 123", false, "CREATE DATABASE `123`"),
        ("create database `123`", true, "CREATE DATABASE `123`"),
        ("create database `12``3`", true, "CREATE DATABASE `12``3`"),
        (
            "create table `123` (123a1 int)",
            true,
            "CREATE TABLE `123` (`123a1` INT)",
        ),
        ("create table 123 (123a1 int)", false, ""),
        ("select .78+123", true, "SELECT 0.78+123"),
        ("select .78+.21", true, "SELECT 0.78+0.21"),
        ("select .78-123", true, "SELECT 0.78-123"),
        ("select .78-.21", true, "SELECT 0.78-0.21"),
        ("select .78--123", true, "SELECT 0.78--123"),
        ("select .78*123", true, "SELECT 0.78*123"),
        ("select .78*.21", true, "SELECT 0.78*0.21"),
        ("select .78/123", true, "SELECT 0.78/123"),
        ("select .78/.21", true, "SELECT 0.78/0.21"),
        ("select .78,123", true, "SELECT 0.78,123"),
        ("select .78,.21", true, "SELECT 0.78,0.21"),
        ("select .78 , 123", true, "SELECT 0.78,123"),
        ("select .78.123", false, ""),
        ("select .78#123", true, "SELECT 0.78"),
        (
            "insert float_test values(.67, 'string');",
            true,
            "INSERT INTO `float_test` VALUES (0.67,_UTF8MB4'string')",
        ),
        ("select .78'123'", true, "SELECT 0.78 AS `123`"),
    ]);
}

fn test_identifier_cases_1() {
    run_cases(&[
        ("select .78`123`", true, "SELECT 0.78 AS `123`"),
        ("select .78\"123\"", true, "SELECT 0.78 AS `123`"),
        ("select 111 as ��", true, "SELECT 111 AS `??`"),
    ]);
}

fn test_type_cases_0() {
    run_cases(&[
        ("CREATE TABLE t( c1 TIME(2), c2 DATETIME(2), c3 TIMESTAMP(2) );", true, "CREATE TABLE `t` (`c1` TIME(2),`c2` DATETIME(2),`c3` TIMESTAMP(2))"),
        ("select x'0a', X'11', 0x11", true, "SELECT x'0a',x'11',x'11'"),
        ("select x'13181C76734725455A'", true, "SELECT x'13181c76734725455a'"),
        ("select x'0xaa'", false, ""),
        ("select 0X11", false, ""),
        ("select 0x4920616D2061206C6F6E672068657820737472696E67", true, "SELECT x'4920616d2061206c6f6e672068657820737472696e67'"),
        ("select 0b01, 0b0, b'11', B'11'", true, "SELECT b'1',b'0',b'11',b'11'"),
        ("create table t (c1 enum('a', 'b'), c2 set('a', 'b'))", true, "CREATE TABLE `t` (`c1` ENUM('a','b'),`c2` SET('a','b'))"),
        ("create table t (c1 enum('a  ', 'b\t'), c2 set('a  ', 'b\t'))", true, "CREATE TABLE `t` (`c1` ENUM('a','b\t'),`c2` SET('a','b\t'))"),
        ("create table t (c1 enum('a', 'b') binary, c2 set('a', 'b') binary)", true, "CREATE TABLE `t` (`c1` ENUM('a','b') BINARY,`c2` SET('a','b') BINARY)"),
        ("create table t (c1 enum(0x61, 'b'), c2 set(0x61, 'b'))", true, "CREATE TABLE `t` (`c1` ENUM('a','b'),`c2` SET('a','b'))"),
        ("create table t (c1 enum(0b01100001, 'b'), c2 set(0b01100001, 'b'))", true, "CREATE TABLE `t` (`c1` ENUM('a','b'),`c2` SET('a','b'))"),
        ("create table t (c1 enum)", false, ""),
        ("create table t (c1 set)", false, ""),
        ("create table t (c1 blob(1024), c2 text(1024))", true, "CREATE TABLE `t` (`c1` BLOB(1024),`c2` TEXT(1024))"),
        ("create table t (y year(4), y1 year)", true, "CREATE TABLE `t` (`y` YEAR(4),`y1` YEAR)"),
        ("create table t (y year(4) unsigned zerofill zerofill, y1 year signed unsigned zerofill)", true, "CREATE TABLE `t` (`y` YEAR(4),`y1` YEAR)"),
        ("create table t (c1 national char(2), c2 national varchar(2))", true, "CREATE TABLE `t` (`c1` CHAR(2),`c2` VARCHAR(2))"),
        ("create table t (a JSON);", true, "CREATE TABLE `t` (`a` JSON)"),
    ]);
}

fn test_privilege_cases_0() {
    run_cases(&[
        ("CREATE USER 'ttt' REQUIRE X509;", true, "CREATE USER `ttt`@`%` REQUIRE X509"),
        ("CREATE USER 'ttt' REQUIRE SSL;", true, "CREATE USER `ttt`@`%` REQUIRE SSL"),
        ("CREATE USER 'ttt' REQUIRE NONE;", true, "CREATE USER `ttt`@`%` REQUIRE NONE"),
        ("CREATE USER 'ttt' REQUIRE ISSUER '/C=SE/ST=Stockholm/L=Stockholm/O=MySQL/CN=CA/emailAddress=ca@example.com' AND CIPHER 'EDH-RSA-DES-CBC3-SHA';", true, "CREATE USER `ttt`@`%` REQUIRE ISSUER '/C=SE/ST=Stockholm/L=Stockholm/O=MySQL/CN=CA/emailAddress=ca@example.com' AND CIPHER 'EDH-RSA-DES-CBC3-SHA'"),
        ("CREATE USER 'ttt' REQUIRE ISSUER '/C=SE/ST=Stockholm/L=Stockholm/O=MySQL/CN=CA/emailAddress=ca@example.com' CIPHER 'EDH-RSA-DES-CBC3-SHA' SUBJECT '/C=SE/ST=Stockholm/L=Stockholm/O=MySQL/CN=CA/emailAddress=ca@example.com';", true, "CREATE USER `ttt`@`%` REQUIRE ISSUER '/C=SE/ST=Stockholm/L=Stockholm/O=MySQL/CN=CA/emailAddress=ca@example.com' AND CIPHER 'EDH-RSA-DES-CBC3-SHA' AND SUBJECT '/C=SE/ST=Stockholm/L=Stockholm/O=MySQL/CN=CA/emailAddress=ca@example.com'"),
        ("CREATE USER 'ttt' REQUIRE SAN 'DNS:mysql-user, URI:spiffe://example.org/myservice'", true, "CREATE USER `ttt`@`%` REQUIRE SAN 'DNS:mysql-user, URI:spiffe://example.org/myservice'"),
        ("CREATE USER 'ttt' WITH MAX_QUERIES_PER_HOUR 2;", true, "CREATE USER `ttt`@`%` WITH MAX_QUERIES_PER_HOUR 2"),
        ("CREATE USER 'ttt'@'localhost' REQUIRE NONE WITH MAX_QUERIES_PER_HOUR 1 MAX_UPDATES_PER_HOUR 10 PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK;", true, "CREATE USER `ttt`@`localhost` REQUIRE NONE WITH MAX_QUERIES_PER_HOUR 1 MAX_UPDATES_PER_HOUR 10 PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"),
        ("CREATE USER 'u1'@'%' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK ;", true, "CREATE USER `u1`@`%` IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"),
        ("CREATE USER 'test'", true, "CREATE USER `test`@`%`"),
        ("CREATE USER test", true, "CREATE USER `test`@`%`"),
        ("CREATE USER `test`", true, "CREATE USER `test`@`%`"),
        ("CREATE USER test-user", false, ""),
        ("CREATE USER test.user", false, ""),
        ("CREATE USER 'test-user'", true, "CREATE USER `test-user`@`%`"),
        ("CREATE USER `test-user`", true, "CREATE USER `test-user`@`%`"),
        ("CREATE USER test.user", false, ""),
        ("CREATE USER 'test.user'", true, "CREATE USER `test.user`@`%`"),
        ("CREATE USER `test.user`", true, "CREATE USER `test.user`@`%`"),
        ("CREATE USER uesr1@LOCALhost", true, "CREATE USER `uesr1`@`localhost`"),
        ("CREATE USER `uesr1`@localhost", true, "CREATE USER `uesr1`@`localhost`"),
        ("CREATE USER uesr1@`localhost`", true, "CREATE USER `uesr1`@`localhost`"),
        ("CREATE USER `uesr1`@`localhost`", true, "CREATE USER `uesr1`@`localhost`"),
        ("CREATE USER 'uesr1'@localhost", true, "CREATE USER `uesr1`@`localhost`"),
        ("CREATE USER uesr1@'localhost'", true, "CREATE USER `uesr1`@`localhost`"),
        ("CREATE USER 'uesr1'@'localhost'", true, "CREATE USER `uesr1`@`localhost`"),
        ("CREATE USER 'uesr1'@`localhost`", true, "CREATE USER `uesr1`@`localhost`"),
        ("CREATE USER `uesr1`@'localhost'", true, "CREATE USER `uesr1`@`localhost`"),
        ("create user 'test@localhost' password expire;", true, "CREATE USER `test@localhost`@`%` PASSWORD EXPIRE"),
        ("create user 'test@localhost' password expire never;", true, "CREATE USER `test@localhost`@`%` PASSWORD EXPIRE NEVER"),
        ("create user 'test@localhost' password expire default;", true, "CREATE USER `test@localhost`@`%` PASSWORD EXPIRE DEFAULT"),
        ("create user 'test@localhost' password expire interval 3 day;", true, "CREATE USER `test@localhost`@`%` PASSWORD EXPIRE INTERVAL 3 DAY"),
        ("create user 'test@localhost' identified by 'password' failed_login_attempts 3 password_lock_time 3;", true, "CREATE USER `test@localhost`@`%` IDENTIFIED BY 'password' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 3"),
        ("create user 'test@localhost' identified by 'password' failed_login_attempts 3 password_lock_time unbounded;", true, "CREATE USER `test@localhost`@`%` IDENTIFIED BY 'password' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME UNBOUNDED"),
        ("create user 'test@localhost' identified by 'password' failed_login_attempts 3;", true, "CREATE USER `test@localhost`@`%` IDENTIFIED BY 'password' FAILED_LOGIN_ATTEMPTS 3"),
        ("create user 'test@localhost' identified by 'password' password_lock_time 3;", true, "CREATE USER `test@localhost`@`%` IDENTIFIED BY 'password' PASSWORD_LOCK_TIME 3"),
        ("create user 'test@localhost' identified by 'password' password_lock_time unbounded;", true, "CREATE USER `test@localhost`@`%` IDENTIFIED BY 'password' PASSWORD_LOCK_TIME UNBOUNDED"),
        ("CREATE USER 'sha_test'@'localhost' IDENTIFIED WITH 'caching_sha2_password' BY 'sha_test'", true, "CREATE USER `sha_test`@`localhost` IDENTIFIED WITH 'caching_sha2_password' BY 'sha_test'"),
        ("CREATE USER 'sha_test3'@'localhost' IDENTIFIED WITH 'caching_sha2_password' AS 0x24412430303524255B03496C662C1055127B3B654A2F04207D01485276703644704B76303247474564416A516662346C5868646D32764C6B514F43585A473779565947514F34", true, "CREATE USER `sha_test3`@`localhost` IDENTIFIED WITH 'caching_sha2_password' AS '$A$005$%[\u{0003}Ilf,\u{0010}U\u{0012}{;eJ/\u{0004} }\u{0001}HRvp6DpKv02GGEdAjQfb4lXhdm2vLkQOCXZG7yVYGQO4'"),
        ("CREATE USER 'sha_test4'@'localhost' IDENTIFIED WITH 'caching_sha2_password' AS '$A$005$%[\u{0003}Ilf,\u{0010}U\u{0012}{;eJ/\u{0004} }\u{0001}HRvp6DpKv02GGEdAjQfb4lXhdm2vLkQOCXZG7yVYGQO4'", true, "CREATE USER `sha_test4`@`localhost` IDENTIFIED WITH 'caching_sha2_password' AS '$A$005$%[\u{0003}Ilf,\u{0010}U\u{0012}{;eJ/\u{0004} }\u{0001}HRvp6DpKv02GGEdAjQfb4lXhdm2vLkQOCXZG7yVYGQO4'"),
    ]);
}

fn test_privilege_cases_1() {
    run_cases(&[
        ("CREATE USER `user@pingcap.com`@'localhost' IDENTIFIED WITH 'tidb_auth_token' REQUIRE token_issuer 'issuer-abc' ATTRIBUTE '{\"email\": \"user@pingcap.com\"}'", true, "CREATE USER `user@pingcap.com`@`localhost` IDENTIFIED WITH 'tidb_auth_token' REQUIRE TOKEN_ISSUER 'issuer-abc' ATTRIBUTE '{\"email\": \"user@pingcap.com\"}'"),
        ("CREATE USER 'nopwd_native'@'localhost' IDENTIFIED WITH 'mysql_native_password'", true, "CREATE USER `nopwd_native`@`localhost` IDENTIFIED WITH 'mysql_native_password'"),
        ("CREATE USER 'nopwd_sha'@'localhost' IDENTIFIED WITH 'caching_sha2_password'", true, "CREATE USER `nopwd_sha`@`localhost` IDENTIFIED WITH 'caching_sha2_password'"),
        ("CREATE ROLE `test-role`, `role1`@'localhost'", true, "CREATE ROLE `test-role`@`%`, `role1`@`localhost`"),
        ("CREATE ROLE `test-role`", true, "CREATE ROLE `test-role`@`%`"),
        ("CREATE ROLE role1", true, "CREATE ROLE `role1`@`%`"),
        ("CREATE ROLE `role1`@'localhost'", true, "CREATE ROLE `role1`@`localhost`"),
        ("create user 'bug19354014user'@'%' identified WITH mysql_native_password", true, "CREATE USER `bug19354014user`@`%` IDENTIFIED WITH 'mysql_native_password'"),
        ("create user 'bug19354014user'@'%' identified WITH mysql_native_password by 'new-password'", true, "CREATE USER `bug19354014user`@`%` IDENTIFIED WITH 'mysql_native_password' BY 'new-password'"),
        ("create user 'bug19354014user'@'%' identified WITH mysql_native_password as 'hashstring'", true, "CREATE USER `bug19354014user`@`%` IDENTIFIED WITH 'mysql_native_password' AS 'hashstring'"),
        ("CREATE USER IF NOT EXISTS 'root'@'localhost' IDENTIFIED BY 'new-password'", true, "CREATE USER IF NOT EXISTS `root`@`localhost` IDENTIFIED BY 'new-password'"),
        ("CREATE USER 'root'@'localhost' IDENTIFIED BY 'new-password'", true, "CREATE USER `root`@`localhost` IDENTIFIED BY 'new-password'"),
        ("CREATE USER 'root'@'localhost' IDENTIFIED BY PASSWORD 'hashstring'", true, "CREATE USER `root`@`localhost` IDENTIFIED WITH 'mysql_native_password' AS 'hashstring'"),
        ("CREATE USER 'root'@'localhost' IDENTIFIED BY 'new-password', 'root'@'127.0.0.1' IDENTIFIED BY PASSWORD 'hashstring'", true, "CREATE USER `root`@`localhost` IDENTIFIED BY 'new-password', `root`@`127.0.0.1` IDENTIFIED WITH 'mysql_native_password' AS 'hashstring'"),
        ("CREATE USER 'root'@'127.0.0.1' IDENTIFIED BY 'hashstring' RESOURCE GROUP rg1", true, "CREATE USER `root`@`127.0.0.1` IDENTIFIED BY 'hashstring' RESOURCE GROUP `rg1`"),
        ("ALTER USER IF EXISTS 'root'@'localhost' IDENTIFIED BY 'new-password'", true, "ALTER USER IF EXISTS `root`@`localhost` IDENTIFIED BY 'new-password'"),
        ("ALTER USER 'root'@'localhost' IDENTIFIED BY 'new-password'", true, "ALTER USER `root`@`localhost` IDENTIFIED BY 'new-password'"),
        ("ALTER USER 'root'@'localhost' RESOURCE GROUP rg2", true, "ALTER USER `root`@`localhost` RESOURCE GROUP `rg2`"),
        ("ALTER USER 'root'@'localhost' IDENTIFIED BY PASSWORD 'hashstring'", true, "ALTER USER `root`@`localhost` IDENTIFIED WITH 'mysql_native_password' AS 'hashstring'"),
        ("ALTER USER 'root'@'localhost' IDENTIFIED BY 'new-password', 'root'@'127.0.0.1' IDENTIFIED BY PASSWORD 'hashstring'", true, "ALTER USER `root`@`localhost` IDENTIFIED BY 'new-password', `root`@`127.0.0.1` IDENTIFIED WITH 'mysql_native_password' AS 'hashstring'"),
        ("ALTER USER USER() IDENTIFIED BY 'new-password'", true, "ALTER USER USER() IDENTIFIED BY 'new-password'"),
        ("ALTER USER IF EXISTS USER() IDENTIFIED BY 'new-password'", true, "ALTER USER IF EXISTS USER() IDENTIFIED BY 'new-password'"),
        ("ALTER USER USER() IDENTIFIED BY PASSWORD '*B50FBDB37F1256824274912F2A1CE648082C3F1F'", false, ""),
        ("alter user 'test@localhost' password expire;", true, "ALTER USER `test@localhost`@`%` PASSWORD EXPIRE"),
        ("alter user 'test@localhost' password expire never;", true, "ALTER USER `test@localhost`@`%` PASSWORD EXPIRE NEVER"),
        ("alter user 'test@localhost' password expire default;", true, "ALTER USER `test@localhost`@`%` PASSWORD EXPIRE DEFAULT"),
        ("alter user 'test@localhost' password expire interval 3 day;", true, "ALTER USER `test@localhost`@`%` PASSWORD EXPIRE INTERVAL 3 DAY"),
        ("ALTER USER 'ttt' REQUIRE X509;", true, "ALTER USER `ttt`@`%` REQUIRE X509"),
        ("ALTER USER 'ttt' REQUIRE SSL;", true, "ALTER USER `ttt`@`%` REQUIRE SSL"),
        ("ALTER USER 'ttt' REQUIRE NONE;", true, "ALTER USER `ttt`@`%` REQUIRE NONE"),
        ("ALTER USER 'ttt' REQUIRE ISSUER '/C=SE/ST=Stockholm/L=Stockholm/O=MySQL/CN=CA/emailAddress=ca@example.com' AND CIPHER 'EDH-RSA-DES-CBC3-SHA';", true, "ALTER USER `ttt`@`%` REQUIRE ISSUER '/C=SE/ST=Stockholm/L=Stockholm/O=MySQL/CN=CA/emailAddress=ca@example.com' AND CIPHER 'EDH-RSA-DES-CBC3-SHA'"),
        ("ALTER USER 'ttt' WITH MAX_QUERIES_PER_HOUR 2;", true, "ALTER USER `ttt`@`%` WITH MAX_QUERIES_PER_HOUR 2"),
        ("ALTER USER 'ttt' WITH MAX_UPDATES_PER_HOUR 2;", true, "ALTER USER `ttt`@`%` WITH MAX_UPDATES_PER_HOUR 2"),
        ("ALTER USER 'ttt' WITH MAX_CONNECTIONS_PER_HOUR 2;", true, "ALTER USER `ttt`@`%` WITH MAX_CONNECTIONS_PER_HOUR 2"),
        ("ALTER USER 'ttt' WITH MAX_USER_CONNECTIONS 2;", true, "ALTER USER `ttt`@`%` WITH MAX_USER_CONNECTIONS 2"),
        ("ALTER USER 'ttt'@'localhost' REQUIRE NONE WITH MAX_QUERIES_PER_HOUR 1 MAX_UPDATES_PER_HOUR 10 PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK;", true, "ALTER USER `ttt`@`localhost` REQUIRE NONE WITH MAX_QUERIES_PER_HOUR 1 MAX_UPDATES_PER_HOUR 10 PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"),
        ("ALTER USER 'u1'@'%' IDENTIFIED BY 'new' RETAIN CURRENT PASSWORD", true, "ALTER USER `u1`@`%` IDENTIFIED BY 'new' RETAIN CURRENT PASSWORD"),
        ("ALTER USER 'u1'@'%' IDENTIFIED WITH 'mysql_native_password' BY 'new' RETAIN CURRENT PASSWORD", true, "ALTER USER `u1`@`%` IDENTIFIED WITH 'mysql_native_password' BY 'new' RETAIN CURRENT PASSWORD"),
        ("ALTER USER 'u1'@'%' IDENTIFIED BY 'p2', 'u2'@'%' IDENTIFIED BY 'q2' RETAIN CURRENT PASSWORD", true, "ALTER USER `u1`@`%` IDENTIFIED BY 'p2', `u2`@`%` IDENTIFIED BY 'q2' RETAIN CURRENT PASSWORD"),
        ("ALTER USER 'u1'@'%' IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD, 'u2'@'%' IDENTIFIED BY 'q2'", true, "ALTER USER `u1`@`%` IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD, `u2`@`%` IDENTIFIED BY 'q2'"),
    ]);
}

fn test_privilege_cases_2() {
    run_cases(&[
        ("ALTER USER 'u1'@'%' DISCARD OLD PASSWORD", true, "ALTER USER `u1`@`%` DISCARD OLD PASSWORD"),
        ("ALTER USER 'u1'@'%' DISCARD OLD PASSWORD, 'u2'@'%'", true, "ALTER USER `u1`@`%` DISCARD OLD PASSWORD, `u2`@`%`"),
        ("SET PASSWORD = 'new' RETAIN CURRENT PASSWORD", true, "SET PASSWORD='new' RETAIN CURRENT PASSWORD"),
        ("SET PASSWORD FOR 'u1'@'%' = 'new' RETAIN CURRENT PASSWORD", true, "SET PASSWORD FOR `u1`@`%`='new' RETAIN CURRENT PASSWORD"),
        ("ALTER USER 'u1'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*B50FBDB37F1256824274912F2A1CE648082C3F1F' RETAIN CURRENT PASSWORD", false, ""),
        ("ALTER USER 'u1'@'%' RETAIN CURRENT PASSWORD", false, ""),
        ("ALTER USER 'u1'@'%' IDENTIFIED WITH 'mysql_native_password' RETAIN CURRENT PASSWORD", false, ""),
        ("CREATE USER 'u1'@'%' IDENTIFIED BY 'p1' RETAIN CURRENT PASSWORD", false, ""),
        ("CREATE USER 'u1'@'%' DISCARD OLD PASSWORD", false, ""),
        ("ALTER USER 'u1'@'%' IDENTIFIED BY 'p1' DISCARD OLD PASSWORD", false, ""),
        ("ALTER USER USER() IDENTIFIED BY 'p1' RETAIN CURRENT PASSWORD", true, "ALTER USER USER() IDENTIFIED BY 'p1' RETAIN CURRENT PASSWORD"),
        ("ALTER USER USER() DISCARD OLD PASSWORD", true, "ALTER USER USER() DISCARD OLD PASSWORD"),
        ("ALTER USER IF EXISTS USER() IDENTIFIED BY 'p1' RETAIN CURRENT PASSWORD", true, "ALTER USER IF EXISTS USER() IDENTIFIED BY 'p1' RETAIN CURRENT PASSWORD"),
        ("DROP USER 'root'@'localhost', 'root1'@'localhost'", true, "DROP USER `root`@`localhost`, `root1`@`localhost`"),
        ("DROP USER IF EXISTS 'root'@'localhost'", true, "DROP USER IF EXISTS `root`@`localhost`"),
        ("RENAME USER 'root'@'localhost' TO 'root'@'%'", true, "RENAME USER `root`@`localhost` TO `root`@`%`"),
        ("RENAME USER 'fred' TO 'barry'", true, "RENAME USER `fred`@`%` TO `barry`@`%`"),
        ("RENAME USER u1 to u2, u3 to u4", true, "RENAME USER `u1`@`%` TO `u2`@`%`, `u3`@`%` TO `u4`@`%`"),
        ("DROP ROLE 'role'@'localhost', 'role1'@'localhost'", true, "DROP ROLE `role`@`localhost`, `role1`@`localhost`"),
        ("DROP ROLE 'administrator', 'developer';", true, "DROP ROLE `administrator`@`%`, `developer`@`%`"),
        ("DROP ROLE IF EXISTS 'role'@'localhost'", true, "DROP ROLE IF EXISTS `role`@`localhost`"),
        ("GRANT ALL ON db1.* TO 'jeffrey'@'localhost' REQUIRE X509;", true, "GRANT ALL ON `db1`.* TO `jeffrey`@`localhost` REQUIRE X509"),
        ("GRANT ALL ON db1.* TO 'jeffrey'@'LOCALhost' REQUIRE SSL;", true, "GRANT ALL ON `db1`.* TO `jeffrey`@`localhost` REQUIRE SSL"),
        ("GRANT ALL ON db1.* TO 'jeffrey'@'localhost' REQUIRE NONE;", true, "GRANT ALL ON `db1`.* TO `jeffrey`@`localhost` REQUIRE NONE"),
        ("GRANT ALL ON db1.* TO 'jeffrey'@'localhost' REQUIRE ISSUER '/C=SE/ST=Stockholm/L=Stockholm/O=MySQL/CN=CA/emailAddress=ca@example.com' AND CIPHER 'EDH-RSA-DES-CBC3-SHA';", true, "GRANT ALL ON `db1`.* TO `jeffrey`@`localhost` REQUIRE ISSUER '/C=SE/ST=Stockholm/L=Stockholm/O=MySQL/CN=CA/emailAddress=ca@example.com' AND CIPHER 'EDH-RSA-DES-CBC3-SHA'"),
        ("GRANT ALL ON db1.* TO 'jeffrey'@'localhost';", true, "GRANT ALL ON `db1`.* TO `jeffrey`@`localhost`"),
        ("GRANT ALL ON TABLE db1.* TO 'jeffrey'@'localhost';", true, "GRANT ALL ON TABLE `db1`.* TO `jeffrey`@`localhost`"),
        ("GRANT ALL ON db1.* TO 'jeffrey'@'localhost' WITH GRANT OPTION;", true, "GRANT ALL ON `db1`.* TO `jeffrey`@`localhost` WITH GRANT OPTION"),
        ("GRANT SELECT ON db2.invoice TO 'jeffrey'@'localhost';", true, "GRANT SELECT ON `db2`.`invoice` TO `jeffrey`@`localhost`"),
        ("GRANT ALL ON *.* TO 'someuser'@'somehost';", true, "GRANT ALL ON *.* TO `someuser`@`somehost`"),
        ("GRANT ALL ON *.* TO 'SOMEuser'@'SOMEhost';", true, "GRANT ALL ON *.* TO `SOMEuser`@`somehost`"),
        ("GRANT SELECT, INSERT ON *.* TO 'someuser'@'somehost';", true, "GRANT SELECT, INSERT ON *.* TO `someuser`@`somehost`"),
        ("GRANT ALL ON mydb.* TO 'someuser'@'somehost';", true, "GRANT ALL ON `mydb`.* TO `someuser`@`somehost`"),
        ("GRANT SELECT, INSERT ON mydb.* TO 'someuser'@'somehost';", true, "GRANT SELECT, INSERT ON `mydb`.* TO `someuser`@`somehost`"),
        ("GRANT ALL ON mydb.mytbl TO 'someuser'@'somehost';", true, "GRANT ALL ON `mydb`.`mytbl` TO `someuser`@`somehost`"),
        ("GRANT SELECT, INSERT ON mydb.mytbl TO 'someuser'@'somehost';", true, "GRANT SELECT, INSERT ON `mydb`.`mytbl` TO `someuser`@`somehost`"),
        ("GRANT SELECT (col1), INSERT (col1,col2) ON mydb.mytbl TO 'someuser'@'somehost';", true, "GRANT SELECT (`col1`), INSERT (`col1`,`col2`) ON `mydb`.`mytbl` TO `someuser`@`somehost`"),
        ("grant all privileges on zabbix.* to 'zabbix'@'localhost' identified by 'password';", true, "GRANT ALL ON `zabbix`.* TO `zabbix`@`localhost` IDENTIFIED BY 'password'"),
        ("GRANT SELECT ON test.* to 'test'", true, "GRANT SELECT ON `test`.* TO `test`@`%`"),
        ("grant PROCESS,usage, REPLICATION SLAVE, REPLICATION CLIENT on *.* to 'xxxxxxxxxx'@'%' identified by password 'xxxxxxxxxxxxxxxxxxxxxxxxxxxx'", true, "GRANT PROCESS, USAGE, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `xxxxxxxxxx`@`%` IDENTIFIED WITH 'mysql_native_password' AS 'xxxxxxxxxxxxxxxxxxxxxxxxxxxx'"),
    ]);
}

fn test_privilege_cases_3() {
    run_cases(&[
        ("/* rds internal mark */ GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, RELOAD, PROCESS, INDEX, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES,      EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT,      TRIGGER on *.* to 'root2'@'%' identified by password '*sdsadsdsadssadsadsadsadsada' with grant option", true, "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, RELOAD, PROCESS, INDEX, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER ON *.* TO `root2`@`%` IDENTIFIED WITH 'mysql_native_password' AS '*sdsadsdsadssadsadsadsadsada' WITH GRANT OPTION"),
        ("GRANT 'role1', 'role2' TO 'user1'@'LOCalhost', 'user2'@'LOcalhost';", true, "GRANT `role1`@`%`, `role2`@`%` TO `user1`@`localhost`, `user2`@`localhost`"),
        ("GRANT 'u1' TO 'u1';", true, "GRANT `u1`@`%` TO `u1`@`%`"),
        ("GRANT 'app_read'@'%','app_write'@'%' TO 'rw_user1'@'localhost'", true, "GRANT `app_read`@`%`, `app_write`@`%` TO `rw_user1`@`localhost`"),
        ("GRANT 'app_developer' TO 'dev1'@'localhost';", true, "GRANT `app_developer`@`%` TO `dev1`@`localhost`"),
        ("GRANT SHUTDOWN ON *.* TO 'dev1'@'localhost';", true, "GRANT SHUTDOWN ON *.* TO `dev1`@`localhost`"),
        ("GRANT CONFIG ON *.* TO 'dev1'@'localhost';", true, "GRANT CONFIG ON *.* TO `dev1`@`localhost`"),
        ("GRANT CREATE ON *.* TO 'dev1'@'localhost';", true, "GRANT CREATE ON *.* TO `dev1`@`localhost`"),
        ("GRANT CREATE TABLESPACE ON *.* TO 'dev1'@'localhost';", true, "GRANT CREATE TABLESPACE ON *.* TO `dev1`@`localhost`"),
        ("GRANT EXECUTE ON FUNCTION db1.anomaly_score TO 'user1'@'domain-or-ip-address1'", true, "GRANT EXECUTE ON FUNCTION `db1`.`anomaly_score` TO `user1`@`domain-or-ip-address1`"),
        ("GRANT EXECUTE ON PROCEDURE mydb.myproc TO 'someuser'@'somehost'", true, "GRANT EXECUTE ON PROCEDURE `mydb`.`myproc` TO `someuser`@`somehost`"),
        ("GRANT APPLICATION_PASSWORD_ADMIN,AUDIT_ADMIN ON *.* TO 'root'@'localhost'", true, "GRANT APPLICATION_PASSWORD_ADMIN, AUDIT_ADMIN ON *.* TO `root`@`localhost`"),
        ("GRANT LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA, INVOKE SAGEMAKER, INVOKE COMPREHEND ON *.* TO 'root'@'localhost'", true, "GRANT LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA, INVOKE SAGEMAKER, INVOKE COMPREHEND ON *.* TO `root`@`localhost`"),
        ("GRANT PROXY ON 'localuser'@'localhost' TO 'externaluser'@'somehost'", true, "GRANT PROXY ON `localuser`@`localhost` TO `externaluser`@`somehost`"),
        ("GRANT PROXY ON ''@'' TO 'root'@'localhost' WITH GRANT OPTION", true, "GRANT PROXY ON ``@`` TO `root`@`localhost` WITH GRANT OPTION"),
        ("GRANT PROXY ON 'proxied_user' TO 'proxy_user1', 'proxy_user2'", true, "GRANT PROXY ON `proxied_user`@`%` TO `proxy_user1`@`%`, `proxy_user2`@`%`"),
        ("grant grant option on *.* to u1", true, "GRANT GRANT OPTION ON *.* TO `u1`@`%`"),
        ("REVOKE ALL ON db1.* FROM 'jeffrey'@'LOCalhost';", true, "REVOKE ALL ON `db1`.* FROM `jeffrey`@`localhost`"),
        ("REVOKE SELECT ON db2.invoice FROM 'jeffrey'@'localhost';", true, "REVOKE SELECT ON `db2`.`invoice` FROM `jeffrey`@`localhost`"),
        ("REVOKE ALL ON *.* FROM 'someuser'@'somehost';", true, "REVOKE ALL ON *.* FROM `someuser`@`somehost`"),
        ("REVOKE SELECT, INSERT ON *.* FROM 'someuser'@'somehost';", true, "REVOKE SELECT, INSERT ON *.* FROM `someuser`@`somehost`"),
        ("REVOKE ALL ON mydb.* FROM 'someuser'@'somehost';", true, "REVOKE ALL ON `mydb`.* FROM `someuser`@`somehost`"),
        ("REVOKE SELECT, INSERT ON mydb.* FROM 'someuser'@'somehost';", true, "REVOKE SELECT, INSERT ON `mydb`.* FROM `someuser`@`somehost`"),
        ("REVOKE ALL ON mydb.mytbl FROM 'someuser'@'somehost';", true, "REVOKE ALL ON `mydb`.`mytbl` FROM `someuser`@`somehost`"),
        ("REVOKE SELECT, INSERT ON mydb.mytbl FROM 'someuser'@'somehost';", true, "REVOKE SELECT, INSERT ON `mydb`.`mytbl` FROM `someuser`@`somehost`"),
        ("REVOKE SELECT (col1), INSERT (col1,col2) ON mydb.mytbl FROM 'someuser'@'somehost';", true, "REVOKE SELECT (`col1`), INSERT (`col1`,`col2`) ON `mydb`.`mytbl` FROM `someuser`@`somehost`"),
        ("REVOKE all privileges on zabbix.* FROM 'zabbix'@'localhost' identified by 'password';", true, "REVOKE ALL ON `zabbix`.* FROM `zabbix`@`localhost` IDENTIFIED BY 'password'"),
        ("REVOKE 'role1', 'role2' FROM 'user1'@'localhost', 'user2'@'localhost';", true, "REVOKE `role1`@`%`, `role2`@`%` FROM `user1`@`localhost`, `user2`@`localhost`"),
        ("REVOKE SHUTDOWN ON *.* FROM 'dev1'@'localhost';", true, "REVOKE SHUTDOWN ON *.* FROM `dev1`@`localhost`"),
        ("REVOKE CONFIG ON *.* FROM 'dev1'@'localhost';", true, "REVOKE CONFIG ON *.* FROM `dev1`@`localhost`"),
        ("REVOKE EXECUTE ON FUNCTION db.func FROM 'user'@'localhost'", true, "REVOKE EXECUTE ON FUNCTION `db`.`func` FROM `user`@`localhost`"),
        ("REVOKE EXECUTE ON PROCEDURE db.func FROM 'user'@'localhost'", true, "REVOKE EXECUTE ON PROCEDURE `db`.`func` FROM `user`@`localhost`"),
        ("REVOKE APPLICATION_PASSWORD_ADMIN,AUDIT_ADMIN ON *.* FROM 'root'@'localhost'", true, "REVOKE APPLICATION_PASSWORD_ADMIN, AUDIT_ADMIN ON *.* FROM `root`@`localhost`"),
        ("revoke all privileges, grant option from u1", true, "REVOKE ALL, GRANT OPTION ON *.* FROM `u1`@`%`"),
        ("revoke all privileges, grant option from u1, u2, u3", true, "REVOKE ALL, GRANT OPTION ON *.* FROM `u1`@`%`, `u2`@`%`, `u3`@`%`"),
    ]);
}

fn test_comment_cases_0() {
    run_cases(&[
        (
            "create table t (c int comment 'comment')",
            true,
            "CREATE TABLE `t` (`c` INT COMMENT 'comment')",
        ),
        (
            "create table t (c int) comment = 'comment'",
            true,
            "CREATE TABLE `t` (`c` INT) COMMENT = 'comment'",
        ),
        (
            "create table t (c int) comment 'comment'",
            true,
            "CREATE TABLE `t` (`c` INT) COMMENT = 'comment'",
        ),
        ("create table t (c int) comment comment", false, ""),
        (
            "create table t (comment text)",
            true,
            "CREATE TABLE `t` (`comment` TEXT)",
        ),
        (
            "START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */",
            true,
            "START TRANSACTION",
        ),
        (
            "/*comment*/ /*comment*/ select c /* this is a comment */ from t;",
            true,
            "SELECT `c` FROM `t`",
        ),
        ("delete from t where a = 7 or 1=1/*' and b = 'p'", false, ""),
        ("create table t (ssl int)", false, ""),
        ("create table t (require int)", false, ""),
        (
            "create table t (account int)",
            true,
            "CREATE TABLE `t` (`account` INT)",
        ),
        (
            "create table t (expire int)",
            true,
            "CREATE TABLE `t` (`expire` INT)",
        ),
        (
            "create table t (cipher int)",
            true,
            "CREATE TABLE `t` (`cipher` INT)",
        ),
        (
            "create table t (issuer int)",
            true,
            "CREATE TABLE `t` (`issuer` INT)",
        ),
        (
            "create table t (never int)",
            true,
            "CREATE TABLE `t` (`never` INT)",
        ),
        (
            "create table t (subject int)",
            true,
            "CREATE TABLE `t` (`subject` INT)",
        ),
        (
            "create table t (x509 int)",
            true,
            "CREATE TABLE `t` (`x509` INT)",
        ),
        (
            "create user commentUser COMMENT '123456' '{\"name\": \"Tom\", \"age\", 19}",
            false,
            "",
        ),
        (
            "alter user commentUser COMMENT '123456' '{\"name\": \"Tom\", \"age\", 19}",
            false,
            "",
        ),
        (
            "create user commentUser COMMENT '123456'",
            true,
            "CREATE USER `commentUser`@`%` COMMENT '123456'",
        ),
        (
            "alter user commentUser COMMENT '123456'",
            true,
            "ALTER USER `commentUser`@`%` COMMENT '123456'",
        ),
        (
            "create user commentUser ATTRIBUTE '{\"name\": \"Tom\", \"age\", 19}'",
            true,
            "CREATE USER `commentUser`@`%` ATTRIBUTE '{\"name\": \"Tom\", \"age\", 19}'",
        ),
        (
            "alter user commentUser ATTRIBUTE '{\"name\": \"Tom\", \"age\", 19}'",
            true,
            "ALTER USER `commentUser`@`%` ATTRIBUTE '{\"name\": \"Tom\", \"age\", 19}'",
        ),
    ]);
}

fn test_subquery_cases_0() {
    run_cases(&[
        ("SELECT 1 > (select 1)", true, "SELECT 1>(SELECT 1)"),
        ("SELECT 1 > ANY (select 1)", true, "SELECT 1>ANY (SELECT 1)"),
        ("SELECT 1 > ALL (select 1)", true, "SELECT 1>ALL (SELECT 1)"),
        ("SELECT 1 > SOME (select 1)", true, "SELECT 1>ANY (SELECT 1)"),
        ("SELECT EXISTS select 1", false, ""),
        ("SELECT EXISTS (select 1)", true, "SELECT EXISTS (SELECT 1)"),
        ("SELECT + EXISTS (select 1)", true, "SELECT +EXISTS (SELECT 1)"),
        ("SELECT - EXISTS (select 1)", true, "SELECT -EXISTS (SELECT 1)"),
        ("SELECT NOT EXISTS (select 1)", true, "SELECT NOT EXISTS (SELECT 1)"),
        ("SELECT + NOT EXISTS (select 1)", false, ""),
        ("SELECT - NOT EXISTS (select 1)", false, ""),
        ("SELECT * FROM t where t.a in (select a from t limit 1, 10)", true, "SELECT * FROM `t` WHERE `t`.`a` IN (SELECT `a` FROM `t` LIMIT 1,10)"),
        ("SELECT * FROM t where t.a in ((select a from t limit 1, 10))", true, "SELECT * FROM `t` WHERE `t`.`a` IN ((SELECT `a` FROM `t` LIMIT 1,10))"),
        ("SELECT * FROM t where t.a in ((select a from t limit 1, 10), 1)", true, "SELECT * FROM `t` WHERE `t`.`a` IN ((SELECT `a` FROM `t` LIMIT 1,10),1)"),
        ("select * from ((select a from t) t1 join t t2) join t3", true, "SELECT * FROM ((SELECT `a` FROM `t`) AS `t1` JOIN `t` AS `t2`) JOIN `t3`"),
        ("SELECT t1.a AS a FROM ((SELECT a FROM t) AS t1)", true, "SELECT `t1`.`a` AS `a` FROM (SELECT `a` FROM `t`) AS `t1`"),
        ("select count(*) from (select a, b from x1 union all select a, b from x3 union all (select x1.a, x3.b from (select * from x3 union all select * from x2) x3 left join x1 on x3.a = x1.b))", true, "SELECT COUNT(1) FROM (SELECT `a`,`b` FROM `x1` UNION ALL SELECT `a`,`b` FROM `x3` UNION ALL (SELECT `x1`.`a`,`x3`.`b` FROM (SELECT * FROM `x3` UNION ALL SELECT * FROM `x2`) AS `x3` LEFT JOIN `x1` ON `x3`.`a`=`x1`.`b`))"),
        ("(SELECT 1 a,3 b) UNION (SELECT 2,1) ORDER BY (SELECT 2)", true, "(SELECT 1 AS `a`,3 AS `b`) UNION (SELECT 2,1) ORDER BY (SELECT 2)"),
        ("((select * from t1)) union (select * from t1)", true, "(SELECT * FROM `t1`) UNION (SELECT * FROM `t1`)"),
        ("(((select * from t1))) union (select * from t1)", true, "(SELECT * FROM `t1`) UNION (SELECT * FROM `t1`)"),
        ("select * from (((select * from t1)) union (select * from t1) union (select * from t1)) a", true, "SELECT * FROM ((SELECT * FROM `t1`) UNION (SELECT * FROM `t1`) UNION (SELECT * FROM `t1`)) AS `a`"),
        ("SELECT COUNT(*) FROM plan_executions WHERE (EXISTS((SELECT * FROM triggers WHERE plan_executions.trigger_id=triggers.id AND triggers.type='CRON')))", true, "SELECT COUNT(1) FROM `plan_executions` WHERE (EXISTS (SELECT * FROM `triggers` WHERE `plan_executions`.`trigger_id`=`triggers`.`id` AND `triggers`.`type`=_UTF8MB4'CRON'))"),
        ("select exists((select 1));", true, "SELECT EXISTS (SELECT 1)"),
        ("select * from ((SELECT 1 a,3 b) UNION (SELECT 2,1) ORDER BY (SELECT 2)) t order by a,b", true, "SELECT * FROM ((SELECT 1 AS `a`,3 AS `b`) UNION (SELECT 2,1) ORDER BY (SELECT 2)) AS `t` ORDER BY `a`,`b`"),
        ("select (select * from t1 where a != t.a union all (select * from t2 where a != t.a) order by a limit 1) from t1 t", true, "SELECT (SELECT * FROM `t1` WHERE `a`!=`t`.`a` UNION ALL (SELECT * FROM `t2` WHERE `a`!=`t`.`a`) ORDER BY `a` LIMIT 1) FROM `t1` AS `t`"),
        ("(WITH v0 AS (SELECT TRUE) (SELECT 'abc' EXCEPT (SELECT TRUE)))", true, "WITH `v0` AS (SELECT TRUE) (SELECT _UTF8MB4'abc' EXCEPT (SELECT TRUE))"),
    ]);
}

fn test_set_operator_cases_0() {
    run_cases(&[
        ("select c1 from t1 union select c2 from t2", true, "SELECT `c1` FROM `t1` UNION SELECT `c2` FROM `t2`"),
        ("select c1 from t1 union (select c2 from t2)", true, "SELECT `c1` FROM `t1` UNION (SELECT `c2` FROM `t2`)"),
        ("select c1 from t1 union (select c2 from t2) order by c1", true, "SELECT `c1` FROM `t1` UNION (SELECT `c2` FROM `t2`) ORDER BY `c1`"),
        ("select c1 from t1 union select c2 from t2 order by c2", true, "SELECT `c1` FROM `t1` UNION SELECT `c2` FROM `t2` ORDER BY `c2`"),
        ("select c1 from t1 union (select c2 from t2) limit 1", true, "SELECT `c1` FROM `t1` UNION (SELECT `c2` FROM `t2`) LIMIT 1"),
        ("select c1 from t1 union (select c2 from t2) limit 1, 1", true, "SELECT `c1` FROM `t1` UNION (SELECT `c2` FROM `t2`) LIMIT 1,1"),
        ("select c1 from t1 union (select c2 from t2) order by c1 limit 1", true, "SELECT `c1` FROM `t1` UNION (SELECT `c2` FROM `t2`) ORDER BY `c1` LIMIT 1"),
        ("(select c1 from t1) union distinct select c2 from t2", true, "(SELECT `c1` FROM `t1`) UNION SELECT `c2` FROM `t2`"),
        ("(select c1 from t1) union distinctrow select c2 from t2", true, "(SELECT `c1` FROM `t1`) UNION SELECT `c2` FROM `t2`"),
        ("(select c1 from t1) union all select c2 from t2", true, "(SELECT `c1` FROM `t1`) UNION ALL SELECT `c2` FROM `t2`"),
        ("(select c1 from t1) union distinct all select c2 from t2", false, ""),
        ("(select c1 from t1) union distinctrow all select c2 from t2", false, ""),
        ("(select c1 from t1) union (select c2 from t2) order by c1 union select c3 from t3", false, ""),
        ("(select c1 from t1) union (select c2 from t2) limit 1 union select c3 from t3", false, ""),
        ("(select c1 from t1) union select c2 from t2 union (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) UNION SELECT `c2` FROM `t2` UNION (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"),
        ("select (select 1 union select 1) as a", true, "SELECT (SELECT 1 UNION SELECT 1) AS `a`"),
        ("select * from (select 1 union select 2) as a", true, "SELECT * FROM (SELECT 1 UNION SELECT 2) AS `a`"),
        ("insert into t select c1 from t1 union select c2 from t2", true, "INSERT INTO `t` SELECT `c1` FROM `t1` UNION SELECT `c2` FROM `t2`"),
        ("insert into t (c) select c1 from t1 union select c2 from t2", true, "INSERT INTO `t` (`c`) SELECT `c1` FROM `t1` UNION SELECT `c2` FROM `t2`"),
        ("select 2 as a from dual union select 1 as b from dual order by a", true, "SELECT 2 AS `a` UNION SELECT 1 AS `b` ORDER BY `a`"),
        ("table t1 union table t2", true, "TABLE `t1` UNION TABLE `t2`"),
        ("table t1 union (table t2)", true, "TABLE `t1` UNION (TABLE `t2`)"),
        ("table t1 union select * from t2", true, "TABLE `t1` UNION SELECT * FROM `t2`"),
        ("select * from t1 union table t2", true, "SELECT * FROM `t1` UNION TABLE `t2`"),
        ("table t1 union (select c2 from t2) order by c1 limit 1", true, "TABLE `t1` UNION (SELECT `c2` FROM `t2`) ORDER BY `c1` LIMIT 1"),
        ("select c1 from t1 union (table t2) order by c1 limit 1", true, "SELECT `c1` FROM `t1` UNION (TABLE `t2`) ORDER BY `c1` LIMIT 1"),
        ("(select c1 from t1) union table t2 union (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) UNION TABLE `t2` UNION (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"),
        ("(table t1) union select c2 from t2 union (table t3) order by c1 limit 1", true, "(TABLE `t1`) UNION SELECT `c2` FROM `t2` UNION (TABLE `t3`) ORDER BY `c1` LIMIT 1"),
        ("values row(1,-2,3), row(5,7,9) union values row(1,-2,3), row(5,7,9)", true, "VALUES ROW(1,-2,3), ROW(5,7,9) UNION VALUES ROW(1,-2,3), ROW(5,7,9)"),
        ("values row(1,-2,3), row(5,7,9) union (values row(1,-2,3), row(5,7,9))", true, "VALUES ROW(1,-2,3), ROW(5,7,9) UNION (VALUES ROW(1,-2,3), ROW(5,7,9))"),
        ("values row(1,-2,3), row(5,7,9) union select * from t", true, "VALUES ROW(1,-2,3), ROW(5,7,9) UNION SELECT * FROM `t`"),
        ("values row(1,-2,3), row(5,7,9) union table t", true, "VALUES ROW(1,-2,3), ROW(5,7,9) UNION TABLE `t`"),
        ("select * from t union values row(1,-2,3), row(5,7,9)", true, "SELECT * FROM `t` UNION VALUES ROW(1,-2,3), ROW(5,7,9)"),
        ("table t union values row(1,-2,3), row(5,7,9)", true, "TABLE `t` UNION VALUES ROW(1,-2,3), ROW(5,7,9)"),
        ("select c1 from t1 except select c2 from t2", true, "SELECT `c1` FROM `t1` EXCEPT SELECT `c2` FROM `t2`"),
        ("select c1 from t1 except (select c2 from t2)", true, "SELECT `c1` FROM `t1` EXCEPT (SELECT `c2` FROM `t2`)"),
        ("select c1 from t1 except (select c2 from t2) order by c1", true, "SELECT `c1` FROM `t1` EXCEPT (SELECT `c2` FROM `t2`) ORDER BY `c1`"),
        ("select c1 from t1 except select c2 from t2 order by c2", true, "SELECT `c1` FROM `t1` EXCEPT SELECT `c2` FROM `t2` ORDER BY `c2`"),
        ("select c1 from t1 except (select c2 from t2) limit 1", true, "SELECT `c1` FROM `t1` EXCEPT (SELECT `c2` FROM `t2`) LIMIT 1"),
        ("select c1 from t1 except (select c2 from t2) limit 1, 1", true, "SELECT `c1` FROM `t1` EXCEPT (SELECT `c2` FROM `t2`) LIMIT 1,1"),
    ]);
}

fn test_set_operator_cases_1() {
    run_cases(&[
        ("select c1 from t1 except (select c2 from t2) order by c1 limit 1", true, "SELECT `c1` FROM `t1` EXCEPT (SELECT `c2` FROM `t2`) ORDER BY `c1` LIMIT 1"),
        ("(select c1 from t1) except (select c2 from t2) order by c1 except select c3 from t3", false, ""),
        ("(select c1 from t1) except (select c2 from t2) limit 1 except select c3 from t3", false, ""),
        ("(select c1 from t1) except select c2 from t2 except (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) EXCEPT SELECT `c2` FROM `t2` EXCEPT (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"),
        ("select (select 1 except select 1) as a", true, "SELECT (SELECT 1 EXCEPT SELECT 1) AS `a`"),
        ("select * from (select 1 except select 2) as a", true, "SELECT * FROM (SELECT 1 EXCEPT SELECT 2) AS `a`"),
        ("insert into t select c1 from t1 except select c2 from t2", true, "INSERT INTO `t` SELECT `c1` FROM `t1` EXCEPT SELECT `c2` FROM `t2`"),
        ("insert into t (c) select c1 from t1 except select c2 from t2", true, "INSERT INTO `t` (`c`) SELECT `c1` FROM `t1` EXCEPT SELECT `c2` FROM `t2`"),
        ("select 2 as a from dual except select 1 as b from dual order by a", true, "SELECT 2 AS `a` EXCEPT SELECT 1 AS `b` ORDER BY `a`"),
        ("table t1 except table t2", true, "TABLE `t1` EXCEPT TABLE `t2`"),
        ("table t1 except (table t2)", true, "TABLE `t1` EXCEPT (TABLE `t2`)"),
        ("table t1 except select * from t2", true, "TABLE `t1` EXCEPT SELECT * FROM `t2`"),
        ("select * from t1 except table t2", true, "SELECT * FROM `t1` EXCEPT TABLE `t2`"),
        ("table t1 except (select c2 from t2) order by c1 limit 1", true, "TABLE `t1` EXCEPT (SELECT `c2` FROM `t2`) ORDER BY `c1` LIMIT 1"),
        ("select c1 from t1 except (table t2) order by c1 limit 1", true, "SELECT `c1` FROM `t1` EXCEPT (TABLE `t2`) ORDER BY `c1` LIMIT 1"),
        ("(select c1 from t1) except table t2 except (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) EXCEPT TABLE `t2` EXCEPT (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"),
        ("(table t1) except select c2 from t2 except (table t3) order by c1 limit 1", true, "(TABLE `t1`) EXCEPT SELECT `c2` FROM `t2` EXCEPT (TABLE `t3`) ORDER BY `c1` LIMIT 1"),
        ("values row(1,-2,3), row(5,7,9) except values row(1,-2,3), row(5,7,9)", true, "VALUES ROW(1,-2,3), ROW(5,7,9) EXCEPT VALUES ROW(1,-2,3), ROW(5,7,9)"),
        ("values row(1,-2,3), row(5,7,9) except (values row(1,-2,3), row(5,7,9))", true, "VALUES ROW(1,-2,3), ROW(5,7,9) EXCEPT (VALUES ROW(1,-2,3), ROW(5,7,9))"),
        ("values row(1,-2,3), row(5,7,9) except select * from t", true, "VALUES ROW(1,-2,3), ROW(5,7,9) EXCEPT SELECT * FROM `t`"),
        ("values row(1,-2,3), row(5,7,9) except table t", true, "VALUES ROW(1,-2,3), ROW(5,7,9) EXCEPT TABLE `t`"),
        ("select * from t except values row(1,-2,3), row(5,7,9)", true, "SELECT * FROM `t` EXCEPT VALUES ROW(1,-2,3), ROW(5,7,9)"),
        ("table t except values row(1,-2,3), row(5,7,9)", true, "TABLE `t` EXCEPT VALUES ROW(1,-2,3), ROW(5,7,9)"),
        ("select c1 from t1 intersect select c2 from t2", true, "SELECT `c1` FROM `t1` INTERSECT SELECT `c2` FROM `t2`"),
        ("select c1 from t1 intersect (select c2 from t2)", true, "SELECT `c1` FROM `t1` INTERSECT (SELECT `c2` FROM `t2`)"),
        ("select c1 from t1 intersect (select c2 from t2) order by c1", true, "SELECT `c1` FROM `t1` INTERSECT (SELECT `c2` FROM `t2`) ORDER BY `c1`"),
        ("select c1 from t1 intersect select c2 from t2 order by c2", true, "SELECT `c1` FROM `t1` INTERSECT SELECT `c2` FROM `t2` ORDER BY `c2`"),
        ("select c1 from t1 intersect (select c2 from t2) limit 1", true, "SELECT `c1` FROM `t1` INTERSECT (SELECT `c2` FROM `t2`) LIMIT 1"),
        ("select c1 from t1 intersect (select c2 from t2) limit 1, 1", true, "SELECT `c1` FROM `t1` INTERSECT (SELECT `c2` FROM `t2`) LIMIT 1,1"),
        ("select c1 from t1 intersect (select c2 from t2) order by c1 limit 1", true, "SELECT `c1` FROM `t1` INTERSECT (SELECT `c2` FROM `t2`) ORDER BY `c1` LIMIT 1"),
        ("(select c1 from t1) intersect (select c2 from t2) order by c1 intersect select c3 from t3", false, ""),
        ("(select c1 from t1) intersect (select c2 from t2) limit 1 intersect select c3 from t3", false, ""),
        ("(select c1 from t1) intersect select c2 from t2 intersect (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) INTERSECT SELECT `c2` FROM `t2` INTERSECT (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"),
        ("select (select 1 intersect select 1) as a", true, "SELECT (SELECT 1 INTERSECT SELECT 1) AS `a`"),
        ("select * from (select 1 intersect select 2) as a", true, "SELECT * FROM (SELECT 1 INTERSECT SELECT 2) AS `a`"),
        ("insert into t select c1 from t1 intersect select c2 from t2", true, "INSERT INTO `t` SELECT `c1` FROM `t1` INTERSECT SELECT `c2` FROM `t2`"),
        ("insert into t (c) select c1 from t1 intersect select c2 from t2", true, "INSERT INTO `t` (`c`) SELECT `c1` FROM `t1` INTERSECT SELECT `c2` FROM `t2`"),
        ("select 2 as a from dual intersect select 1 as b from dual order by a", true, "SELECT 2 AS `a` INTERSECT SELECT 1 AS `b` ORDER BY `a`"),
        ("table t1 intersect table t2", true, "TABLE `t1` INTERSECT TABLE `t2`"),
        ("table t1 intersect (table t2)", true, "TABLE `t1` INTERSECT (TABLE `t2`)"),
    ]);
}

fn test_set_operator_cases_2() {
    run_cases(&[
        ("table t1 intersect select * from t2", true, "TABLE `t1` INTERSECT SELECT * FROM `t2`"),
        ("select * from t1 intersect table t2", true, "SELECT * FROM `t1` INTERSECT TABLE `t2`"),
        ("table t1 intersect (select c2 from t2) order by c1 limit 1", true, "TABLE `t1` INTERSECT (SELECT `c2` FROM `t2`) ORDER BY `c1` LIMIT 1"),
        ("select c1 from t1 intersect (table t2) order by c1 limit 1", true, "SELECT `c1` FROM `t1` INTERSECT (TABLE `t2`) ORDER BY `c1` LIMIT 1"),
        ("(select c1 from t1) intersect table t2 intersect (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) INTERSECT TABLE `t2` INTERSECT (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"),
        ("(table t1) intersect select c2 from t2 intersect (table t3) order by c1 limit 1", true, "(TABLE `t1`) INTERSECT SELECT `c2` FROM `t2` INTERSECT (TABLE `t3`) ORDER BY `c1` LIMIT 1"),
        ("values row(1,-2,3), row(5,7,9) intersect values row(1,-2,3), row(5,7,9)", true, "VALUES ROW(1,-2,3), ROW(5,7,9) INTERSECT VALUES ROW(1,-2,3), ROW(5,7,9)"),
        ("values row(1,-2,3), row(5,7,9) intersect (values row(1,-2,3), row(5,7,9))", true, "VALUES ROW(1,-2,3), ROW(5,7,9) INTERSECT (VALUES ROW(1,-2,3), ROW(5,7,9))"),
        ("values row(1,-2,3), row(5,7,9) intersect select * from t", true, "VALUES ROW(1,-2,3), ROW(5,7,9) INTERSECT SELECT * FROM `t`"),
        ("values row(1,-2,3), row(5,7,9) intersect table t", true, "VALUES ROW(1,-2,3), ROW(5,7,9) INTERSECT TABLE `t`"),
        ("select * from t intersect values row(1,-2,3), row(5,7,9)", true, "SELECT * FROM `t` INTERSECT VALUES ROW(1,-2,3), ROW(5,7,9)"),
        ("table t intersect values row(1,-2,3), row(5,7,9)", true, "TABLE `t` INTERSECT VALUES ROW(1,-2,3), ROW(5,7,9)"),
        ("(select c1 from t1) intersect select c2 from t2 union (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) INTERSECT SELECT `c2` FROM `t2` UNION (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"),
        ("(select c1 from t1) union all select c2 from t2 except (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) UNION ALL SELECT `c2` FROM `t2` EXCEPT (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"),
        ("(select c1 from t1) except select c2 from t2 intersect (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) EXCEPT SELECT `c2` FROM `t2` INTERSECT (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"),
        ("select 1 union distinct select 1 except select 1 intersect select 1", true, "SELECT 1 UNION SELECT 1 EXCEPT SELECT 1 INTERSECT SELECT 1"),
        ("(select c1 from t1) intersect all (select c2 from t2 union (select c3 from t3)) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) INTERSECT ALL (SELECT `c2` FROM `t2` UNION (SELECT `c3` FROM `t3`)) ORDER BY `c1` LIMIT 1"),
        ("(select c1 from t1) union all (select c2 from t2 except select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) UNION ALL (SELECT `c2` FROM `t2` EXCEPT SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"),
        ("((select c1 from t1) except select c2 from t2) intersect all (select c3 from t3) order by c1 limit 1", true, "((SELECT `c1` FROM `t1`) EXCEPT SELECT `c2` FROM `t2`) INTERSECT ALL (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"),
        ("select 1 union distinct (select 1 except all select 1 intersect select 1)", true, "SELECT 1 UNION (SELECT 1 EXCEPT ALL SELECT 1 INTERSECT SELECT 1)"),
        ("select * from a where PK = 0 union all (select * from b where PK = 0 union all (select * from b where PK != 0) order by pk limit 1)", true, "SELECT * FROM `a` WHERE `PK`=0 UNION ALL (SELECT * FROM `b` WHERE `PK`=0 UNION ALL (SELECT * FROM `b` WHERE `PK`!=0) ORDER BY `pk` LIMIT 1)"),
        ("select * from a where PK = 0 union all (select * from b where PK = 0 union all (select * from b where PK != 0) order by pk limit 1) order by pk limit 2", true, "SELECT * FROM `a` WHERE `PK`=0 UNION ALL (SELECT * FROM `b` WHERE `PK`=0 UNION ALL (SELECT * FROM `b` WHERE `PK`!=0) ORDER BY `pk` LIMIT 1) ORDER BY `pk` LIMIT 2"),
        ("(select * from b where pk= 0 union all (select * from b where pk !=0) order by pk limit 1) order by pk limit 2", true, "(SELECT * FROM `b` WHERE `pk`=0 UNION ALL (SELECT * FROM `b` WHERE `pk`!=0) ORDER BY `pk` LIMIT 1) ORDER BY `pk` LIMIT 2"),
        ("(select * from b where pk= 0 union all (select * from b where pk !=0) order by pk limit 1) order by pk", true, "(SELECT * FROM `b` WHERE `pk`=0 UNION ALL (SELECT * FROM `b` WHERE `pk`!=0) ORDER BY `pk` LIMIT 1) ORDER BY `pk`"),
    ]);
}

#[test]
fn test_dbastmt() {
    test_dbastmt_cases_0();
    test_dbastmt_cases_1();
    test_dbastmt_cases_2();
    test_dbastmt_cases_3();
    test_dbastmt_cases_4();
    test_dbastmt_cases_5();
}

#[test]
fn test_expression() {
    test_expression_cases_0();
}

#[test]
fn test_identifier() {
    test_identifier_cases_0();
    test_identifier_cases_1();
}

#[test]
fn test_type() {
    test_type_cases_0();
}

#[test]
fn test_privilege() {
    test_privilege_cases_0();
    test_privilege_cases_1();
    test_privilege_cases_2();
    test_privilege_cases_3();
}

#[test]
fn test_comment() {
    test_comment_cases_0();
}

#[test]
fn test_subquery() {
    test_subquery_cases_0();
}

#[test]
fn test_set_operator() {
    test_set_operator_cases_0();
    test_set_operator_cases_1();
    test_set_operator_cases_2();
}
