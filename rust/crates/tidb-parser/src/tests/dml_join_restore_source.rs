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

//! Source-owned table, join, and condition restore rows from
//! `pkg/parser/ast/dml_test.go`.

use super::*;

fn assert_rows(rows: &[(&str, &str)]) {
    for (input, expected) in rows {
        assert_eq!(r(input), *expected, "restore {input}");
    }
}

/// Go source: `TestTableNameRestore` (`dml_test.go:77`).
#[test]
fn table_name_restore_source_rows() {
    assert_rows(&[
        (
            "CREATE TABLE dbb.`tbb1` (id VARCHAR(128) NOT NULL);",
            "CREATE TABLE `dbb`.`tbb1` (`id` VARCHAR(128) NOT NULL)",
        ),
        (
            "CREATE TABLE `tbb2` (id VARCHAR(128) NOT NULL);",
            "CREATE TABLE `tbb2` (`id` VARCHAR(128) NOT NULL)",
        ),
        (
            "CREATE TABLE tbb3 (id VARCHAR(128) NOT NULL);",
            "CREATE TABLE `tbb3` (`id` VARCHAR(128) NOT NULL)",
        ),
        (
            "CREATE TABLE dbb.`hello-world` (id VARCHAR(128) NOT NULL);",
            "CREATE TABLE `dbb`.`hello-world` (`id` VARCHAR(128) NOT NULL)",
        ),
        (
            "CREATE TABLE `dbb`.`hello-world` (id VARCHAR(128) NOT NULL);",
            "CREATE TABLE `dbb`.`hello-world` (`id` VARCHAR(128) NOT NULL)",
        ),
        (
            "CREATE TABLE `dbb.HelloWorld` (id VARCHAR(128) NOT NULL);",
            "CREATE TABLE `dbb.HelloWorld` (`id` VARCHAR(128) NOT NULL)",
        ),
    ]);
}

/// Go source: `TestTableNameIndexHintsRestore` (`dml_test.go:92`).
#[test]
fn table_name_index_hints_restore_source_rows() {
    assert_rows(&[
        ("SELECT * FROM t USE INDEX (hello)", "SELECT * FROM `t` USE INDEX (`hello`)"),
        ("SELECT * FROM t USE INDEX (hello, world)", "SELECT * FROM `t` USE INDEX (`hello`, `world`)"),
        ("SELECT * FROM t USE INDEX ()", "SELECT * FROM `t` USE INDEX ()"),
        ("SELECT * FROM t USE KEY ()", "SELECT * FROM `t` USE INDEX ()"),
        ("SELECT * FROM t IGNORE KEY ()", "SELECT * FROM `t` IGNORE INDEX ()"),
        ("SELECT * FROM t FORCE KEY ()", "SELECT * FROM `t` FORCE INDEX ()"),
        ("SELECT * FROM t USE INDEX FOR ORDER BY (idx1)", "SELECT * FROM `t` USE INDEX FOR ORDER BY (`idx1`)"),
        ("SELECT * FROM t USE INDEX (hello, world, yes) FORCE KEY (good)", "SELECT * FROM `t` USE INDEX (`hello`, `world`, `yes`) FORCE INDEX (`good`)"),
        ("SELECT * FROM t USE INDEX (hello, world, yes) USE INDEX FOR ORDER BY (good)", "SELECT * FROM `t` USE INDEX (`hello`, `world`, `yes`) USE INDEX FOR ORDER BY (`good`)"),
        ("SELECT * FROM t IGNORE KEY (hello, world, yes) FORCE KEY (good)", "SELECT * FROM `t` IGNORE INDEX (`hello`, `world`, `yes`) FORCE INDEX (`good`)"),
        ("SELECT * FROM t USE INDEX FOR GROUP BY (idx1) USE INDEX FOR ORDER BY (idx2)", "SELECT * FROM `t` USE INDEX FOR GROUP BY (`idx1`) USE INDEX FOR ORDER BY (`idx2`)"),
        ("SELECT * FROM t USE INDEX FOR GROUP BY (idx1) IGNORE KEY FOR ORDER BY (idx2)", "SELECT * FROM `t` USE INDEX FOR GROUP BY (`idx1`) IGNORE INDEX FOR ORDER BY (`idx2`)"),
        ("SELECT * FROM t USE INDEX FOR GROUP BY (idx1) IGNORE KEY FOR GROUP BY (idx2)", "SELECT * FROM `t` USE INDEX FOR GROUP BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`)"),
        ("SELECT * FROM t USE INDEX FOR ORDER BY (idx1) IGNORE KEY FOR GROUP BY (idx2)", "SELECT * FROM `t` USE INDEX FOR ORDER BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`)"),
        ("SELECT * FROM t USE INDEX FOR ORDER BY (idx1) IGNORE KEY FOR GROUP BY (idx2) USE INDEX (idx3)", "SELECT * FROM `t` USE INDEX FOR ORDER BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`) USE INDEX (`idx3`)"),
        ("SELECT * FROM t USE INDEX FOR ORDER BY (idx1) IGNORE KEY FOR GROUP BY (idx2) USE INDEX (idx3)", "SELECT * FROM `t` USE INDEX FOR ORDER BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`) USE INDEX (`idx3`)"),
        ("SELECT * FROM t USE INDEX (`foo``bar`) FORCE INDEX (`baz``1`, `xyz`)", "SELECT * FROM `t` USE INDEX (`foo``bar`) FORCE INDEX (`baz``1`, `xyz`)"),
        ("SELECT * FROM t FORCE INDEX (`foo``bar`) IGNORE INDEX (`baz``1`, xyz)", "SELECT * FROM `t` FORCE INDEX (`foo``bar`) IGNORE INDEX (`baz``1`, `xyz`)"),
        ("SELECT * FROM t IGNORE INDEX (`foo``bar`) FORCE KEY (`baz``1`, xyz)", "SELECT * FROM `t` IGNORE INDEX (`foo``bar`) FORCE INDEX (`baz``1`, `xyz`)"),
        ("SELECT * FROM t IGNORE INDEX (`foo``bar`) IGNORE KEY FOR GROUP BY (`baz``1`, xyz)", "SELECT * FROM `t` IGNORE INDEX (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)"),
        ("SELECT * FROM t IGNORE INDEX (`foo``bar`) IGNORE KEY FOR ORDER BY (`baz``1`, xyz)", "SELECT * FROM `t` IGNORE INDEX (`foo``bar`) IGNORE INDEX FOR ORDER BY (`baz``1`, `xyz`)"),
        ("SELECT * FROM t USE INDEX FOR GROUP BY (`foo``bar`) USE INDEX FOR ORDER BY (`baz``1`, `xyz`)", "SELECT * FROM `t` USE INDEX FOR GROUP BY (`foo``bar`) USE INDEX FOR ORDER BY (`baz``1`, `xyz`)"),
        ("SELECT * FROM t USE INDEX FOR GROUP BY (`foo``bar`) IGNORE KEY FOR ORDER BY (`baz``1`, `xyz`)", "SELECT * FROM `t` USE INDEX FOR GROUP BY (`foo``bar`) IGNORE INDEX FOR ORDER BY (`baz``1`, `xyz`)"),
        ("SELECT * FROM t USE INDEX FOR GROUP BY (`foo``bar`) IGNORE KEY FOR GROUP BY (`baz``1`, `xyz`)", "SELECT * FROM `t` USE INDEX FOR GROUP BY (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)"),
        ("SELECT * FROM t USE INDEX FOR ORDER BY (`foo``bar`) IGNORE KEY FOR GROUP BY (`baz``1`, `xyz`)", "SELECT * FROM `t` USE INDEX FOR ORDER BY (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)"),
        ("SELECT * FROM t tt USE INDEX FOR ORDER BY (`foo``bar`) IGNORE KEY FOR GROUP BY (`baz``1`, `xyz`)", "SELECT * FROM `t` AS `tt` USE INDEX FOR ORDER BY (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)"),
        ("SELECT * FROM t AS tt USE INDEX FOR ORDER BY (`foo``bar`) IGNORE KEY FOR GROUP BY (`baz``1`, `xyz`)", "SELECT * FROM `t` AS `tt` USE INDEX FOR ORDER BY (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)"),
    ]);
}

/// Go source: `TestTableSourceRestore` (`dml_test.go:186`).
#[test]
fn table_source_restore_source_rows() {
    assert_rows(&[
        ("SELECT * FROM tbl", "SELECT * FROM `tbl`"),
        ("SELECT * FROM tbl AS t", "SELECT * FROM `tbl` AS `t`"),
        (
            "SELECT * FROM (SELECT * FROM tbl) AS t",
            "SELECT * FROM (SELECT * FROM `tbl`) AS `t`",
        ),
        (
            "SELECT * FROM (SELECT * FROM a UNION SELECT * FROM b) AS t",
            "SELECT * FROM (SELECT * FROM `a` UNION SELECT * FROM `b`) AS `t`",
        ),
    ]);
}

/// Go source: `TestOnConditionRestore` (`dml_test.go:199`).
#[test]
fn on_condition_restore_source_rows() {
    assert_rows(&[
        (
            "SELECT * FROM t1 JOIN t2 ON t1.a=t2.a",
            "SELECT * FROM `t1` JOIN `t2` ON `t1`.`a`=`t2`.`a`",
        ),
        (
            "SELECT * FROM t1 JOIN t2 ON t1.a=t2.a AND t1.b=t2.b",
            "SELECT * FROM `t1` JOIN `t2` ON `t1`.`a`=`t2`.`a` AND `t1`.`b`=`t2`.`b`",
        ),
    ]);
}

/// Go source: `TestJoinRestore` (`dml_test.go:210`).
#[test]
fn join_restore_source_rows() {
    assert_rows(&[
        ("SELECT * FROM t1 NATURAL JOIN t2", "SELECT * FROM `t1` NATURAL JOIN `t2`"),
        ("SELECT * FROM t1 NATURAL LEFT JOIN t2", "SELECT * FROM `t1` NATURAL LEFT JOIN `t2`"),
        ("SELECT * FROM t1 NATURAL RIGHT OUTER JOIN t2", "SELECT * FROM `t1` NATURAL RIGHT JOIN `t2`"),
        ("SELECT * FROM t1 STRAIGHT_JOIN t2", "SELECT * FROM `t1` STRAIGHT_JOIN `t2`"),
        ("SELECT * FROM t1 STRAIGHT_JOIN t2 ON t1.a>t2.a", "SELECT * FROM `t1` STRAIGHT_JOIN `t2` ON `t1`.`a`>`t2`.`a`"),
        ("SELECT * FROM t1 CROSS JOIN t2", "SELECT * FROM `t1` JOIN `t2`"),
        ("SELECT * FROM t1 CROSS JOIN t2 ON t1.a>t2.a", "SELECT * FROM `t1` JOIN `t2` ON `t1`.`a`>`t2`.`a`"),
        ("SELECT * FROM t1 INNER JOIN t2 USING (b)", "SELECT * FROM `t1` JOIN `t2` USING (`b`)"),
        ("SELECT * FROM t1 JOIN t2 USING (b,c) LEFT JOIN t3 ON t1.a>t3.a", "SELECT * FROM (`t1` JOIN `t2` USING (`b`,`c`)) LEFT JOIN `t3` ON `t1`.`a`>`t3`.`a`"),
        ("SELECT * FROM t1 NATURAL JOIN t2 RIGHT OUTER JOIN t3 USING (b,c)", "SELECT * FROM (`t1` NATURAL JOIN `t2`) RIGHT JOIN `t3` USING (`b`,`c`)"),
        ("SELECT * FROM t1, t2", "SELECT * FROM (`t1`) JOIN `t2`"),
        ("SELECT * FROM t1, t2, t3", "SELECT * FROM ((`t1`) JOIN `t2`) JOIN `t3`"),
        ("SELECT * FROM (SELECT * FROM t) t1, (t2, t3)", "SELECT * FROM (SELECT * FROM `t`) AS `t1`, ((`t2`) JOIN `t3`)"),
        ("SELECT * FROM (SELECT * FROM t) t1, t2", "SELECT * FROM (SELECT * FROM `t`) AS `t1`, `t2`"),
        ("SELECT * FROM (SELECT * FROM (SELECT a FROM t1) tb1) tb", "SELECT * FROM (SELECT * FROM (SELECT `a` FROM `t1`) AS `tb1`) AS `tb`"),
        ("SELECT * FROM (SELECT * FROM t) t1 CROSS JOIN t2", "SELECT * FROM (SELECT * FROM `t`) AS `t1` JOIN `t2`"),
        ("SELECT * FROM (SELECT * FROM t) t1 NATURAL JOIN t2", "SELECT * FROM (SELECT * FROM `t`) AS `t1` NATURAL JOIN `t2`"),
        ("SELECT * FROM (SELECT * FROM t) t1 CROSS JOIN t2 ON t1.a>t2.a", "SELECT * FROM (SELECT * FROM `t`) AS `t1` JOIN `t2` ON `t1`.`a`>`t2`.`a`"),
        ("SELECT * FROM (SELECT * FROM t UNION SELECT * FROM t1) tb1, t2", "SELECT * FROM (SELECT * FROM `t` UNION SELECT * FROM `t1`) AS `tb1`, `t2`"),
        ("SELECT * FROM (SELECT a FROM t) t1 JOIN t t2, t3", "SELECT * FROM ((SELECT `a` FROM `t`) AS `t1` JOIN `t` AS `t2`) JOIN `t3`"),
        ("SELECT * FROM (a al LEFT JOIN b bl ON al.a1 > bl.b1) JOIN (a ar RIGHT JOIN b br ON ar.a1 > br.b1)", "SELECT * FROM (`a` AS `al` LEFT JOIN `b` AS `bl` ON `al`.`a1`>`bl`.`b1`) JOIN (`a` AS `ar` RIGHT JOIN `b` AS `br` ON `ar`.`a1`>`br`.`b1`)"),
        ("SELECT * FROM a al LEFT JOIN b bl ON al.a1 > bl.b1, a ar RIGHT JOIN b br ON ar.a1 > br.b1", "SELECT * FROM (`a` AS `al` LEFT JOIN `b` AS `bl` ON `al`.`a1`>`bl`.`b1`) JOIN (`a` AS `ar` RIGHT JOIN `b` AS `br` ON `ar`.`a1`>`br`.`b1`)"),
        ("SELECT * FROM t1 JOIN (t2 RIGHT JOIN t3 ON t2.a > t3.a JOIN (t4 RIGHT JOIN t5 ON t4.a > t5.a))", "SELECT * FROM `t1` JOIN ((`t2` RIGHT JOIN `t3` ON `t2`.`a`>`t3`.`a`) JOIN (`t4` RIGHT JOIN `t5` ON `t4`.`a`>`t5`.`a`))"),
        ("SELECT * FROM t1 JOIN t2 RIGHT JOIN t3 ON t2.a=t3.a", "SELECT * FROM (`t1` JOIN `t2`) RIGHT JOIN `t3` ON `t2`.`a`=`t3`.`a`"),
        ("SELECT * FROM t1 JOIN (t2 RIGHT JOIN t3 ON t2.a=t3.a)", "SELECT * FROM `t1` JOIN (`t2` RIGHT JOIN `t3` ON `t2`.`a`=`t3`.`a`)"),
    ]);
}
