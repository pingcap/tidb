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

/// Mirrors `pkg/parser/parser_test.go::TestTableSample`.
#[test]
fn test_table_sample() {
    for (sql, expected) in [
        ("select * from tbl tablesample system (50);", "SELECT * FROM `tbl` TABLESAMPLE SYSTEM (50)"),
        ("select * from tbl tablesample system (50 percent);", "SELECT * FROM `tbl` TABLESAMPLE SYSTEM (50 PERCENT)"),
        ("select * from tbl tablesample system (49.9 percent);", "SELECT * FROM `tbl` TABLESAMPLE SYSTEM (49.9 PERCENT)"),
        ("select * from tbl tablesample system (120 rows);", "SELECT * FROM `tbl` TABLESAMPLE SYSTEM (120 ROWS)"),
        ("select * from tbl tablesample bernoulli (50);", "SELECT * FROM `tbl` TABLESAMPLE BERNOULLI (50)"),
        ("select * from tbl tablesample (50);", "SELECT * FROM `tbl` TABLESAMPLE (50)"),
        ("select * from tbl tablesample (50) repeatable (123456789);", "SELECT * FROM `tbl` TABLESAMPLE (50) REPEATABLE(123456789)"),
        ("select * from tbl as a tablesample (50);", "SELECT * FROM `tbl` AS `a` TABLESAMPLE (50)"),
        ("select * from tbl `tablesample` tablesample (50);", "SELECT * FROM `tbl` AS `tablesample` TABLESAMPLE (50)"),
        ("select * from tbl tablesample (50) where id > 20;", "SELECT * FROM `tbl` TABLESAMPLE (50) WHERE `id`>20"),
        ("select * from tbl partition (p0) tablesample (50);", "SELECT * FROM `tbl` PARTITION(`p0`) TABLESAMPLE (50)"),
        ("select * from tbl tablesample (0 percent);", "SELECT * FROM `tbl` TABLESAMPLE (0 PERCENT)"),
        ("select * from tbl tablesample (100 percent);", "SELECT * FROM `tbl` TABLESAMPLE (100 PERCENT)"),
        ("select * from tbl tablesample (0 rows);", "SELECT * FROM `tbl` TABLESAMPLE (0 ROWS)"),
        ("select * from tbl tablesample ('34');", "SELECT * FROM `tbl` TABLESAMPLE (_UTF8MB4'34')"),
        ("select * from tbl1 tablesample (10), tbl2 tablesample (20);", "SELECT * FROM (`tbl1` TABLESAMPLE (10)) JOIN `tbl2` TABLESAMPLE (20)"),
        ("select * from tbl1 a tablesample (10) join tbl2 b tablesample (20) on a.id <> b.id;", "SELECT * FROM `tbl1` AS `a` TABLESAMPLE (10) JOIN `tbl2` AS `b` TABLESAMPLE (20) ON `a`.`id`!=`b`.`id`"),
        ("select * from demo tablesample bernoulli(50) limit 1 into outfile '/tmp/sample.csv';", "SELECT * FROM `demo` TABLESAMPLE BERNOULLI (50) LIMIT 1 INTO OUTFILE '/tmp/sample.csv'"),
        ("select * from demo tablesample bernoulli(50) order by a, b into outfile '/tmp/sample.csv';", "SELECT * FROM `demo` TABLESAMPLE BERNOULLI (50) ORDER BY `a`,`b` INTO OUTFILE '/tmp/sample.csv'"),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }

    for sql in [
        "select * from tbl tablesample system(50) a;",
        "select * from tbl tablesample (50) partition (p0);",
        "select * from tbl where id > 20 tablesample system(50);",
        "select * from (select * from tbl) a tablesample system(50);",
        "select * from tbl tablesample system(50) tablesample system(50);",
        "select * from tbl tablesample system(50, 50);",
        "select * from tbl tablesample dhfksdlfljcoew(50);",
        "select * from tbl tablesample system;",
        "select * from tbl tablesample system (33) repeatable;",
        "select 1 from dual tablesample system (50);",
    ] {
        assert!(parse(sql).is_err(), "expected parse error for: {sql}");
    }

    for sql in [
        "select * from tbl tablesample (33.3 + 44.4);",
        "select * from tbl tablesample (33.3 + 44.4 percent);",
        "select * from tbl tablesample (33 + 44 rows);",
        "select * from tbl tablesample (33 + 44 rows) repeatable (55 + 66);",
        "select * from tbl tablesample (200);",
        "select * from tbl tablesample (-10);",
        "select * from tbl tablesample (null);",
        "select * from tbl tablesample (33.3 rows);",
        "select * from tbl tablesample (-4 rows);",
        "select * from tbl tablesample (50) repeatable ('ssss');",
        "delete from tbl using tbl2 tablesample(10 rows) repeatable (111) where tbl.id = tbl2.id",
        "update tbl tablesample regions() set id = '1'",
    ] {
        assert!(parse(sql).is_ok(), "expected parse success for: {sql}");
    }
}
