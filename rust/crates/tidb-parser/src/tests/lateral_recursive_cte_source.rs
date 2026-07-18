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

//! Source-backed recursive CTE/LATERAL vectors from TiDB's planner tests.

use super::*;
use tidb_ast::{QueryStmt, Stmt};

const SOURCE_ROWS: &[(&str, &str)] = &[
    (
        "with recursive tr (level, b_col) as (select 1, '' union all select level + 1, lat.result_col from tr, lateral (select b as result_col from t2 where a = level order by b limit 1) as lat where level < 3) select * from tr where level > 0 order by level",
        "WITH RECURSIVE `tr` (`level`, `b_col`) AS (SELECT 1,_UTF8MB4'' UNION ALL SELECT `level`+1,`lat`.`result_col` FROM (`tr`) JOIN LATERAL (SELECT `b` AS `result_col` FROM `t2` WHERE `a`=`level` ORDER BY `b` LIMIT 1) AS `lat` WHERE `level`<3) SELECT * FROM `tr` WHERE `level`>0 ORDER BY `level`",
    ),
    (
        "with recursive tr (level, b_col) as (select 1, cast(null as char(10)) union all select level + 1, lat.result_col from tr, lateral (select b as result_col from t2 where a = level order by b limit 1) as lat where level < 3) select * from tr where level > 0 order by level",
        "WITH RECURSIVE `tr` (`level`, `b_col`) AS (SELECT 1,CAST(NULL AS CHAR(10)) UNION ALL SELECT `level`+1,`lat`.`result_col` FROM (`tr`) JOIN LATERAL (SELECT `b` AS `result_col` FROM `t2` WHERE `a`=`level` ORDER BY `b` LIMIT 1) AS `lat` WHERE `level`<3) SELECT * FROM `tr` WHERE `level`>0 ORDER BY `level`",
    ),
];

#[test]
fn recursive_lateral_cte_source_rows_restore_like_go() {
    for (sql, expected) in SOURCE_ROWS {
        let statement = parse(sql).expect("recursive LATERAL CTE parses");
        assert_eq!(statement.restore(), *expected);

        let Stmt::Query(query) = statement else {
            panic!("expected query statement")
        };
        let QueryStmt::Select(select) = *query else {
            panic!("expected SELECT query")
        };
        let Some(with) = select.with else {
            panic!("expected WITH clause")
        };
        assert!(with.recursive);
        assert_eq!(with.ctes.len(), 1);
        assert_eq!(with.ctes[0].name, "tr");
        assert_eq!(with.ctes[0].columns, ["level", "b_col"]);
    }
}
