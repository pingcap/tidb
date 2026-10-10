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

//! OR (union) IndexMerge partials over the table path: Go
//! `accessPathsForConds` builds an `IsCommonHandlePath` partial for a
//! clustered composite primary key, ranged over the PRIMARY index's columns,
//! and keeps a partial whose range is empty.

use crate::tests_support::row_text;
use crate::Session;

fn plan_tree(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(&format!("explain format='plan_tree' {sql}")))
        .into_iter()
        .map(|row| format!("{} {} {}", row[0].trim(), row[2], row[3]))
        .collect()
}

/// `planner/core/casetest/index/index.test`'s clustered `t1`: each OR branch
/// on the primary key is a TableRangeScan partial, as TiDB records. Before,
/// only an integer handle could serve a branch, so the union was never built
/// and the whole table was read.
#[test]
fn a_clustered_common_handle_serves_an_or_branch() {
    let mut session = Session::new();
    session.run("set tidb_enable_clustered_index=on").unwrap();
    session
        .run("create table t1 (a int, b varchar(20), c decimal(40,10), d int, primary key(a,b), key(c))")
        .unwrap();
    session
        .run(r#"insert into t1 values (1,"111",1.1,11), (2,"222",2.2,12), (3,"333",3.3,13)"#)
        .unwrap();
    assert_eq!(
        plan_tree(
            &mut session,
            "select /*+ use_index_merge(t1 primary, c) */ * from t1 where t1.a >= 1 or t1.c = 2.2"
        ),
        vec![
            "IndexMerge  type: union",
            "├─TableRangeScan(Build) table:t1 range:[1,+inf], keep order:false, stats:pseudo",
            "├─IndexRangeScan(Build) table:t1, index:c(c) range:[2.2000000000,2.2000000000], keep order:false, stats:pseudo",
            "└─TableRowIDScan(Probe) table:t1 keep order:false, stats:pseudo",
        ]
    );
    assert_eq!(
        plan_tree(
            &mut session,
            "select /*+ use_index_merge(t1 primary, c) */ * from t1 where t1.a = 1 and t1.b = '111' or t1.c = 3.3"
        )[1],
        r#"├─TableRangeScan(Build) table:t1 range:[1 "111",1 "111"], keep order:false, stats:pseudo"#
    );
    assert_eq!(
        row_text(session.run(
            "select /*+ use_index_merge(t1 primary, c) */ a from t1 \
             where t1.a = 1 and t1.b = '111' or t1.c = 3.3 order by a"
        )),
        vec![vec!["1"], vec!["3"]]
    );
}

/// `planner/core/indexmerge_path.test`'s TestIssue52395, as TiDB records it.
/// The empty `col_38` range is still a partial (Go drops only a full range),
/// named TableFullScan as Go's `IsFullScan` names a scan with no ranges; it
/// had disqualified the union. It reads no keys, as Go's partial table
/// worker does (read as a full scan it returned both rows). The Projection's 7.32 rows are Go's
/// `Selectivity` with the MV index estimating `json_contains` (10 of 10000
/// rows, `getMaskAndSelectivityForMVIndex`) and the empty range clamped to
/// one row; `idx_17` leads with its hidden column, which the statistics
/// collection must resolve against every table column, not the pruned
/// schema. Before, the OR fell to the 0.8 default: 5325.47 rows.
#[test]
fn an_empty_common_handle_range_stays_an_or_partial() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (col_37 json DEFAULT NULL, \
             col_38 timestamp NOT NULL DEFAULT '2010-07-09 00:00:00', \
             UNIQUE KEY idx_14 (col_38,(cast(col_37 as unsigned array))), \
             PRIMARY KEY (col_38) /*T![clustered_index] CLUSTERED */, \
             UNIQUE KEY idx_17 ((cast(col_37 as unsigned array)),col_38))",
        )
        .unwrap();
    session
        .run("INSERT INTO t VALUES ('[6085355592952464235]','1971-01-16 00:00:00'), (NULL,'1980-06-27 00:00:00')")
        .unwrap();
    let sql = "SELECT /*+ use_index_merge(t)*/ MIN(col_37) FROM t \
               WHERE col_38 BETWEEN '1984-12-13' AND '1975-01-28' \
               OR JSON_CONTAINS(col_37, '138480458355390957') \
               GROUP BY col_38 HAVING col_38 != '1988-03-22'";
    let rows = row_text(session.run(&format!("explain format='brief' {sql}")))
        .into_iter()
        .map(|row| format!("{} {} {}", row[0].trim(), row[1], row[3]))
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            "Projection 7.32 ",
            "└─IndexMerge 6.66 ",
            "├─TableFullScan(Build) 0.00 table:t",
            "├─IndexRangeScan(Build) 10.00 table:t, index:idx_17(cast(`col_37` as unsigned array), col_38)",
            "└─Selection(Probe) 6.66 ",
            "└─TableRowIDScan 10.00 table:t",
        ]
    );
    assert!(row_text(session.run(sql)).is_empty());
}

/// `executor/explain.test`'s TestIssue49605: Go walks `PossibleAccessPaths`
/// with the table path first and `cmpAlternatives` keeps the first of equal
/// alternatives, so `h > 240817` reads through the clustered PRIMARY as a
/// TableRangeScan rather than the `idx_23 (h, f)` that ties with it; without
/// STREAM_AGG that union costs more than reading the table.
#[test]
fn a_table_partial_wins_a_tie_against_an_index() {
    let mut session = Session::new();
    session.run(r#"CREATE TABLE `t` (`a` mediumint(9) NOT NULL,`b` year(4) NOT NULL,`c` varbinary(62) NOT NULL,`d` text COLLATE utf8mb4_unicode_ci NOT NULL,`e` tinyint(4) NOT NULL DEFAULT '115',`f` smallint(6) DEFAULT '2675',`g` date DEFAULT '1981-09-17',`h` mediumint(8) unsigned NOT NULL,`i` varchar(384) CHARACTER SET gbk COLLATE gbk_bin DEFAULT NULL,UNIQUE KEY `idx_23` (`h`,`f`),PRIMARY KEY (`h`,`a`) /*T![clustered_index] CLUSTERED */,UNIQUE KEY `idx_25` (`h`,`i`(5),`e`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin PARTITION BY HASH (`h`) PARTITIONS 1"#).unwrap();
    session.run(r#"INSERT INTO `t` VALUES (2065948,1999,_binary '8jxN','rf',-54,-5656,'1987-07-03',259254,'7me坨'),(-8248164,2024,_binary 'zA5A','s)DAkX3',-93,-12983,'2027-12-18',299573,'LUf咲'),(-6131509,2023,_binary 'xdex#Y2','1th%h',-51,19149,'2013-10-28',428279,'矷莒X'),(7545837,1998,_binary 'PCVO','&(lJw6',30,4093,'1987-07-03',736235,'腏@TOIJ'),(-7449472,2029,_binary 'B7&jrl','EjbFfX!',80,-7590,'2011-11-03',765580,'堮ZQF_'),(-7176200,1988,_binary 'tiPglv7mX_#','CnCtNb',-25,NULL,'1987-07-03',842956,'Gq羣嗳殓'),(-115168,2036,_binary 'BqmX$-4It','!8#dvH',82,18787,'1991-09-20',921706,'椉2庘v'),(6665100,1987,_binary '4IJgk0fr4','(D',-73,28628,'1987-07-03',1149668,'摔玝S渉'),(-4065661,2021,_binary '8G%','xDO39xw#',-107,17356,'1970-12-20',1316239,'+0c35掬-阗'),(7622462,1990,_binary '&o+)s)D0','kjoS9Dzld',84,688,'1987-07-03',1403663,'$H鍿_M~'),(5269354,2018,_binary 'wq9hC8','s8XPrN+',-2,-31272,'2008-05-26',1534517,'y椁n躁Q'),(2065948,1982,_binary '8jxNjbksV','g$+i4dg',11,19800,'1987-07-03',1591457,'z^+H~薼A'),(4076971,2024,_binary '&!RrsH','7Mpvk',-63,-632,'2032-10-28',1611011,'鬰+EXmx'),(3522062,1981,_binary ')nq#!UiHKk8','j~wFe77ai',50,6951,'1987-07-03',1716854,'J'),(7859777,2012,_binary 'PBA5xgJ&G&','UM7o!u',18,-5978,'1987-07-03',1967012,'e)浢L獹'),(2065948,2028,_binary '8jxNjbk','JmsEki9t4',51,12002,'2017-12-23',1981288,'mp氏襚')"#).unwrap();
    assert_eq!(
        plan_tree(
            &mut session,
            r#"SELECT /*+ AGG_TO_COP() STREAM_AGG()*/ (NOT (`t`.`i`>=_UTF8MB4'j筧8') OR NOT (`t`.`i`=_UTF8MB4'暈lH忧ll6')) IS TRUE,MAX(`t`.`e`) AS `r0`,QUOTE(`t`.`i`) AS `r1` FROM `t` WHERE `t`.`h`>240817 OR `t`.`i` BETWEEN _UTF8MB4'WVz' AND _UTF8MB4'G#駧褉ZC領*lov' GROUP BY `t`.`i`"#
        )[3..],
        [
            "└─IndexMerge partition:all type: union",
            "├─TableRangeScan(Build) table:t range:(240817,+inf], keep order:false, stats:pseudo",
            "├─IndexFullScan(Build) table:t, index:idx_25(h, i, e) keep order:false, stats:pseudo",
            "└─TableRowIDScan(Probe) table:t keep order:false, stats:pseudo",
        ]
    );
    assert_eq!(
        plan_tree(&mut session, r#"SELECT /*+ AGG_TO_COP() */ (NOT (`t`.`i`>=_UTF8MB4'j筧8') OR NOT (`t`.`i`=_UTF8MB4'暈lH忧ll6')) IS TRUE,MAX(`t`.`e`) AS `r0`,QUOTE(`t`.`i`) AS `r1` FROM `t` WHERE `t`.`h`>240817 OR `t`.`i` BETWEEN _UTF8MB4'WVz' AND _UTF8MB4'G#駧褉ZC領*lov' GROUP BY `t`.`i`"#).last().map(String::as_str),
        Some("└─TableFullScan table:t keep order:false, stats:pseudo")
    );
}
