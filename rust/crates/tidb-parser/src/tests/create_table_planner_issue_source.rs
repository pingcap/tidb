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

//! Source-owned CREATE TABLE restore shape from planner_issue.test.

use super::*;

#[test]
fn planner_issue_create_table_restore_matches_go() {
    let sql = "CREATE TABLE `t_yzyyqbo2u` (\n`c_c4l` int(11) DEFAULT NULL,\n`c_yb_` text DEFAULT NULL,\n`c_pq4c1la6cv` int(11) NOT NULL,\n`c_kbcid` int(11) DEFAULT NULL,\n`c_um` double DEFAULT NULL,\n`c_zjmgh995_6` text DEFAULT NULL,\n`c_fujjmh8m2` double NOT NULL,\n`c_qkf4n` double DEFAULT NULL,\n`c__x9cqrnb0` double NOT NULL,\n`c_b5qjz_jj0` double DEFAULT NULL,\nPRIMARY KEY (`c_pq4c1la6cv`) /*T![clustered_index] NONCLUSTERED */,\nUNIQUE KEY `c__x9cqrnb0` (`c__x9cqrnb0`),\nUNIQUE KEY `c_b5qjz_jj0` (`c_b5qjz_jj0`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=2 */;";
    let got = parse(sql).expect("CREATE TABLE must parse").restore();
    let want = "CREATE TABLE `t_yzyyqbo2u` (`c_c4l` INT(11) DEFAULT NULL,`c_yb_` TEXT DEFAULT NULL,`c_pq4c1la6cv` INT(11) NOT NULL,`c_kbcid` INT(11) DEFAULT NULL,`c_um` DOUBLE DEFAULT NULL,`c_zjmgh995_6` TEXT DEFAULT NULL,`c_fujjmh8m2` DOUBLE NOT NULL,`c_qkf4n` DOUBLE DEFAULT NULL,`c__x9cqrnb0` DOUBLE NOT NULL,`c_b5qjz_jj0` DOUBLE DEFAULT NULL,PRIMARY KEY(`c_pq4c1la6cv`) NONCLUSTERED,UNIQUE `c__x9cqrnb0`(`c__x9cqrnb0`),UNIQUE `c_b5qjz_jj0`(`c_b5qjz_jj0`)) ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_BIN SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 2";
    assert_eq!(got, want);
}
