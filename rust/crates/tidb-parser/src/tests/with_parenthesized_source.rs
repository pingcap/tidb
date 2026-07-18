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

//! Source-owned restore coverage for a parenthesized outer query after WITH.

use super::*;

const SQL: &str = "with cte_192 ( col_1101,col_1102,col_1103,col_1104 ) AS ( select  /*+ use_index_merge( tl6e913fb9 ) */   replace( tl6e913fb9.col_36 , tl6e913fb9.col_36 , tl6e913fb9.col_36 ) as r0 , space( 0 ) as r1 , min( distinct  tl6e913fb9.col_36 ) as r2 , count( distinct  tl6e913fb9.col_36 ) as r3 from tl6e913fb9 where tl6e913fb9.col_36 between 'n92ok$B%W#UU%O' and '()c=KVQ=T%-vzGJ' and tl6e913fb9.col_36 in ( 'T+kf' ,'Lvluod2H' ,'3#Omx@pC^fFkeH' ,'=b$z' ) group by tl6e913fb9.col_36  having tl6e913fb9.col_36 = 'xjV@' or IsNull( tl6e913fb9.col_36 ) ) ( select 1,col_1101,col_1102,col_1103,col_1104 from cte_192 where not( IsNull( cte_192.col_1102 ) ) order by 1,2,3,4,5 limit 72850972 )";

#[test]
fn with_parenthesized_outer_query_restores_go_shape() {
    assert_eq!(
        parse(SQL).unwrap().restore(),
        "WITH `cte_192` (`col_1101`, `col_1102`, `col_1103`, `col_1104`) AS (SELECT /*+ USE_INDEX_MERGE(`tl6e913fb9`)*/ REPLACE(`tl6e913fb9`.`col_36`, `tl6e913fb9`.`col_36`, `tl6e913fb9`.`col_36`) AS `r0`,SPACE(0) AS `r1`,MIN(DISTINCT `tl6e913fb9`.`col_36`) AS `r2`,COUNT(DISTINCT `tl6e913fb9`.`col_36`) AS `r3` FROM `tl6e913fb9` WHERE `tl6e913fb9`.`col_36` BETWEEN _UTF8MB4'n92ok$B%W#UU%O' AND _UTF8MB4'()c=KVQ=T%-vzGJ' AND `tl6e913fb9`.`col_36` IN (_UTF8MB4'T+kf',_UTF8MB4'Lvluod2H',_UTF8MB4'3#Omx@pC^fFkeH',_UTF8MB4'=b$z') GROUP BY `tl6e913fb9`.`col_36` HAVING `tl6e913fb9`.`col_36`=_UTF8MB4'xjV@' OR ISNULL(`tl6e913fb9`.`col_36`)) (SELECT 1,`col_1101`,`col_1102`,`col_1103`,`col_1104` FROM `cte_192` WHERE NOT (ISNULL(`cte_192`.`col_1102`)) ORDER BY 1,2,3,4,5 LIMIT 72850972)"
    );
}
