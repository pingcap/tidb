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

#[test]
/// Direct Go corpus rows from `rule_join_reorder.test:529,530,533` retain
/// recursive `ast.LeadingList` groups in the EXPLAIN wrapper restore.
fn explain_plan_tree_nested_leading_rows_restore_like_go() {
    assert_eq!(
        r("explain format='plan_tree' select /*+ leading((t1, t2), sub) */ * from t1 join t2 on t1.a=t2.a join (select t3.a, t3.b from t3 join t4 on t3.a=t4.a join t5 on t4.b=t5.b) sub on t2.b=sub.b and t1.a=sub.a"),
        "EXPLAIN FORMAT = 'plan_tree' SELECT /*+ LEADING((`t1`, `t2`), `sub`)*/ * FROM (`t1` JOIN `t2` ON `t1`.`a`=`t2`.`a`) JOIN (SELECT `t3`.`a`,`t3`.`b` FROM (`t3` JOIN `t4` ON `t3`.`a`=`t4`.`a`) JOIN `t5` ON `t4`.`b`=`t5`.`b`) AS `sub` ON `t2`.`b`=`sub`.`b` AND `t1`.`a`=`sub`.`a`"
    );
    assert_eq!(
        r("explain format='plan_tree' select /*+ leading((t1, sub), t2) */ * from t1 join t2 on t1.a=t2.a join (select t3.a, t3.b from t3 join t4 on t3.a=t4.a join t5 on t4.b=t5.b) sub on t2.b=sub.b and t1.a=sub.a"),
        "EXPLAIN FORMAT = 'plan_tree' SELECT /*+ LEADING((`t1`, `sub`), `t2`)*/ * FROM (`t1` JOIN `t2` ON `t1`.`a`=`t2`.`a`) JOIN (SELECT `t3`.`a`,`t3`.`b` FROM (`t3` JOIN `t4` ON `t3`.`a`=`t4`.`a`) JOIN `t5` ON `t4`.`b`=`t5`.`b`) AS `sub` ON `t2`.`b`=`sub`.`b` AND `t1`.`a`=`sub`.`a`"
    );
    assert_eq!(
        r("explain format='plan_tree' select /*+ leading(t1, (t2, sub)) */ * from t1 join t2 on t1.a=t2.a join (select t3.a, t3.b from t3 join t4 on t3.a=t4.a join t5 on t4.b=t5.b) sub on t2.b=sub.b and t1.a=sub.a"),
        "EXPLAIN FORMAT = 'plan_tree' SELECT /*+ LEADING(`t1`, (`t2`, `sub`))*/ * FROM (`t1` JOIN `t2` ON `t1`.`a`=`t2`.`a`) JOIN (SELECT `t3`.`a`,`t3`.`b` FROM (`t3` JOIN `t4` ON `t3`.`a`=`t4`.`a`) JOIN `t5` ON `t4`.`b`=`t5`.`b`) AS `sub` ON `t2`.`b`=`sub`.`b` AND `t1`.`a`=`sub`.`a`"
    );
}

#[test]
/// `predicate_simplification.test:345` contains an unterminated
/// `NO_HASH_JOIN` hint that Go warns about and drops while accepting EXPLAIN.
fn explain_plan_tree_malformed_no_hash_join_hint_is_dropped() {
    let sql = "explain format='plan_tree' select /*+ NO_HASH_JOIN(t1, t2 */ * from t1 join t2 on t1.a=t2.a";
    let restored = r(sql);
    assert_eq!(
        restored,
        "EXPLAIN FORMAT = 'plan_tree' SELECT * FROM `t1` JOIN `t2` ON `t1`.`a`=`t2`.`a`"
    );
}

#[test]
/// `planner/core/plan.test:4` exercises the EXPLAIN `hint` format with
/// query-block-qualified index and aggregate hints. Keep this source row
/// explicit while the restore contract is closed against Go.
fn explain_hint_plan_row_restores_like_go() {
    let sql = "explain format='hint'select /*+ use_index(@`sel_2` `test`.`t2` `idx_c2`), hash_agg(@`sel_2`), use_index(@`sel_1` `test`.`t1` `idx_c2`), hash_agg(@`sel_1`) */ count(1) from t t1 where c2 in (select c2 from t t2 where t2.c2 < 15 and t2.c2 > 12)";
    assert_eq!(
        r(sql),
        "EXPLAIN FORMAT = 'hint' SELECT /*+ USE_INDEX(@`sel_2` `test`.`t2` `idx_c2`) HASH_AGG(@`sel_2`) USE_INDEX(@`sel_1` `test`.`t1` `idx_c2`) HASH_AGG(@`sel_1`)*/ COUNT(1) FROM `t` AS `t1` WHERE `c2` IN (SELECT `c2` FROM `t` AS `t2` WHERE `t2`.`c2`<15 AND `t2`.`c2`>12)"
    );
}
