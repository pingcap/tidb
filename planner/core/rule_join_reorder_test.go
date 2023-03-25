// Copyright 2022 PingCAP, Inc.
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

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestJoinOrderHintWithBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2, t3;")
	tk.MustExec("create table t(a int, b int, key(a));")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")

	tk.MustExec("select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	tk.MustExec("create global binding for select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b using select /*+ straight_join() */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustExec("select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res := tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select * from ( `test` . `t1` join `test` . `t2` on `t1` . `a` = `t2` . `a` ) join `test` . `t3` on `t2` . `b` = `t3` . `b`", "SELECT /*+ straight_join()*/ * FROM (`test`.`t1` JOIN `test`.`t2` ON `t1`.`a` = `t2`.`a`) JOIN `test`.`t3` ON `t2`.`b` = `t3`.`b`")

	tk.MustExec("drop global binding for select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustExec("select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, len(res), 0)

	tk.MustExec("create global binding for select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b using select /*+ leading(t3) */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustExec("select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select * from ( `test` . `t1` join `test` . `t2` on `t1` . `a` = `t2` . `a` ) join `test` . `t3` on `t2` . `b` = `t3` . `b`")

	tk.MustExec("drop global binding for select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")

	// test for outer join
	tk.MustExec("select * from t1 join t2 on t1.a=t2.a left join t3 on t2.b=t3.b")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, len(res), 0)

	tk.MustExec("create global binding for select * from t1 join t2 on t1.a=t2.a left join t3 on t2.b=t3.b using select /*+ leading(t2) */ * from t1 join t2 on t1.a=t2.a left join t3 on t2.b=t3.b")
	tk.MustExec("select * from t1 join t2 on t1.a=t2.a left join t3 on t2.b=t3.b")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select * from ( `test` . `t1` join `test` . `t2` on `t1` . `a` = `t2` . `a` ) left join `test` . `t3` on `t2` . `b` = `t3` . `b`")

	tk.MustExec("drop global binding for select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
}

func TestOuterJoinWIthEqCondCrossInnerJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `t1` (`data_status` tinyint(1) DEFAULT '0',`part` tinyint(255) unsigned DEFAULT NULL);")
	tk.MustExec("CREATE TABLE `t2` (`id` bigint(20) NOT NULL AUTO_INCREMENT,`routing_rule_switch` tinyint(1) DEFAULT '0',PRIMARY KEY (`id`));")
	tk.MustExec("CREATE TABLE `t3` (`fk_id` bigint(20) DEFAULT NULL,`offer_pbu_id` varchar(255) DEFAULT NULL ,`market_id` smallint(6) DEFAULT NULL ,`te_partition` tinyint(255) DEFAULT NULL ,UNIQUE KEY `t_pbu_partition_id` (`offer_pbu_id`,`market_id`,`te_partition`));")
	tk.MustExec("insert into t1 values(1,1);")
	tk.MustExec("insert into t2 values(1,0);")
	tk.MustExec("insert into t3 values(8,'a',3,6);")

	sql := `
SELECT tt.market_id,
	tt.offer_pbu_id
FROM   t3 tt
	RIGHT JOIN (SELECT pp.offer_pbu_id,
			   pp.market_id,
			   t.partition_no
		    FROM   (SELECT p.offer_pbu_id,
				   p.market_id
			    FROM   t3 p
				   INNER JOIN t2 e
					   ON p.fk_id = e.id
					      AND e.routing_rule_switch = 1) pp,
			   (SELECT part AS partition_no
			    FROM   t1) t) o
		ON tt.market_id = o.market_id
		   AND tt.offer_pbu_id = o.offer_pbu_id
		   AND tt.te_partition = o.partition_no;`
	tk.MustQuery(sql).Check(testkit.Rows())
	tk.MustQuery("explain format=brief" + sql).Check(testkit.Rows(
		"Projection 155781.72 root  test.t3.market_id, test.t3.offer_pbu_id",
		"└─HashJoin 155781.72 root  right outer join, equal:[eq(test.t3.market_id, test.t3.market_id) eq(test.t3.offer_pbu_id, test.t3.offer_pbu_id) eq(test.t3.te_partition, test.t1.part)]",
		"  ├─IndexReader(Build) 9970.03 root  index:Selection",
		"  │ └─Selection 9970.03 cop[tikv]  not(isnull(test.t3.market_id)), not(isnull(test.t3.te_partition))",
		"  │   └─IndexFullScan 9990.00 cop[tikv] table:tt, index:t_pbu_partition_id(offer_pbu_id, market_id, te_partition) keep order:false, stats:pseudo",
		"  └─HashJoin(Probe) 125000.00 root  CARTESIAN inner join",
		"    ├─HashJoin(Build) 12.50 root  inner join, equal:[eq(test.t2.id, test.t3.fk_id)]",
		"    │ ├─TableReader(Build) 10.00 root  data:Selection",
		"    │ │ └─Selection 10.00 cop[tikv]  eq(test.t2.routing_rule_switch, 1)",
		"    │ │   └─TableFullScan 10000.00 cop[tikv] table:e keep order:false, stats:pseudo",
		"    │ └─TableReader(Probe) 9990.00 root  data:Selection",
		"    │   └─Selection 9990.00 cop[tikv]  not(isnull(test.t3.fk_id))",
		"    │     └─TableFullScan 10000.00 cop[tikv] table:p keep order:false, stats:pseudo",
		"    └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
	))
}
