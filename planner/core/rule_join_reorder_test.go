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

	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
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

func TestAdditionOtherConditionsRemained4OuterJoin(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `queries_identifier` (\n   `id` int(11) NOT NULL AUTO_INCREMENT,\n   `name` varchar(100) COLLATE utf8mb4_general_ci NOT NULL,\n   PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
	tk.MustExec("CREATE TABLE `queries_program` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `identifier_id` int(11) NOT NULL,\n  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n  UNIQUE KEY `identifier_id` (`identifier_id`),\n  CONSTRAINT `queries_program_identifier_id_70ff12a6_fk_queries_identifier_id` FOREIGN KEY (`identifier_id`) REFERENCES `test`.`queries_identifier` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
	tk.MustExec("CREATE TABLE `queries_channel` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `identifier_id` int(11) NOT NULL,\n  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n  UNIQUE KEY `identifier_id` (`identifier_id`),\n  CONSTRAINT `queries_channel_identifier_id_06ac3513_fk_queries_identifier_id` FOREIGN KEY (`identifier_id`) REFERENCES `test`.`queries_identifier` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")

	tk.MustExec("INSERT  INTO queries_identifier(`id`, `name`) values(13, 'i1'), (14, 'i2'), (15, 'i3');")
	tk.MustExec("INSERT  INTO queries_program(`id`, `identifier_id`) values(8, 13), (9, 14);")
	tk.MustExec("INSERT  INTO queries_channel(`id`, `identifier_id`) values(5, 13);")

	tk.MustExec("create table t(a int)")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int, c int)")
	tk.MustExec("create table t3(a int, b int)")
	tk.MustExec("create table t4(a int, b int)")

	testData := core.GetJoinReorderData()
	var (
		input  []string
		output []struct {
			SQL    string
			Output []string
		}
	)
	testData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Output = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Output...))
	}
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

// https://github.com/pingcap/tidb/issues/56704
func TestJoinReorderWithPossibleCostCalcOverflow(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test;")
	tk.MustExec("CREATE TABLE `lrr_test` ( `COL102` double DEFAULT NULL, `COL1` double GENERATED ALWAYS AS (`COL102` + 10) STORED NOT NULL, PRIMARY KEY (`COL1`) /*T![clustered_index] CLUSTERED */ ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into lrr_test (col102) values(-1.704648925036604e308), (-1.6888619680353582e308), (-1.6685908644498436e308), (-1.6311134967437805e308), (-1.6128280680807152e308), (-1.5899713947158026e308), (-1.5709457594070477e308), (-1.4925714566991343e308), (-1.4705985087370154e308), (-1.4451316666300040e308), (-1.3946576985986583e308), (-1.3695679630646804e308), (-1.3208992137984086e308), (-1.2887981369134862e308), (-1.2119996449796167e308), (-1.195172956104992e308), (-1.1929781068369925e308), (-1.1746351299417647e308), (-1.1237012620945195e308), (-1.1223448185004882e308), (-1.0974439629672084e308), (-1.0657654808610821e308), (-1.0582598945271716e308), (-1.0565276887850733e308), (-1.0416104832981696e308), (-1.0368741532690337e308), (-1.033521479407133e308), (-1.0232269544119505e308), (-9.31943312515408e307), (-9.05107332838438e307), (-8.276443475796885e307), (-7.845086666145396e307), (-7.664543340054255e307), (-7.235369799352141e307), (-7.047280050755922e307), (-6.62205033356235e307), (-6.35964999739255e307), (-5.989391229038818e307), (-5.974526205854541e307), (-5.798684586589338e307), (-4.98047732376121e307), (-4.4623979626128605e307), (-4.3248436443381234e307), (-3.3391152928792773e307), (-3.2694282487729395e307), (-3.2461091065368577e307), (-2.8613054009714654e307), (-2.7176814604572905e307), (-2.1301127705458223e307), (-1.7280065154718344e307), (-1.6743061442642827e307), (-4.862812928655648e306), (-3.3262533560429795e305), (4.124952267435051e305), (5.4576487694211726e306), (1.1237742400537221e307), (1.569984332645614e307), (1.7966188405412235e307), (1.8619233341238355e307), (2.1152066540419881e307), (2.1764927570795164e307), (2.99416682762135e307), (3.0545414962788647e307), (3.262967770716021e307), (3.288944887183685e307), (4.9025219351381e307), (5.250864486081297e307), (5.52054372134351e307), (6.311436996747818e307), (6.870852232080436e307), (7.501871137935436e307), (7.925709054822421e307), (8.438195254661318e307), (8.446731596918706e307), (9.43580947190119e307), (9.66735866233596e307), (1.0022043827847664e308), (1.020869767928594e308), (1.0327408606815872e308), (1.0402383684235906e308), (1.0690255622829305e308), (1.1623306052784659e308), (1.1906116361044565e308), (1.2221839628780758e308), (1.3112927565356536e308), (1.3307364382402157e308), (1.3646958839720612e308), (1.425066345632827e308), (1.4433864261103511e308), (1.5038532858735658e308), (1.5079450808097928e308), (1.553628680980576e308), (1.6241456663280369e308), (1.6295729949930798e308), (1.6328703529666413e308), (1.6832354056195887e308), (1.7017315016390902e308), (1.7134206410400048e308), (1.7240829054261275e308), (1.7257738639648862e308), (1.7262297095455299e308), (1.7905151735809062e308);")
	tk.MustExec("analyze table lrr_test;")

	result := tk.MustQuery("select t1.col1, t2.col1 from lrr_test as t1 right join lrr_test as t2 on t1.col1 = t2.col1 where t1.col1 >=0 order by t1.col1;")
	require.Equal(t, 49, len(result.Rows()))
	result = tk.MustQuery("explain select t1.col1, t2.col1 from lrr_test as t1 right join lrr_test as t2 on t1.col1 = t2.col1 where t1.col1 >=0 order by t1.col1;")
	// Layout of explain: operator, estimated rows, task type, access object, operator info.
	require.NotContains(t, "NaN", result.Rows()[0][1])
	require.NotContains(t, "CARTEISAN", result.Rows()[0][4])
}
