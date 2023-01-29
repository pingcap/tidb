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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func runJoinReorderTestData(t *testing.T, tk *testkit.TestKit, name string) {
	var input []string
	var output []struct {
		SQL     string
		Plan    []string
		Warning []string
	}
	joinReorderSuiteData := plannercore.GetJoinReorderSuiteData()
	joinReorderSuiteData.LoadTestCasesByName(name, t, &input, &output)
	require.Equal(t, len(input), len(output))
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + input[i]).Rows())
			output[i].Warning = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain format = 'brief' " + input[i]).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warning...))
	}
}

func TestStraightJoinHint(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t, t1, t2, t3, t4;")
	tk.MustExec("create table t(a int, b int, key(a));")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")
	tk.MustExec("create table t4(a int, b int, key(a));")
	runJoinReorderTestData(t, tk, "TestStraightJoinHint")
}

func TestLeadingJoinHint(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t, t1, t2, t3, t4, t5, t6, t7, t8;")
	tk.MustExec("create table t(a int, b int, key(a));")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")
	tk.MustExec("create table t4(a int, b int, key(a));")
	tk.MustExec("create table t5(a int, b int, key(a));")
	tk.MustExec("create table t6(a int, b int, key(a));")
	tk.MustExec("create table t7(a int, b int, key(a));")
	tk.MustExec("create table t8(a int, b int, key(a));")
	runJoinReorderTestData(t, tk, "TestLeadingJoinHint")

	// test cases for multiple leading hints
	tk.MustExec("select /*+ leading(t1) leading(t2) */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid"))
}

func TestJoinOrderHint(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2, t3, t4, t5, t6, t7, t8;")
	tk.MustExec("create table t(a int, b int, key(a));")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")
	tk.MustExec("create table t4(a int, b int, key(a));")
	tk.MustExec("create table t5(a int, b int, key(a));")
	tk.MustExec("create table t6(a int, b int, key(a));")
	tk.MustExec("create table t7(a int, b int, key(a));")
	tk.MustExec("create table t8(a int, b int, key(a));")

	// test cases for using the leading hint and straight_join hint at the same time
	tk.MustExec("select /*+ leading(t1) straight_join() */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 We can only use the straight_join hint, when we use the leading hint and straight_join hint at the same time, all leading hints will be invalid"))

	tk.MustExec("select /*+ straight_join() leading(t1) */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 We can only use the straight_join hint, when we use the leading hint and straight_join hint at the same time, all leading hints will be invalid"))

	// more join order hints appear in the same time
	tk.MustExec("select /*+ leading(t1) leading(t1) */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid"))

	tk.MustExec("select /*+ leading(t1) leading(t2) */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid"))

	tk.MustExec("select /*+ straight_join() straight_join() */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 STRAIGHT_JOIN() is defined more than once, only the last definition takes effect"))

	// test cases for table name in hint
	// the same table appears in the leading hint
	tk.MustExec("select /*+ leading(t1, t1) */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 There are no matching table names for (t1) in optimizer hint /*+ LEADING(t1, t1) */. Maybe you can use the table alias name",
		"Warning 1815 leading hint is inapplicable, check if the leading hint table is valid"))

	tk.MustExec("select /*+ leading(t1, t2, t1) */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 There are no matching table names for (t1) in optimizer hint /*+ LEADING(t1, t2, t1) */. Maybe you can use the table alias name",
		"Warning 1815 leading hint is inapplicable, check if the leading hint table is valid"))

	// the wrong table appears in the leading hint
	tk.MustExec("select /*+ leading(t) */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 There are no matching table names for (t) in optimizer hint /*+ LEADING(t) */. Maybe you can use the table alias name"))

	tk.MustExec("select /*+ leading(t1, t2, t) */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 There are no matching table names for (t) in optimizer hint /*+ LEADING(t1, t2, t) */. Maybe you can use the table alias name",
		"Warning 1815 leading hint is inapplicable, check if the leading hint table is valid"))

	// table alias in the leading hint
	tk.MustExec("select /*+ leading(t) */ * from t1 t join t2 on t.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustExec("select /*+ leading(t1) */ * from t1 t join t2 on t.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 There are no matching table names for (t1) in optimizer hint /*+ LEADING(t1) */. Maybe you can use the table alias name"))

	tk.MustExec("select /*+ leading(t2, t) */ * from t1 t join t2 on t.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustExec("select /*+ leading(t2, t1) */ * from t1 t join t2 on t.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 There are no matching table names for (t1) in optimizer hint /*+ LEADING(t2, t1) */. Maybe you can use the table alias name",
		"Warning 1815 leading hint is inapplicable, check if the leading hint table is valid"))

	// table name in leading hint cross query block
	// Todo: Can not handle this case yet. Because when we extract the join group, it will get the join group {t1, t2, t3}.
	// So the table 't4' can not be used.
	tk.MustExec("select /*+ leading(t4) */ * from (select t2.b from t1 join t2 on t1.a=t2.a) t4 join t3 on t4.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 leading hint is inapplicable, check if the leading hint table is valid"))

	tk.MustExec("select /*+ leading(t3, t2@sel_2) */ * from (select t2.b from t1 join t2 on t1.a=t2.a) t4 join t3 on t4.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 There are no matching table names for (t2) in optimizer hint /*+ LEADING(t3, t2) */. Maybe you can use the table alias name"))

	tk.MustExec("select * from (select /*+ leading(t1, t3@sel_1) */ t2.b from t1 join t2 on t1.a=t2.a) t4 join t3 on t4.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 There are no matching table names for (t3) in optimizer hint /*+ LEADING(t1, t3) */. Maybe you can use the table alias name"))

	tk.MustExec("select /*+ leading(t3) */ * from (select /*+ leading(t1) */ t2.b from t1 join t2 on t1.a=t2.a) t4 join t3 on t4.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid"))

	runJoinReorderTestData(t, tk, "TestJoinOrderHint")
}

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

func TestJoinOrderHint4StaticPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t, t1, t2, t3;")
	tk.MustExec(`create table t(a int, b int) partition by hash(a) partitions 3`)
	tk.MustExec(`create table t1(a int, b int) partition by hash(a) partitions 4`)
	tk.MustExec(`create table t2(a int, b int) partition by hash(a) partitions 5`)
	tk.MustExec(`create table t3(a int, b int) partition by hash(b) partitions 3`)
	tk.MustExec(`create table t4(a int, b int) partition by hash(a) partitions 4`)
	tk.MustExec(`create table t5(a int, b int) partition by hash(a) partitions 5`)
	tk.MustExec(`create table t6(a int, b int) partition by hash(b) partitions 3`)

	tk.MustExec(`set @@tidb_partition_prune_mode="static"`)
	tk.MustExec("set @@tidb_enable_outer_join_reorder=true")
	runJoinReorderTestData(t, tk, "TestJoinOrderHint4StaticPartitionTable")
}

func TestJoinOrderHint4DynamicPartitionTable(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2, t3;")
	tk.MustExec(`create table t(a int, b int) partition by hash(a) partitions 3`)
	tk.MustExec(`create table t1(a int, b int) partition by hash(a) partitions 4`)
	tk.MustExec(`create table t2(a int, b int) partition by hash(a) partitions 5`)
	tk.MustExec(`create table t3(a int, b int) partition by hash(b) partitions 3`)
	tk.MustExec(`create table t4(a int, b int) partition by hash(a) partitions 4`)
	tk.MustExec(`create table t5(a int, b int) partition by hash(a) partitions 5`)
	tk.MustExec(`create table t6(a int, b int) partition by hash(b) partitions 3`)

	tk.MustExec(`set @@tidb_partition_prune_mode="dynamic"`)
	tk.MustExec("set @@tidb_enable_outer_join_reorder=true")
	runJoinReorderTestData(t, tk, "TestJoinOrderHint4DynamicPartitionTable")
}

func TestJoinOrderHint4DifferentJoinType(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t, t1, t2, t3, t4, t5, t6, t7, t8;")
	tk.MustExec("create table t(a int, b int, key(a));")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")
	tk.MustExec("create table t4(a int, b int, key(a));")
	tk.MustExec("create table t5(a int, b int, key(a));")
	tk.MustExec("create table t6(a int, b int, key(a));")
	tk.MustExec("create table t7(a int, b int, key(a));")
	tk.MustExec("create table t8(a int, b int, key(a));")
	tk.MustExec("set @@tidb_enable_outer_join_reorder=true")

	runJoinReorderTestData(t, tk, "TestJoinOrderHint4DifferentJoinType")
}

func TestJoinOrderHint4TiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2, t3;")
	tk.MustExec("create table t(a int, b int, key(a));")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")
	tk.MustExec("create table t4(a int, b int, key(a));")
	tk.MustExec("create table t5(a int, b int, key(a));")
	tk.MustExec("create table t6(a int, b int, key(a));")
	tk.MustExec("set @@tidb_enable_outer_join_reorder=true")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		tableName := tblInfo.Name.L
		if tableName == "t" || tableName == "t1" || tableName == "t2" || tableName == "t3" || tableName == "t4" || tableName == "t5" || tableName == "t6" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	runJoinReorderTestData(t, tk, "TestJoinOrderHint4TiFlash")
}

func TestJoinOrderHint4Subquery(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t, t1, t2, t3, t4, t5, t6, t7, t8;")
	tk.MustExec("create table t(a int, b int, key(a));")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")
	tk.MustExec("create table t4(a int, b int, key(a));")
	tk.MustExec("create table t5(a int, b int, key(a));")
	tk.MustExec("create table t6(a int, b int, key(a));")
	tk.MustExec("create table t7(a int, b int, key(a));")
	tk.MustExec("create table t8(a int, b int, key(a));")
	tk.MustExec("insert into t3 values(1, 1), (2, 2), (3, 3);")
	tk.MustExec("analyze table t3;")

	runJoinReorderTestData(t, tk, "TestJoinOrderHint4Subquery")
}

func TestLeadingJoinHint4OuterJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec(`set @@tidb_opt_advanced_join_hint=0`)
	tk.MustExec("drop table if exists t, t1, t2, t3, t4, t5, t6, t7, t8;")
	tk.MustExec("create table t(a int, b int, key(a));")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")
	tk.MustExec("create table t4(a int, b int, key(a));")
	tk.MustExec("create table t5(a int, b int, key(a));")
	tk.MustExec("create table t6(a int, b int, key(a));")
	tk.MustExec("create table t7(a int, b int, key(a));")
	tk.MustExec("create table t8(a int, b int, key(a));")
	tk.MustExec("set @@tidb_enable_outer_join_reorder=true")
	runJoinReorderTestData(t, tk, "TestLeadingJoinHint4OuterJoin")
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
