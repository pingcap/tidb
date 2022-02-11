// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by aprettyPrintlicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core_test

import (
	"bytes"
	"fmt"
	"strings"
	"time"

<<<<<<< HEAD
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
=======
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/types"
>>>>>>> f949e01e0... planner, expression: pushdown AggFuncMode to coprocessor (#31392)
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testPlanNormalize{})

type testPlanNormalize struct {
	store kv.Storage
	dom   *domain.Domain

	testData testutil.TestData
}

func (s *testPlanNormalize) SetUpSuite(c *C) {
	testleak.BeforeTest()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.store = store
	s.dom = dom

	s.testData, err = testutil.LoadTestSuiteData("testdata", "plan_normalized_suite")
	c.Assert(err, IsNil)
}

func (s *testPlanNormalize) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testPlanNormalize) TestNormalizedPlan(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2,t3,t4")
	tk.MustExec("create table t1 (a int key,b int,c int, index (b));")
	tk.MustExec("create table t2 (a int key,b int,c int, index (b));")
	tk.MustExec("create table t3 (a int key,b int) partition by hash(a) partitions 2;")
	tk.MustExec("create table t4 (a int, b int, index(a)) partition by range(a) (partition p0 values less than (10),partition p1 values less than MAXVALUE);")
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		tk.Se.GetSessionVars().PlanID = 0
		tk.MustExec(tt)
		info := tk.Se.ShowProcess()
		c.Assert(info, NotNil)
		p, ok := info.Plan.(core.Plan)
		c.Assert(ok, IsTrue)
		normalized, _ := core.NormalizePlan(p)
		normalizedPlan, err := plancodec.DecodeNormalizedPlan(normalized)
		normalizedPlanRows := getPlanRows(normalizedPlan)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = normalizedPlanRows
		})
		compareStringSlice(c, normalizedPlanRows, output[i].Plan)
	}
}

func (s *testPlanNormalize) TestNormalizedPlanForDiffStore(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int, c int, primary key(a))")
	tk.MustExec("insert into t1 values(1,1,1), (2,2,2), (3,3,3)")

	tbl, err := s.dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t1", L: "t1"})
	c.Assert(err, IsNil)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	var input []string
	var output []struct {
		Digest string
		Plan   []string
	}
	s.testData.GetTestCases(c, &input, &output)
	lastDigest := ""
	for i, tt := range input {
		tk.Se.GetSessionVars().PlanID = 0
		tk.MustExec(tt)
		info := tk.Se.ShowProcess()
		c.Assert(info, NotNil)
		ep, ok := info.Plan.(*core.Explain)
		c.Assert(ok, IsTrue)
		normalized, digest := core.NormalizePlan(ep.TargetPlan)
		normalizedPlan, err := plancodec.DecodeNormalizedPlan(normalized)
		normalizedPlanRows := getPlanRows(normalizedPlan)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].Digest = digest
			output[i].Plan = normalizedPlanRows
		})
		compareStringSlice(c, normalizedPlanRows, output[i].Plan)
		c.Assert(digest != lastDigest, IsTrue)
		lastDigest = digest
	}
}

func (s *testPlanNormalize) TestEncodeDecodePlan(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("create table t1 (a int key,b int,c int, index (b));")
	tk.MustExec("set tidb_enable_collect_execution_info=1;")

	tk.Se.GetSessionVars().PlanID = 0
	getPlanTree := func() string {
		info := tk.Se.ShowProcess()
		c.Assert(info, NotNil)
		p, ok := info.Plan.(core.Plan)
		c.Assert(ok, IsTrue)
		encodeStr := core.EncodePlan(p)
		planTree, err := plancodec.DecodePlan(encodeStr)
		c.Assert(err, IsNil)
		return planTree
	}
	tk.MustExec("select max(a) from t1 where a>0;")
	planTree := getPlanTree()
	c.Assert(strings.Contains(planTree, "time"), IsTrue)
	c.Assert(strings.Contains(planTree, "loops"), IsTrue)

	tk.MustExec("insert into t1 values (1,1,1);")
	planTree = getPlanTree()
	c.Assert(strings.Contains(planTree, "Insert"), IsTrue)
	c.Assert(strings.Contains(planTree, "time"), IsTrue)
	c.Assert(strings.Contains(planTree, "loops"), IsTrue)
}

func (s *testPlanNormalize) TestNormalizedDigest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2,t3,t4, bmsql_order_line, bmsql_district,bmsql_stock")
	tk.MustExec("create table t1 (a int key,b int,c int, index (b));")
	tk.MustExec("create table t2 (a int key,b int,c int, index (b));")
	tk.MustExec("create table t3 (a int, b int, index(a)) partition by range(a) (partition p0 values less than (10),partition p1 values less than MAXVALUE);")
	tk.MustExec("create table t4 (a int key,b int) partition by hash(a) partitions 2;")
	tk.MustExec(`CREATE TABLE  bmsql_order_line  (
	   ol_w_id  int(11) NOT NULL,
	   ol_d_id  int(11) NOT NULL,
	   ol_o_id  int(11) NOT NULL,
	   ol_number  int(11) NOT NULL,
	   ol_i_id  int(11) NOT NULL,
	   ol_delivery_d  timestamp NULL DEFAULT NULL,
	   ol_amount  decimal(6,2) DEFAULT NULL,
	   ol_supply_w_id  int(11) DEFAULT NULL,
	   ol_quantity  int(11) DEFAULT NULL,
	   ol_dist_info  char(24) DEFAULT NULL,
	  PRIMARY KEY ( ol_w_id , ol_d_id , ol_o_id , ol_number )
	);`)
	tk.MustExec(`CREATE TABLE  bmsql_district  (
	   d_w_id  int(11) NOT NULL,
	   d_id  int(11) NOT NULL,
	   d_ytd  decimal(12,2) DEFAULT NULL,
	   d_tax  decimal(4,4) DEFAULT NULL,
	   d_next_o_id  int(11) DEFAULT NULL,
	   d_name  varchar(10) DEFAULT NULL,
	   d_street_1  varchar(20) DEFAULT NULL,
	   d_street_2  varchar(20) DEFAULT NULL,
	   d_city  varchar(20) DEFAULT NULL,
	   d_state  char(2) DEFAULT NULL,
	   d_zip  char(9) DEFAULT NULL,
	  PRIMARY KEY ( d_w_id , d_id )
	);`)
	tk.MustExec(`CREATE TABLE  bmsql_stock  (
	   s_w_id  int(11) NOT NULL,
	   s_i_id  int(11) NOT NULL,
	   s_quantity  int(11) DEFAULT NULL,
	   s_ytd  int(11) DEFAULT NULL,
	   s_order_cnt  int(11) DEFAULT NULL,
	   s_remote_cnt  int(11) DEFAULT NULL,
	   s_data  varchar(50) DEFAULT NULL,
	   s_dist_01  char(24) DEFAULT NULL,
	   s_dist_02  char(24) DEFAULT NULL,
	   s_dist_03  char(24) DEFAULT NULL,
	   s_dist_04  char(24) DEFAULT NULL,
	   s_dist_05  char(24) DEFAULT NULL,
	   s_dist_06  char(24) DEFAULT NULL,
	   s_dist_07  char(24) DEFAULT NULL,
	   s_dist_08  char(24) DEFAULT NULL,
	   s_dist_09  char(24) DEFAULT NULL,
	   s_dist_10  char(24) DEFAULT NULL,
	  PRIMARY KEY ( s_w_id , s_i_id )
	);`)
	normalizedDigestCases := []struct {
		sql1   string
		sql2   string
		isSame bool
	}{
		{
			sql1:   "select * from t1;",
			sql2:   "select * from t2;",
			isSame: false,
		},
		{ // test for tableReader and tableScan.
			sql1:   "select * from t1 where a<1",
			sql2:   "select * from t1 where a<2",
			isSame: true,
		},
		{
			sql1:   "select * from t1 where a<1",
			sql2:   "select * from t1 where a=2",
			isSame: false,
		},
		{ // test for point get.
			sql1:   "select * from t1 where a=3",
			sql2:   "select * from t1 where a=2",
			isSame: true,
		},
		{ // test for indexLookUp.
			sql1:   "select * from t1 use index(b) where b=3",
			sql2:   "select * from t1 use index(b) where b=1",
			isSame: true,
		},
		{ // test for indexReader.
			sql1:   "select a+1,b+2 from t1 use index(b) where b=3",
			sql2:   "select a+2,b+3 from t1 use index(b) where b=2",
			isSame: true,
		},
		{ // test for merge join.
			sql1:   "SELECT /*+ TIDB_SMJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_SMJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>2;",
			isSame: true,
		},
		{ // test for indexLookUpJoin.
			sql1:   "SELECT /*+ TIDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: true,
		},
		{ // test for hashJoin.
			sql1:   "SELECT /*+ TIDB_HJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_HJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: true,
		},
		{ // test for diff join.
			sql1:   "SELECT /*+ TIDB_HJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: false,
		},
		{ // test for diff join.
			sql1:   "SELECT /*+ TIDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_SMJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: false,
		},
		{ // test for apply.
			sql1:   "select * from t1 where t1.b > 0 and  t1.a in (select sum(t2.b) from t2 where t2.a=t1.a and t2.b is not null and t2.c >1)",
			sql2:   "select * from t1 where t1.b > 1 and  t1.a in (select sum(t2.b) from t2 where t2.a=t1.a and t2.b is not null and t2.c >0)",
			isSame: true,
		},
		{ // test for apply.
			sql1:   "select * from t1 where t1.b > 0 and  t1.a in (select sum(t2.b) from t2 where t2.a=t1.a and t2.b is not null and t2.c >1)",
			sql2:   "select * from t1 where t1.b > 1 and  t1.a in (select sum(t2.b) from t2 where t2.a=t1.a and t2.b is not null)",
			isSame: false,
		},
		{ // test for topN.
			sql1:   "SELECT * from t1 where a!=1 order by c limit 1",
			sql2:   "SELECT * from t1 where a!=2 order by c limit 2",
			isSame: true,
		},
		{ // test for union
			sql1:   "select count(1) as num,a from t1 where a=1 group by a union select count(1) as num,a from t1 where a=3 group by a;",
			sql2:   "select count(1) as num,a from t1 where a=2 group by a union select count(1) as num,a from t1 where a=4 group by a;",
			isSame: true,
		},
		{ // test for tablescan partition
			sql1:   "select * from t3 where a=5",
			sql2:   "select * from t3 where a=15",
			isSame: true,
		},
		{ // test for point get partition
			sql1:   "select * from t4 where a=4",
			sql2:   "select * from t4 where a=30",
			isSame: true,
		},
		{
			sql1: `SELECT  COUNT(*) AS low_stock
					FROM
					(
						SELECT  *
						FROM bmsql_stock
						WHERE s_w_id = 1
						AND s_quantity < 2
						AND s_i_id IN ( SELECT /*+ TIDB_INLJ(bmsql_order_line) */ ol_i_id FROM bmsql_district JOIN bmsql_order_line ON ol_w_id = d_w_id AND ol_d_id = d_id AND ol_o_id >= d_next_o_id - 20 AND ol_o_id < d_next_o_id WHERE d_w_id = 1 AND d_id = 2 )
					) AS L;`,
			sql2: `SELECT  COUNT(*) AS low_stock
					FROM
					(
						SELECT  *
						FROM bmsql_stock
						WHERE s_w_id = 5
						AND s_quantity < 6
						AND s_i_id IN ( SELECT /*+ TIDB_INLJ(bmsql_order_line) */ ol_i_id FROM bmsql_district JOIN bmsql_order_line ON ol_w_id = d_w_id AND ol_d_id = d_id AND ol_o_id >= d_next_o_id - 70 AND ol_o_id < d_next_o_id WHERE d_w_id = 5 AND d_id = 6 )
					) AS L;`,
			isSame: true,
		},
	}
	for _, testCase := range normalizedDigestCases {
		testNormalizeDigest(tk, c, testCase.sql1, testCase.sql2, testCase.isSame)
	}
}

func testNormalizeDigest(tk *testkit.TestKit, c *C, sql1, sql2 string, isSame bool) {
	tk.Se.GetSessionVars().PlanID = 0
	tk.MustQuery(sql1)
	info := tk.Se.ShowProcess()
	c.Assert(info, NotNil)
	physicalPlan, ok := info.Plan.(core.PhysicalPlan)
	c.Assert(ok, IsTrue)
	normalized1, digest1 := core.NormalizePlan(physicalPlan)

	tk.Se.GetSessionVars().PlanID = 0
	tk.MustQuery(sql2)
	info = tk.Se.ShowProcess()
	c.Assert(info, NotNil)
	physicalPlan, ok = info.Plan.(core.PhysicalPlan)
	c.Assert(ok, IsTrue)
	normalized2, digest2 := core.NormalizePlan(physicalPlan)
	comment := Commentf("sql1: %v, sql2: %v\n%v !=\n%v\n", sql1, sql2, normalized1, normalized2)
	if isSame {
		c.Assert(normalized1, Equals, normalized2, comment)
		c.Assert(digest1, Equals, digest2, comment)
	} else {
		c.Assert(normalized1 != normalized2, IsTrue, comment)
		c.Assert(digest1 != digest2, IsTrue, comment)
	}
}

func getPlanRows(planStr string) []string {
	planStr = strings.Replace(planStr, "\t", " ", -1)
	return strings.Split(planStr, "\n")
}

func compareStringSlice(c *C, ss1, ss2 []string) {
	c.Assert(len(ss1), Equals, len(ss2))
	for i, s := range ss1 {
		c.Assert(s, Equals, ss2[i])
	}
}

func (s *testPlanNormalize) TestExplainFormatHint(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int not null, c2 int not null, key idx_c2(c2)) partition by range (c2) (partition p0 values less than (10), partition p1 values less than (20))")

	tk.MustQuery("explain format='hint' select /*+ use_index(@`sel_2` `test`.`t2` `idx_c2`), hash_agg(@`sel_2`), use_index(@`sel_1` `test`.`t1` `idx_c2`), hash_agg(@`sel_1`) */ count(1) from t t1 where c2 in (select c2 from t t2 where t2.c2 < 15 and t2.c2 > 12)").Check(testkit.Rows(
		"use_index(@`sel_2` `test`.`t2` `idx_c2`), hash_agg(@`sel_2`), use_index(@`sel_1` `test`.`t1` `idx_c2`), hash_agg(@`sel_1`)"))
}

func (s *testPlanNormalize) TestDecodePlanPerformance(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10) key,b int);")
	tk.MustExec("set @@tidb_slow_log_threshold=200000")

	// generate SQL
	buf := bytes.NewBuffer(make([]byte, 0, 1024*1024*4))
	for i := 0; i < 50000; i++ {
		if i > 0 {
			buf.WriteString(" union ")
		}
		buf.WriteString(fmt.Sprintf("select count(1) as num,a from t where a='%v' group by a", i))
	}
	query := buf.String()
	tk.Se.GetSessionVars().PlanID = 0
	tk.MustExec(query)
	info := tk.Se.ShowProcess()
	c.Assert(info, NotNil)
	p, ok := info.Plan.(core.PhysicalPlan)
	c.Assert(ok, IsTrue)
	// TODO: optimize the encode plan performance when encode plan with runtimeStats
	tk.Se.GetSessionVars().StmtCtx.RuntimeStatsColl = nil
	encodedPlanStr := core.EncodePlan(p)
	start := time.Now()
	_, err := plancodec.DecodePlan(encodedPlanStr)
	c.Assert(err, IsNil)
	c.Assert(time.Since(start).Seconds(), Less, 3.0)
}

func (s *testPlanNormalize) TestEncodePlanPerformance(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists th")
	tk.MustExec("set @@session.tidb_enable_table_partition = 1")
	tk.MustExec("create table th (i int, a int,b int, c int, index (a)) partition by hash (a) partitions 1024;")
	tk.MustExec("set @@tidb_slow_log_threshold=200000")

	query := "select count(*) from th t1 join th t2 join th t3 join th t4 join th t5 join th t6 where t1.i=t2.a and t1.i=t3.i and t3.i=t4.i and t4.i=t5.i and t5.i=t6.i"
	tk.Se.GetSessionVars().PlanID = 0
	tk.MustExec(query)
	info := tk.Se.ShowProcess()
	c.Assert(info, NotNil)
	p, ok := info.Plan.(core.PhysicalPlan)
	c.Assert(ok, IsTrue)
	tk.Se.GetSessionVars().StmtCtx.RuntimeStatsColl = nil
	start := time.Now()
	encodedPlanStr := core.EncodePlan(p)
	c.Assert(time.Since(start).Seconds(), Less, 10.0)
	_, err := plancodec.DecodePlan(encodedPlanStr)
	c.Assert(err, IsNil)
}

func TestBuildFinalModeAggregation(t *testing.T) {
	aggSchemaBuilder := func(sctx sessionctx.Context, aggFuncs []*aggregation.AggFuncDesc) *expression.Schema {
		schema := expression.NewSchema(make([]*expression.Column, 0, len(aggFuncs))...)
		for _, agg := range aggFuncs {
			newCol := &expression.Column{
				UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  agg.RetTp,
			}
			schema.Append(newCol)
		}
		return schema
	}
	isFinalAggMode := func(mode aggregation.AggFunctionMode) bool {
		return mode == aggregation.FinalMode || mode == aggregation.CompleteMode
	}
	checkResult := func(sctx sessionctx.Context, aggFuncs []*aggregation.AggFuncDesc, groubyItems []expression.Expression) {
		for partialIsCop := 0; partialIsCop < 2; partialIsCop++ {
			for isMPPTask := 0; isMPPTask < 2; isMPPTask++ {
				partial, final, _ := core.BuildFinalModeAggregation(sctx, &core.AggInfo{
					AggFuncs:     aggFuncs,
					GroupByItems: groubyItems,
					Schema:       aggSchemaBuilder(sctx, aggFuncs),
				}, partialIsCop == 0, isMPPTask == 0)
				if partial != nil {
					for _, aggFunc := range partial.AggFuncs {
						if partialIsCop == 0 {
							require.True(t, !isFinalAggMode(aggFunc.Mode))
						} else {
							require.True(t, isFinalAggMode(aggFunc.Mode))
						}
					}
				}
				if final != nil {
					for _, aggFunc := range final.AggFuncs {
						require.True(t, isFinalAggMode(aggFunc.Mode))
					}
				}
			}
		}
	}

	ctx := core.MockContext()

	aggCol := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	gbyCol := &expression.Column{
		Index:   1,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	orderCol := &expression.Column{
		Index:   2,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}

	emptyGroupByItems := make([]expression.Expression, 0, 1)
	groupByItems := make([]expression.Expression, 0, 1)
	groupByItems = append(groupByItems, gbyCol)

	orderByItems := make([]*util.ByItems, 0, 1)
	orderByItems = append(orderByItems, &util.ByItems{
		Expr: orderCol,
		Desc: true,
	})

	aggFuncs := make([]*aggregation.AggFuncDesc, 0, 5)
	desc, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncMax, []expression.Expression{aggCol}, false)
	require.NoError(t, err)
	aggFuncs = append(aggFuncs, desc)
	desc, err = aggregation.NewAggFuncDesc(ctx, ast.AggFuncFirstRow, []expression.Expression{aggCol}, false)
	require.NoError(t, err)
	aggFuncs = append(aggFuncs, desc)
	desc, err = aggregation.NewAggFuncDesc(ctx, ast.AggFuncCount, []expression.Expression{aggCol}, false)
	require.NoError(t, err)
	aggFuncs = append(aggFuncs, desc)
	desc, err = aggregation.NewAggFuncDesc(ctx, ast.AggFuncSum, []expression.Expression{aggCol}, false)
	require.NoError(t, err)
	aggFuncs = append(aggFuncs, desc)
	desc, err = aggregation.NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{aggCol}, false)
	require.NoError(t, err)
	aggFuncs = append(aggFuncs, desc)

	aggFuncsWithDistinct := make([]*aggregation.AggFuncDesc, 0, 2)
	desc, err = aggregation.NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{aggCol}, true)
	require.NoError(t, err)
	aggFuncsWithDistinct = append(aggFuncsWithDistinct, desc)
	desc, err = aggregation.NewAggFuncDesc(ctx, ast.AggFuncCount, []expression.Expression{aggCol}, true)
	require.NoError(t, err)
	aggFuncsWithDistinct = append(aggFuncsWithDistinct, desc)

	groupConcatAggFuncs := make([]*aggregation.AggFuncDesc, 0, 4)
	groupConcatWithoutDistinctWithoutOrderBy, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncGroupConcat, []expression.Expression{aggCol, aggCol}, false)
	require.NoError(t, err)
	groupConcatAggFuncs = append(groupConcatAggFuncs, groupConcatWithoutDistinctWithoutOrderBy)
	groupConcatWithoutDistinctWithOrderBy, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncGroupConcat, []expression.Expression{aggCol, aggCol}, false)
	require.NoError(t, err)
	groupConcatWithoutDistinctWithOrderBy.OrderByItems = orderByItems
	groupConcatAggFuncs = append(groupConcatAggFuncs, groupConcatWithoutDistinctWithOrderBy)
	groupConcatWithDistinctWithoutOrderBy, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncGroupConcat, []expression.Expression{aggCol, aggCol}, true)
	require.NoError(t, err)
	groupConcatAggFuncs = append(groupConcatAggFuncs, groupConcatWithDistinctWithoutOrderBy)
	groupConcatWithDistinctWithOrderBy, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncGroupConcat, []expression.Expression{aggCol, aggCol}, true)
	require.NoError(t, err)
	groupConcatWithDistinctWithOrderBy.OrderByItems = orderByItems
	groupConcatAggFuncs = append(groupConcatAggFuncs, groupConcatWithDistinctWithOrderBy)

	// case 1 agg without distinct
	checkResult(ctx, aggFuncs, emptyGroupByItems)
	checkResult(ctx, aggFuncs, groupByItems)

	// case 2 agg with distinct
	checkResult(ctx, aggFuncsWithDistinct, emptyGroupByItems)
	checkResult(ctx, aggFuncsWithDistinct, groupByItems)

	// case 3 mixed with distinct and without distinct
	mixedAggFuncs := make([]*aggregation.AggFuncDesc, 0, 10)
	mixedAggFuncs = append(mixedAggFuncs, aggFuncs...)
	mixedAggFuncs = append(mixedAggFuncs, aggFuncsWithDistinct...)
	checkResult(ctx, mixedAggFuncs, emptyGroupByItems)
	checkResult(ctx, mixedAggFuncs, groupByItems)

	// case 4 group concat
	for _, groupConcatAggFunc := range groupConcatAggFuncs {
		checkResult(ctx, []*aggregation.AggFuncDesc{groupConcatAggFunc}, emptyGroupByItems)
		checkResult(ctx, []*aggregation.AggFuncDesc{groupConcatAggFunc}, groupByItems)
	}
	checkResult(ctx, groupConcatAggFuncs, emptyGroupByItems)
	checkResult(ctx, groupConcatAggFuncs, groupByItems)

	// case 5 mixed group concat and other agg funcs
	for _, groupConcatAggFunc := range groupConcatAggFuncs {
		funcs := make([]*aggregation.AggFuncDesc, 0, 10)
		funcs = append(funcs, groupConcatAggFunc)
		funcs = append(funcs, aggFuncs...)
		checkResult(ctx, funcs, emptyGroupByItems)
		checkResult(ctx, funcs, groupByItems)
		funcs = append(funcs, aggFuncsWithDistinct...)
		checkResult(ctx, funcs, emptyGroupByItems)
		checkResult(ctx, funcs, groupByItems)
	}
	mixedAggFuncs = append(mixedAggFuncs, groupConcatAggFuncs...)
	checkResult(ctx, mixedAggFuncs, emptyGroupByItems)
	checkResult(ctx, mixedAggFuncs, groupByItems)
}
