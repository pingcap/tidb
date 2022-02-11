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
	"strings"

<<<<<<< HEAD
	. "github.com/pingcap/check"
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
	"github.com/pingcap/tidb/util/israce"
>>>>>>> f949e01e0... planner, expression: pushdown AggFuncMode to coprocessor (#31392)
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
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("create table t1 (a int key,b int,c int, index (b));")
	tk.MustExec("create table t2 (a int key,b int,c int, index (b));")
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

func (s *testPlanNormalize) TestNormalizedDigest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("create table t1 (a int key,b int,c int, index (b));")
	tk.MustExec("create table t2 (a int key,b int,c int, index (b));")
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
			sql2:   "select a+2,b+3 from t1 use index(b) where b>2",
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
	c.Assert(len(ss1), Equals, len(ss2), Commentf("%v != \n%v\n", ss1, ss2))
	for i, s := range ss1 {
		c.Assert(s, Equals, ss2[i])
	}
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
