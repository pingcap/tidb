// Copyright 2025 PingCAP, Inc.
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

package logicalop

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestLogicalSchemaClone(t *testing.T) {
	ctx := mock.NewContext()
	sp := &logicalop.LogicalSchemaProducer{}
	col1 := &expression.Column{
		ID: 1,
	}
	schema := expression.NewSchema()
	// alloc cap.
	schema.Columns = make([]*expression.Column, 0, 10)
	sp.SetSchema(schema)
	sp.Schema().Append(col1)
	name := &types.FieldName{ColName: ast.NewCIStr("a")}
	names := types.NameSlice{name}
	sp.SetOutputNames(names)
	sp.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, "test", nil, 0)
	child1 := logicalop.NewBaseLogicalPlan(ctx, "child1", nil, 0)
	sp.BaseLogicalPlan.SetChildren(child1.GetBaseLogicalPlan())

	cloneSp := *sp
	require.NotNil(t, cloneSp.Schema())
	require.True(t, sp.Schema().Len() > 0)
	require.True(t, cloneSp.Schema().Len() > 0)
	// *schema is shared
	require.True(t, cloneSp.Schema() == sp.Schema())
	// *Name slice is shallow.
	require.True(t, len(cloneSp.OutputNames()) > 0)
	require.True(t, len(sp.OutputNames()) > 0)
	// BaseLogicalPlan struct is a new one.
	require.False(t, &sp.BaseLogicalPlan == &cloneSp.BaseLogicalPlan)
	// children slice inside BaseLogicalPlan is shared.
	require.True(t, len(sp.Children()) == 1)
	require.True(t, len(cloneSp.Children()) == 1)
	require.True(t, sp.Children()[0] == cloneSp.Children()[0])
	// test clonedSp schema append, should affect sp's schema
	col2 := &expression.Column{
		ID: 2,
	}
	cloneSp.Schema().Append(col2)
	// the column slice inside schema will grow at both case.
	require.Equal(t, cloneSp.Schema().Len(), 2)
	require.Equal(t, sp.Schema().Len(), 2)
}

func TestLogicalApplyClone(t *testing.T) {
	ctx := mock.NewContext()
	sp := logicalop.LogicalSchemaProducer{}
	col1 := &expression.Column{
		ID: 1,
	}
	sp.SetSchema(expression.NewSchema(col1))
	name := &types.FieldName{ColName: ast.NewCIStr("a")}
	names := types.NameSlice{name}
	sp.SetOutputNames(names)
	sp.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, "test", nil, 0)
	child1 := logicalop.NewBaseLogicalPlan(ctx, "child1", nil, 0)
	sp.BaseLogicalPlan.SetChildren(child1.GetBaseLogicalPlan())

	apply := &logicalop.LogicalApply{
		LogicalJoin: logicalop.LogicalJoin{
			LogicalSchemaProducer: sp,
			EqualConditions:       []*expression.ScalarFunction{},
		},
	}
	apply.EqualConditions = make([]*expression.ScalarFunction, 0, 17)
	apply.EqualConditions = append(apply.EqualConditions, &expression.ScalarFunction{FuncName: ast.NewCIStr("f1")})
	apply.EqualConditions = append(apply.EqualConditions, &expression.ScalarFunction{FuncName: ast.NewCIStr("f2")})
	clonedApply := *apply
	// require.True(t, &apply.EqualConditions == &clonedApply.EqualConditions)
	clonedApply.EqualConditions = append(clonedApply.EqualConditions, &expression.ScalarFunction{FuncName: ast.NewCIStr("f3")})
	require.True(t, len(apply.LogicalJoin.EqualConditions) == 2)
	require.True(t, len(clonedApply.LogicalJoin.EqualConditions) == 3)

	tmp := clonedApply.EqualConditions[0]
	clonedApply.EqualConditions[0] = clonedApply.EqualConditions[1]
	clonedApply.EqualConditions[1] = tmp
	require.True(t, clonedApply.EqualConditions[0].FuncName.L == "f2")
	require.True(t, apply.EqualConditions[0].FuncName.L == "f2")
}

func TestFrameBoundCloneDeepCopiesCompareCols(t *testing.T) {
	col := &expression.Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	original := &logicalop.FrameBound{
		CalcFuncs:   []expression.Expression{col},
		CompareCols: []expression.Expression{col},
	}
	cloned := original.Clone()

	require.Len(t, cloned.CompareCols, 1)
	require.NotSame(t, col, cloned.CompareCols[0])

	replaced := &expression.Column{
		UniqueID: 2,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	cloned.CompareCols[0] = replaced
	require.Same(t, col, original.CompareCols[0])
	require.Same(t, col, original.CalcFuncs[0])
}

func TestReplaceColumnOfExprCopyOnWrite(t *testing.T) {
	ctx := mock.NewContext()
	srcCol := &expression.Column{
		UniqueID: 1,
		Index:    0,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	dstCol := &expression.Column{
		UniqueID: 2,
		Index:    0,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	expr, err := expression.NewFunction(ctx.GetExprCtx(), ast.Plus, types.NewFieldType(mysql.TypeLonglong), srcCol, srcCol)
	require.NoError(t, err)
	original := expr.(*expression.ScalarFunction)

	replaced := ruleutil.ReplaceColumnOfExpr(expr, []expression.Expression{dstCol}, expression.NewSchema(srcCol)).(*expression.ScalarFunction)
	require.NotSame(t, original, replaced)
	require.Same(t, srcCol, original.GetArgs()[0])
	require.Same(t, srcCol, original.GetArgs()[1])
	require.Same(t, dstCol, replaced.GetArgs()[0])
	require.Same(t, dstCol, replaced.GetArgs()[1])
}

func TestResolveExprAndReplaceCopyOnWrite(t *testing.T) {
	ctx := mock.NewContext()
	srcCol := &expression.Column{
		UniqueID: 1,
		Index:    0,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	dstCol := &expression.Column{
		UniqueID: 2,
		Index:    0,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	expr, err := expression.NewFunction(ctx.GetExprCtx(), ast.Plus, types.NewFieldType(mysql.TypeLonglong), srcCol, srcCol)
	require.NoError(t, err)
	original := expr.(*expression.ScalarFunction)

	replaced := ruleutil.ResolveExprAndReplace(expr, map[string]*expression.Column{
		string(srcCol.HashCode()): dstCol,
	}).(*expression.ScalarFunction)
	require.NotSame(t, original, replaced)
	require.Same(t, srcCol, original.GetArgs()[0])
	require.Same(t, srcCol, original.GetArgs()[1])
	require.NotSame(t, dstCol, replaced.GetArgs()[0])
	require.NotSame(t, dstCol, replaced.GetArgs()[1])

	replacedLeft := replaced.GetArgs()[0].(*expression.Column)
	replacedRight := replaced.GetArgs()[1].(*expression.Column)
	require.Equal(t, dstCol.UniqueID, replacedLeft.UniqueID)
	require.Equal(t, dstCol.UniqueID, replacedRight.UniqueID)
}

func TestLogicalCTEPreparePossiblePropertiesSkipNilChild(t *testing.T) {
	ctx := mock.NewContext()
	cte := logicalop.LogicalCTE{}.Init(ctx, 0)
	props := cte.PreparePossibleProperties(nil, nil, &base.PossiblePropertiesInfo{HasTiFlash: true})
	require.NotNil(t, props)
	require.True(t, props.HasTiFlash)
}

func TestLogicalProjectionPushDownTopN(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec(`CREATE TABLE table_test (
col16 json DEFAULT NULL,
col17 json DEFAULT NULL
);`)
	sql := `explain format='plan_tree' SELECT
       s.column16 AS column16,
       s.column17 AS column17
FROM
  (SELECT
          col16 -> '$[].optUid' AS column16,
          JSON_UNQUOTE(JSON_EXTRACT(col17, '$[0].value')) AS column17
   FROM
     (SELECT
             col16,
             col17
      FROM table_test) ta24e
   ) AS s
ORDER BY CONVERT(column16 USING GBK) ASC,column17 ASC
LIMIT 0,
      20;`
	tk.MustQuery(sql).Check(testkit.Rows(
		"Projection root  Column, Column",
		"└─TopN root  Column, Column, offset:0, count:20",
		"  └─Projection root  Column, Column, convert(cast(Column, var_string(16777216)), gbk)->Column",
		"    └─TableReader root  data:Projection",
		"      └─Projection cop[tikv]  json_extract(test.table_test.col16, $[].optUid)->Column, json_unquote(cast(json_extract(test.table_test.col17, $[0].value), var_string(16777216)))->Column",
		"        └─TableFullScan cop[tikv] table:table_test keep order:false, stats:pseudo"))
	tk.MustExec(`INSERT INTO mysql.opt_rule_blacklist VALUES("topn_push_down");`)
	tk.MustExec(`admin reload opt_rule_blacklist;`)
	tk.MustQuery(sql).Check(testkit.Rows(
		"Limit root  offset:0, count:20",
		"└─Projection root  Column, Column",
		"  └─Sort root  Column, Column",
		"    └─Projection root  Column, Column, convert(cast(Column, var_string(16777216)), gbk)->Column",
		"      └─TableReader root  data:Projection",
		"        └─Projection cop[tikv]  json_extract(test.table_test.col16, $[].optUid)->Column, json_unquote(cast(json_extract(test.table_test.col17, $[0].value), var_string(16777216)))->Column",
		"          └─TableFullScan cop[tikv] table:table_test keep order:false, stats:pseudo"))
}

func TestLogicalTopNPruneColumnsRefreshesSchemaBeforeInlineProjection(t *testing.T) {
	ctx := mock.NewContext()
	newColumn := func(id int64) *expression.Column {
		return &expression.Column{
			UniqueID: id,
			RetType:  types.NewFieldType(mysql.TypeLonglong),
		}
	}

	out1 := newColumn(1)
	out2 := newColumn(2)
	sortCol := newColumn(3)

	child := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
	child.SetSchema(expression.NewSchema(out1.Clone().(*expression.Column), out2.Clone().(*expression.Column), sortCol.Clone().(*expression.Column)))

	topN := logicalop.LogicalTopN{
		ByItems: []*util.ByItems{{Expr: sortCol.Clone(), Desc: false}},
		Count:   1,
	}.Init(ctx, 0)
	topN.SetChildren(child)
	topN.SetSchema(expression.NewSchema(
		out1.Clone().(*expression.Column),
		out2.Clone().(*expression.Column),
		sortCol.Clone().(*expression.Column),
		sortCol.Clone().(*expression.Column),
	))

	_, err := topN.PruneColumns([]*expression.Column{
		out1.Clone().(*expression.Column),
		out2.Clone().(*expression.Column),
		sortCol.Clone().(*expression.Column),
	})
	require.NoError(t, err)
	require.Equal(t, 3, topN.Schema().Len())
	require.Equal(t, int64(1), topN.Schema().Columns[0].UniqueID)
	require.Equal(t, int64(2), topN.Schema().Columns[1].UniqueID)
	require.Equal(t, int64(3), topN.Schema().Columns[2].UniqueID)
}

func TestLogicalExpandBuildKeyInfo(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test;")
		tk.MustExec("CREATE TABLE `testorg` (\n  `org_id` decimal(19,0) NOT NULL,\n  `org_code` varchar(100) DEFAULT NULL,\n  `org_name` varchar(100) DEFAULT NULL,\n  `org_type` varchar(100) DEFAULT NULL,\n  PRIMARY KEY (`org_id`) /*T![clustered_index] CLUSTERED */\n) ")
		tk.MustExec("CREATE TABLE `testpay` (\n  `bill_code` varchar(100) NOT NULL,\n  `org_id` decimal(19,0) DEFAULT NULL,\n  `amt` decimal(15,2) DEFAULT NULL,\n  `pay_date` varchar(10) DEFAULT NULL,\n  PRIMARY KEY (`bill_code`) /*T![clustered_index] CLUSTERED */\n)")
		tk.MustExec("CREATE TABLE `testreturn` (\n  `bill_code` varchar(100) NOT NULL,\n  `org_id` decimal(19,0) DEFAULT NULL,\n  `amt` decimal(15,2) DEFAULT NULL,\n  `ret_date` varchar(10) DEFAULT NULL,\n  PRIMARY KEY (`bill_code`) /*T![clustered_index] CLUSTERED */\n)")
		tk.MustExec("insert into testorg (org_id,org_code,org_name,org_type) values(1,'ORG0001','部门1','DEPT');" +
			"insert into testorg (org_id,org_code,org_name,org_type) values(2,'ORG0002','部门2','DEPT');" +
			"insert into testorg (org_id,org_code,org_name,org_type) values(3,'ORG0003','部门3','DEPT');" +
			"insert into testorg (org_id,org_code,org_name,org_type) values(4,'ORG0004','部门4','DEPT');" +
			"insert into testorg (org_id,org_code,org_name,org_type) values(5,'ORG0005','公司1','ORG');" +
			"insert into testorg (org_id,org_code,org_name,org_type) values(6,'ORG0006','公司2','ORG');" +
			"insert into testorg (org_id,org_code,org_name,org_type) values(7,'ORG0007','公司3','ORG');")
		tk.MustExec("insert into testpay (bill_code,org_id,amt,pay_date) values('PAY0001',1,100,'2024-06-01');" +
			"insert into testpay (bill_code,org_id,amt,pay_date) values('PAY0002',2,200,'2024-06-02');" +
			"insert into testpay (bill_code,org_id,amt,pay_date) values('PAY0003',3,300,'2024-06-03');" +
			"insert into testpay (bill_code,org_id,amt,pay_date) values('PAY0004',4,400,'2024-07-01');" +
			"insert into testpay (bill_code,org_id,amt,pay_date) values('PAY0005',5,500,'2024-07-02');" +
			"insert into testpay (bill_code,org_id,amt,pay_date) values('PAY0006',6,600,'2024-07-03');")
		tk.MustExec("insert into testreturn (bill_code,org_id,amt,ret_date) values('RET0001',1,100,'2024-06-01');" +
			"insert into testreturn (bill_code,org_id,amt,ret_date) values('RET0002',2,200,'2024-06-02');" +
			"insert into testreturn (bill_code,org_id,amt,ret_date) values('RET0003',3,300,'2024-06-03');" +
			"insert into testreturn (bill_code,org_id,amt,ret_date) values('RET0004',4,400,'2024-07-01'); ")
		res := tk.MustQuery("SELECT\n  SUM(IFNULL(pay.payamt, 0)) AS payamt," +
			"  SUM(IFNULL(ret.retamt, 0)) AS retamt," +
			"  org.org_type," +
			"  org.org_id," +
			"  org.org_name" +
			" FROM testorg org" +
			" LEFT JOIN (" +
			"  SELECT" +
			"    SUM(IFNULL(amt, 0)) AS payamt," +
			"    org_id" +
			"  FROM testpay tp" +
			"  WHERE tp.pay_date BETWEEN '2024-06-01' AND '2024-07-31'" +
			"  GROUP BY org_id" +
			") pay ON pay.org_id = org.org_id" +
			" LEFT JOIN (" +
			"  SELECT" +
			"    SUM(IFNULL(amt, 0)) AS retamt," +
			"    org_id" +
			"  FROM testreturn tr" +
			"  WHERE tr.ret_date BETWEEN '2024-06-01' AND '2024-07-31'" +
			"  GROUP BY org_id" +
			") ret ON ret.org_id = org.org_id" +
			" GROUP BY org.org_type, org.org_id WITH ROLLUP;")
		require.Equal(t, len(res.Rows()), 10)
		res = tk.MustQuery("SELECT * FROM (   SELECT     SUM(IFNULL(pay.payamt, 0)) AS payamt,     SUM(IFNULL(ret.retamt, 0)) AS retamt,     GROUPING(org.org_type) AS grouptype,     org.org_type,     GROUPING(org.org_id) AS groupid,     org.org_id,     org.org_name   FROM testorg org   LEFT JOIN (     SELECT       SUM(IFNULL(amt, 0)) AS payamt,       org_id     FROM testpay tp     WHERE tp.pay_date BETWEEN '2024-06-01' AND '2024-07-31'     GROUP BY org_id   ) pay ON pay.org_id = org.org_id   LEFT JOIN (     SELECT       SUM(IFNULL(amt, 0)) AS retamt,       org_id     FROM testreturn tr     WHERE tr.ret_date BETWEEN '2024-06-01' AND '2024-07-31'     GROUP BY org_id   ) ret ON ret.org_id = org.org_id   GROUP BY org.org_type, org.org_id WITH ROLLUP ) t WHERE groupid = 1 AND grouptype = 1;\n")
		require.Equal(t, len(res.Rows()), 1)
		res = tk.MustQuery("SELECT SUM(IFNULL(pay.payamt, 0)) AS payamt,     SUM(IFNULL(ret.retamt, 0)) AS retamt,     GROUPING(org.org_type) AS grouptype,     org.org_type,     GROUPING(org.org_id) AS groupid,     org.org_id,     org.org_name   FROM testorg org   LEFT JOIN (     SELECT       SUM(IFNULL(amt, 0)) AS payamt,       org_id     FROM testpay tp     WHERE tp.pay_date BETWEEN '2024-06-01' AND '2024-07-31'     GROUP BY org_id   ) pay ON pay.org_id = org.org_id   LEFT JOIN (     SELECT       SUM(IFNULL(amt, 0)) AS retamt,       org_id     FROM testreturn tr     WHERE tr.ret_date BETWEEN '2024-06-01' AND '2024-07-31'     GROUP BY org_id   ) ret ON ret.org_id = org.org_id   GROUP BY org.org_type, org.org_id WITH ROLLUP having  groupid = 1 AND grouptype = 1;")
		require.Equal(t, len(res.Rows()), 1)

		// since the plan may differ under different planner mode, recommend to record explain result to json accordingly.
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		cascadesData := GetCascadesSuiteData()
		cascadesData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format='plan_tree' " + tt).Rows())
			})
			res := tk.MustQuery("explain format='plan_tree' " + tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}
