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
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
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

func TestLogicalPlanDeepClone(t *testing.T) {
	ctx := mock.NewContext()
	left := logicalop.LogicalTableDual{}.Init(ctx, 0)
	left.SetSchema(expression.NewSchema(&expression.Column{ID: 1}))
	right := logicalop.LogicalTableDual{}.Init(ctx, 0)
	right.SetSchema(expression.NewSchema(&expression.Column{ID: 2}))

	join := logicalop.LogicalJoin{}.Init(ctx, 0)
	join.SetSchema(expression.NewSchema(&expression.Column{ID: 3}))
	join.LeftConditions = expression.CNFExprs{&expression.Column{
		ID:      6,
		RetType: types.NewFieldType(mysql.TypeLonglong),
		VirtualExpr: &expression.Constant{
			Value:   types.NewBytesDatum([]byte("abc")),
			RetType: types.NewFieldType(mysql.TypeString),
		},
	}}
	join.DefaultValues = []types.Datum{types.NewStringDatum("origin")}
	join.SetChildren(left, right)

	cloned := join.DeepClone().(*logicalop.LogicalJoin)
	require.NotSame(t, join, cloned)
	require.NotSame(t, join.Schema(), cloned.Schema())
	require.NotSame(t, join.Children()[0], cloned.Children()[0])
	require.NotSame(t, join.LeftConditions[0], cloned.LeftConditions[0])
	require.NotSame(t, join.LeftConditions[0].(*expression.Column).RetType, cloned.LeftConditions[0].(*expression.Column).RetType)
	require.NotSame(t, join.LeftConditions[0].(*expression.Column).VirtualExpr, cloned.LeftConditions[0].(*expression.Column).VirtualExpr)
	require.Same(t, cloned, cloned.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan).Self())

	cloned.LeftConditions[0].(*expression.Column).ID = 7
	cloned.LeftConditions[0].(*expression.Column).RetType.SetType(mysql.TypeDouble)
	cloned.LeftConditions[0].(*expression.Column).VirtualExpr.(*expression.Constant).Value.GetBytes()[0] = 'z'
	cloned.Schema().Append(&expression.Column{ID: 4})
	cloned.DefaultValues[0].SetInt64(42)
	cloned.Children()[0].Schema().Append(&expression.Column{ID: 5})

	require.Equal(t, int64(6), join.LeftConditions[0].(*expression.Column).ID)
	require.Equal(t, mysql.TypeLonglong, join.LeftConditions[0].(*expression.Column).RetType.GetType())
	require.Equal(t, byte('a'), join.LeftConditions[0].(*expression.Column).VirtualExpr.(*expression.Constant).Value.GetBytes()[0])
	require.Equal(t, 1, join.Schema().Len())
	require.Equal(t, "origin", join.DefaultValues[0].GetString())
	require.Equal(t, 1, left.Schema().Len())
}

func TestLogicalPlanDeepCloneClearsOptimizerCaches(t *testing.T) {
	ctx := mock.NewContext()
	left := logicalop.LogicalTableDual{}.Init(ctx, 0)
	left.SetSchema(expression.NewSchema(&expression.Column{ID: 1}))
	right := logicalop.LogicalTableDual{}.Init(ctx, 0)
	right.SetSchema(expression.NewSchema(&expression.Column{ID: 2}))

	join := logicalop.LogicalJoin{}.Init(ctx, 0)
	join.SetChildren(left, right)

	basePlan := join.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan)
	basePlan.SetFDs(&fd.FDSet{HashCodeToUniqueID: map[string]int{"expr": 1}})
	basePlan.SetStats(&property.StatsInfo{
		RowCount: 1,
		ColNDVs:  map[int64]float64{1: 1},
	})
	basePlan.SetMaxOneRow(true)
	basePlan.SetPlanIDsHash(123)

	prop := &property.PhysicalProperty{}
	task := &physicalop.RootTask{}
	basePlan.StoreTask(prop, task)
	require.Same(t, task, basePlan.GetTask(prop))

	cloned := join.DeepClone().(*logicalop.LogicalJoin)
	clonedBase := cloned.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan)

	require.Nil(t, cloned.StatsInfo())
	require.Nil(t, clonedBase.FDs())
	require.Nil(t, clonedBase.GetTask(prop))

	clonedTask := &physicalop.RootTask{}
	clonedBase.StoreTask(prop, clonedTask)
	require.Same(t, clonedTask, clonedBase.GetTask(prop))
	require.Same(t, task, basePlan.GetTask(prop))
}

func TestLogicalPlanDeepCloneComplexTree(t *testing.T) {
	ctx := mock.NewContext()

	newMutableColumn := func(id, uniqueID int64, marker byte) *expression.Column {
		return &expression.Column{
			ID:       id,
			UniqueID: uniqueID,
			RetType:  types.NewFieldType(mysql.TypeLonglong),
			VirtualExpr: &expression.Constant{
				Value:   types.NewBytesDatum([]byte{marker, marker + 1}),
				RetType: types.NewFieldType(mysql.TypeString),
			},
		}
	}
	mustScalarFunc := func(expr expression.Expression) *expression.ScalarFunction {
		sf, ok := expr.(*expression.ScalarFunction)
		require.True(t, ok)
		return sf
	}

	leftDSBaseCol := newMutableColumn(1, 101, 'a')
	leftDSAuxCol := newMutableColumn(2, 102, 'c')
	leftDS := logicalop.DataSource{
		PushedDownConds: []expression.Expression{newMutableColumn(11, 111, 'e')},
		AllConds:        []expression.Expression{newMutableColumn(12, 112, 'g')},
		TblCols:         []*expression.Column{leftDSBaseCol, leftDSAuxCol},
		TblColsByID: map[int64]*expression.Column{
			leftDSBaseCol.ID: leftDSBaseCol,
			leftDSAuxCol.ID:  leftDSAuxCol,
		},
		AskedColumnGroup:   [][]*expression.Column{{leftDSBaseCol, leftDSAuxCol}},
		InterestingColumns: []*expression.Column{leftDSBaseCol},
	}.Init(ctx, 0)
	leftDS.SetSchema(expression.NewSchema(
		leftDSBaseCol.Clone().(*expression.Column),
		leftDSAuxCol.Clone().(*expression.Column),
	))

	leftSelCond := mustScalarFunc(expression.NewFunctionInternal(ctx, ast.GT, types.NewFieldType(mysql.TypeTiny),
		leftDS.Schema().Columns[0].Clone().(*expression.Column),
		&expression.Constant{Value: types.NewIntDatum(10), RetType: types.NewFieldType(mysql.TypeLonglong)},
	))
	leftSel := logicalop.LogicalSelection{
		Conditions: []expression.Expression{leftSelCond},
	}.Init(ctx, 0)
	leftSel.SetChildren(leftDS)

	leftProj := logicalop.LogicalProjection{
		Exprs: []expression.Expression{
			leftDS.Schema().Columns[0].Clone().(*expression.Column),
			leftDS.Schema().Columns[1].Clone().(*expression.Column),
		},
	}.Init(ctx, 0)
	leftProj.SetSchema(expression.NewSchema(
		newMutableColumn(21, 121, 'i'),
		newMutableColumn(22, 122, 'k'),
	))
	leftProj.SetChildren(leftSel)

	aggDesc, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncCount, []expression.Expression{
		leftProj.Exprs[0].Clone(),
	}, false)
	require.NoError(t, err)
	aggDesc.OrderByItems = []*plannerutil.ByItems{
		{Expr: leftProj.Exprs[1].Clone(), Desc: true},
	}
	leftAgg := logicalop.LogicalAggregation{
		AggFuncs:           []*aggregation.AggFuncDesc{aggDesc},
		GroupByItems:       []expression.Expression{leftProj.Exprs[1].Clone()},
		PossibleProperties: [][]*expression.Column{{leftProj.Schema().Columns[0].Clone().(*expression.Column)}},
	}.Init(ctx, 0)
	leftAgg.SetSchema(expression.NewSchema(newMutableColumn(31, 131, 'm')))
	leftAgg.SetChildren(leftProj)

	leftSort := logicalop.LogicalSort{
		ByItems: []*plannerutil.ByItems{
			{Expr: leftAgg.Schema().Columns[0].Clone().(*expression.Column), Desc: true},
		},
	}.Init(ctx, 0)
	leftSort.SetChildren(leftAgg)

	leftLimit := logicalop.LogicalLimit{
		Offset: 1,
		Count:  9,
		PartitionBy: []property.SortItem{
			{Col: leftAgg.Schema().Columns[0].Clone().(*expression.Column), Desc: true},
		},
	}.Init(ctx, 0)
	leftLimit.SetSchema(leftAgg.Schema().Clone())
	leftLimit.SetChildren(leftSort)

	rightDSBaseCol := newMutableColumn(41, 141, 'o')
	rightDSAuxCol := newMutableColumn(42, 142, 'q')
	rightDS := logicalop.DataSource{
		PushedDownConds: []expression.Expression{newMutableColumn(51, 151, 's')},
		AllConds:        []expression.Expression{newMutableColumn(52, 152, 'u')},
		TblCols:         []*expression.Column{rightDSBaseCol, rightDSAuxCol},
		TblColsByID: map[int64]*expression.Column{
			rightDSBaseCol.ID: rightDSBaseCol,
			rightDSAuxCol.ID:  rightDSAuxCol,
		},
		AskedColumnGroup:   [][]*expression.Column{{rightDSBaseCol}},
		InterestingColumns: []*expression.Column{rightDSBaseCol},
	}.Init(ctx, 0)
	rightDS.SetSchema(expression.NewSchema(
		rightDSBaseCol.Clone().(*expression.Column),
		rightDSAuxCol.Clone().(*expression.Column),
	))

	rightSelCond := mustScalarFunc(expression.NewFunctionInternal(ctx, ast.LE, types.NewFieldType(mysql.TypeTiny),
		rightDS.Schema().Columns[0].Clone().(*expression.Column),
		&expression.Constant{Value: types.NewIntDatum(20), RetType: types.NewFieldType(mysql.TypeLonglong)},
	))
	rightSel := logicalop.LogicalSelection{
		Conditions: []expression.Expression{rightSelCond},
	}.Init(ctx, 0)
	rightSel.SetChildren(rightDS)

	rightProj := logicalop.LogicalProjection{
		Exprs: []expression.Expression{
			rightDS.Schema().Columns[0].Clone().(*expression.Column),
			rightDS.Schema().Columns[1].Clone().(*expression.Column),
		},
	}.Init(ctx, 0)
	rightProj.SetSchema(expression.NewSchema(
		newMutableColumn(61, 161, 'w'),
		newMutableColumn(62, 162, 'y'),
	))
	rightProj.SetChildren(rightSel)

	rightSort := logicalop.LogicalSort{
		ByItems: []*plannerutil.ByItems{
			{Expr: rightProj.Schema().Columns[0].Clone().(*expression.Column), Desc: false},
		},
	}.Init(ctx, 0)
	rightSort.SetChildren(rightProj)

	rightLimit := logicalop.LogicalLimit{
		Offset: 0,
		Count:  20,
		PartitionBy: []property.SortItem{
			{Col: rightProj.Schema().Columns[0].Clone().(*expression.Column), Desc: false},
		},
	}.Init(ctx, 0)
	rightLimit.SetSchema(rightProj.Schema().Clone())
	rightLimit.SetChildren(rightSort)

	joinEq := mustScalarFunc(expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny),
		leftLimit.Schema().Columns[0].Clone().(*expression.Column),
		rightLimit.Schema().Columns[0].Clone().(*expression.Column),
	))
	joinOther := mustScalarFunc(expression.NewFunctionInternal(ctx, ast.GE, types.NewFieldType(mysql.TypeTiny),
		leftLimit.Schema().Columns[0].Clone().(*expression.Column),
		&expression.Constant{Value: types.NewIntDatum(1), RetType: types.NewFieldType(mysql.TypeLonglong)},
	))
	join := logicalop.LogicalJoin{
		EqualConditions: []*expression.ScalarFunction{joinEq},
		LeftConditions: expression.CNFExprs{
			newMutableColumn(71, 171, 'A'),
		},
		RightConditions: expression.CNFExprs{
			newMutableColumn(72, 172, 'C'),
		},
		OtherConditions: expression.CNFExprs{joinOther},
		DefaultValues:   []types.Datum{types.NewBytesDatum([]byte("dv"))},
		FullNames: types.NameSlice{
			&types.FieldName{ColName: ast.NewCIStr("c1")},
			&types.FieldName{ColName: ast.NewCIStr("c2")},
		},
	}.Init(ctx, 0)
	join.SetSchema(expression.NewSchema(
		newMutableColumn(81, 181, 'E'),
		newMutableColumn(82, 182, 'G'),
	))
	join.FullSchema = expression.NewSchema(
		newMutableColumn(83, 183, 'I'),
		newMutableColumn(84, 184, 'K'),
	)
	join.SetChildren(leftLimit, rightLimit)

	clonedJoin := join.DeepClone().(*logicalop.LogicalJoin)
	clonedLeftLimit := clonedJoin.Children()[0].(*logicalop.LogicalLimit)
	clonedLeftSort := clonedLeftLimit.Children()[0].(*logicalop.LogicalSort)
	clonedLeftAgg := clonedLeftSort.Children()[0].(*logicalop.LogicalAggregation)
	clonedLeftProj := clonedLeftAgg.Children()[0].(*logicalop.LogicalProjection)
	clonedLeftSel := clonedLeftProj.Children()[0].(*logicalop.LogicalSelection)
	clonedLeftDS := clonedLeftSel.Children()[0].(*logicalop.DataSource)
	clonedRightLimit := clonedJoin.Children()[1].(*logicalop.LogicalLimit)
	clonedRightSort := clonedRightLimit.Children()[0].(*logicalop.LogicalSort)
	clonedRightProj := clonedRightSort.Children()[0].(*logicalop.LogicalProjection)
	clonedRightSel := clonedRightProj.Children()[0].(*logicalop.LogicalSelection)
	clonedRightDS := clonedRightSel.Children()[0].(*logicalop.DataSource)

	require.NotSame(t, join, clonedJoin)
	require.NotSame(t, leftLimit, clonedLeftLimit)
	require.NotSame(t, leftSort, clonedLeftSort)
	require.NotSame(t, leftAgg, clonedLeftAgg)
	require.NotSame(t, leftProj, clonedLeftProj)
	require.NotSame(t, leftSel, clonedLeftSel)
	require.NotSame(t, leftDS, clonedLeftDS)
	require.NotSame(t, rightLimit, clonedRightLimit)
	require.NotSame(t, rightSort, clonedRightSort)
	require.NotSame(t, rightProj, clonedRightProj)
	require.NotSame(t, rightSel, clonedRightSel)
	require.NotSame(t, rightDS, clonedRightDS)
	require.NotSame(t, join.LeftConditions[0], clonedJoin.LeftConditions[0])
	require.NotSame(t, &join.DefaultValues[0], &clonedJoin.DefaultValues[0])
	require.NotSame(t, leftSort.ByItems[0], clonedLeftSort.ByItems[0])
	require.NotSame(t, leftAgg.AggFuncs[0], clonedLeftAgg.AggFuncs[0])
	require.NotSame(t, leftDS.PushedDownConds[0], clonedLeftDS.PushedDownConds[0])
	require.NotSame(t, leftDS.Schema(), clonedLeftDS.Schema())
	require.Same(t, clonedJoin, clonedJoin.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan).Self())

	clonedJoin.LeftConditions[0].(*expression.Column).RetType.SetType(mysql.TypeDouble)
	clonedJoin.LeftConditions[0].(*expression.Column).VirtualExpr.(*expression.Constant).Value.GetBytes()[0] = 'Z'
	clonedJoin.DefaultValues[0].GetBytes()[0] = 'x'
	clonedJoin.Schema().Columns[0].RetType.SetType(mysql.TypeDouble)
	clonedJoin.FullSchema.Columns[0].RetType.SetType(mysql.TypeDouble)

	clonedLeftLimit.PartitionBy[0].Col.RetType.SetType(mysql.TypeDouble)
	clonedLeftSort.ByItems[0].Expr.(*expression.Column).RetType.SetType(mysql.TypeDouble)
	clonedLeftAgg.GroupByItems[0].(*expression.Column).RetType.SetType(mysql.TypeDouble)
	clonedLeftAgg.AggFuncs[0].OrderByItems[0].Expr.(*expression.Column).RetType.SetType(mysql.TypeDouble)
	clonedLeftProj.Exprs[0].(*expression.Column).RetType.SetType(mysql.TypeDouble)
	clonedLeftSel.Conditions[0].(*expression.ScalarFunction).GetArgs()[0].(*expression.Column).RetType.SetType(mysql.TypeDouble)
	clonedLeftDS.PushedDownConds[0].(*expression.Column).VirtualExpr.(*expression.Constant).Value.GetBytes()[0] = 'Y'
	clonedLeftDS.TblColsByID[leftDSBaseCol.ID].RetType.SetType(mysql.TypeDouble)
	clonedLeftDS.Schema().Columns[0].RetType.SetType(mysql.TypeDouble)

	clonedRightLimit.PartitionBy[0].Col.RetType.SetType(mysql.TypeDouble)
	clonedRightSort.ByItems[0].Expr.(*expression.Column).RetType.SetType(mysql.TypeDouble)
	clonedRightProj.Exprs[0].(*expression.Column).RetType.SetType(mysql.TypeDouble)
	clonedRightSel.Conditions[0].(*expression.ScalarFunction).GetArgs()[0].(*expression.Column).RetType.SetType(mysql.TypeDouble)
	clonedRightDS.AllConds[0].(*expression.Column).VirtualExpr.(*expression.Constant).Value.GetBytes()[0] = 'X'

	require.Equal(t, mysql.TypeLonglong, join.LeftConditions[0].(*expression.Column).RetType.GetType())
	require.Equal(t, byte('A'), join.LeftConditions[0].(*expression.Column).VirtualExpr.(*expression.Constant).Value.GetBytes()[0])
	require.Equal(t, byte('d'), join.DefaultValues[0].GetBytes()[0])
	require.Equal(t, mysql.TypeLonglong, join.Schema().Columns[0].RetType.GetType())
	require.Equal(t, mysql.TypeLonglong, join.FullSchema.Columns[0].RetType.GetType())

	require.Equal(t, mysql.TypeLonglong, leftLimit.PartitionBy[0].Col.RetType.GetType())
	require.Equal(t, mysql.TypeLonglong, leftSort.ByItems[0].Expr.(*expression.Column).RetType.GetType())
	require.Equal(t, mysql.TypeLonglong, leftAgg.GroupByItems[0].(*expression.Column).RetType.GetType())
	require.Equal(t, mysql.TypeLonglong, leftAgg.AggFuncs[0].OrderByItems[0].Expr.(*expression.Column).RetType.GetType())
	require.Equal(t, mysql.TypeLonglong, leftProj.Exprs[0].(*expression.Column).RetType.GetType())
	require.Equal(t, mysql.TypeLonglong, leftSel.Conditions[0].(*expression.ScalarFunction).GetArgs()[0].(*expression.Column).RetType.GetType())
	require.Equal(t, byte('e'), leftDS.PushedDownConds[0].(*expression.Column).VirtualExpr.(*expression.Constant).Value.GetBytes()[0])
	require.Equal(t, mysql.TypeLonglong, leftDS.TblColsByID[leftDSBaseCol.ID].RetType.GetType())
	require.Equal(t, mysql.TypeLonglong, leftDS.Schema().Columns[0].RetType.GetType())

	require.Equal(t, mysql.TypeLonglong, rightLimit.PartitionBy[0].Col.RetType.GetType())
	require.Equal(t, mysql.TypeLonglong, rightSort.ByItems[0].Expr.(*expression.Column).RetType.GetType())
	require.Equal(t, mysql.TypeLonglong, rightProj.Exprs[0].(*expression.Column).RetType.GetType())
	require.Equal(t, mysql.TypeLonglong, rightSel.Conditions[0].(*expression.ScalarFunction).GetArgs()[0].(*expression.Column).RetType.GetType())
	require.Equal(t, byte('u'), rightDS.AllConds[0].(*expression.Column).VirtualExpr.(*expression.Constant).Value.GetBytes()[0])
}

func TestLogicalProjectionPushDownTopN(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec(`CREATE TABLE table_test (
col16 json DEFAULT NULL,
col17 json DEFAULT NULL
);`)
	sql := `explain format='brief' SELECT
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
		"Projection 20.00 root  Column#5, Column#6",
		"└─TopN 20.00 root  Column#7, Column#6, offset:0, count:20",
		"  └─Projection 10000.00 root  Column#5, Column#6, convert(cast(Column#5, var_string(16777216)), gbk)->Column#7",
		"    └─TableReader 10000.00 root  data:Projection",
		"      └─Projection 10000.00 cop[tikv]  json_extract(test.table_test.col16, $[].optUid)->Column#5, json_unquote(cast(json_extract(test.table_test.col17, $[0].value), var_string(16777216)))->Column#6",
		"        └─TableFullScan 10000.00 cop[tikv] table:table_test keep order:false, stats:pseudo"))
	tk.MustExec(`INSERT INTO mysql.opt_rule_blacklist VALUES("topn_push_down");`)
	tk.MustExec(`admin reload opt_rule_blacklist;`)
	tk.MustQuery(sql).Check(testkit.Rows(
		"Limit 20.00 root  offset:0, count:20",
		"└─Projection 20.00 root  Column#5, Column#6",
		"  └─Sort 20.00 root  Column#7, Column#6",
		"    └─Projection 10000.00 root  Column#5, Column#6, convert(cast(Column#5, var_string(16777216)), gbk)->Column#7",
		"      └─TableReader 10000.00 root  data:Projection",
		"        └─Projection 10000.00 cop[tikv]  json_extract(test.table_test.col16, $[].optUid)->Column#5, json_unquote(cast(json_extract(test.table_test.col17, $[0].value), var_string(16777216)))->Column#6",
		"          └─TableFullScan 10000.00 cop[tikv] table:table_test keep order:false, stats:pseudo"))
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
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format=brief " + tt).Rows())
			})
			res := tk.MustQuery("explain format=brief " + tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}
