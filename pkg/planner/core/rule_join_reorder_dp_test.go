// Copyright 2018 PingCAP, Inc.
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

package core

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/joinorder"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

type mockLogicalJoin struct {
	logicalop.LogicalSchemaProducer
	involvedNodeSet int
	statsMap        map[int]*property.StatsInfo
	JoinType        base.JoinType
}

func (mj mockLogicalJoin) init(ctx base.PlanContext) *mockLogicalJoin {
	mj.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, "MockLogicalJoin", &mj, 0)
	return &mj
}

// RecursiveDeriveStats implements LogicalPlan interface.
func (mj *mockLogicalJoin) RecursiveDeriveStats(_ [][]*expression.Column) (*property.StatsInfo, bool, error) {
	if mj.StatsInfo() == nil {
		mj.SetStats(mj.statsMap[mj.involvedNodeSet])
		return mj.statsMap[mj.involvedNodeSet], true, nil
	}
	return mj.statsMap[mj.involvedNodeSet], false, nil
}

func newMockJoin(ctx base.PlanContext, statsMap map[int]*property.StatsInfo) func(lChild, rChild base.LogicalPlan, _ []*expression.ScalarFunction, _, _, _ []expression.Expression, joinType base.JoinType) base.LogicalPlan {
	return func(lChild, rChild base.LogicalPlan, _ []*expression.ScalarFunction, _, _, _ []expression.Expression, joinType base.JoinType) base.LogicalPlan {
		retJoin := mockLogicalJoin{}.init(ctx)
		retJoin.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
		retJoin.statsMap = statsMap
		if mj, ok := lChild.(*mockLogicalJoin); ok {
			retJoin.involvedNodeSet = mj.involvedNodeSet
		} else {
			retJoin.involvedNodeSet = 1 << uint(lChild.ID())
		}
		if mj, ok := rChild.(*mockLogicalJoin); ok {
			retJoin.involvedNodeSet |= mj.involvedNodeSet
		} else {
			retJoin.involvedNodeSet |= 1 << uint(rChild.ID())
		}
		retJoin.SetChildren(lChild, rChild)
		retJoin.JoinType = joinType
		return retJoin
	}
}

func makeStatsMapForTPCHQ5() map[int]*property.StatsInfo {
	// Labeled as lineitem -> 0, orders -> 1, customer -> 2, supplier 3, nation 4, region 5
	// This graph can be shown as following:
	// +---------------+            +---------------+
	// |               |            |               |
	// |    lineitem   +------------+    orders     |
	// |               |            |               |
	// +-------+-------+            +-------+-------+
	//         |                            |
	//         |                            |
	//         |                            |
	// +-------+-------+            +-------+-------+
	// |               |            |               |
	// |   supplier    +------------+    customer   |
	// |               |            |               |
	// +-------+-------+            +-------+-------+
	//         |                            |
	//         |                            |
	//         |                            |
	//         |                            |
	//         |      +---------------+     |
	//         |      |               |     |
	//         +------+    nation     +-----+
	//                |               |
	//                +---------------+
	//                        |
	//                +---------------+
	//                |               |
	//                |    region     |
	//                |               |
	//                +---------------+
	statsMap := make(map[int]*property.StatsInfo)
	statsMap[3] = &property.StatsInfo{RowCount: 9103367}
	statsMap[6] = &property.StatsInfo{RowCount: 2275919}
	statsMap[7] = &property.StatsInfo{RowCount: 9103367}
	statsMap[9] = &property.StatsInfo{RowCount: 59986052}
	statsMap[11] = &property.StatsInfo{RowCount: 9103367}
	statsMap[12] = &property.StatsInfo{RowCount: 5999974575}
	statsMap[13] = &property.StatsInfo{RowCount: 59999974575}
	statsMap[14] = &property.StatsInfo{RowCount: 9103543072}
	statsMap[15] = &property.StatsInfo{RowCount: 99103543072}
	statsMap[20] = &property.StatsInfo{RowCount: 1500000}
	statsMap[22] = &property.StatsInfo{RowCount: 2275919}
	statsMap[23] = &property.StatsInfo{RowCount: 7982159}
	statsMap[24] = &property.StatsInfo{RowCount: 100000}
	statsMap[25] = &property.StatsInfo{RowCount: 59986052}
	statsMap[27] = &property.StatsInfo{RowCount: 9103367}
	statsMap[28] = &property.StatsInfo{RowCount: 5999974575}
	statsMap[29] = &property.StatsInfo{RowCount: 59999974575}
	statsMap[30] = &property.StatsInfo{RowCount: 59999974575}
	statsMap[31] = &property.StatsInfo{RowCount: 59999974575}
	statsMap[48] = &property.StatsInfo{RowCount: 5}
	statsMap[52] = &property.StatsInfo{RowCount: 299838}
	statsMap[54] = &property.StatsInfo{RowCount: 454183}
	statsMap[55] = &property.StatsInfo{RowCount: 1815222}
	statsMap[56] = &property.StatsInfo{RowCount: 20042}
	statsMap[57] = &property.StatsInfo{RowCount: 12022687}
	statsMap[59] = &property.StatsInfo{RowCount: 1823514}
	statsMap[60] = &property.StatsInfo{RowCount: 1201884359}
	statsMap[61] = &property.StatsInfo{RowCount: 12001884359}
	statsMap[62] = &property.StatsInfo{RowCount: 12001884359}
	statsMap[63] = &property.StatsInfo{RowCount: 72985}
	return statsMap
}

func newDataSource(ctx base.PlanContext, name string, count int) base.LogicalPlan {
	ds := logicalop.DataSource{}.Init(ctx, 0)
	tan := ast.NewCIStr(name)
	ds.TableAsName = &tan
	ds.SetSchema(expression.NewSchema())
	ds.Schema().Append(&expression.Column{
		UniqueID: ctx.GetSessionVars().PlanColumnID.Add(1),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	ds.SetStats(&property.StatsInfo{
		RowCount: float64(count),
	})
	return ds
}

func planToString(plan base.LogicalPlan) string {
	switch x := plan.(type) {
	case *mockLogicalJoin:
		return fmt.Sprintf("MockJoin{%v, %v}", planToString(x.Children()[0]), planToString(x.Children()[1]))
	case *logicalop.DataSource:
		return x.TableAsName.L
	}
	return ""
}

func TestDPReorderTPCHQ5(t *testing.T) {
	statsMap := makeStatsMapForTPCHQ5()

	ctx := coretestsdk.MockContext()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	ctx.GetSessionVars().PlanID.Store(-1)
	joinGroups := make([]base.LogicalPlan, 0, 6)
	joinGroups = append(joinGroups, newDataSource(ctx, "lineitem", 59986052))
	joinGroups = append(joinGroups, newDataSource(ctx, "orders", 15000000))
	joinGroups = append(joinGroups, newDataSource(ctx, "customer", 1500000))
	joinGroups = append(joinGroups, newDataSource(ctx, "supplier", 100000))
	joinGroups = append(joinGroups, newDataSource(ctx, "nation", 25))
	joinGroups = append(joinGroups, newDataSource(ctx, "region", 5))

	var eqConds []expression.Expression
	eqConds = append(eqConds, expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), joinGroups[0].Schema().Columns[0], joinGroups[1].Schema().Columns[0]))
	eqConds = append(eqConds, expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), joinGroups[1].Schema().Columns[0], joinGroups[2].Schema().Columns[0]))
	eqConds = append(eqConds, expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), joinGroups[2].Schema().Columns[0], joinGroups[3].Schema().Columns[0]))
	eqConds = append(eqConds, expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), joinGroups[0].Schema().Columns[0], joinGroups[3].Schema().Columns[0]))
	eqConds = append(eqConds, expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), joinGroups[2].Schema().Columns[0], joinGroups[4].Schema().Columns[0]))
	eqConds = append(eqConds, expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), joinGroups[3].Schema().Columns[0], joinGroups[4].Schema().Columns[0]))
	eqConds = append(eqConds, expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), joinGroups[4].Schema().Columns[0], joinGroups[5].Schema().Columns[0]))
	eqEdges := make([]*expression.ScalarFunction, 0, len(eqConds))
	for _, cond := range eqConds {
		sf, isSF := cond.(*expression.ScalarFunction)
		require.True(t, isSF)
		eqEdges = append(eqEdges, sf)
	}
	basicJoinGroupInfo := &basicJoinGroupInfo{
		eqEdges: eqEdges,
	}
	baseGroupSolver := &baseSingleGroupJoinOrderSolver{
		ctx:                ctx,
		basicJoinGroupInfo: basicJoinGroupInfo,
	}
	solver := &joinReorderDPSolver{
		baseSingleGroupJoinOrderSolver: baseGroupSolver,
		newJoin:                        newMockJoin(ctx, statsMap),
	}
	result, err := solver.solve(joinGroups)
	require.NoError(t, err)

	expected := "MockJoin{supplier, MockJoin{lineitem, MockJoin{orders, MockJoin{customer, MockJoin{nation, region}}}}}"
	require.Equal(t, expected, planToString(result))
}

func TestDPReorderAllCartesian(t *testing.T) {
	statsMap := makeStatsMapForTPCHQ5()

	ctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	ctx.GetSessionVars().PlanID.Store(-1)

	joinGroup := make([]base.LogicalPlan, 0, 4)
	joinGroup = append(joinGroup, newDataSource(ctx, "a", 100))
	joinGroup = append(joinGroup, newDataSource(ctx, "b", 100))
	joinGroup = append(joinGroup, newDataSource(ctx, "c", 100))
	joinGroup = append(joinGroup, newDataSource(ctx, "d", 100))
	solver := &joinReorderDPSolver{
		baseSingleGroupJoinOrderSolver: &baseSingleGroupJoinOrderSolver{
			ctx:                ctx,
			basicJoinGroupInfo: &basicJoinGroupInfo{},
		},
		newJoin: newMockJoin(ctx, statsMap),
	}
	result, err := solver.solve(joinGroup)
	require.NoError(t, err)

	expected := "MockJoin{MockJoin{a, b}, MockJoin{c, d}}"
	require.Equal(t, expected, planToString(result))
}

func TestInjectedJoinExprMaterializationSafety(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	ctx.GetSessionVars().PlanID.Store(-1)

	makeRandPlusCol := func(col *expression.Column) expression.Expression {
		randExpr := expression.NewFunctionInternal(ctx, ast.Rand, types.NewFieldType(mysql.TypeDouble))
		return expression.NewFunctionInternal(ctx, ast.Plus, types.NewFieldType(mysql.TypeDouble), randExpr, col)
	}

	t.Run("injectExpr keeps repeated rand expressions independent", func(t *testing.T) {
		solver := &baseSingleGroupJoinOrderSolver{
			ctx:                ctx,
			basicJoinGroupInfo: &basicJoinGroupInfo{},
		}
		plan := newDataSource(ctx, "t", 1)
		baseCol := plan.Schema().Columns[0]

		expr1 := makeRandPlusCol(baseCol)
		expr2 := makeRandPlusCol(baseCol)
		require.True(t, expression.ExpressionsSemanticEqual(expr1, expr2))

		plan, injected1 := solver.injectExpr(plan, expr1)
		plan, injected2 := solver.injectExpr(plan, expr2)

		proj, ok := plan.(*logicalop.LogicalProjection)
		require.True(t, ok)
		require.Len(t, proj.Exprs, 3)
		require.NotEqual(t, injected1.UniqueID, injected2.UniqueID)
	})

	t.Run("injectExpr keeps appended expression in child space for existing projections", func(t *testing.T) {
		solver := &baseSingleGroupJoinOrderSolver{
			ctx:                ctx,
			basicJoinGroupInfo: &basicJoinGroupInfo{},
		}
		plan := newDataSource(ctx, "p", 1)
		baseCol := plan.Schema().Columns[0]

		proj := logicalop.LogicalProjection{Exprs: expression.Column2Exprs(plan.Schema().Columns)}.Init(ctx, 0)
		proj.SetSchema(plan.Schema().Clone())
		proj.SetChildren(plan)

		derivedExpr := expression.NewFunctionInternal(ctx, ast.Plus, types.NewFieldType(mysql.TypeLonglong), baseCol, expression.NewOne())
		derivedCol := proj.AppendExpr(derivedExpr)

		exprUsingProjOutput := expression.NewFunctionInternal(ctx, ast.Plus, types.NewFieldType(mysql.TypeLonglong), derivedCol, expression.NewOne())
		newPlan, injectedCol := solver.injectExpr(proj, exprUsingProjOutput)

		newProj, ok := newPlan.(*logicalop.LogicalProjection)
		require.True(t, ok)
		require.NotNil(t, injectedCol)
		require.Len(t, newProj.Exprs, 3)

		appendedExpr := newProj.Exprs[2]
		expectedExpr := expression.NewFunctionInternal(
			ctx,
			ast.Plus,
			types.NewFieldType(mysql.TypeLonglong),
			derivedExpr,
			expression.NewOne(),
		)
		require.True(t, expression.ExpressionsSemanticEqual(appendedExpr, expectedExpr))

		appendedCols := expression.ExtractColumns(appendedExpr)
		require.Len(t, appendedCols, 1)
		require.Equal(t, baseCol.UniqueID, appendedCols[0].UniqueID)
	})
}

func TestJoinReorderInlineSafetyGates(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	ctx.GetSessionVars().PlanID.Store(-1)

	newJoin := func(joinType base.JoinType, left, right base.LogicalPlan) *logicalop.LogicalJoin {
		join := logicalop.LogicalJoin{JoinType: joinType}.Init(ctx, 0)
		join.SetSchema(expression.MergeSchema(left.Schema(), right.Schema()))
		join.SetChildren(left, right)
		return join
	}

	newProjection := func(child base.LogicalPlan, exprs ...expression.Expression) *logicalop.LogicalProjection {
		proj := logicalop.LogicalProjection{Exprs: exprs}.Init(ctx, 0)
		schema := expression.NewSchema()
		for _, expr := range exprs {
			col := &expression.Column{
				UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  expr.GetType(ctx.GetExprCtx().GetEvalCtx()).Clone(),
			}
			col.SetCoercibility(expr.Coercibility())
			col.SetRepertoire(expr.Repertoire())
			schema.Append(col)
		}
		proj.SetSchema(schema)
		proj.SetChildren(child)
		return proj
	}

	t.Run("selection rejects non-deterministic predicates", func(t *testing.T) {
		left := newDataSource(ctx, "sel_l", 10)
		right := newDataSource(ctx, "sel_r", 10)
		join := newJoin(base.InnerJoin, left, right)

		old := ctx.GetSessionVars().TiDBOptJoinReorderThroughSel
		ctx.GetSessionVars().TiDBOptJoinReorderThroughSel = true
		t.Cleanup(func() {
			ctx.GetSessionVars().TiDBOptJoinReorderThroughSel = old
		})

		foundRows := expression.NewFunctionInternal(ctx, ast.FoundRows, types.NewFieldType(mysql.TypeLonglong))
		pred := expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), foundRows, expression.NewOne())
		sel := logicalop.LogicalSelection{Conditions: []expression.Expression{pred}}.Init(ctx, 0)
		sel.SetChildren(join)

		result := extractJoinGroup(sel)
		require.Len(t, result.group, 1)
		require.Same(t, sel, result.group[0])
	})

	t.Run("projection basic gate rejects non-deterministic expressions", func(t *testing.T) {
		left := newDataSource(ctx, "basic_l", 10)
		right := newDataSource(ctx, "basic_r", 10)
		join := newJoin(base.InnerJoin, left, right)

		foundRows := expression.NewFunctionInternal(ctx, ast.FoundRows, types.NewFieldType(mysql.TypeLonglong))
		expr := expression.NewFunctionInternal(
			ctx,
			ast.Plus,
			types.NewFieldType(mysql.TypeLonglong),
			left.Schema().Columns[0],
			foundRows,
		)
		proj := newProjection(join, expr)
		require.False(t, canInlineProjectionBasic(proj))
	})

	t.Run("projection leaf gate rejects cross-leaf expressions", func(t *testing.T) {
		left := newDataSource(ctx, "cross_l", 10)
		right := newDataSource(ctx, "cross_r", 10)
		join := newJoin(base.InnerJoin, left, right)

		expr := expression.NewFunctionInternal(
			ctx,
			ast.Plus,
			types.NewFieldType(mysql.TypeLonglong),
			left.Schema().Columns[0],
			right.Schema().Columns[0],
		)
		proj := newProjection(join, expr)
		require.True(t, canInlineProjectionBasic(proj))
		require.False(t, canInlineProjection(proj, &joinGroupResult{
			group:              []base.LogicalPlan{left, right},
			basicJoinGroupInfo: &basicJoinGroupInfo{},
		}))
	})

	t.Run("projection leaf gate rejects null-extended expressions", func(t *testing.T) {
		left := newDataSource(ctx, "outer_l", 10)
		right := newDataSource(ctx, "outer_r", 10)
		join := newJoin(base.LeftOuterJoin, left, right)

		expr := expression.NewFunctionInternal(
			ctx,
			ast.Plus,
			types.NewFieldType(mysql.TypeLonglong),
			right.Schema().Columns[0],
			expression.NewOne(),
		)
		proj := newProjection(join, expr)
		require.True(t, canInlineProjectionBasic(proj))
		require.False(t, canInlineProjection(proj, &joinGroupResult{
			group: []base.LogicalPlan{left, right},
			basicJoinGroupInfo: &basicJoinGroupInfo{
				nullExtendedCols: expression.NewSchema(right.Schema().Columns...),
			},
		}))
	})

	t.Run("tryInlineProjectionForJoinGroup keeps safe single-leaf derived columns", func(t *testing.T) {
		left := newDataSource(ctx, "inline_l", 10)
		right := newDataSource(ctx, "inline_r", 10)
		join := newJoin(base.InnerJoin, left, right)

		expr := expression.NewFunctionInternal(
			ctx,
			ast.Plus,
			types.NewFieldType(mysql.TypeLonglong),
			left.Schema().Columns[0],
			expression.NewOne(),
		)
		proj := newProjection(join, expr)

		result, handled := tryInlineProjectionForJoinGroup(proj, proj)
		require.True(t, handled)
		require.Len(t, result.group, 2)
		require.Contains(t, result.colExprMap, proj.Schema().Columns[0].UniqueID)
		require.True(t, expression.ExpressionsSemanticEqual(result.colExprMap[proj.Schema().Columns[0].UniqueID], expr))
	})

	t.Run("tryInlineProjectionForJoinGroup keeps cross-leaf projection atomic", func(t *testing.T) {
		left := newDataSource(ctx, "atomic_l", 10)
		right := newDataSource(ctx, "atomic_r", 10)
		join := newJoin(base.InnerJoin, left, right)

		expr := expression.NewFunctionInternal(
			ctx,
			ast.Plus,
			types.NewFieldType(mysql.TypeLonglong),
			left.Schema().Columns[0],
			right.Schema().Columns[0],
		)
		proj := newProjection(join, expr)

		result, handled := tryInlineProjectionForJoinGroup(proj, proj)
		require.True(t, handled)
		require.Len(t, result.group, 1)
		require.Same(t, proj, result.group[0])
	})

	t.Run("outer join side filters touching multiple leaves block reorder", func(t *testing.T) {
		left0 := newDataSource(ctx, "oj_l0", 10)
		left1 := newDataSource(ctx, "oj_l1", 10)
		right := newDataSource(ctx, "oj_r", 10)
		join := newJoin(base.LeftOuterJoin, left0, right)

		multiLeafFilter := expression.NewFunctionInternal(
			ctx,
			ast.GT,
			types.NewFieldType(mysql.TypeTiny),
			expression.NewFunctionInternal(
				ctx,
				ast.Plus,
				types.NewFieldType(mysql.TypeLonglong),
				left0.Schema().Columns[0],
				left1.Schema().Columns[0],
			),
			expression.NewZero(),
		)
		join.LeftConditions = []expression.Expression{multiLeafFilter}

		require.True(t, joinorder.OuterJoinSideFiltersTouchMultipleLeaves(
			join,
			[]base.LogicalPlan{left0, left1},
			nil,
			true,
		))
	})
}
