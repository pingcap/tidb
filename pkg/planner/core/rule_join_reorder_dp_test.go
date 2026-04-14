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

func TestInjectedJoinExprReuseSkipsMutableExpressions(t *testing.T) {
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

	t.Run("buildJoinEdge does not expose rand expressions for otherConds reuse", func(t *testing.T) {
		solver := &baseSingleGroupJoinOrderSolver{
			ctx:                ctx,
			basicJoinGroupInfo: &basicJoinGroupInfo{},
		}
		leftPlan := newDataSource(ctx, "l", 1)
		rightPlan := newDataSource(ctx, "r", 1)
		leftCol := leftPlan.Schema().Columns[0]
		rightExpr := makeRandPlusCol(rightPlan.Schema().Columns[0])

		originalEdge := expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), leftCol, rightExpr).(*expression.ScalarFunction)
		newEdge, expr2Col := solver.buildJoinEdge(originalEdge, leftCol, rightExpr, &leftPlan, &rightPlan)

		require.NotNil(t, newEdge)
		require.Empty(t, expr2Col)

		proj, ok := rightPlan.(*logicalop.LogicalProjection)
		require.True(t, ok)
		require.Len(t, proj.Exprs, 2)
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

	t.Run("SubstituteExprsWithColsInExprs skips mutable expressions defensively", func(t *testing.T) {
		plan := newDataSource(ctx, "s", 1)
		baseCol := plan.Schema().Columns[0]
		volatileExpr := makeRandPlusCol(baseCol)
		replacementCol := &expression.Column{
			UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  volatileExpr.GetType(ctx.GetExprCtx().GetEvalCtx()).Clone(),
		}

		got := joinorder.SubstituteExprsWithColsInExprs(
			[]expression.Expression{volatileExpr},
			map[string]*expression.Column{string(volatileExpr.CanonicalHashCode()): replacementCol},
		)
		require.Len(t, got, 1)
		require.Same(t, volatileExpr, got[0])
	})

	t.Run("SubstituteExprsWithColsInExprs skips conditionally evaluated descendants", func(t *testing.T) {
		plan := newDataSource(ctx, "c", 1)
		baseCol := plan.Schema().Columns[0]
		repeatedExpr := expression.NewFunctionInternal(ctx, ast.Plus, types.NewFieldType(mysql.TypeLonglong), baseCol, expression.NewOne())
		replacementCol := &expression.Column{
			UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  repeatedExpr.GetType(ctx.GetExprCtx().GetEvalCtx()).Clone(),
		}

		plainPredicate := expression.NewFunctionInternal(ctx, ast.GT, types.NewFieldType(mysql.TypeTiny), repeatedExpr, expression.NewZero())
		gotPlain := joinorder.SubstituteExprsWithColsInExprs(
			[]expression.Expression{plainPredicate},
			map[string]*expression.Column{string(repeatedExpr.CanonicalHashCode()): replacementCol},
		)
		require.Len(t, gotPlain, 1)
		require.NotSame(t, plainPredicate, gotPlain[0])
		plainCols := expression.ExtractColumns(gotPlain[0])
		require.Len(t, plainCols, 1)
		require.Equal(t, replacementCol.UniqueID, plainCols[0].UniqueID)

		guardedPredicate := expression.NewFunctionInternal(
			ctx,
			ast.If,
			types.NewFieldType(mysql.TypeLonglong),
			expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), baseCol, expression.NewZero()),
			expression.NewZero(),
			repeatedExpr,
		)
		gotGuarded := joinorder.SubstituteExprsWithColsInExprs(
			[]expression.Expression{guardedPredicate},
			map[string]*expression.Column{string(repeatedExpr.CanonicalHashCode()): replacementCol},
		)
		require.Len(t, gotGuarded, 1)
		require.Same(t, guardedPredicate, gotGuarded[0])
	})
}
