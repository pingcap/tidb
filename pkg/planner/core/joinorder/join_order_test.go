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

package joinorder

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func TestExtractJoinGroupThroughProjection(t *testing.T) {
	ctx := coretestsdk.MockContext()
	t.Cleanup(func() { domain.GetDomain(ctx).StatsHandle().Close() })
	ctx.GetSessionVars().TiDBOptJoinReorderThroughProj = true
	ctx.GetSessionVars().TiDBOptJoinReorderThroughSel = true
	ctx.GetSessionVars().EnableOuterJoinReorder = true
	newLeaf := func() *logicalop.LogicalTableDual {
		leaf := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
		leaf.SetSchema(expression.NewSchema(&expression.Column{
			UniqueID: ctx.GetSessionVars().AllocPlanColumnID(), RetType: types.NewFieldType(mysql.TypeLonglong),
		}))
		return leaf
	}
	fn := func(name string, args ...expression.Expression) *expression.ScalarFunction {
		return expression.NewFunctionInternal(ctx, name, types.NewFieldType(mysql.TypeLonglong), args...).(*expression.ScalarFunction)
	}
	newJoin := func(tp base.JoinType, left, right base.LogicalPlan) *logicalop.LogicalJoin {
		join := logicalop.LogicalJoin{JoinType: tp}.Init(ctx, 0)
		join.SetChildren(left, right)
		join.MergeSchema()
		join.EqualConditions = []*expression.ScalarFunction{fn(ast.EQ, left.Schema().Columns[0], right.Schema().Columns[0])}
		return join
	}
	newProjection := func(child base.LogicalPlan, expr expression.Expression) *logicalop.LogicalProjection {
		proj := logicalop.LogicalProjection{Exprs: []expression.Expression{expr}}.Init(ctx, 0)
		proj.SetChildren(child)
		proj.SetSchema(expression.NewSchema(&expression.Column{
			UniqueID: ctx.GetSessionVars().AllocPlanColumnID(), RetType: expr.GetType(ctx.GetExprCtx().GetEvalCtx()).Clone(),
		}))
		return proj
	}
	left, right := newLeaf(), newLeaf()
	join := newJoin(base.InnerJoin, left, right)
	col := left.Schema().Columns[0]
	derived := fn(ast.Plus, col, expression.NewOne())
	proj := newProjection(join, derived)
	t.Run("safe and disabled", func(t *testing.T) {
		group := extractJoinGroup(proj)
		require.Len(t, group.vertexes, 2)
		require.Same(t, proj, group.root)
		require.True(t, expression.ExpressionsSemanticEqual(derived, group.colExprMap[proj.Schema().Columns[0].UniqueID]))
		ctx.GetSessionVars().TiDBOptJoinReorderThroughProj = false
		require.Equal(t, []base.LogicalPlan{proj}, extractJoinGroup(proj).vertexes)
		ctx.GetSessionVars().TiDBOptJoinReorderThroughProj = true
	})
	t.Run("unsafe expressions", func(t *testing.T) {
		for _, expr := range []expression.Expression{
			expression.NewOne(),
			fn(ast.Plus, col, right.Schema().Columns[0]),
			fn(ast.Plus, col, fn(ast.Rand)),
			fn(ast.Plus, col, fn(ast.FoundRows)),
			fn(ast.LastInsertId, col),
			fn(ast.Plus, col, &expression.Constant{Value: types.NewIntDatum(1), RetType: types.NewFieldType(mysql.TypeLonglong), DeferredExpr: fn(ast.FoundRows)}),
			&expression.CorrelatedColumn{Column: *col, Data: new(types.Datum)},
			newLeaf().Schema().Columns[0],
		} {
			unsafeProj := newProjection(join, expr)
			require.Equal(t, []base.LogicalPlan{unsafeProj}, extractJoinGroup(unsafeProj).vertexes)
		}
		expand := newProjection(join, derived)
		expand.Proj4Expand = true
		require.Equal(t, []base.LogicalPlan{expand}, extractJoinGroup(expand).vertexes)
	})
	t.Run("stacked projections and selection preserve definitions", func(t *testing.T) {
		outerProj := newProjection(proj, fn(ast.Plus, proj.Schema().Columns[0], expression.NewOne()))
		pred := fn(ast.GT, outerProj.Schema().Columns[0], expression.NewOne())
		sel := logicalop.LogicalSelection{Conditions: []expression.Expression{pred}}.Init(ctx, 0)
		sel.SetChildren(outerProj)
		group := extractJoinGroup(sel)
		require.Len(t, group.vertexes, 2)
		def := group.colExprMap[outerProj.Schema().Columns[0].UniqueID]
		require.True(t, expression.ExpressionsSemanticEqual(fn(ast.Plus, derived, expression.NewOne()), def))
		detector := newConflictDetector(ctx)
		_, err := detector.Build(group)
		require.NoError(t, err)
		for _, edge := range detector.innerEdges {
			for _, cond := range edge.nonEQConds {
				require.True(t, expression.ExprFromSchema(cond, join.Schema()))
			}
		}
		require.Same(t, outerProj.Schema().Columns[0], pred.GetArgs()[0])
		for _, threshold := range []int{0, 10} {
			ctx.GetSessionVars().TiDBOptJoinReorderThreshold = threshold
			result, err := optimizeForJoinGroup(ctx, group)
			require.NoError(t, err)
			require.True(t, result.Schema().Equal(sel.Schema()))
			restored := result.(*logicalop.LogicalProjection)
			require.True(t, expression.ExpressionsSemanticEqual(def, restored.Exprs[0]))
			require.True(t, expression.ExprFromSchema(restored.Exprs[0], restored.Children()[0].Schema()))
		}
	})
	t.Run("non-deterministic selection stays atomic", func(t *testing.T) {
		sel := logicalop.LogicalSelection{Conditions: []expression.Expression{fn(ast.EQ, fn(ast.FoundRows), proj.Schema().Columns[0])}}.Init(ctx, 0)
		sel.SetChildren(proj)
		require.Equal(t, []base.LogicalPlan{sel}, extractJoinGroup(sel).vertexes)
	})
	t.Run("join predicates are substituted without changing the original tree", func(t *testing.T) {
		other := newLeaf()
		root := newJoin(base.LeftOuterJoin, proj, other)
		root.LeftConditions = []expression.Expression{fn(ast.GT, proj.Schema().Columns[0], expression.NewOne())}
		root.RightConditions = []expression.Expression{fn(ast.GT, other.Schema().Columns[0], expression.NewOne())}
		root.OtherConditions = []expression.Expression{fn(ast.LT, proj.Schema().Columns[0], other.Schema().Columns[0])}
		originalEq := root.EqualConditions[0]
		group := extractJoinGroup(root)
		require.Len(t, group.vertexes, 3)
		detector := newConflictDetector(ctx)
		_, err := detector.Build(group)
		require.NoError(t, err)
		require.Len(t, detector.nonInnerEdges, 1)
		edge := detector.nonInnerEdges[0]
		schema := expression.MergeSchema(join.Schema(), other.Schema())
		for _, cond := range edge.eqConds {
			require.True(t, expression.ExprFromSchema(cond, schema))
		}
		require.Len(t, edge.nonEQConds, 3)
		for _, cond := range edge.nonEQConds {
			require.True(t, expression.ExprFromSchema(cond, schema))
		}
		require.Same(t, originalEq, root.EqualConditions[0])
		require.Same(t, proj.Schema().Columns[0], originalEq.GetArgs()[0])
		require.Same(t, proj, root.Children()[0])
	})
	t.Run("outer join boundaries", func(t *testing.T) {
		for _, tp := range []base.JoinType{base.LeftOuterJoin, base.RightOuterJoin} {
			outer := newJoin(tp, left, right)
			nullCol := right.Schema().Columns[0]
			if tp == base.RightOuterJoin {
				nullCol = col
			}
			aboveOuter := newProjection(outer, fn(ast.Ifnull, nullCol, expression.NewOne()))
			require.Equal(t, []base.LogicalPlan{aboveOuter}, extractJoinGroup(aboveOuter).vertexes)

			belowOuter := newProjection(join, fn(ast.Ifnull, col, expression.NewOne()))
			var root *logicalop.LogicalJoin
			if tp == base.LeftOuterJoin {
				root = newJoin(tp, newLeaf(), belowOuter)
			} else {
				root = newJoin(tp, belowOuter, newLeaf())
			}
			group := extractJoinGroup(root)
			require.Len(t, group.vertexes, 2)
			require.Contains(t, group.vertexes, belowOuter)
			require.Empty(t, group.colExprMap)
		}
	})
	t.Run("straight join stays atomic", func(t *testing.T) {
		straight := newJoin(base.InnerJoin, left, right)
		straight.StraightJoin = true
		aboveStraight := newProjection(straight, derived)
		require.Equal(t, []base.LogicalPlan{aboveStraight}, extractJoinGroup(aboveStraight).vertexes)
	})
	t.Run("method hint follows optimized projection", func(t *testing.T) {
		ctx.GetSessionVars().EnableAdvancedJoinHint = true
		for _, threshold := range []int{0, 10} {
			ctx.GetSessionVars().TiDBOptJoinReorderThreshold = threshold
			projected := newProjection(newJoin(base.InnerJoin, left, right), derived)
			root := newJoin(base.InnerJoin, projected, newLeaf())
			root.HintInfo = &hint.PlanHints{}
			root.LeftPreferJoinType = hint.PreferHashJoin
			root.SetPreferredJoinType()

			result, err := Optimize(root)
			require.NoError(t, err)
			if restore, ok := result.(*logicalop.LogicalProjection); ok {
				result = restore.Children()[0]
			}
			reordered := result.(*logicalop.LogicalJoin)
			if reordered.Children()[0].Schema().Contains(projected.Schema().Columns[0]) {
				require.Equal(t, hint.PreferHashJoin, reordered.LeftPreferJoinType)
				require.Zero(t, reordered.RightPreferJoinType)
			} else {
				require.True(t, reordered.Children()[1].Schema().Contains(projected.Schema().Columns[0]))
				require.Equal(t, hint.PreferHashJoin, reordered.RightPreferJoinType)
				require.Zero(t, reordered.LeftPreferJoinType)
			}
			require.Same(t, root.HintInfo, reordered.HintInfo)
		}
	})
}

func TestAlignEQCondsWithProjectionExpressions(t *testing.T) {
	ctx := coretestsdk.MockContext()
	t.Cleanup(func() { domain.GetDomain(ctx).StatsHandle().Close() })
	for _, swapped := range []bool{false, true} {
		leftInput := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
		rightInput := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
		leftInput.SetSchema(expression.NewSchema(&expression.Column{UniqueID: ctx.GetSessionVars().AllocPlanColumnID(), RetType: types.NewFieldType(mysql.TypeLonglong)}))
		rightInput.SetSchema(expression.NewSchema(&expression.Column{UniqueID: ctx.GetSessionVars().AllocPlanColumnID(), RetType: types.NewFieldType(mysql.TypeLonglong)}))
		left := logicalop.LogicalProjection{Exprs: expression.Column2Exprs(leftInput.Schema().Columns)}.Init(ctx, 0)
		left.SetSchema(leftInput.Schema().Clone())
		left.SetChildren(leftInput)
		right := logicalop.LogicalProjection{Exprs: expression.Column2Exprs(rightInput.Schema().Columns)}.Init(ctx, 0)
		right.SetSchema(rightInput.Schema().Clone())
		right.SetChildren(rightInput)
		leftExpr := expression.NewFunctionInternal(ctx, ast.Plus, types.NewFieldType(mysql.TypeLonglong), left.Schema().Columns[0], expression.NewOne())
		rightExpr := expression.NewFunctionInternal(ctx, ast.Plus, types.NewFieldType(mysql.TypeLonglong), right.Schema().Columns[0], expression.NewOne())
		arg0, arg1 := leftExpr, rightExpr
		if swapped {
			arg0, arg1 = arg1, arg0
		}
		cond := expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), arg0, arg1).(*expression.ScalarFunction)
		newLeft, newRight, aligned, err := alignEQConds(ctx, left, right, []*expression.ScalarFunction{cond})
		require.NoError(t, err)
		require.Len(t, aligned, 1)
		require.IsType(t, &expression.Column{}, aligned[0].GetArgs()[0])
		require.IsType(t, &expression.Column{}, aligned[0].GetArgs()[1])
		require.True(t, expression.ExprFromSchema(aligned[0].GetArgs()[0], newLeft.Schema()))
		require.True(t, expression.ExprFromSchema(aligned[0].GetArgs()[1], newRight.Schema()))
		require.Same(t, arg0, cond.GetArgs()[0])
		require.Same(t, arg1, cond.GetArgs()[1])
		require.Len(t, left.Exprs, 1)
		require.Len(t, left.Schema().Columns, 1)
		require.Len(t, right.Exprs, 1)
		require.Len(t, right.Schema().Columns, 1)
		require.Same(t, left, newLeft.Children()[0])
		require.Same(t, right, newRight.Children()[0])

		check := &CheckConnectionResult{
			// Cached costs include costs accumulated in earlier enumeration steps.
			node1: &Node{p: left, cumCost: 30}, node2: &Node{p: right, cumCost: 20},
			appliedInnerEdges: []*edge{{joinType: base.InnerJoin, eqConds: []*expression.ScalarFunction{cond}}},
		}
		vertexHints := map[int]*JoinMethodHint{
			left.ID():  {PreferJoinMethod: hint.PreferHashJoin},
			right.ID(): {PreferJoinMethod: hint.PreferHashJoin},
		}
		inner, err := makeInnerJoin(ctx, check, nil, vertexHints)
		require.NoError(t, err)
		require.Same(t, left, check.node1.p)
		require.Same(t, right, check.node2.p)
		require.Equal(t, hint.PreferHashJoin, inner.(*logicalop.LogicalJoin).LeftPreferJoinType)
		require.Equal(t, hint.PreferHashJoin, inner.(*logicalop.LogicalJoin).RightPreferJoinType)
		t.Run("expression key materialization cost", func(t *testing.T) {
			result, err := newConflictDetector(ctx).MakeJoin(check, vertexHints)
			require.NoError(t, err)
			inputCost := check.node1.cumCost + check.node2.cumCost
			// One new projection per input, each preserving its input row count.
			want := inputCost + left.StatsInfo().RowCount + right.StatsInfo().RowCount + result.p.StatsInfo().RowCount
			require.Equal(t, want, result.cumCost)
		})
		check.appliedNonInnerEdge = &edge{joinType: base.LeftOuterJoin, eqConds: []*expression.ScalarFunction{cond}}
		outer, err := makeNonInnerJoin(ctx, check, vertexHints)
		require.NoError(t, err)
		require.Same(t, left, check.node1.p)
		require.Same(t, right, check.node2.p)
		require.Equal(t, hint.PreferHashJoin, outer.LeftPreferJoinType)
		require.Equal(t, hint.PreferHashJoin, outer.RightPreferJoinType)
		t.Run("selection over outer join cost", func(t *testing.T) {
			columnEQ := expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), left.Schema().Columns[0], right.Schema().Columns[0]).(*expression.ScalarFunction)
			check.appliedNonInnerEdge.eqConds = []*expression.ScalarFunction{columnEQ}
			result, err := newConflictDetector(ctx).MakeJoin(check, vertexHints)
			require.NoError(t, err)
			require.IsType(t, &logicalop.LogicalSelection{}, result.p)
			child := result.p.Children()[0].(*logicalop.LogicalJoin)
			require.Equal(t, base.LeftOuterJoin, child.JoinType)
			want := check.node1.cumCost + check.node2.cumCost + child.StatsInfo().RowCount + result.p.StatsInfo().RowCount
			require.Equal(t, want, result.cumCost)
		})
		require.Len(t, left.Exprs, 1)
		require.Len(t, right.Exprs, 1)
	}
}

func TestChooseBestGreedyStart(t *testing.T) {
	t.Run("pick lowest cost", func(t *testing.T) {
		best, startIdx, err := chooseBestGreedyStart(2, func(startIdx int) (*Node, error) {
			costs := []float64{100, 10}
			return &Node{cumCost: costs[startIdx]}, nil
		})
		require.NoError(t, err)
		require.NotNil(t, best)
		require.Equal(t, 1, startIdx)
		require.Equal(t, float64(10), best.cumCost)
	})

	t.Run("skip nil candidate", func(t *testing.T) {
		best, startIdx, err := chooseBestGreedyStart(2, func(startIdx int) (*Node, error) {
			if startIdx == 0 {
				return nil, nil
			}
			return &Node{cumCost: 10}, nil
		})
		require.NoError(t, err)
		require.NotNil(t, best)
		require.Equal(t, 1, startIdx)
		require.Equal(t, float64(10), best.cumCost)
	})

	t.Run("keep earlier start for floating point noise", func(t *testing.T) {
		best, startIdx, err := chooseBestGreedyStart(2, func(startIdx int) (*Node, error) {
			costs := []float64{14166.666666666668, 14166.666666666666}
			return &Node{cumCost: costs[startIdx]}, nil
		})
		require.NoError(t, err)
		require.NotNil(t, best)
		require.Equal(t, 0, startIdx)
		require.Equal(t, 14166.666666666668, best.cumCost)
	})
}

func TestCloneNodesForGreedyStartIsolation(t *testing.T) {
	ctx := coretestsdk.MockContext()
	t.Cleanup(func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	})

	original := []*Node{{
		cumCost:   7,
		usedEdges: map[uint64]struct{}{1: {}},
	}}
	cloned := cloneNodesForGreedyStart(original)
	require.Len(t, cloned, 1)
	require.NotSame(t, original[0], cloned[0])

	delete(cloned[0].usedEdges, 1)
	cloned[0].usedEdges[2] = struct{}{}
	require.Contains(t, original[0].usedEdges, uint64(1))
	require.NotContains(t, original[0].usedEdges, uint64(2))

	cloned[0].p = logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
	require.Nil(t, original[0].p)
	require.NotNil(t, cloned[0].p)
}
