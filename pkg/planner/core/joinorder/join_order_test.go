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
	"reflect"
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

func TestRebindJoinMethodHintsAllHints(t *testing.T) {
	ctx := coretestsdk.MockContext()
	t.Cleanup(func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	})

	oldShuffle := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
	newShuffle := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
	oldMerge := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
	newMerge := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)

	hints := map[int]*JoinMethodHint{
		oldShuffle.ID(): {PreferJoinMethod: hint.PreferShuffleJoin},
		oldMerge.ID():   {PreferJoinMethod: hint.PreferMergeJoin},
	}
	vertexMap := map[int]base.LogicalPlan{
		oldShuffle.ID(): newShuffle,
		oldMerge.ID():   newMerge,
	}

	rebound := RebindJoinMethodHints(hints, vertexMap)
	require.Contains(t, rebound, newShuffle.ID())
	require.Contains(t, rebound, newMerge.ID())
	require.Equal(t, hint.PreferShuffleJoin, rebound[newShuffle.ID()].PreferJoinMethod)
	require.Equal(t, hint.PreferMergeJoin, rebound[newMerge.ID()].PreferJoinMethod)

	// When no hinted vertex changed ID, RebindJoinMethodHints should return the input map directly.
	sameVertexMap := map[int]base.LogicalPlan{
		newShuffle.ID(): newShuffle,
		newMerge.ID():   newMerge,
	}
	unchanged := RebindJoinMethodHints(rebound, sameVertexMap)
	require.Equal(t, reflect.ValueOf(rebound).Pointer(), reflect.ValueOf(unchanged).Pointer())
}

func TestMakeJoinAlignEQCondsPropagatesVertexHint(t *testing.T) {
	ctx := coretestsdk.MockContext()
	t.Cleanup(func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	})

	intTp := types.NewFieldType(mysql.TypeLonglong)
	col1 := &expression.Column{UniqueID: ctx.GetSessionVars().AllocPlanColumnID(), RetType: intTp}
	col2 := &expression.Column{UniqueID: ctx.GetSessionVars().AllocPlanColumnID(), RetType: intTp}

	leftLeaf := logicalop.LogicalTableDual{RowCount: 10}.Init(ctx, 0)
	leftLeaf.SetSchema(expression.NewSchema(col1))
	rightLeaf := logicalop.LogicalTableDual{RowCount: 10}.Init(ctx, 0)
	rightLeaf.SetSchema(expression.NewSchema(col2))

	plusExpr := expression.NewFunctionInternal(ctx.GetExprCtx(), ast.Plus, intTp, col2, expression.NewOne())
	eqCond := expression.NewFunctionInternal(ctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), plusExpr, col1).(*expression.ScalarFunction)

	origJoin := logicalop.LogicalJoin{
		JoinType:        base.InnerJoin,
		EqualConditions: []*expression.ScalarFunction{eqCond},
	}.Init(ctx, 0)
	origJoin.SetChildren(rightLeaf, leftLeaf)
	origJoin.SetSchema(expression.MergeSchema(rightLeaf.Schema(), leftLeaf.Schema()))

	group := &joinGroup{
		root:         origJoin,
		vertexes:     []base.LogicalPlan{rightLeaf, leftLeaf},
		allInnerJoin: true,
	}
	detector := newConflictDetector(ctx)
	nodes, err := detector.Build(group)
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	// Connect (leftLeaf, rightLeaf) in swapped order so alignEQConds injects a LogicalProjection above rightLeaf.
	nodeLeft := nodes[1]
	nodeRight := nodes[0]
	require.Same(t, leftLeaf, nodeLeft.p)
	require.Same(t, rightLeaf, nodeRight.p)

	checkResult, err := detector.CheckConnection(nodeLeft, nodeRight)
	require.NoError(t, err)
	require.True(t, checkResult.Connected())

	planHints := &hint.PlanHints{}
	vertexHints := map[int]*JoinMethodHint{
		rightLeaf.ID(): {
			PreferJoinMethod: hint.PreferINLJ,
			HintInfo:         planHints,
		},
	}

	joinedNode, err := detector.MakeJoin(checkResult, vertexHints)
	require.NoError(t, err)
	require.NotNil(t, joinedNode)

	// MakeJoin must not mutate shared candidate nodes in place.
	require.Same(t, leftLeaf, nodeLeft.p)
	require.Same(t, rightLeaf, nodeRight.p)

	joinedPlan, ok := joinedNode.p.(*logicalop.LogicalJoin)
	require.True(t, ok)
	_, rightWrappedInProj := joinedPlan.Children()[1].(*logicalop.LogicalProjection)
	require.True(t, rightWrappedInProj)
	require.Equal(t, hint.PreferINLJ, joinedPlan.RightPreferJoinType)
	require.NotZero(t, joinedPlan.PreferJoinType&hint.PreferRightAsINLJInner)
	require.Same(t, planHints, joinedPlan.HintInfo)
}

func TestMakeJoinProjectionVertexIsolationAndDedup(t *testing.T) {
	ctx := coretestsdk.MockContext()
	t.Cleanup(func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	})

	intTp := types.NewFieldType(mysql.TypeLonglong)
	col1 := &expression.Column{UniqueID: ctx.GetSessionVars().AllocPlanColumnID(), RetType: intTp}
	baseCol2 := &expression.Column{UniqueID: ctx.GetSessionVars().AllocPlanColumnID(), RetType: intTp}
	projCol2 := &expression.Column{UniqueID: ctx.GetSessionVars().AllocPlanColumnID(), RetType: intTp}

	leftLeaf := logicalop.LogicalTableDual{RowCount: 10}.Init(ctx, 0)
	leftLeaf.SetSchema(expression.NewSchema(col1))

	rightChild := logicalop.LogicalTableDual{RowCount: 10}.Init(ctx, 0)
	rightChild.SetSchema(expression.NewSchema(baseCol2))

	rightProj := logicalop.LogicalProjection{
		Exprs: []expression.Expression{baseCol2},
	}.Init(ctx, 0)
	rightProj.SetSchema(expression.NewSchema(projCol2))
	rightProj.SetOutputNames(types.NameSlice{{
		DBName:  ast.NewCIStr("test"),
		TblName: ast.NewCIStr("dt"),
		ColName: ast.NewCIStr("c2"),
	}})
	rightProj.SetChildren(rightChild)

	plusExpr := expression.NewFunctionInternal(ctx.GetExprCtx(), ast.Plus, intTp, projCol2, expression.NewOne())
	eqCond := expression.NewFunctionInternal(ctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), plusExpr, col1).(*expression.ScalarFunction)

	origJoin := logicalop.LogicalJoin{
		JoinType:        base.InnerJoin,
		EqualConditions: []*expression.ScalarFunction{eqCond},
	}.Init(ctx, 0)
	origJoin.SetChildren(rightProj, leftLeaf)
	origJoin.SetSchema(expression.MergeSchema(rightProj.Schema(), leftLeaf.Schema()))

	group := &joinGroup{
		root:         origJoin,
		vertexes:     []base.LogicalPlan{rightProj, leftLeaf},
		allInnerJoin: true,
	}
	detector := newConflictDetector(ctx)
	nodes, err := detector.Build(group)
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	nodeLeft := nodes[1]
	nodeRight := nodes[0]
	require.Same(t, leftLeaf, nodeLeft.p)
	require.Same(t, rightProj, nodeRight.p)

	checkResult, err := detector.CheckConnection(nodeLeft, nodeRight)
	require.NoError(t, err)
	require.True(t, checkResult.Connected())

	planHints := &hint.PlanHints{}
	vertexHints := map[int]*JoinMethodHint{
		rightProj.ID(): {
			PreferJoinMethod: hint.PreferINLJ,
			HintInfo:         planHints,
		},
	}

	// Simulate DP / multi-start Greedy invoking MakeJoin multiple times on the shared LogicalProjection vertex.
	joined1, err := detector.MakeJoin(checkResult, vertexHints)
	require.NoError(t, err)
	joined2, err := detector.MakeJoin(checkResult, vertexHints)
	require.NoError(t, err)

	// Shared LogicalProjection vertex must not have its Exprs or Schema mutated in place.
	require.Same(t, rightProj, nodeRight.p)
	require.Len(t, rightProj.Exprs, 1)
	require.Equal(t, 1, rightProj.Schema().Len())

	for _, joined := range []*Node{joined1, joined2} {
		joinedPlan, ok := joined.p.(*logicalop.LogicalJoin)
		require.True(t, ok)
		clonedProj, ok := joinedPlan.Children()[1].(*logicalop.LogicalProjection)
		require.True(t, ok)
		require.NotSame(t, rightProj, clonedProj)
		require.Len(t, clonedProj.Exprs, 2)
		require.Equal(t, 2, clonedProj.Schema().Len())
		require.Equal(t, "dt", clonedProj.OutputNames()[0].TblName.L)
		require.Equal(t, hint.PreferINLJ, joinedPlan.RightPreferJoinType)
		require.Same(t, planHints, joinedPlan.HintInfo)
	}

	// When a shared LogicalProjection already projects an equivalent deterministic expression,
	// InjectJoinExpr should reuse that existing column without cloning or appending a duplicate.
	alreadyProjected := cloneLogicalProjection(joined1.p.(*logicalop.LogicalJoin).Children()[1].(*logicalop.LogicalProjection))
	reusedPlan, reusedCol := InjectJoinExpr(alreadyProjected, plusExpr, true, vertexHints)
	require.Same(t, alreadyProjected, reusedPlan)
	require.Equal(t, alreadyProjected.Schema().Columns[1].UniqueID, reusedCol.UniqueID)
	require.Len(t, alreadyProjected.Exprs, 2)
}
