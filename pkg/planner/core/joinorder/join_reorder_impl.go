package joinreorder

import (
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/joinorder/cdc"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

type JoinReorderGreedy struct {
}

func (j *JoinReorderGreedy) build(p base.LogicalPlan) base.LogicalPlan {
	return j.buildForVertex(p)
}

func (j *JoinReorderGreedy) checkIsVertex(p base.LogicalPlan) bool {
	join, isJoin := p.(*logicalop.LogicalJoin)
	if !isJoin {
		return true
	}
	if join.PreferJoinType > uint(0) && p.SCtx().GetSessionVars().EnableAdvancedJoinHint {
		return true
	}
	if join.JoinType != base.InnerJoin && join.JoinType != base.LeftOuterJoin && join.JoinType != base.RightOuterJoin {
		return true
	}
	if slices.ContaionsFunc(join.EqualConditions, func(expr *expression.ScalarFunction) bool {
		return expr.FuncName.L == ast.NullEQ
	}) {
		return true
	}
	return false
}

func (j *JoinReorderGreedy) buildForVertex(p base.LogicalPlan) (base.LogicalPlan, error) {
	detector, err := cdc.NewConflictDetector(p, checker)
	if err != nil {
		return nil, err
	}
	vertexes := detector.GetVertexPlans()
	vertexes.SortByCost()

	for i, v := range vertexes {
		if vertexes[i], err = j.buildForVertex(v.p); err != nil {
			return nil, err
		}
	}

	bushySubTrees := make([]*cdc.Node{}, 0, len(vertexes))

	for len(vertexes) > 0 {
		curTree := vertexes[0]
		vertexes = vertexes[1:]
		for idx, iterNode := range vertexes {
			if detector.CheckConnection(curTree, iterNode) {
				curTree = cdc.NewNode(curTree, iterNode)
				vertexes = append(vertexes[:idx], vertexes[idx+1:]...)
				break
			}
		}
		bushySubTrees = append(bushySubTrees, curTree)
	}

	return makeBushyTree(bushySubTrees)
}
