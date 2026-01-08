package joinorder

import (
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

type JoinOrder struct{}

// A joinGroup is a subtree of the original plan tree. It's the unit for join order.
// root is the root of the subtree, if root is not a join, then the joinGroup only contains one vertex.
// vertexes are the leaf nodes of the subtree, it may have its children, but they are considered as a vertex in this subtree.
type joinGroup struct {
	root     base.LogicalPlan
	vertexes []base.LogicalPlan
}

func (j *JoinOrder) extractGroup(p base.LogicalPlan) *joinGroup {
	join, isJoin := p.(*logicalop.LogicalJoin)
	if !isJoin {
		return makeSingleGroup(p)
	}

	// For now, we only handle inner join and left/right outer join.
	if join.JoinType != base.InnerJoin && join.JoinType != base.LeftOuterJoin && join.JoinType != base.RightOuterJoin {
		return makeSingleGroup(p)
	}

}

func makeSingleGroup(p base.LogicalPlan) *joinGroup {
	return &joinGroup{
		root:     p,
		vertexes: []base.LogicalPlan{p},
	}
}
