package joinorder

import (
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/hint"
)

type JoinOrder struct{}

// A joinGroup is a subtree of the original plan tree. It's the unit for join order.
// root is the root of the subtree, if root is not a join, then the joinGroup only contains one vertex.
// vertexes are the leaf nodes of the subtree, it may have its children, but they are considered as a vertex in this subtree.
type joinGroup struct {
	root     base.LogicalPlan
	vertexes []base.LogicalPlan

	// All leading hints for this join group.
	groupHints []*hint.PlanHints
	// Join method hints for each vertex in this join group.
	// Key is the planID of the vertex.
	// This is for restore join method hints after join reorder.
	vertexHints map[int]*vertexJoinMethodHint

	// There is no need to check ConflictRules if all joins in this group are inner join.
	// This can speed up the join reorder process.
	allInnerJoin bool
}

func (g *joinGroup) merge(other *joinGroup) { {
	g.vertexes = append(g.vertexes, other.vertexes...)
	g.groupHints = append(g.groupHints, other.groupHints...)
	maps.Copy(g.vertexHints, other.vertexHints)
	g.allInnerJoin = g.allInnerJoin && other.allInnerJoin
}

type vertexJoinMethodHint struct {
	preferJoinMethod uint
	hintInfo         *hint.PlanHint
}

func extractJoinGroup(p base.LogicalPlan) *joinGroup {
	join, isJoin := p.(*logicalop.LogicalJoin)
	if !isJoin {
		return makeSingleGroup(p)
	}

	if join.StraightJoin {
		return makeSingleGroup(p)
	}

	// For now, we only handle inner join and left/right outer join.
	if join.JoinType != base.InnerJoin && join.JoinType != base.LeftOuterJoin && join.JoinType != base.RightOuterJoin {
		return makeSingleGroup(p)
	}

	if join.PreferJoinType > uint(0) && !p.SCtx().GetSessionVars().EnableAdvancedJoinHint {
		return makeSingleGroup(p)
	}

	if slices.ContainsFunc(join.EqualConditions, func(expr *expression.ScalarFunction) bool {
		return expr.FuncName.L == ast.NullEQ
	}) {
		return makeSingleGroup(p)
	}

	var leftHasHint, rightHasHint bool
	var vertexHints map[int]*vertexJoinMethodHint
	if p.SCtx().GetSessionVars().EnableAdvancedJoinHint && join.PreferJoinType > uint(0) {
		vertexHints = make(map[int]*vertexJoinMethodHint)
		if join.LeftPreferJoinType > uint(0) {
			vertexHints[join.Children()[0].ID()] = &vertexJoinMethodHint{
				preferJoinMethod: join.LeftPreferJoinType,
				hintInfo:         join.HintInfo,
			}
			leftHasHint = true
		}
		if join.RightPreferJoinType > uint(0) {
			vertexHints[join.Children()[1].ID()] = &vertexJoinMethodHint{
				preferJoinMethod: join.RightPreferJoinType,
				hintInfo:         join.HintInfo,
			}
			rightHasHint = true
		}
	}

	resJoinGroup := &joinGroup{
		root:        p,
		vertexes:    []base.LogicalPlan{},
		vertexHints: vertexHints,
		allInnerJoin:  join.JoinType == base.InnerJoin,
	}
	if join.PreferJoinOrder {
		resJoinGroup.groupHints = []*hint.PlanHints{join.HintInfo}
	}
	if !leftHasHint {
		leftJoinGroup := extractGroup(join.Children()[0])
		resJoinGroup.merge(leftJoinGroup)
	}
	if !rightHasHint {
		rightJoinGroup := extractGroup(join.Children()[1])
		resJoinGroup.merge(rightJoinGroup)
	}
	return resJoinGroup
}

func makeSingleGroup(p base.LogicalPlan) *joinGroup {
	return &joinGroup{
		root:     p,
		vertexes: []base.LogicalPlan{p},
	}
}

func (j *JoinOrder) optimizeRecursive(p base.LogicalPlan) (base.LogicalPlan, error) {
	if _, ok := p.(*logicalop.LogicalCTE); ok {
		return p, nil
	}

	var err error
	joinGroup := extractJoinGroup(p)
	if len(joinGroup.vertexes) == 0 {
		return nil, errors.Errorf("join group has no vertexes, p: %v", p)
	}

	// Only one vertex, no need to reorder. Only need to optimize its children.
	if len(joinGroup.vertexes) == 1 {
		newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
		for _, child := range p.Children() {
			newChild, err := j.optimizeRecursive(child)
			if err != nil {
				return nil, err
			}
			newChildren = append(newChildren, newChild)
		}
		p.SetChildren(newChildren...)
		return p, nil
	}

	// Multiple vertexes, starts to reorder.
	for i, v := range joinGroup.vertexes {
		if joinGroup.vertexes[i], err = j.optimizeRecursive(v); err != nil {
			return nil, err
		}
	}
	if p, err = j.optimizeForJoinGroup(joinGroup); err != nil {
		return nil, err
	}
	return p, nil
}

func (j *JoinOrder) optimizeForJoinGroup(ctx base.PlanContext, group *joinGroup) (base.LogicalPlan, error) {
	originalSchema := group.root.Schema()

	var p base.LogicalPlan
	useGreedy := len(group.vertexes) > ctx.GetSessionVars().TiDBOptJoinReorderThreshold
	if useGreedy {
		joinOrderGreedy := newJoinOrderGreedy(ctx, group)
		if p, err = joinOrderGreedy.optimize(); err != nil {}
			return nil, err
		}
	} else {
		joinOrderDP := newJoinOrderDP(ctx, group)
		if p, err = joinOrderDP.optimize(); err != nil {
			return nil, err
		}
	}

	// Ensure the schema is not changed after join reorder.
	if !p.Schema().Equal(originalSchema) {
		proj := logicalop.LogicalProjection{
			Exprs: expression.Columns2Exprs(originalSchema.Columns),
		}.Init(p.SCtx(), p.SelectBlockOffset())
		proj.SetSchema(originalSchema)
		proj.SetChildren(p)
		return proj, nil
	}
	return p, nil
}

type joinOrderGreedy struct {
	ctx   base.PlanContext
	group *joinGroup
}

func newJoinOrderGreedy(ctx base.PlanContext, group *joinGroup) *joinOrderGreedy {
	return &joinOrderGreedy{
		ctx:   ctx,
		group: group,
	}
}

func (j *joinOrderGreedy) buildJoinByHint(nodes []*Node) (*Node, []*Node) {
	// todo implement join order by hint
	return nil, nodes
}

func (j *joinOrderGreedy) optimize(group *joinGroup) (base.LogicalPlan, error) {
	detector := newConflictDetector()
	nodes := detector.Build(group)
	joinTreeWithHint, nodes = j.buildJoinByHint(nodes)

	slices.SortFunc(nodes, func(a, b *node) int {
		if a.GetCost() < b.GetCost() {
			return -1
		} else if a.GetCost() > b.GetCost() {
			return 1
		}
		return 0
	})

	var curJoinTree *Node
	if joinTreeWithHint != nil {
		curJoinTree = joinTreeWithHint
	} else {
		curJoinTree = nodes[0]
		nodes = nodes[1:]
	}
	var bushyJoinTreeNodes []*Node
	for len(nodes) > 1 {
		var nextNode *Node
		for idx, iterNode := range nodes {
			if detector.CheckConnection(curJoinTree, iterNode) {
				nextNode = iterNode
				nodes = append(nodes[:idx], nodes[idx+1:]...)
				break
			}
		}
		if nextNode == nil {
			bushyJoinTreeNodes = append(bushyJoinTreeNodes, curJoinTree)
			curJoinTree = nodes[0]
			nodes = nodes[1:]
		} else {
			curJoinTree = detector.NewNode(curJoinTree, nextNode)
		}
	}
	return makeBushyTree(bushyJoinTreeNodes), nil
}