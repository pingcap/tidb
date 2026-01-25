package joinorder

import (
	"cmp"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/hint"
)

type JoinOrder struct {
	ctx   base.PlanContext
	group *joinGroup
}

// A joinGroup is a subtree of the original plan tree. It's the unit for join order.
// root is the root of the subtree, if root is not a join, then the joinGroup only contains one vertex.
// vertexes are the leaf nodes of the subtree, it may have its children, but they are considered as a vertex in this subtree.
type joinGroup struct {
	root base.LogicalPlan
	// All vertexes in this join group.
	// A vertex means a leaf node in this join group tree,
	// it may have its own children, but they are considered as a single unit in this join group.
	vertexes []base.LogicalPlan

	// All leading hints for this join group.
	leadingHints []*hint.PlanHints
	// Join method hints for each vertex in this join group.
	// Key is the planID of the vertex.
	// This is for restore join method hints after join reorder.
	vertexHints map[int]*vertexJoinMethodHint

	// There is no need to check ConflictRules if all joins in this group are inner join.
	// This can speed up the join reorder process.
	allInnerJoin bool
}

func (g *joinGroup) merge(other *joinGroup) {
	g.vertexes = append(g.vertexes, other.vertexes...)
	g.leadingHints = append(g.leadingHints, other.leadingHints...)
	if len(other.vertexHints) > 0 {
		if g.vertexHints == nil {
			g.vertexHints = make(map[int]*vertexJoinMethodHint, len(other.vertexHints))
		}
		maps.Copy(g.vertexHints, other.vertexHints)
	}
	g.allInnerJoin = g.allInnerJoin && other.allInnerJoin
}

type vertexJoinMethodHint struct {
	PreferJoinMethod uint
	HintInfo         *hint.PlanHints
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
				PreferJoinMethod: join.LeftPreferJoinType,
				HintInfo:         join.HintInfo,
			}
			leftHasHint = true
		}
		if join.RightPreferJoinType > uint(0) {
			vertexHints[join.Children()[1].ID()] = &vertexJoinMethodHint{
				PreferJoinMethod: join.RightPreferJoinType,
				HintInfo:         join.HintInfo,
			}
			rightHasHint = true
		}
	}

	resJoinGroup := &joinGroup{
		root:         p,
		vertexes:     []base.LogicalPlan{},
		vertexHints:  vertexHints,
		allInnerJoin: join.JoinType == base.InnerJoin,
	}
	if join.PreferJoinOrder {
		resJoinGroup.leadingHints = []*hint.PlanHints{join.HintInfo}
	}

	var leftJoinGroup, rightJoinGroup *joinGroup
	if !leftHasHint {
		leftJoinGroup = extractJoinGroup(join.Children()[0])
	} else {
		leftJoinGroup = makeSingleGroup(join.Children()[0])
	}
	resJoinGroup.merge(leftJoinGroup)

	if !rightHasHint {
		rightJoinGroup = extractJoinGroup(join.Children()[1])
	} else {
		rightJoinGroup = makeSingleGroup(join.Children()[1])
	}
	resJoinGroup.merge(rightJoinGroup)
	return resJoinGroup
}

func makeSingleGroup(p base.LogicalPlan) *joinGroup {
	return &joinGroup{
		root:         p,
		vertexes:     []base.LogicalPlan{p},
		allInnerJoin: true,
	}
}

func Optimize(p base.LogicalPlan) (base.LogicalPlan, error) {
	return optimizeRecursive(p)
}

func optimizeRecursive(p base.LogicalPlan) (base.LogicalPlan, error) {
	if p == nil {
		return nil, nil
	}
	if _, ok := p.(*logicalop.LogicalCTE); ok {
		return p, nil
	}

	var err error
	joinGroup := extractJoinGroup(p)
	if len(joinGroup.vertexes) <= 0 {
		return nil, errors.Errorf("join group has no vertexes, p: %v", p)
	}

	// Only one vertex, no need to reorder. Only need to optimize its children.
	if len(joinGroup.vertexes) == 1 {
		newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
		for _, child := range p.Children() {
			newChild, err := optimizeRecursive(child)
			if err != nil {
				return nil, err
			}
			newChildren = append(newChildren, newChild)
		}
		p.SetChildren(newChildren...)
		return p, nil
	}

	// Multiple vertexes, starts to reorder.
	vertexMap := make(map[int]base.LogicalPlan, len(joinGroup.vertexes))
	for i, v := range joinGroup.vertexes {
		// Make sure the vertexes are all optimized.
		oldID := v.ID()
		if joinGroup.vertexes[i], err = optimizeRecursive(v); err != nil {
			return nil, err
		}
		vertexMap[oldID] = joinGroup.vertexes[i]
	}
	if len(vertexMap) > 0 {
		joinGroup.root = replaceJoinGroupVertexes(joinGroup.root, vertexMap)
	}
	if p, err = optimizeForJoinGroup(p.SCtx(), joinGroup); err != nil {
		return nil, err
	}
	return p, nil
}

func replaceJoinGroupVertexes(root base.LogicalPlan, vertexMap map[int]base.LogicalPlan) base.LogicalPlan {
	if root == nil {
		return nil
	}
	if replacement, ok := vertexMap[root.ID()]; ok {
		return replacement
	}
	children := root.Children()
	if len(children) == 0 {
		return root
	}
	newChildren := make([]base.LogicalPlan, len(children))
	for i, child := range children {
		newChildren[i] = replaceJoinGroupVertexes(child, vertexMap)
	}
	root.SetChildren(newChildren...)
	return root
}

func optimizeForJoinGroup(ctx base.PlanContext, group *joinGroup) (p base.LogicalPlan, err error) {
	originalSchema := group.root.Schema()

	// gjt todo impl DP
	// useGreedy := len(group.vertexes) > ctx.GetSessionVars().TiDBOptJoinReorderThreshold
	useGreedy := true
	if useGreedy {
		joinOrderGreedy := newJoinOrderGreedy(ctx, group)
		if p, err = joinOrderGreedy.optimize(); err != nil {
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
			Exprs: expression.Column2Exprs(originalSchema.Columns),
		}.Init(p.SCtx(), p.QueryBlockOffset())
		proj.SetSchema(originalSchema)
		proj.SetChildren(p)
		return proj, nil
	}
	return p, nil
}

type joinOrderDP struct {
	JoinOrder
}

func newJoinOrderDP(ctx base.PlanContext, group *joinGroup) *joinOrderDP {
	panic("not implement yet")
}

func (j *joinOrderDP) optimize() (base.LogicalPlan, error) {
	panic("not implement yet")
}

type joinOrderGreedy struct {
	JoinOrder
}

func newJoinOrderGreedy(ctx base.PlanContext, group *joinGroup) *joinOrderGreedy {
	return &joinOrderGreedy{
		JoinOrder: JoinOrder{
			ctx:   ctx,
			group: group,
		},
	}
}

// buildJoinByHint builds a join tree according to the leading hints.
func (j *joinOrderGreedy) buildJoinByHint(detector *ConflictDetector, nodes []*Node) (*Node, []*Node, error) {
	if len(j.group.leadingHints) == 0 {
		return nil, nodes, nil
	}

	leadingHint, hasDifferent := checkAndGenerateLeadingHint(j.group.leadingHints)
	if hasDifferent {
		j.ctx.GetSessionVars().StmtCtx.SetHintWarning(
			"We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid")
	}

	checker := func(leftPlan, rightPlan *Node) (*Node, error) {
		checkResult, err := detector.CheckConnection(leftPlan, rightPlan)
		if err != nil {
			return nil, err
		}
		if !checkResult.Connected() {
			return nil, nil
		}
		return detector.MakeJoin(checkResult, j.group.vertexHints)
	}

	if leadingHint == nil || leadingHint.LeadingList == nil {
		return nil, nodes, nil
	}

	return buildLeadingTreeFromList(j.ctx, leadingHint.LeadingList, nodes, checker)
}

func (j *joinOrderGreedy) optimize() (base.LogicalPlan, error) {
	group := j.group
	detector := newConflictDetector(j.ctx)
	nodes, err := detector.Build(group)
	if err != nil {
		return nil, err
	}
	nodeWithHint, nodes, err := j.buildJoinByHint(detector, nodes)
	if err != nil {
		return nil, err
	}
	if len(nodes) < 1 {
		return nodeWithHint.p, nil
	}

	slices.SortFunc(nodes, func(a, b *Node) int {
		return cmp.Compare(a.cumCost, b.cumCost)
	})

	var curJoinTree *Node
	if nodeWithHint != nil {
		curJoinTree = nodeWithHint
	} else {
		curJoinTree = nodes[0]
		nodes = nodes[1:]
	}
	var bushyJoinTreeNodes []*Node
	var cartesianFactor float64 = j.ctx.GetSessionVars().CartesianJoinOrderThreshold
	var disableCartesian = cartesianFactor <= 0
	for len(nodes) >= 1 {
		var bestNode *Node
		var bestIdx int
		for idx, iterNode := range nodes {
			checkResult, err := detector.CheckConnection(curJoinTree, iterNode)
			if err != nil {
				return nil, err
			}
			if !checkResult.Connected() {
				continue
			}
			newNode, err := detector.MakeJoin(checkResult, j.group.vertexHints)
			if err != nil {
				return nil, err
			}
			if newNode == nil {
				continue
			}
			if checkResult.NoEQEdge() {
				if disableCartesian {
					continue
				}
				newNode.cumCost = newNode.cumCost * cartesianFactor
			}
			if bestNode == nil || newNode.cumCost < bestNode.cumCost {
				bestNode = newNode
				bestIdx = idx
			}
		}
		if bestNode == nil {
			bushyJoinTreeNodes = append(bushyJoinTreeNodes, curJoinTree)
			curJoinTree = nodes[0]
			if len(nodes) >= 1 {
				nodes = nodes[1:]
			}
		} else {
			curJoinTree = bestNode
			nodes = append(nodes[:bestIdx], nodes[bestIdx+1:]...)
		}
	}
	// gjt todo better policy
	usedEdges := make(map[uint64]struct{})
	if curJoinTree != nil && curJoinTree.usedEdges != nil {
		maps.Copy(usedEdges, curJoinTree.usedEdges)
	}
	for _, node := range bushyJoinTreeNodes {
		if node != nil && node.usedEdges != nil {
			maps.Copy(usedEdges, node.usedEdges)
		}
	}
	if !detector.CheckAllEdgesUsed(usedEdges) {
		return group.root, nil
	}
	if len(bushyJoinTreeNodes) > 0 {
		return makeBushyTree(j.ctx, bushyJoinTreeNodes)
	}
	return curJoinTree.p, nil
}

func makeBushyTree(ctx base.PlanContext, cartesianNodes []*Node) (base.LogicalPlan, error) {
	if len(cartesianNodes) <= 0 {
		return nil, nil
	}

	var resNodes []*Node
	for len(cartesianNodes) > 0 {
		for i := 0; i < len(cartesianNodes); i += 2 {
			if i+1 >= len(cartesianNodes) {
				resNodes = append(resNodes, cartesianNodes[i])
				break
			}
			newJoin, err := newCartesianJoin(ctx, base.InnerJoin, cartesianNodes[i].p, cartesianNodes[i+1].p, nil)
			if err != nil {
				return nil, err
			}
			resNodes = append(resNodes, &Node{p: newJoin})
		}
		cartesianNodes = resNodes
		resNodes = resNodes[:0]
	}
	return cartesianNodes[0].p, nil
}

// gjt todo refactor these functions to avoid code duplication.
func checkAndGenerateLeadingHint(hintInfo []*hint.PlanHints) (*hint.PlanHints, bool) {
	leadingHintNum := len(hintInfo)
	var leadingHintInfo *hint.PlanHints
	hasDiffLeadingHint := false
	if leadingHintNum > 0 {
		leadingHintInfo = hintInfo[0]
		// One join group has one leading hint at most. Check whether there are different join order hints.
		for i := 1; i < leadingHintNum; i++ {
			if hintInfo[i] != hintInfo[i-1] {
				hasDiffLeadingHint = true
				break
			}
		}
		if hasDiffLeadingHint {
			leadingHintInfo = nil
		}
	}
	return leadingHintInfo, hasDiffLeadingHint
}

type checkAndMakeJoinFunc func(leftPlan, rightPlan *Node) (*Node, error)

func buildLeadingTreeFromList(
	ctx base.PlanContext, leadingList *ast.LeadingList, availableGroups []*Node, checker checkAndMakeJoinFunc,
) (*Node, []*Node, error) {
	if leadingList == nil || len(leadingList.Items) == 0 {
		return nil, availableGroups, nil
	}

	var (
		currentJoin     *Node
		remainingGroups = availableGroups
		err             error
		ok              bool
	)

	for i, item := range leadingList.Items {
		switch element := item.(type) {
		case *ast.HintTable:
			// find and remove the plan node that matches ast.HintTable from remainingGroups
			var tableNode *Node
			tableNode, remainingGroups, ok = findAndRemovePlanByAstHint(ctx, remainingGroups, element)
			if !ok {
				return nil, availableGroups, nil
			}

			if i == 0 {
				currentJoin = tableNode
			} else {
				currentJoin, err = checker(currentJoin, tableNode)
				if err != nil {
					return nil, availableGroups, err
				}
				if currentJoin == nil {
					return nil, availableGroups, nil
				}
			}
		case *ast.LeadingList:
			// recursively handle nested lists
			var nestedJoin *Node
			nestedJoin, remainingGroups, err = buildLeadingTreeFromList(ctx, element, remainingGroups, checker)
			if err != nil {
				return nil, availableGroups, err
			}
			if nestedJoin == nil {
				return nil, availableGroups, nil
			}

			if i == 0 {
				currentJoin = nestedJoin
			} else {
				currentJoin, err = checker(currentJoin, nestedJoin)
				if err != nil {
					return nil, availableGroups, err
				}
				if currentJoin == nil {
					return nil, availableGroups, nil
				}
			}
		default:
			ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint contains unexpected element type")
			return nil, availableGroups, nil
		}
	}

	return currentJoin, remainingGroups, nil
}

func findAndRemovePlanByAstHint(
	ctx base.PlanContext, plans []*Node, astTbl *ast.HintTable,
) (*Node, []*Node, bool) {
	var queryBlockNames []ast.HintTable
	if p := ctx.GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
		queryBlockNames = *p
	}

	// Step 1: Direct match by table name
	for i, joinGroup := range plans {
		plan := joinGroup.p
		tableAlias := util.ExtractTableAlias(plan, plan.QueryBlockOffset())
		if tableAlias != nil {
			// Match db/table (supports astTbl.DBName == "*")
			dbMatch := astTbl.DBName.L == "" || astTbl.DBName.L == tableAlias.DBName.L || astTbl.DBName.L == "*"
			tableMatch := astTbl.TableName.L == tableAlias.TblName.L

			// Match query block names
			// Use SelectOffset to match query blocks
			qbMatch := true
			if astTbl.QBName.L != "" {
				expectedOffset := extractSelectOffset(astTbl.QBName.L)
				if expectedOffset > 0 {
					qbMatch = tableAlias.SelectOffset == expectedOffset
				} else {
					// If QBName cannot be parsed, ignore the QB match.
					qbMatch = true
				}
			}
			if dbMatch && tableMatch && qbMatch {
				newPlans := append(plans[:i], plans[i+1:]...)
				return joinGroup, newPlans, true
			}
		}
	}

	// Step 2: Match by query-block alias (subquery name)
	// Only execute this step if no direct table name match was found
	groupIdx := -1
	for i, joinGroup := range plans {
		plan := joinGroup.p
		blockOffset := plan.QueryBlockOffset()
		if blockOffset > 1 && blockOffset < len(queryBlockNames) {
			blockName := queryBlockNames[blockOffset]
			dbMatch := astTbl.DBName.L == "" || astTbl.DBName.L == blockName.DBName.L
			tableMatch := astTbl.TableName.L == blockName.TableName.L
			if dbMatch && tableMatch {
				// this can happen when multiple join groups are from the same block, for example:
				//   select /*+ leading(tx) */ * from (select * from t1, t2 ...) tx, ...
				// `tx` is split to 2 join groups `t1` and `t2`, and they have the same block offset.
				// TODO: currently we skip this case for simplification, we can support it in the future.
				if groupIdx != -1 {
					groupIdx = -1
					break
				}
				groupIdx = i
			}
		}
	}

	if groupIdx != -1 {
		matched := plans[groupIdx]
		newPlans := append(plans[:groupIdx], plans[groupIdx+1:]...)
		return matched, newPlans, true
	}

	return nil, plans, false
}

// extract the number x from 'sel_x'
func extractSelectOffset(qbName string) int {
	if strings.HasPrefix(qbName, "sel_") {
		if offset, err := strconv.Atoi(qbName[4:]); err == nil {
			return offset
		}
	}
	return -1
}
