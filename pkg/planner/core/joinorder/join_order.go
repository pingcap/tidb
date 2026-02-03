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
	"cmp"
	"fmt"
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
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// JoinOrder is the base struct for join order optimization.
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
	vertexHints map[int]*JoinMethodHint

	// There is no need to check ConflictRules if all joins in this group are inner join.
	// This can speed up the join reorder process.
	allInnerJoin bool
}

func (g *joinGroup) merge(other *joinGroup) {
	g.vertexes = append(g.vertexes, other.vertexes...)
	g.leadingHints = append(g.leadingHints, other.leadingHints...)
	if len(other.vertexHints) > 0 {
		if g.vertexHints == nil {
			g.vertexHints = make(map[int]*JoinMethodHint, len(other.vertexHints))
		}
		maps.Copy(g.vertexHints, other.vertexHints)
	}
	g.allInnerJoin = g.allInnerJoin && other.allInnerJoin
}

// JoinMethodHint records the join method hint for a vertex.
// JoinMethodHint stores join method hint info associated with a vertex.
type JoinMethodHint struct {
	PreferJoinMethod uint
	HintInfo         *hint.PlanHints
}

func extractJoinGroup(p base.LogicalPlan) (resJoinGroup *joinGroup) {
	join, isJoin := p.(*logicalop.LogicalJoin)
	if !isJoin {
		return makeSingleGroup(p)
	}

	defer func() {
		if join.PreferJoinOrder {
			resJoinGroup.leadingHints = []*hint.PlanHints{join.HintInfo}
		}
	}()

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

	// Due to the limited search space of the greedy algorithm and our currently rudimentary cost model, suboptimal join orders may occasionally be generated.
	// For example:
	// Original Order: (R1 INNER R2 ON P12) LEFT JOIN (R3 INNER R4 ON P34) ON P23 (Pxy denotes a join condition using Rx and Ry as inputs.)
	//   The LEFT JOIN condition P23 contains only otherCond (non-equi conditions) without any eqCond.
	// Potential Suboptimal Order: R1 INNER (R2 LEFT JOIN (R3 INNER R4 ON P34) ON P23) ON P12
	//   This implies that the edge P23 (lacking an eqCond) is applied earlier than in the original order.
	//   Since edges without equi-conditions perform poorly (as the executor cannot utilize Hash Join),
	//   and the current single-sequence greedy algorithm cannot explore enough alternative sequences, it may return this poor-performing order directly.
	// So We have temporarily disabled reordering for non INNER JOIN that without eqCond.
	// For INNER JOINs, we introduced a penalty factor. If the factor is set less equal to 0,
	// Cartesian products will only be applied at the final step(which will generate a bushy tree).
	if join.JoinType != base.InnerJoin && len(join.EqualConditions) == 0 {
		return makeSingleGroup(p)
	}

	var leftHasHint, rightHasHint bool
	var vertexHints map[int]*JoinMethodHint
	if p.SCtx().GetSessionVars().EnableAdvancedJoinHint && join.PreferJoinType > uint(0) {
		vertexHints = make(map[int]*JoinMethodHint)
		if join.LeftPreferJoinType > uint(0) {
			vertexHints[join.Children()[0].ID()] = &JoinMethodHint{
				PreferJoinMethod: join.LeftPreferJoinType,
				HintInfo:         join.HintInfo,
			}
			leftHasHint = true
		}
		if join.RightPreferJoinType > uint(0) {
			vertexHints[join.Children()[1].ID()] = &JoinMethodHint{
				PreferJoinMethod: join.RightPreferJoinType,
				HintInfo:         join.HintInfo,
			}
			rightHasHint = true
		}
	}

	resJoinGroup = &joinGroup{
		root:         p,
		vertexes:     []base.LogicalPlan{},
		vertexHints:  vertexHints,
		allInnerJoin: join.JoinType == base.InnerJoin,
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

// Optimize performs join order optimization on the given plan.
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

		if len(joinGroup.leadingHints) > 0 {
			p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable, check the join type or the join algorithm hint")
		}
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

	// TODO impl DP
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
		proj.SetSchema(originalSchema.Clone())
		proj.SetChildren(p)
		return proj, nil
	}
	return p, nil
}

type joinOrderDP struct {
	JoinOrder
}

func newJoinOrderDP(_ base.PlanContext, _ *joinGroup) *joinOrderDP {
	panic("not implement yet")
}

func (*joinOrderDP) optimize() (base.LogicalPlan, error) {
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

	if leadingHint == nil || leadingHint.LeadingList == nil {
		return nil, nodes, nil
	}

	findAndRemoveByHint := func(available []*Node, hint *ast.HintTable) (*Node, []*Node, bool) {
		return FindAndRemovePlanByAstHint(j.ctx, available, hint, func(node *Node) base.LogicalPlan {
			return node.p
		})
	}
	joiner := func(left, right *Node) (*Node, bool, error) {
		_, newNode, err := checkConnectionAndMakeJoin(detector, left, right, j.group.vertexHints, true)
		if err != nil {
			return nil, false, err
		}
		if newNode == nil {
			return nil, false, nil
		}
		return newNode, true, nil
	}
	warn := func() {
		j.ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint contains unexpected element type")
	}

	nodeWithHint, nodes, ok, err := BuildLeadingTreeFromList(leadingHint.LeadingList, nodes, findAndRemoveByHint, joiner, warn)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		j.ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable, check if the leading hint table is valid")
		return nil, nodes, nil
	}
	return nodeWithHint, nodes, nil
}

func checkConnection(detector *ConflictDetector, leftPlan, rightPlan *Node) (*CheckConnectionResult, error) {
	checkResult, err := detector.CheckConnection(leftPlan, rightPlan)
	if err != nil {
		return nil, err
	}
	if checkResult.Connected() {
		return checkResult, nil
	}
	checkResult, err = detector.CheckConnection(rightPlan, leftPlan)
	if err != nil {
		return nil, err
	}
	return checkResult, nil
}

func checkConnectionAndMakeJoin(detector *ConflictDetector, leftPlan, rightPlan *Node, vertexHints map[int]*JoinMethodHint, allowNoEQ bool) (*CheckConnectionResult, *Node, error) {
	checkResult, err := checkConnection(detector, leftPlan, rightPlan)
	if err != nil {
		return nil, nil, err
	}
	if !checkResult.Connected() {
		if !allowNoEQ {
			return nil, nil, nil
		}
		// TODO duplicated with makeBushyTree?
		if checkResult = detector.TryCreateCartesianCheckResult(leftPlan, rightPlan); checkResult == nil {
			return nil, nil, nil
		}
	}
	newNode, err := detector.MakeJoin(checkResult, vertexHints)
	if err != nil {
		return nil, nil, err
	}
	return checkResult, newNode, nil
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

	if nodeWithHint != nil {
		newNodes := make([]*Node, 0, len(nodes)+1)
		newNodes = append(newNodes, nodeWithHint)
		newNodes = append(newNodes, nodes...)
		nodes = newNodes
	}

	var cartesianFactor float64 = j.ctx.GetSessionVars().CartesianJoinOrderThreshold
	var disableCartesian = cartesianFactor <= 0
	allowNoEQ := !disableCartesian && j.group.allInnerJoin
	nodes, err = greedyConnectJoinNodes(detector, nodes, j.group.vertexHints, cartesianFactor, allowNoEQ)
	if err != nil {
		return nil, err
	}
	nodes, usedEdges, err := tryApplyAllRemainingEdges(detector, nodes, j.group.vertexHints, cartesianFactor, allowNoEQ)
	if err != nil {
		return nil, err
	}
	if !detector.CheckAllEdgesUsed(usedEdges) {
		totalEdges, usedEdgeCount, missingEdges, missingDetail, nodeSets := summarizeEdges(detector, usedEdges, nodes, 4)
		logutil.BgLogger().Warn("join reorder skipped because not all edges are used",
			zap.Int("rootID", group.root.ID()),
			zap.Int("nodes", len(nodes)),
			zap.Int("totalEdges", totalEdges),
			zap.Int("usedEdges", usedEdgeCount),
			zap.Int("missingEdges", missingEdges),
			zap.String("missingDetail", missingDetail),
			zap.String("nodeSets", nodeSets),
			zap.Bool("allInnerJoin", group.allInnerJoin))
		return group.root, nil
	}
	if len(nodes) <= 0 {
		return nil, errors.New("internal error: bushy join tree nodes is empty")
	}
	return makeBushyTree(j.ctx, nodes, j.group.vertexHints)
}

func greedyConnectJoinNodes(detector *ConflictDetector, nodes []*Node, vertexHints map[int]*JoinMethodHint, cartesianFactor float64, allowNoEQ bool) ([]*Node, error) {
	var curJoinIdx int
	for curJoinIdx < len(nodes)-1 {
		var bestNode *Node
		var bestIdx int
		curJoinTree := nodes[curJoinIdx]
		for iterIdx := curJoinIdx + 1; iterIdx < len(nodes); iterIdx++ {
			iterNode := nodes[iterIdx]
			checkResult, newNode, err := checkConnectionAndMakeJoin(detector, curJoinTree, iterNode, vertexHints, allowNoEQ)
			if err != nil {
				return nil, err
			}
			if newNode == nil {
				continue
			}
			if checkResult.NoEQEdge() {
				// The original plan tree may have cartesian edges, to avoid cartesian join happens first,
				// we need the check here.
				if !allowNoEQ {
					continue
				}
				// TODO: Non INNER JOIN without eqCond is not supported for now.
				// For INNER JOIN, if cartesianFactor > 0, we apply a penalty to the cost of the newNode,
				// and we might generate a tree with cartesian edge.
				// For non INNER JOIN, the logic in extractJoinGroup ensures we will not reach here,
				// check the comment in extractJoinGroup for more details.
				newNode.cumCost = newNode.cumCost * cartesianFactor
			}
			if bestNode == nil || newNode.cumCost < bestNode.cumCost {
				bestNode = newNode
				bestIdx = iterIdx
			}
		}
		if bestNode == nil {
			curJoinIdx++
		} else {
			nodes[curJoinIdx] = bestNode
			nodes = append(nodes[:bestIdx], nodes[bestIdx+1:]...)
		}
	}
	return nodes, nil
}

// TODO add example
func tryApplyAllRemainingEdges(detector *ConflictDetector, nodes []*Node, vertexHints map[int]*JoinMethodHint, cartesianFactor float64, allowNoEQ bool) ([]*Node, map[uint64]struct{}, error) {
	usedEdges := collectUsedEdges(nodes)
	// If all edges are used, return directly.
	if detector.CheckAllEdgesUsed(usedEdges) {
		return nodes, usedEdges, nil
	}
	// If there is only one node, return directly.
	if len(nodes) < 2 {
		return nodes, usedEdges, nil
	}

	slices.SortFunc(nodes, func(a, b *Node) int {
		return cmp.Compare(a.cumCost, b.cumCost)
	})

	nodes, err := greedyConnectJoinNodes(detector, nodes, vertexHints, cartesianFactor, allowNoEQ)
	if err != nil {
		return nil, nil, err
	}
	// Need to recompute usedEdges.
	usedEdges = collectUsedEdges(nodes)
	return nodes, usedEdges, nil
}

func collectUsedEdges(nodes []*Node) map[uint64]struct{} {
	usedEdges := make(map[uint64]struct{})
	for _, node := range nodes {
		if node != nil && node.usedEdges != nil {
			maps.Copy(usedEdges, node.usedEdges)
		}
	}
	return usedEdges
}

func summarizeEdges(detector *ConflictDetector, usedEdges map[uint64]struct{}, nodes []*Node, limit int) (total, used, missing int, detail, nodeSets string) {
	if usedEdges == nil {
		usedEdges = make(map[uint64]struct{})
	}
	addEdge := func(e *edge, missingList *[]string) {
		if len(e.eqConds) == 0 && len(e.nonEQConds) == 0 {
			return
		}
		total++
		if _, ok := usedEdges[e.idx]; ok {
			used++
			return
		}
		missing++
		if len(*missingList) < limit {
			*missingList = append(*missingList, fmt.Sprintf("{idx:%d type:%v eq:%d nonEq:%d tes:%v left:%v right:%v}",
				e.idx, e.joinType, len(e.eqConds), len(e.nonEQConds), e.tes.String(), e.leftVertexes.String(), e.rightVertexes.String()))
		}
	}

	var missingList []string
	for _, e := range detector.innerEdges {
		addEdge(e, &missingList)
	}
	for _, e := range detector.nonInnerEdges {
		addEdge(e, &missingList)
	}
	if missing > limit {
		missingList = append(missingList, fmt.Sprintf("...(+%d more)", missing-limit))
	}
	detail = strings.Join(missingList, ", ")

	if len(nodes) > 0 {
		nodeBits := make([]string, 0, len(nodes))
		for _, n := range nodes {
			if n == nil {
				continue
			}
			nodeBits = append(nodeBits, n.bitSet.String())
		}
		nodeSets = strings.Join(nodeBits, ",")
	}
	return total, used, missing, detail, nodeSets
}

func makeBushyTree(ctx base.PlanContext, cartesianNodes []*Node, vertexHints map[int]*JoinMethodHint) (base.LogicalPlan, error) {
	var iterNodes []*Node
	for len(cartesianNodes) > 1 {
		for i := 0; i < len(cartesianNodes); i += 2 {
			if i+1 >= len(cartesianNodes) {
				iterNodes = append(iterNodes, cartesianNodes[i])
				break
			}
			newJoin, err := newCartesianJoin(ctx, base.InnerJoin, cartesianNodes[i].p, cartesianNodes[i+1].p, vertexHints)
			if err != nil {
				return nil, err
			}
			iterNodes = append(iterNodes, &Node{p: newJoin})
		}
		cartesianNodes = iterNodes
		iterNodes = iterNodes[:0]
	}
	return cartesianNodes[0].p, nil
}

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

// LeadingTreeFinder finds a node by hint and removes it from the available slice.
type LeadingTreeFinder[T any] func(available []T, hint *ast.HintTable) (T, []T, bool)

// LeadingTreeJoiner joins two nodes in the leading tree.
type LeadingTreeJoiner[T any] func(left, right T) (T, bool, error)

// BuildLeadingTreeFromList constructs a join tree according to a LeadingList.
func BuildLeadingTreeFromList[T any](
	leadingList *ast.LeadingList,
	availableGroups []T,
	findAndRemoveByHint LeadingTreeFinder[T],
	checkAndJoin LeadingTreeJoiner[T],
	warn func(),
) (T, []T, bool, error) {
	var zero T
	if leadingList == nil || len(leadingList.Items) == 0 {
		return zero, availableGroups, false, nil
	}

	var (
		currentJoin T
		err         error
		ok          bool
	)
	// copy here because findAndRemoveByHint will modify the slice.
	remainingGroups := make([]T, len(availableGroups))
	copy(remainingGroups, availableGroups)

	for i, item := range leadingList.Items {
		switch element := item.(type) {
		case *ast.HintTable:
			var tableNode T
			tableNode, remainingGroups, ok = findAndRemoveByHint(remainingGroups, element)
			if !ok {
				return zero, availableGroups, false, nil
			}

			if i == 0 {
				currentJoin = tableNode
			} else {
				currentJoin, ok, err = checkAndJoin(currentJoin, tableNode)
				if err != nil {
					return zero, availableGroups, false, err
				}
				if !ok {
					return zero, availableGroups, false, nil
				}
			}
		case *ast.LeadingList:
			var nestedJoin T
			nestedJoin, remainingGroups, ok, err = BuildLeadingTreeFromList(element, remainingGroups, findAndRemoveByHint, checkAndJoin, warn)
			if err != nil {
				return zero, availableGroups, false, err
			}
			if !ok {
				return zero, availableGroups, false, nil
			}

			if i == 0 {
				currentJoin = nestedJoin
			} else {
				currentJoin, ok, err = checkAndJoin(currentJoin, nestedJoin)
				if err != nil {
					return zero, availableGroups, false, err
				}
				if !ok {
					return zero, availableGroups, false, nil
				}
			}
		default:
			if warn != nil {
				warn()
			}
			return zero, availableGroups, false, nil
		}
	}

	return currentJoin, remainingGroups, true, nil
}

// FindAndRemovePlanByAstHint matches a hint table to a plan and removes it from the slice.
// T is usually be *Node or base.LogicalPlan, we use generics because we want to reuse this function in both the old and new join order code.
func FindAndRemovePlanByAstHint[T any](
	ctx base.PlanContext,
	plans []T,
	astTbl *ast.HintTable,
	getPlan func(T) base.LogicalPlan,
) (T, []T, bool) {
	var zero T
	var queryBlockNames []ast.HintTable
	if p := ctx.GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
		queryBlockNames = *p
	}

	// Step 1: Direct match by table name
	for i, joinGroup := range plans {
		plan := getPlan(joinGroup)
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
		plan := getPlan(joinGroup)
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

	return zero, plans, false
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
