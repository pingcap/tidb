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
	"math"
	"math/bits"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/intset"
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
	// Whether this join group contains any user-provided LEADING hint. Internal
	// ordered-leading preferences reuse the same builder path but should not emit
	// user-facing hint warnings.
	hasUserLeadingHint bool
	// Join method hints for each vertex in this join group.
	// Key is the planID of the vertex.
	// This is for restore join method hints after join reorder.
	vertexHints map[int]*JoinMethodHint

	// There is no need to check ConflictRules if all joins in this group are inner join.
	// This can speed up the join reorder process.
	allInnerJoin bool

	// selConds holds filter conditions collected from Selection operators
	// that were looked through during extractJoinGroup.
	selConds map[int][]expression.Expression
}

func (g *joinGroup) merge(other *joinGroup) {
	g.vertexes = append(g.vertexes, other.vertexes...)
	g.leadingHints = append(g.leadingHints, other.leadingHints...)
	g.hasUserLeadingHint = g.hasUserLeadingHint || other.hasUserLeadingHint
	if len(other.vertexHints) > 0 {
		if g.vertexHints == nil {
			g.vertexHints = make(map[int]*JoinMethodHint, len(other.vertexHints))
		}
		maps.Copy(g.vertexHints, other.vertexHints)
	}
	g.allInnerJoin = g.allInnerJoin && other.allInnerJoin

	if len(other.selConds) > 0 {
		if g.selConds == nil {
			g.selConds = make(map[int][]expression.Expression, len(other.selConds))
		}
		maps.Copy(g.selConds, other.selConds)
	}
}

func extractJoinGroup(p base.LogicalPlan) (resJoinGroup *joinGroup) {
	if sel, isSel := p.(*logicalop.LogicalSelection); isSel {
		if p.SCtx().GetSessionVars().TiDBOptJoinReorderThroughSel &&
			!slices.ContainsFunc(sel.Conditions, expression.IsMutableEffectsExpr) {
			childGroup := extractJoinGroup(sel.Children()[0])
			// This check is necessary: the child JoinGroup must contain at least one join operator.
			// If a table outside the Selection subtree needs to be reordered with tables inside it,
			// the connectivity must be verified through CR. Since the CR of Selection-derived edge will not be generated,
			// so we need rely on CRs of joins in Selection's subtree.
			if len(childGroup.vertexes) > 1 {
				if childGroup.selConds == nil {
					childGroup.selConds = make(map[int][]expression.Expression)
				}
				childGroup.selConds[sel.ID()] = sel.Conditions
				childGroup.root = sel
				return childGroup
			}
		}
		return makeSingleGroup(p)
	}

	join, isJoin := p.(*logicalop.LogicalJoin)
	if !isJoin {
		return makeSingleGroup(p)
	}

	var curLeadingHint *hint.PlanHints
	var curLeadingHintFromUser bool
	if join.PreferJoinOrder {
		curLeadingHint = join.HintInfo
		curLeadingHintFromUser = true
	} else if join.InternalPreferJoinOrder {
		curLeadingHint = join.InternalHintInfo
	}
	defer func() {
		if curLeadingHint != nil {
			resJoinGroup.leadingHints = append(resJoinGroup.leadingHints, curLeadingHint)
			resJoinGroup.hasUserLeadingHint = resJoinGroup.hasUserLeadingHint || curLeadingHintFromUser
		}
	}()

	if join.StraightJoin {
		return makeSingleGroup(p)
	}

	// For now, we only handle inner join and left/right outer join.
	if join.JoinType != base.InnerJoin && join.JoinType != base.LeftOuterJoin && join.JoinType != base.RightOuterJoin {
		return makeSingleGroup(p)
	}

	if !p.SCtx().GetSessionVars().EnableOuterJoinReorder && (join.JoinType == base.LeftOuterJoin || join.JoinType == base.RightOuterJoin) {
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
	//
	// Also we allow both assoc(left, left) and assoc(right, right) without considering null-rejective property,
	// Because for NON-INNER JOIN with eqCond, it must be null-rejective on both sides.
	// If we support reorder NON-INNER JOIN without eqCond in the future, we need to consider null-rejective property here.
	// See assocRuleTable in conflict_detector.go for more details.
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

	leftShouldPreserve := curLeadingHint != nil && IsDerivedTableInLeadingHint(join.Children()[0], curLeadingHint)
	var leftJoinGroup, rightJoinGroup *joinGroup
	if !leftHasHint && !leftShouldPreserve {
		leftJoinGroup = extractJoinGroup(join.Children()[0])
	} else {
		leftJoinGroup = makeSingleGroup(join.Children()[0])
	}
	resJoinGroup.merge(leftJoinGroup)

	rightShouldPreserve := curLeadingHint != nil && IsDerivedTableInLeadingHint(join.Children()[1], curLeadingHint)
	if !rightHasHint && !rightShouldPreserve {
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

		if joinGroup.hasUserLeadingHint && len(joinGroup.leadingHints) > 0 {
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

// replaceJoinGroupVertexes walks the join-group subtree rooted at `root` and
// swaps every leaf vertex with its optimized replacement from vertexMap.
//
// Why this is needed: each vertex in the join group is recursively optimized
// (optimizeRecursive) before join reorder runs. That optimisation may rebuild
// the plan node (new ID, new children), so the original plan tree still points
// to the stale, pre-optimisation nodes. This function patches the tree so that
// the ConflictDetector.Build(), which traverses from root down to locate
// vertexes by plan ID, sees the up-to-date nodes.
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

	// Use DP for any join group under the configured threshold. ConflictDetector
	// is responsible for validating both inner and non-inner join transitions.
	useGreedy := len(group.vertexes) > ctx.GetSessionVars().TiDBOptJoinReorderThreshold
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

func newJoinOrderDP(ctx base.PlanContext, group *joinGroup) *joinOrderDP {
	return &joinOrderDP{
		JoinOrder: JoinOrder{
			ctx:   ctx,
			group: group,
		},
	}
}

func (j *joinOrderDP) optimize() (base.LogicalPlan, error) {
	if len(j.group.leadingHints) > 0 {
		// TODO: Old join reorder doesn't support leading hint either,
		// we can consider supporting leading hint in DP join reorder in the future, but for now we just return a warning.
		j.ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable for the DP join reorder algorithm")
	}

	detector := newConflictDetector(j.ctx)
	nodes, err := detector.Build(j.group)
	if err != nil {
		return nil, err
	}

	plan, ok, err := j.optimizeWithDetector(detector, nodes)
	if err != nil {
		return nil, err
	}
	if !ok {
		j.ctx.GetSessionVars().StmtCtx.SetHintWarning("no valid join order found, the original join order will be used")
		return j.group.root, nil
	}
	return plan, nil
}

func (j *joinOrderDP) optimizeWithDetector(detector *ConflictDetector, nodes []*Node) (base.LogicalPlan, bool, error) {
	if len(nodes) == 0 {
		return nil, false, errors.New("internal error: join group has no nodes")
	}
	if len(nodes) == 1 {
		return nodes[0].p, true, nil
	}

	nodeCount := len(nodes)
	if nodeCount >= 63 {
		// Sanity check: TiDBOptJoinReorderThreshold should prevent this from happening, but we check it here just in case.
		// And 63 for TiDBOptJoinReorderThreshold is too large, we need to decrease it later.
		return nil, false, errors.Errorf("DP join reorder supports at most 62 nodes, got %d", nodeCount)
	}

	bestPlan := make([]*Node, 1<<uint(nodeCount))
	for _, node := range nodes {
		// NOTE: Because hints are currently not effective in DP, the node here must be a single leaf vertex(i.e., only one bit is set to 1 and the rest are 0).
		// Therefore, we can directly use node.bitSet as the mask. If hint support is added in the future, this part must be updated accordingly.
		mask, err := node.bitSet.GetSmallUInt64()
		if err != nil {
			return nil, false, err
		}
		bestPlan[mask] = node
	}

	cartesianFactor := j.ctx.GetSessionVars().CartesianJoinOrderThreshold
	fullMask := (uint64(1) << uint(nodeCount)) - 1
	for subset := uint64(1); subset <= fullMask; subset++ {
		if bits.OnesCount64(subset) == 1 {
			continue
		}
		for left := (subset - 1) & subset; left > 0; left = (left - 1) & subset {
			right := subset ^ left
			if left > right {
				// We only need to consider one direction of the partition (left, right) and skip the other (right, left) to avoid duplicate work,
				// because the join order (A join B) and (B join A) will be considered in the same iteration when left and right are swapped,
				// check ConflictDetector.CheckConnection() for more details.
				continue
			}
			leftPlan := bestPlan[left]
			rightPlan := bestPlan[right]
			if leftPlan == nil || rightPlan == nil {
				// leftPlan or rightPlan will be nil if there is no valid join order for the corresponding subset,
				// for example, when the subset contains two nodes but they cannot be joined together.
				continue
			}

			checkResult, newNode, err := checkConnectionAndMakeJoin(detector, leftPlan, rightPlan, j.group.vertexHints, true)
			if err != nil {
				return nil, false, err
			}
			if newNode == nil {
				continue
			}
			if checkResult.NoEQEdge() {
				newNode.cumCost, err = applyCartesianFactor(newNode.cumCost, cartesianFactor)
				if err != nil {
					return nil, false, err
				}
			}
			if bestPlan[subset] == nil || newNode.cumCost < bestPlan[subset].cumCost {
				bestPlan[subset] = newNode
			}
		}
	}

	finalPlan := bestPlan[fullMask]
	// Example: for (t1 ⋈ t2), (t3 ⋈ t4) with cartesianFactor <= 0, every full plan
	// needs one cartesian edge and therefore has +Inf cost. Returning the first
	// non-nil fullMask plan directly could keep a shape like (t1 × (t3 ⋈ t4)) ⋈ t2.
	// We only return finite full plans here; +Inf full plans go through
	// buildBushyTreeFromDP() so cartesian joins are pushed to the final stitch.
	if finalPlan != nil && !math.IsInf(finalPlan.cumCost, 1) && !detector.HasRemainingEdges(finalPlan.usedEdges) {
		return finalPlan.p, true, nil
	}

	bushyPlan, err := buildBushyTreeFromDP(j.ctx, detector, nodes, bestPlan, j.group.vertexHints)
	if err != nil {
		return nil, false, err
	}
	if bushyPlan != nil && !detector.HasRemainingEdges(bushyPlan.usedEdges) {
		return bushyPlan.p, true, nil
	}

	if finalPlan != nil && math.IsInf(finalPlan.cumCost, 1) && !detector.HasRemainingEdges(finalPlan.usedEdges) {
		return finalPlan.p, true, nil
	}
	return nil, false, nil
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

	leadingHint, hasDifferent := CheckAndGenerateLeadingHint(j.group.leadingHints)
	if hasDifferent && j.group.hasUserLeadingHint {
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
		if j.group.hasUserLeadingHint {
			j.ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint contains unexpected element type")
		}
	}

	// BuildLeadingTreeFromList may modify nodes slice, so we need to clone it first.
	// And only return the modified nodes(nodesAfterHint) when the leading hint is applicable,
	// and original nodes slice will be returned when the leading hint is inapplicable.
	nodeWithHint, nodesAfterHint, ok, err := BuildLeadingTreeFromList(leadingHint.LeadingList, slices.Clone(nodes), findAndRemoveByHint, joiner, warn)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		if j.group.hasUserLeadingHint {
			j.ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable, check if the leading hint table is valid")
		}
		return nil, nodes, nil
	}
	return nodeWithHint, nodesAfterHint, nil
}

func checkConnectionAndMakeJoin(detector *ConflictDetector, leftPlan, rightPlan *Node, vertexHints map[int]*JoinMethodHint, allowNoEQ bool) (*CheckConnectionResult, *Node, error) {
	checkResult, err := detector.CheckConnection(leftPlan, rightPlan)
	if err != nil {
		return nil, nil, err
	}
	if !checkResult.Connected() {
		if !allowNoEQ {
			return nil, nil, nil
		}
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
	if nodes, err = greedyConnectJoinNodes(detector, nodes, j.group.vertexHints, cartesianFactor, allowNoEQ); err != nil {
		return nil, err
	}

	usedEdges := collectUsedEdges(nodes)
	if !allowNoEQ && detector.HasRemainingEdges(usedEdges) {
		// After the first round of greedy connection, there are still some remaining edges,
		// for example: R1 INNER JOIN R2 ON R1.c1 < R2.c2
		// the above join can only be connected when non-eq edges are allowed,
		// and the first round of greedy enumeration is not allowed to use non-eq edges.
		// So we got here and we need to the second round of enumeration with `allowNoEQ` as true.
		befLen := len(nodes)
		// Clamp to 1 to avoid cumCost*0=0 making non-EQ joins appear free.
		if cartesianFactor <= 0 {
			cartesianFactor = 1
		}
		if nodes, err = greedyConnectJoinNodes(detector, nodes, j.group.vertexHints, cartesianFactor, true); err != nil {
			return nil, err
		}
		if len(nodes) != befLen {
			// Only collect usedEdges when new joins are made in the second round.
			usedEdges = collectUsedEdges(nodes)
		}
	}
	if detector.HasRemainingEdges(usedEdges) {
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
		if intest.InTest {
			return nil, errors.New("got remaining edges during join reorder")
		}
		return group.root, nil
	}
	if len(nodes) <= 0 {
		return nil, errors.New("internal error: bushy join tree nodes is empty")
	}
	// makeBushyTree connects the remaining nodes into a bushy tree using cartesian joins,
	// It handles situations where there is no edges between different subgraphs,
	root, err := makeBushyTree(j.ctx, detector, nodes, j.group.vertexHints, true)
	if err != nil {
		return nil, err
	}
	return root.p, nil
}

func greedyConnectJoinNodes(detector *ConflictDetector, nodes []*Node, vertexHints map[int]*JoinMethodHint, cartesianFactor float64, allowNoEQ bool) ([]*Node, error) {
	// Outer loop: keep trying while we have multiple nodes and made progress in the last iteration.
	// This handles cases where conflict rules block some joins until other joins are completed.
	for len(nodes) > 1 {
		madeProgress := false
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
					newNode.cumCost, err = applyCartesianFactor(newNode.cumCost, cartesianFactor)
					if err != nil {
						return nil, err
					}
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
				madeProgress = true
			}
		}
		// If no progress was made in this iteration, we cannot connect any more nodes.
		if !madeProgress {
			break
		}
	}
	return nodes, nil
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

func applyCartesianFactor(cost, cartesianFactor float64) (float64, error) {
	if err := validateCumCost(cost); err != nil {
		return 0, err
	}
	if cartesianFactor <= 0 {
		return math.Inf(1), nil
	}
	if math.IsNaN(cartesianFactor) || math.IsInf(cartesianFactor, 0) {
		return 0, errors.Errorf("invalid cartesian factor: %v", cartesianFactor)
	}
	adjustedCost := cost * cartesianFactor
	if err := validateCumCost(adjustedCost); err != nil {
		return 0, err
	}
	return adjustedCost, nil
}

type dpSubsetCandidate struct {
	mask uint64
	node *Node
}

// buildBushyTreeFromDP reconstructs a valid forest from the DP table and then
// stitches that forest into the final bushy tree.
//
// Why this is needed:
//   - The single DP pass may fail to produce a finite full-mask plan even though
//     some large subsets were optimized successfully.
//   - bestPlan contains many overlapping subset candidates, and not every
//     candidate is safe to reuse as a final subtree. In particular, a subset may
//     exist only because cartesian/no-EQ joins were introduced early, while that
//     subset still has real edges inside it that were never consumed.
//
// The reconstruction therefore does three things:
//  1. Keep only finite, subset-complete candidates (no remaining real edges
//     whose TES is fully inside the subset).
//  2. Greedily pick a disjoint set of the largest/cheapest candidates to form
//     forest roots.
//  3. Add any uncovered leaf back into the forest so every base relation is
//     represented before the final bushy-tree stitch.
func buildBushyTreeFromDP(ctx base.PlanContext, detector *ConflictDetector, leaves []*Node, bestPlan []*Node, vertexHints map[int]*JoinMethodHint) (*Node, error) {
	candidates := make([]dpSubsetCandidate, 0, len(bestPlan))
	for mask, node := range bestPlan {
		if node == nil || math.IsInf(node.cumCost, 1) {
			continue
		}
		if detector.HasRemainingEdgesInSubset(node.bitSet, node.usedEdges) {
			continue
		}
		candidates = append(candidates, dpSubsetCandidate{
			mask: uint64(mask),
			node: node,
		})
	}
	slices.SortFunc(candidates, func(a, b dpSubsetCandidate) int {
		if cmpVal := cmp.Compare(bits.OnesCount64(b.mask), bits.OnesCount64(a.mask)); cmpVal != 0 {
			return cmpVal
		}
		if cmpVal := cmp.Compare(a.node.cumCost, b.node.cumCost); cmpVal != 0 {
			return cmpVal
		}
		return cmp.Compare(a.mask, b.mask)
	})

	forest := make([]*Node, 0, len(candidates))
	var covered intset.FastIntSet
	for _, candidate := range candidates {
		if candidate.node.bitSet.Intersects(covered) {
			continue
		}
		forest = append(forest, candidate.node)
		covered.UnionWith(candidate.node.bitSet)
	}
	for _, leaf := range leaves {
		if leaf.bitSet.Intersects(covered) {
			continue
		}
		forest = append(forest, leaf)
		covered.UnionWith(leaf.bitSet)
	}
	if len(forest) == 0 {
		return nil, nil
	}
	if len(forest) == 1 {
		return forest[0], nil
	}
	return makeBushyTree(ctx, detector, forest, vertexHints, false)
}

// makeJoinWithDetector is for the final bushy-tree stitching stage.
//
// We keep it separate from checkConnectionAndMakeJoin() because that helper is
// designed for search-time candidate enumeration, where nil means "skip this
// candidate". Here we are no longer searching: we must connect the remaining
// groups deterministically, preferring real edges and otherwise creating an
// explicit cartesian edge through ConflictDetector.
func makeJoinWithDetector(detector *ConflictDetector, left, right *Node, vertexHints map[int]*JoinMethodHint) (*Node, error) {
	checkResult, err := detector.CheckConnection(left, right)
	if err != nil {
		return nil, err
	}
	if !checkResult.Connected() {
		checkResult = detector.TryCreateCartesianCheckResult(left, right)
		if checkResult == nil {
			return nil, errors.New("failed to construct bushy tree: no valid join edge found")
		}
	}

	return detector.MakeJoin(checkResult, vertexHints)
}

// makeBushyTree connects the remaining nodes into a bushy tree.
//
// `fastPath` controls how each pairwise merge is performed:
//   - true: build a pure cartesian join directly with newCartesianJoin(),
//     without consulting the ConflictDetector. In this mode, only Node.p is
//     valid; the other fields are not initialized. This should be used only
//     in the final step of bushy-tree construction.
//   - false: use makeJoinWithDetector(), which prefers a real edge selected
//     through the ConflictDetector and falls back to an explicit cartesian
//     edge only if no real edge exists. This path returns a fully initialized
//     *Node.
//
// NOTE: The `true` branch currently serves only the greedy algorithm's final
// stitching step, where the caller reads only root.p. It does not populate
// metadata such as usedEdges. If future callers need bitSet/cumCost/usedEdges,
// this branch must be extended to build a fully initialized Node.
func makeBushyTree(ctx base.PlanContext, detector *ConflictDetector, cartesianNodes []*Node, vertexHints map[int]*JoinMethodHint, fastPath bool) (*Node, error) {
	var iterNodes []*Node
	var err error
	for len(cartesianNodes) > 1 {
		for i := 0; i < len(cartesianNodes); i += 2 {
			if i+1 >= len(cartesianNodes) {
				iterNodes = append(iterNodes, cartesianNodes[i])
				break
			}
			var newJoin *Node
			if fastPath {
				p, err1 := newCartesianJoin(ctx, base.InnerJoin, cartesianNodes[i].p, cartesianNodes[i+1].p, vertexHints)
				newJoin = &Node{p: p}
				err = err1
			} else {
				newJoin, err = makeJoinWithDetector(detector, cartesianNodes[i], cartesianNodes[i+1], vertexHints)
			}
			if err != nil {
				return nil, err
			}
			iterNodes = append(iterNodes, newJoin)
		}
		cartesianNodes = iterNodes
		iterNodes = iterNodes[:0]
	}
	return cartesianNodes[0], nil
}
