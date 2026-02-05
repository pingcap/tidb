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
	"maps"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/intset"
)

// ConflictDetector is used to detect conflicts between join edges in a join graph.
// It's based on the paper "On the Correct and Complete Enumeration of the Core Search Space".
type ConflictDetector struct {
	ctx           base.PlanContext
	groupRoot     base.LogicalPlan
	groupVertexes []*Node
	innerEdges    []*edge
	nonInnerEdges []*edge
	allInnerJoin  bool
}

type edge struct {
	idx uint64
	// No need to store the original join operator,
	// because we may generate a new join operator, so only join conditions are enough.
	joinType base.JoinType
	eqConds  []*expression.ScalarFunction
	// It could be otherCond, leftCond or rightCond.
	nonEQConds expression.CNFExprs

	tes   intset.FastIntSet
	rules []*rule
	// If all joins in the group are inner joins, conflict rules are unnecessary.
	skipRules bool

	leftEdges     []*edge
	rightEdges    []*edge
	leftVertexes  intset.FastIntSet
	rightVertexes intset.FastIntSet
}

// TryCreateCartesianCheckResult creates a CheckConnectionResult representing a cartesian product between left and right nodes.
// This is used when we still want to make a cartesian join even there is no join condition between two nodes.
// This usually happens when there is a leading hint forcing the join order.
func (d *ConflictDetector) TryCreateCartesianCheckResult(left, right *Node) *CheckConnectionResult {
	if !d.allInnerJoin {
		return nil
	}
	cartesianEdge := d.makeEdge(base.InnerJoin, []expression.Expression{}, left.bitSet, right.bitSet, nil, nil)
	return &CheckConnectionResult{
		node1:             left,
		node2:             right,
		appliedInnerEdges: []*edge{cartesianEdge},
		hasEQCond:         false,
	}
}

type rule struct {
	from intset.FastIntSet
	to   intset.FastIntSet
}

// Node can be a leaf node(vertex) or a intermediate node(join of two nodes).
type Node struct {
	bitSet    intset.FastIntSet
	p         base.LogicalPlan
	cumCost   float64
	usedEdges map[uint64]struct{}
}

func calcCumCost(p base.LogicalPlan) float64 {
	cost := p.StatsInfo().RowCount
	for _, child := range p.Children() {
		cost += calcCumCost(child)
	}
	return cost
}

func (n *Node) checkUsedEdges(edgeIdx uint64) bool {
	_, used := n.usedEdges[edgeIdx]
	return used
}

func newConflictDetector(ctx base.PlanContext) *ConflictDetector {
	return &ConflictDetector{
		ctx: ctx,
	}
}

// Build will construct the conflict detector from the given join group.
func (d *ConflictDetector) Build(group *joinGroup) ([]*Node, error) {
	d.groupRoot = group.root
	d.allInnerJoin = group.allInnerJoin

	vertexMap := make(map[int]*Node, len(group.vertexes))
	for i, v := range group.vertexes {
		if _, _, err := v.RecursiveDeriveStats(nil); err != nil {
			return nil, err
		}
		vertexMap[v.ID()] = &Node{
			bitSet:  intset.NewFastIntSet(i),
			p:       v,
			cumCost: calcCumCost(v),
		}
	}

	if _, _, err := d.buildRecursive(group.root, vertexMap); err != nil {
		return nil, err
	}
	return d.groupVertexes, nil
}

func (d *ConflictDetector) buildRecursive(p base.LogicalPlan, vertexMap map[int]*Node) ([]*edge, intset.FastIntSet, error) {
	if vertexNode, ok := vertexMap[p.ID()]; ok {
		d.groupVertexes = append(d.groupVertexes, vertexNode)
		return nil, vertexNode.bitSet, nil
	}

	var curVertexes intset.FastIntSet
	// All internal nodes in the join group should be join operators.
	joinop, ok := p.(*logicalop.LogicalJoin)
	if !ok {
		return nil, intset.FastIntSet{}, errors.New("unexpected plan type in conflict detector")
	}

	leftEdges, leftVertexes, err := d.buildRecursive(joinop.Children()[0], vertexMap)
	if err != nil {
		return nil, curVertexes, err
	}
	rightEdges, rightVertexes, err := d.buildRecursive(joinop.Children()[1], vertexMap)
	if err != nil {
		return nil, curVertexes, err
	}

	var curEdges []*edge
	if joinop.JoinType == base.InnerJoin {
		if curEdges, err = d.makeInnerEdge(joinop, leftVertexes, rightVertexes, leftEdges, rightEdges); err != nil {
			return nil, curVertexes, err
		}
	} else {
		curEdge := d.makeNonInnerEdge(joinop, leftVertexes, rightVertexes, leftEdges, rightEdges)
		curEdges = []*edge{curEdge}
	}
	if leftVertexes.Intersects(rightVertexes) {
		return nil, curVertexes, errors.New("conflicting join edges detected")
	}
	curVertexes = leftVertexes.Union(rightVertexes)

	return append(leftEdges, append(rightEdges, curEdges...)...), curVertexes, nil
}

func (d *ConflictDetector) makeInnerEdge(joinop *logicalop.LogicalJoin, leftVertexes, rightVertexes intset.FastIntSet, leftEdges, rightEdges []*edge) (res []*edge, err error) {
	if len(joinop.NAEQConditions) > 0 {
		return nil, errors.New("NAEQConditions not supported in conflict detector yet")
	}

	conds := expression.ScalarFuncs2Exprs(joinop.EqualConditions)
	nonEQConds := make([]expression.Expression, 0, len(joinop.LeftConditions)+len(joinop.RightConditions)+len(joinop.OtherConditions))
	nonEQConds = append(nonEQConds, joinop.OtherConditions...)
	nonEQConds = append(nonEQConds, joinop.LeftConditions...)
	nonEQConds = append(nonEQConds, joinop.RightConditions...)

	if len(conds) == 0 && len(nonEQConds) == 0 {
		tmp := d.makeEdge(base.InnerJoin, []expression.Expression{}, leftVertexes, rightVertexes, leftEdges, rightEdges)
		res = append(res, tmp)
	}

	condArg := make([]expression.Expression, 1)
	for _, cond := range conds {
		condArg[0] = cond
		tmp := d.makeEdge(base.InnerJoin, condArg, leftVertexes, rightVertexes, leftEdges, rightEdges)
		tmp.eqConds = append(tmp.eqConds, cond.(*expression.ScalarFunction))
		res = append(res, tmp)
	}

	for _, cond := range nonEQConds {
		condArg[0] = cond
		tmp := d.makeEdge(base.InnerJoin, condArg, leftVertexes, rightVertexes, leftEdges, rightEdges)
		tmp.nonEQConds = append(tmp.nonEQConds, cond)
		res = append(res, tmp)
	}
	return
}

func (d *ConflictDetector) makeNonInnerEdge(joinop *logicalop.LogicalJoin, leftVertexes, rightVertexes intset.FastIntSet, leftEdges, rightEdges []*edge) *edge {
	nonEQConds := make([]expression.Expression, 0, len(joinop.LeftConditions)+len(joinop.RightConditions)+len(joinop.OtherConditions))
	nonEQConds = append(nonEQConds, joinop.LeftConditions...)
	nonEQConds = append(nonEQConds, joinop.RightConditions...)
	nonEQConds = append(nonEQConds, joinop.OtherConditions...)

	conds := expression.ScalarFuncs2Exprs(joinop.EqualConditions)
	if len(conds) == 0 && len(nonEQConds) == 0 {
		return d.makeEdge(joinop.JoinType, []expression.Expression{}, leftVertexes, rightVertexes, leftEdges, rightEdges)
	}

	conds = append(conds, nonEQConds...)

	e := d.makeEdge(joinop.JoinType, conds, leftVertexes, rightVertexes, leftEdges, rightEdges)
	e.eqConds = make([]*expression.ScalarFunction, len(joinop.EqualConditions))
	copy(e.eqConds, joinop.EqualConditions)
	e.nonEQConds = nonEQConds

	return e
}

func (d *ConflictDetector) makeEdge(joinType base.JoinType, conds []expression.Expression, leftVertexes, rightVertexes intset.FastIntSet, leftEdges, rightEdges []*edge) *edge {
	e := &edge{
		idx:           uint64(len(d.innerEdges) + len(d.nonInnerEdges)),
		joinType:      joinType,
		leftVertexes:  leftVertexes,
		rightVertexes: rightVertexes,
		leftEdges:     leftEdges,
		rightEdges:    rightEdges,
		skipRules:     d.allInnerJoin,
	}

	// setup TES. Only consider EqualConditions and NAEQConditions.
	// OtherConditions are not edges.
	e.tes = d.calcTES(conds)
	// For degenerate predicates (only one side referenced), force TES to include
	// both sides so the edge can't connect unrelated subsets.
	if !e.tes.Intersects(e.leftVertexes) {
		e.tes = e.tes.Union(e.leftVertexes)
	}
	if !e.tes.Intersects(e.rightVertexes) {
		e.tes = e.tes.Union(e.rightVertexes)
	}

	if joinType == base.InnerJoin {
		d.innerEdges = append(d.innerEdges, e)
	} else {
		d.nonInnerEdges = append(d.nonInnerEdges, e)
	}

	// setup conflict rules
	if d.allInnerJoin {
		return e
	}
	for _, child := range leftEdges {
		if !assoc(child, e) {
			e.rules = append(e.rules, rightToLeftRule(child))
		}
		if !leftAsscom(child, e) {
			e.rules = append(e.rules, leftToRightRule(child))
		}
	}
	for _, child := range rightEdges {
		if !assoc(e, child) {
			e.rules = append(e.rules, leftToRightRule(child))
		}
		if !rightAsscom(e, child) {
			e.rules = append(e.rules, rightToLeftRule(child))
		}
	}

	return e
}

func rightToLeftRule(child *edge) *rule {
	rule := &rule{from: child.rightVertexes}
	if child.leftVertexes.Intersects(child.tes) {
		rule.to = child.leftVertexes.Intersection(child.tes)
	} else {
		rule.to = child.leftVertexes
	}
	return rule
}

func leftToRightRule(child *edge) *rule {
	rule := &rule{from: child.leftVertexes}
	if child.rightVertexes.Intersects(child.tes) {
		rule.to = child.rightVertexes.Intersection(child.tes)
	} else {
		rule.to = child.rightVertexes
	}
	return rule
}

func (d *ConflictDetector) calcTES(conds []expression.Expression) intset.FastIntSet {
	var res intset.FastIntSet
	for _, cond := range conds {
		for _, node := range d.groupVertexes {
			if expression.ExprReferenceSchema(cond, node.p.Schema()) {
				res = res.Union(node.bitSet)
			}
		}
	}
	return res
}

// joinTypeConvertTable maps base.JoinType to indices used in rule tables.
var joinTypeConvertTable = []int{
	0, // INNER
	1, // LEFT OUTER
	2, // RIGHT OUTER
	3, // LEFT SEMI
	4, // LEFT ANTI
	3, // LEFT OUTER SEMI
	4, // ANTI LEFT OUTER SEMI
}

// TODO doesn't support null-rejective for now(2 in the Table-2 and Table-3).
// Table-2 and Table-3 are from the paper "On the Correct and Complete Enumeration of the Core Search Space".
// It will be treated as rule doesn't apply.
func assoc(e1, e2 *edge) bool {
	j1 := joinTypeConvertTable[e1.joinType]
	j2 := joinTypeConvertTable[e2.joinType]
	return assocRuleTable[j1][j2] == 1
}

func leftAsscom(e1, e2 *edge) bool {
	j1 := joinTypeConvertTable[e1.joinType]
	j2 := joinTypeConvertTable[e2.joinType]
	return leftAsscomRuleTable[j1][j2] == 1
}

func rightAsscom(e1, e2 *edge) bool {
	j1 := joinTypeConvertTable[e1.joinType]
	j2 := joinTypeConvertTable[e2.joinType]
	return rightAsscomRuleTable[j1][j2] == 1
}

// CheckConnectionResult contains the result of checking connection between two nodes.
type CheckConnectionResult struct {
	node1               *Node
	node2               *Node
	appliedInnerEdges   []*edge
	appliedNonInnerEdge *edge
	hasEQCond           bool
}

// Connected checks if two nodes are connected.
func (r *CheckConnectionResult) Connected() bool {
	return len(r.appliedInnerEdges) > 0 || r.appliedNonInnerEdge != nil
}

// NoEQEdge checks if there is no EQ edge between two nodes.
func (r *CheckConnectionResult) NoEQEdge() bool {
	return !r.hasEQCond
}

// CheckAndMakeJoin checks the connection between two nodes and makes a join if they are connected.
func (d *ConflictDetector) CheckAndMakeJoin(node1, node2 *Node, vertexHints map[int]*JoinMethodHint) (*Node, error) {
	checkResult, err := d.CheckConnection(node1, node2)
	if err != nil {
		return nil, err
	}
	if !checkResult.Connected() {
		return nil, nil
	}
	return d.MakeJoin(checkResult, vertexHints)
}

// CheckConnection checks if there is a connection between two nodes.
func (d *ConflictDetector) CheckConnection(node1, node2 *Node) (*CheckConnectionResult, error) {
	if node1 == nil || node2 == nil {
		return nil, errors.Errorf("nil node found in CheckConnection, node1: %v, node2: %v", node1, node2)
	}

	result := &CheckConnectionResult{
		node1: node1,
		node2: node2,
	}
	for _, e := range d.innerEdges {
		if node1.checkUsedEdges(e.idx) || node2.checkUsedEdges(e.idx) {
			continue
		}
		if e.checkInnerEdge(node1, node2) {
			result.appliedInnerEdges = append(result.appliedInnerEdges, e)
			result.hasEQCond = result.hasEQCond || len(e.eqConds) > 0
		}
	}
	for _, e := range d.nonInnerEdges {
		if node1.checkUsedEdges(e.idx) || node2.checkUsedEdges(e.idx) {
			continue
		}
		if e.checkNonInnerEdge(node1, node2) {
			if result.appliedNonInnerEdge != nil {
				return nil, errors.New("multiple non-inner edges applied between two nodes")
			}
			result.appliedNonInnerEdge = e
			result.hasEQCond = result.hasEQCond || len(e.eqConds) > 0
		}
	}
	return result, nil
}

func (e *edge) checkInnerEdge(node1, node2 *Node) bool {
	if !e.skipRules && !e.checkRules(node1, node2) {
		return false
	}
	return e.tes.SubsetOf(node1.bitSet.Union(node2.bitSet)) &&
		e.tes.Intersects(node1.bitSet) &&
		e.tes.Intersects(node2.bitSet)
}

func (e *edge) checkNonInnerEdge(node1, node2 *Node) bool {
	if !e.skipRules && !e.checkRules(node1, node2) {
		return false
	}
	return e.leftVertexes.Intersection(e.tes).SubsetOf(node1.bitSet) &&
		e.rightVertexes.Intersection(e.tes).SubsetOf(node2.bitSet) &&
		e.tes.Intersects(node1.bitSet) &&
		e.tes.Intersects(node2.bitSet)
}

func (e *edge) checkRules(node1, node2 *Node) bool {
	s := node1.bitSet.Union(node2.bitSet)
	for _, r := range e.rules {
		if r.from.Intersects(s) && !r.to.SubsetOf(s) {
			return false
		}
	}
	return true
}

// MakeJoin construct a join plan from the check result.
func (d *ConflictDetector) MakeJoin(checkResult *CheckConnectionResult, vertexHints map[int]*JoinMethodHint) (*Node, error) {
	numInnerEdges := len(checkResult.appliedInnerEdges)
	var numNonInnerEdges int
	if checkResult.appliedNonInnerEdge != nil {
		numNonInnerEdges = 1
	}

	var err error
	var p base.LogicalPlan
	var newJoin *logicalop.LogicalJoin
	if numNonInnerEdges > 0 {
		if newJoin, err = makeNonInnerJoin(d.ctx, checkResult, vertexHints); err != nil {
			return nil, err
		}
	}
	if numInnerEdges > 0 {
		if p, err = makeInnerJoin(d.ctx, checkResult, newJoin, vertexHints); err != nil {
			return nil, err
		}
	} else {
		p = newJoin
	}
	if p == nil {
		return nil, errors.New("failed to make join plan")
	}
	if _, _, err := p.RecursiveDeriveStats(nil); err != nil {
		return nil, err
	}

	node1 := checkResult.node1
	node2 := checkResult.node2
	usedEdges := make(map[uint64]struct{}, numInnerEdges+numNonInnerEdges+len(node1.usedEdges)+len(node2.usedEdges))
	for _, e := range checkResult.appliedInnerEdges {
		usedEdges[e.idx] = struct{}{}
	}
	if checkResult.appliedNonInnerEdge != nil {
		usedEdges[checkResult.appliedNonInnerEdge.idx] = struct{}{}
	}
	maps.Copy(usedEdges, node1.usedEdges)
	maps.Copy(usedEdges, node2.usedEdges)
	return &Node{
		bitSet:    node1.bitSet.Union(node2.bitSet),
		p:         p,
		cumCost:   calcCumCost(p),
		usedEdges: usedEdges,
	}, nil
}

func alignEQConds(ctx base.PlanContext, left, right base.LogicalPlan, eqConds []*expression.ScalarFunction) (newLeft base.LogicalPlan, newRight base.LogicalPlan, alignedEQConds []*expression.ScalarFunction, err error) {
	if len(eqConds) == 0 {
		return left, right, nil, nil
	}
	res := make([]*expression.ScalarFunction, 0, len(eqConds))
	for _, cond := range eqConds {
		args := cond.GetArgs()
		if len(args) != 2 {
			return nil, nil, nil, errors.Errorf("unexpected eq condition args: %d", len(args))
		}
		if expression.ExprFromSchema(args[0], left.Schema()) && expression.ExprFromSchema(args[1], right.Schema()) {
			res = append(res, cond)
			continue
		}
		if expression.ExprFromSchema(args[1], left.Schema()) && expression.ExprFromSchema(args[0], right.Schema()) {
			swapped, ok := expression.NewFunctionInternal(ctx.GetExprCtx(), cond.FuncName.L, cond.GetStaticType(), args[1], args[0]).(*expression.ScalarFunction)
			if !ok {
				return nil, nil, nil, errors.New("failed to build swapped eq condition")
			}
			_, isCol0 := swapped.GetArgs()[0].(*expression.Column)
			_, isCol1 := swapped.GetArgs()[1].(*expression.Column)
			if !isCol0 || !isCol1 {
				lCol := swapped.GetArgs()[0]
				rCol := swapped.GetArgs()[1]
				if !isCol0 {
					left, lCol = logicalop.InjectExpr(left, swapped.GetArgs()[0])
				}
				if !isCol1 {
					right, rCol = logicalop.InjectExpr(right, swapped.GetArgs()[1])
				}
				swapped = expression.NewFunctionInternal(ctx.GetExprCtx(), cond.FuncName.L, cond.GetStaticType(),
					lCol, rCol).(*expression.ScalarFunction)
			}
			res = append(res, swapped)
			continue
		}
		return nil, nil, nil, errors.New("eq condition does not match join sides")
	}
	return left, right, res, nil
}

func makeNonInnerJoin(ctx base.PlanContext, checkResult *CheckConnectionResult, vertexHints map[int]*JoinMethodHint) (*logicalop.LogicalJoin, error) {
	e := checkResult.appliedNonInnerEdge
	var alignedEQConds []*expression.ScalarFunction
	var err error

	checkResult.node1.p, checkResult.node2.p, alignedEQConds, err = alignEQConds(ctx, checkResult.node1.p, checkResult.node2.p, e.eqConds)
	if err != nil {
		return nil, err
	}

	left := checkResult.node1.p
	right := checkResult.node2.p

	join, err := newCartesianJoin(ctx, e.joinType, left, right, vertexHints)
	if err != nil {
		return nil, err
	}
	join.EqualConditions = alignedEQConds
	for _, cond := range e.nonEQConds {
		fromLeft := expression.ExprFromSchema(cond, left.Schema())
		fromRight := expression.ExprFromSchema(cond, right.Schema())
		if fromLeft && !fromRight {
			join.LeftConditions = append(join.LeftConditions, cond)
		} else if !fromLeft && fromRight {
			join.RightConditions = append(join.RightConditions, cond)
		} else {
			join.OtherConditions = append(join.OtherConditions, cond)
		}
	}
	return join, nil
}

func makeInnerJoin(ctx base.PlanContext, checkResult *CheckConnectionResult, existingJoin *logicalop.LogicalJoin, vertexHints map[int]*JoinMethodHint) (base.LogicalPlan, error) {
	if existingJoin != nil {
		// Append selections to existing join
		condCap := 0
		for _, e := range checkResult.appliedInnerEdges {
			condCap += len(e.eqConds) + len(e.nonEQConds)
		}
		selection := logicalop.LogicalSelection{
			Conditions: make([]expression.Expression, 0, condCap),
		}
		for _, e := range checkResult.appliedInnerEdges {
			eqExprs := expression.ScalarFuncs2Exprs(e.eqConds)
			selection.Conditions = append(selection.Conditions, eqExprs...)
			selection.Conditions = append(selection.Conditions, e.nonEQConds...)
		}
		resSelection := selection.Init(ctx, existingJoin.QueryBlockOffset())
		resSelection.SetChildren(existingJoin)
		return resSelection, nil
	}

	var err error
	var alignedEQConds []*expression.ScalarFunction
	newEqConds := make([]*expression.ScalarFunction, 0, 8)
	newOtherConds := make([]expression.Expression, 0, 8)
	for _, e := range checkResult.appliedInnerEdges {
		checkResult.node1.p, checkResult.node2.p, alignedEQConds, err = alignEQConds(ctx, checkResult.node1.p, checkResult.node2.p, e.eqConds)
		if err != nil {
			return nil, err
		}
		newEqConds = append(newEqConds, alignedEQConds...)
		newOtherConds = append(newOtherConds, e.nonEQConds...)
	}
	join, err := newCartesianJoin(ctx, checkResult.appliedInnerEdges[0].joinType, checkResult.node1.p, checkResult.node2.p, vertexHints)
	if err != nil {
		return nil, err
	}
	join.EqualConditions = append(join.EqualConditions, newEqConds...)
	join.OtherConditions = append(join.OtherConditions, newOtherConds...)

	return join, nil
}

func newCartesianJoin(ctx base.PlanContext, joinType base.JoinType, left, right base.LogicalPlan, vertexHints map[int]*JoinMethodHint) (*logicalop.LogicalJoin, error) {
	offset := left.QueryBlockOffset()
	if offset != right.QueryBlockOffset() {
		offset = -1
	}

	join := logicalop.LogicalJoin{
		JoinType:  joinType,
		Reordered: true,
	}.Init(ctx, offset)
	join.SetChildren(left, right)
	join.SetSchema(expression.MergeSchema(left.Schema(), right.Schema()))
	join.SetChildren(left, right)

	SetNewJoinWithHint(join, vertexHints)
	return join, nil
}

// CheckAllEdgesUsed checks if all edges with join conditions are used.
func (d *ConflictDetector) CheckAllEdgesUsed(usedEdges map[uint64]struct{}) bool {
	for _, e := range d.innerEdges {
		if len(e.eqConds) > 0 || len(e.nonEQConds) > 0 {
			if _, ok := usedEdges[e.idx]; !ok {
				return false
			}
		}
	}
	for _, e := range d.nonInnerEdges {
		if len(e.eqConds) > 0 || len(e.nonEQConds) > 0 {
			if _, ok := usedEdges[e.idx]; !ok {
				return false
			}
		}
	}
	return true
}

// SetNewJoinWithHint sets the join method hint for the join node.
func SetNewJoinWithHint(newJoin *logicalop.LogicalJoin, vertexHints map[int]*JoinMethodHint) {
	if newJoin == nil {
		return
	}
	lChild := newJoin.Children()[0]
	rChild := newJoin.Children()[1]
	if joinMethodHint, ok := vertexHints[lChild.ID()]; ok {
		newJoin.LeftPreferJoinType = joinMethodHint.PreferJoinMethod
		newJoin.HintInfo = joinMethodHint.HintInfo
	}
	if joinMethodHint, ok := vertexHints[rChild.ID()]; ok {
		newJoin.RightPreferJoinType = joinMethodHint.PreferJoinMethod
		newJoin.HintInfo = joinMethodHint.HintInfo
	}
	newJoin.SetPreferredJoinType()
}

// 0: rule doesn't apply
// 1: rule applies
// 2: rule applies when null-rejective holds
type ruleTableEntry int

var assocRuleTable = [][]ruleTableEntry{
	// INNER
	{
		1, // INNER
		1, // LEFT OUTER
		0, // RIGHT OUTER
		1, // LEFT SEMI and LEFT OUTER SEMI
		1, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT OUTER
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// RIGHT OUTER
	{
		1, // INNER
		1, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT SEMI and LEFT OUTER SEMI
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},

	// LEFT ANTI and ANTI LEFT OUTER SEMI
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
}

var leftAsscomRuleTable = [][]ruleTableEntry{
	// INNER
	{
		1, // INNER
		1, // LEFT OUTER
		0, // RIGHT OUTER
		1, // LEFT SEMI and LEFT OUTER SEMI
		1, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT OUTER
	{
		1, // INNER
		1, // LEFT OUTER
		0, // RIGHT OUTER
		1, // LEFT SEMI and LEFT OUTER SEMI
		1, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// RIGHT OUTER
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT SEMI and LEFT OUTER SEMI
	{
		1, // INNER
		1, // LEFT OUTER
		1, // RIGHT OUTER
		1, // LEFT SEMI and LEFT OUTER SEMI
		1, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT ANTI and ANTI LEFT OUTER SEMI
	{
		1, // INNER
		1, // LEFT OUTER
		1, // RIGHT OUTER
		1, // LEFT SEMI and LEFT OUTER SEMI
		1, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
}

var rightAsscomRuleTable = [][]ruleTableEntry{
	// INNER
	{
		1, // INNER
		1, // LEFT OUTER
		1, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT OUTER
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// RIGHT OUTER
	{
		0, // INNER
		1, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT SEMI and LEFT OUTER SEMI
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT ANTI and ANTI LEFT OUTER SEMI
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
}
