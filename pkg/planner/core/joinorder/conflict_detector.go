package joinorder

import (
	"github.com/pingcap/errors"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

type ConflictDetector struct {
	ctx           base.PlanContext
	groupRoot     base.LogicalPlan
	groupVertexes []*Node
	innerEdges    []*edge
	nonInnerEdges []*edge
}

type edge struct {
	idx uint64
	// No need to store the original join operator,
	// because we may generate a new join operator, so only join conditions are enough.
	joinType base.JoinType
	eqConds  []*expression.ScalarFunction
	// It could be otherCond, leftCond or rightCond.
	nonEQConds expression.CNFExprs

	tes   BitSet
	rules []*rule

	leftEdges     []*edge
	rightEdges    []*edge
	leftVertexes  BitSet
	rightVertexes BitSet
}

type BitSet uint64

func newBitSet(idx int64) BitSet {
	return 1 << idx
}

func (b BitSet) Union(s BitSet) BitSet {
	return b | s
}

func (b BitSet) HasIntersect(s BitSet) bool {
	return (b & s) != 0
}

func (b BitSet) Intersect(s BitSet) BitSet {
	return b & s
}

func (b BitSet) Contains(s BitSet) bool {
	return (b & s) == s
}

func (b BitSet) IsSubsetOf(s BitSet) bool {
	return (b & s) == b
}

type rule struct {
	from BitSet
	to   BitSet
}

type Node struct {
	bitSet    BitSet
	p         base.LogicalPlan
	cumCost   float64
	usedEdges map[uint64]struct{}
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

func (d *ConflictDetector) Build(group *joinGroup) ([]*Node, error) {
	if len(group.vertexes) > 64 {
		return nil, errors.Errorf("too many vertexes in join group: %d, exceeds maximum supported 64", len(group.vertexes))
	}
	d.groupRoot = group.root

	vertexMap := make(map[int]*Node, len(group.vertexes))
	for i, v := range group.vertexes {
		vertexMap[v.ID()] = &Node{
			bitSet: newBitSet(int64(i)),
			p:      v,
		}
	}

	if _, _, err := d.buildRecursive(group.root, group.vertexes, vertexMap); err != nil {
		return nil, err
	}
	return d.groupVertexes, nil
}

func (d *ConflictDetector) buildRecursive(p base.LogicalPlan, vertexes []base.LogicalPlan, vertexMap map[int]*Node) ([]*edge, BitSet, error) {
	if vertexNode, ok := vertexMap[p.ID()]; ok {
		d.groupVertexes = append(d.groupVertexes, vertexNode)
		return nil, vertexNode.bitSet, nil
	}

	// All internal nodes in the join group should be join operators.
	joinop, ok := p.(*logicalop.LogicalJoin)
	if !ok {
		return nil, 0, errors.New("unexpected plan type in conflict detector")
	}

	leftEdges, leftVertexes, err := d.buildRecursive(joinop.Children()[0], vertexes, vertexMap)
	if err != nil {
		return nil, 0, err
	}
	rightEdges, rightVertexes, err := d.buildRecursive(joinop.Children()[1], vertexes, vertexMap)
	if err != nil {
		return nil, 0, err
	}

	var curEdges []*edge
	if joinop.JoinType == base.InnerJoin {
		curEdges = d.makeInnerEdge(joinop, leftVertexes, rightVertexes, leftEdges, rightEdges)
	} else {
		curEdge := d.makeNonInnerEdge(joinop, leftVertexes, rightVertexes, leftEdges, rightEdges)
		curEdges = []*edge{curEdge}
	}
	if leftVertexes.HasIntersect(rightVertexes) {
		return nil, 0, errors.New("conflicting join edges detected")
	}
	curVertexes := leftVertexes.Union(rightVertexes)

	return append(leftEdges, append(rightEdges, curEdges...)...), curVertexes, nil
}

func (d *ConflictDetector) makeInnerEdge(joinop *logicalop.LogicalJoin, leftVertexes, rightVertexes BitSet, leftEdges, rightEdges []*edge) (res []*edge) {
	// todo: handle NAEQConditions
	// e.tes = e.tes.Union(d.calcTES(expression.ScalarFuncs2Exprs(joinop.NAEQConditions)))
	if len(joinop.NAEQConditions) > 0 {
		panic("NAEQConditions not supported in conflict detector yet")
	}

	conds := expression.ScalarFuncs2Exprs(joinop.EqualConditions)
	condArg := make([]expression.Expression, 1)
	for _, cond := range conds {
		condArg[0] = cond
		tmp := d.makeEdge(base.InnerJoin, condArg, leftVertexes, rightVertexes, leftEdges, rightEdges)
		tmp.eqConds = append(tmp.eqConds, cond.(*expression.ScalarFunction))
		res = append(res, tmp)
	}
	for _, cond := range joinop.OtherConditions {
		condArg[0] = cond
		tmp := d.makeEdge(base.InnerJoin, condArg, leftVertexes, rightVertexes, leftEdges, rightEdges)
		tmp.nonEQConds = append(tmp.nonEQConds, cond)
		res = append(res, tmp)
	}
	return
}

func (d *ConflictDetector) makeNonInnerEdge(joinop *logicalop.LogicalJoin, leftVertexes, rightVertexes BitSet, leftEdges, rightEdges []*edge) *edge {
	nonEQConds := make([]expression.Expression, 0, len(joinop.LeftConditions)+len(joinop.RightConditions)+len(joinop.OtherConditions))
	nonEQConds = append(nonEQConds, joinop.LeftConditions...)
	nonEQConds = append(nonEQConds, joinop.RightConditions...)
	nonEQConds = append(nonEQConds, joinop.OtherConditions...)

	conds := expression.ScalarFuncs2Exprs(joinop.EqualConditions)
	conds = append(conds, nonEQConds...)

	e := d.makeEdge(joinop.JoinType, conds, leftVertexes, rightVertexes, leftEdges, rightEdges)
	e.eqConds = make([]*expression.ScalarFunction, len(joinop.EqualConditions))
	copy(e.eqConds, joinop.EqualConditions)
	e.nonEQConds = nonEQConds
	return e
}

func (d *ConflictDetector) makeEdge(joinType base.JoinType, conds []expression.Expression, leftVertexes, rightVertexes BitSet, leftEdges, rightEdges []*edge) *edge {
	e := &edge{
		idx:           uint64(len(d.innerEdges) + len(d.nonInnerEdges)),
		joinType:      joinType,
		leftVertexes:  leftVertexes,
		rightVertexes: rightVertexes,
		leftEdges:     leftEdges,
		rightEdges:    rightEdges,
	}

	// setup TES. Only consider EqualConditions and NAEQConditions.
	// OtherConditions are not edges.
	e.tes = d.calcTES(conds)

	if joinType == base.InnerJoin {
		d.innerEdges = append(d.innerEdges, e)
	} else {
		d.nonInnerEdges = append(d.nonInnerEdges, e)
	}

	// gjt todo: handle CrossProduct

	// setup conflict rules
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
	if child.leftVertexes.HasIntersect(child.tes) {
		rule.to = child.leftVertexes.Intersect(child.tes)
	} else {
		rule.to = child.leftVertexes
	}
	return rule
}

func leftToRightRule(child *edge) *rule {
	rule := &rule{from: child.leftVertexes}
	if child.rightVertexes.HasIntersect(child.tes) {
		rule.to = child.rightVertexes.Intersect(child.tes)
	} else {
		rule.to = child.rightVertexes
	}
	return rule
}

func (d *ConflictDetector) calcTES(conds []expression.Expression) BitSet {
	var res BitSet
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

func assoc(e1, e2 *edge) bool {
	j1 := joinTypeConvertTable[e1.joinType]
	j2 := joinTypeConvertTable[e2.joinType]
	// gjt todo handle null-rejective
	if assocRuleTable[j1][j2] == 0 {
		return false
	}
	return true
}

func leftAsscom(e1, e2 *edge) bool {
	j1 := joinTypeConvertTable[e1.joinType]
	j2 := joinTypeConvertTable[e2.joinType]
	if leftAsscomRuleTable[j1][j2] == 0 {
		return false
	}
	return true
}

func rightAsscom(e1, e2 *edge) bool {
	j1 := joinTypeConvertTable[e1.joinType]
	j2 := joinTypeConvertTable[e2.joinType]
	if rightAsscomRuleTable[j1][j2] == 0 {
		return false
	}
	return true
}

// gjt todo maybe sync.pool
// gjt todo remove
type CheckConnectionResult struct {
	node1               *Node
	node2               *Node
	appliedInnerEdges   []*edge
	appliedNonInnerEdge *edge
	hasEQCond           bool
}

func (r *CheckConnectionResult) Connected() bool {
	return len(r.appliedInnerEdges) > 0 || r.appliedNonInnerEdge != nil
}

func (r *CheckConnectionResult) NoEQEdge() bool {
	return !r.hasEQCond
}

func (d *ConflictDetector) CheckAndMakeJoin(node1, node2 *Node, vertexHints map[int]*vertexJoinMethodHint) (*Node, error) {
	checkResult, err := d.CheckConnection(node1, node2)
	if err != nil {
		return nil, err
	}
	if !checkResult.Connected() {
		return nil, nil
	}
	return d.MakeJoin(checkResult, vertexHints)
}

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
	if !e.checkRules(node1, node2) {
		return false
	}
	// gjt todo refine this check
	return e.tes.IsSubsetOf(node1.bitSet.Union(node2.bitSet))
}

func (e *edge) checkNonInnerEdge(node1, node2 *Node) bool {
	if !e.checkRules(node1, node2) {
		return false
	}
	// gjt todo commutative?
	// gjt todo refine this check
	return e.leftVertexes.IsSubsetOf(node1.bitSet) && e.rightVertexes.IsSubsetOf(node2.bitSet)
}

func (e *edge) checkRules(node1, node2 *Node) bool {
	s := node1.bitSet.Union(node2.bitSet)
	for _, r := range e.rules {
		if r.from.HasIntersect(s) && !r.to.IsSubsetOf(s) {
			return false
		}
	}
	return true
}

func (d *ConflictDetector) MakeJoin(checkResult *CheckConnectionResult, vertexHints map[int]*vertexJoinMethodHint) (*Node, error) {
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
	usedEdges := make(map[uint64]struct{}, numInnerEdges+numNonInnerEdges)
	for _, e := range checkResult.appliedInnerEdges {
		usedEdges[e.idx] = struct{}{}
	}
	if checkResult.appliedNonInnerEdge != nil {
		usedEdges[checkResult.appliedNonInnerEdge.idx] = struct{}{}
	}
	return &Node{
		bitSet:    node1.bitSet.Union(node2.bitSet),
		p:         p,
		cumCost:   node1.cumCost + node2.cumCost + p.StatsInfo().RowCount,
		usedEdges: usedEdges,
	}, nil
}

func makeNonInnerJoin(ctx base.PlanContext, checkResult *CheckConnectionResult, vertexHints map[int]*vertexJoinMethodHint) (*logicalop.LogicalJoin, error) {
	e := checkResult.appliedNonInnerEdge
	left := checkResult.node1.p
	right := checkResult.node2.p

	join, err := newCartesianJoin(ctx, e.joinType, left, right, vertexHints)
	if err != nil {
		return nil, err
	}
	join.EqualConditions = e.eqConds
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

func makeInnerJoin(ctx base.PlanContext, checkResult *CheckConnectionResult, existingJoin *logicalop.LogicalJoin, vertexHints map[int]*vertexJoinMethodHint) (base.LogicalPlan, error) {
	if existingJoin != nil {
		// Append selections to existing join
		selection := logicalop.LogicalSelection{
			Conditions: []expression.Expression{}, // gjt todo reserve space
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

	join, err := newCartesianJoin(ctx, checkResult.appliedInnerEdges[0].joinType, checkResult.node1.p, checkResult.node2.p, vertexHints)
	if err != nil {
		return nil, err
	}
	for _, e := range checkResult.appliedInnerEdges {
		join.EqualConditions = append(join.EqualConditions, e.eqConds...)
		for _, cond := range e.nonEQConds {
			join.OtherConditions = append(join.OtherConditions, cond)
		}
	}
	return join, nil
}

func newCartesianJoin(ctx base.PlanContext, joinType base.JoinType, left, right base.LogicalPlan, vertexHints map[int]*vertexJoinMethodHint) (*logicalop.LogicalJoin, error) {
	offset := left.QueryBlockOffset()
	if offset != right.QueryBlockOffset() {
		return nil, errors.Errorf("cannot make join with different query block offsets: %d and %d", offset, right.QueryBlockOffset())
	}

	// gjt todo other members?
	// gjt todo change init to pointer receiver
	join := logicalop.LogicalJoin{
		JoinType:  joinType,
		Reordered: true,
	}.Init(ctx, offset)
	join.SetChildren(left, right)
	join.SetSchema(expression.MergeSchema(left.Schema(), right.Schema()))
	join.SetChildren(left, right)

	setNewJoinWithHint(join, vertexHints)
	return join, nil
}

func setNewJoinWithHint(newJoin *logicalop.LogicalJoin, vertexHints map[int]*vertexJoinMethodHint) {
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
		0, // RIGHT OUTER gjt todo?
		1, // LEFT SEMI and LEFT OUTER SEMI
		1, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT OUTER
	{
		0, // INNER
		0, // LEFT OUTER gjt todo should be 2!!!
		0, // RIGHT OUTER gjt todo?
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// RIGHT OUTER gjt todo?
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER gjt todo?
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT SEMI and LEFT OUTER SEMI
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER gjt todo?
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},

	// LEFT ANTI and ANTI LEFT OUTER SEMI
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER gjt todo?
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
		0, // RIGHT OUTER
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
		0, // LEFT OUTER
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
