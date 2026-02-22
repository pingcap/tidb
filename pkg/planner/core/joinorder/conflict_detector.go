// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package joinorder

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/intset"
)

// This file implements the CD-C (Conflict Detection C) algorithm
// from the paper:
//
//	"On the Correct and Complete Enumeration of the Core Search Space"
//	  — Guido Moerkotte, Pit Fender, Marius Eich (2013)
//
// # Overview
//
// CD-C determines which join reorderings are semantically valid when outer joins
// (and semi/anti joins) are present. The key insight is that, unlike pure inner
// joins, outer joins are neither associative nor commutative in general.
// Rearranging them arbitrarily can change query results.
//
// # Core Concepts
//
// TES (Total Eligibility Set):
//
//	For each join predicate (edge), the TES records which base relations must be
//	present in a candidate subgraph for that predicate to be applicable. It starts
//	as the SES (Syntactic Eligibility Set, i.e. the relations referenced by the
//	predicate) and is extended by conflict rules.
//
// Conflict Rules:
//
//	A conflict rule {from → to} states: "if any relation in `from` appears in a
//	candidate join's input set S, then every relation in `to` must also appear in S."
//	These rules are derived by checking, for each pair of a parent edge and a child
//	edge, whether the three properties — associativity (assoc), left-asscom, and
//	right-asscom — hold for their join-type combination. When a property does NOT
//	hold, a conflict rule is generated to prevent the invalid reordering.
//
// Validity Check:
//
//	When the join enumerator proposes connecting two subgraphs (S1, S2), each edge
//	checks: (1) its TES is a subset of S1 ∪ S2, (2) it intersects both S1 and S2,
//	and (3) all conflict rules are satisfied. Only then is the join considered valid.
//
// # Rule Tables
//
// The three rule tables (assocRuleTable, leftAsscomRuleTable, rightAsscomRuleTable)
// encode, for every pair of join types, whether the corresponding algebraic property
// holds. They are derived from Table 2 and Table 3 of the paper. Since TiDB does not
// support FULL OUTER JOIN, many conditional entries (requiring null-rejection checks)
// reduce to unconditional values. See the comments on each table for details.

// ConflictDetector builds a join graph from the original plan tree and attaches
// conflict rules to each edge. It is then used by the join enumerator (greedy or
// DP) to validate candidate join pairs at enumeration time.
//
// # Workflow
//
// The lifecycle has two phases:
//
// Phase 1 — Build (called once per join group):
//
//	Build() walks the plan tree bottom-up via buildRecursive(). At each join
//	node it creates one or more edges:
//	  - Inner join: each conjunct becomes a separate edge (makeInnerEdge()),
//	    expanding the search space.
//	  - Non-inner join: all predicates stay in a single edge (makeNonInnerEdge()),
//	    keeping the join atomic.
//	For every new edge, makeEdge() computes its TES(calcSES()) and generates conflict rules
//	by comparing it against child edges from both subtrees.
//
// Phase 2 — CheckConnection (called repeatedly by the join enumerator):
//
//	CheckConnection(node1, node2) iterates over all edges and tests whether
//	each edge can validly connect the two nodes. The check differs by join kind:
//	  - Inner edge (checkInnerEdgeApplicable): TES ⊆ (S1 ∪ S2) and TES intersects both
//	    S1 and S2, plus all conflict rules pass.
//	  - Non-inner edge (checkNonInnerEdgeApplicable): additionally requires that the
//	    original left/right vertexes (intersected with TES) are fully contained
//	    in node1/node2 respectively, preserving outer-join side semantics.
//	When edges pass, MakeJoin() constructs the actual LogicalJoin plan from the
//	collected edges and returns a new merged Node.
type ConflictDetector struct {
	ctx           base.PlanContext
	groupRoot     base.LogicalPlan
	groupVertexes []*Node
	innerEdges    []*edge
	nonInnerEdges []*edge
	allInnerJoin  bool
}

// edge represents a single join predicate (or a group of predicates for non-inner
// joins) in the join graph. Each edge knows its join type, conditions, the base
// relations it originally connects (leftVertexes, rightVertexes), its TES, and
// any conflict rules that constrain how it may be applied.
type edge struct {
	idx      uint64
	joinType base.JoinType
	eqConds  []*expression.ScalarFunction
	// nonEQConds holds otherCond, leftCond, or rightCond — anything that is not
	// an equi-join predicate.
	nonEQConds expression.CNFExprs

	// TES is the Total Eligibility Set: the set of base relations that must be
	// present in the candidate subgraph for this edge to be applicable.
	// For now, TES is totally same with SES, check the TODO in makeEdge().
	tes intset.FastIntSet
	// rules are conflict rules {from → to} derived during Build. They encode
	// reordering constraints imposed by non-assoc/l-asscom/r-asscom join-type combinations.
	rules []*rule
	// skipRules is true when the entire join group is inner-join-only, in which
	// case conflict rules are unnecessary.
	skipRules bool

	// leftEdges/rightEdges are the child edges from the left/right subtrees at
	// build time. They are used to derive conflict rules for this edge.
	leftEdges     []*edge
	rightEdges    []*edge
	leftVertexes  intset.FastIntSet
	rightVertexes intset.FastIntSet
}

// TryCreateCartesianCheckResult creates a CheckConnectionResult representing a
// cartesian product between left and right nodes.
//
// When checkResult.Connected() returns false, there are actually two situations:
//  1. The two nodes are truly invalid to join (e.g. conflict rules forbid it).
//  2. The two nodes have no shared edge, but a cartesian join is still legal.
//
// checkResult.Connected() itself does not distinguish them — both return false.
// We can handle case 2 by adding a fallback "cross edge" when building the ConflictDetector,
// so that Connected() returns true.
// But for now, we take a simpler approach: checkResult.Connected() return false for above both situations,
// and let the caller explicitly call TryCreateCartesianCheckResult to construct a
// cartesian edge when the join group is all-inner-join.
//
// The cartesian edge is created in two situations (callers that pass allowNoEQ=true
// to checkConnectionAndMakeJoin):
//  1. A leading hint forces the connection (e.g. LEADING(R1, R3) when there is no
//     predicate between R1 and R3).
//  2. The greedy enumerator's second pass, where allowing cartesian joins may find
//     a better plan. See https://github.com/pingcap/tidb/issues/63290.
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

func (d *ConflictDetector) iterateEdges(fn func(e *edge) bool) {
	for _, e := range d.innerEdges {
		if !fn(e) {
			return
		}
	}
	for _, e := range d.nonInnerEdges {
		if !fn(e) {
			return
		}
	}
}

// rule is a conflict rule {from → to}: if any relation in `from` appears in the
// candidate set S, then every relation in `to` must also appear in S. Violating
// this would produce a semantically invalid join reordering.
type rule struct {
	from intset.FastIntSet
	to   intset.FastIntSet
}

// Node represents either a leaf vertex (a single base relation) or an
// intermediate result (a join of two nodes). During enumeration, the greedy
// algorithm repeatedly merges two Nodes into one via MakeJoin().
type Node struct {
	// bitSet tracks which base relations (by index) are contained in this node.
	bitSet intset.FastIntSet
	p      base.LogicalPlan
	// cumCost is the cumulative cost (sum of row counts) of this node and all
	// its descendants. It is used by the enumerator for join ordering.
	cumCost float64
	// usedEdges records which edges have already been consumed by joins that
	// produced this node. An edge must not be applied twice.
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

// Build constructs the join graph (edges + conflict rules) from a joinGroup.
// It returns the list of leaf Nodes (vertexes) of current join group, which will be merged to new join by the enumerator.
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

// buildRecursive walks the plan tree bottom-up. For each join node, it:
//  1. Recurses into left and right children to collect their edges and vertex sets.
//  2. Creates new edge(s) for the current join operator.
//  3. Returns the accumulated edges and the union of all vertex sets seen so far.
//
// The returned edges list is used by parent calls to generate conflict rules.
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
		curEdge, err := d.makeNonInnerEdge(joinop, leftVertexes, rightVertexes, leftEdges, rightEdges)
		if err != nil {
			return nil, curVertexes, err
		}
		curEdges = []*edge{curEdge}
	}
	if leftVertexes.Intersects(rightVertexes) {
		return nil, curVertexes, errors.New("conflicting join edges detected")
	}
	curVertexes = leftVertexes.Union(rightVertexes)

	return append(leftEdges, append(rightEdges, curEdges...)...), curVertexes, nil
}

// makeInnerEdge splits an inner join into one edge per conjunct (eq-cond or
// non-eq-cond). We can enlarges the search space by allowing each predicate
// to be applied independently.
// For example: (R1 INNER JOIN R2 on P12) INNER JOIN R3 on P13 and P23
// By spliting the CNF join condition of INNER JOIN, R2 and R3 can also be connected using P23.
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

// makeNonInnerEdge creates a single edge for a non-inner join. Unlike inner
// joins, all predicates are kept together because outer/semi/anti joins are
// atomic — their predicates cannot be applied independently.
func (d *ConflictDetector) makeNonInnerEdge(joinop *logicalop.LogicalJoin, leftVertexes, rightVertexes intset.FastIntSet, leftEdges, rightEdges []*edge) (*edge, error) {
	if len(joinop.NAEQConditions) > 0 {
		return nil, errors.New("NAEQConditions not supported in conflict detector yet")
	}

	nonEQConds := make([]expression.Expression, 0, len(joinop.LeftConditions)+len(joinop.RightConditions)+len(joinop.OtherConditions))
	nonEQConds = append(nonEQConds, joinop.LeftConditions...)
	nonEQConds = append(nonEQConds, joinop.RightConditions...)
	nonEQConds = append(nonEQConds, joinop.OtherConditions...)

	conds := expression.ScalarFuncs2Exprs(joinop.EqualConditions)
	if len(conds) == 0 && len(nonEQConds) == 0 {
		return d.makeEdge(joinop.JoinType, []expression.Expression{}, leftVertexes, rightVertexes, leftEdges, rightEdges), nil
	}

	conds = append(conds, nonEQConds...)

	e := d.makeEdge(joinop.JoinType, conds, leftVertexes, rightVertexes, leftEdges, rightEdges)
	e.eqConds = make([]*expression.ScalarFunction, len(joinop.EqualConditions))
	copy(e.eqConds, joinop.EqualConditions)
	e.nonEQConds = nonEQConds

	return e, nil
}

// makeEdge basically implements the pseudocode for CD-C in paper(Figure-11).
func (d *ConflictDetector) makeEdge(joinType base.JoinType, conds []expression.Expression, leftVertexes, rightVertexes intset.FastIntSet, leftEdges, rightEdges []*edge) *edge {
	e := &edge{
		// Each new edge is appended to either d.innerEdges or d.nonInnerEdges
		// (see below), so their combined length before the append is the next
		// available unique index.
		idx:           uint64(len(d.innerEdges) + len(d.nonInnerEdges)),
		joinType:      joinType,
		leftVertexes:  leftVertexes,
		rightVertexes: rightVertexes,
		leftEdges:     leftEdges,
		rightEdges:    rightEdges,
		skipRules:     d.allInnerJoin,
	}

	// The following implements the first part of the pseudocode for CD-C in the paper(Figure-11):
	// calc the SES(Syntactic Eligibility Set) and init TES(Total Eligibility Set) as SES.
	e.tes = d.calcSES(conds)

	// The following corresponds to the secion 6.2 in the paper(Cross Products and Degenerate Predicates).
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

	// The following implements the conflict rule part of the pseudocode for CD-C in the paper(Figure-11).
	// Conflict rules are generated by checking assoc / l-asscom / r-asscom for every
	// (child, parent) edge pair. Skipped when all joins are inner joins, because
	// inner joins are freely reorderable.
	//
	// TODO: Implement TES extension via conflict rules later.
	// In the section 5.5 of the paper, TES will be extended by conflict rules.
	// The current implementation does not do this step for now.
	// We defer this because the greedy enumerator has a much smaller search space than DP, so the rule-check overhead is low.
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

// rightToLeftRule creates a conflict rule: if child's right vertexes appear in S,
// then child's left vertexes (or the subset intersecting TES) must also be in S.
// The name means "from right to left": the presence of right-side relations
// requires the presence of left-side relations.
func rightToLeftRule(child *edge) *rule {
	rule := &rule{from: child.rightVertexes}
	if child.leftVertexes.Intersects(child.tes) {
		rule.to = child.leftVertexes.Intersection(child.tes)
	} else {
		rule.to = child.leftVertexes
	}
	return rule
}

// leftToRightRule creates a conflict rule: if child's left vertexes appear in S,
// then child's right vertexes (or the subset intersecting TES) must also be in S.
// The name means "from left to right": the presence of left-side relations
// requires the presence of right-side relations.
func leftToRightRule(child *edge) *rule {
	rule := &rule{from: child.leftVertexes}
	if child.rightVertexes.Intersects(child.tes) {
		rule.to = child.rightVertexes.Intersection(child.tes)
	} else {
		rule.to = child.rightVertexes
	}
	return rule
}

func (d *ConflictDetector) calcSES(conds []expression.Expression) intset.FastIntSet {
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
