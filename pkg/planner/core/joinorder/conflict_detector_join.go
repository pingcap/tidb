// Copyright 2024 PingCAP, Inc.
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

	"github.com/cockroachdb/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

func (r *CheckConnectionResult) Connected() bool {
	return len(r.appliedInnerEdges) > 0 || r.appliedNonInnerEdge != nil
}

// NoEQEdge checks if there is no EQ edge between two nodes.
func (r *CheckConnectionResult) NoEQEdge() bool {
	return !r.hasEQCond
}

// CheckConnection tests whether any edge can validly connect node1 and node2.
// It's corresponding to the pseudocode for APPLICABLE(b/c) in the paper(Figure-9).
// The basic idea is: It collects all applicable inner edges (there can be many) and at most one
// non-inner edge. The result is later passed to MakeJoin() to build the plan.
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
		if e.checkInnerEdgeApplicable(node1, node2) {
			result.appliedInnerEdges = append(result.appliedInnerEdges, e)
			result.hasEQCond = result.hasEQCond || len(e.eqConds) > 0
		}
	}
	for _, e := range d.nonInnerEdges {
		if node1.checkUsedEdges(e.idx) || node2.checkUsedEdges(e.idx) {
			continue
		}
		if e.checkNonInnerEdgeApplicable(node1, node2) {
			if result.appliedNonInnerEdge != nil {
				return nil, errors.New("multiple non-inner edges applied between two nodes")
			}
			result.appliedNonInnerEdge = e
			result.hasEQCond = result.hasEQCond || len(e.eqConds) > 0
		}
	}
	return result, nil
}

// checkInnerEdgeApplicable validates that this inner edge can connect node1 and node2.
// For inner joins the two sides are symmetric, so we only require:
//   - All conflict rules are satisfied.
//   - TES ⊆ (S1 ∪ S2): all required relations are present.
//   - TES ∩ S1 ≠ ∅ and TES ∩ S2 ≠ ∅: the edge truly connects both sides.
func (e *edge) checkInnerEdgeApplicable(node1, node2 *Node) bool {
	if !e.skipRules && !e.checkRules(node1, node2) {
		return false
	}
	return e.tes.SubsetOf(node1.bitSet.Union(node2.bitSet)) &&
		e.tes.Intersects(node1.bitSet) &&
		e.tes.Intersects(node2.bitSet)
}

// checkNonInnerEdgeApplicable validates that this non-inner edge can connect node1 (left)
// and node2 (right). Beyond the inner-edge checks, it enforces side semantics:
// the original left vertexes (∩ TES) must land entirely in node1, and the
// original right vertexes (∩ TES) must land entirely in node2. This prevents
// the outer-join's preserved side from being split across the two inputs.
func (e *edge) checkNonInnerEdgeApplicable(node1, node2 *Node) bool {
	if !e.skipRules && !e.checkRules(node1, node2) {
		return false
	}
	return e.leftVertexes.Intersection(e.tes).SubsetOf(node1.bitSet) &&
		e.rightVertexes.Intersection(e.tes).SubsetOf(node2.bitSet) &&
		e.tes.Intersects(node1.bitSet) &&
		e.tes.Intersects(node2.bitSet)
}

// checkRules verifies that all conflict rules are satisfied for the candidate
// set S = S1 ∪ S2. A rule {from → to} fails if from ∩ S ≠ ∅ but to ⊄ S.
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
	// Note that non-inner edges should be processed first then inner edges,
	// because inner joins can be appended to existing non-inner join as selections.
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
		// Append selections to existing join.
		// For example: (R1 LEFT JOIN R2 ON R1.c1 = R2.c1) INNER JOIN R3 ON R2.c2 = R3.c2 and (R1.c3 = R2.c3 and R1.c4 IS NULL)
		// there will be two edges for R1 and R2, one is R1.c1 = R2.c1, the other is R1.c3 = R2.c3 and R1.c4 IS NULL,
		// the second edge is a INNER JOIN edge, and we will append it as selection to the first LEFT JOIN edge.
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
	join.SetSchema(expression.MergeSchema(left.Schema(), right.Schema()))
	join.SetChildren(left, right)
	SetNewJoinWithHint(join, vertexHints)
	return join, nil
}

// HasRemainingEdges checks if there are remaining edges not in usedEdges.
func (d *ConflictDetector) HasRemainingEdges(usedEdges map[uint64]struct{}) (remaining bool) {
	d.iterateEdges(func(e *edge) bool {
		if len(e.eqConds) > 0 || len(e.nonEQConds) > 0 {
			if _, ok := usedEdges[e.idx]; !ok {
				remaining = true
				return false
			}
		}
		return true
	})
	return
}
