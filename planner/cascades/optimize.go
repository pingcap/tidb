// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cascades

import (
	"container/list"
	"math"

	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
)

// DefaultOptimizer is the optimizer which contains all of the default
// transformation and implementation rules.
var DefaultOptimizer = NewOptimizer()

// Optimizer is the struct for cascades optimizer.
type Optimizer struct {
	transformationRuleMap map[memo.Operand][]Transformation
	implementationRuleMap map[memo.Operand][]ImplementationRule
}

// NewOptimizer returns a cascades optimizer with default transformation
// rules and implementation rules.
func NewOptimizer() *Optimizer {
	return &Optimizer{
		transformationRuleMap: defaultTransformationMap,
		implementationRuleMap: defaultImplementationMap,
	}
}

// ResetTransformationRules resets the transformationRuleMap of the optimizer, and returns the optimizer.
func (opt *Optimizer) ResetTransformationRules(rules map[memo.Operand][]Transformation) *Optimizer {
	opt.transformationRuleMap = rules
	return opt
}

// ResetImplementationRules resets the implementationRuleMap of the optimizer, and returns the optimizer.
func (opt *Optimizer) ResetImplementationRules(rules map[memo.Operand][]ImplementationRule) *Optimizer {
	opt.implementationRuleMap = rules
	return opt
}

// GetTransformationRules gets the all the candidate transformation rules of the optimizer
// based on the logical plan node.
func (opt *Optimizer) GetTransformationRules(node plannercore.LogicalPlan) []Transformation {
	return opt.transformationRuleMap[memo.GetOperand(node)]
}

// GetImplementationRules gets all the candidate implementation rules of the optimizer
// for the logical plan node.
func (opt *Optimizer) GetImplementationRules(node plannercore.LogicalPlan) []ImplementationRule {
	return opt.implementationRuleMap[memo.GetOperand(node)]
}

// FindBestPlan is the optimization entrance of the cascades planner. The
// optimization is composed of 3 phases: preprocessing, exploration and implementation.
//
//------------------------------------------------------------------------------
// Phase 1: Preprocessing
//------------------------------------------------------------------------------
//
// The target of this phase is to preprocess the plan tree by some heuristic
// rules which should always be beneficial, for example Column Pruning.
//
//------------------------------------------------------------------------------
// Phase 2: Exploration
//------------------------------------------------------------------------------
//
// The target of this phase is to explore all the logically equivalent
// expressions by exploring all the equivalent group expressions of each group.
//
// At the very beginning, there is only one group expression in a Group. After
// applying some transformation rules on certain expressions of the Group, all
// the equivalent expressions are found and stored in the Group. This procedure
// can be regarded as searching for a weak connected component in a directed
// graph, where nodes are expressions and directed edges are the transformation
// rules.
//
//------------------------------------------------------------------------------
// Phase 3: Implementation
//------------------------------------------------------------------------------
//
// The target of this phase is to search the best physical plan for a Group
// which satisfies a certain required physical property.
//
// In this phase, we need to enumerate all the applicable implementation rules
// for each expression in each group under the required physical property. A
// memo structure is used for a group to reduce the repeated search on the same
// required physical property.
func (opt *Optimizer) FindBestPlan(sctx sessionctx.Context, logical plannercore.LogicalPlan) (p plannercore.PhysicalPlan, err error) {
	logical, err = opt.onPhasePreprocessing(sctx, logical)
	if err != nil {
		return nil, err
	}
	rootGroup := convert2Group(logical)
	err = opt.onPhaseExploration(sctx, rootGroup)
	if err != nil {
		return nil, err
	}
	p, err = opt.onPhaseImplementation(sctx, rootGroup)
	if err != nil {
		return nil, err
	}
	err = p.ResolveIndices()
	return p, err
}

// convert2GroupExpr converts a logical plan to a GroupExpr.
func convert2GroupExpr(node plannercore.LogicalPlan) *memo.GroupExpr {
	e := memo.NewGroupExpr(node)
	e.Children = make([]*memo.Group, 0, len(node.Children()))
	for _, child := range node.Children() {
		childGroup := convert2Group(child)
		e.Children = append(e.Children, childGroup)
	}
	return e
}

// convert2Group converts a logical plan to a Group.
func convert2Group(node plannercore.LogicalPlan) *memo.Group {
	e := convert2GroupExpr(node)
	g := memo.NewGroupWithSchema(e, node.Schema())
	// Stats property for `Group` would be computed after exploration phase.
	return g
}

func (opt *Optimizer) onPhasePreprocessing(sctx sessionctx.Context, plan plannercore.LogicalPlan) (plannercore.LogicalPlan, error) {
	err := plan.PruneColumns(plan.Schema().Columns)
	if err != nil {
		return nil, err
	}
	// TODO: Build key info when convert LogicalPlan to GroupExpr.
	plan.BuildKeyInfo()
	return plan, nil
}

func (opt *Optimizer) onPhaseExploration(sctx sessionctx.Context, g *memo.Group) error {
	for !g.Explored {
		err := opt.exploreGroup(g)
		if err != nil {
			return err
		}
	}
	return nil
}

func (opt *Optimizer) exploreGroup(g *memo.Group) error {
	if g.Explored {
		return nil
	}

	g.Explored = true
	for elem := g.Equivalents.Front(); elem != nil; elem = elem.Next() {
		curExpr := elem.Value.(*memo.GroupExpr)
		if curExpr.Explored {
			continue
		}

		// Explore child groups firstly.
		curExpr.Explored = true
		for _, childGroup := range curExpr.Children {
			if err := opt.exploreGroup(childGroup); err != nil {
				return err
			}
			curExpr.Explored = curExpr.Explored && childGroup.Explored
		}

		eraseCur, err := opt.findMoreEquiv(g, elem)
		if err != nil {
			return err
		}
		if eraseCur {
			g.Delete(curExpr)
		}

		g.Explored = g.Explored && curExpr.Explored
	}
	return nil
}

// findMoreEquiv finds and applies the matched transformation rules.
func (opt *Optimizer) findMoreEquiv(g *memo.Group, elem *list.Element) (eraseCur bool, err error) {
	expr := elem.Value.(*memo.GroupExpr)
	for _, rule := range opt.GetTransformationRules(expr.ExprNode) {
		pattern := rule.GetPattern()
		if !pattern.Operand.Match(memo.GetOperand(expr.ExprNode)) {
			continue
		}
		// Create a binding of the current Group expression and the pattern of
		// the transformation rule to enumerate all the possible expressions.
		iter := memo.NewExprIterFromGroupElem(elem, pattern)
		for ; iter != nil && iter.Matched(); iter.Next() {
			if !rule.Match(iter) {
				continue
			}

			newExprs, erase, _, err := rule.OnTransform(iter)
			if err != nil {
				return false, err
			}

			eraseCur = eraseCur || erase
			for _, e := range newExprs {
				if !g.Insert(e) {
					continue
				}
				// If the new Group expression is successfully inserted into the
				// current Group, we mark the Group expression and the Group as
				// unexplored to enable the exploration on the new Group expression
				// and all the antecedent groups.
				e.Explored = false
				g.Explored = false
			}
		}
	}
	return eraseCur, nil
}

// fillGroupStats computes Stats property for each Group recursively.
func (opt *Optimizer) fillGroupStats(g *memo.Group) (err error) {
	if g.Prop.Stats != nil {
		return nil
	}
	// All GroupExpr in a Group should share same LogicalProperty, so just use
	// first one to compute Stats property.
	elem := g.Equivalents.Front()
	expr := elem.Value.(*memo.GroupExpr)
	childStats := make([]*property.StatsInfo, len(expr.Children))
	for i, childGroup := range expr.Children {
		err = opt.fillGroupStats(childGroup)
		if err != nil {
			return err
		}
		childStats[i] = childGroup.Prop.Stats
	}
	planNode := expr.ExprNode
	g.Prop.Stats, err = planNode.DeriveStats(childStats)
	return err
}

// onPhaseImplementation starts implementation physical operators from given root Group.
func (opt *Optimizer) onPhaseImplementation(sctx sessionctx.Context, g *memo.Group) (plannercore.PhysicalPlan, error) {
	prop := &property.PhysicalProperty{
		ExpectedCnt: math.MaxFloat64,
	}
	// TODO replace MaxFloat64 costLimit by variable from sctx, or other sources.
	impl, err := opt.implGroup(g, prop, math.MaxFloat64)
	if err != nil {
		return nil, err
	}
	if impl == nil {
		return nil, plannercore.ErrInternal.GenWithStackByArgs("Can't find a proper physical plan for this query")
	}
	return impl.GetPlan(), nil
}

// implGroup finds the best Implementation which satisfies the required
// physical property for a Group. The best Implementation should have the
// lowest cost among all the applicable Implementations.
//
// g:			the Group to be implemented.
// reqPhysProp: the required physical property.
// costLimit:   the maximum cost of all the Implementations.
func (opt *Optimizer) implGroup(g *memo.Group, reqPhysProp *property.PhysicalProperty, costLimit float64) (memo.Implementation, error) {
	groupImpl := g.GetImpl(reqPhysProp)
	if groupImpl != nil {
		if groupImpl.GetCost() <= costLimit {
			return groupImpl, nil
		}
		return nil, nil
	}
	// Handle implementation rules for each equivalent GroupExpr.
	var cumCost float64
	var childCosts []float64
	var childPlans []plannercore.PhysicalPlan
	err := opt.fillGroupStats(g)
	if err != nil {
		return nil, err
	}
	outCount := math.Min(g.Prop.Stats.RowCount, reqPhysProp.ExpectedCnt)
	for elem := g.Equivalents.Front(); elem != nil; elem = elem.Next() {
		curExpr := elem.Value.(*memo.GroupExpr)
		impls, err := opt.implGroupExpr(curExpr, reqPhysProp)
		if err != nil {
			return nil, err
		}
		for _, impl := range impls {
			cumCost = 0.0
			childCosts = childCosts[:0]
			childPlans = childPlans[:0]
			for i, childGroup := range curExpr.Children {
				childImpl, err := opt.implGroup(childGroup, impl.GetPlan().GetChildReqProps(i), costLimit-cumCost)
				if err != nil {
					return nil, err
				}
				if childImpl == nil {
					impl.SetCost(math.MaxFloat64)
					break
				}
				childCost := childImpl.GetCost()
				childCosts = append(childCosts, childCost)
				cumCost += childCost
				childPlans = append(childPlans, childImpl.GetPlan())
			}
			if impl.GetCost() == math.MaxFloat64 {
				continue
			}
			cumCost = impl.CalcCost(outCount, childCosts, curExpr.Children...)
			if cumCost > costLimit {
				continue
			}
			if groupImpl == nil || groupImpl.GetCost() > cumCost {
				impl.GetPlan().SetChildren(childPlans...)
				groupImpl = impl
				costLimit = cumCost
			}
		}
	}
	// Handle enforcer rules for required physical property.
	for _, rule := range GetEnforcerRules(reqPhysProp) {
		newReqPhysProp := rule.NewProperty(reqPhysProp)
		enforceCost := rule.GetEnforceCost(outCount)
		childImpl, err := opt.implGroup(g, newReqPhysProp, costLimit-enforceCost)
		if err != nil {
			return nil, err
		}
		if childImpl == nil {
			continue
		}
		impl := rule.OnEnforce(reqPhysProp, childImpl)
		cumCost = enforceCost + childImpl.GetCost()
		impl.SetCost(cumCost)
		if groupImpl == nil || groupImpl.GetCost() > cumCost {
			groupImpl = impl
			costLimit = cumCost
		}
	}
	if groupImpl == nil || groupImpl.GetCost() == math.MaxFloat64 {
		return nil, nil
	}
	g.InsertImpl(reqPhysProp, groupImpl)
	return groupImpl, nil
}

func (opt *Optimizer) implGroupExpr(cur *memo.GroupExpr, reqPhysProp *property.PhysicalProperty) (impls []memo.Implementation, err error) {
	for _, rule := range opt.GetImplementationRules(cur.ExprNode) {
		if !rule.Match(cur, reqPhysProp) {
			continue
		}
		impl, err := rule.OnImplement(cur, reqPhysProp)
		if err != nil {
			return nil, err
		}
		if impl != nil {
			impls = append(impls, impl)
		}
	}
	return impls, nil
}
