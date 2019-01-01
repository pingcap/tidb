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

// Cascades implements the cascades planner.
type Cascades struct {
	// SCtx is the session context.
	SCtx sessionctx.Context

	// TransformationRules is the registered transformation rules.
	// NOTE: read-only, should not be modified during the optimization.
	TransformationRules map[memo.Operand][]Transformation

	// ImplementationRules is the registered implementation rules.
	// NOTE: read-only, should not be modified during the optimization.
	ImplementationRules map[memo.Operand][]ImplementationRule

	// mapExpr2Group maps a GroupExpr to its Group.
	mapExpr2Group map[string]*memo.Group
}

func NewDefaultCascadesPlanner(sctx sessionctx.Context) *Cascades {
	return &Cascades{
		SCtx:                sctx,
		ImplementationRules: implementationMap,
		mapExpr2Group:       make(map[string]*memo.Group),
	}
}

// FindBestPlan is the optimization entrance of the cascades planner. The
// optimization is composed of 2 phases: exploration and implementation.
func (c *Cascades) FindBestPlan(logical plannercore.LogicalPlan) (plannercore.Plan, error) {
	rootGroup := memo.Convert2Group(logical)
	err := c.OnPhaseExploration(rootGroup)
	if err != nil {
		return nil, err
	}
	best, err := c.onPhaseImplementation(rootGroup)
	return best, err
}

// OnPhaseExploration is the exploration phase.
func (c *Cascades) OnPhaseExploration(g *memo.Group) error {
	for !g.Explored {
		err := c.exploreGroup(g)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cascades) exploreGroup(g *memo.Group) error {
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
			if err := c.exploreGroup(childGroup); err != nil {
				return err
			}
			curExpr.Explored = curExpr.Explored && childGroup.Explored
		}

		eraseCur, err := c.findMoreEquiv(g, elem)
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
func (c *Cascades) findMoreEquiv(g *memo.Group, elem *list.Element) (eraseCur bool, err error) {
	expr := elem.Value.(*memo.GroupExpr)
	for _, rule := range c.GetTransformationRules(expr.ExprNode) {
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

			newExpr, erase, err := rule.OnTransform(c.SCtx, iter)
			if err != nil {
				return false, err
			}

			// no transform operation is performed.
			if newExpr == nil {
				continue
			}

			eraseCur = eraseCur || erase
			if !g.Insert(newExpr) {
				continue
			}

			// If the new Group expression is successfully inserted into the
			// current Group, we mark the Group expression and the Group as
			// unexplored to enable the exploration on the new Group expression
			// and all the antecedent groups.
			newExpr.Explored = false
			g.Explored = false
		}
	}
	return eraseCur, nil
}

// GetTransformationRules gets the all the candidate transformation rules based
// on the logical plan node.
func (c *Cascades) GetTransformationRules(node plannercore.LogicalPlan) []Transformation {
	return c.TransformationRules[memo.GetOperand(node)]
}

// onPhaseImplementation starts implementation physical operators from given root Group.
func (c *Cascades) onPhaseImplementation(g *memo.Group) (plannercore.Plan, error) {
	prop := &property.PhysicalProperty{
		ExpectedCnt: math.MaxFloat64,
	}
	// TODO replace MaxFloat64 costLimit by variable from c.SCtx, or other sources.
	impl, err := c.implGroup(g, prop, math.MaxFloat64)
	if err != nil {
		return nil, err
	}
	if impl == nil {
		return nil, plannercore.ErrInternal.GenWithStackByArgs("Can't find a proper physical plan for this query")
	}
	return impl.GetPlan(), nil
}

// implGroup picks one implementation with lowest cost for a Group, which satisfies specified physical property.
func (c *Cascades) implGroup(g *memo.Group, reqPhysProp *property.PhysicalProperty, costLimit float64) (memo.Implementation, error) {
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
	err := c.fillGroupStats(g)
	if err != nil {
		return nil, err
	}
	outCount := math.Min(g.Prop.Stats.RowCount, reqPhysProp.ExpectedCnt)
	for elem := g.Equivalents.Front(); elem != nil; elem = elem.Next() {
		curExpr := elem.Value.(*memo.GroupExpr)
		impls, err := c.implGroupExpr(curExpr, reqPhysProp)
		if err != nil {
			return nil, err
		}
		for _, impl := range impls {
			cumCost = 0.0
			childCosts = childCosts[:0]
			childPlans = childPlans[:0]
			for i, childGroup := range curExpr.Children {
				childImpl, err := c.implGroup(childGroup, impl.GetPlan().GetChildReqProps(i), costLimit-cumCost)
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
		childImpl, err := c.implGroup(g, newReqPhysProp, costLimit-enforceCost)
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

// fillGroupStats computes Stats property for each Group recursively.
func (c *Cascades) fillGroupStats(g *memo.Group) (err error) {
	if g.Prop.Stats != nil {
		return nil
	}
	// All GroupExpr in a Group should share same LogicalProperty, so just use
	// first one to compute Stats property.
	elem := g.Equivalents.Front()
	expr := elem.Value.(*memo.GroupExpr)
	childStats := make([]*property.StatsInfo, len(expr.Children))
	for i, childGroup := range expr.Children {
		err = c.fillGroupStats(childGroup)
		if err != nil {
			return err
		}
		childStats[i] = childGroup.Prop.Stats
	}
	planNode := expr.ExprNode
	g.Prop.Stats, err = planNode.DeriveStats(childStats)
	return err
}

func (c *Cascades) implGroupExpr(cur *memo.GroupExpr, reqPhysProp *property.PhysicalProperty) (impls []memo.Implementation, err error) {
	for _, rule := range c.GetImplementationRules(cur.ExprNode) {
		if !rule.Match(cur, reqPhysProp) {
			continue
		}
		impl, err := rule.OnImplement(cur, reqPhysProp)
		if err != nil {
			return nil, err
		}
		impls = append(impls, impl)
	}
	return impls, nil
}

// GetImplementationRules gets the all the candidate implementation rules based
// on the logical plan node.
func (c *Cascades) GetImplementationRules(node plannercore.LogicalPlan) []ImplementationRule {
	return c.ImplementationRules[memo.GetOperand(node)]
}
