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

// FindBestPlan is the optimization entrance of the cascades planner. The
// optimization is composed of 2 phases: exploration and implementation.
func FindBestPlan(sctx sessionctx.Context, logical plannercore.LogicalPlan) (plannercore.Plan, error) {
	rootGroup := memo.Convert2Group(logical)
	err := OnPhaseExploration(sctx, rootGroup)
	if err != nil {
		return nil, err
	}
	best, err := onPhaseImplementation(sctx, rootGroup)
	return best, err
}

// OnPhaseExploration is the exploration phase.
func OnPhaseExploration(sctx sessionctx.Context, g *memo.Group) error {
	for !g.Explored {
		err := exploreGroup(sctx, g)
		if err != nil {
			return err
		}
	}
	return nil
}

func exploreGroup(sctx sessionctx.Context, g *memo.Group) error {
	if g.Explored {
		return nil
	}

	g.Explored = true
	for elem := g.Equivalents.Front(); elem != nil; elem.Next() {
		curExpr := elem.Value.(*memo.GroupExpr)
		if curExpr.Explored {
			continue
		}

		// Explore child groups firstly.
		curExpr.Explored = true
		for _, childGroup := range curExpr.Children {
			if err := exploreGroup(sctx, childGroup); err != nil {
				return err
			}
			curExpr.Explored = curExpr.Explored && childGroup.Explored
		}

		eraseCur, err := findMoreEquiv(sctx, g, elem)
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
func findMoreEquiv(sctx sessionctx.Context, g *memo.Group, elem *list.Element) (eraseCur bool, err error) {
	expr := elem.Value.(*memo.GroupExpr)
	for _, rule := range GetTransformationRules(expr.ExprNode) {
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

			newExpr, erase, err := rule.OnTransform(sctx, iter)
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

// fillGroupStats computes Stats property for each Group recursively.
func fillGroupStats(g *memo.Group) (err error) {
	if g.Prop.Stats != nil {
		return nil
	}
	// All GroupExpr in a Group should share same LogicalProperty, so just use
	// first one to compute Stats property.
	elem := g.Equivalents.Front()
	expr := elem.Value.(*memo.GroupExpr)
	childStats := make([]*property.StatsInfo, len(expr.Children))
	for i, childGroup := range expr.Children {
		err = fillGroupStats(childGroup)
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
func onPhaseImplementation(sctx sessionctx.Context, g *memo.Group) (plannercore.Plan, error) {
	prop := &property.PhysicalProperty{
		ExpectedCnt: math.MaxFloat64,
	}
	// TODO replace MaxFloat64 costLimit by variable from sctx, or other sources.
	impl, err := implGroup(g, prop, math.MaxFloat64)
	if err != nil {
		return nil, err
	}
	if impl == nil {
		return nil, plannercore.ErrInternal.GenWithStackByArgs("Can't find a proper physical plan for this query")
	}
	return impl.GetPlan(), nil
}

// implGroup picks one implementation with lowest cost for a Group, which satisfies specified physical property.
func implGroup(g *memo.Group, reqPhysProp *property.PhysicalProperty, costLimit float64) (memo.Implementation, error) {
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
	err := fillGroupStats(g)
	if err != nil {
		return nil, err
	}
	outCount := math.Min(g.Prop.Stats.RowCount, reqPhysProp.ExpectedCnt)
	for elem := g.Equivalents.Front(); elem != nil; elem = elem.Next() {
		curExpr := elem.Value.(*memo.GroupExpr)
		impls, err := implGroupExpr(curExpr, reqPhysProp)
		if err != nil {
			return nil, err
		}
		for _, impl := range impls {
			cumCost = 0.0
			childCosts = childCosts[:0]
			childPlans = childPlans[:0]
			for i, childGroup := range curExpr.Children {
				childImpl, err := implGroup(childGroup, impl.GetPlan().GetChildReqProps(i), costLimit-cumCost)
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
		childImpl, err := implGroup(g, newReqPhysProp, costLimit-enforceCost)
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

func implGroupExpr(cur *memo.GroupExpr, reqPhysProp *property.PhysicalProperty) (impls []memo.Implementation, err error) {
	for _, rule := range GetImplementationRules(cur.ExprNode) {
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
