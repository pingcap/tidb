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

	"github.com/pingcap/errors"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
)

// FindBestPlan is the optimization entrance of the cascades planner. The
// optimization is composed of 2 phases: exploration and implementation.
func FindBestPlan(sctx sessionctx.Context, logical plannercore.LogicalPlan) (plannercore.Plan, error) {
	rootGroup := convert2Group(logical)

	err := onPhaseExploration(sctx, rootGroup)
	if err != nil {
		return nil, err
	}

	best, err := onPhaseImplementation(sctx, rootGroup)
	return best, err
}

// convert2Group converts a logical plan to expression groups.
func convert2Group(node plannercore.LogicalPlan) *Group {
	e := NewGroupExpr(node)
	e.children = make([]*Group, 0, len(node.Children()))
	for _, child := range node.Children() {
		childGroup := convert2Group(child)
		e.children = append(e.children, childGroup)
	}
	return NewGroup(e)
}

func onPhaseExploration(sctx sessionctx.Context, g *Group) error {
	for !g.explored {
		err := exploreGroup(g)
		if err != nil {
			return err
		}
	}
	return nil
}

func exploreGroup(g *Group) error {
	if g.explored {
		return nil
	}

	g.explored = true
	for elem := g.equivalents.Front(); elem != nil; elem.Next() {
		curExpr := elem.Value.(*GroupExpr)
		if curExpr.explored {
			continue
		}

		// Explore child groups firstly.
		curExpr.explored = true
		for _, childGroup := range curExpr.children {
			exploreGroup(childGroup)
			curExpr.explored = curExpr.explored && childGroup.explored
		}

		eraseCur, err := findMoreEquiv(g, elem)
		if err != nil {
			return err
		}
		if eraseCur {
			g.Delete(curExpr)
		}

		g.explored = g.explored && curExpr.explored
	}
	return nil
}

// findMoreEquiv finds and applies the matched transformation rules.
func findMoreEquiv(g *Group, elem *list.Element) (eraseCur bool, err error) {
	expr := elem.Value.(*GroupExpr)
	for _, rule := range GetTransformationRules(expr.exprNode) {
		pattern := rule.GetPattern()
		if !pattern.operand.match(GetOperand(expr.exprNode)) {
			continue
		}
		// Create a binding of the current group expression and the pattern of
		// the transformation rule to enumerate all the possible expressions.
		iter := NewExprIterFromGroupElem(elem, pattern)
		for ; iter != nil && iter.Matched(); iter.Next() {
			if !rule.Match(iter) {
				continue
			}

			newExpr, erase, err := rule.OnTransform(iter)
			if err != nil {
				return false, err
			}

			eraseCur = eraseCur || erase
			if !g.Insert(newExpr) {
				continue
			}

			// If the new group expression is successfully inserted into the
			// current group, we mark the group expression and the group as
			// unexplored to enable the exploration on the new group expression
			// and all the antecedent groups.
			newExpr.explored = false
			g.explored = false
		}
	}
	return eraseCur, nil
}

// onPhaseImplementation starts implementating physical operators from given root Group.
func onPhaseImplementation(sctx sessionctx.Context, g *Group) (plannercore.Plan, error) {
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
	return impl.getPlan(), nil
}

// implGroup picks one implementation with lowest cost for a Group, which satisfies specified physical property.
func implGroup(g *Group, reqPhysProp *property.PhysicalProperty, costLimit float64) (Implementation, error) {
	groupImpl := g.getImpl(reqPhysProp)
	if groupImpl != nil {
		if groupImpl.getCost() <= costLimit {
			return groupImpl, nil
		}
		return nil, nil
	}
	// Handle implementation rules for each equivalent GroupExpr.
	var cumCost float64
	var childCosts []float64
	outCount := math.Min(g.prop.Stats.RowCount, reqPhysProp.ExpectedCnt)
	for elem := g.equivalents.Front(); elem != nil; elem = elem.Next() {
		curExpr := elem.Value.(*GroupExpr)
		impls, err := implGroupExpr(curExpr, reqPhysProp)
		if err != nil {
			return nil, err
		}
		for _, impl := range impls {
			cumCost = 0.0
			childCosts = childCosts[:0]
			for i, childGroup := range curExpr.children {
				childImpl, err := implGroup(childGroup, impl.getPlan().GetChildReqProps(i), costLimit-cumCost)
				if err != nil {
					return nil, err
				}
				if childImpl == nil {
					impl.setCost(math.MaxFloat64)
					break
				}
				childCost := childImpl.getCost()
				childCosts = append(childCosts, childCost)
				cumCost = cumCost + childCost
				impl.getPlan().Children()[i] = childImpl.getPlan()
			}
			if impl.getCost() == math.MaxFloat64 {
				continue
			}
			cumCost = impl.calcCost(outCount, childCosts, curExpr.children...)
			if cumCost > costLimit {
				continue
			}
			if groupImpl == nil || groupImpl.getCost() > cumCost {
				groupImpl = impl
			}
		}
	}
	// Handle enforcing rules for required physical property.
	for _, rule := range GetEnforcerRules(reqPhysProp) {
		newProps := rule.NewProperties(reqPhysProp)
		for _, newReqPhysProp := range newProps {
			childImpl, err := implGroup(g, newReqPhysProp, costLimit)
			if err != nil {
				return nil, err
			}
			if childImpl == nil {
				continue
			}
			impl := rule.OnEnforce(childImpl, g)
			if impl == nil {
				continue
			}
			cumCost = impl.getCost()
			if cumCost > costLimit {
				continue
			}
			if groupImpl == nil || groupImpl.getCost() > cumCost {
				groupImpl = impl
			}
		}
	}
	if groupImpl == nil || groupImpl.getCost() == math.MaxFloat64 {
		return nil, nil
	}
	g.insertImpl(reqPhysProp, groupImpl)
	return groupImpl, nil
}

func implGroupExpr(cur *GroupExpr, reqPhysProp *property.PhysicalProperty) (impls []Implementation, err error) {
	for _, rule := range GetImplementationRules(cur.exprNode) {
		if !rule.Match(cur, reqPhysProp) {
			continue
		}
		impl, err := rule.OnImplement(cur)
		if err != nil {
			return nil, err
		}
		impls = append(impls, impl)
	}
	return impls, nil
}
