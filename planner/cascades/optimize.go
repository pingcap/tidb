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
	return errors.New("the onPhaseExploration() of the cascades planner is not implemented")
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
	var childPlans []plannercore.PhysicalPlan
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
			childPlans = childPlans[:0]
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
				cumCost += childCost
				childPlans = append(childPlans, childImpl.getPlan())
			}
			if impl.getCost() == math.MaxFloat64 {
				continue
			}
			cumCost = impl.calcCost(outCount, childCosts, curExpr.children...)
			if cumCost > costLimit {
				continue
			}
			if groupImpl == nil || groupImpl.getCost() > cumCost {
				impl.getPlan().SetChildren(childPlans...)
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
		cumCost = enforceCost + childImpl.getCost()
		impl.setCost(cumCost)
		if groupImpl == nil || groupImpl.getCost() > cumCost {
			groupImpl = impl
			costLimit = cumCost
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
		impl, err := rule.OnImplement(cur, reqPhysProp)
		if err != nil {
			return nil, err
		}
		impls = append(impls, impl)
	}
	return impls, nil
}
