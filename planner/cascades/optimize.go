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
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pkg/errors"
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

		eraseCur, err := findMoreEquiv(curExpr, g)
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
func findMoreEquiv(expr *GroupExpr, g *Group) (eraseCur bool, err error) {
	for _, rule := range GetTransformationRules(expr.exprNode) {
		pattern := rule.GetPattern()
		// Create a binding of the current group expression and the pattern of
		// the transformation rule to enumerate all the possible expressions.
		iter := NewExprIterFromGroupExpr(expr, pattern)
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

func onPhaseImplementation(sctx sessionctx.Context, g *Group) (plannercore.Plan, error) {
	return nil, errors.New("the onPhaseImplementation() of the cascades planner is not implemented")
}
