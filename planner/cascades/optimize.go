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
	rootGroup := Convert2Group(logical)

	err := onPhaseExploration(sctx, rootGroup)
	if err != nil {
		return nil, err
	}

	best, err := onPhaseImplementation(sctx, rootGroup)
	return best, err
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
	for _, curExpr := range g.equivalents {
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

// Find and apply the matched transformation rules.
func findMoreEquiv(cur *GroupExpr, curGroup *Group) (eraseCur bool, err error) {
	return false, errors.New("the findMoreEquiv() of the cascades planner is not implemented")
}

func onPhaseImplementation(sctx sessionctx.Context, g *Group) (plannercore.Plan, error) {
	return nil, errors.New("the onPhaseImplementation() of the cascades planner is not implemented")
}
