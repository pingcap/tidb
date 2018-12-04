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
	"github.com/pingcap/errors"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
)

// FindBestPlan is the optimization entrance of the cascades planner. The
// optimization is composed of 2 phases: exploration and implementation.
func FindBestPlan(sctx sessionctx.Context, logical plannercore.LogicalPlan) (plannercore.Plan, error) {
	rootGroup, err := convert2Group(logical)
	if err != nil {
		return nil, err
	}

	err = onPhaseExploration(sctx, rootGroup)
	if err != nil {
		return nil, err
	}

	best, err := onPhaseImplementation(sctx, rootGroup)
	return best, err
}

// convert2Group converts a logical plan to expression groups.
func convert2Group(node plannercore.LogicalPlan) (*Group, error) {
	e := NewGroupExpr(node)
	e.children = make([]*Group, 0, len(node.Children()))
	for _, child := range node.Children() {
		childGroup, err := convert2Group(child)
		if err != nil {
			return nil, err
		}
		e.children = append(e.children, childGroup)
	}
	g := NewGroup(e)
	stats, err := node.DeriveStats()
	if err != nil {
		return nil, err
	}
	prop := &property.LogicalProperty{Stats: stats, Schema: node.Schema()}
	g.prop = prop
	return g, nil
}

func onPhaseExploration(sctx sessionctx.Context, g *Group) error {
	return errors.New("the onPhaseExploration() of the cascades planner is not implemented")
}

func onPhaseImplementation(sctx sessionctx.Context, g *Group) (plannercore.Plan, error) {
	return nil, errors.New("the onPhaseImplementation() of the cascades planner is not implemented")
}
