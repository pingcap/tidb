// Copyright 2023 PingCAP, Inc.
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

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// PushDownSequenceSolver is used to push down sequence.
type PushDownSequenceSolver struct {
}

// Name implements the base.LogicalOptRule.<1st> interface.
func (*PushDownSequenceSolver) Name() string {
	return "push_down_sequence"
}

// Optimize implements the base.LogicalOptRule.<0th> interface.
func (pdss *PushDownSequenceSolver) Optimize(_ context.Context, lp base.LogicalPlan, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	return pdss.recursiveOptimize(nil, lp), planChanged, nil
}

func (pdss *PushDownSequenceSolver) recursiveOptimize(pushedSequence *logicalop.LogicalSequence, lp base.LogicalPlan) base.LogicalPlan {
	_, ok := lp.(*logicalop.LogicalSequence)
	if !ok && pushedSequence == nil {
		newChildren := make([]base.LogicalPlan, 0, len(lp.Children()))
		for _, child := range lp.Children() {
			newChildren = append(newChildren, pdss.recursiveOptimize(nil, child))
		}
		lp.SetChildren(newChildren...)
		return lp
	}
	switch x := lp.(type) {
	case *logicalop.LogicalSequence:
		if pushedSequence == nil {
			pushedSequence = logicalop.LogicalSequence{}.Init(lp.SCtx(), lp.QueryBlockOffset())
			pushedSequence.SetChildren(lp.Children()...)
			return pdss.recursiveOptimize(pushedSequence, lp.Children()[len(lp.Children())-1])
		}
		childLen := x.ChildLen()
		mainQuery := x.Children()[childLen-1]
		allCTEs := make([]base.LogicalPlan, 0, childLen+pushedSequence.ChildLen()-2)
		allCTEs = append(allCTEs, pushedSequence.Children()[:pushedSequence.ChildLen()-1]...)
		allCTEs = append(allCTEs, x.Children()[:childLen-1]...)
		pushedSequence = logicalop.LogicalSequence{}.Init(lp.SCtx(), lp.QueryBlockOffset())
		pushedSequence.SetChildren(append(allCTEs, mainQuery)...)
		return pdss.recursiveOptimize(pushedSequence, mainQuery)
	case *logicalop.DataSource, *logicalop.LogicalAggregation, *logicalop.LogicalCTE:
		pushedSequence.SetChild(pushedSequence.ChildLen()-1, pdss.recursiveOptimize(nil, lp))
		return pushedSequence
	default:
		if len(lp.Children()) > 1 {
			pushedSequence.SetChild(pushedSequence.ChildLen()-1, lp)
			return pushedSequence
		}
		lp.SetChildren(pdss.recursiveOptimize(pushedSequence, lp.Children()[0]))
		return lp
	}
}
