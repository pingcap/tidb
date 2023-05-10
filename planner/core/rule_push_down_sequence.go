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

import "context"

type pushDownSequenceSolver struct {
}

func (*pushDownSequenceSolver) name() string {
	return "push_down_sequence"
}

func (pdss *pushDownSequenceSolver) optimize(_ context.Context, lp LogicalPlan, _ *logicalOptimizeOp) (LogicalPlan, error) {
	return pdss.recursiveOptimize(nil, lp), nil
}

func (pdss *pushDownSequenceSolver) recursiveOptimize(pushedSequence *LogicalSequence, lp LogicalPlan) LogicalPlan {
	_, ok := lp.(*LogicalSequence)
	if !ok && pushedSequence == nil {
		newChildren := make([]LogicalPlan, 0, len(lp.Children()))
		for _, child := range lp.Children() {
			newChildren = append(newChildren, pdss.recursiveOptimize(nil, child))
		}
		lp.SetChildren(newChildren...)
		return lp
	}
	switch x := lp.(type) {
	case *LogicalSequence:
		if pushedSequence == nil {
			pushedSequence = LogicalSequence{}.Init(lp.SCtx(), lp.SelectBlockOffset())
			pushedSequence.SetChildren(lp.Children()...)
			return pdss.recursiveOptimize(pushedSequence, lp.Children()[len(lp.Children())-1])
		}
		childLen := len(x.children)
		mainQuery := x.children[childLen-1]
		allCTEs := make([]LogicalPlan, 0, childLen+len(pushedSequence.children)-2)
		allCTEs = append(allCTEs, pushedSequence.children[:len(pushedSequence.children)-1]...)
		allCTEs = append(allCTEs, x.children[:childLen-1]...)
		pushedSequence = LogicalSequence{}.Init(lp.SCtx(), lp.SelectBlockOffset())
		pushedSequence.SetChildren(append(allCTEs, mainQuery)...)
		return pdss.recursiveOptimize(pushedSequence, mainQuery)
	case *DataSource, *LogicalAggregation, *LogicalCTE:
		pushedSequence.SetChild(len(pushedSequence.children)-1, pdss.recursiveOptimize(nil, lp))
		return pushedSequence
	default:
		if len(lp.Children()) > 1 {
			pushedSequence.SetChild(len(pushedSequence.children)-1, lp)
			return pushedSequence
		}
		lp.SetChildren(pdss.recursiveOptimize(pushedSequence, lp.Children()[0]))
		return lp
	}
}
