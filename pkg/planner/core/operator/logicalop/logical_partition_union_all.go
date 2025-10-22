// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logicalop

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalPartitionUnionAll represents the LogicalUnionAll plan is for partition table.
type LogicalPartitionUnionAll struct {
	LogicalUnionAll `hash64-equals:"true"`
}

// Init initializes LogicalPartitionUnionAll.
func (p LogicalPartitionUnionAll) Init(ctx base.PlanContext, offset int) *LogicalPartitionUnionAll {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypePartitionUnion, &p, offset)
	return &p
}

// *************************** start implementation of LogicalPlan interface ***************************

// PruneColumns implements LogicalPlan interface.
func (p *LogicalPartitionUnionAll) PruneColumns(parentUsedCols []*expression.Column) (base.LogicalPlan, error) {
	prunedPlan, err := p.LogicalUnionAll.PruneColumns(parentUsedCols)
	if err != nil {
		return nil, err
	}

	unionAll, ok := prunedPlan.(*LogicalUnionAll)
	if !ok {
		// Return the transformed plan if it's no longer a LogicalUnionAll
		return prunedPlan, nil
	}

	// Update the wrapped LogicalUnionAll with the pruned result
	p.LogicalUnionAll = *unionAll
	return p, nil
}

// PushDownTopN implements LogicalPlan interface.
func (p *LogicalPartitionUnionAll) PushDownTopN(topNLogicalPlan base.LogicalPlan) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	for i, child := range p.Children() {
		var newTopN *LogicalTopN
		if topN != nil {
			newTopN = LogicalTopN{Count: topN.Count + topN.Offset, PreferLimitToCop: topN.PreferLimitToCop}.Init(p.SCtx(), topN.QueryBlockOffset())
			for _, by := range topN.ByItems {
				newTopN.ByItems = append(newTopN.ByItems, &util.ByItems{Expr: by.Expr, Desc: by.Desc})
			}
			// newTopN to push down Union's child
		}
		p.Children()[i] = child.PushDownTopN(newTopN)
	}
	if topN != nil {
		return topN.AttachChild(p)
	}
	return p
}

// *************************** end implementation of LogicalPlan interface ***************************
