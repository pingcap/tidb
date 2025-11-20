// Copyright 2025 PingCAP, Inc.
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

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

// EliminateUnionAllDualItem is trying to eliminate dual item(rowcount=0) in union all case.
type EliminateUnionAllDualItem struct {
}

// Name implement the LogicalOptRule's name.
func (*EliminateUnionAllDualItem) Name() string {
	return "union_all_eliminate_dual_item"
}

// Optimize implement LogicalOptRule's Optimize.
func (*EliminateUnionAllDualItem) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	planChanged := false
	p, planChanged = unionAllEliminateDualItem(p)
	return p, planChanged, nil
}

func unionAllEliminateDualItem(p base.LogicalPlan) (base.LogicalPlan, bool) {
	if unionAll, ok := p.(*logicalop.LogicalUnionAll); ok {
		newChildren := make([]base.LogicalPlan, 0, len(unionAll.Children()))
		for _, child := range unionAll.Children() {
			// case 1: direct table dual child item.
			if dual, ok := child.(*logicalop.LogicalTableDual); ok && dual.RowCount == 0 {
				continue
			}
			// case 2: indirect projection + table dual item.
			if proj, ok := child.(*logicalop.LogicalProjection); ok {
				if dual, ok := proj.Children()[0].(*logicalop.LogicalTableDual); ok && dual.RowCount == 0 {
					continue
				}
			}
			newChildren = append(newChildren, child)
		}
		if len(newChildren) == 0 {
			dual := logicalop.LogicalTableDual{}.Init(unionAll.SCtx(), 0)
			dual.SetSchema(unionAll.Schema())
			return dual, true
		}
		unionAll.SetChildren(newChildren...)
	}
	flag := false
	for i, child := range p.Children() {
		c, changed := unionAllEliminateDualItem(child)
		if changed {
			p.Children()[i] = c
		}
		flag = flag || changed
	}
	return p, flag
}
