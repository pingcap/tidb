// Copyright 2016 PingCAP, Inc.
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

package rule

import (
	"context"
	"slices"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// ColumnPruner is used to prune unnecessary columns.
type ColumnPruner struct {
}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (*ColumnPruner) Optimize(_ context.Context, lp base.LogicalPlan) (base.LogicalPlan, bool, error) {
	planChanged := false
	lp, err := lp.PruneColumns(slices.Clone(lp.Schema().Columns))
	if err != nil {
		return nil, planChanged, err
	}
	planChanged, err = pruneCTEColumns(lp)
	if err != nil {
		return nil, planChanged, err
	}
	intest.AssertFunc(func() bool {
		return noUnexpectedZeroColumnSchema(lp)
	}, "After column pruning, some operator got an unexpected zero-column output schema. Please fix it.")
	return lp, planChanged, nil
}

// noUnexpectedZeroColumnSchema checks the post-pruning invariant that a logical
// operator should not expose an empty output schema.
//
// Two cases are exempt:
//  1. Some operators reuse their first child's schema object instead of owning a
//     separate schema, so an empty schema check on the node itself is not useful.
//  2. LogicalTableDual can legitimately end up with zero output columns.
func noUnexpectedZeroColumnSchema(p base.LogicalPlan) bool {
	for _, child := range p.Children() {
		if success := noUnexpectedZeroColumnSchema(child); !success {
			return false
		}
	}
	if p.Schema().Len() == 0 {
		// This node reuses its first child's schema object rather than owning one.
		if len(p.Children()) > 0 && p.Schema() == p.Children()[0].Schema() {
			return true
		}
		_, ok := p.(*logicalop.LogicalTableDual)
		if !ok {
			return false
		}
	}
	return true
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*ColumnPruner) Name() string {
	return "column_prune"
}

// pruneCTEColumns crosses the shared-producer boundary after ordinary pruning
// has collected consumer requirements. Producers are processed before their
// dependencies, so nested CTE consumers contribute to the same union of demands.
func pruneCTEColumns(root base.LogicalPlan) (bool, error) {
	consumers := make(map[*logicalop.CTEClass][]*logicalop.LogicalCTE)
	blocked := make(map[*logicalop.CTEClass]bool)
	seenPlans := make(map[base.LogicalPlan]bool)
	var producers []*logicalop.LogicalCTE
	var collect func(base.LogicalPlan)
	collect = func(p base.LogicalPlan) {
		if p == nil || seenPlans[p] {
			return
		}
		seenPlans[p] = true
		if cte, ok := p.(*logicalop.LogicalCTE); ok && cte.Cte != nil {
			_, seen := consumers[cte.Cte]
			consumers[cte.Cte] = append(consumers[cte.Cte], cte)
			if cte.OnlyUsedAsStorage {
				blocked[cte.Cte] = true
			}
			if !seen {
				if cte.Cte.SeedPartPhysicalPlan == nil {
					collect(cte.Cte.SeedPartLogicalPlan)
					collect(cte.Cte.RecursivePartLogicalPlan)
				}
				producers = append(producers, cte)
			}
		}
		for _, child := range p.Children() {
			collect(child)
		}
	}
	collect(root)
	changed := false
	for i := len(producers) - 1; i >= 0; i-- {
		cte := producers[i]
		if blocked[cte.Cte] {
			cte.Cte.ColumnUsage = nil
		}
		pruned, err := cte.PruneProducerColumns()
		if err != nil {
			return changed, err
		}
		changed = changed || pruned
	}
	for _, refs := range consumers {
		for _, cte := range refs {
			cte.SyncColumnTypes()
		}
	}
	return changed, nil
}
