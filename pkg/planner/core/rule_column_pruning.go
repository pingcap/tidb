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

package core

import (
	"context"
	"slices"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// ColumnPruner is used to prune unnecessary columns.
type ColumnPruner struct {
}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (*ColumnPruner) Optimize(_ context.Context, lp base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	lp, err := lp.PruneColumns(slices.Clone(lp.Schema().Columns), opt)
	if err != nil {
		return nil, planChanged, err
	}
	intest.AssertFunc(func() bool {
		return noZeroColumnLayOut(lp)
	}, "After column pruning, some operator got zero row output. Please fix it.")
	return lp, planChanged, nil
}

func noZeroColumnLayOut(p base.LogicalPlan) bool {
	for _, child := range p.Children() {
		if success := noZeroColumnLayOut(child); !success {
			return false
		}
	}
	if p.Schema().Len() == 0 {
		// The p don't hold its schema. So we don't need check itself.
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
