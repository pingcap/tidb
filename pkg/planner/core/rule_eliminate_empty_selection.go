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

// EmptySelectionEliminator is a logical optimization rule that removes empty selections
type EmptySelectionEliminator struct{}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (e *EmptySelectionEliminator) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	planChanged := false
	return e.recursivePlan(p), planChanged, nil
}

func (e *EmptySelectionEliminator) recursivePlan(p base.LogicalPlan) base.LogicalPlan {
	for idx, child := range p.Children() {
		// The selection may be useless, check and remove it.
		if sel, ok := child.(*logicalop.LogicalSelection); ok {
			if len(sel.Conditions) == 0 {
				p.SetChild(idx, sel.Children()[0])
			}
			e.recursivePlan(sel.Children()[0])
		} else {
			e.recursivePlan(child)
		}
	}
	return p
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*EmptySelectionEliminator) Name() string {
	return "eliminate_empty_selection"
}
