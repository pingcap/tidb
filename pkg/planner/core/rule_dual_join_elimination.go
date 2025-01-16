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

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

type DualJoinEliminator struct{}

func (dje *DualJoinEliminator) Optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	for i, children := range p.Children() {
		dje.execOptimize(children, p, i, opt)
	}
	return p, false, nil
}

func (dje *DualJoinEliminator) execOptimize(currentPlan base.LogicalPlan, parentPlan base.LogicalPlan, currentChildIdx int, opt *optimizetrace.LogicalOptimizeOp) {
	parentPlan.IsDual()
	for i, children := range parentPlan.Children() {
		dje.execOptimize(children, parentPlan, i, opt)
	}
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*DualJoinEliminator) Name() string {
	return "dual_join_eliminator"
}
