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

package decorrelateapply

import (
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	corebase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

var _ rule.Rule = &XFDeCorrelateApply{}

// XFDeCorrelateApply pull the correlated expression from projection as child of apply.
type XFDeCorrelateApply struct {
	*rule.BaseRule
}

// NewXFDeCorrelateApply creates a new XFDeCorrelateApply rule.
func NewXFDeCorrelateApply() *XFDeCorrelateApply {
	pa := pattern.NewPattern(pattern.OperandApply, pattern.EngineTiDBOnly)
	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly), pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly))
	return &XFDeCorrelateApply{
		BaseRule: rule.NewBaseRule(rule.XFDeCorrelateApply, pa),
	}
}

// Match implements the Rule interface.
func (*XFDeCorrelateApply) Match(_ corebase.LogicalPlan) bool {
	return true
}

// XForm implements the Rule interface.
func (*XFDeCorrelateApply) XForm(applyGE corebase.LogicalPlan) ([]corebase.LogicalPlan, error) {
	children := applyGE.Children()
	outerPlanGE := children[0]
	innerPlanGE := children[1]
	// don't modify the apply op's CorCols in-place, which will change the hash64, apply should be re-inserted into the group otherwise.
	corCols := coreusage.ExtractCorColumnsBySchema4LogicalPlan(innerPlanGE.GetWrappedLogicalPlan(), outerPlanGE.GetWrappedLogicalPlan().Schema())
	if len(corCols) == 0 {
		apply := applyGE.GetWrappedLogicalPlan().(*logicalop.LogicalApply)
		// If the inner plan is non-correlated, this apply will be simplified to join.
		clonedJoin := apply.LogicalJoin.LogicalJoinShallowRef()
		// ReAlloc4Cascades is to re-alloc the plan factors for cascades.
		// reset the tp and self, stats to nil, recreate the task map, re-alloc the plan id and so on.
		// set the new GE's stats to nil, since the inherited stats is not precious, which will be filled in physicalOpt.
		clonedJoin.ReAlloc4Cascades(plancodec.TypeJoin, clonedJoin)
		intest.Assert(clonedJoin.Children() != nil)
		return []corebase.LogicalPlan{clonedJoin}, nil
	}
	return nil, nil
}
