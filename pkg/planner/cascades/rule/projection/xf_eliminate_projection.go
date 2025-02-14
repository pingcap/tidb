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

package projection

import (
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	corebase "github.com/pingcap/tidb/pkg/planner/core/base"
)

var _ rule.Rule = &XFEliminateProjection{}

// XFEliminateProjection pull the correlated expression from projection as child of apply.
type XFEliminateProjection struct {
	*rule.BaseRule
}

// NewXFEliminateProjection creates a new EliminateProjection rule.
func NewXFEliminateProjection() *XFEliminateProjection {
	pa1 := pattern.NewPattern(pattern.OperandProjection, pattern.EngineTiDBOnly)
	pa1.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly))
	return &XFEliminateProjection{
		BaseRule: rule.NewBaseRule(rule.XFEliminateProjection, pa1),
	}
}

// ID implement the Rule interface.
func (*XFEliminateProjection) ID() uint {
	return uint(rule.XFEliminateProjection)
}

// XForm implements the Rule interface.
func (*XFEliminateProjection) XForm(projGE corebase.LogicalPlan) ([]corebase.LogicalPlan, bool, error) {
	childProjGE := projGE.Children()[0]
	// The schema len of the two GEs must be the same.
	if childProjGE.Schema().Len() != projGE.Schema().Len() {
		return nil, false, nil
	}

	// the two schema should be the same.
	projCols := projGE.Schema().Columns
	childCols := childProjGE.Schema().Columns
	for i, col := range childCols {
		if !col.EqualColumn(projCols[i]) {
			return nil, false, nil
		}
	}

	// we can return the childProjGE directly.
	// since the childProjGE has its own group, when we return it back
	// it will be reinserted into projection's group again, which will
	// trigger the group merge.
	return []corebase.LogicalPlan{childProjGE}, true, nil
}
