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

package limitpd

import (
	"math"

	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	corebase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

var _ rule.Rule = &XFLimitPushDownLimit{}

// XFLimitPushDownLimit pull the correlated expression from projection as child of apply.
type XFLimitPushDownLimit struct {
	*rule.BaseRule
}

// NewXFLimitPushDownLimit creates a new XFLimitPushDownLimit rule.
func NewXFLimitPushDownLimit() *XFLimitPushDownLimit {
	pa1 := pattern.NewPattern(pattern.OperandLimit, pattern.EngineTiDBOnly)
	pa2 := pattern.NewPattern(pattern.OperandLimit, pattern.EngineTiDBOnly)
	pa2.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly))
	pa1.SetChildren(pa2)
	return &XFLimitPushDownLimit{
		BaseRule: rule.NewBaseRule(rule.XFLimitPushDownLimit, pa1),
	}
}

// ID implement the Rule interface.
func (*XFLimitPushDownLimit) ID() uint {
	return uint(rule.XFLimitPushDownLimit)
}

// PreCheck implements the Rule interface.
func (*XFLimitPushDownLimit) PreCheck(_ corebase.LogicalPlan) bool { return true }

// XForm implements the Rule interface.
func (*XFLimitPushDownLimit) XForm(LimitGE corebase.LogicalPlan) ([]corebase.LogicalPlan, bool, error) {
	limit1 := LimitGE.GetWrappedLogicalPlan().(*logicalop.LogicalLimit)
	limit2 := LimitGE.Children()[0].GetWrappedLogicalPlan().(*logicalop.LogicalLimit)
	childGE := LimitGE.Children()[0].Children()[0]

	if limit2.Count <= limit1.Offset {
		tableDual := logicalop.LogicalTableDual{RowCount: 0}.Init(limit1.SCtx(), limit1.QueryBlockOffset())
		tableDual.SetSchema(limit1.Schema())
		return []corebase.LogicalPlan{tableDual}, true, nil
	}

	// merge two limit together.
	offset := limit2.Offset + limit1.Offset
	count := uint64(math.Min(float64(limit2.Count-limit1.Offset), float64(limit1.Count)))
	newLimit := logicalop.LogicalLimit{
		Offset: offset,
		Count:  count,
	}.Init(limit1.SCtx(), limit1.QueryBlockOffset())
	newLimit.SetSchema(limit1.Schema())
	newLimit.SetChildren(childGE)
	return []corebase.LogicalPlan{newLimit}, true, nil
}
