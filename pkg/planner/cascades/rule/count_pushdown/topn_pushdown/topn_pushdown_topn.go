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

package topnpd

import (
	"math"

	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	corebase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

var _ rule.Rule = &XFTopNPushDownTopN{}

// XFTopNPushDownTopN pull the correlated expression from projection as child of apply.
type XFTopNPushDownTopN struct {
	*rule.BaseRule
}

// NewXFTopNPushDownTopN creates a new XFLimitPushDownLimit rule.
func NewXFTopNPushDownTopN() *XFTopNPushDownTopN {
	pa1 := pattern.NewPattern(pattern.OperandTopN, pattern.EngineTiDBOnly)
	pa2 := pattern.NewPattern(pattern.OperandTopN, pattern.EngineTiDBOnly)
	pa2.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly))
	pa1.SetChildren(pa2)
	return &XFTopNPushDownTopN{
		BaseRule: rule.NewBaseRule(rule.XFTopNPushDownTopN, pa1),
	}
}

// ID implement the Rule interface.
func (*XFTopNPushDownTopN) ID() uint {
	return uint(rule.XFTopNPushDownTopN)
}

// PreCheck implements the Rule interface.
func (*XFTopNPushDownTopN) PreCheck(topNGE corebase.LogicalPlan) bool {
	topN1 := topNGE.GetWrappedLogicalPlan().(*logicalop.LogicalTopN)
	topN2 := topNGE.Children()[0].GetWrappedLogicalPlan().(*logicalop.LogicalTopN)
	// We can use this rule when the sort columns of parent TopN is a prefix of child TopN.
	if len(topN2.ByItems) < len(topN1.ByItems) {
		return false
	}
	for i := 0; i < len(topN1.ByItems); i++ {
		if !topN1.ByItems[i].Equal(topN1.SCtx().GetExprCtx().GetEvalCtx(), topN2.ByItems[i]) {
			return false
		}
	}
	return true
}

// XForm implements the Rule interface.
func (*XFTopNPushDownTopN) XForm(LimitGE corebase.LogicalPlan) ([]corebase.LogicalPlan, bool, error) {
	topN1 := LimitGE.GetWrappedLogicalPlan().(*logicalop.LogicalTopN)
	topN2 := LimitGE.Children()[0].GetWrappedLogicalPlan().(*logicalop.LogicalTopN)
	childGE := LimitGE.Children()[0].Children()[0]

	if topN2.Count <= topN1.Offset {
		tableDual := logicalop.LogicalTableDual{RowCount: 0}.Init(topN1.SCtx(), topN1.QueryBlockOffset())
		tableDual.SetSchema(topN1.Schema())
		return []corebase.LogicalPlan{tableDual}, true, nil
	}

	// merge two limit together.
	offset := topN2.Offset + topN1.Offset
	count := uint64(math.Min(float64(topN2.Count-topN1.Offset), float64(topN1.Count)))
	newLimit := logicalop.LogicalTopN{
		Offset: offset,
		Count:  count,
		// topN2's byItem is the superset, see precheck.
		ByItems: topN2.ByItems,
	}.Init(topN1.SCtx(), topN1.QueryBlockOffset())
	newLimit.SetSchema(topN1.Schema())
	newLimit.SetChildren(childGE)
	return []corebase.LogicalPlan{newLimit}, true, nil
}
