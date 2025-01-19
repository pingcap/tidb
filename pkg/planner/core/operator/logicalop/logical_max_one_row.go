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
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalMaxOneRow checks if a query returns no more than one row.
type LogicalMaxOneRow struct {
	// logical max one row, doesn't have any other attribute to distinguish, use plan id inside.
	BaseLogicalPlan `hash64-equals:"true"`
}

// Init initializes LogicalMaxOneRow.
func (p LogicalMaxOneRow) Init(ctx base.PlanContext, offset int) *LogicalMaxOneRow {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeMaxOneRow, &p, offset)
	return &p
}

// *************************** start implementation of Plan interface ***************************

// Schema implements the Plan.Schema interface.
func (p *LogicalMaxOneRow) Schema() *expression.Schema {
	s := p.Children()[0].Schema().Clone()
	util.ResetNotNullFlag(s, 0, s.Len())
	return s
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (p *LogicalMaxOneRow) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	// MaxOneRow forbids any condition to push down.
	p.BaseLogicalPlan.PredicatePushDown(nil, opt)
	return predicates, p
}

// PruneColumns inherits BaseLogicalPlan.LogicalPlan.<2nd> implementation.

// FindBestTask inherits BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo inherits BaseLogicalPlan.LogicalPlan.<4th> implementation.

// PushDownTopN inherits BaseLogicalPlan.LogicalPlan.<5th> implementation.

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implements base.LogicalPlan.<11th> interface.
func (p *LogicalMaxOneRow) DeriveStats(_ []*property.StatsInfo, selfSchema *expression.Schema, _ []*expression.Schema) (*property.StatsInfo, error) {
	if p.StatsInfo() != nil {
		return p.StatsInfo(), nil
	}
	p.SetStats(getSingletonStats(selfSchema))
	return p.StatsInfo(), nil
}

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties inherits BaseLogicalPlan.LogicalPlan.<13th> implementation.

// ExhaustPhysicalPlans implements base.LogicalPlan.<14th> interface.
func (p *LogicalMaxOneRow) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return utilfuncp.ExhaustPhysicalPlans4LogicalMaxOneRow(p, prop)
}

// ExtractCorrelatedCols inherits BaseLogicalPlan.LogicalPlan.<15th> implementation.

// MaxOneRow inherits BaseLogicalPlan.LogicalPlan.<16th> implementation.

// Children inherits BaseLogicalPlan.LogicalPlan.<17th> implementation.

// SetChildren inherits BaseLogicalPlan.LogicalPlan.<18th> implementation.

// SetChild inherits BaseLogicalPlan.LogicalPlan.<19th> implementation.

// RollBackTaskMap inherits BaseLogicalPlan.LogicalPlan.<20th> implementation.

// CanPushToCop inherits BaseLogicalPlan.LogicalPlan.<21st> implementation.

// ExtractFD inherits BaseLogicalPlan.LogicalPlan.<22nd> implementation.

// GetBaseLogicalPlan inherits BaseLogicalPlan.LogicalPlan.<23rd> implementation.

// ConvertOuterToInnerJoin inherits BaseLogicalPlan.LogicalPlan.<24th> implementation.

// *************************** end implementation of logicalPlan interface ***************************

// Exists and MaxOneRow produce at most one row, so we set the RowCount of stats one.
func getSingletonStats(schema *expression.Schema) *property.StatsInfo {
	ret := &property.StatsInfo{
		RowCount: 1.0,
		ColNDVs:  make(map[int64]float64, schema.Len()),
	}
	for _, col := range schema.Columns {
		ret.ColNDVs[col.UniqueID] = 1
	}
	return ret
}
