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
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalSequence is used to mark the CTE producer in the main query tree.
// Its last child is main query. The previous children are cte producers.
// And there might be dependencies between the CTE producers:
//
//	Suppose that the sequence has 4 children, naming c0, c1, c2, c3.
//	From the definition, c3 is the main query. c0, c1, c2 are CTE producers.
//	It's possible that c1 references c0, c2 references c1 and c2.
//	But it's not possible that c0 references c1 or c2.
//
// We use this property to do complex optimizations for CTEs.
type LogicalSequence struct {
	// logical sequence doesn't have any other attribute to distinguish, use plan id inside.
	BaseLogicalPlan `hash64-equals:"true"`
}

// Init initializes LogicalSequence
func (p LogicalSequence) Init(ctx base.PlanContext, offset int) *LogicalSequence {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeSequence, &p, offset)
	return &p
}

// *************************** start implementation of Plan interface ***************************

// Schema returns its last child(which is the main query plan)'s schema.
func (p *LogicalSequence) Schema() *expression.Schema {
	return p.Children()[p.ChildLen()-1].Schema()
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits the BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown implements the base.LogicalPlan.<1st> interface.
// Currently, we only maintain the main query tree.
func (p *LogicalSequence) PredicatePushDown(predicates []expression.Expression, op *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	lastIdx := p.ChildLen() - 1
	remained, newLastChild := p.Children()[lastIdx].PredicatePushDown(predicates, op)
	p.SetChild(lastIdx, newLastChild)
	return remained, p
}

// PruneColumns implements the base.LogicalPlan.<2nd> interface.
func (p *LogicalSequence) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	var err error
	p.Children()[p.ChildLen()-1], err = p.Children()[p.ChildLen()-1].PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// FindBestTask inherits BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo inherits BaseLogicalPlan.LogicalPlan.<4th> implementation.

// PushDownTopN inherits BaseLogicalPlan.LogicalPlan.<5th> implementation.

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implements the base.LogicalPlan.<11th> interface.
func (p *LogicalSequence) DeriveStats(childStats []*property.StatsInfo, _ *expression.Schema, _ []*expression.Schema) (*property.StatsInfo, error) {
	p.SetStats(childStats[len(childStats)-1])
	return p.StatsInfo(), nil
}

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties inherits BaseLogicalPlan.LogicalPlan.<13th> implementation.

// ExhaustPhysicalPlans implements the base.LogicalPlan.<14th> interface.
func (p *LogicalSequence) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return utilfuncp.ExhaustPhysicalPlans4LogicalSequence(p, prop)
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
