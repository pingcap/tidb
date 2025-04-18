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

// LogicalShowDDLJobs is for showing DDL job list.
type LogicalShowDDLJobs struct {
	LogicalSchemaProducer `hash64-equals:"true"`

	JobNumber int64
}

// Init initializes LogicalShowDDLJobs.
func (p LogicalShowDDLJobs) Init(ctx base.PlanContext) *LogicalShowDDLJobs {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeShowDDLJobs, &p, 0)
	return &p
}

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits the BaseLogicalPlan.<0th> interface.

// PredicatePushDown inherits the BaseLogicalPlan.<1st> interface.

// PruneColumns inherits the BaseLogicalPlan.<2nd> interface.

// FindBestTask implements the base.LogicalPlan.<3rd> interface.
func (p *LogicalShowDDLJobs) FindBestTask(prop *property.PhysicalProperty, planCounter *base.PlanCounterTp, _ *optimizetrace.PhysicalOptimizeOp) (base.Task, int64, error) {
	return utilfuncp.FindBestTask4LogicalShowDDLJobs(p, prop, planCounter, nil)
}

// BuildKeyInfo inherits the BaseLogicalPlan.<4th> interface.

// PushDownTopN inherits the BaseLogicalPlan.<5th> interface.

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implements the base.LogicalPlan.<11th> interface.
func (p *LogicalShowDDLJobs) DeriveStats(_ []*property.StatsInfo, selfSchema *expression.Schema, _ []*expression.Schema, reloads []bool) (*property.StatsInfo, bool, error) {
	var reload bool
	if len(reloads) == 1 {
		reload = reloads[0]
	}
	if !reload && p.StatsInfo() != nil {
		return p.StatsInfo(), false, nil
	}
	// A fake count, just to avoid panic now.
	p.SetStats(getFakeStats(selfSchema))
	return p.StatsInfo(), true, nil
}

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties inherits BaseLogicalPlan.LogicalPlan.<13th> implementation.

// ExhaustPhysicalPlans inherits BaseLogicalPlan.LogicalPlan.<14th> implementation.

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
