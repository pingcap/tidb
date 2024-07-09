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

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalShow represents a show plan.
type LogicalShow struct {
	logicalop.LogicalSchemaProducer
	ShowContents

	Extractor base.ShowPredicateExtractor
}

// Init initializes LogicalShow.
func (p LogicalShow) Init(ctx base.PlanContext) *LogicalShow {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeShow, &p, 0)
	return &p
}

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown inherits BaseLogicalPlan.LogicalPlan.<1st> implementation.

// PruneColumns inherits BaseLogicalPlan.LogicalPlan.<2nd> implementation.

// FindBestTask implements the base.LogicalPlan.<3rd> interface.
func (p *LogicalShow) FindBestTask(prop *property.PhysicalProperty, planCounter *base.PlanCounterTp, _ *optimizetrace.PhysicalOptimizeOp) (base.Task, int64, error) {
	return findBestTask4LogicalShow(p, prop, planCounter, nil)
}

// BuildKeyInfo inherits BaseLogicalPlan.LogicalPlan.<4th> implementation.

// PushDownTopN inherits BaseLogicalPlan.LogicalPlan.<5th> implementation.

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implement base.LogicalPlan.<11th> interface.
func (p *LogicalShow) DeriveStats(_ []*property.StatsInfo, selfSchema *expression.Schema, _ []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.StatsInfo() != nil {
		return p.StatsInfo(), nil
	}
	// A fake count, just to avoid panic now.
	p.SetStats(getFakeStats(selfSchema))
	return p.StatsInfo(), nil
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
