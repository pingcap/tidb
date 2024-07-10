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
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalCTE is for CTE.
type LogicalCTE struct {
	logicalop.LogicalSchemaProducer

	cte       *CTEClass
	cteAsName model.CIStr
	cteName   model.CIStr
	seedStat  *property.StatsInfo

	onlyUsedAsStorage bool
}

// Init only assigns type and context.
func (p LogicalCTE) Init(ctx base.PlanContext, offset int) *LogicalCTE {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeCTE, &p, offset)
	return &p
}

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits the BaseLogicalPlan.<0th> implementation.

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (p *LogicalCTE) PredicatePushDown(predicates []expression.Expression, _ *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	if p.cte.recursivePartLogicalPlan != nil {
		// Doesn't support recursive CTE yet.
		return predicates, p.Self()
	}
	if !p.cte.isOuterMostCTE {
		return predicates, p.Self()
	}
	pushedPredicates := make([]expression.Expression, len(predicates))
	copy(pushedPredicates, predicates)
	// The filter might change the correlated status of the cte.
	// We forbid the push down that makes the change for now.
	// Will support it later.
	if !p.cte.IsInApply {
		for i := len(pushedPredicates) - 1; i >= 0; i-- {
			if len(expression.ExtractCorColumns(pushedPredicates[i])) == 0 {
				continue
			}
			pushedPredicates = append(pushedPredicates[0:i], pushedPredicates[i+1:]...)
		}
	}
	if len(pushedPredicates) == 0 {
		p.cte.pushDownPredicates = append(p.cte.pushDownPredicates, expression.NewOne())
		return predicates, p.Self()
	}
	newPred := make([]expression.Expression, 0, len(predicates))
	for i := range pushedPredicates {
		newPred = append(newPred, pushedPredicates[i].Clone())
		ResolveExprAndReplace(newPred[i], p.cte.ColumnMap)
	}
	p.cte.pushDownPredicates = append(p.cte.pushDownPredicates, expression.ComposeCNFCondition(p.SCtx().GetExprCtx(), newPred...))
	return predicates, p.Self()
}

// PruneColumns implements the base.LogicalPlan.<2nd> interface.
// LogicalCTE just do an empty function call. It's logical optimize is indivisual phase.
func (p *LogicalCTE) PruneColumns(_ []*expression.Column, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	return p, nil
}

// FindBestTask implements the base.LogicalPlan.<3rd> interface.
func (p *LogicalCTE) FindBestTask(prop *property.PhysicalProperty, counter *base.PlanCounterTp, pop *optimizetrace.PhysicalOptimizeOp) (t base.Task, cntPlan int64, err error) {
	return findBestTask4LogicalCTE(p, prop, counter, pop)
}

// BuildKeyInfo inherits the BaseLogicalPlan.<4th> implementation.

// PushDownTopN implements the base.LogicalPlan.<5th> interface.
func (p *LogicalCTE) PushDownTopN(topNLogicalPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	if topN != nil {
		return topN.AttachChild(p, opt)
	}
	return p
}

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implements the base.LogicalPlan.<11th> interface.
func (p *LogicalCTE) DeriveStats(_ []*property.StatsInfo, selfSchema *expression.Schema, _ []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.StatsInfo() != nil {
		return p.StatsInfo(), nil
	}

	var err error
	if p.cte.seedPartPhysicalPlan == nil {
		// Build push-downed predicates.
		if len(p.cte.pushDownPredicates) > 0 {
			newCond := expression.ComposeDNFCondition(p.SCtx().GetExprCtx(), p.cte.pushDownPredicates...)
			newSel := LogicalSelection{Conditions: []expression.Expression{newCond}}.Init(p.SCtx(), p.cte.seedPartLogicalPlan.QueryBlockOffset())
			newSel.SetChildren(p.cte.seedPartLogicalPlan)
			p.cte.seedPartLogicalPlan = newSel
			p.cte.optFlag |= flagPredicatePushDown
		}
		p.cte.seedPartLogicalPlan, p.cte.seedPartPhysicalPlan, _, err = doOptimize(context.TODO(), p.SCtx(), p.cte.optFlag, p.cte.seedPartLogicalPlan)
		if err != nil {
			return nil, err
		}
	}
	if p.onlyUsedAsStorage {
		p.SetChildren(p.cte.seedPartLogicalPlan)
	}
	resStat := p.cte.seedPartPhysicalPlan.StatsInfo()
	// Changing the pointer so that seedStat in LogicalCTETable can get the new stat.
	*p.seedStat = *resStat
	p.SetStats(&property.StatsInfo{
		RowCount: resStat.RowCount,
		ColNDVs:  make(map[int64]float64, selfSchema.Len()),
	})
	for i, col := range selfSchema.Columns {
		p.StatsInfo().ColNDVs[col.UniqueID] += resStat.ColNDVs[p.cte.seedPartLogicalPlan.Schema().Columns[i].UniqueID]
	}
	if p.cte.recursivePartLogicalPlan != nil {
		if p.cte.recursivePartPhysicalPlan == nil {
			p.cte.recursivePartPhysicalPlan, _, err = DoOptimize(context.TODO(), p.SCtx(), p.cte.optFlag, p.cte.recursivePartLogicalPlan)
			if err != nil {
				return nil, err
			}
		}
		recurStat := p.cte.recursivePartLogicalPlan.StatsInfo()
		for i, col := range selfSchema.Columns {
			p.StatsInfo().ColNDVs[col.UniqueID] += recurStat.ColNDVs[p.cte.recursivePartLogicalPlan.Schema().Columns[i].UniqueID]
		}
		if p.cte.IsDistinct {
			p.StatsInfo().RowCount, _ = cardinality.EstimateColsNDVWithMatchedLen(p.Schema().Columns, p.Schema(), p.StatsInfo())
		} else {
			p.StatsInfo().RowCount += recurStat.RowCount
		}
	}
	return p.StatsInfo(), nil
}

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties inherits BaseLogicalPlan.LogicalPlan.<13th> implementation.

// ExhaustPhysicalPlans implements the base.LogicalPlan.<14th> interface.
func (p *LogicalCTE) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return exhaustPhysicalPlans4LogicalCTE(p, prop)
}

// ExtractCorrelatedCols implements the base.LogicalPlan.<15th> interface.
func (p *LogicalCTE) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := coreusage.ExtractCorrelatedCols4LogicalPlan(p.cte.seedPartLogicalPlan)
	if p.cte.recursivePartLogicalPlan != nil {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4LogicalPlan(p.cte.recursivePartLogicalPlan)...)
	}
	return corCols
}

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
