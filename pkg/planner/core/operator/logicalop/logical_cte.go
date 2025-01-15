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
	"context"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// LogicalCTE is for CTE.
type LogicalCTE struct {
	LogicalSchemaProducer

	Cte       *CTEClass
	CteAsName ast.CIStr
	CteName   ast.CIStr
	SeedStat  *property.StatsInfo

	OnlyUsedAsStorage bool
}

// Init only assigns type and context.
func (p LogicalCTE) Init(ctx base.PlanContext, offset int) *LogicalCTE {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeCTE, &p, offset)
	return &p
}

// CTEClass holds the information and plan for a CTE. Most of the fields in this struct are the same as cteInfo.
// But the cteInfo is used when building the plan, and CTEClass is used also for building the executor.
type CTEClass struct {
	// The union between seed part and recursive part is DISTINCT or DISTINCT ALL.
	IsDistinct bool
	// SeedPartLogicalPlan and RecursivePartLogicalPlan are the logical plans for the seed part and recursive part of this CTE.
	SeedPartLogicalPlan base.LogicalPlan
	// RecursivePartLogicalPlan is nil if this CTE is not a recursive CTE.
	RecursivePartLogicalPlan base.LogicalPlan
	// SeedPartPhysicalPlan and RecursivePartPhysicalPlan are the physical plans for the seed part and recursive part of this CTE.
	SeedPartPhysicalPlan      base.PhysicalPlan
	RecursivePartPhysicalPlan base.PhysicalPlan
	// storageID for this CTE.
	IDForStorage int
	// OptFlag is the OptFlag for the whole CTE.
	OptFlag   uint64
	HasLimit  bool
	LimitBeg  uint64
	LimitEnd  uint64
	IsInApply bool
	// PushDownPredicates may be push-downed by different references.
	PushDownPredicates []expression.Expression
	ColumnMap          map[string]*expression.Column
	IsOuterMostCTE     bool
}

const emptyCTEClassSize = int64(unsafe.Sizeof(CTEClass{}))

// MemoryUsage return the memory usage of CTEClass
func (cc *CTEClass) MemoryUsage() (sum int64) {
	if cc == nil {
		return
	}

	sum = emptyCTEClassSize
	if cc.SeedPartPhysicalPlan != nil {
		sum += cc.SeedPartPhysicalPlan.MemoryUsage()
	}
	if cc.RecursivePartPhysicalPlan != nil {
		sum += cc.RecursivePartPhysicalPlan.MemoryUsage()
	}

	for _, expr := range cc.PushDownPredicates {
		sum += expr.MemoryUsage()
	}
	for key, val := range cc.ColumnMap {
		sum += size.SizeOfString + int64(len(key)) + size.SizeOfPointer + val.MemoryUsage()
	}
	return
}

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits the BaseLogicalPlan.<0th> implementation.

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (p *LogicalCTE) PredicatePushDown(predicates []expression.Expression, _ *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	if p.Cte.RecursivePartLogicalPlan != nil {
		// Doesn't support recursive CTE yet.
		return predicates, p.Self()
	}
	if !p.Cte.IsOuterMostCTE {
		return predicates, p.Self()
	}
	pushedPredicates := make([]expression.Expression, len(predicates))
	copy(pushedPredicates, predicates)
	// The filter might change the correlated status of the cte.
	// We forbid the push down that makes the change for now.
	// Will support it later.
	if !p.Cte.IsInApply {
		for i := len(pushedPredicates) - 1; i >= 0; i-- {
			if len(expression.ExtractCorColumns(pushedPredicates[i])) == 0 {
				continue
			}
			pushedPredicates = append(pushedPredicates[0:i], pushedPredicates[i+1:]...)
		}
	}
	if len(pushedPredicates) == 0 {
		p.Cte.PushDownPredicates = append(p.Cte.PushDownPredicates, expression.NewOne())
		return predicates, p.Self()
	}
	newPred := make([]expression.Expression, 0, len(predicates))
	for i := range pushedPredicates {
		newPred = append(newPred, pushedPredicates[i].Clone())
		ruleutil.ResolveExprAndReplace(newPred[i], p.Cte.ColumnMap)
	}
	p.Cte.PushDownPredicates = append(p.Cte.PushDownPredicates, expression.ComposeCNFCondition(p.SCtx().GetExprCtx(), newPred...))
	return predicates, p.Self()
}

// PruneColumns implements the base.LogicalPlan.<2nd> interface.
// LogicalCTE just do an empty function call. It's logical optimize is indivisual phase.
func (p *LogicalCTE) PruneColumns(_ []*expression.Column, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	return p, nil
}

// FindBestTask implements the base.LogicalPlan.<3rd> interface.
func (p *LogicalCTE) FindBestTask(prop *property.PhysicalProperty, counter *base.PlanCounterTp, pop *optimizetrace.PhysicalOptimizeOp) (t base.Task, cntPlan int64, err error) {
	return utilfuncp.FindBestTask4LogicalCTE(p, prop, counter, pop)
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
func (p *LogicalCTE) DeriveStats(_ []*property.StatsInfo, selfSchema *expression.Schema, _ []*expression.Schema) (*property.StatsInfo, error) {
	if p.StatsInfo() != nil {
		return p.StatsInfo(), nil
	}

	var err error
	if p.Cte.SeedPartPhysicalPlan == nil {
		// Build push-downed predicates.
		if len(p.Cte.PushDownPredicates) > 0 {
			newCond := expression.ComposeDNFCondition(p.SCtx().GetExprCtx(), p.Cte.PushDownPredicates...)
			newSel := LogicalSelection{Conditions: []expression.Expression{newCond}}.Init(p.SCtx(), p.Cte.SeedPartLogicalPlan.QueryBlockOffset())
			newSel.SetChildren(p.Cte.SeedPartLogicalPlan)
			p.Cte.SeedPartLogicalPlan = newSel
			p.Cte.OptFlag = ruleutil.SetPredicatePushDownFlag(p.Cte.OptFlag)
		}
		p.Cte.SeedPartLogicalPlan, p.Cte.SeedPartPhysicalPlan, _, err = utilfuncp.DoOptimize(context.TODO(), p.SCtx(), p.Cte.OptFlag, p.Cte.SeedPartLogicalPlan)
		if err != nil {
			return nil, err
		}
	}
	if p.OnlyUsedAsStorage {
		p.SetChildren(p.Cte.SeedPartLogicalPlan)
	}
	resStat := p.Cte.SeedPartPhysicalPlan.StatsInfo()
	// Changing the pointer so that SeedStat in LogicalCTETable can get the new stat.
	*p.SeedStat = *resStat
	p.SetStats(&property.StatsInfo{
		RowCount: resStat.RowCount,
		ColNDVs:  make(map[int64]float64, selfSchema.Len()),
	})
	for i, col := range selfSchema.Columns {
		p.StatsInfo().ColNDVs[col.UniqueID] += resStat.ColNDVs[p.Cte.SeedPartLogicalPlan.Schema().Columns[i].UniqueID]
	}
	if p.Cte.RecursivePartLogicalPlan != nil {
		if p.Cte.RecursivePartPhysicalPlan == nil {
			_, p.Cte.RecursivePartPhysicalPlan, _, err = utilfuncp.DoOptimize(context.TODO(), p.SCtx(), p.Cte.OptFlag, p.Cte.RecursivePartLogicalPlan)
			if err != nil {
				return nil, err
			}
		}
		recurStat := p.Cte.RecursivePartLogicalPlan.StatsInfo()
		for i, col := range selfSchema.Columns {
			p.StatsInfo().ColNDVs[col.UniqueID] += recurStat.ColNDVs[p.Cte.RecursivePartLogicalPlan.Schema().Columns[i].UniqueID]
		}
		if p.Cte.IsDistinct {
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
	return utilfuncp.ExhaustPhysicalPlans4LogicalCTE(p, prop)
}

// ExtractCorrelatedCols implements the base.LogicalPlan.<15th> interface.
func (p *LogicalCTE) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := coreusage.ExtractCorrelatedCols4LogicalPlan(p.Cte.SeedPartLogicalPlan)
	if p.Cte.RecursivePartLogicalPlan != nil {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4LogicalPlan(p.Cte.RecursivePartLogicalPlan)...)
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
