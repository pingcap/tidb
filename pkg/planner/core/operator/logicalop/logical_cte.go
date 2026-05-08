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

// CTEPredicateCounter tracks how many CTE consumers push down the same predicate.
type CTEPredicateCounter struct {
	Expr  expression.Expression
	Count int
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
	// PushDownPredicates stores one CNF branch per consumer for the shared-execution path.
	// It preserves the original branch semantics used by Sequence-based MPP shared CTE.
	PushDownPredicates []expression.Expression
	// ConsumerPushDownPredicates stores one pushed CNF predicate group per CTE consumer.
	ConsumerPushDownPredicates []expression.CNFExprs
	// PredicatePushDownCounter tracks how many times each predicate is pushed from CTE consumers.
	PredicatePushDownCounter map[string]*CTEPredicateCounter
	// PredicatePushDownTotal records how many CTE consumers attempted predicate pushdown.
	PredicatePushDownTotal int
	// CommonPushDownPredicates stores the CNF predicates shared by all consumers.
	CommonPushDownPredicates expression.CNFExprs
	ColumnMap                map[string]*expression.Column
	IsOuterMostCTE           bool
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

	for _, exprs := range cc.ConsumerPushDownPredicates {
		for _, expr := range exprs {
			sum += expr.MemoryUsage()
		}
	}
	for _, expr := range cc.PushDownPredicates {
		sum += expr.MemoryUsage()
	}
	for _, expr := range cc.CommonPushDownPredicates {
		sum += expr.MemoryUsage()
	}
	for key, counter := range cc.PredicatePushDownCounter {
		sum += size.SizeOfString + int64(len(key)) + size.SizeOfPointer
		if counter != nil && counter.Expr != nil {
			sum += counter.Expr.MemoryUsage()
		}
	}
	for key, val := range cc.ColumnMap {
		sum += size.SizeOfString + int64(len(key)) + size.SizeOfPointer + val.MemoryUsage()
	}
	return
}

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits the BaseLogicalPlan.<0th> implementation.

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (p *LogicalCTE) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, base.LogicalPlan, error) {
	if p.Cte.RecursivePartLogicalPlan != nil {
		// Doesn't support recursive CTE yet.
		return predicates, p.Self(), nil
	}
	if !p.Cte.IsOuterMostCTE {
		return predicates, p.Self(), nil
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
	if p.SCtx().GetSessionVars().EnableMPPSharedCTEExecution {
		if len(pushedPredicates) == 0 {
			p.Cte.PushDownPredicates = append(p.Cte.PushDownPredicates, expression.NewOne())
			return predicates, p.Self(), nil
		}
		newPred := make([]expression.Expression, 0, len(pushedPredicates))
		for _, predicate := range pushedPredicates {
			resolved := ruleutil.ResolveExprAndReplace(predicate.Clone(), p.Cte.ColumnMap)
			newPred = append(newPred, resolved)
		}
		p.Cte.PushDownPredicates = append(p.Cte.PushDownPredicates, expression.ComposeCNFCondition(p.SCtx().GetExprCtx(), newPred...))
		return predicates, p.Self(), nil
	}
	newPred := make(expression.CNFExprs, 0, len(pushedPredicates))
	seen := make(map[string]struct{}, len(pushedPredicates))
	for _, predicate := range pushedPredicates {
		resolved := ruleutil.ResolveExprAndReplace(predicate.Clone(), p.Cte.ColumnMap)
		key := string(resolved.CanonicalHashCode())
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		newPred = append(newPred, resolved)
		counter := p.Cte.PredicatePushDownCounter[key]
		if counter == nil {
			p.Cte.PredicatePushDownCounter[key] = &CTEPredicateCounter{
				Expr:  resolved,
				Count: 1,
			}
			continue
		}
		counter.Count++
	}
	p.Cte.PredicatePushDownTotal++
	p.Cte.ConsumerPushDownPredicates = append(p.Cte.ConsumerPushDownPredicates, newPred)
	return predicates, p.Self(), nil
}

// PruneColumns implements the base.LogicalPlan.<2nd> interface.
// LogicalCTE just do an empty function call. It's logical optimize is indivisual phase.
func (p *LogicalCTE) PruneColumns(_ []*expression.Column) (base.LogicalPlan, error) {
	return p, nil
}

// BuildKeyInfo inherits the BaseLogicalPlan.<4th> implementation.

// PushDownTopN implements the base.LogicalPlan.<5th> interface.
func (p *LogicalCTE) PushDownTopN(topNLogicalPlan base.LogicalPlan) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	if topN != nil {
		return topN.AttachChild(p)
	}
	return p
}

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implements the base.LogicalPlan.<11th> interface.
func (p *LogicalCTE) DeriveStats(_ []*property.StatsInfo, selfSchema *expression.Schema, _ []*expression.Schema, reloads []bool) (*property.StatsInfo, bool, error) {
	var reload bool
	if len(reloads) == 1 {
		reload = reloads[0]
	}
	if !reload && p.StatsInfo() != nil {
		return p.StatsInfo(), false, nil
	}

	var err error
	if p.Cte.SeedPartPhysicalPlan == nil {
		// Build push-downed predicates.
		conditions := p.Cte.GetPushDownCondition(p.SCtx())
		if len(conditions) > 0 {
			newSel := LogicalSelection{Conditions: conditions}.Init(p.SCtx(), p.Cte.SeedPartLogicalPlan.QueryBlockOffset())
			newSel.SetChildren(p.Cte.SeedPartLogicalPlan)
			p.Cte.SeedPartLogicalPlan = newSel
			p.Cte.OptFlag = ruleutil.SetPredicatePushDownFlag(p.Cte.OptFlag)
		}
		p.Cte.SeedPartLogicalPlan, p.Cte.SeedPartPhysicalPlan, _, err = utilfuncp.DoOptimize(context.TODO(), p.SCtx(), p.Cte.OptFlag, p.Cte.SeedPartLogicalPlan)
		if err != nil {
			return nil, false, err
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
			// TODO: parallel apply inside a recursive CTE body produces incorrect results
			// (grandchildren are silently dropped) because the CTE iteration model shares
			// mutable state (the working-table buffer) across goroutines, causing rows from
			// deeper recursion levels to be lost.  Disable parallel apply for the recursive
			// body until the executor is fixed to handle this safely.
			// See: TestLateralHierarchyParallelApply (flat query verifies concurrency > 1
			// for non-recursive LATERAL; recursive correctness is tracked separately).
			vars := p.SCtx().GetSessionVars()
			savedParallelApply := vars.EnableParallelApply
			vars.EnableParallelApply = false
			defer func() { vars.EnableParallelApply = savedParallelApply }()
			p.Cte.RecursivePartLogicalPlan, p.Cte.RecursivePartPhysicalPlan, _, err = utilfuncp.DoOptimize(context.TODO(), p.SCtx(), p.Cte.OptFlag, p.Cte.RecursivePartLogicalPlan)
			if err != nil {
				return nil, false, err
			}
		}
		recurStat := p.Cte.RecursivePartLogicalPlan.StatsInfo()
		for i, col := range selfSchema.Columns {
			p.StatsInfo().ColNDVs[col.UniqueID] += recurStat.ColNDVs[p.Cte.RecursivePartLogicalPlan.Schema().Columns[i].UniqueID]
		}
		if p.Cte.IsDistinct {
			p.StatsInfo().RowCount, _ = cardinality.EstimateColsNDVWithMatchedLen(
				p.SCtx(), p.Schema().Columns, p.Schema(), p.StatsInfo())
		} else {
			p.StatsInfo().RowCount += recurStat.RowCount
		}
	}
	return p.StatsInfo(), true, nil
}

// GetPushDownCondition builds the effective pushed predicate list for the CTE producer.
func (cc *CTEClass) GetPushDownCondition(ctx base.PlanContext) []expression.Expression {
	if len(cc.PushDownPredicates) > 0 {
		return []expression.Expression{expression.ComposeDNFCondition(ctx.GetExprCtx(), cc.PushDownPredicates...)}
	}
	conditions := make([]expression.Expression, 0, len(cc.CommonPushDownPredicates)+1)
	for _, expr := range cc.CommonPushDownPredicates {
		conditions = append(conditions, expr)
	}
	if len(cc.ConsumerPushDownPredicates) == 0 {
		return conditions
	}

	residualBranches := make([]expression.Expression, 0, len(cc.ConsumerPushDownPredicates))
	for _, consumerCNF := range cc.ConsumerPushDownPredicates {
		if len(consumerCNF) == 0 {
			return conditions
		}
		residualBranches = append(residualBranches, expression.ComposeCNFCondition(ctx.GetExprCtx(), consumerCNF...))
	}
	if len(residualBranches) == 1 {
		conditions = append(conditions, residualBranches[0])
		return conditions
	}
	if len(residualBranches) > 1 {
		conditions = append(conditions, expression.ComposeDNFCondition(ctx.GetExprCtx(), residualBranches...))
	}
	return conditions
}

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (p *LogicalCTE) PreparePossibleProperties(_ *expression.Schema, childrenProperties ...*base.PossiblePropertiesInfo) *base.PossiblePropertiesInfo {
	if len(childrenProperties) > 0 {
		hasTiFlash := false
		hasValidChild := false
		for _, child := range childrenProperties {
			if child == nil {
				continue
			}
			if !hasValidChild {
				hasTiFlash = child.HasTiFlash
				hasValidChild = true
				continue
			}
			hasTiFlash = hasTiFlash && child.HasTiFlash
		}
		if hasValidChild {
			p.hasTiFlash = hasTiFlash
			return &base.PossiblePropertiesInfo{HasTiFlash: p.hasTiFlash}
		}
	}

	hasTiFlash := false
	if p.Cte != nil && p.Cte.SeedPartLogicalPlan != nil {
		hasTiFlash = GetHasTiFlash(p.Cte.SeedPartLogicalPlan)
	}
	p.hasTiFlash = hasTiFlash
	return &base.PossiblePropertiesInfo{HasTiFlash: p.hasTiFlash}
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
