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
	"slices"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
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
	// ColumnUsage unions demands across consumers and nested optimization.
	// It is cleared once the shared producer has a physical plan.
	ColumnUsage []bool
}

const emptyCTEClassSize = int64(unsafe.Sizeof(CTEClass{}))

// MemoryUsage return the memory usage of CTEClass
func (cc *CTEClass) MemoryUsage() (sum int64) {
	if cc == nil {
		return
	}

	sum = emptyCTEClassSize + int64(cap(cc.ColumnUsage))*size.SizeOfBool
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
	if len(pushedPredicates) == 0 {
		p.Cte.PushDownPredicates = append(p.Cte.PushDownPredicates, expression.NewOne())
		return predicates, p.Self(), nil
	}
	newPred := make([]expression.Expression, 0, len(predicates))
	for i := range pushedPredicates {
		newPred = append(newPred, pushedPredicates[i].Clone())
		newPred[i] = ruleutil.ResolveExprAndReplace(newPred[i], p.Cte.ColumnMap)
	}
	p.Cte.PushDownPredicates = append(p.Cte.PushDownPredicates, expression.ComposeCNFCondition(p.SCtx().GetExprCtx(), newPred...))
	return predicates, p.Self(), nil
}

// PruneColumns records the columns required by this consumer. The column-pruning
// rule combines all consumers before pruning the shared producer.
func (p *LogicalCTE) PruneColumns(parentUsedCols []*expression.Column) (base.LogicalPlan, error) {
	if p.Cte != nil && p.Cte.SeedPartPhysicalPlan == nil &&
		p.Cte.RecursivePartLogicalPlan == nil && !p.Cte.IsDistinct &&
		!p.Cte.IsInApply && !p.OnlyUsedAsStorage {
		if p.Cte.ColumnUsage == nil {
			p.Cte.ColumnUsage = make([]bool, p.Schema().Len())
		}
		for _, col := range parentUsedCols {
			if index := p.Schema().ColumnIndex(col); index >= 0 {
				p.Cte.ColumnUsage[index] = true
			}
		}
	}
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
		if len(p.Cte.PushDownPredicates) > 0 {
			newCond := expression.ComposeDNFCondition(p.SCtx().GetExprCtx(), p.Cte.PushDownPredicates...)
			// Pull common filters out of the per-reference DNF before the seed is optimized,
			// so the seed plan can see them as ordinary conjunctive predicates.
			newConds := expression.ExtractFiltersFromDNFs(p.SCtx().GetExprCtx(), []expression.Expression{newCond})
			newSel := LogicalSelection{Conditions: newConds}.Init(p.SCtx(), p.Cte.SeedPartLogicalPlan.QueryBlockOffset())
			newSel.SetChildren(p.Cte.SeedPartLogicalPlan)
			p.Cte.SeedPartLogicalPlan = newSel
			p.Cte.OptFlag = ruleutil.SetPredicatePushDownFlag(p.Cte.OptFlag)
		}
		p.Cte.SeedPartLogicalPlan, p.Cte.SeedPartPhysicalPlan, _, err = utilfuncp.DoOptimize(context.TODO(), p.SCtx(), p.Cte.OptFlag, p.Cte.SeedPartLogicalPlan)
		if err != nil {
			return nil, false, err
		}
		// Nested producer optimization must retain requirements from consumers
		// outside that subtree until this shared producer is fully planned.
		p.Cte.ColumnUsage = nil
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

// *************************** end implementation of logicalPlan interface ***************************

// PruneProducerColumns applies the combined consumer requirements while keeping
// CTE storage positions stable. Pruning the producer also propagates requirements
// to any other CTEs it references.
func (p *LogicalCTE) PruneProducerColumns() (bool, error) {
	if p.Cte.SeedPartPhysicalPlan != nil {
		return false, nil
	}
	seed := p.Cte.SeedPartLogicalPlan
	used := p.Cte.ColumnUsage
	// Removing an Apply output can eliminate its inner plan, including side effects.
	// Preserve all producer outputs until Apply pruning can prove this is safe.
	if cteProducerHasApply(seed) {
		used = nil
	}
	changed := false
	if used != nil {
		// Predicate pushdown may have removed these columns from the consumers.
		// They are still needed when the producer's filter is installed.
		for _, col := range expression.ExtractColumnsFromExpressions(p.Cte.PushDownPredicates, nil) {
			if index := seed.Schema().ColumnIndex(col); index >= 0 {
				used[index] = true
			}
		}
		schema := seed.Schema().Clone()
		exprs := make([]expression.Expression, seed.Schema().Len())
		for i, col := range seed.Schema().Columns {
			if used[i] {
				exprs[i] = col
			} else {
				nullType := col.RetType.Clone()
				nullType.SetFlag(nullType.GetFlag() & ^mysql.NotNullFlag)
				exprs[i] = expression.NewNullWithFieldType(nullType)
				schema.Columns[i].RetType = nullType
				changed = true
			}
		}
		if changed {
			projection := LogicalProjection{Exprs: exprs}.Init(p.SCtx(), seed.QueryBlockOffset())
			projection.SetChildren(seed)
			projection.SetSchema(schema)
			projection.Schema().PKOrUK = nil
			projection.SetOutputNames(seed.OutputNames())
			seed = projection
		}
	}
	var err error
	p.Cte.SeedPartLogicalPlan, err = seed.PruneColumns(slices.Clone(seed.Schema().Columns))
	if err != nil {
		return false, err
	}
	if recursive := p.Cte.RecursivePartLogicalPlan; recursive != nil {
		p.Cte.RecursivePartLogicalPlan, err = recursive.PruneColumns(slices.Clone(recursive.Schema().Columns))
	}
	return changed, err
}

// SyncColumnTypes makes each consumer accept nullable producer outputs, including
// NULL placeholders retained for unused storage positions.
func (p *LogicalCTE) SyncColumnTypes() {
	seedSchema := p.Cte.SeedPartLogicalPlan.Schema()
	for i, col := range p.Schema().Columns {
		if !mysql.HasNotNullFlag(seedSchema.Columns[i].RetType.GetFlag()) && mysql.HasNotNullFlag(col.RetType.GetFlag()) {
			nullableType := col.RetType.Clone()
			nullableType.SetFlag(nullableType.GetFlag() & ^mysql.NotNullFlag)
			col.RetType = nullableType
		}
	}
}

// cteProducerHasApply examines this producer without crossing into other CTEs.
func cteProducerHasApply(p base.LogicalPlan) bool {
	if _, ok := p.(*LogicalApply); ok {
		return true
	}
	for _, child := range p.Children() {
		if cteProducerHasApply(child) {
			return true
		}
	}
	return false
}
