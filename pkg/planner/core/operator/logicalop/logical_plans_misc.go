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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/util"
)

//go:generate go run ../../generator/hash64_equals/hash64_equals_generator.go -- hash64_equals_generated.go
//go:generate go run ../../generator/shallow_ref/shallow_ref_generator.go -- shallow_ref_generated.go

var (
	_ base.LogicalPlan = &LogicalJoin{}
	_ base.LogicalPlan = &LogicalAggregation{}
	_ base.LogicalPlan = &LogicalProjection{}
	_ base.LogicalPlan = &LogicalSelection{}
	_ base.LogicalPlan = &LogicalApply{}
	_ base.LogicalPlan = &LogicalMaxOneRow{}
	_ base.LogicalPlan = &LogicalTableDual{}
	_ base.LogicalPlan = &DataSource{}
	_ base.LogicalPlan = &TiKVSingleGather{}
	_ base.LogicalPlan = &LogicalTableScan{}
	_ base.LogicalPlan = &LogicalIndexScan{}
	_ base.LogicalPlan = &LogicalUnionAll{}
	_ base.LogicalPlan = &LogicalPartitionUnionAll{}
	_ base.LogicalPlan = &LogicalSort{}
	_ base.LogicalPlan = &LogicalLock{}
	_ base.LogicalPlan = &LogicalLimit{}
	_ base.LogicalPlan = &LogicalWindow{}
	_ base.LogicalPlan = &LogicalExpand{}
	_ base.LogicalPlan = &LogicalUnionScan{}
	_ base.LogicalPlan = &LogicalMemTable{}
	_ base.LogicalPlan = &LogicalShow{}
	_ base.LogicalPlan = &LogicalShowDDLJobs{}
	_ base.LogicalPlan = &LogicalCTE{}
	_ base.LogicalPlan = &LogicalCTETable{}
	_ base.LogicalPlan = &LogicalSequence{}
)

// HasMaxOneRow returns if the LogicalPlan will output at most one row.
func HasMaxOneRow(p base.LogicalPlan, childMaxOneRow []bool) bool {
	if len(childMaxOneRow) == 0 {
		// The reason why we use this check is that, this function
		// is used both in planner/core and planner/cascades.
		// In cascades planner, LogicalPlan may have no `children`.
		return false
	}
	switch x := p.(type) {
	case *LogicalLock, *LogicalLimit, *LogicalSort, *LogicalSelection,
		*LogicalApply, *LogicalProjection, *LogicalWindow, *LogicalAggregation:
		return childMaxOneRow[0]
	case *LogicalMaxOneRow:
		return true
	case *LogicalJoin:
		switch x.JoinType {
		case base.SemiJoin, base.AntiSemiJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin:
			return childMaxOneRow[0]
		default:
			return childMaxOneRow[0] && childMaxOneRow[1]
		}
	}
	return false
}

// AddSelection adds a LogicalSelection to the given LogicalPlan.
func AddSelection(p base.LogicalPlan, child base.LogicalPlan, conditions []expression.Expression, chIdx int) {
	if len(conditions) == 0 {
		p.Children()[chIdx] = child
		return
	}
	conditions = ruleutil.ApplyPredicateSimplification(p.SCtx(), conditions, true, nil)
	if len(conditions) == 0 {
		p.Children()[chIdx] = child
		return
	}
	if dual, ok := child.(*LogicalTableDual); ok && dual.RowCount == 0 {
		p.Children()[chIdx] = child
		return
	}
	// Return table dual when filter is constant false or null.
	dual := Conds2TableDual(child, conditions)
	if dual != nil {
		p.Children()[chIdx] = dual
		return
	}
	selection := LogicalSelection{Conditions: conditions}.Init(p.SCtx(), p.QueryBlockOffset())
	selection.SetChildren(child)
	p.Children()[chIdx] = selection
}

// pushDownTopNForBaseLogicalPlan can be moved when LogicalTopN has been moved to logicalop.
func pushDownTopNForBaseLogicalPlan(lp base.LogicalPlan, topNLogicalPlan base.LogicalPlan) base.LogicalPlan {
	s := lp.GetBaseLogicalPlan().(*BaseLogicalPlan)
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	p := s.Self()
	for i, child := range p.Children() {
		p.Children()[i] = child.PushDownTopN(nil)
	}
	if topN != nil {
		return topN.AttachChild(p)
	}
	return p
}

func pruneByItems(p base.LogicalPlan, old []*util.ByItems) (byItems []*util.ByItems,
	parentUsedCols []*expression.Column) {
	prunedByItems := make([]*util.ByItems, 0)
	byItems = make([]*util.ByItems, 0, len(old))
	seen := make(map[string]struct{}, len(old))
	for _, byItem := range old {
		pruned := true
		hash := string(byItem.Expr.HashCode())
		_, hashMatch := seen[hash]
		seen[hash] = struct{}{}
		cols := expression.ExtractColumns(byItem.Expr)
		if !hashMatch {
			if len(cols) == 0 {
				if !expression.IsRuntimeConstExpr(byItem.Expr) {
					pruned = false
					byItems = append(byItems, byItem)
				}
			} else if byItem.Expr.GetType(p.SCtx().GetExprCtx().GetEvalCtx()).GetType() != mysql.TypeNull {
				pruned = false
				parentUsedCols = append(parentUsedCols, cols...)
				byItems = append(byItems, byItem)
			}
		}
		if pruned {
			prunedByItems = append(prunedByItems, byItem)
		}
	}
	return
}

// CanSelfBeingPushedToCopImpl checks whether the logical operator itself can be pushed to coprocessor.
func CanSelfBeingPushedToCopImpl(lp base.LogicalPlan, storeTp kv.StoreType) bool {
	ret := true
	switch c := lp.(type) {
	case *DataSource:
		validDs := false
		// since CanPushToCopImpl is only used in physical enumeration of physical plan phase.
		// we definitely here should use the specific PossibleAccessPaths for each DS alternative.
		for _, path := range c.PossibleAccessPaths {
			if path.StoreType == storeTp {
				validDs = true
			}
		}
		ret = ret && validDs

		if c.TableInfo.TableCacheStatusType != model.TableCacheStatusDisable {
			// Don't push to cop for cached table, it brings more harm than good:
			// 1. Those tables are small enough, push to cop can't utilize several TiKV to accelerate computation.
			// 2. Cached table use UnionScan to read the cache data, and push to cop is not supported when an UnionScan exists.
			// Once aggregation is pushed to cop, the cache data can't be use anymore.
			return false
		}
		return ret
	case *LogicalUnionAll, *LogicalSort, *LogicalProjection, *LogicalSequence:
		return storeTp == kv.TiFlash
	case *LogicalExpand:
		// Expand itself only contains simple col ref and literal projection. (always ok, check its child)
		return storeTp == kv.TiFlash
	case *LogicalAggregation:
		if storeTp != kv.TiFlash {
			return false
		}
		// logical aggregation has an additional variable to care about.
		return !c.NoCopPushDown
	case *LogicalSelection, *LogicalJoin, *LogicalWindow:
		return storeTp == kv.TiFlash
	case *LogicalLimit, *LogicalTopN:
		// since these check is inside subtree, it's check for whether this subtree can be totally pushed to tikv/tiFlash.
		// currently logicalLimit and logicalTopN can not be fully pushed down to tiFlash. ref: issues/61961
		return false
	case *LogicalCTE:
		if storeTp != kv.TiFlash {
			return false
		}
		if c.Cte.RecursivePartLogicalPlan != nil {
			return false
		}
		// !c.Cte.SeedPartLogicalPlan.CanPushToCop(storeTp) check should return false is left to the children.
		return true
	default:
		lp.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
			"MPP mode may be blocked because operator `" + c.TP() + "` is not supported now.")
		return false
	}
}

// CanPushToCopImpl checks whether the logical plan can be pushed to coprocessor.
// Deprecated: don't depend on subtree based push check, use prop based `CanSelfBeingPushedToCopImpl` instead.
func CanPushToCopImpl(lp base.LogicalPlan, storeTp kv.StoreType) bool {
	p := lp.GetBaseLogicalPlan().(*BaseLogicalPlan)
	ret := true
	for _, ch := range p.Children() {
		switch c := ch.(type) {
		case *DataSource:
			validDs := false
			// since CanPushToCopImpl is only used in physical enumeration of physical plan phase.
			// we definitely here should use the specific PossibleAccessPaths for each DS alternative.
			for _, path := range c.PossibleAccessPaths {
				if path.StoreType == storeTp {
					validDs = true
				}
			}
			ret = ret && validDs

			if c.TableInfo.TableCacheStatusType != model.TableCacheStatusDisable {
				// Don't push to cop for cached table, it brings more harm than good:
				// 1. Those tables are small enough, push to cop can't utilize several TiKV to accelerate computation.
				// 2. Cached table use UnionScan to read the cache data, and push to cop is not supported when an UnionScan exists.
				// Once aggregation is pushed to cop, the cache data can't be use anymore.
				return false
			}
		case *LogicalUnionAll:
			if storeTp != kv.TiFlash {
				return false
			}
			ret = ret && CanPushToCopImpl(&c.BaseLogicalPlan, storeTp)
		case *LogicalSort:
			if storeTp != kv.TiFlash {
				return false
			}
			ret = ret && CanPushToCopImpl(&c.BaseLogicalPlan, storeTp)
		case *LogicalProjection:
			if storeTp != kv.TiFlash {
				return false
			}
			ret = ret && CanPushToCopImpl(&c.BaseLogicalPlan, storeTp)
		case *LogicalExpand:
			// Expand itself only contains simple col ref and literal projection. (always ok, check its child)
			if storeTp != kv.TiFlash {
				return false
			}
			ret = ret && CanPushToCopImpl(&c.BaseLogicalPlan, storeTp)
		case *LogicalAggregation, *LogicalSelection, *LogicalJoin, *LogicalWindow:
			if storeTp != kv.TiFlash {
				return false
			}
			ret = ret && c.CanPushToCop(storeTp)
		// These operators can be partially push down to TiFlash, so we don't raise warning for them.
		case *LogicalLimit, *LogicalTopN:
			// currently logicalLimit and logicalTopN can not be fully pushed down to tiFlash. ref: issues/61961
			return false
		case *LogicalSequence:
			return storeTp == kv.TiFlash
		case *LogicalCTE:
			if storeTp != kv.TiFlash {
				return false
			}
			if c.Cte.RecursivePartLogicalPlan != nil || !c.Cte.SeedPartLogicalPlan.CanPushToCop(storeTp) {
				return false
			}
			return true
		default:
			p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
				"MPP mode may be blocked because operator `" + c.TP() + "` is not supported now.")
			return false
		}
	}
	return ret
}

// GetDupAgnosticAggCols checks whether a base.LogicalPlan is LogicalAggregation.
// It extracts all the columns from the duplicate agnostic aggregate functions.
// The returned column set is nil if not all the aggregate functions are duplicate agnostic.
// Only the following functions are considered to be duplicate agnostic:
//  1. MAX(arg)
//  2. MIN(arg)
//  3. FIRST_ROW(arg)
//  4. Other agg functions with DISTINCT flag, like SUM(DISTINCT arg)
func GetDupAgnosticAggCols(
	p base.LogicalPlan,
	oldAggCols []*expression.Column, // Reuse the original buffer.
) (isAgg bool, newAggCols []*expression.Column) {
	agg, ok := p.(*LogicalAggregation)
	if !ok {
		return false, nil
	}
	newAggCols = oldAggCols[:0]
	for _, aggDesc := range agg.AggFuncs {
		if !aggDesc.HasDistinct &&
			aggDesc.Name != ast.AggFuncFirstRow &&
			aggDesc.Name != ast.AggFuncMax &&
			aggDesc.Name != ast.AggFuncMin &&
			aggDesc.Name != ast.AggFuncApproxCountDistinct {
			// If not all aggregate functions are duplicate agnostic,
			// we should clean the aggCols, so `return true, newAggCols[:0]`.
			return true, newAggCols[:0]
		}
		for _, expr := range aggDesc.Args {
			newAggCols = append(newAggCols, expression.ExtractColumns(expr)...)
		}
	}
	return true, newAggCols
}
