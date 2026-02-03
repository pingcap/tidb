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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalApply gets one row from outer executor and gets one row from inner executor according to outer row.
type LogicalApply struct {
	LogicalJoin `hash64-equals:"true"`

	CorCols []*expression.CorrelatedColumn `hash64-equals:"true"`
	// NoDecorrelate is from /*+ no_decorrelate() */ hint.
	NoDecorrelate bool `hash64-equals:"true"`
	// IsLateral indicates this Apply came from a LATERAL join (not a scalar correlated subquery).
	// LATERAL joins may return multiple rows per left row, so they cannot be eliminated
	// based solely on column pruning (unlike scalar subqueries with MaxOneRow guarantee).
	IsLateral bool `hash64-equals:"true"`
}

// Init initializes LogicalApply.
func (la LogicalApply) Init(ctx base.PlanContext, offset int) *LogicalApply {
	la.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeApply, &la, offset)
	return &la
}

// *************************** start implementation of Plan interface ***************************

// ExplainInfo implements Plan interface.
func (la *LogicalApply) ExplainInfo() string {
	return la.LogicalJoin.ExplainInfo()
}

// ReplaceExprColumns implements base.LogicalPlan interface.
func (la *LogicalApply) ReplaceExprColumns(replace map[string]*expression.Column) {
	la.LogicalJoin.ReplaceExprColumns(replace)
	for _, coCol := range la.CorCols {
		dst := replace[string(coCol.Column.HashCode())]
		if dst != nil {
			coCol.Column = *dst
		}
	}
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits the BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown inherits the BaseLogicalPlan.LogicalPlan.<1st> implementation.

// PruneColumns implements base.LogicalPlan.<2nd> interface.
func (la *LogicalApply) PruneColumns(parentUsedCols []*expression.Column) (base.LogicalPlan, error) {
	leftCols, rightCols := la.ExtractUsedCols(parentUsedCols)
	allowEliminateApply := fixcontrol.GetBoolWithDefault(la.SCtx().GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix45822, true)
	var err error
	// IMPORTANT: We can only eliminate Apply for scalar correlated subqueries (which have MaxOneRow guarantee).
	// For LATERAL joins (IsLateral=true), the subquery may return multiple rows per left row, so eliminating
	// the Apply would change result multiplicity (wrong COUNT(*), aggregate results, etc.).
	if allowEliminateApply && !la.IsLateral && rightCols == nil && la.JoinType == base.LeftOuterJoin {
		resultPlan := la.Children()[0]
		// reEnter the new child's column pruning, returning child[0] as a new child here.
		return resultPlan.PruneColumns(parentUsedCols)
	}

	// column pruning for child-1.
	la.Children()[1], err = la.Children()[1].PruneColumns(rightCols)
	if err != nil {
		return nil, err
	}

	la.CorCols = coreusage.ExtractCorColumnsBySchema4LogicalPlan(la.Children()[1], la.Children()[0].Schema())
	for _, col := range la.CorCols {
		leftCols = append(leftCols, &col.Column)
	}

	// column pruning for child-0.
	la.Children()[0], err = la.Children()[0].PruneColumns(leftCols)
	if err != nil {
		return nil, err
	}
	la.MergeSchema()
	return la, nil
}

// FindBestTask inherits BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo inherits BaseLogicalPlan.LogicalPlan.<4th> implementation.

// PushDownTopN inherits BaseLogicalPlan.LogicalPlan.<5th> implementation.

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implements base.LogicalPlan.<11th> interface.
func (la *LogicalApply) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, reloads []bool) (*property.StatsInfo, bool, error) {
	var reload bool
	for _, one := range reloads {
		reload = reload || one
	}
	if !reload && la.StatsInfo() != nil {
		// Reload GroupNDVs since colGroups may have changed.
		la.StatsInfo().GroupNDVs = la.getGroupNDVs(childStats)
		return la.StatsInfo(), false, nil
	}
	leftProfile := childStats[0]
	rightProfile := childStats[1]
	// For LATERAL joins (IsLateral=true), the right side can return 0..N rows per outer row,
	// so we estimate cardinality based on join multiplicity.
	// For scalar subqueries (IsLateral=false), they return at most 1 row per outer row,
	// so RowCount = leftProfile.RowCount is correct.
	rowCount := leftProfile.RowCount
	if la.IsLateral && (la.JoinType == base.InnerJoin || la.JoinType == base.LeftOuterJoin) {
		// For LATERAL joins, estimate based on join selectivity.
		// Use rightProfile.RowCount as the average number of rows returned per outer row.
		// This is a reasonable approximation when we don't have better statistics.
		rowCount = leftProfile.RowCount * rightProfile.RowCount
	}
	la.SetStats(&property.StatsInfo{
		RowCount: rowCount,
		ColNDVs:  make(map[int64]float64, selfSchema.Len()),
	})
	// TODO: investigate why this cannot be replaced with maps.Copy()
	for id, c := range leftProfile.ColNDVs {
		la.StatsInfo().ColNDVs[id] = c
	}
	if la.JoinType == base.LeftOuterSemiJoin || la.JoinType == base.AntiLeftOuterSemiJoin {
		la.StatsInfo().ColNDVs[selfSchema.Columns[selfSchema.Len()-1].UniqueID] = 2.0
	} else {
		for i := childSchema[0].Len(); i < selfSchema.Len(); i++ {
			la.StatsInfo().ColNDVs[selfSchema.Columns[i].UniqueID] = leftProfile.RowCount
		}
	}
	la.StatsInfo().GroupNDVs = la.getGroupNDVs(childStats)
	return la.StatsInfo(), true, nil
}

// ExtractColGroups implements base.LogicalPlan.<12th> interface.
func (la *LogicalApply) ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column {
	var outerSchema *expression.Schema
	// Apply doesn't have RightOuterJoin.
	if la.JoinType == base.LeftOuterJoin || la.JoinType == base.LeftOuterSemiJoin || la.JoinType == base.AntiLeftOuterSemiJoin {
		outerSchema = la.Children()[0].Schema()
	}
	if len(colGroups) == 0 || outerSchema == nil {
		return nil
	}
	_, offsets := outerSchema.ExtractColGroups(colGroups)
	if len(offsets) == 0 {
		return nil
	}
	extracted := make([][]*expression.Column, len(offsets))
	for i, offset := range offsets {
		extracted[i] = colGroups[offset]
	}
	return extracted
}

// PreparePossibleProperties inherits BaseLogicalPlan.LogicalPlan.<13th> implementation.

// ExtractCorrelatedCols implements base.LogicalPlan.<15th> interface.
func (la *LogicalApply) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := la.LogicalJoin.ExtractCorrelatedCols()
	return slices.DeleteFunc(corCols, func(col *expression.CorrelatedColumn) bool {
		return la.Children()[0].Schema().Contains(&col.Column)
	})
}

// MaxOneRow inherits BaseLogicalPlan.LogicalPlan.<16th> implementation.

// Children inherits BaseLogicalPlan.LogicalPlan.<17th> implementation.

// SetChildren inherits BaseLogicalPlan.LogicalPlan.<18th> implementation.

// SetChild inherits BaseLogicalPlan.LogicalPlan.<19th> implementation.

// RollBackTaskMap inherits BaseLogicalPlan.LogicalPlan.<20th> implementation.

// CanPushToCop inherits BaseLogicalPlan.LogicalPlan.<21st> implementation.

// ExtractFD implements the base.LogicalPlan.<22nd> interface.
func (la *LogicalApply) ExtractFD() *fd.FDSet {
	innerPlan := la.Children()[1]
	equivs := make([][]intset.FastIntSet, 0, 4)
	// for case like select (select t1.a from t2) from t1. <t1.a> will be assigned with new UniqueID after sub query projection is built.
	// we should distinguish them out, building the equivalence relationship from inner <t1.a> == outer <t1.a> in the apply-join for FD derivation.
	// for every correlated column, find the connection with the inner newly built column.
	for _, col := range innerPlan.Schema().Columns {
		if col.CorrelatedColUniqueID != 0 {
			// the correlated column has been projected again in inner.
			// put the CorrelatedColUniqueID in the first of the pair.
			equivs = append(equivs, []intset.FastIntSet{intset.NewFastIntSet(int(col.CorrelatedColUniqueID)), intset.NewFastIntSet(int(col.UniqueID))})
		}
	}
	switch la.JoinType {
	case base.InnerJoin:
		return la.ExtractFDForInnerJoin(equivs)
	case base.LeftOuterJoin, base.RightOuterJoin:
		return la.ExtractFDForOuterJoin(equivs)
	case base.SemiJoin:
		return la.ExtractFDForSemiJoin(equivs)
	default:
		return &fd.FDSet{HashCodeToUniqueID: make(map[string]int)}
	}
}

// GetBaseLogicalPlan inherits BaseLogicalPlan.LogicalPlan.<23rd> implementation.

// ConvertOuterToInnerJoin inherits BaseLogicalPlan.LogicalPlan.<24th> implementation.

// *************************** end implementation of logicalPlan interface ***************************

// CanPullUpAgg checks if an apply can pull an aggregation up.
func (la *LogicalApply) CanPullUpAgg() bool {
	if la.JoinType != base.InnerJoin && la.JoinType != base.LeftOuterJoin {
		return false
	}
	if len(la.EqualConditions)+len(la.LeftConditions)+len(la.RightConditions)+len(la.OtherConditions) > 0 {
		return false
	}
	return len(la.Children()[0].Schema().PKOrUK) > 0
}

// DeCorColFromEqExpr checks whether it's an equal condition of form `col = correlated col`. If so we will change the decorrelated
// column to normal column to make a new equal condition.
func (la *LogicalApply) DeCorColFromEqExpr(expr expression.Expression) expression.Expression {
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok || sf.FuncName.L != ast.EQ {
		return nil
	}
	if col, lOk := sf.GetArgs()[0].(*expression.Column); lOk {
		if corCol, rOk := sf.GetArgs()[1].(*expression.CorrelatedColumn); rOk {
			ret := corCol.Decorrelate(la.Schema())
			if _, ok := ret.(*expression.CorrelatedColumn); ok {
				return nil
			}
			// We should make sure that the equal condition's left side is the join's left join key, right is the right key.
			return expression.NewFunctionInternal(la.SCtx().GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), ret, col)
		}
	}
	if corCol, lOk := sf.GetArgs()[0].(*expression.CorrelatedColumn); lOk {
		if col, rOk := sf.GetArgs()[1].(*expression.Column); rOk {
			ret := corCol.Decorrelate(la.Schema())
			if _, ok := ret.(*expression.CorrelatedColumn); ok {
				return nil
			}
			// We should make sure that the equal condition's left side is the join's left join key, right is the right key.
			return expression.NewFunctionInternal(la.SCtx().GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), ret, col)
		}
	}
	return nil
}

func (la *LogicalApply) getGroupNDVs(childStats []*property.StatsInfo) []property.GroupNDV {
	if la.JoinType == base.LeftOuterSemiJoin || la.JoinType == base.AntiLeftOuterSemiJoin || la.JoinType == base.LeftOuterJoin {
		return childStats[0].GroupNDVs
	}
	return nil
}
