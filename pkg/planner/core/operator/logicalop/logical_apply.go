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
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	base2 "github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace/logicaltrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalApply gets one row from outer executor and gets one row from inner executor according to outer row.
type LogicalApply struct {
	LogicalJoin

	CorCols []*expression.CorrelatedColumn
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

// *************************** start implementation of HashEquals interface ****************************

// Hash64 implements the base.Hash64.<0th> interface.
func (la *LogicalApply) Hash64(h base2.Hasher) {
	la.LogicalJoin.Hash64(h)
	h.HashInt(len(la.CorCols))
	for _, one := range la.CorCols {
		one.Hash64(h)
	}
	h.HashBool(la.NoDecorrelate)
	h.HashBool(la.IsLateral)
}

// Equals implements the base.HashEquals.<1st> interface.
func (la *LogicalApply) Equals(other any) bool {
	if other == nil {
		return false
	}
	la2, ok := other.(*LogicalApply)
	if !ok {
		return false
	}
	ok = la.LogicalJoin.Equals(&la2.LogicalJoin) && len(la.CorCols) == len(la2.CorCols) && la.NoDecorrelate == la2.NoDecorrelate && la.IsLateral == la2.IsLateral
	if !ok {
		return false
	}
	for i, one := range la.CorCols {
		if !one.Equals(la2.CorCols[i]) {
			return false
		}
	}
	return true
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

// findChildFullSchema returns the FullSchema of p if it is a LogicalJoin or
// LogicalApply (possibly wrapped by LogicalSelection from ON clauses). Used
// during column pruning to find redundant USING/NATURAL columns from the
// left child so that LATERAL correlation extraction sees them.
func findChildFullSchema(p base.LogicalPlan) *expression.Schema {
	for {
		switch x := p.(type) {
		case *LogicalJoin:
			return x.FullSchema // may be nil
		case *LogicalApply:
			return x.FullSchema // may be nil
		case *LogicalSelection:
			children := p.Children()
			if len(children) != 1 {
				return nil
			}
			p = children[0]
		default:
			return nil
		}
	}
}

// PruneColumns implements base.LogicalPlan.<2nd> interface.
func (la *LogicalApply) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	leftCols, rightCols := la.ExtractUsedCols(parentUsedCols)
	allowEliminateApply := fixcontrol.GetBoolWithDefault(la.SCtx().GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix45822, true)
	var err error
	// IMPORTANT: We can only eliminate Apply for scalar correlated subqueries (which have MaxOneRow guarantee).
	// For LATERAL joins (IsLateral=true), the subquery may return multiple rows per left row, so eliminating
	// the Apply would change result multiplicity (wrong COUNT(*), aggregate results, etc.).
	if allowEliminateApply && !la.IsLateral && rightCols == nil && la.JoinType == LeftOuterJoin {
		logicaltrace.ApplyEliminateTraceStep(la.Children()[1], opt)
		resultPlan := la.Children()[0]
		// reEnter the new child's column pruning, returning child[0] as a new child here.
		return resultPlan.PruneColumns(parentUsedCols, opt)
	}

	// column pruning for child-1.
	la.Children()[1], err = la.Children()[1].PruneColumns(rightCols, opt)
	if err != nil {
		return nil, err
	}

	// Use FullSchema when available to capture redundant USING/NATURAL columns.
	// Without this, LATERAL over USING joins would lose correlation during pruning.
	outerSchema := la.Children()[0].Schema()
	if fs := findChildFullSchema(la.Children()[0]); fs != nil {
		outerSchema = fs
	}
	la.CorCols = coreusage.ExtractCorColumnsBySchema4LogicalPlan(la.Children()[1], outerSchema)
	for _, col := range la.CorCols {
		leftCols = append(leftCols, &col.Column)
	}

	// column pruning for child-0.
	la.Children()[0], err = la.Children()[0].PruneColumns(leftCols, opt)
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
func (la *LogicalApply) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	if la.StatsInfo() != nil {
		// Reload GroupNDVs since colGroups may have changed.
		la.StatsInfo().GroupNDVs = la.getGroupNDVs(colGroups, childStats)
		return la.StatsInfo(), nil
	}
	leftProfile := childStats[0]
	rightProfile := childStats[1]
	// For LATERAL joins (IsLateral=true), the right side can return 0..N rows per outer row,
	// so we estimate cardinality based on join multiplicity.
	// For scalar subqueries (IsLateral=false), they return at most 1 row per outer row,
	// so RowCount = leftProfile.RowCount is correct.
	rowCount := leftProfile.RowCount
	if la.IsLateral && (la.JoinType == InnerJoin || la.JoinType == LeftOuterJoin) {
		leftJoinKeys, rightJoinKeys, _, _ := la.GetJoinKeys()
		if len(leftJoinKeys) > 0 {
			// Explicit ON-clause join keys: use the same join cardinality estimation as
			// LogicalJoin so that key NDV selectivity is reflected in the row count.
			la.EqualCondOutCnt = cardinality.EstimateFullJoinRowCount(la.SCtx(),
				false,
				leftProfile, rightProfile,
				leftJoinKeys, rightJoinKeys,
				childSchema[0], childSchema[1],
				nil, nil)
			rowCount = la.EqualCondOutCnt
		} else if len(la.CorCols) > 0 {
			// No explicit join keys; the inner plan is a correlated subquery.
			// childStats[1] is derived for the inner plan as a standalone subtree
			// (total rows of that plan), not a per-outer-row execution count.
			// Dividing by the NDV of the outer correlated columns converts it to a
			// per-outer-row estimate before multiplying by the left row count, mirroring
			// the key-based selectivity division in EstimateFullJoinRowCount.
			//
			// TODO: when the inner plan is bounded by LIMIT or a scalar aggregate,
			// rightProfile.RowCount is already effectively per-outer-row (LIMIT caps it;
			// aggregates always return 1 row). In those cases this formula underestimates
			// by ~NDV(outerCols). A future improvement should detect the LIMIT/aggregate
			// case and skip the NDV scaling, restoring the correct left*right product.
			outerCols := make([]*expression.Column, 0, len(la.CorCols))
			for i := range la.CorCols {
				outerCols = append(outerCols, &la.CorCols[i].Column)
			}
			// Use the left child's FullSchema so that redundant USING/NATURAL
			// columns are visible to NDV estimation; fall back to childSchema[0].
			leftSchema := childSchema[0]
			if fs := findChildFullSchema(la.Children()[0]); fs != nil {
				leftSchema = fs
			}
			outerNDV, _ := cardinality.EstimateColsNDVWithMatchedLen(outerCols, leftSchema, leftProfile)
			rowCount = leftProfile.RowCount * rightProfile.RowCount / math.Max(outerNDV, 1)
		} else {
			// No correlation at all: decorrelation will convert this to a plain cross
			// join, so the Cartesian product is the correct upper-bound estimate.
			rowCount = leftProfile.RowCount * rightProfile.RowCount
		}
		if la.JoinType == LeftOuterJoin {
			rowCount = max(rowCount, leftProfile.RowCount)
		}
	} else if la.IsLateral && (la.JoinType == SemiJoin || la.JoinType == AntiSemiJoin) {
		// For LATERAL SemiJoin/AntiSemiJoin Apply operators, apply SelectionFactor
		// to the row count estimate, consistent with LogicalJoin.DeriveStats.
		// Non-lateral Apply (correlated subqueries) keeps the original left row count
		// to avoid changing existing plan estimates.
		rowCount *= cost.SelectionFactor
	}
	la.SetStats(&property.StatsInfo{
		RowCount: rowCount,
		ColNDVs:  make(map[int64]float64, selfSchema.Len()),
	})
	for id, c := range leftProfile.ColNDVs {
		la.StatsInfo().ColNDVs[id] = c
	}
	if la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin {
		la.StatsInfo().ColNDVs[selfSchema.Columns[selfSchema.Len()-1].UniqueID] = 2.0
	} else {
		for i := childSchema[0].Len(); i < selfSchema.Len(); i++ {
			la.StatsInfo().ColNDVs[selfSchema.Columns[i].UniqueID] = leftProfile.RowCount
		}
	}
	la.StatsInfo().GroupNDVs = la.getGroupNDVs(colGroups, childStats)
	return la.StatsInfo(), nil
}

// ExtractColGroups implements base.LogicalPlan.<12th> interface.
func (la *LogicalApply) ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column {
	var outerSchema *expression.Schema
	// Apply doesn't have RightOuterJoin.
	if la.JoinType == LeftOuterJoin || la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin {
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

// ExhaustPhysicalPlans implements base.LogicalPlan.<14th> interface.
func (la *LogicalApply) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return utilfuncp.ExhaustPhysicalPlans4LogicalApply(la, prop)
}

// ExtractCorrelatedCols implements base.LogicalPlan.<15th> interface.
func (la *LogicalApply) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := la.LogicalJoin.ExtractCorrelatedCols()
	for i := len(corCols) - 1; i >= 0; i-- {
		if la.Children()[0].Schema().Contains(&corCols[i].Column) {
			corCols = append(corCols[:i], corCols[i+1:]...)
		}
	}
	return corCols
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
	// build the join correlated equal condition for apply join, this equal condition is used for deriving the transitive FD between outer and inner side.
	correlatedCols := coreusage.ExtractCorrelatedCols4LogicalPlan(innerPlan)
	deduplicateCorrelatedCols := make(map[int64]*expression.CorrelatedColumn)
	for _, cc := range correlatedCols {
		if _, ok := deduplicateCorrelatedCols[cc.UniqueID]; !ok {
			deduplicateCorrelatedCols[cc.UniqueID] = cc
		}
	}
	eqCond := make([]expression.Expression, 0, 4)
	// for case like select (select t1.a from t2) from t1. <t1.a> will be assigned with new UniqueID after sub query projection is built.
	// we should distinguish them out, building the equivalence relationship from inner <t1.a> == outer <t1.a> in the apply-join for FD derivation.
	for _, cc := range deduplicateCorrelatedCols {
		// for every correlated column, find the connection with the inner newly built column.
		for _, col := range innerPlan.Schema().Columns {
			if cc.UniqueID == col.CorrelatedColUniqueID {
				ccc := &cc.Column
				cond := expression.NewFunctionInternal(la.SCtx().GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), ccc, col)
				eqCond = append(eqCond, cond.(*expression.ScalarFunction))
			}
		}
	}
	switch la.JoinType {
	case InnerJoin:
		return la.ExtractFDForInnerJoin(eqCond)
	case LeftOuterJoin, RightOuterJoin:
		return la.ExtractFDForOuterJoin(eqCond)
	case SemiJoin:
		return la.ExtractFDForSemiJoin(eqCond)
	default:
		return &fd.FDSet{HashCodeToUniqueID: make(map[string]int)}
	}
}

// GetBaseLogicalPlan inherits BaseLogicalPlan.LogicalPlan.<23rd> implementation.

// ConvertOuterToInnerJoin inherits BaseLogicalPlan.LogicalPlan.<24th> implementation.

// *************************** end implementation of logicalPlan interface ***************************

// CanPullUpAgg checks if an apply can pull an aggregation up.
func (la *LogicalApply) CanPullUpAgg() bool {
	if la.JoinType != InnerJoin && la.JoinType != LeftOuterJoin {
		return false
	}
	if len(la.EqualConditions)+len(la.LeftConditions)+len(la.RightConditions)+len(la.OtherConditions) > 0 {
		return false
	}
	return len(la.Children()[0].Schema().Keys) > 0
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

func (la *LogicalApply) getGroupNDVs(colGroups [][]*expression.Column, childStats []*property.StatsInfo) []property.GroupNDV {
	if len(colGroups) > 0 && (la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin || la.JoinType == LeftOuterJoin) {
		return childStats[0].GroupNDVs
	}
	return nil
}
