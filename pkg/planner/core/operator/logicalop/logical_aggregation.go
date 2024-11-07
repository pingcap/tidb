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
	"bytes"
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace/logicaltrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalAggregation represents an aggregate plan.
type LogicalAggregation struct {
	LogicalSchemaProducer

	AggFuncs     []*aggregation.AggFuncDesc `hash64-equals:"true"`
	GroupByItems []expression.Expression    `hash64-equals:"true"`

	// PreferAggType And PreferAggToCop stores aggregation hint information.
	PreferAggType  uint
	PreferAggToCop bool

	PossibleProperties [][]*expression.Column `hash64-equals:"true"`
	InputCount         float64                // InputCount is the input count of this plan.

	// NoCopPushDown indicates if planner must not push this agg down to coprocessor.
	// It is true when the agg is in the outer child tree of apply.
	NoCopPushDown bool
}

// Init initializes LogicalAggregation.
func (la LogicalAggregation) Init(ctx base.PlanContext, offset int) *LogicalAggregation {
	la.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeAgg, &la, offset)
	return &la
}

// *************************** start implementation of Plan interface ***************************

// ExplainInfo implements base.Plan.<4th> interface.
func (la *LogicalAggregation) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	if len(la.GroupByItems) > 0 {
		fmt.Fprintf(buffer, "group by:%s, ",
			expression.SortedExplainExpressionList(la.SCtx().GetExprCtx().GetEvalCtx(), la.GroupByItems))
	}
	if len(la.AggFuncs) > 0 {
		buffer.WriteString("funcs:")
		for i, agg := range la.AggFuncs {
			buffer.WriteString(aggregation.ExplainAggFunc(la.SCtx().GetExprCtx().GetEvalCtx(), agg, false))
			if i+1 < len(la.AggFuncs) {
				buffer.WriteString(", ")
			}
		}
	}
	return buffer.String()
}

// ReplaceExprColumns implements base.Plan.<5th> interface.
func (la *LogicalAggregation) ReplaceExprColumns(replace map[string]*expression.Column) {
	for _, agg := range la.AggFuncs {
		for _, aggExpr := range agg.Args {
			ruleutil.ResolveExprAndReplace(aggExpr, replace)
		}
		for _, orderExpr := range agg.OrderByItems {
			ruleutil.ResolveExprAndReplace(orderExpr.Expr, replace)
		}
	}
	for _, gbyItem := range la.GroupByItems {
		ruleutil.ResolveExprAndReplace(gbyItem, replace)
	}
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (la *LogicalAggregation) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	condsToPush, ret := la.splitCondForAggregation(predicates)
	la.BaseLogicalPlan.PredicatePushDown(condsToPush, opt)
	return ret, la
}

// PruneColumns implements base.LogicalPlan.<2nd> interface.
func (la *LogicalAggregation) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	child := la.Children()[0]
	used := expression.GetUsedList(la.SCtx().GetExprCtx().GetEvalCtx(), parentUsedCols, la.Schema())
	prunedColumns := make([]*expression.Column, 0)
	prunedFunctions := make([]*aggregation.AggFuncDesc, 0)
	prunedGroupByItems := make([]expression.Expression, 0)

	allFirstRow := true
	allRemainFirstRow := true
	for i := len(used) - 1; i >= 0; i-- {
		if la.AggFuncs[i].Name != ast.AggFuncFirstRow {
			allFirstRow = false
		}
		if !used[i] && !expression.ExprsHasSideEffects(la.AggFuncs[i].Args) {
			prunedColumns = append(prunedColumns, la.Schema().Columns[i])
			prunedFunctions = append(prunedFunctions, la.AggFuncs[i])
			la.Schema().Columns = append(la.Schema().Columns[:i], la.Schema().Columns[i+1:]...)
			la.AggFuncs = append(la.AggFuncs[:i], la.AggFuncs[i+1:]...)
		} else if la.AggFuncs[i].Name != ast.AggFuncFirstRow {
			allRemainFirstRow = false
		}
	}
	logicaltrace.AppendColumnPruneTraceStep(la, prunedColumns, opt)
	logicaltrace.AppendFunctionPruneTraceStep(la, prunedFunctions, opt)
	//nolint: prealloc
	var selfUsedCols []*expression.Column
	for _, aggrFunc := range la.AggFuncs {
		selfUsedCols = expression.ExtractColumnsFromExpressions(selfUsedCols, aggrFunc.Args, nil)

		var cols []*expression.Column
		aggrFunc.OrderByItems, cols = pruneByItems(la, aggrFunc.OrderByItems, opt)
		selfUsedCols = append(selfUsedCols, cols...)
	}
	if len(la.AggFuncs) == 0 || (!allFirstRow && allRemainFirstRow) {
		// If all the aggregate functions are pruned, we should add an aggregate function to maintain the info of row numbers.
		// For all the aggregate functions except `first_row`, if we have an empty table defined as t(a,b),
		// `select agg(a) from t` would always return one row, while `select agg(a) from t group by b` would return empty.
		// For `first_row` which is only used internally by tidb, `first_row(a)` would always return empty for empty input now.
		var err error
		var newAgg *aggregation.AggFuncDesc
		if allFirstRow {
			newAgg, err = aggregation.NewAggFuncDesc(la.SCtx().GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{expression.NewOne()}, false)
		} else {
			newAgg, err = aggregation.NewAggFuncDesc(la.SCtx().GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
		}
		if err != nil {
			return nil, err
		}
		la.AggFuncs = append(la.AggFuncs, newAgg)
		col := &expression.Column{
			UniqueID: la.SCtx().GetSessionVars().AllocPlanColumnID(),
			RetType:  newAgg.RetTp,
		}
		la.Schema().Columns = append(la.Schema().Columns, col)
	}

	if len(la.GroupByItems) > 0 {
		for i := len(la.GroupByItems) - 1; i >= 0; i-- {
			cols := expression.ExtractColumns(la.GroupByItems[i])
			if len(cols) == 0 && !expression.ExprHasSetVarOrSleep(la.GroupByItems[i]) {
				prunedGroupByItems = append(prunedGroupByItems, la.GroupByItems[i])
				la.GroupByItems = append(la.GroupByItems[:i], la.GroupByItems[i+1:]...)
			} else {
				selfUsedCols = append(selfUsedCols, cols...)
			}
		}
		// If all the group by items are pruned, we should add a constant 1 to keep the correctness.
		// Because `select count(*) from t` is different from `select count(*) from t group by 1`.
		if len(la.GroupByItems) == 0 {
			la.GroupByItems = []expression.Expression{expression.NewOne()}
		}
	}
	logicaltrace.AppendGroupByItemsPruneTraceStep(la, prunedGroupByItems, opt)
	var err error
	la.Children()[0], err = child.PruneColumns(selfUsedCols, opt)
	if err != nil {
		return nil, err
	}
	// update children[0]
	child = la.Children()[0]
	return la, nil
}

// FindBestTask inherits BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo implements base.LogicalPlan.<4th> interface.
func (la *LogicalAggregation) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	// According to the issue#46962, we can ignore the judgment of partial agg
	// Sometimes, the agg inside of subquery and there is a true condition in where clause, the agg function is empty.
	// For example, ``` select xxxx from xxx WHERE TRUE = ALL ( SELECT TRUE GROUP BY 1 LIMIT 1 ) IS NULL IS NOT NULL;
	// In this case, the agg is complete mode and we can ignore this check.
	if len(la.AggFuncs) != 0 && la.IsPartialModeAgg() {
		return
	}
	la.LogicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)
	la.BuildSelfKeyInfo(selfSchema)
}

// PushDownTopN inherits BaseLogicalPlan.LogicalPlan.<5rd> implementation.

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> interface.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implement base.LogicalPlan.<11th> interface.
func (la *LogicalAggregation) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	gbyCols := make([]*expression.Column, 0, len(la.GroupByItems))
	for _, gbyExpr := range la.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		gbyCols = append(gbyCols, cols...)
	}
	if la.StatsInfo() != nil {
		// Reload GroupNDVs since colGroups may have changed.
		la.StatsInfo().GroupNDVs = la.getGroupNDVs(colGroups, childProfile, gbyCols)
		return la.StatsInfo(), nil
	}
	ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(gbyCols, childSchema[0], childProfile)
	la.SetStats(&property.StatsInfo{
		RowCount: ndv,
		ColNDVs:  make(map[int64]float64, selfSchema.Len()),
	})
	// We cannot estimate the ColNDVs for every output, so we use a conservative strategy.
	for _, col := range selfSchema.Columns {
		la.StatsInfo().ColNDVs[col.UniqueID] = ndv
	}
	la.InputCount = childProfile.RowCount
	la.StatsInfo().GroupNDVs = la.getGroupNDVs(colGroups, childProfile, gbyCols)
	return la.StatsInfo(), nil
}

// ExtractColGroups implements base.LogicalPlan.<12th> interface.
func (la *LogicalAggregation) ExtractColGroups(_ [][]*expression.Column) [][]*expression.Column {
	// Parent colGroups would be dicarded, because aggregation would make NDV of colGroups
	// which does not match GroupByItems invalid.
	// Note that gbyCols may not be the exact GROUP BY columns, e.g, GROUP BY a+b,
	// but we have no other approaches for the NDV estimation of these cases
	// except for using the independent assumption, unless we can use stats of expression index.
	gbyCols := make([]*expression.Column, 0, len(la.GroupByItems))
	for _, gbyExpr := range la.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		gbyCols = append(gbyCols, cols...)
	}
	if len(gbyCols) > 1 {
		return [][]*expression.Column{expression.SortColumns(gbyCols)}
	}
	return nil
}

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (la *LogicalAggregation) PreparePossibleProperties(_ *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	childProps := childrenProperties[0]
	// If there's no group-by item, the stream aggregation could have no order property. So we can add an empty property
	// when its group-by item is empty.
	if len(la.GroupByItems) == 0 {
		la.PossibleProperties = [][]*expression.Column{nil}
		return nil
	}
	resultProperties := make([][]*expression.Column, 0, len(childProps))
	groupByCols := la.GetGroupByCols()
	for _, possibleChildProperty := range childProps {
		sortColOffsets := util.GetMaxSortPrefix(possibleChildProperty, groupByCols)
		if len(sortColOffsets) == len(groupByCols) {
			prop := possibleChildProperty[:len(groupByCols)]
			resultProperties = append(resultProperties, prop)
		}
	}
	la.PossibleProperties = resultProperties
	return resultProperties
}

// ExhaustPhysicalPlans implements base.LogicalPlan.<14th> interface.
func (la *LogicalAggregation) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return utilfuncp.ExhaustPhysicalPlans4LogicalAggregation(la, prop)
}

// ExtractCorrelatedCols implements base.LogicalPlan.<15th> interface.
func (la *LogicalAggregation) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(la.GroupByItems)+len(la.AggFuncs))
	for _, expr := range la.GroupByItems {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	for _, fun := range la.AggFuncs {
		for _, arg := range fun.Args {
			corCols = append(corCols, expression.ExtractCorColumns(arg)...)
		}
		for _, arg := range fun.OrderByItems {
			corCols = append(corCols, expression.ExtractCorColumns(arg.Expr)...)
		}
	}
	return corCols
}

// MaxOneRow inherits BaseLogicalPlan.LogicalPlan.<16th> implementation.

// Children inherits BaseLogicalPlan.LogicalPlan.<17th> implementation.

// SetChildren inherits BaseLogicalPlan.LogicalPlan.<18th> implementation.

// SetChild inherits BaseLogicalPlan.LogicalPlan.<19th> implementation.

// RollBackTaskMap inherits BaseLogicalPlan.LogicalPlan.<20th> implementation.

// CanPushToCop implements base.LogicalPlan.<21st> interface.
func (la *LogicalAggregation) CanPushToCop(storeTp kv.StoreType) bool {
	return la.BaseLogicalPlan.CanPushToCop(storeTp) && !la.NoCopPushDown
}

// ExtractFD implements base.LogicalPlan.<22nd> interface.
// 1:
// In most of the cases, using FDs to check the only_full_group_by problem should be done in the buildAggregation phase
// by extracting the bottom-up FDs graph from the `p` --- the sub plan tree that has already been built.
//
// 2:
// and this requires that some conditions push-down into the `p` like selection should be done before building aggregation,
// otherwise, 'a=1 and a can occur in the select lists of a group by' will be miss-checked because it doesn't be implied in the known FDs graph.
//
// 3:
// when a logical agg is built, it's schema columns indicates what the permitted-non-agg columns is. Therefore, we shouldn't
// depend on logicalAgg.ExtractFD() to finish the only_full_group_by checking problem rather than by 1 & 2.
func (la *LogicalAggregation) ExtractFD() *fd.FDSet {
	// basically extract the children's fdSet.
	fds := la.LogicalSchemaProducer.ExtractFD()
	// collect the output columns' unique ID.
	outputColsUniqueIDs := intset.NewFastIntSet()
	notnullColsUniqueIDs := intset.NewFastIntSet()
	groupByColsUniqueIDs := intset.NewFastIntSet()
	groupByColsOutputCols := intset.NewFastIntSet()
	// Since the aggregation is build ahead of projection, the latter one will reuse the column with UniqueID allocated in aggregation
	// via aggMapper, so we don't need unnecessarily maintain the <aggDes, UniqueID> mapping in the FDSet like expr did, just treating
	// it as normal column.
	for _, one := range la.Schema().Columns {
		outputColsUniqueIDs.Insert(int(one.UniqueID))
	}
	// For one like sum(a), we don't need to build functional dependency from a --> sum(a), cause it's only determined by the
	// group-by-item (group-by-item --> sum(a)).
	for _, expr := range la.GroupByItems {
		switch x := expr.(type) {
		case *expression.Column:
			groupByColsUniqueIDs.Insert(int(x.UniqueID))
		case *expression.CorrelatedColumn:
			// shouldn't be here, intercepted by plan builder as unknown column.
			continue
		case *expression.Constant:
			// shouldn't be here, interpreted as pos param by plan builder.
			continue
		case *expression.ScalarFunction:
			hashCode := string(x.HashCode())
			var (
				ok             bool
				scalarUniqueID int
			)
			if scalarUniqueID, ok = fds.IsHashCodeRegistered(hashCode); ok {
				groupByColsUniqueIDs.Insert(scalarUniqueID)
			} else {
				// retrieve unique plan column id.  1: completely new one, allocating new unique id. 2: registered by projection earlier, using it.
				if scalarUniqueID, ok = la.SCtx().GetSessionVars().MapHashCode2UniqueID4ExtendedCol[hashCode]; !ok {
					scalarUniqueID = int(la.SCtx().GetSessionVars().AllocPlanColumnID())
				}
				fds.RegisterUniqueID(hashCode, scalarUniqueID)
				groupByColsUniqueIDs.Insert(scalarUniqueID)
			}
			determinants := intset.NewFastIntSet()
			extractedColumns := expression.ExtractColumns(x)
			extractedCorColumns := expression.ExtractCorColumns(x)
			for _, one := range extractedColumns {
				determinants.Insert(int(one.UniqueID))
				groupByColsOutputCols.Insert(int(one.UniqueID))
			}
			for _, one := range extractedCorColumns {
				determinants.Insert(int(one.UniqueID))
				groupByColsOutputCols.Insert(int(one.UniqueID))
			}
			notnull := util.IsNullRejected(la.SCtx(), la.Schema(), x)
			if notnull || determinants.SubsetOf(fds.NotNullCols) {
				notnullColsUniqueIDs.Insert(scalarUniqueID)
			}
			fds.AddStrictFunctionalDependency(determinants, intset.NewFastIntSet(scalarUniqueID))
		}
	}

	// Some details:
	// For now, select max(a) from t group by c, tidb will see `max(a)` as Max aggDes and `a,b,c` as firstRow aggDes,
	// and keep them all in the schema columns before projection does the pruning. If we build the fake FD eg: {c} ~~> {b}
	// here since we have seen b as firstRow aggDes, for the upper layer projection of `select max(a), b from t group by c`,
	// it will take b as valid projection field of group by statement since it has existed in the FD with {c} ~~> {b}.
	//
	// and since any_value will NOT be pushed down to agg schema, which means every firstRow aggDes in the agg logical operator
	// is meaningless to build the FD with. Let's only store the non-firstRow FD down: {group by items} ~~> {real aggDes}
	realAggFuncUniqueID := intset.NewFastIntSet()
	for i, aggDes := range la.AggFuncs {
		if aggDes.Name != "firstrow" {
			realAggFuncUniqueID.Insert(int(la.Schema().Columns[i].UniqueID))
		}
	}

	// apply operator's characteristic's FD setting.
	if len(la.GroupByItems) == 0 {
		// 1: as the details shown above, output cols (normal column seen as firstrow) of group by are not validated.
		// we couldn't merge them as constant FD with origin constant FD together before projection done.
		// fds.MaxOneRow(outputColsUniqueIDs.Union(groupByColsOutputCols))
		//
		// 2: for the convenience of later judgement, when there is no group by items, we will store a FD: {0} -> {real aggDes}
		// 0 unique id is only used for here.
		groupByColsUniqueIDs.Insert(0)
		for i, ok := realAggFuncUniqueID.Next(0); ok; i, ok = realAggFuncUniqueID.Next(i + 1) {
			fds.AddStrictFunctionalDependency(groupByColsUniqueIDs, intset.NewFastIntSet(i))
		}
	} else {
		// eliminating input columns that are un-projected.
		fds.ProjectCols(outputColsUniqueIDs.Union(groupByColsOutputCols).Union(groupByColsUniqueIDs))

		// note: {a} --> {b,c} is not same with {a} --> {b} and {a} --> {c}
		for i, ok := realAggFuncUniqueID.Next(0); ok; i, ok = realAggFuncUniqueID.Next(i + 1) {
			// group by phrase always produce strict FD.
			// 1: it can always distinguish and group the all-null/part-null group column rows.
			// 2: the rows with all/part null group column are unique row after group operation.
			// 3: there won't be two same group key with different agg values, so strict FD secured.
			fds.AddStrictFunctionalDependency(groupByColsUniqueIDs, intset.NewFastIntSet(i))
		}

		// agg funcDes has been tag not null flag when building aggregation.
		fds.MakeNotNull(notnullColsUniqueIDs)
	}
	fds.GroupByCols = groupByColsUniqueIDs
	fds.HasAggBuilt = true
	// just trace it down in every operator for test checking.
	la.SetFDs(fds)
	return fds
}

// GetBaseLogicalPlan inherits BaseLogicalPlan.LogicalPlan.<23rd> implementation.

// ConvertOuterToInnerJoin inherits BaseLogicalPlan.LogicalPlan.<24th> implementation.

// *************************** end implementation of logicalPlan interface ***************************

// HasDistinct shows whether LogicalAggregation has functions with distinct.
func (la *LogicalAggregation) HasDistinct() bool {
	for _, aggFunc := range la.AggFuncs {
		if aggFunc.HasDistinct {
			return true
		}
	}
	return false
}

// HasOrderBy shows whether LogicalAggregation has functions with order-by items.
func (la *LogicalAggregation) HasOrderBy() bool {
	for _, aggFunc := range la.AggFuncs {
		if len(aggFunc.OrderByItems) > 0 {
			return true
		}
	}
	return false
}

// CopyAggHints copies the aggHints from another LogicalAggregation.
func (la *LogicalAggregation) CopyAggHints(agg *LogicalAggregation) {
	// TODO: Copy the hint may make the un-applicable hint throw the
	// same warning message more than once. We'd better add a flag for
	// `HaveThrownWarningMessage` to avoid this. Besides, finalAgg and
	// partialAgg (in cascades planner) should share the same hint, instead
	// of a copy.
	la.PreferAggType = agg.PreferAggType
	la.PreferAggToCop = agg.PreferAggToCop
}

// IsPartialModeAgg returns if all of the AggFuncs are partialMode.
func (la *LogicalAggregation) IsPartialModeAgg() bool {
	// Since all of the AggFunc share the same AggMode, we only need to check the first one.
	return la.AggFuncs[0].Mode == aggregation.Partial1Mode
}

// IsCompleteModeAgg returns if all of the AggFuncs are CompleteMode.
func (la *LogicalAggregation) IsCompleteModeAgg() bool {
	// Since all of the AggFunc share the same AggMode, we only need to check the first one.
	return la.AggFuncs[0].Mode == aggregation.CompleteMode
}

// GetGroupByCols returns the columns that are group-by items.
// For example, `group by a, b, c+d` will return [a, b].
func (la *LogicalAggregation) GetGroupByCols() []*expression.Column {
	groupByCols := make([]*expression.Column, 0, len(la.GroupByItems))
	for _, item := range la.GroupByItems {
		if col, ok := item.(*expression.Column); ok {
			groupByCols = append(groupByCols, col)
		}
	}
	return groupByCols
}

// GetPotentialPartitionKeys return potential partition keys for aggregation, the potential partition keys are the group by keys
func (la *LogicalAggregation) GetPotentialPartitionKeys() []*property.MPPPartitionColumn {
	groupByCols := make([]*property.MPPPartitionColumn, 0, len(la.GroupByItems))
	for _, item := range la.GroupByItems {
		if col, ok := item.(*expression.Column); ok {
			groupByCols = append(groupByCols, &property.MPPPartitionColumn{
				Col:       col,
				CollateID: property.GetCollateIDByNameForPartition(col.GetStaticType().GetCollate()),
			})
		}
	}
	return groupByCols
}

// GetUsedCols extracts all of the Columns used by agg including GroupByItems and AggFuncs.
func (la *LogicalAggregation) GetUsedCols() (usedCols []*expression.Column) {
	for _, groupByItem := range la.GroupByItems {
		usedCols = append(usedCols, expression.ExtractColumns(groupByItem)...)
	}
	for _, aggDesc := range la.AggFuncs {
		for _, expr := range aggDesc.Args {
			usedCols = append(usedCols, expression.ExtractColumns(expr)...)
		}
		for _, expr := range aggDesc.OrderByItems {
			usedCols = append(usedCols, expression.ExtractColumns(expr.Expr)...)
		}
	}
	return usedCols
}

// ResetHintIfConflicted resets the PreferAggType if they are conflicted,
// and returns the two PreferAggType hints.
func (la *LogicalAggregation) ResetHintIfConflicted() (preferHash bool, preferStream bool) {
	preferHash = (la.PreferAggType & h.PreferHashAgg) > 0
	preferStream = (la.PreferAggType & h.PreferStreamAgg) > 0
	if preferHash && preferStream {
		la.SCtx().GetSessionVars().StmtCtx.SetHintWarning("Optimizer aggregation hints are conflicted")
		la.PreferAggType = 0
		preferHash, preferStream = false, false
	}
	return
}

// DistinctArgsMeetsProperty checks if the distinct args meet the property.
func (la *LogicalAggregation) DistinctArgsMeetsProperty() bool {
	for _, aggFunc := range la.AggFuncs {
		if aggFunc.HasDistinct {
			for _, distinctArg := range aggFunc.Args {
				if !expression.Contains(la.SCtx().GetExprCtx().GetEvalCtx(), la.GroupByItems, distinctArg) {
					return false
				}
			}
		}
	}
	return true
}

// pushDownPredicatesForAggregation split a CNF condition to two parts, can be pushed-down or can not be pushed-down below aggregation.
// It would consider the CNF.
// For example,
// (a > 1 or avg(b) > 1) and (a < 3), and `avg(b) > 1` can't be pushed-down.
// Then condsToPush: a < 3, ret: a > 1 or avg(b) > 1
func (la *LogicalAggregation) pushDownCNFPredicatesForAggregation(cond expression.Expression, groupByColumns *expression.Schema, exprsOriginal []expression.Expression) ([]expression.Expression, []expression.Expression) {
	var condsToPush []expression.Expression
	var ret []expression.Expression
	subCNFItem := expression.SplitCNFItems(cond)
	if len(subCNFItem) == 1 {
		return la.pushDownPredicatesForAggregation(subCNFItem[0], groupByColumns, exprsOriginal)
	}
	exprCtx := la.SCtx().GetExprCtx()
	for _, item := range subCNFItem {
		condsToPushForItem, retForItem := la.pushDownDNFPredicatesForAggregation(item, groupByColumns, exprsOriginal)
		if len(condsToPushForItem) > 0 {
			condsToPush = append(condsToPush, expression.ComposeDNFCondition(exprCtx, condsToPushForItem...))
		}
		if len(retForItem) > 0 {
			ret = append(ret, expression.ComposeDNFCondition(exprCtx, retForItem...))
		}
	}
	return condsToPush, ret
}

// pushDownDNFPredicatesForAggregation split a DNF condition to two parts, can be pushed-down or can not be pushed-down below aggregation.
// It would consider the DNF.
// For example,
// (a > 1 and avg(b) > 1) or (a < 3), and `avg(b) > 1` can't be pushed-down.
// Then condsToPush: (a < 3) and (a > 1), ret: (a > 1 and avg(b) > 1) or (a < 3)
func (la *LogicalAggregation) pushDownDNFPredicatesForAggregation(cond expression.Expression, groupByColumns *expression.Schema, exprsOriginal []expression.Expression) ([]expression.Expression, []expression.Expression) {
	//nolint: prealloc
	var condsToPush []expression.Expression
	var ret []expression.Expression
	subDNFItem := expression.SplitDNFItems(cond)
	if len(subDNFItem) == 1 {
		return la.pushDownPredicatesForAggregation(subDNFItem[0], groupByColumns, exprsOriginal)
	}
	exprCtx := la.SCtx().GetExprCtx()
	for _, item := range subDNFItem {
		condsToPushForItem, retForItem := la.pushDownCNFPredicatesForAggregation(item, groupByColumns, exprsOriginal)
		if len(condsToPushForItem) <= 0 {
			return nil, []expression.Expression{cond}
		}
		condsToPush = append(condsToPush, expression.ComposeCNFCondition(exprCtx, condsToPushForItem...))
		if len(retForItem) > 0 {
			ret = append(ret, expression.ComposeCNFCondition(exprCtx, retForItem...))
		}
	}
	if len(ret) == 0 {
		// All the condition can be pushed down.
		return []expression.Expression{cond}, nil
	}
	dnfPushDownCond := expression.ComposeDNFCondition(exprCtx, condsToPush...)
	// Some condition can't be pushed down, we need to keep all the condition.
	return []expression.Expression{dnfPushDownCond}, []expression.Expression{cond}
}

// splitCondForAggregation splits the condition into those who can be pushed and others.
func (la *LogicalAggregation) splitCondForAggregation(predicates []expression.Expression) ([]expression.Expression, []expression.Expression) {
	var condsToPush []expression.Expression
	var ret []expression.Expression
	exprsOriginal := make([]expression.Expression, 0, len(la.AggFuncs))
	for _, fun := range la.AggFuncs {
		exprsOriginal = append(exprsOriginal, fun.Args[0])
	}
	groupByColumns := expression.NewSchema(la.GetGroupByCols()...)
	// It's almost the same as pushDownCNFPredicatesForAggregation, except that the condition is a slice.
	for _, cond := range predicates {
		subCondsToPush, subRet := la.pushDownDNFPredicatesForAggregation(cond, groupByColumns, exprsOriginal)
		if len(subCondsToPush) > 0 {
			condsToPush = append(condsToPush, subCondsToPush...)
		}
		if len(subRet) > 0 {
			ret = append(ret, subRet...)
		}
	}
	return condsToPush, ret
}

// pushDownPredicatesForAggregation split a condition to two parts, can be pushed-down or can not be pushed-down below aggregation.
func (la *LogicalAggregation) pushDownPredicatesForAggregation(cond expression.Expression, groupByColumns *expression.Schema, exprsOriginal []expression.Expression) ([]expression.Expression, []expression.Expression) {
	var condsToPush []expression.Expression
	var ret []expression.Expression
	switch cond.(type) {
	case *expression.Constant:
		condsToPush = append(condsToPush, cond)
		// Consider SQL list "select sum(b) from t group by a having 1=0". "1=0" is a constant predicate which should be
		// retained and pushed down at the same time. Because we will get a wrong query result that contains one column
		// with value 0 rather than an empty query result.
		ret = append(ret, cond)
	case *expression.ScalarFunction:
		extractedCols := expression.ExtractColumns(cond)
		ok := true
		for _, col := range extractedCols {
			if !groupByColumns.Contains(col) {
				ok = false
				break
			}
		}
		if ok {
			newFunc := expression.ColumnSubstitute(la.SCtx().GetExprCtx(), cond, la.Schema(), exprsOriginal)
			condsToPush = append(condsToPush, newFunc)
		} else {
			ret = append(ret, cond)
		}
	default:
		ret = append(ret, cond)
	}
	return condsToPush, ret
}

// BuildSelfKeyInfo builds the key information for the aggregation itself.
func (la *LogicalAggregation) BuildSelfKeyInfo(selfSchema *expression.Schema) {
	groupByCols := la.GetGroupByCols()
	if len(groupByCols) == len(la.GroupByItems) && len(la.GroupByItems) > 0 {
		indices := selfSchema.ColumnsIndices(groupByCols)
		if indices != nil {
			newKey := make([]*expression.Column, 0, len(indices))
			for _, i := range indices {
				newKey = append(newKey, selfSchema.Columns[i])
			}
			selfSchema.Keys = append(selfSchema.Keys, newKey)
		}
	}
	if len(la.GroupByItems) == 0 {
		la.SetMaxOneRow(true)
	}
}

// CanPullUp checks if an aggregation can be pulled up. An aggregate function like count(*) cannot be pulled up.
func (la *LogicalAggregation) CanPullUp() bool {
	if len(la.GroupByItems) > 0 {
		return false
	}
	for _, f := range la.AggFuncs {
		for _, arg := range f.Args {
			expr := expression.EvaluateExprWithNull(la.SCtx().GetExprCtx(), la.Children()[0].Schema(), arg)
			if con, ok := expr.(*expression.Constant); !ok || !con.Value.IsNull() {
				return false
			}
		}
	}
	return true
}

func (*LogicalAggregation) getGroupNDVs(colGroups [][]*expression.Column, childProfile *property.StatsInfo, gbyCols []*expression.Column) []property.GroupNDV {
	if len(colGroups) == 0 {
		return nil
	}
	// Check if the child profile provides GroupNDV for the GROUP BY columns.
	// Note that gbyCols may not be the exact GROUP BY columns, e.g, GROUP BY a+b,
	// but we have no other approaches for the NDV estimation of these cases
	// except for using the independent assumption, unless we can use stats of expression index.
	groupNDV := childProfile.GetGroupNDV4Cols(gbyCols)
	if groupNDV == nil {
		return nil
	}
	return []property.GroupNDV{*groupNDV}
}
