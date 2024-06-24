package core

import (
	"bytes"
	"fmt"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace/logicaltrace"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intset"
)

// LogicalAggregation represents an aggregate plan.
type LogicalAggregation struct {
	logicalop.LogicalSchemaProducer

	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []expression.Expression

	// PreferAggType And PreferAggToCop stores aggregation hint information.
	PreferAggType  uint
	PreferAggToCop bool

	PossibleProperties [][]*expression.Column
	InputCount         float64 // InputCount is the input count of this plan.

	// NoCopPushDown indicates if planner must not push this agg down to coprocessor.
	// It is true when the agg is in the outer child tree of apply.
	NoCopPushDown bool
}

// *************************** start implementation of Plan interface ***************************

// ExplainInfo implements base.Plan.<4th> interface.
func (p *LogicalAggregation) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	if len(p.GroupByItems) > 0 {
		fmt.Fprintf(buffer, "group by:%s, ",
			expression.SortedExplainExpressionList(p.SCtx().GetExprCtx().GetEvalCtx(), p.GroupByItems))
	}
	if len(p.AggFuncs) > 0 {
		buffer.WriteString("funcs:")
		for i, agg := range p.AggFuncs {
			buffer.WriteString(aggregation.ExplainAggFunc(p.SCtx().GetExprCtx().GetEvalCtx(), agg, false))
			if i+1 < len(p.AggFuncs) {
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
			ResolveExprAndReplace(aggExpr, replace)
		}
		for _, orderExpr := range agg.OrderByItems {
			ResolveExprAndReplace(orderExpr.Expr, replace)
		}
	}
	for _, gbyItem := range la.GroupByItems {
		ResolveExprAndReplace(gbyItem, replace)
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
		if !used[i] && !ExprsHasSideEffects(la.AggFuncs[i].Args) {
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
			if len(cols) == 0 && !exprHasSetVarOrSleep(la.GroupByItems[i]) {
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
	// Do an extra Projection Elimination here. This is specially for empty Projection below Aggregation.
	// This kind of Projection would cause some bugs for MPP plan and is safe to be removed.
	// This kind of Projection should be removed in Projection Elimination, but currently PrunColumnsAgain is
	// the last rule. So we specially handle this case here.
	if childProjection, isProjection := child.(*LogicalProjection); isProjection {
		if len(childProjection.Exprs) == 0 && childProjection.Schema().Len() == 0 {
			childOfChild := childProjection.Children()[0]
			la.SetChildren(childOfChild)
		}
	}
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
	la.buildSelfKeyInfo(selfSchema)
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
		sortColOffsets := getMaxSortPrefix(possibleChildProperty, groupByCols)
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
	if la.PreferAggToCop {
		if !la.CanPushToCop(kv.TiKV) {
			la.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
				"Optimizer Hint AGG_TO_COP is inapplicable")
			la.PreferAggToCop = false
		}
	}

	preferHash, preferStream := la.ResetHintIfConflicted()

	hashAggs := la.getHashAggs(prop)
	if hashAggs != nil && preferHash {
		return hashAggs, true, nil
	}

	streamAggs := la.getStreamAggs(prop)
	if streamAggs != nil && preferStream {
		return streamAggs, true, nil
	}

	aggs := append(hashAggs, streamAggs...)

	if streamAggs == nil && preferStream && !prop.IsSortItemEmpty() {
		la.SCtx().GetSessionVars().StmtCtx.SetHintWarning("Optimizer Hint STREAM_AGG is inapplicable")
	}

	return aggs, !(preferStream || preferHash), nil
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

func (la *LogicalAggregation) getEnforcedStreamAggs(prop *property.PhysicalProperty) []base.PhysicalPlan {
	if prop.IsFlashProp() {
		return nil
	}
	_, desc := prop.AllSameOrder()
	allTaskTypes := prop.GetAllPossibleChildTaskTypes()
	enforcedAggs := make([]base.PhysicalPlan, 0, len(allTaskTypes))
	childProp := &property.PhysicalProperty{
		ExpectedCnt:    math.Max(prop.ExpectedCnt*la.InputCount/la.StatsInfo().RowCount, prop.ExpectedCnt),
		CanAddEnforcer: true,
		SortItems:      property.SortItemsFromCols(la.GetGroupByCols(), desc),
	}
	if !prop.IsPrefix(childProp) {
		return enforcedAggs
	}
	taskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopMultiReadTaskType}
	if la.HasDistinct() {
		// TODO: remove AllowDistinctAggPushDown after the cost estimation of distinct pushdown is implemented.
		// If AllowDistinctAggPushDown is set to true, we should not consider RootTask.
		if !la.CanPushToCop(kv.TiKV) || !la.SCtx().GetSessionVars().AllowDistinctAggPushDown {
			taskTypes = []property.TaskType{property.RootTaskType}
		}
	} else if !la.PreferAggToCop {
		taskTypes = append(taskTypes, property.RootTaskType)
	}
	for _, taskTp := range taskTypes {
		copiedChildProperty := new(property.PhysicalProperty)
		*copiedChildProperty = *childProp // It's ok to not deep copy the "cols" field.
		copiedChildProperty.TaskTp = taskTp

		newGbyItems := make([]expression.Expression, len(la.GroupByItems))
		copy(newGbyItems, la.GroupByItems)
		newAggFuncs := make([]*aggregation.AggFuncDesc, len(la.AggFuncs))
		copy(newAggFuncs, la.AggFuncs)

		agg := basePhysicalAgg{
			GroupByItems: newGbyItems,
			AggFuncs:     newAggFuncs,
		}.initForStream(la.SCtx(), la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), la.QueryBlockOffset(), copiedChildProperty)
		agg.SetSchema(la.Schema().Clone())
		enforcedAggs = append(enforcedAggs, agg)
	}
	return enforcedAggs
}

func (la *LogicalAggregation) distinctArgsMeetsProperty() bool {
	for _, aggFunc := range la.AggFuncs {
		if aggFunc.HasDistinct {
			for _, distinctArg := range aggFunc.Args {
				if !expression.Contains(la.GroupByItems, distinctArg) {
					return false
				}
			}
		}
	}
	return true
}

func (la *LogicalAggregation) getStreamAggs(prop *property.PhysicalProperty) []base.PhysicalPlan {
	// TODO: support CopTiFlash task type in stream agg
	if prop.IsFlashProp() {
		return nil
	}
	all, desc := prop.AllSameOrder()
	if !all {
		return nil
	}

	for _, aggFunc := range la.AggFuncs {
		if aggFunc.Mode == aggregation.FinalMode {
			return nil
		}
	}
	// group by a + b is not interested in any order.
	groupByCols := la.GetGroupByCols()
	if len(groupByCols) != len(la.GroupByItems) {
		return nil
	}

	allTaskTypes := prop.GetAllPossibleChildTaskTypes()
	streamAggs := make([]base.PhysicalPlan, 0, len(la.PossibleProperties)*(len(allTaskTypes)-1)+len(allTaskTypes))
	childProp := &property.PhysicalProperty{
		ExpectedCnt: math.Max(prop.ExpectedCnt*la.InputCount/la.StatsInfo().RowCount, prop.ExpectedCnt),
	}

	for _, possibleChildProperty := range la.PossibleProperties {
		childProp.SortItems = property.SortItemsFromCols(possibleChildProperty[:len(groupByCols)], desc)
		if !prop.IsPrefix(childProp) {
			continue
		}
		// The table read of "CopDoubleReadTaskType" can't promises the sort
		// property that the stream aggregation required, no need to consider.
		taskTypes := []property.TaskType{property.CopSingleReadTaskType}
		if la.HasDistinct() {
			// TODO: remove AllowDistinctAggPushDown after the cost estimation of distinct pushdown is implemented.
			// If AllowDistinctAggPushDown is set to true, we should not consider RootTask.
			if !la.SCtx().GetSessionVars().AllowDistinctAggPushDown || !la.CanPushToCop(kv.TiKV) {
				// if variable doesn't allow DistinctAggPushDown, just produce root task type.
				// if variable does allow DistinctAggPushDown, but OP itself can't be pushed down to tikv, just produce root task type.
				taskTypes = []property.TaskType{property.RootTaskType}
			} else if !la.distinctArgsMeetsProperty() {
				continue
			}
		} else if !la.PreferAggToCop {
			taskTypes = append(taskTypes, property.RootTaskType)
		}
		if !la.CanPushToCop(kv.TiKV) && !la.CanPushToCop(kv.TiFlash) {
			taskTypes = []property.TaskType{property.RootTaskType}
		}
		for _, taskTp := range taskTypes {
			copiedChildProperty := new(property.PhysicalProperty)
			*copiedChildProperty = *childProp // It's ok to not deep copy the "cols" field.
			copiedChildProperty.TaskTp = taskTp

			newGbyItems := make([]expression.Expression, len(la.GroupByItems))
			copy(newGbyItems, la.GroupByItems)
			newAggFuncs := make([]*aggregation.AggFuncDesc, len(la.AggFuncs))
			copy(newAggFuncs, la.AggFuncs)

			agg := basePhysicalAgg{
				GroupByItems: newGbyItems,
				AggFuncs:     newAggFuncs,
			}.initForStream(la.SCtx(), la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), la.QueryBlockOffset(), copiedChildProperty)
			agg.SetSchema(la.Schema().Clone())
			streamAggs = append(streamAggs, agg)
		}
	}
	// If STREAM_AGG hint is existed, it should consider enforce stream aggregation,
	// because we can't trust possibleChildProperty completely.
	if (la.PreferAggType & h.PreferStreamAgg) > 0 {
		streamAggs = append(streamAggs, la.getEnforcedStreamAggs(prop)...)
	}
	return streamAggs
}

// TODO: support more operators and distinct later
func (la *LogicalAggregation) checkCanPushDownToMPP() bool {
	hasUnsupportedDistinct := false
	for _, agg := range la.AggFuncs {
		// MPP does not support distinct except count distinct now
		if agg.HasDistinct {
			if agg.Name != ast.AggFuncCount && agg.Name != ast.AggFuncGroupConcat {
				hasUnsupportedDistinct = true
			}
		}
		// MPP does not support AggFuncApproxCountDistinct now
		if agg.Name == ast.AggFuncApproxCountDistinct {
			hasUnsupportedDistinct = true
		}
	}
	if hasUnsupportedDistinct {
		warnErr := errors.NewNoStackError("Aggregation can not be pushed to storage layer in mpp mode because it contains agg function with distinct")
		if la.SCtx().GetSessionVars().StmtCtx.InExplainStmt {
			la.SCtx().GetSessionVars().StmtCtx.AppendWarning(warnErr)
		} else {
			la.SCtx().GetSessionVars().StmtCtx.AppendExtraWarning(warnErr)
		}
		return false
	}
	return CheckAggCanPushCop(la.SCtx(), la.AggFuncs, la.GroupByItems, kv.TiFlash)
}

func (la *LogicalAggregation) tryToGetMppHashAggs(prop *property.PhysicalProperty) (hashAggs []base.PhysicalPlan) {
	if !prop.IsSortItemEmpty() {
		return nil
	}
	if prop.TaskTp != property.RootTaskType && prop.TaskTp != property.MppTaskType {
		return nil
	}
	if prop.MPPPartitionTp == property.BroadcastType {
		return nil
	}

	// Is this aggregate a final stage aggregate?
	// Final agg can't be split into multi-stage aggregate
	hasFinalAgg := len(la.AggFuncs) > 0 && la.AggFuncs[0].Mode == aggregation.FinalMode
	// count final agg should become sum for MPP execution path.
	// In the traditional case, TiDB take up the final agg role and push partial agg to TiKV,
	// while TiDB can tell the partialMode and do the sum computation rather than counting but MPP doesn't
	finalAggAdjust := func(aggFuncs []*aggregation.AggFuncDesc) {
		for i, agg := range aggFuncs {
			if agg.Mode == aggregation.FinalMode && agg.Name == ast.AggFuncCount {
				oldFT := agg.RetTp
				aggFuncs[i], _ = aggregation.NewAggFuncDesc(la.SCtx().GetExprCtx(), ast.AggFuncSum, agg.Args, false)
				aggFuncs[i].TypeInfer4FinalCount(oldFT)
			}
		}
	}
	// ref: https://github.com/pingcap/tiflash/blob/3ebb102fba17dce3d990d824a9df93d93f1ab
	// 766/dbms/src/Flash/Coprocessor/AggregationInterpreterHelper.cpp#L26
	validMppAgg := func(mppAgg *PhysicalHashAgg) bool {
		isFinalAgg := true
		if mppAgg.AggFuncs[0].Mode != aggregation.FinalMode && mppAgg.AggFuncs[0].Mode != aggregation.CompleteMode {
			isFinalAgg = false
		}
		for _, one := range mppAgg.AggFuncs[1:] {
			otherIsFinalAgg := one.Mode == aggregation.FinalMode || one.Mode == aggregation.CompleteMode
			if isFinalAgg != otherIsFinalAgg {
				// different agg mode detected in mpp side.
				return false
			}
		}
		return true
	}

	if len(la.GroupByItems) > 0 {
		partitionCols := la.GetPotentialPartitionKeys()
		// trying to match the required partitions.
		if prop.MPPPartitionTp == property.HashType {
			// partition key required by upper layer is subset of current layout.
			matches := prop.IsSubsetOf(partitionCols)
			if len(matches) == 0 {
				// do not satisfy the property of its parent, so return empty
				return nil
			}
			partitionCols = choosePartitionKeys(partitionCols, matches)
		} else if prop.MPPPartitionTp != property.AnyType {
			return nil
		}
		// TODO: permute various partition columns from group-by columns
		// 1-phase agg
		// If there are no available partition cols, but still have group by items, that means group by items are all expressions or constants.
		// To avoid mess, we don't do any one-phase aggregation in this case.
		// If this is a skew distinct group agg, skip generating 1-phase agg, because skew data will cause performance issue
		//
		// Rollup can't be 1-phase agg: cause it will append grouping_id to the schema, and expand each row as multi rows with different grouping_id.
		// In a general, group items should also append grouping_id as its group layout, let's say 1-phase agg has grouping items as <a,b,c>, and
		// lower OP can supply <a,b> as original partition layout, when we insert Expand logic between them:
		// <a,b>             -->    after fill null in Expand    --> and this shown two rows should be shuffled to the same node (the underlying partition is not satisfied yet)
		// <1,1> in node A           <1,null,gid=1> in node A
		// <1,2> in node B           <1,null,gid=1> in node B
		if len(partitionCols) != 0 && !la.SCtx().GetSessionVars().EnableSkewDistinctAgg {
			childProp := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.HashType, MPPPartitionCols: partitionCols, CanAddEnforcer: true, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus}
			agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), childProp)
			agg.SetSchema(la.Schema().Clone())
			agg.MppRunMode = Mpp1Phase
			finalAggAdjust(agg.AggFuncs)
			if validMppAgg(agg) {
				hashAggs = append(hashAggs, agg)
			}
		}

		// Final agg can't be split into multi-stage aggregate, so exit early
		if hasFinalAgg {
			return
		}

		// 2-phase agg
		// no partition property down，record partition cols inside agg itself, enforce shuffler latter.
		childProp := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.AnyType, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus}
		agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), childProp)
		agg.SetSchema(la.Schema().Clone())
		agg.MppRunMode = Mpp2Phase
		agg.MppPartitionCols = partitionCols
		if validMppAgg(agg) {
			hashAggs = append(hashAggs, agg)
		}

		// agg runs on TiDB with a partial agg on TiFlash if possible
		if prop.TaskTp == property.RootTaskType {
			childProp := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus}
			agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), childProp)
			agg.SetSchema(la.Schema().Clone())
			agg.MppRunMode = MppTiDB
			hashAggs = append(hashAggs, agg)
		}
	} else if !hasFinalAgg {
		// TODO: support scalar agg in MPP, merge the final result to one node
		childProp := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus}
		agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), childProp)
		agg.SetSchema(la.Schema().Clone())
		if la.HasDistinct() || la.HasOrderBy() {
			// mpp scalar mode means the data will be pass through to only one tiFlash node at last.
			agg.MppRunMode = MppScalar
		} else {
			agg.MppRunMode = MppTiDB
		}
		hashAggs = append(hashAggs, agg)
	}

	// handle MPP Agg hints
	var preferMode AggMppRunMode
	var prefer bool
	if la.PreferAggType&h.PreferMPP1PhaseAgg > 0 {
		preferMode, prefer = Mpp1Phase, true
	} else if la.PreferAggType&h.PreferMPP2PhaseAgg > 0 {
		preferMode, prefer = Mpp2Phase, true
	}
	if prefer {
		var preferPlans []base.PhysicalPlan
		for _, agg := range hashAggs {
			if hg, ok := agg.(*PhysicalHashAgg); ok && hg.MppRunMode == preferMode {
				preferPlans = append(preferPlans, hg)
			}
		}
		hashAggs = preferPlans
	}
	return
}

// getHashAggs will generate some kinds of taskType here, which finally converted to different task plan.
// when deciding whether to add a kind of taskType, there is a rule here. [Not is Not, Yes is not Sure]
// eg: which means
//
//	1: when you find something here that block hashAgg to be pushed down to XXX, just skip adding the XXXTaskType.
//	2: when you find nothing here to block hashAgg to be pushed down to XXX, just add the XXXTaskType here.
//	for 2, the final result for this physical operator enumeration is chosen or rejected is according to more factors later (hint/variable/partition/virtual-col/cost)
//
// That is to say, the non-complete positive judgement of canPushDownToMPP/canPushDownToTiFlash/canPushDownToTiKV is not that for sure here.
func (la *LogicalAggregation) getHashAggs(prop *property.PhysicalProperty) []base.PhysicalPlan {
	if !prop.IsSortItemEmpty() {
		return nil
	}
	if prop.TaskTp == property.MppTaskType && !la.checkCanPushDownToMPP() {
		return nil
	}
	hashAggs := make([]base.PhysicalPlan, 0, len(prop.GetAllPossibleChildTaskTypes()))
	taskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopMultiReadTaskType}
	canPushDownToTiFlash := la.CanPushToCop(kv.TiFlash)
	canPushDownToMPP := canPushDownToTiFlash && la.SCtx().GetSessionVars().IsMPPAllowed() && la.checkCanPushDownToMPP()
	if la.HasDistinct() {
		// TODO: remove after the cost estimation of distinct pushdown is implemented.
		if !la.SCtx().GetSessionVars().AllowDistinctAggPushDown || !la.CanPushToCop(kv.TiKV) {
			// if variable doesn't allow DistinctAggPushDown, just produce root task type.
			// if variable does allow DistinctAggPushDown, but OP itself can't be pushed down to tikv, just produce root task type.
			taskTypes = []property.TaskType{property.RootTaskType}
		}
	} else if !la.PreferAggToCop {
		taskTypes = append(taskTypes, property.RootTaskType)
	}
	if !la.CanPushToCop(kv.TiKV) && !canPushDownToTiFlash {
		taskTypes = []property.TaskType{property.RootTaskType}
	}
	if canPushDownToMPP {
		taskTypes = append(taskTypes, property.MppTaskType)
	} else {
		hasMppHints := false
		var errMsg string
		if la.PreferAggType&h.PreferMPP1PhaseAgg > 0 {
			errMsg = "The agg can not push down to the MPP side, the MPP_1PHASE_AGG() hint is invalid"
			hasMppHints = true
		}
		if la.PreferAggType&h.PreferMPP2PhaseAgg > 0 {
			errMsg = "The agg can not push down to the MPP side, the MPP_2PHASE_AGG() hint is invalid"
			hasMppHints = true
		}
		if hasMppHints {
			la.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
		}
	}
	if prop.IsFlashProp() {
		taskTypes = []property.TaskType{prop.TaskTp}
	}

	for _, taskTp := range taskTypes {
		if taskTp == property.MppTaskType {
			mppAggs := la.tryToGetMppHashAggs(prop)
			if len(mppAggs) > 0 {
				hashAggs = append(hashAggs, mppAggs...)
			}
		} else {
			agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, TaskTp: taskTp, CTEProducerStatus: prop.CTEProducerStatus})
			agg.SetSchema(la.Schema().Clone())
			hashAggs = append(hashAggs, agg)
		}
	}
	return hashAggs
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

func (la *LogicalAggregation) buildSelfKeyInfo(selfSchema *expression.Schema) {
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

// canPullUp checks if an aggregation can be pulled up. An aggregate function like count(*) cannot be pulled up.
func (la *LogicalAggregation) canPullUp() bool {
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
