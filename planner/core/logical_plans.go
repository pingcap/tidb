// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

var (
	_ LogicalPlan = &LogicalJoin{}
	_ LogicalPlan = &LogicalAggregation{}
	_ LogicalPlan = &LogicalProjection{}
	_ LogicalPlan = &LogicalSelection{}
	_ LogicalPlan = &LogicalApply{}
	_ LogicalPlan = &LogicalMaxOneRow{}
	_ LogicalPlan = &LogicalTableDual{}
	_ LogicalPlan = &DataSource{}
	_ LogicalPlan = &LogicalUnionAll{}
	_ LogicalPlan = &LogicalSort{}
	_ LogicalPlan = &LogicalLock{}
	_ LogicalPlan = &LogicalLimit{}
	_ LogicalPlan = &LogicalWindow{}
)

// JoinType contains CrossJoin, InnerJoin, LeftOuterJoin, RightOuterJoin, FullOuterJoin, SemiJoin.
type JoinType int

const (
	// InnerJoin means inner join.
	InnerJoin JoinType = iota
	// LeftOuterJoin means left join.
	LeftOuterJoin
	// RightOuterJoin means right join.
	RightOuterJoin
	// SemiJoin means if row a in table A matches some rows in B, just output a.
	SemiJoin
	// AntiSemiJoin means if row a in table A does not match any row in B, then output a.
	AntiSemiJoin
	// LeftOuterSemiJoin means if row a in table A matches some rows in B, output (a, true), otherwise, output (a, false).
	LeftOuterSemiJoin
	// AntiLeftOuterSemiJoin means if row a in table A matches some rows in B, output (a, false), otherwise, output (a, true).
	AntiLeftOuterSemiJoin
)

// IsOuterJoin returns if this joiner is a outer joiner
func (tp JoinType) IsOuterJoin() bool {
	return tp == LeftOuterJoin || tp == RightOuterJoin ||
		tp == LeftOuterSemiJoin || tp == AntiLeftOuterSemiJoin
}

func (tp JoinType) String() string {
	switch tp {
	case InnerJoin:
		return "inner join"
	case LeftOuterJoin:
		return "left outer join"
	case RightOuterJoin:
		return "right outer join"
	case SemiJoin:
		return "semi join"
	case AntiSemiJoin:
		return "anti semi join"
	case LeftOuterSemiJoin:
		return "left outer semi join"
	case AntiLeftOuterSemiJoin:
		return "anti left outer semi join"
	}
	return "unsupported join type"
}

const (
	preferLeftAsIndexInner = 1 << iota
	preferRightAsIndexInner
	preferHashJoin
	preferMergeJoin
)

// LogicalJoin is the logical join plan.
type LogicalJoin struct {
	logicalSchemaProducer

	JoinType      JoinType
	reordered     bool
	cartesianJoin bool
	StraightJoin  bool

	// hintInfo stores the join algorithm hint information specified by client.
	hintInfo       *tableHintInfo
	preferJoinType uint

	EqualConditions []*expression.ScalarFunction
	LeftConditions  expression.CNFExprs
	RightConditions expression.CNFExprs
	OtherConditions expression.CNFExprs

	LeftJoinKeys    []*expression.Column
	RightJoinKeys   []*expression.Column
	leftProperties  [][]*expression.Column
	rightProperties [][]*expression.Column

	// DefaultValues is only used for left/right outer join, which is values the inner row's should be when the outer table
	// doesn't match any inner table's row.
	// That it's nil just means the default values is a slice of NULL.
	// Currently, only `aggregation push down` phase will set this.
	DefaultValues []types.Datum

	// redundantSchema contains columns which are eliminated in join.
	// For select * from a join b using (c); a.c will in output schema, and b.c will in redundantSchema.
	redundantSchema *expression.Schema
}

func (p *LogicalJoin) columnSubstitute(schema *expression.Schema, exprs []expression.Expression) {
	for i, cond := range p.LeftConditions {
		p.LeftConditions[i] = expression.ColumnSubstitute(cond, schema, exprs)
	}

	for i, cond := range p.RightConditions {
		p.RightConditions[i] = expression.ColumnSubstitute(cond, schema, exprs)
	}

	for i, cond := range p.OtherConditions {
		p.OtherConditions[i] = expression.ColumnSubstitute(cond, schema, exprs)
	}

	for i := len(p.EqualConditions) - 1; i >= 0; i-- {
		newCond := expression.ColumnSubstitute(p.EqualConditions[i], schema, exprs).(*expression.ScalarFunction)

		// If the columns used in the new filter all come from the left child,
		// we can push this filter to it.
		if expression.ExprFromSchema(newCond, p.children[0].Schema()) {
			p.LeftConditions = append(p.LeftConditions, newCond)
			p.EqualConditions = append(p.EqualConditions[:i], p.EqualConditions[i+1:]...)
			continue
		}

		// If the columns used in the new filter all come from the right
		// child, we can push this filter to it.
		if expression.ExprFromSchema(newCond, p.children[1].Schema()) {
			p.RightConditions = append(p.RightConditions, newCond)
			p.EqualConditions = append(p.EqualConditions[:i], p.EqualConditions[i+1:]...)
			continue
		}

		_, lhsIsCol := newCond.GetArgs()[0].(*expression.Column)
		_, rhsIsCol := newCond.GetArgs()[1].(*expression.Column)

		// If the columns used in the new filter are not all expression.Column,
		// we can not use it as join's equal condition.
		if !(lhsIsCol && rhsIsCol) {
			p.OtherConditions = append(p.OtherConditions, newCond)
			p.EqualConditions = append(p.EqualConditions[:i], p.EqualConditions[i+1:]...)
			continue
		}

		p.EqualConditions[i] = newCond
	}
}

func (p *LogicalJoin) attachOnConds(onConds []expression.Expression) {
	eq, left, right, other := p.extractOnCondition(onConds, false, false)
	p.EqualConditions = append(eq, p.EqualConditions...)
	p.LeftConditions = append(left, p.LeftConditions...)
	p.RightConditions = append(right, p.RightConditions...)
	p.OtherConditions = append(other, p.OtherConditions...)
}

func (p *LogicalJoin) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := p.baseLogicalPlan.extractCorrelatedCols()
	for _, fun := range p.EqualConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.LeftConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.RightConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	return corCols
}

// LogicalProjection represents a select fields plan.
type LogicalProjection struct {
	logicalSchemaProducer

	Exprs []expression.Expression

	// calculateGenCols indicates the projection is for calculating generated columns.
	// In *UPDATE*, we should know this to tell different projections.
	calculateGenCols bool

	// calculateNoDelay indicates this Projection is the root Plan and should be
	// calculated without delay and will not return any result to client.
	// Currently it is "true" only when the current sql query is a "DO" statement.
	// See "https://dev.mysql.com/doc/refman/5.7/en/do.html" for more detail.
	calculateNoDelay bool

	// avoidColumnRef is a temporary variable which is ONLY used to avoid
	// building columnEvaluator for the expressions of Projection which is
	// built by buildProjection4Union.
	// This can be removed after column pool being supported.
	// Related issue: TiDB#8141(https://github.com/pingcap/tidb/issues/8141)
	avoidColumnEvaluator bool
}

func (p *LogicalProjection) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := p.baseLogicalPlan.extractCorrelatedCols()
	for _, expr := range p.Exprs {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	return corCols
}

// LogicalAggregation represents an aggregate plan.
type LogicalAggregation struct {
	logicalSchemaProducer

	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []expression.Expression
	// groupByCols stores the columns that are group-by items.
	groupByCols []*expression.Column

	possibleProperties [][]*expression.Column
	inputCount         float64 // inputCount is the input count of this plan.
}

func (la *LogicalAggregation) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := la.baseLogicalPlan.extractCorrelatedCols()
	for _, expr := range la.GroupByItems {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	for _, fun := range la.AggFuncs {
		for _, arg := range fun.Args {
			corCols = append(corCols, expression.ExtractCorColumns(arg)...)
		}
	}
	return corCols
}

// LogicalSelection represents a where or having predicate.
type LogicalSelection struct {
	baseLogicalPlan

	// Originally the WHERE or ON condition is parsed into a single expression,
	// but after we converted to CNF(Conjunctive normal form), it can be
	// split into a list of AND conditions.
	Conditions []expression.Expression
}

func (p *LogicalSelection) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := p.baseLogicalPlan.extractCorrelatedCols()
	for _, cond := range p.Conditions {
		corCols = append(corCols, expression.ExtractCorColumns(cond)...)
	}
	return corCols
}

// LogicalApply gets one row from outer executor and gets one row from inner executor according to outer row.
type LogicalApply struct {
	LogicalJoin

	corCols []*expression.CorrelatedColumn
}

func (la *LogicalApply) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := la.LogicalJoin.extractCorrelatedCols()
	for i := len(corCols) - 1; i >= 0; i-- {
		if la.children[0].Schema().Contains(&corCols[i].Column) {
			corCols = append(corCols[:i], corCols[i+1:]...)
		}
	}
	return corCols
}

// LogicalMaxOneRow checks if a query returns no more than one row.
type LogicalMaxOneRow struct {
	baseLogicalPlan
}

// LogicalTableDual represents a dual table plan.
type LogicalTableDual struct {
	logicalSchemaProducer

	RowCount int
	// placeHolder indicates if this dual plan is a place holder in query optimization
	// for data sources like `Show`, if true, the dual plan would be substituted by
	// `Show` in the final plan.
	placeHolder bool
}

// LogicalUnionScan is only used in non read-only txn.
type LogicalUnionScan struct {
	baseLogicalPlan

	conditions []expression.Expression
}

// DataSource represents a tableScan without condition push down.
type DataSource struct {
	logicalSchemaProducer

	indexHints []*ast.IndexHint
	table      table.Table
	tableInfo  *model.TableInfo
	Columns    []*model.ColumnInfo
	DBName     model.CIStr

	TableAsName *model.CIStr

	// pushedDownConds are the conditions that will be pushed down to coprocessor.
	pushedDownConds []expression.Expression
	// allConds contains all the filters on this table. For now it's maintained
	// in predicate push down and used only in partition pruning.
	allConds []expression.Expression

	statisticTable *statistics.Table
	tableStats     *property.StatsInfo

	// possibleAccessPaths stores all the possible access path for physical plan, including table scan.
	possibleAccessPaths []*accessPath

	// The data source may be a partition, rather than a real table.
	isPartition     bool
	physicalTableID int64
	partitionNames  []model.CIStr

	// TblCols contains the original columns of table before being pruned, and it
	// is used for estimating table scan cost.
	TblCols []*expression.Column
}

// accessPath tells how we access one index or just access table.
type accessPath struct {
	index      *model.IndexInfo
	idxCols    []*expression.Column
	idxColLens []int
	ranges     []*ranger.Range
	// countAfterAccess is the row count after we apply range seek and before we use other filter to filter data.
	countAfterAccess float64
	// countAfterIndex is the row count after we apply filters on index and before we apply the table filters.
	countAfterIndex float64
	accessConds     []expression.Expression
	eqCondCount     int
	indexFilters    []expression.Expression
	tableFilters    []expression.Expression
	// isTablePath indicates whether this path is table path.
	isTablePath bool
	// forced means this path is generated by `use/force index()`.
	forced bool
}

// getTablePath finds the TablePath from a group of accessPaths.
func getTablePath(paths []*accessPath) *accessPath {
	for _, path := range paths {
		if path.isTablePath {
			return path
		}
	}
	return nil
}

// deriveTablePathStats will fulfill the information that the accessPath need.
// And it will check whether the primary key is covered only by point query.
func (ds *DataSource) deriveTablePathStats(path *accessPath) (bool, error) {
	var err error
	sc := ds.ctx.GetSessionVars().StmtCtx
	path.countAfterAccess = float64(ds.statisticTable.Count)
	path.tableFilters = ds.pushedDownConds
	var pkCol *expression.Column
	columnLen := len(ds.schema.Columns)
	isUnsigned := false
	if ds.tableInfo.PKIsHandle {
		if pkColInfo := ds.tableInfo.GetPkColInfo(); pkColInfo != nil {
			isUnsigned = mysql.HasUnsignedFlag(pkColInfo.Flag)
			pkCol = expression.ColInfo2Col(ds.schema.Columns, pkColInfo)
		}
	} else if columnLen > 0 && ds.schema.Columns[columnLen-1].ID == model.ExtraHandleID {
		pkCol = ds.schema.Columns[columnLen-1]
	}
	if pkCol == nil {
		path.ranges = ranger.FullIntRange(isUnsigned)
		return false, nil
	}

	path.ranges = ranger.FullIntRange(isUnsigned)
	if len(ds.pushedDownConds) == 0 {
		return false, nil
	}
	path.accessConds, path.tableFilters = ranger.DetachCondsForColumn(ds.ctx, ds.pushedDownConds, pkCol)
	// If there's no access cond, we try to find that whether there's expression containing correlated column that
	// can be used to access data.
	corColInAccessConds := false
	if len(path.accessConds) == 0 {
		for i, filter := range path.tableFilters {
			eqFunc, ok := filter.(*expression.ScalarFunction)
			if !ok || eqFunc.FuncName.L != ast.EQ {
				continue
			}
			lCol, lOk := eqFunc.GetArgs()[0].(*expression.Column)
			if lOk && lCol.Equal(ds.ctx, pkCol) {
				_, rOk := eqFunc.GetArgs()[1].(*expression.CorrelatedColumn)
				if rOk {
					path.accessConds = append(path.accessConds, filter)
					path.tableFilters = append(path.tableFilters[:i], path.tableFilters[i+1:]...)
					corColInAccessConds = true
					break
				}
			}
			rCol, rOk := eqFunc.GetArgs()[1].(*expression.Column)
			if rOk && rCol.Equal(ds.ctx, pkCol) {
				_, lOk := eqFunc.GetArgs()[0].(*expression.CorrelatedColumn)
				if lOk {
					path.accessConds = append(path.accessConds, filter)
					path.tableFilters = append(path.tableFilters[:i], path.tableFilters[i+1:]...)
					corColInAccessConds = true
					break
				}
			}
		}
	}
	if corColInAccessConds {
		path.countAfterAccess = 1
		return true, nil
	}
	path.ranges, err = ranger.BuildTableRange(path.accessConds, sc, pkCol.RetType)
	if err != nil {
		return false, err
	}
	path.countAfterAccess, err = ds.statisticTable.GetRowCountByIntColumnRanges(sc, pkCol.ID, path.ranges)
	// If the `countAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.countAfterAccess < ds.stats.RowCount {
		path.countAfterAccess = math.Min(ds.stats.RowCount/selectionFactor, float64(ds.statisticTable.Count))
	}
	// Check whether the primary key is covered by point query.
	noIntervalRange := true
	for _, ran := range path.ranges {
		if !ran.IsPoint(sc) {
			noIntervalRange = false
			break
		}
	}
	return noIntervalRange, err
}

// deriveIndexPathStats will fulfill the information that the accessPath need.
// And it will check whether this index is full matched by point query. We will use this check to
// determine whether we remove other paths or not.
func (ds *DataSource) deriveIndexPathStats(path *accessPath) (bool, error) {
	sc := ds.ctx.GetSessionVars().StmtCtx
	path.ranges = ranger.FullRange()
	path.countAfterAccess = float64(ds.statisticTable.Count)
	path.idxCols, path.idxColLens = expression.IndexInfo2Cols(ds.schema.Columns, path.index)
	eqOrInCount := 0
	if len(path.idxCols) != 0 {
		res, err := ranger.DetachCondAndBuildRangeForIndex(ds.ctx, ds.pushedDownConds, path.idxCols, path.idxColLens)
		if err != nil {
			return false, err
		}
		path.ranges = res.Ranges
		path.accessConds = res.AccessConds
		path.tableFilters = res.RemainedConds
		path.eqCondCount = res.EqCondCount
		eqOrInCount = res.EqOrInCount
		path.countAfterAccess, err = ds.tableStats.HistColl.GetRowCountByIndexRanges(sc, path.index.ID, path.ranges)
		if err != nil {
			return false, err
		}
	} else {
		path.tableFilters = ds.pushedDownConds
	}
	if eqOrInCount == len(path.accessConds) {
		accesses, remained := path.splitCorColAccessCondFromFilters(eqOrInCount)
		path.accessConds = append(path.accessConds, accesses...)
		path.tableFilters = remained
		if len(accesses) > 0 && ds.statisticTable.Pseudo {
			path.countAfterAccess = ds.statisticTable.PseudoAvgCountPerValue()
		} else {
			selectivity := path.countAfterAccess / float64(ds.statisticTable.Count)
			for i := range accesses {
				col := path.idxCols[eqOrInCount+i]
				ndv := ds.getColumnNDV(col.ID)
				ndv *= selectivity
				if ndv < 1 {
					ndv = 1.0
				}
				path.countAfterAccess = path.countAfterAccess / ndv
			}
		}
	}
	path.indexFilters, path.tableFilters = splitIndexFilterConditions(path.tableFilters, path.index.Columns, ds.tableInfo)
	// If the `countAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.countAfterAccess < ds.stats.RowCount {
		path.countAfterAccess = math.Min(ds.stats.RowCount/selectionFactor, float64(ds.statisticTable.Count))
	}
	if path.indexFilters != nil {
		selectivity, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, path.indexFilters)
		if err != nil {
			logutil.Logger(context.Background()).Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = selectionFactor
		}
		path.countAfterIndex = math.Max(path.countAfterAccess*selectivity, ds.stats.RowCount)
	}
	// Check whether there's only point query.
	noIntervalRanges := true
	haveNullVal := false
	for _, ran := range path.ranges {
		// Not point or the not full matched.
		if !ran.IsPoint(sc) || len(ran.HighVal) != len(path.index.Columns) {
			noIntervalRanges = false
			break
		}
		// Check whether there's null value.
		for i := 0; i < len(path.index.Columns); i++ {
			if ran.HighVal[i].IsNull() {
				haveNullVal = true
				break
			}
		}
		if haveNullVal {
			break
		}
	}
	return noIntervalRanges && !haveNullVal, nil
}

func (path *accessPath) splitCorColAccessCondFromFilters(eqOrInCount int) (access, remained []expression.Expression) {
	access = make([]expression.Expression, len(path.idxCols)-eqOrInCount)
	used := make([]bool, len(path.tableFilters))
	for i := eqOrInCount; i < len(path.idxCols); i++ {
		matched := false
		for j, filter := range path.tableFilters {
			if used[j] || !isColEqCorColOrConstant(filter, path.idxCols[i]) {
				continue
			}
			matched = true
			access[i-eqOrInCount] = filter
			if path.idxColLens[i] == types.UnspecifiedLength {
				used[j] = true
			}
			break
		}
		if !matched {
			access = access[:i-eqOrInCount]
			break
		}
	}
	for i, ok := range used {
		if !ok {
			remained = append(remained, path.tableFilters[i])
		}
	}
	return access, remained
}

// getEqOrInColOffset checks if the expression is a eq function that one side is constant or correlated column
// and another is column.
func isColEqCorColOrConstant(filter expression.Expression, col *expression.Column) bool {
	f, ok := filter.(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.EQ {
		return false
	}
	if c, ok := f.GetArgs()[0].(*expression.Column); ok {
		if _, ok := f.GetArgs()[1].(*expression.Constant); ok {
			if col.Equal(nil, c) {
				return true
			}
		}
		if _, ok := f.GetArgs()[1].(*expression.CorrelatedColumn); ok {
			if col.Equal(nil, c) {
				return true
			}
		}
	}
	if c, ok := f.GetArgs()[1].(*expression.Column); ok {
		if _, ok := f.GetArgs()[0].(*expression.Constant); ok {
			if col.Equal(nil, c) {
				return true
			}
		}
		if _, ok := f.GetArgs()[0].(*expression.CorrelatedColumn); ok {
			if col.Equal(nil, c) {
				return true
			}
		}
	}
	return false
}

func (ds *DataSource) getPKIsHandleCol() *expression.Column {
	if !ds.tableInfo.PKIsHandle {
		// If the PKIsHandle is false, return the ExtraHandleColumn.
		for i, col := range ds.Columns {
			if col.ID == model.ExtraHandleID {
				return ds.schema.Columns[i]
			}
		}
		return nil
	}
	for i, col := range ds.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
			return ds.schema.Columns[i]
		}
	}
	return nil
}

// TableInfo returns the *TableInfo of data source.
func (ds *DataSource) TableInfo() *model.TableInfo {
	return ds.tableInfo
}

// LogicalUnionAll represents LogicalUnionAll plan.
type LogicalUnionAll struct {
	logicalSchemaProducer
}

// LogicalSort stands for the order by plan.
type LogicalSort struct {
	baseLogicalPlan

	ByItems []*util.ByItems
}

func (ls *LogicalSort) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := ls.baseLogicalPlan.extractCorrelatedCols()
	for _, item := range ls.ByItems {
		corCols = append(corCols, expression.ExtractCorColumns(item.Expr)...)
	}
	return corCols
}

// LogicalTopN represents a top-n plan.
type LogicalTopN struct {
	baseLogicalPlan

	ByItems []*util.ByItems
	Offset  uint64
	Count   uint64
}

// isLimit checks if TopN is a limit plan.
func (lt *LogicalTopN) isLimit() bool {
	return len(lt.ByItems) == 0
}

// LogicalLimit represents offset and limit plan.
type LogicalLimit struct {
	baseLogicalPlan

	Offset uint64
	Count  uint64
}

// LogicalLock represents a select lock plan.
type LogicalLock struct {
	baseLogicalPlan

	Lock             ast.SelectLockType
	partitionedTable []table.PartitionedTable
}

// WindowFrame represents a window function frame.
type WindowFrame struct {
	Type  ast.FrameType
	Start *FrameBound
	End   *FrameBound
}

// FrameBound is the boundary of a frame.
type FrameBound struct {
	Type      ast.BoundType
	UnBounded bool
	Num       uint64
	// CalcFuncs is used for range framed windows.
	// We will build the date_add or date_sub functions for frames like `INTERVAL '2:30' MINUTE_SECOND FOLLOWING`,
	// and plus or minus for frames like `1 preceding`.
	CalcFuncs []expression.Expression
	// CmpFuncs is used to decide whether one row is included in the current frame.
	CmpFuncs []expression.CompareFunc
}

// LogicalWindow represents a logical window function plan.
type LogicalWindow struct {
	logicalSchemaProducer

	WindowFuncDescs []*aggregation.WindowFuncDesc
	PartitionBy     []property.Item
	OrderBy         []property.Item
	Frame           *WindowFrame
}

// GetWindowResultColumns returns the columns storing the result of the window function.
func (p *LogicalWindow) GetWindowResultColumns() []*expression.Column {
	return p.schema.Columns[p.schema.Len()-len(p.WindowFuncDescs):]
}

// extractCorColumnsBySchema only extracts the correlated columns that match the specified schema.
// e.g. If the correlated columns from plan are [t1.a, t2.a, t3.a] and specified schema is [t2.a, t2.b, t2.c],
// only [t2.a] is returned.
func extractCorColumnsBySchema(p LogicalPlan, schema *expression.Schema) []*expression.CorrelatedColumn {
	corCols := p.extractCorrelatedCols()
	resultCorCols := make([]*expression.CorrelatedColumn, schema.Len())
	for _, corCol := range corCols {
		idx := schema.ColumnIndex(&corCol.Column)
		if idx != -1 {
			if resultCorCols[idx] == nil {
				resultCorCols[idx] = &expression.CorrelatedColumn{
					Column: *schema.Columns[idx],
					Data:   new(types.Datum),
				}
			}
			corCol.Data = resultCorCols[idx].Data
		}
	}
	// Shrink slice. e.g. [col1, nil, col2, nil] will be changed to [col1, col2].
	length := 0
	for _, col := range resultCorCols {
		if col != nil {
			resultCorCols[length] = col
			length++
		}
	}
	return resultCorCols[:length]
}
