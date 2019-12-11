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
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
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
	_ LogicalPlan = &TiKVSingleGather{}
	_ LogicalPlan = &LogicalTableScan{}
	_ LogicalPlan = &LogicalIndexScan{}
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
	preferLeftAsINLJInner uint = 1 << iota
	preferRightAsINLJInner
	preferLeftAsINLHJInner
	preferRightAsINLHJInner
	preferLeftAsINLMJInner
	preferRightAsINLMJInner
	preferHashJoin
	preferMergeJoin
	preferHashAgg
	preferStreamAgg
)

const (
	preferTiKV = 1 << iota
	preferTiFlash
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
	redundantNames  types.NameSlice

	// equalCondOutCnt indicates the estimated count of joined rows after evaluating `EqualConditions`.
	equalCondOutCnt float64
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

	// CalculateNoDelay indicates this Projection is the root Plan and should be
	// calculated without delay and will not return any result to client.
	// Currently it is "true" only when the current sql query is a "DO" statement.
	// See "https://dev.mysql.com/doc/refman/5.7/en/do.html" for more detail.
	CalculateNoDelay bool

	// AvoidColumnEvaluator is a temporary variable which is ONLY used to avoid
	// building columnEvaluator for the expressions of Projection which is
	// built by buildProjection4Union.
	// This can be removed after column pool being supported.
	// Related issue: TiDB#8141(https://github.com/pingcap/tidb/issues/8141)
	AvoidColumnEvaluator bool
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

	// aggHints stores aggregation hint information.
	aggHints aggHintInfo

	possibleProperties [][]*expression.Column
	inputCount         float64 // inputCount is the input count of this plan.
}

// CopyAggHints copies the aggHints from another LogicalAggregation.
func (la *LogicalAggregation) CopyAggHints(agg *LogicalAggregation) {
	// TODO: Copy the hint may make the un-applicable hint throw the
	// same warning message more than once. We'd better add a flag for
	// `HaveThrownWarningMessage` to avoid this. Besides, finalAgg and
	// partialAgg (in cascades planner) should share the same hint, instead
	// of a copy.
	la.aggHints = agg.aggHints
}

// IsPartialModeAgg returns if all of the AggFuncs are partialMode.
func (la *LogicalAggregation) IsPartialModeAgg() bool {
	// Since all of the AggFunc share the same AggMode, we only need to check the first one.
	return la.AggFuncs[0].Mode == aggregation.Partial1Mode
}

// GetGroupByCols returns the groupByCols. If the groupByCols haven't be collected,
// this method would collect them at first. If the GroupByItems have been changed,
// we should explicitly collect GroupByColumns before this method.
func (la *LogicalAggregation) GetGroupByCols() []*expression.Column {
	if la.groupByCols == nil {
		la.collectGroupByColumns()
	}
	return la.groupByCols
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

	CorCols []*expression.CorrelatedColumn
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
}

// LogicalMemTable represents a memory table or virtual table
// Some memory tables wants to take the ownership of some predications
// e.g
// SELECT * FROM cluster_log WHERE type='tikv' AND address='192.16.5.32'
// Assume that the table `cluster_log` is a memory table, which is used
// to retrieve logs from remote components. In the above situation we should
// send log search request to the target TiKV (192.16.5.32) directly instead of
// requesting all cluster components log search gRPC interface to retrieve
// log message and filtering them in TiDB node.
type LogicalMemTable struct {
	logicalSchemaProducer

	Extractor MemTablePredicateExtractor
	dbName    model.CIStr
	tableInfo *model.TableInfo
}

// LogicalUnionScan is only used in non read-only txn.
type LogicalUnionScan struct {
	baseLogicalPlan

	conditions []expression.Expression

	handleCol *expression.Column
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
	// indexMergeHints are the hint for indexmerge.
	indexMergeHints []*ast.IndexHint
	// pushedDownConds are the conditions that will be pushed down to coprocessor.
	pushedDownConds []expression.Expression
	// allConds contains all the filters on this table. For now it's maintained
	// in predicate push down and used only in partition pruning.
	allConds []expression.Expression

	statisticTable *statistics.Table
	tableStats     *property.StatsInfo

	// possibleAccessPaths stores all the possible access path for physical plan, including table scan.
	possibleAccessPaths []*util.AccessPath

	// The data source may be a partition, rather than a real table.
	isPartition     bool
	physicalTableID int64
	partitionNames  []model.CIStr

	// handleCol represents the handle column for the datasource, either the
	// int primary key column or extra handle column.
	handleCol *expression.Column
	// TblCols contains the original columns of table before being pruned, and it
	// is used for estimating table scan cost.
	TblCols []*expression.Column
	// TblColHists contains the Histogram of all original table columns,
	// it is converted from statisticTable, and used for IO/network cost estimating.
	TblColHists *statistics.HistColl
	//preferStoreType means the DataSource is enforced to which storage.
	preferStoreType int
}

// TiKVSingleGather is a leaf logical operator of TiDB layer to gather
// tuples from TiKV regions.
type TiKVSingleGather struct {
	logicalSchemaProducer
	Source *DataSource
	// IsIndexGather marks if this TiKVSingleGather gathers tuples from an IndexScan.
	// in implementation phase, we need this flag to determine whether to generate
	// PhysicalTableReader or PhysicalIndexReader.
	IsIndexGather bool
	Index         *model.IndexInfo
}

// LogicalTableScan is the logical table scan operator for TiKV.
type LogicalTableScan struct {
	logicalSchemaProducer
	Source      *DataSource
	Handle      *expression.Column
	AccessConds expression.CNFExprs
	Ranges      []*ranger.Range
}

// LogicalIndexScan is the logical index scan operator for TiKV.
type LogicalIndexScan struct {
	logicalSchemaProducer
	// DataSource should be read-only here.
	Source       *DataSource
	IsDoubleRead bool

	EqCondCount int
	AccessConds expression.CNFExprs
	Ranges      []*ranger.Range

	Index          *model.IndexInfo
	Columns        []*model.ColumnInfo
	fullIdxCols    []*expression.Column
	fullIdxColLens []int
	idxCols        []*expression.Column
	idxColLens     []int
}

// MatchIndexProp checks if the indexScan can match the required property.
func (p *LogicalIndexScan) MatchIndexProp(prop *property.PhysicalProperty) (match bool) {
	if prop.IsEmpty() {
		return true
	}
	if all, _ := prop.AllSameOrder(); !all {
		return false
	}
	for i, col := range p.idxCols {
		if col.Equal(nil, prop.Items[0].Col) {
			return matchIndicesProp(p.idxCols[i:], p.idxColLens[i:], prop.Items)
		} else if i >= p.EqCondCount {
			break
		}
	}
	return false
}

// getTablePath finds the TablePath from a group of accessPaths.
func getTablePath(paths []*util.AccessPath) *util.AccessPath {
	for _, path := range paths {
		if path.IsTablePath {
			return path
		}
	}
	return nil
}

func (ds *DataSource) buildTableGather() LogicalPlan {
	ts := LogicalTableScan{Source: ds, Handle: ds.getHandleCol()}.Init(ds.ctx, ds.blockOffset)
	ts.SetSchema(ds.Schema())
	sg := TiKVSingleGather{Source: ds, IsIndexGather: false}.Init(ds.ctx, ds.blockOffset)
	sg.SetSchema(ds.Schema())
	sg.SetChildren(ts)
	return sg
}

func (ds *DataSource) buildIndexGather(path *util.AccessPath) LogicalPlan {
	is := LogicalIndexScan{
		Source:       ds,
		IsDoubleRead: false,
		Index:        path.Index,
	}.Init(ds.ctx, ds.blockOffset)

	is.Columns = make([]*model.ColumnInfo, len(ds.Columns))
	copy(is.Columns, ds.Columns)
	is.SetSchema(ds.Schema())
	is.idxCols, is.idxColLens = expression.IndexInfo2PrefixCols(is.Columns, is.schema.Columns, is.Index)

	sg := TiKVSingleGather{
		Source:        ds,
		IsIndexGather: true,
		Index:         path.Index,
	}.Init(ds.ctx, ds.blockOffset)
	sg.SetSchema(ds.Schema())
	sg.SetChildren(is)
	return sg
}

// Convert2Gathers builds logical TiKVSingleGathers from DataSource.
func (ds *DataSource) Convert2Gathers() (gathers []LogicalPlan) {
	tg := ds.buildTableGather()
	gathers = append(gathers, tg)
	for _, path := range ds.possibleAccessPaths {
		if !path.IsTablePath {
			path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, path.Index)
			// If index columns can cover all of the needed columns, we can use a IndexGather + IndexScan.
			if isCoveringIndex(ds.schema.Columns, path.FullIdxCols, path.FullIdxColLens, ds.tableInfo.PKIsHandle) {
				gathers = append(gathers, ds.buildIndexGather(path))
			}
			// TODO: If index columns can not cover the schema, use IndexLookUpGather.
		}
	}
	return gathers
}

// deriveTablePathStats will fulfill the information that the AccessPath need.
// And it will check whether the primary key is covered only by point query.
// isIm indicates whether this function is called to generate the partial path for IndexMerge.
func (ds *DataSource) deriveTablePathStats(path *util.AccessPath, conds []expression.Expression, isIm bool) (bool, error) {
	var err error
	sc := ds.ctx.GetSessionVars().StmtCtx
	path.CountAfterAccess = float64(ds.statisticTable.Count)
	path.TableFilters = conds
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
		path.Ranges = ranger.FullIntRange(isUnsigned)
		return false, nil
	}

	path.Ranges = ranger.FullIntRange(isUnsigned)
	if len(conds) == 0 {
		return false, nil
	}
	path.AccessConds, path.TableFilters = ranger.DetachCondsForColumn(ds.ctx, conds, pkCol)
	// If there's no access cond, we try to find that whether there's expression containing correlated column that
	// can be used to access data.
	corColInAccessConds := false
	if len(path.AccessConds) == 0 {
		for i, filter := range path.TableFilters {
			eqFunc, ok := filter.(*expression.ScalarFunction)
			if !ok || eqFunc.FuncName.L != ast.EQ {
				continue
			}
			lCol, lOk := eqFunc.GetArgs()[0].(*expression.Column)
			if lOk && lCol.Equal(ds.ctx, pkCol) {
				_, rOk := eqFunc.GetArgs()[1].(*expression.CorrelatedColumn)
				if rOk {
					path.AccessConds = append(path.AccessConds, filter)
					path.TableFilters = append(path.TableFilters[:i], path.TableFilters[i+1:]...)
					corColInAccessConds = true
					break
				}
			}
			rCol, rOk := eqFunc.GetArgs()[1].(*expression.Column)
			if rOk && rCol.Equal(ds.ctx, pkCol) {
				_, lOk := eqFunc.GetArgs()[0].(*expression.CorrelatedColumn)
				if lOk {
					path.AccessConds = append(path.AccessConds, filter)
					path.TableFilters = append(path.TableFilters[:i], path.TableFilters[i+1:]...)
					corColInAccessConds = true
					break
				}
			}
		}
	}
	if corColInAccessConds {
		path.CountAfterAccess = 1
		return true, nil
	}
	path.Ranges, err = ranger.BuildTableRange(path.AccessConds, sc, pkCol.RetType)
	if err != nil {
		return false, err
	}
	path.CountAfterAccess, err = ds.statisticTable.GetRowCountByIntColumnRanges(sc, pkCol.ID, path.Ranges)
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.stats.RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.stats.RowCount/selectionFactor, float64(ds.statisticTable.Count))
	}
	// Check whether the primary key is covered by point query.
	noIntervalRange := true
	for _, ran := range path.Ranges {
		if !ran.IsPoint(sc) {
			noIntervalRange = false
			break
		}
	}
	return noIntervalRange, err
}

func (ds *DataSource) fillIndexPath(path *util.AccessPath, conds []expression.Expression) error {
	sc := ds.ctx.GetSessionVars().StmtCtx
	path.Ranges = ranger.FullRange()
	path.CountAfterAccess = float64(ds.statisticTable.Count)
	path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.schema.Columns, path.Index)
	path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, path.Index)
	if !path.Index.Unique && !path.Index.Primary && len(path.Index.Columns) == len(path.IdxCols) {
		handleCol := ds.getPKIsHandleCol()
		if handleCol != nil && !mysql.HasUnsignedFlag(handleCol.RetType.Flag) {
			path.IdxCols = append(path.IdxCols, handleCol)
			path.IdxColLens = append(path.IdxColLens, types.UnspecifiedLength)
		}
	}
	if len(path.IdxCols) != 0 {
		res, err := ranger.DetachCondAndBuildRangeForIndex(ds.ctx, conds, path.IdxCols, path.IdxColLens)
		if err != nil {
			return err
		}
		path.Ranges = res.Ranges
		path.AccessConds = res.AccessConds
		path.TableFilters = res.RemainedConds
		path.EqCondCount = res.EqCondCount
		path.EqOrInCondCount = res.EqOrInCount
		path.IsDNFCond = res.IsDNFCond
		path.CountAfterAccess, err = ds.tableStats.HistColl.GetRowCountByIndexRanges(sc, path.Index.ID, path.Ranges)
		if err != nil {
			return err
		}
	} else {
		path.TableFilters = conds
	}
	return nil
}

// deriveIndexPathStats will fulfill the information that the AccessPath need.
// And it will check whether this index is full matched by point query. We will use this check to
// determine whether we remove other paths or not.
// conds is the conditions used to generate the DetachRangeResult for path.
// isIm indicates whether this function is called to generate the partial path for IndexMerge.
func (ds *DataSource) deriveIndexPathStats(path *util.AccessPath, conds []expression.Expression, isIm bool) bool {
	sc := ds.ctx.GetSessionVars().StmtCtx
	if path.EqOrInCondCount == len(path.AccessConds) {
		accesses, remained := path.SplitCorColAccessCondFromFilters(path.EqOrInCondCount)
		path.AccessConds = append(path.AccessConds, accesses...)
		path.TableFilters = remained
		if len(accesses) > 0 && ds.statisticTable.Pseudo {
			path.CountAfterAccess = ds.statisticTable.PseudoAvgCountPerValue()
		} else {
			selectivity := path.CountAfterAccess / float64(ds.statisticTable.Count)
			for i := range accesses {
				col := path.IdxCols[path.EqOrInCondCount+i]
				ndv := ds.getColumnNDV(col.ID)
				ndv *= selectivity
				if ndv < 1 {
					ndv = 1.0
				}
				path.CountAfterAccess = path.CountAfterAccess / ndv
			}
		}
	}
	path.IndexFilters, path.TableFilters = splitIndexFilterConditions(path.TableFilters, path.FullIdxCols, path.FullIdxColLens, ds.tableInfo)
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.stats.RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.stats.RowCount/selectionFactor, float64(ds.statisticTable.Count))
	}
	if path.IndexFilters != nil {
		selectivity, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, path.IndexFilters, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = selectionFactor
		}
		if isIm {
			path.CountAfterIndex = path.CountAfterAccess * selectivity
		} else {
			path.CountAfterIndex = math.Max(path.CountAfterAccess*selectivity, ds.stats.RowCount)
		}
	}
	// Check whether there's only point query.
	noIntervalRanges := true
	haveNullVal := false
	for _, ran := range path.Ranges {
		// Not point or the not full matched.
		if !ran.IsPoint(sc) || len(ran.HighVal) != len(path.Index.Columns) {
			noIntervalRanges = false
			break
		}
		// Check whether there's null value.
		for i := 0; i < len(path.Index.Columns); i++ {
			if ran.HighVal[i].IsNull() {
				haveNullVal = true
				break
			}
		}
		if haveNullVal {
			break
		}
	}
	return noIntervalRanges && !haveNullVal
}

func getPKIsHandleColFromSchema(cols []*model.ColumnInfo, schema *expression.Schema, pkIsHandle bool) *expression.Column {
	if !pkIsHandle {
		// If the PKIsHandle is false, return the ExtraHandleColumn.
		for i, col := range cols {
			if col.ID == model.ExtraHandleID {
				return schema.Columns[i]
			}
		}
		return nil
	}
	for i, col := range cols {
		if mysql.HasPriKeyFlag(col.Flag) {
			return schema.Columns[i]
		}
	}
	return nil
}

func (ds *DataSource) getPKIsHandleCol() *expression.Column {
	return getPKIsHandleColFromSchema(ds.Columns, ds.schema, ds.tableInfo.PKIsHandle)
}

func (p *LogicalIndexScan) getPKIsHandleCol() *expression.Column {
	// We cannot use p.Source.getPKIsHandleCol() here,
	// Because we may re-prune p.Columns and p.schema during the transformation.
	// That will make p.Columns different from p.Source.Columns.
	return getPKIsHandleColFromSchema(p.Columns, p.schema, p.Source.tableInfo.PKIsHandle)
}

func (ds *DataSource) getHandleCol() *expression.Column {
	if ds.handleCol != nil {
		return ds.handleCol
	}

	if !ds.tableInfo.PKIsHandle {
		ds.handleCol = ds.newExtraHandleSchemaCol()
		return ds.handleCol
	}

	for i, col := range ds.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
			ds.handleCol = ds.schema.Columns[i]
			break
		}
	}

	return ds.handleCol
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

	ByItems []*ByItems
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

	ByItems []*ByItems
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

	Lock         ast.SelectLockType
	tblID2Handle map[int64][]*expression.Column
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

// ShowContents stores the contents for the `SHOW` statement.
type ShowContents struct {
	Tp          ast.ShowStmtType // Databases/Tables/Columns/....
	DBName      string
	Table       *ast.TableName  // Used for showing columns.
	Column      *ast.ColumnName // Used for `desc table column`.
	IndexName   model.CIStr
	Flag        int                  // Some flag parsed from sql, such as FULL.
	User        *auth.UserIdentity   // Used for show grants.
	Roles       []*auth.RoleIdentity // Used for show grants.
	Full        bool
	IfNotExists bool // Used for `show create database if not exists`.

	GlobalScope bool // Used by show variables.
}

// LogicalShow represents a show plan.
type LogicalShow struct {
	logicalSchemaProducer
	ShowContents
}

// LogicalShowDDLJobs is for showing DDL job list.
type LogicalShowDDLJobs struct {
	logicalSchemaProducer

	JobNumber int64
}
