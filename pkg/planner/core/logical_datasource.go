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

package core

import (
	"bytes"
	"fmt"
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace/logicaltrace"
	"github.com/pingcap/tidb/pkg/planner/util/tablesampler"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
)

// DataSource represents a tableScan without condition push down.
type DataSource struct {
	logicalop.LogicalSchemaProducer

	AstIndexHints []*ast.IndexHint
	IndexHints    []h.HintedIndex
	table         table.Table
	TableInfo     *model.TableInfo
	Columns       []*model.ColumnInfo
	DBName        model.CIStr

	TableAsName *model.CIStr
	// IndexMergeHints are the hint for indexmerge.
	IndexMergeHints []h.HintedIndex
	// PushedDownConds are the conditions that will be pushed down to coprocessor.
	PushedDownConds []expression.Expression
	// AllConds contains all the filters on this table. For now it's maintained
	// in predicate push down and used in partition pruning/index merge.
	AllConds []expression.Expression

	StatisticTable *statistics.Table
	TableStats     *property.StatsInfo

	// PossibleAccessPaths stores all the possible access path for physical plan, including table scan.
	PossibleAccessPaths []*util.AccessPath

	// The data source may be a partition, rather than a real table.
	PartitionDefIdx *int
	PhysicalTableID int64
	PartitionNames  []model.CIStr

	// handleCol represents the handle column for the datasource, either the
	// int primary key column or extra handle column.
	// handleCol *expression.Column
	HandleCols          util.HandleCols
	UnMutableHandleCols util.HandleCols
	// TblCols contains the original columns of table before being pruned, and it
	// is used for estimating table scan cost.
	TblCols []*expression.Column
	// CommonHandleCols and CommonHandleLens save the info of primary key which is the clustered index.
	CommonHandleCols []*expression.Column
	CommonHandleLens []int
	// TblColHists contains the Histogram of all original table columns,
	// it is converted from StatisticTable, and used for IO/network cost estimating.
	TblColHists *statistics.HistColl
	// PreferStoreType means the DataSource is enforced to which storage.
	PreferStoreType int
	// PreferPartitions store the map, the key represents store type, the value represents the partition name list.
	PreferPartitions map[int][]model.CIStr
	SampleInfo       *tablesampler.TableSampleInfo
	IS               infoschema.InfoSchema
	// IsForUpdateRead should be true in either of the following situations
	// 1. use `inside insert`, `update`, `delete` or `select for update` statement
	// 2. isolation level is RC
	IsForUpdateRead bool

	// contain unique index and the first field is tidb_shard(),
	// such as (tidb_shard(a), a ...), the fields are more than 2
	ContainExprPrefixUk bool

	// ColsRequiringFullLen is the columns that must be fetched with full length.
	// It is used to decide whether single scan is enough when reading from an index.
	ColsRequiringFullLen []*expression.Column

	// AccessPathMinSelectivity is the minimal selectivity among the access paths.
	// It's calculated after we generated the access paths and estimated row count for them, and before entering findBestTask.
	// It considers CountAfterIndex for index paths and CountAfterAccess for table paths and index merge paths.
	AccessPathMinSelectivity float64
}

// Init initializes DataSource.
func (ds DataSource) Init(ctx base.PlanContext, offset int) *DataSource {
	ds.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeDataSource, &ds, offset)
	return &ds
}

// *************************** start implementation of Plan interface ***************************

// ExplainInfo implements Plan interface.
func (ds *DataSource) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	tblName := ds.TableInfo.Name.O
	if ds.TableAsName != nil && ds.TableAsName.O != "" {
		tblName = ds.TableAsName.O
	}
	fmt.Fprintf(buffer, "table:%s", tblName)
	if ds.PartitionDefIdx != nil {
		if pi := ds.TableInfo.GetPartitionInfo(); pi != nil {
			fmt.Fprintf(buffer, ", partition:%s", pi.Definitions[*ds.PartitionDefIdx].Name.O)
		}
	}
	return buffer.String()
}

// *************************** end implementation of Plan interface ****************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.<0th> interface.

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (ds *DataSource) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	predicates = expression.PropagateConstant(ds.SCtx().GetExprCtx(), predicates)
	predicates = DeleteTrueExprs(ds, predicates)
	// Add tidb_shard() prefix to the condtion for shard index in some scenarios
	// TODO: remove it to the place building logical plan
	predicates = ds.AddPrefix4ShardIndexes(ds.SCtx(), predicates)
	ds.AllConds = predicates
	ds.PushedDownConds, predicates = expression.PushDownExprs(GetPushDownCtx(ds.SCtx()), predicates, kv.UnSpecified)
	appendDataSourcePredicatePushDownTraceStep(ds, opt)
	return predicates, ds
}

// PruneColumns implements base.LogicalPlan.<2nd> interface.
func (ds *DataSource) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	used := expression.GetUsedList(ds.SCtx().GetExprCtx().GetEvalCtx(), parentUsedCols, ds.Schema())

	exprCols := expression.ExtractColumnsFromExpressions(nil, ds.AllConds, nil)
	exprUsed := expression.GetUsedList(ds.SCtx().GetExprCtx().GetEvalCtx(), exprCols, ds.Schema())
	prunedColumns := make([]*expression.Column, 0)

	originSchemaColumns := ds.Schema().Columns
	originColumns := ds.Columns

	ds.ColsRequiringFullLen = make([]*expression.Column, 0, len(used))
	for i, col := range ds.Schema().Columns {
		if used[i] || (ds.ContainExprPrefixUk && expression.GcColumnExprIsTidbShard(col.VirtualExpr)) {
			ds.ColsRequiringFullLen = append(ds.ColsRequiringFullLen, col)
		}
	}

	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] && !exprUsed[i] {
			// If ds has a shard index, and the column is generated column by `tidb_shard()`
			// it can't prune the generated column of shard index
			if ds.ContainExprPrefixUk &&
				expression.GcColumnExprIsTidbShard(ds.Schema().Columns[i].VirtualExpr) {
				continue
			}
			prunedColumns = append(prunedColumns, ds.Schema().Columns[i])
			ds.Schema().Columns = append(ds.Schema().Columns[:i], ds.Schema().Columns[i+1:]...)
			ds.Columns = append(ds.Columns[:i], ds.Columns[i+1:]...)
		}
	}
	logicaltrace.AppendColumnPruneTraceStep(ds, prunedColumns, opt)
	addOneHandle := false
	// For SQL like `select 1 from t`, tikv's response will be empty if no column is in schema.
	// So we'll force to push one if schema doesn't have any column.
	if ds.Schema().Len() == 0 {
		var handleCol *expression.Column
		var handleColInfo *model.ColumnInfo
		handleCol, handleColInfo = preferKeyColumnFromTable(ds, originSchemaColumns, originColumns)
		ds.Columns = append(ds.Columns, handleColInfo)
		ds.Schema().Append(handleCol)
		addOneHandle = true
	}
	// ref: https://github.com/pingcap/tidb/issues/44579
	// when first entering columnPruner, we kept a column-a in datasource since upper agg function count(a) is used.
	//		then we mark the HandleCols as nil here.
	// when second entering columnPruner, the count(a) is eliminated since it always not null. we should fill another
	// 		extra col, in this way, handle col is useful again, otherwise, _tidb_rowid will be filled.
	if ds.HandleCols != nil && ds.HandleCols.IsInt() && ds.Schema().ColumnIndex(ds.HandleCols.GetCol(0)) == -1 {
		ds.HandleCols = nil
	}
	// Current DataSource operator contains all the filters on this table, and the columns used by these filters are always included
	// in the output schema. Even if they are not needed by DataSource's parent operator. Thus add a projection here to prune useless columns
	// Limit to MPP tasks, because TiKV can't benefit from this now(projection can't be pushed down to TiKV now).
	// If the parent operator need no columns from the DataSource, we return the smallest column. Don't add the empty proj.
	if !addOneHandle && ds.Schema().Len() > len(parentUsedCols) && len(parentUsedCols) > 0 && ds.SCtx().GetSessionVars().IsMPPEnforced() && ds.TableInfo.TiFlashReplica != nil {
		proj := logicalop.LogicalProjection{
			Exprs: expression.Column2Exprs(parentUsedCols),
		}.Init(ds.SCtx(), ds.QueryBlockOffset())
		proj.SetStats(ds.StatsInfo())
		proj.SetSchema(expression.NewSchema(parentUsedCols...))
		proj.SetChildren(ds)
		return proj, nil
	}
	return ds, nil
}

// FindBestTask implements the base.LogicalPlan.<3rd> interface.
// It will enumerate all the available indices and choose a plan with least cost.
func (ds *DataSource) FindBestTask(prop *property.PhysicalProperty, planCounter *base.PlanCounterTp, opt *optimizetrace.PhysicalOptimizeOp) (t base.Task, cntPlan int64, err error) {
	return findBestTask4DS(ds, prop, planCounter, opt)
}

// BuildKeyInfo implements base.LogicalPlan.<4th> interface.
func (ds *DataSource) BuildKeyInfo(selfSchema *expression.Schema, _ []*expression.Schema) {
	selfSchema.Keys = nil
	var latestIndexes map[int64]*model.IndexInfo
	var changed bool
	var err error
	check := ds.SCtx().GetSessionVars().IsIsolation(ast.ReadCommitted) || ds.IsForUpdateRead
	check = check && ds.SCtx().GetSessionVars().ConnectionID > 0
	// we should check index valid while forUpdateRead, see detail in https://github.com/pingcap/tidb/pull/22152
	if check {
		latestIndexes, changed, err = getLatestIndexInfo(ds.SCtx(), ds.table.Meta().ID, 0)
		if err != nil {
			return
		}
	}
	for _, index := range ds.table.Meta().Indices {
		if ds.IsForUpdateRead && changed {
			latestIndex, ok := latestIndexes[index.ID]
			if !ok || latestIndex.State != model.StatePublic {
				continue
			}
		} else if index.State != model.StatePublic {
			continue
		}
		if uniqueKey, newKey := ruleutil.CheckIndexCanBeKey(index, ds.Columns, selfSchema); newKey != nil {
			selfSchema.Keys = append(selfSchema.Keys, newKey)
		} else if uniqueKey != nil {
			selfSchema.UniqueKeys = append(selfSchema.UniqueKeys, uniqueKey)
		}
	}
	if ds.TableInfo.PKIsHandle {
		for i, col := range ds.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				selfSchema.Keys = append(selfSchema.Keys, []*expression.Column{selfSchema.Columns[i]})
				break
			}
		}
	}
}

// PushDownTopN inherits BaseLogicalPlan.LogicalPlan.<5th> interface.

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification implements the base.LogicalPlan.<7th> interface.
func (ds *DataSource) PredicateSimplification(*optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	p := ds.Self().(*DataSource)
	p.PushedDownConds = applyPredicateSimplification(p.SCtx(), p.PushedDownConds)
	p.AllConds = applyPredicateSimplification(p.SCtx(), p.AllConds)
	return p
}

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implements base.LogicalPlan.<11th> interface.
func (ds *DataSource) DeriveStats(_ []*property.StatsInfo, _ *expression.Schema, _ []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	if ds.StatsInfo() != nil && len(colGroups) == 0 {
		return ds.StatsInfo(), nil
	}
	ds.initStats(colGroups)
	if ds.StatsInfo() != nil {
		// Just reload the GroupNDVs.
		selectivity := ds.StatsInfo().RowCount / ds.TableStats.RowCount
		ds.SetStats(ds.TableStats.Scale(selectivity))
		return ds.StatsInfo(), nil
	}
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
	// two preprocess here.
	// 1: PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	// 2: EliminateNoPrecisionCast here can convert query 'cast(c<int> as bigint) = 1' to 'c = 1' to leverage access range.
	exprCtx := ds.SCtx().GetExprCtx()
	for i, expr := range ds.PushedDownConds {
		ds.PushedDownConds[i] = expression.PushDownNot(exprCtx, expr)
		ds.PushedDownConds[i] = expression.EliminateNoPrecisionLossCast(exprCtx, ds.PushedDownConds[i])
	}
	for _, path := range ds.PossibleAccessPaths {
		if path.IsTablePath() {
			continue
		}
		err := ds.fillIndexPath(path, ds.PushedDownConds)
		if err != nil {
			return nil, err
		}
	}
	// TODO: Can we move ds.deriveStatsByFilter after pruning by heuristics? In this way some computation can be avoided
	// when ds.PossibleAccessPaths are pruned.
	ds.SetStats(ds.deriveStatsByFilter(ds.PushedDownConds, ds.PossibleAccessPaths))
	err := ds.derivePathStatsAndTryHeuristics()
	if err != nil {
		return nil, err
	}

	if err := ds.generateIndexMergePath(); err != nil {
		return nil, err
	}

	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugTraceAccessPaths(ds.SCtx(), ds.PossibleAccessPaths)
	}
	ds.AccessPathMinSelectivity = getMinSelectivityFromPaths(ds.PossibleAccessPaths, float64(ds.TblColHists.RealtimeCount))

	return ds.StatsInfo(), nil
}

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (ds *DataSource) PreparePossibleProperties(_ *expression.Schema, _ ...[][]*expression.Column) [][]*expression.Column {
	result := make([][]*expression.Column, 0, len(ds.PossibleAccessPaths))

	for _, path := range ds.PossibleAccessPaths {
		if path.IsIntHandlePath {
			col := ds.getPKIsHandleCol()
			if col != nil {
				result = append(result, []*expression.Column{col})
			}
			continue
		}

		if len(path.IdxCols) == 0 {
			continue
		}
		result = append(result, make([]*expression.Column, len(path.IdxCols)))
		copy(result[len(result)-1], path.IdxCols)
		for i := 0; i < path.EqCondCount && i+1 < len(path.IdxCols); i++ {
			result = append(result, make([]*expression.Column, len(path.IdxCols)-i-1))
			copy(result[len(result)-1], path.IdxCols[i+1:])
		}
	}
	return result
}

// ExhaustPhysicalPlans inherits BaseLogicalPlan.LogicalPlan.<14th> implementation.

// ExtractCorrelatedCols implements base.LogicalPlan.<15th> interface.
func (ds *DataSource) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(ds.PushedDownConds))
	for _, expr := range ds.PushedDownConds {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
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
func (ds *DataSource) ExtractFD() *fd.FDSet {
	// FD in datasource (leaf node) can be cached and reused.
	// Once the all conditions are not equal to nil, built it again.
	if ds.FDs() == nil || ds.AllConds != nil {
		fds := &fd.FDSet{HashCodeToUniqueID: make(map[string]int)}
		allCols := intset.NewFastIntSet()
		// should use the column's unique ID avoiding fdSet conflict.
		for _, col := range ds.TblCols {
			// todo: change it to int64
			allCols.Insert(int(col.UniqueID))
		}
		// int pk doesn't store its index column in indexInfo.
		if ds.TableInfo.PKIsHandle {
			keyCols := intset.NewFastIntSet()
			for _, col := range ds.TblCols {
				if mysql.HasPriKeyFlag(col.RetType.GetFlag()) {
					keyCols.Insert(int(col.UniqueID))
				}
			}
			fds.AddStrictFunctionalDependency(keyCols, allCols)
			fds.MakeNotNull(keyCols)
		}
		// we should check index valid while forUpdateRead, see detail in https://github.com/pingcap/tidb/pull/22152
		var (
			latestIndexes map[int64]*model.IndexInfo
			changed       bool
			err           error
		)
		check := ds.SCtx().GetSessionVars().IsIsolation(ast.ReadCommitted) || ds.IsForUpdateRead
		check = check && ds.SCtx().GetSessionVars().ConnectionID > 0
		if check {
			latestIndexes, changed, err = getLatestIndexInfo(ds.SCtx(), ds.table.Meta().ID, 0)
			if err != nil {
				ds.SetFDs(fds)
				return fds
			}
		}
		// other indices including common handle.
		for _, idx := range ds.TableInfo.Indices {
			keyCols := intset.NewFastIntSet()
			allColIsNotNull := true
			if ds.IsForUpdateRead && changed {
				latestIndex, ok := latestIndexes[idx.ID]
				if !ok || latestIndex.State != model.StatePublic {
					continue
				}
			}
			if idx.State != model.StatePublic {
				continue
			}
			for _, idxCol := range idx.Columns {
				// Note: even the prefix column can also be the FD. For example:
				// unique(char_column(10)), will also guarantee the prefix to be
				// the unique which means the while column is unique too.
				refCol := ds.TableInfo.Columns[idxCol.Offset]
				if !mysql.HasNotNullFlag(refCol.GetFlag()) {
					allColIsNotNull = false
				}
				keyCols.Insert(int(ds.TblCols[idxCol.Offset].UniqueID))
			}
			if idx.Primary {
				fds.AddStrictFunctionalDependency(keyCols, allCols)
				fds.MakeNotNull(keyCols)
			} else if idx.Unique {
				if allColIsNotNull {
					fds.AddStrictFunctionalDependency(keyCols, allCols)
					fds.MakeNotNull(keyCols)
				} else {
					// unique index:
					// 1: normal value should be unique
					// 2: null value can be multiple
					// for this kind of lax to be strict, we need to make the determinant not-null.
					fds.AddLaxFunctionalDependency(keyCols, allCols)
				}
			}
		}
		// handle the datasource conditions (maybe pushed down from upper layer OP)
		if len(ds.AllConds) != 0 {
			// extract the not null attributes from selection conditions.
			notnullColsUniqueIDs := ExtractNotNullFromConds(ds.AllConds, ds)

			// extract the constant cols from selection conditions.
			constUniqueIDs := ExtractConstantCols(ds.AllConds, ds.SCtx(), fds)

			// extract equivalence cols.
			equivUniqueIDs := ExtractEquivalenceCols(ds.AllConds, ds.SCtx(), fds)

			// apply conditions to FD.
			fds.MakeNotNull(notnullColsUniqueIDs)
			fds.AddConstants(constUniqueIDs)
			for _, equiv := range equivUniqueIDs {
				fds.AddEquivalence(equiv[0], equiv[1])
			}
		}
		// build the dependency for generated columns.
		// the generated column is sequentially dependent on the forward column.
		// a int, b int as (a+1), c int as (b+1), here we can build the strict FD down:
		// {a} -> {b}, {b} -> {c}, put the maintenance of the dependencies between generated columns to the FD graph.
		notNullCols := intset.NewFastIntSet()
		for _, col := range ds.TblCols {
			if col.VirtualExpr != nil {
				dependencies := intset.NewFastIntSet()
				dependencies.Insert(int(col.UniqueID))
				// dig out just for 1 level.
				directBaseCol := expression.ExtractColumns(col.VirtualExpr)
				determinant := intset.NewFastIntSet()
				for _, col := range directBaseCol {
					determinant.Insert(int(col.UniqueID))
				}
				fds.AddStrictFunctionalDependency(determinant, dependencies)
			}
			if mysql.HasNotNullFlag(col.RetType.GetFlag()) {
				notNullCols.Insert(int(col.UniqueID))
			}
		}
		fds.MakeNotNull(notNullCols)
		ds.SetFDs(fds)
	}
	return ds.FDs()
}

// GetBaseLogicalPlan inherits BaseLogicalPlan.LogicalPlan.<23rd> implementation.

// ConvertOuterToInnerJoin inherits BaseLogicalPlan.LogicalPlan.<24th> implementation.

// *************************** end implementation of logicalPlan interface ***************************

// getTablePath finds the TablePath from a group of accessPaths.
func getTablePath(paths []*util.AccessPath) *util.AccessPath {
	for _, path := range paths {
		if path.IsTablePath() {
			return path
		}
	}
	return nil
}

func (ds *DataSource) buildTableGather() base.LogicalPlan {
	ts := LogicalTableScan{Source: ds, HandleCols: ds.HandleCols}.Init(ds.SCtx(), ds.QueryBlockOffset())
	ts.SetSchema(ds.Schema())
	sg := TiKVSingleGather{Source: ds, IsIndexGather: false}.Init(ds.SCtx(), ds.QueryBlockOffset())
	sg.SetSchema(ds.Schema())
	sg.SetChildren(ts)
	return sg
}

func (ds *DataSource) buildIndexGather(path *util.AccessPath) base.LogicalPlan {
	is := LogicalIndexScan{
		Source:         ds,
		IsDoubleRead:   false,
		Index:          path.Index,
		FullIdxCols:    path.FullIdxCols,
		FullIdxColLens: path.FullIdxColLens,
		IdxCols:        path.IdxCols,
		IdxColLens:     path.IdxColLens,
	}.Init(ds.SCtx(), ds.QueryBlockOffset())

	is.Columns = make([]*model.ColumnInfo, len(ds.Columns))
	copy(is.Columns, ds.Columns)
	is.SetSchema(ds.Schema())
	is.IdxCols, is.IdxColLens = expression.IndexInfo2PrefixCols(is.Columns, is.Schema().Columns, is.Index)

	sg := TiKVSingleGather{
		Source:        ds,
		IsIndexGather: true,
		Index:         path.Index,
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	sg.SetSchema(ds.Schema())
	sg.SetChildren(is)
	return sg
}

// Convert2Gathers builds logical TiKVSingleGathers from DataSource.
func (ds *DataSource) Convert2Gathers() (gathers []base.LogicalPlan) {
	tg := ds.buildTableGather()
	gathers = append(gathers, tg)
	for _, path := range ds.PossibleAccessPaths {
		if !path.IsIntHandlePath {
			path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.Schema().Columns, path.Index)
			path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.Schema().Columns, path.Index)
			// If index columns can cover all of the needed columns, we can use a IndexGather + IndexScan.
			if ds.isSingleScan(path.FullIdxCols, path.FullIdxColLens) {
				gathers = append(gathers, ds.buildIndexGather(path))
			}
			// TODO: If index columns can not cover the schema, use IndexLookUpGather.
		}
	}
	return gathers
}

func detachCondAndBuildRangeForPath(
	sctx base.PlanContext,
	path *util.AccessPath,
	conds []expression.Expression,
	histColl *statistics.HistColl,
) error {
	if len(path.IdxCols) == 0 {
		path.TableFilters = conds
		return nil
	}
	res, err := ranger.DetachCondAndBuildRangeForIndex(sctx.GetRangerCtx(), conds, path.IdxCols, path.IdxColLens, sctx.GetSessionVars().RangeMaxSize)
	if err != nil {
		return err
	}
	path.Ranges = res.Ranges
	path.AccessConds = res.AccessConds
	path.TableFilters = res.RemainedConds
	path.EqCondCount = res.EqCondCount
	path.EqOrInCondCount = res.EqOrInCount
	path.IsDNFCond = res.IsDNFCond
	path.ConstCols = make([]bool, len(path.IdxCols))
	if res.ColumnValues != nil {
		for i := range path.ConstCols {
			path.ConstCols[i] = res.ColumnValues[i] != nil
		}
	}
	path.CountAfterAccess, err = cardinality.GetRowCountByIndexRanges(sctx, histColl, path.Index.ID, path.Ranges)
	return err
}

func (ds *DataSource) deriveCommonHandleTablePathStats(path *util.AccessPath, conds []expression.Expression, isIm bool) error {
	path.CountAfterAccess = float64(ds.StatisticTable.RealtimeCount)
	path.Ranges = ranger.FullNotNullRange()
	path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.Schema().Columns, path.Index)
	path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.Schema().Columns, path.Index)
	if len(conds) == 0 {
		return nil
	}
	if err := detachCondAndBuildRangeForPath(ds.SCtx(), path, conds, ds.TableStats.HistColl); err != nil {
		return err
	}
	if path.EqOrInCondCount == len(path.AccessConds) {
		accesses, remained := path.SplitCorColAccessCondFromFilters(ds.SCtx(), path.EqOrInCondCount)
		path.AccessConds = append(path.AccessConds, accesses...)
		path.TableFilters = remained
		if len(accesses) > 0 && ds.StatisticTable.Pseudo {
			path.CountAfterAccess = cardinality.PseudoAvgCountPerValue(ds.StatisticTable)
		} else {
			selectivity := path.CountAfterAccess / float64(ds.StatisticTable.RealtimeCount)
			for i := range accesses {
				col := path.IdxCols[path.EqOrInCondCount+i]
				ndv := cardinality.EstimateColumnNDV(ds.StatisticTable, col.ID)
				ndv *= selectivity
				if ndv < 1 {
					ndv = 1.0
				}
				path.CountAfterAccess = path.CountAfterAccess / ndv
			}
		}
	}
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.StatsInfo().RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.StatsInfo().RowCount/cost.SelectionFactor, float64(ds.StatisticTable.RealtimeCount))
	}
	return nil
}

// deriveTablePathStats will fulfill the information that the AccessPath need.
// isIm indicates whether this function is called to generate the partial path for IndexMerge.
func (ds *DataSource) deriveTablePathStats(path *util.AccessPath, conds []expression.Expression, isIm bool) error {
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
	if path.IsCommonHandlePath {
		return ds.deriveCommonHandleTablePathStats(path, conds, isIm)
	}
	var err error
	path.CountAfterAccess = float64(ds.StatisticTable.RealtimeCount)
	path.TableFilters = conds
	var pkCol *expression.Column
	isUnsigned := false
	if ds.TableInfo.PKIsHandle {
		if pkColInfo := ds.TableInfo.GetPkColInfo(); pkColInfo != nil {
			isUnsigned = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
			pkCol = expression.ColInfo2Col(ds.Schema().Columns, pkColInfo)
		}
	} else {
		pkCol = ds.Schema().GetExtraHandleColumn()
	}
	if pkCol == nil {
		path.Ranges = ranger.FullIntRange(isUnsigned)
		return nil
	}

	path.Ranges = ranger.FullIntRange(isUnsigned)
	if len(conds) == 0 {
		return nil
	}
	// for cnf condition combination, c=1 and c=2 and (1 member of (a)),
	// c=1 and c=2 will derive invalid range represented by an access condition as constant of 0 (false).
	// later this constant of 0 will be built as empty range.
	path.AccessConds, path.TableFilters = ranger.DetachCondsForColumn(ds.SCtx().GetRangerCtx(), conds, pkCol)
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
			if lOk && lCol.Equal(ds.SCtx().GetExprCtx().GetEvalCtx(), pkCol) {
				_, rOk := eqFunc.GetArgs()[1].(*expression.CorrelatedColumn)
				if rOk {
					path.AccessConds = append(path.AccessConds, filter)
					path.TableFilters = append(path.TableFilters[:i], path.TableFilters[i+1:]...)
					corColInAccessConds = true
					break
				}
			}
			rCol, rOk := eqFunc.GetArgs()[1].(*expression.Column)
			if rOk && rCol.Equal(ds.SCtx().GetExprCtx().GetEvalCtx(), pkCol) {
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
		return nil
	}
	var remainedConds []expression.Expression
	path.Ranges, path.AccessConds, remainedConds, err = ranger.BuildTableRange(path.AccessConds, ds.SCtx().GetRangerCtx(), pkCol.RetType, ds.SCtx().GetSessionVars().RangeMaxSize)
	path.TableFilters = append(path.TableFilters, remainedConds...)
	if err != nil {
		return err
	}
	path.CountAfterAccess, err = cardinality.GetRowCountByIntColumnRanges(ds.SCtx(), &ds.StatisticTable.HistColl, pkCol.ID, path.Ranges)
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.StatsInfo().RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.StatsInfo().RowCount/cost.SelectionFactor, float64(ds.StatisticTable.RealtimeCount))
	}
	return err
}

func (ds *DataSource) fillIndexPath(path *util.AccessPath, conds []expression.Expression) error {
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
	path.Ranges = ranger.FullRange()
	path.CountAfterAccess = float64(ds.StatisticTable.RealtimeCount)
	path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.Schema().Columns, path.Index)
	path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.Schema().Columns, path.Index)
	if !path.Index.Unique && !path.Index.Primary && len(path.Index.Columns) == len(path.IdxCols) {
		handleCol := ds.getPKIsHandleCol()
		if handleCol != nil && !mysql.HasUnsignedFlag(handleCol.RetType.GetFlag()) {
			alreadyHandle := false
			for _, col := range path.IdxCols {
				if col.ID == model.ExtraHandleID || col.EqualColumn(handleCol) {
					alreadyHandle = true
				}
			}
			// Don't add one column twice to the index. May cause unexpected errors.
			if !alreadyHandle {
				path.IdxCols = append(path.IdxCols, handleCol)
				path.IdxColLens = append(path.IdxColLens, types.UnspecifiedLength)
				// Also updates the map that maps the index id to its prefix column ids.
				if len(ds.TableStats.HistColl.Idx2ColUniqueIDs[path.Index.ID]) == len(path.Index.Columns) {
					ds.TableStats.HistColl.Idx2ColUniqueIDs[path.Index.ID] = append(ds.TableStats.HistColl.Idx2ColUniqueIDs[path.Index.ID], handleCol.UniqueID)
				}
			}
		}
	}
	err := detachCondAndBuildRangeForPath(ds.SCtx(), path, conds, ds.TableStats.HistColl)
	return err
}

// deriveIndexPathStats will fulfill the information that the AccessPath need.
// conds is the conditions used to generate the DetachRangeResult for path.
// isIm indicates whether this function is called to generate the partial path for IndexMerge.
func (ds *DataSource) deriveIndexPathStats(path *util.AccessPath, _ []expression.Expression, isIm bool) {
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
	if path.EqOrInCondCount == len(path.AccessConds) {
		accesses, remained := path.SplitCorColAccessCondFromFilters(ds.SCtx(), path.EqOrInCondCount)
		path.AccessConds = append(path.AccessConds, accesses...)
		path.TableFilters = remained
		if len(accesses) > 0 && ds.StatisticTable.Pseudo {
			path.CountAfterAccess = cardinality.PseudoAvgCountPerValue(ds.StatisticTable)
		} else {
			selectivity := path.CountAfterAccess / float64(ds.StatisticTable.RealtimeCount)
			for i := range accesses {
				col := path.IdxCols[path.EqOrInCondCount+i]
				ndv := cardinality.EstimateColumnNDV(ds.StatisticTable, col.ID)
				ndv *= selectivity
				if ndv < 1 {
					ndv = 1.0
				}
				path.CountAfterAccess = path.CountAfterAccess / ndv
			}
		}
	}
	var indexFilters []expression.Expression
	indexFilters, path.TableFilters = ds.splitIndexFilterConditions(path.TableFilters, path.FullIdxCols, path.FullIdxColLens)
	path.IndexFilters = append(path.IndexFilters, indexFilters...)
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.StatsInfo().RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.StatsInfo().RowCount/cost.SelectionFactor, float64(ds.StatisticTable.RealtimeCount))
	}
	if path.IndexFilters != nil {
		selectivity, _, err := cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, path.IndexFilters, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = cost.SelectionFactor
		}
		if isIm {
			path.CountAfterIndex = path.CountAfterAccess * selectivity
		} else {
			path.CountAfterIndex = math.Max(path.CountAfterAccess*selectivity, ds.StatsInfo().RowCount)
		}
	} else {
		path.CountAfterIndex = path.CountAfterAccess
	}
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
		if mysql.HasPriKeyFlag(col.GetFlag()) {
			return schema.Columns[i]
		}
	}
	return nil
}

func (ds *DataSource) getPKIsHandleCol() *expression.Column {
	return getPKIsHandleColFromSchema(ds.Columns, ds.Schema(), ds.TableInfo.PKIsHandle)
}

func getMinSelectivityFromPaths(paths []*util.AccessPath, totalRowCount float64) float64 {
	minSelectivity := 1.0
	if totalRowCount <= 0 {
		return minSelectivity
	}
	for _, path := range paths {
		// For table path and index merge path, AccessPath.CountAfterIndex is not set and meaningless,
		// but we still consider their AccessPath.CountAfterAccess.
		if path.IsTablePath() || path.PartialIndexPaths != nil {
			minSelectivity = min(minSelectivity, path.CountAfterAccess/totalRowCount)
			continue
		}
		minSelectivity = min(minSelectivity, path.CountAfterIndex/totalRowCount)
	}
	return minSelectivity
}

// AddPrefix4ShardIndexes add expression prefix for shard index. e.g. an index is test.uk(tidb_shard(a), a).
// DataSource.PredicatePushDown ---> DataSource.AddPrefix4ShardIndexes
// It transforms the sql "SELECT * FROM test WHERE a = 10" to
// "SELECT * FROM test WHERE tidb_shard(a) = val AND a = 10", val is the value of tidb_shard(10).
// It also transforms the sql "SELECT * FROM test WHERE a IN (10, 20, 30)" to
// "SELECT * FROM test WHERE tidb_shard(a) = val1 AND a = 10 OR tidb_shard(a) = val2 AND a = 20"
// @param[in] conds            the original condtion of this datasource
// @retval - the new condition after adding expression prefix
func (ds *DataSource) AddPrefix4ShardIndexes(sc base.PlanContext, conds []expression.Expression) []expression.Expression {
	if !ds.ContainExprPrefixUk {
		return conds
	}

	var err error
	newConds := conds

	for _, path := range ds.PossibleAccessPaths {
		if !path.IsUkShardIndexPath {
			continue
		}
		newConds, err = ds.addExprPrefixCond(sc, path, newConds)
		if err != nil {
			logutil.BgLogger().Error("Add tidb_shard expression failed",
				zap.Error(err),
				zap.Uint64("connection id", sc.GetSessionVars().ConnectionID),
				zap.String("database name", ds.DBName.L),
				zap.String("table name", ds.TableInfo.Name.L),
				zap.String("index name", path.Index.Name.L))
			return conds
		}
	}

	return newConds
}
