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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/planner/util/tablesampler"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/set"
	"go.uber.org/zap"
)

func (b *PlanBuilder) buildDataSource(ctx context.Context, tn *ast.TableName, asName *ast.CIStr) (base.LogicalPlan, error) {
	b.optFlag |= rule.FlagPredicateSimplification
	dbName := tn.Schema
	sessionVars := b.ctx.GetSessionVars()

	if dbName.L == "" {
		// Try CTE.
		p, err := b.tryBuildCTE(ctx, tn, asName)
		if err != nil || p != nil {
			return p, err
		}
		dbName = ast.NewCIStr(sessionVars.CurrentDB)
	}

	is := b.is
	if len(b.buildingViewStack) > 0 {
		// For tables in view, always ignore local temporary table, considering the below case:
		// If a user created a normal table `t1` and a view `v1` referring `t1`, and then a local temporary table with a same name `t1` is created.
		// At this time, executing 'select * from v1' should still return all records from normal table `t1` instead of temporary table `t1`.
		is = temptable.DetachLocalTemporaryTableInfoSchema(is)
	}

	tbl, err := is.TableByName(ctx, dbName, tn.Name)
	if err != nil {
		return nil, err
	}

	tbl, err = tryLockMDLAndUpdateSchemaIfNecessary(ctx, b.ctx, dbName, tbl, b.is)
	if err != nil {
		return nil, err
	}
	tableInfo := tbl.Meta()

	if b.isCreateView && tableInfo.TempTableType == model.TempTableLocal {
		return nil, plannererrors.ErrViewSelectTemporaryTable.GenWithStackByArgs(tn.Name)
	}

	var authErr error
	if sessionVars.User != nil {
		authErr = plannererrors.ErrTableaccessDenied.FastGenByArgs("SELECT", sessionVars.User.AuthUsername, sessionVars.User.AuthHostname, tableInfo.Name.L)
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, dbName.L, tableInfo.Name.L, "", authErr)

	if tbl.Type().IsVirtualTable() {
		if tn.TableSample != nil {
			return nil, expression.ErrInvalidTableSample.GenWithStackByArgs("Unsupported TABLESAMPLE in virtual tables")
		}
		return b.buildMemTable(ctx, dbName, tableInfo)
	}

	tblName := *asName
	if tblName.L == "" {
		tblName = tn.Name
	}

	if tableInfo.GetPartitionInfo() != nil {
		// If `UseDynamicPruneMode` already been false, then we don't need to check whether execute `flagPartitionProcessor`
		// otherwise we need to check global stats initialized for each partition table
		if !b.ctx.GetSessionVars().IsDynamicPartitionPruneEnabled() {
			b.optFlag = b.optFlag | rule.FlagPartitionProcessor
		} else {
			if !b.ctx.GetSessionVars().StmtCtx.UseDynamicPruneMode {
				b.optFlag = b.optFlag | rule.FlagPartitionProcessor
			} else {
				h := domain.GetDomain(b.ctx).StatsHandle()
				tblStats := h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
				isDynamicEnabled := b.ctx.GetSessionVars().IsDynamicPartitionPruneEnabled()
				globalStatsReady := tblStats.IsAnalyzed()
				skipMissingPartition := b.ctx.GetSessionVars().SkipMissingPartitionStats
				// If we already enabled the tidb_skip_missing_partition_stats, the global stats can be treated as exist.
				allowDynamicWithoutStats := fixcontrol.GetBoolWithDefault(b.ctx.GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix44262, skipMissingPartition)

				// If dynamic partition prune isn't enabled or global stats is not ready, we won't enable dynamic prune mode in query
				usePartitionProcessor := !isDynamicEnabled || (!globalStatsReady && !allowDynamicWithoutStats)

				failpoint.Inject("forceDynamicPrune", func(val failpoint.Value) {
					if val.(bool) {
						if isDynamicEnabled {
							usePartitionProcessor = false
						}
					}
				})

				if usePartitionProcessor {
					b.optFlag = b.optFlag | rule.FlagPartitionProcessor
					b.ctx.GetSessionVars().StmtCtx.UseDynamicPruneMode = false
					if isDynamicEnabled {
						b.ctx.GetSessionVars().StmtCtx.AppendWarning(
							fmt.Errorf("disable dynamic pruning due to %s has no global stats", tableInfo.Name.String()))
					}
				}
			}
		}
		pt := tbl.(table.PartitionedTable)
		// check partition by name.
		if len(tn.PartitionNames) > 0 {
			pids := make(map[int64]struct{}, len(tn.PartitionNames))
			for _, name := range tn.PartitionNames {
				pid, err := tables.FindPartitionByName(tableInfo, name.L)
				if err != nil {
					return nil, err
				}
				pids[pid] = struct{}{}
			}
			pt = tables.NewPartitionTableWithGivenSets(pt, pids)
		}
		b.partitionedTable = append(b.partitionedTable, pt)
	} else if len(tn.PartitionNames) != 0 {
		return nil, plannererrors.ErrPartitionClauseOnNonpartitioned
	}

	possiblePaths, err := getPossibleAccessPaths(b.ctx, b.TableHints(), tn.IndexHints, tbl, dbName, tblName, b.isForUpdateRead, b.optFlag&rule.FlagPartitionProcessor > 0)
	if err != nil {
		return nil, err
	}

	if tableInfo.IsView() {
		if tn.TableSample != nil {
			return nil, expression.ErrInvalidTableSample.GenWithStackByArgs("Unsupported TABLESAMPLE in views")
		}

		// Get the hints belong to the current view.
		currentQBNameMap4View := make(map[string][]ast.HintTable)
		currentViewHints := make(map[string][]*ast.TableOptimizerHint)
		for qbName, viewQBNameHintTable := range b.hintProcessor.ViewQBNameToTable {
			if len(viewQBNameHintTable) == 0 {
				continue
			}
			viewSelectOffset := b.getSelectOffset()

			var viewHintSelectOffset int
			if viewQBNameHintTable[0].QBName.L == "" {
				// If we do not explicit set the qbName, we will set the empty qb name to @sel_1.
				viewHintSelectOffset = 1
			} else {
				viewHintSelectOffset = b.hintProcessor.GetHintOffset(viewQBNameHintTable[0].QBName, viewSelectOffset)
			}

			// Check whether the current view can match the view name in the hint.
			if viewQBNameHintTable[0].TableName.L == tblName.L && viewHintSelectOffset == viewSelectOffset {
				// If the view hint can match the current view, we pop the first view table in the query block hint's table list.
				// It means the hint belong the current view, the first view name in hint is matched.
				// Because of the nested views, so we should check the left table list in hint when build the data source from the view inside the current view.
				currentQBNameMap4View[qbName] = viewQBNameHintTable[1:]
				currentViewHints[qbName] = b.hintProcessor.ViewQBNameToHints[qbName]
				b.hintProcessor.ViewQBNameUsed[qbName] = struct{}{}
			}
		}
		return b.BuildDataSourceFromView(ctx, dbName, tableInfo, currentQBNameMap4View, currentViewHints)
	}

	if tableInfo.IsSequence() {
		if tn.TableSample != nil {
			return nil, expression.ErrInvalidTableSample.GenWithStackByArgs("Unsupported TABLESAMPLE in sequences")
		}
		// When the source is a Sequence, we convert it to a TableDual, as what most databases do.
		return b.buildTableDual(), nil
	}

	// remain tikv access path to generate point get acceess path if existed
	// see detail in issue: https://github.com/pingcap/tidb/issues/39543
	if !(b.isForUpdateRead && b.ctx.GetSessionVars().TxnCtx.IsExplicit) {
		// Skip storage engine check for CreateView.
		if b.capFlag&canExpandAST == 0 {
			possiblePaths, err = util.FilterPathByIsolationRead(b.ctx, possiblePaths, tblName, dbName)
			if err != nil {
				return nil, err
			}
		}
	}

	// Try to substitute generate column only if there is an index on generate column.
	for _, index := range tableInfo.Indices {
		if index.State != model.StatePublic {
			continue
		}
		for _, indexCol := range index.Columns {
			colInfo := tbl.Cols()[indexCol.Offset]
			if colInfo.IsGenerated() && !colInfo.GeneratedStored {
				b.optFlag |= rule.FlagGcSubstitute
				break
			}
		}
	}

	var columns []*table.Column
	if b.inUpdateStmt {
		// create table t(a int, b int).
		// Imagine that, There are 2 TiDB instances in the cluster, name A, B. We add a column `c` to table t in the TiDB cluster.
		// One of the TiDB, A, the column type in its infoschema is changed to public. And in the other TiDB, the column type is
		// still StateWriteReorganization.
		// TiDB A: insert into t values(1, 2, 3);
		// TiDB B: update t set a = 2 where b = 2;
		// If we use tbl.Cols() here, the update statement, will ignore the col `c`, and the data `3` will lost.
		columns = tbl.WritableCols()
	} else if b.inDeleteStmt {
		// DeletableCols returns all columns of the table in deletable states.
		columns = tbl.DeletableCols()
	} else {
		columns = tbl.Cols()
	}
	// extract the IndexMergeHint
	var indexMergeHints []h.HintedIndex
	if hints := b.TableHints(); hints != nil {
		for i, hint := range hints.IndexMergeHintList {
			if hint.Match(dbName, tblName) {
				hints.IndexMergeHintList[i].Matched = true
				// check whether the index names in IndexMergeHint are valid.
				invalidIdxNames := make([]string, 0, len(hint.IndexHint.IndexNames))
				for _, idxName := range hint.IndexHint.IndexNames {
					hasIdxName := false
					for _, path := range possiblePaths {
						if path.IsTablePath() {
							if idxName.L == "primary" {
								hasIdxName = true
								break
							}
							continue
						}
						if idxName.L == path.Index.Name.L {
							hasIdxName = true
							break
						}
					}
					if !hasIdxName {
						invalidIdxNames = append(invalidIdxNames, idxName.String())
					}
				}
				if len(invalidIdxNames) == 0 {
					indexMergeHints = append(indexMergeHints, hint)
				} else {
					// Append warning if there are invalid index names.
					errMsg := fmt.Sprintf("use_index_merge(%s) is inapplicable, check whether the indexes (%s) "+
						"exist, or the indexes are conflicted with use_index/ignore_index/force_index hints.",
						hint.IndexString(), strings.Join(invalidIdxNames, ", "))
					b.ctx.GetSessionVars().StmtCtx.SetHintWarning(errMsg)
				}
			}
		}
	}
	allPaths := make([]*util.AccessPath, len(possiblePaths))
	copy(allPaths, possiblePaths)

	countCnt := len(columns) + 2 // +1 for an extra handle column and extra commit ts column
	ds := logicalop.DataSource{
		DBName:                 dbName,
		TableAsName:            asName,
		Table:                  tbl,
		TableInfo:              tableInfo,
		PhysicalTableID:        tableInfo.ID,
		AstIndexHints:          tn.IndexHints,
		IndexHints:             b.TableHints().IndexHintList,
		IndexMergeHints:        indexMergeHints,
		PossibleAccessPaths:    possiblePaths,
		AllPossibleAccessPaths: allPaths,
		Columns:                make([]*model.ColumnInfo, 0, countCnt),
		PartitionNames:         tn.PartitionNames,
		TblCols:                make([]*expression.Column, 0, countCnt),
		TblColsByID:            make(map[int64]*expression.Column, countCnt),
		PreferPartitions:       make(map[int][]ast.CIStr),
		IS:                     b.is,
		IsForUpdateRead:        b.isForUpdateRead,
	}.Init(b.ctx, b.getSelectOffset())
	var handleCols util.HandleCols
	schema := expression.NewSchema(make([]*expression.Column, 0, countCnt)...)
	names := make([]*types.FieldName, 0, countCnt)
	for i, col := range columns {
		ds.Columns = append(ds.Columns, col.ToInfo())
		names = append(names, &types.FieldName{
			DBName:      dbName,
			TblName:     tableInfo.Name,
			ColName:     col.Name,
			OrigTblName: tableInfo.Name,
			OrigColName: col.Name,
			// For update statement and delete statement, internal version should see the special middle state column, while user doesn't.
			NotExplicitUsable: col.State != model.StatePublic,
		})
		newCol := &expression.Column{
			UniqueID: sessionVars.AllocPlanColumnID(),
			ID:       col.ID,
			RetType:  col.FieldType.Clone(),
			OrigName: names[i].String(),
			IsHidden: col.Hidden,
		}
		if col.IsPKHandleColumn(tableInfo) {
			handleCols = util.NewIntHandleCols(newCol)
		}
		schema.Append(newCol)
		ds.AppendTableCol(newCol)
	}
	// We append an extra handle column to the schema when the handle
	// column is not the primary key of "ds".
	if handleCols == nil {
		if tableInfo.IsCommonHandle {
			primaryIdx := tables.FindPrimaryIndex(tableInfo)
			handleCols = util.NewCommonHandleCols(tableInfo, primaryIdx, ds.TblCols)
		} else if !tbl.Type().IsClusterTable() {
			// Cluster tables are memory tables that don't support ExtraHandleID.
			// ExtraHandleID would cause "Column ID -1 not found" errors when
			// coprocessor requests are sent to other TiDB nodes.
			extraCol := ds.NewExtraHandleSchemaCol()
			handleCols = util.NewIntHandleCols(extraCol)
			ds.Columns = append(ds.Columns, model.NewExtraHandleColInfo())
			schema.Append(extraCol)
			names = append(names, &types.FieldName{
				DBName:      dbName,
				TblName:     tableInfo.Name,
				ColName:     model.ExtraHandleName,
				OrigColName: model.ExtraHandleName,
			})
			ds.AppendTableCol(extraCol)
		}
	}
	// Append extra commit ts column to the schema.
	// Cluster tables are memory tables that don't support extra column IDs.
	if !tbl.Type().IsClusterTable() {
		commitTSCol := ds.NewExtraCommitTSSchemaCol()
		ds.Columns = append(ds.Columns, model.NewExtraCommitTSColInfo())
		schema.Append(commitTSCol)
		names = append(names, &types.FieldName{
			DBName:      dbName,
			TblName:     tableInfo.Name,
			ColName:     model.ExtraCommitTSName,
			OrigColName: model.ExtraCommitTSName,
		})
		ds.AppendTableCol(commitTSCol)
	}
	ds.HandleCols = handleCols
	ds.UnMutableHandleCols = handleCols
	handleMap := make(map[int64][]util.HandleCols)
	handleMap[tableInfo.ID] = []util.HandleCols{handleCols}
	b.handleHelper.pushMap(handleMap)
	ds.SetSchema(schema)
	ds.SetOutputNames(names)
	// setPreferredStoreType will mark user preferred path, which should be shared by all ds alternative. Here
	// we only mark it for the AllPossibleAccessPaths(since the element inside is shared by PossibleAccessPaths),
	// and the following ds alternative will clone/inherit this mark from DS copying.
	setPreferredStoreType(ds, b.TableHints())
	ds.SampleInfo = tablesampler.NewTableSampleInfo(tn.TableSample, schema, b.partitionedTable)
	b.isSampling = ds.SampleInfo != nil

	for i, colExpr := range ds.Schema().Columns {
		var expr expression.Expression
		if i < len(columns) {
			if columns[i].IsGenerated() && !columns[i].GeneratedStored {
				var err error
				originVal := b.allowBuildCastArray
				b.allowBuildCastArray = true
				expr, _, err = b.rewrite(ctx, columns[i].GeneratedExpr.Clone(), ds, nil, true)
				b.allowBuildCastArray = originVal
				if err != nil {
					return nil, err
				}
				colExpr.VirtualExpr = expr.Clone()
			}
		}
	}

	// Init CommonHandleCols and CommonHandleLens for data source.
	if tableInfo.IsCommonHandle {
		ds.CommonHandleCols, ds.CommonHandleLens = util.IndexInfo2FullCols(ds.Columns, ds.Schema().Columns, tables.FindPrimaryIndex(tableInfo))
	}
	// Init FullIdxCols, FullIdxColLens for accessPaths.
	for _, path := range ds.AllPossibleAccessPaths {
		if !path.IsIntHandlePath {
			path.FullIdxCols, path.FullIdxColLens = util.IndexInfo2FullCols(ds.Columns, ds.Schema().Columns, path.Index)

			// check whether the path's index has a tidb_shard() prefix and the index column count
			// more than 1. e.g. index(tidb_shard(a), a)
			// set UkShardIndexPath only for unique secondary index
			if !path.IsCommonHandlePath {
				// tidb_shard expression must be first column of index
				col := path.FullIdxCols[0]
				if col != nil &&
					expression.GcColumnExprIsTidbShard(col.VirtualExpr) &&
					len(path.Index.Columns) > 1 &&
					path.Index.Unique {
					path.IsUkShardIndexPath = true
					ds.ContainExprPrefixUk = true
				}
			}
		}
	}

	var result base.LogicalPlan = ds
	dirty := tableHasDirtyContent(b.ctx, tableInfo)
	if dirty || tableInfo.TempTableType == model.TempTableLocal || tableInfo.TableCacheStatusType == model.TableCacheStatusEnable {
		us := logicalop.LogicalUnionScan{HandleCols: handleCols}.Init(b.ctx, b.getSelectOffset())
		us.SetChildren(ds)
		if tableInfo.Partition != nil && b.optFlag&rule.FlagPartitionProcessor == 0 {
			// Adding ExtraPhysTblIDCol for UnionScan (transaction buffer handling)
			// Not using old static prune mode
			// Single TableReader for all partitions, needs the PhysTblID from storage
			_ = addExtraPhysTblIDColumn4DS(ds)
		}
		result = us
	}

	// Adding ExtraPhysTblIDCol for SelectLock (SELECT FOR UPDATE) is done when building SelectLock

	if sessionVars.StmtCtx.TblInfo2UnionScan == nil {
		sessionVars.StmtCtx.TblInfo2UnionScan = make(map[*model.TableInfo]bool)
	}
	sessionVars.StmtCtx.TblInfo2UnionScan[tableInfo] = dirty

	return result, nil
}

func (b *PlanBuilder) timeRangeForSummaryTable() util.QueryTimeRange {
	const defaultSummaryDuration = 30 * time.Minute
	hints := b.TableHints()
	// User doesn't use TIME_RANGE hint
	if hints == nil || (hints.TimeRangeHint.From == "" && hints.TimeRangeHint.To == "") {
		to := time.Now()
		from := to.Add(-defaultSummaryDuration)
		return util.QueryTimeRange{From: from, To: to}
	}

	// Parse time specified by user via TIM_RANGE hint
	parse := func(s string) (time.Time, bool) {
		t, err := time.ParseInLocation(util.MetricTableTimeFormat, s, time.Local)
		if err != nil {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
		return t, err == nil
	}
	from, fromValid := parse(hints.TimeRangeHint.From)
	to, toValid := parse(hints.TimeRangeHint.To)
	switch {
	case !fromValid && !toValid:
		to = time.Now()
		from = to.Add(-defaultSummaryDuration)
	case fromValid && !toValid:
		to = from.Add(defaultSummaryDuration)
	case !fromValid && toValid:
		from = to.Add(-defaultSummaryDuration)
	}

	return util.QueryTimeRange{From: from, To: to}
}

func (b *PlanBuilder) buildMemTable(_ context.Context, dbName ast.CIStr, tableInfo *model.TableInfo) (base.LogicalPlan, error) {
	// We can use the `TableInfo.Columns` directly because the memory table has
	// a stable schema and there is no online DDL on the memory table.
	schema := expression.NewSchema(make([]*expression.Column, 0, len(tableInfo.Columns))...)
	names := make([]*types.FieldName, 0, len(tableInfo.Columns))
	var handleCols util.HandleCols
	for _, col := range tableInfo.Columns {
		names = append(names, &types.FieldName{
			DBName:      dbName,
			TblName:     tableInfo.Name,
			ColName:     col.Name,
			OrigTblName: tableInfo.Name,
			OrigColName: col.Name,
		})
		// NOTE: Rewrite the expression if memory table supports generated columns in the future
		newCol := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			ID:       col.ID,
			RetType:  &col.FieldType,
		}
		if tableInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag()) {
			handleCols = util.NewIntHandleCols(newCol)
		}
		schema.Append(newCol)
	}

	if handleCols != nil {
		handleMap := make(map[int64][]util.HandleCols)
		handleMap[tableInfo.ID] = []util.HandleCols{handleCols}
		b.handleHelper.pushMap(handleMap)
	} else {
		b.handleHelper.pushMap(nil)
	}

	// NOTE: Add a `LogicalUnionScan` if we support update memory table in the future
	p := logicalop.LogicalMemTable{
		DBName:    dbName,
		TableInfo: tableInfo,
		Columns:   make([]*model.ColumnInfo, len(tableInfo.Columns)),
	}.Init(b.ctx, b.getSelectOffset())
	p.SetSchema(schema)
	p.SetOutputNames(names)
	copy(p.Columns, tableInfo.Columns)

	// Some memory tables can receive some predicates
	switch dbName.L {
	case metadef.MetricSchemaName.L:
		p.Extractor = newMetricTableExtractor()
	case metadef.InformationSchemaName.L:
		switch upTbl := strings.ToUpper(tableInfo.Name.O); upTbl {
		case infoschema.TableClusterConfig, infoschema.TableClusterLoad, infoschema.TableClusterHardware, infoschema.TableClusterSystemInfo:
			p.Extractor = &ClusterTableExtractor{}
		case infoschema.TableClusterLog:
			p.Extractor = &ClusterLogTableExtractor{}
		case infoschema.TableTiDBHotRegionsHistory:
			p.Extractor = &HotRegionsHistoryTableExtractor{}
		case infoschema.TableInspectionResult:
			p.Extractor = &InspectionResultTableExtractor{}
			p.QueryTimeRange = b.timeRangeForSummaryTable()
		case infoschema.TableInspectionSummary:
			p.Extractor = &InspectionSummaryTableExtractor{}
			p.QueryTimeRange = b.timeRangeForSummaryTable()
		case infoschema.TableInspectionRules:
			p.Extractor = &InspectionRuleTableExtractor{}
		case infoschema.TableMetricSummary, infoschema.TableMetricSummaryByLabel:
			p.Extractor = &MetricSummaryTableExtractor{}
			p.QueryTimeRange = b.timeRangeForSummaryTable()
		case infoschema.TableSlowQuery:
			p.Extractor = &SlowQueryExtractor{}
		case infoschema.TableStorageStats:
			p.Extractor = &TableStorageStatsExtractor{}
		case infoschema.TableTiFlashTables, infoschema.TableTiFlashSegments, infoschema.TableTiFlashIndexes:
			p.Extractor = &TiFlashSystemTableExtractor{}
		case infoschema.TableStatementsSummary, infoschema.TableStatementsSummaryHistory, infoschema.TableTiDBStatementsStats:
			p.Extractor = &StatementsSummaryExtractor{}
		case infoschema.TableTiKVRegionPeers:
			p.Extractor = &TikvRegionPeersExtractor{}
		case infoschema.TableColumns:
			p.Extractor = NewInfoSchemaColumnsExtractor()
		case infoschema.TableTables:
			p.Extractor = NewInfoSchemaTablesExtractor()
		case infoschema.TablePartitions:
			p.Extractor = NewInfoSchemaPartitionsExtractor()
		case infoschema.TableStatistics:
			p.Extractor = NewInfoSchemaStatisticsExtractor()
		case infoschema.TableSchemata:
			p.Extractor = NewInfoSchemaSchemataExtractor()
		case infoschema.TableSequences:
			p.Extractor = NewInfoSchemaSequenceExtractor()
		case infoschema.TableTiDBIndexUsage:
			p.Extractor = NewInfoSchemaTiDBIndexUsageExtractor()
		case infoschema.TableDDLJobs:
			p.Extractor = NewInfoSchemaDDLExtractor()
		case infoschema.TableCheckConstraints:
			p.Extractor = NewInfoSchemaCheckConstraintsExtractor()
		case infoschema.TableTiDBCheckConstraints:
			p.Extractor = NewInfoSchemaTiDBCheckConstraintsExtractor()
		case infoschema.TableReferConst:
			p.Extractor = NewInfoSchemaReferConstExtractor()
		case infoschema.TableTiDBIndexes:
			p.Extractor = NewInfoSchemaIndexesExtractor()
		case infoschema.TableViews:
			p.Extractor = NewInfoSchemaViewsExtractor()
		case infoschema.TableKeyColumn:
			p.Extractor = NewInfoSchemaKeyColumnUsageExtractor()
		case infoschema.TableConstraints:
			p.Extractor = NewInfoSchemaTableConstraintsExtractor()
		case infoschema.TableTiKVRegionStatus:
			p.Extractor = &TiKVRegionStatusExtractor{tablesID: make([]int64, 0)}
		}
	}
	return p, nil
}

// checkRecursiveView checks whether this view is recursively defined.
func (b *PlanBuilder) checkRecursiveView(dbName ast.CIStr, tableName ast.CIStr) (func(), error) {
	viewFullName := dbName.L + "." + tableName.L
	if b.buildingViewStack == nil {
		b.buildingViewStack = set.NewStringSet()
	}
	// If this view has already been on the building stack, it means
	// this view contains a recursive definition.
	if b.buildingViewStack.Exist(viewFullName) {
		return nil, plannererrors.ErrViewRecursive.GenWithStackByArgs(dbName.O, tableName.O)
	}
	// If the view is being renamed, we return the mysql compatible error message.
	if b.capFlag&renameView != 0 && viewFullName == b.renamingViewName {
		return nil, plannererrors.ErrNoSuchTable.GenWithStackByArgs(dbName.O, tableName.O)
	}
	b.buildingViewStack.Insert(viewFullName)
	return func() { delete(b.buildingViewStack, viewFullName) }, nil
}

// BuildDataSourceFromView is used to build base.LogicalPlan from view
// qbNameMap4View and viewHints are used for the view's hint.
// qbNameMap4View maps the query block name to the view table lists.
// viewHints group the view hints based on the view's query block name.
func (b *PlanBuilder) BuildDataSourceFromView(ctx context.Context, dbName ast.CIStr, tableInfo *model.TableInfo, qbNameMap4View map[string][]ast.HintTable, viewHints map[string][]*ast.TableOptimizerHint) (base.LogicalPlan, error) {
	viewDepth := b.ctx.GetSessionVars().StmtCtx.ViewDepth
	b.ctx.GetSessionVars().StmtCtx.ViewDepth++
	deferFunc, err := b.checkRecursiveView(dbName, tableInfo.Name)
	if err != nil {
		return nil, err
	}
	defer deferFunc()

	charset, collation := b.ctx.GetSessionVars().GetCharsetInfo()
	viewParser := parser.New()
	viewParser.SetParserConfig(b.ctx.GetSessionVars().BuildParserConfig())
	selectNode, err := viewParser.ParseOneStmt(tableInfo.View.SelectStmt, charset, collation)
	if err != nil {
		return nil, err
	}
	originalVisitInfo := b.visitInfo
	b.visitInfo = make([]visitInfo, 0)

	// For the case that views appear in CTE queries,
	// we need to save the CTEs after the views are established.
	var saveCte []*cteInfo
	if len(b.outerCTEs) > 0 {
		saveCte = make([]*cteInfo, len(b.outerCTEs))
		copy(saveCte, b.outerCTEs)
	} else {
		saveCte = nil
	}
	o := b.buildingCTE
	b.buildingCTE = false
	defer func() {
		b.outerCTEs = saveCte
		b.buildingCTE = o
	}()

	hintProcessor := h.NewQBHintHandler(b.ctx.GetSessionVars().StmtCtx)
	selectNode.Accept(hintProcessor)
	currentQbNameMap4View := make(map[string][]ast.HintTable)
	currentQbHints4View := make(map[string][]*ast.TableOptimizerHint)
	currentQbHints := make(map[int][]*ast.TableOptimizerHint)
	currentQbNameMap := make(map[string]int)

	for qbName, viewQbNameHint := range qbNameMap4View {
		// Check whether the view hint belong the current view or its nested views.
		qbOffset := -1
		if len(viewQbNameHint) == 0 {
			qbOffset = 1
		} else if len(viewQbNameHint) == 1 && viewQbNameHint[0].TableName.L == "" {
			qbOffset = hintProcessor.GetHintOffset(viewQbNameHint[0].QBName, -1)
		} else {
			currentQbNameMap4View[qbName] = viewQbNameHint
			currentQbHints4View[qbName] = viewHints[qbName]
		}

		if qbOffset != -1 {
			// If the hint belongs to the current view and not belongs to it's nested views, we should convert the view hint to the normal hint.
			// After we convert the view hint to the normal hint, it can be reused the origin hint's infrastructure.
			currentQbHints[qbOffset] = viewHints[qbName]
			currentQbNameMap[qbName] = qbOffset

			delete(qbNameMap4View, qbName)
			delete(viewHints, qbName)
		}
	}

	hintProcessor.ViewQBNameToTable = qbNameMap4View
	hintProcessor.ViewQBNameToHints = viewHints
	hintProcessor.ViewQBNameUsed = make(map[string]struct{})
	hintProcessor.QBOffsetToHints = currentQbHints
	hintProcessor.QBNameToSelOffset = currentQbNameMap

	originHintProcessor := b.hintProcessor
	originPlannerSelectBlockAsName := b.ctx.GetSessionVars().PlannerSelectBlockAsName.Load()
	b.hintProcessor = hintProcessor
	newPlannerSelectBlockAsName := make([]ast.HintTable, hintProcessor.MaxSelectStmtOffset()+1)
	b.ctx.GetSessionVars().PlannerSelectBlockAsName.Store(&newPlannerSelectBlockAsName)
	defer func() {
		b.hintProcessor.HandleUnusedViewHints()
		b.hintProcessor = originHintProcessor
		b.ctx.GetSessionVars().PlannerSelectBlockAsName.Store(originPlannerSelectBlockAsName)
	}()
	nodeW := resolve.NewNodeWWithCtx(selectNode, b.resolveCtx)
	selectLogicalPlan, err := b.Build(ctx, nodeW)
	if err != nil {
		logutil.BgLogger().Warn("build plan for view failed", zap.Error(err))
		if terror.ErrorNotEqual(err, plannererrors.ErrViewRecursive) &&
			terror.ErrorNotEqual(err, plannererrors.ErrNoSuchTable) &&
			terror.ErrorNotEqual(err, plannererrors.ErrInternal) &&
			terror.ErrorNotEqual(err, plannererrors.ErrFieldNotInGroupBy) &&
			terror.ErrorNotEqual(err, plannererrors.ErrMixOfGroupFuncAndFields) &&
			terror.ErrorNotEqual(err, plannererrors.ErrViewNoExplain) &&
			terror.ErrorNotEqual(err, plannererrors.ErrNotSupportedYet) {
			err = plannererrors.ErrViewInvalid.GenWithStackByArgs(dbName.O, tableInfo.Name.O)
		}
		failpoint.Inject("BuildDataSourceFailed", func() {})
		return nil, err
	}
	pm := privilege.GetPrivilegeManager(b.ctx)
	if viewDepth != 0 &&
		b.ctx.GetSessionVars().StmtCtx.InExplainStmt &&
		pm != nil &&
		!pm.RequestVerification(b.ctx.GetSessionVars().ActiveRoles, dbName.L, tableInfo.Name.L, "", mysql.SelectPriv) {
		return nil, plannererrors.ErrViewNoExplain
	}
	if tableInfo.View.Security == ast.SecurityDefiner {
		if pm != nil {
			for _, v := range b.visitInfo {
				if !pm.RequestVerificationWithUser(ctx, v.db, v.table, v.column, v.privilege, tableInfo.View.Definer) {
					return nil, plannererrors.ErrViewInvalid.GenWithStackByArgs(dbName.O, tableInfo.Name.O)
				}
			}
		}
		b.visitInfo = b.visitInfo[:0]
	}
	b.visitInfo = append(originalVisitInfo, b.visitInfo...)

	if b.ctx.GetSessionVars().StmtCtx.InExplainStmt {
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ShowViewPriv, dbName.L, tableInfo.Name.L, "", plannererrors.ErrViewNoExplain)
	}

	if len(tableInfo.Columns) != selectLogicalPlan.Schema().Len() {
		return nil, plannererrors.ErrViewInvalid.GenWithStackByArgs(dbName.O, tableInfo.Name.O)
	}

	return b.buildProjUponView(ctx, dbName, tableInfo, selectLogicalPlan)
}

func (b *PlanBuilder) buildProjUponView(_ context.Context, dbName ast.CIStr, tableInfo *model.TableInfo, selectLogicalPlan base.Plan) (base.LogicalPlan, error) {
	columnInfo := tableInfo.Cols()
	cols := selectLogicalPlan.Schema().Clone().Columns
	outputNamesOfUnderlyingSelect := selectLogicalPlan.OutputNames().Shallow()
	// In the old version of VIEW implementation, TableInfo.View.Cols is used to
	// store the origin columns' names of the underlying SelectStmt used when
	// creating the view.
	if tableInfo.View.Cols != nil {
		cols = cols[:0]
		outputNamesOfUnderlyingSelect = outputNamesOfUnderlyingSelect[:0]
		for _, info := range columnInfo {
			idx := expression.FindFieldNameIdxByColName(selectLogicalPlan.OutputNames(), info.Name.L)
			if idx == -1 {
				return nil, plannererrors.ErrViewInvalid.GenWithStackByArgs(dbName.O, tableInfo.Name.O)
			}
			cols = append(cols, selectLogicalPlan.Schema().Columns[idx])
			outputNamesOfUnderlyingSelect = append(outputNamesOfUnderlyingSelect, selectLogicalPlan.OutputNames()[idx])
		}
	}

	projSchema := expression.NewSchema(make([]*expression.Column, 0, len(tableInfo.Columns))...)
	projExprs := make([]expression.Expression, 0, len(tableInfo.Columns))
	projNames := make(types.NameSlice, 0, len(tableInfo.Columns))
	for i, name := range outputNamesOfUnderlyingSelect {
		origColName := name.ColName
		if tableInfo.View.Cols != nil {
			origColName = tableInfo.View.Cols[i]
		}
		projNames = append(projNames, &types.FieldName{
			// TblName is the of view instead of the name of the underlying table.
			TblName:     tableInfo.Name,
			OrigTblName: name.OrigTblName,
			ColName:     columnInfo[i].Name,
			OrigColName: origColName,
			DBName:      dbName,
		})
		projSchema.Append(&expression.Column{
			UniqueID: cols[i].UniqueID,
			RetType:  cols[i].GetStaticType(),
		})
		projExprs = append(projExprs, cols[i])
	}
	projUponView := logicalop.LogicalProjection{Exprs: projExprs}.Init(b.ctx, b.getSelectOffset())
	projUponView.SetOutputNames(projNames)
	projUponView.SetChildren(selectLogicalPlan.(base.LogicalPlan))
	projUponView.SetSchema(projSchema)
	return projUponView, nil
}
