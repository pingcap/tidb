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
	"strings"
	"time"

	"github.com/pingcap/failpoint"
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
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/set"
	"go.uber.org/zap"
)

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
