// Copyright 2015 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/domainmisc"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
)

func (b *PlanBuilder) buildSelectLock(src base.LogicalPlan, lock *ast.SelectLockInfo) (*logicalop.LogicalLock, error) {
	var tblID2PhysTblIDCol map[int64]*expression.Column
	if len(b.partitionedTable) > 0 {
		tblID2PhysTblIDCol = make(map[int64]*expression.Column)
		// If a chunk row is read from a partitioned table, which partition the row
		// comes from is unknown. With the existence of Join, the situation could be
		// even worse: SelectLock have to know the `pid` to construct the lock key.
		// To solve the problem, an extra `pid` column is added to the schema, and the
		// DataSource need to return the `pid` information in the chunk row.
		// For dynamic prune mode, it is filled in from the tableID in the key by storage.
		// For static prune mode it is also filled in from the tableID in the key by storage.
		// since it would otherwise be lost in the PartitionUnion executor.
		setExtraPhysTblIDColsOnDataSource(src, tblID2PhysTblIDCol)
	}
	selectLock := logicalop.LogicalLock{
		Lock:               lock,
		TblID2Handle:       b.handleHelper.tailMap(),
		TblID2PhysTblIDCol: tblID2PhysTblIDCol,
	}.Init(b.ctx)
	selectLock.SetChildren(src)
	return selectLock, nil
}

func setExtraPhysTblIDColsOnDataSource(p base.LogicalPlan, tblID2PhysTblIDCol map[int64]*expression.Column) {
	switch ds := p.(type) {
	case *logicalop.DataSource:
		if ds.TableInfo.GetPartitionInfo() == nil {
			return
		}
		tblID2PhysTblIDCol[ds.TableInfo.ID] = addExtraPhysTblIDColumn4DS(ds)
	default:
		for _, child := range p.Children() {
			setExtraPhysTblIDColsOnDataSource(child, tblID2PhysTblIDCol)
		}
	}
}

func (b *PlanBuilder) buildPrepare(x *ast.PrepareStmt) base.Plan {
	p := &Prepare{
		Name: x.Name,
	}
	if x.SQLVar != nil {
		if v, ok := b.ctx.GetSessionVars().GetUserVarVal(strings.ToLower(x.SQLVar.Name)); ok {
			var err error
			p.SQLText, err = v.ToString()
			if err != nil {
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
				p.SQLText = "NULL"
			}
		} else {
			p.SQLText = "NULL"
		}
	} else {
		p.SQLText = x.SQLText
	}
	return p
}
func (b *PlanBuilder) buildAdmin(ctx context.Context, as *ast.AdminStmt) (base.Plan, error) {
	var ret base.Plan
	var err error
	switch as.Tp {
	case ast.AdminCheckTable, ast.AdminCheckIndex:
		ret, err = b.buildAdminCheckTable(ctx, as)
		if err != nil {
			return ret, err
		}
	case ast.AdminRecoverIndex:
		tnW := b.resolveCtx.GetTableName(as.Tables[0])
		p := &RecoverIndex{Table: tnW, IndexName: as.Index}
		p.SetSchemaAndNames(buildRecoverIndexFields())
		ret = p
	case ast.AdminCleanupIndex:
		tnW := b.resolveCtx.GetTableName(as.Tables[0])
		p := &CleanupIndex{Table: tnW, IndexName: as.Index}
		p.SetSchemaAndNames(buildCleanupIndexFields())
		ret = p
	case ast.AdminChecksumTable:
		tnWs := make([]*resolve.TableNameW, 0, len(as.Tables))
		for _, tn := range as.Tables {
			tnWs = append(tnWs, b.resolveCtx.GetTableName(tn))
		}
		p := &ChecksumTable{Tables: tnWs}
		p.SetSchemaAndNames(buildChecksumTableSchema())
		ret = p
	case ast.AdminShowNextRowID:
		p := &ShowNextRowID{TableName: as.Tables[0]}
		p.SetSchemaAndNames(buildShowNextRowID())
		ret = p
	case ast.AdminShowDDL:
		p := &ShowDDL{}
		p.SetSchemaAndNames(buildShowDDLFields())
		ret = p
	case ast.AdminShowDDLJobs:
		p := logicalop.LogicalShowDDLJobs{JobNumber: as.JobNumber}.Init(b.ctx)
		p.SetSchemaAndNames(buildShowDDLJobsFields())
		for _, col := range p.Schema().Columns {
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		ret = p
		if as.Where != nil {
			ret, err = b.buildSelection(ctx, p, as.Where, nil)
			if err != nil {
				return nil, err
			}
		}
	case ast.AdminCancelDDLJobs:
		p := &CancelDDLJobs{JobIDs: as.JobIDs}
		p.SetSchemaAndNames(buildCancelDDLJobsFields())
		ret = p
	case ast.AdminPauseDDLJobs:
		p := &PauseDDLJobs{JobIDs: as.JobIDs}
		p.SetSchemaAndNames(buildPauseDDLJobsFields())
		ret = p
	case ast.AdminResumeDDLJobs:
		p := &ResumeDDLJobs{JobIDs: as.JobIDs}
		p.SetSchemaAndNames(buildResumeDDLJobsFields())
		ret = p
	case ast.AdminCheckIndexRange:
		schema, names, err := b.buildCheckIndexSchema(as.Tables[0], as.Index)
		if err != nil {
			return nil, err
		}

		p := &CheckIndexRange{Table: as.Tables[0], IndexName: as.Index, HandleRanges: as.HandleRanges}
		p.SetSchemaAndNames(schema, names)
		ret = p
	case ast.AdminShowDDLJobQueries:
		p := &ShowDDLJobQueries{JobIDs: as.JobIDs}
		p.SetSchemaAndNames(buildShowDDLJobQueriesFields())
		ret = p
	case ast.AdminShowDDLJobQueriesWithRange:
		p := &ShowDDLJobQueriesWithRange{Limit: as.LimitSimple.Count, Offset: as.LimitSimple.Offset}
		p.SetSchemaAndNames(buildShowDDLJobQueriesWithRangeFields())
		ret = p
	case ast.AdminShowSlow:
		p := &ShowSlow{ShowSlow: as.ShowSlow}
		p.SetSchemaAndNames(buildShowSlowSchema())
		ret = p
	case ast.AdminReloadExprPushdownBlacklist:
		return &ReloadExprPushdownBlacklist{}, nil
	case ast.AdminReloadOptRuleBlacklist:
		return &ReloadOptRuleBlacklist{}, nil
	case ast.AdminPluginEnable:
		return &AdminPlugins{Action: Enable, Plugins: as.Plugins}, nil
	case ast.AdminPluginDisable:
		return &AdminPlugins{Action: Disable, Plugins: as.Plugins}, nil
	case ast.AdminFlushBindings:
		return &SQLBindPlan{SQLBindOp: OpFlushBindings}, nil
	case ast.AdminCaptureBindings:
		return nil, errors.Errorf("Auto Capture is not supported")
	case ast.AdminEvolveBindings:
		return nil, errors.Errorf("Cannot enable baseline evolution feature, it is not generally available now")
	case ast.AdminReloadBindings:
		return &SQLBindPlan{SQLBindOp: OpReloadBindings}, nil
	case ast.AdminReloadClusterBindings:
		return &SQLBindPlan{SQLBindOp: OpReloadClusterBindings}, nil
	case ast.AdminReloadStatistics:
		return &Simple{Statement: as, ResolveCtx: b.resolveCtx}, nil
	case ast.AdminFlushPlanCache:
		return &Simple{Statement: as, ResolveCtx: b.resolveCtx}, nil
	case ast.AdminSetBDRRole, ast.AdminUnsetBDRRole:
		ret = &Simple{Statement: as, ResolveCtx: b.resolveCtx}
	case ast.AdminShowBDRRole:
		p := &AdminShowBDRRole{}
		p.SetSchemaAndNames(buildAdminShowBDRRoleFields())
		ret = p
	case ast.AdminAlterDDLJob:
		ret, err = b.buildAdminAlterDDLJob(ctx, as)
		if err != nil {
			return nil, err
		}
	case ast.AdminWorkloadRepoCreate:
		ret = &WorkloadRepoCreate{}
	default:
		return nil, plannererrors.ErrUnsupportedType.GenWithStack("Unsupported ast.AdminStmt(%T) for buildAdmin", as)
	}

	// Admin command can only be executed by administrator.
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return ret, nil
}

func (b *PlanBuilder) buildPhysicalIndexLookUpReader(_ context.Context, dbName ast.CIStr, tbl table.Table, idx *model.IndexInfo) (base.Plan, error) {
	tblInfo := tbl.Meta()
	physicalID, isPartition := getPhysicalID(tbl, idx.Global)
	fullExprCols, _, err := expression.TableInfo2SchemaAndNames(b.ctx.GetExprCtx(), dbName, tblInfo)
	if err != nil {
		return nil, err
	}
	extraInfo, extraCol, hasExtraCol := tryGetPkExtraColumn(b.ctx.GetSessionVars(), tblInfo)
	pkHandleInfo, pkHandleCol, hasPkIsHandle := tryGetPkHandleCol(tblInfo, fullExprCols)
	commonInfos, commonCols, hasCommonCols := tryGetCommonHandleCols(tbl, fullExprCols)
	idxColInfos := getIndexColumnInfos(tblInfo, idx)
	idxColSchema := getIndexColsSchema(tblInfo, idx, fullExprCols)
	idxCols, idxColLens := util.IndexInfo2PrefixCols(idxColInfos, idxColSchema.Columns, idx)
	pseudoHistColl := statistics.PseudoHistColl(physicalID, false)
	is := physicalop.PhysicalIndexScan{
		Table:            tblInfo,
		TableAsName:      &tblInfo.Name,
		DBName:           dbName,
		Columns:          idxColInfos,
		Index:            idx,
		IdxCols:          idxCols,
		IdxColLens:       idxColLens,
		DataSourceSchema: idxColSchema.Clone(),
		Ranges:           ranger.FullRange(),
		PhysicalTableID:  physicalID,
		IsPartition:      isPartition,
		TblColHists:      &pseudoHistColl,
	}.Init(b.ctx, b.getSelectOffset())
	// There is no alternative plan choices, so just use pseudo stats to avoid panic.
	is.SetStats(&property.StatsInfo{HistColl: &pseudoHistColl})
	if hasCommonCols {
		for _, c := range commonInfos {
			is.Columns = append(is.Columns, c.ColumnInfo)
		}
	}
	is.InitSchema(append(is.IdxCols, commonCols...), true)

	// It's double read case.
	ts := physicalop.PhysicalTableScan{
		Columns:         idxColInfos,
		Table:           tblInfo,
		TableAsName:     &tblInfo.Name,
		DBName:          dbName,
		PhysicalTableID: physicalID,
		TblColHists:     &pseudoHistColl,
	}.Init(b.ctx, b.getSelectOffset())
	ts.SetIsPartition(isPartition)
	ts.SetSchema(idxColSchema)
	ts.Columns = physicalop.ExpandVirtualColumn(ts.Columns, ts.Schema(), ts.Table.Columns)
	switch {
	case hasExtraCol:
		ts.Columns = append(ts.Columns, extraInfo)
		ts.Schema().Append(extraCol)
		ts.HandleIdx = []int{len(ts.Columns) - 1}
	case hasPkIsHandle:
		ts.Columns = append(ts.Columns, pkHandleInfo)
		ts.Schema().Append(pkHandleCol)
		ts.HandleIdx = []int{len(ts.Columns) - 1}
	case hasCommonCols:
		ts.HandleIdx = make([]int, 0, len(commonCols))
		for pkOffset, cInfo := range commonInfos {
			found := false
			for i, c := range ts.Columns {
				if c.ID == cInfo.ID {
					found = true
					ts.HandleIdx = append(ts.HandleIdx, i)
					break
				}
			}
			if !found {
				ts.Columns = append(ts.Columns, cInfo.ColumnInfo)
				ts.Schema().Append(commonCols[pkOffset])
				ts.HandleIdx = append(ts.HandleIdx, len(ts.Columns)-1)
			}
		}
	}
	if is.Index.Global {
		tmpColumns, tmpSchema, _ := physicalop.AddExtraPhysTblIDColumn(b.ctx, ts.Columns, ts.Schema())
		ts.Columns = tmpColumns
		ts.SetSchema(tmpSchema)
	}

	cop := &physicalop.CopTask{
		IndexPlan:        is,
		TablePlan:        ts,
		TblColHists:      is.StatsInfo().HistColl,
		ExtraHandleCol:   extraCol,
		CommonHandleCols: commonCols,
	}
	rootT := cop.ConvertToRootTask(b.ctx).(*physicalop.RootTask)
	if err := rootT.GetPlan().ResolveIndices(); err != nil {
		return nil, err
	}
	return rootT.GetPlan(), nil
}

func getIndexColumnInfos(tblInfo *model.TableInfo, idx *model.IndexInfo) []*model.ColumnInfo {
	ret := make([]*model.ColumnInfo, len(idx.Columns))
	for i, idxCol := range idx.Columns {
		ret[i] = tblInfo.Columns[idxCol.Offset]
	}
	return ret
}

func getIndexColsSchema(tblInfo *model.TableInfo, idx *model.IndexInfo, allColSchema *expression.Schema) *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, len(idx.Columns))...)
	for _, idxCol := range idx.Columns {
		for i, colInfo := range tblInfo.Columns {
			if colInfo.Name.L == idxCol.Name.L {
				schema.Append(allColSchema.Columns[i])
				break
			}
		}
	}
	return schema
}

func getPhysicalID(t table.Table, isGlobalIndex bool) (physicalID int64, isPartition bool) {
	tblInfo := t.Meta()
	if !isGlobalIndex && tblInfo.GetPartitionInfo() != nil {
		pid := t.(table.PhysicalTable).GetPhysicalID()
		return pid, true
	}
	return tblInfo.ID, false
}

func tryGetPkExtraColumn(sv *variable.SessionVars, tblInfo *model.TableInfo) (*model.ColumnInfo, *expression.Column, bool) {
	if tblInfo.IsCommonHandle || tblInfo.PKIsHandle {
		return nil, nil, false
	}
	info := model.NewExtraHandleColInfo()
	expCol := &expression.Column{
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		UniqueID: sv.AllocPlanColumnID(),
		ID:       model.ExtraHandleID,
	}
	return info, expCol, true
}

func tryGetCommonHandleCols(t table.Table, allColSchema *expression.Schema) ([]*table.Column, []*expression.Column, bool) {
	tblInfo := t.Meta()
	if !tblInfo.IsCommonHandle {
		return nil, nil, false
	}
	pk := tables.FindPrimaryIndex(tblInfo)
	commonHandleCols, _ := util.IndexInfo2FullCols(tblInfo.Columns, allColSchema.Columns, pk)
	commonHandelColInfos := tables.TryGetCommonPkColumns(t)
	return commonHandelColInfos, commonHandleCols, true
}

func tryGetPkHandleCol(tblInfo *model.TableInfo, allColSchema *expression.Schema) (*model.ColumnInfo, *expression.Column, bool) {
	if !tblInfo.PKIsHandle {
		return nil, nil, false
	}
	for i, c := range tblInfo.Columns {
		if mysql.HasPriKeyFlag(c.GetFlag()) {
			return c, allColSchema.Columns[i], true
		}
	}
	return nil, nil, false
}

func (b *PlanBuilder) buildPhysicalIndexLookUpReaders(ctx context.Context, dbName ast.CIStr, tbl table.Table, indices []table.Index) ([]base.Plan, []*model.IndexInfo, error) {
	tblInfo := tbl.Meta()
	// get index information
	indexInfos := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	indexLookUpReaders := make([]base.Plan, 0, len(tblInfo.Indices))

	check := b.isForUpdateRead || b.ctx.GetSessionVars().IsIsolation(ast.ReadCommitted)
	check = check && b.ctx.GetSessionVars().ConnectionID > 0
	var latestIndexes map[int64]*model.IndexInfo
	var err error

	for _, idx := range indices {
		idxInfo := idx.Meta()
		if tblInfo.IsCommonHandle && idxInfo.Primary {
			// Skip checking clustered index.
			continue
		}
		if idxInfo.State != model.StatePublic {
			logutil.Logger(ctx).Info("build physical index lookup reader, the index isn't public",
				zap.String("index", idxInfo.Name.O),
				zap.Stringer("state", idxInfo.State),
				zap.String("table", tblInfo.Name.O))
			continue
		}
		if check && latestIndexes == nil {
			latestIndexes, check, err = domainmisc.GetLatestIndexInfo(b.ctx, tblInfo.ID, b.is.SchemaMetaVersion())
			if err != nil {
				return nil, nil, err
			}
		}
		if check {
			if latestIndex, ok := latestIndexes[idxInfo.ID]; !ok || latestIndex.State != model.StatePublic {
				forUpdateState := model.StateNone
				if ok {
					forUpdateState = latestIndex.State
				}
				logutil.Logger(ctx).Info("build physical index lookup reader, the index isn't public in forUpdateRead",
					zap.String("index", idxInfo.Name.O),
					zap.Stringer("state", idxInfo.State),
					zap.Stringer("forUpdateRead state", forUpdateState),
					zap.String("table", tblInfo.Name.O))
				continue
			}
		}
		indexInfos = append(indexInfos, idxInfo)
		// For partition tables except global index.
		if pi := tbl.Meta().GetPartitionInfo(); pi != nil && !idxInfo.Global {
			for _, def := range pi.Definitions {
				t := tbl.(table.PartitionedTable).GetPartition(def.ID)
				reader, err := b.buildPhysicalIndexLookUpReader(ctx, dbName, t, idxInfo)
				if err != nil {
					return nil, nil, err
				}
				indexLookUpReaders = append(indexLookUpReaders, reader)
			}
			continue
		}
		// For non-partition tables.
		reader, err := b.buildPhysicalIndexLookUpReader(ctx, dbName, tbl, idxInfo)
		if err != nil {
			return nil, nil, err
		}
		indexLookUpReaders = append(indexLookUpReaders, reader)
	}
	if len(indexLookUpReaders) == 0 {
		return nil, nil, nil
	}
	return indexLookUpReaders, indexInfos, nil
}

func (b *PlanBuilder) buildAdminCheckTable(ctx context.Context, as *ast.AdminStmt) (*CheckTable, error) {
	if len(as.Tables) > 1 {
		return nil, errors.New("admin check only supports one table at a time")
	}
	tblName := as.Tables[0]
	tnW := b.resolveCtx.GetTableName(tblName)
	tableInfo := tnW.TableInfo
	tbl, ok := b.is.TableByID(ctx, tableInfo.ID)
	if !ok {
		return nil, infoschema.ErrTableNotExists.FastGenByArgs(tnW.DBInfo.Name.O, tableInfo.Name.O)
	}
	p := &CheckTable{
		DBName: tblName.Schema.O,
		Table:  tbl,
	}
	var readerPlans []base.Plan
	var indexInfos []*model.IndexInfo
	var err error
	if as.Tp == ast.AdminCheckIndex {
		// get index information
		var idx table.Index
		idxName := strings.ToLower(as.Index)
		for _, index := range tbl.Indices() {
			if index.Meta().Name.L == idxName {
				idx = index
				break
			}
		}
		if idx == nil {
			return nil, errors.Errorf("secondary index %s does not exist", as.Index)
		}
		if idx.Meta().State != model.StatePublic {
			return nil, errors.Errorf("index %s state %s isn't public", as.Index, idx.Meta().State)
		}
		p.CheckIndex = true
		readerPlans, indexInfos, err = b.buildPhysicalIndexLookUpReaders(ctx, tblName.Schema, tbl, []table.Index{idx})
	} else {
		readerPlans, indexInfos, err = b.buildPhysicalIndexLookUpReaders(ctx, tblName.Schema, tbl, tbl.Indices())
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	readers := make([]*physicalop.PhysicalIndexLookUpReader, 0, len(readerPlans))
	for _, plan := range readerPlans {
		readers = append(readers, plan.(*physicalop.PhysicalIndexLookUpReader))
	}
	p.IndexInfos = indexInfos
	p.IndexLookUpReaders = readers
	return p, nil
}

func (b *PlanBuilder) buildCheckIndexSchema(tn *ast.TableName, indexName string) (*expression.Schema, types.NameSlice, error) {
	schema := expression.NewSchema()
	var names types.NameSlice
	indexName = strings.ToLower(indexName)
	tnW := b.resolveCtx.GetTableName(tn)
	indicesInfo := tnW.TableInfo.Indices
	cols := tnW.TableInfo.Cols()
	for _, idxInfo := range indicesInfo {
		if idxInfo.Name.L != indexName {
			continue
		}
		for _, idxCol := range idxInfo.Columns {
			col := cols[idxCol.Offset]
			names = append(names, &types.FieldName{
				ColName: idxCol.Name,
				TblName: tn.Name,
				DBName:  tn.Schema,
			})
			schema.Append(&expression.Column{
				RetType:  &col.FieldType,
				UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
				ID:       col.ID})
		}
		names = append(names, &types.FieldName{
			ColName: ast.NewCIStr("extra_handle"),
			TblName: tn.Name,
			DBName:  tn.Schema,
		})
		schema.Append(&expression.Column{
			RetType:  types.NewFieldType(mysql.TypeLonglong),
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			ID:       -1,
		})
	}
	if schema.Len() == 0 {
		return nil, nil, errors.Errorf("index %s not found", indexName)
	}
	return schema, names, nil
}

// getColsInfo returns the info of index columns, normal columns and primary key.
func (b *PlanBuilder) getColsInfo(tn *ast.TableName) (indicesInfo []*model.IndexInfo, colsInfo []*model.ColumnInfo) {
	tnW := b.resolveCtx.GetTableName(tn)
	tbl := tnW.TableInfo
	for _, col := range tbl.Columns {
		// The virtual column will not store any data in TiKV, so it should be ignored when collect statistics
		if col.IsVirtualGenerated() {
			continue
		}
		if mysql.HasPriKeyFlag(col.GetFlag()) && tbl.HasClusteredIndex() {
			continue
		}
		colsInfo = append(colsInfo, col)
	}
	for _, idx := range tnW.TableInfo.Indices {
		if idx.State == model.StatePublic {
			indicesInfo = append(indicesInfo, idx)
		}
	}
	return
}
