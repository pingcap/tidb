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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/cznic/sortutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

// executorBuilder builds an Executor from a Plan.
// The InfoSchema must not change during execution.
type executorBuilder struct {
	ctx     sessionctx.Context
	is      infoschema.InfoSchema
	startTS uint64 // cached when the first time getStartTS() is called
	// err is set when there is error happened during Executor building process.
	err error
}

func newExecutorBuilder(ctx sessionctx.Context, is infoschema.InfoSchema) *executorBuilder {
	return &executorBuilder{
		ctx: ctx,
		is:  is,
	}
}

func (b *executorBuilder) build(p plannercore.Plan) Executor {
	switch v := p.(type) {
	case nil:
		return nil
	case *plannercore.CheckTable:
		return b.buildCheckTable(v)
	case *plannercore.CheckIndex:
		return b.buildCheckIndex(v)
	case *plannercore.RecoverIndex:
		return b.buildRecoverIndex(v)
	case *plannercore.CleanupIndex:
		return b.buildCleanupIndex(v)
	case *plannercore.CheckIndexRange:
		return b.buildCheckIndexRange(v)
	case *plannercore.ChecksumTable:
		return b.buildChecksumTable(v)
	case *plannercore.ReloadExprPushdownBlacklist:
		return b.buildReloadExprPushdownBlacklist(v)
	case *plannercore.AdminPlugins:
		return b.buildAdminPlugins(v)
	case *plannercore.DDL:
		return b.buildDDL(v)
	case *plannercore.Deallocate:
		return b.buildDeallocate(v)
	case *plannercore.Delete:
		return b.buildDelete(v)
	case *plannercore.Execute:
		return b.buildExecute(v)
	case *plannercore.Trace:
		return b.buildTrace(v)
	case *plannercore.Explain:
		return b.buildExplain(v)
	case *plannercore.PointGetPlan:
		return b.buildPointGet(v)
	case *plannercore.Insert:
		return b.buildInsert(v)
	case *plannercore.LoadData:
		return b.buildLoadData(v)
	case *plannercore.LoadStats:
		return b.buildLoadStats(v)
	case *plannercore.PhysicalLimit:
		return b.buildLimit(v)
	case *plannercore.Prepare:
		return b.buildPrepare(v)
	case *plannercore.PhysicalLock:
		return b.buildSelectLock(v)
	case *plannercore.CancelDDLJobs:
		return b.buildCancelDDLJobs(v)
	case *plannercore.ShowNextRowID:
		return b.buildShowNextRowID(v)
	case *plannercore.ShowDDL:
		return b.buildShowDDL(v)
	case *plannercore.ShowDDLJobs:
		return b.buildShowDDLJobs(v)
	case *plannercore.ShowDDLJobQueries:
		return b.buildShowDDLJobQueries(v)
	case *plannercore.ShowSlow:
		return b.buildShowSlow(v)
	case *plannercore.Show:
		return b.buildShow(v)
	case *plannercore.Simple:
		return b.buildSimple(v)
	case *plannercore.Set:
		return b.buildSet(v)
	case *plannercore.PhysicalSort:
		return b.buildSort(v)
	case *plannercore.PhysicalTopN:
		return b.buildTopN(v)
	case *plannercore.PhysicalUnionAll:
		return b.buildUnionAll(v)
	case *plannercore.Update:
		return b.buildUpdate(v)
	case *plannercore.PhysicalUnionScan:
		return b.buildUnionScanExec(v)
	case *plannercore.PhysicalHashJoin:
		return b.buildHashJoin(v)
	case *plannercore.PhysicalMergeJoin:
		return b.buildMergeJoin(v)
	case *plannercore.PhysicalIndexJoin:
		return b.buildIndexLookUpJoin(v)
	case *plannercore.PhysicalSelection:
		return b.buildSelection(v)
	case *plannercore.PhysicalHashAgg:
		return b.buildHashAgg(v)
	case *plannercore.PhysicalStreamAgg:
		return b.buildStreamAgg(v)
	case *plannercore.PhysicalProjection:
		return b.buildProjection(v)
	case *plannercore.PhysicalMemTable:
		return b.buildMemTable(v)
	case *plannercore.PhysicalTableDual:
		return b.buildTableDual(v)
	case *plannercore.PhysicalApply:
		return b.buildApply(v)
	case *plannercore.PhysicalMaxOneRow:
		return b.buildMaxOneRow(v)
	case *plannercore.Analyze:
		return b.buildAnalyze(v)
	case *plannercore.PhysicalTableReader:
		return b.buildTableReader(v)
	case *plannercore.PhysicalIndexReader:
		return b.buildIndexReader(v)
	case *plannercore.PhysicalIndexLookUpReader:
		return b.buildIndexLookUpReader(v)
	case *plannercore.SplitRegion:
		return b.buildSplitRegion(v)
	default:
		b.err = ErrUnknownPlan.GenWithStack("Unknown Plan %T", p)
		return nil
	}
}

func (b *executorBuilder) buildCancelDDLJobs(v *plannercore.CancelDDLJobs) Executor {
	e := &CancelDDLJobsExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		jobIDs:       v.JobIDs,
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}

	e.errs, b.err = admin.CancelJobs(txn, e.jobIDs)
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	return e
}

func (b *executorBuilder) buildShowNextRowID(v *plannercore.ShowNextRowID) Executor {
	e := &ShowNextRowIDExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		tblName:      v.TableName,
	}
	return e
}

func (b *executorBuilder) buildShowDDL(v *plannercore.ShowDDL) Executor {
	// We get DDLInfo here because for Executors that returns result set,
	// next will be called after transaction has been committed.
	// We need the transaction to get DDLInfo.
	e := &ShowDDLExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
	}

	var err error
	ownerManager := domain.GetDomain(e.ctx).DDL().OwnerManager()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	e.ddlOwnerID, err = ownerManager.GetOwnerID(ctx)
	cancel()
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}

	ddlInfo, err := admin.GetDDLInfo(txn)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	e.ddlInfo = ddlInfo
	e.selfID = ownerManager.ID()
	return e
}

func (b *executorBuilder) buildShowDDLJobs(v *plannercore.ShowDDLJobs) Executor {
	e := &ShowDDLJobsExec{
		jobNumber:    v.JobNumber,
		is:           b.is,
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
	}
	return e
}

func (b *executorBuilder) buildShowDDLJobQueries(v *plannercore.ShowDDLJobQueries) Executor {
	e := &ShowDDLJobQueriesExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		jobIDs:       v.JobIDs,
	}
	return e
}

func (b *executorBuilder) buildShowSlow(v *plannercore.ShowSlow) Executor {
	e := &ShowSlowExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		ShowSlow:     v.ShowSlow,
	}
	return e
}

func (b *executorBuilder) buildCheckIndex(v *plannercore.CheckIndex) Executor {
	readerExec, err := buildNoRangeIndexLookUpReader(b, v.IndexLookUpReader)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}

	buildIndexLookUpChecker(b, v.IndexLookUpReader, readerExec)

	e := &CheckIndexExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		dbName:       v.DBName,
		tableName:    readerExec.table.Meta().Name.L,
		idxName:      v.IdxName,
		is:           b.is,
		src:          readerExec,
	}
	return e
}

// buildIndexLookUpChecker builds check information to IndexLookUpReader.
func buildIndexLookUpChecker(b *executorBuilder, readerPlan *plannercore.PhysicalIndexLookUpReader,
	readerExec *IndexLookUpExecutor) {
	is := readerPlan.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	readerExec.dagPB.OutputOffsets = make([]uint32, 0, len(is.Index.Columns))
	for i := 0; i <= len(is.Index.Columns); i++ {
		readerExec.dagPB.OutputOffsets = append(readerExec.dagPB.OutputOffsets, uint32(i))
	}
	readerExec.ranges = ranger.FullRange()
	ts := readerPlan.TablePlans[0].(*plannercore.PhysicalTableScan)
	readerExec.handleIdx = ts.HandleIdx

	tps := make([]*types.FieldType, 0, len(is.Columns)+1)
	for _, col := range is.Columns {
		tps = append(tps, &col.FieldType)
	}
	tps = append(tps, types.NewFieldType(mysql.TypeLonglong))
	readerExec.checkIndexValue = &checkIndexValue{genExprs: is.GenExprs, idxColTps: tps}

	colNames := make([]string, 0, len(is.Columns))
	for _, col := range is.Columns {
		colNames = append(colNames, col.Name.O)
	}
	var err error
	readerExec.idxTblCols, err = table.FindCols(readerExec.table.Cols(), colNames, true)
	if err != nil {
		b.err = errors.Trace(err)
		return
	}
}

func (b *executorBuilder) buildCheckTable(v *plannercore.CheckTable) Executor {
	readerExecs := make([]*IndexLookUpExecutor, 0, len(v.IndexLookUpReaders))
	for _, readerPlan := range v.IndexLookUpReaders {
		readerExec, err := buildNoRangeIndexLookUpReader(b, readerPlan)
		if err != nil {
			b.err = errors.Trace(err)
			return nil
		}
		buildIndexLookUpChecker(b, readerPlan, readerExec)

		readerExecs = append(readerExecs, readerExec)
	}

	e := &CheckTableExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		dbName:       v.DBName,
		table:        v.Table,
		indexInfos:   v.IndexInfos,
		is:           b.is,
		srcs:         readerExecs,
		exitCh:       make(chan struct{}),
		retCh:        make(chan error, len(readerExecs)),
	}
	return e
}

func buildRecoverIndexCols(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) []*model.ColumnInfo {
	columns := make([]*model.ColumnInfo, 0, len(indexInfo.Columns))
	for _, idxCol := range indexInfo.Columns {
		columns = append(columns, tblInfo.Columns[idxCol.Offset])
	}

	handleOffset := len(columns)
	handleColsInfo := &model.ColumnInfo{
		ID:     model.ExtraHandleID,
		Name:   model.ExtraHandleName,
		Offset: handleOffset,
	}
	handleColsInfo.FieldType = *types.NewFieldType(mysql.TypeLonglong)
	columns = append(columns, handleColsInfo)
	return columns
}

func (b *executorBuilder) buildRecoverIndex(v *plannercore.RecoverIndex) Executor {
	tblInfo := v.Table.TableInfo
	t, err := b.is.TableByName(v.Table.Schema, tblInfo.Name)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	idxName := strings.ToLower(v.IndexName)
	indices := t.WritableIndices()
	var index table.Index
	for _, idx := range indices {
		if idxName == idx.Meta().Name.L {
			index = idx
			break
		}
	}

	if index == nil {
		b.err = errors.Errorf("index `%v` is not found in table `%v`.", v.IndexName, v.Table.Name.O)
		return nil
	}
	e := &RecoverIndexExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		columns:      buildRecoverIndexCols(tblInfo, index.Meta()),
		index:        index,
		table:        t,
	}
	return e
}

func buildCleanupIndexCols(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) []*model.ColumnInfo {
	columns := make([]*model.ColumnInfo, 0, len(indexInfo.Columns)+1)
	for _, idxCol := range indexInfo.Columns {
		columns = append(columns, tblInfo.Columns[idxCol.Offset])
	}
	handleColsInfo := &model.ColumnInfo{
		ID:     model.ExtraHandleID,
		Name:   model.ExtraHandleName,
		Offset: len(tblInfo.Columns),
	}
	handleColsInfo.FieldType = *types.NewFieldType(mysql.TypeLonglong)
	columns = append(columns, handleColsInfo)
	return columns
}

func (b *executorBuilder) buildCleanupIndex(v *plannercore.CleanupIndex) Executor {
	tblInfo := v.Table.TableInfo
	t, err := b.is.TableByName(v.Table.Schema, tblInfo.Name)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	idxName := strings.ToLower(v.IndexName)
	var index table.Index
	for _, idx := range t.Indices() {
		if idx.Meta().State != model.StatePublic {
			continue
		}
		if idxName == idx.Meta().Name.L {
			index = idx
			break
		}
	}

	if index == nil {
		b.err = errors.Errorf("index `%v` is not found in table `%v`.", v.IndexName, v.Table.Name.O)
		return nil
	}
	e := &CleanupIndexExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		idxCols:      buildCleanupIndexCols(tblInfo, index.Meta()),
		index:        index,
		table:        t,
		batchSize:    20000,
	}
	return e
}

func (b *executorBuilder) buildCheckIndexRange(v *plannercore.CheckIndexRange) Executor {
	tb, err := b.is.TableByName(v.Table.Schema, v.Table.Name)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	e := &CheckIndexRangeExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		handleRanges: v.HandleRanges,
		table:        tb.Meta(),
		is:           b.is,
	}
	idxName := strings.ToLower(v.IndexName)
	for _, idx := range tb.Indices() {
		if idx.Meta().Name.L == idxName {
			e.index = idx.Meta()
			e.startKey = make([]types.Datum, len(e.index.Columns))
			break
		}
	}
	return e
}

func (b *executorBuilder) buildChecksumTable(v *plannercore.ChecksumTable) Executor {
	e := &ChecksumTableExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		tables:       make(map[int64]*checksumContext),
		done:         false,
	}
	startTs, err := b.getStartTS()
	if err != nil {
		b.err = err
		return nil
	}
	for _, t := range v.Tables {
		e.tables[t.TableInfo.ID] = newChecksumContext(t.DBInfo, t.TableInfo, startTs)
	}
	return e
}

func (b *executorBuilder) buildReloadExprPushdownBlacklist(v *plannercore.ReloadExprPushdownBlacklist) Executor {
	return &ReloadExprPushdownBlacklistExec{baseExecutor{ctx: b.ctx}}
}

func (b *executorBuilder) buildAdminPlugins(v *plannercore.AdminPlugins) Executor {
	return &AdminPluginsExec{baseExecutor: baseExecutor{ctx: b.ctx}, Action: v.Action, Plugins: v.Plugins}
}

func (b *executorBuilder) buildDeallocate(v *plannercore.Deallocate) Executor {
	base := newBaseExecutor(b.ctx, nil, v.ExplainID())
	base.initCap = chunk.ZeroCapacity
	e := &DeallocateExec{
		baseExecutor: base,
		Name:         v.Name,
	}
	return e
}

func (b *executorBuilder) buildSelectLock(v *plannercore.PhysicalLock) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	if !b.ctx.GetSessionVars().InTxn() {
		// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
		// is disabled (either by beginning transaction with START TRANSACTION or by setting
		// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
		// See https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
		return src
	}
	e := &SelectLockExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), src),
		Lock:         v.Lock,
	}
	return e
}

func (b *executorBuilder) buildLimit(v *plannercore.PhysicalLimit) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	n := int(mathutil.MinUint64(v.Count, uint64(b.ctx.GetSessionVars().MaxChunkSize)))
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec)
	base.initCap = n
	e := &LimitExec{
		baseExecutor: base,
		begin:        v.Offset,
		end:          v.Offset + v.Count,
	}
	return e
}

func (b *executorBuilder) buildPrepare(v *plannercore.Prepare) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = chunk.ZeroCapacity
	return &PrepareExec{
		baseExecutor: base,
		is:           b.is,
		name:         v.Name,
		sqlText:      v.SQLText,
	}
}

func (b *executorBuilder) buildExecute(v *plannercore.Execute) Executor {
	e := &ExecuteExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		is:           b.is,
		name:         v.Name,
		usingVars:    v.UsingVars,
		id:           v.ExecID,
		stmt:         v.Stmt,
		plan:         v.Plan,
	}
	return e
}

func (b *executorBuilder) buildShow(v *plannercore.Show) Executor {
	e := &ShowExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		Tp:           v.Tp,
		DBName:       model.NewCIStr(v.DBName),
		Table:        v.Table,
		Column:       v.Column,
		IndexName:    v.IndexName,
		User:         v.User,
		Flag:         v.Flag,
		Full:         v.Full,
		GlobalScope:  v.GlobalScope,
		is:           b.is,
	}
	if e.Tp == ast.ShowGrants && e.User == nil {
		e.User = e.ctx.GetSessionVars().User
	}
	if e.Tp == ast.ShowMasterStatus {
		// show master status need start ts.
		if _, err := e.ctx.Txn(true); err != nil {
			b.err = errors.Trace(err)
		}
	}
	return e
}

func (b *executorBuilder) buildSimple(v *plannercore.Simple) Executor {
	switch s := v.Statement.(type) {
	case *ast.GrantStmt:
		return b.buildGrant(s)
	case *ast.RevokeStmt:
		return b.buildRevoke(s)
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = chunk.ZeroCapacity
	e := &SimpleExec{
		baseExecutor: base,
		Statement:    v.Statement,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildSet(v *plannercore.Set) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = chunk.ZeroCapacity
	e := &SetExecutor{
		baseExecutor: base,
		vars:         v.VarAssigns,
	}
	return e
}

func (b *executorBuilder) buildInsert(v *plannercore.Insert) Executor {
	selectExec := b.build(v.SelectPlan)
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	var baseExec baseExecutor
	if selectExec != nil {
		baseExec = newBaseExecutor(b.ctx, nil, v.ExplainID(), selectExec)
	} else {
		baseExec = newBaseExecutor(b.ctx, nil, v.ExplainID())
	}
	baseExec.initCap = chunk.ZeroCapacity

	ivs := &InsertValues{
		baseExecutor: baseExec,
		Table:        v.Table,
		Columns:      v.Columns,
		Lists:        v.Lists,
		SetList:      v.SetList,
		GenColumns:   v.GenCols.Columns,
		GenExprs:     v.GenCols.Exprs,
		hasRefCols:   v.NeedFillDefaultValue,
		SelectExec:   selectExec,
	}
	err := ivs.initInsertColumns()
	if err != nil {
		b.err = err
		return nil
	}

	if v.IsReplace {
		return b.buildReplace(ivs)
	}
	insert := &InsertExec{
		InsertValues: ivs,
		OnDuplicate:  append(v.OnDuplicate, v.GenCols.OnDuplicates...),
	}
	return insert
}

func (b *executorBuilder) buildLoadData(v *plannercore.LoadData) Executor {
	tbl, ok := b.is.TableByID(v.Table.TableInfo.ID)
	if !ok {
		b.err = errors.Errorf("Can not get table %d", v.Table.TableInfo.ID)
		return nil
	}
	insertVal := &InsertValues{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ExplainID()),
		Table:        tbl,
		Columns:      v.Columns,
		GenColumns:   v.GenCols.Columns,
		GenExprs:     v.GenCols.Exprs,
	}
	err := insertVal.initInsertColumns()
	if err != nil {
		b.err = err
		return nil
	}
	loadDataExec := &LoadDataExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ExplainID()),
		IsLocal:      v.IsLocal,
		loadDataInfo: &LoadDataInfo{
			row:          make([]types.Datum, len(insertVal.insertColumns)),
			InsertValues: insertVal,
			Path:         v.Path,
			Table:        tbl,
			FieldsInfo:   v.FieldsInfo,
			LinesInfo:    v.LinesInfo,
			IgnoreLines:  v.IgnoreLines,
			Ctx:          b.ctx,
		},
	}

	return loadDataExec
}

func (b *executorBuilder) buildLoadStats(v *plannercore.LoadStats) Executor {
	e := &LoadStatsExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ExplainID()),
		info:         &LoadStatsInfo{v.Path, b.ctx},
	}
	return e
}

func (b *executorBuilder) buildReplace(vals *InsertValues) Executor {
	replaceExec := &ReplaceExec{
		InsertValues: vals,
	}
	return replaceExec
}

func (b *executorBuilder) buildGrant(grant *ast.GrantStmt) Executor {
	e := &GrantExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, "GrantStmt"),
		Privs:        grant.Privs,
		ObjectType:   grant.ObjectType,
		Level:        grant.Level,
		Users:        grant.Users,
		WithGrant:    grant.WithGrant,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildRevoke(revoke *ast.RevokeStmt) Executor {
	e := &RevokeExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, "RevokeStmt"),
		ctx:          b.ctx,
		Privs:        revoke.Privs,
		ObjectType:   revoke.ObjectType,
		Level:        revoke.Level,
		Users:        revoke.Users,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildDDL(v *plannercore.DDL) Executor {
	e := &DDLExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		stmt:         v.Statement,
		is:           b.is,
	}
	return e
}

// buildTrace builds a TraceExec for future executing. This method will be called
// at build().
func (b *executorBuilder) buildTrace(v *plannercore.Trace) Executor {
	return &TraceExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		stmtNode:     v.StmtNode,
		builder:      b,
	}
}

// buildExplain builds a explain executor. `e.rows` collects final result to `ExplainExec`.
func (b *executorBuilder) buildExplain(v *plannercore.Explain) Executor {
	explainExec := &ExplainExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		explain:      v,
	}
	if v.Analyze {
		b.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl()
		explainExec.analyzeExec = b.build(v.ExecPlan)
	}
	return explainExec
}

func (b *executorBuilder) buildUnionScanExec(v *plannercore.PhysicalUnionScan) Executor {
	reader := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	us, err := b.buildUnionScanFromReader(reader, v)
	if err != nil {
		b.err = err
		return nil
	}
	return us
}

// buildUnionScanFromReader builds union scan executor from child executor.
// Note that this function may be called by inner workers of index lookup join concurrently.
// Be careful to avoid data race.
func (b *executorBuilder) buildUnionScanFromReader(reader Executor, v *plannercore.PhysicalUnionScan) (Executor, error) {
	var err error
	us := &UnionScanExec{baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), reader)}
	// Get the handle column index of the below plannercore.
	// We can guarantee that there must be only one col in the map.
	for _, cols := range v.Children()[0].Schema().TblID2Handle {
		us.belowHandleIndex = cols[0].Index
	}
	switch x := reader.(type) {
	case *TableReaderExecutor:
		us.desc = x.desc
		// Union scan can only be in a write transaction, so DirtyDB should has non-nil value now, thus
		// GetDirtyDB() is safe here. If this table has been modified in the transaction, non-nil DirtyTable
		// can be found in DirtyDB now, so GetDirtyTable is safe; if this table has not been modified in the
		// transaction, empty DirtyTable would be inserted into DirtyDB, it does not matter when multiple
		// goroutines write empty DirtyTable to DirtyDB for this table concurrently. Although the DirtyDB looks
		// safe for data race in all the cases, the map of golang will throw panic when it's accessed in parallel.
		// So we lock it when getting dirty table.
		us.dirty = GetDirtyDB(b.ctx).GetDirtyTable(x.table.Meta().ID)
		us.conditions = v.Conditions
		us.columns = x.columns
		err = us.buildAndSortAddedRows()
	case *IndexReaderExecutor:
		us.desc = x.desc
		for _, ic := range x.index.Columns {
			for i, col := range x.schema.Columns {
				if col.ColName.L == ic.Name.L {
					us.usedIndex = append(us.usedIndex, i)
					break
				}
			}
		}
		us.dirty = GetDirtyDB(b.ctx).GetDirtyTable(x.table.Meta().ID)
		us.conditions = v.Conditions
		us.columns = x.columns
		err = us.buildAndSortAddedRows()
	case *IndexLookUpExecutor:
		us.desc = x.desc
		for _, ic := range x.index.Columns {
			for i, col := range x.schema.Columns {
				if col.ColName.L == ic.Name.L {
					us.usedIndex = append(us.usedIndex, i)
					break
				}
			}
		}
		us.dirty = GetDirtyDB(b.ctx).GetDirtyTable(x.table.Meta().ID)
		us.conditions = v.Conditions
		us.columns = x.columns
		err = us.buildAndSortAddedRows()
	default:
		// The mem table will not be written by sql directly, so we can omit the union scan to avoid err reporting.
		return reader, nil
	}
	if err != nil {
		err = errors.Trace(err)
		return nil, err
	}
	return us, nil
}

// buildMergeJoin builds MergeJoinExec executor.
func (b *executorBuilder) buildMergeJoin(v *plannercore.PhysicalMergeJoin) Executor {
	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}

	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}

	defaultValues := v.DefaultValues
	if defaultValues == nil {
		if v.JoinType == plannercore.RightOuterJoin {
			defaultValues = make([]types.Datum, leftExec.Schema().Len())
		} else {
			defaultValues = make([]types.Datum, rightExec.Schema().Len())
		}
	}

	e := &MergeJoinExec{
		stmtCtx:      b.ctx.GetSessionVars().StmtCtx,
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), leftExec, rightExec),
		compareFuncs: v.CompareFuncs,
		joiner: newJoiner(
			b.ctx,
			v.JoinType,
			v.JoinType == plannercore.RightOuterJoin,
			defaultValues,
			v.OtherConditions,
			leftExec.retTypes(),
			rightExec.retTypes(),
		),
		desc: v.Desc,
	}

	leftKeys := v.LeftKeys
	rightKeys := v.RightKeys

	e.outerIdx = 0
	innerFilter := v.RightConditions

	e.innerTable = &mergeJoinInnerTable{
		reader:   rightExec,
		joinKeys: rightKeys,
	}

	e.outerTable = &mergeJoinOuterTable{
		reader: leftExec,
		filter: v.LeftConditions,
		keys:   leftKeys,
	}

	if v.JoinType == plannercore.RightOuterJoin {
		e.outerIdx = 1
		e.outerTable.reader = rightExec
		e.outerTable.filter = v.RightConditions
		e.outerTable.keys = rightKeys

		innerFilter = v.LeftConditions
		e.innerTable.reader = leftExec
		e.innerTable.joinKeys = leftKeys
	}

	// optimizer should guarantee that filters on inner table are pushed down
	// to tikv or extracted to a Selection.
	if len(innerFilter) != 0 {
		b.err = errors.Annotate(ErrBuildExecutor, "merge join's inner filter should be empty.")
		return nil
	}

	metrics.ExecutorCounter.WithLabelValues("MergeJoinExec").Inc()
	return e
}

func (b *executorBuilder) buildHashJoin(v *plannercore.PhysicalHashJoin) Executor {
	leftHashKey := make([]*expression.Column, 0, len(v.EqualConditions))
	rightHashKey := make([]*expression.Column, 0, len(v.EqualConditions))
	for _, eqCond := range v.EqualConditions {
		ln, _ := eqCond.GetArgs()[0].(*expression.Column)
		rn, _ := eqCond.GetArgs()[1].(*expression.Column)
		leftHashKey = append(leftHashKey, ln)
		rightHashKey = append(rightHashKey, rn)
	}

	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}

	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}

	e := &HashJoinExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), leftExec, rightExec),
		concurrency:  v.Concurrency,
		joinType:     v.JoinType,
		innerIdx:     v.InnerChildIdx,
	}

	defaultValues := v.DefaultValues
	lhsTypes, rhsTypes := leftExec.retTypes(), rightExec.retTypes()
	if v.InnerChildIdx == 0 {
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
		e.innerExec = leftExec
		e.outerExec = rightExec
		e.outerFilter = v.RightConditions
		e.innerKeys = leftHashKey
		e.outerKeys = rightHashKey
		if defaultValues == nil {
			defaultValues = make([]types.Datum, e.innerExec.Schema().Len())
		}
	} else {
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
		e.innerExec = rightExec
		e.outerExec = leftExec
		e.outerFilter = v.LeftConditions
		e.innerKeys = rightHashKey
		e.outerKeys = leftHashKey
		if defaultValues == nil {
			defaultValues = make([]types.Datum, e.innerExec.Schema().Len())
		}
	}
	e.joiners = make([]joiner, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joiners[i] = newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues,
			v.OtherConditions, lhsTypes, rhsTypes)
	}
	metrics.ExecutorCounter.WithLabelValues("HashJoinExec").Inc()
	return e
}

// wrapCastForAggArgs wraps the args of an aggregate function with a cast function.
func (b *executorBuilder) wrapCastForAggArgs(funcs []*aggregation.AggFuncDesc) {
	for _, f := range funcs {
		// We do not need to wrap cast upon these functions,
		// since the EvalXXX method called by the arg is determined by the corresponding arg type.
		if f.Name == ast.AggFuncCount || f.Name == ast.AggFuncMin || f.Name == ast.AggFuncMax || f.Name == ast.AggFuncFirstRow {
			continue
		}
		var castFunc func(ctx sessionctx.Context, expr expression.Expression) expression.Expression
		switch retTp := f.RetTp; retTp.EvalType() {
		case types.ETInt:
			castFunc = expression.WrapWithCastAsInt
		case types.ETReal:
			castFunc = expression.WrapWithCastAsReal
		case types.ETString:
			castFunc = expression.WrapWithCastAsString
		case types.ETDecimal:
			castFunc = expression.WrapWithCastAsDecimal
		default:
			panic("should never happen in executorBuilder.wrapCastForAggArgs")
		}
		for i := range f.Args {
			f.Args[i] = castFunc(b.ctx, f.Args[i])
			if f.Name != ast.AggFuncAvg && f.Name != ast.AggFuncSum {
				continue
			}
			// After wrapping cast on the argument, flen etc. may not the same
			// as the type of the aggregation function. The following part set
			// the type of the argument exactly as the type of the aggregation
			// function.
			// Note: If the `Tp` of argument is the same as the `Tp` of the
			// aggregation function, it will not wrap cast function on it
			// internally. The reason of the special handling for `Column` is
			// that the `RetType` of `Column` refers to the `infoschema`, so we
			// need to set a new variable for it to avoid modifying the
			// definition in `infoschema`.
			if col, ok := f.Args[i].(*expression.Column); ok {
				col.RetType = types.NewFieldType(col.RetType.Tp)
			}
			// originTp is used when the the `Tp` of column is TypeFloat32 while
			// the type of the aggregation function is TypeFloat64.
			originTp := f.Args[i].GetType().Tp
			*(f.Args[i].GetType()) = *(f.RetTp)
			f.Args[i].GetType().Tp = originTp
		}
	}
}

// buildProjBelowAgg builds a ProjectionExec below AggregationExec.
// If all the args of `aggFuncs`, and all the item of `groupByItems`
// are columns or constants, we do not need to build the `proj`.
func (b *executorBuilder) buildProjBelowAgg(aggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression, src Executor) Executor {
	hasScalarFunc := false
	// If the mode is FinalMode, we do not need to wrap cast upon the args,
	// since the types of the args are already the expected.
	if len(aggFuncs) > 0 && aggFuncs[0].Mode != aggregation.FinalMode {
		b.wrapCastForAggArgs(aggFuncs)
	}
	for i := 0; !hasScalarFunc && i < len(aggFuncs); i++ {
		f := aggFuncs[i]
		for _, arg := range f.Args {
			_, isScalarFunc := arg.(*expression.ScalarFunction)
			hasScalarFunc = hasScalarFunc || isScalarFunc
		}
	}
	for i, isScalarFunc := 0, false; !hasScalarFunc && i < len(groupByItems); i++ {
		_, isScalarFunc = groupByItems[i].(*expression.ScalarFunction)
		hasScalarFunc = hasScalarFunc || isScalarFunc
	}
	if !hasScalarFunc {
		return src
	}

	b.ctx.GetSessionVars().PlanID++
	id := b.ctx.GetSessionVars().PlanID
	projFromID := fmt.Sprintf("%s_%d", plannercore.TypeProj, id)

	projSchemaCols := make([]*expression.Column, 0, len(aggFuncs)+len(groupByItems))
	projExprs := make([]expression.Expression, 0, cap(projSchemaCols))
	cursor := 0
	for _, f := range aggFuncs {
		for i, arg := range f.Args {
			if _, isCnst := arg.(*expression.Constant); isCnst {
				continue
			}
			projExprs = append(projExprs, arg)
			newArg := &expression.Column{
				RetType: arg.GetType(),
				ColName: model.NewCIStr(fmt.Sprintf("%s_%d", f.Name, i)),
				Index:   cursor,
			}
			projSchemaCols = append(projSchemaCols, newArg)
			f.Args[i] = newArg
			cursor++
		}
	}
	for i, item := range groupByItems {
		if _, isCnst := item.(*expression.Constant); isCnst {
			continue
		}
		projExprs = append(projExprs, item)
		newArg := &expression.Column{
			RetType: item.GetType(),
			ColName: model.NewCIStr(fmt.Sprintf("group_%d", i)),
			Index:   cursor,
		}
		projSchemaCols = append(projSchemaCols, newArg)
		groupByItems[i] = newArg
		cursor++
	}

	return &ProjectionExec{
		baseExecutor:  newBaseExecutor(b.ctx, expression.NewSchema(projSchemaCols...), projFromID, src),
		numWorkers:    b.ctx.GetSessionVars().ProjectionConcurrency,
		evaluatorSuit: expression.NewEvaluatorSuite(projExprs, false),
	}
}

func (b *executorBuilder) buildHashAgg(v *plannercore.PhysicalHashAgg) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	sessionVars := b.ctx.GetSessionVars()
	e := &HashAggExec{
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), src),
		sc:              sessionVars.StmtCtx,
		PartialAggFuncs: make([]aggfuncs.AggFunc, 0, len(v.AggFuncs)),
		GroupByItems:    v.GroupByItems,
	}
	// We take `create table t(a int, b int);` as example.
	//
	// 1. If all the aggregation functions are FIRST_ROW, we do not need to set the defaultVal for them:
	// e.g.
	// mysql> select distinct a, b from t;
	// 0 rows in set (0.00 sec)
	//
	// 2. If there exists group by items, we do not need to set the defaultVal for them either:
	// e.g.
	// mysql> select avg(a) from t group by b;
	// Empty set (0.00 sec)
	//
	// mysql> select avg(a) from t group by a;
	// +--------+
	// | avg(a) |
	// +--------+
	// |  NULL  |
	// +--------+
	// 1 row in set (0.00 sec)
	if len(v.GroupByItems) != 0 || aggregation.IsAllFirstRow(v.AggFuncs) {
		e.defaultVal = nil
	} else {
		e.defaultVal = chunk.NewChunkWithCapacity(e.retTypes(), 1)
	}
	for _, aggDesc := range v.AggFuncs {
		if aggDesc.HasDistinct {
			e.isUnparallelExec = true
		}
	}
	// When we set both tidb_hashagg_final_concurrency and tidb_hashagg_partial_concurrency to 1,
	// we do not need to parallelly execute hash agg,
	// and this action can be a workaround when meeting some unexpected situation using parallelExec.
	if finalCon, partialCon := sessionVars.HashAggFinalConcurrency, sessionVars.HashAggPartialConcurrency; finalCon <= 0 || partialCon <= 0 || finalCon == 1 && partialCon == 1 {
		e.isUnparallelExec = true
	}
	partialOrdinal := 0
	for i, aggDesc := range v.AggFuncs {
		if e.isUnparallelExec {
			e.PartialAggFuncs = append(e.PartialAggFuncs, aggfuncs.Build(b.ctx, aggDesc, i))
		} else {
			ordinal := []int{partialOrdinal}
			partialOrdinal++
			if aggDesc.Name == ast.AggFuncAvg {
				ordinal = append(ordinal, partialOrdinal+1)
				partialOrdinal++
			}
			partialAggDesc, finalDesc := aggDesc.Split(ordinal)
			partialAggFunc := aggfuncs.Build(b.ctx, partialAggDesc, i)
			finalAggFunc := aggfuncs.Build(b.ctx, finalDesc, i)
			e.PartialAggFuncs = append(e.PartialAggFuncs, partialAggFunc)
			e.FinalAggFuncs = append(e.FinalAggFuncs, finalAggFunc)
			if partialAggDesc.Name == ast.AggFuncGroupConcat {
				// For group_concat, finalAggFunc and partialAggFunc need shared `truncate` flag to do duplicate.
				finalAggFunc.(interface{ SetTruncated(t *int32) }).SetTruncated(
					partialAggFunc.(interface{ GetTruncated() *int32 }).GetTruncated(),
				)
			}
		}
		if e.defaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.defaultVal.AppendDatum(i, &value)
		}
	}

	metrics.ExecutorCounter.WithLabelValues("HashAggExec").Inc()
	return e
}

func (b *executorBuilder) buildStreamAgg(v *plannercore.PhysicalStreamAgg) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	e := &StreamAggExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), src),
		StmtCtx:      b.ctx.GetSessionVars().StmtCtx,
		aggFuncs:     make([]aggfuncs.AggFunc, 0, len(v.AggFuncs)),
		GroupByItems: v.GroupByItems,
	}
	if len(v.GroupByItems) != 0 || aggregation.IsAllFirstRow(v.AggFuncs) {
		e.defaultVal = nil
	} else {
		e.defaultVal = chunk.NewChunkWithCapacity(e.retTypes(), 1)
	}
	for i, aggDesc := range v.AggFuncs {
		aggFunc := aggfuncs.Build(b.ctx, aggDesc, i)
		e.aggFuncs = append(e.aggFuncs, aggFunc)
		if e.defaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.defaultVal.AppendDatum(i, &value)
		}
	}

	metrics.ExecutorCounter.WithLabelValues("StreamAggExec").Inc()
	return e
}

func (b *executorBuilder) buildSelection(v *plannercore.PhysicalSelection) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	e := &SelectionExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
		filters:      v.Conditions,
	}
	return e
}

func (b *executorBuilder) buildProjection(v *plannercore.PhysicalProjection) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	e := &ProjectionExec{
		baseExecutor:     newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
		numWorkers:       b.ctx.GetSessionVars().ProjectionConcurrency,
		evaluatorSuit:    expression.NewEvaluatorSuite(v.Exprs, v.AvoidColumnEvaluator),
		calculateNoDelay: v.CalculateNoDelay,
	}

	// If the calculation row count for this Projection operator is smaller
	// than a Chunk size, we turn back to the un-parallel Projection
	// implementation to reduce the goroutine overhead.
	if int64(v.StatsCount()) < int64(b.ctx.GetSessionVars().MaxChunkSize) {
		e.numWorkers = 0
	}
	return e
}

func (b *executorBuilder) buildTableDual(v *plannercore.PhysicalTableDual) Executor {
	if v.RowCount != 0 && v.RowCount != 1 {
		b.err = errors.Errorf("buildTableDual failed, invalid row count for dual table: %v", v.RowCount)
		return nil
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = v.RowCount
	e := &TableDualExec{
		baseExecutor: base,
		numDualRows:  v.RowCount,
	}
	return e
}

func (b *executorBuilder) getStartTS() (uint64, error) {
	if b.startTS != 0 {
		// Return the cached value.
		return b.startTS, nil
	}

	startTS := b.ctx.GetSessionVars().SnapshotTS
	txn, err := b.ctx.Txn(true)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if startTS == 0 {
		startTS = txn.StartTS()
	}
	b.startTS = startTS
	if b.startTS == 0 {
		return 0, errors.Trace(ErrGetStartTS)
	}
	return startTS, nil
}

func (b *executorBuilder) buildMemTable(v *plannercore.PhysicalMemTable) Executor {
	tb, _ := b.is.TableByID(v.Table.ID)
	e := &TableScanExec{
		baseExecutor:   newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		t:              tb,
		columns:        v.Columns,
		seekHandle:     math.MinInt64,
		isVirtualTable: tb.Type() == table.VirtualTable,
	}
	return e
}

func (b *executorBuilder) buildSort(v *plannercore.PhysicalSort) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	sortExec := SortExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
		ByItems:      v.ByItems,
		schema:       v.Schema(),
	}
	metrics.ExecutorCounter.WithLabelValues("SortExec").Inc()
	return &sortExec
}

func (b *executorBuilder) buildTopN(v *plannercore.PhysicalTopN) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	sortExec := SortExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
		ByItems:      v.ByItems,
		schema:       v.Schema(),
	}
	metrics.ExecutorCounter.WithLabelValues("TopNExec").Inc()
	return &TopNExec{
		SortExec: sortExec,
		limit:    &plannercore.PhysicalLimit{Count: v.Count, Offset: v.Offset},
	}
}

func (b *executorBuilder) buildApply(apply *plannercore.PhysicalApply) *NestedLoopApplyExec {
	v := apply.PhysicalJoin
	leftChild := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	rightChild := b.build(v.Children()[1])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	joinSchema := expression.MergeSchema(leftChild.Schema(), rightChild.Schema())
	// TODO: remove this. Do this in Apply's ResolveIndices.
	for i, cond := range v.EqualConditions {
		v.EqualConditions[i] = cond.ResolveIndices(joinSchema).(*expression.ScalarFunction)
	}
	otherConditions := append(expression.ScalarFuncs2Exprs(v.EqualConditions), v.OtherConditions...)
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Datum, v.Children()[v.InnerChildIdx].Schema().Len())
	}
	tupleJoiner := newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0,
		defaultValues, otherConditions, leftChild.retTypes(), rightChild.retTypes())
	outerExec, innerExec := leftChild, rightChild
	outerFilter, innerFilter := v.LeftConditions, v.RightConditions
	if v.InnerChildIdx == 0 {
		outerExec, innerExec = rightChild, leftChild
		outerFilter, innerFilter = v.RightConditions, v.LeftConditions
	}
	e := &NestedLoopApplyExec{
		baseExecutor: newBaseExecutor(b.ctx, apply.Schema(), v.ExplainID(), outerExec, innerExec),
		innerExec:    innerExec,
		outerExec:    outerExec,
		outerFilter:  outerFilter,
		innerFilter:  innerFilter,
		outer:        v.JoinType != plannercore.InnerJoin,
		joiner:       tupleJoiner,
		outerSchema:  apply.OuterSchema,
	}
	metrics.ExecutorCounter.WithLabelValues("NestedLoopApplyExec").Inc()
	return e
}

func (b *executorBuilder) buildMaxOneRow(v *plannercore.PhysicalMaxOneRow) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec)
	base.initCap = 2
	base.maxChunkSize = 2
	e := &MaxOneRowExec{baseExecutor: base}
	return e
}

func (b *executorBuilder) buildUnionAll(v *plannercore.PhysicalUnionAll) Executor {
	childExecs := make([]Executor, len(v.Children()))
	for i, child := range v.Children() {
		childExecs[i] = b.build(child)
		if b.err != nil {
			b.err = errors.Trace(b.err)
			return nil
		}
	}
	e := &UnionExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExecs...),
	}
	return e
}

func (b *executorBuilder) buildSplitRegion(v *plannercore.SplitRegion) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = 1
	base.maxChunkSize = 1
	if v.IndexInfo != nil {
		return &SplitIndexRegionExec{
			baseExecutor: base,
			tableInfo:    v.TableInfo,
			indexInfo:    v.IndexInfo,
			lower:        v.Lower,
			upper:        v.Upper,
			num:          v.Num,
			valueLists:   v.ValueLists,
		}
	}
	if len(v.ValueLists) > 0 {
		return &SplitTableRegionExec{
			baseExecutor: base,
			tableInfo:    v.TableInfo,
			valueLists:   v.ValueLists,
		}
	}
	return &SplitTableRegionExec{
		baseExecutor: base,
		tableInfo:    v.TableInfo,
		lower:        v.Lower[0],
		upper:        v.Upper[0],
		num:          v.Num,
	}
}

func (b *executorBuilder) buildUpdate(v *plannercore.Update) Executor {
	tblID2table := make(map[int64]table.Table)
	for id := range v.SelectPlan.Schema().TblID2Handle {
		tblID2table[id], _ = b.is.TableByID(id)
	}
	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	columns2Handle := buildColumns2Handle(v.SelectPlan.Schema(), tblID2table)
	base := newBaseExecutor(b.ctx, nil, v.ExplainID(), selExec)
	base.initCap = chunk.ZeroCapacity
	updateExec := &UpdateExec{
		baseExecutor:   base,
		SelectExec:     selExec,
		OrderedList:    v.OrderedList,
		tblID2table:    tblID2table,
		columns2Handle: columns2Handle,
	}
	return updateExec
}

// cols2Handle represents an mapper from column index to handle index.
type cols2Handle struct {
	// start/end represent the ordinal range [start, end) of the consecutive columns.
	start, end int32
	// handleOrdinal represents the ordinal of the handle column.
	handleOrdinal int32
}

// cols2HandleSlice attaches the methods of sort.Interface to []cols2Handle sorting in increasing order.
type cols2HandleSlice []cols2Handle

// Len implements sort.Interface#Len.
func (c cols2HandleSlice) Len() int {
	return len(c)
}

// Swap implements sort.Interface#Swap.
func (c cols2HandleSlice) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

// Less implements sort.Interface#Less.
func (c cols2HandleSlice) Less(i, j int) bool {
	return c[i].start < c[j].start
}

// findHandle finds the ordinal of the corresponding handle column.
func (c cols2HandleSlice) findHandle(ordinal int32) (int32, bool) {
	if c == nil || len(c) == 0 {
		return 0, false
	}
	// find the smallest index of the range that its start great than ordinal.
	// @see https://godoc.org/sort#Search
	rangeBehindOrdinal := sort.Search(len(c), func(i int) bool { return c[i].start > ordinal })
	if rangeBehindOrdinal == 0 {
		return 0, false
	}
	return c[rangeBehindOrdinal-1].handleOrdinal, true
}

// buildColumns2Handle builds columns to handle mapping.
func buildColumns2Handle(schema *expression.Schema, tblID2Table map[int64]table.Table) cols2HandleSlice {
	if len(schema.TblID2Handle) < 2 {
		// skip buildColumns2Handle mapping if there are only single table.
		return nil
	}
	var cols2Handles cols2HandleSlice
	for tblID, handleCols := range schema.TblID2Handle {
		tbl := tblID2Table[tblID]
		for _, handleCol := range handleCols {
			offset := getTableOffset(schema, handleCol)
			end := offset + len(tbl.WritableCols())
			cols2Handles = append(cols2Handles, cols2Handle{int32(offset), int32(end), int32(handleCol.Index)})
		}
	}
	sort.Sort(cols2Handles)
	return cols2Handles
}

func (b *executorBuilder) buildDelete(v *plannercore.Delete) Executor {
	tblID2table := make(map[int64]table.Table)
	for id := range v.SelectPlan.Schema().TblID2Handle {
		tblID2table[id], _ = b.is.TableByID(id)
	}
	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	base := newBaseExecutor(b.ctx, nil, v.ExplainID(), selExec)
	base.initCap = chunk.ZeroCapacity
	deleteExec := &DeleteExec{
		baseExecutor: base,
		SelectExec:   selExec,
		Tables:       v.Tables,
		IsMultiTable: v.IsMultiTable,
		tblID2Table:  tblID2table,
	}
	return deleteExec
}

func (b *executorBuilder) buildAnalyzeIndexPushdown(task plannercore.AnalyzeIndexTask, maxNumBuckets uint64) *AnalyzeIndexExec {
	_, offset := timeutil.Zone(b.ctx.GetSessionVars().Location())
	e := &AnalyzeIndexExec{
		ctx:             b.ctx,
		physicalTableID: task.PhysicalTableID,
		idxInfo:         task.IndexInfo,
		concurrency:     b.ctx.GetSessionVars().IndexSerialScanConcurrency,
		analyzePB: &tipb.AnalyzeReq{
			Tp:             tipb.AnalyzeType_TypeIndex,
			StartTs:        math.MaxUint64,
			Flags:          statementContextToFlags(b.ctx.GetSessionVars().StmtCtx),
			TimeZoneOffset: offset,
		},
		maxNumBuckets: maxNumBuckets,
	}
	e.analyzePB.IdxReq = &tipb.AnalyzeIndexReq{
		BucketSize: int64(maxNumBuckets),
		NumColumns: int32(len(task.IndexInfo.Columns)),
	}
	depth := int32(defaultCMSketchDepth)
	width := int32(defaultCMSketchWidth)
	e.analyzePB.IdxReq.CmsketchDepth = &depth
	e.analyzePB.IdxReq.CmsketchWidth = &width
	return e
}

func (b *executorBuilder) buildAnalyzeColumnsPushdown(task plannercore.AnalyzeColumnsTask, maxNumBuckets uint64) *AnalyzeColumnsExec {
	cols := task.ColsInfo
	keepOrder := false
	if task.PKInfo != nil {
		keepOrder = true
		cols = append([]*model.ColumnInfo{task.PKInfo}, cols...)
	}

	_, offset := timeutil.Zone(b.ctx.GetSessionVars().Location())
	e := &AnalyzeColumnsExec{
		ctx:             b.ctx,
		physicalTableID: task.PhysicalTableID,
		colsInfo:        task.ColsInfo,
		pkInfo:          task.PKInfo,
		concurrency:     b.ctx.GetSessionVars().DistSQLScanConcurrency,
		keepOrder:       keepOrder,
		analyzePB: &tipb.AnalyzeReq{
			Tp:             tipb.AnalyzeType_TypeColumn,
			StartTs:        math.MaxUint64,
			Flags:          statementContextToFlags(b.ctx.GetSessionVars().StmtCtx),
			TimeZoneOffset: offset,
		},
		maxNumBuckets: maxNumBuckets,
	}
	depth := int32(defaultCMSketchDepth)
	width := int32(defaultCMSketchWidth)
	e.analyzePB.ColReq = &tipb.AnalyzeColumnsReq{
		BucketSize:    int64(maxNumBuckets),
		SampleSize:    maxRegionSampleSize,
		SketchSize:    maxSketchSize,
		ColumnsInfo:   model.ColumnsToProto(cols, task.PKInfo != nil),
		CmsketchDepth: &depth,
		CmsketchWidth: &width,
	}
	b.err = errors.Trace(plannercore.SetPBColumnsDefaultValue(b.ctx, e.analyzePB.ColReq.ColumnsInfo, cols))
	return e
}

func (b *executorBuilder) buildAnalyze(v *plannercore.Analyze) Executor {
	e := &AnalyzeExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		tasks:        make([]*analyzeTask, 0, len(v.ColTasks)+len(v.IdxTasks)),
	}
	for _, task := range v.ColTasks {
		e.tasks = append(e.tasks, &analyzeTask{
			taskType: colTask,
			colExec:  b.buildAnalyzeColumnsPushdown(task, v.MaxNumBuckets),
		})
		if b.err != nil {
			b.err = errors.Trace(b.err)
			return nil
		}
	}
	for _, task := range v.IdxTasks {
		e.tasks = append(e.tasks, &analyzeTask{
			taskType: idxTask,
			idxExec:  b.buildAnalyzeIndexPushdown(task, v.MaxNumBuckets),
		})
		if b.err != nil {
			b.err = errors.Trace(b.err)
			return nil
		}
	}
	return e
}

func constructDistExec(sctx sessionctx.Context, plans []plannercore.PhysicalPlan) ([]*tipb.Executor, bool, error) {
	streaming := true
	executors := make([]*tipb.Executor, 0, len(plans))
	for _, p := range plans {
		execPB, err := p.ToPB(sctx)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if !plannercore.SupportStreaming(p) {
			streaming = false
		}
		executors = append(executors, execPB)
	}
	return executors, streaming, nil
}

func (b *executorBuilder) constructDAGReq(plans []plannercore.PhysicalPlan) (dagReq *tipb.DAGRequest, streaming bool, err error) {
	dagReq = &tipb.DAGRequest{}
	dagReq.StartTs, err = b.getStartTS()
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(b.ctx.GetSessionVars().Location())
	sc := b.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = statementContextToFlags(sc)
	dagReq.Executors, streaming, err = constructDistExec(b.ctx, plans)
	return dagReq, streaming, errors.Trace(err)
}

func (b *executorBuilder) corColInDistPlan(plans []plannercore.PhysicalPlan) bool {
	for _, p := range plans {
		x, ok := p.(*plannercore.PhysicalSelection)
		if !ok {
			continue
		}
		for _, cond := range x.Conditions {
			if len(expression.ExtractCorColumns(cond)) > 0 {
				return true
			}
		}
	}
	return false
}

// corColInAccess checks whether there's correlated column in access conditions.
func (b *executorBuilder) corColInAccess(p plannercore.PhysicalPlan) bool {
	var access []expression.Expression
	switch x := p.(type) {
	case *plannercore.PhysicalTableScan:
		access = x.AccessCondition
	case *plannercore.PhysicalIndexScan:
		access = x.AccessCondition
	}
	for _, cond := range access {
		if len(expression.ExtractCorColumns(cond)) > 0 {
			return true
		}
	}
	return false
}

func (b *executorBuilder) buildIndexLookUpJoin(v *plannercore.PhysicalIndexJoin) Executor {
	outerExec := b.build(v.Children()[v.OuterIndex])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	outerTypes := outerExec.retTypes()
	innerPlan := v.Children()[1-v.OuterIndex]
	innerTypes := make([]*types.FieldType, innerPlan.Schema().Len())
	for i, col := range innerPlan.Schema().Columns {
		innerTypes[i] = col.RetType
	}

	var (
		outerFilter           []expression.Expression
		leftTypes, rightTypes []*types.FieldType
	)

	if v.OuterIndex == 1 {
		leftTypes, rightTypes = innerTypes, outerTypes
		outerFilter = v.RightConditions
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		leftTypes, rightTypes = outerTypes, innerTypes
		outerFilter = v.LeftConditions
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	}
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Datum, len(innerTypes))
	}
	e := &IndexLookUpJoin{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), outerExec),
		outerCtx: outerCtx{
			rowTypes: outerTypes,
			filter:   outerFilter,
		},
		innerCtx: innerCtx{
			readerBuilder: &dataReaderBuilder{Plan: innerPlan, executorBuilder: b},
			rowTypes:      innerTypes,
		},
		workerWg:      new(sync.WaitGroup),
		joiner:        newJoiner(b.ctx, v.JoinType, v.OuterIndex == 1, defaultValues, v.OtherConditions, leftTypes, rightTypes),
		indexRanges:   v.Ranges,
		keyOff2IdxOff: v.KeyOff2IdxOff,
	}
	outerKeyCols := make([]int, len(v.OuterJoinKeys))
	for i := 0; i < len(v.OuterJoinKeys); i++ {
		outerKeyCols[i] = v.OuterJoinKeys[i].Index
	}
	e.outerCtx.keyCols = outerKeyCols
	innerKeyCols := make([]int, len(v.InnerJoinKeys))
	for i := 0; i < len(v.InnerJoinKeys); i++ {
		innerKeyCols[i] = v.InnerJoinKeys[i].Index
	}
	e.innerCtx.keyCols = innerKeyCols
	e.joinResult = e.newFirstChunk()
	metrics.ExecutorCounter.WithLabelValues("IndexLookUpJoin").Inc()
	return e
}

// containsLimit tests if the execs contains Limit because we do not know whether `Limit` has consumed all of its' source,
// so the feedback may not be accurate.
func containsLimit(execs []*tipb.Executor) bool {
	for _, exec := range execs {
		if exec.Limit != nil {
			return true
		}
	}
	return false
}

func buildNoRangeTableReader(b *executorBuilder, v *plannercore.PhysicalTableReader) (*TableReaderExecutor, error) {
	dagReq, streaming, err := b.constructDAGReq(v.TablePlans)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)
	tbl, _ := b.is.TableByID(ts.Table.ID)
	isPartition, physicalTableID := ts.IsPartition()
	if isPartition {
		pt := tbl.(table.PartitionedTable)
		tbl = pt.GetPartition(physicalTableID)
	}
	e := &TableReaderExecutor{
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		dagPB:           dagReq,
		physicalTableID: ts.Table.ID,
		table:           tbl,
		keepOrder:       ts.KeepOrder,
		desc:            ts.Desc,
		columns:         ts.Columns,
		streaming:       streaming,
		corColInFilter:  b.corColInDistPlan(v.TablePlans),
		corColInAccess:  b.corColInAccess(v.TablePlans[0]),
		plans:           v.TablePlans,
	}
	if isPartition, physicalTableID := ts.IsPartition(); isPartition {
		e.physicalTableID = physicalTableID
	}
	if containsLimit(dagReq.Executors) {
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, ts.Desc)
	} else {
		e.feedback = statistics.NewQueryFeedback(e.physicalTableID, ts.Hist, int64(ts.StatsCount()), ts.Desc)
	}
	collect := statistics.CollectFeedback(b.ctx.GetSessionVars().StmtCtx, e.feedback, len(ts.Ranges))
	if !collect {
		e.feedback.Invalidate()
	}
	e.dagPB.CollectRangeCounts = &collect

	for i := range v.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	return e, nil
}

// buildTableReader builds a table reader executor. It first build a no range table reader,
// and then update it ranges from table scan plan.
func (b *executorBuilder) buildTableReader(v *plannercore.PhysicalTableReader) *TableReaderExecutor {
	ret, err := buildNoRangeTableReader(b, v)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}

	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)
	ret.ranges = ts.Ranges
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)
	return ret
}

func buildNoRangeIndexReader(b *executorBuilder, v *plannercore.PhysicalIndexReader) (*IndexReaderExecutor, error) {
	dagReq, streaming, err := b.constructDAGReq(v.IndexPlans)
	if err != nil {
		return nil, errors.Trace(err)
	}
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	table, _ := b.is.TableByID(is.Table.ID)
	e := &IndexReaderExecutor{
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		dagPB:           dagReq,
		physicalTableID: is.Table.ID,
		table:           table,
		index:           is.Index,
		keepOrder:       is.KeepOrder,
		desc:            is.Desc,
		columns:         is.Columns,
		streaming:       streaming,
		corColInFilter:  b.corColInDistPlan(v.IndexPlans),
		corColInAccess:  b.corColInAccess(v.IndexPlans[0]),
		idxCols:         is.IdxCols,
		colLens:         is.IdxColLens,
		plans:           v.IndexPlans,
	}
	if isPartition, physicalTableID := is.IsPartition(); isPartition {
		e.physicalTableID = physicalTableID
	}
	if containsLimit(dagReq.Executors) {
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, is.Desc)
	} else {
		e.feedback = statistics.NewQueryFeedback(e.physicalTableID, is.Hist, int64(is.StatsCount()), is.Desc)
	}
	collect := statistics.CollectFeedback(b.ctx.GetSessionVars().StmtCtx, e.feedback, len(is.Ranges))
	if !collect {
		e.feedback.Invalidate()
	}
	e.dagPB.CollectRangeCounts = &collect

	for _, col := range v.OutputColumns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(col.Index))
	}

	return e, nil
}

func (b *executorBuilder) buildIndexReader(v *plannercore.PhysicalIndexReader) *IndexReaderExecutor {
	ret, err := buildNoRangeIndexReader(b, v)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}

	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	ret.ranges = is.Ranges
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.IndexNames = append(sctx.IndexNames, is.Table.Name.O+":"+is.Index.Name.O)
	return ret
}

func buildNoRangeIndexLookUpReader(b *executorBuilder, v *plannercore.PhysicalIndexLookUpReader) (*IndexLookUpExecutor, error) {
	indexReq, indexStreaming, err := b.constructDAGReq(v.IndexPlans)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableReq, tableStreaming, err := b.constructDAGReq(v.TablePlans)
	if err != nil {
		return nil, errors.Trace(err)
	}
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	indexReq.OutputOffsets = []uint32{uint32(len(is.Index.Columns))}
	table, _ := b.is.TableByID(is.Table.ID)

	for i := 0; i < v.Schema().Len(); i++ {
		tableReq.OutputOffsets = append(tableReq.OutputOffsets, uint32(i))
	}

	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)

	e := &IndexLookUpExecutor{
		baseExecutor:      newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		dagPB:             indexReq,
		physicalTableID:   is.Table.ID,
		table:             table,
		index:             is.Index,
		keepOrder:         is.KeepOrder,
		desc:              is.Desc,
		tableRequest:      tableReq,
		columns:           ts.Columns,
		indexStreaming:    indexStreaming,
		tableStreaming:    tableStreaming,
		dataReaderBuilder: &dataReaderBuilder{executorBuilder: b},
		corColInIdxSide:   b.corColInDistPlan(v.IndexPlans),
		corColInTblSide:   b.corColInDistPlan(v.TablePlans),
		corColInAccess:    b.corColInAccess(v.IndexPlans[0]),
		idxCols:           is.IdxCols,
		colLens:           is.IdxColLens,
		idxPlans:          v.IndexPlans,
		tblPlans:          v.TablePlans,
		PushedLimit:       v.PushedLimit,
	}
	if isPartition, physicalTableID := ts.IsPartition(); isPartition {
		e.physicalTableID = physicalTableID
	}

	if containsLimit(indexReq.Executors) {
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, is.Desc)
	} else {
		e.feedback = statistics.NewQueryFeedback(e.physicalTableID, is.Hist, int64(is.StatsCount()), is.Desc)
	}
	// Do not collect the feedback for table request.
	collectTable := false
	e.tableRequest.CollectRangeCounts = &collectTable
	collectIndex := statistics.CollectFeedback(b.ctx.GetSessionVars().StmtCtx, e.feedback, len(is.Ranges))
	if !collectIndex {
		e.feedback.Invalidate()
	}
	e.dagPB.CollectRangeCounts = &collectIndex
	if cols, ok := v.Schema().TblID2Handle[is.Table.ID]; ok {
		e.handleIdx = cols[0].Index
	}
	return e, nil
}

func (b *executorBuilder) buildIndexLookUpReader(v *plannercore.PhysicalIndexLookUpReader) *IndexLookUpExecutor {
	ret, err := buildNoRangeIndexLookUpReader(b, v)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}

	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)

	ret.ranges = is.Ranges
	metrics.ExecutorCounter.WithLabelValues("IndexLookUpExecutor").Inc()
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.IndexNames = append(sctx.IndexNames, is.Table.Name.O+":"+is.Index.Name.O)
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)
	return ret
}

// dataReaderBuilder build an executor.
// The executor can be used to read data in the ranges which are constructed by datums.
// Differences from executorBuilder:
// 1. dataReaderBuilder calculate data range from argument, rather than plan.
// 2. the result executor is already opened.
type dataReaderBuilder struct {
	plannercore.Plan
	*executorBuilder

	selectResultHook // for testing
}

func (builder *dataReaderBuilder) buildExecutorForIndexJoin(ctx context.Context, datums [][]types.Datum,
	IndexRanges []*ranger.Range, keyOff2IdxOff []int) (Executor, error) {
	switch v := builder.Plan.(type) {
	case *plannercore.PhysicalTableReader:
		return builder.buildTableReaderForIndexJoin(ctx, v, datums)
	case *plannercore.PhysicalIndexReader:
		return builder.buildIndexReaderForIndexJoin(ctx, v, datums, IndexRanges, keyOff2IdxOff)
	case *plannercore.PhysicalIndexLookUpReader:
		return builder.buildIndexLookUpReaderForIndexJoin(ctx, v, datums, IndexRanges, keyOff2IdxOff)
	case *plannercore.PhysicalUnionScan:
		return builder.buildUnionScanForIndexJoin(ctx, v, datums, IndexRanges, keyOff2IdxOff)
	}
	return nil, errors.New("Wrong plan type for dataReaderBuilder")
}

func (builder *dataReaderBuilder) buildUnionScanForIndexJoin(ctx context.Context, v *plannercore.PhysicalUnionScan,
	values [][]types.Datum, indexRanges []*ranger.Range, keyOff2IdxOff []int) (Executor, error) {
	childBuilder := &dataReaderBuilder{Plan: v.Children()[0], executorBuilder: builder.executorBuilder}
	reader, err := childBuilder.buildExecutorForIndexJoin(ctx, values, indexRanges, keyOff2IdxOff)
	if err != nil {
		return nil, err
	}
	e, err := builder.buildUnionScanFromReader(reader, v)
	if err != nil {
		return nil, err
	}
	us := e.(*UnionScanExec)
	us.snapshotChunkBuffer = us.newFirstChunk()
	return us, nil
}

func (builder *dataReaderBuilder) buildTableReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalTableReader, datums [][]types.Datum) (Executor, error) {
	e, err := buildNoRangeTableReader(builder.executorBuilder, v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	handles := make([]int64, 0, len(datums))
	for _, datum := range datums {
		handles = append(handles, datum[0].GetInt64())
	}
	return builder.buildTableReaderFromHandles(ctx, e, handles)
}

func (builder *dataReaderBuilder) buildTableReaderFromHandles(ctx context.Context, e *TableReaderExecutor, handles []int64) (Executor, error) {
	sort.Sort(sortutil.Int64Slice(handles))
	var b distsql.RequestBuilder
	kvReq, err := b.SetTableHandles(e.physicalTableID, handles).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.resultHandler = &tableResultHandler{}
	result, err := builder.SelectResult(ctx, builder.ctx, kvReq, e.retTypes(), e.feedback)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.Fetch(ctx)
	e.resultHandler.open(nil, result)
	return e, nil
}

func (builder *dataReaderBuilder) buildIndexReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalIndexReader,
	values [][]types.Datum, indexRanges []*ranger.Range, keyOff2IdxOff []int) (Executor, error) {
	e, err := buildNoRangeIndexReader(builder.executorBuilder, v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	kvRanges, err := buildKvRangesForIndexJoin(e.ctx.GetSessionVars().StmtCtx, e.physicalTableID, e.index.ID, values, indexRanges, keyOff2IdxOff)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = e.open(ctx, kvRanges)
	return e, errors.Trace(err)
}

func (builder *dataReaderBuilder) buildIndexLookUpReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalIndexLookUpReader,
	values [][]types.Datum, indexRanges []*ranger.Range, keyOff2IdxOff []int) (Executor, error) {
	e, err := buildNoRangeIndexLookUpReader(builder.executorBuilder, v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.kvRanges, err = buildKvRangesForIndexJoin(e.ctx.GetSessionVars().StmtCtx, e.physicalTableID, e.index.ID, values, indexRanges, keyOff2IdxOff)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = e.open(ctx)
	return e, errors.Trace(err)
}

// buildKvRangesForIndexJoin builds kv ranges for index join when the inner plan is index scan plan.
func buildKvRangesForIndexJoin(sc *stmtctx.StatementContext, tableID, indexID int64, keyDatums [][]types.Datum, indexRanges []*ranger.Range, keyOff2IdxOff []int) ([]kv.KeyRange, error) {
	kvRanges := make([]kv.KeyRange, 0, len(indexRanges)*len(keyDatums))
	for _, val := range keyDatums {
		for _, ran := range indexRanges {
			for keyOff, idxOff := range keyOff2IdxOff {
				ran.LowVal[idxOff] = val[keyOff]
				ran.HighVal[idxOff] = val[keyOff]
			}
		}

		tmpKvRanges, err := distsql.IndexRangesToKVRanges(sc, tableID, indexID, indexRanges, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		kvRanges = append(kvRanges, tmpKvRanges...)
	}
	// kvRanges don't overlap each other. So compare StartKey is enough.
	sort.Slice(kvRanges, func(i, j int) bool {
		return bytes.Compare(kvRanges[i].StartKey, kvRanges[j].StartKey) < 0
	})
	return kvRanges, nil
}
