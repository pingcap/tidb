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

	"github.com/cznic/sortutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
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

func (b *executorBuilder) build(p plan.Plan) Executor {
	switch v := p.(type) {
	case nil:
		return nil
	case *plan.CheckTable:
		return b.buildCheckTable(v)
	case *plan.CheckIndex:
		return b.buildCheckIndex(v)
	case *plan.RecoverIndex:
		return b.buildRecoverIndex(v)
	case *plan.CleanupIndex:
		return b.buildCleanupIndex(v)
	case *plan.CheckIndexRange:
		return b.buildCheckIndexRange(v)
	case *plan.ChecksumTable:
		return b.buildChecksumTable(v)
	case *plan.DDL:
		return b.buildDDL(v)
	case *plan.Deallocate:
		return b.buildDeallocate(v)
	case *plan.Delete:
		return b.buildDelete(v)
	case *plan.Execute:
		return b.buildExecute(v)
	case *plan.Explain:
		return b.buildExplain(v)
	case *plan.PointGetPlan:
		return b.buildPointGet(v)
	case *plan.Insert:
		return b.buildInsert(v)
	case *plan.LoadData:
		return b.buildLoadData(v)
	case *plan.LoadStats:
		return b.buildLoadStats(v)
	case *plan.PhysicalLimit:
		return b.buildLimit(v)
	case *plan.Prepare:
		return b.buildPrepare(v)
	case *plan.PhysicalLock:
		return b.buildSelectLock(v)
	case *plan.CancelDDLJobs:
		return b.buildCancelDDLJobs(v)
	case *plan.ShowDDL:
		return b.buildShowDDL(v)
	case *plan.ShowDDLJobs:
		return b.buildShowDDLJobs(v)
	case *plan.ShowDDLJobQueries:
		return b.buildShowDDLJobQueries(v)
	case *plan.Show:
		return b.buildShow(v)
	case *plan.Simple:
		return b.buildSimple(v)
	case *plan.Set:
		return b.buildSet(v)
	case *plan.PhysicalSort:
		return b.buildSort(v)
	case *plan.PhysicalTopN:
		return b.buildTopN(v)
	case *plan.PhysicalUnionAll:
		return b.buildUnionAll(v)
	case *plan.Update:
		return b.buildUpdate(v)
	case *plan.PhysicalUnionScan:
		return b.buildUnionScanExec(v)
	case *plan.PhysicalHashJoin:
		return b.buildHashJoin(v)
	case *plan.PhysicalMergeJoin:
		return b.buildMergeJoin(v)
	case *plan.PhysicalIndexJoin:
		return b.buildIndexLookUpJoin(v)
	case *plan.PhysicalSelection:
		return b.buildSelection(v)
	case *plan.PhysicalHashAgg:
		return b.buildHashAgg(v)
	case *plan.PhysicalStreamAgg:
		return b.buildStreamAgg(v)
	case *plan.PhysicalProjection:
		return b.buildProjection(v)
	case *plan.PhysicalMemTable:
		return b.buildMemTable(v)
	case *plan.PhysicalTableDual:
		return b.buildTableDual(v)
	case *plan.PhysicalApply:
		return b.buildApply(v)
	case *plan.PhysicalMaxOneRow:
		return b.buildMaxOneRow(v)
	case *plan.Analyze:
		return b.buildAnalyze(v)
	case *plan.PhysicalTableReader:
		return b.buildTableReader(v)
	case *plan.PhysicalIndexReader:
		return b.buildIndexReader(v)
	case *plan.PhysicalIndexLookUpReader:
		return b.buildIndexLookUpReader(v)
	default:
		b.err = ErrUnknownPlan.Gen("Unknown Plan %T", p)
		return nil
	}
}

func (b *executorBuilder) buildCancelDDLJobs(v *plan.CancelDDLJobs) Executor {
	e := &CancelDDLJobsExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		jobIDs:       v.JobIDs,
	}
	e.errs, b.err = admin.CancelJobs(e.ctx.Txn(), e.jobIDs)
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	return e
}

func (b *executorBuilder) buildShowDDL(v *plan.ShowDDL) Executor {
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

	ddlInfo, err := admin.GetDDLInfo(e.ctx.Txn())
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	e.ddlInfo = ddlInfo
	e.selfID = ownerManager.ID()
	return e
}

func (b *executorBuilder) buildShowDDLJobs(v *plan.ShowDDLJobs) Executor {
	e := &ShowDDLJobsExec{
		jobNumber:    v.JobNumber,
		is:           b.is,
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
	}
	return e
}

func (b *executorBuilder) buildShowDDLJobQueries(v *plan.ShowDDLJobQueries) Executor {
	e := &ShowDDLJobQueriesExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		jobIDs:       v.JobIDs,
	}
	return e
}

func (b *executorBuilder) buildCheckIndex(v *plan.CheckIndex) Executor {
	readerExec, err := buildNoRangeIndexLookUpReader(b, v.IndexLookUpReader)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	readerExec.ranges = ranger.FullRange()
	readerExec.isCheckOp = true

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

func (b *executorBuilder) buildCheckTable(v *plan.CheckTable) Executor {
	e := &CheckTableExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		tables:       v.Tables,
		is:           b.is,
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

func (b *executorBuilder) buildRecoverIndex(v *plan.RecoverIndex) Executor {
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

func (b *executorBuilder) buildCleanupIndex(v *plan.CleanupIndex) Executor {
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

func (b *executorBuilder) buildCheckIndexRange(v *plan.CheckIndexRange) Executor {
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

func (b *executorBuilder) buildChecksumTable(v *plan.ChecksumTable) Executor {
	e := &ChecksumTableExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		tables:       make(map[int64]*checksumContext),
		done:         false,
	}
	startTs := b.getStartTS()
	for _, t := range v.Tables {
		e.tables[t.TableInfo.ID] = newChecksumContext(t.DBInfo, t.TableInfo, startTs)
	}
	return e
}

func (b *executorBuilder) buildDeallocate(v *plan.Deallocate) Executor {
	e := &DeallocateExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ExplainID()),
		Name:         v.Name,
	}
	return e
}

func (b *executorBuilder) buildSelectLock(v *plan.PhysicalLock) Executor {
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

func (b *executorBuilder) buildLimit(v *plan.PhysicalLimit) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	e := &LimitExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
		begin:        v.Offset,
		end:          v.Offset + v.Count,
	}
	return e
}

func (b *executorBuilder) buildPrepare(v *plan.Prepare) Executor {
	e := &PrepareExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		is:           b.is,
		name:         v.Name,
		sqlText:      v.SQLText,
	}
	return e
}

func (b *executorBuilder) buildExecute(v *plan.Execute) Executor {
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

func (b *executorBuilder) buildShow(v *plan.Show) Executor {
	e := &ShowExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		Tp:           v.Tp,
		DBName:       model.NewCIStr(v.DBName),
		Table:        v.Table,
		Column:       v.Column,
		User:         v.User,
		Flag:         v.Flag,
		Full:         v.Full,
		GlobalScope:  v.GlobalScope,
		is:           b.is,
	}
	if e.Tp == ast.ShowGrants && e.User == nil {
		e.User = e.ctx.GetSessionVars().User
	}
	if len(v.Conditions) == 0 {
		return e
	}
	sel := &SelectionExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), e),
		filters:      v.Conditions,
	}
	return sel
}

func (b *executorBuilder) buildSimple(v *plan.Simple) Executor {
	switch s := v.Statement.(type) {
	case *ast.GrantStmt:
		return b.buildGrant(s)
	case *ast.RevokeStmt:
		return b.buildRevoke(s)
	}
	e := &SimpleExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		Statement:    v.Statement,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildSet(v *plan.Set) Executor {
	e := &SetExecutor{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		vars:         v.VarAssigns,
	}
	return e
}

func (b *executorBuilder) buildInsert(v *plan.Insert) Executor {
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

	ivs := &InsertValues{
		baseExecutor:          baseExec,
		Table:                 v.Table,
		Columns:               v.Columns,
		Lists:                 v.Lists,
		SetList:               v.SetList,
		GenColumns:            v.GenCols.Columns,
		GenExprs:              v.GenCols.Exprs,
		needFillDefaultValues: v.NeedFillDefaultValue,
		SelectExec:            selectExec,
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

func (b *executorBuilder) buildLoadData(v *plan.LoadData) Executor {
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
	tableCols := tbl.Cols()
	columns, err := insertVal.getColumns(tableCols)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	loadDataExec := &LoadDataExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ExplainID()),
		IsLocal:      v.IsLocal,
		loadDataInfo: &LoadDataInfo{
			row:          make([]types.Datum, len(columns)),
			InsertValues: insertVal,
			Path:         v.Path,
			Table:        tbl,
			FieldsInfo:   v.FieldsInfo,
			LinesInfo:    v.LinesInfo,
			Ctx:          b.ctx,
			columns:      columns,
		},
	}

	return loadDataExec
}

func (b *executorBuilder) buildLoadStats(v *plan.LoadStats) Executor {
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
		ctx:        b.ctx,
		Privs:      revoke.Privs,
		ObjectType: revoke.ObjectType,
		Level:      revoke.Level,
		Users:      revoke.Users,
		is:         b.is,
	}
	return e
}

func (b *executorBuilder) buildDDL(v *plan.DDL) Executor {
	e := &DDLExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		stmt:         v.Statement,
		is:           b.is,
	}
	return e
}

// buildExplain builds a explain executor. `e.rows` collects final result to `ExplainExec`.
func (b *executorBuilder) buildExplain(v *plan.Explain) Executor {
	e := &ExplainExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
	}
	e.rows = make([][]string, 0, len(v.Rows))
	for _, row := range v.Rows {
		e.rows = append(e.rows, row)
	}
	return e
}

func (b *executorBuilder) buildUnionScanExec(v *plan.PhysicalUnionScan) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	us := &UnionScanExec{baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), src)}
	// Get the handle column index of the below plan.
	// We can guarantee that there must be only one col in the map.
	for _, cols := range v.Children()[0].Schema().TblID2Handle {
		us.belowHandleIndex = cols[0].Index
	}
	switch x := src.(type) {
	case *TableReaderExecutor:
		us.desc = x.desc
		us.dirty = GetDirtyDB(b.ctx).GetDirtyTable(x.table.Meta().ID)
		us.conditions = v.Conditions
		us.columns = x.columns
		b.err = us.buildAndSortAddedRows()
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
		b.err = us.buildAndSortAddedRows()
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
		b.err = us.buildAndSortAddedRows()
	default:
		// The mem table will not be written by sql directly, so we can omit the union scan to avoid err reporting.
		return src
	}
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	return us
}

// buildMergeJoin builds MergeJoinExec executor.
func (b *executorBuilder) buildMergeJoin(v *plan.PhysicalMergeJoin) Executor {
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
		if v.JoinType == plan.RightOuterJoin {
			defaultValues = make([]types.Datum, leftExec.Schema().Len())
		} else {
			defaultValues = make([]types.Datum, rightExec.Schema().Len())
		}
	}

	e := &MergeJoinExec{
		stmtCtx:      b.ctx.GetSessionVars().StmtCtx,
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), leftExec, rightExec),
		joiner: newJoiner(b.ctx, v.JoinType, v.JoinType == plan.RightOuterJoin,
			defaultValues, v.OtherConditions,
			leftExec.retTypes(), rightExec.retTypes()),
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

	if v.JoinType == plan.RightOuterJoin {
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

func (b *executorBuilder) buildHashJoin(v *plan.PhysicalHashJoin) Executor {
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
	projFromID := fmt.Sprintf("%s_%d", plan.TypeProj, id)

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
		evaluatorSuit: expression.NewEvaluatorSuit(projExprs),
	}
}

func (b *executorBuilder) buildHashAgg(v *plan.PhysicalHashAgg) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	src = b.buildProjBelowAgg(v.AggFuncs, v.GroupByItems, src)
	sessionVars := b.ctx.GetSessionVars()
	e := &HashAggExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), src),
		sc:           sessionVars.StmtCtx,
		AggFuncs:     make([]aggregation.Aggregation, 0, len(v.AggFuncs)),
		GroupByItems: v.GroupByItems,
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
	for i, aggDesc := range v.AggFuncs {
		if !e.isUnparallelExec {
			if aggDesc.Mode == aggregation.CompleteMode {
				aggDesc.Mode = aggregation.Partial1Mode
			} else {
				aggDesc.Mode = aggregation.Partial2Mode
			}
		}
		aggFunc := aggDesc.GetAggFunc(b.ctx)
		e.AggFuncs = append(e.AggFuncs, aggFunc)
		if e.defaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.defaultVal.AppendDatum(i, &value)
		}
	}

	metrics.ExecutorCounter.WithLabelValues("HashAggExec").Inc()
	return e
}

func (b *executorBuilder) buildStreamAgg(v *plan.PhysicalStreamAgg) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	src = b.buildProjBelowAgg(v.AggFuncs, v.GroupByItems, src)
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

func (b *executorBuilder) buildSelection(v *plan.PhysicalSelection) Executor {
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

func (b *executorBuilder) buildProjection(v *plan.PhysicalProjection) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	e := &ProjectionExec{
		baseExecutor:     newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
		numWorkers:       b.ctx.GetSessionVars().ProjectionConcurrency,
		evaluatorSuit:    expression.NewEvaluatorSuit(v.Exprs),
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

func (b *executorBuilder) buildTableDual(v *plan.PhysicalTableDual) Executor {
	if v.RowCount != 0 && v.RowCount != 1 {
		b.err = errors.Errorf("buildTableDual failed, invalid row count for dual table: %v", v.RowCount)
		return nil
	}
	e := &TableDualExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		numDualRows:  v.RowCount,
	}
	// Init the startTS for later use.
	b.getStartTS()
	return e
}

func (b *executorBuilder) getStartTS() uint64 {
	if b.startTS != 0 {
		// Return the cached value.
		return b.startTS
	}

	startTS := b.ctx.GetSessionVars().SnapshotTS
	if startTS == 0 && b.ctx.Txn() != nil {
		startTS = b.ctx.Txn().StartTS()
	}
	b.startTS = startTS
	return startTS
}

func (b *executorBuilder) buildMemTable(v *plan.PhysicalMemTable) Executor {
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

func (b *executorBuilder) buildSort(v *plan.PhysicalSort) Executor {
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

func (b *executorBuilder) buildTopN(v *plan.PhysicalTopN) Executor {
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
		limit:    &plan.PhysicalLimit{Count: v.Count, Offset: v.Offset},
	}
}

func (b *executorBuilder) buildApply(apply *plan.PhysicalApply) *NestedLoopApplyExec {
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
		outer:        v.JoinType != plan.InnerJoin,
		joiner:       tupleJoiner,
		outerSchema:  apply.OuterSchema,
	}
	metrics.ExecutorCounter.WithLabelValues("NestedLoopApplyExec").Inc()
	return e
}

func (b *executorBuilder) buildMaxOneRow(v *plan.PhysicalMaxOneRow) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	e := &MaxOneRowExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
	}
	return e
}

func (b *executorBuilder) buildUnionAll(v *plan.PhysicalUnionAll) Executor {
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

func (b *executorBuilder) buildUpdate(v *plan.Update) Executor {
	tblID2table := make(map[int64]table.Table)
	for id := range v.SelectPlan.Schema().TblID2Handle {
		tblID2table[id], _ = b.is.TableByID(id)
	}
	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	columns2Handle := buildColumns2Handle(v.Schema(), tblID2table)
	updateExec := &UpdateExec{
		baseExecutor:   newBaseExecutor(b.ctx, nil, v.ExplainID(), selExec),
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

func (b *executorBuilder) buildDelete(v *plan.Delete) Executor {
	tblID2table := make(map[int64]table.Table)
	for id := range v.SelectPlan.Schema().TblID2Handle {
		tblID2table[id], _ = b.is.TableByID(id)
	}
	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	deleteExec := &DeleteExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ExplainID(), selExec),
		SelectExec:   selExec,
		Tables:       v.Tables,
		IsMultiTable: v.IsMultiTable,
		tblID2Table:  tblID2table,
	}
	return deleteExec
}

func (b *executorBuilder) buildAnalyzeIndexPushdown(task plan.AnalyzeIndexTask) *AnalyzeIndexExec {
	_, offset := zone(b.ctx)
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
	}
	e.analyzePB.IdxReq = &tipb.AnalyzeIndexReq{
		BucketSize: maxBucketSize,
		NumColumns: int32(len(task.IndexInfo.Columns)),
	}
	depth := int32(defaultCMSketchDepth)
	width := int32(defaultCMSketchWidth)
	e.analyzePB.IdxReq.CmsketchDepth = &depth
	e.analyzePB.IdxReq.CmsketchWidth = &width
	return e
}

func (b *executorBuilder) buildAnalyzeColumnsPushdown(task plan.AnalyzeColumnsTask) *AnalyzeColumnsExec {
	cols := task.ColsInfo
	keepOrder := false
	if task.PKInfo != nil {
		keepOrder = true
		cols = append([]*model.ColumnInfo{task.PKInfo}, cols...)
	}

	_, offset := zone(b.ctx)
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
	}
	depth := int32(defaultCMSketchDepth)
	width := int32(defaultCMSketchWidth)
	e.analyzePB.ColReq = &tipb.AnalyzeColumnsReq{
		BucketSize:    maxBucketSize,
		SampleSize:    maxRegionSampleSize,
		SketchSize:    maxSketchSize,
		ColumnsInfo:   model.ColumnsToProto(cols, task.PKInfo != nil),
		CmsketchDepth: &depth,
		CmsketchWidth: &width,
	}
	b.err = errors.Trace(plan.SetPBColumnsDefaultValue(b.ctx, e.analyzePB.ColReq.ColumnsInfo, cols))
	return e
}

func (b *executorBuilder) buildAnalyze(v *plan.Analyze) Executor {
	e := &AnalyzeExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		tasks:        make([]*analyzeTask, 0, len(v.ColTasks)+len(v.IdxTasks)),
	}
	for _, task := range v.ColTasks {
		e.tasks = append(e.tasks, &analyzeTask{
			taskType: colTask,
			colExec:  b.buildAnalyzeColumnsPushdown(task),
		})
		if b.err != nil {
			b.err = errors.Trace(b.err)
			return nil
		}
	}
	for _, task := range v.IdxTasks {
		e.tasks = append(e.tasks, &analyzeTask{
			taskType: idxTask,
			idxExec:  b.buildAnalyzeIndexPushdown(task),
		})
		if b.err != nil {
			b.err = errors.Trace(b.err)
			return nil
		}
	}
	return e
}

func constructDistExec(sctx sessionctx.Context, plans []plan.PhysicalPlan) ([]*tipb.Executor, bool, error) {
	streaming := true
	executors := make([]*tipb.Executor, 0, len(plans))
	for _, p := range plans {
		execPB, err := p.ToPB(sctx)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if !plan.SupportStreaming(p) {
			streaming = false
		}
		executors = append(executors, execPB)
	}
	return executors, streaming, nil
}

func (b *executorBuilder) constructDAGReq(plans []plan.PhysicalPlan) (dagReq *tipb.DAGRequest, streaming bool, err error) {
	dagReq = &tipb.DAGRequest{}
	dagReq.StartTs = b.getStartTS()
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = zone(b.ctx)
	sc := b.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = statementContextToFlags(sc)
	dagReq.Executors, streaming, err = constructDistExec(b.ctx, plans)
	return dagReq, streaming, errors.Trace(err)
}

func (b *executorBuilder) corColInDistPlan(plans []plan.PhysicalPlan) bool {
	for _, p := range plans {
		x, ok := p.(*plan.PhysicalSelection)
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
func (b *executorBuilder) corColInAccess(p plan.PhysicalPlan) bool {
	var access []expression.Expression
	switch x := p.(type) {
	case *plan.PhysicalTableScan:
		access = x.AccessCondition
	case *plan.PhysicalIndexScan:
		access = x.AccessCondition
	}
	for _, cond := range access {
		if len(expression.ExtractCorColumns(cond)) > 0 {
			return true
		}
	}
	return false
}

func (b *executorBuilder) buildIndexLookUpJoin(v *plan.PhysicalIndexJoin) Executor {
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
			readerBuilder: &dataReaderBuilder{innerPlan, b},
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
	e.joinResult = e.newChunk()
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

func buildNoRangeTableReader(b *executorBuilder, v *plan.PhysicalTableReader) (*TableReaderExecutor, error) {
	dagReq, streaming, err := b.constructDAGReq(v.TablePlans)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ts := v.TablePlans[0].(*plan.PhysicalTableScan)
	table, _ := b.is.TableByID(ts.Table.ID)
	e := &TableReaderExecutor{
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		dagPB:           dagReq,
		physicalTableID: ts.Table.ID,
		table:           table,
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
		e.feedback = statistics.NewQueryFeedback(ts.Table.ID, ts.Hist, int64(ts.StatsCount()), ts.Desc)
	}
	collect := e.feedback.CollectFeedback(len(ts.Ranges))
	e.dagPB.CollectRangeCounts = &collect

	for i := range v.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	return e, nil
}

// buildTableReader builds a table reader executor. It first build a no range table reader,
// and then update it ranges from table scan plan.
func (b *executorBuilder) buildTableReader(v *plan.PhysicalTableReader) *TableReaderExecutor {
	ret, err := buildNoRangeTableReader(b, v)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}

	ts := v.TablePlans[0].(*plan.PhysicalTableScan)
	ret.ranges = ts.Ranges
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)
	return ret
}

func buildNoRangeIndexReader(b *executorBuilder, v *plan.PhysicalIndexReader) (*IndexReaderExecutor, error) {
	dagReq, streaming, err := b.constructDAGReq(v.IndexPlans)
	if err != nil {
		return nil, errors.Trace(err)
	}
	is := v.IndexPlans[0].(*plan.PhysicalIndexScan)
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
		e.feedback = statistics.NewQueryFeedback(is.Table.ID, is.Hist, int64(is.StatsCount()), is.Desc)
	}
	collect := e.feedback.CollectFeedback(len(is.Ranges))
	e.dagPB.CollectRangeCounts = &collect

	for _, col := range v.OutputColumns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(col.Index))
	}

	return e, nil
}

func (b *executorBuilder) buildIndexReader(v *plan.PhysicalIndexReader) *IndexReaderExecutor {
	ret, err := buildNoRangeIndexReader(b, v)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}

	is := v.IndexPlans[0].(*plan.PhysicalIndexScan)
	ret.ranges = is.Ranges
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.IndexIDs = append(sctx.IndexIDs, is.Index.ID)
	return ret
}

func buildNoRangeIndexLookUpReader(b *executorBuilder, v *plan.PhysicalIndexLookUpReader) (*IndexLookUpExecutor, error) {
	indexReq, indexStreaming, err := b.constructDAGReq(v.IndexPlans)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableReq, tableStreaming, err := b.constructDAGReq(v.TablePlans)
	if err != nil {
		return nil, errors.Trace(err)
	}
	is := v.IndexPlans[0].(*plan.PhysicalIndexScan)
	indexReq.OutputOffsets = []uint32{uint32(len(is.Index.Columns))}
	table, _ := b.is.TableByID(is.Table.ID)

	for i := 0; i < v.Schema().Len(); i++ {
		tableReq.OutputOffsets = append(tableReq.OutputOffsets, uint32(i))
	}

	ts := v.TablePlans[0].(*plan.PhysicalTableScan)

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
	}
	if isPartition, physicalTableID := ts.IsPartition(); isPartition {
		e.physicalTableID = physicalTableID
	}

	if containsLimit(indexReq.Executors) {
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, is.Desc)
	} else {
		e.feedback = statistics.NewQueryFeedback(is.Table.ID, is.Hist, int64(is.StatsCount()), is.Desc)
	}
	// do not collect the feedback for table request.
	collectTable := false
	e.tableRequest.CollectRangeCounts = &collectTable
	collectIndex := e.feedback.CollectFeedback(len(is.Ranges))
	e.dagPB.CollectRangeCounts = &collectIndex
	if cols, ok := v.Schema().TblID2Handle[is.Table.ID]; ok {
		e.handleIdx = cols[0].Index
	}
	return e, nil
}

func (b *executorBuilder) buildIndexLookUpReader(v *plan.PhysicalIndexLookUpReader) *IndexLookUpExecutor {
	ret, err := buildNoRangeIndexLookUpReader(b, v)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}

	is := v.IndexPlans[0].(*plan.PhysicalIndexScan)
	ts := v.TablePlans[0].(*plan.PhysicalTableScan)

	ret.ranges = is.Ranges
	metrics.ExecutorCounter.WithLabelValues("IndexLookUpExecutor").Inc()
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.IndexIDs = append(sctx.IndexIDs, is.Index.ID)
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)
	return ret
}

// dataReaderBuilder build an executor.
// The executor can be used to read data in the ranges which are constructed by datums.
// Differences from executorBuilder:
// 1. dataReaderBuilder calculate data range from argument, rather than plan.
// 2. the result executor is already opened.
type dataReaderBuilder struct {
	plan.Plan
	*executorBuilder
}

func (builder *dataReaderBuilder) buildExecutorForIndexJoin(ctx context.Context, datums [][]types.Datum,
	IndexRanges []*ranger.Range, keyOff2IdxOff []int) (Executor, error) {
	switch v := builder.Plan.(type) {
	case *plan.PhysicalTableReader:
		return builder.buildTableReaderForIndexJoin(ctx, v, datums)
	case *plan.PhysicalIndexReader:
		return builder.buildIndexReaderForIndexJoin(ctx, v, datums, IndexRanges, keyOff2IdxOff)
	case *plan.PhysicalIndexLookUpReader:
		return builder.buildIndexLookUpReaderForIndexJoin(ctx, v, datums, IndexRanges, keyOff2IdxOff)
	}
	return nil, errors.New("Wrong plan type for dataReaderBuilder")
}

func (builder *dataReaderBuilder) buildTableReaderForIndexJoin(ctx context.Context, v *plan.PhysicalTableReader, datums [][]types.Datum) (Executor, error) {
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
	result, err := distsql.Select(ctx, builder.ctx, kvReq, e.retTypes(), e.feedback)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.Fetch(ctx)
	e.resultHandler.open(nil, result)
	return e, nil
}

func (builder *dataReaderBuilder) buildIndexReaderForIndexJoin(ctx context.Context, v *plan.PhysicalIndexReader,
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

func (builder *dataReaderBuilder) buildIndexLookUpReaderForIndexJoin(ctx context.Context, v *plan.PhysicalIndexLookUpReader,
	values [][]types.Datum, indexRanges []*ranger.Range, keyOff2IdxOff []int) (Executor, error) {
	e, err := buildNoRangeIndexLookUpReader(builder.executorBuilder, v)
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
