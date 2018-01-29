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
	"math"
	"sort"
	"sync"
	"time"

	"github.com/cznic/sortutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

// executorBuilder builds an Executor from a Plan.
// The InfoSchema must not change during execution.
type executorBuilder struct {
	ctx      context.Context
	is       infoschema.InfoSchema
	priority int
	startTS  uint64 // cached when the first time getStartTS() is called
	// err is set when there is error happened during Executor building process.
	err error
}

func newExecutorBuilder(ctx context.Context, is infoschema.InfoSchema, priority int) *executorBuilder {
	return &executorBuilder{
		ctx:      ctx,
		is:       is,
		priority: priority,
	}
}

func (b *executorBuilder) build(p plan.Plan) Executor {
	switch v := p.(type) {
	case nil:
		return nil
	case *plan.CheckTable:
		return b.buildCheckTable(v)
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
	case *plan.Insert:
		return b.buildInsert(v)
	case *plan.LoadData:
		return b.buildLoadData(v)
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
	case *plan.PhysicalExists:
		return b.buildExists(v)
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
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
		jobIDs:       v.JobIDs,
	}
	e.errs, b.err = admin.CancelJobs(e.ctx.Txn(), e.jobIDs)
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildShowDDL(v *plan.ShowDDL) Executor {
	// We get DDLInfo here because for Executors that returns result set,
	// next will be called after transaction has been committed.
	// We need the transaction to get DDLInfo.
	e := &ShowDDLExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
	}

	var err error
	ownerManager := domain.GetDomain(e.ctx).DDL().OwnerManager()
	ctx, cancel := goctx.WithTimeout(goctx.Background(), 3*time.Second)
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
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildShowDDLJobs(v *plan.ShowDDLJobs) Executor {
	e := &ShowDDLJobsExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildCheckTable(v *plan.CheckTable) Executor {
	e := &CheckTableExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
		tables:       v.Tables,
		is:           b.is,
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildDeallocate(v *plan.Deallocate) Executor {
	e := &DeallocateExec{
		baseExecutor: newBaseExecutor(nil, b.ctx),
		Name:         v.Name,
	}
	e.supportChk = true
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
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, src),
		Lock:         v.Lock,
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildLimit(v *plan.PhysicalLimit) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	e := &LimitExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, childExec),
		begin:        v.Offset,
		end:          v.Offset + v.Count,
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildPrepare(v *plan.Prepare) Executor {
	e := &PrepareExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
		is:           b.is,
		name:         v.Name,
		sqlText:      v.SQLText,
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildExecute(v *plan.Execute) Executor {
	e := &ExecuteExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
		is:           b.is,
		name:         v.Name,
		usingVars:    v.UsingVars,
		id:           v.ExecID,
		stmt:         v.Stmt,
		plan:         v.Plan,
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildShow(v *plan.Show) Executor {
	e := &ShowExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
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
	e.supportChk = true
	if e.Tp == ast.ShowGrants && e.User == nil {
		e.User = e.ctx.GetSessionVars().User
	}
	if len(v.Conditions) == 0 {
		return e
	}
	sel := &SelectionExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, e),
		filters:      v.Conditions,
	}
	sel.supportChk = true
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
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
		Statement:    v.Statement,
		is:           b.is,
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildSet(v *plan.Set) Executor {
	e := &SetExecutor{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
		vars:         v.VarAssigns,
	}
	e.supportChk = true
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
		baseExec = newBaseExecutor(nil, b.ctx, selectExec)
	} else {
		baseExec = newBaseExecutor(nil, b.ctx)
	}

	ivs := &InsertValues{
		baseExecutor:          baseExec,
		Columns:               v.Columns,
		Lists:                 v.Lists,
		Setlist:               v.Setlist,
		GenColumns:            v.GenCols.Columns,
		GenExprs:              v.GenCols.Exprs,
		needFillDefaultValues: v.NeedFillDefaultValue,
		SelectExec:            selectExec,
	}

	ivs.Table = v.Table
	if v.IsReplace {
		return b.buildReplace(ivs)
	}
	insert := &InsertExec{
		InsertValues: ivs,
		OnDuplicate:  append(v.OnDuplicate, v.GenCols.OnDuplicates...),
		Priority:     v.Priority,
		IgnoreErr:    v.IgnoreErr,
	}
	insert.supportChk = true
	return insert
}

func (b *executorBuilder) buildLoadData(v *plan.LoadData) Executor {
	tbl, ok := b.is.TableByID(v.Table.TableInfo.ID)
	if !ok {
		b.err = errors.Errorf("Can not get table %d", v.Table.TableInfo.ID)
		return nil
	}
	insertVal := &InsertValues{
		baseExecutor: newBaseExecutor(nil, b.ctx),
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
	loadDataExec := &LoadData{
		baseExecutor: newBaseExecutor(nil, b.ctx),
		IsLocal:      v.IsLocal,
		loadDataInfo: &LoadDataInfo{
			row:        make([]types.Datum, len(columns)),
			insertVal:  insertVal,
			Path:       v.Path,
			Table:      tbl,
			FieldsInfo: v.FieldsInfo,
			LinesInfo:  v.LinesInfo,
			Ctx:        b.ctx,
			columns:    columns,
		},
	}

	loadDataExec.supportChk = true
	return loadDataExec
}

func (b *executorBuilder) buildReplace(vals *InsertValues) Executor {
	replaceExec := &ReplaceExec{
		InsertValues: vals,
	}
	replaceExec.supportChk = true
	return replaceExec
}

func (b *executorBuilder) buildGrant(grant *ast.GrantStmt) Executor {
	e := &GrantExec{
		baseExecutor: newBaseExecutor(nil, b.ctx),
		Privs:        grant.Privs,
		ObjectType:   grant.ObjectType,
		Level:        grant.Level,
		Users:        grant.Users,
		WithGrant:    grant.WithGrant,
		is:           b.is,
	}
	e.supportChk = true
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
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildDDL(v *plan.DDL) Executor {
	e := &DDLExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
		stmt:         v.Statement,
		is:           b.is,
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildExplain(v *plan.Explain) Executor {
	e := &ExplainExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
	}
	e.rows = make([][]string, 0, len(v.Rows))
	for _, row := range v.Rows {
		e.rows = append(e.rows, row)
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildUnionScanExec(v *plan.PhysicalUnionScan) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	us := &UnionScanExec{baseExecutor: newBaseExecutor(v.Schema(), b.ctx, src)}
	// Get the handle column index of the below plan.
	// We can guarantee that there must be only one col in the map.
	for _, cols := range v.Children()[0].Schema().TblID2Handle {
		us.belowHandleIndex = cols[0].Index
	}
	switch x := src.(type) {
	case *TableReaderExecutor:
		us.desc = x.desc
		us.dirty = getDirtyDB(b.ctx).getDirtyTable(x.table.Meta().ID)
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
		us.dirty = getDirtyDB(b.ctx).getDirtyTable(x.table.Meta().ID)
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
		us.dirty = getDirtyDB(b.ctx).getDirtyTable(x.table.Meta().ID)
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
	us.supportChk = true
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

	leftKeys := make([]*expression.Column, 0, len(v.EqualConditions))
	rightKeys := make([]*expression.Column, 0, len(v.EqualConditions))
	for _, eqCond := range v.EqualConditions {
		if len(eqCond.GetArgs()) != 2 {
			b.err = errors.Annotate(ErrBuildExecutor, "invalid join key for equal condition")
			return nil
		}
		leftKey, ok := eqCond.GetArgs()[0].(*expression.Column)
		if !ok {
			b.err = errors.Annotate(ErrBuildExecutor, "left side of join key must be column for merge join")
			return nil
		}
		rightKey, ok := eqCond.GetArgs()[1].(*expression.Column)
		if !ok {
			b.err = errors.Annotate(ErrBuildExecutor, "right side of join key must be column for merge join")
			return nil
		}
		leftKeys = append(leftKeys, leftKey)
		rightKeys = append(rightKeys, rightKey)
	}

	lhsIter := &readerIterator{
		ctx:      b.ctx,
		reader:   leftExec,
		filter:   v.LeftConditions,
		joinKeys: leftKeys,
	}

	rhsIter := &readerIterator{
		ctx:      b.ctx,
		reader:   rightExec,
		filter:   v.RightConditions,
		joinKeys: rightKeys,
	}

	defaultValues := v.DefaultValues
	if defaultValues == nil {
		if v.JoinType == plan.RightOuterJoin {
			defaultValues = make([]types.Datum, leftExec.Schema().Len())
		} else {
			defaultValues = make([]types.Datum, rightExec.Schema().Len())
		}
	}
	lhsColTypes := leftExec.retTypes()
	rhsColTypes := rightExec.retTypes()
	e := &MergeJoinExec{
		baseExecutor:    newBaseExecutor(v.Schema(), b.ctx, leftExec, rightExec),
		resultGenerator: newJoinResultGenerator(b.ctx, v.JoinType, v.JoinType == plan.RightOuterJoin, defaultValues, v.OtherConditions, lhsColTypes, rhsColTypes),
		stmtCtx:         b.ctx.GetSessionVars().StmtCtx,
		// left is the outer side by default.
		outerIdx:  0,
		outerKeys: leftKeys,
		innerKeys: rightKeys,
		outerIter: lhsIter,
		innerIter: rhsIter,
	}

	if v.JoinType == plan.RightOuterJoin {
		e.outerKeys, e.innerKeys = e.innerKeys, e.outerKeys
		e.outerIter, e.innerIter = e.innerIter, e.outerIter
		e.outerIdx = 1
	}

	if v.JoinType != plan.InnerJoin {
		e.outerFilter = e.outerIter.filter
		e.outerIter.filter = nil
	}
	e.supportChk = true
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
	rightExec := b.build(v.Children()[1])

	// for hash join, inner table is always the smaller one.
	e := &HashJoinExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, leftExec, rightExec),
		concurrency:  v.Concurrency,
		joinType:     v.JoinType,
		innerIdx:     v.SmallChildIdx,
	}

	defaultValues := v.DefaultValues
	lhsTypes, rhsTypes := leftExec.retTypes(), rightExec.retTypes()
	if v.SmallChildIdx == 0 {
		e.innerExec = leftExec
		e.outerExec = rightExec
		e.innerFilter = v.LeftConditions
		e.outerFilter = v.RightConditions
		e.innerKeys = leftHashKey
		e.outerKeys = rightHashKey
		if defaultValues == nil {
			defaultValues = make([]types.Datum, e.innerExec.Schema().Len())
		}
	} else {
		e.innerExec = rightExec
		e.outerExec = leftExec
		e.innerFilter = v.RightConditions
		e.outerFilter = v.LeftConditions
		e.innerKeys = rightHashKey
		e.outerKeys = leftHashKey
		if defaultValues == nil {
			defaultValues = make([]types.Datum, e.innerExec.Schema().Len())
		}
	}
	e.resultGenerators = make([]joinResultGenerator, e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		e.resultGenerators[i] = newJoinResultGenerator(b.ctx, v.JoinType, v.SmallChildIdx == 0, defaultValues,
			v.OtherConditions, lhsTypes, rhsTypes)
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildHashAgg(v *plan.PhysicalHashAgg) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	e := &HashAggExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, src),
		sc:           b.ctx.GetSessionVars().StmtCtx,
		AggFuncs:     make([]aggregation.Aggregation, 0, len(v.AggFuncs)),
		GroupByItems: v.GroupByItems,
	}
	e.supportChk = true
	for _, aggDesc := range v.AggFuncs {
		e.AggFuncs = append(e.AggFuncs, aggDesc.GetAggFunc())
	}
	return e
}

func (b *executorBuilder) buildStreamAgg(v *plan.PhysicalStreamAgg) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	e := &StreamAggExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, src),
		StmtCtx:      b.ctx.GetSessionVars().StmtCtx,
		AggFuncs:     make([]aggregation.Aggregation, 0, len(v.AggFuncs)),
		GroupByItems: v.GroupByItems,
	}
	e.supportChk = true
	for _, aggDesc := range v.AggFuncs {
		e.AggFuncs = append(e.AggFuncs, aggDesc.GetAggFunc())
	}
	return e
}

func (b *executorBuilder) buildSelection(v *plan.PhysicalSelection) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	e := &SelectionExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, childExec),
		filters:      v.Conditions,
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildProjection(v *plan.PhysicalProjection) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	e := &ProjectionExec{
		baseExecutor:     newBaseExecutor(v.Schema(), b.ctx, childExec),
		exprs:            v.Exprs,
		evaluatorSuit:    expression.NewEvaluatorSuit(v.Exprs),
		calculateNoDelay: v.CalculateNoDelay,
	}
	e.baseExecutor.supportChk = true
	return e
}

func (b *executorBuilder) buildTableDual(v *plan.PhysicalTableDual) Executor {
	if v.RowCount != 0 && v.RowCount != 1 {
		b.err = errors.Errorf("buildTableDual failed, invalid row count for dual table: %v", v.RowCount)
		return nil
	}
	e := &TableDualExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
		numDualRows:  v.RowCount,
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) getStartTS() uint64 {
	if b.startTS != 0 {
		// Return the cached value.
		return b.startTS
	}

	startTS := b.ctx.GetSessionVars().SnapshotTS
	if startTS == 0 {
		startTS = b.ctx.Txn().StartTS()
	}
	b.startTS = startTS
	return startTS
}

func (b *executorBuilder) buildMemTable(v *plan.PhysicalMemTable) Executor {
	tb, _ := b.is.TableByID(v.Table.ID)
	e := &TableScanExec{
		baseExecutor:   newBaseExecutor(v.Schema(), b.ctx),
		t:              tb,
		columns:        v.Columns,
		seekHandle:     math.MinInt64,
		ranges:         v.Ranges,
		isVirtualTable: tb.Type() == table.VirtualTable,
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildSort(v *plan.PhysicalSort) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	sortExec := SortExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, childExec),
		ByItems:      v.ByItems,
		schema:       v.Schema(),
	}
	sortExec.supportChk = true
	return &sortExec
}

func (b *executorBuilder) buildTopN(v *plan.PhysicalTopN) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	sortExec := SortExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, childExec),
		ByItems:      v.ByItems,
		schema:       v.Schema(),
	}
	sortExec.supportChk = true
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
	for _, cond := range v.EqualConditions {
		col0 := cond.GetArgs()[0].(*expression.Column)
		col0.ResolveIndices(joinSchema)
		col1 := cond.GetArgs()[1].(*expression.Column)
		col1.ResolveIndices(joinSchema)
	}
	otherConditions := append(expression.ScalarFuncs2Exprs(v.EqualConditions), v.OtherConditions...)
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Datum, v.Children()[v.SmallChildIdx].Schema().Len())
	}
	generator := newJoinResultGenerator(b.ctx, v.JoinType, v.SmallChildIdx == 0,
		defaultValues, otherConditions, leftChild.retTypes(), rightChild.retTypes())
	outerExec, innerExec := leftChild, rightChild
	outerFilter, innerFilter := v.LeftConditions, v.RightConditions
	if v.SmallChildIdx == 0 {
		outerExec, innerExec = rightChild, leftChild
		outerFilter, innerFilter = v.RightConditions, v.LeftConditions
	}
	e := &NestedLoopApplyExec{
		baseExecutor:    newBaseExecutor(apply.Schema(), b.ctx, outerExec, innerExec),
		innerExec:       innerExec,
		outerExec:       outerExec,
		outerFilter:     outerFilter,
		innerFilter:     innerFilter,
		outer:           v.JoinType != plan.InnerJoin,
		resultGenerator: generator,
		outerSchema:     apply.OuterSchema,
		outerChunk:      outerExec.newChunk(),
		innerChunk:      innerExec.newChunk(),
	}
	e.resultChunk = e.newChunk()
	e.innerList = chunk.NewList(innerExec.retTypes(), e.maxChunkSize)
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildExists(v *plan.PhysicalExists) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	e := &ExistsExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, childExec),
	}
	e.supportChk = true
	return e
}

func (b *executorBuilder) buildMaxOneRow(v *plan.PhysicalMaxOneRow) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		b.err = errors.Trace(b.err)
		return nil
	}
	e := &MaxOneRowExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, childExec),
	}
	e.supportChk = true
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
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, childExecs...),
	}
	e.supportChk = true
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
	updateExec := &UpdateExec{
		baseExecutor: newBaseExecutor(nil, b.ctx, selExec),
		SelectExec:   selExec,
		OrderedList:  v.OrderedList,
		tblID2table:  tblID2table,
		IgnoreErr:    v.IgnoreErr,
	}
	updateExec.supportChk = true
	return updateExec
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
		baseExecutor: newBaseExecutor(nil, b.ctx, selExec),
		SelectExec:   selExec,
		Tables:       v.Tables,
		IsMultiTable: v.IsMultiTable,
		tblID2Table:  tblID2table,
	}
	deleteExec.supportChk = true
	return deleteExec
}

func (b *executorBuilder) buildAnalyzeIndexPushdown(task plan.AnalyzeIndexTask) *AnalyzeIndexExec {
	e := &AnalyzeIndexExec{
		ctx:         b.ctx,
		tblInfo:     task.TableInfo,
		idxInfo:     task.IndexInfo,
		concurrency: b.ctx.GetSessionVars().IndexSerialScanConcurrency,
		priority:    b.priority,
		analyzePB: &tipb.AnalyzeReq{
			Tp:             tipb.AnalyzeType_TypeIndex,
			StartTs:        math.MaxUint64,
			Flags:          statementContextToFlags(b.ctx.GetSessionVars().StmtCtx),
			TimeZoneOffset: timeZoneOffset(b.ctx),
		},
	}
	e.analyzePB.IdxReq = &tipb.AnalyzeIndexReq{
		BucketSize: maxBucketSize,
		NumColumns: int32(len(task.IndexInfo.Columns)),
	}
	if !task.IndexInfo.Unique {
		depth := int32(defaultCMSketchDepth)
		width := int32(defaultCMSketchWidth)
		e.analyzePB.IdxReq.CmsketchDepth = &depth
		e.analyzePB.IdxReq.CmsketchWidth = &width
	}
	return e
}

func (b *executorBuilder) buildAnalyzeColumnsPushdown(task plan.AnalyzeColumnsTask) *AnalyzeColumnsExec {
	cols := task.ColsInfo
	keepOrder := false
	if task.PKInfo != nil {
		keepOrder = true
		cols = append([]*model.ColumnInfo{task.PKInfo}, cols...)
	}
	e := &AnalyzeColumnsExec{
		ctx:         b.ctx,
		tblInfo:     task.TableInfo,
		colsInfo:    task.ColsInfo,
		pkInfo:      task.PKInfo,
		concurrency: b.ctx.GetSessionVars().DistSQLScanConcurrency,
		priority:    b.priority,
		keepOrder:   keepOrder,
		analyzePB: &tipb.AnalyzeReq{
			Tp:             tipb.AnalyzeType_TypeColumn,
			StartTs:        math.MaxUint64,
			Flags:          statementContextToFlags(b.ctx.GetSessionVars().StmtCtx),
			TimeZoneOffset: timeZoneOffset(b.ctx),
		},
	}
	depth := int32(defaultCMSketchDepth)
	width := int32(defaultCMSketchWidth)
	e.analyzePB.ColReq = &tipb.AnalyzeColumnsReq{
		BucketSize:    maxBucketSize,
		SampleSize:    maxRegionSampleSize,
		SketchSize:    maxSketchSize,
		ColumnsInfo:   distsql.ColumnsToProto(cols, task.TableInfo.PKIsHandle),
		CmsketchDepth: &depth,
		CmsketchWidth: &width,
	}
	b.err = errors.Trace(setPBColumnsDefaultValue(b.ctx, e.analyzePB.ColReq.ColumnsInfo, cols))
	return e
}

func (b *executorBuilder) buildAnalyze(v *plan.Analyze) Executor {
	e := &AnalyzeExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
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
	e.supportChk = true
	return e
}

func (b *executorBuilder) constructDAGReq(plans []plan.PhysicalPlan) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.StartTs = b.getStartTS()
	dagReq.TimeZoneOffset = timeZoneOffset(b.ctx)
	sc := b.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = statementContextToFlags(sc)
	for _, p := range plans {
		execPB, err := p.ToPB(b.ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		dagReq.Executors = append(dagReq.Executors, execPB)
	}
	return dagReq, nil
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
	outerFilter, innerFilter := v.LeftConditions, v.RightConditions
	leftTypes, rightTypes := outerTypes, innerTypes

	if v.OuterIndex == 1 {
		leftTypes, rightTypes = innerTypes, outerTypes
		outerFilter, innerFilter = innerFilter, outerFilter
	}
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Datum, len(innerTypes))
	}
	e := &IndexLookUpJoin{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, outerExec),
		outerCtx: outerCtx{
			rowTypes: outerTypes,
			filter:   outerFilter,
		},
		innerCtx: innerCtx{
			readerBuilder: &dataReaderBuilder{innerPlan, b},
			rowTypes:      innerTypes,
			filter:        innerFilter,
		},
		workerWg:        new(sync.WaitGroup),
		resultGenerator: newJoinResultGenerator(b.ctx, v.JoinType, v.OuterIndex == 1, defaultValues, v.OtherConditions, leftTypes, rightTypes),
		indexRanges:     v.Ranges,
		keyOff2IdxOff:   v.KeyOff2IdxOff,
	}
	e.supportChk = true
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
	dagReq, err := b.constructDAGReq(v.TablePlans)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ts := v.TablePlans[0].(*plan.PhysicalTableScan)
	table, _ := b.is.TableByID(ts.Table.ID)
	pkID := int64(-1)
	if ts.Table.PKIsHandle {
		if pk := ts.Table.GetPkColInfo(); pk != nil {
			pkID = pk.ID
		}
	}
	e := &TableReaderExecutor{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
		dagPB:        dagReq,
		tableID:      ts.Table.ID,
		table:        table,
		keepOrder:    ts.KeepOrder,
		desc:         ts.Desc,
		columns:      ts.Columns,
		priority:     b.priority,
	}
	if containsLimit(dagReq.Executors) {
		e.feedback = statistics.NewQueryFeedback(0, 0, false, 0, 0)
	} else {
		e.feedback = statistics.NewQueryFeedback(ts.Table.ID, pkID, false, ts.HistVersion, ts.StatsInfo().Count())
	}
	e.baseExecutor.supportChk = true

	for i := range v.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	return e, nil
}

func (b *executorBuilder) buildTableReader(v *plan.PhysicalTableReader) *TableReaderExecutor {
	ret, err := buildNoRangeTableReader(b, v)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}

	ts := v.TablePlans[0].(*plan.PhysicalTableScan)
	ret.ranges = ts.Ranges
	return ret
}

func buildNoRangeIndexReader(b *executorBuilder, v *plan.PhysicalIndexReader) (*IndexReaderExecutor, error) {
	dagReq, err := b.constructDAGReq(v.IndexPlans)
	if err != nil {
		return nil, errors.Trace(err)
	}
	is := v.IndexPlans[0].(*plan.PhysicalIndexScan)
	table, _ := b.is.TableByID(is.Table.ID)
	e := &IndexReaderExecutor{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
		dagPB:        dagReq,
		tableID:      is.Table.ID,
		table:        table,
		index:        is.Index,
		keepOrder:    is.KeepOrder,
		desc:         is.Desc,
		columns:      is.Columns,
		priority:     b.priority,
	}
	if containsLimit(dagReq.Executors) {
		e.feedback = statistics.NewQueryFeedback(0, 0, false, 0, 0)
	} else {
		e.feedback = statistics.NewQueryFeedback(is.Table.ID, is.Index.ID, true, is.HistVersion, is.StatsInfo().Count())
	}
	e.supportChk = true

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
	return ret
}

func buildNoRangeIndexLookUpReader(b *executorBuilder, v *plan.PhysicalIndexLookUpReader) (*IndexLookUpExecutor, error) {
	indexReq, err := b.constructDAGReq(v.IndexPlans)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableReq, err := b.constructDAGReq(v.TablePlans)
	if err != nil {
		return nil, errors.Trace(err)
	}
	is := v.IndexPlans[0].(*plan.PhysicalIndexScan)
	indexReq.OutputOffsets = []uint32{uint32(len(is.Index.Columns))}
	table, _ := b.is.TableByID(is.Table.ID)

	for i := 0; i < v.Schema().Len(); i++ {
		tableReq.OutputOffsets = append(tableReq.OutputOffsets, uint32(i))
	}

	e := &IndexLookUpExecutor{
		baseExecutor:      newBaseExecutor(v.Schema(), b.ctx),
		dagPB:             indexReq,
		tableID:           is.Table.ID,
		table:             table,
		index:             is.Index,
		keepOrder:         is.KeepOrder,
		desc:              is.Desc,
		tableRequest:      tableReq,
		columns:           is.Columns,
		priority:          b.priority,
		dataReaderBuilder: &dataReaderBuilder{executorBuilder: b},
	}
	if containsLimit(indexReq.Executors) {
		e.feedback = statistics.NewQueryFeedback(0, 0, false, 0, 0)
	} else {
		e.feedback = statistics.NewQueryFeedback(is.Table.ID, is.Index.ID, true, is.HistVersion, is.StatsInfo().Count())
	}
	e.supportChk = true
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
	ret.ranges = is.Ranges
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

func (builder *dataReaderBuilder) buildExecutorForIndexJoin(goCtx goctx.Context, datums [][]types.Datum,
	IndexRanges []*ranger.NewRange, keyOff2IdxOff []int) (Executor, error) {
	switch v := builder.Plan.(type) {
	case *plan.PhysicalTableReader:
		return builder.buildTableReaderForIndexJoin(goCtx, v, datums)
	case *plan.PhysicalIndexReader:
		return builder.buildIndexReaderForIndexJoin(goCtx, v, datums, IndexRanges, keyOff2IdxOff)
	case *plan.PhysicalIndexLookUpReader:
		return builder.buildIndexLookUpReaderForIndexJoin(goCtx, v, datums, IndexRanges, keyOff2IdxOff)
	}
	return nil, errors.New("Wrong plan type for dataReaderBuilder")
}

func (builder *dataReaderBuilder) buildTableReaderForIndexJoin(goCtx goctx.Context, v *plan.PhysicalTableReader, datums [][]types.Datum) (Executor, error) {
	e, err := buildNoRangeTableReader(builder.executorBuilder, v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	handles := make([]int64, 0, len(datums))
	for _, datum := range datums {
		handles = append(handles, datum[0].GetInt64())
	}
	return builder.buildTableReaderFromHandles(goCtx, e, handles)
}

func (builder *dataReaderBuilder) buildTableReaderFromHandles(goCtx goctx.Context, e *TableReaderExecutor, handles []int64) (Executor, error) {
	sort.Sort(sortutil.Int64Slice(handles))
	var b requestBuilder
	kvReq, err := b.SetTableHandles(e.tableID, handles).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetPriority(e.priority).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.resultHandler = &tableResultHandler{}
	result, err := distsql.SelectDAG(goCtx, builder.ctx, kvReq, e.retTypes())
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.Fetch(goCtx)
	e.resultHandler.open(nil, result)
	return e, nil
}

func (builder *dataReaderBuilder) buildIndexReaderForIndexJoin(goCtx goctx.Context, v *plan.PhysicalIndexReader,
	values [][]types.Datum, indexRanges []*ranger.NewRange, keyOff2IdxOff []int) (Executor, error) {
	e, err := buildNoRangeIndexReader(builder.executorBuilder, v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	kvRanges, err := buildKvRangesForIndexJoin(e.ctx.GetSessionVars().StmtCtx, e.tableID, e.index.ID, values, indexRanges, keyOff2IdxOff)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = e.open(goCtx, kvRanges)
	return e, errors.Trace(err)
}

func (builder *dataReaderBuilder) buildIndexLookUpReaderForIndexJoin(goCtx goctx.Context, v *plan.PhysicalIndexLookUpReader,
	values [][]types.Datum, indexRanges []*ranger.NewRange, keyOff2IdxOff []int) (Executor, error) {
	e, err := buildNoRangeIndexLookUpReader(builder.executorBuilder, v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	kvRanges, err := buildKvRangesForIndexJoin(e.ctx.GetSessionVars().StmtCtx, e.tableID, e.index.ID, values, indexRanges, keyOff2IdxOff)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = e.open(goCtx, kvRanges)
	return e, errors.Trace(err)
}

// buildKvRangesForIndexJoin builds kv ranges for index join when the inner plan is index scan plan.
func buildKvRangesForIndexJoin(sc *stmtctx.StatementContext, tableID, indexID int64, keyDatums [][]types.Datum, indexRanges []*ranger.NewRange, keyOff2IdxOff []int) ([]kv.KeyRange, error) {
	kvRanges := make([]kv.KeyRange, 0, len(indexRanges)*len(keyDatums))
	for _, val := range keyDatums {
		for _, ran := range indexRanges {
			for keyOff, idxOff := range keyOff2IdxOff {
				ran.LowVal[idxOff] = val[keyOff]
				ran.HighVal[idxOff] = val[keyOff]
			}
		}
		tmpKvRanges, err := indexRangesToKVRanges(sc, tableID, indexID, indexRanges)
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
