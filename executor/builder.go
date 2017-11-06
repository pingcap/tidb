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
	"math"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
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
	case *plan.Limit:
		return b.buildLimit(v)
	case *plan.Prepare:
		return b.buildPrepare(v)
	case *plan.SelectLock:
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
	case *plan.Sort:
		return b.buildSort(v)
	case *plan.TopN:
		return b.buildTopN(v)
	case *plan.Union:
		return b.buildUnion(v)
	case *plan.Update:
		return b.buildUpdate(v)
	case *plan.PhysicalUnionScan:
		return b.buildUnionScanExec(v)
	case *plan.PhysicalHashJoin:
		return b.buildHashJoin(v)
	case *plan.PhysicalMergeJoin:
		return b.buildMergeJoin(v)
	case *plan.PhysicalHashSemiJoin:
		return b.buildSemiJoin(v)
	case *plan.PhysicalIndexJoin:
		return b.buildIndexLookUpJoin(v)
	case *plan.Selection:
		return b.buildSelection(v)
	case *plan.PhysicalAggregation:
		return b.buildAggregation(v)
	case *plan.Projection:
		return b.buildProjection(v)
	case *plan.PhysicalMemTable:
		return b.buildMemTable(v)
	case *plan.TableDual:
		return b.buildTableDual(v)
	case *plan.PhysicalApply:
		return b.buildApply(v)
	case *plan.Exists:
		return b.buildExists(v)
	case *plan.MaxOneRow:
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
		JobIDs:       v.JobIDs,
	}
	e.errs, b.err = admin.CancelJobs(e.ctx.Txn(), e.JobIDs)
	if b.err != nil {
		return nil
	}

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
	ownerManager := sessionctx.GetDomain(e.ctx).DDL().OwnerManager()
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
	return e
}

func (b *executorBuilder) buildShowDDLJobs(v *plan.ShowDDLJobs) Executor {
	e := &ShowDDLJobsExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
	}

	var err error
	e.jobs, err = admin.GetDDLJobs(e.ctx.Txn())
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	historyJobs, err := admin.GetHistoryDDLJobs(e.ctx.Txn())
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	e.jobs = append(e.jobs, historyJobs...)
	return e
}

func (b *executorBuilder) buildCheckTable(v *plan.CheckTable) Executor {
	return &CheckTableExec{
		tables: v.Tables,
		ctx:    b.ctx,
		is:     b.is,
	}
}

func (b *executorBuilder) buildDeallocate(v *plan.Deallocate) Executor {
	return &DeallocateExec{
		ctx:  b.ctx,
		Name: v.Name,
	}
}

func (b *executorBuilder) buildSelectLock(v *plan.SelectLock) Executor {
	src := b.build(v.Children()[0])
	if !b.ctx.GetSessionVars().InTxn() {
		// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
		// is disabled (either by beginning transaction with START TRANSACTION or by setting
		// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
		// See https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
		return src
	}
	e := &SelectLockExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, b.build(v.Children()[0])),
		Lock:         v.Lock,
	}
	return e
}

func (b *executorBuilder) buildLimit(v *plan.Limit) Executor {
	e := &LimitExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, b.build(v.Children()[0])),
		Offset:       v.Offset,
		Count:        v.Count,
	}
	return e
}

func (b *executorBuilder) buildPrepare(v *plan.Prepare) Executor {
	return &PrepareExec{
		Ctx:     b.ctx,
		IS:      b.is,
		Name:    v.Name,
		SQLText: v.SQLText,
	}
}

func (b *executorBuilder) buildExecute(v *plan.Execute) Executor {
	return &ExecuteExec{
		Ctx:       b.ctx,
		IS:        b.is,
		Name:      v.Name,
		UsingVars: v.UsingVars,
		ID:        v.ExecID,
		Stmt:      v.Stmt,
		Plan:      v.Plan,
	}
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
	if e.Tp == ast.ShowGrants && e.User == nil {
		e.User = e.ctx.GetSessionVars().User
	}
	return e
}

func (b *executorBuilder) buildSimple(v *plan.Simple) Executor {
	switch s := v.Statement.(type) {
	case *ast.GrantStmt:
		return b.buildGrant(s)
	case *ast.RevokeStmt:
		return b.buildRevoke(s)
	}
	return &SimpleExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
		Statement:    v.Statement,
		is:           b.is,
	}
}

func (b *executorBuilder) buildSet(v *plan.Set) Executor {
	return &SetExecutor{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
		vars:         v.VarAssigns,
	}
}

func (b *executorBuilder) buildInsert(v *plan.Insert) Executor {
	ivs := &InsertValues{
		ctx:                   b.ctx,
		Columns:               v.Columns,
		Lists:                 v.Lists,
		Setlist:               v.Setlist,
		GenColumns:            v.GenCols.Columns,
		GenExprs:              v.GenCols.Exprs,
		needFillDefaultValues: v.NeedFillDefaultValue,
	}
	if len(v.Children()) > 0 {
		ivs.SelectExec = b.build(v.Children()[0])
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
	return insert
}

func (b *executorBuilder) buildLoadData(v *plan.LoadData) Executor {
	tbl, ok := b.is.TableByID(v.Table.TableInfo.ID)
	if !ok {
		b.err = errors.Errorf("Can not get table %d", v.Table.TableInfo.ID)
		return nil
	}
	insertVal := &InsertValues{
		ctx:        b.ctx,
		Table:      tbl,
		Columns:    v.Columns,
		GenColumns: v.GenCols.Columns,
		GenExprs:   v.GenCols.Exprs,
	}
	tableCols := tbl.Cols()
	columns, err := insertVal.getColumns(tableCols)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}

	return &LoadData{
		IsLocal: v.IsLocal,
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
}

func (b *executorBuilder) buildReplace(vals *InsertValues) Executor {
	return &ReplaceExec{
		InsertValues: vals,
	}
}

func (b *executorBuilder) buildGrant(grant *ast.GrantStmt) Executor {
	return &GrantExec{
		ctx:        b.ctx,
		Privs:      grant.Privs,
		ObjectType: grant.ObjectType,
		Level:      grant.Level,
		Users:      grant.Users,
		WithGrant:  grant.WithGrant,
		is:         b.is,
	}
}

func (b *executorBuilder) buildRevoke(revoke *ast.RevokeStmt) Executor {
	return &RevokeExec{
		ctx:        b.ctx,
		Privs:      revoke.Privs,
		ObjectType: revoke.ObjectType,
		Level:      revoke.Level,
		Users:      revoke.Users,
		is:         b.is,
	}
}

func (b *executorBuilder) buildDDL(v *plan.DDL) Executor {
	return &DDLExec{Statement: v.Statement, ctx: b.ctx, is: b.is}
}

func (b *executorBuilder) buildExplain(v *plan.Explain) Executor {
	exec := &ExplainExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
	}
	exec.rows = make([]Row, 0, len(v.Rows))
	for _, row := range v.Rows {
		exec.rows = append(exec.rows, row)
	}
	return exec
}

func (b *executorBuilder) buildUnionScanExec(v *plan.PhysicalUnionScan) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	us := &UnionScanExec{baseExecutor: newBaseExecutor(v.Schema(), b.ctx, src)}
	// Get the handle column index of the below plan.
	// We can guarantee that there must be only one col in the map.
	for _, cols := range v.Children()[0].Schema().TblID2Handle {
		for _, col := range cols {
			us.belowHandleIndex = col.Index
			// If we don't found the handle column in the union scan's schema,
			// we need to remove it when output.
			if us.schema.ColumnIndex(col) != -1 {
				us.handleColIsUsed = true
			}
		}
	}
	switch x := src.(type) {
	case *TableReaderExecutor:
		us.desc = x.desc
		us.dirty = getDirtyDB(b.ctx).getDirtyTable(x.table.Meta().ID)
		us.conditions = v.Conditions
		us.columns = x.columns
		b.err = us.buildAndSortAddedRows(x.table)
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
		b.err = us.buildAndSortAddedRows(x.table)
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
		b.err = us.buildAndSortAddedRows(x.table)
	default:
		// The mem table will not be written by sql directly, so we can omit the union scan to avoid err reporting.
		return src
	}
	if b.err != nil {
		return nil
	}
	return us
}

// buildMergeJoin builds SortMergeJoin executor.
// TODO: Refactor against different join strategies by extracting common code base
func (b *executorBuilder) buildMergeJoin(v *plan.PhysicalMergeJoin) Executor {
	joinBuilder := &joinBuilder{
		context:       b.ctx,
		leftChild:     b.build(v.Children()[0]),
		rightChild:    b.build(v.Children()[1]),
		eqConditions:  v.EqualConditions,
		leftFilter:    v.LeftConditions,
		rightFilter:   v.RightConditions,
		otherFilter:   v.OtherConditions,
		schema:        v.Schema(),
		joinType:      v.JoinType,
		defaultValues: v.DefaultValues,
	}
	exec, err := joinBuilder.BuildMergeJoin()
	if err != nil {
		b.err = err
		return nil
	}
	if exec == nil {
		b.err = ErrBuildExecutor.GenByArgs("failed to generate merge join executor: ", v.ID())
		return nil
	}

	return exec
}

func (b *executorBuilder) buildHashJoin(v *plan.PhysicalHashJoin) Executor {
	var leftHashKey, rightHashKey []*expression.Column
	for _, eqCond := range v.EqualConditions {
		ln, _ := eqCond.GetArgs()[0].(*expression.Column)
		rn, _ := eqCond.GetArgs()[1].(*expression.Column)
		leftHashKey = append(leftHashKey, ln)
		rightHashKey = append(rightHashKey, rn)
	}
	e := &HashJoinExec{
		schema:        v.Schema(),
		otherFilter:   v.OtherConditions,
		prepared:      false,
		ctx:           b.ctx,
		concurrency:   v.Concurrency,
		defaultValues: v.DefaultValues,
	}
	if v.SmallTable == 1 {
		e.smallFilter = v.RightConditions
		e.bigFilter = v.LeftConditions
		e.smallHashKey = rightHashKey
		e.bigHashKey = leftHashKey
		e.leftSmall = false
	} else {
		e.leftSmall = true
		e.smallFilter = v.LeftConditions
		e.bigFilter = v.RightConditions
		e.smallHashKey = leftHashKey
		e.bigHashKey = rightHashKey
	}
	if v.JoinType == plan.LeftOuterJoin || v.JoinType == plan.RightOuterJoin {
		e.outer = true
	}
	if e.leftSmall {
		e.smallExec = b.build(v.Children()[0])
		e.bigExec = b.build(v.Children()[1])
	} else {
		e.smallExec = b.build(v.Children()[1])
		e.bigExec = b.build(v.Children()[0])
	}
	for i := 0; i < e.concurrency; i++ {
		ctx := &hashJoinCtx{}
		if e.bigFilter != nil {
			ctx.bigFilter = e.bigFilter.Clone()
		}
		if e.otherFilter != nil {
			ctx.otherFilter = e.otherFilter.Clone()
		}
		ctx.datumBuffer = make([]types.Datum, len(e.bigHashKey))
		ctx.hashKeyBuffer = make([]byte, 0, 10000)
		e.hashJoinContexts = append(e.hashJoinContexts, ctx)
	}
	return e
}

func (b *executorBuilder) buildSemiJoin(v *plan.PhysicalHashSemiJoin) *HashSemiJoinExec {
	var leftHashKey, rightHashKey []*expression.Column
	for _, eqCond := range v.EqualConditions {
		ln, _ := eqCond.GetArgs()[0].(*expression.Column)
		rn, _ := eqCond.GetArgs()[1].(*expression.Column)
		leftHashKey = append(leftHashKey, ln)
		rightHashKey = append(rightHashKey, rn)
	}
	e := &HashSemiJoinExec{
		schema:       v.Schema(),
		otherFilter:  v.OtherConditions,
		bigFilter:    v.LeftConditions,
		smallFilter:  v.RightConditions,
		bigExec:      b.build(v.Children()[0]),
		smallExec:    b.build(v.Children()[1]),
		prepared:     false,
		ctx:          b.ctx,
		bigHashKey:   leftHashKey,
		smallHashKey: rightHashKey,
		auxMode:      v.WithAux,
		anti:         v.Anti,
	}
	return e
}

func (b *executorBuilder) buildAggregation(v *plan.PhysicalAggregation) Executor {
	if v.AggType == plan.StreamedAgg {
		return &StreamAggExec{
			baseExecutor: newBaseExecutor(v.Schema(), b.ctx, b.build(v.Children()[0])),
			StmtCtx:      b.ctx.GetSessionVars().StmtCtx,
			AggFuncs:     v.AggFuncs,
			GroupByItems: v.GroupByItems,
		}
	}
	return &HashAggExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, b.build(v.Children()[0])),
		sc:           b.ctx.GetSessionVars().StmtCtx,
		AggFuncs:     v.AggFuncs,
		GroupByItems: v.GroupByItems,
		aggType:      v.AggType,
		hasGby:       v.HasGby,
	}
}

func (b *executorBuilder) buildSelection(v *plan.Selection) Executor {
	exec := &SelectionExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, b.build(v.Children()[0])),
		Conditions:   v.Conditions,
	}
	return exec
}

func (b *executorBuilder) buildProjection(v *plan.Projection) Executor {
	return &ProjectionExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, b.build(v.Children()[0])),
		exprs:        v.Exprs,
	}
}

func (b *executorBuilder) buildTableDual(v *plan.TableDual) Executor {
	return &TableDualExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx),
		rowCount:     v.RowCount,
	}
}

func (b *executorBuilder) getStartTS() uint64 {
	startTS := b.ctx.GetSessionVars().SnapshotTS
	if startTS == 0 {
		startTS = b.ctx.Txn().StartTS()
	}
	return startTS
}

func (b *executorBuilder) buildMemTable(v *plan.PhysicalMemTable) Executor {
	tb, _ := b.is.TableByID(v.Table.ID)
	ts := &TableScanExec{
		t:              tb,
		ctx:            b.ctx,
		columns:        v.Columns,
		schema:         v.Schema(),
		seekHandle:     math.MinInt64,
		ranges:         v.Ranges,
		isVirtualTable: tb.Type() == table.VirtualTable,
	}
	return ts
}

func (b *executorBuilder) buildSort(v *plan.Sort) Executor {
	sortExec := SortExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, b.build(v.Children()[0])),
		ByItems:      v.ByItems,
		schema:       v.Schema(),
	}
	if v.ExecLimit != nil {
		return &TopNExec{
			SortExec: sortExec,
			limit:    v.ExecLimit,
		}
	}
	return &sortExec
}

func (b *executorBuilder) buildTopN(v *plan.TopN) Executor {
	sortExec := SortExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, b.build(v.Children()[0])),
		ByItems:      v.ByItems,
		schema:       v.Schema(),
	}
	return &TopNExec{
		SortExec: sortExec,
		limit:    &plan.Limit{Count: v.Count, Offset: v.Offset},
	}
}

func (b *executorBuilder) buildNestedLoopJoin(v *plan.PhysicalHashJoin) *NestedLoopJoinExec {
	for _, cond := range v.EqualConditions {
		cond.GetArgs()[0].(*expression.Column).ResolveIndices(v.Schema())
		cond.GetArgs()[1].(*expression.Column).ResolveIndices(v.Schema())
	}
	if v.SmallTable == 1 {
		return &NestedLoopJoinExec{
			SmallExec:     b.build(v.Children()[1]),
			BigExec:       b.build(v.Children()[0]),
			Ctx:           b.ctx,
			BigFilter:     v.LeftConditions,
			SmallFilter:   v.RightConditions,
			OtherFilter:   append(expression.ScalarFuncs2Exprs(v.EqualConditions), v.OtherConditions...),
			schema:        v.Schema(),
			outer:         v.JoinType != plan.InnerJoin,
			defaultValues: v.DefaultValues,
		}
	}
	return &NestedLoopJoinExec{
		SmallExec:     b.build(v.Children()[0]),
		BigExec:       b.build(v.Children()[1]),
		leftSmall:     true,
		Ctx:           b.ctx,
		BigFilter:     v.RightConditions,
		SmallFilter:   v.LeftConditions,
		OtherFilter:   append(expression.ScalarFuncs2Exprs(v.EqualConditions), v.OtherConditions...),
		schema:        v.Schema(),
		outer:         v.JoinType != plan.InnerJoin,
		defaultValues: v.DefaultValues,
	}
}

func (b *executorBuilder) buildApply(v *plan.PhysicalApply) Executor {
	var join joinExec
	switch x := v.PhysicalJoin.(type) {
	case *plan.PhysicalHashSemiJoin:
		join = b.buildSemiJoin(x)
	case *plan.PhysicalHashJoin:
		if x.JoinType == plan.InnerJoin || x.JoinType == plan.LeftOuterJoin || x.JoinType == plan.RightOuterJoin {
			join = b.buildNestedLoopJoin(x)
		} else {
			b.err = errors.Errorf("Unsupported join type %v in nested loop join", x.JoinType)
		}
	default:
		b.err = errors.Errorf("Unsupported plan type %T in apply", v)
	}
	apply := &ApplyJoinExec{
		join:        join,
		outerSchema: v.OuterSchema,
		schema:      v.Schema(),
	}
	return apply
}

func (b *executorBuilder) buildExists(v *plan.Exists) Executor {
	return &ExistsExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, b.build(v.Children()[0])),
	}
}

func (b *executorBuilder) buildMaxOneRow(v *plan.MaxOneRow) Executor {
	return &MaxOneRowExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, b.build(v.Children()[0])),
	}
}

func (b *executorBuilder) buildUnion(v *plan.Union) Executor {
	srcs := make([]Executor, len(v.Children()))
	for i, sel := range v.Children() {
		selExec := b.build(sel)
		srcs[i] = selExec
	}
	e := &UnionExec{
		baseExecutor: newBaseExecutor(v.Schema(), b.ctx, srcs...),
	}
	return e
}

func (b *executorBuilder) buildUpdate(v *plan.Update) Executor {
	tblID2table := make(map[int64]table.Table)
	for id := range v.Schema().TblID2Handle {
		tblID2table[id], _ = b.is.TableByID(id)
	}
	return &UpdateExec{
		baseExecutor: newBaseExecutor(nil, b.ctx),
		SelectExec:   b.build(v.Children()[0]),
		OrderedList:  v.OrderedList,
		tblID2table:  tblID2table,
		IgnoreErr:    v.IgnoreErr,
	}
}

func (b *executorBuilder) buildDelete(v *plan.Delete) Executor {
	tblID2table := make(map[int64]table.Table)
	for id := range v.Schema().TblID2Handle {
		tblID2table[id], _ = b.is.TableByID(id)
	}
	return &DeleteExec{
		baseExecutor: newBaseExecutor(nil, b.ctx),
		SelectExec:   b.build(v.Children()[0]),
		Tables:       v.Tables,
		IsMultiTable: v.IsMultiTable,
		tblID2Table:  tblID2table,
	}
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
	e.analyzePB.ColReq = &tipb.AnalyzeColumnsReq{
		BucketSize:  maxBucketSize,
		SampleSize:  maxRegionSampleSize,
		SketchSize:  maxSketchSize,
		ColumnsInfo: distsql.ColumnsToProto(cols, task.TableInfo.PKIsHandle),
	}
	b.err = setPBColumnsDefaultValue(b.ctx, e.analyzePB.ColReq.ColumnsInfo, cols)
	return e
}

func (b *executorBuilder) buildAnalyze(v *plan.Analyze) Executor {
	e := &AnalyzeExec{
		ctx:   b.ctx,
		tasks: make([]*analyzeTask, 0, len(v.Children())),
	}
	for _, task := range v.ColTasks {
		e.tasks = append(e.tasks, &analyzeTask{
			taskType: colTask,
			colExec:  b.buildAnalyzeColumnsPushdown(task),
		})
	}
	for _, task := range v.IdxTasks {
		e.tasks = append(e.tasks, &analyzeTask{
			taskType: idxTask,
			idxExec:  b.buildAnalyzeIndexPushdown(task),
		})
	}
	return e
}

func (b *executorBuilder) constructDAGReq(plans []plan.PhysicalPlan) *tipb.DAGRequest {
	dagReq := &tipb.DAGRequest{}
	dagReq.StartTs = b.getStartTS()
	dagReq.TimeZoneOffset = timeZoneOffset(b.ctx)
	sc := b.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = statementContextToFlags(sc)
	for _, p := range plans {
		execPB, err := p.ToPB(b.ctx)
		if err != nil {
			b.err = errors.Trace(err)
			return nil
		}
		dagReq.Executors = append(dagReq.Executors, execPB)
	}
	return dagReq
}

func (b *executorBuilder) constructTableRanges(ts *plan.PhysicalTableScan) (newRanges []types.IntColumnRange) {
	sc := b.ctx.GetSessionVars().StmtCtx
	cols := expression.ColumnInfos2ColumnsWithDBName(ts.DBName, ts.Table.Name, ts.Columns)
	newRanges = ranger.FullIntRange()
	var pkCol *expression.Column
	if ts.Table.PKIsHandle {
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			pkCol = expression.ColInfo2Col(cols, pkColInfo)
		}
	}
	if pkCol != nil {
		var ranges []types.Range
		ranges, b.err = ranger.BuildRange(sc, ts.AccessCondition, ranger.IntRangeType, []*expression.Column{pkCol}, nil)
		if b.err != nil {
			return nil
		}
		newRanges = ranger.Ranges2IntRanges(ranges)
	}
	return newRanges
}

func (b *executorBuilder) constructIndexRanges(is *plan.PhysicalIndexScan) (newRanges []*types.IndexRange) {
	sc := b.ctx.GetSessionVars().StmtCtx
	cols := expression.ColumnInfos2ColumnsWithDBName(is.DBName, is.Table.Name, is.Columns)
	idxCols, colLengths := expression.IndexInfo2Cols(cols, is.Index)
	newRanges = ranger.FullIndexRange()
	if len(idxCols) > 0 {
		var ranges []types.Range
		ranges, b.err = ranger.BuildRange(sc, is.AccessCondition, ranger.IndexRangeType, idxCols, colLengths)
		if b.err != nil {
			return nil
		}
		newRanges = ranger.Ranges2IndexRanges(ranges)
	}
	return newRanges
}

func (b *executorBuilder) buildIndexLookUpJoin(v *plan.PhysicalIndexJoin) Executor {
	batchSize := 1
	if !v.KeepOrder {
		batchSize = b.ctx.GetSessionVars().IndexJoinBatchSize
	}

	// for IndexLookUpJoin, left is always the outer side.
	outerExec := b.build(v.Children()[0])
	innerExec := b.build(v.Children()[1]).(DataReader)

	return &IndexLookUpJoin{
		baseExecutor:     newBaseExecutor(v.Schema(), b.ctx, outerExec),
		outerExec:        outerExec,
		innerExec:        innerExec,
		outerKeys:        v.OuterJoinKeys,
		innerKeys:        v.InnerJoinKeys,
		outerFilter:      v.LeftConditions,
		innerFilter:      v.RightConditions,
		outerOrderedRows: newKeyRowBlock(batchSize, true),
		innerOrderedRows: newKeyRowBlock(batchSize, false),
		resultGenerator:  newJoinResultGenerator(b.ctx, v.JoinType, v.DefaultValues, v.OtherConditions),
		batchSize:        batchSize,
	}
}

func (b *executorBuilder) buildTableReader(v *plan.PhysicalTableReader) Executor {
	dagReq := b.constructDAGReq(v.TablePlans)
	if b.err != nil {
		return nil
	}
	ts := v.TablePlans[0].(*plan.PhysicalTableScan)
	table, _ := b.is.TableByID(ts.Table.ID)
	newRanges := b.constructTableRanges(ts)
	if b.err != nil {
		return nil
	}
	e := &TableReaderExecutor{
		ctx:       b.ctx,
		schema:    v.Schema(),
		dagPB:     dagReq,
		tableID:   ts.Table.ID,
		table:     table,
		keepOrder: ts.KeepOrder,
		desc:      ts.Desc,
		ranges:    newRanges,
		columns:   ts.Columns,
		priority:  b.priority,
	}

	for i := range v.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	return e
}

func (b *executorBuilder) buildIndexReader(v *plan.PhysicalIndexReader) Executor {
	dagReq := b.constructDAGReq(v.IndexPlans)
	if b.err != nil {
		return nil
	}
	is := v.IndexPlans[0].(*plan.PhysicalIndexScan)
	newRanges := b.constructIndexRanges(is)
	if b.err != nil {
		return nil
	}
	table, _ := b.is.TableByID(is.Table.ID)
	e := &IndexReaderExecutor{
		ctx:       b.ctx,
		schema:    v.Schema(),
		dagPB:     dagReq,
		tableID:   is.Table.ID,
		table:     table,
		index:     is.Index,
		keepOrder: !is.OutOfOrder,
		desc:      is.Desc,
		ranges:    newRanges,
		columns:   is.Columns,
		priority:  b.priority,
	}

	for _, col := range v.OutputColumns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(col.Index))
	}

	return e
}

func (b *executorBuilder) buildIndexLookUpReader(v *plan.PhysicalIndexLookUpReader) Executor {
	indexReq := b.constructDAGReq(v.IndexPlans)
	if b.err != nil {
		return nil
	}
	tableReq := b.constructDAGReq(v.TablePlans)
	if b.err != nil {
		return nil
	}
	is := v.IndexPlans[0].(*plan.PhysicalIndexScan)
	indexReq.OutputOffsets = []uint32{uint32(len(is.Index.Columns))}
	var (
		handleCol         *expression.Column
		tableReaderSchema *expression.Schema
	)
	table, _ := b.is.TableByID(is.Table.ID)
	length := v.Schema().Len()
	if v.NeedColHandle {
		handleCol = v.Schema().TblID2Handle[is.Table.ID][0]
	} else if !is.OutOfOrder {
		tableReaderSchema = v.Schema().Clone()
		handleCol = &expression.Column{
			ID:      model.ExtraHandleID,
			ColName: model.NewCIStr("_rowid"),
			Index:   v.Schema().Len(),
			RetType: types.NewFieldType(mysql.TypeLonglong),
		}
		tableReaderSchema.Append(handleCol)
		length = tableReaderSchema.Len()
	}

	for i := 0; i < length; i++ {
		tableReq.OutputOffsets = append(tableReq.OutputOffsets, uint32(i))
	}

	newRanges := b.constructIndexRanges(is)
	if b.err != nil {
		return nil
	}
	ranges := make([]*types.IndexRange, 0, len(newRanges))
	for _, rangeInPlan := range newRanges {
		ranges = append(ranges, rangeInPlan.Clone())
	}

	e := &IndexLookUpExecutor{
		ctx:               b.ctx,
		schema:            v.Schema(),
		dagPB:             indexReq,
		tableID:           is.Table.ID,
		table:             table,
		index:             is.Index,
		keepOrder:         !is.OutOfOrder,
		desc:              is.Desc,
		ranges:            ranges,
		tableRequest:      tableReq,
		columns:           is.Columns,
		handleCol:         handleCol,
		priority:          b.priority,
		tableReaderSchema: tableReaderSchema,
	}
	return e
}
