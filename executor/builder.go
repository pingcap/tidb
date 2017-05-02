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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/inspectkv"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

// executorBuilder builds an Executor from a Plan.
// The InfoSchema must not change during execution.
type executorBuilder struct {
	ctx context.Context
	is  infoschema.InfoSchema
	// err is set when there is error happened during Executor building process.
	err error
}

func newExecutorBuilder(ctx context.Context, is infoschema.InfoSchema) *executorBuilder {
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
	case *plan.ShowDDL:
		return b.buildShowDDL(v)
	case *plan.Show:
		return b.buildShow(v)
	case *plan.Simple:
		return b.buildSimple(v)
	case *plan.Set:
		return b.buildSet(v)
	case *plan.Sort:
		return b.buildSort(v)
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
	case *plan.Selection:
		return b.buildSelection(v)
	case *plan.PhysicalAggregation:
		return b.buildAggregation(v)
	case *plan.Projection:
		return b.buildProjection(v)
	case *plan.PhysicalMemTable:
		return b.buildMemTable(v)
	case *plan.PhysicalTableScan:
		return b.buildTableScan(v)
	case *plan.PhysicalIndexScan:
		return b.buildIndexScan(v)
	case *plan.TableDual:
		return b.buildTableDual(v)
	case *plan.PhysicalApply:
		return b.buildApply(v)
	case *plan.Exists:
		return b.buildExists(v)
	case *plan.MaxOneRow:
		return b.buildMaxOneRow(v)
	case *plan.Cache:
		return b.buildCache(v)
	case *plan.Analyze:
		return b.buildAnalyze(v)
	case *plan.PhysicalTableReader:
		return b.buildTableReader(v)
	case *plan.PhysicalIndexReader:
		return b.buildIndexReader(v)
	default:
		b.err = ErrUnknownPlan.Gen("Unknown Plan %T", p)
		return nil
	}
}

func (b *executorBuilder) buildShowDDL(v *plan.ShowDDL) Executor {
	// We get DDLInfo here because for Executors that returns result set,
	// next will be called after transaction has been committed.
	// We need the transaction to get DDLInfo.
	e := &ShowDDLExec{
		ctx:    b.ctx,
		schema: v.Schema(),
	}
	ddlInfo, err := inspectkv.GetDDLInfo(e.ctx.Txn())
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	bgInfo, err := inspectkv.GetBgDDLInfo(e.ctx.Txn())
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	e.ddlInfo = ddlInfo
	e.bgInfo = bgInfo
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
		Src:    src,
		Lock:   v.Lock,
		ctx:    b.ctx,
		schema: v.Schema(),
	}
	return e
}

func (b *executorBuilder) buildLimit(v *plan.Limit) Executor {
	src := b.build(v.Children()[0])
	e := &LimitExec{
		Src:    src,
		Offset: v.Offset,
		Count:  v.Count,
		schema: v.Schema(),
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
	}
}

func (b *executorBuilder) buildShow(v *plan.Show) Executor {
	e := &ShowExec{
		Tp:          v.Tp,
		DBName:      model.NewCIStr(v.DBName),
		Table:       v.Table,
		Column:      v.Column,
		User:        v.User,
		Flag:        v.Flag,
		Full:        v.Full,
		GlobalScope: v.GlobalScope,
		ctx:         b.ctx,
		is:          b.is,
		schema:      v.Schema(),
	}
	if e.Tp == ast.ShowGrants && len(e.User) == 0 {
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
	return &SimpleExec{Statement: v.Statement, ctx: b.ctx, is: b.is}
}

func (b *executorBuilder) buildSet(v *plan.Set) Executor {
	return &SetExecutor{
		ctx:  b.ctx,
		vars: v.VarAssigns,
	}
}

func (b *executorBuilder) buildInsert(v *plan.Insert) Executor {
	ivs := &InsertValues{
		ctx:     b.ctx,
		Columns: v.Columns,
		Lists:   v.Lists,
		Setlist: v.Setlist,
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
		OnDuplicate:  v.OnDuplicate,
		Priority:     v.Priority,
		Ignore:       v.Ignore,
	}
	return insert
}

func (b *executorBuilder) buildLoadData(v *plan.LoadData) Executor {
	tbl, ok := b.is.TableByID(v.Table.TableInfo.ID)
	if !ok {
		b.err = errors.Errorf("Can not get table %d", v.Table.TableInfo.ID)
		return nil
	}

	return &LoadData{
		IsLocal: v.IsLocal,
		loadDataInfo: &LoadDataInfo{
			row:        make([]types.Datum, len(tbl.Cols())),
			insertVal:  &InsertValues{ctx: b.ctx, Table: tbl},
			Path:       v.Path,
			Table:      tbl,
			FieldsInfo: v.FieldsInfo,
			LinesInfo:  v.LinesInfo,
			Ctx:        b.ctx,
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
	return &ExplainExec{
		StmtPlan: v.StmtPlan,
		schema:   v.Schema(),
	}
}

func (b *executorBuilder) buildUnionScanExec(v *plan.PhysicalUnionScan) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	us := &UnionScanExec{ctx: b.ctx, Src: src, schema: v.Schema()}
	switch x := src.(type) {
	case *XSelectTableExec:
		us.desc = x.desc
		us.dirty = getDirtyDB(b.ctx).getDirtyTable(x.table.Meta().ID)
		us.conditions = v.Conditions
		us.buildAndSortAddedRows(x.table, x.asName)
	case *XSelectIndexExec:
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
		us.buildAndSortAddedRows(x.table, x.asName)
	default:
		// The mem table will not be written by sql directly, so we can omit the union scan to avoid err reporting.
		return src
	}
	return us
}

// buildMergeJoin builds SortMergeJoin executor.
// TODO: Refactor against different join strategies by extracting common code base
func (b *executorBuilder) buildMergeJoin(v *plan.PhysicalMergeJoin) Executor {
	joinBuilder := &joinBuilder{}
	exec, err := joinBuilder.Context(b.ctx).
		LeftChild(b.build(v.Children()[0])).
		RightChild(b.build(v.Children()[1])).
		EqualConditions(v.EqualConditions).
		LeftFilter(v.LeftConditions).
		RightFilter(v.RightConditions).
		OtherFilter(v.OtherConditions).
		Schema(v.Schema()).
		JoinType(v.JoinType).
		DefaultVals(v.DefaultValues).
		BuildMergeJoin(v.Desc)

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
	var targetTypes []*types.FieldType
	for _, eqCond := range v.EqualConditions {
		ln, _ := eqCond.GetArgs()[0].(*expression.Column)
		rn, _ := eqCond.GetArgs()[1].(*expression.Column)
		leftHashKey = append(leftHashKey, ln)
		rightHashKey = append(rightHashKey, rn)
		targetTypes = append(targetTypes, types.NewFieldType(types.MergeFieldType(ln.GetType().Tp, rn.GetType().Tp)))
	}
	e := &HashJoinExec{
		schema:        v.Schema(),
		otherFilter:   v.OtherConditions,
		prepared:      false,
		ctx:           b.ctx,
		targetTypes:   targetTypes,
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
	var targetTypes []*types.FieldType
	for _, eqCond := range v.EqualConditions {
		ln, _ := eqCond.GetArgs()[0].(*expression.Column)
		rn, _ := eqCond.GetArgs()[1].(*expression.Column)
		leftHashKey = append(leftHashKey, ln)
		rightHashKey = append(rightHashKey, rn)
		targetTypes = append(targetTypes, types.NewFieldType(types.MergeFieldType(ln.GetType().Tp, rn.GetType().Tp)))
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
		targetTypes:  targetTypes,
	}
	return e
}

func (b *executorBuilder) buildAggregation(v *plan.PhysicalAggregation) Executor {
	src := b.build(v.Children()[0])
	if v.AggType == plan.StreamedAgg {
		return &StreamAggExec{
			Src:          src,
			schema:       v.Schema(),
			StmtCtx:      b.ctx.GetSessionVars().StmtCtx,
			AggFuncs:     v.AggFuncs,
			GroupByItems: v.GroupByItems,
		}
	}
	return &HashAggExec{
		Src:          src,
		schema:       v.Schema(),
		sc:           b.ctx.GetSessionVars().StmtCtx,
		AggFuncs:     v.AggFuncs,
		GroupByItems: v.GroupByItems,
		aggType:      v.AggType,
		hasGby:       v.HasGby,
	}
}

func (b *executorBuilder) buildSelection(v *plan.Selection) Executor {
	exec := &SelectionExec{
		Src:            b.build(v.Children()[0]),
		schema:         v.Schema(),
		ctx:            b.ctx,
		scanController: v.ScanController,
		Conditions:     v.Conditions,
	}
	return exec
}

func (b *executorBuilder) buildProjection(v *plan.Projection) Executor {
	return &ProjectionExec{
		Src:    b.build(v.Children()[0]),
		ctx:    b.ctx,
		exprs:  v.Exprs,
		schema: v.Schema(),
	}
}

func (b *executorBuilder) buildTableDual(v *plan.TableDual) Executor {
	return &TableDualExec{schema: v.Schema(), rowCount: v.RowCount}
}

func (b *executorBuilder) getStartTS() uint64 {
	startTS := b.ctx.GetSessionVars().SnapshotTS
	if startTS == 0 {
		startTS = b.ctx.Txn().StartTS()
	}
	return startTS
}

func (b *executorBuilder) buildMemTable(v *plan.PhysicalMemTable) Executor {
	table, _ := b.is.TableByID(v.Table.ID)
	ts := &TableScanExec{
		t:            table,
		asName:       v.TableAsName,
		ctx:          b.ctx,
		columns:      v.Columns,
		schema:       v.Schema(),
		seekHandle:   math.MinInt64,
		ranges:       v.Ranges,
		isInfoSchema: strings.EqualFold(v.DBName.L, infoschema.Name),
	}
	return ts
}

func (b *executorBuilder) buildTableScan(v *plan.PhysicalTableScan) Executor {
	startTS := b.getStartTS()
	if b.err != nil {
		return nil
	}
	table, _ := b.is.TableByID(v.Table.ID)
	client := b.ctx.GetClient()
	supportDesc := client.SupportRequestType(kv.ReqTypeSelect, kv.ReqSubTypeDesc)
	e := &XSelectTableExec{
		tableInfo:   v.Table,
		ctx:         b.ctx,
		startTS:     startTS,
		supportDesc: supportDesc,
		asName:      v.TableAsName,
		table:       table,
		schema:      v.Schema(),
		Columns:     v.Columns,
		ranges:      v.Ranges,
		desc:        v.Desc,
		limitCount:  v.LimitCount,
		keepOrder:   v.KeepOrder,
		where:       v.TableConditionPBExpr,
		aggregate:   v.Aggregated,
		aggFuncs:    v.AggFuncsPB,
		byItems:     v.GbyItemsPB,
		orderByList: v.SortItemsPB,
	}
	return e
}

func (b *executorBuilder) buildIndexScan(v *plan.PhysicalIndexScan) Executor {
	startTS := b.getStartTS()
	if b.err != nil {
		return nil
	}
	table, _ := b.is.TableByID(v.Table.ID)
	client := b.ctx.GetClient()
	supportDesc := client.SupportRequestType(kv.ReqTypeIndex, kv.ReqSubTypeDesc)
	e := &XSelectIndexExec{
		tableInfo:            v.Table,
		ctx:                  b.ctx,
		supportDesc:          supportDesc,
		asName:               v.TableAsName,
		table:                table,
		singleReadMode:       !v.DoubleRead,
		startTS:              startTS,
		where:                v.TableConditionPBExpr,
		schema:               v.Schema(),
		ranges:               v.Ranges,
		limitCount:           v.LimitCount,
		sortItemsPB:          v.SortItemsPB,
		columns:              v.Columns,
		index:                v.Index,
		desc:                 v.Desc,
		outOfOrder:           v.OutOfOrder,
		indexConditionPBExpr: v.IndexConditionPBExpr,
		aggregate:            v.Aggregated,
		aggFuncs:             v.AggFuncsPB,
		byItems:              v.GbyItemsPB,
	}
	vars := b.ctx.GetSessionVars()
	if v.OutOfOrder {
		e.scanConcurrency = vars.DistSQLScanConcurrency
	} else {
		// The cost of index scan double-read is higher than single-read. Usually ordered index scan has a limit
		// which may not have been pushed down, so we set concurrency lower to avoid fetching unnecessary data.
		e.scanConcurrency = vars.IndexSerialScanConcurrency
	}
	if !e.aggregate && e.singleReadMode {
		// Single read index result has the schema of full index columns.
		schemaColumns := make([]*expression.Column, len(e.index.Columns))
		for i, col := range e.index.Columns {
			colInfo := e.tableInfo.Columns[col.Offset]
			schemaColumns[i] = &expression.Column{
				Index:   i,
				ColName: col.Name,
				RetType: &colInfo.FieldType,
			}
		}
		e.idxColsSchema = expression.NewSchema(schemaColumns...)
	}
	return e
}

func (b *executorBuilder) buildSort(v *plan.Sort) Executor {
	src := b.build(v.Children()[0])
	if v.ExecLimit != nil {
		return &TopnExec{
			SortExec: SortExec{
				Src:     src,
				ByItems: v.ByItems,
				ctx:     b.ctx,
				schema:  v.Schema()},
			limit: v.ExecLimit,
		}
	}
	return &SortExec{
		Src:     src,
		ByItems: v.ByItems,
		ctx:     b.ctx,
		schema:  v.Schema(),
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
		schema: v.Schema(),
		Src:    b.build(v.Children()[0]),
	}
}

func (b *executorBuilder) buildMaxOneRow(v *plan.MaxOneRow) Executor {
	return &MaxOneRowExec{
		schema: v.Schema(),
		Src:    b.build(v.Children()[0]),
	}
}

func (b *executorBuilder) buildUnion(v *plan.Union) Executor {
	e := &UnionExec{
		schema: v.Schema(),
		Srcs:   make([]Executor, len(v.Children())),
		ctx:    b.ctx,
	}
	for i, sel := range v.Children() {
		selExec := b.build(sel)
		e.Srcs[i] = selExec
	}
	return e
}

func (b *executorBuilder) buildUpdate(v *plan.Update) Executor {
	selExec := b.build(v.Children()[0])
	return &UpdateExec{ctx: b.ctx, SelectExec: selExec, OrderedList: v.OrderedList}
}

func (b *executorBuilder) buildDelete(v *plan.Delete) Executor {
	selExec := b.build(v.Children()[0])
	return &DeleteExec{
		ctx:          b.ctx,
		SelectExec:   selExec,
		Tables:       v.Tables,
		IsMultiTable: v.IsMultiTable,
	}
}

func (b *executorBuilder) buildCache(v *plan.Cache) Executor {
	src := b.build(v.Children()[0])
	return &CacheExec{
		schema: v.Schema(),
		Src:    src,
	}
}

func (b *executorBuilder) buildTableScanForAnalyze(tblInfo *model.TableInfo, cols []*model.ColumnInfo) Executor {
	startTS := b.getStartTS()
	if b.err != nil {
		return nil
	}
	table, _ := b.is.TableByID(tblInfo.ID)
	schema := expression.NewSchema(expression.ColumnInfos2Columns(tblInfo.Name, cols)...)
	ranges := []types.IntColumnRange{{math.MinInt64, math.MaxInt64}}
	e := &XSelectTableExec{
		tableInfo: tblInfo,
		ctx:       b.ctx,
		startTS:   startTS,
		table:     table,
		schema:    schema,
		Columns:   cols,
		ranges:    ranges,
	}
	return e
}

func (b *executorBuilder) buildIndexScanForAnalyze(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) Executor {
	startTS := b.getStartTS()
	if b.err != nil {
		return nil
	}
	table, _ := b.is.TableByID(tblInfo.ID)
	cols := make([]*model.ColumnInfo, len(idxInfo.Columns))
	for i, col := range idxInfo.Columns {
		cols[i] = tblInfo.Columns[col.Offset]
	}
	schema := expression.NewSchema(expression.ColumnInfos2Columns(tblInfo.Name, cols)...)
	idxRange := &types.IndexRange{LowVal: []types.Datum{types.MinNotNullDatum()}, HighVal: []types.Datum{types.MaxValueDatum()}}
	scanConcurrency := b.ctx.GetSessionVars().IndexSerialScanConcurrency
	e := &XSelectIndexExec{
		tableInfo:       tblInfo,
		ctx:             b.ctx,
		table:           table,
		singleReadMode:  true,
		startTS:         startTS,
		idxColsSchema:   schema,
		schema:          schema,
		ranges:          []*types.IndexRange{idxRange},
		columns:         cols,
		index:           idxInfo,
		outOfOrder:      false,
		scanConcurrency: scanConcurrency,
	}
	return e
}

func (b *executorBuilder) buildAnalyze(v *plan.Analyze) Executor {
	e := &AnalyzeExec{
		ctx:   b.ctx,
		tasks: make([]analyzeTask, 0, len(v.Children())),
	}
	for _, task := range v.PkTasks {
		e.tasks = append(e.tasks, analyzeTask{taskType: pkTask,
			src: b.buildTableScanForAnalyze(task.TableInfo, []*model.ColumnInfo{task.PKInfo})})
	}
	for _, task := range v.ColTasks {
		e.tasks = append(e.tasks, analyzeTask{taskType: colTask,
			src: b.buildTableScanForAnalyze(task.TableInfo, task.ColsInfo)})
	}
	for _, task := range v.IdxTasks {
		e.tasks = append(e.tasks, analyzeTask{taskType: idxTask,
			src: b.buildIndexScanForAnalyze(task.TableInfo, task.IndexInfo)})
	}
	return e
}

func (b *executorBuilder) buildTableReader(v *plan.PhysicalTableReader) Executor {
	dagReq := &tipb.DAGRequest{}
	dagReq.StartTs = b.getStartTS()
	dagReq.TimeZoneOffset = timeZoneOffset()
	sc := b.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = statementContextToFlags(sc)
	for _, p := range v.TablePlans {
		execPB, err := p.ToPB(b.ctx)
		if err != nil {
			b.err = errors.Trace(err)
			return nil
		}
		dagReq.Executors = append(dagReq.Executors, execPB)
	}

	ts := v.TablePlans[0].(*plan.PhysicalTableScan)
	table, _ := b.is.TableByID(ts.Table.ID)
	e := &TableReaderExecutor{
		ctx:       b.ctx,
		schema:    v.Schema(),
		dagPB:     dagReq,
		asName:    ts.TableAsName,
		tableID:   ts.Table.ID,
		table:     table,
		keepOrder: ts.KeepOrder,
		desc:      ts.Desc,
		ranges:    ts.Ranges,
	}

	for _, col := range v.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(col.Index))
	}

	b.err = e.doRequest()
	return e
}

func (b *executorBuilder) buildIndexReader(v *plan.PhysicalIndexReader) Executor {
	dagReq := &tipb.DAGRequest{}
	dagReq.StartTs = b.getStartTS()
	dagReq.TimeZoneOffset = timeZoneOffset()
	sc := b.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = statementContextToFlags(sc)
	for _, p := range v.IndexPlans {
		execPB, err := p.ToPB(b.ctx)
		if err != nil {
			b.err = errors.Trace(err)
			return nil
		}
		dagReq.Executors = append(dagReq.Executors, execPB)
	}

	is := v.IndexPlans[0].(*plan.PhysicalIndexScan)
	table, _ := b.is.TableByID(is.Table.ID)
	e := &IndexReaderExecutor{
		ctx:       b.ctx,
		schema:    v.Schema(),
		dagPB:     dagReq,
		asName:    is.TableAsName,
		tableID:   is.Table.ID,
		table:     table,
		index:     is.Index,
		keepOrder: !is.OutOfOrder,
		desc:      is.Desc,
		ranges:    is.Ranges,
	}

	for _, col := range v.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(col.Index))
	}

	b.err = e.doRequest()
	return e
}
