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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/autocommit"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

// executorBuilder builds an Executor from a Plan.
// The InfoSchema must not change during execution.
type executorBuilder struct {
	ctx context.Context
	is  infoschema.InfoSchema
	// If there is any error during Executor building process, err is set.
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
	case *plan.Distinct:
		return b.buildDistinct(v)
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
	case *plan.Sort:
		return b.buildSort(v)
	case *plan.Union:
		return b.buildUnion(v)
	case *plan.Update:
		return b.buildUpdate(v)
	case *plan.PhysicalUnionScan:
		return b.buildUnionScanExec(v)
	case *plan.PhysicalHashJoin:
		return b.buildJoin(v)
	case *plan.PhysicalHashSemiJoin:
		return b.buildSemiJoin(v)
	case *plan.Selection:
		return b.buildSelection(v)
	case *plan.PhysicalAggregation:
		return b.buildAggregation(v)
	case *plan.Projection:
		return b.buildProjection(v)
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
	case *plan.Trim:
		return b.buildTrim(v)
	case *plan.PhysicalDummyScan:
		return b.buildDummyScan(v)
	default:
		b.err = ErrUnknownPlan.Gen("Unknown Plan %T", p)
		return nil
	}
}

func (b *executorBuilder) buildShowDDL(v *plan.ShowDDL) Executor {
	return &ShowDDLExec{
		ctx:    b.ctx,
		schema: v.GetSchema(),
	}
}

func (b *executorBuilder) buildCheckTable(v *plan.CheckTable) Executor {
	return &CheckTableExec{
		tables: v.Tables,
		ctx:    b.ctx,
	}
}

func (b *executorBuilder) buildDeallocate(v *plan.Deallocate) Executor {
	return &DeallocateExec{
		ctx:  b.ctx,
		Name: v.Name,
	}
}

func (b *executorBuilder) buildSelectLock(v *plan.SelectLock) Executor {
	src := b.build(v.GetChildByIndex(0))
	if autocommit.ShouldAutocommit(b.ctx) {
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
		schema: v.GetSchema(),
	}
	return e
}

func (b *executorBuilder) buildLimit(v *plan.Limit) Executor {
	src := b.build(v.GetChildByIndex(0))
	e := &LimitExec{
		Src:    src,
		Offset: v.Offset,
		Count:  v.Count,
		schema: v.GetSchema(),
	}
	return e
}

func (b *executorBuilder) buildDistinct(v *plan.Distinct) Executor {
	return &DistinctExec{Src: b.build(v.GetChildByIndex(0)), schema: v.GetSchema()}
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
		ID:        v.ID,
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
		schema:      v.GetSchema(),
	}
	if e.Tp == ast.ShowGrants && len(e.User) == 0 {
		e.User = variable.GetSessionVars(e.ctx).User
	}
	return e
}

func (b *executorBuilder) buildSimple(v *plan.Simple) Executor {
	switch s := v.Statement.(type) {
	case *ast.GrantStmt:
		return b.buildGrant(s)
	}
	return &SimpleExec{Statement: v.Statement, ctx: b.ctx}
}

func (b *executorBuilder) buildInsert(v *plan.Insert) Executor {
	ivs := &InsertValues{
		ctx:     b.ctx,
		Columns: v.Columns,
		Lists:   v.Lists,
		Setlist: v.Setlist,
	}
	if len(v.GetChildren()) > 0 {
		ivs.SelectExec = b.build(v.GetChildByIndex(0))
	}
	// Get Table
	ts, ok := v.Table.TableRefs.Left.(*ast.TableSource)
	if !ok {
		b.err = errors.New("Can not get table")
		return nil
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		b.err = errors.New("Can not get table")
		return nil
	}
	tableInfo := tn.TableInfo
	tbl, ok := b.is.TableByID(tableInfo.ID)
	if !ok {
		b.err = errors.Errorf("Can not get table %d", tableInfo.ID)
		return nil
	}
	ivs.Table = tbl
	if v.IsReplace {
		return b.buildReplace(ivs)
	}
	insert := &InsertExec{
		InsertValues: ivs,
		OnDuplicate:  v.OnDuplicate,
		Priority:     v.Priority,
		Ignore:       v.Ignore,
	}
	// fields is used to evaluate values expr.
	insert.fields = ts.GetResultFields()
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
	}
}

func (b *executorBuilder) buildDDL(v *plan.DDL) Executor {
	return &DDLExec{Statement: v.Statement, ctx: b.ctx, is: b.is}
}

func (b *executorBuilder) buildExplain(v *plan.Explain) Executor {
	return &ExplainExec{
		StmtPlan: v.StmtPlan,
		schema:   v.GetSchema(),
	}
}

func (b *executorBuilder) buildUnionScanExec(v *plan.PhysicalUnionScan) *UnionScanExec {
	src := b.build(v.GetChildByIndex(0))
	if b.err != nil {
		return nil
	}
	us := &UnionScanExec{ctx: b.ctx, Src: src, schema: v.GetSchema()}
	switch x := src.(type) {
	case *XSelectTableExec:
		us.desc = x.desc
		us.dirty = getDirtyDB(b.ctx).getDirtyTable(x.table.Meta().ID)
		us.condition = v.Condition
		us.buildAndSortAddedRows(x.table, x.asName)
	case *XSelectIndexExec:
		us.desc = x.indexPlan.Desc
		for _, ic := range x.indexPlan.Index.Columns {
			for i, col := range x.indexPlan.GetSchema() {
				if col.ColName.L == ic.Name.L {
					us.usedIndex = append(us.usedIndex, i)
					break
				}
			}
		}
		us.dirty = getDirtyDB(b.ctx).getDirtyTable(x.table.Meta().ID)
		us.condition = v.Condition
		us.buildAndSortAddedRows(x.table, x.asName)
	default:
		b.err = ErrUnknownPlan.Gen("Unknown Plan %T", src)
	}
	return us
}

func (b *executorBuilder) buildJoin(v *plan.PhysicalHashJoin) Executor {
	var leftHashKey, rightHashKey []*expression.Column
	var targetTypes []*types.FieldType
	for _, eqCond := range v.EqualConditions {
		ln, _ := eqCond.Args[0].(*expression.Column)
		rn, _ := eqCond.Args[1].(*expression.Column)
		leftHashKey = append(leftHashKey, ln)
		rightHashKey = append(rightHashKey, rn)
		targetTypes = append(targetTypes, types.NewFieldType(types.MergeFieldType(ln.GetType().Tp, rn.GetType().Tp)))
	}
	e := &HashJoinExec{
		schema:        v.GetSchema(),
		otherFilter:   expression.ComposeCNFCondition(v.OtherConditions),
		prepared:      false,
		ctx:           b.ctx,
		targetTypes:   targetTypes,
		concurrency:   v.Concurrency,
		defaultValues: v.DefaultValues,
	}
	if v.SmallTable == 1 {
		e.smallFilter = expression.ComposeCNFCondition(v.RightConditions)
		e.bigFilter = expression.ComposeCNFCondition(v.LeftConditions)
		e.smallHashKey = rightHashKey
		e.bigHashKey = leftHashKey
		e.leftSmall = false
	} else {
		e.leftSmall = true
		e.smallFilter = expression.ComposeCNFCondition(v.LeftConditions)
		e.bigFilter = expression.ComposeCNFCondition(v.RightConditions)
		e.smallHashKey = leftHashKey
		e.bigHashKey = rightHashKey
	}
	if v.JoinType == plan.LeftOuterJoin || v.JoinType == plan.RightOuterJoin {
		e.outer = true
	}
	if e.leftSmall {
		e.smallExec = b.build(v.GetChildByIndex(0))
		e.bigExec = b.build(v.GetChildByIndex(1))
	} else {
		e.smallExec = b.build(v.GetChildByIndex(1))
		e.bigExec = b.build(v.GetChildByIndex(0))
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

func (b *executorBuilder) buildSemiJoin(v *plan.PhysicalHashSemiJoin) Executor {
	var leftHashKey, rightHashKey []*expression.Column
	var targetTypes []*types.FieldType
	for _, eqCond := range v.EqualConditions {
		ln, _ := eqCond.Args[0].(*expression.Column)
		rn, _ := eqCond.Args[1].(*expression.Column)
		leftHashKey = append(leftHashKey, ln)
		rightHashKey = append(rightHashKey, rn)
		targetTypes = append(targetTypes, types.NewFieldType(types.MergeFieldType(ln.GetType().Tp, rn.GetType().Tp)))
	}
	e := &HashSemiJoinExec{
		schema:       v.GetSchema(),
		otherFilter:  expression.ComposeCNFCondition(v.OtherConditions),
		bigFilter:    expression.ComposeCNFCondition(v.LeftConditions),
		smallFilter:  expression.ComposeCNFCondition(v.RightConditions),
		bigExec:      b.build(v.GetChildByIndex(0)),
		smallExec:    b.build(v.GetChildByIndex(1)),
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
	src := b.build(v.GetChildByIndex(0))
	if v.AggType == plan.StreamedAgg {
		return &StreamAggExec{
			Src:          src,
			schema:       v.GetSchema(),
			ctx:          b.ctx,
			AggFuncs:     v.AggFuncs,
			GroupByItems: v.GroupByItems,
		}
	}
	return &HashAggExec{
		Src:          src,
		schema:       v.GetSchema(),
		ctx:          b.ctx,
		AggFuncs:     v.AggFuncs,
		GroupByItems: v.GroupByItems,
		aggType:      v.AggType,
		hasGby:       v.HasGby,
	}
}

func (b *executorBuilder) buildSelection(v *plan.Selection) Executor {
	exec := &SelectionExec{
		Src:       b.build(v.GetChildByIndex(0)),
		Condition: expression.ComposeCNFCondition(v.Conditions),
		schema:    v.GetSchema(),
		ctx:       b.ctx,
	}
	return exec
}

func (b *executorBuilder) buildProjection(v *plan.Projection) Executor {
	return &ProjectionExec{
		Src:    b.build(v.GetChildByIndex(0)),
		ctx:    b.ctx,
		exprs:  v.Exprs,
		schema: v.GetSchema(),
	}
}

func (b *executorBuilder) buildTableDual(v *plan.TableDual) Executor {
	return &TableDualExec{schema: v.GetSchema()}
}

func (b *executorBuilder) getStartTS() uint64 {
	startTS := variable.GetSnapshotTS(b.ctx)
	if startTS == 0 {
		txn, err := b.ctx.GetTxn(false)
		if err != nil {
			b.err = errors.Trace(err)
			return 0
		}
		startTS = txn.StartTS()
	}
	return startTS
}

func (b *executorBuilder) buildTableScan(v *plan.PhysicalTableScan) Executor {
	startTS := b.getStartTS()
	if b.err != nil {
		return nil
	}
	table, _ := b.is.TableByID(v.Table.ID)
	client := b.ctx.GetClient()
	memDB := infoschema.IsMemoryDB(v.DBName.L)
	supportDesc := client.SupportRequestType(kv.ReqTypeSelect, kv.ReqSubTypeDesc)
	if !memDB && client.SupportRequestType(kv.ReqTypeSelect, 0) {
		st := &XSelectTableExec{
			tableInfo:   v.Table,
			ctx:         b.ctx,
			startTS:     startTS,
			supportDesc: supportDesc,
			asName:      v.TableAsName,
			table:       table,
			schema:      v.GetSchema(),
			Columns:     v.Columns,
			ranges:      v.Ranges,
			desc:        v.Desc,
			limitCount:  v.LimitCount,
			keepOrder:   v.KeepOrder,
			where:       v.ConditionPBExpr,
			aggregate:   v.Aggregated,
			aggFuncs:    v.AggFuncsPB,
			aggFields:   v.AggFields,
			byItems:     v.GbyItemsPB,
			orderByList: v.SortItemsPB,
		}
		st.scanConcurrency, b.err = getScanConcurrency(b.ctx)
		return st
	}

	ts := &TableScanExec{
		t:            table,
		asName:       v.TableAsName,
		ctx:          b.ctx,
		columns:      v.Columns,
		schema:       v.GetSchema(),
		seekHandle:   math.MinInt64,
		ranges:       v.Ranges,
		isInfoSchema: strings.EqualFold(v.DBName.L, infoschema.Name),
	}
	if v.Desc {
		return &ReverseExec{Src: ts}
	}
	return ts
}

func (b *executorBuilder) buildIndexScan(v *plan.PhysicalIndexScan) Executor {
	startTS := b.getStartTS()
	if b.err != nil {
		return nil
	}
	table, _ := b.is.TableByID(v.Table.ID)
	client := b.ctx.GetClient()
	memDB := infoschema.IsMemoryDB(v.DBName.L)
	supportDesc := client.SupportRequestType(kv.ReqTypeIndex, kv.ReqSubTypeDesc)
	if !memDB && client.SupportRequestType(kv.ReqTypeIndex, 0) {
		st := &XSelectIndexExec{
			tableInfo:      v.Table,
			ctx:            b.ctx,
			supportDesc:    supportDesc,
			asName:         v.TableAsName,
			table:          table,
			indexPlan:      v,
			singleReadMode: !v.DoubleRead,
			startTS:        startTS,
			where:          v.ConditionPBExpr,
			aggregate:      v.Aggregated,
			aggFuncs:       v.AggFuncsPB,
			aggFields:      v.AggFields,
			byItems:        v.GbyItemsPB,
		}
		st.scanConcurrency, b.err = getScanConcurrency(b.ctx)
		return st
	}
	b.err = errors.New("not implement yet")
	return nil
}

func (b *executorBuilder) buildSort(v *plan.Sort) Executor {
	src := b.build(v.GetChildByIndex(0))
	if v.ExecLimit != nil {
		return &TopnExec{
			SortExec: SortExec{
				Src:     src,
				ByItems: v.ByItems,
				ctx:     b.ctx,
				schema:  v.GetSchema()},
			limit: v.ExecLimit,
		}
	}
	return &SortExec{
		Src:     src,
		ByItems: v.ByItems,
		ctx:     b.ctx,
		schema:  v.GetSchema(),
	}
}

func (b *executorBuilder) buildApply(v *plan.PhysicalApply) Executor {
	src := b.build(v.GetChildByIndex(0))
	apply := &ApplyExec{
		schema:      v.GetSchema(),
		innerExec:   b.build(v.InnerPlan),
		outerSchema: v.OuterSchema,
		Src:         src,
	}
	if v.Checker != nil {
		apply.checker = &conditionChecker{
			all:     v.Checker.All,
			cond:    v.Checker.Condition,
			trimLen: len(src.Schema()),
			ctx:     b.ctx,
		}
	}
	return apply
}

func (b *executorBuilder) buildExists(v *plan.Exists) Executor {
	return &ExistsExec{
		schema: v.GetSchema(),
		Src:    b.build(v.GetChildByIndex(0)),
	}
}

func (b *executorBuilder) buildMaxOneRow(v *plan.MaxOneRow) Executor {
	return &MaxOneRowExec{
		schema: v.GetSchema(),
		Src:    b.build(v.GetChildByIndex(0)),
	}
}

func (b *executorBuilder) buildTrim(v *plan.Trim) Executor {
	return &TrimExec{
		schema: v.GetSchema(),
		Src:    b.build(v.GetChildByIndex(0)),
		len:    len(v.GetSchema()),
	}
}

func (b *executorBuilder) buildUnion(v *plan.Union) Executor {
	e := &UnionExec{
		schema: v.GetSchema(),
		Srcs:   make([]Executor, len(v.GetChildren())),
	}
	for i, sel := range v.GetChildren() {
		selExec := b.build(sel)
		e.Srcs[i] = selExec
	}
	return e
}

func (b *executorBuilder) buildUpdate(v *plan.Update) Executor {
	selExec := b.build(v.GetChildByIndex(0))
	return &UpdateExec{ctx: b.ctx, SelectExec: selExec, OrderedList: v.OrderedList}
}

func (b *executorBuilder) buildDummyScan(v *plan.PhysicalDummyScan) Executor {
	return &DummyScanExec{
		schema: v.GetSchema(),
	}
}

func (b *executorBuilder) buildDelete(v *plan.Delete) Executor {
	selExec := b.build(v.GetChildByIndex(0))
	return &DeleteExec{
		ctx:          b.ctx,
		SelectExec:   selExec,
		Tables:       v.Tables,
		IsMultiTable: v.IsMultiTable,
	}
}
