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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/autocommit"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

// executorBuilder builds an Executor from a Plan.
// The InfoSchema must be the same one used in InfoBinder.
type executorBuilder struct {
	ctx context.Context
	is  infoschema.InfoSchema
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
	case *plan.Filter:
		src := b.build(v.GetChildByIndex(0))
		return b.buildFilter(src, v.Conditions)
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
		return b.buildTableScan(v, nil)
	case *plan.PhysicalIndexScan:
		return b.buildIndexScan(v, nil)
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

func (b *executorBuilder) buildFilter(src Executor, conditions []ast.ExprNode) Executor {
	if len(conditions) == 0 {
		return src
	}
	return &FilterExec{
		Src:       src,
		Condition: b.joinConditions(conditions),
		ctx:       b.ctx,
	}
}

func (b *executorBuilder) buildShowDDL(v *plan.ShowDDL) Executor {
	return &ShowDDLExec{
		fields: v.Fields(),
		ctx:    b.ctx,
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

func (b *executorBuilder) joinConditions(conditions []ast.ExprNode) ast.ExprNode {
	if len(conditions) == 0 {
		return nil
	}
	if len(conditions) == 1 {
		return conditions[0]
	}
	condition := &ast.BinaryOperationExpr{
		Op: opcode.AndAnd,
		L:  conditions[0],
		R:  b.joinConditions(conditions[1:]),
	}
	ast.MergeChildrenFlags(condition, condition.L, condition.R)
	return condition
}

func (b *executorBuilder) buildSelectLock(v *plan.SelectLock) Executor {
	src := b.build(v.GetChildByIndex(0))
	ac, err := autocommit.ShouldAutocommit(b.ctx)
	if err != nil {
		b.err = errors.Trace(err)
		return src
	}
	if ac {
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
	if x, ok := src.(XExecutor); ok {
		if x.AddLimit(v) && v.Offset == 0 {
			return src
		}
	}
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
		fields:      v.Fields(),
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
	if v.SelectPlan != nil {
		ivs.SelectExec = b.build(v.SelectPlan)
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

func (b *executorBuilder) buildUnionScanExec(src Executor, condition expression.Expression) *UnionScanExec {
	us := &UnionScanExec{ctx: b.ctx, Src: src}
	switch x := src.(type) {
	case *XSelectTableExec:
		us.desc = x.desc
		us.dirty = getDirtyDB(b.ctx).getDirtyTable(x.table.Meta().ID)
		us.condition = condition
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
		us.condition = condition
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
		schema:      v.GetSchema(),
		otherFilter: expression.ComposeCNFCondition(v.OtherConditions),
		prepared:    false,
		ctx:         b.ctx,
		targetTypes: targetTypes,
		concurrency: v.Concurrency,
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
			ctx.bigFilter = e.bigFilter.DeepCopy()
		}
		if e.otherFilter != nil {
			ctx.otherFilter = e.otherFilter.DeepCopy()
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
		withAux:      v.WithAux,
		anti:         v.Anti,
		targetTypes:  targetTypes,
	}
	return e
}

func (b *executorBuilder) buildAggregation(v *plan.PhysicalAggregation) Executor {
	src := b.build(v.GetChildByIndex(0))
	e := &AggregationExec{
		Src:          src,
		schema:       v.GetSchema(),
		ctx:          b.ctx,
		AggFuncs:     v.AggFuncs,
		GroupByItems: v.GroupByItems,
	}
	// Check if the underlying is xapi executor, we should try to push aggregate function down.
	xSrc, ok := src.(XExecutor)
	if !ok {
		return e
	}
	txn, err := b.ctx.GetTxn(false)
	if err != nil {
		b.err = err
		return nil
	}
	client := txn.GetClient()
	if len(v.GroupByItems) > 0 && !client.SupportRequestType(kv.ReqTypeSelect, kv.ReqSubTypeGroupBy) {
		return e
	}
	// Convert aggregate function exprs to pb.
	pbAggFuncs := make([]*tipb.Expr, 0, len(v.AggFuncs))
	for _, af := range v.AggFuncs {
		if af.IsDistinct() {
			// We do not support distinct push down.
			return e
		}
		pbAggFunc := b.AggFuncToPBExpr(client, af, xSrc.GetTable())
		if pbAggFunc == nil {
			return e
		}
		pbAggFuncs = append(pbAggFuncs, pbAggFunc)
	}
	pbByItems := make([]*tipb.ByItem, 0, len(v.GroupByItems))
	// Convert groupby to pb
	for _, item := range v.GroupByItems {
		pbByItem := b.GroupByItemToPB(client, item, xSrc.GetTable())
		if pbByItem == nil {
			return e
		}
		pbByItems = append(pbByItems, pbByItem)
	}
	// compose aggregate info
	// We should infer fields type.
	// Each agg item will be splitted into two datums: count and value
	// The first field should be group key.
	fields := make([]*types.FieldType, 0, 1+2*len(v.AggFuncs))
	gk := types.NewFieldType(mysql.TypeBlob)
	gk.Charset = charset.CharsetBin
	gk.Collate = charset.CollationBin
	fields = append(fields, gk)
	// There will be one or two fields in the result row for each AggregateFuncExpr.
	// Count needs count partial result field.
	// Sum, FirstRow, Max, Min, GroupConcat need value partial result field.
	// Avg needs both count and value partial result field.
	for i, agg := range v.AggFuncs {
		name := strings.ToLower(agg.GetName())
		if needCount(name) {
			// count partial result field
			ft := types.NewFieldType(mysql.TypeLonglong)
			ft.Flen = 21
			ft.Charset = charset.CharsetBin
			ft.Collate = charset.CollationBin
			fields = append(fields, ft)
		}
		if needValue(name) {
			// value partial result field
			col := v.GetSchema()[i]
			fields = append(fields, col.GetType())
		}
	}
	xSrc.AddAggregate(pbAggFuncs, pbByItems, fields)
	hasGroupBy := len(v.GroupByItems) > 0
	xe := &XAggregateExec{
		Src:        src,
		ctx:        b.ctx,
		AggFuncs:   v.AggFuncs,
		hasGroupBy: hasGroupBy,
		schema:     v.GetSchema(),
	}
	log.Debugf("Use XAggregateExec with %d aggs", len(v.AggFuncs))
	return xe
}

func (b *executorBuilder) buildSelection(v *plan.Selection) Executor {
	child := v.GetChildByIndex(0)
	oldConditions := v.Conditions
	var src Executor
	switch x := child.(type) {
	case *plan.PhysicalTableScan:
		if x.LimitCount == nil {
			src = b.buildTableScan(x, v)
		} else {
			src = b.buildTableScan(x, nil)
		}
	case *plan.PhysicalIndexScan:
		if x.LimitCount == nil {
			src = b.buildIndexScan(x, v)
		} else {
			src = b.buildIndexScan(x, nil)
		}
	default:
		src = b.build(x)
	}

	if len(v.Conditions) == 0 {
		v.Conditions = oldConditions
		return src
	}

	exec := &SelectionExec{
		Src:       src,
		Condition: expression.ComposeCNFCondition(v.Conditions),
		schema:    v.GetSchema(),
		ctx:       b.ctx,
	}
	copy(v.Conditions, oldConditions)
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

func (b *executorBuilder) buildTableScan(v *plan.PhysicalTableScan, s *plan.Selection) Executor {
	txn, err := b.ctx.GetTxn(false)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	table, _ := b.is.TableByID(v.Table.ID)
	client := txn.GetClient()
	var memDB bool
	switch v.DBName.L {
	case "information_schema", "performance_schema":
		memDB = true
	}
	supportDesc := client.SupportRequestType(kv.ReqTypeSelect, kv.ReqSubTypeDesc)
	if !memDB && client.SupportRequestType(kv.ReqTypeSelect, 0) {
		var ret Executor
		st := &XSelectTableExec{
			tableInfo:   v.Table,
			ctx:         b.ctx,
			txn:         txn,
			supportDesc: supportDesc,
			asName:      v.TableAsName,
			table:       table,
			schema:      v.GetSchema(),
			Columns:     v.Columns,
			ranges:      v.Ranges,
			desc:        v.Desc,
			limitCount:  v.LimitCount,
			keepOrder:   v.KeepOrder,
		}
		ret = st
		if !txn.IsReadOnly() {
			if s != nil {
				ret = b.buildUnionScanExec(ret,
					expression.ComposeCNFCondition(append(s.Conditions, v.AccessCondition...)))
			} else {
				ret = b.buildUnionScanExec(ret, expression.ComposeCNFCondition(v.AccessCondition))
			}
		}
		if s != nil {
			st.where, s.Conditions = b.toPBExpr(s.Conditions, st.tableInfo)
		}
		return ret
	}

	ts := &TableScanExec{
		t:          table,
		asName:     v.TableAsName,
		ctx:        b.ctx,
		columns:    v.Columns,
		schema:     v.GetSchema(),
		seekHandle: math.MinInt64,
		ranges:     v.Ranges,
	}
	if v.Desc {
		return &ReverseExec{Src: ts}
	}
	return ts
}

func (b *executorBuilder) buildIndexScan(v *plan.PhysicalIndexScan, s *plan.Selection) Executor {
	txn, err := b.ctx.GetTxn(false)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	table, _ := b.is.TableByID(v.Table.ID)
	client := txn.GetClient()
	var memDB bool
	switch v.DBName.L {
	case "information_schema", "performance_schema":
		memDB = true
	}
	supportDesc := client.SupportRequestType(kv.ReqTypeIndex, kv.ReqSubTypeDesc)
	if !memDB && client.SupportRequestType(kv.ReqTypeIndex, 0) {
		var ret Executor
		st := &XSelectIndexExec{
			tableInfo:   v.Table,
			ctx:         b.ctx,
			supportDesc: supportDesc,
			asName:      v.TableAsName,
			table:       table,
			indexPlan:   v,
			txn:         txn,
		}
		ret = st
		if !txn.IsReadOnly() {
			if s != nil {
				ret = b.buildUnionScanExec(ret,
					expression.ComposeCNFCondition(append(s.Conditions, v.AccessCondition...)))
			} else {
				ret = b.buildUnionScanExec(ret, expression.ComposeCNFCondition(v.AccessCondition))
			}
		}
		// It will forbid limit and aggregation to push down.
		if s != nil {
			st.where, s.Conditions = b.toPBExpr(s.Conditions, st.tableInfo)
		}
		return ret
	}

	b.err = errors.New("Not implement yet.")
	return nil
}

func (b *executorBuilder) buildSort(v *plan.Sort) Executor {
	src := b.build(v.GetChildByIndex(0))
	return &SortExec{
		Src:     src,
		ByItems: v.ByItems,
		ctx:     b.ctx,
		schema:  v.GetSchema(),
		Limit:   v.ExecLimit,
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
		fields: v.Fields(),
		Srcs:   make([]Executor, len(v.GetChildren())),
	}
	for i, sel := range v.GetChildren() {
		selExec := b.build(sel)
		e.Srcs[i] = selExec
	}
	return e
}

func (b *executorBuilder) buildUpdate(v *plan.Update) Executor {
	selExec := b.build(v.SelectPlan)
	return &UpdateExec{ctx: b.ctx, SelectExec: selExec, OrderedList: v.OrderedList}
}

func (b *executorBuilder) buildDummyScan(v *plan.PhysicalDummyScan) Executor {
	return &DummyScanExec{
		schema: v.GetSchema(),
	}
}

func (b *executorBuilder) buildDelete(v *plan.Delete) Executor {
	selExec := b.build(v.SelectPlan)
	return &DeleteExec{
		ctx:          b.ctx,
		SelectExec:   selExec,
		Tables:       v.Tables,
		IsMultiTable: v.IsMultiTable,
	}
}
