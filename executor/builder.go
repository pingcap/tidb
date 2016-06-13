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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/autocommit"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
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
	case *plan.Aggregate:
		return b.buildAggregate(v)
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
	case *plan.Having:
		return b.buildHaving(v)
	case *plan.IndexScan:
		return b.buildIndexScan(v)
	case *plan.Insert:
		return b.buildInsert(v)
	case *plan.JoinInner:
		return b.buildJoinInner(v)
	case *plan.JoinOuter:
		return b.buildJoinOuter(v)
	case *plan.Limit:
		return b.buildLimit(v)
	case *plan.Prepare:
		return b.buildPrepare(v)
	case *plan.SelectFields:
		return b.buildSelectFields(v)
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
	case *plan.NewSort:
		return b.buildNewSort(v)
	case *plan.TableDual:
		return b.buildTableDual(v)
	case *plan.TableScan:
		return b.buildTableScan(v)
	case *plan.Union:
		return b.buildUnion(v)
	case *plan.Update:
		return b.buildUpdate(v)
	case *plan.Join:
		return b.buildJoin(v)
	case *plan.Selection:
		return b.buildSelection(v)
	case *plan.Aggregation:
		return b.buildAggregation(v)
	case *plan.Projection:
		return b.buildProjection(v)
	case *plan.NewTableScan:
		return b.buildNewTableScan(v)
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

func (b *executorBuilder) buildTableDual(v *plan.TableDual) Executor {
	e := &TableDualExec{fields: v.Fields()}
	return b.buildFilter(e, v.FilterConditions)
}

func (b *executorBuilder) buildTableScan(v *plan.TableScan) Executor {
	txn, err := b.ctx.GetTxn(false)
	if err != nil {
		b.err = err
		return nil
	}
	table, _ := b.is.TableByID(v.Table.ID)
	client := txn.GetClient()
	var memDB bool
	switch v.Fields()[0].DBName.L {
	case "information_schema", "performance_schema":
		memDB = true
	}
	supportDesc := client.SupportRequestType(kv.ReqTypeSelect, kv.ReqSubTypeDesc)
	if !memDB && client.SupportRequestType(kv.ReqTypeSelect, 0) {
		log.Debug("xapi select table")
		e := &XSelectTableExec{
			table:       table,
			ctx:         b.ctx,
			tablePlan:   v,
			supportDesc: supportDesc,
		}
		where, remained := b.conditionsToPBExpr(client, v.FilterConditions, v.TableName)
		if where != nil {
			e.where = where
		}
		if len(remained) == 0 {
			e.allFiltersPushed = true
		}
		var ex Executor
		if txn.IsReadOnly() {
			ex = e
		} else {
			ex = b.buildUnionScanExec(e)
		}
		return b.buildFilter(ex, remained)
	}

	e := &TableScanExec{
		t:           table,
		tableAsName: v.TableAsName,
		fields:      v.Fields(),
		ctx:         b.ctx,
		ranges:      v.Ranges,
		seekHandle:  math.MinInt64,
	}
	x := b.buildFilter(e, v.FilterConditions)
	if v.Desc {
		x = &ReverseExec{Src: x}
	}
	return x
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

func (b *executorBuilder) buildIndexScan(v *plan.IndexScan) Executor {
	txn, err := b.ctx.GetTxn(false)
	if err != nil {
		b.err = err
		return nil
	}
	tbl, _ := b.is.TableByID(v.Table.ID)
	client := txn.GetClient()
	supportDesc := client.SupportRequestType(kv.ReqTypeIndex, kv.ReqSubTypeDesc)
	var memDB bool
	switch v.Fields()[0].DBName.L {
	case "information_schema", "performance_schema":
		memDB = true
	}
	if !memDB && client.SupportRequestType(kv.ReqTypeIndex, 0) {
		log.Debug("xapi select index")
		e := &XSelectIndexExec{
			table:       tbl,
			ctx:         b.ctx,
			indexPlan:   v,
			supportDesc: supportDesc,
		}
		where, remained := b.conditionsToPBExpr(client, v.FilterConditions, v.TableName)
		if where != nil {
			e.where = where
		}
		var ex Executor
		if txn.IsReadOnly() {
			ex = e
		} else {
			ex = b.buildUnionScanExec(e)
		}
		return b.buildFilter(ex, remained)
	}

	var idx *table.IndexedColumn
	for _, val := range tbl.Indices() {
		if val.IndexInfo.Name.L == v.Index.Name.L {
			idx = val
			break
		}
	}
	e := &IndexScanExec{
		tbl:         tbl,
		tableAsName: v.TableAsName,
		idx:         idx,
		fields:      v.Fields(),
		ctx:         b.ctx,
		Desc:        v.Desc,
		valueTypes:  make([]*types.FieldType, len(idx.Columns)),
	}

	for i, ic := range idx.Columns {
		col := tbl.Cols()[ic.Offset]
		e.valueTypes[i] = &col.FieldType
	}

	e.Ranges = make([]*IndexRangeExec, len(v.Ranges))
	for i, val := range v.Ranges {
		e.Ranges[i] = b.buildIndexRange(e, val)
	}
	x := b.buildFilter(e, v.FilterConditions)
	if v.Desc {
		x = &ReverseExec{Src: x}
	}
	return x
}

func (b *executorBuilder) buildIndexRange(scan *IndexScanExec, v *plan.IndexRange) *IndexRangeExec {
	ran := &IndexRangeExec{
		scan:        scan,
		lowVals:     v.LowVal,
		lowExclude:  v.LowExclude,
		highVals:    v.HighVal,
		highExclude: v.HighExclude,
	}
	return ran
}

func (b *executorBuilder) buildJoinOuter(v *plan.JoinOuter) *JoinOuterExec {
	e := &JoinOuterExec{
		OuterExec: b.build(v.Outer),
		InnerPlan: v.Inner,
		fields:    v.Fields(),
		builder:   b,
	}
	return e
}

func (b *executorBuilder) buildJoinInner(v *plan.JoinInner) *JoinInnerExec {
	e := &JoinInnerExec{
		InnerPlans: v.Inners,
		innerExecs: make([]Executor, len(v.Inners)),
		Condition:  b.joinConditions(v.Conditions),
		fields:     v.Fields(),
		ctx:        b.ctx,
		builder:    b,
	}
	return e
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
	if autocommit.ShouldAutocommit(b.ctx) {
		// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
		// is disabled (either by beginning transaction with START TRANSACTION or by setting
		// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
		// See: https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
		return src
	}
	e := &SelectLockExec{
		Src:  src,
		Lock: v.Lock,
		ctx:  b.ctx,
	}
	return e
}

func (b *executorBuilder) buildSelectFields(v *plan.SelectFields) Executor {
	src := b.build(v.GetChildByIndex(0))
	e := &SelectFieldsExec{
		Src:          src,
		ResultFields: v.Fields(),
		ctx:          b.ctx,
	}
	return e
}

func (b *executorBuilder) buildAggregate(v *plan.Aggregate) Executor {
	src := b.build(v.GetChildByIndex(0))
	e := &AggregateExec{
		Src:          src,
		ResultFields: v.Fields(),
		ctx:          b.ctx,
		AggFuncs:     v.AggFuncs,
		GroupByItems: v.GroupByItems,
	}
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
	log.Debugf("Use XAggregateExec with %d aggs", len(v.AggFuncs))
	// Convert aggregate function exprs to pb.
	pbAggFuncs := make([]*tipb.Expr, 0, len(v.AggFuncs))
	for _, af := range v.AggFuncs {
		if af.Distinct {
			// We do not support distinct push down.
			return e
		}
		pbAggFunc := b.aggFuncToPBExpr(client, af, xSrc.GetTableName())
		if pbAggFunc == nil {
			return e
		}
		pbAggFuncs = append(pbAggFuncs, pbAggFunc)
	}
	pbByItems := make([]*tipb.ByItem, 0, len(v.GroupByItems))
	// Convert groupby to pb
	for _, item := range v.GroupByItems {
		pbByItem := b.groupByItemToPB(client, item, xSrc.GetTableName())
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
	for _, agg := range v.AggFuncs {
		name := strings.ToLower(agg.F)
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
			fields = append(fields, agg.GetType())
		}
	}
	xSrc.AddAggregate(pbAggFuncs, pbByItems, fields)
	hasGroupBy := len(v.GroupByItems) > 0
	xe := &XAggregateExec{
		Src:          src,
		ResultFields: v.Fields(),
		ctx:          b.ctx,
		AggFuncs:     v.AggFuncs,
		hasGroupBy:   hasGroupBy,
	}
	return xe
}

func (b *executorBuilder) buildHaving(v *plan.Having) Executor {
	src := b.build(v.GetChildByIndex(0))
	return b.buildFilter(src, v.Conditions)
}

func (b *executorBuilder) buildSort(v *plan.Sort) Executor {
	src := b.build(v.GetChildByIndex(0))
	e := &SortExec{
		Src:     src,
		ByItems: v.ByItems,
		ctx:     b.ctx,
		Limit:   v.ExecLimit,
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

func (b *executorBuilder) buildUnion(v *plan.Union) Executor {
	e := &UnionExec{
		schema: v.GetSchema(),
		fields: v.Fields(),
		Sels:   make([]Executor, len(v.Selects)),
	}
	for i, sel := range v.Selects {
		selExec := b.build(sel)
		e.Sels[i] = selExec
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

func (b *executorBuilder) buildUpdate(v *plan.Update) Executor {
	selExec := b.build(v.SelectPlan)
	return &UpdateExec{ctx: b.ctx, SelectExec: selExec, OrderedList: v.OrderedList}
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
	}
	// fields is used to evaluate values expr.
	insert.fields = ts.GetResultFields()
	return insert
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
		fields:   v.Fields(),
	}
}

// buildUnionScanExec builds a union scan executor, the src Executor is either
// *XSelectTableExec or *XSelectIndexExec.
func (b *executorBuilder) buildUnionScanExec(src Executor) *UnionScanExec {
	us := &UnionScanExec{ctx: b.ctx, Src: src}
	switch x := src.(type) {
	case *XSelectTableExec:
		us.desc = x.tablePlan.Desc
		us.dirty = getDirtyDB(b.ctx).getDirtyTable(x.table.Meta().ID)
		us.condition = b.joinConditions(append(x.tablePlan.AccessConditions, x.tablePlan.FilterConditions...))
		us.buildAndSortAddedRows(x.table, x.tablePlan.TableAsName)
	case *XSelectIndexExec:
		us.desc = x.indexPlan.Desc
		for _, ic := range x.indexPlan.Index.Columns {
			us.usedIndex = append(us.usedIndex, ic.Offset)
		}
		us.dirty = getDirtyDB(b.ctx).getDirtyTable(x.table.Meta().ID)
		us.condition = b.joinConditions(append(x.indexPlan.AccessConditions, x.indexPlan.FilterConditions...))
		us.buildAndSortAddedRows(x.table, x.indexPlan.TableAsName)
	default:
		b.err = ErrUnknownPlan
	}
	return us
}
