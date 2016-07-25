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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"math"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

//TODO: select join algorithm during cbo phase.
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
	switch v.JoinType {
	case plan.LeftOuterJoin, plan.RightOuterJoin:
		e.outter = true
	default:
		e.outter = false
	}
	if e.leftSmall {
		e.smallExec = b.build(v.GetChildByIndex(0))
		e.bigExec = b.build(v.GetChildByIndex(1))
	} else {
		e.smallExec = b.build(v.GetChildByIndex(1))
		e.bigExec = b.build(v.GetChildByIndex(0))
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

func (b *executorBuilder) buildAggregation(v *plan.Aggregation) Executor {
	src := b.build(v.GetChildByIndex(0))
	e := &AggregationExec{
		Src:          src,
		schema:       v.GetSchema(),
		ctx:          b.ctx,
		AggFuncs:     v.AggFuncs,
		GroupByItems: v.GroupByItems,
	}
	// Check if the underlying is xapi executor, we should try to push aggregate function down.
	xSrc, ok := src.(NewXExecutor)
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
		pbAggFunc := b.newAggFuncToPBExpr(client, af, xSrc.GetTable())
		if pbAggFunc == nil {
			return e
		}
		pbAggFuncs = append(pbAggFuncs, pbAggFunc)
	}
	pbByItems := make([]*tipb.ByItem, 0, len(v.GroupByItems))
	// Convert groupby to pb
	for _, item := range v.GroupByItems {
		pbByItem := b.newGroupByItemToPB(client, item, xSrc.GetTable())
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
	xe := &NewXAggregateExec{
		Src:        src,
		ctx:        b.ctx,
		AggFuncs:   v.AggFuncs,
		hasGroupBy: hasGroupBy,
		schema:     v.GetSchema(),
	}
	log.Debugf("Use XAggregateExec with %d aggs", len(v.AggFuncs))
	return xe
}

func (b *executorBuilder) toPBExpr(conditions []expression.Expression, tbl *model.TableInfo) (
	*tipb.Expr, []expression.Expression) {
	txn, err := b.ctx.GetTxn(false)
	if err != nil {
		b.err = err
		return nil, nil
	}
	client := txn.GetClient()
	return b.newConditionExprToPBExpr(client, conditions, tbl)
}

func (b *executorBuilder) buildSelection(v *plan.Selection) Executor {
	child := v.GetChildByIndex(0)
	var src Executor
	switch x := child.(type) {
	case *plan.PhysicalTableScan:
		src = b.buildNewTableScan(x, v)
	case *plan.PhysicalIndexScan:
		src = b.buildNewIndexScan(x, v)
	default:
		src = b.build(x)
	}

	if len(v.Conditions) == 0 {
		return src
	}

	return &SelectionExec{
		Src:       src,
		Condition: expression.ComposeCNFCondition(v.Conditions),
		schema:    v.GetSchema(),
		ctx:       b.ctx,
	}
}

func (b *executorBuilder) buildProjection(v *plan.Projection) Executor {
	return &ProjectionExec{
		Src:    b.build(v.GetChildByIndex(0)),
		ctx:    b.ctx,
		exprs:  v.Exprs,
		schema: v.GetSchema(),
	}
}

func (b *executorBuilder) buildNewTableDual(v *plan.NewTableDual) Executor {
	return &NewTableDualExec{schema: v.GetSchema()}
}

func (b *executorBuilder) buildNewTableScan(v *plan.PhysicalTableScan, s *plan.Selection) Executor {
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
		st := &NewXSelectTableExec{
			tableInfo:   v.Table,
			ctx:         b.ctx,
			supportDesc: supportDesc,
			asName:      v.TableAsName,
			table:       table,
			schema:      v.GetSchema(),
			Columns:     v.Columns,
			ranges:      v.Ranges,
			desc:        v.Desc,
			limitCount:  v.LimitCount,
		}
		ret = st
		if !txn.IsReadOnly() {
			if s != nil {
				ret = b.buildNewUnionScanExec(ret,
					expression.ComposeCNFCondition(append(s.Conditions, v.AccessCondition...)))
			} else {
				ret = b.buildNewUnionScanExec(ret, expression.ComposeCNFCondition(v.AccessCondition))
			}
		}
		if s != nil {
			st.where, s.Conditions = b.toPBExpr(s.Conditions, st.tableInfo)
		}
		return ret
	}

	ts := &NewTableScanExec{
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

func (b *executorBuilder) buildNewIndexScan(v *plan.PhysicalIndexScan, s *plan.Selection) Executor {
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
		st := &NewXSelectIndexExec{
			tableInfo:   v.Table,
			ctx:         b.ctx,
			supportDesc: supportDesc,
			asName:      v.TableAsName,
			table:       table,
			indexPlan:   v,
		}
		ret = st
		if !txn.IsReadOnly() {
			if s != nil {
				ret = b.buildNewUnionScanExec(ret,
					expression.ComposeCNFCondition(append(s.Conditions, v.AccessCondition...)))
			} else {
				ret = b.buildNewUnionScanExec(ret, expression.ComposeCNFCondition(v.AccessCondition))
			}
		}
		if s != nil {
			st.where, s.Conditions = b.toPBExpr(s.Conditions, st.tableInfo)
		}
		return ret
	}

	b.err = errors.New("Not implement yet.")
	return nil
}

func (b *executorBuilder) buildNewSort(v *plan.NewSort) Executor {
	src := b.build(v.GetChildByIndex(0))
	return &NewSortExec{
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

func (b *executorBuilder) buildNewUnion(v *plan.NewUnion) Executor {
	e := &NewUnionExec{
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
