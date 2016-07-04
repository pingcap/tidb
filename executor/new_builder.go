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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tipb/go-tipb"
)

//TODO: select join algorithm during cbo phase.
func (b *executorBuilder) buildJoin(v *plan.Join) Executor {
	e := &HashJoinExec{
		schema:      v.GetSchema(),
		otherFilter: expression.ComposeCNFCondition(v.OtherConditions),
		prepared:    false,
		ctx:         b.ctx,
	}
	var leftHashKey, rightHashKey []*expression.Column
	for _, eqCond := range v.EqualConditions {
		ln, _ := eqCond.Args[0].(*expression.Column)
		rn, _ := eqCond.Args[1].(*expression.Column)
		leftHashKey = append(leftHashKey, ln)
		rightHashKey = append(rightHashKey, rn)
	}
	switch v.JoinType {
	case plan.LeftOuterJoin:
		e.outter = true
		e.leftSmall = false
		e.smallFilter = expression.ComposeCNFCondition(v.RightConditions)
		e.bigFilter = expression.ComposeCNFCondition(v.LeftConditions)
		e.smallHashKey = rightHashKey
		e.bigHashKey = leftHashKey
	case plan.RightOuterJoin:
		e.outter = true
		e.leftSmall = true
		e.smallFilter = expression.ComposeCNFCondition(v.LeftConditions)
		e.bigFilter = expression.ComposeCNFCondition(v.RightConditions)
		e.smallHashKey = leftHashKey
		e.bigHashKey = rightHashKey
	case plan.InnerJoin:
		//TODO: assume right table is the small one before cbo is realized.
		e.outter = false
		e.leftSmall = false
		e.smallFilter = expression.ComposeCNFCondition(v.RightConditions)
		e.bigFilter = expression.ComposeCNFCondition(v.LeftConditions)
		e.smallHashKey = rightHashKey
		e.bigHashKey = leftHashKey
	default:
		b.err = ErrUnknownPlan.Gen("Unknown Join Type !!")
		return nil
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

func (b *executorBuilder) buildAggregation(v *plan.Aggregation) Executor {
	return &AggregationExec{
		Src:          b.build(v.GetChildByIndex(0)),
		schema:       v.GetSchema(),
		ctx:          b.ctx,
		AggFuncs:     v.AggFuncs,
		GroupByItems: v.GroupByItems,
	}
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
	ts, ok := v.GetChildByIndex(0).(*plan.NewTableScan)
	var src Executor
	if ok {
		src = b.buildNewTableScan(ts, v)
	} else {
		src = b.build(v.GetChildByIndex(0))
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

func (b *executorBuilder) buildNewTableScan(v *plan.NewTableScan, s *plan.Selection) Executor {
	txn, err := b.ctx.GetTxn(false)
	if err != nil {
		b.err = err
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
		ts := &NewTableScanExec{
			tableInfo:   v.Table,
			ctx:         b.ctx,
			supportDesc: supportDesc,
			asName:      v.TableAsName,
			table:       table,
			schema:      v.GetSchema(),
			Columns:     v.Columns,
			ranges:      v.Ranges,
		}
		ret = ts
		if !txn.IsReadOnly() {
			if s != nil {
				ret = b.buildNewUnionScanExec(ret, expression.ComposeCNFCondition(append(s.Conditions, v.AccessCondition...)))
			} else {
				ret = b.buildNewUnionScanExec(ret, nil)
			}
		}
		if s != nil {
			ts.where, s.Conditions = b.toPBExpr(s.Conditions, ts.tableInfo)
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

func (b *executorBuilder) buildApply(v *plan.Apply) Executor {
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
		Srcs:   make([]Executor, len(v.Selects)),
	}
	for i, sel := range v.Selects {
		selExec := b.build(sel)
		e.Srcs[i] = selExec
	}
	return e
}
