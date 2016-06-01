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
	"fmt"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/optimizer/plan"
	"github.com/pingcap/tidb/parser/opcode"
)

// compose CNF items into a balance deep CNF tree, which benefits a lot for pb decoder/encoder.
func composeCondition(conditions []expression.Expression) expression.Expression {
	length := len(conditions)
	if length == 0 {
		return nil
	} else if length == 1 {
		return conditions[0]
	} else {
		eqStr, _ := opcode.Ops[opcode.AndAnd]
		return expression.NewFunction(model.NewCIStr(eqStr), []expression.Expression{composeCondition(conditions[0 : length/2]), composeCondition(conditions[length/2:])})
	}
}

//TODO: select join algorithm during cbo phase.
func (b *executorBuilder) buildJoin(v *plan.Join) Executor {
	e := &HashJoinExec{
		schema:      v.GetSchema(),
		otherFilter: composeCondition(v.OtherConditions),
		prepared:    false,
		ctx:         b.ctx,
	}
	var leftHashKey, rightHashKey []*expression.Column
	for _, eqCond := range v.EqualConditions {
		eqStr, _ := opcode.Ops[opcode.EQ]
		if eqCond.FuncName.L == eqStr {
			ln, lOK := eqCond.Args[0].(*expression.Column)
			rn, rOK := eqCond.Args[1].(*expression.Column)
			if lOK && rOK {
				leftHashKey = append(leftHashKey, ln)
				rightHashKey = append(rightHashKey, rn)
				continue
			}
		}
		b.err = ErrUnknownPlan.Gen(fmt.Sprintf("Invalid Join Equal Condition %s !!", eqCond.ToString()))
	}
	switch v.JoinType {
	case plan.LeftOuterJoin:
		e.outter = true
		e.leftSmall = false
		e.smallFilter = composeCondition(v.RightConditions)
		e.bigFilter = composeCondition(v.LeftConditions)
		e.smallHashKey = rightHashKey
		e.bigHashKey = leftHashKey
	case plan.RightOuterJoin:
		e.outter = true
		e.leftSmall = true
		e.smallFilter = composeCondition(v.LeftConditions)
		e.bigFilter = composeCondition(v.RightConditions)
		e.smallHashKey = leftHashKey
		e.bigHashKey = rightHashKey
	case plan.InnerJoin:
		//TODO: assume right table is the small one before cbo is realized.
		e.outter = false
		e.leftSmall = false
		e.smallFilter = composeCondition(v.RightConditions)
		e.bigFilter = composeCondition(v.LeftConditions)
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
	src := b.build(v.GetChildByIndex(0))
	e := &AggregationExec{
		Src:          src,
		schema:       v.GetSchema(),
		ctx:          b.ctx,
		AggFuncs:     v.AggFuncs,
		GroupByItems: v.GroupByItems,
	}
	return e
}

func (b *executorBuilder) buildSelection(v *plan.Selection) Executor {
	return &SelectionExec{
		Src:       b.build(v.GetChildByIndex(0)),
		Condition: composeCondition(v.Conditions),
		schema:    v.GetSchema(),
		ctx:       b.ctx,
	}
}

func (b *executorBuilder) buildProjection(v *plan.Projection) Executor {
	src := b.build(v.GetChildByIndex(0))
	e := &ProjectionExec{
		Src:    src,
		ctx:    b.ctx,
		exprs:  v.Exprs,
		schema: v.GetSchema(),
	}
	return e
}

func (b *executorBuilder) buildNewTableScan(v *plan.NewTableScan) Executor {
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
		// TODO: support condition pushdown and union scan exec.
		e := &NewTableScanExec{
			tableInfo:   v.Table,
			ctx:         b.ctx,
			supportDesc: supportDesc,
			asName:      v.TableAsName,
			table:       table,
			schema:      v.GetSchema(),
			Columns:     v.Columns,
			ranges:      v.Ranges,
		}
		return e
	}
	b.err = errors.New("Not implement yet.")
	return nil
}

func (b *executorBuilder) buildNewSort(v *plan.NewSort) Executor {
	src := b.build(v.GetChildByIndex(0))
	e := &NewSortExec{
		Src:     src,
		ByItems: v.ByItems,
		ctx:     b.ctx,
		schema:  v.GetSchema(),
	}
	return e
}
