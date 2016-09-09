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

package plan

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalIndexScan represents an index scan plan.
type PhysicalIndexScan struct {
	basePlan
	physicalTableSource

	ctx        context.Context
	Table      *model.TableInfo
	Index      *model.IndexInfo
	Ranges     []*IndexRange
	Columns    []*model.ColumnInfo
	DBName     *model.CIStr
	Desc       bool
	OutOfOrder bool
	// DoubleRead means if the index executor will read kv two times.
	// If the query requires the columns that don't belong to index, DoubleRead will be true.
	DoubleRead bool

	accessEqualCount int
	AccessCondition  []expression.Expression

	TableAsName *model.CIStr

	LimitCount *int64
}

type physicalXPlan interface {
	addAggregation(agg *PhysicalAggregation, ctx context.Context) (expression.Schema, error)
}

type physicalTableSource struct {
	Aggregated bool
	AggFields  []*types.FieldType
	AggFuncs   []*tipb.Expr
	GbyItems   []*tipb.ByItem

	ConditionPBExpr *tipb.Expr
}

func (p *physicalTableSource) clear() {
	p.AggFields = nil
	p.AggFuncs = nil
	p.GbyItems = nil
	p.Aggregated = false
}

func needCount(af expression.AggregationFunction) bool {
	return af.GetName() == ast.AggFuncCount || af.GetName() == ast.AggFuncAvg
}

func needValue(af expression.AggregationFunction) bool {
	return af.GetName() == ast.AggFuncSum || af.GetName() == ast.AggFuncAvg || af.GetName() == ast.AggFuncFirstRow ||
		af.GetName() == ast.AggFuncMax || af.GetName() == ast.AggFuncMin || af.GetName() == ast.AggFuncGroupConcat
}

func (p *physicalTableSource) addAggregation(agg *PhysicalAggregation, ctx context.Context) (expression.Schema, error) {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if txn == nil {
		// for plan test
		return nil, nil
	}
	client := txn.GetClient()
	for _, f := range agg.AggFuncs {
		pb, err := AggFuncToPBExpr(client, f)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if pb == nil {
			p.clear()
			return nil, nil
		}
		p.AggFuncs = append(p.AggFuncs, pb)
	}
	for _, item := range agg.GroupByItems {
		pb, err := GroupByItemToPB(client, item)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if pb == nil {
			p.clear()
			return nil, nil
		}
		p.GbyItems = append(p.GbyItems, pb)
	}
	p.Aggregated = true
	gk := types.NewFieldType(mysql.TypeBlob)
	gk.Charset = charset.CharsetBin
	gk.Collate = charset.CollationBin
	p.AggFields = append(p.AggFields, gk)
	var schema expression.Schema
	cursor := 0
	schema = append(schema, &expression.Column{Index: cursor})
	agg.GroupByItems = []expression.Expression{schema[cursor]}
	newAggFuncs := make([]expression.AggregationFunction, len(agg.AggFuncs))
	for i, aggFun := range agg.AggFuncs {
		fun := expression.NewAggFunction(aggFun.GetName(), nil, false)
		var args []expression.Expression
		if needCount(fun) {
			cursor++
			schema = append(schema, &expression.Column{Index: cursor})
			args = append(args, schema[cursor])
			ft := types.NewFieldType(mysql.TypeLonglong)
			ft.Flen = 21
			ft.Charset = charset.CharsetBin
			ft.Collate = charset.CollationBin
			p.AggFields = append(p.AggFields, ft)
		}
		if needValue(fun) {
			cursor++
			schema = append(schema, &expression.Column{Index: cursor})
			args = append(args, schema[cursor])
			p.AggFields = append(p.AggFields, agg.schema[i].GetType())
		}
		fun.SetArgs(args)
		fun.SetMode(expression.FinalMode)
		newAggFuncs[i] = fun
	}
	agg.AggFuncs = newAggFuncs
	return schema, nil
}

// PhysicalTableScan represents a table scan plan.
type PhysicalTableScan struct {
	basePlan
	physicalTableSource

	ctx     context.Context
	Table   *model.TableInfo
	Columns []*model.ColumnInfo
	DBName  *model.CIStr
	Desc    bool
	Ranges  []TableRange
	pkCol   *expression.Column

	AccessCondition []expression.Expression

	TableAsName *model.CIStr

	LimitCount *int64

	// If sort data by scanning pkcol, KeepOrder should be true.
	KeepOrder bool
}

// PhysicalDummyScan is a dummy table that returns nothing.
type PhysicalDummyScan struct {
	basePlan
}

// PhysicalApply represents apply plan, only used for subquery.
type PhysicalApply struct {
	basePlan

	InnerPlan   PhysicalPlan
	OuterSchema expression.Schema
	Checker     *ApplyConditionChecker
}

// PhysicalHashJoin represents hash join for inner/ outer join.
type PhysicalHashJoin struct {
	basePlan

	JoinType JoinType

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression
	SmallTable      int
	Concurrency     int
}

// PhysicalHashSemiJoin represents hash join for semi join.
type PhysicalHashSemiJoin struct {
	basePlan

	WithAux bool
	Anti    bool

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression
}

type PhysicalUnionScan struct {
	basePlan

	Table     table.Table
	Desc      bool
	Condition expression.Expression
}

type AggregationType int

const (
	StreamedAgg AggregationType = iota
	FinalAgg
	CompleteAgg
)

type PhysicalAggregation struct {
	basePlan

	HasGby       bool
	AggType      AggregationType
	AggFuncs     []expression.AggregationFunction
	GroupByItems []expression.Expression
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalIndexScan) Copy() PhysicalPlan {
	np := *p
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalIndexScan) MarshalJSON() ([]byte, error) {
	limit := 0
	if p.LimitCount != nil {
		limit = int(*p.LimitCount)
	}
	access, err := json.Marshal(p.AccessCondition)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf("\"type\": \"IndexScan\",\n"+
		"\"db\": \"%s\","+
		"\n \"table\": \"%s\","+
		"\n \"index\": \"%s\","+
		"\n \"ranges\": \"%s\","+
		"\n \"desc\": %v,"+
		"\n \"out of order\": %v,"+
		"\n \"double read\": %v,"+
		"\n \"access condition\": %s,"+
		"\n \"limit\": %d\n}",
		p.DBName.O, p.Table.Name.O, p.Index.Name.O, p.Ranges, p.Desc, p.OutOfOrder, p.DoubleRead, access, limit))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalTableScan) Copy() PhysicalPlan {
	np := *p
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalTableScan) MarshalJSON() ([]byte, error) {
	limit := 0
	if p.LimitCount != nil {
		limit = int(*p.LimitCount)
	}
	access, err := json.Marshal(p.AccessCondition)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf("\"type\": \"TableScan\",\n"+
		" \"db\": \"%s\","+
		"\n \"table\": \"%s\","+
		"\n \"desc\": %v,"+
		"\n \"keep order\": %v,"+
		"\n \"access condition\": %s,"+
		"\n \"limit\": %d}",
		p.DBName.O, p.Table.Name.O, p.Desc, p.KeepOrder, access, limit))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalApply) Copy() PhysicalPlan {
	np := *p
	return &np
}

func (p *PhysicalUnionScan) Copy() PhysicalPlan {
	np := *p
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalApply) MarshalJSON() ([]byte, error) {
	innerPlan, err := json.Marshal(p.InnerPlan.(PhysicalPlan))
	if err != nil {
		return nil, errors.Trace(err)
	}
	outerPlan, err := json.Marshal(p.children[0].(PhysicalPlan))
	if err != nil {
		return nil, errors.Trace(err)
	}
	cond := "null"
	if p.Checker != nil {
		cond = "\"" + p.Checker.Condition.String() + "\""
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf("\"type\": \"Apply\",\"innerPlan\": %v,\n \"outerPlan\": %v,\n \"condition\": %s\n}", innerPlan, outerPlan, cond))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalHashSemiJoin) Copy() PhysicalPlan {
	np := *p
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalHashSemiJoin) MarshalJSON() ([]byte, error) {
	leftChild, err := json.Marshal(p.children[0].(PhysicalPlan))
	if err != nil {
		return nil, errors.Trace(err)
	}
	rightChild, err := json.Marshal(p.children[1].(PhysicalPlan))
	if err != nil {
		return nil, errors.Trace(err)
	}
	eqConds, err := json.Marshal(p.EqualConditions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	leftConds, err := json.Marshal(p.LeftConditions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rightConds, err := json.Marshal(p.RightConditions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	otherConds, err := json.Marshal(p.OtherConditions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf(
		"\"type\": \"SemiJoin\",\n "+
			"\"with aux\": %v,"+
			"\"anti\": %v,"+
			"\"eqCond\": %s,\n "+
			"\"leftCond\": %s,\n "+
			"\"rightCond\": %s,\n "+
			"\"otherCond\": %s,\n"+
			"\"leftPlan\": %s,\n "+
			"\"rightPlan\": %s"+
			"}",
		p.WithAux, p.Anti, eqConds, leftConds, rightConds, otherConds, leftChild, rightChild))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalHashJoin) Copy() PhysicalPlan {
	np := *p
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalHashJoin) MarshalJSON() ([]byte, error) {
	leftChild, err := json.Marshal(p.children[0].(PhysicalPlan))
	if err != nil {
		return nil, errors.Trace(err)
	}
	rightChild, err := json.Marshal(p.children[1].(PhysicalPlan))
	if err != nil {
		return nil, errors.Trace(err)
	}
	tp := "InnerJoin"
	if p.JoinType == LeftOuterJoin {
		tp = "LeftJoin"
	} else if p.JoinType == RightOuterJoin {
		tp = "RightJoin"
	}
	eqConds, err := json.Marshal(p.EqualConditions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	leftConds, err := json.Marshal(p.LeftConditions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rightConds, err := json.Marshal(p.RightConditions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	otherConds, err := json.Marshal(p.OtherConditions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf(
		"\"type\": \"%s\",\n "+
			"\"eqCond\": %s,\n "+
			"\"leftCond\": %s,\n "+
			"\"rightCond\": %s,\n "+
			"\"otherCond\": %s,\n"+
			"\"leftPlan\": %s,\n "+
			"\"rightPlan\": %s"+
			"}",
		tp, eqConds, leftConds, rightConds, otherConds, leftChild, rightChild))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Distinct) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Selection) Copy() PhysicalPlan {
	np := *p
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *Selection) MarshalJSON() ([]byte, error) {
	child, err := json.Marshal(p.children[0].(PhysicalPlan))
	if err != nil {
		return nil, errors.Trace(err)
	}
	conds, err := json.Marshal(p.Conditions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf("\"type\": \"Selection\",\n"+
		" \"condition\": %s,\n"+
		" \"child\": %s\n}", conds, child))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Projection) Copy() PhysicalPlan {
	np := *p
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *Projection) MarshalJSON() ([]byte, error) {
	child, err := json.Marshal(p.children[0].(PhysicalPlan))
	if err != nil {
		return nil, errors.Trace(err)
	}
	exprs, err := json.Marshal(p.Exprs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf("\"type\": \"Projection\",\n"+
		" \"exprs\": %s,\n"+
		" \"child\": %s\n}", exprs, child))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Exists) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *MaxOneRow) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Insert) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Limit) Copy() PhysicalPlan {
	np := *p
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *Limit) MarshalJSON() ([]byte, error) {
	child, err := json.Marshal(p.children[0].(PhysicalPlan))
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf("\"type\": \"Limit\",\n"+
		" \"limit\": %d,\n"+
		" \"offset\": %d,\n"+
		" \"child\": %s}", p.Count, p.Offset, child))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Union) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Sort) Copy() PhysicalPlan {
	np := *p
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *Sort) MarshalJSON() ([]byte, error) {
	child, err := json.Marshal(p.children[0].(PhysicalPlan))
	if err != nil {
		return nil, errors.Trace(err)
	}
	exprs, err := json.Marshal(p.ByItems)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf("\"type\": \"Sort\",\n"+
		" \"exprs\": %s,\n"+
		" \"child\": %s}", exprs, child))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *TableDual) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Trim) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *SelectLock) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalAggregation) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Update) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalDummyScan) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Delete) Copy() PhysicalPlan {
	np := *p
	return &np
}
