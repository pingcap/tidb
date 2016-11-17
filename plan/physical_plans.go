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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ physicalDistSQLPlan = &PhysicalTableScan{}
	_ physicalDistSQLPlan = &PhysicalIndexScan{}
)

// PhysicalIndexScan represents an index scan plan.
type PhysicalIndexScan struct {
	basePlan
	physicalTableSource

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

	// All conditions in AccessCondition[accessEqualCount:accessInAndEqCount] are IN expressions or equal conditions.
	accessInAndEqCount int
	// All conditions in AccessCondition[:accessEqualCount] are equal conditions.
	accessEqualCount int

	TableAsName *model.CIStr
}

// physicalDistSQLPlan means the plan that can be executed distributively.
// We can push down other plan like selection, limit, aggregation, topn into this plan.
type physicalDistSQLPlan interface {
	addAggregation(agg *PhysicalAggregation) expression.Schema
	addTopN(prop *requiredProperty) bool
	addLimit(limit *Limit)
	calculateCost(count uint64) float64
}

func (p *PhysicalIndexScan) calculateCost(count uint64) float64 {
	cnt := float64(count)
	// network cost
	cost := cnt * netWorkFactor
	if p.DoubleRead {
		cost *= 2
	}
	// sort cost
	if !p.OutOfOrder && p.DoubleRead {
		cost += float64(count) * cpuFactor
	}
	return cost
}

func (p *PhysicalTableScan) calculateCost(count uint64) float64 {
	cnt := float64(count)
	return cnt * netWorkFactor
}

type physicalTableSource struct {
	client kv.Client

	Aggregated bool
	readOnly   bool
	AggFields  []*types.FieldType
	AggFuncsPB []*tipb.Expr
	GbyItemsPB []*tipb.ByItem

	// ConditionPBExpr is the pb structure of conditions that be pushed down.
	ConditionPBExpr *tipb.Expr

	// AccessCondition is used to calculate range.
	AccessCondition []expression.Expression

	LimitCount  *int64
	SortItemsPB []*tipb.ByItem

	// The following fields are used for explaining and testing. Because pb structures are not human-readable.
	aggFuncs   []expression.AggregationFunction
	gbyItems   []expression.Expression
	sortItems  []*ByItems
	conditions []expression.Expression
}

// MarshalJSON implements json.Marshaler interface.
func (p *physicalTableSource) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	limit := 0
	if p.LimitCount != nil {
		limit = int(*p.LimitCount)
	}
	buffer.WriteString(fmt.Sprintf("\"limit\": %d, \n", limit))
	if p.Aggregated {
		buffer.WriteString(fmt.Sprint("\"aggregated push down\": true, \n"))
		gbyItems, err := json.Marshal(p.gbyItems)
		if err != nil {
			return nil, errors.Trace(err)
		}
		buffer.WriteString(fmt.Sprintf("\"gby items\": %s, \n", gbyItems))
		aggFuncs, err := json.Marshal(p.aggFuncs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		buffer.WriteString(fmt.Sprintf("\"agg funcs\": %s, \n", aggFuncs))
	} else if len(p.sortItems) > 0 {
		sortItems, err := json.Marshal(p.sortItems)
		if err != nil {
			return nil, errors.Trace(err)
		}
		buffer.WriteString(fmt.Sprintf("\"sort items\": %s, \n", sortItems))
	}
	access, err := json.Marshal(p.AccessCondition)
	if err != nil {
		return nil, errors.Trace(err)
	}
	filter, err := json.Marshal(p.conditions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// print condition infos
	buffer.WriteString(fmt.Sprintf("\"access conditions\": %s, \n", access))
	buffer.WriteString(fmt.Sprintf("\"filter conditions\": %s}", filter))
	return buffer.Bytes(), nil
}

func (p *physicalTableSource) clearForAggPushDown() {
	p.AggFields = nil
	p.AggFuncsPB = nil
	p.GbyItemsPB = nil
	p.Aggregated = false

	p.aggFuncs = nil
	p.gbyItems = nil
}

func (p *physicalTableSource) clearForTopnPushDown() {
	p.sortItems = nil
	p.SortItemsPB = nil
	p.LimitCount = nil
}

func needCount(af expression.AggregationFunction) bool {
	return af.GetName() == ast.AggFuncCount || af.GetName() == ast.AggFuncAvg
}

func needValue(af expression.AggregationFunction) bool {
	return af.GetName() == ast.AggFuncSum || af.GetName() == ast.AggFuncAvg || af.GetName() == ast.AggFuncFirstRow ||
		af.GetName() == ast.AggFuncMax || af.GetName() == ast.AggFuncMin || af.GetName() == ast.AggFuncGroupConcat
}

func (p *physicalTableSource) tryToAddUnionScan(resultPlan PhysicalPlan) PhysicalPlan {
	if p.readOnly {
		return resultPlan
	}
	us := &PhysicalUnionScan{
		Condition: expression.ComposeCNFCondition(append(p.conditions, p.AccessCondition...)),
	}
	us.SetChildren(resultPlan)
	us.SetSchema(resultPlan.GetSchema())
	return us
}

func (p *physicalTableSource) addLimit(l *Limit) {
	if l != nil {
		count := int64(l.Count + l.Offset)
		p.LimitCount = &count
	}
}

func (p *physicalTableSource) addTopN(prop *requiredProperty) bool {
	if len(prop.props) == 0 && prop.limit != nil {
		p.addLimit(prop.limit)
		return true
	}
	if p.client == nil || !p.client.SupportRequestType(kv.ReqTypeSelect, kv.ReqSubTypeTopN) {
		return false
	}
	if prop.limit == nil {
		return false
	}
	count := int64(prop.limit.Count + prop.limit.Offset)
	p.LimitCount = &count
	for _, prop := range prop.props {
		item := sortByItemToPB(p.client, prop.col, prop.desc)
		if item == nil {
			// When we fail to convert any sortItem to PB struct, we should clear the environments.
			p.clearForTopnPushDown()
			return false
		}
		p.SortItemsPB = append(p.SortItemsPB, item)
		p.sortItems = append(p.sortItems, &ByItems{Expr: prop.col, Desc: prop.desc})
	}
	return true
}

func (p *physicalTableSource) addAggregation(agg *PhysicalAggregation) expression.Schema {
	if p.client == nil {
		return nil
	}
	for _, f := range agg.AggFuncs {
		pb := aggFuncToPBExpr(p.client, f)
		if pb == nil {
			// When we fail to convert any agg function to PB struct, we should clear the environments.
			p.clearForAggPushDown()
			return nil
		}
		p.AggFuncsPB = append(p.AggFuncsPB, pb)
		p.aggFuncs = append(p.aggFuncs, f.Clone())
	}
	for _, item := range agg.GroupByItems {
		pb := groupByItemToPB(p.client, item)
		if pb == nil {
			// When we fail to convert any group-by item to PB struct, we should clear the environments.
			p.clearForAggPushDown()
			return nil
		}
		p.GbyItemsPB = append(p.GbyItemsPB, pb)
		p.gbyItems = append(p.gbyItems, item.Clone())
	}
	p.Aggregated = true
	gk := types.NewFieldType(mysql.TypeBlob)
	gk.Charset = charset.CharsetBin
	gk.Collate = charset.CollationBin
	p.AggFields = append(p.AggFields, gk)
	var schema expression.Schema
	cursor := 0
	schema = append(schema, &expression.Column{Index: cursor, ColName: model.NewCIStr(fmt.Sprint(agg.GroupByItems))})
	agg.GroupByItems = []expression.Expression{schema[cursor]}
	newAggFuncs := make([]expression.AggregationFunction, len(agg.AggFuncs))
	for i, aggFun := range agg.AggFuncs {
		fun := expression.NewAggFunction(aggFun.GetName(), nil, false)
		var args []expression.Expression
		colName := model.NewCIStr(fmt.Sprint(aggFun.GetArgs()))
		if needCount(fun) {
			cursor++
			schema = append(schema, &expression.Column{Index: cursor, ColName: colName})
			args = append(args, schema[cursor])
			ft := types.NewFieldType(mysql.TypeLonglong)
			ft.Flen = 21
			ft.Charset = charset.CharsetBin
			ft.Collate = charset.CollationBin
			p.AggFields = append(p.AggFields, ft)
		}
		if needValue(fun) {
			cursor++
			schema = append(schema, &expression.Column{Index: cursor, ColName: colName})
			args = append(args, schema[cursor])
			p.AggFields = append(p.AggFields, agg.schema[i].GetType())
		}
		fun.SetArgs(args)
		fun.SetMode(expression.FinalMode)
		newAggFuncs[i] = fun
	}
	agg.AggFuncs = newAggFuncs
	return schema
}

// PhysicalTableScan represents a table scan plan.
type PhysicalTableScan struct {
	basePlan
	physicalTableSource

	Table   *model.TableInfo
	Columns []*model.ColumnInfo
	DBName  *model.CIStr
	Desc    bool
	Ranges  []TableRange
	pkCol   *expression.Column

	TableAsName *model.CIStr

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
	OuterSchema []*expression.CorrelatedColumn
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

	DefaultValues []types.Datum
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

// AggregationType stands for the mode of aggregation plan.
type AggregationType int

const (
	// StreamedAgg supposes its input is sorted by group by key.
	StreamedAgg AggregationType = iota
	// FinalAgg supposes its input is partial results.
	FinalAgg
	// CompleteAgg supposes its input is original results.
	CompleteAgg
)

// PhysicalAggregation is Aggregation's physical plan.
type PhysicalAggregation struct {
	basePlan

	HasGby       bool
	AggType      AggregationType
	AggFuncs     []expression.AggregationFunction
	GroupByItems []expression.Expression
}

// PhysicalUnionScan represents a union scan operator.
type PhysicalUnionScan struct {
	basePlan

	Condition expression.Expression
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalIndexScan) Copy() PhysicalPlan {
	np := *p
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalIndexScan) MarshalJSON() ([]byte, error) {
	pushDownInfo, err := json.Marshal(&p.physicalTableSource)
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
		"\n \"push down info\": %s\n}",
		p.DBName.O, p.Table.Name.O, p.Index.Name.O, p.Ranges, p.Desc, p.OutOfOrder, p.DoubleRead, pushDownInfo))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalTableScan) Copy() PhysicalPlan {
	np := *p
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalTableScan) MarshalJSON() ([]byte, error) {
	pushDownInfo, err := json.Marshal(&p.physicalTableSource)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf("\"type\": \"TableScan\",\n"+
		" \"db\": \"%s\","+
		"\n \"table\": \"%s\","+
		"\n \"desc\": %v,"+
		"\n \"keep order\": %v,"+
		"\n \"push down info\": %s}",
		p.DBName.O, p.Table.Name.O, p.Desc, p.KeepOrder, pushDownInfo))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalApply) Copy() PhysicalPlan {
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
	var child PhysicalPlan
	if len(p.children) > 0 {
		child = p.children[0].(PhysicalPlan)
	}
	childStr, err := json.Marshal(child)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf("\"type\": \"Limit\",\n"+
		" \"limit\": %d,\n"+
		" \"offset\": %d,\n"+
		" \"child\": %s}", p.Count, p.Offset, childStr))
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
	limit, err := json.Marshal(p.ExecLimit)
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
		" \"limit\": %s,\n"+
		" \"child\": %s}", exprs, limit, child))
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

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalAggregation) MarshalJSON() ([]byte, error) {
	child, err := json.Marshal(p.children[0].(PhysicalPlan))
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	var tp string
	if p.AggType == StreamedAgg {
		tp = "StreamedAgg"
	} else if p.AggType == FinalAgg {
		tp = "FinalAgg"
	} else {
		tp = "CompleteAgg"
	}
	aggFuncs, err := json.Marshal(p.AggFuncs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gbyExprs, err := json.Marshal(p.GroupByItems)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer.WriteString(fmt.Sprintf("\"type\": \"%s\",\n"+
		"\"AggFuncs\": %s,\n"+
		"\"GroupByItems\": %s,\n"+
		"\"child\": %s}", tp, aggFuncs, gbyExprs, child))
	return buffer.Bytes(), nil
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

// Copy implements the PhysicalPlan Copy interface.
func (p *Show) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalUnionScan) Copy() PhysicalPlan {
	np := *p
	return &np
}
