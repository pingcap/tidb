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
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ physicalDistSQLPlan = &PhysicalTableScan{}
	_ physicalDistSQLPlan = &PhysicalIndexScan{}
)

const (
	notController = iota
	controlTableScan
	controlIndexScan
)

var (
	_ PhysicalPlan = &Selection{}
	_ PhysicalPlan = &Projection{}
	_ PhysicalPlan = &Exists{}
	_ PhysicalPlan = &MaxOneRow{}
	_ PhysicalPlan = &TableDual{}
	_ PhysicalPlan = &Union{}
	_ PhysicalPlan = &Sort{}
	_ PhysicalPlan = &Update{}
	_ PhysicalPlan = &Delete{}
	_ PhysicalPlan = &SelectLock{}
	_ PhysicalPlan = &Limit{}
	_ PhysicalPlan = &Show{}
	_ PhysicalPlan = &Insert{}
	_ PhysicalPlan = &PhysicalIndexScan{}
	_ PhysicalPlan = &PhysicalTableScan{}
	_ PhysicalPlan = &PhysicalTableReader{}
	_ PhysicalPlan = &PhysicalIndexReader{}
	_ PhysicalPlan = &PhysicalIndexLookUpReader{}
	_ PhysicalPlan = &PhysicalAggregation{}
	_ PhysicalPlan = &PhysicalApply{}
	_ PhysicalPlan = &PhysicalIndexJoin{}
	_ PhysicalPlan = &PhysicalHashJoin{}
	_ PhysicalPlan = &PhysicalHashSemiJoin{}
	_ PhysicalPlan = &PhysicalMergeJoin{}
	_ PhysicalPlan = &PhysicalUnionScan{}
	_ PhysicalPlan = &Cache{}
)

// PhysicalTableReader is the table reader in tidb.
type PhysicalTableReader struct {
	*basePlan
	basePhysicalPlan

	// TablePlans flats the tablePlan to construct executor pb.
	TablePlans []PhysicalPlan
	tablePlan  PhysicalPlan

	// NeedColHandle is used in execution phase.
	NeedColHandle bool
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalTableReader) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// PhysicalIndexReader is the index reader in tidb.
type PhysicalIndexReader struct {
	*basePlan
	basePhysicalPlan

	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlans []PhysicalPlan
	indexPlan  PhysicalPlan

	// OutputColumns represents the columns that index reader should return.
	OutputColumns []*expression.Column

	// NeedColHandle is used in execution phase.
	NeedColHandle bool
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalIndexReader) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// PhysicalIndexLookUpReader is the index look up reader in tidb. It's used in case of double reading.
type PhysicalIndexLookUpReader struct {
	*basePlan
	basePhysicalPlan

	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlans []PhysicalPlan
	// TablePlans flats the tablePlan to construct executor pb.
	TablePlans []PhysicalPlan
	indexPlan  PhysicalPlan
	tablePlan  PhysicalPlan

	// NeedColHandle is used in execution phase.
	NeedColHandle bool
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalIndexLookUpReader) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// PhysicalIndexScan represents an index scan plan.
type PhysicalIndexScan struct {
	physicalTableSource

	Table      *model.TableInfo
	Index      *model.IndexInfo
	Ranges     []*types.IndexRange
	Columns    []*model.ColumnInfo
	DBName     model.CIStr
	Desc       bool
	OutOfOrder bool
	// DoubleRead means if the index executor will read kv two times.
	// If the query requires the columns that don't belong to index, DoubleRead will be true.
	DoubleRead bool

	// accessInAndEqCount is counter of all conditions in AccessCondition[accessEqualCount:accessInAndEqCount].
	accessInAndEqCount int
	// accessEqualCount is counter of all conditions in AccessCondition[:accessEqualCount].
	accessEqualCount int

	TableAsName *model.CIStr

	// dataSourceSchema is the original schema of DataSource. The schema of index scan in KV and index reader in TiDB
	// will be different. The schema of index scan will decode all columns of index but the TiDB only need some of them.
	dataSourceSchema *expression.Schema
}

// PhysicalMemTable reads memory table.
type PhysicalMemTable struct {
	*basePlan
	basePhysicalPlan

	DBName      model.CIStr
	Table       *model.TableInfo
	Columns     []*model.ColumnInfo
	Ranges      []types.IntColumnRange
	TableAsName *model.CIStr

	// NeedColHandle is used in execution phase.
	NeedColHandle bool
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalMemTable) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// physicalDistSQLPlan means the plan that can be executed distributively.
// We can push down other plan like selection, limit, aggregation, topN into this plan.
type physicalDistSQLPlan interface {
	addAggregation(ctx context.Context, agg *PhysicalAggregation) *expression.Schema
	addTopN(ctx context.Context, prop *requiredProperty) bool
	addLimit(limit *Limit)
	calculateCost(resultCount float64, scanCount float64) float64
}

func (p *PhysicalIndexScan) calculateCost(resultCount float64, scanCnt float64) float64 {
	// TODO: Eliminate index cost more precisely.
	cost := resultCount * netWorkFactor
	if p.DoubleRead {
		cost += scanCnt * netWorkFactor
	}
	if len(p.indexFilterConditions) > 0 {
		cost += scanCnt * cpuFactor
	}
	if len(p.tableFilterConditions) > 0 {
		cost += scanCnt * cpuFactor
	}
	// sort cost
	if !p.OutOfOrder && p.DoubleRead {
		cost += scanCnt * cpuFactor
	}
	return cost
}

func (p *PhysicalTableScan) calculateCost(resultCount float64, scanCount float64) float64 {
	cost := resultCount * netWorkFactor
	if len(p.tableFilterConditions) > 0 {
		cost += scanCount * cpuFactor
	}
	return cost
}

type physicalTableSource struct {
	*basePlan
	basePhysicalPlan

	client kv.Client

	Aggregated bool
	readOnly   bool
	AggFuncsPB []*tipb.Expr
	GbyItemsPB []*tipb.ByItem

	// TableConditionPBExpr is the pb structure of conditions that used in the table scan.
	TableConditionPBExpr *tipb.Expr
	// IndexConditionPBExpr is the pb structure of conditions that used in the index scan.
	IndexConditionPBExpr *tipb.Expr

	// AccessCondition is used to calculate range.
	AccessCondition []expression.Expression

	LimitCount  *int64
	SortItemsPB []*tipb.ByItem

	// The following fields are used for explaining and testing. Because pb structures are not human-readable.

	aggFuncs              []aggregation.Aggregation
	gbyItems              []expression.Expression
	sortItems             []*ByItems
	indexFilterConditions []expression.Expression
	tableFilterConditions []expression.Expression

	// filterCondition is only used by new planner.
	filterCondition []expression.Expression

	// NeedColHandle is used in execution phase.
	NeedColHandle bool

	// TODO: This should be removed after old planner was removed.
	unionScanSchema *expression.Schema
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
		buffer.WriteString("\"aggregated push down\": true, \n")
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
	indexFilter, err := json.Marshal(p.indexFilterConditions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableFilter, err := json.Marshal(p.tableFilterConditions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// print condition infos
	buffer.WriteString(fmt.Sprintf("\"access conditions\": %s, \n", access))
	buffer.WriteString(fmt.Sprintf("\"index filter conditions\": %s, \n", indexFilter))
	buffer.WriteString(fmt.Sprintf("\"table filter conditions\": %s}", tableFilter))
	return buffer.Bytes(), nil
}

func (p *physicalTableSource) clearForAggPushDown() {
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

func needCount(af aggregation.Aggregation) bool {
	return af.GetName() == ast.AggFuncCount || af.GetName() == ast.AggFuncAvg
}

func needValue(af aggregation.Aggregation) bool {
	return af.GetName() == ast.AggFuncSum || af.GetName() == ast.AggFuncAvg || af.GetName() == ast.AggFuncFirstRow ||
		af.GetName() == ast.AggFuncMax || af.GetName() == ast.AggFuncMin || af.GetName() == ast.AggFuncGroupConcat
}

func (p *physicalTableSource) tryToAddUnionScan(resultPlan PhysicalPlan, s *expression.Schema) PhysicalPlan {
	if p.readOnly {
		return resultPlan
	}
	conditions := append(p.indexFilterConditions, p.tableFilterConditions...)
	us := PhysicalUnionScan{
		Conditions:    append(conditions, p.AccessCondition...),
		NeedColHandle: p.NeedColHandle,
	}.init(p.allocator, p.ctx)
	us.SetChildren(resultPlan)
	us.SetSchema(s)
	p.NeedColHandle = true
	return us
}

func (p *physicalTableSource) addLimit(l *Limit) {
	if l != nil {
		count := int64(l.Count + l.Offset)
		p.LimitCount = &count
	}
}

func (p *physicalTableSource) addTopN(ctx context.Context, prop *requiredProperty) bool {
	if len(prop.props) == 0 && prop.limit != nil {
		p.addLimit(prop.limit)
		return true
	}
	if p.client == nil || !p.client.IsRequestTypeSupported(kv.ReqTypeSelect, kv.ReqSubTypeTopN) {
		return false
	}
	if prop.limit == nil {
		return false
	}
	sc := ctx.GetSessionVars().StmtCtx
	count := int64(prop.limit.Count + prop.limit.Offset)
	p.LimitCount = &count
	for _, prop := range prop.props {
		item := expression.SortByItemToPB(sc, p.client, prop.col, prop.desc)
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

func (p *physicalTableSource) addAggregation(ctx context.Context, agg *PhysicalAggregation) *expression.Schema {
	if p.client == nil {
		return expression.NewSchema()
	}
	sc := ctx.GetSessionVars().StmtCtx
	for _, f := range agg.AggFuncs {
		pb := aggregation.AggFuncToPBExpr(sc, p.client, f)
		if pb == nil {
			// When we fail to convert any agg function to PB struct, we should clear the environments.
			p.clearForAggPushDown()
			return expression.NewSchema()
		}
		p.AggFuncsPB = append(p.AggFuncsPB, pb)
		p.aggFuncs = append(p.aggFuncs, f.Clone())
	}
	for _, item := range agg.GroupByItems {
		pb := expression.GroupByItemToPB(sc, p.client, item)
		if pb == nil {
			// When we fail to convert any group-by item to PB struct, we should clear the environments.
			p.clearForAggPushDown()
			return expression.NewSchema()
		}
		p.GbyItemsPB = append(p.GbyItemsPB, pb)
		p.gbyItems = append(p.gbyItems, item.Clone())
	}
	p.Aggregated = true
	gkType := types.NewFieldType(mysql.TypeBlob)
	gkType.Charset = charset.CharsetBin
	gkType.Collate = charset.CollationBin
	schema := expression.NewSchema()
	cursor := 0
	schema.Append(&expression.Column{Index: cursor, ColName: model.NewCIStr(fmt.Sprint(agg.GroupByItems)), RetType: gkType})
	agg.GroupByItems = []expression.Expression{schema.Columns[cursor]}
	newAggFuncs := make([]aggregation.Aggregation, len(agg.AggFuncs))
	for i, aggFun := range agg.AggFuncs {
		fun := aggregation.NewAggFunction(aggFun.GetName(), nil, false)
		var args []expression.Expression
		colName := model.NewCIStr(fmt.Sprint(aggFun.GetArgs()))
		if needCount(fun) {
			cursor++
			ft := types.NewFieldType(mysql.TypeLonglong)
			ft.Flen = 21
			ft.Charset = charset.CharsetBin
			ft.Collate = charset.CollationBin
			schema.Append(&expression.Column{Index: cursor, ColName: colName, RetType: ft})
			args = append(args, schema.Columns[cursor])
		}
		if needValue(fun) {
			cursor++
			ft := agg.schema.Columns[i].GetType()
			schema.Append(&expression.Column{Index: cursor, ColName: colName, RetType: ft})
			args = append(args, schema.Columns[cursor])
		}
		fun.SetArgs(args)
		fun.SetMode(aggregation.FinalMode)
		newAggFuncs[i] = fun
	}
	agg.AggFuncs = newAggFuncs
	return schema
}

// PhysicalTableScan represents a table scan plan.
type PhysicalTableScan struct {
	physicalTableSource

	Table   *model.TableInfo
	Columns []*model.ColumnInfo
	DBName  model.CIStr
	Desc    bool
	Ranges  []types.IntColumnRange
	pkCol   *expression.Column

	TableAsName *model.CIStr

	// KeepOrder is true, if sort data by scanning pkcol,
	KeepOrder bool
}

// PhysicalApply represents apply plan, only used for subquery.
type PhysicalApply struct {
	*basePlan
	basePhysicalPlan

	PhysicalJoin PhysicalPlan
	OuterSchema  []*expression.CorrelatedColumn

	rightChOffset int
}

// PhysicalHashJoin represents hash join for inner/ outer join.
type PhysicalHashJoin struct {
	*basePlan
	basePhysicalPlan

	JoinType JoinType

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression
	SmallTable      int
	Concurrency     int

	DefaultValues []types.Datum
}

// PhysicalIndexJoin represents the plan of index look up join.
type PhysicalIndexJoin struct {
	*basePlan
	basePhysicalPlan

	Outer           bool
	OuterJoinKeys   []*expression.Column
	InnerJoinKeys   []*expression.Column
	LeftConditions  expression.CNFExprs
	RightConditions expression.CNFExprs
	OtherConditions expression.CNFExprs
	OuterIndex      int
	KeepOrder       bool
	outerSchema     *expression.Schema
	innerPlan       PhysicalPlan

	DefaultValues []types.Datum
}

// PhysicalMergeJoin represents merge join for inner/ outer join.
type PhysicalMergeJoin struct {
	*basePlan
	basePhysicalPlan

	JoinType JoinType

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression

	DefaultValues []types.Datum
	Desc          bool

	leftKeys  []*expression.Column
	rightKeys []*expression.Column
}

// PhysicalHashSemiJoin represents hash join for semi join.
type PhysicalHashSemiJoin struct {
	*basePlan
	basePhysicalPlan

	WithAux bool
	Anti    bool

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression

	rightChOffset int
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

// String implements fmt.Stringer interface.
func (at AggregationType) String() string {
	switch at {
	case StreamedAgg:
		return "stream"
	case FinalAgg:
		return "final"
	case CompleteAgg:
		return "complete"
	}
	return "unsupported aggregation type"
}

// PhysicalAggregation is Aggregation's physical plan.
type PhysicalAggregation struct {
	*basePlan
	basePhysicalPlan

	HasGby       bool
	AggType      AggregationType
	AggFuncs     []aggregation.Aggregation
	GroupByItems []expression.Expression

	propKeys   []*expression.Column
	inputCount float64 // inputCount is the input count of this plan.
}

// PhysicalUnionScan represents a union scan operator.
type PhysicalUnionScan struct {
	*basePlan
	basePhysicalPlan

	NeedColHandle bool
	Conditions    []expression.Expression
}

// Cache plan is a physical plan which stores the result of its child node.
type Cache struct {
	*basePlan
	basePhysicalPlan
}

func (p *PhysicalMergeJoin) tryConsumeOrder(prop *requiredProperty, eqCond *expression.ScalarFunction) *requiredProperty {
	// TODO: We still can consume a partial sorted results somehow if main key matched.
	// To do that, we need a Sort operator being able to do a secondary sort
	if len(prop.props) != 1 || len(eqCond.GetArgs()) != 2 {
		return prop
	}

	reqSortedColumn := prop.props[0].col
	if prop.props[0].desc {
		return prop
	}

	// Compare either sides of the equal function to see if matches required property
	// If so, we don't have to sort once more
	switch p.JoinType {
	case InnerJoin:
		// In case of inner join, both sides' orders are kept
		lColumn, lOk := eqCond.GetArgs()[0].(*expression.Column)
		rColumn, rOk := eqCond.GetArgs()[1].(*expression.Column)
		if (lOk && lColumn.Equal(reqSortedColumn, p.ctx)) ||
			(rOk && rColumn.Equal(reqSortedColumn, p.ctx)) {
			return removeSortOrder(prop)
		}
	// In case of left/right outer join, driver side's order will be kept
	case LeftOuterJoin:
		lColumn, lOk := eqCond.GetArgs()[0].(*expression.Column)
		if lOk && lColumn.Equal(reqSortedColumn, p.ctx) {
			return removeSortOrder(prop)
		}
	case RightOuterJoin:
		rColumn, rOk := eqCond.GetArgs()[1].(*expression.Column)
		if rOk && rColumn.Equal(reqSortedColumn, p.ctx) {
			return removeSortOrder(prop)
		}
	}
	return prop
}

func (p *PhysicalHashJoin) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := p.basePlan.extractCorrelatedCols()
	for _, fun := range p.EqualConditions {
		corCols = append(corCols, extractCorColumns(fun)...)
	}
	for _, fun := range p.LeftConditions {
		corCols = append(corCols, extractCorColumns(fun)...)
	}
	for _, fun := range p.RightConditions {
		corCols = append(corCols, extractCorColumns(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corCols = append(corCols, extractCorColumns(fun)...)
	}
	return corCols
}

func (p *PhysicalHashSemiJoin) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := p.basePlan.extractCorrelatedCols()
	for _, fun := range p.EqualConditions {
		corCols = append(corCols, extractCorColumns(fun)...)
	}
	for _, fun := range p.LeftConditions {
		corCols = append(corCols, extractCorColumns(fun)...)
	}
	for _, fun := range p.RightConditions {
		corCols = append(corCols, extractCorColumns(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corCols = append(corCols, extractCorColumns(fun)...)
	}
	return corCols
}

func (p *PhysicalApply) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := p.basePlan.extractCorrelatedCols()
	corCols = append(corCols, p.PhysicalJoin.extractCorrelatedCols()...)
	for i := len(corCols) - 1; i >= 0; i-- {
		if p.PhysicalJoin.Children()[0].Schema().Contains(&corCols[i].Column) {
			corCols = append(corCols[:i], corCols[i+1:]...)
		}
	}
	return corCols
}

func (p *PhysicalAggregation) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := p.basePlan.extractCorrelatedCols()
	for _, expr := range p.GroupByItems {
		corCols = append(corCols, extractCorColumns(expr)...)
	}
	for _, fun := range p.AggFuncs {
		for _, arg := range fun.GetArgs() {
			corCols = append(corCols, extractCorColumns(arg)...)
		}
	}
	return corCols
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalIndexScan) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalIndexScan) MarshalJSON() ([]byte, error) {
	pushDownInfo, err := json.Marshal(&p.physicalTableSource)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf(
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

// IsPointGetByUniqueKey checks whether is a point get by unique key.
func (p *PhysicalIndexScan) IsPointGetByUniqueKey(sc *variable.StatementContext) bool {
	return len(p.Ranges) == 1 &&
		p.Index.Unique &&
		len(p.Ranges[0].LowVal) == len(p.Index.Columns) &&
		p.Ranges[0].IsPoint(sc)
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalTableScan) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalTableScan) MarshalJSON() ([]byte, error) {
	pushDownInfo, err := json.Marshal(&p.physicalTableSource)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf(
		" \"db\": \"%s\","+
			"\n \"table\": \"%s\","+
			"\n \"desc\": %v,"+
			"\n \"keep order\": %v,"+
			"\n \"push down info\": %s}",
		p.DBName.O, p.Table.Name.O, p.Desc, p.KeepOrder, pushDownInfo))
	return buffer.Bytes(), nil
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalMemTable) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf(
		" \"db\": \"%s\",\n \"table\": \"%s\"}",
		p.DBName.O, p.Table.Name.O))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalApply) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalApply) MarshalJSON() ([]byte, error) {
	join, err := json.Marshal(p.PhysicalJoin)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	switch x := p.PhysicalJoin.(type) {
	case *PhysicalHashJoin:
		if x.SmallTable == 1 {
			buffer.WriteString(fmt.Sprintf(
				"\"innerPlan\": \"%s\",\n "+
					"\"outerPlan\": \"%s\",\n "+
					"\"join\": %s\n}", p.children[1].ExplainID(), p.children[0].ExplainID(), join))
		} else {
			buffer.WriteString(fmt.Sprintf(
				"\"innerPlan\": \"%s\",\n "+
					"\"outerPlan\": \"%s\",\n "+
					"\"join\": %s\n}", p.children[0].ExplainID(), p.children[1].ExplainID(), join))
		}
	case *PhysicalHashSemiJoin:
		buffer.WriteString(fmt.Sprintf(
			"\"innerPlan\": \"%s\",\n "+
				"\"outerPlan\": \"%s\",\n "+
				"\"join\": %s\n}", p.children[1].ExplainID(), p.children[0].ExplainID(), join))
	}
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalHashSemiJoin) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalHashSemiJoin) MarshalJSON() ([]byte, error) {
	leftChild := p.children[0].(PhysicalPlan)
	rightChild := p.children[1].(PhysicalPlan)
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
		"\"with aux\": %v,"+
			"\"anti\": %v,"+
			"\"eqCond\": %s,\n "+
			"\"leftCond\": %s,\n "+
			"\"rightCond\": %s,\n "+
			"\"otherCond\": %s,\n"+
			"\"leftPlan\": \"%s\",\n "+
			"\"rightPlan\": \"%s\""+
			"}",
		p.WithAux, p.Anti, eqConds, leftConds, rightConds, otherConds, leftChild.ExplainID(), rightChild.ExplainID()))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalHashJoin) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalIndexJoin) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalMergeJoin) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalHashJoin) MarshalJSON() ([]byte, error) {
	leftChild := p.children[0].(PhysicalPlan)
	rightChild := p.children[1].(PhysicalPlan)
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
		"\"eqCond\": %s,\n "+
			"\"leftCond\": %s,\n "+
			"\"rightCond\": %s,\n "+
			"\"otherCond\": %s,\n"+
			"\"leftPlan\": \"%s\",\n "+
			"\"rightPlan\": \"%s\""+
			"}",
		eqConds, leftConds, rightConds, otherConds, leftChild.ExplainID(), rightChild.ExplainID()))
	return buffer.Bytes(), nil
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalMergeJoin) MarshalJSON() ([]byte, error) {
	leftChild := p.children[0].(PhysicalPlan)
	rightChild := p.children[1].(PhysicalPlan)
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
		"\"eqCond\": %s,\n "+
			"\"leftCond\": %s,\n"+
			"\"rightCond\": %s,\n"+
			"\"otherCond\": %s,\n"+
			"\"leftPlan\": \"%s\",\n "+
			"\"rightPlan\": \"%s\",\n"+
			"\"desc\": \"%v\""+
			"}",
		eqConds, leftConds, rightConds, otherConds, leftChild.ExplainID(), rightChild.ExplainID(), p.Desc))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Selection) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *Selection) MarshalJSON() ([]byte, error) {
	conds, err := json.Marshal(p.Conditions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf(""+
		" \"condition\": %s,\n"+
		" \"scanController\": %v,"+
		" \"child\": \"%s\"\n}", conds, p.controllerStatus != notController, p.children[0].ExplainID()))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Projection) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *Projection) MarshalJSON() ([]byte, error) {
	exprs, err := json.Marshal(p.Exprs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf(
		" \"exprs\": %s,\n"+
			" \"child\": \"%s\"\n}", exprs, p.children[0].ExplainID()))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Exists) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *MaxOneRow) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Insert) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Limit) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *Limit) MarshalJSON() ([]byte, error) {
	child := p.children[0].(PhysicalPlan)
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf(
		" \"limit\": %d,\n"+
			" \"offset\": %d,\n"+
			" \"child\": \"%s\"}", p.Count, p.Offset, child.ExplainID()))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Union) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Sort) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *TopN) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *Sort) MarshalJSON() ([]byte, error) {
	exprs, err := json.Marshal(p.ByItems)
	if err != nil {
		return nil, errors.Trace(err)
	}
	limitCount := []byte("null")
	if p.ExecLimit != nil {
		limitCount, err = json.Marshal(p.ExecLimit.Count)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf(
		" \"exprs\": %s,\n"+
			" \"limit\": %s,\n"+
			" \"child\": \"%s\"}", exprs, limitCount, p.children[0].ExplainID()))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *TableDual) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *SelectLock) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalAggregation) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// MarshalJSON implements json.Marshaler interface.
func (p *PhysicalAggregation) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	aggFuncs, err := json.Marshal(p.AggFuncs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gbyExprs, err := json.Marshal(p.GroupByItems)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer.WriteString(fmt.Sprintf(
		"\"AggFuncs\": %s,\n"+
			"\"GroupByItems\": %s,\n"+
			"\"child\": \"%s\"}", aggFuncs, gbyExprs, p.children[0].ExplainID()))
	return buffer.Bytes(), nil
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Update) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Delete) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Show) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalUnionScan) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Cache) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

func buildSchema(p PhysicalPlan) {
	switch x := p.(type) {
	case *Limit, *TopN, *Sort, *Selection, *MaxOneRow, *SelectLock:
		p.SetSchema(p.Children()[0].Schema())
	case *PhysicalHashJoin, *PhysicalMergeJoin, *PhysicalIndexJoin:
		p.SetSchema(expression.MergeSchema(p.Children()[0].Schema(), p.Children()[1].Schema()))
	case *PhysicalApply:
		buildSchema(x.PhysicalJoin)
		x.schema = x.PhysicalJoin.Schema()
	case *PhysicalHashSemiJoin:
		if x.WithAux {
			auxCol := x.schema.Columns[x.Schema().Len()-1]
			x.SetSchema(x.children[0].Schema().Clone())
			x.schema.Append(auxCol)
		} else {
			x.SetSchema(x.children[0].Schema().Clone())
		}
	case *Union:
		panic("Union shouldn't rebuild schema")
	}
}

// rebuildSchema rebuilds the schema for physical plans, because new planner may change indexjoin's schema.
func rebuildSchema(p PhysicalPlan) bool {
	needRebuild := false
	for _, ch := range p.Children() {
		needRebuild = needRebuild || rebuildSchema(ch.(PhysicalPlan))
	}
	if needRebuild {
		buildSchema(p)
	}
	switch p.(type) {
	case *PhysicalIndexJoin, *PhysicalHashJoin, *PhysicalMergeJoin:
		needRebuild = true
	case *Projection, *PhysicalAggregation:
		needRebuild = false
	}
	return needRebuild
}
