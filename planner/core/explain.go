// Copyright 2017 PingCAP, Inc.
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

package core

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/stringutil"
)

// A plan is dataAccesser means it can access underlying data.
// Include `PhysicalTableScan`, `PhysicalIndexScan`, `PointGetPlan`, `BatchPointScan` and `PhysicalMemTable`.
// ExplainInfo = AccessObject + OperatorInfo
type dataAccesser interface {

	// AccessObject return plan's `table`, `partition` and `index`.
	AccessObject() string

	// OperatorInfo return other operator information to be explained.
	OperatorInfo(normalized bool) string
}

// ExplainInfo implements Plan interface.
func (p *PhysicalLock) ExplainInfo() string {
	return p.Lock.String()
}

// ExplainID overrides the ExplainID in order to match different range.
func (p *PhysicalIndexScan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		if p.isFullScan() {
			return "IndexFullScan_" + strconv.Itoa(p.id)
		}
		return "IndexRangeScan_" + strconv.Itoa(p.id)
	})
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexScan) ExplainInfo() string {
	return p.AccessObject() + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexScan) ExplainNormalizedInfo() string {
	return p.AccessObject() + ", " + p.OperatorInfo(true)
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalIndexScan) AccessObject() string {
	buffer := bytes.NewBufferString("")
	tblName := p.Table.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		tblName = p.TableAsName.O
	}
	fmt.Fprintf(buffer, "table:%s", tblName)
	if p.isPartition {
		if pi := p.Table.GetPartitionInfo(); pi != nil {
			partitionName := pi.GetNameByID(p.physicalTableID)
			fmt.Fprintf(buffer, ", partition:%s", partitionName)
		}
	}
	if len(p.Index.Columns) > 0 {
		buffer.WriteString(", index:" + p.Index.Name.O + "(")
		for i, idxCol := range p.Index.Columns {
			buffer.WriteString(idxCol.Name.O)
			if i+1 < len(p.Index.Columns) {
				buffer.WriteString(", ")
			}
		}
		buffer.WriteString(")")
	}
	return buffer.String()
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalIndexScan) OperatorInfo(normalized bool) string {
	buffer := bytes.NewBufferString("")
	if len(p.rangeInfo) > 0 {
		fmt.Fprintf(buffer, "range: decided by %v, ", p.rangeInfo)
	} else if p.haveCorCol() {
		if normalized {
			fmt.Fprintf(buffer, "range: decided by %s, ", expression.SortedExplainNormalizedExpressionList(p.AccessCondition))
		} else {
			fmt.Fprintf(buffer, "range: decided by %v, ", p.AccessCondition)
		}
	} else if len(p.Ranges) > 0 {
		if normalized {
			fmt.Fprint(buffer, "range:[?,?], ")
		} else if !p.isFullScan() {
			fmt.Fprint(buffer, "range:")
			for _, idxRange := range p.Ranges {
				fmt.Fprint(buffer, idxRange.String()+", ")
			}
		}
	}
	fmt.Fprintf(buffer, "keep order:%v, ", p.KeepOrder)
	if p.Desc {
		buffer.WriteString("desc, ")
	}
	if p.stats.StatsVersion == statistics.PseudoVersion && !normalized {
		buffer.WriteString("stats:pseudo, ")
	}
	buffer.Truncate(buffer.Len() - 2)
	return buffer.String()
}

func (p *PhysicalIndexScan) haveCorCol() bool {
	for _, cond := range p.AccessCondition {
		if len(expression.ExtractCorColumns(cond)) > 0 {
			return true
		}
	}
	return false
}

func (p *PhysicalIndexScan) isFullScan() bool {
	if len(p.rangeInfo) > 0 || p.haveCorCol() {
		return false
	}
	for _, ran := range p.Ranges {
		if !ran.IsFullRange() {
			return false
		}
	}
	return true
}

// ExplainID overrides the ExplainID in order to match different range.
func (p *PhysicalTableScan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		if p.isChildOfIndexLookUp {
			return "TableRowIDScan_" + strconv.Itoa(p.id)
		} else if p.isFullScan() {
			return "TableFullScan_" + strconv.Itoa(p.id)
		}
		return "TableRangeScan_" + strconv.Itoa(p.id)
	})
}

// ExplainInfo implements Plan interface.
func (p *PhysicalTableScan) ExplainInfo() string {
	return p.AccessObject() + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalTableScan) ExplainNormalizedInfo() string {
	return p.AccessObject() + ", " + p.OperatorInfo(true)
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalTableScan) AccessObject() string {
	buffer := bytes.NewBufferString("")
	tblName := p.Table.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		tblName = p.TableAsName.O
	}
	fmt.Fprintf(buffer, "table:%s", tblName)
	if p.isPartition {
		if pi := p.Table.GetPartitionInfo(); pi != nil {
			partitionName := pi.GetNameByID(p.physicalTableID)
			fmt.Fprintf(buffer, ", partition:%s", partitionName)
		}
	}
	return buffer.String()
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalTableScan) OperatorInfo(normalized bool) string {
	buffer := bytes.NewBufferString("")
	if p.HandleCol != nil {
		fmt.Fprintf(buffer, "pk col:%s, ", p.HandleCol.ExplainInfo())
	}
	if len(p.RangeDecidedBy) > 0 {
		fmt.Fprintf(buffer, ", range: decided by %v", p.RangeDecidedBy)
	} else if p.haveCorCol() {
		if normalized {
			fmt.Fprintf(buffer, "range: decided by %s, ", expression.SortedExplainNormalizedExpressionList(p.AccessCondition))
		} else {
			fmt.Fprintf(buffer, "range: decided by %v, ", p.AccessCondition)
		}
	} else if len(p.Ranges) > 0 {
		if normalized {
			fmt.Fprint(buffer, "range:[?,?], ")
		} else if !p.isFullScan() {
			fmt.Fprint(buffer, "range:")
			for _, idxRange := range p.Ranges {
				fmt.Fprint(buffer, idxRange.String()+", ")
			}
		}
	}
	fmt.Fprintf(buffer, "keep order:%v, ", p.KeepOrder)
	if p.Desc {
		buffer.WriteString("desc, ")
	}
	if p.stats.StatsVersion == statistics.PseudoVersion && !normalized {
		buffer.WriteString("stats:pseudo, ")
	}
	buffer.Truncate(buffer.Len() - 2)
	return buffer.String()
}

func (p *PhysicalTableScan) haveCorCol() bool {
	for _, cond := range p.AccessCondition {
		if len(expression.ExtractCorColumns(cond)) > 0 {
			return true
		}
	}
	return false
}

func (p *PhysicalTableScan) isFullScan() bool {
	if len(p.RangeDecidedBy) > 0 || p.haveCorCol() {
		return false
	}
	for _, ran := range p.Ranges {
		if !ran.IsFullRange() {
			return false
		}
	}
	return true
}

// ExplainInfo implements Plan interface.
func (p *PhysicalTableReader) ExplainInfo() string {
	return "data:" + p.tablePlan.ExplainID().String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalTableReader) ExplainNormalizedInfo() string {
	return p.ExplainInfo()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexReader) ExplainInfo() string {
	return "index:" + p.indexPlan.ExplainID().String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexReader) ExplainNormalizedInfo() string {
	return p.ExplainInfo()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexLookUpReader) ExplainInfo() string {
	// The children can be inferred by the relation symbol.
	if p.PushedLimit != nil {
		return fmt.Sprintf("limit embedded(offset:%v, count:%v)", p.PushedLimit.Offset, p.PushedLimit.Count)
	}
	return ""
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexMergeReader) ExplainInfo() string {
	return ""
}

// ExplainInfo implements Plan interface.
func (p *PhysicalUnionScan) ExplainInfo() string {
	return string(expression.SortedExplainExpressionList(p.Conditions))
}

// ExplainInfo implements Plan interface.
func (p *PhysicalSelection) ExplainInfo() string {
	return string(expression.SortedExplainExpressionList(p.Conditions))
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalSelection) ExplainNormalizedInfo() string {
	return string(expression.SortedExplainNormalizedExpressionList(p.Conditions))
}

// ExplainInfo implements Plan interface.
func (p *PhysicalProjection) ExplainInfo() string {
	return expression.ExplainExpressionList(p.Exprs, p.schema)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalProjection) ExplainNormalizedInfo() string {
	return string(expression.SortedExplainNormalizedExpressionList(p.Exprs))
}

// ExplainInfo implements Plan interface.
func (p *PhysicalTableDual) ExplainInfo() string {
	return fmt.Sprintf("rows:%v", p.RowCount)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalSort) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	return explainByItems(buffer, p.ByItems).String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalLimit) ExplainInfo() string {
	return fmt.Sprintf("offset:%v, count:%v", p.Offset, p.Count)
}

// ExplainInfo implements Plan interface.
func (p *basePhysicalAgg) ExplainInfo() string {
	return p.explainInfo(false)
}

func (p *basePhysicalAgg) explainInfo(normalized bool) string {
	sortedExplainExpressionList := expression.SortedExplainExpressionList
	if normalized {
		sortedExplainExpressionList = expression.SortedExplainNormalizedExpressionList
	}

	builder := &strings.Builder{}
	if len(p.GroupByItems) > 0 {
		fmt.Fprintf(builder, "group by:%s, ",
			sortedExplainExpressionList(p.GroupByItems))
	}
	for i := 0; i < len(p.AggFuncs); i++ {
		builder.WriteString("funcs:")
		fmt.Fprintf(builder, "%v->%v", aggregation.ExplainAggFunc(p.AggFuncs[i]), p.schema.Columns[i])
		if i+1 < len(p.AggFuncs) {
			builder.WriteString(", ")
		}
	}
	return builder.String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *basePhysicalAgg) ExplainNormalizedInfo() string {
	return p.explainInfo(true)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexJoin) ExplainInfo() string {
	return p.explainInfo(false)
}

func (p *PhysicalIndexJoin) explainInfo(normalized bool) string {
	sortedExplainExpressionList := expression.SortedExplainExpressionList
	if normalized {
		sortedExplainExpressionList = expression.SortedExplainNormalizedExpressionList
	}

	buffer := bytes.NewBufferString(p.JoinType.String())
	fmt.Fprintf(buffer, ", inner:%s", p.Children()[p.InnerChildIdx].ExplainID())
	if len(p.OuterJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", outer key:%s",
			expression.ExplainColumnList(p.OuterJoinKeys))
	}
	if len(p.InnerJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", inner key:%s",
			expression.ExplainColumnList(p.InnerJoinKeys))
	}
	if len(p.LeftConditions) > 0 {
		fmt.Fprintf(buffer, ", left cond:%s",
			sortedExplainExpressionList(p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		fmt.Fprintf(buffer, ", right cond:%s",
			sortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		fmt.Fprintf(buffer, ", other cond:%s",
			sortedExplainExpressionList(p.OtherConditions))
	}
	return buffer.String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexJoin) ExplainNormalizedInfo() string {
	return p.explainInfo(true)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalHashJoin) ExplainInfo() string {
	return p.explainInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalHashJoin) ExplainNormalizedInfo() string {
	return p.explainInfo(true)
}

func (p *PhysicalHashJoin) explainInfo(normalized bool) string {
	sortedExplainExpressionList := expression.SortedExplainExpressionList
	if normalized {
		sortedExplainExpressionList = expression.SortedExplainNormalizedExpressionList
	}

	buffer := new(bytes.Buffer)

	if len(p.EqualConditions) == 0 {
		buffer.WriteString("CARTESIAN ")
	}

	buffer.WriteString(p.JoinType.String())

	if len(p.EqualConditions) > 0 {
		if normalized {
			fmt.Fprintf(buffer, ", equal:%s", expression.SortedExplainNormalizedScalarFuncList(p.EqualConditions))
		} else {
			fmt.Fprintf(buffer, ", equal:%v", p.EqualConditions)
		}
	}
	if len(p.LeftConditions) > 0 {
		if normalized {
			fmt.Fprintf(buffer, ", left cond:%s", expression.SortedExplainNormalizedExpressionList(p.LeftConditions))
		} else {
			fmt.Fprintf(buffer, ", left cond:%s", p.LeftConditions)
		}
	}
	if len(p.RightConditions) > 0 {
		fmt.Fprintf(buffer, ", right cond:%s",
			sortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		fmt.Fprintf(buffer, ", other cond:%s",
			sortedExplainExpressionList(p.OtherConditions))
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalMergeJoin) ExplainInfo() string {
	return p.explainInfo(false)
}

func (p *PhysicalMergeJoin) explainInfo(normalized bool) string {
	sortedExplainExpressionList := expression.SortedExplainExpressionList
	if normalized {
		sortedExplainExpressionList = expression.SortedExplainNormalizedExpressionList
	}

	buffer := bytes.NewBufferString(p.JoinType.String())
	if len(p.LeftJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", left key:%s",
			expression.ExplainColumnList(p.LeftJoinKeys))
	}
	if len(p.RightJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", right key:%s",
			expression.ExplainColumnList(p.RightJoinKeys))
	}
	if len(p.LeftConditions) > 0 {
		if normalized {
			fmt.Fprintf(buffer, ", left cond:%s", expression.SortedExplainNormalizedExpressionList(p.LeftConditions))
		} else {
			fmt.Fprintf(buffer, ", left cond:%s", p.LeftConditions)
		}
	}
	if len(p.RightConditions) > 0 {
		fmt.Fprintf(buffer, ", right cond:%s",
			sortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		fmt.Fprintf(buffer, ", other cond:%s",
			sortedExplainExpressionList(p.OtherConditions))
	}
	return buffer.String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalMergeJoin) ExplainNormalizedInfo() string {
	return p.explainInfo(true)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalTopN) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	buffer = explainByItems(buffer, p.ByItems)
	fmt.Fprintf(buffer, ", offset:%v, count:%v", p.Offset, p.Count)
	return buffer.String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalTopN) ExplainNormalizedInfo() string {
	buffer := bytes.NewBufferString("")
	buffer = explainNormalizedByItems(buffer, p.ByItems)
	return buffer.String()
}

func (p *PhysicalWindow) formatFrameBound(buffer *bytes.Buffer, bound *FrameBound) {
	if bound.Type == ast.CurrentRow {
		buffer.WriteString("current row")
		return
	}
	if bound.UnBounded {
		buffer.WriteString("unbounded")
	} else if len(bound.CalcFuncs) > 0 {
		sf := bound.CalcFuncs[0].(*expression.ScalarFunction)
		switch sf.FuncName.L {
		case ast.DateAdd, ast.DateSub:
			// For `interval '2:30' minute_second`.
			fmt.Fprintf(buffer, "interval %s %s", sf.GetArgs()[1].ExplainInfo(), sf.GetArgs()[2].ExplainInfo())
		case ast.Plus, ast.Minus:
			// For `1 preceding` of range frame.
			fmt.Fprintf(buffer, "%s", sf.GetArgs()[1].ExplainInfo())
		}
	} else {
		fmt.Fprintf(buffer, "%d", bound.Num)
	}
	if bound.Type == ast.Preceding {
		buffer.WriteString(" preceding")
	} else {
		buffer.WriteString(" following")
	}
}

// ExplainInfo implements Plan interface.
func (p *PhysicalWindow) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	formatWindowFuncDescs(buffer, p.WindowFuncDescs, p.schema)
	buffer.WriteString(" over(")
	isFirst := true
	if len(p.PartitionBy) > 0 {
		buffer.WriteString("partition by ")
		for i, item := range p.PartitionBy {
			fmt.Fprintf(buffer, "%s", item.Col.ExplainInfo())
			if i+1 < len(p.PartitionBy) {
				buffer.WriteString(", ")
			}
		}
		isFirst = false
	}
	if len(p.OrderBy) > 0 {
		if !isFirst {
			buffer.WriteString(" ")
		}
		buffer.WriteString("order by ")
		for i, item := range p.OrderBy {
			order := "asc"
			if item.Desc {
				order = "desc"
			}
			fmt.Fprintf(buffer, "%s %s", item.Col.ExplainInfo(), order)
			if i+1 < len(p.OrderBy) {
				buffer.WriteString(", ")
			}
		}
		isFirst = false
	}
	if p.Frame != nil {
		if !isFirst {
			buffer.WriteString(" ")
		}
		if p.Frame.Type == ast.Rows {
			buffer.WriteString("rows")
		} else {
			buffer.WriteString("range")
		}
		buffer.WriteString(" between ")
		p.formatFrameBound(buffer, p.Frame.Start)
		buffer.WriteString(" and ")
		p.formatFrameBound(buffer, p.Frame.End)
	}
	buffer.WriteString(")")
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalShuffle) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "execution info: concurrency:%v, data source:%v", p.Concurrency, p.DataSource.ExplainID())
	return buffer.String()
}

func formatWindowFuncDescs(buffer *bytes.Buffer, descs []*aggregation.WindowFuncDesc, schema *expression.Schema) *bytes.Buffer {
	winFuncStartIdx := len(schema.Columns) - len(descs)
	for i, desc := range descs {
		if i != 0 {
			buffer.WriteString(", ")
		}
		fmt.Fprintf(buffer, "%v->%v", desc, schema.Columns[winFuncStartIdx+i])
	}
	return buffer
}

// ExplainInfo implements Plan interface.
func (p *LogicalJoin) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.JoinType.String())
	if len(p.EqualConditions) > 0 {
		fmt.Fprintf(buffer, ", equal:%v", p.EqualConditions)
	}
	if len(p.LeftConditions) > 0 {
		fmt.Fprintf(buffer, ", left cond:%s",
			expression.SortedExplainExpressionList(p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		fmt.Fprintf(buffer, ", right cond:%s",
			expression.SortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		fmt.Fprintf(buffer, ", other cond:%s",
			expression.SortedExplainExpressionList(p.OtherConditions))
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalAggregation) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	if len(p.GroupByItems) > 0 {
		fmt.Fprintf(buffer, "group by:%s, ",
			expression.SortedExplainExpressionList(p.GroupByItems))
	}
	if len(p.AggFuncs) > 0 {
		buffer.WriteString("funcs:")
		for i, agg := range p.AggFuncs {
			buffer.WriteString(aggregation.ExplainAggFunc(agg))
			if i+1 < len(p.AggFuncs) {
				buffer.WriteString(", ")
			}
		}
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalProjection) ExplainInfo() string {
	return expression.ExplainExpressionList(p.Exprs, p.schema)
}

// ExplainInfo implements Plan interface.
func (p *LogicalSelection) ExplainInfo() string {
	return string(expression.SortedExplainExpressionList(p.Conditions))
}

// ExplainInfo implements Plan interface.
func (p *LogicalApply) ExplainInfo() string {
	return p.LogicalJoin.ExplainInfo()
}

// ExplainInfo implements Plan interface.
func (p *LogicalTableDual) ExplainInfo() string {
	return fmt.Sprintf("rowcount:%d", p.RowCount)
}

// ExplainInfo implements Plan interface.
func (p *DataSource) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	tblName := p.tableInfo.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		tblName = p.TableAsName.O
	}
	fmt.Fprintf(buffer, "table:%s", tblName)
	if p.isPartition {
		if pi := p.tableInfo.GetPartitionInfo(); pi != nil {
			partitionName := pi.GetNameByID(p.physicalTableID)
			fmt.Fprintf(buffer, ", partition:%s", partitionName)
		}
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalUnionScan) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "conds:%s",
		expression.SortedExplainExpressionList(p.conditions))
	fmt.Fprintf(buffer, ", handle:%s", p.handleCol.ExplainInfo())
	return buffer.String()
}

func explainByItems(buffer *bytes.Buffer, byItems []*ByItems) *bytes.Buffer {
	for i, item := range byItems {
		order := "asc"
		if item.Desc {
			order = "desc"
		}
		fmt.Fprintf(buffer, "%s:%s", item.Expr.ExplainInfo(), order)
		if i+1 < len(byItems) {
			buffer.WriteString(", ")
		}
	}
	return buffer
}

func explainNormalizedByItems(buffer *bytes.Buffer, byItems []*ByItems) *bytes.Buffer {
	for i, item := range byItems {
		order := "asc"
		if item.Desc {
			order = "desc"
		}
		fmt.Fprintf(buffer, "%s:%s", item.Expr.ExplainNormalizedInfo(), order)
		if i+1 < len(byItems) {
			buffer.WriteString(", ")
		}
	}
	return buffer
}

// ExplainInfo implements Plan interface.
func (p *LogicalSort) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	return explainByItems(buffer, p.ByItems).String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalTopN) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	buffer = explainByItems(buffer, p.ByItems)
	fmt.Fprintf(buffer, ", offset:%v, count:%v", p.Offset, p.Count)
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalLimit) ExplainInfo() string {
	return fmt.Sprintf("offset:%v, count:%v", p.Offset, p.Count)
}

// ExplainInfo implements Plan interface.
func (p *LogicalTableScan) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.Source.ExplainInfo())
	if p.Handle != nil {
		fmt.Fprintf(buffer, ", pk col:%s", p.Handle.ExplainInfo())
	}
	if len(p.AccessConds) > 0 {
		fmt.Fprintf(buffer, ", cond:%v", p.AccessConds)
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (is *LogicalIndexScan) ExplainInfo() string {
	buffer := bytes.NewBufferString(is.Source.ExplainInfo())
	index := is.Index
	if len(index.Columns) > 0 {
		buffer.WriteString(", index:")
		for i, idxCol := range index.Columns {
			buffer.WriteString(idxCol.Name.O)
			if i+1 < len(index.Columns) {
				buffer.WriteString(", ")
			}
		}
	}
	if len(is.AccessConds) > 0 {
		fmt.Fprintf(buffer, ", cond:%v", is.AccessConds)
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (sg *TiKVSingleGather) ExplainInfo() string {
	buffer := bytes.NewBufferString(sg.Source.ExplainInfo())
	if sg.IsIndexGather {
		buffer.WriteString(", index:" + sg.Index.Name.String())
	}
	return buffer.String()
}

// MetricTableTimeFormat is the time format for metric table explain and format.
const MetricTableTimeFormat = "2006-01-02 15:04:05.999"

// ExplainInfo implements Plan interface.
func (dg *TiKVDoubleGather) ExplainInfo() string {
	buffer := bytes.NewBufferString(dg.Source.ExplainInfo())
	buffer.WriteString(", index:" + dg.index.Name.String())
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalMemTable) ExplainInfo() string {
	accessObject, operatorInfo := p.AccessObject(), p.OperatorInfo(false)
	if len(operatorInfo) == 0 {
		return accessObject
	}
	return accessObject + ", " + operatorInfo
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalMemTable) AccessObject() string {
	return "table:" + p.Table.Name.O
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalMemTable) OperatorInfo(_ bool) string {
	if p.Extractor != nil {
		return p.Extractor.explainInfo(p)
	}
	return ""
}
