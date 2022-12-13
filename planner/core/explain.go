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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

// A plan is dataAccesser means it can access underlying data.
// Include `PhysicalTableScan`, `PhysicalIndexScan`, `PointGetPlan`, `BatchPointScan` and `PhysicalMemTable`.
// ExplainInfo = AccessObject + OperatorInfo
type dataAccesser interface {

	// AccessObject return plan's `table`, `partition` and `index`.
	AccessObject(normalized bool) string

	// OperatorInfo return other operator information to be explained.
	OperatorInfo(normalized bool) string
}

type partitionAccesser interface {
	accessObject(sessionctx.Context) string
}

// ExplainInfo implements Plan interface.
func (p *PhysicalLock) ExplainInfo() string {
	return fmt.Sprintf("%s %v", p.Lock.LockType.String(), p.Lock.WaitSec)
}

// ExplainID overrides the ExplainID in order to match different range.
func (p *PhysicalIndexScan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		if p.ctx != nil && p.ctx.GetSessionVars().StmtCtx.IgnoreExplainIDSuffix {
			return p.TP()
		}
		return p.TP() + "_" + strconv.Itoa(p.id)
	})
}

// TP overrides the TP in order to match different range.
func (p *PhysicalIndexScan) TP() string {
	if p.isFullScan() {
		return plancodec.TypeIndexFullScan
	}
	return plancodec.TypeIndexRangeScan
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexScan) ExplainInfo() string {
	return p.AccessObject(false) + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexScan) ExplainNormalizedInfo() string {
	return p.AccessObject(true) + ", " + p.OperatorInfo(true)
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalIndexScan) AccessObject(normalized bool) string {
	buffer := bytes.NewBufferString("")
	tblName := p.Table.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		tblName = p.TableAsName.O
	}
	fmt.Fprintf(buffer, "table:%s", tblName)
	if p.isPartition {
		if normalized {
			fmt.Fprintf(buffer, ", partition:?")
		} else if pi := p.Table.GetPartitionInfo(); pi != nil {
			partitionName := pi.GetNameByID(p.physicalTableID)
			fmt.Fprintf(buffer, ", partition:%s", partitionName)
		}
	}
	if len(p.Index.Columns) > 0 {
		buffer.WriteString(", index:" + p.Index.Name.O + "(")
		for i, idxCol := range p.Index.Columns {
			if tblCol := p.Table.Columns[idxCol.Offset]; tblCol.Hidden {
				buffer.WriteString(tblCol.GeneratedExprString)
			} else {
				buffer.WriteString(idxCol.Name.O)
			}
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
		if !normalized {
			fmt.Fprintf(buffer, "range: decided by %v, ", p.rangeInfo)
		}
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
		if p.ctx != nil && p.ctx.GetSessionVars().StmtCtx.IgnoreExplainIDSuffix {
			return p.TP()
		}
		return p.TP() + "_" + strconv.Itoa(p.id)
	})
}

// TP overrides the TP in order to match different range.
func (p *PhysicalTableScan) TP() string {
	if p.isChildOfIndexLookUp {
		return plancodec.TypeTableRowIDScan
	} else if p.isFullScan() {
		return plancodec.TypeTableFullScan
	}
	return plancodec.TypeTableRangeScan
}

// ExplainInfo implements Plan interface.
func (p *PhysicalTableScan) ExplainInfo() string {
	return p.AccessObject(false) + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalTableScan) ExplainNormalizedInfo() string {
	return p.AccessObject(true) + ", " + p.OperatorInfo(true)
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalTableScan) AccessObject(normalized bool) string {
	buffer := bytes.NewBufferString("")
	tblName := p.Table.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		tblName = p.TableAsName.O
	}
	fmt.Fprintf(buffer, "table:%s", tblName)
	if p.isPartition {
		if normalized {
			fmt.Fprintf(buffer, ", partition:?")
		} else if pi := p.Table.GetPartitionInfo(); pi != nil {
			partitionName := pi.GetNameByID(p.physicalTableID)
			fmt.Fprintf(buffer, ", partition:%s", partitionName)
		}
	}
	return buffer.String()
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalTableScan) OperatorInfo(normalized bool) string {
	buffer := bytes.NewBufferString("")
	for i, pkCol := range p.PkCols {
		var fmtStr string
		switch i {
		case 0:
			fmtStr = "pk cols: (%s, "
		case len(p.PkCols) - 1:
			fmtStr = "%s)"
		default:
			fmtStr = "%s, "
		}
		fmt.Fprintf(buffer, fmtStr, pkCol.ExplainInfo())
	}
	if len(p.rangeDecidedBy) > 0 {
		fmt.Fprintf(buffer, "range: decided by %v, ", p.rangeDecidedBy)
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
	if p.IsGlobalRead {
		buffer.WriteString("global read, ")
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
	if len(p.rangeDecidedBy) > 0 || p.haveCorCol() {
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
	return ""
}

func (p *PhysicalTableReader) accessObject(sctx sessionctx.Context) string {
	ts := p.TablePlans[0].(*PhysicalTableScan)
	pi := ts.Table.GetPartitionInfo()
	if pi == nil || !sctx.GetSessionVars().UseDynamicPartitionPrune() {
		return ""
	}

	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	tmp, ok := is.TableByID(ts.Table.ID)
	if !ok {
		return "partition table not found" + strconv.FormatInt(ts.Table.ID, 10)
	}
	tbl := tmp.(table.PartitionedTable)

	return partitionAccessObject(sctx, tbl, pi, &p.PartitionInfo)
}

func partitionAccessObject(sctx sessionctx.Context, tbl table.PartitionedTable, pi *model.PartitionInfo, partTable *PartitionInfo) string {
	var buffer bytes.Buffer
	idxArr, err := PartitionPruning(sctx, tbl, partTable.PruningConds, partTable.PartitionNames, partTable.Columns, partTable.ColumnNames)
	if err != nil {
		return "partition pruning error" + err.Error()
	}

	if len(idxArr) == 0 {
		return "partition:dual"
	}

	if len(idxArr) == 1 && idxArr[0] == FullRange {
		return "partition:all"
	}

	for i, idx := range idxArr {
		if i == 0 {
			buffer.WriteString("partition:")
		} else {
			buffer.WriteString(",")
		}
		buffer.WriteString(pi.Definitions[idx].Name.O)
	}

	return buffer.String()
}

// OperatorInfo return other operator information to be explained.
func (p *PhysicalTableReader) OperatorInfo(normalized bool) string {
	return "data:" + p.tablePlan.ExplainID().String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexReader) ExplainInfo() string {
	return "index:" + p.indexPlan.ExplainID().String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexReader) ExplainNormalizedInfo() string {
	return p.ExplainInfo()
}

func (p *PhysicalIndexReader) accessObject(sctx sessionctx.Context) string {
	ts := p.IndexPlans[0].(*PhysicalIndexScan)
	pi := ts.Table.GetPartitionInfo()
	if pi == nil || !sctx.GetSessionVars().UseDynamicPartitionPrune() {
		return ""
	}

	var buffer bytes.Buffer
	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	tmp, ok := is.TableByID(ts.Table.ID)
	if !ok {
		fmt.Fprintf(&buffer, "partition table not found: %d", ts.Table.ID)
		return buffer.String()
	}

	tbl := tmp.(table.PartitionedTable)
	return partitionAccessObject(sctx, tbl, pi, &p.PartitionInfo)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexLookUpReader) ExplainInfo() string {
	// The children can be inferred by the relation symbol.
	if p.PushedLimit != nil {
		return fmt.Sprintf("limit embedded(offset:%v, count:%v)", p.PushedLimit.Offset, p.PushedLimit.Count)
	}
	return ""
}

func (p *PhysicalIndexLookUpReader) accessObject(sctx sessionctx.Context) string {
	ts := p.TablePlans[0].(*PhysicalTableScan)
	pi := ts.Table.GetPartitionInfo()
	if pi == nil || !sctx.GetSessionVars().UseDynamicPartitionPrune() {
		return ""
	}

	var buffer bytes.Buffer
	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	tmp, ok := is.TableByID(ts.Table.ID)
	if !ok {
		fmt.Fprintf(&buffer, "partition table not found: %d", ts.Table.ID)
		return buffer.String()
	}

	tbl := tmp.(table.PartitionedTable)
	return partitionAccessObject(sctx, tbl, pi, &p.PartitionInfo)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexMergeReader) ExplainInfo() string {
	return ""
}

func (p *PhysicalIndexMergeReader) accessObject(sctx sessionctx.Context) string {
	ts := p.TablePlans[0].(*PhysicalTableScan)
	pi := ts.Table.GetPartitionInfo()
	if pi == nil || !sctx.GetSessionVars().UseDynamicPartitionPrune() {
		return ""
	}

	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	tmp, ok := is.TableByID(ts.Table.ID)
	if !ok {
		return "partition table not found" + strconv.FormatInt(ts.Table.ID, 10)
	}
	tbl := tmp.(table.PartitionedTable)

	return partitionAccessObject(sctx, tbl, pi, &p.PartitionInfo)
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
		var colName string
		if normalized {
			colName = p.schema.Columns[i].ExplainNormalizedInfo()
		} else {
			colName = p.schema.Columns[i].ExplainInfo()
		}
		fmt.Fprintf(builder, "%v->%v", aggregation.ExplainAggFunc(p.AggFuncs[i], normalized), colName)
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
	return p.explainInfo(false, false)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexMergeJoin) ExplainInfo() string {
	return p.explainInfo(false, true)
}

func (p *PhysicalIndexJoin) explainInfo(normalized bool, isIndexMergeJoin bool) string {
	sortedExplainExpressionList := expression.SortedExplainExpressionList
	if normalized {
		sortedExplainExpressionList = expression.SortedExplainNormalizedExpressionList
	}

	buffer := bytes.NewBufferString(p.JoinType.String())
	if normalized {
		fmt.Fprintf(buffer, ", inner:%s", p.Children()[p.InnerChildIdx].TP())
	} else {
		fmt.Fprintf(buffer, ", inner:%s", p.Children()[p.InnerChildIdx].ExplainID())
	}
	if len(p.OuterJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", outer key:%s",
			expression.ExplainColumnList(p.OuterJoinKeys))
	}
	if len(p.InnerJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", inner key:%s",
			expression.ExplainColumnList(p.InnerJoinKeys))
	}

	if len(p.OuterHashKeys) > 0 && !isIndexMergeJoin {
		exprs := make([]expression.Expression, 0, len(p.OuterHashKeys))
		for i := range p.OuterHashKeys {
			expr, err := expression.NewFunctionBase(MockContext(), ast.EQ, types.NewFieldType(mysql.TypeLonglong), p.OuterHashKeys[i], p.InnerHashKeys[i])
			if err != nil {
			}
			exprs = append(exprs, expr)
		}
		fmt.Fprintf(buffer, ", equal cond:%s",
			sortedExplainExpressionList(exprs))
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
	return p.explainInfo(true, false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexMergeJoin) ExplainNormalizedInfo() string {
	return p.explainInfo(true, true)
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
			if item.Desc {
				fmt.Fprintf(buffer, "%s desc", item.Col.ExplainInfo())
			} else {
				fmt.Fprintf(buffer, "%s", item.Col.ExplainInfo())
			}

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
	explainIds := make([]fmt.Stringer, len(p.DataSources))
	for i := range p.DataSources {
		explainIds[i] = p.DataSources[i].ExplainID()
	}

	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "execution info: concurrency:%v, data sources:%v", p.Concurrency, explainIds)
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
			buffer.WriteString(aggregation.ExplainAggFunc(agg, false))
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
func (ds *DataSource) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	tblName := ds.tableInfo.Name.O
	if ds.TableAsName != nil && ds.TableAsName.O != "" {
		tblName = ds.TableAsName.O
	}
	fmt.Fprintf(buffer, "table:%s", tblName)
	if ds.isPartition {
		if pi := ds.tableInfo.GetPartitionInfo(); pi != nil {
			partitionName := pi.GetNameByID(ds.physicalTableID)
			fmt.Fprintf(buffer, ", partition:%s", partitionName)
		}
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalExchangeSender) ExplainInfo() string {
	buffer := bytes.NewBufferString("ExchangeType: ")
	switch p.ExchangeType {
	case tipb.ExchangeType_PassThrough:
		fmt.Fprintf(buffer, "PassThrough")
	case tipb.ExchangeType_Broadcast:
		fmt.Fprintf(buffer, "Broadcast")
	case tipb.ExchangeType_Hash:
		fmt.Fprintf(buffer, "HashPartition")
		fmt.Fprintf(buffer, ", Hash Cols: %s", expression.ExplainColumnList(p.HashCols))
	}
	if len(p.Tasks) > 0 {
		fmt.Fprintf(buffer, ", tasks: [")
		for idx, task := range p.Tasks {
			if idx != 0 {
				fmt.Fprintf(buffer, ", ")
			}
			fmt.Fprintf(buffer, "%v", task.ID)
		}
		fmt.Fprintf(buffer, "]")
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalUnionScan) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "conds:%s",
		expression.SortedExplainExpressionList(p.conditions))
	fmt.Fprintf(buffer, ", handle:%s", p.handleCols)
	return buffer.String()
}

func explainByItems(buffer *bytes.Buffer, byItems []*util.ByItems) *bytes.Buffer {
	for i, item := range byItems {
		if item.Desc {
			fmt.Fprintf(buffer, "%s:desc", item.Expr.ExplainInfo())
		} else {
			fmt.Fprintf(buffer, "%s", item.Expr.ExplainInfo())
		}

		if i+1 < len(byItems) {
			buffer.WriteString(", ")
		}
	}
	return buffer
}

func explainNormalizedByItems(buffer *bytes.Buffer, byItems []*util.ByItems) *bytes.Buffer {
	for i, item := range byItems {
		if item.Desc {
			fmt.Fprintf(buffer, "%s:desc", item.Expr.ExplainNormalizedInfo())
		} else {
			fmt.Fprintf(buffer, "%s", item.Expr.ExplainNormalizedInfo())
		}

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
	if p.Source.handleCols != nil {
		fmt.Fprintf(buffer, ", pk col:%s", p.Source.handleCols)
	}
	if len(p.AccessConds) > 0 {
		fmt.Fprintf(buffer, ", cond:%v", p.AccessConds)
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalIndexScan) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.Source.ExplainInfo())
	index := p.Index
	if len(index.Columns) > 0 {
		buffer.WriteString(", index:")
		for i, idxCol := range index.Columns {
			if tblCol := p.Source.tableInfo.Columns[idxCol.Offset]; tblCol.Hidden {
				buffer.WriteString(tblCol.GeneratedExprString)
			} else {
				buffer.WriteString(idxCol.Name.O)
			}
			if i+1 < len(index.Columns) {
				buffer.WriteString(", ")
			}
		}
	}
	if len(p.AccessConds) > 0 {
		fmt.Fprintf(buffer, ", cond:%v", p.AccessConds)
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *TiKVSingleGather) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.Source.ExplainInfo())
	if p.IsIndexGather {
		buffer.WriteString(", index:" + p.Index.Name.String())
	}
	return buffer.String()
}

// MetricTableTimeFormat is the time format for metric table explain and format.
const MetricTableTimeFormat = "2006-01-02 15:04:05.999"

// ExplainInfo implements Plan interface.
func (p *PhysicalMemTable) ExplainInfo() string {
	accessObject, operatorInfo := p.AccessObject(false), p.OperatorInfo(false)
	if len(operatorInfo) == 0 {
		return accessObject
	}
	return accessObject + ", " + operatorInfo
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalMemTable) AccessObject(_ bool) string {
	return "table:" + p.Table.Name.O
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalMemTable) OperatorInfo(_ bool) string {
	if p.Extractor != nil {
		return p.Extractor.explainInfo(p)
	}
	return ""
}
