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

const (
	// e.g. keep order:false; stats:pseudo
	entriesSeparator = "; "
	// e.g. eq(test.sgc1.a, test.sgc2.a)
	itemsSeparator = ", "
	// e.g. range:[10, 99) | (100, +inf]
	complexItemsSeparator = " | "
	// e.g. over(partition by test.t.a)
	wordsSeparator = " "
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
	return stringutil.NewSeparatedString(entriesSeparator).
		Append(p.AccessObject(false)).
		Append(p.OperatorInfo(false)).
		String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexScan) ExplainNormalizedInfo() string {
	return stringutil.NewSeparatedString(entriesSeparator).
		Append(p.AccessObject(true)).
		Append(p.OperatorInfo(true)).
		String()
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalIndexScan) AccessObject(normalized bool) string {
	builder := stringutil.NewSeparatedString(entriesSeparator)
	tblName := p.Table.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		tblName = p.TableAsName.O
	}
	builder.AppendF("table:%s", tblName)
	if p.isPartition {
		if normalized {
			builder.Append("partition:?")
		} else if pi := p.Table.GetPartitionInfo(); pi != nil {
			partitionName := pi.GetNameByID(p.physicalTableID)
			builder.AppendF("partition:%s", partitionName)
		}
	}
	if len(p.Index.Columns) > 0 {
		builder.Append("index:" + p.Index.Name.O + "(")
		builder.StartScope(itemsSeparator)
		{
			for _, idxCol := range p.Index.Columns {
				if tblCol := p.Table.Columns[idxCol.Offset]; tblCol.Hidden {
					builder.Append(tblCol.GeneratedExprString)
				} else {
					builder.Append(idxCol.Name.O)
				}
			}
		}
		builder.EndScope()
		builder.WriteString(")")
	}
	return builder.String()
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalIndexScan) OperatorInfo(normalized bool) string {
	builder := stringutil.NewSeparatedString(entriesSeparator)
	if len(p.rangeInfo) > 0 {
		if !normalized {
			builder.AppendF("range: decided by %v", p.rangeInfo)
		}
	} else if p.haveCorCol() {
		if normalized {
			builder.AppendF("range: decided by %s", expression.SortedExplainNormalizedExpressionList(p.AccessCondition))
		} else {
			builder.AppendF("range: decided by %v", p.AccessCondition)
		}
	} else if len(p.Ranges) > 0 {
		if normalized {
			builder.Append("range:[?,?]")
		} else if !p.isFullScan() {
			builder.Append("range:")
			builder.StartScope(complexItemsSeparator)
			{
				for _, idxRange := range p.Ranges {
					builder.Append(idxRange.String())
				}
			}
			builder.EndScope()
		}
	}
	builder.AppendF("keep order:%v", p.KeepOrder)
	if p.Desc {
		builder.Append("desc")
	}
	if p.stats.StatsVersion == statistics.PseudoVersion && !normalized {
		builder.Append("stats:pseudo")
	}
	return builder.String()
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
	return stringutil.NewSeparatedString(entriesSeparator).
		Append(p.AccessObject(false)).
		Append(p.OperatorInfo(false)).
		String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalTableScan) ExplainNormalizedInfo() string {
	return stringutil.NewSeparatedString(entriesSeparator).
		Append(p.AccessObject(true)).
		Append(p.OperatorInfo(true)).
		String()
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalTableScan) AccessObject(normalized bool) string {
	builder := stringutil.NewSeparatedString(entriesSeparator)
	tblName := p.Table.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		tblName = p.TableAsName.O
	}
	builder.AppendF("table:%s", tblName)
	if p.isPartition {
		if normalized {
			builder.Append("partition:?")
		} else if pi := p.Table.GetPartitionInfo(); pi != nil {
			partitionName := pi.GetNameByID(p.physicalTableID)
			builder.AppendF("partition:%s", partitionName)
		}
	}
	return builder.String()
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalTableScan) OperatorInfo(normalized bool) string {
	builder := stringutil.NewSeparatedString(entriesSeparator)
	if len(p.PkCols) > 0 {
		builder.Append("pk cols: (")
		builder.StartScope(itemsSeparator)
		{
			for _, pkCol := range p.PkCols {
				builder.Append(pkCol.ExplainInfo())
			}
		}
		builder.EndScope()
		builder.WriteString(")")
	}

	if len(p.rangeDecidedBy) > 0 {
		builder.AppendF("range: decided by %v", p.rangeDecidedBy)
	} else if p.haveCorCol() {
		if normalized {
			builder.AppendF("range: decided by %s", expression.SortedExplainNormalizedExpressionList(p.AccessCondition))
		} else {
			builder.AppendF("range: decided by %v", p.AccessCondition)
		}
	} else if len(p.Ranges) > 0 {
		if normalized {
			builder.Append("range:[?,?]")
		} else if !p.isFullScan() {
			builder.Append("range:")
			builder.StartScope(complexItemsSeparator)
			{
				for _, idxRange := range p.Ranges {
					builder.Append(idxRange.String())
				}
			}
			builder.EndScope()
		}
	}
	builder.AppendF("keep order:%v", p.KeepOrder)
	if p.Desc {
		builder.Append("desc")
	}
	if p.stats.StatsVersion == statistics.PseudoVersion && !normalized {
		builder.Append("stats:pseudo")
	}
	if p.IsGlobalRead {
		builder.Append("global read")
	}
	return builder.String()
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

	is := infoschema.GetInfoSchema(sctx)
	tmp, ok := is.TableByID(ts.Table.ID)
	if !ok {
		return "partition table not found" + strconv.FormatInt(ts.Table.ID, 10)
	}
	tbl := tmp.(table.PartitionedTable)

	return partitionAccessObject(sctx, tbl, pi, &p.PartitionInfo)
}

func partitionAccessObject(sctx sessionctx.Context, tbl table.PartitionedTable, pi *model.PartitionInfo, partTable *PartitionInfo) string {
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

	builder := stringutil.NewSeparatedString(itemsSeparator)
	builder.WriteString("partition:")
	for _, idx := range idxArr {
		builder.Append(pi.Definitions[idx].Name.O)
	}
	return builder.String()
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
	is := infoschema.GetInfoSchema(sctx)
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
	is := infoschema.GetInfoSchema(sctx)
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

	is := infoschema.GetInfoSchema(sctx)
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
	return explainByItems(p.ByItems)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalLimit) ExplainInfo() string {
	return stringutil.NewSeparatedString(entriesSeparator).
		AppendF("offset:%v", p.Offset).
		AppendF("count:%v", p.Count).
		String()
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

	builder := stringutil.NewSeparatedString(entriesSeparator)
	if len(p.GroupByItems) > 0 {
		builder.AppendF("group by:%s",
			sortedExplainExpressionList(p.GroupByItems))
	}
	for i := 0; i < len(p.AggFuncs); i++ {
		var colName string
		if normalized {
			colName = p.schema.Columns[i].ExplainNormalizedInfo()
		} else {
			colName = p.schema.Columns[i].ExplainInfo()
		}
		builder.AppendF("funcs:%v->%v", aggregation.ExplainAggFunc(p.AggFuncs[i], normalized), colName)
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

	builder := stringutil.NewSeparatedString(entriesSeparator).Append(p.JoinType.String())
	if normalized {
		builder.AppendF("inner:%s", p.Children()[p.InnerChildIdx].TP())
	} else {
		builder.AppendF("inner:%s", p.Children()[p.InnerChildIdx].ExplainID())
	}
	if len(p.OuterJoinKeys) > 0 {
		builder.AppendF("outer key:%s",
			expression.ExplainColumnList(p.OuterJoinKeys))
	}
	if len(p.InnerJoinKeys) > 0 {
		builder.AppendF("inner key:%s",
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
		builder.AppendF("equal cond:%s",
			sortedExplainExpressionList(exprs))
	}
	if len(p.LeftConditions) > 0 {
		builder.AppendF("left cond:%s",
			sortedExplainExpressionList(p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		builder.AppendF("right cond:%s",
			sortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		builder.AppendF("other cond:%s",
			sortedExplainExpressionList(p.OtherConditions))
	}
	return builder.String()
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

	builder := stringutil.NewSeparatedString(entriesSeparator)

	builder.StartNext()
	if len(p.EqualConditions) == 0 {
		builder.WriteString("CARTESIAN ")
	}

	builder.WriteString(p.JoinType.String())

	if len(p.EqualConditions) > 0 {
		if normalized {
			builder.AppendF("equal:%s", expression.SortedExplainNormalizedScalarFuncList(p.EqualConditions))
		} else {
			builder.AppendF("equal:%v", p.EqualConditions)
		}
	}
	if len(p.LeftConditions) > 0 {
		if normalized {
			builder.AppendF("left cond:%s", expression.SortedExplainNormalizedExpressionList(p.LeftConditions))
		} else {
			builder.AppendF("left cond:%s", p.LeftConditions)
		}
	}
	if len(p.RightConditions) > 0 {
		builder.AppendF("right cond:%s",
			sortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		builder.AppendF("other cond:%s",
			sortedExplainExpressionList(p.OtherConditions))
	}
	return builder.String()
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

	builder := stringutil.NewSeparatedString(entriesSeparator).Append(p.JoinType.String())
	if len(p.LeftJoinKeys) > 0 {
		builder.AppendF("left key:%s",
			expression.ExplainColumnList(p.LeftJoinKeys))
	}
	if len(p.RightJoinKeys) > 0 {
		builder.AppendF("right key:%s",
			expression.ExplainColumnList(p.RightJoinKeys))
	}
	if len(p.LeftConditions) > 0 {
		if normalized {
			builder.AppendF("left cond:%s", expression.SortedExplainNormalizedExpressionList(p.LeftConditions))
		} else {
			builder.AppendF("left cond:%s", p.LeftConditions)
		}
	}
	if len(p.RightConditions) > 0 {
		builder.AppendF("right cond:%s",
			sortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		builder.AppendF("other cond:%s",
			sortedExplainExpressionList(p.OtherConditions))
	}
	return builder.String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalMergeJoin) ExplainNormalizedInfo() string {
	return p.explainInfo(true)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalTopN) ExplainInfo() string {
	return stringutil.NewSeparatedString(entriesSeparator).
		Append(explainByItems(p.ByItems)).
		AppendF("offset:%v", p.Offset).
		AppendF("count:%v", p.Count).
		String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalTopN) ExplainNormalizedInfo() string {
	return explainNormalizedByItems(p.ByItems)
}

func (p *PhysicalWindow) formatFrameBound(bound *FrameBound) string {
	if bound.Type == ast.CurrentRow {
		return "current row"
	}

	// simple string concatenation by plus is fastest
	// ref: https://dev.to/pmalhaire/concatenate-strings-in-golang-a-quick-benchmark-4ahh
	var result string
	if bound.UnBounded {
		result = "unbounded"
	} else if len(bound.CalcFuncs) > 0 {
		sf := bound.CalcFuncs[0].(*expression.ScalarFunction)
		switch sf.FuncName.L {
		case ast.DateAdd, ast.DateSub:
			// For `interval '2:30' minute_second`.
			result = fmt.Sprintf("interval %s %s", sf.GetArgs()[1].ExplainInfo(), sf.GetArgs()[2].ExplainInfo())
		case ast.Plus, ast.Minus:
			// For `1 preceding` of range frame.
			result = fmt.Sprintf("%s", sf.GetArgs()[1].ExplainInfo())
		}
	} else {
		result = fmt.Sprintf("%d", bound.Num)
	}
	if bound.Type == ast.Preceding {
		result += " preceding"
	} else {
		result += " following"
	}
	return result
}

// ExplainInfo implements Plan interface.
func (p *PhysicalWindow) ExplainInfo() string {
	builder := stringutil.NewSeparatedString(wordsSeparator)
	builder.WriteString(formatWindowFuncDescs(p.WindowFuncDescs, p.schema))
	builder.WriteString(" over(")
	if len(p.PartitionBy) > 0 {
		builder.Append("partition by ")
		builder.StartScope(itemsSeparator)
		{
			for _, item := range p.PartitionBy {
				builder.Append(item.Col.ExplainInfo())
			}
		}
		builder.EndScope()
	}
	if len(p.OrderBy) > 0 {
		builder.Append("order by ")
		builder.StartScope(itemsSeparator)
		{
			for _, item := range p.OrderBy {
				if item.Desc {
					builder.AppendF("%s desc", item.Col.ExplainInfo())
				} else {
					builder.Append(item.Col.ExplainInfo())
				}
			}
		}
		builder.EndScope()
	}
	if p.Frame != nil {
		builder.StartNext()
		if p.Frame.Type == ast.Rows {
			builder.WriteString("rows")
		} else {
			builder.WriteString("range")
		}
		builder.WriteString(" between ")
		builder.WriteString(p.formatFrameBound(p.Frame.Start))
		builder.WriteString(" and ")
		builder.WriteString(p.formatFrameBound(p.Frame.End))
	}
	builder.WriteString(")")
	return builder.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalShuffle) ExplainInfo() string {
	explainIds := make([]fmt.Stringer, len(p.DataSources))
	for i := range p.DataSources {
		explainIds[i] = p.DataSources[i].ExplainID()
	}
	return fmt.Sprintf("execution info: concurrency:%v, data sources:%v", p.Concurrency, explainIds)
}

func formatWindowFuncDescs(descs []*aggregation.WindowFuncDesc, schema *expression.Schema) string {
	winFuncStartIdx := len(schema.Columns) - len(descs)
	builder := stringutil.NewSeparatedString(complexItemsSeparator)
	for i, desc := range descs {
		builder.AppendF("%v->%v", desc, schema.Columns[winFuncStartIdx+i])
	}
	return builder.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalJoin) ExplainInfo() string {
	builder := stringutil.NewSeparatedString(entriesSeparator).Append(p.JoinType.String())
	if len(p.EqualConditions) > 0 {
		builder.AppendF("equal:%v", p.EqualConditions)
	}
	if len(p.LeftConditions) > 0 {
		builder.AppendF("left cond:%s",
			expression.SortedExplainExpressionList(p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		builder.AppendF("right cond:%s",
			expression.SortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		builder.AppendF("other cond:%s",
			expression.SortedExplainExpressionList(p.OtherConditions))
	}
	return builder.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalAggregation) ExplainInfo() string {
	builder := stringutil.NewSeparatedString(entriesSeparator)
	if len(p.GroupByItems) > 0 {
		builder.AppendF("group by:%s",
			expression.SortedExplainExpressionList(p.GroupByItems))
	}
	if len(p.AggFuncs) > 0 {
		builder.Append("funcs:")
		builder.StartScope(itemsSeparator)
		{
			for _, agg := range p.AggFuncs {
				builder.Append(aggregation.ExplainAggFunc(agg, false))
			}
		}
		builder.EndScope()
	}
	return builder.String()
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
	builder := stringutil.NewSeparatedString(entriesSeparator)
	tblName := p.tableInfo.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		tblName = p.TableAsName.O
	}
	builder.AppendF("table:%s", tblName)
	if p.isPartition {
		if pi := p.tableInfo.GetPartitionInfo(); pi != nil {
			partitionName := pi.GetNameByID(p.physicalTableID)
			builder.AppendF("partition:%s", partitionName)
		}
	}
	return builder.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalExchangeSender) ExplainInfo() string {
	builder := stringutil.NewSeparatedString(entriesSeparator).Append("ExchangeType: ")
	switch p.ExchangeType {
	case tipb.ExchangeType_PassThrough:
		builder.WriteString("PassThrough")
	case tipb.ExchangeType_Broadcast:
		builder.WriteString("Broadcast")
	case tipb.ExchangeType_Hash:
		builder.WriteString("HashPartition")
		builder.AppendF("Hash Cols: %s", expression.ExplainColumnList(p.HashCols))
	}
	if len(p.Tasks) > 0 {
		builder.Append("tasks: [")
		builder.StartScope(itemsSeparator)
		{
			for _, task := range p.Tasks {
				builder.AppendF("%v", task.ID)
			}
		}
		builder.EndScope()
		builder.WriteString("]")
	}
	return builder.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalUnionScan) ExplainInfo() string {
	return stringutil.NewSeparatedString(entriesSeparator).
		AppendF("conds:%s", expression.SortedExplainExpressionList(p.conditions)).
		AppendF("handle:%s", p.handleCols).
		String()
}

func explainByItems(byItems []*util.ByItems) string {
	builder := stringutil.NewSeparatedString(itemsSeparator)
	for _, item := range byItems {
		if item.Desc {
			builder.AppendF("%s:desc", item.Expr.ExplainInfo())
		} else {
			builder.Append(item.Expr.ExplainInfo())
		}
	}
	return builder.String()
}

func explainNormalizedByItems(byItems []*util.ByItems) string {
	builder := stringutil.NewSeparatedString(itemsSeparator)
	for _, item := range byItems {
		if item.Desc {
			builder.AppendF("%s:desc", item.Expr.ExplainNormalizedInfo())
		} else {
			builder.Append(item.Expr.ExplainNormalizedInfo())
		}
	}
	return builder.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalSort) ExplainInfo() string {
	return explainByItems(p.ByItems)
}

// ExplainInfo implements Plan interface.
func (p *LogicalTopN) ExplainInfo() string {
	return stringutil.NewSeparatedString(entriesSeparator).
		Append(explainByItems(p.ByItems)).
		AppendF("offset:%v", p.Offset).
		AppendF("count:%v", p.Count).
		String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalLimit) ExplainInfo() string {
	return stringutil.NewSeparatedString(entriesSeparator).
		AppendF("offset:%v", p.Offset).
		AppendF("count:%v", p.Count).
		String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalTableScan) ExplainInfo() string {
	builder := stringutil.NewSeparatedString(entriesSeparator).Append(p.Source.ExplainInfo())
	if p.Source.handleCols != nil {
		builder.AppendF("pk col:%s", p.Source.handleCols)
	}
	if len(p.AccessConds) > 0 {
		builder.AppendF("cond:%v", p.AccessConds)
	}
	return builder.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalIndexScan) ExplainInfo() string {
	builder := stringutil.NewSeparatedString(entriesSeparator).Append(p.Source.ExplainInfo())
	index := p.Index
	if len(index.Columns) > 0 {
		builder.Append("index:")
		builder.StartScope(itemsSeparator)
		{
			for _, idxCol := range index.Columns {
				if tblCol := p.Source.tableInfo.Columns[idxCol.Offset]; tblCol.Hidden {
					builder.Append(tblCol.GeneratedExprString)
				} else {
					builder.Append(idxCol.Name.O)
				}
			}
		}
		builder.EndScope()
	}
	if len(p.AccessConds) > 0 {
		builder.AppendF("cond:%v", p.AccessConds)
	}
	return builder.String()
}

// ExplainInfo implements Plan interface.
func (p *TiKVSingleGather) ExplainInfo() string {
	builder := stringutil.NewSeparatedString(entriesSeparator).Append(p.Source.ExplainInfo())
	if p.IsIndexGather {
		builder.AppendF("index:%s", p.Index.Name.String())
	}
	return builder.String()
}

// MetricTableTimeFormat is the time format for metric table explain and format.
const MetricTableTimeFormat = "2006-01-02 15:04:05.999"

// ExplainInfo implements Plan interface.
func (p *PhysicalMemTable) ExplainInfo() string {
	accessObject, operatorInfo := p.AccessObject(false), p.OperatorInfo(false)
	if len(operatorInfo) == 0 {
		return accessObject
	}
	return stringutil.NewSeparatedString(entriesSeparator).
		Append(accessObject).
		Append(operatorInfo).
		String()
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
