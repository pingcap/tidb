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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// ExplainInfo implements Plan interface.
func (p *PhysicalLock) ExplainInfo() string {
	var str strings.Builder
	str.WriteString(p.Lock.LockType.String())
	str.WriteString(" ")
	str.WriteString(strconv.FormatUint(p.Lock.WaitSec, 10))
	return str.String()
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
	return p.AccessObject().String() + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexScan) ExplainNormalizedInfo() string {
	return p.AccessObject().NormalizedString() + ", " + p.OperatorInfo(true)
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalIndexScan) OperatorInfo(normalized bool) string {
	var buffer strings.Builder
	if len(p.rangeInfo) > 0 {
		if !normalized {
			buffer.WriteString("range: decided by ")
			buffer.WriteString(p.rangeInfo)
			buffer.WriteString(", ")
		}
	} else if p.haveCorCol() {
		if normalized {
			buffer.WriteString("range: decided by ")
			buffer.Write(expression.SortedExplainNormalizedExpressionList(p.AccessCondition))
			buffer.WriteString(", ")
		} else {
			buffer.WriteString("range: decided by [")
			for i, expr := range p.AccessCondition {
				if i != 0 {
					buffer.WriteString(" ")
				}
				buffer.WriteString(expr.String())
			}
			buffer.WriteString("], ")
		}
	} else if len(p.Ranges) > 0 {
		if normalized {
			buffer.WriteString("range:[?,?], ")
		} else if !p.isFullScan() {
			buffer.WriteString("range:")
			for _, idxRange := range p.Ranges {
				buffer.WriteString(idxRange.String())
				buffer.WriteString(", ")
			}
		}
	}
	buffer.WriteString("keep order:")
	buffer.WriteString(strconv.FormatBool(p.KeepOrder))
	if p.Desc {
		buffer.WriteString(", desc")
	}
	if p.stats.StatsVersion == statistics.PseudoVersion && !normalized {
		buffer.WriteString(", stats:pseudo")
	}
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
		if !ran.IsFullRange(false) {
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
	return p.AccessObject().String() + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalTableScan) ExplainNormalizedInfo() string {
	return p.AccessObject().NormalizedString() + ", " + p.OperatorInfo(true)
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalTableScan) OperatorInfo(normalized bool) string {
	var buffer strings.Builder
	if len(p.rangeInfo) > 0 {
		// TODO: deal with normalized case
		buffer.WriteString("range: decided by ")
		buffer.WriteString(p.rangeInfo)
		buffer.WriteString(", ")
	} else if p.haveCorCol() {
		if normalized {
			buffer.WriteString("range: decided by ")
			buffer.Write(expression.SortedExplainNormalizedExpressionList(p.AccessCondition))
			buffer.WriteString(", ")
		} else {
			buffer.WriteString("range: decided by [")
			for i, AccessCondition := range p.AccessCondition {
				if i != 0 {
					buffer.WriteString(" ")
				}
				buffer.WriteString(AccessCondition.String())
			}
			buffer.WriteString("], ")
		}
	} else if len(p.Ranges) > 0 {
		if normalized {
			buffer.WriteString("range:[?,?], ")
		} else if !p.isFullScan() {
			buffer.WriteString("range:")
			for _, idxRange := range p.Ranges {
				buffer.WriteString(idxRange.String())
				buffer.WriteString(", ")
			}
		}
	}
	buffer.WriteString("keep order:")
	buffer.WriteString(strconv.FormatBool(p.KeepOrder))
	if p.Desc {
		buffer.WriteString(", desc")
	}
	if p.stats.StatsVersion == statistics.PseudoVersion && !normalized {
		buffer.WriteString(", stats:pseudo")
	}
	if p.StoreType == kv.TiFlash && p.Table.GetPartitionInfo() != nil && p.IsMPPOrBatchCop && p.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		buffer.WriteString(", PartitionTableScan:true")
	}
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
	if len(p.rangeInfo) > 0 || p.haveCorCol() {
		return false
	}
	var unsignedIntHandle bool
	if p.Table.PKIsHandle {
		if pkColInfo := p.Table.GetPkColInfo(); pkColInfo != nil {
			unsignedIntHandle = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
		}
	}
	for _, ran := range p.Ranges {
		if !ran.IsFullRange(unsignedIntHandle) {
			return false
		}
	}
	return true
}

// ExplainInfo implements Plan interface.
func (p *PhysicalTableReader) ExplainInfo() string {
	tablePlanInfo := "data:" + p.tablePlan.ExplainID().String()

	if p.ReadReqType == MPP {
		return fmt.Sprintf("MppVersion: %d, %s", p.ctx.GetSessionVars().ChooseMppVersion(), tablePlanInfo)
	}

	return tablePlanInfo
}

// ExplainNormalizedInfo implements Plan interface.
func (*PhysicalTableReader) ExplainNormalizedInfo() string {
	return ""
}

// OperatorInfo return other operator information to be explained.
func (p *PhysicalTableReader) OperatorInfo(_ bool) string {
	return "data:" + p.tablePlan.ExplainID().String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexReader) ExplainInfo() string {
	return "index:" + p.indexPlan.ExplainID().String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexReader) ExplainNormalizedInfo() string {
	return "index:" + p.indexPlan.TP()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexLookUpReader) ExplainInfo() string {
	var str strings.Builder
	// The children can be inferred by the relation symbol.
	if p.PushedLimit != nil {
		str.WriteString("limit embedded(offset:")
		str.WriteString(strconv.FormatUint(p.PushedLimit.Offset, 10))
		str.WriteString(", count:")
		str.WriteString(strconv.FormatUint(p.PushedLimit.Count, 10))
		str.WriteString(")")
	}
	return str.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexMergeReader) ExplainInfo() string {
	if p.IsIntersectionType {
		return "type: intersection"
	}
	return "type: union"
}

// ExplainInfo implements Plan interface.
func (p *PhysicalUnionScan) ExplainInfo() string {
	return string(expression.SortedExplainExpressionList(p.Conditions))
}

// ExplainInfo implements Plan interface.
func (p *PhysicalSelection) ExplainInfo() string {
	exprStr := string(expression.SortedExplainExpressionList(p.Conditions))
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		exprStr += fmt.Sprintf(", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount)
	}
	return exprStr
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalSelection) ExplainNormalizedInfo() string {
	return string(expression.SortedExplainNormalizedExpressionList(p.Conditions))
}

// ExplainInfo implements Plan interface.
func (p *PhysicalProjection) ExplainInfo() string {
	exprStr := expression.ExplainExpressionList(p.Exprs, p.schema)
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		exprStr += fmt.Sprintf(", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount)
	}
	return exprStr
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalProjection) ExplainNormalizedInfo() string {
	return string(expression.SortedExplainNormalizedExpressionList(p.Exprs))
}

// ExplainInfo implements Plan interface.
func (p *PhysicalTableDual) ExplainInfo() string {
	var str strings.Builder
	str.WriteString("rows:")
	str.WriteString(strconv.Itoa(p.RowCount))
	return str.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalSort) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	buffer = explainByItems(buffer, p.ByItems)
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		buffer.WriteString(fmt.Sprintf(", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount))
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalLimit) ExplainInfo() string {
	var str strings.Builder
	str.WriteString("offset:")
	str.WriteString(strconv.FormatUint(p.Offset, 10))
	str.WriteString(", count:")
	str.WriteString(strconv.FormatUint(p.Count, 10))
	return str.String()
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
		builder.WriteString("group by:")
		builder.Write(sortedExplainExpressionList(p.GroupByItems))
		builder.WriteString(", ")
	}
	for i := 0; i < len(p.AggFuncs); i++ {
		builder.WriteString("funcs:")
		var colName string
		if normalized {
			colName = p.schema.Columns[i].ExplainNormalizedInfo()
		} else {
			colName = p.schema.Columns[i].ExplainInfo()
		}
		builder.WriteString(aggregation.ExplainAggFunc(p.AggFuncs[i], normalized))
		builder.WriteString("->")
		builder.WriteString(colName)
		if i+1 < len(p.AggFuncs) {
			builder.WriteString(", ")
		}
	}
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		builder.WriteString(fmt.Sprintf(", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount))
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
	buffer.WriteString(", inner:")
	if normalized {
		buffer.WriteString(p.Children()[p.InnerChildIdx].TP())
	} else {
		buffer.WriteString(p.Children()[p.InnerChildIdx].ExplainID().String())
	}
	if len(p.OuterJoinKeys) > 0 {
		buffer.WriteString(", outer key:")
		buffer.Write(expression.ExplainColumnList(p.OuterJoinKeys))
	}
	if len(p.InnerJoinKeys) > 0 {
		buffer.WriteString(", inner key:")
		buffer.Write(expression.ExplainColumnList(p.InnerJoinKeys))
	}

	if len(p.OuterHashKeys) > 0 && !isIndexMergeJoin {
		exprs := make([]expression.Expression, 0, len(p.OuterHashKeys))
		for i := range p.OuterHashKeys {
			expr, err := expression.NewFunctionBase(MockContext(), ast.EQ, types.NewFieldType(mysql.TypeLonglong), p.OuterHashKeys[i], p.InnerHashKeys[i])
			if err != nil {
				logutil.BgLogger().Warn("fail to NewFunctionBase", zap.Error(err))
			}
			exprs = append(exprs, expr)
		}
		buffer.WriteString(", equal cond:")
		buffer.Write(sortedExplainExpressionList(exprs))
	}
	if len(p.LeftConditions) > 0 {
		buffer.WriteString(", left cond:")
		buffer.Write(sortedExplainExpressionList(p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		buffer.WriteString(", right cond:")
		buffer.Write(sortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		buffer.WriteString(", other cond:")
		buffer.Write(sortedExplainExpressionList(p.OtherConditions))
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

	buffer := new(strings.Builder)

	if len(p.EqualConditions) == 0 {
		if len(p.NAEqualConditions) == 0 {
			buffer.WriteString("CARTESIAN ")
		} else {
			buffer.WriteString("Null-aware ")
		}
	}

	buffer.WriteString(p.JoinType.String())

	if len(p.EqualConditions) > 0 {
		if normalized {
			buffer.WriteString(", equal:")
			buffer.Write(expression.SortedExplainNormalizedScalarFuncList(p.EqualConditions))
		} else {
			buffer.WriteString(", equal:[")
			for i, EqualConditions := range p.EqualConditions {
				if i != 0 {
					buffer.WriteString(" ")
				}
				buffer.WriteString(EqualConditions.String())
			}
			buffer.WriteString("]")
		}
	}
	if len(p.NAEqualConditions) > 0 {
		if normalized {
			buffer.WriteString(", equal:")
			buffer.Write(expression.SortedExplainNormalizedScalarFuncList(p.NAEqualConditions))
		} else {
			buffer.WriteString(", equal:[")
			for i, NAEqualCondition := range p.NAEqualConditions {
				if i != 0 {
					buffer.WriteString(" ")
				}
				buffer.WriteString(NAEqualCondition.String())
			}
			buffer.WriteString("]")
		}
	}
	if len(p.LeftConditions) > 0 {
		if normalized {
			buffer.WriteString(", left cond:")
			buffer.Write(expression.SortedExplainNormalizedExpressionList(p.LeftConditions))
		} else {
			buffer.WriteString(", left cond:[")
			for i, LeftConditions := range p.LeftConditions {
				if i != 0 {
					buffer.WriteString(" ")
				}
				buffer.WriteString(LeftConditions.String())
			}
			buffer.WriteString("]")
		}
	}
	if len(p.RightConditions) > 0 {
		buffer.WriteString(", right cond:")
		buffer.Write(sortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		buffer.WriteString(", other cond:")
		buffer.Write(sortedExplainExpressionList(p.OtherConditions))
	}
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		buffer.WriteString(fmt.Sprintf(", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount))
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

func (*PhysicalWindow) formatFrameBound(buffer *bytes.Buffer, bound *FrameBound) {
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
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		buffer.WriteString(fmt.Sprintf(", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount))
	}
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
	var str strings.Builder
	str.WriteString("rowcount:")
	str.WriteString(strconv.Itoa(p.RowCount))
	return str.String()
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
	}
	if p.CompressionMode != kv.ExchangeCompressionModeNONE {
		fmt.Fprintf(buffer, ", Compression: %s", p.CompressionMode.Name())
	}
	if p.ExchangeType == tipb.ExchangeType_Hash {
		fmt.Fprintf(buffer, ", Hash Cols: %s", property.ExplainColumnList(p.HashCols))
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
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		buffer.WriteString(fmt.Sprintf(", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount))
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalExchangeReceiver) ExplainInfo() (res string) {
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		res = fmt.Sprintf("stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount)
	}
	return res
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
func (lt *LogicalTopN) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	buffer = explainByItems(buffer, lt.ByItems)
	fmt.Fprintf(buffer, ", offset:%v, count:%v", lt.Offset, lt.Count)
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalLimit) ExplainInfo() string {
	var str strings.Builder
	str.WriteString("offset:")
	str.WriteString(strconv.FormatUint(p.Offset, 10))
	str.WriteString(", count:")
	str.WriteString(strconv.FormatUint(p.Count, 10))
	return str.String()
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
	accessObject, operatorInfo := p.AccessObject().String(), p.OperatorInfo(false)
	if len(operatorInfo) == 0 {
		return accessObject
	}
	return accessObject + ", " + operatorInfo
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalMemTable) OperatorInfo(_ bool) string {
	if p.Extractor != nil {
		return p.Extractor.explainInfo(p)
	}
	return ""
}
