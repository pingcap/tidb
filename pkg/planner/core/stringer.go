// Copyright 2015 PingCAP, Inc.
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
	"strings"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// ToString explains a Plan, returns description string.
func ToString(p base.Plan) string {
	strs, _ := toString(p, []string{}, []int{})
	return strings.Join(strs, "->")
}

// FDToString explains fd transfer over a Plan, returns description string.
func FDToString(p base.LogicalPlan) string {
	strs, _ := fdToString(p, []string{}, []int{})
	for i, j := 0, len(strs)-1; i < j; i, j = i+1, j-1 {
		strs[i], strs[j] = strs[j], strs[i]
	}
	return strings.Join(strs, " >>> ")
}

func needIncludeChildrenString(plan base.Plan) bool {
	switch x := plan.(type) {
	case *logicalop.LogicalUnionAll, *physicalop.PhysicalUnionAll, *logicalop.LogicalPartitionUnionAll:
		// after https://github.com/pingcap/tidb/pull/25218, the union may contain less than 2 children,
		// but we still wants to include its child plan's information when calling `toString` on union.
		return true
	case base.LogicalPlan:
		return len(x.Children()) > 1
	case base.PhysicalPlan:
		return len(x.Children()) > 1
	default:
		return false
	}
}

func fdToString(in base.LogicalPlan, strs []string, idxs []int) ([]string, []int) {
	switch x := in.(type) {
	case *logicalop.LogicalProjection:
		strs = append(strs, "{"+x.FDs().String()+"}")
		for _, child := range x.Children() {
			strs, idxs = fdToString(child, strs, idxs)
		}
	case *logicalop.LogicalAggregation:
		strs = append(strs, "{"+x.FDs().String()+"}")
		for _, child := range x.Children() {
			strs, idxs = fdToString(child, strs, idxs)
		}
	case *logicalop.DataSource:
		strs = append(strs, "{"+x.FDs().String()+"}")
	case *logicalop.LogicalApply:
		strs = append(strs, "{"+x.FDs().String()+"}")
	case *logicalop.LogicalJoin:
		strs = append(strs, "{"+x.FDs().String()+"}")
	case *logicalop.LogicalUnionAll:
		strs = append(strs, "{"+x.FDs().String()+"}")
	default:
	}
	return strs, idxs
}

func toString(in base.Plan, strs []string, idxs []int) ([]string, []int) {
	var ectx expression.EvalContext
	if in.SCtx() != nil {
		// Not all `base.Plan` has a non-nil `SCtx`. For example, the `SCtx` of `Analyze` plan is nil.
		ectx = in.SCtx().GetExprCtx().GetEvalCtx()
	}

	switch x := in.(type) {
	case base.LogicalPlan:
		if needIncludeChildrenString(in) {
			idxs = append(idxs, len(strs))
		}

		for _, c := range x.Children() {
			strs, idxs = toString(c, strs, idxs)
		}
	case *physicalop.PhysicalExchangeReceiver: // do nothing
	case base.PhysicalPlan:
		if needIncludeChildrenString(in) {
			idxs = append(idxs, len(strs))
		}

		for _, c := range x.Children() {
			strs, idxs = toString(c, strs, idxs)
		}
	}

	var str string
	switch x := in.(type) {
	case *CheckTable:
		str = "CheckTable"
	case *physicalop.PhysicalIndexScan:
		str = fmt.Sprintf("Index(%s.%s)%v", x.Table.Name.L, x.Index.Name.L, x.Ranges)
	case *physicalop.PhysicalTableScan:
		str = fmt.Sprintf("Table(%s)", x.Table.Name.L)
	case *physicalop.PhysicalHashJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		if x.InnerChildIdx == 0 {
			str = "RightHashJoin{" + strings.Join(children, "->") + "}"
		} else {
			str = "LeftHashJoin{" + strings.Join(children, "->") + "}"
		}
		for _, eq := range x.EqualConditions {
			l := eq.GetArgs()[0].StringWithCtx(ectx, perrors.RedactLogDisable)
			r := eq.GetArgs()[1].StringWithCtx(ectx, perrors.RedactLogDisable)
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *physicalop.PhysicalMergeJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		id := "MergeJoin"
		switch x.JoinType {
		case base.SemiJoin:
			id = "MergeSemiJoin"
		case base.AntiSemiJoin:
			id = "MergeAntiSemiJoin"
		case base.LeftOuterSemiJoin:
			id = "MergeLeftOuterSemiJoin"
		case base.AntiLeftOuterSemiJoin:
			id = "MergeAntiLeftOuterSemiJoin"
		case base.LeftOuterJoin:
			id = "MergeLeftOuterJoin"
		case base.RightOuterJoin:
			id = "MergeRightOuterJoin"
		case base.InnerJoin:
			id = "MergeInnerJoin"
		}
		str = id + "{" + strings.Join(children, "->") + "}"
		for i := range x.LeftJoinKeys {
			l := x.LeftJoinKeys[i].StringWithCtx(ectx, perrors.RedactLogDisable)
			r := x.RightJoinKeys[i].StringWithCtx(ectx, perrors.RedactLogDisable)
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *logicalop.LogicalApply, *physicalop.PhysicalApply:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "Apply{" + strings.Join(children, "->") + "}"
	case *logicalop.LogicalMaxOneRow, *physicalop.PhysicalMaxOneRow:
		str = "MaxOneRow"
	case *logicalop.LogicalLimit, *physicalop.PhysicalLimit:
		str = "Limit"
	case *physicalop.PhysicalLock, *logicalop.LogicalLock:
		str = "Lock"
	case *ShowDDL:
		str = "ShowDDL"
	case *logicalop.LogicalShow:
		str = "Show"
		if pl := in.(*logicalop.LogicalShow); pl.Extractor != nil {
			str = str + "(" + pl.Extractor.ExplainInfo() + ")"
		}
	case *physicalop.PhysicalShow:
		str = "Show"
		if pl := in.(*physicalop.PhysicalShow); pl.Extractor != nil {
			str = str + "(" + pl.Extractor.ExplainInfo() + ")"
		}
	case *logicalop.LogicalShowDDLJobs, *physicalop.PhysicalShowDDLJobs:
		str = "ShowDDLJobs"
	case *logicalop.LogicalSort, *physicalop.PhysicalSort:
		str = "Sort"
	case *logicalop.LogicalJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		str = "Join{" + strings.Join(children, "->") + "}"
		idxs = idxs[:last]
		for _, eq := range x.EqualConditions {
			l := eq.GetArgs()[0].StringWithCtx(ectx, perrors.RedactLogDisable)
			r := eq.GetArgs()[1].StringWithCtx(ectx, perrors.RedactLogDisable)
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *logicalop.LogicalUnionAll, *physicalop.PhysicalUnionAll, *logicalop.LogicalPartitionUnionAll:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		name := "UnionAll"
		if x.TP() == plancodec.TypePartitionUnion {
			name = "PartitionUnionAll"
		}
		str = name + "{" + strings.Join(children, "->") + "}"
		idxs = idxs[:last]
	case *logicalop.LogicalSequence:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		name := "Sequence"
		str = name + "{" + strings.Join(children, ",") + "}"
		idxs = idxs[:last]
	case *logicalop.DataSource:
		if x.PartitionDefIdx != nil {
			// TODO: Change this to:
			//str = fmt.Sprintf("Partition(%d)", x.TableInfo.Partition.Definitions[*x.PartitionDefIdx].Name.O)
			str = fmt.Sprintf("Partition(%d)", x.PhysicalTableID)
		} else {
			if x.TableAsName != nil && x.TableAsName.L != "" {
				str = fmt.Sprintf("DataScan(%s)", x.TableAsName)
			} else {
				str = fmt.Sprintf("DataScan(%s)", x.TableInfo.Name)
			}
		}
	case *logicalop.LogicalSelection:
		str = fmt.Sprintf("Sel(%s)", expression.StringifyExpressionsWithCtx(ectx, x.Conditions))
	case *physicalop.PhysicalSelection:
		str = fmt.Sprintf("Sel(%s)", expression.StringifyExpressionsWithCtx(ectx, x.Conditions))
	case *logicalop.LogicalProjection, *physicalop.PhysicalProjection:
		str = "Projection"
	case *logicalop.LogicalTopN:
		str = fmt.Sprintf("TopN(%v,%d,%d)", util.StringifyByItemsWithCtx(ectx, x.ByItems), x.Offset, x.Count)
	case *physicalop.PhysicalTopN:
		str = fmt.Sprintf("TopN(%v,%d,%d)", util.StringifyByItemsWithCtx(ectx, x.ByItems), x.Offset, x.Count)
	case *logicalop.LogicalTableDual, *physicalop.PhysicalTableDual:
		str = "Dual"
	case *physicalop.PhysicalHashAgg:
		str = "HashAgg"
	case *physicalop.PhysicalStreamAgg:
		str = "StreamAgg"
	case *logicalop.LogicalAggregation:
		str = "Aggr("
		for i, aggFunc := range x.AggFuncs {
			str += aggFunc.StringWithCtx(ectx, perrors.RedactLogDisable)
			if i != len(x.AggFuncs)-1 {
				str += ","
			}
		}
		str += ")"
	case *physicalop.PhysicalTableReader:
		str = fmt.Sprintf("TableReader(%s)", ToString(x.TablePlan))
	case *physicalop.PhysicalIndexReader:
		str = fmt.Sprintf("IndexReader(%s)", ToString(x.IndexPlan))
	case *physicalop.PhysicalIndexLookUpReader:
		str = fmt.Sprintf("IndexLookUp(%s, %s)", ToString(x.IndexPlan), ToString(x.TablePlan))
	case *physicalop.PhysicalIndexMergeReader:
		str = "IndexMergeReader(PartialPlans->["
		for i, paritalPlan := range x.PartialPlansRaw {
			if i > 0 {
				str += ", "
			}
			str += ToString(paritalPlan)
		}
		str += "], TablePlan->" + ToString(x.TablePlan) + ")"
	case *physicalop.PhysicalUnionScan:
		str = fmt.Sprintf("UnionScan(%s)", expression.StringifyExpressionsWithCtx(ectx, x.Conditions))
	case *physicalop.PhysicalIndexJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "IndexJoin{" + strings.Join(children, "->") + "}"
		for i := range x.OuterJoinKeys {
			l := x.OuterJoinKeys[i]
			r := x.InnerJoinKeys[i]
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *physicalop.PhysicalIndexMergeJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "IndexMergeJoin{" + strings.Join(children, "->") + "}"
		for i := range x.OuterJoinKeys {
			l := x.OuterJoinKeys[i]
			r := x.InnerJoinKeys[i]
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *physicalop.PhysicalIndexHashJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "IndexHashJoin{" + strings.Join(children, "->") + "}"
		for i := range x.OuterJoinKeys {
			l := x.OuterJoinKeys[i]
			r := x.InnerJoinKeys[i]
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *Analyze:
		str = "Analyze{"
		var children []string
		for _, idx := range x.IdxTasks {
			children = append(children, fmt.Sprintf("Index(%s)", idx.IndexInfo.Name.O))
		}
		for _, col := range x.ColTasks {
			var colNames []string
			if col.HandleCols != nil {
				colNames = append(colNames, col.HandleCols.StringWithCtx(ectx, perrors.RedactLogDisable))
			}
			for _, c := range col.ColsInfo {
				colNames = append(colNames, c.Name.O)
			}
			children = append(children, fmt.Sprintf("Table(%s)", strings.Join(colNames, ", ")))
		}
		str = str + strings.Join(children, ",") + "}"
	case *physicalop.Update:
		str = fmt.Sprintf("%s->Update", ToString(x.SelectPlan))
	case *physicalop.Delete:
		str = fmt.Sprintf("%s->Delete", ToString(x.SelectPlan))
	case *physicalop.Insert:
		str = "Insert"
		if x.SelectPlan != nil {
			str = fmt.Sprintf("%s->Insert", ToString(x.SelectPlan))
		}
	case *logicalop.LogicalWindow:
		buffer := bytes.NewBufferString("")
		physicalop.FormatWindowFuncDescs(ectx, buffer, x.WindowFuncDescs, x.Schema())
		str = fmt.Sprintf("Window(%s)", buffer.String())
	case *physicalop.PhysicalWindow:
		str = fmt.Sprintf("Window(%s)", x.ExplainInfo())
	case *physicalop.PhysicalShuffle:
		str = fmt.Sprintf("Partition(%s)", x.ExplainInfo())
	case *physicalop.PhysicalShuffleReceiverStub:
		str = fmt.Sprintf("PartitionReceiverStub(%s)", x.ExplainInfo())
	case *physicalop.PointGetPlan:
		str = "PointGet("
		if x.IndexInfo != nil {
			str += fmt.Sprintf("Index(%s.%s)%v)", x.TblInfo.Name.L, x.IndexInfo.Name.L, x.IndexValues)
		} else {
			str += fmt.Sprintf("Handle(%s.%s)%v)", x.TblInfo.Name.L, x.TblInfo.GetPkName().L, x.Handle)
		}
	case *physicalop.BatchPointGetPlan:
		str = "BatchPointGet("
		if x.IndexInfo != nil {
			str += fmt.Sprintf("Index(%s.%s)%v)", x.TblInfo.Name.L, x.IndexInfo.Name.L, x.IndexValues)
		} else {
			str += fmt.Sprintf("Handle(%s.%s)%v)", x.TblInfo.Name.L, x.TblInfo.GetPkName().L, x.Handles)
		}
	case *physicalop.PhysicalExchangeReceiver:
		str = "Recv("
		for _, task := range x.Tasks {
			str += fmt.Sprintf("%d, ", task.ID)
		}
		str += ")"
	case *physicalop.PhysicalExchangeSender:
		str = "Send("
		for _, task := range x.TargetTasks {
			str += fmt.Sprintf("%d, ", task.ID)
		}
		for _, tasks := range x.TargetCTEReaderTasks {
			str += "("
			for _, task := range tasks {
				str += fmt.Sprintf("%d, ", task.ID)
			}
			str += ")"
		}
		str += ")"
	case *physicalop.PhysicalCTE:
		str = "CTEReader("
		str += fmt.Sprintf("%v", x.CTE.IDForStorage)
		str += ")"
	default:
		str = fmt.Sprintf("%T", in)
	}
	strs = append(strs, str)
	return strs, idxs
}
