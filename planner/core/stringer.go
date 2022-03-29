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

	"github.com/pingcap/tidb/util/plancodec"
)

// ToString explains a Plan, returns description string.
func ToString(p Plan) string {
	strs, _ := toString(p, []string{}, []int{})
	return strings.Join(strs, "->")
}

// FDToString explains fd transfer over a Plan, returns description string.
func FDToString(p LogicalPlan) string {
	strs, _ := fdToString(p, []string{}, []int{})
	for i, j := 0, len(strs)-1; i < j; i, j = i+1, j-1 {
		strs[i], strs[j] = strs[j], strs[i]
	}
	return strings.Join(strs, " >>> ")
}

func needIncludeChildrenString(plan Plan) bool {
	switch x := plan.(type) {
	case *LogicalUnionAll, *PhysicalUnionAll, *LogicalPartitionUnionAll:
		// after https://github.com/pingcap/tidb/pull/25218, the union may contain less than 2 children,
		// but we still wants to include its child plan's information when calling `toString` on union.
		return true
	case LogicalPlan:
		return len(x.Children()) > 1
	case PhysicalPlan:
		return len(x.Children()) > 1
	default:
		return false
	}
}

func fdToString(in LogicalPlan, strs []string, idxs []int) ([]string, []int) {
	switch x := in.(type) {
	case *LogicalProjection:
		strs = append(strs, "{"+x.fdSet.String()+"}")
		for _, child := range x.Children() {
			strs, idxs = fdToString(child, strs, idxs)
		}
	case *LogicalAggregation:
		strs = append(strs, "{"+x.fdSet.String()+"}")
		for _, child := range x.Children() {
			strs, idxs = fdToString(child, strs, idxs)
		}
	case *DataSource:
		strs = append(strs, "{"+x.fdSet.String()+"}")
	case *LogicalApply:
		strs = append(strs, "{"+x.fdSet.String()+"}")
	case *LogicalJoin:
		strs = append(strs, "{"+x.fdSet.String()+"}")
	default:
	}
	return strs, idxs
}

func toString(in Plan, strs []string, idxs []int) ([]string, []int) {
	switch x := in.(type) {
	case LogicalPlan:
		if needIncludeChildrenString(in) {
			idxs = append(idxs, len(strs))
		}

		for _, c := range x.Children() {
			strs, idxs = toString(c, strs, idxs)
		}
	case *PhysicalExchangeReceiver: // do nothing
	case PhysicalPlan:
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
	case *PhysicalIndexScan:
		str = fmt.Sprintf("Index(%s.%s)%v", x.Table.Name.L, x.Index.Name.L, x.Ranges)
	case *PhysicalTableScan:
		str = fmt.Sprintf("Table(%s)", x.Table.Name.L)
	case *PhysicalHashJoin:
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
			l := eq.GetArgs()[0].String()
			r := eq.GetArgs()[1].String()
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *PhysicalMergeJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		id := "MergeJoin"
		switch x.JoinType {
		case SemiJoin:
			id = "MergeSemiJoin"
		case AntiSemiJoin:
			id = "MergeAntiSemiJoin"
		case LeftOuterSemiJoin:
			id = "MergeLeftOuterSemiJoin"
		case AntiLeftOuterSemiJoin:
			id = "MergeAntiLeftOuterSemiJoin"
		case LeftOuterJoin:
			id = "MergeLeftOuterJoin"
		case RightOuterJoin:
			id = "MergeRightOuterJoin"
		case InnerJoin:
			id = "MergeInnerJoin"
		}
		str = id + "{" + strings.Join(children, "->") + "}"
		for i := range x.LeftJoinKeys {
			l := x.LeftJoinKeys[i].String()
			r := x.RightJoinKeys[i].String()
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *LogicalApply, *PhysicalApply:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "Apply{" + strings.Join(children, "->") + "}"
	case *LogicalMaxOneRow, *PhysicalMaxOneRow:
		str = "MaxOneRow"
	case *LogicalLimit, *PhysicalLimit:
		str = "Limit"
	case *PhysicalLock, *LogicalLock:
		str = "Lock"
	case *ShowDDL:
		str = "ShowDDL"
	case *LogicalShow:
		str = "Show"
		if pl := in.(*LogicalShow); pl.Extractor != nil {
			str = str + "(" + pl.Extractor.explainInfo() + ")"
		}
	case *PhysicalShow:
		str = "Show"
		if pl := in.(*PhysicalShow); pl.Extractor != nil {
			str = str + "(" + pl.Extractor.explainInfo() + ")"
		}
	case *LogicalShowDDLJobs, *PhysicalShowDDLJobs:
		str = "ShowDDLJobs"
	case *LogicalSort, *PhysicalSort:
		str = "Sort"
	case *LogicalJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		str = "Join{" + strings.Join(children, "->") + "}"
		idxs = idxs[:last]
		for _, eq := range x.EqualConditions {
			l := eq.GetArgs()[0].String()
			r := eq.GetArgs()[1].String()
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *LogicalUnionAll, *PhysicalUnionAll, *LogicalPartitionUnionAll:
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
	case *DataSource:
		if x.isPartition {
			str = fmt.Sprintf("Partition(%d)", x.physicalTableID)
		} else {
			if x.TableAsName != nil && x.TableAsName.L != "" {
				str = fmt.Sprintf("DataScan(%s)", x.TableAsName)
			} else {
				str = fmt.Sprintf("DataScan(%s)", x.tableInfo.Name)
			}
		}
	case *LogicalSelection:
		str = fmt.Sprintf("Sel(%s)", x.Conditions)
	case *PhysicalSelection:
		str = fmt.Sprintf("Sel(%s)", x.Conditions)
	case *LogicalProjection, *PhysicalProjection:
		str = "Projection"
	case *LogicalTopN:
		str = fmt.Sprintf("TopN(%v,%d,%d)", x.ByItems, x.Offset, x.Count)
	case *PhysicalTopN:
		str = fmt.Sprintf("TopN(%v,%d,%d)", x.ByItems, x.Offset, x.Count)
	case *LogicalTableDual, *PhysicalTableDual:
		str = "Dual"
	case *PhysicalHashAgg:
		str = "HashAgg"
	case *PhysicalStreamAgg:
		str = "StreamAgg"
	case *LogicalAggregation:
		str = "Aggr("
		for i, aggFunc := range x.AggFuncs {
			str += aggFunc.String()
			if i != len(x.AggFuncs)-1 {
				str += ","
			}
		}
		str += ")"
	case *PhysicalTableReader:
		str = fmt.Sprintf("TableReader(%s)", ToString(x.tablePlan))
	case *PhysicalIndexReader:
		str = fmt.Sprintf("IndexReader(%s)", ToString(x.indexPlan))
	case *PhysicalIndexLookUpReader:
		str = fmt.Sprintf("IndexLookUp(%s, %s)", ToString(x.indexPlan), ToString(x.tablePlan))
	case *PhysicalIndexMergeReader:
		str = "IndexMergeReader(PartialPlans->["
		for i, paritalPlan := range x.partialPlans {
			if i > 0 {
				str += ", "
			}
			str += ToString(paritalPlan)
		}
		str += "], TablePlan->" + ToString(x.tablePlan) + ")"
	case *PhysicalUnionScan:
		str = fmt.Sprintf("UnionScan(%s)", x.Conditions)
	case *PhysicalIndexJoin:
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
	case *PhysicalIndexMergeJoin:
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
	case *PhysicalIndexHashJoin:
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
				colNames = append(colNames, col.HandleCols.String())
			}
			for _, c := range col.ColsInfo {
				colNames = append(colNames, c.Name.O)
			}
			children = append(children, fmt.Sprintf("Table(%s)", strings.Join(colNames, ", ")))
		}
		str = str + strings.Join(children, ",") + "}"
	case *Update:
		str = fmt.Sprintf("%s->Update", ToString(x.SelectPlan))
	case *Delete:
		str = fmt.Sprintf("%s->Delete", ToString(x.SelectPlan))
	case *Insert:
		str = "Insert"
		if x.SelectPlan != nil {
			str = fmt.Sprintf("%s->Insert", ToString(x.SelectPlan))
		}
	case *LogicalWindow:
		buffer := bytes.NewBufferString("")
		formatWindowFuncDescs(buffer, x.WindowFuncDescs, x.schema)
		str = fmt.Sprintf("Window(%s)", buffer.String())
	case *PhysicalWindow:
		str = fmt.Sprintf("Window(%s)", x.ExplainInfo())
	case *PhysicalShuffle:
		str = fmt.Sprintf("Partition(%s)", x.ExplainInfo())
	case *PhysicalShuffleReceiverStub:
		str = fmt.Sprintf("PartitionReceiverStub(%s)", x.ExplainInfo())
	case *PointGetPlan:
		str = "PointGet("
		if x.IndexInfo != nil {
			str += fmt.Sprintf("Index(%s.%s)%v)", x.TblInfo.Name.L, x.IndexInfo.Name.L, x.IndexValues)
		} else {
			str += fmt.Sprintf("Handle(%s.%s)%v)", x.TblInfo.Name.L, x.TblInfo.GetPkName().L, x.Handle)
		}
	case *BatchPointGetPlan:
		str = "BatchPointGet("
		if x.IndexInfo != nil {
			str += fmt.Sprintf("Index(%s.%s)%v)", x.TblInfo.Name.L, x.IndexInfo.Name.L, x.IndexValues)
		} else {
			str += fmt.Sprintf("Handle(%s.%s)%v)", x.TblInfo.Name.L, x.TblInfo.GetPkName().L, x.Handles)
		}
	case *PhysicalExchangeReceiver:
		str = "Recv("
		for _, task := range x.Tasks {
			str += fmt.Sprintf("%d, ", task.ID)
		}
		str += ")"
	case *PhysicalExchangeSender:
		str = "Send("
		for _, task := range x.TargetTasks {
			str += fmt.Sprintf("%d, ", task.ID)
		}
		str += ")"
	default:
		str = fmt.Sprintf("%T", in)
	}
	strs = append(strs, str)
	return strs, idxs
}
