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
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"strings"
)

// ToString explains a Plan, returns description string.
func ToString(p Plan) string {
	strs, _ := toString(p, []string{}, []int{})
	return strings.Join(strs, "->")
}

func toString(in Plan, strs []string, idxs []int) ([]string, []int) {
	switch in.(type) {
	case *LogicalJoin, *Union, *PhysicalHashJoin, *PhysicalHashSemiJoin, *LogicalApply, *PhysicalApply, *PhysicalMergeJoin, *PhysicalIndexJoin:
		idxs = append(idxs, len(strs))
	}

	for _, c := range in.Children() {
		strs, idxs = toString(c, strs, idxs)
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
		if x.SmallTable == 0 {
			str = "RightHashJoin{" + strings.Join(children, "->") + "}"
		} else {
			str = "LeftHashJoin{" + strings.Join(children, "->") + "}"
		}
		for _, eq := range x.EqualConditions {
			l := eq.GetArgs()[0].String()
			r := eq.GetArgs()[1].String()
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *PhysicalHashSemiJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		if x.WithAux {
			str = "SemiJoinWithAux{" + strings.Join(children, "->") + "}"
		} else {
			str = "SemiJoin{" + strings.Join(children, "->") + "}"
		}
		if UseDAGPlanBuilder(x.ctx) {
			for _, eq := range x.EqualConditions {
				l := eq.GetArgs()[0].String()
				r := eq.GetArgs()[1].String()
				str += fmt.Sprintf("(%s,%s)", l, r)
			}
		}
	case *PhysicalMergeJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "MergeJoin{" + strings.Join(children, "->") + "}"
		for _, eq := range x.EqualConditions {
			l := eq.GetArgs()[0].String()
			r := eq.GetArgs()[1].String()
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *LogicalApply, *PhysicalApply:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "Apply{" + strings.Join(children, "->") + "}"
	case *Exists:
		str = "Exists"
	case *MaxOneRow:
		str = "MaxOneRow"
	case *Limit:
		str = "Limit"
	case *SelectLock:
		str = "Lock"
	case *ShowDDL:
		str = "ShowDDL"
	case *Sort:
		str = "Sort"
		if x.ExecLimit != nil {
			str += fmt.Sprintf(" + Limit(%v) + Offset(%v)", x.ExecLimit.Count, x.ExecLimit.Offset)
		}
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
	case *Union:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		str = "UnionAll{" + strings.Join(children, "->") + "}"
		idxs = idxs[:last]
	case *DataSource:
		if x.TableAsName != nil && x.TableAsName.L != "" {
			str = fmt.Sprintf("DataScan(%s)", x.TableAsName)
		} else {
			str = fmt.Sprintf("DataScan(%s)", x.tableInfo.Name)
		}
	case *Selection:
		str = "Selection"
		if UseDAGPlanBuilder(x.ctx) {
			str = fmt.Sprintf("Sel(%s)", x.Conditions)
		}
	case *Projection:
		str = "Projection"
	case *TopN:
		str = fmt.Sprintf("TopN(%s,%d,%d)", x.ByItems, x.Offset, x.Count)
	case *TableDual:
		str = "Dual"
	case *PhysicalAggregation:
		switch x.AggType {
		case StreamedAgg:
			str = "StreamAgg"
		default:
			str = "HashAgg"
		}
	case *LogicalAggregation:
		str = "Aggr("
		for i, aggFunc := range x.AggFuncs {
			str += aggFunc.String()
			if i != len(x.AggFuncs)-1 {
				str += ","
			}
		}
		str += ")"
	case *Cache:
		str = "Cache"
	case *PhysicalTableReader:
		str = fmt.Sprintf("TableReader(%s)", ToString(x.tablePlan))
	case *PhysicalIndexReader:
		str = fmt.Sprintf("IndexReader(%s)", ToString(x.indexPlan))
	case *PhysicalIndexLookUpReader:
		str = fmt.Sprintf("IndexLookUp(%s, %s)", ToString(x.indexPlan), ToString(x.tablePlan))
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
	default:
		str = fmt.Sprintf("%T", in)
	}
	strs = append(strs, str)
	return strs, idxs
}
