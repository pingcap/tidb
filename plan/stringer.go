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
	case *Join, *Union, *PhysicalHashJoin, *PhysicalHashSemiJoin:
		idxs = append(idxs, len(strs))
	}

	for _, c := range in.GetChildren() {
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
	case *PhysicalDummyScan:
		str = "Dummy"
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
			l := eq.Args[0].String()
			r := eq.Args[1].String()
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
	case *Apply:
		str = fmt.Sprintf("Apply(%s)", ToString(x.InnerPlan))
	case *PhysicalApply:
		str = fmt.Sprintf("Apply(%s)", ToString(x.InnerPlan))
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
	case *Filter:
		str = "Filter"
	case *Sort:
		str = "Sort"
		if x.ExecLimit != nil {
			str += fmt.Sprintf(" + Limit(%v) + Offset(%v)", x.ExecLimit.Count, x.ExecLimit.Offset)
		}
	case *Join:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		str = "Join{" + strings.Join(children, "->") + "}"
		idxs = idxs[:last]
	case *Union:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		str = "UnionAll{" + strings.Join(children, "->") + "}"
		idxs = idxs[:last]
	case *DataSource:
		str = fmt.Sprintf("DataScan(%v)", x.Table.Name.L)
	case *Selection:
		str = "Selection"
	case *Projection:
		str = "Projection"
	case *PhysicalAggregation:
		switch x.AggType {
		case StreamedAgg:
			str = "StreamAgg"
		default:
			str = "HashAgg"
		}
	case *Aggregation:
		str = "Aggr"
	case *Distinct:
		str = "Distinct"
	case *Trim:
		str = "Trim"
	default:
		str = fmt.Sprintf("%T", in)
	}
	strs = append(strs, str)
	return strs, idxs
}
