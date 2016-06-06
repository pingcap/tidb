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
	"math"
	"strings"
)

// ToString explains a Plan, returns description string.
func ToString(p Plan) string {
	strs, _ := toString(p, []string{}, []int{})
	return strings.Join(strs, "->")
}

func toString(in Plan, strs []string, idxs []int) ([]string, []int) {
	switch in.(type) {
	case *JoinOuter, *JoinInner, *Join, *Union:
		idxs = append(idxs, len(strs))
	}

	for _, c := range in.GetChildren() {
		strs, idxs = toString(c, strs, idxs)
	}

	var str string
	switch x := in.(type) {
	case *CheckTable:
		str = "CheckTable"
	case *IndexScan:
		str = fmt.Sprintf("Index(%s.%s)", x.Table.Name.L, x.Index.Name.L)
		if x.LimitCount != nil {
			str += fmt.Sprintf(" + Limit(%v)", *x.LimitCount)
		}
	case *Limit:
		str = "Limit"
	case *SelectFields:
		str = "Fields"
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
	case *TableScan:
		if len(x.Ranges) > 0 {
			ran := x.Ranges[0]
			if ran.LowVal != math.MinInt64 || ran.HighVal != math.MaxInt64 {
				str = fmt.Sprintf("Range(%s)", x.Table.Name.L)
			} else {
				str = fmt.Sprintf("Table(%s)", x.Table.Name.L)
			}
		} else {
			str = fmt.Sprintf("Table(%s)", x.Table.Name.L)
		}
		if x.LimitCount != nil {
			str += fmt.Sprintf(" + Limit(%v)", *x.LimitCount)
		}
	case *JoinOuter:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		str = "OuterJoin{" + strings.Join(children, "->") + "}"
		idxs = idxs[:last]
	case *JoinInner:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		str = "InnerJoin{" + strings.Join(children, "->") + "}"
		idxs = idxs[:last]
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
	case *NewTableScan:
		str = fmt.Sprintf("DataScan(%v)", x.Table.Name.L)
	case *Selection:
		str = "Selection"
	case *Projection:
		str = "Projection"
	case *Aggregation:
		str = "Aggr"
	case *Aggregate:
		str = "Aggregate"
	case *Distinct:
		str = "Distinct"
	default:
		str = fmt.Sprintf("%T", in)
	}
	strs = append(strs, str)
	return strs, idxs
}
