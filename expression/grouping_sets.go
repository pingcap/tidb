// Copyright 2022 PingCAP, Inc.
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

package expression

import (
	"strings"

	"github.com/pingcap/tidb/kv"
	fd "github.com/pingcap/tidb/planner/funcdep"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// GroupingSets indicates the grouping sets definition.
type GroupingSets []GroupingSet

// GroupingSet indicates one grouping set definition.
type GroupingSet []GroupingExprs

// GroupingExprs indicates one grouping-expressions inside a grouping set.
type GroupingExprs []Expression

// Merge function will explore the internal grouping expressions and try to find the minimum grouping sets. (prefix merging)
func (gss GroupingSets) Merge() GroupingSets {
	// for now, there is precondition that all grouping expressions are columns.
	// for example: (a,b,c) and (a,b) and (a) will be merged as a one.
	// Eg:
	// before merging, there are 4 grouping sets.
	// GroupingSets:
	//     [
	//         [[a,b,c],]             // every set including a grouping Expressions for initial.
	//         [[a,b],]
	//         [[a],]
	//         [[e],]
	//     ]
	//
	// after merging, there is only 2 grouping set.
	// GroupingSets:
	//     [
	//         [[a],[a,b],[a,b,c],]   // one set including 3 grouping Expressions after merging if possible.
	//         [[e],]
	//     ]
	//
	// care about the prefix order, which should be taken as following the group layout expanding rule. (simple way)
	// [[a],[b,a],[c,b,a],] is also conforming the rule, gradually including one/more column(s) inside for one time.

	newGroupingSets := make(GroupingSets, 0, len(gss))
	for _, oneGroupingSet := range gss {
		for _, oneGroupingExpr := range oneGroupingSet {
			if len(newGroupingSets) == 0 {
				// means there is nothing in new grouping sets, adding current one anyway.
				newGroupingSets = append(newGroupingSets, newGroupingSet(oneGroupingExpr))
				continue
			}
			newGroupingSets = newGroupingSets.MergeOne(oneGroupingExpr)
		}
	}
	return newGroupingSets
}

// MergeOne is used to merge one grouping expressions into current grouping sets.
func (gss GroupingSets) MergeOne(targetOne GroupingExprs) GroupingSets {
	// for every existing grouping set, check the grouping-exprs inside and whether the current grouping-exprs is
	// super-set of it or sub-set of it, adding current one to the correct position of grouping-exprs slice.
	//
	//                  [[a,b]]
	//                     |
	//               /  offset 0
	//              |
	//             [b]    when adding [b] grouping expr here, since it's a sub-set of current [a,b] with offset 0, take the offset 0.
	//              |
	//     insert with offset 0, and the other elements move right.
	//
	//                 [[b],   [a,b]]
	//             offset 0       1
	//                                \
	//                                [a,b,c,d]
	//        when adding [a,b,c,d] grouping expr here, since it's a super-set of current [a,b] with offset 1, take the offset as 1+1.
	//
	//     result grouping set:  [[b], [a,b], [a,b,c,d]]， expanding with step with two or more columns is acceptable and reasonable.
	//                             ｜    ｜       ｜
	//                              +----+-------+       every previous one is the subset of the latter one.
	//
	for i, oneNewGroupingSet := range gss {
		// for every group set，try to find its position to insert if possible，otherwise create a new grouping set.
		for j := len(oneNewGroupingSet) - 1; j >= 0; j-- {
			cur := oneNewGroupingSet[j]
			if targetOne.SubSetOf(cur) {
				if j == 0 {
					// the right pos should be the head (-1)
					cp := make(GroupingSet, 0, len(oneNewGroupingSet)+1)
					cp = append(cp, targetOne)
					cp = append(cp, oneNewGroupingSet...)
					gss[i] = cp
					return gss
				}
				// do the left shift to find the right insert pos.
				continue
			}
			if j == len(oneNewGroupingSet)-1 {
				// which means the targetOne itself is the super set of current right-most grouping set.
				if cur.SubSetOf(targetOne) {
					// the right pos should be the len(oneNewGroupingSet)
					oneNewGroupingSet = append(oneNewGroupingSet, targetOne)
					gss[i] = oneNewGroupingSet
					return gss
				}
				// which means the targetOne can't fit itself in this grouping set, continue next grouping set.
				break
			}
			// successfully fit in, current j is the right pos to insert.
			cp := make(GroupingSet, 0, len(oneNewGroupingSet)+1)
			cp = append(cp, oneNewGroupingSet[:j+1]...)
			cp = append(cp, targetOne)
			cp = append(cp, oneNewGroupingSet[j+1:]...)
			gss[i] = cp
			return gss
		}
	}
	// here means we couldn't find even one GroupingSet to fill the targetOne, creating a new one.
	gss = append(gss, newGroupingSet(targetOne))
	// gs is an alias of slice [], we should return it back after being changed.
	return gss
}

// TargetOne is used to find a valid group layout for normal agg, note that: the args in normal agg are not necessary to be column.
func (gss GroupingSets) TargetOne(normalAggArgs []Expression) int {
	// it has three cases.
	//	1: group sets {<a,b>}, {<c>} when the normal agg(d), agg(d) can only occur in the group of <a,b> or <c> after tuple split. (normal column are appended)
	//  2: group sets {<a,b>}, {<c>} when the normal agg(c), agg(c) can only be found in group of <c>, since it will be filled with null in group of <a,b>
	//  3: group sets {<a,b>}, {<c>} when the normal agg with multi col args, it will be a little difficult, which is banned in canUse3Stage4MultiDistinctAgg by now.
	//		eg1: agg(c,d), the c and d can only be found in group of <c> in which d is also attached, while c will be filled with null in group of <a,b>
	//		eg2: agg(b,c,d), we couldn't find a valid group either in <a,b> or <c>, unless we copy one column from b and attach it to the group data of <c>. (cp c to <a,b> is also effective)
	//
	// from the theoretical, why we have to fill the non-current-group-column with null value? even we may fill it as null value and copy it again like c in case 3,
	// the basic reason is that we need a unified group layout sequence to feed different layout-required distinct agg. Like what we did here, we should group the
	// original source with sequence columns as <a,b,c>. For distinct(a,b), we don't wanna c in this <a,b,c> group layout to impact what we are targeting for --- the
	// real group from <a,b>. So even we had the groupingID in repeated data row to identify which distinct agg this row is prepared for, we still need to fill the c
	// as null and groupingID as 1 to guarantee group<a,b,c,ID> operator can equate to group<a,b,null,1> which is equal to group<a,b>, that's what the upper layer
	// distinct(a,b) want for. For distinct(c), the same is true.
	//
	// For normal agg you better choose your targeting group-set data, otherwise, otherwise your get is all groups，most of them is endless null values filled by
	// Expand operator, and null value can also influence your non null-strict normal agg, although you don't want them to.
	//
	// here we only consider 1&2 since normal agg with multi col args is banned.
	columnInNormalAggArgs := make([]*Column, 0, len(normalAggArgs))
	for _, one := range normalAggArgs {
		columnInNormalAggArgs = append(columnInNormalAggArgs, ExtractColumns(one)...)
	}
	if len(columnInNormalAggArgs) == 0 {
		// no column in normal agg. eg: count(1), specify the default grouping set ID 0+1.
		return 0
	}
	// for other normal agg args like: count(a), count(a+b), count(not(a is null)) and so on.
	normalAggArgsIDSet := fd.NewFastIntSet()
	for _, one := range columnInNormalAggArgs {
		normalAggArgsIDSet.Insert(int(one.UniqueID))
	}

	// identify the suitable grouping set for normal agg.
	allGroupingColIDs := gss.AllSetsColIDs()
	for idx, groupingSet := range gss {
		// diffCols are those columns being filled with null in the group row data of current grouping set.
		diffCols := allGroupingColIDs.Difference(*groupingSet.allSetColIDs())
		if diffCols.Intersects(normalAggArgsIDSet) {
			// normal agg's target arg columns are being filled with null value in this grouping set, continue next grouping set check.
			continue
		}
		return idx
	}
	// todo: if we couldn't find a existed current valid group layout, we need to copy the column out from being filled with null value.
	return -1
}

// NeedCloneColumn indicate whether we need to copy column to when expanding datasource.
func (gss GroupingSets) NeedCloneColumn() bool {
	// for grouping sets like: {<a,c>},{<c>} / {<a,c>},{<b,c>}
	// the column c should be copied one more time here, otherwise it will be filled with null values and not visible for the other grouping set again.
	setIDs := make([]*fd.FastIntSet, 0, len(gss))
	for _, groupingSet := range gss {
		setIDs = append(setIDs, groupingSet.allSetColIDs())
	}
	for idx, oneSetIDs := range setIDs {
		for j := idx + 1; j < len(setIDs); j++ {
			otherSetIDs := setIDs[j]
			if oneSetIDs.Intersects(*otherSetIDs) {
				return true
			}
		}
	}
	return false
}

// IsEmpty indicates whether current grouping set is empty.
func (gs GroupingSet) IsEmpty() bool {
	if len(gs) == 0 {
		return true
	}
	for _, g := range gs {
		if !g.IsEmpty() {
			return false
		}
	}
	return true
}

func (gs GroupingSet) allSetColIDs() *fd.FastIntSet {
	res := fd.NewFastIntSet()
	for _, groupingExprs := range gs {
		for _, one := range groupingExprs {
			res.Insert(int(one.(*Column).UniqueID))
		}
	}
	return &res
}

// ExtractCols is used to extract basic columns from one grouping set.
func (gs GroupingSet) ExtractCols() []*Column {
	cols := make([]*Column, 0, len(gs))
	for _, groupingExprs := range gs {
		for _, one := range groupingExprs {
			cols = append(cols, one.(*Column))
		}
	}
	return cols
}

// Clone is used to clone a copy of current grouping set.
func (gs GroupingSet) Clone() GroupingSet {
	gc := make(GroupingSet, 0, len(gs))
	for _, one := range gs {
		gc = append(gc, one.Clone())
	}
	return gc
}

// String is used to output a string which simply described current grouping set.
func (gs GroupingSet) String() string {
	var str strings.Builder
	str.WriteString("{")
	for i, one := range gs {
		if i != 0 {
			str.WriteString(",")
		}
		str.WriteString(one.String())
	}
	str.WriteString("}")
	return str.String()
}

// MemoryUsage is used to output current memory usage by current grouping set.
func (gs GroupingSet) MemoryUsage() int64 {
	sum := size.SizeOfSlice + int64(cap(gs))*size.SizeOfPointer
	for _, one := range gs {
		sum += one.MemoryUsage()
	}
	return sum
}

// ToPB is used to convert current grouping set to pb constructor.
func (gs GroupingSet) ToPB(sc *stmtctx.StatementContext, client kv.Client) (*tipb.GroupingSet, error) {
	res := &tipb.GroupingSet{}
	for _, gExprs := range gs {
		gExprsPB, err := ExpressionsToPBList(sc, gExprs, client)
		if err != nil {
			return nil, err
		}
		res.GroupingExprs = append(res.GroupingExprs, &tipb.GroupingExpr{GroupingExpr: gExprsPB})
	}
	return res, nil
}

// IsEmpty indicates whether current grouping sets is empty.
func (gss GroupingSets) IsEmpty() bool {
	if len(gss) == 0 {
		return true
	}
	for _, gs := range gss {
		if !gs.IsEmpty() {
			return false
		}
	}
	return true
}

// AllSetsColIDs is used to collect all the column id inside into a fast int set.
func (gss GroupingSets) AllSetsColIDs() *fd.FastIntSet {
	res := fd.NewFastIntSet()
	for _, groupingSet := range gss {
		res.UnionWith(*groupingSet.allSetColIDs())
	}
	return &res
}

// String is used to output a string which simply described current grouping sets.
func (gss GroupingSets) String() string {
	var str strings.Builder
	str.WriteString("[")
	for i, gs := range gss {
		if i != 0 {
			str.WriteString(",")
		}
		str.WriteString(gs.String())
	}
	str.WriteString("]")
	return str.String()
}

// ToPB is used to convert current grouping sets to pb constructor.
func (gss GroupingSets) ToPB(sc *stmtctx.StatementContext, client kv.Client) ([]*tipb.GroupingSet, error) {
	res := make([]*tipb.GroupingSet, 0, len(gss))
	for _, gs := range gss {
		one, err := gs.ToPB(sc, client)
		if err != nil {
			return nil, err
		}
		res = append(res, one)
	}
	return res, nil
}

func newGroupingSet(oneGroupingExpr GroupingExprs) GroupingSet {
	res := make(GroupingSet, 0, 1)
	res = append(res, oneGroupingExpr)
	return res
}

// IsEmpty indicates whether current grouping expressions are empty.
func (g GroupingExprs) IsEmpty() bool {
	return len(g) == 0
}

// SubSetOf is used to do the logical computation of subset between two grouping expressions.
func (g GroupingExprs) SubSetOf(other GroupingExprs) bool {
	oldOne := fd.NewFastIntSet()
	newOne := fd.NewFastIntSet()
	for _, one := range g {
		oldOne.Insert(int(one.(*Column).UniqueID))
	}
	for _, one := range other {
		newOne.Insert(int(one.(*Column).UniqueID))
	}
	return oldOne.SubsetOf(newOne)
}

// IDSet is used to collect column ids inside grouping expressions into a fast int set.
func (g GroupingExprs) IDSet() *fd.FastIntSet {
	res := fd.NewFastIntSet()
	for _, one := range g {
		res.Insert(int(one.(*Column).UniqueID))
	}
	return &res
}

// Clone is used to clone a copy of current grouping expressions.
func (g GroupingExprs) Clone() GroupingExprs {
	gc := make(GroupingExprs, 0, len(g))
	for _, one := range g {
		gc = append(gc, one.Clone())
	}
	return gc
}

// String is used to output a string which simply described current grouping expressions.
func (g GroupingExprs) String() string {
	var str strings.Builder
	str.WriteString("<")
	for i, one := range g {
		if i != 0 {
			str.WriteString(",")
		}
		str.WriteString(one.String())
	}
	str.WriteString(">")
	return str.String()
}

// MemoryUsage is used to output current memory usage by current grouping expressions.
func (g GroupingExprs) MemoryUsage() int64 {
	sum := size.SizeOfSlice + int64(cap(g))*size.SizeOfInterface
	for _, one := range g {
		sum += one.MemoryUsage()
	}
	return sum
}
