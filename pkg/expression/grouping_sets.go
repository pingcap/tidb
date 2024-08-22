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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// GroupingSets indicates the grouping sets definition.
type GroupingSets []GroupingSet

// GroupingSet indicates one grouping set definition.
type GroupingSet []GroupingExprs

// GroupingExprs indicates one grouping-expressions inside a grouping set.
type GroupingExprs []Expression

// NewGroupingSets new a grouping sets from a slice of expression.
func NewGroupingSets(groupCols []Expression) GroupingSets {
	gss := make(GroupingSets, 0, len(groupCols))
	for _, gCol := range groupCols {
		ge := make(GroupingExprs, 0, 1)
		ge = append(ge, gCol)
		gss = append(gss, GroupingSet{ge})
	}
	return gss
}

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
	normalAggArgsIDSet := intset.NewFastIntSet()
	for _, one := range columnInNormalAggArgs {
		normalAggArgsIDSet.Insert(int(one.UniqueID))
	}

	// identify the suitable grouping set for normal agg.
	allGroupingColIDs := gss.AllSetsColIDs()
	for idx, groupingSet := range gss {
		// diffCols are those columns being filled with null in the group row data of current grouping set.
		diffCols := allGroupingColIDs.Difference(*groupingSet.AllColIDs())
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
	setIDs := make([]*intset.FastIntSet, 0, len(gss))
	for _, groupingSet := range gss {
		setIDs = append(setIDs, groupingSet.AllColIDs())
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

// AllColIDs collect all the grouping col's uniqueID. (here assuming that all the grouping expressions are single col)
func (gs GroupingSet) AllColIDs() *intset.FastIntSet {
	res := intset.NewFastIntSet()
	for _, groupingExprs := range gs {
		// on the condition that every grouping expression is single column.
		// eg: group by a, b, c
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
	return gs.StringWithCtx(errors.RedactLogDisable)
}

// StringWithCtx is used to output a string which simply described current grouping set.
func (gs GroupingSet) StringWithCtx(redact string) string {
	var str strings.Builder
	str.WriteString("{")
	for i, one := range gs {
		if i != 0 {
			str.WriteString(",")
		}
		str.WriteString(one.StringWithCtx(redact))
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
func (gs GroupingSet) ToPB(ctx EvalContext, client kv.Client) (*tipb.GroupingSet, error) {
	res := &tipb.GroupingSet{}
	for _, gExprs := range gs {
		gExprsPB, err := ExpressionsToPBList(ctx, gExprs, client)
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
func (gss GroupingSets) AllSetsColIDs() *intset.FastIntSet {
	res := intset.NewFastIntSet()
	for _, groupingSet := range gss {
		res.UnionWith(*groupingSet.AllColIDs())
	}
	return &res
}

// String is used to output a string which simply described current grouping sets.
func (gss GroupingSets) String() string {
	return gss.StringWithCtx(errors.RedactLogDisable)
}

// StringWithCtx is used to output a string which simply described current grouping sets.
func (gss GroupingSets) StringWithCtx(redact string) string {
	var str strings.Builder
	str.WriteString("[")
	for i, gs := range gss {
		if i != 0 {
			str.WriteString(",")
		}
		str.WriteString(gs.StringWithCtx(redact))
	}
	str.WriteString("]")
	return str.String()
}

// ToPB is used to convert current grouping sets to pb constructor.
func (gss GroupingSets) ToPB(ctx EvalContext, client kv.Client) ([]*tipb.GroupingSet, error) {
	res := make([]*tipb.GroupingSet, 0, len(gss))
	for _, gs := range gss {
		one, err := gs.ToPB(ctx, client)
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
	oldOne := intset.NewFastIntSet()
	newOne := intset.NewFastIntSet()
	for _, one := range g {
		oldOne.Insert(int(one.(*Column).UniqueID))
	}
	for _, one := range other {
		newOne.Insert(int(one.(*Column).UniqueID))
	}
	return oldOne.SubsetOf(newOne)
}

// IDSet is used to collect column ids inside grouping expressions into a fast int set.
func (g GroupingExprs) IDSet() *intset.FastIntSet {
	res := intset.NewFastIntSet()
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
	return g.StringWithCtx(errors.RedactLogDisable)
}

// StringWithCtx is used to output a string which simply described current grouping expressions.
func (g GroupingExprs) StringWithCtx(redact string) string {
	var str strings.Builder
	str.WriteString("<")
	for i, one := range g {
		if i != 0 {
			str.WriteString(",")
		}
		str.WriteString(one.StringWithCtx(redact))
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

/********************************* ROllUP SUPPORT UTILITY *********************************/
//
// 		case: select count(1) from t group by a,a,a,a,a with rollup.
//
// Eg: in the group by a,a,a,a,a with rollup cases. We don't really need rollup
// the grouping sets as: {a,a,a,a,a},{a,a,a,a},{a,a,a},{a,a},{a},{}, although it
// can work with copying the column 'a' multi times and distinguish them as a1, a2...
//
// since group by {a,a} is same as group by {a,a,a} and {a}...; or since group by
// {b,a,a} is same as group by {b,a} and {b,a,a,a}. In order to avoid unnecessary
// redundant column copy, we introduce a new gen_col named as "grouping_pos" to
// distinguish several grouping set with same basic distinct group items.
//
// the above case can be simplified as: {a, gid, gpos}, and gid is used to distinguish
// deduplicated grouping sets, and gpos is indicated to distinguish duplicated grouping
// sets. the grouping sets are:
// {a,0,0}, {a,0,1}, {a,0,2}, {a,0,3}, {a,0,4},{null,1,5}
//
// when a is existed, the gid is all the same as 0, that's to say, the first 5 grouping
// set are all the same one grouping set logically, but why we still need to store them
// 5 times? because we want to output needed aggregation row as required from rollup syntax;
// so here we use gpos to distinguish them. In sixth grouping set, the 'a' is null, the
// gid is newed as 1, it means a new grouping set different with those previous ones, for
// computation convenience, we increment its gpos number as well.
//
// so should care about the gpos when doing the data aggregation? Yes we did, in the above
// case, shuffle keys should also include the grouping_pos, while the grouping function
// still only care about the gid as before.
//
// mysql> select count(1), grouping(a+1) from t group by a+1, 1+a with rollup;
// original grouping sets are:
//        1: {a+1,  1+a}    ---> duplicate! proj a+1 let's say as column#1
//        2: {a+1}
//        3: {}
//  distinguish them in projection item:
//        Expand schema:[a, column#1, gid, gpos]
//          |     L1 proj: [a, column#1, 0, 0]; L2 proj: [a, column#1, 0, 1]; L3 proj: [a, null, 1, 2]
//          |
//          +--- Projection a+1 as column#1
//
// +----------+---------------+
// | count(1) | grouping(a+1) |
// +----------+---------------+                            a+1,  gid,  gpos
// |        1 |             0 | ---+-----> grouping set{column#1,  0,   0}
// |        1 |             0 | ---+
// |        1 |             0 | ---+-----> grouping set{column#1,  0,   1}
// |        1 |             0 | ---+
// |        2 |             1 | ---------> grouping set{column#1,  1,   2}
// +----------+---------------+
// 5 rows in set (0.01 sec)
//
// Yes, as you see it, tidb should be able to
// 1: grouping item semantic equivalence check logic
// 2: matching select list item to group by item (this seems hard)

// RollupGroupingSets roll up the given expressions, and iterate out a slice of grouping set expressions.
func RollupGroupingSets(rollupExprs []Expression) GroupingSets {
	// rollupExpr is recursive decrease pattern.
	// eg: [a,b,c] => {a,b,c}, {a,b},{a},{}
	res := GroupingSets{}
	// loop from -1 leading the inner loop start from {}, {a}, ...
	for i := -1; i < len(rollupExprs); i++ {
		groupingExprs := GroupingExprs{}
		for j := 0; j <= i; j++ {
			// append all the items from 0 to j.
			groupingExprs = append(groupingExprs, rollupExprs[j])
		}
		res = append(res, newGroupingSet(groupingExprs))
	}
	return res
}

// AdjustNullabilityFromGroupingSets adjust the nullability of the Expand schema out.
func AdjustNullabilityFromGroupingSets(gss GroupingSets, schema *Schema) {
	// If anyone (grouping set) of the grouping sets doesn't include one grouping-set col, meaning that
	// this col must be fill the null value in one level projection.
	// Exception: let's say a non-rollup case: grouping sets (a,b,c),(a,b),(a), the `a` is in every grouping
	// set, so it won't be filled with null value at any time, the nullable change is unnecessary.
	groupingIDs := gss.AllSetsColIDs()
	// cache the grouping ids set to avoid fetch them multi times below.
	groupingIDsSlice := make([]*intset.FastIntSet, 0, len(gss))
	for _, oneGroupingSet := range gss {
		groupingIDsSlice = append(groupingIDsSlice, oneGroupingSet.AllColIDs())
	}
	for idx, col := range schema.Columns {
		if !groupingIDs.Has(int(col.UniqueID)) {
			// not a grouping-set col, maybe un-related column, keep it real.
			continue
		}
		for _, oneSetIDs := range groupingIDsSlice {
			if !oneSetIDs.Has(int(col.UniqueID)) {
				// this col may be projected as null value, change it as nullable.
				schema.Columns[idx].RetType.SetFlag(col.RetType.GetFlag() &^ mysql.NotNullFlag)
			}
		}
	}
}

// DeduplicateGbyExpression is try to detect the semantic equivalence expression, and mark them into same position.
// eg: group by a+b, b+a, b with rollup.
// the 1st and 2nd expression is semantically equivalent, so we only need to keep the distinct expression: [a+b, b]
// down, and output another position slice out, the [0, 0, 1] for the case above.
func DeduplicateGbyExpression(exprs []Expression) ([]Expression, []int) {
	distinctExprs := make([]Expression, 0, len(exprs))
	distinctMap := make(map[string]int, len(exprs))
	for _, expr := range exprs {
		// -1 means pos is not assigned yet.
		distinctMap[string(expr.CanonicalHashCode())] = -1
	}
	// pos is from 0 to len(distinctMap)-1
	pos := 0
	posSlice := make([]int, 0, len(exprs))
	for _, one := range exprs {
		key := string(one.CanonicalHashCode())
		if val, ok := distinctMap[key]; ok {
			if val == -1 {
				// means a new distinct expr.
				distinctExprs = append(distinctExprs, one)
				posSlice = append(posSlice, pos)
				// assign this pos to val.
				distinctMap[key] = pos
				pos++
			} else {
				// means this expr is SemanticEqual with a previous one, ref the pos
				posSlice = append(posSlice, val)
			}
		}
	}
	return distinctExprs, posSlice
}

// RestoreGbyExpression restore the new gby expression according to recorded idxes reference.
func RestoreGbyExpression(exprs []*Column, idxes []int) []Expression {
	res := make([]Expression, 0, len(idxes))
	for _, pos := range idxes {
		res = append(res, exprs[pos].Clone())
	}
	return res
}

// DistinctSize judge whether there exist duplicate grouping set and output the distinct grouping set size.
// for example: grouping set{a, a, b}, and grouping set{a, b}, they actually group on the same thing --- (a,b)
// we can tell that from its unique id set equation. Note: group item here must be a column at instant.
func (gss GroupingSets) DistinctSize() (int, []uint64, map[int]map[uint64]struct{}) {
	// default 64
	return gss.DistinctSizeWithThreshold(64)
}

// DistinctSizeWithThreshold is exported for test with smaller injected N rather constructing a large grouping sets.
func (gss GroupingSets) DistinctSizeWithThreshold(N int) (int, []uint64, map[int]map[uint64]struct{}) {
	// all the group by item are col, deduplicate from id-set.
	distinctGroupingIDsPos := make([]int, 0, len(gss))
	originGroupingIDsSlice := make([]*intset.FastIntSet, 0, len(gss))

	for _, oneGroupingSet := range gss {
		curIDs := oneGroupingSet.AllColIDs()
		duplicate := false
		for _, offset := range distinctGroupingIDsPos {
			if originGroupingIDsSlice[offset].Equals(*curIDs) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			// if distinct, record its offset in originGroupingIDsSlice.
			distinctGroupingIDsPos = append(distinctGroupingIDsPos, len(originGroupingIDsSlice))
		}
		// record current grouping set ids.
		originGroupingIDsSlice = append(originGroupingIDsSlice, curIDs)
	}
	// once distinct size greater than 64, just allocating gid for every grouping set here (utilizing cached slice here).
	// otherwise, the later gid allocation logic will compute the logic above again.
	if len(distinctGroupingIDsPos) > N {
		gids := make([]uint64, len(originGroupingIDsSlice))
		// for every distinct grouping set, the gid allocation is from 0 ~ (distinctSize-1).
		for gid, offset := range distinctGroupingIDsPos {
			oneDistinctSetIDs := originGroupingIDsSlice[offset]
			for idx, oneOriginSetIDs := range originGroupingIDsSlice {
				// for every equivalent grouping set, store the same gid.
				if oneDistinctSetIDs.Equals(*oneOriginSetIDs) {
					gids[idx] = uint64(gid)
				}
			}
		}

		allIDs := gss.AllSetsColIDs()
		id2GIDs := make(map[int]map[uint64]struct{}, allIDs.Len())
		allIDs.ForEach(func(i int) {
			collectionMap := make(map[uint64]struct{})
			// for every original column unique, traverse the all grouping set.
			for idx, oneOriginSetIDs := range originGroupingIDsSlice {
				if oneOriginSetIDs.Has(i) {
					// this column is needed in this grouping set. maintaining the map.
					collectionMap[gids[idx]] = struct{}{}
				}
			}
			// id2GIDs maintained the needed-column's grouping sets (GIDs)
			id2GIDs[i] = collectionMap
		})
		return len(distinctGroupingIDsPos), gids, id2GIDs
	}
	return len(distinctGroupingIDsPos), nil, nil
}
