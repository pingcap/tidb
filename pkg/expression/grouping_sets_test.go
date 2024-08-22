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
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
)

func TestGroupSetsTargetOne(t *testing.T) {
	defer view.Stop()
	a := &Column{
		UniqueID: 1,
	}
	b := &Column{
		UniqueID: 2,
	}
	c := &Column{
		UniqueID: 3,
	}
	d := &Column{
		UniqueID: 4,
	}
	// non-merged group sets
	//	1: group sets {<a,b>}, {<c>} when the normal agg(d), d can occur in either group of <a,b> or <c> after tuple split. (normal column are appended)
	//  2: group sets {<a,b>}, {<c>} when the normal agg(c), c can only be found in group of <c>, since it will be filled with null in group of <a,b>
	//  3: group sets {<a,b>}, {<c>} when the normal agg with multi col args, it will be a little difficult, which is banned in canUse3Stage4MultiDistinctAgg by now.
	newGroupingSets := make(GroupingSets, 0, 3)
	groupingExprs1 := []Expression{a, b}
	groupingExprs2 := []Expression{c}
	newGroupingSets = append(newGroupingSets, GroupingSet{groupingExprs1})
	newGroupingSets = append(newGroupingSets, GroupingSet{groupingExprs2})

	targetOne := []Expression{d}
	offset := newGroupingSets.TargetOne(targetOne)
	require.NotEqual(t, offset, -1)
	require.Equal(t, offset, 0) // <a,b> and <c> both ok

	targetOne = []Expression{c}
	offset = newGroupingSets.TargetOne(targetOne)
	require.NotEqual(t, offset, -1)
	require.Equal(t, offset, 1) // only <c> is ok

	targetOne = []Expression{b}
	offset = newGroupingSets.TargetOne(targetOne)
	require.NotEqual(t, offset, -1)
	require.Equal(t, offset, 0) // only <a,b> is ok

	targetOne = []Expression{a}
	offset = newGroupingSets.TargetOne(targetOne)
	require.NotEqual(t, offset, -1)
	require.Equal(t, offset, 0) // only <a,b> is ok

	targetOne = []Expression{b, c}
	offset = newGroupingSets.TargetOne(targetOne)
	require.Equal(t, offset, -1) // no valid one can be found.

	// merged group sets
	// merged group sets {<a>, <a,b>} when the normal agg(d),
	// from prospect from data grouping, we need keep the widest grouping layout <a> when trying
	// to shuffle data targeting for grouping layout {<a>, <a,b>}; Additional, since column b
	// hasn't been pre-agg or something because current layout is <a>, we should keep all the
	// complete rows until to the upper receiver, which means there is no agg/group operator before
	// shuffler take place.
}

func TestGroupSetsTargetOneCompoundArgs(t *testing.T) {
	defer view.Stop()
	a := &Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	b := &Column{
		UniqueID: 2,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	c := &Column{
		UniqueID: 3,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	d := &Column{
		UniqueID: 4,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	// grouping set: {a,b}, {c}
	newGroupingSets := make(GroupingSets, 0, 3)
	groupingExprs1 := []Expression{a, b}
	groupingExprs2 := []Expression{c}
	newGroupingSets = append(newGroupingSets, GroupingSet{groupingExprs1})
	newGroupingSets = append(newGroupingSets, GroupingSet{groupingExprs2})

	var normalAggArgs Expression
	// mock normal agg count(1)
	normalAggArgs = newLonglong(1)
	offset := newGroupingSets.TargetOne([]Expression{normalAggArgs})
	require.NotEqual(t, offset, -1)
	require.Equal(t, offset, 0) // default

	// mock normal agg count(d+1)
	normalAggArgs = newFunctionWithMockCtx(ast.Plus, d, newLonglong(1))
	offset = newGroupingSets.TargetOne([]Expression{normalAggArgs})
	require.NotEqual(t, offset, -1)
	require.Equal(t, offset, 0) // default

	// mock normal agg count(d+c)
	normalAggArgs = newFunctionWithMockCtx(ast.Plus, d, c)
	offset = newGroupingSets.TargetOne([]Expression{normalAggArgs})
	require.NotEqual(t, offset, -1)
	require.Equal(t, offset, 1) // only {c} can supply d and c

	// mock normal agg count(d+a)
	normalAggArgs = newFunctionWithMockCtx(ast.Plus, d, a)
	offset = newGroupingSets.TargetOne([]Expression{normalAggArgs})
	require.NotEqual(t, offset, -1)
	require.Equal(t, offset, 0) // only {a,b} can supply d and a

	// mock normal agg count(d+a+c)
	normalAggArgs = newFunctionWithMockCtx(ast.Plus, d, newFunctionWithMockCtx(ast.Plus, a, c))
	offset = newGroupingSets.TargetOne([]Expression{normalAggArgs})
	require.Equal(t, offset, -1) // couldn't find a group that supply d, a and c simultaneously.
}

func TestGroupingSetsMergeOneUnitTest(t *testing.T) {
	defer view.Stop()
	a := &Column{
		UniqueID: 1,
	}
	b := &Column{
		UniqueID: 2,
	}
	c := &Column{
		UniqueID: 3,
	}
	d := &Column{
		UniqueID: 4,
	}
	// test case about the right most fitness.
	newGroupingSets := make(GroupingSets, 0, 3)
	newGroupingSets = newGroupingSets[:0]
	groupingExprs := []Expression{a}
	newGroupingSets = append(newGroupingSets, GroupingSet{groupingExprs})
	newGroupingSets.MergeOne([]Expression{a, b})

	require.Equal(t, len(newGroupingSets), 1)
	require.Equal(t, len(newGroupingSets[0]), 2)
	//{a}
	require.Equal(t, len(newGroupingSets[0][0]), 1)
	//{a,b}
	require.Equal(t, len(newGroupingSets[0][1]), 2)

	newGroupingSets = newGroupingSets[:0]
	groupingExprs = []Expression{a}
	newGroupingSets = append(newGroupingSets, GroupingSet{groupingExprs})
	newGroupingSets.MergeOne([]Expression{d, c, b, a})
	newGroupingSets.MergeOne([]Expression{d, b, a})
	newGroupingSets.MergeOne([]Expression{b, a})

	require.Equal(t, len(newGroupingSets), 1)
	require.Equal(t, len(newGroupingSets[0]), 4)
	//{a}
	require.Equal(t, len(newGroupingSets[0][0]), 1)
	//{b,a}
	require.Equal(t, len(newGroupingSets[0][1]), 2)
	//{d,b,a}
	require.Equal(t, len(newGroupingSets[0][2]), 3)
	//{d,c,b,a}
	require.Equal(t, len(newGroupingSets[0][3]), 4)
}

func TestRollupGroupingSets(t *testing.T) {
	defer view.Stop()
	aTp := types.NewFieldType(mysql.TypeLong)
	aTp.SetFlag(mysql.NotNullFlag)
	a := &Column{
		UniqueID: 1,
		RetType:  aTp,
	}
	bTp := types.NewFieldType(mysql.TypeLong)
	bTp.SetFlag(mysql.NotNullFlag)
	b := &Column{
		UniqueID: 2,
		RetType:  bTp,
	}
	cTp := types.NewFieldType(mysql.TypeLong)
	cTp.SetFlag(mysql.NotNullFlag)
	c := &Column{
		UniqueID: 3,
		RetType:  cTp,
	}
	dTp := types.NewFieldType(mysql.TypeLong)
	dTp.SetFlag(mysql.NotNullFlag)
	// un-related col.
	d := &Column{
		UniqueID: 4,
		RetType:  dTp,
	}
	rollupExprs := make([]Expression, 0, 4)
	rollupExprs = append(rollupExprs, a, b, c)
	rollupGroupingSets := RollupGroupingSets(rollupExprs)
	require.Equal(t, len(rollupGroupingSets), 4)
	require.Equal(t, len(rollupGroupingSets[0]), 1)
	require.Equal(t, len(rollupGroupingSets[0][0]), 0)

	require.Equal(t, len(rollupGroupingSets[1]), 1)
	require.Equal(t, len(rollupGroupingSets[1][0]), 1)

	require.Equal(t, len(rollupGroupingSets[2]), 1)
	require.Equal(t, len(rollupGroupingSets[2][0]), 2)

	require.Equal(t, len(rollupGroupingSets[3]), 1)
	require.Equal(t, len(rollupGroupingSets[3][0]), 3)

	expandSchema := NewSchema(a, b, c, d)
	expandSchema2 := expandSchema.Clone()
	// remove the first grouping set {}
	// so the {a,b,c},{a,b},{a}, a is every grouping set, and it should be changed as nullable.
	rollupGroupingSets2 := rollupGroupingSets[1:]
	AdjustNullabilityFromGroupingSets(rollupGroupingSets2, expandSchema2)
	require.Equal(t, mysql.HasNotNullFlag(expandSchema2.Columns[0].RetType.GetFlag()), true)
	require.Equal(t, mysql.HasNotNullFlag(expandSchema2.Columns[1].RetType.GetFlag()), false)
	require.Equal(t, mysql.HasNotNullFlag(expandSchema2.Columns[2].RetType.GetFlag()), false)
	require.Equal(t, mysql.HasNotNullFlag(expandSchema2.Columns[3].RetType.GetFlag()), true)

	AdjustNullabilityFromGroupingSets(rollupGroupingSets, expandSchema)
	require.Equal(t, mysql.HasNotNullFlag(expandSchema.Columns[0].RetType.GetFlag()), false)
	require.Equal(t, mysql.HasNotNullFlag(expandSchema.Columns[1].RetType.GetFlag()), false)
	require.Equal(t, mysql.HasNotNullFlag(expandSchema.Columns[2].RetType.GetFlag()), false)
	require.Equal(t, mysql.HasNotNullFlag(expandSchema.Columns[3].RetType.GetFlag()), true)
}

func TestGroupingSetsMergeUnitTest(t *testing.T) {
	defer view.Stop()
	a := &Column{
		UniqueID: 1,
	}
	b := &Column{
		UniqueID: 2,
	}
	c := &Column{
		UniqueID: 3,
	}
	d := &Column{
		UniqueID: 4,
	}
	// [[c,d,a,b], [b], [a,b]]
	newGroupingSets := make(GroupingSets, 0, 3)
	groupingExprs := []Expression{c, d, a, b}
	newGroupingSets = append(newGroupingSets, GroupingSet{groupingExprs})
	groupingExprs = []Expression{b}
	newGroupingSets = append(newGroupingSets, GroupingSet{groupingExprs})
	groupingExprs = []Expression{a, b}
	newGroupingSets = append(newGroupingSets, GroupingSet{groupingExprs})

	// [[b], [a,b], [c,d,a,b]]
	newGroupingSets = newGroupingSets.Merge()
	require.Equal(t, len(newGroupingSets), 1)
	// only one grouping set with 3 grouping expressions.
	require.Equal(t, len(newGroupingSets[0]), 3)
	// [b]
	require.Equal(t, len(newGroupingSets[0][0]), 1)
	// [a,b]
	require.Equal(t, len(newGroupingSets[0][1]), 2)
	// [c,d,a,b]]
	require.Equal(t, len(newGroupingSets[0][2]), 4)

	//     [
	//         [[a],[b,a],[c,b,a],]   // one set including 3 grouping Expressions after merging if possible.
	//         [[d],]
	//     ]
	newGroupingSets = newGroupingSets[:0]
	groupingExprs = []Expression{c, b, a}
	newGroupingSets = append(newGroupingSets, GroupingSet{groupingExprs})
	groupingExprs = []Expression{a}
	newGroupingSets = append(newGroupingSets, GroupingSet{groupingExprs})
	groupingExprs = []Expression{b, a}
	newGroupingSets = append(newGroupingSets, GroupingSet{groupingExprs})
	groupingExprs = []Expression{d}
	newGroupingSets = append(newGroupingSets, GroupingSet{groupingExprs})

	//         [[a],[b,a],[c,b,a],]
	//         [[d],]
	newGroupingSets = newGroupingSets.Merge()
	require.Equal(t, len(newGroupingSets), 2)
	// [a]
	require.Equal(t, len(newGroupingSets[0][0]), 1)
	// [b,a]
	require.Equal(t, len(newGroupingSets[0][1]), 2)
	// [c,b,a]
	require.Equal(t, len(newGroupingSets[0][2]), 3)
	// [d]
	require.Equal(t, len(newGroupingSets[1][0]), 1)
}

func TestDistinctGroupingSets(t *testing.T) {
	defer view.Stop()
	// premise: every grouping item in grouping sets should be a col.
	a := &Column{
		UniqueID: 1,
	}
	b := &Column{
		UniqueID: 2,
	}
	c := &Column{
		UniqueID: 3,
	}
	d := &Column{
		UniqueID: 4,
	}
	// case1: non-duplicated case.
	// raw rollup expressions: [a,b,c,d]
	rawRollupExprs := []Expression{a, b, c, d}
	deduplicateExprs, pos := DeduplicateGbyExpression(rawRollupExprs)

	// nothing to deduplicate.
	require.Equal(t, len(rawRollupExprs), len(deduplicateExprs))
	require.Equal(t, len(pos), 4)
	require.Equal(t, pos[0], 0)
	require.Equal(t, pos[1], 1)
	require.Equal(t, pos[2], 2)
	require.Equal(t, pos[3], 3)

	// rollup grouping sets.
	gss := RollupGroupingSets(deduplicateExprs)
	require.Equal(t, len(gss), 5) // len = N + 1 (N = len(raw rollup expression))
	require.Equal(t, gss[0].AllColIDs().String(), "()")
	require.Equal(t, gss[1].AllColIDs().String(), "(1)")
	require.Equal(t, gss[2].AllColIDs().String(), "(1,2)")
	require.Equal(t, gss[3].AllColIDs().String(), "(1-3)")
	require.Equal(t, gss[4].AllColIDs().String(), "(1-4)")

	size, _, _ := gss.DistinctSize()
	require.Equal(t, size, 5)

	// case2: duplicated case.
	rawRollupExprs = []Expression{a, b, b, c}
	deduplicateExprs, pos = DeduplicateGbyExpression(rawRollupExprs)
	require.Equal(t, len(deduplicateExprs), 3)
	require.Equal(t, deduplicateExprs[0].StringWithCtx(errors.RedactLogDisable), "Column#1")
	require.Equal(t, deduplicateExprs[1].StringWithCtx(errors.RedactLogDisable), "Column#2")
	require.Equal(t, deduplicateExprs[2].StringWithCtx(errors.RedactLogDisable), "Column#3")
	deduplicateColumns := make([]*Column, 0, len(deduplicateExprs))
	for _, one := range deduplicateExprs {
		deduplicateColumns = append(deduplicateColumns, one.(*Column))
	}
	require.Equal(t, len(pos), 4)
	require.Equal(t, pos[0], 0)
	require.Equal(t, pos[1], 1) // ref to column#2
	require.Equal(t, pos[2], 1) // ref to column#2
	require.Equal(t, pos[3], 2)

	// restoreGbyExpression (some complicated expression will be projected as column before enter Expand)
	// so cases like below is possible:
	// GBY: a+b, b+a, c  ==>  Expand:  rollup with (column#1, column#1, c)
	//                          +-----  proj: (a+b) as column#1
	// so that why restore gby expression according to their pos is necessary.
	restoreGbyExpressions := RestoreGbyExpression(deduplicateColumns, pos)
	require.Equal(t, len(restoreGbyExpressions), 4)
	require.Equal(t, restoreGbyExpressions[0].StringWithCtx(errors.RedactLogDisable), "Column#1")
	require.Equal(t, restoreGbyExpressions[1].StringWithCtx(errors.RedactLogDisable), "Column#2")
	require.Equal(t, restoreGbyExpressions[2].StringWithCtx(errors.RedactLogDisable), "Column#2")
	require.Equal(t, restoreGbyExpressions[3].StringWithCtx(errors.RedactLogDisable), "Column#3")

	// rollup grouping sets (build grouping sets on the restored gby expression, because all the
	// complicated expressions have been projected as simple columns at this time).
	gss = RollupGroupingSets(restoreGbyExpressions)
	require.Equal(t, len(gss), 5) // len = N + 1 (N = len(raw rollup expression))
	require.Equal(t, gss[0].AllColIDs().String(), "()")
	require.Equal(t, gss[1].AllColIDs().String(), "(1)")
	require.Equal(t, gss[2].AllColIDs().String(), "(1,2)")
	require.Equal(t, gss[3].AllColIDs().String(), "(1,2)")
	require.Equal(t, gss[4].AllColIDs().String(), "(1-3)")

	// after gss is built on all the simple columns, get the distinct size if possible.
	size, _, _ = gss.DistinctSize()
	require.Equal(t, size, 4)

	// case3: when grouping sets number bigger than 64, we will allocate gid incrementally rather bit map.
	// here we inject the N as 3, so we can test the computation logic easily.
	size, gids, id2Gids := gss.DistinctSizeWithThreshold(3)
	require.Equal(t, size, 4)
	require.Equal(t, len(gids), len(gss))
	// assigned gids should be equal to the origin gss size.
	// for this case:
	// {}          ()      --> 0
	// {a}         (1)     --> 1
	// {a,b}       (1,2)   --> 2  --+---> has the same gid
	// {a,b,b}     (1,2)   --> 2  __/
	// {a,b,b,c}   (1,2,3) --> 3
	require.Equal(t, gids[0], uint64(0))
	require.Equal(t, gids[1], uint64(1))
	require.Equal(t, gids[2], uint64(2))
	require.Equal(t, gids[3], uint64(2))
	require.Equal(t, gids[4], uint64(3))

	// for every col id, mapping them to a slice of gid.
	require.Equal(t, len(id2Gids), 3)
	// 0 -->  when grouping(column#1), the corresponding affected gids should be {0}
	//            +--- explanation: when grouping(a), col-a is needed when grouping id = 1,2,3.
	require.Equal(t, len(id2Gids[1]), 3)
	_, ok := id2Gids[1][1]
	require.Equal(t, ok, true)
	_, ok = id2Gids[1][2]
	require.Equal(t, ok, true)
	_, ok = id2Gids[1][3]
	require.Equal(t, ok, true)
	// 1 --> when grouping(column#2), the corresponding affected gids should be {0,1}
	//            +--- explanation: when grouping(b), col-b is needed when grouping id = 2 or 3.
	require.Equal(t, len(id2Gids[2]), 2)
	_, ok = id2Gids[2][2]
	require.Equal(t, ok, true)
	_, ok = id2Gids[2][3]
	require.Equal(t, ok, true)
	// 2 --> when grouping(column#3), the corresponding affected gids should be {0,1,2}
	//            +--- explanation: when grouping(c), col-c is needed when grouping id = 3.
	require.Equal(t, len(id2Gids[3]), 1)
	_, ok = id2Gids[3][3]
	require.Equal(t, ok, true)
	// column d is not in the grouping set columns, so it won't be here.
}
