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

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
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
	normalAggArgs = newFunction(ast.Plus, d, newLonglong(1))
	offset = newGroupingSets.TargetOne([]Expression{normalAggArgs})
	require.NotEqual(t, offset, -1)
	require.Equal(t, offset, 0) // default

	// mock normal agg count(d+c)
	normalAggArgs = newFunction(ast.Plus, d, c)
	offset = newGroupingSets.TargetOne([]Expression{normalAggArgs})
	require.NotEqual(t, offset, -1)
	require.Equal(t, offset, 1) // only {c} can supply d and c

	// mock normal agg count(d+a)
	normalAggArgs = newFunction(ast.Plus, d, a)
	offset = newGroupingSets.TargetOne([]Expression{normalAggArgs})
	require.NotEqual(t, offset, -1)
	require.Equal(t, offset, 0) // only {a,b} can supply d and a

	// mock normal agg count(d+a+c)
	normalAggArgs = newFunction(ast.Plus, d, newFunction(ast.Plus, a, c))
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
