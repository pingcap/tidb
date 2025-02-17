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

package ranger_test

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

type rangeWithStr struct {
	ran ranger.Range
	str string
}

func rangeToString(ran *ranger.Range) string {
	if ran == nil {
		return "<nil>"
	}
	return ran.String()
}

func sampleRanges() []rangeWithStr {
	result := []rangeWithStr{
		{
			ran: ranger.Range{
				LowVal:    []types.Datum{types.NewIntDatum(1)},
				HighVal:   []types.Datum{types.NewIntDatum(1)},
				Collators: collate.GetBinaryCollatorSlice(1),
			},
			str: "[1,1]",
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Datum{types.NewIntDatum(1)},
				HighVal:     []types.Datum{types.NewIntDatum(1)},
				HighExclude: true,
				Collators:   collate.GetBinaryCollatorSlice(1),
			},
			str: "[1,1)",
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Datum{types.NewIntDatum(1)},
				HighVal:     []types.Datum{types.NewIntDatum(2)},
				LowExclude:  true,
				HighExclude: true,
				Collators:   collate.GetBinaryCollatorSlice(1),
			},
			str: "(1,2)",
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Datum{types.NewFloat64Datum(1.1)},
				HighVal:     []types.Datum{types.NewFloat64Datum(1.9)},
				HighExclude: true,
				Collators:   collate.GetBinaryCollatorSlice(1),
			},
			str: "[1.1,1.9)",
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Datum{types.MinNotNullDatum()},
				HighVal:     []types.Datum{types.NewIntDatum(1)},
				HighExclude: true,
				Collators:   collate.GetBinaryCollatorSlice(1),
			},
			str: "[-inf,1)",
		},
	}
	return result
}

func TestRange(t *testing.T) {
	simpleTests := sampleRanges()
	for _, v := range simpleTests {
		require.Equal(t, v.str, v.ran.String())
	}

	isPointTests := []struct {
		ran     ranger.Range
		isPoint bool
	}{
		{
			ran: ranger.Range{
				LowVal:    []types.Datum{types.NewIntDatum(1)},
				HighVal:   []types.Datum{types.NewIntDatum(1)},
				Collators: collate.GetBinaryCollatorSlice(1),
			},
			isPoint: true,
		},
		{
			ran: ranger.Range{
				LowVal:    []types.Datum{types.NewStringDatum("abc")},
				HighVal:   []types.Datum{types.NewStringDatum("abc")},
				Collators: collate.GetBinaryCollatorSlice(1),
			},
			isPoint: true,
		},
		{
			ran: ranger.Range{
				LowVal:    []types.Datum{types.NewIntDatum(1)},
				HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1)},
				Collators: collate.GetBinaryCollatorSlice(1),
			},
			isPoint: false,
		},
		{
			ran: ranger.Range{
				LowVal:     []types.Datum{types.NewIntDatum(1)},
				HighVal:    []types.Datum{types.NewIntDatum(1)},
				LowExclude: true,
				Collators:  collate.GetBinaryCollatorSlice(1),
			},
			isPoint: false,
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Datum{types.NewIntDatum(1)},
				HighVal:     []types.Datum{types.NewIntDatum(1)},
				HighExclude: true,
				Collators:   collate.GetBinaryCollatorSlice(1),
			},
			isPoint: false,
		},
		{
			ran: ranger.Range{
				LowVal:    []types.Datum{types.NewIntDatum(1)},
				HighVal:   []types.Datum{types.NewIntDatum(2)},
				Collators: collate.GetBinaryCollatorSlice(1),
			},
			isPoint: false,
		},
	}
	for _, v := range isPointTests {
		ctx := core.MockContext()
		require.Equal(t, v.isPoint, v.ran.IsPoint(ctx.GetRangerCtx()))
		domain.GetDomain(ctx).StatsHandle().Close()
	}
}

func TestIsFullRange(t *testing.T) {
	nullDatum := types.MinNotNullDatum()
	nullDatum.SetNull()
	isFullRangeTests := []struct {
		ran               ranger.Range
		unsignedIntHandle bool
		isFullRange       bool
	}{
		{
			ran: ranger.Range{
				LowVal:    []types.Datum{types.NewIntDatum(math.MinInt64)},
				HighVal:   []types.Datum{types.NewIntDatum(math.MaxInt64)},
				Collators: collate.GetBinaryCollatorSlice(1),
			},
			unsignedIntHandle: false,
			isFullRange:       true,
		},
		{
			ran: ranger.Range{
				LowVal:    []types.Datum{types.NewIntDatum(math.MaxInt64)},
				HighVal:   []types.Datum{types.NewIntDatum(math.MinInt64)},
				Collators: collate.GetBinaryCollatorSlice(1),
			},
			unsignedIntHandle: false,
			isFullRange:       false,
		},
		{
			ran: ranger.Range{
				LowVal:    []types.Datum{types.NewIntDatum(1)},
				HighVal:   []types.Datum{types.NewUintDatum(math.MaxUint64)},
				Collators: collate.GetBinaryCollatorSlice(1),
			},
			unsignedIntHandle: false,
			isFullRange:       false,
		},
		{
			ran: ranger.Range{
				LowVal:    []types.Datum{*nullDatum.Clone()},
				HighVal:   []types.Datum{types.NewUintDatum(math.MaxUint64)},
				Collators: collate.GetBinaryCollatorSlice(1),
			},
			unsignedIntHandle: false,
			isFullRange:       true,
		},
		{
			ran: ranger.Range{
				LowVal:    []types.Datum{*nullDatum.Clone()},
				HighVal:   []types.Datum{*nullDatum.Clone()},
				Collators: collate.GetBinaryCollatorSlice(1),
			},
			unsignedIntHandle: false,
			isFullRange:       false,
		},
		{
			ran: ranger.Range{
				LowVal:    []types.Datum{types.MinNotNullDatum()},
				HighVal:   []types.Datum{types.MaxValueDatum()},
				Collators: collate.GetBinaryCollatorSlice(1),
			},
			unsignedIntHandle: false,
			isFullRange:       true,
		},
		{
			ran: ranger.Range{
				LowVal:    []types.Datum{types.NewUintDatum(0)},
				HighVal:   []types.Datum{types.NewUintDatum(math.MaxUint64)},
				Collators: collate.GetBinaryCollatorSlice(1),
			},
			unsignedIntHandle: true,
			isFullRange:       true,
		},
	}
	for _, v := range isFullRangeTests {
		require.Equal(t, v.isFullRange, v.ran.IsFullRange(v.unsignedIntHandle))
	}
}

func TestRangeMemUsage(t *testing.T) {
	r1 := ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(0)},
		HighVal:   []types.Datum{types.NewIntDatum(1)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	mem1 := ranger.EmptyRangeSize + 2*types.EmptyDatumSize + 16
	require.Equal(t, mem1, r1.MemUsage())
	r2 := ranger.Range{
		LowVal:    []types.Datum{types.NewStringDatum("abcde")},
		HighVal:   []types.Datum{types.NewStringDatum("fghij")},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	mem2 := mem1 + int64(cap(r2.LowVal[0].GetBytes())) + int64(len(r2.LowVal[0].Collation())) + int64(cap(r2.HighVal[0].GetBytes())) + int64(len(r2.HighVal[0].Collation()))
	require.Equal(t, mem2, r2.MemUsage())
	ranges := ranger.Ranges{&r1, &r2}
	require.Equal(t, mem1+mem2, ranges.MemUsage())
}

// Test intersections that result in empty sets.
func TestIntersectionEmpty(t *testing.T) {
	expectedResult := make(map[string]string)
	actualResult := make(map[string]string)
	expectedResult["[1,2] [3,4]"] = "<nil>"
	expectedResult["(1,2] (3,4]"] = "<nil>"
	expectedResult["[1,2) [3,4)"] = "<nil>"
	expectedResult["(1,2) (3,4)"] = "<nil>"
	expectedResult["[1 2,1 3] (1 3,1 4]"] = "<nil>"
	expectedResult["[-inf,1] [2,+inf]"] = "<nil>"
	expectedResult["[1 2,1 3] [1 3,1 4]"] = "[1 3,1 3]"
	expectedResult["[1 1 2,1 1 5] (1 2,1 3)"] = "<nil>"

	var range1, range2 ranger.Range
	var intersection1, intersection2 *ranger.Range
	ctx := core.MockContext()

	// All ranges closed. intersection of [1,2] [3,4] is empty
	range1 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1)},
		HighVal:   []types.Datum{types.NewIntDatum(2)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	range2 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(3)},
		HighVal:   []types.Datum{types.NewIntDatum(4)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// Low open.  intersection of (1,2] (3,4] is empty
	range1.LowExclude = true
	range1.HighExclude = false
	range2.LowExclude = true
	range2.HighExclude = false
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// high bound open. intersection of [1,2) [3,4) is empty
	range1.LowExclude = false
	range1.HighExclude = true
	range2.LowExclude = false
	range2.HighExclude = true
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// both open. intersection of (1,2) (3,4) is empty
	range1.LowExclude = true
	range1.HighExclude = true
	range2.LowExclude = true
	range2.HighExclude = true
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)
	// Use of inifinity in ragnes. Intersection of [-infinity, 1] and [2, +infinity]
	range1 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(math.MinInt64)},
		HighVal:   []types.Datum{types.NewIntDatum(1)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	range2 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(2)},
		HighVal:   []types.Datum{types.NewIntDatum(math.MaxInt64)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// Multi column range. Intersection of [1 2,1 3] (1 3,1 4] is empty
	range1 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(2)},
		HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(3)},
		Collators: collate.GetBinaryCollatorSlice(2),
	}
	range2 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(3)},
		HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(4)},
		Collators: collate.GetBinaryCollatorSlice(2),
	}
	range2.LowExclude = true
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	range2.LowExclude = false // negative test for disjoint ranges when high = low.
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// Multi column range with different number of columns. Intersection of [1 1 2,1 1 5] (1 2,1 3) is empty
	range1 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1), types.NewIntDatum(2)},
		HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1), types.NewIntDatum(5)},
		Collators: collate.GetBinaryCollatorSlice(3),
	}
	range2 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(2)},
		HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(3)},
		Collators: collate.GetBinaryCollatorSlice(2),
	}
	range2.LowExclude = true
	range2.HighExclude = true
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// Compare actual vs expected
	for k, v := range actualResult {
		require.Equal(t, v, expectedResult[k])
	}
	// cleanup
	domain.GetDomain(ctx).StatsHandle().Close()
}

// Test intersections where one input is a subset of the other.
func TestIntersectionSubset(t *testing.T) {
	expectedResult := make(map[string]string)
	actualResult := make(map[string]string)
	expectedResult["[1,5] [2,4]"] = "[2,4]"
	expectedResult["(1,5] (2,4]"] = "(2,4]"
	expectedResult["[1,5) [2,4)"] = "[2,4)"
	expectedResult["(1,5) (2,4)"] = "(2,4)"
	expectedResult["[-inf,5] [2,4]"] = "[2,4]"
	expectedResult["[1 2,1 5] (1 3,1 4]"] = "(1 3,1 4]"
	expectedResult["[1 1 -inf,1 1 15] [1 1,1 1]"] = "[1 1 -inf,1 1 15]"

	var range1, range2 ranger.Range
	var intersection1, intersection2 *ranger.Range
	ctx := core.MockContext()

	// All ranges closed. intersection of [1,5] [2,4] is [2,4]
	range1 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1)},
		HighVal:   []types.Datum{types.NewIntDatum(5)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	range2 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(2)},
		HighVal:   []types.Datum{types.NewIntDatum(4)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// Low open.  intersection of (1,5] (2,4] is (2,4]
	range1.LowExclude = true
	range1.HighExclude = false
	range2.LowExclude = true
	range2.HighExclude = false
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// high bound open. intersection of [1,5) [2,4) is [2,4)
	range1.LowExclude = false
	range1.HighExclude = true
	range2.LowExclude = false
	range2.HighExclude = true
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// both open. intersection of (1,5) (2,4) is (2,4)
	range1.LowExclude = true
	range1.HighExclude = true
	range2.LowExclude = true
	range2.HighExclude = true
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)
	// Use of inifinity in ragnes. Intersection of [-infinity, 5] and [2, 4]
	range1 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(math.MinInt64)},
		HighVal:   []types.Datum{types.NewIntDatum(5)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	range2 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(2)},
		HighVal:   []types.Datum{types.NewIntDatum(4)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// Multi column range. Intersection of [1 2,1 5] (1 3,1 4] = (1 3,1 4]
	range1 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(2)},
		HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(5)},
		Collators: collate.GetBinaryCollatorSlice(2),
	}
	range2 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(3)},
		HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(4)},
		Collators: collate.GetBinaryCollatorSlice(2),
	}
	range2.LowExclude = true
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// Multi column range with different number of columns. Intersection of [1 1 -inf,1 1 15] [1 1,1 1] is [1 1 -inf,1 1 15]
	range1 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1), types.NewIntDatum(math.MinInt64)},
		HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1), types.NewIntDatum(15)},
		Collators: collate.GetBinaryCollatorSlice(3),
	}
	range2 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1)},
		HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1)},
		Collators: collate.GetBinaryCollatorSlice(2),
	}
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// Compare actual vs expected
	for k, v := range actualResult {
		require.Equal(t, v, expectedResult[k])
	}
	// cleanup
	domain.GetDomain(ctx).StatsHandle().Close()
}

// Test intersections for overlapping ranges and not subset.
func TestIntersectionOverlap(t *testing.T) {
	expectedResult := make(map[string]string)
	actualResult := make(map[string]string)
	expectedResult["[1,5] [2,7]"] = "[2,5]"
	expectedResult["(1,5] (2,7]"] = "(2,5]"
	expectedResult["[1,5) [2,7)"] = "[2,5)"
	expectedResult["(1,5) (2,7)"] = "(2,5)"
	expectedResult["[-inf,5] [2,14]"] = "[2,5]"
	expectedResult["[1 2,1 5] (1 3,1 4]"] = "(1 3,1 4]"
	expectedResult["[1 1 -inf,1 1 15] [1 1 4,1 1 25]"] = "[1 1 4,1 1 15]"

	var range1, range2 ranger.Range
	var intersection1, intersection2 *ranger.Range
	ctx := core.MockContext()

	// All ranges closed. intersection of [1,5] [2,7] is [2,5]
	range1 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1)},
		HighVal:   []types.Datum{types.NewIntDatum(5)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	range2 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(2)},
		HighVal:   []types.Datum{types.NewIntDatum(7)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// Low open.  intersection of (1,5] (2,7] is (2,5]
	range1.LowExclude = true
	range1.HighExclude = false
	range2.LowExclude = true
	range2.HighExclude = false
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// high bound open. intersection of [1,5) [2,7) is [2,5)
	range1.LowExclude = false
	range1.HighExclude = true
	range2.LowExclude = false
	range2.HighExclude = true
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// both open. intersection of (1,5) (2,7) is (2,5)
	range1.LowExclude = true
	range1.HighExclude = true
	range2.LowExclude = true
	range2.HighExclude = true
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)
	// Use of inifinity in ragnes. Intersection of [-infinity, 5] and [2, 14]
	range1 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(math.MinInt64)},
		HighVal:   []types.Datum{types.NewIntDatum(5)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	range2 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(2)},
		HighVal:   []types.Datum{types.NewIntDatum(14)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// Multi column range. Intersection of [1 2,1 5] (1 3,1 4] = (1 3,1 4]
	range1 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(2)},
		HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(5)},
		Collators: collate.GetBinaryCollatorSlice(2),
	}
	range2 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(3)},
		HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(4)},
		Collators: collate.GetBinaryCollatorSlice(2),
	}
	range2.LowExclude = true
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// Multi column range with different number of columns. Intersection of [1 1 -inf,1 1 15] [1 1 4,1 1 25] is [1 1 4,1 1 15]
	range1 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1), types.NewIntDatum(math.MinInt64)},
		HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1), types.NewIntDatum(15)},
		Collators: collate.GetBinaryCollatorSlice(3),
	}
	range2 = ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1), types.NewIntDatum(4)},
		HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1), types.NewIntDatum(25)},
		Collators: collate.GetBinaryCollatorSlice(3),
	}
	intersection1, _ = range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
	actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	intersection2, _ = range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
	require.Equal(t, intersection1, intersection2)

	// Compare actual vs expected
	for k, v := range actualResult {
		require.Equal(t, expectedResult[k], v)
	}
	// cleanup
	domain.GetDomain(ctx).StatsHandle().Close()
}
