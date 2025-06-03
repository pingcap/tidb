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
	"strings"
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
				LowVal:    []types.Datum{types.MinNotNullDatum()},
				HighVal:   []types.Datum{types.MaxValueDatum()},
				Collators: collate.GetBinaryCollatorSlice(1),
			},
			unsignedIntHandle: false,
			isFullRange:       true,
		},
		{
			ran: ranger.Range{
				LowVal:    []types.Datum{types.MaxValueDatum()},
				HighVal:   []types.Datum{types.MinNotNullDatum()},
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

// Build an integer range value with input: lowVal, highVal, open or closed for lower/upper limit.
func buildRange(lowVals []int64, highVals []int64, lowExclude bool, highExclude bool) ranger.Range {
	datums := func(ints []int64) []types.Datum {
		ds := make([]types.Datum, len(ints))
		for i, val := range ints {
			if val == math.MinInt64 {
				ds[i] = types.MinNotNullDatum()
			} else if val == math.MaxInt64 {
				ds[i] = types.MaxValueDatum()
			} else {
				ds[i] = types.NewIntDatum(val)
			}
		}
		return ds
	}

	return ranger.Range{
		LowVal:      datums(lowVals),
		HighVal:     datums(highVals),
		LowExclude:  lowExclude,
		HighExclude: highExclude,
		Collators:   collate.GetBinaryCollatorSlice(len(lowVals)),
	}
}

func TestIntersectionList(t *testing.T) {
	ctx := core.MockContext()
	defer domain.GetDomain(ctx).StatsHandle().Close()

	// First list of ranges representing: (a > 100) OR (a = 100 AND b > 0)
	r1 := buildRange([]int64{100, 0}, []int64{100, math.MaxInt64}, true, false) // (100 0, 100 +inf]
	r2 := buildRange([]int64{100}, []int64{math.MaxInt64}, true, false)         // (100, +inf]
	rangeList1 := ranger.Ranges{&r1, &r2}

	// Second list of ranges representing: (a < 101) OR (a = 101 AND b < 10)
	r3 := buildRange([]int64{math.MinInt64}, []int64{101}, false, true)          // [-inf, 101)
	r4 := buildRange([]int64{101, math.MinInt64}, []int64{101, 10}, false, true) // [101 -inf, 101 10)
	rangeList2 := ranger.Ranges{&r3, &r4}

	// Intersect the two range lists
	intersected := rangeList1.IntersectRanges(ctx.GetRangerCtx().TypeCtx, rangeList2)

	// Convert result to string representation
	actual := joinRanges(intersected)
	expected := "(100 0,100 +inf],(100,101),[101 -inf,101 10)"
	require.Equal(t, expected, actual)
}

// Helper to join a list of ranges into a single string representation.
func joinRanges(ranges []*ranger.Range) string {
	var sb strings.Builder
	for i, r := range ranges {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(rangeToString(r))
	}
	return sb.String()
}

// Test intersections that result in empty sets.
func TestIntersectionEmpty(t *testing.T) {
	type testCase struct {
		start1, end1 []int64
		exclLow1     bool
		exclHigh1    bool
		start2, end2 []int64
		exclLow2     bool
		exclHigh2    bool
		expected     string
	}

	cases := []testCase{
		{[]int64{1}, []int64{2}, false, false, []int64{3}, []int64{4}, false, false, "<nil>"},
		{[]int64{1}, []int64{2}, true, false, []int64{3}, []int64{4}, true, false, "<nil>"},
		{[]int64{1}, []int64{2}, false, true, []int64{3}, []int64{4}, false, true, "<nil>"},
		{[]int64{1}, []int64{2}, true, true, []int64{3}, []int64{4}, true, true, "<nil>"},
		{[]int64{1, 2}, []int64{1, 3}, false, false, []int64{1, 3}, []int64{1, 4}, true, false, "<nil>"},
		{[]int64{math.MinInt64}, []int64{1}, false, false, []int64{2}, []int64{math.MaxInt64}, false, false, "<nil>"},
		{[]int64{1, 2}, []int64{1, 3}, false, false, []int64{1, 3}, []int64{1, 4}, false, false, "[1 3,1 3]"},
		{[]int64{1, 1, 2}, []int64{1, 1, 5}, false, false, []int64{1, 2}, []int64{1, 3}, true, true, "<nil>"},
		{[]int64{100, 0}, []int64{100, math.MaxInt64}, true, false, []int64{math.MinInt64, math.MinInt64}, []int64{100, math.MinInt64}, false, false, "<nil>"},
		{[]int64{100, 0}, []int64{100, math.MaxInt64}, true, false, []int64{math.MinInt64}, []int64{100}, false, true, "<nil>"},
		{[]int64{5}, []int64{5}, false, false, []int64{5}, []int64{math.MaxInt64}, true, false, "<nil>"},
		{[]int64{1}, []int64{1}, false, false, []int64{5}, []int64{math.MaxInt64}, true, false, "<nil>"},
		{[]int64{5}, []int64{5}, false, false, []int64{5, 1}, []int64{5, math.MaxInt64}, true, false, "<nil>"},
		{[]int64{1}, []int64{1}, false, false, []int64{5, 1}, []int64{5, math.MaxInt64}, true, false, "<nil>"},
	}

	expectedResult := map[string]string{
		"[1,2] [3,4]":                           "<nil>",
		"(1,2] (3,4]":                           "<nil>",
		"[1,2) [3,4)":                           "<nil>",
		"(1,2) (3,4)":                           "<nil>",
		"[1 2,1 3] (1 3,1 4]":                   "<nil>",
		"[-inf,1] [2,+inf]":                     "<nil>",
		"[1 2,1 3] [1 3,1 4]":                   "[1 3,1 3]",
		"[1 1 2,1 1 5] (1 2,1 3)":               "<nil>",
		"(100 0,100 +inf] [-inf -inf,100 -inf]": "<nil>",
		"(100 0,100 +inf] [-inf,100)":           "<nil>",
		"[5,5] (5,+inf]":                        "<nil>",
		"[1,1] (5,+inf]":                        "<nil>",
		"[5,5] (5 1,5 +inf]":                    "(5 1,5 +inf]",
		"[1,1] (5 1,5 +inf]":                    "<nil>",
	}

	ctx := core.MockContext()
	actualResult := make(map[string]string)

	for _, c := range cases {
		range1 := buildRange(c.start1, c.end1, c.exclLow1, c.exclHigh1)
		range2 := buildRange(c.start2, c.end2, c.exclLow2, c.exclHigh2)
		intersection1, _ := range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
		intersection2, _ := range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)
		require.Equal(t, intersection1, intersection2)

		key := range1.String() + " " + range2.String()
		value := rangeToString(intersection1)

		actualResult[key] = value
	}

	for k, v := range actualResult {
		require.Equal(t, v, expectedResult[k])
	}

	domain.GetDomain(ctx).StatsHandle().Close()
}

// Test intersections where one input is a subset of the other.
func TestIntersectionSubset(t *testing.T) {
	expectedResult := map[string]string{
		"[1,5] [2,4]":                 "[2,4]",
		"(1,5] (2,4]":                 "(2,4]",
		"[1,5) [2,4)":                 "[2,4)",
		"(1,5) (2,4)":                 "(2,4)",
		"[-inf,5] [2,4]":              "[2,4]",
		"[1 2,1 5] (1 3,1 4]":         "(1 3,1 4]",
		"[1 1 -inf,1 1 15] [1 1,1 1]": "[1 1 -inf,1 1 15]",
	}

	type testCase struct {
		low1, high1     []int64
		lowEx1, highEx1 bool
		coll1           int
		low2, high2     []int64
		lowEx2, highEx2 bool
		coll2           int
	}

	cases := []testCase{
		{[]int64{1}, []int64{5}, false, false, 1, []int64{2}, []int64{4}, false, false, 1},
		{[]int64{1}, []int64{5}, true, false, 1, []int64{2}, []int64{4}, true, false, 1},
		{[]int64{1}, []int64{5}, false, true, 1, []int64{2}, []int64{4}, false, true, 1},
		{[]int64{1}, []int64{5}, true, true, 1, []int64{2}, []int64{4}, true, true, 1},
		{[]int64{math.MinInt64}, []int64{5}, false, false, 1, []int64{2}, []int64{4}, false, false, 1},
		{[]int64{1, 2}, []int64{1, 5}, false, false, 2, []int64{1, 3}, []int64{1, 4}, true, false, 2},
		{[]int64{1, 1, math.MinInt64}, []int64{1, 1, 15}, false, false, 3, []int64{1, 1}, []int64{1, 1}, false, false, 2},
	}

	actualResult := make(map[string]string)
	ctx := core.MockContext()

	for _, c := range cases {
		range1 := buildRange(c.low1, c.high1, c.lowEx1, c.highEx1)
		range1.Collators = collate.GetBinaryCollatorSlice(c.coll1)

		range2 := buildRange(c.low2, c.high2, c.lowEx2, c.highEx2)
		range2.Collators = collate.GetBinaryCollatorSlice(c.coll2)

		intersection1, _ := range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range2)
		intersection2, _ := range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &range1)

		require.Equal(t, intersection1, intersection2)

		actualResult[range1.String()+" "+range2.String()] = rangeToString(intersection1)
	}

	for k, v := range actualResult {
		require.Equal(t, v, expectedResult[k])
	}

	domain.GetDomain(ctx).StatsHandle().Close()
}

// Test intersections for overlapping ranges and not subset.
func TestIntersectionOverlap(t *testing.T) {
	ctx := core.MockContext()
	type testCase struct {
		range1         ranger.Range
		range2         ranger.Range
		expectedString string
	}

	col := collate.GetBinaryCollatorSlice(1)
	col2 := collate.GetBinaryCollatorSlice(2)
	col3 := collate.GetBinaryCollatorSlice(3)
	negInfinity := types.MinNotNullDatum()

	tests := []testCase{
		{
			ranger.Range{
				LowVal:    []types.Datum{types.NewIntDatum(1)},
				HighVal:   []types.Datum{types.NewIntDatum(5)},
				Collators: col,
			},
			ranger.Range{
				LowVal:    []types.Datum{types.NewIntDatum(2)},
				HighVal:   []types.Datum{types.NewIntDatum(7)},
				Collators: col,
			},
			"[2,5]",
		},
		{
			ranger.Range{
				LowVal:      []types.Datum{types.NewIntDatum(1)},
				HighVal:     []types.Datum{types.NewIntDatum(5)},
				LowExclude:  true,
				HighExclude: false,
				Collators:   col,
			},
			ranger.Range{
				LowVal:      []types.Datum{types.NewIntDatum(2)},
				HighVal:     []types.Datum{types.NewIntDatum(7)},
				LowExclude:  true,
				HighExclude: false,
				Collators:   col,
			},
			"(2,5]",
		},
		{
			ranger.Range{
				LowVal:      []types.Datum{types.NewIntDatum(1)},
				HighVal:     []types.Datum{types.NewIntDatum(5)},
				LowExclude:  false,
				HighExclude: true,
				Collators:   col,
			},
			ranger.Range{
				LowVal:      []types.Datum{types.NewIntDatum(2)},
				HighVal:     []types.Datum{types.NewIntDatum(7)},
				LowExclude:  false,
				HighExclude: true,
				Collators:   col,
			},
			"[2,5)",
		},
		{
			ranger.Range{
				LowVal:      []types.Datum{types.NewIntDatum(1)},
				HighVal:     []types.Datum{types.NewIntDatum(5)},
				LowExclude:  true,
				HighExclude: true,
				Collators:   col,
			},
			ranger.Range{
				LowVal:      []types.Datum{types.NewIntDatum(2)},
				HighVal:     []types.Datum{types.NewIntDatum(7)},
				LowExclude:  true,
				HighExclude: true,
				Collators:   col,
			},
			"(2,5)",
		},
		{
			ranger.Range{
				LowVal:    []types.Datum{negInfinity},
				HighVal:   []types.Datum{types.NewIntDatum(5)},
				Collators: col,
			},
			ranger.Range{
				LowVal:    []types.Datum{types.NewIntDatum(2)},
				HighVal:   []types.Datum{types.NewIntDatum(14)},
				Collators: col,
			},
			"[2,5]",
		},
		{
			ranger.Range{
				LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(2)},
				HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(5)},
				Collators: col2,
			},
			ranger.Range{
				LowVal:     []types.Datum{types.NewIntDatum(1), types.NewIntDatum(3)},
				HighVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(4)},
				LowExclude: true,
				Collators:  col2,
			},
			"(1 3,1 4]",
		},
		{
			ranger.Range{
				LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1), negInfinity},
				HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1), types.NewIntDatum(15)},
				Collators: col3,
			},
			ranger.Range{
				LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1), types.NewIntDatum(4)},
				HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1), types.NewIntDatum(25)},
				Collators: col3,
			},
			"[1 1 4,1 1 15]",
		},
	}

	for _, tt := range tests {
		intersection1, _ := tt.range1.IntersectRange(ctx.GetRangerCtx().TypeCtx, &tt.range2)
		intersection2, _ := tt.range2.IntersectRange(ctx.GetRangerCtx().TypeCtx, &tt.range1)

		label := tt.range1.String() + " " + tt.range2.String()
		actual := rangeToString(intersection1)
		require.Equal(t, tt.expectedString, actual, "unexpected intersection for %s", label)
		require.Equal(t, intersection1, intersection2, "intersection not symmetric for %s", label)
	}

	domain.GetDomain(ctx).StatsHandle().Close()
}
