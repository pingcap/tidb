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

func TestRange(t *testing.T) {
	simpleTests := []struct {
		ran ranger.Range
		str string
	}{
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
