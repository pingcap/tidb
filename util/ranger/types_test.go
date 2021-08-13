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

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/stretchr/testify/require"
)

func TestRange(t *testing.T) {
	t.Parallel()
	simpleTests := []struct {
		ran ranger.Range
		str string
	}{
		{
			ran: ranger.Range{
				LowVal:  []types.Datum{types.NewIntDatum(1)},
				HighVal: []types.Datum{types.NewIntDatum(1)},
			},
			str: "[1,1]",
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Datum{types.NewIntDatum(1)},
				HighVal:     []types.Datum{types.NewIntDatum(1)},
				HighExclude: true,
			},
			str: "[1,1)",
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Datum{types.NewIntDatum(1)},
				HighVal:     []types.Datum{types.NewIntDatum(2)},
				LowExclude:  true,
				HighExclude: true,
			},
			str: "(1,2)",
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Datum{types.NewFloat64Datum(1.1)},
				HighVal:     []types.Datum{types.NewFloat64Datum(1.9)},
				HighExclude: true,
			},
			str: "[1.1,1.9)",
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Datum{types.MinNotNullDatum()},
				HighVal:     []types.Datum{types.NewIntDatum(1)},
				HighExclude: true,
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
				LowVal:  []types.Datum{types.NewIntDatum(1)},
				HighVal: []types.Datum{types.NewIntDatum(1)},
			},
			isPoint: true,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Datum{types.NewStringDatum("abc")},
				HighVal: []types.Datum{types.NewStringDatum("abc")},
			},
			isPoint: true,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Datum{types.NewIntDatum(1)},
				HighVal: []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1)},
			},
			isPoint: false,
		},
		{
			ran: ranger.Range{
				LowVal:     []types.Datum{types.NewIntDatum(1)},
				HighVal:    []types.Datum{types.NewIntDatum(1)},
				LowExclude: true,
			},
			isPoint: false,
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Datum{types.NewIntDatum(1)},
				HighVal:     []types.Datum{types.NewIntDatum(1)},
				HighExclude: true,
			},
			isPoint: false,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Datum{types.NewIntDatum(1)},
				HighVal: []types.Datum{types.NewIntDatum(2)},
			},
			isPoint: false,
		},
	}
	sc := new(stmtctx.StatementContext)
	for _, v := range isPointTests {
		require.Equal(t, v.isPoint, v.ran.IsPoint(sc))
	}
}

func TestIsFullRange(t *testing.T) {
	t.Parallel()
	nullDatum := types.MinNotNullDatum()
	nullDatum.SetNull()
	isFullRangeTests := []struct {
		ran         ranger.Range
		isFullRange bool
	}{
		{
			ran: ranger.Range{
				LowVal:  []types.Datum{types.NewIntDatum(math.MinInt64)},
				HighVal: []types.Datum{types.NewIntDatum(math.MaxInt64)},
			},
			isFullRange: true,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Datum{types.NewIntDatum(math.MaxInt64)},
				HighVal: []types.Datum{types.NewIntDatum(math.MinInt64)},
			},
			isFullRange: false,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Datum{types.NewIntDatum(1)},
				HighVal: []types.Datum{types.NewUintDatum(math.MaxUint64)},
			},
			isFullRange: false,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Datum{*nullDatum.Clone()},
				HighVal: []types.Datum{types.NewUintDatum(math.MaxUint64)},
			},
			isFullRange: true,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Datum{*nullDatum.Clone()},
				HighVal: []types.Datum{*nullDatum.Clone()},
			},
			isFullRange: false,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Datum{types.MinNotNullDatum()},
				HighVal: []types.Datum{types.MaxValueDatum()},
			},
			isFullRange: true,
		},
	}
	for _, v := range isFullRangeTests {
		require.Equal(t, v.isFullRange, v.ran.IsFullRange())
	}
}
