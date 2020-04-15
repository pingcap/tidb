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
// See the License for the specific language governing permissions and
// limitations under the License.

package ranger_test

import (
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/sessionctx/stmtctx"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/ranger"
)

var _ = Suite(&testRangeSuite{})

type testRangeSuite struct {
}

func (s *testRangeSuite) TestRange(c *C) {
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
	for _, t := range simpleTests {
		c.Assert(t.ran.String(), Equals, t.str)
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
	for _, t := range isPointTests {
		c.Assert(t.ran.IsPoint(sc), Equals, t.isPoint)
	}
}

func (s *testRangeSuite) TestIsFullRange(c *C) {
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
			isFullRange: true,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Datum{types.MinNotNullDatum()},
				HighVal: []types.Datum{types.MaxValueDatum()},
			},
			isFullRange: true,
		},
	}
	for _, t := range isFullRangeTests {
		c.Assert(t.ran.IsFullRange(), Equals, t.isFullRange)
	}
}
