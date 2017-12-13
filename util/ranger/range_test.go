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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
)

var _ = Suite(&testRangeSuite{})

type testRangeSuite struct {
}

func (s *testRangeSuite) TestRange(c *C) {
	simpleTests := []struct {
		ran ranger.IndexRange
		str string
	}{
		{
			ran: ranger.IndexRange{
				LowVal:  []types.Datum{types.NewIntDatum(1)},
				HighVal: []types.Datum{types.NewIntDatum(1)},
			},
			str: "[1,1]",
		},
		{
			ran: ranger.IndexRange{
				LowVal:      []types.Datum{types.NewIntDatum(1)},
				HighVal:     []types.Datum{types.NewIntDatum(1)},
				HighExclude: true,
			},
			str: "[1,1)",
		},
		{
			ran: ranger.IndexRange{
				LowVal:      []types.Datum{types.NewIntDatum(1)},
				HighVal:     []types.Datum{types.NewIntDatum(2)},
				LowExclude:  true,
				HighExclude: true,
			},
			str: "(1,2)",
		},
		{
			ran: ranger.IndexRange{
				LowVal:      []types.Datum{types.NewFloat64Datum(1.1)},
				HighVal:     []types.Datum{types.NewFloat64Datum(1.9)},
				HighExclude: true,
			},
			str: "[1.1,1.9)",
		},
		{
			ran: ranger.IndexRange{
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
		ran     ranger.IndexRange
		isPoint bool
	}{
		{
			ran: ranger.IndexRange{
				LowVal:  []types.Datum{types.NewIntDatum(1)},
				HighVal: []types.Datum{types.NewIntDatum(1)},
			},
			isPoint: true,
		},
		{
			ran: ranger.IndexRange{
				LowVal:  []types.Datum{types.NewStringDatum("abc")},
				HighVal: []types.Datum{types.NewStringDatum("abc")},
			},
			isPoint: true,
		},
		{
			ran: ranger.IndexRange{
				LowVal:  []types.Datum{types.NewIntDatum(1)},
				HighVal: []types.Datum{types.NewIntDatum(1), types.NewIntDatum(1)},
			},
			isPoint: false,
		},
		{
			ran: ranger.IndexRange{
				LowVal:     []types.Datum{types.NewIntDatum(1)},
				HighVal:    []types.Datum{types.NewIntDatum(1)},
				LowExclude: true,
			},
			isPoint: false,
		},
		{
			ran: ranger.IndexRange{
				LowVal:      []types.Datum{types.NewIntDatum(1)},
				HighVal:     []types.Datum{types.NewIntDatum(1)},
				HighExclude: true,
			},
			isPoint: false,
		},
		{
			ran: ranger.IndexRange{
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

func (s *testRangeSuite) TestIntColumnRangeString(c *C) {
	tests := []struct {
		ran ranger.IntColumnRange
		ans string
	}{
		{
			ran: ranger.IntColumnRange{
				LowVal:  math.MinInt64,
				HighVal: 2,
			},
			ans: "(-inf,2]",
		},
		{
			ran: ranger.IntColumnRange{
				LowVal:  3,
				HighVal: math.MaxInt64,
			},
			ans: "[3,+inf)",
		},
	}
	for _, t := range tests {
		c.Assert(t.ran.String(), Equals, t.ans)
	}
}

func (s *testRangeSuite) TestColumnRangeString(c *C) {
	tests := []struct {
		ran ranger.ColumnRange
		ans string
	}{
		{
			ran: ranger.ColumnRange{
				Low:      types.NewStringDatum("a"),
				High:     types.MaxValueDatum(),
				HighExcl: true,
			},
			ans: "[a,+inf)",
		},
		{
			ran: ranger.ColumnRange{
				Low:     types.NewFloat64Datum(3.2),
				LowExcl: true,
				High:    types.NewFloat64Datum(6.4),
			},
			ans: "(3.2,6.4]",
		},
		{
			ran: ranger.ColumnRange{
				Low:  types.Datum{},
				High: types.Datum{},
			},
			ans: "[<nil>,<nil>]",
		},
	}
	for _, t := range tests {
		c.Assert(t.ran.String(), Equals, t.ans)
	}
}
