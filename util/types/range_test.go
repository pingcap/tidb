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

package types

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/variable"
)

var _ = Suite(&testRangeSuite{})

type testRangeSuite struct {
}

func (s *testRangeSuite) TestRange(c *C) {
	alignedTests := []struct {
		ran IndexRange
		str string
	}{
		{
			ran: IndexRange{
				LowVal:  []Datum{NewIntDatum(1)},
				HighVal: []Datum{NewIntDatum(1)},
			},
			str: "[1 <nil>,1 +inf]",
		},
		{
			ran: IndexRange{
				LowVal:      []Datum{NewIntDatum(1)},
				HighVal:     []Datum{NewIntDatum(1)},
				HighExclude: true,
			},
			str: "[1 <nil>,1 <nil>)",
		},
		{
			ran: IndexRange{
				LowVal:      []Datum{NewIntDatum(1)},
				HighVal:     []Datum{NewIntDatum(2)},
				LowExclude:  true,
				HighExclude: true,
			},
			str: "(1 +inf,2 <nil>)",
		},
		{
			ran: IndexRange{
				LowVal:      []Datum{NewFloat64Datum(1.1)},
				HighVal:     []Datum{NewFloat64Datum(1.9)},
				HighExclude: true,
			},
			str: "[1.1 <nil>,1.9 <nil>)",
		},
		{
			ran: IndexRange{
				LowVal:      []Datum{MinNotNullDatum()},
				HighVal:     []Datum{NewIntDatum(1)},
				HighExclude: true,
			},
			str: "[-inf <nil>,1 <nil>)",
		},
	}
	for _, t := range alignedTests {
		t.ran.Align(2)
		c.Assert(t.ran.String(), Equals, t.str)
	}

	isPointTests := []struct {
		ran     IndexRange
		isPoint bool
	}{
		{
			ran: IndexRange{
				LowVal:  []Datum{NewIntDatum(1)},
				HighVal: []Datum{NewIntDatum(1)},
			},
			isPoint: true,
		},
		{
			ran: IndexRange{
				LowVal:  []Datum{NewStringDatum("abc")},
				HighVal: []Datum{NewStringDatum("abc")},
			},
			isPoint: true,
		},
		{
			ran: IndexRange{
				LowVal:  []Datum{NewIntDatum(1)},
				HighVal: []Datum{NewIntDatum(1), NewIntDatum(1)},
			},
			isPoint: false,
		},
		{
			ran: IndexRange{
				LowVal:     []Datum{NewIntDatum(1)},
				HighVal:    []Datum{NewIntDatum(1)},
				LowExclude: true,
			},
			isPoint: false,
		},
		{
			ran: IndexRange{
				LowVal:      []Datum{NewIntDatum(1)},
				HighVal:     []Datum{NewIntDatum(1)},
				HighExclude: true,
			},
			isPoint: false,
		},
		{
			ran: IndexRange{
				LowVal:  []Datum{NewIntDatum(1)},
				HighVal: []Datum{NewIntDatum(2)},
			},
			isPoint: false,
		},
	}
	sc := new(variable.StatementContext)
	for _, t := range isPointTests {
		c.Assert(t.ran.IsPoint(sc), Equals, t.isPoint)
	}
}
