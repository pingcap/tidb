// Copyright 2018 PingCAP, Inc.
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
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

var _ = Suite(&testTypeHelperSuite{})

type testTypeHelperSuite struct {
}

func (s *testTypeHelperSuite) TestStrToInt(c *C) {
	tests := []struct {
		input  string
		output string
		err    error
	}{
		{"9223372036854775806", "9223372036854775806", nil},
		{"9223372036854775807", "9223372036854775807", nil},
		{"9223372036854775808", "9223372036854775807", ErrBadNumber},
		{"-9223372036854775807", "-9223372036854775807", nil},
		{"-9223372036854775808", "-9223372036854775808", nil},
		{"-9223372036854775809", "-9223372036854775808", ErrBadNumber},
	}
	for _, tt := range tests {
		output, err := strToInt(tt.input)
		c.Assert(errors.Cause(err), Equals, tt.err)
		c.Check(strconv.FormatInt(output, 10), Equals, tt.output)
	}
}

func (s *testTypeHelperSuite) TestCalculateMergeFloat64(c *C) {
	tests := []struct {
		partialCount    int64
		mergeCount      int64
		partialSum      float64
		mergeSum        float64
		partialVariance float64
		mergeVariance   float64
		variance        float64
		sum             float64
		err             error
	}{
		{4, 2, 12, 6, 10.1, 0, 10.1, 18, nil},
		{3, 9, 22.2, 32.5, 19.89, 21.49, 73.68027777777777, 54.7, nil},
	}
	for _, tt := range tests {
		variance, sum, err := CalculateMergeFloat64(tt.partialCount, tt.mergeCount, tt.partialSum,
			tt.mergeSum, tt.partialVariance, tt.mergeVariance)
		c.Assert(err, Equals, tt.err)
		c.Assert(variance, Equals, tt.variance)
		c.Assert(sum, Equals, tt.sum)
	}
}

func (s *testTypeHelperSuite) TestCalculateMergeDecimal(c *C) {
	tests := []struct {
		partialCount    int64
		mergeCount      int64
		partialSum      float64
		mergeSum        float64
		partialVariance float64
		mergeVariance   float64
		variance        string
		sum             string
		err             error
	}{
		{4, 2, 12, 6, 10.1, 0, "10.10000000", "18", nil},
		{3, 9, 22.2, 32.5, 19.89, 21.49, "73.680277839347222252", "54.7", nil},
	}
	for _, tt := range tests {
		variance, sum, err := CalculateMergeDecimal(tt.partialCount, tt.mergeCount,
			NewDecFromFloatForTest(tt.partialSum), NewDecFromFloatForTest(tt.mergeSum),
			NewDecFromFloatForTest(tt.partialVariance), NewDecFromFloatForTest(tt.mergeVariance))
		c.Assert(err, Equals, tt.err)
		c.Assert(variance.String(), Equals, tt.variance)
		c.Assert(sum.String(), Equals, tt.sum)
	}
}
