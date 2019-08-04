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
	c.Parallel()
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

func (s *testTypeHelperSuite) TestTruncate(c *C) {
	c.Parallel()
	tests := []struct {
		f        float64
		dec      int
		expected float64
	}{
		{123.45, 0, 123},
		{123.45, 1, 123.4},
		{123.45, 2, 123.45},
		{123.45, 3, 123.450},
	}
	for _, tt := range tests {
		res := Truncate(tt.f, tt.dec)
		c.Assert(res, Equals, tt.expected)
	}
}
