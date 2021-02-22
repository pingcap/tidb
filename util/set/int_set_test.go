// Copyright 2019 PingCAP, Inc.
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

package set

import (
	"fmt"
	"testing"

	"github.com/pingcap/check"
)

var _ = check.Suite(&intSetTestSuite{})

type intSetTestSuite struct{}

func (s *intSetTestSuite) TestIntSet(c *check.C) {
	set := NewIntSet()
	vals := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for i := range vals {
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
	}
	c.Assert(set.Count(), check.Equals, len(vals))

	c.Assert(len(set), check.Equals, len(vals))
	for i := range vals {
		c.Assert(set.Exist(vals[i]), check.IsTrue)
	}

	c.Assert(set.Exist(11), check.IsFalse)
}

func (s *intSetTestSuite) TestInt64Set(c *check.C) {
	set := NewInt64Set()
	vals := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for i := range vals {
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
	}

	c.Assert(len(set), check.Equals, len(vals))
	for i := range vals {
		c.Assert(set.Exist(vals[i]), check.IsTrue)
	}

	c.Assert(set.Exist(11), check.IsFalse)

	set = NewInt64Set(1, 2, 3, 4, 5, 6)
	for i := 1; i < 7; i++ {
		c.Assert(set.Exist(int64(i)), check.IsTrue)
	}
	c.Assert(set.Exist(7), check.IsFalse)
}

func BenchmarkInt64SetMemoryUsage(b *testing.B) {
	b.ReportAllocs()
	type testCase struct {
		rowNum    int
		expectedB int
	}
	cases := []testCase{
		{
			rowNum:    0,
			expectedB: 0,
		},
		{
			rowNum:    100,
			expectedB: 4,
		},
		{
			rowNum:    10000,
			expectedB: 11,
		},
		{
			rowNum:    1000000,
			expectedB: 18,
		},
		{
			rowNum:    851968, // 6.5 * (1 << 17)
			expectedB: 17,
		},
		{
			rowNum:    851969, // 6.5 * (1 << 17) + 1
			expectedB: 18,
		},
		{
			rowNum:    425984, // 6.5 * (1 << 16)
			expectedB: 16,
		},
		{
			rowNum:    425985, // 6.5 * (1 << 16) + 1
			expectedB: 17,
		},
	}

	for _, c := range cases {
		b.Run(fmt.Sprintf("MapRows %v", c.rowNum), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				int64Set, _ := NewInt64SetWithMemoryUsage()
				for num := 0; num < c.rowNum; num++ {
					int64Set.Insert(int64(num))
				}
			}
		})
	}
}
