// Copyright 2020 PingCAP, Inc.
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

package executor

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func (s *pkgTestSuite) TestBatchRetrieverHelper(c *C) {
	rangeStarts := make([]int, 0)
	rangeEnds := make([]int, 0)
	collect := func(start, end int) error {
		rangeStarts = append(rangeStarts, start)
		rangeEnds = append(rangeEnds, end)
		return nil
	}

	r := &batchRetrieverHelper{}
	err := r.nextBatch(collect)
	c.Assert(err, IsNil)
	c.Assert(rangeStarts, DeepEquals, []int{})
	c.Assert(rangeEnds, DeepEquals, []int{})

	r = &batchRetrieverHelper{
		retrieved: true,
		batchSize: 3,
		totalRows: 10,
	}
	err = r.nextBatch(collect)
	c.Assert(err, IsNil)
	c.Assert(rangeStarts, DeepEquals, []int{})
	c.Assert(rangeEnds, DeepEquals, []int{})

	r = &batchRetrieverHelper{
		batchSize: 3,
		totalRows: 10,
	}
	err = r.nextBatch(func(start, end int) error {
		return errors.New("some error")
	})
	c.Assert(err, NotNil)
	c.Assert(r.retrieved, IsTrue)

	r = &batchRetrieverHelper{
		batchSize: 3,
		totalRows: 10,
	}
	for !r.retrieved {
		err = r.nextBatch(collect)
		c.Assert(err, IsNil)
	}
	c.Assert(rangeStarts, DeepEquals, []int{0, 3, 6, 9})
	c.Assert(rangeEnds, DeepEquals, []int{3, 6, 9, 10})
	rangeStarts = rangeStarts[:0]
	rangeEnds = rangeEnds[:0]

	r = &batchRetrieverHelper{
		batchSize: 3,
		totalRows: 9,
	}
	for !r.retrieved {
		err = r.nextBatch(collect)
		c.Assert(err, IsNil)
	}
	c.Assert(rangeStarts, DeepEquals, []int{0, 3, 6})
	c.Assert(rangeEnds, DeepEquals, []int{3, 6, 9})
	rangeStarts = rangeStarts[:0]
	rangeEnds = rangeEnds[:0]

	r = &batchRetrieverHelper{
		batchSize: 100,
		totalRows: 10,
	}
	for !r.retrieved {
		err = r.nextBatch(collect)
		c.Assert(err, IsNil)
	}
	c.Assert(rangeStarts, DeepEquals, []int{0})
	c.Assert(rangeEnds, DeepEquals, []int{10})
}
