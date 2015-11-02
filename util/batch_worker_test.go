// Copyright 2015 PingCAP, Inc.
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

package util

import (
	"sync"

	. "github.com/pingcap/check"
)

var _ = Suite(&testBatchWorkSuite{})

type testBatchWorkSuite struct{}

func (s *testBatchWorkSuite) TestBatchWorker(c *C) {
	total := 1024
	cnt := 0
	worker := NewBatchWorker(10, func(jobs []interface{}) {
		cnt += len(jobs)
	})
	for i := 0; i < total; i++ {
		worker.Submit(i)
	}
	worker.Flush()
	c.Assert(cnt, Equals, 1024)
}

func (s *testBatchWorkSuite) TestConcurrentSubmit(c *C) {
	var wg sync.WaitGroup
	cnt := 0
	worker := NewBatchWorker(8, func(jobs []interface{}) {
		cnt += len(jobs)
	})
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				worker.Submit(j * i)
			}
		}(i)
	}
	wg.Wait()
	worker.Flush()
	c.Assert(cnt, Equals, 100)
}
