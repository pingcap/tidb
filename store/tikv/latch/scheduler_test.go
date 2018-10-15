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

package latch

import (
	"sync"

	. "github.com/pingcap/check"
)

var _ = Suite(&testSchedulerSuite{})

type testSchedulerSuite struct {
}

func (s *testSchedulerSuite) SetUpTest(c *C) {
}

func (s *testSchedulerSuite) TestWithConcurrency(c *C) {
	txns := [][][]byte{
		{[]byte("a"), []byte("b"), []byte("c")},
		{[]byte("a"), []byte("d"), []byte("e"), []byte("f")},
		{[]byte("e"), []byte("f"), []byte("g"), []byte("h")},
	}
	sched := NewScheduler(1024)
	defer sched.Close()

	var wg sync.WaitGroup
	wg.Add(len(txns))
	for _, txn := range txns {
		go func(txn [][]byte, wg *sync.WaitGroup) {
			lock := sched.Lock(getTso(), txn)
			defer sched.UnLock(lock)
			if lock.IsStale() {
				// Should restart the transaction or return error
			} else {
				lock.SetCommitTS(getTso())
				// Do 2pc
			}
			wg.Done()
		}(txn, &wg)
	}
	wg.Wait()
}
