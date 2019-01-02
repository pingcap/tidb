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
	"bytes"
	"math/rand"
	"sync"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testSchedulerSuite{})

type testSchedulerSuite struct {
}

func (s *testSchedulerSuite) SetUpTest(c *C) {
}

func (s *testSchedulerSuite) TestWithConcurrency(c *C) {
	sched := NewScheduler(7)
	defer sched.Close()
	rand.Seed(time.Now().Unix())

	ch := make(chan [][]byte, 100)
	const workerCount = 10
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func(ch <-chan [][]byte, wg *sync.WaitGroup) {
			for txn := range ch {
				lock := sched.Lock(getTso(), txn)
				if lock.IsStale() {
					// Should restart the transaction or return error
				} else {
					lock.SetCommitTS(getTso())
					// Do 2pc
				}
				sched.UnLock(lock)
			}
			wg.Done()
		}(ch, &wg)
	}

	for i := 0; i < 999; i++ {
		ch <- generate()
	}
	close(ch)

	wg.Wait()
}

// generate generates something like:
// {[]byte("a"), []byte("b"), []byte("c")}
// {[]byte("a"), []byte("d"), []byte("e"), []byte("f")}
// {[]byte("e"), []byte("f"), []byte("g"), []byte("h")}
// The data should not repeat in the sequence.
func generate() [][]byte {
	table := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}
	ret := make([][]byte, 0, 5)
	chance := []int{100, 60, 40, 20}
	for i := 0; i < len(chance); i++ {
		needMore := rand.Intn(100) < chance[i]
		if needMore {
			randBytes := []byte{table[rand.Intn(len(table))]}
			if !contains(randBytes, ret) {
				ret = append(ret, randBytes)
			}
		}
	}
	return ret
}

func contains(x []byte, set [][]byte) bool {
	for _, y := range set {
		if bytes.Compare(x, y) == 0 {
			return true
		}
	}
	return false
}
