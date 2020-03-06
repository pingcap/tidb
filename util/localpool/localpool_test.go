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

// +build !race

package localpool

import (
	"math/rand"
	"runtime"
	"sync"
	"testing"

	. "github.com/pingcap/check"
)

type Obj struct {
	val int64
}

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testPoolSuite{})

type testPoolSuite struct {
}

func (s *testPoolSuite) TestPool(c *C) {
	numWorkers := runtime.GOMAXPROCS(0)
	wg := new(sync.WaitGroup)
	wg.Add(numWorkers)
	pool := NewLocalPool(16, func() interface{} {
		return new(Obj)
	}, nil)
	n := 1000
	for i := 0; i < numWorkers; i++ {
		go func() {
			for j := 0; j < n; j++ {
				GetAndPut(pool)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	var getHit, getMiss, putHit, putMiss int
	for _, slot := range pool.slots {
		getHit += slot.getHit
		getMiss += slot.getMiss
		putHit += slot.putHit
		putMiss += slot.putMiss
	}
	c.Assert(getHit, Greater, getMiss)
	c.Assert(putHit, Greater, putMiss)
}

func GetAndPut(pool *LocalPool) {
	objs := make([]interface{}, rand.Intn(4)+1)
	for i := 0; i < len(objs); i++ {
		objs[i] = pool.Get()
	}
	runtime.Gosched()
	for _, obj := range objs {
		pool.Put(obj)
	}
}
