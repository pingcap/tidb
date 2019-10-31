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

package bitmap

import (
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testBitmap{})

type testBitmap struct{}

func (s *testBitmap) TestConcurrentBitmapSet(c *C) {
	const loopCount = 1000
	const interval = 2

	bm := NewConcurrentBitmap(loopCount * interval)
	wg := &sync.WaitGroup{}
	for i := 0; i < loopCount; i++ {
		wg.Add(1)
		go func(bitIndex int) {
			bm.Set(bitIndex)
			wg.Done()
		}(i * interval)
	}
	wg.Wait()

	for i := 0; i < loopCount; i++ {
		if i%interval == 0 {
			c.Assert(bm.UnsafeIsSet(i), IsTrue)
		} else {
			c.Assert(bm.UnsafeIsSet(i), IsFalse)
		}
	}
}

// TestConcurrentBitmapUniqueSetter checks if isSetter is unique everytime
// when a bit is set.
func (s *testBitmap) TestConcurrentBitmapUniqueSetter(c *C) {
	const loopCount = 10000
	const competitorsPerSet = 50

	wg := &sync.WaitGroup{}
	bm := NewConcurrentBitmap(32)
	var setterCounter uint64
	var clearCounter uint64
	// Concurrently set bit, and check if isSetter count matchs zero clearing count.
	for i := 0; i < loopCount; i++ {
		// Clear bitmap to zero.
		if atomic.CompareAndSwapUint32(&(bm.segments[0]), 0x00000001, 0x00000000) {
			atomic.AddUint64(&clearCounter, 1)
		}
		// Concurrently set.
		for j := 0; j < competitorsPerSet; j++ {
			wg.Add(1)
			go func() {
				if bm.Set(31) {
					atomic.AddUint64(&setterCounter, 1)
				}
				wg.Done()
			}()
		}
	}
	wg.Wait()
	// If clearCounter is too big, it means setter concurrency of this test is not enough.
	c.Assert(clearCounter < loopCount, Equals, true)
	c.Assert(setterCounter, Equals, clearCounter+1)
}
