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
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testAllocSuite{})

type testAllocSuite struct{}

func (s *testAllocSuite) SetUpSuite(c *C) {}

func (s *testAllocSuite) TestAlloc(c *C) {
	a := newAllocator()
	b, _ := a.Alloc(0)
	c.Assert(len(b), Equals, 0)

	_, t1 := a.Alloc(666)
	_, t2 := a.Alloc(42)
	_, t3 := a.Alloc(2)
	a.GC(t1)
	c.Assert(len(a.blocks), Equals, 1)
	a.GC(t2)
	c.Assert(len(a.blocks), Equals, 1)
	a.GC(t3)
	c.Assert(len(a.blocks), Equals, 1)

	var t, tmp uint64
	for i := 0; i < 100; i++ {
		_, tmp = a.Alloc(rand.Intn(blockSize))
		if i == 50 {
			t = tmp
		}
	}
	blockCount := len(a.blocks)
	a.GC(t)
	c.Assert(len(a.blocks), Less, blockCount)

	a.GC(tmp)
	c.Assert(len(a.blocks), Equals, 1)

	var fail bool
	func() {
		defer func() {
			if recover() != nil {
				fail = true
			}
		}()
		a.Alloc(blockSize)
	}()

	c.Assert(fail, IsTrue)
}

func (s *testAllocSuite) TestAllocKey(c *C) {
	const count = 100000
	srcs := make([][]byte, count)
	dests := make([][]byte, count)
	var xx byte
	for i := 0; i < count; i++ {
		length := 10 + rand.Intn(40)
		src := make([]byte, length)
		for idx, _ := range src {
			src[idx] = xx
			xx++
		}
		dst := allocKey(src)

		srcs[i] = src
		dests[i] = dst
	}
	for i := 0; i < count; i++ {
		c.Assert(bytes.Compare(srcs[i], dests[i]), Equals, 0)
	}
}

func BenchmarkAlloc(b *testing.B) {
	a := newAllocator()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Alloc(7)
	}
	b.ReportAllocs()
}

func BenchmarkStandard(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		standard(7)
	}
	b.ReportAllocs()
}

func standard(n int) []byte {
	return make([]byte, n)
}
