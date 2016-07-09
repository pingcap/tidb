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

package parser

import (
	"runtime"
	"testing"
	"unsafe"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testAllocator{})

type testAllocator struct {
}

func (t *testAllocator) TestSimple(c *C) {
	defer testleak.AfterTest(c)()
	ac := newAllocator()

	// test allocated object is zero-initialized.
	ptr := (*[100]byte)(ac.alloc(100))
	c.Assert(*ptr, Equals, [100]byte{})
	ac.reset()
	ptr = (*[100]byte)(ac.alloc(100))
	c.Assert(*ptr, Equals, [100]byte{})
}

type inCache struct {
	x    byte
	y    float64
	z    int
	next *inCache
	ptr  *inHeap
}

type inHeap struct {
	v int
}

func f(ac *allocator) *inCache {
	size := int(unsafe.Sizeof(inCache{}))
	head := (*inCache)(ac.alloc(size))
	for i := 0; i < 2000; i++ {
		p := (*inCache)(ac.alloc(size))
		p.ptr = (*inHeap)(ac.alloc(16))
		// p.ptr = &inHeap{v: i}
		p.next = head.next
		head.next = p
	}
	return head
}

func testOnceGC(ac *allocator) {
	n := f(ac)
	runtime.GC()
	for p := n.next; p != nil; p = p.next {
		p.ptr.v = '7'
	}
	ac.reset()
}

func (t *testAllocator) TestGCSafe(c *C) {
	ac := newAllocator()
	for i := 0; i < 100; i++ {
		testOnceGC(ac)
	}
}
