// Copyright 2021 PingCAP, Inc.
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

package membuf

import (
	"crypto/rand"
	"testing"

	. "github.com/pingcap/check"
)

func init() {
	allocBufLen = 1024
}

type bufferSuite struct{}

var _ = Suite(&bufferSuite{})

func Test(t *testing.T) {
	TestingT(t)
}

type testAllocator struct {
	allocs int
	frees  int
}

func (t *testAllocator) Alloc(n int) []byte {
	t.allocs++
	return make([]byte, n)
}

func (t *testAllocator) Free(_ []byte) {
	t.frees++
}

func (*bufferSuite) TestBufferPool(c *C) {
	allocator := &testAllocator{}
	pool := NewPool(2, allocator)

	bytesBuf := pool.NewBuffer()
	bytesBuf.AllocBytes(256)
	c.Assert(allocator.allocs, Equals, 1)
	bytesBuf.AllocBytes(512)
	c.Assert(allocator.allocs, Equals, 1)
	bytesBuf.AllocBytes(257)
	c.Assert(allocator.allocs, Equals, 2)
	bytesBuf.AllocBytes(767)
	c.Assert(allocator.allocs, Equals, 2)

	c.Assert(allocator.frees, Equals, 0)
	bytesBuf.Destroy()
	c.Assert(allocator.frees, Equals, 0)

	bytesBuf = pool.NewBuffer()
	for i := 0; i < 6; i++ {
		bytesBuf.AllocBytes(512)
	}
	bytesBuf.Destroy()
	c.Assert(allocator.allocs, Equals, 3)
	c.Assert(allocator.frees, Equals, 1)
}

func (*bufferSuite) TestBufferIsolation(c *C) {
	bytesBuf := NewBuffer()
	defer bytesBuf.Destroy()

	b1 := bytesBuf.AllocBytes(16)
	b2 := bytesBuf.AllocBytes(16)
	c.Assert(cap(b1), Equals, len(b1))
	c.Assert(cap(b2), Equals, len(b2))

	_, err := rand.Read(b2)
	c.Assert(err, IsNil)
	b3 := append([]byte(nil), b2...)
	b1 = append(b1, 0, 1, 2, 3)
	c.Assert(b2, DeepEquals, b3)
}
