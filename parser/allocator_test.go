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
	"unsafe"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testAllocateSuite{})

type testAllocateSuite struct {
}

func (s *testAllocateSuite) TestSimple(c *C) {
	defer testleak.AfterTest(c)()
	ac := newAllocator()
	for i := 0; i < 5000; i++ {
		ac.alloc(100)
	}
	c.Assert(ac.head != ac.tail, IsTrue)

	ac.reset()
	c.Assert(ac.head, Equals, ac.tail)
	c.Assert(ac.tail.next, NotNil)

	ptr1 := ac.alloc(0)
	c.Assert(ptr1, Equals, ac.alloc(0))
	ptr2 := ac.allocInsertStmt()
	c.Assert(uintptr(unsafe.Pointer(ptr2)), Equals, uintptr(ptr1))
}
