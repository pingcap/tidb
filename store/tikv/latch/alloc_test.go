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
	"math/rand"

	. "github.com/pingcap/check"
)

var _ = Suite(&testNodeAlloc{})

type testNodeAlloc struct{}

func (_ *testNodeAlloc) SetUpTest(c *C) {}

func (_ *testNodeAlloc) TestNodeAlloc(c *C) {
	var alloc nodeAlloc
	var ptrs []nodePtr
	for i := 0; i < nodeBlockSize*3+1; i++ {
		ptr := alloc.New()
		ptrs = append(ptrs, ptr)
	}
	for i := 0; i < len(ptrs); i++ {
		ptrs[i].Free(&alloc)
	}

	for len(ptrs) < nodeBlockSize*2 {
		if rand.Intn(3) == 0 && len(ptrs) > 0 {
			r := rand.Intn(len(ptrs))
			ptr := ptrs[r]
			if ptr.IsNil() {
				ptr.Free(&alloc)
				ptrs[r] = 0
			}
		} else {
			ptr := alloc.New()
			ptrs = append(ptrs, ptr)
		}
	}
}
