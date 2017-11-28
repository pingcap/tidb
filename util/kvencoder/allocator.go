// Copyright 2017 PingCAP, Inc.
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

package kvenc

import (
	"sync/atomic"

	"github.com/pingcap/tidb/meta/autoid"
)

var _ autoid.Allocator = &allocator{}

var (
	step = int64(5000)
)

func NewAllocator() autoid.Allocator {
	return &allocator{}
}

// allocator make sure that only use it in single thread
type allocator struct {
	base int64
}

func (alloc *allocator) Alloc(tableID int64) (int64, error) {
	return atomic.AddInt64(&alloc.base, 1), nil
}

func (alloc *allocator) Rebase(tableID, newBase int64, allocIDs bool) error {
	alloc.base = newBase
	return nil
}

func (alloc *allocator) Base() int64 {
	return alloc.base
}

func (alloc *allocator) End() int64 {
	return alloc.base + step
}
