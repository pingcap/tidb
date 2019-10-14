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

var _ autoid.Allocator = &Allocator{}

var (
	step = int64(5000)
)

// NewAllocator creates an Allocator.
func NewAllocator() *Allocator {
	return &Allocator{}
}

// Allocator is an id allocator, it is only used in lightning.
type Allocator struct {
	base int64
}

// Alloc allocs a next autoID for table with tableID.
func (alloc *Allocator) Alloc(tableID int64, n uint64) (int64, int64, error) {
	min := alloc.base
	return min, atomic.AddInt64(&alloc.base, int64(n)), nil
}

// Reset allow newBase smaller than alloc.base, and will set the alloc.base to newBase.
func (alloc *Allocator) Reset(newBase int64) {
	atomic.StoreInt64(&alloc.base, newBase)
}

// Rebase not allow newBase smaller than alloc.base, and will skip the smaller newBase.
func (alloc *Allocator) Rebase(tableID, newBase int64, allocIDs bool) error {
	// CAS
	for {
		oldBase := atomic.LoadInt64(&alloc.base)
		if newBase <= oldBase {
			break
		}
		if atomic.CompareAndSwapInt64(&alloc.base, oldBase, newBase) {
			break
		}
	}

	return nil
}

// Base returns the current base of Allocator.
func (alloc *Allocator) Base() int64 {
	return atomic.LoadInt64(&alloc.base)
}

// End is only used for test.
func (alloc *Allocator) End() int64 {
	return alloc.Base() + step
}

// NextGlobalAutoID returns the next global autoID.
func (alloc *Allocator) NextGlobalAutoID(tableID int64) (int64, error) {
	return alloc.End() + 1, nil
}
