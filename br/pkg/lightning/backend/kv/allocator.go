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

// TODO combine with the pkg/kv package outside.

package kv

import (
	"sync/atomic"

	"github.com/pingcap/tidb/meta/autoid"
)

// panickingAllocator is an ID allocator which panics on all operations except Rebase
type panickingAllocator struct {
	autoid.Allocator
	base *int64
	ty   autoid.AllocatorType
}

// NewPanickingAllocator creates a PanickingAllocator shared by all allocation types.
func NewPanickingAllocators(base int64) autoid.Allocators {
	sharedBase := &base
	return autoid.NewAllocators(
		&panickingAllocator{base: sharedBase, ty: autoid.RowIDAllocType},
		&panickingAllocator{base: sharedBase, ty: autoid.AutoIncrementType},
		&panickingAllocator{base: sharedBase, ty: autoid.AutoRandomType},
	)
}

// Rebase implements the autoid.Allocator interface
func (alloc *panickingAllocator) Rebase(tableID, newBase int64, allocIDs bool) error {
	// CAS
	for {
		oldBase := atomic.LoadInt64(alloc.base)
		if newBase <= oldBase {
			break
		}
		if atomic.CompareAndSwapInt64(alloc.base, oldBase, newBase) {
			break
		}
	}
	return nil
}

// Base implements the autoid.Allocator interface
func (alloc *panickingAllocator) Base() int64 {
	return atomic.LoadInt64(alloc.base)
}

func (alloc *panickingAllocator) GetType() autoid.AllocatorType {
	return alloc.ty
}
