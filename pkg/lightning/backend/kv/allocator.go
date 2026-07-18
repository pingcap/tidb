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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// TODO combine with the pkg/kv package outside.

package kv

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/meta/autoid"
)

// panickingAllocator is an ID allocator which panics on all operations except Rebase
type panickingAllocator struct {
	autoid.Allocator
	base atomic.Int64
	ty   autoid.AllocatorType
}

// NewPanickingAllocators creates a PanickingAllocator with default base values.
func NewPanickingAllocators(sepAutoInc bool) autoid.Allocators {
	return NewPanickingAllocatorsWithBase(sepAutoInc, 0, 0, 0)
}

// NewPanickingAllocatorsWithBase creates a PanickingAllocator shared by all allocation types.
// we use this to collect the max id(either _tidb_rowid or auto_increment id or auto_random) used
// during import, and we will use this info to do ALTER TABLE xxx AUTO_RANDOM_BASE or AUTO_INCREMENT
// on post-process phase.
func NewPanickingAllocatorsWithBase(sepAutoInc bool, autoRandBase, autoIncrBase, autoRowIDBase int64) autoid.Allocators {
	allocs := make([]autoid.Allocator, 0, 3)
	for _, t := range []struct {
		Type autoid.AllocatorType
		Base int64
	}{
		{Type: autoid.AutoRandomType, Base: autoRandBase},
		{Type: autoid.AutoIncrementType, Base: autoIncrBase},
		{Type: autoid.RowIDAllocType, Base: autoRowIDBase},
	} {
		pa := &panickingAllocator{ty: t.Type}
		pa.base.Store(t.Base)
		allocs = append(allocs, pa)
	}
	return autoid.NewAllocators(sepAutoInc, allocs...)
}

// Rebase implements the autoid.Allocator interface
func (alloc *panickingAllocator) Rebase(_ context.Context, newBase int64, _ bool) error {
	// CAS
	for {
		oldBase := alloc.base.Load()
		if newBase <= oldBase {
			break
		}
		if alloc.base.CompareAndSwap(oldBase, newBase) {
			break
		}
	}
	return nil
}

// Base implements the autoid.Allocator interface
func (alloc *panickingAllocator) Base() int64 {
	return alloc.base.Load()
}

func (alloc *panickingAllocator) GetType() autoid.AllocatorType {
	return alloc.ty
}
