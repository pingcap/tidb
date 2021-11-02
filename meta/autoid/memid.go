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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package autoid

import (
	"context"
	"math"

	"github.com/pingcap/tidb/parser/model"
)

// NewAllocatorFromTempTblInfo creates an in-memory allocator from a temporary table info.
func NewAllocatorFromTempTblInfo(tblInfo *model.TableInfo) Allocator {
	hasRowID := !tblInfo.PKIsHandle && !tblInfo.IsCommonHandle
	hasAutoIncID := tblInfo.GetAutoIncrementColInfo() != nil
	// Temporary tables don't support auto_random and sequence.
	if hasRowID || hasAutoIncID {
		return &inMemoryAllocator{
			isUnsigned: tblInfo.IsAutoIncColUnsigned(),
			allocType:  RowIDAllocType,
		}
	}
	return nil
}

// inMemoryAllocator is typically used for temporary tables.
// Some characteristics:
// - It allocates IDs from memory.
// - It's session-wide and thus won't be visited concurrently.
// - It doesn't support sequence.
// - The metrics are not reported.
type inMemoryAllocator struct {
	base       int64
	isUnsigned bool
	allocType  AllocatorType
}

// Base implements autoid.Allocator Base interface.
func (alloc *inMemoryAllocator) Base() int64 {
	return alloc.base
}

// End implements autoid.Allocator End interface.
func (alloc *inMemoryAllocator) End() int64 {
	// It doesn't matter.
	return 0
}

// GetType implements autoid.Allocator GetType interface.
func (alloc *inMemoryAllocator) GetType() AllocatorType {
	return alloc.allocType
}

// NextGlobalAutoID implements autoid.Allocator NextGlobalAutoID interface.
func (alloc *inMemoryAllocator) NextGlobalAutoID() (int64, error) {
	return alloc.base, nil
}

func (alloc *inMemoryAllocator) Alloc(ctx context.Context, n uint64, increment, offset int64) (int64, int64, error) {
	if n == 0 {
		return 0, 0, nil
	}
	if alloc.allocType == AutoIncrementType || alloc.allocType == RowIDAllocType {
		if !validIncrementAndOffset(increment, offset) {
			return 0, 0, errInvalidIncrementAndOffset.GenWithStackByArgs(increment, offset)
		}
	}
	if alloc.isUnsigned {
		return alloc.alloc4Unsigned(n, increment, offset)
	}
	return alloc.alloc4Signed(n, increment, offset)
}

// Rebase implements autoid.Allocator Rebase interface.
// The requiredBase is the minimum base value after Rebase.
// The real base may be greater than the required base.
func (alloc *inMemoryAllocator) Rebase(ctx context.Context, requiredBase int64, allocIDs bool) error {
	if alloc.isUnsigned {
		if uint64(requiredBase) > uint64(alloc.base) {
			alloc.base = requiredBase
		}
	} else {
		if requiredBase > alloc.base {
			alloc.base = requiredBase
		}
	}
	return nil
}

// ForceRebase implements autoid.Allocator ForceRebase interface.
func (alloc *inMemoryAllocator) ForceRebase(requiredBase int64) error {
	alloc.base = requiredBase
	return nil
}

func (alloc *inMemoryAllocator) alloc4Signed(n uint64, increment, offset int64) (int64, int64, error) {
	// Check offset rebase if necessary.
	if offset-1 > alloc.base {
		alloc.base = offset - 1
	}
	// CalcNeededBatchSize calculates the total batch size needed.
	n1 := CalcNeededBatchSize(alloc.base, int64(n), increment, offset, alloc.isUnsigned)

	// Condition alloc.base+N1 > alloc.end will overflow when alloc.base + N1 > MaxInt64. So need this.
	if math.MaxInt64-alloc.base <= n1 {
		return 0, 0, ErrAutoincReadFailed
	}

	min := alloc.base
	alloc.base += n1
	return min, alloc.base, nil
}

func (alloc *inMemoryAllocator) alloc4Unsigned(n uint64, increment, offset int64) (int64, int64, error) {
	// Check offset rebase if necessary.
	if uint64(offset)-1 > uint64(alloc.base) {
		alloc.base = int64(uint64(offset) - 1)
	}

	// CalcNeededBatchSize calculates the total batch size needed.
	n1 := CalcNeededBatchSize(alloc.base, int64(n), increment, offset, alloc.isUnsigned)

	// Condition alloc.base+n1 > alloc.end will overflow when alloc.base + n1 > MaxInt64. So need this.
	if math.MaxUint64-uint64(alloc.base) <= uint64(n1) {
		return 0, 0, ErrAutoincReadFailed
	}

	min := alloc.base
	// Use uint64 n directly.
	alloc.base = int64(uint64(alloc.base) + uint64(n1))
	return min, alloc.base, nil
}

func (alloc *inMemoryAllocator) AllocSeqCache() (int64, int64, int64, error) {
	return 0, 0, 0, errNotImplemented.GenWithStackByArgs()
}

func (alloc *inMemoryAllocator) RebaseSeq(requiredBase int64) (int64, bool, error) {
	return 0, false, errNotImplemented.GenWithStackByArgs()
}
