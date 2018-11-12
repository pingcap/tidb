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

package autoid

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	log "github.com/sirupsen/logrus"
)

// Test needs to change it, so it's a variable.
var step = int64(30000)

var errInvalidTableID = terror.ClassAutoid.New(codeInvalidTableID, "invalid TableID")

// Allocator is an auto increment id generator.
// Just keep id unique actually.
type Allocator interface {
	// Alloc allocs the next autoID for table with tableID.
	// It gets a batch of autoIDs at a time. So it does not need to access storage for each call.
	Alloc(tableID int64) (int64, error)
	// Rebase rebases the autoID base for table with tableID and the new base value.
	// If allocIDs is true, it will allocate some IDs and save to the cache.
	// If allocIDs is false, it will not allocate IDs.
	Rebase(tableID, newBase int64, allocIDs bool) error
	// Base return the current base of Allocator.
	Base() int64
	// End is only used for test.
	End() int64
	// NextGlobalAutoID returns the next global autoID.
	NextGlobalAutoID(tableID int64) (int64, error)
}

type allocator struct {
	mu    sync.Mutex
	base  int64
	end   int64
	store kv.Storage
	// dbID is current database's ID.
	dbID       int64
	isUnsigned bool
}

// GetStep is only used by tests
func GetStep() int64 {
	return step
}

// SetStep is only used by tests
func SetStep(s int64) {
	step = s
}

// Base implements autoid.Allocator Base interface.
func (alloc *allocator) Base() int64 {
	return alloc.base
}

// End implements autoid.Allocator End interface.
func (alloc *allocator) End() int64 {
	return alloc.end
}

// NextGlobalAutoID implements autoid.Allocator NextGlobalAutoID interface.
func (alloc *allocator) NextGlobalAutoID(tableID int64) (int64, error) {
	var autoID int64
	startTime := time.Now()
	err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
		var err1 error
		m := meta.NewMeta(txn)
		autoID, err1 = m.GetAutoTableID(alloc.dbID, tableID)
		return errors.Trace(err1)
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.GlobalAutoID, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if alloc.isUnsigned {
		return int64(uint64(autoID) + 1), err
	}
	return autoID + 1, err
}

// Rebase implements autoid.Allocator Rebase interface.
// The requiredBase is the minimum base value after Rebase.
// The real base may be greater than the required base.
func (alloc *allocator) Rebase(tableID, requiredBase int64, allocIDs bool) error {
	if tableID == 0 {
		return errInvalidTableID.GenWithStack("Invalid tableID")
	}

	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	// Satisfied by alloc.base, nothing to do.
	if !alloc.isUnsigned && requiredBase <= alloc.base {
		return nil
	} else if alloc.isUnsigned && uint64(requiredBase) <= uint64(alloc.base) {
		return nil
	}

	// Satisfied by alloc.end, need to update alloc.base.
	if !alloc.isUnsigned && requiredBase <= alloc.end {
		alloc.base = requiredBase
		return nil
	} else if alloc.isUnsigned && uint64(requiredBase) <= uint64(alloc.end) {
		alloc.base = requiredBase
		return nil
	}
	var newBase, newEnd int64
	var uNewBase, uNewEnd uint64
	startTime := time.Now()
	err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		currentEnd, err1 := m.GetAutoTableID(alloc.dbID, tableID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		uCurrentEnd, uRequiredBase := uint64(currentEnd), uint64(requiredBase)
		if allocIDs {
			if alloc.isUnsigned {
				uNewBase = mathutil.MaxUint64(uCurrentEnd, uRequiredBase)
				uNewEnd = mathutil.MinUint64(math.MaxUint64-uint64(step), uNewBase) + uint64(step)
			} else {
				newBase = mathutil.MaxInt64(currentEnd, requiredBase)
				newEnd = mathutil.MinInt64(math.MaxInt64-step, newBase) + step
			}
		} else {
			if !alloc.isUnsigned && currentEnd >= requiredBase {
				newBase = currentEnd
				newEnd = currentEnd
				// Required base satisfied, we don't need to update KV.
				return nil
			} else if alloc.isUnsigned && uCurrentEnd >= uRequiredBase {
				uNewBase = uCurrentEnd
				uNewEnd = uCurrentEnd
				return nil
			}
			// If we don't want to allocate IDs, for example when creating a table with a given base value,
			// We need to make sure when other TiDB server allocates ID for the first time, requiredBase + 1
			// will be allocated, so we need to increase the end to exactly the requiredBase.
			if !alloc.isUnsigned {
				newBase = requiredBase
				newEnd = requiredBase
			} else {
				uNewBase = uRequiredBase
				uNewEnd = uRequiredBase
			}
		}
		if !alloc.isUnsigned {
			_, err1 = m.GenAutoTableID(alloc.dbID, tableID, newEnd-currentEnd)
		} else {
			_, err1 = m.GenAutoTableID(alloc.dbID, tableID, int64(uNewEnd-uCurrentEnd))
		}
		return errors.Trace(err1)
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return errors.Trace(err)
	}
	if !alloc.isUnsigned {
		alloc.base, alloc.end = newBase, newEnd
	} else {
		alloc.base, alloc.end = int64(uNewBase), int64(uNewEnd)
	}
	return nil
}

// Alloc implements autoid.Allocator Alloc interface.
func (alloc *allocator) Alloc(tableID int64) (int64, error) {
	if tableID == 0 {
		return 0, errInvalidTableID.GenWithStack("Invalid tableID")
	}
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	if alloc.base == alloc.end { // step
		var newBase, newEnd int64
		startTime := time.Now()
		err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
			m := meta.NewMeta(txn)
			var err1 error
			newBase, err1 = m.GetAutoTableID(alloc.dbID, tableID)
			if err1 != nil {
				return errors.Trace(err1)
			}
			tmpStep := step
			if alloc.isUnsigned {
				tmpStep = int64(mathutil.MinUint64(math.MaxUint64-uint64(newBase), uint64(step)))
			} else {
				tmpStep = mathutil.MinInt64(math.MaxInt64-newBase, step)
			}
			newEnd, err1 = m.GenAutoTableID(alloc.dbID, tableID, tmpStep)
			if err1 != nil {
				return errors.Trace(err1)
			}
			return nil
		})
		metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDAlloc, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		if err != nil {
			return 0, err
		}
		alloc.base, alloc.end = newBase, newEnd
	}
	log.Warning(uint64(alloc.base), uint64(alloc.end))

	if alloc.isUnsigned {
		if uint64(alloc.base) == math.MaxUint64 {
			return 0, ErrAutoincReadFailed
		}
		alloc.base = int64(uint64(alloc.base) + 1)
		log.Debugf("[kv] Alloc id %d, table ID:%d, from %p, database ID:%d", uint64(alloc.base), tableID, alloc, alloc.dbID)
	} else {
		if alloc.base < alloc.end {
			alloc.base++
		}
		log.Debugf("[kv] Alloc id %d, table ID:%d, from %p, database ID:%d", alloc.base, tableID, alloc, alloc.dbID)
	}
	return alloc.base, nil
}

var (
	memID     int64
	memIDLock sync.Mutex
)

type memoryAllocator struct {
	mu   sync.Mutex
	base int64
	end  int64
	dbID int64
}

// Base implements autoid.Allocator Base interface.
func (alloc *memoryAllocator) Base() int64 {
	return alloc.base
}

// End implements autoid.Allocator End interface.
func (alloc *memoryAllocator) End() int64 {
	return alloc.end
}

// NextGlobalAutoID implements autoid.Allocator NextGlobalAutoID interface.
func (alloc *memoryAllocator) NextGlobalAutoID(tableID int64) (int64, error) {
	memIDLock.Lock()
	defer memIDLock.Unlock()
	return memID + 1, nil
}

// Rebase implements autoid.Allocator Rebase interface.
func (alloc *memoryAllocator) Rebase(tableID, newBase int64, allocIDs bool) error {
	// TODO: implement it.
	return nil
}

// Alloc implements autoid.Allocator Alloc interface.
func (alloc *memoryAllocator) Alloc(tableID int64) (int64, error) {
	if tableID == 0 {
		return 0, errInvalidTableID.GenWithStack("Invalid tableID")
	}
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	if alloc.base == alloc.end { // step
		memIDLock.Lock()
		memID = memID + step
		alloc.end = memID
		alloc.base = alloc.end - step
		memIDLock.Unlock()
	}
	alloc.base++
	return alloc.base, nil
}

// NewAllocator returns a new auto increment id generator on the store.
func NewAllocator(store kv.Storage, dbID int64, isUnsigned bool) Allocator {
	return &allocator{
		store:      store,
		dbID:       dbID,
		isUnsigned: isUnsigned,
	}
}

// NewMemoryAllocator returns a new auto increment id generator in memory.
func NewMemoryAllocator(dbID int64) Allocator {
	return &memoryAllocator{
		dbID: dbID,
	}
}

//autoid error codes.
const codeInvalidTableID terror.ErrCode = 1

var localSchemaID = int64(math.MaxInt64)

// GenLocalSchemaID generates a local schema ID.
func GenLocalSchemaID() int64 {
	return atomic.AddInt64(&localSchemaID, -1)
}
