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

	log "github.com/Sirupsen/logrus"
	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/terror"
)

// Test needs to change it, so it's a variable.
var step = int64(5000)

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
	// Base is only used for test.
	Base() int64
	// End is only used for test.
	End() int64
}

type allocator struct {
	mu    sync.Mutex
	base  int64
	end   int64
	store kv.Storage
	// originalDBID saves original schemaID to keep autoID unchanged
	// while renaming a table from one database to another.
	originalDBID int64
	// dbID is current database's ID.
	dbID int64
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

// Rebase implements autoid.Allocator Rebase interface.
// The requiredBase is the minimum base value after Rebase.
// The real base may be greater than the required base.
func (alloc *allocator) Rebase(tableID, requiredBase int64, allocIDs bool) error {
	if tableID == 0 {
		return errInvalidTableID.Gen("Invalid tableID")
	}

	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	if requiredBase <= alloc.base {
		// Satisfied by alloc.base, nothing to do.
		return nil
	}
	if requiredBase <= alloc.end {
		// Satisfied by alloc.end, need to updata alloc.base.
		alloc.base = requiredBase
		return nil
	}
	var newBase, newEnd int64
	err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		currentEnd, err1 := m.GetAutoTableID(alloc.originalDBID, tableID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		if allocIDs {
			newBase = mathutil.MaxInt64(currentEnd, requiredBase)
			newEnd = newBase + step
		} else {
			if currentEnd >= requiredBase {
				newBase = currentEnd
				newEnd = currentEnd
				// Required base satisfied, we don't need to update KV.
				return nil
			}
			// If we don't want to allocate IDs, for example when creating a table with a given base value,
			// We need to make sure when other TiDB server allocates ID for the first time, requiredBase + 1
			// will be allocated, so we need to increase the end to exactly the requiredBase.
			newBase = requiredBase
			newEnd = requiredBase
		}
		_, err1 = m.GenAutoTableID(alloc.originalDBID, alloc.dbID, tableID, newEnd-currentEnd)
		return errors.Trace(err1)
	})
	if err != nil {
		return errors.Trace(err)
	}
	alloc.base, alloc.end = newBase, newEnd
	return nil
}

// Alloc implements autoid.Allocator Alloc interface.
func (alloc *allocator) Alloc(tableID int64) (int64, error) {
	if tableID == 0 {
		return 0, errInvalidTableID.Gen("Invalid tableID")
	}
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	if alloc.base == alloc.end { // step
		var newBase, newEnd int64
		err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
			m := meta.NewMeta(txn)
			var err1 error
			newBase, err1 = m.GetAutoTableID(alloc.originalDBID, tableID)
			if err1 != nil {
				return errors.Trace(err1)
			}
			newEnd, err1 = m.GenAutoTableID(alloc.originalDBID, alloc.dbID, tableID, step)
			if err1 != nil {
				return errors.Trace(err1)
			}
			return nil
		})

		if err != nil {
			return 0, errors.Trace(err)
		}
		alloc.base, alloc.end = newBase, newEnd
	}

	alloc.base++
	log.Debugf("[kv] Alloc id %d, table ID:%d, from %p, database ID:%d", alloc.base, tableID, alloc, alloc.dbID)
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

// Rebase implements autoid.Allocator Rebase interface.
func (alloc *memoryAllocator) Rebase(tableID, newBase int64, allocIDs bool) error {
	// TODO: implement it.
	return nil
}

// Alloc implements autoid.Allocator Alloc interface.
func (alloc *memoryAllocator) Alloc(tableID int64) (int64, error) {
	if tableID == 0 {
		return 0, errInvalidTableID.Gen("Invalid tableID")
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
func NewAllocator(store kv.Storage, originalDBID, dbID int64) Allocator {
	// If original DB ID is zero, it means that the orignial DB ID is equal to the current DB ID.
	if originalDBID == 0 {
		originalDBID = dbID
	}
	return &allocator{
		store:        store,
		originalDBID: originalDBID,
		dbID:         dbID,
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
