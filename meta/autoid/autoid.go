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
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
)

const (
	step = 1000
)

// Allocator is an auto increment id generator.
// Just keep id unique actually.
type Allocator interface {
	Alloc(tableID int64) (int64, error)
}

type allocator struct {
	mu    sync.Mutex
	base  int64
	end   int64
	store kv.Storage
}

// Alloc allocs the next autoID for table with tableID.
// It gets a batch of autoIDs at a time. So it does not need to access storage for each call.
func (alloc *allocator) Alloc(tableID int64) (int64, error) {
	if tableID == 0 {
		return 0, errors.New("Invalid tableID")
	}
	metaKey := meta.AutoIDKey(tableID)
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	if alloc.base == alloc.end { // step
		err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
			end, genIDErr := meta.GenID(txn, []byte(metaKey), step)
			if genIDErr != nil {
				return errors.Trace(genIDErr)
			}

			alloc.end = end
			alloc.base = alloc.end - step
			return nil
		})

		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	alloc.base++
	log.Infof("Alloc id %d, table ID:%d, from %p, store ID:%s", alloc.base, tableID, alloc, alloc.store.UUID())
	return alloc.base, nil
}

// NewAllocator returns a new auto increment id generator on the store.
func NewAllocator(store kv.Storage) Allocator {
	return &allocator{
		store: store,
	}
}
