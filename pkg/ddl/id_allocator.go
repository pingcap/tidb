// Copyright 2024 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"context"
	"sync"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
)

var (
	// IDAllocatorStep is the default step for id allocator.
	IDAllocatorStep = 10
)

// IDAllocator is used to allocate global ID.
type IDAllocator interface {
	AllocIDs(n int) ([]int64, error)
}

type allocator struct {
	cache []int64
	store kv.Storage
	mu    sync.Mutex
}

// NewAllocator creates a new IDAllocator.
func NewAllocator(store kv.Storage) IDAllocator {
	return &allocator{
		cache: make([]int64, 0, IDAllocatorStep),
		store: store,
	}
}

func (a *allocator) AllocIDs(n int) ([]int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var ids []int64
	// return the range of IDs n is less than or equal to the range
	if n <= len(a.cache) {
		ids = a.cache[:n]
		a.cache = a.cache[n:]
		return ids, nil
	}

	count := max(IDAllocatorStep, n-len(a.cache))
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	var (
		ret []int64
		err error
	)
	err2 := kv.RunInNewTxn(ctx, a.store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		ret, err = m.GenGlobalIDs(count)
		return err
	})
	if err2 != nil {
		return nil, err2
	}
	// append the new IDs to the cache
	a.cache = append(a.cache, ret...)
	ids = a.cache[:n]
	a.cache = a.cache[n:]
	return ids, nil
}
