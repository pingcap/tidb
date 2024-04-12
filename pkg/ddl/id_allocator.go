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

const (
	defaultIDAllocatorStep = 10
)

type IDAllocator interface {
	AllocIDs(n int) ([]int64, error)
}

type idAllocator struct {
	cache []int64
	store kv.Storage
	mu    sync.Mutex
}

func NewIDAllocator(store kv.Storage) IDAllocator {
	return &idAllocator{
		cache: make([]int64, 0, defaultIDAllocatorStep),
		store: store,
	}
}

func (ia *idAllocator) AllocIDs(n int) ([]int64, error) {
	ia.mu.Lock()
	defer ia.mu.Unlock()

	ids := make([]int64, 0, n)
	// return the range of IDs n is less than or equal to the range
	if n <= len(ia.cache) {
		ids = ia.cache[:n]
		ia.cache = ia.cache[n:]
		return ids, nil
	}

	count := max(defaultIDAllocatorStep, n-len(ia.cache))
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	var (
		ret []int64
		err error
	)
	err2 := kv.RunInNewTxn(ctx, ia.store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		ret, err = m.GenGlobalIDs(count)
		return err
	})
	if err2 != nil {
		return nil, err2
	}
	// append the new IDs to the cache
	ia.cache = append(ia.cache, ret...)
	ids = ia.cache[:n]
	ia.cache = ia.cache[n:]
	return ids, nil
}
