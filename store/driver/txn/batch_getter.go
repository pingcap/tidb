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
// See the License for the specific language governing permissions and
// limitations under the License.

package txn

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
)

// BatchBufferGetter is the interface for BatchGet.
type BatchBufferGetter interface {
	Len() int
	Getter
}

// BatchGetter is the interface for BatchGet.
type BatchGetter interface {
	// BatchGet gets a batch of values.
	BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error)
}

// Getter is the interface for the Get method.
type Getter interface {
	// Get gets the value for key k from kv store.
	// If corresponding kv pair does not exist, it returns nil and ErrNotExist.
	Get(ctx context.Context, k kv.Key) ([]byte, error)
}

// BufferBatchGetter is the type for BatchGet with MemBuffer.
type BufferBatchGetter struct {
	buffer   BatchBufferGetter
	middle   Getter
	snapshot BatchGetter
}

// NewBufferBatchGetter creates a new BufferBatchGetter.
func NewBufferBatchGetter(buffer BatchBufferGetter, middleCache Getter, snapshot BatchGetter) *BufferBatchGetter {
	return &BufferBatchGetter{buffer: buffer, middle: middleCache, snapshot: snapshot}
}

// BatchGet implements the BatchGetter interface.
func (b *BufferBatchGetter) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	if b.buffer.Len() == 0 {
		return b.snapshot.BatchGet(ctx, keys)
	}
	bufferValues := make([][]byte, len(keys))
	shrinkKeys := make([]kv.Key, 0, len(keys))
	for i, key := range keys {
		val, err := b.buffer.Get(ctx, key)
		if err == nil {
			bufferValues[i] = val
			continue
		}
		if !kv.IsErrNotFound(err) {
			return nil, errors.Trace(err)
		}
		if b.middle != nil {
			val, err = b.middle.Get(ctx, key)
			if err == nil {
				bufferValues[i] = val
				continue
			}
		}
		shrinkKeys = append(shrinkKeys, key)
	}
	storageValues, err := b.snapshot.BatchGet(ctx, shrinkKeys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i, key := range keys {
		if len(bufferValues[i]) == 0 {
			continue
		}
		storageValues[string(key)] = bufferValues[i]
	}
	return storageValues, nil
}
