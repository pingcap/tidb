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

package kv

import (
	"context"
	"strconv"

	"github.com/pingcap/errors"
)

// IncInt64 increases the value for key k in kv store by step.
func IncInt64(rm RetrieverMutator, k Key, step int64) (int64, error) {
	val, err := rm.Get(context.TODO(), k)
	if IsErrNotFound(err) {
		err = rm.Set(k, []byte(strconv.FormatInt(step, 10)))
		if err != nil {
			return 0, err
		}
		return step, nil
	}
	if err != nil {
		return 0, err
	}

	intVal, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}

	intVal += step
	err = rm.Set(k, []byte(strconv.FormatInt(intVal, 10)))
	if err != nil {
		return 0, err
	}
	return intVal, nil
}

// GetInt64 get int64 value which created by IncInt64 method.
func GetInt64(ctx context.Context, r Retriever, k Key) (int64, error) {
	val, err := r.Get(ctx, k)
	if IsErrNotFound(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	intVal, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return intVal, errors.Trace(err)
	}
	return intVal, nil
}

// WalkMemBuffer iterates all buffered kv pairs in memBuf
func WalkMemBuffer(memBuf Retriever, f func(k Key, v []byte) error) error {
	iter, err := memBuf.Iter(nil, nil)
	if err != nil {
		return errors.Trace(err)
	}

	defer iter.Close()
	for iter.Valid() {
		if err = f(iter.Key(), iter.Value()); err != nil {
			return errors.Trace(err)
		}
		err = iter.Next()
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// BufferBatchGetter is the type for BatchGet with MemBuffer.
type BufferBatchGetter struct {
	buffer   MemBuffer
	middle   Getter
	snapshot Snapshot
}

// NewBufferBatchGetter creates a new BufferBatchGetter.
func NewBufferBatchGetter(buffer MemBuffer, middleCache Getter, snapshot Snapshot) *BufferBatchGetter {
	return &BufferBatchGetter{buffer: buffer, middle: middleCache, snapshot: snapshot}
}

// BatchGet implements the BatchGetter interface.
func (b *BufferBatchGetter) BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error) {
	if b.buffer.Len() == 0 {
		return b.snapshot.BatchGet(ctx, keys)
	}
	bufferValues := make([][]byte, len(keys))
	shrinkKeys := make([]Key, 0, len(keys))
	for i, key := range keys {
		val, err := b.buffer.Get(ctx, key)
		if err == nil {
			bufferValues[i] = val
			continue
		}
		if !IsErrNotFound(err) {
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
