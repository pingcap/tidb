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

package txn

import (
	"context"
	"unsafe"

	"github.com/pingcap/tidb/kv"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

// tikvBatchGetter is the BatchGetter struct for tikv
// In order to directly call NewBufferBatchGetter in client-go
// We need to implement the interface (transaction.BatchGetter) in client-go for tikvBatchGetter
type tikvBatchGetter struct {
	tidbBatchGetter BatchGetter
}

func (b tikvBatchGetter) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	// toTiDBKeys
	kvKeys := *(*[]kv.Key)(unsafe.Pointer(&keys))
	vals, err := b.tidbBatchGetter.BatchGet(ctx, kvKeys)
	return vals, err
}

// tikvBatchBufferGetter is the BatchBufferGetter struct for tikv
// In order to directly call NewBufferBatchGetter in client-go
// We need to implement the interface (transaction.BatchBufferGetter) in client-go for tikvBatchBufferGetter
type tikvBatchBufferGetter struct {
	tidbMiddleCache Getter
	tidbBuffer      BatchBufferGetter
}

func (b tikvBatchBufferGetter) Get(k []byte) ([]byte, error) {
	// Get from buffer
	val, err := b.tidbBuffer.Get(context.TODO(), k)
	if err == nil || !kv.IsErrNotFound(err) || b.tidbMiddleCache == nil {
		if kv.IsErrNotFound(err) {
			err = tikverr.ErrNotExist
		}
		return val, err
	}
	// Get from middle cache
	val, err = b.tidbMiddleCache.Get(context.TODO(), k)
	if err == nil {
		return val, err
	}
	// TiDB err NotExist to TiKV err NotExist
	// The BatchGet method in client-go will call this method
	// Therefore, the error needs to convert to TiKV's type, otherwise the error will not be handled properly in client-go
	err = tikverr.ErrNotExist
	return val, err
}

func (b tikvBatchBufferGetter) Len() int {
	return b.tidbBuffer.Len()
}

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
	tikvBufferBatchGetter transaction.BufferBatchGetter
}

// NewBufferBatchGetter creates a new BufferBatchGetter.
func NewBufferBatchGetter(buffer BatchBufferGetter, middleCache Getter, snapshot BatchGetter) *BufferBatchGetter {
	tikvBuffer := tikvBatchBufferGetter{tidbMiddleCache: middleCache, tidbBuffer: buffer}
	tikvSnapshot := tikvBatchGetter{snapshot}
	return &BufferBatchGetter{tikvBufferBatchGetter: *transaction.NewBufferBatchGetter(tikvBuffer, tikvSnapshot)}
}

// BatchGet implements the BatchGetter interface.
func (b *BufferBatchGetter) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	tikvKeys := toTiKVKeys(keys)
	storageValues, err := b.tikvBufferBatchGetter.BatchGet(ctx, tikvKeys)

	return storageValues, err
}
