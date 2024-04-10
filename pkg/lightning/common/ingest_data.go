// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"context"

	"github.com/pingcap/tidb/br/pkg/membuf"
)

// IngestData describes a common interface that is needed by TiKV write +
// ingest RPC.
type IngestData interface {
	// GetFirstAndLastKey returns the first and last key of the data reader in the
	// range [lowerBound, upperBound). Empty or nil bounds means unbounded.
	// lowerBound must be less than upperBound.
	// when there is no data in the range, it should return nil, nil, nil
	GetFirstAndLastKey(lowerBound, upperBound []byte) ([]byte, []byte, error)
	// NewIter creates an iterator. The only expected usage of the iterator is read
	// batches of key-value pairs by caller. Due to the implementation of IngestData,
	// the iterator may need to allocate memories to retain the key-value pair batch,
	// these memories will be allocated from given bufPool and be released when the
	// iterator is closed or ForwardIter.ReleaseBuf is called.
	NewIter(ctx context.Context, lowerBound, upperBound []byte, bufPool *membuf.Pool) ForwardIter
	// GetTS will be used as the start/commit TS of the data.
	GetTS() uint64
	// IncRef should be called every time when IngestData is referred by regionJob.
	// Multiple regionJob can share one IngestData. Same amount of DecRef should be
	// called to release the IngestData.
	IncRef()
	// DecRef is used to cooperate with IncRef to release IngestData.
	DecRef()
	// Finish will be called when the data is ingested successfully. Note that
	// one IngestData maybe partially ingested, so this function may be called
	// multiple times.
	Finish(totalBytes, totalCount int64)
}

// ForwardIter describes a iterator that can only move forward.
type ForwardIter interface {
	// First moves this iter to the first key.
	First() bool
	// Valid check this iter reach the end.
	Valid() bool
	// Next moves this iter forward.
	Next() bool
	// Key returns current position pair's key. The key is accessible after more
	// Next() or Key() invocations but is invalidated by Close() or ReleaseBuf().
	Key() []byte
	// Value returns current position pair's Value. The value is accessible after
	// more Next() or Value() invocations but is invalidated by Close() or
	// ReleaseBuf().
	Value() []byte
	// Close close this iter.
	Close() error
	// Error return current error on this iter.
	Error() error
	// ReleaseBuf release the memory that saves the previously returned keys and
	// values. These previously returned keys and values should not be accessed
	// again.
	ReleaseBuf()
}

// DataAndRange is a pair of IngestData and Range.
type DataAndRange struct {
	Data  IngestData
	Range Range
}
