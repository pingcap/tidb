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

import "context"

// IngestData describes a common interface that is needed by TiKV write +
// ingest RPC.
type IngestData interface {
	// GetFirstAndLastKey returns the first and last key of the data reader in the
	// range [lowerBound, upperBound). Empty or nil bounds means unbounded.
	// lowerBound must be less than upperBound.
	// when there is no data in the range, it should return nil, nil, nil
	GetFirstAndLastKey(lowerBound, upperBound []byte) ([]byte, []byte, error)
	NewIter(ctx context.Context, lowerBound, upperBound []byte) ForwardIter
	// GetTS will be used as the start/commit TS of the data.
	GetTS() uint64
	// Finish will be called when the data is ingested successfully.
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
	// Key represents current position pair's key.
	Key() []byte
	// Value represents current position pair's Value.
	Value() []byte
	// Close close this iter.
	Close() error
	// Error return current error on this iter.
	Error() error
}
