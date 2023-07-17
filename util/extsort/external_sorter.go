// Copyright 2023 PingCAP, Inc.
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

package extsort

import "context"

// ExternalSorter is an interface for sorting key-value pairs in external storage.
// The key-value pairs are sorted by the key, duplicate keys are automatically removed.
type ExternalSorter interface {
	// NewWriter creates a new writer for writing key-value pairs before sorting.
	// If the sorter starts sorting or is already sorted, it will return an error.
	//
	// Multiple writers can be opened and used concurrently.
	NewWriter(ctx context.Context) (Writer, error)
	// Sort sorts the key-value pairs written by the writer.
	// It should be called after all open writers are closed.
	//
	// Implementations should guarantee that Sort() is idempotent and atomic.
	// If it returns an error, or process is killed during Sort(), the sorter should be able
	// to recover from the error or crash, and the external storage should not be corrupted.
	Sort(ctx context.Context) error
	// IsSorted returns true if the sorter is already sorted, iterators are ready to create.
	IsSorted() bool
	// NewIterator creates a new iterator for iterating over the key-value pairs after sorting.
	// If the sorter is not sorted yet, it will return an error.
	//
	// Multiple iterators can be opened and used concurrently.
	NewIterator(ctx context.Context) (Iterator, error)
	// Close releases all resources held by the sorter. It will not clean up the external storage,
	// so the sorter can recover from a crash.
	Close() error
	// CloseAndCleanup closes the external sorter and cleans up all resources created by the sorter.
	CloseAndCleanup() error
}

// Writer is an interface for writing key-value pairs to the external sorter.
type Writer interface {
	// Put adds a key-value pair to the writer.
	//
	// Implementations must not modify or retain slices passed to Put().
	Put(key, value []byte) error
	// Flush flushes all buffered key-value pairs to the external sorter.
	// the writer can be reused after calling Flush().
	Flush() error
	// Close flushes all buffered key-value pairs to the external sorter,
	// and releases all resources held by the writer.
	Close() error
}

// Iterator is an interface for iterating over the key-value pairs after sorting.
type Iterator interface {
	// Seek moves the iterator to the first key-value pair whose key is greater
	// than or equal to the given key.
	Seek(key []byte) bool
	// First moves the iterator to the first key-value pair.
	First() bool
	// Next moves the iterator to the next key-value pair.
	//
	// Implementations must guarantee the next key is greater than the current key.
	Next() bool
	// Last moves the iterator to the last key-value pair.
	Last() bool
	// Valid returns true if the iterator is positioned at a valid key-value pair.
	Valid() bool
	// Error returns the error, if any, that was encountered during iteration.
	Error() error
	// UnsafeKey returns the key of the current key-value pair, without copying.
	// The memory is only valid until the next call to change the iterator.
	UnsafeKey() []byte
	// UnsafeValue returns the value of the current key-value pair, without copying.
	// The memory is only valid until the next call to change the iterator.
	UnsafeValue() []byte
	// Close releases all resources held by the iterator.
	Close() error
}
