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

import (
	"context"
	"errors"
)

var (
	// ErrSorted is returned when the items are already sorted,
	// ew items are not allowed to be added.
	ErrSorted = errors.New("already sorted")
	// ErrNotSorted is returned when the items are not sorted,
	// iterator is not allowed to be created.
	ErrNotSorted = errors.New("not sorted")
)

// Item is an interface for items to be sorted.
type Item[T any] interface {
	// Compare compares the item with the given item.
	// It returns -1 if receiver is lesser, 0 if equal, 1 if greater.
	Compare(other T) int
}

// ItemCodec is an interface for transforming items to bytes and vice versa.
type ItemCodec[T Item[T]] interface {
	// Encode encodes the item to bytes, appends the encoded bytes to
	// the given buffer and returns the final buffer.
	Encode(appendTo []byte, item T) []byte
	// Decode decodes the item from the given bytes. If target is non-nil,
	// the decoded item is stored in target and returned.
	//
	// The result may or may not be the same as target. So the caller should
	// always use the returned value.
	Decode(data []byte, target T) (T, error)
}

// ExternalSorter is an interface for external sorting.
type ExternalSorter[T Item[T]] interface {
	// NewWriter creates a new writer for writing items before sorting.
	// It should be called before calling Sort().
	NewWriter(ctx context.Context) (Writer[T], error)
	// Sort sorts all items written by the writers.
	// It should be called after all open writers are closed.
	Sort(ctx context.Context) error
	// NewIterator creates a new iterator for iterating over the items
	// after sorting. It should be called after calling Sort().
	NewIterator(ctx context.Context) (Iterator[T], error)
	// Close closes all open resources held by the sorter.
	Close() error
	// CloseAndCleanup closes the external sorter and cleans up all
	// resources created by the sorter.
	CloseAndCleanup() error
}

// Writer is an interface for writing items before sorting.
type Writer[T Item[T]] interface {
	// Put adds the given item to the writer. The caller should not
	// modify the item after calling Put().
	Put(item T) error
	// Flush flushes all buffered data to the external sorter,
	// the writer can be reused after calling Flush().
	Flush() error
	// Close flushes remaining data to the external sorter
	// and releases all resources held by the writer.
	Close() error
}

// Iterator is an interface for iterating over the items after sorting.
type Iterator[T Item[T]] interface {
	// Seek moves the iterator to the first item which is greater than
	// or equal to the given item.
	Seek(item T) bool
	// First moves the iterator to the first item.
	First() bool
	// Next moves the iterator to the next item.
	Next() bool
	// Last moves the iterator to the last item.
	Last() bool
	// Valid returns true if the iterator is positioned at a valid item.
	Valid() bool
	// Error returns the error, if any, that was encountered during iteration.
	Error() error
	// Item returns the current item. The caller should not modify the item.
	// It may change on the next call to move the iterator.
	Item() T
	// Close releases all resources held by the iterator.
	Close() error
}
