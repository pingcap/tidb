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

package sortedmap

// SortedMap is map that keys are iterated in sorted order.
type SortedMap interface {
	// NewIterator returns a new iterator that can be used to iterate over the
	// key/value pairs in the map in sorted order. Note that a new iterator is
	// unpositioned and must be seeked to a valid key before calling any methods.
	NewIterator() Iterator
	// NewWriter returns a new writer that can be used to batch write key/value pairs.
	NewWriter() Writer
	// Close releases all resources held by the map.
	Close() error
}

// Iterator is an iterator over a SortedMap.
type Iterator interface {
	// Seek moves the iterator to the first key/value pair whose key is greater
	// than or equal to the given key.
	Seek(key []byte) bool
	// First moves the iterator to the first key/value pair.
	First() bool
	// Next moves the iterator to the next key/value pair.
	Next() bool
	// Last moves the iterator to the last key/value pair.
	Last() bool
	// Valid returns true if the iterator is positioned at a valid key/value pair.
	Valid() bool
	// Error returns the error, if any, that was encountered during iteration.
	Error() error
	// UnsafeKey returns the key of the current key/value pair, without copying.
	// The memory is only valid until the next call to other methods.
	UnsafeKey() []byte
	// UnsafeValue returns the value of the current key/value pair, without copying.
	// The memory is only valid until the next call to other methods.
	UnsafeValue() []byte
	// Close releases all resources held by the iterator.
	Close() error
}

// Writer is a writer that can be used to batch write key/value pairs.
type Writer interface {
	// Put adds a key/value pair to the writer.
	Put(key, value []byte) error
	// Size returns the total size of buffered key/value pairs.
	Size() int64
	// Flush flushes all buffered key/value pairs to the underlying storage,
	// the writer can be reused after calling Flush().
	Flush() error
	// Close flushes all buffered key/value pairs and releases all resources held by the writer.
	Close() error
}
