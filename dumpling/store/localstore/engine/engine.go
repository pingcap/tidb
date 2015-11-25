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

package engine

// Driver is the interface that must be implemented by a local storage db engine.
type Driver interface {
	// Open opens or creates a local storage DB.
	// The schema is a string for a local storage DB specific format.
	Open(schema string) (DB, error)
}

// DB is the interface for local storage.
type DB interface {
	// Get the associated value with key
	// return nil, nil if no value found.
	Get(key []byte) ([]byte, error)
	// Seek seeks the iterator to the first key in the engine which
	// is >= startKey in byte-order.
	Seek(startKey []byte) (Iterator, error)
	// NewBatch creates a Batch for writing.
	NewBatch() Batch
	// Commit writes the changed data in Batch.
	Commit(b Batch) error
	// Close closes database.
	Close() error
}

// Iterator is the interface for local storage.
type Iterator interface {
	// Next moves the iterator to the next key/value pair,
	// returns true/false if the iterator is exhausted.
	Next() bool
	// Key returns the current key of the key/value pair or nil
	// if the iterator is done.
	Key() []byte
	// Value returns the current value of the key/value pair or nil
	// if the iterator is done.
	Value() []byte
	// Seek moves the iterator to the first key/value pair whose key is greater
	// or equal to the given key.
	// It returns whether such pair exists or not.
	Seek(startKey []byte) bool
	// Release releases current iterator.
	Release()
}

// Batch is the interface for local storage.
type Batch interface {
	// Put appends 'put operation' of the key/value to the batch.
	Put(key []byte, value []byte)
	// Delete appends 'delete operation' of the key/value to the batch.
	Delete(key []byte)
	// Len return length of the batch
	Len() int
}
