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

// DecodeFn is a function that decode data after fetch from store.
type DecodeFn func(raw interface{}) (interface{}, error)

// EncodeFn is a function that encode data before put into store
type EncodeFn func(raw interface{}) (interface{}, error)

// Transaction defines the interface for operations inside a Transaction.
// This is not thread safe.
type Transaction interface {
	// Get gets the value for key k from KV store.
	Get(k []byte) ([]byte, error)
	// Set sets the value for key k as v into KV store.
	Set(k []byte, v []byte) error
	// Seek searches for the entry with key k in KV store.
	Seek(k []byte, fnKeyCmp func(key []byte) bool) (Iterator, error)
	// Inc increases the value for key k in KV store by step.
	Inc(k []byte, step int64) (int64, error)
	// Deletes removes the entry for key k from KV store.
	Delete(k []byte) error
	// Commit commites the transaction operations to KV store.
	Commit() error
	// Rollback undoes the transaction operations to KV store.
	Rollback() error
	// String implements Stringer.String() interface.
	String() string
	// LockKeys tries to lock the entries with the keys in KV store.
	LockKeys(keys ...[]byte) error
}

// Snapshot defines the interface for the snapshot fetched from KV store.
type Snapshot interface {
	// Get gets the value for key k from snapshot.
	Get(k []byte) ([]byte, error)
	// NewIterator gets a new iterator on the snapshot.
	NewIterator(param interface{}) Iterator
	// Release releases the snapshot to store.
	Release()
}

// Driver is the interface that must be implemented by a kv storage.
type Driver interface {
	// Open returns a new Storage.
	// The schema is the string for storage specific format.
	Open(schema string) (Storage, error)
}

// Storage defines the interface for storage.
// Isolation should be at least SI(SNAPSHOT ISOLATION)
type Storage interface {
	// Begin transaction
	Begin() (Transaction, error)
	// Close store
	Close() error
	// Storage's unique ID
	UUID() string
}

// FnKeyCmp is the function for iterator the keys
type FnKeyCmp func(key []byte) bool

// Iterator is the interface for a interator on KV store.
type Iterator interface {
	Next(FnKeyCmp) (Iterator, error)
	Value() []byte
	Key() string
	Valid() bool
	Close()
}

// IndexIterator is the interface for iterator of index data on KV store.
type IndexIterator interface {
	Next() (k []interface{}, h int64, err error)
	Close()
}

// Index is the interface for index data on KV store.
type Index interface {
	Create(txn Transaction, indexedValues []interface{}, h int64) error                          // supports insert into statement
	Delete(txn Transaction, indexedValues []interface{}, h int64) error                          // supports delete from statement
	Drop(txn Transaction) error                                                                  // supports drop table, drop index statements
	Seek(txn Transaction, indexedValues []interface{}) (iter IndexIterator, hit bool, err error) // supports where clause
	SeekFirst(txn Transaction) (iter IndexIterator, err error)                                   // supports aggregate min / ascending order by
}
