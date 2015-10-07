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
	"bytes"
	"math"

	"github.com/juju/errors"
)

// EncodedKey represents encoded key in low-level storage engine.
type EncodedKey []byte

// Key represents high-level Key type.
type Key []byte

// Next returns the next key in byte-order.
func (k Key) Next() Key {
	// add \x0 to the end of key
	buf := make([]byte, len([]byte(k))+1)
	copy(buf, []byte(k))
	return buf
}

// Cmp returns the comparison result of two key.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (k Key) Cmp(another Key) int {
	return bytes.Compare(k, another)
}

// Cmp returns the comparison result of two key.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (k EncodedKey) Cmp(another EncodedKey) int {
	return bytes.Compare(k, another)
}

// Next returns the next key in byte-order.
func (k EncodedKey) Next() EncodedKey {
	return EncodedKey(bytes.Join([][]byte{k, Key{0}}, nil))
}

// VersionProvider provides increasing IDs.
type VersionProvider interface {
	CurrentVersion() (Version, error)
}

// Version is the wrapper of KV's version.
type Version struct {
	Ver uint64
}

var (
	// MaxVersion is the maximum version, notice that it's not a valid version.
	MaxVersion = Version{Ver: math.MaxUint64}
	// MinVersion is the minimum version, it's not a valid version, too.
	MinVersion = Version{Ver: 0}
)

// NewVersion creates a new Version struct.
func NewVersion(v uint64) Version {
	return Version{
		Ver: v,
	}
}

// Cmp returns the comparison result of two versions.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (v Version) Cmp(another Version) int {
	if v.Ver > another.Ver {
		return 1
	} else if v.Ver < another.Ver {
		return -1
	}
	return 0
}

// DecodeFn is a function that decode data after fetch from store.
type DecodeFn func(raw interface{}) (interface{}, error)

// EncodeFn is a function that encode data before put into store.
type EncodeFn func(raw interface{}) (interface{}, error)

// ErrNotCommitted is the error returned by CommitVersion when the this
// transaction is not committed.
var ErrNotCommitted = errors.New("this transaction is not committed")

// Transaction defines the interface for operations inside a Transaction.
// This is not thread safe.
type Transaction interface {
	// Get gets the value for key k from KV store.
	Get(k Key) ([]byte, error)
	// Set sets the value for key k as v into KV store.
	Set(k Key, v []byte) error
	// Seek searches for the entry with key k in KV store.
	Seek(k Key, fnKeyCmp func(key Key) bool) (Iterator, error)
	// Inc increases the value for key k in KV store by step.
	Inc(k Key, step int64) (int64, error)
	// GetInt64 get int64 which created by Inc method.
	GetInt64(k Key) (int64, error)
	// Deletes removes the entry for key k from KV store.
	Delete(k Key) error
	// Commit commites the transaction operations to KV store.
	Commit() error
	// CommittedVersion returns the verion of this committed transaction. If this
	// transaction has not been committed, returns ErrNotCommitted error.
	CommittedVersion() (Version, error)
	// Rollback undoes the transaction operations to KV store.
	Rollback() error
	// String implements Stringer.String() interface.
	String() string
	// LockKeys tries to lock the entries with the keys in KV store.
	LockKeys(keys ...Key) error
}

// MvccSnapshot is used to get/seek a specific verion in a snaphot.
type MvccSnapshot interface {
	// MvccGet returns the specific version of given key, if the version doesn't
	// exist, returns the nearest(lower) version's data.
	MvccGet(k Key, ver Version) ([]byte, error)
	// MvccIterator seeks to the key in the specific version's snapshot, if the
	// version doesn't exist, returns the nearest(lower) version's snaphot.
	NewMvccIterator(k Key, ver Version) Iterator
	// Release releases this snapshot.
	MvccRelease()
}

// Snapshot defines the interface for the snapshot fetched from KV store.
type Snapshot interface {
	// Get gets the value for key k from snapshot.
	Get(k Key) ([]byte, error)
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
	// GetSnapshot gets a snaphot that is able to read any version of data.
	GetSnapshot() (MvccSnapshot, error)
	// Close store
	Close() error
	// Storage's unique ID
	UUID() string
}

// FnKeyCmp is the function for iterator the keys
type FnKeyCmp func(key Key) bool

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
