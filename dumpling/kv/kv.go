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

// Key represents high-level Key type.
type Key []byte

// Next returns the next key in byte-order.
func (k Key) Next() Key {
	// add 0x0 to the end of key
	buf := make([]byte, len([]byte(k))+1)
	copy(buf, []byte(k))
	return buf
}

// Cmp returns the comparison result of two key.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (k Key) Cmp(another Key) int {
	return bytes.Compare(k, another)
}

// EncodedKey represents encoded key in low-level storage engine.
type EncodedKey []byte

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

// DecodeFn is a function that decodes data after fetching from store.
type DecodeFn func(raw interface{}) (interface{}, error)

// EncodeFn is a function that encodes data before putting into store.
type EncodeFn func(raw interface{}) (interface{}, error)

// ErrNotCommitted is the error returned by CommitVersion when this
// transaction is not committed.
var ErrNotCommitted = errors.New("this transaction is not committed")

// Option is used for customizing kv store's behaviors during a transaction.
type Option int

// Options is an interface of a set of options. Each option is associated with a value.
type Options interface {
	// Get gets an option value.
	Get(opt Option) (v interface{}, ok bool)
}

const (
	// RangePrefetchOnCacheMiss directives that when dealing with a Get operation but failing to read data from cache,
	// it will launch a RangePrefetch to underlying storage instead of Get. The range starts from requested key and
	// has a limit of the option value. The feature is disabled if option value <= 0 or value type is not int.
	// This option is particularly useful when we have to do sequential Gets, e.g. table scans.
	RangePrefetchOnCacheMiss Option = iota + 1

	// PresumeKeyNotExists directives that when dealing with a Get operation but failing to read data from cache,
	// we presume that the key does not exist in Store. The actual existence will be checked before the
	// transaction's commit.
	// This option is an optimization for frequent checks during a transaction, e.g. batch inserts.
	PresumeKeyNotExists
)

// Transaction defines the interface for operations inside a Transaction.
// This is not thread safe.
type Transaction interface {
	// Get gets the value for key k from KV store.
	Get(k Key) ([]byte, error)
	// BatchPrefetch fetches values from KV storage to cache for later use.
	BatchPrefetch(keys []Key) error
	// RangePrefetch fetches values in the range [start, end] from KV storage
	// to cache for later use. Maximum number of values is up to limit.
	RangePrefetch(start, end Key, limit int) error
	// Set sets the value for key k as v into KV store.
	Set(k Key, v []byte) error
	// Seek searches for the entry with key k in KV store.
	Seek(k Key) (Iterator, error)
	// Inc increases the value for key k in KV store by step.
	Inc(k Key, step int64) (int64, error)
	// GetInt64 get int64 which created by Inc method.
	GetInt64(k Key) (int64, error)
	// Delete removes the entry for key k from KV store.
	Delete(k Key) error
	// Commit commits the transaction operations to KV store.
	Commit() error
	// CommittedVersion returns the version of this committed transaction. If this
	// transaction has not been committed, returns ErrNotCommitted error.
	CommittedVersion() (Version, error)
	// Rollback undoes the transaction operations to KV store.
	Rollback() error
	// String implements fmt.Stringer interface.
	String() string
	// LockKeys tries to lock the entries with the keys in KV store.
	LockKeys(keys ...Key) error
	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option.
	SetOption(opt Option, val interface{})
	// DelOption deletes an option.
	DelOption(opt Option)
}

// MvccSnapshot is used to get/seek a specific version in a snapshot.
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
	// BatchGet gets a batch of values from snapshot.
	BatchGet(keys []Key) (map[string][]byte, error)
	// RangeGet gets values in the range [start, end] from snapshot. Maximum
	// number of values is up to limit.
	RangeGet(start, end Key, limit int) (map[string][]byte, error)
	// NewIterator gets a new iterator on the snapshot.
	NewIterator(param interface{}) Iterator
	// Release releases the snapshot to store.
	Release()
}

// MemBuffer is the interface for transaction buffer of update in a transaction
type MemBuffer interface {
	// Get gets the value for key k from buffer. If not found, it returns ErrNotExist.
	Get(k Key) ([]byte, error)
	// Set associates key with value
	Set([]byte, []byte) error
	// NewIterator gets a new iterator on the buffer.
	NewIterator(param interface{}) Iterator
	// Release releases the buffer.
	Release()
}

// Driver is the interface that must be implemented by a KV storage.
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
	// GetSnapshot gets a snaphot that is able to read any data which data is <= ver.
	// if ver is MaxVersion or > current max committed version, we will use current version for this snapshot.
	GetSnapshot(ver Version) (MvccSnapshot, error)
	// Close store
	Close() error
	// Storage's unique ID
	UUID() string
	// CurrentVersion returns current max committed version.
	CurrentVersion() (Version, error)
}

// FnKeyCmp is the function for iterator the keys
type FnKeyCmp func(key Key) bool

// Iterator is the interface for a interator on KV store.
type Iterator interface {
	Next() error
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
	Exist(txn Transaction, indexedValues []interface{}, h int64) (bool, int64, error)            // supports check index exist
	GenIndexKey(indexedValues []interface{}, h int64) (key []byte, distinct bool, err error)     // supports index check
	Seek(txn Transaction, indexedValues []interface{}) (iter IndexIterator, hit bool, err error) // supports where clause
	SeekFirst(txn Transaction) (iter IndexIterator, err error)                                   // supports aggregate min / ascending order by
}
