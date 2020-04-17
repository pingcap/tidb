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
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/memory"
)

// Transaction options
const (
	// PresumeKeyNotExists indicates that when dealing with a Get operation but failing to read data from cache,
	// we presume that the key does not exist in Store. The actual existence will be checked before the
	// transaction's commit.
	// This option is an optimization for frequent checks during a transaction, e.g. batch inserts.
	PresumeKeyNotExists Option = iota + 1
	// PresumeKeyNotExistsError is the option key for error.
	// When PresumeKeyNotExists is set and condition is not match, should throw the error.
	PresumeKeyNotExistsError
	// BinlogInfo contains the binlog data and client.
	BinlogInfo
	// SchemaChecker is used for checking schema-validity.
	SchemaChecker
	// IsolationLevel sets isolation level for current transaction. The default level is SI.
	IsolationLevel
	// Priority marks the priority of this transaction.
	Priority
	// NotFillCache makes this request do not touch the LRU cache of the underlying storage.
	NotFillCache
	// SyncLog decides whether the WAL(write-ahead log) of this request should be synchronized.
	SyncLog
	// KeyOnly retrieve only keys, it can be used in scan now.
	KeyOnly
	// Pessimistic is defined for pessimistic lock
	Pessimistic
	// SnapshotTS is defined to set snapshot ts.
	SnapshotTS
	// Set replica read
	ReplicaRead
)

// Priority value for transaction priority.
const (
	PriorityNormal = iota
	PriorityLow
	PriorityHigh
)

// UnCommitIndexKVFlag uses to indicate the index key/value is no need to commit.
// This is used in the situation of the index key/value was unchanged when do update.
// Usage:
// 1. For non-unique index: normally, the index value is '0'.
// Change the value to '1' indicate the index key/value is no need to commit.
// 2. For unique index: normally, the index value is the record handle ID, 8 bytes.
// Append UnCommitIndexKVFlag to the value indicate the index key/value is no need to commit.
const UnCommitIndexKVFlag byte = '1'

// MaxTxnTimeUse is the max time a Txn may use (in ms) from its begin to commit.
// We use it to abort the transaction to guarantee GC worker will not influence it.
const MaxTxnTimeUse = 24 * 60 * 60 * 1000

// IsoLevel is the transaction's isolation level.
type IsoLevel int

const (
	// SI stands for 'snapshot isolation'.
	SI IsoLevel = iota
	// RC stands for 'read committed'.
	RC
)

// ReplicaReadType is the type of replica to read data from
type ReplicaReadType byte

const (
	// ReplicaReadLeader stands for 'read from leader'.
	ReplicaReadLeader ReplicaReadType = 1 << iota
	// ReplicaReadFollower stands for 'read from follower'.
	ReplicaReadFollower
	// ReplicaReadMixed stands for 'read from leader and follower and learner'.
	ReplicaReadMixed
)

// IsFollowerRead checks if leader is going to be used to read data.
func (r ReplicaReadType) IsFollowerRead() bool {
	// In some cases the default value is 0, which should be treated as `ReplicaReadLeader`.
	return r != ReplicaReadLeader && r != 0
}

// Those limits is enforced to make sure the transaction can be well handled by TiKV.
var (
	// TxnEntrySizeLimit is limit of single entry size (len(key) + len(value)).
	TxnEntrySizeLimit = 6 * 1024 * 1024
	// TxnTotalSizeLimit is limit of the sum of all entry size.
	TxnTotalSizeLimit uint64 = config.DefTxnTotalSizeLimit
)

// Getter is the interface for the Get method.
type Getter interface {
	// Get gets the value for key k from kv store.
	// If corresponding kv pair does not exist, it returns nil and ErrNotExist.
	Get(ctx context.Context, k Key) ([]byte, error)
}

// Retriever is the interface wraps the basic Get and Seek methods.
type Retriever interface {
	Getter
	// Iter creates an Iterator positioned on the first entry that k <= entry's key.
	// If such entry is not found, it returns an invalid Iterator with no error.
	// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
	// The Iterator must be Closed after use.
	Iter(k Key, upperBound Key) (Iterator, error)

	// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
	// The returned iterator will iterate from greater key to smaller key.
	// If k is nil, the returned iterator will be positioned at the last key.
	// TODO: Add lower bound limit
	IterReverse(k Key) (Iterator, error)
}

// Mutator is the interface wraps the basic Set and Delete methods.
type Mutator interface {
	// Set sets the value for key k as v into kv store.
	// v must NOT be nil or empty, otherwise it returns ErrCannotSetNilValue.
	Set(k Key, v []byte) error
	// Delete removes the entry for key k from kv store.
	Delete(k Key) error
}

// RetrieverMutator is the interface that groups Retriever and Mutator interfaces.
type RetrieverMutator interface {
	Retriever
	Mutator
}

// MemBuffer is an in-memory kv collection, can be used to buffer write operations.
type MemBuffer interface {
	RetrieverMutator
	// Size returns sum of keys and values length.
	Size() int
	// Len returns the number of entries in the DB.
	Len() int
	// NewStagingBuffer returns a new write buffer,
	// modifications in the returned buffer will not influence this buffer
	// until you call Flush, or you can use Discard to discard all of them.
	//
	// Note: you cannot modify this MemBuffer until the child buffer finished,
	// otherwise the Set operation will panic.
	NewStagingBuffer() MemBuffer
	// Flush flushes all kvs in this buffer to parrent buffer.
	Flush() (int, error)
	// Discard discads all kvs in this buffer.
	Discard()
}

// Transaction defines the interface for operations inside a Transaction.
// This is not thread safe.
type Transaction interface {
	MemBuffer
	// Commit commits the transaction operations to KV store.
	Commit(context.Context) error
	// Rollback undoes the transaction operations to KV store.
	Rollback() error
	// String implements fmt.Stringer interface.
	String() string
	// LockKeys tries to lock the entries with the keys in KV store.
	LockKeys(ctx context.Context, lockCtx *LockCtx, keys ...Key) error
	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option.
	SetOption(opt Option, val interface{})
	// DelOption deletes an option.
	DelOption(opt Option)
	// IsReadOnly checks if the transaction has only performed read operations.
	IsReadOnly() bool
	// StartTS returns the transaction start timestamp.
	StartTS() uint64
	// Valid returns if the transaction is valid.
	// A transaction become invalid after commit or rollback.
	Valid() bool
	// GetMemBuffer return the MemBuffer binding to this transaction.
	GetMemBuffer() MemBuffer
	// GetSnapshot returns the Snapshot binding to this transaction.
	GetSnapshot() Snapshot
	// SetVars sets variables to the transaction.
	SetVars(vars *Variables)
	// BatchGet gets kv from the memory buffer of statement and transaction, and the kv storage.
	// Do not use len(value) == 0 or value == nil to represent non-exist.
	// If a key doesn't exist, there shouldn't be any corresponding entry in the result map.
	BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error)
	IsPessimistic() bool
}

// LockCtx contains information for LockKeys method.
type LockCtx struct {
	Killed                *uint32
	ForUpdateTS           uint64
	LockWaitTime          int64
	WaitStartTime         time.Time
	PessimisticLockWaited *int32
	LockKeysDuration      *time.Duration
	LockKeysCount         *int32
	ReturnValues          bool
	Values                map[string]ReturnedValue
	ValuesLock            sync.Mutex
	LockExpired           *uint32
}

// ReturnedValue pairs the Value and AlreadyLocked flag for PessimisticLock return values result.
type ReturnedValue struct {
	Value         []byte
	AlreadyLocked bool
}

// Client is used to send request to KV layer.
type Client interface {
	// Send sends request to KV layer, returns a Response.
	Send(ctx context.Context, req *Request, vars *Variables) Response

	// IsRequestTypeSupported checks if reqType and subType is supported.
	IsRequestTypeSupported(reqType, subType int64) bool
}

// ReqTypes.
const (
	ReqTypeSelect   = 101
	ReqTypeIndex    = 102
	ReqTypeDAG      = 103
	ReqTypeAnalyze  = 104
	ReqTypeChecksum = 105

	ReqSubTypeBasic      = 0
	ReqSubTypeDesc       = 10000
	ReqSubTypeGroupBy    = 10001
	ReqSubTypeTopN       = 10002
	ReqSubTypeSignature  = 10003
	ReqSubTypeAnalyzeIdx = 10004
	ReqSubTypeAnalyzeCol = 10005
)

// StoreType represents the type of a store.
type StoreType uint8

const (
	// TiKV means the type of a store is TiKV.
	TiKV StoreType = iota
	// TiFlash means the type of a store is TiFlash.
	TiFlash
	// TiDB means the type of a store is TiDB.
	TiDB
	// UnSpecified means the store type is unknown
	UnSpecified = 255
)

// Name returns the name of store type.
func (t StoreType) Name() string {
	if t == TiFlash {
		return "tiflash"
	} else if t == TiDB {
		return "tidb"
	} else if t == TiKV {
		return "tikv"
	}
	return "unspecified"
}

// Request represents a kv request.
type Request struct {
	// Tp is the request type.
	Tp        int64
	StartTs   uint64
	Data      []byte
	KeyRanges []KeyRange

	// Concurrency is 1, if it only sends the request to a single storage unit when
	// ResponseIterator.Next is called. If concurrency is greater than 1, the request will be
	// sent to multiple storage units concurrently.
	Concurrency int
	// IsolationLevel is the isolation level, default is SI.
	IsolationLevel IsoLevel
	// Priority is the priority of this KV request, its value may be PriorityNormal/PriorityLow/PriorityHigh.
	Priority int
	// memTracker is used to trace and control memory usage in co-processor layer.
	MemTracker *memory.Tracker
	// KeepOrder is true, if the response should be returned in order.
	KeepOrder bool
	// Desc is true, if the request is sent in descending order.
	Desc bool
	// NotFillCache makes this request do not touch the LRU cache of the underlying storage.
	NotFillCache bool
	// SyncLog decides whether the WAL(write-ahead log) of this request should be synchronized.
	SyncLog bool
	// Streaming indicates using streaming API for this request, result in that one Next()
	// call would not corresponds to a whole region result.
	Streaming bool
	// ReplicaRead is used for reading data from replicas, only follower is supported at this time.
	ReplicaRead ReplicaReadType
	// StoreType represents this request is sent to the which type of store.
	StoreType StoreType
	// Cacheable is true if the request can be cached. Currently only deterministic DAG requests can be cached.
	Cacheable bool
	// SchemaVer is for any schema-ful storage to validate schema correctness if necessary.
	SchemaVar int64
	// BatchCop indicates whether send batch coprocessor request to tiflash.
	BatchCop bool
	// TaskID is an unique ID for an execution of a statement
	TaskID uint64
}

// ResultSubset represents a result subset from a single storage unit.
// TODO: Find a better interface for ResultSubset that can reuse bytes.
type ResultSubset interface {
	// GetData gets the data.
	GetData() []byte
	// GetStartKey gets the start key.
	GetStartKey() Key
	// GetExecDetails gets the detail information.
	GetExecDetails() *execdetails.ExecDetails
	// MemSize returns how many bytes of memory this result use for tracing memory usage.
	MemSize() int64
	// RespTime returns the response time for the request.
	RespTime() time.Duration
}

// Response represents the response returned from KV layer.
type Response interface {
	// Next returns a resultSubset from a single storage unit.
	// When full result set is returned, nil is returned.
	Next(ctx context.Context) (resultSubset ResultSubset, err error)
	// Close response.
	Close() error
}

// Snapshot defines the interface for the snapshot fetched from KV store.
type Snapshot interface {
	Retriever
	// BatchGet gets a batch of values from snapshot.
	BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error)
	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option. Only ReplicaRead is supported for snapshot
	SetOption(opt Option, val interface{})
	// DelOption deletes an option.
	DelOption(opt Option)
}

// BatchGetter is the interface for BatchGet.
type BatchGetter interface {
	// BatchGet gets a batch of values.
	BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error)
}

// Driver is the interface that must be implemented by a KV storage.
type Driver interface {
	// Open returns a new Storage.
	// The path is the string for storage specific format.
	Open(path string) (Storage, error)
}

// Storage defines the interface for storage.
// Isolation should be at least SI(SNAPSHOT ISOLATION)
type Storage interface {
	// Begin transaction
	Begin() (Transaction, error)
	// BeginWithStartTS begins transaction with startTS.
	BeginWithStartTS(startTS uint64) (Transaction, error)
	// GetSnapshot gets a snapshot that is able to read any data which data is <= ver.
	// if ver is MaxVersion or > current max committed version, we will use current version for this snapshot.
	GetSnapshot(ver Version) (Snapshot, error)
	// GetClient gets a client instance.
	GetClient() Client
	// Close store
	Close() error
	// UUID return a unique ID which represents a Storage.
	UUID() string
	// CurrentVersion returns current max committed version.
	CurrentVersion() (Version, error)
	// GetOracle gets a timestamp oracle client.
	GetOracle() oracle.Oracle
	// SupportDeleteRange gets the storage support delete range or not.
	SupportDeleteRange() (supported bool)
	// Name gets the name of the storage engine
	Name() string
	// Describe returns of brief introduction of the storage
	Describe() string
	// ShowStatus returns the specified status of the storage
	ShowStatus(ctx context.Context, key string) (interface{}, error)
}

// FnKeyCmp is the function for iterator the keys
type FnKeyCmp func(key Key) bool

// Iterator is the interface for a iterator on KV store.
type Iterator interface {
	Valid() bool
	Key() Key
	Value() []byte
	Next() error
	Close()
}

// SplittableStore is the kv store which supports split regions.
type SplittableStore interface {
	SplitRegions(ctx context.Context, splitKey [][]byte, scatter bool) (regionID []uint64, err error)
	WaitScatterRegionFinish(regionID uint64, backOff int) error
	CheckRegionInScattering(regionID uint64) (bool, error)
}

// Used for pessimistic lock wait time
// these two constants are special for lock protocol with tikv
// 0 means always wait, -1 means nowait, others meaning lock wait in milliseconds
var (
	LockAlwaysWait = int64(0)
	LockNoWait     = int64(-1)
)
