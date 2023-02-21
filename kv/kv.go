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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"bytes"
	"context"
	"crypto/tls"
	"time"

	"github.com/pingcap/errors"
	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/trxevents"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	"golang.org/x/exp/slices"
)

// UnCommitIndexKVFlag uses to indicate the index key/value is no need to commit.
// This is used in the situation of the index key/value was unchanged when do update.
// Usage:
// 1. For non-unique index: normally, the index value is '0'.
// Change the value to '1' indicate the index key/value is no need to commit.
// 2. For unique index: normally, the index value is the record handle ID, 8 bytes.
// Append UnCommitIndexKVFlag to the value indicate the index key/value is no need to commit.
const UnCommitIndexKVFlag byte = '1'

// Those limits is enforced to make sure the transaction can be well handled by TiKV.
var (
	// TxnEntrySizeLimit is limit of single entry size (len(key) + len(value)).
	TxnEntrySizeLimit uint64 = config.DefTxnEntrySizeLimit
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

// EmptyIterator is an iterator without any entry
type EmptyIterator struct{}

// Valid returns true if the current iterator is valid.
func (*EmptyIterator) Valid() bool { return false }

// Key returns the current key. Always return nil for this iterator
func (*EmptyIterator) Key() Key { return nil }

// Value returns the current value. Always return nil for this iterator
func (*EmptyIterator) Value() []byte { return nil }

// Next goes the next position. Always return error for this iterator
func (*EmptyIterator) Next() error { return errors.New("iterator is invalid") }

// Close closes the iterator.
func (*EmptyIterator) Close() {}

// EmptyRetriever is a retriever without any entry
type EmptyRetriever struct{}

// Get gets the value for key k from kv store. Always return nil for this retriever
func (*EmptyRetriever) Get(_ context.Context, _ Key) ([]byte, error) {
	return nil, ErrNotExist
}

// Iter creates an Iterator. Always return EmptyIterator for this retriever
func (*EmptyRetriever) Iter(_ Key, _ Key) (Iterator, error) { return &EmptyIterator{}, nil }

// IterReverse creates a reversed Iterator. Always return EmptyIterator for this retriever
func (*EmptyRetriever) IterReverse(_ Key) (Iterator, error) {
	return &EmptyIterator{}, nil
}

// Mutator is the interface wraps the basic Set and Delete methods.
type Mutator interface {
	// Set sets the value for key k as v into kv store.
	// v must NOT be nil or empty, otherwise it returns ErrCannotSetNilValue.
	Set(k Key, v []byte) error
	// Delete removes the entry for key k from kv store.
	Delete(k Key) error
}

// StagingHandle is the reference of a staging buffer.
type StagingHandle int

var (
	// InvalidStagingHandle is an invalid handler, MemBuffer will check handler to ensure safety.
	InvalidStagingHandle StagingHandle = 0
	// LastActiveStagingHandle is an special handler which always point to the last active staging buffer.
	LastActiveStagingHandle StagingHandle = -1
)

// RetrieverMutator is the interface that groups Retriever and Mutator interfaces.
type RetrieverMutator interface {
	Retriever
	Mutator
}

// MemBuffer is an in-memory kv collection, can be used to buffer write operations.
type MemBuffer interface {
	RetrieverMutator

	// RLock locks the MemBuffer for shared read.
	// In the most case, MemBuffer will only used by single goroutine,
	// but it will be read by multiple goroutine when combined with executor.UnionScanExec.
	// To avoid race introduced by executor.UnionScanExec, MemBuffer expose read lock for it.
	RLock()
	// RUnlock unlocks the MemBuffer.
	RUnlock()

	// GetFlags returns the latest flags associated with key.
	GetFlags(Key) (KeyFlags, error)
	// SetWithFlags put key-value into the last active staging buffer with the given KeyFlags.
	SetWithFlags(Key, []byte, ...FlagsOp) error
	// UpdateFlags updates the flags associated with key.
	UpdateFlags(Key, ...FlagsOp)
	// DeleteWithFlags delete key with the given KeyFlags
	DeleteWithFlags(Key, ...FlagsOp) error

	// Staging create a new staging buffer inside the MemBuffer.
	// Subsequent writes will be temporarily stored in this new staging buffer.
	// When you think all modifications looks good, you can call `Release` to public all of them to the upper level buffer.
	Staging() StagingHandle
	// Release publish all modifications in the latest staging buffer to upper level.
	Release(StagingHandle)
	// Cleanup cleanup the resources referenced by the StagingHandle.
	// If the changes are not published by `Release`, they will be discarded.
	Cleanup(StagingHandle)
	// InspectStage used to inspect the value updates in the given stage.
	InspectStage(StagingHandle, func(Key, KeyFlags, []byte))

	// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
	SnapshotGetter() Getter
	// SnapshotIter returns a Iterator for a snapshot of MemBuffer.
	SnapshotIter(k, upperbound Key) Iterator

	// Len returns the number of entries in the DB.
	Len() int

	// Size returns sum of keys and values length.
	Size() int

	// RemoveFromBuffer removes the entry from the buffer. It's used for testing.
	RemoveFromBuffer(Key)
}

// FindKeysInStage returns all keys in the given stage that satisfies the given condition.
func FindKeysInStage(m MemBuffer, h StagingHandle, predicate func(Key, KeyFlags, []byte) bool) []Key {
	result := make([]Key, 0)
	m.InspectStage(h, func(k Key, f KeyFlags, v []byte) {
		if predicate(k, f, v) {
			result = append(result, k)
		}
	})
	return result
}

// LockCtx contains information for LockKeys method.
type LockCtx = tikvstore.LockCtx

// Transaction defines the interface for operations inside a Transaction.
// This is not thread safe.
type Transaction interface {
	RetrieverMutator
	AssertionProto
	AggressiveLockingController
	// Size returns sum of keys and values length.
	Size() int
	// Mem returns the memory consumption of the transaction.
	Mem() uint64
	// SetMemoryFootprintChangeHook sets the hook that will be called when the memory footprint changes.
	SetMemoryFootprintChangeHook(func(uint64))
	// Len returns the number of entries in the DB.
	Len() int
	// Reset reset the Transaction to initial states.
	Reset()
	// Commit commits the transaction operations to KV store.
	Commit(context.Context) error
	// Rollback undoes the transaction operations to KV store.
	Rollback() error
	// String implements fmt.Stringer interface.
	String() string
	// LockKeys tries to lock the entries with the keys in KV store.
	// Will block until all keys are locked successfully or an error occurs.
	LockKeys(ctx context.Context, lockCtx *LockCtx, keys ...Key) error
	// LockKeysFunc tries to lock the entries with the keys in KV store.
	// Will block until all keys are locked successfully or an error occurs.
	// fn is called before LockKeys unlocks the keys.
	LockKeysFunc(ctx context.Context, lockCtx *LockCtx, fn func(), keys ...Key) error
	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option.
	SetOption(opt int, val interface{})
	// GetOption returns the option
	GetOption(opt int) interface{}
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
	SetVars(vars interface{})
	// GetVars gets variables from the transaction.
	GetVars() interface{}
	// BatchGet gets kv from the memory buffer of statement and transaction, and the kv storage.
	// Do not use len(value) == 0 or value == nil to represent non-exist.
	// If a key doesn't exist, there shouldn't be any corresponding entry in the result map.
	BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error)
	IsPessimistic() bool
	// CacheTableInfo caches the index name.
	// PresumeKeyNotExists will use this to help decode error message.
	CacheTableInfo(id int64, info *model.TableInfo)
	// GetTableInfo returns the cached index name.
	// If there is no such index already inserted through CacheIndexName, it will return UNKNOWN.
	GetTableInfo(id int64) *model.TableInfo

	// SetDiskFullOpt set allowed options of current operation in each TiKV disk usage level.
	SetDiskFullOpt(level kvrpcpb.DiskFullOpt)
	// ClearDiskFullOpt clear allowed flag
	ClearDiskFullOpt()

	// GetMemDBCheckpoint gets the transaction's memDB checkpoint.
	GetMemDBCheckpoint() *tikv.MemDBCheckpoint

	// RollbackMemDBToCheckpoint rollbacks the transaction's memDB to the specified checkpoint.
	RollbackMemDBToCheckpoint(*tikv.MemDBCheckpoint)

	// UpdateMemBufferFlags updates the flags of a node in the mem buffer.
	UpdateMemBufferFlags(key []byte, flags ...FlagsOp)
}

// AssertionProto is an interface defined for the assertion protocol.
type AssertionProto interface {
	// SetAssertion sets an assertion for an operation on the key.
	// TODO: Use a special type instead of `FlagsOp`. Otherwise there's risk that the assertion flag is incorrectly used
	// in other places like `MemBuffer.SetWithFlags`.
	SetAssertion(key []byte, assertion ...FlagsOp) error
}

// AggressiveLockingController is the interface that defines aggressive locking related operations.
type AggressiveLockingController interface {
	StartAggressiveLocking() error
	RetryAggressiveLocking(ctx context.Context) error
	CancelAggressiveLocking(ctx context.Context) error
	DoneAggressiveLocking(ctx context.Context) error
	IsInAggressiveLockingMode() bool
}

// Client is used to send request to KV layer.
type Client interface {
	// Send sends request to KV layer, returns a Response.
	Send(ctx context.Context, req *Request, vars interface{}, option *ClientSendOption) Response

	// IsRequestTypeSupported checks if reqType and subType is supported.
	IsRequestTypeSupported(reqType, subType int64) bool
}

// ClientSendOption wraps options during Client Send
type ClientSendOption struct {
	SessionMemTracker          *memory.Tracker
	EnabledRateLimitAction     bool
	EventCb                    trxevents.EventCallback
	EnableCollectExecutionInfo bool
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

// KeyRanges wrap the ranges for partitioned table cases.
// We might send ranges from different in the one request.
type KeyRanges struct {
	ranges        [][]KeyRange
	rowCountHints [][]int

	isPartitioned bool
}

// NewPartitionedKeyRanges constructs a new RequestRange for partitioned table.
func NewPartitionedKeyRanges(ranges [][]KeyRange) *KeyRanges {
	return NewPartitionedKeyRangesWithHints(ranges, nil)
}

// NewNonParitionedKeyRanges constructs a new RequestRange for a non partitioned table.
func NewNonParitionedKeyRanges(ranges []KeyRange) *KeyRanges {
	return NewNonParitionedKeyRangesWithHint(ranges, nil)
}

// NewPartitionedKeyRangesWithHints constructs a new RequestRange for partitioned table with row count hint.
func NewPartitionedKeyRangesWithHints(ranges [][]KeyRange, hints [][]int) *KeyRanges {
	return &KeyRanges{
		ranges:        ranges,
		rowCountHints: hints,
		isPartitioned: true,
	}
}

// NewNonParitionedKeyRangesWithHint constructs a new RequestRange for a non partitioned table with rou count hint.
func NewNonParitionedKeyRangesWithHint(ranges []KeyRange, hints []int) *KeyRanges {
	rr := &KeyRanges{
		ranges:        [][]KeyRange{ranges},
		isPartitioned: false,
	}
	if hints != nil {
		rr.rowCountHints = [][]int{hints}
	}
	return rr
}

// FirstPartitionRange returns the the result of first range.
// We may use some func to generate ranges for both partitioned table and non partitioned table.
// This method provides a way to fallback to non-partitioned ranges.
func (rr *KeyRanges) FirstPartitionRange() []KeyRange {
	if len(rr.ranges) == 0 {
		return []KeyRange{}
	}
	return rr.ranges[0]
}

// SetToNonPartitioned set the status to non-partitioned.
func (rr *KeyRanges) SetToNonPartitioned() error {
	if len(rr.ranges) > 1 {
		return errors.Errorf("you want to change the partitioned ranges to non-partitioned ranges")
	}
	rr.isPartitioned = false
	return nil
}

// AppendSelfTo appends itself to another slice.
func (rr *KeyRanges) AppendSelfTo(ranges []KeyRange) []KeyRange {
	for _, r := range rr.ranges {
		ranges = append(ranges, r...)
	}
	return ranges
}

// SortByFunc sorts each partition's ranges.
// Since the ranges are sorted in most cases, we check it first.
func (rr *KeyRanges) SortByFunc(sortFunc func(i, j KeyRange) bool) {
	if !slices.IsSortedFunc(rr.ranges, func(i, j []KeyRange) bool {
		// A simple short-circuit since the empty range actually won't make anything wrong.
		if len(i) == 0 || len(j) == 0 {
			return true
		}
		return sortFunc(i[0], j[0])
	}) {
		slices.SortFunc(rr.ranges, func(i, j []KeyRange) bool {
			if len(i) == 0 {
				return true
			}
			if len(j) == 0 {
				return false
			}
			return sortFunc(i[0], j[0])
		})
	}
	for i := range rr.ranges {
		if !slices.IsSortedFunc(rr.ranges[i], sortFunc) {
			slices.SortFunc(rr.ranges[i], sortFunc)
		}
	}
}

// ForEachPartitionWithErr runs the func for each partition with an error check.
func (rr *KeyRanges) ForEachPartitionWithErr(theFunc func([]KeyRange, []int) error) (err error) {
	for i := range rr.ranges {
		var hints []int
		if len(rr.rowCountHints) > i {
			hints = rr.rowCountHints[i]
		}
		err = theFunc(rr.ranges[i], hints)
		if err != nil {
			return err
		}
	}
	return nil
}

// ForEachPartition runs the func for each partition without error check.
func (rr *KeyRanges) ForEachPartition(theFunc func([]KeyRange)) {
	for i := range rr.ranges {
		theFunc(rr.ranges[i])
	}
}

// PartitionNum returns how many partition is involved in the ranges.
func (rr *KeyRanges) PartitionNum() int {
	return len(rr.ranges)
}

// IsFullySorted checks whether the ranges are sorted inside partition and each partition is also sorated.
func (rr *KeyRanges) IsFullySorted() bool {
	sortedByPartition := slices.IsSortedFunc(rr.ranges, func(i, j []KeyRange) bool {
		// A simple short-circuit since the empty range actually won't make anything wrong.
		if len(i) == 0 || len(j) == 0 {
			return true
		}
		return bytes.Compare(i[0].StartKey, j[0].StartKey) < 0
	})
	if !sortedByPartition {
		return false
	}
	for _, ranges := range rr.ranges {
		if !slices.IsSortedFunc(ranges, func(i, j KeyRange) bool {
			return bytes.Compare(i.StartKey, j.StartKey) < 0
		}) {
			return false
		}
	}
	return true
}

// TotalRangeNum returns how many ranges there are.
func (rr *KeyRanges) TotalRangeNum() int {
	ret := 0
	for _, r := range rr.ranges {
		ret += len(r)
	}
	return ret
}

// Request represents a kv request.
type Request struct {
	// Tp is the request type.
	Tp      int64
	StartTs uint64
	Data    []byte

	// KeyRanges makes sure that the request is sent first by partition then by region.
	// When the table is small, it's possible that multiple partitions are in the same region.
	KeyRanges *KeyRanges

	// For PartitionTableScan used by tiflash.
	PartitionIDAndRanges []PartitionIDAndRanges

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
	// TiDBServerID is the specified TiDB serverID to execute request. `0` means all TiDB instances.
	TiDBServerID uint64
	// TxnScope is the scope of the txn
	TxnScope string
	// ReadReplicaScope is the scope of the read replica.
	ReadReplicaScope string
	// IsStaleness indicates whether the request read staleness data
	IsStaleness bool
	// ClosestReplicaReadAdjuster used to adjust a copr request.
	ClosestReplicaReadAdjuster CoprRequestAdjuster
	// MatchStoreLabels indicates the labels the store should be matched
	MatchStoreLabels []*metapb.StoreLabel
	// ResourceGroupTagger indicates the kv request task group tagger.
	ResourceGroupTagger tikvrpc.ResourceGroupTagger
	// Paging indicates whether the request is a paging request.
	Paging struct {
		Enable bool
		// MinPagingSize is used when Paging is true.
		MinPagingSize uint64
		// MaxPagingSize is used when Paging is true.
		MaxPagingSize uint64
	}
	// RequestSource indicates whether the request is an internal request.
	RequestSource util.RequestSource
	// StoreBatchSize indicates the batch size of coprocessor in the same store.
	StoreBatchSize int
	// ResourceGroupName is the name of the bind resource group.
	ResourceGroupName string
	// LimitSize indicates whether the request is scan and limit
	LimitSize uint64
}

// CoprRequestAdjuster is used to check and adjust a copr request according to specific rules.
// return true if the request is changed.
type CoprRequestAdjuster func(*Request, int) bool

// PartitionIDAndRanges used by PartitionTableScan in tiflash.
type PartitionIDAndRanges struct {
	ID        int64
	KeyRanges []KeyRange
}

const (
	// GlobalReplicaScope indicates the default replica scope for tidb to request
	GlobalReplicaScope = oracle.GlobalTxnScope
)

// ResultSubset represents a result subset from a single storage unit.
// TODO: Find a better interface for ResultSubset that can reuse bytes.
type ResultSubset interface {
	// GetData gets the data.
	GetData() []byte
	// GetStartKey gets the start key.
	GetStartKey() Key
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
	SetOption(opt int, val interface{})
}

// SnapshotInterceptor is used to intercept snapshot's read operation
type SnapshotInterceptor interface {
	// OnGet intercepts Get operation for Snapshot
	OnGet(ctx context.Context, snap Snapshot, k Key) ([]byte, error)
	// OnBatchGet intercepts BatchGet operation for Snapshot
	OnBatchGet(ctx context.Context, snap Snapshot, keys []Key) (map[string][]byte, error)
	// OnIter intercepts Iter operation for Snapshot
	OnIter(snap Snapshot, k Key, upperBound Key) (Iterator, error)
	// OnIterReverse intercepts IterReverse operation for Snapshot
	OnIterReverse(snap Snapshot, k Key) (Iterator, error)
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
	// Begin a global transaction
	Begin(opts ...tikv.TxnOption) (Transaction, error)
	// GetSnapshot gets a snapshot that is able to read any data which data is <= ver.
	// if ver is MaxVersion or > current max committed version, we will use current version for this snapshot.
	GetSnapshot(ver Version) Snapshot
	// GetClient gets a client instance.
	GetClient() Client
	// GetMPPClient gets a mpp client instance.
	GetMPPClient() MPPClient
	// Close store
	Close() error
	// UUID return a unique ID which represents a Storage.
	UUID() string
	// CurrentVersion returns current max committed version with the given txnScope (local or global).
	CurrentVersion(txnScope string) (Version, error)
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
	// GetMemCache return memory manager of the storage.
	GetMemCache() MemManager
	// GetMinSafeTS return the minimal SafeTS of the storage with given txnScope.
	GetMinSafeTS(txnScope string) uint64
	// GetLockWaits return all lock wait information
	GetLockWaits() ([]*deadlockpb.WaitForEntry, error)
	// GetCodec gets the codec of the storage.
	GetCodec() tikv.Codec
}

// EtcdBackend is used for judging a storage is a real TiKV.
type EtcdBackend interface {
	EtcdAddrs() ([]string, error)
	TLSConfig() *tls.Config
	StartGCWorker() error
}

// StorageWithPD is used to get pd client.
type StorageWithPD interface {
	GetPDClient() pd.Client
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
	SplitRegions(ctx context.Context, splitKey [][]byte, scatter bool, tableID *int64) (regionID []uint64, err error)
	WaitScatterRegionFinish(ctx context.Context, regionID uint64, backOff int) error
	CheckRegionInScattering(regionID uint64) (bool, error)
}

// Priority value for transaction priority.
const (
	PriorityNormal = iota
	PriorityLow
	PriorityHigh
)

// IsoLevel is the transaction's isolation level.
type IsoLevel int

const (
	// SI stands for 'snapshot isolation'.
	SI IsoLevel = iota
	// RC stands for 'read committed'.
	RC
	// RCCheckTS stands for 'read consistency read with ts check'.
	RCCheckTS
)
