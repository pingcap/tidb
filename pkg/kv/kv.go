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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/tiflash"
	"github.com/pingcap/tidb/pkg/util/trxevents"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/atomic"
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
	TxnEntrySizeLimit = atomic.NewUint64(config.DefTxnEntrySizeLimit)
	// TxnTotalSizeLimit is limit of the sum of all entry size.
	TxnTotalSizeLimit = atomic.NewUint64(config.DefTxnTotalSizeLimit)
)

// ValueEntry represents the value entry stored in kv store.
type ValueEntry = tikvstore.ValueEntry

// NewValueEntry creates a ValueEntry.
func NewValueEntry(value []byte, commitTS uint64) ValueEntry {
	return tikvstore.NewValueEntry(value, commitTS)
}

// GetOption is the option for kv Get operation.
type GetOption = tikvstore.GetOption

// BatchGetOption is the option for kv BatchGet operation.
type BatchGetOption = tikvstore.BatchGetOption

// GetOptions is the options for kv Get operation.
type GetOptions = tikvstore.GetOptions

// BatchGetOptions is the options for kv BatchGet operation.
type BatchGetOptions = tikvstore.BatchGetOptions

// BatchGetToGetOptions converts []BatchGetOption to []GetOption.
func BatchGetToGetOptions(options []BatchGetOption) []GetOption {
	if len(options) == 0 {
		return nil
	}
	return tikvstore.BatchGetToGetOptions(options)
}

// WithReturnCommitTS is used to indicate that the returned value should contain commit ts.
func WithReturnCommitTS() tikvstore.GetOrBatchGetOption {
	return tikvstore.WithReturnCommitTS()
}

// Getter is the interface for the Get method.
type Getter interface {
	// Get gets the value for key k from kv store.
	// If corresponding kv pair does not exist, it returns nil and ErrNotExist.
	// The returned ValueEntry contains both value and some extra meta such as `CommitTS`.
	// The `CommitTS` is 0 by default, indicating that the commit timestamp is unknown,
	// if you need it, please set the option `WithReturnCommitTS`.
	Get(ctx context.Context, k Key, options ...GetOption) (ValueEntry, error)
}

// GetValue gets the value for key k from kv store.
func GetValue(ctx context.Context, getter Getter, k Key) (value []byte, _ error) {
	entry, err := getter.Get(ctx, k)
	return entry.Value, err
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
	// It yields only keys that >= lowerBound. If lowerBound is nil, it means the lowerBound is unbounded.
	IterReverse(k, lowerBound Key) (Iterator, error)
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
func (*EmptyRetriever) Get(_ context.Context, _ Key, _ ...GetOption) (ValueEntry, error) {
	return ValueEntry{}, ErrNotExist
}

// Iter creates an Iterator. Always return EmptyIterator for this retriever
func (*EmptyRetriever) Iter(_ Key, _ Key) (Iterator, error) { return &EmptyIterator{}, nil }

// IterReverse creates a reversed Iterator. Always return EmptyIterator for this retriever
func (*EmptyRetriever) IterReverse(_ Key, _ Key) (Iterator, error) {
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
	InvalidStagingHandle StagingHandle
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
	// UpdateAssertionFlags updates the assertion flags associated with key.
	UpdateAssertionFlags(Key, AssertionOp)
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
	// SnapshotIterReverse returns a reverse Iterator for a snapshot of MemBuffer.
	SnapshotIterReverse(k, lowerBound Key) Iterator

	// Len returns the number of entries in the DB.
	Len() int

	// Size returns sum of keys and values length.
	Size() int

	// RemoveFromBuffer removes the entry from the buffer. It's used for testing.
	RemoveFromBuffer(Key)

	// GetLocal checks if the key exists in the buffer in local memory.
	GetLocal(context.Context, []byte) ([]byte, error)

	// BatchGet gets values from the memory buffer.
	// The returned `ValueEntry.CommitTS` is always 0 because it is not committed yet.
	BatchGet(ctx context.Context, keys [][]byte, options ...BatchGetOption) (map[string]ValueEntry, error)
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
	FairLockingController
	// Size returns sum of keys and values length.
	Size() int
	// Mem returns the memory consumption of the transaction.
	Mem() uint64
	// SetMemoryFootprintChangeHook sets the hook that will be called when the memory footprint changes.
	SetMemoryFootprintChangeHook(func(uint64))
	// MemHookSet returns whether the memory footprint change hook is set.
	MemHookSet() bool
	// Len returns the number of entries in the DB.
	Len() int
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
	SetOption(opt int, val any)
	// GetOption returns the option
	GetOption(opt int) any
	// IsReadOnly checks if the transaction has only performed read operations.
	IsReadOnly() bool
	// StartTS returns the transaction start timestamp.
	StartTS() uint64
	// CommitTS returns the transaction commit timestamp if it is committed; otherwise it returns 0.
	CommitTS() uint64
	// Valid returns if the transaction is valid.
	// A transaction become invalid after commit or rollback.
	Valid() bool
	// GetMemBuffer return the MemBuffer binding to this transaction.
	GetMemBuffer() MemBuffer
	// GetSnapshot returns the Snapshot binding to this transaction.
	GetSnapshot() Snapshot
	// SetVars sets variables to the transaction.
	SetVars(vars any)
	// GetVars gets variables from the transaction.
	GetVars() any
	// BatchGet gets kv from the memory buffer of statement and transaction, and the kv storage.
	// Do not use len(value) == 0 or value == nil to represent non-exist.
	// If a key doesn't exist, there shouldn't be any corresponding entry in the result map.
	// The returned ValueEntry contains both value and some extra meta such as `CommitTS`.
	// The `CommitTS` is 0 by default, indicating that the commit timestamp is unknown,
	// if you need it, please set the option `WithReturnCommitTS`.
	BatchGet(ctx context.Context, keys []Key, options ...BatchGetOption) (map[string]ValueEntry, error)
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
	// IsPipelined returns whether the transaction is used for pipelined DML.
	IsPipelined() bool
	// MayFlush flush the pipelined memdb if the keys or size exceeds threshold, no effect for standard DML.
	MayFlush() error
}

// FairLockingController is the interface that defines fair locking related operations.
type FairLockingController interface {
	StartFairLocking() error
	RetryFairLocking(ctx context.Context) error
	CancelFairLocking(ctx context.Context) error
	DoneFairLocking(ctx context.Context) error
	IsInFairLockingMode() bool
}

// Client is used to send request to KV layer.
type Client interface {
	// Send sends request to KV layer, returns a Response.
	Send(ctx context.Context, req *Request, vars any, option *ClientSendOption) Response

	// IsRequestTypeSupported checks if reqType and subType is supported.
	IsRequestTypeSupported(reqType, subType int64) bool
}

// ClientSendOption wraps options during Client Send
type ClientSendOption struct {
	SessionMemTracker          *memory.Tracker
	EnabledRateLimitAction     bool
	EventCb                    trxevents.EventCallback
	EnableCollectExecutionInfo bool
	TiFlashReplicaRead         tiflash.ReplicaRead
	AppendWarning              func(warn error)
	TryCopLiteWorker           *atomic.Uint32
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

// StoreType represents the type of storage engine.
type StoreType uint8

const (
	// TiKV means the type of store engine is TiKV.
	TiKV StoreType = iota
	// TiFlash means the type of store engine is TiFlash.
	TiFlash
	// TiDB means the type of store engine is TiDB.
	// used to read memory data from other instances to have a global view of the
	// data, such as for information_schema.cluster_slow_query.
	TiDB
	// UnSpecified means the store engine type is unknown
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
