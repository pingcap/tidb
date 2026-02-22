// Copyright 2016 PingCAP, Inc.
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
	"crypto/tls"
	"time"

	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/metapb"
	pdhttp "github.com/tikv/pd/client/http"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	pd "github.com/tikv/pd/client"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/resourcegrouptag"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
)

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
	ResourceGroupTagger *ResourceGroupTagBuilder
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
	// StoreBusyThreshold is the threshold for the store to return ServerIsBusy
	StoreBusyThreshold time.Duration
	// TiKVClientReadTimeout is the timeout of kv read request
	TiKVClientReadTimeout uint64
	// MaxExecutionTime is the timeout of the whole query execution
	MaxExecutionTime uint64

	RunawayChecker resourcegroup.RunawayChecker

	// ConnID stores the session connection id.
	ConnID uint64
	// ConnAlias stores the session connection alias.
	ConnAlias string
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
	BatchGet(ctx context.Context, keys []Key, options ...BatchGetOption) (map[string]ValueEntry, error)
	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option. Only ReplicaRead is supported for snapshot
	SetOption(opt int, val any)
}

// SnapshotInterceptor is used to intercept snapshot's read operation
type SnapshotInterceptor interface {
	// OnGet intercepts Get operation for Snapshot
	OnGet(ctx context.Context, snap Snapshot, k Key, options ...GetOption) (ValueEntry, error)
	// OnBatchGet intercepts BatchGet operation for Snapshot
	OnBatchGet(ctx context.Context, snap Snapshot, keys []Key, options ...BatchGetOption) (map[string]ValueEntry, error)
	// OnIter intercepts Iter operation for Snapshot
	OnIter(snap Snapshot, k Key, upperBound Key) (Iterator, error)
	// OnIterReverse intercepts IterReverse operation for Snapshot
	OnIterReverse(snap Snapshot, k Key, lowerBound Key) (Iterator, error)
}

// BatchGetter is the interface for BatchGet.
type BatchGetter interface {
	// BatchGet gets a batch of values.
	// The returned ValueEntry contains both value and some extra meta such as `CommitTS`.
	// The `CommitTS` is 0 by default, indicating that the commit timestamp is unknown,
	// if you need it, please set the option `WithReturnCommitTS`.
	BatchGet(ctx context.Context, keys []Key, options ...BatchGetOption) (map[string]ValueEntry, error)
}

// BatchGetValue gets a batch of values from BatchGetter.
func BatchGetValue(ctx context.Context, getter BatchGetter, keys []Key) (map[string][]byte, error) {
	entries, err := getter.BatchGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	values := make(map[string][]byte, len(entries))
	for k, entry := range entries {
		values[k] = entry.Value
	}
	return values, nil
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
	ShowStatus(ctx context.Context, key string) (any, error)
	// GetMemCache return memory manager of the storage.
	GetMemCache() MemManager
	// GetMinSafeTS return the minimal SafeTS of the storage with given txnScope.
	GetMinSafeTS(txnScope string) uint64
	// GetLockWaits return all lock wait information
	GetLockWaits() ([]*deadlockpb.WaitForEntry, error)
	// GetCodec gets the codec of the storage.
	GetCodec() tikv.Codec
	// SetOption is a thin wrapper around sync.Map.
	SetOption(k any, v any)
	// GetOption is a thin wrapper around sync.Map.
	GetOption(k any) (any, bool)
	// GetClusterID returns the physical cluster ID of the storage.
	// for nextgen, all keyspace in the storage share the same cluster ID.
	GetClusterID() uint64
	// GetKeyspace returns the keyspace name of the storage.
	GetKeyspace() string
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
	GetPDHTTPClient() pdhttp.Client
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

// ResourceGroupTagBuilder is used to build the resource group tag for a kv request.
type ResourceGroupTagBuilder struct {
	sqlDigest    *parser.Digest
	planDigest   *parser.Digest
	keyspaceName []byte
}

// NewResourceGroupTagBuilder creates a new ResourceGroupTagBuilder.
func NewResourceGroupTagBuilder(keyspaceName []byte) *ResourceGroupTagBuilder {
	return &ResourceGroupTagBuilder{keyspaceName: keyspaceName}
}

// SetSQLDigest sets the sql digest for the request.
func (b *ResourceGroupTagBuilder) SetSQLDigest(digest *parser.Digest) *ResourceGroupTagBuilder {
	b.sqlDigest = digest
	return b
}

// SetPlanDigest sets the plan digest for the request.
func (b *ResourceGroupTagBuilder) SetPlanDigest(digest *parser.Digest) *ResourceGroupTagBuilder {
	b.planDigest = digest
	return b
}

// BuildProtoTagger sets the access key for the request.
func (b *ResourceGroupTagBuilder) BuildProtoTagger() tikvrpc.ResourceGroupTagger {
	return func(req *tikvrpc.Request) {
		b.Build(req)
	}
}

// EncodeTagWithKey encodes the resource group tag, returns the encoded bytes.
func (b *ResourceGroupTagBuilder) EncodeTagWithKey(key []byte) []byte {
	tag := &tipb.ResourceGroupTag{KeyspaceName: b.keyspaceName}
	if b.sqlDigest != nil {
		tag.SqlDigest = b.sqlDigest.Bytes()
	}
	if b.planDigest != nil {
		tag.PlanDigest = b.planDigest.Bytes()
	}
	if len(key) > 0 {
		tag.TableId = decodeTableID(key)
		label := resourcegrouptag.GetResourceGroupLabelByKey(key)
		tag.Label = &label
	}
	tagEncoded, err := tag.Marshal()
	if err != nil {
		return nil
	}
	return tagEncoded
}

// Build builds the resource group tag for the request.
func (b *ResourceGroupTagBuilder) Build(req *tikvrpc.Request) {
	if req == nil {
		return
	}
	if encodedBytes := b.EncodeTagWithKey(resourcegrouptag.GetFirstKeyFromRequest(req)); len(encodedBytes) > 0 {
		req.ResourceGroupTag = encodedBytes
	}
}

// DecodeTableIDFunc is used to decode table id from key.
var DecodeTableIDFunc func(Key) int64

// avoid import cycle, not import tablecodec in kv package.
func decodeTableID(key Key) int64 {
	if DecodeTableIDFunc != nil {
		return DecodeTableIDFunc(key)
	}
	return 0
}
