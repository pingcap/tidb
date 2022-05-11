// Copyright 2021 PingCAP, Inc.

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

// Transaction options
const (
	// BinlogInfo contains the binlog data and client.
	BinlogInfo int = iota + 1
	// SchemaChecker is used for checking schema-validity.
	SchemaChecker
	// IsolationLevel sets isolation level for current transaction. The default level is SI.
	IsolationLevel
	// Priority marks the priority of this transaction.
	Priority
	// NotFillCache makes this request do not touch the LRU cache of the underlying storage.
	NotFillCache
	// SyncLog is not used anymore.
	SyncLog
	// KeyOnly retrieve only keys, it can be used in scan now.
	KeyOnly
	// Pessimistic is defined for pessimistic lock
	Pessimistic
	// SnapshotTS is defined to set snapshot ts.
	SnapshotTS
	// Set replica read
	ReplicaRead
	// Set task ID
	TaskID
	// InfoSchema is schema version used by txn startTS.
	InfoSchema
	// CollectRuntimeStats is used to enable collect runtime stats.
	CollectRuntimeStats
	// SchemaAmender is used to amend mutations for pessimistic transactions
	SchemaAmender
	// SampleStep skips 'SampleStep - 1' number of keys after each returned key.
	SampleStep
	// CommitHook is a callback function called right after the transaction gets committed
	CommitHook
	// EnableAsyncCommit indicates whether async commit is enabled
	EnableAsyncCommit
	// Enable1PC indicates whether one-phase commit is enabled
	Enable1PC
	// GuaranteeLinearizability indicates whether to guarantee linearizability at the cost of an extra tso request before prewrite
	GuaranteeLinearizability
	// TxnScope indicates which @@txn_scope this transaction will work with.
	TxnScope
	// ReadReplicaScope
	ReadReplicaScope
	// StalenessReadOnly indicates whether the transaction is staleness read only transaction
	IsStalenessReadOnly
	// MatchStoreLabels indicates the labels the store should be matched
	MatchStoreLabels
	// ResourceGroupTag indicates the resource group tag of the kv request.
	ResourceGroupTag
	// ResourceGroupTagger can be used to set the ResourceGroupTag dynamically according to the request content. It will be used only when ResourceGroupTag is nil.
	ResourceGroupTagger
	// KVFilter indicates the filter to ignore key-values in the transaction's memory buffer.
	KVFilter
	// SnapInterceptor is used for setting the interceptor for snapshot
	SnapInterceptor
	// CommitTSUpperBoundChec is used by cached table
	// The commitTS must be greater than all the write lock lease of the visited cached table.
	CommitTSUpperBoundCheck
	// RPCInterceptor is interceptor.RPCInterceptor on Transaction or Snapshot, used to decorate
	// additional logic before and after the underlying client-go RPC request.
	RPCInterceptor
	// TableToColumnMaps is a map from tableID to a series of maps. The maps are needed when checking data consistency.
	// Save them here to reduce redundant computations.
	TableToColumnMaps
	// AssertionLevel controls how strict the assertions on data during transactions should be.
	AssertionLevel
)

// ReplicaReadType is the type of replica to read data from
type ReplicaReadType byte

const (
	// ReplicaReadLeader stands for 'read from leader'.
	ReplicaReadLeader ReplicaReadType = iota
	// ReplicaReadFollower stands for 'read from follower'.
	ReplicaReadFollower
	// ReplicaReadMixed stands for 'read from leader and follower'.
	ReplicaReadMixed
	// ReplicaReadClosest stands for 'read from leader and follower which locates with the same zone'
	ReplicaReadClosest
)

// IsFollowerRead checks if follower is going to be used to read data.
func (r ReplicaReadType) IsFollowerRead() bool {
	return r != ReplicaReadLeader
}

// IsClosestRead checks whether is going to request closet store to read
func (r ReplicaReadType) IsClosestRead() bool {
	return r == ReplicaReadClosest
}
