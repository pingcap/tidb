// Copyright 2021 PingCAP, Inc.

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
	// StalenessReadOnly indicates whether the transaction is staleness read only transaction
	IsStalenessReadOnly
	// MatchStoreLabels indicates the labels the store should be matched
	MatchStoreLabels
)

// Priority value for transaction priority.
const (
	PriorityNormal = iota
	PriorityLow
	PriorityHigh
)
