// Copyright 2026 PingCAP, Inc.
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

// Package extworkload exposes a Manager that lets TiDB coordinate background
// workloads (GC, distributed background subtasks, TTL, auto-analyze, remote
// queries) with an external workload controller. The Manager is only installed
// under the Starter deploy mode and only when the [external-workload] section is
// enabled; otherwise GetManager returns nil and TiDB continues to run all
// background workloads itself.
package extworkload

import (
	"context"

	externalworkloadpb "github.com/pingcap/kvproto/pkg/externalworkloadpb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
)

// Manager coordinates TiDB background workloads with the external workload controller.
//
// Every method maps to a single controller RPC (see kvproto/externalworkloadpb).
// Implementations are responsible for emitting metrics and surfacing
// transport errors; callers do not need to wrap.
type Manager interface {
	// Role returns the role this TiDB is acting in (see config.Role* constants).
	Role() string
	// Meta returns the keyspace metadata bound to this TiDB.
	Meta() *keyspacepb.KeyspaceMeta

	// GC v1 (delete-range based) ----------------------------------------------

	// RegisterGC notifies the controller of a newly inserted delete-range entry.
	RegisterGC(ctx context.Context, deletionTs uint64) error
	// RecycleGC notifies the controller that delete-range entries at or before
	// safePoint have been processed.
	RecycleGC(ctx context.Context, safePoint uint64) error

	// GC v2 (keyspace-level safe point) ---------------------------------------

	// InitializeGCV2 seeds the controller with an initial keyspace-level GC task.
	InitializeGCV2(ctx context.Context) error
	// AbortGCV2 asks the controller to abort all outstanding keyspace-level GC tasks.
	AbortGCV2(ctx context.Context) error
	// RegisterGCV2 reports that a keyspace-level GC round at safePoint has completed.
	RegisterGCV2(ctx context.Context, safePoint uint64, gcLifeTime int64) error
	// RecycleGCV2 reports that keyspace-level GC up to safePoint has been processed.
	RecycleGCV2(ctx context.Context, safePoint uint64) error
	// UpdateGCLifeTime reports that the user's gc_life_time has changed.
	UpdateGCLifeTime(ctx context.Context, gcLifeTime int64) error

	// Distributed background task framework -----------------------------------

	// GetBgTaskConfig fetches the worker-pool configuration for workerType.
	GetBgTaskConfig(ctx context.Context, workerType string) (workerCount int, autoScaleEnabled bool, err error)
	// RegisterBgTask reports that a distributed subtask has been created.
	RegisterBgTask(ctx context.Context, taskType, taskKey string, gTaskID, subTaskID int64, execID string) error
	// RecycleBgTask reports that a distributed subtask has completed. taskType
	// is only used for metrics; the wire request carries only the global task ID
	// and subtask ID.
	RecycleBgTask(ctx context.Context, taskType string, gTaskID, subTaskID int64) error
	// UpdateBgTaskExecID reports that the subtask-to-exec-ID assignment for a
	// global task has changed.
	UpdateBgTaskExecID(ctx context.Context, gTaskID int64, assignments []*externalworkloadpb.SubtaskExecIDAssignment) error

	// Remote query ------------------------------------------------------------

	// RegisterRemoteQuery reports that a query is being routed to a remote TiDB.
	RegisterRemoteQuery(ctx context.Context, queryID, queryAddr string) error

	// TTL ---------------------------------------------------------------------

	// RegisterTTLTask reports that a table with TTL has been created or altered.
	RegisterTTLTask(ctx context.Context, tableID int64, ttlJobEnable bool) error
	// DeleteTTLTableInfo reports that TTL was removed from a table, or the
	// table was dropped.
	DeleteTTLTableInfo(ctx context.Context, tableID int64) error
	// RecycleTTLTask reports that a TTL job has completed.
	RecycleTTLTask(ctx context.Context, completedJobCreateTime uint64) error
	// UpdateTTLJobEnable reports a change in the tidb_ttl_job_enable system variable.
	UpdateTTLJobEnable(ctx context.Context, ttlJobEnable bool) error

	// Auto-analyze ------------------------------------------------------------

	// RegisterAutoAnalyze reports a newly registered auto-analyze task.
	RegisterAutoAnalyze(ctx context.Context, taskID uint64) error
	// RecycleAutoAnalyze reports that an auto-analyze task has completed.
	RecycleAutoAnalyze(ctx context.Context, taskID uint64) error
}

var globalManager Manager

// GetManager returns the installed Manager, or nil if none is installed.
//
// Callers should check the return value (or use IsEnabled / role predicates
// in util.go) before invoking methods.
func GetManager() Manager { return globalManager }

// SetManagerForTest installs m as the global manager and returns a restore
// function. It bypasses InitManager's deploy-mode and idempotency guards and
// must only be used in tests.
func SetManagerForTest(m Manager) (restore func()) {
	prev := globalManager
	globalManager = m
	return func() { globalManager = prev }
}
