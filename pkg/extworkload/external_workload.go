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
// workloads (GCV2, TTL, and auto-analyze) with an external workload controller.
// TiDB only creates a Manager in Starter deploy mode when [external-workload]
// is enabled.
package extworkload

import (
	"context"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
)

// Manager coordinates TiDB background workloads with the external workload controller.
type Manager interface {
	Close() error

	// Role returns the role this TiDB is acting in.
	Role() config.ExternalWorkloadRole
	// Meta returns the keyspace metadata bound to this TiDB.
	Meta() *keyspacepb.KeyspaceMeta

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

	// RegisterTTLTask reports that a table with TTL has been created or altered.
	RegisterTTLTask(ctx context.Context, tableID int64, ttlJobEnable bool) error
	// DeleteTTLTableInfo reports that TTL was removed from a table, or the
	// table was dropped.
	DeleteTTLTableInfo(ctx context.Context, tableID int64) error
	// RecycleTTLTask reports that a TTL job has completed.
	RecycleTTLTask(ctx context.Context, completedJobCreateTime uint64) error
	// UpdateTTLJobEnable reports a change in the tidb_ttl_job_enable system variable.
	UpdateTTLJobEnable(ctx context.Context, ttlJobEnable bool) error

	// RegisterAutoAnalyze reports a newly registered auto-analyze task.
	RegisterAutoAnalyze(ctx context.Context, taskID uint64) error
	// RecycleAutoAnalyze reports that an auto-analyze task has completed.
	RecycleAutoAnalyze(ctx context.Context, taskID uint64) error
}
