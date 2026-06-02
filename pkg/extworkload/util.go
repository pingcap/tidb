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

package extworkload

import (
	"context"

	"github.com/pingcap/tidb/pkg/config"
)

// IsEnabled reports whether a Manager is installed for this process.
func IsEnabled() bool { return globalManager != nil }

// IsMaster reports whether this TiDB is acting as the external workload master.
func IsMaster() bool { return roleIs(config.RoleMaster) }

// IsGCWorker reports whether this TiDB is a dedicated GC worker.
func IsGCWorker() bool { return roleIs(config.RoleGCWorker) }

// IsGCV2Worker reports whether this TiDB is a dedicated keyspace-level GC worker.
func IsGCV2Worker() bool { return roleIs(config.RoleGCV2Worker) }

// IsDDLWorker reports whether this TiDB is a dedicated DDL worker.
func IsDDLWorker() bool { return roleIs(config.RoleDDLWorker) }

// IsBatchWorker reports whether this TiDB is a dedicated batch worker.
func IsBatchWorker() bool { return roleIs(config.RoleBatchWorker) }

// IsImportIntoWorker reports whether this TiDB is a dedicated IMPORT INTO worker.
func IsImportIntoWorker() bool { return roleIs(config.RoleImportIntoWorker) }

// IsSharedWorker reports whether this TiDB is a shared worker (handles ddl,
// batch, import-into and optionally ttl / auto-analyze depending on the
// shared-worker bg-task config).
func IsSharedWorker() bool { return roleIs(config.RoleSharedWorker) }

// IsRemoteQueryWorker reports whether this TiDB is a remote-query worker.
func IsRemoteQueryWorker() bool { return roleIs(config.RoleRemoteQueryWorker) }

// IsTTLTaskWorker reports whether this TiDB should run TTL jobs: either the
// dedicated ttl role, or the shared role with ttl enabled by the controller.
func IsTTLTaskWorker() bool {
	if !IsEnabled() {
		return false
	}
	if globalManager.Role() == config.RoleTTLTaskWorker {
		return true
	}
	return globalManager.Role() == config.RoleSharedWorker &&
		IsBgTaskEnabled(context.Background(), WorkerTypeTTL)
}

// IsAutoAnalyzeWorker reports whether this TiDB should run auto-analyze jobs.
func IsAutoAnalyzeWorker() bool {
	if !IsEnabled() {
		return false
	}
	if globalManager.Role() == config.RoleAutoAnalyzeWorker {
		return true
	}
	return globalManager.Role() == config.RoleSharedWorker &&
		IsBgTaskEnabled(context.Background(), WorkerTypeAutoAnalyze)
}

// IsBgTaskEnabled reports whether the controller has provisioned at least
// one worker (or has auto-scaling enabled) for the given background task type.
// Errors talking to the controller are treated as "not enabled".
func IsBgTaskEnabled(ctx context.Context, workerType string) bool {
	if !IsEnabled() {
		return false
	}
	count, autoScale, err := globalManager.GetBgTaskConfig(ctx, workerType)
	if err != nil {
		return false
	}
	return autoScale || count > 0
}

func roleIs(role string) bool {
	return IsEnabled() && globalManager.Role() == role
}
