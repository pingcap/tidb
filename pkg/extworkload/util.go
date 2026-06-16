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

import "github.com/pingcap/tidb/pkg/config"

// IsEnabled reports whether a Manager is installed for this process.
func IsEnabled() bool { return globalManager != nil }

// IsMaster reports whether this TiDB is acting as the external workload master.
func IsMaster() bool { return roleIs(config.RoleMaster) }

// IsGCV2Worker reports whether this TiDB is a dedicated keyspace-level GC worker.
func IsGCV2Worker() bool { return roleIs(config.RoleGCV2Worker) }

// IsTTLTaskWorker reports whether this TiDB should run TTL jobs.
func IsTTLTaskWorker() bool { return roleIs(config.RoleTTLTaskWorker) }

// IsAutoAnalyzeWorker reports whether this TiDB should run auto-analyze jobs.
func IsAutoAnalyzeWorker() bool { return roleIs(config.RoleAutoAnalyzeWorker) }

func roleIs(role config.ExternalWorkloadRole) bool {
	return IsEnabled() && globalManager.Role() == role
}
