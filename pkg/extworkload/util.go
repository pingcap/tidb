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

// IsEnabled reports whether a Manager is present.
func IsEnabled(m Manager) bool { return m != nil }

// IsMaster reports whether this TiDB is in the regular TiDB role.
func IsMaster(m Manager) bool { return roleIs(m, config.RoleMaster) }

// IsGCV2Worker reports whether this TiDB is a dedicated keyspace-level GC worker.
func IsGCV2Worker(m Manager) bool { return roleIs(m, config.RoleGCV2Worker) }

// IsTTLTaskWorker reports whether this TiDB should run TTL jobs.
func IsTTLTaskWorker(m Manager) bool { return roleIs(m, config.RoleTTLTaskWorker) }

// IsAutoAnalyzeWorker reports whether this TiDB should run auto-analyze jobs.
func IsAutoAnalyzeWorker(m Manager) bool { return roleIs(m, config.RoleAutoAnalyzeWorker) }

func roleIs(m Manager, role config.ExternalWorkloadRole) bool {
	return IsEnabled(m) && m.Role() == role
}
