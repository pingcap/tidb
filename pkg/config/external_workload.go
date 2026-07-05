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

package config

import (
	"fmt"
	"strings"
)

// ExternalWorkloadRole is the role name accepted by [external-workload].
type ExternalWorkloadRole string

// External workload roles accepted by the [external-workload] config.
const (
	// RoleMaster is the regular TiDB role that delegates selected background work to the controller.
	RoleMaster ExternalWorkloadRole = "master"
	// RoleGCV2Worker runs keyspace-level GC work assigned by the controller.
	RoleGCV2Worker ExternalWorkloadRole = "gcv2"
	// RoleTTLTaskWorker runs TTL work assigned by the controller.
	RoleTTLTaskWorker ExternalWorkloadRole = "ttl"
	// RoleAutoAnalyzeWorker runs auto-analyze work assigned by the controller.
	RoleAutoAnalyzeWorker ExternalWorkloadRole = "auto-analyze"
)

// ExternalWorkload is the Starter-only [external-workload] section.
type ExternalWorkload struct {
	Enable bool                 `toml:"enable" json:"enable,omitempty"`
	Role   ExternalWorkloadRole `toml:"role" json:"role,omitempty"`
	// TidbPool names the serving pool, for example vip-tidb-pool or super-vip-tidb-pool.
	TidbPool string `toml:"tidb-pool" json:"tidb-pool,omitempty"`
	// ControllerAddr is the external workload controller address.
	ControllerAddr string `toml:"controller-addr" json:"controller-addr,omitempty"`
}

func defaultExternalWorkload() ExternalWorkload {
	return ExternalWorkload{}
}

// Valid normalizes and validates an enabled [external-workload] section.
func (w *ExternalWorkload) Valid() error {
	if !w.Enable {
		return nil
	}
	w.Role = w.Role.normalized()
	if w.Role == "" {
		w.Role = RoleMaster
	}
	w.ControllerAddr = strings.TrimSpace(w.ControllerAddr)
	w.TidbPool = strings.TrimSpace(w.TidbPool)
	if w.ControllerAddr == "" {
		return fmt.Errorf("external-workload controller-addr must not be empty when enabled")
	}
	if !w.Role.valid() {
		return fmt.Errorf("invalid external-workload role %q", w.Role)
	}
	if w.TidbPool == "" {
		return fmt.Errorf("external-workload tidb-pool must not be empty when enabled")
	}
	return nil
}

func (w ExternalWorkload) isConfigured() bool {
	return w.Enable ||
		w.Role.normalized() != "" ||
		strings.TrimSpace(w.ControllerAddr) != "" ||
		strings.TrimSpace(w.TidbPool) != ""
}

func (r ExternalWorkloadRole) normalized() ExternalWorkloadRole {
	return ExternalWorkloadRole(strings.ToLower(strings.TrimSpace(string(r))))
}

func (r ExternalWorkloadRole) valid() bool {
	switch r {
	case RoleMaster, RoleGCV2Worker, RoleTTLTaskWorker, RoleAutoAnalyzeWorker:
		return true
	default:
		return false
	}
}
