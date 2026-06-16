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
	// RoleMaster is the regular TiDB role that reports background work to the controller.
	RoleMaster            ExternalWorkloadRole = "master"
	RoleGCV2Worker        ExternalWorkloadRole = "gcv2"
	RoleTTLTaskWorker     ExternalWorkloadRole = "ttl"
	RoleAutoAnalyzeWorker ExternalWorkloadRole = "auto-analyze"
)

// ExternalWorkload is the Starter-only [external-workload] section.
type ExternalWorkload struct {
	Enable        bool                 `toml:"enable" json:"enable,omitempty"`
	Role          ExternalWorkloadRole `toml:"role" json:"role,omitempty"`
	TidbPool      string               `toml:"tidb-pool" json:"tidb-pool,omitempty"`
	APIServerAddr string               `toml:"api-server" json:"api-server,omitempty"`
}

func defaultExternalWorkload() ExternalWorkload {
	return ExternalWorkload{}
}

// Valid normalizes and validates an enabled [external-workload] section.
func (w *ExternalWorkload) Valid() error {
	if !w.Enable {
		return nil
	}
	w.Role = ExternalWorkloadRole(strings.ToLower(strings.TrimSpace(string(w.Role))))
	if w.Role == "" {
		w.Role = RoleMaster
	}
	w.APIServerAddr = strings.TrimSpace(w.APIServerAddr)
	w.TidbPool = strings.TrimSpace(w.TidbPool)
	if w.APIServerAddr == "" {
		return fmt.Errorf("external-workload api-server must not be empty when enabled")
	}
	switch w.Role {
	case RoleMaster:
	case RoleGCV2Worker,
		RoleTTLTaskWorker,
		RoleAutoAnalyzeWorker:
	default:
		return fmt.Errorf("invalid external-workload role %q", w.Role)
	}
	if w.TidbPool == "" {
		return fmt.Errorf("external-workload tidb-pool must not be empty when enabled")
	}
	return nil
}

func (w ExternalWorkload) isConfigured() bool {
	role := ExternalWorkloadRole(strings.ToLower(strings.TrimSpace(string(w.Role))))
	return w.Enable ||
		role != "" ||
		strings.TrimSpace(w.APIServerAddr) != "" ||
		strings.TrimSpace(w.TidbPool) != ""
}
