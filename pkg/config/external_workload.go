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

// External workload role names accepted by [external-workload].
const (
	RoleMaster            = "master"
	RoleGCV2Worker        = "gcv2"
	RoleTTLTaskWorker     = "ttl"
	RoleAutoAnalyzeWorker = "auto-analyze"
)

// ExternalWorkload is the Starter-only [external-workload] section.
type ExternalWorkload struct {
	Enable        bool   `toml:"enable" json:"enable"`
	Role          string `toml:"role" json:"role"`
	TidbPool      string `toml:"tidb-pool" json:"tidb-pool"`
	APIServerAddr string `toml:"api-server" json:"api-server"`
}

func defaultExternalWorkload() ExternalWorkload {
	return ExternalWorkload{
		Enable: false,
		Role:   RoleMaster,
	}
}

// Valid normalizes and validates an enabled [external-workload] section.
func (w *ExternalWorkload) Valid() error {
	if !w.Enable {
		return nil
	}
	w.Role = strings.ToLower(strings.TrimSpace(w.Role))
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
