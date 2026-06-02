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
	"os"
	"strings"
)

// Roles for the [external-workload] section. Master TiDBs register background
// workloads with the external workload controller; worker TiDBs consume them.
const (
	RoleMaster            = "master"
	RoleGCWorker          = "gc"
	RoleGCV2Worker        = "gcv2"
	RoleDDLWorker         = "ddl"
	RoleBatchWorker       = "batch"
	RoleImportIntoWorker  = "import-into"
	RoleSharedWorker      = "shared"
	RoleRemoteQueryWorker = "remote-query"
	RoleTTLTaskWorker     = "ttl"
	RoleAutoAnalyzeWorker = "auto-analyze"
)

// Environment variables consulted by Valid for worker roles. When a TiDB is
// scheduled by the controller these are typically injected into the pod and
// override the corresponding TOML fields.
const (
	EnvVarExecID   = "EXEC_ID"
	EnvVarTiDBPool = "TIDB_POOL"
)

// ExternalWorkload is the [external-workload] section of the TiDB configuration. It is
// only honored under Starter deploy mode; in all other deploy modes the
// section is parsed but has no effect.
type ExternalWorkload struct {
	Enable        bool   `toml:"enable" json:"enable"`
	Role          string `toml:"role" json:"role"`
	TidbPool      string `toml:"tidb-pool" json:"tidb-pool"`
	APIServerAddr string `toml:"api-server" json:"api-server"`
	ExecID        string `toml:"exec-id" json:"exec-id"`
}

func defaultExternalWorkload() ExternalWorkload {
	return ExternalWorkload{
		Enable:   false,
		Role:     RoleMaster,
		TidbPool: "tidb-pool",
	}
}

// Valid normalizes and validates a [external-workload] section. It is a no-op when
// Enable is false. Worker roles may also pick up their ExecID / TidbPool from
// environment variables (see EnvVar*).
func (w *ExternalWorkload) Valid() error {
	if !w.Enable {
		return nil
	}
	w.Role = strings.ToLower(w.Role)
	switch w.Role {
	case RoleMaster, RoleGCWorker, RoleGCV2Worker, RoleTTLTaskWorker:
	case RoleDDLWorker,
		RoleBatchWorker,
		RoleImportIntoWorker,
		RoleSharedWorker,
		RoleRemoteQueryWorker,
		RoleAutoAnalyzeWorker:
		if v := os.Getenv(EnvVarExecID); v != "" {
			w.ExecID = v
		}
		if v := os.Getenv(EnvVarTiDBPool); v != "" {
			w.TidbPool = v
		}
	default:
		return fmt.Errorf("invalid external-workload role %q", w.Role)
	}
	return nil
}
