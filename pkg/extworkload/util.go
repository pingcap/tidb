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
	"github.com/pingcap/tidb/pkg/kv"
)

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

type managerStoreKey struct{}

// SetManagerForStore binds the manager to store. Passing nil removes it.
func SetManagerForStore(store kv.Storage, mgr Manager) {
	if store == nil {
		return
	}
	store.SetOption(managerStoreKey{}, mgr)
}

// GetManagerFromStore returns the manager bound to store.
func GetManagerFromStore(store kv.Storage) Manager {
	if store == nil {
		return nil
	}
	v, ok := store.GetOption(managerStoreKey{})
	if !ok {
		return nil
	}
	mgr, _ := v.(Manager)
	return mgr
}

// AbortGCV2ForUpgrade aborts GCV2 work when this TiDB is the dedicated GCV2
// worker. It returns true when the caller must terminate after the abort.
func AbortGCV2ForUpgrade(ctx context.Context, mgr Manager) (bool, error) {
	if !IsGCV2Worker(mgr) {
		return false, nil
	}
	if err := mgr.AbortGCV2(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func roleIs(m Manager, role config.ExternalWorkloadRole) bool {
	return IsEnabled(m) && m.Role() == role
}
