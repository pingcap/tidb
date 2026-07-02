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
	"os"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
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

// AbortGCV2ForUpgrade aborts GCV2 work and exits when this TiDB is the dedicated
// GCV2 worker, which must not run normal bootstrap upgrade work.
func AbortGCV2ForUpgrade(store kv.Storage) {
	mgr := GetManagerFromStore(store)
	if !IsGCV2Worker(mgr) {
		return
	}
	if err := mgr.AbortGCV2(context.Background()); err != nil {
		logutil.BgLogger().Fatal("abort GCV2 worker failed", zap.Error(err))
	}
	logutil.BgLogger().Info("GCV2 worker aborted")
	if intest.InTest {
		return
	}
	os.Exit(0)
}

func roleIs(m Manager, role config.ExternalWorkloadRole) bool {
	return IsEnabled(m) && m.Role() == role
}
