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

import "github.com/pingcap/tidb/pkg/config/deploymode"

// ManagerProvider is implemented by storage instances that carry an external
// workload manager.
type ManagerProvider interface {
	ExternalWorkloadManager() Manager
}

// ManagerSetter is implemented by storage instances that can install an
// external workload manager.
type ManagerSetter interface {
	SetExternalWorkloadManager(Manager)
}

// SetManagerForStore installs the manager on a storage instance that supports it.
func SetManagerForStore(store any, mgr Manager) bool {
	setter, ok := store.(ManagerSetter)
	if !ok {
		return false
	}
	setter.SetExternalWorkloadManager(mgr)
	return true
}

// GetManagerFromStore returns the manager bound to store only in Starter deploy mode.
func GetManagerFromStore(store any) Manager {
	if !deploymode.IsStarter() {
		return nil
	}
	provider, ok := store.(ManagerProvider)
	if !ok {
		return nil
	}
	return provider.ExternalWorkloadManager()
}
